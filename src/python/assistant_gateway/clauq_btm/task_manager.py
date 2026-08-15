from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Dict, Optional
from uuid import uuid4

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager
    from assistant_gateway.clauq_btm.queue_manager.subscription import EventSubscription


class BackgroundTasksUnavailableError(Exception):
    """Raised when background task operations are attempted but queue manager is not available."""

    pass


# Type alias for task executor: (task) -> Any
TaskExecutor = Callable[[ClauqBTMTask], Awaitable[Any]]

# Type alias for post-execution callback: (task, result) -> None
PostExecutionCallback = Callable[[ClauqBTMTask, Any], Awaitable[None]]


class BTMTaskManager:
    """
    Task Manager for both synchronous and background task execution.

    Example: sync mode - executor passed directly:
        task_manager = BTMTaskManager()
        async with task_manager:
            task, result = await task_manager.create_and_execute_sync(
                executor=my_executor,
            )

    Example: background mode - executor looked up by name:
        # Executor must be pre-registered in queue_manager.executor_registry
        task_manager = BTMTaskManager(queue_manager)
        async with task_manager:
            task = await task_manager.create_and_enqueue(
                queue_id="my_queue",
                executor_name="my_task",  # Must be pre-registered
            )
    """

    def __init__(self, queue_manager: Optional["CeleryQueueManager"] = None) -> None:
        self._queue_manager = queue_manager
        self._lock = asyncio.Lock() # TODO: check if centralized locking is needed

        self._sync_tasks: Dict[str, ClauqBTMTask] = {}

    def create_task(
        self,
        is_background_task: bool,
        queue_id: Optional[str] = None,
        executor_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ClauqBTMTask:
        now = datetime.now(timezone.utc)
        return ClauqBTMTask(
            id=str(uuid4()),
            is_background_task=is_background_task,
            queue_id=queue_id,
            executor_name=executor_name,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            payload=payload or {},
            metadata=metadata or {},
        )

    async def create_and_execute_sync(
        self,
        executor: TaskExecutor,
        post_execution: Optional[PostExecutionCallback] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> tuple[ClauqBTMTask, Optional[Any]]:
        """
        Create and execute a task synchronously (inline).
        """
        task = self.create_task(
            is_background_task=False,
            queue_id=None,
            executor_name=None,
            payload=payload,
            metadata=metadata,
        )

        async with self._lock:
            self._sync_tasks[task.id] = task

        if task.is_interrupted():
            return task, None

        await self._update_task_status(task, TaskStatus.in_progress)

        try:
            result = await executor(task)

            current_task = await self.get_task(task.id)
            if current_task and current_task.is_interrupted():
                return current_task, None

            if post_execution:
                await post_execution(task, result)

            task.result = result
            await self._update_task_status(task, TaskStatus.completed)
            return task, result

        except Exception as exc:
            task.error = str(exc)
            await self._update_task_status(task, TaskStatus.failed)
            raise

    async def create_and_enqueue(
        self,
        queue_id: str,
        executor_name: str,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ClauqBTMTask:
        """
        Create and enqueue a task for background execution.

        The executor is looked up by name from the queue manager's executor_registry.
        Executors must be pre-registered before calling this method.
        """
        if self._queue_manager is None:
            raise BackgroundTasksUnavailableError(
                "Background tasks are not available because queue manager setup failed. "
                "Use create_and_execute_sync() for synchronous task execution."
            )

        task = self.create_task(
            is_background_task=True,
            queue_id=queue_id,
            executor_name=executor_name,
            payload=payload,
            metadata=metadata,
        )

        await self._queue_manager.enqueue(task)

        return task

    async def get_task(self, task_id: str) -> Optional[ClauqBTMTask]:
        async with self._lock:
            if task_id in self._sync_tasks:
                return self._sync_tasks[task_id]

        if self._queue_manager is None:
            return None
        return await self._queue_manager.get(task_id)

    async def interrupt_task(self, task_id: str) -> Optional[ClauqBTMTask]:
        task = await self.get_task(task_id)
        if task is None:
            return None

        if task.is_terminal():
            return task

        async with self._lock:
            if task_id in self._sync_tasks:
                sync_task = self._sync_tasks[task_id]
                if sync_task.status in (TaskStatus.pending, TaskStatus.in_progress):
                    sync_task.status = TaskStatus.interrupted
                    sync_task.updated_at = datetime.now(timezone.utc)
                    self._sync_tasks[task_id] = sync_task
                return sync_task

        if self._queue_manager is None or task.queue_id is None:
            return None
        return await self._queue_manager.interrupt(task.queue_id, task_id)

    async def is_task_interrupted(self, task_id: str) -> bool:
        task = await self.get_task(task_id)
        return task is not None and task.is_interrupted()

    @asynccontextmanager
    async def subscribe(self, queue_id: str) -> AsyncIterator["EventSubscription"]:
        if self._queue_manager is None:
            raise BackgroundTasksUnavailableError(
                "Event subscription is not available because queue manager setup failed."
            )

        async with self._queue_manager.subscribe(queue_id) as subscription:
            yield subscription

    async def _update_task_status(self, task: ClauqBTMTask, status: TaskStatus) -> None:
        task.status = status
        task.updated_at = datetime.now(timezone.utc)
        async with self._lock:
            if task.id in self._sync_tasks:
                self._sync_tasks[task.id] = task

    async def start(self) -> None:
        if self._queue_manager is not None:
            await self._queue_manager.start()

    async def stop(self) -> None:
        if self._queue_manager is not None:
            await self._queue_manager.stop()

    @property
    def is_running(self) -> bool:
        if self._queue_manager is None:
            return True
        return self._queue_manager.is_running

    @property
    def is_background_tasks_available(self) -> bool:
        return self._queue_manager is not None

    async def __aenter__(self) -> "BTMTaskManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
