"""
Task Manager for the Clau-Queue Background Task Manager.

Provides a unified interface for both synchronous and background task execution.
Handles task lifecycle management and queue coordination.

For distributed execution (Celery), executors must be pre-registered at
application initialization time. The same module that registers executors
must be imported by both API servers and Celery workers.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING
from uuid import uuid4

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager


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

    This manager provides:
    - Unified task creation for sync and background modes
    - Task lifecycle management (get, interrupt, wait)
    - Sync task execution (inline, no queue required)
    - Background task execution (via queue manager, queue_id required)

    For background tasks, executors must be pre-registered in the queue manager's
    executor_registry before tasks are enqueued. The BTMTaskManager does not
    maintain its own registry - it delegates to the queue manager.

    The queue_manager is optional:
    - If provided: Both sync and background tasks work
    - If None: Only sync tasks work; background tasks raise BackgroundTasksUnavailableError

    Usage (sync mode - executor passed directly):
        task_manager = BTMTaskManager(queue_manager)
        async with task_manager:
            task, result = await task_manager.create_and_execute_sync(
                executor=my_executor,
            )

    Usage (background mode - executor looked up by name):
        # Executor must be pre-registered in queue_manager.executor_registry
        task_manager = BTMTaskManager(queue_manager)
        async with task_manager:
            task = await task_manager.create_and_enqueue(
                queue_id="my_queue",
                executor_name="my_task",  # Must be pre-registered
            )
            result = await task_manager.wait_for_completion(task.id)
    """

    def __init__(self, queue_manager: Optional["CeleryQueueManager"] = None) -> None:
        """
        Initialize the task manager.

        Args:
            queue_manager: Optional queue manager for background task execution.
                          If None, only sync tasks will be available.
                          Must have executor_registry with pre-registered executors.
        """
        self._queue_manager = queue_manager
        self._lock = asyncio.Lock()

        # Maps task_id -> queue_id for background tasks only (sync tasks don't have queue_id)
        self._task_queue_mapping: Dict[str, str] = {}

        # Maps task_id -> ClauqBTMTask for sync tasks (background tasks stored in queue manager)
        self._sync_tasks: Dict[str, ClauqBTMTask] = {}

    # -------------------------------------------------------------------------
    # Task Creation and Execution
    # -------------------------------------------------------------------------

    def create_task(
        self,
        is_background_task: bool,
        queue_id: Optional[str] = None,
        executor_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ClauqBTMTask:
        """
        Create a new task with auto-generated ID.

        Args:
            is_background_task: Whether this is a background task (queued) or sync task
            queue_id: The queue to assign the task to (required for background tasks, must be None for sync tasks)
            executor_name: Name of pre-registered executor (required for background tasks)
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            A new ClauqBTMTask instance
        """
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

        The task runs immediately in the current context, not via the queue.
        Handles the full lifecycle: create -> check interrupt -> run -> post-execute.

        For sync execution, the executor is passed directly (not looked up from registry).

        Args:
            executor: The async function that executes the task
            post_execution: Optional callback after successful execution
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            Tuple of (task, result) where result is None if interrupted
        """
        # Create the task (sync tasks don't have a queue_id or executor_name)
        task = self.create_task(
            is_background_task=False,
            queue_id=None,
            executor_name=None,
            payload=payload,
            metadata=metadata,
        )

        # Register the task (sync tasks are tracked by task_id only, no queue_id)
        async with self._lock:
            self._sync_tasks[task.id] = task

        # Check if already interrupted before starting
        if task.is_interrupted():
            return task, None

        # Update to in_progress
        await self._update_task_status(task, TaskStatus.in_progress)

        try:
            # Run the executor
            result = await executor(task)

            # Check if interrupted during execution
            current_task = await self.get_task(task.id)
            if current_task and current_task.is_interrupted():
                return current_task, None

            # Post-execution callback
            if post_execution:
                await post_execution(task, result)

            # Mark as completed
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

        Args:
            queue_id: The queue to add the task to
            executor_name: Name of the pre-registered executor (required)
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            The created task (already enqueued)

        Raises:
            BackgroundTasksUnavailableError: If queue manager is not available
            KeyError: If executor_name is not found in the registry
        """
        if self._queue_manager is None:
            raise BackgroundTasksUnavailableError(
                "Background tasks are not available because queue manager setup failed. "
                "Use create_and_execute_sync() for synchronous task execution."
            )

        # Create the task (background tasks require queue_id and executor_name)
        task = self.create_task(
            is_background_task=True,
            queue_id=queue_id,
            executor_name=executor_name,
            payload=payload,
            metadata=metadata,
        )
        
        print('[BGDEBUG] clauq_btm task_manager create_and_enqueue task created:', task)

        # Track the mapping
        async with self._lock:
            self._task_queue_mapping[task.id] = queue_id

        # Enqueue the task (executor looked up by name in queue manager)
        await self._queue_manager.enqueue(task)

        return task

    # -------------------------------------------------------------------------
    # Task Operations
    # -------------------------------------------------------------------------

    async def get_task(self, task_id: str) -> Optional[ClauqBTMTask]:
        """
        Get a task by ID.

        Checks both sync tasks (local storage) and background tasks (queue manager).

        Args:
            task_id: The task ID to look up

        Returns:
            The task if found, None otherwise
        """
        # Check sync tasks first
        async with self._lock:
            if task_id in self._sync_tasks:
                return self._sync_tasks[task_id]
            queue_id = self._task_queue_mapping.get(task_id)

        if queue_id is None:
            return None

        # Check background tasks in queue manager
        if self._queue_manager is None:
            return None
        return await self._queue_manager.get(queue_id, task_id)

    async def interrupt_task(self, task_id: str) -> Optional[ClauqBTMTask]:
        """
        Interrupt a task by ID.

        Handles both sync and background tasks. Returns early if task
        is already in a terminal state.

        Args:
            task_id: The task ID to interrupt

        Returns:
            The interrupted task if found, None otherwise
        """
        task = await self.get_task(task_id)
        if task is None:
            return None

        # Already in terminal state - nothing to do
        if task.is_terminal():
            return task

        # Check if it's a sync task
        async with self._lock:
            if task_id in self._sync_tasks:
                sync_task = self._sync_tasks[task_id]
                if sync_task.status in (TaskStatus.pending, TaskStatus.in_progress):
                    sync_task.status = TaskStatus.interrupted
                    sync_task.updated_at = datetime.now(timezone.utc)
                    self._sync_tasks[task_id] = sync_task
                return sync_task

            queue_id = self._task_queue_mapping.get(task_id)

        if queue_id is None:
            return None

        # Interrupt background task via queue manager
        if self._queue_manager is None:
            return None
        return await self._queue_manager.interrupt(queue_id, task_id)

    async def wait_for_completion(
        self,
        task_id: str,
        timeout: Optional[float] = None,
    ) -> Optional[ClauqBTMTask]:
        """
        Wait for a task to complete.

        For sync tasks, returns immediately (they're already complete or running inline).
        For background tasks, waits via the queue manager.

        Args:
            task_id: The task ID to wait for
            timeout: Optional timeout in seconds

        Returns:
            The final task state, or None if not found or timeout occurs
        """
        # Check if it's a sync task (already complete)
        async with self._lock:
            if task_id in self._sync_tasks:
                return self._sync_tasks[task_id]
            queue_id = self._task_queue_mapping.get(task_id)

        if queue_id is None:
            return None

        if self._queue_manager is None:
            return None
        return await self._queue_manager.wait_for_completion(queue_id, task_id, timeout)

    async def is_task_interrupted(self, task_id: str) -> bool:
        """
        Check if a task has been interrupted.

        Args:
            task_id: The task ID to check

        Returns:
            True if the task was interrupted, False otherwise
        """
        task = await self.get_task(task_id)
        return task is not None and task.is_interrupted()

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    async def _update_task_status(self, task: ClauqBTMTask, status: TaskStatus) -> None:
        """Update the status of a sync task."""
        task.status = status
        task.updated_at = datetime.now(timezone.utc)
        async with self._lock:
            if task.id in self._sync_tasks:
                self._sync_tasks[task.id] = task

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start the task manager and its underlying queue manager.

        This must be called before enqueueing background tasks.
        If queue manager is not available, this is a no-op (sync tasks always work).
        """
        if self._queue_manager is not None:
            await self._queue_manager.start()

    async def stop(self) -> None:
        """
        Stop the task manager gracefully.

        Shuts down the queue manager and waits for running tasks to complete.
        If queue manager is not available, this is a no-op.
        """
        if self._queue_manager is not None:
            await self._queue_manager.stop()

    @property
    def is_running(self) -> bool:
        """
        Returns True if the task manager is running.

        If queue manager is not available, returns True (sync tasks always work).
        """
        if self._queue_manager is None:
            return True  # Sync-only mode is always "running"
        return self._queue_manager.is_running

    @property
    def is_background_tasks_available(self) -> bool:
        """Returns True if background tasks are available."""
        return self._queue_manager is not None

    async def __aenter__(self) -> "BTMTaskManager":
        """Start the task manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the task manager when exiting context."""
        await self.stop()
