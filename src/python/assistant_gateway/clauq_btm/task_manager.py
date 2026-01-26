"""
Task Manager for the Clau-Queue Background Task Manager.

Provides a unified interface for both synchronous and background task execution.
Handles task lifecycle management, interrupt handling, and queue coordination.

The executor registry is internal to this module - external code should not
interact with it directly. Executors are registered automatically when
create_and_enqueue is called with both executor and executor_name.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional
from uuid import uuid4

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.executor_registry import ExecutorRegistry
from assistant_gateway.clauq_btm.queue_manager.base import QueueManager


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
    - Sync task execution (inline)
    - Background task execution (via queue manager)
    - Internal executor registry management (hidden from external code)

    The executor registry is managed internally. When create_and_enqueue is called
    with both executor and executor_name, the executor is automatically registered.
    External code should not need to interact with the registry directly.

    Usage (sync mode):
        task_manager = BTMTaskManager(queue_manager)
        async with task_manager:
            task, result = await task_manager.create_and_execute_sync(
                queue_id="my_queue",
                executor=my_executor,
            )

    Usage (background mode):
        task_manager = BTMTaskManager(queue_manager)
        async with task_manager:
            task = await task_manager.create_and_enqueue(
                queue_id="my_queue",
                executor=my_executor,
                executor_name="my_task",  # Auto-registered internally
            )
            result = await task_manager.wait_for_completion(task.id)
    """

    def __init__(self, queue_manager: QueueManager) -> None:
        """
        Initialize the task manager.

        Args:
            queue_manager: The queue manager for background task execution
        """
        self._queue_manager = queue_manager
        self._lock = asyncio.Lock()

        # Internal executor registry - managed by this class
        self._executor_registry = ExecutorRegistry()

        # Share registry with queue manager (for Celery distributed execution)
        if hasattr(queue_manager, "set_executor_registry"):
            queue_manager.set_executor_registry(self._executor_registry)

        # Maps task_id -> queue_id for all tasks (sync and background)
        self._task_queue_mapping: Dict[str, str] = {}

        # Maps task_id -> ClauqBTMTask for sync tasks (background tasks stored in queue manager)
        self._sync_tasks: Dict[str, ClauqBTMTask] = {}

    # -------------------------------------------------------------------------
    # Task Creation and Execution
    # -------------------------------------------------------------------------

    def create_task(
        self,
        queue_id: str,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ClauqBTMTask:
        """
        Create a new task with auto-generated ID.

        Args:
            queue_id: The queue to assign the task to
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            A new ClauqBTMTask instance
        """
        now = datetime.now(timezone.utc)
        return ClauqBTMTask(
            id=str(uuid4()),
            queue_id=queue_id,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            payload=payload or {},
            metadata=metadata or {},
        )

    async def create_and_execute_sync(
        self,
        queue_id: str,
        executor: TaskExecutor,
        post_execution: Optional[PostExecutionCallback] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> tuple[ClauqBTMTask, Optional[Any]]:
        """
        Create and execute a task synchronously (inline).

        The task runs immediately in the current context, not via the queue.
        Handles the full lifecycle: create -> check interrupt -> run -> post-execute.

        Args:
            queue_id: The queue ID (for tracking purposes)
            executor: The async function that executes the task
            post_execution: Optional callback after successful execution
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            Tuple of (task, result) where result is None if interrupted
        """
        # Create the task
        task = self.create_task(queue_id, payload, metadata)

        # Register the task
        async with self._lock:
            self._task_queue_mapping[task.id] = queue_id
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
        executor: TaskExecutor,
        executor_name: str,
        post_execution: Optional[PostExecutionCallback] = None,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> ClauqBTMTask:
        """
        Create and enqueue a task for background execution.

        The executor is automatically registered in the internal registry
        using the provided executor_name. This registration is required for
        distributed execution (e.g., Celery workers).

        For in-memory execution, the executor is also embedded in the task.

        Args:
            queue_id: The queue to add the task to
            executor: The executor function (required)
            executor_name: Name for the executor (required, used for registration)
            post_execution: Optional callback after successful execution
            payload: Optional data payload for task execution
            metadata: Optional application-specific metadata

        Returns:
            The created task (already enqueued)
        """
        # Create the task
        task = self.create_task(queue_id, payload, metadata)

        # Create wrapper that handles post-execution and interrupt checking
        async def wrapped_executor(t: ClauqBTMTask) -> Any:
            # Check if interrupted before starting
            current = await self.get_task(t.id)
            if current and current.is_interrupted():
                return None

            # Run the actual executor
            result = await executor(t)

            # Check if interrupted during execution
            current = await self.get_task(t.id)
            if current and current.is_interrupted():
                return None

            # Post-execution callback
            if post_execution:
                await post_execution(t, result)

            return result

        # Register the wrapped executor internally (for distributed execution)
        self._executor_registry.add(executor_name, wrapped_executor)

        # Also embed executor in task (for in-memory execution)
        task.executor = wrapped_executor

        # Track the mapping
        async with self._lock:
            self._task_queue_mapping[task.id] = queue_id

        # Enqueue the task
        await self._queue_manager.enqueue(task, executor_name=executor_name)

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
        """
        await self._queue_manager.start()

    async def stop(self) -> None:
        """
        Stop the task manager gracefully.

        Shuts down the queue manager and waits for running tasks to complete.
        """
        await self._queue_manager.stop()

    @property
    def is_running(self) -> bool:
        """Returns True if the task manager is running."""
        return self._queue_manager.is_running

    async def __aenter__(self) -> "BTMTaskManager":
        """Start the task manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the task manager when exiting context."""
        await self.stop()
