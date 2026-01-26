"""
In-memory Queue Manager implementation.

This implementation uses asyncio for single-process task execution.
Suitable for development, testing, and single-instance deployments.
"""

from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import (
    Any,
    AsyncIterator,
    Deque,
    Dict,
    List,
    Optional,
    Set,
)

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.queue_manager.base import (
    EventSubscription,
    QueueInfo,
    QueueManager,
)


# -----------------------------------------------------------------------------
# Asyncio Event Subscription
# -----------------------------------------------------------------------------


class AsyncioEventSubscription(EventSubscription):
    """Event subscription backed by an asyncio.Queue."""

    def __init__(self) -> None:
        self._queue: asyncio.Queue[TaskEvent] = asyncio.Queue()
        self._closed = False

    def put_nowait(self, event: TaskEvent) -> None:
        """Add an event to the subscription queue."""
        if not self._closed:
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def __aiter__(self) -> AsyncIterator[TaskEvent]:
        return self

    async def __anext__(self) -> TaskEvent:
        """Get the next event from the subscription."""
        if self._closed:
            raise StopAsyncIteration

        while not self._closed:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                return event
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise StopAsyncIteration

        raise StopAsyncIteration

    async def close(self) -> None:
        """Close the subscription."""
        self._closed = True


# -----------------------------------------------------------------------------
# InMemoryQueueManager
# -----------------------------------------------------------------------------


class InMemoryQueueManager(QueueManager):
    """
    In-memory queue manager that processes tasks sequentially per queue.

    This implementation follows the worker/consumer pattern:
    - Each queue_id has its own FIFO queue and dedicated worker
    - Workers process tasks one at a time in order
    - New tasks are added to the back of the queue
    - Completion notifications are emitted when tasks finish

    Lifecycle:
    - Call start() to begin processing
    - Call stop() for graceful shutdown
    - Can be used as async context manager:

        async with InMemoryQueueManager() as qm:
            await qm.enqueue(task)

    Note: This implementation does not persist state. All tasks are lost
    on restart. For production use with persistence, use CeleryQueueManager.
    """

    def __init__(self) -> None:
        # Task queues: queue_id -> deque of pending tasks
        self._queues: Dict[str, Deque[ClauqBTMTask]] = {}

        # Currently executing task per queue: queue_id -> task
        self._current_tasks: Dict[str, ClauqBTMTask] = {}

        # Maps task_id -> running asyncio.Task for cancellation
        self._running_asyncio_tasks: Dict[str, asyncio.Task[Any]] = {}

        # Worker tasks per queue: queue_id -> worker asyncio.Task
        self._workers: Dict[str, asyncio.Task[Any]] = {}

        # Event to signal new work available: queue_id -> Event
        self._work_available: Dict[str, asyncio.Event] = {}

        # Event objects for waiting on task completion: task_id -> Event
        self._completion_events: Dict[str, asyncio.Event] = {}

        # Subscriptions for streaming events: queue_id -> list of subscriptions
        self._subscriptions: Dict[str, List[AsyncioEventSubscription]] = {}

        # Store completed/interrupted tasks that were removed from queue
        self._completed_tasks: Dict[str, ClauqBTMTask] = {}

        # Lifecycle state
        self._is_running = False
        self._shutdown_event: Optional[asyncio.Event] = None

        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

        # Track all queue_ids that have been created
        self._known_queues: Set[str] = set()

        # Queue creation timestamps
        self._queue_created_at: Dict[str, datetime] = {}

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the queue manager's background processing."""
        if self._is_running:
            return

        self._is_running = True
        self._shutdown_event = asyncio.Event()

        # Start workers for any existing queues
        async with self._lock:
            for queue_id in self._known_queues:
                self._ensure_worker_started(queue_id)

    async def stop(self) -> None:
        """
        Stop the queue manager gracefully.

        Cancels all running tasks and shuts down workers.
        """
        if not self._is_running:
            return

        self._is_running = False

        # Signal shutdown
        if self._shutdown_event:
            self._shutdown_event.set()

        # Wake up all workers so they can exit
        async with self._lock:
            for event in self._work_available.values():
                event.set()

        # Cancel all running task executions
        async with self._lock:
            for asyncio_task in self._running_asyncio_tasks.values():
                if not asyncio_task.done():
                    asyncio_task.cancel()

        # Wait for all workers to finish
        workers = list(self._workers.values())
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

        async with self._lock:
            self._workers.clear()

        # Close all subscriptions
        async with self._lock:
            for subs in self._subscriptions.values():
                for sub in subs:
                    await sub.close()
            self._subscriptions.clear()

    @property
    def is_running(self) -> bool:
        """Returns True if the queue manager is running."""
        return self._is_running

    def _ensure_running(self) -> None:
        """Raise if not running."""
        if not self._is_running:
            raise RuntimeError("Queue manager is not running. Call start() first.")

    # -------------------------------------------------------------------------
    # Queue Management
    # -------------------------------------------------------------------------

    async def create_queue(self, queue_id: str) -> QueueInfo:
        """Create a new queue on demand."""
        async with self._lock:
            if queue_id not in self._known_queues:
                self._known_queues.add(queue_id)
                self._queues[queue_id] = deque()
                self._work_available[queue_id] = asyncio.Event()
                self._queue_created_at[queue_id] = datetime.now(timezone.utc)

                if self._is_running:
                    self._ensure_worker_started(queue_id)

        return await self.get_queue_info(queue_id) or QueueInfo(
            queue_id=queue_id, pending_count=0
        )

    async def get_queue_info(self, queue_id: str) -> Optional[QueueInfo]:
        """Get information about a queue."""
        async with self._lock:
            if queue_id not in self._known_queues:
                return None

            queue = self._queues.get(queue_id, deque())
            current_task = self._current_tasks.get(queue_id)

            return QueueInfo(
                queue_id=queue_id,
                pending_count=len(queue),
                current_task_id=current_task.id if current_task else None,
                is_processing=current_task is not None,
                created_at=self._queue_created_at.get(queue_id),
            )

    async def delete_queue(self, queue_id: str) -> None:
        """Delete a queue and all its tasks."""
        async with self._lock:
            if queue_id not in self._known_queues:
                return

            # Interrupt current task
            current = self._current_tasks.get(queue_id)
            if current:
                running_task = self._running_asyncio_tasks.get(current.id)
                if running_task and not running_task.done():
                    running_task.cancel()

            # Cancel worker
            worker = self._workers.get(queue_id)
            if worker and not worker.done():
                worker.cancel()
                try:
                    await worker
                except asyncio.CancelledError:
                    pass

            # Clean up
            self._queues.pop(queue_id, None)
            self._workers.pop(queue_id, None)
            self._work_available.pop(queue_id, None)
            self._current_tasks.pop(queue_id, None)
            self._queue_created_at.pop(queue_id, None)
            self._known_queues.discard(queue_id)

            # Close subscriptions for this queue
            for sub in self._subscriptions.pop(queue_id, []):
                await sub.close()

    # -------------------------------------------------------------------------
    # Worker Loop
    # -------------------------------------------------------------------------

    def _ensure_worker_started(self, queue_id: str) -> None:
        """Ensure a worker is running for the given queue. Must be called with lock held."""
        if queue_id not in self._workers or self._workers[queue_id].done():
            if queue_id not in self._work_available:
                self._work_available[queue_id] = asyncio.Event()
            self._workers[queue_id] = asyncio.create_task(self._worker_loop(queue_id))

    async def _worker_loop(self, queue_id: str) -> None:
        """
        Background worker that processes tasks for a specific queue.

        Runs continuously, waiting for tasks and processing them one at a time.
        """
        while self._is_running:
            # Wait for work to be available
            work_event = self._work_available.get(queue_id)
            if work_event is None:
                break

            try:
                # Wait with a timeout so we can check shutdown periodically
                await asyncio.wait_for(work_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # Check if we should shut down
            if not self._is_running:
                break

            # Get the next task from the front of the queue
            task: Optional[ClauqBTMTask] = None
            async with self._lock:
                queue = self._queues.get(queue_id)
                if queue:
                    task = queue.popleft() if queue else None
                    if task:
                        self._current_tasks[queue_id] = task

                # Clear the event if queue is empty
                if not queue:
                    work_event.clear()

            if task is None:
                continue

            # Execute the task
            try:
                await self._execute_task(queue_id, task)
            except Exception:
                # Errors are handled in _execute_task, this is just a safety net
                pass
            finally:
                async with self._lock:
                    self._current_tasks.pop(queue_id, None)

    async def _execute_task(self, queue_id: str, task: ClauqBTMTask) -> None:
        """
        Execute a task using the provided executor.
        Handles status updates and completion notifications.
        """
        try:
            # Update to in_progress
            task.status = TaskStatus.in_progress
            task.updated_at = datetime.now(timezone.utc)

            # Emit started event
            await self._emit_event(
                queue_id, TaskEvent.from_task(TaskEventType.STARTED, task)
            )

            # Check if already interrupted before starting
            async with self._lock:
                if task.id in self._completed_tasks:
                    return  # Already interrupted

            # Create the execution task so it can be cancelled
            exec_task = asyncio.create_task(task.execute())
            async with self._lock:
                self._running_asyncio_tasks[task.id] = exec_task

            # Execute using the task's embedded executor
            result = await exec_task

            # Check if interrupted during execution
            async with self._lock:
                if task.id in self._completed_tasks:
                    return  # Was interrupted

            # Success
            task.status = TaskStatus.completed
            task.result = result
            task.updated_at = datetime.now(timezone.utc)

        except asyncio.CancelledError:
            # Task was interrupted via cancel
            async with self._lock:
                if task.id not in self._completed_tasks:
                    task.status = TaskStatus.interrupted
                    task.updated_at = datetime.now(timezone.utc)
            # Don't return here - we need to clean up in finally

        except Exception as exc:
            # Task failed
            task.status = TaskStatus.failed
            task.error = str(exc)
            task.updated_at = datetime.now(timezone.utc)

        finally:
            # Move to completed storage and clean up
            async with self._lock:
                if task.id not in self._completed_tasks:
                    self._completed_tasks[task.id] = task
                self._running_asyncio_tasks.pop(task.id, None)
                self._current_tasks.pop(queue_id, None)

            # Emit completion event
            event_type = {
                TaskStatus.completed: TaskEventType.COMPLETED,
                TaskStatus.failed: TaskEventType.FAILED,
                TaskStatus.interrupted: TaskEventType.INTERRUPTED,
            }.get(task.status, TaskEventType.COMPLETED)

            await self._emit_event(
                queue_id,
                TaskEvent.from_task(event_type, task, error=task.error),
            )

            # Signal completion waiters
            async with self._lock:
                event = self._completion_events.get(task.id)
                if event:
                    event.set()

    async def _emit_event(self, queue_id: str, event: TaskEvent) -> None:
        """Emit an event to all subscribers."""
        async with self._lock:
            for subscription in self._subscriptions.get(queue_id, []):
                subscription.put_nowait(event)

    # -------------------------------------------------------------------------
    # Task Operations
    # -------------------------------------------------------------------------

    async def enqueue(
        self,
        task: ClauqBTMTask,
        executor_name: Optional[str] = None,
    ) -> None:
        """
        Add a task to the back of the queue.

        The task will be picked up by the background worker and executed
        in FIFO order relative to other tasks in the same queue.

        Args:
            task: The task to enqueue (must have executor set)
            executor_name: Ignored for in-memory (executor must be on task)
        """
        self._ensure_running()

        if task.executor is None:
            raise RuntimeError(
                "Task executor not set. Set task.executor before enqueueing."
            )

        queue_id = task.queue_id

        async with self._lock:
            # Initialize queue if needed
            if queue_id not in self._queues:
                self._queues[queue_id] = deque()
                self._work_available[queue_id] = asyncio.Event()
                self._known_queues.add(queue_id)
                self._queue_created_at[queue_id] = datetime.now(timezone.utc)

            # Add task to queue
            self._queues[queue_id].append(task)
            self._completion_events[task.id] = asyncio.Event()

            # Ensure worker is running for this queue
            self._ensure_worker_started(queue_id)

            # Signal that work is available
            self._work_available[queue_id].set()

        # Emit queued event (outside lock)
        await self._emit_event(
            queue_id, TaskEvent.from_task(TaskEventType.QUEUED, task)
        )

    async def get(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        """Get a task by ID. Checks completed tasks, current task, and queue."""
        async with self._lock:
            # Check completed tasks first
            if task_id in self._completed_tasks:
                return self._completed_tasks[task_id]

            # Check currently executing task
            current = self._current_tasks.get(queue_id)
            if current and current.id == task_id:
                return current

            # Check pending queue
            for task in self._queues.get(queue_id, deque()):
                if task.id == task_id:
                    return task

        return None

    async def update(self, queue_id: str, task: ClauqBTMTask) -> None:
        """Update a task in the queue."""
        async with self._lock:
            # Check if it's the current task
            current = self._current_tasks.get(queue_id)
            if current and current.id == task.id:
                # Can't update running tasks
                raise RuntimeError(
                    "Cannot update a running task. Use interrupt() to cancel it."
                )

            # Check pending queue
            queue = self._queues.get(queue_id, deque())
            for idx, existing in enumerate(queue):
                if existing.id == task.id:
                    queue[idx] = task
                    return

            raise RuntimeError(f"Task {task.id} not found in queue {queue_id}")

    async def delete(self, queue_id: str, task_id: str) -> None:
        """Remove a pending task from the queue."""
        async with self._lock:
            # Check if it's the current task
            current = self._current_tasks.get(queue_id)
            if current and current.id == task_id:
                raise RuntimeError(
                    "Cannot delete a running task. Use interrupt() instead."
                )

            queue = self._queues.get(queue_id)
            if queue:
                self._queues[queue_id] = deque(t for t in queue if t.id != task_id)

            # Clean up completion event
            self._completion_events.pop(task_id, None)

    async def list_tasks(self, queue_id: str) -> List[ClauqBTMTask]:
        """List all tasks: current task (if any) + pending queue."""
        async with self._lock:
            result = []

            # Add current task if any
            current = self._current_tasks.get(queue_id)
            if current:
                result.append(current)

            # Add pending tasks
            result.extend(self._queues.get(queue_id, deque()))

            return result

    async def interrupt(
        self, queue_id: str, task_id: str
    ) -> Optional[ClauqBTMTask]:
        """
        Interrupt a running or pending task.

        - If task is currently executing, cancels it
        - If task is pending in queue, removes it
        - Marks the task as interrupted
        """
        task: Optional[ClauqBTMTask] = None
        was_current = False

        async with self._lock:
            # Check if already completed
            if task_id in self._completed_tasks:
                return self._completed_tasks[task_id]

            # Check if it's the currently executing task
            current = self._current_tasks.get(queue_id)
            if current and current.id == task_id:
                task = current
                was_current = True
            else:
                # Check pending queue
                queue = self._queues.get(queue_id, deque())
                for t in queue:
                    if t.id == task_id:
                        task = t
                        break

                # Remove from pending queue
                if task:
                    self._queues[queue_id] = deque(t for t in queue if t.id != task_id)

            if task is None:
                return None

            # Only interrupt if task is pending or in progress
            if task.status not in (TaskStatus.pending, TaskStatus.in_progress):
                return task

            # Mark as interrupted
            task.status = TaskStatus.interrupted
            task.updated_at = datetime.now(timezone.utc)

            # Cancel the running asyncio task if currently executing
            if was_current:
                running_task = self._running_asyncio_tasks.get(task_id)
                if running_task and not running_task.done():
                    running_task.cancel()

            # Store in completed (will be done by _execute_task for current tasks)
            if not was_current:
                self._completed_tasks[task_id] = task

        # Emit event and notify completion (outside lock to avoid deadlock)
        if not was_current:
            await self._emit_event(
                queue_id, TaskEvent.from_task(TaskEventType.INTERRUPTED, task)
            )

            async with self._lock:
                event = self._completion_events.get(task_id)
                if event:
                    event.set()

        return task

    async def wait_for_completion(
        self, queue_id: str, task_id: str, timeout: Optional[float] = None
    ) -> Optional[ClauqBTMTask]:
        """Wait for a task to complete, fail, or be interrupted."""
        # Check if already completed
        async with self._lock:
            if task_id in self._completed_tasks:
                return self._completed_tasks[task_id]
            event = self._completion_events.get(task_id)

        if event is None:
            return await self.get(queue_id, task_id)

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

        return await self.get(queue_id, task_id)

    # -------------------------------------------------------------------------
    # Event Subscription
    # -------------------------------------------------------------------------

    @asynccontextmanager
    async def subscribe(self, queue_id: str) -> AsyncIterator[EventSubscription]:
        """
        Subscribe to events for a specific queue.

        Usage:
            async with await queue_manager.subscribe(queue_id) as subscription:
                async for event in subscription:
                    print(event)
        """
        subscription = AsyncioEventSubscription()

        async with self._lock:
            if queue_id not in self._subscriptions:
                self._subscriptions[queue_id] = []
            self._subscriptions[queue_id].append(subscription)

        try:
            yield subscription
        finally:
            async with self._lock:
                subs = self._subscriptions.get(queue_id, [])
                if subscription in subs:
                    subs.remove(subscription)
            await subscription.close()
