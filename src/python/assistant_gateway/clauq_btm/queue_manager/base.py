"""
Base abstractions for the Queue Manager.

The Queue Manager provides a worker/consumer pattern for executing tasks:
- Maintains FIFO queues per queue_id
- Background workers process tasks sequentially
- Supports task lifecycle management (query, interrupt, delete)
- Emits completion events for subscribers
"""

from __future__ import annotations

import abc
from dataclasses import dataclass
from datetime import datetime
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    List,
    Optional,
)

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask
from assistant_gateway.clauq_btm.events import TaskEvent


# -----------------------------------------------------------------------------
# Queue Info
# -----------------------------------------------------------------------------


@dataclass
class QueueInfo:
    """Information about a task queue."""

    queue_id: str
    pending_count: int
    current_task_id: Optional[str] = None
    is_processing: bool = False
    created_at: Optional[datetime] = None


# -----------------------------------------------------------------------------
# Subscription
# -----------------------------------------------------------------------------


class EventSubscription(abc.ABC):
    """
    Abstract subscription to task events.

    Implementations should support async iteration:
        async for event in subscription:
            print(event)
    """

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[TaskEvent]:
        """Iterate over events."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the subscription and release resources."""
        ...

    async def __aenter__(self) -> "EventSubscription":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


# -----------------------------------------------------------------------------
# Queue Manager Interface
# -----------------------------------------------------------------------------


class QueueManager(abc.ABC):
    """
    Abstract base class for task queue management.

    The queue manager provides:
    - On-demand queue creation
    - FIFO task execution per queue
    - Task lifecycle management (query, interrupt, delete)
    - Completion event subscriptions

    Lifecycle:
    - Call start() to begin processing tasks
    - Call stop() for graceful shutdown
    - Can be used as async context manager:

        async with queue_manager:
            await queue_manager.enqueue(task)

    Implementations:
    - InMemoryQueueManager: Single-process, uses asyncio
    - CeleryQueueManager: Distributed, uses Celery + Redis
    """

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    @abc.abstractmethod
    async def start(self) -> None:
        """
        Start the queue manager's background processing.

        This must be called before enqueueing tasks. The queue manager will
        begin processing tasks from all queues.
        """
        ...

    @abc.abstractmethod
    async def stop(self) -> None:
        """
        Stop the queue manager gracefully.

        - Stops accepting new tasks
        - Waits for currently running tasks to complete (or cancels them)
        - Shuts down all worker loops
        """
        ...

    @property
    @abc.abstractmethod
    def is_running(self) -> bool:
        """Returns True if the queue manager is running."""
        ...

    async def __aenter__(self) -> "QueueManager":
        """Start the queue manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the queue manager when exiting context."""
        await self.stop()

    # -------------------------------------------------------------------------
    # Executor Registry (for distributed execution)
    # -------------------------------------------------------------------------

    def set_executor_registry(self, registry: Any) -> None:
        """
        Set the executor registry for distributed execution.

        This is called by BTMTaskManager to share its registry with the queue manager.
        Override in subclasses that need registry access (e.g., CeleryQueueManager).

        Args:
            registry: The executor registry to use
        """
        pass  # Default implementation does nothing (in-memory doesn't need it)

    # -------------------------------------------------------------------------
    # Queue Management
    # -------------------------------------------------------------------------

    async def create_queue(self, queue_id: str) -> QueueInfo:
        """
        Create a new queue on demand.

        This is optional - queues are created automatically on first enqueue.
        Use this to pre-create queues or get queue info.

        Returns QueueInfo for the created/existing queue.
        """
        raise NotImplementedError("create_queue not supported by this implementation")

    async def get_queue_info(self, queue_id: str) -> Optional[QueueInfo]:
        """
        Get information about a queue.

        Returns None if the queue doesn't exist.
        """
        raise NotImplementedError(
            "get_queue_info not supported by this implementation"
        )

    async def delete_queue(self, queue_id: str) -> None:
        """
        Delete a queue and all its tasks.

        Running tasks will be interrupted.
        """
        raise NotImplementedError("delete_queue not supported by this implementation")

    # -------------------------------------------------------------------------
    # Task Operations
    # -------------------------------------------------------------------------

    @abc.abstractmethod
    async def enqueue(
        self,
        task: ClauqBTMTask,
        executor_name: Optional[str] = None,
    ) -> None:
        """
        Add a task to the back of its queue.

        The task will be picked up by a background worker and executed
        in FIFO order relative to other tasks in the same queue.

        The queue_id is taken from task.queue_id.

        Args:
            task: The task to enqueue (must have executor set OR provide executor_name)
            executor_name: Name of registered executor (for distributed execution)

        Raises:
            RuntimeError: If queue manager is not running or executor not set
        """
        raise NotImplementedError("enqueue not supported by this implementation")

    @abc.abstractmethod
    async def get(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        """
        Get a task by ID from a specific queue.

        Returns the task if found, None otherwise.
        """
        raise NotImplementedError("get not supported by this implementation")

    @abc.abstractmethod
    async def update(self, queue_id: str, task: ClauqBTMTask) -> None:
        """
        Update a task in the queue.

        Only pending tasks can be updated.
        """
        raise NotImplementedError("update not supported by this implementation")

    @abc.abstractmethod
    async def delete(self, queue_id: str, task_id: str) -> None:
        """
        Remove a pending task from the queue.

        Cannot delete running tasks - use interrupt() instead.
        """
        raise NotImplementedError("delete not supported by this implementation")

    @abc.abstractmethod
    async def list_tasks(self, queue_id: str) -> List[ClauqBTMTask]:
        """
        List all tasks in a queue.

        Returns tasks in order: current running task (if any) followed by
        pending tasks in FIFO order.
        """
        raise NotImplementedError("list_tasks not supported by this implementation")

    @abc.abstractmethod
    async def interrupt(
        self, queue_id: str, task_id: str
    ) -> Optional[ClauqBTMTask]:
        """
        Interrupt a running or pending task.

        - If task is currently executing, cancels it
        - If task is pending in queue, removes it
        - Marks the task as interrupted

        Returns the updated task if found, None otherwise.
        """
        raise NotImplementedError("interrupt not supported by this implementation")

    @abc.abstractmethod
    async def wait_for_completion(
        self, queue_id: str, task_id: str, timeout: Optional[float] = None
    ) -> Optional[ClauqBTMTask]:
        """
        Wait for a task to complete, fail, or be interrupted.

        Returns the final task state or None if timeout occurs.
        """
        raise NotImplementedError(
            "wait_for_completion not supported by this implementation"
        )

    # -------------------------------------------------------------------------
    # Event Subscription
    # -------------------------------------------------------------------------

    @abc.abstractmethod
    async def subscribe(self, queue_id: str) -> AsyncContextManager[EventSubscription]:
        """
        Subscribe to events for a specific queue.

        Returns an async context manager yielding an EventSubscription.

        Usage:
            async with await queue_manager.subscribe(queue_id) as subscription:
                async for event in subscription:
                    if event.event_type == TaskEventType.COMPLETED:
                        print(f"Task {event.task_id} completed!")
        """
        raise NotImplementedError("subscribe not supported by this implementation")

    async def subscribe_all(self) -> AsyncContextManager[EventSubscription]:
        """
        Subscribe to events from all queues.

        Default implementation raises NotImplementedError.
        Subclasses may override to support this.
        """
        raise NotImplementedError("subscribe_all not supported by this implementation")
