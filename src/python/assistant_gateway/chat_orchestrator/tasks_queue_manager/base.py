"""
Base abstractions for the Task Queue Manager.

The Task Queue Manager provides a worker/consumer pattern for executing tasks:
- Maintains FIFO queues per queue_id
- Background workers process tasks sequentially
- Supports task lifecycle management (query, interrupt, delete)
- Emits completion events for subscribers
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    TypeVar,
)

from assistant_gateway.chat_orchestrator.core.schemas import BackgroundAgentTask
from assistant_gateway.schemas import TaskStatus


# -----------------------------------------------------------------------------
# Events
# -----------------------------------------------------------------------------


class TaskEventType(str, Enum):
    """Types of task events emitted by the queue manager."""

    QUEUED = "queued"
    STARTED = "started"
    COMPLETED = "completed"
    FAILED = "failed"
    INTERRUPTED = "interrupted"
    PROGRESS = "progress"


@dataclass
class TaskEvent:
    """Event emitted during task lifecycle."""

    event_type: TaskEventType
    task_id: str
    queue_id: str
    status: TaskStatus
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    task: Optional[BackgroundAgentTask] = None
    error: Optional[str] = None
    progress: Optional[Dict[str, Any]] = None

    @classmethod
    def from_task(
        cls,
        event_type: TaskEventType,
        task: BackgroundAgentTask,
        error: Optional[str] = None,
        progress: Optional[Dict[str, Any]] = None,
    ) -> "TaskEvent":
        """Create an event from a task."""
        return cls(
            event_type=event_type,
            task_id=task.id,
            queue_id=task.queue_id,
            status=task.status,
            task=task,
            error=error,
            progress=progress,
        )


# -----------------------------------------------------------------------------
# Executor Registry
# -----------------------------------------------------------------------------

# Type for executor functions
ExecutorFunc = Callable[[BackgroundAgentTask], Awaitable[Any]]
T = TypeVar("T")


class ExecutorRegistry:
    """
    Registry for task executor functions.

    In distributed scenarios (e.g., Celery workers), executors can't be serialized
    with the task. Instead, executors are registered by name and looked up at
    execution time.

    Usage:
        registry = ExecutorRegistry()

        @registry.register("my_executor")
        async def my_executor(task: BackgroundAgentTask) -> AgentOutput:
            ...

        # Later, look up by name
        executor = registry.get("my_executor")
    """

    def __init__(self) -> None:
        self._executors: Dict[str, ExecutorFunc] = {}

    def register(self, name: str) -> Callable[[ExecutorFunc], ExecutorFunc]:
        """Decorator to register an executor function."""

        def decorator(func: ExecutorFunc) -> ExecutorFunc:
            self._executors[name] = func
            return func

        return decorator

    def add(self, name: str, executor: ExecutorFunc) -> None:
        """Register an executor function by name."""
        self._executors[name] = executor

    def get(self, name: str) -> Optional[ExecutorFunc]:
        """Get an executor by name."""
        return self._executors.get(name)

    def get_or_raise(self, name: str) -> ExecutorFunc:
        """Get an executor by name, raising if not found."""
        executor = self._executors.get(name)
        if executor is None:
            raise KeyError(f"Executor '{name}' not found in registry")
        return executor

    def list_names(self) -> List[str]:
        """List all registered executor names."""
        return list(self._executors.keys())


# Global executor registry (can be used by workers)
default_executor_registry = ExecutorRegistry()


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
# Task Queue Manager Interface
# -----------------------------------------------------------------------------


class TasksQueueManager(abc.ABC):
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
            await queue_manager.enqueue(...)

    Implementations:
    - InMemoryTasksQueueManager: Single-process, uses asyncio
    - CeleryTasksQueueManager: Distributed, uses Celery + Redis
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

    async def __aenter__(self) -> "TasksQueueManager":
        """Start the queue manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the queue manager when exiting context."""
        await self.stop()

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
        raise NotImplementedError("get_queue_info not supported by this implementation")

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
        queue_id: str,
        task: BackgroundAgentTask,
        executor_name: Optional[str] = None,
    ) -> None:
        """
        Add a task to the back of the queue.

        The task will be picked up by a background worker and executed
        in FIFO order relative to other tasks in the same queue.

        Args:
            queue_id: The queue to add the task to
            task: The task to enqueue (must have executor set OR provide executor_name)
            executor_name: Name of registered executor (for distributed execution)

        Raises:
            RuntimeError: If queue manager is not running or executor not set
        """
        raise NotImplementedError("enqueue not supported by this implementation")

    @abc.abstractmethod
    async def get(self, queue_id: str, task_id: str) -> Optional[BackgroundAgentTask]:
        """
        Get a task by ID from a specific queue.

        Returns the task if found, None otherwise.
        """
        raise NotImplementedError("get not supported by this implementation")

    @abc.abstractmethod
    async def update(self, queue_id: str, task: BackgroundAgentTask) -> None:
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
    async def list(self, queue_id: str) -> List[BackgroundAgentTask]:
        """
        List all tasks in a queue.

        Returns tasks in order: current running task (if any) followed by
        pending tasks in FIFO order.
        """
        raise NotImplementedError("list not supported by this implementation")

    @abc.abstractmethod
    async def interrupt(
        self, queue_id: str, task_id: str
    ) -> Optional[BackgroundAgentTask]:
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
    ) -> Optional[BackgroundAgentTask]:
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
