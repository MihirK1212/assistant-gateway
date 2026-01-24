from __future__ import annotations

from typing import List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import BackgroundAgentTask


class TaskCompletionEvent:
    """Event emitted when a task completes, fails, or is interrupted."""
    
    def __init__(self, task: BackgroundAgentTask) -> None:
        self.task = task
        self.task_id = task.id
        self.queue_id = task.queue_id
        self.status = task.status


class TasksQueueManager:
    """
    Abstraction for queue operations and task execution.
    
    The queue manager is responsible for:
    - Storing tasks in queues
    - Picking up pending tasks and executing them (via task.execute())
    - Managing task lifecycle (status updates, interruption)
    - Emitting completion notifications
    
    Each BackgroundAgentTask carries its own executor, making tasks self-contained.
    The queue manager simply calls task.execute() to run tasks.
    """

    async def enqueue(self, queue_id: str, task: BackgroundAgentTask) -> None:
        """
        Add a task to the queue and start its execution.
        The task must have an executor set. The queue manager will call task.execute().
        """
        raise NotImplementedError

    async def get(self, queue_id: str, task_id: str) -> Optional[BackgroundAgentTask]:
        """Get a task by ID from a specific queue."""
        raise NotImplementedError

    async def update(self, queue_id: str, task: BackgroundAgentTask) -> None:
        """Update a task in the queue."""
        raise NotImplementedError

    async def delete(self, queue_id: str, task_id: str) -> None:
        """Remove a task from the queue."""
        raise NotImplementedError

    async def list(self, queue_id: str) -> List[BackgroundAgentTask]:
        """List all tasks in a queue."""
        raise NotImplementedError

    async def interrupt(self, queue_id: str, task_id: str) -> Optional[BackgroundAgentTask]:
        """
        Interrupt a running or pending task.
        - Marks the task as interrupted
        - Cancels the running asyncio task if any
        - Removes the task from the queue
        Returns the updated task if found, None otherwise.
        """
        raise NotImplementedError

    async def wait_for_completion(
        self, queue_id: str, task_id: str, timeout: Optional[float] = None
    ) -> Optional[BackgroundAgentTask]:
        """
        Wait for a task to complete, fail, or be interrupted.
        Returns the final task state or None if timeout occurs.
        """
        raise NotImplementedError

