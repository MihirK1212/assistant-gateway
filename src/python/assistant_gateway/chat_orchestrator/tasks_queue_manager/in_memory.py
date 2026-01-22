from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator, Dict, List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import BackgroundAgentTask
from assistant_gateway.chat_orchestrator.tasks_queue_manager.base import (
    TasksQueueManager,
    TaskCompletionEvent,
)
from assistant_gateway.schemas import TaskStatus


class InMemoryTasksQueueManager(TasksQueueManager):
    """
    In-memory queue manager that actively executes tasks.
    
    When a task is enqueued, the queue manager:
    1. Stores the task
    2. Picks it up and executes it via task.execute()
    3. Updates the task status based on execution result
    4. Emits completion notifications
    
    Each task carries its own executor, making tasks self-contained.
    """

    def __init__(self) -> None:
        self._queues: Dict[str, List[BackgroundAgentTask]] = {}
        self._lock = asyncio.Lock()
        
        # Maps task_id -> running asyncio.Task for cancellation
        self._running_tasks: Dict[str, asyncio.Task] = {}
        
        # Event objects for waiting on task completion
        self._completion_events: Dict[str, asyncio.Event] = {}
        
        # Queues for streaming completion events to subscribers
        self._subscriber_queues: Dict[str, List[asyncio.Queue[TaskCompletionEvent]]] = {}
        
        # Store completed/interrupted tasks that were removed from queue
        self._completed_tasks: Dict[str, BackgroundAgentTask] = {}

    async def enqueue(self, queue_id: str, task: BackgroundAgentTask) -> None:
        """
        Add a task to the queue and start its execution.
        The task must have an executor set.
        """
        if task.executor is None:
            raise RuntimeError("Task executor not set. Set task.executor before enqueueing.")
        
        async with self._lock:
            self._queues.setdefault(queue_id, []).append(task)
            self._completion_events[task.id] = asyncio.Event()
        
        # Start task execution
        running_task = asyncio.create_task(
            self._execute_task(queue_id, task)
        )
        
        async with self._lock:
            self._running_tasks[task.id] = running_task

    async def get(self, queue_id: str, task_id: str) -> Optional[BackgroundAgentTask]:
        """Get a task by ID. Checks both active queue and completed tasks."""
        async with self._lock:
            # Check completed tasks first
            if task_id in self._completed_tasks:
                return self._completed_tasks[task_id]
            
            # Check active queue
            for task in self._queues.get(queue_id, []):
                if task.id == task_id:
                    return task
        return None

    async def update(self, queue_id: str, task: BackgroundAgentTask) -> None:
        """Update a task in the queue."""
        async with self._lock:
            tasks = self._queues.get(queue_id, [])
            for idx, existing in enumerate(tasks):
                if existing.id == task.id:
                    tasks[idx] = task
                    break

    async def delete(self, queue_id: str, task_id: str) -> None:
        """Remove a task from the queue."""
        async with self._lock:
            tasks = self._queues.get(queue_id, [])
            self._queues[queue_id] = [t for t in tasks if t.id != task_id]

    async def list(self, queue_id: str) -> List[BackgroundAgentTask]:
        """List all tasks in a queue."""
        async with self._lock:
            return list(self._queues.get(queue_id, []))

    async def interrupt(self, queue_id: str, task_id: str) -> Optional[BackgroundAgentTask]:
        """
        Interrupt a running or pending task.
        - Marks the task as interrupted
        - Cancels the running asyncio task
        - Moves task to completed storage
        """
        async with self._lock:
            # Find the task
            tasks = self._queues.get(queue_id, [])
            task = None
            for t in tasks:
                if t.id == task_id:
                    task = t
                    break
            
            if task is None:
                # Maybe already completed?
                return self._completed_tasks.get(task_id)
            
            # Only interrupt if task is pending or in progress
            if task.status not in (TaskStatus.pending, TaskStatus.in_progress):
                return task
            
            # Mark as interrupted
            task.status = TaskStatus.interrupted
            task.updated_at = datetime.now(timezone.utc)
            
            # Cancel the running asyncio task
            running_task = self._running_tasks.get(task_id)
            if running_task and not running_task.done():
                running_task.cancel()
            
            # Remove from queue and store in completed
            self._queues[queue_id] = [t for t in tasks if t.id != task_id]
            self._completed_tasks[task_id] = task
            self._running_tasks.pop(task_id, None)
        
        # Notify completion (outside lock to avoid deadlock)
        await self._notify_completion(queue_id, task)
        
        return task

    async def wait_for_completion(
        self, queue_id: str, task_id: str, timeout: Optional[float] = None
    ) -> Optional[BackgroundAgentTask]:
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

    async def subscribe_to_completions(
        self, queue_id: str
    ) -> AsyncIterator[TaskCompletionEvent]:
        """Subscribe to task completion events for a specific queue."""
        queue: asyncio.Queue[TaskCompletionEvent] = asyncio.Queue()
        
        async with self._lock:
            self._subscriber_queues.setdefault(queue_id, []).append(queue)
        
        try:
            while True:
                event = await queue.get()
                yield event
        finally:
            async with self._lock:
                queues = self._subscriber_queues.get(queue_id, [])
                if queue in queues:
                    queues.remove(queue)

    async def _execute_task(
        self, queue_id: str, task: BackgroundAgentTask
    ) -> None:
        """
        Execute a task using the provided executor.
        Handles status updates and completion notifications.
        """
        try:
            # Update to in_progress
            task.status = TaskStatus.in_progress
            task.updated_at = datetime.now(timezone.utc)
            await self.update(queue_id, task)
            
            # Check if already interrupted before starting
            async with self._lock:
                if task.id in self._completed_tasks:
                    return  # Already interrupted
            
            # Execute using the task's embedded executor
            result = await task.execute()
            
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
            return
            
        except Exception as exc:
            # Task failed
            task.status = TaskStatus.failed
            task.error = str(exc)
            task.updated_at = datetime.now(timezone.utc)
        
        finally:
            # Move to completed storage and clean up
            async with self._lock:
                if task.id not in self._completed_tasks:  # Don't overwrite if interrupted
                    tasks = self._queues.get(queue_id, [])
                    self._queues[queue_id] = [t for t in tasks if t.id != task.id]
                    self._completed_tasks[task.id] = task
                self._running_tasks.pop(task.id, None)
            
            # Notify completion
            await self._notify_completion(queue_id, task)

    async def _notify_completion(
        self, queue_id: str, task: BackgroundAgentTask
    ) -> None:
        """Notify all subscribers and waiting coroutines that a task has completed."""
        # Signal the completion event
        async with self._lock:
            event = self._completion_events.get(task.id)
            if event:
                event.set()
            
            # Notify all subscribers
            completion_event = TaskCompletionEvent(task)
            for subscriber_queue in self._subscriber_queues.get(queue_id, []):
                try:
                    subscriber_queue.put_nowait(completion_event)
                except asyncio.QueueFull:
                    pass

