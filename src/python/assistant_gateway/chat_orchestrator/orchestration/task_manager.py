from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, Optional
from uuid import uuid4

from assistant_gateway.chat_orchestrator.core.schemas import (
    BackgroundAgentTask,
    ChatMetadata,
    SynchronousAgentTask,
    UserContext,
    BackendServerContext,
)
from assistant_gateway.chat_orchestrator.tasks_queue_manager import TasksQueueManager
from assistant_gateway.schemas import AgentOutput, TaskStatus


# Type alias for background task executor
BackgroundTaskExecutor = Callable[[BackgroundAgentTask], Awaitable[AgentOutput]]


class TaskManager:
    """
    Manages task creation, queue assignment, and task-to-queue mapping.
    
    This module provides a clean interface for:
    - Creating tasks for sync and background execution modes
    - Determining the queue_id for background tasks
    - Tracking which tasks are scheduled to which queues
    - Managing sync task lifecycle (background tasks are managed by the queue)
    
    Note: Background task execution is handled by the QueueManager.
    The manager just creates and enqueues tasks.
    """

    def __init__(self, queue_manager: TasksQueueManager) -> None:
        self._queue_manager = queue_manager
        self._lock = asyncio.Lock()
        
        # Maps task_id -> queue_id for background tasks
        self._task_queue_mapping: Dict[str, str] = {}
        
        # Maps task_id -> SynchronousAgentTask for sync tasks
        self._sync_tasks: Dict[str, SynchronousAgentTask] = {}

    def get_queue_id_for_chat(
        self, chat: ChatMetadata, interaction_id: str
    ) -> str:
        """
        Determine the queue_id for a given chat and interaction.
        
        Currently uses chat_id as the queue_id to ensure sequential processing
        per chat. This can be customized in the future for more complex routing.
        """
        return chat.chat_id

    async def create_sync_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> SynchronousAgentTask:
        """
        Create a synchronous task for immediate execution.
        Sync tasks are executed directly by the orchestrator, not queued.
        """
        now = datetime.now(timezone.utc)
        task = SynchronousAgentTask(
            id=str(uuid4()),
            chat_id=chat.chat_id,
            interaction_id=interaction_id,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            payload={
                "user_context": user_context.model_dump() if user_context else None,
                "backend_server_context": (
                    backend_server_context.model_dump() if backend_server_context else None
                ),
            },
        )
        
        async with self._lock:
            self._sync_tasks[task.id] = task
        
        return task

    async def create_and_enqueue_background_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor: BackgroundTaskExecutor,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> BackgroundAgentTask:
        """
        Create a background task with embedded executor and enqueue it.
        
        The executor is embedded in the task itself, making the task self-contained.
        The queue manager will call task.execute() to run it.
        """
        queue_id = self.get_queue_id_for_chat(chat, interaction_id)
        now = datetime.now(timezone.utc)
        
        task = BackgroundAgentTask(
            id=str(uuid4()),
            queue_id=queue_id,
            chat_id=chat.chat_id,
            interaction_id=interaction_id,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            executor=executor,
            payload={
                "user_context": user_context.model_dump() if user_context else None,
                "backend_server_context": (
                    backend_server_context.model_dump() if backend_server_context else None
                ),
            },
        )
        
        # Track the mapping before enqueueing
        async with self._lock:
            self._task_queue_mapping[task.id] = queue_id
        
        # Enqueue the task - queue manager will execute it via task.execute()
        await self._queue_manager.enqueue(queue_id, task)
        
        return task

    async def get_sync_task(self, task_id: str) -> Optional[SynchronousAgentTask]:
        """Get a synchronous task by ID."""
        async with self._lock:
            return self._sync_tasks.get(task_id)

    async def update_sync_task(self, task: SynchronousAgentTask) -> None:
        """Update a synchronous task."""
        async with self._lock:
            if task.id in self._sync_tasks:
                self._sync_tasks[task.id] = task

    async def get_background_task(
        self, task_id: str
    ) -> Optional[BackgroundAgentTask]:
        """Get a background task by ID from the queue manager."""
        async with self._lock:
            queue_id = self._task_queue_mapping.get(task_id)
        
        if queue_id is None:
            return None
        
        return await self._queue_manager.get(queue_id, task_id)

    async def interrupt_sync_task(self, task_id: str) -> Optional[SynchronousAgentTask]:
        """
        Interrupt a synchronous task.
        Note: Actual cancellation of running code must be handled by the caller.
        """
        async with self._lock:
            task = self._sync_tasks.get(task_id)
            if task and task.status in (TaskStatus.pending, TaskStatus.in_progress):
                task.status = TaskStatus.interrupted
                task.updated_at = datetime.now(timezone.utc)
                self._sync_tasks[task_id] = task
                return task
            return task

    async def interrupt_background_task(
        self, task_id: str
    ) -> Optional[BackgroundAgentTask]:
        """
        Interrupt a background task.
        The queue manager handles cancellation of the running task.
        """
        async with self._lock:
            queue_id = self._task_queue_mapping.get(task_id)
        
        if queue_id is None:
            return None
        
        # Queue manager handles interrupt + cancellation
        return await self._queue_manager.interrupt(queue_id, task_id)

    async def wait_for_background_task(
        self, task_id: str, timeout: Optional[float] = None
    ) -> Optional[BackgroundAgentTask]:
        """
        Wait for a background task to complete.
        Returns the final task state or None if timeout occurs.
        """
        async with self._lock:
            queue_id = self._task_queue_mapping.get(task_id)
        
        if queue_id is None:
            return None
        
        return await self._queue_manager.wait_for_completion(queue_id, task_id, timeout)

    def get_queue_id_for_task(self, task_id: str) -> Optional[str]:
        """Get the queue_id for a given task_id."""
        return self._task_queue_mapping.get(task_id)

    async def is_task_interrupted(self, task_id: str, is_background: bool) -> bool:
        """
        Check if a task has been interrupted.
        """
        if is_background:
            task = await self.get_background_task(task_id)
        else:
            task = await self.get_sync_task(task_id)
        
        return task is not None and task.is_interrupted()
