from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union
from uuid import uuid4

from assistant_gateway.chat_orchestrator.core.schemas import (
    AgentTask,
    BackgroundAgentTask,
    ChatMetadata,
    SynchronousAgentTask,
)
from assistant_gateway.chat_orchestrator.tasks_queue_manager import TasksQueueManager
from assistant_gateway.schemas import AgentOutput, TaskStatus


# Type alias for task executor: (task, **payload) -> AgentOutput
# The executor receives the task and unpacked payload kwargs
TaskExecutor = Callable[..., Awaitable[AgentOutput]]

# Type alias for post-execution callback: (task, response) -> None
PostExecution = Callable[[AgentTask, AgentOutput], Awaitable[None]]


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

    async def create_and_execute_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor: TaskExecutor,
        post_execution: PostExecution,
        executor_payload: Dict[str, Any],
        run_in_background: bool = False,
    ) -> Tuple[Union[SynchronousAgentTask, BackgroundAgentTask], Optional[AgentOutput]]:
        """
        Create and execute a task, either synchronously or in background.

        This is the unified entry point for task creation and execution.
        Based on run_in_background flag, delegates to the appropriate method.

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID this task is for
            executor: Callback to run the agent
            post_execution: Callback to run after successful execution (e.g., persist response)
            executor_payload: Payload to be passed to the executor
            run_in_background: If True, enqueue task for background execution; otherwise execute synchronously

        Returns:
            Tuple of (task, response) where:
            - For sync mode: response is the AgentOutput (or None if interrupted)
            - For background mode: response is always None (task executes asynchronously)
        """
        if run_in_background:
            task = await self._create_and_enqueue_background_task(
                chat=chat,
                interaction_id=interaction_id,
                executor=executor,
                post_execution=post_execution,
                executor_payload=executor_payload,
            )
            return task, None
        else:
            return await self._create_and_execute_sync_task(
                chat=chat,
                interaction_id=interaction_id,
                executor=executor,
                post_execution=post_execution,
                executor_payload=executor_payload,
            )

    async def _create_and_execute_sync_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor: TaskExecutor,
        post_execution: PostExecution,
        executor_payload: Dict[str, Any],
    ) -> Tuple[SynchronousAgentTask, Optional[AgentOutput]]:
        """
        Create and execute a synchronous task directly.

        Sync tasks are not queued - they run inline with the request.
        This method handles the full task lifecycle:
        1. Create the task
        2. Check if interrupted - if so, don't execute
        3. Set status to in_progress
        4. Run the agent via executor
        5. Check if interrupted during execution - if not, call post_execution

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID this task is for
            executor: Callback to run the agent
            post_execution: Callback to run after successful execution (e.g., persist response)
            executor_payload: Payload to be passed to the executor

        Returns:
            Tuple of (task, response) where response is None if interrupted
        """
        # Create and register the task
        now = datetime.now(timezone.utc)
        task = SynchronousAgentTask(
            id=str(uuid4()),
            chat_id=chat.chat_id,
            interaction_id=interaction_id,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            payload=executor_payload,
        )

        async with self._lock:
            self._sync_tasks[task.id] = task

        # Check if already interrupted before starting
        if await self.is_task_interrupted(task.id):
            await self._update_sync_task_status(task, TaskStatus.interrupted)
            return task, None

        # Update task to in_progress
        await self._update_sync_task_status(task, TaskStatus.in_progress)

        try:
            # Run the agent
            response = await executor(task, **task.payload)

            # Check if task was interrupted during execution
            if await self.is_task_interrupted(task.id):
                await self._update_sync_task_status(task, TaskStatus.interrupted)
                return task, None

            # Post-execution (e.g., persist the response)
            await post_execution(task, response)

            # Task completed successfully
            await self._update_sync_task_status(task, TaskStatus.completed)
            return task, response

        except Exception as exc:
            task.error = str(exc)
            await self._update_sync_task_status(task, TaskStatus.failed)
            raise

    async def _create_and_enqueue_background_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor: TaskExecutor,
        post_execution: PostExecution,
        executor_payload: Dict[str, Any],
    ) -> BackgroundAgentTask:
        """
        Create a background task and enqueue it.

        Uses the same executor and post_execution callbacks as sync tasks.
        The task manager creates a wrapper that implements the unified execution flow:
        1. Check if interrupted - if so, don't execute
        2. Run the agent via executor
        3. Check if interrupted during execution - if not, call post_execution

        Note: Status updates (in_progress, completed, failed) are handled by the queue manager.

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID this task is for
            executor: Callback to run the agent (same as sync tasks)
            post_execution: Callback to run after successful execution (same as sync tasks)
            executor_payload: Payload to be passed to the executor

        Returns:
            The created BackgroundAgentTask (already enqueued)
        """
        queue_id = self._get_queue_id_for_chat(chat, interaction_id)
        now = datetime.now(timezone.utc)

        # Create wrapper that implements the unified execution flow
        async def execution_wrapper(task: BackgroundAgentTask) -> Optional[AgentOutput]:
            # Check if interrupted before starting
            if await self.is_task_interrupted(task.id):
                return None

            # Run the agent
            response = await executor(task, **task.payload)

            # Check if interrupted during execution - if not, call post_execution
            if not await self.is_task_interrupted(task.id):
                await post_execution(task, response)

            return response

        task = BackgroundAgentTask(
            id=str(uuid4()),
            queue_id=queue_id,
            chat_id=chat.chat_id,
            interaction_id=interaction_id,
            status=TaskStatus.pending,
            created_at=now,
            updated_at=now,
            executor=execution_wrapper,
            payload=executor_payload,
        )

        # Track the mapping before enqueueing
        async with self._lock:
            self._task_queue_mapping[task.id] = queue_id

        # Enqueue the task - queue manager will execute it via task.execute()
        await self._queue_manager.enqueue(queue_id, task)

        return task

    async def get_task(
        self, task_id: str
    ) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        """
        Get a task by ID, checking both sync and background task stores.

        Args:
            task_id: The task ID to look up

        Returns:
            The task if found, None otherwise
        """
        # Try sync tasks first
        task = await self._get_sync_task(task_id)
        if task is not None:
            return task

        # Fall back to background tasks
        return await self._get_background_task(task_id)

    # -------------------------------------------------------------------------
    # Sync Task Management
    # -------------------------------------------------------------------------
    async def _get_sync_task(self, task_id: str) -> Optional[SynchronousAgentTask]:
        """Get a synchronous task by ID."""
        async with self._lock:
            return self._sync_tasks.get(task_id)

    async def _update_sync_task_status(
        self, task: SynchronousAgentTask, status: TaskStatus
    ) -> None:
        """Update the status of a synchronous task."""
        task.status = status
        task.updated_at = datetime.now(timezone.utc)
        async with self._lock:
            if task.id in self._sync_tasks:
                self._sync_tasks[task.id] = task

    async def _interrupt_sync_task(
        self, task_id: str
    ) -> Optional[SynchronousAgentTask]:
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

    # -------------------------------------------------------------------------
    # Background Task Management
    # -------------------------------------------------------------------------

    async def _get_background_task(
        self, task_id: str
    ) -> Optional[BackgroundAgentTask]:
        """Get a background task by ID from the queue manager."""
        async with self._lock:
            queue_id = self._task_queue_mapping.get(task_id)

        if queue_id is None:
            return None

        return await self._queue_manager.get(queue_id, task_id)

    async def _get_queue_id_for_task(self, task_id: str) -> Optional[str]:
        """Get the queue ID for a background task."""
        async with self._lock:
            return self._task_queue_mapping.get(task_id)

    async def _interrupt_background_task(
        self, task_id: str
    ) -> Optional[BackgroundAgentTask]:
        """
        Interrupt a background task.

        The queue manager handles cancellation of the running task.
        """
        queue_id = await self._get_queue_id_for_task(task_id)
        if queue_id is None:
            return None

        return await self._queue_manager.interrupt(queue_id, task_id)

    # -------------------------------------------------------------------------
    # Public Task Operations
    # -------------------------------------------------------------------------

    async def interrupt_task(
        self, task_id: str
    ) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        """
        Interrupt a task by ID.

        Args:
            task_id: The task ID to interrupt

        Returns:
            The interrupted task, or None if not found
        """
        task = await self.get_task(task_id)
        if task is None:
            return None

        # Already in terminal state - nothing to do
        if task.is_terminal():
            return task

        if task.is_background:
            return await self._interrupt_background_task(task.id)
        else:
            return await self._interrupt_sync_task(task.id)

    async def wait_for_background_task(
        self, task_id: str, timeout: Optional[float] = None
    ) -> Optional[BackgroundAgentTask]:
        """
        Wait for a background task to complete.

        Args:
            task_id: The task ID to wait for
            timeout: Optional timeout in seconds

        Returns:
            The final task state, or None if not found or timeout occurs
        """
        queue_id = await self._get_queue_id_for_task(task_id)
        if queue_id is None:
            return None

        return await self._queue_manager.wait_for_completion(queue_id, task_id, timeout)

    async def is_task_interrupted(self, task_id: str) -> bool:
        """Check if a task has been interrupted."""
        task = await self.get_task(task_id)
        return task is not None and task.is_interrupted()

    # -------------------------------------------------------------------------
    # Queue ID Resolution
    # -------------------------------------------------------------------------

    def _get_queue_id_for_chat(self, chat: ChatMetadata, interaction_id: str) -> str:
        """
        Determine the queue_id for a given chat and interaction.

        Currently uses chat_id as the queue_id to ensure sequential processing
        per chat. This can be customized in the future for more complex routing.

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID (unused, reserved for future routing)

        Returns:
            The queue ID to use for this chat
        """
        # interaction_id is unused but kept for future routing flexibility
        _ = interaction_id
        return chat.chat_id
