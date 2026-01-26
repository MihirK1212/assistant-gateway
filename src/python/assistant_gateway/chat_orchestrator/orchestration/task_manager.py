"""
Task Manager for the Chat Orchestrator.

A thin wrapper around clauq_btm that handles transformation between
agent-specific task types (AgentTask, SynchronousAgentTask, BackgroundAgentTask)
and the generic ClauqBTMTask.

All task management logic (execution, interrupt, wait) is delegated to clauq_btm.
This module only handles:
1. Creating BTM tasks with agent-specific metadata
2. Wrapping BTM tasks in agent task types
3. Reconstructing agent tasks from BTM tasks via get_task
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, Union

from assistant_gateway.chat_orchestrator.core.schemas import (
    AgentTask,
    BackgroundAgentTask,
    ChatMetadata,
    SynchronousAgentTask,
)
from assistant_gateway.clauq_btm import (
    BTMTaskManager,
    ClauqBTMTask,
    QueueManager,
)
from assistant_gateway.schemas import AgentOutput


# Type alias for task executor: (task, executor_payload) -> AgentOutput
AgentTaskExecutor = Callable[[AgentTask, Dict[str, Any]], Awaitable[AgentOutput]]

# Type alias for post-execution callback: (task, response) -> None
PostExecution = Callable[[AgentTask, AgentOutput], Awaitable[None]]


# Metadata keys for agent-specific data
METADATA_CHAT_ID = "chat_id"
METADATA_INTERACTION_ID = "interaction_id"
METADATA_IS_BACKGROUND = "is_background"


class AgentTaskManager:
    """
    Wrapper around BTMTaskManager for agent-specific task handling.

    This class provides a bridge between the chat orchestration layer and
    the generic clauq_btm task manager. It handles:
    - Converting agent-specific data to BTM task metadata
    - Wrapping BTM tasks in SynchronousAgentTask/BackgroundAgentTask
    - Reconstructing agent tasks from BTM tasks

    All actual task management (execution, interrupt, wait) is delegated to clauq_btm.

    Usage:
        queue_manager = InMemoryQueueManager()
        task_manager = TaskManager(queue_manager)

        async with task_manager:
            task, response = await task_manager.create_and_execute_task(
                chat=chat_metadata,
                interaction_id="...",
                executor=my_executor,
                post_execution=my_callback,
                executor_payload={...},
                run_in_background=False,
            )
    """

    def __init__(self, queue_manager: QueueManager) -> None:
        """
        Initialize the TaskManager.

        Args:
            queue_manager: The clauq_btm queue manager for task execution
        """
        self._btm_manager = BTMTaskManager(queue_manager)

    # -------------------------------------------------------------------------
    # Task Creation and Execution
    # -------------------------------------------------------------------------

    async def create_and_execute_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor: AgentTaskExecutor,
        post_execution: PostExecution,
        executor_payload: Dict[str, Any],
        run_in_background: bool = False,
        executor_name: Optional[str] = None,
    ) -> Tuple[Union[SynchronousAgentTask, BackgroundAgentTask], Optional[AgentOutput]]:
        """
        Create and execute a task, either synchronously or in background.

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID this task is for
            executor: Callback to run the agent
            post_execution: Callback after successful execution
            executor_payload: Payload to be passed to the executor
            run_in_background: If True, enqueue for background execution
            executor_name: Name for the executor (required for background mode)

        Returns:
            Tuple of (agent_task, response) where:
            - For sync mode: response is the AgentOutput (or None if interrupted)
            - For background mode: response is always None
        """
        queue_id = self._get_queue_id_for_chat(chat)
        metadata = {
            METADATA_CHAT_ID: chat.chat_id,
            METADATA_INTERACTION_ID: interaction_id,
            METADATA_IS_BACKGROUND: run_in_background,
        }

        # Create BTM executor wrapper that bridges to agent executor
        async def btm_executor(btm_task: ClauqBTMTask) -> AgentOutput:
            agent_task = self._btm_to_agent_task(btm_task)
            return await executor(agent_task, btm_task.payload)

        # Create BTM post-execution wrapper
        async def btm_post_execution(btm_task: ClauqBTMTask, result: Any) -> None:
            agent_task = self._btm_to_agent_task(btm_task)
            await post_execution(agent_task, result)

        if run_in_background:
            if executor_name is None:
                raise ValueError("executor_name is required for background execution")

            btm_task = await self._btm_manager.create_and_enqueue(
                queue_id=queue_id,
                executor=btm_executor,
                executor_name=executor_name,
                post_execution=btm_post_execution,
                payload=executor_payload,
                metadata=metadata,
            )
            return self._btm_to_agent_task(btm_task), None
        else:
            btm_task, result = await self._btm_manager.create_and_execute_sync(
                queue_id=queue_id,
                executor=btm_executor,
                post_execution=btm_post_execution,
                payload=executor_payload,
                metadata=metadata,
            )
            return self._btm_to_agent_task(btm_task), result

    # -------------------------------------------------------------------------
    # Task Operations (delegated to BTM manager)
    # -------------------------------------------------------------------------

    async def get_task(
        self, task_id: str
    ) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        """
        Get a task by ID and reconstruct the agent task from BTM task.

        Args:
            task_id: The task ID to look up

        Returns:
            The agent task if found, None otherwise
        """
        btm_task = await self._btm_manager.get_task(task_id)
        if btm_task is None:
            return None
        return self._btm_to_agent_task(btm_task)

    async def interrupt_task(
        self, task_id: str
    ) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        """
        Interrupt a task by ID.

        Args:
            task_id: The task ID to interrupt

        Returns:
            The interrupted agent task, or None if not found
        """
        btm_task = await self._btm_manager.interrupt_task(task_id)
        if btm_task is None:
            return None
        return self._btm_to_agent_task(btm_task)

    async def wait_for_background_task(
        self, task_id: str, timeout: Optional[float] = None
    ) -> Optional[BackgroundAgentTask]:
        """
        Wait for a background task to complete.

        Args:
            task_id: The task ID to wait for
            timeout: Optional timeout in seconds

        Returns:
            The final agent task state, or None if not found or timeout
        """
        btm_task = await self._btm_manager.wait_for_completion(task_id, timeout)
        if btm_task is None:
            return None
        agent_task = self._btm_to_agent_task(btm_task)
        if isinstance(agent_task, BackgroundAgentTask):
            return agent_task
        return None

    async def is_task_interrupted(self, task_id: str) -> bool:
        """Check if a task has been interrupted."""
        return await self._btm_manager.is_task_interrupted(task_id)

    # -------------------------------------------------------------------------
    # Conversion Helpers
    # -------------------------------------------------------------------------

    def _btm_to_agent_task(
        self, btm_task: ClauqBTMTask
    ) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        """
        Convert a ClauqBTMTask to an agent-specific task type.

        Uses metadata to determine task type and populate agent-specific fields.
        """
        chat_id = btm_task.metadata.get(METADATA_CHAT_ID, "")
        interaction_id = btm_task.metadata.get(METADATA_INTERACTION_ID, "")
        is_background = btm_task.metadata.get(METADATA_IS_BACKGROUND, False)

        if is_background:
            return BackgroundAgentTask(
                id=btm_task.id,
                queue_id=btm_task.queue_id,
                chat_id=chat_id,
                interaction_id=interaction_id,
                status=btm_task.status,
                created_at=btm_task.created_at,
                updated_at=btm_task.updated_at,
                payload=btm_task.payload,
                result=btm_task.result,
                error=btm_task.error,
            )
        else:
            return SynchronousAgentTask(
                id=btm_task.id,
                chat_id=chat_id,
                interaction_id=interaction_id,
                status=btm_task.status,
                created_at=btm_task.created_at,
                updated_at=btm_task.updated_at,
                payload=btm_task.payload,
                result=btm_task.result,
                error=btm_task.error,
            )

    def _get_queue_id_for_chat(self, chat: ChatMetadata) -> str:
        """
        Determine the queue_id for a given chat.

        Uses chat_id as queue_id to ensure sequential processing per chat.
        """
        return chat.chat_id

    # -------------------------------------------------------------------------
    # Lifecycle Management (delegated to BTM manager)
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the task manager."""
        await self._btm_manager.start()

    async def stop(self) -> None:
        """Stop the task manager gracefully."""
        await self._btm_manager.stop()

    @property
    def is_running(self) -> bool:
        """Returns True if the task manager is running."""
        return self._btm_manager.is_running

    async def __aenter__(self) -> "AgentTaskManager":
        """Start the task manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the task manager when exiting context."""
        await self.stop()
