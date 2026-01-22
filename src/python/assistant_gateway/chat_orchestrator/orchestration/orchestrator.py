from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Union
from uuid import uuid4
from fastapi import HTTPException, status

from assistant_gateway.schemas import (
    AgentOutput,
    Role,
    UserInput,
    AgentOutput,
    TaskStatus,
)
from assistant_gateway.chat_orchestrator.core.config import (
    GatewayConfig,
)
from assistant_gateway.chat_orchestrator.core.schemas import (
    AgentTask,
    BackgroundAgentTask,
    SynchronousAgentTask,
    BackendServerContext,
    ChatMetadata,
    ChatStatus,
    AgentInteraction,
    UserContext,
)
from assistant_gateway.chat_orchestrator.orchestration.agent_session_manager import (
    AgentSessionManager,
)
from assistant_gateway.chat_orchestrator.orchestration.task_manager import (
    TaskManager,
)


class ConversationOrchestrator:
    """
    Coordinates chat lifecycle, persistence, background processing, and agent
    session reuse.

    Supports interrupt and rerun functionality:
    - Interrupt: Stop a running task and mark it as interrupted
    - Rerun: Create a new task for the same interaction (after interrupting the current one)

    Architecture:
    - Sync tasks: Executed directly by the orchestrator
    - Background tasks: Submitted to queue manager, which executes them
    """

    def __init__(
        self,
        *,
        config: GatewayConfig,
    ) -> None:
        self._config = config
        self._chat_store = self._config.get_chat_store()
        self._queue_manager = self._config.get_queue_manager()
        self._task_scheduler = TaskManager(self._queue_manager)
        self._chat_locks: Dict[str, asyncio.Lock] = {}

        agent_configs = self._config.get_agent_configs()
        if not agent_configs:
            raise ValueError("No agent configs provided")

        self._agent_session_manager = AgentSessionManager(
            agent_configs=agent_configs,
            default_fallback_config=self._config.default_fallback_config,
        )

    async def create_chat(
        self,
        user_id: str,
        agent_name: str,
        metadata: Optional[Dict] = None,
        extra_metadata: Optional[Dict] = None,
    ) -> ChatMetadata:
        """
        Create a new chat.
        """

        chat_id = str(uuid4())
        now = datetime.now(timezone.utc)
        chat = ChatMetadata(
            chat_id=chat_id,
            user_id=user_id,
            agent_name=agent_name,
            status=ChatStatus.active,
            created_at=now,
            updated_at=now,
            metadata=metadata or {},
            extra_metadata=extra_metadata or {},
        )
        await self._chat_store.create_chat(chat)
        return chat

    async def get_chat(self, chat_id: str) -> ChatMetadata:
        """
        Get a chat by ID.
        """

        chat = await self._chat_store.get_chat(chat_id)
        if not chat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Chat not found"
            )
        return chat

    async def list_interactions(self, chat_id: str) -> List[AgentInteraction]:
        """
        List all interactions in a chat.
        """

        chat = await self.get_chat(chat_id)
        interactions = await self._chat_store.list_interactions(chat.chat_id)
        return [
            self._coerce_agent_interaction(interaction) for interaction in interactions
        ]

    async def send_message(
        self,
        chat_id: str,
        content: str,
        run_in_background: bool,
        message_metadata: Optional[Dict] = None,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        Send a message and process it either synchronously or in background.
        If run_in_background is True, the task is returned.
        If run_in_background is False, the assistant response is returned.
        Returns:
            Tuple of (chat_metadata, assistant_response, task)
            - For sync mode: response is populated, task is SynchronousAgentTask
            - For background mode: response is None, task is BackgroundAgentTask

        Raises:
            HTTPException 409: If another send_message operation is already in progress on this chat
        """

        # Acquire lock to prevent concurrent operations on the same chat
        lock = self._get_chat_lock(chat_id)
        if lock.locked():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Another operation is already in progress on this chat",
            )

        async with lock:
            # Step 1: Get the chat
            chat = await self.get_chat(chat_id)

            # Step 2: Create and add user input to chat as the last interaction
            await self._create_and_add_user_input_to_chat(
                chat.chat_id, content, message_metadata
            )

            # Step 3: Run the agent using all the interaction upto the recently added user input
            return await self._run_agent_using_all_interactions(
                chat_id=chat.chat_id,
                user_context=user_context,
                backend_server_context=backend_server_context,
                run_in_background=run_in_background,
            )

    async def get_task(
        self, chat_id: str, task_id: str
    ) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        """
        Get a task by ID.
        """

        task = await self._task_scheduler.get_task(task_id)
        if task and task.chat_id == chat_id:
            return task

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )

    async def interrupt_task(
        self, chat_id: str, task_id: str
    ) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        """
        Interrupt a task.
        """

        # Step 1: Get the task
        existing_task = await self.get_task(chat_id, task_id)

        # Step 2: Interrupt the task
        task = await self._task_scheduler.interrupt_task(existing_task.id)

        # Step 3: Update chat's current task if this was the current task
        chat = await self.get_chat(chat_id)
        if chat.current_task_id == task_id:
            chat.current_task_id = None
            chat.updated_at = datetime.now(timezone.utc)
            await self._chat_store.update_chat(chat)

        return task

    async def rerun_task(
        self,
        chat_id: str,
        task_id: str,
        run_in_background: bool,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        Rerun a task.

        Raises:
            HTTPException 409: If another operation is already in progress on this chat
        """

        # Acquire lock to prevent concurrent operations on the same chat
        lock = self._get_chat_lock(chat_id)
        if lock.locked():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Another operation is already in progress on this chat",
            )

        async with lock:
            # Step 1: Get the chat
            chat = await self.get_chat(chat_id)

            # Step 2: Interrupt the original task
            await self.interrupt_task(chat.chat_id, task_id)

            # Step 3: Run the agent using all the interaction upto the recently added user input
            return await self._run_agent_using_all_interactions(
                chat_id=chat.chat_id,
                user_context=user_context,
                backend_server_context=backend_server_context,
                run_in_background=run_in_background,
            )

    async def _create_and_add_user_input_to_chat(
        self, chat_id: str, content: str, message_metadata: Optional[Dict] = None
    ) -> UserInput:
        chat = await self.get_chat(chat_id)
        user_input = UserInput(
            role=Role.user,
            content=content,
            metadata=message_metadata or {},
        )
        await self._chat_store.append_interaction(chat_id, user_input)
        chat.updated_at = datetime.now(timezone.utc)
        await self._chat_store.update_chat(chat)
        return user_input

    async def _run_agent_using_all_interactions(
        self,
        chat_id: str,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
        run_in_background: bool = False,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        Run the agent for all interactions in the chat.
        """
        chat = await self.get_chat(chat_id)

        # Step 1: Get all interactions in the chat
        interactions = await self._chat_store.list_interactions(chat.chat_id)

        if not interactions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="No interactions found"
            )

        user_input_interaction = sorted(interactions, key=lambda x: x.created_at)[-1]
        if user_input_interaction.role != Role.user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Inconsistency in interactions: Last interaction is not a user input",
            )

        # Step 2: Submit background task to queue
        if run_in_background:
            task = await self._task_scheduler.create_and_enqueue_background_task(
                chat=chat,
                interaction_id=user_input_interaction.id,
                executor=self._execute_background_task,
                user_context=user_context,
                backend_server_context=backend_server_context,
            )
            await self._add_task_to_chat(chat, task)
            return chat, None, task

        # Step 3: Execute sync task directly
        else:
            task, assistant_response = await self._execute_sync_task(
                chat=chat,
                interaction_id=user_input_interaction.id,
                user_context=user_context,
                backend_server_context=backend_server_context,
            )
            await self._add_task_to_chat(chat, task)
            return chat, assistant_response, task

    async def _get_interactions_up_to(
        self, chat_id: str, interaction_id: str
    ) -> List[AgentInteraction]:
        """
        Get all interactions up to and including the specified interaction_id.
        This is used when running/rerunning a task for a specific interaction.
        """
        all_interactions = await self._chat_store.list_interactions(chat_id)
        result = []
        for interaction in all_interactions:
            result.append(interaction)
            if interaction.id == interaction_id:
                break
        return result

    async def _execute_sync_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Tuple[SynchronousAgentTask, Optional[AgentOutput]]:
        """
        Execute a synchronous task directly and return the task and response.

        Sync tasks are not queued - they run inline with the request.
        """
        # Create the task
        task = await self._task_scheduler.create_sync_task(
            chat=chat,
            interaction_id=interaction_id,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )

        # Update task to in_progress
        await self._set_sync_task_status(task, TaskStatus.in_progress)

        try:
            # Check if already interrupted before starting
            if await self._task_scheduler.is_task_interrupted(task.id):
                await self._set_sync_task_status(task, TaskStatus.interrupted)
                return task, None

            # Run the agent
            response = await self._run_agent_for_task(
                chat=chat,
                task=task,
                user_context=user_context,
                backend_server_context=backend_server_context,
            )

            # Check if task was interrupted during execution
            if await self._task_scheduler.is_task_interrupted(task.id):
                await self._set_sync_task_status(task, TaskStatus.interrupted)
                return task, None

            # Persist the response
            await self._persist_assistant_response(
                chat_id=chat.chat_id, response=response
            )

            # Task completed successfully
            await self._set_sync_task_status(task, TaskStatus.completed)
            return task, response

        except Exception as exc:
            task.error = str(exc)
            await self._set_sync_task_status(task, TaskStatus.failed)
            raise

    async def _execute_background_task(self, task: BackgroundAgentTask) -> AgentOutput:
        """
        Executor function called by the queue manager to run a background task.

        This is the callback provided to the queue manager. The queue manager
        handles task status updates, so we just need to run the agent and
        return the result (or raise an exception on failure).
        """
        # Reconstruct context from task payload
        user_context = None
        backend_server_context = None

        if task.payload.get("user_context"):
            user_context = UserContext(**task.payload["user_context"])
        if task.payload.get("backend_server_context"):
            backend_server_context = BackendServerContext(
                **task.payload["backend_server_context"]
            )

        # Get the chat
        chat = await self.get_chat(task.chat_id)

        # Run the agent and return the result
        return await self._run_agent_for_task(
            chat=chat,
            task=task,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )

    async def _run_agent_for_task(
        self,
        chat: ChatMetadata,
        task: AgentTask,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> AgentOutput:
        """
        Run the agent for a task and persist the response.

        Note: Interrupt checking should be done by the caller before/after
        calling this method. For background tasks, the queue manager handles it.
        """
        # Get interactions up to the specified interaction
        interactions = await self._get_interactions_up_to(
            chat.chat_id, task.interaction_id
        )

        agent = self._agent_session_manager.get_or_create(
            chat_id=chat.chat_id,
            agent_name=chat.agent_name,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )
        response = await agent.run(interactions=interactions)

        return response

    async def _persist_assistant_response(
        self, chat_id: str, response: AgentOutput
    ) -> None:
        """Persist the assistant response to the chat store."""
        if not response.messages and not response.final_text and not response.steps:
            return

        stored_agent_response = AgentOutput(
            **response.model_dump()
        )
        await self._chat_store.append_interaction(chat_id, stored_agent_response)

    def _coerce_agent_interaction(
        self, interaction: AgentInteraction
    ) -> AgentInteraction:
        if isinstance(interaction, (UserInput, AgentOutput)):
            return interaction

        if isinstance(interaction, AgentOutput):
            # Backward-compatibility: convert legacy AgentOutput instances persisted before
            # AgentOutput was introduced.
            created_at = getattr(interaction, "created_at", datetime.now(timezone.utc))
            return AgentOutput(
                **interaction.model_dump(),
                metadata=getattr(interaction, "metadata", {}),
            )

        raise ValueError(f"Unsupported interaction type: {type(interaction)}")

    async def _add_task_to_chat(self, chat: ChatMetadata, task: AgentTask) -> None:
        """Add a task reference to the chat metadata."""
        chat.current_task_id = task.id
        chat.task_ids.append(task.id)
        chat.updated_at = datetime.now(timezone.utc)
        await self._chat_store.update_chat(chat)

    async def _set_sync_task_status(self, task: AgentTask, status: TaskStatus) -> None:
        task.status = status
        task.updated_at = datetime.now(timezone.utc)
        await self._task_scheduler.update_sync_task(task)

    def _get_chat_lock(self, chat_id: str) -> asyncio.Lock:
        """Get or create a lock for the specified chat."""
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]
