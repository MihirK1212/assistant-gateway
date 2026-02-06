"""
Conversation Orchestrator for the Chat System.

Coordinates chat lifecycle, persistence, background processing, and agent
session reuse. Supports both synchronous and background execution modes.

For Celery/distributed execution:
- The same module that creates the orchestrator must be imported by workers
- Executors are registered at initialization time
- Workers will have access to the same registered executors
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import (
    Any,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    TYPE_CHECKING,
    Union,
)
from uuid import uuid4

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.queue_manager.subscription import EventSubscription

from fastapi import HTTPException, status

from assistant_gateway.chat_orchestrator.core.config import GatewayConfig
from assistant_gateway.chat_orchestrator.core.schemas import (
    AgentInteraction,
    AgentTask,
    BackendServerContext,
    BackgroundAgentTask,
    ChatMetadata,
    ChatStatus,
    SynchronousAgentTask,
    UserContext,
)
from assistant_gateway.chat_orchestrator.orchestration.agent_session_manager import (
    AgentSessionManager,
)
from assistant_gateway.chat_orchestrator.orchestration.task_manager import (
    AgentTaskManager,
)
from assistant_gateway.chat_orchestrator.orchestration.serialization import (
    RunAgentExecutorPayload,
)
from assistant_gateway.schemas import AgentOutput, Role, UserInput


class ConversationOrchestrator:
    """
    Coordinates chat lifecycle, persistence, background processing, and agent
    session reuse.

    Supports interrupt and rerun functionality:
    - Interrupt: Stop a running task and mark it as interrupted
    - Rerun: Create a new task for the same interaction (after interrupting the current one)

    Architecture:
    - Sync tasks: Executed directly by the orchestrator
    - Background tasks: Submitted to task manager's queue for execution

    For Celery/distributed execution:
    - Executors are registered at initialization (in __init__)
    - The same orchestrator setup must be used by workers
    - Import the module that creates the orchestrator in your worker config

    Lifecycle:
    - Call start() to start background task processing
    - Call stop() for graceful shutdown
    - Can be used as async context manager:

        async with ConversationOrchestrator(config=config) as orchestrator:
            await orchestrator.send_message(...)
    """

    def __init__(
        self,
        *,
        config: GatewayConfig,
    ) -> None:
        self._config = config
        self._chat_store = self._config.get_chat_store()
        self._chat_locks: Dict[str, asyncio.Lock] = {}

        agent_configs = self._config.get_agent_configs()
        if not agent_configs:
            raise ValueError("No agent configs provided")

        self._agent_session_manager = AgentSessionManager(
            agent_configs=agent_configs,
            default_fallback_config=self._config.default_fallback_config,
        )

        # Create task manager with executor registration
        # This registers executors at init time for distributed execution
        self._task_manager = AgentTaskManager(
            clauq_btm=self._config.get_clauq_btm(),
            executor=self._run_agent_for_task,
            post_execution=self._persist_assistant_response,
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
        return await self._list_interactions_in_sequence(chat.chat_id)

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
        async with self._acquire_chat_lock(chat_id):
            # Step 1: Get the chat
            chat = await self.get_chat(chat_id)

            # Step 2: Create and add user input to chat as the last interaction
            await self._create_and_add_user_input_to_chat(
                chat, content, message_metadata
            )

            # Step 3: Run the agent using all the interaction upto the recently added user input
            return await self._run_agent_using_all_interactions(
                chat=chat,
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

        task = await self._task_manager.get_task(task_id)
        if task and task.chat_id == chat_id:
            return task

        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Task not found"
        )

    async def interrupt_task(
        self, chat_id: str, task_id: str
    ) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        """
        Interrupt a task and clear it from the chat's current task if applicable.

        Args:
            chat_id: The chat ID
            task_id: The task ID to interrupt

        Returns:
            The interrupted task

        Raises:
            HTTPException 404: If task not found
        """
        # Validate task exists and belongs to this chat
        existing_task = await self.get_task(chat_id, task_id)

        # Interrupt the task
        task = await self._task_manager.interrupt_task(existing_task.id)

        # Clear current task reference if this was the active task
        chat = await self.get_chat(chat_id)
        if chat.current_task_id == task_id:
            chat.current_task_id = None
            await self._update_chat_timestamp(chat)

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
        async with self._acquire_chat_lock(chat_id):
            # Step 1: Get the chat
            chat = await self.get_chat(chat_id)

            # Step 2: Interrupt the original task
            await self.interrupt_task(chat.chat_id, task_id)

            # Step 3: Run the agent using all the interaction upto the recently added user input
            return await self._run_agent_using_all_interactions(
                chat=chat,
                user_context=user_context,
                backend_server_context=backend_server_context,
                run_in_background=run_in_background,
            )

    @asynccontextmanager
    async def subscribe_to_events(
        self, chat_id: str
    ) -> AsyncIterator["EventSubscription"]:
        """
        Subscribe to task events for a specific chat.

        This allows real-time streaming of task lifecycle events (queued, started,
        completed, failed, interrupted, progress) for all tasks in the given chat.

        Args:
            chat_id: The chat ID to subscribe to

        Yields:
            EventSubscription: An async iterator of TaskEvent objects

        Example:
            async with orchestrator.subscribe_to_events("chat-123") as subscription:
                async for event in subscription:
                    print(f"Event: {event.event_type} for task {event.task_id}")
        """
        async with self._task_manager.subscribe(chat_id) as subscription:
            yield subscription

    async def _create_and_add_user_input_to_chat(
        self, chat: ChatMetadata, content: str, message_metadata: Optional[Dict] = None
    ) -> UserInput:
        """
        Create a user input and append it to the chat's interactions.

        Args:
            chat: The chat to add the input to
            content: The message content
            message_metadata: Optional metadata for the message

        Returns:
            The created UserInput
        """
        user_input = UserInput(
            role=Role.user,
            content=content,
            metadata=message_metadata or {},
        )
        await self._chat_store.append_interaction(chat.chat_id, user_input)
        await self._update_chat_timestamp(chat)
        return user_input

    async def _run_agent_using_all_interactions(
        self,
        chat: ChatMetadata,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
        run_in_background: bool = False,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        Run the agent for all interactions in the chat.

        Validates that the last interaction is a user input, then creates and
        executes/enqueues a task based on the run_in_background flag.

        Args:
            chat: The chat metadata
            user_context: Optional user context for the agent
            backend_server_context: Optional backend server context
            run_in_background: If True, enqueue for background execution

        Returns:
            Tuple of (chat, response, task)
        """
        # Re-fetch chat to ensure we have the latest state after adding user input
        chat = await self.get_chat(chat.chat_id)

        # Step 1: Get the last user input interaction
        user_input_interaction = await self._get_last_user_input_interaction(chat)

        # Step 2: Build the executor payload
        payload = RunAgentExecutorPayload(
            chat=chat,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )

        # Step 3: Serialize payload
        executor_payload = payload.serialize()

        # Step 4: Create and execute task (sync or background based on flag)
        task, assistant_response = await self._task_manager.create_and_execute_task(
            chat=chat,
            interaction_id=user_input_interaction.id,
            executor_payload=executor_payload,
            run_in_background=run_in_background,
        )
        await self._add_task_to_chat(chat, task)
        return chat, assistant_response, task

    async def _run_agent_for_task(
        self, task: AgentTask, executor_payload: dict[str, Any]
    ) -> AgentOutput:
        """
        Run the agent for a task.

        This method handles both sync and background execution:
        - Sync mode: Receives Pydantic objects directly via kwargs
        - Background/Celery mode: Receives serialized dicts in task.payload

        Note: Interrupt checking is handled by the queue manager/BTM layer.
        """
        payload = RunAgentExecutorPayload.deserialize(executor_payload)

        chat = payload.chat
        user_context = payload.user_context
        backend_server_context = payload.backend_server_context

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
        self, task: AgentTask, response: AgentOutput
    ) -> None:
        """Persist the assistant response to the chat store."""
        # Skip persistence if response has no content
        if not response.messages and not response.final_text and not response.steps:
            return None

        # the user input interaction id is the interaction id of the user input interaction that triggered the task
        user_input_interaction_id = task.interaction_id
        if not user_input_interaction_id:
            raise ValueError(
                "There must be a last user input interaction id for the task"
            )

        chat = await self.get_chat(task.chat_id)
        stored_agent_response = AgentOutput(**response.model_dump())
        stored_agent_response.user_input_interaction_id = user_input_interaction_id
        await self._chat_store.append_interaction(chat.chat_id, stored_agent_response)

    async def _get_interactions_up_to(
        self, chat_id: str, interaction_id: str
    ) -> List[AgentInteraction]:
        """
        Get all interactions up to and including the specified interaction_id.
        This is used when running/rerunning a task for a specific interaction.
        """
        all_interactions = await self._list_interactions_in_sequence(chat_id)
        result = []
        for interaction in all_interactions:
            result.append(interaction)
            if interaction.id == interaction_id:
                break
        return result

    async def _get_last_user_input_interaction(self, chat: ChatMetadata) -> UserInput:
        """
        Get the last interaction from the chat, validating it's a user input.

        Raises:
            HTTPException 400: If no interactions found or last interaction is not a UserInput
        """
        interactions = await self._list_interactions_in_sequence(chat.chat_id)

        if not interactions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No interactions found",
            )

        # Sort by created_at to ensure we get the chronologically last interaction
        # (store implementations may not guarantee order)
        last_interaction = max(interactions, key=lambda x: x.sequence_id)

        if last_interaction.role != Role.user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Inconsistency in interactions: Last interaction is not a UserInput",
            )

        return last_interaction

    async def _list_interactions_in_sequence(
        self, chat_id: str
    ) -> List[AgentInteraction]:
        """
        List all interactions in the chat.

        Sort them by the following order:
        1. User inputs should be sorted by created_at in ascending order
        2. Each assistant response should have an user_input_interaction_id associated with it. Place the assistant response after the user input that it is associated with.S
        3. Assign a sequence id / rank to each interaction based on the order of the interactions.
        """
        # Get all interactions from the store
        all_interactions = await self._chat_store.list_interactions(chat_id)

        # Separate user inputs and assistant responses
        from assistant_gateway.schemas import Role

        user_inputs = []
        assistant_responses = {}  # Map user_input_id -> list of assistant responses

        for interaction in all_interactions:
            if interaction.role == Role.user:
                user_inputs.append(interaction)
            elif interaction.role == Role.assistant:
                # Group assistant responses by their associated user input
                user_input_id = interaction.user_input_interaction_id
                if user_input_id not in assistant_responses:
                    assistant_responses[user_input_id] = []
                assistant_responses[user_input_id].append(interaction)

        # Sort user inputs by created_at in ascending order
        user_inputs.sort(key=lambda x: x.created_at)

        # Build the final ordered list
        ordered_interactions = []
        for user_input in user_inputs:
            # Add the user input
            ordered_interactions.append(user_input)

            # Add any associated assistant responses (sorted by created_at for consistency)
            if user_input.id in assistant_responses:
                responses = assistant_responses[user_input.id]
                responses.sort(key=lambda x: x.created_at)
                ordered_interactions.extend(responses)

        # Assign sequence IDs
        for idx, interaction in enumerate(ordered_interactions):
            interaction.sequence_id = idx

        return ordered_interactions

    # -------------------------------------------------------------------------
    # Chat Update Helpers
    # -------------------------------------------------------------------------

    async def _update_chat_timestamp(self, chat: ChatMetadata) -> None:
        """Update the chat's updated_at timestamp."""
        chat.updated_at = datetime.now(timezone.utc)
        await self._chat_store.update_chat(chat)

    async def _add_task_to_chat(self, chat: ChatMetadata, task: AgentTask) -> None:
        """Add a task reference to the chat metadata."""
        chat.current_task_id = task.id
        chat.task_ids.append(task.id)
        await self._update_chat_timestamp(chat)

    @asynccontextmanager
    async def _acquire_chat_lock(self, chat_id: str) -> AsyncGenerator[None, None]:
        """
        Acquire lock for chat, raising 409 if already locked.

        Uses timeout=0 to atomically try-acquire the lock, avoiding the race
        condition of checking locked() then acquiring separately.
        """
        # TODO: centralize all the locking to a distributed lock manager instead of in-memory

        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        lock = self._chat_locks[chat_id]

        try:
            await asyncio.wait_for(lock.acquire(), timeout=1)
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Another operation is already in progress on this chat",
            )
        try:
            yield
        finally:
            lock.release()

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the orchestrator and its task manager."""
        await self._task_manager.start()

    async def stop(self) -> None:
        """Stop the orchestrator gracefully."""
        await self._task_manager.stop()

    @property
    def is_running(self) -> bool:
        """Returns True if the orchestrator is running."""
        return self._task_manager.is_running

    async def __aenter__(self) -> "ConversationOrchestrator":
        """Start the orchestrator when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the orchestrator when exiting context."""
        await self.stop()
