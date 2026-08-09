from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)
from uuid import uuid4

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
from assistant_gateway.chat_orchestrator.orchestration.serialization import (
    RunAgentExecutorPayload,
)
from assistant_gateway.chat_orchestrator.orchestration.task_manager import (
    AgentTaskManager,
)
from assistant_gateway.schemas import AgentOutput, Role, UserInput
from fastapi import HTTPException, status

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.queue_manager.subscription import EventSubscription


class ConversationOrchestrator:
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

        # exectutor is registered in the init AgentTaskManager
        self._task_manager = AgentTaskManager(
            clauq_btm=self._config.get_clauq_btm(),
            executor=self._run_agent_for_task,
            post_execution=self._persist_assistant_response,
        )

    async def create_chat(
        self,
        user_id: str,
        agent_name: str,
    ) -> ChatMetadata:
        chat_id = str(uuid4())
        now = datetime.now(timezone.utc)
        chat = ChatMetadata(
            chat_id=chat_id,
            user_id=user_id,
            agent_name=agent_name,
            status=ChatStatus.active,
            created_at=now,
            updated_at=now,
        )
        await self._chat_store.create_chat(chat)
        return chat

    async def get_chat(self, chat_id: str) -> ChatMetadata:
        chat = await self._chat_store.get_chat(chat_id)
        if not chat:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Chat not found")
        return chat

    async def list_interactions(self, chat_id: str) -> List[AgentInteraction]:
        chat = await self.get_chat(chat_id)
        return await self._list_interactions_in_sequence(chat.chat_id)

    async def send_message(
        self,
        chat_id: str,
        content: str,
        run_in_background: bool,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        If run_in_background is True, the task is returned.
        If run_in_background is False, the assistant response is returned.
        """
        async with self._acquire_chat_lock(chat_id):
            chat = await self.get_chat(chat_id)

            await self._create_and_add_user_input_to_chat(chat, content)

            return await self._run_agent_using_all_interactions(
                chat=chat,
                user_context=user_context,
                backend_server_context=backend_server_context,
                run_in_background=run_in_background,
            )

    async def get_task(self, chat_id: str, task_id: str) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        task = await self._task_manager.get_task(task_id)
        if task and task.chat_id == chat_id:
            return task

        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

    async def interrupt_task(self, chat_id: str, task_id: str) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        async with self._acquire_chat_lock(chat_id):
            return await self._interrupt_task_unlocked(chat_id, task_id)

    async def rerun_task(
        self,
        chat_id: str,
        task_id: str,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        async with self._acquire_chat_lock(chat_id):
            task = await self.get_task(chat_id, task_id)

            if task.is_background:
                # TODO: implement background task retry
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Rerunning background tasks is not supported. "
                    "Background task retry will be implemented separately.",
                )

            chat = await self.get_chat(chat_id)

            await self._interrupt_task_unlocked(chat.chat_id, task_id)

            return await self._run_agent_using_all_interactions(
                chat=chat,
                user_context=user_context,
                backend_server_context=backend_server_context,
                run_in_background=False,
            )

    @asynccontextmanager
    async def subscribe_to_events(self, chat_id: str) -> AsyncIterator["EventSubscription"]:
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

    async def _run_agent_using_all_interactions(
        self,
        chat: ChatMetadata,
        user_context: Optional[UserContext] = None,
        backend_server_context: Optional[BackendServerContext] = None,
        run_in_background: bool = False,
    ) -> Tuple[ChatMetadata, Optional[AgentOutput], Optional[AgentTask]]:
        """
        Runs the agent for all interactions.
        Validates that last interaction is a user input
        """
        chat = await self.get_chat(chat.chat_id)

        user_input_interaction = await self._get_last_user_input_interaction(chat)

        payload = RunAgentExecutorPayload(
            chat=chat,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )

        executor_payload = payload.serialize()

        task, assistant_response = await self._task_manager.create_and_execute_task(
            chat=chat,
            interaction_id=user_input_interaction.id,
            executor_payload=executor_payload,
            run_in_background=run_in_background,
        )
        await self._add_task_to_chat(chat, task)
        return chat, assistant_response, task

    async def _run_agent_for_task(self, task: AgentTask, executor_payload: dict[str, Any]) -> AgentOutput:
        """
        Run the agent for a task
        Doesn't care whether the task is synchronous or background
        Just executes the agent for the given task
        """
        payload = RunAgentExecutorPayload.deserialize(executor_payload)

        chat = payload.chat
        user_context = payload.user_context
        backend_server_context = payload.backend_server_context

        interactions = await self._get_interactions_up_to(chat.chat_id, task.interaction_id)

        agent = self._agent_session_manager.get_or_create(
            chat_id=chat.chat_id,
            agent_name=chat.agent_name,
            user_context=user_context,
            backend_server_context=backend_server_context,
        )
        response = await agent.run(interactions=interactions)

        if response.user_input_interaction_id != task.interaction_id:
            raise ValueError(
                f"Agent returned user_input_interaction_id "
                f"'{response.user_input_interaction_id}' but task expected "
                f"'{task.interaction_id}'"
            )

        return response

    async def _persist_assistant_response(self, task: AgentTask, response: AgentOutput) -> None:
        if not response.messages and not response.final_text and not response.steps:
            return None

        await self._chat_store.append_interaction(task.chat_id, response)

    async def _create_and_add_user_input_to_chat(self, chat: ChatMetadata, content: str) -> UserInput:
        user_input = UserInput(
            role=Role.user,
            content=content,
        )
        await self._chat_store.append_interaction(chat.chat_id, user_input)
        await self._update_chat_timestamp(chat)
        return user_input

    async def _interrupt_task_unlocked(
        self, chat_id: str, task_id: str
    ) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
        existing_task = await self.get_task(chat_id, task_id)

        task = await self._task_manager.interrupt_task(existing_task.id)

        chat = await self.get_chat(chat_id)
        if chat.current_task_id == task_id:
            chat.current_task_id = None
            await self._update_chat_timestamp(chat)

        return task

    async def _get_interactions_up_to(self, chat_id: str, interaction_id: str) -> List[AgentInteraction]:
        all_interactions = await self._list_interactions_in_sequence(chat_id)
        result = []
        for interaction in all_interactions:
            result.append(interaction)
            if interaction.id == interaction_id:
                break
        return result

    async def _get_last_user_input_interaction(self, chat: ChatMetadata) -> UserInput:
        interactions = await self._list_interactions_in_sequence(chat.chat_id)

        if not interactions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No interactions found",
            )

        last_interaction = max(interactions, key=lambda x: x.sequence_id)

        if last_interaction.role != Role.user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Inconsistency in interactions: Last interaction is not a UserInput",
            )

        return last_interaction

    async def _list_interactions_in_sequence(self, chat_id: str) -> List[AgentInteraction]:
        """
        List all interactions in the chat.

        Rules:
        1. user inputs should be sorted by created_at in ascending order
        2. each assistant response should have an user_input_interaction_id associated with it.
        3  place the assistant response after the user input that it is associated with.
        4. assign a sequence id / rank to each interaction based on the order of the interactions.
        """
        all_interactions = await self._chat_store.list_interactions(chat_id)

        from assistant_gateway.schemas import Role

        user_inputs = []
        assistant_responses = {}

        for interaction in all_interactions:
            if interaction.role == Role.user:
                user_inputs.append(interaction)
            elif interaction.role == Role.assistant:
                user_input_id = interaction.user_input_interaction_id
                if user_input_id not in assistant_responses:
                    assistant_responses[user_input_id] = []
                assistant_responses[user_input_id].append(interaction)

        user_inputs.sort(key=lambda x: x.created_at)

        ordered_interactions = []
        for user_input in user_inputs:
            ordered_interactions.append(user_input)

            if user_input.id in assistant_responses:
                responses = assistant_responses[user_input.id]
                responses.sort(key=lambda x: x.created_at)
                ordered_interactions.extend(responses)

        for idx, interaction in enumerate(ordered_interactions):
            interaction.sequence_id = idx

        return ordered_interactions

    async def _update_chat_timestamp(self, chat: ChatMetadata) -> None:
        chat.updated_at = datetime.now(timezone.utc)
        await self._chat_store.update_chat(chat)

    async def _add_task_to_chat(self, chat: ChatMetadata, task: AgentTask) -> None:
        # TODO: why is current_task_id being updated immediately? what about queued tasks?
        chat.current_task_id = task.id
        chat.task_ids.append(task.id)
        await self._update_chat_timestamp(chat)

    @asynccontextmanager
    async def _acquire_chat_lock(self, chat_id: str) -> AsyncGenerator[None, None]:
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

    async def start(self) -> None:
        await self._task_manager.start()

    async def stop(self) -> None:
        await self._task_manager.stop()

    @property
    def is_running(self) -> bool:
        return self._task_manager.is_running

    async def __aenter__(self) -> "ConversationOrchestrator":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
