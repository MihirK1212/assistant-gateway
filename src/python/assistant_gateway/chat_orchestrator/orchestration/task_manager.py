from __future__ import annotations

from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Optional,
    Tuple,
    Union,
)

from assistant_gateway.chat_orchestrator.core.schemas import (
    AgentTask,
    BackgroundAgentTask,
    ChatMetadata,
    SynchronousAgentTask,
)
from assistant_gateway.clauq_btm import (
    BTMTaskManager,
    ClauqBTMTask,
)
from assistant_gateway.schemas import AgentOutput

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm import ClauqBTM
    from assistant_gateway.clauq_btm.queue_manager.subscription import EventSubscription


AgentTaskExecutor = Callable[[AgentTask, Dict[str, Any]], Awaitable[AgentOutput]]
PostExecution = Callable[[AgentTask, AgentOutput], Awaitable[None]]


METADATA_CHAT_ID = "chat_id"
METADATA_INTERACTION_ID = "interaction_id"
METADATA_IS_BACKGROUND = "is_background"

DEFAULT_EXECUTOR_NAME = "orchestrator.run_agent"


class AgentTaskManager:
    """
    bridges between the chat orchestration layer and
    the generic clauq_btm task manager

    transforms agent specifc task types => generic clauq_btm task types
    """

    def __init__(
        self,
        clauq_btm: "ClauqBTM",
        executor: Optional[AgentTaskExecutor] = None,
        post_execution: Optional[PostExecution] = None,
        executor_name: str = DEFAULT_EXECUTOR_NAME,
    ) -> None:
        self._clauq_btm = clauq_btm
        self._executor_name = executor_name
        self._executor = executor
        self._post_execution = post_execution

        # this is where the executors are registered with the clauq_btm instance

        if executor is not None and not clauq_btm.is_setup_complete:
            executor_mapping = self._create_executor_mapping(executor, post_execution, executor_name)
            self._btm_task_manager: BTMTaskManager = clauq_btm.setup(executors=executor_mapping)

        elif not clauq_btm.is_setup_complete:
            self._btm_task_manager = clauq_btm.setup()

        else:
            self._btm_task_manager = clauq_btm.get_task_manager()

    async def create_and_execute_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor_payload: Dict[str, Any],
        run_in_background: bool = False,
    ) -> Tuple[Union[SynchronousAgentTask, BackgroundAgentTask], Optional[AgentOutput]]:
        """
        Create and execute a task, either synchronously or in background.
        Returns only task if it is a background task, otherwise returns the result of the task execution
        """
        queue_id = self._get_queue_id_for_chat(chat)
        metadata = {
            METADATA_CHAT_ID: chat.chat_id,
            METADATA_INTERACTION_ID: interaction_id,
            METADATA_IS_BACKGROUND: run_in_background,
        }

        if run_in_background:
            btm_task = await self._btm_task_manager.create_and_enqueue(
                queue_id=queue_id,
                executor_name=self._executor_name,
                payload=executor_payload,
                metadata=metadata,
            )
            return self._btm_to_agent_task(btm_task), None
        else:
            if self._executor is None:
                raise ValueError(
                    "No executor provided for sync execution. Pass executor at initialization or use background mode."
                )

            btm_executor, btm_post_execution = self._create_btm_wrappers(self._executor, self._post_execution)

            btm_task, result = await self._btm_task_manager.create_and_execute_sync(
                executor=btm_executor,
                post_execution=btm_post_execution,
                payload=executor_payload,
                metadata=metadata,
            )
            return self._btm_to_agent_task(btm_task), result

    async def get_task(self, task_id: str) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        btm_task = await self._btm_task_manager.get_task(task_id)
        if btm_task is None:
            return None
        return self._btm_to_agent_task(btm_task)

    async def interrupt_task(self, task_id: str) -> Optional[Union[SynchronousAgentTask, BackgroundAgentTask]]:
        btm_task = await self._btm_task_manager.interrupt_task(task_id)
        if btm_task is None:
            return None
        return self._btm_to_agent_task(btm_task)

    async def is_task_interrupted(self, task_id: str) -> bool:
        return await self._btm_task_manager.is_task_interrupted(task_id)

    @asynccontextmanager
    async def subscribe(self, chat_id: str) -> AsyncIterator["EventSubscription"]:
        """
        Subscribe to task events for a specific chat.

        Args:
            chat_id: The chat ID to subscribe to

        Yields:
            EventSubscription: An async iterator of TaskEvent objects

        Example:
            async with task_manager.subscribe("chat-123") as subscription:
                async for event in subscription:
                    print(f"Event: {event.event_type} for task {event.task_id}")
        """
        async with self._btm_task_manager.subscribe(queue_id=chat_id) as subscription:
            yield subscription

    def _btm_to_agent_task(self, btm_task: ClauqBTMTask) -> Union[SynchronousAgentTask, BackgroundAgentTask]:
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

    def _create_btm_wrappers(
        self,
        executor: AgentTaskExecutor,
        post_execution: Optional[PostExecution],
    ) -> Tuple[Any, Optional[Any]]:
        """
        Wrappers that bridge between agent task types and generic clauq_btm task types
        """

        async def btm_executor(btm_task: ClauqBTMTask) -> AgentOutput:
            agent_task = self._btm_to_agent_task(btm_task)
            return await executor(agent_task, btm_task.payload)

        btm_post_execution = None
        if post_execution is not None:

            async def btm_post_execution(btm_task: ClauqBTMTask, result: Any) -> None:
                agent_task = self._btm_to_agent_task(btm_task)
                await post_execution(agent_task, result)

        return btm_executor, btm_post_execution

    def _create_executor_mapping(
        self,
        executor: AgentTaskExecutor,
        post_execution: Optional[PostExecution],
        executor_name: str,
    ) -> Dict[str, Any]:
        btm_executor, btm_post_execution = self._create_btm_wrappers(executor, post_execution)

        return {
            executor_name: {
                "executor": btm_executor,
                "post_execution": btm_post_execution,
            }
        }

    def _get_queue_id_for_chat(self, chat: ChatMetadata) -> str:
        return chat.chat_id


    async def start(self) -> None:
        await self._btm_task_manager.start()

    async def stop(self) -> None:
        await self._btm_task_manager.stop()

    async def __aenter__(self) -> "AgentTaskManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
