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

For distributed execution (Celery), executors must be registered at initialization
time so that both API servers and workers have access to the same executors.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional, Tuple, TYPE_CHECKING, Union

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


# Type alias for task executor: (task, executor_payload) -> AgentOutput
AgentTaskExecutor = Callable[[AgentTask, Dict[str, Any]], Awaitable[AgentOutput]]

# Type alias for post-execution callback: (task, response) -> None
PostExecution = Callable[[AgentTask, AgentOutput], Awaitable[None]]


# Metadata keys for agent-specific data
METADATA_CHAT_ID = "chat_id"
METADATA_INTERACTION_ID = "interaction_id"
METADATA_IS_BACKGROUND = "is_background"

# Default executor name for agent tasks
DEFAULT_EXECUTOR_NAME = "orchestrator.run_agent"


class AgentTaskManager:
    """
    Wrapper around BTMTaskManager for agent-specific task handling.

    This class provides a bridge between the chat orchestration layer and
    the generic clauq_btm task manager. It handles:
    - Converting agent-specific data to BTM task metadata
    - Wrapping BTM tasks in SynchronousAgentTask/BackgroundAgentTask
    - Reconstructing agent tasks from BTM tasks
    - Registering executors for distributed execution

    For distributed execution (Celery):
    - Pass executor and post_execution at initialization
    - These are registered in the ClauqBTM's executor registry
    - Both API servers and workers must create the same AgentTaskManager setup

    Usage:
        from assistant_gateway.clauq_btm import ClauqBTM

        # Create ClauqBTM instance
        clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

        # Create task manager with executor registration
        task_manager = AgentTaskManager(
            clauq_btm=clauq,
            executor=my_executor,
            post_execution=my_callback,
        )

        async with task_manager:
            task, response = await task_manager.create_and_execute_task(
                chat=chat_metadata,
                interaction_id="...",
                executor_payload={...},
                run_in_background=False,
            )
    """

    def __init__(
        self,
        clauq_btm: "ClauqBTM",
        executor: Optional[AgentTaskExecutor] = None,
        post_execution: Optional[PostExecution] = None,
        executor_name: str = DEFAULT_EXECUTOR_NAME,
    ) -> None:
        """
        Initialize the TaskManager.

        Args:
            clauq_btm: The ClauqBTM instance for task management.
                      Provides access to executor_registry and task_manager.
            executor: Optional executor function for background tasks.
                     If provided, will be registered via ClauqBTM.setup().
            post_execution: Optional callback after successful execution.
                           If provided, will be registered with the executor.
            executor_name: Name to register the executor under (default: "orchestrator.run_agent")

        Initialization Flow:
            - If executor provided AND setup not complete:
              Uses ClauqBTM.setup() with executor mapping (cleanest approach)
            - If no executor AND setup not complete:
              Calls finalize_setup() (executors registered elsewhere)
            - If setup already complete:
              Gets existing task manager

        This ensures executors are registered before the Celery task is created,
        so workers have access to all executors when the task executes.
        """
        self._clauq_btm = clauq_btm
        self._executor_name = executor_name
        self._executor = executor
        self._post_execution = post_execution

        # Initialize BTM manager based on setup state
        if executor is not None and not clauq_btm.is_setup_complete:
            # Use setup() with executor mapping - cleanest approach
            executor_mapping = self._create_executor_mapping(
                executor, post_execution, executor_name
            )
            self._btm_manager: BTMTaskManager = clauq_btm.setup(
                executors=executor_mapping
            )

        elif not clauq_btm.is_setup_complete:
            # No executor provided but setup not complete
            # Just finalize (executors should have been registered elsewhere)
            self._btm_manager = clauq_btm.finalize_setup()

        else:
            # Setup already complete - get the existing task manager
            # Executor should have been registered during prior setup
            self._btm_manager = clauq_btm.get_task_manager()

    # -------------------------------------------------------------------------
    # Task Creation and Execution
    # -------------------------------------------------------------------------

    async def create_and_execute_task(
        self,
        chat: ChatMetadata,
        interaction_id: str,
        executor_payload: Dict[str, Any],
        run_in_background: bool = False,
    ) -> Tuple[Union[SynchronousAgentTask, BackgroundAgentTask], Optional[AgentOutput]]:
        """
        Create and execute a task, either synchronously or in background.

        For sync mode: executor is called directly (must be provided at init or here)
        For background mode: executor is looked up by name from registry

        Args:
            chat: The chat metadata
            interaction_id: The interaction ID this task is for
            executor_payload: Payload to be passed to the executor
            run_in_background: If True, enqueue for background execution

        Returns:
            Tuple of (agent_task, response) where:
            - For sync mode: response is the AgentOutput (or None if interrupted)
            - For background mode: response is always None

        Raises:
            ValueError: If no executor is available for the requested mode
        """
        queue_id = self._get_queue_id_for_chat(chat)
        metadata = {
            METADATA_CHAT_ID: chat.chat_id,
            METADATA_INTERACTION_ID: interaction_id,
            METADATA_IS_BACKGROUND: run_in_background,
        }

        print('[BGDEBUG] create_and_execute_task called queue_id:', queue_id, 'interaction_id:', interaction_id, 'run_in_background:', run_in_background)

        if run_in_background:
            # Background mode: use registered executor by name
            btm_task = await self._btm_manager.create_and_enqueue(
                queue_id=queue_id,
                executor_name=self._executor_name,
                payload=executor_payload,
                metadata=metadata,
            )
            return self._btm_to_agent_task(btm_task), None
        else:
            # Sync mode: pass executor directly
            if self._executor is None:
                raise ValueError(
                    "No executor provided for sync execution. "
                    "Pass executor at initialization or use background mode."
                )

            # Create BTM wrappers for sync execution
            btm_executor, btm_post_execution = self._create_btm_wrappers(
                self._executor, self._post_execution
            )

            btm_task, result = await self._btm_manager.create_and_execute_sync(
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
    # Clauq BTM helper methods
    # -------------------------------------------------------------------------

    def _create_btm_wrappers(
        self,
        executor: AgentTaskExecutor,
        post_execution: Optional[PostExecution],
    ) -> Tuple[Any, Optional[Any]]:
        """
        Create BTM executor and post-execution wrapper functions.

        These wrappers bridge between AgentTask and ClauqBTMTask by:
        1. Converting ClauqBTMTask to AgentTask
        2. Calling the agent executor/post_execution with AgentTask
        3. Returning the result

        Args:
            executor: The agent executor function to wrap.
            post_execution: Optional post-execution callback to wrap.

        Returns:
            Tuple of (btm_executor, btm_post_execution) wrapper functions.
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
        """
        Create executor mapping dict for ClauqBTM.setup().

        Returns:
            Dict in format: {executor_name: {'executor': fn, 'post_execution': fn}}
        """
        btm_executor, btm_post_execution = self._create_btm_wrappers(
            executor, post_execution
        )

        return {
            executor_name: {
                "executor": btm_executor,
                "post_execution": btm_post_execution,
            }
        }

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
