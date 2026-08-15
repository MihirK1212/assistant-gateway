from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional, TypedDict

from assistant_gateway.clauq_btm.executor_registry import (
    ExecutorConfig,
    ExecutorRegistry,
)
from assistant_gateway.clauq_btm.task_manager import (
    BackgroundTasksUnavailableError,
    BTMTaskManager,
)

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager
    from assistant_gateway.clauq_btm.schemas import ClauqBTMTask
    from celery import Celery


logger = logging.getLogger(__name__)


ExecutorFunc = Callable[["ClauqBTMTask"], Awaitable[Any]]
PostExecutionFunc = Callable[["ClauqBTMTask", Any], Awaitable[None]]


# {<executor_name>: {'executor': ExecutorFunc, 'post_execution': PostExecutionFunc}}
class ExecutorMapping(TypedDict, total=False):
    executor: ExecutorFunc
    post_execution: PostExecutionFunc


class SetupState(Enum):
    NOT_STARTED = "not_started"
    EXECUTORS_REGISTERED = "executors_registered"
    SETUP_COMPLETE = "setup_complete"


class ClauqBTMSetupError(Exception):
    pass


@dataclass
class ClauqBTMConfig:
    redis_url: str
    celery_app_name: str
    additional_celery_config: dict = field(default_factory=dict)

    # Optional fixed set of queue names declared at startup
    default_queues: List[str] = field(default_factory=list)


class ClauqBTM:
    def __init__(
        self,
        redis_url: str,
        celery_app_name: str,
        additional_celery_config: Optional[dict] = None,
        default_queues: Optional[List[str]] = None,
    ) -> None:
        self._config = ClauqBTMConfig(
            redis_url=redis_url,
            celery_app_name=celery_app_name,
            additional_celery_config=additional_celery_config or {},
            default_queues=default_queues or [],
        )

        self._executor_registry: ExecutorRegistry = ExecutorRegistry()

        self._celery_app: Optional["Celery"] = None
        self._queue_manager: Optional["CeleryQueueManager"] = None
        self._task_manager: Optional[BTMTaskManager] = None

        self._setup_state: SetupState = SetupState.NOT_STARTED
        self._background_setup_error: Optional[Exception] = None
        self._is_running = False

    @property
    def config(self) -> ClauqBTMConfig:
        return self._config

    @property
    def redis_url(self) -> str:
        return self._config.redis_url

    @property
    def setup_state(self) -> SetupState:
        return self._setup_state

    @property
    def is_setup_complete(self) -> bool:
        return self._setup_state == SetupState.SETUP_COMPLETE

    @property
    def celery_app(self) -> "Celery":
        self._ensure_setup_complete("celery_app")
        if self._background_setup_error is not None:
            raise BackgroundTasksUnavailableError(
                "Celery app is not available because background task setup failed. "
                f"Original error: {self._background_setup_error}. "
                "Sync tasks are still available via create_and_execute_sync()."
            )
        assert self._celery_app is not None, "Internal error: celery_app is None after successful setup"
        return self._celery_app

    @property
    def executor_registry(self) -> ExecutorRegistry:
        return self._executor_registry

    @property
    def queue_manager(self) -> "CeleryQueueManager":
        self._ensure_setup_complete("queue_manager")
        if self._background_setup_error is not None:
            raise BackgroundTasksUnavailableError(
                "Queue manager is not available because background task setup failed. "
                f"Original error: {self._background_setup_error}. "
                "Sync tasks are still available via create_and_execute_sync()."
            )
        assert self._queue_manager is not None, "Internal error: queue_manager is None after successful setup"
        return self._queue_manager

    @property
    def is_running(self) -> bool:
        return self._is_running

    @property
    def is_background_tasks_available(self) -> bool:
        return self._setup_state == SetupState.SETUP_COMPLETE and self._background_setup_error is None

    @property
    def background_setup_error(self) -> Optional[Exception]:
        return self._background_setup_error

    def setup(
        self,
        executors: Optional[Dict[str, ExecutorMapping]] = None,
    ) -> BTMTaskManager:
        """
        Setup the ClauqBTM instance and return a BTMTaskManager.

        if executors provided => clear any previously registered executors and register the given ones
        if executors not provided => finalize with whatever executors were already registered 
                                    (via clauq_btm.register_executor() or @clauq_btm.executor_registry.register())
        """
        self._ensure_not_setup_complete("setup")

        if executors is not None:
            if not executors:
                raise ValueError("executors dict cannot be empty. ")
            self._executor_registry.clear()
            self._register_executors(executors)

        self._ensure_executors_registered("setup")

        celery_app = None
        queue_manager = None
        background_error = None

        try:
            celery_app = ClauqBTM._create_celery_app(
                self._config.celery_app_name,
                self._config.redis_url,
                self._config.default_queues,
                self._config.additional_celery_config,
            )
            queue_manager = ClauqBTM._create_queue_manager(
                celery_app, self._executor_registry, self._config.redis_url, self._config.default_queues
            )
        except Exception as e:
            logger.warning(
                f"Failed to set up Celery/Redis backend: {e}. Background tasks will fail; sync tasks will still work."
            )
            background_error = e
            celery_app = None
            queue_manager = None

        task_manager = BTMTaskManager(queue_manager)

        self._celery_app = celery_app
        self._queue_manager = queue_manager
        self._task_manager = task_manager
        self._background_setup_error = background_error
        self._setup_state = SetupState.SETUP_COMPLETE

        return task_manager

    def get_task_manager(self) -> BTMTaskManager:
        self._ensure_setup_complete("get_task_manager")
        assert self._task_manager is not None, "Internal error: task_manager is None after setup"
        return self._task_manager

    def register_executor(
        self,
        name: str,
        executor: ExecutorFunc,
        post_execution: Optional[PostExecutionFunc] = None,
    ) -> None:
        self._ensure_not_setup_complete("register_executor")

        if name in self._executor_registry:
            raise ValueError(f"Executor '{name}' is already registered. Each executor name must be unique.")

        self._executor_registry.add(
            name,
            ExecutorConfig(executor=executor, post_execution=post_execution),
        )

        if self._setup_state == SetupState.NOT_STARTED:
            self._setup_state = SetupState.EXECUTORS_REGISTERED

    def _register_executors(self, executors: Dict[str, ExecutorMapping]) -> None:
        for name, config in executors.items():
            if "executor" not in config:
                raise ValueError(
                    f"Executor '{name}' is missing required 'executor' function. "
                    "Each executor config must have an 'executor' key."
                )

            self.register_executor(
                name=name,
                executor=config["executor"],
                post_execution=config.get("post_execution"),
            )

    def _ensure_setup_complete(self, operation: str) -> None:
        if self._setup_state != SetupState.SETUP_COMPLETE:
            raise ClauqBTMSetupError(
                f"Cannot access '{operation}' before setup is complete. "
                f"Current state: {self._setup_state.value}. "
                "Call setup() first."
            )

    def _ensure_not_setup_complete(self, operation: str) -> None:
        if self._setup_state == SetupState.SETUP_COMPLETE:
            raise ClauqBTMSetupError(
                f"Cannot perform '{operation}' after setup is complete. "
                "Executors must be registered before calling setup()."
            )

    def _ensure_executors_registered(self, operation: str) -> None:
        if len(self._executor_registry) == 0:
            raise ClauqBTMSetupError(
                f"Cannot perform '{operation}' without registered executors. "
                "Register at least one executor before proceeding."
            )

    @staticmethod
    def _create_celery_app(
        celery_app_name: str,
        redis_url: str,
        default_queues: Optional[List[str]] = None,
        celery_config: Optional[dict] = None,
    ) -> "Celery":
        try:
            from celery import Celery
        except ImportError:
            raise ImportError("celery is required for ClauqBTM. Install it with: pip install celery[redis]")

        app = Celery(
            celery_app_name,
            broker=redis_url,
            backend=redis_url,
        )

        app.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
            timezone="UTC",
            enable_utc=True,
            task_track_started=True,
            task_acks_late=True,
            worker_prefetch_multiplier=1,
        )

        if default_queues:
            try:
                from kombu import Queue as KombuQueue
            except ImportError:
                raise ImportError("kombu is required for default_queues. Install it with: pip install kombu")

            app.conf.task_queues = [KombuQueue(name) for name in default_queues]
            app.conf.task_default_queue = default_queues[0]

        if celery_config:
            app.conf.update(celery_config)

        return app

    @staticmethod
    def _create_queue_manager(
        celery_app: "Celery",
        executor_registry: ExecutorRegistry,
        redis_url: str,
        default_queues: List[str],
    ) -> "CeleryQueueManager":
        from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager

        return CeleryQueueManager(
            celery_app=celery_app,
            executor_registry=executor_registry,
            redis_url=redis_url,
            default_queues=default_queues,
        )

    async def start(self) -> None:
        self._ensure_setup_complete("start")

        if self._is_running:
            return

        if self._queue_manager is not None:
            await self._queue_manager.start()
        self._is_running = True

    async def stop(self) -> None:
        if not self._is_running:
            return

        self._is_running = False

        if self._queue_manager is not None:
            await self._queue_manager.stop()

    async def __aenter__(self) -> "ClauqBTM":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
