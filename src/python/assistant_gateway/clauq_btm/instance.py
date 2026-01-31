"""
ClauqBTM Instance Manager.

Provides a centralized configuration and factory for creating ClauqBTM components
(Celery app, queue manager, task manager) with a single entry point.

This module simplifies the setup of distributed task execution by managing:
- Celery application configuration
- Queue manager instantiation (CeleryQueueManager)
- Task manager instantiation (BTMTaskManager)

Celery/Redis Requirements:
    - Celery and Redis are required for background task execution
    - If Celery/Redis setup fails, background tasks will raise BackgroundTasksUnavailableError
    - Sync tasks (create_and_execute_sync) always work regardless of Celery setup
    - Use background_setup_error to check if setup failed

Initialization Order (Critical for Distributed Execution):
    1. ClauqBTM instance is created
    2. Executors are registered in executor_registry
    3. Celery app is created
    4. Celery task is created (when queue_manager is created)
    5. CeleryQueueManager is instantiated
    6. BTMTaskManager is instantiated

Recommended Usage (Master Setup - simplest approach):
    from assistant_gateway.clauq_btm import ClauqBTM

    async def my_executor(task):
        return {"result": task.payload}

    async def my_post_execution(task, result):
        print(f"Task {task.id} completed")

    # Single setup call handles everything
    clauq_btm = ClauqBTM(redis_url='redis://localhost:6379/0')
    task_manager = clauq_btm.setup(executors={
        'my_executor': {
            'executor': my_executor,
            'post_execution': my_post_execution,
        },
        'another_executor': {
            'executor': another_fn,
        },
    })

    # For Celery workers, export the celery_app
    celery_app = clauq_btm.celery_app
    # Then run: celery -A shared_setup worker --loglevel=info

Alternative Usage (Manual Registration):
    from assistant_gateway.clauq_btm import ClauqBTM

    clauq_btm = ClauqBTM(redis_url='redis://localhost:6379/0')

    @clauq_btm.executor_registry.register("my_executor")
    async def my_executor(task):
        return {"result": task.payload}

    # Finalize setup after all executors registered
    task_manager = clauq_btm.finalize_setup()
    celery_app = clauq_btm.celery_app
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING, TypedDict

from assistant_gateway.clauq_btm.executor_registry import (
    ExecutorRegistry,
    ExecutorConfig,
)
from assistant_gateway.clauq_btm.task_manager import (
    BTMTaskManager,
    BackgroundTasksUnavailableError,
)

if TYPE_CHECKING:
    from celery import Celery
    from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager
    from assistant_gateway.clauq_btm.schemas import ClauqBTMTask


logger = logging.getLogger(__name__)


# Type aliases for executor functions
ExecutorFunc = Callable[["ClauqBTMTask"], Awaitable[Any]]
PostExecutionFunc = Callable[["ClauqBTMTask", Any], Awaitable[None]]


class ExecutorMapping(TypedDict, total=False):
    """Type definition for executor configuration in setup()."""

    executor: ExecutorFunc  # Required
    post_execution: PostExecutionFunc  # Optional


class SetupState(Enum):
    """State machine for ClauqBTM setup process."""

    NOT_STARTED = "not_started"
    EXECUTORS_REGISTERED = "executors_registered"
    SETUP_COMPLETE = "setup_complete"


class ClauqBTMSetupError(Exception):
    """Raised when ClauqBTM setup operations are called in wrong order."""

    pass


@dataclass
class ClauqBTMConfig:
    """
    Configuration for ClauqBTM instance.

    Attributes:
        redis_url: Redis connection URL for both Celery broker and result backend.
        celery_app_name: Name for the Celery application.
        celery_config: Additional Celery configuration options.
    """

    redis_url: str = "redis://localhost:6379/0"
    celery_app_name: str = "clauq_btm"
    celery_config: dict = field(default_factory=dict)


class ClauqBTM:
    """
    Central manager for ClauqBTM components.

    This class provides a single entry point for configuring and obtaining
    the various ClauqBTM components (Celery app, queue manager, task manager).

    It handles:
    - Creating and configuring the Celery application
    - Creating the CeleryQueueManager with the Celery app
    - Creating the BTMTaskManager with the queue manager
    - Managing lifecycle (start/stop)
    - State validation to ensure correct initialization order

    This class uses CeleryQueueManager for background task execution.

    Setup State Machine:
        NOT_STARTED -> EXECUTORS_REGISTERED -> SETUP_COMPLETE

    Example (Recommended - Master Setup):
        clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

        # Single call to setup everything
        task_manager = clauq.setup(executors={
            'process_data': {'executor': process_fn, 'post_execution': callback_fn},
        })

        # For Celery workers
        celery_app = clauq.celery_app

    Example (Manual Registration):
        clauq = ClauqBTM(redis_url='redis://localhost:6379/0')

        @clauq.executor_registry.register("process_data")
        async def process_data(task):
            return {"processed": task.payload}

        task_manager = clauq.finalize_setup()
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        celery_app_name: str = "clauq_btm",
        celery_config: Optional[dict] = None,
        config: Optional[ClauqBTMConfig] = None,
    ) -> None:
        """
        Initialize ClauqBTM instance.

        Args:
            redis_url: Redis connection URL. Used for Celery broker/backend and
                       queue manager Redis connection.
            celery_app_name: Name for the Celery application.
            celery_config: Additional Celery configuration options.
            config: Alternative way to pass configuration via ClauqBTMConfig object.
                   If provided, other parameters are ignored.
        """
        if config is not None:
            self._config = config
        else:
            self._config = ClauqBTMConfig(
                redis_url=redis_url,
                celery_app_name=celery_app_name,
                celery_config=celery_config or {},
            )

        # Lazily initialized components
        self._celery_app: Optional["Celery"] = None
        self._queue_manager: Optional["CeleryQueueManager"] = None
        self._task_manager: Optional[BTMTaskManager] = None
        self._executor_registry: Optional[ExecutorRegistry] = None

        # Setup state tracking
        self._setup_state: SetupState = SetupState.NOT_STARTED
        self._registered_executor_names: list[str] = []

        # Background task setup error tracking
        # If Celery/Redis setup fails, background tasks will fail
        # but sync tasks will still work
        self._background_setup_error: Optional[Exception] = None

        # Runtime state
        self._is_running = False

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def config(self) -> ClauqBTMConfig:
        """Return the configuration."""
        return self._config

    @property
    def redis_url(self) -> str:
        """Return the Redis URL."""
        return self._config.redis_url

    @property
    def setup_state(self) -> SetupState:
        """Return the current setup state."""
        return self._setup_state

    @property
    def is_setup_complete(self) -> bool:
        """Returns True if setup has been completed."""
        return self._setup_state == SetupState.SETUP_COMPLETE

    @property
    def is_executors_registered(self) -> bool:
        """Returns True if executors have been registered."""
        return self._setup_state in (
            SetupState.EXECUTORS_REGISTERED,
            SetupState.SETUP_COMPLETE,
        )

    @property
    def registered_executor_names(self) -> list[str]:
        """Return list of registered executor names."""
        return list(self._registered_executor_names)

    @property
    def celery_app(self) -> "Celery":
        """
        Return the Celery application instance.

        Creates the Celery app on first access.

        NOTE: This only creates the Celery app, NOT the Celery task.
        The Celery task is registered when queue_manager is created.

        Raises:
            ClauqBTMSetupError: If setup is not complete (for safety).
            BackgroundTasksUnavailableError: If Celery setup failed.
        """
        self._ensure_setup_complete("celery_app")
        if self._background_setup_error is not None:
            raise BackgroundTasksUnavailableError(
                "Celery app is not available because background task setup failed. "
                f"Original error: {self._background_setup_error}. "
                "Sync tasks are still available via create_and_execute_sync()."
            )
        if self._celery_app is None:
            self._celery_app = self._create_celery_app()
        return self._celery_app

    @property
    def executor_registry(self) -> ExecutorRegistry:
        """
        Return the executor registry.

        The registry is shared between this instance and the queue manager.
        Register executors here before calling finalize_setup().

        NOTE: After setup is complete, the registry is read-only conceptually.
        Adding executors after setup will not register them with the Celery task.
        """
        if self._executor_registry is None:
            self._executor_registry = ExecutorRegistry()
        return self._executor_registry

    @property
    def queue_manager(self) -> "CeleryQueueManager":
        """
        Return the CeleryQueueManager instance.

        Raises:
            ClauqBTMSetupError: If setup is not complete.
            BackgroundTasksUnavailableError: If Celery setup failed.
        """
        self._ensure_setup_complete("queue_manager")
        if self._background_setup_error is not None:
            raise BackgroundTasksUnavailableError(
                "Queue manager is not available because background task setup failed. "
                f"Original error: {self._background_setup_error}. "
                "Sync tasks are still available via create_and_execute_sync()."
            )
        if self._queue_manager is None:
            self._queue_manager = self._create_queue_manager()
        return self._queue_manager

    @property
    def is_running(self) -> bool:
        """Returns True if the instance is running."""
        return self._is_running

    @property
    def is_background_tasks_available(self) -> bool:
        """
        Returns True if background tasks are available.

        When False, background tasks will fail but sync tasks will still work.
        This happens when Celery/Redis setup fails.
        """
        return (
            self._setup_state == SetupState.SETUP_COMPLETE
            and self._background_setup_error is None
        )

    @property
    def background_setup_error(self) -> Optional[Exception]:
        """
        Returns the error that occurred during background task setup, if any.

        This is useful for debugging when background tasks are unavailable.
        """
        return self._background_setup_error

    # -------------------------------------------------------------------------
    # State Validation
    # -------------------------------------------------------------------------

    def _ensure_setup_complete(self, operation: str) -> None:
        """
        Validate that setup is complete before allowing an operation.

        Raises:
            ClauqBTMSetupError: If setup is not complete.
        """
        if self._setup_state != SetupState.SETUP_COMPLETE:
            raise ClauqBTMSetupError(
                f"Cannot access '{operation}' before setup is complete. "
                f"Current state: {self._setup_state.value}. "
                "Call setup() or finalize_setup() first."
            )

    def _ensure_not_setup_complete(self, operation: str) -> None:
        """
        Validate that setup is NOT complete (allows modification).

        Raises:
            ClauqBTMSetupError: If setup is already complete.
        """
        if self._setup_state == SetupState.SETUP_COMPLETE:
            raise ClauqBTMSetupError(
                f"Cannot perform '{operation}' after setup is complete. "
                "Executors must be registered before calling setup() or finalize_setup()."
            )

    def _ensure_executors_registered(self, operation: str) -> None:
        """
        Validate that at least one executor has been registered.

        Raises:
            ClauqBTMSetupError: If no executors are registered.
        """
        if not self._registered_executor_names:
            raise ClauqBTMSetupError(
                f"Cannot perform '{operation}' without registered executors. "
                "Register at least one executor before proceeding."
            )

    # -------------------------------------------------------------------------
    # Master Setup Methods
    # -------------------------------------------------------------------------

    def setup(
        self,
        executors: Dict[str, ExecutorMapping],
    ) -> BTMTaskManager:
        """
        Master setup function that initializes all ClauqBTM components.

        This is the recommended way to set up ClauqBTM. It handles the complete
        initialization in the correct order:
        1. Registers all executors in the executor registry
        2. Creates the Celery app
        3. Creates the CeleryQueueManager (which registers the Celery task)
        4. Creates the BTMTaskManager
        5. Returns the task manager for use

        Args:
            executors: Dictionary mapping executor names to their configurations.
                Format: {
                    'executor_name': {
                        'executor': async_function,      # Required
                        'post_execution': async_callback # Optional
                    },
                    ...
                }

        Returns:
            BTMTaskManager: The task manager ready for use.

        Raises:
            ClauqBTMSetupError: If setup has already been completed.
            ValueError: If executors dict is empty or invalid.

        Example:
            async def process_data(task):
                return {"processed": task.payload}

            async def on_complete(task, result):
                print(f"Task {task.id} done")

            clauq_btm = ClauqBTM(redis_url='redis://localhost:6379/0')
            task_manager = clauq_btm.setup(executors={
                'process_data': {
                    'executor': process_data,
                    'post_execution': on_complete,
                },
                'simple_task': {
                    'executor': simple_fn,
                },
            })

            # For Celery workers, export:
            celery_app = clauq_btm.celery_app
        """
        # Validate state
        self._ensure_not_setup_complete("setup")

        # Validate executors
        if not executors:
            raise ValueError(
                "executors dict cannot be empty. "
                "Provide at least one executor mapping."
            )

        # Step 1: Register all executors
        self._register_executors(executors)

        # Step 2-5: Finalize setup and return task manager
        return self.finalize_setup()

    def finalize_setup(self) -> BTMTaskManager:
        """
        Finalize the setup after executors have been registered.

        This method completes the initialization:
        1. Validates that executors are registered
        2. Attempts to create the Celery app and CeleryQueueManager
        3. If Celery/Redis setup fails, stores the error (background tasks will fail)
        4. Creates the BTMTaskManager
        5. Marks setup as complete
        6. Returns the task manager

        If Celery/Redis setup fails:
        - Sync tasks (create_and_execute_sync) will still work
        - Background tasks (create_and_enqueue) will raise BackgroundTasksUnavailableError
        - Check is_background_tasks_available or background_setup_error to detect this

        Use this after registering executors via decorator or register_executor().

        Returns:
            BTMTaskManager: The task manager ready for use.

        Raises:
            ClauqBTMSetupError: If setup is already complete or no executors registered.

        Example:
            clauq_btm = ClauqBTM(redis_url='...')

            @clauq_btm.executor_registry.register("my_executor")
            async def my_executor(task):
                return {"result": task.payload}

            # Must track registration manually when using decorator
            clauq_btm._registered_executor_names.append("my_executor")

            task_manager = clauq_btm.finalize_setup()

            # Check if background tasks are available
            if not clauq_btm.is_background_tasks_available:
                print(f"Warning: Background tasks unavailable: {clauq_btm.background_setup_error}")
        """
        # Validate state
        self._ensure_not_setup_complete("finalize_setup")
        self._ensure_executors_registered("finalize_setup")

        # Try to set up Celery-based distributed queue manager
        if self._queue_manager is None:
            try:
                # Step 1: Create Celery app
                if self._celery_app is None:
                    self._celery_app = self._create_celery_app()

                # Step 2: Create CeleryQueueManager (this registers the Celery task)
                self._queue_manager = self._create_queue_manager()

            except Exception as e:
                # Log the error - background tasks will fail but sync tasks will work
                logger.warning(
                    f"Failed to set up Celery/Redis backend: {e}. "
                    "Background tasks will fail; sync tasks will still work."
                )
                self._background_setup_error = e
                self._celery_app = None  # Clear any partial Celery app
                self._queue_manager = None

        # Step 3: Create task manager
        # If queue manager is None (setup failed), BTMTaskManager handles this gracefully
        # - Sync tasks will still work
        # - Background tasks will raise BackgroundTasksUnavailableError
        if self._task_manager is None:
            self._task_manager = BTMTaskManager(self._queue_manager)

        # Step 4: Mark setup as complete
        self._setup_state = SetupState.SETUP_COMPLETE

        return self._task_manager

    def register_executor(
        self,
        name: str,
        executor: ExecutorFunc,
        post_execution: Optional[PostExecutionFunc] = None,
    ) -> None:
        """
        Register a single executor function.

        This is an alternative to using the decorator pattern. Call this
        before finalize_setup().

        Args:
            name: Unique name for the executor.
            executor: The async executor function.
            post_execution: Optional post-execution callback.

        Raises:
            ClauqBTMSetupError: If setup is already complete.
            ValueError: If executor with same name already registered.

        Example:
            clauq_btm = ClauqBTM(redis_url='...')

            clauq_btm.register_executor(
                name='my_task',
                executor=my_executor_fn,
                post_execution=my_callback,
            )

            task_manager = clauq_btm.finalize_setup()
        """
        self._ensure_not_setup_complete("register_executor")

        if name in self._registered_executor_names:
            raise ValueError(
                f"Executor '{name}' is already registered. "
                "Each executor name must be unique."
            )

        # Register in the executor registry
        self.executor_registry.add(
            name,
            ExecutorConfig(executor=executor, post_execution=post_execution),
        )
        self._registered_executor_names.append(name)

        # Update state if this is the first executor
        if self._setup_state == SetupState.NOT_STARTED:
            self._setup_state = SetupState.EXECUTORS_REGISTERED

    def _register_executors(self, executors: Dict[str, ExecutorMapping]) -> None:
        """
        Register multiple executors from a mapping dict.

        Args:
            executors: Dictionary mapping executor names to configurations.
        """
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

    # -------------------------------------------------------------------------
    # Public Methods
    # -------------------------------------------------------------------------

    def get_task_manager(self) -> BTMTaskManager:
        """
        Return the BTMTaskManager instance.

        Requires setup to be complete. Use setup() or finalize_setup() first.

        Returns:
            The BTMTaskManager instance configured with this ClauqBTM's queue manager.

        Raises:
            ClauqBTMSetupError: If setup is not complete.
        """
        self._ensure_setup_complete("get_task_manager")
        if self._task_manager is None:
            self._task_manager = self._create_task_manager()
        return self._task_manager

    def launch_celery_worker(
        self,
        queues: Optional[list[str]] = None,
        concurrency: int = 1,
        loglevel: str = "INFO",
        **kwargs: Any,
    ) -> None:
        """
        Launch a Celery worker programmatically.

        This is a convenience method for launching a worker in the current process.
        For production, it's recommended to run workers via the command line:
            celery -A your_module worker --loglevel=info

        Requires setup to be complete and Celery to be available.

        Args:
            queues: List of queue names to consume from. If None, consumes from all queues.
            concurrency: Number of worker processes/threads.
            loglevel: Logging level for the worker.
            **kwargs: Additional arguments passed to worker.start()

        Raises:
            ClauqBTMSetupError: If setup is not complete.
            BackgroundTasksUnavailableError: If Celery setup failed.

        Note:
            This method blocks the current thread. Use in a separate process or thread
            if you need to continue execution.
        """
        self._ensure_setup_complete("launch_celery_worker")
        if self._background_setup_error is not None:
            raise BackgroundTasksUnavailableError(
                "Cannot launch Celery worker because Celery setup failed. "
                f"Original error: {self._background_setup_error}."
            )
        worker = self.celery_app.Worker(
            queues=queues,
            concurrency=concurrency,
            loglevel=loglevel,
            **kwargs,
        )
        worker.start()

    def get_setup_status(self) -> Dict[str, Any]:
        """
        Get detailed status of the ClauqBTM setup.

        Returns a dictionary with setup state information useful for debugging.

        Returns:
            Dict containing setup state, registered executors, and component status.
        """
        status = {
            "setup_state": self._setup_state.value,
            "is_setup_complete": self.is_setup_complete,
            "is_executors_registered": self.is_executors_registered,
            "registered_executor_names": list(self._registered_executor_names),
            "executor_count": len(self._registered_executor_names),
            "celery_app_created": self._celery_app is not None,
            "queue_manager_created": self._queue_manager is not None,
            "task_manager_created": self._task_manager is not None,
            "is_running": self._is_running,
            "redis_url": self._config.redis_url,
            "celery_app_name": self._config.celery_app_name,
            "is_background_tasks_available": self.is_background_tasks_available,
        }

        # Include error info if background setup failed
        if self._background_setup_error is not None:
            status["background_setup_error"] = str(self._background_setup_error)
            status["background_setup_error_type"] = type(
                self._background_setup_error
            ).__name__

        return status

    # -------------------------------------------------------------------------
    # Factory Methods
    # -------------------------------------------------------------------------

    def _create_celery_app(self) -> "Celery":
        """Create and configure the Celery application, without any external dependencies or state validation."""
        try:
            from celery import Celery
        except ImportError:
            raise ImportError(
                "celery is required for ClauqBTM. "
                "Install it with: pip install celery[redis]"
            )

        app = Celery(
            self._config.celery_app_name,
            broker=self._config.redis_url,
            backend=self._config.redis_url,
        )

        # Apply default configuration
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

        # Apply user-provided configuration
        if self._config.celery_config:
            app.conf.update(self._config.celery_config)

        return app

    def _create_queue_manager(self) -> "CeleryQueueManager":
        """
        Create the CeleryQueueManager instance.

        NOTE: Uses internal _celery_app to avoid validation during setup.
        """
        from assistant_gateway.clauq_btm.queue_manager import (
            CeleryQueueManager,
        )

        # Use internal variables to avoid validation errors during finalize_setup()
        if self._celery_app is None:
            raise ClauqBTMSetupError(
                "Cannot create queue_manager: Celery app not created. "
                "This is an internal error - celery_app should be created first."
            )

        return CeleryQueueManager(
            celery_app=self._celery_app,
            executor_registry=self.executor_registry,
            redis_url=self._config.redis_url,
        )

    def _create_task_manager(self) -> BTMTaskManager:
        """
        Create the BTMTaskManager instance.

        NOTE: Uses internal _queue_manager to avoid validation during setup.
        If queue_manager is None (Celery setup failed), BTMTaskManager will still
        work for sync tasks but fail for background tasks.
        """
        return BTMTaskManager(self._queue_manager)

    # -------------------------------------------------------------------------
    # Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """
        Start the ClauqBTM instance.

        This starts the queue manager which initializes Redis connections.
        Must be called before using the task manager for background tasks.

        Raises:
            ClauqBTMSetupError: If setup is not complete.
        """
        self._ensure_setup_complete("start")

        if self._is_running:
            return

        # Start the queue manager (uses internal variable, validated above)
        if self._queue_manager is not None:
            await self._queue_manager.start()
        self._is_running = True

    async def stop(self) -> None:
        """
        Stop the ClauqBTM instance gracefully.

        This stops the queue manager and closes Redis connections.
        """
        if not self._is_running:
            return

        self._is_running = False

        # Stop the queue manager
        if self._queue_manager is not None:
            await self._queue_manager.stop()

    async def __aenter__(self) -> "ClauqBTM":
        """Start the instance when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the instance when exiting context."""
        await self.stop()
