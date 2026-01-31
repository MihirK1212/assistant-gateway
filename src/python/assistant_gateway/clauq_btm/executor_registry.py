"""
Executor registry for the Clau-Queue Background Task Manager.

Provides a registry for named executor functions, enabling distributed
execution where executors can't be serialized with tasks.

Executors must be pre-registered at application startup (before starting
Celery workers) to ensure all processes have access to the same executors.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.schemas import ClauqBTMTask


# Type for executor functions: (task) -> result
ExecutorFunc = Callable[["ClauqBTMTask"], Awaitable[Any]]

# Type for post-execution callbacks: (task, result) -> None
PostExecutionFunc = Callable[["ClauqBTMTask", Any], Awaitable[None]]


@dataclass
class ExecutorConfig:
    """
    Configuration for a registered executor.

    Attributes:
        executor: The async function that executes the task
        post_execution: Optional callback after successful execution
    """

    executor: ExecutorFunc
    post_execution: Optional[PostExecutionFunc] = None


class ExecutorRegistry:
    """
    Registry for task executor functions.

    In distributed scenarios (e.g., Celery workers), executors can't be serialized
    with the task. Instead, executors are registered by name and looked up at
    execution time.

    IMPORTANT: Executors must be registered at module import time (or during app
    initialization) so that both API servers and Celery workers have access to
    the same executors.

    Usage:
        registry = ExecutorRegistry()

        # Method 1: Decorator
        @registry.register("my_executor")
        async def my_executor(task: ClauqBTMTask) -> Any:
            return {"result": task.payload["data"]}

        # Method 2: Direct registration with config
        registry.add("my_executor", ExecutorConfig(
            executor=my_executor,
            post_execution=my_callback,
        ))

        # Later, look up by name
        config = registry.get_config("my_executor")
        result = await config.executor(task)
        if config.post_execution:
            await config.post_execution(task, result)
    """

    def __init__(self) -> None:
        self._executors: Dict[str, ExecutorConfig] = {}

    def register(
        self,
        name: str,
        post_execution: Optional[PostExecutionFunc] = None,
    ) -> Callable[[ExecutorFunc], ExecutorFunc]:
        """
        Decorator to register an executor function.

        Args:
            name: Unique name for the executor
            post_execution: Optional callback after successful execution

        Example:
            @registry.register("process_data")
            async def process_data(task: ClauqBTMTask) -> dict:
                return {"processed": True}

            # With post-execution callback
            @registry.register("process_data", post_execution=my_callback)
            async def process_data(task: ClauqBTMTask) -> dict:
                return {"processed": True}
        """

        def decorator(func: ExecutorFunc) -> ExecutorFunc:
            self._executors[name] = ExecutorConfig(
                executor=func,
                post_execution=post_execution,
            )
            return func

        return decorator

    def add(self, name: str, config: ExecutorConfig) -> None:
        """
        Register an executor by name with full configuration.

        Args:
            name: Unique name for the executor
            config: The executor configuration
        """
        self._executors[name] = config

    def get_config(self, name: str) -> Optional[ExecutorConfig]:
        """
        Get the full executor configuration by name.

        Args:
            name: Name of the registered executor

        Returns:
            The ExecutorConfig if found, None otherwise
        """
        return self._executors.get(name)

    def __contains__(self, name: str) -> bool:
        """Check if an executor is registered."""
        return name in self._executors

    def __len__(self) -> int:
        """Return the number of registered executors."""
        return len(self._executors)
