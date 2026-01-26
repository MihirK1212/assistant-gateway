"""
Executor registry for the Clau-Queue Background Task Manager.

Provides a registry for named executor functions, enabling distributed
execution where executors can't be serialized with tasks.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask


# Type for executor functions
ExecutorFunc = Callable[[ClauqBTMTask], Awaitable[Any]]


class ExecutorRegistry:
    """
    Registry for task executor functions.

    In distributed scenarios (e.g., Celery workers), executors can't be serialized
    with the task. Instead, executors are registered by name and looked up at
    execution time.

    Usage:
        registry = ExecutorRegistry()

        @registry.register("my_executor")
        async def my_executor(task: ClauqBTMTask) -> Any:
            return {"result": task.payload["data"]}

        # Later, look up by name
        executor = registry.get("my_executor")
        result = await executor(task)
    """

    def __init__(self) -> None:
        self._executors: Dict[str, ExecutorFunc] = {}

    def register(self, name: str) -> Callable[[ExecutorFunc], ExecutorFunc]:
        """
        Decorator to register an executor function.

        Args:
            name: Unique name for the executor

        Example:
            @registry.register("process_data")
            async def process_data(task: ClauqBTMTask) -> dict:
                return {"processed": True}
        """

        def decorator(func: ExecutorFunc) -> ExecutorFunc:
            self._executors[name] = func
            return func

        return decorator

    def add(self, name: str, executor: ExecutorFunc) -> None:
        """
        Register an executor function by name.

        Args:
            name: Unique name for the executor
            executor: The async function that executes tasks
        """
        self._executors[name] = executor

    def get(self, name: str) -> Optional[ExecutorFunc]:
        """
        Get an executor by name.

        Args:
            name: Name of the registered executor

        Returns:
            The executor function if found, None otherwise
        """
        return self._executors.get(name)

    def get_or_raise(self, name: str) -> ExecutorFunc:
        """
        Get an executor by name, raising if not found.

        Args:
            name: Name of the registered executor

        Returns:
            The executor function

        Raises:
            KeyError: If executor is not found
        """
        executor = self._executors.get(name)
        if executor is None:
            raise KeyError(f"Executor '{name}' not found in registry")
        return executor

    def list_names(self) -> List[str]:
        """List all registered executor names."""
        return list(self._executors.keys())

    def remove(self, name: str) -> bool:
        """
        Remove an executor from the registry.

        Args:
            name: Name of the executor to remove

        Returns:
            True if removed, False if not found
        """
        if name in self._executors:
            del self._executors[name]
            return True
        return False

    def clear(self) -> None:
        """Remove all registered executors."""
        self._executors.clear()


# Global executor registry (can be used by workers)
default_executor_registry = ExecutorRegistry()
