from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional

if TYPE_CHECKING:
    from assistant_gateway.clauq_btm.schemas import ClauqBTMTask


# type for executor functions: (task) -> result
ExecutorFunc = Callable[["ClauqBTMTask"], Awaitable[Any]]

# type for post-execution callbacks: (task, result) -> None
PostExecutionFunc = Callable[["ClauqBTMTask", Any], Awaitable[None]]


@dataclass
class ExecutorConfig:
    executor: ExecutorFunc
    post_execution: Optional[PostExecutionFunc] = None


class ExecutorRegistry:
    def __init__(self) -> None:
        self._executors: Dict[str, ExecutorConfig] = {}

    def register(
        self,
        name: str,
        post_execution: Optional[PostExecutionFunc] = None,
    ) -> Callable[[ExecutorFunc], ExecutorFunc]:
        """
        Decorator to register an executor function.

        example:

        @registry.register("my_executor_name", post_execution=my_callback)
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
        self._executors[name] = config

    def get_config(self, name: str) -> Optional[ExecutorConfig]:
        return self._executors.get(name)

    @property
    def names(self) -> list[str]:
        return list(self._executors.keys())

    def clear(self) -> None:
        self._executors = {}

    def __contains__(self, name: str) -> bool:
        return name in self._executors

    def __len__(self) -> int:
        return len(self._executors)
