from __future__ import annotations

from copy import deepcopy
from typing import Any, Awaitable, Callable, Dict, Optional, Type

from assistant_gateway.schemas import ToolResult
from pydantic import BaseModel, Field


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


class ToolConfig(BaseModel):
    """
    Configuration for a tool.
    Config is static and is set when the tool is registered.
    """

    name: str = Field(description="The name of the tool")
    description: str = Field(description="The description of the tool")
    input_model: Optional[Type[BaseModel]] = Field(
        default=None, description="The input model of the tool"
    )
    input_description: Optional[str] = Field(
        default=None, description="The input description of the tool"
    )
    output_model: Optional[Type[BaseModel]] = Field(
        default=None, description="The output model of the tool"
    )
    output_description: Optional[str] = Field(
        default=None, description="The output description of the tool"
    )
    timeout_seconds: int = Field(
        default=30, description="Timeout in seconds for the tool execution"
    )
    tool_level_input_overrides: Optional[Dict[str, Any]] = Field(
        default=None,
        description=(
            "Author-enforced input overrides applied last"
            "Cannot be overridden by runtime input_overrides"
        ),
    )


class ToolContext(BaseModel):
    """
    Runtime context passed to tools.

    The context carries per request metadata such as input payload
    Context is dynamic and is set when the tool is called.
    """

    input: Dict[str, Any] = Field(default_factory=dict)

    def with_input(self, payload: Dict[str, Any]) -> "ToolContext":
        data = deepcopy(self.model_dump())
        data["input"] = payload
        return ToolContext(**data)

    def apply_input_overrides(self, input_overrides: Optional[Dict[str, Any]]) -> "ToolContext":
        if not isinstance(input_overrides, dict) or not input_overrides:
            return self
        self.input = _deep_merge(self.input, input_overrides)
        return self


RunCallable = Callable[[ToolContext], Awaitable[ToolResult]]


class Tool:
    def __init__(
        self,
        *,
        name: str,
        description: str,
        run_callable: RunCallable,
        input_model: Optional[Type[BaseModel]] = None,
        input_description: Optional[str] = None,
        output_model: Optional[Type[BaseModel]] = None,
        output_description: Optional[str] = None,
        timeout_seconds: int = 30,
        tool_level_input_overrides: Optional[Dict[str, Any]] = None,
    ):
        self._run_callable = run_callable
        self._config = ToolConfig(
            name=name,
            description=description,
            input_model=input_model,
            input_description=input_description,
            output_model=output_model,
            output_description=output_description,
            timeout_seconds=timeout_seconds,
            tool_level_input_overrides=tool_level_input_overrides,
        )

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def config(self) -> ToolConfig:
        return self._config

    async def run(self, context: ToolContext) -> ToolResult:
        context = context.apply_input_overrides(self._config.tool_level_input_overrides)
        if self._config.input_model is not None:
            self._config.input_model.model_validate(context.input)
        return await self._run_callable(context)
