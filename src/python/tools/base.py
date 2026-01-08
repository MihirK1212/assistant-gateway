from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Any, Dict, Optional, Type

from pydantic import BaseModel, Field

from assistant_gateway.schemas import ToolResult


class ToolConfig(BaseModel):
    """
    Configuration for a tool.
    Config is static and is set when the tool is registered.
    """
    name: str = Field(description="The name of the tool")
    description: str = Field(description="The description of the tool")
    input_model: Optional[Type[BaseModel]] = Field(default=None, description="The input model of the tool")
    output_description: Optional[str] = Field(default=None, description="The output description of the tool")
    output_model: Optional[Type[BaseModel]] = Field(default=None, description="The output model of the tool")


class ToolContext(BaseModel):
    """
    Runtime context passed to tools.

    The context carries per request metadata such as input payload, timeout, and metadata.
    Context is dynamic and is set when the tool is called.
    """

    input: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = Field(
        default=30, description="Timeout in seconds for the tool execution"
    )

    def with_input(self, payload: Dict[str, Any]) -> "ToolContext":
        """
        Return a cloned context embedding the tool-specific input payload.

        This avoids mutating the shared context when multiple tools are called
        within the same agent turn.
        """

        data = deepcopy(self.model_dump())
        data["input"] = payload
        return ToolContext(**data)


class Tool(ABC):
    def __init__(self, config: ToolConfig):
        self.config = config

    @property
    def name(self) -> str:
        return self.config.name

    @abstractmethod
    async def run(self, context: ToolContext) -> ToolResult:
        raise NotImplementedError(
            "The run method must be implemented by the subclass for a Tool"
        )
