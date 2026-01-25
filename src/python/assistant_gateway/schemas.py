from __future__ import annotations

import uuid
from enum import Enum
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Role(str, Enum):
    user = "user"
    assistant = "assistant"


class ToolCall(BaseModel):
    """A tool invocation request from the assistant."""

    id: str
    name: str
    input: Dict[str, Any] = Field(default_factory=dict)


class ToolResult(BaseModel):
    """Result of a tool execution.

    Used in two contexts:
    1. Tool implementations return this with name + output
    2. Agent response parsing populates tool_call_id to link back to ToolCall
    """

    output: Any
    name: Optional[str] = None  # Tool name (set by tool implementations)
    tool_call_id: Optional[str] = (
        None  # Links back to ToolCall.id (set when parsing agent response)
    )
    is_error: bool = False  # Whether the tool execution failed
    raw_response: Any = None  # Optional: preserve original response for debugging


class AgentStep(BaseModel):
    """A single step in the agent's reasoning process."""

    thought: Optional[str] = None
    tool_calls: List[ToolCall] = Field(default_factory=list)
    tool_results: List[ToolResult] = Field(
        default_factory=list
    )  # Results for this step's tool calls


class AgentInteraction(BaseModel):
    """An interaction in the conversation."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    role: Role


class UserInput(AgentInteraction):
    """A message from the user."""

    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data):
        super().__init__(**data)
        if self.role != Role.user:
            raise ValueError("UserInput.role must be 'user'")


class AgentOutput(AgentInteraction):
    """An output from the assistant."""

    messages: List[str]
    steps: List[AgentStep] = Field(default_factory=list)
    final_text: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    def __init__(self, **data):
        super().__init__(**data)
        if self.role != Role.assistant:
            raise ValueError("AgentOutput.role must be 'assistant'")


class TaskStatus(str, Enum):
    """Status of an agent task."""

    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    interrupted = "interrupted"
