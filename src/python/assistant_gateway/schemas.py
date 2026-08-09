from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Role(str, Enum):
    user = "user"
    assistant = "assistant"


class ToolCall(BaseModel):
    id: str
    name: str
    input: Dict[str, Any] = Field(default_factory=dict)


class ToolResult(BaseModel):
    """
    Used in two contexts:
    1. Tool implementations return this with name + output
    2. Agent response parsing populates tool_call_id to link back to ToolCall
    """

    output: Any
    name: Optional[str] = None  
    tool_call_id: Optional[str] = (
        None 
    )
    is_error: bool = False  
    raw_response: Any = None  


class AgentStep(BaseModel):
    """A single step in the agent's reasoning process."""

    thought: Optional[str] = None
    tool_calls: List[ToolCall] = Field(default_factory=list)
    tool_results: List[ToolResult] = Field(
        default_factory=list
    ) 


class AgentInteraction(BaseModel):
    """
    Base class for all agent interactions.
    Can be either a user input or an agent output.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    sequence_id: Optional[int] = Field(default=None, description="The sequence id of the interaction in the conversation")
    role: Role


class UserInput(AgentInteraction):
    content: str

    def __init__(self, **data):
        super().__init__(**data)
        if self.role != Role.user:
            raise ValueError("UserInput.role must be 'user'")


class AgentOutput(AgentInteraction):
    messages: List[str]
    steps: List[AgentStep] = Field(default_factory=list)
    final_text: Optional[str] = None
    user_input_interaction_id: str

    def __init__(self, **data):
        super().__init__(**data)
        if self.role != Role.assistant:
            raise ValueError("AgentOutput.role must be 'assistant'")


class TaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"
    interrupted = "interrupted"
