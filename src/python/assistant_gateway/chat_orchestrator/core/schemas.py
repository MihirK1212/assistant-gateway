from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Annotated
from pydantic import BaseModel, Field

from assistant_gateway.schemas import AgentOutput, UserInput


class ChatStatus(str, Enum):
    active = "active"
    archived = "archived"


class StoredInteractionMetadata(BaseModel):
    id: str
    created_at: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)


class StoredUserInput(UserInput, StoredInteractionMetadata):
    pass


class StoredAgentOutput(AgentOutput, StoredInteractionMetadata):
    pass

StoredAgentInteraction = Union[
    StoredUserInput,
    StoredAgentOutput,
]

class UserContext(BaseModel):
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    auth_token: Optional[str] = None
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)


class BackendServerContext(BaseModel):
    base_url: Optional[str] = None
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)


class GatewayDefaultFallbackConfig(BaseModel):
    fallback_backend_url: Optional[str] = None


class ChatMetadata(BaseModel):
    chat_id: str
    user_id: str
    agent_name: str
    status: ChatStatus = ChatStatus.active
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Fixed metadata persisted for the chat lifetime",
    )
    extra_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Chat-specific metadata that can vary per conversation",
    )
    last_task_id: Optional[str] = None


class Chat(BaseModel):
    chat: ChatMetadata
    interactions: List[StoredAgentInteraction] = Field(default_factory=list)


class TaskStatus(str, Enum):
    pending = "pending"
    in_progress = "in_progress"
    completed = "completed"
    failed = "failed"


class BackgroundTask(BaseModel):
    id: str
    queue_id: str
    chat_id: str
    status: TaskStatus
    created_at: datetime
    updated_at: datetime
    payload: Dict[str, Any] = Field(default_factory=dict)
    result: Optional[AgentOutput] = None
    error: Optional[str] = None
