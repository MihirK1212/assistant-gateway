from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from assistant_gateway.schemas import AgentOutput
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackgroundTask,
    ChatMetadata,
    StoredAgentInteraction,
    BackendServerContext,
)
from assistant_gateway.chat_orchestrator.core.schemas import UserContext


class RunMode(str, Enum):
    sync = "sync"
    background = "background"


class CreateChatRequest(BaseModel):
    user_id: str
    agent_name: Optional[str] = Field(
        default=None, description="Agent to use for this chat"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)
    extra_metadata: Dict[str, Any] = Field(default_factory=dict)


class CreateChatResponse(BaseModel):
    chat: ChatMetadata


class SendMessageRequest(BaseModel):
    content: str
    run_mode: RunMode = RunMode.sync
    message_metadata: Dict[str, Any] = Field(default_factory=dict)
    user_context: Optional[UserContext] = None
    backend_server_context: Optional[BackendServerContext] = None


class SendMessageResponse(BaseModel):
    chat: ChatMetadata
    assistant_response: Optional[AgentOutput] = None
    task: Optional[BackgroundTask] = None


class ChatInteractionsResponse(BaseModel):
    chat_id: str
    interactions: List[StoredAgentInteraction]


class ChatResponse(BaseModel):
    chat: ChatMetadata


class TaskResponse(BaseModel):
    task: BackgroundTask
