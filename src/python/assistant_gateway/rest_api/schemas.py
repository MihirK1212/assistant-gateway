from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from assistant_gateway.schemas import AgentOutput, UserInput
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackgroundAgentTask,
    SynchronousAgentTask,
    ChatMetadata,
    BackendServerContext,
    UserContext,
)


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
    task: Optional[Union[SynchronousAgentTask, BackgroundAgentTask]] = None


class ChatInteractionsResponse(BaseModel):
    chat_id: str
    interactions: List[Union[UserInput, AgentOutput]]


class ChatResponse(BaseModel):
    chat: ChatMetadata


class TaskResponse(BaseModel):
    task: Union[SynchronousAgentTask, BackgroundAgentTask]


class InterruptTaskRequest(BaseModel):
    """Request to interrupt a running task."""
    pass  # No additional fields needed, task_id comes from URL


class InterruptTaskResponse(BaseModel):
    """Response after interrupting a task."""
    task: Union[SynchronousAgentTask, BackgroundAgentTask]


class RerunTaskRequest(BaseModel):
    """Request to rerun a task."""
    run_mode: RunMode = RunMode.sync
    user_context: Optional[UserContext] = None
    backend_server_context: Optional[BackendServerContext] = None


class RerunTaskResponse(BaseModel):
    """Response after rerunning a task."""
    chat: ChatMetadata
    assistant_response: Optional[AgentOutput] = None
    task: Optional[Union[SynchronousAgentTask, BackgroundAgentTask]] = None
