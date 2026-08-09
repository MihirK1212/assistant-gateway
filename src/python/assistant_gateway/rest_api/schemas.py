from __future__ import annotations

from enum import Enum
from typing import List, Optional, Union

from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    BackgroundAgentTask,
    ChatMetadata,
    SynchronousAgentTask,
    UserContext,
)
from assistant_gateway.schemas import AgentOutput, UserInput
from pydantic import BaseModel, Field


class RunMode(str, Enum):   
    sync = "sync"
    background = "background"


class CreateChatRequest(BaseModel):
    user_id: str
    agent_name: Optional[str] = Field(
        default=None, description="Agent to use for this chat"
    )


class CreateChatResponse(BaseModel):
    chat: ChatMetadata


class SendMessageRequest(BaseModel):
    content: str
    run_mode: RunMode = RunMode.sync
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
    pass  


class InterruptTaskResponse(BaseModel):
    task: Union[SynchronousAgentTask, BackgroundAgentTask]


class RerunTaskRequest(BaseModel):
    run_mode: RunMode = RunMode.sync
    user_context: Optional[UserContext] = None
    backend_server_context: Optional[BackendServerContext] = None


class RerunTaskResponse(BaseModel):
    chat: ChatMetadata
    assistant_response: Optional[AgentOutput] = None
    task: Optional[Union[SynchronousAgentTask, BackgroundAgentTask]] = None
