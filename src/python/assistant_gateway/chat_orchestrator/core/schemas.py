from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional
from pydantic import BaseModel, Field

from assistant_gateway.schemas import AgentOutput, AgentInteraction, TaskStatus


class ChatStatus(str, Enum):
    active = "active"
    archived = "archived"


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


class AgentTask(BaseModel):
    """
    Base class for agent tasks. A task represents the execution of an agent
    for a specific user interaction.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    chat_id: str
    interaction_id: str = Field(
        description="The user interaction ID this task is processing"
    )
    status: TaskStatus = TaskStatus.pending
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    result: Optional[AgentOutput] = None
    error: Optional[str] = None
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional data needed for task execution (e.g., user_context, backend_server_context)",
    )
    is_background: bool = False

    def is_interrupted(self) -> bool:
        """Check if the task was interrupted."""
        return self.status == TaskStatus.interrupted

    def is_terminal(self) -> bool:
        """Check if the task is in a terminal state (completed, failed, or interrupted)."""
        return self.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        )


class SynchronousAgentTask(AgentTask):
    """Task for synchronous execution mode."""

    is_background: Literal[False] = Field(default=False, frozen=True)


class BackgroundAgentTask(AgentTask):
    """Task for background execution mode with queue support.
    
    The executor is embedded in the task itself, making the task self-contained.
    The queue manager simply calls the executor to run the task.
    """
    model_config = {"arbitrary_types_allowed": True}
    
    queue_id: str = Field(description="The queue ID where this task is scheduled")
    executor: Optional[Callable[["BackgroundAgentTask"], Awaitable[AgentOutput]]] = Field(
        default=None,
        exclude=True,  # Don't serialize the executor
        description="The async function that executes this task and returns AgentOutput",
    )

    is_background: Literal[True] = Field(default=True, frozen=True)
    
    async def execute(self) -> AgentOutput:
        """Execute this task using the embedded executor."""
        if self.executor is None:
            raise RuntimeError("Task executor not set")
        return await self.executor(self)


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
    task_ids: List[str] = Field(default_factory=list)
    current_task_id: Optional[str] = Field(
        default=None,
        description="The currently active task ID for this chat",
    )


class Chat(BaseModel):
    chat: ChatMetadata
    interactions: List[AgentInteraction] = Field(default_factory=list)
