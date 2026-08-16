from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Literal, Optional

from assistant_gateway.schemas import AgentInteraction, AgentOutput, TaskStatus
from pydantic import BaseModel, Field


class ChatStatus(str, Enum):
    active = "active"
    archived = "archived"


class AgentTask(BaseModel):
    """
    A task represents the execution of an agent for a specific user interaction.
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
        description="payload passed to the executor function",
    )
    is_background: bool = False

    def is_interrupted(self) -> bool:
        return self.status == TaskStatus.interrupted

    def is_terminal(self) -> bool:
        return self.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        )


class SynchronousAgentTask(AgentTask):
    is_background: Literal[False] = Field(default=False, frozen=True)


class BackgroundAgentTask(AgentTask):
    """Task for background execution mode with ClauqBTM support.

    In-Memory mode: set the `executor` directly on the task 
    ClauqBTM mode: set the `executor_name` to reference a registered executor
    """

    model_config = {"arbitrary_types_allowed": True}

    queue_id: str = Field(description="The queue ID where this task is scheduled")

    executor: Optional[Callable[["BackgroundAgentTask"], Awaitable[AgentOutput]]] = (
        Field(
            default=None,
            exclude=True, 
            description="the async function that executes this task",
        )
    )

    executor_name: Optional[str] = Field(
        default=None,
        description="name of the registered executor function for celery in clauq_btm",
    )

    is_background: Literal[True] = Field(default=True, frozen=True)

    async def execute(self) -> AgentOutput:
        if self.executor is None:
            raise RuntimeError(
                "Task executor not set. For in-memory mode, set task.executor. "
                "For Celery mode, the worker handles execution via executor_name."
            )
        return await self.executor(self)


class ChatMetadata(BaseModel):
    chat_id: str
    user_id: str
    agent_name: str
    status: ChatStatus = ChatStatus.active
    created_at: datetime
    updated_at: datetime
    task_ids: List[str] = Field(default_factory=list)
    current_task_id: Optional[str] = Field(
        default=None,
        description="The currently active task ID for this chat",
    )


class Chat(BaseModel):
    chat: ChatMetadata
    interactions: List[AgentInteraction] = Field(default_factory=list)
