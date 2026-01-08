from __future__ import annotations

from typing import List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import BackgroundTask


class TasksQueueManager:
    """Abstraction for queue operations so backends can be swapped later."""

    async def enqueue(self, queue_id: str, task: BackgroundTask) -> None:
        raise NotImplementedError

    async def get(self, queue_id: str, task_id: str) -> Optional[BackgroundTask]:
        raise NotImplementedError

    async def update(self, queue_id: str, task: BackgroundTask) -> None:
        raise NotImplementedError

    async def delete(self, queue_id: str, task_id: str) -> None:
        raise NotImplementedError

    async def list(self, queue_id: str) -> List[BackgroundTask]:
        raise NotImplementedError

