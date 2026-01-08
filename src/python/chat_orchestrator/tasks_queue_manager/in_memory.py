from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from assistant_gateway.chat_orchestrator.core.schemas import BackgroundTask
from assistant_gateway.chat_orchestrator.tasks_queue_manager.base import TasksQueueManager

class InMemoryTasksQueueManager(TasksQueueManager):
    """
    Simple in-memory queue manager. Keeps the contract identical to a
    production-ready queue backend so we can swap implementations later.
    """

    def __init__(self) -> None:
        self._queues: Dict[str, List[BackgroundTask]] = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, queue_id: str, task: BackgroundTask) -> None:
        async with self._lock:
            self._queues.setdefault(queue_id, []).append(task)

    async def get(self, queue_id: str, task_id: str) -> Optional[BackgroundTask]:
        async with self._lock:
            for task in self._queues.get(queue_id, []):
                if task.id == task_id:
                    return task
        return None

    async def update(self, queue_id: str, task: BackgroundTask) -> None:
        async with self._lock:
            tasks = self._queues.get(queue_id, [])
            for idx, existing in enumerate(tasks):
                if existing.id == task.id:
                    tasks[idx] = task
                    break

    async def delete(self, queue_id: str, task_id: str) -> None:
        async with self._lock:
            tasks = self._queues.get(queue_id, [])
            self._queues[queue_id] = [task for task in tasks if task.id != task_id]

    async def list(self, queue_id: str) -> List[BackgroundTask]:
        async with self._lock:
            return list(self._queues.get(queue_id, []))

