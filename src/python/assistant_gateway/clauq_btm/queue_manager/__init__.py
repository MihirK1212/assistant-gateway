"""
Queue Manager module for the Clau-Queue Background Task Manager.

This implementation uses Celery for distributed task execution and Redis for:
- Task state persistence
- FIFO queue management per queue_id
- Real-time event pub/sub for subscribers

Architecture:
    ┌─────────────────┐      ┌─────────────────┐
    │  Application    │      │  Celery Worker  │
    │  (enqueue)      │─────▶│  (execute)      │
    └────────┬────────┘      └────────┬────────┘
             │                        │
             ▼                        ▼
    ┌─────────────────────────────────────────┐
    │              Redis                       │
    │  - Task State (Hash)                    │
    │  - Queue (Sorted Set for FIFO)          │
    │  - Events (Pub/Sub)                     │
    └─────────────────────────────────────────┘

Usage:
    # In shared_setup.py (imported by both API and workers)
    from celery import Celery
    from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager

    celery_app = Celery('tasks', broker='redis://localhost:6379/0')
    queue_manager = CeleryQueueManager(
        celery_app=celery_app,
        redis_url='redis://localhost:6379/0',
    )

    # Register executors (can be done after manager creation)
    @queue_manager.executor_registry.register("my_executor")
    async def my_executor(task: ClauqBTMTask) -> Any:
        return {"result": task.payload}

    # Use queue manager
    async with queue_manager:
        await queue_manager.enqueue(task, executor_name="my_executor")

    # Run workers with: celery -A shared_setup worker
"""

from assistant_gateway.clauq_btm.queue_manager.manager import (
    CeleryQueueManager,
    QueueInfo,
)
from assistant_gateway.clauq_btm.queue_manager.celery_task import (
    create_celery_task,
)
from assistant_gateway.clauq_btm.queue_manager.subscription import (
    EventSubscription,
    RedisEventSubscription,
)
from assistant_gateway.clauq_btm.queue_manager.constants import (
    TASK_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
    QUEUE_META_PREFIX,
    CELERY_TASK_PREFIX,
    EVENTS_CHANNEL_PREFIX,
    ALL_EVENTS_CHANNEL,
    COMPLETED_TASK_TTL,
)

__all__ = [
    # Main classes
    "CeleryQueueManager",
    "RedisEventSubscription",
    "QueueInfo",
    "EventSubscription",
    # Factory function
    "create_celery_task",
    # Constants
    "TASK_KEY_PREFIX",
    "QUEUE_KEY_PREFIX",
    "QUEUE_META_PREFIX",
    "CELERY_TASK_PREFIX",
    "EVENTS_CHANNEL_PREFIX",
    "ALL_EVENTS_CHANNEL",
    "COMPLETED_TASK_TTL",
]
