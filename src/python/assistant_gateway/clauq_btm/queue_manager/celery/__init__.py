"""
Celery-based Queue Manager.

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
    from celery import Celery

    app = Celery('tasks', broker='redis://localhost:6379/0')

    queue_manager = CeleryQueueManager(
        celery_app=app,
        redis_url='redis://localhost:6379/0',
    )

    async with queue_manager:
        await queue_manager.enqueue(task, executor_name="my_executor")

Worker Setup:
    # In your worker module, register executors:
    from assistant_gateway.clauq_btm import default_executor_registry

    @default_executor_registry.register("my_executor")
    async def my_executor(task: ClauqBTMTask) -> Any:
        ...

    # Then run celery worker:
    # celery -A your_module worker -l info
"""

from assistant_gateway.clauq_btm.queue_manager.celery.manager import (
    CeleryQueueManager,
)
from assistant_gateway.clauq_btm.queue_manager.celery.celery_task import (
    create_celery_task,
)
from assistant_gateway.clauq_btm.queue_manager.celery.subscription import (
    RedisEventSubscription,
)
from assistant_gateway.clauq_btm.queue_manager.celery.utils import (
    create_celery_app,
)
from assistant_gateway.clauq_btm.queue_manager.celery.constants import (
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
    # Factory functions
    "create_celery_task",
    "create_celery_app",
    # Constants
    "TASK_KEY_PREFIX",
    "QUEUE_KEY_PREFIX",
    "QUEUE_META_PREFIX",
    "CELERY_TASK_PREFIX",
    "EVENTS_CHANNEL_PREFIX",
    "ALL_EVENTS_CHANNEL",
    "COMPLETED_TASK_TTL",
]
