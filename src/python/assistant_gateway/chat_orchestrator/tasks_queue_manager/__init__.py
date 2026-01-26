"""
Task Queue Manager module.

Provides abstractions and implementations for task queue management:
- TasksQueueManager: Abstract base class defining the interface
- InMemoryTasksQueueManager: Single-process implementation using asyncio
- CeleryTasksQueueManager: Distributed implementation using Celery + Redis

Usage:
    # In-memory (development/testing)
    from assistant_gateway.chat_orchestrator.tasks_queue_manager import (
        InMemoryTasksQueueManager,
    )

    async with InMemoryTasksQueueManager() as qm:
        await qm.enqueue(queue_id, task)

    # Celery (production/distributed)
    from assistant_gateway.chat_orchestrator.tasks_queue_manager.celery import (
        CeleryTasksQueueManager,
        executor_registry,
        create_celery_app,
    )

    @executor_registry.register("my_task")
    async def my_executor(task):
        ...

    app = create_celery_app()
    qm = CeleryTasksQueueManager(celery_app=app)

    async with qm:
        await qm.enqueue(queue_id, task, executor_name="my_task")
"""

from assistant_gateway.chat_orchestrator.tasks_queue_manager.base import (
    EventSubscription,
    ExecutorRegistry,
    QueueInfo,
    TaskEvent,
    TaskEventType,
    TasksQueueManager,
    default_executor_registry,
)
from assistant_gateway.chat_orchestrator.tasks_queue_manager.in_memory import (
    InMemoryTasksQueueManager,
)

__all__ = [
    # Base classes and types
    "EventSubscription",
    "ExecutorRegistry",
    "QueueInfo",
    "TaskEvent",
    "TaskEventType",
    "TasksQueueManager",
    "default_executor_registry",
    # Implementations
    "InMemoryTasksQueueManager",
    # Note: CeleryTasksQueueManager is imported from .celery submodule
    # to avoid requiring celery/redis as mandatory dependencies
]
