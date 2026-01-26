"""
Queue Manager module for the Clau-Queue Background Task Manager.

Provides abstractions and implementations for task queue management:
- QueueManager: Abstract base class defining the interface
- InMemoryQueueManager: Single-process implementation using asyncio
- CeleryQueueManager: Distributed implementation using Celery + Redis

Usage:
    # In-memory (development/testing)
    from assistant_gateway.clauq_btm.queue_manager import InMemoryQueueManager

    async with InMemoryQueueManager() as qm:
        await qm.enqueue(task)

    # Celery (production/distributed)
    from assistant_gateway.clauq_btm.queue_manager.celery import (
        CeleryQueueManager,
        create_celery_app,
    )

    app = create_celery_app()
    qm = CeleryQueueManager(celery_app=app)

    async with qm:
        await qm.enqueue(task, executor_name="my_task")
"""

from assistant_gateway.clauq_btm.queue_manager.base import (
    EventSubscription,
    QueueInfo,
    QueueManager,
)
from assistant_gateway.clauq_btm.queue_manager.in_memory import InMemoryQueueManager

__all__ = [
    # Base classes and types
    "EventSubscription",
    "QueueInfo",
    "QueueManager",
    # Implementations
    "InMemoryQueueManager",
    # Note: CeleryQueueManager is imported from .celery submodule
    # to avoid requiring celery/redis as mandatory dependencies
]
