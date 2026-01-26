"""
Clau-Queue Background Task Manager (clauq_btm).

A standalone library for managing background task execution with queue support.
Provides task lifecycle management, FIFO queue processing, and event notifications.

This module is designed to be independent and can be extracted as a separate
package in the future.

Features:
    - Generic task abstraction (ClauqBTMTask)
    - FIFO queue management per queue_id
    - Task lifecycle events (queued, started, completed, failed, interrupted)
    - Pluggable queue backends (in-memory, Celery+Redis)
    - Internal executor registry (managed by BTMTaskManager)

Basic Usage:
    from assistant_gateway.clauq_btm import (
        BTMTaskManager,
        ClauqBTMTask,
        InMemoryQueueManager,
    )

    # Create manager with in-memory backend
    queue_manager = InMemoryQueueManager()
    task_manager = BTMTaskManager(queue_manager)

    async def my_executor(task: ClauqBTMTask) -> dict:
        return {"result": task.payload}

    async with task_manager:
        # Sync execution
        task, result = await task_manager.create_and_execute_sync(
            queue_id="my_queue",
            executor=my_executor,
        )

        # Background execution
        task = await task_manager.create_and_enqueue(
            queue_id="my_queue",
            executor=my_executor,
            executor_name="my_task",  # Registered internally
        )

Distributed Usage (Celery):
    from assistant_gateway.clauq_btm import BTMTaskManager, ClauqBTMTask
    from assistant_gateway.clauq_btm.queue_manager.celery import (
        CeleryQueueManager,
        create_celery_app,
    )

    # Create Celery backend
    app = create_celery_app()
    queue_manager = CeleryQueueManager(celery_app=app)
    task_manager = BTMTaskManager(queue_manager)

    # Executors are registered automatically via create_and_enqueue
"""

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.task_manager import BTMTaskManager
from assistant_gateway.clauq_btm.queue_manager import (
    QueueInfo,
    QueueManager,
    InMemoryQueueManager,
)

__all__ = [
    # Core schemas
    "ClauqBTMTask",
    "TaskStatus",
    # Events
    "TaskEvent",
    "TaskEventType",
    # Task manager
    "BTMTaskManager",
    # Queue manager
    "QueueInfo",
    "QueueManager",
    "InMemoryQueueManager",
    # Note: CeleryQueueManager is imported from .queue_manager.celery
    # to avoid requiring celery/redis as mandatory dependencies
]
