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
    - Distributed execution via Celery+Redis
    - Pre-registered executors via ExecutorRegistry
    - Simplified setup via ClauqBTM instance manager with state validation

IMPORTANT: For distributed execution (Celery), executors must be pre-registered
at application initialization time. The same module that registers executors
must be imported by both API servers and Celery workers.

Celery/Redis Requirements:
    - Celery and Redis are required for background task execution
    - If Celery/Redis setup fails, background tasks will fail
    - Sync tasks (create_and_execute_sync) always work regardless of Celery setup

Recommended Usage (Master Setup - simplest):
    from assistant_gateway.clauq_btm import ClauqBTM

    async def my_executor(task):
        return {"result": task.payload}

    async def on_complete(task, result):
        print(f"Task {task.id} completed")

    # Single setup() call handles everything
    clauq_btm = ClauqBTM(redis_url='redis://localhost:6379/0')
    task_manager = clauq_btm.setup(executors={
        'my_executor': {
            'executor': my_executor,
            'post_execution': on_complete,
        },
        'another_task': {
            'executor': another_fn,
        },
    })

    # For Celery workers, export:
    celery_app = clauq_btm.celery_app

    # Use task manager
    async with clauq_btm:
        task = await task_manager.create_and_enqueue(
            queue_id="my_queue",
            executor_name="my_executor",
        )

    # Run workers with: celery -A your_module worker

Alternative Usage (Manual Registration):
    from assistant_gateway.clauq_btm import ClauqBTM

    clauq_btm = ClauqBTM(redis_url='redis://localhost:6379/0')

    # Register executors one by one
    clauq_btm.register_executor(
        name="my_executor",
        executor=my_executor_fn,
        post_execution=my_callback,
    )

    # Finalize setup
    task_manager = clauq_btm.finalize_setup()
    celery_app = clauq_btm.celery_app
"""

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.task_manager import BTMTaskManager
from assistant_gateway.clauq_btm.executor_registry import (
    ExecutorRegistry,
    ExecutorConfig,
    ExecutorFunc,
    PostExecutionFunc,
)
from assistant_gateway.clauq_btm.queue_manager import (
    QueueInfo,
    CeleryQueueManager,
)
from assistant_gateway.clauq_btm.instance import (
    BackgroundTasksUnavailableError,
    ClauqBTM,
    ClauqBTMConfig,
    ClauqBTMSetupError,
    SetupState,
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
    # Executor registry
    "ExecutorRegistry",
    "ExecutorConfig",
    "ExecutorFunc",
    "PostExecutionFunc",
    # Queue manager
    "QueueInfo",
    "CeleryQueueManager",
    # Instance manager
    "BackgroundTasksUnavailableError",
    "ClauqBTM",
    "ClauqBTMConfig",
    "ClauqBTMSetupError",
    "SetupState",
]
