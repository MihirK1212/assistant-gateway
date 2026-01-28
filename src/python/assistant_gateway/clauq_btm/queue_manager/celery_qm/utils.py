"""
Utility functions for setting up Celery with the queue manager.

This module provides create_celery_app() which creates a Celery app with
the clauq.execute_task already registered. This is important because Celery
workers need to see the task at import time.

Both this module and CeleryQueueManager use default_executor_registry,
ensuring that executors registered by BTMTaskManager are visible to workers.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

from assistant_gateway.clauq_btm.executor_registry import default_executor_registry
from assistant_gateway.clauq_btm.queue_manager.celery_qm.constants import (
    COMPLETED_TASK_TTL,
)
from assistant_gateway.clauq_btm.queue_manager.celery_qm.celery_task import (
    create_celery_task,
)

if TYPE_CHECKING:
    from celery import Celery


def create_celery_app(
    name: str = "clauq",
    broker_url: str = "redis://localhost:6379/0",
    result_backend: str = "redis://localhost:6379/0",
    **kwargs: Any,
) -> "Celery":
    """
    Create a pre-configured Celery app for the queue manager.

    This registers the clauq.execute_task with default_executor_registry,
    which is shared with BTMTaskManager and CeleryQueueManager. This ensures
    executors registered at runtime are visible to Celery workers.

    Args:
        name: Name of the Celery app
        broker_url: Message broker URL (Redis recommended)
        result_backend: Result backend URL
        **kwargs: Additional Celery configuration

    Returns:
        Configured Celery app

    Example:
        # In your worker module (e.g., tasks.py):
        app = create_celery_app()

        # Executors can be registered via decorator (at module load time)
        @default_executor_registry.register("my_task")
        async def my_task(task: ClauqBTMTask) -> Any:
            return {"result": task.payload}

        # Or registered dynamically via BTMTaskManager.create_and_enqueue()
        # Both use default_executor_registry, so workers will find them.

        # Start worker:
        # celery -A tasks:app worker -l info -Q clauq_queue_id
    """
    from celery import Celery

    app = Celery(
        name,
        broker=broker_url,
        backend=result_backend,
    )

    # Configure for task queue manager
    app.conf.update(
        # Task settings
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        # Worker settings
        worker_prefetch_multiplier=1,  # One task at a time for FIFO
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        # Result settings
        result_expires=COMPLETED_TASK_TTL,
        # Apply any additional configuration
        **kwargs,
    )

    # Register the execute_task
    create_celery_task(app, default_executor_registry)

    return app
