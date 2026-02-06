"""
Celery task definition for executing ClauqBTMTask.

The execute_task is a single pre-registered Celery task that:
1. Looks up the executor by name from the registry
2. Checks for interruption before/after execution
3. Runs the executor
4. Runs post_execution callback if registered
5. Updates task state in Redis
6. Publishes completion events
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, TYPE_CHECKING

from assistant_gateway.clauq_btm.schemas import TaskStatus
from assistant_gateway.clauq_btm.events import TaskEventType
from assistant_gateway.clauq_btm.executor_registry import ExecutorRegistry
from assistant_gateway.clauq_btm.queue_manager.constants import (
    TASK_KEY_PREFIX,
    EVENTS_CHANNEL_PREFIX,
    COMPLETED_TASK_TTL,
)
from assistant_gateway.clauq_btm.queue_manager.serialization import (
    deserialize_task,
    serialize_task,
)

if TYPE_CHECKING:
    from celery import Celery


logger = logging.getLogger(__name__)


def create_celery_task(
    celery_app: "Celery",
    executor_registry: ExecutorRegistry,
) -> Any:
    """
    Create the Celery task that executes ClauqBTMTask.

    This should be called once during application initialization (at import time
    for the module that both API servers and workers import). The task looks up
    executors from the executor_registry by name.

    IMPORTANT: The executor_registry must have all executors registered BEFORE
    this function is called and before Celery workers start.

    Args:
        celery_app: The Celery application instance
        executor_registry: Registry containing pre-registered executors
    """

    @celery_app.task(
        bind=True,
        name="clauq.execute_task",
        acks_late=True,
        reject_on_worker_lost=True,
        max_retries=0,  # No automatic retries - let the caller handle it
    )
    def execute_task(
        self: Any,
        task_data: Dict[str, Any],
        executor_name: str,
        redis_url: str,
    ) -> Dict[str, Any]:
        """
        Celery task that executes a ClauqBTMTask.

        This task:
        1. Deserializes the task
        2. Checks if task was interrupted before starting
        3. Looks up the executor by name from the registry
        4. Runs the executor (handles async)
        5. Checks if task was interrupted after execution
        6. Runs post_execution callback if registered
        7. Updates task state in Redis
        8. Publishes completion event
        """
        import asyncio
        import redis

        # Connect to Redis (sync client for Celery)
        redis_client = redis.from_url(redis_url)

        task_id = task_data["id"]
        queue_id = task_data["queue_id"]
        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        def _get_task_status() -> Optional[str]:
            """Get current task status from Redis."""
            status = redis_client.hget(task_key, "status")
            if isinstance(status, bytes):
                return status.decode()
            return status

        def _update_task_state(
            status: TaskStatus,
            result: Optional[Dict[str, Any]] = None,
            error: Optional[str] = None,
        ) -> None:
            """Update task state in Redis."""
            now = datetime.now(timezone.utc).isoformat()
            updates: Dict[str, Any] = {
                "status": status.value,
                "updated_at": now,
            }
            if result is not None:
                updates["result"] = json.dumps(result)
            if error is not None:
                updates["error"] = error

            redis_client.hset(task_key, mapping=updates)

            # Set TTL for completed tasks
            if status in (
                TaskStatus.completed,
                TaskStatus.failed,
                TaskStatus.interrupted,
            ):
                redis_client.expire(task_key, COMPLETED_TASK_TTL)

        def _publish_event(
            event_type: TaskEventType, error: Optional[str] = None
        ) -> None:
            """Publish event to Redis pub/sub."""
            # Get current task state
            task_data_raw = redis_client.hgetall(task_key)
            task_data_decoded: Dict[str, Any] = {
                k.decode() if isinstance(k, bytes) else k: (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in task_data_raw.items()
            }

            # Deserialize JSON fields (result, payload, metadata) stored as strings in Redis
            for field in ("result", "payload", "metadata"):
                if field in task_data_decoded and task_data_decoded[field]:
                    try:
                        task_data_decoded[field] = json.loads(
                            task_data_decoded[field]
                        )
                    except (json.JSONDecodeError, TypeError):
                        pass

            # Properly serialize the task using the same format as manager.py
            serialized_task = None
            if task_data_decoded:
                try:
                    # Deserialize to ClauqBTMTask then serialize back to ensure consistent format
                    task_obj = deserialize_task(task_data_decoded)
                    serialized_task = serialize_task(task_obj)
                except Exception as e:
                    logger.warning(f"Failed to serialize task for event: {e}")
                    serialized_task = None

            event = {
                "event_type": event_type.value,
                "task_id": task_id,
                "queue_id": queue_id,
                "status": task_data_decoded.get("status", TaskStatus.pending.value),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": error,
                "progress": None,
                "task": serialized_task,
            }
            logger.info(f"Publishing event: {event} for task {task_id}")
            redis_client.publish(events_channel, json.dumps(event))

        try:
            # Check if interrupted before starting
            current_status = _get_task_status()
            if current_status == TaskStatus.interrupted.value:
                logger.info(f"Task {task_id} was interrupted before starting")
                _publish_event(TaskEventType.INTERRUPTED, error="Task was interrupted")
                return {"status": "interrupted"}

            # Mark as in progress
            _update_task_state(TaskStatus.in_progress)
            _publish_event(TaskEventType.STARTED)

            # Get executor config from registry
            executor_config = executor_registry.get_config(executor_name)
            if executor_config is None:
                raise RuntimeError(
                    f"Executor '{executor_name}' not found in registry. "
                    "Make sure executors are registered before starting workers."
                )

            # Deserialize task
            task = deserialize_task(task_data)

            # Run the async executor
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(executor_config.executor(task))

                # Check if interrupted during execution
                current_status = _get_task_status()
                if current_status == TaskStatus.interrupted.value:
                    logger.info(f"Task {task_id} was interrupted during execution")
                    _publish_event(
                        TaskEventType.INTERRUPTED, error="Task was interrupted"
                    )
                    return {"status": "interrupted"}

                # Run post_execution callback if registered
                if executor_config.post_execution is not None:
                    loop.run_until_complete(
                        executor_config.post_execution(task, result)
                    )

            finally:
                loop.close()

            # Serialize result
            result_data = None
            if result is not None:
                if hasattr(result, "model_dump"):
                    result_data = result.model_dump(mode="json")
                elif hasattr(result, "dict"):
                    result_data = result.dict()
                else:
                    result_data = result

            # Mark as completed
            _update_task_state(TaskStatus.completed, result=result_data)
            _publish_event(TaskEventType.COMPLETED)

            return {"status": "completed", "result": result_data}

        except Exception as e:
            error_msg = str(e)
            logger.exception(f"Task {task_id} failed: {error_msg}")

            # Check if this was a revocation (interrupt)
            if "TaskRevokedError" in type(e).__name__ or self.request.called_directly:
                _update_task_state(TaskStatus.interrupted, error="Task was interrupted")
                _publish_event(TaskEventType.INTERRUPTED, error="Task was interrupted")
                return {"status": "interrupted"}

            # Mark as failed
            _update_task_state(TaskStatus.failed, error=error_msg)
            _publish_event(TaskEventType.FAILED, error=error_msg)

            return {"status": "failed", "error": error_msg}

        finally:
            redis_client.close()

    return execute_task
