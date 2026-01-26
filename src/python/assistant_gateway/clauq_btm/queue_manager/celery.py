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

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import (
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
    TYPE_CHECKING,
)

from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus
from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.executor_registry import ExecutorRegistry
from assistant_gateway.clauq_btm.queue_manager.base import (
    EventSubscription,
    QueueInfo,
    QueueManager,
)

if TYPE_CHECKING:
    from celery import Celery
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub


logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Constants
# -----------------------------------------------------------------------------

# Redis key prefixes
TASK_KEY_PREFIX = "clauq:task:"  # Hash storing task data
QUEUE_KEY_PREFIX = "clauq:queue:"  # Sorted set for FIFO ordering
QUEUE_META_PREFIX = "clauq:queue_meta:"  # Hash for queue metadata
CELERY_TASK_PREFIX = "clauq:celery_task:"  # Maps task_id -> celery_task_id
EVENTS_CHANNEL_PREFIX = "clauq:events:"  # Pub/sub channel for events
ALL_EVENTS_CHANNEL = "clauq:events:*"  # Pattern for all events

# Task state TTL (7 days) - completed tasks are kept for reference
COMPLETED_TASK_TTL = 7 * 24 * 60 * 60


# -----------------------------------------------------------------------------
# Redis Event Subscription
# -----------------------------------------------------------------------------


class RedisEventSubscription(EventSubscription):
    """Event subscription backed by Redis pub/sub."""

    def __init__(
        self,
        redis: "Redis",
        channel: str,
        pattern: bool = False,
    ) -> None:
        self._redis = redis
        self._channel = channel
        self._pattern = pattern
        self._pubsub: Any = None
        self._closed = False

    async def _ensure_subscribed(self) -> None:
        """Ensure we're subscribed to the channel."""
        if self._pubsub is None:
            self._pubsub: PubSub = self._redis.pubsub()
            if self._pattern:
                await self._pubsub.psubscribe(self._channel)
            else:
                await self._pubsub.subscribe(self._channel)

    def __aiter__(self) -> AsyncIterator[TaskEvent]:
        return self

    async def __anext__(self) -> TaskEvent:
        """Get the next event from the subscription."""
        if self._closed:
            raise StopAsyncIteration

        await self._ensure_subscribed()

        while not self._closed:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0,
                )
                if message is None:
                    continue

                if message["type"] in ("message", "pmessage"):
                    data = json.loads(message["data"])
                    return _deserialize_event(data)

            except asyncio.CancelledError:
                raise StopAsyncIteration
            except Exception as e:
                logger.warning(f"Error reading from pubsub: {e}")
                await asyncio.sleep(0.1)

        raise StopAsyncIteration

    async def close(self) -> None:
        """Close the subscription."""
        self._closed = True
        if self._pubsub is not None:
            try:
                if self._pattern:
                    await self._pubsub.punsubscribe(self._channel)
                else:
                    await self._pubsub.unsubscribe(self._channel)
                await self._pubsub.close()
            except Exception as e:
                logger.warning(f"Error closing pubsub: {e}")
            finally:
                self._pubsub = None


# -----------------------------------------------------------------------------
# Serialization Helpers
# -----------------------------------------------------------------------------


def _serialize_task(task: ClauqBTMTask) -> Dict[str, Any]:
    """Serialize a task for Redis storage."""
    return task.model_dump(mode="json", exclude={"executor"})


def _deserialize_task(data: Dict[str, Any]) -> ClauqBTMTask:
    """Deserialize a task from Redis storage."""
    return ClauqBTMTask.model_validate(data)


def _serialize_event(event: TaskEvent) -> Dict[str, Any]:
    """Serialize an event for pub/sub."""
    return {
        "event_type": event.event_type.value,
        "task_id": event.task_id,
        "queue_id": event.queue_id,
        "status": event.status.value,
        "timestamp": event.timestamp.isoformat(),
        "error": event.error,
        "progress": event.progress,
        "task": _serialize_task(event.task) if event.task else None,
    }


def _deserialize_event(data: Dict[str, Any]) -> TaskEvent:
    """Deserialize an event from pub/sub."""
    return TaskEvent(
        event_type=TaskEventType(data["event_type"]),
        task_id=data["task_id"],
        queue_id=data["queue_id"],
        status=TaskStatus(data["status"]),
        timestamp=datetime.fromisoformat(data["timestamp"]),
        error=data.get("error"),
        progress=data.get("progress"),
        task=_deserialize_task(data["task"]) if data.get("task") else None,
    )


# -----------------------------------------------------------------------------
# Celery Task Definition
# -----------------------------------------------------------------------------


def create_celery_task(
    celery_app: "Celery",
    executor_registry: ExecutorRegistry,
) -> Any:
    """
    Create the Celery task that executes ClauqBTMTask.

    This should be called once when setting up your Celery app.
    The task will look up executors from the executor_registry.
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
        2. Looks up the executor by name
        3. Runs the executor (handles async)
        4. Updates task state in Redis
        5. Publishes completion event
        """
        import asyncio
        import redis

        # Connect to Redis (sync client for Celery)
        redis_client = redis.from_url(redis_url)

        task_id = task_data["id"]
        queue_id = task_data["queue_id"]
        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        def _update_task_state(
            status: TaskStatus,
            result: Optional[Dict[str, Any]] = None,
            error: Optional[str] = None,
        ) -> None:
            """Update task state in Redis."""
            now = datetime.now(timezone.utc).isoformat()
            updates = {
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
            task_data_decoded = {
                k.decode() if isinstance(k, bytes) else k: (
                    v.decode() if isinstance(v, bytes) else v
                )
                for k, v in task_data_raw.items()
            }

            # Handle result field if it's JSON
            if "result" in task_data_decoded and task_data_decoded["result"]:
                try:
                    task_data_decoded["result"] = json.loads(
                        task_data_decoded["result"]
                    )
                except (json.JSONDecodeError, TypeError):
                    pass

            event = {
                "event_type": event_type.value,
                "task_id": task_id,
                "queue_id": queue_id,
                "status": task_data_decoded.get("status", TaskStatus.pending.value),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "error": error,
                "progress": None,
                "task": task_data_decoded if task_data_decoded else None,
            }
            redis_client.publish(events_channel, json.dumps(event))

        try:
            # Mark as in progress
            _update_task_state(TaskStatus.in_progress)
            _publish_event(TaskEventType.STARTED)

            # Get executor
            executor = executor_registry.get(executor_name)
            if executor is None:
                raise RuntimeError(f"Executor '{executor_name}' not found in registry")

            # Deserialize task
            task = _deserialize_task(task_data)

            # Run the async executor
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(executor(task))
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


# -----------------------------------------------------------------------------
# CeleryQueueManager
# -----------------------------------------------------------------------------


class CeleryQueueManager(QueueManager):
    """
    Distributed task queue manager using Celery and Redis.

    Features:
    - Distributed task execution via Celery workers
    - Persistent task state in Redis
    - FIFO ordering per queue using Redis sorted sets
    - Real-time event subscription via Redis pub/sub
    - Task interruption via Celery revoke

    Setup:
        1. Configure Celery app with Redis broker
        2. Register executors with default_executor_registry
        3. Run Celery workers with your task module
        4. Use this class in your application

    Example:
        app = Celery('tasks', broker='redis://localhost:6379/0')

        # Register executor
        @default_executor_registry.register("process_data")
        async def process_data(task: ClauqBTMTask) -> Any:
            return {"processed": task.payload}

        # Use queue manager
        queue_manager = CeleryQueueManager(
            celery_app=app,
            redis_url='redis://localhost:6379/0',
        )

        async with queue_manager:
            await queue_manager.enqueue(
                task=task,
                executor_name="process_data",
            )
    """

    def __init__(
        self,
        celery_app: "Celery",
        redis_url: str = "redis://localhost:6379/0",
    ) -> None:
        """
        Initialize the Celery queue manager.

        Args:
            celery_app: Configured Celery application
            redis_url: Redis connection URL
        """
        self._celery_app = celery_app
        self._redis_url = redis_url
        self._executor_registry: Optional[ExecutorRegistry] = None

        # Redis async client (initialized on start)
        self._redis: Optional["Redis"] = None

        # Celery task (created on start)
        self._celery_task: Optional[Any] = None

        # State
        self._is_running = False

        # Lock for async operations
        self._lock = asyncio.Lock()

    # -------------------------------------------------------------------------
    # Executor Registry
    # -------------------------------------------------------------------------

    def set_executor_registry(self, registry: ExecutorRegistry) -> None:
        """
        Set the executor registry for distributed execution.

        This is called by BTMTaskManager to share its registry.
        Must be called before start() for Celery workers to find executors.

        Args:
            registry: The executor registry to use
        """
        self._executor_registry = registry

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the queue manager."""
        if self._is_running:
            return

        if self._executor_registry is None:
            raise RuntimeError(
                "Executor registry not set. CeleryQueueManager must be used with "
                "BTMTaskManager which sets the registry automatically."
            )

        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "redis[async] is required for CeleryQueueManager. "
                "Install it with: pip install redis[async]"
            )

        # Initialize Redis async client
        self._redis = aioredis.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=True,
        )

        # Test connection
        await self._redis.ping()

        # Create Celery task
        self._celery_task = create_celery_task(
            self._celery_app, self._executor_registry
        )

        self._is_running = True
        logger.info("CeleryQueueManager started")

    async def stop(self) -> None:
        """Stop the queue manager gracefully."""
        if not self._is_running:
            return

        self._is_running = False

        # Close Redis connection
        if self._redis is not None:
            await self._redis.close()
            self._redis = None

        logger.info("CeleryQueueManager stopped")

    @property
    def is_running(self) -> bool:
        """Returns True if the queue manager is running."""
        return self._is_running

    def _ensure_running(self) -> None:
        """Raise if not running."""
        if not self._is_running:
            raise RuntimeError("Queue manager is not running. Call start() first.")
        if self._redis is None:
            raise RuntimeError("Redis client not initialized")

    # -------------------------------------------------------------------------
    # Queue Management
    # -------------------------------------------------------------------------

    async def create_queue(self, queue_id: str) -> QueueInfo:
        """Create a new queue on demand."""
        self._ensure_running()
        assert self._redis is not None

        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            # Check if already exists
            exists = await self._redis.exists(meta_key)

            if not exists:
                # Create queue metadata
                await self._redis.hset(
                    meta_key,
                    mapping={
                        "queue_id": queue_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    },
                )

        return await self.get_queue_info(queue_id) or QueueInfo(
            queue_id=queue_id, pending_count=0
        )

    async def get_queue_info(self, queue_id: str) -> Optional[QueueInfo]:
        """Get information about a queue."""
        self._ensure_running()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            # Check if queue exists
            exists = await self._redis.exists(meta_key)
            if not exists:
                return None

            # Get pending count
            pending_count = await self._redis.zcard(queue_key)

            # Get metadata
            meta = await self._redis.hgetall(meta_key)

            # Find current task (in_progress status)
            task_ids = await self._redis.zrange(queue_key, 0, 0)
            current_task_id = None
            is_processing = False

            if task_ids:
                first_task_key = f"{TASK_KEY_PREFIX}{task_ids[0]}"
                status = await self._redis.hget(first_task_key, "status")
                if status == TaskStatus.in_progress.value:
                    current_task_id = task_ids[0]
                    is_processing = True

            created_at = None
            if meta.get("created_at"):
                created_at = datetime.fromisoformat(meta["created_at"])

            return QueueInfo(
                queue_id=queue_id,
                pending_count=pending_count,
                current_task_id=current_task_id,
                is_processing=is_processing,
                created_at=created_at,
            )

    async def delete_queue(self, queue_id: str) -> None:
        """Delete a queue and all its tasks."""
        self._ensure_running()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            # Get all task IDs in the queue
            task_ids = await self._redis.zrange(queue_key, 0, -1)

            # Interrupt all tasks
            for task_id in task_ids:
                await self._interrupt_task_internal(queue_id, task_id)

            # Delete queue metadata
            await self._redis.delete(queue_key, meta_key)

    # -------------------------------------------------------------------------
    # Task Operations
    # -------------------------------------------------------------------------

    async def enqueue(
        self,
        task: ClauqBTMTask,
        executor_name: Optional[str] = None,
    ) -> None:
        """
        Add a task to the back of the queue.

        The task will be sent to Celery for execution. Tasks are processed
        in FIFO order per queue.

        Args:
            task: The task to enqueue
            executor_name: Name of the registered executor function

        Raises:
            RuntimeError: If queue manager is not running or executor not specified
        """
        self._ensure_running()
        assert self._redis is not None

        if executor_name is None:
            raise RuntimeError(
                "executor_name is required for CeleryQueueManager. "
                "Register your executor with default_executor_registry and provide its name."
            )

        # Verify executor is registered
        if self._executor_registry.get(executor_name) is None:
            logger.warning(
                f"Executor '{executor_name}' not found in local registry. "
                "Make sure it's registered in your Celery worker."
            )

        queue_id = task.queue_id
        task_key = f"{TASK_KEY_PREFIX}{task.id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task.id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        # Serialize task
        task_data = _serialize_task(task)
        task_data["executor_name"] = executor_name

        async with self._lock:
            await self.create_queue(queue_id)

            # Store task data
            await self._redis.hset(
                task_key,
                mapping={
                    **{
                        k: (
                            json.dumps(v)
                            if isinstance(v, (dict, list))
                            else str(v) if v is not None else ""
                        )
                        for k, v in task_data.items()
                    },
                },
            )

            # Add to queue (sorted set with timestamp as score for FIFO)
            score = time.time()
            await self._redis.zadd(queue_key, {task.id: score})

            # Send to Celery
            if self._celery_task is not None:
                # Apply async to Celery
                # Use a unique queue per queue_id for ordering
                celery_result = self._celery_task.apply_async(
                    args=[task_data, executor_name, self._redis_url],
                    queue=f"clauq_{queue_id}",
                    task_id=f"clauq_{task.id}",
                )

                # Store Celery task ID mapping
                await self._redis.set(celery_task_key, celery_result.id)

            # Publish queued event
            event = TaskEvent.from_task(TaskEventType.QUEUED, task)
            await self._redis.publish(
                events_channel, json.dumps(_serialize_event(event))
            )

        logger.debug(f"Task {task.id} enqueued to {queue_id}")

    async def get(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        """Get a task by ID."""
        self._ensure_running()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"

        data = await self._redis.hgetall(task_key)
        if not data:
            return None

        # Deserialize JSON fields
        parsed_data: Dict[str, Any] = {}
        for k, v in data.items():
            if k in ("result", "payload") and v:
                try:
                    parsed_data[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    parsed_data[k] = v
            elif v == "":
                parsed_data[k] = None
            else:
                parsed_data[k] = v

        return _deserialize_task(parsed_data)

    async def update(self, queue_id: str, task: ClauqBTMTask) -> None:
        """Update a task in the queue."""
        self._ensure_running()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task.id}"

        # Check if task exists
        exists = await self._redis.exists(task_key)
        if not exists:
            raise RuntimeError(f"Task {task.id} not found")

        # Check current status - only allow updating pending tasks
        current_status = await self._redis.hget(task_key, "status")
        if current_status != TaskStatus.pending.value:
            raise RuntimeError(
                f"Cannot update task with status {current_status}. "
                "Only pending tasks can be updated."
            )

        # Update task data
        task_data = _serialize_task(task)
        await self._redis.hset(
            task_key,
            mapping={
                **{
                    k: (
                        json.dumps(v)
                        if isinstance(v, (dict, list))
                        else str(v) if v is not None else ""
                    )
                    for k, v in task_data.items()
                },
            },
        )

    async def delete(self, queue_id: str, task_id: str) -> None:
        """Remove a pending task from the queue."""
        self._ensure_running()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task_id}"

        async with self._lock:
            # Check current status
            current_status = await self._redis.hget(task_key, "status")
            if current_status == TaskStatus.in_progress.value:
                raise RuntimeError(
                    "Cannot delete a running task. Use interrupt() instead."
                )

            # Remove from queue
            await self._redis.zrem(queue_key, task_id)

            # Revoke Celery task if pending
            celery_task_id = await self._redis.get(celery_task_key)
            if celery_task_id:
                self._celery_app.control.revoke(celery_task_id, terminate=False)

            # Delete task data
            await self._redis.delete(task_key, celery_task_key)

    async def list_tasks(self, queue_id: str) -> List[ClauqBTMTask]:
        """List all tasks in a queue in FIFO order."""
        self._ensure_running()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"

        # Get task IDs in order
        task_ids = await self._redis.zrange(queue_key, 0, -1)

        tasks = []
        for task_id in task_ids:
            task = await self.get(queue_id, task_id)
            if task:
                tasks.append(task)

        return tasks

    async def _interrupt_task_internal(
        self, queue_id: str, task_id: str
    ) -> Optional[ClauqBTMTask]:
        """Internal interrupt implementation (no lock)."""
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task_id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        # Get current task
        task = await self.get(queue_id, task_id)
        if task is None:
            return None

        # Check if already in terminal state
        if task.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        ):
            return task

        # Revoke Celery task
        celery_task_id = await self._redis.get(celery_task_key)
        if celery_task_id:
            self._celery_app.control.revoke(
                celery_task_id,
                terminate=True,
                signal="SIGTERM",
            )

        # Update status
        now = datetime.now(timezone.utc).isoformat()
        await self._redis.hset(
            task_key,
            mapping={
                "status": TaskStatus.interrupted.value,
                "updated_at": now,
            },
        )

        # Remove from queue
        await self._redis.zrem(queue_key, task_id)

        # Set TTL
        await self._redis.expire(task_key, COMPLETED_TASK_TTL)

        # Get updated task
        task = await self.get(queue_id, task_id)

        # Publish event
        if task:
            event = TaskEvent.from_task(TaskEventType.INTERRUPTED, task)
            await self._redis.publish(
                events_channel, json.dumps(_serialize_event(event))
            )

        return task

    async def interrupt(
        self, queue_id: str, task_id: str
    ) -> Optional[ClauqBTMTask]:
        """Interrupt a running or pending task."""
        self._ensure_running()

        async with self._lock:
            return await self._interrupt_task_internal(queue_id, task_id)

    async def wait_for_completion(
        self, queue_id: str, task_id: str, timeout: Optional[float] = None
    ) -> Optional[ClauqBTMTask]:
        """Wait for a task to complete, fail, or be interrupted."""
        self._ensure_running()
        assert self._redis is not None

        # Check if already complete
        task = await self.get(queue_id, task_id)
        if task and task.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        ):
            return task

        # Subscribe to events
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(events_channel)

        start_time = time.time()

        try:
            while True:
                # Check timeout
                if timeout is not None:
                    elapsed = time.time() - start_time
                    if elapsed >= timeout:
                        return None
                    remaining = timeout - elapsed
                else:
                    remaining = 1.0

                # Wait for message
                message = await pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=min(remaining, 1.0),
                )

                if message is not None and message["type"] == "message":
                    data = json.loads(message["data"])
                    if data["task_id"] == task_id and data["event_type"] in (
                        TaskEventType.COMPLETED.value,
                        TaskEventType.FAILED.value,
                        TaskEventType.INTERRUPTED.value,
                    ):
                        return await self.get(queue_id, task_id)

                # Periodically check task state directly
                task = await self.get(queue_id, task_id)
                if task and task.status in (
                    TaskStatus.completed,
                    TaskStatus.failed,
                    TaskStatus.interrupted,
                ):
                    return task

        finally:
            await pubsub.unsubscribe(events_channel)
            await pubsub.close()

    # -------------------------------------------------------------------------
    # Event Subscription
    # -------------------------------------------------------------------------

    @asynccontextmanager
    async def subscribe(self, queue_id: str) -> AsyncIterator[EventSubscription]:
        """Subscribe to events for a specific queue."""
        self._ensure_running()
        assert self._redis is not None

        channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"
        subscription = RedisEventSubscription(self._redis, channel)

        try:
            yield subscription
        finally:
            await subscription.close()

    @asynccontextmanager
    async def subscribe_all(self) -> AsyncIterator[EventSubscription]:
        """Subscribe to events from all queues."""
        self._ensure_running()
        assert self._redis is not None

        subscription = RedisEventSubscription(
            self._redis,
            ALL_EVENTS_CHANNEL,
            pattern=True,
        )

        try:
            yield subscription
        finally:
            await subscription.close()


# -----------------------------------------------------------------------------
# Utility Functions
# -----------------------------------------------------------------------------


def create_celery_app(
    name: str = "clauq",
    broker_url: str = "redis://localhost:6379/0",
    result_backend: str = "redis://localhost:6379/0",
    **kwargs: Any,
) -> "Celery":
    """
    Create a pre-configured Celery app for the queue manager.

    Args:
        name: Name of the Celery app
        broker_url: Message broker URL (Redis recommended)
        result_backend: Result backend URL
        **kwargs: Additional Celery configuration

    Returns:
        Configured Celery app

    Example:
        app = create_celery_app()

        @default_executor_registry.register("my_task")
        async def my_task(task: ClauqBTMTask) -> Any:
            return {"result": task.payload}

        # In a separate terminal:
        # celery -A your_module:app worker -l info -Q clauq_queue_id
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
