"""
Celery-based distributed queue manager implementation.

IMPORTANT: For distributed execution to work correctly:
1. Create an ExecutorRegistry and register all executors at module level
2. Pass the same registry to CeleryQueueManager
3. Both API servers and Celery workers must import the same module
4. This ensures all processes have access to the same executors
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
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
from assistant_gateway.clauq_btm.queue_manager.constants import (
    TASK_KEY_PREFIX,
    QUEUE_KEY_PREFIX,
    QUEUE_META_PREFIX,
    CELERY_TASK_PREFIX,
    EVENTS_CHANNEL_PREFIX,
    ALL_EVENTS_CHANNEL,
    COMPLETED_TASK_TTL,
)
from assistant_gateway.clauq_btm.queue_manager.serialization import (
    serialize_task,
    serialize_event,
    deserialize_task,
    serialize_for_redis_hset,
)
from assistant_gateway.clauq_btm.queue_manager.subscription import (
    EventSubscription,
    RedisEventSubscription,
)
from assistant_gateway.clauq_btm.queue_manager.celery_task import (
    create_celery_task,
)

if TYPE_CHECKING:
    from celery import Celery
    from redis.asyncio import Redis


logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Queue Info
# -----------------------------------------------------------------------------


@dataclass
class QueueInfo:
    """Information about a task queue."""

    queue_id: str
    pending_count: int
    current_task_id: Optional[str] = None
    is_processing: bool = False
    created_at: Optional[datetime] = None


class CeleryQueueManager:
    """
    Distributed task queue manager using Celery and Redis.

    Features:
    - Distributed task execution via Celery workers
    - Persistent task state in Redis
    - FIFO ordering per queue using Redis sorted sets
    - Real-time event subscription via Redis pub/sub
    - Task interruption via Celery revoke

    IMPORTANT: Executors must be registered in the executor_registry before
    tasks are enqueued. Both API servers and workers must have access to the
    same registered executors.

    Setup:
        1. Create an ExecutorRegistry and register executors
        2. Create a Celery app
        3. Create CeleryQueueManager with the registry
        4. Both API servers and workers must import the same module

    Example:
        # In shared_setup.py (imported by both API and workers)
        from celery import Celery
        from assistant_gateway.clauq_btm.queue_manager import CeleryQueueManager
        from assistant_gateway.clauq_btm.executor_registry import ExecutorRegistry

        # Create executor registry and register executors
        executor_registry = ExecutorRegistry()

        @executor_registry.register("process_data")
        async def process_data(task: ClauqBTMTask) -> Any:
            return {"processed": task.payload}

        # Create Celery app and queue manager
        celery_app = Celery('tasks', broker='redis://localhost:6379/0')
        queue_manager = CeleryQueueManager(
            celery_app=celery_app,
            executor_registry=executor_registry,
            redis_url='redis://localhost:6379/0',
        )

        # Run workers with: celery -A shared_setup worker

    Args:
        celery_app: Configured Celery application
        executor_registry: Registry containing executors for task processing
        redis_url: Redis connection URL
    """

    def __init__(
        self,
        celery_app: "Celery",
        executor_registry: ExecutorRegistry,
        redis_url: str,
    ) -> None:
        """
        Initialize the Celery queue manager.

        The Celery task is registered at init time. Executors can be registered
        in the executor_registry after init but before tasks are processed.

        Args:
            celery_app: Configured Celery application
            executor_registry: Registry containing executors for task processing
            redis_url: Redis connection URL
        """
        self._celery_app = celery_app
        self._redis_url = redis_url
        self._executor_registry = executor_registry

        # Register Celery task at init time (critical for distributed execution)
        # This ensures the task is available when workers import this module
        # Executors can still be registered later - the registry is captured by reference
        self._celery_task = create_celery_task(celery_app, self._executor_registry)

        # Redis async client (initialized on start)
        self._redis: Optional["Redis"] = None

        # State
        self._is_running = False

        # Lock for async operations
        self._lock = asyncio.Lock()

    # -------------------------------------------------------------------------
    # Properties
    # -------------------------------------------------------------------------

    @property
    def celery_app(self) -> "Celery":
        """Return the Celery application instance."""
        return self._celery_app

    @property
    def executor_registry(self) -> ExecutorRegistry:
        """Return the executor registry."""
        return self._executor_registry

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the queue manager."""
        if self._is_running:
            return

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

    async def __aenter__(self) -> "CeleryQueueManager":
        """Start the queue manager when entering context."""
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop the queue manager when exiting context."""
        await self.stop()

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
        task: ClauqBTMTask
    ) -> None:
        """
        Add a task to the back of the queue.

        The task will be sent to Celery for execution. Tasks are processed
        in FIFO order per queue.

        Args:
            task: The task to enqueue

        Raises:
            RuntimeError: If queue manager is not running
            KeyError: If executor is not found in registry
        """
        self._ensure_running()
        assert self._redis is not None

        # Use executor_name from task
        executor_name = task.executor_name
        if executor_name is None:
            raise RuntimeError(
                "executor_name is required for CeleryQueueManager. "
                "Set task.executor_name."
            )

        # Verify executor is registered (fail fast)
        if executor_name not in self._executor_registry:
            raise KeyError(
                f"Executor '{executor_name}' not found in registry. "
                "Make sure it's registered before enqueueing tasks."
            )

        queue_id = task.queue_id
        task_key = f"{TASK_KEY_PREFIX}{task.id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task.id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        # Ensure executor_name is set on task
        task.executor_name = executor_name

        # Serialize task
        task_data = serialize_task(task)
        task_data["executor_name"] = executor_name

        async with self._lock:
            await self.create_queue(queue_id)

            # Store task data
            await self._redis.hset(
                task_key,
                mapping=serialize_for_redis_hset(task_data),
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
                events_channel, json.dumps(serialize_event(event))
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

        return deserialize_task(parsed_data)

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
        task_data = serialize_task(task)
        await self._redis.hset(
            task_key,
            mapping=serialize_for_redis_hset(task_data),
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

    async def interrupt(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        """Interrupt a running or pending task."""
        self._ensure_running()

        async with self._lock:
            return await self._interrupt_task_internal(queue_id, task_id)

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
                events_channel, json.dumps(serialize_event(event))
            )

        return task

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
