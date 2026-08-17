from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    List,
    Optional,
)

from assistant_gateway.clauq_btm.events import TaskEvent, TaskEventType
from assistant_gateway.clauq_btm.executor_registry import ExecutorRegistry
from assistant_gateway.clauq_btm.queue_manager.celery_task import (
    create_celery_task,
)
from assistant_gateway.clauq_btm.queue_manager.constants import (
    ALL_EVENTS_CHANNEL,
    CELERY_TASK_PREFIX,
    COMPLETED_TASK_TTL,
    EVENTS_CHANNEL_PREFIX,
    QUEUE_KEY_PREFIX,
    QUEUE_META_PREFIX,
    TASK_KEY_PREFIX,
)
from assistant_gateway.clauq_btm.queue_manager.serialization import (
    deserialize_task,
    serialize_event,
    serialize_for_redis_hset,
    serialize_task,
)
from assistant_gateway.clauq_btm.queue_manager.subscription import (
    EventSubscription,
    RedisEventSubscription,
)
from assistant_gateway.clauq_btm.schemas import ClauqBTMTask, TaskStatus

if TYPE_CHECKING:
    from celery import Celery
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


@dataclass
class QueueInfo:
    queue_id: str
    pending_count: int
    current_task_id: Optional[str] = None
    is_processing: bool = False
    created_at: Optional[datetime] = None
    is_default: bool = False


class CeleryQueueManager:
    """
    Distributed task queue manager using Celery and Redis.

    IMPORTANT: Executors must be registered in the executor_registry before
    tasks are enqueued. Both API servers and workers must have access to the
    same registered executors.
    """

    def __init__(
        self,
        celery_app: "Celery",
        executor_registry: ExecutorRegistry,
        redis_url: str,
        default_queues: Optional[List[str]] = None,
    ) -> None:
        self._celery_app = celery_app
        self._redis_url = redis_url
        self._executor_registry = executor_registry
        self._default_queues: List[str] = list(default_queues or [])

        # celery task that executes a given "executor_name" from the executor_registry
        self._celery_task = create_celery_task(celery_app, self._executor_registry)

        self._redis: Optional["Redis"] = None
        self._started = False
        self._lock = asyncio.Lock() # TODO: check if centralized locking is needed

    @property
    def celery_app(self) -> "Celery":
        return self._celery_app

    @property
    def executor_registry(self) -> ExecutorRegistry:
        return self._executor_registry

    async def enqueue(
        self,
        task: ClauqBTMTask
    ) -> None:
        """
        Add a task to the back of the queue and route it to the corresponding
        Celery queue via apply_async(queue=queue_id)
        """
        self._ensure_started()
        assert self._redis is not None

        executor_name = task.executor_name
        if executor_name is None:
            raise RuntimeError(
                "executor_name is required for CeleryQueueManager. "
                "Set task.executor_name."
            )

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

        task.executor_name = executor_name

        task_data = serialize_task(task)
        task_data["executor_name"] = executor_name

        if self._default_queues:
            await self.create_queue(queue_id)

        async with self._lock:
            await self._redis.hset(
                task_key,
                mapping=serialize_for_redis_hset(task_data),
            )

            score = time.time()
            await self._redis.zadd(queue_key, {task.id: score})

            if self._celery_task is not None:
                apply_kwargs: Dict[str, Any] = {
                    "args": [task_data, executor_name, self._redis_url],
                    "task_id": f"clauq_{task.id}",
                }
                if self._default_queues:
                    apply_kwargs["queue"] = queue_id

                celery_result = self._celery_task.apply_async(**apply_kwargs)

                await self._redis.set(celery_task_key, celery_result.id)

            event = TaskEvent.from_task(TaskEventType.QUEUED, task)
            await self._redis.publish(
                events_channel, json.dumps(serialize_event(event))
            )

    async def create_queue(self, queue_id: str) -> QueueInfo:
        """
        Create Redis metadata for a queue if it doesn't exist yet.
        """
        self._ensure_started()
        assert self._redis is not None

        if not self._default_queues:
            raise ValueError(
                "create_queue() requires default_queues to be configured. "
                "Without default_queues, all tasks route to the single default "
                "Celery queue and named queue management is not available."
            )

        if queue_id not in self._default_queues:
            raise ValueError(
                f"Queue '{queue_id}' is not in the configured default_queues. "
                f"Allowed queues: {self._default_queues}"
            )

        is_default = True

        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            exists = await self._redis.exists(meta_key)

            if not exists:
                await self._redis.hset(
                    meta_key,
                    mapping={
                        "queue_id": queue_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "is_default": "1" if is_default else "0",
                    },
                )

        return await self.get_queue_info(queue_id) or QueueInfo(
            queue_id=queue_id, pending_count=0, is_default=is_default
        )

    async def get_queue_info(self, queue_id: str) -> Optional[QueueInfo]:
        self._ensure_started()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            exists = await self._redis.exists(meta_key)
            if not exists:
                return None

            pending_count = await self._redis.zcard(queue_key)
            meta = await self._redis.hgetall(meta_key)

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

            is_default = meta.get("is_default", "0") == "1"

            return QueueInfo(
                queue_id=queue_id,
                pending_count=pending_count,
                current_task_id=current_task_id,
                is_processing=is_processing,
                created_at=created_at,
                is_default=is_default,
            )

    async def delete_queue(self, queue_id: str) -> None:
        self._ensure_started()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        async with self._lock:
            task_ids = await self._redis.zrange(queue_key, 0, -1)

            for task_id in task_ids:
                await self._interrupt_task_internal(queue_id, task_id)

            await self._redis.delete(queue_key, meta_key)

    async def get(self, task_id: str) -> Optional[ClauqBTMTask]:
        self._ensure_started()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"

        data = await self._redis.hgetall(task_key)
        if not data:
            return None

        parsed_data: Dict[str, Any] = {}
        for k, v in data.items():
            if k in ("result", "payload", "metadata") and v:
                try:
                    parsed_data[k] = json.loads(v)
                except (json.JSONDecodeError, TypeError):
                    parsed_data[k] = v
            elif v == "":
                parsed_data[k] = None
            else:
                parsed_data[k] = v

        return deserialize_task(parsed_data)

    async def update(self, task: ClauqBTMTask) -> None:
        self._ensure_started()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task.id}"

        exists = await self._redis.exists(task_key)
        if not exists:
            raise RuntimeError(f"Task {task.id} not found")

        current_status = await self._redis.hget(task_key, "status")
        if current_status != TaskStatus.pending.value:
            raise RuntimeError(
                f"Cannot update task with status {current_status}. "
                "Only pending tasks can be updated."
            )

        task_data = serialize_task(task)
        await self._redis.hset(
            task_key,
            mapping=serialize_for_redis_hset(task_data),
        )

    async def delete(self, queue_id: str, task_id: str) -> None:
        self._ensure_started()
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task_id}"

        async with self._lock:
            current_status = await self._redis.hget(task_key, "status")
            if current_status == TaskStatus.in_progress.value:
                raise RuntimeError(
                    "Cannot delete a running task. Use interrupt() instead."
                )

            await self._redis.zrem(queue_key, task_id)

            celery_task_id = await self._redis.get(celery_task_key)
            if celery_task_id:
                self._celery_app.control.revoke(celery_task_id, terminate=False)

            await self._redis.delete(task_key, celery_task_key)

    async def list_tasks(self, queue_id: str) -> List[ClauqBTMTask]:
        self._ensure_started()
        assert self._redis is not None

        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"

        task_ids = await self._redis.zrange(queue_key, 0, -1)

        tasks = []
        for task_id in task_ids:
            task = await self.get(task_id)
            if task:
                tasks.append(task)

        return tasks

    async def interrupt(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        self._ensure_started()

        async with self._lock:
            return await self._interrupt_task_internal(queue_id, task_id)

    async def _interrupt_task_internal(
        self, queue_id: str, task_id: str
    ) -> Optional[ClauqBTMTask]:
        assert self._redis is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task_id}"
        events_channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"

        task = await self.get(task_id)
        if task is None:
            return None

        if task.status in (
            TaskStatus.completed,
            TaskStatus.failed,
            TaskStatus.interrupted,
        ):
            return task

        celery_task_id = await self._redis.get(celery_task_key)
        if celery_task_id:
            self._celery_app.control.revoke(
                celery_task_id,
                terminate=True,
                signal="SIGTERM",
            )

        now = datetime.now(timezone.utc).isoformat()
        await self._redis.hset(
            task_key,
            mapping={
                "status": TaskStatus.interrupted.value,
                "updated_at": now,
            },
        )

        await self._redis.zrem(queue_key, task_id)
        await self._redis.expire(task_key, COMPLETED_TASK_TTL)

        task = await self.get(task_id)

        if task:
            event = TaskEvent.from_task(TaskEventType.INTERRUPTED, task)
            await self._redis.publish(
                events_channel, json.dumps(serialize_event(event))
            )

        return task

    @asynccontextmanager
    async def subscribe(self, queue_id: str) -> AsyncIterator[EventSubscription]:
        self._ensure_started()
        assert self._redis is not None

        channel = f"{EVENTS_CHANNEL_PREFIX}{queue_id}"
        subscription = RedisEventSubscription(self._redis, channel)

        try:
            yield subscription
        finally:
            await subscription.close()

    @asynccontextmanager
    async def subscribe_all(self) -> AsyncIterator[EventSubscription]:
        self._ensure_started()
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

    async def is_healthy(self) -> bool:
        if not self._started or self._redis is None:
            return False

        try:
            await self._redis.ping()
        except Exception:
            return False

        try:
            inspector = self._celery_app.control.inspect(timeout=1.0)
            pong = await asyncio.to_thread(inspector.ping)
            return bool(pong)
        except Exception:
            return False

    def _ensure_started(self) -> None:
        if not self._started:
            raise RuntimeError("Queue manager is not running. Call start() first.")
        if self._redis is None:
            raise RuntimeError("Redis client not initialized")

    async def start(self) -> None:
        if self._started:
            return

        try:
            import redis.asyncio as aioredis
        except ImportError:
            raise ImportError(
                "redis[async] is required for CeleryQueueManager. "
                "Install it with: pip install redis[async]"
            )

        self._redis = aioredis.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=True,
        )

        await self._redis.ping()

        self._started = True

        # Create Redis metadata for all default queues on startup.
        for queue_id in self._default_queues:
            await self.create_queue(queue_id)

    async def stop(self) -> None:
        if not self._started:
            return

        self._started = False

        if self._redis is not None:
            await self._redis.close()
            self._redis = None

    async def __aenter__(self) -> "CeleryQueueManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
