from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
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
from assistant_gateway.clauq_btm.queue_manager.lua_scripts import (
    UPDATE_TASK_LUA,
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
from assistant_gateway.locking import RedisLockManager

if TYPE_CHECKING:
    from celery import Celery
    from redis.asyncio import Redis

logger = logging.getLogger(__name__)


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
        self._lock_manager: Optional[RedisLockManager] = None
        self._started = False

    @property
    def celery_app(self) -> "Celery":
        return self._celery_app

    @property
    def executor_registry(self) -> ExecutorRegistry:
        return self._executor_registry

    async def enqueue(self, task: ClauqBTMTask) -> None:
        """
        Add a task to the back of the queue and route it to the corresponding
        Celery queue via apply_async(queue=queue_id)
        """
        self._ensure_started()
        assert self._redis is not None
        assert self._lock_manager is not None

        executor_name = task.executor_name
        if executor_name is None:
            raise RuntimeError("executor_name is required for CeleryQueueManager. Set task.executor_name.")

        if executor_name not in self._executor_registry:
            raise KeyError(
                f"Executor '{executor_name}' not found in registry. Make sure it's registered before enqueueing tasks."
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

        async with self._lock_manager.acquire(f"queue:{queue_id}", timeout=10.0):
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
            await self._redis.publish(events_channel, json.dumps(serialize_event(event)))

    async def create_queue(self, queue_id: str) -> str:
        """
        Create Redis metadata for a queue if it doesn't exist yet using HSETNX.
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
                f"Queue '{queue_id}' is not in the configured default_queues. Allowed queues: {self._default_queues}"
            )

        is_default = True
        meta_key = f"{QUEUE_META_PREFIX}{queue_id}"

        created = await self._redis.hsetnx(meta_key, "queue_id", queue_id)
        if created:
            await self._redis.hset(
                meta_key,
                mapping={
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "is_default": "1" if is_default else "0",
                },
            )

        return queue_id

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
        task_data = serialize_task(task)
        mapping = serialize_for_redis_hset(task_data)

        flat_args: List[str] = [TaskStatus.pending.value]
        for k, v in mapping.items():
            flat_args.append(k)
            flat_args.append(str(v))

        res = await self._redis.eval(
            UPDATE_TASK_LUA,
            1,
            task_key,
            *flat_args,
        )

        if res == -1:
            raise RuntimeError(f"Task {task.id} not found")
        elif res == -2:
            current_status = await self._redis.hget(task_key, "status")
            raise RuntimeError(f"Cannot update task with status {current_status}. Only pending tasks can be updated.")

    async def delete(self, queue_id: str, task_id: str) -> None:
        self._ensure_started()
        assert self._redis is not None
        assert self._lock_manager is not None

        task_key = f"{TASK_KEY_PREFIX}{task_id}"
        queue_key = f"{QUEUE_KEY_PREFIX}{queue_id}"
        celery_task_key = f"{CELERY_TASK_PREFIX}{task_id}"

        async with self._lock_manager.acquire(f"task:{task_id}", timeout=10.0):
            current_status = await self._redis.hget(task_key, "status")
            if current_status == TaskStatus.in_progress.value:
                raise RuntimeError("Cannot delete a running task. Use interrupt() instead.")

            await self._redis.zrem(queue_key, task_id)

            celery_task_id = await self._redis.get(celery_task_key)
            if celery_task_id:
                self._celery_app.control.revoke(celery_task_id, terminate=False)

            await self._redis.delete(task_key, celery_task_key)

    async def interrupt(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
        self._ensure_started()
        assert self._lock_manager is not None

        async with self._lock_manager.acquire(f"task:{task_id}", timeout=10.0):
            return await self._interrupt_task_internal(queue_id, task_id)

    async def _interrupt_task_internal(self, queue_id: str, task_id: str) -> Optional[ClauqBTMTask]:
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
            await self._redis.publish(events_channel, json.dumps(serialize_event(event)))

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
                "redis[async] is required for CeleryQueueManager. Install it with: pip install redis[async]"
            )

        self._redis = aioredis.from_url(
            self._redis_url,
            encoding="utf-8",
            decode_responses=True,
        )

        await self._redis.ping()

        self._lock_manager = RedisLockManager(redis_client=self._redis)

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

        self._lock_manager = None

    async def __aenter__(self) -> "CeleryQueueManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
