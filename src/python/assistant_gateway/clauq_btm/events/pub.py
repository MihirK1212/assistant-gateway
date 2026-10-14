from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Dict, Optional, Union

from assistant_gateway.clauq_btm.events.schema import TaskEvent
from assistant_gateway.clauq_btm.events.serialization import serialize_event

if TYPE_CHECKING:
    import redis
    import redis.asyncio as aioredis

REQUIRED_EVENT_KEYS = {"event_type", "task_id", "queue_id", "status", "timestamp"}


def _serialize_event_payload(event: Union[TaskEvent, Dict[str, Any]]) -> str:
    if isinstance(event, TaskEvent):
        return json.dumps(serialize_event(event))
    elif isinstance(event, dict):
        missing_keys = REQUIRED_EVENT_KEYS - event.keys()
        if missing_keys:
            raise ValueError(f"Event dictionary missing required keys: {missing_keys}")
        return json.dumps(event)
    else:
        raise TypeError(f"Expected TaskEvent or dict, got {type(event).__name__}")


class EventPublisher:
    """Publishes task events to Redis channels.

    Accepts either a live Redis client or a Redis URL. When constructed
    with a URL, the client is created lazily on first publish.

    Usage with URL (external consumers):
        publisher = EventPublisher(redis_url="redis://localhost:6379/0")
        await publisher.publish(event, channel)
        await publisher.aclose()

    Usage with client (internal / existing code):
        publisher = EventPublisher(redis=existing_client)
        publisher.publish_sync(event, channel)
    """

    def __init__(
        self,
        *,
        redis: Optional[Union[redis.Redis, aioredis.Redis]] = None,
        redis_url: Optional[str] = None,
    ) -> None:
        if redis is None and redis_url is None:
            raise ValueError("Provide either a redis client or redis_url")
        self._redis: Any = redis
        self._redis_url = redis_url
        self._owns_client = redis is None

    def _get_sync_client(self) -> redis.Redis:
        if self._redis is None:
            import redis

            assert self._redis_url is not None
            self._redis = redis.from_url(self._redis_url)
        return self._redis

    async def _get_async_client(self) -> aioredis.Redis:
        if self._redis is None:
            import redis.asyncio as aioredis

            assert self._redis_url is not None
            self._redis = aioredis.from_url(self._redis_url)
        return self._redis

    async def publish(
        self,
        event: Union[TaskEvent, Dict[str, Any]],
        channel: str,
    ) -> None:
        client = await self._get_async_client()
        payload = _serialize_event_payload(event)
        await client.publish(channel, payload)

    def publish_sync(
        self,
        event: Union[TaskEvent, Dict[str, Any]],
        channel: str,
    ) -> None:
        client = self._get_sync_client()
        payload = _serialize_event_payload(event)
        client.publish(channel, payload)

    async def aclose(self) -> None:
        if self._owns_client and self._redis is not None:
            if hasattr(self._redis, "aclose"):
                await self._redis.aclose()
            elif hasattr(self._redis, "close"):
                self._redis.close()
            self._redis = None

    def close(self) -> None:
        if self._owns_client and self._redis is not None:
            if hasattr(self._redis, "close"):
                self._redis.close()
            self._redis = None

    async def __aenter__(self) -> EventPublisher:
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        await self.aclose()

    def __enter__(self) -> EventPublisher:
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        self.close()

