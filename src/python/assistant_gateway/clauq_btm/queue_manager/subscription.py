"""
Redis pub/sub event subscription for the Celery queue manager.
"""

from __future__ import annotations

import abc
import asyncio
import json
import logging
from typing import Any, AsyncIterator, TYPE_CHECKING

from assistant_gateway.clauq_btm.events import TaskEvent
from assistant_gateway.clauq_btm.queue_manager.serialization import (
    deserialize_event,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub


logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Event Subscription Base Class
# -----------------------------------------------------------------------------


class EventSubscription(abc.ABC):
    """
    Abstract subscription to task events.

    Implementations should support async iteration:
        async for event in subscription:
            print(event)
    """

    @abc.abstractmethod
    def __aiter__(self) -> AsyncIterator[TaskEvent]:
        """Iterate over events."""
        ...

    @abc.abstractmethod
    async def close(self) -> None:
        """Close the subscription and release resources."""
        ...

    async def __aenter__(self) -> "EventSubscription":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


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
                    return deserialize_event(data)

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
