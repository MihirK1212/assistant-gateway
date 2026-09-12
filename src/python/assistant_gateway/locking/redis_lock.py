from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Optional

from assistant_gateway.locking.base import LockAcquisitionTimeout, LockManager

if TYPE_CHECKING:
    from redis.asyncio import Redis

RELEASE_LUA = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""


class RedisLockManager(LockManager):
    """
    Redis distributed lock manager.
    - get lock: use SET NX EX
    - release lock: use Lua script
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        redis_client: Optional["Redis"] = None,
        key_prefix: str = "lock:",
        default_timeout: float = 10.0,
        default_ttl: float = 30.0,
        retry_interval: float = 0.05,
    ) -> None:
        if redis_url is None and redis_client is None:
            raise ValueError("Either redis_url or redis_client must be provided")

        self._redis_url = redis_url
        self._redis = redis_client
        self._owns_client = redis_client is None
        self._key_prefix = key_prefix
        self._default_timeout = default_timeout
        self._default_ttl = default_ttl
        self._retry_interval = retry_interval

    async def _get_redis(self) -> "Redis":
        if self._redis is None:
            import redis.asyncio as aioredis

            assert self._redis_url is not None
            self._redis = aioredis.from_url(
                self._redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
        return self._redis

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
    ) -> AsyncIterator[None]:
        effective_timeout = timeout if timeout is not None else self._default_timeout
        effective_ttl = ttl if ttl is not None else self._default_ttl

        redis = await self._get_redis()
        full_key = f"{self._key_prefix}{key}"
        token = str(uuid.uuid4())

        start_time = time.monotonic()
        acquired = False

        while True:
            # set key with TTL in milliseconds only if key does not exist (nx=True)
            res = await redis.set(
                full_key,
                token,
                nx=True,
                px=int(effective_ttl * 1000),
            )
            if res:
                acquired = True
                break

            elapsed = time.monotonic() - start_time
            if effective_timeout is not None and elapsed >= effective_timeout:
                break

            remaining = (effective_timeout - elapsed) if effective_timeout is not None else self._retry_interval
            await asyncio.sleep(min(self._retry_interval, max(0.001, remaining)))

        if not acquired:
            raise LockAcquisitionTimeout(
                f"Timed out after {effective_timeout}s waiting for redis lock on '{key}'"
            )

        try:
            yield
        finally:
            try:
                await redis.eval(RELEASE_LUA, 1, full_key, token)
            except Exception:
                pass

    async def close(self) -> None:
        if self._owns_client and self._redis is not None:
            await self._redis.close()
            self._redis = None