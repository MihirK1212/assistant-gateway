from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Dict, Optional

from assistant_gateway.locking.base import LockAcquisitionTimeout, LockManager


class InMemoryLockManager(LockManager):
    """
    In-memory lock manager.
    - get lock: use asyncio.Lock
    - release lock: use asyncio.Lock.release() 

    The key -> lock mapping itself is protected by a master lock.
    """

    _instance: Optional["InMemoryLockManager"] = None

    @classmethod
    def instance(cls) -> "InMemoryLockManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """For testing only. Resets the singleton instance."""
        cls._instance = None

    def __init__(self, default_timeout: float = 10.0) -> None:
        self._locks: Dict[str, asyncio.Lock] = {}
        self._master_lock = asyncio.Lock()
        self._default_timeout = default_timeout

    async def _get_or_create_lock(self, key: str) -> asyncio.Lock:
        async with self._master_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
    ) -> AsyncIterator[None]:
        effective_timeout = timeout if timeout is not None else self._default_timeout
        lock = await self._get_or_create_lock(key)

        try:
            if effective_timeout is not None and effective_timeout >= 0:
                await asyncio.wait_for(lock.acquire(), timeout=effective_timeout)
            else:
                await lock.acquire()
        except asyncio.TimeoutError:
            raise LockAcquisitionTimeout(
                f"Timed out after {effective_timeout}s waiting for in-memory lock on '{key}'"
            )

        try:
            yield
        finally:
            if lock.locked():
                lock.release()
