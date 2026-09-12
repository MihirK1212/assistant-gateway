from __future__ import annotations

import abc
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional


class LockAcquisitionTimeout(Exception):
    pass


class LockManager(abc.ABC):
    """Abstract interface for managing resource locks."""

    @abc.abstractmethod
    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: Optional[float] = None,
        ttl: Optional[float] = None,
    ) -> AsyncIterator[None]:
        """
        Acquire a lock for the given key.
        """
        yield
