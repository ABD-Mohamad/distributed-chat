"""
retry_policy.py — Retry with Exponential Backoff + Jitter.

Only retries on **network-level** errors (timeout, connection refused).
HTTP 5xx responses are NOT retried here — the Circuit Breaker handles
repeated backend failures at a higher level.

Idempotency rule:
  Only safe HTTP methods (GET, HEAD, OPTIONS) are retried by default.
  POST/PUT/DELETE may cause duplicate side-effects if retried blindly.
"""

import asyncio
import logging
import random
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Methods considered safe to retry automatically.
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class RetryPolicy:
    def __init__(
        self,
        max_retries:      int   = 2,
        base_delay:       float = 0.05,   # seconds
        max_delay:        float = 5.0,    # seconds
        jitter:           float = 0.1,    # max random seconds added per attempt
        retryable_methods: frozenset = SAFE_METHODS,
    ) -> None:
        self.max_retries       = max_retries
        self.base_delay        = base_delay
        self.max_delay         = max_delay
        self.jitter            = jitter
        self.retryable_methods = frozenset(m.upper() for m in retryable_methods)

    def is_retryable(self, method: str) -> bool:
        """Return True if this HTTP method may be safely retried."""
        return method.upper() in self.retryable_methods

    def _delay_for(self, attempt: int) -> float:
        """Exponential backoff: base * 2^attempt + uniform jitter."""
        backoff = self.base_delay * (2 ** attempt)
        return min(backoff + random.uniform(0.0, self.jitter), self.max_delay)

    async def execute(
        self, func: Callable[[], Any]
    ) -> tuple[Any, int]:
        """
        Execute `func` (a zero-argument async callable) with automatic retries.

        Returns:
            (result, attempts_used)  — attempts_used = 0 means first try succeeded.

        Raises:
            The last exception if all retries are exhausted.
        """
        last_exc: Exception = RuntimeError("execute() called with max_retries=0")

        for attempt in range(self.max_retries + 1):
            try:
                result = await func()
                if attempt > 0:
                    logger.info(f"Retry succeeded on attempt #{attempt + 1}")
                return result, attempt

            except Exception as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    delay = self._delay_for(attempt)
                    logger.warning(
                        f"Attempt #{attempt + 1} failed ({type(exc).__name__}: {exc}). "
                        f"Retrying in {delay:.3f}s …"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"All {self.max_retries + 1} attempts failed. "
                        f"Last error: {type(exc).__name__}: {exc}"
                    )

        raise last_exc
