"""
circuit_breaker.py — Circuit Breaker Pattern implementation.

States:
  CLOSED   → normal operation; all requests pass through.
  OPEN     → backend considered down; requests rejected immediately (fail-fast).
  HALF_OPEN→ recovery probe; one request allowed through to test backend health.

Transitions:
  CLOSED  → OPEN      : failure_count >= failure_threshold
  OPEN    → HALF_OPEN : recovery_timeout seconds have elapsed
  HALF_OPEN→ CLOSED   : probe request succeeded
  HALF_OPEN→ OPEN     : probe request failed (restart timeout)
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int   = 3,
        recovery_timeout:  float = 30.0,
        name: str = "",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout  = recovery_timeout
        self.name              = name

        self._state:             CircuitState    = CircuitState.CLOSED
        self._failure_count:     int             = 0
        self._last_failure_time: Optional[float] = None
        self._opened_at:         Optional[float] = None
        self._lock               = asyncio.Lock()

    # ── Read-only helpers (safe to call without lock in asyncio context) ──────

    @property
    def state(self) -> CircuitState:
        return self._state

    def is_available(self) -> bool:
        """
        Non-mutating synchronous availability check used for pre-filtering
        backends before strategy selection.
        Returns True if the breaker will *likely* allow a request right now.
        """
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.HALF_OPEN:
            return True
        # OPEN — check if recovery window has passed
        return (
            self._last_failure_time is not None
            and time.time() - self._last_failure_time >= self.recovery_timeout
        )

    def info(self) -> dict:
        """Snapshot of current state for API/dashboard consumption."""
        time_in_state: Optional[float] = None
        if self._opened_at is not None:
            time_in_state = round(time.time() - self._opened_at, 1)

        return {
            "state":             self._state.value,
            "failure_count":     self._failure_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout":  self.recovery_timeout,
            "last_failure_time": self._last_failure_time,
            "seconds_in_state":  time_in_state,
        }

    # ── Mutating async methods ────────────────────────────────────────────────

    async def can_execute(self) -> bool:
        """
        Check whether a request may proceed AND trigger the
        OPEN → HALF_OPEN transition when the recovery timeout expires.
        """
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if (
                    self._last_failure_time is not None
                    and time.time() - self._last_failure_time >= self.recovery_timeout
                ):
                    self._state      = CircuitState.HALF_OPEN
                    self._opened_at  = time.time()
                    logger.info(f"Circuit [{self.name}] → HALF_OPEN (probe allowed)")
                    return True
                return False

            return True  # HALF_OPEN: probe request is allowed

    async def record_success(self) -> None:
        async with self._lock:
            prev = self._state
            self._state         = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at     = None
            if prev != CircuitState.CLOSED:
                logger.info(f"Circuit [{self.name}] → CLOSED (recovered)")

    async def record_failure(self) -> None:
        async with self._lock:
            self._failure_count     += 1
            self._last_failure_time  = time.time()

            if self._state == CircuitState.HALF_OPEN:
                # Probe failed → back to OPEN, restart timeout
                self._state     = CircuitState.OPEN
                self._opened_at = time.time()
                logger.warning(f"Circuit [{self.name}] HALF_OPEN probe failed → OPEN")

            elif (
                self._state == CircuitState.CLOSED
                and self._failure_count >= self.failure_threshold
            ):
                self._state     = CircuitState.OPEN
                self._opened_at = time.time()
                logger.warning(
                    f"Circuit [{self.name}] → OPEN "
                    f"(failures={self._failure_count}/{self.failure_threshold})"
                )
