"""
strategies.py — All 10 load balancing algorithm implementations.

Each strategy inherits from LBStrategy (Strategy Pattern).
Algorithms are grouped into:
  - Static  : RoundRobin, WeightedRoundRobin, StickySession, ConsistentHashing
  - Dynamic : LeastConnections, PowerOfTwoChoices, LeastResponseTime,
              ResourceAware, AdaptiveFeedback, JoinIdleQueue
"""

import asyncio
import bisect
import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract Base
# ─────────────────────────────────────────────────────────────────────────────

class LBStrategy(ABC):
    """Abstract base for all load balancing strategies."""

    @abstractmethod
    async def select(
        self,
        backends: list[str],
        metrics: dict[str, dict] | None = None,
        **kwargs,          # session_id, request_key, etc.
    ) -> str:
        """Select and return a target backend URL."""


# ─────────────────────────────────────────────────────────────────────────────
# 1. Round-Robin
# ─────────────────────────────────────────────────────────────────────────────

class RoundRobinStrategy(LBStrategy):
    """Cyclic distribution — backend = backends[index % N]."""

    def __init__(self) -> None:
        self._index = 0
        self._lock = asyncio.Lock()

    async def select(self, backends, metrics=None, **kwargs) -> str:
        async with self._lock:
            server = backends[self._index % len(backends)]
            self._index += 1
            return server


# ─────────────────────────────────────────────────────────────────────────────
# 2. Weighted Round-Robin  (smooth / nginx-style)
# ─────────────────────────────────────────────────────────────────────────────

class WeightedRoundRobinStrategy(LBStrategy):
    """
    Smooth Weighted Round-Robin (used by nginx).
    Default weights: [3, 2, 1] → backend-1 gets 3x traffic vs backend-3.
    Backends beyond the weights list receive weight 1.
    """

    DEFAULT_WEIGHTS: list[int] = [3, 2, 1]

    def __init__(self, weights: list[int] | None = None) -> None:
        self._base_weights = weights or self.DEFAULT_WEIGHTS
        self._current_weights: list[int] = []
        self._lock = asyncio.Lock()

    def _weights_for(self, n: int) -> list[int]:
        """Pad / trim base weights to length n."""
        w = list(self._base_weights[:n])
        while len(w) < n:
            w.append(1)
        return w

    async def select(self, backends, metrics=None, **kwargs) -> str:
        async with self._lock:
            n = len(backends)
            weights = self._weights_for(n)

            # Re-initialise current_weights if backend list length changed.
            if len(self._current_weights) != n:
                self._current_weights = [0] * n

            total = sum(weights)
            for i in range(n):
                self._current_weights[i] += weights[i]

            best = max(range(n), key=lambda i: self._current_weights[i])
            self._current_weights[best] -= total
            return backends[best]


# ─────────────────────────────────────────────────────────────────────────────
# 3. Sticky Session
# ─────────────────────────────────────────────────────────────────────────────

class StickySessionStrategy(LBStrategy):
    """
    Session affinity via session_id → server mapping with TTL.

    New sessions are assigned by hashing the session_id (NOT sequential
    round-robin). This guarantees:
      1. The same session_id always maps to the same backend.
      2. Different session_ids distribute evenly across backends.
      3. Idempotent: parallel requests for the same new session compute
         the same backend without needing extra locking coordination.

    Falls back to round-robin when no session_id is present.
    """

    SESSION_TTL: int = 300  # seconds

    def __init__(self) -> None:
        self._sessions: dict[str, tuple] = {}   # session_id → (backend, ts)
        self._rr_index: int = 0                 # fallback for sessionless requests
        self._lock = asyncio.Lock()

    def _hash_backend(self, session_id: str, backends: list) -> str:
        """Derive backend index from session_id MD5 hash (deterministic)."""
        h = int(hashlib.md5(session_id.encode()).hexdigest(), 16)
        return backends[h % len(backends)]

    async def select(self, backends, metrics=None, **kwargs) -> str:
        session_id: str | None = kwargs.get("session_id")
        now = time.time()

        async with self._lock:
            if session_id:
                entry = self._sessions.get(session_id)
                if entry:
                    backend, ts = entry
                    # Return cached backend if alive AND still available.
                    if now - ts < self.SESSION_TTL and backend in backends:
                        self._sessions[session_id] = (backend, now)
                        return backend

                # New or expired session — assign via session_id hash.
                backend = self._hash_backend(session_id, backends)
                self._sessions[session_id] = (backend, now)
                logger.debug(f"StickySession: {session_id!r} → {backend}")
                return backend

            # No session_id — plain round-robin fallback.
            backend = backends[self._rr_index % len(backends)]
            self._rr_index += 1
            return backend


# ─────────────────────────────────────────────────────────────────────────────
# 4. Consistent Hashing
# ─────────────────────────────────────────────────────────────────────────────

class ConsistentHashingStrategy(LBStrategy):
    """
    Hash ring with 50 virtual nodes per server.
    Same request_key → same server (deterministic).
    Falls back to MD5 hash of the request path when no key is provided.
    """

    VNODES: int = 50

    def __init__(self) -> None:
        self._ring: dict[int, str] = {}
        self._sorted_keys: list[int] = []
        self._last_backends: list[str] = []
        self._lock = asyncio.Lock()

    def _build_ring(self, backends: list[str]) -> None:
        self._ring = {}
        for backend in backends:
            for i in range(self.VNODES):
                key_str = f"{backend}#{i}"
                h = int(hashlib.md5(key_str.encode()).hexdigest(), 16)
                self._ring[h] = backend
        self._sorted_keys = sorted(self._ring)

    async def select(self, backends, metrics=None, **kwargs) -> str:
        request_key: str = kwargs.get("request_key", "default")

        async with self._lock:
            if backends != self._last_backends:
                self._build_ring(backends)
                self._last_backends = list(backends)

            if not self._sorted_keys:
                return backends[0]

            h = int(hashlib.md5(request_key.encode()).hexdigest(), 16)
            idx = bisect.bisect_left(self._sorted_keys, h) % len(self._sorted_keys)
            return self._ring[self._sorted_keys[idx]]


# ─────────────────────────────────────────────────────────────────────────────
# 5. Least Connections
# ─────────────────────────────────────────────────────────────────────────────

class LeastConnectionsStrategy(LBStrategy):
    """Route to the server with the fewest active_connections."""

    async def select(self, backends, metrics=None, **kwargs) -> str:
        if not metrics:
            return random.choice(backends)
        return min(backends, key=lambda b: metrics.get(b, {}).get("active_connections", 0))


# ─────────────────────────────────────────────────────────────────────────────
# 6. Power of Two Choices  (O(1) approximation of least-loaded)
# ─────────────────────────────────────────────────────────────────────────────

class PowerOfTwoChoicesStrategy(LBStrategy):
    """
    Randomly sample 2 servers and route to the less loaded one.
    Achieves near-optimal load distribution in O(1).
    """

    async def select(self, backends, metrics=None, **kwargs) -> str:
        if len(backends) < 2:
            return backends[0]

        a, b = random.sample(backends, 2)
        if not metrics:
            return random.choice([a, b])

        a_conn = metrics.get(a, {}).get("active_connections", 0)
        b_conn = metrics.get(b, {}).get("active_connections", 0)
        return a if a_conn <= b_conn else b


# ─────────────────────────────────────────────────────────────────────────────
# 7. Least Response Time
# ─────────────────────────────────────────────────────────────────────────────

class LeastResponseTimeStrategy(LBStrategy):
    """Route to the server with the lowest exponentially-smoothed avg_latency."""

    async def select(self, backends, metrics=None, **kwargs) -> str:
        if not metrics:
            return random.choice(backends)
        return min(backends, key=lambda b: metrics.get(b, {}).get("avg_latency", float("inf")))


# ─────────────────────────────────────────────────────────────────────────────
# 8. Resource Aware
# ─────────────────────────────────────────────────────────────────────────────

class ResourceAwareStrategy(LBStrategy):
    """Route to the server reporting the lowest cpu_usage metric."""

    async def select(self, backends, metrics=None, **kwargs) -> str:
        if not metrics:
            return random.choice(backends)
        return min(backends, key=lambda b: metrics.get(b, {}).get("cpu_usage", 0))


# ─────────────────────────────────────────────────────────────────────────────
# 9. Adaptive Feedback
# ─────────────────────────────────────────────────────────────────────────────

class AdaptiveFeedbackStrategy(LBStrategy):
    """
    Dynamically adjusts server weights based on error_rate and avg_latency.
    Penalises struggling servers and rewards healthy ones via exponential
    smoothing of a composite score.
    """

    def __init__(self) -> None:
        self._weights: dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def select(self, backends, metrics=None, **kwargs) -> str:
        async with self._lock:
            # Initialise weights for new backends.
            for b in backends:
                self._weights.setdefault(b, 1.0)

            if metrics:
                for b in backends:
                    m = metrics.get(b, {})
                    error_rate = m.get("error_rate", 0.0)
                    avg_latency = m.get("avg_latency", 0.1)

                    # Lower score = worse server.
                    score = 1.0 / (1.0 + error_rate * 5.0 + avg_latency * 2.0)
                    # Smooth weight update (blend old weight with new score).
                    self._weights[b] = 0.7 * self._weights[b] + 0.3 * score
                    self._weights[b] = max(0.01, self._weights[b])  # floor

            # Weighted random selection.
            total = sum(self._weights.get(b, 1.0) for b in backends)
            r = random.uniform(0.0, total)
            cumulative = 0.0
            for b in backends:
                cumulative += self._weights.get(b, 1.0)
                if r <= cumulative:
                    return b
            return backends[-1]


# ─────────────────────────────────────────────────────────────────────────────
# 10. Join Idle Queue
# ─────────────────────────────────────────────────────────────────────────────

class JoinIdleQueueStrategy(LBStrategy):
    """
    Only routes to servers that are in the idle set.
    main.py calls mark_busy() before dispatch and mark_idle() on completion
    to keep the idle set accurate.
    Falls back to random selection when all servers are busy.
    """

    def __init__(self) -> None:
        self._idle: set = set()
        self._lock = asyncio.Lock()

    async def mark_idle(self, backend: str) -> None:
        async with self._lock:
            self._idle.add(backend)

    async def mark_busy(self, backend: str) -> None:
        async with self._lock:
            self._idle.discard(backend)

    async def select(self, backends, metrics=None, **kwargs) -> str:
        async with self._lock:
            # Bootstrap: treat every server as idle on first call.
            if not self._idle:
                self._idle = set(backends)

            available = [b for b in backends if b in self._idle]

            if not available:
                logger.warning("JIQ: all servers busy — falling back to random selection")
                return random.choice(backends)

            chosen = random.choice(available)
            # mark_busy is called explicitly by the proxy *before* select in JIQ,
            # but we guard here too so direct unit-test calls behave correctly.
            self._idle.discard(chosen)
            return chosen
