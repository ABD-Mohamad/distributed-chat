import asyncio
import logging
import os
import time
from pathlib import Path

START_TIME = time.time()

import httpx
from app.circuit_breaker import CircuitBreaker, CircuitState
from app.metrics_exporter import (
    PROMETHEUS_CONTENT_TYPE,
    circuit_open_total,
    prometheus_output,
    request_duration_seconds,
    requests_rejected_total,
    requests_total,
    retries_total,
    status_class,
    sync_gauges,
)
from app.retry_policy import RetryPolicy
from app.strategies import (
    AdaptiveFeedbackStrategy,
    ConsistentHashingStrategy,
    JoinIdleQueueStrategy,
    LBStrategy,
    LeastConnectionsStrategy,
    LeastResponseTimeStrategy,
    PowerOfTwoChoicesStrategy,
    ResourceAwareStrategy,
    RoundRobinStrategy,
    StickySessionStrategy,
    WeightedRoundRobinStrategy,
)
from app.telemetry import setup_telemetry
from app.ws_proxy import ws_proxy_handler
from fastapi import FastAPI, Query, Request, Response, WebSocket
from fastapi.responses import FileResponse, JSONResponse

logger = logging.getLogger(__name__)

app = FastAPI(title="NexusChat Load Balancer", version="1.0.0")

setup_telemetry(app=app)

_backends_env = os.getenv(
    "BACKENDS",
    "http://chat-service-1:8000,http://chat-service-2:8000,http://chat-service-3:8000",
)
BACKENDS: list[str] = [b.strip() for b in _backends_env.split(",") if b.strip()]
WS_BACKENDS: list[str] = [
    b.replace("http://", "ws://").replace("https://", "wss://")
    for b in BACKENDS
]
logger.info(f"REST backends: {BACKENDS}")
logger.info(f"WS backends: {WS_BACKENDS}")

_strategies: dict[str, LBStrategy] = {
    "rr":       RoundRobinStrategy(),
    "wrr":      WeightedRoundRobinStrategy(),
    "sticky":   StickySessionStrategy(),
    "hash":     ConsistentHashingStrategy(),
    "lc":       LeastConnectionsStrategy(),
    "p2c":      PowerOfTwoChoicesStrategy(),
    "lrt":      LeastResponseTimeStrategy(),
    "ra":       ResourceAwareStrategy(),
    "adaptive": AdaptiveFeedbackStrategy(),
    "jiq":      JoinIdleQueueStrategy(),
}

STRATEGY_DESCRIPTIONS: dict[str, str] = {
    "rr":       "Round-Robin: cyclic distribution",
    "wrr":      "Weighted RR: proportional by capacity (weights 3:2:1)",
    "sticky":   "Sticky Session: session affinity with 5-minute TTL",
    "hash":     "Consistent Hashing: deterministic by X-Request-Key header",
    "lc":       "Least Connections: minimum active connections",
    "p2c":      "Power of Two Choices: sample 2, pick least loaded (O(1))",
    "lrt":      "Least Response Time: lowest avg latency (alpha=0.2 smoothing)",
    "ra":       "Resource Aware: lowest simulated CPU usage",
    "adaptive": "Adaptive Feedback: dynamic weights from error_rate + latency",
    "jiq":      "Join Idle Queue: route only to servers that signalled idle",
}

_active_strategy_name: str = "hash"
_strategy_lock = asyncio.Lock()

CB_FAILURE_THRESHOLD: int   = int(os.getenv("CB_FAILURE_THRESHOLD", "3"))
CB_RECOVERY_TIMEOUT:  float = float(os.getenv("CB_RECOVERY_TIMEOUT", "30"))

_circuit_breakers: dict[str, CircuitBreaker] = {
    b: CircuitBreaker(
        failure_threshold=CB_FAILURE_THRESHOLD,
        recovery_timeout=CB_RECOVERY_TIMEOUT,
        name=b.split("/")[-1],
    )
    for b in BACKENDS
}

_retry_policy = RetryPolicy(
    max_retries=int(os.getenv("RETRY_MAX", "2")),
    base_delay=float(os.getenv("RETRY_BASE_DELAY", "0.05")),
    max_delay=float(os.getenv("RETRY_MAX_DELAY", "5.0")),
)

ALPHA = 0.2


def _blank_metrics() -> dict:
    return {
        "active_connections": 0,
        "avg_latency":        0.0,
        "total_latency":      0.0,
        "request_count":      0,
        "error_rate":         0.0,
        "cpu_usage":          0,
    }


_metrics: dict[str, dict] = {b: _blank_metrics() for b in BACKENDS}
_metrics_lock = asyncio.Lock()


async def _record_start(backend: str) -> None:
    async with _metrics_lock:
        m = _metrics.setdefault(backend, _blank_metrics())
        m["active_connections"] += 1
        m["cpu_usage"] = min(100, m["active_connections"] * 15)


async def _record_end(backend: str, latency: float, success: bool) -> None:
    async with _metrics_lock:
        m = _metrics.setdefault(backend, _blank_metrics())
        m["active_connections"] = max(0, m["active_connections"] - 1)
        m["cpu_usage"]          = min(100, m["active_connections"] * 15)
        m["total_latency"]     += latency
        m["request_count"]     += 1
        m["avg_latency"]        = ALPHA * latency + (1 - ALPHA) * m["avg_latency"]
        if success:
            m["error_rate"] = max(0.0, m["error_rate"] * 0.9)
        else:
            m["error_rate"] = min(1.0, m["error_rate"] + 0.2)


_HOP_BY_HOP = frozenset({
    "host", "content-length", "transfer-encoding",
    "connection", "keep-alive", "proxy-authenticate",
    "proxy-authorization", "te", "trailers", "upgrade",
})


def _sanitize_req_headers(h: dict) -> dict:
    return {k: v for k, v in h.items() if k.lower() not in _HOP_BY_HOP}


def _sanitize_resp_headers(h) -> dict:
    skip = {"transfer-encoding", "connection", "content-encoding"}
    return {k: v for k, v in h.items() if k.lower() not in skip}


async def _current_strategy() -> tuple[str, LBStrategy]:
    async with _strategy_lock:
        name = _active_strategy_name
    return name, _strategies[name]


def _cb_snapshot() -> dict:
    return {b: cb.info() for b, cb in _circuit_breakers.items()}


# ── Management Endpoints ──────────────────────────────────────────────────

@app.get("/health", tags=["Management"])
async def health():
    async with _strategy_lock:
        name = _active_strategy_name
    cb_summary = {b: cb.state.value for b, cb in _circuit_breakers.items()}
    open_count = sum(1 for s in cb_summary.values() if s == "open")
    return {
        "status":               "degraded" if open_count > 0 else "healthy",
        "service":              "load-balancer",
        "version":              "1.0.0",
        "uptime":               round(time.time() - START_TIME, 2),
        "active_strategy":      _strategies[name].__class__.__name__,
        "available_algorithms": list(_strategies.keys()),
        "backends_count":       len(BACKENDS),
        "circuit_breakers":     cb_summary,
        "open_circuits":        open_count,
    }


@app.get("/ready", tags=["Management"])
async def ready():
    return {"status": "ok", "service": "load-balancer"}


@app.get("/live", tags=["Management"])
async def live():
    return {"status": "ok", "service": "load-balancer"}


@app.get("/strategies", tags=["Management"])
async def list_strategies():
    async with _strategy_lock:
        name = _active_strategy_name
    return {"active": name, "available": STRATEGY_DESCRIPTIONS}


@app.get("/metrics", tags=["Management"])
async def get_metrics_json():
    async with _metrics_lock:
        return {b: dict(m) for b, m in _metrics.items()}


@app.get("/metrics/prometheus", tags=["Management"])
async def get_metrics_prometheus():
    async with _metrics_lock:
        m_snap = {b: dict(m) for b, m in _metrics.items()}
    sync_gauges(m_snap, _cb_snapshot())
    return Response(content=prometheus_output(), media_type=PROMETHEUS_CONTENT_TYPE)


@app.get("/circuit-breakers", tags=["Management"])
async def get_circuit_breakers():
    return _cb_snapshot()


@app.get("/switch-strategy/{name}", tags=["Management"])
async def switch_strategy(name: str):
    if name not in _strategies:
        return JSONResponse(status_code=400, content={"error": f"Unknown strategy '{name}'.", "available": list(_strategies.keys())})
    async with _strategy_lock:
        _active_strategy_name = name
    logger.info(f"Strategy switched -> {name} ({_strategies[name].__class__.__name__})")
    return {"status": "success", "active_strategy": _strategies[name].__class__.__name__, "key": name}


@app.get("/dashboard", tags=["Management"], include_in_schema=False)
async def dashboard():
    for candidate in [Path(__file__).parent.parent / "dashboard.html", Path("/app/dashboard.html")]:
        if candidate.exists():
            return FileResponse(str(candidate), media_type="text/html")
    return JSONResponse(status_code=404, content={"error": "dashboard.html not found"})


# ── Catch-All REST Proxy ──────────────────────────────────────────────────

@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"], tags=["Proxy"])
async def proxy(request: Request, path: str):
    strategy_name, strategy = await _current_strategy()
    request_key = request.headers.get("X-Request-Key") or request.url.path

    available = [b for b in BACKENDS if _circuit_breakers[b].is_available()]
    if not available:
        logger.error("All backends unavailable")
        return JSONResponse(status_code=503, content={"error": "All backends unavailable", "details": _cb_snapshot()})

    async with _metrics_lock:
        metrics_snapshot = {b: dict(m) for b, m in _metrics.items()}

    try:
        backend = await strategy.select(available, metrics=metrics_snapshot, request_key=request_key)
    except Exception as exc:
        logger.error(f"Strategy '{strategy_name}' selection failed: {exc}")
        return JSONResponse(status_code=502, content={"error": "Backend selection failed"})

    await _record_start(backend)
    start = time.perf_counter()
    success = False
    retry_count = 0

    try:
        cb = _circuit_breakers[backend]
        if not await cb.can_execute():
            requests_rejected_total.labels(backend=backend).inc()
            logger.warning(f"Request rejected - circuit OPEN for {backend}")
            return JSONResponse(status_code=503, content={"error": f"Circuit breaker OPEN for {backend}. Retry after {cb.recovery_timeout}s."})

        target_url = f"{backend}/{path}"
        if request.url.query:
            target_url += f"?{request.url.query}"

        headers = _sanitize_req_headers(dict(request.headers))
        body = await request.body()

        async def _forward():
            async with httpx.AsyncClient(timeout=10.0) as client:
                return await client.request(method=request.method, url=target_url, headers=headers, content=body)

        if _retry_policy.is_retryable(request.method):
            resp, retry_count = await _retry_policy.execute(_forward)
        else:
            resp = await _forward()

        success = resp.status_code < 500

        if success:
            await cb.record_success()
        else:
            prev_state = cb.state
            await cb.record_failure()
            if cb.state == CircuitState.OPEN and prev_state != CircuitState.OPEN:
                circuit_open_total.labels(backend=backend).inc()

        latency = time.perf_counter() - start
        sc = status_class(resp.status_code)
        requests_total.labels(backend=backend, method=request.method, status_class=sc).inc()
        request_duration_seconds.labels(backend=backend).observe(latency)
        if retry_count > 0:
            retries_total.labels(backend=backend).inc(retry_count)

        logger.info(f"[{strategy_name}] {request.method} /{path} -> {backend} HTTP {resp.status_code}  {latency*1000:.1f}ms" + (f"  retries={retry_count}" if retry_count > 0 else ""))

        resp_headers = _sanitize_resp_headers(dict(resp.headers))
        if retry_count > 0:
            resp_headers["X-Retry-Count"] = str(retry_count)

        return Response(content=resp.content, status_code=resp.status_code, headers=resp_headers)

    except httpx.TimeoutException:
        await cb.record_failure()
        logger.error(f"Timeout reaching {backend}")
        return JSONResponse(status_code=502, content={"error": f"Backend '{backend}' timed out after 10s"})

    except Exception as exc:
        await cb.record_failure()
        logger.error(f"Proxy error -> {backend}: {exc}")
        return JSONResponse(status_code=502, content={"error": str(exc)})

    finally:
        latency = time.perf_counter() - start
        await _record_end(backend, latency, success)
        if strategy_name == "jiq" and isinstance(strategy, JoinIdleQueueStrategy):
            await strategy.mark_idle(backend)


# ── WebSocket Proxy ───────────────────────────────────────────────────────

@app.websocket("/ws/{chat_id}")
async def ws_proxy_route(websocket: WebSocket, chat_id: str, token: str = Query(...)):
    available = [b for b in BACKENDS if _circuit_breakers[b].is_available()]
    if not available:
        await websocket.close(code=1013)
        return
    backend_idx = abs(hash(chat_id)) % len(available)
    backend = WS_BACKENDS[BACKENDS.index(available[backend_idx])]
    await ws_proxy_handler(websocket, backend, chat_id, token)
