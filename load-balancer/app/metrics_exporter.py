"""
metrics_exporter.py — Prometheus metrics definitions and helpers.

Exposes load balancer internals in Prometheus text format so that
external tools (Grafana, Alertmanager) can scrape and alert on them.

Metric naming convention: lb_<name>_<unit>
All metrics use a custom CollectorRegistry to avoid "already registered"
errors on hot-reload / test runs.
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

# ── Custom registry (avoids conflicts on hot-reload) ──────────────────────────
REGISTRY = CollectorRegistry(auto_describe=True)

# ── Counters ──────────────────────────────────────────────────────────────────

requests_total = Counter(
    "lb_requests_total",
    "Total HTTP requests proxied by the load balancer.",
    ["backend", "method", "status_class"],   # status_class: 2xx / 3xx / 4xx / 5xx / err
    registry=REGISTRY,
)

retries_total = Counter(
    "lb_retries_total",
    "Total retry attempts triggered by the retry policy.",
    ["backend"],
    registry=REGISTRY,
)

circuit_open_total = Counter(
    "lb_circuit_open_total",
    "Total number of times a circuit breaker transitioned to OPEN.",
    ["backend"],
    registry=REGISTRY,
)

requests_rejected_total = Counter(
    "lb_requests_rejected_total",
    "Requests rejected because the circuit breaker was OPEN.",
    ["backend"],
    registry=REGISTRY,
)

# ── Histograms ────────────────────────────────────────────────────────────────

request_duration_seconds = Histogram(
    "lb_request_duration_seconds",
    "End-to-end request latency observed by the proxy (seconds).",
    ["backend"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    registry=REGISTRY,
)

# ── Gauges ────────────────────────────────────────────────────────────────────

active_connections = Gauge(
    "lb_active_connections",
    "Number of in-flight requests currently open to each backend.",
    ["backend"],
    registry=REGISTRY,
)

error_rate = Gauge(
    "lb_error_rate",
    "Exponentially-smoothed error rate for each backend (0.0–1.0).",
    ["backend"],
    registry=REGISTRY,
)

avg_latency_seconds = Gauge(
    "lb_avg_latency_seconds",
    "Exponentially-smoothed average latency for each backend (seconds).",
    ["backend"],
    registry=REGISTRY,
)

cpu_usage_percent = Gauge(
    "lb_cpu_usage_percent",
    "Simulated CPU usage for each backend (active_connections × 15, capped at 100).",
    ["backend"],
    registry=REGISTRY,
)

circuit_breaker_state = Gauge(
    "lb_circuit_breaker_state",
    "Circuit breaker state: 0=CLOSED, 1=OPEN, 2=HALF_OPEN.",
    ["backend"],
    registry=REGISTRY,
)

# ── State → numeric mapping ───────────────────────────────────────────────────

_STATE_NUM = {"closed": 0, "open": 1, "half_open": 2}


# ── Helpers called by main.py ─────────────────────────────────────────────────

def status_class(code: int) -> str:
    """Map an HTTP status code to its class string (e.g. 200 → '2xx')."""
    return f"{code // 100}xx"


def sync_gauges(json_metrics: dict, cb_info: dict) -> None:
    """
    Update Prometheus gauges from the in-memory JSON metrics dict and
    circuit breaker info dict.  Called once per completed proxy request.
    """
    for backend, m in json_metrics.items():
        lbl = {"backend": backend}
        active_connections.labels(**lbl).set(m.get("active_connections", 0))
        error_rate.labels(**lbl).set(m.get("error_rate", 0.0))
        avg_latency_seconds.labels(**lbl).set(m.get("avg_latency", 0.0))
        cpu_usage_percent.labels(**lbl).set(m.get("cpu_usage", 0))

    for backend, cb in cb_info.items():
        state_val = _STATE_NUM.get(cb.get("state", "closed"), 0)
        circuit_breaker_state.labels(backend=backend).set(state_val)


def prometheus_output() -> bytes:
    """Return the Prometheus text exposition format as bytes."""
    return generate_latest(REGISTRY)


PROMETHEUS_CONTENT_TYPE: str = CONTENT_TYPE_LATEST
