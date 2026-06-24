# API Reference

## Auth Endpoints

### `POST /register`

Create a new user account.

**Request:**
```json
{
  "username": "alice",
  "password": "securepass123"
}
```

**Response (201):**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer"
}
```

**Errors:** `409 Conflict` — username already taken

---

### `POST /login`

Authenticate and receive a JWT token.

**Request:**
```json
{
  "username": "alice",
  "password": "securepass123"
}
```

**Response (200):**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIs...",
  "token_type": "bearer"
}
```

**Errors:** `401 Unauthorized` — invalid credentials

---

## Chat Endpoints

All chat endpoints require `Authorization: Bearer <token>` header.

### `POST /chats`

Create a new chat room.

**Request:**
```json
{
  "name": "My Chat Room"
}
```

**Response (200):**
```json
{
  "id": "uuid",
  "name": "My Chat Room"
}
```

---

### `GET /chats`

List all chats the authenticated user is a member of.

**Response (200):**
```json
[
  {
    "id": "uuid",
    "name": "My Chat Room",
    "created_at": "2026-06-24T12:00:00+00:00"
  }
]
```

---

### `POST /chats/{id}/join`

Join an existing chat room.

**Response (200):**
```json
{
  "detail": "Joined chat"
}
```

**Errors:** `404 Not Found` — chat does not exist

---

### `GET /chats/{id}/messages?limit=50`

Retrieve message history for a chat room. Reads from Redis cache (300s TTL) on hit, falls back to shard replica.

**Response (200):**
```json
[
  {
    "id": "uuid",
    "chat_id": "uuid",
    "sender_id": "uuid",
    "sender_username": "alice",
    "body": "Hello!",
    "sent_at": "2026-06-24T12:00:00+00:00"
  }
]
```

**Errors:** `403 Forbidden` — not a member of the chat

---

## WebSocket

### `ws://localhost:8080/ws/{chat_id}?token=<jwt>`

Connect to a chat room for real-time messaging.

- **Authentication:** JWT token as `token` query parameter
- **Send:** `{"body": "Hello!"}`
- **Receive:** `{"id": "uuid", "chat_id": "uuid", "sender_id": "uuid", "sender_username": "alice", "body": "Hello!", "sent_at": "2026-06-24T12:00:00+00:00"}`
- **Close codes:** `4001` — invalid/expired token

---

## Event Endpoints

### `GET /events?limit=50&offset=0`

List recently consumed Kafka events. Event service maintains an in-memory buffer of the last 1000 events.

**Response (200):**
```json
[
  {
    "event_type": "message.sent",
    "version": 1,
    "timestamp": "2026-06-24T12:00:00+00:00",
    "payload": { ... },
    "_meta": {
      "topic": "chat.messages",
      "partition": 0,
      "offset": 42,
      "consumed_at": "2026-06-24T12:00:00+00:00"
    }
  }
]
```

---

### `GET /events/count`

Get total count of events consumed.

**Response (200):**
```json
{
  "total": 142
}
```

---

## Load Balancer Management

### `GET /health`

Load balancer health and status.

**Response (200):**
```json
{
  "status": "healthy",
  "active_strategy": "ConsistentHashingStrategy",
  "available_algorithms": ["rr", "wrr", "sticky", "hash", "lc", "p2c", "lrt", "ra", "adaptive"],
  "backends_count": 3,
  "circuit_breakers": {
    "http://chat-service-1:8000": "closed",
    "http://chat-service-2:8000": "closed",
    "http://chat-service-3:8000": "closed"
  },
  "open_circuits": 0
}
```

---

### `GET /strategies`

List available load balancing algorithms and the active one.

---

### `GET /switch-strategy/{name}`

Switch the active load balancing strategy at runtime.

| Name | Key |
|------|-----|
| Round-Robin | `rr` |
| Weighted Round-Robin | `wrr` |
| Sticky Session | `sticky` |
| Consistent Hashing | `hash` |
| Least Connections | `lc` |
| Power of Two Choices | `p2c` |
| Least Response Time | `lrt` |
| Resource Aware | `ra` |
| Adaptive Feedback | `adaptive` |

**Response (200):**
```json
{
  "status": "success",
  "active_strategy": "RoundRobinStrategy",
  "key": "rr"
}
```

---

### `GET /circuit-breakers`

View circuit breaker state for each backend.

---

### `GET /metrics/prometheus`

Prometheus metrics in text format. Exported metrics:

| Metric | Type | Labels |
|--------|------|--------|
| `lb_requests_total` | Counter | `backend`, `method`, `status_class` |
| `lb_retries_total` | Counter | `backend` |
| `lb_circuit_open_total` | Counter | `backend` |
| `lb_requests_rejected_total` | Counter | `backend` |
| `lb_request_duration_seconds` | Histogram | `backend` |
| `lb_active_connections` | Gauge | `backend` |
| `lb_error_rate` | Gauge | `backend` |
| `lb_avg_latency_seconds` | Gauge | `backend` |
| `lb_cpu_usage_percent` | Gauge | `backend` |
| `lb_circuit_breaker_state` | Gauge | `backend` |

---

## Health Endpoints

| Service | Endpoint |
|---------|----------|
| Gateway | `GET /health` |
| Load Balancer | `GET /health` |
| Auth Service | `GET /health` |
| Chat Service | `GET /health` |
| Event Service | `GET /health` |

All return `{"status": "ok"}` when healthy.
