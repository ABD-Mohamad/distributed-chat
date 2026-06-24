# Changelog

## [1.0.0] - 2026-06-24

### Added

- **Phase 0:** Project skeleton, Docker Compose baseline, `/health` endpoints for all services.
- **Phase 1:** MVP chat with Postgres persistence, JWT authentication (bcrypt hashing), REST + WebSocket real-time messaging, concurrent message load testing.
- **Phase 2:** Service split into gateway, auth-service, chat-service; gRPC internal communication with Protobuf contracts (`proto/chat.proto`); token-bucket rate limiting at gateway layer; server-streaming gRPC SubscribeMessages.
- **Phase 3:** Load balancing layer with 9 algorithms (Round-Robin, Weighted RR, Sticky Session, Consistent Hashing, Least Connections, Power of Two Choices, Least Response Time, Resource Aware, Adaptive Feedback); circuit breaker with CLOSED/OPEN/HALF_OPEN states; retry policy with exponential backoff + jitter; Postgres sharding (2 shards, MD5 consistent hash); streaming replication per shard (primary + replica); Redis cache-aside with LRU eviction (300s TTL, `allkeys-lru` policy).
- **Phase 4:** Kafka event streaming (chat.messages and chat.events topics); RabbitMQ fan-out exchange for cross-replica message broadcast; durable event publishing with `DeliveryMode.PERSISTENT`; event-service Kafka consumer with in-memory event buffer.
- **Phase 5:** OpenTelemetry distributed tracing exported via OTLP/HTTP to Jaeger (all services instrumented: FastAPI, gRPC server, SQLAlchemy, Redis client, aio-pika, HTTPX); structured JSON logging with trace_id/span_id context enrichment across all services; Prometheus metrics from load balancer (request counts, latency histogram, circuit breaker state, active connections, error rates).
