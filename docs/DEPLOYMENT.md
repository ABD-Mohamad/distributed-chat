# Deployment Guide

## Prerequisites

- Docker Desktop 4.x+ with WSL2 backend (Windows) or Docker Engine 24+
- Docker Compose v2
- Python 3.11+ (for local development)
- 8 GB RAM minimum (16 GB recommended for full stack)

---

## Environment Variables

All services read configuration from `.env` file or environment variables.

| Variable | Default | Services | Description |
|----------|---------|----------|-------------|
| `GATEWAY_PORT` | `8080` | gateway | Public HTTP port |
| `POSTGRES_PORT` | `5432` | postgres-auth | Auth DB port |
| `POSTGRES_USER` | `nexuschat` | all Postgres | DB username |
| `POSTGRES_PASSWORD` | `nexuschat` | all Postgres | DB password |
| `POSTGRES_DB` | `nexuschat` | all Postgres | DB name |
| `JWT_SECRET` | `nexuschat-dev-secret-change-in-production` | gateway, auth, chat | JWT signing key |
| `JWT_ALGORITHM` | `HS256` | gateway, auth, chat | JWT algorithm |
| `JWT_EXPIRE_MINUTES` | `1440` | auth, chat | Token expiry |
| `LB_URL` | `http://load-balancer:8000` | gateway | Load balancer REST URL |
| `LB_WS_URL` | `ws://load-balancer:8000` | gateway | Load balancer WS URL |
| `GRPC_PORT` | `50051` | chat-service | gRPC server port |
| `REDIS_URL` | `redis://redis:6379/0` | chat-service | Redis connection string |
| `SHARD0_PRIMARY_URL` | `postgresql+asyncpg://...` | chat-service | Shard 0 primary |
| `SHARD0_REPLICA_URL` | `postgresql+asyncpg://...` | chat-service | Shard 0 replica |
| `SHARD1_PRIMARY_URL` | `postgresql+asyncpg://...` | chat-service | Shard 1 primary |
| `SHARD1_REPLICA_URL` | `postgresql+asyncpg://...` | chat-service | Shard 1 replica |
| `AUTH_DB_URL` | `postgresql+asyncpg://...` | chat-service | Auth DB for username lookups |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | chat, event | Kafka brokers |
| `KAFKA_TOPIC_MESSAGES` | `chat.messages` | chat, event | Message events topic |
| `KAFKA_TOPIC_EVENTS` | `chat.events` | chat, event | System events topic |
| `KAFKA_GROUP_ID` | `event-service` | event-service | Consumer group |
| `RABBITMQ_URL` | `amqp://guest:guest@rabbitmq:5672/` | chat-service | RabbitMQ connection |
| `RABBITMQ_EXCHANGE` | `nexuschat.fanout` | chat-service | Fanout exchange name |
| `RABBITMQ_QUEUE_PREFIX` | `nexuschat.replica` | chat-service | Queue name prefix |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://jaeger:4318` | all | OpenTelemetry OTLP endpoint |
| `OTEL_SERVICE_NAME` | varies | all | Service name for traces |
| `BACKENDS` | `http://chat-service-1:8000,...` | load-balancer | Upstream backend list |
| `CB_FAILURE_THRESHOLD` | `3` | load-balancer | Circuit breaker open threshold |
| `CB_RECOVERY_TIMEOUT` | `30` | load-balancer | Circuit breaker recovery (s) |
| `RETRY_MAX` | `2` | load-balancer | Max retries |
| `RETRY_BASE_DELAY` | `0.05` | load-balancer | Base backoff delay (s) |
| `RETRY_MAX_DELAY` | `5.0` | load-balancer | Max backoff delay (s) |

---

## Docker Compose Quick Start

```bash
# Clone and enter the project
git clone https://github.com/your-org/nexuschat
cd nexuschat

# (Optional) Configure environment
cp .env.example .env

# Start all services
docker compose up -d

# Check status
docker compose ps

# View logs
docker compose logs -f gateway load-balancer

# Stop everything
docker compose down

# Stop and remove volumes (resets DBs)
docker compose down -v
```

---

## Adding Replicas

To scale chat-service replicas:

1. Add a new service block in `docker-compose.yml`:
```yaml
chat-service-4:
  build:
    context: .
    dockerfile: chat-service/Dockerfile
  env_file: .env
  environment:
    GRPC_PORT: "50054"
    OTEL_SERVICE_NAME: chat-service-4
  depends_on: [kafka, rabbitmq, redis, shard0-primary, shard1-primary, postgres-auth, jaeger]
```

2. Update the `BACKENDS` env var in load-balancer:
```yaml
environment:
  BACKENDS: "http://chat-service-1:8000,http://chat-service-2:8000,http://chat-service-3:8000,http://chat-service-4:8000"
```

3. Rebuild and start:
```bash
docker compose up -d --build
```

> The consistent hashing ring rebalances automatically with 50 virtual nodes per backend. Raft majority threshold becomes 3 of 4.

---

## Production Considerations

### TLS Termination

Place Nginx (or any reverse proxy) in front of the gateway. Example at `nginx/nginx.conf`:

```nginx
server {
    listen 443 ssl;
    server_name nexuschat.example.com;

    ssl_certificate     /etc/ssl/certs/nexuschat.crt;
    ssl_certificate_key /etc/ssl/private/nexuschat.key;

    location / {
        proxy_pass http://gateway:8000;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### Secrets Management

- **Never** use the default `JWT_SECRET` in production.
- Use Docker secrets, HashiCorp Vault, or cloud secret manager.
- Set secrets via environment variables or mounted files, never baked into images.

### Resource Limits

Add resource constraints to prevent noisy-neighbor issues:

```yaml
chat-service-1:
  deploy:
    resources:
      limits:
        cpus: "1.0"
        memory: 512M
      reservations:
        cpus: "0.5"
        memory: 256M
```

### Postgres

- Enable TLS for all connections
- Set `wal_level = replica` for streaming replication
- Configure `max_wal_senders` and `max_replication_slots` appropriately
- Regular backups with `pg_dump` or `pgBackRest`

---

## Kubernetes Deployment (Future)

The Docker Compose setup maps naturally to K8s:

| Compose | Kubernetes |
|---------|-----------|
| Service | Deployment + Service |
| Postgres | StatefulSet + PVC |
| Kafka | Strimzi Operator / Confluent Operator |
| RabbitMQ | RabbitMQ Cluster Operator |
| Redis | Redis Helm chart |
| Jaeger | Jaeger Operator |
| Env vars | ConfigMap + Secret |
| Healthchecks | Readiness + Liveness probes |

### Example Service Definition (chat-service)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: chat-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: chat-service
  template:
    metadata:
      labels:
        app: chat-service
    spec:
      containers:
      - name: chat-service
        image: nexuschat/chat-service:latest
        ports:
        - containerPort: 8000
        - containerPort: 50051
        env:
        - name: GRPC_PORT
          value: "50051"
        - name: OTEL_EXPORTER_OTLP_ENDPOINT
          value: "http://jaeger:4318"
        - name: JWT_SECRET
          valueFrom:
            secretKeyRef:
              name: nexuschat-secrets
              key: jwt-secret
```
