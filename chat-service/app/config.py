from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    jwt_secret: str = "nexuschat-dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440
    grpc_port: int = 50051

    redis_url: str = "redis://redis:6379/0"
    cors_origins: str = "*"

    shard0_primary_url: str = "postgresql+asyncpg://nexuschat:nexuschat@shard0-primary:5432/nexuschat"
    shard0_replica_url: str = "postgresql+asyncpg://nexuschat:nexuschat@shard0-replica:5432/nexuschat"
    shard1_primary_url: str = "postgresql+asyncpg://nexuschat:nexuschat@shard1-primary:5432/nexuschat"
    shard1_replica_url: str = "postgresql+asyncpg://nexuschat:nexuschat@shard1-replica:5432/nexuschat"

    auth_db_url: str = "postgresql+asyncpg://nexuschat:nexuschat@postgres-auth:5432/nexuschat"

    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_messages: str = "chat.messages"
    kafka_topic_events: str = "chat.events"

    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    rabbitmq_exchange: str = "nexuschat.fanout"
    rabbitmq_queue_prefix: str = "nexuschat.replica"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
