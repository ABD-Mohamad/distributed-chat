from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    kafka_topic_messages: str = "chat.messages"
    kafka_topic_events: str = "chat.events"
    kafka_group_id: str = "event-service"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
