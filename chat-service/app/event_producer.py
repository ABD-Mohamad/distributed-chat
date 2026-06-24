import json
import logging
from datetime import UTC

from aiokafka import AIOKafkaProducer

from .config import settings

logger = logging.getLogger(__name__)

kafka_producer: AIOKafkaProducer | None = None


async def init_kafka():
    global kafka_producer
    try:
        kafka_producer = AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await kafka_producer.start()
        logger.info("Kafka producer started")
    except Exception as e:
        logger.warning(f"Kafka unavailable: {e}")
        kafka_producer = None


async def close_kafka():
    global kafka_producer
    if kafka_producer:
        await kafka_producer.stop()
        kafka_producer = None


async def publish_event(event_type: str, payload: dict):
    if not kafka_producer:
        return
    topic = settings.kafka_topic_messages if event_type == "message.sent" else settings.kafka_topic_events
    try:
        from datetime import datetime
        record = {
            "event_type": event_type,
            "version": 1,
            "timestamp": datetime.now(UTC).isoformat(),
            "payload": payload,
        }
        await kafka_producer.send(topic, record)
        logger.debug(f"Published {event_type} to {topic}")
    except Exception as e:
        logger.warning(f"Kafka publish failed: {e}")
