import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI

from .config import settings
from .telemetry import setup_telemetry

logger = logging.getLogger(__name__)

_events: list[dict] = []
MAX_EVENTS = 1000


async def consume():
    global _events
    try:
        consumer = AIOKafkaConsumer(
            settings.kafka_topic_messages,
            settings.kafka_topic_events,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id=settings.kafka_group_id,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        await consumer.start()
        logger.info(f"Kafka consumer started, subscribed to {settings.kafka_topic_messages}, {settings.kafka_topic_events}")
        try:
            async for msg in consumer:
                record = msg.value
                record["_meta"] = {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "consumed_at": datetime.now(timezone.utc).isoformat(),
                }
                _events.append(record)
                if len(_events) > MAX_EVENTS:
                    _events = _events[-MAX_EVENTS:]
                logger.info(f"Event consumed: {record.get('event_type')} from {msg.topic}")
        finally:
            await consumer.stop()
    except Exception as e:
        logger.warning(f"Kafka consumer failed: {e}")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    task = asyncio.ensure_future(consume())
    yield
    task.cancel()


app = FastAPI(title="NexusChat Event Service", lifespan=lifespan)

setup_telemetry("event-service", app=app)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/events")
async def list_events(limit: int = 50, offset: int = 0):
    return _events[offset:offset + limit]


@app.get("/events/count")
async def event_count():
    return {"total": len(_events)}
