import logging

import aio_pika

from .config import settings

logger = logging.getLogger(__name__)

_connection: aio_pika.RobustConnection | None = None
_channel: aio_pika.RobustChannel | None = None
_exchange: aio_pika.RobustExchange | None = None
_consumer_tag: str | None = None
_on_message_callback = None


async def init_rabbitmq(consumer_callback=None):
    global _connection, _channel, _exchange, _on_message_callback
    _on_message_callback = consumer_callback
    try:
        _connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        _channel = await _connection.channel()
        _exchange = await _channel.declare_exchange(
            settings.rabbitmq_exchange,
            aio_pika.ExchangeType.FANOUT,
            durable=True,
        )
        logger.info("RabbitMQ connected")
    except Exception as e:
        logger.warning(f"RabbitMQ unavailable: {e}")
        _connection = None


async def start_consumer():
    global _consumer_tag
    if not _channel or not _exchange or not _on_message_callback:
        logger.warning(f"RMQ consumer skipped: chan={_channel is not None}, exch={_exchange is not None}, cb={_on_message_callback is not None}")
        return
    try:
        queue = await _channel.declare_queue(
            f"{settings.rabbitmq_queue_prefix}.{settings.grpc_port}",
            durable=True,
            auto_delete=True,
        )
        await queue.bind(_exchange)
        _consumer_tag = await queue.consume(_on_message_callback)
        logger.info(f"RabbitMQ consumer started on {queue.name}")
    except Exception as e:
        logger.warning(f"RabbitMQ consumer failed: {e}")


async def publish_to_fanout(payload: dict):
    if not _exchange:
        logger.warning("RMQ publish skipped: no exchange")
        return
    try:
        import json
        message = aio_pika.Message(
            body=json.dumps(payload, default=str).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await _exchange.publish(message, routing_key="")
        logger.info(f"RMQ published: chat_id={payload.get('chat_id')}")
    except Exception as e:
        logger.warning(f"RabbitMQ publish failed: {e}")


async def close_rabbitmq():
    global _connection, _channel, _exchange, _consumer_tag
    if _consumer_tag and _channel:
        await _channel.cancel(_consumer_tag)
    if _connection:
        await _connection.close()
    _connection = None
    _channel = None
    _exchange = None
    _consumer_tag = None
