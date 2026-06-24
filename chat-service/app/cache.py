import json
import logging

import redis.asyncio as aioredis

from .config import settings

logger = logging.getLogger(__name__)

redis_client: aioredis.Redis | None = None
CACHE_TTL = 300


async def init_cache():
    global redis_client
    redis_client = aioredis.Redis.from_url(settings.redis_url, decode_responses=True)
    try:
        await redis_client.ping()
        logger.info("Redis connected")
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        redis_client = None


async def close_cache():
    global redis_client
    if redis_client:
        await redis_client.close()
        redis_client = None


async def get_cached_history(chat_id: str, page: int = 1) -> list[dict] | None:
    if not redis_client:
        return None
    key = f"history:{chat_id}:{page}"
    data = await redis_client.get(key)
    if data:
        return json.loads(data)
    return None


async def set_cached_history(chat_id: str, messages: list[dict], page: int = 1):
    if not redis_client:
        return
    key = f"history:{chat_id}:{page}"
    await redis_client.setex(key, CACHE_TTL, json.dumps(messages, default=str))


async def invalidate_history_cache(chat_id: str):
    if not redis_client:
        return
    keys = await redis_client.keys(f"history:{chat_id}:*")
    if keys:
        await redis_client.delete(*keys)
