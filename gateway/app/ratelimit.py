import hashlib
import time
import uuid

import redis.asyncio as redis

from .config import settings

_redis = None


async def get_redis():
    global _redis
    if _redis is None:
        _redis = await redis.from_url(settings.redis_url, decode_responses=True)
    return _redis


def _token_key(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def blacklist_token(token: str, expires_in: int):
    r = await get_redis()
    key = _token_key(token)
    await r.setex(f"blacklist:{key}", expires_in, "1")


async def is_blacklisted(token: str) -> bool:
    r = await get_redis()
    key = _token_key(token)
    return await r.exists(f"blacklist:{key}") == 1


async def check_rate_limit(key: str, max_requests: int, window_seconds: int) -> bool:
    r = await get_redis()
    now = time.time()
    window_start = now - window_seconds
    await r.zremrangebyscore(key, 0, window_start)
    count = await r.zcard(key)
    if count >= max_requests:
        return False
    await r.zadd(key, {str(uuid.uuid4()): now})
    await r.expire(key, window_seconds)
    return True
