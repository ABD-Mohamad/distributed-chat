from .auth import rate_limiter


class RateLimiter:
    def allow(self, key: str) -> bool:
        return rate_limiter.consume(key)
