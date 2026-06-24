import time
from collections import defaultdict

from fastapi import HTTPException, status
from jose import JWTError, jwt

from .config import settings


def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
    except JWTError:
        return None


def get_user_id_from_token(authorization: str) -> str:
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid authorization header")
    payload = decode_token(authorization[7:])
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    return payload["sub"]


def get_user_id_from_query(token: str) -> str | None:
    payload = decode_token(token)
    if payload is None:
        return None
    return payload["sub"]


class TokenBucket:
    def __init__(self, rate: float = 10.0, capacity: int = 20):
        self.rate = rate
        self.capacity = capacity
        self.tokens: dict[str, float] = defaultdict(lambda: float(capacity))
        self.last_refill: dict[str, float] = defaultdict(time.monotonic)

    def consume(self, key: str, tokens: int = 1) -> bool:
        now = time.monotonic()
        elapsed = now - self.last_refill[key]
        self.tokens[key] = min(self.capacity, self.tokens[key] + elapsed * self.rate)
        self.last_refill[key] = now
        if self.tokens[key] >= tokens:
            self.tokens[key] -= tokens
            return True
        return False


rate_limiter = TokenBucket(rate=10.0, capacity=20)
