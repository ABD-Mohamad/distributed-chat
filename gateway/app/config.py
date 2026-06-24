from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    jwt_secret: str = "nexuschat-dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440
    lb_url: str = "http://load-balancer:8000"
    lb_ws_url: str = "ws://load-balancer:8000"
    redis_url: str = "redis://redis:6379/0"
    cors_origins: str = "*"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
