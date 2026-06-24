from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql+asyncpg://nexuschat:nexuschat@postgres:5432/nexuschat"
    jwt_secret: str = "nexuschat-dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    jwt_expire_minutes: int = 1440

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
