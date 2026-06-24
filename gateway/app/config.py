from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    jwt_secret: str = "nexuschat-dev-secret-change-in-production"
    jwt_algorithm: str = "HS256"
    lb_url: str = "http://load-balancer:8000"
    lb_ws_url: str = "ws://load-balancer:8000"

    model_config = {"env_file": ".env", "extra": "ignore"}


settings = Settings()
