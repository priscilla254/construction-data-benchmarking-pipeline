import os

from pydantic import BaseModel


class Settings(BaseModel):
    app_name: str = "Excel Ingestion API"
    api_prefix: str = "/api"
    app_version: str = "0.1.0"
    cors_origins: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000"]


def _parse_cors_origins() -> list[str]:
    raw = os.getenv("FASTAPI_CORS_ORIGINS", "").strip()
    if not raw:
        return Settings().cors_origins
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


settings = Settings(cors_origins=_parse_cors_origins())
