import os

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class Settings(BaseModel):
    app_name: str = "Excel Ingestion API"
    api_prefix: str = "/api"
    app_version: str = "0.1.0"
    cors_origins: list[str] = ["http://localhost:3000", "http://127.0.0.1:3000"]
    superset_url: str | None = os.getenv("SUPERSET_URL")
    superset_api_username: str | None = os.getenv("SUPERSET_API_USERNAME")
    superset_api_password: str | None = os.getenv("SUPERSET_API_PASSWORD")
    superset_default_dashboard_id: str | None = os.getenv("SUPERSET_DEFAULT_DASHBOARD_ID")


def _parse_cors_origins() -> list[str]:
    raw = os.getenv("FASTAPI_CORS_ORIGINS", "").strip()
    if not raw:
        return Settings().cors_origins
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


settings = Settings(cors_origins=_parse_cors_origins())
