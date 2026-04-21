from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .api.routes.health import router as health_router
from .api.routes.ingestion import router as ingestion_router
from .core.settings import settings

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="API layer for the Excel ingestion pipeline and validation output.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health_router, prefix=settings.api_prefix)
app.include_router(ingestion_router, prefix=settings.api_prefix)
