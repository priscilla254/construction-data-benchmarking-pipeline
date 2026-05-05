import os
from urllib.parse import quote_plus

from cachelib.redis import RedisCache


def _as_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "please-change-me")

SUPERSET_DB_USER = os.getenv("SUPERSET_DB_USER", "superset")
SUPERSET_DB_PASSWORD = os.getenv("SUPERSET_DB_PASSWORD", "superset")
SUPERSET_DB_HOST = os.getenv("SUPERSET_DB_HOST", "db")
SUPERSET_DB_PORT = os.getenv("SUPERSET_DB_PORT", "5432")
SUPERSET_DB_NAME = os.getenv("SUPERSET_DB_NAME", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{SUPERSET_DB_USER}:{quote_plus(SUPERSET_DB_PASSWORD)}"
    f"@{SUPERSET_DB_HOST}:{SUPERSET_DB_PORT}/{SUPERSET_DB_NAME}"
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "1"))
REDIS_CELERY_DB = int(os.getenv("REDIS_CELERY_DB", "2"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_DB,
}

DATA_CACHE_CONFIG = CACHE_CONFIG

RESULTS_BACKEND = RedisCache(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    key_prefix="superset_results_",
)

CELERY_CONFIG = {
    "broker_url": f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}",
    "result_backend": f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}",
    "imports": ("superset.sql_lab",),
    "worker_prefetch_multiplier": 1,
    "task_acks_late": True,
}

FEATURE_FLAGS = {
    "EMBEDDED_SUPERSET": True,
}

ENABLE_PROXY_FIX = True

GUEST_ROLE_NAME = "Gamma"
GUEST_TOKEN_JWT_SECRET = os.getenv("SUPERSET_GUEST_TOKEN_JWT_SECRET", SECRET_KEY)
GUEST_TOKEN_JWT_ALGO = "HS256"
GUEST_TOKEN_JWT_EXP_SECONDS = int(os.getenv("SUPERSET_GUEST_TOKEN_TTL_SECONDS", "300"))

TALISMAN_ENABLED = False
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_SECURE = _as_bool(os.getenv("SUPERSET_SESSION_COOKIE_SECURE"), default=False)
