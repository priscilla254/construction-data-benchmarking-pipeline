#!/bin/sh
set -e

MODE="${1:-web}"

if [ "$MODE" = "worker" ]; then
  celery --app=superset.tasks.celery_app:app worker -O fair -l INFO
  exit 0
fi

superset db upgrade

if [ -n "${SUPERSET_ADMIN_USERNAME}" ] && [ -n "${SUPERSET_ADMIN_PASSWORD}" ]; then
  superset fab create-admin \
    --username "${SUPERSET_ADMIN_USERNAME}" \
    --firstname "${SUPERSET_ADMIN_FIRSTNAME:-Superset}" \
    --lastname "${SUPERSET_ADMIN_LASTNAME:-Admin}" \
    --email "${SUPERSET_ADMIN_EMAIL:-superset-admin@example.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD}" || true
fi

superset init

gunicorn \
  --bind "0.0.0.0:8088" \
  --workers 2 \
  --timeout 120 \
  "superset.app:create_app()"
