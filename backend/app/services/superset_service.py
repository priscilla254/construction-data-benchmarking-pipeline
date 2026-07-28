from __future__ import annotations

import os
from functools import lru_cache

import requests
from fastapi import HTTPException

from ..core.settings import settings


class SupersetClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 20) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self._session = requests.Session()

    def _login(self) -> str:
        response = self._session.post(
            f"{self.base_url}/api/v1/security/login",
            json={
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            },
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail="Failed to authenticate with Superset API.",
            )
        data = response.json()
        access_token = data.get("access_token")
        if not access_token:
            raise HTTPException(status_code=502, detail="Superset access token missing from login.")
        return access_token

    def _get_csrf_token(self, access_token: str) -> str:
        response = self._session.get(
            f"{self.base_url}/api/v1/security/csrf_token/",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail="Failed to fetch Superset CSRF token.",
            )
        csrf_token = response.json().get("result")
        if not csrf_token:
            raise HTTPException(status_code=502, detail="Superset CSRF token missing from response.")
        return csrf_token

    def create_guest_token(
        self,
        dashboard_id: str,
        user_username: str | None = None,
        user_first_name: str | None = None,
        user_last_name: str | None = None,
    ) -> str:
        access_token = self._login()
        csrf_token = self._get_csrf_token(access_token)
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-CSRFToken": csrf_token,
            "Referer": self.base_url,
        }
        payload = {
            "resources": [{"type": "dashboard", "id": dashboard_id}],
            "rls": [],
            "user": {
                "username": user_username or "embedded-user",
                "first_name": user_first_name or "Embedded",
                "last_name": user_last_name or "Viewer",
            },
        }
        response = self._session.post(
            f"{self.base_url}/api/v1/security/guest_token/",
            json=payload,
            headers=headers,
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise HTTPException(status_code=502, detail="Failed to create Superset guest token.")
        token = response.json().get("token")
        if not token:
            raise HTTPException(status_code=502, detail="Superset guest token missing from response.")
        return token


def _require(value: str | None, env_name: str) -> str:
    if value and value.strip():
        return value.strip()
    raise HTTPException(status_code=500, detail=f"Missing required Superset setting: {env_name}")


@lru_cache(maxsize=1)
def get_superset_client() -> SupersetClient:
    base_url = _require(settings.superset_url, "SUPERSET_URL")
    username = _require(settings.superset_api_username, "SUPERSET_API_USERNAME")
    password = _require(settings.superset_api_password, "SUPERSET_API_PASSWORD")
    timeout = int(os.getenv("SUPERSET_API_TIMEOUT_SECONDS", "20"))
    return SupersetClient(base_url=base_url, username=username, password=password, timeout=timeout)
