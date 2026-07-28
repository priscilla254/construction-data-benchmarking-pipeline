from pydantic import BaseModel


class SupersetGuestTokenRequest(BaseModel):
    dashboard_id: str | None = None
    user_username: str | None = None
    user_first_name: str | None = None
    user_last_name: str | None = None


class SupersetGuestTokenResponse(BaseModel):
    superset_url: str
    dashboard_id: str
    guest_token: str
