from fastapi import APIRouter, HTTPException

from ...core.settings import settings
from ...schemas.superset import SupersetGuestTokenRequest, SupersetGuestTokenResponse
from ...services.superset_service import get_superset_client

router = APIRouter(prefix="/superset", tags=["superset"])


@router.post("/guest-token", response_model=SupersetGuestTokenResponse)
def create_superset_guest_token(
    payload: SupersetGuestTokenRequest,
) -> SupersetGuestTokenResponse:
    dashboard_id = (payload.dashboard_id or settings.superset_default_dashboard_id or "").strip()
    if not dashboard_id:
        raise HTTPException(
            status_code=500,
            detail="Missing required Superset setting: SUPERSET_DEFAULT_DASHBOARD_ID",
        )

    client = get_superset_client()
    token = client.create_guest_token(
        dashboard_id=dashboard_id,
        user_username=payload.user_username,
        user_first_name=payload.user_first_name,
        user_last_name=payload.user_last_name,
    )
    return SupersetGuestTokenResponse(
        superset_url=settings.superset_url or "",
        dashboard_id=dashboard_id,
        guest_token=token,
    )
