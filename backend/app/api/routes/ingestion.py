from fastapi import APIRouter, File, UploadFile
from fastapi.responses import StreamingResponse

from ...schemas.ingestion import (
    AIQueryRequest,
    AIQueryResponse,
    BatchSummaryResponse,
    IngestionRunResponse,
    ValidationErrorCountResponse,
    ValidationErrorDetailResponse,
    ValidationErrorRowResponse,
)
from ...services.ingestion_service import (
    build_batch_error_csv,
    get_batch_error_counts,
    get_batch_error_details,
    get_batch_error_rows,
    get_batch_summary,
    run_ai_query,
    run_ingestion_from_upload,
)

router = APIRouter(tags=["ingestion"])


@router.post("/ingestion/upload", response_model=IngestionRunResponse)
async def trigger_ingestion_from_upload(
    file: UploadFile = File(...),
) -> IngestionRunResponse:
    result = await run_ingestion_from_upload(file)
    return IngestionRunResponse(**result)


@router.get("/batches/{load_batch_id}/summary", response_model=BatchSummaryResponse)
def fetch_batch_summary(load_batch_id: str) -> BatchSummaryResponse:
    summary = get_batch_summary(load_batch_id)
    return BatchSummaryResponse(**summary)


@router.get(
    "/batches/{load_batch_id}/error-counts",
    response_model=list[ValidationErrorCountResponse],
)
def fetch_batch_error_counts(load_batch_id: str) -> list[ValidationErrorCountResponse]:
    counts = get_batch_error_counts(load_batch_id)
    return [ValidationErrorCountResponse(**row) for row in counts]


@router.get(
    "/batches/{load_batch_id}/error-details",
    response_model=list[ValidationErrorDetailResponse],
)
def fetch_batch_error_details(load_batch_id: str) -> list[ValidationErrorDetailResponse]:
    details = get_batch_error_details(load_batch_id)
    return [ValidationErrorDetailResponse(**row) for row in details]


@router.get(
    "/batches/{load_batch_id}/error-rows",
    response_model=list[ValidationErrorRowResponse],
)
def fetch_batch_error_rows(load_batch_id: str) -> list[ValidationErrorRowResponse]:
    rows = get_batch_error_rows(load_batch_id)
    return [ValidationErrorRowResponse(**row) for row in rows]


@router.get("/batches/{load_batch_id}/download-errors")
def download_batch_error_details(load_batch_id: str) -> StreamingResponse:
    csv_buffer = build_batch_error_csv(load_batch_id)
    filename = f"batch-{load_batch_id}-errors.csv"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(csv_buffer, media_type="text/csv", headers=headers)


@router.post("/ai/query", response_model=AIQueryResponse)
def run_ai_sql_query(payload: AIQueryRequest) -> AIQueryResponse:
    result = run_ai_query(payload.question)
    return AIQueryResponse(**result)
