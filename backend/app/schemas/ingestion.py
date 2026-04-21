from datetime import datetime
from typing import Any

from pydantic import BaseModel


class IngestionRunResponse(BaseModel):
    load_batch_id: str
    status: str
    error_count: int = 0
    source_file_name: str | None = None
    exception: str | None = None


class BatchSummaryResponse(BaseModel):
    LoadBatchID: str
    SourceFileName: str | None = None
    SourceFilePath: str | None = None
    BatchStatus: str
    ErrorCount: int | None = None
    CreatedAt: datetime | None = None


class ValidationErrorCountResponse(BaseModel):
    Severity: str | None = None
    ErrorType: str | None = None
    SheetName: str | None = None
    Cnt: int


class ValidationErrorDetailResponse(BaseModel):
    Severity: str | None = None
    SheetName: str | None = None
    RowNum: int | None = None
    ColumnName: str | None = None
    ErrorType: str | None = None
    ErrorMessage: str | None = None


class ValidationErrorRowResponse(BaseModel):
    Severity: str | None = None
    SheetName: str | None = None
    RowNum: int | None = None
    ColumnName: str | None = None
    ErrorType: str | None = None
    ErrorMessage: str | None = None
    RowData: dict[str, Any] | None = None


class AIQueryRequest(BaseModel):
    question: str


class AIQueryResponse(BaseModel):
    question: str
    generated_sql: str
    row_count: int
    rows: list[dict[str, Any]]
