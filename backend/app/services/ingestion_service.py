import csv
import io
import json
from decimal import Decimal
from datetime import date, datetime

from fastapi import HTTPException, UploadFile

from ingestion_engine import excel_file_ingestion as ingestion

ROW_DATA_MARKER = "||ROW_DATA_JSON||"


def run_ingestion_from_path(input_path: str) -> dict:
    return ingestion.process_local_file(input_path)


async def run_ingestion_from_upload(upload: UploadFile) -> dict:
    if not upload.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")
    if not upload.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx uploads are supported.")

    file_bytes = await upload.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    return ingestion.process_uploaded_file(upload.filename, file_bytes)


def get_batch_summary(load_batch_id: str) -> dict:
    summary = ingestion.get_batch_summary(load_batch_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="Load batch not found.")
    return summary


def get_batch_error_counts(load_batch_id: str) -> list[dict]:
    return ingestion.get_batch_error_counts(load_batch_id)


def get_batch_error_details(load_batch_id: str) -> list[dict]:
    details = ingestion.get_batch_error_details(load_batch_id)
    cleaned: list[dict] = []
    for row in details:
        updated = dict(row)
        message = updated.get("ErrorMessage")
        if isinstance(message, str) and ROW_DATA_MARKER in message:
            updated["ErrorMessage"] = message.split(ROW_DATA_MARKER, 1)[0]
        cleaned.append(updated)
    return cleaned


def _coerce_sql_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _table_name_for_sheet(sheet_name: str | None) -> str | None:
    mapping = {
        "ProjectInformation": "stg.ProjectInformation",
        "ProjectQuants": "stg.ProjectQuants",
        "ElementQuants_L2": "stg.ElementQuants_L2",
        "Level2": "stg.Level2",
        "LineItem_L3": "stg.LineItem_L3",
        "Adjustments": "stg.Adjustments",
    }
    return mapping.get(sheet_name or "")


def get_batch_error_rows(load_batch_id: str) -> list[dict]:
    details = ingestion.get_batch_error_details(load_batch_id)
    rows_with_data: list[dict] = []

    for detail in details:
        sheet_name = detail.get("SheetName")
        row_num = detail.get("RowNum")
        table_name = _table_name_for_sheet(sheet_name)
        row_data = None
        message = detail.get("ErrorMessage")
        cleaned_message = message

        if isinstance(message, str) and ROW_DATA_MARKER in message:
            base_message, payload = message.split(ROW_DATA_MARKER, 1)
            cleaned_message = base_message
            try:
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    row_data = parsed
            except Exception:
                row_data = None

        if table_name and row_num is not None:
            sql = f"""
                SELECT TOP 1 *
                FROM {table_name}
                WHERE LoadBatchID = ?
                  AND RowNum = ?
            """
            matches = ingestion.fetch_all(sql, (load_batch_id, row_num))
            if matches:
                raw_row = matches[0]
                row_data = {
                    key: _coerce_sql_value(value)
                    for key, value in raw_row.items()
                    if key not in {"LoadBatchID", "SourceFileName"}
                    and not key.lower().startswith("stage")
                }

        merged = dict(detail)
        merged["ErrorMessage"] = cleaned_message
        merged["RowData"] = row_data
        rows_with_data.append(merged)

    return rows_with_data


def build_batch_error_csv(load_batch_id: str) -> io.StringIO:
    details = get_batch_error_details(load_batch_id)

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "Severity",
            "SheetName",
            "RowNum",
            "ColumnName",
            "ErrorType",
            "ErrorMessage",
        ],
    )
    writer.writeheader()
    for row in details:
        writer.writerow(row)

    buffer.seek(0)
    return buffer
