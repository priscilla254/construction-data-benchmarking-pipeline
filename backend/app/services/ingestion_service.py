import csv
import io
import json
from decimal import Decimal
from datetime import date, datetime

from fastapi import HTTPException, UploadFile
from ingestion_engine import excel_file_ingestion as ingestion

"""
This service is responsible for ingesting Excel files into the database.
It uses the ingestion_engine library to process the files.
it contains the business logic for handling excel file uploads, batch tracking, error reporting etc.
it does not define routes,instead it provides functions that the routes can call.
"""

ROW_DATA_MARKER = "||ROW_DATA_JSON||"

# process a local file
def run_ingestion_from_path(input_path: str) -> dict:
    return ingestion.process_local_file(input_path)


"""
called when a user uploads a file via the web frontend.
UploadFile is a fastAPI object that represents the uploaded file.
it validates, filename exists, ends with .xlsx, and is not empty.
then it reads the file into memory and calls the ingestion engine to process it.
async keyword is used because reading the file is I/O operation that can be done asynchronously.
under the hood, FastAPI hands off the read to an event loop so the server can handle other requests while waiting for the file to be read.
returns a dictionary with the load batch id, status, error count, and source file name.
"""

async def run_ingestion_from_upload(upload: UploadFile) -> dict:
    if not upload.filename:
        raise HTTPException(status_code=400, detail="Uploaded file must have a filename.")
    if not upload.filename.lower().endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx uploads are supported.")

    file_bytes = await upload.read()
    if not file_bytes:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    return ingestion.process_uploaded_file(upload.filename, file_bytes)

# batch inspection functions.
# returns the summary record (e.g. status, error count, timestamps. etc.) for a given load batch id.
# if no batch is found, raises a 404 error.
def get_batch_summary(load_batch_id: str) -> dict:
    summary = ingestion.get_batch_summary(load_batch_id)
    if summary is None:
        raise HTTPException(status_code=404, detail="Load batch not found.")
    return summary

# returns a list of error counts grouped by severity, error type, sheet name.
def get_batch_error_counts(load_batch_id: str) -> list[dict]:
    return ingestion.get_batch_error_counts(load_batch_id)

# returns a detailed list of every validation error.
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

# helper function to convert SQL values to a format that can be serialized to JSON.
# this is necessary because SQL values are often stored as Decimal or datetime objects, which are not JSON serializable.
def _coerce_sql_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


# helper function to map sheet names to their corresponding table names in the database.
# this is necessary because the ingestion engine returns sheet names, but the database tables have different names.
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
