# cost-benchmarking-poc

POC scaffold for a React frontend, FastAPI backend, Excel ingestion engine, and database assets.

## Overview

This repository is structured so the backend API can be built first, with the frontend added on top once the ingestion and batch-reporting endpoints are stable.

Current backend capabilities:

- upload an Excel workbook for ingestion
- create and track a load batch
- return batch summary and validation errors
- download validation errors as CSV
- ask natural-language database questions via GROQ AI SQL Assistant

The ingestion flow supports uploaded files and local file testing only.

## Setup

Create and activate a virtual environment from the repository root.

```powershell
py -3 -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

If PowerShell blocks activation, run:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

## Run The Backend

Start the FastAPI app from the repository root:

```powershell
python -m uvicorn backend.app.main:app --reload --reload-dir backend --reload-dir ingestion_engine
```

Open the API docs in your browser:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/redoc`

Health check:

- `http://127.0.0.1:8000/api/health`

## Run The Frontend

From the repository root:

```powershell
cd frontend
npm install
npm run dev
```

Open in browser:

- `http://127.0.0.1:5173`

Notes:

- Keep the backend running on `http://127.0.0.1:8000` while using the frontend.
- Vite is configured to proxy `/api` requests to the backend.

## AI SQL Assistant (GROQ)

The backend includes an AI SQL Assistant that converts natural-language questions
to SQL and executes read-only queries against your database.

### Environment variable

Set this in `.env` before using the AI endpoint:

```env
GROQ_API_KEY=your_groq_api_key
```

### Backend endpoint

- `POST /api/ai/query`

Example request body:

```json
{
  "question": "Show top 10 Level2 elements by total cost"
}
```

Example response shape:

```json
{
  "question": "Show top 10 Level2 elements by total cost",
  "generated_sql": "SELECT TOP 10 L2Name, TotalCost FROM stg.Level2 ORDER BY TotalCost DESC",
  "row_count": 10,
  "rows": [
    {
      "L2Name": "Frame",
      "TotalCost": 1050875.0
    }
  ]
}
```

### Safety guardrails

The AI endpoint is read-only by design:

- single SQL statement only
- SQL must start with `SELECT`
- mutating/DDL keywords are blocked (`INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.)

### Frontend usage

The React app includes a dedicated **AI QS Assistant** page where users can:

- enter natural-language questions
- click preset question chips
- run the AI query
- inspect generated SQL and returned rows

## AI Report Draft Endpoint

The backend includes a draft-report endpoint used by the frontend **AI Report Generation** page.

### Backend endpoint

- `POST /api/ai/report-draft`

Example request body (preferred):

```json
{
  "project_id": "P2402"
}
```

Optional fallback request body:

```json
{
  "load_batch_id": "8d5f3dcb-2f4f-4e22-9d30-123456789abc"
}
```

Example response shape:

```json
{
  "project_id": "P2402",
  "load_batch_id": "8d5f3dcb-2f4f-4e22-9d30-123456789abc",
  "source_file_name": "P2402_benchmark.xlsx",
  "report_context": {
    "project": {
      "project_id": "P2402"
    },
    "audit": {
      "load_batch_id": "8d5f3dcb-2f4f-4e22-9d30-123456789abc"
    }
  }
}
```

## Backend API

Current backend endpoints:

- `POST /api/ingestion/upload`
- `GET /api/batches/{load_batch_id}/summary`
- `GET /api/batches/{load_batch_id}/error-counts`
- `GET /api/batches/{load_batch_id}/error-details`
- `GET /api/batches/{load_batch_id}/error-rows`
- `GET /api/batches/{load_batch_id}/download-errors`
- `POST /api/ai/query`
- `POST /api/ai/report-draft`

## How To Test The Backend

Recommended smoke-test flow:

1. Start the backend server.
2. Open `http://127.0.0.1:8000/docs`.
3. Run `POST /api/ingestion/upload` with a test `.xlsx` file.
4. Copy the returned `load_batch_id`.
5. Use that `load_batch_id` in the batch endpoints.

Example PowerShell upload:

```powershell
curl -X POST "http://127.0.0.1:8000/api/ingestion/upload" `
  -H "accept: application/json" `
  -H "Content-Type: multipart/form-data" `
  -F "file=@C:/path/to/your/test.xlsx"
```

Expected upload response shape:

```json
{
  "load_batch_id": "8d5f3dcb-2f4f-4e22-9d30-123456789abc",
  "status": "COMMITTED",
  "error_count": 0,
  "source_file_name": "test.xlsx"
}
```

Then query the batch:

```powershell
curl "http://127.0.0.1:8000/api/batches/<load_batch_id>/summary"
curl "http://127.0.0.1:8000/api/batches/<load_batch_id>/error-counts"
curl "http://127.0.0.1:8000/api/batches/<load_batch_id>/error-details"
curl "http://127.0.0.1:8000/api/batches/<load_batch_id>/error-rows"
curl -OJ "http://127.0.0.1:8000/api/batches/<load_batch_id>/download-errors"
```

AI query example:

```powershell
curl -X POST "http://127.0.0.1:8000/api/ai/query" `
  -H "Content-Type: application/json" `
  -d "{\"question\":\"Show top 10 Level2 elements by total cost\"}"
```

AI report draft example:

```powershell
curl -X POST "http://127.0.0.1:8000/api/ai/report-draft" `
  -H "Content-Type: application/json" `
  -d "{\"project_id\":\"P2402\"}"
```

## Notes

- The upload form field name must be `file`.
- `load_batch_id` is returned by the upload endpoint and is required for all batch endpoints.
- `project_id` is the preferred key for `POST /api/ai/report-draft`; backend resolves the latest matching batch.
- Validation errors can be inspected via JSON endpoints or downloaded as CSV.
- `error-rows` includes `RowData` for row-level troubleshooting and mapped SUMMARY cell references when available.
- AI query endpoint is read-only and enforces single-statement `SELECT` SQL generation.
- The ingestion engine lives in `ingestion_engine/excel_file_ingestion.py`.
- The frontend scaffold is present but backend-first development is the current focus.

## Structure

```text
cost-benchmarking-poc/
├── requirements.txt
├── frontend/
│   ├── src/
│   │   ├── api/
│   │   ├── components/
│   │   ├── pages/
│   │   ├── hooks/
│   │   ├── types/
│   │   ├── App.tsx
│   │   └── main.tsx
│   └── package.json
├── backend/
│   ├── app/
│   │   ├── main.py
│   │   ├── api/
│   │   ├── services/
│   │   ├── repositories/
│   │   └── schemas/
├── ingestion_engine/
│   └── excel_file_ingestion.py
├── database/
│   ├── schema/
│   └── procedures/
└── README.md
```
