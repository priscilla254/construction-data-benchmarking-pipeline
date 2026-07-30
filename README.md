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
- embed Apache Superset analytics dashboards in the frontend

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

- Keep the backend running on `http://127.0.0.1:8001` while using the frontend.
- Vite is configured to proxy `/api` requests to the backend.

## Run The Backend

Start the FastAPI app from the repository root:

```powershell
python -m uvicorn backend.app.main:app --reload --reload-dir backend --reload-dir ingestion_engine --port 8001
```

Superset embed runtime expects these values in root `.env`:

```env
SUPERSET_URL=http://127.0.0.1:8088
SUPERSET_API_USERNAME=admin
SUPERSET_API_PASSWORD=your_password
SUPERSET_DEFAULT_DASHBOARD_ID=94db7fc1-ef6a-4d80-bedf-44546a4f6d60
```

Open the API docs in your browser:

- `http://127.0.0.1:8001/docs`
- `http://127.0.0.1:8001/redoc`

Health check:

- `http://127.0.0.1:8001/api/health`

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
- submit the question with the enter button
- inspect generated SQL and returned rows

### Suggested query options

Examples you can ask in AI QS Assistant:

- Show top 10 Level2 elements by total cost.
- What is the average TotalCost by L1Name?
- Show TotalCost by SectorName for committed batches.
- Which Level2 elements have the highest average Rate?
- List validation error counts by ErrorType and Severity.
- Compare CostPerM2 by ProjectName and SectorName.
- Show projects where GIFA is above 3000 and CostPerM2 is above 12000.
- Show tenderers ranked by FinalAdjustedTenderSum.

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
- `POST /api/superset/guest-token`

## How To Test The Backend

Recommended smoke-test flow:

1. Start the backend server.
2. Open `http://127.0.0.1:8001/docs`.
3. Run `POST /api/ingestion/upload` with a test `.xlsx` file.
4. Copy the returned `load_batch_id`.
5. Use that `load_batch_id` in the batch endpoints.

Example PowerShell upload:

```powershell
curl -X POST "http://127.0.0.1:8001/api/ingestion/upload" `
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
curl "http://127.0.0.1:8001/api/batches/<load_batch_id>/summary"
curl "http://127.0.0.1:8001/api/batches/<load_batch_id>/error-counts"
curl "http://127.0.0.1:8001/api/batches/<load_batch_id>/error-details"
curl "http://127.0.0.1:8001/api/batches/<load_batch_id>/error-rows"
curl -OJ "http://127.0.0.1:8001/api/batches/<load_batch_id>/download-errors"
```

AI query example:

```powershell
curl -X POST "http://127.0.0.1:8001/api/ai/query" `
  -H "Content-Type: application/json" `
  -d "{\"question\":\"Show top 10 Level2 elements by total cost\"}"
```

AI report draft example:

```powershell
curl -X POST "http://127.0.0.1:8001/api/ai/report-draft" `
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

## Apache Superset Service (Dockerized)

Superset runs as an isolated sidecar service so it can evolve independently from the
existing FastAPI and React runtime.

### Why separate service

- avoids dependency conflicts with the backend virtual environment
- keeps BI runtime concerns (cache/worker/metadata) outside app API code
- enables embedded dashboards without changing ingestion pipeline behavior

### Files

- `docker/superset/docker-compose.superset.yml`
- `docker/superset/Dockerfile`
- `docker/superset/superset_config.py`
- `docker/superset/bootstrap.sh`
- `docker/superset/.env.example`
- `database/schema/002_superset_reporting_views.sql`

### 1) First-time setup

From repository root:

```powershell
cd docker/superset
copy .env.example .env
```

Edit `.env` values before startup:

- `SUPERSET_SECRET_KEY`
- `SUPERSET_DB_PASSWORD`
- `SUPERSET_ADMIN_PASSWORD`
- `SQLSERVER_USERNAME` and `SQLSERVER_PASSWORD` (read-only SQL login)

### 2) Start Superset stack

```powershell
cd docker/superset
docker compose -f docker-compose.superset.yml up -d --build
```

Superset URL:

- `http://127.0.0.1:8088`

### 3) Health checks

Service-level checks:

```powershell
cd docker/superset
docker compose -f docker-compose.superset.yml ps
docker compose -f docker-compose.superset.yml logs superset --tail 80
docker compose -f docker-compose.superset.yml logs superset-worker --tail 80
```

HTTP check:

```powershell
curl http://127.0.0.1:8088/health
```

### 4) Configure SQL Server data source in Superset

In Superset UI:

1. Settings -> Database Connections -> + Database.
2. Use SQLAlchemy URI:

```text
mssql+pyodbc://<SQLSERVER_USERNAME>:<SQLSERVER_PASSWORD>@<SQLSERVER_HOST>:<SQLSERVER_PORT>/<SQLSERVER_DATABASE>?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes
```

3. Test connection and save.

Recommended: grant this SQL user read-only access to BI views (not full table write access).

### 5) Curated datasets for dashboards

Run:

- `database/schema/002_superset_reporting_views.sql`

Then add these views as Superset datasets:

- `dbo.vw_BI_ProjectOverview`
- `dbo.vw_BI_TenderReview`
- `dbo.vw_BI_Level2CostBreakdown`
- `dbo.vw_BI_AdjustmentSummary`

### 6) Embed dashboards into the React app

Backend endpoint:

- `POST /api/superset/guest-token`

Required backend env vars:

- `SUPERSET_URL` (for example `http://127.0.0.1:8088`)
- `SUPERSET_API_USERNAME`
- `SUPERSET_API_PASSWORD`
- `SUPERSET_DEFAULT_DASHBOARD_ID`

Frontend behavior:

- Analytics tab auto-loads the configured default dashboard
- guest token fetched from FastAPI
- dashboard rendered with `@superset-ui/embedded-sdk`

### 7) Stop/reset

Stop containers:

```powershell
cd docker/superset
docker compose -f docker-compose.superset.yml down
```

Reset stack including metadata (destructive):

```powershell
cd docker/superset
docker compose -f docker-compose.superset.yml down -v
```

### 8) Backups and secrets

- Metadata persistence lives in `superset_db_data` Docker volume.
- Backup strategy: periodic `pg_dump` from `superset-postgres` container.
- Do not commit `.env` with live secrets.
- Rotate Superset admin and SQL credentials periodically.

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
