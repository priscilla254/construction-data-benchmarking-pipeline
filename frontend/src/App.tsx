import { useMemo, useState } from "react";

import {
  getBatchErrorCounts,
  getBatchErrorRows,
  getBatchSummary,
  getErrorDownloadUrl,
  runAIQuery,
  uploadWorkbookWithProgress,
} from "./api/ingestion";
import type {
  AIQueryResponse,
  BatchSummary,
  IngestionRunResponse,
  ValidationErrorCount,
  ValidationErrorRow,
} from "./types/ingestion";

function toFriendlyErrorMessage(row: ValidationErrorRow): string {
  const errorType = (row.ErrorType ?? "").toUpperCase();
  const sheet = row.SheetName ?? "the sheet";
  const rowText = row.RowNum ? `row ${row.RowNum}` : "a row";
  const column = row.ColumnName ?? "a required field";

  if (errorType === "MISSING_TOTALCOST_SKIPPED") {
    return `In ${sheet}, ${rowText} has no value in ${column}. This row was skipped and not loaded. Add a total cost value for the selected contractor and re-upload.`;
  }

  if (errorType === "MISSING_COLUMN") {
    return `A required column is missing in ${sheet}. Add the expected column and upload again.`;
  }

  if (errorType === "INVALID_NUMBER") {
    return `A value in ${sheet} ${rowText} is not a valid number. Correct the numeric value and upload again.`;
  }

  if (errorType === "DOMAIN") {
    return `A value in ${sheet} ${rowText} is outside the allowed options. Check the accepted values and upload again.`;
  }

  if (errorType === "DECIMAL_PRECISION") {
    return `A numeric value in ${sheet} ${rowText} is too large or has too many decimal places for the database. Reduce precision and upload again.`;
  }

  if (errorType === "EXCEPTION") {
    return "The ingestion run failed unexpectedly. Review the technical message or contact support.";
  }

  return row.ErrorMessage ?? "Validation issue detected. Please review this row and try again.";
}

function severityLabel(value?: string | null): string {
  const s = (value ?? "").toUpperCase();
  if (s === "ERROR") {
    return "Error";
  }
  if (s === "WARNING") {
    return "Warning";
  }
  return value ?? "-";
}

function getStatusClass(status?: string | null) {
  const value = (status ?? "").toUpperCase();
  if (value === "COMMITTED" || value === "VALIDATED") {
    return "status-badge status-success";
  }
  if (value === "FAILED" || value === "ERROR") {
    return "status-badge status-error";
  }
  if (value === "STAGED" || value === "RECEIVED") {
    return "status-badge status-warning";
  }
  return "status-badge status-neutral";
}

const AI_PRESET_QUESTIONS = [
  "Show top 10 Level2 elements by total cost.",
  "What is the average TotalCost by L1Name?",
  "Show TotalCost by SectorName for committed batches.",
  "Which Level2 elements have the highest average Rate?",
  "List validation error counts by ErrorType and Severity.",
];

function App() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploadResult, setUploadResult] = useState<IngestionRunResponse | null>(null);
  const [summary, setSummary] = useState<BatchSummary | null>(null);
  const [errorCounts, setErrorCounts] = useState<ValidationErrorCount[]>([]);
  const [errorDetails, setErrorDetails] = useState<ValidationErrorRow[]>([]);
  const [busyAction, setBusyAction] = useState<string | null>(null);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [aiQuestion, setAiQuestion] = useState("");
  const [aiResult, setAiResult] = useState<AIQueryResponse | null>(null);
  const [aiError, setAiError] = useState<string | null>(null);

  const activeBatchId = useMemo(() => uploadResult?.load_batch_id || "", [uploadResult]);
  const currentStatus = summary?.BatchStatus ?? uploadResult?.status ?? "Not started";

  async function loadBatchData(loadBatchId: string) {
    const [summaryResult, countsResult, detailsResult] = await Promise.all([
      getBatchSummary(loadBatchId),
      getBatchErrorCounts(loadBatchId),
      getBatchErrorRows(loadBatchId),
    ]);
    setSummary(summaryResult);
    setErrorCounts(countsResult);
    setErrorDetails(detailsResult);
  }

  async function handleUpload() {
    if (!selectedFile) {
      setErrorMessage("Choose an Excel file before uploading.");
      return;
    }

    setBusyAction("upload");
    setErrorMessage(null);
    setUploadProgress(0);
    setUploadResult(null);
    setSummary(null);
    setErrorCounts([]);
    setErrorDetails([]);
    try {
      const result = await uploadWorkbookWithProgress(selectedFile, setUploadProgress);
      setUploadResult(result);
      await loadBatchData(result.load_batch_id);
    } catch (error) {
      setErrorMessage(
        error instanceof Error ? error.message : "Upload failed for an unknown reason.",
      );
    } finally {
      setBusyAction(null);
    }
  }

  async function handleRunAIQuery() {
    if (!aiQuestion.trim()) {
      setAiError("Enter a question for the AI assistant.");
      return;
    }

    setBusyAction("ai-query");
    setAiError(null);
    setAiResult(null);
    try {
      const result = await runAIQuery(aiQuestion.trim());
      setAiResult(result);
    } catch (error) {
      setAiError(error instanceof Error ? error.message : "AI query failed.");
    } finally {
      setBusyAction(null);
    }
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <div className="hero-card">
          <div className="hero-kicker">Backend-connected POC</div>
          <h1>Benchmarking Ingestion Console</h1>
          <p>
            Upload benchmark workbooks, inspect batch processing results, review validation
            issues, and download a clean CSV of errors from the FastAPI backend.
          </p>
          <div className="hero-metrics">
            <div className="metric-card">
              <span className="metric-label">Current batch</span>
              <span className="metric-value mono">
                {activeBatchId ? activeBatchId.slice(0, 8) : "--"}
              </span>
            </div>
            <div className="metric-card">
              <span className="metric-label">Error groups</span>
              <span className="metric-value">{errorCounts.length}</span>
            </div>
            <div className="metric-card">
              <span className="metric-label">Error rows</span>
              <span className="metric-value">{errorDetails.length}</span>
            </div>
          </div>
        </div>
      </section>

      <section className="section-grid">
        <section className="section-card span-5">
          <div className="section-header">
            <div>
              <h2 className="section-title">Upload workbook</h2>
              <p className="section-subtitle">
                Submit a `.xlsx` file and monitor the upload and ingestion result in one place.
              </p>
            </div>
          </div>

          <div className="field-stack">
            <label className="field-label" htmlFor="workbook-file">
              Excel workbook
            </label>
            <input
              id="workbook-file"
              className="file-picker"
              type="file"
              accept=".xlsx"
              onChange={(event) => setSelectedFile(event.target.files?.[0] ?? null)}
            />
          </div>

          <div className="action-row">
            <button
              className="button"
              onClick={() => void handleUpload()}
              disabled={busyAction === "upload"}
            >
              {busyAction === "upload" ? "Uploading..." : "Upload and run"}
            </button>
            {activeBatchId ? (
              <a className="button-link" href={getErrorDownloadUrl(activeBatchId)}>
                Download errors CSV
              </a>
            ) : null}
          </div>

          <div className="progress-block">
            <div className="progress-meta">
              <span className="progress-label">Upload progress</span>
              <span className="progress-value">{uploadProgress}%</span>
            </div>
            <div className="progress-track" aria-hidden="true">
              <div
                className="progress-fill"
                style={{ width: `${uploadProgress}%` }}
              />
            </div>
          </div>

          {!uploadResult ? (
            <p className="message-muted" style={{ marginTop: "18px" }}>
              No upload has been run in this session yet.
            </p>
          ) : null}

          {uploadResult?.exception ? (
            <div className="message-error">{uploadResult.exception}</div>
          ) : null}
          {errorMessage ? <div className="message-error">{errorMessage}</div> : null}
          {!errorMessage && uploadResult?.status === "COMMITTED" ? (
            <div className="message-success">Upload completed successfully.</div>
          ) : null}
          {!errorMessage && uploadResult?.status === "FAILED" ? (
            <div className="message-error">
              Upload completed but the batch failed validation. Review the errors below for the
              reason.
            </div>
          ) : null}
        </section>

        <section className="section-card span-7">
          <div className="section-header">
            <div>
              <h2 className="section-title">Run overview</h2>
              <p className="section-subtitle">
                Summary of the latest upload attempt and the backend response.
              </p>
            </div>
            <span className={getStatusClass(currentStatus)}>{currentStatus}</span>
          </div>

          <div className="summary-grid">
            <div className="summary-item">
              <span className="summary-item-label">Selected file</span>
              <span className="summary-item-value">{selectedFile?.name ?? "-"}</span>
            </div>
            <div className="summary-item">
              <span className="summary-item-label">Upload progress</span>
              <span className="summary-item-value">{uploadProgress}%</span>
            </div>
            <div className="summary-item">
              <span className="summary-item-label">Current batch</span>
              <span className="summary-item-value mono">{activeBatchId || "-"}</span>
            </div>
            <div className="summary-item">
              <span className="summary-item-label">Status</span>
              <span className="summary-item-value">{currentStatus}</span>
            </div>
            <div className="summary-item">
              <span className="summary-item-label">Error groups</span>
              <span className="summary-item-value">{errorCounts.length}</span>
            </div>
            <div className="summary-item">
              <span className="summary-item-label">Detailed rows</span>
              <span className="summary-item-value">{errorDetails.length}</span>
            </div>
          </div>
        </section>

        <section className="section-card span-12">
          <div className="section-header">
            <div>
              <h2 className="section-title">Error insights</h2>
              <p className="section-subtitle">
                Review aggregated error counts and detailed row-level validation output.
              </p>
            </div>
            {activeBatchId ? (
              <a className="button-link" href={getErrorDownloadUrl(activeBatchId)}>
                Download errors CSV
              </a>
            ) : null}
          </div>

          {errorCounts.length > 0 ? (
            <div className="table-wrap" style={{ marginBottom: "14px" }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Severity</th>
                    <th>Error type</th>
                    <th>Sheet</th>
                    <th>Count</th>
                  </tr>
                </thead>
                <tbody>
                  {errorCounts.map((row, index) => (
                    <tr key={`${row.ErrorType}-${row.SheetName}-${index}`}>
                      <td>{row.Severity ?? "-"}</td>
                      <td>{row.ErrorType ?? "-"}</td>
                      <td>{row.SheetName ?? "-"}</td>
                      <td>{row.Cnt}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="message-muted" style={{ marginBottom: "12px" }}>
              No error counts loaded yet.
            </p>
          )}

          {errorDetails.length > 0 ? (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Severity</th>
                    <th>Sheet</th>
                    <th>Row</th>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Message</th>
                  </tr>
                </thead>
                <tbody>
                  {errorDetails.map((row, index) => (
                    <tr key={`${row.ErrorType}-${row.RowNum}-${index}`}>
                      <td>{severityLabel(row.Severity)}</td>
                      <td>{row.SheetName ?? "-"}</td>
                      <td>{row.RowNum ?? "-"}</td>
                      <td>{row.ColumnName ?? "-"}</td>
                      <td>{row.ErrorType ?? "-"}</td>
                      <td>
                        <div>{toFriendlyErrorMessage(row)}</div>
                        {row.RowData ? (
                          <details className="row-data-details">
                            <summary>View row values</summary>
                            <pre className="row-data-pre">
                              {JSON.stringify(row.RowData, null, 2)}
                            </pre>
                          </details>
                        ) : (
                          <div className="row-data-unavailable">
                            Row values are not available for this error. This usually means the
                            row was skipped before staging (for example, missing required
                            TotalCost), so no staged snapshot exists.
                          </div>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="message-muted">No error details loaded yet.</p>
          )}
        </section>

        <section className="section-card span-12">
          <div className="section-header">
            <div>
              <h2 className="section-title">AI SQL Assistant</h2>
              <p className="section-subtitle">
                Ask a natural-language question, review generated SQL, and inspect query results.
              </p>
            </div>
          </div>

          <div className="field-stack">
            <label className="field-label" htmlFor="ai-question">
              Question
            </label>
            <textarea
              id="ai-question"
              className="text-input ai-textarea"
              placeholder="e.g. Show top 10 Level2 elements by total cost"
              value={aiQuestion}
              onChange={(event) => setAiQuestion(event.target.value)}
            />
            <div className="preset-row">
              {AI_PRESET_QUESTIONS.map((preset) => (
                <button
                  key={preset}
                  type="button"
                  className="button-secondary preset-button"
                  onClick={() => setAiQuestion(preset)}
                >
                  {preset}
                </button>
              ))}
            </div>
          </div>

          <div className="action-row">
            <button
              className="button"
              onClick={() => void handleRunAIQuery()}
              disabled={busyAction === "ai-query"}
            >
              {busyAction === "ai-query" ? "Running..." : "Run AI query"}
            </button>
          </div>

          {aiError ? <div className="message-error">{aiError}</div> : null}

          {aiResult ? (
            <>
              <div className="summary-grid" style={{ marginTop: "18px" }}>
                <div className="summary-item">
                  <span className="summary-item-label">Question</span>
                  <span className="summary-item-value">{aiResult.question}</span>
                </div>
                <div className="summary-item">
                  <span className="summary-item-label">Rows returned</span>
                  <span className="summary-item-value">{aiResult.row_count}</span>
                </div>
              </div>

              <details className="row-data-details" style={{ marginTop: "12px" }} open>
                <summary>Generated SQL</summary>
                <pre className="row-data-pre">{aiResult.generated_sql}</pre>
              </details>

              {aiResult.rows.length > 0 ? (
                <div className="table-wrap" style={{ marginTop: "12px" }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        {Object.keys(aiResult.rows[0]).map((col) => (
                          <th key={col}>{col}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {aiResult.rows.map((row, idx) => (
                        <tr key={idx}>
                          {Object.keys(aiResult.rows[0]).map((col) => (
                            <td key={`${idx}-${col}`}>{String(row[col] ?? "")}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="message-muted" style={{ marginTop: "12px" }}>
                  Query ran successfully but returned no rows.
                </p>
              )}
            </>
          ) : null}
        </section>
      </section>
    </main>
  );
}

export default App;
