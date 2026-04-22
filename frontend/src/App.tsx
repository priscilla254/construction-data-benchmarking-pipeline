import { useMemo, useState } from "react";
import { Editor } from "@tinymce/tinymce-react";
import "tinymce/tinymce";
import "tinymce/icons/default";
import "tinymce/themes/silver";
import "tinymce/models/dom";
import "tinymce/plugins/lists";
import "tinymce/plugins/link";
import "tinymce/plugins/table";
import "tinymce/plugins/code";
import "tinymce/skins/ui/oxide/skin.min.css";
import "tinymce/skins/content/default/content.min.css";

import {
  createAIReportDraft,
  exportAIReportDocx,
  exportAIReportPdf,
  getBatchErrorCounts,
  getBatchErrorRows,
  getBatchSummary,
  getErrorDownloadUrl,
  runAIQuery,
  saveAIReportDraft,
  uploadWorkbookWithProgress,
} from "./api/ingestion";
import type {
  AIReportDraftResponse,
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

type PageId = "ingestion" | "ai-report" | "ai-qs";
type EditableDraftSections = {
  executiveSummaryBody: string;
  executiveSummaryRecommendation: string;
  commercialAnalysisBody: string;
};
type SectionEditState = {
  executiveSummary: boolean;
  commercialAnalysis: boolean;
};

function toEditorHtml(value: unknown): string {
  const text = String(value ?? "").trim();
  if (!text) {
    return "<p></p>";
  }
  // If backend text is plain text/newline based, convert to simple paragraphs.
  if (!/[<>]/.test(text)) {
    return text
      .split(/\n{2,}/)
      .map((part) => `<p>${part.replace(/\n/g, "<br />")}</p>`)
      .join("");
  }
  return text;
}

function App() {
  const [activePage, setActivePage] = useState<PageId>("ingestion");
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
  const [reportProjectNumber, setReportProjectNumber] = useState("");
  const [reportDraft, setReportDraft] = useState<AIReportDraftResponse | null>(null);
  const [reportDraftError, setReportDraftError] = useState<string | null>(null);
  const [editableDraftSections, setEditableDraftSections] = useState<EditableDraftSections | null>(
    null,
  );
  const [reportDraftSavedAt, setReportDraftSavedAt] = useState<string | null>(null);
  const [sectionEditState, setSectionEditState] = useState<SectionEditState>({
    executiveSummary: false,
    commercialAnalysis: false,
  });

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

  async function handleGenerateReportDraft(regenerateFresh = false) {
    if (!reportProjectNumber.trim()) {
      setReportDraftError("Enter a Project ID to generate a report draft.");
      return;
    }

    setBusyAction("ai-report-draft");
    setReportDraftError(null);
    setReportDraft(null);
    setEditableDraftSections(null);
    setReportDraftSavedAt(null);
    setSectionEditState({
      executiveSummary: false,
      commercialAnalysis: false,
    });
    try {
      const result = await createAIReportDraft(reportProjectNumber.trim(), { regenerateFresh });
      setReportDraft(result);
      const sections = result.draft_sections as Record<string, unknown>;
      const executive = (sections.executive_summary ?? {}) as Record<string, unknown>;
      const commercial = (sections.commercial_analysis ?? {}) as Record<string, unknown>;
      setEditableDraftSections({
        executiveSummaryBody: toEditorHtml(executive.body),
        executiveSummaryRecommendation: toEditorHtml(executive.recommendation),
        commercialAnalysisBody: toEditorHtml(commercial.body),
      });
    } catch (error) {
      setReportDraftError(
        error instanceof Error ? error.message : "Failed to generate AI report draft.",
      );
    } finally {
      setBusyAction(null);
    }
  }

  async function handleSaveReportDraft() {
    if (!reportDraft || !editableDraftSections) {
      setReportDraftError("Generate a draft before saving.");
      return;
    }
    setBusyAction("ai-report-save");
    setReportDraftError(null);
    try {
      const nextDraftSections = buildCurrentDraftSections(reportDraft, editableDraftSections);
      const saved = await saveAIReportDraft({
        project_id: reportDraft.project_id ?? reportProjectNumber,
        load_batch_id: reportDraft.load_batch_id,
        source_file_name: reportDraft.source_file_name,
        draft_sections: nextDraftSections,
      });
      setReportDraft((prev) =>
        prev
          ? {
              ...prev,
              draft_sections: saved.draft_sections,
            }
          : prev,
      );
      setReportDraftSavedAt(saved.saved_at_utc);
      setSectionEditState({
        executiveSummary: false,
        commercialAnalysis: false,
      });
    } catch (error) {
      setReportDraftError(error instanceof Error ? error.message : "Failed to save report draft.");
    } finally {
      setBusyAction(null);
    }
  }

  function buildCurrentDraftSections(
    draft: AIReportDraftResponse,
    editable: EditableDraftSections,
  ): Record<string, unknown> {
    return {
      ...draft.draft_sections,
      executive_summary: {
        ...((draft.draft_sections as Record<string, unknown>).executive_summary as Record<
          string,
          unknown
        >),
        body: editable.executiveSummaryBody,
        recommendation: editable.executiveSummaryRecommendation,
      },
      commercial_analysis: {
        ...((draft.draft_sections as Record<string, unknown>).commercial_analysis as Record<
          string,
          unknown
        >),
        body: editable.commercialAnalysisBody,
      },
    };
  }

  async function handleExportDocx() {
    if (!reportDraft || !editableDraftSections) {
      setReportDraftError("Generate a draft before exporting.");
      return;
    }
    setBusyAction("ai-report-docx");
    setReportDraftError(null);
    try {
      await exportAIReportDocx({
        project_id: reportDraft.project_id ?? reportProjectNumber,
        load_batch_id: reportDraft.load_batch_id,
        source_file_name: reportDraft.source_file_name,
        draft_sections: buildCurrentDraftSections(reportDraft, editableDraftSections),
        report_context: reportDraft.report_context,
      });
    } catch (error) {
      setReportDraftError(error instanceof Error ? error.message : "DOCX export failed.");
    } finally {
      setBusyAction(null);
    }
  }

  async function handleExportPdf() {
    if (!reportDraft || !editableDraftSections) {
      setReportDraftError("Generate a draft before exporting.");
      return;
    }
    setBusyAction("ai-report-pdf");
    setReportDraftError(null);
    try {
      await exportAIReportPdf({
        project_id: reportDraft.project_id ?? reportProjectNumber,
        load_batch_id: reportDraft.load_batch_id,
        source_file_name: reportDraft.source_file_name,
        draft_sections: buildCurrentDraftSections(reportDraft, editableDraftSections),
        report_context: reportDraft.report_context,
      });
    } catch (error) {
      setReportDraftError(error instanceof Error ? error.message : "PDF export failed.");
    } finally {
      setBusyAction(null);
    }
  }

  return (
    <main className="app-shell">
      <div className="top-nav">
        <button
          type="button"
          className={`tab-button ${activePage === "ingestion" ? "tab-button-active" : ""}`}
          onClick={() => setActivePage("ingestion")}
        >
          Ingestion Console
        </button>
        <button
          type="button"
          className={`tab-button ${activePage === "ai-qs" ? "tab-button-active" : ""}`}
          onClick={() => setActivePage("ai-qs")}
        >
          AI QS Assistant
        </button>
        <button
          type="button"
          className={`tab-button ${activePage === "ai-report" ? "tab-button-active" : ""}`}
          onClick={() => setActivePage("ai-report")}
        >
          AI Report Generation
        </button>
      </div>

      {activePage === "ingestion" ? (
        <>
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

      </section>
        </>
      ) : activePage === "ai-report" ? (
        <>
          <section className="hero">
            <div className="hero-card">
              <div className="hero-kicker">AI-assisted report workflow</div>
              <h1>AI Report Generation</h1>
              <p>
                Generate a report draft context from a processed batch. This draft is the backend
                contract for the editable report page and final PDF/DOCX output flow.
              </p>
            </div>
          </section>

          <section className="section-grid">
            <section className="section-card span-12">
              <div className="section-header">
                <div>
                  <h2 className="section-title">Create report draft</h2>
                  <p className="section-subtitle">
                    Enter a Project ID to fetch the latest matching report context.
                  </p>
                </div>
              </div>

              <div className="field-stack">
                <label className="field-label" htmlFor="report-project-number">
                  Project ID
                </label>
                <input
                  id="report-project-number"
                  className="text-input"
                  placeholder="e.g. P2402"
                  value={reportProjectNumber}
                  onChange={(event) => setReportProjectNumber(event.target.value)}
                />
              </div>

              <div className="action-row">
                <button
                  className="button"
                  onClick={() => void handleGenerateReportDraft(false)}
                  disabled={busyAction === "ai-report-draft"}
                >
                  {busyAction === "ai-report-draft" ? "Generating..." : "Generate report draft"}
                </button>
                <button
                  className="button-secondary"
                  onClick={() => void handleGenerateReportDraft(true)}
                  disabled={busyAction === "ai-report-draft"}
                >
                  {busyAction === "ai-report-draft" ? "Regenerating..." : "Regenerate fresh"}
                </button>
                <button
                  className="button-secondary"
                  onClick={() => void handleSaveReportDraft()}
                  disabled={
                    busyAction === "ai-report-save" ||
                    !reportDraft ||
                    !editableDraftSections
                  }
                >
                  {busyAction === "ai-report-save" ? "Saving..." : "Save draft edits"}
                </button>
                <button
                  className="button-secondary"
                  onClick={() => void handleExportDocx()}
                  disabled={busyAction === "ai-report-docx" || !reportDraft || !editableDraftSections}
                >
                  {busyAction === "ai-report-docx" ? "Generating DOCX..." : "Generate .docx"}
                </button>
                <button
                  className="button-secondary"
                  onClick={() => void handleExportPdf()}
                  disabled={busyAction === "ai-report-pdf" || !reportDraft || !editableDraftSections}
                >
                  {busyAction === "ai-report-pdf" ? "Generating PDF..." : "Generate .pdf"}
                </button>
              </div>

              {reportDraftError ? <div className="message-error">{reportDraftError}</div> : null}
              {reportDraft?.saved_draft_loaded && reportDraft.saved_at_utc ? (
                <div className="message-success">
                  Saved draft loaded from {new Date(reportDraft.saved_at_utc).toLocaleString()}.
                </div>
              ) : null}
              {reportDraftSavedAt ? (
                <div className="message-success">
                  Draft saved successfully at {new Date(reportDraftSavedAt).toLocaleString()}.
                </div>
              ) : null}

              {reportDraft ? (
                <>
                  <div className="summary-grid" style={{ marginTop: "18px" }}>
                    <div className="summary-item">
                      <span className="summary-item-label">Source file</span>
                      <span className="summary-item-value">
                        {reportDraft.source_file_name ?? "-"}
                      </span>
                    </div>
                  </div>

                  <details className="row-data-details" style={{ marginTop: "12px" }} open>
                    <summary>Draft sections JSON</summary>
                    <pre className="row-data-pre">
                      {JSON.stringify(reportDraft.draft_sections, null, 2)}
                    </pre>
                  </details>
                  {editableDraftSections ? (
                    <div className="section-grid" style={{ marginTop: "12px" }}>
                      <section className="section-card span-12 report-section-editor">
                        <div className="section-header">
                          <div>
                            <h3 className="section-title">Executive Summary</h3>
                          </div>
                          <button
                            type="button"
                            className="button-secondary"
                            onClick={() =>
                              setSectionEditState((prev) => ({
                                ...prev,
                                executiveSummary: !prev.executiveSummary,
                              }))
                            }
                          >
                            {sectionEditState.executiveSummary ? "Cancel edit" : "Edit section"}
                          </button>
                        </div>
                        <div className="field-stack">
                          <label className="field-label" htmlFor="exec-body">
                            Summary body
                          </label>
                          {sectionEditState.executiveSummary ? (
                            <Editor
                              id="exec-body"
                              value={editableDraftSections.executiveSummaryBody}
                              onEditorChange={(value) =>
                                setEditableDraftSections((prev) =>
                                  prev ? { ...prev, executiveSummaryBody: value } : prev,
                                )
                              }
                              init={{
                                license_key: "gpl",
                                height: 220,
                                menubar: false,
                                branding: false,
                                plugins: ["lists", "link", "table", "code"],
                                toolbar:
                                  "undo redo | blocks | bold italic underline | bullist numlist | table | link | code",
                              }}
                            />
                          ) : (
                            <div
                              className="report-preview-html"
                              dangerouslySetInnerHTML={{ __html: editableDraftSections.executiveSummaryBody }}
                            />
                          )}
                        </div>
                        <div className="field-stack" style={{ marginTop: "10px" }}>
                          <label className="field-label" htmlFor="exec-recommendation">
                            Recommendation
                          </label>
                          {sectionEditState.executiveSummary ? (
                            <Editor
                              id="exec-recommendation"
                              value={editableDraftSections.executiveSummaryRecommendation}
                              onEditorChange={(value) =>
                                setEditableDraftSections((prev) =>
                                  prev ? { ...prev, executiveSummaryRecommendation: value } : prev,
                                )
                              }
                              init={{
                                license_key: "gpl",
                                height: 180,
                                menubar: false,
                                branding: false,
                                plugins: ["lists", "link"],
                                toolbar:
                                  "undo redo | blocks | bold italic underline | bullist numlist | link",
                              }}
                            />
                          ) : (
                            <div
                              className="report-preview-html"
                              dangerouslySetInnerHTML={{
                                __html: editableDraftSections.executiveSummaryRecommendation,
                              }}
                            />
                          )}
                        </div>
                      </section>
                      <section className="section-card span-12 report-section-editor">
                        <div className="section-header">
                          <div>
                            <h3 className="section-title">Commercial Analysis</h3>
                          </div>
                          <button
                            type="button"
                            className="button-secondary"
                            onClick={() =>
                              setSectionEditState((prev) => ({
                                ...prev,
                                commercialAnalysis: !prev.commercialAnalysis,
                              }))
                            }
                          >
                            {sectionEditState.commercialAnalysis ? "Cancel edit" : "Edit section"}
                          </button>
                        </div>
                        <div className="field-stack">
                          <label className="field-label" htmlFor="commercial-body">
                            Analysis body
                          </label>
                          {sectionEditState.commercialAnalysis ? (
                            <Editor
                              id="commercial-body"
                              value={editableDraftSections.commercialAnalysisBody}
                              onEditorChange={(value) =>
                                setEditableDraftSections((prev) =>
                                  prev ? { ...prev, commercialAnalysisBody: value } : prev,
                                )
                              }
                              init={{
                                license_key: "gpl",
                                height: 240,
                                menubar: false,
                                branding: false,
                                plugins: ["lists", "link", "table", "code"],
                                toolbar:
                                  "undo redo | blocks | bold italic underline | bullist numlist | table | link | code",
                              }}
                            />
                          ) : (
                            <div
                              className="report-preview-html"
                              dangerouslySetInnerHTML={{ __html: editableDraftSections.commercialAnalysisBody }}
                            />
                          )}
                        </div>
                      </section>
                    </div>
                  ) : null}
                  <details className="row-data-details" style={{ marginTop: "12px" }}>
                    <summary>Report context JSON</summary>
                    <pre className="row-data-pre">
                      {JSON.stringify(reportDraft.report_context, null, 2)}
                    </pre>
                  </details>
                </>
              ) : (
                <p className="message-muted" style={{ marginTop: "14px" }}>
                  No draft generated yet.
                </p>
              )}
            </section>
          </section>
        </>
      ) : (
        <>
          <section className="hero">
            <div className="hero-card">
              <div className="hero-kicker">Natural language SQL</div>
              <h1>AI QS Assistant</h1>
              <p>
                Ask natural-language benchmarking questions, inspect the generated SQL, and review
                query results in one dedicated page.
              </p>
            </div>
          </section>

          <section className="section-grid">
            <section className="section-card span-12">
              <div className="section-header">
                <div>
                  <h2 className="section-title">AI SQL Assistant</h2>
                  <p className="section-subtitle">
                    Ask a natural-language question, review generated SQL, and inspect query
                    results.
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
        </>
      )}
    </main>
  );
}

export default App;
