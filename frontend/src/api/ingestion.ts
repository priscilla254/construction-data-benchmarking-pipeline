import type {
  AIReportDraftResponse,
  AIReportDraftSaveResponse,
  AIQueryResponse,
  BatchSummary,
  IngestionRunResponse,
  ValidationErrorCount,
  ValidationErrorDetail,
  ValidationErrorRow,
} from "../types/ingestion";

async function parseJsonResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = (await response.json()) as { detail?: string };
      detail = body.detail ?? detail;
    } catch {
      // Ignore JSON parsing issues and fall back to the status text.
    }
    throw new Error(detail || "Request failed.");
  }

  return (await response.json()) as T;
}

export async function uploadWorkbook(file: File): Promise<IngestionRunResponse> {
  return uploadWorkbookWithProgress(file);
}

export async function uploadWorkbookWithProgress(
  file: File,
  onProgress?: (percent: number) => void,
): Promise<IngestionRunResponse> {
  const formData = new FormData();
  formData.append("file", file);

  return new Promise<IngestionRunResponse>((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", "/api/ingestion/upload");

    request.upload.onprogress = (event) => {
      if (!onProgress || !event.lengthComputable) {
        return;
      }
      const percent = Math.min(100, Math.round((event.loaded / event.total) * 100));
      onProgress(percent);
    };

    request.onerror = () => {
      reject(new Error("Upload failed."));
    };

    request.onload = () => {
      try {
        const responseText = request.responseText || "{}";
        const parsed = JSON.parse(responseText) as IngestionRunResponse & { detail?: string };

        if (request.status >= 200 && request.status < 300) {
          onProgress?.(100);
          resolve(parsed);
          return;
        }

        reject(new Error(parsed.detail ?? request.statusText ?? "Upload failed."));
      } catch {
        reject(new Error(request.statusText || "Upload failed."));
      }
    };

    request.send(formData);
  });
}

export async function getBatchSummary(loadBatchId: string): Promise<BatchSummary> {
  const response = await fetch(`/api/batches/${loadBatchId}/summary`);
  return parseJsonResponse<BatchSummary>(response);
}

export async function getBatchErrorCounts(
  loadBatchId: string,
): Promise<ValidationErrorCount[]> {
  const response = await fetch(`/api/batches/${loadBatchId}/error-counts`);
  return parseJsonResponse<ValidationErrorCount[]>(response);
}

export async function getBatchErrorDetails(
  loadBatchId: string,
): Promise<ValidationErrorDetail[]> {
  const response = await fetch(`/api/batches/${loadBatchId}/error-details`);
  return parseJsonResponse<ValidationErrorDetail[]>(response);
}

export async function getBatchErrorRows(
  loadBatchId: string,
): Promise<ValidationErrorRow[]> {
  const response = await fetch(`/api/batches/${loadBatchId}/error-rows`);
  return parseJsonResponse<ValidationErrorRow[]>(response);
}

export function getErrorDownloadUrl(loadBatchId: string): string {
  return `/api/batches/${loadBatchId}/download-errors`;
}

export async function runAIQuery(question: string): Promise<AIQueryResponse> {
  const response = await fetch("/api/ai/query", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ question }),
  });
  return parseJsonResponse<AIQueryResponse>(response);
}

export async function createAIReportDraft(
  projectId: string,
  options?: { regenerateFresh?: boolean },
): Promise<AIReportDraftResponse> {
  const response = await fetch("/api/ai/report-draft", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      project_id: projectId,
      regenerate_fresh: Boolean(options?.regenerateFresh),
    }),
  });
  return parseJsonResponse<AIReportDraftResponse>(response);
}

export async function saveAIReportDraft(payload: {
  project_id?: string | null;
  load_batch_id: string;
  source_file_name?: string | null;
  draft_sections: Record<string, unknown>;
}): Promise<AIReportDraftSaveResponse> {
  const response = await fetch("/api/ai/report-draft/save", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<AIReportDraftSaveResponse>(response);
}

async function exportAIReport(
  endpoint: string,
  payload: {
    project_id?: string | null;
    load_batch_id: string;
    source_file_name?: string | null;
    draft_sections: Record<string, unknown>;
    report_context: Record<string, unknown>;
  },
): Promise<void> {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      const body = (await response.json()) as { detail?: string };
      detail = body.detail ?? detail;
    } catch {
      // no-op
    }
    throw new Error(detail || "Export failed.");
  }
  const blob = await response.blob();
  const disposition = response.headers.get("Content-Disposition") || "";
  const filenameMatch = disposition.match(/filename="?([^"]+)"?/i);
  const fileName = filenameMatch?.[1] ?? "report";
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = fileName;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export async function exportAIReportDocx(payload: {
  project_id?: string | null;
  load_batch_id: string;
  source_file_name?: string | null;
  draft_sections: Record<string, unknown>;
  report_context: Record<string, unknown>;
}): Promise<void> {
  await exportAIReport("/api/ai/report-export/docx", payload);
}

export async function exportAIReportPdf(payload: {
  project_id?: string | null;
  load_batch_id: string;
  source_file_name?: string | null;
  draft_sections: Record<string, unknown>;
  report_context: Record<string, unknown>;
}): Promise<void> {
  await exportAIReport("/api/ai/report-export/pdf", payload);
}
