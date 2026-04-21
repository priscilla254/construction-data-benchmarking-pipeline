import type {
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
