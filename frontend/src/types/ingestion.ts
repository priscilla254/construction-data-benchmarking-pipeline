export type IngestionRunResponse = {
  load_batch_id: string;
  status: string;
  error_count: number;
  source_file_name?: string | null;
  exception?: string | null;
};

export type BatchSummary = {
  LoadBatchID: string;
  SourceFileName?: string | null;
  SourceFilePath?: string | null;
  BatchStatus: string;
  ErrorCount?: number | null;
  CreatedAt?: string | null;
};

export type ValidationErrorCount = {
  Severity?: string | null;
  ErrorType?: string | null;
  SheetName?: string | null;
  Cnt: number;
};

export type ValidationErrorDetail = {
  Severity?: string | null;
  SheetName?: string | null;
  RowNum?: number | null;
  ColumnName?: string | null;
  ErrorType?: string | null;
  ErrorMessage?: string | null;
};

export type ValidationErrorRow = ValidationErrorDetail & {
  RowData?: Record<string, unknown> | null;
};

export type AIQueryResponse = {
  question: string;
  generated_sql: string;
  row_count: number;
  rows: Record<string, unknown>[];
};
