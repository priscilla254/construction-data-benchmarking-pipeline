from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

import excel_file_ingestion as ingestion


STATUS_PROGRESS = {
    "RECEIVED": 15,
    "STAGED": 55,
    "VALIDATED": 80,
    "COMMITTED": 100,
    "FAILED": 100,
}


def _query_to_dataframe(query: str, params: list | None = None) -> pd.DataFrame:
    conn = ingestion.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, params or [])
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description] if cur.description else []
        if not rows:
            return pd.DataFrame(columns=cols)
        return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        conn.close()


def get_batch_summary(load_batch_id: str) -> pd.DataFrame:
    query = """
        SELECT
            LoadBatchID,
            SourceFileName,
            BatchStatus,
            ErrorCount,
            CreatedAt
        FROM stg.LoadBatch
        WHERE LoadBatchID = ?
    """
    return _query_to_dataframe(query, [load_batch_id])


def get_error_counts(load_batch_id: str) -> pd.DataFrame:
    query = """
        SELECT
            Severity,
            ErrorType,
            SheetName,
            COUNT(*) AS Cnt
        FROM stg.ValidationError
        WHERE LoadBatchID = ?
        GROUP BY Severity, ErrorType, SheetName
        ORDER BY Cnt DESC
    """
    return _query_to_dataframe(query, [load_batch_id])


def get_error_details(load_batch_id: str) -> pd.DataFrame:
    query = """
        SELECT
            Severity,
            SheetName,
            RowNum,
            ColumnName,
            ErrorType,
            ErrorMessage
        FROM stg.ValidationError
        WHERE LoadBatchID = ?
        ORDER BY
            CASE WHEN Severity = 'ERROR' THEN 0 ELSE 1 END,
            SheetName,
            RowNum
    """
    return _query_to_dataframe(query, [load_batch_id])


def render_status_bar(status: str | None):
    status_norm = (status or "UNKNOWN").upper()
    progress = STATUS_PROGRESS.get(status_norm, 5)
    st.progress(progress, text=f"Batch Status: {status_norm} ({progress}%)")
    if status_norm == "COMMITTED":
        st.success("Committed successfully. Data is now in final tables.")
    elif status_norm == "FAILED":
        st.error("Batch failed. Check validation details below.")
    else:
        st.info(f"Current status: {status_norm}")


st.set_page_config(page_title="Excel Ingestion POC", layout="wide")
st.title("Excel Ingestion POC")
st.caption("Upload a workbook and receive batch status + detailed validation feedback.")

uploaded = st.file_uploader("Upload Excel workbook", type=["xlsx"])

if uploaded is not None:
    if st.button("Run Ingestion", type="primary"):
        progress_placeholder = st.empty()
        progress_placeholder.progress(5, text="Preparing upload...")

        # Save upload to a temp file so existing ingestion logic can process it.
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded.getbuffer())
            temp_path = Path(tmp.name)

        progress_placeholder.progress(20, text="File saved. Running ingestion...")
        result = ingestion.process_local_file(str(temp_path))

        if not result:
            st.error("Ingestion did not return a result object.")
        else:
            load_batch_id = result.get("load_batch_id")
            st.success(f"Processed. LoadBatchID: `{load_batch_id}`")

            summary_df = get_batch_summary(load_batch_id)
            if not summary_df.empty:
                latest_status = str(summary_df.iloc[0]["BatchStatus"])
                render_status_bar(latest_status)
                st.subheader("Batch Summary")
                st.dataframe(summary_df, width="stretch", hide_index=True)
            else:
                # Fallback when summary row is unavailable.
                render_status_bar(result.get("status"))

            counts_df = get_error_counts(load_batch_id)
            st.subheader("Error/Warning Counts")
            if counts_df.empty:
                st.info("No validation records found.")
            else:
                st.dataframe(counts_df, width="stretch", hide_index=True)

            details_df = get_error_details(load_batch_id)
            st.subheader("Validation Details")
            if details_df.empty:
                st.info("No detailed errors/warnings logged.")
            else:
                st.dataframe(details_df, width="stretch", hide_index=True)

                csv_data = details_df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "Download Validation Details (CSV)",
                    data=csv_data,
                    file_name=f"{load_batch_id}_validation_details.csv",
                    mime="text/csv",
                )
