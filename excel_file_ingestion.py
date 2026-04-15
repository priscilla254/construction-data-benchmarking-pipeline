import io
import os
import re
import traceback
import uuid
import html
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import dropbox
import pandas as pd
import pyodbc # Microsoft SQL Server ODBC driver
from dropbox.files import FileMetadata
from dotenv import load_dotenv
load_dotenv() # Load environment variables from .env file
# ============================================================
# CONFIG
# ============================================================

# Security: do NOT hard-code tokens in source. Set environment variable:
#   $env:DROPBOX_ACCESS_TOKEN="..."
DROPBOX_ACCESS_TOKEN = os.getenv("DBX_TOKEN", "").strip()
DROPBOX_SOURCE_FOLDER = os.getenv("DBX_FOLDER", "").strip()

# Local quick-test mode (set this to an .xlsx path to bypass Dropbox).
LOCAL_TEST_FILE_PATH = os.getenv("LOCAL_TEST_FILE_PATH", "").strip()
PROCESS_ADJUSTMENTS = os.getenv("PROCESS_ADJUSTMENTS", "0").strip().lower() in {"1", "true", "yes", "y"}
DEBUG_LEVEL2 = os.getenv("DEBUG_LEVEL2", "0").strip().lower() in {"1", "true", "yes", "y"}

# Keep Dropbox token optional so local test mode can run without Dropbox credentials.

SERVER = os.getenv("SQL_SERVER")
DATABASE = os.getenv("SQL_DB")

SQL_CONNECTION_STRING = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=PRISCILLA_BAIYA\SQLEXPRESS;"
    r"DATABASE=benchmarking_test_final;"
    r"Trusted_Connection=yes;"
    r"TrustServerCertificate=yes;"
)


# Expected workbook base sheets (names must match exactly)
REQUIRED_BASE_SHEETS = [
    "ProjectInformation",
    "ProjectQuants",
    "ElementQuants_L2",
    "SUMMARY",
]
if PROCESS_ADJUSTMENTS:
    REQUIRED_BASE_SHEETS.append("Adjustments") # Adjustments sheet is optional, so only add if PROCESS_ADJUSTMENTS is True

# Column maps: Excel header -> staging column name
COLUMN_MAPS = {
    "ProjectInformation": {
        "ProjectID": "ProjectID",
        "ProjectName": "ProjectName",
        "ClientName": "ClientName",
        "LocationLabel": "LocationLabel",
        "SectorCode": "SectorCode",
        "CostStage": "CostStage",
        "BudgetStage": "BudgetStage",
        "SelectedContractor": "SelectedContractor",
        "DataStatus": "DataStatus",
        "Demolition": "Demolition",
        "NewBuild": "NewBuild",
        "Refurbishment": "Refurbishment",
        "HorizontalExtension": "HorizontalExtension",
        "VerticalExtension": "VerticalExtension",
        "Basement": "Basement",
        "Asbestos": "Asbestos",
        "Contamination": "Contamination",
        "BaseDate": "BaseDate",
        "Currency": "Currency",
        "ProgrammeLengthInWeeks": "ProgrammeLengthInWeeks",
        "ProgrammeType": "ProgrammeType",
        "GIFA": "GIFA",
        "Notes": "Notes",
    },
    "ProjectQuants": {
        "ProjectQuantCode": "ProjectQuantCode",
        "ProjectQuantName": "ProjectQuantName",
        "Qty": "Qty",
        "Unit": "Unit",
        "Comment": "Comment",
    },
    "ElementQuants_L2": {
        "L2Code": "L2Code",
        "L2Name": "L2Name",
        "Qty": "Qty",
        "Unit": "Unit",
        "Comment": "Comment",
    },
    "Level2": {
        "L1Code": "L1Code",
        "L1Name": "L1Name",
        "L2Code": "L2Code",
        "L2Name": "L2Name",
        "Rate": "Rate",
        "TotalCost": "TotalCost",

    },
    "LineItem_L3": {
        "L2Code": "L2Code",
        "L2Name": "L2Name",
        "LineID": "LineID",
        "DisplayOrder": "DisplayOrder",
        "ItemDescription": "ItemDescription",
        "Qty": "Quantity",
        "Unit": "Unit",
        "Rate": "Rate",
        "Total": "TotalCost",
        "RowType": "RowType",
    },
    "Adjustments": {
        "AdjCategory": "AdjCategory",
        "AdjSubType": "AdjSubType",
        "Amount": "Amount",
        "Method": "Method",
        "RatePercent": "RatePercent",
        "AppliedToBase": "AppliedToBase",
        "IncludedInComparison": "IncludedInComparison",
    },
}

# Required Excel columns per sheet (minimum contract)
REQUIRED_COLUMNS = {
    "ProjectInformation": ["ProjectID", "ProjectName", "LocationLabel", "SectorCode", "CostStage", "SelectedContractor"],
    "ProjectQuants": ["Name", "Qty", "Unit"],
    "ElementQuants_L2": ["L2Code", "QuantTypeCode", "Qty"],
    "Level2": ["L2Code", "L2Name", "TotalCost"],
    "LineItem_L3": ["L2Code", "ItemDescription", "RowType"],
    "Adjustments": ["AdjCategory", "Amount"],
}

# Staging table names
STAGING_TABLES = {
    "ProjectInformation": "stg.ProjectInformation",
    "ProjectQuants": "stg.ProjectQuants",
    "ElementQuants_L2": "stg.ElementQuants_L2",
    "Level2": "stg.Level2",
    "LineItem_L3": "stg.LineItem_L3",
    "Adjustments": "stg.Adjustments",
}


# ============================================================
# DB HELPERS
# ============================================================
# creates and returns a connection to the 
# sql server using connection string from the environment variables
def get_connection():
    return pyodbc.connect(SQL_CONNECTION_STRING)

# executes a non-query sql statement and commits the changes to the database
# eg INSERT, UPDATE, DELETE, executing stored procedures.
def execute_non_query(sql, params=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()

# executes a query sql statement and returns the first row of the result
# eg checking how many validation errors exist.
def fetch_one(sql, params=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        if params is not None:
            cur.execute(sql, params)
        else:
            cur.execute(sql)
        return cur.fetchone()
    finally:
        conn.close()


# ============================================================
# LOGGING / BATCH CONTROL
# ============================================================
# creates a new ro in stg.LoadBatch and generates a unique load batch id
# every file processed should have its own batch Id for traceability.
def create_load_batch(file_name: str, dropbox_path: str) -> str:
    load_batch_id = str(uuid.uuid4())

    sql = """
        INSERT INTO stg.LoadBatch (
            LoadBatchID,
            SourceFileName,
            SourceFilePath,
            BatchStatus
        )
        VALUES (?, ?, ?, ?)
    """
    execute_non_query(sql, (load_batch_id, file_name, dropbox_path, "RECEIVED"))
    return load_batch_id

# updates the batch status in stg.LoadBatch
# eg RECEIVED, STAGED, VALIDATED, COMMITTED, FAILED
def update_batch_status(load_batch_id: str, status: str):
    sql = """
        UPDATE stg.LoadBatch
        SET BatchStatus = ?
        WHERE LoadBatchID = ?
    """
    execute_non_query(sql, (status, load_batch_id))

# inserts one error row into stg.ValidationError
# used whenever a required column is missing, a value is invalid, the workbook
# shape is wrong or an exception occurs.
def log_validation_error(
    load_batch_id: str,
    sheet_name: str | None = None,
    row_num: int | None = None,
    column_name: str | None = None,
    error_type: str = "VALIDATION",
    error_message: str = "",
    severity: str = "ERROR",
):
    sql = """
        INSERT INTO stg.ValidationError (
            LoadBatchID,
            SheetName,
            RowNum,
            ColumnName,
            ErrorType,
            ErrorMessage,
            Severity
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    execute_non_query(
        sql,
        (
            load_batch_id,
            sheet_name,
            row_num,
            column_name,
            error_type,
            (error_message or "")[:1000],
            severity,
        ),
    )

# counts how many errors exist in stg.ValidationError for a given load batch id where the severity is ERROR
# used to determine can the script continue or the should the batch fail.
def get_error_count(load_batch_id: str) -> int:
    row = fetch_one(
        """
        SELECT COUNT(*)
        FROM stg.ValidationError
        WHERE LoadBatchID = ?
          AND Severity = 'ERROR'
        """,
        (load_batch_id,),
    )
    return int(row[0]) if row else 0

# updates the error count in stg.LoadBatch
# used to track how many errors have been logged for a given batch.
def update_batch_error_count(load_batch_id: str):
    sql = """
        UPDATE lb
        SET ErrorCount = x.ErrorCount
        FROM stg.LoadBatch lb
        CROSS APPLY (
            SELECT COUNT(*) AS ErrorCount
            FROM stg.ValidationError ve
            WHERE ve.LoadBatchID = lb.LoadBatchID
              AND ve.Severity = 'ERROR'
        ) x
        WHERE lb.LoadBatchID = ?
    """
    execute_non_query(sql, (load_batch_id,))


# ============================================================
# DROPBOX HELPERS
# ============================================================
# creates and returns a DropBox API client.
def get_dropbox_client():
    if not DROPBOX_ACCESS_TOKEN:
        raise RuntimeError(
            "DROPBOX_ACCESS_TOKEN is not set. Set it as an environment variable."
        )
    return dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)


def list_dropbox_excel_files(dbx: dropbox.Dropbox, folder_path: str):
    files: list[FileMetadata] = []
    result = dbx.files_list_folder(folder_path)

    while True:
        for entry in result.entries:
            if isinstance(entry, FileMetadata) and entry.name.lower().endswith(".xlsx"):
                files.append(entry)
        if not result.has_more:
            break
        result = dbx.files_list_folder_continue(result.cursor)

    return files


def download_dropbox_file_to_memory(dbx: dropbox.Dropbox, path_lower: str) -> io.BytesIO:
    _, response = dbx.files_download(path_lower)
    return io.BytesIO(response.content)


# ============================================================
# DATA CLEANING HELPERS
# ============================================================
# makes data cleaner before inserting into SQL.
# turns pandas NaN into None, pandas timestamps into datetime objects, etc.
def clean_value(value):
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return value

    if isinstance(value, str):
        value = value.strip()
        return value if value != "" else None

    return value

# converts a value to an integer ideal for fields like DisplayOrder, ProgrammeLengthInWeeks, etc.
def to_int(value):
    value = clean_value(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except Exception:
        return None

# converts a value to a decimal ideal for fields like Rate, TotalCost, etc.
def to_decimal(value):
    value = clean_value(value)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None

# converts a value to a bit ideal for fields like Demolition, NewBuild, etc.
def to_bit(value):
    value = clean_value(value)
    if value is None:
        return None

    if isinstance(value, bool):
        return int(value)

    if isinstance(value, (int, float)):
        return 1 if value else 0

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return 1
    if text in {"0", "false", "no", "n"}:
        return 0
    return None


# resolves the sector code from the workbook to the canonical dbo.DimSector.SectorCode.
# accepts either sector code or sector name from the workbook.
def resolve_sector_code(value: str | None) -> str | None:

    raw = clean_value(value)
    if raw is None:
        return None

    text = str(raw).strip()
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT TOP 1 SectorCode
            FROM dbo.DimSector
            WHERE UPPER(LTRIM(RTRIM(SectorCode))) = UPPER(LTRIM(RTRIM(?)))
               OR UPPER(LTRIM(RTRIM(SectorName))) = UPPER(LTRIM(RTRIM(?)))
            ORDER BY SectorKey
            """,
            (text, text),
        )
        row = cur.fetchone()
        if row and row[0]:
            return str(row[0]).strip()
        # Keep original value if no lookup match; SQL validation/commit will surface it.
        return text
    finally:
        conn.close()

# normalizes text to a lowercase string with only alphanumeric characters.
# used to compare text values in the workbook to the canonical values in the database.
def normalize_text(value) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", _unescape_html_text(value).strip().lower())


def _unescape_html_text(value) -> str:
    """
    Decode HTML entities safely, including double-encoded text like '&amp;amp;'.
    """
    text = str(value)
    previous = None
    current = text
    for _ in range(3):
        if current == previous:
            break
        previous = current
        current = html.unescape(current)
    return current


# gets the selected contractor from the project information sheet.
# preferred columns are SelectedContractor
def get_selected_contractor(project_info_df: pd.DataFrame) -> str | None:
    if project_info_df is None or project_info_df.empty:
        return None

    preferred_cols = ["SelectedContractor"]
    for col in preferred_cols:
        if col in project_info_df.columns:
            series = project_info_df[col].dropna()
            if not series.empty:
                contractor = clean_value(series.iloc[0])
                return str(contractor).strip() if contractor else None
    return None


def detect_selected_contractor_from_sheet_row(raw_df: pd.DataFrame) -> str | None:
    if raw_df is None or raw_df.empty:
        return None

    # Find first non-empty row (works for merged-header style sheets).
    first_row_values = None
    for _, row in raw_df.iterrows():
        vals = [clean_value(v) for v in row.tolist()]
        if any(v is not None for v in vals):
            first_row_values = vals
            break

    if not first_row_values:
        return None

    text_values = [str(v).strip() for v in first_row_values if v is not None]
    if not text_values:
        return None

    # Pattern: "Selected Contractor: <name>" in one merged cell.
    for text in text_values:
        low = text.lower()
        if "selected contractor" in low and ":" in text:
            candidate = text.split(":", 1)[1].strip()
            if candidate:
                return candidate

    # Pattern: one cell has label, next populated cell has value.
    for i, text in enumerate(text_values):
        low = text.lower()
        if "selected contractor" in low or low == "contractor" or "contractor name" in low:
            for j in range(i + 1, len(text_values)):
                candidate = text_values[j].strip()
                if candidate:
                    return candidate

    return None

# detects the selected contractor from the workbook.
# looks for the selected contractor in the first 5 rows of each sheet.
def detect_selected_contractor_from_workbook(xls: pd.ExcelFile) -> str | None:
    for sheet in xls.sheet_names:
        try:
            raw_df = pd.read_excel(
                xls,
                sheet_name=sheet,
                engine="openpyxl",
                header=None,
                nrows=5,
            )
        except Exception:
            continue

        contractor = detect_selected_contractor_from_sheet_row(raw_df)
        if contractor:
            return contractor

    return None

# used when worksheets contain multiple contractor columns to determine columns.
def resolve_metric_column(df: pd.DataFrame, metric_aliases: list[str], selected_contractor: str | None) -> str | None:
    columns = [str(c) for c in df.columns]
    norm_to_original = {normalize_text(c): c for c in columns}

    # 1) Direct canonical match first.
    for alias in metric_aliases:
        n = normalize_text(alias)
        if n in norm_to_original:
            return norm_to_original[n]

    contractor_key = normalize_text(selected_contractor) if selected_contractor else ""

    # 2) If contractor is known, prefer columns that contain BOTH contractor and metric text.
    if contractor_key:
        for col in columns:
            n = normalize_text(col)
            if contractor_key in n and any(normalize_text(a) in n for a in metric_aliases):
                return col

    # 3) Fallback only when metric appears in exactly one column.
    metric_candidates = []
    for col in columns:
        n = normalize_text(col)
        if any(normalize_text(a) in n for a in metric_aliases):
            metric_candidates.append(col)
    if len(metric_candidates) == 1:
        return metric_candidates[0]

    return None


def normalize_contractor_metrics(df: pd.DataFrame, selected_contractor: str | None, context_name: str) -> pd.DataFrame:
    metric_specs = {
        "Qty": ["Qty", "Quantity"],
        "Unit": ["Unit", "UOM"],
        "Rate": ["Rate"],
        "TotalCost": ["TotalCost", "Total", "Amount", "Value"],
    }

    resolved = {}
    for canonical, aliases in metric_specs.items():
        col = resolve_metric_column(df, aliases, selected_contractor)
        if col is None:
            raise ValueError(
                f"Could not resolve '{canonical}' column for selected contractor "
                f"'{selected_contractor or '<unknown>'}' in sheet '{context_name}'."
            )
        resolved[canonical] = col

    out = df.copy()
    for canonical, source_col in resolved.items():
        out[canonical] = out[source_col]

    return out


# infers the row type of an L3 sheet row.
# ITEM requires all core pricing fields; otherwise treat as HEADING.
def infer_l3_row_type(quantity, unit, rate, total_cost) -> str:
    # ITEM requires all core pricing fields; otherwise treat as HEADING.
    has_qty = to_decimal(quantity) is not None
    has_unit = clean_value(unit) is not None
    has_rate = to_decimal(rate) is not None
    has_total = to_decimal(total_cost) is not None
    return "ITEM" if (has_qty and has_unit and has_rate and has_total) else "HEADING"

# to skip blank rows in the L3 sheet.
def is_effectively_blank_row(row: pd.Series) -> bool:
    for value in row.values:
        if clean_value(value) is not None:
            return False
    return True

# finds the first row that looks like the metric header block:
# repeated Qty/Unit/Rate/Total groups.
def _find_l3_metric_header_row(raw_df: pd.DataFrame) -> int | None:
    """
    L3 sheets often have navigation rows first (e.g. "Return to ...").
    Detect the first row that looks like the metric header block:
    repeated Qty/Unit/Rate/Total groups.
    """
    for idx in range(len(raw_df)):
        row_values = [normalize_text(v) for v in raw_df.iloc[idx].tolist()]
        if not row_values:
            continue
        qty_count = sum(1 for v in row_values if v in {"qty", "quantity"})
        unit_count = sum(1 for v in row_values if v in {"unit", "uom"})
        rate_count = sum(1 for v in row_values if v == "rate")
        total_count = sum(1 for v in row_values if v in {"total", "totalcost", "amount", "value"})
        # Need at least one full metric group to consider this header-like.
        if qty_count >= 1 and unit_count >= 1 and rate_count >= 1 and total_count >= 1:
            return idx
    return None


def _find_contiguous_metric_blocks(
    header_row: list,
    expected_tokens: tuple[str, ...],
) -> list[tuple[int, ...]]:
    tokens = [normalize_text(v) for v in header_row]
    blocks: list[tuple[int, ...]] = []
    exp_len = len(expected_tokens)
    for i in range(0, len(tokens) - exp_len + 1):
        if tuple(tokens[i : i + exp_len]) == expected_tokens:
            blocks.append(tuple(range(i, i + exp_len)))
    return blocks


def _select_metric_block_for_contractor(
    raw_df: pd.DataFrame,
    metric_row_idx: int,
    blocks: list[tuple[int, ...]],
    selected_contractor: str | None,
) -> tuple[int, ...] | None:
    if not blocks:
        return None
    if not selected_contractor:
        return blocks[0]

    contractor_key = normalize_text(selected_contractor)
    if not contractor_key:
        return blocks[0]

    # Search rows above metric row for merged-contractor headers near block start col.
    for block in blocks:
        start_col = block[0]
        for r in range(max(0, metric_row_idx - 3), metric_row_idx):
            row_vals = raw_df.iloc[r].tolist()
            left = max(0, start_col - 1)
            right = min(len(row_vals), start_col + 2)
            probe = " ".join(str(v) for v in row_vals[left:right] if clean_value(v) is not None)
            if contractor_key and contractor_key in normalize_text(probe):
                return block

    return blocks[0]


def _format_code_text(value) -> str | None:
    cv = clean_value(value)
    if cv is None:
        return None
    text = str(cv).strip()
    # Normalize trailing ".0" for L1 codes, keep decimals for L2 codes.
    try:
        d = Decimal(text)
    except Exception:
        return text
    return format(d.normalize(), "f")


def _split_l1_l2_code(ref_value) -> tuple[str | None, str | None]:
    code = _format_code_text(ref_value)
    if code is None:
        return None, None
    if "." not in code:
        return code, None
    major, minor = code.split(".", 1)
    if minor.strip("0") == "":
        return f"{major}.0", None
    return f"{major}.0", code


def _normalize_l3_sheet(
    raw_df: pd.DataFrame,
    l2_code: str | None,
    l2_name: str | None,
    selected_contractor: str | None,
) -> pd.DataFrame:
    """
    Build a normalized L3 dataframe with canonical columns expected downstream.
    Uses the first Qty/Unit/Rate/Total block after skipping top nav rows.
    """
    header_idx = _find_l3_metric_header_row(raw_df)
    if header_idx is None:
        raise ValueError("Could not detect L3 metric header row (Qty/Unit/Rate/Total).")

    header_row = raw_df.iloc[header_idx].tolist()
    data_df = raw_df.iloc[header_idx + 1 :].copy()
    if data_df.empty:
        return pd.DataFrame(columns=["L2Code", "L2Name", "ItemDescription", "Quantity", "Unit", "Rate", "TotalCost"])

    blocks = _find_contiguous_metric_blocks(
        header_row,
        ("qty", "unit", "rate", "total"),
    )
    selected_block = _select_metric_block_for_contractor(
        raw_df,
        header_idx,
        blocks,
        selected_contractor,
    )
    if selected_block is None:
        raise ValueError("Could not resolve first Qty/Unit/Rate/Total block in L3 sheet.")
    qty_col, unit_col, rate_col, total_col = selected_block

    # Item description is usually column 1 (second column) in this workbook.
    item_col = 1 if data_df.shape[1] > 1 else 0

    out = pd.DataFrame()
    out["L2Code"] = l2_code
    out["L2Name"] = l2_name
    out["ItemDescription"] = data_df.iloc[:, item_col]
    out["Quantity"] = data_df.iloc[:, qty_col]
    out["Unit"] = data_df.iloc[:, unit_col]
    out["Rate"] = data_df.iloc[:, rate_col]
    out["TotalCost"] = data_df.iloc[:, total_col]
    out["RowType"] = out.apply(
        lambda r: infer_l3_row_type(r.get("Quantity"), r.get("Unit"), r.get("Rate"), r.get("TotalCost")),
        axis=1,
    )
    out = out.dropna(how="all")
    return out


def _normalize_summary_sheet(raw_df: pd.DataFrame, selected_contractor: str | None) -> pd.DataFrame:
    """
    SUMMARY also includes top navigation rows before the real metric header.
    Detect the first Qty/Unit/Rate/Total row and rebuild with canonical headers.
    """
    # Find logical header row containing Code/Ref + Element.
    header_idx = None
    for i in range(min(10, len(raw_df))):
        row_tokens = [normalize_text(v) for v in raw_df.iloc[i].tolist()]
        has_code_col = any(t in {"ref", "reference", "code"} for t in row_tokens)
        has_name_col = any(t in {"element", "name", "l2name"} for t in row_tokens)
        if has_code_col and has_name_col:
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()

    header_row = raw_df.iloc[header_idx].tolist()
    data_df = raw_df.iloc[header_idx + 1 :].copy()
    if data_df.empty:
        return pd.DataFrame()

    ref_col = name_col = rate_col = total_col = None
    selected_block = None
    for c, value in enumerate(header_row):
        token = normalize_text(value)
        if ref_col is None and token in {"ref", "reference", "code"}:
            ref_col = c
        elif name_col is None and token in {"element", "name", "l2name"}:
            name_col = c
        elif rate_col is None and token == "rate":
            rate_col = c
        elif total_col is None and token in {"averagetender", "total", "totalcost", "amount", "value", "cost"}:
            total_col = c

    # If a separate metric row exists with repeating Rate/Total pairs (contractor blocks),
    # choose contractor-specific block; otherwise fall back to header-level "Average Tender"/Total.
    contractor_metric_row_idx = None
    for i in range(header_idx, min(header_idx + 8, len(raw_df))):
        row_tokens = [normalize_text(v) for v in raw_df.iloc[i].tolist()]
        has_rate = row_tokens.count("rate") >= 1
        has_total_like = any(t in {"total", "totalcost", "amount", "value"} for t in row_tokens)
        if has_rate and has_total_like:
            contractor_metric_row_idx = i
            break

    if contractor_metric_row_idx is not None:
        metric_row = raw_df.iloc[contractor_metric_row_idx].tolist()
        # SUMMARY has contractor pairs (Rate, Total/TotalCost/etc.)
        metric_tokens = [normalize_text(v) for v in metric_row]
        blocks = []
        for i in range(0, len(metric_tokens) - 1):
            if metric_tokens[i] == "rate" and metric_tokens[i + 1] in {"total", "totalcost", "amount", "value"}:
                blocks.append((i, i + 1))
        selected_block = _select_metric_block_for_contractor(
            raw_df,
            contractor_metric_row_idx,
            blocks,
            selected_contractor,
        )
        if selected_block is not None:
            rate_col, total_col = selected_block
            # Move data start to after metric header row.
            data_df = raw_df.iloc[contractor_metric_row_idx + 1 :].copy()

    out = pd.DataFrame()
    ref_series = data_df.iloc[:, ref_col] if ref_col is not None else data_df.iloc[:, 0]
    name_series = data_df.iloc[:, name_col] if name_col is not None else data_df.iloc[:, 1]

    l1_codes = []
    l1_names = []
    l2_codes = []
    l2_names = []
    current_l1_code = None
    current_l1_name = None
    for ref_val, name_val in zip(ref_series.tolist(), name_series.tolist()):
        l1_code, l2_code = _split_l1_l2_code(ref_val)
        name_clean = clean_value(name_val)
        if l2_code is None:
            current_l1_code = l1_code
            current_l1_name = name_clean
            l1_codes.append(l1_code)
            l1_names.append(name_clean)
            l2_codes.append(None)
            l2_names.append(None)
        else:
            l1_codes.append(current_l1_code or l1_code)
            l1_names.append(current_l1_name)
            l2_codes.append(l2_code)
            l2_names.append(name_clean)

    out["L1Code"] = l1_codes
    out["L1Name"] = l1_names
    out["L2Code"] = l2_codes
    out["L2Name"] = l2_names
    if rate_col is not None:
        out["Rate"] = data_df.iloc[:, rate_col]
    if total_col is not None:
        out["TotalCost"] = data_df.iloc[:, total_col]
    if "Rate" not in out.columns:
        out["Rate"] = None
    if "TotalCost" not in out.columns:
        out["TotalCost"] = None

    # Keep only L2 rows for Level2 staging.
    out = out[out["L2Code"].notna()].copy()
    if DEBUG_LEVEL2:
        print(
            "[DEBUG_LEVEL2] selected_contractor=",
            selected_contractor or "<unknown>",
            " header_row=",
            header_idx,
            " metric_row=",
            contractor_metric_row_idx,
            " block=",
            selected_block,
            " rate_col=",
            rate_col,
            " total_col=",
            total_col,
        )
        preview_cols = [c for c in ["L1Code", "L1Name", "L2Code", "L2Name", "Rate", "TotalCost"] if c in out.columns]
        print("[DEBUG_LEVEL2] preview:")
        try:
            print(out[preview_cols].head(8).to_string(index=False))
        except Exception:
            print(out.head(8).to_string(index=False))
    return out.dropna(how="all")


def _normalize_project_information_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if "ProjectID" in df.columns and "ProjectName" in df.columns:
        return df

    cols = list(df.columns)
    if len(cols) < 2:
        return df
    key_col, val_col = cols[0], cols[1]

    key_map = {
        "projectid": "ProjectID",
        "projectname": "ProjectName",
        "clientname": "ClientName",
        "location": "LocationLabel",
        "locationlabel": "LocationLabel",
        "region": "LocationLabel",
        "sector": "SectorCode",
        "sectorcode": "SectorCode",
        "coststage": "CostStage",
        "budgetstage": "BudgetStage",
        "contractorname": "SelectedContractor",
        "selectedcontractor": "SelectedContractor",
        "datastatus": "DataStatus",
        "demolition": "Demolition",
        "newbuild": "NewBuild",
        "refurbishment": "Refurbishment",
        "horizontalextension": "HorizontalExtension",
        "verticalextension": "VerticalExtension",
        "basement": "Basement",
        "asbestos": "Asbestos",
        "contamination": "Contamination",
        "basedate": "BaseDate",
        "currency": "Currency",
        "programmelengthinweeks": "ProgrammeLengthInWeeks",
        "programmetype": "ProgrammeType",
        "gifa": "GIFA",
        "notes": "Notes",
    }

    out: dict[str, object] = {}
    for _, row in df.iterrows():
        k = clean_value(row.get(key_col))
        v = clean_value(row.get(val_col))
        if k is None:
            continue
        canonical = key_map.get(normalize_text(k))
        if canonical:
            # Keep first meaningful value; do not overwrite with null from later rows.
            if v is not None or canonical not in out:
                out[canonical] = v

    return pd.DataFrame([out]) if out else df


def _normalize_element_quants_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    rename_map = {}
    for col in out.columns:
        n = normalize_text(col)
        if n in {"elementalquants", "element", "name", "l2name"}:
            rename_map[col] = "L2Name"
        elif n in {"quant", "quantity", "qty"}:
            rename_map[col] = "Qty"
        elif n in {"quanttype", "quanttypecode"}:
            rename_map[col] = "QuantTypeCode"
    out = out.rename(columns=rename_map)

    if "L2Code" not in out.columns and "L2Name" in out.columns:
        out["L2Code"] = [f"L2-{i+1:03d}" for i in range(len(out))]
    if "QuantTypeCode" not in out.columns:
        out["QuantTypeCode"] = "DEFAULT"
    return out


def _normalize_adjustments_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    rename_map = {}
    for col in out.columns:
        n = normalize_text(col)
        if n in {"adjcategory", "category"}:
            rename_map[col] = "AdjCategory"
        elif n in {"adjsubtype", "subtype"}:
            rename_map[col] = "AdjSubType"
        elif n in {"amount", "value", "total"}:
            rename_map[col] = "Amount"
        elif n == "method":
            rename_map[col] = "Method"
        elif n in {"ratepercent", "percent", "rate"}:
            rename_map[col] = "RatePercent"
        elif n == "appliedtobase":
            rename_map[col] = "AppliedToBase"
        elif n == "includedincomparison":
            rename_map[col] = "IncludedInComparison"
    return out.rename(columns=rename_map)


# ============================================================
# WORKBOOK READING / VALIDATION
# ============================================================

def read_workbook(file_like: io.BytesIO):
    xls = pd.ExcelFile(file_like, engine="openpyxl")

    missing_sheets = [s for s in REQUIRED_BASE_SHEETS if s not in xls.sheet_names]
    if missing_sheets:
        raise ValueError(f"Missing required sheets: {missing_sheets}")

    # Level 3 sheets use naming convention: "<L2Code> <L2Name>".
    # Use a stricter pattern so sheets like "Project Information" are not
    # misclassified as L3 (L2 code must contain at least one digit).
    l3_sheet_name_pattern = re.compile(r"^\s*([A-Za-z]*\d+(?:\.\d+)*)\s+(.+?)\s*$")
    # Keep both actual sheet name (for pd.read_excel) and decoded text
    # (for pattern parsing / code-name extraction).
    l3_sheets: list[tuple[str, str]] = []
    for sheet in xls.sheet_names:
        actual_name = str(sheet)
        decoded_name = _unescape_html_text(sheet).strip()
        if decoded_name in REQUIRED_BASE_SHEETS or actual_name in REQUIRED_BASE_SHEETS:
            continue
        if l3_sheet_name_pattern.match(decoded_name):
            l3_sheets.append((actual_name, decoded_name))
    if not l3_sheets:
        raise ValueError(
            "Missing Level 3 sheet(s). Expected at least one sheet named like '<L2Code> <L2Name>'."
        )

    dataframes = {}
    for sheet in REQUIRED_BASE_SHEETS:
        df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        if sheet == "ProjectInformation":
            df = _normalize_project_information_sheet(df)
        elif sheet == "ElementQuants_L2":
            df = _normalize_element_quants_sheet(df)
        elif sheet == "Adjustments":
            df = _normalize_adjustments_sheet(df)
        dataframes[sheet] = df

    selected_contractor = get_selected_contractor(dataframes.get("ProjectInformation"))
    if not selected_contractor:
        selected_contractor = detect_selected_contractor_from_workbook(xls)

    # Combine all Level 3 sheets into one dataframe used by existing pipeline.
    l3_frames = []
    for actual_sheet_name, decoded_sheet_name in l3_sheets:
        raw_l3_df = pd.read_excel(
            xls,
            sheet_name=actual_sheet_name,
            engine="openpyxl",
            header=None,
        )

        m = l3_sheet_name_pattern.match(decoded_sheet_name)
        l2_code_from_sheet = m.group(1).strip() if m else None
        l2_name_from_sheet = _unescape_html_text(m.group(2)).strip() if m else None

        # Skip top navigation rows and build canonical columns from true metric header.
        # Some code-like sheets (e.g., inflation summary tabs) are not true L3 tabs.
        try:
            df = _normalize_l3_sheet(
                raw_l3_df,
                l2_code_from_sheet,
                l2_name_from_sheet,
                selected_contractor,
            )
        except ValueError:
            continue
        l3_frames.append(df)

    dataframes["LineItem_L3"] = (
        pd.concat(l3_frames, ignore_index=True) if l3_frames else pd.DataFrame()
    )

    # Map SUMMARY -> Level2. SUMMARY can have a different tabular layout.
    summary_raw = pd.read_excel(xls, sheet_name="SUMMARY", engine="openpyxl", header=None)
    summary_df = _normalize_summary_sheet(summary_raw, selected_contractor)
    dataframes["Level2"] = summary_df

    return dataframes


def validate_sheet_columns(load_batch_id: str, sheet_name: str, df: pd.DataFrame):
    for col in REQUIRED_COLUMNS[sheet_name]:
        if col not in df.columns:
            log_validation_error(
                load_batch_id=load_batch_id,
                sheet_name=sheet_name,
                column_name=col,
                error_type="MISSING_COLUMN",
                error_message=f"Missing required column '{col}' in sheet '{sheet_name}'",
            )


def validate_workbook_data(load_batch_id: str, dataframes: dict):
    # Column checks only for canonical sheets that have REQUIRED_COLUMNS.
    # Skip intermediate/raw sheets (e.g. SUMMARY) that are transformed later.
    for sheet_name, df in dataframes.items():
        if sheet_name not in REQUIRED_COLUMNS:
            continue
        validate_sheet_columns(load_batch_id, sheet_name, df)

    # ProjectInformation should usually contain exactly 1 row
    pi_df = dataframes["ProjectInformation"]
    non_blank_rows = pi_df.dropna(how="all")
    if len(non_blank_rows) != 1:
        log_validation_error(
            load_batch_id,
            "ProjectInformation",
            error_type="ROW_COUNT",
            error_message="ProjectInformation should contain exactly 1 populated row",
        )

    # Level2 total cost numeric check
    lvl2_df = dataframes["Level2"]
    if "TotalCost" in lvl2_df.columns:
        for idx, val in enumerate(lvl2_df["TotalCost"], start=2):
            if clean_value(val) is not None and to_decimal(val) is None:
                log_validation_error(
                    load_batch_id,
                    "Level2",
                    row_num=idx,
                    column_name="TotalCost",
                    error_type="INVALID_NUMBER",
                    error_message=f"Invalid TotalCost value: {val}",
                )

    # LineItem_L3 row type domain check
    l3_df = dataframes["LineItem_L3"]
    allowed_row_types = {"ITEM", "HEADING", "SUBTOTAL"}
    if "RowType" in l3_df.columns:
        for idx, val in enumerate(l3_df["RowType"], start=2):
            cv = clean_value(val)
            if cv is not None and str(cv).upper() not in allowed_row_types:
                log_validation_error(
                    load_batch_id,
                    "LineItem_L3",
                    row_num=idx,
                    column_name="RowType",
                    error_type="DOMAIN",
                    error_message="Invalid RowType '%s'. Allowed: ITEM, HEADING, SUBTOTAL" % (val,),
                )

    # ProjectQuants qty numeric check
    pq_df = dataframes["ProjectQuants"]
    if "Qty" in pq_df.columns:
        for idx, val in enumerate(pq_df["Qty"], start=2):
            if clean_value(val) is not None and to_decimal(val) is None:
                log_validation_error(
                    load_batch_id,
                    "ProjectQuants",
                    row_num=idx,
                    column_name="Qty",
                    error_type="INVALID_NUMBER",
                    error_message=f"Invalid Qty value: {val}",
                )


# ============================================================
# STAGING INSERTS
# ============================================================

def insert_dataframe_rows(conn, table_name: str, rows: list[dict]):
    if not rows:
        return

    columns = list(rows[0].keys())
    col_sql = ", ".join(columns)
    placeholder_sql = ", ".join(["?"] * len(columns))

    sql = f"INSERT INTO {table_name} ({col_sql}) VALUES ({placeholder_sql})"

    values = []
    for row in rows:
        values.append(tuple(row.get(col) for col in columns))

    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany(sql, values)
    conn.commit()


def _get_decimal_metadata(table_full_name: str) -> dict[str, tuple[int, int]]:
    """
    Returns decimal/numeric metadata for table columns:
    {column_name: (precision, scale)}
    """
    schema, table = table_full_name.split(".")
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COLUMN_NAME, NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
              AND TABLE_NAME = ?
              AND DATA_TYPE IN ('decimal', 'numeric')
            """,
            (schema, table),
        )
        out: dict[str, tuple[int, int]] = {}
        for row in cur.fetchall():
            out[str(row[0])] = (int(row[1]), int(row[2]))
        return out
    finally:
        conn.close()


def _decimal_fits_precision_scale(value: Decimal, precision: int, scale: int) -> bool:
    """
    True if Decimal value can fit into DECIMAL(precision, scale)
    without losing precision.
    """
    sign, digits, exponent = value.as_tuple()
    frac_digits = -exponent if exponent < 0 else 0
    int_digits = len(digits) - frac_digits
    if int_digits < 0:
        int_digits = 0
    return frac_digits <= scale and int_digits <= (precision - scale)


def _coerce_decimal_to_precision_scale(
    value: Decimal | None, precision: int, scale: int
) -> Decimal | None:
    """
    Round decimal to DB scale and ensure it fits DECIMAL(precision, scale).
    Returns None if value still cannot fit.
    """
    if value is None:
        return None
    quant = Decimal("1").scaleb(-scale)  # e.g. scale=2 -> Decimal('0.01')
    rounded = value.quantize(quant, rounding=ROUND_HALF_UP)
    if _decimal_fits_precision_scale(rounded, precision, scale):
        return rounded
    return None


def stage_project_information(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["ProjectInformation"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        # Workbook commonly carries SectorName; resolve to canonical SectorCode.
        mapped["SectorCode"] = resolve_sector_code(mapped.get("SectorCode"))

        # Explicitly coerce bit flags expected by downstream dimensions.
        mapped["Demolition"] = to_bit(row.get("Demolition"))
        mapped["NewBuild"] = to_bit(row.get("NewBuild"))
        mapped["Refurbishment"] = to_bit(row.get("Refurbishment"))
        mapped["HorizontalExtension"] = to_bit(row.get("HorizontalExtension"))
        mapped["VerticalExtension"] = to_bit(row.get("VerticalExtension"))
        mapped["Basement"] = to_bit(row.get("Basement"))
        mapped["Asbestos"] = to_bit(row.get("Asbestos"))
        mapped["Contamination"] = to_bit(row.get("Contamination"))
        mapped["ProgrammeLengthInWeeks"] = to_int(row.get("ProgrammeLengthInWeeks"))
        mapped["GIFA"] = to_decimal(row.get("GIFA"))

        rows.append(mapped)

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["ProjectInformation"], rows)
    finally:
        conn.close()


def stage_project_quants(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["ProjectQuants"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        mapped["Qty"] = to_decimal(row.get("Qty"))
        rows.append(mapped)

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["ProjectQuants"], rows)
    finally:
        conn.close()


def stage_element_quants_l2(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["ElementQuants_L2"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        mapped["Qty"] = to_decimal(row.get("Qty"))
        rows.append(mapped)

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["ElementQuants_L2"], rows)
    finally:
        conn.close()


def stage_level2(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []
    decimal_meta = _get_decimal_metadata(STAGING_TABLES["Level2"])
    level2_decimal_cols = ("Rate", "TotalCost")
    precision_errors = 0

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["Level2"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        mapped["Rate"] = to_decimal(row.get("Rate"))
        mapped["TotalCost"] = to_decimal(row.get("TotalCost"))

        # Skip Level2 rows without TotalCost (FactElementCostL2.TotalCost is NOT NULL).
        if mapped["TotalCost"] is None:
            log_validation_error(
                load_batch_id=load_batch_id,
                sheet_name="Level2",
                row_num=int(idx) + 2,
                column_name="TotalCost",
                error_type="MISSING_TOTALCOST_SKIPPED",
                error_message=(
                    "Skipped Level2 row because TotalCost is null/blank for selected contractor."
                ),
                severity="WARNING",
            )
            continue

        # Coerce decimals to SQL precision/scale (e.g. DECIMAL(18,2) -> round 2 dp).
        for dec_col in level2_decimal_cols:
            dec_val = mapped.get(dec_col)
            if dec_val is None or not isinstance(dec_val, Decimal):
                continue
            if dec_col not in decimal_meta:
                continue
            precision, scale = decimal_meta[dec_col]
            coerced = _coerce_decimal_to_precision_scale(dec_val, precision, scale)
            if coerced is None:
                precision_errors += 1
                log_validation_error(
                    load_batch_id=load_batch_id,
                    sheet_name="Level2",
                    row_num=int(idx) + 2,
                    column_name=dec_col,
                    error_type="DECIMAL_PRECISION",
                    error_message=(
                        f"Value '{dec_val}' does not fit DECIMAL({precision},{scale}) "
                        f"for column '{dec_col}' in {STAGING_TABLES['Level2']}."
                    ),
                )
            else:
                mapped[dec_col] = coerced

        rows.append(mapped)

    if precision_errors > 0:
        raise ValueError(
            f"Level2 contains {precision_errors} value(s) that exceed SQL decimal precision/scale. "
            "See stg.ValidationError for row+column details."
        )

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["Level2"], rows)
    finally:
        conn.close()


def stage_lineitem_l3(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []
    display_order_by_l2: dict[str, int] = {}

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["LineItem_L3"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        mapped["Quantity"] = to_decimal(row.get("Quantity"))
        mapped["Rate"] = to_decimal(row.get("Rate"))
        mapped["TotalCost"] = to_decimal(row.get("TotalCost"))

        l2_key = str(mapped.get("L2Code") or "").strip().upper() or "__NO_L2__"
        next_order = display_order_by_l2.get(l2_key, 0) + 1
        display_order_by_l2[l2_key] = next_order
        mapped["DisplayOrder"] = next_order

        mapped["RowType"] = infer_l3_row_type(
            mapped.get("Quantity"),
            mapped.get("Unit"),
            mapped.get("Rate"),
            mapped.get("TotalCost"),
        )

        rows.append(mapped)

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["LineItem_L3"], rows)
    finally:
        conn.close()


def stage_cost_adjustments(load_batch_id: str, source_file: str, df: pd.DataFrame):
    rows = []

    for idx, row in df.iterrows():
        if is_effectively_blank_row(row):
            continue
        mapped = {
            "LoadBatchID": load_batch_id,
            "RowNum": int(idx) + 2,
            "SourceFileName": source_file,
        }

        for excel_col, db_col in COLUMN_MAPS["Adjustments"].items():
            mapped[db_col] = clean_value(row.get(excel_col))

        mapped["Amount"] = to_decimal(row.get("Amount"))
        mapped["RatePercent"] = to_decimal(row.get("RatePercent"))
        mapped["AppliedToBase"] = to_bit(row.get("AppliedToBase"))
        mapped["IncludedInComparison"] = to_bit(row.get("IncludedInComparison"))

        rows.append(mapped)

    conn = get_connection()
    try:
        insert_dataframe_rows(conn, STAGING_TABLES["Adjustments"], rows)
    finally:
        conn.close()


def stage_all_sheets(load_batch_id: str, source_file: str, dataframes: dict):
    stage_project_information(load_batch_id, source_file, dataframes["ProjectInformation"])
    stage_project_quants(load_batch_id, source_file, dataframes["ProjectQuants"])
    stage_element_quants_l2(load_batch_id, source_file, dataframes["ElementQuants_L2"])
    stage_level2(load_batch_id, source_file, dataframes["Level2"])
    stage_lineitem_l3(load_batch_id, source_file, dataframes["LineItem_L3"])
    if PROCESS_ADJUSTMENTS and "Adjustments" in dataframes:
        stage_cost_adjustments(load_batch_id, source_file, dataframes["Adjustments"])


# ============================================================
# CALL SQL VALIDATION / COMMIT PROCS
# ============================================================

def run_sql_validation(load_batch_id: str):
    # Works whether your proc uses @LoadBatchID or @BatchID as the single param
    sql = "EXEC stg.usp_ValidateBatch ?"
    execute_non_query(sql, (load_batch_id,))


def run_sql_commit(load_batch_id: str):
    sql = "EXEC stg.usp_CommitBatch ?"
    execute_non_query(sql, (load_batch_id,))


# ============================================================
# MAIN FILE PROCESSING
# ============================================================

def process_file(dbx: dropbox.Dropbox, file_meta: FileMetadata):
    file_name = file_meta.name
    file_path = file_meta.path_lower

    print(f"\nProcessing: {file_name}")
    load_batch_id = create_load_batch(file_name, file_path)

    try:
        excel_stream = download_dropbox_file_to_memory(dbx, file_path)
        dataframes = read_workbook(excel_stream)

        validate_workbook_data(load_batch_id, dataframes)
        initial_errors = get_error_count(load_batch_id)
        update_batch_error_count(load_batch_id)

        if initial_errors > 0:
            update_batch_status(load_batch_id, "FAILED")
            print(f"FAILED before staging: {file_name} ({initial_errors} errors)")
            return

        stage_all_sheets(load_batch_id, file_name, dataframes)
        update_batch_status(load_batch_id, "STAGED")

        # DB-side validation after staging
        run_sql_validation(load_batch_id)

        final_errors = get_error_count(load_batch_id)
        update_batch_error_count(load_batch_id)

        if final_errors > 0:
            update_batch_status(load_batch_id, "FAILED")
            print(f"FAILED after staging: {file_name} ({final_errors} errors)")
            return

        update_batch_status(load_batch_id, "VALIDATED")

        # Commit into final schema
        run_sql_commit(load_batch_id)

        update_batch_status(load_batch_id, "COMMITTED")
        print(f"COMMITTED: {file_name}")

    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        log_validation_error(
            load_batch_id=load_batch_id,
            sheet_name=None,
            row_num=None,
            column_name=None,
            error_type="EXCEPTION",
            error_message=error_message + " | " + traceback.format_exc()[:700],
            severity="ERROR",
        )
        update_batch_error_count(load_batch_id)
        update_batch_status(load_batch_id, "FAILED")
        print(f"FAILED: {file_name} - {error_message}")


def process_local_file(local_file_path: str):
    file_name = os.path.basename(local_file_path)
    file_path = local_file_path

    print(f"\nProcessing local file: {file_name}")
    load_batch_id = create_load_batch(file_name, file_path)

    try:
        with open(local_file_path, "rb") as f:
            excel_stream = io.BytesIO(f.read())

        dataframes = read_workbook(excel_stream)

        validate_workbook_data(load_batch_id, dataframes)
        initial_errors = get_error_count(load_batch_id)
        update_batch_error_count(load_batch_id)

        if initial_errors > 0:
            update_batch_status(load_batch_id, "FAILED")
            print(f"FAILED before staging: {file_name} ({initial_errors} errors)")
            return {"load_batch_id": load_batch_id, "status": "FAILED", "error_count": initial_errors}

        stage_all_sheets(load_batch_id, file_name, dataframes)
        update_batch_status(load_batch_id, "STAGED")

        run_sql_validation(load_batch_id)

        final_errors = get_error_count(load_batch_id)
        update_batch_error_count(load_batch_id)

        if final_errors > 0:
            update_batch_status(load_batch_id, "FAILED")
            print(f"FAILED after staging: {file_name} ({final_errors} errors)")
            return {"load_batch_id": load_batch_id, "status": "FAILED", "error_count": final_errors}

        update_batch_status(load_batch_id, "VALIDATED")
        run_sql_commit(load_batch_id)

        update_batch_status(load_batch_id, "COMMITTED")
        print(f"COMMITTED: {file_name}")
        return {"load_batch_id": load_batch_id, "status": "COMMITTED", "error_count": 0}

    except Exception as e:
        error_message = f"{type(e).__name__}: {str(e)}"
        log_validation_error(
            load_batch_id=load_batch_id,
            sheet_name=None,
            row_num=None,
            column_name=None,
            error_type="EXCEPTION",
            error_message=error_message + " | " + traceback.format_exc()[:700],
            severity="ERROR",
        )
        update_batch_error_count(load_batch_id)
        update_batch_status(load_batch_id, "FAILED")
        print(f"FAILED: {file_name} - {error_message}")
        return {
            "load_batch_id": load_batch_id,
            "status": "FAILED",
            "error_count": get_error_count(load_batch_id),
            "exception": error_message,
        }


def main():
    # Local quick-test path (recommended while iterating on template logic).
    if LOCAL_TEST_FILE_PATH:
        process_local_file(LOCAL_TEST_FILE_PATH)
        return

    print("LOCAL_TEST_FILE_PATH is not set.")
    print("Set LOCAL_TEST_FILE_PATH in your .env for manual file testing.")

    # Dropbox mode disabled for quick local testing.
    # dbx = get_dropbox_client()
    # files = list_dropbox_excel_files(dbx, DROPBOX_SOURCE_FOLDER)
    #
    # if not files:
    #     print("No Excel files found in Dropbox folder.")
    #     return
    #
    # print(f"Found {len(files)} Excel file(s).")
    #
    # for file_meta in files:
    #     process_file(dbx, file_meta)


if __name__ == "__main__":
    main()