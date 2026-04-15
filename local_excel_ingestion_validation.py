from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pandas as pd


# NOTE: This script is intentionally self-contained so it can validate files
# without importing `excel_file_ingestion.py` (which imports `dropbox`).

REQUIRED_SHEETS = [
    "ProjectInformation",
    "ProjectQuants",
    "ElementQuants_L2",
    "Level2",
    "LineItem_L3",
    "costAdjustments",
]

REQUIRED_COLUMNS = {
    "ProjectInformation": ["ProjectID", "ProjectName", "LocationLabel", "SectorCode", "CostStage", "ContractorName"],
    "ProjectQuants": ["ProjectQuantCode", "Qty", "Unit"],
    "ElementQuants_L2": ["L2Code", "QuantTypeCode", "Qty"],
    "Level2": ["L2Code", "L2Name", "TotalCost"],
    "LineItem_L3": ["L2Code", "ItemDescription", "RowType"],
    "costAdjustments": ["AdjCategory", "Amount"],
}


def clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        v = value.strip()
        return v if v != "" else None
    return value


def to_decimal(value: Any) -> Decimal | None:
    value = clean_value(value)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None


def read_workbook(file_like: str) -> dict[str, pd.DataFrame]:
    xls = pd.ExcelFile(file_like, engine="openpyxl")

    missing_sheets = [s for s in REQUIRED_SHEETS if s not in xls.sheet_names]
    if missing_sheets:
        raise ValueError(f"Missing required sheets: {missing_sheets}")

    dataframes: dict[str, pd.DataFrame] = {}
    for sheet in REQUIRED_SHEETS:
        df = pd.read_excel(xls, sheet_name=sheet, engine="openpyxl")
        df.columns = [str(c).strip() for c in df.columns]
        dataframes[sheet] = df
    return dataframes


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Local (no-DB, no-Dropbox) validation for excel_file_ingestion.py."
    )
    parser.add_argument("--file", required=True, help="Path to the .xlsx file to validate")
    args = parser.parse_args()

    try:
        dataframes: dict[str, pd.DataFrame] = read_workbook(args.file)
    except Exception as e:
        print(f"FAILED to read workbook: {type(e).__name__}: {e}")
        raise SystemExit(1)

    errors: list[dict[str, Any]] = []

    # Required columns checks
    for sheet_name, df in dataframes.items():
        for col in REQUIRED_COLUMNS[sheet_name]:
            if col not in df.columns:
                errors.append(
                    {
                        "sheet": sheet_name,
                        "row_num": None,
                        "column": col,
                        "type": "MISSING_COLUMN",
                        "message": f"Missing required column '{col}' in sheet '{sheet_name}'",
                    }
                )

    # ProjectInformation should usually contain exactly 1 populated row
    pi_df = dataframes.get("ProjectInformation")
    if pi_df is not None:
        non_blank_rows = pi_df.dropna(how="all")
        if len(non_blank_rows) != 1:
            errors.append(
                {
                    "sheet": "ProjectInformation",
                    "row_num": None,
                    "column": None,
                    "type": "ROW_COUNT",
                    "message": "ProjectInformation should contain exactly 1 populated row",
                }
            )

    # Level2 total cost numeric check
    lvl2_df = dataframes.get("Level2")
    if lvl2_df is not None and "TotalCost" in lvl2_df.columns:
        for idx, val in enumerate(lvl2_df["TotalCost"], start=2):
            if clean_value(val) is not None and to_decimal(val) is None:
                errors.append(
                    {
                        "sheet": "Level2",
                        "row_num": idx,
                        "column": "TotalCost",
                        "type": "INVALID_NUMBER",
                        "message": f"Invalid TotalCost value: {val}",
                    }
                )

    # LineItem_L3 row type domain check
    l3_df = dataframes.get("LineItem_L3")
    allowed_row_types = {"ITEM", "HEADING", "SUBTOTAL"}
    if l3_df is not None and "RowType" in l3_df.columns:
        for idx, val in enumerate(l3_df["RowType"], start=2):
            cv = clean_value(val)
            if cv is not None and str(cv).upper() not in allowed_row_types:
                errors.append(
                    {
                        "sheet": "LineItem_L3",
                        "row_num": idx,
                        "column": "RowType",
                        "type": "DOMAIN",
                        "message": f"Invalid RowType '{val}'. Allowed: ITEM, HEADING, SUBTOTAL",
                    }
                )

    # ProjectQuants qty numeric check
    pq_df = dataframes.get("ProjectQuants")
    if pq_df is not None and "Qty" in pq_df.columns:
        for idx, val in enumerate(pq_df["Qty"], start=2):
            if clean_value(val) is not None and to_decimal(val) is None:
                errors.append(
                    {
                        "sheet": "ProjectQuants",
                        "row_num": idx,
                        "column": "Qty",
                        "type": "INVALID_NUMBER",
                        "message": f"Invalid Qty value: {val}",
                    }
                )

    if errors:
        print(f"Validation FAILED: {len(errors)} issue(s)\n")
        # Print a compact summary: up to 50 errors
        for i, err in enumerate(errors[:50], start=1):
            loc = f"{err['sheet']}"
            if err["row_num"] is not None:
                loc += f" row {err['row_num']}"
            if err["column"]:
                loc += f" col {err['column']}"
            print(f"{i}. [{err['type']}] {loc} - {err['message']}")
        if len(errors) > 50:
            print(f"... and {len(errors) - 50} more")
        raise SystemExit(2)

    print("Validation PASSED (file matches excel_file_ingestion.py's expected contract).")


if __name__ == "__main__":
    main()

