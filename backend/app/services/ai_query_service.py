import os
import re

import pandas as pd
from fastapi import HTTPException
from groq import Groq

from ingestion_engine import excel_file_ingestion as ingestion

MAX_AI_SQL_ATTEMPTS = 3
READ_ONLY_SQL_PATTERN = re.compile(r"^\s*select\b", re.IGNORECASE | re.DOTALL)
SQL_FORBIDDEN_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|exec|execute|merge)\b",
    re.IGNORECASE,
)


def _get_groq_client() -> Groq:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is not configured.")
    return Groq(api_key=api_key)


def _extract_sql(text: str) -> str:
    cleaned = (text or "").strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)
        cleaned = cleaned.strip()
    return cleaned


def _validate_sql_read_only(sql: str) -> None:
    if ";" in sql.strip().rstrip(";"):
        raise HTTPException(
            status_code=400,
            detail="Only a single SQL statement is allowed.",
        )
    if not READ_ONLY_SQL_PATTERN.match(sql):
        raise HTTPException(
            status_code=400,
            detail="Generated SQL must start with SELECT.",
        )
    if SQL_FORBIDDEN_PATTERN.search(sql):
        raise HTTPException(
            status_code=400,
            detail="Generated SQL contains forbidden keywords.",
        )


def _build_tables_context(
    table_schema: str,
    table_name_like: str,
    fallback_lines: list[str] | None = None,
) -> str:
    """
    Build a schema context string for matched tables in the database.
    """
    sql = """
        SELECT
            TABLE_SCHEMA,
            TABLE_NAME,
            COLUMN_NAME,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ?
          AND TABLE_NAME LIKE ?
        ORDER BY TABLE_NAME, ORDINAL_POSITION
    """
    try:
        rows = ingestion.fetch_all(sql, (table_schema, table_name_like))
    except Exception:
        return "\n".join(fallback_lines or [])

    if not rows:
        return "\n".join(fallback_lines or [])

    grouped: dict[str, list[str]] = {}
    for row in rows:
        table = f"{row.get('TABLE_SCHEMA')}.{row.get('TABLE_NAME')}"
        grouped.setdefault(table, []).append(str(row.get("COLUMN_NAME")))

    lines = []
    for table_name, cols in grouped.items():
        col_list = ", ".join(cols)
        lines.append(f"- {table_name} ({col_list})")
    return "\n".join(lines)


def generate_sql_from_question(question: str) -> str:
    if not question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty.")

    staging_tables_context = _build_tables_context(
        table_schema="stg",
        table_name_like="%",
        fallback_lines=[
            "- stg.ProjectInformation (ProjectID, ProjectName, SectorCode, GIFA)",
            "- stg.Level2 (LoadBatchID, L1Code, L1Name, L2Code, L2Name, Rate, TotalCost)",
            "- stg.ValidationError (LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity)",
        ],
    )
    dim_tables_context = _build_tables_context(
        table_schema="dbo",
        table_name_like="Dim%",
        fallback_lines=["- dbo.DimSector (SectorKey, SectorCode, SectorName)"],
    )
    fact_tables_context = _build_tables_context(
        table_schema="dbo",
        table_name_like="Fact%",
        fallback_lines=[],
    )

    system_prompt = f"""
You are an expert SQL assistant for a construction benchmarking database.
Return only one read-only SQL SELECT statement and nothing else.

Staging tables in this database:
{staging_tables_context}

Dim tables in this database:
{dim_tables_context}

Fact tables in this database:
{fact_tables_context if fact_tables_context else "- (No dbo.Fact* tables found from schema introspection.)"}

Rules:
1. Return ONLY SQL, no explanation.
2. Use LIKE for flexible text search when filtering text fields.
3. Never generate INSERT/UPDATE/DELETE/DDL/procedure calls.
4. Prefer TOP clauses for broad queries.
5. For business/reporting questions, prefer dbo.Fact* joined with dbo.Dim* tables.
6. For ingestion troubleshooting/debug questions, prefer stg.* tables.
7. IMPORTANT: GIFA is sourced from dbo.DimCostSet (not from staging tables).
8. For "cost per m2" style questions, calculate as total cost divided by GIFA using NULLIF for divide-by-zero protection.
9. When using GIFA in business queries, prefer joining Fact tables to dbo.DimCostSet via available cost set keys.
10. Never invent column names or join keys. Only use exact column names listed in schema context.
11. If a suitable join key is not present in both tables, avoid that join and answer from available tables.
"""

    client = _get_groq_client()
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": question},
        ],
        temperature=0.1,
    )

    content = completion.choices[0].message.content or ""
    sql = _extract_sql(content)
    _validate_sql_read_only(sql)
    return sql


def regenerate_sql_from_error(question: str, failed_sql: str, db_error: str) -> str:
    staging_tables_context = _build_tables_context(
        table_schema="stg",
        table_name_like="%",
        fallback_lines=[
            "- stg.ProjectInformation (ProjectID, ProjectName, SectorCode)",
            "- stg.Level2 (LoadBatchID, L1Code, L1Name, L2Code, L2Name, Rate, TotalCost)",
            "- stg.ValidationError (LoadBatchID, SheetName, RowNum, ColumnName, ErrorType, ErrorMessage, Severity)",
        ],
    )
    dim_tables_context = _build_tables_context(
        table_schema="dbo",
        table_name_like="Dim%",
        fallback_lines=["- dbo.DimSector (SectorKey, SectorCode, SectorName)"],
    )
    fact_tables_context = _build_tables_context(
        table_schema="dbo",
        table_name_like="Fact%",
        fallback_lines=[],
    )

    system_prompt = f"""
You are an expert SQL assistant for a construction benchmarking database.
Return only one read-only SQL SELECT statement and nothing else.

The previous SQL failed. Fix it using the exact schema below.

Staging tables in this database:
{staging_tables_context}

Dim tables in this database:
{dim_tables_context}

Fact tables in this database:
{fact_tables_context if fact_tables_context else "- (No dbo.Fact* tables found from schema introspection.)"}

Rules:
1. Return ONLY SQL, no explanation.
2. Use only columns that appear in the schema context above.
3. Never generate INSERT/UPDATE/DELETE/DDL/procedure calls.
4. For business/reporting questions, prefer dbo.Fact* joined with dbo.Dim* tables.
5. For ingestion troubleshooting/debug questions, prefer stg.* tables.
6. IMPORTANT: GIFA is sourced from dbo.DimCostSet.
7. For "cost per m2" questions, calculate cost/GIFA with NULLIF for divide-by-zero protection.
8. Never invent column names or join keys. Use exact schema names only.
9. If a join key does not exist in both joined tables, remove that join.
"""

    client = _get_groq_client()
    completion = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": (
                    f"Question: {question}\n\n"
                    f"Failed SQL:\n{failed_sql}\n\n"
                    f"Database error:\n{db_error}\n\n"
                    "Rewrite the SQL so it runs successfully and follows the rules."
                ),
            },
        ],
        temperature=0.0,
    )

    content = completion.choices[0].message.content or ""
    repaired_sql = _extract_sql(content)
    _validate_sql_read_only(repaired_sql)
    return repaired_sql


def run_ai_query(question: str) -> dict:
    generated_sql = generate_sql_from_question(question)

    conn = ingestion.get_connection()
    try:
        current_sql = generated_sql
        try:
            df = pd.read_sql_query(current_sql, conn)
            rows = df.to_dict(orient="records")
            return {
                "question": question,
                "generated_sql": current_sql,
                "row_count": len(rows),
                "rows": rows,
            }
        except Exception as first_exc:
            last_exc = first_exc
            for _ in range(1, MAX_AI_SQL_ATTEMPTS):
                repaired_sql = regenerate_sql_from_error(
                    question=question,
                    failed_sql=current_sql,
                    db_error=str(last_exc),
                )
                try:
                    df = pd.read_sql_query(repaired_sql, conn)
                    rows = df.to_dict(orient="records")
                    return {
                        "question": question,
                        "generated_sql": repaired_sql,
                        "row_count": len(rows),
                        "rows": rows,
                    }
                except Exception as repair_exc:
                    current_sql = repaired_sql
                    last_exc = repair_exc

            raise HTTPException(status_code=400, detail=f"Error running query: {last_exc}") from last_exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Error running query: {exc}") from exc
    finally:
        conn.close()
