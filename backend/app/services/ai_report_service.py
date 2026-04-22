import json
import os
from copy import deepcopy
from datetime import date
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from groq import Groq

from ingestion_engine import excel_file_ingestion as ingestion


REPORT_TEMPLATE_PATH = (
    Path(__file__).resolve().parent.parent
    / "reporting"
    / "report_context_template.json"
)
SAVED_DRAFTS_DIR = Path(__file__).resolve().parent.parent / "reporting" / "saved_drafts"


def load_report_context_template() -> dict[str, Any]:
    """
    Load the baseline report-context JSON template used by AI report generation.
    """
    with REPORT_TEMPLATE_PATH.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _get_groq_client() -> Groq:
    api_key = os.getenv("GROQ_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY is not configured.")
    return Groq(api_key=api_key)


def _resolve_load_batch_id_from_project_id(project_id: str) -> str:
    sql = """
        SELECT TOP 1
            CAST(pi.LoadBatchID AS NVARCHAR(36)) AS LoadBatchID
        FROM stg.ProjectInformation pi
        INNER JOIN stg.LoadBatch lb
            ON lb.LoadBatchID = pi.LoadBatchID
        WHERE LTRIM(RTRIM(ISNULL(pi.ProjectID, ''))) = ?
        ORDER BY lb.CreatedAt DESC
    """
    rows = ingestion.fetch_all(sql, (project_id.strip(),))
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No batch found for project_id '{project_id}'.",
        )
    return str(rows[0].get("LoadBatchID", "")).strip()


def _coerce_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _fetch_project_information(load_batch_id: str) -> dict[str, Any] | None:
    sql = """
        SELECT TOP 1 *
        FROM stg.ProjectInformation
        WHERE LoadBatchID = ?
        ORDER BY RowNum ASC
    """
    rows = ingestion.fetch_all(sql, (load_batch_id,))
    return rows[0] if rows else None


def _fetch_level2_rows(load_batch_id: str) -> list[dict[str, Any]]:
    sql = """
        SELECT
            L1Name,
            L2Name,
            Rate,
            TotalCost
        FROM stg.Level2
        WHERE LoadBatchID = ?
        ORDER BY RowNum ASC
    """
    return ingestion.fetch_all(sql, (load_batch_id,))


def _fetch_adjustments(load_batch_id: str) -> list[dict[str, Any]]:
    sql = """
        SELECT
            AdjCategory,
            AdjSubType,
            Amount
        FROM stg.Adjustments
        WHERE LoadBatchID = ?
        ORDER BY RowNum ASC
    """
    try:
        return ingestion.fetch_all(sql, (load_batch_id,))
    except Exception:
        # Adjustments staging may not be deployed yet in some environments.
        return []


def _as_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _populate_context_from_staging(
    context: dict[str, Any],
    project_row: dict[str, Any] | None,
    level2_rows: list[dict[str, Any]],
    adjustment_rows: list[dict[str, Any]],
) -> None:
    if project_row:
        context["project"]["project_id"] = project_row.get("ProjectID") or context["project"]["project_id"]
        context["project"]["project_name"] = project_row.get("ProjectName") or context["project"]["project_name"]
        context["project"]["project_description"] = project_row.get("Notes") or ""
        context["project"]["location"] = project_row.get("LocationLabel") or ""

        selected_contractor = (project_row.get("SelectedContractor") or "").strip()
        if selected_contractor:
            context["selected_contractor"]["name"] = selected_contractor
            context["tender_meta"]["tenderers"] = [selected_contractor]
            context["tender_meta"]["responses_count"] = 1
            context["budget_position"]["overall_position_by_contractor"] = [
                {
                    "contractor": selected_contractor,
                    "total": 0.0,
                    "within_budget": False,
                }
            ]

        context["programme"]["contractor_programmes"] = [
            {
                "contractor": selected_contractor,
                "precon_start": "",
                "works_completion": "",
                "notes": (
                    f"Programme length: {project_row.get('ProgrammeLengthInWeeks')} weeks"
                    if project_row.get("ProgrammeLengthInWeeks") is not None
                    else ""
                ),
            }
        ]

    l2_items: list[dict[str, Any]] = []
    total_cost_sum = 0.0
    for row in level2_rows:
        total_cost = _as_float(row.get("TotalCost"))
        rate = _as_float(row.get("Rate"))
        total_cost_sum += total_cost
        l2_items.append(
            {
                "l1_name": row.get("L1Name") or "",
                "l2_name": row.get("L2Name") or "",
                "rate": rate,
                "total_cost": total_cost,
            }
        )
    context["element_analysis"]["selected_contractor_l2"] = l2_items

    fixed_adjustments_sum = 0.0
    risk_adjustments_sum = 0.0
    provisional_sums: list[dict[str, Any]] = []
    for row in adjustment_rows:
        amount = _as_float(row.get("Amount"))
        category = str(row.get("AdjCategory") or "")
        subtype = str(row.get("AdjSubType") or "")
        text = f"{category} {subtype}".lower()
        if "risk" in text:
            risk_adjustments_sum += amount
        else:
            fixed_adjustments_sum += amount
        if "provisional" in text:
            provisional_sums.append(
                {
                    "item": subtype or category or "Provisional item",
                    "status": "captured",
                    "notes": f"Adjustment amount: {amount:,.2f}",
                }
            )

    context["commercial"]["construction_budget"] = total_cost_sum
    context["commercial"]["tender_comparison"] = [
        {
            "contractor": context["selected_contractor"]["name"] or "",
            "initial_tender_sum": total_cost_sum,
            "fixed_adjustments": fixed_adjustments_sum,
            "risk_adjustments": risk_adjustments_sum,
            "final_adjusted_tender_sum": total_cost_sum + fixed_adjustments_sum + risk_adjustments_sum,
            "variance_to_budget": 0.0,
        }
    ]
    context["commercial"]["provisional_sums"] = provisional_sums

    if context["budget_position"]["overall_position_by_contractor"]:
        context["budget_position"]["overall_position_by_contractor"][0]["total"] = (
            total_cost_sum + fixed_adjustments_sum + risk_adjustments_sum
        )
    context["budget_position"]["overall_budget"] = total_cost_sum


def _build_draft_sections(context: dict[str, Any]) -> dict[str, Any]:
    project = context.get("project", {})
    selected = context.get("selected_contractor", {})
    commercial = context.get("commercial", {})
    budget_position = context.get("budget_position", {})
    programme = context.get("programme", {})
    tender_meta = context.get("tender_meta", {})

    project_name = str(project.get("project_name") or "this project")
    project_id = str(project.get("project_id") or "")
    location = str(project.get("location") or "")
    selected_contractor = str(selected.get("name") or "the preferred contractor")
    tenderers = tender_meta.get("tenderers", [])
    if not isinstance(tenderers, list):
        tenderers = []

    tender_comparison = commercial.get("tender_comparison", [])
    top_row = tender_comparison[0] if isinstance(tender_comparison, list) and tender_comparison else {}
    final_sum = _as_float(top_row.get("final_adjusted_tender_sum"))
    fixed_adj = _as_float(top_row.get("fixed_adjustments"))
    risk_adj = _as_float(top_row.get("risk_adjustments"))
    budget_total = _as_float(budget_position.get("overall_budget"))

    programme_rows = programme.get("contractor_programmes", [])
    programme_notes = ""
    if isinstance(programme_rows, list) and programme_rows:
        programme_notes = str(programme_rows[0].get("notes") or "")

    executive_body_fallback = (
        f"Project {project_id} ({project_name})"
        f"{' in ' + location if location else ''} has been evaluated against current commercial benchmarks.\n\n"
        f"{selected_contractor} currently presents the strongest commercial position based on adjusted tender outcomes.\n\n"
        f"Current final adjusted tender sum: {final_sum:,.2f}. "
        f"Applied fixed adjustments: {fixed_adj:,.2f}. "
        f"Applied risk adjustments: {risk_adj:,.2f}."
    )

    commercial_body_fallback = (
        f"Commercial analysis for {project_name} (Project ID: {project_id}) is based on staged Level2 and adjustments data.\n\n"
        f"Construction baseline total: {budget_total:,.2f}.\n"
        f"Final adjusted tender sum: {final_sum:,.2f}.\n"
        f"Fixed adjustments total: {fixed_adj:,.2f}.\n"
        f"Risk adjustments total: {risk_adj:,.2f}.\n\n"
        "QS should review provisional sums and any excluded adjustments before final issue."
    )
    introduction_fallback = (
        f"This report has been prepared by Costplan Services (South East) Ltd for {project_name} "
        f"(Project ID: {project_id}){' in ' + location if location else ''}. "
        "It summarises the tender position, recommendation, and key commercial considerations."
    )

    deterministic_facts = {
        "project_id": project_id,
        "project_name": project_name,
        "location": location,
        "preferred_contractor": selected_contractor,
        "tenderers": tenderers,
        "final_adjusted_tender_sum": final_sum,
        "fixed_adjustments": fixed_adj,
        "risk_adjustments": risk_adj,
        "construction_baseline_total": budget_total,
    }

    executive_body = _generate_groq_section_text(
        section_name="Executive Summary",
        writing_brief=(
            "Write a concise executive summary paragraph suitable for a client tender report. "
            "Mention the preferred contractor and key commercial position."
        ),
        deterministic_facts=deterministic_facts,
        fallback=executive_body_fallback,
    )
    commercial_body = _generate_groq_section_text(
        section_name="Commercial Analysis",
        writing_brief=(
            "Write a concise commercial analysis paragraph for a client report using only the provided facts. "
            "Do not invent or alter numeric values."
        ),
        deterministic_facts=deterministic_facts,
        fallback=commercial_body_fallback,
    )
    introduction_body = _generate_groq_section_text(
        section_name="Introduction",
        writing_brief=(
            "Write an introduction paragraph starting with: "
            "'This report has been prepared by Costplan Services (South East) Ltd'. "
            "Reference the project and location."
        ),
        deterministic_facts=deterministic_facts,
        fallback=introduction_fallback,
    )

    recommendation = _generate_groq_recommendation(
        project_name=project_name,
        selected_contractor=selected_contractor,
        final_sum=final_sum,
        fixed_adj=fixed_adj,
        risk_adj=risk_adj,
        budget_total=budget_total,
    )
    next_steps = _generate_groq_next_steps(
        deterministic_facts=deterministic_facts,
        fallback=[
            "Confirm contractor recommendation with internal review.",
            "Validate budget and risk allowances before client issue.",
            "Prepare issue-ready report pack and appendices.",
        ],
    )

    return {
        "executive_summary": {
            "title": "01 - Executive Summary",
            "body": executive_body,
            "recommendation": recommendation,
            "next_steps": next_steps,
        },
        "commercial_analysis": {
            "title": "04 - Commercial Analysis",
            "body": commercial_body,
            "programme_note": programme_notes,
            "headline_metrics": {
                "construction_baseline_total": budget_total,
                "final_adjusted_tender_sum": final_sum,
                "fixed_adjustments_total": fixed_adj,
                "risk_adjustments_total": risk_adj,
            },
        },
        "introduction": {
            "title": "02 - Introduction",
            "body": introduction_body,
            "tenderers": tenderers,
        },
    }


def _generate_groq_recommendation(
    project_name: str,
    selected_contractor: str,
    final_sum: float,
    fixed_adj: float,
    risk_adj: float,
    budget_total: float,
) -> str:
    fallback = (
        f"We recommend appointing {selected_contractor} as preferred contractor for {project_name} "
        "subject to final commercial clarifications and client approval."
    )
    try:
        client = _get_groq_client()
    except Exception:
        return fallback

    prompt = (
        "Write a concise professional QS recommendation paragraph for a client tender report. "
        "Do not mention staging pipelines, databases, AI, or technical systems. "
        "Do not mention internal identifiers such as Project ID. "
        "Focus on commercial position and decision rationale.\n\n"
        f"Project Name: {project_name}\n"
        f"Preferred Contractor: {selected_contractor}\n"
        f"Final Adjusted Tender Sum: {final_sum:,.2f}\n"
        f"Fixed Adjustments: {fixed_adj:,.2f}\n"
        f"Risk Adjustments: {risk_adj:,.2f}\n"
        f"Construction Baseline: {budget_total:,.2f}\n"
    )
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an expert quantity surveying report writer."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        content = (completion.choices[0].message.content or "").strip()
        if not content:
            return fallback
        return content
    except Exception:
        return fallback


def _generate_groq_section_text(
    section_name: str,
    writing_brief: str,
    deterministic_facts: dict[str, Any],
    fallback: str,
) -> str:
    try:
        client = _get_groq_client()
    except Exception:
        return fallback

    prompt = (
        f"Section: {section_name}\n"
        f"Task: {writing_brief}\n\n"
        "Rules:\n"
        "1. Use a professional quantity surveying tone for a client-facing tender report.\n"
        "2. Do not mention databases, staging tables, SQL, or AI.\n"
        "3. Use only the facts provided below; do not invent or alter numbers.\n"
        "4. Return one concise paragraph only.\n\n"
        f"Facts:\n{json.dumps(deterministic_facts, ensure_ascii=True)}\n"
    )
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an expert quantity surveying report writer."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        content = (completion.choices[0].message.content or "").strip()
        if not content:
            return fallback
        return content
    except Exception:
        return fallback


def _generate_groq_next_steps(
    deterministic_facts: dict[str, Any],
    fallback: list[str],
) -> list[str]:
    try:
        client = _get_groq_client()
    except Exception:
        return fallback

    prompt = (
        "Produce exactly 5 concise recommended next steps for a client-facing tender report.\n"
        "Rules:\n"
        "1. Return JSON only in this exact format: {\"next_steps\": [\"...\", \"...\"]}\n"
        "2. Each step must be action-oriented and commercially practical.\n"
        "3. Do not mention SQL, staging tables, or AI.\n"
        "4. Use only these facts; do not invent numbers.\n\n"
        f"Facts:\n{json.dumps(deterministic_facts, ensure_ascii=True)}\n"
    )
    try:
        completion = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are an expert quantity surveying report writer."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
        )
        content = (completion.choices[0].message.content or "").strip()
        if not content:
            return fallback
        # Remove fenced wrappers if present.
        if content.startswith("```"):
            content = content.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        parsed = json.loads(content)
        items = parsed.get("next_steps") if isinstance(parsed, dict) else None
        if not isinstance(items, list):
            return fallback
        cleaned = [str(item).strip() for item in items if str(item).strip()]
        return cleaned if cleaned else fallback
    except Exception:
        return fallback


def _normalize_client_wording(draft_sections: dict[str, Any]) -> dict[str, Any]:
    """
    Ensure client-facing wording uses 'preferred contractor'.
    """
    text_replacements = {
        "selected contractor": "preferred contractor",
        "Selected contractor": "Preferred contractor",
        "Selected Contractor": "Preferred Contractor",
    }

    def _walk(value: Any) -> Any:
        if isinstance(value, str):
            updated = value
            for old, new in text_replacements.items():
                updated = updated.replace(old, new)
            return updated
        if isinstance(value, list):
            return [_walk(v) for v in value]
        if isinstance(value, dict):
            return {k: _walk(v) for k, v in value.items()}
        return value

    normalized = _walk(draft_sections)
    return normalized if isinstance(normalized, dict) else draft_sections


def _saved_draft_path(load_batch_id: str) -> Path:
    safe_name = load_batch_id.replace("/", "_").replace("\\", "_")
    return SAVED_DRAFTS_DIR / f"{safe_name}.json"


def load_saved_draft(load_batch_id: str) -> dict[str, Any] | None:
    path = _saved_draft_path(load_batch_id)
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as fp:
            parsed = json.load(fp)
            return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def save_report_draft_state(
    load_batch_id: str,
    project_id: str | None,
    source_file_name: str | None,
    draft_sections: dict[str, Any],
) -> dict[str, Any]:
    if not load_batch_id.strip():
        raise HTTPException(status_code=400, detail="load_batch_id is required.")
    SAVED_DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "project_id": project_id,
        "load_batch_id": load_batch_id,
        "source_file_name": source_file_name,
        "draft_sections": draft_sections,
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path = _saved_draft_path(load_batch_id)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, indent=2)
    return payload


def build_report_draft(
    project_id: str | None = None,
    load_batch_id: str | None = None,
    use_saved_draft: bool = True,
) -> dict[str, Any]:
    """
    Build an initial AI report draft context for a project or ingestion batch.
    """
    project_id_clean = (project_id or "").strip()
    load_batch_id_clean = (load_batch_id or "").strip()

    if not project_id_clean and not load_batch_id_clean:
        raise HTTPException(
            status_code=400,
            detail="Either project_id or load_batch_id is required.",
        )

    if not load_batch_id_clean:
        load_batch_id_clean = _resolve_load_batch_id_from_project_id(project_id_clean)

    batch_summary = ingestion.get_batch_summary(load_batch_id_clean)
    if batch_summary is None:
        raise HTTPException(status_code=404, detail="Load batch not found.")

    context = deepcopy(load_report_context_template())

    source_file_name = batch_summary.get("SourceFileName") or ""
    context["audit"]["load_batch_id"] = load_batch_id_clean
    context["audit"]["source_file_name"] = source_file_name
    context["audit"]["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    context["project"]["project_id"] = project_id_clean

    project_row = _fetch_project_information(load_batch_id_clean)
    level2_rows = _fetch_level2_rows(load_batch_id_clean)
    adjustment_rows = _fetch_adjustments(load_batch_id_clean)
    _populate_context_from_staging(context, project_row, level2_rows, adjustment_rows)

    if not context["project"]["project_name"] and source_file_name:
        context["project"]["project_name"] = source_file_name.rsplit(".", 1)[0]
    draft_sections = _build_draft_sections(context)
    saved_draft_loaded = False
    saved_at_utc = None
    if use_saved_draft:
        saved = load_saved_draft(load_batch_id_clean)
        if saved and isinstance(saved.get("draft_sections"), dict):
            draft_sections = saved["draft_sections"]
            saved_draft_loaded = True
            saved_at_utc = saved.get("saved_at_utc")
    draft_sections = _normalize_client_wording(draft_sections)

    return {
        "project_id": context["project"]["project_id"] or project_id_clean or None,
        "load_batch_id": load_batch_id_clean,
        "source_file_name": source_file_name or None,
        "saved_draft_loaded": saved_draft_loaded,
        "saved_at_utc": saved_at_utc,
        "draft_sections": draft_sections,
        "report_context": _coerce_value(context),
    }

