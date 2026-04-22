from datetime import datetime, timezone
from html import unescape
from pathlib import Path
import re
from typing import Any

from docxtpl import DocxTemplate
from fastapi import HTTPException
from weasyprint import HTML


REPORTING_DIR = Path(__file__).resolve().parent.parent / "reporting"
TEMPLATE_PATH = REPORTING_DIR / "templates" / "TCR_Template.docx"
EXPORTS_DIR = REPORTING_DIR / "exports"


def _slug(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
    return cleaned.strip("_") or "report"


def _to_docx_text(value: Any) -> str:
    """
    Convert potential TinyMCE HTML into plain text safe for docxtpl placeholders.
    """
    text = str(value or "")
    if not text:
        return ""
    # Convert common block tags into line breaks before stripping.
    text = re.sub(r"</(p|div|li|h[1-6])\s*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    # Normalize excessive whitespace/newlines.
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _prepare_docx_context(payload: dict[str, Any]) -> dict[str, Any]:
    report_context = payload.get("report_context", {}) if isinstance(payload.get("report_context"), dict) else {}
    draft_sections = payload.get("draft_sections", {}) if isinstance(payload.get("draft_sections"), dict) else {}
    project = report_context.get("project", {}) if isinstance(report_context.get("project"), dict) else {}
    commercial = report_context.get("commercial", {}) if isinstance(report_context.get("commercial"), dict) else {}
    tender_meta = report_context.get("tender_meta", {}) if isinstance(report_context.get("tender_meta"), dict) else {}
    executive = (
        draft_sections.get("executive_summary", {})
        if isinstance(draft_sections.get("executive_summary"), dict)
        else {}
    )
    commercial_section = (
        draft_sections.get("commercial_analysis", {})
        if isinstance(draft_sections.get("commercial_analysis"), dict)
        else {}
    )
    introduction_section = (
        draft_sections.get("introduction", {})
        if isinstance(draft_sections.get("introduction"), dict)
        else {}
    )

    next_steps = executive.get("next_steps", [])
    if not isinstance(next_steps, list):
        next_steps = []
    tender_rows = commercial.get("tender_comparison", [])
    if not isinstance(tender_rows, list):
        tender_rows = []
    first_tender_row = tender_rows[0] if tender_rows else {}
    tenderers = tender_meta.get("tenderers", [])
    if not isinstance(tenderers, list):
        tenderers = []

    contractor_names = [str(row.get("contractor") or "") for row in tender_rows]
    final_adjusted_values = [
        row.get("final_adjusted_tender_sum", 0) for row in tender_rows
    ]
    construction_budget = commercial.get("construction_budget", 0)
    construction_budget_values = [construction_budget for _ in tender_rows]
    variance_values = [row.get("variance_to_budget", 0) for row in tender_rows]

    tender_review_rows = [
        {"label": "Final Adjusted Tender Sum", "values": final_adjusted_values},
        {"label": "Construction Budget", "values": construction_budget_values},
        {"label": "Variance to Budget", "values": variance_values},
    ]

    return {
        "project_id": payload.get("project_id") or project.get("project_id") or "",
        "project_name": project.get("project_name") or "",
        "project_location": project.get("location") or "",
        "source_file_name": payload.get("source_file_name") or "",
        "project_description": project.get("project_description") or "",
        "responses_count": (report_context.get("tender_meta", {}) or {}).get("responses_count", ""),
        "executive_summary": _to_docx_text(executive.get("body") or ""),
        "recommendation": _to_docx_text(executive.get("recommendation") or ""),
        "next_steps": [_to_docx_text(step) for step in next_steps],
        "introduction": _to_docx_text(introduction_section.get("body") or ""),
        "tenderers": tenderers,
        "commercial_analysis": _to_docx_text(commercial_section.get("body") or ""),
        "tender_rows": tender_rows,
        # Compatibility aliases for templates that use `row` directly.
        "row": first_tender_row,
        "rows": tender_rows,
        "tender_review_contractors": contractor_names,
        "tender_review_rows": tender_review_rows,
        "r": {"label": "", "values": []},
        "name": "",
        "v": "",
        "step": "",
    }


def _build_pdf_html(payload: dict[str, Any]) -> str:
    context = _prepare_docx_context(payload)
    next_steps_html = "".join(f"<li>{step}</li>" for step in context["next_steps"])
    tenderers_html = "".join(f"<li>{name}</li>" for name in context["tenderers"])
    rows_html = "".join(
        (
            "<tr>"
            f"<td>{row.get('contractor', '')}</td>"
            f"<td>{row.get('initial_tender_sum', '')}</td>"
            f"<td>{row.get('fixed_adjustments', '')}</td>"
            f"<td>{row.get('risk_adjustments', '')}</td>"
            f"<td>{row.get('final_adjusted_tender_sum', '')}</td>"
            f"<td>{row.get('variance_to_budget', '')}</td>"
            "</tr>"
        )
        for row in context["tender_rows"]
    )
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <style>
    body {{ font-family: Arial, sans-serif; color: #1f2937; margin: 32px; }}
    h1, h2 {{ margin-bottom: 8px; }}
    p {{ line-height: 1.5; white-space: pre-wrap; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px; text-align: left; }}
    th {{ background: #f3f4f6; }}
  </style>
</head>
<body>
  <h1>Tender Comparison Report</h1>
  <p><strong>Project ID:</strong> {context["project_id"]}</p>
  <p><strong>Project Name:</strong> {context["project_name"]}</p>
  <p><strong>Location:</strong> {context["project_location"]}</p>
  <h2>01 - Executive Summary</h2>
  <p>{context["executive_summary"]}</p>
  <h2>Recommendation</h2>
  <p>{context["recommendation"]}</p>
  <h2>Recommended Next Steps</h2>
  <ol>{next_steps_html}</ol>
  <h2>02 - Introduction</h2>
  <p>{context["introduction"]}</p>
  <h2>Tenderer List</h2>
  <ul>{tenderers_html}</ul>
  <h2>04 - Commercial Analysis</h2>
  <p>{context["commercial_analysis"]}</p>
  <h2>Tender Review</h2>
  <table>
    <thead>
      <tr>
        <th>Contractor</th>
        <th>Initial Tender Sum</th>
        <th>Fixed Adjustments</th>
        <th>Risk Adjustments</th>
        <th>Final Adjusted Tender Sum</th>
        <th>Variance to Budget</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
</body>
</html>
"""


def export_report_docx(payload: dict[str, Any]) -> Path:
    if not TEMPLATE_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Word template not found at '{TEMPLATE_PATH}'. Add TCR_Template.docx first.",
        )
    context = _prepare_docx_context(payload)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"Tender_Comparison_{_slug(context['project_id'] or 'project')}_{stamp}.docx"
    output_path = EXPORTS_DIR / file_name

    doc = DocxTemplate(str(TEMPLATE_PATH))
    doc.render(context)
    doc.save(str(output_path))
    return output_path


def export_report_pdf(payload: dict[str, Any]) -> Path:
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    project_id = payload.get("project_id") or "project"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_name = f"Tender_Comparison_{_slug(str(project_id))}_{stamp}.pdf"
    output_path = EXPORTS_DIR / file_name
    html = _build_pdf_html(payload)
    HTML(string=html).write_pdf(str(output_path))
    return output_path

