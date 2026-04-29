from datetime import datetime, timezone
from html import unescape
from pathlib import Path
import re
from typing import Any

from docxtpl import DocxTemplate
from fastapi import HTTPException
from weasyprint import HTML

"""
the module is a report generator that takes the structured project and tender data(from your frontend or API) and produces a word and pdf report.
"""

REPORTING_DIR = Path(__file__).resolve().parent.parent / "reporting"
TEMPLATE_PATH = REPORTING_DIR / "templates" / "TCR_Template.docx"
EXPORTS_DIR = REPORTING_DIR / "exports"
ASSETS_DIR = REPORTING_DIR / "assets"
FONTS_DIR = ASSETS_DIR / "fonts"
LOGOS_DIR = ASSETS_DIR / "logos"
DEFAULT_LOGO_PATH = LOGOS_DIR / "company_logo.png"
DEFAULT_FONT_FAMILY = "Archivo"
DEFAULT_FONT_BODY_FILE = "Archivo_Expanded-Light.ttf"
DEFAULT_FONT_HEADING_FILE = "Archivo_Expanded-Bold.ttf"

# helper function to slugify the project id.
def _slug(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)
    return cleaned.strip("_") or "report"

# helper function to convert TinyMCE HTML to plain text safe for docxtpl placeholders.
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
    variance_values = [
        row.get("variance_to_construction_budget", row.get("variance_to_budget", 0))
        for row in tender_rows
    ]

    tender_review_rows = [
        {"label": "Final Adjusted Tender Sum", "values": final_adjusted_values},
        {"label": "Deduct Construction Budget", "values": construction_budget_values},
        {"label": "Variance to Construction Budget", "values": variance_values},
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
        "construction_budget": construction_budget,
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
    logo_html = (
        '<img class="brand-logo" src="assets/logos/company_logo.png" alt="Company logo" />'
        if DEFAULT_LOGO_PATH.exists()
        else ""
    )
    font_face_css = (
        f"""
    @font-face {{
      font-family: "{DEFAULT_FONT_FAMILY}";
      src: url("assets/fonts/{DEFAULT_FONT_BODY_FILE}") format("truetype");
      font-weight: 400;
      font-style: normal;
    }}
    @font-face {{
      font-family: "{DEFAULT_FONT_FAMILY}";
      src: url("assets/fonts/{DEFAULT_FONT_HEADING_FILE}") format("truetype");
      font-weight: 700;
      font-style: normal;
    }}
"""
        if (FONTS_DIR / DEFAULT_FONT_BODY_FILE).exists() and (FONTS_DIR / DEFAULT_FONT_HEADING_FILE).exists()
        else ""
    )
    next_steps_html = "".join(f"<li>{step}</li>" for step in context["next_steps"])
    project_info_rows_html = "".join(
        (
            "<tr>"
            f"<td>{field}</td>"
            f"<td>{value}</td>"
            "</tr>"
        )
        for field, value in [
            ("Project Name", context.get("project_name", "")),
            ("Project Description", context.get("project_description", "")),
            ("Number of Responses", context.get("responses_count", "")),
        ]
    )
    tenderers_html = "".join(f"<li>{name}</li>" for name in context["tenderers"])
    rows_html = "".join(
        (
            "<tr>"
            f"<td>{row.get('contractor', '')}</td>"
            f"<td>{row.get('final_adjusted_tender_sum', '')}</td>"
            f"<td>{row.get('construction_budget', context.get('construction_budget', ''))}</td>"
            f"<td>{row.get('variance_to_construction_budget', row.get('variance_to_budget', ''))}</td>"
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
    {font_face_css}
    @page {{
      size: A4;
      margin: 20mm 12mm 36mm 12mm;
    }}
    body {{
      font-family: "{DEFAULT_FONT_FAMILY}", Arial, sans-serif;
      background: #ffffff;
      color: #000000;
      font-size: 10px;
      margin: 0;
    }}
    .brand-logo {{ max-height: 52px; margin-bottom: 14px; }}
    h1 {{ margin-bottom: 8px; color: #425667; font-weight: 700; font-size: 18px; }}
    h2 {{ margin-bottom: 8px; color: #32c3e2; font-weight: 700; font-size: 12px; }}
    h3 {{ margin-bottom: 8px; color: #425667; font-weight: 700; font-size: 11px; }}
    p {{ line-height: 1.5; white-space: pre-wrap; }}
    ol {{ margin-top: 10px; margin-bottom: 14px; padding-left: 22px; }}
    ol li {{ margin-bottom: 8px; line-height: 1.6; }}
    ul {{ margin-top: 8px; margin-bottom: 20px; }}
    ul li {{ margin-bottom: 6px; line-height: 1.5; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px; text-align: left; color: #000000; }}
    th {{ background: #4f6577; color: #32c3e2; }}
    .tender-review-note {{ margin-top: 8px; font-style: italic; }}
    .pdf-footer {{
      position: fixed;
      left: 12mm;
      right: 12mm;
      bottom: -24mm;
      color: #334155;
      font-size: 8px;
    }}
    .pdf-footer-line {{
      border-top: 2px solid #8fd7e7;
      margin-bottom: 8px;
    }}
    .pdf-footer-row {{
      display: table;
      width: 100%;
      table-layout: fixed;
    }}
    .pdf-footer-col {{
      display: table-cell;
      vertical-align: middle;
    }}
    .pdf-footer-left {{
      text-transform: uppercase;
      letter-spacing: 0.3px;
      line-height: 1.3;
    }}
    .pdf-footer-center {{
      text-align: center;
      font-size: 10px;
      font-weight: 700;
      color: #475569;
    }}
    .pdf-footer-right {{
      text-align: right;
      white-space: nowrap;
    }}
    .footer-mark {{
      display: inline-block;
      width: 40px;
      height: 0;
      border-top: 10px solid #425667;
      border-right: 8px solid transparent;
      vertical-align: middle;
      margin-right: 8px;
    }}
    .footer-mark-accent {{
      color: #32c3e2;
      font-weight: 700;
      margin-right: 8px;
      vertical-align: middle;
    }}
    .footer-page::before {{
      content: "Page " counter(page) " of " counter(pages);
      font-size: 10px;
      color: #334155;
    }}
  </style>
</head>
<body>
  {logo_html}
  <h1>Tender Comparison Report</h1>
  <p><strong>Project ID:</strong> {context["project_id"]}</p>
  <p><strong>Project Name:</strong> {context["project_name"]}</p>
  <p><strong>Location:</strong> {context["project_location"]}</p>
  <h2>01 - Executive Summary</h2>
  <h3>Project Information</h3>
  <table>
    <thead>
      <tr>
        <th>Field</th>
        <th>Value</th>
      </tr>
    </thead>
    <tbody>
      {project_info_rows_html}
    </tbody>
  </table>
  <h3>Tender Review</h3>
  <table>
    <thead>
      <tr>
        <th>Contractor</th>
        <th>Final Adjusted Tender Sum</th>
        <th>Deduct Construction Budget</th>
        <th>Variance to Construction Budget</th>
      </tr>
    </thead>
    <tbody>
      {rows_html}
    </tbody>
  </table>
  <p class="tender-review-note">The above figures include any adjustments we have made in the submissions to ensure an equal and fair comparison is conducted. The figures also include any deductions made by the tendering contractors following the post tender discussions and negotiations. The detailed breakdown of all tender returns is located under the appendices.</p>
  <h3>Recommendation</h3>
  <p>{context["recommendation"]}</p>
  <h3>Recommended Next Steps</h3>
  <ol>{next_steps_html}</ol>

  <h2>02 - Introduction</h2>
  <h3>Report Overview</h3>
  <p>{context["introduction"]}</p>
  <h3>Tenderer List</h3>
  <ul>{tenderers_html}</ul>

  <footer class="pdf-footer">
    <div class="pdf-footer-line"></div>
    <div class="pdf-footer-row">
      <div class="pdf-footer-col pdf-footer-left">
        COSTPLAN SERVICES (SOUTH EAST) LTD<br/>
        CN 08842649
      </div>
      <div class="pdf-footer-col pdf-footer-center">cspsqs.com</div>
      <div class="pdf-footer-col pdf-footer-right">
        <span class="footer-mark"></span>
        <span class="footer-mark-accent">/</span>
        <span class="footer-page"></span>
      </div>
    </div>
  </footer>
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
    HTML(string=html, base_url=str(REPORTING_DIR)).write_pdf(str(output_path))
    return output_path

