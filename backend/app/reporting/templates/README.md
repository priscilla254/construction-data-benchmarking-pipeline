Place your Word template file here as:

`TCR_Template.docx`

Use docxtpl placeholders in the template, for example:

- `{{ project_id }}`
- `{{ project_name }}`
- `{{ executive_summary }}`
- `{{ recommendation }}`
- `{{ commercial_analysis }}`
- `{{ project_description }}`
- `{{ responses_count }}`
- `{{ tenders_issued_date }}`
- `{{ tender_deadline_date }}`
- `{{ addendums_issued_count }}`

Loop examples:

```jinja2
{% for step in next_steps %}
- {{ step }}
{% endfor %}
```

```jinja2
{% for row in tender_rows %}
{{ row.contractor }} | {{ row.final_adjusted_tender_sum }}
{% endfor %}
```

Tender review matrix (contractors as columns, three fixed rows):

```jinja2
Header row:
Item | {% for name in tender_review_contractors %}{{ name }} | {% endfor %}

Body rows:
{% for r in tender_review_rows %}
{{ r.label }} | {% for v in r.values %}{{ v }} | {% endfor %}
{% endfor %}
```
