"""Knowledge base for the site's Ask page: short passages built only from what the site already publishes.

Every passage is derived from the catalog, governance, dashboard and job outputs, so nothing here is
more detailed than the pages themselves: no rows, no withheld column values, and chart counts already
passed through the small-group rule.
"""

from __future__ import annotations

from datetime import date


def _label(column: str) -> str:
    return column.replace("_", " ").replace("phd", "PhD").capitalize()


def _fmt_date(value: str) -> str:
    if value and len(value) == 8 and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value or "unknown"


def _values(values: list[dict]) -> str:
    parts = []
    for v in values:
        parts.append(f"{v['value']}: {'fewer than 5' if v['count'] is None else v['count']}")
    return "; ".join(parts)


def build(catalog: dict, dashboard: dict, governance: dict, jobs: dict, contract: dict) -> dict:
    passages: list[dict] = []

    def add(kind: str, title: str, text: str, link: str, source: str | None = None) -> None:
        passages.append({"id": f"P{len(passages) + 1}", "kind": kind, "title": title, "text": text,
                         "link": link, "source": source})

    add("platform", "How the IM Data Platform works",
        "Google Sheets in the live Drive folder (plus sheets listed in config/platform.yml) are read by a "
        "GitHub Actions pipeline every 6 hours, on a manual run, and on every push to main. The pipeline loads "
        "registered sheets into DuckDB (schema raw), builds governed views without personal columns (schema "
        "curated), and publishes only aggregated, small-group-suppressed figures to this GitHub Pages site, "
        "encrypted with the site PIN. There is no server; row-level data never leaves the pipeline run.",
        "#/")
    add("platform", "Source format (data contract)",
        f"A sheet is registered only when it has three tabs: {contract['data_tab']} (header row of form "
        f"questions, then one row per record), {contract['column_metadata_tab']} (columns "
        f"{', '.join(contract['column_metadata_headers'])}; one row per DATA column, form_question must equal "
        f"the DATA header at column_header_location, column_name in snake_case) and "
        f"{contract['table_metadata_tab']} (columns {', '.join(contract['table_metadata_headers'])}; needs a row "
        "with table_name = data, an owner email, and dates as YYYYMMDD). Sheets that fail are shown as Rejected "
        "with the reasons; sheets the service account cannot open are Unreachable.",
        "#/ingestion")

    for s in catalog.get("sources", []):
        sid, title = s["id"], s["title"]
        if s["status"] != "registered":
            add("source", f"{title}: {s['status']}",
                f"The source '{title}' (spreadsheet {sid}) is {s['status']}. Reasons: "
                + ("; ".join(s.get("errors", [])) or "none recorded") + ".", "#/ingestion", sid)
            continue
        t = s.get("table", {})
        table = s["fqn"].split(".")[-1]
        add("source", f"{title}: overview",
            f"Source '{title}' is registered as table {s['fqn']} (governed view {s['curated_fqn']}). "
            f"It has {s['rows']} records and {len(s['columns'])} columns. Owner: {t.get('owner_email', 'unknown')}. "
            f"Description: {t.get('table_description', '')} Purpose: {t.get('purpose', '')} "
            f"Operation period: {_fmt_date(t.get('operation_date_start', ''))} to {_fmt_date(t.get('operation_date_end', ''))}; "
            f"populated {_fmt_date(t.get('populated_date', ''))}; version {t.get('version', '')}. "
            f"Last ingested {s.get('ingested_at', 'unknown')}.", f"#/catalog/{table}/details/raw", sid)
        cols = s["columns"]
        # The dictionary in groups of 8 columns keeps passages short enough to retrieve precisely.
        for i in range(0, len(cols), 8):
            chunk = cols[i:i + 8]
            lines = []
            for c in chunk:
                filled = "unknown" if c["completeness"] is None else f"{round(c['completeness'] * 100)}% filled"
                lines.append(f"{c['name']} ({c['location']}, tag {c['class']}, {filled}, "
                             f"{'published as aggregates' if c['published'] else 'withheld'}): {c['description']} "
                             f"Form question: \"{c['form_question'][:160]}\"")
            add("column", f"{title}: columns {i + 1}-{i + len(chunk)}", " | ".join(lines), f"#/catalog/{table}", sid)

    for g in governance.get("sources", []):
        add("governance", f"{g['title']}: governance",
            f"Column tags for '{g['title']}': " + ", ".join(f"{k} {v}" for k, v in g["classes"].items())
            + f". Withheld columns (never published, not even as counts): {', '.join(g['withheld']) or 'none'}. "
            + "Consent coverage: " + ("; ".join(
                f"{c['column']} answered by {'unknown' if c['completeness'] is None else str(round(c['completeness'] * 100)) + '%'}"
                for c in g["consent"]) or "no consent column") + ".", "#/governance", g["id"])
    add("governance", "Privacy and publication policy",
        f"Rows never leave the pipeline run. Columns tagged personal or sensitive are dropped before anything is "
        f"counted; free-text columns only show completeness; categorical and metadata columns are published as "
        f"counts. Any group with fewer than {governance.get('min_group_size', 5)} people is folded into 'Other', "
        "and hidden as 'Suppressed' when that remainder is itself too small. Dashboard filters only use coarse "
        "single-answer columns, with rare values grouped as 'Other'. Published files are encrypted with "
        "AES-256-GCM using a key derived from the 12-digit site PIN (PBKDF2-SHA256).", "#/governance")

    for sid, d in dashboard.get("sources", {}).items():
        title = d["title"]
        k = d["kpis"]
        quality = "; ".join(f"{q['check']}: {q['value']} ({q['status']})" for q in d.get("quality", []))
        add("quality", f"{title}: data quality",
            f"'{title}' has {k['records']} records. Duplicate submissions (same email): "
            f"{k.get('duplicate_submissions', 'not measured')}. Average completeness: "
            f"{'unknown' if k.get('avg_completeness') is None else str(round(k['avg_completeness'] * 100)) + '%'} "
            f"across {k.get('columns_measured', k.get('columns'))} measured columns. Checks: {quality or 'none'}.",
            f"#/dashboards/{sid}", sid)
        days = [p for p in d.get("timeline", []) if p.get("date")]
        if days:
            peak = max(days, key=lambda p: p["count"])
            months: dict[str, int] = {}
            for p in days:
                months[p["date"][:7]] = months.get(p["date"][:7], 0) + p["count"]
            add("timeline", f"{title}: submissions over time",
                f"Submissions to '{title}' ran from {days[0]['date']} to {days[-1]['date']} across {len(days)} days "
                f"with submissions. The busiest day was {peak['date']} with {peak['count']} submissions. By month: "
                + "; ".join(f"{m}: {n}" for m, n in sorted(months.items())) + ".", f"#/dashboards/{sid}", sid)
        for c in d.get("charts", []):
            add("chart", f"{title}: {_label(c['column'])}",
                f"Answers to '{_label(c['column'])}' in '{title}'"
                + (" (people could pick several options, so counts add up to more than the number of applicants)"
                   if c.get("multi") else "")
                + f": {_values(c['values'])}.", f"#/dashboards/{sid}", sid)

    lh = catalog.get("lakehouse") or {}
    add("platform", "Lakehouse layers: bronze, silver, gold and SCD",
        "Every registered sheet flows through three layers in DuckDB, saved as Parquet in the Shared Drive "
        f"({'configured' if lh.get('storage') == 'drive' else 'not configured yet, so history does not survive between runs'}). "
        f"Bronze keeps every run's snapshot exactly as read for {lh.get('bronze_retention_days', 30)} days. Silver cleans the "
        "values (trims spaces, lower-cases emails, parses the timestamp), runs the quality rules, and keeps one version per "
        f"record and change as SCD type 2 (record_key from {', '.join(lh.get('record_key') or ['row hash'])}; valid_from, "
        "valid_to, is_current, is_deleted). The latest submission of a record wins. Gold holds current records that passed every "
        "error-level rule and feeds the dashboard; gold.consolidated stacks every source; gold.dq_issues lists rule failures "
        "without the values.", "#/catalog")
    add("platform", "Previewing rows and running SQL",
        "Rows are never shown on this site. Full previews are Google Sheets in the Shared Drive: one per source (tabs CURRENT, "
        "HISTORY, DQ_ISSUES, ABOUT) plus CONSOLIDATED (ALL_DATA, DQ_ISSUES, SUMMARY), rewritten on every run. The SQL Editor "
        "runs one read-only DuckDB SELECT over bronze, silver and gold in GitHub Actions and writes the result to a sheet in "
        f"previews/, deleted after {lh.get('preview_retention_hours', 2)} hours. Only people with write access to the "
        "repository can run queries.", "#/sql")
    for s in catalog.get("sources", []):
        if s.get("status") != "registered" or not s.get("layers"):
            continue
        sv, b = s["layers"].get("silver") or {}, s["layers"].get("bronze") or {}
        dq = sv.get("dq", {})
        add("quality", f"{s['title']}: layers and quality rules",
            f"Latest run for '{s['title']}': bronze appended {b.get('appended')} rows ({b.get('total_rows')} kept across "
            f"{b.get('snapshots')} snapshots). Silver: {sv.get('inserted')} inserted, {sv.get('updated')} updated, "
            f"{sv.get('deleted')} deleted, {sv.get('unchanged')} unchanged; {sv.get('duplicates_superseded')} resubmissions "
            f"merged; {sv.get('current')} current records; quality {dq.get('pass')} pass, {dq.get('warn')} warn, "
            f"{dq.get('fail')} fail. Gold: {s['layers'].get('gold')} records. Rules: "
            + "; ".join(f"{r['id']} ({r['severity']}, {r['origin']}): {r['failed']} of {r['checked']} failed"
                        for r in s.get("quality_rules", [])) + ".", "#/quality", s["id"])

    runs = jobs.get("runs", [])
    if runs:
        last = runs[0]
        ok = sum(1 for r in runs if r.get("status") == "succeeded")
        add("pipeline", "Pipeline runs",
            f"The job sheets_to_duckdb has {len(runs)} recorded runs ({ok} succeeded). The latest run "
            f"{last.get('run_number') or last.get('run_id')} started {last.get('started_at')} ({last.get('trigger')}) "
            f"and ended {last.get('status')}"
            + (f" with sources {last['sources']}" if last.get("sources") else "")
            + (f"; error: {last['error']}" if last.get("error") else "")
            + ". To refresh now, open Jobs & Pipelines and use Run now (it opens the GitHub Actions Run workflow page).",
            "#/runs")

    return {"generated_at": catalog.get("generated_at") or date.today().isoformat(), "passages": passages}
