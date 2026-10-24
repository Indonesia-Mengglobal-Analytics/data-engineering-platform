"""Sheets -> DuckDB -> sealed JSON for the GitHub Pages site.

Tasks, in order (shown on the site's Jobs page):
  discover   list sheets in the Drive folders plus the configured IDs
  validate   check each sheet against the format contract
  ingest     load registered sheets into DuckDB (raw.<table>)
  govern     classify columns, build curated.<table> views without withheld columns
  publish    aggregate, seal with the site PIN, write site/data/*.json

Usage:
  python pipeline/build.py                          # Google, needs GOOGLE_SERVICE_ACCOUNT_JSON
  python pipeline/build.py --fixtures tests/fixtures  # local demo data
Env: SITE_PIN (12 digits, required), PREVIOUS_DIR (last published site/data, optional),
     ONLY_SOURCE (refresh one spreadsheet ID, keep the rest from PREVIOUS_DIR).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).parent))

from contract import check  # noqa: E402
from governance import COUNTED, WITHHELD, classify, suppress  # noqa: E402
from seal import Sealer, unseal  # noqa: E402
from sources import GoogleSource, SourceError, from_fixtures  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUTPUTS = ("catalog", "jobs", "governance", "dashboard")
MAX_RUNS = 50
# Google Forms writes the timestamp in the sheet's locale, so the format is chosen per column, not per value:
# "01/07/2026" is 1 July in an Indonesian sheet and 7 January in a US one.
TIMESTAMP_FORMATS = ["%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y", "%m/%d/%Y"]


def now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def slug(text: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", text.lower())).strip("_") or "sheet"


class Tasks:
    """Timing and status per pipeline task, for the run record."""

    def __init__(self):
        self.items: list[dict] = []

    def run(self, key: str, fn, *args):
        started = time.monotonic()
        item = {"key": key, "status": "running"}
        self.items.append(item)
        try:
            result = fn(*args)
            item["status"] = "succeeded"
            return result
        except Exception as error:
            item["status"] = "failed"
            item["detail"] = f"{type(error).__name__}: {error}"
            raise
        finally:
            item["duration_ms"] = int((time.monotonic() - started) * 1000)


def load_previous(folder: str | None, pin: str) -> dict:
    previous: dict = {}
    if not folder:
        return previous
    for name in OUTPUTS:
        path = Path(folder) / f"{name}.json"
        if not path.exists():
            continue
        try:
            previous[name] = unseal(json.loads(path.read_text()), pin)
        except Exception:
            # PIN changed or file damaged: start that output fresh.
            print(f"warning: could not open previous {name}.json, starting fresh", file=sys.stderr)
    return previous


def discover(config: dict, fixtures: str | None, only: str | None):
    if fixtures:
        sheets = from_fixtures(Path(fixtures))
        return [s for s in sheets if not only or getattr(s, "spreadsheet_id", None) == only], "fixtures"
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
    google = GoogleSource(json.loads(raw))
    ids = [only] if only else google.discover(config["sources"]["drive_folder_ids"], config["sources"]["spreadsheet_ids"])
    return [google.read(i) for i in ids], google.account


def validate(sheets, contract: dict):
    checked = []
    for sheet in sheets:
        if isinstance(sheet, SourceError):
            checked.append((sheet, None))
        else:
            checked.append((sheet, check(sheet, contract)))
    return checked


def ingest(con, checked, contract: dict) -> dict[str, str]:
    """Load each registered sheet into raw.<table>. Returns spreadsheet_id -> table name."""
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    tables, used = {}, set()
    for sheet, result in checked:
        if result is None or not result.registered:
            continue
        table = slug(sheet.title)
        while table in used:
            table += "_2"
        used.add(table)
        names = [c["column_name"] for c in result.columns]
        rows = sheet.tabs[contract["data_tab"]][1:]
        rows = [[(r[i] if i < len(r) else None) for i in range(len(names))] for r in rows if any(str(v).strip() for v in r)]
        cols = ", ".join(f'"{n}" VARCHAR' for n in names)
        con.execute(f'CREATE OR REPLACE TABLE raw."{table}" ({cols})')
        if rows:
            placeholders = ", ".join("?" for _ in names)
            con.executemany(f'INSERT INTO raw."{table}" VALUES ({placeholders})', rows)
        tables[sheet.spreadsheet_id] = table
    return tables


def govern(con, checked, tables: dict, rules: list[dict]) -> dict[str, dict[str, str]]:
    """Classify columns; curated.<table> holds only what may be aggregated for publishing."""
    con.execute("CREATE SCHEMA IF NOT EXISTS curated")
    classes = {}
    for sheet, result in checked:
        if result is None or not result.registered:
            continue
        table = tables[sheet.spreadsheet_id]
        classes[sheet.spreadsheet_id] = {c["column_name"]: classify(c["column_name"], rules) for c in result.columns}
        kept = [n for n, k in classes[sheet.spreadsheet_id].items() if k not in WITHHELD]
        select = ", ".join(f'"{n}"' for n in kept) or "1 AS empty"
        con.execute(f'CREATE OR REPLACE VIEW curated."{table}" AS SELECT {select} FROM raw."{table}"')
    return classes


def _filled(column: str) -> str:
    return f"""count(*) FILTER (WHERE nullif(trim("{column}"), '') IS NOT NULL)"""


def profile(con, table: str, columns: list[str], unread: frozenset[str]) -> tuple[int, dict[str, float | None]]:
    total = con.execute(f'SELECT count(*) FROM raw."{table}"').fetchone()[0]
    read = [c for c in columns if c not in unread]
    completeness: dict[str, float | None] = {c: None for c in columns}
    if read:
        exprs = ", ".join(_filled(c) for c in read)
        filled = con.execute(f'SELECT {exprs} FROM raw."{table}"').fetchone()
        completeness.update({c: (round(f / total, 4) if total else 0.0) for c, f in zip(read, filled)})
    return total, completeness


def timestamp_format(con, table: str) -> str | None:
    """The format that parses the most timestamps in the column (first listed wins a tie)."""
    best, best_count = None, 0
    for fmt in TIMESTAMP_FORMATS:
        count = con.execute(
            f"""SELECT count(try_strptime(trim("timestamp"), '{fmt}')) FROM raw."{table}" """
        ).fetchone()[0]
        if count > best_count:
            best, best_count = fmt, count
    return best


def dashboard_for(con, table: str, classes: dict[str, str], gov: dict, total: int, completeness: dict,
                  unread: frozenset[str] = frozenset()) -> dict:
    min_group, max_categories = gov["min_group_size"], gov["max_categories"]
    multi = set(gov.get("multi_select", []))
    out: dict = {"kpis": {"records": total}, "charts": [], "timeline": [], "quality": []}

    fmt = None
    if "timestamp" in classes and "timestamp" not in unread:
        fmt = timestamp_format(con, table)
        if fmt:
            rows = con.execute(
                f"""SELECT CAST(try_strptime(trim("timestamp"), '{fmt}') AS DATE) AS day, count(*)
                    FROM raw."{table}" GROUP BY 1 ORDER BY 1"""
            ).fetchall()
            out["timeline"] = [{"date": str(d) if d else None, "count": n} for d, n in rows]
            unparsed = sum(n for d, n in rows if d is None)
        else:
            unparsed = total
        if unparsed:
            out["quality"].append({"check": "Unreadable timestamps", "value": unparsed, "status": "warn"})

    for column, klass in classes.items():
        if klass not in COUNTED or column == "timestamp" or column in unread:
            continue
        if column in multi:
            # One count per option ticked; a person can appear under several options.
            counts = con.execute(
                f"""SELECT option, count(*) FROM (
                      SELECT trim(unnest(string_split("{column}", ', '))) AS option
                      FROM curated."{table}" WHERE nullif(trim("{column}"), '') IS NOT NULL)
                    WHERE option <> '' GROUP BY 1"""
            ).fetchall()
        else:
            counts = con.execute(
                f"""SELECT coalesce(nullif(trim("{column}"), ''), '(blank)'), count(*)
                    FROM curated."{table}" GROUP BY 1"""
            ).fetchall()
        out["charts"].append({"column": column, "class": klass, "multi": column in multi, "distinct": len(counts),
                              "values": suppress(counts, min_group, max_categories)})

    if "email_address" in classes and "email_address" not in unread and total:
        dupes = con.execute(
            f"""SELECT coalesce(sum(n - 1), 0) FROM (
                  SELECT count(*) AS n FROM raw."{table}"
                  WHERE nullif(trim(email_address), '') IS NOT NULL
                  GROUP BY lower(trim(email_address)) HAVING count(*) > 1)"""
        ).fetchone()[0]
        out["kpis"]["duplicate_submissions"] = int(dupes)
        out["quality"].append(
            {"check": "Duplicate submissions (same email)", "value": int(dupes), "status": "warn" if dupes else "ok"}
        )

    consent = [c for c in classes if "consent" in c and completeness.get(c) is not None]
    for column in consent:
        out["quality"].append(
            {"check": f"Consent answered: {column}", "value": f"{completeness[column]:.0%}",
             "status": "ok" if completeness[column] >= 0.999 else "fail"}
        )
    measured = [v for v in completeness.values() if v is not None]
    if measured:
        out["kpis"]["avg_completeness"] = round(sum(measured) / len(measured), 4)
    out["kpis"]["columns"] = len(classes)
    out["kpis"]["columns_measured"] = len(measured)
    cube = filter_cube(con, table, classes, gov, unread, fmt, [c["column"] for c in out["charts"]])
    if cube:
        out["cube"] = cube
    return out


def filter_cube(con, table: str, classes: dict[str, str], gov: dict, unread: frozenset[str],
                fmt: str | None, charted: list[str]) -> dict | None:
    """Grouped counts that let the dashboard filter without publishing rows.

    Only the configured filter columns are used as dimensions, each value held by fewer
    than min_group_size people is generalised to "Other", and withheld or multi-select
    columns are never dimensions. The browser applies the same small-group rule again to
    every number it shows, and hides charts when a filter leaves fewer than that many people.
    """
    k = gov["min_group_size"]
    multi = set(gov.get("multi_select", []))
    dims = [c for c in gov.get("dashboard_filters", [])
            if classes.get(c) in COUNTED and c not in unread and c not in multi]
    if not dims:
        return None

    # Generalised dimension expressions: rare values become "Other", blanks "(blank)".
    exprs, values = [], []
    for column in dims:
        rows = con.execute(
            f"""SELECT coalesce(nullif(trim("{column}"), ''), '(blank)') AS v, count(*) FROM curated."{table}" GROUP BY 1"""
        ).fetchall()
        keep = sorted((v for v, n in rows if n >= k), key=lambda v: -dict(rows)[v])
        has_other = any(n < k for _, n in rows)
        values.append(keep + (["Other"] if has_other else []))
        kept = ", ".join("'" + v.replace("'", "''") + "'" for v in keep) or "NULL"
        exprs.append(f"""CASE WHEN coalesce(nullif(trim("{column}"), ''), '(blank)') IN ({kept})
                        THEN coalesce(nullif(trim("{column}"), ''), '(blank)') ELSE 'Other' END""")
    index = [{v: i for i, v in enumerate(vals)} for vals in values]
    dim_sql = ", ".join(f"{e} AS d{i}" for i, e in enumerate(exprs))
    group = ", ".join(f"d{i}" for i in range(len(dims)))

    day = (f"""CAST(try_strptime(trim("timestamp"), '{fmt}') AS DATE)""" if fmt else "NULL")
    base_rows = con.execute(
        f"""SELECT {group}, day, count(*) FROM (
              SELECT {dim_sql}, {day} AS day FROM raw."{table}") GROUP BY ALL ORDER BY ALL"""
    ).fetchall()
    days = sorted({str(r[-2]) for r in base_rows if r[-2] is not None})
    day_index = {d: i for i, d in enumerate(days)}
    base = [[index[i][r[i]] for i in range(len(dims))] + [day_index.get(str(r[-2]), -1), r[-1]] for r in base_rows]

    charts = {}
    for column in charted:
        if column in multi:
            value = f"""trim(unnest(string_split("{column}", ', ')))"""
            where = f"""WHERE nullif(trim("{column}"), '') IS NOT NULL"""
        else:
            value = f"""coalesce(nullif(trim("{column}"), ''), '(blank)')"""
            where = ""
        rows = con.execute(
            f"""SELECT {group}, day, v, count(*) FROM (
                  SELECT {dim_sql}, {day} AS day, {value} AS v FROM raw."{table}" {where}) WHERE v <> '' GROUP BY ALL ORDER BY ALL"""
        ).fetchall()
        # Answers held by fewer than k people overall are grouped too, so no rare value leaves the pipeline.
        totals: dict[str, int] = {}
        for r in rows:
            totals[r[-2]] = totals.get(r[-2], 0) + r[-1]
        kept = {v for v, n in totals.items() if n >= k}
        label = lambda v: v if v in kept else "Other"  # noqa: E731
        labels = sorted(kept, key=lambda v: -totals[v]) + (["Other"] if len(kept) < len(totals) else [])
        label_index = {v: i for i, v in enumerate(labels)}
        merged: dict[tuple, int] = {}
        for r in rows:
            key = (tuple(index[i][r[i]] for i in range(len(dims)))
                   + (day_index.get(str(r[-3]), -1), label_index[label(r[-2])]))
            merged[key] = merged.get(key, 0) + r[-1]
        charts[column] = {"values": labels, "rows": [list(key) + [n] for key, n in sorted(merged.items())]}
    # Row layout: base = [dim indexes..., day index, count]; charts = [dim indexes..., day index, value index, count].
    return {"min_group_size": k, "max_categories": gov["max_categories"],
            "dims": [{"column": c, "values": v} for c, v in zip(dims, values)],
            "days": days, "base": base, "charts": charts}


def publish(con, checked, tables, classes, config, previous, only, run) -> dict:
    gov = config["governance"]
    old_catalog = {s["id"]: s for s in previous.get("catalog", {}).get("sources", [])}
    old_dash = previous.get("dashboard", {}).get("sources", {})
    catalog, dashboard = [], {}

    for sheet, result in checked:
        if isinstance(sheet, SourceError):
            sid = str(sheet).split(":")[0]
            catalog.append({"id": sid, "title": old_catalog.get(sid, {}).get("title", sid), "status": "unreachable",
                            "errors": [str(sheet)], "warnings": [], "columns": [], "checked_at": run["started_at"]})
            continue
        entry = {
            "id": sheet.spreadsheet_id,
            "title": sheet.title,
            "url": f"https://docs.google.com/spreadsheets/d/{sheet.spreadsheet_id}/edit",
            "status": "registered" if result.registered else "rejected",
            "errors": result.errors,
            "warnings": result.warnings,
            "table": result.table,
            "modified_time": sheet.modified_time,
            "checked_at": run["started_at"],
            "columns": [],
        }
        if result.registered:
            table = tables[sheet.spreadsheet_id]
            names = [c["column_name"] for c in result.columns]
            total, completeness = profile(con, table, names, sheet.unread)
            entry.update({"fqn": f"sheets.raw.{table}", "curated_fqn": f"sheets.curated.{table}", "rows": total,
                          "ingested_at": run["started_at"], "partial_load": bool(sheet.unread)})
            for c in result.columns:
                klass = classes[sheet.spreadsheet_id][c["column_name"]]
                entry["columns"].append({
                    "name": c["column_name"], "description": c.get("column_description", ""),
                    "form_question": c.get("form_question", ""), "location": c.get("column_header_location", ""),
                    "class": klass, "published": klass not in WITHHELD,
                    "completeness": completeness[c["column_name"]],
                })
            dashboard[sheet.spreadsheet_id] = {
                "title": sheet.title,
                **dashboard_for(con, table, classes[sheet.spreadsheet_id], gov, total, completeness, sheet.unread)
            }
        catalog.append(entry)

    if only:
        # One sheet refreshed: keep every other source as last published.
        fresh = {s["id"] for s in catalog}
        catalog += [s for sid, s in old_catalog.items() if sid not in fresh]
        dashboard = {**{k: v for k, v in old_dash.items() if k not in fresh}, **dashboard}

    catalog.sort(key=lambda s: ({"registered": 0, "rejected": 1, "unreachable": 2}[s["status"]], s["title"].lower()))
    governance = {
        "min_group_size": gov["min_group_size"],
        "max_categories": gov["max_categories"],
        "rules": gov["rules"],
        "withheld_classes": sorted(WITHHELD),
        "counted_classes": sorted(COUNTED),
        "sources": [
            {"id": s["id"], "title": s["title"], "owner": s.get("table", {}).get("owner_email", ""),
             "purpose": s.get("table", {}).get("purpose", ""),
             "classes": {k: sum(1 for c in s["columns"] if c["class"] == k) for k in
                         ("personal", "sensitive", "free_text", "categorical", "metadata")},
             "withheld": [c["name"] for c in s["columns"] if not c["published"]],
             "consent": [{"column": c["name"], "completeness": c["completeness"]} for c in s["columns"] if "consent" in c["name"]]}
            for s in catalog if s["status"] == "registered"
        ],
    }
    return {
        "catalog": {"generated_at": run["started_at"], "sources": catalog,
                    "contract": config["contract"]},
        "dashboard": {"generated_at": run["started_at"], "sources": dashboard},
        "governance": {"generated_at": run["started_at"], **governance},
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures", help="read sheets from local JSON fixtures instead of Google")
    parser.add_argument("--out", default=str(ROOT / "site" / "data"))
    parser.add_argument("--warehouse", default=str(ROOT / "build" / "warehouse.duckdb"))
    args = parser.parse_args()

    config = yaml.safe_load((ROOT / "config" / "platform.yml").read_text())
    pin = os.environ.get("SITE_PIN", "").strip()
    sealer = Sealer(pin)
    only = os.environ.get("ONLY_SOURCE", "").strip() or None
    previous = load_previous(os.environ.get("PREVIOUS_DIR"), pin)

    run = {
        "run_id": os.environ.get("GITHUB_RUN_ID", f"local-{int(time.time())}"),
        "run_number": os.environ.get("GITHUB_RUN_NUMBER"),
        "trigger": os.environ.get("GITHUB_EVENT_NAME", "local"),
        "actor": os.environ.get("GITHUB_ACTOR", os.environ.get("USER", "local")),
        "only_source": only,
        "started_at": now(),
    }
    if os.environ.get("GITHUB_RUN_ID"):
        server, repo = os.environ.get("GITHUB_SERVER_URL", "https://github.com"), os.environ.get("GITHUB_REPOSITORY", "")
        run["url"] = f"{server}/{repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"

    Path(args.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.warehouse)
    tasks = Tasks()
    started = time.monotonic()
    outputs: dict = {}
    failed = None
    try:
        sheets, reader = tasks.run("discover", discover, config, args.fixtures, only)
        run["reader"] = reader
        checked = tasks.run("validate", validate, sheets, config["contract"])
        tables = tasks.run("ingest", ingest, con, checked, config["contract"])
        classes = tasks.run("govern", govern, con, checked, tables, config["governance"]["rules"])
        outputs = tasks.run("publish", publish, con, checked, tables, classes, config, previous, only, run)
        statuses = [s["status"] for s in outputs["catalog"]["sources"] if not only or s["id"] == only]
        run["sources"] = {k: statuses.count(k) for k in ("registered", "rejected", "unreachable")}
        run["status"] = "succeeded" if statuses and all(s == "registered" for s in statuses) else "partial"
    except Exception as error:
        failed = error
        run["status"] = "failed"
        run["error"] = f"{type(error).__name__}: {error}"
    finally:
        con.close()

    run["finished_at"] = now()
    run["duration_s"] = round(time.monotonic() - started, 1)
    run["tasks"] = tasks.items
    done = {t["key"] for t in tasks.items}
    run["tasks"] += [{"key": k, "status": "skipped"} for k in ("discover", "validate", "ingest", "govern", "publish") if k not in done]

    runs = [run] + previous.get("jobs", {}).get("runs", [])
    jobs = {"generated_at": run["finished_at"], "runs": runs[:MAX_RUNS],
            "workflow": {"repository": config["github"]["repository"], "file": config["github"]["workflow_file"],
                         "schedule": os.environ.get("PIPELINE_SCHEDULE", "")}}

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if failed:
        # Keep the last good data; only the run history changes.
        outputs = {k: previous[k] for k in ("catalog", "dashboard", "governance") if k in previous}
    for name, payload in {**outputs, "jobs": jobs}.items():
        (out / f"{name}.json").write_text(json.dumps(sealer.seal(payload)))
    print(json.dumps({k: run.get(k) for k in ("status", "sources", "duration_s", "error")}))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
