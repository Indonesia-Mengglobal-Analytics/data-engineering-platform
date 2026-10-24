"""Run one read-only SQL query on the lakehouse and write the result to a short-lived preview sheet.

    python pipeline/query.py --sql "SELECT * FROM silver.mentee_data WHERE is_current" --label "current mentees"
    python pipeline/query.py --cleanup-only

Tables: bronze.<t>, silver.<t>, gold.<t>, gold.consolidated, gold.dq_issues (DuckDB SQL).
Only one SELECT statement is allowed; file and network access are switched off before it runs.
Previews land in the Shared Drive's previews/ folder and are deleted after preview_retention_hours.
Started from GitHub Actions (query.yml), so only people with write access to the repository can run it.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).parent))

import lakehouse  # noqa: E402
from exports import GoogleSheets, LocalSheets, table  # noqa: E402
from sources import GoogleSource  # noqa: E402
from storage import DriveStorage, LocalStorage  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
TIMEOUT_S = 120


class QueryError(Exception):
    pass


def check_sql(con, sql: str) -> str:
    sql = sql.strip().rstrip(";").strip()
    if not sql:
        raise QueryError("The query is empty.")
    try:
        statements = con.extract_statements(sql)
    except duckdb.Error as error:
        raise QueryError(f"SQL error: {error}") from None
    if len(statements) != 1:
        raise QueryError("Run one statement at a time.")
    if statements[0].type != duckdb.StatementType.SELECT:
        raise QueryError("Only SELECT queries are allowed (WITH … SELECT is fine).")
    return sql


def open_lakehouse(con, storage) -> list[str]:
    lakehouse.setup(con)
    manifest = json.loads((storage.read("lakehouse/_manifest.json") or b"{}").decode() or "{}")
    loaded = []
    for t in manifest.get("tables", {}):
        for layer in ("bronze", "silver", "gold"):
            if lakehouse.load(con, storage, layer, t):
                loaded.append(f"{layer}.{t}")
    for name in ("consolidated", "dq_issues"):
        if lakehouse.load(con, storage, "gold", name):
            loaded.append(f"gold.{name}")
    if not loaded:
        raise QueryError("The lakehouse is empty: run the pipeline first.")
    return loaded


def run_query(con, sql: str, max_rows: int) -> tuple[list[str], list[list], bool]:
    con.execute("SET enable_external_access = false")
    con.execute("SET lock_configuration = true")
    timer = threading.Timer(TIMEOUT_S, con.interrupt)
    timer.start()
    try:
        header, rows = table(con, f"SELECT * FROM ({sql}) LIMIT {int(max_rows) + 1}")
    except duckdb.InterruptException:
        raise QueryError(f"The query took longer than {TIMEOUT_S} seconds.") from None
    except duckdb.Error as error:
        raise QueryError(f"SQL error: {error}") from None
    finally:
        timer.cancel()
    return header, rows[:max_rows], len(rows) > max_rows


def summary(text: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text + "\n")
    print(text)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", default=os.environ.get("QUERY_SQL", ""))
    parser.add_argument("--label", default=os.environ.get("QUERY_LABEL", ""))
    parser.add_argument("--lakehouse", help="local lakehouse folder (tests and runs without a Shared Drive)")
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args()
    config = yaml.safe_load((ROOT / "config" / "platform.yml").read_text())
    retention = float(config["outputs"]["preview_retention_hours"])

    folder = config.get("storage", {}).get("shared_drive_folder_id", "")
    raw_key = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if raw_key and folder and not args.lakehouse:
        google = GoogleSource(json.loads(raw_key))
        storage = DriveStorage(google.drive, folder)
        writer = GoogleSheets(google.drive, google.sheets, storage)
    elif args.lakehouse:
        storage = LocalStorage(Path(args.lakehouse))
        writer = LocalSheets(Path(args.lakehouse) / "sheets")
    else:
        summary("No Shared Drive configured (storage.shared_drive_folder_id) and no --lakehouse: nothing to query.")
        return 1

    removed = writer.cleanup("previews", retention)
    if removed:
        summary(f"Deleted {len(removed)} preview(s) older than {retention:g} h: " + ", ".join(removed))
    if args.cleanup_only:
        return 0

    con = duckdb.connect()
    try:
        loaded = open_lakehouse(con, storage)
        sql = check_sql(con, args.sql)
        header, rows, truncated = run_query(con, sql, int(config["outputs"]["preview_max_rows"]))
    except QueryError as error:
        summary(f"### Query failed\n\n{error}")
        return 1
    created = datetime.now(timezone.utc)
    expires = created + timedelta(hours=retention)
    label = re.sub(r"[^\w .-]", "", args.label)[:60] or "query"
    actor = os.environ.get("GITHUB_ACTOR", os.environ.get("USER", "local"))
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    name = f"Preview {created:%Y-%m-%d %H%M} UTC · {label} · {actor} · expires {expires:%H:%M} UTC"
    url = writer.create("previews", name, {
        "RESULT": (header, rows),
        "QUERY": (["field", "value"], [["sql", sql], ["label", label], ["requested_by", actor], ["run_id", run_id],
                                       ["created_at", created.isoformat()], ["expires_at", expires.isoformat()],
                                       ["rows", len(rows)], ["truncated", "TRUE" if truncated else "FALSE"],
                                       ["tables_available", ", ".join(loaded)]]),
    })
    summary(f"### Preview ready\n\n- **Rows:** {len(rows)}{' (truncated)' if truncated else ''}\n"
            f"- **Sheet:** {url or name}\n- **Deleted after:** {expires:%Y-%m-%d %H:%M} UTC\n\n```sql\n{sql}\n```")
    return 0


if __name__ == "__main__":
    sys.exit(main())
