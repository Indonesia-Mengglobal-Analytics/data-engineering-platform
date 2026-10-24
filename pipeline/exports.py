"""Spreadsheet outputs: one per source, one consolidated, and short-lived query previews.

Outputs live in the Shared Drive under outputs/ (rewritten every run) and previews/ (one sheet per
query, deleted after preview_retention_hours). Google Drive controls who can open them, so these are
the only place row-level data is shown.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

CELL_LIMIT = 49_000  # Google Sheets allows 50,000 characters per cell
CHUNK = 5_000


def _cell(v) -> str | int | float:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat(sep=" ") if isinstance(v, datetime) else v.isoformat()
    s = str(v)
    return s if len(s) <= CELL_LIMIT else s[:CELL_LIMIT] + "… (truncated)"


def table(con, sql: str, params: list | None = None) -> tuple[list[str], list[list]]:
    cur = con.execute(sql, params or [])
    header = [d[0] for d in cur.description]
    return header, [[_cell(v) for v in row] for row in cur.fetchall()]


class LocalSheets:
    """Writes each spreadsheet as JSON (tests and runs without a Shared Drive)."""

    kind = "local"

    def __init__(self, root: Path):
        self.root = Path(root)

    def _path(self, folder: str, name: str) -> Path:
        return self.root / folder / (name.replace("/", "_") + ".json")

    def upsert(self, folder: str, name: str, tabs: dict) -> str | None:
        p = self._path(folder, name)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"name": name, "created": datetime.now(timezone.utc).isoformat(),
                                 "tabs": {t: {"header": h, "rows": r} for t, (h, r) in tabs.items()}}, default=str))
        return None

    create = upsert

    def cleanup(self, folder: str, max_age_hours: float) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        removed = []
        for p in (self.root / folder).glob("*.json") if (self.root / folder).exists() else []:
            if datetime.fromisoformat(json.loads(p.read_text())["created"]) < cutoff:
                p.unlink()
                removed.append(p.stem)
        return removed


class GoogleSheets:
    kind = "google"

    def __init__(self, drive, sheets, storage):
        self.drive, self.sheets, self.storage = drive, sheets, storage

    def _find(self, folder: str, name: str) -> str | None:
        parent = self.storage.folder(folder)
        safe = name.replace("'", "\\'")
        found = self.storage._list(f"'{parent}' in parents and name = '{safe}' and trashed = false")
        return found[0]["id"] if found else None

    def _new(self, folder: str, name: str) -> str:
        return self.drive.files().create(
            body={"name": name, "mimeType": "application/vnd.google-apps.spreadsheet", "parents": [self.storage.folder(folder)]},
            fields="id", supportsAllDrives=True).execute()["id"]

    def _write(self, sid: str, tabs: dict) -> None:
        meta = self.sheets.spreadsheets().get(spreadsheetId=sid, fields="sheets.properties").execute()
        existing = {s["properties"]["title"]: s["properties"]["sheetId"] for s in meta.get("sheets", [])}
        requests = [{"addSheet": {"properties": {"title": t, "gridProperties": {"frozenRowCount": 1}}}}
                    for t in tabs if t not in existing]
        if requests:
            self.sheets.spreadsheets().batchUpdate(spreadsheetId=sid, body={"requests": requests}).execute()
        extra = [sheet_id for title, sheet_id in existing.items() if title not in tabs]
        if extra:
            self.sheets.spreadsheets().batchUpdate(
                spreadsheetId=sid, body={"requests": [{"deleteSheet": {"sheetId": i}} for i in extra]}).execute()
        for title, (header, rows) in tabs.items():
            rng = f"'{title}'"
            self.sheets.spreadsheets().values().clear(spreadsheetId=sid, range=rng, body={}).execute()
            values = [header] + rows
            for start in range(0, len(values), CHUNK):
                self.sheets.spreadsheets().values().update(
                    spreadsheetId=sid, range=f"'{title}'!A{start + 1}", valueInputOption="RAW",
                    body={"values": values[start:start + CHUNK]}).execute()

    def upsert(self, folder: str, name: str, tabs: dict) -> str:
        sid = self._find(folder, name) or self._new(folder, name)
        self._write(sid, tabs)
        return f"https://docs.google.com/spreadsheets/d/{sid}/edit"

    def create(self, folder: str, name: str, tabs: dict) -> str:
        sid = self._new(folder, name)
        self._write(sid, tabs)
        return f"https://docs.google.com/spreadsheets/d/{sid}/edit"

    def cleanup(self, folder: str, max_age_hours: float) -> list[str]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=max_age_hours)
        parent = self.storage.folder(folder)
        removed = []
        for f in self.storage._list(f"'{parent}' in parents and trashed = false"):
            created = datetime.fromisoformat(f["createdTime"].replace("Z", "+00:00"))
            if created >= cutoff:
                continue
            try:
                self.drive.files().delete(fileId=f["id"], supportsAllDrives=True).execute()
            except Exception:
                # Content managers can't delete permanently in a Shared Drive; trash instead (purged after 30 days).
                self.drive.files().update(fileId=f["id"], body={"trashed": True}, supportsAllDrives=True).execute()
            removed.append(f["name"])
        return removed


def export_outputs(con, writer, sources: dict[str, dict], run: dict, layers: dict) -> list[dict]:
    """sources: table -> {"title", "id", "rules"}; writes one sheet per source plus CONSOLIDATED."""
    outputs = []
    for t, src in sources.items():
        about = [["run_id", run["run_id"]], ["generated_at", run["started_at"]], ["source", src["title"]],
                 ["spreadsheet_id", src["id"]],
                 ["layers", "bronze (as read) → silver (cleaned, SCD2) → gold (current, passed error rules)"],
                 ["CURRENT", "current version of every record, with dq_status and dq_issues"],
                 ["HISTORY", "every version (SCD type 2): valid_from, valid_to, is_current, is_deleted"],
                 ["DQ_ISSUES", "every rule failure in the latest snapshot"]]
        about += [[f"metric: {k}", json.dumps(v) if isinstance(v, dict) else v]
                  for k, v in layers["silver"][t].items() if k != "rules"]
        about += [[f"rule: {r['id']}", f"{r['severity']} · {r['origin']} · failed {r['failed']} of {r['checked']}"]
                  for r in layers["silver"][t]["rules"]]
        tabs = {
            "CURRENT": table(con, f'SELECT * EXCLUDE (valid_to, is_deleted) FROM silver."{t}" WHERE is_current ORDER BY submitted_at NULLS LAST'),
            "HISTORY": table(con, f'SELECT * FROM silver."{t}" ORDER BY record_key, valid_from'),
            "DQ_ISSUES": table(con, f'SELECT * EXCLUDE (source_table) FROM gold.dq_issues WHERE source_table = ? ORDER BY sheet_row', [t]),
            "ABOUT": (["item", "value"], about),
        }
        name = f"{src['title']} · silver"
        url = writer.upsert("outputs", name, tabs)
        outputs.append({"name": name, "url": url, "rows": {k: len(v[1]) for k, v in tabs.items()}})
    if sources:
        summary = [[t, s["title"], layers["silver"][t]["current"], layers["gold"]["tables"][t],
                    layers["silver"][t]["dq"]["pass"], layers["silver"][t]["dq"]["warn"], layers["silver"][t]["dq"]["fail"]]
                   for t, s in sources.items()]
        tabs = {
            "ALL_DATA": table(con, "SELECT * FROM gold.consolidated ORDER BY source_table, submitted_at NULLS LAST"),
            "DQ_ISSUES": table(con, "SELECT * FROM gold.dq_issues ORDER BY source_table, sheet_row"),
            "SUMMARY": (["source_table", "title", "current_records", "gold_records", "dq_pass", "dq_warn", "dq_fail"], summary),
        }
        url = writer.upsert("outputs", "CONSOLIDATED", tabs)
        outputs.append({"name": "CONSOLIDATED", "url": url, "rows": {k: len(v[1]) for k, v in tabs.items()}})
    return outputs
