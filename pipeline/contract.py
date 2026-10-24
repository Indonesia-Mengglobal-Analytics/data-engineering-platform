"""The source format contract: which Google Sheets may be registered.

A sheet is registered when it has three tabs:
  DATA             one header row, then one row per record
  COLUMN_METADATA  one row per DATA column (data dictionary)
  TABLE_METADATA   one row per table, including a row whose table_name is "data"
and the DATA header matches the dictionary column for column.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime

SNAKE_CASE = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass
class SheetSnapshot:
    """What the pipeline read from one spreadsheet. Tabs map to rows of strings."""

    spreadsheet_id: str
    title: str
    modified_time: str | None
    tabs: dict[str, list[list[str]]]
    # Columns whose values were not read (left out of a partial load); profiled as unknown.
    unread: frozenset[str] = frozenset()


@dataclass
class ContractResult:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    table: dict[str, str] = field(default_factory=dict)
    columns: list[dict[str, str]] = field(default_factory=list)

    @property
    def registered(self) -> bool:
        return not self.errors


def _norm(text: str) -> str:
    return " ".join(str(text).split()).casefold()


def _records(rows: list[list[str]]) -> tuple[list[str], list[dict[str, str]]]:
    if not rows:
        return [], []
    header = [str(h).strip() for h in rows[0]]
    records = []
    for row in rows[1:]:
        if not any(str(v).strip() for v in row):
            continue
        records.append({h: (str(row[i]).strip() if i < len(row) else "") for i, h in enumerate(header)})
    return header, records


def _column_index(location: str) -> int | None:
    """'B1' -> 1, 'AA1' -> 26."""
    match = re.fullmatch(r"([A-Za-z]+)1", location.strip())
    if not match:
        return None
    index = 0
    for char in match.group(1).upper():
        index = index * 26 + (ord(char) - 64)
    return index - 1


def _valid_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y%m%d")
        return True
    except ValueError:
        return False


def check(sheet: SheetSnapshot, contract: dict) -> ContractResult:
    result = ContractResult()
    data_tab = contract["data_tab"]
    column_tab = contract["column_metadata_tab"]
    table_tab = contract["table_metadata_tab"]

    missing = [t for t in (data_tab, column_tab, table_tab) if t not in sheet.tabs]
    if missing:
        result.errors.append(f"Missing tab(s): {', '.join(missing)}")
        return result

    # TABLE_METADATA
    header, tables = _records(sheet.tabs[table_tab])
    absent = [h for h in contract["table_metadata_headers"] if h not in header]
    if absent:
        result.errors.append(f"{table_tab} is missing column(s): {', '.join(absent)}")
    data_rows = [t for t in tables if t.get("table_name", "").casefold() == data_tab.casefold()]
    if not data_rows:
        result.errors.append(f'{table_tab} has no row with table_name "{data_tab.lower()}"')
    else:
        result.table = data_rows[0]
        for key in ("operation_date_start", "operation_date_end", "populated_date"):
            value = result.table.get(key, "")
            if value and not _valid_date(value):
                result.errors.append(f"{table_tab}.{key} must be YYYYMMDD, got {value!r}")
        if not result.table.get("owner_email"):
            result.errors.append(f"{table_tab}.owner_email is empty: every source needs an owner")
        start, end = result.table.get("operation_date_start", ""), result.table.get("operation_date_end", "")
        if _valid_date(start) and _valid_date(end) and start > end:
            result.errors.append(f"{table_tab}: operation_date_start is after operation_date_end")

    # COLUMN_METADATA
    header, columns = _records(sheet.tabs[column_tab])
    absent = [h for h in contract["column_metadata_headers"] if h not in header]
    if absent:
        result.errors.append(f"{column_tab} is missing column(s): {', '.join(absent)}")
        return result
    result.columns = columns

    names = [c["column_name"] for c in columns]
    bad = [n for n in names if not SNAKE_CASE.match(n)]
    if bad:
        result.errors.append(f"column_name must be snake_case: {', '.join(bad[:5])}")
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        result.errors.append(f"Duplicate column_name: {', '.join(dupes)}")
    undocumented = [c["column_name"] for c in columns if not c.get("column_description")]
    if undocumented:
        result.warnings.append(f"{len(undocumented)} column(s) have no description: {', '.join(undocumented[:5])}")

    # DATA header must match the dictionary.
    data_rows_raw = sheet.tabs[data_tab]
    data_header = [str(h) for h in data_rows_raw[0]] if data_rows_raw else []
    while data_header and not data_header[-1].strip():
        data_header.pop()
    if len(data_header) != len(columns):
        result.errors.append(
            f"{data_tab} has {len(data_header)} columns but {column_tab} documents {len(columns)}"
        )
    for column in columns:
        index = _column_index(column.get("column_header_location", ""))
        if index is None:
            result.errors.append(f"{column['column_name']}: column_header_location must look like 'B1'")
            continue
        actual = data_header[index] if index < len(data_header) else ""
        if _norm(actual) != _norm(column.get("form_question", "")):
            result.errors.append(
                f"{column['column_name']}: {data_tab}!{column['column_header_location']} is "
                f"{actual.strip()[:60]!r}, dictionary says {column.get('form_question', '')[:60]!r}"
            )
    return result
