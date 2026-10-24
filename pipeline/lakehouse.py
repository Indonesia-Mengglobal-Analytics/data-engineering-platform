"""Bronze, silver and gold layers in DuckDB, persisted as Parquet in the pipeline's storage.

bronze.<t>   every run's snapshot of the sheet, as read, plus _run_id, _ingested_at, _row, _row_hash.
             Append-only; snapshots older than bronze_retention_days are pruned.
silver.<t>   cleaned rows (trimmed, whitespace collapsed, emails lowercased), quality-checked, one version
             per record and change: SCD type 2 with record_key, valid_from, valid_to, is_current, is_deleted.
             dq_status / dq_issues describe the current evaluation of the rules.
gold.<t>     current records that passed every error-level rule: what the dashboard and exports use.
gold.consolidated   current records of every source stacked, with source_table.
gold.dq_issues      every rule failure on the latest snapshot, without the failing values.
"""

from __future__ import annotations

import os
import tempfile

import quality

SCD_COLUMNS = ["record_key", "row_hash", "submitted_at", "dq_status", "dq_issues",
               "valid_from", "valid_to", "is_current", "is_deleted", "_run_id"]


def _cols(con, schema: str, table: str) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        [schema, table]).fetchall()]


def _exists(con, schema: str, table: str) -> bool:
    return bool(_cols(con, schema, table))


def _add_missing(con, schema: str, table: str, columns: list[str], kind: str = "VARCHAR") -> list[str]:
    have = set(_cols(con, schema, table))
    added = [c for c in columns if c not in have]
    for c in added:
        con.execute(f'ALTER TABLE {schema}."{table}" ADD COLUMN "{c}" {kind}')
    return added


def setup(con) -> None:
    for schema in ("bronze", "silver", "gold", "raw"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def load(con, storage, layer: str, table: str) -> bool:
    data = storage.read(f"lakehouse/{layer}/{table}.parquet")
    if not data:
        return False
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        f.write(data)
        path = f.name
    try:
        con.execute(f"CREATE OR REPLACE TABLE {layer}.\"{table}\" AS SELECT * FROM read_parquet('{path}')")
    finally:
        os.unlink(path)
    return True


def save(con, storage, layer: str, table: str) -> int:
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        con.execute(f"COPY {layer}.\"{table}\" TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        with open(path, "rb") as f:
            data = f.read()
    finally:
        os.unlink(path)
    storage.write(f"lakehouse/{layer}/{table}.parquet", data)
    return len(data)


def _hash(columns: list[str], prefix: str = "") -> str:
    parts = ", ".join(f"coalesce({prefix}\"{c}\", '∅')" for c in columns) or "''"
    return f"md5(concat_ws('␟', {parts}))"


def _snap(table: str) -> str:
    return f'"snap__{table}"'


def bronze(con, table: str, source_id: str, names: list[str], rows: list[tuple[int, list]], run_id: str, now,
           retention_days: int) -> dict:
    """rows: (sheet row number, values) for every non-empty DATA row."""
    snap = _snap(table)
    con.execute(f"CREATE OR REPLACE TEMP TABLE {snap} (" + ", ".join(f'"{n}" VARCHAR' for n in names) + ", _row BIGINT)")
    if rows:
        con.executemany(f"INSERT INTO {snap} VALUES ({', '.join('?' for _ in names)}, ?)",
                        [list(values) + [number] for number, values in rows])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bronze_append AS SELECT *, ? AS _run_id, ?::TIMESTAMP AS _ingested_at,
                    ? AS _source_id, {_hash(names)} AS _row_hash FROM {snap}""", [run_id, now, source_id])
    if _exists(con, "bronze", table):
        _add_missing(con, "bronze", table, names)
        con.execute(f'INSERT INTO bronze."{table}" BY NAME SELECT * FROM bronze_append')
    else:
        con.execute(f'CREATE TABLE bronze."{table}" AS SELECT * FROM bronze_append')
    pruned = con.execute(
        f"""DELETE FROM bronze."{table}" WHERE _ingested_at < ?::TIMESTAMP - INTERVAL ({int(retention_days)}) DAY""",
        [now]).fetchone()[0]
    total, snapshots = con.execute(f'SELECT count(*), count(DISTINCT _run_id) FROM bronze."{table}"').fetchone()
    return {"appended": len(rows), "total_rows": total, "snapshots": snapshots, "pruned": pruned,
            "columns": len(names), "retention_days": retention_days}


def silver(con, table: str, names: list[str], rules: list[quality.Rule], key_columns: list[str],
           lower_columns: list[str], run_id: str, now) -> dict:
    # Clean the latest snapshot.
    def clean(c: str) -> str:
        expr = f"""nullif(regexp_replace(trim("{c}"), '\\s+', ' ', 'g'), '')"""
        return f"lower({expr})" if c in lower_columns else expr
    con.execute("CREATE OR REPLACE TEMP TABLE clean AS SELECT _row, "
                + ", ".join(f'{clean(c)} AS "{c}"' for c in names) + f" FROM {_snap(table)}")
    results = quality.evaluate(con, "clean", rules)

    keys = [c for c in key_columns if c in names]
    key_expr = (f"""coalesce({" || '|' || ".join(f'"{c}"' for c in keys)}, 'row:' || {_hash(names)})"""
                if keys else f"'row:' || {_hash(names)}")
    ts = (f"""try_strptime("timestamp", [{", ".join("'" + f + "'" for f in quality.TIMESTAMP_FORMATS)}])"""
          if "timestamp" in names else "NULL::TIMESTAMP")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE staged AS
        SELECT c.*, {key_expr} AS record_key, {_hash(names, 'c.')} AS row_hash, {ts} AS submitted_at,
               coalesce(d.dq_status, 'pass') AS dq_status, d.dq_issues
        FROM clean c LEFT JOIN (
          SELECT _row, CASE WHEN bool_or(severity = 'error') THEN 'fail' ELSE 'warn' END AS dq_status,
                 string_agg(message, '; ' ORDER BY severity, message) AS dq_issues
          FROM dq_issues GROUP BY _row) d USING (_row)""")
    # One version per record: the latest submission wins.
    con.execute("""CREATE OR REPLACE TEMP TABLE latest AS SELECT * FROM staged
                   QUALIFY row_number() OVER (PARTITION BY record_key ORDER BY submitted_at DESC NULLS LAST, _row DESC) = 1""")
    staged_n, latest_n = (con.execute("SELECT count(*) FROM staged").fetchone()[0],
                          con.execute("SELECT count(*) FROM latest").fetchone()[0])

    if not _exists(con, "silver", table):
        con.execute(f"""CREATE TABLE silver."{table}" AS SELECT * EXCLUDE (_row),
                        NULL::TIMESTAMP AS valid_from, NULL::TIMESTAMP AS valid_to, NULL::BOOLEAN AS is_current,
                        NULL::BOOLEAN AS is_deleted, NULL::VARCHAR AS _run_id FROM latest WHERE false""")
    else:
        _add_missing(con, "silver", table, names)

    t = f'silver."{table}"'
    changed = con.execute(f"""SELECT count(*) FROM latest l JOIN {t} s ON s.record_key = l.record_key AND s.is_current
                              WHERE s.row_hash <> l.row_hash""").fetchone()[0]
    inserted = con.execute(f"""SELECT count(*) FROM latest l WHERE NOT EXISTS (
                               SELECT 1 FROM {t} s WHERE s.record_key = l.record_key AND s.is_current)""").fetchone()[0]
    deleted = con.execute(f"""SELECT count(*) FROM {t} s WHERE s.is_current AND NOT EXISTS (
                              SELECT 1 FROM latest l WHERE l.record_key = s.record_key)""").fetchone()[0]
    con.execute(f"""UPDATE {t} SET valid_to = ?::TIMESTAMP, is_current = false
                    WHERE is_current AND record_key IN (
                      SELECT l.record_key FROM latest l JOIN {t} s ON s.record_key = l.record_key AND s.is_current
                      WHERE s.row_hash <> l.row_hash)""", [now])
    con.execute(f"""UPDATE {t} SET valid_to = ?::TIMESTAMP, is_current = false, is_deleted = true
                    WHERE is_current AND record_key NOT IN (SELECT record_key FROM latest)""", [now])
    con.execute(f"""INSERT INTO {t} BY NAME SELECT * EXCLUDE (_row), ?::TIMESTAMP AS valid_from, NULL::TIMESTAMP AS valid_to,
                    true AS is_current, false AS is_deleted, ? AS _run_id
                    FROM latest l WHERE NOT EXISTS (SELECT 1 FROM {t} s WHERE s.record_key = l.record_key AND s.is_current)""",
                [now, run_id])
    # Rules can change without the data changing: refresh the evaluation on current versions in place.
    con.execute(f"""UPDATE {t} SET dq_status = l.dq_status, dq_issues = l.dq_issues FROM latest l
                    WHERE {t}.is_current AND {t}.record_key = l.record_key""")

    status = dict(con.execute("SELECT dq_status, count(*) FROM latest GROUP BY 1").fetchall())
    current, versions = con.execute(f"SELECT count(*) FILTER (WHERE is_current), count(*) FROM {t}").fetchone()
    con.execute(f"""CREATE OR REPLACE TEMP TABLE "issues_{table}" AS
                    SELECT l.record_key, i.column_name, i.rule, i.severity, i.message, i._row AS sheet_row
                    FROM dq_issues i JOIN staged l USING (_row)""")
    return {"rows_read": staged_n, "duplicates_superseded": staged_n - latest_n, "current": current,
            "versions_total": versions, "inserted": inserted, "updated": changed, "deleted": deleted,
            "unchanged": latest_n - inserted - changed,
            "dq": {"pass": status.get("pass", 0), "warn": status.get("warn", 0), "fail": status.get("fail", 0)},
            "rules": results, "record_key": keys or ["row hash"]}


def gold(con, tables: dict[str, str]) -> dict:
    """tables: table name -> source title. Builds gold tables and the raw.* views the dashboard reads."""
    out = {"tables": {}}
    for table in tables:
        con.execute(f"""CREATE OR REPLACE TABLE gold."{table}" AS SELECT * EXCLUDE (valid_to, is_deleted)
                        FROM silver."{table}" WHERE is_current AND dq_status <> 'fail'""")
        con.execute(f'CREATE OR REPLACE VIEW raw."{table}" AS SELECT * FROM gold."{table}"')
        out["tables"][table] = con.execute(f'SELECT count(*) FROM gold."{table}"').fetchone()[0]
    if tables:
        con.execute("CREATE OR REPLACE TABLE gold.consolidated AS " + " UNION ALL BY NAME ".join(
            f"""SELECT '{t}' AS source_table, * EXCLUDE (valid_to, is_deleted) FROM silver."{t}" WHERE is_current"""
            for t in tables))
        con.execute("CREATE OR REPLACE TABLE gold.dq_issues AS " + " UNION ALL ".join(
            f"""SELECT '{t}' AS source_table, * FROM "issues_{t}\"""" for t in tables))
        out["consolidated_rows"] = con.execute("SELECT count(*) FROM gold.consolidated").fetchone()[0]
        out["dq_issue_rows"] = con.execute("SELECT count(*) FROM gold.dq_issues").fetchone()[0]
    return out
