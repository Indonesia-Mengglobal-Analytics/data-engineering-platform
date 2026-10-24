"""Data quality rules: a standard set from config plus each sheet's optional QUALITY_RULES tab.

A rule checks one column. Failing rows are flagged, never dropped: severity "error" marks the row
dq_status = fail (left out of gold and the dashboard), "warn" marks it warn (kept, but reported).
Issue messages never contain the value itself, only what was wrong with it.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

RULES = {"not_null", "email", "phone", "url", "date", "timestamp", "regex", "allowed_values",
         "min_length", "max_length", "unique"}
SEVERITIES = {"error", "warn"}
DATE_FORMATS = ["%d/%m/%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d %B %Y", "%d %b %Y"]
TIMESTAMP_FORMATS = ["%d/%m/%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                     "%d/%m/%Y", "%m/%d/%Y"]
RULES_TAB_HEADERS = ["column_name", "rule", "parameter", "severity", "description"]


@dataclass(frozen=True)
class Rule:
    column: str
    rule: str
    param: str = ""
    severity: str = "warn"
    origin: str = "standard"
    description: str = ""

    @property
    def id(self) -> str:
        return f"{self.column}:{self.rule}" + (f"({self.param})" if self.param else "")

    def to_dict(self) -> dict:
        return {**asdict(self), "id": self.id}


def standard_rules(columns: list[str], config: list[dict]) -> list[Rule]:
    rules = []
    for spec in config:
        for column in columns:
            if re.search(spec["match"], column):
                rules.append(Rule(column, spec["rule"], str(spec.get("param", "")), spec.get("severity", "warn"),
                                  "standard", spec.get("description", "")))
    return rules


def sheet_rules(rows: list[list[str]] | None, columns: list[str]) -> tuple[list[Rule], list[str]]:
    """Rules from a QUALITY_RULES tab, and warnings for rows that can't be used."""
    if not rows:
        return [], []
    header = [str(h).strip() for h in rows[0]]
    missing = [h for h in RULES_TAB_HEADERS[:2] if h not in header]
    if missing:
        return [], [f"QUALITY_RULES ignored: missing column(s) {', '.join(missing)}"]
    rules, warnings = [], []
    for n, row in enumerate(rows[1:], start=2):
        rec = {h: (str(row[i]).strip() if i < len(row) else "") for i, h in enumerate(header)}
        if not any(rec.values()):
            continue
        column, rule = rec.get("column_name", ""), rec.get("rule", "").lower()
        severity = (rec.get("severity") or "warn").lower()
        if column not in columns:
            warnings.append(f"QUALITY_RULES row {n}: unknown column {column!r}")
        elif rule not in RULES:
            warnings.append(f"QUALITY_RULES row {n}: unknown rule {rule!r} (use one of {', '.join(sorted(RULES))})")
        elif severity not in SEVERITIES:
            warnings.append(f"QUALITY_RULES row {n}: severity must be error or warn")
        elif rule in {"regex", "allowed_values", "min_length", "max_length"} and not rec.get("parameter"):
            warnings.append(f"QUALITY_RULES row {n}: rule {rule} needs a parameter")
        elif rule in {"min_length", "max_length"} and not rec["parameter"].isdigit():
            warnings.append(f"QUALITY_RULES row {n}: {rule} parameter must be a whole number")
        else:
            rules.append(Rule(column, rule, rec.get("parameter", ""), severity, "sheet", rec.get("description", "")))
    return rules, warnings


def _q(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _formats(fmts: list[str]) -> str:
    return "[" + ", ".join(_q(f) for f in fmts) + "]"


def failure_sql(rule: Rule) -> tuple[str, str]:
    """(SQL predicate that is true when the row fails, human message)."""
    c = f'"{rule.column}"'
    present = f"{c} IS NOT NULL"
    if rule.rule == "not_null":
        return f"{c} IS NULL", "is empty"
    if rule.rule == "email":
        return f"{present} AND NOT regexp_matches({c}, '^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$')", "is not a valid email address"
    if rule.rule == "phone":
        return (f"{present} AND NOT regexp_matches(regexp_replace({c}, '[\\s().-]', '', 'g'), '^\\+?[0-9]{{8,15}}$')",
                "is not a valid phone number")
    if rule.rule == "url":
        return f"{present} AND NOT regexp_matches({c}, '^(https?://)?[^\\s/$.?#][^\\s]*\\.[^\\s]+$')", "is not a valid link"
    if rule.rule == "date":
        return f"{present} AND try_strptime({c}, {_formats(DATE_FORMATS)}) IS NULL", "is not a readable date"
    if rule.rule == "timestamp":
        return f"{present} AND try_strptime({c}, {_formats(TIMESTAMP_FORMATS)}) IS NULL", "is not a readable timestamp"
    if rule.rule == "regex":
        return f"{present} AND NOT regexp_matches({c}, {_q(rule.param)})", "does not match the expected pattern"
    if rule.rule == "allowed_values":
        values = [v.strip() for v in re.split(r"[|;]", rule.param) if v.strip()]
        return (f"{present} AND lower({c}) NOT IN ({', '.join(_q(v.lower()) for v in values)})",
                "is not one of the allowed values")
    if rule.rule == "min_length":
        return f"{present} AND length({c}) < {int(rule.param)}", f"is shorter than {int(rule.param)} characters"
    if rule.rule == "max_length":
        return f"{present} AND length({c}) > {int(rule.param)}", f"is longer than {int(rule.param)} characters"
    if rule.rule == "unique":
        return f"{present} AND count(*) OVER (PARTITION BY lower({c})) > 1", "appears more than once"
    raise ValueError(rule.rule)


def evaluate(con, table: str, rules: list[Rule]) -> list[dict]:
    """Run every rule against table (which has a _row column); returns per-rule results and fills dq_issues."""
    con.execute("CREATE OR REPLACE TEMP TABLE dq_issues (_row BIGINT, rule_id VARCHAR, column_name VARCHAR, "
                "rule VARCHAR, severity VARCHAR, message VARCHAR)")
    total = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    results = []
    for rule in rules:
        predicate, message = failure_sql(rule)
        con.execute(
            f"""INSERT INTO dq_issues SELECT _row, ?, ?, ?, ?, ? FROM (
                  SELECT _row, ({predicate}) AS failed FROM {table}) WHERE failed""",
            [rule.id, rule.column, rule.rule, rule.severity, f"{rule.column} {message}"],
        )
        failed = con.execute("SELECT count(*) FROM dq_issues WHERE rule_id = ?", [rule.id]).fetchone()[0]
        results.append({**rule.to_dict(), "checked": total, "failed": failed, "passed": total - failed})
    return results
