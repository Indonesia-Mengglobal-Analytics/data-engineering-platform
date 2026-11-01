"""People dataset: everyone in management, events and mentorship, deduplicated into one row per person.

Steps (shown as the people_dataset job on the site):
  read      every configured source tab (config/people.yml)
  normalize email, phone, name and year, and flag anomalies: dummy/test data, broken emails and
            phones, impossible years, duplicate rows
  resolve   one person per normalized email; phone/name-only matches go to a review queue
  build     gold tables: people, activities, anomalies, review, sources
  store     Parquet in the lakehouse (people/) and the PEOPLE spreadsheet in outputs/
  publish   aggregates only, sealed with the site PIN (site/data/people.json)
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

EMAIL_RE = re.compile(r"^[a-z0-9._%+'-]+@[a-z0-9-]+(\.[a-z0-9-]+)*\.[a-z]{2,}$")
DUMMY_WORDS = {"test", "tes", "testing", "tester", "dummy", "sample", "example", "contoh", "coba", "asdf", "asd",
               "qwerty", "qwe", "abc", "abcd", "xxx", "xx", "x", "aaa", "na", "n/a", "none", "null", "nil", "-",
               "no name", "noname", "anonymous", "anon", "user", "admin", "foo", "bar", "lorem", "ipsum", "blabla"}
DUMMY_DOMAINS = {"example.com", "example.org", "example.net", "test.com", "test.id", "mailinator.com", "yopmail.com",
                 "tempmail.com", "10minutemail.com", "guerrillamail.com", "sharklasers.com", "trashmail.com",
                 "email.com", "mail.com.invalid"}
GMAIL = {"gmail.com", "googlemail.com"}
KEYBOARD_ROWS = ("qwertyuiop", "asdfghjkl", "zxcvbnm", "1234567890")


# ---------------------------------------------------------------- normalization

def norm_email(raw: str | None) -> str | None:
    if raw is None:
        return None
    e = str(raw).strip().lower().replace("mailto:", "").replace(" ", "").strip(".;,<>()[]\"'")
    if not e:
        return None
    if "@" in e:
        local, _, domain = e.rpartition("@")
        if domain in GMAIL:
            # Gmail ignores dots and anything after "+": these are the same mailbox.
            local, domain = local.split("+", 1)[0].replace(".", ""), "gmail.com"
        e = f"{local}@{domain}"
    return e


def norm_phone(raw: str | None) -> tuple[str | None, list[str]]:
    """(E.164-like digits with leading +, issues). Indonesian local numbers become +62."""
    issues: list[str] = []
    if raw is None:
        return None, issues
    s = str(raw).strip().strip('"').strip("‪‬")
    if not s:
        return None, issues
    if s.upper().startswith("#") or "error" in s.lower():
        return None, ["invalid_phone"]
    parts = re.split(r"\s*(?:/|;|,|\bor\b|\batau\b)\s*", s)
    if len([p for p in parts if re.search(r"\d{6,}", re.sub(r"\D", "", p))]) > 1:
        issues.append("multiple_phones")
    first = parts[0]
    plus = first.strip().startswith("+")
    digits = re.sub(r"\D", "", first)
    if digits.startswith("00"):
        digits, plus = digits[2:], True
    if digits.startswith("620"):
        digits = "62" + digits[3:]
    elif digits.startswith("0"):
        digits = "62" + digits[1:]
    elif not plus and digits.startswith("8") and 9 <= len(digits) <= 12:
        digits = "62" + digits
    if not 8 <= len(digits) <= 15:
        issues.append("invalid_phone")
        return None, issues
    body = digits[2:] if digits.startswith("62") else digits
    if len(set(body)) <= 2 or body in "0123456789012345" or body in "9876543210987654":
        issues.append("placeholder_phone")
    return "+" + digits, issues


def clean_name(raw: str | None) -> str | None:
    if raw is None:
        return None
    n = re.sub(r"\s+", " ", str(raw)).strip().strip(".,;-")
    return n or None


def name_key(name: str | None) -> str:
    if not name:
        return ""
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", " ", n)).strip()


def parse_year(raw, min_year: int, max_year: int) -> int | None:
    if raw is None:
        return None
    m = re.findall(r"(?<!\d)(20\d{2}|19\d{2})(?!\d)", str(raw))
    for y in m:
        if min_year <= int(y) <= max_year:
            return int(y)
    return int(m[0]) if m else None


def person_id(email_norm: str) -> str:
    """Stable across runs: the same email always gets the same ID."""
    return "IMP-" + hashlib.sha256(email_norm.encode()).hexdigest()[:10].upper()


# ---------------------------------------------------------------- anomaly checks

def is_dummy_token(t: str) -> bool:
    t = t.strip().lower()
    if not t:
        return False
    if t in DUMMY_WORDS or re.fullmatch(r"(test|tes|testing|dummy|sample|coba|asdf|qwerty|abc|user)\d*", t):
        return True
    if len(t) >= 3 and len(set(t)) == 1:  # aaaa, xxxx, 1111
        return True
    return any(len(t) >= 4 and t in row for row in KEYBOARD_ROWS)  # asdf, qwer, 1234


def email_anomalies(e: str | None) -> list[str]:
    if not e:
        return ["missing_email"]
    if not EMAIL_RE.match(e):
        return ["invalid_email"]
    local, _, domain = e.partition("@")
    if domain in DUMMY_DOMAINS or is_dummy_token(local) or domain.split(".")[0] in {"test", "dummy", "example"}:
        return ["dummy_email"]
    return []


def name_anomalies(name: str | None) -> list[str]:
    if not name:
        return ["missing_name"]
    key = name_key(name)
    out = []
    if not key or is_dummy_token(key) or all(is_dummy_token(t) for t in key.split()) or len(key.replace(" ", "")) <= 1:
        out.append("dummy_name")
    if "@" in name or re.search(r"https?://|www\.", name, re.I):
        out.append("name_contains_contact")
    elif re.search(r"\d", name):
        out.append("name_contains_digits")
    return out


ANOMALY_INFO = {
    "missing_email": ("error", "No email: the person can't be identified", "excluded"),
    "invalid_email": ("error", "Email is not a valid address", "excluded"),
    "dummy_email": ("error", "Test or dummy email (test@, example.com, asdf…)", "excluded"),
    "dummy_name": ("error", "Test or dummy name (test, asdf, aaaa, single letter…)", "excluded"),
    "missing_name": ("warn", "Name is empty", "kept"),
    "name_contains_contact": ("warn", "Name holds an email or link", "kept"),
    "name_contains_digits": ("warn", "Name contains digits", "kept"),
    "invalid_phone": ("warn", "Phone can't be read (#ERROR!, too short or long)", "phone dropped"),
    "placeholder_phone": ("warn", "Placeholder phone (repeated or sequential digits)", "phone dropped"),
    "multiple_phones": ("info", "More than one phone in one cell: the first is used", "kept"),
    "missing_year": ("warn", "No batch or year", "kept"),
    "year_out_of_range": ("warn", "Year outside the program's lifetime", "kept"),
    "duplicate_row": ("info", "Same person submitted twice in the same source and year", "merged"),
    "dummy_source_column": ("warn", "Source has a column named dummy: check what it holds", "kept"),
}
EXCLUDING = {k for k, v in ANOMALY_INFO.items() if v[2] == "excluded"}


# ---------------------------------------------------------------- records

@dataclass
class Record:
    source: str
    sheet_row: int
    label: str
    status: str
    email_raw: str | None
    name_raw: str | None
    phone_raw: str | None
    year: int | None
    event: str | None = None
    linkedin: str | None = None
    attrs: dict = field(default_factory=dict)
    source_has_name: bool = True
    email: str | None = None
    name: str | None = None
    phone: str | None = None
    anomalies: list[str] = field(default_factory=list)

    @property
    def excluded(self) -> bool:
        return any(a in EXCLUDING for a in self.anomalies)


def _header_index(header: list[str]) -> dict[str, int]:
    idx: dict[str, int] = {}
    for i, h in enumerate(header):
        k = re.sub(r"\s+", " ", str(h)).strip().lower()
        if k and k not in idx:
            idx[k] = i
    return idx


def _pick(row: list, idx: dict[str, int], names: list[str]):
    for n in names:
        i = idx.get(n.lower())
        if i is not None and i < len(row):
            v = row[i]
            if v is not None and str(v).strip() != "":
                return v
    return None


def attribute_columns(source: dict, attributes: dict) -> dict[str, list[str]]:
    """Profile field -> candidate headers; a source can override them (attributes: in its config)."""
    return {**attributes, **{k: (v if isinstance(v, list) else [v]) for k, v in (source.get("attributes") or {}).items()}}


def extract(source: dict, rows: list[list], cols: dict, min_year: int, max_year: int,
            attributes: dict | None = None) -> tuple[list[Record], dict]:
    """Rows of one source tab (header first) -> records, plus source-level facts."""
    hr = int(source.get("header_row", 1))
    if len(rows) < hr:
        return [], {"rows": 0, "header": []}
    header = [str(h) for h in rows[hr - 1]]
    idx = _header_index(header)
    facts = {"header": header, "rows": 0, "dummy_columns": [h for h in header if "dummy" in h.lower()]}
    roles = {k.lower(): v for k, v in (source.get("roles") or {}).items()}
    year_cols = ([source["year_column"]] if source.get("year_column") else []) + cols["year"] + ["timestamp"]
    attr_cols = attribute_columns(source, attributes or {})
    out = []
    for n, row in enumerate(rows[hr:], start=hr + 1):
        if not any(str(v).strip() for v in row):
            continue
        facts["rows"] += 1
        label, status = source["label"], source.get("status", "")
        if source.get("selected_column"):
            sel = str(_pick(row, idx, [source["selected_column"]]) or "").strip().lower()
            status = "SELECTED" if sel in {"true", "yes", "1", "selected", "ya"} else "CANDIDATE"
        if source.get("role_column"):
            role = str(_pick(row, idx, [source["role_column"]]) or "").strip().lower()
            if role in roles:
                label, status = roles[role]["label"], roles[role]["status"]
        year = source.get("year") or parse_year(_pick(row, idx, year_cols), min_year, max_year)
        rec = Record(source=source["key"], sheet_row=n, label=label, status=status,
                     email_raw=_pick(row, idx, cols["email"]), name_raw=_pick(row, idx, cols["full_name"]),
                     phone_raw=_pick(row, idx, cols["phone"]), year=year,
                     event=_pick(row, idx, [source["event_column"]]) if source.get("event_column") else None,
                     linkedin=_pick(row, idx, cols["linkedin"]),
                     attrs={a: _pick(row, idx, names) for a, names in attr_cols.items()})
        out.append(rec)
    return out, facts


def check(rec: Record, min_year: int, max_year: int) -> None:
    rec.email = norm_email(rec.email_raw)
    rec.anomalies += email_anomalies(rec.email)
    rec.name = clean_name(rec.name_raw)
    # Sources without a name column (bootcamp lists) are not flagged for it.
    if rec.source_has_name:
        rec.anomalies += name_anomalies(rec.name)
    rec.phone, issues = norm_phone(rec.phone_raw)
    rec.anomalies += issues
    if "placeholder_phone" in issues:
        rec.phone = None
    if rec.year is None:
        rec.anomalies.append("missing_year")
    elif not min_year <= rec.year <= max_year:
        rec.anomalies.append("year_out_of_range")


# ---------------------------------------------------------------- resolution

def _similar(a: str, b: str) -> bool:
    ta, tb = set(name_key(a).split()), set(name_key(b).split())
    return bool(ta and tb and (ta & tb))


def resolve(records: list[Record]) -> dict:
    """Group clean records into people by normalized email; find pairs a person must review."""
    good = [r for r in records if not r.excluded]
    by_email: dict[str, list[Record]] = defaultdict(list)
    for r in good:
        by_email[r.email].append(r)

    # Same email, same source, same year, same label: one submission counts once.
    seen: set = set()
    for r in good:
        k = (r.email, r.source, r.year, r.label)
        if k in seen:
            r.anomalies.append("duplicate_row")
        seen.add(k)

    people, activities = [], {}
    for email, recs in by_email.items():
        pid = person_id(email)
        names = Counter(r.name for r in recs if r.name and "dummy_name" not in r.anomalies)
        latest = max(recs, key=lambda r: (r.year or 0, r.sheet_row))
        best_name = max(names.items(), key=lambda kv: (kv[1], kv[0] == latest.name))[0] if names else None
        phones = [r.phone for r in sorted(recs, key=lambda r: (r.year or 0, r.sheet_row), reverse=True) if r.phone]
        # Show an address the person actually typed; the normalized key only groups them.
        typed = Counter(str(r.email_raw).strip().lower() for r in recs)
        shown = max(typed.items(), key=lambda kv: (kv[1], kv[0] == str(latest.email_raw).strip().lower()))[0]
        labels = sorted({r.label for r in recs})
        years = sorted({r.year for r in recs if r.year})
        for r in recs:
            key = (pid, r.label, r.year, r.status)
            if key not in activities:
                activities[key] = {"activity_id": "IMA-" + hashlib.sha256("|".join(map(str, key)).encode()).hexdigest()[:10].upper(),
                                   "person_id": pid, "full_name": best_name, "email": shown, "activity_year": r.year,
                                   "label": r.label, "status": r.status, "event": r.event, "sources": {r.source}}
            else:
                activities[key]["sources"].add(r.source)
                if r.event and not activities[key]["event"]:
                    activities[key]["event"] = r.event
        people.append({"person_id": pid, "full_name": best_name, "email": shown, "email_key": email,
                       "raw_emails": sorted(set(typed) - {shown}),
                       "phone": phones[0] if phones else None,
                       "linkedin": next((r.linkedin for r in recs if r.linkedin), None),
                       "years": years, "first_year": years[0] if years else None, "last_year": years[-1] if years else None,
                       "labels": labels, "n_records": len(recs), "n_sources": len({r.source for r in recs}),
                       "name_variants": sorted(names)})

    review = []
    # Same email, clearly different names.
    for p in people:
        v = p["name_variants"]
        if len(v) > 1 and not all(_similar(v[0], x) for x in v[1:]):
            review.append({"type": "same_email_different_names", "person_ids": [p["person_id"]],
                           "detail": f"{len(v)} name variants under one email", "suggestion": "check the email isn't shared"})
    # Same phone, different emails: maybe one person with two addresses.
    by_phone: dict[str, set] = defaultdict(set)
    for p in people:
        if p["phone"]:
            by_phone[p["phone"]].add(p["person_id"])
    pmap = {p["person_id"]: p for p in people}
    for phone, ids in by_phone.items():
        if len(ids) > 1:
            ids = sorted(ids)
            same = all(_similar(pmap[ids[0]]["full_name"] or "", pmap[i]["full_name"] or "") for i in ids[1:])
            review.append({"type": "same_phone_similar_name" if same else "same_phone_different_name", "person_ids": ids,
                           "detail": f"{len(ids)} people share one phone",
                           "suggestion": "likely one person with two emails: merge after checking" if same
                           else "shared or mistyped phone: check before merging"})
    # Same full name (at least two words), different emails, no shared phone.
    by_name: dict[str, set] = defaultdict(set)
    for p in people:
        k = name_key(p["full_name"])
        if len(k.split()) >= 2:
            by_name[k].add(p["person_id"])
    phone_pairs = {frozenset(r["person_ids"]) for r in review if r["type"].startswith("same_phone")}
    for k, ids in by_name.items():
        if len(ids) > 1 and frozenset(ids) not in phone_pairs:
            review.append({"type": "same_name_different_email", "person_ids": sorted(ids),
                           "detail": f"{len(ids)} people share the full name", "suggestion": "may be namesakes: check institution or phone"})
    # Mentor and mentee in the same program and year.
    acts_by_person = defaultdict(set)
    for a in activities.values():
        acts_by_person[a["person_id"]].add((a["label"], a["activity_year"]))
    for pid, acts in acts_by_person.items():
        for prog in ("PHD", "MASTER"):
            mentor_years = {y for lbl, y in acts if lbl == f"{prog}_MENTOR"}
            mentee_years = {y for lbl, y in acts if lbl == f"{prog}_MENTEE"}
            both = sorted(y for y in mentor_years & mentee_years if y)
            if both:
                review.append({"type": "mentor_and_mentee_same_year", "person_ids": [pid],
                               "detail": f"{prog.title()} mentor and mentee in {', '.join(map(str, both))}",
                               "suggestion": "kept apart per the 2025 dedup report: confirm which role is right"})
    for i, r in enumerate(review):
        r["review_id"] = f"R{i + 1:04d}"
    return {"people": people, "activities": list(activities.values()), "review": review}


def run_checks(sources: list[dict], tabs: dict, cols: dict, min_year: int, max_year: int,
               attributes: dict | None = None) -> tuple[list[Record], list[dict]]:
    """tabs: source key -> rows (or an Exception). Returns records and the source inventory."""
    records, inventory = [], []
    for s in sources:
        data = tabs.get(s["key"])
        entry = {"key": s["key"], "title": s.get("title", s["key"]), "label": s["label"], "spreadsheet_id": s.get("spreadsheet_id"),
                 "tab": s.get("tab")}
        if isinstance(data, Exception) or data is None:
            entry.update({"status": "unreachable", "error": str(data) if data else "not read", "rows": 0})
            inventory.append(entry)
            continue
        recs, facts = extract(s, data, cols, min_year, max_year, attributes)
        has_name = any(h.strip().lower() in [c.lower() for c in cols["full_name"]] for h in facts["header"])
        for r in recs:
            r.source_has_name = has_name
            check(r, min_year, max_year)
            if facts["dummy_columns"]:
                r.anomalies.append("dummy_source_column")
        mapped = {k: next((h for h in facts["header"] if re.sub(r"\s+", " ", h).strip().lower() in [c.lower() for c in v]), None)
                  for k, v in {**cols, **attribute_columns(s, attributes or {})}.items()}
        entry.update({"status": "read", "rows": facts["rows"], "records": len(recs),
                      "excluded": sum(1 for r in recs if r.excluded), "mapped_columns": mapped,
                      "dummy_columns": facts["dummy_columns"]})
        records += recs
        inventory.append(entry)
    return records, inventory


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
