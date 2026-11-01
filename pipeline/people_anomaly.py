"""Anomaly detection for the people dataset: how likely a row is dummy data, and how likely two people are one.

Both are estimates, not a trained model: each signal adds evidence (log-odds), and a logistic turns the sum
into a probability. Weights were set by hand so that obvious test rows land above 90% and ordinary rows
below 10%; anything in between is for a person to check. Every score keeps its reasons.

  dummy_score(record)          -> (probability, [(code, label, weight)])
  duplicate_candidates(...)    -> pairs of people with a probability of being the same person
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter, defaultdict

import people as P

PRIOR = -4.2          # about 1.5% before any signal
DUP_PRIOR = -3.0      # about 5% for two people picked by a blocking rule
LIKELY, SUSPICIOUS = 0.80, 0.50
PLACEHOLDER_NAMES = {"john doe", "jane doe", "john smith", "fulan", "fulanah", "fulan bin fulan", "nama", "nama lengkap", "nama saya",
                     "full name", "your name", "first last", "firstname lastname", "no name", "anonim", "anonymous", "mr x", "x x"}

GIBBERISH_RE = re.compile(r"[bcdfghjklmnpqrstvwxz]{5,}", re.I)


def logistic(x: float) -> float:
    return 1 / (1 + math.exp(-x))


def _letters(s: str) -> str:
    return re.sub(r"[^a-z]", "", unicodedata.normalize("NFKD", s or "").encode("ascii", "ignore").decode().lower())


def _is_keyboard_run(s: str) -> bool:
    s = _letters(s)
    return len(s) >= 4 and any(s in row or s in row[::-1] for row in P.KEYBOARD_ROWS)


def _one_letter(s: str) -> bool:
    s = _letters(s)
    return len(s) >= 3 and len(set(s)) == 1


def _gibberish(name: str) -> bool:
    letters = _letters(name)
    if len(letters) < 6:
        return False
    vowels = sum(c in "aeiouy" for c in letters)
    return vowels / len(letters) < 0.18 or bool(GIBBERISH_RE.search(letters))


# ---------------------------------------------------------------- dummy rows

SIGNALS = {
    "dummy_email_domain": ("Test or throwaway email domain (example.com, mailinator…)", 6.0),
    "dummy_email_local": ("Email name is a test word (test@, asdf@, aaaa@…)", 4.5),
    "invalid_email": ("Email is not a valid address", 1.6),
    "missing_email": ("No email", 1.2),
    "dummy_name": ("Name is a test word (test, asdf, dummy…)", 4.0),
    "one_letter_name": ("Name is one letter repeated or a single letter", 3.0),
    "keyboard_name": ("Name is a keyboard run (qwerty, asdf…)", 3.5),
    "gibberish_name": ("Name looks random (almost no vowels)", 2.2),
    "placeholder_name": ("Placeholder name (John Doe, Fulan, Nama Lengkap…)", 4.5),
    "name_is_email": ("One-word name typed exactly as the email user name", 1.2),
    "digits_in_name": ("Digits in the name", 1.0),
    "placeholder_phone": ("Placeholder phone (0000…, 1234…)", 2.2),
    "invalid_phone": ("Phone can't be read", 0.6),
    "year_out_of_range": ("Year outside the program", 0.8),
    "row_dummy_text": ("Lorem ipsum, 'test' or 'dummy' typed in the row", 2.0),
    "duplicate_row": ("Same person submitted twice in one source and year", 0.4),
    "dummy_source_column": ("Source has a column named dummy", 0.3),
}


def dummy_score(r: P.Record) -> tuple[float, list[tuple[str, str, float]]]:
    hits: list[str] = []
    email = r.email or ""
    local, _, domain = email.partition("@")
    if domain in P.DUMMY_DOMAINS or domain.endswith((".test", ".invalid", ".example")):
        hits.append("dummy_email_domain")
    if local and (P.is_dummy_token(local) or _is_keyboard_run(local) or _one_letter(local)):
        hits.append("dummy_email_local")
    if "invalid_email" in r.anomalies:
        hits.append("invalid_email")
    if "missing_email" in r.anomalies:
        hits.append("missing_email")
    name = r.name or ""
    if "dummy_name" in r.anomalies and not (_one_letter(name) or _is_keyboard_run(name)):
        hits.append("dummy_name")
    if name and (_one_letter(name) or len(_letters(name)) == 1):
        hits.append("one_letter_name")
    if name and _is_keyboard_run(name):
        hits.append("keyboard_name")
    if name and _gibberish(name) and "keyboard_name" not in hits:
        hits.append("gibberish_name")
    if P.name_key(name) in PLACEHOLDER_NAMES:
        hits.append("placeholder_name")
    # Real people often use their name as the address (ayu.pratama@); only a one-word name typed as is counts.
    if name and local and " " not in name.strip() and name.strip().lower() == local and len(local) >= 3:
        hits.append("name_is_email")
    if "name_contains_digits" in r.anomalies:
        hits.append("digits_in_name")
    for code in ("placeholder_phone", "invalid_phone", "year_out_of_range", "duplicate_row", "dummy_source_column"):
        if code in r.anomalies:
            hits.append(code)
    digits = re.sub(r"\D", "", str(r.phone_raw or ""))
    if "placeholder_phone" not in hits and len(digits) >= 8 and re.search(r"1234567|2345678|9876543|(\d)\1{6}", digits):
        hits.append("placeholder_phone")
    if r.row_dummy_hits:
        hits.append("row_dummy_text")
    reasons = [(c, SIGNALS[c][0], SIGNALS[c][1]) for c in dict.fromkeys(hits)]
    return logistic(PRIOR + sum(w for _, _, w in reasons)), reasons


def level(p: float) -> str:
    return "likely" if p >= LIKELY else "suspicious" if p >= SUSPICIOUS else "clean"


def mask_email(e: str | None) -> str | None:
    """t***@example.com: enough to recognise a pattern, not to contact anyone."""
    if not e or "@" not in str(e):
        return None
    local, _, domain = str(e).strip().lower().partition("@")
    return f"{local[:1]}{'*' * min(4, max(1, len(local) - 1))}@{domain}"


# ---------------------------------------------------------------- duplicates

def jaro_winkler(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a or not b:
        return 0.0
    rng = max(len(a), len(b)) // 2 - 1
    am, bm = [False] * len(a), [False] * len(b)
    m = 0
    for i, c in enumerate(a):
        for j in range(max(0, i - rng), min(len(b), i + rng + 1)):
            if not bm[j] and b[j] == c:
                am[i] = bm[j] = True
                m += 1
                break
    if not m:
        return 0.0
    t, k = 0, 0
    for i, c in enumerate(a):
        if am[i]:
            while not bm[k]:
                k += 1
            t += c != b[k]
            k += 1
    jaro = (m / len(a) + m / len(b) + (m - t / 2) / m) / 3
    prefix = 0
    for x, y in zip(a[:4], b[:4]):
        if x != y:
            break
        prefix += 1
    return jaro + prefix * 0.1 * (1 - jaro)


def _name_sim(a: str | None, b: str | None) -> float:
    ka, kb = P.name_key(a), P.name_key(b)
    if not ka or not kb:
        return 0.0
    whole = jaro_winkler(ka, kb)
    ta, tb = ka.split(), kb.split()
    # Token match also accepts initials: "m rizky" ~ "muhammad rizky".
    def tok(x, ys):
        return max((1.0 if (len(x) == 1 and y.startswith(x)) or (len(y) == 1 and x.startswith(y)) else jaro_winkler(x, y)) for y in ys)
    tokens = (sum(tok(x, tb) for x in ta) / len(ta) + sum(tok(y, ta) for y in tb) / len(tb)) / 2
    return max(whole, tokens)


DUP_SIGNALS = {
    "same_phone": ("Same phone number", 3.4),
    "same_name": ("Same full name", 2.6),
    "similar_name": ("Very similar name", 1.8),
    "same_birthday": ("Same date of birth", 3.0),
    "similar_email": ("Similar email user name", 1.6),
    "same_institution": ("Same institution", 0.8),
    "same_city": ("Same city", 0.5),
    "different_name": ("Clearly different names", -3.0),
    "different_birthday": ("Different dates of birth", -3.5),
    "both_roles_same_year": ("Mentor and mentee in the same program year", 0.0),
}


def duplicate_candidates(result: dict, records: list[P.Record], gold_by_id: dict, review: list[dict]) -> list[dict]:
    """Pairs from the review queue plus similar-name pairs found by blocking; each with a probability."""
    people = {p["person_id"]: p for p in result["people"]}
    bday: dict[str, set] = defaultdict(set)
    for r in records:
        if not r.excluded and r.email and r.birthday:
            bday[P.person_id(r.email)].add(re.sub(r"\D", "", str(r.birthday)))
    pairs: dict[frozenset, dict] = {}
    for rv in review:
        ids = rv["person_ids"]
        if len(ids) < 2:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                pairs.setdefault(frozenset((ids[i], ids[j])), {"source": rv["type"], "review_id": rv["review_id"]})
    # Blocking, two keys so initials still meet ("M. Rizki Pratama" ~ "Muhammad Rizky Pratama"):
    # first initial + surname start, and first-name start + surname initial. Only close names are kept.
    blocks: dict[tuple, list[str]] = defaultdict(list)
    for pid, p in people.items():
        toks = P.name_key(p["full_name"]).split()
        if len(toks) >= 2:
            blocks[("a", toks[0][:1], toks[-1][:3])].append(pid)
            blocks[("b", toks[0][:3], toks[-1][:1])].append(pid)
    for ids in blocks.values():
        if len(ids) > 400:   # very common names: rely on phone and exact-name rules instead
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = people[ids[i]], people[ids[j]]
                if _name_sim(a["full_name"], b["full_name"]) >= 0.92:
                    pairs.setdefault(frozenset((ids[i], ids[j])), {"source": "similar_name", "review_id": None})
    out = []
    for key, meta in pairs.items():
        ia, ib = sorted(key)
        a, b = people[ia], people[ib]
        ga, gb = gold_by_id.get(ia, {}), gold_by_id.get(ib, {})
        hits: list[str] = []
        if a["phone"] and a["phone"] == b["phone"]:
            hits.append("same_phone")
        ns = _name_sim(a["full_name"], b["full_name"])
        if P.name_key(a["full_name"]) and P.name_key(a["full_name"]) == P.name_key(b["full_name"]):
            hits.append("same_name")
        elif ns >= 0.9:
            hits.append("similar_name")
        elif ns < 0.6 and a["full_name"] and b["full_name"]:
            hits.append("different_name")
        if bday[ia] and bday[ib]:
            hits.append("same_birthday" if bday[ia] & bday[ib] else "different_birthday")
        la, lb = a["email_key"].split("@")[0], b["email_key"].split("@")[0]
        if jaro_winkler(_letters(la), _letters(lb)) >= 0.85:
            hits.append("similar_email")
        if ga.get("institution") and ga.get("institution") == gb.get("institution"):
            hits.append("same_institution")
        if ga.get("residence_city") and ga.get("residence_city") == gb.get("residence_city"):
            hits.append("same_city")
        reasons = [(c, DUP_SIGNALS[c][0], DUP_SIGNALS[c][1]) for c in hits]
        p = logistic(DUP_PRIOR + sum(w for _, _, w in reasons) + 2.0 * max(0.0, ns - 0.85))
        if meta["source"] == "similar_name" and p < SUSPICIOUS:
            continue  # blocking finds many namesakes; keep only the ones worth a look
        out.append({"a": ia, "b": ib, "probability": round(p, 4), "type": meta["source"], "review_id": meta["review_id"],
                    "name_similarity": round(ns, 3), "reasons": reasons})
    out.sort(key=lambda x: -x["probability"])
    return out


# ---------------------------------------------------------------- site aggregates (counts only)

def _hist(ps: list[float]) -> list[dict]:
    bins = [0] * 10
    for p in ps:
        bins[min(9, int(p * 10))] += 1
    return [{"from": i * 10, "to": i * 10 + 10, "count": n} for i, n in enumerate(bins)]


def summary(records: list[P.Record], inventory: list[dict], dups: list[dict]) -> dict:
    ps = [r.dummy_p for r in records]
    levels = Counter(level(p) for p in ps)
    reasons = Counter(code for r in records for code, _, _ in r.dummy_reasons if r.dummy_p >= SUSPICIOUS)
    titles = {s["key"]: s["title"] for s in inventory}
    by_src: dict[str, list[float]] = defaultdict(list)
    for r in records:
        by_src[r.source].append(r.dummy_p)
    return {
        "method": "Weighted signals turned into a probability (logistic). An estimate to rank what to check, not a trained model.",
        "thresholds": {"likely": LIKELY, "suspicious": SUSPICIOUS},
        "dummy": {
            "rows": len(ps), "likely": levels["likely"], "suspicious": levels["suspicious"], "clean": levels["clean"],
            "hist": _hist(ps),
            "reasons": [{"code": c, "label": SIGNALS[c][0], "weight": SIGNALS[c][1], "rows": n} for c, n in reasons.most_common()],
            "signals": [{"code": c, "label": l, "weight": w} for c, (l, w) in SIGNALS.items()],
            "by_source": sorted([{"key": k, "title": titles.get(k, k), "rows": len(v), "likely": sum(p >= LIKELY for p in v),
                                  "suspicious": sum(SUSPICIOUS <= p < LIKELY for p in v), "mean": round(sum(v) / len(v), 4)}
                                 for k, v in by_src.items()], key=lambda x: -(x["likely"] + x["suspicious"])),
        },
        "duplicates": {
            "pairs": len(dups), "likely": sum(d["probability"] >= LIKELY for d in dups),
            "suspicious": sum(SUSPICIOUS <= d["probability"] < LIKELY for d in dups),
            "hist": _hist([d["probability"] for d in dups]),
            "by_type": dict(Counter(d["type"] for d in dups)),
            "signals": [{"code": c, "label": l, "weight": w} for c, (l, w) in DUP_SIGNALS.items()],
        },
    }


def unnamed(records: list[P.Record], result: dict, inventory: list[dict]) -> dict:
    """People with no name in any of their records, and why: the source has no name column, or it was left blank."""
    titles = {s["key"]: s["title"] for s in inventory}
    by_pid: dict[str, list[P.Record]] = defaultdict(list)
    for r in records:
        if not r.excluded and r.email:
            by_pid[P.person_id(r.email)].append(r)
    people = [p for p in result["people"] if not p["full_name"]]
    src_people: Counter = Counter()
    cause: Counter = Counter()
    rows = []
    for p in people:
        recs = by_pid.get(p["person_id"], [])
        srcs = sorted({r.source for r in recs})
        for s in srcs:
            src_people[s] += 1
        why = "no_name_column" if all(not r.source_has_name for r in recs) else "left_blank"
        cause[why] += 1
        rows.append({"person_id": p["person_id"], "email_masked": mask_email(p["email"]), "sources": srcs, "cause": why,
                     "labels": p["labels"], "years": p["years"]})
    has_name = {s["key"]: any(r.source_has_name for r in records if r.source == s["key"]) for s in inventory}
    return {
        "people": len(people), "of": len(result["people"]),
        "causes": {"no_name_column": cause["no_name_column"], "left_blank": cause["left_blank"]},
        "by_source": [{"key": k, "title": titles.get(k, k), "people": n, "has_name_column": has_name.get(k, True)}
                      for k, n in src_people.most_common()],
        "rows": rows,
    }
