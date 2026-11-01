"""People gold: one profile row per person, mentor-mentee links, and a people graph.

Builds on the resolved people (people.resolve): every clean record of a person adds profile
fields (where they live, were born, studied, want to study), events, and roles. Mentor-mentee
pairs come from the pairing sheets in config/people.yml (pairings). The graph connects people to
events, places, institutions, roles and each other.

Rows (names, emails) go to the lakehouse and the PEOPLE sheet only. site_aggregates() turns the
gold table into counts by role and region with groups under K suppressed.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import Counter, defaultdict

import people as P

ROLE_ORDER = ["MANAGEMENT_MEMBER", "PHD_MENTOR", "MASTER_MENTOR", "EVENT_SPEAKER", "EVENT_COMMITTEE", "PHD_MENTEE",
              "MASTER_MENTEE", "MANAGEMENT_CANDIDATE", "EVENT_PARTICIPANT"]
EVENT_BUCKETS = [(0, 0, "0"), (1, 1, "1"), (2, 2, "2"), (3, 5, "3–5"), (6, 10, "6–10"), (11, 10**9, "11+")]
RESIDENCE_FIELDS = ("residence_city", "residence_province", "residence_country")
MENTEE_BUCKETS = [(1, 1, "1"), (2, 2, "2"), (3, 3, "3"), (4, 5, "4–5"), (6, 10**9, "6+")]


# ---------------------------------------------------------------- places

def _norm_text(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"\b(kota|kabupaten|kab|city of|city|regency|province|provinsi|prov)\b\.?", " ", s)
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


class Places:
    """Matches free-text places to city, province, island region and country."""

    def __init__(self, cfg: dict):
        self.city: dict[str, tuple[str, str]] = {}      # alias -> (city, province)
        self.province: dict[str, str] = {}              # alias -> province
        self.country: dict[str, str] = {}               # alias -> country
        self.island = {p: v["region"] for p, v in cfg.get("provinces", {}).items()}
        self.world = {c: v["region"] for c, v in cfg.get("countries", {}).items()}
        for prov, v in cfg.get("provinces", {}).items():
            for a in v.get("aliases", []) + [prov]:
                self.province[_norm_text(a)] = prov
            for c in v.get("cities", []):
                self.city[_norm_text(c)] = (c.title(), prov)
        for country, v in cfg.get("countries", {}).items():
            for a in v.get("aliases", []) + [country]:
                self.country[_norm_text(a)] = country
        # Longest aliases first, so "jakarta selatan" wins over "jakarta" and "padang panjang" over "padang".
        self._long = sorted(((a, k) for k, d in (("city", self.city), ("province", self.province), ("country", self.country))
                             for a in d if len(a) >= 4), key=lambda x: -len(x[0]))

    def _lookup(self, alias: str, kind: str):
        return {"city": self.city, "province": self.province, "country": self.country}[kind].get(alias)

    def match(self, raw) -> dict:
        """{'city','province','island_region','country','world_region','matched'} from one answer."""
        out = {"city": None, "province": None, "island_region": None, "country": None, "world_region": None, "matched": False}
        text = _norm_text(raw)
        if not text:
            return out
        # Every alias found, as a whole comma part or (4+ letters) inside the text; per kind the longest
        # wins, so "Kuningan, Jakarta Selatan" is Jakarta, and short codes like "UK" or "DIY" only match whole.
        found: dict[str, tuple[int, object]] = {}
        parts = [text] + [_norm_text(x) for x in re.split(r"[,/;()\-]|\bdan\b|\band\b", str(raw))]
        for part in parts:
            for kind in ("city", "province", "country"):
                v = self._lookup(part, kind)
                if v and len(part) > found.get(kind, (-1, None))[0]:
                    found[kind] = (len(part), v)
        padded = f" {text} "
        for alias, kind in self._long:
            if f" {alias} " in padded and len(alias) > found.get(kind, (-1, None))[0]:
                found[kind] = (len(alias), self._lookup(alias, kind))
        hits = {kind: v for kind, (_, v) in found.items()}
        # A city must agree with the province named next to it.
        if "city" in hits and "province" in hits and hits["city"][1] != hits["province"] and found["province"][0] > found["city"][0]:
            del hits["city"]
        if "city" in hits:
            out["city"], out["province"] = hits["city"]
        elif "province" in hits:
            out["province"] = hits["province"]
        if out["province"]:
            out["island_region"], out["country"] = self.island.get(out["province"]), "Indonesia"
        elif "country" in hits:
            out["country"] = hits["country"]
        out["world_region"] = self.world.get(out["country"]) if out["country"] else None
        out["matched"] = bool(out["country"])
        return out

    def country_of(self, raw) -> str | None:
        return self.match(raw)["country"]


LINKEDIN_RE = re.compile(r"(?:https?://)?(?:[a-z]{2,3}\.)?linkedin\.com/(in|pub|profile)/([^?#\s]+)", re.I)


def linkedin_url(raw) -> str | None:
    """A LinkedIn profile URL from what people typed (full URL, linkedin.com/in/x, or a bare handle)."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in {"-", "n/a", "na", "none", "nan", "tidak ada", "belum ada", "0"}:
        return None
    m = LINKEDIN_RE.search(s)
    if m:
        kind, rest = m.group(1).lower(), m.group(2).strip("/")
        return f"https://www.linkedin.com/{kind}/{rest.split('/')[0] if kind == 'in' else rest}"
    handle = s.lstrip("@").strip("/")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9\-_%.]{2,99}", handle) and "." not in handle:
        return f"https://www.linkedin.com/in/{handle}"
    return None


def web_url(raw) -> str | None:
    """A plain http(s) link (research profile, website); anything else is dropped."""
    if raw is None:
        return None
    s = str(raw).strip().split()[0] if str(raw).strip() else ""
    if not s:
        return None
    if not re.match(r"https?://", s, re.I):
        if re.match(r"(www\.)?[a-z0-9-]+(\.[a-z0-9-]+)+(/\S*)?$", s, re.I):
            s = "https://" + s
        else:
            return None
    return s if re.match(r"https?://[a-z0-9-]+(\.[a-z0-9-]+)+(:\d+)?(/\S*)?$", s, re.I) else None


def clean_label(raw) -> str | None:
    """Tidy free text for display and grouping (institutions, areas of study)."""
    if raw is None:
        return None
    s = re.sub(r"\s+", " ", str(raw)).strip(" .,-")
    if not s or s.lower() in {"-", "n/a", "na", "none", "nan", "tidak ada", "belum", "0"}:
        return None
    return s if any(c.islower() for c in s) else s.title() if len(s) > 6 else s


# ---------------------------------------------------------------- pairings

def read_pairings(pair_sources: list[dict], tabs: dict, result: dict, records: list[P.Record]) -> tuple[list[dict], list[dict]]:
    """Mentor-mentee links -> (mentorships, inventory). Matches by email, then phone, then a unique exact name."""
    people = result["people"]
    by_email = {p["email_key"]: p["person_id"] for p in people}
    by_phone: dict[str, set] = defaultdict(set)
    by_name: dict[str, set] = defaultdict(set)
    labels = {p["person_id"]: set(p["labels"]) for p in people}
    for p in people:
        if p["phone"]:
            by_phone[p["phone"]].add(p["person_id"])
        for n in [p["full_name"], *p["name_variants"]]:
            if n:
                by_name[P.name_key(n)].add(p["person_id"])

    def find(email=None, phone=None, name=None, want: str = "") -> tuple[str | None, str]:
        if email:
            pid = by_email.get(P.norm_email(email))
            if pid:
                return pid, "email"
        if phone:
            ph, _ = P.norm_phone(phone)
            ids = by_phone.get(ph or "", set())
            if len(ids) == 1:
                return next(iter(ids)), "phone"
        if name:
            ids = by_name.get(P.name_key(P.clean_name(name)), set())
            # Prefer people who hold the role the pairing sheet says they have.
            with_role = {i for i in ids if want in labels.get(i, set())}
            ids = with_role or ids
            if len(ids) == 1:
                return next(iter(ids)), "name"
            if len(ids) > 1:
                return None, "ambiguous_name"
        return None, "not_found"

    links, inventory = [], []
    for s in pair_sources:
        entry = {"key": s["key"], "title": s.get("title", s["key"]), "program": s["program"], "year": s.get("year"),
                 "spreadsheet_id": s.get("spreadsheet_id"), "tab": s.get("tab")}
        data = tabs.get(s["key"])
        if isinstance(data, Exception) or data is None:
            entry.update({"status": "unreachable", "error": str(data) if data else "not read", "rows": 0, "links": 0})
            inventory.append(entry)
            continue
        hr = int(s.get("header_row", 1))
        idx = P._header_index([str(h) for h in data[hr - 1]]) if len(data) >= hr else {}
        lookups = {}
        for side in ("mentor", "mentee"):
            lk = (s.get("lookups") or {}).get(side)
            rows = tabs.get(f"{s['key']}__{side}")
            if lk and isinstance(rows, list) and rows:
                li = P._header_index([str(h) for h in rows[0]])
                lookups[side] = {str(P._pick(r, li, [lk["id"]]) or "").strip(): {
                    "name": P._pick(r, li, [lk["name"]]), "phone": P._pick(r, li, [lk["phone"]]) if lk.get("phone") else None,
                    "email": P._pick(r, li, [lk["email"]]) if lk.get("email") else None} for r in rows[1:]}
        want = {"mentor": f"{s['program']}_MENTOR", "mentee": f"{s['program']}_MENTEE"}
        stats = Counter()
        n_rows = 0
        for row in data[hr:]:
            if not any(str(v).strip() for v in row):
                continue
            n_rows += 1
            ends = {}
            for side in ("mentor", "mentee"):
                c = s.get(f"{side}_columns", {})
                email = P._pick(row, idx, c.get("email", []))
                name = P._pick(row, idx, c.get("name", []))
                phone = None
                ident = P._pick(row, idx, c.get("id", []))
                if ident is not None and side in lookups:
                    hit = lookups[side].get(str(ident).strip(), {})
                    email, name, phone = email or hit.get("email"), name or hit.get("name"), hit.get("phone")
                ends[side] = find(email, phone, name, want[side])
            (mentor, how_a), (mentee, how_b) = ends["mentor"], ends["mentee"]
            if mentor and mentee and mentor != mentee:
                match = "email" if how_a == how_b == "email" else ", ".join(sorted({how_a, how_b}))
                links.append({"mentor_id": mentor, "mentee_id": mentee, "program": s["program"], "year": s.get("year"),
                              "group": P._pick(row, idx, [s["group_column"]]) if s.get("group_column") else None,
                              "source": s["key"], "match": match})
                stats["linked"] += 1
            else:
                stats[f"mentor_{how_a}" if not mentor else f"mentee_{how_b}" if not mentee else "same_person"] += 1
        entry.update({"status": "read", "rows": n_rows, "links": stats.pop("linked", 0), "unresolved": dict(stats)})
        inventory.append(entry)
    # One link per mentor, mentee and program year.
    seen, unique = set(), []
    for link in links:
        k = (link["mentor_id"], link["mentee_id"], link["program"], link["year"])
        if k not in seen:
            seen.add(k)
            unique.append(link)
    for i, link in enumerate(unique):
        link["mentorship_id"] = "IMM-" + hashlib.sha256("|".join(map(str, (link["mentor_id"], link["mentee_id"], link["program"],
                                                                            link["year"]))).encode()).hexdigest()[:10].upper()
    return unique, inventory


# ---------------------------------------------------------------- gold profiles

def _latest(recs: list[P.Record], key: str):
    for r in sorted(recs, key=lambda r: (r.year or 0, r.sheet_row), reverse=True):
        v = r.attrs.get(key)
        if v is not None and str(v).strip():
            return v
    return None


def event_key(name) -> str:
    return re.sub(r"\s+", " ", str(name or "")).strip().lower()


def build(records: list[P.Record], result: dict, mentorships: list[dict], places: Places) -> dict:
    """-> {'gold': [...], 'events': [...], 'nodes': [...], 'edges': [...]}"""
    recs_by_pid: dict[str, list[P.Record]] = defaultdict(list)
    for r in records:
        if not r.excluded and r.email:
            recs_by_pid[P.person_id(r.email)].append(r)

    # Events: one per normalized name and year.
    events: dict[tuple, dict] = {}
    attended: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for pid, recs in recs_by_pid.items():
        for r in recs:
            if r.label.startswith("EVENT_") and r.event:
                k = (event_key(r.event), r.year)
                if k not in events:
                    events[k] = {"event_id": "IME-" + hashlib.sha256(f"{k[0]}|{k[1]}".encode()).hexdigest()[:10].upper(),
                                 "event_name": re.sub(r"\s+", " ", str(r.event)).strip(), "event_year": r.year,
                                 "participants": set(), "speakers": set(), "committee": set()}
                role = {"EVENT_PARTICIPANT": "participants", "EVENT_SPEAKER": "speakers", "EVENT_COMMITTEE": "committee"}[r.label]
                events[k][role].add(pid)
                attended[pid][role].add(events[k]["event_id"])

    mentees_of: dict[str, set] = defaultdict(set)
    mentors_of: dict[str, set] = defaultdict(set)
    for m in mentorships:
        mentees_of[m["mentor_id"]].add(m["mentee_id"])
        mentors_of[m["mentee_id"]].add(m["mentor_id"])

    review_of: dict[str, list[str]] = defaultdict(list)
    for rv in result.get("review", []):
        for rp in rv["person_ids"]:
            review_of[rp].append(f"{rv['review_id']} {rv['type']}")
    gold = []
    for p in result["people"]:
        pid = p["person_id"]
        recs = recs_by_pid.get(pid, [])
        a = lambda k: _latest(recs, k)  # noqa: E731
        # Residence: all three answers from the newest record that has one, so an old province
        # answer never overrides a newer "London, UK".
        res = next((r.attrs for r in sorted(recs, key=lambda r: (r.year or 0, r.sheet_row), reverse=True)
                    if any(r.attrs.get(f) for f in RESIDENCE_FIELDS)), {})
        home = places.match(res.get("residence_city"))
        if not home["province"] and home["country"] in (None, "Indonesia"):
            prov = places.match(res.get("residence_province"))
            if prov["province"]:
                home = {**prov, "city": home["city"]}
        if not home["country"]:
            home = places.match(res.get("residence_country"))
        residence_raw = ", ".join(str(res[f]).strip() for f in RESIDENCE_FIELDS if res.get(f))
        birth = places.match(a("birth_place"))
        origin_country = places.country_of(a("origin_country")) or birth["country"]
        mentor_country = places.country_of(a("doctoral_country")) or places.country_of(a("master_country")) \
            or places.country_of(a("institution_country"))
        target_country = places.country_of(a("target_country"))
        institution = clean_label(a("doctoral_institution") or a("master_institution") or a("institution") or a("bachelor_institution"))
        labels = set(p["labels"])
        ev = attended.get(pid, {})
        primary = next((r for r in ROLE_ORDER if r in labels), None)
        selected = sorted({r.label for r in recs if r.status in {"SELECTED", "ACTIVE"}})
        gold.append({
            "person_id": pid, "full_name": p["full_name"], "email": p["email"], "phone": p["phone"], "linkedin": p["linkedin"],
            "primary_role": primary, "roles": ", ".join(sorted(labels)), "n_roles": len(labels), "selected_roles": ", ".join(selected),
            **{f"is_{lbl.lower()}": lbl in labels for lbl in ROLE_ORDER},
            "first_year": p["first_year"], "last_year": p["last_year"],
            # Years with IM: distinct years with any role; span counts from the first to the last year.
            "years_with_im": len(p["years"]), "years_span": (p["last_year"] - p["first_year"] + 1) if p["first_year"] else 0,
            "linkedin_url": linkedin_url(p["linkedin"]), "profile_url": web_url(a("profile_url")),
            "email_variants": 1 + len(p["raw_emails"]), "review_ids": ", ".join(review_of.get(pid, [])),
            "residence_raw": residence_raw or None,
            "residence_city": home["city"], "residence_province": home["province"], "residence_island_region": home["island_region"],
            "residence_country": home["country"],
            "birth_city": birth["city"], "birth_province": birth["province"], "birth_island_region": birth["island_region"],
            "birth_country": birth["country"], "origin_country": origin_country,
            "study_country": mentor_country, "study_world_region": places.world.get(mentor_country) if mentor_country else None,
            "target_country": target_country, "target_university": clean_label(a("target_university")),
            "institution": institution, "area_of_study": clean_label(a("area_of_study")),
            "occupation_institution": clean_label(a("occupation_institution")),
            "events_attended": len(ev.get("participants", set())), "events_spoken": len(ev.get("speakers", set())),
            "events_committee": len(ev.get("committee", set())),
            "mentees_mentored": len(mentees_of.get(pid, set())), "mentors_had": len(mentors_of.get(pid, set())),
        })

    nodes, edges = graph(gold, result, list(events.values()), mentorships)
    return {"gold": gold, "events": [{**e, "participants": len(e["participants"]), "speakers": len(e["speakers"]),
                                       "committee": len(e["committee"]), "_ids": e} for e in events.values()],
            "nodes": nodes, "edges": edges}


def _nid(kind: str, value) -> str:
    return f"{kind}:{value}"


def graph(gold: list[dict], result: dict, events: list[dict], mentorships: list[dict]) -> tuple[list[dict], list[dict]]:
    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def node(nid, kind, label, **extra):
        if nid not in nodes:
            nodes[nid] = {"node_id": nid, "node_type": kind, "label": label, **extra}
        return nid

    def edge(src, dst, kind, year=None, **extra):
        edges.append({"src": src, "dst": dst, "edge_type": kind, "year": year, **extra})

    for g in gold:
        node(g["person_id"], "person", g["full_name"] or g["email"], primary_role=g["primary_role"])
    for a in result["activities"]:
        edge(a["person_id"], node(_nid("role", a["label"]), "role", a["label"]), "HAS_ROLE", a["activity_year"], status=a["status"])
    for e in events:
        eid = node(e["event_id"], "event", e["event_name"], year=e["event_year"])
        for role, kind in (("participants", "ATTENDED"), ("speakers", "SPOKE_AT"), ("committee", "ORGANIZED")):
            for pid in sorted(e[role]):
                edge(pid, eid, kind, e["event_year"])
    for m in mentorships:
        edge(m["mentor_id"], m["mentee_id"], "MENTORED", m["year"], program=m["program"], match=m["match"])
    for g in gold:
        pid = g["person_id"]
        if g["residence_country"]:
            country = node(_nid("country", g["residence_country"]), "country", g["residence_country"])
            place = country
            if g["residence_province"]:
                prov = node(_nid("province", g["residence_province"]), "province", g["residence_province"],
                            island_region=g["residence_island_region"])
                place = prov
                if g["residence_city"]:
                    place = node(_nid("city", g["residence_city"]), "city", g["residence_city"], province=g["residence_province"])
            edge(pid, place, "LIVES_IN")
        if g["birth_province"]:
            place = node(_nid("province", g["birth_province"]), "province", g["birth_province"])
            if g["birth_city"]:
                place = node(_nid("city", g["birth_city"]), "city", g["birth_city"], province=g["birth_province"])
            edge(pid, place, "BORN_IN")
        if g["study_country"]:
            edge(pid, node(_nid("country", g["study_country"]), "country", g["study_country"]), "STUDIED_IN")
        if g["target_country"]:
            edge(pid, node(_nid("country", g["target_country"]), "country", g["target_country"]), "TARGETS")
        if g["institution"]:
            edge(pid, node(_nid("institution", g["institution"].lower()), "institution", g["institution"]), "STUDIED_AT")
    # Place hierarchy: city -> province -> country.
    for n in list(nodes.values()):
        if n["node_type"] == "city":
            edges.append({"src": n["node_id"], "dst": _nid("province", n["province"]), "edge_type": "IN", "year": None})
        if n["node_type"] == "province":
            node(_nid("country", "Indonesia"), "country", "Indonesia")
            edges.append({"src": n["node_id"], "dst": _nid("country", "Indonesia"), "edge_type": "IN", "year": None})
    seen, unique = set(), []
    for e in edges:
        k = (e["src"], e["dst"], e["edge_type"], e.get("year"))
        if k not in seen:
            seen.add(k)
            unique.append(e)
    return list(nodes.values()), unique


# ---------------------------------------------------------------- site aggregates (counts only)

def _suppress(n: int, k: int):
    return None if 0 < n < k else n


def _dist(values: list, k: int, top: int = 12) -> dict:
    """Counts of non-empty values; values under k fold into Other, which is hidden when it is itself under k."""
    c = Counter(v for v in values if v not in (None, ""))
    shown = [(v, n) for v, n in c.most_common() if n >= k][:top]
    rest = sum(c.values()) - sum(n for _, n in shown)
    out = [{"value": str(v), "count": n} for v, n in shown]
    if rest:
        out.append({"value": "Other", "count": _suppress(rest, k), "folded": True})
    return {"known": _suppress(sum(c.values()), k), "values": out}


def _hist(nums: list[int], buckets, k: int) -> list[dict]:
    return [{"value": label, "count": _suppress(sum(1 for n in nums if lo <= n <= hi), k)} for lo, hi, label in buckets]


def site_aggregates(built: dict, mentorships: list[dict], pair_inventory: list[dict], k: int) -> dict:
    gold = built["gold"]
    roles = ["ALL"] + [r for r in ROLE_ORDER if any(g[f"is_{r.lower()}"] for g in gold)]
    regions = ["ALL"] + sorted({g["residence_island_region"] for g in gold if g["residence_island_region"]}) + ["Abroad"]

    def in_region(g, reg):
        return reg == "ALL" or g["residence_island_region"] == reg or (reg == "Abroad" and g["residence_country"]
                                                                        and g["residence_country"] != "Indonesia")

    slices = {}
    for role in roles:
        for reg in regions:
            rows = [g for g in gold if (role == "ALL" or g[f"is_{role.lower()}"]) and in_region(g, reg)]
            if len(rows) < k:
                slices[f"{role}|{reg}"] = {"people": _suppress(len(rows), k)}
                continue
            mentors = [g for g in rows if g["mentees_mentored"] > 0]
            slices[f"{role}|{reg}"] = {
                "people": len(rows),
                "event_goers": _suppress(sum(1 for g in rows if g["events_attended"]), k),
                "events_attended_total": sum(g["events_attended"] for g in rows) if len(rows) >= k else None,
                "mentors_with_mentees": _suppress(len(mentors), k),
                "mentees_total": sum(g["mentees_mentored"] for g in mentors) if len(mentors) >= k else None,
                "province": _dist([g["residence_province"] for g in rows], k),
                "city": _dist([g["residence_city"] for g in rows], k),
                "abroad_country": _dist([g["residence_country"] for g in rows if g["residence_country"] != "Indonesia"], k),
                "birth_province": _dist([g["birth_province"] for g in rows], k),
                "study_country": _dist([g["study_country"] for g in rows], k),
                "target_country": _dist([g["target_country"] for g in rows], k),
                "institution": _dist([g["institution"] for g in rows], k),
                "area_of_study": _dist([g["area_of_study"] for g in rows], k),
                "events_hist": _hist([g["events_attended"] for g in rows], EVENT_BUCKETS, k),
                "mentees_hist": _hist([g["mentees_mentored"] for g in mentors], MENTEE_BUCKETS, k),
                "n_roles": _hist([g["n_roles"] for g in rows], [(1, 1, "1 role"), (2, 2, "2 roles"), (3, 10**9, "3+ roles")], k),
            }

    top_events = sorted(built["events"], key=lambda e: -(e["participants"] + e["speakers"]))
    events = [{"event": e["event_name"], "year": e["event_year"], "participants": _suppress(e["participants"], k),
               "speakers": _suppress(e["speakers"], k)} for e in top_events if e["participants"] + e["speakers"] >= k][:20]
    by_prog = Counter((m["program"], m["year"]) for m in mentorships)
    by_match = Counter(m["match"] for m in mentorships)

    gold_n = len(gold)
    coverage = {f: _suppress(sum(1 for g in gold if g[f]), k) for f in
                ("residence_province", "residence_city", "residence_country", "birth_province", "study_country",
                 "target_country", "institution", "area_of_study")}
    return {
        "roles": roles, "regions": regions, "slices": slices, "events": events, "coverage": coverage, "people": gold_n,
        "mentorship": {"links": len(mentorships), "by_program": [{"program": p, "year": y, "pairs": _suppress(n, k)} for (p, y), n in sorted(by_prog.items(), key=lambda x: (x[0][0], x[0][1] or 0))],
                       "by_match": {m: _suppress(n, k) for m, n in by_match.items()},
                       "sources": pair_inventory},
        "graph": aggregate_graph(gold, built["events"], mentorships, k),
    }


def aggregate_graph(gold: list[dict], events: list[dict], mentorships: list[dict], k: int) -> dict:
    """A network of roles, regions, study countries and events; edge weight = people linking them (>= k only)."""
    nodes: dict[str, dict] = {}
    weights: Counter = Counter()

    def nid(kind, v):
        return f"{kind}:{v}"

    role_people = defaultdict(set)
    for g in gold:
        for r in ROLE_ORDER:
            if g[f"is_{r.lower()}"]:
                role_people[r].add(g["person_id"])
                if g["residence_island_region"]:
                    weights[(nid("role", r), nid("region", g["residence_island_region"]), "lives in")] += 1
                elif g["residence_country"] and g["residence_country"] != "Indonesia":
                    weights[(nid("role", r), nid("region", "Abroad"), "lives in")] += 1
                c = g["study_country"] if r.endswith("MENTOR") else g["target_country"] if r.endswith("MENTEE") else None
                if c and c != "Indonesia":
                    weights[(nid("role", r), nid("country", c), "studied in" if r.endswith("MENTOR") else "targets")] += 1
    for a in ROLE_ORDER:
        for b in ROLE_ORDER:
            if a < b:
                weights[(nid("role", a), nid("role", b), "also")] = len(role_people[a] & role_people[b])
    for e in sorted(events, key=lambda e: -e["participants"])[:8]:
        ids = e["_ids"]["participants"]
        for r in ("MASTER_MENTEE", "MASTER_MENTOR", "PHD_MENTEE", "PHD_MENTOR", "MANAGEMENT_MEMBER"):
            w = len(ids & role_people[r])
            if w:
                label = e["event_name"] if not e["event_year"] or str(e["event_year"]) in e["event_name"] else f"{e['event_name']} {e['event_year']}"
                weights[(nid("role", r), nid("event", label), "attended")] += w
    for m in mentorships:
        weights[(nid("role", f"{m['program']}_MENTOR"), nid("role", f"{m['program']}_MENTEE"), "mentored")] += 1

    edges = []
    for (a, b, kind), w in weights.items():
        if w >= k:
            edges.append({"source": a, "target": b, "kind": kind, "weight": w})
    for e in edges:
        for n in (e["source"], e["target"]):
            kind, _, label = n.partition(":")
            nodes.setdefault(n, {"id": n, "kind": kind, "label": label, "weight": 0})
            nodes[n]["weight"] += e["weight"]
    for n in nodes.values():
        if n["kind"] == "role":
            n["weight"] = len(role_people[n["label"]])
    return {"nodes": list(nodes.values()), "edges": edges}


# ---------------------------------------------------------------- directory (tree view, names)

DIRECTORY_FIELDS = ["name", "roles", "city", "province", "region", "country", "study_country", "target_country",
                    "institution", "first_year", "last_year", "events_attended", "events_spoken", "mentees_mentored", "mentors_had",
                    "birth_city", "birth_province", "birth_region", "origin_country", "years_with_im", "linkedin", "profile",
                    "email_variants"]


def directory(built: dict, mentorships: list[dict], review: list[dict] | None = None) -> dict:
    """Names for the site's tree view, only when config people.publish_directory is true.

    Deliberately minimal: no email, phone, LinkedIn, birth date or birth place. People are referred to
    by their position in the list, not by person_id.
    """
    gold = sorted(built["gold"], key=lambda g: (g["full_name"] or "").lower())
    index = {g["person_id"]: i for i, g in enumerate(gold)}
    people = [[g["full_name"] or "(no name)", [r for r in ROLE_ORDER if g[f"is_{r.lower()}"]], g["residence_city"],
               g["residence_province"], g["residence_island_region"], g["residence_country"], g["study_country"],
               g["target_country"], g["institution"], g["first_year"], g["last_year"], g["events_attended"],
               g["events_spoken"], g["mentees_mentored"], g["mentors_had"], g["birth_city"], g["birth_province"],
               g["birth_island_region"], g["origin_country"], g["years_with_im"], g["linkedin_url"], g["profile_url"],
               g["email_variants"]] for g in gold]
    pairs = [[index[m["mentor_id"]], index[m["mentee_id"]], m["program"], m["year"]] for m in mentorships
             if m["mentor_id"] in index and m["mentee_id"] in index]
    events = []
    for e in sorted(built["events"], key=lambda e: (-(e["event_year"] or 0), e["event_name"])):
        ids = e["_ids"]
        events.append([e["event_name"], e["event_year"], sorted(index[p] for p in ids["participants"] if p in index),
                       sorted(index[p] for p in ids["speakers"] if p in index), sorted(index[p] for p in ids["committee"] if p in index)])
    # Possible duplicates from the review queue: same phone, same name, or one email with different names.
    duplicates = [[r["type"], [index[p] for p in r["person_ids"] if p in index], r["detail"]] for r in (review or [])]
    duplicates = [d for d in duplicates if d[1]]
    return {"fields": DIRECTORY_FIELDS, "people": people, "mentorships": pairs, "events": events, "duplicates": duplicates,
            "event_fields": ["name", "year", "participants", "speakers", "committee"]}
