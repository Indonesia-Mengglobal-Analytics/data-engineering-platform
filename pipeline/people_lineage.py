"""Lineage of the people dataset: every source tab to every place the data is shown, with counts.

Two levels, both published in people.json (no personal data):
  steps    nodes (sources, tables, outputs) and edges with the row counts of this run
  columns  for each people gold column: the rule that builds it and the source headers that feed it
"""

from __future__ import annotations

from collections import Counter

STEP_INFO = {
    "records": ("people.records", "table", 1, "One row per source row, with normalized email, phone, name and year, and the anomalies found."),
    "pair_rows": ("Pairing rows", "step", 1, "Rows of the pairing sheets; Master ids are looked up in the raw mentor and mentee tabs."),
    "anomalies": ("people.anomalies", "table", 2, "One row per anomaly: dummy or invalid email, dummy name, broken phone, year out of range…"),
    "people": ("people.people", "table", 2, "One row per person: records merged by normalized email (Gmail dots and +tags removed)."),
    "event_rows": ("Event attendance", "step", 2, "Event participant, speaker and committee records with the event name and year."),
    "activities": ("people.activities", "table", 3, "One row per person, role (glossary label), year and status."),
    "review": ("people.review", "table", 3, "Possible duplicates that are never merged automatically: same phone, same name, mentor and mentee in one year."),
    "events": ("people.events", "table", 3, "One row per event name and year, with participant, speaker and committee counts."),
    "mentorships": ("people.mentorships", "table", 3, "Mentor-mentee pairs: each side matched to a person by email, else a unique phone, else a unique exact name; program, year and match type."),
    "gold": ("people.gold_people", "table", 4, "People gold: one row per person with roles, places, origin, study, years with IM, events, mentees and links."),
    "detect": ("Anomaly detection", "step", 5, "Probability that each row is dummy data and that two people are one person, with the signals behind every score (people.records.dummy_probability, people.duplicate_candidates)."),
    "graph": ("people.graph_*", "table", 6, "Person-level graph: people, events, places, institutions and roles, linked by ATTENDED, MENTORED, LIVES_IN…"),
    "aggregates": ("Site aggregates", "step", 6, "Counts by role and region, groups under 5 hidden, sealed with the site PIN (people.json)."),
    "directory": ("People directory", "step", 6, "Names, roles, places, study, years, events, mentees, mentor pairs, possible duplicates and links for the tree (people_directory.json, sealed)."),
    "lakehouse": ("Lakehouse (Parquet)", "store", 7, "Every people.* table (records, people, activities, anomalies, review, events, mentorships, gold, graph) as people/*.parquet in the Shared Drive (or this run only, until the folder is set)."),
    "out_pages": ("People pages", "output", 8, "People gold, Dedup overview, Anomalies, Sources and Runs tabs on this site."),
    "out_tree": ("People tree", "output", 8, "The Tree tab: hierarchy down to names, search and filters."),
    "out_ask": ("Ask", "output", 8, "Knowledge passages built from the aggregates, for the Ask page."),
    "out_sheet": ("PEOPLE sheet", "output", 8, "Gold, Mentorships, Events, People, Activities, Anomalies, Review, Sources and About tabs in the Shared Drive."),
    "out_sql": ("SQL editor", "output", 8, "people.* tables queryable through the query workflow; results land in a preview sheet for 2 hours."),
}

# gold column -> (attributes or identity fields that feed it, rule)
COLUMN_RULES = {
    "full_name": (["full_name"], "Most frequent name across the person's records (dummy names ignored)."),
    "email": (["email"], "The address the person typed most; people are keyed by the normalized email."),
    "phone": (["phone"], "Newest valid phone, as +62…; placeholders and #ERROR! dropped."),
    "linkedin_url": (["linkedin"], "Newest LinkedIn answer, cleaned to https://www.linkedin.com/in/<handle>."),
    "profile_url": (["profile_url"], "Newest research-profile or website link that is a valid http(s) URL."),
    "roles": (["label"], "Glossary labels of every record: the source's label, or the event role / selected flag."),
    "primary_role": (["label"], "The highest role held: management, mentor, speaker, committee, mentee, applicant, participant."),
    "first_year": (["year"], "Earliest year of any record (batch, year, event date or form timestamp)."),
    "last_year": (["year"], "Latest year of any record."),
    "years_with_im": (["year"], "Number of distinct years with any role."),
    "residence_city": (["residence_city", "residence_province", "residence_country"], "Newest record with a residence answer, matched to a city with config/places.yml."),
    "residence_province": (["residence_city", "residence_province", "residence_country"], "Province of the matched city, or the province answer."),
    "residence_island_region": (["residence_city", "residence_province", "residence_country"], "Island region of the province (Java, Sumatra, …)."),
    "residence_country": (["residence_city", "residence_province", "residence_country"], "Indonesia when a province matched, else the country named."),
    "birth_city": (["birth_place"], "Place of birth matched to a city."),
    "birth_province": (["birth_place"], "Province of the place of birth."),
    "origin_country": (["origin_country", "birth_place"], "Country of origin answer, else the country of the place of birth."),
    "study_country": (["doctoral_country", "master_country"], "Country of the PhD, else of the master's degree."),
    "target_country": (["target_country"], "Where a mentee wants to study (first application)."),
    "target_university": (["target_university"], "University of the first application."),
    "institution": (["doctoral_institution", "master_institution", "bachelor_institution"], "Latest institution: PhD, else master's, else bachelor's."),
    "area_of_study": (["area_of_study"], "Newest area-of-study answer."),
    "events_attended": (["event"], "Distinct events (name and year) the person attended as a participant."),
    "events_spoken": (["event"], "Distinct events where the person was a speaker, panelist, moderator or keynote."),
    "mentees_mentored": (["pairing"], "Distinct mentees linked to the person in the pairing sheets."),
    "mentors_had": (["pairing"], "Distinct mentors linked to the person in the pairing sheets."),
    "email_variants": (["email"], "How many spellings of the email address were merged."),
}


def build(inventory: list[dict], pair_inventory: list[dict], records, result: dict, built: dict, links: list[dict],
          config: dict, outputs: dict) -> dict:
    kept = [r for r in records if not r.excluded]
    by_source = Counter(r.source for r in records)
    kept_by_source = Counter(r.source for r in kept)
    nodes, edges = [], []

    for s in inventory:
        nodes.append({"id": f"src:{s['key']}", "label": s["title"], "kind": "source", "col": 0, "status": s["status"],
                      "count": s.get("rows", 0), "unit": "rows", "group": s["label"],
                      "url": f"https://docs.google.com/spreadsheets/d/{s['spreadsheet_id']}" if s.get("spreadsheet_id") else None,
                      "desc": f"Google Sheet tab '{s.get('tab')}', read with the service account. Label {s['label']}."
                              + (" Archived: not read, so it is left out of people gold." if s["status"] == "archived"
                                 else f" Not read: {s.get('error')}." if s["status"] != "read" else ""),
                      "key": s["key"], "archived": s.get("archived"),
                      "columns": {k: v for k, v in (s.get("mapped_columns") or {}).items() if v}})
        if s["status"] == "read":
            edges.append({"from": f"src:{s['key']}", "to": "records", "count": by_source.get(s["key"], 0)})
    for s in pair_inventory:
        nodes.append({"id": f"pair:{s['key']}", "label": s["title"], "kind": "source", "col": 0, "status": s["status"],
                      "count": s.get("rows", 0), "unit": "pairs", "group": f"{s['program']}_PAIRING",
                      "url": f"https://docs.google.com/spreadsheets/d/{s['spreadsheet_id']}" if s.get("spreadsheet_id") else None,
                      "key": s["key"], "archived": s.get("archived"),
                      "desc": f"Pairing sheet tab '{s.get('tab')}' ({s['program'].title()} {s.get('year') or ''})."
                              + (" Archived: not read, so its pairs are left out." if s["status"] == "archived" else "")
                              + (f" Not linked: {', '.join(f'{k} {v}' for k, v in s['unresolved'].items())}." if s.get("unresolved") else ""),
                      "columns": {}})
        if s["status"] == "read":
            edges.append({"from": f"pair:{s['key']}", "to": "pair_rows", "count": s.get("rows", 0)})

    anomaly_rows = sum(len(set(r.anomalies)) for r in records)
    event_recs = sum(1 for r in kept if r.label.startswith("EVENT_") and r.event)
    counts = {
        "records": (len(records), "records"), "pair_rows": (sum(s.get("rows", 0) for s in pair_inventory), "rows"),
        "anomalies": (anomaly_rows, "anomalies"), "people": (len(result["people"]), "people"), "event_rows": (event_recs, "records"),
        "activities": (len(result["activities"]), "activities"),
        "review": (len(result["review"]), "items"), "events": (len(built["events"]), "events"),
        "mentorships": (len(links), "pairs"), "gold": (len(built["gold"]), "people"),
        "detect": (sum(1 for r in records if r.dummy_p >= 0.5), "rows ≥ 50%"),
        "graph": (len(built["nodes"]) + len(built["edges"]), "nodes + edges"), "aggregates": (None, ""),
        "directory": (len(built["gold"]) if config.get("publish_directory") else 0, "people"),
        "lakehouse": (None, ""), "out_pages": (None, ""), "out_tree": (None, ""), "out_ask": (None, ""),
        "out_sheet": (None, ""), "out_sql": (None, ""),
    }
    for key, (label, kind, col, desc) in STEP_INFO.items():
        n, unit = counts[key]
        node = {"id": key, "label": label, "kind": kind, "col": col, "count": n, "unit": unit, "desc": desc, "status": "ok"}
        if key == "lakehouse":
            node["desc"] += f" This run: {'Shared Drive' if outputs.get('storage') == 'drive' else 'kept for the run only'}."
        if key == "out_sheet":
            node["url"] = (outputs.get("sheet") or {}).get("url")
            if not node["url"]:
                node["status"] = "off"
                node["desc"] += " Not written yet: set storage.shared_drive_folder_id."
        if key in ("directory", "out_tree") and not config.get("publish_directory"):
            node["status"] = "off"
        if key == "gold":
            node["columns"] = {c: rule for c, (_, rule) in COLUMN_RULES.items()}
        nodes.append(node)

    def e(a, b, n=None):
        edges.append({"from": a, "to": b, "count": n})
    e("records", "anomalies", anomaly_rows)
    e("records", "people", len(kept))
    e("records", "event_rows", event_recs)
    e("people", "activities", len(result["activities"]))
    e("people", "review", len(result["review"]))
    e("event_rows", "events", len(built["events"]))
    e("pair_rows", "mentorships", counts["pair_rows"][0])
    e("people", "mentorships", len(links))
    for a in ("people", "activities", "events", "mentorships", "review"):
        e(a, "gold")
    for b in ("graph", "aggregates", "directory", "lakehouse"):
        e("gold", b)
    e("graph", "lakehouse")
    e("records", "detect", len(records))
    e("gold", "detect")
    for b in ("aggregates", "directory", "lakehouse"):
        e("detect", b)
    e("aggregates", "out_pages")
    e("aggregates", "out_ask")
    e("directory", "out_tree")
    e("lakehouse", "out_sheet")
    e("lakehouse", "out_sql")

    # Column lineage: gold column -> source headers that feed it.
    attr_sources: dict[str, list[dict]] = {}
    for s in inventory:
        for field, header in (s.get("mapped_columns") or {}).items():
            if header:
                attr_sources.setdefault(field, []).append({"source": s["key"], "title": s["title"], "header": header})
    for s in inventory:
        attr_sources.setdefault("label", []).append({"source": s["key"], "title": s["title"], "header": f"label {s['label']}"})
    for src in config.get("sources", []):
        if src.get("event_column"):
            attr_sources.setdefault("event", []).append({"source": src["key"], "title": src.get("title", src["key"]),
                                                         "header": f"{src['event_column']} / {src.get('role_column', '')}".strip(" /")})
    for s in pair_inventory:
        attr_sources.setdefault("pairing", []).append({"source": s["key"], "title": s["title"], "header": "mentor / mentee"})
    columns = {}
    for col, (fields, rule) in COLUMN_RULES.items():
        feeds = [dict(x, field=f) for f in fields for x in attr_sources.get(f, [])]
        columns[col] = {"rule": rule, "fields": fields, "feeds": feeds}
    return {"nodes": nodes, "edges": edges, "columns": columns}
