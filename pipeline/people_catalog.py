"""Catalog and governance metadata for the people.* tables, published in people.json (schema only, no values).

Built from the tables the run actually wrote (DESCRIBE + row counts), so the site's Catalog, Governance, SQL editor
and search always match the latest run.
"""

from __future__ import annotations

import re

from people_lineage import COLUMN_RULES

TABLES = {
    "gold_people": ("gold", "People gold: one row per person with roles, places, origin, study, management position, years with IM, events, mentees and links."),
    "people": ("silver", "One row per person: records merged by normalized email (Gmail dots and +tags removed)."),
    "activities": ("silver", "One row per person, role (glossary label), year and status."),
    "records": ("bronze", "One row per source row as read, with normalized fields, anomalies and the dummy-data probability."),
    "anomalies": ("silver", "One row per anomaly found in a source row (dummy, invalid email, broken phone…)."),
    "review": ("silver", "Possible duplicates that are never merged automatically."),
    "duplicate_candidates": ("gold", "Pairs of people that may be one person, with a probability and its signals."),
    "mentorships": ("gold", "Mentor-mentee pairs from the pairing sheets, with how each side was matched."),
    "events": ("gold", "One row per event name and year, with participant, speaker and committee counts."),
    "graph_nodes": ("gold", "People, events, places, institutions and roles as graph nodes."),
    "graph_edges": ("gold", "Links between nodes: ATTENDED, MENTORED, LIVES_IN, BORN_IN, STUDIED_IN, HAS_ROLE…"),
    "sources": ("bronze", "Every configured source tab with its status and row counts."),
}

GENERIC = {
    "person_id": "Stable id of a person: IMP- + hash of the normalized email.",
    "email": "The address the person typed most.",
    "email_key": "Normalized email used as the person key.",
    "email_raw": "Email exactly as typed in the source row.",
    "full_name": "Name in proper case (raw records keep it as typed).",
    "phone": "Phone as +62…, placeholders removed.",
    "source": "Source key from config/people.yml.",
    "sheet_row": "Row number in the source sheet.",
    "label": "Glossary label (role) of the record.",
    "status": "Lifecycle status (CANDIDATE, SELECTED, ACTIVE…).",
    "year": "Year of the record.",
    "activity_year": "Year of the activity.",
    "anomalies": "Anomaly rules that fired on the row.",
    "excluded": "True when an error-level anomaly keeps the row out of the dataset.",
    "dummy_probability": "Estimated probability that the row is dummy data (0–1).",
    "dummy_level": "likely (80%+), suspicious (50–79%) or clean.",
    "dummy_reasons": "Signals behind the dummy-data probability.",
    "probability": "Estimated probability that the two people are one person (0–1).",
    "reasons": "Signals behind the same-person probability, with weights.",
    "review_id": "Id in the review list.",
    "review_ids": "Review items this person appears in.",
    "rule": "Anomaly rule.",
    "mgmt_position": "Latest management position held.",
    "mgmt_division": "Division of that position.",
    "mgmt_level": "Leadership, Manager / lead or Team member.",
    "mgmt_history": "Management positions by year.",
    "applied_position": "Management position applied for.",
    "linkedin": "LinkedIn as typed (newest answer).",
    "n_roles": "Number of different roles held.",
    "selected_roles": "Roles in which the person was selected or active.",
    "occupation_institution": "Where the person works, as answered.",
    "events_committee": "Events the person helped organize.",
    "years_span": "Years from the first to the last activity, inclusive.",
    "residence_raw": "Residence answer as typed, before place matching.",
    "birth_island_region": "Island region of the place of birth.",
    "birth_country": "Country of the place of birth.",
    "study_world_region": "World region of the study country.",
    "other_emails": "Other spellings of the same email that were merged.",
    "name_variants": "Every spelling of the name found in the sources.",
    "n_records": "Source rows merged into this person.",
    "n_sources": "Source tabs this person appears in.",
    "first_year": "Earliest year of any record.",
    "last_year": "Latest year of any record.",
    "years": "Every year with a record.",
    "labels": "Glossary labels held.",
}

PERSONAL = re.compile(r"^(full_name|name_[ab]|mentor_name|mentee_name|email|email_raw|email_key|email_[ab]|mentor_email|mentee_email|"
                      r"other_emails|name_variants|phone|linkedin|linkedin_url|profile_url|residence_raw|label)$")
IDENTIFIER = re.compile(r"(_id|_ids)$|^(person_[ab]|src|dst|node_id)$")
FREE_TEXT = re.compile(r"^(dummy_reasons|reasons|detail|suggestion|mgmt_history|properties|anomalies|sources|decision)$")

# Columns of gold_people shown by name in the tree (people_directory.json) and as counts (people.json).
DIRECTORY = {"full_name", "primary_role", "roles", "residence_city", "residence_province", "residence_island_region", "residence_country",
             "birth_city", "birth_province", "birth_island_region", "origin_country", "study_country", "target_country", "institution",
             "first_year", "last_year", "years_with_im", "events_attended", "events_spoken", "mentees_mentored", "mentors_had",
             "linkedin_url", "profile_url", "email_variants", "mgmt_position", "mgmt_division", "mgmt_level", "mgmt_history",
             "applied_position", "area_of_study", "target_university"}
COUNTS = {"residence_province", "residence_city", "residence_island_region", "residence_country", "birth_province", "study_country",
          "target_country", "institution", "area_of_study", "events_attended", "mentees_mentored", "n_roles", "primary_role", "roles",
          "first_year", "last_year"}


def classify(table: str, column: str, dtype: str) -> str:
    if column == "label" and table != "gold_people":
        return "categorical"
    if PERSONAL.match(column):
        return "personal"
    if IDENTIFIER.search(column):
        return "identifier"
    if FREE_TEXT.match(column):
        return "free_text"
    if dtype.upper() in ("BOOLEAN", "INTEGER", "BIGINT", "DOUBLE", "FLOAT") or column.endswith(("_year", "year")):
        return "metadata"
    return "categorical"


def shown(table: str, column: str, publish_directory: bool) -> str:
    if table == "gold_people":
        if column == "email":
            return "tree (only for people with no name)" if publish_directory else "lakehouse and PEOPLE sheet"
        if publish_directory and column in DIRECTORY:
            return "tree (names directory)"
        if column in COUNTS or column.startswith("is_"):
            return "counts (groups under 5 hidden)"
    if table == "records" and column in ("dummy_probability", "dummy_level", "dummy_reasons"):
        return "counts; rows to check with masked email"
    if table == "duplicate_candidates" and publish_directory and column in ("probability", "reasons", "type"):
        return "tree and Anomalies tab"
    if table in ("events", "sources", "mentorships") and column in ("event_name", "event_year", "participants", "speakers", "key", "title",
                                                                   "status", "rows", "program", "year", "match"):
        return "counts and names of events / sources"
    return "lakehouse and PEOPLE sheet"


def build(con, publish_directory: bool) -> dict:
    tables = []
    for t, (layer, desc) in TABLES.items():
        try:
            cols = con.execute(f"DESCRIBE people.{t}").fetchall()
            rows = con.execute(f"SELECT count(*) FROM people.{t}").fetchone()[0]
        except Exception:
            continue
        columns = []
        for name, dtype, *_ in cols:
            columns.append({"name": name, "type": dtype.lower(),
                            "description": GENERIC.get(name) or (COLUMN_RULES.get(name, (None, ""))[1] if name in COLUMN_RULES else "")
                            or (f"True when the person holds the role {name[3:].upper()}." if name.startswith("is_") else ""),
                            "class": classify(t, name, dtype), "shown": shown(t, name, publish_directory)})
        tables.append({"name": t, "fqn": f"people.{t}", "layer": layer, "description": desc, "rows": rows, "columns": columns})
    classes = {}
    for t in tables:
        for c in t["columns"]:
            classes[c["class"]] = classes.get(c["class"], 0) + 1
    return {
        "tables": tables, "classes": classes,
        "governance": {
            "publish_directory": publish_directory,
            "files": [
                {"file": "people.json", "holds": "Counts, overlaps, journeys, anomaly and detection summaries, lineage, catalog schema",
                 "personal_data": "None: groups under 5 hidden"},
                {"file": "people_directory.json", "holds": "Names, roles, places, study, management positions, years, events, mentee counts, "
                 "LinkedIn/profile links people gave; full email only for people with no name; masked emails in the rows to check",
                 "personal_data": "Names and links" if publish_directory else "Not published (publish_directory: false)"},
                {"file": "knowledge.json", "holds": "Ask passages built from the counts", "personal_data": "None"},
            ],
            "never": ["phone", "date of birth", "email (except people with no name)", "free-text answers"],
        },
    }
