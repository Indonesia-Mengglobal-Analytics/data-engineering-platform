"""Run the people dataset job.

    python pipeline/people_build.py                            # Google, needs GOOGLE_SERVICE_ACCOUNT_JSON
    python pipeline/people_build.py --fixtures tests/fixtures/people --lakehouse build/lake

Env: SITE_PIN (12 digits), PREVIOUS_DIR (last published site/data, for run history).
Writes site/data/people.json (aggregates only, sealed) and refreshes the people passages in
site/data/knowledge.json when it exists. Rows go only to the lakehouse (people/*.parquet) and the
PEOPLE spreadsheet in the Shared Drive's outputs/ folder.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import yaml

sys.path.insert(0, str(Path(__file__).parent))

import knowledge  # noqa: E402
import lakehouse  # noqa: E402
import people as P  # noqa: E402
import people_gold as G  # noqa: E402
from exports import GoogleSheets, LocalSheets, table  # noqa: E402
from seal import Sealer, unseal  # noqa: E402
from sources import GoogleSource  # noqa: E402
from storage import DriveStorage, LocalStorage  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
TASK_KEYS = ("read", "normalize", "resolve", "gold", "build", "store", "publish")
K = 5  # small-group rule for people counts on the site
MAX_RUNS = 50


def read_google(google: GoogleSource, sources: list[dict]) -> dict:
    """Every configured tab. A tab that still fails after retries is recorded as unreachable, with the
    reason, and the run goes on with the others (the run then shows as partial)."""
    from googleapiclient.errors import HttpError

    def get(spreadsheet_id: str, rng: str) -> list:
        # num_retries retries timeouts, connection errors, 429 and 5xx with exponential backoff.
        return google.sheets.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=rng, valueRenderOption="FORMATTED_VALUE").execute(num_retries=RETRIES).get("values", [])

    tabs: dict = {}
    cache: dict = {}
    for s in sources:
        try:
            values = get(s["spreadsheet_id"], f"'{s['tab']}'")
            if s.get("header_from_column_metadata"):
                key = s["spreadsheet_id"]
                if key not in cache:
                    cache[key] = get(key, "'COLUMN_METADATA'")
                values = rename_from_metadata(values, cache[key])
            tabs[s["key"]] = values
        except HttpError as error:
            status = error.resp.status
            tabs[s["key"]] = Exception(f"no access: share with {google.account}" if status in (403, 404) else f"HTTP {status}")
        except (TimeoutError, OSError) as error:
            tabs[s["key"]] = Exception(f"Google did not answer after {RETRIES + 1} tries ({type(error).__name__}); retried next run")
    return tabs


RETRIES = 4


def rename_from_metadata(values: list[list], meta: list[list]) -> list[list]:
    """DATA tabs keep form questions as headers; COLUMN_METADATA gives their snake_case names."""
    if not values or not meta:
        return values
    head = [str(h).strip() for h in meta[0]]
    if "column_name" not in head:
        return values
    i = head.index("column_name")
    names = [r[i] for r in meta[1:] if len(r) > i and r[i]]
    header = list(values[0])
    for j, n in enumerate(names[:len(header)]):
        header[j] = n
    return [header] + values[1:]


def read_fixtures(folder: Path, sources: list[dict]) -> dict:
    tabs: dict = {}
    books = {}
    for p in folder.glob("*.json"):
        b = json.loads(p.read_text())
        books[b["spreadsheet_id"]] = b
    for s in sources:
        b = books.get(s.get("spreadsheet_id"))
        if not b or s["tab"] not in b["tabs"]:
            tabs[s["key"]] = Exception("no access: not in fixtures")
            continue
        values = b["tabs"][s["tab"]]
        if s.get("header_from_column_metadata") and "COLUMN_METADATA" in b["tabs"]:
            values = rename_from_metadata(values, b["tabs"]["COLUMN_METADATA"])
        tabs[s["key"]] = values
    return tabs


def suppress(n: int | None) -> int | None:
    return None if n is None or 0 < n < K else n


def aggregates(records, result, inventory, labels_cfg, config) -> dict:
    people, acts, review = result["people"], result["activities"], result["review"]
    kept = [r for r in records if not r.excluded]
    by_label = defaultdict(lambda: {"people": set(), "activities": 0, "status": Counter()})
    for a in acts:
        d = by_label[a["label"]]
        d["people"].add(a["person_id"])
        d["activities"] += 1
        d["status"][a["status"]] += 1
    labels = sorted(by_label)
    label_people = {lbl: by_label[lbl]["people"] for lbl in labels}
    overlap = [[suppress(len(label_people[a] & label_people[b])) for b in labels] for a in labels]
    combos = Counter(" + ".join(p["labels"]) for p in people)
    years = defaultdict(lambda: defaultdict(set))
    for a in acts:
        if a["activity_year"]:
            years[a["activity_year"]][labels_cfg.get(a["label"], {}).get("domain", a["label"])].add(a["person_id"])
    # Journeys across roles, by first year in each.
    first = defaultdict(dict)
    for a in acts:
        if a["activity_year"]:
            f = first[a["person_id"]]
            f[a["label"]] = min(f.get(a["label"], 9999), a["activity_year"])
    def journey(a, b):
        return sum(1 for f in first.values() if a in f and b in f and f[a] <= f[b])
    journeys = [
        ("Master mentee → Master mentor", "MASTER_MENTEE", "MASTER_MENTOR"),
        ("PhD mentee → PhD mentor", "PHD_MENTEE", "PHD_MENTOR"),
        ("Master mentee → PhD mentee", "MASTER_MENTEE", "PHD_MENTEE"),
        ("Mentee → management", "MASTER_MENTEE", "MANAGEMENT_MEMBER"),
        ("Event participant → mentee", "EVENT_PARTICIPANT", "MASTER_MENTEE"),
        ("Mentor → event speaker", "MASTER_MENTOR", "EVENT_SPEAKER"),
        ("Management candidate → member", "MANAGEMENT_CANDIDATE", "MANAGEMENT_MEMBER"),
    ]
    anomalies = defaultdict(lambda: {"count": 0, "by_source": Counter()})
    for r in records:
        for a in set(r.anomalies):
            anomalies[a]["count"] += 1
            anomalies[a]["by_source"][r.source] += 1
    return {
        "kpis": {
            "sources_configured": len(inventory), "sources_read": sum(1 for s in inventory if s["status"] == "read"),
            "records": len(records), "excluded": len(records) - len(kept), "people": len(people), "activities": len(acts),
            "merged_records": len(kept) - len(people), "review_items": len(review),
            "with_phone": sum(1 for p in people if p["phone"]), "multi_role": sum(1 for p in people if len(p["labels"]) > 1),
        },
        "labels": [{"label": lbl, **{k: v for k, v in labels_cfg.get(lbl, {}).items()},
                    "people": suppress(len(by_label[lbl]["people"])), "activities": suppress(by_label[lbl]["activities"]),
                    "status": {k: suppress(v) for k, v in by_label[lbl]["status"].items()}} for lbl in labels],
        "overlap": {"labels": labels, "matrix": overlap},
        "combinations": [{"labels": c, "people": n} for c, n in combos.most_common(15) if n >= K],
        "years": [{"year": y, **{d: suppress(len(ids)) for d, ids in sorted(doms.items())}} for y, doms in sorted(years.items())],
        "journeys": [{"name": n, "from": a, "to": b, "people": suppress(journey(a, b))} for n, a, b in journeys],
        "anomalies": [{"rule": k, "severity": P.ANOMALY_INFO[k][0], "description": P.ANOMALY_INFO[k][1],
                       "action": P.ANOMALY_INFO[k][2], "count": v["count"], "by_source": dict(v["by_source"])}
                      for k, v in sorted(anomalies.items(), key=lambda kv: (["error", "warn", "info"].index(P.ANOMALY_INFO[kv[0]][0]), -kv[1]["count"]))],
        "review": dict(Counter(r["type"] for r in review)),
        "sources": [{k: v for k, v in s.items() if k != "header"} for s in inventory],
        "not_used": config.get("not_used", []),
        "rules": {"key": "normalized email (Gmail dots and +tags removed)", "min_group_size": K,
                  "no_auto_merge": ["same phone", "same name", "mentor and mentee in the same year"]},
    }


def write_tables(con, records, result, inventory) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS people")
    con.execute("""CREATE OR REPLACE TABLE people.records (source VARCHAR, sheet_row BIGINT, label VARCHAR, status VARCHAR,
                   year INTEGER, event VARCHAR, email_raw VARCHAR, email VARCHAR, full_name VARCHAR, phone VARCHAR,
                   anomalies VARCHAR, excluded BOOLEAN, person_id VARCHAR)""")
    con.executemany("INSERT INTO people.records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        [r.source, r.sheet_row, r.label, r.status, r.year, r.event, None if r.email_raw is None else str(r.email_raw),
         r.email, r.name, r.phone, ", ".join(sorted(set(r.anomalies))), r.excluded,
         None if r.excluded or not r.email else P.person_id(r.email)] for r in records])
    con.execute("""CREATE OR REPLACE TABLE people.people (person_id VARCHAR, full_name VARCHAR, email VARCHAR, email_key VARCHAR, other_emails VARCHAR,
                   phone VARCHAR, linkedin VARCHAR, years VARCHAR, first_year INTEGER, last_year INTEGER, labels VARCHAR,
                   n_records INTEGER, n_sources INTEGER, name_variants VARCHAR, review_ids VARCHAR)""")
    review_by_person = defaultdict(list)
    for rv in result["review"]:
        for pid in rv["person_ids"]:
            review_by_person[pid].append(rv["review_id"])
    con.executemany("INSERT INTO people.people VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", [
        [p["person_id"], p["full_name"], p["email"], p["email_key"], ", ".join(p["raw_emails"]), p["phone"], p["linkedin"],
         ", ".join(map(str, p["years"])), p["first_year"], p["last_year"], ", ".join(p["labels"]), p["n_records"],
         p["n_sources"], " | ".join(p["name_variants"]), ", ".join(review_by_person[p["person_id"]])] for p in result["people"]])
    con.execute("""CREATE OR REPLACE TABLE people.activities (activity_id VARCHAR, person_id VARCHAR, full_name VARCHAR,
                   email VARCHAR, activity_year INTEGER, label VARCHAR, domain VARCHAR, education_level VARCHAR, role VARCHAR,
                   status VARCHAR, event VARCHAR, sources VARCHAR)""")
    return_rows = []
    for a in result["activities"]:
        info = LABELS.get(a["label"], {})
        return_rows.append([a["activity_id"], a["person_id"], a["full_name"], a["email"], a["activity_year"], a["label"],
                            info.get("domain"), info.get("education_level"), info.get("role"), a["status"], a["event"],
                            ", ".join(sorted(a["sources"]))])
    con.executemany("INSERT INTO people.activities VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", return_rows)
    con.execute("""CREATE OR REPLACE TABLE people.anomalies AS
                   SELECT source, sheet_row, unnest(string_split(anomalies, ', ')) AS rule, label, year, email_raw, full_name, excluded
                   FROM people.records WHERE anomalies <> ''""")
    con.execute("""CREATE OR REPLACE TABLE people.review (review_id VARCHAR, type VARCHAR, person_ids VARCHAR, detail VARCHAR,
                   suggestion VARCHAR, decision VARCHAR)""")
    con.executemany("INSERT INTO people.review VALUES (?,?,?,?,?,'')", [
        [r["review_id"], r["type"], ", ".join(r["person_ids"]), r["detail"], r["suggestion"]] for r in result["review"]])
    con.execute("CREATE OR REPLACE TABLE people.sources (key VARCHAR, title VARCHAR, label VARCHAR, status VARCHAR, rows BIGINT, records BIGINT, excluded BIGINT, error VARCHAR)")
    con.executemany("INSERT INTO people.sources VALUES (?,?,?,?,?,?,?,?)", [
        [s["key"], s["title"], s["label"], s["status"], s.get("rows", 0), s.get("records", 0), s.get("excluded", 0), s.get("error")]
        for s in inventory])


def pairing_tabs(pairings: list[dict]) -> list[dict]:
    """The tabs the pairing sources need: the pairs, and the id lookup tabs (key__mentor, key__mentee)."""
    out = []
    for s in pairings:
        out.append({"key": s["key"], "spreadsheet_id": s["spreadsheet_id"], "tab": s["tab"]})
        for side, lk in (s.get("lookups") or {}).items():
            out.append({"key": f"{s['key']}__{side}", "spreadsheet_id": s["spreadsheet_id"], "tab": lk["tab"]})
    return out


def write_gold(con, built: dict, mentorships: list[dict]) -> None:
    gold = built["gold"]
    cols = list(gold[0].keys()) if gold else ["person_id"]
    types = {bool: "BOOLEAN", int: "INTEGER"}
    sample = gold[0] if gold else {}
    defs = ", ".join(f'"{c}" {types.get(type(sample.get(c)), "VARCHAR")}' for c in cols)
    con.execute(f"CREATE OR REPLACE TABLE people.gold_people ({defs})")
    if gold:
        con.executemany(f"INSERT INTO people.gold_people VALUES ({','.join('?' * len(cols))})", [[g[c] for c in cols] for g in gold])
    names = {g["person_id"]: (g["full_name"], g["email"]) for g in gold}
    con.execute("""CREATE OR REPLACE TABLE people.mentorships (mentorship_id VARCHAR, program VARCHAR, year INTEGER, mentor_id VARCHAR,
                   mentor_name VARCHAR, mentor_email VARCHAR, mentee_id VARCHAR, mentee_name VARCHAR, mentee_email VARCHAR,
                   "group" VARCHAR, match VARCHAR, source VARCHAR)""")
    con.executemany("INSERT INTO people.mentorships VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", [
        [m["mentorship_id"], m["program"], m["year"], m["mentor_id"], *names.get(m["mentor_id"], (None, None)),
         m["mentee_id"], *names.get(m["mentee_id"], (None, None)), m["group"], m["match"], m["source"]] for m in mentorships])
    con.execute("""CREATE OR REPLACE TABLE people.events (event_id VARCHAR, event_name VARCHAR, event_year INTEGER,
                   participants INTEGER, speakers INTEGER, committee INTEGER)""")
    con.executemany("INSERT INTO people.events VALUES (?,?,?,?,?,?)", [
        [e["event_id"], e["event_name"], e["event_year"], e["participants"], e["speakers"], e["committee"]] for e in built["events"]])
    con.execute("""CREATE OR REPLACE TABLE people.graph_nodes (node_id VARCHAR, node_type VARCHAR, label VARCHAR, properties VARCHAR)""")
    con.executemany("INSERT INTO people.graph_nodes VALUES (?,?,?,?)", [
        [n["node_id"], n["node_type"], n["label"], json.dumps({k: v for k, v in n.items() if k not in ("node_id", "node_type", "label")})]
        for n in built["nodes"]])
    con.execute("""CREATE OR REPLACE TABLE people.graph_edges (src VARCHAR, dst VARCHAR, edge_type VARCHAR, year INTEGER, properties VARCHAR)""")
    con.executemany("INSERT INTO people.graph_edges VALUES (?,?,?,?,?)", [
        [e["src"], e["dst"], e["edge_type"], e.get("year"), json.dumps({k: v for k, v in e.items() if k not in ("src", "dst", "edge_type", "year")})]
        for e in built["edges"]])


GOLD_TABLES = ("gold_people", "mentorships", "events", "graph_nodes", "graph_edges")
LABELS: dict = {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures", help="folder of spreadsheet JSON fixtures instead of Google")
    parser.add_argument("--out", default=str(ROOT / "site" / "data"))
    parser.add_argument("--lakehouse", help="local lakehouse folder when no Shared Drive is configured")
    args = parser.parse_args()

    platform = yaml.safe_load((ROOT / "config" / "platform.yml").read_text())
    config = yaml.safe_load((ROOT / "config" / "people.yml").read_text())["people"]
    LABELS.update(config["labels"])
    pin = os.environ.get("SITE_PIN", "").strip()
    sealer = Sealer(pin)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    previous_dir = os.environ.get("PREVIOUS_DIR")
    previous_runs = []
    if previous_dir and (Path(previous_dir) / "people.json").exists():
        try:
            previous_runs = unseal(json.loads((Path(previous_dir) / "people.json").read_text()), pin).get("runs", [])
        except Exception:
            print("warning: could not open previous people.json, starting fresh", file=sys.stderr)

    run = {"run_id": (f"{os.environ['GITHUB_RUN_ID']}.{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
                      if os.environ.get("GITHUB_RUN_ID") else f"local-{int(time.time())}-{os.getpid()}"),
           "run_number": os.environ.get("GITHUB_RUN_NUMBER"), "trigger": os.environ.get("GITHUB_EVENT_NAME", "local"),
           "actor": os.environ.get("GITHUB_ACTOR", os.environ.get("USER", "local")), "started_at": P.utcnow()}
    if os.environ.get("GITHUB_RUN_ID"):
        run["url"] = f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
    tasks, started, failed, payload, directory = [], time.monotonic(), None, None, None
    max_year = datetime.now(timezone.utc).year + 1
    min_year = int(config.get("min_year", 2015))

    def step(key, fn, *a):
        t0 = time.monotonic()
        item = {"key": key, "status": "running"}
        tasks.append(item)
        try:
            res = fn(*a)
            item["status"] = "succeeded"
            return res
        except Exception as error:
            item.update({"status": "failed", "detail": f"{type(error).__name__}: {error}"})
            raise
        finally:
            item["duration_ms"] = int((time.monotonic() - t0) * 1000)

    con = duckdb.connect()
    try:
        google = None
        raw_key = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
        if raw_key and not args.fixtures:
            google = GoogleSource(json.loads(raw_key))
        folder = platform.get("storage", {}).get("shared_drive_folder_id", "")
        if google and folder:
            storage = DriveStorage(google.drive, folder)
            writer = GoogleSheets(google.drive, google.sheets, storage)
        else:
            storage = LocalStorage(Path(args.lakehouse or ROOT / "build" / "lakehouse"))
            writer = LocalSheets(Path(args.lakehouse) / "sheets") if args.lakehouse else None
        run["storage"] = storage.kind

        def read():
            if args.fixtures:
                return read_fixtures(Path(args.fixtures), config["sources"] + pairing_tabs(config.get("pairings", [])))
            if google is None:
                raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is not set")
            return read_google(google, config["sources"] + pairing_tabs(config.get("pairings", [])))
        tabs = step("read", read)
        records, inventory = step("normalize", P.run_checks, config["sources"], tabs, config["columns"], min_year, max_year,
                                  config.get("attributes", {}))
        result = step("resolve", P.resolve, records)

        def gold():
            places = G.Places(yaml.safe_load((ROOT / "config" / "places.yml").read_text()))
            links, pair_inventory = G.read_pairings(config.get("pairings", []), tabs, result, records)
            return G.build(records, result, links, places), links, pair_inventory
        built, links, pair_inventory = step("gold", gold)
        payload = step("build", aggregates, records, result, inventory, config["labels"], config)
        payload["gold"] = G.site_aggregates(built, links, pair_inventory, K)
        directory = G.directory(built, links) if config.get("publish_directory") else None

        def store():
            write_tables(con, records, result, inventory)
            write_gold(con, built, links)
            sizes = {f"people.{t}": lakehouse.save(con, storage, "people", t)
                     for t in ("records", "people", "activities", "anomalies", "review", "sources") + GOLD_TABLES}
            link = None
            if writer is not None:
                tabs_out = {
                    "Gold": table(con, "SELECT * FROM people.gold_people ORDER BY full_name NULLS LAST"),
                    "Mentorships": table(con, "SELECT * FROM people.mentorships ORDER BY program, year, mentor_name, mentee_name"),
                    "Events": table(con, "SELECT * FROM people.events ORDER BY event_year DESC, participants DESC"),
                    "People": table(con, "SELECT * FROM people.people ORDER BY full_name NULLS LAST"),
                    "Activities": table(con, "SELECT * FROM people.activities ORDER BY person_id, activity_year, label"),
                    "Anomalies": table(con, "SELECT * FROM people.anomalies ORDER BY excluded DESC, rule, source, sheet_row"),
                    "Review": table(con, "SELECT * FROM people.review ORDER BY review_id"),
                    "Sources": table(con, "SELECT * FROM people.sources"),
                    "About": (["item", "value"], [["run_id", run["run_id"]], ["generated_at", run["started_at"]],
                              ["person key", "normalized email (Gmail dots and +tags removed); person_id is stable per email"],
                              ["not merged automatically", "same phone, same name, mentor and mentee in the same year: see Review"],
                              ["Review.decision", "note your decision here; to merge, fix the email in the source sheet (this tab is rewritten each run)"],
                              ["labels", "from [Data-020] glossary; MASTER_* mirror PHD_* until the glossary defines them"],
                              ["Gold", "one row per person: roles, where they live (city, province, island region), birth place, study and target country, institution, events attended, mentees mentored"],
                              ["Mentorships", "mentor-mentee pairs from the pairing sheets; match = how each side was found (email, phone, name)"],
                              ["graph", "people.graph_nodes and people.graph_edges in the lakehouse (SQL editor): ATTENDED, SPOKE_AT, MENTORED, LIVES_IN, BORN_IN, STUDIED_IN, TARGETS, STUDIED_AT, HAS_ROLE"]]),
                }
                link = writer.upsert("outputs", "PEOPLE", tabs_out)
            return {"bytes": sizes, "sheet": link, "kind": storage.kind}
        stored = step("store", store)
        payload["outputs"] = {"sheet": stored["sheet"], "storage": stored["kind"]}
        run["status"] = "succeeded" if all(s["status"] == "read" for s in inventory) else "partial"
        run["summary"] = {k: payload["kpis"][k] for k in ("records", "excluded", "people", "merged_records", "review_items", "sources_read")}
        run["summary"]["mentorships"] = len(links)
    except Exception as error:
        failed = error
        run.update({"status": "failed", "error": f"{type(error).__name__}: {error}"})
    finally:
        con.close()

    run["finished_at"] = P.utcnow()
    run["duration_s"] = round(time.monotonic() - started, 1)
    done = {t["key"] for t in tasks}
    run["tasks"] = tasks + [{"key": k, "status": "skipped"} for k in TASK_KEYS if k not in done and k != "publish"]
    run["tasks"].append({"key": "publish", "status": "succeeded" if payload else "skipped"})
    runs = ([run] + previous_runs)[:MAX_RUNS]
    if payload is None and previous_dir and (Path(previous_dir) / "people.json").exists():
        try:  # keep the last good aggregates, only the run history changes
            payload = unseal(json.loads((Path(previous_dir) / "people.json").read_text()), pin)
        except Exception:
            payload = None
    doc = {**(payload or {}), "generated_at": run["finished_at"], "runs": runs,
           "workflow": {"repository": platform["github"]["repository"], "file": "people.yml"}}
    (out / "people.json").write_text(json.dumps(sealer.seal(doc)))
    # Names for the tree view: a separate sealed file, written only when the run succeeded (a failed run
    # keeps the last one) and removed when publishing is switched off.
    dir_path = out / "people_directory.json"
    if not config.get("publish_directory"):
        dir_path.unlink(missing_ok=True)
    elif payload is not None and not failed and directory is not None:
        dir_path.write_text(json.dumps(sealer.seal({**directory, "generated_at": run["finished_at"]})))

    kb_path = out / "knowledge.json"
    if payload and kb_path.exists():
        try:
            kb = unseal(json.loads(kb_path.read_text()), pin)
            kb["passages"] = knowledge.with_people(kb["passages"], doc)
            kb_path.write_text(json.dumps(sealer.seal(kb)))
        except Exception as error:
            print(f"warning: knowledge base not updated: {error}", file=sys.stderr)
    print(json.dumps({k: run.get(k) for k in ("status", "summary", "duration_s", "error")}))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
