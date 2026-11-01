import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))

import people as P  # noqa: E402
from seal import unseal  # noqa: E402

PIN = "123456789012"


@pytest.mark.parametrize("raw, expected", [
    ("Rara.KINASIH+im@Gmail.com", "rarakinasih@gmail.com"),
    ("  Budi@UI.ac.id ", "budi@ui.ac.id"),
    ("mailto:siti@googlemail.com", "siti@gmail.com"),
    ("a.b+x@yahoo.com", "a.b+x@yahoo.com"),  # only Gmail ignores dots and +tags
])
def test_norm_email(raw, expected):
    assert P.norm_email(raw) == expected


@pytest.mark.parametrize("raw, phone, issues", [
    ("08110005123", "+628110005123", []),
    ("+620811007345", "+62811007345", []),
    ("+6281100005123 (WA only)", "+6281100005123", []),
    ("+821000005123 (wa) / 081100007345", "+821000005123", ["multiple_phones"]),
    ("#ERROR!", None, ["invalid_phone"]),
    ("+62811", None, ["invalid_phone"]),
    ("0000000000", "+620000000", ["placeholder_phone"]),
    ('""', None, []),
])
def test_norm_phone(raw, phone, issues):
    assert P.norm_phone(raw) == (phone, issues)


@pytest.mark.parametrize("email, rule", [
    ("test@example.com", "dummy_email"), ("asdf@gmail.com", "dummy_email"), ("aaaa@yahoo.com", "dummy_email"),
    ("x@mailinator.com", "dummy_email"), ("budi at gmail", "invalid_email"), (None, "missing_email"),
])
def test_email_anomalies(email, rule):
    assert P.email_anomalies(P.norm_email(email)) == [rule]


@pytest.mark.parametrize("name, rules", [
    ("Test Test", ["dummy_name"]), ("asdf", ["dummy_name"]), ("B", ["dummy_name"]), ("Budi Santoso", []),
    ("Siti Nur 2", ["name_contains_digits"]), ("dewi@gmail.com", ["name_contains_contact"]),
])
def test_name_anomalies(name, rules):
    assert P.name_anomalies(P.clean_name(name)) == rules


def test_person_id_is_stable_and_email_based():
    assert P.person_id("a@b.co") == P.person_id("a@b.co") != P.person_id("c@b.co")
    assert P.person_id("a@b.co").startswith("IMP-")


def _run(tmp_path, **env):
    lake, out = tmp_path / "lake", tmp_path / "out"
    res = subprocess.run([sys.executable, str(ROOT / "pipeline" / "people_build.py"), "--fixtures",
                          str(ROOT / "tests" / "fixtures" / "people"), "--lakehouse", str(lake), "--out", str(out)],
                         env={**{k: v for k, v in os.environ.items() if not k.startswith("GITHUB_")}, "SITE_PIN": PIN, **env},
                         capture_output=True, text=True)
    return res, lake, out


def test_people_job_end_to_end(tmp_path):
    import duckdb

    res, lake, out = _run(tmp_path)
    assert res.returncode == 0, res.stderr
    doc = unseal(json.loads((out / "people.json").read_text()), PIN)
    k = doc["kpis"]
    assert k["excluded"] == 5  # 3 dummy rows, 1 invalid email, 1 missing email
    assert k["people"] + k["merged_records"] + k["excluded"] == k["records"]
    assert doc["review"] == {"same_phone_similar_name": 1, "same_phone_different_name": 1,
                             "same_name_different_email": 1, "mentor_and_mentee_same_year": 1}
    statuses = {s["key"]: s["status"] for s in doc["sources"]}
    assert statuses["mentee_new_2025"] == "unreachable" and statuses["all_time_mentee"] == "read"
    assert next(s for s in doc["sources"] if s["key"] == "phd_bootcamp_participant")["dummy_columns"] == ["Dummy_Motivation"]

    # People counts under 5 are suppressed everywhere (anomaly counts are row counts and stay exact).
    small = [n for y in doc["years"] for k, n in y.items() if k != "year" and n is not None and n < 5]
    small += [lb["activities"] for lb in doc["labels"] if lb["activities"] is not None and 0 < lb["activities"] < 5]
    assert small == []

    # Nothing identifying reaches the site file.
    blob = json.dumps(doc)
    for leaked in ("@gmail.com", "Rara", "+62812", "Dimas"):
        assert leaked not in blob

    con = duckdb.connect()
    people = f"read_parquet('{lake}/lakehouse/people/people.parquet')"
    # A Gmail dot/uppercase variant is the same person; roles across programs land on one row.
    row = con.execute(f"SELECT email, email_key, labels FROM {people} WHERE full_name = 'Dimas Wijaya'").fetchone()
    assert row[0] == "dimas.wijaya3@gmail.com" and row[1] == "dimaswijaya3@gmail.com"
    assert {"MASTER_MENTEE", "MASTER_MENTOR", "EVENT_PARTICIPANT", "MANAGEMENT_MEMBER"} <= set(row[2].split(", "))
    # One person with two addresses is not merged: two people, one review item.
    two = con.execute(f"SELECT count(*), count(DISTINCT review_ids) FROM {people} WHERE full_name = 'Rara Kinasih'").fetchone()
    assert two == (2, 1)
    # Excluded rows never become people.
    assert con.execute(f"SELECT count(*) FROM {people} WHERE email IN ('test@example.com', 'asdf@gmail.com')").fetchone()[0] == 0
    # Activities: one per person, label, year and status.
    acts = f"read_parquet('{lake}/lakehouse/people/activities.parquet')"
    assert con.execute(f"SELECT count(*) - count(DISTINCT (person_id, label, activity_year, status)) FROM {acts}").fetchone()[0] == 0
    sheet = json.loads((lake / "sheets" / "outputs" / "PEOPLE.json").read_text())
    assert set(sheet["tabs"]) == {"Gold", "Mentorships", "Events", "People", "Activities", "Anomalies", "Review", "Sources", "About"}


def test_people_ids_are_stable_between_runs(tmp_path):
    import duckdb

    a, lake_a, _ = _run(tmp_path / "a")
    b, lake_b, _ = _run(tmp_path / "b")
    assert a.returncode == b.returncode == 0
    q = "SELECT list(person_id ORDER BY person_id) FROM read_parquet('{}/lakehouse/people/people.parquet')"
    con = duckdb.connect()
    assert con.execute(q.format(lake_a)).fetchone() == con.execute(q.format(lake_b)).fetchone()


def test_people_passages_replace_only_people_kind():
    import knowledge

    base = [{"id": "P1", "kind": "platform", "text": "x"}, {"id": "P2", "kind": "people", "text": "old"}]
    doc = {"kpis": {"sources_read": 1, "sources_configured": 2, "records": 10, "excluded": 1, "people": 8,
                    "activities": 9, "merged_records": 1, "multi_role": 2, "review_items": 0}, "labels": []}
    out = knowledge.with_people(base, doc)
    assert out[0] == base[0] and all(p["kind"] == "people" for p in out[1:]) and "old" not in json.dumps(out)
    assert len({p["id"] for p in out}) == len(out)


def test_people_tables_are_queryable(tmp_path):
    from test_pipeline import _query

    res, lake, _ = _run(tmp_path)
    assert res.returncode == 0, res.stderr
    ok = _query(lake, "SELECT label, count(*) AS n FROM people.activities GROUP BY 1 ORDER BY 1", "--label", "people")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    preview = next((lake / "sheets" / "previews").glob("*.json"))
    rows = json.loads(preview.read_text())["tabs"]["RESULT"]["rows"]
    assert ["MASTER_MENTEE"] in [r[:1] for r in rows]


# ---------------------------------------------------------------- people gold

@pytest.fixture(scope="module")
def places():
    import yaml
    import people_gold as G
    return G.Places(yaml.safe_load((ROOT / "config" / "places.yml").read_text()))


@pytest.mark.parametrize("raw, city, province, region, country", [
    ("Jakarta Selatan", "Jakarta Selatan", "DKI Jakarta", "Java", "Indonesia"),
    ("Kuningan, Jakarta Selatan", "Jakarta Selatan", "DKI Jakarta", "Java", "Indonesia"),
    ("Kota Bandung, Jawa Barat", "Bandung", "Jawa Barat", "Java", "Indonesia"),
    ("Jogja", None, "DI Yogyakarta", "Java", "Indonesia"),
    ("Kab. Sleman", "Sleman", "DI Yogyakarta", "Java", "Indonesia"),
    ("NTB", None, "Nusa Tenggara Barat", "Bali & Nusa Tenggara", "Indonesia"),
    ("Padang Panjang", "Padang Panjang", "Sumatera Barat", "Sumatra", "Indonesia"),
    ("London, UK", None, None, None, "United Kingdom"),
    ("Belanda", None, None, None, "Netherlands"),
    ("Amerika Serikat", None, None, None, "United States"),
    ("as soon as possible", None, None, None, None),
    ("", None, None, None, None),
])
def test_places(places, raw, city, province, region, country):
    m = places.match(raw)
    assert (m["city"], m["province"], m["island_region"], m["country"]) == (city, province, region, country)


@pytest.fixture(scope="module")
def gold_run(tmp_path_factory):
    import duckdb

    res, lake, out = _run(tmp_path_factory.mktemp("gold"))
    assert res.returncode == 0, res.stderr
    con = duckdb.connect()
    for t in ("gold_people", "mentorships", "graph_nodes", "graph_edges", "events"):
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{lake}/lakehouse/people/{t}.parquet')")
    return con, unseal(json.loads((out / "people.json").read_text()), PIN), lake


def test_gold_one_row_per_person_with_counts(gold_run):
    con, doc, _ = gold_run
    assert con.execute("SELECT count(*), count(DISTINCT person_id) FROM gold_people").fetchone() == (doc["kpis"]["people"],) * 2
    # Events attended equals the person's ATTENDED edges; mentees mentored equals MENTORED edges.
    bad = con.execute("""SELECT count(*) FROM gold_people g
        WHERE events_attended <> (SELECT count(*) FROM graph_edges e WHERE e.src = g.person_id AND edge_type = 'ATTENDED')
           OR mentees_mentored <> (SELECT count(DISTINCT dst) FROM graph_edges e WHERE e.src = g.person_id AND edge_type = 'MENTORED')""").fetchone()[0]
    assert bad == 0
    dimas = con.execute("SELECT is_master_mentee, is_master_mentor, events_attended, residence_province FROM gold_people WHERE full_name = 'Dimas Wijaya'").fetchone()
    assert dimas[:2] == (True, True) and dimas[2] >= 1
    # A newer "London, UK" answer is not overridden by an older province answer.
    assert con.execute("SELECT count(*) FROM gold_people WHERE residence_raw = 'London, UK' AND residence_country <> 'United Kingdom'").fetchone()[0] == 0


def test_pairings_resolve_by_email_phone_and_unique_name(gold_run):
    con, doc, _ = gold_run
    assert dict(con.execute("SELECT program, count(*) FROM mentorships GROUP BY 1").fetchall()) == {"PHD": 6, "MASTER": 15}
    # A capitalised email still matches; mentor ids resolve by phone, or by name when no phone.
    assert con.execute("SELECT count(*) FROM mentorships WHERE program = 'PHD' AND match = 'email'").fetchone()[0] == 6
    assert set(r[0] for r in con.execute("SELECT DISTINCT match FROM mentorships WHERE program = 'MASTER'").fetchall()) == {"name, phone", "name"}
    # "Dewi Lestari" belongs to two people: never guessed.
    master = next(s for s in doc["gold"]["mentorship"]["sources"] if s["key"] == "master_pairing")
    assert master["unresolved"] == {"mentee_ambiguous_name": 1, "mentee_not_found": 1}
    assert con.execute("SELECT count(*) FROM mentorships WHERE mentee_name = 'Dewi Lestari'").fetchone()[0] == 0


def test_graph_edges_point_at_nodes(gold_run):
    con, _, _ = gold_run
    dangling = con.execute("""SELECT count(*) FROM graph_edges e WHERE e.src NOT IN (SELECT node_id FROM graph_nodes)
                              OR e.dst NOT IN (SELECT node_id FROM graph_nodes)""").fetchone()[0]
    assert dangling == 0
    kinds = {r[0] for r in con.execute("SELECT DISTINCT edge_type FROM graph_edges").fetchall()}
    assert {"HAS_ROLE", "ATTENDED", "SPOKE_AT", "MENTORED", "LIVES_IN", "BORN_IN", "STUDIED_IN", "TARGETS", "IN"} <= kinds


def test_gold_site_aggregates_hide_small_groups(gold_run):
    _, doc, _ = gold_run
    g = doc["gold"]

    def counts(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("count", "people", "weight", "pairs", "participants", "speakers", "known") and isinstance(v, int):
                    yield v
                else:
                    yield from counts(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from counts(v)

    small = [n for n in counts({k: v for k, v in g.items() if k != "mentorship"}) if 0 < n < 5]
    assert small == []
    assert g["slices"]["ALL|ALL"]["province"]["values"][0]["value"] == "Jawa Barat"
    assert g["graph"]["edges"] and all(e["weight"] >= 5 for e in g["graph"]["edges"])
    blob = json.dumps(g)
    for leaked in ("@gmail.com", "Dimas", "+62812", "IMP-"):
        assert leaked not in blob
