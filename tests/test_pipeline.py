import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))

from contract import SheetSnapshot, check  # noqa: E402
from governance import classify, suppress  # noqa: E402
from seal import Sealer, unseal  # noqa: E402

CONFIG = yaml.safe_load((ROOT / "config" / "platform.yml").read_text())
DEMO = json.loads((ROOT / "tests" / "fixtures" / "demo_phd_mentorship.json").read_text())
PIN = "123456789012"


def snapshot(tabs=None) -> SheetSnapshot:
    return SheetSnapshot(DEMO["spreadsheet_id"], DEMO["title"], None, copy.deepcopy(tabs or DEMO["tabs"]))


def test_demo_sheet_is_registered_despite_stray_header_spaces():
    result = check(snapshot(), CONFIG["contract"])
    assert result.errors == []
    assert result.registered
    assert len(result.columns) == 53


def test_missing_tab_is_rejected():
    tabs = copy.deepcopy(DEMO["tabs"])
    del tabs["COLUMN_METADATA"]
    result = check(snapshot(tabs), CONFIG["contract"])
    assert not result.registered
    assert "Missing tab(s): COLUMN_METADATA" in result.errors[0]


def test_header_that_drifted_from_dictionary_is_rejected():
    tabs = copy.deepcopy(DEMO["tabs"])
    tabs["DATA"][0][2] = "Nama Lengkap"
    result = check(snapshot(tabs), CONFIG["contract"])
    assert any(e.startswith("full_name:") for e in result.errors)


def test_extra_data_column_is_rejected():
    tabs = copy.deepcopy(DEMO["tabs"])
    tabs["DATA"][0].append("New question")
    result = check(snapshot(tabs), CONFIG["contract"])
    assert any("54 columns but COLUMN_METADATA documents 53" in e for e in result.errors)


def test_bad_dates_and_missing_owner_are_rejected():
    tabs = copy.deepcopy(DEMO["tabs"])
    tabs["TABLE_METADATA"][1][2] = ""
    tabs["TABLE_METADATA"][1][3] = "2026-07-06"
    errors = check(snapshot(tabs), CONFIG["contract"]).errors
    assert any("owner_email is empty" in e for e in errors)
    assert any("operation_date_start must be YYYYMMDD" in e for e in errors)


@pytest.mark.parametrize(
    "column, expected",
    [
        ("email_address", "personal"),
        ("whatsapp_phone_number", "personal"),
        ("date_of_birth", "personal"),
        ("legal_signature", "personal"),
        ("video_motivation_link", "personal"),
        ("special_assistance_details", "sensitive"),
        ("require_special_assistance_or_accommodation", "sensitive"),
        ("management_team_connection", "sensitive"),
        ("research_proposal_draft", "free_text"),
        ("bachelors_degree_url_of_institution", "free_text"),
        ("privacy_policy_consent", "metadata"),
        ("preferred_country_of_study", "categorical"),
        ("funding_plan_name_of_scholarship", "categorical"),
    ],
)
def test_classification(column, expected):
    assert classify(column, CONFIG["governance"]["rules"]) == expected


def test_suppress_folds_small_groups_and_hides_a_lone_remainder():
    values = suppress([("A", 10), ("B", 6), ("C", 2)], min_group=5, max_categories=12)
    assert values == [
        {"value": "A", "count": 10},
        {"value": "B", "count": 6},
        {"value": "Suppressed", "count": None, "folded": True},
    ]
    values = suppress([("A", 10), ("C", 3), ("D", 3)], min_group=5, max_categories=12)
    assert values[-1] == {"value": "Other (groups < 5)", "count": 6, "folded": True}


def test_seal_round_trip_and_wrong_pin_fails():
    envelope = Sealer(PIN).seal({"hello": "dunia"})
    assert "dunia" not in json.dumps(envelope)
    assert unseal(envelope, PIN) == {"hello": "dunia"}
    with pytest.raises(Exception):
        unseal(envelope, "000000000000")


def test_pin_must_be_12_digits():
    with pytest.raises(ValueError):
        Sealer("1234")


def run_build(out: Path, **env) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(ROOT / "pipeline" / "build.py"), "--fixtures", str(ROOT / "tests" / "fixtures"),
         "--out", str(out), "--warehouse", str(out / "w.duckdb")],
        env={**os.environ, "SITE_PIN": PIN, **env}, capture_output=True, text=True,
    )


def test_build_publishes_only_aggregates(tmp_path):
    result = run_build(tmp_path)
    assert result.returncode == 0, result.stderr
    data = {n: unseal(json.loads((tmp_path / f"{n}.json").read_text()), PIN)
            for n in ("catalog", "dashboard", "governance", "jobs")}

    statuses = {s["title"]: s["status"] for s in data["catalog"]["sources"]}
    assert statuses == {"Mentee_Data_PhD_Mentorship_2026 (demo)": "registered",
                        "Volunteer_Signups_2026 (demo, wrong format)": "rejected"}

    published = json.dumps(data, ensure_ascii=False)
    # No row-level personal value may appear anywhere in the output.
    for leaked in ("applicant1@example.com", "Demo Applicant 1", "+62 812"):
        assert leaked not in published

    dash = data["dashboard"]["sources"]["demo-phd-mentorship-2026"]
    src = next(s for s in data["catalog"]["sources"] if s["id"] == "demo-phd-mentorship-2026")
    sil = src["layers"]["silver"]
    # 148 rows read, 3 resubmissions superseded (latest wins), rows without privacy consent fail and stay out of gold.
    assert sil["rows_read"] == 148 and sil["duplicates_superseded"] == 3 and sil["current"] == 145
    assert sil["dq"]["fail"] > 0
    assert dash["kpis"]["records"] == sil["current"] - sil["dq"]["fail"] == src["layers"]["gold"]
    charted = {c["column"] for c in dash["charts"]}
    assert "email_address" not in charted and "require_special_assistance_or_accommodation" not in charted
    for chart in dash["charts"]:
        for value in chart["values"]:
            assert value["count"] is None or value["count"] >= 5

    assert data["jobs"]["runs"][0]["status"] == "partial"
    assert [t["key"] for t in data["jobs"]["runs"][0]["tasks"]] == ["discover", "validate", "bronze", "silver", "gold",
                                                                      "store", "govern", "export", "publish"]


def test_run_history_accumulates_and_single_source_refresh_keeps_others(tmp_path):
    first = tmp_path / "first"
    assert run_build(first).returncode == 0
    second = tmp_path / "second"
    result = run_build(second, PREVIOUS_DIR=str(first), ONLY_SOURCE="demo-phd-mentorship-2026")
    assert result.returncode == 0, result.stderr
    jobs = unseal(json.loads((second / "jobs.json").read_text()), PIN)
    catalog = unseal(json.loads((second / "catalog.json").read_text()), PIN)
    assert len(jobs["runs"]) == 2
    assert jobs["runs"][0]["only_source"] == "demo-phd-mentorship-2026"
    assert len(catalog["sources"]) == 2


def test_failed_run_keeps_last_good_data(tmp_path):
    first = tmp_path / "first"
    assert run_build(first).returncode == 0
    second = tmp_path / "second"
    # No fixtures flag and no Google credentials: discover fails.
    result = subprocess.run(
        [sys.executable, str(ROOT / "pipeline" / "build.py"), "--out", str(second), "--warehouse", str(second / "w.duckdb")],
        env={**{k: v for k, v in os.environ.items() if k != "GOOGLE_SERVICE_ACCOUNT_JSON"},
             "SITE_PIN": PIN, "PREVIOUS_DIR": str(first)},
        capture_output=True, text=True,
    )
    assert result.returncode == 1
    jobs = unseal(json.loads((second / "jobs.json").read_text()), PIN)
    catalog = unseal(json.loads((second / "catalog.json").read_text()), PIN)
    assert jobs["runs"][0]["status"] == "failed"
    assert jobs["runs"][0]["tasks"][0]["status"] == "failed"
    assert len(catalog["sources"]) == 2


def test_day_first_timestamps_and_multi_select_and_partial_load(tmp_path):
    fixtures = tmp_path / "fx"
    fixtures.mkdir()
    sheet = copy.deepcopy(DEMO)
    rows = sheet["tabs"]["DATA"][1:]
    for i, row in enumerate(rows):
        # Day-first dates that would also parse month-first (01/07 = 1 July, not 7 January).
        row[0] = f"0{1 + i % 3}/07/2026 10:00:00"
        row[23] = "Europe, Asia Pacific" if i % 2 else "Europe"
    sheet["unread_columns"] = ["email_address", "research_proposal_draft"]
    (fixtures / "sheet.json").write_text(json.dumps(sheet))
    result = subprocess.run(
        [sys.executable, str(ROOT / "pipeline" / "build.py"), "--fixtures", str(fixtures),
         "--out", str(tmp_path / "out"), "--warehouse", str(tmp_path / "w.duckdb")],
        env={**os.environ, "SITE_PIN": PIN}, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    dash = unseal(json.loads((tmp_path / "out" / "dashboard.json").read_text()), PIN)["sources"][DEMO["spreadsheet_id"]]
    assert [p["date"] for p in dash["timeline"]] == ["2026-07-01", "2026-07-02", "2026-07-03"]
    regions = next(c for c in dash["charts"] if c["column"] == "which_regions_are_you_interested_in_studying_in")
    assert regions["multi"]
    counts = {v["value"]: v["count"] for v in regions["values"]}
    assert set(counts) == {"Europe", "Asia Pacific"}
    # Everyone picked Europe, every other row also picked Asia Pacific: per-option counts, not combinations.
    assert counts["Europe"] == dash["kpis"]["records"] and 0.4 < counts["Asia Pacific"] / counts["Europe"] < 0.6
    assert "duplicate_submissions" not in dash["kpis"]
    catalog = unseal(json.loads((tmp_path / "out" / "catalog.json").read_text()), PIN)
    cols = {c["name"]: c["completeness"] for c in catalog["sources"][0]["columns"]}
    assert cols["email_address"] is None and cols["timestamp"] == 1.0
    assert catalog["sources"][0]["partial_load"] is True


def test_filter_cube_totals_and_generalisation(tmp_path):
    fixtures = tmp_path / "fx"
    fixtures.mkdir()
    sheet = copy.deepcopy(DEMO)
    sheet["tabs"]["DATA"][1][6] = "Papua"  # one person: must not be a filter value
    (fixtures / "sheet.json").write_text(json.dumps(sheet))
    result = subprocess.run(
        [sys.executable, str(ROOT / "pipeline" / "build.py"), "--fixtures", str(fixtures),
         "--out", str(tmp_path), "--warehouse", str(tmp_path / "w.duckdb")],
        env={**os.environ, "SITE_PIN": PIN}, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stderr
    dash = unseal(json.loads((tmp_path / "dashboard.json").read_text()), PIN)["sources"][DEMO["spreadsheet_id"]]
    cube = dash["cube"]
    dims = [d["column"] for d in cube["dims"]]
    assert dims == [c for c in CONFIG["governance"]["dashboard_filters"]]
    assert not {"email_address", "full_name", "referral_source"} & set(dims)
    assert sum(r[-1] for r in cube["base"]) == dash["kpis"]["records"]
    # Every published filter value other than "Other" covers at least min_group_size people.
    k = cube["min_group_size"]
    for i, dim in enumerate(cube["dims"]):
        for j, value in enumerate(dim["values"]):
            total = sum(r[-1] for r in cube["base"] if r[i] == j)
            assert value == "Other" or total >= k, (dim["column"], value, total)
    provinces = cube["dims"][dims.index("your_current_residency_province")]["values"]
    assert "Other" in provinces and "Papua" not in provinces
    assert "Papua" not in json.dumps(cube)
    # Chart cubes add up to the chart totals for single-answer columns.
    sector = cube["charts"]["current_occupation_job_sector"]
    assert sum(r[-1] for r in sector["rows"]) == dash["kpis"]["records"]


def test_knowledge_base_is_built_from_published_aggregates_only(tmp_path):
    assert run_build(tmp_path).returncode == 0
    kb = unseal(json.loads((tmp_path / "knowledge.json").read_text()), PIN)
    kinds = {p["kind"] for p in kb["passages"]}
    assert {"platform", "source", "column", "governance", "chart", "quality", "pipeline"} <= kinds
    text = json.dumps(kb)
    for leaked in ("applicant1@example.com", "Demo Applicant", "+62 812"):
        assert leaked not in text
    assert all(p["id"] and p["text"] and p["link"].startswith("#/") for p in kb["passages"])


def _build_into(fixtures: Path, out: Path, lake: Path, previous: Path | None = None):
    # Two builds in one test are two runs: don't inherit the CI run's GITHUB_RUN_ID.
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("GITHUB_")}, "SITE_PIN": PIN}
    if previous:
        env["PREVIOUS_DIR"] = str(previous)
    return subprocess.run(
        [sys.executable, str(ROOT / "pipeline" / "build.py"), "--fixtures", str(fixtures), "--out", str(out),
         "--warehouse", str(out / "w.duckdb"), "--lakehouse", str(lake)],
        env=env, capture_output=True, text=True,
    )


def test_lakehouse_layers_scd2_quality_rules_and_outputs(tmp_path):
    import duckdb

    fx, lake = tmp_path / "fx", tmp_path / "lake"
    fx.mkdir()
    sheet = copy.deepcopy(DEMO)
    sheet["tabs"]["QUALITY_RULES"] = [
        ["column_name", "rule", "parameter", "severity", "description"],
        ["prior_research_experience", "allowed_values", "Yes|No", "error", "Only Yes or No"],
        ["nope", "not_null", "", "warn", ""],
    ]
    (fx / "sheet.json").write_text(json.dumps(sheet))
    first = _build_into(fx, tmp_path / "out1", lake)
    assert first.returncode == 0, first.stderr

    # Second load: one applicant edits an answer, one row disappears, one new applicant arrives.
    rows = sheet["tabs"]["DATA"]
    rows[5][36] = "No" if rows[5][36] == "Yes" else "Yes"
    removed_email = rows[10][1]
    del rows[10]
    new = list(rows[1])
    new[0], new[1] = "28/07/2026 09:00:00", "brand.new@example.com"
    rows.append(new)
    (fx / "sheet.json").write_text(json.dumps(sheet))
    second = _build_into(fx, tmp_path / "out2", lake, previous=tmp_path / "out1")
    assert second.returncode == 0, second.stderr

    run = unseal(json.loads((tmp_path / "out2" / "jobs.json").read_text()), PIN)["runs"][0]
    table = next(iter(run["layers"]["silver"]))
    sil, bro = run["layers"]["silver"][table], run["layers"]["bronze"][table]
    assert (sil["inserted"], sil["updated"], sil["deleted"]) == (1, 1, 1)
    assert bro["snapshots"] == 2 and bro["total_rows"] == 148 + 148
    rule_ids = {r["id"]: r for r in sil["rules"]}
    assert "prior_research_experience:allowed_values(Yes|No)" in rule_ids
    assert rule_ids["prior_research_experience:allowed_values(Yes|No)"]["origin"] == "sheet"
    catalog = unseal(json.loads((tmp_path / "out2" / "catalog.json").read_text()), PIN)
    assert any("unknown column 'nope'" in w for w in catalog["sources"][0]["warnings"])

    con = duckdb.connect()
    con.execute(f"CREATE TABLE s AS SELECT * FROM read_parquet('{lake}/lakehouse/silver/{table}.parquet')")
    removed = con.execute("SELECT is_current, is_deleted, valid_to IS NOT NULL FROM s WHERE record_key = ?",
                          [removed_email.lower()]).fetchall()
    assert removed == [(False, True, True)]
    versions = con.execute("SELECT count(*) FROM s GROUP BY record_key HAVING count(*) > 1").fetchall()
    assert len(versions) == 1  # the edited applicant has two versions
    assert con.execute("SELECT count(*) FROM s WHERE is_current AND is_deleted").fetchone()[0] == 0

    sheets = {p.stem: json.loads(p.read_text()) for p in (lake / "sheets" / "outputs").glob("*.json")}
    assert "CONSOLIDATED" in sheets
    per_source = next(v for k, v in sheets.items() if k.endswith("silver"))
    assert set(per_source["tabs"]) == {"CURRENT", "HISTORY", "DQ_ISSUES", "ABOUT"}
    assert len(per_source["tabs"]["HISTORY"]["rows"]) == len(per_source["tabs"]["CURRENT"]["rows"]) + 2
    consolidated = sheets["CONSOLIDATED"]["tabs"]["ALL_DATA"]
    assert consolidated["header"][0] == "source_table"
    assert len(consolidated["rows"]) == sil["current"]


def _query(lake: Path, sql: str, *extra):
    return subprocess.run([sys.executable, str(ROOT / "pipeline" / "query.py"), "--lakehouse", str(lake), "--sql", sql, *extra],
                          env={k: v for k, v in os.environ.items() if k != "GOOGLE_SERVICE_ACCOUNT_JSON"},
                          capture_output=True, text=True)


def test_query_previews_are_read_only_and_expire(tmp_path):
    lake = tmp_path / "lake"
    assert _build_into(ROOT / "tests" / "fixtures", tmp_path / "out", lake).returncode == 0
    ok = _query(lake, "SELECT source_table, count(*) AS n FROM gold.consolidated GROUP BY 1", "--label", "count")
    assert ok.returncode == 0, ok.stdout + ok.stderr
    previews = list((lake / "sheets" / "previews").glob("*.json"))
    assert len(previews) == 1
    result = json.loads(previews[0].read_text())["tabs"]["RESULT"]
    assert result["header"] == ["source_table", "n"] and result["rows"][0][1] > 0

    for bad in ("DELETE FROM silver.x", "SELECT 1; SELECT 2", "COPY (SELECT 1) TO 'x.csv'",
                "SELECT * FROM read_csv('/etc/passwd')"):
        res = _query(lake, bad)
        assert res.returncode == 1, bad
        assert "Query failed" in res.stdout

    # Age the preview past the retention window: the next run deletes it.
    data = json.loads(previews[0].read_text())
    data["created"] = "2000-01-01T00:00:00+00:00"
    previews[0].write_text(json.dumps(data))
    cleaned = _query(lake, "", "--cleanup-only")
    assert cleaned.returncode == 0 and not previews[0].exists()
