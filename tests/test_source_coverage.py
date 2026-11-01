"""The rule in docs/DATA_SOURCE_RULES.md: every data source shows on every page.

Builds both jobs (registered sheets and people) into one site folder, the way the two workflows deploy it, and checks
that each source reaches every file the pages read, and that every page reads both jobs' files.
"""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline"))

from seal import unseal  # noqa: E402

PIN = "123456789012"


@pytest.fixture(scope="module")
def site(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("site")
    env = {**{k: v for k, v in os.environ.items() if not k.startswith("GITHUB_")}, "SITE_PIN": PIN}
    out = tmp / "data"
    main = subprocess.run([sys.executable, str(ROOT / "pipeline" / "build.py"), "--fixtures", str(ROOT / "tests" / "fixtures"),
                           "--out", str(out), "--warehouse", str(tmp / "w.duckdb"), "--lakehouse", str(tmp / "lake")],
                          env=env, capture_output=True, text=True)
    assert main.returncode == 0, main.stderr
    people = subprocess.run([sys.executable, str(ROOT / "pipeline" / "people_build.py"), "--fixtures", str(ROOT / "tests" / "fixtures" / "people"),
                             "--lakehouse", str(tmp / "plake"), "--out", str(out)], env=env, capture_output=True, text=True)
    assert people.returncode == 0, people.stderr
    files = {}
    for f in ("catalog", "jobs", "governance", "dashboard", "knowledge", "people", "people_directory"):
        files[f] = unseal(json.loads((out / f"{f}.json").read_text()), PIN)
    return files


def test_every_registered_sheet_reaches_every_page(site):
    cat, gov, dash, kb, jobs = site["catalog"], site["governance"], site["dashboard"], site["knowledge"], site["jobs"]
    text = json.dumps(kb["passages"], ensure_ascii=False)
    assert cat["sources"], "fixtures must have sources"
    for s in cat["sources"]:
        assert s["title"] in text, f"Ask has no passage for {s['title']}"                  # Ask
        if s["status"] == "registered":
            assert any(g["id"] == s["id"] for g in gov["sources"]), s["title"]             # Governance
            assert s["id"] in dash["sources"] or any(d["title"] == s["title"] for d in dash["sources"].values()), s["title"]  # Dashboards
            assert s.get("columns") and s.get("fqn"), s["title"]                           # Catalog, SQL, search
            assert s.get("quality_rules") is not None, s["title"]                          # Data Quality
        else:
            assert s.get("errors"), f"{s['title']} is {s['status']} without a reason"       # Data Ingestion, Need attention
    assert jobs["runs"][0].get("environment") == "dev"                                      # DEV label


def test_every_people_source_reaches_every_page(site):
    people, kb, directory = site["people"], site["knowledge"], site["people_directory"]
    text = json.dumps(kb["passages"], ensure_ascii=False)
    nodes = {n["id"]: n for n in people["lineage"]["nodes"]}
    for s in people["sources"]:
        assert s["status"] in ("read", "unreachable", "archived")                          # Data Ingestion, People → Sources
        assert f"src:{s['key']}" in nodes, s["key"]                                         # Lineage
        assert s["title"] in text, f"Ask has no passage for {s['title']}"                  # Ask
        if s["status"] == "unreachable":
            assert s.get("error"), s["key"]                                                 # Need attention says why
    for s in people["gold"]["mentorship"]["sources"]:
        assert f"pair:{s['key']}" in nodes, s["key"]
    # The people tables are described for Catalog, Governance, SQL and search, and the tree has the people.
    assert {t["name"] for t in people["catalog"]["tables"]} >= {"gold_people", "records", "sources"}
    assert len(directory["people"]) == people["kpis"]["people"]
    assert people["workflow"]["environment"] == "dev"


def test_every_page_reads_both_jobs():
    js = (ROOT / "site" / "assets" / "app.js").read_text()

    def body(name):
        m = re.search(rf"\n  function {name}\(", js)
        assert m, name
        nxt = re.search(r"\n  (?:function |const |// ---)", js[m.end():])
        return js[m.start(): m.end() + (nxt.start() if nxt else len(js))]

    pages = {"home": "Workspace", "catalog": "Catalog", "jobs": "Jobs & Pipelines", "runsPage": "Job Runs",
             "ingestion": "Data Ingestion", "governance": "Governance", "qualityPage": "Data Quality",
             "mountSql": "SQL Editor", "buildSearch": "Search"}
    for fn, page in pages.items():
        b = body(fn)
        pipeline = re.search(r"sources\(\)|registered\(\)|runs\(\)|D\.catalog|lake\(\)|D\.governance|JOB_ID", b)
        people = re.search(r"D\.people|peopleTables\(\)|peopleRuns\(\)|allRuns\(\)|peopleJobRow\(\)|peopleGovernance\(\)|sheetSources\(\)", b)
        assert pipeline and people, f"{page} ({fn}) must read both the pipeline's and the people job's data"
