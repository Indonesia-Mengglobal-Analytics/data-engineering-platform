# IM Data Platform: notes for AI assistants and contributors

- **Environment: DEV.** `config/platform.yml` → `environment: dev`. Do not point anything at prd without the team.
- **Data sources follow `docs/DATA_SOURCE_RULES.md`.** Every new source must show on every page (Workspace, Catalog,
  Jobs, Job Runs, Ingestion, Governance, Quality, Dashboards / People, SQL editor, search, Ask). Pages read only the
  files the runs publish; never hard-code a list of sources in the site. `tests/test_source_coverage.py` enforces it.
- Two jobs deploy one site: `pipeline.yml` (registered sheets) and `people.yml` (people dataset). Each carries the
  other's published files; a new page must read both.
- People sources are archived, not deleted: `config/people_sources_state.yml` via the Manage people sources workflow.
- Never commit rows, exports or screenshots of real data. Test fixtures are invented.
- Run `pytest -q tests` before pushing.
