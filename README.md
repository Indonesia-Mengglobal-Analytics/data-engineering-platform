# IM Data Platform

Indonesia Mengglobal's data workspace: **Google Sheets → DuckDB on GitHub Actions → GitHub Pages**. No servers.

```
Live Drive folder ─┐                ┌─ GitHub Actions (every 6 h, manual, on push) ─────────────┐
config/platform.yml┴─> discover ──> validate ──> ingest ──> govern ──> publish ──> GitHub Pages
                       (Drive API)  (format     (DuckDB    (drop PII,  (aggregates,   (static site,
                                     contract)   raw.*)     curated.*)  AES-encrypted)  12-digit PIN)
```

The site looks and works like a lakehouse workspace console:

| Page | What it shows |
|---|---|
| Workspace | Sources, records, last run, withheld columns, things needing attention |
| Catalog | `sheets.raw.*` tables and `sheets.curated.*` views: columns, tags, details, permissions, lineage, quality |
| Jobs & Pipelines | The `sheets_to_duckdb` job, one ingestion pipeline per sheet, run matrix, task graph, **Run now** |
| Job Runs | Every run with status, trigger, task timings, link to the GitHub logs |
| Data Ingestion | Every sheet found: registered, rejected (with reasons) or unreachable, and the format to follow |
| Dashboards | Per source: submissions per day, category charts, data quality |
| Governance | Classification policy, withheld columns, consent coverage, rules |

## Security model

The repository and its Pages site are public, so:

1. **Rows never leave the Actions run.** The DuckDB file is deleted at the end of the job. Only aggregates are published.
2. **Personal and sensitive columns are dropped** before anything is counted (`config/platform.yml` → `governance.rules`).
   Category groups smaller than `min_group_size` (5) are folded together, and hidden when the fold is itself under 5.
3. **Published files are encrypted** with AES-256-GCM, using a key derived from the 12-digit site PIN (PBKDF2-SHA256, 600,000 iterations).
   The browser decrypts them after login; without the PIN the JSON on Pages is unreadable. The PIN is never in the repo.

Use a random PIN, not a pattern. Changing `SITE_PIN` resets the run history shown on the site (old files can't be opened with the new PIN).

## Setup

1. **GitHub Pages:** Settings → Pages → Source: **GitHub Actions**.
2. **Google service account:** in Google Cloud, create a service account, enable the Drive API and Sheets API, create a JSON key.
   Share the live Drive folder (or each sheet) with the service account's email as **Viewer**.
3. **Secrets** (Settings → Secrets and variables → Actions):
   - `GOOGLE_SERVICE_ACCOUNT_JSON`: the whole JSON key.
   - `SITE_PIN`: 12 digits.
4. **Config:** put the Drive folder ID in `config/platform.yml` → `sources.drive_folder_ids` (the ID is the last part of the folder URL).
5. **Run:** Actions → *Pipeline (Sheets to DuckDB to Pages)* → **Run workflow**. The site URL is shown on the deploy job.

## Source format

A sheet is registered only when it has these three tabs:

| Tab | Content |
|---|---|
| `DATA` | Row 1: header (the form questions). Then one row per record. |
| `COLUMN_METADATA` | `no`, `column_name` (snake_case, unique), `column_description`, `column_header_location` (`A1`, `B1`, …), `form_question` (must equal the `DATA` header at that location). One row per `DATA` column. |
| `TABLE_METADATA` | `table_name`, `table_description`, `owner_email`, `operation_date_start`, `operation_date_end`, `populated_date` (dates `YYYYMMDD`), `purpose`, `version`. Needs a row with `table_name = data`. |

`Mentee_Data_PhD_Mentorship_2026` is the reference example. Anything else shows as **Rejected** on Data Ingestion, with the reason.

## Develop locally

```bash
pip install -r pipeline/requirements.txt pytest
pytest -q tests                                   # contract, governance, encryption, end-to-end build
python tests/fixtures/make_demo.py                # regenerate demo fixtures (made-up rows)
SITE_PIN=123456789012 python pipeline/build.py --fixtures tests/fixtures
cd site && python -m http.server 8000             # open http://localhost:8000, PIN 123456789012
```

`site/data/*.json` is git-ignored: it is built by the workflow, never committed.

| Path | Purpose |
|---|---|
| `config/platform.yml` | Sources, format contract, governance rules |
| `pipeline/build.py` | The five tasks: discover, validate, ingest, govern, publish |
| `pipeline/contract.py` | Source format check |
| `pipeline/governance.py` | Column classification and small-group suppression |
| `pipeline/seal.py` | PIN-based encryption of published files |
| `pipeline/sources.py` | Google Drive / Sheets reader and fixture reader |
| `site/` | The static console |
| `.github/workflows/pipeline.yml` | Test, build, deploy |
