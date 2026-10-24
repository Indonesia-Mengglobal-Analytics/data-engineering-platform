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

## Lakehouse: bronze, silver, gold

Every registered sheet flows through three layers in DuckDB. They are saved as Parquet in a Google Shared Drive
(`storage.shared_drive_folder_id`), so history survives between runs.

| Layer | Table | What it holds |
|---|---|---|
| Bronze | `bronze.<table>` | Every run's snapshot exactly as read, plus `_run_id`, `_ingested_at`, `_row`, `_row_hash`. Append-only, pruned after `bronze_retention_days` (30). |
| Silver | `silver.<table>` | Cleaned values (trimmed, emails lower-cased, timestamp parsed), quality-checked (`dq_status`, `dq_issues`), **SCD type 2**: one version per record and change, keyed by `quality.record_key` (`email_address`), with `valid_from`, `valid_to`, `is_current`, `is_deleted`. The latest submission of a record wins. |
| Gold | `gold.<table>` | Current records that passed every error-level rule. Feeds the dashboard and Ask. |
| Gold | `gold.consolidated` | Current records of every source, stacked, with `source_table`. |
| Gold | `gold.dq_issues` | Every rule failure in the latest snapshot (column, rule, severity, message, sheet row). Values are never copied. |

Each run's counts (appended, inserted, updated, deleted, merged resubmissions, pass/warn/fail per rule, gold rows,
sheets written) are published to the site: Job Runs → a run → "What each layer did", and Catalog → a table → Layer metrics.

### Quality rules

A standard set runs on every source (`quality.standard` in `config/platform.yml`): timestamp present and readable, email
present and valid, phone, URL and date formats, consent answered. `error` keeps a row out of gold and the dashboard;
`warn` only reports it. A sheet can add its own rules in a `QUALITY_RULES` tab:

| column_name | rule | parameter | severity | description |
|---|---|---|---|---|
| prior_research_experience | allowed_values | Yes\|No | error | Only Yes or No |
| motivation_for_pursuing_phd | min_length | 600 | warn | The form asks for 600+ characters |

Rules: `not_null`, `email`, `phone`, `url`, `date`, `timestamp`, `regex`, `allowed_values` (`a|b|c`), `min_length`,
`max_length`, `unique`. Rows that can't be used are listed as warnings on Data Ingestion.

### Spreadsheet outputs and previews

Rows are only ever shown in Google Sheets in the Shared Drive, where Google controls access:

- `outputs/<source> · silver`: tabs CURRENT, HISTORY (every SCD version), DQ_ISSUES, ABOUT. Rewritten every run.
- `outputs/CONSOLIDATED`: tabs ALL_DATA (stacked, `source_table` first), DQ_ISSUES, SUMMARY.
- `previews/`: results of the SQL Editor. The **Query** workflow (`query.yml`) runs one read-only DuckDB `SELECT` over
  bronze, silver and gold (file and network access are switched off), writes the result to a new sheet, and deletes
  previews older than `outputs.preview_retention_hours` (2) every 30 minutes. Only people with write access to the
  repository can run queries. Content managers can't delete permanently in a Shared Drive, so an expired preview may sit in
  the Shared Drive's trash, where Google purges it after 30 days.

The site's SQL Editor starts the query workflow with a fine-grained GitHub token (this repository only,
`Actions: Read and write`) kept in the browser tab, or opens the workflow on GitHub with the SQL copied.

It is light: a few hundred rows run through DuckDB in seconds, and each run writes a handful of Parquet files and sheets.

## Security model

The repository and its Pages site are public, so:

1. **Rows never reach this site.** They live only in Google: the source sheets, and the Shared Drive (lakehouse Parquet,
   output sheets, previews), where Drive sharing decides who can open them. The run's local DuckDB file is deleted at the
   end of the job, and only aggregates are published to Pages.
2. **Personal and sensitive columns are dropped** before anything is counted (`config/platform.yml` → `governance.rules`).
   Category groups smaller than `min_group_size` (5) are folded together, and hidden when the fold is itself under 5.
3. **Dashboard filters use grouped counts, not rows.** For the columns in `governance.dashboard_filters`, the pipeline
   publishes counts per combination of filter values (and per submission day). Filter values and chart answers held by
   fewer than 5 people are grouped as "Other" first. The browser applies the under-5 rule again to every number, and
   hides the charts when a filter leaves fewer than 5 people. A PIN holder who decrypts the files can still see small
   combinations of these coarse fields, so keep the filter list short and free of personal or sensitive columns.
4. **Published files are encrypted** with AES-256-GCM, using a key derived from the 12-digit site PIN (PBKDF2-SHA256, 600,000 iterations).
   The browser decrypts them after login; without the PIN the JSON on Pages is unreadable. The PIN is never in the repo.

Use a random PIN, not a pattern. Changing `SITE_PIN` resets the run history shown on the site (old files can't be opened with the new PIN).

## Setup

1. **GitHub Pages:** Settings → Pages → Source: **GitHub Actions**.
2. **Shared Drive for the lakehouse:** create a Shared Drive (or a folder in one), add the service account as
   **Content manager**, and put the folder ID in `config/platform.yml` → `storage.shared_drive_folder_id`.
   Without it, each run keeps the lakehouse only while it runs: no history, no spreadsheet outputs, no SQL previews.
3. **Google service account:** in Google Cloud, create a service account, enable the Drive API and Sheets API, create a JSON key.
   Share the live Drive folder (or each sheet) with the service account's email as **Viewer**.
4. **Secrets** (Settings → Secrets and variables → Actions):
   - `GOOGLE_SERVICE_ACCOUNT_JSON`: the whole JSON key.
   - `SITE_PIN`: 12 digits.
5. **Config:** put the Drive folder ID in `config/platform.yml` → `sources.drive_folder_ids` (the ID is the last part of the folder URL).
6. **Run:** Actions → *Pipeline (Sheets to DuckDB to Pages)* → **Run workflow**. The site URL is shown on the deploy job.

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
| `pipeline/build.py` | The tasks: discover, validate, bronze, silver, gold, store, govern, export, publish |
| `pipeline/lakehouse.py` | Bronze append, silver cleaning and SCD type 2, gold tables |
| `pipeline/quality.py` | Standard and per-sheet quality rules |
| `pipeline/storage.py` | Shared Drive (or local) storage for the lakehouse |
| `pipeline/exports.py` | Per-source, CONSOLIDATED and preview sheets |
| `pipeline/query.py` | Read-only SQL to a preview sheet, and preview clean-up |
| `pipeline/knowledge.py` | Passages for the Ask page, built from published aggregates |
| `pipeline/contract.py` | Source format check |
| `pipeline/governance.py` | Column classification and small-group suppression |
| `pipeline/seal.py` | PIN-based encryption of published files |
| `pipeline/sources.py` | Google Drive / Sheets reader and fixture reader |
| `site/` | The static console |
| `.github/workflows/pipeline.yml` | Test, build, deploy |
| `.github/workflows/query.yml` | SQL previews on demand, expired previews deleted every 30 minutes |
