# Data source rules

Every data source that enters the IM Data Platform must be visible on **every page** of the site, from the
Workspace overview to Ask. This file is the rule; `tests/test_source_coverage.py` checks it on every push.

## How data reaches the platform

```
(team member)@indonesiamengglobal.com        owns the sheet
        │
        ▼
   Google Sheets                              forms and tables, kept by the team
        │
        ▼
   Data Sources (dev / prd)                   one Drive folder per environment
        ▲
        │  GitHub Actions with the service account
        │  (daily schedule, or Run workflow by hand)
        ▼
   DE Platform                                lakehouse, PEOPLE sheet, this site
```

Everything in the environment's Data Sources folder is fed to the platform as a source and a catalog entry.

## Environments

| Environment | Status | Where it is set |
|---|---|---|
| **dev** | **Current.** The site shows a **DEV** label. | `config/platform.yml` → `environment: dev`, folders and sheet IDs under `environments.dev` |
| prd | Not used yet. Sheets are promoted here when the team signs them off. | `environments.prd` |

A manual run can pick the environment (workflow input `environment`); schedules run the configured one. Never point
dev and prd at the same folder.

## Two ways a source comes in

| Kind | Job | How to add it | What it needs |
|---|---|---|---|
| **Registered sheet** | `sheets_to_duckdb` (`pipeline.yml`, every 6 hours) | Put the Google Sheet in the environment's Data Sources folder (`drive_folder_ids`), or add its ID to `spreadsheet_ids`. | Tabs `DATA`, `COLUMN_METADATA`, `TABLE_METADATA` in the agreed format (README "Source format"). An owner email in `TABLE_METADATA`. Optional `QUALITY_RULES` tab. |
| **People source** | `people_dataset` (`people.yml`, daily) | Add an entry under `sources` (or `pairings`) in `config/people.yml`: `key`, `title`, `spreadsheet_id`, `tab`, `label`, and `status` / `year` / `header_row` when needed. Map new column headers under `columns` / `attributes`. | A column the job can identify people by (email). A glossary label (`[Data-020]`). |

In both cases: share the sheet (or folder) with the service account as **Viewer**. A sheet it cannot open is not
silently dropped; it shows as **unreachable** everywhere below.

## The rule: one source, every page

When a source is added (or changes status), the next run must show it on all of these, with no hand-edited list:

| Page | Registered sheet | People source |
|---|---|---|
| **Workspace** | counted in the "Google Sheets sources" tile and the flow; a table in Recents; listed in Need attention if rejected or unreachable | counted in the same tile and flow; people tiles; Need attention if unreachable |
| **Catalog** | `sheets.bronze/silver/gold.<table>` with columns, tags, layers, lineage, permissions | its rows land in the `people.*` tables, whose columns, tags and "shown on this site" are listed |
| **Jobs & Pipelines** | its own ingestion pipeline row, with a Run button | the `people_dataset` job row |
| **Job Runs** | the run's per-source counts | the run's sources read |
| **Data Ingestion** | status (registered, rejected with reasons, unreachable), owner, rows | status (read, unreachable, archived), rows, excluded |
| **Governance** | column classification, withheld columns, consent coverage | people dataset classification and which site file shows what |
| **Data Quality** | its quality rules with pass rates | anomalies and the dummy / duplicate detection |
| **Dashboards** | its own dashboard (counts only, groups under 5 hidden) | People gold, Tree, Anomalies |
| **People → Sources / Lineage** | | its row with the columns used, and its box in the lineage with counts |
| **SQL Editor** | its tables and columns in the schema list | the `people.*` tables |
| **Search (Ctrl+P)** | its table, columns, source and dashboard | its people, by name |
| **Ask** | passages about it in the knowledge base | the people passages |

The pages read only what the runs publish (`catalog.json`, `governance.json`, `dashboard.json`, `jobs.json`,
`people.json`, `people_directory.json`, `knowledge.json`), so a source that is in a run is on every page. Do not add a
page that keeps its own list of sources; read the published files instead. A new page must handle **both** jobs' data
(see README "One site, two jobs").

## Archive and restore (people sources)

A people source can be **archived** instead of deleted: it is not read, so people gold, the tree, every count and the
lineage are rebuilt without it. Use **Archive** on People → Lineage (click the source) or People → Sources, or run the
**Manage people sources** workflow. Each change is committed to `config/people_sources_state.yml` with who, when and
why. **Restore** brings it back on the next run. Registered sheets are archived by moving them out of the Data Sources
folder (or removing the ID).

## Privacy

- Never commit rows, exports or screenshots of real data. `site/data/*.json` is built by the runs and git-ignored.
- Columns are classified on every run (personal, sensitive, free text, categorical, metadata). Personal and sensitive
  columns never reach the site as values; the only exception is the People tree directory while
  `publish_directory: true` (names, roles, places, positions; never phone or date of birth).
- Counts on the site hide groups under 5 people.

## Definition of done for a new source

- [ ] The sheet is in the right environment's Data Sources folder (or its ID / `config/people.yml` entry is added) and
      shared with the service account.
- [ ] Registered sheet: the three tabs pass the format check (Data Ingestion shows **registered**).
- [ ] People source: Data Ingestion and People → Sources show it as **read**, with the expected columns mapped.
- [ ] It appears on every page in the table above (check after the run, or rely on the coverage test).
- [ ] Personal columns are tagged personal (Governance) and nothing personal shows as a value outside the Tree.
- [ ] `pytest -q tests` passes.
