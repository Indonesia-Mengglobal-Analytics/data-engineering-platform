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
| People | The people dataset: roles, overlaps, journeys, anomalies, review list, sources, runs |

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

## Ask (RAG)

The pipeline writes an encrypted knowledge base (`knowledge.json`) of short passages built only from what the site
already shows: catalog, column dictionary, governance, data quality, layer metrics, dashboard figures and runs.
The Ask page searches it in the browser (BM25) and answers with one of two engines:

| Engine | Cost | Where it runs | Notes |
|---|---|---|---|
| **Free · in browser** (default) | Free | On the viewer's device, with [WebLLM](https://github.com/mlc-ai/web-llm) and WebGPU | Qwen2.5 1.5B (~1.6 GB), 0.5B (~0.9 GB) or 3B (~2.5 GB), downloaded once per browser and cached. Needs Chrome or Edge on a laptop or desktop. Answers from the 5 best passages; nothing leaves the device. |
| **Claude · API key** | The viewer's Anthropic API account | api.anthropic.com | `claude-opus-5-5`; can also count records with filters through the privacy-filtered dashboard cube. The key stays in the browser tab. |

Without WebGPU or a key, Ask still shows the best-matching passages.

## People dataset

`.github/workflows/people.yml` (daily, manual, on changes to its files) builds one row per person from every
mentorship (Master and PhD, mentee and mentor), event and management sheet listed in `config/people.yml`.

```
read ──> normalize ──> resolve ──> build ──> store ──> publish
(Sheets  (emails, phones,  (merge by      (counts,     (people/*.parquet,  (people.json,
 API)     names, years;     normalized     overlaps,    PEOPLE sheet)       encrypted)
          flag anomalies)   email; queue   journeys)
                            the unsure)
```

- **Person key:** the email, lowercased, with Gmail dots and `+tags` removed. `person_id` (`IMP-` + a hash of that key)
  stays the same for that email in every run. The People tab shows the address the person typed most.
- **Labels** follow the [Data-020] glossary: `PHD_MENTEE`, `PHD_MENTOR`, `EVENT_PARTICIPANT`, `EVENT_SPEAKER`,
  `EVENT_COMMITTEE`, `MANAGEMENT_CANDIDATE`, `MANAGEMENT_MEMBER`, plus `MASTER_MENTEE` and `MASTER_MENTOR`, which mirror the PhD
  labels until the glossary defines them. An activity is one person in one role, year and status.
- **Not merged automatically**, following the 2025 duplicates report: same phone, same name, and mentor and mentee in
  the same year. They go to the **Review** tab with a reason.
- **Anomalies:** errors exclude the row (dummy or invalid email, missing email, dummy name); warnings keep it (broken or
  placeholder phone, year outside the program, digits in the name, a source column named "dummy"); info notes duplicates
  and cells with several phones.

Outputs: `people.people`, `people.activities`, `people.records`, `people.anomalies`, `people.review` and `people.sources`
in the lakehouse (queryable from the SQL editor), and the **PEOPLE** sheet in the Shared Drive's `outputs/` folder.
The site gets counts only, with groups under 5 shown as "<5".

A source the service account can't open shows as **unreachable** on People → Sources; share it as Viewer. To add a source,
add an entry to `config/people.yml` with its spreadsheet ID, tab, label and status (`header_row` when the header is not row 1).

### People gold and the people graph

The `gold` step turns the deduplicated people into **`people.gold_people`**, one row per person:

| Group | Columns |
|---|---|
| Who | `person_id`, `full_name`, `email`, `phone`, `linkedin` |
| Roles | `primary_role`, `roles`, `is_master_mentee`, `is_master_mentor`, `is_phd_mentee`, `is_phd_mentor`, `is_event_participant`, `is_event_speaker`, `is_event_committee`, `is_management_member`, `is_management_candidate`, `first_year`, `last_year` |
| Where | `residence_city`, `residence_province`, `residence_island_region` (Java, Sumatra, …), `residence_country`, `birth_city`, `birth_province` |
| Study | `study_country` (master's or PhD), `target_country`, `target_university`, `institution`, `area_of_study` |
| Activity | `events_attended`, `events_spoken`, `mentees_mentored`, `mentors_had` |

Free-text places ("Jaksel", "Kota Bandung, Jabar", "Jogja", "London, UK") are matched with `config/places.yml`
(Indonesian provinces with their cities, island regions, countries with their Bahasa Indonesia names). The newest
answer of a person wins. Add an alias there when a common spelling is left unmatched (`residence_raw` keeps the answer).

Mentor–mentee pairs (`people.mentorships`) come from the pairing sheets under `pairings` in `config/people.yml`:
the PhD bootcamp *final result* (by email) and the Master *mentee mentor pairing* (ids, resolved through the raw tabs by
phone, else an exact name only one person with that role has). Ambiguous or unknown ids are counted, never guessed.

The person-level graph is `people.graph_nodes` and `people.graph_edges`: people, events, cities, provinces, countries,
institutions and roles, linked by `HAS_ROLE`, `ATTENDED`, `SPOKE_AT`, `ORGANIZED`, `MENTORED`, `LIVES_IN`, `BORN_IN`,
`STUDIED_IN`, `STUDIED_AT`, `TARGETS` and `IN` (city → province → country). Query it in the SQL editor, for example
everyone a mentor mentored:

```sql
SELECT n.label AS mentee, e.year FROM people.graph_edges e JOIN people.graph_nodes n ON n.node_id = e.dst
WHERE e.edge_type = 'MENTORED' AND e.src = (SELECT person_id FROM people.gold_people WHERE email = 'mentor@example.com')
```

On the site, **People → People gold** filters by role and region where people live, and shows provinces, cities,
birth provinces, study and target countries, institutions, events attended and mentees per mentor (counts only, groups
under 5 hidden). The gold table itself is the **Gold** tab of the PEOPLE sheet (plus **Mentorships** and **Events**).

**People → Tree** is a left-to-right hierarchy with right-angle (orthogonal) edges down to people's names. Children of a
parent share one vertical trunk, so lines never cross; branches expand and collapse with a short animation (none when the
system asks for reduced motion), and long lists of people show 40 at a time. Five groupings:

| View | Levels |
|---|---|
| By role | Management team / Master mentorship / PhD mentorship / Events → role → region → province → city → people |
| By place | Region (or Abroad → country) → province → city → role group → role → people |
| Mentorship | Program → year → mentor → mentees |
| Events | Year → event → speakers / committee / participants → people |
| Study country | Country studied in (mentors) or targeted (mentees) → role → people |
| Origin | Island region of birth (or born abroad → country) → province → city → role group → people |
| Years with IM | 1 year / 2 years / 3–4 years / 5+ years (distinct years with any role) → role group → people |
| Management | Management team → division → position (leadership first) → people; Applicants → position applied for → people |
| Possible duplicates | Reason (same phone, same name, one email with different names, mentor and mentee in one year) → the people involved |

Filters: search (name, city, province, region, country, institution), roles, region, province, Indonesia or abroad,
study/target country, minimum events attended, mentored someone, active years. Clicking a person opens their roles,
place, origin, years with IM, study, events, mentors, mentees and possible duplicates (each clickable), and the LinkedIn
or research-profile link they gave us. Without one, a **Search LinkedIn** button opens a LinkedIn search for their
name and campus: a suggestion for a team member to check, never stored. Profiles are not crawled or guessed. The tree reads `people_directory.json`, written only while
`publish_directory: true` in `config/people.yml`.

### Management positions

From the management sheets the gold table takes each member's **position** and **division** (Accepted: Division and
Position; SELECTED: Division and Role), the **position history** by year ("2023: Head of Data, Data | 2025: Chief Data
Officer, Data"), a **level** (Leadership: president, VP, director, head, chief, editor in chief; Manager / lead:
manager, lead, coordinator; otherwise Team member) and, for applicants, the **position applied for** (Applied and
Recruitment: Position choice 1). Titles are tidied ("head of data" → "Head of Data", "ceo" → "CEO"). The tree has a
**Management** view and Level, Division and Position filters; the person panel shows the position, level and history.

People with no name in any source are labelled by their full email in the tree (split at the "@" onto two lines when it
does not fit), since it is the only way to tell them apart.

### Anomaly detection

The `detect` step (`pipeline/people_anomaly.py`) gives every source row a **probability of being dummy data** and every
pair of people that may be one person a **probability of being the same person**. Each signal adds evidence (log-odds)
and a logistic turns the sum into a percentage; every score keeps its reasons. These are estimates to rank what to check,
not a trained model, and nothing is deleted or merged because of them.

| Dummy signals (weight) | Same-person signals (weight) |
|---|---|
| test or throwaway email domain (+6.0), email name is a test word (+4.5), placeholder name such as John Doe or Fulan (+4.5), test word as name (+4.0), keyboard run (+3.5), one letter repeated (+3.0), placeholder or sequential phone (+2.2), random letters (+2.2), lorem ipsum / test / dummy typed in the row (+2.0), invalid or missing email, digits in the name, impossible year, duplicate submission | same phone (+3.4), same date of birth (+3.0), same full name (+2.6), very similar name with initials allowed (+1.8), similar email user name (+1.6), same institution (+0.8), same city (+0.5); clearly different names (−3.0) and different dates of birth (−3.5) lower it |

80% or more is **likely**, 50–79% **suspicious**. Results: `people.records.dummy_probability`, `dummy_reasons`,
`people.duplicate_candidates`, and the **Dummy check** and **Duplicate check** tabs of the PEOPLE sheet. Besides the review
list, similar names are found by blocking (first initial + surname, first name + surname initial), so "M. Rizki Pratama"
meets "Muhammad Rizky Pratama".

**People → Anomalies & review** shows the counts, both probability histograms, the signals with their weights, the rows to
check (masked emails), the duplicate pairs, and **people without a name**: real addresses that only appear in sheets with
no name column (for example the management Accepted/Applied tabs and the PhD bootcamp lists) or that left the name blank.
They show in the tree as "No name · b****@gmail.com" until a name column is added to those sheets.

### Names

Names follow one display standard everywhere (gold table, PEOPLE sheet, tree, SQL): word by word, ALL CAPS and all
lowercase become proper case ("BIMA YATNA" → "Bima Yatna"), mixed case is kept as typed ("McDonald"), capitals follow
hyphens and apostrophes ("Nur-Aini", "O'Brien"), and joining words such as bin, binti, van stay lowercase. Raw records keep
the name exactly as typed. Labels in the tree and lineage are fitted by measured pixel width, so long names never run
under a badge; the full name shows on hover.

### Lineage

**People → Lineage** maps the dataset from every Google Sheet tab to every place it is shown, with this run's counts:

```
Sources ─> Read ─> Clean & merge ─> Resolve & link ─> Gold ─> Build ─> Store ─> Shown in
(sheet     people.  anomalies,        activities,       gold_    graph,   lakehouse  People pages, Tree,
 tabs)     records  people, events    review, events,   people   site     (Parquet)  Ask, PEOPLE sheet,
                                      mentorships                aggregates,         SQL editor
                                                                 directory
```

The view is a layered left-to-right diagram with right-angle edges: links into one box share a vertical bus, and links
that skip a column run through empty lanes, so no line crosses a box. Click a box to light up everything upstream and
downstream and see its description, inputs, outputs and columns; pick a gold column ("Trace a gold column") to see the
rule that builds it and the exact source headers that feed it. It is built by `pipeline/people_lineage.py` into
`people.json` (counts and header names only).

The two workflows deploy to the same Pages site. Each downloads the files the other published and carries them forward.
Run the main pipeline once before the first people run.

## Security model

The repository and its Pages site are public, so:

1. **Rows never reach this site, except the people directory.** With `publish_directory: true` (decided by the data
   team on 1 Nov 2026), `people_directory.json` holds each person's name, roles, city/province/region or country,
   place of birth, study and target country, institution, years with IM, events attended, mentees mentored, the
   LinkedIn or profile link they gave, and possible-duplicate pairs, for the People → Tree view. Never email, phone or
   birth date. Set it to `false` to remove the file on the next run. Everything else lives only in Google: the source sheets, and the Shared Drive (lakehouse Parquet,
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
SITE_PIN=123456789012 python pipeline/people_build.py --fixtures tests/fixtures/people --lakehouse build/people
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
