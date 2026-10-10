# Steam Medallion Architecture

An automated, idempotent data pipeline that collects daily Steam game statistics from the [SteamSpy API](https://steamspy.com/api.php) and processes them through a **Bronze to Silver** medallion architecture on Databricks (Delta Lake / PySpark). A GitHub Actions workflow fetches the data every day and triggers the Databricks pipeline job.

Built as a pair project for a Data Analysis and Visualization course. **Current status: Phase 2 complete** (Bronze and Silver layers). The Gold layer and Power BI dashboard are planned for Phase 3.

## Contents

1. [Project Overview](#project-overview)
2. [Architecture](#architecture)
3. [Engineering Highlights](#engineering-highlights)
4. [Data Models](#data-models)
5. [Repository Structure](#repository-structure)
6. [Setup](#setup)
7. [Execution Guide](#execution-guide)
8. [Testing Schema Drift](#testing-schema-drift)
9. [Test Results](#test-results)
10. [Notes and Limitations](#notes-and-limitations)
11. [Roadmap: Phase 3](#roadmap-phase-3)
12. [Team](#team)

## Project Overview

The project treats Steam, the largest PC game distribution platform, as a live market and tracks how games perform over time in pricing, player engagement, review sentiment and publisher presence. The insights are aimed at game publishers, investors and market analysts.

- **Source:** SteamSpy Public API (`https://steamspy.com/api.php?request=all&page={n}`). It returns paginated JSON of up to 1,000 games per page and needs no API key. Pages can overlap, so the same game may appear on more than one page.
- **Refresh cycle:** SteamSpy refreshes its aggregates about once a day, which suits a daily incremental pipeline.
- **PII:** none. Every field is a public, market-level statistic (game ID and name, studio names, aggregate review counts, ownership ranges, store prices, concurrent users). No individual user is identified, so no masking or hashing is needed. Silver only removes inactive entries (0 reviews and 0 concurrent users) as a data quality step.

## Architecture

```
GitHub Actions (daily, 22:00 UTC)
        |
        |  scripts/fetch_steamspy.py  ->  steamspy_<date>.jsonl
        v
Databricks Volume  (/Volumes/workspace/default/steam)
        |
        |  triggers Databricks job steam_daily_pipeline (load_type=incremental, date=<date>)
        v
Task 1: 02_raw_to_bronze    -> bronze_steam
Task 2: 03_bronze_to_silver -> silver_steam  (+ silver_quarantine)

Every step also writes to pipeline_execution_logs.
Schema changes are recorded in schema_drift_log.
```

The scheduled job runs notebooks 02 and 03 directly as two tasks. `04_pipeline_runner` runs the same two notebooks in order and is used for manual runs from the workspace.

All tables live in the `workspace.steam` schema.

**Tech stack:** Databricks (Unity Catalog, Volumes, Jobs), Delta Lake, PySpark, Python 3.11 (`requests`), GitHub Actions, SteamSpy API.

## Engineering Highlights

| Requirement | How it is implemented |
|-------------|-----------------------|
| Explicit schema-on-read | Raw files are read with a `StructType` / `StructField` schema. `inferSchema` is never used. Bronze reads every source column as `STRING`. |
| Strict casting | Casting happens in Bronze to Silver with `try_cast`. A failed cast gives `NULL` instead of an error. |
| `load_timestamp` everywhere | Present in every Bronze, Silver, quarantine and drift-log record. |
| Idempotency | Bronze uses `MERGE` on (`appid`, `load_date`). Silver uses `MERGE` on `appid`. Re-running the same date inserts no duplicates and updates only rows whose `row_hash` changed. |
| Incremental detection | Each row gets a SHA-256 `row_hash` over the 17 source columns. On incremental loads, only new appids or rows whose hash differs from the latest earlier Bronze hash are kept. |
| Parameterized backfills | Notebooks take `load_type`, `date`, `landing_path` and `batch_id` as widget parameters. No date is hardcoded. The default landing folder (`/Volumes/workspace/default/steam`) is a constant in `02_raw_to_bronze`, and `landing_path` overrides it for any single run. |
| Schema drift | New columns are detected, added to Bronze with `MERGE WITH SCHEMA EVOLUTION` and logged in `schema_drift_log`. Wrong-typed values are sent to `silver_quarantine`, so the batch does not crash. |
| Audit logging | Every layer and every run writes a row to `pipeline_execution_logs`, including failures with the error message. |

## Data Models

Delta tables do not enforce primary keys, so the keys below are **logical keys** that the `MERGE` statements rely on.

### Bronze: `bronze_steam`

Raw SteamSpy data stored as strings, plus load metadata. It keeps history: one row per game per load date on which that game was new or changed. Duplicate appids inside one landing file are removed before loading.

**Primary key:** (`appid`, `load_date`)

| Column | Type | Description |
|--------|------|-------------|
| `appid` | STRING | Steam application ID |
| `name` | STRING | Game name |
| `developer` | STRING | Developer name(s) |
| `publisher` | STRING | Publisher name(s) |
| `score_rank` | STRING | SteamSpy score rank |
| `userscore` | STRING | User score |
| `positive` | STRING | Positive review count |
| `negative` | STRING | Negative review count |
| `owners` | STRING | Estimated owner range, e.g. `"1,000,000 .. 2,000,000"` |
| `average_forever` | STRING | Average playtime, all time (minutes) |
| `average_2weeks` | STRING | Average playtime, last 2 weeks (minutes) |
| `median_forever` | STRING | Median playtime, all time (minutes) |
| `median_2weeks` | STRING | Median playtime, last 2 weeks (minutes) |
| `price` | STRING | Current price in US cents |
| `initialprice` | STRING | Launch price in US cents |
| `discount` | STRING | Current discount percentage |
| `ccu` | STRING | Concurrent users (peak, yesterday) |
| `source_file` | STRING | Landing file the row came from |
| `batch_id` | STRING | UUID of the Raw-to-Bronze run |
| `load_type` | STRING | `full` or `incremental` |
| `load_date` | DATE | Date of the data being loaded |
| `row_hash` | STRING | SHA-256 of the 17 source columns, used for change detection |
| `load_timestamp` | TIMESTAMP | When the record was ingested |

New columns that appear in the source are added to this table automatically (schema evolution). They are not part of `row_hash`.

### Silver: `silver_steam`

Typed, cleaned, current-state view: exactly one row per game. Silver keeps only the fields needed for analysis, so `score_rank`, `userscore` and the playtime columns stay in Bronze only.

**Primary key:** `appid`

| Column | Type | Description / transformation |
|--------|------|------------------------------|
| `appid` | INT | Cast from string |
| `name` | STRING | Trimmed, empty string becomes `NULL` |
| `developer` | STRING | Trimmed, empty string becomes `NULL` |
| `publisher` | STRING | Trimmed, empty string becomes `NULL` |
| `positive` | INT | Cast from string |
| `negative` | INT | Cast from string |
| `total_reviews` | INT | `positive + negative` |
| `approval_rate` | DOUBLE | `positive / total_reviews`, a ratio from 0 to 1 (`NULL` if no reviews) |
| `ccu` | INT | Cast from string |
| `owners_min` | BIGINT | Lower bound parsed from the `owners` range |
| `owners_max` | BIGINT | Upper bound parsed from the `owners` range |
| `price_usd` | DOUBLE | `price / 100` (cents to dollars) |
| `initialprice_usd` | DOUBLE | `initialprice / 100` |
| `discount_pct` | DOUBLE | Cast from string |
| `is_free` | BOOLEAN | `price_usd = 0` |
| `row_hash` | STRING | Carried over from Bronze |
| `first_load_date` | DATE | Date the game first appeared in Silver |
| `last_load_date` | DATE | Date the game's data was last updated |
| `load_timestamp` | TIMESTAMP | When the record was last processed |

**Cleaning rules:**
- Rows that fail a cast or validation (invalid `appid`, uncastable numeric fields, unparsable `owners`) go to `silver_quarantine`.
- Dead entries with 0 reviews and 0 concurrent users are dropped. They are not quarantined and not counted in any log column.
- Only one row per `appid` is kept per run (the most recent).
- Update guard: an existing Silver row is updated only if its `row_hash` changed **and** the incoming `load_date` is not older than the stored `last_load_date`. Backfilling an old date therefore never overwrites newer data.

### Supporting tables

**`silver_quarantine`** has the 17 source columns above (as `STRING`) plus the following. **Logical key:** (`appid`, `load_date`, `reject_reason`).

| Column | Type | Description |
|--------|------|-------------|
| `reject_reason` | STRING | Every failed check for the row, separated by `; ` |
| `load_date` | DATE | Date of the batch |
| `load_timestamp` | TIMESTAMP | When the row was quarantined |

**`schema_drift_log`**

| Column | Type | Description |
|--------|------|-------------|
| `column_name` | STRING | Column that appeared in the source |
| `drift_type` | STRING | Currently `new_column` |
| `detected_on` | DATE | Date of the load that revealed it |
| `source_file` | STRING | File it appeared in |
| `load_timestamp` | TIMESTAMP | When it was logged |

**`pipeline_execution_logs`**

Each log row gets its own unique `run_id`. The runner's row is not linked to the step rows by ID; match them by `start_time` and `end_time`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | STRING | Unique ID of this log entry |
| `layer` | STRING | `Raw-to-Bronze`, `Bronze-to-Silver` or `Pipeline-Runner` |
| `load_type` | STRING | `full` / `incremental` |
| `file_processed` | STRING | What was processed: the landing file path (Raw-to-Bronze), `bronze_steam load_date=<date>` (Bronze-to-Silver) or `date=<date>` (Pipeline-Runner) |
| `batch_id` | STRING | Bronze batch ID |
| `start_time` | TIMESTAMP | Execution start |
| `end_time` | TIMESTAMP | Execution end |
| `status` | STRING | `Success` or `Failure` |
| `rows_inserted` | BIGINT | Rows inserted |
| `rows_updated` | BIGINT | Rows updated |
| `rows_skipped_unchanged` | BIGINT | Rows skipped because nothing changed |
| `rows_quarantined` | BIGINT | Rows sent to quarantine |
| `error_message` | STRING | Error text on failure |

The Pipeline-Runner row carries no row counts; those are on each step's own row. It is written only when `04_pipeline_runner` is used. Scheduled job runs write the two step rows.

## Repository Structure

```
.github/workflows/daily.yml               Daily fetch, upload and job trigger
scripts/fetch_steamspy.py                 Pages through SteamSpy and writes a .jsonl file
notebooks/00_setup.ipynb                  Creates schema and all tables (safe to re-run)
notebooks/02_raw_to_bronze.ipynb          Landing file to Bronze
notebooks/03_bronze_to_silver.ipynb       Bronze to Silver, quarantine bad rows
notebooks/04_pipeline_runner.ipynb        Runs 02 then 03 for one date (manual runs)
notebooks/05_run_stats.ipynb              New / changed game counts per run
notebooks/06_schema_drift_test.ipynb      Demo of schema drift and bad-data handling
steam_data_collection.ipynb               Initial data collection and sample exploration
bronze_full_load_sample.json              Sample full load (~82,500 games, ~42 MB)
bronze_incremental_2026-10-07.json        Sample incremental load (1,000 games)
bronze_incremental_2026-10-09.json        Sample incremental load (1,000 games)
steam_store_incremental_2026-10-09.json   Sample Steam Store API data (989 games)
```

There is no `01_` notebook. The initial data collection and exploration lives in `steam_data_collection.ipynb` at the repo root, and the pipeline notebooks keep their original numbers.

## Setup

**Prerequisites:** a Databricks workspace with Unity Catalog and a Volume at `/Volumes/workspace/default/steam`, a Databricks personal access token, and a GitHub account for Actions.

1. **Clone the repo**
   ```bash
   git clone https://github.com/Murtazagonhit10/Steam_Medallion_Architecture.git
   cd Steam_Medallion_Architecture
   ```
2. **Import the notebooks** from `notebooks/` into your Databricks workspace. Keep all the notebooks in the same workspace folder, because `04_pipeline_runner` calls the others by relative path.
3. **Create the tables** by running `00_setup`.
4. **Create a Databricks job** with two notebook tasks: `raw_to_bronze`, which runs `02_raw_to_bronze`, and `bronze_to_silver`, which runs `03_bronze_to_silver` and depends on the first. Under **Job parameters**, define `load_type` (default `incremental`) and `date` (default empty). Job parameters are passed to both tasks, and the workflow sends these names, so they must match exactly. Copy the job's ID into `JOB_ID` in `.github/workflows/daily.yml`. Do not use `04_pipeline_runner` as the job task: a notebook that starts other notebooks from inside a job did not get compute in our workspace.
5. **Add GitHub repository secrets** (Settings > Secrets and variables > Actions):

   | Secret | Value |
   |--------|-------|
   | `DATABRICKS_HOST` | Workspace URL, e.g. `https://<workspace>.cloud.databricks.com` |
   | `DATABRICKS_TOKEN` | Databricks personal access token |

The workflow then runs daily at 22:00 UTC and can also be started from the Actions tab with *Run workflow*. Each run also keeps the fetched file as a GitHub Actions artifact named `steamspy-data`.

## Execution Guide

The pipeline reads `steamspy_<date>.jsonl` from the landing Volume. The fetch script and the notebooks all take the date as a parameter.

### Notebook parameters

| Notebook | Parameter | Values | Purpose |
|----------|-----------|--------|---------|
| `04_pipeline_runner` | `load_type` | `full` / `incremental` | Load mode for the whole run |
| | `date` | `YYYY-MM-DD` | Which day's file to process (blank = today) |
| `02_raw_to_bronze` | `load_type`, `date` | as above | |
| | `landing_path` | path (optional) | Override the default file path |
| `03_bronze_to_silver` | `date` | `YYYY-MM-DD` | Which Bronze `load_date` to process |
| | `batch_id` | UUID (optional) | Reprocess a single Bronze batch only |
| `05_run_stats` | `date` | `YYYY-MM-DD` | Which load date to report (blank = latest Bronze date) |

### 1. Standard incremental load (automatic)

Nothing to do. GitHub Actions fetches the data, uploads `steamspy_<today>.jsonl` and starts the Databricks job with:

```json
{"load_type": "incremental", "date": "<today UTC>"}
```

The job runs `02_raw_to_bronze` and then `03_bronze_to_silver`. Only new or changed games are written to Bronze, and Silver is upserted from them.

### 2. Initial full load

Run `04_pipeline_runner` manually with:

| Parameter | Value |
|-----------|-------|
| `load_type` | `full` |
| `date` | date of the baseline file |

A full load writes every row in the file to Bronze, with no change filtering.

### 3. Backfill (re-process any historical date)

**Raw to Bronze to Silver for a past date:**

1. Make sure the landing file for that date, `steamspy_<date>.jsonl`, is in the Volume. It has to be a file that was fetched on that date. The API only returns current data, so a past date cannot be fetched again later. Each daily file is also kept as a GitHub Actions artifact.
2. Run `04_pipeline_runner` with `load_type = incremental` and `date = <historical date>`.

**Bronze to Silver only** (for example after fixing a cleaning rule): run `03_bronze_to_silver` with `date = <date>`. Add `batch_id` to reprocess one batch only.

**Raw to Bronze from a non-standard file:** run `02_raw_to_bronze` with `landing_path = <path to file>`.

Backfills and re-runs are safe:
- Re-running the same date does not duplicate rows, because Bronze merges on (`appid`, `load_date`) and Silver on `appid`.
- Backfilling an older date cannot overwrite newer Silver data, because of the `load_date >= last_load_date` guard.

### 4. Check a run

- `05_run_stats` shows new vs changed games for a date, Bronze and Silver totals, and a duplicate-appid check (should be 0).
- Query the audit log:
  ```sql
  SELECT layer, load_type, file_processed, status, rows_inserted, rows_updated,
         rows_skipped_unchanged, rows_quarantined, error_message, start_time, end_time
  FROM workspace.steam.pipeline_execution_logs
  ORDER BY start_time DESC;
  ```

### Running the fetch script locally

```bash
pip install requests
python scripts/fetch_steamspy.py --date 2026-10-09
```

This writes `steamspy_2026-10-09.jsonl` with one game per line. It retries failed pages up to 4 times, waits 2 seconds between pages, and removes duplicate games that appear on more than one page. If `--date` is omitted, it uses today's date on the machine running it.

## Testing Schema Drift

`06_schema_drift_test` takes the first 5 games from `steamspy_2026-10-09.jsonl` in the Volume, adds an unexpected `genre` column and sets one `positive` value to non-numeric text, then runs the file through both layers under the test date `2026-10-01`.

**Before running:** that source file must already exist in the Volume, and the notebook checks Silver for five hardcoded sample appids (730, 1172470, 578080, 1623730, 440).

Expected result:

- `genre` is added to Bronze and logged in `schema_drift_log`
- the bad row is sent to `silver_quarantine`
- existing Silver values are not touched
- both runs appear in `pipeline_execution_logs`
- the test rows are deleted from Bronze at the end

**What the test leaves behind:** the `genre` column stays in `bronze_steam`, and the rows in `schema_drift_log` and `silver_quarantine` are not deleted.

## Test Results

These runs were carried out on the real data and are recorded in `pipeline_execution_logs`.

| Test | Run | Bronze | Silver |
|------|-----|--------|--------|
| Full load | 2026-10-09, `full`, 82,518 rows in file | 82,518 inserted | 82,040 inserted |
| Re-run of the same date | 2026-10-09 again | 0 inserted, 0 updated | 0 inserted, 0 updated |
| Incremental load | 2026-10-10, `incremental`, 82,521 rows in file | 6,674 inserted, 75,847 skipped | 26 inserted, 6,632 updated |
| Re-run of the same date | 2026-10-10 again | 0 inserted, 0 updated | 0 inserted, 0 updated |
| Backfill | 2026-10-09 run after 2026-10-10 was loaded | 0 inserted, 0 updated | 0 inserted, 0 updated |
| Schema drift | 5 rows with a new `genre` column and one text value in `positive` | 5 inserted, `genre` column added | 4 passed, 1 quarantined |
| Scheduled run | Workflow started from GitHub Actions on 2026-10-10 | Job triggered and succeeded | Job triggered and succeeded |

- Silver has fewer rows than the file on a full load because 478 dead entries were dropped during cleaning.
- Of the 6,674 rows loaded on 2026-10-10, 26 were new games and 6,648 were existing games whose values had changed.
- After these runs Bronze holds 89,192 rows and Silver holds 82,066 rows, with no duplicate `appid` in Silver.

## Notes and Limitations

- SteamSpy owner counts are estimated ranges, not exact figures.
- Data is a once-a-day snapshot, so changes between refreshes are not captured.
- The workflow fetches at 22:00 UTC. File names and `load_date` use the UTC date of the fetch.
- There is no delete step. A game that disappears from the API stays in Silver with the last values that were loaded.
- The Databricks token in the `DATABRICKS_TOKEN` secret has a limited lifetime and must be replaced when it expires.
- Rows dropped as dead entries (0 reviews and 0 concurrent users) are not logged anywhere, so Bronze and Silver counts will not reconcile exactly with the quarantine and skipped counts.
- `JOB_ID` is hardcoded in `daily.yml` and must be changed for a different Databricks workspace.

## Roadmap: Phase 3

Planned Gold layer (business-ready tables for the Power BI dashboard):

| Table | Purpose |
|-------|---------|
| `gold_publisher_summary` | Games per publisher, average approval rate, total estimated owners, average price |
| `gold_top_games` | Top 500 games by concurrent players, with price, approval rate and ownership estimates |
| `gold_price_tier_analysis` | Free / under $5 / $5 to $20 / above $20 brackets with average approval rate and player count |
| `gold_daily_ccu_trends` | Concurrent players per game over time, built from the daily loads |

Planned Power BI dashboard visuals:
1. Top 15 publishers by combined player base, with average approval rate
2. Price vs. approval rate, sized by concurrent players
3. Daily concurrent player trends for the top 10 games

## Team

Built as a pair project for a Data Analysis and Visualization course.

| Name | GitHub |
|------|--------|
| Ghulam Murtaza | [@Murtazagonhit10](https://github.com/Murtazagonhit10) |
| Aima Shakeel | [@aimashakeelsaddozai-code](https://github.com/aimashakeelsaddozai-code) |

## Data Source

Data comes from the [SteamSpy API](https://steamspy.com/api.php). SteamSpy refreshes its aggregates once a day, so stats such as concurrent users (`ccu`) and review counts change slowly for the biggest games.
