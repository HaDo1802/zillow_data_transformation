# Learning Note: Automating This Transformation Pipeline with Apache Airflow

## 1) What you asked for
You asked how to automate your transformation process using **"ariflow"** (Apache **Airflow**).

In this repository, your full pipeline is naturally:
1. Load raw Zillow CSV from S3 into PostgreSQL raw table (`scripts/load_s3_to_postgres.py`)
2. Run dbt models in `zillow_transformation/`
3. Run dbt tests to validate data quality

This note explains how that is automated with one Airflow DAG and how to operate it.

## 2) What was added
A new DAG file was added:
- `airflow/dags/real_estate_transformation_dag.py`

DAG id:
- `real_estate_transformation`

Task order:
1. `load_raw_s3_to_postgres`
2. `dbt_deps`
3. `dbt_seed`
4. `dbt_run`
5. `dbt_test`

Schedule:
- `@daily`

Manual trigger support:
- pass `s3_file_key` via `dag_run.conf`

## 3) How this DAG works (mapped to your current code)

### Task 1: `load_raw_s3_to_postgres`
- Uses your existing function `load_csv_from_s3_to_postgres(...)` from `scripts/load_s3_to_postgres.py`.
- Reads the file key in this priority:
  1. `dag_run.conf["s3_file_key"]`
  2. environment variable `S3_FILE_KEY`
- Loads data into:
  - schema: `POSTGRES_SCHEMA` (default `raw`)
  - table: `POSTGRES_TABLE` (default `property_master_data`)
- Adds metadata (`ingested_time`, `snapshot_date`, `source_file`) and de-duplicates with a unique index.

### Task 2-5: dbt orchestration
- `dbt deps`: install dbt packages
- `dbt seed`: load seed files (like district mapping)
- `dbt run`: build silver and gold models
- `dbt test`: run schema/data tests

All dbt commands run against:
- project dir: `DBT_PROJECT_DIR` (default: `<repo>/zillow_transformation`)
- profiles dir: `DBT_PROFILES_DIR` (default: same as project dir)

## 4) Required environment variables
At runtime (Airflow worker/scheduler environment), set at least:

- AWS / S3:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`
  - `S3_BUCKET`

- Postgres:
  - `POSTGRES_HOST`
  - `POSTGRES_DB`
  - `POSTGRES_USER`
  - `POSTGRES_PASSWORD`
  - `POSTGRES_PORT`
  - `POSTGRES_TABLE` (optional default exists)
  - `POSTGRES_SCHEMA` (optional default exists)

- dbt / pathing:
  - `REPO_ROOT` (optional)
  - `DBT_PROJECT_DIR` (optional)
  - `DBT_PROFILES_DIR` (optional)

- Optional for scheduled runs without manual config:
  - `S3_FILE_KEY`

## 5) dbt profile reminder (important)
Your dbt project references profile name:
- `profile: zillow_transformation` in `zillow_transformation/dbt_project.yml`

So your `profiles.yml` (inside `DBT_PROFILES_DIR`) must contain a matching profile key:
- `zillow_transformation`

Minimal structure example:

```yaml
zillow_transformation:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <POSTGRES_HOST>
      user: <POSTGRES_USER>
      password: <POSTGRES_PASSWORD>
      port: 5432
      dbname: <POSTGRES_DB>
      schema: public
      threads: 4
```

If this profile is missing, `dbt run/test` tasks will fail.

## 6) How to trigger the DAG

### Option A: Airflow UI (recommended)
1. Open Airflow UI
2. Enable DAG `real_estate_transformation`
3. Click "Trigger DAG"
4. Provide JSON config with S3 key

Example config:

```json
{
  "s3_file_key": "raw/raw_20260207_20260207_2306.csv"
}
```

### Option B: Airflow CLI

```bash
airflow dags trigger real_estate_transformation \
  --conf '{"s3_file_key":"raw/raw_20260207_20260207_2306.csv"}'
```

## 7) Typical production schedule pattern
For real usage, this pattern is common:
1. Scraper/extractor lands new file to S3
2. Airflow triggers this DAG (event-based or scheduled)
3. Raw load happens first
4. dbt builds silver/gold
5. dbt tests gate quality
6. Optional: send alert if tests fail

Right now your DAG is daily (`@daily`). Later, you can switch to:
- Cron schedule (for specific time windows)
- Dataset/event-triggered workflows

## 8) Operations checklist
Before each run:
1. Confirm S3 key exists in bucket
2. Confirm Airflow worker can reach Postgres
3. Confirm `profiles.yml` exists and matches profile name
4. Confirm dbt and adapters are installed in Airflow runtime

After each run:
1. Check task logs for row counts from `load_raw_s3_to_postgres`
2. Check `dbt run` summary (models built/updated)
3. Check `dbt test` summary (pass/fail)

## 9) Common failure modes and fixes

### Error: missing `s3_file_key`
Cause:
- neither DAG conf nor `S3_FILE_KEY` is set

Fix:
- trigger with `{"s3_file_key":"..."}` or set env var

### Error: dbt profile not found
Cause:
- `profiles.yml` not in `DBT_PROFILES_DIR`

Fix:
- place `profiles.yml` in that directory and ensure profile key is `zillow_transformation`

### Error: dbt package not found
Cause:
- `dbt deps` did not run or failed

Fix:
- inspect `dbt_deps` logs and rerun task

### Error: duplicate/raw conflicts in load
Cause:
- data already exists for the unique key (`zpid`, `extracted_at`, `price`)

Fix:
- usually no action required; duplicates are skipped by design

### Error: connectivity/auth issues
Cause:
- AWS credentials/Postgres credentials incorrect or not visible in worker env

Fix:
- validate env propagation in Airflow deployment

## 10) Why Airflow is useful here
With this DAG, you get:
- repeatable orchestration (same order every run)
- observability (task-level logs and status)
- retry behavior for transient issues
- one place to monitor extraction + transformation + testing

This is much stronger than manually running scripts and dbt commands.

## 11) Suggested next upgrades
1. Add data freshness sensor before dbt run
2. Add Slack/email alerts on task failure
3. Add task-level SLA and timeout settings
4. Add separate DAGs for "ingestion" and "transform" with dataset dependency
5. Parameterize dbt selection (`--select`) for faster partial runs
6. Add source freshness check (`dbt source freshness`) as a dedicated task

## 12) Files to review now
- `airflow/dags/real_estate_transformation_dag.py`
- `scripts/load_s3_to_postgres.py`
- `zillow_transformation/dbt_project.yml`
- `zillow_transformation/models/sources.yml`

---

## Quick command reference

```bash
# list DAGs
airflow dags list

# test a single task locally (example)
airflow tasks test real_estate_transformation load_raw_s3_to_postgres 2026-02-12

# trigger full DAG with runtime config
airflow dags trigger real_estate_transformation \
  --conf '{"s3_file_key":"raw/raw_20260207_20260207_2306.csv"}'
```

That is the practical foundation to automate your real estate transformation workflow with Airflow in this repo.

## 13) Running this pipeline with Docker (recommended for weak local setup)
You now have a Docker-based Airflow stack in this repo.

### Added files
- `docker-compose.airflow.yml`
- `docker/airflow/Dockerfile`
- `docker/airflow/requirements-airflow.txt`
- `docker/README.md`
- `zillow_transformation/profiles.yml.example`

### Quick start
1. `cp zillow_transformation/profiles.yml.example zillow_transformation/profiles.yml`
2. `docker compose -f docker-compose.airflow.yml build`
3. `docker compose -f docker-compose.airflow.yml up airflow-init`
4. `docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler`
5. Open Airflow at `http://localhost:8080`

### Why this helps your machine
- Environment drift is reduced (all deps are inside image).
- You avoid local Python/dbt/Airflow conflicts.
- You can later move the same compose/image approach to a stronger host.

## Redshift debug
- 

## 14) Debug Note: `psycopg2.errors.NumericValueOutOfRange: bigint out of range`

This happened during your one-time bulk load from Supabase Storage CSV files into table `raw.raw_property_master_data`.

### What this error means in simple words
Postgres has fixed numeric limits for each integer type.

- `smallint`: -32,768 to 32,767
- `integer`: -2,147,483,648 to 2,147,483,647
- `bigint`: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807

If a CSV value is bigger than what the target column allows, Postgres rejects the insert and throws:

`NumericValueOutOfRange: bigint out of range`

### Why you saw it only on file `[3/13]`
Your loader was processing files in sequence.

1. File 1 and 2 had valid numeric values for mapped integer columns.
2. File 3 (`raw/raw_20260210_20260210_0632.csv`) contained at least one value that overflowed a numeric column in the target table.
3. The insert batch failed at `execute_values(...)`, so the script stopped.

So the script logic was fine, but one or more source values did not fit the destination numeric type.

### Realistic examples

Example A: large identifier accidentally treated as bigint

- CSV value: `100000000000000000000` (20 digits)
- Target column type: `bigint`
- Result: fail, because this exceeds bigint max (`9223372036854775807`)

Example B: exponential notation from CSV parsing

- CSV value: `9.999999999999999e+25`
- Target column type: `bigint`
- Result: fail, way outside bigint range

Example C: mixed dirty data in one column

- Most rows: normal integers
- Some rows: very large numbers, bad strings, non-integer forms
- Insert works for many rows, then crashes when one bad row is in a batch

### Why this is common in CSV pipelines
CSV has no strict schema by itself.

1. Producer side may export numbers without strict typing.
2. Pandas may infer types differently between files.
3. Destination table has strict Postgres types.
4. Any row violating target type causes batch insert failure.

### What we changed in `scripts/load_file_to_database.py`
We added two layers of protection.

#### Layer 1: pre-sanitize integer columns using table metadata
Before insert, the script queries `information_schema.columns` for integer-like columns in the target table and sanitizes values.

- Non-integer or out-of-range values are converted to `NULL`
- Valid values remain

Functions involved:

- `_get_integer_column_types(...)`
- `_sanitize_integer_columns_for_postgres(...)`

#### Layer 2: runtime fallback if a batch still fails
Even after pre-sanitize, if a batch still hits `NumericValueOutOfRange`, we now:

1. rollback only that failed batch
2. retry rows one-by-one
3. for failing rows, apply focused repair (`_coerce_value_for_bigint_overflow(...)`)
4. retry insert for repaired row
5. if still failing, skip row and continue

This prevents one bad row from killing the full 13-file one-time load.

### Why the first fix did not fully solve it
Your first run still failed because overflow can appear in shapes that are not fully caught by simple column-level coercion alone. Batch insert can still encounter edge cases.

The row-level fallback is the robust safety net for one-time ingestion.

### Data quality tradeoff
Converting out-of-range values to `NULL` means:

- Pro: pipeline continues, you do not lose entire file
- Pro: one-time historical load can complete
- Con: specific invalid numeric values are not preserved as numbers
- Con: if column is business-critical, you should later investigate those rows

For one-time backfill, this is usually the right pragmatic choice.

### How to run now

```bash
python scripts/load_file_to_database.py --all --prefix raw
```

Expected log patterns:

- `Batch ... hit bigint overflow; retrying row-by-row with repair.`
- `Repaired X row(s) with bigint overflow values`
- `Skipped Y bad row(s) that still failed after repair` (only if unrecoverable)
- final bulk summary with total inserted rows

### What to check after run

1. Check row counts in target table.
2. Check logs for repaired/skipped row counts.
3. Spot-check suspicious numeric columns for `NULL` spikes after the load.

Example SQL checks:

```sql
SELECT COUNT(*) AS total_rows
FROM raw.raw_property_master_data;
```

```sql
SELECT
  COUNT(*) AS null_price_rows
FROM raw.raw_property_master_data
WHERE price IS NULL;
```

```sql
SELECT
  source_file,
  COUNT(*) AS rows_loaded
FROM raw.raw_property_master_data
GROUP BY source_file
ORDER BY rows_loaded DESC;
```

### Long-term clean solution
For production-grade recurring loads, better options are:

1. enforce strict schema earlier before writing CSV
2. stage into all-text/raw landing table first
3. cast/validate in SQL transform layer with explicit rules
4. quarantine invalid rows into a separate error table for audit

For your current goal, one-time bulk load from existing files, the implemented fix is appropriate and safe enough to complete the ingestion.

## 15) Backfill + dbt Grain Fix (Detailed)

This section documents what changed after migrating many old files from S3 to Supabase and loading them in one shot.

### Problem observed
`dbt test` failures:
- `unique_int_zillow_property_history_property_sk`
- `dbt_utils_unique_combination_of_columns_fact_property_snapshot_property_id__snapshot_date`

Root cause:
1. One-time backfill loaded many files on the same current date.
2. Raw `snapshot_date` can reflect load date instead of true event date.
3. Multiple records existed for the same `property_id + snapshot_date`.
4. Existing keys/grain expected fewer duplicates than what backfill created.

### Why this happens in simple terms
Think of one property scraped many times:
- 2026-02-09 06:31
- 2026-02-09 08:10
- 2026-02-09 21:45

If model key uses only `(property_id, snapshot_date)`, all three are treated like one slot and collide.

### What we changed in dbt

#### A) Silver: use true event date
File:
- `zillow_transformation/models/silver/staging/stg_zillow_property_master.sql`

Change:
- `snapshot_date` now prefers `extracted_at::date` first.
- Fallback still uses raw `snapshot_date` when needed.

Why:
- `extracted_at` is the true timestamp of when data was captured.
- Backfill load date should not redefine business event date.

#### B) Silver: make event key unique enough
File:
- `zillow_transformation/models/silver/staging/stg_zillow_property_master.sql`

Change:
- `property_sk` now uses:
  - `zillow_property_id`
  - `snapshot_date`
  - `extracted_at`

Why:
- Multiple captures on same day must remain uniquely represented in silver history.

#### C) Gold snapshot fact: keep one row per property/day
File:
- `zillow_transformation/models/gold/facts/fact_property_snapshot.sql`

Change:
- Added `row_number()` partitioned by `(property_id, snapshot_date)`
- Ordered by latest `extracted_at`, then latest `digested_time`
- Kept only `row_num = 1`

Why:
- Gold snapshot fact test expects one row per property per day.
- For intraday duplicates, keep latest daily state.

### Grain definitions after fix

#### Silver (`int_zillow_property_history`)
- Grain: one row per property event (can have multiple rows per day).
- Key: `property_sk` based on property + day + extracted timestamp.

#### Gold snapshot (`fact_property_snapshot`)
- Grain: one row per property per snapshot date.
- If multiple events exist in same day, latest event wins.

#### Gold latest (`fact_property_latest`)
- Grain: one row per property (latest overall).

### Row count expectations
Do not expect same row counts.

Usually:
- `fact_property_snapshot` >= `fact_property_latest`

Equal only when each property appears on exactly one snapshot date.

### Example
Property `123` appears:
- 2026-02-09 06:31, price 500k
- 2026-02-09 18:00, price 495k
- 2026-02-10 07:00, price 490k

Result:
- Silver history: 3 rows (all events kept)
- Gold snapshot: 2 rows (2026-02-09 keeps latest event, and 2026-02-10)
- Gold latest: 1 row (2026-02-10)

### Commands to rebuild safely after this change
Use full refresh because incremental tables may already hold old-grain data.

```bash
dbt run --full-refresh --project-dir zillow_transformation --profiles-dir zillow_transformation
dbt test --project-dir zillow_transformation --profiles-dir zillow_transformation
```

### Daily production behavior going forward
This design is stable for regular ETL runs:
1. New daily snapshots append in raw.
2. Silver preserves event history.
3. Gold snapshot keeps one row/property/day (latest daily state).
4. Gold latest stays one row/property.

If you later need intraday analytics in gold, change snapshot fact grain to include `extracted_at` and update related uniqueness tests accordingly.

## 16) GitHub Actions + Supabase DB Connectivity Note

This section documents a CI-specific issue when running:

```bash
python scripts/load_file_to_database.py
```

### Symptom A
Error in GitHub Actions:

`psycopg2.OperationalError: ... Network is unreachable`

Observed with DB host resolving to an IPv6 address (`2600:...`).

### Root cause A
The Supabase **Direct connection** endpoint is not IPv4-compatible in this setup.  
GitHub-hosted runners could not reach that IPv6 path.

### Symptom B
After switching host/user:

`FATAL: Tenant or user not found`

### Root cause B
Connection settings were mixed across methods (pooler host/user + direct port/password).  
Supabase pooler expects a fully consistent parameter set.

### Correct fix for GitHub Actions
Use one complete **Session Pooler** connection set from Supabase Dashboard (do not mix with Direct connection values):

1. `SUPABASE_DB_HOST` = session pooler host (example: `aws-1-us-west-1.pooler.supabase.com`)
2. `SUPABASE_DB_PORT` = session pooler port
3. `SUPABASE_DB_USER` = session pooler user (example format: `postgres.<project_ref>`)
4. `SUPABASE_DB_PASSWORD` = DB password for that project/user
5. `SUPABASE_DB_NAME` = usually `postgres`
6. `SUPABASE_DB_SSLMODE` = `require`

### Practical rule
- Local laptop can still use Direct connection if it works there.
- GitHub Actions should use Session Pooler unless you have Supabase IPv4 add-on for Direct connection.
