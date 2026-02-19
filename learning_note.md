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