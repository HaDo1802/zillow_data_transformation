# Airflow + dbt in Docker

## Why this setup
This runs your orchestration stack in containers so your host machine only needs Docker.

## Files
- `docker-compose.yml`: Airflow stack
- `docker/airflow/Dockerfile`: custom Airflow image with dbt + Python deps
- `docker/airflow/requirements-airflow.txt`: pinned dependencies
- `zillow_transformation/profiles.yml.example`: dbt profile template

## One-time setup
1. Confirm `.env` is present and includes your Supabase credentials plus the `AIRFLOW_POSTGRES_*` variables used by Docker Compose.
2. Copy dbt profile template:
```bash
cp zillow_transformation/profiles.yml.example zillow_transformation/profiles.yml
```
3. Build image:
```bash
docker compose build
```
4. Initialize Airflow DB + admin user:
```bash
docker compose up airflow-init
```

## Start services
```bash
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow UI:
- http://localhost:8080
- username/password come from `.env`:
  - `AIRFLOW_WWW_USER_USERNAME`
  - `AIRFLOW_WWW_USER_PASSWORD`

## Trigger DAG manually
```bash
docker compose exec airflow-scheduler \
  airflow dags trigger real_estate_transformation \
  --conf '{"storage_file_path":"raw/raw_20260207_20260207_2306.csv"}'
```

## Logs
```bash
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-webserver
```

## Stop
```bash
docker compose down
```

## Notes for your Supabase DB setup
- In Docker, `localhost` is the container itself.
- For Supabase, use `.env` values for `SUPABASE_DB_HOST`, `SUPABASE_DB_PORT`, `SUPABASE_DB_NAME`, `SUPABASE_DB_USER`, `SUPABASE_DB_PASSWORD`, and `SUPABASE_DB_SSLMODE=require`.
- Keep `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, and `SUPABASE_STORAGE_BUCKET` set so Airflow can download CSV files from Supabase Storage.
