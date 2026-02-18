# Airflow + dbt in Docker

## Why this setup
This runs your orchestration stack in containers so your host machine only needs Docker.

## Files
- `docker-compose.airflow.yml`: Airflow stack
- `docker/airflow/Dockerfile`: custom Airflow image with dbt + Python deps
- `docker/airflow/requirements-airflow.txt`: pinned dependencies
- `zillow_transformation/profiles.yml.example`: dbt profile template

## One-time setup
1. Copy dbt profile template:
```bash
cp zillow_transformation/profiles.yml.example zillow_transformation/profiles.yml
```
2. Build image:
```bash
docker compose -f docker-compose.airflow.yml build
```
3. Initialize Airflow DB + admin user:
```bash
docker compose -f docker-compose.airflow.yml up airflow-init
```

## Start services
```bash
docker compose -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```

Airflow UI:
- http://localhost:8080
- username/password come from `.env`:
  - `AIRFLOW_WWW_USER_USERNAME`
  - `AIRFLOW_WWW_USER_PASSWORD`

## Trigger DAG manually
```bash
docker compose -f docker-compose.airflow.yml exec airflow-scheduler \
  airflow dags trigger real_estate_transformation \
  --conf '{"s3_file_key":"raw/raw_20260207_20260207_2306.csv"}'
```

## Logs
```bash
docker compose -f docker-compose.airflow.yml logs -f airflow-scheduler
docker compose -f docker-compose.airflow.yml logs -f airflow-webserver
```

## Stop
```bash
docker compose -f docker-compose.airflow.yml down
```

## Notes for your current Mac/local DB setup
- In Docker, `localhost` is the container itself.
- The compose file defaults `POSTGRES_HOST=host.docker.internal` so containers can reach your host PostgreSQL.
- If your Postgres is remote/cloud, set `POSTGRES_HOST` in `.env` to that endpoint instead.
