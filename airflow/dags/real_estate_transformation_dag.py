
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# Resolve repository paths safely for both local and containerized Airflow.
_THIS_FILE = Path(__file__).resolve()
REPO_ROOT = Path(os.getenv("REPO_ROOT", _THIS_FILE.parents[2]))
SCRIPTS_DIR = REPO_ROOT / "scripts"
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", REPO_ROOT / "zillow_transformation"))
DBT_PROFILES_DIR = Path(
    os.getenv("DBT_PROFILES_DIR", DBT_PROJECT_DIR)
)  # defaults to project dir

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from load_s3_to_postgres import load_csv_from_s3_to_postgres  # noqa: E402


def _load_raw_from_s3(**context: dict) -> None:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}

    s3_file_key = dag_conf.get("s3_file_key") or os.getenv("S3_FILE_KEY")
    if not s3_file_key:
        raise ValueError(
            "Missing S3 key. Set dag_run.conf['s3_file_key'] or S3_FILE_KEY env var."
        )

    load_csv_from_s3_to_postgres(
        s3_file_key=s3_file_key,
        s3_bucket=os.getenv("S3_BUCKET"),
        table_name=os.getenv("POSTGRES_TABLE", "property_master_data"),
        schema=os.getenv("POSTGRES_SCHEMA", "raw"),
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="real_estate_transformation",
    description="Load Zillow snapshots from S3 and run dbt transformations/tests",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["real-estate", "dbt", "postgres", "s3"],
) as dag:
    load_raw_data = PythonOperator(
        task_id="load_raw_s3_to_postgres",
        python_callable=_load_raw_from_s3,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            "cd {{ params.dbt_project_dir }} "
            "&& dbt deps --project-dir {{ params.dbt_project_dir }} "
            "--profiles-dir {{ params.dbt_profiles_dir }}"
        ),
        params={
            "dbt_project_dir": str(DBT_PROJECT_DIR),
            "dbt_profiles_dir": str(DBT_PROFILES_DIR),
        },
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            "cd {{ params.dbt_project_dir }} "
            "&& dbt seed --project-dir {{ params.dbt_project_dir }} "
            "--profiles-dir {{ params.dbt_profiles_dir }}"
        ),
        params={
            "dbt_project_dir": str(DBT_PROJECT_DIR),
            "dbt_profiles_dir": str(DBT_PROFILES_DIR),
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd {{ params.dbt_project_dir }} "
            "&& dbt run --project-dir {{ params.dbt_project_dir }} "
            "--profiles-dir {{ params.dbt_profiles_dir }}"
        ),
        params={
            "dbt_project_dir": str(DBT_PROJECT_DIR),
            "dbt_profiles_dir": str(DBT_PROFILES_DIR),
        },
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd {{ params.dbt_project_dir }} "
            "&& dbt test --project-dir {{ params.dbt_project_dir }} "
            "--profiles-dir {{ params.dbt_profiles_dir }}"
        ),
        params={
            "dbt_project_dir": str(DBT_PROJECT_DIR),
            "dbt_profiles_dir": str(DBT_PROFILES_DIR),
        },
    )

    load_raw_data >> dbt_deps >> dbt_seed >> dbt_run >> dbt_test
