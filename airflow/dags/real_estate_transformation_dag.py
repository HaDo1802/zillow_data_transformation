
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

from load_supabase_to_supabase import load_csv_from_supabase_storage_to_table  # noqa: E402


def _load_raw_from_supabase_storage(**context: dict) -> None:
    dag_run = context.get("dag_run")
    dag_conf = dag_run.conf if dag_run and dag_run.conf else {}

    storage_file_path = (
        dag_conf.get("storage_file_path")
        or os.getenv("SUPABASE_FILE_PATH")
    )
    if not storage_file_path:
        raise ValueError(
            "Missing storage file path. Set dag_run.conf['storage_file_path'] or SUPABASE_FILE_PATH env var."
        )

    load_csv_from_supabase_storage_to_table(
        storage_file_path=storage_file_path,
        storage_bucket=os.getenv("SUPABASE_STORAGE_BUCKET"),
        table_name=os.getenv("SUPABASE_RAW_TABLE", "property_master_data"),
        schema=os.getenv("SUPABASE_SCHEMA", "public"),
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
    description="Load Zillow snapshots from Supabase Storage and run dbt transformations/tests",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 2, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["real-estate", "dbt", "postgres", "supabase"],
) as dag:
    load_raw_data = PythonOperator(
        task_id="load_raw_supabase_storage_to_table",
        python_callable=_load_raw_from_supabase_storage,
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
