"""
daily_refresh

Single production DAG for the local Zillow pipeline.

Flow:
1. Ensure the bronze raw contract exists.
2. Load raw CSV files from Supabase Storage into `raw.raw_property_master_data`.
3. Let Cosmos render and run the dbt graph for all `tag:daily` models.
4. Run source freshness checks after the dbt graph succeeds.

This file is intentionally self-contained so a data engineer can open one DAG
file and understand the entire orchestration story without jumping across
custom Airflow operators or manifest bootstrap modules.

Trigger config for manual runs:
{
  "load_mode": "latest" | "all" | "file",
  "file_path": "raw/raw_20260318_20260318.csv",
  "prefix": "raw"
}

Scheduled runs default to `load_mode=latest`.
"""

from __future__ import annotations

import importlib.util
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import psycopg2

from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.config import ExecutionConfig
from cosmos.constants import LoadMode

try:
    from airflow.hooks.base import BaseHook
except ImportError:  # pragma: no cover - local linting without Airflow installed
    BaseHook = None


AIRFLOW_HOME = Path(os.getenv("AIRFLOW_PROJECT_ROOT", "/opt/airflow"))
SCRIPTS_DIR = AIRFLOW_HOME / "scripts"
SQL_DIR = SCRIPTS_DIR / "sql"
PYTHON_SCRIPTS_DIR = SCRIPTS_DIR / "python"
DBT_PROJECT_DIR = AIRFLOW_HOME / "zillow_transformation"
PROFILES_YML = DBT_PROJECT_DIR / "profiles.yml"

DEFAULT_DB_CONN_ID = os.getenv("AIRFLOW_DB_CONN_ID", "supabase_postgres")
DEFAULT_STORAGE_CONN_ID = os.getenv("AIRFLOW_STORAGE_CONN_ID", "supabase_storage")
DEFAULT_DBT_TARGET = os.getenv("DBT_TARGET", "prod")
DEFAULT_RAW_PREFIX = os.getenv("SUPABASE_RAW_PREFIX", "raw")


def _get_airflow_connection(conn_id: str):
    if not conn_id or BaseHook is None:
        return None
    try:
        return BaseHook.get_connection(conn_id)
    except Exception:
        return None


def _get_postgres_connection(conn_id: str = DEFAULT_DB_CONN_ID):
    conn = _get_airflow_connection(conn_id)
    if conn is not None:
        return psycopg2.connect(
            host=conn.host,
            database=conn.schema or os.getenv("SUPABASE_DB_NAME", "postgres"),
            user=conn.login,
            password=conn.password,
            port=conn.port or os.getenv("SUPABASE_DB_PORT", "5432"),
            sslmode=conn.extra_dejson.get(
                "sslmode", os.getenv("SUPABASE_DB_SSLMODE", "require")
            ),
        )

    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        database=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        port=os.getenv("SUPABASE_DB_PORT", "5432"),
        sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
    )


def ensure_bronze_schema(conn_id: str = DEFAULT_DB_CONN_ID) -> None:
    sql_path = SQL_DIR / "bronze_setup.sql"
    sql_text = sql_path.read_text()

    conn = _get_postgres_connection(conn_id=conn_id)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_bronze_load(**context) -> None:
    module_path = PYTHON_SCRIPTS_DIR / "load_bronze.py"
    spec = importlib.util.spec_from_file_location("airflow_load_bronze", module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load bronze loader from {module_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    params = context.get("params", {})
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    load_mode = conf.get("load_mode", params.get("load_mode", "latest"))
    prefix = conf.get("prefix", params.get("prefix", DEFAULT_RAW_PREFIX))
    file_path = conf.get("file_path", params.get("file_path"))

    if load_mode == "all":
        module.load_all(
            prefix=prefix,
            db_conn_id=DEFAULT_DB_CONN_ID,
            storage_conn_id=DEFAULT_STORAGE_CONN_ID,
        )
        return

    if load_mode == "file":
        if not file_path:
            raise ValueError("load_mode=file requires file_path in dag_run.conf")
        module.load_one(
            file_path,
            db_conn_id=DEFAULT_DB_CONN_ID,
            storage_conn_id=DEFAULT_STORAGE_CONN_ID,
        )
        return

    if load_mode == "latest":
        module.load_latest(
            prefix=prefix,
            db_conn_id=DEFAULT_DB_CONN_ID,
            storage_conn_id=DEFAULT_STORAGE_CONN_ID,
        )
        return

    raise ValueError("load_mode must be one of: latest, all, file")


def _render_config() -> RenderConfig:
    return RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=["tag:daily"],
        emit_datasets=False,
    )


def _project_config() -> ProjectConfig:
    return ProjectConfig(
        dbt_project_path=str(DBT_PROJECT_DIR),
        partial_parse=False,
    )


def _dbt_command(command: str, target: str = DEFAULT_DBT_TARGET) -> str:
    return (
        f"cd {DBT_PROJECT_DIR} && "
        f"dbt deps && "
        f"dbt {command} --target {target}"
    )


with DAG(
    dag_id="daily_refresh",
    description="Daily raw ingestion and dbt transformation pipeline for Las Vegas real estate data",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,  # flip to True and set email once on a server
    },
    params={
        "load_mode": Param("latest", type="string", enum=["latest", "all", "file"]),
        "file_path": Param("", type="string"),
        "prefix": Param(DEFAULT_RAW_PREFIX, type="string"),
    },
    tags=["zillow", "dbt", "daily"],
    doc_md=__doc__,
) as dag:
    prepare_bronze_contract = PythonOperator(
        task_id="prepare_bronze_contract",
        python_callable=ensure_bronze_schema,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=run_bronze_load,
        retries=2,
    )

    dbt_daily = DbtTaskGroup(
        group_id="dbt_daily",
        project_config=_project_config(),
        profile_config=ProfileConfig(
            profile_name="zillow_transformation",
            target_name=DEFAULT_DBT_TARGET,
            profiles_yml_filepath=str(PROFILES_YML),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="dbt",
        ),
        render_config=_render_config(),
    )

    source_freshness = BashOperator(
        task_id="source_freshness",
        bash_command=_dbt_command("source freshness"),
    )

    prepare_bronze_contract >> load_bronze >> dbt_daily >> source_freshness
