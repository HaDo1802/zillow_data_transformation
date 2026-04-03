"""
Bronze loader — Supabase Storage CSV -> raw.raw_property_master_data

Idempotent at the row level via ON CONFLICT DO NOTHING.
Safe against partial loads — retrying a failed file completes it correctly.

Usage:
    python scripts/load_bronze.py --file raw/raw_20260207.csv
    python scripts/load_bronze.py --all
    python scripts/load_bronze.py --latest
    python scripts/load_bronze.py --all --prefix raw
"""

import argparse
import json
import os
from io import BytesIO, StringIO
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------

TARGET_TABLE = "raw.raw_property_master_data"

COLS = [
    "source_file",
    "extracted_at",
    "snapshot_date",
    "ingested_at",
    "zpid",
    "price",
    "pricechange",
    "zestimate",
    "rentzestimate",
    "propertytype",
    "bedrooms",
    "bathrooms",
    "livingarea",
    "lotareavalue",
    "lotareaunit",
    "listingstatus",
    "listingsubtype",
    "daysonzillow",
    "datepricechanged",
    "comingsoononmarketdate",
    "contingentlistingtype",
    "brokername",
    "address",
    "latitude",
    "longitude",
    "has3dmodel",
    "hasimage",
    "hasvideo",
]

COLS_SQL = ", ".join(COLS)
DEFAULT_DB_CONN_ID = os.getenv("AIRFLOW_DB_CONN_ID", "supabase_postgres")
DEFAULT_STORAGE_CONN_ID = os.getenv("AIRFLOW_STORAGE_CONN_ID", "supabase_storage")
DEFAULT_LATEST_POINTER_PATH = os.getenv(
    "SUPABASE_LATEST_POINTER_PATH", "raw/_latest.json"
)

# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------


def _get_airflow_connection(conn_id: str):
    if not conn_id:
        return None

    try:
        from airflow.hooks.base import BaseHook
    except ImportError:  # local non-Airflow execution
        return None

    try:
        return BaseHook.get_connection(conn_id)
    except Exception:
        return None


def get_conn(conn_id: Optional[str] = None) -> psycopg2.extensions.connection:
    env_host = os.getenv("SUPABASE_DB_HOST")
    env_user = os.getenv("SUPABASE_DB_USER")
    env_password = os.getenv("SUPABASE_DB_PASSWORD")

    if env_host and env_user and env_password:
        return psycopg2.connect(
            host=env_host,
            database=os.getenv("SUPABASE_DB_NAME", "postgres"),
            user=env_user,
            password=env_password,
            port=os.getenv("SUPABASE_DB_PORT", "5432"),
            sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
        )

    conn = _get_airflow_connection(conn_id or DEFAULT_DB_CONN_ID)
    if conn is None:
        raise ValueError("No Postgres credentials found in env vars or Airflow connection.")

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


def get_storage(conn_id: Optional[str] = None):
    env_url = os.getenv("SUPABASE_URL")
    env_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if env_url and env_key:
        return create_client(env_url, env_key)

    conn = _get_airflow_connection(conn_id or DEFAULT_STORAGE_CONN_ID)
    if conn is None:
        raise ValueError("No Supabase storage credentials found in env vars or Airflow connection.")

    api_url = conn.host
    if api_url and not api_url.startswith("http"):
        api_url = f"https://{api_url}"
    api_key = conn.password
    if not api_url or not api_key:
        raise ValueError(
            f"Airflow connection '{conn.conn_id}' must define host and password."
        )
    return create_client(api_url, api_key)


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------


def _get_storage_bucket(conn_id: Optional[str] = None) -> str:
    env_bucket = os.getenv("SUPABASE_STORAGE_BUCKET")
    if env_bucket:
        return env_bucket

    conn = _get_airflow_connection(conn_id or DEFAULT_STORAGE_CONN_ID)
    if conn is not None:
        bucket = conn.extra_dejson.get("bucket") or conn.extra_dejson.get(
            "storage_bucket"
        )
        if bucket:
            return bucket
    raise ValueError("No Supabase storage bucket found in env vars or Airflow connection.")


def list_files(prefix: str = "raw", storage_conn_id: Optional[str] = None) -> list[str]:
    bucket = _get_storage_bucket(storage_conn_id)
    objects = (
        get_storage(storage_conn_id)
        .storage.from_(bucket)
        .list(
            path=prefix,
            options={"limit": 1000, "sortBy": {"column": "name", "order": "asc"}},
        )
    )
    return [
        f"{prefix}/{o['name']}" for o in objects if o.get("name", "").endswith(".csv")
    ]


def download(path: str, storage_conn_id: Optional[str] = None) -> pd.DataFrame:
    content = download_bytes(path, storage_conn_id=storage_conn_id)
    if not content:
        raise ValueError(f"empty response: {path}")
    return pd.read_csv(BytesIO(content))


def download_bytes(path: str, storage_conn_id: Optional[str] = None) -> bytes:
    bucket = _get_storage_bucket(storage_conn_id)
    content = get_storage(storage_conn_id).storage.from_(bucket).download(path)
    if not content:
        raise ValueError(f"empty response: {path}")
    return content


def resolve_latest_file(
    prefix: str = "raw",
    storage_conn_id: Optional[str] = None,
    latest_pointer_path: str = DEFAULT_LATEST_POINTER_PATH,
) -> str:
    payload = download_bytes(latest_pointer_path, storage_conn_id=storage_conn_id)
    document = json.loads(payload.decode("utf-8"))

    candidate = (
        document.get("raw_file")
        or document.get("path")
        or document.get("file")
        or document.get("latest_file")
        or document.get("key")
    )
    if not candidate:
        raise ValueError(
            f"{latest_pointer_path} must contain one of: raw_file, path, file, latest_file, key"
        )
    if "/" not in candidate:
        return f"{prefix}/{candidate}"
    return candidate


# ---------------------------------------------------------------------------
# preparation
# ---------------------------------------------------------------------------


def prepare(df: pd.DataFrame, source_file: str) -> StringIO:
    """
    Normalize column names, inject source_file, write to in-memory CSV.
    No type coercion — everything stays text, casting is silver's job.
    """
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()
    df["source_file"] = source_file
    df["ingested_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    extracted_ts = pd.to_datetime(df.get("extracted_at"), errors="coerce", utc=True)
    df["snapshot_date"] = extracted_ts.dt.strftime("%Y-%m-%d")

    for col in COLS:
        if col not in df.columns:
            df[col] = None

    buf = StringIO()
    df[COLS].to_csv(buf, index=False)
    buf.seek(0)
    return buf


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------


def insert(conn, buf: StringIO, n_rows: int) -> tuple[int, int]:
    """
    1. COPY into temp table — fast, no constraints
    2. INSERT into target  — skip duplicates via ON CONFLICT
    Returns (inserted, skipped).
    """
    temp_cols = COLS_SQL.replace(", ", " text, ") + " text"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            create temp table bronze_stg ({temp_cols})
            on commit drop;
        """
        )

        cur.copy_expert(
            f"copy bronze_stg ({COLS_SQL}) "
            f"from stdin with (format csv, header true);",
            buf,
        )

        cur.execute(
            f"""
            insert into {TARGET_TABLE} ({COLS_SQL})
            select {COLS_SQL} from bronze_stg
            on conflict on constraint raw_property_master_data_uk do nothing;
        """
        )
        inserted = cur.rowcount

    conn.commit()
    return inserted, n_rows - inserted


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------


def load_one(
    path: str,
    db_conn_id: Optional[str] = None,
    storage_conn_id: Optional[str] = None,
) -> dict:
    print("  downloading...")
    df = download(path, storage_conn_id=storage_conn_id)
    buf = prepare(df, source_file=path)
    print(f"  {len(df)} rows in file")

    conn = get_conn(conn_id=db_conn_id)
    try:
        inserted, skipped = insert(conn, buf, len(df))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"  {inserted} inserted | {skipped} skipped (already exist)")
    return {"path": path, "inserted": inserted, "skipped": skipped}


def load_all(
    prefix: str = "raw",
    db_conn_id: Optional[str] = None,
    storage_conn_id: Optional[str] = None,
) -> None:
    paths = list_files(prefix, storage_conn_id=storage_conn_id)
    if not paths:
        print(f"no CSV files found under {prefix}/")
        return

    print(f"found {len(paths)} files\n")

    total_inserted = 0
    total_skipped = 0
    failures = []

    for i, path in enumerate(paths, 1):
        print(f"[{i}/{len(paths)}] {path}")
        try:
            result = load_one(
                path,
                db_conn_id=db_conn_id,
                storage_conn_id=storage_conn_id,
            )
            total_inserted += result["inserted"]
            total_skipped += result["skipped"]
        except Exception as e:
            print(f"  FAILED: {e}")
            failures.append(path)

    print("\n--- summary ---")
    print(f"files processed : {len(paths) - len(failures)}/{len(paths)}")
    print(f"rows inserted   : {total_inserted}")
    print(f"rows skipped    : {total_skipped}  (already existed)")
    if failures:
        print(f"failures        : {len(failures)}")
        for f in failures:
            print(f"  {f}")


def load_latest(
    prefix: str = "raw",
    db_conn_id: Optional[str] = None,
    storage_conn_id: Optional[str] = None,
    latest_pointer_path: str = DEFAULT_LATEST_POINTER_PATH,
) -> dict:
    path = resolve_latest_file(
        prefix=prefix,
        storage_conn_id=storage_conn_id,
        latest_pointer_path=latest_pointer_path,
    )
    print(f"latest file selected: {path}\n")
    return load_one(
        path,
        db_conn_id=db_conn_id,
        storage_conn_id=storage_conn_id,
    )


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Zillow CSVs into bronze layer")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="single file e.g. raw/raw_20260207.csv")
    group.add_argument("--all", action="store_true", help="load all files")
    group.add_argument("--latest", action="store_true", help="load only the latest file")
    parser.add_argument("--prefix", default="raw", help="storage prefix (default: raw)")
    parser.add_argument(
        "--latest-pointer-path",
        default=DEFAULT_LATEST_POINTER_PATH,
        help=(
            "storage path to the JSON document that points to the latest raw file; "
            "default: _latest.json"
        ),
    )
    parser.add_argument(
        "--db-conn-id",
        default=DEFAULT_DB_CONN_ID,
        help="Airflow connection id for Postgres (default: supabase_postgres)",
    )
    parser.add_argument(
        "--storage-conn-id",
        default=DEFAULT_STORAGE_CONN_ID,
        help="Airflow connection id for Supabase Storage API (default: supabase_storage)",
    )
    args = parser.parse_args()

    if args.file:
        print(f"loading: {args.file}\n")
        load_one(
            args.file,
            db_conn_id=args.db_conn_id,
            storage_conn_id=args.storage_conn_id,
        )
    elif args.latest:
        load_latest(
            prefix=args.prefix,
            db_conn_id=args.db_conn_id,
            storage_conn_id=args.storage_conn_id,
            latest_pointer_path=args.latest_pointer_path,
        )
    else:
        load_all(
            prefix=args.prefix,
            db_conn_id=args.db_conn_id,
            storage_conn_id=args.storage_conn_id,
        )
