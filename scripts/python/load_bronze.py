"""
Bronze loader — Supabase Storage CSV -> raw.raw_property_master_data

Idempotent at the row level via ON CONFLICT DO NOTHING.
Safe against partial loads — retrying a failed file completes it correctly.

Usage:
    python scripts/load_bronze.py --file raw/raw_20260207.csv
    python scripts/load_bronze.py --all
    python scripts/load_bronze.py --all --prefix raw
"""

import argparse
import os
from io import BytesIO, StringIO

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

# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------


def get_conn() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        database=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        port=os.getenv("SUPABASE_DB_PORT", "5432"),
        sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
    )


def get_storage():
    return create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    )


# ---------------------------------------------------------------------------
# storage
# ---------------------------------------------------------------------------


def list_files(prefix: str = "raw") -> list[str]:
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET")
    objects = (
        get_storage()
        .storage.from_(bucket)
        .list(
            path=prefix,
            options={"limit": 1000, "sortBy": {"column": "name", "order": "asc"}},
        )
    )
    return [
        f"{prefix}/{o['name']}" for o in objects if o.get("name", "").endswith(".csv")
    ]


def download(path: str) -> pd.DataFrame:
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET")
    content = get_storage().storage.from_(bucket).download(path)
    if not content:
        raise ValueError(f"empty response: {path}")
    return pd.read_csv(BytesIO(content))


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


def load_one(path: str) -> dict:
    print(f"  downloading...")
    df = download(path)
    buf = prepare(df, source_file=path)
    print(f"  {len(df)} rows in file")

    conn = get_conn()
    try:
        inserted, skipped = insert(conn, buf, len(df))
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"  {inserted} inserted | {skipped} skipped (already exist)")
    return {"path": path, "inserted": inserted, "skipped": skipped}


def load_all(prefix: str = "raw") -> None:
    paths = list_files(prefix)
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
            result = load_one(path)
            total_inserted += result["inserted"]
            total_skipped += result["skipped"]
        except Exception as e:
            print(f"  FAILED: {e}")
            failures.append(path)

    print(f"\n--- summary ---")
    print(f"files processed : {len(paths) - len(failures)}/{len(paths)}")
    print(f"rows inserted   : {total_inserted}")
    print(f"rows skipped    : {total_skipped}  (already existed)")
    if failures:
        print(f"failures        : {len(failures)}")
        for f in failures:
            print(f"  {f}")


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Zillow CSVs into bronze layer")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="single file e.g. raw/raw_20260207.csv")
    group.add_argument("--all", action="store_true", help="load all files")
    parser.add_argument("--prefix", default="raw", help="storage prefix (default: raw)")
    args = parser.parse_args()

    if args.file:
        print(f"loading: {args.file}\n")
        load_one(args.file)
    else:
        load_all(prefix=args.prefix)
