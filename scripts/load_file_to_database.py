import os
from datetime import datetime, timezone
from io import BytesIO
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from supabase import create_client

load_dotenv()


def _create_engine():
    host = os.getenv("SUPABASE_DB_HOST")
    port = os.getenv("SUPABASE_DB_PORT", "5432")
    dbname = os.getenv("SUPABASE_DB_NAME", "postgres")
    user = os.getenv("SUPABASE_DB_USER")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    sslmode = os.getenv("SUPABASE_DB_SSLMODE", "require")

    db_url = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=dbname,
    )

    return create_engine(db_url, connect_args={"sslmode": sslmode})


def _create_connection():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        database=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        port=os.getenv("SUPABASE_DB_PORT", "5432"),
        sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
    )


def _ensure_schema(engine, schema: str):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def _ensure_unique_index(conn, table_name: str, schema: str | None, columns: list[str]):
    if not columns:
        raise ValueError("Unique index columns are required")

    index_name = f"{table_name}_{'_'.join(columns)}_uniq"
    table_ident = (
        sql.Identifier(schema, table_name) if schema else sql.Identifier(table_name)
    )
    index_ident = sql.Identifier(index_name)
    column_idents = [sql.Identifier(c) for c in columns]

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "CREATE UNIQUE INDEX IF NOT EXISTS {index} ON {table} ({cols})"
            ).format(
                index=index_ident,
                table=table_ident,
                cols=sql.SQL(", ").join(column_idents),
            )
        )
    conn.commit()


def load_csv_from_supabase_storage_to_table(
    storage_file_path: str | None = None,
    storage_bucket: str | None = None,
    table_name: str | None = None,
    schema: str | None = None,
) -> int:
    """Download a CSV from Supabase Storage and idempotently load it into Supabase Postgres."""
    storage_file_path = storage_file_path
    storage_bucket = storage_bucket or os.getenv("SUPABASE_STORAGE_BUCKET")
    table_name = table_name or os.getenv("SUPABASE_RAW_TABLE")
    schema = schema or os.getenv("SUPABASE_SCHEMA")

    if not storage_file_path:
        raise ValueError(
            "Missing file path: set storage_file_path or SUPABASE_FILE_PATH"
        )
    if not storage_bucket:
        raise ValueError(
            "Missing bucket: set storage_bucket or SUPABASE_STORAGE_BUCKET"
        )
    if not table_name:
        raise ValueError("Missing target table: set table_name or SUPABASE_RAW_TABLE")

    supabase = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
    )

    print(f"Downloading '{storage_file_path}' from bucket '{storage_bucket}'...")
    content = supabase.storage.from_(storage_bucket).download(storage_file_path)
    if not content:
        raise ValueError(
            f"Could not download '{storage_file_path}' from '{storage_bucket}'"
        )

    df = pd.read_csv(BytesIO(content))
    print(f"Read {len(df)} rows and {len(df.columns)} columns")

    now_utc = datetime.now(timezone.utc)
    df["ingested_time"] = now_utc
    df["snapshot_date"] = now_utc.strftime("%Y%m%d")
    df["source_file"] = storage_file_path

    unique_cols = ["zpid", "extracted_at", "price"]
    missing = [col for col in unique_cols if col not in df.columns]
    if missing:
        raise ValueError(f"Unique key columns not found in CSV: {missing}")

    engine = _create_engine()
    conn = _create_connection()

    _ensure_schema(engine, schema)
    df.head(0).to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists="append",
        index=False,
    )
    _ensure_unique_index(conn, table_name, schema, unique_cols)

    qualified_table = f"{schema}.{table_name}"
    print(f"Idempotent loading into '{qualified_table}'...")

    table_ident = (
        sql.Identifier(schema, table_name) if schema else sql.Identifier(table_name)
    )
    col_idents = [sql.Identifier(col) for col in df.columns]
    conflict_cols = [sql.Identifier(col) for col in unique_cols]
    insert_stmt = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT ({conflict_cols}) DO NOTHING"
    ).format(
        table=table_ident,
        cols=sql.SQL(", ").join(col_idents),
        conflict_cols=sql.SQL(", ").join(conflict_cols),
    )

    rows = list(df.itertuples(index=False, name=None))
    batch_size = 1000
    inserted = 0
    with conn.cursor() as cur:
        for start in range(0, len(rows), batch_size):
            batch = rows[start : start + batch_size]
            execute_values(cur, insert_stmt, batch, page_size=len(batch))
            inserted += cur.rowcount
    conn.commit()

    skipped = len(df) - inserted
    print(f"Done: inserted {inserted} rows into '{qualified_table}'")
    if skipped > 0:
        print(f"Skipped {skipped} duplicate rows")

    conn.close()
    engine.dispose()
    return inserted


if __name__ == "__main__":
    load_csv_from_supabase_storage_to_table(
        storage_file_path="raw/raw_20260219_20260219_0644.csv"
    )
