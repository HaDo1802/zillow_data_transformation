import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import boto3
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()
# Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION")

# PostgreSQL connection details
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")


def connect_postgres(
    POSTGRES_HOST=POSTGRES_HOST,
    POSTGRES_DB=POSTGRES_DB,
    POSTGRES_USER=POSTGRES_USER,
    POSTGRES_PASSWORD=POSTGRES_PASSWORD,
    POSTGRES_PORT=POSTGRES_PORT,
):
    """Create a psycopg2 connection to PostgreSQL."""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
    )


def create_postgres_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    return create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )


def ensure_schema_exists(engine, schema):
    """Create schema if it doesn't exist."""
    if not schema:
        return
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def ensure_unique_index(conn, table_name, schema, columns):
    """Create unique index for incremental load de-duplication."""
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


def load_csv_from_s3_to_postgres(
    s3_file_key=None,
    s3_bucket=None,
    table_name=None,
    schema=None,
):
    """Load CSV file from S3 bucket into PostgreSQL database"""

    if s3_file_key is None:
        raise ValueError(
            "S3_FILE_KEY is not set. Provide s3_file_key or set S3_FILE_KEY env var."
        )

    if s3_bucket is None:
        s3_bucket = S3_BUCKET
    if not s3_bucket:
        raise ValueError(
            "S3_BUCKET is not set. Provide s3_bucket or set S3_BUCKET env var."
        )

    print(f"Downloading {s3_file_key} from S3 bucket {s3_bucket}...")
    s3_client = boto3.client("s3", region_name=AWS_REGION)
    obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_file_key)
    df = pd.read_csv(obj["Body"])
    print(f"CSV loaded: {len(df)} rows, {len(df.columns)} columns")

    print("Connecting to PostgreSQL...")
    conn = connect_postgres()

    if table_name is None:
        table_name = POSTGRES_TABLE
    if not table_name:
        raise ValueError(
            "POSTGRES_TABLE is not set. Provide table_name or set POSTGRES_TABLE env var."
        )

    if schema is None:
        schema = POSTGRES_SCHEMA

    qualified_table = f"{schema}.{table_name}" if schema else table_name
    print(f"Loading data into table '{qualified_table}'...")

    # Using SQLAlchemy for schema creation only
    engine = create_postgres_engine()

    ensure_schema_exists(engine, schema)

    df["ingested_time"] = datetime.now(timezone.utc)
    df["snapshot_date"] = datetime.now(timezone.utc).strftime("%Y%m%d")
    df["source_file"] = s3_file_key
    df.head(0).to_sql(
        table_name, engine, schema=schema, if_exists="append", index=False
    )
    # Ensure unique index for incremental loads
    unique_cols = ["zpid", "extracted_at", "price"]
    missing = [c for c in unique_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Unique key columns not found in CSV: {missing}")
    ensure_unique_index(conn, table_name, schema, unique_cols)

    # Insert with ON CONFLICT DO NOTHING to skip existing rows
    table_ident = (
        sql.Identifier(schema, table_name) if schema else sql.Identifier(table_name)
    )
    columns = list(df.columns)
    col_idents = [sql.Identifier(c) for c in columns]
    conflict_idents = [sql.Identifier(c) for c in unique_cols]
    insert_sql = sql.SQL(
        "INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT ({conflict_cols}) DO NOTHING"
    ).format(
        table=table_ident,
        cols=sql.SQL(", ").join(col_idents),
        conflict_cols=sql.SQL(", ").join(conflict_idents),
    )
    with conn.cursor() as cur:
        execute_values(cur, insert_sql, df.itertuples(index=False, name=None))
        inserted = cur.rowcount
    conn.commit()

    skipped = len(df) - inserted
    print(f"Successfully loaded {inserted} new rows into {qualified_table}")
    if skipped > 0:
        print(f"Skipped {skipped} duplicate rows already in {qualified_table}")

    # Close connections
    conn.close()
    engine.dispose()


if __name__ == "__main__":
    load_csv_from_s3_to_postgres(
        s3_file_key="raw/raw_20260207_20260207_2306.csv",
        s3_bucket=S3_BUCKET,
        table_name="property_master_data",
        schema="raw",
    )
