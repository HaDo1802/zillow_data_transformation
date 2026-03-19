import os
from io import BytesIO, StringIO
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

SCHEMA = "raw"
STG_TABLE = "raw_property_master_data_stg"
TGT_TABLE = "raw_property_master_data"

BIGINT_MIN = -9223372036854775808
BIGINT_MAX =  9223372036854775807

EXPECTED_COLS = [
    "address","bathrooms","bedrooms","brokername","carouselphotos",
    "comingsoononmarketdate","contingentlistingtype","country","currency",
    "datepricechanged","daysonzillow","detailurl","has3dmodel","hasimage",
    "hasvideo","imgsrc","latitude","listingstatus","listingsubtype",
    "livingarea","longitude","lotareaunit","lotareavalue","price",
    "pricechange","propertytype","rentzestimate","variabledata","zestimate",
    "zpid","unit","newconstructiontype","extracted_at",
    "ingested_time","snapshot_date","source_file"
]


def get_conn():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_DB_HOST"),
        database=os.getenv("SUPABASE_DB_NAME", "postgres"),
        user=os.getenv("SUPABASE_DB_USER"),
        password=os.getenv("SUPABASE_DB_PASSWORD"),
        port=os.getenv("SUPABASE_DB_PORT", "5432"),
        sslmode=os.getenv("SUPABASE_DB_SSLMODE", "require"),
    )


def download_csv_from_storage(bucket: str, path: str) -> pd.DataFrame:
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    print(f"Downloading '{path}' from bucket '{bucket}'...")
    content = supabase.storage.from_(bucket).download(path)
    if not content:
        raise ValueError(f"Could not download '{path}' from '{bucket}'")
    return pd.read_csv(BytesIO(content))


def resolve_latest_csv_path(bucket: str, prefix: str = "raw") -> str:
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
    prefix = (prefix or "raw").strip("/")

    # Supabase list returns object metadata including "name"
    objects = supabase.storage.from_(bucket).list(
        path=prefix,
        options={
            "limit": 1000,
            "offset": 0,
            "sortBy": {"column": "name", "order": "desc"},
        },
    )
    csv_names = sorted(
        [obj["name"] for obj in objects if obj.get("name", "").endswith(".csv")],
        reverse=True,
    )
    if not csv_names:
        raise ValueError(f"No CSV files found in {bucket}/{prefix}")
    latest_name = csv_names[0]
    return f"{prefix}/{latest_name}"


def to_bool_sql(expr: str) -> str:
    return f"""
    CASE
      WHEN lower({expr}) IN ('true','t','1','yes','y') THEN TRUE
      WHEN lower({expr}) IN ('false','f','0','no','n') THEN FALSE
      ELSE NULL
    END
    """


def safe_int_sql(expr: str) -> str:
    return f"""
    CASE
      WHEN {expr} ~ '^-?\\d+$' THEN ({expr})::INTEGER
      ELSE NULL
    END
    """


def safe_numeric_sql(expr: str) -> str:
    return f"""
    CASE
      WHEN {expr} ~ '^-?\\d+(\\.\\d+)?$' THEN ({expr})::NUMERIC
      ELSE NULL
    END
    """


def safe_float_sql(expr: str) -> str:
    return f"""
    CASE
      WHEN {expr} ~ '^-?\\d+(\\.\\d+)?$' THEN ({expr})::DOUBLE PRECISION
      ELSE NULL
    END
    """


def safe_bigint_sql(expr: str) -> str:
    # Only cast if integer string AND within bigint range
    return f"""
    CASE
      WHEN {expr} ~ '^-?\\d+$'
       AND ({expr})::NUMERIC BETWEEN {BIGINT_MIN} AND {BIGINT_MAX}
      THEN ({expr})::BIGINT
      ELSE NULL
    END
    """


def load_to_staging(conn, df: pd.DataFrame, schema: str = SCHEMA, staging_table: str = STG_TABLE):
    # Normalize headers to match Postgres behavior
    df.columns = df.columns.str.strip().str.lower()

    # Add metadata
    now_utc = datetime.now(timezone.utc)
    df["ingested_time"] = now_utc.isoformat()
    df["snapshot_date"] = now_utc.strftime("%Y%m%d")
    # source_file will be set by caller

    # Keep only expected columns (missing -> NULL, extra -> dropped)
    for c in EXPECTED_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[EXPECTED_COLS]

    # Convert everything to string for TEXT staging (None stays empty in CSV, we handle with NULLIF later)
    df2 = df.copy()
    for c in df2.columns:
        df2[c] = df2[c].astype("string")

    buf = StringIO()
    df2.to_csv(buf, index=False, header=True)
    buf.seek(0)

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {schema}.{staging_table};")
        copy_sql = f"""
            COPY {schema}.{staging_table} ({", ".join(EXPECTED_COLS)})
            FROM STDIN WITH (FORMAT CSV, HEADER TRUE);
        """
        cur.copy_expert(copy_sql, buf)

    conn.commit()


def stage_to_target(
    conn,
    schema: str = SCHEMA,
    staging_table: str = STG_TABLE,
    target_table: str = TGT_TABLE,
) -> int:
    s = lambda c: f"s.{c}"

    insert_sql = f"""
    INSERT INTO {schema}.{target_table} (
      address, bathrooms, bedrooms, brokername, carouselphotos,
      comingsoononmarketdate, contingentlistingtype, country, currency,
      datepricechanged, daysonzillow, detailurl, has3dmodel, hasimage, hasvideo,
      imgsrc, latitude, listingstatus, listingsubtype, livingarea, longitude,
      lotareaunit, lotareavalue, price, pricechange, propertytype,
      rentzestimate, variabledata, zestimate, zpid, unit, newconstructiontype,
      extracted_at, ingested_time, snapshot_date, source_file
    )
    SELECT
      NULLIF({s('address')}, ''),
      ({safe_numeric_sql(s('bathrooms'))})::NUMERIC(4,1),
      {safe_int_sql(s('bedrooms'))},
      NULLIF({s('brokername')}, ''),
      NULLIF({s('carouselphotos')}, ''),
      NULLIF({s('comingsoononmarketdate')}, ''),
      NULLIF({s('contingentlistingtype')}, ''),
      NULLIF({s('country')}, ''),
      NULLIF({s('currency')}, ''),
      NULLIF({s('datepricechanged')}, ''),
      {safe_int_sql(s('daysonzillow'))},
      NULLIF({s('detailurl')}, ''),
      {to_bool_sql(s('has3dmodel'))},
      {to_bool_sql(s('hasimage'))},
      {to_bool_sql(s('hasvideo'))},
      NULLIF({s('imgsrc')}, ''),
      {safe_float_sql(s('latitude'))},
      NULLIF({s('listingstatus')}, ''),
      NULLIF({s('listingsubtype')}, ''),
      {safe_int_sql(s('livingarea'))},
      {safe_float_sql(s('longitude'))},
      NULLIF({s('lotareaunit')}, ''),
      {safe_float_sql(s('lotareavalue'))},
      {safe_bigint_sql(s('price'))},
      {safe_bigint_sql(s('pricechange'))},
      NULLIF({s('propertytype')}, ''),
      {safe_bigint_sql(s('rentzestimate'))},
      NULLIF({s('variabledata')}, ''),
      {safe_bigint_sql(s('zestimate'))},
      {safe_bigint_sql(s('zpid'))},
      NULLIF({s('unit')}, ''),
      NULLIF({s('newconstructiontype')}, ''),
      NULLIF({s('extracted_at')}, '')::timestamptz,
      NULLIF({s('ingested_time')}, '')::timestamptz,
      NULLIF({s('snapshot_date')}, ''),
      NULLIF({s('source_file')}, '')
    FROM {schema}.{staging_table} s
    WHERE NULLIF({s('zpid')}, '') IS NOT NULL
      AND NULLIF({s('extracted_at')}, '') IS NOT NULL
    ON CONFLICT (zpid, extracted_at, price) DO NOTHING;
    """

    with conn.cursor() as cur:
        cur.execute(insert_sql)
        inserted = cur.rowcount

    conn.commit()
    return inserted


def load_csv_from_supabase_storage_to_table(
    storage_file_path: Optional[str] = None,
    storage_bucket: Optional[str] = None,
    schema: str = SCHEMA,
    table_name: str = TGT_TABLE,
    raw_prefix: str = "raw",
) -> int:
    bucket = storage_bucket or os.getenv("SUPABASE_STORAGE_BUCKET")
    if not bucket:
        raise ValueError("Missing SUPABASE_STORAGE_BUCKET")

    resolved_path = storage_file_path or os.getenv("SUPABASE_FILE_PATH")
    if not resolved_path:
        resolved_path = resolve_latest_csv_path(bucket=bucket, prefix=(raw_prefix or "raw"))
        print(f"Resolved latest raw CSV: {resolved_path}")

    df = download_csv_from_storage(bucket, resolved_path)
    print(f"Read {len(df)} rows and {len(df.columns)} columns")

    # add source_file now (used in staging)
    df["source_file"] = resolved_path

    conn = get_conn()
    try:
        load_to_staging(conn, df, schema=schema)
        inserted = stage_to_target(conn, schema=schema, target_table=table_name)
        print(f"✅ Done. Inserted {inserted} new rows into {schema}.{table_name}")
        return inserted
    finally:
        conn.close()


def main():
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET")
    if not bucket:
        raise ValueError("Missing SUPABASE_STORAGE_BUCKET in .env")
    load_csv_from_supabase_storage_to_table(
        storage_file_path=os.getenv("SUPABASE_FILE_PATH"),
        storage_bucket=bucket,
        schema=os.getenv("SUPABASE_SCHEMA", SCHEMA),
        table_name=os.getenv("SUPABASE_RAW_TABLE", TGT_TABLE),
        raw_prefix=os.getenv("SUPABASE_RAW_PREFIX", "raw"),
    )


if __name__ == "__main__":
    main()
