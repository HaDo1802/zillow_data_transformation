import os
from pathlib import Path
from supabase import create_client
import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()
AWS_REGION = os.getenv("AWS_REGION", "us-west-1")
S3_BUCKET = os.getenv("S3_BUCKET", "real-estate-scraped-data")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "real-estate-data")


def list_s3_keys(s3, bucket: str, prefix: str):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            yield key


def migrate_s3_to_supabase(
    s3_prefix: str,
    dest_prefix: str = "raw",
    overwrite: bool = False,
):
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        config=Config(retries={"max_attempts": 10, "mode": "standard"}),
    )

    keys = list(list_s3_keys(s3, S3_BUCKET, s3_prefix))
    if not keys:
        print(f"No files found in s3://{S3_BUCKET}/{s3_prefix}")
        return

    print(f"Found {len(keys)} file(s) to migrate.")

    for i, key in enumerate(keys, 1):
        filename = Path(key).name
        dest_path = f"{dest_prefix}/{filename}"

        # Download from S3 (bytes)
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()

        # Upload to Supabase Storage
        # (supabase-py expects bytes; content-type helps UI/download behavior)
        options = {"content-type": "text/csv", "upsert": overwrite}
        supabase.storage.from_(SUPABASE_BUCKET).upload(dest_path, data, options)

        print(f"[{i}/{len(keys)}] {key}  ->  {SUPABASE_BUCKET}/{dest_path}")


if __name__ == "__main__":
    # Example: migrate 7 files under "transformed/" in S3 into "raw/" in Supabase Storage
    migrate_s3_to_supabase(
        s3_prefix="transformed/", dest_prefix="transformed", overwrite=False
    )
