import io
import boto3
import pandas as pd
from utils.logger import get_logger

logger = get_logger(__name__)

BUCKET = "omini-financial-datalake"
REGION = "ap-south-1"


def get_s3_client():
    return boto3.client("s3", region_name=REGION)


def write_parquet_to_s3(df: pd.DataFrame, s3_key: str) -> None:
    """Write a DataFrame as parquet to S3."""
    s3 = get_s3_client()
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=buffer.getvalue())
    logger.info("Written → s3://%s/%s (%d rows)", BUCKET, s3_key, len(df))


def read_parquet_from_s3(s3_key: str) -> pd.DataFrame:
    """Read a parquet file from S3 into a DataFrame."""
    s3 = get_s3_client()
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=s3_key)
        return pd.read_parquet(io.BytesIO(obj["Body"].read()))
    except s3.exceptions.NoSuchKey:
        logger.warning("File not found in S3: %s", s3_key)
        return pd.DataFrame()
    except Exception as e:
        logger.error("Failed to read %s: %s", s3_key, e)
        return pd.DataFrame()


def list_s3_keys(prefix: str) -> list:
    """List all parquet file keys under a given S3 prefix."""
    s3 = get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    logger.info("Found %d files under s3://%s/%s", len(keys), BUCKET, prefix)
    return keys