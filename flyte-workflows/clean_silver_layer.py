"""Reset the Silver layer: drop Trino Iceberg/Hive schemas and remove silver staging objects in MinIO."""

from __future__ import annotations

import trino
from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

WAREHOUSE_BUCKET = "warehouse"

# Parquet staging prefixes written by silver tasks (bronze/gold paths untouched)
SILVER_STAGING_PREFIXES = (
    "staging/cdr/",
    "staging/towers/",
    "staging/network_logs/",
    "staging/call_tests/",
)


def _purge_s3_prefix(s3_client, bucket: str, prefix: str) -> int:
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents")
        if not contents:
            continue
        for i in range(0, len(contents), 1000):
            batch = contents[i : i + 1000]
            resp = s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": o["Key"]} for o in batch]},
            )
            errs = resp.get("Errors") or []
            if errs:
                raise RuntimeError(f"S3 delete failed for prefix {prefix!r}: {errs}")
            deleted += len(batch)
    return deleted


@task(container_image=medallion_image, environment=TASK_ENV)
def clean_silver_layer() -> str:
    """Drop silver Trino schemas and remove silver staging files from MinIO (not bronze or gold)."""
    logger.info("Dropping iceberg.silver and hive.staging in Trino")
    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        cur.execute("DROP SCHEMA IF EXISTS iceberg.silver CASCADE")
        cur.fetchall()
        logger.info("Dropped iceberg.silver")

        cur.execute("DROP SCHEMA IF EXISTS hive.staging CASCADE")
        cur.fetchall()
        logger.info("Dropped hive.staging")
    finally:
        conn.close()

    s3 = minio_s3_client()
    total_deleted = 0
    for prefix in SILVER_STAGING_PREFIXES:
        n = _purge_s3_prefix(s3, WAREHOUSE_BUCKET, prefix)
        total_deleted += n
        logger.info(
            "Removed %s objects under s3://%s/%s", n, WAREHOUSE_BUCKET, prefix
        )

    msg = (
        f"Silver layer reset: Trino schemas iceberg.silver + hive.staging dropped; "
        f"removed {total_deleted} objects from MinIO under staging/cdr|towers|network_logs|call_tests."
    )
    logger.info(msg)
    return msg


@workflow
def clean_silver_workflow() -> str:
    return clean_silver_layer()
