"""Reset the Gold layer in Trino and MinIO.

Drops ``iceberg.gold`` (CASCADE: all gold Iceberg tables). Purges MinIO ``gold/``
(Iceberg data for that schema). Silver, staging, and bronze are untouched.
Recreate tables by running ``ensure_gold_layer_environment`` and the ``build_gold_*`` tasks."""

from __future__ import annotations

import trino
from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

WAREHOUSE_BUCKET = "warehouse"

# Documentação / logs (removidas pelo CASCADE)
GOLD_TABLES = (
    "churn_risk_daily",
    "network_quality_daily",
)

GOLD_ICEBERG_PREFIX = "gold/"


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
def clean_gold_layer() -> str:
    """Drop iceberg.gold (CASCADE); purge MinIO gold/."""
    logger.info(
        "Removing iceberg.gold (CASCADE drops: %s)",
        ", ".join(GOLD_TABLES),
    )
    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        cur.execute("DROP SCHEMA IF EXISTS iceberg.gold CASCADE")
        cur.fetchall()
        logger.info("Dropped iceberg.gold")
    finally:
        conn.close()

    s3 = minio_s3_client()
    n_gold = _purge_s3_prefix(s3, WAREHOUSE_BUCKET, GOLD_ICEBERG_PREFIX)
    logger.info(
        "Removed %s objects under s3://%s/%s",
        n_gold,
        WAREHOUSE_BUCKET,
        GOLD_ICEBERG_PREFIX,
    )

    msg = (
        f"Gold layer reset: iceberg.gold dropped (tables: {', '.join(GOLD_TABLES)}); "
        f"removed {n_gold} objects from MinIO ({GOLD_ICEBERG_PREFIX})."
    )
    logger.info(msg)
    return msg


@workflow
def reset_gold_workflow() -> str:
    """Flyte workflow: wipe gold schema and warehouse gold/ prefix."""
    return clean_gold_layer()
