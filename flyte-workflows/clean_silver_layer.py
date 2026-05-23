"""Reset the Silver layer in Trino and MinIO.

Drops ``iceberg.silver`` (CASCADE: tabelas silver + quarentena) e ``hive.staging``.
Purges MinIO: ``staging/*`` (parquet temporário) e ``silver/`` (ficheiros Iceberg da
camada silver, incluindo quarentenas). A task ``ensure`` volta a criar schemas e tabelas."""

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

# Tabelas de quarentena em iceberg.silver (removidas pelo CASCADE; listadas para logs/documentação)
SILVER_QUARANTINE_TABLES = (
    "network_logs_quarantine_raw",
    "network_logs_quarantine_audit",
    "cdr_quarantine_raw",
    "cdr_quarantine_audit",
    "call_tests_quarantine_raw",
    "call_tests_quarantine_audit",
    "towers_quarantine_raw",
    "towers_quarantine_audit",
)

# Parquet staging (hive.staging) — bronze/gold intactos
SILVER_STAGING_PREFIXES = (
    "staging/cdr/",
    "staging/towers/",
    "staging/network_logs/",
    "staging/call_tests/",
)

# Localização do schema iceberg.silver (ensure_pipeline_layers): s3a://warehouse/silver/
# Apaga ficheiros Iceberg das tabelas silver incluindo quarentenas após DROP SCHEMA
SILVER_ICEBERG_PREFIX = "silver/"


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
    """Drop iceberg.silver (inclui quarentenas via CASCADE), hive.staging; purge staging + silver/."""
    logger.info(
        "Removing iceberg.silver (CASCADE drops silver tables + quarantine: %s)",
        ", ".join(SILVER_QUARANTINE_TABLES),
    )
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

    n_silver = _purge_s3_prefix(s3, WAREHOUSE_BUCKET, SILVER_ICEBERG_PREFIX)
    total_deleted += n_silver
    logger.info(
        "Removed %s objects under s3://%s/%s (Iceberg silver + quarantine data)",
        n_silver,
        WAREHOUSE_BUCKET,
        SILVER_ICEBERG_PREFIX,
    )

    msg = (
        f"Silver layer reset: iceberg.silver (incl. quarantine) + hive.staging dropped; "
        f"removed {total_deleted} objects from MinIO (staging/* + {SILVER_ICEBERG_PREFIX})."
    )
    logger.info(msg)
    return msg


@workflow
def clean_silver_workflow() -> str:
    return clean_silver_layer()
