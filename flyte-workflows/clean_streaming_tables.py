"""Reset das tabelas Iceberg do pipeline streaming (sem tocar no batch).

Remove dados das quatro tabelas streaming, faz DROP das tabelas, purga os prefixos
MinIO correspondentes e recria o DDL via ``setup_streaming_tables.sql`` (inline).
"""

from __future__ import annotations

import trino
from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_streaming_env",
    packages=["boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

WAREHOUSE_BUCKET = "warehouse"

STREAMING_TABLES = (
    "iceberg.gold.network_events_hourly",
    "iceberg.silver.network_events_clean",
    "iceberg.bronze.network_events_raw",
    "iceberg.bronze.streaming_checkpoints",
)

STREAMING_S3_PREFIXES = (
    "gold/network_events_hourly/",
    "silver/network_events_clean/",
    "bronze/network_events_raw/",
    "bronze/streaming_checkpoints/",
)

SETUP_DDL_STATEMENTS = (
    """
    CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/bronze/')
    """,
    """
    CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://warehouse/silver/')
    """,
    """
    CREATE SCHEMA IF NOT EXISTS iceberg.gold WITH (location = 's3a://warehouse/gold/')
    """,
    """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.network_events_raw (
        event_id VARCHAR,
        phone_number VARCHAR,
        device_id VARCHAR,
        network_type VARCHAR,
        rsrp DOUBLE,
        sinr DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        event_time_raw VARCHAR,
        ingestion_timestamp TIMESTAMP(6) WITH TIME ZONE,
        ingest_batch_id VARCHAR
    )
    WITH (
        format = 'PARQUET',
        location = 's3a://warehouse/bronze/network_events_raw/'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS iceberg.bronze.streaming_checkpoints (
        pipeline_name VARCHAR,
        last_silver_watermark TIMESTAMP(6) WITH TIME ZONE,
        updated_at TIMESTAMP(6) WITH TIME ZONE
    )
    WITH (
        format = 'PARQUET',
        location = 's3a://warehouse/bronze/streaming_checkpoints/'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS iceberg.silver.network_events_clean (
        event_id VARCHAR,
        phone_number VARCHAR,
        device_id VARCHAR,
        network_type VARCHAR,
        rsrp DOUBLE,
        sinr DOUBLE,
        latitude DOUBLE,
        longitude DOUBLE,
        event_time TIMESTAMP(6) WITH TIME ZONE,
        ingested_at TIMESTAMP(6) WITH TIME ZONE
    )
    WITH (
        format = 'PARQUET',
        location = 's3a://warehouse/silver/network_events_clean/'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS iceberg.gold.network_events_hourly (
        bucket_hour TIMESTAMP(6) WITH TIME ZONE,
        network_type VARCHAR,
        event_count BIGINT,
        avg_rsrp DOUBLE,
        avg_sinr DOUBLE,
        poor_signal_count BIGINT,
        updated_at TIMESTAMP(6) WITH TIME ZONE
    )
    WITH (
        format = 'PARQUET',
        location = 's3a://warehouse/gold/network_events_hourly/'
    )
    """,
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
def clean_streaming_tables() -> str:
    """DROP tabelas streaming, purge MinIO e recria DDL."""
    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        for table in STREAMING_TABLES:
            logger.info("Dropping %s", table)
            cur.execute(f"DROP TABLE IF EXISTS {table}")
            cur.fetchall()
    finally:
        conn.close()

    s3 = minio_s3_client()
    total_deleted = 0
    for prefix in STREAMING_S3_PREFIXES:
        n = _purge_s3_prefix(s3, WAREHOUSE_BUCKET, prefix)
        total_deleted += n
        logger.info(
            "Removed %s objects under s3://%s/%s", n, WAREHOUSE_BUCKET, prefix
        )

    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        for ddl in SETUP_DDL_STATEMENTS:
            cur.execute(ddl)
            cur.fetchall()
        logger.info("Recreated streaming tables (setup_streaming_tables DDL)")
    finally:
        conn.close()

    tables = ", ".join(t.split(".", 1)[1] for t in STREAMING_TABLES)
    msg = (
        f"Streaming reset: dropped and recreated ({tables}); "
        f"removed {total_deleted} objects from MinIO. "
        "Re-run producer + jdpt_streaming_full_sync to repopulate."
    )
    logger.info(msg)
    return msg


@workflow
def reset_streaming_workflow() -> str:
    """Flyte workflow: wipe streaming Iceberg tables and warehouse prefixes."""
    return clean_streaming_tables()
