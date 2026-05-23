"""Ingere eventos do tópico Kafka para bronze Iceberg (MERGE idempotente por event_id)."""

from __future__ import annotations

import uuid
from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from loki_logging import get_logger
from streaming_audit import fetch_scalar, log_table_insert
from streaming_trino_client import execute_with_retry, get_trino_connection

logger = get_logger(__name__)

BRONZE_TABLE = "iceberg.bronze.network_events_raw"
KAFKA_TABLE = "kafka.default.network_events"

streaming_image = ImageSpec(
    name="jdpt_streaming_env",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000",
)

STREAMING_TASK_KWARGS = {
    "container_image": streaming_image,
    "environment": TASK_ENV,
    "retries": 3,
    "timeout": timedelta(minutes=15),
}

_KAFKA_SOURCE_SUBQUERY = """
    SELECT
        event_id,
        phone_number,
        device_id,
        network_type,
        rsrp,
        sinr,
        latitude,
        longitude,
        event_time_raw
    FROM (
        SELECT
            event_id,
            phone_number,
            device_id,
            network_type,
            rsrp,
            sinr,
            latitude,
            longitude,
            event_time AS event_time_raw,
            ROW_NUMBER() OVER (
                PARTITION BY event_id
                ORDER BY event_time DESC
            ) AS rn
        FROM kafka.default.network_events
        WHERE event_id IS NOT NULL
    ) kafka_deduped
    WHERE rn = 1
"""


def _ingest_kafka_to_bronze() -> str:
    function = "ingest_kafka_to_bronze"
    batch_id = str(uuid.uuid4())
    log_table_insert(
        logger,
        function=function,
        phase="start",
        target_table=BRONZE_TABLE,
        source_table=KAFKA_TABLE,
        operation="MERGE",
        ingest_batch_id=batch_id,
    )

    conn = get_trino_connection(catalog="iceberg", schema="bronze")
    cur = conn.cursor()

    bronze_before = fetch_scalar(cur, f"SELECT COUNT(*) FROM {BRONZE_TABLE}")
    kafka_source_rows = fetch_scalar(
        cur, f"SELECT COUNT(*) FROM ({_KAFKA_SOURCE_SUBQUERY}) src"
    )
    kafka_new_rows = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM ({_KAFKA_SOURCE_SUBQUERY}) src
        LEFT JOIN {BRONZE_TABLE} tgt ON src.event_id = tgt.event_id
        WHERE tgt.event_id IS NULL
        """,
    )
    log_table_insert(
        logger,
        function=function,
        phase="before_merge",
        target_table=BRONZE_TABLE,
        source_table=KAFKA_TABLE,
        kafka_source_rows=kafka_source_rows,
        new_rows=kafka_new_rows,
        bronze_rows_before=bronze_before,
        ingest_batch_id=batch_id,
    )

    merge_sql = f"""
    MERGE INTO {BRONZE_TABLE} AS target
    USING ({_KAFKA_SOURCE_SUBQUERY}) AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN INSERT (
        event_id, phone_number, device_id, network_type,
        rsrp, sinr, latitude, longitude,
        event_time_raw, ingestion_timestamp, ingest_batch_id
    ) VALUES (
        source.event_id, source.phone_number, source.device_id,
        source.network_type, source.rsrp, source.sinr,
        source.latitude, source.longitude,
        source.event_time_raw, CURRENT_TIMESTAMP,
        '{batch_id}'
    )
    """
    execute_with_retry(cur, merge_sql)

    bronze_inserted_batch = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*) FROM {BRONZE_TABLE}
        WHERE ingest_batch_id = '{batch_id}'
        """,
    )
    bronze_after = fetch_scalar(cur, f"SELECT COUNT(*) FROM {BRONZE_TABLE}")
    log_table_insert(
        logger,
        function=function,
        phase="complete",
        target_table=BRONZE_TABLE,
        source_table=KAFKA_TABLE,
        operation="MERGE",
        kafka_source_rows=kafka_source_rows,
        new_rows=kafka_new_rows,
        rows_inserted=bronze_inserted_batch,
        bronze_rows_after=bronze_after,
        ingest_batch_id=batch_id,
    )

    return (
        f"Kafka → bronze: MERGE em {BRONZE_TABLE} — "
        f"fonte={kafka_source_rows}, novos={kafka_new_rows}, "
        f"inseridos_neste_run={bronze_inserted_batch}, total_bronze={bronze_after} "
        f"(ingest_batch_id={batch_id})."
    )


@task(**STREAMING_TASK_KWARGS)
def ingest_kafka_to_bronze() -> str:
    try:
        return _ingest_kafka_to_bronze()
    except Exception as exc:
        logger.exception("Kafka → bronze falhou")
        raise RuntimeError(str(exc)) from exc


@workflow
def streaming_kafka_to_bronze_workflow() -> str:
    return ingest_kafka_to_bronze()
