"""Kafka → bronze: MERGE idempotente por event_id."""

from __future__ import annotations

import logging
import uuid

from workflow_functions.streaming_audit import fetch_scalar, log_table_insert
from workflow_functions.streaming_trino_client import execute_with_retry, get_trino_connection

BRONZE_TABLE = "iceberg.bronze.network_events_raw"
KAFKA_TABLE = "kafka.default.network_events"

KAFKA_SOURCE_SUBQUERY = """
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


def process_kafka_to_bronze(logger: logging.Logger) -> str:
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
        cur, f"SELECT COUNT(*) FROM ({KAFKA_SOURCE_SUBQUERY}) src"
    )
    kafka_new_rows = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM ({KAFKA_SOURCE_SUBQUERY}) src
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
    USING ({KAFKA_SOURCE_SUBQUERY}) AS source
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
