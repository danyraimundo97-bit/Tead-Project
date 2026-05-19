"""Ingere eventos do tópico Kafka para bronze Iceberg (MERGE idempotente por event_id)."""

from __future__ import annotations

import uuid
from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from streaming_trino_client import execute_with_retry, get_trino_connection

streaming_image = ImageSpec(
    name="jdpt_streaming_env",
    packages=["trino"],
    registry="localhost:30000",
)

STREAMING_TASK_KWARGS = {
    "container_image": streaming_image,
    "environment": TASK_ENV,
    "retries": 3,
    "timeout": timedelta(minutes=15),
}


@task(**STREAMING_TASK_KWARGS)
def ingest_kafka_to_bronze() -> str:
    batch_id = str(uuid.uuid4())
    conn = get_trino_connection(catalog="iceberg", schema="bronze")
    cur = conn.cursor()

    merge_sql = f"""
    MERGE INTO iceberg.bronze.network_events_raw AS target
    USING (
        SELECT
            event_id,
            phone_number,
            device_id,
            network_type,
            rsrp,
            sinr,
            latitude,
            longitude,
            event_time AS event_time_raw
        FROM kafka.default.network_events
        WHERE event_id IS NOT NULL
    ) AS source
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
    return (
        f"Kafka → bronze (MERGE idempotente) concluído. ingest_batch_id={batch_id}"
    )


@workflow
def streaming_kafka_to_bronze_workflow() -> str:
    return ingest_kafka_to_bronze()
