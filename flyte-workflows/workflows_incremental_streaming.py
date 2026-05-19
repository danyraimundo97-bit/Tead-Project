"""Workflows incrementais (watermark + MERGE + checkpoint) para streaming."""

from __future__ import annotations

from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from streaming_trino_client import (
    execute_with_retry,
    format_trino_timestamp,
    get_trino_connection,
    read_silver_watermark,
    write_silver_checkpoint,
)

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

WATERMARK_LOOKBACK_HOURS = 1
GOLD_WINDOW_DAYS = 7


@task(**STREAMING_TASK_KWARGS)
def incremental_bronze_to_silver_network_events() -> str:
    conn = get_trino_connection(catalog="iceberg")
    cur = conn.cursor()

    watermark = read_silver_watermark(cur)
    lower_bound = watermark - timedelta(hours=WATERMARK_LOOKBACK_HOURS)
    lower_literal = format_trino_timestamp(lower_bound)

    merge_sql = f"""
    MERGE INTO iceberg.silver.network_events_clean AS target
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
            event_time,
            ingestion_timestamp
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
                from_iso8601_timestamp(event_time_raw) AS event_time,
                ingestion_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM iceberg.bronze.network_events_raw
            WHERE ingestion_timestamp > {lower_literal}
              AND event_id IS NOT NULL
              AND rsrp IS NOT NULL
        ) deduped
        WHERE rn = 1
    ) AS source
    ON target.event_id = source.event_id
    WHEN NOT MATCHED THEN
        INSERT (
            event_id, phone_number, device_id, network_type,
            rsrp, sinr, latitude, longitude, event_time, ingested_at
        )
        VALUES (
            source.event_id, source.phone_number, source.device_id,
            source.network_type, source.rsrp, source.sinr,
            source.latitude, source.longitude,
            source.event_time, source.ingestion_timestamp
        )
    """
    execute_with_retry(cur, merge_sql)
    write_silver_checkpoint(cur)
    return (
        f"Bronze → silver incremental "
        f"(watermark={watermark}, lookback={WATERMARK_LOOKBACK_HOURS}h)."
    )


@task(**STREAMING_TASK_KWARGS)
def silver_to_gold_network_events_hourly() -> str:
    """Gold idempotente: DELETE da janela + INSERT (chave bucket_hour, network_type)."""
    conn = get_trino_connection(catalog="iceberg")
    cur = conn.cursor()

    execute_with_retry(
        cur,
        f"""
        DELETE FROM iceberg.gold.network_events_hourly
        WHERE bucket_hour >= date_trunc('hour', CURRENT_TIMESTAMP)
              - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
        """,
    )
    execute_with_retry(
        cur,
        f"""
        INSERT INTO iceberg.gold.network_events_hourly
        SELECT
            date_trunc('hour', event_time) AS bucket_hour,
            network_type,
            COUNT(*) AS event_count,
            AVG(rsrp) AS avg_rsrp,
            AVG(sinr) AS avg_sinr,
            COUNT_IF(rsrp < -110) AS poor_signal_count,
            CURRENT_TIMESTAMP AS updated_at
        FROM iceberg.silver.network_events_clean
        WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
        GROUP BY 1, 2
        """,
    )
    return (
        f"Silver → gold.network_events_hourly "
        f"(janela {GOLD_WINDOW_DAYS} dias, idempotente por DELETE+INSERT)."
    )


@workflow
def jdpt_streaming_incremental_sync() -> str:
    """Promove eventos de bronze para silver (idempotente por event_id)."""
    return incremental_bronze_to_silver_network_events()


@workflow
def jdpt_streaming_batch_gold_sync() -> str:
    """Agrega silver streaming para gold."""
    return silver_to_gold_network_events_hourly()


@workflow
def jdpt_streaming_full_sync() -> str:
    """Bronze → silver → gold no ramo streaming."""
    silver = incremental_bronze_to_silver_network_events()
    gold = silver_to_gold_network_events_hourly()
    silver >> gold
    return gold
