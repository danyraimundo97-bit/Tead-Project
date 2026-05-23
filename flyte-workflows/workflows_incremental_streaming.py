"""Bronze → silver incremental (watermark + MERGE + checkpoint)."""

from __future__ import annotations

from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from loki_logging import get_logger
from streaming_audit import fetch_scalar, log_table_insert
from streaming_trino_client import (
    execute_with_retry,
    format_trino_timestamp,
    get_trino_connection,
    read_silver_watermark,
    write_silver_checkpoint,
)

logger = get_logger(__name__)

BRONZE_TABLE = "iceberg.bronze.network_events_raw"
SILVER_TABLE = "iceberg.silver.network_events_clean"

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

WATERMARK_LOOKBACK_HOURS = 1


def _cleansed_source_subquery(lower_literal: str) -> str:
    return f"""
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
                TRIM(phone_number) AS phone_number,
                TRIM(device_id) AS device_id,
                CASE
                    WHEN network_type IS NULL OR TRIM(network_type) = '' THEN 'OTHER'
                    WHEN UPPER(TRIM(network_type)) IN ('LTE', 'NR', 'UMTS', 'GSM')
                        THEN UPPER(TRIM(network_type))
                    WHEN UPPER(network_type) LIKE '%NR%'
                      OR UPPER(network_type) LIKE '%5G%' THEN 'NR'
                    WHEN UPPER(network_type) LIKE '%LTE%'
                      OR UPPER(network_type) LIKE '%4G%' THEN 'LTE'
                    WHEN UPPER(network_type) LIKE '%UMTS%'
                      OR UPPER(network_type) LIKE '%3G%' THEN 'UMTS'
                    WHEN UPPER(network_type) LIKE '%GSM%'
                      OR UPPER(network_type) LIKE '%2G%' THEN 'GSM'
                    ELSE 'OTHER'
                END AS network_type,
                CASE
                    WHEN rsrp IS NULL THEN NULL
                    ELSE LEAST(GREATEST(rsrp, -140.0), -60.0)
                END AS rsrp,
                CASE
                    WHEN sinr IS NULL THEN NULL
                    ELSE LEAST(GREATEST(sinr, -20.0), 40.0)
                END AS sinr,
                latitude,
                longitude,
                COALESCE(
                    TRY(from_iso8601_timestamp(event_time_raw)),
                    TRY(CAST(event_time_raw AS TIMESTAMP WITH TIME ZONE))
                ) AS event_time,
                ingestion_timestamp,
                ROW_NUMBER() OVER (
                    PARTITION BY event_id
                    ORDER BY ingestion_timestamp DESC
                ) AS rn
            FROM {BRONZE_TABLE}
            WHERE ingestion_timestamp > {lower_literal}
              AND event_id IS NOT NULL
        ) cleansed
        WHERE rn = 1
          AND phone_number IS NOT NULL
          AND regexp_like(phone_number, '^9[0-9]{{8}}$')
          AND event_time IS NOT NULL
          AND latitude IS NOT NULL
          AND longitude IS NOT NULL
          AND rsrp IS NOT NULL
    """


def _incremental_bronze_to_silver_network_events() -> str:
    function = "incremental_bronze_to_silver_network_events"
    conn = get_trino_connection(catalog="iceberg")
    cur = conn.cursor()

    watermark = read_silver_watermark(cur)
    lower_bound = watermark - timedelta(hours=WATERMARK_LOOKBACK_HOURS)
    lower_literal = format_trino_timestamp(lower_bound)
    #source_sql = _cleansed_source_subquery(lower_literal + " + INTERVAL '2 hours'")
    source_sql = _cleansed_source_subquery(lower_literal)

    log_table_insert(
        logger,
        function=function,
        phase="start",
        target_table=SILVER_TABLE,
        source_table=BRONZE_TABLE,
        operation="MERGE",
        watermark=watermark.isoformat(),
        lower_bound=lower_bound.isoformat(),
        lookback_hours=WATERMARK_LOOKBACK_HOURS,
    )

    silver_before = fetch_scalar(cur, f"SELECT COUNT(*) FROM {SILVER_TABLE}")
    bronze_in_window = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM {BRONZE_TABLE}
        WHERE ingestion_timestamp > {lower_literal}
          AND event_id IS NOT NULL
        """,
    )
    source_rows = fetch_scalar(
        cur, f"SELECT COUNT(*) FROM ({source_sql}) src"
    )
    new_rows = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM ({source_sql}) src
        LEFT JOIN {SILVER_TABLE} tgt ON src.event_id = tgt.event_id
        WHERE tgt.event_id IS NULL
        """,
    )
    log_table_insert(
        logger,
        function=function,
        phase="before_merge",
        target_table=SILVER_TABLE,
        source_table=BRONZE_TABLE,
        bronze_rows_in_window=bronze_in_window,
        source_rows=source_rows,
        new_rows=new_rows,
        silver_rows_before=silver_before,
    )

    merge_sql = f"""
    MERGE INTO {SILVER_TABLE} AS target
    USING ({source_sql}) AS source
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

    silver_after = fetch_scalar(cur, f"SELECT COUNT(*) FROM {SILVER_TABLE}")
    rows_inserted = max(silver_after - silver_before, 0)
    log_table_insert(
        logger,
        function=function,
        phase="complete",
        target_table=SILVER_TABLE,
        source_table=BRONZE_TABLE,
        operation="MERGE",
        source_rows=source_rows,
        new_rows=new_rows,
        rows_inserted=rows_inserted,
        silver_rows_after=silver_after,
        watermark=watermark.isoformat(),
    )

    return (
        f"Bronze → silver: MERGE em {SILVER_TABLE} — "
        f"fonte_limpa={source_rows}, novos={new_rows}, "
        f"inseridos_estimados={rows_inserted}, total_silver={silver_after} "
        f"(watermark={watermark.isoformat()}, lookback={WATERMARK_LOOKBACK_HOURS}h)."
    )


@task(**STREAMING_TASK_KWARGS)
def incremental_bronze_to_silver_network_events() -> str:
    try:
        return _incremental_bronze_to_silver_network_events()
    except Exception as exc:
        logger.exception("Bronze → silver incremental falhou")
        raise RuntimeError(str(exc)) from exc


@workflow
def jdpt_streaming_incremental_sync() -> str:
    """Promove eventos de bronze para silver (idempotente por event_id)."""
    return incremental_bronze_to_silver_network_events()
