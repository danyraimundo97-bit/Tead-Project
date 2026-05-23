"""Silver → gold streaming (agregação horária idempotente)."""

from __future__ import annotations

from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from loki_logging import get_logger
from streaming_audit import fetch_scalar, log_table_insert
from streaming_trino_client import execute_with_retry, get_trino_connection

logger = get_logger(__name__)

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

GOLD_TABLE = "iceberg.gold.network_events_hourly"
SILVER_TABLE = "iceberg.silver.network_events_clean"
GOLD_WINDOW_DAYS = 7


def _silver_to_gold_network_events_hourly() -> str:
    function = "silver_to_gold_network_events_hourly"
    log_table_insert(
        logger,
        function=function,
        phase="start",
        target_table=GOLD_TABLE,
        source_table=SILVER_TABLE,
        operation="DELETE+INSERT",
        window_days=GOLD_WINDOW_DAYS,
    )

    conn = get_trino_connection(catalog="iceberg")
    cur = conn.cursor()

    rows_in_window = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM {SILVER_TABLE}
        WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
        """,
    )
    gold_rows_before = fetch_scalar(cur, f"SELECT COUNT(*) FROM {GOLD_TABLE}")

    rows_to_delete = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM {GOLD_TABLE}
        WHERE bucket_hour >= date_trunc('hour', CURRENT_TIMESTAMP)
              - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
        """,
    )
    log_table_insert(
        logger,
        function=function,
        phase="before_delete",
        target_table=GOLD_TABLE,
        silver_rows_in_window=rows_in_window,
        gold_rows_before=gold_rows_before,
        gold_rows_to_delete=rows_to_delete,
    )

    execute_with_retry(
        cur,
        f"""
        DELETE FROM {GOLD_TABLE}
        WHERE bucket_hour >= date_trunc('hour', CURRENT_TIMESTAMP)
              - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
        """,
    )
    log_table_insert(
        logger,
        function=function,
        phase="after_delete",
        target_table=GOLD_TABLE,
        operation="DELETE",
        rows_affected=rows_to_delete,
    )

    rows_to_insert = fetch_scalar(
        cur,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT 1
            FROM {SILVER_TABLE}
            WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
            GROUP BY date_trunc('hour', event_time), network_type
        ) grouped
        """,
    )

    insert_sql = f"""
    INSERT INTO {GOLD_TABLE}
    SELECT
        date_trunc('hour', event_time) AS bucket_hour,
        network_type,
        COUNT(*) AS event_count,
        AVG(rsrp) AS avg_rsrp,
        AVG(sinr) AS avg_sinr,
        COUNT_IF(rsrp < -110) AS poor_signal_count,
        CURRENT_TIMESTAMP AS updated_at
    FROM {SILVER_TABLE}
    WHERE event_time >= CURRENT_TIMESTAMP - INTERVAL '{GOLD_WINDOW_DAYS}' DAY
    GROUP BY date_trunc('hour', event_time), network_type
    """
    execute_with_retry(cur, insert_sql)

    gold_rows_after = fetch_scalar(cur, f"SELECT COUNT(*) FROM {GOLD_TABLE}")
    log_table_insert(
        logger,
        function=function,
        phase="complete",
        target_table=GOLD_TABLE,
        source_table=SILVER_TABLE,
        operation="INSERT",
        new_rows=rows_to_insert,
        rows_affected=rows_to_insert,
        gold_rows_after=gold_rows_after,
        silver_rows_in_window=rows_in_window,
    )

    return (
        f"Silver → gold: inserted {rows_to_insert} hourly buckets into {GOLD_TABLE} "
        f"(silver rows in window={rows_in_window}, gold total={gold_rows_after})."
    )


@task(**STREAMING_TASK_KWARGS)
def silver_to_gold_network_events_hourly() -> str:
    try:
        return _silver_to_gold_network_events_hourly()
    except Exception as exc:
        logger.exception("Silver → gold falhou")
        raise RuntimeError(str(exc)) from exc


@workflow
def jdpt_streaming_silver_to_gold_sync() -> str:
    return silver_to_gold_network_events_hourly()
