"""Quality checks para o ramo streaming (duplicados, órfãos bronze→silver)."""

from __future__ import annotations

from datetime import timedelta

from flytekit import ImageSpec, task, workflow

from flyte_task_env import TASK_ENV
from loki_logging import get_logger
from streaming_trino_client import (
    fetch_one_with_retry,
    get_trino_connection,
)

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

ORPHAN_WINDOW_HOURS = 24


@task(**STREAMING_TASK_KWARGS)
def avaliar_streaming() -> str:
    conn = get_trino_connection(catalog="iceberg")
    cur = conn.cursor()

    row = fetch_one_with_retry(
        cur,
        """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT event_id) AS distinct_ids
        FROM iceberg.bronze.network_events_raw
        """,
    )
    bronze_total, bronze_distinct = (
        (int(row[0]), int(row[1])) if row else (0, 0)
    )
    bronze_dupes = bronze_total - bronze_distinct

    row = fetch_one_with_retry(
        cur,
        """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT event_id) AS distinct_ids
        FROM iceberg.silver.network_events_clean
        """,
    )
    silver_total, silver_distinct = (
        (int(row[0]), int(row[1])) if row else (0, 0)
    )
    silver_dupes = silver_total - silver_distinct

    row = fetch_one_with_retry(
        cur,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT b.event_id
            FROM iceberg.bronze.network_events_raw b
            LEFT JOIN iceberg.silver.network_events_clean s
                ON b.event_id = s.event_id
            WHERE s.event_id IS NULL
              AND b.ingestion_timestamp >= CURRENT_TIMESTAMP
                    - INTERVAL '{ORPHAN_WINDOW_HOURS}' HOUR
              AND b.event_id IS NOT NULL
              AND b.rsrp IS NOT NULL
        )
        """,
    )
    orphans = int(row[0]) if row else 0

    logger.info(
        "Bronze: total=%s distinct=%s duplicados=%s",
        bronze_total,
        bronze_distinct,
        bronze_dupes,
    )
    logger.info(
        "Silver: total=%s distinct=%s duplicados=%s",
        silver_total,
        silver_distinct,
        silver_dupes,
    )
    logger.info(
        "Órfãos bronze→silver (últimas %sh): %s",
        ORPHAN_WINDOW_HOURS,
        orphans,
    )

    issues: list[str] = []
    if bronze_dupes > 0:
        issues.append(f"bronze tem {bronze_dupes} linhas duplicadas por event_id")
    if silver_dupes > 0:
        issues.append(f"silver tem {silver_dupes} linhas duplicadas por event_id")
    if orphans > 0:
        issues.append(
            f"{orphans} event_id(s) em bronze (válidos) ainda não estão na silver"
        )

    if issues:
        msg = "Streaming QA: ALERTA — " + "; ".join(issues)
        logger.warning(msg)
        return msg

    msg = "Streaming QA: OK (sem duplicados nem órfãos na janela verificada)."
    logger.info(msg)
    return msg


@workflow
def jdpt_streaming_quality_check() -> str:
    return avaliar_streaming()
