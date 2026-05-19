"""Cliente Trino partilhado para tasks streaming (conexão + retries em falhas transitórias)."""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Sequence

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS: tuple[int, ...] = (2, 5, 10)

_TRANSIENT_PATTERNS = re.compile(
    r"timeout|timed out|connection|network|broken pipe|"
    r"server shutting down|too many requests|service unavailable",
    re.IGNORECASE,
)

_NON_TRANSIENT_PATTERNS = re.compile(
    r"syntax error|column .* cannot be resolved|table .* does not exist|"
    r"schema .* does not exist|type mismatch|invalid cast",
    re.IGNORECASE,
)

PIPELINE_CHECKPOINT_NAME = "bronze_to_silver_network_events"


def get_trino_connection(
    *,
    catalog: str = "iceberg",
    schema: str | None = None,
):
    import trino

    kwargs: dict = {
        "host": TRINO_HOST,
        "port": TRINO_PORT,
        "user": TRINO_USER,
        "catalog": catalog,
    }
    if schema is not None:
        kwargs["schema"] = schema
    return trino.dbapi.connect(**kwargs)


def format_trino_timestamp(value: datetime) -> str:
    """Literal TIMESTAMP para SQL Trino (com timezone UTC)."""
    from datetime import timezone

    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"TIMESTAMP '{text} UTC'"


def is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    message = str(exc)
    if _NON_TRANSIENT_PATTERNS.search(message):
        return False
    if _TRANSIENT_PATTERNS.search(message):
        return True
    exc_name = type(exc).__name__
    if exc_name in ("TrinoConnectionError", "TrinoExternalError"):
        return True
    return False


def execute_with_retry(
    cur,
    sql: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: Sequence[int] = DEFAULT_BACKOFF_SECONDS,
) -> None:
    """Executa SQL com backoff em erros transitórios."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            cur.execute(sql)
            return
        except BaseException as exc:
            last_exc = exc
            if attempt >= max_attempts - 1 or not is_transient_error(exc):
                raise
            wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
            time.sleep(wait)
    if last_exc is not None:
        raise last_exc


def fetch_one_with_retry(
    cur,
    sql: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: Sequence[int] = DEFAULT_BACKOFF_SECONDS,
):
    """Executa SELECT e devolve uma linha com retry."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            cur.execute(sql)
            return cur.fetchone()
        except BaseException as exc:
            last_exc = exc
            if attempt >= max_attempts - 1 or not is_transient_error(exc):
                raise
            wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
            time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    return None


def read_silver_watermark(cur) -> datetime:
    """Watermark: checkpoint persistido, com fallback para MAX(ingested_at) na silver."""
    row = fetch_one_with_retry(
        cur,
        f"""
        SELECT last_silver_watermark
        FROM iceberg.bronze.streaming_checkpoints
        WHERE pipeline_name = '{PIPELINE_CHECKPOINT_NAME}'
        """,
    )
    if row and row[0] is not None:
        return row[0]

    row = fetch_one_with_retry(
        cur,
        """
        SELECT COALESCE(
            MAX(ingested_at),
            TIMESTAMP '1970-01-01 00:00:00 UTC'
        )
        FROM iceberg.silver.network_events_clean
        """,
    )
    return row[0] if row else datetime(1970, 1, 1)


def write_silver_checkpoint(cur) -> None:
    """Atualiza checkpoint após MERGE bronze → silver bem-sucedido."""
    merge_checkpoint = f"""
    MERGE INTO iceberg.bronze.streaming_checkpoints AS target
    USING (
        SELECT
            '{PIPELINE_CHECKPOINT_NAME}' AS pipeline_name,
            COALESCE(
                MAX(ingested_at),
                TIMESTAMP '1970-01-01 00:00:00 UTC'
            ) AS last_silver_watermark,
            CURRENT_TIMESTAMP AS updated_at
        FROM iceberg.silver.network_events_clean
    ) AS source
    ON target.pipeline_name = source.pipeline_name
    WHEN MATCHED THEN UPDATE SET
        last_silver_watermark = source.last_silver_watermark,
        updated_at = source.updated_at
    WHEN NOT MATCHED THEN INSERT (
        pipeline_name, last_silver_watermark, updated_at
    ) VALUES (
        source.pipeline_name,
        source.last_silver_watermark,
        source.updated_at
    )
    """
    execute_with_retry(cur, merge_checkpoint)
