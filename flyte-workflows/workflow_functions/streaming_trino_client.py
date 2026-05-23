"""Cliente Trino partilhado para tasks streaming (conexão + retries em falhas transitórias)."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Sequence

TRINO_HOST = "host.docker.internal"
TRINO_PORT = 8080
TRINO_USER = "tead"

DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BACKOFF_SECONDS: tuple[int, ...] = (2, 5, 10)

PIPELINE_CHECKPOINT_NAME = "bronze_to_silver_network_events"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _reraise_trino(exc: BaseException) -> None:
    """Flyte falha ao serializar TrinoUserError; usar RuntimeError com mensagem legível."""
    raise RuntimeError(f"Trino/SQL error: {exc}") from exc


def normalize_watermark(value: Any) -> datetime:
    """Converte valor devolvido pelo driver Trino para datetime com timezone UTC."""
    #Se o valor for None, retorna o epoch
    #Se o valor for um datetime, retorna o datetime como timezone UTC
    #Se o valor for uma string, converte para datetime como timezone UTC
    #Se o valor for de um tipo não suportado, retorna um erro
    if value is None:
        return _EPOCH
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        text = value.replace(" UTC", "").strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise RuntimeError(f"Invalid watermark timestamp: {value!r}") from exc
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    raise RuntimeError(f"Unsupported watermark type: {type(value)!r} ({value!r})")


def ensure_streaming_prerequisites(cur) -> None:
    """Garante tabela de checkpoint (evita falha se migrate/setup não foi corrido)."""
    execute_with_retry(
        cur,
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
    )


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
        # ** this is to convert the dictionary into keyword arguments
    return trino.dbapi.connect(**kwargs)


def format_trino_timestamp(value: datetime) -> str:
    """Literal TIMESTAMP para SQL Trino (com timezone UTC)."""
    from datetime import timezone

    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    text = value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"TIMESTAMP '{text} UTC'"


def execute_with_retry(
    cur,
    sql: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: Sequence[int] = DEFAULT_BACKOFF_SECONDS,
) -> None:
    """Executa SQL com até ``max_attempts`` tentativas (backoff entre falhas)."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            cur.execute(sql)
            return
        except BaseException as exc:
            last_exc = exc
            if attempt >= max_attempts - 1:
                _reraise_trino(exc)
            time.sleep(backoff_seconds[min(attempt, len(backoff_seconds) - 1)])
    if last_exc is not None:
        _reraise_trino(last_exc)


def fetch_one_with_retry(
    cur,
    sql: str,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    backoff_seconds: Sequence[int] = DEFAULT_BACKOFF_SECONDS,
):
    """Executa SELECT e devolve uma linha com até ``max_attempts`` tentativas."""
    last_exc: BaseException | None = None
    for attempt in range(max_attempts):
        try:
            cur.execute(sql)
            return cur.fetchone()
        except BaseException as exc:
            last_exc = exc
            if attempt >= max_attempts - 1:
                _reraise_trino(exc)
            time.sleep(backoff_seconds[min(attempt, len(backoff_seconds) - 1)])
    if last_exc is not None:
        _reraise_trino(last_exc)
    return None


def read_silver_watermark(cur) -> datetime:
    """Watermark: checkpoint persistido, com fallback para MAX(ingested_at) na silver."""
    ensure_streaming_prerequisites(cur)

    row = None
    try:
        row = fetch_one_with_retry(
            cur,
            f"""
            SELECT last_silver_watermark
            FROM iceberg.bronze.streaming_checkpoints
            WHERE pipeline_name = '{PIPELINE_CHECKPOINT_NAME}'
            """,
        )
    except RuntimeError as exc:
        if "does not exist" not in str(exc).lower():
            raise
        # Tabela em falta: fallback para silver

    if row and row[0] is not None:
        return normalize_watermark(row[0])

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
    return normalize_watermark(row[0] if row else None)


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
