"""Substituição atómica de tabelas Iceberg: ``CREATE OR REPLACE TABLE … AS <select>``."""

from __future__ import annotations

import logging


def replace_iceberg_table(
    conn,
    *,
    table_fqn: str,
    select_sql: str,
    logger: logging.Logger | None = None,
) -> None:
    """``select_sql`` deve ser um ``SELECT`` ou ``WITH … SELECT`` com colunas nomeadas."""
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE OR REPLACE TABLE {table_fqn} AS {select_sql.strip()}")
        cur.fetchall()
        if logger:
            logger.info("Iceberg replace: %s (CREATE OR REPLACE)", table_fqn)
    except Exception:
        if logger:
            logger.error("Iceberg replace failed for %s", table_fqn)
        raise
