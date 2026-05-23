"""Logs estruturados e contagens para auditoria streaming (Grafana/Loki)."""

from __future__ import annotations

import logging
from typing import Any

from workflow_functions.streaming_trino_client import fetch_one_with_retry


def fetch_scalar(cur, sql: str) -> int:
    row = fetch_one_with_retry(cur, sql)
    if not row or row[0] is None:
        return 0
    return int(row[0])


def log_table_insert(logger: logging.Logger, **fields: Any) -> None:
    """Linha única pesquisável em Loki: ``|= "table_insert"``."""
    parts = " ".join(f"{key}={value}" for key, value in fields.items())
    logger.info("table_insert %s", parts)
