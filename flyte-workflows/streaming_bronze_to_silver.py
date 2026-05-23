"""Flyte task: bronze → silver (logs Loki: module=workflows_incremental_streaming)."""

from __future__ import annotations

from flytekit import task

from workflow_functions.loki_logging import get_logger
from workflow_functions.streaming.bronze_to_silver import process_bronze_to_silver
from workflow_functions.streaming.task_config import STREAMING_TASK_KWARGS

logger = get_logger(__name__)


@task(**STREAMING_TASK_KWARGS)
def ingest_bronze_to_silver() -> str:
    try:
        return process_bronze_to_silver(logger)
    except Exception as exc:
        logger.exception("Bronze → silver incremental falhou")
        raise RuntimeError(str(exc)) from exc
