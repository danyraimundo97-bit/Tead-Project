"""Flyte task: silver → gold (logs Loki: module=streaming_silver_to_gold)."""

from __future__ import annotations

from flytekit import task

from workflow_functions.loki_logging import get_logger
from workflow_functions.streaming.silver_to_gold import process_silver_to_gold
from workflow_functions.streaming.task_config import STREAMING_TASK_KWARGS

logger = get_logger(__name__)


@task(**STREAMING_TASK_KWARGS)
def silver_to_gold_network_events_hourly() -> str:
    try:
        return process_silver_to_gold(logger)
    except Exception as exc:
        logger.exception("Silver → gold falhou")
        raise RuntimeError(str(exc)) from exc
