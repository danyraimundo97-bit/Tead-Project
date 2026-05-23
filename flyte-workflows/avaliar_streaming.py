"""Flyte task/workflow: QA streaming (logs Loki: module=avaliar_streaming)."""

from __future__ import annotations

from flytekit import task, workflow

from workflow_functions.loki_logging import get_logger
from workflow_functions.streaming.quality import avaliar_streaming_quality
from workflow_functions.streaming.task_config import STREAMING_TASK_KWARGS

logger = get_logger(__name__)


@task(**STREAMING_TASK_KWARGS)
def avaliar_streaming() -> str:
    return avaliar_streaming_quality(logger)


@workflow
def jdpt_streaming_quality_check() -> str:
    return avaliar_streaming()
