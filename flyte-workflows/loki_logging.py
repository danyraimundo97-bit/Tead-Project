"""Shared Grafana Loki logging (view logs in Grafana Explore).

Set ``LOG_LEVEL`` to control verbosity (standard for apps and batch jobs), e.g.
``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``. Defaults to ``INFO``.
Flyte tasks inject defaults via ``flyte_task_env.TASK_ENV`` (override with
``FLYTE_TASK_LOG_LEVEL`` / ``FLYTE_TASK_LOKI_URL`` when registering).
"""

from __future__ import annotations

import logging
import os

import logging_loki.handlers

_DEFAULT_LOKI_PUSH_URL = "http://host.docker.internal:3100/loki/api/v1/push"


def _log_level_from_env() -> int:
    name = (os.environ.get("LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, name, None)
    return level if isinstance(level, int) else logging.INFO


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = _log_level_from_env()
    stream = logging.StreamHandler()
    stream.setFormatter(logging.Formatter("%(levelname)s [%(name)s] %(message)s"))
    stream.setLevel(level)
    logger.addHandler(stream)
    loki_handler = logging_loki.handlers.LokiHandler(
        url=os.environ.get("LOKI_URL", _DEFAULT_LOKI_PUSH_URL),
        tags={"application": "tead", "module": name},
        version="1",
    )
    loki_handler.setLevel(level)
    logger.addHandler(loki_handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
