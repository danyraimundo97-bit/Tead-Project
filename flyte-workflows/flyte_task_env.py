"""Environment variables injected into Flyte task pods.

Override at ``pyflyte register`` time via host env if needed:
``FLYTE_TASK_LOG_LEVEL``, ``FLYTE_TASK_LOKI_URL``.
"""

from __future__ import annotations

import os

TASK_ENV: dict[str, str] = {
    "LOG_LEVEL": os.environ.get("FLYTE_TASK_LOG_LEVEL", "DEBUG"),
    "LOKI_URL": os.environ.get(
        "FLYTE_TASK_LOKI_URL",
        "http://host.docker.internal:3100/loki/api/v1/push",
    ),
}
