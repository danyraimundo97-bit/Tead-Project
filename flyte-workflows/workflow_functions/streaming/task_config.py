"""ImageSpec e kwargs partilhados pelas tasks streaming."""

from __future__ import annotations

from datetime import timedelta

from flytekit import ImageSpec

from flyte_task_env import TASK_ENV

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
