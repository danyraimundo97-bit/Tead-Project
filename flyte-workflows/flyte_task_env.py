"""Environment variables injected into Flyte task pods.

Override at ``pyflyte register`` time via host env if needed:
``FLYTE_TASK_LOG_LEVEL``, ``FLYTE_TASK_LOKI_URL``,
``FLYTE_TASK_MINIO_ACCESS_KEY``, ``FLYTE_TASK_MINIO_SECRET_KEY``,
``FLYTE_TASK_MINIO_ENDPOINT``.
"""

from __future__ import annotations

import os

_MINIO_ENDPOINT_DEFAULT = "http://host.docker.internal:9000"

TASK_ENV: dict[str, str] = {
    "LOG_LEVEL": os.environ.get("FLYTE_TASK_LOG_LEVEL", "DEBUG"),
    "LOKI_URL": os.environ.get(
        "FLYTE_TASK_LOKI_URL",
        "http://host.docker.internal:3100/loki/api/v1/push",
    ),
    "AWS_ACCESS_KEY_ID": os.environ.get(
        "FLYTE_TASK_MINIO_ACCESS_KEY", "minioadmin"
    ),
    "AWS_SECRET_ACCESS_KEY": os.environ.get(
        "FLYTE_TASK_MINIO_SECRET_KEY", "minioadmin"
    ),
    "MLFLOW_S3_ENDPOINT_URL": os.environ.get(
        "FLYTE_TASK_MINIO_ENDPOINT", _MINIO_ENDPOINT_DEFAULT
    ),
}


def minio_s3_client():
    """Boto3 S3 client for MinIO; uses the same env vars as ``TASK_ENV``."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get(
            "MLFLOW_S3_ENDPOINT_URL", _MINIO_ENDPOINT_DEFAULT
        ),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )
