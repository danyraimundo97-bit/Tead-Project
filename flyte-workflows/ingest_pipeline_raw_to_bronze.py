"""Uma task Flyte: Raw → Bronze completo (usa ``bronze_storm_simulation``)."""

from flytekit import ImageSpec, task

from bronze_storm_simulation import get_storage_options, run_bronze_storm_simulation
from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

ingestion_image = ImageSpec(
    name="jdpt_ingestion_env",
    packages=["pandas", "s3fs", "boto3", "python-logging-loki"],
    registry="localhost:30000",
)


@task(container_image=ingestion_image, environment=TASK_ENV)
def ingest_pipeline_raw_to_bronze() -> str:
    """Monólito num pod: torres → logs → call_tests → CDR."""
    return run_bronze_storm_simulation(get_storage_options(), logger)
