from flytekit import workflow

from avaliar_bronze import avaliar_bronze
from ingest_pipeline_raw_to_bronze import ingest_pipeline_raw_to_bronze


@workflow
def ingestion_workflow() -> str:
    """
    Um pod: pipeline Raw→Bronze completo (``run_bronze_storm_simulation``) → avaliação.
    """
    pipeline = ingest_pipeline_raw_to_bronze()
    bronze_checks = avaliar_bronze()
    pipeline >> bronze_checks
    return "Success: Simulated Telemetry Pushed to MinIO and Evaluated!"
