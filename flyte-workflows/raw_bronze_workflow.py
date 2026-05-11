from flytekit import workflow
from ingest_cdr_raw_to_bronze import ingest_cdr_raw_to_bronze
from ingest_logs_raw_to_bronze import ingest_logs_raw_to_bronze
from ingest_call_tests_raw_to_bronze import ingest_call_tests_raw_to_bronze
from ingest_towers_raw_to_bronze import ingest_towers_raw_to_bronze

@workflow
def ingestion_workflow() -> str:
    """Orquestra a exportação diária dos sistemas de telecomunicações para a Camada Bronze."""
    
    cdr_status = ingest_cdr_raw_to_bronze()
    logs_status = ingest_logs_raw_to_bronze()
    call_status = ingest_call_tests_raw_to_bronze()
    towers_status = ingest_towers_raw_to_bronze()
    
    return "Success: Simulated Telemetry and Billing Systems Pushed to MinIO Bronze Layer"