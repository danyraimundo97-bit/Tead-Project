from flytekit import workflow
from ingest_cdr_raw_to_bronze import ingest_cdr_raw_to_bronze
from ingest_logs_raw_to_bronze import ingest_logs_raw_to_bronze
from ingest_call_tests_raw_to_bronze import ingest_call_tests_raw_to_bronze
from ingest_towers_raw_to_bronze import ingest_towers_raw_to_bronze
from avaliar_bronze import avaliar_bronze

@workflow
def ingestion_workflow() -> str:
    """Orquestra a exportação diária dos sistemas de telecomunicações para a Camada Bronze."""
    
    # As tarefas de ingestão correm em paralelo
    cdr_status = ingest_cdr_raw_to_bronze()
    logs_status = ingest_logs_raw_to_bronze()
    call_status = ingest_call_tests_raw_to_bronze()
    towers_status = ingest_towers_raw_to_bronze()

    # A tarefa de avaliação
    bronze_checks = avaliar_bronze()

    # As dependências lógicas (a avaliação espera que a ingestão termine)
    cdr_status >> bronze_checks
    logs_status >> bronze_checks
    call_status >> bronze_checks
    towers_status >> bronze_checks
    
    return "Success: Simulated Telemetry Pushed to MinIO and Evaluated!"