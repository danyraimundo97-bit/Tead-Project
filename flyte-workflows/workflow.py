from flytekit import workflow


from process_call_tests_to_silver import process_call_tests_to_silver
from process_cdr_to_silver import process_cdr_to_silver
from process_logs_to_silver import process_logs_to_silver
from process_towers_to_silver import process_towers_to_silver
from avaliar_silver import avaliar_silver
from build_gold_network_quality import build_gold_network_quality
from build_gold_churn_risk import build_gold_churn_risk
from avaliar_gold import avaliar_gold
from ensure_pipeline_layers import (
    ensure_gold_layer_environment,
    ensure_silver_layer_environment,
)


@workflow
def jdpt_lakehouse_pipeline() -> str:
    # Ambiente + inputs bronze + DDL silver
    silver_env = ensure_silver_layer_environment()

    # Bronze → silver (paralelo; logs, call_tests, cdr_customers, towers)
    silver_cdr = process_cdr_to_silver()
    silver_logs = process_logs_to_silver()
    silver_tests = process_call_tests_to_silver()
    silver_towers = process_towers_to_silver()

    # Verificação dos dados / qualidade da camada silver
    silver_checks = avaliar_silver()

    # Schema gold + tabelas silver presentes
    gold_env = ensure_gold_layer_environment()

    # Silver → gold
    gold_churn_risk = build_gold_churn_risk()
    gold_network_quality = build_gold_network_quality()

    # Auditoria Final da Camada Gold
    gold_checks = avaliar_gold()

    # Dependências de Execução
    silver_env >> silver_cdr
    silver_env >> silver_logs
    silver_env >> silver_tests
    silver_env >> silver_towers

    silver_cdr >> silver_checks
    silver_logs >> silver_checks
    silver_tests >> silver_checks
    silver_towers >> silver_checks

    silver_checks >> gold_env

    gold_env >> gold_churn_risk
    gold_env >> gold_network_quality

    gold_churn_risk >> gold_checks
    gold_network_quality >> gold_checks

    return "Lakehouse Successfully Updated (Full Medallion Pipeline Executed and Evaluated)!"