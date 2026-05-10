from flytekit import workflow

from avaliar_silver import avaliar_silver
from build_gold_churn_risk import build_gold_churn_risk
from process_call_tests_to_silver import process_call_tests_to_silver
from process_cdr_to_silver import process_cdr_to_silver
from process_logs_to_silver import process_logs_to_silver
from process_towers_to_silver import process_towers_to_silver
from ensure_pipeline_layers import (
    ensure_gold_layer_environment,
    ensure_silver_layer_environment,
)
from build_gold_network_quality import build_gold_network_quality


@workflow
def jdpt_lakehouse_pipeline(target_date_str: str) -> str:
    # 1) Ambiente + inputs bronze + DDL silver
    silver_env = ensure_silver_layer_environment()

    # 2) Bronze → silver (paralelo)
    silver_cdr = process_cdr_to_silver()
    silver_logs = process_logs_to_silver(target_date_str=target_date_str)
    silver_tests = process_call_tests_to_silver(target_date_str=target_date_str)
    silver_towers = process_towers_to_silver()

    # 3) Verificação dos dados / qualidade da camada silver
    silver_checks = avaliar_silver()

    # 4) Schema gold + tabelas silver presentes
    gold_env = ensure_gold_layer_environment()

    # 5) Silver → gold
    gold_churn_risk = build_gold_churn_risk()
    gold_network_quality = build_gold_network_quality()

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

    return f"Lakehouse successfully updated for {target_date_str}!"