from flytekit import workflow
from process_call_tests_to_silver import process_call_tests_to_silver
from process_cdr_to_silver import process_cdr_to_silver
from process_logs_to_silver import process_logs_to_silver
from process_towers_to_silver import process_towers_to_silver
from build_gold_churn_risk import build_gold_churn_risk
from avaliar_silver import avaliar_silver

@workflow
def jdpt_lakehouse_pipeline(target_date_str: str) -> str:
    
    # ---------------------------------------------------------
    # STEP 1: The Silver Layer (These 4 run in parallel!)
    # ---------------------------------------------------------
    silver_cdr = process_cdr_to_silver()

    silver_logs = process_logs_to_silver(target_date_str=target_date_str)

    silver_tests = process_call_tests_to_silver(target_date_str=target_date_str)

    silver_towers = process_towers_to_silver()

    silver_aval = avaliar_silver() # This is just a check task to validate the Silver layer before we build Gold. It doesn't return anything, just logs info.

    
    # ---------------------------------------------------------
    # STEP 2: The Gold Layer (The Data Products)
    # ---------------------------------------------------------
    gold_churn_risk = build_gold_churn_risk()
    # gold_network_quality = build_gold_network_quality() # (If you made this task too!)

    # ---------------------------------------------------------
    # STEP 3: The Rules (Dependencies)
    # ---------------------------------------------------------
    # We force the Gold task to wait for the required Silver tasks
    silver_cdr >> silver_aval
    silver_tests >> silver_aval
    silver_logs >> silver_aval
    silver_towers >> silver_aval
    silver_aval >> gold_churn_risk
    
    return f"Lakehouse successfully updated for {target_date_str}!"