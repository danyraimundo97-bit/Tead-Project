from flytekit import workflow

@workflow
def jdpt_lakehouse_pipeline(minio_access: str, minio_secret: str) -> str:
    # 1. Run Silver Transformations (These will run in parallel!)
    silver_tests = process_call_tests_to_silver(minio_access=minio_access, minio_secret=minio_secret)
    # silver_cdr = process_cdr_to_silver(...)
    # silver_logs = process_logs_to_silver(...)
    # silver_towers = process_towers_to_silver(...)
    
    # 2. Run Gold Transformations (Wait for Silver to finish by using the outputs as dummy inputs, or using >> syntax)
    # To force Flyte to wait, we use the `>>` operator to define execution order:
    silver_tests >> build_gold_churn_risk()
    
    return "Pipeline completed! Lakehouse is up to date."