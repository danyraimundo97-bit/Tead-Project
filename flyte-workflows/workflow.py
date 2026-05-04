from flytekit import task, workflow, LaunchPlan
import os

@task
def check_mlflow_config(mlflow_url: str, minio_access: str, minio_secret: str) -> str:
    return f"URL: {mlflow_url} | Access: {minio_access} | Secret: {minio_secret}"

@workflow
def my_workflow(mlflow_url: str, minio_access: str, minio_secret: str) -> str:
    return check_mlflow_config(mlflow_url=mlflow_url, minio_access=minio_access, minio_secret=minio_secret)

local_compose_config = LaunchPlan.get_or_create(
    name="workflow_compose_config",
    workflow=my_workflow,
    default_inputs={
        "mlflow_url": "http://host.docker.internal:9000",
        "minio_access": "minioadmin",
        "minio_secret": "minioadmin" 
    }
)

if __name__ == "__main__":
    print(my_workflow(mlflow_url="http://host.docker.internal:9000", minio_access="minioadmin", minio_secret="minioadmin"))