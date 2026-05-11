import pandas as pd
import numpy as np
import warnings
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from loki_logging import get_logger

warnings.filterwarnings('ignore')
logger = get_logger(__name__)

ingestion_image = ImageSpec(name="jdpt_ingestion_env", packages=["pandas", "s3fs", "boto3", "python-logging-loki"], registry="localhost:30000")

def get_storage_options():
    return {
        "key": TASK_ENV.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": TASK_ENV.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "client_kwargs": {"endpoint_url": TASK_ENV.get("MLFLOW_S3_ENDPOINT_URL", "http://host.docker.internal:9000")}
    }

@task(container_image=ingestion_image, environment=TASK_ENV)
def ingest_cdr_raw_to_bronze() -> str:
    logger.info("=== GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) - CDR ===")
    storage_ops = get_storage_options()

    logger.info("A descarregar os dados RAW do MinIO...")
    df_cdr = pd.read_csv('s3://warehouse/Dados_Raw/CDR-Call-Details.csv', sep=';', storage_options=storage_ops)
    df_logs = pd.read_csv('s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv', sep=';', storage_options=storage_ops)

    logger.info("A descobrir clientes afetados com chaves cruzadas...")
    np.random.seed(42)
    unique_phones = df_cdr['Phone Number'].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)

    # Replicar rapidamente a lógica dos logs para garantir que afetamos as mesmas pessoas no Leste
    lon_offset = -8.8071 - 83.40
    df_logs['Longitude'] = df_logs['Longitude'] + lon_offset
    df_logs['Timestamp'] = pd.to_datetime(df_logs['Timestamp'])
    df_logs['Timestamp'] = df_logs['Timestamp'] + (pd.to_datetime('2026-01-20 00:00:00') - df_logs['Timestamp'].min())
    
    logs_expandidos = [df_logs.copy()]
    for i in range(1, 6):
        df_copy = df_logs.copy()
        df_copy['Timestamp'] = df_copy['Timestamp'] + pd.Timedelta(days=3 * i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)
    df_logs['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_logs))

    inicio_tempestade = pd.to_datetime('2026-01-28 00:00:00')
    fim_tempestade = pd.to_datetime('2026-01-30 23:59:59')
    cond_log_tempestade_Leste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] > -8.80)
    clientes_Leste = df_logs.loc[cond_log_tempestade_Leste, 'Phone_Number'].unique()

    logger.info("A injetar o desastre de Leiria na Faturação...")
    idx_cdr_afetados = df_cdr['Phone Number'].isin(clientes_Leste)
    df_cdr.loc[idx_cdr_afetados, 'CustServ Calls'] += np.random.randint(3, 8, size=idx_cdr_afetados.sum())
    df_cdr.loc[idx_cdr_afetados, 'Churn'] = np.random.choice([True, False], p=[0.85, 0.15], size=idx_cdr_afetados.sum())

    df_cdr['_ingested_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    logger.info("=== PARTICIONAR OS DADOS E ENVIAR PARA A CAMADA BRONZE ===")
    datas_gerais = pd.date_range(start='2026-01-20', end='2026-02-04').strftime('%Y-%m-%d')
    for dia in datas_gerais:
        caminho = f"s3://warehouse/bronze/cdr_customers/day={dia}/data.csv"
        df_cdr.to_csv(caminho, index=False, sep=';', storage_options=storage_ops)
    
    logger.info(f" -> CDR (Faturação) replicado em {len(datas_gerais)} dias.")
    return "CDR Bronze Layer Created"