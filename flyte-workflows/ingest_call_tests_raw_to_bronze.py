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
def ingest_call_tests_raw_to_bronze() -> str:
    logger.info("=== GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) - CALL TESTS ===")
    storage_ops = get_storage_options()

    logger.info("A descarregar os dados RAW do MinIO...")
    df_call = pd.read_csv('s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv', sep=';', storage_options=storage_ops)
    df_cdr = pd.read_csv('s3://warehouse/Dados_Raw/CDR-Call-Details.csv', sep=';', storage_options=storage_ops)
    df_logs = pd.read_csv('s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv', sep=';', storage_options=storage_ops)

    logger.info("A comprimir e ajustar a dimensão temporal...")
    segundos_em_16_dias = 16 * 24 * 60 * 60
    random_deltas = pd.to_timedelta(np.random.randint(0, segundos_em_16_dias, size=len(df_call)), unit='s')
    df_call['Date Of Test'] = pd.to_datetime('2026-01-20 00:00:00') + random_deltas

    logger.info("A criar clientes afetados com chaves cruzadas...")
    np.random.seed(42)
    unique_phones = df_cdr['Phone Number'].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    df_call['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_call))

    # Replicar rapidamente a lógica dos logs para descobrir os afetados no Leste
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

    logger.info("A injetar o desastre de Leiria...")
    cond_call_tempestade = (df_call['Date Of Test'] >= inicio_tempestade) & (df_call['Date Of Test'] <= fim_tempestade) & (df_call['Phone_Number'].isin(clientes_Leste))
    
    df_call.loc[cond_call_tempestade, 'Call Test Result'] = 'DROP'
    df_call.loc[cond_call_tempestade, 'MOS'] = np.random.uniform(1.0, 1.8, size=cond_call_tempestade.sum()).astype(str)
    df_call['MOS'] = df_call['MOS'].astype(str).str.replace('.', ',')
    df_call.loc[cond_call_tempestade, 'Call Test Duration (s)'] = np.random.uniform(2.0, 12.0, size=cond_call_tempestade.sum()).astype(str)
    df_call['Call Test Duration (s)'] = df_call['Call Test Duration (s)'].astype(str).str.replace('.', ',')
    df_call.loc[cond_call_tempestade, 'Call Test Setup Time (s)'] = np.random.uniform(15.0, 45.0, size=cond_call_tempestade.sum()).astype(str)
    df_call['Call Test Setup Time (s)'] = df_call['Call Test Setup Time (s)'].astype(str).str.replace('.', ',')
    df_call.loc[cond_call_tempestade, 'Distance from site (m)'] = np.random.uniform(5000, 15000, size=cond_call_tempestade.sum()).astype(str)
    df_call['Distance from site (m)'] = df_call['Distance from site (m)'].astype(str).str.replace('.', ',')

    df_call['_ingested_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    logger.info("=== PARTICIONAR OS DADOS E ENVIAR PARA A CAMADA BRONZE ===")
    df_call['Data_Fatia'] = pd.to_datetime(df_call['Date Of Test']).dt.strftime('%Y-%m-%d')
    datas_call = df_call['Data_Fatia'].dropna().unique()
    for dia in datas_call:
        df_dia = df_call[df_call['Data_Fatia'] == dia].drop(columns=['Data_Fatia'])
        df_dia.to_csv(f"s3://warehouse/bronze/call_tests/day={dia}/data.csv", index=False, sep=';', storage_options=storage_ops)
    
    logger.info(f" -> Call Tests particionados em {len(datas_call)} dias.")
    return "Call Tests Bronze Layer Created"