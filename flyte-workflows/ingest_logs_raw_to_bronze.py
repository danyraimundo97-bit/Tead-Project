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
def ingest_logs_raw_to_bronze() -> str:
    logger.info("=== GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) - LOGS ===")
    storage_ops = get_storage_options()

    logger.info("A descarregar os dados RAW do MinIO...")
    df_logs = pd.read_csv('s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv', sep=';', storage_options=storage_ops)
    df_cdr = pd.read_csv('s3://warehouse/Dados_Raw/CDR-Call-Details.csv', sep=';', storage_options=storage_ops)

    logger.info("=== A APLICAR A SIMULAÇÃO DA TEMPESTADE EM LEIRIA ===")
    lat_offset = 39.7436 - 18.11
    lon_offset = -8.8071 - 83.40
    df_logs['Latitude'] = df_logs['Latitude'] + lat_offset
    df_logs['Longitude'] = df_logs['Longitude'] + lon_offset

    logger.info("A comprimir e ajustar a dimensão temporal...")
    df_logs['Timestamp'] = pd.to_datetime(df_logs['Timestamp'])
    data_inicio_alvo = pd.to_datetime('2026-01-20 00:00:00')
    df_logs['Timestamp'] = df_logs['Timestamp'] + (data_inicio_alvo - df_logs['Timestamp'].min())

    logs_expandidos = [df_logs.copy()]
    for i in range(1, 6):
        df_copy = df_logs.copy()
        df_copy['Timestamp'] = df_copy['Timestamp'] + pd.Timedelta(days=3 * i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)

    data_fim_alvo = pd.to_datetime('2026-02-04 23:59:59')
    df_logs = df_logs[df_logs['Timestamp'] <= data_fim_alvo]

    logger.info("A criar clientes afetados com chaves cruzadas...")
    np.random.seed(42)
    unique_phones = df_cdr['Phone Number'].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    df_logs['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_logs))

    logger.info("A injetar o desastre de Leiria...")
    inicio_tempestade = pd.to_datetime('2026-01-28 00:00:00')
    fim_tempestade = pd.to_datetime('2026-01-30 23:59:59')

    cond_log_tempestade_Leste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] > -8.80)
    cond_log_tempestade_Oeste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] <= -8.80)
    
    df_logs.loc[cond_log_tempestade_Leste, 'RSRP'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRP'].apply(lambda val: f"{int(val.replace(' dBm', '').strip()) - 35} dBm" if isinstance(val, str) and 'dBm' in val else val)
    df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'].apply(lambda val: f"{int(val.replace(' dB', '').strip()) - 15} dB" if isinstance(val, str) and 'dB' in val else val)
    
    df_logs.loc[cond_log_tempestade_Leste, 'SINR'] = np.random.uniform(-15.0, -5.0, size=cond_log_tempestade_Leste.sum()).astype(str)
    df_logs.loc[cond_log_tempestade_Leste, 'Velocity(km/h)'] = "0.0 km/h" 
    df_logs.loc[cond_log_tempestade_Leste, 'NetworkType'] = np.random.choice(['UMTS', 'GSM'], size=cond_log_tempestade_Leste.sum()).astype(str)

    df_logs.loc[cond_log_tempestade_Oeste, 'Downlink(Mbps)'] = "1.5 Mbps" 
    df_logs.loc[cond_log_tempestade_Oeste, 'Uplink(Mbps)'] = "0.2 Mbps" 

    if 'DeviceID' in df_logs.columns:
        df_logs = df_logs.drop(columns=['DeviceID'])

    df_logs['_ingested_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    logger.info("=== PARTICIONAR OS DADOS E ENVIAR PARA A CAMADA BRONZE ===")
    df_logs['Data_Fatia'] = pd.to_datetime(df_logs['Timestamp']).dt.strftime('%Y-%m-%d')
    datas_logs = df_logs['Data_Fatia'].dropna().unique()
    for dia in datas_logs:
        df_dia = df_logs[df_logs['Data_Fatia'] == dia].drop(columns=['Data_Fatia'])
        df_dia.to_csv(f"s3://warehouse/bronze/network_logs/day={dia}/data.csv", index=False, sep=';', storage_options=storage_ops)
    
    logger.info(f" -> Network Logs particionados em {len(datas_logs)} dias.")
    return "Network Logs Bronze Layer Created"