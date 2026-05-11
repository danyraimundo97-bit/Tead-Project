import pandas as pd
import numpy as np
import warnings
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from loki_logging import get_logger

warnings.filterwarnings('ignore')
logger = get_logger(__name__)

ingestion_image = ImageSpec(
    name="jdpt_ingestion_env",
    packages=["pandas", "s3fs", "boto3", "python-logging-loki"],
    registry="localhost:30000"
)

def get_storage_options():
    """Lê as credenciais diretamente do ambiente seguro do Flyte Pod"""
    return {
        "key": TASK_ENV.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": TASK_ENV.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "client_kwargs": {"endpoint_url": TASK_ENV.get("MLFLOW_S3_ENDPOINT_URL", "http://host.docker.internal:9000")}
    }

@task(container_image=ingestion_image, environment=TASK_ENV)
def ingest_towers_raw_to_bronze() -> str:
    logger.info("=== GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) - TOWERS ===")
    storage_ops = get_storage_options()

    logger.info("A descarregar os dados RAW do MinIO...")
    df_towers = pd.read_csv('s3://warehouse/Dados_Raw/opencellid_pt.csv', sep=',', storage_options=storage_ops)

    logger.info("A filtrar as torres de Leiria...")
    df_towers_leiria = df_towers[
        (df_towers['lat'] >= 39.5) & (df_towers['lat'] <= 39.9) &
        (df_towers['lon'] >= -9.0) & (df_towers['lon'] <= -8.6)
    ].copy()

    # =====================================================================
    # HISTÓRICO DE TORRES
    # =====================================================================
    logger.info("A criar snapshots diários do inventário de Antenas...")
    dias_simulacao = pd.date_range(start='2026-01-20', end='2026-02-04', freq='D')
    snapshots_torres = []
    
    np.random.seed(42)
    torres_Leste_indices = df_towers_leiria[df_towers_leiria['lon'] > -8.80].index
    torres_destruidas_indices = np.random.choice(torres_Leste_indices, size=int(len(torres_Leste_indices)*0.80), replace=False)

    for dia in dias_simulacao:
        df_t = df_towers_leiria.copy()
        df_t['Snapshot_Date'] = dia.strftime('%Y-%m-%d')
        df_t['Status'] = 'ACTIVE'
        if dia >= pd.to_datetime('2026-01-28'):
            df_t.loc[torres_destruidas_indices, 'Status'] = 'DOWN'
        snapshots_torres.append(df_t)
    df_towers_leiria = pd.concat(snapshots_torres, ignore_index=True)

    df_towers_leiria['_ingested_at'] = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    # =====================================================================
    # PARTICIONAMENTO DIÁRIO NO MINIO (LAKEHOUSE)
    # =====================================================================
    logger.info("=== PARTICIONAR OS DADOS E ENVIAR PARA A CAMADA BRONZE ===")
    datas_towers = df_towers_leiria['Snapshot_Date'].dropna().unique()
    for dia in datas_towers:
        df_dia = df_towers_leiria[df_towers_leiria['Snapshot_Date'] == dia]
        caminho = f"s3://warehouse/bronze/towers/day={dia}/data.csv"
        df_dia.to_csv(caminho, index=False, sep=',', storage_options=storage_ops)
    
    logger.info(f" -> Towers particionadas em {len(datas_towers)} dias.")
    return "Towers Bronze Layer Created"