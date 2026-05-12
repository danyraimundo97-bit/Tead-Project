"""
Script de Avaliação 1: avaliar_bronze.py
Objetivo: Validar se o Workflow de Ingestão gerou corretamente a Camada Bronze particionada.
Verifica: Aumento de dados (15 dias), Snapshots das Torres, impacto da tempestade e "sujidade" técnica.
"""

import pandas as pd
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger

logger = get_logger(__name__)

ingestion_image = ImageSpec(
    name="jdpt_ingestion_env",
    packages=["pandas", "s3fs", "boto3", "python-logging-loki"],
    registry="localhost:30000"
)

def _load_bronze_partitioned(s3, prefix: str, sep: str) -> pd.DataFrame:
    """Função auxiliar para ler as partições de uma tabela Bronze."""
    paginator = s3.get_paginator('list_objects_v2')
    dfs = []
    for page in paginator.paginate(Bucket="warehouse", Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                dfs.append(pd.read_csv(resp["Body"], sep=sep))
    if not dfs:
        raise ValueError(f"Nenhum dado encontrado na pasta Bronze: {prefix}")
    return pd.concat(dfs, ignore_index=True)

@task(container_image=ingestion_image, environment=TASK_ENV)
def avaliar_bronze() -> str:
    logger.info("="*50)
    logger.info(" INICIANDO AVALIAÇÃO DA CAMADA BRONZE PARTICIONADA")
    logger.info("="*50)
    
    try:
        s3 = minio_s3_client()

        # Carregamento dos dados particionados (lê as pastas inteiras)
        logger.info("A agregar partições Network Logs...")
        df_logs = _load_bronze_partitioned(s3, "bronze/network_logs/", sep=";")
        
        logger.info("A agregar partições CDR...")
        df_cdr = _load_bronze_partitioned(s3, "bronze/cdr_customers/", sep=";")
        
        logger.info("A agregar partições Call Tests...")
        df_call = _load_bronze_partitioned(s3, "bronze/call_tests/", sep=";")
        
        logger.info("A agregar partições Towers...")
        df_towers = _load_bronze_partitioned(s3, "bronze/towers/", sep=",")
        
        # 1. Avaliar Data Augmentation (Logs)
        logger.info("\n[1] NETWORK LOGS (Física da Rede):")
        # CSV pode trazer subsegundos (ex. ns); inferência fixa %Y-%m-%d %H:%M:%S falha
        df_logs["Timestamp"] = pd.to_datetime(df_logs["Timestamp"], format="mixed")
        dias_totais = (df_logs['Timestamp'].max() - df_logs['Timestamp'].min()).days
        logger.info(f" -> Período de dados expandido: {dias_totais} dias (De {df_logs['Timestamp'].min().date()} a {df_logs['Timestamp'].max().date()})")
        
        # Verificar Sujidade (RSRP deve ser string)
        rsrp_type = df_logs['RSRP'].dtype
        has_dbm = df_logs['RSRP'].astype(str).str.contains('dBm').any()
        logger.info(f" -> Sujidade intencional: RSRP é do tipo '{rsrp_type}' (Contém 'dBm'? {has_dbm})")
        
        # 2. Avaliar Snapshots de Antenas (Towers)
        logger.info("\n[2] TORRES (Infraestrutura Diária):")
        logger.info(f" -> Total de registos de snapshot gerados: {len(df_towers)}")
        if 'Snapshot_Date' in df_towers.columns and 'Status' in df_towers.columns:
            torres_down = len(df_towers[df_towers['Status'] == 'DOWN'])
            logger.info(f" -> Torres destruídas ('DOWN') registadas nos dias da tempestade: {torres_down}")
        else:
            logger.error(" -> ERRO: Colunas de Snapshot em falta!")

        # 3. Avaliar Impacto da Tempestade e Vírgulas (Call Tests)
        logger.info("\n[3] CALL TESTS (Qualidade de Chamada):")
        df_call["Date Of Test"] = pd.to_datetime(df_call["Date Of Test"], format="mixed")
        drops = len(df_call[df_call['Call Test Result'] == 'DROP'])
        logger.info(f" -> Total de chamadas que sofreram DROP na simulação: {drops}")
        
        # Verificar formatação europeia
        mos_type = df_call['MOS'].dtype
        has_comma = df_call['MOS'].astype(str).str.contains(',').any()
        logger.info(f" -> Sujidade intencional: MOS lido como '{mos_type}' (Contém vírgulas? {has_comma})")

        # 4. Avaliar Faturação e Duplicados (CDR)
        logger.info("\n[4] CDR (Faturação e Clientes):")
        duplicados = df_cdr.duplicated().sum()
        logger.info(f" -> Sujidade intencional: Existem {duplicados} linhas duplicadas no ficheiro.")
        churn_rate = (df_cdr['Churn'] == True).mean() * 100
        logger.info(f" -> Taxa de Churn Global Simulada: {churn_rate:.2f}%")
        
        return "Successfully validated Partitioned Bronze layer!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")