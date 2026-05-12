"""
Script de Avaliação: avaliar_bronze.py
Objetivo: Data Profiling da Camada Bronze.
Verifica: Bounding Box Geográfica (Leiria), Janela Temporal da simulação, retenção de chaves de quarentena e formatação injetada.
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
    paginator = s3.get_paginator('list_objects_v2')
    dfs = []
    for page in paginator.paginate(Bucket="warehouse", Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".csv"):
                resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                dfs.append(pd.read_csv(resp["Body"], sep=sep))
    if not dfs:
        raise ValueError(f"CRÍTICO: Nenhum ficheiro encontrado no prefixo {prefix}")
    return pd.concat(dfs, ignore_index=True)


@task(container_image=ingestion_image, environment=TASK_ENV)
def avaliar_bronze():
    logger.info("="*60)
    logger.info(" INICIANDO DATA PROFILING PROFUNDO: CAMADA BRONZE")
    logger.info("="*60)

    try:
        s3 = minio_s3_client()
        df_logs = _load_bronze_partitioned(s3, "bronze/network_logs/", sep=";")
        df_towers = _load_bronze_partitioned(s3, "bronze/towers/", sep=",")
        df_call = _load_bronze_partitioned(s3, "bronze/call_tests/", sep=";")
        df_cdr = _load_bronze_partitioned(s3, "bronze/cdr_customers/", sep=";")

        # --- AVALIAÇÃO: TORRES ---
        logger.info("\n[1] TORRES DE LEIRIA (Validação Geográfica e Status)")
        lat_min, lat_max = df_towers['lat'].min(), df_towers['lat'].max()
        lon_min, lon_max = df_towers['lon'].min(), df_towers['lon'].max()
        logger.info(f" -> [GEO] Bounding Box Torres: Lat [{lat_min:.3f}, {lat_max:.3f}] | Lon [{lon_min:.3f}, {lon_max:.3f}]")
        if lat_min < 39.0 or lat_max > 40.0:
            logger.error(" -> ERRO CRÍTICO: Existem torres fora da região Centro/Leiria!")
        
        down_count = (df_towers['Status'] == 'DOWN').sum()
        logger.info(f" -> [NEGÓCIO] Torres no estado DOWN durante todo o período: {down_count}")

        # --- AVALIAÇÃO: NETWORK LOGS ---
        logger.info("\n[2] NETWORK LOGS (Verificação da Tempestade)")
        df_logs["Timestamp"] = pd.to_datetime(df_logs["Timestamp"])
        dt_min, dt_max = df_logs["Timestamp"].min(), df_logs["Timestamp"].max()
        logger.info(f" -> [TEMPO] Janela Simulada: {dt_min} até {dt_max} (Esperado: 20 Jan a 04 Fev 2026)")
        
        lat_log_min, lat_log_max = df_logs['Latitude'].min(), df_logs['Latitude'].max()
        logger.info(f" -> [GEO] Bounding Box Telemóveis: Lat [{lat_log_min:.3f}, {lat_log_max:.3f}]")
        if lat_log_max > 40.0:
            logger.error(" -> ERRO CRÍTICO: O offset geográfico falhou! Há telemóveis em Espanha ou no Oceano.")
        
        if "DeviceID" not in df_logs.columns:
            logger.error(" -> ERRO CRÍTICO: A coluna 'DeviceID' desapareceu! A Quarentena da Silver vai falhar por KeyError.")
        else:
            logger.info(" -> [ESTRUTURA] Coluna 'DeviceID' preservada com sucesso para auditoria na Silver.")

        # --- AVALIAÇÃO: CALL TESTS ---
        logger.info("\n[3] CALL TESTS (Sujidade Intencional)")
        drops = (df_call['Call Test Result'] == 'DROP').sum()
        logger.info(f" -> [NEGÓCIO] Total de Call Drops injetados: {drops}")
        has_comma = df_call['MOS'].astype(str).str.contains(',').any()
        logger.info(f" -> [FORMATO] Numéricos com padrão Europeu (vírgulas em vez de pontos)? {'SIM (Esperado)' if has_comma else 'NÃO (Erro)'}")

        # --- AVALIAÇÃO: CDR ---
        logger.info("\n[4] CDR CUSTOMERS (Risco e Duplicação)")
        duplicados = df_cdr.duplicated().sum()
        logger.info(f" -> [FORMATO] Linhas duplicadas injetadas para teste de limpeza: {duplicados}")
        churn_rate = (df_cdr['Churn'] == True).mean() * 100
        logger.info(f" -> [NEGÓCIO] Taxa Global de Churn da amostra: {churn_rate:.1f}%")

        logger.info("="*60)
        logger.info(" AVALIAÇÃO BRONZE CONCLUÍDA: DADOS PRONTOS PARA ETL (SILVER) ")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Falha Crítica na Avaliação Bronze: {str(e)}")
        raise e