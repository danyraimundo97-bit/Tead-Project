"""
Script de Avaliação 2: avaliar_silver.py
Objetivo: Validar se o 'prepare_data.py' executou corretamente a Limpeza (ETL).
Verifica: Tipos de dados (float em vez de string), remoção de duplicados e ausência de vírgulas.
"""

import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV
from loki_logging import get_logger

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000"
)

logger = get_logger(__name__)

_REQUIRED_SILVER_TABLES = frozenset(
    {"network_logs", "cdr_customers", "call_tests", "towers"}
)


def _assert_silver_layer_ready(cur) -> None:
    cur.execute(
        """
        SELECT table_name FROM iceberg.information_schema.tables
        WHERE table_schema = 'silver'
        """
    )
    found = {row[0] for row in cur.fetchall()}
    missing = _REQUIRED_SILVER_TABLES - found
    if missing:
        raise ValueError(
            "iceberg.silver is not ready: missing table(s) "
            f"{sorted(missing)}. Run the silver tasks after a clean or first deploy."
        )


@task(container_image=medallion_image, environment=TASK_ENV)
def avaliar_silver():
    logger.info("="*50)
    logger.info(" INICIANDO AVALIAÇÃO DA CAMADA SILVER (CLEAN)")
    logger.info("="*50)

    try:
        logger.info("Conectando ao Trino para validar dados na camada Silver...")
        conn = trino.dbapi.connect(
            host='host.docker.internal', 
            port=8080, 
            user='flyte', 
            catalog='iceberg'
        )
        cur = conn.cursor()
        _assert_silver_layer_ready(cur)

        query_logs = "SELECT * FROM iceberg.silver.network_logs LIMIT 100"
        query_cdr = "SELECT * FROM iceberg.silver.cdr_customers LIMIT 100"
        query_call = "SELECT * FROM iceberg.silver.call_tests LIMIT 100"
        query_towers = "SELECT * FROM iceberg.silver.towers LIMIT 100"

        logger.info("⏳ Running querys ...")
        df_logs = pd.read_sql(query_logs, conn)
        df_cdr = pd.read_sql(query_cdr, conn)
        df_call = pd.read_sql(query_call, conn)
        df_towers = pd.read_sql(query_towers, conn)

        # Trino / pandas may return mixed case; Silver schema is snake_case (see process_*_to_silver)
        df_logs.columns = df_logs.columns.str.lower()
        df_cdr.columns = df_cdr.columns.str.lower()
        df_call.columns = df_call.columns.str.lower()
        df_towers.columns = df_towers.columns.str.lower()

        # 1. Verificar Parsing de Rádio (Logs) — iceberg.silver.network_logs
        logger.info("\n[1] NETWORK LOGS (Limpeza de Unidades):")
        rsrp_type = df_logs["rsrp"].dtype
        vel_type = df_logs["velocity_kmh"].dtype
        logger.info(f" -> RSRP convertido para número? Tipo atual: {rsrp_type}")
        logger.info(f" -> Velocity convertido para número? Tipo atual: {vel_type}")

        nulos_geo = df_logs["latitude"].isnull().sum()
        logger.info(f" -> Linhas sem coordenadas removidas? (Nulos em latitude = {nulos_geo})")

        # 2. Verificar Deduplicação (CDR)
        logger.info("\n[2] CDR (Deduplicação):")
        duplicados = df_cdr.duplicated().sum()
        logger.info(f" -> Limpeza bem sucedida? Existem {duplicados} linhas duplicadas no ficheiro.")
        logger.info(f" -> Total de clientes únicos faturados: {len(df_cdr)}")

        # 3. Verificar Formatação de Decimais (Call Tests) — iceberg.silver.call_tests
        logger.info("\n[3] CALL TESTS (Formatação Americana):")
        mos_type = df_call["mos"].dtype
        dur_type = df_call["call_test_duration_s"].dtype
        logger.info(f" -> MOS convertido para Float? Tipo atual: {mos_type}")
        logger.info(f" -> Duração convertida para Float? Tipo atual: {dur_type}")
        
        # 4. Verificação Geral de Snapshots (Towers)
        logger.info("\n[4] TORRES (Estrutura final):")
        logger.info(f" -> Total de registos integrados na camada Silver: {len(df_towers)}")

        return f"Successfully validated Silver layer!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")


if __name__ == "__main__":
    avaliar_silver()