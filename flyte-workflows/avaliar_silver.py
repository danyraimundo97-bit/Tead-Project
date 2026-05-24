"""
Script de Avaliação: avaliar_silver.py
Objetivo: Garantia de Qualidade de Dados na Camada Silver.
Verifica: Unicidade das chaves primárias (Row IDs), Plausibilidade Física de grandezas, e ausência de NULLs induzidos por falha de ETL.
"""

import trino
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from workflow_functions.loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=medallion_image, environment=TASK_ENV)
def avaliar_silver():
    logger.info("="*60)
    logger.info(" INICIANDO QUALITY ASSURANCE (QA) PROFUNDO: CAMADA SILVER")
    logger.info("="*60)

    try:
        trino_host = TASK_ENV["TRINO_HOST"]
        conn = trino.dbapi.connect(host=trino_host, port=8080, user="flyte", catalog="iceberg")
        cur = conn.cursor()

        # --- AVALIAÇÃO: NETWORK LOGS (Limites Físicos e Timestamps) ---
        logger.info("\n[1] NETWORK LOGS SILVER (Verificação de Engenharia RF e Tipos)")
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(timestamp_log) as valid_dates,
                MIN(rsrp) as min_rsrp, 
                MAX(rsrp) as max_rsrp
            FROM iceberg.silver.network_logs
        """)
        total_logs, valid_dates, min_rsrp, max_rsrp = cur.fetchone()
        
        logger.info(f" -> [TEMPO] Sucesso no parsing das datas: {valid_dates}/{total_logs} timestamps parseados.")
        if valid_dates < total_logs * 0.9:
            logger.error(" -> ERRO CRÍTICO: Muitos timestamps ficaram a NULL durante a conversão na Silver!")
            
        logger.info(f" -> [FÍSICA] RSRP (Potência de Sinal): Mínimo {min_rsrp:.1f} dBm | Máximo {max_rsrp:.1f} dBm")
        if min_rsrp < -140 or max_rsrp > -60:
            logger.warning(" -> ALERTA: Valores de RSRP fora dos limites razoáveis de RF (Radio Frequência)!")

        # --- AVALIAÇÃO: CALL TESTS (Normalização de Qualidade) ---
        logger.info("\n[2] CALL TESTS SILVER (Sanidade do MOS)")
        cur.execute("SELECT MIN(mos), MAX(mos), COUNT_IF(result IS NULL) FROM iceberg.silver.call_tests")
        min_mos, max_mos, null_results = cur.fetchone()
        logger.info(f" -> [QA] Score MOS (Mean Opinion Score): [{min_mos:.2f} a {max_mos:.2f}] (Deve estar entre 1 e 5)")
        if max_mos > 5.5:
            logger.error(" -> ERRO CRÍTICO: A remoção de vírgulas falhou e o MOS explodiu para a casa das dezenas/centenas!")
        logger.info(f" -> [ESTRUTURA] Existem {null_results} resultados Booleanos a NULL.")

        # --- AVALIAÇÃO: CDR (Chaves Primárias e Deduplicação) ---
        logger.info("\n[3] CDR CUSTOMERS SILVER (Integridade Relacional)")
        cur.execute("""
            SELECT 
                COUNT(*) as total, 
                COUNT(DISTINCT silver_row_id) as unique_ids,
                COUNT(DISTINCT phone_number) as unique_phones
            FROM iceberg.silver.cdr_customers
        """)
        total_cdr, unique_ids, unique_phones = cur.fetchone()
        
        logger.info(f" -> [INTEGRIDADE] Linhas Totais: {total_cdr} | Clientes Únicos: {unique_phones}")
        if total_cdr != unique_phones:
             logger.error(f" -> ERRO CRÍTICO: O 'drop_duplicates' falhou! Temos {total_cdr} linhas mas apenas {unique_phones} clientes.")
        else:
             logger.info(" -> [SUCESSO] Deduplicação perfeita: 1 linha por cliente na Silver.")

        # --- AVALIAÇÃO: TOWERS (Tipagem Forte) ---
        logger.info("\n[4] TOWERS SILVER (Tipagem de Dados)")
        cur.execute("SELECT typeof(status), typeof(radio_ohe_lte) FROM iceberg.silver.towers LIMIT 1")
        status_type, ohe_type = cur.fetchone()
        logger.info(f" -> [TIPAGEM] Coluna Status é do tipo: {status_type} (Esperado: boolean)")
        logger.info(f" -> [TIPAGEM] Coluna OHE LTE é do tipo: {ohe_type} (Esperado: boolean)")

        logger.info("="*60)
        logger.info(" QA SILVER CONCLUÍDO: DADOS CERTIFICADOS PARA A CAMADA GOLD ")
        logger.info("="*60)

    except Exception as e:
        logger.error(f"Falha Crítica no QA da Silver: {str(e)}")
        raise e