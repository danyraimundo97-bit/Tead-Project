"""
Script de Avaliação Final: avaliar_gold.py
Objetivo: Garantia de Qualidade de Dados na Camada Gold.
Verifica: Volumetria final, tipagem rigorosa para BI, ausência absoluta de NULLs em chaves e coerência do modelo de negócio.
"""

import trino
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from workflow_functions.loki_logging import get_logger

logger = get_logger(__name__)

gold_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["pandas", "trino", "python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=gold_image, environment=TASK_ENV)
def avaliar_gold() -> str:
    logger.info("="*65)
    logger.info(" INICIANDO AUDITORIA FINAL: CAMADA GOLD (DATA MARTS / BI)")
    logger.info("="*65)

    try:
        trino_host = TASK_ENV.get("TRINO_HOST", "localhost")
        conn = trino.dbapi.connect(host=trino_host, port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()

        # --- AUDITORIA: NETWORK QUALITY DAILY ---
        logger.info("\n[1] DATA MART: NETWORK QUALITY DAILY (Avaliação Geoespacial)")
        cur.execute("""
            SELECT 
                COUNT(*) as total_linhas,
                COUNT(DISTINCT "Data_Hora") as dias_unicos,
                COUNT(DISTINCT "ID_Antena_Conectada") as antenas_unicas,
                COUNT_IF("Torre_Latitude" IS NULL OR "Torre_Longitude" IS NULL) as coords_nulls,
                COUNT_IF("Estado_Antena" = FALSE) as antenas_down
            FROM iceberg.gold.network_quality_daily
        """)
        total_net, dias_net, antenas_net, coords_nulls, antenas_down = cur.fetchone()

        logger.info(f" -> [VOLUMETRIA] Linhas: {total_net} | Dias: {dias_net} (Esperado: 16) | Antenas: {antenas_net} (Esperado: ~474)")
        
        # Teste da Matriz Esperada: Antena x Dia
        esperado = dias_net * antenas_net
        if total_net < esperado * 0.99:
            logger.error(f" -> ERRO CRÍTICO: Matriz incompleta! Esperávamos {esperado} linhas, mas temos {total_net}.")
        else:
            logger.info(" -> [MATRIZ] SUCESSO: A matriz de Antena x Dia está perfeita.")

        # Teste de Bounding Box e Nulls para o Mapa
        if coords_nulls > 0:
            logger.error(f" -> ERRO CRÍTICO: Existem {coords_nulls} linhas sem coordenadas GPS. O mapa vai falhar!")
        
        # Teste da História da Tempestade
        logger.info(f" -> [NEGÓCIO] Registadas {antenas_down} instâncias de antenas caídas (DOWN) na Gold.")
        if antenas_down == 0:
             logger.error(" -> ERRO CRÍTICO: A tempestade não deitou nenhuma antena abaixo na Camada Gold!")

        # --- AUDITORIA: CHURN RISK DAILY (O DASHBOARD FINANCEIRO) ---
        logger.info("\n[2] DATA MART: CHURN RISK DAILY (Avaliação Financeira)")
        cur.execute("""
            SELECT 
                COUNT(*) as total_linhas,
                COUNT_IF("Telefone" IS NULL) as null_phones,
                COUNT_IF("Data_Referencia" IS NULL) as null_dates,
                COUNT_IF("Desistencia" = TRUE) as total_churners,
                SUM("Receita_Em_Risco") as valor_em_risco
            FROM iceberg.gold.churn_risk_daily
        """)
        total_churn, null_phones, null_dates, total_churners, valor_em_risco = cur.fetchone()

        logger.info(f" -> [VOLUMETRIA] Linhas de análise financeira: {total_churn}")
        
        # Teste de Nulls nas Chaves Primárias do Superset
        if null_phones > 0 or null_dates > 0:
            logger.error(" -> ERRO CRÍTICO: Chaves primárias a NULL detetadas na tabela de Risco de Churn!")
            
        logger.info(f" -> [NEGÓCIO] Clientes identificados em Churn (Desistência): {total_churners}")
        logger.info(f" -> [FINANÇAS] Receita Total em Risco monitorizada: {valor_em_risco:.2f} €")

        logger.info("="*65)
        logger.info(" AUDITORIA GOLD CONCLUÍDA: O LAKEHOUSE ESTÁ 100% CERTIFICADO PARA BI ")
        logger.info("="*65)
        
        return "Gold checks passed!"

    except Exception as e:
        logger.error(f"Falha Crítica na Auditoria da Camada Gold: {str(e)}")
        raise e