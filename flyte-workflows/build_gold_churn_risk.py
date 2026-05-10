import trino
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

# Reutilizamos a imagem configurada para a camada Gold
gold_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_churn_risk(target_date_str: str) -> str:
    """
    Constrói o Produto B (Risco de Churn Diário) na camada Gold.
    Cruza dados de faturação (CDR) com testes de rede e logs de localização.
    """
    logger.info(f"🚀 A iniciar processamento Gold: Produto B para {target_date_str}")
    
    conn = trino.dbapi.connect(
        host='host.docker.internal', 
        port=8080, 
        user='flyte', 
        catalog='iceberg'
    )
    cur = conn.cursor()
    
    try:
        # 1. Garantir que o Schema Gold existe
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
        cur.fetchall()

        # 2. Criar a tabela Gold de Churn (se não existir)
        # Granularidade: 1 linha por Cliente por Dia
        logger.info("⏳ A garantir que a tabela gold.churn_risk_daily existe...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.gold.churn_risk_daily (
                snapshot_date DATE,
                phone_number VARCHAR,
                storm_affected BOOLEAN,
                receita_em_risco DOUBLE,
                account_length INTEGER,
                total_chamadas_suporte INTEGER,
                total_drops BIGINT,
                qualidade_audio_mos DOUBLE,
                churn BOOLEAN
            ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(snapshot_date)']
            )
        """)
        cur.fetchall()

        # 3. IDEMPOTÊNCIA: Limpar snapshots antigos da mesma data
        logger.info(f"🧹 A limpar snapshots antigos de {target_date_str} na Gold...")
        cur.execute(f"DELETE FROM iceberg.gold.churn_risk_daily WHERE snapshot_date = DATE '{target_date_str}'")
        cur.fetchall()

        # 4. PROCESSAMENTO CUSTOMER 360
        # Cruzamos CDR (Base) com Call Tests (Técnico) e Logs (Geográfico)
        logger.info("⚙️ A calcular métricas de retenção via Trino Engine...")
        
        insert_query = f"""
            INSERT INTO iceberg.gold.churn_risk_daily
            SELECT 
                DATE '{target_date_str}' AS snapshot_date,
                cdr.phone_number,
                
                -- Verificamos se o cliente teve logs na zona Leste (exemplo da tempestade)
                COALESCE(bool_or(nl.longitude > -8.80), FALSE) AS storm_affected,
                
                -- Soma das taxas (Day + Eve + Night + Intl Charge)
                (cdr.day_charge + cdr.eve_charge + cdr.night_charge + cdr.intl_charge) AS receita_em_risco,
                
                cdr.account_length,
                cdr.custserv_calls AS total_chamadas_suporte,
                
                -- Contagem de chamadas que caíram (DROP) no dia alvo
                COUNT_IF(ct.call_test_result = 'DROP') AS total_drops,
                
                -- Média de qualidade percetível (MOS)
                ROUND(AVG(ct.mos), 2) AS qualidade_audio_mos,
                
                cdr.churn
                
            FROM iceberg.silver.cdr_customers cdr
            
            -- Join com Testes de Chamada (mesmo dia)
            LEFT JOIN iceberg.silver.call_tests ct 
                ON cdr.phone_number = ct.phone_number 
                AND CAST(ct.date_of_test AS DATE) = DATE '{target_date_str}'
                
            -- Join com Logs de Rede (mesmo dia) para verificar localização
            LEFT JOIN iceberg.silver.network_logs nl 
                ON cdr.phone_number = nl.phone_number 
                AND CAST(nl.timestamp_log AS DATE) = DATE '{target_date_str}'
                
            GROUP BY 
                cdr.phone_number, 
                cdr.account_length, 
                cdr.day_charge, cdr.eve_charge, cdr.night_charge, cdr.intl_charge,
                cdr.custserv_calls, 
                cdr.churn
        """
        cur.execute(insert_query)
        cur.fetchall()

        logger.info(f"✅ Sucesso! Produto Gold 'Churn Risk' atualizado para {target_date_str}")
        return f"Gold churn_risk_daily updated for {target_date_str}"

    except Exception as e:
        logger.error(f"❌ Falha na construção do Produto B: {str(e)}")
        raise Exception(f"Churn Gold Task Failed: {str(e)}")
    finally:
        conn.close()