import trino
from flytekit import task, ImageSpec
from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

# Definimos a imagem com o driver do Trino
gold_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_network_quality(target_date_str: str) -> str:
    """
    Constrói o Produto A (Qualidade de Rede) na camada Gold.
    Cruza logs de rede com o cadastro de torres e calcula métricas agregadas.
    """
    logger.info(f"🚀 A iniciar processamento Gold: Produto A para {target_date_str}")
    
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

        # 2. Criar a tabela Gold (se não existir)
        # Nota: Usamos partitioning por dia para que o Dashboard seja super rápido
        logger.info("⏳ A garantir que a tabela gold.network_quality_daily existe...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.gold.network_quality_daily (
                date_of_test DATE,
                zona_leiria VARCHAR,
                radio VARCHAR,
                cell INTEGER,
                network_provider VARCHAR,
                avg_rsrp DOUBLE,
                avg_rsrq DOUBLE,
                avg_sinr DOUBLE,
                avg_downlink_mbps DOUBLE,
                poor_signal_connections BIGINT,
                total_connections BIGINT
            ) WITH (
                format = 'PARQUET',
                partitioning = ARRAY['day(date_of_test)']
            )
        """)
        cur.fetchall()

        # 3. IDEMPOTÊNCIA: Limpar dados do dia alvo antes de processar
        # Isto permite re-executar a task sem duplicar dados
        logger.info(f"🧹 A limpar snapshots antigos de {target_date_str}...")
        cur.execute(f"DELETE FROM iceberg.gold.network_quality_daily WHERE date_of_test = DATE '{target_date_str}'")
        cur.fetchall()

        # 4. PROCESSAMENTO PESADO (O "Coração" do Produto A)
        # Cruzamos os logs (nl) com as torres (t) para obter a tecnologia (radio)
        logger.info("⚙️ A executar agregação via Trino Engine...")
        
        insert_query = f"""
            INSERT INTO iceberg.gold.network_quality_daily
            SELECT 
                DATE '{target_date_str}' AS date_of_test,
                -- Lógica de Zona baseada nas coordenadas (Norte/Sul/Leste/Oeste)
                CASE 
                    WHEN nl.latitude > 39.75 AND nl.longitude > -8.80 THEN 'Norte-Leste'
                    WHEN nl.latitude > 39.75 THEN 'Norte-Oeste'
                    WHEN nl.longitude > -8.80 THEN 'Sul-Leste'
                    ELSE 'Sul-Oeste'
                END AS zona_leiria,
                t.radio,
                t.cell,
                nl.network_provider,
                ROUND(AVG(nl.rsrp), 2) AS avg_rsrp,
                ROUND(AVG(nl.rsrq), 2) AS avg_rsrq,
                ROUND(AVG(nl.sinr), 2) AS avg_sinr,
                ROUND(AVG(nl.downlink_mbps), 2) AS avg_downlink_mbps,
                COUNT_IF(nl.rsrp < -100) AS poor_signal_connections,
                COUNT(*) AS total_connections
            FROM iceberg.silver.network_logs nl
            LEFT JOIN iceberg.silver.towers t ON nl.pci = t.cell
            WHERE CAST(nl.timestamp_log AS DATE) = DATE '{target_date_str}'
            GROUP BY 
                t.radio, t.cell, nl.network_provider,
                -- Repetimos a lógica do CASE no group by ou agrupamos pela coluna calculada
                CASE 
                    WHEN nl.latitude > 39.75 AND nl.longitude > -8.80 THEN 'Norte-Leste'
                    WHEN nl.latitude > 39.75 THEN 'Norte-Oeste'
                    WHEN nl.longitude > -8.80 THEN 'Sul-Leste'
                    ELSE 'Sul-Oeste'
                END
        """
        cur.execute(insert_query)
        cur.fetchall()

        logger.info(f"✅ Sucesso! Produto Gold 'Network Quality' atualizado para {target_date_str}")
        return f"Gold network_quality_daily updated for {target_date_str}"

    except Exception as e:
        logger.error(f"❌ Falha na construção da Gold: {str(e)}")
        raise Exception(f"Gold Task Failed: {str(e)}")
    finally:
        conn.close()