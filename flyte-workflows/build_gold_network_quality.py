import trino
from flytekit import ImageSpec, task

from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

gold_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000",
)


@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_network_quality() -> str:
    """
    Gold Produto A: qualidade de rede agregada por dia (timestamp do log),
    zona, tecnologia (a partir de radio_ohe_* em towers) e célula.
    """
    logger.info("Gold network_quality_daily: full batch from silver")

    conn = trino.dbapi.connect(
        host="host.docker.internal",
        port=8080,
        user="flyte",
        catalog="iceberg",
    )
    cur = conn.cursor()

    try:
        logger.info("Truncating gold.network_quality_daily for full rebuild")
        cur.execute("TRUNCATE TABLE iceberg.gold.network_quality_daily")
        cur.fetchall()

        insert_query = """
            INSERT INTO iceberg.gold.network_quality_daily
            WITH base AS (
                SELECT
                    CAST(nl.timestamp_log AS DATE) AS date_of_test,
                    CASE
                        WHEN nl.latitude > 39.75 AND nl.longitude > -8.80 THEN 'Norte-Leste'
                        WHEN nl.latitude > 39.75 THEN 'Norte-Oeste'
                        WHEN nl.longitude > -8.80 THEN 'Sul-Leste'
                        ELSE 'Sul-Oeste'
                    END AS zona_leiria,
                    CASE
                        WHEN t.radio_ohe_lte THEN 'LTE'
                        WHEN t.radio_ohe_gsm THEN 'GSM'
                        WHEN t.radio_ohe_umts THEN 'UMTS'
                        WHEN t.radio_ohe_nr THEN 'NR'
                        WHEN t.radio_ohe_cdma THEN 'CDMA'
                        ELSE 'OTHER'
                    END AS radio,
                    t.cell,
                    nl.network_provider,
                    nl.rsrp,
                    nl.rsrq,
                    nl.sinr,
                    nl.downlink_mbps
                FROM iceberg.silver.network_logs nl
                LEFT JOIN iceberg.silver.towers t ON nl.pci = t.cell
            )
            SELECT
                date_of_test,
                zona_leiria,
                radio,
                cell,
                network_provider,
                ROUND(AVG(rsrp), 2) AS avg_rsrp,
                ROUND(AVG(rsrq), 2) AS avg_rsrq,
                ROUND(AVG(sinr), 2) AS avg_sinr,
                ROUND(AVG(downlink_mbps), 2) AS avg_downlink_mbps,
                COUNT_IF(rsrp < -100) AS poor_signal_connections,
                COUNT(*) AS total_connections
            FROM base
            GROUP BY
                date_of_test,
                zona_leiria,
                radio,
                cell,
                network_provider
        """
        cur.execute(insert_query)
        cur.fetchall()

        logger.info("Gold network_quality_daily full batch completed")
        return "Gold network_quality_daily rebuilt for all log dates"

    except Exception as e:
        logger.error("Network quality gold task failed: %s", str(e))
        raise Exception(f"Gold Task Failed: {str(e)}") from e
    finally:
        conn.close()
