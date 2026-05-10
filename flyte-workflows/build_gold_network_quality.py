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

# Haversine distance (m) between network log (nl) and tower row (t); requires all coords non-null.
_HAVERSINE_NL_T = """(
    6371000.0 * acos(greatest(
        -1.0,
        least(
            1.0,
            (sin(radians(nl.latitude)) * sin(radians(t.lat)))
            + (cos(radians(nl.latitude)) * cos(radians(t.lat))
                * cos(radians(t.lon - nl.longitude)))
        )
    ))
)"""

_COUNT_TOWERS_WITH_COORDS = """
    SELECT COUNT(*) FROM iceberg.silver.towers
    WHERE lat IS NOT NULL AND lon IS NOT NULL
"""

_COUNT_SILVER_LOGS = """
    SELECT COUNT(*) FROM iceberg.silver.network_logs
"""

_CHECK_GOLD_ANTENNA_COLUMNS = """
    SELECT
        COUNT(*) AS n,
        COUNT_IF("ID_Antena_Conectada" IS NULL) AS id_nulls,
        COUNT_IF("Estado_Antena" IS NULL) AS estado_nulls,
        COUNT_IF("Distancia_Antena_m" IS NULL) AS dist_nulls
    FROM iceberg.gold.network_quality_daily
"""


def _assert_antenna_columns_sane(cur, *, context: str) -> None:
    cur.execute(_CHECK_GOLD_ANTENNA_COLUMNS)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"{context}: no stats row from gold table")
    n, id_n, est_n, dist_n = (int(row[0]), int(row[1]), int(row[2]), int(row[3]))
    if n == 0:
        logger.warning("%s: gold.network_quality_daily is empty", context)
        return
    problems: list[str] = []
    if id_n == n:
        problems.append("ID_Antena_Conectada is NULL for every row")
    if est_n == n:
        problems.append("Estado_Antena is NULL for every row")
    if dist_n == n:
        problems.append("Distancia_Antena_m is NULL for every row")
    if problems:
        raise RuntimeError(f"{context}: {'; '.join(problems)}")


@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_network_quality() -> str:
    """
    Gold Produto A: **1 linha por torre (antena) por dia** de calendário.
    Cada log é atribuído à torre mais próxima (haversine); depois agrega-se por
    (dia, silver_row_id da torre).
    """
    logger.info("Gold network_quality_daily: grain = tower × day (closest tower per log)")

    conn = trino.dbapi.connect(
        host="host.docker.internal",
        port=8080,
        user="flyte",
        catalog="iceberg",
    )
    cur = conn.cursor()

    try:
        cur.execute(_COUNT_TOWERS_WITH_COORDS)
        n_towers = int(cur.fetchone()[0])
        if n_towers == 0:
            raise RuntimeError(
                "Silver towers has no rows with lat/lon; cannot assign closest tower"
            )
        logger.info("Towers with coordinates available: %s", n_towers)

        cur.execute(_COUNT_SILVER_LOGS)
        n_logs = int(cur.fetchone()[0])
        if n_logs == 0:
            logger.warning("Silver network_logs is empty; truncating gold only")
            cur.execute("TRUNCATE TABLE iceberg.gold.network_quality_daily")
            cur.fetchall()
            return "Gold network_quality_daily empty (no silver logs)"

        logger.info("Truncating gold.network_quality_daily for full rebuild")
        cur.execute("TRUNCATE TABLE iceberg.gold.network_quality_daily")
        cur.fetchall()

        insert_query = f"""
            INSERT INTO iceberg.gold.network_quality_daily
            WITH tests_by_day AS (
                SELECT
                    phone_number,
                    CAST(date_of_test AS DATE) AS d,
                    max_by(result, date_of_test) AS last_result
                FROM iceberg.silver.call_tests
                GROUP BY phone_number, CAST(date_of_test AS DATE)
            ),
            log_tower_dist AS (
                SELECT
                    nl.silver_row_id,
                    t.silver_row_id AS tower_silver_row_id,
                    {_HAVERSINE_NL_T} AS dist_m
                FROM iceberg.silver.network_logs nl
                INNER JOIN iceberg.silver.towers t
                    ON t.lat IS NOT NULL
                    AND t.lon IS NOT NULL
                    AND nl.latitude IS NOT NULL
                    AND nl.longitude IS NOT NULL
            ),
            dist_ranked AS (
                SELECT
                    silver_row_id,
                    tower_silver_row_id,
                    dist_m,
                    ROW_NUMBER() OVER (
                        PARTITION BY silver_row_id ORDER BY dist_m ASC
                    ) AS rn
                FROM log_tower_dist
            ),
            closest_tower AS (
                SELECT silver_row_id, tower_silver_row_id, dist_m
                FROM dist_ranked
                WHERE rn = 1
            ),
            enriched AS (
                SELECT
                    nl.timestamp_log,
                    nl.phone_number,
                    nl.rsrp,
                    nl.rsrq,
                    nl.sinr,
                    nl.downlink_mbps,
                    ct.tower_silver_row_id,
                    ct.dist_m,
                    CAST(nl.timestamp_log AS DATE) AS log_day
                FROM iceberg.silver.network_logs nl
                INNER JOIN closest_tower ct ON nl.silver_row_id = ct.silver_row_id
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY e.log_day, e.tower_silver_row_id) AS gold_row_id,
                CAST(e.log_day AS TIMESTAMP(3)) AS "Data_Hora",
                CASE
                    WHEN t.lat > 39.75 AND t.lon > -8.80 THEN 'Norte-Leste'
                    WHEN t.lat > 39.75 THEN 'Norte-Oeste'
                    WHEN t.lon > -8.80 THEN 'Sul-Leste'
                    ELSE 'Sul-Oeste'
                END AS "Zona_Leiria",
                t.lat AS "Latitude_Ocorrencia",
                t.lon AS "Longitude_Ocorrencia",
                CAST(t.cell AS BIGINT) AS "ID_Antena_Conectada",
                t.status AS "Estado_Antena",
                ROUND(AVG(e.dist_m), 2) AS "Distancia_Antena_m",
                CASE
                    WHEN t.radio_ohe_lte THEN 'LTE'
                    WHEN t.radio_ohe_gsm THEN 'GSM'
                    WHEN t.radio_ohe_umts THEN 'UMTS'
                    WHEN t.radio_ohe_nr THEN 'NR'
                    WHEN t.radio_ohe_cdma THEN 'CDMA'
                    ELSE 'OTHER'
                END AS "Tecnologia_Rede",
                ROUND(AVG(e.rsrp), 2) AS "Potencia_RSRP",
                ROUND(AVG(e.rsrq), 2) AS "Qualidade_RSRQ",
                ROUND(AVG(e.sinr), 2) AS "Ruido_SINR",
                ROUND(AVG(e.downlink_mbps), 2) AS "Velocidade_Downlink",
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result = TRUE THEN e.phone_number END) AS BIGINT)
                    AS "Telefones_Sucesso",
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result = FALSE THEN e.phone_number END) AS BIGINT)
                    AS "Telefones_Falha",
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result IS NULL THEN e.phone_number END) AS BIGINT)
                    AS "Telefones_Sem_Teste"
            FROM enriched e
            INNER JOIN iceberg.silver.towers t ON t.silver_row_id = e.tower_silver_row_id
            LEFT JOIN tests_by_day tb
                ON e.phone_number = tb.phone_number
                AND e.log_day = tb.d
            GROUP BY
                e.log_day,
                e.tower_silver_row_id,
                t.cell,
                t.status,
                t.lat,
                t.lon,
                t.radio_ohe_lte,
                t.radio_ohe_gsm,
                t.radio_ohe_umts,
                t.radio_ohe_nr,
                t.radio_ohe_cdma,
                t.radio_ohe_other
        """
        cur.execute(insert_query)
        cur.fetchall()

        _assert_antenna_columns_sane(cur, context="After INSERT network_quality")

        logger.info("Gold network_quality_daily full batch completed (tower x day)")
        return "Gold network_quality_daily rebuilt (1 row per tower per day)"

    except Exception as e:
        logger.error("Network quality gold task failed: %s", str(e))
        raise RuntimeError(f"Gold Task Failed: {str(e)}") from None
    finally:
        conn.close()
