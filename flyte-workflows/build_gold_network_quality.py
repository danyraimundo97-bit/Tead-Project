import trino
from flytekit import ImageSpec, task

from flyte_task_env import TASK_ENV
from workflow_functions.loki_logging import get_logger
from workflow_functions.iceberg_replace import replace_iceberg_table

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

# Silver guarda vários snapshots da mesma célula (mesmo mcc/net/area/cell/unit) com
# silver_row_id diferentes; o join completo duplica distâncias idênticas e parte os agregados.
_TOWERS_DEDUP_SQL = """
    SELECT
        silver_row_id, mcc, net, area, cell, unit, lat, lon, range_m, samples,
        changeable, created, updated, average_signal, snapshot_date, status,
        radio_ohe_gsm, radio_ohe_umts, radio_ohe_lte, radio_ohe_nr,
        radio_ohe_cdma, radio_ohe_other
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY mcc, net, area, cell, unit
                ORDER BY COALESCE(TRY_CAST(snapshot_date AS date), DATE '1900-01-01') DESC,
                    silver_row_id ASC
            ) AS tower_rn
        FROM iceberg.silver.towers
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    ) s
    WHERE tower_rn = 1
"""

_COUNT_SILVER_LOGS = """
    SELECT COUNT(*) FROM iceberg.silver.network_logs
"""

_CHECK_GOLD_ANTENNA_COLUMNS = """
    SELECT
        COUNT(*) AS n,
        COUNT_IF(id_antena_conectada IS NULL) AS id_nulls,
        COUNT_IF(estado_antena IS NULL) AS estado_nulls,
        COUNT_IF(distancia_antena_m IS NULL) AS dist_nulls
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
        problems.append("id_antena_conectada is NULL for every row")
    if est_n == n:
        problems.append("estado_antena is NULL for every row")
    if dist_n == n:
        problems.append("distancia_antena_m is NULL for every row")
    if problems:
        raise RuntimeError(f"{context}: {'; '.join(problems)}")


@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_network_quality() -> str:
    """
    Gold Produto A: **1 linha por torre (antena) por dia** de calendário.
    Cada log é atribuído à torre mais próxima (haversine); torres silver são
    deduplicadas por (mcc, net, area, cell, unit) com snapshot mais recente,
    para não multiplicar a mesma antena física. Médias RSRP/RSRQ/SINR ignoram
    valores fora de intervalos plausíveis (RSRP ~3GPP -140..-44 dBm; outliers
    de bronze/silver). Logs sem lat/lon não entram. Zona_Leiria e tecnologia
    de rede refletem a torre mais próxima, não o nt_ohe_* do terminal.
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
            logger.warning("Silver network_logs is empty; clearing gold (CREATE OR REPLACE)")
            replace_iceberg_table(
                conn,
                table_fqn="iceberg.gold.network_quality_daily",
                select_sql=(
                    "SELECT * FROM iceberg.gold.network_quality_daily WHERE FALSE"
                ),
                logger=logger,
            )
            return "Gold network_quality_daily empty (no silver logs)"

        logger.info("Replacing gold.network_quality_daily (CREATE OR REPLACE)")
        load_sql = f"""
            WITH tests_by_day AS (
                SELECT
                    phone_number,
                    CAST(date_of_test AS DATE) AS d,
                    max_by(result, date_of_test) AS last_result
                FROM iceberg.silver.call_tests
                GROUP BY phone_number, CAST(date_of_test AS DATE)
            ),
            towers_for_geo AS (
                {_TOWERS_DEDUP_SQL.strip()}
            ),
            log_tower_dist AS (
                SELECT
                    nl.silver_row_id,
                    t.silver_row_id AS tower_silver_row_id,
                    {_HAVERSINE_NL_T} AS dist_m
                FROM iceberg.silver.network_logs nl
                INNER JOIN towers_for_geo t
                    ON nl.latitude IS NOT NULL
                    AND nl.longitude IS NOT NULL
            ),
            dist_ranked AS (
                SELECT
                    silver_row_id,
                    tower_silver_row_id,
                    dist_m,
                    ROW_NUMBER() OVER (
                        PARTITION BY silver_row_id
                        ORDER BY dist_m ASC, tower_silver_row_id ASC
                    ) AS rn
                FROM log_tower_dist
            ),
            closest_tower AS (
                SELECT 
                    dr.silver_row_id AS log_silver_row_id, 
                    t.mcc, t.net, t.area, t.cell, t.unit,
                    dr.dist_m
                FROM dist_ranked dr
                INNER JOIN towers_for_geo t ON dr.tower_silver_row_id = t.silver_row_id
                WHERE dr.rn = 1
            ),
            enriched AS (
                SELECT
                    nl.phone_number,
                    nl.rsrp,
                    nl.rsrq,
                    nl.sinr,
                    nl.downlink_mbps,
                    ct.mcc, ct.net, ct.area, ct.cell, ct.unit,
                    ct.dist_m,
                    CAST(nl.timestamp_log AS DATE) AS log_day
                FROM iceberg.silver.network_logs nl
                INNER JOIN closest_tower ct ON nl.silver_row_id = ct.log_silver_row_id
            ),
            -- A SOLUÇÃO: A nossa matriz mestra agora é a Silver Towers, que já tem 1 linha por dia!
            daily_towers AS (
                SELECT 
                    *,
                    CAST(snapshot_date AS DATE) AS log_day
                FROM iceberg.silver.towers
                WHERE lat IS NOT NULL AND lon IS NOT NULL
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY dt.log_day, dt.silver_row_id) AS gold_row_id,
                CAST(dt.log_day AS TIMESTAMP(3)) AS data_hora,
                CASE
                    WHEN dt.lat > 39.75 AND dt.lon > -8.80 THEN 'Norte-Leste'
                    WHEN dt.lat > 39.75 THEN 'Norte-Oeste'
                    WHEN dt.lon > -8.80 THEN 'Sul-Leste'
                    ELSE 'Sul-Oeste'
                END AS zona_leiria,
                dt.lat AS latitude_ocorrencia,
                dt.lon AS longitude_ocorrencia,
                dt.lat AS torre_latitude,
                dt.lon AS torre_longitude,
                CAST(dt.cell AS BIGINT) AS id_antena_conectada,
                dt.status AS estado_antena,
                ROUND(AVG(e.dist_m), 2) AS distancia_antena_m,
                CASE
                    WHEN dt.radio_ohe_lte THEN 'LTE'
                    WHEN dt.radio_ohe_gsm THEN 'GSM'
                    WHEN dt.radio_ohe_umts THEN 'UMTS'
                    WHEN dt.radio_ohe_nr THEN 'NR'
                    WHEN dt.radio_ohe_cdma THEN 'CDMA'
                    ELSE 'OTHER'
                END AS tecnologia_rede,
                ROUND(AVG(CASE WHEN e.rsrp BETWEEN -140 AND -44 THEN e.rsrp END), 2) AS potencia_rsrp,
                ROUND(AVG(CASE WHEN e.rsrq BETWEEN -50 AND 30 THEN e.rsrq END), 2) AS qualidade_rsrq,
                ROUND(AVG(CASE WHEN e.sinr BETWEEN -30 AND 80 THEN e.sinr END), 2) AS ruido_sinr,
                ROUND(AVG(CASE WHEN e.downlink_mbps BETWEEN 0 AND 5000 THEN e.downlink_mbps END), 2) AS velocidade_downlink,
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result = TRUE THEN e.phone_number END) AS BIGINT) AS telefones_sucesso,
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result = FALSE THEN e.phone_number END) AS BIGINT) AS telefones_falha,
                CAST(COUNT(DISTINCT CASE WHEN tb.last_result IS NULL THEN e.phone_number END) AS BIGINT) AS telefones_sem_teste
            FROM daily_towers dt
            -- O LEFT JOIN garante que as antenas vazias continuam no mapa com ZERO problemas
            LEFT JOIN enriched e 
                ON dt.mcc = e.mcc AND dt.net = e.net AND dt.area = e.area AND dt.cell = e.cell AND dt.unit = e.unit 
                AND dt.log_day = e.log_day
            LEFT JOIN tests_by_day tb
                ON e.phone_number = tb.phone_number
                AND e.log_day = tb.d
            GROUP BY
                dt.log_day,
                dt.silver_row_id,
                dt.cell,
                dt.status,
                dt.lat,
                dt.lon,
                dt.radio_ohe_lte,
                dt.radio_ohe_gsm,
                dt.radio_ohe_umts,
                dt.radio_ohe_nr,
                dt.radio_ohe_cdma,
                dt.radio_ohe_other
        """
        replace_iceberg_table(
            conn,
            table_fqn="iceberg.gold.network_quality_daily",
            select_sql=load_sql,
            logger=logger,
        )

        _assert_antenna_columns_sane(cur, context="After INSERT network_quality")

        logger.info("Gold network_quality_daily full batch completed (tower x day)")
        return "Gold network_quality_daily rebuilt (1 row per tower per day)"

    except Exception as e:
        logger.error("Network quality gold task failed: %s", str(e))
        raise RuntimeError(f"Gold Task Failed: {str(e)}") from None
    finally:
        conn.close()
