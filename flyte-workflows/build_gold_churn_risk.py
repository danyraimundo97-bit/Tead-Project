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


def _snapshot_dates_sql() -> str:
    return """
        SELECT DISTINCT CAST(date_of_test AS DATE) AS d
        FROM iceberg.silver.call_tests
        UNION
        SELECT DISTINCT CAST(timestamp_log AS DATE) AS d
        FROM iceberg.silver.network_logs
        ORDER BY 1
    """


def _insert_one_snapshot_date(*, snapshot_date_sql: str) -> str:
    """One day: CDR × at-most-one row per fact table — avoids cross-join blowups in Trino."""
    return f"""
        INSERT INTO iceberg.gold.churn_risk_daily
        SELECT
            DATE '{snapshot_date_sql}' AS snapshot_date,
            cdr.phone_number,
            COALESCE(al.storm_affected, FALSE) AS storm_affected,
            (cdr.day_charge + cdr.eve_charge + cdr.night_charge + cdr.intl_charge)
                AS receita_em_risco,
            cdr.account_length,
            cdr.custserv_calls AS total_chamadas_suporte,
            COALESCE(ac.total_drops, CAST(0 AS BIGINT)) AS total_drops,
            ac.qualidade_audio_mos,
            cdr.churn
        FROM iceberg.silver.cdr_customers cdr
        LEFT JOIN (
            SELECT
                phone_number,
                COUNT_IF(result = FALSE) AS total_drops,
                ROUND(AVG(mos), 2) AS qualidade_audio_mos
            FROM iceberg.silver.call_tests
            WHERE CAST(date_of_test AS DATE) = DATE '{snapshot_date_sql}'
            GROUP BY phone_number
        ) ac ON cdr.phone_number = ac.phone_number
        LEFT JOIN (
            SELECT
                phone_number,
                COALESCE(bool_or(longitude > -8.80), FALSE) AS storm_affected
            FROM iceberg.silver.network_logs
            WHERE CAST(timestamp_log AS DATE) = DATE '{snapshot_date_sql}'
            GROUP BY phone_number
        ) al ON cdr.phone_number = al.phone_number
    """


@task(container_image=gold_image, environment=TASK_ENV)
def build_gold_churn_risk() -> str:
    """
    Gold Produto B: uma linha por cliente por dia, para cada data em call_tests ou network_logs.
    Inserções **por dia** para ficar abaixo do limite de memória do Trino (sem CROSS JOIN global).
    """
    logger.info("Gold churn_risk_daily: full batch from silver (chunked by snapshot_date)")

    conn = trino.dbapi.connect(
        host="host.docker.internal",
        port=8080,
        user="flyte",
        catalog="iceberg",
    )
    cur = conn.cursor()

    try:
        logger.info("Truncating gold.churn_risk_daily for full rebuild")
        cur.execute("TRUNCATE TABLE iceberg.gold.churn_risk_daily")
        cur.fetchall()

        cur.execute(_snapshot_dates_sql())
        raw_dates = [row[0] for row in cur.fetchall()]

        def _fmt(d) -> str:
            if hasattr(d, "isoformat"):
                return d.isoformat()
            return str(d)[:10]

        dates = [_fmt(d) for d in raw_dates]
        logger.info("Churn gold: %s distinct snapshot date(s) to load", len(dates))

        for snapshot_date_sql in dates:
            logger.info("Churn gold INSERT snapshot_date=%s", snapshot_date_sql)
            cur.execute(_insert_one_snapshot_date(snapshot_date_sql=snapshot_date_sql))
            cur.fetchall()

        logger.info("Gold churn_risk_daily full batch completed (%s day chunk(s))", len(dates))
        return f"Gold churn_risk_daily rebuilt: {len(dates)} snapshot day(s)"

    except Exception as e:
        logger.error("Churn gold task failed: %s", str(e))
        raise Exception(f"Churn Gold Task Failed: {str(e)}") from e
    finally:
        conn.close()
