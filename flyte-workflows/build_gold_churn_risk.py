import trino
from flytekit import ImageSpec, task

from flyte_task_env import TASK_ENV
from workflow_functions.loki_logging import get_logger

logger = get_logger(__name__)

gold_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000",
)

_COUNT_CDR = """
    SELECT COUNT(*) FROM iceberg.silver.cdr_customers
"""

_CHECK_GOLD_CHURN_COLUMNS = """
    SELECT
        COUNT(*) AS n,
        COUNT_IF("Telefone" IS NULL) AS telefone_nulls,
        COUNT_IF("Data_Referencia" IS NULL) AS data_ref_nulls,
        COUNT_IF("Desistencia" IS NULL) AS desistencia_nulls
    FROM iceberg.gold.churn_risk_daily
"""


def _assert_churn_columns_sane(cur, *, context: str) -> None:
    """Fail if the gold table has rows but any core column is NULL for every row."""
    cur.execute(_CHECK_GOLD_CHURN_COLUMNS)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError(f"{context}: no stats row from gold churn table")
    n, tel_n, ref_n, des_n = (int(row[0]), int(row[1]), int(row[2]), int(row[3]))
    if n == 0:
        logger.warning("%s: gold.churn_risk_daily is empty", context)
        return
    problems: list[str] = []
    if tel_n == n:
        problems.append("Telefone is NULL for every row")
    if ref_n == n:
        problems.append("Data_Referencia is NULL for every row")
    if des_n == n:
        problems.append("Desistencia is NULL for every row")
    if problems:
        raise RuntimeError(f"{context}: {'; '.join(problems)}")


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
    """One day: CDR × pre-aggregated facts; assigns monotonic gold_row_id."""
    return f"""
        INSERT INTO iceberg.gold.churn_risk_daily
        SELECT
            (SELECT COALESCE(MAX(gold_row_id), CAST(0 AS BIGINT))
                FROM iceberg.gold.churn_risk_daily)
                + ROW_NUMBER() OVER (ORDER BY cdr.phone_number) AS gold_row_id,
            DATE '{snapshot_date_sql}' AS "Data_Referencia",
            cdr.phone_number AS "Telefone",
            COALESCE(al.storm_affected, FALSE) AS "Afetado_Tempestade",
            (cdr.day_charge + cdr.eve_charge + cdr.night_charge + cdr.intl_charge)
                AS "Receita_Em_Risco",
            cdr.account_length AS "Tempo_Subscrito",
            cdr.custserv_calls AS "Total_Chamadas_Suporte",
            COALESCE(ac.total_drops, CAST(0 AS BIGINT)) AS "Total_Drops",
            ac.qualidade_audio_mos AS "Qualidade_Audio_MOS",
            cdr.churn AS "Desistencia"
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
    Gold Produto B: uma linha por cliente por dia (Data_Referencia), com gold_row_id.
    Inserções por dia para limitar memória no Trino; validação pré/pós como network quality.
    """
    logger.info("Gold churn_risk_daily: full batch from silver (chunked by Data_Referencia)")

    conn = trino.dbapi.connect(
        host="host.docker.internal",
        port=8080,
        user="flyte",
        catalog="iceberg",
    )
    cur = conn.cursor()

    try:
        cur.execute(_COUNT_CDR)
        n_cdr = int(cur.fetchone()[0])
        if n_cdr == 0:
            raise RuntimeError(
                "Silver cdr_customers is empty; cannot build churn gold"
            )
        logger.info("Silver CDR rows: %s", n_cdr)

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
        logger.info("Churn gold: %s distinct Data_Referencia value(s) to load", len(dates))

        if len(dates) == 0:
            logger.warning(
                "No snapshot dates in call_tests or network_logs; gold churn stays empty"
            )
            _assert_churn_columns_sane(cur, context="After churn rebuild (no dates)")
            return "Gold churn_risk_daily empty (no activity dates in silver)"

        for snapshot_date_sql in dates:
            logger.debug("Churn gold INSERT Data_Referencia=%s", snapshot_date_sql)
            cur.execute(_insert_one_snapshot_date(snapshot_date_sql=snapshot_date_sql))
            cur.fetchall()

        _assert_churn_columns_sane(cur, context="After INSERT churn_risk")

        logger.info("Gold churn_risk_daily full batch completed (%s day chunk(s))", len(dates))
        return f"Gold churn_risk_daily rebuilt: {len(dates)} snapshot day(s)"

    except Exception as e:
        logger.error("Churn gold task failed: %s", str(e))
        raise RuntimeError(f"Churn Gold Task Failed: {str(e)}") from None
    finally:
        conn.close()
