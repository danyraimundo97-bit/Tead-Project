import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

_REQUIRED_SILVER_TABLES = frozenset(
    {"network_logs", "cdr_customers", "call_tests", "towers"}
)


def _assert_silver_tables_exist(cur) -> None:
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
            "Cannot build gold: iceberg.silver is missing table(s) "
            f"{sorted(missing)}. Load silver first."
        )


medallion_image = ImageSpec(
    name="jdpt_lakehouse_gold",
    packages=["trino", "python-logging-loki"],
    registry="localhost:30000",
)


@task(container_image=medallion_image, environment=TASK_ENV)
def build_gold_churn_risk() -> str:
    try:
        logger.info("Building gold table iceberg.gold.churn_risk_daily")
        conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()

        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
        cur.fetchall()
        _assert_silver_tables_exist(cur)

        sql = """
        CREATE OR REPLACE TABLE iceberg.gold.churn_risk_daily AS
        WITH customer_network_impact AS (
            SELECT
                CAST(date_of_test AS DATE) AS data_evento,
                phone_number,
                AVG(mos) AS avg_daily_mos,
                COUNT(CASE WHEN call_test_result = 'DROP' THEN 1 END) AS total_drops
            FROM iceberg.silver.call_tests
            GROUP BY CAST(date_of_test AS DATE), phone_number
        )
        SELECT
            i.data_evento, c.phone_number, i.avg_daily_mos, i.total_drops,
            (c.day_charge + c.eve_charge + c.night_charge) AS daily_charge,
            c.custserv_calls, c.churn
        FROM customer_network_impact i
        JOIN iceberg.silver.cdr_customers c ON i.phone_number = c.phone_number
        """

        cur.execute(sql)
        cur.fetchall()
        logger.info("Gold table churn_risk_daily created successfully")
        return "Gold Table: Churn Risk Daily created successfully!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")