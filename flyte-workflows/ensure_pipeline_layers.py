"""Garantir ambiente, schemas e tabelas Iceberg silver/gold antes dos passos do pipeline."""

from __future__ import annotations

import trino
from botocore.exceptions import ClientError
from flytekit import ImageSpec, task

from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger

logger = get_logger(__name__)

WAREHOUSE_BUCKET = "warehouse"

REQUIRED_SILVER_TABLES = frozenset(
    {"network_logs", "cdr_customers", "call_tests", "towers"}
)

BRONZE_OBJECT_KEYS = (
    "bronze/cdr_customers.csv",
    "bronze/network_logs.csv",
    "bronze/call_tests.csv",
    "bronze/towers.csv",
)

ensure_image = ImageSpec(
    name="jdpt_lakehouse_ensure",
    packages=["boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)


def assert_silver_tables_exist(cur, *, detail: str = "") -> None:
    """Garante que as quatro tabelas Iceberg em ``iceberg.silver`` existem."""
    cur.execute(
        """
        SELECT table_name FROM iceberg.information_schema.tables
        WHERE table_schema = 'silver'
        """
    )
    found = {row[0] for row in cur.fetchall()}
    missing = REQUIRED_SILVER_TABLES - found
    if missing:
        msg = (
            f"iceberg.silver em falta: tabela(s) {sorted(missing)}. "
            "Execute primeiro as tasks bronze → silver."
        )
        if detail:
            msg = f"{detail} {msg}"
        raise ValueError(msg)


def ensure_silver_schemas_and_iceberg_tables(cur) -> None:
    """Hive staging + Iceberg silver schema e tabelas silver (sem tabelas temp de staging)."""
    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS hive.staging "
        "WITH (location = 's3a://warehouse/staging/')"
    )
    cur.fetchall()

    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS iceberg.silver "
        "WITH (location = 's3a://warehouse/silver/')"
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.cdr_customers (
            phone_number VARCHAR, account_length INTEGER, vmail_message INTEGER,
            day_mins DOUBLE, day_calls INTEGER, day_charge DOUBLE,
            eve_mins DOUBLE, eve_calls INTEGER, eve_charge DOUBLE,
            night_mins DOUBLE, night_calls INTEGER, night_charge DOUBLE,
            intl_mins DOUBLE, intl_calls INTEGER, intl_charge DOUBLE,
            custserv_calls INTEGER, churn BOOLEAN
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.network_logs (
            timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
            network_provider VARCHAR, network_type VARCHAR, rsrp DOUBLE,
            rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
            uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
            longitude DOUBLE, phone_number VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.call_tests (
            date_of_test TIMESTAMP(3), signal_dbm DOUBLE, speed_m_s DOUBLE,
            distance_from_site_m DOUBLE, call_test_duration_s DOUBLE, call_test_result VARCHAR,
            call_test_technology VARCHAR, call_test_setup_time_s DOUBLE, mos DOUBLE, phone_number VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.towers (
            radio VARCHAR, mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER,
            unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER,
            changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE,
            snapshot_date VARCHAR, status VARCHAR
        )
        """
    )
    cur.fetchall()


@task(container_image=ensure_image, environment=TASK_ENV)
def ensure_silver_layer_environment() -> str:
    """Bronze no MinIO, Trino OK, schemas hive.staging / iceberg.silver e tabelas Iceberg silver."""
    s3 = minio_s3_client()
    for key in BRONZE_OBJECT_KEYS:
        try:
            s3.head_object(Bucket=WAREHOUSE_BUCKET, Key=key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey", "NotFound"):
                raise ValueError(
                    f"Bronze em falta no MinIO: s3://{WAREHOUSE_BUCKET}/{key}"
                ) from e
            raise
        logger.info("Bronze OK: s3://%s/%s", WAREHOUSE_BUCKET, key)

    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1")
        cur.fetchall()
        ensure_silver_schemas_and_iceberg_tables(cur)
    finally:
        conn.close()

    msg = (
        "Silver garantido: bronze no MinIO, Trino OK, hive.staging + iceberg.silver "
        f"e {len(REQUIRED_SILVER_TABLES)} tabelas Iceberg."
    )
    logger.info(msg)
    return msg


@task(container_image=ensure_image, environment=TASK_ENV)
def ensure_gold_layer_environment() -> str:
    """Schema gold e presença das tabelas silver antes do CTAS gold."""
    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.gold")
        cur.fetchall()
        assert_silver_tables_exist(cur, detail="Ambiente gold:")
    finally:
        conn.close()

    msg = (
        "Gold garantido: schema iceberg.gold e tabelas iceberg.silver "
        f"({', '.join(sorted(REQUIRED_SILVER_TABLES))}) OK."
    )
    logger.info(msg)
    return msg
