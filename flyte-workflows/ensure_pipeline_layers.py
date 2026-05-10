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

REQUIRED_GOLD_TABLES = frozenset(
    {"churn_risk_daily", "network_quality_daily"}
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


def assert_gold_tables_exist(cur, *, detail: str = "") -> None:
    """Garante que as tabelas Iceberg esperadas existem em ``iceberg.gold``."""
    cur.execute(
        """
        SELECT table_name FROM iceberg.information_schema.tables
        WHERE table_schema = 'gold'
        """
    )
    found = {row[0] for row in cur.fetchall()}
    missing = REQUIRED_GOLD_TABLES - found
    if missing:
        msg = (
            f"iceberg.gold em falta: tabela(s) {sorted(missing)}. "
            "Execute ensure_gold_layer_environment antes dos build gold."
        )
        if detail:
            msg = f"{detail} {msg}"
        raise ValueError(msg)


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
            silver_row_id BIGINT NOT NULL,
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
            silver_row_id BIGINT NOT NULL,
            timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
            network_provider VARCHAR,
            nt_ohe_lte BOOLEAN, nt_ohe_gsm BOOLEAN, nt_ohe_umts BOOLEAN,
            nt_ohe_nr BOOLEAN, nt_ohe_cdma BOOLEAN, nt_ohe_other BOOLEAN,
            rsrp DOUBLE, rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
            uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
            longitude DOUBLE, phone_number VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.network_logs_quarantine_raw (
            row_id VARCHAR,
            "timestamp" VARCHAR,
            deviceid VARCHAR,
            devicemake VARCHAR,
            devicemodel VARCHAR,
            "network provi." VARCHAR,
            networktype VARCHAR,
            rsrp VARCHAR,
            rsrq VARCHAR,
            sinr VARCHAR,
            pci VARCHAR,
            "downlink(mbps)" VARCHAR,
            "uplink(mbps)" VARCHAR,
            "velocity(km/h)" VARCHAR,
            latitude VARCHAR,
            longitude VARCHAR,
            phone_number VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.network_logs_quarantine_audit (
            row_id VARCHAR,
            line_number BIGINT,
            source_file VARCHAR,
            error_description VARCHAR,
            loaded_at TIMESTAMP(3)
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.cdr_quarantine_raw (
            row_id VARCHAR,
            phone_number VARCHAR,
            account_length VARCHAR,
            vmail_message VARCHAR,
            day_mins VARCHAR,
            day_calls VARCHAR,
            day_charge VARCHAR,
            eve_mins VARCHAR,
            eve_calls VARCHAR,
            eve_charge VARCHAR,
            night_mins VARCHAR,
            night_calls VARCHAR,
            night_charge VARCHAR,
            intl_mins VARCHAR,
            intl_calls VARCHAR,
            intl_charge VARCHAR,
            custserv_calls VARCHAR,
            churn VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.cdr_quarantine_audit (
            row_id VARCHAR,
            line_number BIGINT,
            source_file VARCHAR,
            error_description VARCHAR,
            loaded_at TIMESTAMP(3)
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.call_tests_quarantine_raw (
            row_id VARCHAR,
            date_of_test VARCHAR,
            signal_dbm VARCHAR,
            speed_m_s VARCHAR,
            distance_from_site_m VARCHAR,
            call_test_duration_s VARCHAR,
            call_test_result VARCHAR,
            call_test_technology VARCHAR,
            call_test_setup_time_s VARCHAR,
            mos VARCHAR,
            phone_number VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.call_tests_quarantine_audit (
            row_id VARCHAR,
            line_number BIGINT,
            source_file VARCHAR,
            error_description VARCHAR,
            loaded_at TIMESTAMP(3)
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.towers_quarantine_raw (
            row_id VARCHAR,
            radio VARCHAR,
            mcc VARCHAR,
            net VARCHAR,
            area VARCHAR,
            cell VARCHAR,
            unit VARCHAR,
            lon VARCHAR,
            lat VARCHAR,
            range_m VARCHAR,
            samples VARCHAR,
            changeable VARCHAR,
            created VARCHAR,
            updated VARCHAR,
            average_signal VARCHAR,
            snapshot_date VARCHAR,
            status VARCHAR
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.towers_quarantine_audit (
            row_id VARCHAR,
            line_number BIGINT,
            source_file VARCHAR,
            error_description VARCHAR,
            loaded_at TIMESTAMP(3)
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.call_tests (
            silver_row_id BIGINT NOT NULL,
            date_of_test TIMESTAMP(3), signal_dbm DOUBLE, speed_m_s DOUBLE,
            distance_from_site_m DOUBLE, duration_s DOUBLE, setup_time_s DOUBLE,
            result BOOLEAN, mos DOUBLE, phone_number VARCHAR,
            tech_ohe_gsm BOOLEAN, tech_ohe_umts BOOLEAN, tech_ohe_lte BOOLEAN,
            tech_ohe_volte BOOLEAN, tech_ohe_nr BOOLEAN, tech_ohe_other BOOLEAN
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.silver.towers (
            silver_row_id BIGINT NOT NULL,
            mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER,
            unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER,
            changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE,
            snapshot_date VARCHAR, status BOOLEAN,
            radio_ohe_gsm BOOLEAN, radio_ohe_umts BOOLEAN, radio_ohe_lte BOOLEAN,
            radio_ohe_nr BOOLEAN, radio_ohe_cdma BOOLEAN, radio_ohe_other BOOLEAN
        )
        """
    )
    cur.fetchall()


def ensure_gold_schemas_and_iceberg_tables(cur) -> None:
    """Schema ``iceberg.gold`` com localização no warehouse e tabelas gold (Iceberg)."""
    cur.execute(
        "CREATE SCHEMA IF NOT EXISTS iceberg.gold "
        "WITH (location = 's3a://warehouse/gold/')"
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.churn_risk_daily (
            gold_row_id BIGINT NOT NULL,
            "Data_Referencia" DATE,
            "Telefone" VARCHAR,
            "Afetado_Tempestade" BOOLEAN,
            "Receita_Em_Risco" DOUBLE,
            "Tempo_Subscrito" INTEGER,
            "Total_Chamadas_Suporte" INTEGER,
            "Total_Drops" BIGINT,
            "Qualidade_Audio_MOS" DOUBLE,
            "Desistencia" BOOLEAN
        ) WITH (
            format = 'PARQUET'
        )
        """
    )
    cur.fetchall()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS iceberg.gold.network_quality_daily (
            gold_row_id BIGINT NOT NULL,
            "Data_Hora" TIMESTAMP(3),
            "Zona_Leiria" VARCHAR,
            "Latitude_Ocorrencia" DOUBLE,
            "Longitude_Ocorrencia" DOUBLE,
            "ID_Antena_Conectada" BIGINT,
            "Estado_Antena" BOOLEAN,
            "Distancia_Antena_m" DOUBLE,
            "Tecnologia_Rede" VARCHAR,
            "Potencia_RSRP" DOUBLE,
            "Qualidade_RSRQ" DOUBLE,
            "Ruido_SINR" DOUBLE,
            "Velocidade_Downlink" DOUBLE,
            "Telefones_Sucesso" BIGINT,
            "Telefones_Falha" BIGINT,
            "Telefones_Sem_Teste" BIGINT
        ) WITH (
            format = 'PARQUET'
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
    """Silver populável + schema ``iceberg.gold`` e DDL das tabelas gold (como na camada silver)."""
    conn = trino.dbapi.connect(
        host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
    )
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1")
        cur.fetchall()
        assert_silver_tables_exist(cur, detail="Gold env:")
        ensure_gold_schemas_and_iceberg_tables(cur)
        assert_gold_tables_exist(cur, detail="Gold env post-DDL:")
    except Exception as e:
        # Trino/driver errors sometimes carry types that break Flyte protobuf error serialization.
        logger.exception("ensure_gold_layer_environment failed")
        raise RuntimeError(str(e)) from None
    finally:
        conn.close()

    msg = (
        "Gold OK: iceberg.gold at s3a://warehouse/gold/ with "
        f"{len(REQUIRED_GOLD_TABLES)} table(s); "
        f"iceberg.silver OK."
    )
    logger.info(msg)
    return msg
