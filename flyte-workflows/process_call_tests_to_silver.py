import numpy as np
import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger
from silver_quarantine import NUMERIC_DESTROY_THRESHOLD, insert_quarantine_rows

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

SOURCE_FILE_KEY = "bronze/call_tests.csv"

CALL_TESTS_BRONZE_COL_ORDER = [
    "date_of_test",
    "signal_dbm",
    "speed_m_s",
    "distance_from_site_m",
    "call_test_duration_s",
    "call_test_result",
    "call_test_technology",
    "call_test_setup_time_s",
    "mos",
    "phone_number",
]

CALL_TESTS_RAW_INSERT_SQL = (
    "row_id, date_of_test, signal_dbm, speed_m_s, distance_from_site_m, "
    "call_test_duration_s, call_test_result, call_test_technology, call_test_setup_time_s, "
    "mos, phone_number"
)

CALL_TESTS_NUMERIC_COLS = [
    "signal_dbm",
    "speed_m_s",
    "distance_from_site_m",
    "call_test_duration_s",
    "call_test_setup_time_s",
    "mos",
]


@task(container_image=medallion_image, environment=TASK_ENV)
def process_call_tests_to_silver(target_date_str: str) -> str:
    try:
        logger.info("🟢 Starting call tests bronze -> silver for date=%s", target_date_str)
        s3 = minio_s3_client()

        logger.info("⏳ Downloading bronze/call_tests.csv from MinIO...")
        s3.download_file("warehouse", SOURCE_FILE_KEY, "/tmp/raw_tests.csv")

        df = pd.read_csv("/tmp/raw_tests.csv", sep=";", decimal=",")
        lines = pd.Series(np.arange(2, len(df) + 2, dtype=np.int64), index=df.index)

        df.columns = (
            df.columns.str.strip()
            .str.replace(" ", "_")
            .str.replace("(", "")
            .str.replace(")", "")
            .str.replace("/", "_")
            .str.lower()
        )
        bronze_only = df.copy()
        df.rename(columns={"phone_number": "phone_number"}, inplace=True)

        total_rows = len(df)
        row_quarantine = pd.Series(False, index=df.index)
        issues_by_row: dict[int, list[str]] = {}
        cleaned_by_col: dict[str, pd.Series] = {}

        logger.info("🔧 Cleaning numeric call_tests fields (quarantine + 5%% circuit breaker)...")
        for col in CALL_TESTS_NUMERIC_COLS:
            if col not in df.columns:
                raise ValueError(
                    f"❌ DATA STRUCTURE CHANGE: Expected column '{col}' not found in source data!"
                )
            raw_values = df[col].replace(["", "-", " "], None).copy()
            if df[col].dtype == object:
                ser = df[col].astype(str).str.replace(",", ".")
            else:
                ser = df[col].astype(str).str.replace(",", ".")
            ser = ser.str.replace(r"[a-zA-Z\s]", "", regex=True)
            ser = ser.replace(["", "-"], None)
            cleaned_values = pd.to_numeric(ser, errors="coerce")
            destroyed_mask = raw_values.notna() & cleaned_values.isna()
            destroyed_count = int(destroyed_mask.sum())

            if destroyed_count > 0:
                logger.warning(
                    "⚠️ AUDIT WARNING: Column '%s' had %s values destroyed during cleaning.",
                    col,
                    destroyed_count,
                )

            failure_rate = destroyed_count / total_rows if total_rows else 0.0
            if failure_rate > NUMERIC_DESTROY_THRESHOLD:
                raise ValueError(
                    f"❌ DATA QUALITY BREACH: '{col}' lost {failure_rate * 100:.1f}% of its data!"
                )

            row_quarantine = row_quarantine | destroyed_mask
            for i in df.index[destroyed_mask]:
                issues_by_row.setdefault(int(i), []).append(col)
            cleaned_by_col[col] = cleaned_values

        if "date_of_test" not in df.columns:
            raise ValueError("❌ DATA STRUCTURE CHANGE: Expected column 'date_of_test' not found!")
        raw_dt = df["date_of_test"]
        str_nonempty = raw_dt.notna() & (raw_dt.astype(str).str.strip() != "") & (
            raw_dt.astype(str).str.strip().str.lower() != "nan"
        )
        cleaned_dt = pd.to_datetime(raw_dt, errors="coerce")
        destroyed_dt = str_nonempty & cleaned_dt.isna()
        destroyed_count = int(destroyed_dt.sum())
        if destroyed_count > 0:
            logger.warning(
                "⚠️ AUDIT WARNING: Column 'date_of_test' had %s values destroyed during parsing.",
                destroyed_count,
            )
        failure_rate = destroyed_count / total_rows if total_rows else 0.0
        if failure_rate > NUMERIC_DESTROY_THRESHOLD:
            raise ValueError(
                f"❌ DATA QUALITY BREACH: 'date_of_test' lost {failure_rate * 100:.1f}% of its data!"
            )
        row_quarantine = row_quarantine | destroyed_dt
        for i in df.index[destroyed_dt]:
            issues_by_row.setdefault(int(i), []).append("date_of_test")
        cleaned_by_col["date_of_test"] = cleaned_dt

        for col in CALL_TESTS_NUMERIC_COLS:
            if col in cleaned_by_col:
                df[col] = cleaned_by_col[col]
        df["date_of_test"] = cleaned_by_col["date_of_test"]

        q_idx = df.index[row_quarantine]
        if len(q_idx) > 0:
            conn_q = trino.dbapi.connect(
                host="host.docker.internal",
                port=8080,
                user="flyte",
                catalog="iceberg",
            )
            cur_q = conn_q.cursor()
            try:
                insert_quarantine_rows(
                    cur_q,
                    bronze_only,
                    lines,
                    q_idx,
                    issues_by_row,
                    source_file=SOURCE_FILE_KEY,
                    bronze_col_order=CALL_TESTS_BRONZE_COL_ORDER,
                    raw_table_fqn="iceberg.silver.call_tests_quarantine_raw",
                    raw_columns_sql=CALL_TESTS_RAW_INSERT_SQL,
                    audit_table_fqn="iceberg.silver.call_tests_quarantine_audit",
                    logger=logger,
                    log_label="call_tests",
                )
            finally:
                conn_q.close()
            df.drop(index=q_idx, inplace=True)

        daily_df = df[df["date_of_test"].dt.strftime("%Y-%m-%d") == target_date_str].copy()

        if daily_df.empty:
            logger.warning("No call tests for date=%s; skipping", target_date_str)
            return f"No Call Tests found for {target_date_str}. Skipping."

        staging_key = f"staging/call_tests/day={target_date_str}/data.parquet"
        daily_df.to_parquet("/tmp/clean_tests.parquet", engine="pyarrow", index=False)
        s3.upload_file("/tmp/clean_tests.parquet", "warehouse", staging_key)

        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        safe_date = target_date_str.replace("-", "")
        temp_location = staging_key.replace("/data.parquet", "")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_tests_{safe_date} (
                date_of_test TIMESTAMP(3), signal_dbm DOUBLE, speed_m_s DOUBLE,
                distance_from_site_m DOUBLE, call_test_duration_s DOUBLE, call_test_result VARCHAR,
                call_test_technology VARCHAR, call_test_setup_time_s DOUBLE, mos DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """
        )
        cur.fetchall()

        logger.info("🧹 Limpar dados antigos do dia %s para evitar duplicados...", target_date_str)
        cur.execute(
            f"DELETE FROM iceberg.silver.call_tests WHERE CAST(date_of_test AS DATE) = DATE '{target_date_str}'"
        )
        cur.fetchall()

        cur.execute(
            f"""
            INSERT INTO iceberg.silver.call_tests (
                silver_row_id, date_of_test, signal_dbm, speed_m_s, distance_from_site_m,
                call_test_duration_s, call_test_result, call_test_technology,
                call_test_setup_time_s, mos, phone_number
            )
            SELECT
                (SELECT COALESCE(MAX(silver_row_id), CAST(0 AS BIGINT)) FROM iceberg.silver.call_tests)
                    + ROW_NUMBER() OVER (ORDER BY date_of_test, phone_number),
                date_of_test, signal_dbm, speed_m_s, distance_from_site_m,
                call_test_duration_s, call_test_result, call_test_technology,
                call_test_setup_time_s, mos, phone_number
            FROM hive.staging.temp_tests_{safe_date}
        """
        )
        cur.fetchall()

        cur.execute(f"DROP TABLE hive.staging.temp_tests_{safe_date}")
        cur.fetchall()

        logger.info("✅ Call tests silver load finished for date=%s", target_date_str)
        return f"Successfully loaded Call Tests for {target_date_str}!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
