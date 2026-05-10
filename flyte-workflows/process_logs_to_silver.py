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

SOURCE_FILE_KEY = "bronze/network_logs.csv"

BRONZE_QUARANTINE_COLS = [
    "timestamp",
    "deviceid",
    "devicemake",
    "devicemodel",
    "network provi.",
    "networktype",
    "rsrp",
    "rsrq",
    "sinr",
    "pci",
    "downlink(mbps)",
    "uplink(mbps)",
    "velocity(km/h)",
    "latitude",
    "longitude",
    "phone_number",
]

_RAW_INSERT_NAMES = (
    "row_id, "
    '"timestamp", deviceid, devicemake, devicemodel, "network provi.", networktype, '
    "rsrp, rsrq, sinr, pci, "
    '"downlink(mbps)", "uplink(mbps)", "velocity(km/h)", '
    "latitude, longitude, phone_number"
)


@task(container_image=medallion_image, environment=TASK_ENV)
def process_logs_to_silver(target_date_str: str) -> str:
    """Cleans Network Logs, applies GPS offset, and uploads partitioned by Day."""
    try:
        logger.info("Starting network logs bronze -> silver for date=%s", target_date_str)
        s3 = minio_s3_client()

        logger.info("Downloading bronze/network_logs.csv from MinIO")
        s3.download_file("warehouse", SOURCE_FILE_KEY, "/tmp/raw_logs.csv")

        df = pd.read_csv("/tmp/raw_logs.csv", sep=";")
        lines = pd.Series(np.arange(2, len(df) + 2, dtype=np.int64), index=df.index)

        df.columns = df.columns.str.strip().str.lower()
        bronze_only = df.copy()

        df.rename(
            columns={
                "network provi.": "network_provider",
                "networktype": "network_type",
                "downlink(mbps)": "downlink_mbps",
                "uplink(mbps)": "uplink_mbps",
                "velocity(km/h)": "velocity_kmh",
            },
            inplace=True,
        )

        logger.info("🔧 Cleaning decimal formatting and enforcing numeric types...")
        numeric_cols = [
            "rsrp",
            "rsrq",
            "sinr",
            "pci",
            "downlink_mbps",
            "uplink_mbps",
            "velocity_kmh",
            "latitude",
            "longitude",
        ]

        total_rows = len(df)
        row_quarantine = pd.Series(False, index=df.index)
        issues_by_row: dict[int, list[str]] = {}
        cleaned_by_col: dict[str, pd.Series] = {}

        for col in numeric_cols:
            if col not in df.columns:
                error_msg = (
                    f"❌ DATA STRUCTURE CHANGE: Expected column '{col}' not found in source data! "
                    "Pipeline halted to prevent corruption."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            raw_values = df[col].replace(["", "-", " "], None).copy()
            clean_col = df[col].astype(str).str.replace(",", ".")
            if col == "velocity_kmh":
                clean_col = clean_col.str.replace(r"[^\d.-]", "", regex=True)
            else:
                clean_col = clean_col.str.replace(r"[a-zA-Z\s]", "", regex=True)
            clean_col = clean_col.replace(["", "-"], None)
            cleaned_values = pd.to_numeric(clean_col, errors="coerce")

            destroyed_mask = raw_values.notna() & cleaned_values.isna()
            destroyed_count = int(destroyed_mask.sum())
            logger.debug(
                "Column '%s': %s values destroyed out of %s total rows.",
                col,
                destroyed_count,
                total_rows,
            )

            if destroyed_count > 0:
                sample_wiped = raw_values[destroyed_mask].head(5).tolist()
                logger.warning(
                    "⚠️ AUDIT WARNING: Column '%s' had %s values destroyed during cleaning.",
                    col,
                    destroyed_count,
                )
                logger.warning("   -> Example destroyed values: %s", sample_wiped)

            failure_rate = destroyed_count / total_rows if total_rows else 0.0
            if failure_rate > NUMERIC_DESTROY_THRESHOLD:
                error_msg = (
                    f"❌ DATA QUALITY BREACH: '{col}' lost {failure_rate * 100:.1f}% of its data! "
                    "Pipeline halted to prevent corruption."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)

            row_quarantine = row_quarantine | destroyed_mask
            for i in df.index[destroyed_mask]:
                issues_by_row.setdefault(int(i), []).append(col)

            cleaned_by_col[col] = cleaned_values

        for col in numeric_cols:
            df[col] = cleaned_by_col[col]

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
                    bronze_col_order=BRONZE_QUARANTINE_COLS,
                    raw_table_fqn="iceberg.silver.network_logs_quarantine_raw",
                    raw_columns_sql=_RAW_INSERT_NAMES,
                    audit_table_fqn="iceberg.silver.network_logs_quarantine_audit",
                    logger=logger,
                    log_label="network_logs",
                )
            finally:
                conn_q.close()

        df.drop(index=q_idx, inplace=True)

        before_gps = len(df)
        df.dropna(subset=["latitude", "longitude"], inplace=True)
        logger.info(
            "Dropped %s rows with missing lat/lon (%s rows remain)",
            before_gps - len(df),
            len(df),
        )

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        daily_df = df[df["timestamp"].dt.strftime("%Y-%m-%d") == target_date_str].copy()

        if daily_df.empty:
            logger.warning("No network logs for date=%s; skipping", target_date_str)
            return f"No Network Logs found for {target_date_str}. Skipping."

        logger.info(
            "Filtered network logs to %s rows for date=%s", len(daily_df), target_date_str
        )

        daily_df.rename(columns={"timestamp": "timestamp_log"}, inplace=True)

        staging_key = f"staging/network_logs/day={target_date_str}/data.parquet"
        daily_df.to_parquet("/tmp/clean_logs.parquet", engine="pyarrow", index=False)
        s3.upload_file("/tmp/clean_logs.parquet", "warehouse", staging_key)
        logger.info("Uploaded staging %s; loading Trino silver.network_logs", staging_key)

        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        safe_date = target_date_str.replace("-", "")

        temp_location = staging_key.replace("/data.parquet", "")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_logs_{safe_date} (
                timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
                network_provider VARCHAR, network_type VARCHAR, rsrp DOUBLE,
                rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
                uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
                longitude DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """
        )
        cur.fetchall()

        logger.info(
            "🧹 Limpar dados antigos do dia %s para evitar duplicados...", target_date_str
        )
        cur.execute(
            f"DELETE FROM iceberg.silver.network_logs WHERE CAST(timestamp_log AS DATE) = DATE '{target_date_str}'"
        )
        cur.fetchall()

        cur.execute(
            f"""
            INSERT INTO iceberg.silver.network_logs (
                silver_row_id, timestamp_log, devicemake, devicemodel, network_provider,
                network_type, rsrp, rsrq, sinr, pci, downlink_mbps, uplink_mbps, velocity_kmh,
                latitude, longitude, phone_number
            )
            SELECT
                (SELECT COALESCE(MAX(silver_row_id), CAST(0 AS BIGINT)) FROM iceberg.silver.network_logs)
                    + ROW_NUMBER() OVER (ORDER BY timestamp_log, phone_number),
                timestamp_log, devicemake, devicemodel, network_provider, network_type,
                rsrp, rsrq, sinr, pci, downlink_mbps, uplink_mbps, velocity_kmh,
                latitude, longitude, phone_number
            FROM hive.staging.temp_logs_{safe_date}
        """
        )
        cur.fetchall()

        cur.execute(f"DROP TABLE hive.staging.temp_logs_{safe_date}")
        cur.fetchall()

        logger.info("Network logs silver load finished for date=%s", target_date_str)
        return f"Successfully loaded Network Logs for {target_date_str}!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
