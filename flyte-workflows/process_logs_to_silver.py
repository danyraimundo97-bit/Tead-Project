import os
import numpy as np
import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger
from workflow_functions.silver_quarantine import NUMERIC_DESTROY_THRESHOLD, insert_quarantine_rows
from workflow_functions.silver_transforms import transform_network_logs_silver_features
from workflow_functions.trino_acid import replace_table_transaction

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

SOURCE_FILE_KEY = "bronze/network_logs/"

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
def process_logs_to_silver() -> str:
    """Cleans network logs and loads the full bronze batch into silver (replace)."""
    try:
        logger.info("🟢 Starting network logs bronze -> silver (full batch)")
        s3 = minio_s3_client()

        logger.info("⏳ A transferir ficheiros particionados de %s...", SOURCE_FILE_KEY)
        paginator = s3.get_paginator('list_objects_v2')
        dfs = []
        for page in paginator.paginate(Bucket="warehouse", Prefix=SOURCE_FILE_KEY):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                    dfs.append(pd.read_csv(resp["Body"], sep=";"))
        
        if not dfs:
            raise ValueError(f"❌ Nenhum dado particionado encontrado em {SOURCE_FILE_KEY}")
            
        df = pd.concat(dfs, ignore_index=True)

        lines = pd.Series(np.arange(2, len(df) + 2, dtype=np.int64), index=df.index)

        df.columns = df.columns.str.strip().str.lower()
        
        # Remover colunas duplicadas (mantendo a nossa injeção da tempestade que está no final)
        df = df.loc[:, ~df.columns.duplicated(keep='last')]
        
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

        df = transform_network_logs_silver_features(df)

        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", format="mixed")
        df = df.dropna(subset=["timestamp"])
        if df.empty:
            logger.warning("No network logs with valid timestamps; skipping silver load")
            return "No Network Logs with valid timestamps. Skipping."

        logger.info("Prepared %s network log rows for full silver load", len(df))

        df.rename(columns={"timestamp": "timestamp_log"}, inplace=True)

        # --- FORÇAR LIMITES DE RADIOFREQUÊNCIA (RF BOUNDARIES) ---
        logger.info("A aplicar clipping geofísico aos sinais de rede...")
        if "rsrp" in df.columns:
            df["rsrp"] = df["rsrp"].clip(lower=-140.0, upper=-60.0)
        if "rsrq" in df.columns:
            df["rsrq"] = df["rsrq"].clip(lower=-30.0, upper=2.0)
        if "sinr" in df.columns:
            df["sinr"] = df["sinr"].clip(lower=-25.0, upper=25.0)
        # ---------------------------------------------------------

        # Cria a pasta 'temp' se não existir
        os.makedirs("temp", exist_ok=True)
        # Salva o DataFrame limpo como Parquet e faz upload para o staging
        staging_key = "staging/network_logs/batch/data.parquet"
        df.to_parquet("temp/clean_logs.parquet", engine="pyarrow", index=False)
        s3.upload_file("temp/clean_logs.parquet", "warehouse", staging_key)
        logger.info("Uploaded staging %s; loading Trino silver.network_logs", staging_key)

        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        temp_location = staging_key.replace("/data.parquet", "")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_logs_batch (
                timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
                network_provider VARCHAR,
                nt_ohe_lte BOOLEAN, nt_ohe_gsm BOOLEAN, nt_ohe_umts BOOLEAN,
                nt_ohe_nr BOOLEAN, nt_ohe_cdma BOOLEAN, nt_ohe_other BOOLEAN,
                rsrp DOUBLE, rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
                uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
                longitude DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """
        )
        cur.fetchall()

        logger.info("Replacing iceberg.silver.network_logs with full batch (ACID)")
        replace_table_transaction(
            conn,
            table_fqn="iceberg.silver.network_logs",
            insert_sql="""
            INSERT INTO iceberg.silver.network_logs (
                silver_row_id, timestamp_log, devicemake, devicemodel, network_provider,
                nt_ohe_lte, nt_ohe_gsm, nt_ohe_umts, nt_ohe_nr, nt_ohe_cdma, nt_ohe_other,
                rsrp, rsrq, sinr, pci, downlink_mbps, uplink_mbps, velocity_kmh,
                latitude, longitude, phone_number
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY timestamp_log, phone_number),
                timestamp_log, devicemake, devicemodel, network_provider,
                nt_ohe_lte, nt_ohe_gsm, nt_ohe_umts, nt_ohe_nr, nt_ohe_cdma, nt_ohe_other,
                rsrp, rsrq, sinr, pci, downlink_mbps, uplink_mbps, velocity_kmh,
                latitude, longitude, phone_number
            FROM hive.staging.temp_logs_batch
            """,
            logger=logger,
        )

        cur.execute("DROP TABLE hive.staging.temp_logs_batch")
        cur.fetchall()

        logger.info("Network logs silver load finished (full batch, %s rows)", len(df))
        return f"Successfully loaded {len(df)} Network Log rows (full batch)!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
