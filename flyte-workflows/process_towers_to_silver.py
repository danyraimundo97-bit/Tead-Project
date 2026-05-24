import os
import numpy as np
import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger
from workflow_functions.silver_quarantine import NUMERIC_DESTROY_THRESHOLD, insert_quarantine_rows
from workflow_functions.silver_transforms import transform_towers_silver_features
from workflow_functions.trino_acid import replace_table_transaction

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

SOURCE_FILE_KEY = "bronze/towers/"

TOWERS_BRONZE_COL_ORDER = [
    "radio",
    "mcc",
    "net",
    "area",
    "cell",
    "unit",
    "lon",
    "lat",
    "range_m",
    "samples",
    "changeable",
    "created",
    "updated",
    "average_signal",
    "snapshot_date",
    "status",
]

TOWERS_RAW_INSERT_SQL = (
    "row_id, radio, mcc, net, area, cell, unit, lon, lat, range_m, samples, changeable, "
    "created, updated, average_signal, snapshot_date, status"
)

TOWERS_NUMERIC_COLS = [
    "mcc",
    "net",
    "area",
    "cell",
    "unit",
    "lon",
    "lat",
    "range_m",
    "samples",
    "changeable",
    "average_signal",
]


@task(container_image=medallion_image, environment=TASK_ENV)
def process_towers_to_silver() -> str:
    try:
        logger.info("🟢 Starting towers bronze -> silver")
        s3 = minio_s3_client()

        logger.info("⏳ A transferir ficheiros particionados de %s...", SOURCE_FILE_KEY)
        paginator = s3.get_paginator('list_objects_v2')
        dfs = []
        for page in paginator.paginate(Bucket="warehouse", Prefix=SOURCE_FILE_KEY):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                    # As torres usam vírgula como separador
                    dfs.append(pd.read_csv(resp["Body"], sep=","))
        
        if not dfs:
            raise ValueError(f"❌ Nenhum dado particionado encontrado em {SOURCE_FILE_KEY}")
            
        df = pd.concat(dfs, ignore_index=True)

        lines = pd.Series(np.arange(2, len(df) + 2, dtype=np.int64), index=df.index)

        df.rename(
            columns={
                "range": "range_m",
                "averageSignal": "average_signal",
                "Snapshot_Date": "snapshot_date",
                "Status": "status",
            },
            inplace=True,
        )
        df.columns = df.columns.str.lower()
        bronze_only = df.copy()

        total_rows = len(df)
        row_quarantine = pd.Series(False, index=df.index)
        issues_by_row: dict[int, list[str]] = {}
        cleaned_by_col: dict[str, pd.Series] = {}

        logger.info("🔧 Cleaning numeric towers fields (quarantine + 5%% circuit breaker)...")
        for col in TOWERS_NUMERIC_COLS:
            if col not in df.columns:
                raise ValueError(
                    f"❌ DATA STRUCTURE CHANGE: Expected column '{col}' not found in source data!"
                )
            raw_values = df[col].replace(["", "-", " "], None).copy()
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

        for col in TOWERS_NUMERIC_COLS:
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
                    bronze_col_order=TOWERS_BRONZE_COL_ORDER,
                    raw_table_fqn="iceberg.silver.towers_quarantine_raw",
                    raw_columns_sql=TOWERS_RAW_INSERT_SQL,
                    audit_table_fqn="iceberg.silver.towers_quarantine_audit",
                    logger=logger,
                    log_label="towers",
                )
            finally:
                conn_q.close()
            df.drop(index=q_idx, inplace=True)

        df = transform_towers_silver_features(df)

        # Prevenir acumulação de simulações antigas no S3 (Chave composta OpenCelliD + Data)
        initial_towers = len(df)
        df.drop_duplicates(subset=["snapshot_date", "mcc", "net", "area", "cell", "unit"], keep="last", inplace=True)
        if initial_towers - len(df) > 0:
            logger.info(f"Deduplicação Torres: varridas {initial_towers - len(df)} linhas antigas.")

        # Forçar limites da realidade (Clipping geográfico e físico)
        logger.info("A aplicar clipping geofísico às Torres...")
        if "lat" in df.columns:
            df["lat"] = df["lat"].clip(lower=38, upper=42)
        if "lon" in df.columns:
            df["lon"] = df["lon"].clip(lower=-10, upper=-7)
        if "range_m" in df.columns:
            df["range_m"] = df["range_m"].clip(lower=0.0)
        if "average_signal" in df.columns:
            # 0 costuma significar 'desconhecido' no OpenCelliD. Sinal real é negativo.
            df["average_signal"] = df["average_signal"].clip(lower=-140.0, upper=0.0)
        # -------------------------------------------------------

        final_count = len(df)

        logger.info("📍 Preparing %s daily tower snapshots for Silver Layer", final_count)

        # Cria a pasta 'temp' se não existir
        os.makedirs("temp", exist_ok=True)
        # Salva o DataFrame limpo como Parquet e faz upload para o staging
        df.to_parquet("temp/clean_towers.parquet", engine="pyarrow", index=False)
        s3.upload_file("temp/clean_towers.parquet", "warehouse", "staging/towers/data.parquet")

        logger.info("⏳ Loading into Iceberg via Trino...")
        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hive.staging.temp_towers (
                mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER,
                unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER,
                changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE,
                snapshot_date VARCHAR, status BOOLEAN,
                radio_ohe_gsm BOOLEAN, radio_ohe_umts BOOLEAN, radio_ohe_lte BOOLEAN,
                radio_ohe_nr BOOLEAN, radio_ohe_cdma BOOLEAN, radio_ohe_other BOOLEAN
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/towers/')
        """
        )
        cur.fetchall()

        replace_table_transaction(
            conn,
            table_fqn="iceberg.silver.towers",
            insert_sql="""
            INSERT INTO iceberg.silver.towers (
                silver_row_id, mcc, net, area, cell, unit, lon, lat, range_m, samples,
                changeable, created, updated, average_signal, snapshot_date, status,
                radio_ohe_gsm, radio_ohe_umts, radio_ohe_lte, radio_ohe_nr,
                radio_ohe_cdma, radio_ohe_other
            )
            SELECT
                (SELECT COALESCE(MAX(silver_row_id), CAST(0 AS BIGINT)) FROM iceberg.silver.towers)
                    + ROW_NUMBER() OVER (ORDER BY mcc, net, area, cell, unit),
                mcc, net, area, cell, unit, lon, lat, range_m, samples,
                changeable, created, updated, average_signal, snapshot_date, status,
                radio_ohe_gsm, radio_ohe_umts, radio_ohe_lte, radio_ohe_nr,
                radio_ohe_cdma, radio_ohe_other
            FROM hive.staging.temp_towers
            """,
            logger=logger,
        )

        cur.execute("DROP TABLE hive.staging.temp_towers")
        cur.fetchall()

        logger.info("✅ Towers silver load finished (%s rows)", final_count)
        return f"Successfully loaded {final_count} Tower records to Silver!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
