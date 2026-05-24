import os
import numpy as np
import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger
from workflow_functions.silver_quarantine import NUMERIC_DESTROY_THRESHOLD, insert_quarantine_rows
from workflow_functions.silver_transforms import transform_call_tests_silver_features
from workflow_functions.trino_acid import replace_table_transaction

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

SOURCE_FILE_KEY = "bronze/call_tests/"

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
def process_call_tests_to_silver() -> str:
    try:
        logger.info("🟢 Starting call tests bronze -> silver (full batch)")
        s3 = minio_s3_client()

        logger.info("⏳ A transferir ficheiros particionados de %s...", SOURCE_FILE_KEY)
        paginator = s3.get_paginator('list_objects_v2')
        dfs = []
        for page in paginator.paginate(Bucket="warehouse", Prefix=SOURCE_FILE_KEY):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".csv"):
                    resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                    # ATENÇÃO ao decimal="," exigido nos Call Tests!
                    dfs.append(pd.read_csv(resp["Body"], sep=";", decimal=","))
        
        if not dfs:
            raise ValueError(f"❌ Nenhum dado particionado encontrado em {SOURCE_FILE_KEY}")
            
        df = pd.concat(dfs, ignore_index=True)

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
        # CSV com subsegundos (ex. ns) — inferência fixa %Y-%m-%d %H:%M:%S gera NaT em massa
        cleaned_dt = pd.to_datetime(raw_dt, errors="coerce", format="mixed")
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

        df = transform_call_tests_silver_features(df)

        df = df.dropna(subset=["date_of_test"])
        if df.empty:
            logger.warning("No call tests with valid date_of_test; skipping silver load")
            return "No Call Tests with valid dates. Skipping."

        # Prevenir acumulação de simulações antigas no S3
        initial_tests = len(df)
        df.drop_duplicates(subset=["date_of_test", "phone_number"], keep="last", inplace=True)
        if initial_tests - len(df) > 0:
            logger.info(f"Deduplicação Call Tests: varridas {initial_tests - len(df)} linhas fantasma antigas.")

        # Forçar limites de sinal e de negócio (QA) antes de carregar para Silver
        logger.info("A aplicar clipping físico e de negócio aos Call Tests...")
        if "signal_dbm" in df.columns:
            df["signal_dbm"] = df["signal_dbm"].clip(lower=-140.0, upper=-30.0)
        if "mos" in df.columns:
            df["mos"] = df["mos"].clip(lower=1.0, upper=5.0)
        if "distance_from_site_m" in df.columns:
            df["distance_from_site_m"] = df["distance_from_site_m"].clip(lower=0.0)
        if "speed_m_s" in df.columns:
            df["speed_m_s"] = df["speed_m_s"].clip(lower=0.0)
        # -------------------------------------------------------

        logger.info("Prepared %s call test rows for full silver load", len(df))

        # Cria a pasta 'temp' se não existir
        os.makedirs("temp", exist_ok=True)
        # Salva o DataFrame limpo como Parquet e faz upload para o staging
        staging_key = "staging/call_tests/batch/data.parquet"
        df.to_parquet("temp/clean_tests.parquet", engine="pyarrow", index=False)
        s3.upload_file("temp/clean_tests.parquet", "warehouse", staging_key)

        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        temp_location = staging_key.replace("/data.parquet", "")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_tests_batch (
                date_of_test TIMESTAMP(3), signal_dbm DOUBLE, speed_m_s DOUBLE,
                distance_from_site_m DOUBLE, duration_s DOUBLE, setup_time_s DOUBLE,
                result BOOLEAN, mos DOUBLE, phone_number VARCHAR,
                tech_ohe_gsm BOOLEAN, tech_ohe_umts BOOLEAN, tech_ohe_lte BOOLEAN,
                tech_ohe_volte BOOLEAN, tech_ohe_nr BOOLEAN, tech_ohe_other BOOLEAN
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """
        )
        cur.fetchall()

        logger.info("Replacing iceberg.silver.call_tests with full batch (ACID)")
        replace_table_transaction(
            conn,
            table_fqn="iceberg.silver.call_tests",
            insert_sql="""
            INSERT INTO iceberg.silver.call_tests (
                silver_row_id, date_of_test, signal_dbm, speed_m_s, distance_from_site_m,
                duration_s, setup_time_s, result, mos, phone_number,
                tech_ohe_gsm, tech_ohe_umts, tech_ohe_lte, tech_ohe_volte,
                tech_ohe_nr, tech_ohe_other
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY date_of_test, phone_number),
                date_of_test, signal_dbm, speed_m_s, distance_from_site_m,
                duration_s, setup_time_s, result, mos, phone_number,
                tech_ohe_gsm, tech_ohe_umts, tech_ohe_lte, tech_ohe_volte,
                tech_ohe_nr, tech_ohe_other
            FROM hive.staging.temp_tests_batch
            """,
            logger=logger,
        )

        cur.execute("DROP TABLE hive.staging.temp_tests_batch")
        cur.fetchall()

        logger.info("✅ Call tests silver load finished (full batch, %s rows)", len(df))
        return f"Successfully loaded {len(df)} Call Test rows (full batch)!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
