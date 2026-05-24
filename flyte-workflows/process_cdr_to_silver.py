import gc
import os

import numpy as np
import pandas as pd
import trino
from flytekit import ImageSpec, Resources, task

from flyte_task_env import TASK_ENV, minio_s3_client
from workflow_functions.loki_logging import get_logger
from workflow_functions.silver_quarantine import NUMERIC_DESTROY_THRESHOLD, insert_quarantine_rows
from workflow_functions.trino_acid import replace_table_transaction

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000",
)

SOURCE_FILE_KEY = "bronze/cdr_customers/"

CDR_BRONZE_COL_ORDER = [
    "phone_number",
    "account_length",
    "vmail_message",
    "day_mins",
    "day_calls",
    "day_charge",
    "eve_mins",
    "eve_calls",
    "eve_charge",
    "night_mins",
    "night_calls",
    "night_charge",
    "intl_mins",
    "intl_calls",
    "intl_charge",
    "custserv_calls",
    "churn",
]

CDR_RAW_INSERT_SQL = (
    "row_id, phone_number, account_length, vmail_message, day_mins, day_calls, day_charge, "
    "eve_mins, eve_calls, eve_charge, night_mins, night_calls, night_charge, intl_mins, "
    "intl_calls, intl_charge, custserv_calls, churn"
)

CDR_NUMERIC_COLS = [c for c in CDR_BRONZE_COL_ORDER if c not in ("phone_number", "churn")]


def _clean_churn_series(raw: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Devolve (cleaned_boolean, destroyed_mask)."""
    if raw.dtype == bool:
        return raw.copy(), pd.Series(False, index=raw.index)
    if pd.api.types.is_numeric_dtype(raw):
        num = pd.to_numeric(raw, errors="coerce")
        destroyed = (num.isna() & raw.notna()) | (num.notna() & ~num.isin([0, 1]))
        cleaned = num.isin([1])
        return cleaned, destroyed
    m = raw.astype(str).str.strip().str.lower()
    raw_meaningful = m.notna() & (m != "") & (m != "nan")
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "t": True,
        "f": False,
        "yes": True,
        "no": False,
    }
    cleaned = m.map(mapping)
    destroyed = raw_meaningful & cleaned.isna()
    return cleaned, destroyed


def _cdr_task_resources() -> tuple[Resources, Resources]:
    """Small defaults for dev / laptops; override at ``pyflyte register`` time via host env."""
    return (
        Resources(
            cpu=os.environ.get("FLYTE_CDR_TASK_CPU_REQUEST", "500m"),
            mem=os.environ.get("FLYTE_CDR_TASK_MEM_REQUEST", "512Mi"),
        ),
        Resources(
            cpu=os.environ.get("FLYTE_CDR_TASK_CPU_LIMIT", "2"),
            mem=os.environ.get("FLYTE_CDR_TASK_MEM_LIMIT", "2Gi"),
        ),
    )


_cdr_requests, _cdr_limits = _cdr_task_resources()


@task(
    container_image=medallion_image,
    environment=TASK_ENV,
    requests=_cdr_requests,
    limits=_cdr_limits,
)
def process_cdr_to_silver() -> str:
    """Downloads Bronze CDR, deduplicates it, and uploads to Silver Iceberg."""
    try:
        logger.info("🟢 Starting CDR bronze -> silver")
        s3 = minio_s3_client()

        logger.info("⏳ A transferir ficheiros particionados de %s...", SOURCE_FILE_KEY)
        paginator = s3.get_paginator("list_objects_v2")
        df: pd.DataFrame | None = None
        for page in paginator.paginate(Bucket="warehouse", Prefix=SOURCE_FILE_KEY):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".csv"):
                    continue
                resp = s3.get_object(Bucket="warehouse", Key=obj["Key"])
                try:
                    part = pd.read_csv(resp["Body"], sep=";")
                finally:
                    resp["Body"].close()
                if df is None:
                    df = part
                else:
                    df = pd.concat([df, part], ignore_index=True, copy=False)
                    del part
        # Limpar o cache do pandas
        gc.collect()
        if df is None:
            raise ValueError(f"❌ Nenhum dado particionado encontrado em {SOURCE_FILE_KEY}")

        lines = pd.Series(np.arange(2, len(df) + 2, dtype=np.int64), index=df.index)

        # normalizar os nomes das colunas
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.lower()
        bronze_only = df.copy()
        # TODO: Testar se o rename é necessário
        #df.rename(columns={"phone_number": "phone_number"}, inplace=True)

        total_rows = len(df)
        row_quarantine = pd.Series(False, index=df.index)
        issues_by_row: dict[int, list[str]] = {}
        cleaned_by_col: dict[str, pd.Series] = {}

        logger.info("🔧 Cleaning numeric CDR fields (quarantine + 5%% circuit breaker)...")
        for col in CDR_NUMERIC_COLS:
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
            # adicionar as linhas destruídas ao quarantena
            row_quarantine = row_quarantine | destroyed_mask
            # adicionar as colunas destruídas ao dicionário de problemas
            for i in df.index[destroyed_mask]:
                # adicionar a coluna destruída ao dicionário de problemas
                issues_by_row.setdefault(int(i), []).append(col)
            # adicionar os valores limpos às colunas limpas
            cleaned_by_col[col] = cleaned_values

        if "churn" not in df.columns:
            raise ValueError("❌ DATA STRUCTURE CHANGE: Expected column 'churn' not found!")
        # limpar a coluna churn
        cleaned_churn, destroyed_churn = _clean_churn_series(df["churn"])
        # contar as linhas destruídas
        destroyed_count = int(destroyed_churn.sum())
        # se houver linhas destruídas, emitir um aviso
        if destroyed_count > 0:
            logger.warning(
                "⚠️ AUDIT WARNING: Column 'churn' had %s values destroyed during cleaning.",
                destroyed_count,
            )
        failure_rate = destroyed_count / total_rows if total_rows else 0.0
        if failure_rate > NUMERIC_DESTROY_THRESHOLD:
            raise ValueError(
                f"❌ DATA QUALITY BREACH: 'churn' lost {failure_rate * 100:.1f}% of its data!"
            )
        row_quarantine = row_quarantine | destroyed_churn
        for i in df.index[destroyed_churn]:
            issues_by_row.setdefault(int(i), []).append("churn")
        cleaned_by_col["churn"] = cleaned_churn

        # adicionar os valores limpos às colunas limpas
        for col in CDR_NUMERIC_COLS:
            df[col] = cleaned_by_col[col]
        # adicionar os valores limpos à coluna churn
        df["churn"] = cleaned_by_col["churn"].fillna(False).astype(bool)

        # obter os índices das linhas quarantinadas
        q_idx = df.index[row_quarantine]
        # se houver linhas quarantinadas, inserir as linhas na tabela de quarantena
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
                    bronze_col_order=CDR_BRONZE_COL_ORDER,
                    raw_table_fqn="iceberg.silver.cdr_quarantine_raw",
                    raw_columns_sql=CDR_RAW_INSERT_SQL,
                    audit_table_fqn="iceberg.silver.cdr_quarantine_audit",
                    logger=logger,
                    log_label="cdr",
                )
            finally:
                conn_q.close()
            df.drop(index=q_idx, inplace=True)

        initial_rows = len(df)
        df.drop_duplicates(subset=["phone_number"], keep="last", inplace=True)
        final_rows = len(df)
        logger.info(
            "Deduplicated CDR: %s rows in, %s rows out (removed %s)",
            initial_rows,
            final_rows,
            initial_rows - final_rows,
        )

        # Cria a pasta 'temp' se não existir
        os.makedirs("temp", exist_ok=True)
        # Salva o DataFrame limpo como Parquet e faz upload para o staging
        df.to_parquet("temp/clean_cdr.parquet", engine="pyarrow", index=False)
        s3.upload_file("temp/clean_cdr.parquet", "warehouse", "staging/cdr/data.parquet")
        logger.info("Uploaded staging parquet; loading Iceberg silver.cdr_customers (full replace)")

        conn = trino.dbapi.connect(
            host="host.docker.internal", port=8080, user="flyte", catalog="iceberg"
        )
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hive.staging.temp_cdr (
                phone_number VARCHAR, account_length INTEGER, vmail_message INTEGER,
                day_mins DOUBLE, day_calls INTEGER, day_charge DOUBLE,
                eve_mins DOUBLE, eve_calls INTEGER, eve_charge DOUBLE,
                night_mins DOUBLE, night_calls INTEGER, night_charge DOUBLE,
                intl_mins DOUBLE, intl_calls INTEGER, intl_charge DOUBLE,
                custserv_calls INTEGER, churn BOOLEAN
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/cdr/')
        """
        )
        cur.fetchall()

        replace_table_transaction(
            conn,
            table_fqn="iceberg.silver.cdr_customers",
            insert_sql="""
            INSERT INTO iceberg.silver.cdr_customers (
                silver_row_id, phone_number, account_length, vmail_message, day_mins, day_calls,
                day_charge, eve_mins, eve_calls, eve_charge, night_mins, night_calls, night_charge,
                intl_mins, intl_calls, intl_charge, custserv_calls, churn
            )
            SELECT
                (SELECT COALESCE(MAX(silver_row_id), CAST(0 AS BIGINT)) FROM iceberg.silver.cdr_customers)
                    + ROW_NUMBER() OVER (ORDER BY phone_number),
                phone_number, account_length, vmail_message, day_mins, day_calls, day_charge,
                eve_mins, eve_calls, eve_charge, night_mins, night_calls, night_charge,
                intl_mins, intl_calls, intl_charge, custserv_calls, churn
            FROM hive.staging.temp_cdr
            """,
            logger=logger,
        )

        cur.execute("DROP TABLE hive.staging.temp_cdr")
        cur.fetchall()

        logger.info("CDR silver load finished successfully")
        return "Successfully loaded CDR to Silver!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}") from e
