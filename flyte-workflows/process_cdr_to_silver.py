import pandas as pd
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino","python-logging-loki"],
    registry="localhost:30000" # This pushes it to your local Sandbox registry
)

@task(container_image=medallion_image, environment=TASK_ENV)
def process_cdr_to_silver() -> str:
    """Downloads Bronze CDR, deduplicates it, and uploads to Silver Iceberg."""
    try:
        logger.info("Starting CDR bronze -> silver")
        s3 = minio_s3_client()

        # Download from Bronze
        logger.info("Downloading bronze/cdr_customers.csv from MinIO")
        s3.download_file('warehouse', 'bronze/cdr_customers.csv', '/tmp/raw_cdr.csv')

        # Read CSV
        df = pd.read_csv('/tmp/raw_cdr.csv', sep=';')
        initial_rows = len(df)

        # Standardize Columns
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        df.rename(columns={'phone_number': 'phone_number'}, inplace=True)

        # Deduplicate (Project Requirement)
        df.drop_duplicates(inplace=True)
        final_rows = len(df)
        logger.info("Deduplicated CDR: %s rows in, %s rows out (removed %s)", initial_rows, final_rows, initial_rows - final_rows)

        # Upload to Staging
        df.to_parquet('/tmp/clean_cdr.parquet', engine='pyarrow', index=False)
        s3.upload_file('/tmp/clean_cdr.parquet', 'warehouse', 'staging/cdr/data.parquet')
        logger.info("Uploaded staging parquet; loading Iceberg silver.cdr_customers (full replace)")

        # Load to Iceberg via Trino (Full Replace since it's a dimension table)
        conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_cdr (
                phone_number VARCHAR, account_length INTEGER, vmail_message INTEGER,
                day_mins DOUBLE, day_calls INTEGER, day_charge DOUBLE,
                eve_mins DOUBLE, eve_calls INTEGER, eve_charge DOUBLE,
                night_mins DOUBLE, night_calls INTEGER, night_charge DOUBLE,
                intl_mins DOUBLE, intl_calls INTEGER, intl_charge DOUBLE,
                custserv_calls INTEGER, churn BOOLEAN
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/cdr/')
        """)
        cur.fetchall()

        logger.info("⏳ Ensuring Iceberg schema and table exist for CDR...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
        cur.fetchall()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.silver.cdr_customers (
                phone_number VARCHAR, account_length INTEGER, vmail_message INTEGER,
                day_mins DOUBLE, day_calls INTEGER, day_charge DOUBLE,
                eve_mins DOUBLE, eve_calls INTEGER, eve_charge DOUBLE,
                night_mins DOUBLE, night_calls INTEGER, night_charge DOUBLE,
                intl_mins DOUBLE, intl_calls INTEGER, intl_charge DOUBLE,
                custserv_calls INTEGER, churn BOOLEAN
            )
        """)
        cur.fetchall()

        cur.execute("TRUNCATE TABLE iceberg.silver.cdr_customers")
        cur.fetchall()

        cur.execute("INSERT INTO iceberg.silver.cdr_customers SELECT * FROM hive.staging.temp_cdr")
        cur.fetchall()

        cur.execute("DROP TABLE hive.staging.temp_cdr")
        cur.fetchall()

        logger.info("CDR silver load finished successfully")
        return "Successfully loaded CDR to Silver!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")