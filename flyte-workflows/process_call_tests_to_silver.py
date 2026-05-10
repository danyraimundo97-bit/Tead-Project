import pandas as pd
import trino
from botocore.exceptions import ClientError
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV, minio_s3_client
from loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino", "python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=medallion_image, environment=TASK_ENV)
def process_call_tests_to_silver(target_date_str: str) -> str:
    try:
        logger.info(f"🟢 Starting call tests bronze -> silver for date={target_date_str}")
        s3 = minio_s3_client()
        
        # Download from Bronze
        logger.info("⏳ Downloading bronze/call_tests.csv from MinIO...")
        s3.download_file('warehouse', 'bronze/call_tests.csv', '/tmp/raw_tests.csv')
        
        # Read CSV
        df = pd.read_csv('/tmp/raw_tests.csv', sep=';', decimal=',')
        
        # Standardize Columns
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('/', '_').str.lower()
        df.rename(columns={'phone_number': 'phone_number'}, inplace=True)
        
        logger.info("🔧 Cleaning decimal formatting and enforcing numeric types...")
        numeric_cols = [
            'signal_dbm', 'speed_m_s', 'distance_from_site_m', 
            'call_test_duration_s', 'call_test_setup_time_s', 'mos'
        ]
        
        for col in numeric_cols:
            if col in df.columns:
                if df[col].dtype == 'object':
                    # Replace commas with dots and force it to be a float number
                    df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
                else:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

        df['date_of_test'] = pd.to_datetime(df['date_of_test'], errors='coerce')
        daily_df = df[df['date_of_test'].dt.strftime('%Y-%m-%d') == target_date_str].copy()
        
        if daily_df.empty:
            logger.warning(f"No call tests for date={target_date_str}; skipping")
            return f"No Call Tests found for {target_date_str}. Skipping."
        
        # Upload to Staging
        staging_key = f'staging/call_tests/day={target_date_str}/data.parquet'
        daily_df.to_parquet('/tmp/clean_tests.parquet', engine='pyarrow', index=False)
        s3.upload_file('/tmp/clean_tests.parquet', 'warehouse', staging_key)
        
        # Load to Iceberg via Trino
        conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()

        safe_date = target_date_str.replace('-','')
        temp_location = staging_key.replace('/data.parquet', '')
        
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_tests_{safe_date} (
                date_of_test TIMESTAMP(3), signal_dbm DOUBLE, speed_m_s DOUBLE, 
                distance_from_site_m DOUBLE, call_test_duration_s DOUBLE, call_test_result VARCHAR, 
                call_test_technology VARCHAR, call_test_setup_time_s DOUBLE, mos DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """)
        cur.fetchall()

        logger.info(f"🧹 Limpar dados antigos do dia {target_date_str} para evitar duplicados...")
        cur.execute(f"DELETE FROM iceberg.silver.call_tests WHERE CAST(date_of_test AS DATE) = DATE '{target_date_str}'")
        cur.fetchall()
        
        cur.execute(f"""
            INSERT INTO iceberg.silver.call_tests 
            SELECT * FROM hive.staging.temp_tests_{safe_date}
        """)
        cur.fetchall()
        
        cur.execute(f"DROP TABLE hive.staging.temp_tests_{safe_date}")
        cur.fetchall()
        
        logger.info(f"✅ Call tests silver load finished for date={target_date_str}")
        return f"Successfully loaded Call Tests for {target_date_str}!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")