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
def process_logs_to_silver(target_date_str: str) -> str:
    """Cleans Network Logs, applies GPS offset, and uploads partitioned by Day."""
    try:
        logger.info("Starting network logs bronze -> silver for date=%s", target_date_str)
        s3 = minio_s3_client()

        # Download from Bronze
        logger.info("Downloading bronze/network_logs.csv from MinIO")
        s3.download_file('warehouse', 'bronze/network_logs.csv', '/tmp/raw_logs.csv')

        # Read CSV
        df = pd.read_csv('/tmp/raw_logs.csv', sep=';')

        # Standardize columns
        df.columns = df.columns.str.strip().str.lower()
        df.rename(columns={
            'network provi.': 'network_provider',
            'networktype': 'network_type',
            'downlink(mbps)': 'downlink_mbps',
            'uplink(mbps)': 'uplink_mbps',
            'velocity(km/h)': 'velocity_kmh'
        }, inplace=True)

        logger.info("🔧 Cleaning decimal formatting and enforcing numeric types...")
        numeric_cols = [
            'rsrp', 'rsrq', 'sinr', 'pci', 'downlink_mbps', 
            'uplink_mbps', 'velocity_kmh', 'latitude', 'longitude'
        ]
        
        total_rows = len(df)

        # Data Quality Gate
        # in the safety check, i want to check if one of the numeric columns does not match the with the df.columns, i want raise an error and stop the pipeline, because it means the source data has changed and our cleaning logic might be broken. So we want to catch that early before we do any damage.  
        for col in numeric_cols:
            if col not in df.columns:
                error_msg = f"❌ DATA STRUCTURE CHANGE: Expected column '{col}' not found in source data! Pipeline halted to prevent corruption."
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            if col in df.columns:
                # Save the "Before" state (handling any raw blanks)
                raw_values = df[col].replace(['', '-', ' '], None).copy()
                
                # Perform the aggressive cleaning
                clean_col = df[col].astype(str).str.replace(',', '.')
                clean_col = clean_col.str.replace(r'[a-zA-Z\s]', '', regex=True)
                clean_col = clean_col.replace(['', '-'], None)
                cleaned_values = pd.to_numeric(clean_col, errors='coerce')
                
                # Find rows that HAD data, but now HAVE NO data
                # (regex/conversion destroyed it)
                destroyed_mask = raw_values.notna() & cleaned_values.isna()
                destroyed_count = destroyed_mask.sum()
                logger.debug(f"Column '{col}': {destroyed_count} values destroyed out of {total_rows} total rows.")

                if destroyed_count > 0:
                    # Grab a sample of the exact values that got wiped out
                    sample_wiped = raw_values[destroyed_mask].head(5).tolist()
                    logger.warning(f"⚠️ AUDIT WARNING: Column '{col}' had {destroyed_count} values destroyed during cleaning.")
                    logger.warning(f"   -> Example destroyed values: {sample_wiped}")
                    
                    # THE CIRCUIT BREAKER: If more than 5% of data is wiped, crash the pipeline!
                    failure_rate = destroyed_count / total_rows
                    if failure_rate > 0.05:
                        error_msg = f"❌ DATA QUALITY BREACH: '{col}' lost {failure_rate*100:.1f}% of its data! Pipeline halted to prevent corruption."
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                
                # If we pass the audit, officially apply the cleaned data to the dataframe
                df[col] = cleaned_values

        # Drop rows missing GPS
        before_gps = len(df)
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        logger.info("Dropped %s rows with missing lat/lon (%s rows remain)", before_gps - len(df), len(df))

        # Filter by the target date (errors='coerce' por segurança)
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        daily_df = df[df['timestamp'].dt.strftime('%Y-%m-%d') == target_date_str].copy()

        if daily_df.empty:
            logger.warning("No network logs for date=%s; skipping", target_date_str)
            return f"No Network Logs found for {target_date_str}. Skipping."

        logger.info("Filtered network logs to %s rows for date=%s", len(daily_df), target_date_str)
        
        # Upload to Staging
        # We rename 'timestamp' to 'timestamp_log' to match our Trino schema
        daily_df.rename(columns={'timestamp': 'timestamp_log'}, inplace=True)

        staging_key = f'staging/network_logs/day={target_date_str}/data.parquet'
        daily_df.to_parquet('/tmp/clean_logs.parquet', engine='pyarrow', index=False)
        s3.upload_file('/tmp/clean_logs.parquet', 'warehouse', staging_key)
        logger.info("Uploaded staging %s; loading Trino silver.network_logs", staging_key)

        # Load to Iceberg via Trino
        conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()

        safe_date = target_date_str.replace('-','')

        temp_location = staging_key.replace('/data.parquet', '')

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_logs_{safe_date} (
                timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
                network_provider VARCHAR, network_type VARCHAR, rsrp DOUBLE,
                rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
                uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
                longitude DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{temp_location}/')
        """)
        cur.fetchall()

        logger.info("⏳ Ensuring Iceberg schema and table exist for Network Logs...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
        cur.fetchall()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.silver.network_logs (
                timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
                network_provider VARCHAR, network_type VARCHAR, rsrp DOUBLE,
                rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
                uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
                longitude DOUBLE, phone_number VARCHAR
            )
        """)
        cur.fetchall()

        logger.info(f"🧹 Limpar dados antigos do dia {target_date_str} para evitar duplicados...")
        cur.execute(f"DELETE FROM iceberg.silver.network_logs WHERE CAST(timestamp_log AS DATE) = DATE '{target_date_str}'")
        cur.fetchall()

        cur.execute(f"""
            INSERT INTO iceberg.silver.network_logs
            SELECT * FROM hive.staging.temp_logs_{safe_date}
        """)
        cur.fetchall()

        cur.execute(f"DROP TABLE hive.staging.temp_logs_{safe_date}")
        cur.fetchall()

        logger.info("Network logs silver load finished for date=%s", target_date_str)
        return f"Successfully loaded Network Logs for {target_date_str}!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")