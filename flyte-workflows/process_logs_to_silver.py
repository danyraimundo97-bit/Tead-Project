import pandas as pd
import boto3
import trino
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino","python-logging-loki"],
    registry="localhost:30000" # This pushes it to your local Sandbox registry
)

@task(container_image=medallion_image, environment=TASK_ENV)
def process_logs_to_silver(minio_access: str, minio_secret: str, target_date_str: str) -> str:
    """Cleans Network Logs, applies GPS offset, and uploads partitioned by Day."""
    try:
        logger.info("Starting network logs bronze -> silver for date=%s", target_date_str)
        s3 = boto3.client('s3', endpoint_url='http://host.docker.internal:9000',
                          aws_access_key_id=minio_access, aws_secret_access_key=minio_secret)

        logger.info("Downloading bronze/network_logs.csv from MinIO")
        s3.download_file('warehouse', 'bronze/network_logs.csv', '/tmp/raw_logs.csv')

        # Read CSV
        df = pd.read_csv('/tmp/raw_logs.csv', sep=';')

        # Standardize columns
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()

        logger.info("🔧 Cleaning decimal formatting and enforcing numeric types...")
        numeric_cols = [
            'rsrp', 'rsrq', 'sinr', 'pci', 'downlink_mbps', 
            'uplink_mbps', 'velocity_kmh', 'latitude', 'longitude'
        ]
        
        total_rows = len(df)
        # in the saphety check, i want to check if one of the numeric columns does not match the with the df.columns, i want raise an error and stop the pipeline, because it means the source data has changed and our cleaning logic might be broken. So we want to catch that early before we do any damage.  
        for col in numeric_cols:
            if col not in df.columns:
                error_msg = f"❌ DATA STRUCTURE CHANGE: Expected column '{col}' not found in source data! Pipeline halted to prevent corruption."
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            if col in df.columns:
                # 1. Save the "Before" state (handling any raw blanks)
                raw_values = df[col].replace(['', '-', ' '], None).copy()
                
                # 2. Perform the aggressive cleaning
                clean_col = df[col].astype(str).str.replace(',', '.')
                clean_col = clean_col.str.replace(r'[a-zA-Z\s]', '', regex=True)
                clean_col = clean_col.replace(['', '-'], None)
                cleaned_values = pd.to_numeric(clean_col, errors='coerce')
                
                # 3. THE AUDIT: Find rows that HAD data, but now HAVE NO data
                # (Meaning our regex/conversion destroyed it)
                destroyed_mask = raw_values.notna() & cleaned_values.isna()
                destroyed_count = destroyed_mask.sum()
                logger.debug(f"Column '{col}': {destroyed_count} values destroyed out of {total_rows} total rows.")

                if destroyed_count > 0:
                    # Grab a sample of the exact values that got wiped out
                    sample_wiped = raw_values[destroyed_mask].head(5).tolist()
                    logger.warning(f"⚠️ AUDIT WARNING: Column '{col}' had {destroyed_count} values destroyed during cleaning.")
                    logger.warning(f"   -> Example destroyed values: {sample_wiped}")
                    
                    # 4. THE CIRCUIT BREAKER: If more than 5% of data is wiped, crash the pipeline!
                    failure_rate = destroyed_count / total_rows
                    if failure_rate > 0.05:
                        error_msg = f"❌ DATA QUALITY BREACH: '{col}' lost {failure_rate*100:.1f}% of its data! Pipeline halted to prevent corruption."
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                
                # If we pass the audit, officially apply the cleaned data to the dataframe
                df[col] = cleaned_values

        df.rename(columns={
            'phone_number': 'phone_number',
            'network_provi.': 'network_provider',
            'networktype': 'network_type'
        }, inplace=True)

        # Drop rows missing GPS
        before_gps = len(df)
        df.dropna(subset=['latitude', 'longitude'], inplace=True)
        logger.info("Dropped %s rows with missing lat/lon (%s rows remain)", before_gps - len(df), len(df))

        # Apply Leiria Geographic Translation
        df['latitude'] = df['latitude'] + 21.63
        df['longitude'] = df['longitude'] - 92.20

        # Filter by the target date
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        daily_df = df[df['timestamp'].dt.strftime('%Y-%m-%d') == target_date_str]

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

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_logs_{safe_date} (
                timestamp_log TIMESTAMP(3), devicemake VARCHAR, devicemodel VARCHAR,
                network_provider VARCHAR, network_type VARCHAR, rsrp DOUBLE,
                rsrq DOUBLE, sinr DOUBLE, pci DOUBLE, downlink_mbps DOUBLE,
                uplink_mbps DOUBLE, velocity_kmh DOUBLE, latitude DOUBLE,
                longitude DOUBLE, phone_number VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/{staging_key.replace('/data.parquet', '')}/')
        """)
        cur.fetchall()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_towers (
                radio VARCHAR, mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER,
                unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER,
                changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE,
                snapshot_date VARCHAR, status VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/towers/')
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