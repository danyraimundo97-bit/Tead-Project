import pandas as pd
import boto3
import trino
from botocore.exceptions import ClientError
from flytekit import task, ImageSpec

from flyte_task_env import TASK_ENV
from loki_logging import get_logger

logger = get_logger(__name__)

medallion_image = ImageSpec(
    name="jdpt_lakehouse_env",
    packages=["pandas", "pyarrow", "boto3", "trino","python-logging-loki"],
    registry="localhost:30000"
)

@task(container_image=medallion_image, environment=TASK_ENV)
def process_towers_to_silver(minio_access: str, minio_secret: str) -> str:
    try:
        logger.info("🟢 Starting towers bronze -> silver")
        s3 = boto3.client('s3', endpoint_url='http://host.docker.internal:9000',
                          aws_access_key_id=minio_access, aws_secret_access_key=minio_secret)
        
        logger.info("⏳ Downloading bronze/towers.csv from MinIO...")
        s3.download_file('warehouse', 'bronze/towers.csv', '/tmp/raw_towers.csv')
        
        df = pd.read_csv('/tmp/raw_towers.csv')
        
        # --- THE FIX: RENAME COLUMNS TO MATCH TRINO EXACTLY ---
        df.rename(columns={
            'range': 'range_m',
            'averageSignal': 'average_signal',
            'Snapshot_Date': 'snapshot_date',
            'Status': 'status'
        }, inplace=True)
        # Ensure everything is perfectly lowercase just to be safe
        df.columns = df.columns.str.lower()
        
        initial_count = len(df)
        leiria_towers = df[
            (df['lat'] >= 39.5) & (df['lat'] <= 39.9) &
            (df['lon'] >= -9.0) & (df['lon'] <= -8.6)
        ]
        final_count = len(leiria_towers)
        logger.info(f"📍 Geographic filter: {initial_count} national towers -> {final_count} in Leiria")
        
        leiria_towers.to_parquet('/tmp/clean_towers.parquet', engine='pyarrow', index=False)
        s3.upload_file('/tmp/clean_towers.parquet', 'warehouse', 'staging/towers/data.parquet')
        
        logger.info("⏳ Loading into Iceberg via Trino...")
        conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS hive.staging.temp_towers (
                radio VARCHAR, mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER, 
                unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER, 
                changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE, 
                snapshot_date VARCHAR, status VARCHAR
            ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/towers/')
        """)
        cur.fetchall()

        logger.info("⏳ Ensuring Iceberg schema and table exist...")
        cur.execute("CREATE SCHEMA IF NOT EXISTS iceberg.silver")
        cur.fetchall()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.silver.towers (
                radio VARCHAR, mcc INTEGER, net INTEGER, area INTEGER, cell INTEGER, 
                unit BIGINT, lon DOUBLE, lat DOUBLE, range_m INTEGER, samples INTEGER, 
                changeable INTEGER, created VARCHAR, updated VARCHAR, average_signal DOUBLE, 
                snapshot_date VARCHAR, status VARCHAR
            )
        """)
        cur.fetchall()
        
        cur.execute("DELETE FROM iceberg.silver.towers")
        cur.fetchall()
        
        cur.execute("INSERT INTO iceberg.silver.towers SELECT * FROM hive.staging.temp_towers")
        cur.fetchall()
        
        cur.execute("DROP TABLE hive.staging.temp_towers")
        cur.fetchall()
        
        logger.info(f"✅ Towers silver load finished ({final_count} rows)")
        return f"Successfully loaded {final_count} Leiria Towers to Silver!"

    except Exception as e:
        logger.error(f"❌ TASK FAILED: {str(e)}")
        raise Exception(f"Captured Task Error: {str(e)}")