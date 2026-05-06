import pandas as pd
import boto3
import trino
from flytekit import task

@task
def process_call_tests_to_silver(minio_access: str, minio_secret: str) -> str:
    # 1. Connect to MinIO
    s3 = boto3.client('s3', endpoint_url='http://host.docker.internal:9000',
                      aws_access_key_id=minio_access, aws_secret_access_key=minio_secret)
    
    # 2. Download Bronze CSV
    s3.download_file('warehouse', 'bronze/Bronze_Call_Tests.csv', '/tmp/raw_tests.csv')
    
    # 3. Clean with Pandas (Fixing European commas as per your project plan)
    df = pd.read_csv('/tmp/raw_tests.csv', sep=';', decimal=',')
    
    # Standardize column names (lowercase, underscores)
    df.columns = df.columns.str.strip().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.lower()
    df.rename(columns={'phone_number': 'phone_number'}, inplace=True) 
    
    # 4. Save as Parquet and upload to a temporary Staging folder
    df.to_parquet('/tmp/clean_tests.parquet', index=False)
    s3.upload_file('/tmp/clean_tests.parquet', 'warehouse', 'staging/call_tests/data.parquet')
    
    # 5. Tell Trino to move the data from Staging into the Iceberg Silver Table
    conn = trino.dbapi.connect(host='host.docker.internal', port=8080, user='flyte', catalog='iceberg')
    cur = conn.cursor()
    
    # Map the staging parquet file
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hive.staging.temp_tests (
            date_of_test TIMESTAMP(6), signal_dbm DOUBLE, speed_m_s DOUBLE, 
            distance_from_site_m DOUBLE, call_test_duration_s DOUBLE, call_test_result VARCHAR, 
            call_test_technology VARCHAR, call_test_setup_time_s DOUBLE, mos DOUBLE, phone_number VARCHAR
        ) WITH (format = 'PARQUET', external_location = 's3a://warehouse/staging/call_tests/')
    """)
    cur.fetchall()
    
    # Insert into Silver and cleanup
    cur.execute("CREATE TABLE IF NOT EXISTS iceberg.silver.call_tests AS SELECT * FROM hive.staging.temp_tests")
    cur.fetchall()
    cur.execute("DROP TABLE hive.staging.temp_tests")
    cur.fetchall()
    
    return "Call Tests successfully processed to Silver!"