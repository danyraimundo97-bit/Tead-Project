import boto3
import os

# 1. Configure the S3 client to point to your local MinIO
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000', # MinIO API port
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    region_name='us-east-1' # Required by boto3, even for local
)

bucket_name = 'warehouse'

# Using plural variable names to match the loop below
local_file_paths = (
    './datasets/Datasets_Bronze_Leiria/Bronze_Call_Tests.csv', 
    './datasets/Datasets_Bronze_Leiria/Bronze_CDR_Customers.csv', 
    './datasets/Datasets_Bronze_Leiria/Bronze_Network_Logs_Leiria.csv', 
    './datasets/Datasets_Bronze_Leiria/Bronze_Towers_Leiria.csv'
)

minio_destination_paths = (
    'bronze/call_tests.csv', 
    'bronze/cdr_customers.csv', 
    'bronze/network_logs.csv', 
    'bronze/towers.csv'
)

# 2. Upload the files
for local_file, minio_path in zip(local_file_paths, minio_destination_paths):
    print(f"Uploading {local_file} to {bucket_name}/{minio_path}...")
    s3.upload_file(local_file, bucket_name, minio_path)

print("Upload complete!")