"""
Script de Inicialização: setup_raw_data.py
Objetivo: Fazer o upload dos ficheiros CSV locais (Raw Data) para o MinIO.
Isto resolve o "Cold Start" simulando o envio de ficheiros por entidades externas.
"""

import os
import boto3
from botocore.exceptions import ClientError

def setup_raw_data():
    print("="*50)
    print(" INICIALIZAÇÃO DA LANDING ZONE (DADOS RAW)")
    print("="*50)

    # 1. Configuração de acesso direto ao MinIO local
    s3 = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    BUCKET_NAME = 'warehouse'
    DEST_FOLDER = 'Dados_Raw'

    # Repo root: script lives in python_scripts/, datasets/ is one level up
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    BASE_DIR = os.path.join(repo_root, "datasets", "Datasets_Raw")

    # Os 4 ficheiros vitais do projeto
    FILES_TO_UPLOAD = [
        "Cellular Network Handover Prediction Dataset.csv",
        "CDR-Call-Details.csv",
        "Call Tests Measurements for MOS prediction.csv",
        "opencellid_pt.csv"
    ]

    # 2. Garantir que o Bucket existe
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' já existe.")
    except ClientError:
        print(f"A criar bucket '{BUCKET_NAME}'...")
        s3.create_bucket(Bucket=BUCKET_NAME)

    # 3. Fazer o Upload
    print("\nA iniciar o upload dos ficheiros para o MinIO...")
    todos_sucesso = True

    for file_name in FILES_TO_UPLOAD:
        local_path = os.path.join(BASE_DIR, file_name)
        s3_path = f"{DEST_FOLDER}/{file_name}"
        
        if os.path.exists(local_path):
            print(f" ⏳ A fazer upload de '{file_name}'...")
            try:
                s3.upload_file(local_path, BUCKET_NAME, s3_path)
                print(f"    -> Sucesso: s3://{BUCKET_NAME}/{s3_path}")
            except Exception as e:
                print(f"    -> ERRO no upload: {e}")
                todos_sucesso = False
        else:
            print(f" ❌ ERRO: Ficheiro não encontrado no teu PC: '{local_path}'")
            todos_sucesso = False

    if todos_sucesso:
        print("\n🚀 SETUP CONCLUÍDO! O teu MinIO está pronto para a Ingestão Flyte.")
    else:
        print("\n⚠️ O setup terminou com alguns erros. Verifica os caminhos dos ficheiros.")

if __name__ == "__main__":
    setup_raw_data()