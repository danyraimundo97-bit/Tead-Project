import logging
import pandas as pd
import numpy as np
from flytekit import task, workflow

logger = logging.getLogger(__name__)
if not logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter("%(levelname)s [%(name)s] %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)
    logger.propagate = False

# Configuração mágica para o Pandas comunicar diretamente com o MinIO local
MINIO_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"} #http://host.docker.internal:9000
}

@task
def gerar_camada_bronze() -> str:
    logger.info("Fase 1: leitura dos dados raw do MinIO")
    
    # Em vez de ler do disco (C:\...), lemos diretamente do Data Lake (s3://...)
    # Certifique-se que o nome dos ficheiros na pasta Dados_Raw corresponde a estes!
    df_logs = pd.read_csv('s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_cdr = pd.read_csv('s3://warehouse/Dados_Raw/CDR-Call-Details.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_call = pd.read_csv('s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_towers = pd.read_csv('s3://warehouse/Dados_Raw/opencellid_pt.csv', sep=',', storage_options=MINIO_OPTIONS)
    logger.info(
        "Raw carregado: logs=%s cdr=%s call_tests=%s towers=%s",
        len(df_logs), len(df_cdr), len(df_call), len(df_towers),
    )

    logger.info("Fase 2: transformação e simulação (Leiria)")
    # Translação Geográfica (Leiria)
    lat_offset = 39.7436 - 18.11
    lon_offset = -8.8071 - 83.40
    df_logs['Latitude'] = df_logs['Latitude'] + lat_offset
    df_logs['Longitude'] = df_logs['Longitude'] + lon_offset

    df_towers_leiria = df_towers[
        (df_towers['lat'] >= 39.5) & (df_towers['lat'] <= 39.9) &
        (df_towers['lon'] >= -9.0) & (df_towers['lon'] <= -8.6)
    ].copy()

    # Translação Temporal
    df_logs['Timestamp'] = pd.to_datetime(df_logs['Timestamp'])
    df_call['Date Of Test'] = pd.to_datetime(df_call['Date Of Test'], dayfirst=True, errors='coerce')

    data_inicio_alvo = pd.to_datetime('2026-01-20 00:00:00')
    df_logs['Timestamp'] = df_logs['Timestamp'] + (data_inicio_alvo - df_logs['Timestamp'].min())
    df_call['Date Of Test'] = df_call['Date Of Test'] + (data_inicio_alvo - df_call['Date Of Test'].min())

    # Multiplicar os Logs de 3 dias para 15 dias (Data Augmentation)
    logs_expandidos = [df_logs.copy()]
    for i in range(1, 5):
        df_copy = df_logs.copy()
        df_copy['Timestamp'] = df_copy['Timestamp'] + pd.Timedelta(days=3.3 * i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)

    # Garantir o Cruzamento
    logger.info("A atribuir phone numbers Leiria para logs e call tests")
    np.random.seed(42)
    unique_phones = df_cdr['Phone Number'].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    
    df_logs['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_logs))
    df_call['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_call))

    # Histórico Diário das Antenas
    dias_simulacao = pd.date_range(start='2026-01-20', end='2026-02-04', freq='D')
    logger.info("A criar snapshots diários do inventário de antenas (%s dias)", len(dias_simulacao))
    snapshots_torres = []
    
    np.random.seed(42)
    torres_Leste_indices = df_towers_leiria[df_towers_leiria['lon'] > -8.80].index
    torres_destruidas_indices = np.random.choice(torres_Leste_indices, size=int(len(torres_Leste_indices)*0.80), replace=False)

    for dia in dias_simulacao:
        df_t = df_towers_leiria.copy()
        df_t['Snapshot_Date'] = dia.strftime('%Y-%m-%d')
        df_t['Status'] = 'ACTIVE'
        if dia >= pd.to_datetime('2026-01-28'):
            df_t.loc[torres_destruidas_indices, 'Status'] = 'DOWN'
        snapshots_torres.append(df_t)
    df_towers_leiria = pd.concat(snapshots_torres, ignore_index=True)

    # TEMPESTADE (28 a 30 de Janeiro)
    logger.info("A injetar cenário tempestade (28–30 Jan); torres leste afetadas=%s", len(torres_destruidas_indices))
    inicio_tempestade = pd.to_datetime('2026-01-28 00:00:00')
    fim_tempestade = pd.to_datetime('2026-01-30 23:59:59')

    cond_log_tempestade_Leste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] > -8.80)
    cond_log_tempestade_Oeste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] <= -8.80)
    
    def piorar_rsrp_string(val):
        if isinstance(val, str) and 'dBm' in val:
            return f"{int(val.replace(' dBm', '').strip()) - 35} dBm"
        return val

    def piorar_rsrq_string(val):
        if isinstance(val, str) and 'dB' in val:
            return f"{int(val.replace(' dB', '').strip()) - 15} dB"
        return val

    df_logs.loc[cond_log_tempestade_Leste, 'RSRP'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRP'].apply(piorar_rsrp_string)
    df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'].apply(piorar_rsrq_string)
    df_logs.loc[cond_log_tempestade_Leste, 'SINR'] = np.nan
    df_logs.loc[cond_log_tempestade_Leste, 'Velocity(km/h)'] = "0.0 km/h" 

    df_logs.loc[cond_log_tempestade_Oeste, 'Downlink(Mbps)'] = "1.5 Mbps" 
    df_logs.loc[cond_log_tempestade_Oeste, 'Uplink(Mbps)'] = "0.2 Mbps" 

    clientes_Leste = df_logs.loc[cond_log_tempestade_Leste, 'Phone_Number'].unique()

    cond_call_tempestade = (df_call['Date Of Test'] >= inicio_tempestade) & (df_call['Date Of Test'] <= fim_tempestade) & (df_call['Phone_Number'].isin(clientes_Leste))
    
    df_call.loc[cond_call_tempestade, 'Call Test Result'] = 'DROP'
    df_call.loc[cond_call_tempestade, 'MOS'] = np.random.uniform(1.0, 1.8, size=cond_call_tempestade.sum()).astype(str)
    df_call['MOS'] = df_call['MOS'].astype(str).str.replace('.', ',')
    df_call.loc[cond_call_tempestade, 'Call Test Duration (s)'] = np.random.uniform(2.0, 12.0, size=cond_call_tempestade.sum()).astype(str)
    df_call['Call Test Duration (s)'] = df_call['Call Test Duration (s)'].astype(str).str.replace('.', ',')

    idx_cdr_afetados = df_cdr['Phone Number'].isin(clientes_Leste)
    df_cdr.loc[idx_cdr_afetados, 'CustServ Calls'] += np.random.randint(3, 8, size=idx_cdr_afetados.sum())
    df_cdr.loc[idx_cdr_afetados, 'Churn'] = np.random.choice([True, False], p=[0.85, 0.15], size=idx_cdr_afetados.sum())

    if 'DeviceID' in df_logs.columns:
        df_logs = df_logs.drop(columns=['DeviceID'])

    logger.info("Fase 3: escrita bronze no MinIO (logs=%s rows)", len(df_logs))
    # Gravar diretamente para a pasta Dados_Bronze no MinIO!
    df_logs.to_csv('s3://warehouse/Dados_Bronze/Bronze_Network_Logs_Leiria.csv', index=False, sep=';', storage_options=MINIO_OPTIONS)
    df_cdr.to_csv('s3://warehouse/Dados_Bronze/Bronze_CDR_Customers.csv', index=False, sep=';', storage_options=MINIO_OPTIONS)
    df_call.to_csv('s3://warehouse/Dados_Bronze/Bronze_Call_Tests.csv', index=False, sep=';', storage_options=MINIO_OPTIONS)
    df_towers_leiria.to_csv('s3://warehouse/Dados_Bronze/Bronze_Towers_Leiria.csv', index=False, sep=',', storage_options=MINIO_OPTIONS) 
    logger.info("Bronze gravado no MinIO com sucesso")
    return "Fase 1 (Raw -> Bronze) concluída com sucesso! Os ficheiros estão no MinIO."

@workflow
def pipeline_extracao_bronze() -> str:
    """Workflow que orquestra a geração da camada Bronze."""
    resultado = gerar_camada_bronze()
    return resultado

if __name__ == "__main__":
    # Permite testar o código localmente como um script normal de Python
    logging.basicConfig(level=logging.INFO, format="%(levelname)s [%(name)s] %(message)s")
    print(pipeline_extracao_bronze())