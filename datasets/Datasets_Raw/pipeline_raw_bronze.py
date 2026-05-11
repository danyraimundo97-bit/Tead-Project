"""
Pipeline (Raw -> Bronze)
Objetivo: Transformar os datasets originais num cenário "Bronze"
          localizado em Leiria, durante a tempestade Kristin (Jan 2026),
          e guardar com particionamento diário (day=YYYY-MM-DD) para a camada Bronze no MinIO.
"""

import pandas as pd
import numpy as np
import warnings

# Ignorar os avisos do Pandas
warnings.filterwarnings('ignore')

# Configuração para o Pandas comunicar diretamente com o MinIO local
MINIO_OPTIONS = {
    "key": "minioadmin",
    "secret": "minioadmin",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"} 
}

def gerar_camada_bronze_diaria():
    print("=== GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) ===")
    
    # Carregar CSVs Originais a partir da pasta RAW do MinIO
    print("A descarregar os dados RAW do MinIO...")
    df_logs = pd.read_csv('s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_cdr = pd.read_csv('s3://warehouse/Dados_Raw/CDR-Call-Details.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_call = pd.read_csv('s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv', sep=';', storage_options=MINIO_OPTIONS)
    df_towers = pd.read_csv('s3://warehouse/Dados_Raw/opencellid_pt.csv', sep=',', storage_options=MINIO_OPTIONS)

    print("=== A APLICAR A SIMULACÂO DA TEMPESTADE EM LEIRIA) ===")
    
    # Translação Geográfica (Leiria)
    print("A aplicar Translação Geográfica e Temporal...")
    lat_offset = 39.7436 - 18.11
    lon_offset = -8.8071 - 83.40
    df_logs['Latitude'] = df_logs['Latitude'] + lat_offset
    df_logs['Longitude'] = df_logs['Longitude'] + lon_offset

    # Filtrar só as torres de Leiria
    print("A filtrar as torres de Leiria...")
    df_towers_leiria = df_towers[
        (df_towers['lat'] >= 39.5) & (df_towers['lat'] <= 39.9) &
        (df_towers['lon'] >= -9.0) & (df_towers['lon'] <= -8.6)
    ].copy()

    # =====================================================================
    # TRANSLAÇÃO TEMPORAL (Trasladar para Janeiro de 2026)
    # =====================================================================
    print("A comprimir e ajustar a dimensão temporal...")
    df_logs['Timestamp'] = pd.to_datetime(df_logs['Timestamp'])
    data_inicio_alvo = pd.to_datetime('2026-01-20 00:00:00')
    df_logs['Timestamp'] = df_logs['Timestamp'] + (data_inicio_alvo - df_logs['Timestamp'].min())

    # Multiplicar os Logs de 3 para 15 dias (Data Augmentation)
    logs_expandidos = [df_logs.copy()]
    for i in range(1, 6): # Vamos até 5 saltos para garantir que cobrimos o início de Fevereiro
        df_copy = df_logs.copy()
        df_copy['Timestamp'] = df_copy['Timestamp'] + pd.Timedelta(days=3 * i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)

    # Cortar tudo o que passe do dia 04 de Fevereiro
    data_fim_alvo = pd.to_datetime('2026-02-04 23:59:59')
    df_logs = df_logs[df_logs['Timestamp'] <= data_fim_alvo]

    # Compressão do tempo nos Call Tests (de 119 dias para 16 dias)
    # Gera um número aleatório de segundos (até 16 dias) e soma a 20 de Janeiro
    segundos_em_16_dias = 16 * 24 * 60 * 60
    random_deltas = pd.to_timedelta(np.random.randint(0, segundos_em_16_dias, size=len(df_call)), unit='s')
    df_call['Date Of Test'] = pd.to_datetime('2026-01-20 00:00:00') + random_deltas

    # =====================================================================
    # CRUZAMENTO DE IDENTIDADES (PHONE NUMBERS)
    # =====================================================================
    print("A criar clientes afetados com chaves cruzadas...")
    np.random.seed(42)
    # Procurar números de telefone únicos que existem na faturação (CDR)
    unique_phones = df_cdr['Phone Number'].dropna().unique()
    
    # Escolher aleatoriamente 2500 clientes para Leiria
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    
    # Atribuir os clientes aos Logs de Rede
    df_logs['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_logs))
    
    # Atribuir os mesmos números aos Call Tests
    df_call['Phone_Number'] = np.random.choice(clientes_leiria, size=len(df_call))

    # =====================================================================
    # HISTÓRICO DE TORRES
    # =====================================================================
    print("A criar snapshots diários do inventário de Antenas...")
    dias_simulacao = pd.date_range(start='2026-01-20', end='2026-02-04', freq='D')
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

    # =====================================================================
    # INJEÇÃO DA TEMPESTADE (28 a 30 de Janeiro)
    # =====================================================================
    print("A injetar o desastre de Leiria...")
    inicio_tempestade = pd.to_datetime('2026-01-28 00:00:00')
    fim_tempestade = pd.to_datetime('2026-01-30 23:59:59')

    # DEGRADAR O SINAL DE RÁDIO (Logs) E CONGESTIONAMENTO
    cond_log_tempestade_Leste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] > -8.80)
    cond_log_tempestade_Oeste = (df_logs['Timestamp'] >= inicio_tempestade) & (df_logs['Timestamp'] <= fim_tempestade) & (df_logs['Longitude'] <= -8.80)
    
    # Degradar Logs de Rede (RSRP)
    def piorar_rsrp_string(val):
        if isinstance(val, str) and 'dBm' in val:
            return f"{int(val.replace(' dBm', '').strip()) - 35} dBm"
        return val

    # Degradar Logs de Rede (RSRQ)
    def piorar_rsrq_string(val):
        if isinstance(val, str) and 'dB' in val:
            return f"{int(val.replace(' dB', '').strip()) - 15} dB"
        return val

    df_logs.loc[cond_log_tempestade_Leste, 'RSRP'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRP'].apply(piorar_rsrp_string)
    df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'] = df_logs.loc[cond_log_tempestade_Leste, 'RSRQ'].apply(piorar_rsrq_string)
    
    # SINR Negativo (Muito ruído)
    df_logs.loc[cond_log_tempestade_Leste, 'SINR'] = np.random.uniform(-15.0, -5.0, size=cond_log_tempestade_Leste.sum()).astype(str)
    df_logs.loc[cond_log_tempestade_Leste, 'Velocity(km/h)'] = "0.0 km/h" 

    # Fallback de Tecnologia: Telemóveis agarram-se a antenas 2G/3G distantes sobreviventes
    fallback_techs = np.random.choice(['UMTS', 'GSM'], size=cond_log_tempestade_Leste.sum())
    df_logs.loc[cond_log_tempestade_Leste, 'NetworkType'] = fallback_techs.astype(str)

    # Oeste sente apenas lentidão (Congestionamento)
    df_logs.loc[cond_log_tempestade_Oeste, 'Downlink(Mbps)'] = "1.5 Mbps" 
    df_logs.loc[cond_log_tempestade_Oeste, 'Uplink(Mbps)'] = "0.2 Mbps" 

    # Descobrimos QUAIS OS NÚMEROS DE TELEFONE exatos que estavam no Leste durante a tempestade
    clientes_Leste = df_logs.loc[cond_log_tempestade_Leste, 'Phone_Number'].unique()

    # QUEDAS DE CHAMADA APENAS PARA NÚMEROS DE TELEFONE QUE ESTAVAM NO LESTE DURANTE A TEMPESTADE
    cond_call_tempestade = (df_call['Date Of Test'] >= inicio_tempestade) & (df_call['Date Of Test'] <= fim_tempestade) & (df_call['Phone_Number'].isin(clientes_Leste))
    
    df_call.loc[cond_call_tempestade, 'Call Test Result'] = 'DROP'
    df_call.loc[cond_call_tempestade, 'MOS'] = np.random.uniform(1.0, 1.8, size=cond_call_tempestade.sum()).astype(str)
    df_call['MOS'] = df_call['MOS'].astype(str).str.replace('.', ',')

    # Aumento do tempo de ligação (congestionamento) e distância (ligados a antenas distantes)
    df_call.loc[cond_call_tempestade, 'Call Test Duration (s)'] = np.random.uniform(2.0, 12.0, size=cond_call_tempestade.sum()).astype(str)
    df_call['Call Test Duration (s)'] = df_call['Call Test Duration (s)'].astype(str).str.replace('.', ',')

    df_call.loc[cond_call_tempestade, 'Call Test Setup Time (s)'] = np.random.uniform(15.0, 45.0, size=cond_call_tempestade.sum()).astype(str)
    df_call['Call Test Setup Time (s)'] = df_call['Call Test Setup Time (s)'].astype(str).str.replace('.', ',')
    
    df_call.loc[cond_call_tempestade, 'Distance from site (m)'] = np.random.uniform(5000, 15000, size=cond_call_tempestade.sum()).astype(str)
    df_call['Distance from site (m)'] = df_call['Distance from site (m)'].astype(str).str.replace('.', ',')

    # CAOS NO SUPORTE E CHURN APENAS PARA ESSES NÚMEROS (Na Tabela de Faturação)
    idx_cdr_afetados = df_cdr['Phone Number'].isin(clientes_Leste)
    df_cdr.loc[idx_cdr_afetados, 'CustServ Calls'] += np.random.randint(3, 8, size=idx_cdr_afetados.sum())
    df_cdr.loc[idx_cdr_afetados, 'Churn'] = np.random.choice([True, False], p=[0.85, 0.15], size=idx_cdr_afetados.sum())

    # Remover a coluna original inútil de DeviceID
    if 'DeviceID' in df_logs.columns:
        df_logs = df_logs.drop(columns=['DeviceID'])

    # Metadados de Ingestão (Audit Trail para o Lakehouse)
    timestamp_ingestao = pd.Timestamp.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    df_logs['_ingested_at'] = timestamp_ingestao
    df_call['_ingested_at'] = timestamp_ingestao
    df_towers_leiria['_ingested_at'] = timestamp_ingestao
    df_cdr['_ingested_at'] = timestamp_ingestao


    # =====================================================================
    # PARTICIONAMENTO DIÁRIO NO MINIO (LAKEHOUSE)
    # =====================================================================
    print("=== PARTICIONAR OS DADOS E ENVIAR PARA A CAMADA BRONZE ===")

    # Particionar Network Logs
    df_logs['Data_Fatia'] = pd.to_datetime(df_logs['Timestamp']).dt.strftime('%Y-%m-%d')
    datas_logs = df_logs['Data_Fatia'].dropna().unique()
    for dia in datas_logs:
        df_dia = df_logs[df_logs['Data_Fatia'] == dia].drop(columns=['Data_Fatia'])
        caminho = f"s3://warehouse/bronze/network_logs/day={dia}/data.csv"
        df_dia.to_csv(caminho, index=False, sep=';', storage_options=MINIO_OPTIONS)
    print(f" -> Network Logs particionados em {len(datas_logs)} dias.")

    # Particionar Call Tests
    df_call['Data_Fatia'] = pd.to_datetime(df_call['Date Of Test']).dt.strftime('%Y-%m-%d')
    datas_call = df_call['Data_Fatia'].dropna().unique()
    for dia in datas_call:
        df_dia = df_call[df_call['Data_Fatia'] == dia].drop(columns=['Data_Fatia'])
        caminho = f"s3://warehouse/bronze/call_tests/day={dia}/data.csv"
        df_dia.to_csv(caminho, index=False, sep=';', storage_options=MINIO_OPTIONS)
    print(f" -> Call Tests particionados em {len(datas_call)} dias.")

    # Particionar Towers
    datas_towers = df_towers_leiria['Snapshot_Date'].dropna().unique()
    for dia in datas_towers:
        df_dia = df_towers_leiria[df_towers_leiria['Snapshot_Date'] == dia]
        caminho = f"s3://warehouse/bronze/towers/day={dia}/data.csv"
        df_dia.to_csv(caminho, index=False, sep=',', storage_options=MINIO_OPTIONS)
    print(f" -> Towers particionadas em {len(datas_towers)} dias.")

    # Particionar CDR (Faturação) - Repetido diariamente para corresponder aos snapshots
    datas_gerais = pd.date_range(start='2026-01-20', end='2026-02-04').strftime('%Y-%m-%d')
    for dia in datas_gerais:
        caminho = f"s3://warehouse/bronze/cdr_customers/day={dia}/data.csv"
        df_cdr.to_csv(caminho, index=False, sep=';', storage_options=MINIO_OPTIONS)
    print(f" -> CDR (Faturação) replicado em {len(datas_gerais)} dias.")

    print("\n✅ Simulação Completa. A Camada Bronze está particionada no MinIO.")

if __name__ == "__main__":
    gerar_camada_bronze_diaria()