"""
Script 1: simular_tempestade.py (Data Generation)
Objetivo: Transformar os datasets originais num cenário "RAW" (Bruto)
          localizado em Leiria, durante a tempestade Kristin (Jan 2026).
"""

import pandas as pd
import numpy as np

def gerar_camada_bronze():
    print("=== FASE 1: GERAÇÃO DO CENÁRIO RAW (CAMADA BRONZE) ===")
    
    # Carregar CSVs Originais
    df_logs = pd.read_csv('Cellular Network Handover Prediction Dataset.csv', sep=';')
    df_cdr = pd.read_csv('CDR-Call-Details.csv', sep=';')
    df_call = pd.read_csv('Call Tests Measurements for MOS prediction.csv', sep=';')
    df_towers = pd.read_csv('opencellid_pt.csv', sep=',')

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
    # =====================================================================
    print("A criar um 'Bairro' de clientes afetados com chaves cruzadas...")
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

    # Histórico Diário das Antenas
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

    # TEMPESTADE (28 a 30 de Janeiro)
    # =====================================================================
    print("A injetar o desastre de Leiria...")
    inicio_tempestade = pd.to_datetime('2026-01-28 00:00:00')
    fim_tempestade = pd.to_datetime('2026-01-30 23:59:59')

    # ESTRAGAR SINAL RÁDIO (Logs) E CONGESTIONAMENTO
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

    # Descobrimos QUAIS SÃO OS NÚMEROS DE TELEFONE exatos que estavam na rua no Leste durante a tempestade
    clientes_Leste = df_logs.loc[cond_log_tempestade_Leste, 'Phone_Number'].unique()

    # QUEDAS DE CHAMADA APENAS PARA ESSES NÚMEROS DE TELEFONE
    cond_call_tempestade = (df_call['Date Of Test'] >= inicio_tempestade) & (df_call['Date Of Test'] <= fim_tempestade) & (df_call['Phone_Number'].isin(clientes_Leste))
    
    df_call.loc[cond_call_tempestade, 'Call Test Result'] = 'DROP'
    df_call.loc[cond_call_tempestade, 'MOS'] = np.random.uniform(1.0, 1.8, size=cond_call_tempestade.sum())
    df_call['MOS'] = df_call['MOS'].astype(str).str.replace('.', ',')
    df_call.loc[cond_call_tempestade, 'Call Test Duration (s)'] = np.random.uniform(2.0, 12.0, size=cond_call_tempestade.sum())
    df_call['Call Test Duration (s)'] = df_call['Call Test Duration (s)'].astype(str).str.replace('.', ',')

    # CAOS NO SUPORTE E CHURN APENAS PARA ESSES NÚMEROS (Na Tabela de Faturação)
    idx_cdr_afetados = df_cdr['Phone Number'].isin(clientes_Leste)
    df_cdr.loc[idx_cdr_afetados, 'CustServ Calls'] += np.random.randint(3, 8, size=idx_cdr_afetados.sum())
    df_cdr.loc[idx_cdr_afetados, 'Churn'] = np.random.choice([True, False], p=[0.85, 0.15], size=idx_cdr_afetados.sum())

    # Remover a coluna original inútil de DeviceID
    if 'DeviceID' in df_logs.columns:
        df_logs = df_logs.drop(columns=['DeviceID'])

    # Exportar BRONZE
    print("A exportar os dados brutos simulados (Camada Bronze)...")
    df_logs.to_csv('Bronze_Network_Logs_Leiria.csv', index=False, sep=';')
    df_cdr.to_csv('Bronze_CDR_Customers.csv', index=False, sep=';')
    df_call.to_csv('Bronze_Call_Tests.csv', index=False, sep=';')
    df_towers_leiria.to_csv('Bronze_Towers_Leiria.csv', index=False, sep=',') 
    
    print("Fase 1 concluída! Chaves estrangeiras perfeitamente mapeadas em todas as tabelas.")

if __name__ == "__main__":
    gerar_camada_bronze()