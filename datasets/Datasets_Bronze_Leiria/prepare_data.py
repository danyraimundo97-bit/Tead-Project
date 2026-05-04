"""
Script 2: prepare_data.py (Data Cleansing ETL)
Objetivo: Atuar como o pipeline de Engenharia de Dados que consome a camada Bronze 
          e aplica transformações de qualidade para gerar a camada Silver.
Ações:
- Deduplicação do CDR.
- Parsing e Casting (remoção de strings como "dBm" e conversão para Float).
- Substituição de notação europeia (vírgulas) por notação computacional (pontos).
- Limpeza de colunas e tratamento de valores nulos estruturais.
Saída: Ficheiros da Camada SILVER.
"""

import pandas as pd

def processar_bronze_para_silver():
    print("=== FASE 2: PIPELINE ETL (BRONZE PARA SILVER) ===")
    
    # 1. Ingestão da Camada Bronze
    print("A carregar os dados brutos da Camada Bronze...")
    df_logs = pd.read_csv('Bronze_Network_Logs_Leiria.csv', sep=';')
    df_cdr = pd.read_csv('Bronze_CDR_Customers.csv', sep=';')
    df_call = pd.read_csv('Bronze_Call_Tests.csv', sep=';')
    df_towers = pd.read_csv('Bronze_Towers_Leiria.csv', sep=',')

    # 2. Limpeza do Dataset: Faturação (CDR)
    print("[1/3] A limpar o CDR (Deduplicação)...")
    # Remoção do problema de ~40k linhas duplicadas
    df_cdr_silver = df_cdr.drop_duplicates()

    # 3. Limpeza do Dataset: Logs Físicos da Rede
    print("[2/3] A limpar os Logs de Rede (Parsing de Unidades e Nulos)...")
    df_logs_silver = df_logs.copy()
    
    # Trim dos nomes das colunas
    df_logs_silver.columns = df_logs_silver.columns.str.strip() 
    
    # Remoção de registos sem coordenadas
    df_logs_silver = df_logs_silver.dropna(subset=['Latitude', 'Longitude'])
    
    # Parsing de Strings para Float
    def extrair_numeros(val):
        if isinstance(val, str):
            limpo = val.replace(' dBm', '').replace(' dB', '').replace(' Mbps', '').replace(' km/h', '').strip()
            return pd.to_numeric(limpo, errors='coerce')
        return val
    
    colunas_sujas = ['RSRP', 'RSRQ', 'SINR', 'Downlink(Mbps)', 'Uplink(Mbps)', 'Velocity(km/h)']
    for col in colunas_sujas:
        if col in df_logs_silver.columns:
            df_logs_silver[col] = df_logs_silver[col].apply(extrair_numeros)

    # 4. Limpeza do Dataset: Testes de Chamada (MOS)
    print("[3/3] A limpar os Testes de Chamada (Formatação de Decimais)...")
    df_call_silver = df_call.copy()
    
    colunas_decimais_pt = ['Speed (m/s)', 'Distance from site (m)', 'Call Test Duration (s)', 'Call Test Setup Time (s)', 'MOS']
    for col in colunas_decimais_pt:
        if col in df_call_silver.columns:
            # Trocar vírgula por ponto para permitir análises no Trino/Iceberg
            df_call_silver[col] = df_call_silver[col].astype(str).str.replace(',', '.').astype(float)

    # 5. Exportação para a Camada Silver
    print("A exportar os dados estruturados e limpos para a Camada Silver...")
    df_logs_silver.to_csv('Silver_Network_Logs_Leiria.csv', index=False)
    df_cdr_silver.to_csv('Silver_CDR_Customers.csv', index=False)
    df_call_silver.to_csv('Silver_Call_Tests.csv', index=False)
    df_towers.to_csv('Silver_Towers_Leiria.csv', index=False)
    
    print("Fase 2 concluída com sucesso! Os ficheiros Silver estão prontos para consumo no Trino/PowerBI.")

if __name__ == "__main__":
    processar_bronze_para_silver()