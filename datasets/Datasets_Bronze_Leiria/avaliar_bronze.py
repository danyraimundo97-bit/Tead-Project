"""
Script de Avaliação 1: avaliar_bronze.py
Objetivo: Validar se o 'simular_tempestade.py' gerou corretamente a Camada Bronze.
Verifica: Aumento de dados (15 dias), Snapshots das Torres, impacto da tempestade e "sujidade" técnica.
"""

import pandas as pd

def avaliar_bronze():
    print("="*50)
    print(" INICIANDO AVALIAÇÃO DA CAMADA BRONZE (RAW)")
    print("="*50)
    
    try:
        df_logs = pd.read_csv('Bronze_Network_Logs_Leiria.csv', sep=';')
        df_cdr = pd.read_csv('Bronze_CDR_Customers.csv', sep=';')
        df_call = pd.read_csv('Bronze_Call_Tests.csv', sep=';')
        df_towers = pd.read_csv('Bronze_Towers_Leiria.csv', sep=',')
        
        # 1. Avaliar Data Augmentation (Logs)
        print("\n[1] NETWORK LOGS (Física da Rede):")
        df_logs['Timestamp'] = pd.to_datetime(df_logs['Timestamp'])
        dias_totais = (df_logs['Timestamp'].max() - df_logs['Timestamp'].min()).days
        print(f" -> Período de dados expandido: {dias_totais} dias (De {df_logs['Timestamp'].min().date()} a {df_logs['Timestamp'].max().date()})")
        
        # Verificar Sujidade (RSRP deve ser string)
        rsrp_type = df_logs['RSRP'].dtype
        print(f" -> Sujidade intencional: RSRP é do tipo '{rsrp_type}' (Contém 'dBm'? {df_logs['RSRP'].astype(str).str.contains('dBm').any()})")
        
        # 2. Avaliar Snapshots de Antenas (Towers)
        print("\n[2] TORRES (Infraestrutura Diária):")
        print(f" -> Total de registos de snapshot gerados: {len(df_towers)}")
        if 'Snapshot_Date' in df_towers.columns and 'Status' in df_towers.columns:
            torres_down = len(df_towers[df_towers['Status'] == 'DOWN'])
            print(f" -> Torres destruídas ('DOWN') registadas nos dias da tempestade: {torres_down}")
        else:
            print(" -> ERRO: Colunas de Snapshot em falta!")

        # 3. Avaliar Impacto da Tempestade e Vírgulas (Call Tests)
        print("\n[3] CALL TESTS (Qualidade de Chamada):")
        df_call['Date Of Test'] = pd.to_datetime(df_call['Date Of Test'])
        tempestade_calls = df_call[(df_call['Date Of Test'] >= '2026-01-28') & (df_call['Date Of Test'] <= '2026-01-30')]
        drops = len(tempestade_calls[tempestade_calls['Call Test Result'] == 'DROP'])
        print(f" -> Chamadas que sofreram DROP durante a tempestade: {drops}")
        
        # Verificar formatação europeia
        mos_type = df_call['MOS'].dtype
        print(f" -> Sujidade intencional: MOS lido como '{mos_type}' (Contém vírgulas? {df_call['MOS'].astype(str).str.contains(',').any()})")

        # 4. Avaliar Faturação e Duplicados (CDR)
        print("\n[4] CDR (Faturação e Clientes):")
        duplicados = df_cdr.duplicated().sum()
        print(f" -> Sujidade intencional: Existem {duplicados} linhas duplicadas no ficheiro.")
        churn_rate = (df_cdr['Churn'] == True).mean() * 100
        print(f" -> Taxa de Churn Global Simulada: {churn_rate:.2f}%")
        
    except FileNotFoundError as e:
        print(f"ERRO: Ficheiro não encontrado - {e}")

if __name__ == "__main__":
    avaliar_bronze()