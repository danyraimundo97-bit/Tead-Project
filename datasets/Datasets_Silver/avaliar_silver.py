"""
Script de Avaliação 2: avaliar_silver.py
Objetivo: Validar se o 'prepare_data.py' executou corretamente a Limpeza (ETL).
Verifica: Tipos de dados (float em vez de string), remoção de duplicados e ausência de vírgulas.
"""

import pandas as pd

def avaliar_silver():
    print("="*50)
    print(" INICIANDO AVALIAÇÃO DA CAMADA SILVER (CLEAN)")
    print("="*50)
    
    try:
        df_logs = pd.read_csv('Silver_Network_Logs_Leiria.csv')
        df_cdr = pd.read_csv('Silver_CDR_Customers.csv')
        df_call = pd.read_csv('Silver_Call_Tests.csv')
        df_towers = pd.read_csv('Silver_Towers_Leiria.csv')
        
        # 1. Verificar Parsing de Rádio (Logs)
        print("\n[1] NETWORK LOGS (Limpeza de Unidades):")
        rsrp_type = df_logs['RSRP'].dtype
        vel_type = df_logs['Velocity(km/h)'].dtype
        print(f" -> RSRP convertido para número? Tipo atual: {rsrp_type}")
        print(f" -> Velocity convertido para número? Tipo atual: {vel_type}")
        
        nulos_geo = df_logs['Latitude'].isnull().sum()
        print(f" -> Linhas sem coordenadas removidas? (Nulos em Latitude = {nulos_geo})")

        # 2. Verificar Deduplicação (CDR)
        print("\n[2] CDR (Deduplicação):")
        duplicados = df_cdr.duplicated().sum()
        print(f" -> Limpeza bem sucedida? Existem {duplicados} linhas duplicadas no ficheiro.")
        print(f" -> Total de clientes únicos faturados: {len(df_cdr)}")

        # 3. Verificar Formatação de Decimais (Call Tests)
        print("\n[3] CALL TESTS (Formatação Americana):")
        mos_type = df_call['MOS'].dtype
        dur_type = df_call['Call Test Duration (s)'].dtype
        print(f" -> MOS convertido para Float? Tipo atual: {mos_type}")
        print(f" -> Duração convertida para Float? Tipo atual: {dur_type}")
        
        # 4. Verificação Geral de Snapshots (Towers)
        print("\n[4] TORRES (Estrutura final):")
        print(f" -> Total de registos integrados na camada Silver: {len(df_towers)}")

    except FileNotFoundError as e:
        print(f"ERRO: Ficheiro não encontrado - {e}")

if __name__ == "__main__":
    avaliar_silver()