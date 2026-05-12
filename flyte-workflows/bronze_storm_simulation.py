"""
Simulação Raw → Bronze (Leiria / tempestade Jan 2026).

Espelha ``datasets/Datasets_Raw/pipeline_raw_bronze.py``.
Tudo corre num único processo: estado entre fases (torres → logs → call_tests → CDR)
passa em memória, sem ficheiros de handoff em S3.
"""

from __future__ import annotations

import logging
from typing import Any, Mapping

import numpy as np
import pandas as pd

from flyte_task_env import TASK_ENV


def get_storage_options() -> dict[str, Any]:
    """Credenciais S3/MinIO a partir do ambiente do pod Flyte."""
    return {
        "key": TASK_ENV.get("AWS_ACCESS_KEY_ID", "minioadmin"),
        "secret": TASK_ENV.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        "client_kwargs": {
            "endpoint_url": TASK_ENV.get(
                "MLFLOW_S3_ENDPOINT_URL", "http://host.docker.internal:9000"
            )
        },
    }


def _towers_phase(
    storage_options: Mapping[str, Any], log: logging.Logger
) -> tuple[str, dict]:
    log.info("Bronze towers: opencellid → snapshots (down coords em memória)")
    df_towers = pd.read_csv(
        "s3://warehouse/Dados_Raw/opencellid_pt.csv",
        sep=",",
        storage_options=storage_options,
    )
    df_towers_leiria = df_towers[
        (df_towers["lat"] >= 39.5)
        & (df_towers["lat"] <= 39.9)
        & (df_towers["lon"] >= -9.0)
        & (df_towers["lon"] <= -8.6)
    ].copy()

    dias_simulacao = pd.date_range(start="2026-01-20", end="2026-02-04", freq="D")
    snapshots_torres = []
    np.random.seed(42)
    
    torres_Leste_indices = df_towers_leiria[df_towers_leiria["lon"] > -8.80].index
    torres_destruidas_indices = np.random.choice(
        torres_Leste_indices,
        size=int(len(torres_Leste_indices) * 0.80),
        replace=False,
    )

    # --- LÓGICA DE REPARAÇÃO GRADUAL ---
    rng_repair = np.random.default_rng(99)
    ordem_reparacao = rng_repair.permutation(torres_destruidas_indices)
    
    # Reparar 15 antenas/dia nos últimos 3 dias para impacto visível no Dashboard
    reparadas_dia_2 = ordem_reparacao[0:15]
    reparadas_dia_3 = ordem_reparacao[0:30]
    reparadas_dia_4 = ordem_reparacao[0:45]
    
    down_dict = {} # Mapeia os dias para as coordenadas das torres que estão DOWN

    for dia in dias_simulacao:
        df_t = df_towers_leiria.copy()
        df_t["Snapshot_Date"] = dia.strftime("%Y-%m-%d")
        df_t["Status"] = "ACTIVE"
        date_str = dia.strftime("%Y-%m-%d")
        
        if dia >= pd.to_datetime("2026-01-28"):
            down_atuais = set(torres_destruidas_indices)
            
            # Subtrai as antenas que já foram reparadas
            if dia >= pd.to_datetime("2026-02-04"):
                down_atuais -= set(reparadas_dia_4)
            elif dia >= pd.to_datetime("2026-02-03"):
                down_atuais -= set(reparadas_dia_3)
            elif dia >= pd.to_datetime("2026-02-02"):
                down_atuais -= set(reparadas_dia_2)
                
            down_list = list(down_atuais)
            df_t.loc[down_list, "Status"] = "DOWN"
            
            # Guardar apenas as coordenadas das antenas destruídas NESTE dia
            coords = df_towers_leiria.loc[down_list, ["lat", "lon"]].drop_duplicates().to_numpy()
            down_dict[date_str] = coords
        else:
            down_dict[date_str] = np.array([])
            
        snapshots_torres.append(df_t)
        
    df_towers_leiria = pd.concat(snapshots_torres, ignore_index=True)
    ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df_towers_leiria["_ingested_at"] = ts

    for dia in df_towers_leiria["Snapshot_Date"].dropna().unique():
        df_dia = df_towers_leiria[df_towers_leiria["Snapshot_Date"] == dia]
        df_dia.to_csv(
            f"s3://warehouse/bronze/towers/day={dia}/data.csv",
            index=False, sep=",", storage_options=storage_options,
        )
    n = len(df_towers_leiria["Snapshot_Date"].dropna().unique())
    return f"Towers bronze OK ({n} snapshot days)", down_dict


def _logs_phase(
    storage_options: Mapping[str, Any],
    log: logging.Logger,
    down_dict: dict,
) -> tuple[str, list, pd.DataFrame]:
    log.info("Bronze network_logs + preparar call tests (coords DOWN em memória)")

    df_logs = pd.read_csv("s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv", sep=";", storage_options=storage_options)
    df_cdr = pd.read_csv("s3://warehouse/Dados_Raw/CDR-Call-Details.csv", sep=";", storage_options=storage_options)
    df_call = pd.read_csv("s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv", sep=";", storage_options=storage_options)

    # --- NORMALIZAÇÃO GEOGRÁFICA EXATA (Impede distâncias absurdas de 300km) ---
    lat_min, lat_max, lon_min, lon_max = 39.5, 39.9, -9.0, -8.6
    lat_orig_min, lat_orig_max = df_logs["Latitude"].min(), df_logs["Latitude"].max()
    lon_orig_min, lon_orig_max = df_logs["Longitude"].min(), df_logs["Longitude"].max()
    
    lat_norm = (df_logs["Latitude"] - lat_orig_min) / (lat_orig_max - lat_orig_min)
    lon_norm = (df_logs["Longitude"] - lon_orig_min) / (lon_orig_max - lon_orig_min)
    df_logs["Latitude"] = lat_min + lat_norm * (lat_max - lat_min)
    df_logs["Longitude"] = lon_min + lon_norm * (lon_max - lon_min)

    # --- REPLICAÇÃO DIÁRIA DE LOGS (Remove os buracos de tempo) ---
    df_logs["Timestamp"] = pd.to_datetime(df_logs["Timestamp"])
    data_inicio_alvo = pd.to_datetime("2026-01-20 00:00:00")
    df_logs["Timestamp"] = df_logs["Timestamp"] + (data_inicio_alvo - df_logs["Timestamp"].min())

    logs_expandidos = [df_logs.copy()]
    for i in range(1, 16):
        df_copy = df_logs.copy()
        df_copy["Timestamp"] = df_copy["Timestamp"] + pd.Timedelta(days=i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)
    df_logs = df_logs[df_logs["Timestamp"] <= pd.to_datetime("2026-02-04 23:59:59")]

    segundos_em_16_dias = 16 * 24 * 60 * 60
    random_deltas = pd.to_timedelta(np.random.randint(0, segundos_em_16_dias, size=len(df_call)), unit="s")
    df_call["Date Of Test"] = (pd.to_datetime("2026-01-20 00:00:00") + random_deltas).astype("datetime64[ns]")

    np.random.seed(42)
    unique_phones = df_cdr["Phone Number"].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    df_logs["Phone_Number"] = np.random.choice(clientes_leiria, size=len(df_logs))
    df_call["Phone_Number"] = np.random.choice(clientes_leiria, size=len(df_call))

    def piorar_rsrp_string(val):
        if isinstance(val, str) and "dBm" in val:
            return f"{int(val.replace(' dBm', '').strip()) - 35} dBm"
        return val

    def piorar_rsrq_string(val):
        if isinstance(val, str) and "dB" in val:
            return f"{int(val.replace(' dB', '').strip()) - 15} dB"
        return val

    # --- LÓGICA DINÂMICA DE DEGRADAÇÃO (Baseada nas reparações) ---
    df_logs["Data_Fatia"] = df_logs["Timestamp"].dt.strftime("%Y-%m-%d")
    clientes_Leste_set = set()
    total_destruidas_inicial = len(down_dict.get("2026-01-28", []))

    for date_str, down_coords in down_dict.items():
        if len(down_coords) > 0:
            cond_leste_dia = (df_logs["Data_Fatia"] == date_str) & (df_logs["Longitude"] > -8.80)
            n_storm = int(cond_leste_dia.sum())
            
            if n_storm > 0:
                # O segredo: A percentagem de telemóveis afetados diminui à medida que reparamos as torres!
                fraction_down = len(down_coords) / float(total_destruidas_inicial)
                n_degrade = int(n_storm * fraction_down)
                
                idx_leste_dia = df_logs[cond_leste_dia].index
                idx_to_degrade = np.random.choice(idx_leste_dia, size=n_degrade, replace=False)
                
                rng_geo = np.random.default_rng(44)
                picks = down_coords[rng_geo.integers(0, len(down_coords), size=n_degrade)]
                jitter_lat = rng_geo.uniform(-0.0018, 0.0018, size=n_degrade)
                jitter_lon = rng_geo.uniform(-0.0018, 0.0018, size=n_degrade)
                
                df_logs.loc[idx_to_degrade, "Latitude"] = picks[:, 0] + jitter_lat
                df_logs.loc[idx_to_degrade, "Longitude"] = picks[:, 1] + jitter_lon
                
                df_logs.loc[idx_to_degrade, "RSRP"] = df_logs.loc[idx_to_degrade, "RSRP"].apply(piorar_rsrp_string)
                df_logs.loc[idx_to_degrade, "RSRQ"] = df_logs.loc[idx_to_degrade, "RSRQ"].apply(piorar_rsrq_string)
                df_logs.loc[idx_to_degrade, "SINR"] = np.random.uniform(-15.0, -5.0, size=n_degrade).astype(str)
                df_logs.loc[idx_to_degrade, "Velocity(km/h)"] = "0.0 km/h"
                df_logs.loc[idx_to_degrade, "NetworkType"] = np.random.choice(["UMTS", "GSM"], size=n_degrade).astype(str)
                
                clientes_Leste_set.update(df_logs.loc[idx_to_degrade, "Phone_Number"].unique())

    clientes_Leste = list(clientes_Leste_set)
    df_call["Data_Fatia_Call"] = df_call["Date Of Test"].dt.strftime("%Y-%m-%d")
    
    for date_str, down_coords in down_dict.items():
        if len(down_coords) > 0:
            fraction_down = len(down_coords) / float(total_destruidas_inicial)
            cond_call_dia = (df_call["Data_Fatia_Call"] == date_str) & (df_call["Phone_Number"].isin(clientes_Leste))
            n_call = int(cond_call_dia.sum())
            
            if n_call > 0:
                n_degrade = int(n_call * fraction_down)
                idx_call_dia = df_call[cond_call_dia].index
                idx_to_degrade = np.random.choice(idx_call_dia, size=n_degrade, replace=False)
                
                df_call.loc[idx_to_degrade, "Call Test Result"] = "DROP"
                df_call.loc[idx_to_degrade, "MOS"] = [str(round(x, 2)).replace(".", ",") for x in np.random.uniform(1.0, 1.8, size=n_degrade)]
                df_call.loc[idx_to_degrade, "Call Test Duration (s)"] = [str(round(x, 2)).replace(".", ",") for x in np.random.uniform(2.0, 12.0, size=n_degrade)]
                df_call.loc[idx_to_degrade, "Call Test Setup Time (s)"] = [str(round(x, 2)).replace(".", ",") for x in np.random.uniform(15.0, 45.0, size=n_degrade)]
                df_call.loc[idx_to_degrade, "Distance from site (m)"] = [str(round(x, 2)).replace(".", ",") for x in np.random.uniform(5000, 15000, size=n_degrade)]

    ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df_call["_ingested_at"] = ts
    df_call.drop(columns=["Data_Fatia_Call"], inplace=True, errors="ignore")
    
    # Não apagar o DeviceID pois a camada Silver Quarentena precisa desta coluna
    # if "DeviceID" in df_logs.columns:
    #     df_logs = df_logs.drop(columns=["DeviceID"])

    df_logs["_ingested_at"] = ts

    for dia in df_logs["Data_Fatia"].dropna().unique():
        df_dia = df_logs[df_logs["Data_Fatia"] == dia].drop(columns=["Data_Fatia"])
        df_dia.to_csv(
            f"s3://warehouse/bronze/network_logs/day={dia}/data.csv",
            index=False, sep=";", storage_options=storage_options,
        )
    n_days = len(df_logs["Data_Fatia"].dropna().unique())
    return f"Logs bronze OK ({n_days} days) + prepared call_tests", clientes_Leste, df_call

def _call_tests_phase(
    storage_options: Mapping[str, Any], log: logging.Logger, df_call: pd.DataFrame
) -> str:
    log.info("Bronze call_tests: particiona df_call preparado em memória")
    df_call = df_call.copy()
    df_call["Data_Fatia"] = pd.to_datetime(df_call["Date Of Test"]).dt.strftime("%Y-%m-%d")
    for dia in df_call["Data_Fatia"].dropna().unique():
        df_dia = df_call[df_call["Data_Fatia"] == dia].drop(columns=["Data_Fatia"])
        df_dia.to_csv(
            f"s3://warehouse/bronze/call_tests/day={dia}/data.csv",
            index=False,
            sep=";",
            storage_options=storage_options,
        )
    n = len(df_call["Data_Fatia"].dropna().unique())
    return f"Call tests bronze OK ({n} days)"


def _cdr_phase(
    storage_options: Mapping[str, Any],
    log: logging.Logger,
    clientes_Leste: list,
) -> str:
    log.info("Bronze cdr_customers: RAW + clientes Leste em memória")
    df_cdr = pd.read_csv(
        "s3://warehouse/Dados_Raw/CDR-Call-Details.csv",
        sep=";",
        storage_options=storage_options,
    )
    idx_cdr_afetados = df_cdr["Phone Number"].isin(clientes_Leste)
    df_cdr.loc[idx_cdr_afetados, "CustServ Calls"] += np.random.randint(
        3, 8, size=idx_cdr_afetados.sum()
    )
    df_cdr.loc[idx_cdr_afetados, "Churn"] = np.random.choice(
        [True, False], p=[0.85, 0.15], size=idx_cdr_afetados.sum()
    )
    df_cdr["_ingested_at"] = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    datas_gerais = pd.date_range(start="2026-01-20", end="2026-02-04").strftime("%Y-%m-%d")
    for dia in datas_gerais:
        df_cdr.to_csv(
            f"s3://warehouse/bronze/cdr_customers/day={dia}/data.csv",
            index=False,
            sep=";",
            storage_options=storage_options,
        )
    return f"CDR bronze OK ({len(datas_gerais)} days)"


def run_bronze_storm_simulation(
    storage_options: Mapping[str, Any], log: logging.Logger
) -> str:
    """Torres → logs → call_tests → CDR num único processo (sem handoff S3)."""
    m1, down_dict = _towers_phase(storage_options, log)
    m2, clientes_Leste, df_call = _logs_phase(storage_options, log, down_dict)
    m3 = _call_tests_phase(storage_options, log, df_call)
    m4 = _cdr_phase(storage_options, log, clientes_Leste)
    return "; ".join([m1, m2, m3, m4])
