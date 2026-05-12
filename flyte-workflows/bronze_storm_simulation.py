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
) -> tuple[str, np.ndarray]:
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
    down_arr = (
        df_towers_leiria.loc[torres_destruidas_indices, ["lat", "lon"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .to_numpy()
    )

    for dia in dias_simulacao:
        df_t = df_towers_leiria.copy()
        df_t["Snapshot_Date"] = dia.strftime("%Y-%m-%d")
        df_t["Status"] = "ACTIVE"
        if dia >= pd.to_datetime("2026-01-28"):
            df_t.loc[torres_destruidas_indices, "Status"] = "DOWN"
        snapshots_torres.append(df_t)
    df_towers_leiria = pd.concat(snapshots_torres, ignore_index=True)

    ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df_towers_leiria["_ingested_at"] = ts

    for dia in df_towers_leiria["Snapshot_Date"].dropna().unique():
        df_dia = df_towers_leiria[df_towers_leiria["Snapshot_Date"] == dia]
        df_dia.to_csv(
            f"s3://warehouse/bronze/towers/day={dia}/data.csv",
            index=False,
            sep=",",
            storage_options=storage_options,
        )
    n = len(df_towers_leiria["Snapshot_Date"].dropna().unique())
    return f"Towers bronze OK ({n} snapshot days)", down_arr


def _logs_phase(
    storage_options: Mapping[str, Any],
    log: logging.Logger,
    down_arr: np.ndarray,
) -> tuple[str, list, pd.DataFrame]:
    log.info("Bronze network_logs + preparar call tests (coords DOWN em memória)")

    df_logs = pd.read_csv(
        "s3://warehouse/Dados_Raw/Cellular Network Handover Prediction Dataset.csv",
        sep=";",
        storage_options=storage_options,
    )
    df_cdr = pd.read_csv(
        "s3://warehouse/Dados_Raw/CDR-Call-Details.csv",
        sep=";",
        storage_options=storage_options,
    )
    df_call = pd.read_csv(
        "s3://warehouse/Dados_Raw/Call Tests Measurements for MOS prediction.csv",
        sep=";",
        storage_options=storage_options,
    )

    lat_offset = 39.7436 - 18.11
    lon_offset = -8.8071 - 83.40
    df_logs["Latitude"] = df_logs["Latitude"] + lat_offset
    df_logs["Longitude"] = df_logs["Longitude"] + lon_offset

    df_logs["Timestamp"] = pd.to_datetime(df_logs["Timestamp"])
    data_inicio_alvo = pd.to_datetime("2026-01-20 00:00:00")
    df_logs["Timestamp"] = df_logs["Timestamp"] + (
        data_inicio_alvo - df_logs["Timestamp"].min()
    )

    logs_expandidos = [df_logs.copy()]
    for i in range(1, 6):
        df_copy = df_logs.copy()
        df_copy["Timestamp"] = df_copy["Timestamp"] + pd.Timedelta(days=3 * i)
        logs_expandidos.append(df_copy)
    df_logs = pd.concat(logs_expandidos, ignore_index=True)
    data_fim_alvo = pd.to_datetime("2026-02-04 23:59:59")
    df_logs = df_logs[df_logs["Timestamp"] <= data_fim_alvo]

    segundos_em_16_dias = 16 * 24 * 60 * 60
    random_deltas = pd.to_timedelta(
        np.random.randint(0, segundos_em_16_dias, size=len(df_call)),
        unit="s",
    )
    df_call["Date Of Test"] = pd.to_datetime("2026-01-20 00:00:00") + random_deltas
    # read_csv pode deixar datetime64[us]; atribuições com ns (ex. uniform em s) falham nesse dtype
    df_call["Date Of Test"] = df_call["Date Of Test"].astype("datetime64[ns]")

    np.random.seed(42)
    unique_phones = df_cdr["Phone Number"].dropna().unique()
    clientes_leiria = np.random.choice(unique_phones, size=2500, replace=False)
    df_logs["Phone_Number"] = np.random.choice(clientes_leiria, size=len(df_logs))
    df_call["Phone_Number"] = np.random.choice(clientes_leiria, size=len(df_call))

    inicio_tempestade = pd.to_datetime("2026-01-28 00:00:00")
    fim_tempestade = pd.to_datetime("2026-01-30 23:59:59")

    cond_log_tempestade_Leste = (
        (df_logs["Timestamp"] >= inicio_tempestade)
        & (df_logs["Timestamp"] <= fim_tempestade)
        & (df_logs["Longitude"] > -8.80)
    )
    cond_log_tempestade_Oeste = (
        (df_logs["Timestamp"] >= inicio_tempestade)
        & (df_logs["Timestamp"] <= fim_tempestade)
        & (df_logs["Longitude"] <= -8.80)
    )

    if len(down_arr) > 0:
        n_storm = int(cond_log_tempestade_Leste.sum())
        if n_storm > 0:
            rng_geo = np.random.default_rng(44)
            picks = down_arr[rng_geo.integers(0, len(down_arr), size=n_storm)]
            jitter_lat = rng_geo.uniform(-0.0018, 0.0018, size=n_storm)
            jitter_lon = rng_geo.uniform(-0.0018, 0.0018, size=n_storm)
            df_logs.loc[cond_log_tempestade_Leste, "Latitude"] = picks[:, 0] + jitter_lat
            df_logs.loc[cond_log_tempestade_Leste, "Longitude"] = picks[:, 1] + jitter_lon

    def piorar_rsrp_string(val):
        if isinstance(val, str) and "dBm" in val:
            return f"{int(val.replace(' dBm', '').strip()) - 35} dBm"
        return val

    def piorar_rsrq_string(val):
        if isinstance(val, str) and "dB" in val:
            return f"{int(val.replace(' dB', '').strip()) - 15} dB"
        return val

    df_logs.loc[cond_log_tempestade_Leste, "RSRP"] = df_logs.loc[
        cond_log_tempestade_Leste, "RSRP"
    ].apply(piorar_rsrp_string)
    df_logs.loc[cond_log_tempestade_Leste, "RSRQ"] = df_logs.loc[
        cond_log_tempestade_Leste, "RSRQ"
    ].apply(piorar_rsrq_string)
    df_logs.loc[cond_log_tempestade_Leste, "SINR"] = np.random.uniform(
        -15.0, -5.0, size=cond_log_tempestade_Leste.sum()
    ).astype(str)
    df_logs.loc[cond_log_tempestade_Leste, "Velocity(km/h)"] = "0.0 km/h"
    fallback_techs = np.random.choice(["UMTS", "GSM"], size=cond_log_tempestade_Leste.sum())
    df_logs.loc[cond_log_tempestade_Leste, "NetworkType"] = fallback_techs.astype(str)
    df_logs.loc[cond_log_tempestade_Oeste, "Downlink(Mbps)"] = "1.5 Mbps"
    df_logs.loc[cond_log_tempestade_Oeste, "Uplink(Mbps)"] = "0.2 Mbps"

    clientes_Leste = (
        df_logs.loc[cond_log_tempestade_Leste, "Phone_Number"].unique().tolist()
    )

    rng_call = np.random.default_rng(45)
    mask_phone_l = df_call["Phone_Number"].isin(clientes_Leste)
    mask_in_storm = (df_call["Date Of Test"] >= inicio_tempestade) & (
        df_call["Date Of Test"] <= fim_tempestade
    )
    outside_storm = mask_phone_l & ~mask_in_storm
    if outside_storm.any():
        idx_out = df_call.loc[outside_storm].index.to_numpy()
        move = rng_call.random(len(idx_out)) < 0.72
        to_move = idx_out[move]
        if len(to_move) > 0:
            span_sec = (fim_tempestade - inicio_tempestade).total_seconds()
            new_times = inicio_tempestade + pd.to_timedelta(
                rng_call.uniform(0.0, span_sec, size=len(to_move)),
                unit="s",
            )
            df_call.loc[to_move, "Date Of Test"] = new_times

    cond_call_tempestade = (
        (df_call["Date Of Test"] >= inicio_tempestade)
        & (df_call["Date Of Test"] <= fim_tempestade)
        & (df_call["Phone_Number"].isin(clientes_Leste))
    )
    n_drop = int(cond_call_tempestade.sum())

    df_call.loc[cond_call_tempestade, "Call Test Result"] = "DROP"
    df_call.loc[cond_call_tempestade, "MOS"] = np.random.uniform(
        1.0, 1.8, size=n_drop
    ).astype(str)
    df_call["MOS"] = df_call["MOS"].astype(str).str.replace(".", ",")
    df_call.loc[cond_call_tempestade, "Call Test Duration (s)"] = np.random.uniform(
        2.0, 12.0, size=n_drop
    ).astype(str)
    df_call["Call Test Duration (s)"] = (
        df_call["Call Test Duration (s)"].astype(str).str.replace(".", ",")
    )
    df_call.loc[cond_call_tempestade, "Call Test Setup Time (s)"] = np.random.uniform(
        15.0, 45.0, size=n_drop
    ).astype(str)
    df_call["Call Test Setup Time (s)"] = (
        df_call["Call Test Setup Time (s)"].astype(str).str.replace(".", ",")
    )
    df_call.loc[cond_call_tempestade, "Distance from site (m)"] = np.random.uniform(
        5000, 15000, size=n_drop
    ).astype(str)
    df_call["Distance from site (m)"] = (
        df_call["Distance from site (m)"].astype(str).str.replace(".", ",")
    )

    ts = pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    df_call["_ingested_at"] = ts

    if "DeviceID" in df_logs.columns:
        df_logs = df_logs.drop(columns=["DeviceID"])
    df_logs["_ingested_at"] = ts

    df_logs["Data_Fatia"] = pd.to_datetime(df_logs["Timestamp"]).dt.strftime("%Y-%m-%d")
    for dia in df_logs["Data_Fatia"].dropna().unique():
        df_dia = df_logs[df_logs["Data_Fatia"] == dia].drop(columns=["Data_Fatia"])
        df_dia.to_csv(
            f"s3://warehouse/bronze/network_logs/day={dia}/data.csv",
            index=False,
            sep=";",
            storage_options=storage_options,
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
    m1, down_arr = _towers_phase(storage_options, log)
    m2, clientes_Leste, df_call = _logs_phase(storage_options, log, down_arr)
    m3 = _call_tests_phase(storage_options, log, df_call)
    m4 = _cdr_phase(storage_options, log, clientes_Leste)
    return "; ".join([m1, m2, m3, m4])
