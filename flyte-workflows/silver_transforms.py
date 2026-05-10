"""Transformações de features na camada silver (boolean, one-hot) partilhadas pelos process_*."""

from __future__ import annotations

import pandas as pd

# --- Towers ---
TOWERS_RADIO_OHE_COLS: tuple[str, ...] = (
    "radio_ohe_gsm",
    "radio_ohe_umts",
    "radio_ohe_lte",
    "radio_ohe_nr",
    "radio_ohe_cdma",
    "radio_ohe_other",
)

_RADIO_CODE_MAP: dict[str, str] = {
    "0": "OTHER",
    "1": "GSM",
    "2": "CDMA",
    "3": "UMTS",
    "4": "LTE",
    "5": "NR",
}


def _normalize_radio_token(raw) -> str:
    s = str(raw).strip().upper()
    if s in _RADIO_CODE_MAP:
        return _RADIO_CODE_MAP[s]
    if s in ("GSM", "CDMA", "UMTS", "LTE", "NR", "5G"):
        if s == "5G":
            return "NR"
        return s
    if "LTE" in s or "4G" in s:
        return "LTE"
    if "NR" in s or "5G" in s:
        return "NR"
    if "UMTS" in s or "3G" in s:
        return "UMTS"
    if "GSM" in s or "2G" in s:
        return "GSM"
    if "CDMA" in s:
        return "CDMA"
    return "OTHER"


def transform_towers_silver_features(df: pd.DataFrame) -> pd.DataFrame:
    """status → boolean; radio → one-hot boolean columns; remove ``radio``."""
    out = df.copy()
    if "status" in out.columns:
        st = out["status"].astype(str).str.strip().str.lower()
        out["status"] = st.isin(
            ("live", "active", "true", "1", "yes", "operational", "ok")
        ).astype(bool)
    if "radio" not in out.columns:
        raise ValueError("Expected column 'radio' for towers encoding")
    norm = out["radio"].map(_normalize_radio_token)
    for label, col in zip(
        ("GSM", "UMTS", "LTE", "NR", "CDMA", "OTHER"), TOWERS_RADIO_OHE_COLS
    ):
        out[col] = (norm == label).astype(bool)
    out.drop(columns=["radio"], inplace=True)
    return out


# --- Network logs ---
NETWORK_TYPE_OHE_COLS: tuple[str, ...] = (
    "nt_ohe_lte",
    "nt_ohe_gsm",
    "nt_ohe_umts",
    "nt_ohe_nr",
    "nt_ohe_cdma",
    "nt_ohe_other",
)


def _classify_network_type(raw) -> str:
    s = str(raw).lower()
    if "nr" in s or "5g" in s:
        return "NR"
    if "lte" in s or "4g" in s:
        return "LTE"
    if "umts" in s or "3g" in s:
        return "UMTS"
    if "gsm" in s or "2g" in s:
        return "GSM"
    if "cdma" in s:
        return "CDMA"
    return "OTHER"


def transform_network_logs_silver_features(df: pd.DataFrame) -> pd.DataFrame:
    """network_type → one-hot booleans; remove ``network_type``."""
    out = df.copy()
    if "network_type" not in out.columns:
        raise ValueError("Expected column 'network_type' for network_logs encoding")
    norm = out["network_type"].map(_classify_network_type)
    for label, col in zip(
        ("LTE", "GSM", "UMTS", "NR", "CDMA", "OTHER"), NETWORK_TYPE_OHE_COLS
    ):
        out[col] = (norm == label).astype(bool)
    out.drop(columns=["network_type"], inplace=True)
    return out


# --- Call tests ---
CALL_TESTS_TECH_OHE_COLS: tuple[str, ...] = (
    "tech_ohe_gsm",
    "tech_ohe_umts",
    "tech_ohe_lte",
    "tech_ohe_volte",
    "tech_ohe_nr",
    "tech_ohe_other",
)


def _result_to_bool(raw) -> bool:
    m = str(raw).strip().lower()
    if m in ("pass", "passed", "ok", "success", "true", "1", "yes"):
        return True
    if m in ("drop", "fail", "failed", "false", "0", "no"):
        return False
    return False


def _classify_call_tech(raw) -> str:
    s = str(raw).strip().upper()
    if "VOLTE" in s or "VOICE LTE" in s:
        return "VOLTE"
    if "NR" in s or "5G" in s:
        return "NR"
    if "LTE" in s or "4G" in s:
        return "LTE"
    if "UMTS" in s or "3G" in s or "HSPA" in s:
        return "UMTS"
    if "GSM" in s or "2G" in s or "EDGE" in s:
        return "GSM"
    return "OTHER"


def transform_call_tests_silver_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    call_test_result → ``result`` boolean; call_test_technology → tech_ohe_*;
    remove ``call_test_*`` prefix from metric columns.
    """
    out = df.copy()
    if "call_test_result" not in out.columns:
        raise ValueError("Expected column 'call_test_result'")
    out["result"] = out["call_test_result"].map(_result_to_bool).astype(bool)
    out.drop(columns=["call_test_result"], inplace=True)

    if "call_test_technology" not in out.columns:
        raise ValueError("Expected column 'call_test_technology'")
    norm = out["call_test_technology"].map(_classify_call_tech)
    for label, col in zip(
        ("GSM", "UMTS", "LTE", "VOLTE", "NR", "OTHER"), CALL_TESTS_TECH_OHE_COLS
    ):
        out[col] = (norm == label).astype(bool)
    out.drop(columns=["call_test_technology"], inplace=True)

    rename_map = {
        "call_test_duration_s": "duration_s",
        "call_test_setup_time_s": "setup_time_s",
    }
    for old, new in rename_map.items():
        if old not in out.columns:
            raise ValueError(f"Expected column '{old}'")
        out.rename(columns={old: new}, inplace=True)
    return out
