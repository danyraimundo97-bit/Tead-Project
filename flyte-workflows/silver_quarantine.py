"""Quarentena partilhada: limpeza numérica com circuito 5% e gravação raw+audit no Iceberg."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import numpy as np
import pandas as pd

NUMERIC_DESTROY_THRESHOLD = 0.05


def sql_str(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def bronze_cell_sql(v) -> str:
    if v is None or v is pd.NA:
        return "NULL"
    if isinstance(v, float) and np.isnan(v):
        return "NULL"
    return sql_str(str(v))


def insert_quarantine_rows(
    cur,
    bronze_only: pd.DataFrame,
    lines: pd.Series,
    q_idx: pd.Index,
    issues_by_row: dict,
    *,
    source_file: str,
    bronze_col_order: list[str],
    raw_table_fqn: str,
    raw_columns_sql: str,
    audit_table_fqn: str,
    logger,
    log_label: str,
) -> None:
    """Insere linhas em *_quarantine_raw e *_quarantine_audit (mesmo formato de audit)."""
    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    for idx in q_idx:
        rid = str(uuid.uuid4())
        br = bronze_only.loc[idx]
        line_no = int(lines.loc[idx])
        cols_bad = issues_by_row.get(int(idx), [])
        err = (
            "Numeric cleaning produced null from non-null raw values in columns: "
            f"{', '.join(cols_bad)}"
        )
        raw_vals = [sql_str(rid)]
        for c in bronze_col_order:
            if c not in bronze_only.columns:
                raw_vals.append("NULL")
            else:
                raw_vals.append(bronze_cell_sql(br.get(c)))
        cur.execute(
            f"INSERT INTO {raw_table_fqn} ({raw_columns_sql}) "
            f"VALUES ({', '.join(raw_vals)})"
        )
        cur.fetchall()
        cur.execute(
            f"INSERT INTO {audit_table_fqn} "
            "(row_id, line_number, source_file, error_description, loaded_at) VALUES ("
            f"{sql_str(rid)}, {line_no}, {sql_str(source_file)}, {sql_str(err)}, "
            f"TIMESTAMP '{loaded_at}')"
        )
        cur.fetchall()
    logger.info("Quarantine %s: %s row(s) written", log_label, len(q_idx))
