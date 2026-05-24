"""Transacções Trino para substituição atómica de tabelas Iceberg (DELETE + INSERT + COMMIT)."""

from __future__ import annotations

import logging
from typing import Sequence


def replace_table_transaction(
    conn,
    *,
    table_fqn: str,
    insert_sql: str,
    logger: logging.Logger | None = None,
    extra_sql: Sequence[str] = (),
) -> None:
    """
    Substitui ``table_fqn`` numa transacção: DELETE WHERE TRUE + INSERT + COMMIT.
    Em falha faz ROLLBACK (snapshot Iceberg anterior preservado).
    """
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        cur.fetchall()

        cur.execute(f"DELETE FROM {table_fqn} WHERE TRUE")
        cur.fetchall()

        cur.execute(insert_sql)
        cur.fetchall()

        for sql in extra_sql:
            cur.execute(sql)
            cur.fetchall()

        cur.execute("COMMIT")
        cur.fetchall()
        if logger:
            logger.info("ACID commit: %s replaced atomically", table_fqn)
    except Exception:
        try:
            cur.execute("ROLLBACK")
            cur.fetchall()
            if logger:
                logger.warning("ACID rollback: %s unchanged", table_fqn)
        except Exception as rb_exc:
            if logger:
                logger.error("ROLLBACK failed for %s: %s", table_fqn, rb_exc)
        raise


def replace_table_transaction_multi_insert(
    conn,
    *,
    table_fqn: str,
    insert_sqls: Sequence[str],
    logger: logging.Logger | None = None,
) -> None:
    """DELETE + vários INSERT na mesma transacção (ex.: churn gold por dia)."""
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute("START TRANSACTION")
        cur.fetchall()

        cur.execute(f"DELETE FROM {table_fqn} WHERE TRUE")
        cur.fetchall()

        for insert_sql in insert_sqls:
            cur.execute(insert_sql)
            cur.fetchall()

        cur.execute("COMMIT")
        cur.fetchall()
        if logger:
            logger.info(
                "ACID commit: %s replaced atomically (%s INSERT(s))",
                table_fqn,
                len(insert_sqls),
            )
    except Exception:
        try:
            cur.execute("ROLLBACK")
            cur.fetchall()
            if logger:
                logger.warning("ACID rollback: %s unchanged", table_fqn)
        except Exception as rb_exc:
            if logger:
                logger.error("ROLLBACK failed for %s: %s", table_fqn, rb_exc)
        raise
