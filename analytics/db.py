"""
db.py — PostgreSQL connection helper for analytics dashboards.
"""
from __future__ import annotations

import psycopg2
import pandas as pd
from contextlib import contextmanager
from typing import Any, Dict


@contextmanager
def get_connection(cfg: Dict[str, Any]):
    """Yield a psycopg2 connection from config dict."""
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
        connect_timeout=10,
    )
    try:
        yield conn
    finally:
        conn.close()


def run_query(cfg: Dict[str, Any], sql: str, params=None) -> pd.DataFrame:
    """Execute SQL and return a pandas DataFrame."""
    with get_connection(cfg) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        if cur.description is None:
            cur.close()
            return pd.DataFrame()
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame(data, columns=columns)
