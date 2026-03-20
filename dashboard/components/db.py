import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

"""
Cached database connection for Streamlit.
st.cache_resource ensures one connection pool
is shared across all dashboard pages and reruns.

Production context: In a real BI tool this would be
a connection pool manager (pgBouncer) sitting between
the dashboard and PostgreSQL, handling hundreds of
concurrent users. For our single-user dashboard,
SQLAlchemy's built-in pool is sufficient.
"""

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

from config.settings import settings


@st.cache_resource
def get_engine():
    """
    Create and cache the SQLAlchemy engine.
    Called once per Streamlit session — not per page render.
    """
    return create_engine(
        settings.DATABASE_URL,
        pool_size=3,
        max_overflow=1,
        pool_pre_ping=True,
        connect_args={"sslmode": "require"},
    )


def query(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Execute SQL and return a DataFrame with numeric columns auto-cast.
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

            # ── Auto-cast: try numeric conversion on every column ─────────────
            # SQLAlchemy NUMERIC/DECIMAL columns arrive as Python Decimal objects
            # which pandas stores as object dtype. This converts them silently.
            for col in df.columns:
                if df[col].dtype == object:
                    try:
                        converted = pd.to_numeric(df[col], errors="ignore")
                        # Only apply if conversion actually changed the dtype
                        if converted.dtype != object:
                            df[col] = converted
                    except Exception:
                        pass

            return df
    except Exception as exc:
        st.error(f"Database query failed: {exc}")
        return pd.DataFrame()
