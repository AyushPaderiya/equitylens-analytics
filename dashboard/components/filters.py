"""
Reusable sidebar filter components.
Every page imports what it needs from here.
Keeps filter logic in one place — change once, applies everywhere.
"""

from datetime import date, timedelta
import streamlit as st
from dashboard.components.db import query


def render_header(title: str, subtitle: str) -> None:
    """Consistent page header across all dashboard pages."""
    st.markdown(
        f"""
        <div style='padding: 1rem 0 0.5rem 0;'>
            <h1 style='color: #00D4AA; margin-bottom: 0.2rem;'>{title}</h1>
            <p style='color: #888; font-size: 0.95rem; margin-top: 0;'>{subtitle}</p>
            <hr style='border-color: #1A1F2E; margin-top: 0.5rem;'>
        </div>
        """,
        unsafe_allow_html=True,
    )


def date_range_filter(
    key: str = "date_range",
    default_days: int = 252
) -> tuple:
    """
    Sidebar date range picker.
    Returns (start_date, end_date) as date objects.
    """
    st.sidebar.markdown("### 📅 Date Range")
    end_default   = date.today()
    start_default = end_default - timedelta(days=default_days)

    col1, col2 = st.sidebar.columns(2)
    with col1:
        start = st.date_input("From", value=start_default, key=f"{key}_start")
    with col2:
        end = st.date_input("To", value=end_default, key=f"{key}_end")

    return start, end


def sector_filter(key: str = "sector", multi: bool = True):
    """
    Sector multi-select filter.
    Returns list of selected sector names.
    """
    df = query("SELECT sector_name FROM dim_sectors ORDER BY sector_name")
    sectors = df["sector_name"].tolist() if not df.empty else []

    st.sidebar.markdown("### 🏢 Sector")
    if multi:
        selected = st.sidebar.multiselect(
            "Select sectors",
            options=sectors,
            default=sectors,
            key=key,
        )
    else:
        selected = st.sidebar.selectbox(
            "Select sector",
            options=sectors,
            key=key,
        )
    return selected


def ticker_filter(
    key: str = "ticker",
    multi: bool = False,
    default: str = "AAPL"
) -> str:
    """
    Ticker selector — single or multi.
    Returns selected ticker string or list.
    """
    df = query("""
        SELECT ticker, company_name
        FROM dim_companies
        WHERE is_benchmark = FALSE
        ORDER BY ticker
    """)

    options = df["ticker"].tolist() if not df.empty else []
    labels  = {
        row.ticker: f"{row.ticker} — {row.company_name}"
        for _, row in df.iterrows()
    } if not df.empty else {}

    st.sidebar.markdown("### 📈 Stock")
    if multi:
        selected = st.sidebar.multiselect(
            "Select tickers",
            options=options,
            default=options[:5],
            format_func=lambda x: labels.get(x, x),
            key=key,
        )
    else:
        idx = options.index(default) if default in options else 0
        selected = st.sidebar.selectbox(
            "Select ticker",
            options=options,
            index=idx,
            format_func=lambda x: labels.get(x, x),
            key=key,
        )
    return selected
