import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

"""
EquityLens Analytics — Streamlit entry point.
Run with: streamlit run dashboard/app.py

Multi-page app using Streamlit's native pages system.
Each page in dashboard/pages/ is auto-discovered.
"""

import streamlit as st

st.set_page_config(
    page_title     = "EquityLens Analytics",
    page_icon      = "📈",
    layout         = "wide",
    initial_sidebar_state = "expanded",
)

# ── Landing page ─────────────────────────────────────────────────────────────
st.markdown("""
<div style='text-align: center; padding: 3rem 0 1rem 0;'>
    <h1 style='color: #00D4AA; font-size: 3rem; margin-bottom: 0;'>
        📈 EquityLens Analytics
    </h1>
    <p style='color: #888; font-size: 1.1rem; margin-top: 0.5rem;'>
        S&P 500 Market Intelligence Platform
    </p>
    <hr style='border-color: #1A1F2E; margin: 1.5rem auto; width: 60%;'>
</div>
""", unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div style='background:#1A1F2E; padding:1.5rem; border-radius:8px;
                border-left: 3px solid #00D4AA;'>
        <h3 style='color:#00D4AA; margin:0;'>📊 Executive Summary</h3>
        <p style='color:#888; font-size:0.85rem; margin:0.5rem 0 0 0;'>
        Sector performance, top movers, market overview
        </p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown("""
    <div style='background:#1A1F2E; padding:1.5rem; border-radius:8px;
                border-left: 3px solid #FFD700;'>
        <h3 style='color:#FFD700; margin:0;'>⚠️ Sector Risk</h3>
        <p style='color:#888; font-size:0.85rem; margin:0.5rem 0 0 0;'>
        Volatility, drawdown, RSI signals, risk metrics
        </p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown("""
    <div style='background:#1A1F2E; padding:1.5rem; border-radius:8px;
                border-left: 3px solid #FF6B6B;'>
        <h3 style='color:#FF6B6B; margin:0;'>🌍 Macro Overlay</h3>
        <p style='color:#888; font-size:0.85rem; margin:0.5rem 0 0 0;'>
        Fed rates, CPI, VIX vs market performance
        </p>
    </div>
    """, unsafe_allow_html=True)

with col4:
    st.markdown("""
    <div style='background:#1A1F2E; padding:1.5rem; border-radius:8px;
                border-left: 3px solid #888;'>
        <h3 style='color:#888; margin:0;'>🔬 Fundamentals</h3>
        <p style='color:#888; font-size:0.85rem; margin:0.5rem 0 0 0;'>
        Quality scores, value traps, P/E analysis
        </p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("""
<div style='text-align:center; margin-top: 2rem; color: #555; font-size:0.8rem;'>
    Data sourced from yfinance · FRED · Alpha Vantage &nbsp;|&nbsp;
    Built with Python, PostgreSQL, Streamlit &nbsp;|&nbsp;
    Refreshed daily after NYSE close
</div>
""", unsafe_allow_html=True)
