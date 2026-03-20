import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd
from dashboard.components.db      import query
from dashboard.components.filters import render_header, date_range_filter, ticker_filter
from dashboard.components.charts  import (
    candlestick_chart, line_chart, gauge_chart, scatter_chart
)
from dashboard.components.kpi_cards import render_kpi_row

st.set_page_config(page_title="Sector Risk", page_icon="⚠️", layout="wide")

render_header(
    "⚠️ Sector Risk Analysis",
    "Volatility trends, drawdown, RSI signals, and volume anomalies"
)

# ── Filters ───────────────────────────────────────────────────────────────────
start, end = date_range_filter(key="risk_date", default_days=180)
ticker     = ticker_filter(key="risk_ticker", multi=False, default="AAPL")

# ── Stock KPI row ─────────────────────────────────────────────────────────────
stock_kpi = query("""
    SELECT ticker, company_name, sector_name,
           close, daily_return_pct, cumulative_return_pct,
           volatility_30d, rsi_14, drawdown_from_ath_pct,
           market_signal, golden_cross
    FROM vw_stock_performance
    WHERE ticker = :ticker
    ORDER BY date DESC LIMIT 1
""", {"ticker": ticker})

if not stock_kpi.empty:
    r = stock_kpi.iloc[0]
    render_kpi_row([
        {
            "label": "Latest Close",
            "value": f"${float(r['close']):,.2f}",
            "delta": f"{float(r['daily_return_pct']):+.2f}% today",
            "delta_color": "normal",
        },
        {
            "label": "Cumulative Return",
            "value": f"{float(r['cumulative_return_pct']):+.1f}%",
            "help": "Total return since Jan 2022",
        },
        {
            "label": "30D Volatility",
            "value": f"{float(r['volatility_30d']):.1f}%",
            "help": "Annualised 30-day rolling volatility",
        },
        {
            "label": "Drawdown from ATH",
            "value": f"{float(r['drawdown_from_ath_pct']):.1f}%",
            "help": "How far below all-time high",
            "delta_color": "inverse",
        },
        {
            "label": "Market Signal",
            "value": str(r["market_signal"]).replace("_", " ").title(),
            "help": "Based on golden cross + RSI",
        },
    ])

st.markdown("---")

# ── Candlestick chart ─────────────────────────────────────────────────────────
st.subheader(f"🕯️ {ticker} Price Chart (OHLCV + SMA50/200)")

candle_df = query("""
    SELECT date, open, high, low, close, volume,
           daily_return, sma_50, sma_200, golden_cross
    FROM fact_daily_prices fdp
    JOIN dim_companies dc ON fdp.ticker = dc.ticker
    WHERE fdp.ticker = :ticker
      AND fdp.date BETWEEN :start AND :end
    ORDER BY date
""", {"ticker": ticker, "start": start, "end": end})

if not candle_df.empty:
    fig = candlestick_chart(candle_df, ticker, height=480)
    st.plotly_chart(fig, use_container_width=True)

# ── RSI + MACD row ────────────────────────────────────────────────────────────
col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("📡 RSI Signal")
    rsi_val = float(stock_kpi.iloc[0]["rsi_14"]) if not stock_kpi.empty else 50
    fig = gauge_chart(
        value=rsi_val,
        title=f"RSI(14) — {ticker}",
        min_val=0, max_val=100,
        height=280,
    )
    st.plotly_chart(fig, use_container_width=True)
    if rsi_val < 30:
        st.success("🟢 Oversold — potential buy signal")
    elif rsi_val > 70:
        st.warning("🟡 Overbought — potential sell signal")
    else:
        st.info("⚪ RSI in neutral zone")

with col2:
    st.subheader("📊 MACD")
    macd_df = query("""
        SELECT date, macd, macd_signal, macd_hist
        FROM fact_daily_prices
        WHERE ticker = :ticker
          AND date BETWEEN :start AND :end
          AND macd IS NOT NULL
        ORDER BY date
    """, {"ticker": ticker, "start": start, "end": end})

    if not macd_df.empty:
        import plotly.graph_objects as go
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=macd_df["date"], y=macd_df["macd"],
            name="MACD", line=dict(color="#00D4AA", width=1.5)
        ))
        fig.add_trace(go.Scatter(
            x=macd_df["date"], y=macd_df["macd_signal"],
            name="Signal", line=dict(color="#FF6B6B", width=1.5)
        ))
        fig.add_trace(go.Bar(
            x=macd_df["date"], y=macd_df["macd_hist"],
            name="Histogram",
            marker_color=[
                "#00D4AA" if v >= 0 else "#FF6B6B"
                for v in macd_df["macd_hist"]
            ],
        ))
        fig.update_layout(
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="#FAFAFA"),
            margin=dict(l=40, r=20, t=30, b=40),
            height=280,
            legend=dict(bgcolor="#1A1F2E"),
            xaxis=dict(gridcolor="#2A2F3E"),
            yaxis=dict(gridcolor="#2A2F3E"),
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Volatility comparison ─────────────────────────────────────────────────────
st.subheader("📉 30D Rolling Volatility — Sector Comparison")

vol_df = query("""
    SELECT date, sector_name,
           ROUND(AVG(volatility_30d)::NUMERIC, 2) AS avg_volatility
    FROM vw_stock_performance
    WHERE date BETWEEN :start AND :end
    GROUP BY date, sector_name
    ORDER BY date
""", {"start": start, "end": end})

if not vol_df.empty:
    fig = line_chart(
        vol_df, x="date", y="avg_volatility",
        color_col="sector_name",
        title="30D Annualised Volatility % by Sector",
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Risk vs Return scatter ────────────────────────────────────────────────────
st.subheader("🎯 Risk vs Return — All Stocks")

scatter_df = query("""
    SELECT ticker, sector_name,
           ROUND(AVG(volatility_30d)::NUMERIC, 2)        AS avg_volatility,
           ROUND(AVG(daily_return_pct)::NUMERIC, 4)      AS avg_daily_return,
           ROUND(AVG(cumulative_return_pct)::NUMERIC, 2) AS cum_return
    FROM vw_stock_performance
    WHERE date BETWEEN :start AND :end
    GROUP BY ticker, sector_name
""", {"start": start, "end": end})

# Cast numeric columns
for col in ["avg_volatility", "avg_daily_return", "cum_return"]:
    if col in scatter_df.columns:
        scatter_df[col] = pd.to_numeric(scatter_df[col], errors="coerce")

if not scatter_df.empty:
    fig = scatter_chart(
        scatter_df,
        x="avg_volatility", y="avg_daily_return",
        color_col="sector_name",
        size_col="cum_return",
        text_col="ticker",
        title="Risk (Volatility) vs Return — Bubble size = Cumulative Return",
        height=440,
    )
    fig.add_hline(y=0, line_dash="dash", line_color="#555")
    st.plotly_chart(fig, use_container_width=True)
