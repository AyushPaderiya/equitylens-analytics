import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


import streamlit as st
import pandas as pd
from dashboard.components.db       import query
from dashboard.components.filters  import render_header, date_range_filter, sector_filter
from dashboard.components.kpi_cards import render_kpi_row
from dashboard.components.charts   import line_chart, bar_chart, heatmap_chart



render_header(
    "📊 Executive Summary",
    "S&P 500 sector performance, top movers, and market overview"
)

# ── Sidebar filters ───────────────────────────────────────────────────────────
start, end     = date_range_filter(key="exec_date", default_days=252)
sectors        = sector_filter(key="exec_sector")

if not sectors:
    st.warning("Please select at least one sector.")
    st.stop()

# ── KPI Row — market snapshot ─────────────────────────────────────────────────
kpi_df = query("""
    SELECT
        MAX(close)                                          AS spx_latest,
        ROUND(AVG(daily_return) * 100, 2)                  AS avg_daily_return,
        ROUND(AVG(volatility_30d), 2)                      AS avg_vol,
        SUM(CASE WHEN daily_return > 0 THEN 1 ELSE 0 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100                    AS pct_positive
    FROM fact_daily_prices fdp
    JOIN dim_companies dc ON fdp.ticker = dc.ticker
    WHERE dc.is_benchmark = FALSE
      AND fdp.date = (SELECT MAX(date) FROM fact_daily_prices)
""")

vix_df = query("""
    SELECT value AS vix
    FROM fact_macro_indicators
    WHERE series_id = 'VIXCLS'
    ORDER BY date DESC LIMIT 1
""")

fed_df = query("""
    SELECT value AS fed_rate
    FROM fact_macro_indicators
    WHERE series_id = 'FEDFUNDS'
    ORDER BY date DESC LIMIT 1
""")

if not kpi_df.empty:
    row = kpi_df.iloc[0]
    vix     = round(float(vix_df.iloc[0]["vix"]), 2)     if not vix_df.empty else "N/A"
    fed     = round(float(fed_df.iloc[0]["fed_rate"]), 2) if not fed_df.empty else "N/A"
    avg_ret = float(row["avg_daily_return"])

    render_kpi_row([
        {
            "label": "Avg Daily Return",
            "value": f"{avg_ret:+.2f}%",
            "delta": "positive" if avg_ret > 0 else "negative",
            "delta_color": "normal",
            "help": "Equal-weighted avg return across all 29 stocks today",
        },
        {
            "label": "Avg 30D Volatility",
            "value": f"{float(row['avg_vol']):.1f}%",
            "help": "Annualised 30-day rolling volatility",
        },
        {
            "label": "% Stocks Up Today",
            "value": f"{float(row['pct_positive']):.0f}%",
            "help": "Percentage of stocks with positive daily return",
        },
        {
            "label": "VIX (Fear Index)",
            "value": str(vix),
            "help": "CBOE Volatility Index — above 30 = high fear",
        },
        {
            "label": "Fed Funds Rate",
            "value": f"{fed}%",
            "help": "Current Federal Funds Effective Rate",
        },
    ])

st.markdown("---")

# ── Cumulative Returns by Sector ──────────────────────────────────────────────
st.subheader("📈 Cumulative Returns by Sector")

cum_df = query("""
    SELECT date, sector_name,
           ROUND(AVG(cumulative_return_pct), 2) AS cumulative_return_pct
    FROM vw_stock_performance
    WHERE date BETWEEN :start AND :end
      AND sector_name = ANY(:sectors)
    GROUP BY date, sector_name
    ORDER BY date
""", {"start": start, "end": end, "sectors": sectors})

if not cum_df.empty:
    fig = line_chart(
        cum_df, x="date", y="cumulative_return_pct",
        color_col="sector_name",
        title="Cumulative Return % by Sector (Equal-Weighted)",
        y_format="%",
        height=420,
    )
    st.plotly_chart(fig, width='stretch')
else:
    st.info("No data for selected filters.")

# ── Sector Sharpe Ratio Ranking ───────────────────────────────────────────────
st.subheader("🏆 Sector Risk-Adjusted Performance (Sharpe Ratio)")

sharpe_df = query("""
    SELECT sector_name,
           ROUND(AVG(sharpe_30d)::NUMERIC, 3)         AS avg_sharpe,
           ROUND(AVG(avg_volatility_pct)::NUMERIC, 2) AS avg_volatility
    FROM vw_sector_performance
    WHERE date BETWEEN :start AND :end
      AND sector_name = ANY(:sectors)
    GROUP BY sector_name
    ORDER BY avg_sharpe DESC
""", {"start": start, "end": end, "sectors": sectors})

col1, col2 = st.columns(2)

with col1:
    if not sharpe_df.empty:
        fig = bar_chart(
            sharpe_df.sort_values("avg_sharpe"),
            x="avg_sharpe", y="sector_name",
            title="Average 30D Sharpe Ratio by Sector",
            color_col="avg_sharpe",
            height=380,
        )
        fig.update_layout(showlegend=False)
        fig.update_traces(
            marker_color=[
                "#00D4AA" if v >= 0 else "#FF6B6B"
                for v in sharpe_df.sort_values("avg_sharpe")["avg_sharpe"]
            ]
        )
        st.plotly_chart(fig, width='stretch')

with col2:
    if not sharpe_df.empty:
        fig = bar_chart(
            sharpe_df.sort_values("avg_volatility", ascending=False),
            x="avg_volatility", y="sector_name",
            title="Average 30D Annualised Volatility % by Sector",
            height=380,
        )
        st.plotly_chart(fig, width='stretch')

# ── Top & Bottom Movers ───────────────────────────────────────────────────────
st.subheader("🚀 Top & Bottom Movers — Latest Trading Day")

movers_df = query("""
    SELECT ticker, company_name, sector_name,
           daily_return_pct, cumulative_return_pct,
           volatility_30d, market_signal, rsi_14
    FROM vw_stock_performance
    WHERE date = (SELECT MAX(date) FROM vw_stock_performance)
      AND sector_name = ANY(:sectors)
    ORDER BY daily_return_pct DESC
""", {"sectors": sectors})

col1, col2 = st.columns(2)

with col1:
    st.markdown("#### 🟢 Top 5 Gainers")
    if not movers_df.empty:
        top5 = movers_df.head(5)[
            ["ticker", "company_name", "daily_return_pct", "rsi_14", "market_signal"]
        ].reset_index(drop=True)
        st.dataframe(
            top5.style.format({
                "daily_return_pct": "{:+.2f}%",
                "rsi_14": "{:.1f}",
            }).background_gradient(subset=["daily_return_pct"], cmap="Greens"),
            width='stretch',
            hide_index=True,
        )

with col2:
    st.markdown("#### 🔴 Bottom 5 Losers")
    if not movers_df.empty:
        bot5 = movers_df.tail(5).sort_values("daily_return_pct")[
            ["ticker", "company_name", "daily_return_pct", "rsi_14", "market_signal"]
        ].reset_index(drop=True)
        st.dataframe(
            bot5.style.format({
                "daily_return_pct": "{:+.2f}%",
                "rsi_14": "{:.1f}",
            }).background_gradient(subset=["daily_return_pct"], cmap="Reds_r"),
            width='stretch',
            hide_index=True,
        )

# ── Monthly Return Heatmap ────────────────────────────────────────────────────
st.subheader("🗓️ Monthly Return Heatmap by Sector")

heatmap_df = query("""
    SELECT sector_name,
           TO_CHAR(date, 'Mon YY')          AS period,
           DATE_TRUNC('month', date)::DATE  AS month_start,
           ROUND(AVG(avg_daily_return) * 21 * 100, 2) AS monthly_return_pct
    FROM vw_sector_performance
    WHERE date BETWEEN :start AND :end
      AND sector_name = ANY(:sectors)
    GROUP BY sector_name, TO_CHAR(date, 'Mon YY'), DATE_TRUNC('month', date)::DATE
    ORDER BY month_start, sector_name
""", {"start": start, "end": end, "sectors": sectors})

if not heatmap_df.empty:
    fig = heatmap_chart(
        heatmap_df,
        x="period", y="sector_name",
        z="monthly_return_pct",
        title="Estimated Monthly Return % by Sector",
        height=420,
    )
    st.plotly_chart(fig, width='stretch')

