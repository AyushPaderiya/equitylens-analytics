import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dashboard.components.db      import query
from dashboard.components.filters import render_header, date_range_filter, sector_filter
from dashboard.components.charts  import line_chart, scatter_chart
from dashboard.components.kpi_cards import render_kpi_row



render_header(
    "🌍 Macro Overlay",
    "Federal Reserve policy, inflation, and treasury yields vs market performance"
)

# ── Filters ───────────────────────────────────────────────────────────────────
start, end = date_range_filter(key="macro_date", default_days=365)
sectors    = sector_filter(key="macro_sector")

# ── Macro KPI row ─────────────────────────────────────────────────────────────
macro_kpi = query("""
    SELECT
        MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi,
        MAX(CASE WHEN series_id = 'GS10'     THEN value END) AS treasury_10y,
        MAX(CASE WHEN series_id = 'UNRATE'   THEN value END) AS unemployment,
        MAX(CASE WHEN series_id = 'VIXCLS'   THEN value END) AS vix
    FROM fact_macro_indicators
    WHERE date = (
        SELECT MAX(date) FROM fact_macro_indicators
        WHERE series_id = 'FEDFUNDS'
    )
""")

if not macro_kpi.empty:
    r = macro_kpi.iloc[0]
    render_kpi_row([
        {
            "label": "Fed Funds Rate",
            "value": f"{float(r['fed_rate'] or 0):.2f}%",
            "help":  "Current Federal Funds Effective Rate",
        },
        {
            "label": "CPI Index",
            "value": f"{float(r['cpi'] or 0):.1f}",
            "help":  "Consumer Price Index (1982-84 = 100)",
        },
        {
            "label": "10Y Treasury Yield",
            "value": f"{float(r['treasury_10y'] or 0):.2f}%",
            "help":  "10-Year US Treasury Constant Maturity Rate",
        },
        {
            "label": "Unemployment Rate",
            "value": f"{float(r['unemployment'] or 0):.1f}%",
            "help":  "US Unemployment Rate",
        },
        {
            "label": "VIX",
            "value": f"{float(r['vix'] or 0):.1f}",
            "help":  "CBOE Volatility Index — fear gauge",
        },
    ])

st.markdown("---")

# ── Fed Rate + Market Performance (dual axis) ─────────────────────────────────
st.subheader("🏦 Fed Funds Rate vs S&P 500 Sector Returns")

macro_df = query("""
    SELECT month, sector_name,
           avg_daily_return,
           fed_funds_rate,
           treasury_10y,
           cpi,
           vix
    FROM vw_macro_overlay
    WHERE month BETWEEN :start AND :end
      AND sector_name = ANY(:sectors)
    ORDER BY month
""", {"start": start, "end": end, "sectors": sectors})

if not macro_df.empty:
    # Dual axis: sector returns (left) + fed rate (right)
    sectors_list = macro_df["sector_name"].unique().tolist()

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    import plotly.express as px
    colors = px.colors.qualitative.Set2

    for i, sec in enumerate(sectors_list):
        sec_data = macro_df[macro_df["sector_name"] == sec]
        # Monthly return approximation
        sec_data = sec_data.copy()
        sec_data["monthly_return_pct"] = sec_data["avg_daily_return"] * 21 * 100
        fig.add_trace(
            go.Scatter(
                x=sec_data["month"],
                y=sec_data["monthly_return_pct"],
                name=sec,
                line=dict(color=colors[i % len(colors)], width=1.5),
                opacity=0.8,
            ),
            secondary_y=False,
        )

    # Fed rate overlay
    fed_data = macro_df[macro_df["sector_name"] == sectors_list[0]]
    fig.add_trace(
        go.Scatter(
            x=fed_data["month"],
            y=fed_data["fed_funds_rate"],
            name="Fed Funds Rate",
            line=dict(color="#FF6B6B", width=2.5, dash="dot"),
        ),
        secondary_y=True,
    )

    fig.update_layout(
        paper_bgcolor="#0E1117",
        plot_bgcolor="#0E1117",
        font=dict(color="#FAFAFA"),
        height=440,
        margin=dict(l=40, r=60, t=50, b=40),
        legend=dict(bgcolor="#1A1F2E", bordercolor="#2A2F3E"),
        xaxis=dict(gridcolor="#2A2F3E"),
        yaxis=dict(gridcolor="#2A2F3E", title="Monthly Return %"),
        title="Sector Monthly Returns vs Fed Funds Rate",
        title_font_size=15,
    )
    fig.update_yaxes(title_text="Fed Funds Rate %", secondary_y=True)
    st.plotly_chart(fig, width='stretch')

# ── VIX vs Market Volatility ──────────────────────────────────────────────────
st.subheader("😨 VIX (Fear Index) vs Market Volatility")

col1, col2 = st.columns(2)

with col1:
    vix_df = query("""
        SELECT date, value AS vix
        FROM fact_macro_indicators
        WHERE series_id = 'VIXCLS'
          AND date BETWEEN :start AND :end
        ORDER BY date
    """, {"start": start, "end": end})

    if not vix_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=vix_df["date"], y=vix_df["vix"],
            fill="tozeroy",
            line=dict(color="#FF6B6B", width=1.5),
            fillcolor="rgba(255,107,107,0.15)",
            name="VIX",
        ))
        # Threshold lines
        fig.add_hline(y=20, line_dash="dash",
                      line_color="#FFD700", annotation_text="20 — Elevated")
        fig.add_hline(y=30, line_dash="dash",
                      line_color="#FF6B6B", annotation_text="30 — High Fear")

        fig.update_layout(
            paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
            font=dict(color="#FAFAFA"),
            xaxis=dict(gridcolor="#2A2F3E"),
            yaxis=dict(gridcolor="#2A2F3E"),
            height=360, margin=dict(l=40, r=20, t=40, b=40),
            title="VIX Daily — Fear Index",
        )
        st.plotly_chart(fig, width='stretch')

with col2:
    # Correlation: VIX vs Tech volatility
    corr_df = query("""
        SELECT mo.month,
               mo.vix,
               mo.avg_volatility_pct
        FROM vw_macro_overlay mo
        WHERE mo.sector_name = 'Technology'
          AND mo.month BETWEEN :start AND :end
          AND mo.vix IS NOT NULL
        ORDER BY mo.month
    """, {"start": start, "end": end})

    if not corr_df.empty:
        corr_val = corr_df["vix"].corr(corr_df["avg_volatility_pct"])
        fig = scatter_chart(
            corr_df,
            x="vix", y="avg_volatility_pct",
            title=f"VIX vs Tech Volatility (r = {corr_val:.2f})",
            height=360,
        )
        fig.update_traces(marker_color="#00D4AA", marker_size=7)
        st.plotly_chart(fig, width='stretch')

# ── CPI + Treasury Yield ──────────────────────────────────────────────────────
st.subheader("📊 CPI Trend & 10-Year Treasury Yield")

macro_series_df = query("""
    SELECT date, series_id, value
    FROM fact_macro_indicators
    WHERE series_id IN ('CPIAUCSL', 'GS10', 'FEDFUNDS')
      AND date BETWEEN :start AND :end
    ORDER BY date
""", {"start": start, "end": end})

if not macro_series_df.empty:
    fig = make_subplots(
        rows=1, cols=3,
        subplot_titles=("CPI Index", "10Y Treasury Yield %", "Fed Funds Rate %"),
    )

    series_config = {
        "CPIAUCSL": (1, "#00D4AA"),
        "GS10":     (2, "#FFD700"),
        "FEDFUNDS": (3, "#FF6B6B"),
    }

    for sid, (col_num, color) in series_config.items():
        s_df = macro_series_df[macro_series_df["series_id"] == sid]
        if not s_df.empty:
            fig.add_trace(
                go.Scatter(
                    x=s_df["date"], y=s_df["value"],
                    name=sid,
                    line=dict(color=color, width=2),
                    fill="tozeroy",
                    fillcolor=f"rgba{tuple(list(int(color.lstrip('#')[i:i+2], 16) for i in (0,2,4)) + [0.1])}",
                ),
                row=1, col=col_num,
            )

    fig.update_layout(
        paper_bgcolor="#0E1117", plot_bgcolor="#0E1117",
        font=dict(color="#FAFAFA"),
        height=340,
        margin=dict(l=40, r=20, t=50, b=40),
        showlegend=False,
    )
    for i in range(1, 4):
        fig.update_xaxes(gridcolor="#2A2F3E", row=1, col=i)
        fig.update_yaxes(gridcolor="#2A2F3E", row=1, col=i)

    st.plotly_chart(fig, width='stretch')
