import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import streamlit as st
import pandas as pd
from dashboard.components.db      import query
from dashboard.components.filters import render_header, sector_filter
from dashboard.components.charts  import scatter_chart, bar_chart
from dashboard.components.kpi_cards import render_kpi_row, signal_badge



render_header(
    "🔬 Fundamental Scorecard",
    "Quality scores, value trap detection, and sector-relative valuation"
)

# ── Filters ───────────────────────────────────────────────────────────────────
sectors = sector_filter(key="fund_sector")

signal_options = [
    "All", "high_quality", "moderate_quality",
    "value_trap_risk", "low_quality"
]
st.sidebar.markdown("### 🎯 Signal Filter")
signal_filter = st.sidebar.selectbox(
    "Fundamental Signal",
    options=signal_options,
    key="fund_signal",
)

# ── Load data ─────────────────────────────────────────────────────────────────
fund_df = query("""
    SELECT *
    FROM vw_fundamental_scorecard
    WHERE sector_name = ANY(:sectors)
    ORDER BY quality_score DESC
""", {"sectors": sectors})

if fund_df.empty:
    st.warning("No fundamental data available.")
    st.stop()

# ── Cast all numeric columns — SQLAlchemy returns them as object dtype ────────
numeric_cols = [
    "quality_score", "pe_ratio", "forward_pe", "sector_median_pe",
    "pe_vs_sector_median", "price_to_book", "revenue_growth_pct",
    "earnings_growth_pct", "profit_margin_pct", "operating_margin_pct",
    "roe_pct", "roa_pct", "debt_to_equity", "net_cash_billions",
    "current_ratio", "beta", "dividend_yield_pct", "market_cap_billions",
    "latest_close", "volatility_30d", "rsi_14", "price_52w_position_pct",
]
for col in numeric_cols:
    if col in fund_df.columns:
        fund_df[col] = pd.to_numeric(fund_df[col], errors="coerce")

# Apply signal filter
if signal_filter != "All":
    fund_df = fund_df[fund_df["fundamental_signal"] == signal_filter]

# ── KPI row ───────────────────────────────────────────────────────────────────
render_kpi_row([
    {
        "label": "Companies Analysed",
        "value": str(len(fund_df)),
    },
    {
        "label": "High Quality",
        "value": str((fund_df["fundamental_signal"] == "high_quality").sum()),
        "help":  "Quality score ≥ 60, no value trap flag",
    },
    {
        "label": "Value Trap Risks",
        "value": str(fund_df["value_trap_flag"].sum()),
        "help":  "P/E > 30 with revenue growth < 5%",
        "delta_color": "inverse",
    },
    {
        "label": "Avg Quality Score",
        "value": f"{fund_df['quality_score'].mean():.1f}/100",
        "help":  "Composite: ROE + margins + debt safety + growth",
    },
    {
        "label": "Avg P/E Ratio",
        "value": f"{fund_df['pe_ratio'].dropna().mean():.1f}x",
        "help":  "Equal-weighted average trailing P/E",
    },
])

st.markdown("---")

# ── Scorecard table ───────────────────────────────────────────────────────────
st.subheader("📋 Full Fundamental Scorecard")

display_cols = [
    "ticker", "company_name", "sector_name", "market_cap_billions",
    "quality_score", "pe_ratio", "pe_vs_sector_median",
    "revenue_growth_pct", "profit_margin_pct", "roe_pct",
    "debt_to_equity", "fundamental_signal", "value_trap_flag",
]

available = [c for c in display_cols if c in fund_df.columns]
display_df = fund_df[available].copy()

st.dataframe(
    display_df.style.format({
        "market_cap_billions":  "${:.1f}B",
        "quality_score":        "{:.1f}",
        "pe_ratio":             "{:.1f}x",
        "pe_vs_sector_median":  "{:+.1f}",
        "revenue_growth_pct":   "{:+.1f}%",
        "profit_margin_pct":    "{:.1f}%",
        "roe_pct":              "{:.1f}%",
        "debt_to_equity":       "{:.1f}",
        }, na_rep="N/A").background_gradient(
        subset=["quality_score"], cmap="RdYlGn", vmin=0, vmax=100
    ).background_gradient(
        subset=["pe_ratio"], cmap="RdYlGn_r", vmin=0, vmax=60
    ),
    width='stretch',
    hide_index=True,
    height=420,
)

st.markdown("---")

# ── P/E vs Revenue Growth scatter ────────────────────────────────────────────
st.subheader("💰 P/E Ratio vs Revenue Growth — Value Trap Detector")

scatter_df = fund_df.dropna(subset=["pe_ratio", "revenue_growth_pct"])

if not scatter_df.empty:
    fig = scatter_chart(
        scatter_df,
        x="revenue_growth_pct",
        y="pe_ratio",
        color_col="sector_name",
        size_col="market_cap_billions",
        text_col="ticker",
        title="P/E Ratio vs Revenue Growth % — Bubble size = Market Cap",
        height=460,
    )
    # Value trap quadrant shading
    fig.add_hrect(
        y0=30, y1=scatter_df["pe_ratio"].max() + 10,
        x0=scatter_df["revenue_growth_pct"].min() - 1, x1=5,
        fillcolor="rgba(255,75,75,0.08)",
        line_width=0,
        annotation_text="⚠️ Value Trap Zone",
        annotation_position="top left",
        annotation_font_color="#FF6B6B",
    )
    fig.add_hline(y=30, line_dash="dash", line_color="#FF6B6B",
                  annotation_text="P/E = 30")
    fig.add_vline(x=5,  line_dash="dash", line_color="#FFD700",
                  annotation_text="5% Revenue Growth")
    st.plotly_chart(fig, width='stretch')

# ── Quality Score ranking ─────────────────────────────────────────────────────
st.subheader("🏆 Quality Score Ranking by Company")

col1, col2 = st.columns(2)

with col1:
    top_quality = fund_df.nlargest(10, "quality_score")[
        ["ticker", "sector_name", "quality_score", "fundamental_signal"]
    ].reset_index(drop=True)

    fig = bar_chart(
        top_quality.sort_values("quality_score"),
        x="quality_score", y="ticker",
        title="Top 10 — Highest Quality Score",
        height=380,
    )
    fig.update_traces(marker_color="#00D4AA")
    fig.update_xaxes(range=[0, 100])
    st.plotly_chart(fig, width='stretch')

with col2:
    # ROE vs Debt-to-Equity scatter
    roe_df = fund_df.dropna(subset=["roe_pct", "debt_to_equity"])
    if not roe_df.empty:
        fig = scatter_chart(
            roe_df,
            x="debt_to_equity", y="roe_pct",
            color_col="sector_name",
            text_col="ticker",
            title="Return on Equity vs Debt-to-Equity",
            height=380,
        )
        fig.add_vline(x=200, line_dash="dash", line_color="#FF6B6B",
                      annotation_text="High Debt")
        fig.add_hline(y=0, line_dash="dash", line_color="#555")
        st.plotly_chart(fig, width='stretch')

# ── Sector fundamental comparison ────────────────────────────────────────────
st.subheader("📊 Sector Fundamental Comparison")

sector_avg = fund_df.groupby("sector_name").agg(
    avg_quality     = ("quality_score",    "mean"),
    avg_pe          = ("pe_ratio",         "mean"),
    avg_roe         = ("roe_pct",          "mean"),
    avg_margin      = ("profit_margin_pct","mean"),
    value_trap_count= ("value_trap_flag",  "sum"),
).round(2).reset_index()

col1, col2, col3 = st.columns(3)

with col1:
    fig = bar_chart(
        sector_avg.sort_values("avg_quality"),
        x="avg_quality", y="sector_name",
        title="Avg Quality Score by Sector",
        height=340,
    )
    fig.update_traces(marker_color="#00D4AA")
    st.plotly_chart(fig, width='stretch')

with col2:
    fig = bar_chart(
        sector_avg.sort_values("avg_roe"),
        x="avg_roe", y="sector_name",
        title="Avg ROE % by Sector",
        height=340,
    )
    fig.update_traces(marker_color="#FFD700")
    st.plotly_chart(fig, width='stretch')

with col3:
    fig = bar_chart(
        sector_avg.sort_values("avg_margin"),
        x="avg_margin", y="sector_name",
        title="Avg Profit Margin % by Sector",
        height=340,
    )
    fig.update_traces(marker_color="#FF6B6B")
    st.plotly_chart(fig, width='stretch')

# ── 52-Week position table ────────────────────────────────────────────────────
st.subheader("📍 52-Week Price Position")

pos_df = fund_df.dropna(subset=["price_52w_position_pct"])[
    ["ticker", "company_name", "sector_name",
     "latest_close", "price_52w_position_pct",
     "rsi_14", "fundamental_signal"]
].sort_values("price_52w_position_pct", ascending=False).reset_index(drop=True)

st.dataframe(
    pos_df.style.format({
        "latest_close":          "${:.2f}",
        "price_52w_position_pct":"{:.1f}%",
        "rsi_14":                "{:.1f}",
    }, na_rep="N/A").background_gradient(
        subset=["price_52w_position_pct"], cmap="RdYlGn", vmin=0, vmax=100
    ),
    width='stretch',
    hide_index=True,
)

