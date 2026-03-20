"""
Reusable Plotly chart factory functions.
Every chart in the dashboard is built here — pages
just call the function with their data.

Design principles:
- Dark theme consistent with .streamlit/config.toml
- Branded color palette (#00D4AA primary)
- All charts return fig objects — never render inside this module
"""

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Optional

# ── Brand palette ────────────────────────────────────────────────────────────
COLORS = {
    "primary":    "#00D4AA",
    "secondary":  "#FF6B6B",
    "warning":    "#FFD700",
    "neutral":    "#888888",
    "bg":         "#0E1117",
    "card_bg":    "#1A1F2E",
    "grid":       "#2A2F3E",
    "text":       "#FAFAFA",
}

SECTOR_COLORS = px.colors.qualitative.Set2

LAYOUT_BASE = dict(
    paper_bgcolor = COLORS["bg"],
    plot_bgcolor  = COLORS["bg"],
    font          = dict(color=COLORS["text"], family="sans-serif"),
    margin        = dict(l=40, r=20, t=50, b=40),
    xaxis         = dict(gridcolor=COLORS["grid"], showgrid=True),
    yaxis         = dict(gridcolor=COLORS["grid"], showgrid=True),
    legend        = dict(bgcolor=COLORS["card_bg"], bordercolor=COLORS["grid"]),
)


def line_chart(
    df: pd.DataFrame,
    x: str,
    y: str | list,
    title: str,
    color_col: Optional[str] = None,
    y_format: str = "",
    height: int = 420,
) -> go.Figure:
    """
    Multi-line time series chart.
    Used for: price history, cumulative returns, macro overlay.
    """
    if color_col:
        fig = px.line(
            df, x=x, y=y, color=color_col,
            title=title,
            color_discrete_sequence=SECTOR_COLORS,
            height=height,
        )
    else:
        fig = px.line(
            df, x=x, y=y,
            title=title,
            color_discrete_sequence=[COLORS["primary"]],
            height=height,
        )

    fig.update_layout(**LAYOUT_BASE, title_font_size=15)
    fig.update_traces(line_width=1.8)

    if y_format == "%":
        fig.update_yaxes(tickformat=".1%")
    elif y_format == "$":
        fig.update_yaxes(tickprefix="$")

    return fig


def bar_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color_col: Optional[str] = None,
    color_map: Optional[dict] = None,
    height: int = 380,
) -> go.Figure:
    """
    Bar chart. Used for: sector comparison, volume rankings.
    """
    fig = px.bar(
        df, x=x, y=y,
        title=title,
        color=color_col,
        color_discrete_map=color_map,
        color_discrete_sequence=SECTOR_COLORS,
        height=height,
    )
    fig.update_layout(**LAYOUT_BASE, title_font_size=15)
    return fig


def candlestick_chart(
    df: pd.DataFrame,
    ticker: str,
    height: int = 460,
) -> go.Figure:
    """
    OHLCV candlestick with volume subplot.
    Used for: individual stock deep dive.
    """
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.06,
        row_heights=[0.75, 0.25],
        subplot_titles=(f"{ticker} Price", "Volume"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df["date"],
            open=df["open"], high=df["high"],
            low=df["low"],   close=df["close"],
            name="OHLC",
            increasing_line_color=COLORS["primary"],
            decreasing_line_color=COLORS["secondary"],
        ),
        row=1, col=1,
    )

    # SMA overlays
    if "sma_50" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["sma_50"],
                name="SMA 50", line=dict(color=COLORS["warning"], width=1.2),
            ),
            row=1, col=1,
        )
    if "sma_200" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df["date"], y=df["sma_200"],
                name="SMA 200", line=dict(color=COLORS["secondary"], width=1.2),
            ),
            row=1, col=1,
        )

    # Volume bars
    colors = [
        COLORS["primary"] if r >= 0 else COLORS["secondary"]
        for r in df["daily_return"].fillna(0)
    ]
    fig.add_trace(
        go.Bar(x=df["date"], y=df["volume"], name="Volume", marker_color=colors),
        row=2, col=1,
    )

    fig.update_layout(
        **LAYOUT_BASE,
        height=height,
        xaxis_rangeslider_visible=False,
        showlegend=True,
    )
    return fig


def heatmap_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    z: str,
    title: str,
    height: int = 420,
) -> go.Figure:
    """
    Pivot heatmap. Used for: sector × month return heatmap.
    """
    pivot = df.pivot_table(index=y, columns=x, values=z, aggfunc="mean")

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[
                [0.0,  "#FF4B4B"],
                [0.5,  "#1A1F2E"],
                [1.0,  "#00D4AA"],
            ],
            zmid=0,
            text=[[f"{v:.1f}%" if v == v else "" for v in row]
                  for row in pivot.values],
            texttemplate="%{text}",
            hovertemplate="%{y} | %{x}<br>Value: %{z:.4f}<extra></extra>",
        )
    )
    fig.update_layout(**LAYOUT_BASE, title=title, title_font_size=15, height=height)
    return fig


def scatter_chart(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    color_col: Optional[str] = None,
    size_col:  Optional[str] = None,
    text_col:  Optional[str] = None,
    height: int = 420,
) -> go.Figure:
    """
    Scatter plot. Built with go.Figure instead of px.scatter
    to avoid narwhals/plotly Series compatibility issue.
    """
    import numpy as np

    # ── Pre-compute size as plain numpy array ─────────────────────────────────
    if size_col and size_col in df.columns:
        raw = pd.to_numeric(df[size_col], errors="coerce").fillna(1.0)
        min_v, max_v = raw.min(), raw.max()
        if max_v > min_v:
            sizes = ((raw - min_v) / (max_v - min_v) * 35 + 5).to_numpy()
        else:
            sizes = np.full(len(df), 15.0)
    else:
        sizes = np.full(len(df), 10.0)

    # ── Build one trace per color group ──────────────────────────────────────
    fig = go.Figure()

    if color_col and color_col in df.columns:
        groups = df[color_col].unique()
        for i, grp in enumerate(groups):
            mask    = df[color_col] == grp
            grp_df  = df[mask]
            grp_sz  = sizes[mask.to_numpy()]

            text_vals = grp_df[text_col].tolist() if text_col else None

            fig.add_trace(go.Scatter(
                x          = grp_df[x].tolist(),
                y          = grp_df[y].tolist(),
                mode       = "markers+text" if text_col else "markers",
                name       = str(grp),
                text       = text_vals,
                textposition = "top center",
                marker     = dict(
                    size    = grp_sz.tolist(),   # ← plain Python list
                    color   = SECTOR_COLORS[i % len(SECTOR_COLORS)],
                    opacity = 0.8,
                    line    = dict(width=0.5, color="#0E1117"),
                ),
                hovertemplate=(
                    f"<b>%{{text}}</b><br>"
                    f"{x}: %{{x}}<br>"
                    f"{y}: %{{y}}<extra>{grp}</extra>"
                ) if text_col else None,
            ))
    else:
        text_vals = df[text_col].tolist() if text_col else None
        fig.add_trace(go.Scatter(
            x          = df[x].tolist(),
            y          = df[y].tolist(),
            mode       = "markers+text" if text_col else "markers",
            text       = text_vals,
            textposition = "top center",
            marker     = dict(
                size    = sizes.tolist(),        # ← plain Python list
                color   = COLORS["primary"],
                opacity = 0.8,
            ),
        ))

    fig.update_layout(
        **LAYOUT_BASE,
        title      = title,
        title_font_size = 15,
        height     = height,
    )
    return fig



def gauge_chart(
    value: float,
    title: str,
    min_val: float = 0,
    max_val: float = 100,
    height: int = 250,
) -> go.Figure:
    """
    Gauge chart. Used for: RSI indicator, quality score.
    """
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=value,
            title={"text": title, "font": {"color": COLORS["text"]}},
            gauge={
                "axis": {"range": [min_val, max_val],
                         "tickcolor": COLORS["text"]},
                "bar":  {"color": COLORS["primary"]},
                "bgcolor": COLORS["card_bg"],
                "steps": [
                    {"range": [0,  30], "color": "#2D0E0E"},
                    {"range": [30, 70], "color": "#1A1F2E"},
                    {"range": [70, 100],"color": "#2D2600"},
                ],
                "threshold": {
                    "line":  {"color": COLORS["secondary"], "width": 3},
                    "thickness": 0.75,
                    "value": value,
                },
            },
            number={"font": {"color": COLORS["text"]}},
        )
    )
    fig.update_layout(
        paper_bgcolor=COLORS["bg"],
        font=dict(color=COLORS["text"]),
        height=height,
        margin=dict(l=20, r=20, t=40, b=10),
    )
    return fig
