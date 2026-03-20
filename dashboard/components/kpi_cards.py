"""
KPI metric card components.
Renders the top-row metric tiles seen on every
professional analytics dashboard.
"""

import streamlit as st


def metric_card(
    label: str,
    value: str,
    delta: str = None,
    delta_color: str = "normal",
    help_text: str = None,
) -> None:
    """Wrapper around st.metric with consistent styling."""
    st.metric(
        label=label,
        value=value,
        delta=delta,
        delta_color=delta_color,
        help=help_text,
    )


def render_kpi_row(metrics: list) -> None:
    """
    Render a row of KPI cards.

    Args:
        metrics: list of dicts with keys:
                 label, value, delta (opt), delta_color (opt), help (opt)

    Example:
        render_kpi_row([
            {"label": "S&P 500", "value": "5,234", "delta": "+1.2%"},
            {"label": "VIX",     "value": "23.5",  "delta": "-2.1"},
        ])
    """
    cols = st.columns(len(metrics))
    for col, m in zip(cols, metrics):
        with col:
            st.metric(
                label       = m["label"],
                value       = m["value"],
                delta       = m.get("delta"),
                delta_color = m.get("delta_color", "normal"),
                help        = m.get("help"),
            )


def signal_badge(signal: str) -> str:
    """
    Return HTML badge for a market signal string.
    Used in dataframe displays.
    """
    colors = {
        "bullish":           ("#00D4AA", "#0E2D26"),
        "bearish":           ("#FF4B4B", "#2D0E0E"),
        "oversold":          ("#FFD700", "#2D2600"),
        "overbought":        ("#FF8C00", "#2D1A00"),
        "neutral":           ("#888888", "#1A1A1A"),
        "accumulation":      ("#00D4AA", "#0E2D26"),
        "distribution":      ("#FF4B4B", "#2D0E0E"),
        "high_quality":      ("#00D4AA", "#0E2D26"),
        "value_trap_risk":   ("#FF4B4B", "#2D0E0E"),
        "moderate_quality":  ("#FFD700", "#2D2600"),
        "low_quality":       ("#888888", "#1A1A1A"),
    }
    color, bg = colors.get(signal, ("#888", "#1A1A1A"))
    return (
        f"<span style='background:{bg}; color:{color}; "
        f"padding: 2px 8px; border-radius: 4px; "
        f"border: 1px solid {color}; font-size: 0.8rem;'>"
        f"{signal.replace('_', ' ').title()}</span>"
    )
