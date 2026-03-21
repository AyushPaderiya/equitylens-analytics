import streamlit as st
import sys
from pathlib import Path

st.set_page_config(
    page_title     = "EquityLens Analytics",
    page_icon      = "📈",
    layout         = "wide",
    initial_sidebar_state = "expanded",
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Custom Navigation Routing ─────────────────────────────────────────────────
# st.Page() allows us to define the exact title and icon for each file in the sidebar.

home_page  = st.Page("home.py", title="Home Dashboard", icon="🏠", default=True)
exec_page  = st.Page("pages/01_executive_summary.py", title="Executive Summary", icon="📊")
risk_page  = st.Page("pages/02_sector_risk.py", title="Sector Risk", icon="⚠️")
macro_page = st.Page("pages/03_macro_overlay.py", title="Macro Analysis", icon="🌍")
fund_page  = st.Page("pages/04_fundamentals.py", title="Company Fundamentals", icon="🔬")

# Initialize and render the navigation
# By grouping them in a dictionary, we also get clean headers in the sidebar!
pg = st.navigation({
    "Platform": [home_page],
    "Analytics Modules": [exec_page, risk_page, macro_page, fund_page]
})

pg.run()
