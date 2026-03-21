import streamlit as st
# ── Global Premium CSS ────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

    /* ── Base Typography ─────────────────────────────────────────────── */
    html, body, [class*="css"] {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif !important;
    }

    /* ── Remove Streamlit default padding ─────────────────────────── */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0 !important;
    }

    /* ── Hero Section ────────────────────────────────────────────────── */
    .hero-container {
        text-align: center;
        padding: 3.5rem 2rem 2rem 2rem;
        position: relative;
        overflow: hidden;
    }
    .hero-container::before {
        content: '';
        position: absolute;
        top: -50%;
        left: -50%;
        width: 200%;
        height: 200%;
        background: radial-gradient(ellipse at 30% 50%, rgba(0,212,170,0.06) 0%, transparent 50%),
                    radial-gradient(ellipse at 70% 50%, rgba(255,107,107,0.04) 0%, transparent 50%);
        animation: pulse-bg 8s ease-in-out infinite alternate;
        z-index: 0;
    }
    @keyframes pulse-bg {
        0% { transform: scale(1); opacity: 0.8; }
        100% { transform: scale(1.15); opacity: 1; }
    }
    .hero-title {
        font-size: 3.2rem;
        font-weight: 800;
        background: linear-gradient(135deg, #00D4AA, #00B894, #55EFC4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin-bottom: 0.3rem;
        letter-spacing: -0.02em;
        position: relative;
        z-index: 1;
        animation: fadeInUp 0.8s ease-out;
    }
    .hero-subtitle {
        color: #888;
        font-size: 1.15rem;
        font-weight: 400;
        letter-spacing: 0.03em;
        margin-top: 0;
        position: relative;
        z-index: 1;
        animation: fadeInUp 1s ease-out;
    }
    .hero-badge {
        display: inline-block;
        background: rgba(0,212,170,0.1);
        border: 1px solid rgba(0,212,170,0.25);
        color: #00D4AA;
        padding: 0.3rem 1rem;
        border-radius: 20px;
        font-size: 0.78rem;
        font-weight: 500;
        letter-spacing: 0.05em;
        margin-top: 0.8rem;
        position: relative;
        z-index: 1;
        animation: fadeInUp 1.2s ease-out;
    }

    /* ── Animated Divider ────────────────────────────────────────────── */
    .gradient-divider {
        height: 2px;
        background: linear-gradient(90deg, transparent, #00D4AA, #FFD700, #FF6B6B, transparent);
        border: none;
        margin: 1.5rem auto;
        width: 70%;
        border-radius: 1px;
        animation: shimmer 3s ease-in-out infinite;
    }
    @keyframes shimmer {
        0%, 100% { opacity: 0.5; }
        50% { opacity: 1; }
    }

    /* ── Feature Cards ───────────────────────────────────────────────── */
    .feature-card {
        background: rgba(26, 31, 46, 0.7);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(255,255,255,0.06);
        border-radius: 16px;
        padding: 1.8rem 1.5rem;
        height: 400px; /* Forces all cards to be exactly the same length */
        display: flex;
        flex-direction: column;
        transition: all 0.35s cubic-bezier(0.4, 0, 0.2, 1);
        position: relative;
        overflow: hidden;
        cursor: pointer;
    }
    .feature-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        border-radius: 16px 16px 0 0;
        transition: height 0.35s ease;
    }
    .feature-card:hover {
        transform: translateY(-6px);
        border-color: rgba(255,255,255,0.12);
        box-shadow: 0 20px 40px rgba(0,0,0,0.3),
                    0 0 30px rgba(0,212,170,0.05);
    }
    .feature-card:hover::before {
        height: 4px;
    }

    /* Card accent colors */
    .card-green::before  { background: linear-gradient(90deg, #00D4AA, #00B894); }
    .card-yellow::before { background: linear-gradient(90deg, #FFD700, #F0C040); }
    .card-red::before    { background: linear-gradient(90deg, #FF6B6B, #FF4B4B); }
    .card-purple::before { background: linear-gradient(90deg, #A29BFE, #6C5CE7); }

    .card-green:hover  { box-shadow: 0 20px 40px rgba(0,0,0,0.3), 0 0 30px rgba(0,212,170,0.1); }
    .card-yellow:hover { box-shadow: 0 20px 40px rgba(0,0,0,0.3), 0 0 30px rgba(255,215,0,0.08); }
    .card-red:hover    { box-shadow: 0 20px 40px rgba(0,0,0,0.3), 0 0 30px rgba(255,107,107,0.08); }
    .card-purple:hover { box-shadow: 0 20px 40px rgba(0,0,0,0.3), 0 0 30px rgba(162,155,254,0.08); }

    .card-icon {
        font-size: 2.2rem;
        margin-bottom: 0.6rem;
        display: block;
    }
    .card-title {
        font-size: 1.15rem;
        font-weight: 700;
        margin: 0 0 0.5rem 0;
        letter-spacing: -0.01em;
    }
    .card-green .card-title  { color: #00D4AA; }
    .card-yellow .card-title { color: #FFD700; }
    .card-red .card-title    { color: #FF6B6B; }
    .card-purple .card-title { color: #A29BFE; }

    .card-desc {
        color: #8892A4;
        font-size: 0.85rem;
        line-height: 1.6;
        margin: 0;
    }
    .card-features {
        margin-top: 0.8rem;
        padding: 0;
        list-style: none;
    }
    .card-features li {
        color: #6B7280;
        font-size: 0.78rem;
        padding: 0.15rem 0;
    }
    .card-features li::before {
        content: '›';
        margin-right: 0.5rem;
        font-weight: 700;
    }
    .card-green .card-features li::before  { color: #00D4AA; }
    .card-yellow .card-features li::before { color: #FFD700; }
    .card-red .card-features li::before    { color: #FF6B6B; }
    .card-purple .card-features li::before { color: #A29BFE; }

    /* ── Stats Bar ───────────────────────────────────────────────────── */
    .stats-bar {
        display: flex;
        justify-content: center;
        gap: 3rem;
        padding: 1.2rem 0;
        margin: 1.5rem 0;
    }
    .stat-item {
        text-align: center;
    }
    .stat-value {
        font-size: 1.6rem;
        font-weight: 700;
        color: #FAFAFA;
        display: block;
    }
    .stat-label {
        font-size: 0.72rem;
        color: #6B7280;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 500;
    }

    /* ── Footer ──────────────────────────────────────────────────────── */
    .footer {
        text-align: center;
        padding: 1.5rem 0 0.5rem 0;
        margin-top: 2rem;
        border-top: 1px solid rgba(255,255,255,0.04);
    }
    .footer-text {
        color: #4A5568;
        font-size: 0.75rem;
        letter-spacing: 0.03em;
    }
    .footer-tech {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        flex-wrap: wrap;
        justify-content: center;
    }
    .tech-badge {
        display: inline-block;
        background: rgba(255,255,255,0.04);
        border: 1px solid rgba(255,255,255,0.06);
        color: #6B7280;
        padding: 0.2rem 0.6rem;
        border-radius: 6px;
        font-size: 0.7rem;
        font-weight: 500;
    }

    /* ── Animations ──────────────────────────────────────────────────── */
    @keyframes fadeInUp {
        from { opacity: 0; transform: translateY(20px); }
        to   { opacity: 1; transform: translateY(0); }
    }
    .animate-in-1 { animation: fadeInUp 0.6s ease-out 0.1s both; }
    .animate-in-2 { animation: fadeInUp 0.6s ease-out 0.2s both; }
    .animate-in-3 { animation: fadeInUp 0.6s ease-out 0.3s both; }
    .animate-in-4 { animation: fadeInUp 0.6s ease-out 0.4s both; }

    /* ── Sidebar enhancements ────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0E1117 0%, #141922 100%);
        border-right: 1px solid rgba(255,255,255,0.04);
    }
    section[data-testid="stSidebar"] .stMarkdown h3 {
        font-size: 0.85rem !important;
        font-weight: 600;
        letter-spacing: 0.03em;
        color: #8892A4 !important;
    }

    /* ── Streamlit metric cards enhancement ───────────────────────────── */
    [data-testid="stMetric"] {
        background: rgba(26, 31, 46, 0.6);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255,255,255,0.05);
        border-radius: 12px;
        padding: 1rem 1.2rem !important;
        transition: all 0.3s ease;
    }
    [data-testid="stMetric"]:hover {
        border-color: rgba(0,212,170,0.15);
        transform: translateY(-2px);
    }
    [data-testid="stMetricLabel"] {
        font-weight: 500 !important;
        letter-spacing: 0.02em;
    }
</style>
""", unsafe_allow_html=True)

# ── Hero Section ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="hero-container">
    <div class="hero-title">EquityLens Analytics</div>
    <p class="hero-subtitle">S&P 500 Market Intelligence Platform</p>
    <div class="hero-badge">● LIVE — 30 Stocks · 10 Sectors · 3 Years of History</div>
</div>
<div class="gradient-divider"></div>
""", unsafe_allow_html=True)

# ── Stats Bar ─────────────────────────────────────────────────────────────────
st.markdown("""
<div class="stats-bar">
    <div class="stat-item">
        <span class="stat-value">30</span>
        <span class="stat-label">S&P 500 Stocks</span>
    </div>
    <div class="stat-item">
        <span class="stat-value">10</span>
        <span class="stat-label">GICS Sectors</span>
    </div>
    <div class="stat-item">
        <span class="stat-value">5</span>
        <span class="stat-label">Macro Indicators</span>
    </div>
    <div class="stat-item">
        <span class="stat-value">6</span>
        <span class="stat-label">Technical Indicators</span>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Feature Cards with Navigation ─────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown("""
    <div class="feature-card card-green animate-in-1">
        <span class="card-icon">📊</span>
        <h3 class="card-title">Executive Summary</h3>
        <p class="card-desc">
            Complete market overview with sector performance and top movers.
        </p>
        <ul class="card-features">
            <li>Cumulative sector returns</li>
            <li>Sharpe ratio rankings</li>
            <li>Monthly return heatmap</li>
            <li>Top gainers & losers</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/01_executive_summary.py", label="Open Executive Summary →", icon="📊")

with col2:
    st.markdown("""
    <div class="feature-card card-yellow animate-in-2">
        <span class="card-icon">⚠️</span>
        <h3 class="card-title">Sector Risk Analysis</h3>
        <p class="card-desc">
            Deep-dive into volatility, drawdown, and technical signals.
        </p>
        <ul class="card-features">
            <li>OHLCV candlestick charts</li>
            <li>RSI & MACD indicators</li>
            <li>Risk vs return scatter</li>
            <li>Volatility comparison</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/02_sector_risk.py", label="Open Sector Risk →", icon="⚠️")

with col3:
    st.markdown("""
    <div class="feature-card card-red animate-in-3">
        <span class="card-icon">🌍</span>
        <h3 class="card-title">Macro Overlay</h3>
        <p class="card-desc">
            Fed policy, inflation, and yields vs market performance.
        </p>
        <ul class="card-features">
            <li>Fed rate impact analysis</li>
            <li>VIX fear gauge</li>
            <li>CPI & Treasury yield</li>
            <li>Macro-market correlation</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/03_macro_overlay.py", label="Open Macro Overlay →", icon="🌍")

with col4:
    st.markdown("""
    <div class="feature-card card-purple animate-in-4">
        <span class="card-icon">🔬</span>
        <h3 class="card-title">Fundamentals</h3>
        <p class="card-desc">
            Quality scores, value trap detection, and sector valuation.
        </p>
        <ul class="card-features">
            <li>Composite quality scores</li>
            <li>Value trap detector</li>
            <li>P/E ratio analysis</li>
            <li>52-week positioning</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/04_fundamentals.py", label="Open Fundamentals →", icon="🔬")

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="footer">
    <div class="footer-tech">
        <span class="tech-badge">🐍 Python</span>
        <span class="tech-badge">🐘 PostgreSQL</span>
        <span class="tech-badge">📊 Streamlit</span>
        <span class="tech-badge">📈 Plotly</span>
        <span class="tech-badge">🏛️ yfinance</span>
        <span class="tech-badge">🏦 FRED API</span>
        <span class="tech-badge">📉 Alpha Vantage</span>
    </div>
    <p class="footer-text" style="margin-top: 0.8rem;">
        Built as a production-grade ETL + Analytics platform &nbsp;·&nbsp;
        Data refreshed daily after NYSE close
    </p>
</div>
""", unsafe_allow_html=True)
