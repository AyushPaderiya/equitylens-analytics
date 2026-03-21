<div align="center">

<img src="https://img.shields.io/badge/Python-3.11+-3776AB?style=for-the-badge&logo=python&logoColor=white" />
<img src="https://img.shields.io/badge/PostgreSQL-17-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" />
<img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" />
<img src="https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge" />

<br/>
<br/>

# 📈 EquityLens Analytics

### S&P 500 Market Intelligence Platform

A production-grade ETL pipeline and interactive analytics dashboard for equity market intelligence — extracting data from 3 live sources, transforming it through a medallion architecture, and serving actionable insights through a multi-page Streamlit dashboard.

<br/>

[![Live Dashboard](https://img.shields.io/badge/🌐%20View%20Live%20Dashboard-equitylens--analytics.streamlit.app-FF4B4B?style=for-the-badge)](https://equitylens-analytics.streamlit.app)

</div>

---

## 📸 Dashboard Screenshots

<table>
  <tr>
    <td align="center"><b>Executive Summary</b></td>
  </tr>
  <tr>
    <td> <img src="docs/screenshots/01_executive_summary.png" alt="Executive Summary" width="100%"/></td>
  </tr>
  <tr>
    <td align="center"><b>Sector Risk</b></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/02_sector_risk.png" alt="Sector Risk" width="100%"/></td>
  </tr>
  <tr>
    <td align="center"><b>Macro Overlay</b></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/03_macro_overlay.png" alt="Macro Overlay" width="100%"/></td>
  </tr>
  <tr>
    <td align="center"><b>Fundamentals Scorecard</b></td>
  </tr>
  <tr>
    <td><img src="docs/screenshots/04_fundamentals.png" alt="Fundamentals" width="100%"/></td>
  </tr>
</table>


---

## ✨ What It Does

EquityLens covers the full analytics engineering lifecycle — from raw API calls to polished dashboard:

- **Extracts** OHLCV prices, fundamentals, macro indicators, and sentiment data from 3 live APIs
- **Validates & Transforms** through a medallion architecture (Bronze → Silver → Gold) with quality checks at each layer
- **Loads** into PostgreSQL via idempotent upserts, with 7 pre-built analytical views
- **Visualises** through an interactive Streamlit dashboard with Plotly charts, KPI cards, and signal badges

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                               │
│     yfinance (OHLCV + Fundamentals)  ·  FRED  ·  Alpha Vantage     │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────▼────────────────┐
              │         BRONZE LAYER             │
              │   Raw JSON → data/raw/{source}/  │   Extract
              │   Retry logic · Caching ·        │   3 API sources
              │   Idempotent fetch               │
              └────────────────┬────────────────┘
                               │
              ┌────────────────▼────────────────┐
              │         SILVER LAYER             │
              │   Schema · Null · Range ·        │   Validate
              │   Freshness checks               │   + Transform
              │   RSI · MACD · BBands · ATR      │
              └────────────────┬────────────────┘
                               │
              ┌────────────────▼────────────────┐
              │         GOLD LAYER               │
              │   PostgreSQL (Neon Serverless)   │   Load
              │   ON CONFLICT DO UPDATE          │   (Idempotent)
              └────────────────┬────────────────┘
                               │
         ┌─────────────────────┼────────────────────┐
         ▼                     ▼                    ▼
  vw_stock_performance   vw_sector_performance  vw_macro_overlay
  vw_fundamental_scorecard  vw_risk_metrics    vw_volume_anomalies
                         vw_forecast_input
                               │
              ┌────────────────▼────────────────┐
              │       STREAMLIT DASHBOARD        │
              │   4 Pages · Plotly · KPI Cards   │
              └─────────────────────────────────┘
```

---

## 📊 Dashboard Pages

| # | Page | What You'll Find |
|---|------|-----------------|
| 1 | **Executive Summary** | Sector performance heatmap, cumulative returns, Sharpe ratios, top/bottom movers |
| 2 | **Sector Risk** | Candlestick chart, RSI gauge, MACD, volatility trends, risk vs. return scatter |
| 3 | **Macro Overlay** | Fed Funds Rate vs. sector returns, VIX fear index, CPI trends, Treasury yields |
| 4 | **Fundamentals** | Quality scores (0–100), value trap detection, P/E analysis, 52-week positioning |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11+ |
| Database | PostgreSQL 17 — [Neon](https://neon.tech) serverless |
| Dashboard | Streamlit + Plotly |
| ETL | Custom Python — Medallion Architecture |
| Data Sources | yfinance · FRED API · Alpha Vantage |
| Technical Indicators | pandas-ta-classic (RSI, MACD, Bollinger Bands, ATR, SMA) |
| ORM | SQLAlchemy 2.0 |
| Scheduling | Python `schedule` + GitHub Actions |

---

## 📈 Stock Universe

**30 S&P 500 stocks** across **10 GICS sectors**, plus `^GSPC` as benchmark:

| Sector | Tickers |
|--------|---------|
| Technology | AAPL, MSFT, NVDA, GOOGL, META, AVGO |
| Financials | JPM, BAC, WFC, GS, MS |
| Healthcare | UNH, JNJ, PFE, ABBV |
| Consumer Discretionary | AMZN, TSLA, HD, MCD |
| Energy | XOM, CVX |
| Industrials | CAT, BA, HON |
| Communication | T, VZ |
| Materials | LIN |
| Real Estate | AMT |
| Utilities | NEE |

---

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL database — [Neon free tier](https://neon.tech) works great
- API keys for [FRED](https://fred.stlouisfed.org/docs/api/api_key.html) and [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

### 1. Clone & Install

```bash
git clone https://github.com/AyushPaderiya/equitylens-analytics.git
cd equitylens-analytics
python -m venv venv

# Activate virtual environment
venv\Scripts\activate        # Windows
source venv/bin/activate     # macOS / Linux

pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Open `.env` and fill in your credentials:

```env
FRED_API_KEY=your_fred_key_here
ALPHA_VANTAGE_KEY=your_av_key_here
DATABASE_URL=postgresql://user:password@host/dbname
LOG_LEVEL=INFO
ENVIRONMENT=development
```

### 3. Set Up the Database (First Run Only)

```bash
# 1. Force-fetch fresh data from all APIs
python -m src.pipeline.run_extraction --force

# 2. Run migrations, seed dimensions, and load data
python -m src.pipeline.run_database_setup

# 3. Deploy the Gold layer SQL views
python -m src.pipeline.run_analytics_layer
```

### 4. Launch the Dashboard

```bash
streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 🔄 Pipeline Commands

| Command | Purpose |
|---------|---------|
| `python -m src.pipeline.run_extraction` | Extract data from all APIs |
| `python -m src.pipeline.run_extraction --force` | Force re-fetch (bypass cache) |
| `python -m src.pipeline.run_database_setup` | Full DB setup — migrations + data load |
| `python -m src.pipeline.run_database_setup --load-only` | Data load only — skip migrations |
| `python -m src.pipeline.run_analytics_layer` | Deploy / refresh SQL views |
| `python -m src.pipeline.main_pipeline` | Full ETL run — extract + transform + load |
| `python -m src.pipeline.scheduler` | Start the automated daily scheduler |

---

## 📁 Project Structure

```
equitylens-analytics/
│
├── config/
│   ├── settings.py                  # Central config: tickers, API keys, paths
│   └── logging_config.py            # Rotating file + console logger
│
├── src/
│   ├── extractors/                  # Bronze layer — API data fetchers
│   │   ├── base_extractor.py        # ABC with retry, caching, idempotency
│   │   ├── yfinance_extractor.py
│   │   ├── fred_extractor.py
│   │   └── alphavantage_extractor.py
│   │
│   ├── transformers/                # Silver layer — data cleaning & enrichment
│   │   ├── technical_indicators.py  # RSI, MACD, BBands, ATR, SMA
│   │   ├── price_transformer.py     # Type casting, outlier detection
│   │   └── macro_transformer.py     # FRED series alignment, YoY changes
│   │
│   ├── validators/
│   │   └── data_validator.py        # Schema, null, range, freshness checks
│   │
│   ├── loaders/
│   │   └── postgres_loader.py       # Idempotent upserts (ON CONFLICT)
│   │
│   └── pipeline/
│       ├── main_pipeline.py         # Master ETL orchestrator
│       ├── run_extraction.py        # Extract-only runner
│       ├── run_database_setup.py    # Full DB setup
│       ├── run_analytics_layer.py   # Deploy SQL views
│       └── scheduler.py             # Daily scheduler
│
├── sql/
│   ├── migrations/                  # Schema DDL — tables, indexes, views
│   │   ├── V1__create_schema.sql
│   │   ├── V2__add_indexes.sql
│   │   └── V3__create_views.sql
│   └── views/                       # Gold layer views
│       ├── vw_stock_performance.sql
│       ├── vw_sector_performance.sql
│       ├── vw_fundamental_scorecard.sql
│       ├── vw_macro_overlay.sql
│       ├── vw_risk_metrics.sql
│       ├── vw_volume_anomalies.sql
│       └── vw_forecast_input.sql
│
├── dashboard/
│   ├── app.py                       # Streamlit entry point + landing page
│   ├── components/
│   │   ├── db.py                    # Cached DB connection
│   │   ├── charts.py                # Plotly chart factory (7 chart types)
│   │   ├── filters.py               # Sidebar filter components
│   │   └── kpi_cards.py             # KPI metric cards + signal badges
│   └── pages/
│       ├── 01_executive_summary.py
│       ├── 02_sector_risk.py
│       ├── 03_macro_overlay.py
│       └── 04_fundamentals.py
│
├── data/raw/                        # Bronze layer — gitignored JSON files
├── docs/
│   ├── data_dictionary.md           # Full schema documentation
│   └── screenshots/                 # Dashboard screenshots
├── tests/verify_views.py            # SQL view validation queries
├── .streamlit/config.toml           # Dark theme configuration
├── requirements.txt
└── .env.example
```

---

## 🔑 FRED Macro Indicators

| Series ID | Description | Frequency |
|-----------|-------------|-----------|
| `FEDFUNDS` | Federal Funds Effective Rate | Monthly |
| `CPIAUCSL` | CPI — All Urban Consumers | Monthly |
| `GS10` | 10-Year Treasury Yield | Monthly |
| `UNRATE` | Unemployment Rate | Monthly |
| `VIXCLS` | CBOE Volatility Index (VIX) | Daily |

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

<div align="center">

Built by [Ayush Paderiya](https://github.com/AyushPaderiya) · Give it a ⭐ if you found it useful!

</div>