# 📈 EquityLens Analytics

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://equitylens-analytics.streamlit.app)

**S&P 500 Market Intelligence Platform** — A production-grade ETL pipeline and interactive analytics dashboard for equity market analysis.

**[🌐 View Live Dashboard](https://equitylens-analytics.streamlit.app)**

EquityLens extracts data from 3 API sources, transforms it through a medallion architecture (Bronze → Silver → Gold), loads it into PostgreSQL, and serves it through an interactive Streamlit dashboard with 4 analytical pages.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                 │
│   yfinance (OHLCV + Fundamentals) · FRED (Macro) · Alpha Vantage   │
└──────────────────────────┬──────────────────────────────────────────┘
                           │
                    ┌──────▼──────┐
                    │   EXTRACT   │  Bronze Layer (raw JSON)
                    │  3 Sources  │  data/raw/{yfinance,fred,alphavantage}/
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  VALIDATE   │  Schema, null, range, freshness checks
                    │  + TRANSFORM│  Silver Layer (technical indicators)
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │    LOAD     │  Upsert into PostgreSQL
                    │ (Idempotent)│  ON CONFLICT DO UPDATE
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │       PostgreSQL        │
              │  dim_dates              │
              │  dim_sectors            │   Gold Layer
              │  dim_companies          │   (SQL Views)
              │  fact_daily_prices      │────► vw_stock_performance
              │  fact_macro_indicators  │────► vw_sector_performance
              │                         │────► vw_fundamental_scorecard
              │                         │────► vw_macro_overlay
              │                         │────► vw_volume_anomalies
              │                         │────► vw_risk_metrics
              └────────────┬────────────┘────► vw_forecast_input
                           │
                    ┌──────▼──────┐
                    │  Streamlit  │  4 Interactive Dashboard Pages
                    │  Dashboard  │  Plotly Charts + KPI Cards
                    └─────────────┘
```

---

## 📊 Dashboard Pages

| Page | Description |
|------|-------------|
| **Executive Summary** | Sector performance, cumulative returns, Sharpe ratios, monthly heatmap, top/bottom movers |
| **Sector Risk** | OHLCV candlestick, RSI gauge, MACD, volatility trends, risk vs return scatter |
| **Macro Overlay** | Fed Funds Rate vs sector returns, VIX fear index, CPI trends, Treasury yields |
| **Fundamentals** | Quality scores (0–100), value trap detection, P/E analysis, 52-week positioning |

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Language** | Python 3.11+ |
| **Database** | PostgreSQL 17 (Neon serverless) |
| **Dashboard** | Streamlit + Plotly |
| **ETL Pipeline** | Custom Python (Medallion Architecture) |
| **API Sources** | yfinance, FRED API, Alpha Vantage |
| **Technical Indicators** | pandas-ta-classic (RSI, MACD, Bollinger Bands, ATR, SMA) |
| **ORM** | SQLAlchemy 2.0 |
| **Scheduling** | schedule (Python) / GitHub Actions |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL database (or [Neon](https://neon.tech) free tier)
- API Keys: [FRED](https://fred.stlouisfed.org/docs/api/api_key.html), [Alpha Vantage](https://www.alphavantage.co/support/#api-key)

### 1. Clone & Install

```bash
git clone https://github.com/AyushPaderiya/equitylens-analytics.git
cd equitylens-analytics
python -m venv venv
venv\Scripts\activate      # Windows
# source venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` with your credentials:

```
FRED_API_KEY=your_fred_key_here
ALPHA_VANTAGE_KEY=your_av_key_here
DATABASE_URL=postgresql://user:password@host/dbname
LOG_LEVEL=INFO
ENVIRONMENT=development
```

### 3. Run Database Setup (First Time)

```bash
# Creates tables, seeds dimensions, extracts data, and loads everything
python -m src.pipeline.run_extraction --force
python -m src.pipeline.run_database_setup
python -m src.pipeline.run_analytics_layer
```

### 4. Launch Dashboard

```bash
streamlit run dashboard/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## 📁 Project Structure

```
equitylens-analytics/
│
├── config/
│   ├── settings.py              # Central config: tickers, API keys, paths
│   └── logging_config.py        # Rotating file + console logger
│
├── src/
│   ├── extractors/              # Bronze layer — API data fetchers
│   │   ├── base_extractor.py    # ABC with retry, caching, idempotency
│   │   ├── yfinance_extractor.py
│   │   ├── fred_extractor.py
│   │   └── alphavantage_extractor.py
│   │
│   ├── transformers/            # Silver layer — data cleaning + enrichment
│   │   ├── technical_indicators.py  # RSI, MACD, BBands, ATR, SMA
│   │   ├── price_transformer.py     # Type casting, outlier detection
│   │   └── macro_transformer.py     # FRED series alignment, YoY changes
│   │
│   ├── validators/
│   │   └── data_validator.py    # Schema, null, range, freshness checks
│   │
│   ├── loaders/
│   │   └── postgres_loader.py   # Idempotent upserts (ON CONFLICT)
│   │
│   └── pipeline/
│       ├── main_pipeline.py     # Master ETL orchestrator
│       ├── run_extraction.py    # Extract-only runner
│       ├── run_database_setup.py # Full DB setup (migrations + load)
│       ├── run_analytics_layer.py # Deploy SQL views
│       └── scheduler.py         # Cron-like daily scheduler
│
├── sql/
│   ├── migrations/              # Schema DDL (tables, indexes, views)
│   │   ├── V1__create_schema.sql
│   │   ├── V2__add_indexes.sql
│   │   └── V3__create_views.sql
│   └── views/                   # Individual Gold layer views
│       ├── vw_stock_performance.sql
│       ├── vw_sector_performance.sql
│       ├── vw_fundamental_scorecard.sql
│       ├── vw_macro_overlay.sql
│       ├── vw_risk_metrics.sql
│       ├── vw_volume_anomalies.sql
│       └── vw_forecast_input.sql
│
├── dashboard/
│   ├── app.py                   # Streamlit entry point + landing page
│   ├── components/
│   │   ├── db.py                # Cached DB connection
│   │   ├── charts.py            # Plotly chart factory (7 chart types)
│   │   ├── filters.py           # Sidebar filter components
│   │   └── kpi_cards.py         # KPI metric cards + signal badges
│   └── pages/
│       ├── 01_executive_summary.py
│       ├── 02_sector_risk.py
│       ├── 03_macro_overlay.py
│       └── 04_fundamentals.py
│
├── data/raw/                    # Bronze layer (gitignored JSON files)
├── docs/data_dictionary.md      # Full schema documentation
├── tests/verify_views.py        # SQL view validation queries
├── .streamlit/config.toml       # Dark theme configuration
├── requirements.txt
└── .env.example
```

---

## 📈 Stock Universe

**30 S&P 500 stocks** across **10 GICS sectors**:

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

**Benchmark:** ^GSPC (S&P 500 Index)

---

## 🔄 Pipeline Operations

| Command | Purpose |
|---------|---------|
| `python -m src.pipeline.run_extraction` | Extract data from all APIs |
| `python -m src.pipeline.run_extraction --force` | Force re-fetch (bypass cache) |
| `python -m src.pipeline.run_database_setup` | Full DB setup (migrations + data load) |
| `python -m src.pipeline.run_database_setup --load-only` | Data load only (skip migrations) |
| `python -m src.pipeline.run_analytics_layer` | Deploy/refresh SQL views |
| `python -m src.pipeline.main_pipeline` | Full ETL run (extract + transform + load) |
| `python -m src.pipeline.scheduler` | Start automated daily scheduler |

---

## 🔑 FRED Macro Series

| Series | Description | Frequency |
|--------|-------------|-----------|
| FEDFUNDS | Federal Funds Effective Rate | Monthly |
| CPIAUCSL | CPI — All Urban Consumers | Monthly |
| GS10 | 10-Year Treasury Yield | Monthly |
| UNRATE | Unemployment Rate | Monthly |
| VIXCLS | CBOE Volatility Index (VIX) | Daily |

---

## 📄 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
