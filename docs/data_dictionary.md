# EquityLens Analytics — Data Dictionary

**Version:** 1.0  
**Last updated:** March 2026  
**Database:** PostgreSQL 17 (Neon — AWS ap-southeast-1)

---

## Tables

### `dim_dates`
Pre-populated calendar dimension covering 2020–2030.

| Column | Type | Description |
|--------|------|-------------|
| date_id | SERIAL PK | Surrogate key |
| date | DATE UNIQUE | Calendar date |
| year | SMALLINT | Calendar year |
| quarter | SMALLINT | Quarter (1–4) |
| month | SMALLINT | Month (1–12) |
| week_of_year | SMALLINT | ISO week number (1–53) |
| day_of_week | SMALLINT | 0=Monday, 6=Sunday |
| day_of_year | SMALLINT | Day of year (1–366) |
| month_name | VARCHAR | e.g. 'January' |
| quarter_label | VARCHAR | e.g. 'Q1 26' |
| is_weekend | BOOLEAN | TRUE for Saturday/Sunday |

---

### `dim_sectors`
GICS sector classification lookup.

| Column | Type | Description |
|--------|------|-------------|
| sector_id | SERIAL PK | Surrogate key |
| sector_name | VARCHAR UNIQUE | e.g. 'Technology' |

---

### `dim_companies`
S&P 500 company master — 29 stocks + S&P 500 benchmark.  
Type 1 SCD: always holds the latest fundamental snapshot.

| Column | Type | Description |
|--------|------|-------------|
| company_id | SERIAL PK | Surrogate key |
| ticker | VARCHAR UNIQUE | Stock symbol e.g. 'AAPL' |
| company_name | VARCHAR | Full company name |
| sector_id | INT FK | References dim_sectors |
| industry | VARCHAR | GICS industry |
| market_cap | BIGINT | Market capitalisation in USD |
| beta | NUMERIC | Price sensitivity vs S&P 500 |
| trailing_pe | NUMERIC | Trailing 12-month P/E ratio |
| forward_pe | NUMERIC | Forward P/E (analyst estimates) |
| price_to_book | NUMERIC | Price-to-book ratio |
| revenue_growth | NUMERIC | YoY revenue growth (decimal) |
| earnings_growth | NUMERIC | YoY earnings growth (decimal) |
| profit_margins | NUMERIC | Net profit margin (decimal) |
| debt_to_equity | NUMERIC | Total debt / total equity |
| return_on_equity | NUMERIC | ROE (decimal) |
| return_on_assets | NUMERIC | ROA (decimal) |
| dividend_yield | NUMERIC | Annual dividend yield (decimal) |
| is_benchmark | BOOLEAN | TRUE only for ^GSPC |
| updated_at | TIMESTAMPTZ | Last fundamental refresh |

---

### `fact_daily_prices`
Primary analytical table. One row per ticker per trading day.  
~31,000 rows: 30 tickers × 1,054 trading days (Jan 2022 – present).

| Column | Type | Description |
|--------|------|-------------|
| price_id | BIGSERIAL PK | Surrogate key |
| ticker | VARCHAR FK | References dim_companies |
| date_id | INT FK | References dim_dates |
| date | DATE | Trading date |
| open | NUMERIC | Opening price (split-adjusted) |
| high | NUMERIC | Intraday high |
| low | NUMERIC | Intraday low |
| close | NUMERIC | Closing price (split-adjusted) |
| volume | BIGINT | Shares traded |
| daily_return | NUMERIC | (close - prev_close) / prev_close |
| volatility_30d | NUMERIC | Annualised 30-day rolling std dev (%) |
| rsi_14 | NUMERIC | RSI(14) — 0 to 100 |
| macd | NUMERIC | MACD line (12,26,9) |
| macd_signal | NUMERIC | MACD signal line |
| macd_hist | NUMERIC | MACD histogram (macd - signal) |
| bb_upper | NUMERIC | Bollinger Band upper (20, 2σ) |
| bb_mid | NUMERIC | Bollinger Band midline (SMA20) |
| bb_lower | NUMERIC | Bollinger Band lower (20, 2σ) |
| bb_pct_b | NUMERIC | %B: price position within band (0=lower, 1=upper) |
| atr_14 | NUMERIC | Average True Range (14) |
| sma_50 | NUMERIC | 50-day simple moving average |
| sma_200 | NUMERIC | 200-day simple moving average |
| golden_cross | SMALLINT | 1 = SMA50 > SMA200 (bullish), 0 = bearish |
| created_at | TIMESTAMPTZ | Row insertion timestamp |

**Unique constraint:** `(ticker, date)` — one row per stock per trading day.

---

### `fact_macro_indicators`
FRED macroeconomic time series. One row per series per observation date.

| Column | Type | Description |
|--------|------|-------------|
| macro_id | BIGSERIAL PK | Surrogate key |
| series_id | VARCHAR | FRED series code e.g. 'FEDFUNDS' |
| date | DATE | Observation date |
| value | NUMERIC | Series value |
| description | VARCHAR | Human-readable series name |
| units | VARCHAR | e.g. '%', 'Index 1982-84=100' |
| frequency | VARCHAR | D=Daily, M=Monthly |
| created_at | TIMESTAMPTZ | Row insertion timestamp |

**FRED Series loaded:**

| series_id | Description | Frequency | Units |
|-----------|-------------|-----------|-------|
| FEDFUNDS | Federal Funds Effective Rate | Monthly | % |
| CPIAUCSL | CPI — All Urban Consumers | Monthly | Index |
| GS10 | 10-Year Treasury Yield | Monthly | % |
| UNRATE | Unemployment Rate | Monthly | % |
| VIXCLS | CBOE Volatility Index | Daily | Index |

**Unique constraint:** `(series_id, date)`

---

## Views (Gold Layer)

### `vw_sector_performance`
Daily sector-level aggregated returns, cumulative performance,
and 30-day rolling Sharpe ratio. Primary view for the
Executive Summary dashboard page.

**Key columns:**
| Column | Description |
|--------|-------------|
| sector_name | GICS sector |
| date | Trading date |
| avg_daily_return | Equal-weighted avg return across sector stocks |
| cumulative_return | Compounded return from Jan 2022 |
| sharpe_30d | 30-day rolling annualised Sharpe ratio |
| avg_volatility_pct | 30-day annualised volatility % |

---

### `vw_stock_performance`
Per-stock daily performance with cumulative returns, drawdown
from ATH, Sharpe ratio, and market signal classification.

**Key columns:**
| Column | Description |
|--------|-------------|
| cumulative_return_pct | Total return from Jan 2022 (%) |
| drawdown_from_ath_pct | % below all-time high |
| sharpe_30d | 30-day rolling annualised Sharpe |
| market_signal | bullish / bearish / oversold / overbought / neutral |
| rank_in_sector | Price rank within sector peers |

---

### `vw_fundamental_scorecard`
Company fundamental quality scoring with value trap detection
and sector-relative P/E valuation.

**Key columns:**
| Column | Description |
|--------|-------------|
| quality_score | Composite 0–100 score (ROE + margins + debt + growth) |
| value_trap_flag | TRUE if P/E > 30 with revenue growth < 5% |
| pe_vs_sector_median | P/E premium/discount vs sector peers |
| fundamental_signal | high_quality / moderate_quality / value_trap_risk / low_quality |
| price_52w_position_pct | Where current price sits in 52-week range (0–100) |

---

### `vw_volume_anomalies`
Volume spike detection using 20-day z-scores with institutional
accumulation and distribution signal classification.

**Key columns:**
| Column | Description |
|--------|-------------|
| volume_ratio | Today's volume / 20-day average volume |
| volume_zscore | Standard deviations above 20-day mean |
| spike_severity | normal / moderate / high / extreme |
| institutional_signal | accumulation / distribution / indecision / normal |
| is_anomaly | TRUE when spike_severity is high or extreme |

---

### `vw_risk_metrics`
Per-stock risk metrics including drawdown, ATR, RSI signals,
and volume spike flags. Primary view for the Risk Analysis page.

**Key columns:**
| Column | Description |
|--------|-------------|
| drawdown_pct | % below rolling all-time high |
| avg_volume_30d | 30-day average volume baseline |
| volume_spike_flag | TRUE when volume > 2x 30-day average |
| rsi_signal | oversold / mild_oversold / neutral / mild_overbought / overbought |

---

### `vw_macro_overlay`
Monthly sector performance joined with FRED macro indicators
for correlation and regime analysis.

**Key columns:**
| Column | Description |
|--------|-------------|
| fed_funds_rate | Federal Funds Rate (%) |
| cpi | CPI index value |
| treasury_10y | 10-Year Treasury yield (%) |
| vix | VIX fear index |

---

### `vw_forecast_input`
Feature-rich time series view for Prophet price forecasting.
Includes price, technical indicators, lagged returns,
momentum, and macro regressors.

**Key columns:**
| Column | Description |
|--------|-------------|
| ds | Date (Prophet required column name) |
| y | Close price (Prophet required column name) |
| return_lag_1d | Prior day return (regressor) |
| return_lag_5d | 5-day lagged return (regressor) |
| momentum_20d | 20-day price momentum (regressor) |
| vix | VIX as market fear regressor |

---

## Business Questions Answered

| BQ | Question | View |
|----|----------|------|
| BQ1 | Which sectors delivered best risk-adjusted returns? | vw_sector_performance, vw_stock_performance |
| BQ2 | How do Fed rate changes correlate with sector rotation? | vw_macro_overlay |
| BQ3 | Which stocks show abnormal volume — institutional activity? | vw_volume_anomalies |
| BQ4 | What is rolling volatility trend by sector? | vw_risk_metrics, vw_sector_performance |
| BQ5 | Which stocks show P/E expansion without revenue growth? | vw_fundamental_scorecard |
| BQ6 | What is the 30-day price forecast for top 5 stocks? | vw_forecast_input |

---

## Pipeline Run Schedule

| Run | Time (UTC) | Time (IST) | What it does |
|-----|-----------|------------|--------------|
| Morning refresh | 06:00 | 11:30 | Load-only, cached data |
| EOD full run | 21:30 | 03:00 | Force re-fetch after NYSE close |
