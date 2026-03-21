-- vw_risk_metrics.sql
-- Answers BQ2, BQ4: Rolling volatility, drawdown, RSI signals
-- Used in: Risk Analysis, Deep Dive pages

CREATE OR REPLACE VIEW vw_risk_metrics AS
WITH price_series AS (
    SELECT
        fdp.ticker,
        dc.company_name,
        ds.sector_name,
        fdp.date,
        fdp.close,
        fdp.volume,
        fdp.daily_return,
        fdp.volatility_30d,
        fdp.rsi_14,
        fdp.macd,
        fdp.macd_signal,
        fdp.macd_hist,
        fdp.sma_50,
        fdp.sma_200,
        fdp.golden_cross,
        fdp.atr_14,
        -- Running max close (for drawdown calc)
        MAX(fdp.close) OVER (
            PARTITION BY fdp.ticker
            ORDER BY fdp.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_max_close
    FROM fact_daily_prices fdp
    JOIN dim_companies     dc ON fdp.ticker    = dc.ticker
    JOIN dim_sectors       ds ON dc.sector_id  = ds.sector_id
    WHERE dc.is_benchmark = FALSE
),
with_drawdown AS (
    SELECT
        *,
        -- Drawdown: how far below the all-time-high is current price?
        ROUND(((close - running_max_close) / running_max_close * 100)::NUMERIC, 4)
            AS drawdown_pct,
        -- 30-day average volume for anomaly baseline
        AVG(volume) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS avg_volume_30d
    FROM price_series
)
SELECT
    ticker,
    company_name,
    sector_name,
    date,
    close,
    ROUND(daily_return::NUMERIC, 6)     AS daily_return,
    volatility_30d,
    rsi_14,
    macd,
    macd_signal,
    macd_hist,
    sma_50,
    sma_200,
    golden_cross,
    atr_14,
    drawdown_pct,
    ROUND(avg_volume_30d::NUMERIC, 0)   AS avg_volume_30d,
    volume,
    -- Volume spike flag: today's volume > 2x 30-day average
    CASE WHEN avg_volume_30d > 0
         AND volume > (avg_volume_30d * 2)
         THEN TRUE ELSE FALSE
    END AS volume_spike_flag,
    -- RSI classification
    CASE
        WHEN rsi_14 < 30  THEN 'oversold'
        WHEN rsi_14 < 45  THEN 'mild_oversold'
        WHEN rsi_14 <= 55 THEN 'neutral'
        WHEN rsi_14 <= 70 THEN 'mild_overbought'
        ELSE 'overbought'
    END AS rsi_signal
FROM with_drawdown
ORDER BY ticker, date;

COMMENT ON VIEW vw_risk_metrics IS
    'Per-stock risk metrics: drawdown, volatility, RSI signals, volume spike detection';
