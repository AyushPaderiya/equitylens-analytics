-- vw_forecast_input.sql
-- Answers BQ6: Clean time series input for 30-day price forecasting
-- Includes price + technical features + macro context
-- Used by: ML forecasting layer (Phase 5) and Forecast dashboard page

CREATE OR REPLACE VIEW vw_forecast_input AS
WITH price_features AS (
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
        fdp.macd_hist,
        fdp.bb_upper,
        fdp.bb_lower,
        fdp.bb_pct_b,
        fdp.sma_50,
        fdp.sma_200,
        fdp.golden_cross,
        fdp.atr_14,

        -- Lagged returns (used as regressors in Prophet)
        LAG(fdp.daily_return, 1) OVER (
            PARTITION BY fdp.ticker ORDER BY fdp.date
        ) AS return_lag_1d,

        LAG(fdp.daily_return, 5) OVER (
            PARTITION BY fdp.ticker ORDER BY fdp.date
        ) AS return_lag_5d,

        -- 5-day and 10-day average volume (momentum proxy)
        AVG(fdp.volume) OVER (
            PARTITION BY fdp.ticker
            ORDER BY fdp.date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_volume_5d,

        -- Price momentum: close vs 20-day ago
        LAG(fdp.close, 20) OVER (
            PARTITION BY fdp.ticker ORDER BY fdp.date
        ) AS close_20d_ago

    FROM fact_daily_prices fdp
    JOIN dim_companies     dc ON fdp.ticker   = dc.ticker
    JOIN dim_sectors       ds ON dc.sector_id = ds.sector_id
    WHERE dc.is_benchmark = FALSE
        AND fdp.close IS NOT NULL
),
with_macro AS (
    -- Join latest available macro value to each trading date
    -- LEFT JOIN LATERAL gets the most recent macro obs <= trading date
    SELECT
        pf.*,
        fed.value   AS fed_funds_rate,
        cpi.value   AS cpi,
        gs10.value  AS treasury_10y,
        vix.value   AS vix

    FROM price_features pf

    LEFT JOIN LATERAL (
        SELECT value FROM fact_macro_indicators
        WHERE series_id = 'FEDFUNDS' AND date <= pf.date
        ORDER BY date DESC LIMIT 1
    ) fed  ON TRUE

    LEFT JOIN LATERAL (
        SELECT value FROM fact_macro_indicators
        WHERE series_id = 'CPIAUCSL' AND date <= pf.date
        ORDER BY date DESC LIMIT 1
    ) cpi  ON TRUE

    LEFT JOIN LATERAL (
        SELECT value FROM fact_macro_indicators
        WHERE series_id = 'GS10' AND date <= pf.date
        ORDER BY date DESC LIMIT 1
    ) gs10 ON TRUE

    LEFT JOIN LATERAL (
        SELECT value FROM fact_macro_indicators
        WHERE series_id = 'VIXCLS' AND date <= pf.date
        ORDER BY date DESC LIMIT 1
    ) vix  ON TRUE
)
SELECT
    ticker,
    company_name,
    sector_name,
    date,
    -- Prophet requires columns named 'ds' and 'y'
    date                                                AS ds,
    ROUND(close::NUMERIC, 4)                            AS y,
    ROUND(close::NUMERIC, 4)                            AS close,
    volume,
    ROUND(daily_return::NUMERIC, 6)                     AS daily_return,
    ROUND(volatility_30d::NUMERIC, 4)                   AS volatility_30d,
    ROUND(rsi_14::NUMERIC, 4)                           AS rsi_14,
    ROUND(macd::NUMERIC, 6)                             AS macd,
    ROUND(macd_hist::NUMERIC, 6)                        AS macd_hist,
    ROUND(bb_pct_b::NUMERIC, 4)                         AS bb_pct_b,
    ROUND(atr_14::NUMERIC, 4)                           AS atr_14,
    golden_cross,
    ROUND(return_lag_1d::NUMERIC, 6)                    AS return_lag_1d,
    ROUND(return_lag_5d::NUMERIC, 6)                    AS return_lag_5d,
    ROUND(avg_volume_5d::NUMERIC, 0)                    AS avg_volume_5d,
    ROUND(
        CASE WHEN close_20d_ago > 0
             THEN (close - close_20d_ago) / close_20d_ago
             ELSE NULL
        END::NUMERIC, 6
    )                                                   AS momentum_20d,
    fed_funds_rate,
    cpi,
    treasury_10y,
    vix
FROM with_macro
ORDER BY ticker, date;

COMMENT ON VIEW vw_forecast_input IS
    'Feature-rich time series view for Prophet forecasting. Includes price, technicals, lags, momentum, and macro regressors.';
