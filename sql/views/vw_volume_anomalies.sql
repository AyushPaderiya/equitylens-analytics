-- vw_volume_anomalies.sql
-- Answers BQ3: Volume spike detection — potential institutional activity
-- Uses 20-day average volume as baseline
-- Used by: Anomaly Detection dashboard page

CREATE OR REPLACE VIEW vw_volume_anomalies AS
WITH volume_baseline AS (
    SELECT
        fdp.ticker,
        dc.company_name,
        ds.sector_name,
        fdp.date,
        fdp.volume,
        fdp.close,
        fdp.daily_return,
        fdp.rsi_14,

        -- 20-day rolling average volume (baseline)
        AVG(fdp.volume) OVER (
            PARTITION BY fdp.ticker
            ORDER BY fdp.date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d,

        -- 20-day volume std dev (for z-
                -- 20-day volume std dev (for z-score)
        STDDEV(fdp.volume) OVER (
            PARTITION BY fdp.ticker
            ORDER BY fdp.date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS stddev_volume_20d,

        -- Price direction on spike day
        CASE
            WHEN fdp.daily_return > 0 THEN 'up'
            WHEN fdp.daily_return < 0 THEN 'down'
            ELSE 'flat'
        END AS price_direction

    FROM fact_daily_prices fdp
    JOIN dim_companies     dc ON fdp.ticker   = dc.ticker
    JOIN dim_sectors       ds ON dc.sector_id = ds.sector_id
    WHERE dc.is_benchmark = FALSE
        AND fdp.volume IS NOT NULL
        AND fdp.volume > 0
),
with_zscore AS (
    SELECT
        *,
        -- Volume ratio: today vs 20-day average
        CASE
            WHEN avg_volume_20d > 0
            THEN ROUND((volume / avg_volume_20d)::NUMERIC, 4)
            ELSE NULL
        END AS volume_ratio,

        -- Z-score: how many std devs above normal?
        CASE
            WHEN stddev_volume_20d > 0
            THEN ROUND(
                ((volume - avg_volume_20d) / stddev_volume_20d)::NUMERIC, 4
            )
            ELSE NULL
        END AS volume_zscore,

        -- Spike severity classification
        CASE
            WHEN stddev_volume_20d > 0
                AND ((volume - avg_volume_20d) / stddev_volume_20d) >= 3  THEN 'extreme'
            WHEN stddev_volume_20d > 0
                AND ((volume - avg_volume_20d) / stddev_volume_20d) >= 2  THEN 'high'
            WHEN stddev_volume_20d > 0
                AND ((volume - avg_volume_20d) / stddev_volume_20d) >= 1  THEN 'moderate'
            ELSE 'normal'
        END AS spike_severity
    FROM volume_baseline
),
with_context AS (
    SELECT
        *,
        -- Institutional accumulation signal:
        -- High volume + price up + RSI not overbought = likely buying
        CASE
            WHEN spike_severity IN ('high', 'extreme')
                AND price_direction = 'up'
                AND COALESCE(rsi_14, 50) < 70
            THEN 'accumulation'
            -- Distribution signal: High volume + price down
            WHEN spike_severity IN ('high', 'extreme')
                AND price_direction = 'down'
            THEN 'distribution'
            -- High volume but price flat = indecision
            WHEN spike_severity IN ('high', 'extreme')
                AND price_direction = 'flat'
            THEN 'indecision'
            ELSE 'normal'
        END AS institutional_signal,

        -- Rank spikes within each ticker (most extreme first)
        RANK() OVER (
            PARTITION BY ticker
            ORDER BY volume_ratio DESC NULLS LAST
        ) AS spike_rank_for_ticker

    FROM with_zscore
)
SELECT
    ticker,
    company_name,
    sector_name,
    date,
    volume,
    ROUND(avg_volume_20d::NUMERIC, 0)          AS avg_volume_20d,
    ROUND(close::NUMERIC, 4)                   AS close,
    ROUND((daily_return * 100)::NUMERIC, 4)    AS daily_return_pct,
    rsi_14,
    price_direction,
    volume_ratio,
    volume_zscore,
    spike_severity,
    institutional_signal,
    spike_rank_for_ticker,
    -- Flag: only TRUE when this is a genuine anomaly worth surfacing
    CASE
        WHEN spike_severity IN ('high','extreme') THEN TRUE
        ELSE FALSE
    END AS is_anomaly
FROM with_context
ORDER BY date DESC, volume_ratio DESC;

COMMENT ON VIEW vw_volume_anomalies IS
    'Volume spike detection with z-scores, severity classification, and institutional accumulation/distribution signals.';
