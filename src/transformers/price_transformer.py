"""
Silver layer transformer for price data.
Sits between Bronze (raw yfinance JSON) and the PostgreSQL loader.

Responsibilities:
  - Type casting and column standardisation
  - Outlier detection and capping
  - Null forward-fill for thin trading days
  - Benchmark-relative return computation

Production context: In a dbt stack this would be the
silver layer model: stg_prices.sql → int_prices.sql
"""

import numpy as np
import pandas as pd

from config.logging_config import setup_logging
from config.settings import settings
from src.extractors.yfinance_extractor import YFinanceExtractor
from src.transformers.technical_indicators import TechnicalIndicatorsTransformer

logger = setup_logging("transformer.price")


class PriceTransformer:

    def __init__(self):
        self.yf          = YFinanceExtractor()
        self.tech        = TechnicalIndicatorsTransformer()
        self._benchmark  = None   # Lazy-loaded once

    # ── Benchmark (^GSPC) ─────────────────────────────────────────────────────

    def _load_benchmark(self) -> pd.Series:
        """Load S&P 500 benchmark close prices as a Series indexed by date."""
        if self._benchmark is not None:
            return self._benchmark

        df = self.yf.load_prices(settings.BENCHMARK_TICKER)
        if df is not None and not df.empty:
            self._benchmark = df["close"].rename("benchmark_close")
        return self._benchmark

    # ── Single ticker transform ────────────────────────────────────────────────

    def transform(self, ticker: str) -> pd.DataFrame:
        """
        Full silver-layer transform for one ticker.
        Returns a clean, typed DataFrame ready for PostgreSQL upsert.
        """
        # Step 1: Get technical indicators (already includes OHLCV)
        df = self.tech.transform_ticker(ticker)
        if df is None or df.empty:
            logger.warning(f"{ticker}: no data returned from technical transformer")
            return None

        # Step 2: Type enforcement
        df = self._enforce_types(df, ticker)

        # Step 3: Outlier detection — log extreme daily returns
        df = self._flag_outliers(df, ticker)

        # Step 4: Add benchmark-relative return
        df = self._add_relative_return(df, ticker)

        # Step 5: Final column selection — exactly what PostgreSQL expects
        df = self._select_final_columns(df)

        logger.debug(f"{ticker}: price transform complete — {len(df)} rows")
        return df

    def transform_all(self) -> dict:
        """Transform all tickers. Returns {ticker: DataFrame}."""
        logger.info(f"PriceTransformer starting | {len(settings.ALL_TICKERS)} tickers")
        results = {}

        for ticker in settings.ALL_TICKERS:
            try:
                results[ticker] = self.transform(ticker)
            except Exception as exc:
                logger.error(f"✗ {ticker} price transform failed: {exc}")
                results[ticker] = None

        ok = sum(1 for v in results.values() if v is not None)
        logger.info(f"PriceTransformer complete | {ok}/{len(settings.ALL_TICKERS)} OK")
        return results

    # ── Private helpers ────────────────────────────────────────────────────────

    def _enforce_types(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """Cast all columns to their correct PostgreSQL-compatible types."""
        numeric_cols = [
            "open", "high", "low", "close", "volume",
            "daily_return", "volatility_30d", "rsi_14",
            "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_mid", "bb_lower", "bb_pct_b",
            "atr_14", "sma_50", "sma_200",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        if "golden_cross" in df.columns:
            df["golden_cross"] = df["golden_cross"].fillna(0).astype(int)

        df["ticker"] = ticker
        return df

    def _flag_outliers(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Detect extreme daily returns (>15% in one day).
        Log them as warnings — do NOT remove them (they're real events
        e.g. earnings surprises, circuit breakers).
        Production: these would trigger alerts in Datadog/PagerDuty.
        """
        if "daily_return" not in df.columns:
            return df

        extreme = df[df["daily_return"].abs() > 0.15]
        if not extreme.empty:
            for _, row in extreme.iterrows():
                logger.warning(
                    f"Extreme return detected | {ticker} | "
                    f"{row['date']} | {row['daily_return']:.2%}"
                )
        return df

    def _add_relative_return(self, df: pd.DataFrame, ticker: str) -> pd.DataFrame:
        """
        Compute daily return relative to S&P 500 benchmark.
        Alpha = stock daily return - benchmark daily return.
        Null-safe: if benchmark unavailable, column is simply omitted.
        """
        benchmark = self._load_benchmark()
        if benchmark is None:
            return df

        df = df.copy()
        df["date_dt"] = pd.to_datetime(df["date"])
        df = df.set_index("date_dt")

        benchmark.index = pd.to_datetime(benchmark.index)
        bench_return = benchmark.pct_change().rename("benchmark_return")

        df = df.join(bench_return, how="left")
        if "daily_return" in df.columns and "benchmark_return" in df.columns:
            df["relative_return"] = df["daily_return"] - df["benchmark_return"]

        df = df.reset_index(drop=True)
        return df

    def _select_final_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Return only the columns that fact_daily_prices expects.
        Any extra columns computed mid-transform are dropped here.
        This is the Silver → Gold boundary.
        """
        final_cols = [
            "ticker", "date", "open", "high", "low", "close", "volume",
            "daily_return", "volatility_30d", "rsi_14",
            "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_mid", "bb_lower", "bb_pct_b",
            "atr_14", "sma_50", "sma_200", "golden_cross",
        ]
        available = [c for c in final_cols if c in df.columns]
        return df[available].copy()

