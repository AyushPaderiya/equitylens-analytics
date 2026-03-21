"""
Computes technical indicators for all 30 tickers directly from
yfinance OHLCV data already in the Bronze layer.

Indicators computed:
  - RSI(14)             — Momentum: overbought/oversold signal
  - MACD(12,26,9)       — Trend: momentum direction + crossovers
  - Bollinger Bands(20) — Volatility: price envelope
  - ATR(14)             — Volatility: average true range
  - SMA(50), SMA(200)   — Trend: moving average crossover (Golden Cross)

Production context: In a dbt project, these would be SQL-computed
columns in the silver layer model using window functions, or a
Python dbt model using pandas-ta. We're doing the equivalent here.
"""

from typing import Optional
import pandas as pd
import pandas_ta_classic as ta

from config.settings import settings
from config.logging_config import setup_logging
from src.extractors.yfinance_extractor import YFinanceExtractor

logger = setup_logging("transformer.technical_indicators")


class TechnicalIndicatorsTransformer:

    def __init__(self):
        self.extractor = YFinanceExtractor()

    def transform_ticker(self, ticker: str) -> Optional[pd.DataFrame]:
        """
        Load Bronze price data for one ticker and compute all indicators.
        Returns a clean DataFrame ready for the Silver layer / PostgreSQL load.
        """
        df = self.extractor.load_prices(ticker)
        if df is None or df.empty:
            logger.warning(f"No Bronze price data found for {ticker} — skipping")
            return None

        if len(df) < 200:
            logger.warning(
                f"{ticker}: only {len(df)} rows — some indicators (SMA200) "
                f"will have leading NaNs"
            )

        df = df.copy().sort_index()

        # ── RSI (14) ──────────────────────────────────────────────────────────
        df["rsi_14"] = ta.rsi(df["close"], length=14)

        # ── MACD (12, 26, 9) ─────────────────────────────────────────────────
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            df["macd"]        = macd["MACD_12_26_9"]
            df["macd_signal"] = macd["MACDs_12_26_9"]
            df["macd_hist"]   = macd["MACDh_12_26_9"]

        # ── Bollinger Bands (20, 2σ) ──────────────────────────────────────────
        bbands = ta.bbands(df["close"], length=20, std=2)
        if bbands is not None:
            df["bb_upper"] = bbands["BBU_20_2.0"]
            df["bb_mid"]   = bbands["BBM_20_2.0"]
            df["bb_lower"] = bbands["BBL_20_2.0"]
            # %B: where price sits within the band (0 = lower, 1 = upper)
            df["bb_pct_b"] = bbands["BBP_20_2.0"]

        # ── ATR (14) — Average True Range ────────────────────────────────────
        df["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)

        # ── Moving Averages ───────────────────────────────────────────────────
        df["sma_50"]  = ta.sma(df["close"], length=50)
        df["sma_200"] = ta.sma(df["close"], length=200)

        # ── Derived signals ───────────────────────────────────────────────────
        # Golden Cross flag: SMA50 crosses above SMA200 (bullish trend signal)
        df["golden_cross"] = (df["sma_50"] > df["sma_200"]).astype(int)

        # Daily return and rolling volatility (annualised)
        df["daily_return"]       = df["close"].pct_change()
        df["volatility_30d"]     = (
            df["daily_return"].rolling(30).std() * (252 ** 0.5) * 100
        ).round(4)

        # RSI signal classification — used directly in dashboard filters
        df["rsi_signal"] = pd.cut(
            df["rsi_14"],
            bins=[0, 30, 45, 55, 70, 100],
            labels=["oversold", "mild_oversold", "neutral", "mild_overbought", "overbought"],
        )

        # Clean up: drop rows where all indicator columns are NaN
        indicator_cols = [
            "rsi_14", "macd", "bb_upper", "atr_14", "sma_50"
        ]
        df = df.dropna(subset=indicator_cols, how="all")

        logger.info(
            f"✓ {ticker}: {len(df)} rows | "
            f"RSI ✓ | MACD ✓ | BBands ✓ | ATR ✓ | SMA50/200 ✓"
        )
        return df.reset_index()

    def transform_all(self) -> dict:
        """
        Run transform_ticker() for every ticker in the universe.
        Returns a dict: {ticker: DataFrame or None}

        Called by the ETL pipeline after extraction completes.
        """
        logger.info(
            f"Technical indicators transform started | "
            f"Tickers: {len(settings.ALL_TICKERS)}"
        )

        results = {}
        success, failed = 0, 0

        for ticker in settings.ALL_TICKERS:
            try:
                df = self.transform_ticker(ticker)
                results[ticker] = df
                if df is not None:
                    success += 1
                else:
                    failed += 1
            except Exception as exc:
                logger.error(f"✗ {ticker} transform failed: {exc}")
                results[ticker] = None
                failed += 1

        logger.info(
            f"Technical indicators transform complete | "
            f"✓ {success} succeeded | ✗ {failed} failed"
        )
        return results
