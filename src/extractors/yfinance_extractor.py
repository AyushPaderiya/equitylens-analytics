from datetime import date
from typing import Optional

import pandas as pd
import yfinance as yf

from config.settings import settings
from src.extractors.base_extractor import BaseExtractor


class YFinanceExtractor(BaseExtractor):

    def __init__(self):
        super().__init__("yfinance")
        self.start_date = settings.HISTORICAL_START
        self.end_date   = date.today().isoformat()

    # ── Public entry point ───────────────────────────────────────────────────

    def extract(self, force_refresh: bool = False) -> dict:
        """
        Full extraction run:
        1. S&P 500 benchmark prices
        2. OHLCV prices for all 30 tickers
        3. Fundamental data for all 30 tickers
        """
        self.logger.info(
            f"yfinance extraction started | "
            f"Tickers: {len(settings.ALL_TICKERS)} stocks + benchmark | "
            f"Window: {self.start_date} → {self.end_date}"
        )

        results = {"prices": {}, "fundamentals": {}, "benchmark": None}

        # Benchmark
        results["benchmark"] = self._extract_prices(
            settings.BENCHMARK_TICKER, force_refresh=force_refresh
        )

        # Prices — all tickers
        for sector, tickers in settings.TICKERS.items():
            for ticker in tickers:
                self.logger.info(f"  → prices  {ticker:6s} [{sector}]")
                results["prices"][ticker] = self._extract_prices(
                    ticker, force_refresh=force_refresh
                )

        # Fundamentals — all tickers (refreshed weekly, not daily)
        for ticker in settings.ALL_TICKERS:
            self.logger.info(f"  → fundamentals  {ticker}")
            results["fundamentals"][ticker] = self._extract_fundamentals(
                ticker, force_refresh=force_refresh
            )

        # Summary log
        price_ok = sum(1 for v in results["prices"].values() if v in ("success", "cached"))
        fund_ok  = sum(1 for v in results["fundamentals"].values() if v in ("success", "cached"))
        self.logger.info(
            f"yfinance extraction complete | "
            f"Prices {price_ok}/{len(settings.ALL_TICKERS)} OK | "
            f"Fundamentals {fund_ok}/{len(settings.ALL_TICKERS)} OK"
        )
        return results

    # ── Private helpers ───────────────────────────────────────────────────────

    def _extract_prices(self, ticker: str, force_refresh: bool = False) -> str:
        """Download OHLCV history. Saved as: prices_{TICKER}.json"""
        safe_name = ticker.replace("^", "IDX_")
        filename  = f"prices_{safe_name}"

        if not force_refresh and self._is_fresh(filename):
            self.logger.debug(f"Cache hit: {ticker} prices")
            return "cached"

        def _fetch():
            stock = yf.Ticker(ticker)
            df    = stock.history(
                start=self.start_date,
                end=self.end_date,
                auto_adjust=True,   # Adjusts for splits & dividends automatically
                actions=False,
            )
            if df.empty:
                raise ValueError(f"Empty price history returned for {ticker}")

            df.index   = df.index.strftime("%Y-%m-%d")
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            return {
                "ticker":       ticker,
                "extracted_at": self._utc_now(),
                "start_date":   self.start_date,
                "end_date":     self.end_date,
                "row_count":    len(df),
                "columns":      list(df.columns),
                "data":         df.to_dict(orient="index"),
            }

        try:
            payload = self._with_retry(_fetch)
            self._save_raw(payload, filename)
            self.logger.info(f"✓ {ticker}: {payload['row_count']} price rows saved")
            return "success"
        except Exception as exc:
            self.logger.error(f"✗ {ticker} prices failed: {exc}")
            return f"error:{exc}"

    def _extract_fundamentals(self, ticker: str, force_refresh: bool = False) -> str:
        """
        Download key fundamental fields.
        Weekly cache (167h) — fundamentals don't change daily.
        Saved as: fundamentals_{TICKER}.json
        """
        filename = f"fundamentals_{ticker}"

        if not force_refresh and self._is_fresh(filename, max_age_hours=167):
            self.logger.debug(f"Cache hit: {ticker} fundamentals")
            return "cached"

        # Only the fields we actually use in analysis — not all 100+ yfinance keys
        _FIELDS = [
            "symbol", "shortName", "sector", "industry",
            "marketCap", "trailingPE", "forwardPE", "priceToBook",
            "trailingEps", "revenueGrowth", "earningsGrowth",
            "profitMargins", "operatingMargins", "totalRevenue",
            "totalDebt", "totalCash", "debtToEquity",
            "returnOnEquity", "returnOnAssets", "beta",
            "fiftyTwoWeekHigh", "fiftyTwoWeekLow",
            "averageVolume", "sharesOutstanding",
            "dividendYield", "currentRatio", "quickRatio",
        ]

        def _fetch():
            info = yf.Ticker(ticker).info
            if not info.get("marketCap"):
                raise ValueError(f"No fundamental data for {ticker}")
            return {
                "ticker":       ticker,
                "extracted_at": self._utc_now(),
                "data":         {k: info.get(k) for k in _FIELDS},
            }

        try:
            payload = self._with_retry(_fetch)
            self._save_raw(payload, filename)
            self.logger.info(f"✓ {ticker}: fundamentals saved")
            return "success"
        except Exception as exc:
            self.logger.error(f"✗ {ticker} fundamentals failed: {exc}")
            return f"error:{exc}"

    # ── DataFrame accessors (used by transformers in Phase 3) ─────────────────

    def load_prices(self, ticker: str) -> Optional[pd.DataFrame]:
        """Return Bronze price data as a typed DataFrame."""
        safe_name = ticker.replace("^", "IDX_")
        raw = self._load_raw(f"prices_{safe_name}")
        if raw is None:
            return None
        df = pd.DataFrame.from_dict(raw["data"], orient="index")
        df.index = pd.to_datetime(df.index)
        df.index.name = "date"
        df.insert(0, "ticker", ticker)
        return df

    def load_fundamentals(self, ticker: str) -> Optional[dict]:
        """Return Bronze fundamental data as a plain dict."""
        raw = self._load_raw(f"fundamentals_{ticker}")
        return raw["data"] if raw else None
