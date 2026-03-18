import time
from typing import Optional

import pandas as pd
import requests

from config.settings import settings
from src.extractors.base_extractor import BaseExtractor


class AlphaVantageExtractor(BaseExtractor):
    _BASE_URL = "https://www.alphavantage.co/query"

    def __init__(self):
        super().__init__("alphavantage")
        if not settings.ALPHA_VANTAGE_KEY:
            raise EnvironmentError("ALPHA_VANTAGE_KEY missing from .env")
        self.api_key    = settings.ALPHA_VANTAGE_KEY
        self._call_count = 0

        def extract(self, force_refresh: bool = False) -> dict:
            """Pull RSI only for AV_TICKERS. MACD is computed locally via pandas-ta."""
            self.logger.info(
                f"Alpha Vantage extraction started | "
                f"Tickers: {settings.AV_TICKERS} | Indicator: RSI only"
            )

            results = {}
            for ticker in settings.AV_TICKERS:
                self.logger.info(f"  → {ticker}: RSI")
                rsi_status = self._extract_indicator(
                    ticker, "RSI", force_refresh=force_refresh
                )
                self._throttle()
                results[ticker] = {"rsi": rsi_status}

            ok = sum(1 for v in results.values() if v["rsi"] in ("success", "cached"))
            self.logger.info(
                f"Alpha Vantage extraction complete | {ok}/{len(results)} tickers OK"
            )
            return results


    def _extract_indicator(
        self, ticker: str, indicator: str, force_refresh: bool = False
    ) -> str:
        """Generic indicator fetcher — handles both RSI and MACD."""
        filename = f"{indicator.lower()}_{ticker}"

        if not force_refresh and self._is_fresh(filename):
            self.logger.debug(f"Cache hit: AV/{indicator}/{ticker}")
            return "cached"

        params = self._build_params(ticker, indicator)

        def _fetch():
            response = requests.get(
                self._BASE_URL, params=params, timeout=30
            )
            response.raise_for_status()
            data = response.json()

            # Alpha Vantage returns errors inside 200 responses — check explicitly
            if "Error Message" in data:
                raise ValueError(f"AV API error: {data['Error Message']}")
            if "Note" in data:
                raise RuntimeError(
                    f"Rate limit reached: {data['Note']}"
                )

            key = f"Technical Analysis: {indicator}"
            if key not in data:
                raise ValueError(
                    f"Unexpected response for {ticker}/{indicator}. "
                    f"Got keys: {list(data.keys())}"
                )
            return data[key]

        try:
            raw_data = self._with_retry(_fetch)
            payload = {
                "ticker":       ticker,
                "indicator":    indicator,
                "interval":     "daily",
                "extracted_at": self._utc_now(),
                "row_count":    len(raw_data),
                "params":       {k: v for k, v in params.items() if k != "apikey"},
                "data":         raw_data,
            }
            self._save_raw(payload, filename)
            self._call_count += 1
            self.logger.info(
                f"✓ {ticker} {indicator}: {payload['row_count']} rows saved "
                f"[API calls today: {self._call_count}]"
            )
            return "success"
        except Exception as exc:
            self.logger.error(f"✗ {ticker} {indicator} failed: {exc}")
            return f"error:{exc}"

    def _build_params(self, ticker: str, indicator: str) -> dict:
        """Return correct parameter set for each indicator type."""
        base = {"symbol": ticker, "interval": "daily", "apikey": self.api_key}
        if indicator == "RSI":
            return {**base, "function": "RSI", "time_period": 14, "series_type": "close"}
        if indicator == "MACD":
            return {**base, "function": "MACD", "series_type": "close",
                    "fastperiod": 12, "slowperiod": 26, "signalperiod": 9}
        raise ValueError(f"Unknown indicator: {indicator}")

    def _throttle(self) -> None:
        """5 calls/min limit. RSI only = 5 calls total, simple 13s gap."""
        time.sleep(13)

    # ── DataFrame accessors ──────────────────────────────────────────────────

    def load_rsi(self, ticker: str) -> Optional[pd.DataFrame]:
        raw = self._load_raw(f"rsi_{ticker}")
        if raw is None:
            return None
        df = pd.DataFrame(
            [(d, float(v["RSI"])) for d, v in raw["data"].items()],
            columns=["date", "rsi"],
        )
        df["date"]   = pd.to_datetime(df["date"])
        df["ticker"] = ticker
        return df.sort_values("date").reset_index(drop=True)

    def load_macd(self, ticker: str) -> Optional[pd.DataFrame]:
        raw = self._load_raw(f"macd_{ticker}")
        if raw is None:
            return None
        df = pd.DataFrame(
            [
                (d, float(v["MACD"]), float(v["MACD_Signal"]), float(v["MACD_Hist"]))
                for d, v in raw["data"].items()
            ],
            columns=["date", "macd", "macd_signal", "macd_hist"],
        )
        df["date"]   = pd.to_datetime(df["date"])
        df["ticker"] = ticker
        return df.sort_values("date").reset_index(drop=True)
