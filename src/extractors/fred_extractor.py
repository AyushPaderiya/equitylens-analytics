import time
from typing import Optional

import pandas as pd
from fredapi import Fred

from config.settings import settings
from src.extractors.base_extractor import BaseExtractor


class FredExtractor(BaseExtractor):

    def __init__(self):
        super().__init__("fred")
        if not settings.FRED_API_KEY:
            raise EnvironmentError("FRED_API_KEY missing from .env")
        self.client     = Fred(api_key=settings.FRED_API_KEY)
        self.start_date = settings.HISTORICAL_START

    def extract(self, force_refresh: bool = False) -> dict:
        """Pull all configured FRED series with polite rate limiting."""
        self.logger.info(
            f"FRED extraction started | Series: {list(settings.FRED_SERIES.keys())}"
        )

        results = {}
        for series_id, description in settings.FRED_SERIES.items():
            self.logger.info(f"  → {series_id}: {description}")
            results[series_id] = self._extract_series(
                series_id, description, force_refresh=force_refresh
            )
            time.sleep(0.3)  # Polite pause — FRED doesn't publish a hard rate limit

        ok = sum(1 for v in results.values() if v in ("success", "cached"))
        self.logger.info(
            f"FRED extraction complete | {ok}/{len(results)} series OK"
        )
        return results

    def _extract_series(
        self, series_id: str, description: str, force_refresh: bool = False
    ) -> str:
        """Fetch a single FRED series + its metadata."""
        filename = f"series_{series_id}"

        if not force_refresh and self._is_fresh(filename):
            self.logger.debug(f"Cache hit: FRED/{series_id}")
            return "cached"

        def _fetch():
            series = self.client.get_series(
                series_id, observation_start=self.start_date
            )
            if series is None or series.empty:
                raise ValueError(f"Empty series returned for {series_id}")

            info = self.client.get_series_info(series_id)

            return {
                "series_id":    series_id,
                "description":  description,
                "frequency":    str(info.get("frequency_short", "N/A")),
                "units":        str(info.get("units_short", "N/A")),
                "extracted_at": self._utc_now(),
                "row_count":    len(series),
                # NaN → None for clean JSON serialisation
                "data": {
                    d.strftime("%Y-%m-%d"): (None if pd.isna(v) else round(float(v), 4))
                    for d, v in series.items()
                },
            }

        try:
            payload = self._with_retry(_fetch)
            self._save_raw(payload, filename)
            self.logger.info(
                f"✓ {series_id}: {payload['row_count']} observations saved "
                f"[{payload['frequency']}, {payload['units']}]"
            )
            return "success"
        except Exception as exc:
            self.logger.error(f"✗ {series_id} failed: {exc}")
            return f"error:{exc}"

    def load_series(self, series_id: str) -> Optional[pd.DataFrame]:
        """Return a FRED series as a clean DataFrame."""
        raw = self._load_raw(f"series_{series_id}")
        if raw is None:
            return None

        df = pd.DataFrame(
            list(raw["data"].items()),
            columns=["date", series_id.lower()],
        )
        df["date"]        = pd.to_datetime(df["date"])
        df["series_id"]   = series_id
        df["description"] = raw["description"]
        df["units"]       = raw["units"]
        return df.sort_values("date").reset_index(drop=True)
