"""
Silver layer transformer for FRED macro data.
Aligns monthly/daily series to a common format,
computes YoY changes, and forward-fills gaps.

Production context: In a real macro data pipeline,
you'd also handle seasonal adjustment flags and
vintage data (FRED revises historical values).
"""

import pandas as pd

from config.logging_config import setup_logging
from config.settings import settings
from src.extractors.fred_extractor import FredExtractor

logger = setup_logging("transformer.macro")


class MacroTransformer:

    def __init__(self):
        self.fred = FredExtractor()

    def transform_all(self) -> dict:
        """
        Load and transform all FRED series.
        Returns {series_id: DataFrame} ready for postgres_loader.
        """
        logger.info(
            f"MacroTransformer starting | "
            f"Series: {list(settings.FRED_SERIES.keys())}"
        )
        results = {}

        for series_id in settings.FRED_SERIES.keys():
            try:
                df = self.fred.load_series(series_id)
                if df is None or df.empty:
                    logger.warning(f"{series_id}: no Bronze data found — skipping")
                    continue

                df = self._transform_series(df, series_id)
                results[series_id] = df
                logger.info(
                    f"✓ {series_id}: {len(df)} rows transformed"
                )
            except Exception as exc:
                logger.error(f"✗ {series_id} macro transform failed: {exc}")
                results[series_id] = None

        ok = sum(1 for v in results.values() if v is not None)
        logger.info(f"MacroTransformer complete | {ok}/{len(settings.FRED_SERIES)} OK")
        return results

    def _transform_series(self, df: pd.DataFrame, series_id: str) -> pd.DataFrame:
        """
        Standardise a single FRED series:
        1. Sort by date ascending
        2. Drop nulls at the edges (FRED often has trailing nulls)
        3. Forward-fill interior gaps (e.g. monthly → daily alignment)
        4. Compute year-over-year % change
        """
        value_col = series_id.lower()

        df = df.copy().sort_values("date").reset_index(drop=True)

        # Drop leading/trailing nulls — keep interior nulls for ffill
        first_valid = df[value_col].first_valid_index()
        last_valid  = df[value_col].last_valid_index()
        if first_valid is None:
            return df
        df = df.loc[first_valid:last_valid].copy()

        # Forward-fill interior gaps (e.g. weekends in daily VIX series)
        df[value_col] = df[value_col].ffill()

        # YoY % change — requires at least 13 months of data
        if len(df) >= 13:
            df[f"{value_col}_yoy_chg"] = df[value_col].pct_change(periods=12).round(6)

        # Round value for clean storage
        df[value_col] = df[value_col].round(4)

        return df
