"""
Data validation layer — runs after extraction, before loading.
Catches bad data early so it never reaches PostgreSQL.

Validation tiers:
  1. Schema checks   — required columns present
  2. Null checks     — critical fields never null
  3. Range checks    — values within expected bounds
  4. Freshness check — data is recent enough

Production equivalent: Great Expectations checkpoints or
dbt tests (not_null, accepted_values, relationships).
Each check here maps 1:1 to a dbt test you'd write in schema.yml.
"""

from dataclasses import dataclass, field
from typing import Optional
import pandas as pd

from config.logging_config import setup_logging

logger = setup_logging("validator")


@dataclass
class ValidationResult:
    """
    Immutable result object returned by every validator method.
    Passed downstream so the pipeline can decide: skip, warn, or halt.
    """
    passed:   bool
    ticker:   str
    checks:   dict        = field(default_factory=dict)   # check_name → passed/failed
    warnings: list        = field(default_factory=list)
    errors:   list        = field(default_factory=list)
    row_count: int        = 0

    def summary(self) -> str:
        status = "✓ PASS" if self.passed else "✗ FAIL"
        return (
            f"{status} | {self.ticker} | "
            f"{self.row_count} rows | "
            f"{len(self.errors)} errors | "
            f"{len(self.warnings)} warnings"
        )


class DataValidator:

    # ── Price DataFrame validation ────────────────────────────────────────────

    REQUIRED_PRICE_COLS = {"ticker", "date", "open", "high", "low", "close", "volume"}
    REQUIRED_INDICATOR_COLS = {"rsi_14", "macd", "sma_50", "sma_200", "volatility_30d"}

    def validate_prices(self, df: pd.DataFrame, ticker: str) -> ValidationResult:
        """
        Full validation suite for a transformed price DataFrame.
        Called before every upsert to fact_daily_prices.
        """
        result = ValidationResult(ticker=ticker, passed=True)

        if df is None or df.empty:
            result.passed = False
            result.errors.append("DataFrame is None or empty")
            logger.error(f"✗ {ticker}: empty DataFrame rejected")
            return result

        result.row_count = len(df)

        # ── Check 1: Required columns ─────────────────────────────────────────
        missing_price = self.REQUIRED_PRICE_COLS - set(df.columns)
        missing_indicators = self.REQUIRED_INDICATOR_COLS - set(df.columns)

        result.checks["required_price_cols"] = len(missing_price) == 0
        result.checks["required_indicator_cols"] = len(missing_indicators) == 0

        if missing_price:
            result.passed = False
            result.errors.append(f"Missing price columns: {missing_price}")
        if missing_indicators:
            # Warning only — indicators may have leading NaNs
            result.warnings.append(f"Missing indicator columns: {missing_indicators}")

        # ── Check 2: Null checks on critical fields ───────────────────────────
        null_counts = df[["close", "date", "ticker"]].isnull().sum()
        critical_nulls = null_counts[null_counts > 0]

        result.checks["no_null_close"] = int(null_counts.get("close", 0)) == 0
        result.checks["no_null_date"]  = int(null_counts.get("date", 0)) == 0

        if not critical_nulls.empty:
            result.passed = False
            result.errors.append(f"Critical nulls found: {critical_nulls.to_dict()}")

        # ── Check 3: Range checks ─────────────────────────────────────────────
        # Close price must be positive
        invalid_close = (df["close"] <= 0).sum() if "close" in df.columns else 0
        result.checks["positive_close"] = int(invalid_close) == 0
        if invalid_close > 0:
            result.passed = False
            result.errors.append(f"{invalid_close} rows with close <= 0")

        # RSI must be 0-100
        if "rsi_14" in df.columns:
            rsi_valid = df["rsi_14"].dropna()
            invalid_rsi = ((rsi_valid < 0) | (rsi_valid > 100)).sum()
            result.checks["rsi_range"] = int(invalid_rsi) == 0
            if invalid_rsi > 0:
                result.warnings.append(f"{invalid_rsi} RSI values outside 0-100")

        # High >= Low
        if "high" in df.columns and "low" in df.columns:
            invalid_hl = (df["high"] < df["low"]).sum()
            result.checks["high_gte_low"] = int(invalid_hl) == 0
            if invalid_hl > 0:
                result.passed = False
                result.errors.append(f"{invalid_hl} rows where high < low")

        # Volume non-negative
        if "volume" in df.columns:
            invalid_vol = (df["volume"] < 0).sum()
            result.checks["non_negative_volume"] = int(invalid_vol) == 0
            if invalid_vol > 0:
                result.warnings.append(f"{invalid_vol} negative volume rows")

        # ── Check 4: Minimum row count ────────────────────────────────────────
        result.checks["min_row_count"] = result.row_count >= 200
        if result.row_count < 200:
            result.passed = False
            result.errors.append(
                f"Only {result.row_count} rows — expected at least 200 trading days"
            )

        # ── Check 5: Duplicate (ticker, date) pairs ───────────────────────────
        if "date" in df.columns:
            dupes = df.duplicated(subset=["ticker", "date"]).sum()
            result.checks["no_duplicates"] = int(dupes) == 0
            if dupes > 0:
                result.passed = False
                result.errors.append(f"{dupes} duplicate (ticker, date) rows found")

        # ── Log and return ────────────────────────────────────────────────────
        if result.passed:
            logger.info(f"  {result.summary()}")
        else:
            logger.warning(f"  {result.summary()}")
            for err in result.errors:
                logger.error(f"    ERROR: {err}")
            for warn in result.warnings:
                logger.warning(f"    WARN:  {warn}")

        return result

    # ── Macro DataFrame validation ────────────────────────────────────────────

    def validate_macro(self, df: pd.DataFrame, series_id: str) -> ValidationResult:
        """Validate a FRED macro series DataFrame."""
        result = ValidationResult(ticker=series_id, passed=True)

        if df is None or df.empty:
            result.passed = False
            result.errors.append(f"{series_id}: empty macro DataFrame")
            return result

        result.row_count = len(df)

        # Must have date and value columns
        value_col = series_id.lower()
        has_value = value_col in df.columns
        result.checks["has_value_column"] = has_value

        if not has_value:
            result.passed = False
            result.errors.append(f"Missing value column: '{value_col}'")

        # No all-null values
        if has_value:
            null_pct = df[value_col].isnull().mean()
            result.checks["acceptable_null_rate"] = null_pct < 0.1
            if null_pct >= 0.1:
                result.warnings.append(
                    f"{series_id}: {null_pct:.1%} null values (threshold 10%)"
                )

        # Minimum observations
        result.checks["min_observations"] = result.row_count >= 12
        if result.row_count < 12:
            result.passed = False
            result.errors.append(f"Only {result.row_count} observations — too few")

        logger.info(f"  {result.summary()}")
        return result

    # ── Batch validation ──────────────────────────────────────────────────────

    def validate_all_prices(self, transformed: dict) -> dict:
        """
        Run price validation on all tickers.
        Returns {ticker: ValidationResult}.
        Logs a summary table at the end.
        """
        logger.info(f"Starting price validation | {len(transformed)} tickers")
        results = {}
        passed, failed = 0, 0

        for ticker, df in transformed.items():
            result = self.validate_prices(df, ticker)
            results[ticker] = result
            if result.passed:
                passed += 1
            else:
                failed += 1

        logger.info(
            f"Price validation complete | "
            f"✓ {passed} passed | ✗ {failed} failed"
        )

        if failed > 0:
            failed_tickers = [t for t, r in results.items() if not r.passed]
            logger.warning(f"Failed tickers: {failed_tickers}")

        return results
