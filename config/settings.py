import os
from pathlib import Path
from dotenv import load_dotenv

# ── Directory layout ────────────────────────────────────────────────────────
BASE_DIR           = Path(__file__).resolve().parent.parent
DATA_RAW_DIR       = BASE_DIR / "data" / "raw"
DATA_PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR           = BASE_DIR / "logs"

# Ensure all raw subdirs exist on import
for _source in ("yfinance", "fred", "alphavantage"):
    (DATA_RAW_DIR / _source).mkdir(parents=True, exist_ok=True)
(DATA_PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(BASE_DIR / ".env")


class Settings:
    # ── API Credentials ──────────────────────────────────────────────────────
    FRED_API_KEY:       str = os.getenv("FRED_API_KEY", "")
    ALPHA_VANTAGE_KEY:  str = os.getenv("ALPHA_VANTAGE_KEY", "")
    DATABASE_URL:       str = os.getenv("DATABASE_URL", "")
    LOG_LEVEL:          str = os.getenv("LOG_LEVEL", "INFO")
    ENVIRONMENT:        str = os.getenv("ENVIRONMENT", "development")

    # ── S&P 500 universe — 30 stocks across 10 sectors ───────────────────────
    TICKERS: dict = {
        "Technology":             ["AAPL", "MSFT", "NVDA", "GOOGL", "META", "AVGO"],
        "Financials":             ["JPM", "BAC", "WFC", "GS", "MS"],
        "Healthcare":             ["UNH", "JNJ", "PFE", "ABBV"],
        "Consumer Discretionary": ["AMZN", "TSLA", "HD", "MCD"],
        "Energy":                 ["XOM", "CVX"],
        "Industrials":            ["CAT", "BA", "HON"],
        "Communication":          ["T", "VZ"],
        "Materials":              ["LIN"],
        "Real Estate":            ["AMT"],
        "Utilities":              ["NEE"],
    }

    # Flat list of all tickers — derived automatically, never manually maintain
    ALL_TICKERS: list = [t for tickers in TICKERS.values() for t in tickers]
    BENCHMARK_TICKER: str = "^GSPC"  # S&P 500 index for relative performance

    # ── FRED macro series ────────────────────────────────────────────────────
    FRED_SERIES: dict = {
        "FEDFUNDS": "Federal Funds Effective Rate",
        "CPIAUCSL": "CPI — All Urban Consumers",
        "GS10":     "10-Year Treasury Constant Maturity Rate",
        "UNRATE":   "Unemployment Rate",
        "VIXCLS":   "CBOE Volatility Index (VIX)",
    }

    # ── Alpha Vantage — top 5 by market cap only (25 req/day limit) ──────────
    AV_TICKERS: list = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN"]

    # ── Pipeline config ──────────────────────────────────────────────────────
    HISTORICAL_START:  str = "2022-01-01"   # ~3 years of history
    MAX_RETRIES:       int = 3
    RETRY_DELAY_SEC:   int = 5              # Base delay for exponential backoff

    def validate(self) -> None:
        """
        Call this once at pipeline startup.
        Fails loudly and early — never silently proceeds with missing credentials.
        Production pattern: fail-fast on config errors before any API calls.
        """
        missing = [
            key for key, val in {
                "FRED_API_KEY":      self.FRED_API_KEY,
                "ALPHA_VANTAGE_KEY": self.ALPHA_VANTAGE_KEY,
                "DATABASE_URL":      self.DATABASE_URL,
            }.items() if not val
        ]
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                f"→ Copy .env.example to .env and fill in all values."
            )


settings = Settings()
