"""
config.py — All pipeline parameters. No hardcoded secrets.

Set environment variables in pipeline/.env:
  DATABENTO_API_KEY=db-xxxxxxxxxxxx
  GCS_BUCKET=my-bucket-name
  GCP_PROJECT=my-gcp-project
  BQ_DATASET=market_data          # optional

Schema strategy
───────────────
  Lit exchanges  → "mbp-10"  (10-level order book, full quote + trade data)
  ATS / TRF      → "trades"  (dark pools don't publish order books; trade-only)

Verify ATS dataset IDs for your account before running:
  python -c "import databento as db; print(db.Historical().metadata.list_datasets())"
"""
from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── API credentials (never commit) ───────────────────────────────────────────
DATABENTO_API_KEY: str = os.getenv("DATABENTO_API_KEY", "")
GCS_BUCKET: str        = os.getenv("GCS_BUCKET", "")

# ── Universe ──────────────────────────────────────────────────────────────────
_tickers_file = Path(__file__).parent / "tickers.txt"
TICKERS: list[str] = (
    [t.strip() for t in _tickers_file.read_text().splitlines() if t.strip()]
    if _tickers_file.exists()
    else []
)

# ── Date range ────────────────────────────────────────────────────────────────
START_DATE = "2022-01-01"
END_DATE   = "2025-12-31"

# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────

LIT_SCHEMA   = "mbp-10"    # Market-by-price, 10 levels — full bid/ask book + trades
ATS_SCHEMA   = "trades"    # Trades only — dark pools have no public order book
OHLCV_SCHEMA = "ohlcv-1h"  # OHLCV hourly bars — compact, cheap, good for lit venues

# ─────────────────────────────────────────────────────────────────────────────
# Venues
# ─────────────────────────────────────────────────────────────────────────────

# 15 US lit (NMS) exchanges — all support mbp-10
LIT_VENUES: list[str] = [
    "XNAS.ITCH",        # Nasdaq (largest US exchange by volume)
    "ARCX.PILLAR",      # NYSE Arca (ETF primary listing venue)
    "BATS.PITCH",       # Cboe BZX (formerly BATS BZX)
    "EDGX.PITCH",       # Cboe EDGX (maker-taker, high rebate)
    "XNYS.PILLAR",      # NYSE (legacy floor exchange)
    "BATY.PITCH",       # Cboe BYX (taker-maker, inverted fee)
    "EDGA.PITCH",       # Cboe EDGA (low-fee, maker-taker)
    "XBOS.ITCH",        # Nasdaq BX (Boston, inverted fee)
    "XPSX.ITCH",        # Nasdaq PSX (Philadelphia, inverted fee)
    "MEMX.MEMOIR",      # MEMX (Member Exchange, launched 2020)
    "EPRL.DOM",         # MIAX Pearl Equities (launched 2020)
    "IEXG.TOPS",        # IEX (Investors Exchange, speed bump)
    "XCHI.PILLAR",      # NYSE Chicago (formerly CHX)
    "XASE.PILLAR",      # NYSE American (formerly AMEX, small-cap)
    "XCIS.TRADESBBO",   # NYSE National (formerly NSX)
]

# ATS and Non-ATS OTC venues — trade-only (no order book)
# ⚠ VERIFY these dataset IDs against your Databento subscription:
#   python -c "import databento as db; print(db.Historical().metadata.list_datasets())"
ATS_VENUES: list[str] = [
    # ── FINRA Trade Reporting Facilities (TRFs) ───────────────────────────────
    # TRFs aggregate trade reports from all registered ATSs and broker-dealers.
    # A single TRF dataset captures ~30 ATSs at once (Virtu, Goldman Sigma X,
    # JPMorgan, Citadel, UBS, Instinet, Liquidnet, etc.).
    "FINN.NLS",         # FINRA/Nasdaq TRF Carteret — largest, ~30% of OTC ADV
    "FNYX.NLS",         # FINRA/NYSE TRF — second largest OTC reporting facility
    "FINY.NLS",         # FINRA/Nasdaq TRF Chicago — regional OTC trades

    # ── Direct ATS feeds ─────────────────────────────────────────────────────
    "OCEA.MEMOIR",      # Blue Ocean ATS — only ATS with its own Databento feed
                        # (off-hours equities trading, overnight market)
]

# Combined map: schema → venues (used by download orchestrator)
SCHEMA_VENUE_MAP: dict[str, list[str]] = {
    LIT_SCHEMA:   LIT_VENUES,
    ATS_SCHEMA:   ATS_VENUES,
    OHLCV_SCHEMA: LIT_VENUES,  # ohlcv-1h available on all 15 lit venues
}

# ─────────────────────────────────────────────────────────────────────────────
# Required columns per schema (checked by validator check D)
# ─────────────────────────────────────────────────────────────────────────────
REQUIRED_COLS: dict[str, list[str]] = {
    "mbp-10": [
        "ts_recv", "ts_event", "symbol", "price", "size",
        "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00",
        "bid_px_01", "ask_px_01",   # require at least 2 book levels
    ],
    "trades": [
        "ts_recv", "ts_event", "symbol", "price", "size",
    ],
}

# ── Session filter ────────────────────────────────────────────────────────────
ET_TZ         = "America/New_York"
SESSION_START = "09:30:00"   # inclusive  (regular session open)
SESSION_END   = "16:00:00"   # exclusive  (regular session close)
# Blue Ocean ATS trades after-hours — set SESSION_END = "20:00:00" if needed

# ── Validator thresholds ──────────────────────────────────────────────────────
# Check A: fixed-point price guard
#   Databento returns prices already in dollars. If median(price) > this
#   threshold, the data is still in fixed-point (e.g., 1/10000 units → abort).
PRICE_MEDIAN_MAX = 10_000       # dollars

# Check G: stale quote detection
#   ts_recv is when the data arrived at Databento's gateway.
#   ts_event is the exchange-stamped event time.
#   A gap > STALE_QUOTE_SECS suggests the quote was held/delayed.
STALE_QUOTE_SECS = 60           # seconds

# Check H: corporate event detection
#   A daily VWAP shift > this threshold vs the prior day flags a probable
#   stock split, dividend, or M&A announcement. Data is still valid but
#   downstream models should handle the discontinuity.
CORP_EVENT_THRESH = 0.30        # 30 % VWAP day-over-day change

# ── Parallelism ───────────────────────────────────────────────────────────────
MAX_DOWNLOAD_WORKERS  = 12
MAX_UPLOAD_WORKERS    = 6
MAX_VALIDATE_WORKERS  = 4

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT          = Path(__file__).parent
CACHE_DIR      = _ROOT / "cache"
LOG_DIR        = _ROOT / "logs"
QUARANTINE_DIR = CACHE_DIR / "quarantine"

# ── Parquet settings ──────────────────────────────────────────────────────────
PARQUET_COMPRESSION = "snappy"
PARQUET_ROW_GROUP   = 50_000

# ── GCS layout ───────────────────────────────────────────────────────────────
# gs://{GCS_BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
GCS_PREFIX      = "raw"
GCS_TEST_PREFIX = "test"

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_DATASET = os.getenv("BQ_DATASET", "market_data")
BQ_PROJECT = os.getenv("GCP_PROJECT", "")

# ── Retry ─────────────────────────────────────────────────────────────────────
RETRY_ATTEMPTS  = 3
RETRY_MIN_WAIT  = 4    # seconds
RETRY_MAX_WAIT  = 60   # seconds

# ── Dry-run size estimates (heuristic) ────────────────────────────────────────
# mbp-10 is large: 10 price levels × bid+ask price+size+count = 40 fields per row
# trades is compact: ~5 fields per row
ESTIMATE_RECORDS_PER_CHUNK: dict[str, int] = {
    "mbp-10":   7_000,
    "trades":   15_000,
    "ohlcv-1h": 130,    # ~21 trading days × 6.5 h/day per month
}
ESTIMATE_MB_PER_CHUNK: dict[str, float] = {
    "mbp-10":   5.0,
    "trades":   0.5,
    "ohlcv-1h": 0.01,   # OHLCV bars are tiny (~5 fields, 130 rows/month)
}
ESTIMATE_USD_PER_GB: dict[str, float] = {
    "mbp-10":   2.00,
    "trades":   0.50,
    "ohlcv-1h": 0.30,
}

# ── Backward-compat aliases (used by legacy code / test_pipeline) ─────────────
VENUES  = LIT_VENUES
SCHEMAS = [LIT_SCHEMA, ATS_SCHEMA, OHLCV_SCHEMA]

# Venues que no tienen data completa desde START_DATE
VENUE_EARLIEST_START: dict[str, str] = {
    "IEXG.TOPS":      "2023-04-01",
    "MEMX.MEMOIR":    "2023-04-01",
    "XCIS.TRADESBBO": "2023-04-01",
    "EPRL.DOM":       "2023-04-01",
    "XCHI.PILLAR":    "2023-04-01",
}

# ── OHLCV data directory (used by validator) ──────────────────────────────────
OHLCV_DATA_DIR = CACHE_DIR / "ohlcv_1h"

# ── Required columns for ohlcv-1h (check D) ──────────────────────────────────
REQUIRED_COLS["ohlcv-1h"] = ["open", "high", "low", "close", "volume"]
