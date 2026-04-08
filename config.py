"""
config.py — All pipeline parameters. No hardcoded secrets.

Set environment variables in .env:
  DATABENTO_API_KEY=db-xxxxxxxxxxxx
  GCS_BUCKET=my-bucket-name
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
_tickers_csv = Path(__file__).parent / "tickers.csv"
TICKERS: list[str] = (
    pd.read_csv(_tickers_csv, header=None)[0].str.strip().tolist()
    if _tickers_csv.exists()
    else []
)

# ── Date range ────────────────────────────────────────────────────────────────
START_DATE = "2022-01-01"
END_DATE   = "2024-12-31"

# ── Schemas ───────────────────────────────────────────────────────────────────
SCHEMAS = ["tbbo", "mbp-10"]

# Expected columns per schema (after to_df() and symbol mapping).
# Presence of these columns is checked in validator check D.
REQUIRED_COLS: dict[str, list[str]] = {
    "tbbo": [
        "ts_recv", "ts_event", "symbol", "price", "size",
        "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00",
    ],
    "mbp-10": [
        "ts_recv", "ts_event", "symbol", "price", "size",
        "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00",
        "bid_px_01", "ask_px_01",  # require at least 2 levels
    ],
}

# ── Venues (15 US lit exchanges) ──────────────────────────────────────────────
VENUES: list[str] = [
    "XNAS.ITCH",
    "ARCX.PILLAR",
    "BATS.PITCH",
    "EDGX.PITCH",
    "XNYS.PILLAR",
    "BATY.PITCH",
    "EDGA.PITCH",
    "XBOS.ITCH",
    "XPSX.ITCH",
    "MEMX.MEMOIR",
    "EPRL.DOM",
    "IEXG.TOPS",
    "XCHI.PILLAR",
    "XASE.PILLAR",
    "XCIS.TRADESBBO",
]

# ── Session filter ────────────────────────────────────────────────────────────
ET_TZ         = "America/New_York"
SESSION_START = "09:30:00"   # inclusive
SESSION_END   = "16:00:00"   # exclusive

# ── Validator thresholds ──────────────────────────────────────────────────────
PRICE_MEDIAN_MAX    = 10_000    # check A: fixed-point guard
STALE_QUOTE_SECS    = 60        # check G: ts_recv - ts_event > 60s → stale
CORP_EVENT_THRESH   = 0.30      # check H: VWAP shift > 30% → possible split

# ── Parallelism ───────────────────────────────────────────────────────────────
MAX_DOWNLOAD_WORKERS  = 4
MAX_UPLOAD_WORKERS    = 6
MAX_VALIDATE_WORKERS  = 4   # ThreadPoolExecutor workers for validate_all()

# ── Paths ─────────────────────────────────────────────────────────────────────
_ROOT      = Path(__file__).parent
CACHE_DIR  = _ROOT / "cache"
LOG_DIR    = _ROOT / "logs"
QUARANTINE_DIR = CACHE_DIR / "quarantine"

# ── Parquet settings ──────────────────────────────────────────────────────────
PARQUET_COMPRESSION  = "snappy"
PARQUET_ROW_GROUP    = 50_000

# ── GCS layout ───────────────────────────────────────────────────────────────
# gs://{GCS_BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
GCS_PREFIX = "raw"
GCS_TEST_PREFIX = "test"

# ── BigQuery ──────────────────────────────────────────────────────────────────
BQ_DATASET = os.getenv("BQ_DATASET", "market_data")
BQ_PROJECT = os.getenv("GCP_PROJECT", "")

# ── Retry ─────────────────────────────────────────────────────────────────────
RETRY_ATTEMPTS    = 3
RETRY_MIN_WAIT    = 4    # seconds
RETRY_MAX_WAIT    = 60   # seconds

# ── Dry-run size estimates (heuristic, no API calls) ─────────────────────────
# tbbo: ~7k records/chunk, ~0.7 MB parquet/chunk
# mbp-10: ~7k records/chunk, ~5 MB parquet/chunk (10 levels × more fields)
ESTIMATE_RECORDS_PER_CHUNK = {"tbbo": 7_000, "mbp-10": 7_000}
ESTIMATE_MB_PER_CHUNK      = {"tbbo": 0.7,   "mbp-10": 5.0}
ESTIMATE_USD_PER_GB        = {"tbbo": 0.50,  "mbp-10": 2.00}
