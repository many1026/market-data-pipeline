# Databento Market Data Pipeline

Production-grade pipeline to download, validate, and upload US equity market data from
[Databento](https://databento.com) to Google Cloud Storage (GCS) and BigQuery.

---

## Universe & Scope

| Parameter | Value |
|---|---|
| **Tickers** | 297 US equities (`binary_score = 5` from tradability ranking) |
| **Date range** | 2022-01-01 → 2025-12-31 (4 years) |
| **Lit venues** | 15 NMS exchanges (see full list below) |
| **ATS / TRF venues** | 4 dark-pool venues (FINRA TRFs + Blue Ocean ATS) |
| **OHLCV schema** | `ohlcv-1h` — hourly OHLCV bars across all 15 lit venues |
| **LIT schema** | `mbp-10` — 10-level order book + trades (tick-by-tick) |
| **ATS schema** | `trades` — trade prints only (dark pools have no public book) |

**Actual API cost (queried live):** `$34.23` for 297 tickers × 15 venues × 4 years of `ohlcv-1h`.

---

## Why Three Schemas?

| Mode | Schema | Venue type | What you get |
|---|---|---|---|
| `ohlcv` | `ohlcv-1h` | 15 lit exchanges | Compact hourly OHLCV bars. ~130 bars/month/venue. Cheap (~$0.12/ticker for 4 years). Good for strategy screening and signal research. |
| `lit` | `mbp-10` | 15 lit exchanges | Full 10-level order book reconstructed in real time. Each row is a quote update or trade. ~7,000 rows/month/venue. Used for microstructure research, spread analysis, and adverse selection features. |
| `ats` | `trades` | 4 ATS / TRF venues | Dark-pool trade prints only. ATSs intentionally hide their books — only completed trades are reported to FINRA TRFs. ~15,000 rows/month/venue. Used for dark pool participation rate and fragmentation analysis. |

---

## Architecture

```
Databento Historical API
          │
          ├── 15 lit exchanges ──── schema: ohlcv-1h  (hourly bars, cheap, 4 years)
          │       XNAS.ITCH         schema: mbp-10    (10-level book, tick-by-tick)
          │       XNYS.PILLAR
          │       ARCX.PILLAR
          │       BATS.PITCH
          │       EDGX.PITCH
          │       EDGA.PITCH
          │       BATY.PITCH
          │       XBOS.ITCH
          │       XPSX.ITCH
          │       XASE.PILLAR
          │       IEXG.TOPS    ← ohlcv-1h available from 2023-04-01 only
          │       MEMX.MEMOIR  ← ohlcv-1h available from 2023-04-01 only
          │       XCIS.TRADESBBO ← ohlcv-1h available from 2023-04-01 only
          │       EPRL.DOM     ← ohlcv-1h available from 2023-04-01 only
          │       XCHI.PILLAR  ← ohlcv-1h available from 2023-04-01 only
          │
          └── 4 ATS / TRF venues ── schema: trades  (dark pool prints)
                  FINN.NLS   (FINRA/Nasdaq TRF Carteret — largest, ~30% OTC ADV)
                  FNYX.NLS   (FINRA/NYSE TRF)
                  FINY.NLS   (FINRA/Nasdaq TRF Chicago)
                  OCEA.MEMOIR (Blue Ocean ATS — off-hours equities)

          │ parallel download (ThreadPoolExecutor, configurable workers)
          │ chunk granularity: (schema, symbol, venue, YYYY-MM) → one parquet file
          │ atomic writes: .tmp → rename on success (no corrupt files on crash)
          │ retry on failure: 3 attempts, exponential backoff (4 s → 60 s)
          │ skip already-cached chunks automatically (safe to Ctrl+C and restart)
          ▼

cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
          │
          │ 8 data quality checks per chunk (parallel, 4 workers)
          │ checks: price scale, session filter, duplicate timestamps,
          │         required columns, timestamp ordering, stale quotes,
          │         zero-size rows, corporate event detection
          ▼

PASS → upload to GCS        WARNING → upload + flag        ABORT → quarantine locally

          │ MD5 checksum verified after every upload
          │ optional BigQuery external table load
          ▼

gs://{BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
gs://{BUCKET}/raw/{schema}/{symbol}/{symbol}_manifest.json
```

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone https://github.com/many1026/market-data-pipeline.git
cd market-data-pipeline
pip install -r requirements.txt
```

### 2. Configure credentials

Create a `.env` file in the project root (never commit this):

```bash
DATABENTO_API_KEY=db-xxxxxxxxxxxxxxxxxxxx
GCS_BUCKET=your-gcs-bucket-name
GCP_PROJECT=your-gcp-project-id
BQ_DATASET=market_data          # optional, defaults to "market_data"
```

### 3. Set your ticker universe

Edit `tickers.txt` — one ticker symbol per line:

```
AAPL
MSFT
NVDA
...
```

The pipeline currently ships with **297 tickers** selected by `binary_score = 5`
from a tradability ranking (liquidity × spread × market-cap composite score).

### 4. Estimate cost before downloading

```bash
python databento_cost_estimate.py
```

This queries the live Databento API for real prices with no data downloaded.
Example output for 297 tickers × 15 venues × 4 years of `ohlcv-1h`:

```
  XNAS.ITCH              $      3.55
  XNYS.PILLAR            $      2.98
  ...
  Total API cost     : $34.23
  Cost per ticker    : $0.12
  Est. parquet size  : ~2.09 GB
```

### 5. Dry run (no download, no cost)

```bash
python main.py download --mode ohlcv --dry-run
```

### 6. Download

```bash
# OHLCV hourly bars (cheapest, recommended first)
python main.py download --mode ohlcv

# Full order book tick data — lit exchanges only
python main.py download --mode lit

# Dark pool trade prints — ATS / TRF venues
python main.py download --mode ats

# All three schemas in parallel
python main.py download --mode all
```

**The download is fully resumable.** Already-cached chunks are skipped automatically.
You can safely Ctrl+C and restart at any time — no data is lost or re-downloaded.

### 7. Validate

```bash
python main.py validate --mode ohlcv
python main.py validate --mode ohlcv --symbol AAPL   # single ticker
python main.py validate --mode ohlcv --fix-quarantine  # re-download bad chunks
```

### 8. Upload to GCS (and optionally BigQuery)

```bash
python main.py upload --mode ohlcv
python main.py upload --mode ohlcv --no-bq     # GCS only, skip BigQuery
python main.py upload --mode ohlcv --overwrite  # re-upload existing objects
```

---

## Cache Layout

```
cache/
  ohlcv-1h/
    AAPL/
      XNAS_ITCH/
        2022-01.parquet   ← one file per (ticker, venue, month)
        2022-02.parquet
        ...
      XNYS_PILLAR/
        2022-01.parquet
        ...
  mbp-10/
    AAPL/
      XNAS_ITCH/
        2022-01.parquet
        ...
  trades/
    AAPL/
      FINN_NLS/
        2022-01.parquet
        ...
```

A **0-row parquet file is a valid cache entry** — it means the ticker was not traded
at that venue during that month. The pipeline will not re-download it.

---

## Data Quality Checks

The validator runs 8 checks on every cached parquet file:

| Check | Label | What it detects | Outcome on failure |
|---|---|---|---|
| A | Price scale | Median price > $10,000 (still in fixed-point units) | ABORT → quarantine |
| B | Session filter | Data outside 09:30–16:00 ET | WARNING |
| C | Duplicate timestamps | Same `ts_recv` appears more than once | ABORT → quarantine |
| D | Required columns | Missing mandatory fields for schema | ABORT → quarantine |
| E | Timestamp ordering | `ts_recv` not monotonically increasing | ABORT → quarantine |
| F | Stale quotes | `ts_recv − ts_event > 60 s` (quote held at gateway) | WARNING |
| G | Zero-size rows | Rows where `size == 0` | WARNING |
| H | Corporate events | Daily VWAP shifts > 30% day-over-day (split / div / M&A) | WARNING |

Chunks that ABORT are moved to `cache/quarantine/` and excluded from GCS upload.
Use `--fix-quarantine` to automatically re-download quarantined chunks.

---

## Key Bugs Fixed

### Bug 1 — `tickers.csv` hardcoded (CRITICAL)
**Symptom:** `TICKERS` was always an empty list — nothing ever downloaded.  
**Root cause:** `config.py` read from `tickers.csv` (which didn't exist) instead of `tickers.txt`.  
**Fix:** Changed to `tickers.txt` with simple line-by-line reading (no pandas dependency for this).

### Bug 2 — 0-row parquets caused infinite re-downloads (CRITICAL)
**Symptom:** Every month where a ticker didn't trade at a venue was re-downloaded
on every pipeline restart, burning API quota on empty responses.  
**Root cause:** `cache_ok()` returned `False` for files with 0 rows, treating
"symbol absent at venue" as a cache miss instead of a valid result.  
**Fix:** Any readable parquet file (including 0-row) is now a valid cache hit.
Only missing or corrupt files trigger a re-download.

### Bug 3 — Partial-coverage venues caused 422 errors (CRITICAL for `ohlcv-1h`)
**Symptom:** Five lit venues (IEX, MEMX, NYSE National, MIAX Pearl, NYSE Chicago)
returned HTTP 422 errors for every month before 2023-03-28, wasting 3 retries
× exponential backoff × 22,275 invalid chunks = potentially 74+ hours of wasted wall time.  
**Root cause:** `generate_chunks()` used a single global `START_DATE = 2022-01-01`
for all venues, but these five venues only have `ohlcv-1h` data from 2023-03-28.  
**Fix:** Added `VENUE_EARLIEST_START` dict to `config.py` and updated
`generate_chunks()` to clamp each venue's start date, so invalid chunks are
never generated at all.

### Bug 4 — `ohlcv-1h` schema missing from config dicts
**Symptom:** `KeyError` in dry-run mode when using `--mode ohlcv`.  
**Fix:** Added `ohlcv-1h` entries to all schema-keyed dicts
(`ESTIMATE_RECORDS_PER_CHUNK`, `ESTIMATE_MB_PER_CHUNK`, `ESTIMATE_USD_PER_GB`,
`SCHEMA_VENUE_MAP`).

---

## Configuration Reference (`config.py`)

| Variable | Default | Description |
|---|---|---|
| `START_DATE` | `2022-01-01` | Download start (inclusive) |
| `END_DATE` | `2025-12-31` | Download end (inclusive) |
| `MAX_DOWNLOAD_WORKERS` | `4` | Parallel download threads (increase to 12 on Cloud Shell) |
| `MAX_UPLOAD_WORKERS` | `6` | Parallel GCS upload threads |
| `MAX_VALIDATE_WORKERS` | `4` | Parallel validation threads |
| `PARQUET_COMPRESSION` | `snappy` | Parquet compression codec |
| `RETRY_ATTEMPTS` | `3` | API retry attempts per chunk |
| `RETRY_MIN_WAIT` | `4 s` | Minimum backoff between retries |
| `RETRY_MAX_WAIT` | `60 s` | Maximum backoff between retries |
| `PRICE_MEDIAN_MAX` | `$10,000` | Check A threshold |
| `STALE_QUOTE_SECS` | `60 s` | Check F threshold |
| `CORP_EVENT_THRESH` | `30%` | Check H threshold (VWAP day-over-day) |
| `VENUE_EARLIEST_START` | see config | Per-venue earliest date for `ohlcv-1h` |

---

## Running on GCP Cloud Shell

Cloud Shell provides a free Linux VM with fast egress to GCS. Recommended setup:

```bash
# 1. Upload your zip and unpack
unzip pipeline.zip -d pipeline && cd pipeline

# 2. Install dependencies
pip install -r requirements.txt

# 3. Create .env with your credentials
echo "DATABENTO_API_KEY=db-xxxx" >> .env
echo "GCS_BUCKET=your-bucket" >> .env
echo "GCP_PROJECT=your-project" >> .env

# 4. Increase workers for Cloud Shell (faster than local)
sed -i 's/MAX_DOWNLOAD_WORKERS  = 4/MAX_DOWNLOAD_WORKERS  = 12/' config.py

# 5. Run inside tmux so the session survives disconnect
tmux new -s download
python3 main.py download --mode ohlcv

# Detach without killing: Ctrl+B, then D
# Reattach later:
tmux attach -t download

# 6. After download completes, upload to GCS
python3 main.py upload --mode ohlcv --no-bq
```

---

## GCS Layout

```
gs://{BUCKET}/
  raw/
    ohlcv-1h/
      AAPL/
        XNAS_ITCH/
          2022-01.parquet
        XNYS_PILLAR/
          2022-01.parquet
        ...
      MSFT/
        ...
    mbp-10/
      ...
    trades/
      ...
```

---

## File Reference

| File | Description |
|---|---|
| `main.py` | CLI entrypoint — `download`, `validate`, `upload` subcommands |
| `config.py` | All pipeline parameters and constants |
| `downloader.py` | Databento API client, chunk generation, parallel download, atomic write |
| `validator.py` | 8 data quality checks per chunk, quarantine logic |
| `uploader.py` | GCS upload with MD5 verification, optional BigQuery load |
| `test_pipeline.py` | Integration test — small sample download + validate + upload |
| `databento_cost_estimate.py` | Live cost estimation via Databento API (no data downloaded) |
| `tickers.txt` | 297-ticker universe (`binary_score = 5`) |
| `requirements.txt` | Python dependencies |

---

## Requirements

```
databento>=0.46
pandas
pyarrow
google-cloud-storage
google-cloud-bigquery
tenacity
tqdm
python-dotenv
```

Install with:
```bash
pip install -r requirements.txt
```
