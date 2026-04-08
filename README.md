# Market Data Pipeline

Production-grade pipeline to download raw tick data from [Databento](https://databento.com), validate each chunk, and upload to **Google Cloud Storage + BigQuery**.

**Universe:** 144 US equities × 15 lit exchanges × 36 months × 2 schemas (TBBO / MBP-10)  
**Max chunks per full run:** ~155,520

---

## Architecture

```
Databento API
      │
      │  download (parallel, 4 workers)
      ▼
cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
      │
      │  validate (parallel, 4 workers)
      ▼
8 quality checks → PASS / WARNING / ABORT (quarantine)
      │
      │  upload (parallel, 6 workers)
      ▼
gs://{BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
      │
      ▼
BigQuery: market_data.{schema}
  partitioned by DATE(ts_recv)
  clustered by (symbol, venue)
```

---

## Files

| File | Description |
|---|---|
| `config.py` | All parameters — reads secrets from `.env` |
| `downloader.py` | Parallel chunk download with retry & atomic writes |
| `validator.py` | 8-check data quality suite |
| `uploader.py` | Parallel GCS upload + BigQuery load jobs |
| `main.py` | CLI: `download` / `validate` / `upload` |
| `test_pipeline.py` | Integration smoke test (3 tickers, 3 days) |
| `tickers.csv` | 144 ticker symbols (one per line) |

---

## Setup

```bash
pip install databento pandas pyarrow tenacity tqdm python-dotenv \
            google-cloud-storage google-cloud-bigquery pytz
```

Create `pipeline/.env`:

```
DATABENTO_API_KEY=db-xxxxxxxxxxxx
GCS_BUCKET=my-bucket-name
GCP_PROJECT=my-gcp-project
BQ_DATASET=market_data          # optional, default: market_data
```

---

## Usage

### 1. Smoke test (run this first)

```bash
cd pipeline/
python test_pipeline.py
```

Downloads 3 tickers × 2 venues × 3 days, validates, uploads to `gs://{BUCKET}/test/`, and verifies MD5 roundtrip.

### 2. Download

```bash
# Preview plan (no API calls, no cost)
python main.py download --dry-run

# Download one schema
python main.py download --schema tbbo

# Download all schemas in parallel
python main.py download

# Force re-download even if cached
python main.py download --force
```

### 3. Validate

```bash
# Validate all cached chunks
python main.py validate

# Validate a single symbol
python main.py validate --symbol AAPL

# Re-download quarantined chunks automatically
python main.py validate --fix-quarantine
```

### 4. Upload to GCS + BigQuery

```bash
# Upload all validated chunks
python main.py upload

# Skip BigQuery (GCS only)
python main.py upload --no-bq

# Overwrite existing GCS objects
python main.py upload --overwrite
```

---

## Data Quality Checks

| Check | Severity | Rule |
|---|---|---|
| A | **ABORT** | `median(price) > 10,000` — fixed-point not converted |
| B | WARNING | Negative or zero prices / sizes |
| C | **ABORT** | `ts_recv` not monotonically non-decreasing |
| D | **ABORT** | Expected schema columns missing |
| E | PASS | Session filter: keep only 09:30–16:00 ET rows |
| F | WARNING | Duplicate `(ts_recv, venue)` nanosecond timestamps |
| G | WARNING | `ts_recv − ts_event > 60s` (stale quote) |
| H | WARNING | Daily VWAP shift > 30% (possible corporate event / split) |

- **ABORT** → chunk moved to `cache/quarantine/`, skipped on upload.
- **WARNING** → uploaded with warning flags recorded in manifest JSON.
- Report saved as `{YYYY-MM}_validation.json` alongside each parquet.

---

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `TICKERS` | 144 symbols | Read from `tickers.csv` |
| `VENUES` | 15 exchanges | All US lit venues |
| `SCHEMAS` | `["tbbo", "mbp-10"]` | L1 and L2 order book |
| `START_DATE` | `2022-01-01` | Download window start |
| `END_DATE` | `2024-12-31` | Download window end |
| `MAX_DOWNLOAD_WORKERS` | `4` | Concurrent Databento API calls |
| `MAX_UPLOAD_WORKERS` | `6` | Concurrent GCS upload threads |
| `MAX_VALIDATE_WORKERS` | `4` | Concurrent validation threads |
| `RETRY_ATTEMPTS` | `3` | Exponential backoff retries |
| `RETRY_MIN_WAIT` | `4s` | Backoff floor |
| `RETRY_MAX_WAIT` | `60s` | Backoff ceiling |
| `PARQUET_COMPRESSION` | `snappy` | Parquet codec |
| `PRICE_MEDIAN_MAX` | `10,000` | Fixed-point price guard |
| `STALE_QUOTE_SECS` | `60` | Stale quote threshold |
| `CORP_EVENT_THRESH` | `0.30` | VWAP shift → possible split |

---

## Venues (15 US Lit Exchanges)

| MIC | Protocol |
|---|---|
| XNAS | ITCH |
| ARCX | PILLAR |
| BATS | PITCH |
| EDGX | PITCH |
| XNYS | PILLAR |
| BATY | PITCH |
| EDGA | PITCH |
| XBOS | ITCH |
| XPSX | ITCH |
| MEMX | MEMOIR |
| EPRL | DOM |
| IEXG | TOPS |
| XCHI | PILLAR |
| XASE | PILLAR |
| XCIS | TRADESBBO |

---

## Cache & GCS Layout

**Local cache:**
```
pipeline/cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
pipeline/cache/{schema}/{symbol}/{venue}/{YYYY-MM}_validation.json
pipeline/cache/quarantine/...
```

**GCS:**
```
gs://{BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
gs://{BUCKET}/raw/{schema}/{symbol}/{symbol}_manifest.json
```

**BigQuery:**
```
{PROJECT}.market_data.tbbo       -- partitioned by DATE(ts_recv), clustered by (symbol, venue)
{PROJECT}.market_data.mbp_10     -- same layout
```

---

## Cost & Size Estimates (dry-run output)

| Schema | Records/chunk | Size/chunk | API cost/GB |
|---|---|---|---|
| `tbbo` | ~7,000 | ~0.7 MB | ~$0.50 |
| `mbp-10` | ~7,000 | ~5 MB | ~$2.00 |

Run `python main.py download --dry-run` to get a full estimate before spending any API credits.
