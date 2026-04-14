# Market Data Pipeline

Production-grade pipeline to download raw tick data from [Databento](https://databento.com), validate each chunk with 8 data quality checks, and upload to **Google Cloud Storage + BigQuery**.

**Universe:** 144 US equities  
**Lit venues:** 15 NMS exchanges — schema `mbp-10` (10-level order book)  
**ATS venues:** FINRA TRFs + direct ATS feeds — schema `trades` (dark pool prints)  
**Date range:** Apr 2022 – Apr 2026 (4 years, 48 months)

---

## Why two schemas?

| Venue type | Schema | Why |
|---|---|---|
| Lit exchanges | `mbp-10` | Registered exchanges publish a continuous, public order book. Databento can reconstruct 10 bid and 10 ask price levels in real time. |
| ATSs / TRFs | `trades` | Dark pools (ATSs) intentionally hide their order books. They only report completed trades to FINRA's Trade Reporting Facilities. No book data is available. |

---

## Architecture

```
Databento API
      │
      ├── 15 lit exchanges  → schema: mbp-10
      │         (XNAS, ARCX, BATS, EDGX, XNYS,
      │          BATY, EDGA, XBOS, XPSX, MEMX,
      │          EPRL, IEXG, XCHI, XASE, XCIS)
      │
      └── ATS / TRF venues  → schema: trades
                (FINN.NLS, FNYX.NLS, FINY.NLS, OCEA.MEMOIR)

      │ parallel download (4 workers per schema)
      │ atomic writes (.tmp → rename)
      │ retry on failure (3 attempts, 4 s → 60 s backoff)
      ▼

cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
      │
      │ 8 quality checks per chunk (parallel, 4 workers)
      ▼

PASS → upload        WARNING → upload + flag        ABORT → quarantine

      │
      │ parallel GCS upload (6 workers)
      │ MD5 checksum verified after every upload
      ▼

gs://{BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
gs://{BUCKET}/raw/{schema}/{symbol}/{symbol}_manifest.json
      │
      ▼
BigQuery: {PROJECT}.market_data.mbp_10
          {PROJECT}.market_data.trades
  partitioned by DATE(ts_recv)
  clustered by (symbol, venue)
```

---

## Files

| File | Description |
|---|---|
| `config.py` | All parameters — reads secrets from `.env`, never hardcoded |
| `downloader.py` | Parallel chunk download with retry, caching, dry-run |
| `validator.py` | 8 data quality checks (A–H) |
| `uploader.py` | Parallel GCS upload + BigQuery load + manifest update |
| `main.py` | CLI: `download` / `validate` / `upload` |
| `test_pipeline.py` | Parameterized LIT mbp-10 downloader — 15 venues × N tickers × 4 years |
| `tickers.csv` | 144 ticker symbols (one per line) |

---

## Setup

### 1. Install dependencies

```bash
pip install databento pandas pyarrow tenacity tqdm python-dotenv \
            google-cloud-storage google-cloud-bigquery pytz
```

### 2. Create `pipeline/.env`

```
DATABENTO_API_KEY=db-xxxxxxxxxxxx
GCS_BUCKET=my-bucket-name
GCP_PROJECT=my-gcp-project
BQ_DATASET=market_data          # optional, this is the default
```

### 3. Verify ATS dataset IDs

Before running any download, confirm which ATS datasets are available in your account:

```bash
cd pipeline/
python test_pipeline.py --list-only
```

This calls `db.Historical().metadata.list_datasets()` and prints which configured ATS venues (`FINN.NLS`, `FNYX.NLS`, etc.) appear in your subscription. If any are missing, remove them from `ATS_VENUES` in `config.py` before downloading.

---

## Venues

### Lit Exchanges (15) — schema: `mbp-10`

| Dataset ID | Exchange | Operator | Notes |
|---|---|---|---|
| `XNAS.ITCH` | Nasdaq | Nasdaq | Largest US exchange (~30% ADV) |
| `ARCX.PILLAR` | NYSE Arca | NYSE | ETF primary listing venue |
| `BATS.PITCH` | Cboe BZX | Cboe | Maker-taker, high volume |
| `EDGX.PITCH` | Cboe EDGX | Cboe | Maker-taker, inverted |
| `XNYS.PILLAR` | NYSE | NYSE | Legacy floor exchange |
| `BATY.PITCH` | Cboe BYX | Cboe | Taker-maker (inverted fee) |
| `EDGA.PITCH` | Cboe EDGA | Cboe | Low-fee |
| `XBOS.ITCH` | Nasdaq BX | Nasdaq | Boston, inverted fee |
| `XPSX.ITCH` | Nasdaq PSX | Nasdaq | Philadelphia, inverted fee |
| `MEMX.MEMOIR` | MEMX | MEMX | Member Exchange, 2020 |
| `EPRL.DOM` | MIAX Pearl | MIAX | Equities arm of options exchange |
| `IEXG.TOPS` | IEX | IEX | Speed bump (350 µs crumple zone) |
| `XCHI.PILLAR` | NYSE Chicago | NYSE | Formerly CHX |
| `XASE.PILLAR` | NYSE American | NYSE | Small-cap, formerly AMEX |
| `XCIS.TRADESBBO` | NYSE National | NYSE | Formerly NSX |

### ATS / TRF Venues — schema: `trades`

| Dataset ID | Venue | Description |
|---|---|---|
| `FINN.NLS` | FINRA/Nasdaq TRF Carteret | Largest trade reporting facility. Aggregates prints from ~30 ATSs (Virtu MatchIt, Citadel Apogee, JPMorgan, Goldman Sigma X, UBS ATS, Instinet, Liquidnet, and others). ~30% of consolidated ADV. |
| `FNYX.NLS` | FINRA/NYSE TRF | Second trade reporting facility. ATSs that route through the NYSE TRF. |
| `FINY.NLS` | FINRA/Nasdaq TRF Chicago | Regional TRF, smaller volume. |
| `OCEA.MEMOIR` | Blue Ocean ATS | Only ATS with a direct Databento feed. Specializes in off-hours equities (overnight trading, 8 PM – 4 AM ET). |

> **Note:** ATSs don't have their own feeds — they submit trade reports to FINRA TRFs. Accessing `FINN.NLS` gives you the aggregated tape of all ATS prints that reported to the Nasdaq TRF that day. You can filter to a specific ATS by its MPID (Market Participant Identifier) using the `symbols` parameter.

---

## Usage

### test_pipeline.py — LIT mbp-10 full download

Downloads `mbp-10` data for any set of tickers across all 15 LIT US venues for the full 4-year window (2022-04-13 → 2026-04-13). Includes validation and optional GCS upload.

```bash
cd pipeline/

# Verify API key + available datasets (no download)
python test_pipeline.py --list-only

# Estimate cost/time before spending API credits (no download)
python test_pipeline.py --tickers AAPL,MSFT,NVDA --dry-run

# Download specific tickers
python test_pipeline.py --tickers AAPL,MSFT,NVDA

# Download all tickers from tickers.csv
python test_pipeline.py

# Skip GCS upload step
python test_pipeline.py --tickers AAPL --skip-upload

# Force re-download even if cache exists
python test_pipeline.py --tickers AAPL --force
```

### Step 1 — Dry run (free, no API calls)

```bash
python main.py download --mode lit --dry-run    # see lit plan + cost estimate
python main.py download --mode ats --dry-run    # see ATS plan + cost estimate
python main.py download --mode all --dry-run    # see both
```

### Step 2 — Download

```bash
# Lit exchanges (mbp-10) — large files, ~5 MB/chunk
python main.py download --mode lit

# ATS/TRF (trades) — compact files, ~0.5 MB/chunk
python main.py download --mode ats

# Both in parallel
python main.py download --mode all

# Force re-download even if cache exists
python main.py download --mode lit --force
```

### Step 3 — Validate

```bash
python main.py validate --mode all

# Single symbol only
python main.py validate --mode lit --symbol AAPL

# Re-download quarantined chunks automatically
python main.py validate --mode all --fix-quarantine
```

### Step 4 — Upload to GCS + BigQuery

```bash
python main.py upload --mode all

# GCS only (skip BigQuery)
python main.py upload --mode all --no-bq

# Overwrite existing GCS objects
python main.py upload --mode all --overwrite
```

---

## Data Quality Checks (Security Filters)

Every downloaded chunk goes through 8 checks before it can be uploaded.
Checks are run in parallel (`ThreadPoolExecutor`, 4 workers).

### Severity levels

| Level | Action |
|---|---|
| **ABORT** | Chunk is **quarantined** — moved to `cache/quarantine/`, never uploaded. Can be re-downloaded with `--fix-quarantine`. |
| **WARNING** | Chunk is **uploaded** but flagged in the manifest JSON. Downstream code should handle the anomaly. |
| **PASS** | Check passed cleanly. |

---

### Check A — Fixed-Point Price Guard `[ABORT]`

**What it checks:** `median(price) > 10,000`

**Why it exists:**  
Databento returns prices as floating-point dollars. However, some raw exchange feeds encode prices in fixed-point with a divisor (e.g., 1/10,000 — so $10.00 is stored as `100,000`). If `to_df()` fails to convert, prices will be ~10,000× too large. This check catches that before the data poisons any downstream model.

**What triggers it:**  
Any chunk where the median trade price exceeds $10,000. This is very rare for normal equities (only for stocks like NVR or BKNG) so the threshold effectively detects encoding errors.

**Threshold:** `PRICE_MEDIAN_MAX = 10_000` (adjustable in `config.py`)

---

### Check B — Negative / Zero Prices and Sizes `[WARNING]`

**What it checks:** Count of rows where `price ≤ 0` or `size ≤ 0`.

**Why it exists:**  
A trade with zero price or zero shares is economically impossible. These rows typically come from erroneous quotes, cancelled prints that leaked through, or feed glitches. They don't abort the chunk (there can be isolated bad rows in otherwise clean data), but they need to be flagged so downstream VWAP / spread calculations aren't contaminated.

**What triggers it:**  
Any rows with non-positive price or size. The report records `n_bad_price` and `n_bad_size` counts.

---

### Check C — Timestamp Ordering `[ABORT]`

**What it checks:** `ts_recv` must be monotonically non-decreasing.

**Why it exists:**  
`ts_recv` is the nanosecond timestamp when Databento's gateway received the message. Because Databento applies monotonic normalization, any violation means the file was assembled incorrectly (e.g., two segments concatenated out of order, or a feed replay issue). Non-monotonic timestamps break time-series joins (`pd.merge_asof`) and forward-fill logic — the data is unusable until re-downloaded.

**What triggers it:**  
Any row where `ts_recv` is strictly earlier than the previous row's `ts_recv`. The report records the number of violations.

---

### Check D — Schema Column Coverage `[ABORT]`

**What it checks:** All columns listed in `REQUIRED_COLS[schema]` are present.

**Why it exists:**  
Databento occasionally changes field names between schema versions, or a venue may not populate a required field. Missing columns crash downstream processing immediately — better to catch it here and quarantine rather than discover it mid-analysis.

**Required columns by schema:**

| Schema | Required columns |
|---|---|
| `mbp-10` | `ts_recv`, `ts_event`, `symbol`, `price`, `size`, `bid_px_00`, `ask_px_00`, `bid_sz_00`, `ask_sz_00`, `bid_px_01`, `ask_px_01` |
| `trades` | `ts_recv`, `ts_event`, `symbol`, `price`, `size` |

---

### Check E — Regular Session Filter `[PASS]`

**What it checks:** Keeps only rows between 09:30:00 and 16:00:00 ET.

**Why it exists:**  
Pre-market and after-hours trading has dramatically different microstructure: wider spreads, thinner books, and more noise. The session filter isolates regular trading hours where the NBBO is most meaningful and where institutional volume is concentrated.

**Configuration:**  
```python
SESSION_START = "09:30:00"   # inclusive
SESSION_END   = "16:00:00"   # exclusive
```

> For Blue Ocean ATS (`OCEA.MEMOIR`), which trades overnight (8 PM – 4 AM ET), you may want to set `SESSION_END = "20:00:00"` or disable the filter entirely for ATS chunks.

**Output:** Reports `n_kept` (rows inside session) and `n_dropped` (rows outside). Always PASS — it's informational + transformation, not a quality gate.

---

### Check F — Duplicate Nanosecond Timestamps `[WARNING]`

**What it checks:** Multiple rows sharing the same `(ts_recv, venue)` pair.

**Why it exists:**  
Two events at identical nanoseconds from the same venue is theoretically possible (two messages in the same gateway batch), but a high count suggests feed duplication or replay artifacts. Downstream NBBO construction uses `ts_event` for ordering, but large numbers of duplicate timestamps can cause instability in `groupby` and `merge_asof` operations.

**What triggers it:**  
Any `(ts_recv, venue)` pair that appears more than once. Reports `n_dups`.

---

### Check G — Stale Quote Detection `[WARNING]`

**What it checks:** `ts_recv − ts_event > 60 seconds`

**Why it exists:**  
`ts_event` is the exchange's own timestamp for the event (nanosecond precision, directly from the exchange clock). `ts_recv` is when Databento's co-located gateway received it. The gap is normally a few microseconds (network propagation + processing). A gap of more than 60 seconds means the quote was buffered, delayed, or held somewhere in the chain — it does not reflect the market state at `ts_recv`. Using stale quotes in a spread calculation overstates effective spread.

**Configuration:** `STALE_QUOTE_SECS = 60` (adjustable in `config.py`)

**What triggers it:** Any row where the latency exceeds the threshold. Reports `n_stale` and `max_latency_s`.

---

### Check H — Corporate Event Detection `[WARNING]`

**What it checks:** Day-over-day VWAP change > 30%

**Why it exists:**  
A stock split, reverse split, spin-off, or large special dividend causes an overnight price discontinuity. Models that treat prices as a continuous series across this event will compute nonsensical returns. The check flags the specific dates so downstream code can apply an adjustment factor or exclude the transition.

**How it works:**
1. Computes daily VWAP from session-filtered data: `VWAP = Σ(price × size) / Σ(size)`
2. Computes `|VWAP_t / VWAP_{t-1} - 1|` for each consecutive trading day
3. Flags any day where this ratio exceeds the threshold

**Configuration:** `CORP_EVENT_THRESH = 0.30` (30% — adjustable in `config.py`)

**What triggers it:** Any day with a VWAP shift > 30% vs the prior day. Reports `corp_event_dates` and `n_corp_event_days`.

---

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `LIT_SCHEMA` | `"mbp-10"` | Schema for all 15 lit exchanges |
| `ATS_SCHEMA` | `"trades"` | Schema for ATS/TRF venues |
| `LIT_VENUES` | 15 exchanges | All US NMS lit venues |
| `ATS_VENUES` | 4 venues | FINRA TRFs + Blue Ocean |
| `TICKERS` | 144 symbols | Read from `tickers.csv` |
| `START_DATE` | `2022-04-13` | Download window start (4-year window) |
| `END_DATE` | `2026-04-13` | Download window end (today) |
| `MAX_DOWNLOAD_WORKERS` | `4` | Concurrent Databento API calls |
| `MAX_UPLOAD_WORKERS` | `6` | Concurrent GCS upload threads |
| `MAX_VALIDATE_WORKERS` | `4` | Concurrent validation threads |
| `RETRY_ATTEMPTS` | `3` | API retry attempts |
| `RETRY_MIN_WAIT` | `4s` | Exponential backoff floor |
| `RETRY_MAX_WAIT` | `60s` | Exponential backoff ceiling |
| `PARQUET_COMPRESSION` | `snappy` | Parquet codec |
| `PARQUET_ROW_GROUP` | `50,000` | Rows per row group |
| `PRICE_MEDIAN_MAX` | `10,000` | Check A threshold (dollars) |
| `STALE_QUOTE_SECS` | `60` | Check G threshold (seconds) |
| `CORP_EVENT_THRESH` | `0.30` | Check H threshold (30% VWAP shift) |
| `SESSION_START` | `09:30:00` | Check E — session open |
| `SESSION_END` | `16:00:00` | Check E — session close |

---

## Cache and GCS Layout

**Local cache:**
```
pipeline/cache/
  mbp-10/{symbol}/{venue}/{YYYY-MM}.parquet
  mbp-10/{symbol}/{venue}/{YYYY-MM}_validation.json
  trades/{symbol}/{venue}/{YYYY-MM}.parquet
  trades/{symbol}/{venue}/{YYYY-MM}_validation.json
  quarantine/...          ← ABORT chunks moved here
```

**GCS:**
```
gs://{BUCKET}/raw/mbp-10/{symbol}/{venue}/{YYYY-MM}.parquet
gs://{BUCKET}/raw/mbp-10/{symbol}/{symbol}_manifest.json
gs://{BUCKET}/raw/trades/{symbol}/{venue}/{YYYY-MM}.parquet
gs://{BUCKET}/raw/trades/{symbol}/{symbol}_manifest.json
```

**BigQuery:**
```
{PROJECT}.market_data.mbp_10
{PROJECT}.market_data.trades
  — both partitioned by DATE(ts_recv)
  — both clustered by (symbol, venue)
```

---

## Cost & Size Estimates

| Schema | Venues | Records/chunk | Size/chunk | API cost/GB |
|---|---|---|---|---|
| `mbp-10` | 15 lit | ~7,000 | ~5 MB | ~$2.00 |
| `trades` | 4 ATS/TRF | ~15,000 | ~0.5 MB | ~$0.50 |

Full run estimate (144 tickers × 48 months):

| Schema | Chunks | Est. storage | Est. cost |
|---|---|---|---|
| `mbp-10` | 103,680 | ~507 GB | ~$1,014 |
| `trades` | 27,648 | ~14 GB | ~$7 |

Run `python main.py download --mode all --dry-run` for exact numbers per schema before spending any API credits.

---

## Troubleshooting

**`DATABENTO_API_KEY not set`**  
Create `pipeline/.env` with `DATABENTO_API_KEY=db-...`

**ATS venue not found in account**  
Run `python test_pipeline.py --list-only` to see your available datasets. Remove missing venues from `ATS_VENUES` in `config.py`.

**Check A fires on a valid chunk**  
The symbol has a very high price (e.g., BKNG ~$3,000). Raise `PRICE_MEDIAN_MAX` in `config.py` for that ticker, or filter it out of `tickers.csv`.

**Check C fires (timestamp ordering)**  
Re-download the chunk: `python main.py validate --fix-quarantine`. If it keeps failing, the feed has a known gap — report to Databento support.

**Check H fires on many tickers**  
Normal during earnings seasons or market-wide events. The flagged dates in `corp_event_dates` are the days to inspect — not necessarily errors.

**GCS upload rate limited**  
Lower `MAX_UPLOAD_WORKERS` in `config.py` (default 6 → try 3).

**BigQuery table not found**  
The uploader auto-creates it. Check that `GCP_PROJECT` and `BQ_DATASET` are set in `.env` and that the service account has `bigquery.tables.create` permission.

Sources:
- [Databento US Equities](https://databento.com/catalog/us-equities)
- [Supported venues | Databento](https://databento.com/venues)
- [Venues and datasets | Databento Docs](https://databento.com/docs/venues-and-datasets)
- [Databento Equities Max](https://databento.com/datasets/DBEQ.MAX)
