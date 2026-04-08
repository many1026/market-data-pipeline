"""
downloader.py — Databento API client with parallel chunk downloads.

Chunk granularity: (schema, symbol, venue, YYYY-MM)
Cache layout:      ./cache/{schema}/{symbol}/{venue_safe}/{YYYY-MM}.parquet
Atomic writes:     .tmp → rename on success
Retry:             3 attempts with exponential backoff via tenacity
Logging:           structured JSON to ./logs/download_{date}.jsonl
"""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import databento as db
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)
from tqdm import tqdm

from config import (
    DATABENTO_API_KEY,
    TICKERS,
    VENUES,
    SCHEMAS,
    START_DATE,
    END_DATE,
    CACHE_DIR,
    LOG_DIR,
    MAX_DOWNLOAD_WORKERS,
    PARQUET_COMPRESSION,
    PARQUET_ROW_GROUP,
    RETRY_ATTEMPTS,
    RETRY_MIN_WAIT,
    RETRY_MAX_WAIT,
    ESTIMATE_RECORDS_PER_CHUNK,
    ESTIMATE_MB_PER_CHUNK,
    ESTIMATE_USD_PER_GB,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Data types
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ChunkSpec:
    schema:     str
    symbol:     str
    venue:      str
    year_month: str   # "YYYY-MM"
    start:      str   # ISO-8601 UTC, inclusive
    end:        str   # ISO-8601 UTC, exclusive (first day of next month)


@dataclass
class ChunkResult:
    chunk:    ChunkSpec
    success:  bool
    message:  str
    rows:     int = 0
    elapsed:  float = 0.0
    cached:   bool = False


# ─────────────────────────────────────────────────────────────────────────────
# JSON logging setup
# ─────────────────────────────────────────────────────────────────────────────

class _JsonHandler(logging.FileHandler):
    """Emit one JSON object per log line."""
    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = {
                "ts":      datetime.now(timezone.utc).isoformat(),
                "level":   record.levelname,
                "logger":  record.name,
                "msg":     record.getMessage(),
            }
            if record.exc_info:
                payload["exc"] = self.formatException(record.exc_info)
            self.stream.write(json.dumps(payload) + "\n")
            self.flush()
        except Exception:
            self.handleError(record)


def setup_download_logger() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    date_tag = datetime.now().strftime("%Y%m%d")
    handler = _JsonHandler(LOG_DIR / f"download_{date_tag}.jsonl", mode="a")
    handler.setLevel(logging.DEBUG)
    logging.getLogger().addHandler(handler)


# ─────────────────────────────────────────────────────────────────────────────
# Chunk generation
# ─────────────────────────────────────────────────────────────────────────────

def generate_chunks(
    schema: str,
    tickers: list[str] | None = None,
    venues: list[str] | None = None,
    start_date: str = START_DATE,
    end_date: str = END_DATE,
) -> list[ChunkSpec]:
    """Return all (schema, symbol, venue, month) specs for the given range."""
    tickers = tickers or TICKERS
    venues  = venues  or VENUES
    months  = pd.period_range(start_date, end_date, freq="M")

    chunks = []
    for symbol in tickers:
        for venue in venues:
            for month in months:
                start = month.start_time.strftime("%Y-%m-%dT00:00:00Z")
                end   = (month + 1).start_time.strftime("%Y-%m-%dT00:00:00Z")
                chunks.append(ChunkSpec(
                    schema=schema,
                    symbol=symbol,
                    venue=venue,
                    year_month=str(month),
                    start=start,
                    end=end,
                ))
    return chunks


# ─────────────────────────────────────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────────────────────────────────────

def _venue_safe(venue: str) -> str:
    """Sanitize venue name for filesystem use: XNAS.ITCH → XNAS_ITCH."""
    return venue.replace(".", "_")


def cache_path(chunk: ChunkSpec) -> Path:
    return (
        CACHE_DIR
        / chunk.schema
        / chunk.symbol
        / _venue_safe(chunk.venue)
        / f"{chunk.year_month}.parquet"
    )


def validation_report_path(chunk: ChunkSpec) -> Path:
    p = cache_path(chunk)
    return p.with_name(p.stem + "_validation.json")


def cache_ok(path: Path) -> tuple[bool, str]:
    """
    Return (ok, reason). Reads only the first column to verify readability
    and a non-zero row count.
    """
    if not path.exists():
        return False, "missing"
    try:
        pf = pq.read_table(path, columns=[pq.read_schema(path).names[0]])
        n = len(pf)
        if n == 0:
            return False, "0 rows"
        return True, f"{n:,} rows"
    except Exception as e:
        return False, f"corrupt: {str(e)[:80]}"


# ─────────────────────────────────────────────────────────────────────────────
# Download (with retry)
# ─────────────────────────────────────────────────────────────────────────────

def _make_retry_decorator():
    return retry(
        retry=retry_if_exception_type(Exception),
        wait=wait_exponential(multiplier=2, min=RETRY_MIN_WAIT, max=RETRY_MAX_WAIT),
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


def _fetch_from_api(chunk: ChunkSpec) -> pd.DataFrame:
    """
    Single API call to Databento. Returns a DataFrame.
    Decorated with retry at call site to allow fresh decorator per call.
    """
    @_make_retry_decorator()
    def _call() -> pd.DataFrame:
        os.environ["DATABENTO_API_KEY"] = DATABENTO_API_KEY
        client = db.Historical()
        store = client.timeseries.get_range(
            dataset=chunk.venue,
            schema=chunk.schema,
            symbols=[chunk.symbol],
            stype_in="raw_symbol",
            start=chunk.start,
            end=chunk.end,
        )
        df = store.to_df()
        # Add venue tag so it survives concatenation
        df["venue"] = chunk.venue
        return df

    return _call()


def download_chunk(chunk: ChunkSpec, force: bool = False) -> ChunkResult:
    """
    Download one chunk and save to parquet.

    - Skips if cache is valid and force=False.
    - Uses atomic write: .tmp → rename on success.
    - Returns ChunkResult with outcome details.
    """
    t0  = time.monotonic()
    out = cache_path(chunk)

    # ── Cache check ───────────────────────────────────────────────────────────
    if not force:
        ok, reason = cache_ok(out)
        if ok:
            logger.debug(f"CACHE HIT  {chunk.symbol} {chunk.venue} {chunk.year_month} — {reason}")
            return ChunkResult(chunk=chunk, success=True, message=f"cached ({reason})",
                               rows=int(reason.split()[0].replace(",", "")),
                               elapsed=time.monotonic() - t0, cached=True)

    # ── Download ──────────────────────────────────────────────────────────────
    out.parent.mkdir(parents=True, exist_ok=True)
    try:
        df = _fetch_from_api(chunk)

        if df.empty:
            # Empty response is valid (symbol not traded at venue that month).
            # Save empty parquet so cache_ok() doesn't trigger re-download.
            # Write a single-row header-only parquet.
            tmp = out.with_suffix(".tmp")
            df.to_parquet(tmp, index=False)
            tmp.rename(out)
            logger.info(f"EMPTY      {chunk.symbol} {chunk.venue} {chunk.year_month}")
            return ChunkResult(chunk=chunk, success=True, message="0 rows (symbol absent at venue)",
                               rows=0, elapsed=time.monotonic() - t0)

        # ── Atomic write ──────────────────────────────────────────────────────
        tmp = out.with_suffix(".tmp")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(
            table, tmp,
            compression=PARQUET_COMPRESSION,
            row_group_size=PARQUET_ROW_GROUP,
        )
        tmp.rename(out)

        n = len(df)
        elapsed = time.monotonic() - t0
        logger.info(f"DOWNLOADED {chunk.symbol} {chunk.venue} {chunk.year_month} "
                    f"— {n:,} rows in {elapsed:.1f}s")
        del df, table
        return ChunkResult(chunk=chunk, success=True, message=f"{n:,} rows",
                           rows=n, elapsed=elapsed)

    except Exception as e:
        elapsed = time.monotonic() - t0
        msg = str(e)[:200]
        logger.error(f"FAILED     {chunk.symbol} {chunk.venue} {chunk.year_month} — {msg}")
        # Clean up any partial .tmp file
        tmp = out.with_suffix(".tmp")
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        return ChunkResult(chunk=chunk, success=False, message=msg, elapsed=elapsed)


# ─────────────────────────────────────────────────────────────────────────────
# Parallel download orchestration
# ─────────────────────────────────────────────────────────────────────────────

def download_all(
    schema: str,
    tickers: list[str] | None = None,
    venues: list[str] | None = None,
    start_date: str = START_DATE,
    end_date: str = END_DATE,
    max_workers: int = MAX_DOWNLOAD_WORKERS,
    force: bool = False,
    dry_run: bool = False,
) -> list[ChunkResult]:
    """
    Download all chunks for the given schema in parallel.

    Args:
        dry_run: Print plan and return without downloading.
        force:   Re-download even if cache is valid.

    Returns list of ChunkResult (empty if dry_run).
    """
    chunks = generate_chunks(schema, tickers, venues, start_date, end_date)

    if dry_run:
        _print_dry_run_plan(schema, chunks)
        return []

    setup_download_logger()
    logger.info(f"Starting download: schema={schema}, chunks={len(chunks)}, workers={max_workers}")

    results: list[ChunkResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(download_chunk, c, force): c for c in chunks}
        ok_count = fail_count = cached_count = 0
        with tqdm(total=len(chunks), desc=f"Download [{schema}]", unit="chunk") as bar:
            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)
                if res.cached:
                    cached_count += 1
                elif res.success:
                    ok_count += 1
                else:
                    fail_count += 1
                status = "✓" if res.success else "✗"
                tqdm.write(
                    f"  {status}  {res.chunk.symbol:<6} {res.chunk.venue:<18} "
                    f"{res.chunk.year_month}  {res.message}"
                )
                bar.update(1)

    logger.info(f"Download complete: {ok_count} new, {cached_count} cached, {fail_count} failed")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Dry-run plan printer
# ─────────────────────────────────────────────────────────────────────────────

def _print_dry_run_plan(schema: str, chunks: list[ChunkSpec]) -> None:
    """Print the download plan and estimates WITHOUT any API calls."""
    # Check which chunks are already cached
    cached = [c for c in chunks if cache_ok(cache_path(c))[0]]
    to_dl  = [c for c in chunks if not cache_ok(cache_path(c))[0]]

    n_total    = len(chunks)
    n_cached   = len(cached)
    n_to_dl    = len(to_dl)

    est_records    = ESTIMATE_RECORDS_PER_CHUNK.get(schema, 7_000)
    est_mb         = ESTIMATE_MB_PER_CHUNK.get(schema, 1.0)
    est_usd_per_gb = ESTIMATE_USD_PER_GB.get(schema, 1.0)

    total_records = n_to_dl * est_records
    total_gb      = n_to_dl * est_mb / 1024
    total_cost    = total_gb * est_usd_per_gb

    # Throughput estimate: 50 chunks/hour → minutes for n_to_dl chunks
    est_hours = n_to_dl / 50.0

    # Unique tickers and venues in the plan
    tickers_set = sorted({c.symbol for c in chunks})
    venues_set  = sorted({c.venue  for c in chunks})
    months_set  = sorted({c.year_month for c in chunks})

    w = 66
    print("\n" + "═" * w)
    print(f"  DRY RUN — Download Plan  |  Schema: {schema}")
    print("═" * w)
    print(f"  Tickers      : {len(tickers_set)}")
    print(f"  Venues       : {len(venues_set)}")
    print(f"  Months       : {len(months_set)}  ({months_set[0]} → {months_set[-1]})")
    print(f"  Total chunks : {n_total:,}")
    print(f"  Already cached : {n_cached:,}  ({100*n_cached/n_total:.1f}%)")
    print(f"  To download    : {n_to_dl:,}")
    print("─" * w)
    print(f"  Estimated records  : ~{total_records:,.0f}  (@ ~{est_records:,}/chunk)")
    print(f"  Estimated size     : ~{total_gb:.1f} GB parquet  (@ ~{est_mb} MB/chunk)")
    print(f"  Estimated API cost : ~${total_cost:,.0f}  (@ ~${est_usd_per_gb}/GB, rough)")
    print("─" * w)
    print(f"  Workers            : {MAX_DOWNLOAD_WORKERS}")
    print(f"  Est. download time : ~{est_hours:.1f} hours  (@ 50 chunks/hour)")
    print("─" * w)
    print("  NOTE: Cost & size estimates are heuristic (no API calls made).")
    print("        Run `python main.py download --schema <s>` to start download.")
    print("═" * w + "\n")
