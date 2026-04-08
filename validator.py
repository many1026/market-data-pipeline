"""
validator.py — Data quality checks on every downloaded chunk.

Checks:
  A  Fixed-point price guard     — median(price) > 10,000 → ABORT (quarantine)
  B  Negative price/size rows    — count & log (warning, still upload)
  C  Timestamp ordering          — ts_recv monotonically non-decreasing (ABORT if not)
  D  Schema column coverage      — all expected columns present (ABORT if missing)
  E  Session filter              — keep only 09:30–16:00 ET rows
  F  Duplicate nanoseconds       — same ts_recv per venue flagged (warning)
  G  Stale quote detection       — ts_recv - ts_event > 60s flagged (warning)
  H  Corporate event detection   — VWAP shift > 30% vs prior day flagged (warning)

Severity levels:

  ABORT   → quarantine chunk, do NOT upload (checks A, C, D)
  WARNING → upload WITH warning flags in metadata (checks B, F, G, H)
  PASS    → check passed cleanly

Output: validation_report.json saved alongside each parquet.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import time as dtime
from pathlib import Path

import numpy as np
import pandas as pd
import pytz

from config import (
    ET_TZ,
    SESSION_START,
    SESSION_END,
    PRICE_MEDIAN_MAX,
    STALE_QUOTE_SECS,
    CORP_EVENT_THRESH,
    REQUIRED_COLS,
    QUARANTINE_DIR,
    MAX_VALIDATE_WORKERS,
)
from downloader import ChunkSpec, cache_path, validation_report_path

logger = logging.getLogger(__name__)

ET          = pytz.timezone(ET_TZ)
_SESS_START = dtime.fromisoformat(SESSION_START)
_SESS_END   = dtime.fromisoformat(SESSION_END)

ABORT   = "ABORT"
WARNING = "WARNING"
PASS    = "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_A(df: pd.DataFrame) -> dict:
    """Fixed-point price guard: median > PRICE_MEDIAN_MAX → ABORT."""
    if "price" not in df.columns or df.empty:
        return {"status": PASS, "detail": "no price column or empty df"}
    med = float(df["price"].median())
    if med > PRICE_MEDIAN_MAX:
        return {
            "status": ABORT,
            "detail": f"median price = {med:,.0f} > {PRICE_MEDIAN_MAX} (fixed-point not converted)",
        }
    return {"status": PASS, "detail": f"median price = {med:.4f}"}


def _check_B(df: pd.DataFrame) -> dict:
    """Negative/zero price & size: count and log (WARNING only)."""
    n_bad_p = int((df.get("price", pd.Series(dtype=float)) <= 0).sum())
    n_bad_s = int((df.get("size",  pd.Series(dtype=float)) <= 0).sum())
    if n_bad_p or n_bad_s:
        return {
            "status":  WARNING,
            "detail":  f"{n_bad_p} rows with price ≤ 0; {n_bad_s} rows with size ≤ 0",
            "n_bad_price": n_bad_p,
            "n_bad_size":  n_bad_s,
        }
    return {"status": PASS, "detail": "all prices and sizes positive"}


def _check_C(df: pd.DataFrame) -> dict:
    """ts_recv must be monotonically non-decreasing — ABORT if not."""
    if "ts_recv" not in df.columns or df.empty:
        return {"status": PASS, "detail": "ts_recv not present or empty"}
    ts = df["ts_recv"]
    if not ts.is_monotonic_increasing:
        n_violations = int((ts.diff() < pd.Timedelta(0)).sum())
        return {
            "status":       ABORT,
            "detail":       f"{n_violations} ts_recv ordering violations",
            "n_violations": n_violations,
        }
    return {"status": PASS, "detail": "ts_recv monotonically non-decreasing"}


def _check_D(df: pd.DataFrame, schema: str) -> dict:
    """All expected columns present — ABORT if any missing."""
    expected = REQUIRED_COLS.get(schema, [])
    missing  = [c for c in expected if c not in df.columns]
    if missing:
        return {
            "status":  ABORT,
            "detail":  f"missing columns: {missing}",
            "missing": missing,
        }
    return {"status": PASS, "detail": f"all {len(expected)} expected columns present"}


def _check_E(df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    """
    Session filter: keep only 09:30–16:00 ET rows.
    Returns (check_result, filtered_df).
    """
    if "ts_event" not in df.columns or df.empty:
        return {"status": PASS, "detail": "no ts_event column"}, df

    ts = df["ts_event"]
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")
    t_et = ts.dt.tz_convert(ET).dt.time
    mask = (t_et >= _SESS_START) & (t_et < _SESS_END)
    filtered = df[mask].copy()
    n_dropped = len(df) - len(filtered)
    return (
        {
            "status":    PASS,
            "detail":    f"kept {len(filtered):,} rows; dropped {n_dropped:,} outside session",
            "n_kept":    len(filtered),
            "n_dropped": n_dropped,
        },
        filtered,
    )


def _check_F(df: pd.DataFrame) -> dict:
    """Duplicate nanoseconds: same ts_recv within the same venue — WARNING."""
    if "ts_recv" not in df.columns or "venue" not in df.columns or df.empty:
        return {"status": PASS, "detail": "ts_recv or venue column absent"}
    dups = df.duplicated(subset=["ts_recv", "venue"], keep=False).sum()
    if dups:
        return {
            "status":  WARNING,
            "detail":  f"{int(dups)} rows share (ts_recv, venue) with another row",
            "n_dups":  int(dups),
        }
    return {"status": PASS, "detail": "no duplicate (ts_recv, venue) pairs"}


def _check_G(df: pd.DataFrame) -> dict:
    """Stale quotes: ts_recv - ts_event > STALE_QUOTE_SECS — WARNING."""
    if "ts_recv" not in df.columns or "ts_event" not in df.columns or df.empty:
        return {"status": PASS, "detail": "ts_recv or ts_event absent"}

    recv  = df["ts_recv"]
    event = df["ts_event"]

    # Ensure both are tz-aware UTC
    if recv.dt.tz is None:
        recv = recv.dt.tz_localize("UTC")
    if event.dt.tz is None:
        event = event.dt.tz_localize("UTC")

    latency_s = (recv - event).dt.total_seconds()
    n_stale   = int((latency_s > STALE_QUOTE_SECS).sum())
    if n_stale:
        return {
            "status":   WARNING,
            "detail":   f"{n_stale} quotes with latency > {STALE_QUOTE_SECS}s",
            "n_stale":  n_stale,
            "max_latency_s": float(latency_s.max()),
        }
    return {"status": PASS, "detail": f"max latency = {latency_s.max():.2f}s"}


def _check_H(df: pd.DataFrame) -> dict:
    """Corporate event detection: daily VWAP shift > 30% vs prior day — WARNING."""
    if "price" not in df.columns or "size" not in df.columns or "ts_event" not in df.columns:
        return {"status": PASS, "detail": "required columns absent for check H"}
    if df.empty:
        return {"status": PASS, "detail": "empty chunk"}

    ts = df["ts_event"]
    if ts.dt.tz is None:
        ts = ts.dt.tz_localize("UTC")

    temp = df[["price", "size"]].copy()
    temp["date"] = ts.dt.tz_convert(ET).dt.date
    temp["dv"]   = temp["price"] * temp["size"]

    daily = (
        temp.groupby("date")
        .agg(dv=("dv", "sum"), shares=("size", "sum"))
        .sort_index()
    )
    daily["vwap"]     = daily["dv"] / daily["shares"].clip(lower=1)
    daily["vwap_ret"] = daily["vwap"].pct_change().abs()
    big = daily[daily["vwap_ret"] > CORP_EVENT_THRESH]

    if not big.empty:
        dates = [str(d) for d in big.index.tolist()]
        return {
            "status":            WARNING,
            "detail":            f"{len(big)} days with VWAP shift > {CORP_EVENT_THRESH*100:.0f}%",
            "corp_event_dates":  dates,
            "n_corp_event_days": len(big),
        }
    return {"status": PASS, "detail": "no VWAP shift > 30% detected"}


# ─────────────────────────────────────────────────────────────────────────────
# Main validation entry point
# ─────────────────────────────────────────────────────────────────────────────

def validate_chunk(
    df: pd.DataFrame,
    chunk: ChunkSpec,
) -> dict:
    """
    Run all checks on `df` for the given chunk.

    Returns a report dict with keys:
      checks     — {A: ..., B: ..., ...}
      quarantine — True if any ABORT check fired
      warnings   — list of check names that returned WARNING
      n_rows_in  — row count before session filter
      n_rows_out — row count after session filter (df passed back)
      filtered_df — the filtered DataFrame (session hours only)

    Saves validation_report.json alongside the parquet.
    """
    n_in = len(df)
    checks: dict[str, dict] = {}

    # ── Checks that don't modify df ───────────────────────────────────────────
    checks["A"] = _check_A(df)
    checks["B"] = _check_B(df)
    checks["C"] = _check_C(df)
    checks["D"] = _check_D(df, chunk.schema)

    # ── Session filter (modifies df) ──────────────────────────────────────────
    checks["E"], df_filtered = _check_E(df)

    # ── Checks on session-filtered df ─────────────────────────────────────────
    checks["F"] = _check_F(df_filtered)
    checks["G"] = _check_G(df_filtered)
    checks["H"] = _check_H(df_filtered)

    # ── Determine quarantine / warning status ─────────────────────────────────
    quarantine = any(v["status"] == ABORT for v in checks.values())
    warnings   = [k for k, v in checks.items() if v["status"] == WARNING]

    report = {
        "chunk":       {
            "schema":     chunk.schema,
            "symbol":     chunk.symbol,
            "venue":      chunk.venue,
            "year_month": chunk.year_month,
        },
        "quarantine":  quarantine,
        "warnings":    warnings,
        "n_rows_in":   n_in,
        "n_rows_out":  len(df_filtered),
        "checks":      checks,
    }

    # ── Save report JSON ──────────────────────────────────────────────────────
    rpt_path = validation_report_path(chunk)
    rpt_path.parent.mkdir(parents=True, exist_ok=True)
    with open(rpt_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # ── Quarantine: move parquet to quarantine dir ────────────────────────────
    if quarantine:
        src = cache_path(chunk)
        if src.exists():
            dest = QUARANTINE_DIR / src.relative_to(src.parents[4])  # keep sub-path
            dest.parent.mkdir(parents=True, exist_ok=True)
            src.rename(dest)
            logger.warning(
                f"QUARANTINED {chunk.symbol} {chunk.venue} {chunk.year_month} "
                f"— aborted checks: {[k for k,v in checks.items() if v['status']==ABORT]}"
            )
    elif warnings:
        logger.warning(
            f"WARNINGS    {chunk.symbol} {chunk.venue} {chunk.year_month} "
            f"— checks with warnings: {warnings}"
        )
    else:
        logger.info(f"PASSED      {chunk.symbol} {chunk.venue} {chunk.year_month}")

    report["filtered_df"] = df_filtered
    return report


# ─────────────────────────────────────────────────────────────────────────────
# Batch validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_all(
    schema: str,
    symbol_filter: str | None = None,
) -> list[dict]:
    """
    Validate all cached parquet files for the given schema in parallel.

    Args:
        symbol_filter: if set, only validate this symbol.

    Returns list of report dicts (without filtered_df for memory efficiency).
    """
    from config import CACHE_DIR  # local import to avoid circular issues
    from tqdm import tqdm

    pattern = (
        f"{schema}/{symbol_filter}/**/*.parquet"
        if symbol_filter
        else f"{schema}/**/*.parquet"
    )
    parquet_files = list(CACHE_DIR.glob(pattern))
    # Exclude quarantined files
    parquet_files = [p for p in parquet_files if "quarantine" not in str(p)]

    if not parquet_files:
        logger.warning(f"No cached parquets found for schema={schema}, symbol={symbol_filter}")
        return []

    def _validate_file(ppath: Path) -> dict | None:
        """Validate one parquet file. Returns clean report dict or None on error."""
        # Reconstruct chunk spec from path
        # Expected: cache/{schema}/{symbol}/{venue_safe}/{YYYY-MM}.parquet
        parts = ppath.relative_to(CACHE_DIR).parts
        if len(parts) < 4:
            logger.warning(f"Unexpected path structure: {ppath}")
            return None
        _, sym, venue_safe, fname = parts[0], parts[1], parts[2], parts[3]
        venue      = venue_safe.replace("_", ".", 1)   # XNAS_ITCH → XNAS.ITCH
        year_month = fname.replace(".parquet", "")

        chunk = ChunkSpec(
            schema=schema, symbol=sym, venue=venue,
            year_month=year_month, start="", end="",
        )
        try:
            df = pd.read_parquet(ppath)
            rpt = validate_chunk(df, chunk)
            return {k: v for k, v in rpt.items() if k != "filtered_df"}
        except Exception as e:
            logger.error(f"Validation error for {ppath}: {e}")
            return None

    reports: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_VALIDATE_WORKERS) as pool:
        futures = {pool.submit(_validate_file, p): p for p in parquet_files}
        with tqdm(total=len(parquet_files), desc=f"Validate [{schema}]", unit="chunk") as bar:
            for fut in as_completed(futures):
                rpt = fut.result()
                if rpt is not None:
                    reports.append(rpt)
                bar.update(1)

    # Summary
    total      = len(reports)
    quarantine = sum(1 for r in reports if r.get("quarantine"))
    warned     = sum(1 for r in reports if r.get("warnings"))
    passed     = total - quarantine - warned
    print(f"\nValidation complete: {passed} passed, {warned} warned, {quarantine} quarantined / {total} total")
    return reports
