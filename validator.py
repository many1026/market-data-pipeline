"""
validator.py — Data quality checks on OHLCV hourly parquet files.

Data format: ohlcv_1h/{SYMBOL}.parquet
  - Columns: open, high, low, close, volume
  - Index:   tz-aware DatetimeIndex in America/New_York, hourly bars

Checks:
  A  Fixed-point price guard   — median(close) > PRICE_MEDIAN_MAX → ABORT
  B  Non-positive OHLC/volume  — OHLC ≤ 0 or volume < 0 → ABORT (volume = 0 is valid)
  C  Timestamp ordering        — index monotonically non-decreasing → ABORT
  D  Schema column coverage    — all required columns present → ABORT
  E  Session filter            — keep only 09:30–16:00 ET bars (informational)
  F  Duplicate timestamps      — → WARNING
  H  Corporate event detection — daily VWAP shift > CORP_EVENT_THRESH → WARNING
  I  Hourly gap detector       — missing bars within a trading day → WARNING
  J  OHLC sanity               — high ≥ max(O,C), low ≤ min(O,C), high ≥ low → ABORT

Severity:
  ABORT   → quarantine file, do NOT upload
  WARNING → upload with warning flags in metadata
  PASS    → check passed cleanly

Output: {SYMBOL}_validation.json saved next to each parquet.
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
    CACHE_DIR,
    ET_TZ,
    SESSION_START,
    SESSION_END,
    PRICE_MEDIAN_MAX,
    CORP_EVENT_THRESH,
    REQUIRED_COLS,
    QUARANTINE_DIR,
    MAX_VALIDATE_WORKERS,
    OHLCV_DATA_DIR,
)

logger = logging.getLogger(__name__)

ET          = pytz.timezone(ET_TZ)
_SESS_START = dtime.fromisoformat(SESSION_START)
_SESS_END   = dtime.fromisoformat(SESSION_END)

ABORT   = "ABORT"
WARNING = "WARNING"
PASS    = "PASS"

SCHEMA = "ohlcv-1h"


# ─────────────────────────────────────────────────────────────────────────────
# Normalization
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure df has a tz-aware DatetimeIndex in ET.
    If the timestamp is still a regular column, lift it to the index.
    """
    if not isinstance(df.index, pd.DatetimeIndex):
        for cand in ("Datetime", "datetime", "ts_event", "timestamp", "index"):
            if cand in df.columns:
                df = df.set_index(cand)
                break
    if isinstance(df.index, pd.DatetimeIndex):
        if df.index.tz is None:
            df.index = df.index.tz_localize("UTC").tz_convert(ET)
        else:
            df.index = df.index.tz_convert(ET)
    return df


def _idx_time_et(df: pd.DataFrame) -> np.ndarray:
    """Return an array of wall-clock times (ET) for the DatetimeIndex."""
    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    return idx.tz_convert(ET).time


# ─────────────────────────────────────────────────────────────────────────────
# Individual checks
# ─────────────────────────────────────────────────────────────────────────────

def _check_A(df: pd.DataFrame) -> dict:
    """Fixed-point guard: median(close) > PRICE_MEDIAN_MAX → ABORT."""
    if "close" not in df.columns or df.empty:
        return {"status": PASS, "detail": "no close column or empty df"}
    med = float(df["close"].median())
    if med > PRICE_MEDIAN_MAX:
        return {
            "status": ABORT,
            "detail": f"median close = {med:,.0f} > {PRICE_MEDIAN_MAX} (fixed-point not converted)",
        }
    return {"status": PASS, "detail": f"median close = {med:.4f}"}


def _check_B(df: pd.DataFrame) -> dict:
    """
    Non-positive OHLC or negative volume → ABORT.
    Volume = 0 is legitimate for illiquid small-cap hours — reported but not flagged.
    """
    if df.empty:
        return {"status": PASS, "detail": "empty df"}
    ohlc_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    n_bad_ohlc = int(sum(int((df[c] <= 0).sum()) for c in ohlc_cols))
    vol = df.get("volume", pd.Series(dtype=float))
    n_neg_vol  = int((vol < 0).sum())
    n_zero_vol = int((vol == 0).sum())
    if n_bad_ohlc or n_neg_vol:
        return {
            "status":       ABORT,
            "detail":       f"{n_bad_ohlc} rows with OHLC ≤ 0; {n_neg_vol} rows with volume < 0",
            "n_bad_ohlc":   n_bad_ohlc,
            "n_neg_volume": n_neg_vol,
        }
    return {
        "status":        PASS,
        "detail":        f"all OHLC positive; {n_zero_vol} bars with volume=0 (valid)",
        "n_zero_volume": n_zero_vol,
    }


def _check_C(df: pd.DataFrame) -> dict:
    """Index (timestamps) must be monotonically non-decreasing — ABORT if not."""
    if not isinstance(df.index, pd.DatetimeIndex) or df.empty:
        return {"status": PASS, "detail": "no datetime index or empty"}
    if not df.index.is_monotonic_increasing:
        diffs = df.index.to_series().diff()
        n = int((diffs < pd.Timedelta(0)).sum())
        return {
            "status":       ABORT,
            "detail":       f"{n} index ordering violations",
            "n_violations": n,
        }
    return {"status": PASS, "detail": "index monotonically non-decreasing"}


def _check_D(df: pd.DataFrame) -> dict:
    """All required columns present — ABORT if any missing."""
    expected = REQUIRED_COLS.get(SCHEMA, [])
    missing  = [c for c in expected if c not in df.columns]
    if missing:
        return {"status": ABORT, "detail": f"missing columns: {missing}", "missing": missing}
    return {"status": PASS, "detail": f"all {len(expected)} expected columns present"}


def _check_E(df: pd.DataFrame) -> tuple[dict, pd.DataFrame]:
    """
    Session filter: keep only [09:30, 16:00) ET rows.
    Returns (check_result, filtered_df). Non-modifying if df is empty / non-indexed.
    """
    if not isinstance(df.index, pd.DatetimeIndex) or df.empty:
        return {"status": PASS, "detail": "no datetime index or empty"}, df
    t_et = _idx_time_et(df)
    mask = (t_et >= _SESS_START) & (t_et < _SESS_END)
    filtered  = df[mask].copy()
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
    """Duplicate timestamps — must be exactly 1 bar per hour per ticker. WARNING."""
    if not isinstance(df.index, pd.DatetimeIndex) or df.empty:
        return {"status": PASS, "detail": "no datetime index or empty"}
    n_dups = int(df.index.duplicated().sum())
    if n_dups:
        return {
            "status": WARNING,
            "detail": f"{n_dups} duplicate timestamps",
            "n_dups": n_dups,
        }
    return {"status": PASS, "detail": "no duplicate timestamps"}


def _check_H(df: pd.DataFrame) -> dict:
    """
    Corporate event detection: daily VWAP shift > CORP_EVENT_THRESH vs prior day.
    VWAP_day = Σ(close·volume) / Σ(volume) over the day's intraday bars.
    """
    if df.empty or not {"close", "volume"}.issubset(df.columns):
        return {"status": PASS, "detail": "required columns absent for check H"}
    if not isinstance(df.index, pd.DatetimeIndex):
        return {"status": PASS, "detail": "no datetime index"}

    idx  = df.index.tz_convert(ET) if df.index.tz is not None else df.index
    date = pd.Index(idx.date, name="date")

    daily = pd.DataFrame({
        "dv": (df["close"].values * df["volume"].values),
        "v":  df["volume"].values,
    }, index=date).groupby(level=0).sum().sort_index()

    daily["vwap"] = np.where(daily["v"] > 0, daily["dv"] / daily["v"].clip(lower=1), np.nan)
    vwap_series   = pd.Series(daily["vwap"].values, index=daily.index).dropna()
    ret = vwap_series.pct_change().abs()
    big = ret[ret > CORP_EVENT_THRESH]
    if not big.empty:
        return {
            "status":            WARNING,
            "detail":            f"{len(big)} days with VWAP shift > {CORP_EVENT_THRESH*100:.0f}%",
            "corp_event_dates":  [str(d) for d in big.index.tolist()],
            "n_corp_event_days": len(big),
        }
    return {"status": PASS, "detail": f"no VWAP shift > {CORP_EVENT_THRESH*100:.0f}% detected"}


def _check_I(df: pd.DataFrame) -> dict:
    """
    Hourly gap detector: within each trading day, bars should be contiguous at 1h step
    from the first observed bar to the last. Flags missing slots — critical for CPCV
    purging since a missing hour breaks the t1 alignment of downstream labels.
    """
    if not isinstance(df.index, pd.DatetimeIndex) or df.empty:
        return {"status": PASS, "detail": "no datetime index or empty"}

    idx = df.index.tz_convert(ET) if df.index.tz is not None else df.index
    s   = pd.Series(idx, index=idx)

    n_missing     = 0
    n_days_gappy  = 0
    sample_gaps: list[str] = []
    for day, ts in s.groupby(idx.date):
        ts_sorted = ts.sort_values()
        if len(ts_sorted) < 2:
            continue
        expected = pd.date_range(
            ts_sorted.iloc[0], ts_sorted.iloc[-1], freq="1h", tz=ts_sorted.iloc[0].tz,
        )
        missing = expected.difference(pd.DatetimeIndex(ts_sorted))
        if len(missing):
            n_missing    += len(missing)
            n_days_gappy += 1
            if len(sample_gaps) < 5:
                sample_gaps.append(f"{day}: {[str(m) for m in missing[:3]]}")

    if n_missing:
        return {
            "status":         WARNING,
            "detail":         f"{n_missing} missing hourly bars across {n_days_gappy} days",
            "n_missing_bars": n_missing,
            "n_days_gappy":   n_days_gappy,
            "sample_gaps":    sample_gaps,
        }
    return {"status": PASS, "detail": "no intra-day hourly gaps"}


def _check_J(df: pd.DataFrame) -> dict:
    """
    OHLC sanity: high ≥ max(open, close), low ≤ min(open, close), high ≥ low.
    Any violation is an impossible bar → ABORT.
    """
    cols = ("open", "high", "low", "close")
    if df.empty or not all(c in df.columns for c in cols):
        return {"status": PASS, "detail": "OHLC columns absent or empty"}
    hi, lo, op, cl = df["high"], df["low"], df["open"], df["close"]
    n_bad_hi = int(((hi < op) | (hi < cl)).sum())
    n_bad_lo = int(((lo > op) | (lo > cl)).sum())
    n_bad_hl = int((hi < lo).sum())
    total = n_bad_hi + n_bad_lo + n_bad_hl
    if total:
        return {
            "status": ABORT,
            "detail": (f"{n_bad_hi} high<open|close; "
                       f"{n_bad_lo} low>open|close; "
                       f"{n_bad_hl} high<low"),
            "n_bad_high":     n_bad_hi,
            "n_bad_low":      n_bad_lo,
            "n_bad_high_low": n_bad_hl,
        }
    return {"status": PASS, "detail": "OHLC relations consistent"}


# ─────────────────────────────────────────────────────────────────────────────
# Main entry
# ─────────────────────────────────────────────────────────────────────────────

def validate_ohlcv(df: pd.DataFrame, symbol: str) -> dict:
    """
    Run all checks on one ticker's OHLCV frame.

    Returns a report dict with keys:
      symbol, quarantine, warnings, n_rows_in, n_rows_out, checks, filtered_df.
    """
    df   = _normalize(df)
    n_in = len(df)

    checks: dict[str, dict] = {}
    checks["A"] = _check_A(df)
    checks["B"] = _check_B(df)
    checks["C"] = _check_C(df)
    checks["D"] = _check_D(df)
    checks["J"] = _check_J(df)

    checks["E"], df_filtered = _check_E(df)
    checks["F"] = _check_F(df_filtered)
    checks["H"] = _check_H(df_filtered)
    checks["I"] = _check_I(df_filtered)

    quarantine = any(v["status"] == ABORT for v in checks.values())
    warnings   = [k for k, v in checks.items() if v["status"] == WARNING]

    return {
        "symbol":      symbol,
        "schema":      SCHEMA,
        "quarantine":  quarantine,
        "warnings":    warnings,
        "n_rows_in":   n_in,
        "n_rows_out":  len(df_filtered),
        "checks":      checks,
        "filtered_df": df_filtered,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Batch validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_all_ohlcv(
    data_dir: Path | None = None,
    symbol_filter: str | None = None,
) -> list[dict]:
    """
    Validate every parquet in `data_dir` (default: OHLCV_DATA_DIR) in parallel.

    Writes {SYMBOL}_validation.json next to each parquet. Quarantines any file
    that failed an ABORT check.
    """
    from tqdm import tqdm

    data_dir = Path(data_dir or OHLCV_DATA_DIR)
    pattern  = f"{symbol_filter}.parquet" if symbol_filter else "*.parquet"
    files    = [
        p for p in data_dir.glob(pattern)
        if "quarantine" not in p.parts and not p.stem.endswith("_validation")
    ]

    if not files:
        logger.warning(f"No parquets found under {data_dir} (pattern={pattern})")
        return []

    def _validate_file(path: Path) -> dict | None:
        symbol = path.stem
        try:
            df  = pd.read_parquet(path)
            rpt = validate_ohlcv(df, symbol)

            payload  = {k: v for k, v in rpt.items() if k != "filtered_df"}
            rpt_path = path.with_name(f"{symbol}_validation.json")
            with open(rpt_path, "w") as f:
                json.dump(payload, f, indent=2, default=str)

            if rpt["quarantine"]:
                QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
                dest = QUARANTINE_DIR / path.name
                path.rename(dest)
                aborted = [k for k, v in rpt["checks"].items() if v["status"] == ABORT]
                logger.warning(f"QUARANTINED {symbol} — aborts: {aborted}")
            elif rpt["warnings"]:
                logger.warning(f"WARNINGS    {symbol} — {rpt['warnings']}")
            else:
                logger.info(f"PASSED      {symbol}")
            return payload
        except Exception as e:
            logger.error(f"Validation error for {path}: {e}")
            return None

    reports: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_VALIDATE_WORKERS) as pool:
        futures = {pool.submit(_validate_file, p): p for p in files}
        with tqdm(total=len(files), desc="Validate OHLCV", unit="file") as bar:
            for fut in as_completed(futures):
                r = fut.result()
                if r is not None:
                    reports.append(r)
                bar.update(1)

    total = len(reports)
    q = sum(1 for r in reports if r.get("quarantine"))
    w = sum(1 for r in reports if r.get("warnings"))
    p = total - q - w
    print(f"\nValidation complete: {p} passed, {w} warned, {q} quarantined / {total} total")
    return reports


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s %(message)s")
    validate_all_ohlcv()

# Alias for main.py compatibility
validate_all = validate_all_ohlcv


def validate_all(schema: str, symbol_filter: str | None = None) -> list[dict]:
    """
    Wrapper para main.py — estructura cache/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet
    Consolida todos los chunks de un símbolo antes de validar.
    """
    import pandas as pd

    schema_dir = CACHE_DIR / schema.replace("-", "-")
    if not schema_dir.exists():
        logger.warning(f"No existe directorio: {schema_dir}")
        return []

    # Agrupar parquets por símbolo
    symbol_files: dict[str, list[Path]] = {}
    for p in schema_dir.rglob("*.parquet"):
        if "quarantine" in p.parts:
            continue
        symbol = p.parts[len(schema_dir.parts)]  # primer nivel = símbolo
        if symbol_filter and symbol != symbol_filter:
            continue
        symbol_files.setdefault(symbol, []).append(p)

    if not symbol_files:
        logger.warning(f"No parquets encontrados en {schema_dir}")
        return []

    reports = []
    for symbol, paths in symbol_files.items():
        try:
            df = pd.concat([pd.read_parquet(p) for p in paths]).sort_index()
            rpt = validate_ohlcv(df, symbol)
            payload = {k: v for k, v in rpt.items() if k != "filtered_df"}
            reports.append(payload)
            if rpt["quarantine"]:
                logger.warning(f"QUARANTINED {symbol}")
            elif rpt["warnings"]:
                logger.warning(f"WARNINGS    {symbol} — {rpt['warnings']}")
            else:
                logger.info(f"PASSED      {symbol}")
        except Exception as e:
            logger.error(f"Error validando {symbol}: {e}")

    total = len(reports)
    q = sum(1 for r in reports if r.get("quarantine"))
    w = sum(1 for r in reports if r.get("warnings"))
    print(f"\nValidation complete: {total-q-w} passed, {w} warned, {q} quarantined / {total} total")
    return reports
