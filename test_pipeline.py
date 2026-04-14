"""
test_pipeline.py — LIT mbp-10 full download (4-year window, all 15 venues).

What it does
────────────
  Step 0 — Lists all datasets available in your Databento account
  Step 1 — Downloads tickers × 15 lit venues × 4 years (mbp-10)
            Date range: 2022-04-13 → 2026-04-13
  Step 2 — Validates all downloaded chunks (all 8 checks)
  Step 3 — (Optional) Uploads to gs://{BUCKET}/raw/ and verifies MD5
  Step 4 — Prints full-run cost & time estimate

Usage:
  cd pipeline/
  python test_pipeline.py                              # all tickers from tickers.csv
  python test_pipeline.py --tickers AAPL,MSFT,NVDA    # specific tickers
  python test_pipeline.py --tickers AAPL --list-only  # only list available datasets
  python test_pipeline.py --tickers AAPL --skip-upload
  python test_pipeline.py --tickers AAPL --dry-run    # estimate without downloading
  python test_pipeline.py --tickers AAPL --force      # re-download even if cached

Prerequisites:
  DATABENTO_API_KEY set in pipeline/.env
  GCS_BUCKET set in pipeline/.env (optional — only for step 3)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from config import (
    DATABENTO_API_KEY,
    GCS_BUCKET,
    LIT_SCHEMA,
    LIT_VENUES,
    TICKERS,
    ESTIMATE_MB_PER_CHUNK,
    ESTIMATE_USD_PER_GB,
    ESTIMATE_RECORDS_PER_CHUNK,
)
from downloader import ChunkSpec, download_chunk, cache_path, generate_chunks
from validator import validate_chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Fixed parameters ──────────────────────────────────────────────────────────
# 4-year window: today is 2026-04-13
DOWNLOAD_START  = "2022-04-13"
DOWNLOAD_END    = "2026-04-13"
DOWNLOAD_SCHEMA = LIT_SCHEMA          # mbp-10
DOWNLOAD_VENUES = LIT_VENUES          # all 15 NMS lit exchanges
GCS_PREFIX      = "raw"


# ─────────────────────────────────────────────────────────────────────────────
# Step 0 — Dataset discovery
# ─────────────────────────────────────────────────────────────────────────────

def step0_list_datasets() -> None:
    """List all datasets available in your Databento account."""
    print("\n" + "═" * 66)
    print("  STEP 0 — Available Databento Datasets")
    print("═" * 66)

    if not DATABENTO_API_KEY:
        print("  ✗  DATABENTO_API_KEY not set — cannot list datasets\n")
        return

    try:
        import databento as db
        client   = db.Historical()
        datasets = client.metadata.list_datasets()
        print(f"  Found {len(datasets)} datasets in your account:\n")
        for d in sorted(datasets):
            tag = "  ← lit exchange (mbp-10)" if d in LIT_VENUES else ""
            print(f"    {d}{tag}")

        missing = [v for v in LIT_VENUES if v not in datasets]
        if missing:
            print(f"\n  ⚠  LIT venues in config NOT found in your account:")
            for v in missing:
                print(f"    - {v}  → check subscription")
        else:
            print(f"\n  ✓  All {len(LIT_VENUES)} configured LIT venues are available")

    except Exception as e:
        print(f"  ✗  Could not list datasets: {e}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Download lit (mbp-10) — tickers × 15 venues × 4 years
# ─────────────────────────────────────────────────────────────────────────────

def step1_download_lit(tickers: list[str], dry_run: bool = False, force: bool = False) -> list[ChunkSpec]:
    """Download mbp-10 data from all 15 LIT venues for the given tickers."""
    print("═" * 66)
    print(f"  STEP 1 — Download LIT  [{DOWNLOAD_SCHEMA}]")
    print(f"  Tickers    : {tickers}")
    print(f"  Venues     : {len(DOWNLOAD_VENUES)} (all lit US)")
    print(f"  Date range : {DOWNLOAD_START} → {DOWNLOAD_END}")
    print("═" * 66)

    if not DATABENTO_API_KEY:
        print("  ✗  DATABENTO_API_KEY not set — skipping\n")
        return []

    chunks = generate_chunks(
        schema=DOWNLOAD_SCHEMA,
        tickers=tickers,
        venues=DOWNLOAD_VENUES,
        start_date=DOWNLOAD_START,
        end_date=DOWNLOAD_END,
    )

    print(f"  Total chunks: {len(chunks):,}  "
          f"({len(tickers)} tickers × {len(DOWNLOAD_VENUES)} venues × "
          f"{len(pd.period_range(DOWNLOAD_START, DOWNLOAD_END, freq='M'))} months)\n")

    if dry_run:
        print("  [dry-run] No downloads performed.\n")
        return []

    downloaded: list[ChunkSpec] = []
    for chunk in chunks:
        print(f"  → {chunk.symbol:<6} {chunk.venue:<20} {chunk.year_month}")
        result = download_chunk(chunk, force=force)
        icon = "✓" if result.success else "✗"
        print(f"    {icon}  {result.message}  ({result.elapsed:.1f}s)")
        if result.success:
            downloaded.append(chunk)

    print(f"\n  {len(downloaded)}/{len(chunks)} chunks downloaded\n")
    return downloaded


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Validation
# ─────────────────────────────────────────────────────────────────────────────

def step2_validate(chunks: list[ChunkSpec]) -> list[dict]:
    """Run all 8 data quality checks on every downloaded chunk."""
    print("═" * 66)
    print("  STEP 2 — Validation (8 checks per chunk)")
    print("═" * 66)

    if not chunks:
        print("  No chunks to validate\n")
        return []

    reports: list[dict] = []
    for chunk in chunks:
        ppath = cache_path(chunk)
        if not ppath.exists():
            print(f"  ✗  File missing: {ppath}")
            continue

        df  = pd.read_parquet(ppath)
        rpt = validate_chunk(df, chunk)

        q     = rpt.get("quarantine", False)
        warns = rpt.get("warnings", [])
        n_in  = rpt.get("n_rows_in", 0)
        n_out = rpt.get("n_rows_out", 0)

        if q:
            status = "QUARANTINE"
        elif warns:
            status = f"WARN({','.join(warns)})"
        else:
            status = "PASS"

        print(f"\n  [{chunk.schema}] {chunk.symbol} @ {chunk.venue}  {chunk.year_month}")
        print(f"  Status: {status}  |  rows: {n_in:,} in → {n_out:,} after session filter")
        print(f"  {'Check':<6} {'Result':<10} Detail")
        print(f"  {'─'*6} {'─'*10} {'─'*40}")
        for check_id, check in rpt.get("checks", {}).items():
            s    = check.get("status", "?")
            d    = check.get("detail", "")[:60]
            icon = "✓" if s == "PASS" else ("✗" if s == "ABORT" else "⚠")
            print(f"  {icon} [{check_id}]   {s:<10} {d}")

        reports.append({k: v for k, v in rpt.items() if k != "filtered_df"})

    quarantined = sum(1 for r in reports if r.get("quarantine"))
    print(f"\n  Validation summary: "
          f"{len(reports)-quarantined} passed, {quarantined} quarantined / {len(reports)} total\n")
    return reports


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — GCS upload + MD5 roundtrip (optional)
# ─────────────────────────────────────────────────────────────────────────────

def step3_upload_and_verify(
    chunks: list[ChunkSpec],
    reports: list[dict],
    skip: bool = False,
) -> bool:
    """Upload to gs://{BUCKET}/raw/ and verify MD5 checksums."""
    print("═" * 66)
    print("  STEP 3 — GCS Upload + MD5 Roundtrip")
    print("═" * 66)

    if skip:
        print("  Skipped (--skip-upload)\n")
        return True

    if not GCS_BUCKET:
        print("  GCS_BUCKET not set — skipping (set in pipeline/.env to enable)\n")
        return True

    if not chunks:
        print("  No chunks to upload\n")
        return True

    from uploader import upload_chunk
    from google.cloud import storage as gcs_lib

    gcs_client = gcs_lib.Client()
    quarantined_keys = {
        f"{r['chunk']['symbol']}|{r['chunk']['venue']}|{r['chunk']['year_month']}|{r['chunk']['schema']}"
        for r in reports if r.get("quarantine")
    }

    uploaded: list[tuple[ChunkSpec, str]] = []
    for chunk in chunks:
        key = f"{chunk.symbol}|{chunk.venue}|{chunk.year_month}|{chunk.schema}"
        if key in quarantined_keys:
            print(f"  ⊘  Skip quarantined: {chunk.symbol} {chunk.venue}")
            continue

        result = upload_chunk(
            chunk, gcs_client,
            bq_client=None,
            overwrite=True,
            gcs_prefix=GCS_PREFIX,
            load_bq=False,
        )
        icon = "✓" if result.success else "✗"
        print(f"  {icon}  {chunk.symbol} {chunk.venue} → {result.message}")
        if result.success:
            uploaded.append((chunk, result.gcs_uri))

    # MD5 roundtrip
    all_ok = True
    if uploaded:
        print(f"\n  Verifying MD5 for {len(uploaded)} uploads...")
        import base64
        for chunk, uri in uploaded:
            blob_name = uri.replace(f"gs://{GCS_BUCKET}/", "")
            blob      = gcs_client.bucket(GCS_BUCKET).blob(blob_name)
            blob.reload()

            local = cache_path(chunk)
            h = hashlib.md5()
            with open(local, "rb") as f:
                for block in iter(lambda: f.read(65536), b""):
                    h.update(block)
            local_md5  = h.hexdigest()
            remote_md5 = base64.b64decode(blob.md5_hash).hex() if blob.md5_hash else "?"

            ok = local_md5 == remote_md5
            if not ok:
                all_ok = False
            icon = "✓" if ok else "✗"
            print(f"  {icon}  {chunk.symbol} {chunk.venue}  "
                  f"local={local_md5[:8]}…  remote={remote_md5[:8]}…")

    print(f"\n  GCS step: {'✓ OK' if all_ok else '✗ MD5 mismatch detected'}\n")
    return all_ok


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Cost & time estimate
# ─────────────────────────────────────────────────────────────────────────────

def step4_estimate(tickers: list[str]) -> None:
    """Print estimated cost, storage, and time for this run."""
    print("═" * 66)
    print("  STEP 4 — Run Estimate  (mbp-10, LIT, 4 years)")
    print("═" * 66)

    n_tickers = len(tickers)
    n_venues  = len(DOWNLOAD_VENUES)
    months    = pd.period_range(DOWNLOAD_START, DOWNLOAD_END, freq="M")
    n_months  = len(months)
    n_chunks  = n_tickers * n_venues * n_months

    rec_per    = ESTIMATE_RECORDS_PER_CHUNK.get(DOWNLOAD_SCHEMA, 7_000)
    mb_per     = ESTIMATE_MB_PER_CHUNK.get(DOWNLOAD_SCHEMA, 5.0)
    usd_per_gb = ESTIMATE_USD_PER_GB.get(DOWNLOAD_SCHEMA, 2.0)

    total_gb   = n_chunks * mb_per / 1024
    total_cost = total_gb * usd_per_gb
    est_hours  = n_chunks / 50.0

    print(f"\n  Schema     : {DOWNLOAD_SCHEMA}")
    print(f"  Date range : {DOWNLOAD_START} → {DOWNLOAD_END}  ({n_months} months)")
    print(f"  Tickers × venues × months : {n_tickers} × {n_venues} × {n_months} = {n_chunks:,} chunks")
    print(f"  Est. records              : ~{n_chunks * rec_per / 1e9:.2f}B")
    print(f"  Est. parquet storage      : ~{total_gb:.0f} GB")
    print(f"  Est. API cost             : ~${total_cost:,.0f}")
    print(f"  Est. download time        : ~{est_hours:.0f} hours  ({n_venues} venues / 4 workers)")
    print("\n  NOTE: Run with --dry-run to see this estimate before spending API credits.")
    print("═" * 66 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="LIT mbp-10 full download — 15 venues × N tickers × 4 years"
    )
    p.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated tickers to download (e.g. AAPL,MSFT,NVDA). "
             "Defaults to all tickers in tickers.csv.",
    )
    p.add_argument("--list-only",   action="store_true",
                   help="Only list available Databento datasets (Step 0), then exit")
    p.add_argument("--skip-upload", action="store_true",
                   help="Skip GCS upload step (Step 3)")
    p.add_argument("--dry-run",     action="store_true",
                   help="Print plan and cost estimate without downloading")
    p.add_argument("--force",       action="store_true",
                   help="Re-download even if cached parquet files already exist")
    return p


def main() -> None:
    args = build_parser().parse_args()

    # ── Resolve tickers ───────────────────────────────────────────────────────
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()]
    else:
        tickers = TICKERS

    if not tickers and not args.list_only:
        print("  ✗  No tickers specified. Use --tickers AAPL,MSFT or add tickers.csv")
        sys.exit(1)

    print("\n" + "═" * 66)
    print("  LIT MBP-10 DOWNLOAD  (4-year window)")
    print(f"  Tickers    : {tickers}")
    print(f"  Venues     : {len(DOWNLOAD_VENUES)} lit US exchanges")
    print(f"  Schema     : {DOWNLOAD_SCHEMA}")
    print(f"  Date range : {DOWNLOAD_START} → {DOWNLOAD_END}")
    print("═" * 66 + "\n")

    # Step 0: list available datasets
    step0_list_datasets()
    if args.list_only:
        return

    # Step 4 estimate (always printed first so user knows what they're about to run)
    step4_estimate(tickers)

    if args.dry_run:
        return

    # Step 1: download mbp-10 across all 15 lit venues
    lit_chunks = step1_download_lit(tickers, dry_run=False, force=args.force)

    # Step 2: validate
    reports = step2_validate(lit_chunks)

    # Step 3: upload
    ok = step3_upload_and_verify(lit_chunks, reports, skip=args.skip_upload)

    # ── Final verdict ─────────────────────────────────────────────────────────
    print("═" * 66)
    print("  RESULTS")
    print("─" * 66)
    quarantined = sum(1 for r in reports if r.get("quarantine"))
    print(f"  Tickers downloaded     : {len(tickers)}")
    print(f"  Venues                 : {len(DOWNLOAD_VENUES)}")
    print(f"  Chunks downloaded      : {len(lit_chunks)}")
    print(f"  Validation passed      : {len(reports) - quarantined}/{len(reports)}")
    print(f"  GCS roundtrip          : {'✓' if ok else '⊘ skipped / ✗ failed'}")

    if lit_chunks and not quarantined:
        print("\n  ✓  Download complete.")
    elif not lit_chunks:
        print("\n  ⚠  No data downloaded. Check DATABENTO_API_KEY in pipeline/.env")
    else:
        print(f"\n  ⚠  {quarantined} chunk(s) quarantined. Check validation output above.")
    print("═" * 66 + "\n")


if __name__ == "__main__":
    main()
