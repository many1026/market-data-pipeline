"""
test_pipeline.py — Integration smoke test. Run this BEFORE any full-scale download.

What it does
────────────
  Step 0 — Lists all datasets available in your Databento account
            (use this to confirm ATS dataset IDs before the full run)
  Step 1 — Downloads 3 tickers × 1 lit venue × Jan 2024 (mbp-10)
  Step 2 — Downloads 3 tickers × 1 ATS venue × Jan 2024 (trades)
  Step 3 — Validates all downloaded chunks (all 8 checks)
  Step 4 — (Optional) Uploads to gs://{BUCKET}/test/ and verifies MD5
  Step 5 — Prints full-run cost & time estimate

Usage:
  cd pipeline/
  python test_pipeline.py                  # full test (needs API key)
  python test_pipeline.py --list-only      # only list available datasets
  python test_pipeline.py --skip-upload    # skip GCS step

Prerequisites:
  DATABENTO_API_KEY set in pipeline/.env
  GCS_BUCKET set in pipeline/.env (optional — only for step 4)
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
    ATS_SCHEMA,
    LIT_VENUES,
    ATS_VENUES,
    TICKERS,
    START_DATE,
    END_DATE,
    ESTIMATE_MB_PER_CHUNK,
    ESTIMATE_USD_PER_GB,
    ESTIMATE_RECORDS_PER_CHUNK,
    SCHEMA_VENUE_MAP,
)
from downloader import ChunkSpec, download_chunk, cache_path, generate_chunks
from validator import validate_chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Test parameters ───────────────────────────────────────────────────────────
# Small subset: 3 tickers, 1 month, 1 venue per type
TEST_TICKERS      = ["AAPL", "GPRO", "GRPN"]   # mix of activity levels
TEST_LIT_VENUES   = ["XNAS.ITCH"]               # Nasdaq — most liquid, good test
TEST_ATS_VENUES   = ["FINN.NLS"]                # FINRA/Nasdaq TRF — largest ATS aggregator
TEST_START        = "2024-01-01"
TEST_END          = "2024-01-31"                # 1 month (one chunk per ticker/venue)
TEST_GCS_PREFIX   = "test"


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
            tag = ""
            if d in LIT_VENUES:
                tag = "  ← lit exchange (mbp-10)"
            elif d in ATS_VENUES:
                tag = "  ← ATS/TRF (trades)"
            print(f"    {d}{tag}")

        # Check which configured ATS venues are actually available
        missing_ats = [v for v in ATS_VENUES if v not in datasets]
        if missing_ats:
            print(f"\n  ⚠  ATS venues in config NOT found in your account:")
            for v in missing_ats:
                print(f"    - {v}  → remove from ATS_VENUES or check subscription")
        else:
            print(f"\n  ✓  All {len(ATS_VENUES)} configured ATS venues are available")

    except Exception as e:
        print(f"  ✗  Could not list datasets: {e}")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Download lit (mbp-10)
# ─────────────────────────────────────────────────────────────────────────────

def step1_download_lit() -> list[ChunkSpec]:
    """Download mbp-10 data from 1 lit venue for 3 tickers × 1 month."""
    print("═" * 66)
    print(f"  STEP 1 — Download LIT  [{LIT_SCHEMA}]  {TEST_LIT_VENUES}")
    print("═" * 66)

    if not DATABENTO_API_KEY:
        print("  ✗  DATABENTO_API_KEY not set — skipping\n")
        return []

    chunks = generate_chunks(
        schema=LIT_SCHEMA,
        tickers=TEST_TICKERS,
        venues=TEST_LIT_VENUES,
        start_date=TEST_START,
        end_date=TEST_END,
    )

    downloaded: list[ChunkSpec] = []
    for chunk in chunks:
        print(f"  → {chunk.symbol:<6} {chunk.venue:<18} {chunk.year_month}")
        result = download_chunk(chunk, force=False)
        icon = "✓" if result.success else "✗"
        print(f"    {icon}  {result.message}  ({result.elapsed:.1f}s)")
        if result.success:
            downloaded.append(chunk)

    print(f"\n  {len(downloaded)}/{len(chunks)} chunks downloaded\n")
    return downloaded


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Download ATS (trades)
# ─────────────────────────────────────────────────────────────────────────────

def step2_download_ats() -> list[ChunkSpec]:
    """Download trades data from 1 ATS venue for 3 tickers × 1 month."""
    print("═" * 66)
    print(f"  STEP 2 — Download ATS  [{ATS_SCHEMA}]  {TEST_ATS_VENUES}")
    print("═" * 66)

    if not DATABENTO_API_KEY:
        print("  ✗  DATABENTO_API_KEY not set — skipping\n")
        return []

    chunks = generate_chunks(
        schema=ATS_SCHEMA,
        tickers=TEST_TICKERS,
        venues=TEST_ATS_VENUES,
        start_date=TEST_START,
        end_date=TEST_END,
    )

    downloaded: list[ChunkSpec] = []
    for chunk in chunks:
        print(f"  → {chunk.symbol:<6} {chunk.venue:<18} {chunk.year_month}")
        result = download_chunk(chunk, force=False)
        icon = "✓" if result.success else "✗"
        print(f"    {icon}  {result.message}  ({result.elapsed:.1f}s)")
        if result.success:
            downloaded.append(chunk)

    print(f"\n  {len(downloaded)}/{len(chunks)} chunks downloaded\n")
    return downloaded


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Validation
# ─────────────────────────────────────────────────────────────────────────────

def step3_validate(chunks: list[ChunkSpec]) -> list[dict]:
    """Run all 8 data quality checks on every downloaded chunk."""
    print("═" * 66)
    print("  STEP 3 — Validation (8 checks per chunk)")
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
# Step 4 — GCS upload + MD5 roundtrip (optional)
# ─────────────────────────────────────────────────────────────────────────────

def step4_upload_and_verify(
    chunks: list[ChunkSpec],
    reports: list[dict],
    skip: bool = False,
) -> bool:
    """Upload to gs://{BUCKET}/test/ and verify MD5 checksums."""
    print("═" * 66)
    print("  STEP 4 — GCS Upload + MD5 Roundtrip")
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

    uploaded: list[tuple[ChunkSpec, str]] = []   # (chunk, gcs_uri)
    for chunk in chunks:
        key = f"{chunk.symbol}|{chunk.venue}|{chunk.year_month}|{chunk.schema}"
        if key in quarantined_keys:
            print(f"  ⊘  Skip quarantined: {chunk.symbol} {chunk.venue}")
            continue

        result = upload_chunk(
            chunk, gcs_client,
            bq_client=None,
            overwrite=True,
            gcs_prefix=TEST_GCS_PREFIX,
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
# Step 5 — Full-run cost estimate
# ─────────────────────────────────────────────────────────────────────────────

def step5_estimate() -> None:
    """Print estimated cost, storage, and time for the full production run."""
    print("═" * 66)
    print("  STEP 5 — Full-run Estimate")
    print("═" * 66)

    n_tickers = len(TICKERS)
    months    = pd.period_range(START_DATE, END_DATE, freq="M")
    n_months  = len(months)

    total_cost = 0.0
    for schema, venues in SCHEMA_VENUE_MAP.items():
        n_venues = len(venues)
        n_chunks = n_tickers * n_venues * n_months
        rec_per  = ESTIMATE_RECORDS_PER_CHUNK.get(schema, 7_000)
        mb_per   = ESTIMATE_MB_PER_CHUNK.get(schema, 1.0)
        usd_per  = ESTIMATE_USD_PER_GB.get(schema, 1.0)

        total_gb   = n_chunks * mb_per / 1024
        cost       = total_gb * usd_per
        total_cost += cost
        est_hours  = n_chunks / 50.0

        label = "LIT (mbp-10)" if schema == LIT_SCHEMA else "ATS (trades)"
        print(f"\n  {label}")
        print(f"    Venues × tickers × months : {n_venues} × {n_tickers} × {n_months} = {n_chunks:,} chunks")
        print(f"    Est. records              : ~{n_chunks * rec_per / 1e9:.2f}B")
        print(f"    Est. parquet storage      : ~{total_gb:.0f} GB")
        print(f"    Est. API cost             : ~${cost:,.0f}")
        print(f"    Est. download time        : ~{est_hours:.0f} hours  (4 workers)")

    print(f"\n  TOTAL estimated API cost    : ~${total_cost:,.0f}")
    print("  NOTE: Estimates are heuristic. Run `python main.py download --dry-run`")
    print("        for a per-schema breakdown before spending API credits.")
    print("═" * 66 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Pipeline smoke test")
    p.add_argument("--list-only",    action="store_true",
                   help="Only list available Databento datasets (Step 0), then exit")
    p.add_argument("--skip-upload",  action="store_true",
                   help="Skip GCS upload step (Step 4)")
    p.add_argument("--skip-ats",     action="store_true",
                   help="Skip ATS download step (Step 2); useful if ATS IDs unverified")
    return p


def main() -> None:
    args = build_parser().parse_args()

    print("\n" + "═" * 66)
    print("  PIPELINE SMOKE TEST")
    print(f"  Tickers    : {TEST_TICKERS}")
    print(f"  Date range : {TEST_START} → {TEST_END}  (1 month)")
    print(f"  Lit schema : {LIT_SCHEMA}  @ {TEST_LIT_VENUES}")
    print(f"  ATS schema : {ATS_SCHEMA}  @ {TEST_ATS_VENUES}")
    print("═" * 66 + "\n")

    # Step 0: list available datasets
    step0_list_datasets()
    if args.list_only:
        return

    # Step 1: download lit
    lit_chunks = step1_download_lit()

    # Step 2: download ATS
    ats_chunks: list[ChunkSpec] = []
    if not args.skip_ats:
        ats_chunks = step2_download_ats()
    else:
        print("  STEP 2 — ATS download skipped (--skip-ats)\n")

    all_chunks = lit_chunks + ats_chunks

    # Step 3: validate
    reports = step3_validate(all_chunks)

    # Step 4: upload
    ok = step4_upload_and_verify(all_chunks, reports, skip=args.skip_upload)

    # Step 5: estimate
    step5_estimate()

    # ── Final verdict ─────────────────────────────────────────────────────────
    print("═" * 66)
    print("  RESULTS")
    print("─" * 66)
    quarantined = sum(1 for r in reports if r.get("quarantine"))
    print(f"  Lit chunks downloaded  : {len(lit_chunks)}")
    print(f"  ATS chunks downloaded  : {len(ats_chunks)}")
    print(f"  Validation passed      : {len(reports) - quarantined}/{len(reports)}")
    print(f"  GCS roundtrip          : {'✓' if ok else '⊘ skipped / ✗ failed'}")

    if all_chunks and not quarantined:
        print("\n  ✓  Smoke test passed. Safe to run the full pipeline:")
        print("     python main.py download --mode lit")
        print("     python main.py download --mode ats  # after verifying ATS IDs")
    elif not all_chunks:
        print("\n  ⚠  No data downloaded. Check DATABENTO_API_KEY in pipeline/.env")
        print("     and verify ATS dataset IDs with --list-only")
    else:
        print(f"\n  ⚠  {quarantined} chunk(s) quarantined. Check validation output above.")
    print("═" * 66 + "\n")


if __name__ == "__main__":
    main()
