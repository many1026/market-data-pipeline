"""
test_pipeline.py — Integration test for the full pipeline.

Tests:
  1. Downloads 3 days of data for 3 tickers (1 L1/tbbo, 1 L2/mbp-10)
  2. Runs full validation suite and prints report
  3. Uploads to GCS test prefix (gs://{BUCKET}/test/)
  4. Verifies roundtrip: download from GCS and compare checksums
  5. Prints estimated cost and time for full 144-ticker run

Run before any full-scale download:
  python test_pipeline.py

Prerequisites:
  - DATABENTO_API_KEY set in .env
  - GCS_BUCKET set in .env (optional — skip GCS steps if not set)
"""
from __future__ import annotations

import hashlib
import json
import logging
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from config import (
    DATABENTO_API_KEY,
    GCS_BUCKET,
    ESTIMATE_MB_PER_CHUNK,
    ESTIMATE_USD_PER_GB,
    ESTIMATE_RECORDS_PER_CHUNK,
    VENUES,
    SCHEMAS,
    TICKERS,
    START_DATE,
    END_DATE,
)
from downloader import (
    ChunkSpec,
    download_chunk,
    cache_path,
    cache_ok,
    generate_chunks,
)
from validator import validate_chunk

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ── Test parameters ───────────────────────────────────────────────────────────
TEST_TICKERS  = ["AAPL", "GPRO", "GRPN"]   # 3 tickers (mix of activity levels)
TEST_SCHEMAS  = ["tbbo", "mbp-10"]
TEST_VENUES   = ["XNAS.ITCH", "ARCX.PILLAR"]  # 2 venues for speed
TEST_DATE     = "2024-01-02"                # 3 trading days starting here
TEST_END_DATE = "2024-01-05"
TEST_GCS_PREFIX = "test"


def step1_download() -> list[ChunkSpec]:
    """Download 3 days of data for 3 tickers across 2 venues and 2 schemas."""
    print("\n" + "═" * 60)
    print("STEP 1 — Download (test subset)")
    print("═" * 60)

    if not DATABENTO_API_KEY:
        print("  ✗  DATABENTO_API_KEY not set — skipping download step")
        return []

    chunks = generate_chunks(
        schema="tbbo",
        tickers=TEST_TICKERS,
        venues=TEST_VENUES,
        start_date=TEST_DATE,
        end_date=TEST_END_DATE,
    )
    chunks += generate_chunks(
        schema="mbp-10",
        tickers=TEST_TICKERS[:1],  # 1 ticker for L2 (larger data)
        venues=TEST_VENUES[:1],
        start_date=TEST_DATE,
        end_date=TEST_END_DATE,
    )

    downloaded = []
    for chunk in chunks:
        print(f"  Downloading: {chunk.symbol} {chunk.venue} {chunk.year_month} [{chunk.schema}]")
        result = download_chunk(chunk, force=False)
        icon = "✓" if result.success else "✗"
        print(f"    {icon}  {result.message}  ({result.elapsed:.1f}s)")
        if result.success:
            downloaded.append(chunk)

    print(f"\n  Downloaded: {len(downloaded)}/{len(chunks)} chunks")
    return downloaded


def step2_validate(chunks: list[ChunkSpec]) -> list[dict]:
    """Run full validation suite on all downloaded chunks."""
    print("\n" + "═" * 60)
    print("STEP 2 — Validation")
    print("═" * 60)

    reports = []
    for chunk in chunks:
        ppath = cache_path(chunk)
        if not ppath.exists():
            print(f"  ✗  Missing: {ppath}")
            continue

        df = pd.read_parquet(ppath)
        rpt = validate_chunk(df, chunk)

        q = rpt.get("quarantine", False)
        w = rpt.get("warnings", [])
        n_in  = rpt.get("n_rows_in", 0)
        n_out = rpt.get("n_rows_out", 0)

        status = "QUARANTINE" if q else (f"WARN({','.join(w)})" if w else "PASS")
        print(f"  {chunk.symbol:<6} {chunk.venue:<18} {chunk.year_month} "
              f"[{chunk.schema}]  {status}  ({n_in:,}→{n_out:,} rows)")

        # Print check details
        for check_id, check in rpt.get("checks", {}).items():
            s = check.get("status", "?")
            d = check.get("detail", "")
            icon = "✓" if s == "PASS" else ("✗" if s == "ABORT" else "⚠")
            print(f"    {icon} [{check_id}] {s:<8}  {d}")

        rpt_clean = {k: v for k, v in rpt.items() if k != "filtered_df"}
        reports.append(rpt_clean)

    quarantined = sum(1 for r in reports if r.get("quarantine"))
    print(f"\n  Passed: {len(reports)-quarantined}/{len(reports)}, quarantined: {quarantined}")
    return reports


def step3_upload(chunks: list[ChunkSpec], reports: list[dict]) -> list[str]:
    """Upload validated chunks to GCS test prefix."""
    print("\n" + "═" * 60)
    print("STEP 3 — GCS Upload (test prefix)")
    print("═" * 60)

    if not GCS_BUCKET:
        print("  GCS_BUCKET not set — skipping GCS upload step")
        return []

    from uploader import upload_chunk, UploadResult
    from google.cloud import storage as gcs_lib

    gcs_client = gcs_lib.Client()
    quarantined_chunks = {
        f"{r['chunk']['symbol']}|{r['chunk']['venue']}|{r['chunk']['year_month']}|{r['chunk']['schema']}"
        for r in reports if r.get("quarantine")
    }

    uploaded_uris = []
    for chunk in chunks:
        key = f"{chunk.symbol}|{chunk.venue}|{chunk.year_month}|{chunk.schema}"
        if key in quarantined_chunks:
            print(f"  ⊘  SKIP quarantined: {chunk.symbol} {chunk.venue} {chunk.year_month}")
            continue

        result = upload_chunk(
            chunk, gcs_client,
            bq_client=None,     # skip BQ in test
            overwrite=True,
            gcs_prefix=TEST_GCS_PREFIX,
            load_bq=False,
        )
        icon = "✓" if result.success else "✗"
        print(f"  {icon}  {chunk.symbol} {chunk.venue} {chunk.year_month} → {result.message}")
        if result.success:
            uploaded_uris.append(result.gcs_uri)

    print(f"\n  Uploaded: {len(uploaded_uris)}/{len(chunks)} chunks")
    return uploaded_uris


def step4_roundtrip(uploaded_uris: list[str]) -> bool:
    """Download files back from GCS and compare checksums."""
    print("\n" + "═" * 60)
    print("STEP 4 — Roundtrip checksum verification")
    print("═" * 60)

    if not uploaded_uris:
        print("  No uploads to verify — skipping")
        return True

    from google.cloud import storage as gcs_lib
    gcs_client = gcs_lib.Client()

    all_ok = True
    for uri in uploaded_uris:
        # uri = gs://bucket/test/schema/symbol/venue/YYYY-MM.parquet
        blob_name = uri.replace(f"gs://{GCS_BUCKET}/", "")
        blob      = gcs_client.bucket(GCS_BUCKET).blob(blob_name)

        # Reconstruct local path from blob_name
        # test/{schema}/{symbol}/{venue_safe}/{YYYY-MM}.parquet
        parts = blob_name.split("/")
        if len(parts) < 5:
            continue
        _, schema, symbol, venue_safe, fname = parts[0], parts[1], parts[2], parts[3], parts[4]
        venue = venue_safe.replace("_", ".", 1)
        year_month = fname.replace(".parquet", "")
        chunk = ChunkSpec(schema=schema, symbol=symbol, venue=venue,
                          year_month=year_month, start="", end="")
        local = cache_path(chunk)

        if not local.exists():
            print(f"  ✗  Local file missing: {local}")
            all_ok = False
            continue

        # Local MD5
        h = hashlib.md5()
        with open(local, "rb") as f:
            for block in iter(lambda: f.read(65536), b""):
                h.update(block)
        local_md5 = h.hexdigest()

        # Remote MD5 (base64 → hex)
        import base64
        blob.reload()
        remote_md5 = base64.b64decode(blob.md5_hash).hex() if blob.md5_hash else "?"

        ok   = local_md5 == remote_md5
        icon = "✓" if ok else "✗"
        print(f"  {icon}  {symbol} {venue} {year_month}  local={local_md5[:8]}… remote={remote_md5[:8]}…")
        if not ok:
            all_ok = False

    return all_ok


def step5_cost_estimate() -> None:
    """Print estimated cost and time for the full 144-ticker run."""
    print("\n" + "═" * 60)
    print("STEP 5 — Full-run cost & time estimate")
    print("═" * 60)

    n_tickers = len(TICKERS)
    n_venues  = len(VENUES)
    months    = pd.period_range(START_DATE, END_DATE, freq="M")
    n_months  = len(months)

    for schema in SCHEMAS:
        n_chunks    = n_tickers * n_venues * n_months
        est_records = ESTIMATE_RECORDS_PER_CHUNK.get(schema, 7_000)
        est_mb      = ESTIMATE_MB_PER_CHUNK.get(schema, 1.0)
        est_usd_gb  = ESTIMATE_USD_PER_GB.get(schema, 1.0)

        total_records = n_chunks * est_records
        total_gb      = n_chunks * est_mb / 1024
        total_cost    = total_gb * est_usd_gb
        est_hours     = n_chunks / 50.0  # 50 chunks/hour at 4 workers

        print(f"\n  Schema: {schema}")
        print(f"    Chunks         : {n_chunks:,}  ({n_tickers} tickers × {n_venues} venues × {n_months} months)")
        print(f"    Est. records   : ~{total_records/1e9:.2f}B")
        print(f"    Est. storage   : ~{total_gb:.0f} GB parquet")
        print(f"    Est. API cost  : ~${total_cost:,.0f}  (rough, @ ${est_usd_gb}/GB)")
        print(f"    Est. DL time   : ~{est_hours:.0f} hours  (4 workers, 50 chunks/hr)")

    print(f"\n  NOTE: Estimates are heuristic. Actual costs depend on Databento")
    print(f"        pricing tier and actual data volume for each symbol/venue.")
    print("═" * 60 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    print("\n" + "═" * 60)
    print("  PIPELINE INTEGRATION TEST")
    print(f"  Tickers: {TEST_TICKERS}")
    print(f"  Date range: {TEST_DATE} → {TEST_END_DATE}")
    print(f"  Schemas: {TEST_SCHEMAS}")
    print(f"  Venues: {TEST_VENUES}")
    print("═" * 60)

    # Step 1: Download
    chunks = step1_download()

    # Step 2: Validate
    reports = step2_validate(chunks) if chunks else []

    # Step 3: Upload to GCS test prefix
    uploaded_uris = step3_upload(chunks, reports)

    # Step 4: Roundtrip checksum verification
    roundtrip_ok = step4_roundtrip(uploaded_uris)

    # Step 5: Full-run estimate
    step5_cost_estimate()

    # ── Final verdict ─────────────────────────────────────────────────────────
    print("═" * 60)
    print("  TEST RESULTS")
    print("─" * 60)
    print(f"  Download    : {'✓' if chunks else '⊘ (skipped)'}")
    print(f"  Validation  : {'✓' if reports else '⊘ (skipped)'}")
    print(f"  GCS Upload  : {'✓' if uploaded_uris else '⊘ (skipped)'}")
    print(f"  Roundtrip   : {'✓' if roundtrip_ok else '✗ checksum mismatch'}")

    all_ok = bool(chunks) and roundtrip_ok
    if all_ok:
        print("\n  ✓  All tests passed. Safe to run the full pipeline.")
        print("     python main.py download --schema tbbo")
    else:
        print("\n  ⚠  Some steps were skipped or failed.")
        print("     Set DATABENTO_API_KEY and GCS_BUCKET in pipeline/.env to run all steps.")
    print("═" * 60 + "\n")


if __name__ == "__main__":
    main()
