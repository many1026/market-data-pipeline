"""
main.py — CLI orchestrator for the Databento market data pipeline.

Usage:
  python main.py download  [--schema tbbo|mbp-10] [--dry-run] [--force]
  python main.py validate  [--schema tbbo|mbp-10] [--symbol AAPL] [--fix-quarantine]
  python main.py upload    [--schema tbbo|mbp-10] [--overwrite] [--no-bq]

Run test_pipeline.py first to validate the setup with a small data sample.
"""
from __future__ import annotations

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Ensure imports work from any working directory
sys.path.insert(0, str(Path(__file__).parent))

from config import (
    DATABENTO_API_KEY,
    GCS_BUCKET,
    TICKERS,
    SCHEMAS,
    VENUES,
    START_DATE,
    END_DATE,
    MAX_DOWNLOAD_WORKERS,
    MAX_UPLOAD_WORKERS,
    LOG_DIR,
    CACHE_DIR,
    QUARANTINE_DIR,
)


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging(level: int = logging.INFO) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=level,
        format="%(asctime)s  %(levelname)-7s  %(name)s — %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pre-flight checks
# ─────────────────────────────────────────────────────────────────────────────

def _require_api_key() -> None:
    if not DATABENTO_API_KEY:
        sys.exit(
            "\n  ✗  DATABENTO_API_KEY not set.\n"
            "     Create pipeline/.env with:\n"
            "     DATABENTO_API_KEY=db-xxxxxxxxxxxx\n"
        )


def _require_gcs_bucket() -> None:
    if not GCS_BUCKET:
        sys.exit(
            "\n  ✗  GCS_BUCKET not set.\n"
            "     Create pipeline/.env with:\n"
            "     GCS_BUCKET=my-bucket-name\n"
        )


def _check_tickers() -> None:
    if not TICKERS:
        sys.exit(
            "\n  ✗  No tickers found. Make sure pipeline/tickers.csv exists.\n"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Command handlers
# ─────────────────────────────────────────────────────────────────────────────

def cmd_download(args: argparse.Namespace) -> None:
    from downloader import download_all, setup_download_logger

    _check_tickers()
    if not args.dry_run:
        _require_api_key()

    schemas = [args.schema] if args.schema else SCHEMAS

    all_results = []

    if args.dry_run or len(schemas) == 1:
        # Dry-run: sequential (output is human-readable, no interleaving needed)
        # Single schema: no benefit from an outer pool
        for schema in schemas:
            results = download_all(
                schema=schema,
                tickers=None,
                venues=None,
                start_date=START_DATE,
                end_date=END_DATE,
                max_workers=MAX_DOWNLOAD_WORKERS,
                force=args.force,
                dry_run=args.dry_run,
            )
            all_results.extend(results)
    else:
        # Multiple schemas: run them in parallel (each schema already
        # uses its own ThreadPoolExecutor internally)
        def _run_schema(schema: str):
            return download_all(
                schema=schema,
                tickers=None,
                venues=None,
                start_date=START_DATE,
                end_date=END_DATE,
                max_workers=MAX_DOWNLOAD_WORKERS,
                force=args.force,
                dry_run=False,
            )

        with ThreadPoolExecutor(max_workers=len(schemas)) as pool:
            futures = {pool.submit(_run_schema, s): s for s in schemas}
            for fut in as_completed(futures):
                all_results.extend(fut.result())

    if args.dry_run:
        return  # dry-run already printed plan

    # ── Final download summary ────────────────────────────────────────────────
    total    = len(all_results)
    ok       = sum(1 for r in all_results if r.success and not r.cached)
    cached   = sum(1 for r in all_results if r.cached)
    failed   = sum(1 for r in all_results if not r.success)
    total_rows = sum(r.rows for r in all_results)

    print("\n" + "═" * 66)
    print("  DOWNLOAD SUMMARY")
    print("─" * 66)
    print(f"  Total chunks  : {total:,}")
    print(f"  Downloaded    : {ok:,}")
    print(f"  From cache    : {cached:,}")
    print(f"  Failed        : {failed:,}")
    print(f"  Total rows    : {total_rows:,}")
    if failed:
        print("\n  Failed chunks:")
        for r in all_results:
            if not r.success:
                print(f"    {r.chunk.symbol} {r.chunk.venue} {r.chunk.year_month}: {r.message}")
    print("═" * 66 + "\n")


def cmd_validate(args: argparse.Namespace) -> None:
    from validator import validate_all

    schemas = [args.schema] if args.schema else SCHEMAS

    all_reports = []

    if len(schemas) == 1:
        reports = validate_all(schema=schemas[0], symbol_filter=args.symbol)
        all_reports.extend(reports)
    else:
        # Multiple schemas validated in parallel (each validate_all already
        # uses its own ThreadPoolExecutor internally)
        def _run_validate(schema: str):
            return validate_all(schema=schema, symbol_filter=args.symbol)

        with ThreadPoolExecutor(max_workers=len(schemas)) as pool:
            futures = {pool.submit(_run_validate, s): s for s in schemas}
            for fut in as_completed(futures):
                all_reports.extend(fut.result())

    quarantined = [r for r in all_reports if r.get("quarantine")]
    warned      = [r for r in all_reports if r.get("warnings") and not r.get("quarantine")]
    passed      = [r for r in all_reports if not r.get("quarantine") and not r.get("warnings")]

    corp_event_tickers = set()
    for r in all_reports:
        for check_key, check_val in r.get("checks", {}).items():
            if check_key == "H" and check_val.get("status") == "WARNING":
                corp_event_tickers.add(r["chunk"]["symbol"])

    print("\n" + "═" * 66)
    print("  VALIDATION SUMMARY")
    print("─" * 66)
    print(f"  Total validated : {len(all_reports):,}")
    print(f"  Passed          : {len(passed):,}")
    print(f"  Warnings        : {len(warned):,}")
    print(f"  Quarantined     : {len(quarantined):,}")
    if corp_event_tickers:
        print(f"  Corp-event tickers ({len(corp_event_tickers)}): {sorted(corp_event_tickers)}")
    if quarantined:
        print("\n  Quarantined chunks (reasons):")
        for r in quarantined[:20]:
            c = r["chunk"]
            aborted = [k for k, v in r["checks"].items() if v.get("status") == "ABORT"]
            print(f"    {c['symbol']} {c['venue']} {c['year_month']} — checks: {aborted}")
        if len(quarantined) > 20:
            print(f"    ... and {len(quarantined)-20} more")

    # ── --fix-quarantine: re-download quarantined chunks in parallel ──────────
    if args.fix_quarantine and quarantined:
        _require_api_key()
        import pandas as pd
        from downloader import ChunkSpec, download_chunk

        def _redownload(r: dict):
            c = r["chunk"]
            chunk = ChunkSpec(
                schema=c["schema"], symbol=c["symbol"], venue=c["venue"],
                year_month=c["year_month"], start="", end="",
            )
            month = pd.Period(c["year_month"], freq="M")
            chunk.start = month.start_time.strftime("%Y-%m-%dT00:00:00Z")
            chunk.end   = (month + 1).start_time.strftime("%Y-%m-%dT00:00:00Z")
            return download_chunk(chunk, force=True)

        print(f"\n  Re-downloading {len(quarantined)} quarantined chunks "
              f"({MAX_DOWNLOAD_WORKERS} workers)...")
        with ThreadPoolExecutor(max_workers=MAX_DOWNLOAD_WORKERS) as pool:
            futures = {pool.submit(_redownload, r): r for r in quarantined}
            for fut in as_completed(futures):
                result = fut.result()
                print(f"    {'✓' if result.success else '✗'}  {result.message}")

    print("═" * 66 + "\n")


def cmd_upload(args: argparse.Namespace) -> None:
    _require_gcs_bucket()

    from uploader import upload_all, estimate_gcs_storage

    schemas = [args.schema] if args.schema else SCHEMAS

    all_results = []

    if len(schemas) == 1:
        results = upload_all(
            schema=schemas[0],
            overwrite=args.overwrite,
            max_workers=MAX_UPLOAD_WORKERS,
            load_bq=not args.no_bq,
        )
        all_results.extend(results)
    else:
        # Multiple schemas uploaded in parallel (each upload_all already uses
        # its own ThreadPoolExecutor internally)
        def _run_upload(schema: str):
            return upload_all(
                schema=schema,
                overwrite=args.overwrite,
                max_workers=MAX_UPLOAD_WORKERS,
                load_bq=not args.no_bq,
            )

        with ThreadPoolExecutor(max_workers=len(schemas)) as pool:
            futures = {pool.submit(_run_upload, s): s for s in schemas}
            for fut in as_completed(futures):
                all_results.extend(fut.result())

    total   = len(all_results)
    ok      = sum(1 for r in all_results if r.success and not r.skipped)
    skipped = sum(1 for r in all_results if r.skipped)
    failed  = sum(1 for r in all_results if not r.success)
    bq_ok   = sum(1 for r in all_results if r.bq_ok)

    # Estimate total GCS storage from local cache
    total_gb = sum(estimate_gcs_storage(s) for s in schemas)

    print("\n" + "═" * 66)
    print("  UPLOAD SUMMARY")
    print("─" * 66)
    print(f"  Total chunks     : {total:,}")
    print(f"  Uploaded         : {ok:,}")
    print(f"  Skipped (exists) : {skipped:,}")
    print(f"  Failed           : {failed:,}")
    print(f"  BQ loads ok      : {bq_ok:,}")
    print(f"  Est. GCS storage : {total_gb:.2f} GB (from local cache)")
    print("═" * 66 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI argument parser
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Databento market data pipeline — download / validate / upload",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")

    sub = parser.add_subparsers(dest="command", required=True)

    # download
    dl = sub.add_parser("download", help="Download data from Databento")
    dl.add_argument("--schema", choices=SCHEMAS, default=None,
                    help="Schema to download (default: all)")
    dl.add_argument("--dry-run", action="store_true",
                    help="Print plan without downloading")
    dl.add_argument("--force", action="store_true",
                    help="Re-download even if cache is valid")

    # validate
    va = sub.add_parser("validate", help="Validate cached parquet files")
    va.add_argument("--schema", choices=SCHEMAS, default=None)
    va.add_argument("--symbol", default=None, help="Validate single symbol only")
    va.add_argument("--fix-quarantine", action="store_true",
                    help="Re-download quarantined chunks")

    # upload
    up = sub.add_parser("upload", help="Upload validated chunks to GCS + BigQuery")
    up.add_argument("--schema", choices=SCHEMAS, default=None)
    up.add_argument("--overwrite", action="store_true",
                    help="Overwrite existing GCS objects")
    up.add_argument("--no-bq", action="store_true",
                    help="Skip BigQuery load (GCS only)")

    return parser


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()
    _setup_logging(logging.DEBUG if args.verbose else logging.INFO)

    dispatch = {
        "download": cmd_download,
        "validate": cmd_validate,
        "upload":   cmd_upload,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
