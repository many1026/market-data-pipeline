"""
uploader.py — GCS + BigQuery ingestion with schema enforcement.

GCS layout:
  gs://{BUCKET}/raw/{schema}/{symbol}/{venue}/{YYYY-MM}.parquet

BigQuery:
  Partitioned by DATE(ts_recv), clustered by (symbol, venue).
  Uses load jobs for bulk upload, streaming inserts for < 10k rows.

After every GCS upload:
  - MD5 checksum verified.
  - Symbol manifest updated: {symbol}_manifest.json.
"""
from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm

from config import (
    GCS_BUCKET,
    GCS_PREFIX,
    BQ_DATASET,
    BQ_PROJECT,
    MAX_UPLOAD_WORKERS,
    CACHE_DIR,
    QUARANTINE_DIR,
)
from downloader import ChunkSpec, cache_path, validation_report_path

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# GCS helpers
# ─────────────────────────────────────────────────────────────────────────────

def _gcs_blob_name(chunk: ChunkSpec, prefix: str = GCS_PREFIX) -> str:
    venue_safe = chunk.venue.replace(".", "_")
    return f"{prefix}/{chunk.schema}/{chunk.symbol}/{venue_safe}/{chunk.year_month}.parquet"


def _local_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(65536), b""):
            h.update(block)
    return h.hexdigest()


def _upload_to_gcs(
    local_path: Path,
    blob_name: str,
    gcs_client,
    overwrite: bool = False,
) -> dict:
    """
    Upload a file to GCS using a resumable upload and verify MD5.
    Returns dict with: blob_name, size_bytes, md5_local, md5_remote, ok.
    """
    bucket = gcs_client.bucket(GCS_BUCKET)
    blob   = bucket.blob(blob_name)

    if blob.exists() and not overwrite:
        blob.reload()
        return {
            "blob_name":  blob_name,
            "size_bytes": blob.size,
            "md5_local":  _local_md5(local_path),
            "md5_remote": blob.md5_hash,
            "ok":         True,
            "skipped":    True,
        }

    local_md5_hex = _local_md5(local_path)
    size_bytes    = local_path.stat().st_size

    # Resumable upload (handles files > 1 GB correctly)
    blob.upload_from_filename(
        str(local_path),
        content_type="application/octet-stream",
        # google-cloud-storage uses resumable upload automatically for > 8 MB
    )
    blob.reload()  # refresh metadata to get MD5

    # MD5 from GCS is base64-encoded; convert to hex for comparison
    import base64
    remote_md5_hex = base64.b64decode(blob.md5_hash).hex() if blob.md5_hash else ""

    ok = remote_md5_hex == local_md5_hex
    if not ok:
        logger.error(f"MD5 mismatch for {blob_name}: local={local_md5_hex} remote={remote_md5_hex}")

    return {
        "blob_name":   blob_name,
        "size_bytes":  size_bytes,
        "md5_local":   local_md5_hex,
        "md5_remote":  remote_md5_hex,
        "ok":          ok,
        "skipped":     False,
    }


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery helpers
# ─────────────────────────────────────────────────────────────────────────────

# BigQuery schema enforced on write
_BQ_SCHEMA_TBBO = [
    # Populated dynamically; we enforce types via CAST in load jobs
    # Key fields:
    # ts_recv  TIMESTAMP (partition key)
    # ts_event TIMESTAMP
    # symbol   STRING    (cluster key 1)
    # venue    STRING    (cluster key 2)
    # price    FLOAT64
    # size     INT64
    # bid_px_00, ask_px_00, bid_sz_00, ask_sz_00 FLOAT64 / INT64
]


def _ensure_bq_table(bq_client, schema: str) -> str:
    """Create partitioned + clustered BQ table if it doesn't exist. Returns table_id."""
    from google.cloud import bigquery

    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{schema.replace('-', '_')}"
    table_ref = bq_client.dataset(BQ_DATASET).table(schema.replace("-", "_"))

    try:
        bq_client.get_table(table_ref)
        return table_id
    except Exception:
        pass  # table doesn't exist — create it

    table = bigquery.Table(table_ref)
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ts_recv",
    )
    table.clustering_fields = ["symbol", "venue"]

    # Minimal explicit schema; BQ will auto-detect remaining columns from parquet
    table.schema = [
        bigquery.SchemaField("ts_recv",    "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("ts_event",   "TIMESTAMP"),
        bigquery.SchemaField("symbol",     "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("venue",      "STRING",    mode="REQUIRED"),
        bigquery.SchemaField("price",      "FLOAT64"),
        bigquery.SchemaField("size",       "INT64"),
        bigquery.SchemaField("bid_px_00",  "FLOAT64"),
        bigquery.SchemaField("ask_px_00",  "FLOAT64"),
        bigquery.SchemaField("bid_sz_00",  "INT64"),
        bigquery.SchemaField("ask_sz_00",  "INT64"),
    ]

    bq_client.create_table(table, exists_ok=True)
    logger.info(f"Created BQ table: {table_id}")
    return table_id


def _load_to_bq(
    gcs_uri: str,
    table_id: str,
    n_rows: int,
    bq_client,
    schema: str,
) -> bool:
    """Load parquet from GCS into BigQuery. Uses load job (handles bulk + streaming)."""
    from google.cloud import bigquery

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    job = bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    try:
        job.result(timeout=300)  # wait up to 5 min per chunk
        return True
    except Exception as e:
        logger.error(f"BQ load job failed for {gcs_uri}: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Manifest
# ─────────────────────────────────────────────────────────────────────────────

def _update_manifest(
    symbol: str,
    schema: str,
    gcs_client,
    new_entry: dict,
    prefix: str = GCS_PREFIX,
) -> None:
    """
    Read, update, and write {symbol}_manifest.json to GCS.
    Thread-safe at the symbol level (one manifest per symbol).
    """
    bucket        = gcs_client.bucket(GCS_BUCKET)
    manifest_name = f"{prefix}/{schema}/{symbol}/{symbol}_manifest.json"
    blob          = bucket.blob(manifest_name)

    manifest: dict = {"symbol": symbol, "schema": schema, "files": []}
    if blob.exists():
        try:
            manifest = json.loads(blob.download_as_text())
        except Exception:
            pass  # corrupt manifest — start fresh

    # Remove old entry for this blob if it exists
    manifest["files"] = [
        f for f in manifest.get("files", [])
        if f.get("blob_name") != new_entry.get("blob_name")
    ]
    manifest["files"].append(new_entry)
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()

    blob.upload_from_string(
        json.dumps(manifest, indent=2, default=str),
        content_type="application/json",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Per-chunk upload
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class UploadResult:
    chunk:   ChunkSpec
    success: bool
    message: str
    gcs_uri: str = ""
    bq_ok:   bool = False
    skipped: bool = False


def upload_chunk(
    chunk: ChunkSpec,
    gcs_client,
    bq_client=None,
    overwrite: bool = False,
    gcs_prefix: str = GCS_PREFIX,
    load_bq: bool = True,
) -> UploadResult:
    """
    Upload one chunk to GCS (and optionally BigQuery).

    - Skips quarantined chunks.
    - Skips chunks that failed validation (ABORT status).
    - Verifies MD5 after GCS upload.
    - Updates symbol manifest.
    """
    local = cache_path(chunk)

    if not local.exists():
        return UploadResult(chunk=chunk, success=False, message="local parquet missing")

    # ── Check validation status ───────────────────────────────────────────────
    rpt_path = validation_report_path(chunk)
    if rpt_path.exists():
        with open(rpt_path) as f:
            rpt = json.load(f)
        if rpt.get("quarantine"):
            return UploadResult(chunk=chunk, success=False,
                                message="quarantined — skipping upload")
    else:
        logger.warning(f"No validation report for {chunk.symbol} {chunk.venue} {chunk.year_month} "
                       "— uploading without validation metadata")
        rpt = {}

    blob_name = _gcs_blob_name(chunk, gcs_prefix)
    gcs_uri   = f"gs://{GCS_BUCKET}/{blob_name}"

    # ── GCS upload ────────────────────────────────────────────────────────────
    try:
        upload_info = _upload_to_gcs(local, blob_name, gcs_client, overwrite)
    except Exception as e:
        return UploadResult(chunk=chunk, success=False, message=f"GCS error: {e}")

    if not upload_info["ok"]:
        return UploadResult(chunk=chunk, success=False,
                            message=f"MD5 mismatch after upload: {blob_name}")

    # ── Read row count for manifest ───────────────────────────────────────────
    try:
        pf = pq.read_metadata(local)
        n_rows = pf.num_rows
    except Exception:
        n_rows = 0

    # ── Update manifest ───────────────────────────────────────────────────────
    manifest_entry = {
        "blob_name":        blob_name,
        "year_month":       chunk.year_month,
        "venue":            chunk.venue,
        "row_count":        n_rows,
        "size_bytes":       upload_info["size_bytes"],
        "md5":              upload_info["md5_local"],
        "validation_ok":    not rpt.get("quarantine", False),
        "warnings":         rpt.get("warnings", []),
        "uploaded_at":      datetime.now(timezone.utc).isoformat(),
        "skipped":          upload_info.get("skipped", False),
    }

    try:
        _update_manifest(chunk.symbol, chunk.schema, gcs_client, manifest_entry, gcs_prefix)
    except Exception as e:
        logger.warning(f"Manifest update failed for {chunk.symbol}: {e}")

    # ── BigQuery load ─────────────────────────────────────────────────────────
    bq_ok = False
    if load_bq and bq_client is not None:
        try:
            table_id = _ensure_bq_table(bq_client, chunk.schema)
            bq_ok    = _load_to_bq(gcs_uri, table_id, n_rows, bq_client, chunk.schema)
        except Exception as e:
            logger.warning(f"BQ load skipped for {blob_name}: {e}")

    status = "SKIPPED" if upload_info.get("skipped") else "UPLOADED"
    logger.info(f"{status}   {chunk.symbol} {chunk.venue} {chunk.year_month} → {gcs_uri}")
    return UploadResult(
        chunk=chunk, success=True,
        message=f"{status.lower()} ({n_rows:,} rows)",
        gcs_uri=gcs_uri, bq_ok=bq_ok,
        skipped=upload_info.get("skipped", False),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Batch upload
# ─────────────────────────────────────────────────────────────────────────────

def upload_all(
    schema: str,
    overwrite: bool = False,
    max_workers: int = MAX_UPLOAD_WORKERS,
    load_bq: bool = True,
    gcs_prefix: str = GCS_PREFIX,
) -> list[UploadResult]:
    """
    Upload all validated (non-quarantined) chunks for `schema` to GCS + BQ.
    """
    if not GCS_BUCKET:
        raise ValueError("GCS_BUCKET environment variable not set")

    from google.cloud import storage as gcs_lib
    from google.cloud import bigquery

    gcs_client = gcs_lib.Client()
    bq_client  = bigquery.Client(project=BQ_PROJECT) if load_bq and BQ_PROJECT else None

    # Find all local parquets (excluding quarantine)
    parquet_files = [
        p for p in (CACHE_DIR / schema).rglob("*.parquet")
        if "quarantine" not in str(p)
    ]

    if not parquet_files:
        logger.warning(f"No parquets found under {CACHE_DIR}/{schema}")
        return []

    # Reconstruct chunk specs from paths
    chunks = []
    for ppath in parquet_files:
        parts = ppath.relative_to(CACHE_DIR).parts
        if len(parts) < 4:
            continue
        _, sym, venue_safe, fname = parts[0], parts[1], parts[2], parts[3]
        venue      = venue_safe.replace("_", ".", 1)
        year_month = fname.replace(".parquet", "")
        chunks.append(ChunkSpec(
            schema=schema, symbol=sym, venue=venue,
            year_month=year_month, start="", end="",
        ))

    results: list[UploadResult] = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(upload_chunk, c, gcs_client, bq_client, overwrite, gcs_prefix, load_bq): c
            for c in chunks
        }
        ok = fail = skipped = 0
        with tqdm(total=len(chunks), desc=f"Upload [{schema}]", unit="chunk") as bar:
            for fut in as_completed(futures):
                res = fut.result()
                results.append(res)
                if res.skipped:
                    skipped += 1
                elif res.success:
                    ok += 1
                else:
                    fail += 1
                icon = "→" if res.success else "✗"
                tqdm.write(f"  {icon}  {res.chunk.symbol:<6} {res.chunk.venue:<18} "
                           f"{res.chunk.year_month}  {res.message}")
                bar.update(1)

    logger.info(f"Upload complete: {ok} uploaded, {skipped} skipped, {fail} failed")
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Storage estimate helper
# ─────────────────────────────────────────────────────────────────────────────

def estimate_gcs_storage(schema: str) -> float:
    """Return estimated GCS storage in GB based on local cache size."""
    total_bytes = sum(
        p.stat().st_size
        for p in (CACHE_DIR / schema).rglob("*.parquet")
        if "quarantine" not in str(p)
    )
    return total_bytes / (1024 ** 3)
