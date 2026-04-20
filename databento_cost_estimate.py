"""
Databento cost estimation — ohlcv-1h, 297 tickers (binary_score=5), 15 US lit venues, 4 years.
"""

import csv
import databento as db

API_KEY     = "db-FucuexXxQ6EQHSbTN9kJvSdFVfPx7"
IDEAL_START = "2022-01-01"
END         = "2026-01-01"
SCHEMA      = "ohlcv-1h"

# --------------------------------------------------------------------------
# Load 297 tickers (binary_score == 5)
# --------------------------------------------------------------------------
def load_tickers():
    tickers = []
    with open("/Users/mny_1026/Desktop/pipeline/tickers.txt") as f:
        for line in f:
            t = line.strip()
            if t:
                tickers.append(t)
    return tickers

tickers = load_tickers()
print(f"Tickers loaded: {len(tickers)}")
print()

client = db.Historical(key=API_KEY)

# --------------------------------------------------------------------------
# All 15 US lit equity venues
# --------------------------------------------------------------------------
LIT_VENUES = [
    "XNAS.ITCH",       # NASDAQ
    "XNYS.PILLAR",     # NYSE
    "ARCX.PILLAR",     # NYSE Arca
    "XBOS.ITCH",       # NASDAQ BX
    "XPSX.ITCH",       # NASDAQ PSX
    "BATS.PITCH",      # CBOE BZX
    "EDGX.PITCH",      # CBOE EDGX
    "EDGA.PITCH",      # CBOE EDGA
    "BATY.PITCH",      # CBOE BYX
    "IEXG.TOPS",       # IEX
    "MEMX.MEMOIR",     # MEMX
    "XASE.PILLAR",     # NYSE American
    "XCIS.TRADESBBO",  # NYSE National
    "EPRL.DOM",        # MIAX Pearl
    "XCHI.PILLAR",     # Chicago
]

# --------------------------------------------------------------------------
# Check availability per venue
# --------------------------------------------------------------------------
print("=" * 70)
print(f"Checking {SCHEMA} availability per venue ...")
print("=" * 70)

venue_starts = {}
for ds in LIT_VENUES:
    try:
        rng = client.metadata.get_dataset_range(dataset=ds)
        schema_ranges = rng.get("schema", {})
        if SCHEMA in schema_ranges:
            ds_start = schema_ranges[SCHEMA]["start"][:10]
            actual = max(IDEAL_START, ds_start)
            venue_starts[ds] = actual
            note = "✓ full" if ds_start <= IDEAL_START else f"from {ds_start}"
            print(f"  {ds:<22} {note}")
        else:
            venue_starts[ds] = None
            print(f"  {ds:<22} N/A")
    except Exception as e:
        venue_starts[ds] = None
        print(f"  {ds:<22} ERROR: {e}")

print()

# --------------------------------------------------------------------------
# Query cost per venue
# --------------------------------------------------------------------------
print("=" * 70)
print(f"Querying costs: {len(tickers)} tickers × {len(LIT_VENUES)} venues")
print(f"Period: {IDEAL_START} → {END}  |  Schema: {SCHEMA}")
print("=" * 70)

costs = {}
for ds in LIT_VENUES:
    start = venue_starts.get(ds)
    if start is None:
        costs[ds] = None
        print(f"  {ds:<22} SKIPPED (schema unavailable)")
        continue
    try:
        cost = client.metadata.get_cost(
            dataset=ds,
            symbols=tickers,
            schema=SCHEMA,
            start=start,
            end=END,
            stype_in="raw_symbol",
        )
        costs[ds] = float(cost)
        note = "  ← partial period" if start > IDEAL_START else ""
        print(f"  {ds:<22} ${float(cost):>10,.2f}{note}")
    except Exception as e:
        costs[ds] = None
        print(f"  {ds:<22} ERROR: {e}")

print()

# --------------------------------------------------------------------------
# Summary
# --------------------------------------------------------------------------
available  = {ds: c for ds, c in costs.items() if c is not None}
total_cost = sum(available.values())
n_tickers  = len(tickers)
n_venues   = len(available)
months     = 48  # 4 years
chunks     = n_tickers * n_venues * months

print("=" * 70)
print(f"  SUMMARY  —  {SCHEMA}  |  {IDEAL_START} → {END}")
print("=" * 70)
print(f"  Tickers            : {n_tickers}")
print(f"  Venues with data   : {n_venues} / {len(LIT_VENUES)}")
print(f"  Total API cost     : ${total_cost:,.2f}")
print(f"  Cost per ticker    : ${total_cost/n_tickers:,.2f}")
print(f"  Total chunks       : {chunks:,}  (ticker × venue × month)")
print(f"  Est. records       : ~{chunks * 130:,.0f}  (~130 bars/month)")
print(f"  Est. parquet size  : ~{chunks * 0.01 / 1024:.2f} GB")
print("=" * 70)
