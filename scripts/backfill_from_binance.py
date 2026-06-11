#!/usr/bin/env python3
"""
Backfill ETH-USD and SOL-USD 1m candle data from Binance bulk archive.

Downloads monthly ZIP files from data.binance.vision, extracts CSVs,
and inserts into the local candles.db (same schema as Coinbase data).

Only fills gaps before the earliest existing Coinbase data per pair.
Uses file lock to prevent conflicts with the cron updater.

!! DATA-QUALITY CAVEAT !!
Binance prices are USDT-quoted; Coinbase prices are USD-quoted, and the venues
differ. The USDT/USD basis has reached multiple percent during depegs, so every
splice seam between Binance and Coinbase rows introduces an artificial price
jump — fake volatility and fake grid/DCA fills in any backtest crossing the
seam. Treat backtests over spliced ranges as approximate. Requires --confirm.
"""

import csv
import fcntl
import io
import os
import sqlite3
import sys
import time
import zipfile
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'candles.db')
LOCK_FILE = os.path.join(BASE_DIR, 'candles.lock')

# Mapping: Coinbase pair -> Binance symbol
PAIRS = {
    'ETH-USD': 'ETHUSDT',
    'SOL-USD': 'SOLUSDT',
}

# How far back to fill (April 2024)
TARGET_START = int(datetime(2024, 4, 9, tzinfo=timezone.utc).timestamp())

BINANCE_BASE = "https://data.binance.vision/data/spot/monthly/klines"


def get_earliest_timestamp(conn, pair):
    row = conn.execute("SELECT MIN(start) FROM candles_1m WHERE pair = ?", (pair,)).fetchone()
    return row[0] if row and row[0] else None


def get_row_count(conn, pair):
    row = conn.execute("SELECT COUNT(*) FROM candles_1m WHERE pair = ?", (pair,)).fetchone()
    return row[0] if row else 0


def download_and_parse_zip(symbol, year, month):
    """Download a monthly kline ZIP from Binance and return parsed rows."""
    url = f"{BINANCE_BASE}/{symbol}/1m/{symbol}-1m-{year}-{month:02d}.zip"
    try:
        req = Request(url, headers={'User-Agent': 'candle-backfill/1.0'})
        resp = urlopen(req, timeout=30)
        data = resp.read()
    except HTTPError as e:
        if e.code == 404:
            return None  # month not available
        raise

    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_name = zf.namelist()[0]
    with zf.open(csv_name) as f:
        reader = csv.reader(io.TextIOWrapper(f, encoding='utf-8'))
        rows = []
        for row in reader:
            # Binance CSV: open_time, open, high, low, close, volume, close_time, ...
            # Timestamp format changed: 13 digits = ms (pre-2025), 16 digits = us (2025+)
            raw_ts = int(row[0])
            if raw_ts > 1e15:      # microseconds (16 digits)
                ts = raw_ts // 1_000_000
            elif raw_ts > 1e12:    # milliseconds (13 digits)
                ts = raw_ts // 1_000
            else:                  # already seconds
                ts = raw_ts
            o = float(row[1])
            h = float(row[2])
            l = float(row[3])
            c = float(row[4])
            v = float(row[5])
            rows.append((ts, o, h, l, c, v))
    return rows


def backfill_pair(conn, cb_pair, binance_symbol):
    """Backfill a single pair from Binance bulk data."""
    count_before = get_row_count(conn, cb_pair)
    earliest = get_earliest_timestamp(conn, cb_pair)
    if earliest:
        earliest_dt = datetime.fromtimestamp(earliest, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')
        print(f"  {cb_pair}: Existing {count_before} rows, earliest {earliest_dt}")
    else:
        print(f"  {cb_pair}: No existing data")

    # Build list of months from target start to now
    start_dt = datetime.fromtimestamp(TARGET_START, tz=timezone.utc)
    now_dt = datetime.now(timezone.utc)

    months = []
    y, m = start_dt.year, start_dt.month
    while (y, m) <= (now_dt.year, now_dt.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m = 1
            y += 1

    print(f"  {cb_pair}: Checking {len(months)} monthly files for gaps...")

    total_inserted = 0
    for i, (year, month) in enumerate(months):
        sys.stdout.write(f"\r  {cb_pair}: [{i+1}/{len(months)}] {year}-{month:02d} ... ")
        sys.stdout.flush()

        rows = download_and_parse_zip(binance_symbol, year, month)
        if rows is None:
            sys.stdout.write("not found, skipping\n")
            continue

        # INSERT OR IGNORE handles deduplication — existing Coinbase data preserved
        batch = [(cb_pair, ts, o, h, l, c, v) for ts, o, h, l, c, v in rows]

        if batch:
            conn.executemany(
                "INSERT OR IGNORE INTO candles_1m (pair, start, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                batch
            )
            new_rows = conn.total_changes - (count_before + total_inserted)
            conn.commit()
            inserted_this_month = get_row_count(conn, cb_pair) - count_before - total_inserted
            total_inserted += inserted_this_month
            if inserted_this_month > 0:
                sys.stdout.write(f"+{inserted_this_month} new rows\n")
            else:
                sys.stdout.write("already complete\n")

    return total_inserted


def main():
    if '--confirm' not in sys.argv:
        print("=" * 70)
        print("WARNING: this splices Binance USDT-quoted candles into the Coinbase")
        print("USD-quoted series. USDT/USD basis (multi-percent during depegs) and")
        print("venue differences create artificial jumps at every seam — backtests")
        print("crossing spliced ranges will show fake volatility and fake fills.")
        print("Re-run with --confirm to proceed.")
        print("=" * 70)
        return
    print("=== Binance Backfill ===")
    print(f"DB: {DB_PATH}")
    print(f"Target start: {datetime.utcfromtimestamp(TARGET_START).strftime('%Y-%m-%d')}")
    print()

    # Acquire lock
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("ERROR: Could not acquire lock — another process is using the DB.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")

        for cb_pair, binance_sym in PAIRS.items():
            before = get_row_count(conn, cb_pair)
            inserted = backfill_pair(conn, cb_pair, binance_sym)
            after = get_row_count(conn, cb_pair)
            print(f"  {cb_pair}: {before} -> {after} rows (+{inserted} inserted)\n")

        conn.close()
        print("Done.")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


if __name__ == '__main__':
    main()
