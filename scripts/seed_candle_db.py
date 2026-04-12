#!/usr/bin/env python3
"""
Seed the candle database with historical 1-minute data.
Run once: /home/pi/dashboard/venv/bin/python3 /home/pi/dashboard/scripts/seed_candle_db.py

Fetches up to 2 years of 1m candles for each configured pair.
Takes ~9 minutes per pair per year. Run overnight for full history.

Options:
    --pairs BTC-USD,ETH-USD    Override default pair list
    --days 365                  How many days back to fetch (default 730 = 2 years)
    --pair BTC-USD              Seed a single pair
"""
import os
import sys
import time
import fcntl
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from candle_db import init_db, fetch_and_store_1m, get_last_timestamp, get_first_timestamp, get_candle_count, DEFAULT_PAIRS

LOCK_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'candles.lock')


def progress(pair, pct, fetched):
    print(f"\r  {pair}: {pct}% ({fetched:,} candles fetched)", end='', flush=True)


def seed_pair(pair, days_back):
    end_ts = int(time.time())
    target_start = end_ts - (days_back * 86400)
    t0 = time.time()
    total_added = 0

    first = get_first_timestamp(pair)
    last = get_last_timestamp(pair)

    # Fill BACKWARD: if earliest data doesn't reach target start
    if first > 0 and first > target_start + 60:
        back_days = (first - target_start) / 86400
        print(f"  {pair}: Filling backward {back_days:.1f} days...")
        count = fetch_and_store_1m(pair, target_start, first - 60, progress_cb=progress)
        total_added += count
        print()

    # Fill FORWARD: if latest data doesn't reach now
    if last > 0:
        gap = (end_ts - last) / 86400
        if gap > 0.01:  # more than ~15 min gap
            print(f"  {pair}: Filling forward {gap:.1f} days...")
            count = fetch_and_store_1m(pair, last + 60, end_ts, progress_cb=progress)
            total_added += count
            print()
    elif last == 0:
        # No data at all — full fetch
        print(f"  {pair}: Fetching {days_back} days of 1m data...")
        count = fetch_and_store_1m(pair, target_start, end_ts, progress_cb=progress)
        total_added += count
        print()

    elapsed = time.time() - t0
    total = get_candle_count(pair)
    print(f"  {pair}: Done. +{total_added:,} new candles in {elapsed:.0f}s. Total: {total:,}")


def main():
    parser = argparse.ArgumentParser(description='Seed candle database with historical 1m data')
    parser.add_argument('--pairs', type=str, help='Comma-separated pair list')
    parser.add_argument('--pair', type=str, help='Single pair to seed')
    parser.add_argument('--days', type=int, default=730, help='Days of history (default 730)')
    args = parser.parse_args()

    if args.pair:
        pairs = [args.pair.upper()]
    elif args.pairs:
        pairs = [p.strip().upper() for p in args.pairs.split(',')]
    else:
        pairs = DEFAULT_PAIRS

    # File lock — prevents cron updates from running simultaneously
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("ERROR: Another seed or update is already running. Exiting.")
        sys.exit(1)

    try:
        init_db()
        print(f"Seeding {len(pairs)} pairs, {args.days} days back")
        print(f"Estimated time: ~{len(pairs) * args.days / 365 * 8.8:.0f} minutes")
        print()

        for i, pair in enumerate(pairs):
            print(f"[{i+1}/{len(pairs)}] {pair}")
            try:
                seed_pair(pair, args.days)
            except Exception as e:
                print(f"  ERROR: {e}")
            print()

        print("Seed complete.")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


if __name__ == '__main__':
    main()
