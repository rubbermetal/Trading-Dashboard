#!/usr/bin/env python3
"""
Incremental candle database update — fetches new 1m candles since last stored.
Run via cron every 15 minutes:
    */15 * * * * /home/pi/dashboard/venv/bin/python3 /home/pi/dashboard/scripts/update_candle_db.py >> /home/pi/dashboard/logs/candle_update.log 2>&1

Handles:
- Pairs not yet seeded (skips them)
- Gaps of any size (fetches everything from last stored to now)
- Concurrent access (lock file prevents overlap with seed script)
"""
import os
import sys
import time
import fcntl
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from candle_db import init_db, fetch_and_store_1m, get_last_timestamp, list_pairs, DEFAULT_PAIRS

LOCK_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'candles.lock')


def main():
    # File lock — prevents running simultaneously with seed script or another update
    lock_fd = open(LOCK_FILE, 'w')
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(f"[{datetime.now():%Y-%m-%d %H:%M}] Skipped — seed or another update is running")
        return

    try:
        init_db()
        end_ts = int(time.time())
        total_new = 0
        ts = datetime.now().strftime('%Y-%m-%d %H:%M')

        # Use all pairs in DB (includes any auto-seeded by backtests)
        pairs = list_pairs() or DEFAULT_PAIRS
        for pair in pairs:
            last = get_last_timestamp(pair)
            if last == 0:
                continue  # not seeded yet, skip

            start_ts = last + 60
            gap_minutes = (end_ts - start_ts) // 60
            if gap_minutes < 1:
                continue  # already up to date

            try:
                count = fetch_and_store_1m(pair, start_ts, end_ts)
                if count > 0:
                    total_new += count
                    if gap_minutes > 30:
                        print(f"[{ts}] {pair}: filled {gap_minutes}min gap, +{count} candles")
            except Exception as e:
                print(f"[{ts}] {pair}: ERROR {e}")

        if total_new > 0:
            print(f"[{ts}] Updated: +{total_new} candles across {len(pairs)} pairs")
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


if __name__ == '__main__':
    main()
