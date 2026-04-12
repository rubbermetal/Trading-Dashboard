"""
Historical candle database — stores 1-minute candles in SQLite,
converts to any arbitrary timeframe on the fly via aggregation.
"""
import os
import time
import sqlite3
import pandas as pd
from shared import client
from logger import get_logger

log = get_logger('candle_db')

DB_PATH = os.path.join(os.path.dirname(__file__), 'candles.db')

DEFAULT_PAIRS = [
    'BTC-USD', 'ETH-USD', 'SOL-USD', 'XRP-USD', 'DOGE-USD',
    'ADA-USD', 'AVAX-USD', 'LINK-USD', 'LTC-USD', 'UNI-USD'
]

MAX_PER_REQUEST = 300
FETCH_SLEEP = 0.3


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    """Create the candles table if it doesn't exist."""
    conn = _get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS candles_1m (
            pair    TEXT    NOT NULL,
            start   INTEGER NOT NULL,
            open    REAL    NOT NULL,
            high    REAL    NOT NULL,
            low     REAL    NOT NULL,
            close   REAL    NOT NULL,
            volume  REAL    NOT NULL,
            PRIMARY KEY (pair, start)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_candles_pair_start ON candles_1m(pair, start)")
    conn.commit()
    conn.close()


def fetch_and_store_1m(pair, start_ts, end_ts, progress_cb=None):
    """
    Fetch 1m candles from Coinbase API and store in SQLite.
    Paginates backward in 300-candle chunks. Returns count of new rows inserted.
    """
    init_db()
    conn = _get_conn()
    total_inserted = 0
    cursor_end = end_ts
    total_est = max(1, (end_ts - start_ts) // 60)
    fetched = 0

    while cursor_end > start_ts:
        cursor_start = max(start_ts, cursor_end - (MAX_PER_REQUEST * 60))
        try:
            res = client.get(
                f"/api/v3/brokerage/products/{pair}/candles",
                params={
                    "start": str(cursor_start),
                    "end": str(cursor_end),
                    "granularity": "ONE_MINUTE"
                }
            )
            candles = res.get('candles', [])
            if not candles:
                break

            rows = []
            for c in candles:
                rows.append((
                    pair, int(c['start']),
                    float(c['open']), float(c['high']),
                    float(c['low']), float(c['close']),
                    float(c.get('volume', 0))
                ))

            conn.executemany(
                "INSERT OR IGNORE INTO candles_1m (pair, start, open, high, low, close, volume) VALUES (?,?,?,?,?,?,?)",
                rows
            )
            conn.commit()
            total_inserted += len(rows)
            fetched += len(candles)

            if progress_cb:
                pct = min(99, int(fetched / total_est * 100))
                progress_cb(pair, pct, fetched)

        except Exception as e:
            log.error(f"[{pair}] Fetch error at {cursor_start}-{cursor_end}: {e}")
            break

        cursor_end = cursor_start
        time.sleep(FETCH_SLEEP)

    conn.close()
    return total_inserted


def get_last_timestamp(pair):
    """Get the most recent 1m candle timestamp for a pair. Returns 0 if no data."""
    init_db()
    conn = _get_conn()
    row = conn.execute("SELECT MAX(start) FROM candles_1m WHERE pair = ?", (pair,)).fetchone()
    conn.close()
    return row[0] if row and row[0] else 0


def get_first_timestamp(pair):
    """Get the earliest 1m candle timestamp for a pair. Returns 0 if no data."""
    init_db()
    conn = _get_conn()
    row = conn.execute("SELECT MIN(start) FROM candles_1m WHERE pair = ?", (pair,)).fetchone()
    conn.close()
    return row[0] if row and row[0] else 0


def get_candle_count(pair):
    """Get total number of 1m candles stored for a pair."""
    init_db()
    conn = _get_conn()
    row = conn.execute("SELECT COUNT(*) FROM candles_1m WHERE pair = ?", (pair,)).fetchone()
    conn.close()
    return row[0] if row else 0


def list_pairs():
    """List all pairs that have data in the database."""
    init_db()
    conn = _get_conn()
    rows = conn.execute("SELECT DISTINCT pair FROM candles_1m ORDER BY pair").fetchall()
    conn.close()
    return [r[0] for r in rows]


def query(pair, tf_minutes, start_ts, end_ts):
    """
    Query candles for any arbitrary timeframe (in minutes).
    Reads 1m data from SQLite and aggregates on the fly.
    Returns a pandas DataFrame with columns: start, open, high, low, close, volume
    """
    init_db()
    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT start, open, high, low, close, volume FROM candles_1m WHERE pair = ? AND start >= ? AND start <= ? ORDER BY start",
        conn, params=(pair, start_ts, end_ts)
    )
    conn.close()

    if df.empty:
        return df

    if tf_minutes <= 1:
        return df

    # Aggregate to requested timeframe
    period = tf_minutes * 60
    df['tf_group'] = (df['start'] // period) * period

    result = df.groupby('tf_group').agg(
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum')
    ).reset_index()
    result.rename(columns={'tf_group': 'start'}, inplace=True)

    # Drop incomplete candles at the edges
    expected_bars = tf_minutes
    bar_counts = df.groupby('tf_group').size()
    complete_groups = bar_counts[bar_counts >= expected_bars].index
    result = result[result['start'].isin(complete_groups)]

    return result.reset_index(drop=True)


def get_chart_candles(pair, tf_minutes, limit):
    """
    Return the most recent `limit` bars at `tf_minutes` timeframe from the 1m store.
    Keeps the in-progress (incomplete) tail bar so the chart always shows the live candle.
    Returns a pandas DataFrame with columns: start, open, high, low, close, volume.
    Empty DataFrame if the pair has no data.
    """
    init_db()
    last_ts = get_last_timestamp(pair)
    if last_ts == 0:
        return pd.DataFrame(columns=['start', 'open', 'high', 'low', 'close', 'volume'])

    end_ts = last_ts
    # Pull a generous window of 1m rows, then aggregate + slice
    lookback_seconds = max(limit, 100) * tf_minutes * 60 + tf_minutes * 60
    start_ts = end_ts - lookback_seconds

    conn = _get_conn()
    df = pd.read_sql_query(
        "SELECT start, open, high, low, close, volume FROM candles_1m "
        "WHERE pair = ? AND start >= ? AND start <= ? ORDER BY start",
        conn, params=(pair, start_ts, end_ts)
    )
    conn.close()

    if df.empty:
        return df

    if tf_minutes <= 1:
        return df.tail(limit).reset_index(drop=True)

    period = tf_minutes * 60
    df['tf_group'] = (df['start'] // period) * period
    result = df.groupby('tf_group').agg(
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum')
    ).reset_index()
    result.rename(columns={'tf_group': 'start'}, inplace=True)
    return result.tail(limit).reset_index(drop=True)


def get_backtest_candles(pair, tf_minutes, start_ts, end_ts, progress_cb=None):
    """
    Get candles for backtesting — local DB first, auto-seed from Coinbase if needed.

    1. Check if pair has data in DB covering the requested range
    2. Fill backward/forward gaps via Coinbase API fetch_and_store_1m()
    3. If pair is entirely new, seed it (it will then be picked up by cron updater)
    4. Return aggregated DataFrame at requested timeframe
    """
    first_ts = get_first_timestamp(pair)
    last_ts = get_last_timestamp(pair)

    if first_ts == 0:
        # No data at all — full seed
        if progress_cb:
            progress_cb('Seeding candles (new pair)...', 0)
        log.info(f"[{pair}] No data in DB — seeding from Coinbase")
        fetch_and_store_1m(pair, start_ts, end_ts, progress_cb=progress_cb)
    else:
        # Fill backward gap
        if start_ts < first_ts - 60:
            if progress_cb:
                progress_cb('Fetching older candles...', 0)
            log.info(f"[{pair}] Backfilling gap: {start_ts} to {first_ts}")
            fetch_and_store_1m(pair, start_ts, first_ts, progress_cb=progress_cb)

        # Fill forward gap
        if end_ts > last_ts + 60:
            if progress_cb:
                progress_cb('Fetching recent candles...', 50)
            log.info(f"[{pair}] Forward-filling gap: {last_ts} to {end_ts}")
            fetch_and_store_1m(pair, last_ts, end_ts, progress_cb=progress_cb)

    if progress_cb:
        progress_cb('Aggregating candles...', 90)

    return query(pair, tf_minutes, start_ts, end_ts)


def get_db_stats():
    """Return summary stats for all pairs in the database."""
    init_db()
    conn = _get_conn()
    rows = conn.execute("""
        SELECT pair, COUNT(*) as count, MIN(start) as earliest, MAX(start) as latest
        FROM candles_1m GROUP BY pair ORDER BY pair
    """).fetchall()
    conn.close()

    stats = []
    for pair, count, earliest, latest in rows:
        days = (latest - earliest) / 86400 if latest and earliest else 0
        stats.append({
            'pair': pair,
            'candles': count,
            'earliest': earliest,
            'latest': latest,
            'days': round(days, 1)
        })
    return stats
