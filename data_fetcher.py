"""
Paginated Coinbase candle fetcher with local CSV cache.
Fetches historical OHLCV data in 300-candle chunks and caches to CSV
to avoid re-fetching on repeated backtests.
"""
import os
import time
import pandas as pd
from shared import client
from bot_utils import TF_MAP
from logger import get_logger

log = get_logger('data_fetcher')

CACHE_DIR = os.path.join(os.path.dirname(__file__), 'cache', 'candles')
MAX_CANDLES_PER_REQUEST = 300


def _cache_path(pair, tf_key):
    return os.path.join(CACHE_DIR, f"{pair}_{tf_key}.csv")


def _load_cache(pair, tf_key):
    path = _cache_path(pair, tf_key)
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_csv(path)
        if df.empty:
            return None
        df['start'] = df['start'].astype(int)
        return df.sort_values('start').reset_index(drop=True)
    except Exception as e:
        log.warning(f"Cache read error for {pair}_{tf_key}: {e}")
        return None


def _save_cache(pair, tf_key, df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = _cache_path(pair, tf_key)
    df = df.drop_duplicates(subset='start').sort_values('start').reset_index(drop=True)
    df.to_csv(path, index=False)


def _fetch_range(pair, cb_gran, tf_sec, start_ts, end_ts, progress_cb=None):
    """Fetch candles from Coinbase API, paginating backward in 300-candle chunks."""
    all_candles = []
    cursor_end = end_ts
    total_bars_est = max(1, (end_ts - start_ts) // tf_sec)
    fetched = 0

    while cursor_end > start_ts:
        cursor_start = max(start_ts, cursor_end - (MAX_CANDLES_PER_REQUEST * tf_sec))
        try:
            res = client.get(
                f"/api/v3/brokerage/products/{pair}/candles",
                params={
                    "start": str(cursor_start),
                    "end": str(cursor_end),
                    "granularity": cb_gran
                }
            )
            candles = res.get('candles', [])
            if not candles:
                break

            for c in candles:
                all_candles.append({
                    'start': int(c['start']),
                    'open': float(c['open']),
                    'high': float(c['high']),
                    'low': float(c['low']),
                    'close': float(c['close']),
                    'volume': float(c.get('volume', 0))
                })

            fetched += len(candles)
            if progress_cb:
                progress_cb('fetching', min(99, int(fetched / total_bars_est * 100)))

        except Exception as e:
            log.error(f"Fetch error for {pair} at {cursor_start}-{cursor_end}: {e}")
            break

        cursor_end = cursor_start
        time.sleep(0.3)

    return all_candles


def get_candles(pair, tf_key, start_ts, end_ts, progress_cb=None):
    """
    Get historical candles for a pair/timeframe range.
    Uses CSV cache, fetches missing ranges from Coinbase API.
    Returns a sorted DataFrame with columns: start, open, high, low, close, volume
    """
    if tf_key not in TF_MAP:
        raise ValueError(f"Unknown timeframe: {tf_key}")

    cb_gran, tf_sec = TF_MAP[tf_key]
    cached = _load_cache(pair, tf_key)

    if cached is not None and len(cached) > 0:
        cache_min = int(cached['start'].min())
        cache_max = int(cached['start'].max())

        need_before = start_ts < cache_min
        need_after = end_ts > cache_max + tf_sec

        new_candles = []
        if need_before:
            log.info(f"Fetching {pair} {tf_key} before cache: {start_ts} to {cache_min}")
            new_candles.extend(_fetch_range(pair, cb_gran, tf_sec, start_ts, cache_min, progress_cb))
        if need_after:
            log.info(f"Fetching {pair} {tf_key} after cache: {cache_max} to {end_ts}")
            new_candles.extend(_fetch_range(pair, cb_gran, tf_sec, cache_max, end_ts, progress_cb))

        if new_candles:
            new_df = pd.DataFrame(new_candles)
            combined = pd.concat([cached, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset='start').sort_values('start').reset_index(drop=True)
            _save_cache(pair, tf_key, combined)
        else:
            combined = cached
    else:
        log.info(f"No cache for {pair} {tf_key}. Fetching full range.")
        raw = _fetch_range(pair, cb_gran, tf_sec, start_ts, end_ts, progress_cb)
        if not raw:
            return pd.DataFrame(columns=['start', 'open', 'high', 'low', 'close', 'volume'])
        combined = pd.DataFrame(raw).drop_duplicates(subset='start').sort_values('start').reset_index(drop=True)
        _save_cache(pair, tf_key, combined)

    result = combined[(combined['start'] >= start_ts) & (combined['start'] <= end_ts)].copy()
    return result.sort_values('start').reset_index(drop=True)
