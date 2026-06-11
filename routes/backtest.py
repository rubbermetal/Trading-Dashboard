"""
Backtesting API — run strategy backtests with background execution.
"""
import os
import json
import time
import uuid
import threading
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request
from data_fetcher import get_candles
from backtest_engine import run_backtest
from bot_utils import TF_MAP, STRATEGY_DEFAULT_TF
from candle_db import get_backtest_candles, query as candle_db_query
from logger import get_logger

RESULTS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'cache', 'backtest_results')

log = get_logger('backtest_api')

backtest_bp = Blueprint('backtest', __name__)

# Background job storage (max 1 concurrent)
_BACKTEST_JOBS = {}
_BACKTEST_LOCK = threading.Lock()

COMMON_PAIRS = [
    'BTC-USD', 'ETH-USD', 'SOL-USD', 'XRP-USD', 'DOGE-USD',
    'ADA-USD', 'AVAX-USD', 'LINK-USD', 'DOT-USD', 'MATIC-USD',
    'LTC-USD', 'UNI-USD', 'NEAR-USD', 'ATOM-USD', 'APT-USD'
]

SUPPORTED_STRATEGIES = list(STRATEGY_DEFAULT_TF.keys())


def _cleanup_old_jobs():
    """Remove expired jobs and detect dead threads."""
    now = time.time()
    for jid in list(_BACKTEST_JOBS.keys()):
        j = _BACKTEST_JOBS[jid]
        # Detect dead threads — if status is running but thread is dead, mark as error
        thread = j.get('thread')
        if j['status'] in ('fetching', 'running'):
            if thread and not thread.is_alive():
                j['status'] = 'error'
                j['error'] = 'Thread died unexpectedly (service restart or crash)'
            # Timeout: mark RUNNING jobs failed after 20 minutes
            # (previously unreachable: an earlier >1h check popped the job first)
            elif now - j['created_at'] > 1200:
                j['status'] = 'error'
                j['error'] = 'Timed out after 20 minutes'
        # Evict finished/errored jobs after 1 hour
        if now - j['created_at'] > 3600 and j['status'] not in ('fetching', 'running'):
            _BACKTEST_JOBS.pop(jid, None)


def _run_job(job_id, pair, strategy, tf_key, tf_minutes, start_ts, end_ts, capital, params):
    """Background worker for a backtest job."""
    job = _BACKTEST_JOBS[job_id]

    def progress_cb(phase, pct):
        job['phase'] = phase
        job['progress'] = pct

    try:
        # Phase 1: Fetch data from local candle DB (auto-seeds from Coinbase if needed)
        job['status'] = 'fetching'
        job['phase'] = 'Loading candles from DB...'
        df = get_backtest_candles(pair, tf_minutes, start_ts, end_ts, progress_cb=progress_cb)

        if df.empty or len(df) < 220:
            job['status'] = 'error'
            job['error'] = f'Insufficient data: {len(df)} candles (need 220+)'
            return

        # Compute buy-and-hold for comparison
        first_close = float(df.iloc[210]['close'])
        last_close = float(df.iloc[-1]['close'])
        bh_return = ((last_close / first_close) - 1) * 100 if first_close > 0 else 0

        # Phase 2: Run backtest
        job['status'] = 'running'
        job['phase'] = 'Simulating...'
        job['progress'] = 0

        result = run_backtest(pair, strategy, tf_key, df, capital, params, progress_cb=progress_cb)

        if 'error' in result:
            job['status'] = 'error'
            job['error'] = result['error']
            return

        result['summary']['buy_hold_return_pct'] = round(bh_return, 2)
        job['status'] = 'complete'
        job['progress'] = 100
        job['result'] = result
        log.info(f"Backtest {job_id} complete: {strategy} on {pair} — {result['summary']['total_trades']} trades, {result['summary']['total_return_pct']}% return")

        # Persist to disk so results survive page refresh / service restart
        try:
            os.makedirs(RESULTS_DIR, exist_ok=True)
            with open(os.path.join(RESULTS_DIR, f'{job_id}.json'), 'w') as f:
                json.dump(result, f)
            # Also save as 'latest.json' for quick retrieval
            with open(os.path.join(RESULTS_DIR, 'latest.json'), 'w') as f:
                json.dump(result, f)
        except Exception as e:
            log.warning(f"Failed to persist backtest result: {e}")

    except Exception as e:
        log.error(f"Backtest {job_id} failed: {e}")
        job['status'] = 'error'
        job['error'] = str(e)


@backtest_bp.route('/api/backtest/run', methods=['POST'])
def start_backtest():
    with _BACKTEST_LOCK:
        _cleanup_old_jobs()

        # Check for actually-alive running job (cleanup catches dead threads).
        # Held under the lock so two simultaneous POSTs can't both pass.
        running = [j for j in _BACKTEST_JOBS.values()
                   if j['status'] in ('queued', 'fetching', 'running')
                   and (j.get('thread') is None or j['thread'].is_alive())]
        if running:
            elapsed = int(time.time() - running[0]['created_at'])
            return jsonify(success=False, error=f'A backtest is running ({elapsed}s elapsed, {running[0]["progress"]}% done). Wait for it to complete.'), 429

        # Reserve the slot immediately; filled in below after validation
        _slot_id = f"bt_{uuid.uuid4().hex[:8]}"
        _BACKTEST_JOBS[_slot_id] = {
            'status': 'queued', 'phase': 'Validating...', 'progress': 0,
            'result': None, 'error': None, 'created_at': time.time(), 'thread': None
        }

    data = request.get_json()
    pair = data.get('pair', '').upper()
    strategy = data.get('strategy', '').upper()
    tf_key = data.get('timeframe', STRATEGY_DEFAULT_TF.get(strategy, '15m'))
    start_date = data.get('start_date', '')
    end_date = data.get('end_date', '')
    capital = float(data.get('capital', 1000))
    params = data.get('params', {})

    if not pair or '-' not in pair:
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error='Invalid pair'), 400
    if strategy not in SUPPORTED_STRATEGIES:
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error=f'Unsupported strategy: {strategy}'), 400
    # Parse timeframe: accept TF_MAP keys ("15m") or raw minutes ("7m", "120m")
    try:
        tf_minutes = int(tf_key.rstrip('mM'))
    except (ValueError, AttributeError):
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error=f'Invalid timeframe: {tf_key}'), 400
    if tf_minutes < 1 or tf_minutes > 1440:
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error=f'Timeframe must be 1-1440 minutes, got {tf_minutes}'), 400
    if capital < 10:
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error='Minimum capital is $10'), 400

    try:
        start_ts = int(datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp())
        end_ts = int(datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=timezone.utc).timestamp()) + 86399
    except (ValueError, TypeError):
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error='Invalid date format. Use YYYY-MM-DD.'), 400

    if end_ts <= start_ts:
        _BACKTEST_JOBS.pop(_slot_id, None)
        return jsonify(success=False, error='End date must be after start date'), 400

    job_id = _slot_id
    _BACKTEST_JOBS[job_id]['phase'] = 'Starting...'

    t = threading.Thread(target=_run_job, args=(job_id, pair, strategy, tf_key, tf_minutes, start_ts, end_ts, capital, params), daemon=True)
    _BACKTEST_JOBS[job_id]['thread'] = t
    t.start()

    return jsonify(success=True, data={'job_id': job_id})


@backtest_bp.route('/api/backtest/status/<job_id>')
def backtest_status(job_id):
    job = _BACKTEST_JOBS.get(job_id)
    if not job:
        return jsonify(success=False, error='Job not found'), 404
    return jsonify(success=True, data={
        'status': job['status'],
        'phase': job['phase'],
        'progress': job['progress'],
        'error': job.get('error')
    })


@backtest_bp.route('/api/backtest/result/<job_id>')
def backtest_result(job_id):
    job = _BACKTEST_JOBS.get(job_id)
    if job:
        if job['status'] != 'complete':
            return jsonify(success=False, error=f'Job status: {job["status"]}'), 400
        return jsonify(success=True, data=job['result'])
    # Try loading from disk
    path = os.path.join(RESULTS_DIR, f'{job_id}.json')
    if os.path.exists(path):
        with open(path) as f:
            return jsonify(success=True, data=json.load(f))
    return jsonify(success=False, error='Job not found'), 404


@backtest_bp.route('/api/backtest/latest')
def backtest_latest():
    """Return the most recent completed backtest result (persisted to disk)."""
    path = os.path.join(RESULTS_DIR, 'latest.json')
    if not os.path.exists(path):
        return jsonify(success=False, error='No completed backtests yet')
    try:
        with open(path) as f:
            return jsonify(success=True, data=json.load(f))
    except Exception as e:
        return jsonify(success=False, error=str(e))


@backtest_bp.route('/api/backtest/candles')
def backtest_candles():
    """Return candles from local DB for trade chart visualization."""
    pair = request.args.get('pair', '')
    tf = int(request.args.get('tf', 5))
    start_ts = int(request.args.get('start', 0))
    end_ts = int(request.args.get('end', 0))

    if not pair or not start_ts or not end_ts:
        return jsonify(success=False, error='Missing pair, start, or end'), 400
    if tf < 1 or tf > 1440:
        return jsonify(success=False, error='tf must be 1-1440'), 400

    try:
        df = candle_db_query(pair, tf, start_ts, end_ts)
        if df.empty:
            return jsonify(success=True, data=[])
        candles = df[['start', 'open', 'high', 'low', 'close', 'volume']].rename(
            columns={'start': 'time'}
        ).to_dict(orient='records')
        return jsonify(success=True, data=candles)
    except Exception as e:
        return jsonify(success=False, error=str(e)), 500
