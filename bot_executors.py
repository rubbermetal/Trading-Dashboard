import time
import uuid
import pandas as pd
from shared import client, ACTIVE_BOTS
from notifier import notify_bot_entry, notify_bot_exit, notify_drawdown
from strategies import (calculate_quad_rotation, calculate_orb,
    calculate_trap, calculate_momentum, calculate_dca, calculate_npr, NPR_CONFIG, _compute_zone,
    calculate_vwap_mr, calculate_squeeze)
from bot_utils import (
    get_bot_tf, is_derivative, get_contract_multiplier,
    snap_to_increment, record_trade, save_bots,
    extract_fee, poll_market_fill
)
from logger import get_logger

log = get_logger('bot_engine')


def paper_fill_buy(bot, pair, qty, price, mult):
    """Simulate a buy fill for paper trading bots."""
    filled_size = float(qty)
    fill_px = float(price)
    old_held = bot.get('asset_held', 0)
    old_cost = bot.get('total_cost', 0)
    new_cost = fill_px * filled_size * mult
    total_held = old_held + filled_size
    new_avg = (old_cost + new_cost) / (total_held * mult) if total_held > 0 else fill_px

    sim_fee = new_cost * 0.004  # simulate maker fee
    bot['asset_held'] = total_held
    bot['total_cost'] = old_cost + new_cost
    bot['avg_entry'] = new_avg
    bot['entry_price'] = new_avg
    bot['current_usd'] -= (new_cost + sim_fee)
    bot['position_side'] = 'LONG'
    bot['total_buys'] = bot.get('total_buys', 0) + 1
    bot['buy_count_this_cycle'] = bot.get('buy_count_this_cycle', 0) + 1
    bot['dca_state'] = 'ACCUMULATING'
    bot['last_cross_direction'] = 'BELOW'
    bot.pop('pending_buy_oid', None)
    bot.pop('pending_buy_time', None)
    bot.pop('buy_retries', None)
    save_bots()
    log.info(f"[{pair}] PAPER BUY: {filled_size:.8f} at ${fill_px:.2f}. Avg entry ${new_avg:.2f}")
    notify_bot_entry(pair, 'DCA (PAPER)', fill_px, filled_size)


def paper_fill_sell(bot, pair, tier_pct, qty, price, mult):
    """Simulate a sell fill for paper trading bots."""
    filled_size = float(qty)
    fill_px = float(price)
    sim_fee = filled_size * fill_px * mult * 0.004

    bot['asset_held'] = max(0, bot.get('asset_held', 0) - filled_size)
    old_held = bot.get('asset_held', 0) + filled_size
    if old_held > 0:
        sold_fraction = filled_size / old_held
        bot['total_cost'] = max(0, bot.get('total_cost', 0) * (1 - sold_fraction))
    gross = filled_size * fill_px * mult
    bot['current_usd'] += gross - sim_fee
    bot['highest_tier_sold'] = max(bot.get('highest_tier_sold', 0), tier_pct)
    save_bots()
    pnl = (fill_px - bot.get('avg_entry', fill_px)) * filled_size * mult
    log.info(f"[{pair}] PAPER SELL tier {tier_pct}%: {filled_size:.8f} at ${fill_px:.2f} PnL ${pnl:.4f}")
    notify_bot_exit(pair, 'DCA (PAPER)', fill_px, pnl, f'Tier {tier_pct}%')
    # Don't call record_trade — paper trades must not pollute permanent stats


# ==========================================
# STRATEGY EXECUTORS (NON-GRID)
# ==========================================

def execute_orb(bot_id, bot, pair):
    """ORB: Opening Range Breakout with ATR stops, R-multiple TP, risk-based sizing."""
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (288 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
    except Exception as e:
        log.error(f"[{pair}] Candle fetch error: {e}")
        return
    if len(candles) < 50: return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    pos_side = bot.get('position_side', 'FLAT')
    entry_price = bot.get('entry_price', 0.0)
    orb_data = bot.get('orb_data', None)
    tp_stage = bot.get('tp_stage', 0)
    settings = bot.get('settings', {})
    current_px = float(df.iloc[-1]['close'])

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    signal, reason, orb_meta = calculate_orb(
        df, pos_side, entry_price, orb_data, tp_stage,
        range_start_hour=settings.get('orb_start_hour', 14),
        range_duration_min=settings.get('orb_duration_min', 60),
        expiry_hours=settings.get('orb_expiry_hours', 8)
    )

    # --- ATR trailing stop (after T1, 1.5x ATR from HWM/LWM) ---
    if pos_side == 'LONG' and tp_stage >= 1 and orb_data:
        atr = orb_data.get('atr', current_px * 0.01)
        hwm = bot.get('high_water_mark', current_px)
        if current_px > hwm:
            bot['high_water_mark'] = current_px
            hwm = current_px
            save_bots()
        trail_stop = hwm - (1.5 * atr)
        if current_px <= trail_stop:
            signal = 'EXIT_LONG'
            reason = f"ATR TRAIL: {current_px:.2f} <= HWM {hwm:.2f} - 1.5*ATR"
    elif pos_side == 'SHORT' and tp_stage >= 1 and orb_data:
        atr = orb_data.get('atr', current_px * 0.01)
        lwm = bot.get('low_water_mark', current_px)
        if current_px < lwm:
            bot['low_water_mark'] = current_px
            lwm = current_px
            save_bots()
        trail_stop = lwm + (1.5 * atr)
        if current_px >= trail_stop:
            signal = 'EXIT_SHORT'
            reason = f"ATR TRAIL: {current_px:.2f} >= LWM {lwm:.2f} + 1.5*ATR"

    log.debug(f"[{pair}] ORB {signal}: {reason}")

    # --- LONG ENTRY (risk-based sizing: 2% of capital / stop_distance) ---
    if signal == 'LONG' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        stop_dist = orb_meta.get('stop_distance', current_px * 0.015)
        risk_usd = bot['current_usd'] * 0.02
        alloc = (risk_usd / stop_dist) * current_px if stop_dist > 0 else bot['current_usd'] * 0.10
        alloc = min(alloc, bot['current_usd'] * 0.50)

        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1: return
        else:
            qty = round((alloc * 0.99) / current_px, 6)

        try:
            oid = str(uuid.uuid4())
            if bot.get('paper'):
                log.info(f"[{pair}] PAPER ORB BUY: {qty} at ${current_px:.4f}")
            elif deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(alloc * 0.99, 2)))

            bot['asset_held'] = qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'LONG'
            bot['entry_price'] = current_px
            bot['orb_data'] = orb_meta
            bot['tp_stage'] = 0
            bot['high_water_mark'] = current_px
            save_bots()
            log.info(f"[{pair}] ORB LONG: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Order failed: {e}")

    elif signal == 'SHORT' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        if not deriv_flag:
            log.warning(f"[{pair}] Cannot short spot pair.")
            return

        stop_dist = orb_meta.get('stop_distance', current_px * 0.015)
        risk_usd = bot['current_usd'] * 0.02
        alloc = (risk_usd / stop_dist) * current_px if stop_dist > 0 else bot['current_usd'] * 0.10
        alloc = min(alloc, bot['current_usd'] * 0.50)

        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1: return

        try:
            oid = str(uuid.uuid4())
            if bot.get('paper'):
                log.info(f"[{pair}] PAPER ORB SELL (SHORT entry): {qty} at ${current_px:.4f}")
            else:
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty))

            bot['asset_held'] = -qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'SHORT'
            bot['entry_price'] = current_px
            bot['orb_data'] = orb_meta
            bot['tp_stage'] = 0
            bot['low_water_mark'] = current_px
            save_bots()
            log.info(f"[{pair}] ORB SHORT: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Order failed: {e}")

    # --- PARTIAL EXIT (T1: sell 50%, stop -> breakeven) ---
    elif signal == 'PARTIAL_EXIT_LONG' and pos_side == 'LONG' and bot['asset_held'] > 0:
        try:
            held = abs(bot['asset_held'])
            sell_qty = round(held * 0.5, 6) if not deriv_flag else max(1, int(held * 0.5))
            if bot.get('paper'):
                actual_exit = current_px
                actual_fee = current_px * sell_qty * mult * 0.004
                log.info(f"[{pair}] PAPER ORB PARTIAL EXIT LONG: {sell_qty} at ${current_px:.4f}")
            else:
                oid = str(uuid.uuid4())
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(sell_qty))
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else current_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * sell_qty * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, sell_qty, 'LONG', 'TARGET_1', pair, mult, actual_fee=actual_fee)

            profit = (actual_exit - bot['entry_price']) * sell_qty * mult
            bot['current_usd'] += profit - actual_fee
            bot['asset_held'] -= sell_qty
            bot['tp_stage'] = 1
            bot['high_water_mark'] = current_px
            save_bots()
            log.info(f"[{pair}] ORB PARTIAL EXIT LONG (T1): Sold 50%, stop->BE. {reason}")
        except Exception as e:
            log.error(f"[{pair}] Partial exit failed: {e}")

    elif signal == 'PARTIAL_EXIT_SHORT' and pos_side == 'SHORT' and bot['asset_held'] < 0:
        try:
            held = abs(bot['asset_held'])
            cover_qty = max(1, int(held * 0.5))
            if bot.get('paper'):
                actual_exit = current_px
                actual_fee = current_px * cover_qty * mult * 0.004
                log.info(f"[{pair}] PAPER ORB PARTIAL EXIT SHORT: {cover_qty} at ${current_px:.4f}")
            else:
                oid = str(uuid.uuid4())
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(cover_qty))
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else current_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * cover_qty * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, cover_qty, 'SHORT', 'TARGET_1', pair, mult, actual_fee=actual_fee)

            profit = (bot['entry_price'] - actual_exit) * cover_qty * mult
            bot['current_usd'] += profit - actual_fee
            bot['asset_held'] += cover_qty
            bot['tp_stage'] = 1
            bot['low_water_mark'] = current_px
            save_bots()
            log.info(f"[{pair}] ORB PARTIAL EXIT SHORT (T1): Covered 50%, stop->BE. {reason}")
        except Exception as e:
            log.error(f"[{pair}] Partial exit failed: {e}")

    # --- FULL EXITS (SL, T2, trail, timeout) ---
    elif signal == 'EXIT_LONG' and pos_side == 'LONG' and bot['asset_held'] > 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else ('TRAILING_STOP' if 'TRAIL' in reason.upper() else 'SIGNAL')
        try:
            held = abs(bot['asset_held'])
            if bot.get('paper'):
                actual_exit = current_px
                actual_fee = current_px * held * mult * 0.004
                log.info(f"[{pair}] PAPER ORB EXIT LONG ({exit_reason}): {held} at ${current_px:.4f}")
            else:
                oid = str(uuid.uuid4())
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(held))
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else current_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (actual_exit - bot['entry_price']) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot.pop('orb_data', None)
            bot.pop('tp_stage', None)
            bot.pop('high_water_mark', None)
            bot.pop('trail_active', None)
            save_bots()
            log.info(f"[{pair}] ORB EXIT LONG: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Exit failed: {e}")

    elif signal == 'EXIT_SHORT' and pos_side == 'SHORT' and bot['asset_held'] < 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else ('TRAILING_STOP' if 'TRAIL' in reason.upper() else 'SIGNAL')
        try:
            held = abs(bot['asset_held'])
            if bot.get('paper'):
                actual_exit = current_px
                actual_fee = current_px * held * mult * 0.004
                log.info(f"[{pair}] PAPER ORB EXIT SHORT ({exit_reason}): {held} at ${current_px:.4f}")
            else:
                oid = str(uuid.uuid4())
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(held))
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else current_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['entry_price'], actual_exit, held, 'SHORT', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (bot['entry_price'] - actual_exit) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot.pop('orb_data', None)
            bot.pop('tp_stage', None)
            bot.pop('low_water_mark', None)
            bot.pop('trail_active', None)
            save_bots()
            log.info(f"[{pair}] ORB EXIT SHORT: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Exit failed: {e}")

def _quad_exit(bot, pair, current_px, mult, exit_reason, reason):
    """Shared exit logic for QUAD Standard — sells full position, records trade, cleans state."""
    held = bot.get('asset_held', 0)
    if held <= 0:
        return
    try:
        if bot.get('paper'):
            actual_exit = current_px
            actual_fee = current_px * held * mult * 0.004
            log.info(f"[{pair}] QUAD PAPER SELL: {held} at ${current_px:.2f} ({exit_reason})")
        else:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(held))
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

        entry_px = bot.get('entry_price', current_px)
        record_trade(bot, entry_px, actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

        profit = (actual_exit - entry_px) * held * mult
        bot['current_usd'] += profit - actual_fee + (entry_px * held * mult)
        bot['asset_held'] = 0.0
        bot['position_side'] = 'FLAT'
        for key in ['entry_atr', 'entry_signal', 'stop_price', 'target_price', 'high_water_mark']:
            bot.pop(key, None)
        save_bots()
        log.info(f"[{pair}] QUAD exit ({exit_reason}): {reason}. PnL={profit:.2f}")
    except Exception as e:
        log.error(f"[{pair}] QUAD exit order failed: {e}")


# SL/TP ATR multipliers by signal confidence tier (5-tier DTRS)
_QUAD_SL_MULT = {'STRICT_PULLBACK': 2.5, 'SUPER_SIGNAL': 2.5, 'HOLY_GRAIL': 2.0, 'SEQ_ROTATION': 2.0, 'KD_CROSS': 1.5}
_QUAD_TP_MULT = {'STRICT_PULLBACK': 4.0, 'SUPER_SIGNAL': 3.5, 'HOLY_GRAIL': 3.0, 'SEQ_ROTATION': 3.0, 'KD_CROSS': 2.0}


def execute_quad(bot_id, bot, pair):
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (250 * tf_sec)
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
    except Exception as e:
        return
    if len(candles) < 200:
        return
    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']), 'low': float(c['low']), 'close': float(c['close'])} for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    current_px = float(df.iloc[-1]['close'])
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    # ══════════════════════════════════════════════════════════
    # DTRS Quad Rotation — unified 5-tier confidence sizing + SL/TP
    # ══════════════════════════════════════════════════════════
    rot_win = bot.get('settings', {}).get('rotation_window', 20)
    signal, reason, meta = calculate_quad_rotation(df, rotation_window=rot_win)
    pos_side = bot.get('position_side', 'FLAT')

    # ── LONG: evaluate exit priority chain ──
    if pos_side == 'LONG' and bot.get('asset_held', 0) > 0:
        hwm = bot.get('high_water_mark', current_px)
        if current_px > hwm:
            bot['high_water_mark'] = current_px
            save_bots()

        exit_reason = None
        exit_msg = reason

        # Priority 1: Hard stop loss (ATR-based, set at entry)
        stop_px = bot.get('stop_price', 0)
        if stop_px > 0 and current_px <= stop_px:
            exit_reason = 'STOP_LOSS'
            exit_msg = f"Stop Loss: price {current_px:.2f} <= stop {stop_px:.2f} ({bot.get('entry_signal', '?')} ATR-based)"

        # Priority 2: Counter-trend danger (dynamic signal exit)
        elif signal == 'SELL' and meta.get('exit_type') == 'COUNTER_TREND':
            exit_reason = 'SIGNAL'
            exit_msg = reason

        # Priority 3: Hard take profit (ATR-based, set at entry)
        elif current_px >= bot.get('target_price', float('inf')):
            exit_reason = 'TAKE_PROFIT'
            exit_msg = f"Take Profit: price {current_px:.2f} >= target {bot['target_price']:.2f} ({bot.get('entry_signal', '?')} ATR-based)"

        # Priority 4-5: Sequential bear rotation / Bear K/D cross
        elif signal == 'SELL':
            exit_reason = 'SIGNAL'
            exit_msg = reason

        if exit_reason:
            _quad_exit(bot, pair, current_px, mult, exit_reason, exit_msg)
        return

    # ── FLAT: evaluate entry with confidence sizing ──
    if signal == 'BUY' and bot.get('current_usd', 0) > 5.0:
        confidence = meta.get('confidence', 0.5)
        atr = meta.get('atr', current_px * 0.01)
        signal_type = meta.get('signal_type', 'UNKNOWN')

        allocation_usd = bot['current_usd'] * confidence * 0.99
        if allocation_usd < 5.0:
            return

        if deriv_flag:
            qty = int(allocation_usd / (current_px * mult))
            if qty < 1:
                return
        else:
            qty = round(allocation_usd / current_px, 6)

        try:
            if bot.get('paper'):
                log.info(f"[{pair}] QUAD PAPER BUY: {qty} at ${current_px:.2f} ({signal_type}, conf={confidence:.0%})")
            else:
                oid = str(uuid.uuid4())
                if deriv_flag:
                    client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
                else:
                    client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(allocation_usd))

            sl_mult = _QUAD_SL_MULT.get(signal_type, 2.0)
            tp_mult = _QUAD_TP_MULT.get(signal_type, 3.0)

            bot['asset_held'] += qty
            bot['current_usd'] -= allocation_usd
            bot['entry_price'] = current_px
            bot['position_side'] = 'LONG'
            bot['entry_atr'] = atr
            bot['entry_signal'] = signal_type
            bot['stop_price'] = round(current_px - (sl_mult * atr), 6)
            bot['target_price'] = round(current_px + (tp_mult * atr), 6)
            bot['high_water_mark'] = current_px
            save_bots()
            log.info(f"[{pair}] QUAD {signal_type} entry at {current_px:.2f}. Conf={confidence:.0%} Qty={qty}. SL={bot['stop_price']:.2f} TP={bot['target_price']:.2f}")
        except Exception as e:
            log.error(f"[{pair}] QUAD order failed: {e}")

def execute_trap(bot_id, bot, pair):
    """
    TRAP: Oliver Velez Elephant Bar / Narrow-Band Breakout
    Three-stage pyramiding (10% x3), R-multiple tiered TP, 2x ATR / elephant bar stop.
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (250 * tf_sec)
    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
    except Exception as e:
        log.error(f"[{pair}] Candle fetch error: {e}")
        return
    if len(candles) < 210: return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    pos_side = bot.get('position_side', 'FLAT')
    entry_stage = bot.get('entry_stage', 0)
    avg_entry = bot.get('avg_entry', 0.0)
    breakout_data = bot.get('breakout_data', None)
    tp_stage = bot.get('tp_stage', 0)
    current_px = float(df.iloc[-1]['close'])

    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    signal, reason, bo_data = calculate_trap(df, pos_side, entry_stage, avg_entry, breakout_data, tp_stage)
    log.debug(f"[{pair}] {signal}: {reason}")

    # --- BREAKOUT ENTRY (10% of capital, Velez) ---
    if signal == 'BREAKOUT_LONG' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        alloc = bot['current_usd'] * 0.10
        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1:
                return
        else:
            qty = round((alloc * 0.99) / current_px, 6)

        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(alloc * 0.99, 2)))

            bot['asset_held'] = qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'LONG'
            bot['entry_price'] = current_px
            bot['avg_entry'] = current_px
            bot['entry_stage'] = 1
            bot['tp_stage'] = 0
            bot['breakout_data'] = bo_data
            save_bots()
            log.info(f"[{pair}] LONG STAGE 1: 10% at {current_px:.2f}")
        except Exception as e:
            log.error(f"[{pair}] Stage 1 order failed: {e}")

    elif signal == 'BREAKOUT_SHORT' and pos_side == 'FLAT' and bot['current_usd'] > 5.0:
        if not deriv_flag:
            return

        alloc = bot['current_usd'] * 0.10
        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1:
            return

        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty))

            bot['asset_held'] = -qty
            bot['current_usd'] -= alloc
            bot['position_side'] = 'SHORT'
            bot['entry_price'] = current_px
            bot['avg_entry'] = current_px
            bot['entry_stage'] = 1
            bot['tp_stage'] = 0
            bot['breakout_data'] = bo_data
            save_bots()
            log.info(f"[{pair}] SHORT STAGE 1: 10% at {current_px:.2f}")
        except Exception as e:
            log.error(f"[{pair}] Stage 1 order failed: {e}")

    # --- ADD TO POSITION (10% each add, up to 2 adds, Velez 3-stage pyramiding) ---
    elif signal == 'ADD_LONG' and pos_side == 'LONG' and entry_stage in (1, 2) and bot['current_usd'] > 5.0:
        alloc = bot['allocated_usd'] * 0.10
        alloc = min(alloc, bot['current_usd'] * 0.99)
        if deriv_flag:
            qty = int((alloc * 0.99) / (current_px * mult))
            if qty < 1: return
        else:
            qty = round((alloc * 0.99) / current_px, 6)

        try:
            oid = str(uuid.uuid4())
            if deriv_flag:
                client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(qty))
            else:
                client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(alloc * 0.99, 2)))

            old_size = abs(bot['asset_held'])
            old_cost = old_size * bot['avg_entry']
            new_cost = qty * current_px
            total_size = old_size + qty
            bot['avg_entry'] = (old_cost + new_cost) / total_size if total_size > 0 else current_px

            bot['asset_held'] += qty
            bot['current_usd'] -= alloc
            bot['entry_stage'] = entry_stage + 1
            save_bots()
            log.info(f"[{pair}] LONG ADD {entry_stage+1}: +10% at {current_px:.2f}, avg {bot['avg_entry']:.2f}")
        except Exception as e:
            log.error(f"[{pair}] Add order failed: {e}")

    elif signal == 'ADD_SHORT' and pos_side == 'SHORT' and entry_stage in (1, 2) and bot['current_usd'] > 5.0:
        if not deriv_flag: return

        alloc = bot['allocated_usd'] * 0.10
        alloc = min(alloc, bot['current_usd'] * 0.99)
        qty = int((alloc * 0.99) / (current_px * mult))
        if qty < 1: return

        try:
            oid = str(uuid.uuid4())
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(qty))

            old_size = abs(bot['asset_held'])
            old_cost = old_size * bot['avg_entry']
            new_cost = qty * current_px
            total_size = old_size + qty
            bot['avg_entry'] = (old_cost + new_cost) / total_size if total_size > 0 else current_px

            bot['asset_held'] -= qty
            bot['current_usd'] -= alloc
            bot['entry_stage'] = entry_stage + 1
            save_bots()
            log.info(f"[{pair}] SHORT ADD {entry_stage+1}: +10% at {current_px:.2f}, avg {bot['avg_entry']:.2f}")
        except Exception as e:
            log.error(f"[{pair}] Add order failed: {e}")

    # --- PARTIAL EXIT (T1: sell 50%, move stop to breakeven) ---
    elif signal == 'PARTIAL_EXIT_LONG' and pos_side == 'LONG' and bot['asset_held'] > 0:
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            sell_qty = round(held * 0.5, 6) if not deriv_flag else max(1, int(held * 0.5))
            if deriv_flag:
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(sell_qty))
            else:
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(sell_qty))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * sell_qty * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, sell_qty, 'LONG', 'TARGET_1', pair, mult, actual_fee=actual_fee)

            profit = (actual_exit - bot['avg_entry']) * sell_qty * mult
            bot['current_usd'] += profit - actual_fee
            bot['asset_held'] -= sell_qty
            bot['tp_stage'] = 1
            save_bots()
            log.info(f"[{pair}] PARTIAL EXIT LONG (T1): Sold 50% ({sell_qty}), stop->BE. {reason}")
        except Exception as e:
            log.error(f"[{pair}] Partial exit failed: {e}")

    elif signal == 'PARTIAL_EXIT_SHORT' and pos_side == 'SHORT' and bot['asset_held'] < 0:
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            cover_qty = max(1, int(held * 0.5))
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(cover_qty))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * cover_qty * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, cover_qty, 'SHORT', 'TARGET_1', pair, mult, actual_fee=actual_fee)

            profit = (bot['avg_entry'] - actual_exit) * cover_qty * mult
            bot['current_usd'] += profit - actual_fee
            bot['asset_held'] += cover_qty
            bot['tp_stage'] = 1
            save_bots()
            log.info(f"[{pair}] PARTIAL EXIT SHORT (T1): Covered 50% ({cover_qty}), stop->BE. {reason}")
        except Exception as e:
            log.error(f"[{pair}] Partial exit failed: {e}")

    # --- FULL EXITS (T2, stop loss, extended) ---
    elif signal == 'EXIT_LONG' and pos_side == 'LONG' and bot['asset_held'] > 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else 'SIGNAL'
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str(held))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (actual_exit - bot['avg_entry']) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot['entry_stage'] = 0
            bot['avg_entry'] = 0.0
            bot.pop('breakout_data', None)
            bot.pop('tp_stage', None)
            save_bots()
            log.info(f"[{pair}] EXIT LONG: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Exit failed: {e}")

    elif signal == 'EXIT_SHORT' and pos_side == 'SHORT' and bot['asset_held'] < 0:
        exit_reason = 'STOP_LOSS' if 'STOP' in reason.upper() else 'SIGNAL'
        try:
            oid = str(uuid.uuid4())
            held = abs(bot['asset_held'])
            client.market_order_buy(client_order_id=oid, product_id=pair, base_size=str(held))

            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else current_px
            actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

            record_trade(bot, bot['avg_entry'], actual_exit, held, 'SHORT', exit_reason, pair, mult, actual_fee=actual_fee)

            profit = (bot['avg_entry'] - actual_exit) * held * mult
            bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            bot['entry_stage'] = 0
            bot['avg_entry'] = 0.0
            bot.pop('breakout_data', None)
            bot.pop('tp_stage', None)
            save_bots()
            log.info(f"[{pair}] EXIT SHORT: {reason}")
        except Exception as e:
            log.error(f"[{pair}] Exit failed: {e}")

# ==========================================
# MOMENTUM EXECUTOR
# ==========================================

def momentum_get_stop_price(bot, cur_px):
    """
    Calculates the current stop price based on the three-phase trailing stop.
    Returns (stop_price, phase) tuple.
    Used by both REST cycle and WS tick handler for identical logic.
    """
    entry_px = bot.get('entry_price', 0)
    entry_atr = bot.get('entry_atr', 0)
    hwm = bot.get('high_water_mark', cur_px)
    fee_est = bot.get('fee_estimate', 0)
    held = abs(bot.get('asset_held', 0))
    mult = get_contract_multiplier(bot.get('pair', ''))

    if entry_px <= 0 or entry_atr <= 0 or held <= 0:
        return 0.0, 0

    pnl = (cur_px - entry_px) * held * mult
    fee_per_unit = fee_est / held if held > 0 else 0

    if pnl >= fee_est * 2:
        # Phase 3: LOCKED PROFIT — trail 0.75x ATR, floor at breakeven + fees
        phase = 3
        trail_stop = hwm - (0.75 * entry_atr)
        floor_stop = entry_px + fee_per_unit
        stop_px = max(trail_stop, floor_stop)
    elif pnl >= fee_est:
        # Phase 2: TIGHTENED — trail 1.0x ATR, floor at entry - 0.5x ATR
        phase = 2
        trail_stop = hwm - (1.0 * entry_atr)
        floor_stop = entry_px - (0.5 * entry_atr)
        stop_px = max(trail_stop, floor_stop)
    else:
        # Phase 1: INITIAL — trail 1.5x ATR
        phase = 1
        stop_px = hwm - (1.5 * entry_atr)

    return round(stop_px, 6), phase

def execute_momentum(bot_id, bot, pair):
    """
    MOMENTUM: Trend-pullback reversal with maker limit entry and 3-phase trailing stop.
    
    State machine:
    - FLAT + no pending: scan for signal, place maker limit on BUY
    - FLAT + pending: check fill, re-evaluate on timeout (90s, max 3 retries)
    - LONG: evaluate trailing stop (REST fallback; WS is primary via bot_ws.py)
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = getattr(p_info, 'base_increment', '0.00000001')
        quote_inc = getattr(p_info, 'quote_increment', '0.01')
    except Exception as e:
        log.error(f"[{pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    pos_side = bot.get('position_side', 'FLAT')
    deriv_flag = is_derivative(pair)
    mult = get_contract_multiplier(pair)

    # ==========================================
    # STATE: LONG — Trailing Stop (REST fallback)
    # ==========================================
    if pos_side == 'LONG' and bot.get('asset_held', 0) > 0:
        # Update high water mark
        hwm = bot.get('high_water_mark', cur_px)
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            hwm = cur_px

        stop_px, phase = momentum_get_stop_price(bot, cur_px)
        bot['stop_phase'] = phase

        if stop_px > 0 and cur_px <= stop_px:
            # STOP TRIGGERED
            exit_reason = 'STOP_LOSS' if phase == 1 else 'TRAILING_STOP'
            held = abs(bot['asset_held'])
            log.warning(f"[{pair}] Phase {phase} stop triggered: price {cur_px:.2f} <= stop {stop_px:.2f}")

            try:
                if bot.get('paper'):
                    actual_exit = cur_px
                    actual_fee = cur_px * held * mult * 0.004
                    log.info(f"[{pair}] PAPER MOMENTUM EXIT ({exit_reason}): {held} at ${cur_px:.4f}")
                else:
                    oid = str(uuid.uuid4())
                    str_qty = snap_to_increment(held, base_inc)
                    client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)

                    fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                    actual_exit = fill_px if fill_px else cur_px
                    actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] = bot['allocated_usd'] + profit - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                # Clear momentum state
                for key in ['entry_atr', 'high_water_mark', 'stop_phase', 'fee_estimate',
                            'pending_order_oid', 'pending_order_time', 'signal_retries']:
                    bot.pop(key, None)
                save_bots()
                log.info(f"[{pair}] EXIT ({exit_reason}): PnL ${profit:.2f}")
            except Exception as e:
                log.error(f"[{pair}] Exit sell failed: {e}")
        else:
            save_bots()
        return

    # ==========================================
    # STATE: FLAT + PENDING ORDER — Check fill / timeout
    # ==========================================
    pending_oid = bot.get('pending_order_oid')
    if pos_side == 'FLAT' and pending_oid:
        pending_time = bot.get('pending_order_time', 0)
        elapsed = time.time() - pending_time
        retries = bot.get('signal_retries', 0)

        # Check if order filled
        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            mom_order_obj = None
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_oid:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    mom_order_obj = o
                    break
        except Exception as e:
            log.error(f"[{pair}] Fill check error: {e}")
            return

        if filled and filled_size > 0:
            # --- ORDER FILLED: Transition to LONG ---
            import pandas_ta as pta
            atr_series = pta.atr(df['high'], df['low'], df['close'], 14)
            entry_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else cur_px * 0.01

            real_fee = extract_fee(mom_order_obj)
            gross_cost = avg_fill_px * filled_size * mult
            if real_fee is None:
                real_fee = gross_cost * 0.0025

            bot['asset_held'] = filled_size
            bot['current_usd'] -= (gross_cost + real_fee)
            bot['position_side'] = 'LONG'
            bot['entry_price'] = avg_fill_px
            bot['entry_atr'] = entry_atr
            bot['high_water_mark'] = avg_fill_px
            bot['stop_phase'] = 1
            bot['fee_estimate'] = real_fee
            bot.pop('pending_order_oid', None)
            bot.pop('pending_order_time', None)
            bot.pop('signal_retries', None)
            save_bots()
            log.info(f"[{pair}] FILLED at {avg_fill_px:.2f}. ATR={entry_atr:.2f}. Phase 1 stop at {avg_fill_px - 1.5*entry_atr:.2f}")
            return

        # Check timeout
        if elapsed >= 90:
            # Cancel the stale order
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                            log.debug(f"[{pair}] Cancelled stale limit order ({elapsed:.0f}s)")
                        break
            except Exception as e:
                log.error(f"[{pair}] Cancel error: {e}")

            # Re-evaluate signal
            signal, reason, sig_atr = calculate_momentum(df)

            if signal == 'BUY' and retries < 3:
                # Re-place at current best price
                limit_px = cur_px - float(quote_inc)
                str_price = snap_to_increment(limit_px, quote_inc)

                if deriv_flag:
                    qty = int((bot['current_usd'] * 0.99) / (limit_px * mult))
                    if qty < 1:
                        bot.pop('pending_order_oid', None)
                        bot.pop('pending_order_time', None)
                        bot.pop('signal_retries', None)
                        save_bots()
                        return
                    str_qty = str(qty)
                else:
                    qty = float(bot['current_usd'] * 0.99) / limit_px
                    str_qty = snap_to_increment(qty, base_inc)

                try:
                    new_oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(
                        client_order_id=new_oid, product_id=pair,
                        base_size=str_qty, limit_price=str_price, post_only=True
                    )
                    bot['pending_order_oid'] = new_oid
                    bot['pending_order_time'] = time.time()
                    bot['signal_retries'] = retries + 1
                    save_bots()
                    log.debug(f"[{pair}] Re-placed limit buy at {str_price} (retry {retries + 1}/3)")
                except Exception as e:
                    log.error(f"[{pair}] Re-place failed: {e}")
                    bot.pop('pending_order_oid', None)
                    bot.pop('pending_order_time', None)
                    bot.pop('signal_retries', None)
                    save_bots()
            else:
                # Signal died or max retries — abandon
                reason_str = f"signal lost ({reason})" if signal != 'BUY' else f"max retries ({retries})"
                log.warning(f"[{pair}] Abandoned entry: {reason_str}")
                bot.pop('pending_order_oid', None)
                bot.pop('pending_order_time', None)
                bot.pop('signal_retries', None)
                save_bots()
        return

    # ==========================================
    # STATE: FLAT — Scan for signal
    # ==========================================
    if pos_side != 'FLAT' or bot['current_usd'] <= 5.0:
        return

    signal, reason, sig_atr = calculate_momentum(df)

    if signal != 'BUY':
        return

    log.info(f"[{pair}] SIGNAL: {reason}")

    # Place maker limit buy 1 tick below current price
    limit_px = cur_px - float(quote_inc)
    str_price = snap_to_increment(limit_px, quote_inc)

    if deriv_flag:
        qty = int((bot['current_usd'] * 0.99) / (limit_px * mult))
        if qty < 1:
            log.warning(f"[{pair}] Insufficient capital for 1 derivative contract.")
            return
        str_qty = str(qty)
    else:
        qty = float(bot['current_usd'] * 0.99) / limit_px
        str_qty = snap_to_increment(qty, base_inc)

    if float(str_qty) <= 0:
        return

    # Paper mode: instant-fill at current price, skip the pending/retry dance
    if bot.get('paper'):
        import pandas_ta as pta
        atr_series = pta.atr(df['high'], df['low'], df['close'], 14)
        entry_atr = float(atr_series.iloc[-1]) if atr_series is not None and not atr_series.empty else cur_px * 0.01
        filled = float(str_qty)
        gross_cost = cur_px * filled * mult
        sim_fee = gross_cost * 0.004
        bot['asset_held'] = filled
        bot['current_usd'] -= (gross_cost + sim_fee)
        bot['position_side'] = 'LONG'
        bot['entry_price'] = cur_px
        bot['entry_atr'] = entry_atr
        bot['high_water_mark'] = cur_px
        bot['stop_phase'] = 1
        bot['fee_estimate'] = sim_fee
        save_bots()
        log.info(f"[{pair}] PAPER MOMENTUM LONG: {filled:.8f} at ${cur_px:.4f}. ATR={entry_atr:.4f}. Phase 1 stop at ${cur_px - 1.5*entry_atr:.4f}")
        return

    try:
        oid = str(uuid.uuid4())
        api_res = client.limit_order_gtc_buy(
            client_order_id=oid, product_id=pair,
            base_size=str_qty, limit_price=str_price, post_only=True
        )
        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))

        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
            bot['pending_order_oid'] = oid
            bot['pending_order_time'] = time.time()
            bot['signal_retries'] = 0
            save_bots()
            log.info(f"[{pair}] Limit BUY placed at {str_price} (post_only). Waiting for fill...")
        else:
            log.error(f"[{pair}] Limit order rejected: {fail_reason}")
    except Exception as e:
        log.error(f"[{pair}] Order placement failed: {e}")

# ==========================================
# DCA EXECUTOR
# ==========================================

DCA_PROFIT_TIERS = [
    (3.0,  0.20),  # 3.0% profit → sell 20% of remaining (net ~2.2% after fees)
    (5.0,  0.25),  # 5.0% → 25%
    (7.5,  0.30),  # 7.5% → 30%
    (10.0, 0.35),  # 10.0% → 35%
    (15.0, 0.50),  # 15.0% → 50%
    (20.0, 0.75),  # 20.0%+ → 75%, remainder rides as moonbag
]

# Research engine: mode-specific configs (mirrors backtest_engine.py DCA_MODE_CONFIG)
DCA_NORMAL_TIERS   = DCA_PROFIT_TIERS  # alias — same tiers for NORMAL mode
DCA_CAUTIOUS_TIERS = [(2.0, 0.25), (3.0, 0.35), (5.0, 0.50)]
DCA_SCALP_TIERS    = [(1.5, 1.0)]

DCA_MODE_CONFIG = {
    'NORMAL':   {'size_mult': 1.0,  'arm_thresh': -0.30, 'depth_cap': 6.0, 'tiers': DCA_NORMAL_TIERS,   'circuit_break': 25},
    'CAUTIOUS': {'size_mult': 0.60, 'arm_thresh': -0.50, 'depth_cap': 3.0, 'tiers': DCA_CAUTIOUS_TIERS, 'circuit_break': 20},
    'SCALP':    {'size_mult': 0.30, 'arm_thresh': -1.00, 'depth_cap': 2.0, 'tiers': DCA_SCALP_TIERS,    'circuit_break': 15},
}


# ==========================================
# DCA RESEARCH ENGINE HELPERS
# ==========================================

def _dca_compute_trend_ema(pair, tf_minutes, ema_len):
    """Fetch higher-TF candles and compute EMA. Returns most recent completed (not in-progress) EMA value.
    Uses Coinbase's closest natural granularity then resamples to exact tf_minutes via pandas."""
    try:
        # Pick the largest natural granularity that divides tf_minutes
        cb_gran_map = {1: 'ONE_MINUTE', 5: 'FIVE_MINUTE', 15: 'FIFTEEN_MINUTE',
                       30: 'THIRTY_MINUTE', 60: 'ONE_HOUR', 360: 'SIX_HOURS', 1440: 'ONE_DAY'}
        # Find the largest natural granularity <= tf_minutes that divides it cleanly
        natural = 60  # default
        for g in sorted(cb_gran_map.keys(), reverse=True):
            if g <= tf_minutes and tf_minutes % g == 0:
                natural = g
                break
        cb_gran = cb_gran_map[natural]
        lookback_minutes = (ema_len + 10) * tf_minutes
        end_ts = int(time.time())
        start_ts = end_ts - (lookback_minutes * 60)
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        if len(candles) < ema_len + 2:
            return None
        parsed = sorted([{'start': int(c['start']), 'close': float(c['close'])} for c in candles],
                        key=lambda x: x['start'])
        df = pd.DataFrame(parsed)
        # If natural granularity == tf_minutes, use directly; else resample
        if natural < tf_minutes:
            df['_dt'] = pd.to_datetime(df['start'], unit='s')
            resampled = df.set_index('_dt').resample(f'{tf_minutes}min').agg({'close': 'last'}).dropna()
            if len(resampled) < ema_len + 2:
                return None
            close_series = resampled['close']
        else:
            close_series = df['close']
        ema_series = close_series.ewm(span=ema_len, adjust=False).mean()
        if ema_series is None or len(ema_series) < 2:
            return None
        # Use PREVIOUS completed period's EMA to avoid look-ahead (matches backtest)
        prev_ema = float(ema_series.iloc[-2]) if not pd.isna(ema_series.iloc[-2]) else None
        return prev_ema
    except Exception as e:
        log.warning(f"[{pair}] _dca_compute_trend_ema failed: {e}")
        return None


def _dca_get_trend_mode(cur_px, trend_ema):
    """Classify NORMAL / CAUTIOUS / SCALP from price vs higher-TF EMA. Mirrors backtest _get_trend_mode."""
    if trend_ema is None or trend_ema <= 0:
        return 'NORMAL'
    pct_below = max(0, (trend_ema - cur_px) / trend_ema * 100)
    if pct_below <= 0:
        return 'NORMAL'
    elif pct_below <= 15:
        return 'CAUTIOUS'
    else:
        return 'SCALP'


def _dca_update_dip_tracking(bot, fast_roc, slow_roc):
    """Track local ROC bottoms for dynamic depth threshold. Updates bot state in place."""
    settings = bot.get('settings', {})
    window = int(settings.get('dynamic_depth_window', 10))

    worst_roc = min(fast_roc, slow_roc)
    dip_in_progress = bot.get('dip_in_progress', False)
    current_dip_low = bot.get('current_dip_low', 0.0)
    recent_dip_lows = bot.get('recent_dip_lows', [])

    if not dip_in_progress and worst_roc < 0:
        dip_in_progress = True
        current_dip_low = worst_roc
    elif dip_in_progress:
        if worst_roc < current_dip_low:
            current_dip_low = worst_roc
        # Dip ends when both ROCs return non-negative
        if fast_roc >= 0 and slow_roc >= 0:
            recent_dip_lows.append(current_dip_low)
            if len(recent_dip_lows) > window:
                recent_dip_lows.pop(0)
            dip_in_progress = False
            current_dip_low = 0.0

    bot['dip_in_progress'] = dip_in_progress
    bot['current_dip_low'] = current_dip_low
    bot['recent_dip_lows'] = recent_dip_lows


def _dca_resolve_arm_threshold(bot, mode):
    """Pick arm_threshold: dynamic depth > mode threshold > legacy -0.30.
    Dynamic depth only applies when adaptive_defense AND dynamic_depth are both on."""
    settings = bot.get('settings', {})
    adaptive_on = settings.get('adaptive_defense', False)
    dynamic_on = adaptive_on and settings.get('dynamic_depth', False)

    if dynamic_on:
        recent = bot.get('recent_dip_lows', [])
        if len(recent) >= 3:
            multiplier = float(settings.get('dynamic_depth_multiplier', 0.85))
            floor = float(settings.get('dynamic_depth_floor', -0.30))
            avg_low = sum(recent) / len(recent)
            dyn_thresh = avg_low * multiplier
            return min(floor, dyn_thresh)  # more negative = deeper

    if adaptive_on:
        return DCA_MODE_CONFIG.get(mode, DCA_MODE_CONFIG['NORMAL'])['arm_thresh']

    return -0.30  # legacy default


def _dca_compute_crisis_score(drawdown_pct, exposure_ratio, mode, recovery_distance, cycles_to_recover):
    """5-factor crisis score (0-100). Ported verbatim from backtest_engine.py _compute_crisis_score.
    Higher score = more urgency to cut position and free capital."""
    # Factor 1: Drawdown severity (0-25)
    if drawdown_pct >= 25:    f1 = 25
    elif drawdown_pct >= 20:  f1 = 22
    elif drawdown_pct >= 15:  f1 = 18
    elif drawdown_pct >= 10:  f1 = 12
    elif drawdown_pct >= 5:   f1 = 5
    else:                     f1 = 0

    # Factor 2: Capital exposure (0-25)
    if exposure_ratio >= 0.85:    f2 = 25
    elif exposure_ratio >= 0.70:  f2 = 22
    elif exposure_ratio >= 0.50:  f2 = 18
    elif exposure_ratio >= 0.30:  f2 = 12
    elif exposure_ratio >= 0.15:  f2 = 5
    else:                         f2 = 0

    # Factor 3: Trend hostility (0-20)
    if mode == 'SCALP':       f3 = 20
    elif mode == 'CAUTIOUS':  f3 = 10
    else:                     f3 = 0

    # Factor 4: Recovery distance (0-15)
    if recovery_distance >= 20:    f4 = 15
    elif recovery_distance >= 10:  f4 = 10
    elif recovery_distance >= 5:   f4 = 5
    else:                          f4 = 0

    # Factor 5: Opportunity cost — fewer cycles to recover = cheaper to cut (0-15)
    if cycles_to_recover <= 3:     f5 = 15
    elif cycles_to_recover <= 7:   f5 = 10
    elif cycles_to_recover <= 15:  f5 = 5
    else:                          f5 = 0

    score = f1 + f2 + f3 + f4 + f5
    factors = {'drawdown': f1, 'exposure': f2, 'trend': f3, 'distance': f4, 'opportunity': f5}
    return score, factors


def _dca_cancel_all_sells(bot, pair):
    """Cancel all pending DCA sell orders. Called when a new buy fills (avg entry changed)."""
    pending = bot.get('pending_sells', [])
    if not pending:
        return
    cancelled = 0
    for sell in list(pending):
        sell_oid = sell.get('oid', '')
        if sell_oid:
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == sell_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                            cancelled += 1
                        break
            except Exception as e:
                log.error(f"[{pair}] Cancel sell error: {e}")
    bot['pending_sells'] = []
    bot['highest_tier_sold'] = 0
    if cancelled:
        log.debug(f"[{pair}] Cancelled {cancelled} pending sells (avg entry changed)")
    save_bots()

def _dca_check_sell_fills(bot, pair):
    """Check if any pending DCA sell orders have filled."""
    pending = bot.get('pending_sells', [])
    if not pending:
        return

    # Track processed OIDs to prevent re-processing the same fill
    processed = set(bot.get('_processed_sell_oids', []))

    mult = get_contract_multiplier(pair)
    try:
        order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "FILLED", "product_id": pair, "limit": 20
        })
        filled_orders = {o.get('client_order_id'): o for o in order_data.get('orders', []) if o.get('client_order_id')}
    except Exception as e:
        log.error(f"[{pair}] Sell fill check error: {e}")
        return

    new_pending = []
    changes = False
    for sell in pending:
        oid = sell.get('oid', '')
        if oid in processed:
            # Already processed this fill, skip entirely
            continue
        match = filled_orders.get(oid)
        if match:
            filled_size = float(match.get('filled_size', 0))
            avg_px = float(match.get('average_filled_price', sell.get('price', 0)))
            if filled_size > 0:
                # Mark as processed FIRST, before anything else
                processed.add(oid)
                bot['_processed_sell_oids'] = list(processed)[-50:]  # keep last 50

                bot['asset_held'] = max(0, bot.get('asset_held', 0) - filled_size)
                # Reduce total_cost proportionally to the fraction sold
                old_held = bot.get('asset_held', 0) + filled_size  # what held WAS before the line above
                if old_held > 0:
                    sold_fraction = filled_size / old_held
                    bot['total_cost'] = max(0, bot.get('total_cost', 0) * (1 - sold_fraction))
                real_fee = extract_fee(match)
                gross_proceeds = filled_size * avg_px * mult
                if real_fee is not None:
                    bot['current_usd'] += gross_proceeds - real_fee
                else:
                    real_fee = gross_proceeds * 0.0025  # fallback
                    bot['current_usd'] += gross_proceeds - real_fee
                tier_pct = sell.get('tier', 0)
                bot['highest_tier_sold'] = max(bot.get('highest_tier_sold', 0), tier_pct)
                changes = True
                pnl_val = (avg_px - bot.get('avg_entry', avg_px)) * filled_size * mult
                log.info(f"[{pair}] Tier {tier_pct}% sell FILLED: {filled_size:.8f} at ${avg_px:.2f} (fee=${real_fee:.4f})")
                notify_bot_exit(pair, 'DCA', avg_px, pnl_val, f'Tier {tier_pct}%')

                # Record trade AFTER state is fully updated and sell is NOT in new_pending
                try:
                    record_trade(bot, bot.get('avg_entry', avg_px), avg_px, filled_size, 'LONG', 'DCA_TIER', pair, mult, actual_fee=real_fee)
                except Exception as e:
                    log.error(f"[{pair}] record_trade error: {e}")
                continue  # Don't add to new_pending
        new_pending.append(sell)

    if changes:
        bot['pending_sells'] = new_pending
        held_remaining = bot.get('asset_held', 0)
        min_tradeable = float(bot.get('base_min_size', 0))

        # Cycle is done when remaining position is below minimum tradeable size
        # (all meaningful tiers have been sold, only untradeable dust or nothing left)
        cycle_done = (held_remaining < min_tradeable) and len(new_pending) == 0

        if cycle_done:
            if held_remaining > 0:
                # Dust remains — normalize total_cost to just the dust's actual value
                # so it doesn't inflate avg_entry on the next buy cycle
                dust_cost = held_remaining * bot.get('avg_entry', 0)
                bot['total_cost'] = dust_cost
                log.info(f"[{pair}] Tiers scaled out. {held_remaining:.8f} remains at avg ${bot.get('avg_entry',0):.2f}. Cycling to SCANNING.")
                bot['position_side'] = 'LONG'
            else:
                # Truly empty (asset_held == 0 exactly)
                log.info(f"[{pair}] Position fully closed. Resetting to SCANNING.")
                bot['position_side'] = 'FLAT'
                bot['avg_entry'] = 0
                bot['total_cost'] = 0
            # Trading cycle state resets independently of position
            bot['total_buys'] = 0
            bot['buy_count_this_cycle'] = 0
            bot['highest_tier_sold'] = 0
            bot['pending_sells'] = []
            bot['dca_state'] = 'SCANNING'
            bot['last_cross_direction'] = 'ABOVE'
        save_bots()

def _dca_manage_stale_sells(bot, pair, cur_px):
    """Cancel pending sells only when profit drops to zero or below (underwater).
    Leaves sells on the book as long as position is still profitable.
    Also requires a minimum 10-minute pending time to avoid churn."""
    pending = bot.get('pending_sells', [])
    avg_entry = bot.get('avg_entry', 0)
    if not pending or avg_entry <= 0:
        return

    profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
    now = time.time()
    changed = False

    for sell in list(pending):
        tier_pct = sell.get('tier', 0)
        placed_at = sell.get('placed_at', 0)
        pending_secs = now - placed_at if placed_at > 0 else 9999

        # Only cancel if profit has gone to zero or below AND sell has been
        # pending for at least 10 minutes (avoid thrashing on fresh placements)
        if profit_pct <= 0 and pending_secs >= 600:
            sell_oid = sell.get('oid', '')
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == sell_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                log.error(f"[{pair}] Stale sell cancel error: {e}")
            pending.remove(sell)
            changed = True
            log.warning(f"[{pair}] Cancelled stale {tier_pct}% sell (profit now {profit_pct:.2f}%, underwater)")

    if changed:
        bot['pending_sells'] = pending
        save_bots()

def execute_dca(bot_id, bot, pair):
    """
    DCA router — dispatches to legacy or research engine based on settings.dca_engine.
    - 'legacy' (default): existing tested code path, binary circuit breaker, no adaptive defense
    - 'research': full port of backtest DCA strategy with crisis scoring, sub-cycles,
      adaptive defense, dynamic depth threshold (minus Feature 2 dynamic TF switching)
    """
    engine = bot.get('settings', {}).get('dca_engine', 'legacy')
    if engine == 'research':
        _execute_dca_research(bot_id, bot, pair)
    else:
        _execute_dca_legacy(bot_id, bot, pair)


def _execute_dca_legacy(bot_id, bot, pair):
    """
    DCA LEGACY: Signal-gated accumulation with tiered profit-taking.
    All orders maker (post_only). Weighted avg entry tracking.
    Binary circuit breaker at -25% drawdown. No adaptive defense.
    This is the existing tested code path — do not modify unless porting a fix from research.
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        base_min = float(getattr(p_info, 'base_min_size', '0.00001'))
        if base_min * cur_px < 0.25:
            base_min = 0.25 / cur_px
    except Exception as e:
        log.error(f"[{pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    # Cache product info
    bot['base_min_size'] = base_min
    bot['base_increment'] = base_inc
    bot['quote_increment'] = quote_inc

    dca_state = bot.get('dca_state', 'SCANNING')
    last_cross = bot.get('last_cross_direction', 'ABOVE')
    avg_entry = bot.get('avg_entry', 0)
    held = bot.get('asset_held', 0)
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    # ==========================================
    # STEP 1: DRAWDOWN MANAGEMENT (graduated response)
    # ==========================================
    # -10% to -20%: half-size buys (keep accumulating, reduce exposure)
    # -20% to -25%: quarter-size buys (minimal, still averaging down)
    # Beyond -25%: full pause (catastrophic, something is fundamentally broken)
    drawdown_pct = 0
    drawdown_mult = 1.0
    if held >= base_min and avg_entry > 0:
        drawdown_pct = ((avg_entry - cur_px) / avg_entry) * 100

        # CIRCUIT BREAKER: hard exit at -25% drawdown
        if drawdown_pct >= 25 and dca_state not in ('CIRCUIT_BREAK', 'PAUSED'):
            log.warning(f"[{pair}] CIRCUIT BREAKER: {drawdown_pct:.1f}% drawdown — liquidating position")
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)
                record_trade(bot, avg_entry, actual_exit, held, 'LONG', 'CIRCUIT_BREAK', pair, mult, actual_fee=actual_fee)
                bot['current_usd'] += (held * actual_exit * mult) - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                bot['avg_entry'] = 0
                bot['total_cost'] = 0
                bot['dca_state'] = 'CIRCUIT_BREAK'
                notify_drawdown(pair, 'DCA', drawdown_pct)
                save_bots()
            except Exception as e:
                log.error(f"[{pair}] Circuit breaker sell failed: {e}")
            return

        if dca_state != 'PAUSED' and drawdown_pct >= 25:
            bot['dca_state'] = 'PAUSED'
            bot['paused_at'] = time.time()
            log.warning(f"[{pair}] PAUSED: {drawdown_pct:.1f}% drawdown — catastrophic threshold")
            notify_drawdown(pair, 'DCA', drawdown_pct)
            save_bots()
        elif dca_state == 'PAUSED' and drawdown_pct < 22:  # 3% hysteresis
            bot['dca_state'] = 'ACCUMULATING'
            bot['paused_at'] = 0
            log.info(f"[{pair}] UN-PAUSED: drawdown recovered to {drawdown_pct:.1f}%")
            save_bots()
        elif drawdown_pct >= 20:
            drawdown_mult = 0.25  # quarter-size buys
        elif drawdown_pct >= 10:
            drawdown_mult = 0.50  # half-size buys
        dca_state = bot.get('dca_state', 'SCANNING')

    # ==========================================
    # STEP 2: CHECK PENDING BUY FILL
    # ==========================================
    pending_buy = bot.get('pending_buy_oid')
    if pending_buy and dca_state == 'BUYING':
        pending_time = bot.get('pending_buy_time', 0)
        elapsed = time.time() - pending_time
        retries = bot.get('buy_retries', 0)

        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            buy_order_obj = None
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_buy:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    buy_order_obj = o
                    break
        except Exception as e:
            log.error(f"[{pair}] Buy fill check error: {e}")
            return

        if filled and filled_size > 0:
            # --- BUY FILLED ---
            old_held = bot.get('asset_held', 0)
            old_cost = bot.get('total_cost', 0)
            new_cost = avg_fill_px * filled_size * mult
            total_held = old_held + filled_size
            new_avg = (old_cost + new_cost) / (total_held * mult) if total_held > 0 else avg_fill_px

            buy_fee = extract_fee(buy_order_obj) if buy_order_obj else None
            if buy_fee is None:
                buy_fee = new_cost * 0.0025  # fallback

            bot['asset_held'] = total_held
            bot['total_cost'] = old_cost + new_cost
            bot['avg_entry'] = new_avg
            bot['entry_price'] = new_avg
            bot['current_usd'] -= (new_cost + buy_fee)
            bot['position_side'] = 'LONG'
            bot['total_buys'] = bot.get('total_buys', 0) + 1
            bot['buy_count_this_cycle'] = bot.get('buy_count_this_cycle', 0) + 1
            bot['dca_state'] = 'ACCUMULATING'
            bot['last_cross_direction'] = 'BELOW'  # just bought on a cross-below, need cross-above to re-arm
            bot.pop('pending_buy_oid', None)
            bot.pop('pending_buy_time', None)
            bot.pop('buy_retries', None)

            # Cancel all pending sells — avg entry just changed
            _dca_cancel_all_sells(bot, pair)

            save_bots()
            log.info(f"[{pair}] BUY FILLED: {filled_size:.8f} at ${avg_fill_px:.2f}. Avg entry now ${new_avg:.2f}. Total buys: {bot['total_buys']}")
            notify_bot_entry(pair, 'DCA', avg_fill_px, filled_size)
            return

        if elapsed >= 90:
            # Cancel stale buy
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_buy:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                log.error(f"[{pair}] Cancel buy error: {e}")

            # Re-evaluate
            signal, reason, data = calculate_dca(df, 'ARMED', last_cross)
            if signal == 'BUY' and retries < 3:
                limit_px = cur_px - float(quote_inc)
                str_price = snap_to_increment(limit_px, quote_inc)
                depth_mult = data.get('depth_multiplier', 1.0)
                buy_qty = base_min * depth_mult * drawdown_mult
                if deriv_flag:
                    buy_qty = max(1, int(buy_qty))
                str_qty = snap_to_increment(buy_qty, base_inc)

                buy_usd = float(str_qty) * limit_px * mult
                if buy_usd > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                    bot['dca_state'] = 'ACCUMULATING' if held > 0 else 'SCANNING'
                    bot.pop('pending_buy_oid', None)
                    bot.pop('pending_buy_time', None)
                    bot.pop('buy_retries', None)
                    save_bots()
                    return

                try:
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(client_order_id=oid, product_id=pair, base_size=str_qty, limit_price=str_price, post_only=True)
                    bot['pending_buy_oid'] = oid
                    bot['pending_buy_time'] = time.time()
                    bot['buy_retries'] = retries + 1
                    save_bots()
                    log.debug(f"[{pair}] Re-placed buy at ${str_price} (retry {retries+1}/3)")
                except Exception as e:
                    log.error(f"[{pair}] Re-place failed: {e}")
            else:
                bot['dca_state'] = 'ACCUMULATING' if held > 0 else 'ARMED'
                bot.pop('pending_buy_oid', None)
                bot.pop('pending_buy_time', None)
                bot.pop('buy_retries', None)
                save_bots()
        return

    # ==========================================
    # STEP 3: CHECK PENDING SELL FILLS
    # ==========================================
    _dca_check_sell_fills(bot, pair)

    # ==========================================
    # STEP 4: MANAGE STALE SELLS
    # ==========================================
    _dca_manage_stale_sells(bot, pair, cur_px)

    # ==========================================
    # STEP 4.5: TIER RESET AT -3% DRAWDOWN
    # ==========================================
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    min_tradeable = float(bot.get('base_min_size', 0))
    if held >= min_tradeable and avg_entry > 0:
        profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
        highest_sold = bot.get('highest_tier_sold', 0)
        if profit_pct <= -3.0 and highest_sold > 0:
            log.warning(f"[{pair}] TIER RESET: profit {profit_pct:.2f}% hit -3.0% with tier {highest_sold}% sold. Resetting tiers.")
            _dca_cancel_all_sells(bot, pair)
            bot['highest_tier_sold'] = 0
            bot['tier_reset_at'] = time.time()
            save_bots()

    # ==========================================
    # STEP 5: EVALUATE PROFIT TIERS
    # ==========================================
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    min_tradeable = float(bot.get('base_min_size', 0))
    if held >= min_tradeable and avg_entry > 0:
        # Cooldown: don't place new tier sells within 10 min of a tier reset
        tier_reset_at = bot.get('tier_reset_at', 0)
        if tier_reset_at > 0 and (time.time() - tier_reset_at) < 600:
            pass  # skip tier evaluation, still in cooldown
        else:
            if tier_reset_at > 0:
                bot.pop('tier_reset_at', None)  # cooldown expired, clean up

            profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
            highest_sold = bot.get('highest_tier_sold', 0)
            pending_sells = bot.get('pending_sells', [])
            pending_tiers = {s.get('tier') for s in pending_sells}

            # Deduct qty already committed to pending sells so tiers don't overcommit
            committed_qty = sum(s.get('qty', 0) for s in pending_sells)
            available_held = max(0, held - committed_qty)

            for tier_pct, sell_frac in DCA_PROFIT_TIERS:
                if tier_pct <= highest_sold:
                    continue  # already sold at this tier
                if tier_pct in pending_tiers:
                    continue  # already have a pending sell at this tier

                if profit_pct >= tier_pct and available_held > 0:
                    sell_qty = available_held * sell_frac
                    sell_px = avg_entry * (1 + tier_pct / 100.0)
                    if sell_px <= cur_px:
                        sell_px = cur_px + float(quote_inc)  # price already past tier, sell at market edge
                    str_price = snap_to_increment(sell_px, quote_inc)
                    str_qty = snap_to_increment(sell_qty, base_inc)

                    if float(str_qty) <= 0:
                        continue

                    if bot.get('paper'):
                        # Paper mode: simulate immediate sell fill
                        paper_fill_sell(bot, pair, tier_pct, str_qty, str_price, mult)
                        available_held -= float(str_qty)
                        continue

                    try:
                        oid = str(uuid.uuid4())
                        api_res = client.limit_order_gtc_sell(
                            client_order_id=oid, product_id=pair,
                            base_size=str_qty, limit_price=str_price, post_only=True
                        )
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        if success or (isinstance(api_res, dict) and api_res.get('failure_reason') == 'UNKNOWN_FAILURE_REASON') or getattr(api_res, 'failure_reason', '') == 'UNKNOWN_FAILURE_REASON':
                            pending_sells.append({
                                'tier': tier_pct,
                                'oid': oid,
                                'price': float(str_price),
                                'qty': float(str_qty),
                                'placed_at': time.time(),
                            })
                            available_held -= float(str_qty)  # deduct for next tier in this loop
                            bot['pending_sells'] = pending_sells
                            save_bots()
                            log.info(f"[{pair}] Placed {tier_pct}% tier sell: {str_qty} at ${str_price}")
                        else:
                            fail = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                            log.error(f"[{pair}] Tier sell rejected: {fail}")
                    except Exception as e:
                        log.error(f"[{pair}] Tier sell error: {e}")

    # ==========================================
    # STEP 6: SIGNAL EVALUATION
    # ==========================================
    dca_state = bot.get('dca_state', 'SCANNING')

    if dca_state == 'PAUSED':
        return  # No buys while paused, tiers still managed above

    if dca_state in ('SCANNING', 'ARMED'):
        signal, reason, data = calculate_dca(df, dca_state, last_cross)

        if signal == 'ARM':
            bot['dca_state'] = 'ARMED'
            bot['armed_at'] = time.time()
            save_bots()
            log.debug(f"[{pair}] ARMED: {reason}")

        elif signal == 'DISARM':
            bot['dca_state'] = 'SCANNING'
            save_bots()
            log.debug(f"[{pair}] DISARMED: {reason}")

        elif signal == 'BUY':
            if bot['current_usd'] < base_min * cur_px:
                log.warning(f"[{pair}] Signal BUY but insufficient capital (${bot['current_usd']:.2f})")
                return

            # Correlation guard: if multiple bots are entering simultaneously,
            # reduce size to avoid overexposure to the same broad market move
            concurrent_entries = sum(
                1 for b in ACTIVE_BOTS.values()
                if b.get('strategy') == 'DCA'
                and b.get('pair') != pair
                and b.get('dca_state') in ('ARMED', 'BUYING')
            )
            corr_mult = 1.0
            if concurrent_entries >= 3:
                corr_mult = 0.33
                log.warning(f"[{pair}] CORRELATION GUARD: {concurrent_entries} other bots entering, sizing to 33%")
            elif concurrent_entries >= 2:
                corr_mult = 0.50
                log.warning(f"[{pair}] CORRELATION GUARD: {concurrent_entries} other bots entering, sizing to 50%")

            depth_mult = data.get('depth_multiplier', 1.0)
            buy_pct = bot.get('buy_pct', 5.0)

            # Kelly Criterion: override buy_pct if user hasn't manually set it
            if not bot.get('buy_pct_manual'):
                from bot_utils import calculate_kelly_pct
                kelly_pct = calculate_kelly_pct('DCA', pair)
                if kelly_pct is not None:
                    buy_pct = kelly_pct
                    if buy_pct != bot.get('_last_kelly_pct'):
                        log.debug(f"[{pair}] Kelly sizing: {kelly_pct}% (was {bot.get('buy_pct', 5.0)}%)")
                        bot['_last_kelly_pct'] = buy_pct

            buy_usd = bot['current_usd'] * (buy_pct / 100.0) * depth_mult * drawdown_mult * corr_mult
            min_qty = max(base_min, 0.25 / cur_px) if cur_px > 0 else base_min
            buy_qty = buy_usd / (cur_px * mult) if cur_px > 0 else min_qty
            if buy_qty < min_qty:
                buy_qty = min_qty
            if deriv_flag:
                buy_qty = max(1, int(buy_qty))

            limit_px = cur_px - float(quote_inc)
            str_price = snap_to_increment(limit_px, quote_inc)
            str_qty = snap_to_increment(buy_qty, base_inc)

            buy_usd = float(str_qty) * limit_px * mult
            if buy_usd > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                log.warning(f"[{pair}] Buy size ${buy_usd:.2f} exceeds available ${bot['current_usd']:.2f}")
                return

            if bot.get('paper'):
                # Paper mode: simulate immediate fill
                paper_fill_buy(bot, pair, str_qty, str_price, mult)
                return

            try:
                oid = str(uuid.uuid4())
                api_res = client.limit_order_gtc_buy(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_price, post_only=True
                )
                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                    bot['dca_state'] = 'BUYING'
                    bot['pending_buy_oid'] = oid
                    bot['pending_buy_time'] = time.time()
                    bot['buy_retries'] = 0
                    save_bots()
                    log.info(f"[{pair}] BUYING: {str_qty} at ${str_price} ({depth_mult:.2f}x min). {reason}")
                else:
                    log.error(f"[{pair}] Buy rejected: {fail_reason}")
            except Exception as e:
                log.error(f"[{pair}] Buy placement failed: {e}")

    elif dca_state == 'ACCUMULATING':
        # Check for re-arm cycle
        signal, reason, data = calculate_dca(df, dca_state, last_cross)

        if signal == 'CROSS_ABOVE':
            bot['last_cross_direction'] = 'ABOVE'
            save_bots()
            log.debug(f"[{pair}] {reason}")

        elif signal == 'ARM':
            bot['dca_state'] = 'ARMED'
            bot['armed_at'] = time.time()
            bot['last_cross_direction'] = 'BELOW'
            save_bots()
            log.debug(f"[{pair}] RE-ARMED: {reason}")


# ==========================================
# DCA RESEARCH ENGINE
# Full port of _run_dca_backtest strategy logic to live execution.
# Includes: 5-factor crisis scoring, graduated cuts, sub-cycles, WOUNDED mode,
# adaptive defense, mode-specific configs, vol/degrade multipliers, dynamic depth.
# Excludes: Feature 2 dynamic TF switching.
# ==========================================

def _execute_dca_research(bot_id, bot, pair):
    """
    DCA research engine — mirrors backtest_engine.py::_run_dca_backtest semantics.
    Per-cycle flow matches the backtest bar loop, adapted for live execution with
    real maker limit orders, pending fill polling, and persistent state.
    """
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        base_min = float(getattr(p_info, 'base_min_size', '0.00001'))
        if base_min * cur_px < 0.25:
            base_min = 0.25 / cur_px
    except Exception as e:
        log.error(f"[{pair}] Research engine data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    # Cache product info
    bot['base_min_size'] = base_min
    bot['base_increment'] = base_inc
    bot['quote_increment'] = quote_inc

    # ── State reads ──
    settings = bot.get('settings', {})
    dca_state = bot.get('dca_state', 'SCANNING')
    last_cross = bot.get('last_cross_direction', 'ABOVE')
    avg_entry = bot.get('avg_entry', 0)
    held = bot.get('asset_held', 0)
    sub_held = bot.get('sub_held', 0.0)
    sub_avg = bot.get('sub_avg', 0.0)
    sub_cost = bot.get('sub_cost', 0.0)
    wounded_mode = bot.get('wounded_mode', False)
    bag_frozen = bot.get('bag_frozen', False)
    last_action_score = bot.get('last_action_score', 0)
    completed_cycle_profits = bot.get('completed_cycle_profits', [])
    mult = get_contract_multiplier(pair)
    deriv_flag = is_derivative(pair)

    # Pull config
    adaptive_defense = settings.get('adaptive_defense', False)
    flat_tp = settings.get('flat_tp_pct', 0)

    # ── STEP 1: Compute vol_mult (ATR ratio, matches backtest) ──
    try:
        import pandas_ta as pta
        atr_series = pta.atr(df['high'], df['low'], df['close'], length=14)
        atr_baseline_series = atr_series.ewm(span=200, adjust=False).mean()
        cur_atr = float(atr_series.iloc[-1]) if not pd.isna(atr_series.iloc[-1]) else cur_px * 0.02
        baseline_atr = float(atr_baseline_series.iloc[-1]) if not pd.isna(atr_baseline_series.iloc[-1]) else cur_atr
        vol_mult = max(0.1, min(1.5, baseline_atr / cur_atr if cur_atr > 0 else 1.0))
        bot['atr_baseline'] = round(baseline_atr, 8)
    except Exception as e:
        log.warning(f"[{pair}] vol_mult compute failed: {e}")
        vol_mult = 1.0

    # ── STEP 2: Compute adaptive defense mode ──
    mode = 'NORMAL'
    if adaptive_defense:
        defense_tf = int(settings.get('defense_tf', 240))
        defense_ema_len = int(settings.get('defense_ema_len', 50))
        trend_ema = _dca_compute_trend_ema(pair, defense_tf, defense_ema_len)
        mode = _dca_get_trend_mode(cur_px, trend_ema)
        bot['defense_mode'] = mode

    mode_cfg = DCA_MODE_CONFIG[mode]
    active_tiers = [(flat_tp, 1.0)] if flat_tp > 0 else mode_cfg['tiers']

    # ══════════════════════════════════════════
    # STEP 3: CHECK PENDING BUY FILL (same infrastructure as legacy)
    # ══════════════════════════════════════════
    pending_buy = bot.get('pending_buy_oid')
    if pending_buy and dca_state == 'BUYING':
        pending_time = bot.get('pending_buy_time', 0)
        elapsed = time.time() - pending_time
        retries = bot.get('buy_retries', 0)
        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            buy_order_obj = None
            filled_size = 0
            avg_fill_px = cur_px
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_buy:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    buy_order_obj = o
                    break
        except Exception as e:
            log.error(f"[{pair}] Research buy fill check error: {e}")
            return

        if filled and filled_size > 0:
            # Route fill: if wounded/frozen, goes to sub-cycle; else normal bag
            sub_route = wounded_mode or bag_frozen
            new_cost = avg_fill_px * filled_size * mult
            buy_fee = extract_fee(buy_order_obj) if buy_order_obj else None
            if buy_fee is None:
                buy_fee = new_cost * 0.0025

            if sub_route:
                # Sub-cycle entry (only if empty)
                if sub_held < 1e-10:
                    bot['sub_held'] = filled_size
                    bot['sub_avg'] = avg_fill_px
                    bot['sub_cost'] = new_cost
                    bot['sub_entry_time'] = int(time.time())
                    bot['current_usd'] -= (new_cost + buy_fee)
                    log.info(f"[{pair}] SUB-CYCLE BUY FILLED: {filled_size:.8f} @ ${avg_fill_px:.2f}")
            else:
                old_held = bot.get('asset_held', 0)
                old_cost = bot.get('total_cost', 0)
                total_held = old_held + filled_size
                new_avg = (old_cost + new_cost) / (total_held * mult) if total_held > 0 else avg_fill_px
                bot['asset_held'] = total_held
                bot['total_cost'] = old_cost + new_cost
                bot['avg_entry'] = new_avg
                bot['entry_price'] = new_avg
                bot['current_usd'] -= (new_cost + buy_fee)
                bot['position_side'] = 'LONG'
                bot['total_buys'] = bot.get('total_buys', 0) + 1
                bot['buy_count_this_cycle'] = bot.get('buy_count_this_cycle', 0) + 1
                log.info(f"[{pair}] BAG BUY FILLED: {filled_size:.8f} @ ${avg_fill_px:.2f}. Avg ${new_avg:.2f}")
                # Cancel any pending tier sells — avg entry changed
                _dca_cancel_all_sells(bot, pair)

            bot['dca_state'] = 'ACCUMULATING'
            bot['last_cross_direction'] = 'BELOW'
            bot.pop('pending_buy_oid', None)
            bot.pop('pending_buy_time', None)
            bot.pop('buy_retries', None)
            save_bots()
            notify_bot_entry(pair, 'DCA', avg_fill_px, filled_size)
            return

        if elapsed >= 90:
            # Timeout — cancel and retry up to 3 times
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_buy:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                log.error(f"[{pair}] Research cancel buy error: {e}")

            retries += 1
            if retries >= 3:
                bot['dca_state'] = 'ACCUMULATING' if (held > 0 or sub_held > 0) else 'SCANNING'
                bot.pop('pending_buy_oid', None)
                bot.pop('pending_buy_time', None)
                bot.pop('buy_retries', None)
                log.warning(f"[{pair}] Research buy retry limit reached — resetting to {bot['dca_state']}")
            else:
                bot['buy_retries'] = retries
                bot['pending_buy_time'] = time.time()
                log.debug(f"[{pair}] Research buy stale — cancelled, retry {retries}/3 on next cycle")
            save_bots()
            return

    # ══════════════════════════════════════════
    # STEP 4: CHECK PENDING SELL FILLS (same infrastructure)
    # ══════════════════════════════════════════
    _dca_check_sell_fills(bot, pair)
    _dca_manage_stale_sells(bot, pair, cur_px)

    # Re-read state after sell processing
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)
    sub_held = bot.get('sub_held', 0.0)
    sub_avg = bot.get('sub_avg', 0.0)

    # ══════════════════════════════════════════
    # STEP 5: CRISIS SCORING + GRADUATED CUTS
    # ══════════════════════════════════════════
    crisis_score = 0
    if held >= base_min and avg_entry > 0 and not bag_frozen:
        bag_dd = ((avg_entry - cur_px) / avg_entry) * 100
        if bag_dd > 0:
            pos_val = held * cur_px
            total_cap = bot.get('current_usd', 0) + pos_val + (sub_held * cur_px)
            exp_ratio = pos_val / total_cap if total_cap > 0 else 1.0
            first_tier = active_tiers[0][0] if active_tiers else 3.0
            rec_dist = bag_dd + first_tier
            win_profs = [p for p in completed_cycle_profits if p > 0]
            avg_cyc_prof = sum(win_profs) / len(win_profs) if win_profs else bot.get('allocated_usd', 1000) * 0.02
            unrealized = abs((avg_entry - cur_px) * held)
            cyc_to_rec = unrealized / avg_cyc_prof if avg_cyc_prof > 0 else 999

            crisis_score, factors = _dca_compute_crisis_score(bag_dd, exp_ratio, mode, rec_dist, cyc_to_rec)
            bot['crisis_score'] = crisis_score

            # Graduated cuts — only on score INCREASE to prevent re-triggering
            if crisis_score > last_action_score:
                cut_pct = 0.0
                action_name = 'NONE'
                if crisis_score >= 90:   cut_pct, action_name = 1.0,  'FULL_CUT'
                elif crisis_score >= 75: cut_pct, action_name = 0.75, 'HEAVY_CUT'
                elif crisis_score >= 60: cut_pct, action_name = 0.50, 'MOD_CUT'
                elif crisis_score >= 45: cut_pct, action_name = 0.25, 'LIGHT_CUT'

                if cut_pct > 0:
                    try:
                        sell_qty = held * cut_pct
                        _dca_cancel_all_sells(bot, pair)  # cancel pending tier sells first
                        time.sleep(0.2)
                        oid = str(uuid.uuid4())
                        str_qty = snap_to_increment(sell_qty, base_inc)
                        if float(str_qty) > 0:
                            if bot.get('paper'):
                                actual_exit = cur_px
                                actual_fee = cur_px * float(str_qty) * mult * 0.004
                            else:
                                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                                actual_exit = fill_px if fill_px else cur_px
                                actual_fee = fill_fee if fill_fee is not None else (actual_exit * float(str_qty) * mult * 0.0025)
                            f_str = ' '.join(f'{k[0]}={v}' for k, v in factors.items())
                            exit_reason = f'{action_name} (score={crisis_score} {f_str})'
                            if not bot.get('paper'):
                                record_trade(bot, avg_entry, actual_exit, float(str_qty), 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

                            # Update state
                            sold_frac = float(str_qty) / held if held > 0 else 1.0
                            bot['asset_held'] = held - float(str_qty)
                            bot['total_cost'] = bot.get('total_cost', 0) * (1 - sold_frac)
                            bot['current_usd'] += (float(str_qty) * actual_exit * mult) - actual_fee
                            bot['last_action_score'] = crisis_score
                            bot['wounded_mode'] = True
                            bot['bag_frozen'] = True
                            bot['dca_state'] = 'SCANNING'
                            bot['last_cross_direction'] = 'ABOVE'

                            # If full cut or bag drained, clear bag state
                            if bot['asset_held'] < base_min:
                                bot['asset_held'] = 0.0
                                bot['avg_entry'] = 0
                                bot['total_cost'] = 0
                                bot['bag_frozen'] = False  # no bag to freeze
                                bot['last_action_score'] = 0

                            log.warning(f"[{pair}] CRISIS CUT: {action_name} score={crisis_score} qty={str_qty}")
                            save_bots()
                            # Re-read held after mutation
                            held = bot.get('asset_held', 0)
                    except Exception as e:
                        log.error(f"[{pair}] Crisis cut failed: {e}")

    # ══════════════════════════════════════════
    # STEP 5.5: SUB-CYCLE TP/STOP CHECK
    # ══════════════════════════════════════════
    if sub_held > 0 and sub_avg > 0:
        sub_profit_pct = ((cur_px - sub_avg) / sub_avg) * 100
        sub_tp = flat_tp if flat_tp > 0 else 3.0
        sub_stop = -5.0

        exit_sub = False
        exit_reason = ''
        if sub_profit_pct >= sub_tp:
            exit_sub = True
            exit_reason = f'SUB_TP_{sub_tp}%'
        elif sub_profit_pct <= sub_stop:
            exit_sub = True
            exit_reason = 'SUB_STOP'

        if exit_sub:
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(sub_held, base_inc)
                if float(str_qty) > 0:
                    if bot.get('paper'):
                        actual_exit = cur_px
                        actual_fee = cur_px * float(str_qty) * mult * 0.004
                    else:
                        client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                        fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair, retries=2, delay=0.5)
                        actual_exit = fill_px if fill_px else cur_px
                        actual_fee = fill_fee if fill_fee is not None else (actual_exit * float(str_qty) * mult * 0.0025)
                    pnl = (actual_exit - sub_avg) * float(str_qty) * mult - actual_fee
                    if not bot.get('paper'):
                        record_trade(bot, sub_avg, actual_exit, float(str_qty), 'LONG', exit_reason, pair, mult, actual_fee=actual_fee)

                    bot['current_usd'] += (float(str_qty) * actual_exit * mult) - actual_fee
                    bot['sub_held'] = 0.0
                    bot['sub_avg'] = 0.0
                    bot['sub_cost'] = 0.0
                    sub_held = 0.0

                    if exit_reason.startswith('SUB_TP'):
                        completed_cycle_profits.append(pnl)
                        bot['completed_cycle_profits'] = completed_cycle_profits[-20:]  # cap list

                    log.info(f"[{pair}] {exit_reason}: ${actual_exit:.2f} pnl=${pnl:.2f}")
                    save_bots()
            except Exception as e:
                log.error(f"[{pair}] Sub-cycle exit failed: {e}")

    # ══════════════════════════════════════════
    # STEP 5.6: WOUNDED RECOVERY CHECK
    # ══════════════════════════════════════════
    if wounded_mode and mode == 'NORMAL' and crisis_score < 20:
        if bot.get('asset_held', 0) < base_min:
            bot['wounded_mode'] = False
            bot['bag_frozen'] = False
            wounded_mode = False
            log.info(f"[{pair}] WOUNDED mode cleared — recovery complete")
            save_bots()

    # ══════════════════════════════════════════
    # STEP 5.7: TIER EXITS ON BAG (only if not frozen)
    # ══════════════════════════════════════════
    held = bot.get('asset_held', 0)
    avg_entry = bot.get('avg_entry', 0)

    # Tier reset at -3% (same as legacy)
    if held >= base_min and avg_entry > 0 and not bag_frozen:
        profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
        highest_sold = bot.get('highest_tier_sold', 0)
        if profit_pct <= -3.0 and highest_sold > 0:
            log.warning(f"[{pair}] TIER RESET at {profit_pct:.2f}%")
            _dca_cancel_all_sells(bot, pair)
            bot['highest_tier_sold'] = 0
            bot['tier_reset_at'] = time.time()
            save_bots()

    # Tier sells (only if not frozen)
    if held >= base_min and avg_entry > 0 and not bag_frozen:
        tier_reset_at = bot.get('tier_reset_at', 0)
        if tier_reset_at > 0 and (time.time() - tier_reset_at) < 600:
            pass  # cooldown
        else:
            if tier_reset_at > 0:
                bot.pop('tier_reset_at', None)
            profit_pct = ((cur_px - avg_entry) / avg_entry) * 100
            highest_sold = bot.get('highest_tier_sold', 0)
            pending_sells = bot.get('pending_sells', [])
            pending_tiers = {s.get('tier') for s in pending_sells}
            # Sub-cycle sells don't count against bag tiers — filter to bag-type only
            committed_qty = sum(s.get('qty', 0) for s in pending_sells if s.get('type', 'bag') == 'bag')
            available_held = max(0, held - committed_qty)

            for tier_pct, sell_frac in active_tiers:
                if tier_pct <= highest_sold:
                    continue
                if tier_pct in pending_tiers:
                    continue
                if profit_pct >= tier_pct and available_held > 0:
                    sell_qty = available_held * sell_frac
                    sell_px = avg_entry * (1 + tier_pct / 100.0)
                    if sell_px <= cur_px:
                        sell_px = cur_px + float(quote_inc)
                    str_price = snap_to_increment(sell_px, quote_inc)
                    str_qty = snap_to_increment(sell_qty, base_inc)
                    if float(str_qty) <= 0:
                        continue

                    if bot.get('paper'):
                        paper_fill_sell(bot, pair, tier_pct, str_qty, str_price, mult)
                        available_held -= float(str_qty)
                        continue

                    try:
                        oid = str(uuid.uuid4())
                        api_res = client.limit_order_gtc_sell(
                            client_order_id=oid, product_id=pair,
                            base_size=str_qty, limit_price=str_price, post_only=True
                        )
                        success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                        fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                        if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                            pending_sells.append({
                                'tier': tier_pct, 'oid': oid,
                                'price': float(str_price), 'qty': float(str_qty),
                                'placed_at': time.time(), 'type': 'bag'
                            })
                            available_held -= float(str_qty)
                            bot['pending_sells'] = pending_sells
                            save_bots()
                            log.info(f"[{pair}] RESEARCH tier sell: {tier_pct}% {str_qty}@${str_price}")
                    except Exception as e:
                        log.error(f"[{pair}] Research tier sell error: {e}")

    # ══════════════════════════════════════════
    # STEP 6: SIGNAL EVALUATION + ENTRY
    # ══════════════════════════════════════════
    dca_state = bot.get('dca_state', 'SCANNING')
    last_cross = bot.get('last_cross_direction', 'ABOVE')

    # Resolve arm threshold (dynamic > mode > default, WOUNDED override)
    if wounded_mode:
        arm_thresh = -1.50  # match backtest wounded threshold
    else:
        arm_thresh = _dca_resolve_arm_threshold(bot, mode)

    if dca_state in ('SCANNING', 'ARMED', 'ACCUMULATING'):
        signal, reason, data = calculate_dca(df, dca_state, last_cross, arm_threshold=arm_thresh)

        # Update dip tracking (only when enabled)
        if adaptive_defense and settings.get('dynamic_depth', False):
            _dca_update_dip_tracking(bot, data.get('fast_roc', 0), data.get('slow_roc', 0))

        if signal == 'ARM' and dca_state == 'SCANNING':
            bot['dca_state'] = 'ARMED'
            bot['armed_at'] = time.time()
            save_bots()
            log.debug(f"[{pair}] ARMED (thresh={arm_thresh:.2f}, mode={mode}): {reason}")

        elif signal == 'DISARM' and dca_state == 'ARMED':
            bot['dca_state'] = 'SCANNING'
            save_bots()
            log.debug(f"[{pair}] DISARMED: {reason}")

        elif signal == 'BUY' and dca_state == 'ARMED' and bot['current_usd'] > base_min * cur_px:
            # Compute buy size with all multipliers (matches backtest)
            depth_mult = min(data.get('depth_multiplier', 1.0), mode_cfg['depth_cap'])

            # Drawdown multiplier (live carry-over)
            drawdown_mult = 1.0
            if held >= base_min and avg_entry > 0:
                dd = ((avg_entry - cur_px) / avg_entry) * 100
                if dd >= 20: drawdown_mult = 0.25
                elif dd >= 10: drawdown_mult = 0.50

            # Dynamic degradation — scales down as crisis worsens
            degrade_mult = max(0.05, 1.0 - (crisis_score / 45.0) ** 2) if crisis_score < 45 else 0.05

            # Correlation guard (multi-bot coordination, live only)
            concurrent = sum(
                1 for b in ACTIVE_BOTS.values()
                if b.get('strategy') == 'DCA'
                and b.get('pair') != pair
                and b.get('dca_state') in ('ARMED', 'BUYING')
            )
            corr_mult = 1.0
            if concurrent >= 3: corr_mult = 0.33
            elif concurrent >= 2: corr_mult = 0.50

            # Kelly override (live only, skipped when buy_pct_manual is True)
            buy_pct = bot.get('buy_pct', 5.0)
            if not bot.get('buy_pct_manual'):
                try:
                    from bot_utils import calculate_kelly_pct
                    kelly_pct = calculate_kelly_pct('DCA', pair)
                    if kelly_pct is not None:
                        buy_pct = kelly_pct
                except Exception:
                    pass

            buy_usd = bot['current_usd'] * (buy_pct / 100.0) * depth_mult * drawdown_mult * \
                      mode_cfg['size_mult'] * vol_mult * degrade_mult * corr_mult
            buy_usd = min(buy_usd, bot['current_usd'] * 0.50)

            min_qty = max(base_min, 0.25 / cur_px) if cur_px > 0 else base_min
            buy_qty = buy_usd / (cur_px * mult) if cur_px > 0 else min_qty
            if buy_qty < min_qty:
                log.warning(f"[{pair}] Buy size too small ({buy_qty:.8f} < {min_qty:.8f}) — skipping")
                bot['dca_state'] = 'ACCUMULATING' if (held > 0 or sub_held > 0) else 'SCANNING'
                save_bots()
                return
            if deriv_flag:
                buy_qty = max(1, int(buy_qty))

            limit_px = cur_px - float(quote_inc)
            str_price = snap_to_increment(limit_px, quote_inc)
            str_qty = snap_to_increment(buy_qty, base_inc)
            buy_usd_final = float(str_qty) * limit_px * mult

            if buy_usd_final > bot['current_usd'] * 0.99 or float(str_qty) <= 0:
                log.warning(f"[{pair}] Buy size exceeds available capital")
                return

            if bot.get('paper'):
                paper_fill_buy(bot, pair, str_qty, str_price, mult)
                return

            try:
                oid = str(uuid.uuid4())
                api_res = client.limit_order_gtc_buy(
                    client_order_id=oid, product_id=pair,
                    base_size=str_qty, limit_price=str_price, post_only=True
                )
                success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                    bot['dca_state'] = 'BUYING'
                    bot['pending_buy_oid'] = oid
                    bot['pending_buy_time'] = time.time()
                    bot['buy_retries'] = 0
                    save_bots()
                    route = 'SUB' if (wounded_mode or bag_frozen) else 'BAG'
                    log.info(f"[{pair}] RESEARCH BUYING ({route}, mode={mode}, size_mult={mode_cfg['size_mult']:.2f}): {str_qty}@${str_price}")
            except Exception as e:
                log.error(f"[{pair}] Research buy placement failed: {e}")

        elif dca_state == 'ACCUMULATING':
            # Re-arm cycle (same as legacy)
            if signal == 'CROSS_ABOVE':
                bot['last_cross_direction'] = 'ABOVE'
                save_bots()
            elif signal == 'ARM':
                bot['dca_state'] = 'ARMED'
                bot['armed_at'] = time.time()
                bot['last_cross_direction'] = 'BELOW'
                save_bots()
                log.debug(f"[{pair}] RE-ARMED: {reason}")


# ==========================================
# NPR EXECUTOR
# ==========================================

def npr_get_stop_and_trail(bot, cur_px):
    """Returns (should_exit, exit_reason) for WS tick-level monitoring."""
    if bot.get('position_side') == 'FLAT':
        return False, None
    event_stop = bot.get('event_stop', 0)
    side = bot.get('position_side')
    hwm = bot.get('high_water_mark', 0)
    lwm = bot.get('low_water_mark', 999999)
    trail_dist = bot.get('trail_distance', 0)
    partial = bot.get('partial_filled', False)
    if side == 'LONG':
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
            hwm = cur_px
    elif side == 'SHORT':
        if cur_px < lwm:
            bot['low_water_mark'] = cur_px
            lwm = cur_px
    if side == 'LONG' and event_stop > 0 and cur_px <= event_stop:
        return True, 'EVENT_STOP'
    if side == 'SHORT' and event_stop > 0 and cur_px >= event_stop:
        return True, 'EVENT_STOP'
    if partial and trail_dist > 0:
        if side == 'LONG' and hwm > 0 and cur_px <= (hwm - trail_dist):
            return True, 'TRAILING_STOP'
        elif side == 'SHORT' and lwm < 999999 and cur_px >= (lwm + trail_dist):
            return True, 'TRAILING_STOP'
    return False, None


def execute_npr(bot_id, bot, pair):
    """NPR: Three-check price action strategy for derivatives. 2m bars with entry timing."""
    import datetime
    from strategies import calculate_npr, NPR_CONFIG, _compute_zone

    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles", params={
            "start": str(start_ts), "end": str(end_ts), "granularity": cb_gran
        })
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
        base_min = float(getattr(p_info, 'base_min_size', '0.00001'))
        if base_min * cur_px < 0.25:
            base_min = 0.25 / cur_px
    except Exception as e:
        log.error(f"[{pair}] Data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'start': int(c['start']), 'open': float(c['open']), 'high': float(c['high']),
               'low': float(c['low']), 'close': float(c['close']), 'volume': float(c.get('volume', 0))}
              for c in candles]
    df = pd.DataFrame(parsed).sort_values('start').reset_index(drop=True)

    mult = get_contract_multiplier(pair)
    npr_state = bot.get('npr_state', 'SCANNING')
    max_loss_trade = bot.get('max_loss_per_trade', 10.0)
    max_loss_day = bot.get('max_loss_per_day', max_loss_trade * 3)

    # === DAILY LOSS TRACKING ===
    today_str = datetime.datetime.utcnow().strftime('%Y-%m-%d')
    if bot.get('daily_loss_date') != today_str:
        bot['daily_loss'] = 0.0
        bot['daily_loss_date'] = today_str
        if npr_state == 'DAILY_HALT':
            bot['npr_state'] = 'SCANNING'
            npr_state = 'SCANNING'
            log.info(f"[{pair}] New UTC day -- resuming from DAILY_HALT")
        save_bots()

    if bot.get('daily_loss', 0) >= max_loss_day:
        if npr_state != 'DAILY_HALT':
            bot['npr_state'] = 'DAILY_HALT'
            save_bots()
            log.warning(f"[{pair}] DAILY HALT: Loss ${bot['daily_loss']:.2f} >= max ${max_loss_day:.2f}")
        return

    # === IN_POSITION: Manage exits ===
    if npr_state == 'IN_POSITION' and bot.get('position_side') != 'FLAT':
        side = bot['position_side']
        entry_px = bot.get('entry_price', cur_px)
        atr = bot.get('atr_at_entry', 1.0)
        partial = bot.get('partial_filled', False)

        should_exit, exit_reason = npr_get_stop_and_trail(bot, cur_px)
        if should_exit:
            try:
                held = bot['asset_held']
                if bot.get('paper'):
                    actual_exit = cur_px
                    actual_fee = cur_px * held * mult * 0.004
                    log.info(f"[{pair}] PAPER NPR EXIT {side} ({exit_reason}): {held} at ${cur_px:.4f}")
                else:
                    if side == 'LONG':
                        str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                        str_qty = snap_to_increment(held, base_inc)
                        oid = str(uuid.uuid4())
                        client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                    base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                        str_qty = snap_to_increment(held, base_inc)
                        oid = str(uuid.uuid4())
                        client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                                   base_size=str_qty, limit_price=str_price, post_only=True)

                    fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                    actual_exit = fill_px if fill_px else cur_px
                    actual_fee = fill_fee if fill_fee is not None else (actual_exit * held * mult * 0.0025)

                pnl = (actual_exit - entry_px) * held * mult if side == 'LONG' else (entry_px - actual_exit) * held * mult
                record_trade(bot, entry_px, actual_exit, held, side, exit_reason, pair, mult, actual_fee=actual_fee)
                if pnl < 0:
                    bot['daily_loss'] = bot.get('daily_loss', 0) + abs(pnl)
                gross_proceeds = held * actual_exit * mult
                bot['current_usd'] += gross_proceeds - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                bot['npr_state'] = 'SCANNING'
                for key in ['event_stop', 'event_type', 'event_direction', 'event_bar_data',
                            'high_water_mark', 'low_water_mark', 'trail_distance', 'partial_filled',
                            'atr_at_entry', 'pending_order_oid', 'pending_order_time']:
                    bot.pop(key, None)
                save_bots()
                log.info(f"[{pair}] EXIT ({exit_reason}): PnL ${pnl:.2f}")
            except Exception as e:
                log.error(f"[{pair}] Exit order failed: {e}")
            return

        # Activate trailing after breakeven + 0.25 ATR
        if not partial:
            be_reached = (side == 'LONG' and cur_px >= entry_px + 0.25 * atr) or \
                         (side == 'SHORT' and cur_px <= entry_px - 0.25 * atr)
            if be_reached:
                event_power = bot.get('event_power', 1.0)
                entry_zone = abs(bot.get('zone', 1))
                if event_power >= 2.0:
                    bot['trail_distance'] = 1.5 * atr
                elif entry_zone == 3:
                    bot['trail_distance'] = 0.75 * atr
                else:
                    bot['trail_distance'] = 1.0 * atr
                bot['partial_filled'] = True
                if side == 'LONG':
                    bot['event_stop'] = entry_px + float(quote_inc)
                else:
                    bot['event_stop'] = entry_px - float(quote_inc)
                save_bots()
                log.debug(f"[{pair}] Breakeven + trail. dist={bot['trail_distance']:.2f}")
        return

    # === ENTERING: Check pending order ===
    if npr_state == 'ENTERING':
        pending_oid = bot.get('pending_order_oid')
        if not pending_oid:
            bot['npr_state'] = 'SCANNING'
            save_bots()
            return
        elapsed = time.time() - bot.get('pending_order_time', 0)
        retries = bot.get('entry_retries', 0)

        try:
            order_data = client.get("/api/v3/brokerage/orders/historical/batch", params={
                "order_status": "FILLED", "product_id": pair, "limit": 10
            })
            filled = False
            for o in order_data.get('orders', []):
                if o.get('client_order_id') == pending_oid:
                    filled = True
                    filled_size = float(o.get('filled_size', 0))
                    avg_fill_px = float(o.get('average_filled_price', cur_px))
                    break
        except Exception as e:
            log.error(f"[{pair}] Fill check error: {e}")
            return

        if filled and filled_size > 0:
            direction = bot.get('event_direction', 'BULL')
            bot['position_side'] = 'LONG' if direction == 'BULL' else 'SHORT'
            bot['entry_price'] = avg_fill_px
            bot['asset_held'] = filled_size
            bot['npr_state'] = 'IN_POSITION'
            bot['high_water_mark'] = avg_fill_px
            bot['low_water_mark'] = avg_fill_px
            bot['partial_filled'] = False
            for key in ['pending_order_oid', 'pending_order_time', 'entry_retries',
                        'signal_bar_time', '_entry_size', 'entry_bar_start']:
                bot.pop(key, None)
            save_bots()
            log.info(f"[{pair}] FILLED {bot['position_side']}: {filled_size:.8f} at ${avg_fill_px:.2f} "
                     f"Event={bot.get('event_type')} Zone={bot.get('zone')} Score={bot.get('check_score')}")
            return

        if elapsed >= 30:
            try:
                open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
                    "order_status": "OPEN", "limit": 50
                })
                for o in open_res.get('orders', []):
                    if o.get('client_order_id') == pending_oid:
                        real_id = o.get('order_id')
                        if real_id:
                            client.cancel_orders(order_ids=[real_id])
                        break
            except Exception as e:
                log.error(f"[{pair}] Cancel error: {e}")

            bar_start = bot.get('entry_bar_start', 0)
            if retries < 2 and (time.time() - bar_start) < tf_sec:
                direction = bot.get('event_direction', 'BULL')
                entry_size = bot.get('_entry_size', base_min)
                if direction == 'BULL':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                str_qty = snap_to_increment(entry_size, base_inc)
                oid = str(uuid.uuid4())
                try:
                    if direction == 'BULL':
                        client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                                   base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                   base_size=str_qty, limit_price=str_price, post_only=True)
                    bot['pending_order_oid'] = oid
                    bot['pending_order_time'] = time.time()
                    bot['entry_retries'] = retries + 1
                    save_bots()
                    log.debug(f"[{pair}] Retry {retries+1}/2 at ${str_price}")
                except Exception as e:
                    log.error(f"[{pair}] Retry failed: {e}")
                return

            bot['npr_state'] = 'SCANNING'
            for key in ['pending_order_oid', 'pending_order_time', 'entry_retries',
                        'entry_bar_start', 'signal_bar_time', '_entry_size',
                        'event_type', 'event_direction', 'event_stop', 'event_bar_data',
                        'event_power', 'zone', 'check_score', 'position_checks', 'atr_at_entry']:
                bot.pop(key, None)
            save_bots()
            log.warning(f"[{pair}] Entry abandoned")
        return

    # === SCANNING / SIGNAL_WAIT: Detect events ===
    if npr_state in ('SCANNING', 'SIGNAL_WAIT'):
        signal = calculate_npr(df)

        bar_start = (end_ts // tf_sec) * tf_sec
        elapsed_in_bar = end_ts - bar_start
        in_entry_window = elapsed_in_bar >= (tf_sec * 0.66)

        if signal['signal'] != 'HOLD':
            stop_distance = abs(cur_px - signal['event_stop'])
            if stop_distance <= 0:
                return
            position_size = max_loss_trade / (stop_distance * mult) if mult > 0 else 0
            position_cost = position_size * cur_px * mult
            if position_cost > bot['current_usd'] * 0.95:
                position_size = (bot['current_usd'] * 0.95) / (cur_px * mult)
            if position_size < base_min:
                log.warning(f"[{pair}] Size too small for risk. Skip.")
                return

            if in_entry_window:
                direction = signal['event_direction']
                if direction == 'BULL':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                str_qty = snap_to_increment(position_size, base_inc)

                # Paper mode: instant-fill at current price, skip ENTERING state entirely
                if bot.get('paper'):
                    new_side = 'LONG' if direction == 'BULL' else 'SHORT'
                    filled = float(str_qty)
                    gross_cost = cur_px * filled * mult
                    sim_fee = gross_cost * 0.004
                    bot['asset_held'] = filled
                    bot['current_usd'] -= (gross_cost + sim_fee)
                    bot['position_side'] = new_side
                    bot['entry_price'] = cur_px
                    bot['npr_state'] = 'IN_POSITION'
                    bot['high_water_mark'] = cur_px
                    bot['low_water_mark'] = cur_px
                    bot['partial_filled'] = False
                    bot['event_type'] = signal['event_type']
                    bot['event_direction'] = signal['event_direction']
                    bot['event_stop'] = signal['event_stop']
                    bot['event_power'] = signal['event_power']
                    bot['event_bar_data'] = signal['event_bar_data']
                    bot['zone'] = signal['zone']
                    bot['check_score'] = signal['check_score']
                    bot['position_checks'] = signal['position_checks']
                    bot['atr_at_entry'] = signal['atr']
                    save_bots()
                    log.info(f"[{pair}] PAPER NPR ENTRY {new_side}: {filled:.8f} at ${cur_px:.4f} — {signal['reason']}")
                    return

                oid = str(uuid.uuid4())
                try:
                    if direction == 'BULL':
                        api_res = client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                                             base_size=str_qty, limit_price=str_price, post_only=True)
                    else:
                        api_res = client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                             base_size=str_qty, limit_price=str_price, post_only=True)
                    success = getattr(api_res, 'success', False) or (isinstance(api_res, dict) and api_res.get('success', False))
                    fail_reason = getattr(api_res, 'failure_reason', '') or (isinstance(api_res, dict) and api_res.get('failure_reason', ''))
                    if success or fail_reason == 'UNKNOWN_FAILURE_REASON':
                        bot['npr_state'] = 'ENTERING'
                        bot['pending_order_oid'] = oid
                        bot['pending_order_time'] = time.time()
                        bot['entry_retries'] = 0
                        bot['entry_bar_start'] = bar_start
                        bot['event_type'] = signal['event_type']
                        bot['event_direction'] = signal['event_direction']
                        bot['event_stop'] = signal['event_stop']
                        bot['event_power'] = signal['event_power']
                        bot['event_bar_data'] = signal['event_bar_data']
                        bot['zone'] = signal['zone']
                        bot['check_score'] = signal['check_score']
                        bot['position_checks'] = signal['position_checks']
                        bot['atr_at_entry'] = signal['atr']
                        bot['_entry_size'] = position_size
                        save_bots()
                        log.info(f"[{pair}] ENTERING: {signal['reason']}")
                    else:
                        log.error(f"[{pair}] Order rejected: {fail_reason}")
                except Exception as e:
                    log.error(f"[{pair}] Order failed: {e}")
            else:
                if npr_state != 'SIGNAL_WAIT':
                    bot['npr_state'] = 'SIGNAL_WAIT'
                    bot['signal_bar_time'] = float(df['start'].iloc[-1])
                    save_bots()
                    log.debug(f"[{pair}] SIGNAL_WAIT: {signal['reason']} (bar {elapsed_in_bar:.0f}s/{tf_sec}s)")
        elif npr_state == 'SIGNAL_WAIT':
            bot['npr_state'] = 'SCANNING'
            bot.pop('signal_bar_time', None)
            save_bots()


# ==========================================
# VWAP MEAN REVERSION EXECUTOR
# ==========================================
def execute_vwap_mr(bot_id, bot, pair):
    """VWAP_MR: Buy at VWAP - 1σ with RSI<35, sell at VWAP touch. ATR trailing stop."""
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                         params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
    except Exception as e:
        log.error(f"[{pair}] VWAP_MR data fetch error: {e}")
        return

    if len(candles) < 100:
        return

    parsed = [{'open': float(c['open']), 'high': float(c['high']), 'low': float(c['low']),
               'close': float(c['close']), 'volume': float(c.get('volume', 0))} for c in candles]
    df = pd.DataFrame(parsed).sort_values(by=pd.RangeIndex(len(parsed))).reset_index(drop=True)

    signal, reason, atr_val = calculate_vwap_mr(df)
    mult = get_contract_multiplier(pair)
    held = bot.get('asset_held', 0)
    pos_side = bot.get('position_side', 'FLAT')

    if signal == 'BUY' and pos_side == 'FLAT':
        buy_usd = bot['current_usd'] * 0.95
        if buy_usd < 1:
            log.warning(f"[{pair}] VWAP_MR insufficient capital")
            return
        try:
            oid = str(uuid.uuid4())
            client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(buy_usd, 2)))
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_px = fill_px if fill_px else cur_px
            actual_sz = fill_sz if fill_sz else buy_usd / cur_px
            actual_fee = fill_fee if fill_fee is not None else buy_usd * 0.0025
            bot['asset_held'] = actual_sz
            bot['entry_price'] = actual_px
            bot['position_side'] = 'LONG'
            bot['current_usd'] -= (actual_sz * actual_px * mult + actual_fee)
            bot['high_water_mark'] = actual_px
            bot['entry_atr'] = atr_val if atr_val > 0 else actual_px * 0.015
            save_bots()
            log.info(f"[{pair}] VWAP_MR BUY: {actual_sz:.8f} @ ${actual_px:.2f} ({reason})")
            notify_bot_entry(pair, 'VWAP_MR', actual_px, actual_sz)
        except Exception as e:
            log.error(f"[{pair}] VWAP_MR buy failed: {e}")

    elif signal == 'SELL' and pos_side == 'LONG' and held > 0:
        try:
            oid = str(uuid.uuid4())
            str_qty = snap_to_increment(held, base_inc)
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else cur_px
            actual_fee = fill_fee if fill_fee is not None else actual_exit * held * mult * 0.0025
            record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', 'VWAP_TOUCH', pair, mult, actual_fee=actual_fee)
            profit = (actual_exit - bot['entry_price']) * held * mult
            bot['current_usd'] += (held * actual_exit * mult) - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            for k in ['entry_price', 'high_water_mark', 'entry_atr']:
                bot.pop(k, None)
            save_bots()
            log.info(f"[{pair}] VWAP_MR EXIT: PnL ${profit:.2f} ({reason})")
            notify_bot_exit(pair, 'VWAP_MR', actual_exit, profit)
        except Exception as e:
            log.error(f"[{pair}] VWAP_MR sell failed: {e}")

    elif pos_side == 'LONG' and held > 0:
        # ATR trailing stop while in position
        hwm = bot.get('high_water_mark', bot.get('entry_price', cur_px))
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
        entry_atr = bot.get('entry_atr', cur_px * 0.015)
        stop_px = bot['high_water_mark'] - (1.5 * entry_atr)
        if cur_px <= stop_px:
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else actual_exit * held * mult * 0.0025
                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', 'TRAILING_STOP', pair, mult, actual_fee=actual_fee)
                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] += (held * actual_exit * mult) - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                for k in ['entry_price', 'high_water_mark', 'entry_atr']:
                    bot.pop(k, None)
                save_bots()
                log.warning(f"[{pair}] VWAP_MR TRAILING STOP: PnL ${profit:.2f}")
                notify_bot_exit(pair, 'VWAP_MR', actual_exit, profit)
            except Exception as e:
                log.error(f"[{pair}] VWAP_MR stop sell failed: {e}")


# ==========================================
# BOLLINGER SQUEEZE BREAKOUT EXECUTOR
# ==========================================
def execute_squeeze(bot_id, bot, pair):
    """SQUEEZE: Enter on BB/KC squeeze release, exit on momentum reversal or ATR trail."""
    cb_gran, tf_sec = get_bot_tf(bot)
    end_ts = int(time.time())
    start_ts = end_ts - (300 * tf_sec)

    try:
        res = client.get(f"/api/v3/brokerage/products/{pair}/candles",
                         params={"start": str(start_ts), "end": str(end_ts), "granularity": cb_gran})
        candles = res.get('candles', [])
        p_info = client.get_product(product_id=pair)
        cur_px = float(p_info.price)
        base_inc = str(getattr(p_info, 'base_increment', '0.00000001'))
        quote_inc = str(getattr(p_info, 'quote_increment', '0.01'))
    except Exception as e:
        log.error(f"[{pair}] SQUEEZE data fetch error: {e}")
        return

    if len(candles) < 210:
        return

    parsed = [{'open': float(c['open']), 'high': float(c['high']), 'low': float(c['low']),
               'close': float(c['close']), 'volume': float(c.get('volume', 0))} for c in candles]
    df = pd.DataFrame(parsed).sort_values(by=pd.RangeIndex(len(parsed))).reset_index(drop=True)

    signal, reason, atr_val = calculate_squeeze(df)
    mult = get_contract_multiplier(pair)
    held = bot.get('asset_held', 0)
    pos_side = bot.get('position_side', 'FLAT')

    if signal == 'BUY' and pos_side == 'FLAT':
        buy_usd = bot['current_usd'] * 0.95
        if buy_usd < 1:
            log.warning(f"[{pair}] SQUEEZE insufficient capital")
            return
        try:
            oid = str(uuid.uuid4())
            client.market_order_buy(client_order_id=oid, product_id=pair, quote_size=str(round(buy_usd, 2)))
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_px = fill_px if fill_px else cur_px
            actual_sz = fill_sz if fill_sz else buy_usd / cur_px
            actual_fee = fill_fee if fill_fee is not None else buy_usd * 0.0025
            bot['asset_held'] = actual_sz
            bot['entry_price'] = actual_px
            bot['position_side'] = 'LONG'
            bot['current_usd'] -= (actual_sz * actual_px * mult + actual_fee)
            bot['high_water_mark'] = actual_px
            bot['entry_atr'] = atr_val if atr_val > 0 else actual_px * 0.015
            save_bots()
            log.info(f"[{pair}] SQUEEZE BUY: {actual_sz:.8f} @ ${actual_px:.2f} ({reason})")
            notify_bot_entry(pair, 'SQUEEZE', actual_px, actual_sz)
        except Exception as e:
            log.error(f"[{pair}] SQUEEZE buy failed: {e}")

    elif signal == 'EXIT_LONG' and pos_side == 'LONG' and held > 0:
        try:
            oid = str(uuid.uuid4())
            str_qty = snap_to_increment(held, base_inc)
            client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
            fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
            actual_exit = fill_px if fill_px else cur_px
            actual_fee = fill_fee if fill_fee is not None else actual_exit * held * mult * 0.0025
            record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', 'MOMENTUM_REVERSAL', pair, mult, actual_fee=actual_fee)
            profit = (actual_exit - bot['entry_price']) * held * mult
            bot['current_usd'] += (held * actual_exit * mult) - actual_fee
            bot['asset_held'] = 0.0
            bot['position_side'] = 'FLAT'
            for k in ['entry_price', 'high_water_mark', 'entry_atr']:
                bot.pop(k, None)
            save_bots()
            log.info(f"[{pair}] SQUEEZE EXIT: PnL ${profit:.2f} ({reason})")
            notify_bot_exit(pair, 'SQUEEZE', actual_exit, profit)
        except Exception as e:
            log.error(f"[{pair}] SQUEEZE sell failed: {e}")

    elif pos_side == 'LONG' and held > 0:
        # ATR trailing stop
        hwm = bot.get('high_water_mark', bot.get('entry_price', cur_px))
        if cur_px > hwm:
            bot['high_water_mark'] = cur_px
        entry_atr = bot.get('entry_atr', cur_px * 0.015)
        stop_px = bot['high_water_mark'] - (2.0 * entry_atr)
        if cur_px <= stop_px:
            try:
                oid = str(uuid.uuid4())
                str_qty = snap_to_increment(held, base_inc)
                client.market_order_sell(client_order_id=oid, product_id=pair, base_size=str_qty)
                fill_px, fill_sz, fill_fee = poll_market_fill(oid, pair)
                actual_exit = fill_px if fill_px else cur_px
                actual_fee = fill_fee if fill_fee is not None else actual_exit * held * mult * 0.0025
                record_trade(bot, bot['entry_price'], actual_exit, held, 'LONG', 'TRAILING_STOP', pair, mult, actual_fee=actual_fee)
                profit = (actual_exit - bot['entry_price']) * held * mult
                bot['current_usd'] += (held * actual_exit * mult) - actual_fee
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                for k in ['entry_price', 'high_water_mark', 'entry_atr']:
                    bot.pop(k, None)
                save_bots()
                log.warning(f"[{pair}] SQUEEZE TRAILING STOP: PnL ${profit:.2f}")
                notify_bot_exit(pair, 'SQUEEZE', actual_exit, profit)
            except Exception as e:
                log.error(f"[{pair}] SQUEEZE stop sell failed: {e}")
