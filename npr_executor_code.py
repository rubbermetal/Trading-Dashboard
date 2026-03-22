

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
    except Exception as e:
        print(f"[NPR | {pair}] Data fetch error: {e}")
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
            print(f"[NPR | {pair}] New UTC day -- resuming from DAILY_HALT")
        save_bots()

    if bot.get('daily_loss', 0) >= max_loss_day:
        if npr_state != 'DAILY_HALT':
            bot['npr_state'] = 'DAILY_HALT'
            save_bots()
            print(f"[NPR | {pair}] DAILY HALT: Loss ${bot['daily_loss']:.2f} >= max ${max_loss_day:.2f}")
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
                if side == 'LONG':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                    str_qty = snap_to_increment(bot['asset_held'], base_inc)
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_sell(client_order_id=oid, product_id=pair,
                                                base_size=str_qty, limit_price=str_price, post_only=True)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                    str_qty = snap_to_increment(bot['asset_held'], base_inc)
                    oid = str(uuid.uuid4())
                    client.limit_order_gtc_buy(client_order_id=oid, product_id=pair,
                                               base_size=str_qty, limit_price=str_price, post_only=True)
                pnl = (cur_px - entry_px) * bot['asset_held'] * mult if side == 'LONG' else (entry_px - cur_px) * bot['asset_held'] * mult
                record_trade(bot, entry_px, cur_px, bot['asset_held'], side, exit_reason, pair, mult)
                if pnl < 0:
                    bot['daily_loss'] = bot.get('daily_loss', 0) + abs(pnl)
                bot['current_usd'] += bot['asset_held'] * cur_px * mult * 0.9975
                bot['asset_held'] = 0.0
                bot['position_side'] = 'FLAT'
                bot['npr_state'] = 'SCANNING'
                for key in ['event_stop', 'event_type', 'event_direction', 'event_bar_data',
                            'high_water_mark', 'low_water_mark', 'trail_distance', 'partial_filled',
                            'atr_at_entry', 'pending_order_oid', 'pending_order_time']:
                    bot.pop(key, None)
                save_bots()
                print(f"[NPR | {pair}] EXIT ({exit_reason}): PnL ${pnl:.2f}")
            except Exception as e:
                print(f"[NPR | {pair}] Exit order failed: {e}")
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
                print(f"[NPR | {pair}] Breakeven + trail. dist={bot['trail_distance']:.2f}")
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
            print(f"[NPR | {pair}] Fill check error: {e}")
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
            print(f"[NPR | {pair}] FILLED {bot['position_side']}: {filled_size:.8f} at ${avg_fill_px:.2f} "
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
                print(f"[NPR | {pair}] Cancel error: {e}")

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
                    print(f"[NPR | {pair}] Retry {retries+1}/2 at ${str_price}")
                except Exception as e:
                    print(f"[NPR | {pair}] Retry failed: {e}")
                return

            bot['npr_state'] = 'SCANNING'
            for key in ['pending_order_oid', 'pending_order_time', 'entry_retries',
                        'entry_bar_start', 'signal_bar_time', '_entry_size',
                        'event_type', 'event_direction', 'event_stop', 'event_bar_data',
                        'event_power', 'zone', 'check_score', 'position_checks', 'atr_at_entry']:
                bot.pop(key, None)
            save_bots()
            print(f"[NPR | {pair}] Entry abandoned")
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
                print(f"[NPR | {pair}] Size too small for risk. Skip.")
                return

            if in_entry_window:
                direction = signal['event_direction']
                if direction == 'BULL':
                    str_price = snap_to_increment(cur_px - float(quote_inc), quote_inc)
                else:
                    str_price = snap_to_increment(cur_px + float(quote_inc), quote_inc)
                str_qty = snap_to_increment(position_size, base_inc)
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
                        print(f"[NPR | {pair}] ENTERING: {signal['reason']}")
                    else:
                        print(f"[NPR | {pair}] Order rejected: {fail_reason}")
                except Exception as e:
                    print(f"[NPR | {pair}] Order failed: {e}")
            else:
                if npr_state != 'SIGNAL_WAIT':
                    bot['npr_state'] = 'SIGNAL_WAIT'
                    bot['signal_bar_time'] = float(df['start'].iloc[-1])
                    save_bots()
                    print(f"[NPR | {pair}] SIGNAL_WAIT: {signal['reason']} (bar {elapsed_in_bar:.0f}s/{tf_sec}s)")
        elif npr_state == 'SIGNAL_WAIT':
            bot['npr_state'] = 'SCANNING'
            bot.pop('signal_bar_time', None)
            save_bots()
