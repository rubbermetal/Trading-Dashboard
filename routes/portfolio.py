import uuid, time
from flask import Blueprint, jsonify, request
from shared import client, MANUAL_SPOT_ENTRIES, REBALANCE_TARGETS, TRAILING_STOPS, ACTIVE_BOTS

portfolio_bp = Blueprint('portfolio', __name__)

def fetch_data():
    active, history, total = [], [], 0.0
    spot_map = {}
    total_usd_balance = 0.0 # Track raw stablecoin/fiat buying power
    
    # --- 1. Spot Positions Fetch ---
    try:
        accs = client.get_accounts(limit=100)
        for a in accs.accounts:
            avail = float(a.available_balance.get('value', '0'))
            hold = float(a.hold.get('value', '0')) if hasattr(a, 'hold') and a.hold else 0.0
            val = avail + hold
            
            if val > 0.000001:
                currency = a.currency.upper()
                
                if currency in ['USD', 'USDC', 'USDT']:
                    total += val  
                    total_usd_balance += val # Accumulate buying power
                    continue      
                
                prod_id = f"{currency}-USD"
                try:
                    p = client.get_product(product_id=prod_id)
                    px = float(p.price)
                except: px = 0.0
                    
                usd = val * px
                total += usd
                spot_map[currency] = {"val": val, "usd": usd, "px": px}
                
                entry_px = "N/A"
                pnl_str = ""
                
                if usd > 0.05: 
                    try:
                        fills = client.get("/api/v3/brokerage/orders/historical/batch", params={"product_id": prod_id, "order_status": "FILLED", "limit": 100})
                        buy_orders = [o for o in fills.get('orders', []) if o.get('side', '').upper() == 'BUY']
                        
                        if buy_orders:
                            accumulated_size, total_cost = 0.0, 0.0
                            for o in buy_orders:
                                o_size = float(o.get('filled_size', o.get('base_size', 0)))
                                o_px = float(o.get('average_filled_price', o.get('price', 0)))
                                
                                if o_size > 0 and o_px > 0:
                                    needed = min(o_size, val - accumulated_size)
                                    if needed > 0:
                                        accumulated_size += needed
                                        total_cost += needed * o_px
                                    
                                if accumulated_size >= val * 0.99: break
                            
                            if accumulated_size > 0:
                                entry_px = total_cost / accumulated_size
                    except: pass
                    
                    if entry_px == "N/A" and currency in MANUAL_SPOT_ENTRIES:
                        entry_px = float(MANUAL_SPOT_ENTRIES[currency])

                    if entry_px != "N/A":
                        pnl_val = (px - entry_px) * val
                        pnl_str = f"{'+' if pnl_val > 0 else ''}${pnl_val:.2f}"
                
                active.append({
                    'type': 'SPOT', 'asset': currency, 'size': f"{val:g}", 
                    'price': px, 'entry_price': entry_px, 'usd_value': usd, 
                    'liquidation': 'N/A', 'pnl': pnl_str
                })
    except Exception as e: print(f"Spot Error: {e}")

    # --- 2. CFM Margin Fetch ---
    cfm_equity, maint_margin = 0.0, 0.0
    try:
        cfm_res = client.get("/api/v3/brokerage/cfm/balance_summary")
        if isinstance(cfm_res, dict):
            summary = cfm_res.get('balance_summary', {})
            cfm_equity = float(summary.get('total_equity', {}).get('value', 0))
            maint_margin = float(summary.get('maintenance_margin_total', {}).get('value', 0))
    except: pass

    if cfm_equity == 0:
        try:
            res = client.get_futures_balance_summary()
            if hasattr(res, 'balance_summary'):
                eq = getattr(res.balance_summary, 'total_equity', None)
                mm = getattr(res.balance_summary, 'maintenance_margin_total', None)
                if eq: cfm_equity = float(getattr(eq, 'value', 0))
                if mm: maint_margin = float(getattr(mm, 'value', 0))
        except: pass

    base_equity = cfm_equity if cfm_equity > 0 else total

    # --- 3. Derivative Positions ---
    try:
        f_data = client.get("/api/v3/brokerage/cfm/positions")
        for pos in f_data.get('positions', []):
            qty = float(pos.get('number_of_contracts', 0))
            if qty != 0:
                pnl = float(pos.get('unrealized_pnl', 0))
                cur_px = float(pos.get('current_price', 0))
                entry_px = float(pos.get('avg_entry_price', cur_px))
                if entry_px == 0: entry_px = cur_px
                
                side = pos.get('side', '').upper()
                asset = pos.get('product_id', '')
                
                multiplier = 1.0
                if 'BTC' in asset: multiplier = 0.01
                elif 'ETH' in asset: multiplier = 0.1
                elif 'DOGE' in asset: multiplier = 100.0
                elif 'SHIB' in asset or 'PEPE' in asset: multiplier = 1000000.0
                elif 'BCH' in asset or 'LTC' in asset: multiplier = 1.0
                
                actual_size = qty * multiplier
                est_liq = "N/A"
                
                if actual_size > 0:
                    static_equity = base_equity - pnl
                    if static_equity <= 0: static_equity = (actual_size * cur_px * 0.05)
                        
                    safe_buffer = (static_equity - maint_margin) if maint_margin > 0 else static_equity

                    if side == 'LONG':
                        liq_val = entry_px - (safe_buffer / actual_size)
                        est_liq = f"${max(0, liq_val):,.2f}"
                    else:
                        liq_val = entry_px + (safe_buffer / actual_size)
                        est_liq = f"${max(0, liq_val):,.2f}"

                active.append({
                    'type': 'DERIVATIVE', 'asset': asset, 'size': f"{qty:g}", 'side': side, 
                    'price': cur_px, 'entry_price': entry_px, 'usd_value': pnl, 
                    'pnl': f"{'+' if pnl > 0 else ''}${pnl:.2f}", 'liquidation': est_liq
                })
    except Exception as e: print(f"Derivative Error: {e}")

    # --- 4. Recent Order History (For the main dashboard snippet) ---
    try:
        orders_raw = client.get("/api/v3/brokerage/orders/historical/batch", params={"limit": 10})
        for o in orders_raw.get('orders', []):
            history.append({
                'pair': o.get('product_id'), 'side': o.get('side'), 'status': o.get('status'),
                'time': o.get('created_time')[:16].replace('T', ' ')
            })
    except: pass
        
    active.sort(key=lambda x: x['usd_value'], reverse=True)
    display_total = base_equity if base_equity > 0 else total
    return active, display_total, history, spot_map, total_usd_balance

@portfolio_bp.route('/api/data')
def api_data():
    pos, total, hist, spot_map, total_usd_balance = fetch_data()
    
    # Calculate Bot liquidity isolation
    bot_locked_usd = sum(bot.get('current_usd', 0.0) for bot in ACTIVE_BOTS.values())
    free_usd = max(0.0, total_usd_balance - bot_locked_usd)

    reb = []
    if REBALANCE_TARGETS:
        for asset, target_pct in REBALANCE_TARGETS.items():
            current_usd = spot_map.get(asset, {}).get('usd', 0)
            actual_pct = current_usd / total if total > 0 else 0
            diff_pct = actual_pct - target_pct
            diff_usd = current_usd - (total * target_pct)
            
            status = "Balanced"
            action = "None"
            
            if diff_pct > 0.05:
                status = "Over"
                action = f"Sell ${abs(diff_usd):.2f}"
            elif diff_pct < -0.05:
                status = "Under"
                action = f"Buy ${abs(diff_usd):.2f}"
                
            reb.append({
                "asset": asset, "target": f"{target_pct*100}%", "actual": f"{actual_pct*100:.1f}%", 
                "status": status, "action": action
            })

    return jsonify(
        positions=pos, total_value=total, history=hist, rebalance=reb, 
        rebalance_configured=bool(REBALANCE_TARGETS),
        current_targets={k: v*100 for k,v in REBALANCE_TARGETS.items()},
        trails=list(TRAILING_STOPS.keys()),
        free_usd=free_usd,
        bot_locked_usd=bot_locked_usd
    )

@portfolio_bp.route('/api/config_rebalance', methods=['POST'])
def config_rebalance():
    d = request.json
    try:
        total_pct = sum(float(v) for v in d.values() if v)
        if not (99.0 <= total_pct <= 101.0): return jsonify(success=False, error="Total percentage must equal exactly 100%.")
        
        REBALANCE_TARGETS.clear()
        REBALANCE_TARGETS.update({k.upper(): float(v)/100 for k, v in d.items() if float(v) > 0})
        return jsonify(success=True, message="Rebalance configuration saved!")
    except Exception as e: return jsonify(success=False, error=str(e))

@portfolio_bp.route('/api/execute_rebalance', methods=['POST'])
def execute_rebalance():
    if not REBALANCE_TARGETS: return jsonify(success=False, error="Configure target percentages first.")
        
    try:
        _, total, _, spot_map, _ = fetch_data()
        sells, buys, msgs = [], [], []
        
        for asset, target_pct in REBALANCE_TARGETS.items():
            current_usd = spot_map.get(asset, {}).get('usd', 0)
            px = spot_map.get(asset, {}).get('px', 0)
            if px == 0: continue
            
            diff_usd = current_usd - (total * target_pct)
            actual_pct = current_usd / total if total > 0 else 0
            
            if abs(actual_pct - target_pct) > 0.05 and abs(diff_usd) > 5.0:
                if diff_usd > 0: sells.append({"pair": f"{asset}-USD", "size": str(round(diff_usd / px, 6))})
                else: buys.append({"pair": f"{asset}-USD", "size": str(round(abs(diff_usd), 2))})
        
        for s in sells:
            client.market_order_sell(client_order_id=str(uuid.uuid4()), product_id=s['pair'], base_size=s['size'])
            msgs.append(f"Sold {s['size']} {s['pair']}")
            
        if sells and buys: time.sleep(2) 
            
        for b in buys:
            client.market_order_buy(client_order_id=str(uuid.uuid4()), product_id=b['pair'], quote_size=b['size'])
            msgs.append(f"Bought ${b['size']} of {b['pair']}")
            
        if not msgs: return jsonify(success=True, message="Portfolio is already balanced.")
        return jsonify(success=True, message=" | ".join(msgs))
    except Exception as e: return jsonify(success=False, error=str(e))

# ==========================================
# DEDICATED ORDERS TAB API
# ==========================================
@portfolio_bp.route('/api/orders', methods=['GET'])
def get_all_orders():
    """Fetches OPEN and HISTORICAL orders separately to bypass Coinbase API 400 errors."""
    combined_orders = []
    
    # 1. Fetch strictly OPEN orders
    try:
        open_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": "OPEN", 
            "limit": 50
        })
        for o in open_res.get('orders', []):
            combined_orders.append(format_order_data(o))
    except Exception as e:
        print(f"Error fetching OPEN orders: {e}")

    # 2. Fetch Historical orders (FILLED, CANCELLED, FAILED)
    try:
        hist_res = client.get("/api/v3/brokerage/orders/historical/batch", params={
            "order_status": ["FILLED", "CANCELLED", "FAILED"], 
            "limit": 50
        })
        for o in hist_res.get('orders', []):
            combined_orders.append(format_order_data(o))
    except Exception as e:
        print(f"Error fetching historical orders: {e}")

    # Sort the combined list by time, newest first
    combined_orders.sort(key=lambda x: x['raw_time'], reverse=True)
    return jsonify(combined_orders)

def format_order_data(o):
    """Helper to cleanly parse Coinbase API order objects for the frontend."""
    o_type = o.get('order_configuration', {})
    price = "Market"
    size = o.get('base_size', o.get('quote_size', '0'))
    
    if 'limit_limit_gtc' in o_type:
        price = f"${float(o_type['limit_limit_gtc'].get('limit_price', 0)):,.4f}"
        size = o_type['limit_limit_gtc'].get('base_size', size)
    elif 'stop_limit_stop_limit_gtc' in o_type:
        price = f"Stop: ${float(o_type['stop_limit_stop_limit_gtc'].get('stop_price', 0)):,.4f}"
        size = o_type['stop_limit_stop_limit_gtc'].get('base_size', size)

    return {
        'id': o.get('order_id'),
        'pair': o.get('product_id'),
        'side': o.get('side', '').upper(),
        'status': o.get('status', '').upper(),
        'price': price,
        'size': float(size) if size else 0,
        'filled': float(o.get('filled_size', 0)),
        'raw_time': o.get('created_time', ''),
        'time': o.get('created_time', '')[:16].replace('T', ' ')
    }

@portfolio_bp.route('/api/orders/cancel', methods=['POST'])
def cancel_order():
    """Cancels a specific active order."""
    d = request.json
    order_id = d.get('order_id')
    if not order_id:
        return jsonify(success=False, error="No order ID provided.")
    
    try:
        res = client.cancel_orders(order_ids=[order_id])
        results = res.get('results', [])
        if results and results[0].get('success'):
            return jsonify(success=True, message="Order successfully cancelled.")
        else:
            return jsonify(success=False, error=results[0].get('failure_reason', 'Cancellation failed.'))
    except Exception as e:
        return jsonify(success=False, error=str(e))
