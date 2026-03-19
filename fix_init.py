# Fix the broken INIT section in bot_manager.py

import sys

with open(sys.argv[1], 'r') as f:
code = f.read()

# The broken section: recenter logic got nested inside elif mode == 'SHORT'

BROKEN = """    if mode == 'LONG':
sell_levels = []
elif mode == 'SHORT':
buy_levels = []
if not deriv_flag:
print(f"[GRID BOT | {pair}] WARNING: SHORT mode on spot requires inventory.")

```
    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        print(f"[GRID BOT | {pair}] No valid levels in old range. Auto-recentering on {cur_px:.2f}")
        grid_count = settings.get('grid_count', max(2, int(bot['current_usd'] / settings.get('min_order_usd', 5))))
    if mode == 'LONG':
        lower = cur_px - (grid_count * step)
        upper = cur_px
    elif mode == 'SHORT':
        lower = cur_px
        upper = cur_px + (grid_count * step)
    else:
        half = grid_count // 2
        lower = cur_px - (half * step)
        upper = cur_px + ((grid_count - half) * step)
    settings['lower_price'] = lower
    settings['upper_price'] = upper
    buy_levels, sell_levels = [], []
    level = lower
    while level <= upper:
        if level < cur_px * 0.999: buy_levels.append(level)
        elif level > cur_px * 1.001: sell_levels.append(level)
        level += step
    if mode == 'LONG': sell_levels = []
    elif mode == 'SHORT': buy_levels = []
    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        print(f"[GRID BOT | {pair}] Still no valid levels after recenter. Waiting.")
        return
    print(f"[GRID BOT | {pair}] Recentered: {lower:.2f} - {upper:.2f}, {total_orders} levels")


chunk_size_usd = bot['current_usd'] / total_orders"""
```

FIXED = """    if mode == 'LONG':
sell_levels = []
elif mode == 'SHORT':
buy_levels = []
if not deriv_flag:
print(f"[GRID BOT | {pair}] WARNING: SHORT mode on spot requires inventory.")

```
total_orders = len(buy_levels) + len(sell_levels)
if total_orders == 0:
    print(f"[GRID BOT | {pair}] No valid levels in old range. Auto-recentering on {cur_px:.2f}")
    grid_count = settings.get('grid_count', max(2, int(bot['current_usd'] / settings.get('min_order_usd', 5))))
    if mode == 'LONG':
        lower = cur_px - (grid_count * step)
        upper = cur_px
    elif mode == 'SHORT':
        lower = cur_px
        upper = cur_px + (grid_count * step)
    else:
        half = grid_count // 2
        lower = cur_px - (half * step)
        upper = cur_px + ((grid_count - half) * step)
    settings['lower_price'] = lower
    settings['upper_price'] = upper
    buy_levels, sell_levels = [], []
    level = lower
    while level <= upper:
        if level < cur_px * 0.999: buy_levels.append(level)
        elif level > cur_px * 1.001: sell_levels.append(level)
        level += step
    if mode == 'LONG': sell_levels = []
    elif mode == 'SHORT': buy_levels = []
    total_orders = len(buy_levels) + len(sell_levels)
    if total_orders == 0:
        print(f"[GRID BOT | {pair}] Still no valid levels after recenter. Waiting.")
        return
    print(f"[GRID BOT | {pair}] Recentered: {lower:.2f} - {upper:.2f}, {total_orders} levels")

chunk_size_usd = bot['current_usd'] / total_orders"""
```

if BROKEN in code:
code = code.replace(BROKEN, FIXED, 1)
with open(sys.argv[1], 'w') as f:
f.write(code)
print("FIXED: Recenter logic dedented to correct scope")
else:
print("ERROR: Could not find broken section. Dumping context…")
# Try to find it partially
if "total_orders = len(buy_levels) + len(sell_levels)" in code:
idx = code.index("total_orders = len(buy_levels) + len(sell_levels)")
print(f"Found total_orders at char {idx}")
print("Context:")
print(code[max(0,idx-200):idx+200])
else:
print("total_orders line not found at all")
