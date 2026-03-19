import sys, shutil, os

FILE = "routes/bot_manager.py"
if not os.path.exists(FILE):
    FILE = "bot_manager.py"
if not os.path.exists(FILE):
    print("ERROR: Cannot find bot_manager.py")
    sys.exit(1)

shutil.copy(FILE, FILE + ".bak_deadband")
print("Backed up to " + FILE + ".bak_deadband")

with open(FILE, "r") as f:
    code = f.read()

patches_applied = 0

OLD_HALT_END = "    risk['halt_mode'] = halt_mode\n    save_bots()\n\n# ==========================================\n# GRID FOLLOW ENGINE"

NEW_HALT_END = "    risk['halt_mode'] = halt_mode\n    risk['halt_trigger_price'] = cur_px\n    risk['direction_streak'] = 0\n    risk['last_streak_direction'] = direction\n    save_bots()\n\n# ==========================================\n# GRID FOLLOW ENGINE"

if OLD_HALT_END in code:
    code = code.replace(OLD_HALT_END, NEW_HALT_END, 1)
    patches_applied += 1
    print("PATCH 1: halt_trigger_price storage - APPLIED")
else:
    print("PATCH 1: SKIPPED (already applied or code differs)")

OLD_HALT_SHIFT = """    # --- 8. Halted state management ---
    if is_halted:
        halt_mode = risk.get('halt_mode', 'NEUTRAL')

        # Direction can shift each cycle -> update halt mode
        if direction == 'RISING':
            halt_mode = 'FAVORABLE'
        elif direction == 'FALLING':
            halt_mode = 'ADVERSE'
        else:
            halt_mode = 'NEUTRAL'
        risk['halt_mode'] = halt_mode"""

NEW_HALT_SHIFT = """    # --- 8. Halted state management (with deadband) ---
    if is_halted:
        halt_mode = risk.get('halt_mode', 'NEUTRAL')
        halt_trigger_px = risk.get('halt_trigger_price', cur_px)
        streak = risk.get('direction_streak', 0)
        last_dir = risk.get('last_streak_direction', 'CHOPPY')

        # Track consecutive direction readings
        if direction == last_dir and direction != 'CHOPPY':
            streak += 1
        elif direction != last_dir:
            streak = 1 if direction != 'CHOPPY' else 0
        risk['direction_streak'] = streak
        risk['last_streak_direction'] = direction

        # Deadband state machine
        new_mode = halt_mode

        if halt_mode == 'ADVERSE':
            if cur_px > halt_trigger_px:
                new_mode = 'NEUTRAL'
                risk['direction_streak'] = 0
                print(f"[DEADBAND | {pair}] ADVERSE -> NEUTRAL: price {cur_px:.2f} > trigger {halt_trigger_px:.2f}")

        elif halt_mode == 'NEUTRAL':
            if direction == 'RISING' and streak >= 3:
                new_mode = 'FAVORABLE'
                risk['direction_streak'] = 0
                print(f"[DEADBAND | {pair}] NEUTRAL -> FAVORABLE: 3 consecutive RISING")
            elif cur_px < halt_trigger_px or (direction == 'FALLING' and streak >= 2):
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                reason = f"price {cur_px:.2f} < trigger {halt_trigger_px:.2f}" if cur_px < halt_trigger_px else "2 consecutive FALLING"
                print(f"[DEADBAND | {pair}] NEUTRAL -> ADVERSE: {reason}")

        elif halt_mode == 'FAVORABLE':
            if direction == 'FALLING' and streak >= 3:
                new_mode = 'ADVERSE'
                risk['direction_streak'] = 0
                risk['halt_trigger_price'] = cur_px
                print(f"[DEADBAND | {pair}] FAVORABLE -> ADVERSE: 3 consecutive FALLING (fast path)")
            elif direction == 'FALLING' and streak >= 2:
                new_mode = 'NEUTRAL'
                risk['direction_streak'] = 0
                print(f"[DEADBAND | {pair}] FAVORABLE -> NEUTRAL: 2 consecutive FALLING")

        halt_mode = new_mode
        risk['halt_mode'] = halt_mode"""

if OLD_HALT_SHIFT in code:
    code = code.replace(OLD_HALT_SHIFT, NEW_HALT_SHIFT, 1)
    patches_applied += 1
    print("PATCH 2: Deadband halt mode transitions - APPLIED")
else:
    print("PATCH 2: SKIPPED (already applied or code differs)")

with open(FILE, "w") as f:
    f.write(code)

print(f"\nDone. {patches_applied}/2 patches applied.")
if patches_applied > 0:
    print("Restart the service: sudo systemctl restart cryptoterminal")
