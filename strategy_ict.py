"""
STRATEGY 4 (live id ICT_NDOG) — ICT Opening-Gap BREAKOUT
========================================================
Faithful live port of ict_gap_v2_backtest.py (the file provided).

NOTE on variant: the v2 file is a gap BREAKOUT (market entry when a candle
closes beyond the gap edge), NOT the midpoint-fade described in the spec md.
This port follows the actual code. Live config (from the v2 sweep grid):
  exit_mode = 'tp'  ->  fixed swing-anchored SL (capped at sl_pct) + fixed TP
  sl_pct = 0.5, tp_pct = 1.5, session filter OFF (crypto trades 24/7)

LOGIC (per symbol, per UTC day = NDOG):
  - At each new UTC day, define the gap from the previous candle's close and the
    day's first open:  gap_top = max(prev_close, open), gap_btm = min(...).
  - While price is inside the gap and flat, track a swing anchor:
        close > gap_top -> swing = min(lows seen)   (long stop anchor)
        close < gap_btm -> swing = max(highs seen)   (short stop anchor)
  - BREAKOUT entry (market on the breakout candle's close):
        LONG  : close > gap_top  and the prior candle's low  <= gap_top
        SHORT : close < gap_btm  and the prior candle's high >= gap_btm
  - One trade per UTC day per symbol.
  - SL = nearer of (swing, entry +/- sl_pct);  TP = entry +/- tp_pct.
"""

import logging
from strategy_base import BaseStrategy, SignalEvent, fmt_ts

log = logging.getLogger('strategy.ict')

MS_DAY = 24 * 60 * 60 * 1000   # UTC day in ms


class ICTStrategy(BaseStrategy):

    def _new_state(self, symbol):
        return {'period': None, 'gap_top': None, 'gap_btm': None,
                'swing': None, 'trade_taken': False, 'last_ts': 0}

    def on_candle_close(self, symbol, candle, ind, candle_list):
        if not self.owns(symbol):
            return

        st = self.state(symbol)
        ts = candle['t']
        if ts <= st['last_ts']:
            return
        st['last_ts'] = ts

        o, hi, lo, c = candle['o'], candle['h'], candle['l'], candle['c']

        # previous candle (for gap definition + prior-edge breakout test)
        if len(candle_list) >= 2:
            prev = candle_list[-2]
            prev_close, prev_low, prev_high = prev['c'], prev['l'], prev['h']
        else:
            prev_close, prev_low, prev_high = o, None, None

        # ── new UTC day -> redefine the gap, reset the day's trade ────────────
        period = ts // MS_DAY
        if period != st['period']:
            st['period']      = period
            st['gap_top']     = max(prev_close, o)
            st['gap_btm']     = min(prev_close, o)
            st['trade_taken'] = False
            st['swing']       = None

        # While a position is open, do not track swing or enter (matches backtest).
        if self.is_open(symbol):
            return

        gap_top, gap_btm = st['gap_top'], st['gap_btm']
        if gap_top is None:
            return

        # ── swing anchor: track while price pokes outside the gap ─────────────
        in_gap = (hi >= gap_btm and lo <= gap_top)
        if in_gap:
            if c > gap_top:
                st['swing'] = lo if st['swing'] is None else min(st['swing'], lo)
            elif c < gap_btm:
                st['swing'] = hi if st['swing'] is None else max(st['swing'], hi)

        # ── breakout entry ────────────────────────────────────────────────────
        can_trade = (not st['trade_taken']) and (st['swing'] is not None)
        long_cond  = (can_trade and c > gap_top and prev_low  is not None and prev_low  <= gap_top)
        short_cond = (can_trade and c < gap_btm and prev_high is not None and prev_high >= gap_btm)

        direction = 'LONG' if long_cond else ('SHORT' if short_cond else None)
        if direction is None:
            return

        entry  = c                      # indicative; order manager re-prices at fill
        swing  = st['swing']
        sl_pct = self.p['sl_pct']       # 0.5
        tp_pct = self.p['tp_pct']       # 1.5

        if direction == 'LONG':
            cap_sl   = entry * (1 - sl_pct / 100.0)
            sl_price = max(swing, cap_sl)            # nearer (tighter) of the two
            tp_price = entry * (1 + tp_pct / 100.0)
        else:
            cap_sl   = entry * (1 + sl_pct / 100.0)
            sl_price = min(swing, cap_sl)
            tp_price = entry * (1 - tp_pct / 100.0)

        # Guard against a degenerate stop (swing on the wrong side / zero risk).
        if (direction == 'LONG' and sl_price >= entry) or (direction == 'SHORT' and sl_price <= entry):
            log.warning(f"{symbol} ICT: degenerate SL ({sl_price:.6f} vs entry {entry:.6f}) — skipping")
            return

        st['trade_taken'] = True
        st['swing']       = None

        sig = SignalEvent(
            strategy_id = self.strategy_id,
            symbol      = symbol,
            direction   = direction,
            entry_type  = self.entry_type,      # MARKET
            signal_price= entry,
            sl_price    = sl_price,
            tp_price    = tp_price,
            signal_ts   = ts,
            signal_time = fmt_ts(ts),
            reason      = (f"NDOG breakout {direction} | gap=[{gap_btm:.6f},{gap_top:.6f}] "
                          f"swing={swing:.6f}"),
            exit_model  = self.exit_model,      # FIXED
            exit_plan   = {'sl_price': sl_price, 'tp_price': tp_price},
            indicators  = {'gap_top': gap_top, 'gap_btm': gap_btm, 'swing': swing},
        )
        self._emit(sig)
