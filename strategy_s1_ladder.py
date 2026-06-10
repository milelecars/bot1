"""
STRATEGY 5 (live id S1_EMA_CROSS) — EMA 9/26 Cross + Trailing Ladder
====================================================================
Faithful live port of BackTest_S1_ladder_2000d.py's entry logic.

ENTRY (all read from the engine's indicator snapshot — exact-parity formulas):
  - EMA9/EMA26 cross:  bullish (e9_prev<=e26_prev and e9>e26) -> LONG candidate
                       bearish (e9_prev>=e26_prev and e9<e26) -> SHORT candidate
  - the pending cross must confirm within 2 bars (ts <= cross_ts + 2*15m)
  - confirmation, ALL must pass:
      F2: candle closes in the cross direction AND beyond both EMA9 and EMA26
      F3: close on the correct side of EMA200
      F4: ADX > 25.0 (strict)
      F5: DI+ > DI- for LONG ; DI- > DI+ for SHORT
      F6: MACD agrees (LONG: macd>signal and hist>0 ; SHORT: macd<signal and hist<0)
  - backtest enters at the next candle open; live fires a market order on the
    signal-candle close (the order manager re-prices at fill and anchors SL/TP).

EXIT — trailing ladder (managed by the order manager's LADDER model):
  - initial hard stop 0.5% from entry
  - rungs at 0.5 / 0.7 / 1.0 / 1.5 / 1.9%; reaching a rung ratchets the stop to
    the PREVIOUS rung (reaching the first rung moves the stop to break-even)
  - the final rung (+1.9%) is a hard take-profit CAP
"""

import logging
from strategy_base import BaseStrategy, SignalEvent, CANDLE_MS, fmt_ts

log = logging.getLogger('strategy.s1')

# Signal-only thresholds (live in the detector, per the registry/detector split).
S1_ADX_MIN     = 25.0     # ADX must be strictly greater than this
PENDING_BARS   = 2        # pending cross expires after 2 bars

REQUIRED = ['ema9', 'ema26', 'ema200', 'ema9_prev', 'ema26_prev',
            'adx', 'di_plus', 'di_minus', 'macd', 'macd_signal', 'macd_hist']


class S1LadderStrategy(BaseStrategy):

    def _new_state(self, symbol):
        return {'pending_dir': None, 'pending_ts': None, 'last_ts': 0}

    def on_candle_close(self, symbol, candle, ind, candle_list):
        if not self.owns(symbol):
            return
        if any(ind.get(k) is None for k in REQUIRED):
            return

        st = self.state(symbol)
        ts = candle['t']
        if ts <= st['last_ts']:
            return
        st['last_ts'] = ts

        # While a position is open on this symbol, the backtest does no cross
        # detection and holds no pending — mirror that.
        if self.is_open(symbol):
            st['pending_dir'] = None
            st['pending_ts'] = None
            return

        e9, e9p = ind['ema9'], ind['ema9_prev']
        e26, e26p = ind['ema26'], ind['ema26_prev']
        e200 = ind['ema200']
        adx, dip, din = ind['adx'], ind['di_plus'], ind['di_minus']
        mac, macs, mach = ind['macd'], ind['macd_signal'], ind['macd_hist']
        cc, co = candle['c'], candle['o']

        # 1) detect a fresh cross (this resets any existing pending, as in backtest)
        bx  = (e9p <= e26p) and (e9 > e26)     # bullish cross
        brx = (e9p >= e26p) and (e9 < e26)     # bearish cross
        if bx or brx:
            st['pending_dir'] = 'LONG' if bx else 'SHORT'
            st['pending_ts']  = ts

        if st['pending_dir'] is None:
            return

        d = st['pending_dir']

        # 2) expire pending after 2 bars
        if ts > st['pending_ts'] + PENDING_BARS * CANDLE_MS:
            st['pending_dir'] = None
            st['pending_ts']  = None
            return

        # 3) confirmation filters (all must pass)
        if d == 'LONG':
            ok = cc > co and cc > e9 and cc > e26
        else:
            ok = cc < co and cc < e9 and cc < e26
        if ok and d == 'LONG' and cc <= e200:
            ok = False
        if ok and d == 'SHORT' and cc >= e200:
            ok = False
        if ok and adx <= S1_ADX_MIN:
            ok = False
        if ok and d == 'LONG' and not (dip > din):
            ok = False
        if ok and d == 'SHORT' and not (din > dip):
            ok = False
        if ok and d == 'LONG' and not (mac > macs and mach > 0):
            ok = False
        if ok and d == 'SHORT' and not (mac < macs and mach < 0):
            ok = False

        if not ok:
            return

        # ── confirmed — build the signal ──────────────────────────────────────
        entry  = cc                              # indicative; order manager re-prices at fill
        sl_pct = self.p['sl_pct']                # 0.5
        rungs  = self.p['rungs']                 # [0.5, 0.7, 1.0, 1.5, 1.9]

        if d == 'LONG':
            sl_price   = entry * (1 - sl_pct / 100.0)
            rung_px    = [entry * (1 + r / 100.0) for r in rungs]
        else:
            sl_price   = entry * (1 + sl_pct / 100.0)
            rung_px    = [entry * (1 - r / 100.0) for r in rungs]

        tp_cap = rung_px[-1]                      # final rung is the hard TP cap

        # SL level after each reached rung (matches simulate_ladder's sl_level()):
        #   reached 0 -> entry (break-even); reached i>=1 -> previous rung price.
        sl_after_reached = [entry] + rung_px[:-1]   # indices 0..len(rungs)-1

        st['pending_dir'] = None
        st['pending_ts']  = None

        sig = SignalEvent(
            strategy_id = self.strategy_id,
            symbol      = symbol,
            direction   = d,
            entry_type  = self.entry_type,        # MARKET
            signal_price= entry,
            sl_price    = sl_price,
            tp_price    = tp_cap,
            signal_ts   = ts,
            signal_time = fmt_ts(ts),
            reason      = (f"EMA9/26 cross {d} | ADX={adx:.1f} DI+={dip:.1f} DI-={din:.1f} "
                          f"| MACD={mac:.5f}>{macs:.5f}"),
            exit_model  = self.exit_model,        # LADDER
            exit_plan   = {
                'sl_price':         sl_price,
                'tp_price':         tp_cap,
                'rung_prices':      rung_px,
                'sl_after_reached': sl_after_reached,
                'final_mode':       self.p.get('final_mode', 'cap'),
                'sl_pct':           sl_pct,
            },
            indicators  = {'ema9': e9, 'ema26': e26, 'ema200': e200,
                           'adx': adx, 'di_plus': dip, 'di_minus': din,
                           'macd': mac, 'macd_signal': macs, 'macd_hist': mach},
        )
        self._emit(sig)
