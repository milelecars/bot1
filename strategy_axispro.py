"""
STRATEGY 2 (live id AXISPRO) — BOS + Fibonacci Pullback
=======================================================
Faithful live port of AxisPro_Backtest_15m_top50.py's entry logic.

Scalars (EMA21/50/200, ATR, SMA(ATR,100), 1h EMA200 bias) are read from the
engine snapshot; structure (swing pivots, impulse leg, Fib zones) is computed
from the candle list — same formulas as the backtest.

FLOW (per symbol, per closed 15m candle):
  1. HTF bias gate    : 1h EMA200 (ind['htf_bias']) — long-only above, short-only below
  2. ATR expansion    : ATR / SMA(ATR,100) >= 1.05
  3. Break of structure: close crosses the last 5/5 swing pivot (+ 0.10*ATR buffer)
  4. Fib pullback     : after the break, wait for a retrace into the 38-62% zone
                        of the impulse leg (invalidated beyond 80%)
  5. Confirmation     : strong close beyond EMA21 + prior candle, inside the
                        EMA21/EMA50 band, on the right side of EMA200, 5-bar cooldown
  6. Entry            : market on the signal-candle close

EXIT (managed by the order manager's PARTIAL_TRAIL model):
  SL = nearer of (1.6*ATR, 50-bar swing), capped at 6*ATR
  TP1 = 1.5R (close 50%) -> SL to break-even -> trail runner by EMA21 -> TP2 = 3R
"""

import logging
from strategy_base import BaseStrategy, SignalEvent, CANDLE_MS, fmt_ts

log = logging.getLogger('strategy.axispro')

# ── signal params (mirror the backtest) ──────────────────────────────────────
PIVOT_L, PIVOT_R = 5, 5
IMP_LOOK   = 60
FIB_MIN, FIB_MAX, FIB_CANCEL = 0.38, 0.62, 0.80
COOLDOWN   = 5            # bars
ATR_RATIO  = 1.05
BOS_BUF_ATR = 0.10
SWING_LB   = 50

REQUIRED = ['ema21', 'ema50', 'ema200', 'atr', 'atr_sma']


def _pivot_high(highs, i, left, right):
    if i - left < 0 or i + right >= len(highs):
        return None
    piv = highs[i]
    for j in range(i - left, i + right + 1):
        if j != i and highs[j] >= piv:
            return None
    return piv


def _pivot_low(lows, i, left, right):
    if i - left < 0 or i + right >= len(lows):
        return None
    piv = lows[i]
    for j in range(i - left, i + right + 1):
        if j != i and lows[j] <= piv:
            return None
    return piv


class AxisProStrategy(BaseStrategy):

    def _new_state(self, symbol):
        return {'last_sh': None, 'last_sl': None,
                'armed_long': False, 'armed_short': False,
                'imp_start': None, 'imp_end': None,
                'z_lo': None, 'z_hi': None, 'z_inv': None,
                'last_entry_ts': None, 'last_ts': 0}

    def on_candle_close(self, symbol, candle, ind, candle_list):
        if not self.owns(symbol):
            return
        st = self.state(symbol)
        ts = candle['t']
        if ts <= st['last_ts']:
            return
        st['last_ts'] = ts

        # One position per symbol: while open, hold off (reset arming so a fresh
        # BOS is required after the trade closes).
        if self.is_open(symbol):
            st['armed_long'] = st['armed_short'] = False
            return

        if any(ind.get(k) is None for k in REQUIRED):
            return
        if len(candle_list) < IMP_LOOK + 2:
            return

        atr      = ind['atr']
        ema_f    = ind['ema21']
        ema_m    = ind['ema50']
        ema2     = ind['ema200']
        atr_base = ind['atr_sma']

        highs = [c['h'] for c in candle_list]
        lows  = [c['l'] for c in candle_list]
        closes = [c['c'] for c in candle_list]
        last = len(candle_list) - 1

        c_close, c_open, c_high, c_low = candle['c'], candle['o'], candle['h'], candle['l']
        prev_close = closes[last - 1]
        prev_high  = highs[last - 1]
        prev_low   = lows[last - 1]

        # ── update swing pivots (confirmed PIVOT_R bars back) ─────────────────
        pv = last - PIVOT_R
        if pv >= 0:
            ph = _pivot_high(highs, pv, PIVOT_L, PIVOT_R)
            pl = _pivot_low(lows, pv, PIVOT_L, PIVOT_R)
            if ph is not None:
                st['last_sh'] = ph
            if pl is not None:
                st['last_sl'] = pl

        # ── ATR expansion gate ────────────────────────────────────────────────
        atr_ok = (atr_base is not None and atr_base > 0 and atr / atr_base >= ATR_RATIO)

        # ── HTF bias gate ─────────────────────────────────────────────────────
        bias = ind.get('htf_bias', 0)
        bias_long, bias_short = bias > 0, bias < 0

        bos_buf = atr * BOS_BUF_ATR
        cross_up = st['last_sh'] is not None and prev_close <= st['last_sh'] and c_close > st['last_sh']
        cross_dn = st['last_sl'] is not None and prev_close >= st['last_sl'] and c_close < st['last_sl']
        bos_up = bias_long  and atr_ok and cross_up and (c_close > st['last_sh'] + bos_buf)
        bos_dn = bias_short and atr_ok and cross_dn and (c_close < st['last_sl'] - bos_buf)

        # ── impulse leg extremes over the lookback window ─────────────────────
        win_lo = min(lows[-IMP_LOOK:])
        win_hi = max(highs[-IMP_LOOK:])

        if bos_up:
            st['armed_long'], st['armed_short'] = True, False
            st['imp_start'], st['imp_end'] = win_lo, c_high
        if bos_dn:
            st['armed_short'], st['armed_long'] = True, False
            st['imp_start'], st['imp_end'] = win_hi, c_low

        # ── build Fib zones ───────────────────────────────────────────────────
        if st['armed_long'] and st['imp_start'] is not None and st['imp_end'] is not None:
            rng = st['imp_end'] - st['imp_start']
            st['z_hi']  = st['imp_end'] - rng * FIB_MIN
            st['z_lo']  = st['imp_end'] - rng * FIB_MAX
            st['z_inv'] = st['imp_end'] - rng * FIB_CANCEL
        if st['armed_short'] and st['imp_start'] is not None and st['imp_end'] is not None:
            rng = st['imp_start'] - st['imp_end']
            st['z_lo']  = st['imp_end'] + rng * FIB_MIN
            st['z_hi']  = st['imp_end'] + rng * FIB_MAX
            st['z_inv'] = st['imp_end'] + rng * FIB_CANCEL

        # ── invalidation (retrace beyond 80%) ─────────────────────────────────
        if st['armed_long'] and st['z_inv'] is not None and c_low < st['z_inv']:
            st['armed_long'] = False
        if st['armed_short'] and st['z_inv'] is not None and c_high > st['z_inv']:
            st['armed_short'] = False

        # ── zone touch ────────────────────────────────────────────────────────
        touch_long  = st['armed_long']  and st['z_lo'] is not None and c_low <= st['z_hi'] and c_high >= st['z_lo']
        touch_short = st['armed_short'] and st['z_lo'] is not None and c_high >= st['z_lo'] and c_low <= st['z_hi']

        # ── strong-close confirmation ─────────────────────────────────────────
        confirm_long  = c_close > ema_f and c_close > prev_high
        confirm_short = c_close < ema_f and c_close < prev_low

        # ── near-band: close between EMA21 and EMA50 ──────────────────────────
        band_lo, band_hi = min(ema_f, ema_m), max(ema_f, ema_m)
        near_long  = band_lo <= c_close <= band_hi
        near_short = band_lo <= c_close <= band_hi

        # ── EMA200 filter ─────────────────────────────────────────────────────
        ema_ok_long  = c_close > ema2
        ema_ok_short = c_close < ema2

        # ── cooldown ──────────────────────────────────────────────────────────
        cd_ok = st['last_entry_ts'] is None or (ts - st['last_entry_ts']) > COOLDOWN * CANDLE_MS

        long_sig  = cd_ok and touch_long  and confirm_long  and near_long  and ema_ok_long
        short_sig = cd_ok and touch_short and confirm_short and near_short and ema_ok_short

        if not (long_sig or short_sig):
            return

        direction = 'LONG' if long_sig else 'SHORT'
        entry = c_close       # indicative; order manager re-prices at fill

        # ── SL: nearer of (1.6*ATR, 50-bar swing), capped at 6*ATR ────────────
        sw_low  = min(lows[-SWING_LB:])
        sw_high = max(highs[-SWING_LB:])
        max_sl  = atr * 6.0
        if direction == 'LONG':
            sl0 = min(entry - atr * 1.6, sw_low)
            if (entry - sl0) > max_sl:
                sl0 = entry - max_sl
            risk = entry - sl0
            tp1 = entry + risk * 1.5
            tp2 = entry + risk * 3.0
        else:
            sl0 = max(entry + atr * 1.6, sw_high)
            if (sl0 - entry) > max_sl:
                sl0 = entry + max_sl
            risk = sl0 - entry
            tp1 = entry - risk * 1.5
            tp2 = entry - risk * 3.0

        if risk <= 0:
            return

        # consumed — require a fresh BOS for the next trade
        st['armed_long'] = st['armed_short'] = False
        st['last_entry_ts'] = ts

        sig = SignalEvent(
            strategy_id = self.strategy_id,
            symbol      = symbol,
            direction   = direction,
            entry_type  = self.entry_type,      # MARKET
            signal_price= entry,
            sl_price    = sl0,
            tp_price    = tp1,                  # primary target reported as TP1
            signal_ts   = ts,
            signal_time = fmt_ts(ts),
            reason      = (f"BOS+Fib {direction} | risk={risk:.6f} "
                          f"({risk/entry*100:.2f}%) ATR={atr:.6f} bias={bias:+d}"),
            exit_model  = self.exit_model,      # PARTIAL_TRAIL
            exit_plan   = {
                'sl_price':   sl0,
                'tp1_price':  tp1,
                'tp2_price':  tp2,
                'tp1_frac':   self.p.get('tp1_frac', 0.50),
                'risk':       risk,
                'move_be':    self.p.get('move_be', True),
                'trail_ema':  self.p.get('trail_ema', 21),
            },
            indicators  = {'ema21': ema_f, 'ema50': ema_m, 'ema200': ema2,
                           'atr': atr, 'htf_bias': bias},
        )
        self._emit(sig)
