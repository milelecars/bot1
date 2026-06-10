"""
STRATEGY 3 (live id BREAKOUT_NY4H) — NY Opening-Range Breakout
==============================================================
Faithful live port of BacktestBreakout_6coins_2000d_RR3.py entry logic.

  - Builds the opening range over the NY 09:00-13:00 session (America/New_York,
    DST-correct via zoneinfo); never trades while the range is building.
  - After the session, a 15m candle that CLOSES beyond the range is a breakout.
  - Filters: range >= 0.20% of price, ATR% in [0.10, 5.0], ADX >= 20,
    breakout-candle volume >= 1.2x the 20-bar average, EMA200 trend gate.
  - Multiple entries per NY day (max 4), one position at a time per symbol.

EXIT (managed by the order manager's BREAKOUT_TRAIL model):
  initial SL = opposite side of the range; fixed TP = 3R; trailing stop arms at
  +1.5R (trails 1.5*ATR), trailing TP arms at +2.5R (banks on a 1.2*ATR pullback),
  192-bar (48h) time stop.
"""

import logging
from datetime import datetime, timezone, timedelta
from strategy_base import BaseStrategy, SignalEvent, fmt_ts

log = logging.getLogger('strategy.breakout')

try:
    from zoneinfo import ZoneInfo
    _NY = ZoneInfo("America/New_York")
except Exception:                       # pragma: no cover
    _NY = None
    log.warning("zoneinfo/tzdata unavailable — falling back to fixed UTC-4 for NY session")

# backtest defaults (overridable via registry params)
DEF = dict(session_start=9, session_end=13, rr=3.0, use_range_sl=True,
           atr_sl_mult=1.5, min_range_pct=0.20, min_atr_pct=0.10, max_atr_pct=5.0,
           adx_min=20.0, vol_surge=1.2, max_trades_per_day=4,
           trail_trigger_r=1.5, trail_sl_atr=1.5, tp_lock_r=2.5, trail_tp_atr=1.2,
           max_hold_candles=192)

REQUIRED = ['ema200', 'adx', 'atr', 'atr_pct', 'vol_sma']


def _ny_parts(ms):
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    dt = dt.astimezone(_NY) if _NY else (dt - timedelta(hours=4))
    return dt.strftime('%Y-%m-%d'), dt.hour


class BreakoutStrategy(BaseStrategy):

    def _p(self, key):
        return self.p.get(key, DEF[key])

    def _new_state(self, symbol):
        return {'day': None, 'range_high': None, 'range_low': None,
                'locked': False, 'trades_today': 0, 'last_ts': 0}

    def on_candle_close(self, symbol, candle, ind, candle_list):
        if not self.owns(symbol):
            return
        st = self.state(symbol)
        ts = candle['t']
        if ts <= st['last_ts']:
            return
        st['last_ts'] = ts

        day, hour = _ny_parts(ts)

        # ── new NY day → reset the opening range ──────────────────────────────
        if day != st['day']:
            st['day'] = day
            st['range_high'] = None
            st['range_low'] = None
            st['locked'] = False
            st['trades_today'] = 0

        in_sess = (self._p('session_start') <= hour < self._p('session_end'))

        # ── build the range during the session; never trade inside it ─────────
        if in_sess:
            st['range_high'] = candle['h'] if st['range_high'] is None else max(st['range_high'], candle['h'])
            st['range_low']  = candle['l'] if st['range_low']  is None else min(st['range_low'],  candle['l'])
            return

        # ── lock the range once the session ends ──────────────────────────────
        if (not st['locked']) and st['range_high'] is not None:
            st['locked'] = True

        if not st['locked']:
            return

        # one position at a time; re-arm after it closes
        if self.is_open(symbol):
            return
        if st['trades_today'] >= self._p('max_trades_per_day'):
            return
        if any(ind.get(k) is None for k in REQUIRED):
            return

        c_close = candle['c']
        atr     = ind['atr']
        rng     = st['range_high'] - st['range_low']
        rng_pct = (rng / c_close * 100) if c_close else 0.0
        vol     = candle.get('v', ind.get('volume', 0.0))
        vol_sma = ind['vol_sma']

        # ── filters ───────────────────────────────────────────────────────────
        if rng_pct < self._p('min_range_pct'):
            return
        if not (self._p('min_atr_pct') <= ind['atr_pct'] <= self._p('max_atr_pct')):
            return
        if ind['adx'] < self._p('adx_min'):
            return
        if vol_sma and vol < self._p('vol_surge') * vol_sma:
            return

        # ── breakout (close only) ─────────────────────────────────────────────
        long_bo  = c_close > st['range_high']
        short_bo = c_close < st['range_low']
        if not (long_bo or short_bo):
            return
        direction = 'LONG' if long_bo else 'SHORT'

        # ── EMA200 trend gate ─────────────────────────────────────────────────
        if direction == 'LONG' and c_close <= ind['ema200']:
            return
        if direction == 'SHORT' and c_close >= ind['ema200']:
            return

        entry = c_close       # indicative; order manager re-prices at fill

        # ── SL / TP ───────────────────────────────────────────────────────────
        if self._p('use_range_sl'):
            sl = st['range_low'] if direction == 'LONG' else st['range_high']
        else:
            sl = (entry - self._p('atr_sl_mult') * atr) if direction == 'LONG' \
                 else (entry + self._p('atr_sl_mult') * atr)

        risk = abs(entry - sl)
        if risk <= 0:
            return
        rr = self._p('rr')
        tp = entry + rr * risk if direction == 'LONG' else entry - rr * risk

        st['trades_today'] += 1

        sig = SignalEvent(
            strategy_id = self.strategy_id,
            symbol      = symbol,
            direction   = direction,
            entry_type  = self.entry_type,      # MARKET
            signal_price= entry,
            sl_price    = sl,
            tp_price    = tp,
            signal_ts   = ts,
            signal_time = fmt_ts(ts),
            reason      = (f"ORB {direction} | range=[{st['range_low']:.6f},{st['range_high']:.6f}] "
                          f"({rng_pct:.2f}%) ADX={ind['adx']:.1f} vol={vol/ max(vol_sma,1e-9):.2f}x "
                          f"trade {st['trades_today']}/{self._p('max_trades_per_day')}"),
            exit_model  = self.exit_model,      # BREAKOUT_TRAIL
            exit_plan   = {
                'sl_price':        sl,
                'tp_price':        tp,
                'atr':             atr,
                'risk':            risk,
                'trail_trigger_r': self._p('trail_trigger_r'),
                'trail_sl_atr':    self._p('trail_sl_atr'),
                'tp_lock_r':       self._p('tp_lock_r'),
                'trail_tp_atr':    self._p('trail_tp_atr'),
                'max_hold':        self._p('max_hold_candles'),
            },
            indicators  = {'ema200': ind['ema200'], 'adx': ind['adx'],
                           'atr': atr, 'atr_pct': ind['atr_pct']},
        )
        self._emit(sig)
