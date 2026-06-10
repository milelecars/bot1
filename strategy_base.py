"""
STRATEGY BASE  —  shared contract for every detector
=====================================================
All five strategy detectors subclass BaseStrategy and emit the SAME
SignalEvent, so the order manager has one interface to handle regardless of
which strategy fired.

A detector's job:
  - own a disjoint set of symbols (from its registry spec)
  - on each closed 15m candle for one of its symbols, decide whether a setup
    just confirmed
  - if so, compute the CONCRETE entry/SL/TP prices + an exit_plan and emit()

The order manager then:
  - places the entry (market or resting limit)
  - reads exit_model + exit_plan to manage SL/TP/trailing/laddering
  - calls back on_trade_closed() to release the per-symbol gate

Signal params (filter thresholds) live in each detector. Sizing + exit model +
exit params + symbols live in strategy_registry.py.
"""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable

log = logging.getLogger('strategy')

# 15-minute bar in milliseconds (shared by detectors for bar-count math).
CANDLE_MS = 15 * 60 * 1000


@dataclass
class SignalEvent:
    """Everything the order manager needs to open and then manage a trade."""
    strategy_id: str
    symbol:      str
    direction:   str               # 'LONG' | 'SHORT'
    entry_type:  str               # 'MARKET' | 'LIMIT'
    signal_price: float            # indicative entry (signal-candle close) — SL/TP anchor
    sl_price:    float             # initial stop
    tp_price:    float             # primary take-profit (or final cap, for the ladder)
    signal_ts:   int
    signal_time: str
    reason:      str = ''
    limit_price: Optional[float] = None     # resting price for LIMIT entries (ICT)
    exit_model:  Optional[str] = None       # from the registry; order manager dispatches on this
    exit_plan:   dict = field(default_factory=dict)   # model-specific runtime params
    indicators:  dict = field(default_factory=dict)

    # ── aliases so the existing order-manager code (signal.entry_price /
    #    signal.strategy) works against the unified event unchanged ───────────
    @property
    def entry_price(self) -> float:
        return self.signal_price

    @property
    def strategy(self) -> str:
        return self.strategy_id


def fmt_ts(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')


class BaseStrategy:
    """Common state + gate management. Subclasses implement on_candle_close()."""

    def __init__(self, spec, on_signal: Optional[Callable] = None):
        self.spec        = spec
        self.strategy_id = spec.strategy_id
        self.symbols     = list(spec.symbols)
        self._symset     = set(self.symbols)
        self.on_signal   = on_signal
        self.exit_model  = spec.exit_model
        self.entry_type  = spec.entry_type
        self.p           = dict(spec.params)        # numeric params for this strategy
        self.margin_usdt = spec.margin_usdt
        self.leverage    = spec.leverage
        self._gate       = {s: False for s in self.symbols}   # per-symbol "trade open"
        self._state      = {s: self._new_state(s) for s in self.symbols}
        self._lock       = threading.Lock()

    # ── per-symbol state (subclasses override) ───────────────────────────────
    def _new_state(self, symbol):
        return {}

    def state(self, symbol):
        if symbol not in self._state:
            self._state[symbol] = self._new_state(symbol)
        return self._state[symbol]

    # ── ownership / gate ─────────────────────────────────────────────────────
    def owns(self, symbol):
        return symbol in self._symset

    def is_open(self, symbol):
        return self._gate.get(symbol, False)

    def set_gate(self, symbol, value):
        self._gate[symbol] = value

    def on_trade_closed(self, symbol, strategy_id, outcome):
        """Called by the order manager when a position closes (any outcome) OR
        when an entry attempt is aborted — releases the per-symbol gate."""
        if strategy_id == self.strategy_id and symbol in self._gate:
            self._gate[symbol] = False
            log.info(f"{self.strategy_id} {symbol}: gate released ({outcome})")

    # ── main hook (subclasses implement) ──────────────────────────────────────
    def on_candle_close(self, symbol, candle, ind, candle_list):
        raise NotImplementedError

    # ── emit ───────────────────────────────────────────────────────────────────
    def _emit(self, sig: SignalEvent):
        # Block further signals on this symbol until the order manager releases
        # the gate (on close OR on aborted entry — the manager guarantees both).
        self.set_gate(sig.symbol, True)
        log.info(f"[SIGNAL] {sig.strategy_id} {sig.symbol} {sig.direction} "
                 f"({sig.entry_type}) entry~{sig.signal_price:.6f} "
                 f"SL={sig.sl_price:.6f} TP={sig.tp_price:.6f} | {sig.reason}")
        if self.on_signal:
            try:
                self.on_signal(sig)
            except Exception as e:
                log.error(f"on_signal handler error: {e}", exc_info=True)
        else:
            log.warning(f"Signal emitted but no handler set: {sig.symbol} {sig.strategy_id}")
