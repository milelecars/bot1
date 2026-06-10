"""
STRATEGY REGISTRY  —  single source of truth for the 5-strategy live bot
========================================================================
Every other module imports from here. Nothing about a strategy's identity,
sizing, symbol list, entry type, or exit model is defined anywhere else.

What lives HERE (per strategy):
  - canonical strategy id (the string written to the DB `strategy` column)
  - enabled flag (flip a strategy off without touching code)
  - symbol list  (DISJOINT across strategies — see note below)
  - sizing       (margin, leverage)
  - entry type   ('MARKET' or 'LIMIT')
  - exit model   (one of EXIT_* below) + the numeric params that model needs
  - indicator / timeframe requirements (so the candle engine knows what to feed)

What lives in each DETECTOR module (NOT here):
  - the signal-generation logic and its filter thresholds
    (e.g. S2's FVG depth, AxisPro's Fib ratios, S1's MACD/DI gates)

----------------------------------------------------------------------------
DISJOINT SYMBOLS
----------------------------------------------------------------------------
Binance USDT-M futures runs in one-way mode on the demo account: ONE net
position per symbol. If two strategies traded the same coin, their positions
would net together and per-strategy SL/TP tracking would break. So every coin
belongs to exactly one strategy. Overlaps were resolved by keeping the live S2
set and the (smallest) Breakout set intact and trimming AxisPro and ICT:
    WLD  -> S2 (removed from AxisPro)
    XPL  -> S2 (removed from ICT)
    SUI  -> Breakout (removed from AxisPro)
    XLM  -> Breakout (removed from ICT)
    ZEC  -> Breakout (removed from ICT)

----------------------------------------------------------------------------
DEMO-API NOTE
----------------------------------------------------------------------------
Many of these symbols (especially in ICT and S1) are newer/illiquid perps that
may NOT exist on the Binance demo/testnet futures venue. The candle engine runs
a startup availability filter against the demo exchangeInfo and drops + logs any
symbol that isn't listed, so a missing coin is a logged no-op, never a crash.
"""

from dataclasses import dataclass, field
from typing import Dict, List


# ============================================================================
# EXIT MODELS  — interpreted by the order manager's exit engine
# ============================================================================
EXIT_FIXED_LP        = "FIXED_LP"        # fixed SL/TP + lock-profit move          (S2)
EXIT_FIXED           = "FIXED"           # fixed SL/TP, exit at whichever hits      (ICT breakout)
EXIT_PARTIAL_TRAIL   = "PARTIAL_TRAIL"   # TP1 partial -> BE -> EMA trail -> TP2    (AxisPro)
EXIT_BREAKOUT_TRAIL  = "BREAKOUT_TRAIL"  # fixed TP + ATR trail + time stop         (Breakout)
EXIT_FIXED_TRAIL     = "FIXED_TRAIL"     # fixed SL/TP + % trailing stop            (ICT)
EXIT_LADDER          = "LADDER"          # rung-ratcheted SL + final cap            (S1)

# ============================================================================
# ENTRY TYPES
# ============================================================================
ENTRY_MARKET = "MARKET"   # market order fired on signal-candle close
ENTRY_LIMIT  = "LIMIT"    # resting limit order (cancelled at its window boundary)


@dataclass
class StrategySpec:
    strategy_id: str
    name:        str
    enabled:     bool
    symbols:     List[str]
    margin_usdt: float
    leverage:    int
    entry_type:  str
    exit_model:  str
    # model-specific numeric params (see each strategy's block below)
    params:      Dict = field(default_factory=dict)
    # what the candle engine must compute / provide for this strategy
    needs:       Dict = field(default_factory=dict)


# ============================================================================
# STRATEGY DEFINITIONS
# ============================================================================

STRATEGIES: Dict[str, StrategySpec] = {

    # ── 1) S2 — FVG Retest + EMA50/100 + ADX  (already live) ─────────────────
    "S2_FVG_RETEST": StrategySpec(
        strategy_id = "S2_FVG_RETEST",
        name        = "S2 FVG Retest",
        enabled     = True,
        symbols     = ["BTCUSDT", "ENAUSDT", "WLDUSDT", "PUMPUSDT",
                       "XPLUSDT", "1000SHIBUSDT", "APTUSDT", "MONUSDT"],
        margin_usdt = 20.0,
        leverage    = 50,
        entry_type  = ENTRY_MARKET,
        exit_model  = EXIT_FIXED_LP,
        params = {
            "sl_pct":         0.4,    # SL distance from entry, %
            "tp_pct":         1.2,    # TP distance from entry, % (RR 3.0)
            "lp_trigger_pct": 50.0,   # arm lock-profit at 50% of the way to TP
            "lp_lock_pct":    10.0,   # lock 10% of the TP distance from entry
        },
        needs = {
            "ema":        [50, 100],
            "adx":        14,
            "timeframes": ["15m"],
        },
    ),

    # ── 2) AxisPro — BOS + Fibonacci pullback ────────────────────────────────
    "AXISPRO": StrategySpec(
        strategy_id = "AXISPRO",
        name        = "AxisPro (BOS + Fib)",
        enabled     = True,
        symbols     = ["SOLUSDT", "TRXUSDT", "OPUSDT", "RUNEUSDT",
                       "HBARUSDT", "ALGOUSDT", "DYDXUSDT", "JUPUSDT"],
        margin_usdt = 20.0,
        leverage    = 50,
        entry_type  = ENTRY_MARKET,
        exit_model  = EXIT_PARTIAL_TRAIL,
        params = {
            "sl_x_atr":     1.6,    # SL = entry -/+ 1.6*ATR ...
            "max_sl_xatr":  6.0,    #   ... but never wider than 6*ATR
            "swing_lb":     50,     # also no tighter than the 50-bar swing
            "tp1_rr":       1.5,    # TP1 at 1.5R ...
            "tp1_frac":     0.50,   #   ... closes 50% of the position
            "tp2_rr":       3.0,    # remainder targets 3.0R
            "move_be":      True,   # SL -> break-even after TP1
            "trail_ema":    21,     # then trail the runner by EMA21
        },
        needs = {
            "ema":        [21, 50, 200],
            "atr":        14,
            "atr_sma":    100,      # SMA of ATR for the expansion gate
            "pivots":     [5, 5],   # left/right for swing pivots
            "htf":        {"interval": "1h", "ema": 200},  # 1h EMA200 bias gate
            "timeframes": ["15m", "1h"],
        },
    ),

    # ── 3) Breakout — NY 4h opening-range break ──────────────────────────────
    "BREAKOUT_NY4H": StrategySpec(
        strategy_id = "BREAKOUT_NY4H",
        name        = "Breakout NY4H",
        enabled     = True,
        symbols     = ["XLMUSDT", "ZECUSDT", "SUIUSDT",
                       "INJUSDT", "TAOUSDT", "FETUSDT"],
        margin_usdt = 20.0,
        leverage    = 25,           # lower than the others, per its backtest
        entry_type  = ENTRY_MARKET,
        exit_model  = EXIT_BREAKOUT_TRAIL,
        params = {
            "rr":                3.0,    # fixed TP at 3R
            "use_range_sl":      True,   # SL at opposite edge of the range
            "atr_sl_mult":       1.5,    #   (fallback if range-SL disabled)
            "trail_trigger_r":   1.5,    # start ATR-trailing the stop at +1.5R
            "trail_sl_atr":      1.5,    #   trail distance = 1.5*ATR
            "tp_lock_trigger_r": 2.5,    # arm trailing-TP at +2.5R
            "trail_tp_atr":      1.2,    #   close if price pulls back 1.2*ATR from extreme
            "time_stop_bars":    192,    # force-close after 192 bars (~48h)
            "max_trades_per_day": 4,     # per symbol
        },
        needs = {
            "ema":        [200],
            "adx":        14,
            "atr":        14,
            "volume":     20,           # 20-bar volume average for the surge filter
            "session":    {"tz": "America/New_York", "start_hour": 9, "end_hour": 13},
            "timeframes": ["15m"],
        },
    ),

    # ── 4) ICT — NY opening-gap BREAKOUT  (port of ict_gap_v2_backtest.py) ───
    # The provided backtest is a gap BREAKOUT (market entry), not the midpoint
    # fade in the spec md. Live config from the v2 sweep: exit_mode='tp'
    # (fixed swing-capped SL + fixed TP), session filter off (24/7 crypto).
    "ICT_NDOG": StrategySpec(
        strategy_id = "ICT_NDOG",
        name        = "ICT Opening-Gap Breakout",
        enabled     = True,
        symbols     = ["AIGENSYNUSDT", "BEATUSDT", "CRCLUSDT", "ESPORTUSDT",
                       "GENIUSDT", "HETUSDT", "HYPEUSDT", "IOUSDT",
                       "MSTUSDT", "MUUSDT", "SNDKUSDT"],
        margin_usdt = 20.0,
        leverage    = 50,
        entry_type  = ENTRY_MARKET,     # breakout candle close
        exit_model  = EXIT_FIXED,
        params = {
            "sl_pct":      0.5,         # SL = nearer of (swing anchor, entry +/- 0.5%)
            "tp_pct":      1.5,         # fixed TP, %
            "use_session": False,       # no equities-session filter on crypto
            "gap_type":    "NDOG",      # New-Day Opening Gap (UTC daily)
        },
        needs = {
            "daily_gap":  True,          # prev-candle close vs day open (computed in detector)
            "timeframes": ["15m"],
        },
    ),

    # ── 5) S1 — EMA 9/26 cross + trailing ladder ─────────────────────────────
    "S1_EMA_CROSS": StrategySpec(
        strategy_id = "S1_EMA_CROSS",
        name        = "S1 EMA Cross (Ladder)",
        enabled     = True,
        symbols     = ["BNBUSDT", "VIRTUALUSDT", "VVVUSDT", "ADAUSDT",
                       "DOTUSDT", "MAGMAUSDT", "PLAYUSDT", "HUSDT",
                       "LITUSDT", "LABUSDT"],
        margin_usdt = 20.0,
        leverage    = 50,
        entry_type  = ENTRY_MARKET,
        exit_model  = EXIT_LADDER,
        params = {
            "sl_pct":      0.5,                       # initial hard stop %
            "rungs":       [0.5, 0.7, 1.0, 1.5, 1.9], # ladder rungs, %
            "trail_mode":  "previous_rung",           # ratchet SL to the previous rung
            "final_mode":  "cap",                     # final rung (+1.9%) is a hard TP cap
        },
        needs = {
            "ema":        [9, 26, 200],
            "macd":       [12, 26, 9],
            "adx":        14,            # ADX + DI+ / DI-
            "atr":        14,
            "timeframes": ["15m"],
        },
    ),
}


# ============================================================================
# HELPERS
# ============================================================================

def enabled_strategies() -> List[StrategySpec]:
    """All strategies with enabled=True."""
    return [s for s in STRATEGIES.values() if s.enabled]


def get_spec(strategy_id: str) -> StrategySpec:
    return STRATEGIES[strategy_id]


def all_symbols() -> List[str]:
    """Deduplicated union of every enabled strategy's symbols (order-preserving)."""
    seen, out = set(), []
    for spec in enabled_strategies():
        for sym in spec.symbols:
            if sym not in seen:
                seen.add(sym)
                out.append(sym)
    return out


def strategy_for_symbol(symbol: str) -> str:
    """Which strategy owns this symbol (lists are disjoint, so at most one)."""
    for spec in enabled_strategies():
        if symbol in spec.symbols:
            return spec.strategy_id
    return None


def sizing_for(strategy_id: str) -> Dict:
    spec = STRATEGIES[strategy_id]
    return {"margin_usdt": spec.margin_usdt, "leverage": spec.leverage}


def validate_disjoint() -> None:
    """Raise if any symbol is claimed by more than one enabled strategy."""
    owner = {}
    for spec in enabled_strategies():
        for sym in spec.symbols:
            if sym in owner:
                raise ValueError(
                    f"Symbol {sym} is in both {owner[sym]} and {spec.strategy_id} "
                    f"— strategy symbol lists must be disjoint."
                )
            owner[sym] = spec.strategy_id


# Fail fast at import time if the lists ever drift back into overlap.
validate_disjoint()


if __name__ == "__main__":
    print("Enabled strategies and their (disjoint) symbol counts:\n")
    total = 0
    for s in enabled_strategies():
        print(f"  {s.strategy_id:<16} {s.exit_model:<16} "
              f"{s.leverage:>3}x  {len(s.symbols):>2} symbols  -> {', '.join(s.symbols)}")
        total += len(s.symbols)
    print(f"\n  Union: {len(all_symbols())} unique symbols across {len(enabled_strategies())} strategies "
          f"({total} total, 0 overlaps).")
