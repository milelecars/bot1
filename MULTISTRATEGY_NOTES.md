# Milele Prime — 5-Strategy Bot (build notes)

This bot runs **five strategies concurrently** on Binance USDT-M Futures
(**testnet** by default). One WebSocket engine streams 15m candles for a single
**disjoint** union of symbols; each closed candle is routed to the one detector
that owns that symbol, and to the order manager for exit management.

## Strategies (configured in `strategy_registry.py`)

| id | name | dir | lev | exit model | symbols |
|----|------|-----|-----|-----------|---------|
| `S2_FVG_RETEST` | FVG retest + EMA50/100 + ADX | L/S | 50x | FIXED_LP | 8 |
| `AXISPRO` | BOS + Fib pullback | L/S | 50x | PARTIAL_TRAIL | 8 |
| `BREAKOUT_NY4H` | NY 09:00–13:00 opening-range breakout | L/S | 25x | BREAKOUT_TRAIL | 6 |
| `ICT_NDOG` | New-day opening-gap **breakout** | L/S | 50x | FIXED | 11 |
| `S1_EMA_CROSS` | EMA9/26 cross + filters | L/S | 50x | LADDER | 10 |

All sizing is **$20 margin × the leverage above**. Symbol lists are disjoint
(43 unique symbols, 0 overlaps) — `strategy_registry.validate_disjoint()` enforces
this at import time. To enable/disable a strategy, flip `enabled` in the registry.

## Exit models (in `step3_order_manager.py`)
- **FIXED** — static SL + TP; exit at whichever fills (ICT).
- **FIXED_LP** — FIXED plus a lock-profit move: when price reaches 50% of the way
  to TP, the stop is moved to lock 10% of the TP distance (S2).
- **LADDER** — as price closes through each rung (0.5/0.7/1.0/1.5/1.9%), the stop
  ratchets up to the previous rung; +1.9% hard-cap TP (S1).
- **BREAKOUT_TRAIL** — fixed 3R TP; trail the stop 1.5×ATR behind the extreme once
  +1.5R; arm a trailing TP at +2.5R (bank on a 1.2×ATR pullback); 48h time stop.
- **PARTIAL_TRAIL** — TP1 closes 50% at 1.5R → stop to break-even → trail the
  runner by EMA21 → TP2 at 3R (AxisPro).

Exit management is candle-driven: `main.candle_callback` calls
`OrderManager.on_candle_update(symbol, candle, ind)` on every closed candle.

## Key fixes carried in this build
- **`set_margin_type` removed** — demo-fapi is always CROSSED; the call timed out
  and aborted trades.
- **Gate-leak fixed** — `_handle_signal` now releases the owning detector's
  per-symbol gate on *every* abort path (was 2 of ~8), so symbols never freeze.
- WebSocket `run_forever()` args + pinned `websocket-client==1.6.4`; `tzdata`
  pinned for the NY session.

## ICT note (important)
The provided `ict_gap_v2_backtest.py` is a gap **breakout** (market entry), not the
midpoint *fade* in the spec markdown. This port follows the actual code. Live
config from the v2 sweep: `exit_mode='tp'` (swing-anchored SL capped at 0.5% +
fixed 1.5% TP), session filter **off** (crypto is 24/7). To change it, edit the
`ICT_NDOG` params in the registry.

## v1 caveats (be aware before going live)
- Detectors are individually validated; the order-manager exit **management**
  (ladder / trails / partial) passed an offline integration smoke test with a
  mocked exchange but has **not** been validated against live demo fills yet.
  Watch the first sessions on testnet.
- For **PARTIAL_TRAIL**, the TP1 partial profit is not written as its own DB row;
  the position's DB record reflects the runner's close. (Functionally correct on
  the exchange; a reporting limitation only.)
- **AxisPro** and **ICT** hold off generating new signals while a position is open
  on that symbol (one position per symbol). This is the correct live behaviour and
  may produce slightly fewer trades than the backtests in overlapping-setup cases.
- The `-4005` / `-2027` retry paths place a full-qty TP (no partial split) on the
  rare occasions they fire.

## Run
```
pip install -r requirements.txt
cp .env.example .env      # fill in Binance (testnet) + Supabase keys
python main.py            # or: gunicorn main:app --workers 1   (Railway)
```
Keep gunicorn at **--workers 1** (multiple workers = duplicate bots/trades).
`/health` returns engine + open-position status.
