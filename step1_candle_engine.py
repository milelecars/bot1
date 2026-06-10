"""
STEP 1 — Candle Engine  (multi-strategy edition)
================================================
Single WebSocket feed for the UNION of all enabled strategies' symbols
(see strategy_registry.py). On every closed 15m candle it computes the full
indicator SUPERSET that the five strategies need and hands it to the callback.

What this engine provides per closed 15m candle (in the `indicators` dict):
  EMA 9 / 21 / 26 / 50 / 100 / 200
  ATR(14), ATR% , SMA(ATR,100)
  ADX(14) + DI+ / DI-
  MACD(12,26,9)  (macd line, signal, histogram)
  volume + 20-bar volume SMA
  prev close + prev EMA50  (for break-cross detection)
  htf_bias               (1h EMA200 bias: +1 / -1 / 0 — for AxisPro)
  raw OHLCV + quote volume + timestamp

Strategy-specific stateful detection (FVGs, swing pivots, opening-range,
daily gaps) is NOT done here — detectors compute that from the candle LIST,
which the engine also shares (via set_candle_list in the dispatcher). The
engine's job is the common indicator layer + multi-timeframe data feeds.

Multi-timeframe:
  - 15m kline stream for every symbol (entry timeframe).
  - 1h kline stream for symbols whose strategy declares an `htf` need
    (AxisPro). The 1h EMA200 bias is recomputed on each 1h close and injected
    into the 15m snapshot as `htf_bias`.

Demo-API safety:
  - Startup availability filter: queries the demo exchangeInfo and drops +
    logs any symbol the venue doesn't list (so a missing coin is a logged
    no-op, never a crash). Falls back to the full list if the call fails.
  - WebSocket uses the testnet stream host and `run_forever()` with NO ping
    args (ping_interval/ping_timeout block frame receipt on
    websocket-client >= 1.7.0; requirements pins 1.6.4 as well).
"""

import json
import threading
import time
import requests
import logging
import sys
import io
import os
from collections import deque
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from strategy_registry import all_symbols, enabled_strategies

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ============================================================================
# CONFIGURATION
# ============================================================================

TESTNET       = os.getenv('TESTNET', 'true').lower() == 'true'
INTERVAL      = os.getenv('INTERVAL', '15m')
HTF_INTERVAL  = '1h'
CANDLE_LIMIT  = 320      # 15m rolling window (>=210 needed for EMA200 warm-up)
HTF_LIMIT     = 260      # 1h rolling window (>=210 needed for 1h EMA200)
RECONNECT_SEC = 5

# Indicator periods (superset across all strategies)
EMA_PERIODS   = (9, 21, 26, 50, 100, 200)
EMA_TREND     = 200      # warm-up gate
ADX_PERIOD    = 14
ATR_PERIOD    = 14
ATR_SMA_LEN   = 100      # SMA of ATR (AxisPro expansion gate)
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
VOL_SMA_LEN   = 20       # volume surge baseline (Breakout)
HTF_EMA       = 200      # 1h bias EMA (AxisPro)

WARMUP_BARS   = EMA_TREND + 10   # 210

# Endpoints — trading + exchangeInfo go to demo on testnet; klines always
# seed from live public fapi (demo historical data is sparse).
if TESTNET:
    REST_BASE = "https://demo-fapi.binance.com/fapi"
    WS_BASE   = "wss://fstream.binancefuture.com/stream"
else:
    REST_BASE = "https://fapi.binance.com/fapi"
    WS_BASE   = "wss://fstream.binance.com/stream"

REST_DATA_BASE = "https://fapi.binance.com/fapi"   # candle seeding (always live public)

# ============================================================================
# SYMBOL UNIVERSE  (union of all enabled strategies, deduped, in registry order)
# ============================================================================

SYMBOLS = all_symbols()

# Symbols whose strategy needs a higher-timeframe (1h) feed.
HTF_SYMBOLS = []
for _spec in enabled_strategies():
    if _spec.needs.get('htf'):
        HTF_SYMBOLS.extend(_spec.symbols)
HTF_SYMBOLS = list(dict.fromkeys(HTF_SYMBOLS))   # dedupe, preserve order

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.FileHandler('bot.log', encoding='utf-8'),
              logging.StreamHandler()],
)
log = logging.getLogger('candle_engine')


# ============================================================================
# AVAILABILITY FILTER — drop symbols the demo venue doesn't list
# ============================================================================

def filter_available_symbols(symbols):
    """
    Query the (demo) futures exchangeInfo and return only the symbols that are
    actually listed and TRADING. Logs the dropped ones. On any failure, returns
    the original list unchanged (never blocks startup over a transient error).
    """
    try:
        resp = requests.get(f"{REST_BASE}/v1/exchangeInfo", timeout=20)
        if resp.status_code != 200:
            log.warning(f"[AVAIL] exchangeInfo HTTP {resp.status_code} — skipping filter, "
                        f"using all {len(symbols)} symbols")
            return list(symbols)
        info = resp.json()
        listed = {s['symbol'] for s in info.get('symbols', [])
                  if s.get('status') == 'TRADING'}
        available = [s for s in symbols if s in listed]
        dropped   = [s for s in symbols if s not in listed]
        if dropped:
            log.warning(f"[AVAIL] {len(dropped)} symbol(s) not listed/trading on "
                        f"{'demo' if TESTNET else 'live'} futures — DROPPED: {', '.join(dropped)}")
        log.info(f"[AVAIL] {len(available)}/{len(symbols)} symbols available for trading")
        return available
    except Exception as e:
        log.warning(f"[AVAIL] availability check failed ({e}) — using all {len(symbols)} symbols")
        return list(symbols)


# ============================================================================
# INDICATOR HELPERS  (formulas mirror the strategy backtests)
# ============================================================================

def ema_series(values, period):
    """EMA series, seeded with the SMA of the first `period` values."""
    n = len(values)
    out = [None] * n
    if n < period:
        return out
    k = 2.0 / (period + 1)
    out[period - 1] = sum(values[:period]) / period
    for i in range(period, n):
        out[i] = values[i] * k + out[i - 1] * (1 - k)
    return out


def _first_non_none(seq):
    for i, v in enumerate(seq):
        if v is not None:
            return i
    return None


def _ema_sparse(values, period):
    """EMA over a series whose non-None values form one contiguous tail block
    (used for the MACD signal line, since the MACD line warms late)."""
    n = len(values)
    out = [None] * n
    f = _first_non_none(values)
    if f is None or (n - f) < period:
        return out
    sub = values[f:]
    k = 2.0 / (period + 1)
    out[f + period - 1] = sum(sub[:period]) / period
    for i in range(period, len(sub)):
        out[f + i] = sub[i] * k + out[f + i - 1] * (1 - k)
    return out


def atr_series(highs, lows, closes, period):
    """Wilder's ATR."""
    n = len(closes)
    tr = [0.0] * n
    for i in range(1, n):
        tr[i] = max(highs[i] - lows[i],
                    abs(highs[i] - closes[i - 1]),
                    abs(lows[i] - closes[i - 1]))
    out = [None] * n
    if n > period:
        out[period] = sum(tr[1:period + 1]) / period
        for i in range(period + 1, n):
            out[i] = (out[i - 1] * (period - 1) + tr[i]) / period
    return out


def sma_of_series(series, period):
    """SMA over a series that may contain leading None values."""
    n = len(series)
    out = [None] * n
    for i in range(n):
        if i + 1 < period:
            continue
        window = series[i - period + 1:i + 1]
        if any(v is None for v in window):
            continue
        out[i] = sum(window) / period
    return out


def compute_indicators(candles):
    """
    Given a list of candle dicts {'t','o','h','l','c','v','q'}, return the full
    indicator superset AT THE LAST CANDLE, or None if there isn't enough data
    (< WARMUP_BARS). Indicators that haven't warmed yet are None individually;
    each detector checks the ones it needs.
    """
    n = len(candles)
    if n < WARMUP_BARS:
        return None

    closes = [c['c'] for c in candles]
    highs  = [c['h'] for c in candles]
    lows   = [c['l'] for c in candles]
    vols   = [c.get('v', 0.0) for c in candles]

    # ── EMAs ────────────────────────────────────────────────────────────────
    emas = {p: ema_series(closes, p) for p in EMA_PERIODS}
    ema = {p: emas[p][-1] for p in EMA_PERIODS}
    ema9_prev  = emas[9][-2]  if len(emas[9])  >= 2 else None
    ema26_prev = emas[26][-2] if len(emas[26]) >= 2 else None
    ema50_prev = emas[50][-2] if len(emas[50]) >= 2 else None
    close_prev = closes[-2] if len(closes) >= 2 else None

    # ── ATR + ATR% + SMA(ATR) ────────────────────────────────────────────────
    atr_s = atr_series(highs, lows, closes, ATR_PERIOD)
    atr   = atr_s[-1]
    atr_sma_s = sma_of_series(atr_s, ATR_SMA_LEN)
    atr_sma = atr_sma_s[-1]
    atr_pct = (atr / closes[-1] * 100) if (atr and closes[-1] > 0) else None

    # ── ADX + DI+ / DI- (Wilder) ─────────────────────────────────────────────
    p = ADX_PERIOD
    tr_raw = [0.0] * n
    dm_p   = [0.0] * n
    dm_n   = [0.0] * n
    for i in range(1, n):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        tr_raw[i] = max(h - l, abs(h - pc), abs(l - pc))
        up   = highs[i]    - highs[i - 1]
        down = lows[i - 1] - lows[i]
        if up > down and up > 0:   dm_p[i] = up
        if down > up and down > 0: dm_n[i] = down

    s_tr = [0.0] * n; s_dp = [0.0] * n; s_dn = [0.0] * n
    dip_s = [None] * n; din_s = [None] * n; dx_s = [None] * n
    if n > p:
        s_tr[p] = sum(tr_raw[1:p + 1])
        s_dp[p] = sum(dm_p[1:p + 1])
        s_dn[p] = sum(dm_n[1:p + 1])
        for i in range(p + 1, n):
            s_tr[i] = s_tr[i - 1] - s_tr[i - 1] / p + tr_raw[i]
            s_dp[i] = s_dp[i - 1] - s_dp[i - 1] / p + dm_p[i]
            s_dn[i] = s_dn[i - 1] - s_dn[i - 1] / p + dm_n[i]
        for i in range(p, n):
            atr_v = s_tr[i]
            if atr_v == 0:
                continue
            dip_s[i] = 100.0 * s_dp[i] / atr_v
            din_s[i] = 100.0 * s_dn[i] / atr_v
            denom = dip_s[i] + din_s[i]
            dx_s[i] = 0.0 if denom == 0 else 100.0 * abs(dip_s[i] - din_s[i]) / denom

    first_dx = next((i for i in range(n) if dx_s[i] is not None), None)
    adx_s = [None] * n
    if first_dx is not None:
        se = first_dx + p
        if se <= n:
            sv = [dx_s[i] for i in range(first_dx, se) if dx_s[i] is not None]
            if len(sv) == p:
                adx_s[se - 1] = sum(sv) / p
                for i in range(se, n):
                    if dx_s[i] is not None and adx_s[i - 1] is not None:
                        adx_s[i] = (adx_s[i - 1] * (p - 1) + dx_s[i]) / p

    adx      = adx_s[-1]
    di_plus  = dip_s[-1]
    di_minus = din_s[-1]

    # ── MACD(12,26,9) ────────────────────────────────────────────────────────
    ema_fast = ema_series(closes, MACD_FAST)
    ema_slow = ema_series(closes, MACD_SLOW)
    macd_line = [(ema_fast[i] - ema_slow[i])
                 if (ema_fast[i] is not None and ema_slow[i] is not None) else None
                 for i in range(n)]
    # Signal line: zero-fill the warm-up region then EMA — matches the S1 backtest's
    # MACD seeding exactly (it does ema_series([v or 0.0 for v in macd_line], 9)).
    signal_line = ema_series([v if v is not None else 0.0 for v in macd_line], MACD_SIG)
    macd        = macd_line[-1]
    macd_signal = signal_line[-1]
    macd_hist   = (macd - macd_signal) if (macd is not None and macd_signal is not None) else None

    # ── Volume SMA ─────────────────────────────────────────────────────────────
    vol_sma = (sum(vols[-VOL_SMA_LEN:]) / VOL_SMA_LEN) if len(vols) >= VOL_SMA_LEN else None

    return {
        # raw
        'open':   candles[-1]['o'], 'high': candles[-1]['h'],
        'low':    candles[-1]['l'], 'close': candles[-1]['c'],
        'volume': candles[-1].get('v', 0.0), 'quote_volume': candles[-1].get('q', 0.0),
        'time':   candles[-1]['t'],
        # EMAs
        'ema9':   ema[9],  'ema21': ema[21], 'ema26': ema[26],
        'ema50':  ema[50], 'ema100': ema[100], 'ema200': ema[200],
        'ema9_prev': ema9_prev, 'ema26_prev': ema26_prev,
        'ema50_prev': ema50_prev, 'close_prev': close_prev,
        # ATR
        'atr': atr, 'atr_pct': atr_pct, 'atr_sma': atr_sma,
        # ADX / DI
        'adx': adx, 'di_plus': di_plus, 'di_minus': di_minus,
        # MACD
        'macd': macd, 'macd_signal': macd_signal, 'macd_hist': macd_hist,
        # volume
        'vol_sma': vol_sma,
        # htf bias is injected by the engine (0 here as a default)
        'htf_bias': 0,
    }


# ============================================================================
# CANDLE STORE  (one rolling deque per symbol; now carries volume)
# ============================================================================

class CandleStore:
    def __init__(self, symbols, limit=CANDLE_LIMIT):
        self._lock = threading.Lock()
        self._candles = {sym: deque(maxlen=limit) for sym in symbols}

    def seed(self, symbol, candle_list):
        with self._lock:
            for c in candle_list:
                self._candles[symbol].append(c)

    def push(self, symbol, candle):
        with self._lock:
            self._candles[symbol].append(candle)

    def get_list(self, symbol):
        with self._lock:
            return list(self._candles.get(symbol, []))

    def size(self, symbol):
        with self._lock:
            return len(self._candles.get(symbol, []))


# ============================================================================
# REST SEEDERS
# ============================================================================

def _parse_klines(rows):
    return [{'t': int(x[0]), 'o': float(x[1]), 'h': float(x[2]),
             'l': float(x[3]), 'c': float(x[4]), 'v': float(x[5]),
             'q': float(x[7])} for x in rows]


def seed_symbol(symbol, store, interval=INTERVAL, limit=CANDLE_LIMIT):
    """Fetch `limit` closed candles from live public fapi and load into store."""
    try:
        resp = requests.get(f"{REST_DATA_BASE}/v1/klines",
                            params={'symbol': symbol, 'interval': interval, 'limit': limit},
                            timeout=15)
        if resp.status_code != 200:
            log.warning(f"Seed {symbol} {interval}: HTTP {resp.status_code}")
            return False
        data = resp.json()
        if not isinstance(data, list) or not data:
            log.warning(f"Seed {symbol} {interval}: empty response")
            return False
        candles = _parse_klines(data[:-1])   # drop still-open last candle
        store.seed(symbol, candles)
        return True
    except Exception as e:
        log.error(f"Seed {symbol} {interval} error: {e}")
        return False


def seed_all(symbols, store, interval=INTERVAL, limit=CANDLE_LIMIT):
    threads = []
    for sym in symbols:
        t = threading.Thread(target=seed_symbol, args=(sym, store, interval, limit), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.05)   # gentle rate limiting
    for t in threads:
        t.join()
    sizes = ", ".join(f"{s}={store.size(s)}" for s in symbols[:4])
    log.info(f"Seeding ({interval}) complete. Sample sizes: {sizes} ...")


# ============================================================================
# WEBSOCKET ENGINE
# ============================================================================

class CandleEngine:
    """
    Combined WebSocket stream for all symbols (15m) plus 1h for HTF symbols.
    Calls callback(symbol, candle, indicators) on each closed 15m candle.
    `indicators['htf_bias']` carries the latest 1h EMA200 bias for HTF symbols.
    """

    def __init__(self, symbols, callback=None):
        self.symbols      = list(symbols)
        self.htf_symbols  = [s for s in HTF_SYMBOLS if s in self.symbols]
        self.callback     = callback
        self.store        = CandleStore(self.symbols)
        self._htf_store   = {s: deque(maxlen=HTF_LIMIT) for s in self.htf_symbols}
        self._htf_bias    = {s: 0 for s in self.htf_symbols}
        self._running     = False
        self._last_msg_ts = 0
        self._ws_app      = None

    # ── HTF bias ──────────────────────────────────────────────────────────────
    def htf_bias(self, symbol):
        return self._htf_bias.get(symbol, 0)

    def _recompute_htf_bias(self, symbol):
        candles = list(self._htf_store.get(symbol, []))
        if len(candles) < HTF_EMA + 1:
            return
        closes = [c['c'] for c in candles]
        e = ema_series(closes, HTF_EMA)
        if e[-1] is None:
            return
        last = closes[-1]
        self._htf_bias[symbol] = 1 if last > e[-1] else (-1 if last < e[-1] else 0)

    # ── lifecycle ──────────────────────────────────────────────────────────────
    def start(self):
        log.info(f"CandleEngine starting -- {len(self.symbols)} symbols "
                 f"({len(self.htf_symbols)} with 1h bias)")
        log.info(f"Testnet: {TESTNET}  |  Interval: {INTERVAL}")

        # Availability filter (drops symbols the demo venue doesn't list).
        available = filter_available_symbols(self.symbols)
        self.symbols     = available
        self.htf_symbols = [s for s in self.htf_symbols if s in available]
        self.store       = CandleStore(self.symbols)
        self._htf_store  = {s: deque(maxlen=HTF_LIMIT) for s in self.htf_symbols}
        self._htf_bias   = {s: 0 for s in self.htf_symbols}

        if not self.symbols:
            log.error("No tradeable symbols after availability filter — nothing to do.")
            return

        log.info("Seeding 15m history...")
        seed_all(self.symbols, self.store, INTERVAL, CANDLE_LIMIT)
        if self.htf_symbols:
            log.info("Seeding 1h history (HTF bias)...")
            for s in self.htf_symbols:
                seed_symbol(s, _DequeStore(self._htf_store[s]), HTF_INTERVAL, HTF_LIMIT)
                self._recompute_htf_bias(s)
                time.sleep(0.05)

        self._running = True
        watchdog = threading.Thread(target=self._watchdog_loop, daemon=True, name='ws_watchdog')
        watchdog.start()
        self._ws_loop()

    def stop(self):
        self._running = False

    # ── watchdog ───────────────────────────────────────────────────────────────
    def _watchdog_loop(self):
        STALE_AFTER_SEC = 90
        CHECK_EVERY_SEC = 30
        while self._running:
            time.sleep(CHECK_EVERY_SEC)
            if self._last_msg_ts == 0:
                continue
            silence = time.time() - self._last_msg_ts
            if silence > STALE_AFTER_SEC:
                log.warning(f"[WATCHDOG] No websocket messages for {silence:.0f}s -- forcing reconnect.")
                ws = self._ws_app
                if ws is not None:
                    try:
                        ws.close()
                    except Exception as e:
                        log.error(f"[WATCHDOG] Error closing stale socket: {e}")
                self._last_msg_ts = time.time()

    # ── websocket loop ──────────────────────────────────────────────────────────
    def _ws_loop(self):
        import websocket
        while self._running:
            streams = [f"{s.lower()}@kline_{INTERVAL}" for s in self.symbols]
            streams += [f"{s.lower()}@kline_{HTF_INTERVAL}" for s in self.htf_symbols]
            url = f"{WS_BASE}?streams={'/'.join(streams)}"
            log.info(f"Connecting WebSocket ({len(streams)} streams)...")

            ws = websocket.WebSocketApp(
                url,
                on_message=self._on_message,
                on_error=self._on_error,
                on_close=self._on_close,
                on_open=self._on_open,
            )
            self._ws_app = ws
            ws.run_forever()   # NO ping args — they block frames on websocket-client >= 1.7.0

            self._ws_app = None
            if self._running:
                log.warning(f"WebSocket disconnected. Reconnecting in {RECONNECT_SEC}s...")
                time.sleep(RECONNECT_SEC)

    def _on_open(self, ws):
        log.info("WebSocket connected [OK]")
        self._last_msg_ts = time.time()

    def _on_error(self, ws, error):
        log.error(f"WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        log.info(f"WebSocket closed: {code} {msg}")

    def _on_message(self, ws, raw):
        self._last_msg_ts = time.time()
        try:
            msg  = json.loads(raw)
            data = msg.get('data', {})
            k    = data.get('k', {})
            if not k.get('x', False):
                return   # candle not closed yet

            symbol   = data.get('s', '').upper()
            interval = k.get('i', '')
            candle = {
                't': int(k['t']), 'o': float(k['o']), 'h': float(k['h']),
                'l': float(k['l']), 'c': float(k['c']),
                'v': float(k.get('v', 0.0)), 'q': float(k.get('q', 0.0)),
            }

            if interval == HTF_INTERVAL:
                if symbol in self._htf_store:
                    self._htf_store[symbol].append(candle)
                    self._recompute_htf_bias(symbol)
                return

            if interval != INTERVAL or symbol not in self.symbols:
                return

            self.store.push(symbol, candle)
            candle_list = self.store.get_list(symbol)
            indicators  = compute_indicators(candle_list)

            if indicators and self.callback:
                indicators['htf_bias'] = self.htf_bias(symbol)
                try:
                    self.callback(symbol, candle, indicators)
                except Exception as e:
                    log.error(f"Callback error for {symbol}: {e}", exc_info=True)

        except Exception as e:
            log.error(f"Message parse error: {e}", exc_info=True)


class _DequeStore:
    """Tiny adapter so seed_symbol() can populate an existing 1h deque."""
    def __init__(self, dq):
        self._dq = dq
    def seed(self, symbol, candle_list):
        for c in candle_list:
            self._dq.append(c)
    def size(self, symbol):
        return len(self._dq)


# ============================================================================
# STANDALONE TEST
# ============================================================================

if __name__ == '__main__':
    print(f"Universe: {len(SYMBOLS)} symbols | HTF (1h) symbols: {HTF_SYMBOLS}")
    print("Connecting (Ctrl+C to stop)...\n")

    def _cb(symbol, candle, ind):
        ts = datetime.fromtimestamp(candle['t'] / 1000, tz=timezone.utc).strftime('%H:%M')
        print(f"{symbol:<14} {ts}  close={candle['c']:.4f}  "
              f"EMA50={ind['ema50']:.4f} ADX={ind['adx']:.1f} "
              f"MACD={ind['macd']:.5f} htf={ind['htf_bias']:+d}")

    engine = CandleEngine(SYMBOLS, callback=_cb)
    try:
        engine.start()
    except KeyboardInterrupt:
        print("\nStopped.")
