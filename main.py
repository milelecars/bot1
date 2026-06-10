"""
MAIN — Full Bot Entry Point  (Futures Edition)
================================================
"""
import sys, io, logging, os, time, hmac, hashlib, requests, csv
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify, request, render_template

load_dotenv()

print("TESTNET:", os.environ.get("TESTNET"))
print("BINANCE_API_KEY loaded:", bool(os.environ.get("BINANCE_API_KEY")))
print("BINANCE_API_SECRET loaded:", bool(os.environ.get("BINANCE_API_SECRET")))

# ── Flask proxy app ──────────────────────────────────────────────────────────
app = Flask(__name__)

TESTNET = os.environ.get('TESTNET', 'true').lower() == 'true'
if TESTNET:
    BINANCE_BASE = 'https://demo-fapi.binance.com/fapi'
else:
    BINANCE_BASE = 'https://fapi.binance.com/fapi'

API_KEY    = os.environ.get('BINANCE_API_KEY', '')
API_SECRET = os.environ.get('BINANCE_API_SECRET', '')


def _sign(params: dict) -> dict:
    params['timestamp'] = int(time.time() * 1000)
    qs  = '&'.join(f'{k}={v}' for k, v in params.items())
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    params['signature'] = sig
    return params


def binance_signed(path, params={}):
    p = _sign(dict(params))
    qs  = '&'.join(f'{k}={v}' for k, v in p.items())
    url = f'{BINANCE_BASE}{path}?{qs}'
    return requests.get(url, headers={'X-MBX-APIKEY': API_KEY}, timeout=10).json()




@app.route('/')
def dashboard():
    return render_template('index.html')

# ── Futures proxy routes ─────────────────────────────────────────────────────

@app.route('/proxy/fapi/v2/account')
def proxy_fapi_account():
    return jsonify(binance_signed('/v2/account'))


@app.route('/proxy/fapi/v1/openOrders')
def proxy_fapi_open_orders():
    sym = request.args.get('symbol', '')
    params = {'symbol': sym} if sym else {}
    return jsonify(binance_signed('/v1/openOrders', params))


@app.route('/proxy/fapi/v1/allOrders')
def proxy_fapi_all_orders():
    sym = request.args.get('symbol', '')
    return jsonify(binance_signed('/v1/allOrders', {'symbol': sym, 'limit': 500}))


@app.route('/proxy/fapi/v1/ticker/price')
def proxy_fapi_ticker_price():
    sym = request.args.get('symbol', '')
    url = f'{BINANCE_BASE}/v1/ticker/price?symbol={sym}'
    return jsonify(requests.get(url, timeout=10).json())


@app.route('/proxy/fapi/v2/positionRisk')
def proxy_fapi_position_risk():
    sym = request.args.get('symbol', '')
    params = {'symbol': sym} if sym else {}
    return jsonify(binance_signed('/v2/positionRisk', params))


# ── Local history helpers ──────────────────────────────────────────────────────

def load_trade_log_csv(path='trade_log.csv'):
    if not os.path.exists(path):
        return []
    try:
        with open(path, 'r', encoding='utf-8', newline='') as f:
            rows = list(csv.DictReader(f))
        return rows
    except Exception as e:
        log = logging.getLogger('main')
        log.warning(f"Could not read {path}: {e}")
        return []


# ── Bot-internal data routes ─────────────────────────────────────────────────

@app.route('/proxy/trades')
def proxy_trades():
    # Priority: Supabase -> in-memory session history -> local CSV history
    if manager:
        rows = manager.supabase.select_all('trades')
        if rows:
            return jsonify(rows)

    if manager and manager.closed_positions:
        return jsonify(manager.closed_positions)

    csv_rows = load_trade_log_csv()
    if csv_rows:
        return jsonify(csv_rows)

    return jsonify([])

@app.route('/proxy/open_positions')
def proxy_open_positions():
    """Return live open positions with duration info from the in-memory tracker."""
    if manager:
        return jsonify(manager.get_open_positions_list())
    return jsonify([])


@app.route('/proxy/stats')
def proxy_stats():
    """Return strategy-level stats: consecutive losses, open count."""
    if manager:
        return jsonify(manager.get_stats())
    return jsonify({'consec_losses': {'S2': 0}, 'open_count': 0})


@app.route('/health')
def health():
    """Liveness probe for Railway: reports engine/manager state."""
    return jsonify({
        'status':     'ok',
        'testnet':    TESTNET,
        'symbols':    len(SYMBOLS),
        'strategies': list(detectors.keys()),
        'engine_up':  bool(engine and getattr(engine, '_running', False)),
        'open_positions': (manager.get_stats().get('open_count', 0) if manager else 0),
    })


@app.after_request
def cors(r):
    r.headers['Access-Control-Allow-Origin'] = '*'
    return r


# ── Windows UTF-8 fix ────────────────────────────────────────────────────────
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except AttributeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s  %(levelname)-7s  %(name)-16s  %(message)s',
    datefmt  = '%Y-%m-%d %H:%M:%S',
    handlers = [
        logging.FileHandler('bot.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger('main')
logging.getLogger('werkzeug').setLevel(logging.ERROR)

from step1_candle_engine   import CandleEngine, SYMBOLS, TESTNET
from step2_signal_detector import SignalDetector
from step3_order_manager   import OrderManager
from step4_telegram        import AlertManager

# The four backtest-ported detectors + registry routing.
from strategy_axispro  import AxisProStrategy
from strategy_breakout import BreakoutStrategy
from strategy_ict      import ICTStrategy
from strategy_s1_ladder import S1LadderStrategy
from strategy_registry import enabled_strategies, strategy_for_symbol

# Map each registry strategy_id to its detector constructor. S2 is special
# (its own FVG detector class); the rest take their spec.
def _build_detector(spec):
    sid = spec.strategy_id
    if sid == 'S2_FVG_RETEST':
        return SignalDetector()
    if sid == 'AXISPRO':
        return AxisProStrategy(spec)
    if sid == 'BREAKOUT_NY4H':
        return BreakoutStrategy(spec)
    if sid == 'ICT_NDOG':
        return ICTStrategy(spec)
    if sid == 'S1_EMA_CROSS':
        return S1LadderStrategy(spec)
    raise ValueError(f"No detector wired for strategy '{sid}'")


detectors = {}          # strategy_id -> detector
route      = {}          # symbol -> detector  (disjoint, so 1:1)
engine = manager = alerts = None


def candle_callback(symbol, candle, indicators):
    candle_list = engine.store.get_list(symbol)
    # 1) Manage any EXISTING position on this symbol first, so a position opened
    #    on this same candle is not trailed on its own entry bar.
    if manager:
        manager.on_candle_update(symbol, candle, indicators)
    # 2) Route the closed candle to the single detector that owns this symbol.
    det = route.get(symbol)
    if det is not None:
        det.on_candle_close(symbol, candle, indicators, candle_list)


def on_signal_with_alert(signal):
    if alerts:  alerts.on_signal(signal)
    if manager: manager.on_signal(signal)


def main():
    global engine, manager, alerts

    print(f"""
+------------------------------------------------------------+
|  MILELE PRIME -- Multi-Strategy Futures Bot                |
|  Strategies : {len(enabled_strategies())}  (S2, AxisPro, Breakout, ICT, S1)
|  Symbols    : {len(SYMBOLS)} (disjoint across strategies)
|  Timeframe  : 15m candles
|  Mode       : {'TESTNET (paper money)' if TESTNET else 'LIVE'}
|  Started    : {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
+------------------------------------------------------------+
""")

    alerts = AlertManager()

    # Build one detector per enabled strategy and the symbol -> detector route
    # map (symbol lists are disjoint, so each symbol maps to exactly one).
    for spec in enabled_strategies():
        det = _build_detector(spec)
        det.on_signal = on_signal_with_alert
        detectors[spec.strategy_id] = det
        for sym in spec.symbols:
            route[sym] = det
    log.info(f"Detectors ready: {', '.join(detectors)} | routing {len(route)} symbols")

    try:
        manager = OrderManager(detectors=detectors, alerts=alerts)
    except ValueError as e:
        log.error(str(e)); sys.exit(1)

    _orig_open = manager._log_trade_open
    def _patched_open(pos):
        _orig_open(pos)
        if alerts:
            alerts.on_trade_opened(pos.symbol, pos.strategy, pos.direction,
                                   pos.entry_price, pos.sl_price, pos.tp_price, pos.quantity)
    manager._log_trade_open = _patched_open

    _orig_close = manager._log_trade_close
    def _patched_close(pos, outcome, exit_price=None):
        _orig_close(pos, outcome, exit_price=exit_price)
        if alerts:
            alert_exit = exit_price or (pos.tp_price if outcome == 'WIN' else pos.sl_price)
            alerts.on_trade_closed(pos.symbol, pos.strategy, pos.direction,
                                   pos.entry_price, alert_exit, outcome)
    manager._log_trade_close = _patched_close

    engine = CandleEngine(SYMBOLS, callback=candle_callback)
    alerts.send_startup(len(SYMBOLS), TESTNET)

    log.info("All components ready. Starting candle engine...")

    try:
        engine.start()
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
        if alerts: alerts.on_error("Bot stopped by user (KeyboardInterrupt)")


# ── Bot bootstrap ────────────────────────────────────────────────────────────
# IMPORTANT: starting the bot must happen at MODULE LEVEL so it works under both:
#   1) `python main.py`           (local / VPS direct run)
#   2) `gunicorn main:app`        (Railway / Heroku / any WSGI host)
#
# If the bot were only started inside `if __name__ == '__main__':`, gunicorn
# would import this module, serve Flask, and never run the trading thread —
# the dashboard would load but no signals would fire and no trades would open.
# That was the bug: on Railway the bot ran the Flask routes only.
import threading

_bot_thread_lock  = threading.Lock()
_bot_thread       = None

def _start_bot_once():
    """Start the trading thread exactly once per process."""
    global _bot_thread
    # Skip the Werkzeug auto-reloader's parent process so we don't start twice in dev
    if os.environ.get('WERKZEUG_RUN_MAIN') == 'false':
        return
    with _bot_thread_lock:
        if _bot_thread is not None and _bot_thread.is_alive():
            return
        log.info("Starting bot trading thread...")
        _bot_thread = threading.Thread(target=main, daemon=True, name='bot_main')
        _bot_thread.start()

# Start the bot when the module is imported (covers gunicorn) and also when
# run directly. The lock above guarantees a single start per process — if
# gunicorn is configured with multiple workers, each worker will start its
# own bot, which would cause duplicate trades. Keep gunicorn at --workers 1
# (see Procfile) for that reason.
_start_bot_once()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)),
            use_reloader=False)  # reloader would fork and double-start the bot
