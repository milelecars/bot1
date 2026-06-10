const fmtNum = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n)
    ? n.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d })
    : '--';
};

const fmtPct = (v, d = 2) => {
  const n = Number(v);
  return Number.isFinite(n) ? `${n.toFixed(d)}%` : '--';
};

const textClass = (n) => !Number.isFinite(n) ? '' : (n > 0 ? 'positive' : (n < 0 ? 'negative' : ''));

const esc = (s) => String(s ?? '').replace(/[&<>"']/g,
  c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

let timer = null;
let latestTrades = [];

// ─────────────────────────────────────────────────────────────────────────────
// STRATEGY METADATA  (mirrors strategy_registry.py — display side only)
// ─────────────────────────────────────────────────────────────────────────────
// `id`        : the value written to the DB `strategy` column (and the detector
//               key returned by /health).
// `consecKey` : how the backend keys consecutive losses. step3 uses
//               pos.strategy.split('_')[0], so S2_FVG_RETEST -> "S2",
//               BREAKOUT_NY4H -> "BREAKOUT", ICT_NDOG -> "ICT", AXISPRO -> "AXISPRO".
// `accent`    : a colour name from style.css (--blue/--green/--amber/--purple/...).
//
// Which cards actually render is decided at runtime from /health (the set of
// detectors the bot has ENABLED). Flip `enabled` in the Python registry and the
// dashboard follows automatically — nothing here needs touching to turn a
// strategy on or off. This list only supplies the nice label/description/accent.
const STRATEGY_META = [
  { id: 'S2_FVG_RETEST', short: 'S2',   name: 'S2 — FVG RETEST',     chip: 'FVG retest + EMA50/100 + ADX',         consecKey: 'S2',       accent: 'amber'  },
  { id: 'AXISPRO',       short: 'AXIS', name: 'AXISPRO — BOS + FIB', chip: 'BOS + Fibonacci pullback',             consecKey: 'AXISPRO',  accent: 'purple' },
  { id: 'BREAKOUT_NY4H', short: 'BRK',  name: 'BREAKOUT — NY4H',     chip: 'NY 09:00–13:00 opening-range break',   consecKey: 'BREAKOUT', accent: 'cyan'   },
  { id: 'ICT_NDOG',      short: 'ICT',  name: 'ICT — OPENING-GAP',   chip: 'New-day opening-gap breakout',         consecKey: 'ICT',      accent: 'green'  },
  { id: 'S1_EMA_CROSS',  short: 'S1',   name: 'S1 — EMA CROSS',      chip: 'EMA9/26 cross + trailing ladder',      consecKey: 'S1',       accent: 'blue'   },
];

// Short labels for the trade-history table, incl. historical ids still in the DB.
const TABLE_LABELS = { S2_MA44_BOUNCE: 'S2·MA44' };

const PALETTE = ['amber', 'purple', 'cyan', 'green', 'blue', 'slate'];

// Strategies currently shown (resolved from /health each refresh; defaults to all).
let displayStrategies = STRATEGY_META.slice();

function metaForId(id) {
  const found = STRATEGY_META.find(m => m.id === id);
  if (found) return found;
  const short = String(id || '').split('_')[0] || '--';
  return { id, short, name: id || '--', chip: '', consecKey: short, accent: 'slate' };
}

// Build the render list from /health's enabled-detector list. Falls back to the
// full STRATEGY_META list if /health is unavailable, so cards never vanish on a
// transient error.
function resolveDisplayStrategies(health) {
  const ids = Array.isArray(health && health.strategies) ? health.strategies : [];
  if (!ids.length) return STRATEGY_META.slice();
  return ids.map((id, i) => {
    const m = STRATEGY_META.find(x => x.id === id);
    if (m) return m;
    const short = String(id).split('_')[0] || '--';
    return { id, short, name: id, chip: '', consecKey: short, accent: PALETTE[i % PALETTE.length] };
  });
}

const nameStyle = (accent) => `color:var(--${accent});background:var(--${accent}-soft)`;
const tagStyle  = (accent) => `color:var(--${accent});background:var(--${accent}-soft)`;

function consecFor(stats, meta) {
  const cl = (stats && stats.consec_losses) || {};
  return cl[meta.consecKey] ?? cl[meta.id] ?? 0;
}

async function getJson(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} returned ${res.status}`);
  return await res.json();
}

function parseTradeRows(rows) {
  return (rows || []).filter(r => (r.outcome || '').toUpperCase() !== 'OPEN');
}

function summarizeTrades(rows, strategyIds) {
  const trades = parseTradeRows(rows);
  const today = new Date().toISOString().slice(0, 10);
  let pnl = 0, todayPnl = 0, wins = 0, losses = 0;

  // One bucket per displayed strategy. Trades whose strategy id doesn't match a
  // displayed strategy (e.g. a retired strategy still in the DB) still count
  // toward the OVERALL totals below — they just don't land in a per-strategy box.
  const ids = (strategyIds && strategyIds.length) ? strategyIds : STRATEGY_META.map(m => m.id);
  const byStrategy = {};
  for (const id of ids) byStrategy[id] = { trades: 0, pnl: 0, wins: 0, losses: 0 };

  for (const t of trades) {
    const p = Number(t.pnl_usdt || 0);
    pnl += p;
    const closeTime = `${t.close_time || ''}`;
    if (closeTime.startsWith(today)) todayPnl += p;
    // LP_WIN is a (smaller) win and counts toward win stats
    const isWin  = (t.outcome === 'WIN' || t.outcome === 'LP_WIN');
    const isLoss = (t.outcome === 'LOSS');
    if (isWin)  wins++;
    if (isLoss) losses++;
    const s = byStrategy[t.strategy];
    if (s) {
      s.trades++;
      s.pnl += p;
      if (isWin)  s.wins++;
      if (isLoss) s.losses++;
    }
  }

  return { trades, pnl, todayPnl, wins, losses, byStrategy };
}

function updateChip(id, label, state = 'neutral') {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = label;
  el.className = `status-chip ${state}`;
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function renderSummary(account, stats, summary) {
  const assets = Array.isArray(account.assets) ? account.assets : [];
  const usdt = assets.find(a => a.asset === 'USDT');

  setText('usdtBalance', usdt ? `$${fmtNum(usdt.availableBalance)}` : '--');
  setText('walletSub', usdt ? 'Available in wallet' : 'Waiting for wallet response');

  const totalPnl = document.getElementById('totalPnl');
  totalPnl.textContent = `${summary.pnl >= 0 ? '+' : ''}$${fmtNum(summary.pnl)}`;
  totalPnl.className = `value ${textClass(summary.pnl)}`;

  const todayPnl = document.getElementById('todayPnl');
  todayPnl.textContent = `${summary.todayPnl >= 0 ? '+' : ''}$${fmtNum(summary.todayPnl)}`;
  todayPnl.className = `value ${textClass(summary.todayPnl)}`;

  const total = summary.wins + summary.losses;
  const wr = total ? (summary.wins * 100 / total) : 0;
  const winRate = document.getElementById('winRate');
  winRate.textContent = fmtPct(wr, 1);
  winRate.className = `value ${textClass(wr === 0 ? NaN : wr - 50)}`;
  setText('winSub', `${summary.wins}W / ${summary.losses}L`);

  setText('openCount', `${stats.open_count || 0}`);
  setText('totalTrades', `${summary.trades.length}`);
}

// Render one card per displayed strategy into #strategyList.
function renderStrategies(stats, summary, strategies) {
  const box = document.getElementById('strategyList');
  if (!box) return;

  if (!strategies.length) {
    box.className = 'strategy-list empty';
    box.textContent = 'No strategies enabled';
    return;
  }

  box.className = 'strategy-list';
  box.innerHTML = strategies.map(meta => {
    const data = (summary.byStrategy && summary.byStrategy[meta.id]) || { trades: 0, pnl: 0, wins: 0, losses: 0 };
    const wr = data.trades ? (data.wins * 100 / data.trades) : 0;
    const consec = consecFor(stats, meta);
    const pnlTxt = `${data.pnl >= 0 ? '+' : ''}${fmtNum(data.pnl)}`;
    return `
      <div class="strategy-box">
        <div class="strategy-topline">
          <div class="strategy-name" style="${nameStyle(meta.accent)}">${esc(meta.name)}</div>
          ${meta.chip ? `<span class="mini-chip">${esc(meta.chip)}</span>` : ''}
        </div>
        <div class="strategy-stats">
          <div><span>Trades</span><strong>${data.trades}</strong></div>
          <div><span>Win Rate</span><strong>${fmtPct(wr, 0)}</strong></div>
          <div><span>P&amp;L</span><strong class="${textClass(data.pnl)}">${pnlTxt}</strong></div>
          <div><span>Consec. Losses</span><strong>${consec || 0}</strong></div>
        </div>
      </div>`;
  }).join('');
}

function renderWallet(account) {
  const assets = (account.assets || []).filter(a => Number(a.walletBalance || a.balance || 0) > 0 || Number(a.availableBalance || 0) > 0);
  const box = document.getElementById('walletAssets');
  setText('walletAssetsCount', `${assets.length} assets`);

  if (!assets.length) {
    box.className = 'wallet-list empty';
    box.textContent = account.msg || 'No wallet data';
    return;
  }

  box.className = 'wallet-list';
  box.innerHTML = assets.map(a => `
    <div class="wallet-row">
      <div class="wallet-asset">
        <span class="asset-dot">${(a.asset || '?').slice(0, 4)}</span>
        <div>
          <strong>${a.asset}</strong>
          <div class="muted">Available ${fmtNum(a.availableBalance || a.balance || 0)}</div>
        </div>
      </div>
      <div><strong>${fmtNum(a.walletBalance || a.balance || 0)}</strong></div>
    </div>`).join('');
}

function renderPositions(positions) {
  const box = document.getElementById('openPositions');
  setText('openPositionsPill', `${positions.length} / 30`);

  if (!positions.length) {
    box.className = 'positions-grid empty';
    box.textContent = 'No open positions';
    return;
  }

  box.className = 'positions-grid';
  box.innerHTML = positions.map(p => {
    // Tag the position with whichever strategy owns it (any of the five).
    const meta = metaForId(p.strategy);
    const sideClass = (p.direction || '').toLowerCase();
    const symbolLabel = (p.symbol || '--').replace('USDT', '/USDT');
    return `
      <div class="position-card">
        <div class="position-head">
          <div>
            <h3>${symbolLabel}</h3>
            <div class="position-sub">Opened ${p.open_time || '--'}</div>
          </div>
          <div class="tags">
            <span class="tag" style="${tagStyle(meta.accent)}">${esc(meta.short)}</span>
            <span class="tag ${sideClass}">${p.direction || '--'}</span>
          </div>
        </div>
        <div class="kv">
          <div class="k">Entry</div><div>${fmtNum(p.entry_price, 6)}</div>
          <div class="k">Margin</div><div>$${fmtNum(p.margin_usdt)} x ${p.leverage}</div>
          <div class="k">Qty</div><div>${fmtNum(p.quantity, 3)}</div>
          <div class="k">Duration</div><div>${p.duration || '--'}</div>
          <div class="k">Take Profit</div><div class="positive">${fmtNum(p.tp_price, 6)}</div>
          <div class="k">Stop Loss</div><div class="negative">${fmtNum(p.sl_price, 6)}</div>
          <div class="k">Strategy</div><div>${esc(p.strategy || '--')}</div>
        </div>
      </div>`;
  }).join('');
}

function getFilteredTrades(rows) {
  const search = (document.getElementById('tradeSearch')?.value || '').trim().toUpperCase();
  const outcome = document.getElementById('outcomeFilter')?.value || 'ALL';
  const side = document.getElementById('sideFilter')?.value || 'ALL';
  const strategy = document.getElementById('strategyFilter')?.value || 'ALL';

  return parseTradeRows(rows).filter(t => {
    if (search && !(t.symbol || '').toUpperCase().includes(search)) return false;
    if (outcome !== 'ALL' && (t.outcome || '') !== outcome) return false;
    if (side !== 'ALL' && (t.direction || '') !== side) return false;
    if (strategy !== 'ALL' && (t.strategy || '') !== strategy) return false;
    return true;
  }).slice().reverse();
}

function tradeStrategyLabel(id) {
  if (TABLE_LABELS[id]) return TABLE_LABELS[id];
  const m = STRATEGY_META.find(x => x.id === id);
  return m ? m.short : (id || '--');
}

function renderTrades(rows) {
  latestTrades = rows || [];
  const trades = getFilteredTrades(latestTrades);
  setText('tradeCountPill', `${trades.length} trades`);
  const body = document.getElementById('tradeRows');

  if (!trades.length) {
    body.innerHTML = '<tr><td colspan="10" class="muted center">No trades match the current filters</td></tr>';
    return;
  }

  body.innerHTML = trades.map(t => {
    const pnl = Number(t.pnl_usdt || 0);
    const pnlPct = Number(t.pnl_pct || 0);
    // LP_WIN (locked-in profit) is a positive outcome like WIN
    const isWinLike  = (t.outcome === 'WIN' || t.outcome === 'LP_WIN');
    const isLossLike = (t.outcome === 'LOSS');
    const outcomeClass = isWinLike ? 'positive' : (isLossLike ? 'negative' : '');
    const strategyShort = tradeStrategyLabel(t.strategy);
    return `<tr>
      <td>${t.open_time || '--'}<br><span class="muted">${t.close_time || '--'}</span></td>
      <td><strong>${t.symbol || '--'}</strong></td>
      <td>${esc(strategyShort)}</td>
      <td>${t.direction || '--'}</td>
      <td>${fmtNum(t.entry_price, 6)}</td>
      <td>SL ${fmtNum(t.sl_price, 6)}<br><span class="muted">TP ${fmtNum(t.tp_price, 6)}</span></td>
      <td class="${outcomeClass}">${t.outcome || '--'}</td>
      <td class="${textClass(pnl)}">${pnl >= 0 ? '+' : ''}${fmtNum(pnl)}</td>
      <td>${fmtNum(t.fee_usdt || 0)}</td>
      <td class="${textClass(pnlPct)}">${pnlPct >= 0 ? '+' : ''}${fmtPct(pnlPct, 2)}</td>
    </tr>`;
  }).join('');
}

// Keep the trade-history strategy filter in sync with the strategies on show.
function populateStrategyFilter(strategies) {
  const sel = document.getElementById('strategyFilter');
  if (!sel) return;
  const prev = sel.value || 'ALL';
  const opts = ['<option value="ALL">All strategies</option>']
    .concat(strategies.map(m => `<option value="${esc(m.id)}">${esc(m.short)}</option>`));
  const html = opts.join('');
  if (sel.innerHTML !== html) sel.innerHTML = html;
  // Restore previous selection if it still exists.
  sel.value = [...sel.options].some(o => o.value === prev) ? prev : 'ALL';
}

function renderStatus(account, stats, openPositions, trades, strategies) {
  const items = [];
  const accountOk = account && !account.code && !account.msg;

  updateChip('botStateChip', `Bot Live - ${openPositions.length} open`, 'neutral');
  updateChip('apiStateChip', accountOk ? 'Binance API OK' : 'API Needs Attention', accountOk ? 'good' : 'bad');

  items.push(`<li>Binance account API: <strong class="${accountOk ? 'positive' : 'negative'}">${accountOk ? 'OK' : 'Error'}</strong>${account.msg ? ` - ${account.msg}` : ''}</li>`);
  items.push(`<li>Strategies enabled: <strong>${strategies.length}</strong></li>`);
  items.push(`<li>Open positions tracker: <strong>${openPositions.length}</strong> active</li>`);
  items.push(`<li>Closed trades loaded: <strong>${parseTradeRows(trades).length}</strong></li>`);
  const consecTxt = strategies
    .map(m => `${esc(m.short)}: ${consecFor(stats, m) || 0}`)
    .join(' · ');
  items.push(`<li>Consecutive losses — <strong>${consecTxt || '--'}</strong></li>`);

  document.getElementById('statusList').innerHTML = items.join('');
}

async function refresh() {
  updateChip('apiStateChip', 'Refreshing...', 'neutral');
  try {
    const [account, stats, openPositions, trades, health] = await Promise.all([
      getJson('/proxy/fapi/v2/account').catch(() => ({ msg: 'Failed to reach account API' })),
      getJson('/proxy/stats').catch(() => ({ consec_losses: {}, open_count: 0 })),
      getJson('/proxy/open_positions').catch(() => ([])),
      getJson('/proxy/trades').catch(() => ([])),
      getJson('/health').catch(() => ({})),
    ]);

    displayStrategies = resolveDisplayStrategies(health);
    populateStrategyFilter(displayStrategies);

    const safePositions = Array.isArray(openPositions) ? openPositions : [];
    const safeTrades = Array.isArray(trades) ? trades : [];
    const summary = summarizeTrades(safeTrades, displayStrategies.map(s => s.id));
    renderSummary(account, stats, summary);
    renderStrategies(stats, summary, displayStrategies);
    renderWallet(account);
    renderPositions(safePositions);
    renderTrades(safeTrades);
    renderStatus(account, stats, safePositions, safeTrades, displayStrategies);
    setText('lastUpdated', `Updated ${new Date().toLocaleTimeString()}`);
  } catch (e) {
    console.error(e);
    updateChip('apiStateChip', 'Refresh Failed', 'bad');
  }
}

function applyTimer() {
  if (timer) clearInterval(timer);
  const sec = Number(document.getElementById('refreshSelect').value || 0);
  if (sec > 0) timer = setInterval(refresh, sec * 1000);
}

function bindFilters() {
  ['tradeSearch', 'outcomeFilter', 'sideFilter', 'strategyFilter'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener('input', () => renderTrades(latestTrades));
    el.addEventListener('change', () => renderTrades(latestTrades));
  });
}

document.getElementById('refreshBtn').addEventListener('click', refresh);
document.getElementById('refreshSelect').addEventListener('change', applyTimer);
bindFilters();
applyTimer();
refresh();
