#!/usr/bin/env node
/**
 * Standalone Binance USDC-M 0%-Fee Market-Making Tick Replay Tester
 *
 * Completely decoupled from the tick collector. Reads gzipped NDJSON tick chunks
 * captured by tokyo_engine/tick_collector_go (either from S3 manifest or a local dir)
 * and replays bookTicker + aggTrade events with conservative queue position,
 * GTX Post-Only enforcement, cancel-race latency, and idle vs active regime tracking.
 *
 * Usage:
 *   node research/usdc_mm_tester.js --from-s3 --last 12 --latency 10 --clip 50
 *   node research/usdc_mm_tester.js --dir ./research/tick_cache --latencies 10,25,50
 */

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const https = require('https');
const readline = require('readline');

const DEFAULT_MANIFEST_URL =
  'https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/ticks/manifest.json';

const TICK_SIZE = {
  BTC: 0.1,
  ETH: 0.01,
  SOL: 0.01,
  XRP: 0.0001,
  DOGE: 0.00001,
  NEAR: 0.001,
  UNI: 0.001,
  '1000PEPE': 0.0000001,
  BNB: 0.01,
  HYPE: 0.001,
};

function parseArgs(argv) {
  const cfg = {
    fromS3: false,
    dir: '',
    cacheDir: path.join(__dirname, 'tick_cache'),
    manifestUrl: DEFAULT_MANIFEST_URL,
    last: 0,
    coins: null,
    latencies: [10, 25],
    clipUsd: 50,
    maxInvClips: 3,
    makerFeeBps: 0.0,
    takerFeeBps: 4.0,
    stopBps: 25.0,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--from-s3') cfg.fromS3 = true;
    else if (a === '--dir' && argv[i + 1]) cfg.dir = argv[++i];
    else if (a === '--cache-dir' && argv[i + 1]) cfg.cacheDir = argv[++i];
    else if (a === '--manifest' && argv[i + 1]) cfg.manifestUrl = argv[++i];
    else if (a === '--last' && argv[i + 1]) cfg.last = parseInt(argv[++i], 10) || 0;
    else if (a === '--coins' && argv[i + 1]) cfg.coins = argv[++i].toUpperCase().split(',').map(s => s.trim()).filter(Boolean);
    else if (a === '--latency' && argv[i + 1]) cfg.latencies = [parseFloat(argv[++i]) || 10];
    else if (a === '--latencies' && argv[i + 1]) cfg.latencies = argv[++i].split(',').map(x => parseFloat(x)).filter(x => x >= 0);
    else if (a === '--clip' && argv[i + 1]) cfg.clipUsd = parseFloat(argv[++i]) || 50;
    else if (a === '--max-inv' && argv[i + 1]) cfg.maxInvClips = parseInt(argv[++i], 10) || 3;
    else if (a === '--maker-fee' && argv[i + 1]) cfg.makerFeeBps = parseFloat(argv[++i]) || 0;
    else if (a === '--taker-fee' && argv[i + 1]) cfg.takerFeeBps = parseFloat(argv[++i]) || 4;
    else if (a === '--stop-bps' && argv[i + 1]) cfg.stopBps = parseFloat(argv[++i]) || 25;
  }
  if (!cfg.dir && !cfg.fromS3) cfg.fromS3 = true;
  return cfg;
}

function httpGetBuffer(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, res => {
        if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
          return resolve(httpGetBuffer(res.headers.location));
        }
        if (res.statusCode !== 200) {
          res.resume();
          return reject(new Error(`HTTP ${res.statusCode} for ${url}`));
        }
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      })
      .on('error', reject);
  });
}

async function resolveInputFiles(cfg) {
  if (cfg.dir) {
    const entries = fs
      .readdirSync(cfg.dir)
      .filter(f => f.endsWith('.ndjson.gz') || f.endsWith('.ndjson'))
      .sort();
    const sel = cfg.last > 0 ? entries.slice(-cfg.last) : entries;
    return { files: sel.map(f => path.join(cfg.dir, f)), manifest: null };
  }

  fs.mkdirSync(cfg.cacheDir, { recursive: true });
  const raw = await httpGetBuffer(cfg.manifestUrl);
  const manifest = JSON.parse(raw.toString('utf8'));
  const chunks = Array.isArray(manifest.chunks) ? manifest.chunks : [];
  const sel = cfg.last > 0 ? chunks.slice(-cfg.last) : chunks;
  const files = [];
  for (const ch of sel) {
    const localPath = path.join(cfg.cacheDir, ch.file);
    if (!fs.existsSync(localPath) || fs.statSync(localPath).size !== ch.bytes) {
      const buf = await httpGetBuffer(ch.url);
      fs.writeFileSync(localPath, buf);
    }
    files.push(localPath);
  }
  return { files, manifest };
}

function roundTick(px, tick, dir) {
  if (!tick || tick <= 0) return px;
  const steps = px / tick;
  const r = dir < 0 ? Math.floor(steps + 1e-9) : dir > 0 ? Math.ceil(steps - 1e-9) : Math.round(steps);
  return Number((r * tick).toFixed(10));
}

function splitSymbol(sym) {
  if (sym.endsWith('USDC')) return { base: sym.slice(0, -4), quote: 'USDC' };
  if (sym.endsWith('USDT')) return { base: sym.slice(0, -4), quote: 'USDT' };
  return null;
}

class CoinStrategySim {
  constructor(strategyId, base, cfg, latencyMs) {
    this.strategyId = strategyId;
    this.base = base;
    this.cfg = cfg;
    this.latencyMs = latencyMs;
    this.tick = TICK_SIZE[base] || 0.0001;

    // Live resting orders (null or { px, qty, queueAhead, placedMs, cancelReqMs, regime })
    this.bid = null;
    this.ask = null;
    // Pending delayed commands: { dueMs, side, action: 'SET'|'CANCEL', px, qty, regime }
    this.pending = [];

    // Position & PnL state
    this.posQty = 0; // signed base units
    this.avgEntry = 0;
    this.realizedUsd = 0;
    this.feesUsd = 0;
    this.peakEqUsd = 0;
    this.maxDdUsd = 0;
    this.lastMid = 0;

    // Diagnostics
    this.fills = 0;
    this.makerFills = 0;
    this.takerStops = 0;
    this.cancelRaceFills = 0;
    this.gtxRejects = 0;
    this.closedTrips = 0;
    this.wins = 0;
    this.idleRealizedUsd = 0;
    this.activeRealizedUsd = 0;
    this.idleFills = 0;
    this.activeFills = 0;
    this.lastQuoteDecisionMs = 0;

    // 5-second post-fill markout tracking
    this.pendingMarkouts = []; // { dueMs, side, fillPx }
    this.markoutSumBps = 0;
    this.markoutCount = 0;
  }

  equityUsd() {
    const upnl = this.posQty !== 0 && this.lastMid > 0 ? this.posQty * (this.lastMid - this.avgEntry) : 0;
    return this.realizedUsd - this.feesUsd + upnl;
  }

  updateDrawdown() {
    const eq = this.equityUsd();
    if (eq > this.peakEqUsd) this.peakEqUsd = eq;
    const dd = this.peakEqUsd - eq;
    if (dd > this.maxDdUsd) this.maxDdUsd = dd;
  }

  flushMarkouts(nowMs, mid) {
    if (!mid) return;
    while (this.pendingMarkouts.length > 0 && this.pendingMarkouts[0].dueMs <= nowMs) {
      const m = this.pendingMarkouts.shift();
      const bps = m.side === 'BUY' ? ((mid - m.fillPx) / m.fillPx) * 10000 : ((m.fillPx - mid) / m.fillPx) * 10000;
      this.markoutSumBps += bps;
      this.markoutCount++;
    }
  }

  applyPending(nowMs, usdcBook) {
    while (this.pending.length > 0 && this.pending[0].dueMs <= nowMs) {
      const cmd = this.pending.shift();
      if (cmd.action === 'CANCEL') {
        if (cmd.side === 'BUY') this.bid = null;
        else this.ask = null;
      } else if (cmd.action === 'SET') {
        if (!usdcBook) continue;
        if (cmd.side === 'BUY') {
          if (cmd.px >= usdcBook.ap) {
            this.gtxRejects++;
            this.bid = null;
            continue;
          }
          // Keep queue position if same price as existing live order
          if (this.bid && Math.abs(this.bid.px - cmd.px) < this.tick * 0.1) {
            this.bid.cancelReqMs = 0;
            continue;
          }
          const qAhead = cmd.px > usdcBook.bp ? 0 : cmd.px === usdcBook.bp ? usdcBook.bq : usdcBook.bq * 2;
          this.bid = { px: cmd.px, qty: cmd.qty, queueAhead: qAhead, placedMs: nowMs, cancelReqMs: 0, regime: cmd.regime };
        } else {
          if (cmd.px <= usdcBook.bp) {
            this.gtxRejects++;
            this.ask = null;
            continue;
          }
          if (this.ask && Math.abs(this.ask.px - cmd.px) < this.tick * 0.1) {
            this.ask.cancelReqMs = 0;
            continue;
          }
          const qAhead = cmd.px < usdcBook.ap ? 0 : cmd.px === usdcBook.ap ? usdcBook.aq : usdcBook.aq * 2;
          this.ask = { px: cmd.px, qty: cmd.qty, queueAhead: qAhead, placedMs: nowMs, cancelReqMs: 0, regime: cmd.regime };
        }
      }
    }
  }

  queueCmd(nowMs, side, action, px = 0, qty = 0, regime = 'IDLE') {
    // Avoid spamming duplicate pending commands
    const dueMs = nowMs + this.latencyMs;
    if (action === 'CANCEL') {
      if (side === 'BUY' && this.bid && !this.bid.cancelReqMs) this.bid.cancelReqMs = nowMs;
      if (side === 'SELL' && this.ask && !this.ask.cancelReqMs) this.ask.cancelReqMs = nowMs;
    }
    this.pending.push({ dueMs, side, action, px, qty, regime });
  }

  executeFill(nowMs, side, px, qty, isTaker, regime, wasCancelRace) {
    this.fills++;
    if (isTaker) this.takerStops++;
    else this.makerFills++;
    if (wasCancelRace) this.cancelRaceFills++;
    if (regime === 'IDLE') this.idleFills++;
    else this.activeFills++;

    const feeRate = (isTaker ? this.cfg.takerFeeBps : this.cfg.makerFeeBps) / 10000;
    const fee = Math.abs(qty * px) * feeRate;
    this.feesUsd += fee;

    const signed = side === 'BUY' ? qty : -qty;
    const prevPos = this.posQty;
    let realizedDelta = 0;

    if (prevPos === 0 || Math.sign(prevPos) === Math.sign(signed)) {
      const totalAbs = Math.abs(prevPos) + Math.abs(signed);
      this.avgEntry = (Math.abs(prevPos) * this.avgEntry + Math.abs(signed) * px) / totalAbs;
      this.posQty = prevPos + signed;
    } else {
      const closeQty = Math.min(Math.abs(prevPos), Math.abs(signed));
      realizedDelta = prevPos > 0 ? closeQty * (px - this.avgEntry) : closeQty * (this.avgEntry - px);
      this.realizedUsd += realizedDelta;
      if (regime === 'IDLE') this.idleRealizedUsd += realizedDelta - fee;
      else this.activeRealizedUsd += realizedDelta - fee;

      this.closedTrips++;
      if (realizedDelta - fee > 0) this.wins++;

      const rem = prevPos + signed;
      if (Math.abs(rem) < 1e-12) {
        this.posQty = 0;
        this.avgEntry = 0;
      } else if (Math.sign(rem) === Math.sign(prevPos)) {
        this.posQty = rem;
      } else {
        this.posQty = rem;
        this.avgEntry = px;
      }
    }

    if (!isTaker) {
      this.pendingMarkouts.push({ dueMs: nowMs + 5000, side, fillPx: px });
    }
    this.updateDrawdown();
  }

  onUsdcTrade(nowMs, px, qty, isBuyerMaker, regime) {
    if (this.bid && isBuyerMaker && px <= this.bid.px) {
      const raced = this.bid.cancelReqMs > 0;
      if (px < this.bid.px) {
        const o = this.bid;
        this.bid = null;
        this.executeFill(nowMs, 'BUY', o.px, o.qty, false, o.regime || regime, raced);
      } else {
        this.bid.queueAhead -= qty;
        if (this.bid.queueAhead <= 0) {
          const o = this.bid;
          this.bid = null;
          this.executeFill(nowMs, 'BUY', o.px, o.qty, false, o.regime || regime, raced);
        }
      }
    }
    if (this.ask && !isBuyerMaker && px >= this.ask.px) {
      const raced = this.ask.cancelReqMs > 0;
      if (px > this.ask.px) {
        const o = this.ask;
        this.ask = null;
        this.executeFill(nowMs, 'SELL', o.px, o.qty, false, o.regime || regime, raced);
      } else {
        this.ask.queueAhead -= qty;
        if (this.ask.queueAhead <= 0) {
          const o = this.ask;
          this.ask = null;
          this.executeFill(nowMs, 'SELL', o.px, o.qty, false, o.regime || regime, raced);
        }
      }
    }
  }

  onUsdcBook(nowMs, book, regime) {
    this.lastMid = (book.bp + book.ap) / 2;
    this.flushMarkouts(nowMs, this.lastMid);

    // Toxic sweep fills if top-of-book strictly crosses our resting quote
    if (this.bid && book.ap < this.bid.px) {
      const o = this.bid;
      this.bid = null;
      this.executeFill(nowMs, 'BUY', o.px, o.qty, false, o.regime || regime, o.cancelReqMs > 0);
    }
    if (this.ask && book.bp > this.ask.px) {
      const o = this.ask;
      this.ask = null;
      this.executeFill(nowMs, 'SELL', o.px, o.qty, false, o.regime || regime, o.cancelReqMs > 0);
    }

    // Hard emergency stop on adverse inventory move
    if (this.posQty !== 0 && this.avgEntry > 0) {
      const advBps =
        this.posQty > 0
          ? ((this.avgEntry - book.bp) / this.avgEntry) * 10000
          : ((book.ap - this.avgEntry) / this.avgEntry) * 10000;
      if (advBps >= this.cfg.stopBps) {
        const closeSide = this.posQty > 0 ? 'SELL' : 'BUY';
        const exitPx = closeSide === 'SELL' ? book.bp : book.ap;
        this.bid = null;
        this.ask = null;
        this.pending = [];
        this.executeFill(nowMs, closeSide, exitPx, Math.abs(this.posQty), true, regime, false);
      }
    }
    this.updateDrawdown();
  }

  decideQuotes(nowMs, usdcBook, fairPx, vol60sBps, mom3sBps, regime) {
    if (!usdcBook || usdcBook.bp <= 0 || usdcBook.ap <= usdcBook.bp) return;
    const mid = (usdcBook.bp + usdcBook.ap) / 2;
    const spreadTicks = Math.round((usdcBook.ap - usdcBook.bp) / this.tick);
    const clipQty = this.cfg.clipUsd / mid;
    const invClips = this.posQty / clipQty; // signed clips held

    // Immediate USDT-M toxic cancel shield (runs on EVERY tick, zero throttle!)
    if (this.strategyId !== 'S1_NAIVE_MM' && fairPx > 0) {
      const shieldBps =
        this.strategyId === 'S4_ADAPTIVE_HYBRID'
          ? regime === 'ACTIVE'
            ? Math.max(1.2, vol60sBps * 0.25)
            : 0.45
          : 0.55;
      if (this.bid && !this.bid.cancelReqMs) {
        const bidEdgeBps = ((fairPx - this.bid.px) / fairPx) * 10000;
        if (bidEdgeBps < shieldBps - 0.15) {
          this.queueCmd(nowMs, 'BUY', 'CANCEL');
        }
      }
      if (this.ask && !this.ask.cancelReqMs) {
        const askEdgeBps = ((this.ask.px - fairPx) / fairPx) * 10000;
        if (askEdgeBps < shieldBps - 0.15) {
          this.queueCmd(nowMs, 'SELL', 'CANCEL');
        }
      }
    }

    // Throttle fresh quote placements to every 250ms (realistic rate-limit hygiene)
    if (nowMs - this.lastQuoteDecisionMs < 250) return;
    this.lastQuoteDecisionMs = nowMs;

    const wantLong = invClips < this.cfg.maxInvClips - 0.1;
    const wantShort = invClips > -(this.cfg.maxInvClips - 0.1);
    let targetBid = 0;
    let targetAsk = 0;

    if (this.strategyId === 'S1_NAIVE_MM') {
      // Control group: naive top-of-book quoting with light inventory skew
      const skewTicks = Math.round(invClips);
      targetBid = roundTick(usdcBook.bp - Math.max(0, skewTicks) * this.tick, this.tick, -1);
      targetAsk = roundTick(usdcBook.ap + Math.max(0, -skewTicks) * this.tick, this.tick, 1);
    } else if (fairPx > 0) {
      const invSkewBps = invClips * 1.1; // push quotes away from heavy inventory side
      const momSkewBps = this.strategyId === 'S4_ADAPTIVE_HYBRID' ? Math.max(-2.5, Math.min(2.5, mom3sBps * 0.5)) : 0;
      const baseGuardBps =
        this.strategyId === 'S2_SHIELDED_MM'
          ? 0.8
          : this.strategyId === 'S3_INSIDE_QUEUE_MM'
            ? 0.45
            : regime === 'ACTIVE'
              ? Math.max(1.4, vol60sBps * 0.3)
              : 0.45;

      const fairAdj = fairPx * (1 + (momSkewBps - invSkewBps) / 10000);
      let rawBid = roundTick(fairAdj * (1 - baseGuardBps / 10000), this.tick, -1);
      let rawAsk = roundTick(fairAdj * (1 + baseGuardBps / 10000), this.tick, 1);

      // Inside-tick priority for S3 and S4-IDLE when spread >= 2 ticks
      const allowInside =
        this.strategyId === 'S3_INSIDE_QUEUE_MM' ||
        (this.strategyId === 'S4_ADAPTIVE_HYBRID' && regime === 'IDLE');

      if (allowInside && spreadTicks >= 2) {
        const insideBid = roundTick(usdcBook.bp + this.tick, this.tick, 0);
        const insideAsk = roundTick(usdcBook.ap - this.tick, this.tick, 0);
        if (insideBid < usdcBook.ap && insideBid <= rawBid) targetBid = insideBid;
        else targetBid = Math.min(rawBid, usdcBook.bp);
        if (insideAsk > usdcBook.bp && insideAsk >= rawAsk) targetAsk = insideAsk;
        else targetAsk = Math.max(rawAsk, usdcBook.ap);
      } else {
        targetBid = Math.min(rawBid, usdcBook.bp);
        targetAsk = Math.max(rawAsk, usdcBook.ap);
      }

      // If holding inventory, prioritize flattening side right at best BBO if safe
      if (invClips >= 1 && usdcBook.ap >= fairPx * (1 - 0.15 / 10000)) {
        targetAsk = usdcBook.ap;
      } else if (invClips <= -1 && usdcBook.bp <= fairPx * (1 + 0.15 / 10000)) {
        targetBid = usdcBook.bp;
      }
    }

    if (wantLong && targetBid > 0 && targetBid < usdcBook.ap) {
      if (!this.bid || Math.abs(this.bid.px - targetBid) >= this.tick * 0.5) {
        this.queueCmd(nowMs, 'BUY', 'SET', targetBid, clipQty, regime);
      }
    } else if (this.bid && !this.bid.cancelReqMs) {
      this.queueCmd(nowMs, 'BUY', 'CANCEL');
    }

    if (wantShort && targetAsk > 0 && targetAsk > usdcBook.bp) {
      if (!this.ask || Math.abs(this.ask.px - targetAsk) >= this.tick * 0.5) {
        this.queueCmd(nowMs, 'SELL', 'SET', targetAsk, clipQty, regime);
      }
    } else if (this.ask && !this.ask.cancelReqMs) {
      this.queueCmd(nowMs, 'SELL', 'CANCEL');
    }
  }
}

async function runReplay(files, cfg, latencyMs) {
  const strategies = ['S1_NAIVE_MM', 'S2_SHIELDED_MM', 'S3_INSIDE_QUEUE_MM', 'S4_ADAPTIVE_HYBRID'];
  const books = Object.create(null); // sym -> { bp, bq, ap, aq, mid }
  const fxState = Object.create(null); // base -> { eq, samples }
  const volState = Object.create(null); // base -> [{ ts, mid }]
  const sims = Object.create(null); // `${strat}:${base}` -> CoinStrategySim

  let firstRx = 0;
  let lastRx = 0;
  let totalEvents = 0;
  let idleSamples = 0;
  let activeSamples = 0;

  function getSim(strat, base) {
    const k = `${strat}:${base}`;
    if (!sims[k]) sims[k] = new CoinStrategySim(strat, base, cfg, latencyMs);
    return sims[k];
  }

  for (const filePath of files) {
    const rawStream = fs.createReadStream(filePath);
    const stream = filePath.endsWith('.gz') ? rawStream.pipe(zlib.createGunzip()) : rawStream;
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    for await (const line of rl) {
      if (!line || line[0] !== '{') continue;
      let ev;
      try {
        ev = JSON.parse(line);
      } catch {
        continue;
      }
      const sp = splitSymbol(ev.s || '');
      if (!sp) continue;
      if (cfg.coins && !cfg.coins.includes(sp.base)) continue;

      const nowMs = ev.r;
      if (!firstRx) firstRx = nowMs;
      lastRx = nowMs;
      totalEvents++;

      const usdcSym = sp.base + 'USDC';
      const usdtSym = sp.base + 'USDT';

      if (ev.t === 'b') {
        const mid = (ev.bp + ev.ap) / 2;
        books[ev.s] = { bp: ev.bp, bq: ev.bq, ap: ev.ap, aq: ev.aq, mid };

        // Update equilibrium fx & 60s volatility from USDT-M
        if (books[usdcSym] && books[usdtSym] && books[usdtSym].mid > 0) {
          const ratio = books[usdcSym].mid / books[usdtSym].mid;
          if (!fxState[sp.base]) fxState[sp.base] = { eq: ratio, samples: 1 };
          else {
            const alpha = fxState[sp.base].samples < 200 ? 0.05 : 0.0015;
            fxState[sp.base].eq = fxState[sp.base].eq * (1 - alpha) + ratio * alpha;
            fxState[sp.base].samples++;
          }
        }
        if (sp.quote === 'USDT') {
          let arr = volState[sp.base];
          if (!arr) arr = volState[sp.base] = [];
          if (arr.length === 0 || nowMs - arr[arr.length - 1].ts >= 1000) {
            arr.push({ ts: nowMs, mid });
            while (arr.length > 0 && nowMs - arr[0].ts > 60000) arr.shift();
          }
        }
      }

      const usdcBook = books[usdcSym];
      const usdtBook = books[usdtSym];
      const fx = fxState[sp.base];
      const fairPx = usdtBook && fx && fx.samples >= 10 ? usdtBook.mid * fx.eq : 0;

      // Compute 60s range volatility and 3s momentum on USDT-M
      let vol60sBps = 0;
      let mom3sBps = 0;
      const arr = volState[sp.base];
      if (arr && arr.length >= 5) {
        let lo = Infinity;
        let hi = -Infinity;
        let ref3s = arr[0].mid;
        for (const pt of arr) {
          if (pt.mid < lo) lo = pt.mid;
          if (pt.mid > hi) hi = pt.mid;
          if (nowMs - pt.ts <= 3500) {
            if (ref3s === arr[0].mid) ref3s = pt.mid;
          }
        }
        vol60sBps = ((hi - lo) / lo) * 10000;
        if (usdtBook && ref3s > 0) mom3sBps = ((usdtBook.mid - ref3s) / ref3s) * 10000;
      }
      const regime = vol60sBps >= 4.0 ? 'ACTIVE' : 'IDLE';
      if (ev.t === 'b' && sp.quote === 'USDC') {
        if (regime === 'IDLE') idleSamples++;
        else activeSamples++;
      }

      for (const strat of strategies) {
        const sim = getSim(strat, sp.base);
        sim.applyPending(nowMs, usdcBook);
        if (sp.quote === 'USDC') {
          if (ev.t === 't') sim.onUsdcTrade(nowMs, ev.p, ev.q, ev.m, regime);
          else if (ev.t === 'b') sim.onUsdcBook(nowMs, usdcBook, regime);
        }
        if (usdcBook) {
          sim.decideQuotes(nowMs, usdcBook, fairPx, vol60sBps, mom3sBps, regime);
        }
      }
    }
  }

  const durHours = Math.max((lastRx - firstRx) / 3600000, 1 / 3600);
  const durMin = durHours * 60;
  const idlePct = idleSamples + activeSamples > 0 ? (idleSamples / (idleSamples + activeSamples)) * 100 : 0;

  return { strategies, sims, totalEvents, durHours, durMin, idlePct };
}

function summarize(res, latencyMs) {
  console.log(
    `\n=== REPLAY SUMMARY | latency=${latencyMs}ms | span=${res.durMin.toFixed(1)}m (${res.totalEvents.toLocaleString()} ticks) | idle=${res.idlePct.toFixed(0)}% ===`
  );
  console.log('STRATEGY             | Net($)   | $/hr    | Fills/m | Win%  | MaxDD($) | Idle($)  | Actv($)  | Mk5s(bp) | Race | Stops');
  console.log('---------------------|----------|---------|---------|-------|----------|----------|----------|----------|------|------');

  const coinWinners = [];
  for (const strat of res.strategies) {
    let net = 0,
      fills = 0,
      trips = 0,
      wins = 0,
      dd = 0,
      idle = 0,
      actv = 0,
      mkSum = 0,
      mkCnt = 0,
      race = 0,
      stops = 0;
    for (const [k, s] of Object.entries(res.sims)) {
      if (!k.startsWith(strat + ':')) continue;
      const eq = s.equityUsd();
      net += eq;
      fills += s.fills;
      trips += s.closedTrips;
      wins += s.wins;
      dd += s.maxDdUsd;
      idle += s.idleRealizedUsd;
      actv += s.activeRealizedUsd;
      mkSum += s.markoutSumBps;
      mkCnt += s.markoutCount;
      race += s.cancelRaceFills;
      stops += s.takerStops;
      coinWinners.push({ strat, base: s.base, eq, hr: eq / res.durHours, fills: s.fills, mk: s.markoutCount ? s.markoutSumBps / s.markoutCount : 0, race: s.cancelRaceFills, stops: s.takerStops });
    }
    const wr = trips > 0 ? ((wins / trips) * 100).toFixed(0) + '%' : '  n/a';
    const mk = mkCnt > 0 ? (mkSum / mkCnt).toFixed(2) : '  n/a';
    console.log(
      `${strat.padEnd(20)} | ${net.toFixed(3).padStart(8)} | ${(net / res.durHours).toFixed(2).padStart(7)} | ${(fills / res.durMin).toFixed(2).padStart(7)} | ${wr.padStart(5)} | ${dd.toFixed(3).padStart(8)} | ${idle.toFixed(3).padStart(8)} | ${actv.toFixed(3).padStart(8)} | ${mk.padStart(8)} | ${String(race).padStart(4)} | ${String(stops).padStart(5)}`
    );
  }

  console.log('\nTop 8 Coin x Strategy Pairs:');
  coinWinners
    .filter(c => c.fills > 0)
    .sort((a, b) => b.eq - a.eq)
    .slice(0, 8)
    .forEach(c => {
      console.log(
        `  ${c.base.padEnd(9)} ${c.strat.padEnd(19)} net=$${c.eq.toFixed(3)} ($${c.hr.toFixed(2)}/hr) fills=${c.fills} mk5s=${c.mk.toFixed(2)}bp race=${c.race} stops=${c.stops}`
      );
    });
}

(async () => {
  const cfg = parseArgs(process.argv);
  const { files, manifest } = await resolveInputFiles(cfg);
  if (manifest && manifest.collectorStatus) {
    const cs = manifest.collectorStatus;
    console.log(
      `[COLLECTOR] v=${cs.version} uptime=${cs.uptimeSec}s events=${cs.eventsTotal} rate=${(cs.eventsPerSec || 0).toFixed(0)}/s avgWsLag=${(cs.avgWsLagMs || 0).toFixed(1)}ms chunks=${manifest.chunkCount}`
    );
  }
  if (!files.length) {
    console.log('No tick chunks available yet. Wait 5 minutes for the first finalized chunk from Tokyo EC2.');
    process.exit(0);
  }
  console.log(`Loaded ${files.length} chunk(s): ${path.basename(files[0])} .. ${path.basename(files[files.length - 1])}`);
  for (const lat of cfg.latencies) {
    const res = await runReplay(files, cfg, lat);
    summarize(res, lat);
  }
})().catch(err => {
  console.error('Tester error:', err.message);
  process.exit(1);
});
