#!/usr/bin/env node
/**
 * Live Demo Forward-Tester for Uncrowded / Newly Listed Lighter <-> Binance Markets
 *
 * Tests BOTH structural VIP-0 edges simultaneously with 100% REAL execution constraints:
 *   1. Strategy A (Niche Crossed-Book IOC): waits 350ms (Lighter's real speed bump) and
 *      re-fetches FRESH REST books on both venues BEFORE counting any fill!
 *   2. Strategy B (Niche Funding Carry): uses true Binance /fapi/v1/fundingInfo intervals
 *      vs Lighter hourly funding, tracking BOTH accrued funding AND live mark-to-market basis!
 *
 * Zero real orders placed. 100% read-only public REST APIs.
 * Usage: node research/lt_niche_demo.js --seconds 180 --clip 40
 */

const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const cfg = { seconds: 180, clipUsd: 40, minVolUsd: 150000, maxCoins: 15, outJson: '' };
  for (let i = 2; i < argv.length; i++) {
    if (argv[i] === '--seconds' && argv[i + 1]) cfg.seconds = parseInt(argv[++i], 10) || 180;
    else if (argv[i] === '--clip' && argv[i + 1]) cfg.clipUsd = parseFloat(argv[++i]) || 40;
    else if (argv[i] === '--max-coins' && argv[i + 1]) cfg.maxCoins = parseInt(argv[++i], 10) || 15;
    else if (argv[i] === '--out' && argv[i + 1]) cfg.outJson = argv[++i];
  }
  return cfg;
}

async function getJson(url) {
  const r = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function fetchLtBook(marketId) {
  const ob = await getJson(`https://mainnet.zklighter.elliot.ai/api/v1/orderBookOrders?market_id=${marketId}&limit=5`);
  const bids = ob.bids || [];
  const asks = ob.asks || [];
  if (!bids.length || !asks.length) return null;
  const lb = +bids[0].price, lbq = +bids[0].remaining_base_amount;
  const la = +asks[0].price, laq = +asks[0].remaining_base_amount;
  if (!(lb > 0 && la > lb)) return null;
  return { bid: lb, ask: la, bidUsd: lb * lbq, askUsd: la * laq, mid: 0.5 * (lb + la) };
}

async function fetchBnBook(sym) {
  const b = await getJson(`https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=${sym}`);
  const bb = +b.bidPrice, ba = +b.askPrice, bbq = +b.bidQty, baq = +b.askQty;
  if (!(bb > 0 && ba > bb)) return null;
  return { bid: bb, ask: ba, bidUsd: bb * bbq, askUsd: ba * baq, mid: 0.5 * (bb + ba) };
}

async function discoverNicheCoins(cfg) {
  const [ltDet, ltFr, bnBooks, bnPrem, bnFi] = await Promise.all([
    getJson('https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails'),
    getJson('https://mainnet.zklighter.elliot.ai/api/v1/funding-rates'),
    getJson('https://fapi.binance.com/fapi/v1/ticker/bookTicker'),
    getJson('https://fapi.binance.com/fapi/v1/premiumIndex'),
    getJson('https://fapi.binance.com/fapi/v1/fundingInfo'),
  ]);

  const bnBookMap = Object.fromEntries(bnBooks.map(x => [x.symbol, x]));
  const bnPremMap = Object.fromEntries(bnPrem.map(x => [x.symbol, +x.lastFundingRate]));
  const bnIntMap = Object.fromEntries(bnFi.map(x => [x.symbol, x.fundingIntervalHours || 8]));
  const ltFrMap = Object.fromEntries(
    (ltFr.funding_rates || []).filter(x => x.exchange === 'lighter').map(x => [x.symbol, +x.rate])
  );

  // Exclude the 4 hyper-crowded majors (BTC, ETH, SOL, XRP) to focus strictly on low-bot niches
  const crowded = new Set(['BTC', 'ETH', 'SOL', 'XRP']);
  const cands = [];

  for (const m of ltDet.order_book_details || []) {
    const sym = m.symbol || '';
    if (crowded.has(sym)) continue;
    const vol = +(m.daily_quote_token_volume || 0);
    const lp = +(m.last_trade_price || 0);
    if (vol < cfg.minVolUsd || lp <= 0) continue;

    // Prefer USDC-M (0% maker / 4bp taker), else USDT-M (2bp maker / 5bp taker)
    const hasUsdc = !!bnBookMap[sym + 'USDC'];
    const hasUsdt = !!bnBookMap[sym + 'USDT'];
    if (!hasUsdc && !hasUsdt) continue;
    const bnSym = hasUsdc ? sym + 'USDC' : sym + 'USDT';
    const b = bnBookMap[bnSym];
    const bmid = 0.5 * (+b.bidPrice + +b.askPrice);
    if (!bmid || Math.abs(lp - bmid) / bmid > 0.03) continue; // skip mismatched tickers like PRL!

    const bnIntH = bnIntMap[sym + 'USDT'] || bnIntMap[bnSym] || 8;
    const bnRateRaw = bnPremMap[sym + 'USDT'] ?? bnPremMap[bnSym] ?? 0;
    const bnApr = (bnRateRaw / bnIntH) * 24 * 365 * 100;
    const ltApr = ((ltFrMap[sym] || 0) / 8) * 24 * 365 * 100;
    const fundDiffApr = ltApr - bnApr; // positive => Short LT / Long BN earns; negative => Long LT / Short BN earns
    const pxGapBps = (Math.abs(lp - bmid) / bmid) * 10000;

    // Score by funding gap + price dislocation
    const score = Math.abs(fundDiffApr) + pxGapBps * 1.5;
    cands.push({
      sym,
      marketId: +m.market_id,
      bnSym,
      isUsdc: hasUsdc,
      bnTakerBps: hasUsdc ? 4.0 : 5.0,
      bnMakerBps: hasUsdc ? 0.0 : 2.0,
      volUsd: vol,
      bnApr,
      ltApr,
      fundDiffApr,
      pxGapBps,
      score,
    });
  }

  cands.sort((a, b) => b.score - a.score);
  return cands.slice(0, cfg.maxCoins);
}

(async () => {
  const cfg = parseArgs(process.argv);
  const startMs = Date.now();
  const coins = await discoverNicheCoins(cfg);
  console.log(`=== DISCOVERED ${coins.length} LOW-BOT LIGHTER<->BINANCE NICHE COINS (clip=$${cfg.clipUsd}, dur=${cfg.seconds}s) ===`);
  coins.forEach(c => {
    console.log(
      `  ${c.sym.padEnd(8)} id=${String(c.marketId).padStart(5)} ${c.bnSym.padEnd(12)} | LT=${c.ltApr.toFixed(1).padStart(7)}% BN=${c.bnApr.toFixed(1).padStart(7)}% Diff=${c.fundDiffApr.toFixed(1).padStart(7)}% APR | vol=$${(c.volUsd / 1e6).toFixed(2)}M`
    );
  });

  // Strategy A: 350ms-Delayed Fresh-REST Crossed-Book IOC
  const openArb = {}; // sym -> { dir, ltEp, bnEp, entryFeeBps, openedMs, entryRawBps }
  const closedArb = [];
  let arbSignals = 0, arbMissed350ms = 0, arbDepthSkip = 0;
  const maxCross = Object.fromEntries(coins.map(c => [c.sym, -999]));

  // Strategy B: Funding Carry (top 3 coins with |fundDiffApr| >= 30% & depth >= 2*clip)
  const carryPos = {}; // sym -> { dir, ltEp, bnEp, entryCostBps, openedMs, diffApr, accruedUsd }

  const endMs = startMs + cfg.seconds * 1000;
  let round = 0;

  while (Date.now() < endMs) {
    round++;
    // Fetch all Binance books in 1 call
    let bnAllMap = {};
    try {
      const arr = await getJson('https://fapi.binance.com/fapi/v1/ticker/bookTicker');
      bnAllMap = Object.fromEntries(arr.map(x => [x.symbol, x]));
    } catch {
      await sleep(1000);
      continue;
    }

    for (const c of coins) {
      if (Date.now() >= endMs) break;
      const rawBn = bnAllMap[c.bnSym];
      if (!rawBn) continue;
      const bn = {
        bid: +rawBn.bidPrice,
        ask: +rawBn.askPrice,
        bidUsd: +rawBn.bidPrice * +rawBn.bidQty,
        askUsd: +rawBn.askPrice * +rawBn.askQty,
        mid: 0.5 * (+rawBn.bidPrice + +rawBn.askPrice),
      };
      let lt;
      try {
        lt = await fetchLtBook(c.marketId);
      } catch {
        continue;
      }
      if (!lt) continue;

      const longLtBps = ((bn.bid - lt.ask) / lt.ask) * 10000;
      const shortLtBps = ((lt.bid - bn.ask) / bn.ask) * 10000;
      const bestCross = Math.max(longLtBps, shortLtBps);
      if (bestCross > maxCross[c.sym]) maxCross[c.sym] = bestCross;

      // --- STRATEGY A: 350ms-Verified Crossed-Book IOC ---
      const rtFeeBps = c.bnTakerBps + c.bnMakerBps; // 4bp USDC-M, 7bp USDT-M
      const minEntryBps = rtFeeBps + 6.0; // must clear full RT fee + 6bp safety margin!

      if (!openArb[c.sym]) {
        const wantDir = longLtBps >= minEntryBps ? 'LONG_LT' : shortLtBps >= minEntryBps ? 'SHORT_LT' : null;
        if (wantDir) {
          arbSignals++;
          const needDepth = cfg.clipUsd * 2;
          const okDepth =
            wantDir === 'LONG_LT'
              ? lt.askUsd >= needDepth && bn.bidUsd >= needDepth
              : lt.bidUsd >= needDepth && bn.askUsd >= needDepth;
          if (!okDepth) {
            arbDepthSkip++;
          } else {
            // CRITICAL REALISM: Wait 350ms (Lighter speed bump) and re-fetch FRESH books!
            await sleep(350);
            try {
              const [lt2, bn2] = await Promise.all([fetchLtBook(c.marketId), fetchBnBook(c.bnSym)]);
              if (lt2 && bn2) {
                const reCross =
                  wantDir === 'LONG_LT'
                    ? ((bn2.bid - lt2.ask) / lt2.ask) * 10000
                    : ((lt2.bid - bn2.ask) / bn2.ask) * 10000;
                const okDepth2 =
                  wantDir === 'LONG_LT'
                    ? lt2.askUsd >= cfg.clipUsd && bn2.bidUsd >= cfg.clipUsd
                    : lt2.bidUsd >= cfg.clipUsd && bn2.askUsd >= cfg.clipUsd;
                if (reCross >= rtFeeBps + 3.0 && okDepth2) {
                  openArb[c.sym] = {
                    dir: wantDir,
                    ltEp: wantDir === 'LONG_LT' ? lt2.ask : lt2.bid,
                    bnEp: wantDir === 'LONG_LT' ? bn2.bid : bn2.ask,
                    rtFeeBps,
                    openedMs: Date.now(),
                    entryRawBps: reCross,
                  };
                  console.log(
                    `  [ARB_OPEN] ${c.sym.padEnd(7)} ${wantDir} after350ms=${reCross.toFixed(1)}bp (fee=${rtFeeBps}bp) ltDepth=$${(wantDir === 'LONG_LT' ? lt2.askUsd : lt2.bidUsd).toFixed(0)} bnDepth=$${(wantDir === 'LONG_LT' ? bn2.bidUsd : bn2.askUsd).toFixed(0)}`
                  );
                } else {
                  arbMissed350ms++;
                }
              }
            } catch {}
          }
        }
      } else {
        const p = openArb[c.sym];
        const grossBps =
          p.dir === 'LONG_LT'
            ? ((lt.bid - p.ltEp) / p.ltEp + (p.bnEp - bn.bid) / p.bnEp) * 10000
            : ((p.ltEp - lt.ask) / p.ltEp + (bn.ask - p.bnEp) / p.bnEp) * 10000;
        const netBps = grossBps - p.rtFeeBps;
        if (netBps >= 3.0) {
          // Wait 350ms for Lighter IOC exit too!
          await sleep(350);
          try {
            const [lt2, bn2] = await Promise.all([fetchLtBook(c.marketId), fetchBnBook(c.bnSym)]);
            if (lt2 && bn2) {
              const g2 =
                p.dir === 'LONG_LT'
                  ? ((lt2.bid - p.ltEp) / p.ltEp + (p.bnEp - bn2.bid) / p.bnEp) * 10000
                  : ((p.ltEp - lt2.ask) / p.ltEp + (bn2.ask - p.bnEp) / p.bnEp) * 10000;
              const n2 = g2 - p.rtFeeBps;
              const usd = (n2 / 10000) * cfg.clipUsd;
              closedArb.push({ sym: c.sym, dir: p.dir, entryRawBps: p.entryRawBps, netBps: n2, usd, holdSec: (Date.now() - p.openedMs) / 1000 });
              console.log(
                `  [ARB_CLOSE] ${c.sym.padEnd(7)} ${p.dir} net=${n2 >= 0 ? '+' : ''}${n2.toFixed(2)}bp ($${usd >= 0 ? '+' : ''}${usd.toFixed(4)}) hold=${((Date.now() - p.openedMs) / 1000).toFixed(0)}s`
              );
              delete openArb[c.sym];
            }
          } catch {}
        }
      }

      // --- STRATEGY B: Funding Carry (enter top 3 with |diffApr| >= 30% & depth >= 2*clip) ---
      if (!carryPos[c.sym] && Object.keys(carryPos).length < 3 && Math.abs(c.fundDiffApr) >= 30.0) {
        const dir = c.fundDiffApr > 0 ? 'SHORT_LT' : 'LONG_LT';
        const crossNow = dir === 'LONG_LT' ? longLtBps : shortLtBps;
        const okDepth =
          dir === 'LONG_LT'
            ? lt.askUsd >= cfg.clipUsd * 2 && bn.bidUsd >= cfg.clipUsd * 2
            : lt.bidUsd >= cfg.clipUsd * 2 && bn.askUsd >= cfg.clipUsd * 2;
        if (okDepth && crossNow >= -12.0) {
          // Only enter carry if we don't pay >12bp adverse basis!
          carryPos[c.sym] = {
            dir,
            ltEp: dir === 'LONG_LT' ? lt.ask : lt.bid,
            bnEp: dir === 'LONG_LT' ? bn.bid : bn.ask,
            rtFeeBps,
            openedMs: Date.now(),
            diffApr: Math.abs(c.fundDiffApr),
            entryBasisBps: crossNow,
            lastLt: lt,
            lastBn: bn,
          };
          console.log(
            `  [CARRY_OPEN] ${c.sym.padEnd(7)} ${dir} diffAPR=${Math.abs(c.fundDiffApr).toFixed(1)}% entryBasis=${crossNow >= 0 ? '+' : ''}${crossNow.toFixed(1)}bp`
          );
        }
      } else if (carryPos[c.sym]) {
        carryPos[c.sym].lastLt = lt;
        carryPos[c.sym].lastBn = bn;
      }

      await sleep(120); // polite Lighter REST pacing (<8 req/s)
    }
  }

  const durMin = (Date.now() - startMs) / 60000;
  console.log(`\n=== LIVE DEMO RESULTS (${durMin.toFixed(1)} min, ${round} full scans, 350ms Lighter lag enforced) ===`);
  console.log(`Strategy A (Niche Crossed-Book IOC): signals=${arbSignals} depthSkip=${arbDepthSkip} missed350ms=${arbMissed350ms} closed=${closedArb.length} open=${Object.keys(openArb).length}`);
  const arbNetUsd = closedArb.reduce((a, x) => a + x.usd, 0);
  const arbWins = closedArb.filter(x => x.netBps > 0).length;
  console.log(`  Closed PnL: $${arbNetUsd >= 0 ? '+' : ''}${arbNetUsd.toFixed(4)} (${arbWins}/${closedArb.length} wins)`);
  for (const [sym, p] of Object.entries(openArb)) {
    console.log(`  Open Arb  : ${sym} ${p.dir} entryRaw=${p.entryRawBps.toFixed(1)}bp hold=${((Date.now() - p.openedMs) / 1000).toFixed(0)}s`);
  }

  console.log(`\nStrategy B (Niche Funding Carry, ${Object.keys(carryPos).length} active positions, clip=$${cfg.clipUsd}):`);
  let carryEstDay = 0;
  for (const [sym, p] of Object.entries(carryPos)) {
    const heldHrs = (Date.now() - p.openedMs) / 3600000;
    const estFundUsd = cfg.clipUsd * (p.diffApr / 100) * (heldHrs / 8760);
    const estDayUsd = cfg.clipUsd * (p.diffApr / 100) / 365;
    carryEstDay += estDayUsd;
    const lt = p.lastLt, bn = p.lastBn;
    const mtmBps =
      p.dir === 'LONG_LT'
        ? ((lt.bid - p.ltEp) / p.ltEp + (p.bnEp - bn.ask) / p.bnEp) * 10000 - p.rtFeeBps
        : ((p.ltEp - lt.ask) / p.ltEp + (bn.bid - p.bnEp) / p.bnEp) * 10000 - p.rtFeeBps;
    console.log(
      `  ${sym.padEnd(7)} ${p.dir} APR=${p.diffApr.toFixed(1)}% ($${estDayUsd.toFixed(3)}/day on $${cfg.clipUsd}) | entryBasis=${p.entryBasisBps >= 0 ? '+' : ''}${p.entryBasisBps.toFixed(1)}bp | liveExitMTM=${mtmBps >= 0 ? '+' : ''}${mtmBps.toFixed(1)}bp | accrued=$${estFundUsd.toFixed(5)}`
    );
  }
  console.log(`  Total Carry Run-Rate (at $${cfg.clipUsd}/slot): $${carryEstDay.toFixed(3)}/day (at $350/slot = $${(carryEstDay * (350 / cfg.clipUsd)).toFixed(2)}/day)`);

  console.log('\nPeak Live Crosses Seen Across Niche Coins:');
  Object.entries(maxCross)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 8)
    .forEach(([s, v]) => console.log(`  ${s.padEnd(8)} maxCross=${v >= 0 ? '+' : ''}${v.toFixed(1)}bp`));
})();
