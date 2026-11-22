package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elliottech/lighter-go/types"
)

const (
	LiveArenaVer       = "LIVE-MICRO-v105.4-ROLLING-4H"
	StratStopLossUSD   = -1.50 // Auto-halt individual strategy if realized+unrealized drops <= -$1.50 (~3.3x max historical $15 DD)
	GlobalStopLossUSD  = -6.50 // Auto-flatten & S3-pause ALL live strategies at <= -$6.50 ($300.33 Perp equity floor)
	MaxLtTxPerMin      = 48    // Strict guard below #770015 Standard 60 tx/min quota
)

type LivePosView struct {
	Venue     string  `json:"venue"`
	Symbol    string  `json:"symbol"`
	Qty       float64 `json:"qty"`
	AvgPx     float64 `json:"avgPx"`
	OpenedMs  int64   `json:"openedMs"`
	UpdatedMs int64   `json:"updatedMs"`
	Tag       string  `json:"tag"`
}

type LiveFillAudit struct {
	TsMs      int64   `json:"ts"`
	StratID   string  `json:"strat"`
	Venue     string  `json:"venue"`
	Symbol    string  `json:"sym"`
	Side      string  `json:"side"`
	ExecType  string  `json:"type"`
	FillPx    float64 `json:"px"`
	SignalPx  float64 `json:"sigPx"`
	Qty       float64 `json:"qty"`
	Notional  float64 `json:"usd"`
	FeeUsd    float64 `json:"fee"`
	Realized  float64 `json:"realized"`
	SlipBp    float64 `json:"slipBp"`
	LatencyMs int64   `json:"latMs"`
	Tag       string  `json:"tag"`
}

type LiveEqPoint struct {
	TsMs int64   `json:"ts"`
	Cash float64 `json:"cash"`
	Eq   float64 `json:"eq"`
}

type LiveStratAccount struct {
	ID              string        `json:"id"`
	Name            string        `json:"name"`
	Desc            string        `json:"desc"`
	VenueDesc       string        `json:"venueDesc"`
	ClipUsd         float64       `json:"clipUsd"`
	StartCash       float64       `json:"startCash"`
	Cash            float64       `json:"cash"`
	RealizedGross   float64       `json:"realizedGross"`
	FeesPaid        float64       `json:"feesPaid"`
	FundingUsd      float64       `json:"fundingUsd"`
	UnrealizedPnL   float64       `json:"unrealizedPnl"`
	Equity          float64       `json:"equity"`
	PeakEquity      float64       `json:"peakEquity"`
	MaxDrawdownUsd  float64       `json:"maxDrawdownUsd"`
	HourlyRunRate   float64       `json:"hourlyRunRate"`
	RealizedHourly  float64       `json:"realizedHourly"`
	Rolling4hHourly float64       `json:"rolling4hHourly"`
	Scaled489Hourly float64       `json:"scaled489Hourly"`
	MakerPlaced     int64         `json:"makerPlaced"`
	MakerCancelled  int64         `json:"makerCancelled"`
	MakerFills      int64         `json:"makerFills"`
	TakerFills      int64         `json:"takerFills"`
	RoundTrips      int64         `json:"roundTrips"`
	WinTrips        int64         `json:"winTrips"`
	GrossVolumeUsd  float64       `json:"grossVolumeUsd"`
	ActiveMakers    int           `json:"activeMakers"`
	PosSnap         []LivePosView `json:"positions"`
	EquityCurve     []LiveEqPoint `json:"equityCurve,omitempty"`
	Halted          bool          `json:"halted"`
	HaltReason      string        `json:"haltReason"`
	State           string        `json:"state"`
	ActiveSym       string        `json:"activeSym"`
	RotatedNote     string        `json:"rotatedNote,omitempty"`
	StatusNote      string        `json:"statusNote"`
}

type LiveSpreadSnap struct {
	Symbol     string  `json:"sym"`
	StratID    string  `json:"stratId"`
	LtBid      float64 `json:"ltBid"`
	LtAsk      float64 `json:"ltAsk"`
	LtSpreadBp float64 `json:"ltSpdBp"`
	PeerVenue  string  `json:"peerVenue"`
	PeerBid    float64 `json:"bnBid"`
	PeerAsk    float64 `json:"bnAsk"`
	PeerSpdBp  float64 `json:"bnSpdBp"`
	BasisBp    float64 `json:"basisBp"`
	EmaBp      float64 `json:"emaBp"`
	DislocBp   float64 `json:"dislocBp"`
}

type LiveArenaTelemetry struct {
	Version          string              `json:"version"`
	BootTsMs         int64               `json:"bootTsMs"`
	NowTsMs          int64               `json:"nowTsMs"`
	UptimeSec        float64             `json:"uptimeSec"`
	UptimeHours      float64             `json:"uptimeHours"`
	Paused           bool                `json:"paused"`
	LtTxLastMin      int                 `json:"ltTxLastMin"`
	TotalCapitalUsd  float64             `json:"totalCapitalUsd"`
	LighterEqUsd     float64             `json:"lighterEqUsd"`
	BinanceMarginUsd float64             `json:"binanceMarginUsd"`
	AsterMarginUsd   float64             `json:"asterMarginUsd"`
	CombinedNetUsd   float64             `json:"combinedNetUsd"`
	CombinedHourly   float64             `json:"combinedHourlyUsd"`
	Scaled489BestHr  float64             `json:"scaled489BestHourlyUsd"`
	BestStratID      string              `json:"bestStratId"`
	BestHourlyUsd    float64             `json:"bestHourlyUsd"`
	Strategies       []*LiveStratAccount `json:"strategies"`
	RecentFills      []LiveFillAudit     `json:"recentFills"`
	MarketSpreads    []LiveSpreadSnap    `json:"marketSpreads"`
	SlotSnapshots    []SavedSlotSnap     `json:"slotSnapshots,omitempty"`
}

type LiveSlotRuntime struct {
	StratID      string
	PeerVenue    string // "BN_USDC" or "ASTER_C" or "ASTER_RWA"
	Symbols      []string
	ClipUsd      float64
	ScaleMult489 float64 // Full-$489 Arena slotUsd / micro ClipUsd
	EntryDisloc  float64
	ExitNetBp    float64
	CancelTolBp  float64
	MaxRestSec   float64
	MaxHoldSec   float64

	// Live execution state machine (IDLE -> LT_ENTRY_OPEN -> HEDGED -> PEER_EXIT_OPEN)
	State       string
	Sym         string
	PeerSym     string
	LtSide      string // "BID" (BUY) or "ASK" (SELL) on Lighter Maker entry
	PeerSide    string // "SELL" or "BUY" on Peer Taker hedge
	LtCOI       int64
	LtLimitPx   float64
	LtOrderQty  float64
	PlacedAt    time.Time
	CancelSent  bool
	CancelAt    time.Time

	HedgedQty    float64
	EntryLtPx    float64
	EntryPeerPx  float64
	EntryFx      float64
	EntryPeerFee float64
	OpenedAt     time.Time
	TradeID      int64

	ExitPeerOid int64
	ExitPeerPx  float64
	ExitPlaced  time.Time
	LastOrdPoll time.Time
	CoolUntil   time.Time

	// IDLE-Safe Rotation Queue: never swaps symbols/venues mid-position!
	OrigPeerVenue string
	OrigSymbols   []string
	PendingVenue  string
	PendingSyms   []string
	RotatedNote   string
}

type SavedSlotSnap struct {
	StratID      string  `json:"stratId"`
	State        string  `json:"state"`
	Sym          string  `json:"sym"`
	PeerVenue    string  `json:"peerVenue"`
	PeerSym      string  `json:"peerSym"`
	LtSide       string  `json:"ltSide"`
	PeerSide     string  `json:"peerSide"`
	HedgedQty    float64 `json:"hedgedQty"`
	EntryLtPx    float64 `json:"entryLtPx"`
	EntryPeerPx  float64 `json:"entryPeerPx"`
	EntryFx      float64 `json:"entryFx"`
	EntryPeerFee float64 `json:"entryPeerFee"`
	OpenedMs     int64   `json:"openedMs"`
	TradeID      int64   `json:"tradeId"`
	ExitPeerOid  int64   `json:"exitPeerOid"`
	ExitPeerPx   float64 `json:"exitPeerPx"`
	RotatedNote  string  `json:"rotatedNote"`
}

var (
	liveMu           sync.Mutex
	liveBootMs       = time.Now().UnixMilli()
	liveStrats       = map[string]*LiveStratAccount{}
	liveStratOrder   = []string{}
	liveSlots        = map[string]*LiveSlotRuntime{}
	liveFills        = []LiveFillAudit{}
	ltTxTimesMs      = []int64{}
	lastLiveSaveMs   int64
	liveInitDone     atomic.Bool
)

func canSendLtTxLocked(nowMs int64, isUrgentClose bool) bool {
	cutoff := nowMs - 60000
	kept := ltTxTimesMs[:0]
	for _, t := range ltTxTimesMs {
		if t >= cutoff {
			kept = append(kept, t)
		}
	}
	ltTxTimesMs = kept
	if isUrgentClose {
		return len(ltTxTimesMs) < 57
	}
	return len(ltTxTimesMs) < MaxLtTxPerMin
}

func recordLtTxLocked(nowMs int64) {
	ltTxTimesMs = append(ltTxTimesMs, nowMs)
}

func countLtTxLastMinLocked(nowMs int64) int {
	cutoff := nowMs - 60000
	cnt := 0
	for _, t := range ltTxTimesMs {
		if t >= cutoff {
			cnt++
		}
	}
	return cnt
}

func initLiveMicroArena() {
	liveMu.Lock()
	if len(liveStratOrder) > 0 {
		liveMu.Unlock()
		return
	}

	defs := []struct {
		id        string
		name      string
		venueDesc string
		desc      string
		peerVenue string
		clip      float64
		scale489  float64
		startCash float64
		entryBp   float64
		exitBp    float64
		syms      []string
	}{
		{
			id:        "LIVE_S5_MARGIN_STRICT",
			name:      "0% LT Maker -> BN USDC-M Strict (#1 Winner)",
			venueDesc: "LT #770015 (0%) <-> BN USDC-M (0% M / 4bp T)",
			desc:      "Top L10-verified winner (+$9.51/hr at $489 scale). Rests 0% PostOnly Maker on Lighter -> <4ms Tokyo Taker hedge on Binance USDC-M -> 0% GTX Maker exit on Binance -> 0% Lighter IOC close (4.00bp total 4-leg fee).",
			peerVenue: "BN_USDC",
			clip:      15.0,
			scale489:  550.0 / 15.0,
			startCash: 40.0,
			entryBp:   5.5,
			exitBp:    2.3,
			syms:      []string{"UNI", "HYPE", "ENA", "NEAR", "SUI", "ARB"},
		},
		{
			id:        "LIVE_S3_LT_ASTER_CRYPTO",
			name:      "0% LT Maker <-> AsterDEX Crypto Arb (#2 Winner)",
			venueDesc: "LT #770015 (0%) <-> Aster Crypto (0% M / 4bp T)",
			desc:      "Runner-up L10-verified winner (+$8.88/hr at $489 scale). Rests 0% PostOnly Maker on Lighter -> <8ms Tokyo Taker hedge on AsterDEX -> 0% GTX Maker exit on Aster -> 0% Lighter IOC close (4.00bp total 4-leg fee).",
			peerVenue: "ASTER_C",
			clip:      15.0,
			scale489:  580.0 / 15.0,
			startCash: 25.0,
			entryBp:   5.5,
			exitBp:    2.2,
			syms:      []string{"VVV", "XMR", "LIT"},
		},
		{
			id:        "LIVE_S4_ASTER_LT_RWA",
			name:      "0% LT Maker <-> AsterDEX 1.25bp RWA Equities (#3 Winner)",
			venueDesc: "LT #770015 (0%) <-> Aster RWA (0% M / 1.25bp T)",
			desc:      "Lowest-fee cross-venue arb in crypto: 0% Lighter #770015 Maker -> 1.25bp Aster RWA Taker hedge -> 0% Aster GTX Maker exit -> 0% Lighter IOC close = ONLY 1.25bp total 4-leg fee across NVDA/AAPL/TSLA/QQQ/SPY/META/COIN/MSTR!",
			peerVenue: "ASTER_RWA",
			clip:      15.0,
			scale489:  1100.0 / 15.0,
			startCash: 25.0,
			entryBp:   3.8,
			exitBp:    1.8,
			syms:      []string{"NVDA", "AAPL", "TSLA", "QQQ", "SPY", "META", "COIN", "MSTR"},
		},
		{
			id:        "LIVE_S2_ZERO_FEE_DN",
			name:      "0% LT Maker -> BN USDC-M Broad Alts (#4 Winner)",
			venueDesc: "LT #770015 (0%) <-> BN USDC-M (0% M / 4bp T)",
			desc:      "High-frequency broad alt bucket (+$5.40/hr L10-verified). Disjoint symbol partition (AAVE/AVAX/WLD/LINK/DOGE/XRP/TIA/PENGU/TRUMP) with 0% Lighter Maker entry and 0% Binance USDC-M Maker exit.",
			peerVenue: "BN_USDC",
			clip:      15.0,
			scale489:  650.0 / 15.0,
			startCash: 40.0,
			entryBp:   4.8,
			exitBp:    2.2,
			syms:      []string{"AAVE", "AVAX", "WLD", "LINK", "DOGE", "XRP", "TIA", "PENGU", "TRUMP"},
		},
	}

	for _, d := range defs {
		liveStratOrder = append(liveStratOrder, d.id)
		liveStrats[d.id] = &LiveStratAccount{
			ID:         d.id,
			Name:       d.name,
			Desc:       d.desc,
			VenueDesc:  d.venueDesc,
			ClipUsd:    d.clip,
			StartCash:  d.startCash,
			Cash:       d.startCash,
			Equity:     d.startCash,
			PeakEquity: d.startCash,
			State:      "IDLE",
		}
		liveSlots[d.id] = &LiveSlotRuntime{
			StratID:       d.id,
			PeerVenue:     d.peerVenue,
			Symbols:       append([]string(nil), d.syms...),
			OrigPeerVenue: d.peerVenue,
			OrigSymbols:   append([]string(nil), d.syms...),
			ClipUsd:       d.clip,
			ScaleMult489:  d.scale489,
			EntryDisloc:   d.entryBp,
			ExitNetBp:     d.exitBp,
			CancelTolBp:   2.5,
			MaxRestSec:    35.0,
			MaxHoldSec:    900.0,
			State:         "IDLE",
		}
	}
	liveMu.Unlock()
	loadLiveArenaState()
	liveInitDone.Store(true)
}

func loadLiveArenaState() {
	raw, err := os.ReadFile(LiveArenaSavePath)
	if err != nil || len(raw) < 20 {
		return
	}
	var saved LiveArenaTelemetry
	if json.Unmarshal(raw, &saved) != nil || !strings.HasPrefix(saved.Version, "LIVE-MICRO-v105.") || saved.BootTsMs <= 0 {
		return
	}
	liveMu.Lock()
	defer liveMu.Unlock()
	liveBootMs = saved.BootTsMs
	for _, st := range saved.Strategies {
		dst := liveStrats[st.ID]
		if dst == nil {
			continue
		}
		dst.Cash = st.Cash
		dst.RealizedGross = st.RealizedGross
		dst.FeesPaid = st.FeesPaid
		dst.FundingUsd = st.FundingUsd
		dst.Equity = st.Cash
		dst.PeakEquity = st.PeakEquity
		dst.MaxDrawdownUsd = st.MaxDrawdownUsd
		dst.MakerPlaced = st.MakerPlaced
		dst.MakerCancelled = st.MakerCancelled
		dst.MakerFills = st.MakerFills
		dst.TakerFills = st.TakerFills
		dst.RoundTrips = st.RoundTrips
		dst.WinTrips = st.WinTrips
		dst.GrossVolumeUsd = st.GrossVolumeUsd
		dst.EquityCurve = st.EquityCurve
		dst.Halted = st.Halted
		dst.HaltReason = st.HaltReason

		// Fallback seamless position recovery even when upgrading from v105.2 (before SlotSnapshots existed):
		rt := liveSlots[st.ID]
		if rt != nil && len(saved.SlotSnapshots) == 0 && len(st.PosSnap) >= 2 && (st.State == "HEDGED" || st.State == "PEER_EXIT_OPEN") {
			var ltP, peerP LivePosView
			for _, pv := range st.PosSnap {
				if pv.Venue == "LT_STD" {
					ltP = pv
				} else {
					peerP = pv
				}
			}
			if ltP.Symbol != "" && peerP.Symbol != "" && math.Abs(ltP.Qty) > 0 {
				rt.State = "HEDGED" // safely re-evaluates & re-rests GTX exit on boot!
				rt.Sym = ltP.Symbol
				rt.PeerVenue = peerP.Venue
				rt.PeerSym = peerP.Symbol
				rt.HedgedQty = math.Abs(ltP.Qty)
				rt.EntryLtPx = ltP.AvgPx
				rt.EntryPeerPx = peerP.AvgPx
				rt.EntryFx = 1.0
				rt.EntryPeerFee = 4.0
				if peerP.Venue == "ASTER_RWA" {
					rt.EntryPeerFee = 1.25
				}
				if ltP.Qty < 0 {
					rt.LtSide, rt.PeerSide = "ASK", "BUY"
				} else {
					rt.LtSide, rt.PeerSide = "BID", "SELL"
				}
				rt.OpenedAt = time.UnixMilli(ltP.OpenedMs)
				rt.TradeID = ltP.OpenedMs
			}
		}
	}
	for _, sn := range saved.SlotSnapshots {
		rt := liveSlots[sn.StratID]
		if rt == nil || (sn.State != "HEDGED" && sn.State != "PEER_EXIT_OPEN") || sn.HedgedQty <= 0 {
			continue
		}
		rt.State = sn.State
		rt.Sym = sn.Sym
		rt.PeerVenue = sn.PeerVenue
		rt.PeerSym = sn.PeerSym
		rt.LtSide = sn.LtSide
		rt.PeerSide = sn.PeerSide
		rt.HedgedQty = sn.HedgedQty
		rt.EntryLtPx = sn.EntryLtPx
		rt.EntryPeerPx = sn.EntryPeerPx
		rt.EntryFx = sn.EntryFx
		rt.EntryPeerFee = sn.EntryPeerFee
		rt.OpenedAt = time.UnixMilli(sn.OpenedMs)
		rt.TradeID = sn.TradeID
		rt.ExitPeerOid = sn.ExitPeerOid
		rt.ExitPeerPx = sn.ExitPeerPx
		rt.ExitPlaced = time.Now()
		rt.RotatedNote = sn.RotatedNote
	}
	if len(saved.RecentFills) > 0 {
		liveFills = make([]LiveFillAudit, len(saved.RecentFills))
		for i := range saved.RecentFills {
			liveFills[i] = saved.RecentFills[len(saved.RecentFills)-1-i]
		}
	}
	fmt.Printf("[%s] Hot-reloaded live micro-arena state seamlessly (uptime %.1fm)\n", LiveArenaVer, float64(time.Now().UnixMilli()-liveBootMs)/60000.0)
}

func hasAdoptedLivePositions() bool {
	liveMu.Lock()
	defer liveMu.Unlock()
	for _, rt := range liveSlots {
		if rt != nil && (rt.State == "HEDGED" || rt.State == "PEER_EXIT_OPEN") && rt.HedgedQty > 0 {
			return true
		}
	}
	return false
}

func appendLiveFillLocked(f LiveFillAudit) {
	liveFills = append(liveFills, f)
	if len(liveFills) > 120 {
		liveFills = append([]LiveFillAudit(nil), liveFills[len(liveFills)-120:]...)
	}
}

func placeLtPostOnlyMakerSync(sym, side string, px, qty float64) (coi int64, ok bool) {
	m, exists := markets[sym]
	if !exists || px <= 0 || qty < m.MinB {
		return 0, false
	}
	baseInt := int64(math.Round(qty * math.Pow10(m.SzDec)))
	pxF := roundTo(px, m.PxDec)
	pxInt := uint32(math.Round(pxF * math.Pow10(m.PxDec)))
	if baseInt <= 0 || pxInt == 0 {
		return 0, false
	}
	var isAsk uint8
	if side == "ASK" {
		isAsk = 1
	}
	coi = coiSeq.Add(1)
	exp := time.Now().Add(24 * time.Hour).UnixMilli()
	dbg := fmt.Sprintf("LT_PO_%s_%s px=%.5f sz=%.5f", sym, side, pxF, qty)
	err := sendSignedTx(fmt.Sprintf("po_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      m.ID,
			ClientOrderIndex: coi,
			BaseAmount:       baseInt,
			Price:            pxInt,
			IsAsk:            isAsk,
			Type:             0, // LIMIT
			TimeInForce:      2, // POST_ONLY (0% Maker strictly!)
			ReduceOnly:       0,
			OrderExpiry:      exp,
		}, opts)
		if e != nil {
			return 0, "", e
		}
		s, e := ti.GetTxInfo()
		return ti.GetTxType(), s, e
	})
	if err != nil {
		return 0, false
	}
	return coi, true
}

func cancelLtOrderByCOISync(sym string, coi int64) {
	m, exists := markets[sym]
	if !exists || coi <= 0 {
		return
	}
	_ = sendSignedTx(fmt.Sprintf("c_%d", coi), "cancel_"+sym, func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCancelOrderTransaction(&types.CancelOrderTxReq{
			MarketIndex: m.ID,
			Index:       coi,
		}, opts)
		if e != nil {
			return 0, "", e
		}
		s, e := ti.GetTxInfo()
		return ti.GetTxType(), s, e
	})
}

func getPeerSpecAndBook(peerVenue, sym string, now time.Time) (peerSym string, qDec, pxDec int, step, tick, minNot, mFeeBp, tFeeBp, bid, ask, emaBps, jumpBps, fx float64, warmTicks int, bookAge time.Duration, ok bool) {
	if peerVenue == "BN_USDC" {
		pSym := sym + "USDC"
		dnMu.Lock()
		sp, exists := dnByBnSym[pSym]
		db := dnBooks[pSym]
		fxVal, okFx := dnQuoteFxToUsdtLocked("USDC", now)
		blocked := dnBlockedSym[pSym]
		if db != nil {
			bid, ask, emaBps, jumpBps, warmTicks = db.Bid, db.Ask, db.EmaBps, db.LastJumpBps, db.WarmTicks
			bookAge = now.Sub(db.TS)
		}
		dnMu.Unlock()
		if !exists || blocked || !okFx || db == nil || bid <= 0 || ask <= bid {
			return "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 0, 999 * time.Second, false
		}
		// Note: for BN_USDC, bnMidUsd = bnMid * fxVal => 1 USDC = fxVal USDT (~1.000)
		return pSym, sp.QDec, sp.PxDec, sp.Step, sp.Tick, sp.MinNot, 0.0, 4.0, bid, ask, emaBps, jumpBps, fxVal, warmTicks, bookAge, true
	}

	// ASTER_C or ASTER_RWA (USDT-quoted)
	pSym := sym + "USDT"
	asterMu.Lock()
	sp, exists := asterSpecs[pSym]
	db := asterBooks[pSym]
	if db != nil {
		bid, ask, emaBps, jumpBps, warmTicks = db.Bid, db.Ask, db.EmaBps, db.LastJumpBps, db.WarmTicks
		bookAge = now.Sub(db.TS)
	}
	asterMu.Unlock()
	if !exists || db == nil || bid <= 0 || ask <= bid {
		return "", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 0, 999 * time.Second, false
	}
	dnMu.Lock()
	fxUsdt, okFx := dnQuoteFxToUsdtLocked("USDC", now) // BTCUSDT / BTCUSDC (~1.0005 USDT per USD)
	dnMu.Unlock()
	if !okFx || fxUsdt <= 0 {
		fxUsdt = 1.0005
	}
	// Convert Aster USDT price -> USD price by multiplying by (1 / fxUsdt)
	return pSym, sp.QDec, sp.PxDec, sp.Step, sp.Tick, sp.MinNot, sp.MakerFeeBp, sp.TakerFeeBp, bid, ask, emaBps, jumpBps, 1.0 / fxUsdt, warmTicks, bookAge, true
}

func executePeerMarketOrder(peerVenue, peerSym, side string, qty float64, qDec int, reduceOnly bool) (filledQty, avgPx float64, latMs int64, ok bool) {
	t0 := time.Now()
	p := url.Values{}
	p.Set("symbol", peerSym)
	p.Set("side", side)
	p.Set("type", "MARKET")
	if reduceOnly {
		p.Set("reduceOnly", "true")
	}
	p.Set("quantity", strconv.FormatFloat(qty, 'f', qDec, 64))
	p.Set("newOrderRespType", "RESULT")

	var sc int
	var body []byte
	var err error
	if peerVenue == "BN_USDC" {
		ensureBnLeverage(peerSym)
		sc, body, err = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
	} else {
		lev := 5
		if peerVenue == "ASTER_RWA" {
			lev = 8
		}
		ensureAsterLeverage(peerSym, lev)
		sc, body, err = asterSignReq("POST", "/fapi/v3/order", p)
	}
	latMs = time.Since(t0).Milliseconds()
	if err != nil || sc != 200 {
		fmt.Printf("[PEER_MKT_ERR venue=%s sym=%s side=%s qty=%.5f sc=%d err=%v body=%s]\n",
			peerVenue, peerSym, side, qty, sc, err, string(body))
		return 0, 0, latMs, false
	}
	var or struct {
		OrderID int64  `json:"orderId"`
		Status  string `json:"status"`
		ExecQty string `json:"executedQty"`
		AvgPx   string `json:"avgPrice"`
	}
	if json.Unmarshal(body, &or) != nil {
		return 0, 0, latMs, false
	}
	fq, _ := strconv.ParseFloat(or.ExecQty, 64)
	ap, _ := strconv.ParseFloat(or.AvgPx, 64)
	if (fq <= 0 || ap <= 0) && or.OrderID > 0 {
		time.Sleep(120 * time.Millisecond)
		if peerVenue == "BN_USDC" {
			_, fq2, ap2, _, ok2 := queryBnOrder(peerSym, or.OrderID, time.Time{}, true)
			if ok2 && fq2 > 0 {
				fq, ap = fq2, ap2
			}
		} else {
			_, fq2, ap2, _, ok2 := queryAsterOrder(peerSym, or.OrderID, time.Time{}, true)
			if ok2 && fq2 > 0 {
				fq, ap = fq2, ap2
			}
		}
	}
	return fq, ap, latMs, fq > 0
}

func placePeerMakerExit(peerVenue, peerSym, side string, qty, px float64, qDec, pxDec int) (oid int64, ok bool) {
	p := url.Values{}
	p.Set("symbol", peerSym)
	p.Set("side", side)
	p.Set("type", "LIMIT")
	p.Set("timeInForce", "GTX") // PostOnly 0% Maker on both Binance & AsterDEX!
	p.Set("reduceOnly", "true")
	p.Set("quantity", strconv.FormatFloat(qty, 'f', qDec, 64))
	p.Set("price", strconv.FormatFloat(px, 'f', pxDec, 64))

	var sc int
	var body []byte
	var err error
	if peerVenue == "BN_USDC" {
		sc, body, err = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
	} else {
		sc, body, err = asterSignReq("POST", "/fapi/v3/order", p)
	}
	if err != nil || sc != 200 {
		return 0, false
	}
	var or struct {
		OrderID int64  `json:"orderId"`
		Status  string `json:"status"`
	}
	if json.Unmarshal(body, &or) != nil || or.OrderID <= 0 || or.Status == "EXPIRED" || or.Status == "CANCELED" {
		return 0, false
	}
	return or.OrderID, true
}

func cancelPeerOrder(peerVenue, peerSym string, oid int64) (status string, execQty, avgPx float64) {
	qp := url.Values{}
	qp.Set("symbol", peerSym)
	qp.Set("orderId", strconv.FormatInt(oid, 10))
	var sc int
	var body []byte
	if peerVenue == "BN_USDC" {
		sc, body, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
	} else {
		sc, body, _ = asterSignReq("DELETE", "/fapi/v3/order", qp)
	}
	var co struct {
		Status  string `json:"status"`
		ExecQty string `json:"executedQty"`
		AvgPx   string `json:"avgPrice"`
	}
	_ = json.Unmarshal(body, &co)
	if sc != 200 {
		if peerVenue == "BN_USDC" {
			st2, eq2, ap2, _, ok2 := queryBnOrder(peerSym, oid, time.Time{}, true)
			if ok2 {
				return st2, eq2, ap2
			}
		} else {
			st2, eq2, ap2, _, ok2 := queryAsterOrder(peerSym, oid, time.Time{}, true)
			if ok2 {
				return st2, eq2, ap2
			}
		}
	}
	eq, _ := strconv.ParseFloat(co.ExecQty, 64)
	ap, _ := strconv.ParseFloat(co.AvgPx, 64)
	return co.Status, eq, ap
}

func closeLtLegGuaranteedSync(sym string, expectedPos float64, lb, la float64) (exitEstPx float64, latMs int64) {
	t0 := time.Now()
	m := markets[sym]
	curPos := expectedPos
	if st := mktStates[sym]; st != nil {
		st.mu.Lock()
		if math.Abs(st.pos) >= m.MinB {
			curPos = st.pos
		}
		if st.ltBid > 0 && st.ltAsk > 0 {
			lb, la = st.ltBid, st.ltAsk
		}
		st.mu.Unlock()
	}
	if lb <= 0 || la <= 0 {
		lb, la = fetchLtBookREST(m.ID)
	}
	closeSide := "BID"
	closePx := roundTo(la*1.005, m.PxDec)
	exitEstPx = la
	if curPos > 0 {
		closeSide = "ASK"
		closePx = roundTo(lb*0.995, m.PxDec)
		exitEstPx = lb
	}
	qty := math.Abs(curPos)
	if qty < m.MinB {
		return exitEstPx, 0
	}

	liveMu.Lock()
	recordLtTxLocked(time.Now().UnixMilli())
	liveMu.Unlock()

	placeLtHedgeIOC(sym, closeSide, closePx, qty, true)

	cDeadline := time.Now().Add(1100 * time.Millisecond)
	restChecked := false
	for time.Now().Before(cDeadline) {
		if st := mktStates[sym]; st != nil {
			st.mu.Lock()
			pNow := st.pos
			st.mu.Unlock()
			if math.Abs(pNow) < m.MinB {
				return exitEstPx, time.Since(t0).Milliseconds()
			}
		}
		if !restChecked && time.Since(t0) >= 600*time.Millisecond {
			restChecked = true
			if rp, _ := fetchLtPosREST(sym); math.Abs(rp) < m.MinB {
				return exitEstPx, time.Since(t0).Milliseconds()
			}
		}
		select {
		case <-ltFillCh:
		case <-time.After(45 * time.Millisecond):
		}
	}

	// Verify via REST and fire 1.5% wide sweep if still non-zero:
	if remPos, _ := fetchLtPosREST(sym); math.Abs(remPos) >= m.MinB {
		lb2, la2 := fetchLtBookREST(m.ID)
		if lb2 <= 0 || la2 <= 0 {
			lb2, la2 = lb, la
		}
		rSide := "BID"
		rPx := roundTo(la2*1.015, m.PxDec)
		if remPos > 0 {
			rSide = "ASK"
			rPx = roundTo(lb2*0.985, m.PxDec)
		}
		liveMu.Lock()
		recordLtTxLocked(time.Now().UnixMilli())
		liveMu.Unlock()
		placeLtHedgeIOC(sym, rSide, rPx, math.Abs(remPos), true)
		time.Sleep(600 * time.Millisecond)
		_, _ = fetchLtPosREST(sym)
	}
	return exitEstPx, time.Since(t0).Milliseconds()
}

func finalizeSlotClose(rt *LiveSlotRuntime, peerExitPx, peerExitFeeBp float64, execType, reason string, exitLatMs int64, now time.Time) {
	m := markets[rt.Sym]
	var lb, la, ltPos float64
	if st := mktStates[rt.Sym]; st != nil {
		st.mu.Lock()
		lb, la, ltPos = st.ltBid, st.ltAsk, st.pos
		st.mu.Unlock()
	}
	expectedLtPos := -rt.HedgedQty
	if rt.LtSide == "BID" {
		expectedLtPos = rt.HedgedQty
	}
	if math.Abs(ltPos) >= m.MinB {
		expectedLtPos = ltPos
	}
	ltExitPx, ltLatMs := closeLtLegGuaranteedSync(rt.Sym, expectedLtPos, lb, la)
	if ltExitPx <= 0 {
		ltExitPx = rt.EntryLtPx
	}

	qty := rt.HedgedQty
	fx := rt.EntryFx
	if fx <= 0 {
		fx = 1.0
	}
	notional := qty * rt.EntryLtPx
	entryFeeUsd := (qty * rt.EntryPeerPx * fx) * (rt.EntryPeerFee / 10000.0)
	exitFeeUsd := (qty * peerExitPx * fx) * (peerExitFeeBp / 10000.0)
	totalFeeUsd := entryFeeUsd + exitFeeUsd

	var ltGross, peerGross float64
	if rt.LtSide == "ASK" {
		// Short Lighter, Long Peer
		ltGross = (rt.EntryLtPx - ltExitPx) * qty
		peerGross = (peerExitPx - rt.EntryPeerPx) * fx * qty
	} else {
		// Long Lighter, Short Peer
		ltGross = (ltExitPx - rt.EntryLtPx) * qty
		peerGross = (rt.EntryPeerPx - peerExitPx) * fx * qty
	}
	grossTotal := ltGross + peerGross
	netUsd := grossTotal - totalFeeUsd
	netBps := 0.0
	if notional > 0 {
		netBps = netUsd / notional * 10000.0
	}
	holdSec := now.Sub(rt.OpenedAt).Seconds()

	closePeerSide := "SELL"
	if rt.PeerSide == "SELL" {
		closePeerSide = "BUY"
	}
	closeLtSide := "BUY"
	if rt.LtSide == "BID" {
		closeLtSide = "SELL"
	}

	nowMs := now.UnixMilli()
	liveMu.Lock()
	acct := liveStrats[rt.StratID]
	if acct != nil {
		// Note: entryFeeUsd was already added to FeesPaid at hedge open; add exitFeeUsd + grossTotal now!
		acct.RealizedGross = roundTo(acct.RealizedGross+grossTotal, 4)
		acct.FeesPaid = roundTo(acct.FeesPaid+exitFeeUsd, 4)
		acct.Cash = roundTo(acct.StartCash+acct.RealizedGross-acct.FeesPaid+acct.FundingUsd, 4)
		acct.GrossVolumeUsd = roundTo(acct.GrossVolumeUsd+notional*2.0, 2)
		if execType == "MAKER_GTX_EXIT" {
			acct.MakerFills++
		} else {
			acct.TakerFills++
		}
		acct.TakerFills++ // Lighter IOC close
		acct.RoundTrips++
		if netUsd > 0 {
			acct.WinTrips++
		}
	}
	appendLiveFillLocked(LiveFillAudit{
		TsMs:      nowMs,
		StratID:   rt.StratID,
		Venue:     rt.PeerVenue,
		Symbol:    rt.PeerSym,
		Side:      closePeerSide,
		ExecType:  execType,
		FillPx:    peerExitPx,
		SignalPx:  peerExitPx,
		Qty:       qty,
		Notional:  roundTo(notional, 2),
		FeeUsd:    roundTo(exitFeeUsd, 4),
		Realized:  roundTo(peerGross-exitFeeUsd, 4),
		LatencyMs: exitLatMs,
		Tag:       "PEER_EXIT",
	})
	appendLiveFillLocked(LiveFillAudit{
		TsMs:      nowMs,
		StratID:   rt.StratID,
		Venue:     "LT_STD",
		Symbol:    rt.Sym,
		Side:      closeLtSide,
		ExecType:  "LT_IOC_CLOSE",
		FillPx:    ltExitPx,
		SignalPx:  ltExitPx,
		Qty:       qty,
		Notional:  roundTo(notional, 2),
		FeeUsd:    0.0,
		Realized:  roundTo(ltGross, 4),
		LatencyMs: ltLatMs,
		Tag:       reason,
	})
	rt.State = "IDLE"
	rt.Sym = ""
	rt.PeerSym = ""
	rt.LtCOI = 0
	rt.ExitPeerOid = 0
	rt.HedgedQty = 0
	rt.CoolUntil = now.Add(3 * time.Second)
	liveMu.Unlock()

	recordStratClose(rt.TradeID, rt.StratID,
		fmt.Sprintf("LT_%s_%s+%s_%s", rt.Sym, rt.LtSide, rt.PeerVenue, rt.PeerSide),
		rt.Sym, notional, holdSec, netBps, netUsd, peerGross-totalFeeUsd, ltGross, totalFeeUsd, reason)
}

func stepLiveSlot(rt *LiveSlotRuntime, now time.Time) {
	liveMu.Lock()
	acct := liveStrats[rt.StratID]
	halted := acct != nil && acct.Halted
	liveMu.Unlock()
	if acct == nil {
		return
	}

	// -------------------------------------------------------------------------
	// STATE 1: LT_ENTRY_OPEN (0% PostOnly Maker resting on Lighter #770015)
	// -------------------------------------------------------------------------
	if rt.State == "LT_ENTRY_OPEN" {
		m := markets[rt.Sym]
		st := mktStates[rt.Sym]
		if st == nil {
			return
		}
		st.mu.Lock()
		ltPos, ltAvg, lb, la := st.pos, st.avgEntry, st.ltBid, st.ltAsk
		st.mu.Unlock()

		// Periodic 2.5s REST fallback check while order is resting so WS drops never miss a fill:
		if now.Sub(rt.LastOrdPoll) >= 2500*time.Millisecond {
			rt.LastOrdPoll = now
			if rp, rae := fetchLtPosREST(rt.Sym); math.Abs(rp) >= m.MinB {
				ltPos, ltAvg = rp, rae
			}
		}

		// Did Lighter Maker fill (fully or partially >= MinB)?
		// HEDGE-FIRST (<8ms): Fire Peer Market Hedge IMMEDIATELY upon WS fill, BEFORE canceling remainder on Lighter!
		if math.Abs(ltPos) >= m.MinB {
			fillQty := math.Abs(ltPos)
			if ltAvg <= 0 {
				ltAvg = rt.LtLimitPx
			}
			coiToCancel := rt.LtCOI
			symToCancel := rt.Sym
			go func() {
				cancelLtOrderByCOISync(symToCancel, coiToCancel)
				liveMu.Lock()
				recordLtTxLocked(time.Now().UnixMilli())
				liveMu.Unlock()
			}()

			peerSym, qDec, _, step, _, minNot, _, tFeeBp, pBid, pAsk, _, _, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
			if !okP {
				peerSym = rt.PeerSym
				tFeeBp = 4.0
				if rt.PeerVenue == "ASTER_RWA" {
					tFeeBp = 1.25
				}
				fx = 1.0
			}
			hQty := fillQty
			if step > 0 {
				hQty = roundTo(math.Floor(fillQty/step+1e-9)*step, qDec)
			}
			sigPx := pBid
			if rt.PeerSide == "BUY" {
				sigPx = pAsk
			}
			if hQty <= 0 || (minNot > 0 && sigPx > 0 && hQty*sigPx < minNot-0.4) {
				// Dust fill below peer minNotional: immediately flatten Lighter leg cleanly!
				_, _ = closeLtLegGuaranteedSync(rt.Sym, ltPos, lb, la)
				liveMu.Lock()
				rt.State = "IDLE"
				rt.Sym = ""
				rt.CoolUntil = now.Add(5 * time.Second)
				liveMu.Unlock()
				return
			}

			fq, ap, latMs, okH := executePeerMarketOrder(rt.PeerVenue, peerSym, rt.PeerSide, hQty, qDec, false)
			if !okH || fq <= 0 {
				// Zero unhedged tolerance: immediately flatten Lighter leg if peer market order fails!
				_, _ = closeLtLegGuaranteedSync(rt.Sym, ltPos, lb, la)
				liveMu.Lock()
				rt.State = "IDLE"
				rt.Sym = ""
				rt.CoolUntil = now.Add(10 * time.Second)
				liveMu.Unlock()
				return
			}
			if ap <= 0 {
				ap = sigPx
			}
			notional := fq * ltAvg
			entryFeeUsd := (fq * ap * fx) * (tFeeBp / 10000.0)
			slipBp := 0.0
			if sigPx > 0 {
				if rt.PeerSide == "BUY" {
					slipBp = ((ap - sigPx) / sigPx) * 10000.0
				} else {
					slipBp = ((sigPx - ap) / sigPx) * 10000.0
				}
			}
			ltSideHuman := "BUY"
			if rt.LtSide == "ASK" {
				ltSideHuman = "SELL"
			}

			tid := now.UnixMilli()
			liveMu.Lock()
			acct.MakerFills++
			acct.TakerFills++
			acct.FeesPaid = roundTo(acct.FeesPaid+entryFeeUsd, 4)
			acct.Cash = roundTo(acct.StartCash+acct.RealizedGross-acct.FeesPaid+acct.FundingUsd, 4)
			acct.GrossVolumeUsd = roundTo(acct.GrossVolumeUsd+notional*2.0, 2)
			appendLiveFillLocked(LiveFillAudit{
				TsMs:      tid,
				StratID:   rt.StratID,
				Venue:     "LT_STD",
				Symbol:    rt.Sym,
				Side:      ltSideHuman,
				ExecType:  "LT_MAKER_0FEE",
				FillPx:    ltAvg,
				SignalPx:  rt.LtLimitPx,
				Qty:       fq,
				Notional:  roundTo(notional, 2),
				FeeUsd:    0.0,
				Realized:  0.0,
				LatencyMs: now.Sub(rt.PlacedAt).Milliseconds(),
				Tag:       "LT_ENTRY",
			})
			appendLiveFillLocked(LiveFillAudit{
				TsMs:      tid,
				StratID:   rt.StratID,
				Venue:     rt.PeerVenue,
				Symbol:    peerSym,
				Side:      rt.PeerSide,
				ExecType:  "TOKYO_TAKER_HEDGE",
				FillPx:    ap,
				SignalPx:  sigPx,
				Qty:       fq,
				Notional:  roundTo(fq*ap*fx, 2),
				FeeUsd:    roundTo(entryFeeUsd, 4),
				Realized:  roundTo(-entryFeeUsd, 4),
				SlipBp:    roundTo(slipBp, 2),
				LatencyMs: latMs,
				Tag:       "PEER_HEDGE",
			})
			rt.State = "HEDGED"
			rt.HedgedQty = fq
			rt.EntryLtPx = ltAvg
			rt.EntryPeerPx = ap
			rt.EntryFx = fx
			rt.EntryPeerFee = tFeeBp
			rt.OpenedAt = now
			rt.TradeID = tid
			liveMu.Unlock()

			lockedEdgeBp := ((ltAvg - ap*fx) / (ap * fx) * 10000.0) - tFeeBp
			if rt.LtSide == "BID" {
				lockedEdgeBp = ((ap*fx - ltAvg) / (ap * fx) * 10000.0) - tFeeBp
			}
			recordStratOpen(tid, rt.StratID,
				fmt.Sprintf("LT_%s_%s+%s_%s", rt.Sym, rt.LtSide, rt.PeerVenue, rt.PeerSide),
				rt.Sym, notional, lockedEdgeBp,
				fmt.Sprintf("LiveMicro Lt0pct@%.5f+Peer@%.5f(%dms)", ltAvg, ap, latMs))
			return
		}

		// If CancelSent is already in flight (waiting past 305ms speed-bump), check completion:
		if rt.CancelSent {
			if now.Sub(rt.CancelAt) >= 750*time.Millisecond {
				if rp, _ := fetchLtPosREST(rt.Sym); math.Abs(rp) >= m.MinB {
					return // next tick will catch math.Abs(ltPos) >= m.MinB and hedge it immediately!
				}
				liveMu.Lock()
				rt.State = "IDLE"
				rt.Sym = ""
				rt.LtCOI = 0
				rt.CancelSent = false
				rt.CoolUntil = now.Add(1500 * time.Millisecond)
				liveMu.Unlock()
			}
			return
		}

		// Check if resting Lighter Maker order drifted or timed out (min 5s age to conserve #770015 tx/min):
		ageS := now.Sub(rt.PlacedAt).Seconds()
		_, _, _, _, _, _, _, _, pBid, pAsk, emaBps, jumpBps, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
		shouldCancel := halted || ageS >= rt.MaxRestSec
		if okP && ageS >= 4.5 {
			pBidUsd := pBid * fx
			pAskUsd := pAsk * fx
			curDisloc := (((rt.LtLimitPx - pAskUsd) / pAskUsd) * 10000.0) - emaBps
			refBbo := la
			if rt.LtSide == "BID" {
				curDisloc = (((pBidUsd - rt.LtLimitPx) / pBidUsd) * 10000.0) + emaBps
				refBbo = lb
			}
			driftBp := 0.0
			if refBbo > 0 {
				driftBp = math.Abs(rt.LtLimitPx-refBbo) / refBbo * 10000.0
			}
			if curDisloc < rt.EntryDisloc-2.2 || driftBp > rt.CancelTolBp || math.Abs(jumpBps) > 2.5 {
				shouldCancel = true
			}
		}
		if shouldCancel {
			liveMu.Lock()
			if !canSendLtTxLocked(now.UnixMilli(), true) {
				liveMu.Unlock()
				return
			}
			recordLtTxLocked(now.UnixMilli())
			acct.MakerCancelled++
			liveMu.Unlock()
			cancelLtOrderByCOISync(rt.Sym, rt.LtCOI)
			rt.CancelSent = true
			rt.CancelAt = now
		}
		return
	}

	// -------------------------------------------------------------------------
	// STATE 2: HEDGED (Look for 0%-Fee GTX Maker Exit on Peer Venue)
	// -------------------------------------------------------------------------
	if rt.State == "HEDGED" {
		st := mktStates[rt.Sym]
		if st == nil {
			return
		}
		st.mu.Lock()
		lb, la := st.ltBid, st.ltAsk
		st.mu.Unlock()
		peerSym, qDec, pxDec, _, _, _, mFeeBp, tFeeBp, pBid, pAsk, _, jumpBps, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
		if !okP || lb <= 0 || la <= lb {
			return
		}

		holdS := now.Sub(rt.OpenedAt).Seconds()
		rtMakerFeeBp := rt.EntryPeerFee + mFeeBp // 4.0bp (or 1.25bp on RWA!) + 0.0bp = 4.0bp / 1.25bp!

		var closePeerSide string
		var exitMakerPx, combMakerNetBp float64
		if rt.PeerSide == "BUY" {
			// Long Peer, Short Lighter -> rest SELL Maker at pAsk on Peer, close BUY at la on Lighter
			closePeerSide = "SELL"
			exitMakerPx = roundTo(pAsk, pxDec)
			combMakerNetBp = (((exitMakerPx-rt.EntryPeerPx)*fx)+(rt.EntryLtPx-la))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
		} else {
			// Short Peer, Long Lighter -> rest BUY Maker at pBid on Peer, close SELL at lb on Lighter
			closePeerSide = "BUY"
			exitMakerPx = roundTo(pBid, pxDec)
			combMakerNetBp = (((rt.EntryPeerPx-exitMakerPx)*fx)+(lb-rt.EntryLtPx))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
		}

		// Zero per-trade basis stop-loss (pure 1:1 hedged convergence); only flush if strategy circuit breaker tripped:
		if halted {
			_, ap, latMs, okM := executePeerMarketOrder(rt.PeerVenue, peerSym, closePeerSide, rt.HedgedQty, qDec, true)
			if okM {
				if ap <= 0 {
					ap = 0.5 * (pBid + pAsk)
				}
				finalizeSlotClose(rt, ap, tFeeBp, "TAKER_HALT_FLUSH", "STRAT_CIRCUIT_BREAKER_FLUSH", latMs, now)
			}
			return
		}

		reqNetBp := rt.ExitNetBp
		if holdS >= 180.0 && holdS < rt.MaxHoldSec {
			reqNetBp = math.Max(1.0, rt.ExitNetBp-1.0)
		} else if holdS >= rt.MaxHoldSec {
			reqNetBp = 0.30
		}

		if combMakerNetBp >= reqNetBp && math.Abs(jumpBps) <= 2.0 {
			if oid, okE := placePeerMakerExit(rt.PeerVenue, peerSym, closePeerSide, rt.HedgedQty, exitMakerPx, qDec, pxDec); okE {
				liveMu.Lock()
				acct.MakerPlaced++
				rt.State = "PEER_EXIT_OPEN"
				rt.ExitPeerOid = oid
				rt.ExitPeerPx = exitMakerPx
				rt.ExitPlaced = now
				rt.LastOrdPoll = time.Time{}
				liveMu.Unlock()
			}
		}
		return
	}

	// -------------------------------------------------------------------------
	// STATE 3: PEER_EXIT_OPEN (0%-Fee GTX Maker Exit Resting on BN_USDC / Aster)
	// -------------------------------------------------------------------------
	if rt.State == "PEER_EXIT_OPEN" {
		st := mktStates[rt.Sym]
		if st == nil {
			return
		}
		st.mu.Lock()
		lb, la := st.ltBid, st.ltAsk
		st.mu.Unlock()
		peerSym, qDec, _, _, _, _, mFeeBp, _, pBid, pAsk, _, _, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
		if !okP {
			peerSym = rt.PeerSym
			fx = rt.EntryFx
		}

		var status string
		var avgP float64
		var polled, okQ bool
		if rt.PeerVenue == "BN_USDC" {
			status, _, avgP, polled, okQ = queryBnOrder(peerSym, rt.ExitPeerOid, rt.LastOrdPoll, false)
		} else {
			status, _, avgP, polled, okQ = queryAsterOrder(peerSym, rt.ExitPeerOid, rt.LastOrdPoll, false)
		}
		if polled {
			rt.LastOrdPoll = now
		}
		if !okQ {
			return
		}
		if avgP <= 0 {
			avgP = rt.ExitPeerPx
		}
		if status == "FILLED" {
			finalizeSlotClose(rt, avgP, mFeeBp, "MAKER_GTX_EXIT", "ZERO_FEE_4LEG_TP", now.Sub(rt.ExitPlaced).Milliseconds(), now)
			return
		}
		if status == "CANCELED" || status == "EXPIRED" {
			rt.State = "HEDGED"
			rt.ExitPeerOid = 0
			return
		}

		// Check if Lighter leg moved adversely while Peer Maker exit is resting:
		rtMakerFeeBp := rt.EntryPeerFee + mFeeBp
		curNetBp := (((rt.ExitPeerPx-rt.EntryPeerPx)*fx)+(rt.EntryLtPx-la))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
		if rt.PeerSide == "SELL" {
			curNetBp = (((rt.EntryPeerPx-rt.ExitPeerPx)*fx)+(lb-rt.EntryLtPx))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
		}
		minKeep := 0.80
		if now.Sub(rt.OpenedAt).Seconds() >= rt.MaxHoldSec {
			minKeep = -0.20
		}
		if halted || curNetBp < minKeep || now.Sub(rt.ExitPlaced) >= 20*time.Second {
			cSt, cQty, cPx := cancelPeerOrder(rt.PeerVenue, peerSym, rt.ExitPeerOid)
			liveMu.Lock()
			acct.MakerCancelled++
			liveMu.Unlock()
			if cPx <= 0 {
				cPx = avgP
			}
			m := markets[rt.Sym]
			if cSt == "FILLED" || cQty >= rt.HedgedQty-m.MinB*0.5 {
				finalizeSlotClose(rt, cPx, mFeeBp, "MAKER_GTX_EXIT", "ZERO_FEE_4LEG_TP", now.Sub(rt.ExitPlaced).Milliseconds(), now)
				return
			}
			if cQty >= m.MinB && cQty < rt.HedgedQty-m.MinB*0.5 {
				// Partial Maker exit filled -> close matching slice on Lighter and keep remaining hedged!
				partPos := -cQty
				if rt.LtSide == "BID" {
					partPos = cQty
				}
				_, _ = closeLtLegGuaranteedSync(rt.Sym, partPos, lb, la)
				rt.HedgedQty = roundTo(rt.HedgedQty-cQty, qDec)
			}
			if pBid > 0 && pAsk > 0 {
				_ = pBid
			}
			rt.State = "HEDGED"
			rt.ExitPeerOid = 0
		}
		return
	}

	// -------------------------------------------------------------------------
	// STATE 0: IDLE (Apply Pending Rotation Safely ONLY When 100% Flat & IDLE!)
	// -------------------------------------------------------------------------
	liveMu.Lock()
	if len(rt.PendingSyms) > 0 && rt.State == "IDLE" && rt.HedgedQty == 0 && rt.Sym == "" {
		rt.PeerVenue = rt.PendingVenue
		rt.Symbols = append([]string(nil), rt.PendingSyms...)
		rt.PendingVenue = ""
		rt.PendingSyms = nil
		fmt.Printf("[IDLE_SAFE_ROTATE strat=%s newVenue=%s syms=%v note=%s]\n",
			rt.StratID, rt.PeerVenue, rt.Symbols, rt.RotatedNote)
	}
	liveMu.Unlock()

	if halted || now.Before(rt.CoolUntil) || now.UnixMilli() < quotaCoolUntil.Load() {
		return
	}

	// Verify venue available margin before quoting:
	if rt.PeerVenue == "BN_USDC" {
		dnMu.Lock()
		avail := dnSharedAvail
		dnMu.Unlock()
		if avail < 14.0 {
			return
		}
	} else {
		asterMu.Lock()
		avail := asterAvail
		asterMu.Unlock()
		if avail < 10.0 {
			return
		}
	}

	liveMu.Lock()
	canQuote := canSendLtTxLocked(now.UnixMilli(), false)
	liveMu.Unlock()
	if !canQuote {
		return
	}

	bestSym, bestPeerSym, bestLtSide, bestPeerSide := "", "", "", ""
	bestDisloc, bestLtPx, bestQty := -999.0, 0.0, 0.0

	for _, sym := range rt.Symbols {
		switch sym {
		case "PRL", "BB", "ARM", "OPENAI", "ZEC":
			continue
		}
		m, okM := markets[sym]
		st := mktStates[sym]
		if !okM || st == nil {
			continue
		}
		st.mu.Lock()
		lb, la, ltPos, lTS := st.ltBid, st.ltAsk, st.pos, st.lastBookTS
		st.mu.Unlock()
		if math.Abs(ltPos) >= m.MinB || lb <= 0 || la <= lb || now.Sub(lTS) > 8*time.Second {
			continue
		}
		ltSp := (la - lb) / lb * 10000.0
		if ltSp > 12.0 {
			continue
		}

		peerSym, qDec, _, step, _, minNot, _, tFeeBp, pBid, pAsk, emaBps, jumpBps, fx, warmTicks, bookAge, okP := getPeerSpecAndBook(rt.PeerVenue, sym, now)
		if !okP || warmTicks < 8 || bookAge > 8*time.Second || math.Abs(emaBps) > 35.0 || math.Abs(jumpBps) > 1.8 {
			continue
		}
		pSp := (pAsk - pBid) / pBid * 10000.0
		if pSp > 8.0 {
			continue
		}

		pBidUsd := pBid * fx
		pAskUsd := pAsk * fx
		ltMid := 0.5 * (lb + la)

		commonStep := math.Max(step, math.Pow10(-m.SzDec))
		qtyDec := qDec
		if m.SzDec < qtyDec {
			qtyDec = m.SzDec
		}
		qty := roundTo(math.Floor((rt.ClipUsd/ltMid)/commonStep+1e-9)*commonStep, qtyDec)
		if qty*ltMid < 10.5 && (qty+commonStep)*ltMid <= rt.ClipUsd+3.5 {
			qty = roundTo(qty+commonStep, qtyDec)
		}
		if qty < m.MinB || qty*ltMid < math.Max(10.5, minNot) {
			continue
		}

		// Candidate A: SELL PostOnly Maker on Lighter @ la, BUY Taker Hedge on Peer @ pAsk
		rawShortBp := ((la - pAskUsd) / pAskUsd) * 10000.0
		shortDislocBp := rawShortBp - emaBps
		if shortDislocBp >= rt.EntryDisloc && rawShortBp >= tFeeBp+1.0 && shortDislocBp > bestDisloc {
			bestDisloc = shortDislocBp
			bestSym, bestPeerSym = sym, peerSym
			bestLtSide, bestPeerSide = "ASK", "BUY"
			bestLtPx, bestQty = la, qty
		}

		// Candidate B: BUY PostOnly Maker on Lighter @ lb, SELL Taker Hedge on Peer @ pBid
		rawLongBp := ((pBidUsd - lb) / pBidUsd) * 10000.0
		longDislocBp := rawLongBp + emaBps
		if longDislocBp >= rt.EntryDisloc && rawLongBp >= tFeeBp+1.0 && longDislocBp > bestDisloc {
			bestDisloc = longDislocBp
			bestSym, bestPeerSym = sym, peerSym
			bestLtSide, bestPeerSide = "BID", "SELL"
			bestLtPx, bestQty = lb, qty
		}
	}

	if bestSym == "" || bestQty <= 0 {
		return
	}

	coi, placed := placeLtPostOnlyMakerSync(bestSym, bestLtSide, bestLtPx, bestQty)
	if !placed {
		rt.CoolUntil = now.Add(2 * time.Second)
		return
	}

	liveMu.Lock()
	recordLtTxLocked(now.UnixMilli())
	acct.MakerPlaced++
	rt.State = "LT_ENTRY_OPEN"
	rt.Sym = bestSym
	rt.PeerSym = bestPeerSym
	rt.LtSide = bestLtSide
	rt.PeerSide = bestPeerSide
	rt.LtCOI = coi
	rt.LtLimitPx = bestLtPx
	rt.LtOrderQty = bestQty
	rt.PlacedAt = now
	rt.LastOrdPoll = now
	rt.CancelSent = false
	liveMu.Unlock()

	fmt.Printf("[LIVE_QUOTE strat=%s sym=%s lt=%s qty=%.4f @ %.5f peer=%s disloc=%+.2fbp]\n",
		rt.StratID, bestSym, bestLtSide, bestQty, bestLtPx, bestPeerSym, bestDisloc)
}

func writeLiveArenaTelemetry(now time.Time) {
	nowMs := now.UnixMilli()
	upSec := math.Max(1.0, float64(nowMs-liveBootMs)/1000.0)
	upHr := upSec / 3600.0
	denomHr := math.Max(upHr, 0.05) // 3-min floor to prevent early-second extrapolation spikes

	ltEq := float64(eqNow.Load()) / 1e6
	dnMu.Lock()
	bnMargin := dnSharedAvail
	dnMu.Unlock()
	asterMu.Lock()
	asMargin := asterWallet.MarginBalance
	asterMu.Unlock()

	liveMu.Lock()
	txLastMin := countLtTxLastMinLocked(nowMs)
	outStrats := make([]*LiveStratAccount, 0, len(liveStratOrder))
	combinedNet := 0.0
	bestID := ""
	bestHr := -1e9
	bestScaled489 := 0.0

	for _, id := range liveStratOrder {
		acct := liveStrats[id]
		rt := liveSlots[id]
		if acct == nil || rt == nil {
			continue
		}

		upnl := 0.0
		snaps := []LivePosView{}
		activeMakers := 0
		if rt.State == "LT_ENTRY_OPEN" || rt.State == "PEER_EXIT_OPEN" {
			activeMakers = 1
		}
		if (rt.State == "HEDGED" || rt.State == "PEER_EXIT_OPEN") && rt.HedgedQty > 0 {
			if st := mktStates[rt.Sym]; st != nil {
				st.mu.Lock()
				lb, la := st.ltBid, st.ltAsk
				st.mu.Unlock()
				ltQtySigned := rt.HedgedQty
				ltMark := lb
				if rt.LtSide == "ASK" {
					ltQtySigned = -rt.HedgedQty
					ltMark = la
				}
				if ltMark > 0 {
					if ltQtySigned > 0 {
						upnl += rt.HedgedQty * (ltMark - rt.EntryLtPx)
					} else {
						upnl += rt.HedgedQty * (rt.EntryLtPx - ltMark)
					}
				}
				snaps = append(snaps, LivePosView{
					Venue:     "LT_STD",
					Symbol:    rt.Sym,
					Qty:       ltQtySigned,
					AvgPx:     rt.EntryLtPx,
					OpenedMs:  rt.OpenedAt.UnixMilli(),
					UpdatedMs: nowMs,
					Tag:       rt.State,
				})
			}
			_, _, _, _, _, _, _, _, pBid, pAsk, _, _, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
			peerQtySigned := rt.HedgedQty
			pMark := pBid
			if rt.PeerSide == "SELL" {
				peerQtySigned = -rt.HedgedQty
				pMark = pAsk
			}
			if okP && pMark > 0 {
				if peerQtySigned > 0 {
					upnl += rt.HedgedQty * (pMark - rt.EntryPeerPx) * fx
				} else {
					upnl += rt.HedgedQty * (rt.EntryPeerPx - pMark) * fx
				}
			}
			snaps = append(snaps, LivePosView{
				Venue:     rt.PeerVenue,
				Symbol:    rt.PeerSym,
				Qty:       peerQtySigned,
				AvgPx:     rt.EntryPeerPx,
				OpenedMs:  rt.OpenedAt.UnixMilli(),
				UpdatedMs: nowMs,
				Tag:       rt.State,
			})
		}

		acct.UnrealizedPnL = roundTo(upnl, 4)
		acct.Cash = roundTo(acct.StartCash+acct.RealizedGross-acct.FeesPaid+acct.FundingUsd, 4)
		acct.Equity = roundTo(acct.Cash+upnl, 4)
		if acct.Equity > acct.PeakEquity {
			acct.PeakEquity = acct.Equity
		}
		dd := acct.PeakEquity - acct.Equity
		if dd > acct.MaxDrawdownUsd {
			acct.MaxDrawdownUsd = roundTo(dd, 4)
		}

		netStrat := acct.Equity - acct.StartCash
		realStrat := acct.Cash - acct.StartCash
		combinedNet += netStrat

		// Per-strategy -$1.50 circuit breaker:
		if !acct.Halted && netStrat <= StratStopLossUSD {
			acct.Halted = true
			acct.HaltReason = fmt.Sprintf("Strategy SL hit ($%+.2f <= $%.2f)", netStrat, StratStopLossUSD)
			fmt.Printf("[STRAT_CIRCUIT_BREAKER id=%s net=$%+.4f] Halting strategy!\n", id, netStrat)
		}

		if len(acct.EquityCurve) == 0 || nowMs-acct.EquityCurve[len(acct.EquityCurve)-1].TsMs >= 30000 {
			acct.EquityCurve = append(acct.EquityCurve, LiveEqPoint{TsMs: nowMs, Cash: acct.Cash, Eq: acct.Equity})
			if len(acct.EquityCurve) > 540 { // 4.5h rolling history
				acct.EquityCurve = append([]LiveEqPoint(nil), acct.EquityCurve[len(acct.EquityCurve)-540:]...)
			}
		}

		// Compute 4h rolling realized $/hr (signal-clean: ignores temporary hedged mark-to-market noise!):
		acct.HourlyRunRate = roundTo(netStrat/denomHr, 3)
		acct.RealizedHourly = roundTo(realStrat/denomHr, 3)
		roll4h := acct.RealizedHourly
		targetTs := nowMs - 4*3600*1000
		if len(acct.EquityCurve) >= 2 {
			refPt := acct.EquityCurve[0]
			for _, pt := range acct.EquityCurve {
				if pt.TsMs <= targetTs {
					refPt = pt
				} else {
					break
				}
			}
			dtHr := float64(nowMs-refPt.TsMs) / 3600000.0
			if dtHr >= 0.25 {
				roll4h = roundTo((acct.Cash-refPt.Cash)/dtHr, 3)
			}
		}
		acct.Rolling4hHourly = roll4h
		// Rank & scale on Closed Realized 4h $/hr once >= 1 closed trip so open spread wicks never distort signal!
		sigHr := roll4h
		if acct.RoundTrips == 0 {
			sigHr = acct.HourlyRunRate
		}
		acct.Scaled489Hourly = roundTo(sigHr*rt.ScaleMult489, 2)
		acct.ActiveMakers = activeMakers
		acct.PosSnap = snaps
		acct.State = rt.State
		acct.ActiveSym = rt.Sym
		acct.RotatedNote = rt.RotatedNote
		if acct.Halted {
			acct.StatusNote = "HALTED: " + acct.HaltReason
		} else {
			acct.StatusNote = fmt.Sprintf("State:%s %s | Closed $%+.3f | 4h:$%+.2f/hr (@$489: $%+.2f/hr)",
				rt.State, rt.Sym, realStrat, roll4h, acct.Scaled489Hourly)
		}

		if sigHr > bestHr {
			bestHr = sigHr
			bestID = id
			bestScaled489 = acct.Scaled489Hourly
		}
		cp := *acct
		outStrats = append(outStrats, &cp)
	}

	revFills := make([]LiveFillAudit, len(liveFills))
	for i := range liveFills {
		revFills[i] = liveFills[len(liveFills)-1-i]
	}
	liveMu.Unlock()

	// Global -$4.00 combined circuit breaker across all 4 strategies:
	if combinedNet <= GlobalStopLossUSD && breakerTripped.CompareAndSwap(false, true) {
		go flattenAndExitPause(fmt.Sprintf("STOP-LOSS Global Micro-Arena PnL=$%+.2f <= $%.2f", combinedNet, GlobalStopLossUSD))
	}

	// Build live dislocation radar across all 4 strategy symbol sets:
	spreads := make([]LiveSpreadSnap, 0, 26)
	for _, id := range liveStratOrder {
		rt := liveSlots[id]
		if rt == nil {
			continue
		}
		for _, sym := range rt.Symbols {
			st := mktStates[sym]
			if st == nil {
				continue
			}
			st.mu.Lock()
			lb, la := st.ltBid, st.ltAsk
			st.mu.Unlock()
			if lb <= 0 || la <= lb {
				continue
			}
			_, _, _, _, _, _, _, _, pBid, pAsk, emaBps, _, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, sym, now)
			if !okP || pBid <= 0 || pAsk <= pBid {
				continue
			}
			ltMid := 0.5 * (lb + la)
			ltSp := (la - lb) / ltMid * 10000.0
			pSp := (pAsk - pBid) / (0.5 * (pBid + pAsk)) * 10000.0
			pMidUsd := 0.5 * (pBid + pAsk) * fx
			basis := (ltMid - pMidUsd) / pMidUsd * 10000.0
			dislocShort := (((la - pAsk*fx) / (pAsk * fx)) * 10000.0) - emaBps
			dislocLong := (((pBid*fx - lb) / (pBid * fx)) * 10000.0) + emaBps
			bestD := math.Max(dislocShort, dislocLong)
			spreads = append(spreads, LiveSpreadSnap{
				Symbol:     sym,
				StratID:    strings.TrimPrefix(id, "LIVE_"),
				LtBid:      lb,
				LtAsk:      la,
				LtSpreadBp: roundTo(ltSp, 2),
				PeerVenue:  rt.PeerVenue,
				PeerBid:    pBid,
				PeerAsk:    pAsk,
				PeerSpdBp:  roundTo(pSp, 2),
				BasisBp:    roundTo(basis, 2),
				EmaBp:      roundTo(emaBps, 2),
				DislocBp:   roundTo(bestD, 2),
			})
		}
	}

	liveMu.Lock()
	slotSnaps := make([]SavedSlotSnap, 0, len(liveStratOrder))
	for _, id := range liveStratOrder {
		if rt := liveSlots[id]; rt != nil {
			slotSnaps = append(slotSnaps, SavedSlotSnap{
				StratID:      rt.StratID,
				State:        rt.State,
				Sym:          rt.Sym,
				PeerVenue:    rt.PeerVenue,
				PeerSym:      rt.PeerSym,
				LtSide:       rt.LtSide,
				PeerSide:     rt.PeerSide,
				HedgedQty:    rt.HedgedQty,
				EntryLtPx:    rt.EntryLtPx,
				EntryPeerPx:  rt.EntryPeerPx,
				EntryFx:      rt.EntryFx,
				EntryPeerFee: rt.EntryPeerFee,
				OpenedMs:     rt.OpenedAt.UnixMilli(),
				TradeID:      rt.TradeID,
				ExitPeerOid:  rt.ExitPeerOid,
				ExitPeerPx:   rt.ExitPeerPx,
				RotatedNote:  rt.RotatedNote,
			})
		}
	}
	liveMu.Unlock()

	tel := LiveArenaTelemetry{
		Version:          LiveArenaVer,
		BootTsMs:         liveBootMs,
		NowTsMs:          nowMs,
		UptimeSec:        roundTo(upSec, 1),
		UptimeHours:      roundTo(upHr, 3),
		Paused:           remotePaused.Load(),
		LtTxLastMin:      txLastMin,
		TotalCapitalUsd:  roundTo(ltEq+bnMargin+asMargin, 2),
		LighterEqUsd:     roundTo(ltEq, 2),
		BinanceMarginUsd: roundTo(bnMargin, 2),
		AsterMarginUsd:   roundTo(asMargin, 2),
		CombinedNetUsd:   roundTo(combinedNet, 4),
		CombinedHourly:   roundTo(combinedNet/denomHr, 3),
		Scaled489BestHr:  bestScaled489,
		BestStratID:      bestID,
		BestHourlyUsd:    roundTo(bestHr, 3),
		Strategies:       outStrats,
		RecentFills:      revFills,
		MarketSpreads:    spreads,
		SlotSnapshots:    slotSnaps,
	}

	if raw, err := json.Marshal(tel); err == nil {
		_ = os.WriteFile(LiveArenaOutPath+".tmp", raw, 0644)
		_ = os.Rename(LiveArenaOutPath+".tmp", LiveArenaOutPath)
		if nowMs-lastLiveSaveMs >= 10000 {
			lastLiveSaveMs = nowMs
			_ = os.WriteFile(LiveArenaSavePath+".tmp", raw, 0644)
			_ = os.Rename(LiveArenaSavePath+".tmp", LiveArenaSavePath)
		}
	}
}

func evaluateForwardArenaIdleSafeRotation() {
	raw, err := os.ReadFile("/tmp/forward_arena.json")
	if err != nil || len(raw) < 50 {
		return
	}
	var fa struct {
		UptimeHours float64 `json:"uptimeHours"`
		Strategies  []struct {
			ID              string  `json:"id"`
			HourlyRunRate   float64 `json:"hourlyRunRate"`
			Rolling4hHourly float64 `json:"rolling4hHourly"`
		} `json:"strategies"`
	}
	if json.Unmarshal(raw, &fa) != nil || fa.UptimeHours < 1.0 {
		return
	}
	hrMap := map[string]float64{}
	for _, s := range fa.Strategies {
		// Use the 4-hour rolling $/hr directly so weekday/weekend & US-open shifts rotate automatically!
		r := s.Rolling4hHourly
		if r == 0 && s.HourlyRunRate != 0 {
			r = s.HourlyRunRate
		}
		hrMap[s.ID] = r
	}
	s4Hr := hrMap["S4_ASTER_LT_BN_RWA_15X"]
	s5Hr := hrMap["S5_LT_STD_MARGIN_STRICT"]
	s3Hr := hrMap["S3_LT_ASTER_CRYPTO_ARB"]
	topCryptoHr := math.Max(s5Hr, s3Hr)

	liveMu.Lock()
	defer liveMu.Unlock()
	rt4 := liveSlots["LIVE_S4_ASTER_LT_RWA"]
	if rt4 == nil {
		return
	}
	// Never swap mid-position! Queue into PendingSyms so it only applies when State == "IDLE"!
	if s4Hr < 4.50 && topCryptoHr >= 6.50 {
		wantSyms := []string{"KAITO", "CRV", "ETHFI", "WLFI"} // strictly disjoint from S5/S3/S2!
		if rt4.PeerVenue != "BN_USDC" && len(rt4.PendingSyms) == 0 {
			rt4.PendingVenue = "BN_USDC"
			rt4.PendingSyms = wantSyms
			rt4.RotatedNote = fmt.Sprintf("4h Rolling Auto-Swap: RWA $%.2f/hr -> Crypto Boost (waits for IDLE)", s4Hr)
		}
	} else if s4Hr >= 5.00 && rt4.PeerVenue != rt4.OrigPeerVenue && len(rt4.PendingSyms) == 0 {
		rt4.PendingVenue = rt4.OrigPeerVenue
		rt4.PendingSyms = append([]string(nil), rt4.OrigSymbols...)
		rt4.RotatedNote = fmt.Sprintf("4h Rolling Auto-Swap: Restored RWA Equities ($%.2f/hr >= $5.00)", s4Hr)
	}
}

func runLiveMicroArenaLoop() {
	initLiveMicroArena()
	time.Sleep(4 * time.Second)
	if remotePaused.Load() || !setupBnUsdcMargin() {
		return
	}
	_, _, _ = fetchAsterAccountMargin()
	emitBnAuditLine()
	evaluateForwardArenaIdleSafeRotation()
	writeLiveArenaTelemetry(time.Now())

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	lastMarginPoll := time.Now()
	lastRotCheck := time.Now()
	lastTelWrite := time.Time{}

	for {
		select {
		case <-engineWakeCh:
		case <-tick.C:
		}
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now()
		if now.Sub(lastMarginPoll) >= 4*time.Second {
			lastMarginPoll = now
			go func() {
				_, _, _, _ = fetchBnAccountMargin()
				_, _, _ = fetchAsterAccountMargin()
			}()
		}
		if now.Sub(lastRotCheck) >= 60*time.Second {
			lastRotCheck = now
			go evaluateForwardArenaIdleSafeRotation()
		}
		for _, id := range liveStratOrder {
			if remotePaused.Load() || breakerTripped.Load() {
				return
			}
			if rt := liveSlots[id]; rt != nil {
				stepLiveSlot(rt, now)
			}
		}
		if now.Sub(lastTelWrite) >= 1000*time.Millisecond {
			lastTelWrite = now
			writeLiveArenaTelemetry(now)
		}
	}
}
