package main

import (
	"encoding/json"
	"fmt"
	"io"
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
	LiveArenaVer         = "LIVE-v109.0-SIMPLE"
	Baseline3VenueUSD    = 476.2300 // Exact 100%-flat 3-venue Perp cash ($203.14 LT + $225.49 BN + $47.60 AS)
	MaxDemoteDrawdownUSD = 8.00     // -$8.00 strategy MaxDD demotes back to Tier 0 ($0)
	GlobalStopLossUSD    = -8.00    // -$8.00 real 3-venue equity stop flattens & pauses
	MaxLtTxPerMin        = 50       // Guard below #770015 Standard 60 tx/min quota
	MaxLiveSlots         = 3        // Up to 3 simultaneous hedged LT <-> BN_USDC pairs (at most 1 resting LT Maker order!)
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
	TsMs  int64   `json:"ts"`
	Cash  float64 `json:"cash"`
	Eq    float64 `json:"eq"`
	MidEq float64 `json:"midEq,omitempty"`
}

type LiveStratAccount struct {
	ID              string        `json:"id"`
	ForwardStratID  string        `json:"forwardStratId"`
	SecondStratID   string        `json:"secondStratId,omitempty"`
	Name            string        `json:"name"`
	Desc            string        `json:"desc"`
	VenueDesc       string        `json:"venueDesc"`
	Tier            int           `json:"tier"` // 0=$0, 1=1x$20, 2=2x$45, 3=3x$75
	TierLabel       string        `json:"tierLabel"`
	TierChangedMs   int64         `json:"tierChangedMs"`
	StratActiveMs   int64         `json:"stratActiveMs"`
	ActiveElapsedS  float64       `json:"activeElapsedSec"`
	TierElapsedS    float64       `json:"tierElapsedSec"`
	TierBaseTrips   int64         `json:"tierBaseTrips"`
	ClipUsd         float64       `json:"clipUsd"`
	Tier3ClipUsd    float64       `json:"tier3ClipUsd"`
	StartCash       float64       `json:"startCash"`
	Cash            float64       `json:"cash"`
	ClosedNetUsd    float64       `json:"closedNetUsd"`
	WithdrawableUsd float64       `json:"withdrawableUsd"`
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
	Forward45mHr    float64       `json:"forward45mHourly"`
	ForwardPF       float64       `json:"forwardProfitFactor"`
	ForwardTrips    int64         `json:"forwardTrips"`
	MakerPlaced     int64         `json:"makerPlaced"`
	MakerCancelled  int64         `json:"makerCancelled"`
	MakerFills      int64         `json:"makerFills"`
	TakerFills      int64         `json:"takerFills"`
	RoundTrips      int64         `json:"roundTrips"`
	WinTrips        int64         `json:"winTrips"`
	GrossWinUsd     float64       `json:"grossWinUsd"`
	GrossLossUsd    float64       `json:"grossLossUsd"`
	ProfitFactor    float64       `json:"profitFactor"`
	RecentTripPnls  []float64     `json:"recentTripPnls,omitempty"`
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

type SavedSlotSnap struct {
	SlotKey      string  `json:"slotKey,omitempty"`
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
	EntryFeeUsd  float64 `json:"entryFeeUsd,omitempty"`
	OpenedMs     int64   `json:"openedMs"`
	TradeID      int64   `json:"tradeId"`
	ExitPeerOid  int64   `json:"exitPeerOid"`
	ExitPeerPx   float64 `json:"exitPeerPx"`
	RotatedNote  string  `json:"rotatedNote"`
}

type LiveArenaTelemetry struct {
	Version             string              `json:"version"`
	BootTsMs            int64               `json:"bootTsMs"`
	NowTsMs             int64               `json:"nowTsMs"`
	UptimeSec           float64             `json:"uptimeSec"`
	UptimeHours         float64             `json:"uptimeHours"`
	Paused              bool                `json:"paused"`
	LtTxLastMin         int                 `json:"ltTxLastMin"`
	TotalCapitalUsd     float64             `json:"totalCapitalUsd"`
	LighterEqUsd        float64             `json:"lighterEqUsd"`
	BinanceMarginUsd    float64             `json:"binanceMarginUsd"`
	AsterMarginUsd      float64             `json:"asterMarginUsd"`
	Real3VenueWalletUsd float64             `json:"real3VenueWalletUsd"`
	Real3VenueEquityUsd float64             `json:"real3VenueEquityUsd"`
	RealWalletDeltaUsd  float64             `json:"realWalletDeltaUsd"`
	RealEquityDeltaUsd  float64             `json:"realEquityDeltaUsd"`
	CombinedNetUsd      float64             `json:"combinedNetUsd"`
	CombinedHourly      float64             `json:"combinedHourlyUsd"`
	Scaled489BestHr     float64             `json:"scaled489BestHourlyUsd"`
	BestStratID         string              `json:"bestStratId"`
	BestHourlyUsd       float64             `json:"bestHourlyUsd"`
	Strategies          []*LiveStratAccount `json:"strategies"`
	RecentFills         []LiveFillAudit     `json:"recentFills"`
	MarketSpreads       []LiveSpreadSnap    `json:"marketSpreads"`
	SlotSnapshots       []SavedSlotSnap     `json:"slotSnapshots,omitempty"`
}

type StratPreset struct {
	ForwardID    string
	Label        string
	MinRawEdgeBp float64
	EntryDisloc  float64
	TakerGrabBp  float64
	ExitNetBp    float64
	DecaySec     float64
	MaxHoldSec   float64
	Symbols      []string
}

type SimpleSlot struct {
	Idx           int
	State         string // IDLE | LT_ENTRY_OPEN | HEDGED | PEER_EXIT_OPEN
	Sym           string
	PeerSym       string
	LtSide        string // BID | ASK
	PeerSide      string // BUY | SELL
	LtCOI         int64
	LtLimitPx     float64
	PlacedAt      time.Time
	CancelSent    bool
	CancelAt      time.Time
	HedgedQty     float64
	EntryLtPx     float64
	EntryPeerPx   float64
	EntryFx       float64
	EntryFeeUsd   float64
	OpenedAt      time.Time
	TradeID       int64
	ExitPeerOid   int64
	ExitPeerPx    float64
	ExitPlaced    time.Time
	LastOrdPoll   time.Time
	LastLtRestBBO time.Time
	CoolUntil     time.Time
}

var (
	liveMu                sync.Mutex
	liveBootMs            = time.Now().UnixMilli()
	liveAcct              *LiveStratAccount
	liveSlots             [MaxLiveSlots]*SimpleSlot
	liveFills             = []LiveFillAudit{}
	ltTxTimesMs           = []int64{}
	lastLiveSaveMs        int64
	lastPresetSwitchMs    int64
	activePreset          StratPreset
	secondPreset          StratPreset
	dynamic3VenueBaseline = Baseline3VenueUSD
	liveWakeCh            = make(chan struct{}, 1)
	liveInitDone          atomic.Bool
	bnOrphanSeen          = map[string]int{}

	bnPresets = map[string]StratPreset{
		"S7_BN_HIGH_CONV_RAW": {
			ForwardID: "S7_BN_HIGH_CONV_RAW", Label: "S7 High-Conviction (>=8.5bp Maker)",
			MinRawEdgeBp: 4.5, EntryDisloc: 8.5, TakerGrabBp: 0, ExitNetBp: 3.5, DecaySec: 420, MaxHoldSec: 1200,
			Symbols: []string{"ETH", "SOL", "BNB", "UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "LINK", "DOGE", "XRP"},
		},
		"S2_LT_BN_FAST_ROTATE": {
			ForwardID: "S2_LT_BN_FAST_ROTATE", Label: "S2 Fast-Rotate (>=6.5bp Maker)",
			MinRawEdgeBp: 2.5, EntryDisloc: 6.5, TakerGrabBp: 0, ExitNetBp: 2.5, DecaySec: 300, MaxHoldSec: 900,
			Symbols: []string{"ETH", "SOL", "BNB", "UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "TRUMP", "TIA", "CRV", "ETHFI", "LINK", "DOGE", "XRP"},
		},
		"E3_MAJORS_FAST_TURN": {
			ForwardID: "E3_MAJORS_FAST_TURN", Label: "E3 Majors Fast-Turn (90s Step-Down)",
			MinRawEdgeBp: 2.0, EntryDisloc: 6.0, TakerGrabBp: 0, ExitNetBp: 2.0, DecaySec: 90, MaxHoldSec: 360,
			Symbols: []string{"BTC", "ETH", "SOL", "BNB", "HYPE", "XRP", "AAVE", "NEAR", "ENA", "SUI", "UNI"},
		},
		"S9_PURE_CROSSED_TAKER": {
			ForwardID: "S9_PURE_CROSSED_TAKER", Label: "S9 Pure Crossed-Book Grab (>=8.5bp IOC)",
			MinRawEdgeBp: 4.5, EntryDisloc: 8.5, TakerGrabBp: 8.5, ExitNetBp: 2.5, DecaySec: 300, MaxHoldSec: 1200,
			Symbols: []string{"ETH", "SOL", "BNB", "UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "TRUMP", "TIA", "CRV", "ETHFI", "LINK", "DOGE", "XRP"},
		},
	}
)

// ---------- Quota, Tiers & Initialization ----------

func canSendLtTxLocked(nowMs int64, isUrgent bool) bool {
	cut := nowMs - 60000
	kept := ltTxTimesMs[:0]
	for _, t := range ltTxTimesMs {
		if t >= cut {
			kept = append(kept, t)
		}
	}
	ltTxTimesMs = kept
	if isUrgent {
		return len(ltTxTimesMs) < 57
	}
	return len(ltTxTimesMs) < MaxLtTxPerMin
}

func recordLtTxLocked(nowMs int64) { ltTxTimesMs = append(ltTxTimesMs, nowMs) }

func countLtTxLastMinLocked(nowMs int64) int {
	cut := nowMs - 60000
	n := 0
	for _, t := range ltTxTimesMs {
		if t >= cut {
			n++
		}
	}
	return n
}

func wakeAllLiveSlots() {
	if !liveInitDone.Load() {
		return
	}
	select {
	case liveWakeCh <- struct{}{}:
	default:
	}
}

func tierSpec(tier int) (slots int, clip float64, label string) {
	switch tier {
	case 1:
		return 1, 20.0, "Tier 1: Micro (1x $20)"
	case 2:
		return 2, 45.0, "Tier 2: Medium (2x $45)"
	case 3:
		return 3, 75.0, "Tier 3: Full Safe (3x $75)"
	default:
		return 0, 0.0, "Tier 0: Forward Only ($0)"
	}
}

func applyTierLocked() {
	if liveAcct == nil {
		return
	}
	_, clip, lbl := tierSpec(liveAcct.Tier)
	liveAcct.ClipUsd = clip
	liveAcct.TierLabel = lbl
}

func initLiveMicroArena() {
	liveMu.Lock()
	if liveAcct != nil {
		liveMu.Unlock()
		return
	}
	nowMs := time.Now().UnixMilli()
	activePreset = bnPresets["S7_BN_HIGH_CONV_RAW"]
	liveAcct = &LiveStratAccount{
		ID:             activePreset.ForwardID,
		ForwardStratID: activePreset.ForwardID,
		Name:           activePreset.Label,
		Desc:           "Single-loop engine: auto-picks top 45m Forward Lab LT<->BN winner when idle and scales $20 -> $45 -> $75 (3 slots, -$8 stop).",
		VenueDesc:      "LT #770015 0% <-> BN USDC-M (4bp In / 0% Out)",
		Tier:           0,
		TierLabel:      "Tier 0: Forward Only ($0)",
		TierChangedMs:  nowMs,
		Tier3ClipUsd:   75.0,
		StartCash:      200.0,
		Cash:           200.0,
		Equity:         200.0,
		PeakEquity:     200.0,
		State:          "IDLE",
		RotatedNote:    activePreset.Label,
	}
	for i := 0; i < MaxLiveSlots; i++ {
		liveSlots[i] = &SimpleSlot{Idx: i, State: "IDLE"}
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
	if json.Unmarshal(raw, &saved) != nil || saved.BootTsMs <= 0 || !strings.HasPrefix(saved.Version, "LIVE-v109.") {
		return
	}
	liveMu.Lock()
	defer liveMu.Unlock()
	liveBootMs = saved.BootTsMs
	if len(saved.Strategies) > 0 && saved.Strategies[0] != nil {
		st := saved.Strategies[0]
		liveAcct.Tier = st.Tier
		liveAcct.TierChangedMs = st.TierChangedMs
		liveAcct.StratActiveMs = st.StratActiveMs
		liveAcct.TierBaseTrips = st.TierBaseTrips
		liveAcct.Cash = st.Cash
		liveAcct.RealizedGross = st.RealizedGross
		liveAcct.FeesPaid = st.FeesPaid
		liveAcct.FundingUsd = st.FundingUsd
		liveAcct.Equity = st.Cash
		liveAcct.PeakEquity = st.PeakEquity
		liveAcct.MaxDrawdownUsd = st.MaxDrawdownUsd
		liveAcct.MakerPlaced = st.MakerPlaced
		liveAcct.MakerCancelled = st.MakerCancelled
		liveAcct.MakerFills = st.MakerFills
		liveAcct.TakerFills = st.TakerFills
		liveAcct.RoundTrips = st.RoundTrips
		liveAcct.WinTrips = st.WinTrips
		liveAcct.GrossWinUsd = st.GrossWinUsd
		liveAcct.GrossLossUsd = st.GrossLossUsd
		liveAcct.ProfitFactor = st.ProfitFactor
		liveAcct.RecentTripPnls = st.RecentTripPnls
		liveAcct.GrossVolumeUsd = st.GrossVolumeUsd
		liveAcct.EquityCurve = st.EquityCurve
		liveAcct.Halted = st.Halted
		liveAcct.HaltReason = st.HaltReason
		if p, ok := bnPresets[st.ForwardStratID]; ok {
			activePreset = p
			liveAcct.ID = p.ForwardID
			liveAcct.ForwardStratID = p.ForwardID
			liveAcct.Name = p.Label
			liveAcct.RotatedNote = p.Label
		}
		applyTierLocked()
	}
	for i, sn := range saved.SlotSnapshots {
		if i >= MaxLiveSlots || (sn.State != "HEDGED" && sn.State != "PEER_EXIT_OPEN") || sn.HedgedQty <= 0 {
			continue
		}
		sl := liveSlots[i]
		sl.State = sn.State
		sl.Sym = sn.Sym
		sl.PeerSym = sn.PeerSym
		sl.LtSide = sn.LtSide
		sl.PeerSide = sn.PeerSide
		sl.HedgedQty = sn.HedgedQty
		sl.EntryLtPx = sn.EntryLtPx
		sl.EntryPeerPx = sn.EntryPeerPx
		sl.EntryFx = sn.EntryFx
		if sl.EntryFx <= 0 {
			sl.EntryFx = 1.0
		}
		sl.EntryFeeUsd = sn.EntryFeeUsd
		sl.OpenedAt = time.UnixMilli(sn.OpenedMs)
		sl.TradeID = sn.TradeID
		sl.ExitPeerOid = sn.ExitPeerOid
		sl.ExitPeerPx = sn.ExitPeerPx
		sl.ExitPlaced = time.Now()
	}
	for i := len(saved.RecentFills) - 1; i >= 0; i-- {
		liveFills = append(liveFills, saved.RecentFills[i])
	}
	fmt.Printf("[%s] Hot-reloaded single-loop state (tier=%d preset=%s)\n", LiveArenaVer, liveAcct.Tier, activePreset.ForwardID)
}

func hasAdoptedLivePositions() bool {
	liveMu.Lock()
	defer liveMu.Unlock()
	for _, sl := range liveSlots {
		if sl != nil && (sl.State == "HEDGED" || sl.State == "PEER_EXIT_OPEN") && sl.HedgedQty > 0 {
			return true
		}
	}
	return false
}

func clearAllLiveSlotsOnFlatten() {
	liveMu.Lock()
	defer liveMu.Unlock()
	for _, sl := range liveSlots {
		if sl != nil {
			*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE"}
		}
	}
}

func appendLiveFillLocked(f LiveFillAudit) {
	liveFills = append(liveFills, f)
	if len(liveFills) > 120 {
		liveFills = append([]LiveFillAudit(nil), liveFills[len(liveFills)-120:]...)
	}
}

// ---------- Exchange Execution Helpers (Lighter 0% + Binance USDC-M Only) ----------

func getBnUsdcBook(sym string, now time.Time) (bnSym string, qDec, pxDec int, step, minNot, bid, ask, emaBps, jumpBps, fx float64, warm int, age time.Duration, ok bool) {
	pSym := sym + "USDC"
	dnMu.Lock()
	sp, exists := dnByBnSym[pSym]
	db := dnBooks[pSym]
	fxVal, okFx := dnQuoteFxToUsdtLocked("USDC", now)
	blocked := dnBlockedSym[pSym]
	if db != nil {
		bid, ask, emaBps, jumpBps, warm = db.Bid, db.Ask, db.EmaBps, db.LastJumpBps, db.WarmTicks
		age = now.Sub(db.TS)
	}
	dnMu.Unlock()
	if !exists || blocked || !okFx || db == nil || bid <= 0 || ask <= bid {
		return "", 0, 0, 0, 0, 0, 0, 0, 0, 1.0, 0, 999 * time.Second, false
	}
	return pSym, sp.QDec, sp.PxDec, sp.Step, sp.MinNot, bid, ask, emaBps, jumpBps, fxVal, warm, age, true
}

func bnTopDepthUsd(bnSym, side string) float64 {
	dnMu.Lock()
	defer dnMu.Unlock()
	db := dnBooks[bnSym]
	if db == nil {
		return 0
	}
	if side == "BUY" {
		return db.Ask * db.AskQ
	}
	return db.Bid * db.BidQ
}

func placeLtPostOnlyMakerSync(sym, side string, px, qty float64) (int64, bool) {
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
	coi := coiSeq.Add(1)
	exp := time.Now().Add(10 * time.Minute).UnixMilli()
	err := sendSignedTx(fmt.Sprintf("po_%d", coi), fmt.Sprintf("LT_PO_%s_%s px=%.5f sz=%.5f", sym, side, pxF, qty), func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex: m.ID, ClientOrderIndex: coi, BaseAmount: baseInt, Price: pxInt,
			IsAsk: isAsk, Type: 0, TimeInForce: 2, ReduceOnly: 0, OrderExpiry: exp,
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

func fetchLtMarketOpenOrderCountREST(sym string) (int, bool) {
	m, ok := markets[sym]
	if !ok {
		return 0, false
	}
	snap, okS := fetchLtAccountCached(1500*time.Millisecond, 0)
	if !okS || snap == nil {
		return 0, false
	}
	for _, p := range snap.Positions {
		if p.MarketID == m.ID {
			return p.OpenOrderCount, true
		}
	}
	return 0, true
}

func executeBnMarketOrder(bnSym, side string, qty float64, qDec int, reduceOnly bool) (float64, float64, int64, bool) {
	t0 := time.Now()
	p := url.Values{}
	p.Set("symbol", bnSym)
	p.Set("side", side)
	p.Set("type", "MARKET")
	if reduceOnly {
		p.Set("reduceOnly", "true")
	}
	p.Set("quantity", strconv.FormatFloat(qty, 'f', qDec, 64))
	p.Set("newOrderRespType", "RESULT")
	sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
	if err != nil || sc != 200 {
		fmt.Printf("[BN_MKT_ERR sym=%s side=%s qty=%.5f sc=%d err=%v body=%s]\n", bnSym, side, qty, sc, err, string(body))
		return 0, 0, time.Since(t0).Milliseconds(), false
	}
	var or struct {
		OrderID int64  `json:"orderId"`
		Status  string `json:"status"`
		ExecQty string `json:"executedQty"`
		AvgPx   string `json:"avgPrice"`
	}
	if json.Unmarshal(body, &or) != nil {
		return 0, 0, time.Since(t0).Milliseconds(), false
	}
	fq, _ := strconv.ParseFloat(or.ExecQty, 64)
	ap, _ := strconv.ParseFloat(or.AvgPx, 64)
	if (fq <= 0 || ap <= 0) && or.OrderID > 0 {
		for _, w := range []time.Duration{120 * time.Millisecond, 300 * time.Millisecond, 600 * time.Millisecond} {
			time.Sleep(w)
			if st2, fq2, ap2, _, ok2 := queryBnOrder(bnSym, or.OrderID, time.Time{}, true); ok2 {
				if fq2 > fq {
					fq = fq2
				}
				if ap2 > 0 {
					ap = ap2
				}
				if (st2 == "FILLED" && fq > 0 && ap > 0) || st2 == "CANCELED" || st2 == "EXPIRED" {
					break
				}
			}
		}
	}
	return fq, ap, time.Since(t0).Milliseconds(), fq > 0
}

func placeBnMakerExit(bnSym, side string, qty, px float64, qDec, pxDec int) (int64, bool) {
	p := url.Values{}
	p.Set("symbol", bnSym)
	p.Set("side", side)
	p.Set("type", "LIMIT")
	p.Set("timeInForce", "GTX")
	p.Set("reduceOnly", "true")
	p.Set("quantity", strconv.FormatFloat(qty, 'f', qDec, 64))
	p.Set("price", strconv.FormatFloat(px, 'f', pxDec, 64))
	sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
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

func cancelBnOrder(bnSym string, oid int64) (string, float64, float64) {
	qp := url.Values{}
	qp.Set("symbol", bnSym)
	qp.Set("orderId", strconv.FormatInt(oid, 10))
	sc, body, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
	var co struct {
		Status  string `json:"status"`
		ExecQty string `json:"executedQty"`
		AvgPx   string `json:"avgPrice"`
	}
	_ = json.Unmarshal(body, &co)
	if sc != 200 {
		if st2, eq2, ap2, _, ok2 := queryBnOrder(bnSym, oid, time.Time{}, true); ok2 {
			return st2, eq2, ap2
		}
	}
	eq, _ := strconv.ParseFloat(co.ExecQty, 64)
	ap, _ := strconv.ParseFloat(co.AvgPx, 64)
	return co.Status, eq, ap
}

func closeLtLegSync(sym string, expectedPos, lb, la float64) (float64, int64) {
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
	qty := math.Abs(curPos)
	if qty < m.MinB || lb <= 0 || la <= 0 {
		return 0, 0
	}
	side, px, estPx := "BID", roundTo(la*1.0075, m.PxDec), la
	if curPos > 0 {
		side, px, estPx = "ASK", roundTo(lb*0.9925, m.PxDec), lb
	}
	liveMu.Lock()
	recordLtTxLocked(time.Now().UnixMilli())
	liveMu.Unlock()
	coi, _ := placeLtHedgeIOC(sym, side, px, qty, true)
	targetAbs := math.Max(0.0, math.Abs(curPos)-qty+m.MinB*0.5)

	deadline := time.Now().Add(3200 * time.Millisecond)
	chk1, chk2 := false, false
	for time.Now().Before(deadline) {
		if st := mktStates[sym]; st != nil {
			st.mu.Lock()
			pNow := st.pos
			st.mu.Unlock()
			if math.Abs(pNow) <= targetAbs {
				return estPx, time.Since(t0).Milliseconds()
			}
		}
		el := time.Since(t0)
		if (!chk1 && el >= 700*time.Millisecond) || (!chk2 && el >= 2000*time.Millisecond) {
			if el >= 2000*time.Millisecond {
				chk2 = true
			} else {
				chk1 = true
			}
			if coi > 0 {
				if sq, vp, okT := fetchLtTradeFillByCOI(sym, coi); okT && math.Abs(sq) >= qty-m.MinB*0.5 {
					if vp > 0 {
						estPx = vp
					}
					return estPx, time.Since(t0).Milliseconds()
				}
			}
			if rp, _, okP := fetchLtPosRESTChecked(sym); okP && math.Abs(rp) <= targetAbs {
				return estPx, time.Since(t0).Milliseconds()
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	// Stage 2: 200bp fallback if volatile move skipped Stage 1
	if rem, _, okR := fetchLtPosRESTChecked(sym); okR && math.Abs(rem) > targetAbs {
		lb2, la2 := fetchLtBookREST(m.ID)
		if lb2 <= 0 || la2 <= 0 {
			lb2, la2 = lb, la
		}
		rSide, rPx := "BID", roundTo(la2*1.020, m.PxDec)
		if rem > 0 {
			rSide, rPx = "ASK", roundTo(lb2*0.980, m.PxDec)
		}
		liveMu.Lock()
		recordLtTxLocked(time.Now().UnixMilli())
		liveMu.Unlock()
		_, _ = placeLtHedgeIOC(sym, rSide, rPx, math.Abs(rem)-targetAbs, true)
		time.Sleep(1200 * time.Millisecond)
	}
	return estPx, time.Since(t0).Milliseconds()
}

func waitExactLtCloseTrade(sym string, minTsMs int64) (float64, float64, bool) {
	for _, w := range []time.Duration{250 * time.Millisecond, 600 * time.Millisecond, 1100 * time.Millisecond, 1500 * time.Millisecond} {
		time.Sleep(w)
		if time.Now().UnixMilli() < ltRestCoolUntil.Load() {
			return 0, 0, false
		}
		u := fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=20&sort_by=timestamp", BaseURL, AcctIdx)
		if txCli != nil {
			if tok, e := txCli.GetAuthToken(time.Now().Add(10 * time.Minute)); e == nil && tok != "" {
				u += "&auth=" + url.QueryEscape(tok)
			}
		}
		resp, err := ltGet(ltRestHTTP, u)
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 || resp.StatusCode == 403 {
			ltRestCoolUntil.Store(time.Now().Add(12 * time.Second).UnixMilli())
			return 0, 0, false
		}
		if resp.StatusCode != 200 {
			continue
		}
		var tr struct {
			Trades []struct {
				Sym       string `json:"market_symbol"`
				MarketID  int64  `json:"market_id"`
				Timestamp int64  `json:"timestamp"`
				Price     string `json:"price"`
				Size      string `json:"size"`
				BidAcct   int64  `json:"bid_account_id"`
				AskAcct   int64  `json:"ask_account_id"`
				BidPnl    string `json:"bid_account_pnl"`
				AskPnl    string `json:"ask_account_pnl"`
			} `json:"trades"`
		}
		if json.Unmarshal(b, &tr) != nil {
			continue
		}
		m := markets[sym]
		var cost, totSz, sumPnl float64
		found := false
		for _, t := range tr.Trades {
			ts := t.Timestamp
			if ts > 0 && ts < 1e11 {
				ts *= 1000
			}
			if ts < minTsMs-3000 || (t.Sym != sym && t.MarketID != m.ID) {
				continue
			}
			pnlStr := ""
			if t.BidAcct == AcctIdx {
				pnlStr = t.BidPnl
			} else if t.AskAcct == AcctIdx {
				pnlStr = t.AskPnl
			}
			if strings.TrimSpace(pnlStr) == "" {
				continue
			}
			px, _ := strconv.ParseFloat(t.Price, 64)
			sz, _ := strconv.ParseFloat(t.Size, 64)
			pv, _ := strconv.ParseFloat(pnlStr, 64)
			if px > 0 && sz > 0 {
				cost += px * sz
				totSz += sz
				sumPnl += pv
				found = true
			}
		}
		if found && totSz > 0 {
			return cost / totSz, sumPnl, true
		}
	}
	return 0, 0, false
}

// ---------- Single-Loop Slot State Machine (Zero Concurrency Races!) ----------

func finalizeSimpleClose(sl *SimpleSlot, bnExitPx, exitFeeBp float64, execType, reason string, exitLatMs int64, now time.Time) {
	closeStartMs := time.Now().UnixMilli()
	cSym, cPeerSym, cLtSide, cPeerSide := sl.Sym, sl.PeerSym, sl.LtSide, sl.PeerSide
	qty, eLtPx, eBnPx, fx, eFeeUsd, openedAt, tid := sl.HedgedQty, sl.EntryLtPx, sl.EntryPeerPx, sl.EntryFx, sl.EntryFeeUsd, sl.OpenedAt, sl.TradeID
	if fx <= 0 {
		fx = 1.0
	}
	var lb, la float64
	if st := mktStates[cSym]; st != nil {
		st.mu.Lock()
		lb, la = st.ltBid, st.ltAsk
		st.mu.Unlock()
	}
	expLtPos := -qty
	if cLtSide == "BID" {
		expLtPos = qty
	}
	ltExitPx, ltLatMs := closeLtLegSync(cSym, expLtPos, lb, la)
	if ltExitPx <= 0 {
		ltExitPx = eLtPx
	}
	ltGross := (eLtPx - ltExitPx) * qty
	bnGross := (bnExitPx - eBnPx) * fx * qty
	if cLtSide == "BID" {
		ltGross = (ltExitPx - eLtPx) * qty
		bnGross = (eBnPx - bnExitPx) * fx * qty
	}
	if exPx, exPnl, okEx := waitExactLtCloseTrade(cSym, closeStartMs); okEx {
		ltExitPx, ltGross = exPx, exPnl
	}
	notional := qty * eLtPx
	exitFeeUsd := (qty * bnExitPx * fx) * (exitFeeBp / 10000.0)
	totalFeeUsd := eFeeUsd + exitFeeUsd
	netUsd := (ltGross + bnGross) - totalFeeUsd
	netBps := 0.0
	if notional > 0 {
		netBps = netUsd / notional * 10000.0
	}
	closeBnSide, closeLtSide := "SELL", "BUY"
	if cPeerSide == "SELL" {
		closeBnSide, closeLtSide = "BUY", "SELL"
	}

	liveMu.Lock()
	liveAcct.RealizedGross = roundTo(liveAcct.RealizedGross+ltGross+bnGross, 4)
	liveAcct.FeesPaid = roundTo(liveAcct.FeesPaid+totalFeeUsd, 4)
	liveAcct.Cash = roundTo(liveAcct.StartCash+liveAcct.RealizedGross-liveAcct.FeesPaid+liveAcct.FundingUsd, 4)
	liveAcct.GrossVolumeUsd = roundTo(liveAcct.GrossVolumeUsd+notional*2.0, 2)
	if execType == "MAKER_GTX_EXIT" {
		liveAcct.MakerFills++
	} else {
		liveAcct.TakerFills++
	}
	liveAcct.TakerFills++
	liveAcct.RoundTrips++
	if netUsd > 0 {
		liveAcct.WinTrips++
		liveAcct.GrossWinUsd = roundTo(liveAcct.GrossWinUsd+netUsd, 4)
	} else if netUsd < 0 {
		liveAcct.GrossLossUsd = roundTo(liveAcct.GrossLossUsd+math.Abs(netUsd), 4)
	}
	liveAcct.RecentTripPnls = append(liveAcct.RecentTripPnls, roundTo(netUsd, 4))
	if len(liveAcct.RecentTripPnls) > 12 {
		liveAcct.RecentTripPnls = append([]float64(nil), liveAcct.RecentTripPnls[len(liveAcct.RecentTripPnls)-12:]...)
	}
	if liveAcct.GrossLossUsd > 1e-6 {
		liveAcct.ProfitFactor = roundTo(liveAcct.GrossWinUsd/liveAcct.GrossLossUsd, 2)
	} else if liveAcct.GrossWinUsd > 0 {
		liveAcct.ProfitFactor = 99.0
	}
	nowMs := now.UnixMilli()
	appendLiveFillLocked(LiveFillAudit{TsMs: nowMs, StratID: liveAcct.ID, Venue: "BN_USDC", Symbol: cPeerSym, Side: closeBnSide, ExecType: execType, FillPx: bnExitPx, SignalPx: bnExitPx, Qty: qty, Notional: roundTo(notional, 2), FeeUsd: roundTo(exitFeeUsd, 4), Realized: roundTo(bnGross-exitFeeUsd, 4), LatencyMs: exitLatMs, Tag: "PEER_EXIT"})
	appendLiveFillLocked(LiveFillAudit{TsMs: nowMs, StratID: liveAcct.ID, Venue: "LT_STD", Symbol: cSym, Side: closeLtSide, ExecType: "LT_IOC_CLOSE", FillPx: ltExitPx, SignalPx: ltExitPx, Qty: qty, Notional: roundTo(notional, 2), FeeUsd: 0, Realized: roundTo(ltGross, 4), LatencyMs: ltLatMs, Tag: reason})
	*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE", CoolUntil: time.Now().Add(3 * time.Second)}
	liveMu.Unlock()

	recordStratClose(tid, liveAcct.ID, fmt.Sprintf("LT_%s_%s+BN_%s", cSym, cLtSide, cPeerSide), cSym, notional, now.Sub(openedAt).Seconds(), netBps, netUsd, bnGross-totalFeeUsd, ltGross, totalFeeUsd, reason)
}

func symUsedInSlots(sym string, skipIdx int) bool {
	for _, s := range liveSlots {
		if s != nil && s.Idx != skipIdx && s.Sym == sym && s.Sym != "" {
			return true
		}
	}
	return false
}

func anySlotRestingLt() bool {
	for _, s := range liveSlots {
		if s != nil && s.State == "LT_ENTRY_OPEN" {
			return true
		}
	}
	return false
}

func activeNotionalTotal() float64 {
	tot := 0.0
	for _, s := range liveSlots {
		if s != nil && s.State != "IDLE" {
			if s.HedgedQty > 0 && s.EntryLtPx > 0 {
				tot += s.HedgedQty * s.EntryLtPx
			} else {
				tot += liveAcct.ClipUsd
			}
		}
	}
	return tot
}

func refreshSlotLtBBO(sl *SimpleSlot) (float64, float64) {
	st := mktStates[sl.Sym]
	var lb, la float64
	if st != nil {
		st.mu.Lock()
		lb, la = st.ltBid, st.ltAsk
		st.mu.Unlock()
	}
	if time.Since(sl.LastLtRestBBO) >= 3500*time.Millisecond {
		sl.LastLtRestBBO = time.Now()
		if rb, ra := fetchLtBookREST(markets[sl.Sym].ID); rb > 0 && ra > rb {
			lb, la = rb, ra
		}
	}
	return lb, la
}

func slotPresetAndClip(idx, tier int, baseClip float64) (StratPreset, float64) {
	if tier < 2 || secondPreset.ForwardID == "" || secondPreset.ForwardID == activePreset.ForwardID {
		return activePreset, baseClip
	}
	if tier == 2 {
		if idx == 1 {
			return secondPreset, roundTo(baseClip*0.8, 0) // 40% to #2 ($36)
		}
		return activePreset, roundTo(baseClip*1.2, 0) // 60% to #1 ($54)
	}
	if idx == 2 {
		return secondPreset, baseClip // 33% to #2 (1x $75)
	}
	return activePreset, baseClip // 67% to #1 (2x $75)
}

func stepSimpleSlot(sl *SimpleSlot, now time.Time) {
	maxSlots, baseClip, _ := tierSpec(liveAcct.Tier)
	preset, clipUsd := slotPresetAndClip(sl.Idx, liveAcct.Tier, baseClip)
	halted := liveAcct.Halted || liveAcct.Tier <= 0

	// 1. STATE: LT_ENTRY_OPEN (Max 1 resting 0% PostOnly order across entire bot)
	if sl.State == "LT_ENTRY_OPEN" {
		m := markets[sl.Sym]
		st := mktStates[sl.Sym]
		if st == nil {
			return
		}
		st.mu.Lock()
		ltPos, ltAvg, lb, la := st.pos, st.avgEntry, st.ltBid, st.ltAsk
		st.mu.Unlock()
		if now.Sub(sl.LastOrdPoll) >= 2500*time.Millisecond {
			sl.LastOrdPoll = now
			if rp, rae, okR := fetchLtPosRESTChecked(sl.Sym); okR && math.Abs(rp) >= m.MinB {
				ltPos, ltAvg = rp, rae
			}
		}
		if math.Abs(ltPos) >= m.MinB {
			expSign := 1.0
			if sl.LtSide == "ASK" {
				expSign = -1.0
			}
			if ltPos*expSign < 0 {
				fmt.Printf("[WRONG_SIGN_ORPHAN sym=%s want=%s gotPos=%.4f] Flattening instead of same-side hedge!\n", sl.Sym, sl.LtSide, ltPos)
				cancelAll()
				_, _ = closeLtLegSync(sl.Sym, ltPos, lb, la)
				*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE", CoolUntil: time.Now().Add(8 * time.Second)}
				return
			}
			// Derive Binance hedge side strictly from actual Lighter position sign!
			if ltPos > 0 {
				sl.LtSide, sl.PeerSide = "BID", "SELL"
			} else {
				sl.LtSide, sl.PeerSide = "ASK", "BUY"
			}
			cancelAll()
			liveMu.Lock()
			recordLtTxLocked(time.Now().UnixMilli())
			liveMu.Unlock()

			fillQty := math.Abs(ltPos)
			if ltAvg <= 0 {
				ltAvg = sl.LtLimitPx
			}
			bnSym, qDec, _, step, minNot, pBid, pAsk, _, _, fx, _, _, okB := getBnUsdcBook(sl.Sym, now)
			if !okB {
				bnSym, fx = sl.PeerSym, 1.0
			}
			hQty := fillQty
			if step > 0 {
				hQty = roundTo(math.Floor(fillQty/step+1e-9)*step, qDec)
			}
			sigPx := pBid
			if sl.PeerSide == "BUY" {
				sigPx = pAsk
			}
			if hQty <= 0 || (minNot > 0 && sigPx > 0 && hQty*sigPx < minNot-0.4) {
				_, _ = closeLtLegSync(sl.Sym, ltPos, lb, la)
				*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE", CoolUntil: time.Now().Add(5 * time.Second)}
				return
			}
			fq, ap, latMs, okH := executeBnMarketOrder(bnSym, sl.PeerSide, hQty, qDec, false)
			if !okH || fq <= 0 {
				_, _ = closeLtLegSync(sl.Sym, ltPos, lb, la)
				*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE", CoolUntil: time.Now().Add(10 * time.Second)}
				return
			}
			if trim := fillQty - fq; trim >= m.MinB {
				if ltPos < 0 {
					trim = -trim
				}
				_, _ = closeLtLegSync(sl.Sym, trim, lb, la)
			}
			if ap <= 0 {
				ap = sigPx
			}
			notional := fq * ltAvg
			entryFeeUsd := (fq * ap * fx) * (4.0 / 10000.0)
			ltHuman := "BUY"
			if sl.LtSide == "ASK" {
				ltHuman = "SELL"
			}
			tid := time.Now().UnixMilli()
			liveMu.Lock()
			liveAcct.MakerFills++
			liveAcct.TakerFills++
			liveAcct.GrossVolumeUsd = roundTo(liveAcct.GrossVolumeUsd+notional*2.0, 2)
			appendLiveFillLocked(LiveFillAudit{TsMs: tid, StratID: liveAcct.ID, Venue: "LT_STD", Symbol: sl.Sym, Side: ltHuman, ExecType: "LT_MAKER_0FEE", FillPx: ltAvg, SignalPx: sl.LtLimitPx, Qty: fq, Notional: roundTo(notional, 2), LatencyMs: now.Sub(sl.PlacedAt).Milliseconds(), Tag: "LT_ENTRY"})
			appendLiveFillLocked(LiveFillAudit{TsMs: tid, StratID: liveAcct.ID, Venue: "BN_USDC", Symbol: bnSym, Side: sl.PeerSide, ExecType: "TOKYO_TAKER_HEDGE", FillPx: ap, SignalPx: sigPx, Qty: fq, Notional: roundTo(fq*ap*fx, 2), FeeUsd: roundTo(entryFeeUsd, 4), LatencyMs: latMs, Tag: "PEER_HEDGE"})
			sl.State = "HEDGED"
			sl.HedgedQty = fq
			sl.EntryLtPx = ltAvg
			sl.EntryPeerPx = ap
			sl.EntryFx = fx
			sl.EntryFeeUsd = roundTo(entryFeeUsd, 4)
			sl.OpenedAt = time.Now()
			sl.TradeID = tid
			sl.CancelSent = false
			liveMu.Unlock()
			edgeBp := ((ltAvg - ap*fx) / (ap * fx) * 10000.0) - 4.0
			if sl.LtSide == "BID" {
				edgeBp = ((ap*fx - ltAvg) / (ap * fx) * 10000.0) - 4.0
			}
			recordStratOpen(tid, liveAcct.ID, fmt.Sprintf("LT_%s_%s+BN_%s", sl.Sym, sl.LtSide, sl.PeerSide), sl.Sym, notional, edgeBp, fmt.Sprintf("Simple Lt0%%@%.5f+Bn@%.5f(%dms)", ltAvg, ap, latMs))
			return
		}
		if sl.CancelSent {
			if time.Since(sl.CancelAt) >= 4500*time.Millisecond {
				if sq, _, okT := fetchLtTradeFillByCOI(sl.Sym, sl.LtCOI); okT && math.Abs(sq) >= m.MinB {
					sl.CancelSent = false
					return
				}
				rp, _, okP := fetchLtPosRESTChecked(sl.Sym)
				oo, okOO := fetchLtMarketOpenOrderCountREST(sl.Sym)
				if !okP || !okOO {
					return
				}
				if math.Abs(rp) >= m.MinB {
					sl.CancelSent = false
					return
				}
				if oo > 0 {
					cancelAll()
					sl.CancelAt = time.Now()
					return
				}
				*sl = SimpleSlot{Idx: sl.Idx, State: "IDLE", CoolUntil: time.Now().Add(1500 * time.Millisecond)}
			}
			return
		}
		ageS := now.Sub(sl.PlacedAt).Seconds()
		_, _, _, _, _, pBid, pAsk, emaBps, jumpBps, fx, _, _, okB := getBnUsdcBook(sl.Sym, now)
		shouldCancel := halted || ageS >= 22.0
		if okB && ageS >= 4.5 {
			curDisloc := (((sl.LtLimitPx - pAsk*fx) / (pAsk * fx)) * 10000.0) - emaBps
			refBbo := la
			if sl.LtSide == "BID" {
				curDisloc = (((pBid*fx - sl.LtLimitPx) / (pBid * fx)) * 10000.0) + emaBps
				refBbo = lb
			}
			driftBp := 0.0
			if refBbo > 0 {
				driftBp = math.Abs(sl.LtLimitPx-refBbo) / refBbo * 10000.0
			}
			if curDisloc < preset.EntryDisloc-2.2 || driftBp > 2.0 || math.Abs(jumpBps) > 2.5 {
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
			liveAcct.MakerCancelled++
			liveMu.Unlock()
			cancelAll()
			sl.CancelSent = true
			sl.CancelAt = time.Now()
		}
		return
	}

	// 2. STATE: HEDGED (Look for 0%-Fee Binance USDC-M GTX Maker Exit)
	if sl.State == "HEDGED" {
		lb, la := refreshSlotLtBBO(sl)
		bnSym, qDec, pxDec, _, _, pBid, pAsk, _, jumpBps, fx, _, _, okB := getBnUsdcBook(sl.Sym, now)
		if !okB || lb <= 0 || la <= lb {
			return
		}
		holdS := now.Sub(sl.OpenedAt).Seconds()
		closeBnSide, exitPx := "SELL", roundTo(pAsk, pxDec)
		combNetBp := (((exitPx-sl.EntryPeerPx)*fx)+(sl.EntryLtPx-la))/sl.EntryLtPx*10000.0 - 4.0
		if sl.PeerSide == "SELL" {
			closeBnSide, exitPx = "BUY", roundTo(pBid, pxDec)
			combNetBp = (((sl.EntryPeerPx-exitPx)*fx)+(lb-sl.EntryLtPx))/sl.EntryLtPx*10000.0 - 4.0
		}
		legDrift := math.Abs(0.5*(lb+la)-sl.EntryLtPx) / math.Max(sl.EntryLtPx, 1e-9)
		if liveAcct.Halted || legDrift >= 0.10 {
			if _, ap, latMs, okM := executeBnMarketOrder(bnSym, closeBnSide, sl.HedgedQty, qDec, true); okM {
				if ap <= 0 {
					ap = 0.5 * (pBid + pAsk)
				}
				finalizeSimpleClose(sl, ap, 4.0, "TAKER_HALT_FLUSH", "GUARD_FLUSH", latMs, now)
			}
			return
		}
		reqNetBp := preset.ExitNetBp
		if halted {
			reqNetBp = 0.15
		} else if holdS >= preset.MaxHoldSec {
			reqNetBp = 0.30
		} else if holdS >= preset.DecaySec {
			reqNetBp = math.Max(0.8, preset.ExitNetBp-1.2)
		}
		if combNetBp >= reqNetBp && math.Abs(jumpBps) <= 2.0 {
			if rb, ra := fetchLtBookREST(markets[sl.Sym].ID); rb > 0 && ra > rb {
				if sl.PeerSide == "BUY" {
					combNetBp = (((exitPx-sl.EntryPeerPx)*fx)+(sl.EntryLtPx-ra))/sl.EntryLtPx*10000.0 - 4.0
				} else {
					combNetBp = (((sl.EntryPeerPx-exitPx)*fx)+(rb-sl.EntryLtPx))/sl.EntryLtPx*10000.0 - 4.0
				}
				if combNetBp >= reqNetBp {
					if oid, okE := placeBnMakerExit(bnSym, closeBnSide, sl.HedgedQty, exitPx, qDec, pxDec); okE {
						liveAcct.MakerPlaced++
						sl.State = "PEER_EXIT_OPEN"
						sl.ExitPeerOid = oid
						sl.ExitPeerPx = exitPx
						sl.ExitPlaced = now
						sl.LastOrdPoll = time.Time{}
						sl.LastLtRestBBO = now
					}
				}
			}
		}
		return
	}

	// 3. STATE: PEER_EXIT_OPEN (Resting 0% Binance GTX Maker Exit)
	if sl.State == "PEER_EXIT_OPEN" {
		lb, la := refreshSlotLtBBO(sl)
		bnSym, qDec, _, _, _, _, _, _, _, fx, _, _, okB := getBnUsdcBook(sl.Sym, now)
		if !okB {
			bnSym, fx = sl.PeerSym, sl.EntryFx
		}
		status, _, avgP, polled, okQ := queryBnOrder(bnSym, sl.ExitPeerOid, sl.LastOrdPoll, false)
		if polled {
			sl.LastOrdPoll = now
		}
		if !okQ {
			return
		}
		if avgP <= 0 {
			avgP = sl.ExitPeerPx
		}
		if status == "FILLED" {
			finalizeSimpleClose(sl, avgP, 0.0, "MAKER_GTX_EXIT", "ZERO_FEE_4LEG_TP", now.Sub(sl.ExitPlaced).Milliseconds(), now)
			return
		}
		if status == "CANCELED" || status == "EXPIRED" {
			sl.State = "HEDGED"
			sl.ExitPeerOid = 0
			return
		}
		curNetBp := (((sl.ExitPeerPx-sl.EntryPeerPx)*fx)+(sl.EntryLtPx-la))/sl.EntryLtPx*10000.0 - 4.0
		if sl.PeerSide == "SELL" {
			curNetBp = (((sl.EntryPeerPx-sl.ExitPeerPx)*fx)+(lb-sl.EntryLtPx))/sl.EntryLtPx*10000.0 - 4.0
		}
		minKeep := 0.50
		if now.Sub(sl.OpenedAt).Seconds() >= preset.MaxHoldSec {
			minKeep = 0.0
		}
		if liveAcct.Halted || curNetBp < minKeep || now.Sub(sl.ExitPlaced) >= 20*time.Second {
			cSt, cQty, cPx := cancelBnOrder(bnSym, sl.ExitPeerOid)
			liveAcct.MakerCancelled++
			if cPx <= 0 {
				cPx = avgP
			}
			m := markets[sl.Sym]
			if cSt == "FILLED" || cQty >= sl.HedgedQty-m.MinB*0.5 {
				finalizeSimpleClose(sl, cPx, 0.0, "MAKER_GTX_EXIT", "ZERO_FEE_4LEG_TP", now.Sub(sl.ExitPlaced).Milliseconds(), now)
				return
			}
			if cQty >= m.MinB {
				partPos := -cQty
				if sl.LtSide == "BID" {
					partPos = cQty
				}
				_, _ = closeLtLegSync(sl.Sym, partPos, lb, la)
				sl.HedgedQty = roundTo(sl.HedgedQty-cQty, qDec)
			}
			sl.State = "HEDGED"
			sl.ExitPeerOid = 0
		}
		return
	}

	// 4. STATE: IDLE — Open next clean LT <-> BN_USDC trade if slot allowed
	if halted || sl.Idx >= maxSlots || clipUsd < 10.0 || now.Before(sl.CoolUntil) ||
		now.UnixMilli() < quotaCoolUntil.Load() || now.UnixMilli() < ltRestCoolUntil.Load() {
		return
	}
	ltEqNow := float64(eqNow.Load()) / 1e6
	dnMu.Lock()
	bnAvail, bnTot := dnSharedAvail, bnTotalMargin
	dnMu.Unlock()
	if ltEqNow < 30 || bnTot < 30 || bnAvail < math.Max(15.0, bnTot*0.25) {
		return
	}
	if activeNotionalTotal()+clipUsd > math.Min(math.Min(ltEqNow*3.5, bnTot*2.5), 600.0) {
		return
	}
	liveMu.Lock()
	canTx := canSendLtTxLocked(now.UnixMilli(), false)
	liveMu.Unlock()
	if !canTx {
		return
	}

	// Mode A: Pure Crossed-Book Grab (when preset.TakerGrabBp > 0)
	if preset.TakerGrabBp > 0 {
		gSym, gBnSym, gLtSide, gBnSide := "", "", "", ""
		gCross, gSigPx, gQty, gStep, gFx := -999.0, 0.0, 0.0, 0.0, 1.0
		gQDec := 0
		for _, sym := range preset.Symbols {
			if symUsedInSlots(sym, sl.Idx) {
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
			bnSym, qDec, _, step, minNot, pBid, pAsk, _, jumpBps, fx, warm, age, okB := getBnUsdcBook(sym, now)
			if !okB || warm < 8 || age > 8*time.Second || math.Abs(jumpBps) > 1.8 || (pAsk-pBid)/pBid*10000.0 > 8.0 {
				continue
			}
			cStep := math.Max(step, math.Pow10(-m.SzDec))
			qD := qDec
			if m.SzDec < qD {
				qD = m.SzDec
			}
			ltMid := 0.5 * (lb + la)
			qty := roundTo(math.Floor((clipUsd/ltMid)/cStep+1e-9)*cStep, qD)
			if qty < m.MinB || qty*ltMid < math.Max(10.5, minNot) {
				continue
			}
			if cs := ((lb - pAsk*fx) / (pAsk * fx)) * 10000.0; cs >= preset.TakerGrabBp && cs > gCross && bnTopDepthUsd(bnSym, "BUY") >= clipUsd*2.5 {
				gCross, gSym, gBnSym, gLtSide, gBnSide, gSigPx, gQty, gStep, gQDec, gFx = cs, sym, bnSym, "ASK", "BUY", lb, qty, step, qDec, fx
			}
			if cl := ((pBid*fx - la) / (pBid * fx)) * 10000.0; cl >= preset.TakerGrabBp && cl > gCross && bnTopDepthUsd(bnSym, "SELL") >= clipUsd*2.5 {
				gCross, gSym, gBnSym, gLtSide, gBnSide, gSigPx, gQty, gStep, gQDec, gFx = cl, sym, bnSym, "BID", "SELL", la, qty, step, qDec, fx
			}
		}
		if gSym == "" {
			return
		}
		m := markets[gSym]
		rb, ra := fetchLtBookREST(m.ID)
		if rb <= 0 || ra <= rb {
			return
		}
		_, _, _, _, _, pBidC, pAskC, _, _, fxC, _, _, okC := getBnUsdcBook(gSym, now)
		if !okC {
			return
		}
		iocPx, bnSig := roundTo(ra*1.0012, m.PxDec), pBidC
		if gLtSide == "ASK" {
			if ((rb-pAskC*fxC)/(pAskC*fxC))*10000.0 < preset.TakerGrabBp {
				return
			}
			gSigPx, bnSig, iocPx = rb, pAskC, roundTo(rb*0.9988, m.PxDec)
		} else {
			if ((pBidC*fxC-ra)/(pBidC*fxC))*10000.0 < preset.TakerGrabBp {
				return
			}
			gSigPx = ra
		}
		if rpPre, _, okPre := fetchLtPosRESTChecked(gSym); !okPre || math.Abs(rpPre) >= m.MinB {
			return
		}
		ensureBnLeverage(gBnSym)
		liveMu.Lock()
		recordLtTxLocked(time.Now().UnixMilli())
		liveMu.Unlock()
		t0 := time.Now()
		coi, sent := placeLtHedgeIOC(gSym, gLtSide, iocPx, gQty, false)
		var ltPos, ltAvg float64
		if sent {
			ltPos, ltAvg = waitLtPositionFill(gSym, coi, m.MinB, 4200*time.Millisecond)
		}
		if math.Abs(ltPos) < m.MinB {
			sl.CoolUntil = time.Now().Add(2500 * time.Millisecond)
			return
		}
		expS := 1.0
		if gLtSide == "ASK" {
			expS = -1.0
		}
		if ltPos*expS < 0 {
			_, _ = closeLtLegSync(gSym, ltPos, rb, ra)
			sl.CoolUntil = time.Now().Add(8 * time.Second)
			return
		}
		if ltPos > 0 {
			gLtSide, gBnSide = "BID", "SELL"
		} else {
			gLtSide, gBnSide = "ASK", "BUY"
		}
		fillQty := math.Abs(ltPos)
		if ltAvg <= 0 {
			ltAvg = gSigPx
		}
		hQty := fillQty
		if gStep > 0 {
			hQty = roundTo(math.Floor(fillQty/gStep+1e-9)*gStep, gQDec)
		}
		fq, ap, latMs, okH := executeBnMarketOrder(gBnSym, gBnSide, hQty, gQDec, false)
		if !okH || fq <= 0 {
			_, _ = closeLtLegSync(gSym, ltPos, rb, ra)
			sl.CoolUntil = time.Now().Add(8 * time.Second)
			return
		}
		if ap <= 0 {
			ap = bnSig
		}
		notional := fq * ltAvg
		feeUsd := (fq * ap * gFx) * (4.0 / 10000.0)
		tid := time.Now().UnixMilli()
		ltH := "BUY"
		if gLtSide == "ASK" {
			ltH = "SELL"
		}
		liveMu.Lock()
		liveAcct.TakerFills += 2
		liveAcct.GrossVolumeUsd = roundTo(liveAcct.GrossVolumeUsd+notional*2.0, 2)
		appendLiveFillLocked(LiveFillAudit{TsMs: tid, StratID: liveAcct.ID, Venue: "LT_STD", Symbol: gSym, Side: ltH, ExecType: "LT_IOC_0FEE_GRAB", FillPx: ltAvg, SignalPx: gSigPx, Qty: fq, Notional: roundTo(notional, 2), LatencyMs: time.Since(t0).Milliseconds(), Tag: "S9_GRAB"})
		appendLiveFillLocked(LiveFillAudit{TsMs: tid, StratID: liveAcct.ID, Venue: "BN_USDC", Symbol: gBnSym, Side: gBnSide, ExecType: "TOKYO_TAKER_HEDGE", FillPx: ap, SignalPx: bnSig, Qty: fq, Notional: roundTo(fq*ap*gFx, 2), FeeUsd: roundTo(feeUsd, 4), LatencyMs: latMs, Tag: "PEER_HEDGE"})
		sl.State, sl.Sym, sl.PeerSym, sl.LtSide, sl.PeerSide = "HEDGED", gSym, gBnSym, gLtSide, gBnSide
		sl.HedgedQty, sl.EntryLtPx, sl.EntryPeerPx, sl.EntryFx, sl.EntryFeeUsd = fq, ltAvg, ap, gFx, roundTo(feeUsd, 4)
		sl.OpenedAt, sl.TradeID = time.Now(), tid
		liveMu.Unlock()
		return
	}

	// Mode B: 0% Lighter PostOnly Maker Entry (strictly max 1 resting order across entire bot!)
	if anySlotRestingLt() {
		return
	}
	bSym, bBnSym, bLtSide, bBnSide := "", "", "", ""
	bDisloc, bLtPx, bQty := -999.0, 0.0, 0.0
	minRaw := 4.0 + math.Max(preset.MinRawEdgeBp, 2.0)
	for _, sym := range preset.Symbols {
		if symUsedInSlots(sym, sl.Idx) {
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
		if math.Abs(ltPos) >= m.MinB || lb <= 0 || la <= lb || now.Sub(lTS) > 8*time.Second || (la-lb)/lb*10000.0 > 10.0 {
			continue
		}
		bnSym, qDec, _, step, minNot, pBid, pAsk, emaBps, jumpBps, fx, warm, age, okB := getBnUsdcBook(sym, now)
		if !okB || warm < 8 || age > 8*time.Second || math.Abs(jumpBps) > 1.8 || (pAsk-pBid)/pBid*10000.0 > 8.0 {
			continue
		}
		cStep := math.Max(step, math.Pow10(-m.SzDec))
		qD := qDec
		if m.SzDec < qD {
			qD = m.SzDec
		}
		ltMid := 0.5 * (lb + la)
		qty := roundTo(math.Floor((clipUsd/ltMid)/cStep+1e-9)*cStep, qD)
		if qty < m.MinB || qty*ltMid < math.Max(10.5, minNot) {
			continue
		}
		rawS := ((la - pAsk*fx) / (pAsk * fx)) * 10000.0
		if rawS >= minRaw && (rawS >= preset.EntryDisloc || rawS-emaBps >= preset.EntryDisloc) && rawS > bDisloc && bnTopDepthUsd(bnSym, "BUY") >= clipUsd*2.5 {
			bDisloc, bSym, bBnSym, bLtSide, bBnSide, bLtPx, bQty = rawS, sym, bnSym, "ASK", "BUY", la, qty
		}
		rawL := ((pBid*fx - lb) / (pBid * fx)) * 10000.0
		if rawL >= minRaw && (rawL >= preset.EntryDisloc || rawL+emaBps >= preset.EntryDisloc) && rawL > bDisloc && bnTopDepthUsd(bnSym, "SELL") >= clipUsd*2.5 {
			bDisloc, bSym, bBnSym, bLtSide, bBnSide, bLtPx, bQty = rawL, sym, bnSym, "BID", "SELL", lb, qty
		}
	}
	if bSym == "" {
		return
	}
	rb, ra := fetchLtBookREST(markets[bSym].ID)
	if rb <= 0 || ra <= rb {
		return
	}
	_, _, _, _, _, pBidC, pAskC, _, _, fxC, _, _, okC := getBnUsdcBook(bSym, now)
	if !okC {
		return
	}
	if bLtSide == "ASK" {
		bLtPx = ra
		if ((ra-pAskC*fxC)/(pAskC*fxC))*10000.0 < minRaw {
			return
		}
	} else {
		bLtPx = rb
		if ((pBidC*fxC-rb)/(pBidC*fxC))*10000.0 < minRaw {
			return
		}
	}
	if rpPre, _, okPre := fetchLtPosRESTChecked(bSym); !okPre || math.Abs(rpPre) >= markets[bSym].MinB {
		return
	}
	ensureBnLeverage(bBnSym)
	liveMu.Lock()
	recordLtTxLocked(time.Now().UnixMilli())
	liveMu.Unlock()
	coi, placed := placeLtPostOnlyMakerSync(bSym, bLtSide, bLtPx, bQty)
	if !placed {
		sl.CoolUntil = time.Now().Add(2 * time.Second)
		return
	}
	liveMu.Lock()
	liveAcct.MakerPlaced++
	sl.State, sl.Sym, sl.PeerSym, sl.LtSide, sl.PeerSide = "LT_ENTRY_OPEN", bSym, bBnSym, bLtSide, bBnSide
	sl.LtCOI, sl.LtLimitPx, sl.PlacedAt, sl.LastOrdPoll, sl.CancelSent = coi, bLtPx, time.Now(), time.Now(), false
	liveMu.Unlock()
	fmt.Printf("[LIVE_QUOTE slot=%d preset=%s sym=%s lt=%s qty=%.4f @ %.5f raw=%+.2fbp]\n", sl.Idx, preset.ForwardID, bSym, bLtSide, bQty, bLtPx, bDisloc)
}

// ---------- 45-Min Forward Lab Auto-Selector & Tier Auto-Scaler ----------

func evaluateForwardWinnerAndTiers() {
	raw, err := os.ReadFile("/tmp/forward_arena.json")
	if err != nil || len(raw) < 50 {
		return
	}
	var fa struct {
		NowTsMs     int64   `json:"nowTsMs"`
		UptimeHours float64 `json:"uptimeHours"`
		Strategies  []struct {
			ID               string  `json:"id"`
			Name             string  `json:"name"`
			StratBootMs      int64   `json:"stratBootMs"`
			HourlyRunRate    float64 `json:"hourlyRunRate"`
			Rolling45mHourly float64 `json:"rolling45mHourly"`
			ProfitFactor     float64 `json:"profitFactor"`
			MaxDrawdownUsd   float64 `json:"maxDrawdownUsd"`
			RoundTrips       int64   `json:"roundTrips"`
		} `json:"strategies"`
	}
	if json.Unmarshal(raw, &fa) != nil {
		return
	}
	refNow := fa.NowTsMs
	if refNow <= 0 {
		refNow = time.Now().UnixMilli()
	}
	bestID, bestName, bestHr, bestPF, bestDD := "", "", -1e9, 0.0, 999.0
	secID, secHr := "", -1e9
	curFwdHr := 0.0
	var bestTrips int64
	bestAgeMin := 0.0
	for _, s := range fa.Strategies {
		if _, isBnPreset := bnPresets[s.ID]; !isBnPreset {
			continue
		}
		hr := s.Rolling45mHourly
		if hr == 0 {
			hr = s.HourlyRunRate
		}
		if s.ID == activePreset.ForwardID {
			curFwdHr = hr
		}
		ageM := fa.UptimeHours * 60.0
		if s.StratBootMs > 0 {
			ageM = float64(refNow-s.StratBootMs) / 60000.0
		}
		if ageM >= 45.0 && hr >= 2.0 && s.ProfitFactor >= 1.40 && s.RoundTrips >= 5 && s.MaxDrawdownUsd < 40.0 {
			if hr > bestHr {
				if bestID != "" && bestHr > secHr {
					secID, secHr = bestID, bestHr
				}
				bestID, bestName, bestHr, bestPF, bestDD, bestTrips, bestAgeMin = s.ID, s.Name, hr, s.ProfitFactor, s.MaxDrawdownUsd, s.RoundTrips, ageM
			} else if s.ID != bestID && hr > secHr {
				secID, secHr = s.ID, hr
			}
		}
	}

	nowMs := time.Now().UnixMilli()
	liveMu.Lock()
	defer liveMu.Unlock()
	if bestID != "" {
		liveAcct.Forward45mHr = roundTo(bestHr, 2)
		liveAcct.ForwardPF = roundTo(bestPF, 2)
		liveAcct.ForwardTrips = bestTrips
		allIdle, restingLt := true, false
		for _, sl := range liveSlots {
			if sl != nil && sl.State != "IDLE" {
				allIdle = false
				if sl.State == "LT_ENTRY_OPEN" {
					restingLt = true
				}
			}
		}
		negSwitch := curFwdHr < 0 && !restingLt
		schedSwitch := allIdle && (lastPresetSwitchMs == 0 || nowMs-lastPresetSwitchMs >= 45*60*1000)
		if (negSwitch || schedSwitch) && bestID != activePreset.ForwardID {
			oldID := activePreset.ForwardID
			activePreset = bnPresets[bestID]
			liveAcct.ID = bestID
			liveAcct.ForwardStratID = bestID
			if bestName != "" {
				liveAcct.Name = bestName
			} else {
				liveAcct.Name = activePreset.Label
			}
			lastPresetSwitchMs = nowMs
			fmt.Printf("[AUTO_SELECT_WINNER old=%s(%.2f/hr) -> preset=%s fwd45=$%+.2f/hr PF=%.2f trips=%d negSwitch=%v]\n", oldID, curFwdHr, bestID, bestHr, bestPF, bestTrips, negSwitch)
		}
	}
	// Weighted 2-strategy split at Tier 2+ (60/40 at T2, 67/33 at T3) whenever a 2nd 45m+ profitable strategy qualifies:
	splitID := secID
	if (splitID == "" || splitID == activePreset.ForwardID) && bestID != "" && bestID != activePreset.ForwardID {
		splitID = bestID
	}
	if splitID != "" && splitID != activePreset.ForwardID {
		secondPreset = bnPresets[splitID]
		liveAcct.SecondStratID = splitID
	} else {
		secondPreset = StratPreset{}
		liveAcct.SecondStratID = ""
	}
	if liveAcct.Tier >= 2 && secondPreset.ForwardID != "" {
		w1, w2 := 67, 33
		if liveAcct.Tier == 2 {
			w1, w2 = 60, 40
		}
		liveAcct.RotatedNote = fmt.Sprintf("Weighted Split: %s (%d%%) + %s (%d%%)", activePreset.ForwardID, w1, secondPreset.ForwardID, w2)
	} else {
		liveAcct.RotatedNote = activePreset.Label
	}
	if liveAcct.Halted {
		return
	}
	// Strict -$8.00 MaxDD or 6-trip PF < 0.95 demotion back to Tier 0 ($0):
	if liveAcct.Tier > 0 {
		if liveAcct.MaxDrawdownUsd >= MaxDemoteDrawdownUSD {
			liveAcct.Tier = 0
			liveAcct.TierChangedMs = nowMs
			liveAcct.TierBaseTrips = liveAcct.RoundTrips
			liveAcct.HaltReason = fmt.Sprintf("Demoted: MaxDD $%.2f >= $8.00", liveAcct.MaxDrawdownUsd)
			applyTierLocked()
			return
		}
		if len(liveAcct.RecentTripPnls) >= 6 {
			w, l := 0.0, 0.0
			for _, p := range liveAcct.RecentTripPnls[len(liveAcct.RecentTripPnls)-6:] {
				if p > 0 {
					w += p
				} else if p < 0 {
					l += math.Abs(p)
				}
			}
			pf6 := 99.0
			if l > 1e-6 {
				pf6 = w / l
			}
			if pf6 < 0.95 && (liveAcct.Equity-liveAcct.StartCash) < -1.0 {
				liveAcct.Tier = 0
				liveAcct.TierChangedMs = nowMs
				liveAcct.TierBaseTrips = liveAcct.RoundTrips
				liveAcct.HaltReason = fmt.Sprintf("Demoted: 6-trip PF %.2f < 0.95", pf6)
				applyTierLocked()
				return
			}
		}
	}
	dwellMin := float64(nowMs-liveAcct.TierChangedMs) / 60000.0
	tripsInTier := liveAcct.RoundTrips - liveAcct.TierBaseTrips
	netGain := liveAcct.Equity - liveAcct.StartCash
	switch liveAcct.Tier {
	case 0:
		if bestID != "" && bestAgeMin >= 45.0 && bestDD < 40.0 && (liveAcct.HaltReason == "" || dwellMin >= 15.0) {
			activePreset = bnPresets[bestID]
			liveAcct.ID = bestID
			liveAcct.ForwardStratID = bestID
			if bestName != "" {
				liveAcct.Name = bestName
			} else {
				liveAcct.Name = activePreset.Label
			}
			liveAcct.RotatedNote = activePreset.Label
			liveAcct.Tier = 1
			liveAcct.StratActiveMs = nowMs
			liveAcct.TierChangedMs = nowMs
			liveAcct.TierBaseTrips = liveAcct.RoundTrips
			liveAcct.MaxDrawdownUsd = 0
			liveAcct.PeakEquity = liveAcct.Equity
			liveAcct.HaltReason = ""
			lastPresetSwitchMs = nowMs
			applyTierLocked()
			fmt.Printf("[AUTO_SCALE -> Tier 1 ($20) preset=%s fwd45=$%+.2f/hr PF=%.2f]\n", bestID, bestHr, bestPF)
		}
	case 1:
		if dwellMin >= 20.0 && tripsInTier >= 3 && netGain > 0 && liveAcct.ProfitFactor >= 1.30 && liveAcct.MaxDrawdownUsd < 4.0 {
			liveAcct.Tier = 2
			liveAcct.TierChangedMs = nowMs
			liveAcct.TierBaseTrips = liveAcct.RoundTrips
			applyTierLocked()
			fmt.Printf("[AUTO_SCALE -> Tier 2 (2x $45) RealPF=%.2f Net=$%+.3f]\n", liveAcct.ProfitFactor, netGain)
		}
	case 2:
		if dwellMin >= 30.0 && tripsInTier >= 5 && netGain > 0.15 && liveAcct.ProfitFactor >= 1.35 && liveAcct.MaxDrawdownUsd < 5.0 {
			liveAcct.Tier = 3
			liveAcct.TierChangedMs = nowMs
			liveAcct.TierBaseTrips = liveAcct.RoundTrips
			applyTierLocked()
			fmt.Printf("[AUTO_SCALE -> Tier 3 (3x $75) RealPF=%.2f Net=$%+.3f]\n", liveAcct.ProfitFactor, netGain)
		}
	}
}

// ---------- Orphan Sweeper & Telemetry ----------

func sweepUntrackedLtOrphansSync() {
	if remotePaused.Load() || breakerTripped.Load() {
		return
	}
	snap, ok := fetchLtAccountCached(2500*time.Millisecond, 0)
	if !ok || snap == nil {
		return
	}
	for _, p := range snap.Positions {
		sym, okS := idToSym[p.MarketID]
		if !okS {
			continue
		}
		m, okM := markets[sym]
		if !okM {
			continue
		}
		sz, _ := strconv.ParseFloat(p.Position, 64)
		if p.Sign < 0 && sz > 0 {
			sz = -sz
		}
		if math.Abs(sz) < m.MinB {
			continue
		}
		tracked := false
		for _, sl := range liveSlots {
			if sl != nil && sl.Sym == sym && (sl.State == "HEDGED" || sl.State == "PEER_EXIT_OPEN" || (sl.State == "LT_ENTRY_OPEN" && time.Since(sl.PlacedAt) < 12*time.Second)) {
				tracked = true
				break
			}
		}
		if !tracked {
			lb, la := fetchLtBookREST(m.ID)
			fmt.Printf("[ORPHAN_SWEEP_LT sym=%s sz=%.4f]\n", sym, sz)
			_, _ = closeLtLegSync(sym, sz, lb, la)
		}
	}
}

func sweepUntrackedBnOrphansSync() {
	if remotePaused.Load() || breakerTripped.Load() {
		return
	}
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/positionRisk", nil)
	if err != nil || sc != 200 {
		return
	}
	var rows []struct {
		Symbol      string `json:"symbol"`
		PositionAmt string `json:"positionAmt"`
	}
	if json.Unmarshal(body, &rows) != nil {
		return
	}
	seen := map[string]bool{}
	for _, r := range rows {
		amt, _ := strconv.ParseFloat(r.PositionAmt, 64)
		if math.Abs(amt) <= 1e-6 {
			continue
		}
		tracked := false
		for _, sl := range liveSlots {
			if sl != nil && sl.State != "IDLE" && sl.PeerSym == r.Symbol {
				tracked = true
				break
			}
		}
		if tracked {
			delete(bnOrphanSeen, r.Symbol)
			continue
		}
		seen[r.Symbol] = true
		bnOrphanSeen[r.Symbol]++
		if bnOrphanSeen[r.Symbol] >= 2 {
			delete(bnOrphanSeen, r.Symbol)
			qDec := 3
			dnMu.Lock()
			if sp, ok := dnByBnSym[r.Symbol]; ok {
				qDec = sp.QDec
			}
			dnMu.Unlock()
			cSide := "SELL"
			if amt < 0 {
				cSide = "BUY"
			}
			fmt.Printf("[ORPHAN_SWEEP_BN sym=%s amt=%.5f]\n", r.Symbol, amt)
			_, _, _, _ = executeBnMarketOrder(r.Symbol, cSide, math.Abs(amt), qDec, true)
		}
	}
	for k := range bnOrphanSeen {
		if !seen[k] {
			delete(bnOrphanSeen, k)
		}
	}
}

func writeLiveArenaTelemetry(now time.Time) {
	nowMs := now.UnixMilli()
	upSec := math.Max(1.0, float64(nowMs-liveBootMs)/1000.0)
	upHr := upSec / 3600.0
	ltEq := float64(eqNow.Load()) / 1e6
	ltCol := float64(ltCollateral.Load()) / 1e6
	if ltCol <= 0 {
		ltCol = ltEq
	}
	dnMu.Lock()
	bnMargin, bnWallet := bnTotalMargin, bnTotalWallet
	if bnMargin <= 0 {
		bnMargin = dnSharedAvail
	}
	dnMu.Unlock()
	asterMu.Lock()
	asMargin, asWallet := asterWallet.MarginBalance, asterWallet.WalletBalance
	asterMu.Unlock()

	realW3, realEq3, realWDelta, realEqDelta := 0.0, 0.0, 0.0, 0.0
	if ltCol > 10 && bnWallet > 50 {
		curW3 := ltCol + bnWallet + asWallet
		if curW3 > dynamic3VenueBaseline+25.0 {
			dynamic3VenueBaseline = curW3
		}
		realW3 = roundTo(curW3, 4)
		realEq3 = roundTo(ltEq+bnMargin+asMargin, 4)
		realWDelta = roundTo(realW3-dynamic3VenueBaseline, 4)
		realEqDelta = roundTo(realEq3-dynamic3VenueBaseline, 4)
	}

	liveMu.Lock()
	upnl := 0.0
	snaps := []LivePosView{}
	var syms, parts []string
	activeMakers := 0
	primaryState := "IDLE"
	slotSnaps := make([]SavedSlotSnap, 0, MaxLiveSlots)
	for _, sl := range liveSlots {
		if sl == nil {
			continue
		}
		slotSnaps = append(slotSnaps, SavedSlotSnap{
			SlotKey: fmt.Sprintf("%s:%d", liveAcct.ID, sl.Idx), StratID: liveAcct.ID, State: sl.State,
			Sym: sl.Sym, PeerVenue: "BN_USDC", PeerSym: sl.PeerSym, LtSide: sl.LtSide, PeerSide: sl.PeerSide,
			HedgedQty: sl.HedgedQty, EntryLtPx: sl.EntryLtPx, EntryPeerPx: sl.EntryPeerPx, EntryFx: sl.EntryFx,
			EntryPeerFee: 4.0, EntryFeeUsd: sl.EntryFeeUsd, OpenedMs: sl.OpenedAt.UnixMilli(),
			TradeID: sl.TradeID, ExitPeerOid: sl.ExitPeerOid, ExitPeerPx: sl.ExitPeerPx,
		})
		if sl.State == "LT_ENTRY_OPEN" || sl.State == "PEER_EXIT_OPEN" {
			activeMakers++
		}
		if sl.State != "IDLE" {
			if primaryState == "IDLE" || sl.State == "HEDGED" || sl.State == "PEER_EXIT_OPEN" {
				primaryState = sl.State
			}
			if sl.Sym != "" {
				syms = append(syms, sl.Sym)
				parts = append(parts, fmt.Sprintf("%s(%s)", sl.State, sl.Sym))
			}
		}
		if (sl.State == "HEDGED" || sl.State == "PEER_EXIT_OPEN") && sl.HedgedQty > 0 {
			upnl -= sl.EntryFeeUsd
			if st := mktStates[sl.Sym]; st != nil {
				st.mu.Lock()
				lb, la := st.ltBid, st.ltAsk
				st.mu.Unlock()
				ltSigned, mark := sl.HedgedQty, lb
				if sl.LtSide == "ASK" {
					ltSigned, mark = -sl.HedgedQty, la
				}
				if mark > 0 {
					if ltSigned > 0 {
						upnl += sl.HedgedQty * (mark - sl.EntryLtPx)
					} else {
						upnl += sl.HedgedQty * (sl.EntryLtPx - mark)
					}
				}
				snaps = append(snaps, LivePosView{Venue: "LT_STD", Symbol: sl.Sym, Qty: ltSigned, AvgPx: sl.EntryLtPx, OpenedMs: sl.OpenedAt.UnixMilli(), UpdatedMs: nowMs, Tag: sl.State})
			}
			if _, _, _, _, _, pBid, pAsk, _, _, fx, _, _, okB := getBnUsdcBook(sl.Sym, now); okB {
				bnSigned, pMark := sl.HedgedQty, pBid
				if sl.PeerSide == "SELL" {
					bnSigned, pMark = -sl.HedgedQty, pAsk
				}
				if pMark > 0 {
					exitFee := (sl.HedgedQty * pMark * fx) * (4.0 / 10000.0)
					if bnSigned > 0 {
						upnl += sl.HedgedQty*(pMark-sl.EntryPeerPx)*fx - exitFee
					} else {
						upnl += sl.HedgedQty*(sl.EntryPeerPx-pMark)*fx - exitFee
					}
				}
				snaps = append(snaps, LivePosView{Venue: "BN_USDC", Symbol: sl.PeerSym, Qty: bnSigned, AvgPx: sl.EntryPeerPx, OpenedMs: sl.OpenedAt.UnixMilli(), UpdatedMs: nowMs, Tag: sl.State})
			}
		}
	}

	liveAcct.UnrealizedPnL = roundTo(upnl, 4)
	liveAcct.Cash = roundTo(liveAcct.StartCash+liveAcct.RealizedGross-liveAcct.FeesPaid+liveAcct.FundingUsd, 4)
	liveAcct.ClosedNetUsd = roundTo(liveAcct.Cash-liveAcct.StartCash, 4)
	liveAcct.Equity = roundTo(liveAcct.Cash+upnl, 4)
	liveAcct.WithdrawableUsd = liveAcct.Equity
	if liveAcct.Equity > liveAcct.PeakEquity {
		liveAcct.PeakEquity = liveAcct.Equity
	}
	if dd := liveAcct.PeakEquity - liveAcct.Equity; dd > liveAcct.MaxDrawdownUsd {
		liveAcct.MaxDrawdownUsd = roundTo(dd, 4)
	}
	if liveAcct.Tier > 0 {
		if liveAcct.StratActiveMs <= 0 {
			liveAcct.StratActiveMs = nowMs
		}
		liveAcct.ActiveElapsedS = roundTo(math.Max(0, float64(nowMs-liveAcct.StratActiveMs)/1000.0), 1)
	}
	if liveAcct.TierChangedMs > 0 {
		liveAcct.TierElapsedS = roundTo(math.Max(0, float64(nowMs-liveAcct.TierChangedMs)/1000.0), 1)
	}
	if len(liveAcct.EquityCurve) == 0 || nowMs-liveAcct.EquityCurve[len(liveAcct.EquityCurve)-1].TsMs >= 30000 {
		liveAcct.EquityCurve = append(liveAcct.EquityCurve, LiveEqPoint{TsMs: nowMs, Cash: liveAcct.Cash, Eq: liveAcct.Equity, MidEq: liveAcct.Cash})
		if len(liveAcct.EquityCurve) > 540 {
			liveAcct.EquityCurve = append([]LiveEqPoint(nil), liveAcct.EquityCurve[len(liveAcct.EquityCurve)-540:]...)
		}
	}
	denomHr := math.Max(liveAcct.ActiveElapsedS/3600.0, 0.25)
	liveAcct.RealizedHourly = roundTo(liveAcct.ClosedNetUsd/denomHr, 3)
	if liveAcct.Tier <= 0 || liveAcct.RoundTrips == 0 {
		liveAcct.RealizedHourly = 0
	}
	liveAcct.HourlyRunRate = liveAcct.RealizedHourly
	liveAcct.Rolling4hHourly = liveAcct.RealizedHourly
	slotsN, clipNow, _ := tierSpec(liveAcct.Tier)
	if liveAcct.Tier <= 0 {
		liveAcct.Scaled489Hourly = roundTo(liveAcct.Forward45mHr, 2)
	} else {
		liveAcct.Scaled489Hourly = roundTo(liveAcct.RealizedHourly*(720.0/math.Max(float64(slotsN)*clipNow, 20.0)), 2)
	}
	liveAcct.ActiveMakers = activeMakers
	liveAcct.PosSnap = snaps
	liveAcct.State = primaryState
	liveAcct.ActiveSym = strings.Join(syms, ",")
	stateDisp := liveAcct.TierLabel
	if len(parts) > 0 {
		stateDisp += " [" + strings.Join(parts, "+") + "]"
	}
	liveAcct.StatusNote = fmt.Sprintf("%s | %s | Closed $%+.3f | Open $%+.3f | PF %.2f | MaxDD $%.2f/$8.00", stateDisp, activePreset.Label, liveAcct.ClosedNetUsd, upnl, liveAcct.ProfitFactor, liveAcct.MaxDrawdownUsd)

	acctCopy := *liveAcct
	revFills := make([]LiveFillAudit, len(liveFills))
	for i := range liveFills {
		revFills[i] = liveFills[len(liveFills)-1-i]
	}
	txLastMin := countLtTxLastMinLocked(nowMs)
	combNet := liveAcct.ClosedNetUsd
	liveMu.Unlock()

	if (combNet <= GlobalStopLossUSD || (realEq3 > 100 && realEqDelta <= GlobalStopLossUSD)) && breakerTripped.CompareAndSwap(false, true) {
		go flattenAndExitPause(fmt.Sprintf("STOP-LOSS Global PnL=$%+.2f (WalletDelta=$%+.2f) <= $%.2f", combNet, realEqDelta, GlobalStopLossUSD))
	}

	tel := LiveArenaTelemetry{
		Version: LiveArenaVer, BootTsMs: liveBootMs, NowTsMs: nowMs,
		UptimeSec: roundTo(upSec, 1), UptimeHours: roundTo(upHr, 3), Paused: remotePaused.Load(),
		LtTxLastMin: txLastMin, TotalCapitalUsd: roundTo(ltEq+bnMargin+asMargin, 2),
		LighterEqUsd: roundTo(ltEq, 2), BinanceMarginUsd: roundTo(bnMargin, 2), AsterMarginUsd: roundTo(asMargin, 2),
		Real3VenueWalletUsd: realW3, Real3VenueEquityUsd: realEq3, RealWalletDeltaUsd: realWDelta, RealEquityDeltaUsd: realEqDelta,
		CombinedNetUsd: combNet, CombinedHourly: roundTo(combNet/math.Max(upHr, 0.25), 3),
		Scaled489BestHr: acctCopy.Scaled489Hourly, BestStratID: acctCopy.ID, BestHourlyUsd: acctCopy.RealizedHourly,
		Strategies: []*LiveStratAccount{&acctCopy}, RecentFills: revFills, SlotSnapshots: slotSnaps,
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

func runLiveMicroArenaLoop() {
	initLiveMicroArena()
	time.Sleep(3 * time.Second)
	if remotePaused.Load() || !setupBnUsdcMargin() {
		return
	}
	_, _, _ = fetchAsterAccountMargin()
	emitBnAuditLine()
	sweepUntrackedLtOrphansSync()
	evaluateForwardWinnerAndTiers()
	writeLiveArenaTelemetry(time.Now())

	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
	lastMargin, lastOrphan, lastEval, lastTel := time.Now(), time.Now(), time.Now(), time.Now()

	// ONE single sequential loop: zero concurrent slot races, zero REST 429 storms!
	for {
		select {
		case <-liveWakeCh:
		case <-tick.C:
		}
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now()
		for i := 0; i < MaxLiveSlots; i++ {
			stepSimpleSlot(liveSlots[i], now)
		}
		if now.Sub(lastMargin) >= 5*time.Second {
			lastMargin = now
			_, _, _, _ = fetchBnAccountMargin()
		}
		if now.Sub(lastOrphan) >= 20*time.Second {
			lastOrphan = now
			sweepUntrackedLtOrphansSync()
			sweepUntrackedBnOrphansSync()
		}
		if now.Sub(lastEval) >= 30*time.Second {
			lastEval = now
			evaluateForwardWinnerAndTiers()
		}
		if now.Sub(lastTel) >= 1*time.Second {
			lastTel = now
			writeLiveArenaTelemetry(now)
		}
	}
}
