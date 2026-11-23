package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
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
	LiveArenaVer         = "LIVE-v108.0-TIERED-KISS"
	Baseline3VenueUSD    = 222.9000 // Exact 100% Flat 3-venue Perp cash ($90.69 LT + $82.31 BN Perp + $49.90 Aster; Spot $264.73 untouched!)
	MaxDemoteDrawdownUSD = 8.00     // -$8.00 Total-Equity Max Drawdown demotes strategy back to Tier 0 (Forward Testing Only)
	GlobalStopLossUSD    = -8.00    // -$8.00 ($214.90 true Perp floor) global 3-wallet circuit breaker
	MaxLtTxPerMin        = 48       // Strict guard below #770015 Standard 60 tx/min quota
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
	ForwardStratID  string        `json:"forwardStratId"`
	Name            string        `json:"name"`
	Desc            string        `json:"desc"`
	VenueDesc       string        `json:"venueDesc"`
	Tier            int           `json:"tier"` // 0=Forward Only ($0), 1=Micro (1x $10), 2=Medium (2x $25), 3=Full Safe Cap (2x $55 / 2x $35)
	TierLabel       string        `json:"tierLabel"`
	TierChangedMs   int64         `json:"tierChangedMs"`
	TierBaseTrips   int64         `json:"tierBaseTrips"`
	ClipUsd         float64       `json:"clipUsd"`
	Tier3ClipUsd    float64       `json:"tier3ClipUsd"`
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

type LiveArenaTelemetry struct {
	Version              string              `json:"version"`
	BootTsMs             int64               `json:"bootTsMs"`
	NowTsMs              int64               `json:"nowTsMs"`
	UptimeSec            float64             `json:"uptimeSec"`
	UptimeHours          float64             `json:"uptimeHours"`
	Paused               bool                `json:"paused"`
	LtTxLastMin          int                 `json:"ltTxLastMin"`
	TotalCapitalUsd      float64             `json:"totalCapitalUsd"`
	LighterEqUsd         float64             `json:"lighterEqUsd"`
	BinanceMarginUsd     float64             `json:"binanceMarginUsd"`
	AsterMarginUsd       float64             `json:"asterMarginUsd"`
	Real3VenueWalletUsd  float64             `json:"real3VenueWalletUsd"`
	Real3VenueEquityUsd  float64             `json:"real3VenueEquityUsd"`
	RealWalletDeltaUsd   float64             `json:"realWalletDeltaUsd"`
	RealEquityDeltaUsd   float64             `json:"realEquityDeltaUsd"`
	CombinedNetUsd       float64             `json:"combinedNetUsd"`
	CombinedHourly       float64             `json:"combinedHourlyUsd"`
	Scaled489BestHr      float64             `json:"scaled489BestHourlyUsd"`
	BestStratID          string              `json:"bestStratId"`
	BestHourlyUsd        float64             `json:"bestHourlyUsd"`
	Strategies           []*LiveStratAccount `json:"strategies"`
	RecentFills          []LiveFillAudit     `json:"recentFills"`
	MarketSpreads        []LiveSpreadSnap    `json:"marketSpreads"`
	SlotSnapshots        []SavedSlotSnap     `json:"slotSnapshots,omitempty"`
}

type LiveSlotRuntime struct {
	SlotKey        string
	SubIdx         int
	StratID        string
	PeerVenue      string // "BN_USDC" or "ASTER_C" or "ASTER_RWA"
	PeerMakerFirst bool   // true for S10/S1: rests 0% GTX Maker on BN/Aster first -> 0% Lighter IOC hedge!
	Symbols        []string
	ClipUsd        float64
	ScaleMult489   float64 // Full-$489 Arena slotUsd / micro ClipUsd
	MinRawEdgeBp   float64 // Strict SellPx > BuyPx positive raw spread required above fees!
	EntryDisloc    float64
	TakerGrabBp    float64 // >0 enables S9 Pure Crossed-Book Taker-Grab!
	ExitNetBp      float64
	CancelTolBp    float64
	MaxRestSec     float64
	MaxHoldSec     float64

	// Live execution state machine (IDLE -> PEER_ENTRY_OPEN / LT_ENTRY_OPEN -> HEDGING_LT -> HEDGED -> PEER_EXIT_OPEN)
	State       string
	Sym         string
	PeerSym     string
	LtSide      string // "BID" (BUY) or "ASK" (SELL) on Lighter leg
	PeerSide    string // "SELL" or "BUY" on Peer leg
	LtCOI       int64
	LtLimitPx   float64
	LtOrderQty  float64
	EntryPeerOid int64
	PlacedAt    time.Time
	CancelSent  bool
	CancelAt    time.Time

	HedgedQty    float64
	EntryLtPx    float64
	EntryPeerPx  float64
	EntryFx      float64
	EntryPeerFee float64
	EntryFeeUsd  float64
	OpenedAt     time.Time
	TradeID      int64

	ExitPeerOid int64
	ExitPeerPx  float64
	ExitPlaced  time.Time
	LastOrdPoll   time.Time
	LastLtRestBBO time.Time
	CoolUntil     time.Time

	// IDLE-Safe Rotation Queue: never swaps symbols/venues mid-position!
	OrigPeerVenue string
	OrigSymbols   []string
	PendingVenue  string
	PendingSyms   []string
	PendingGrabBp float64
	LastSwapAt    time.Time
	RotatedNote   string
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

var (
	liveMu           sync.Mutex
	liveBootMs       = time.Now().UnixMilli()
	liveStrats       = map[string]*LiveStratAccount{}
	liveStratOrder   = []string{}
	liveSlots        = map[string]*LiveSlotRuntime{} // key: SlotKey (e.g. StratID+":0", StratID+":1")
	liveSlotKeys     = []string{}
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

func tierLabelFor(tier int, t3Clip float64) string {
	switch tier {
	case 1:
		return "Tier 1: Micro (1x $10)"
	case 2:
		return "Tier 2: Medium (2x $25)"
	case 3:
		return fmt.Sprintf("Tier 3: Full Safe (2x $%.0f)", t3Clip)
	default:
		return "Tier 0: Forward Only ($0)"
	}
}

func applyTierClipLocked(acct *LiveStratAccount) {
	if acct == nil {
		return
	}
	acct.TierLabel = tierLabelFor(acct.Tier, acct.Tier3ClipUsd)
	switch acct.Tier {
	case 1:
		acct.ClipUsd = 10.0
	case 2:
		acct.ClipUsd = 25.0
	case 3:
		acct.ClipUsd = acct.Tier3ClipUsd
	default:
		acct.ClipUsd = 0.0
	}
	for sub := 0; sub < 2; sub++ {
		rt := liveSlots[fmt.Sprintf("%s:%d", acct.ID, sub)]
		if rt == nil {
			continue
		}
		if acct.Tier <= 0 || (acct.Tier == 1 && sub > 0) {
			rt.ClipUsd = 0.0
			rt.ScaleMult489 = 0.0
		} else {
			rt.ClipUsd = acct.ClipUsd
			activeUsd := acct.ClipUsd
			if acct.Tier >= 2 {
				activeUsd = acct.ClipUsd * 2.0
			}
			rt.ScaleMult489 = 720.0 / math.Max(activeUsd, 10.0)
		}
	}
}

func initLiveMicroArena() {
	liveMu.Lock()
	if len(liveStratOrder) > 0 {
		liveMu.Unlock()
		return
	}

	nowInitMs := time.Now().UnixMilli()
	defs := []struct {
		id             string
		forwardID      string
		name           string
		venueDesc      string
		desc           string
		peerVenue      string
		peerMakerFirst bool
		tier3Clip      float64
		startCash      float64
		minRawEdgeBp   float64
		entryBp        float64
		takerGrabBp    float64
		exitBp         float64
		syms           []string
	}{
		{
			id:             "LIVE_S10_BN_MAKER_FIRST",
			forwardID:      "S10_BN_USDC_MAKER_FIRST",
			name:           "S10: 0% BN USDC-M Maker -> 0% Lighter IOC (#1 Leader)",
			venueDesc:      "BN USDC-M 0% GTX -> LT #770015 0% IOC (0.00bp 4-Leg Fee!)",
			desc:           "Primary Slot #1: Rests 0% GTX Maker on BN USDC-M (>=5.0bp positive raw spread) -> 0% Lighter IOC hedge (850ms fallback) -> 0% BN Maker exit.",
			peerVenue:      "BN_USDC",
			peerMakerFirst: true,
			tier3Clip:      55.0,
			startCash:      60.0,
			minRawEdgeBp:   5.0,
			entryBp:        5.0,
			exitBp:         2.5,
			syms:           []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "TRUMP", "TIA", "CRV", "ETHFI", "LINK", "DOGE", "XRP"},
		},
		{
			id:           "LIVE_S7_BN_HIGH_CONV",
			forwardID:    "S7_BN_HIGH_CONV_RAW",
			name:         "S7: High-Conviction >=8.5bp Raw Spread (LT <-> BN USDC-M)",
			venueDesc:    "LT #770015 0% M -> BN USDC-M (4.00bp Fee, Zero FX Drift)",
			desc:         "Primary Slot #2: 0% Lighter Maker (>=8.5bp positive raw spread) -> <8ms BN USDC-M hedge -> 0% BN Maker exit.",
			peerVenue:    "BN_USDC",
			tier3Clip:    55.0,
			startCash:    60.0,
			minRawEdgeBp: 4.5,
			entryBp:      8.5,
			exitBp:       3.5,
			syms:         []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "LINK", "DOGE", "XRP"},
		},
		{
			id:           "LIVE_S9_PURE_CROSSED_GRAB",
			forwardID:    "S9_PURE_CROSSED_TAKER",
			name:         "S9: Pure Crossed-Book Taker Grab (Bid_A - Ask_B >= +8.5bp)",
			venueDesc:    "LT #770015 0% IOC <-> BN USDC-M (+4.5bp Locked at Entry!)",
			desc:         "Burst Slot #3: Fires 0% Lighter IOC + <8ms BN USDC-M hedge ONLY when true crossed book >= +8.5bp -> 0% BN Maker exit.",
			peerVenue:    "BN_USDC",
			tier3Clip:    50.0,
			startCash:    50.0,
			minRawEdgeBp: 4.5,
			entryBp:      8.5,
			takerGrabBp:  8.5,
			exitBp:       2.5,
			syms:         []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "TRUMP", "TIA", "CRV", "ETHFI", "LINK", "DOGE", "XRP"},
		},
		{
			id:             "LIVE_S1_ASTER_RWA_MAKER",
			forwardID:      "S1_ASTER_RWA_MAKER_FIRST",
			name:           "S1: 0% Aster RWA Equities Maker -> 0% Lighter IOC",
			venueDesc:      "Aster RWA 0% GTX <-> LT #770015 0% IOC (0.00bp 4-Leg Fee!)",
			desc:           "US Equity Slot #4: 0% GTX Maker on Aster RWA Equities (>=4.5bp positive raw spread) -> 0% Lighter IOC hedge -> 0% Aster Maker exit.",
			peerVenue:      "ASTER_RWA",
			peerMakerFirst: true,
			tier3Clip:      35.0,
			startCash:      50.0,
			minRawEdgeBp:   4.5,
			entryBp:        4.5,
			exitBp:         2.5,
			syms:           []string{"NVDA", "AAPL", "TSLA", "QQQ", "SPY", "META", "COIN", "MSTR"},
		},
	}

	for _, d := range defs {
		liveStratOrder = append(liveStratOrder, d.id)
		acct := &LiveStratAccount{
			ID:             d.id,
			ForwardStratID: d.forwardID,
			Name:           d.name,
			Desc:           d.desc,
			VenueDesc:      d.venueDesc,
			Tier:           0, // Starts strictly in Tier 0 (Forward Testing Only) until 45m $5+/hr & PF >= 1.35 proven!
			TierLabel:      tierLabelFor(0, d.tier3Clip),
			TierChangedMs:  nowInitMs,
			Tier3ClipUsd:   d.tier3Clip,
			ClipUsd:        0.0,
			StartCash:      d.startCash,
			Cash:           d.startCash,
			Equity:         d.startCash,
			PeakEquity:     d.startCash,
			State:          "IDLE",
		}
		liveStrats[d.id] = acct
		for sub := 0; sub < 2; sub++ {
			sk := fmt.Sprintf("%s:%d", d.id, sub)
			liveSlotKeys = append(liveSlotKeys, sk)
			liveSlots[sk] = &LiveSlotRuntime{
				SlotKey:        sk,
				SubIdx:         sub,
				StratID:        d.id,
				PeerVenue:      d.peerVenue,
				PeerMakerFirst: d.peerMakerFirst,
				Symbols:        append([]string(nil), d.syms...),
				OrigPeerVenue:  d.peerVenue,
				OrigSymbols:    append([]string(nil), d.syms...),
				ClipUsd:        0.0,
				ScaleMult489:   0.0,
				MinRawEdgeBp:   d.minRawEdgeBp,
				EntryDisloc:    d.entryBp,
				TakerGrabBp:    d.takerGrabBp,
				ExitNetBp:      d.exitBp,
				CancelTolBp:    2.0,
				MaxRestSec:     25.0,
				MaxHoldSec:     900.0,
				State:          "IDLE",
			}
		}
		applyTierClipLocked(acct)
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
	// Clean zero-history cutover to LIVE-v108.0-TIERED-KISS:
	if json.Unmarshal(raw, &saved) != nil || !strings.HasPrefix(saved.Version, "LIVE-v108.") || saved.BootTsMs <= 0 {
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
		dst.Tier = st.Tier
		dst.TierChangedMs = st.TierChangedMs
		dst.TierBaseTrips = st.TierBaseTrips
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
		dst.GrossWinUsd = st.GrossWinUsd
		dst.GrossLossUsd = st.GrossLossUsd
		dst.ProfitFactor = st.ProfitFactor
		dst.RecentTripPnls = st.RecentTripPnls
		dst.GrossVolumeUsd = st.GrossVolumeUsd
		dst.EquityCurve = st.EquityCurve
		dst.Halted = st.Halted
		dst.HaltReason = st.HaltReason
		applyTierClipLocked(dst)
	}
	for _, sn := range saved.SlotSnapshots {
		sk := sn.SlotKey
		if sk == "" {
			sk = sn.StratID + ":0"
		}
		rt := liveSlots[sk]
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
		if rt.EntryFx <= 0 {
			rt.EntryFx = 1.0
		}
		rt.EntryPeerFee = sn.EntryPeerFee
		rt.EntryFeeUsd = sn.EntryFeeUsd
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
	fmt.Printf("[%s] Hot-reloaded tiered live arena state seamlessly (uptime %.1fm)\n", LiveArenaVer, float64(time.Now().UnixMilli()-liveBootMs)/60000.0)
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

func isSymUsedByAnySlot(sym, excludeSlotKey string) bool {
	liveMu.Lock()
	defer liveMu.Unlock()
	for k, other := range liveSlots {
		if k != excludeSlotKey && other != nil && other.State != "IDLE" && other.Sym == sym {
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
	return placeLtPostOnlyOrderSync(sym, side, px, qty, false)
}

func placeLtPostOnlyOrderSync(sym, side string, px, qty float64, reduceOnly bool) (coi int64, ok bool) {
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
	var isAsk, ro uint8
	if side == "ASK" {
		isAsk = 1
	}
	if reduceOnly {
		ro = 1
	}
	coi = coiSeq.Add(1)
	exp := time.Now().Add(24 * time.Hour).UnixMilli()
	dbg := fmt.Sprintf("LT_PO_%s_%s px=%.5f sz=%.5f ro=%d", sym, side, pxF, qty, ro)
	err := sendSignedTx(fmt.Sprintf("po_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      m.ID,
			ClientOrderIndex: coi,
			BaseAmount:       baseInt,
			Price:            pxInt,
			IsAsk:            isAsk,
			Type:             0, // LIMIT
			TimeInForce:      2, // POST_ONLY (0% Maker strictly!)
			ReduceOnly:       ro,
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

func peerTopDepthUsd(peerVenue, peerSym, hedgeSide string) float64 {
	if peerVenue == "BN_USDC" {
		dnMu.Lock()
		db := dnBooks[peerSym]
		var usd float64
		if db != nil {
			if hedgeSide == "BUY" {
				usd = db.Ask * db.AskQ
			} else {
				usd = db.Bid * db.BidQ
			}
		}
		dnMu.Unlock()
		return usd
	}
	asterMu.Lock()
	db := asterBooks[peerSym]
	var usd float64
	if db != nil {
		if hedgeSide == "BUY" {
			usd = db.Ask * db.AskQ
		} else {
			usd = db.Bid * db.BidQ
		}
	}
	asterMu.Unlock()
	return usd
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
		sc, body, err = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
	} else {
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

func placePeerMakerOrder(peerVenue, peerSym, side string, qty, px float64, qDec, pxDec int, reduceOnly bool) (oid int64, ok bool) {
	p := url.Values{}
	p.Set("symbol", peerSym)
	p.Set("side", side)
	p.Set("type", "LIMIT")
	p.Set("timeInForce", "GTX") // PostOnly 0% Maker on both Binance & AsterDEX!
	if reduceOnly {
		p.Set("reduceOnly", "true")
	}
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

func placePeerMakerExit(peerVenue, peerSym, side string, qty, px float64, qDec, pxDec int) (oid int64, ok bool) {
	return placePeerMakerOrder(peerVenue, peerSym, side, qty, px, qDec, pxDec, true)
}

func venueActiveNotionalLocked(peerVenue string) (ltNotional, bnNotional, asterNotional float64) {
	for _, rt := range liveSlots {
		if rt == nil || rt.State == "IDLE" {
			continue
		}
		u := rt.ClipUsd
		if rt.HedgedQty > 0 && rt.EntryLtPx > 0 {
			u = rt.HedgedQty * rt.EntryLtPx
		}
		ltNotional += u
		if rt.PeerVenue == "BN_USDC" {
			bnNotional += u
		} else {
			asterNotional += u
		}
	}
	return ltNotional, bnNotional, asterNotional
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
	qty := math.Abs(curPos)
	if qty < m.MinB {
		return exitEstPx, 0
	}

	// STAGE 0 (0ms speed-bump, 0% Maker, +spread bonus!):
	// Try 550ms ReduceOnly PostOnly Maker close on Lighter BBO first before paying any IOC crossing spread!
	poSide := "BID"
	poPx := roundTo(lb, m.PxDec)
	if curPos > 0 {
		poSide = "ASK"
		poPx = roundTo(la, m.PxDec)
	}
	if poPx > 0 {
		liveMu.Lock()
		allowPO := canSendLtTxLocked(time.Now().UnixMilli(), false)
		if allowPO {
			recordLtTxLocked(time.Now().UnixMilli())
		}
		liveMu.Unlock()
		if allowPO {
			if poCoi, okPO := placeLtPostOnlyOrderSync(sym, poSide, poPx, qty, true); okPO {
				poDeadline := time.Now().Add(550 * time.Millisecond)
				for time.Now().Before(poDeadline) {
					if st := mktStates[sym]; st != nil {
						st.mu.Lock()
						pNow := st.pos
						st.mu.Unlock()
						if math.Abs(pNow) < m.MinB {
							return poPx, time.Since(t0).Milliseconds()
						}
					}
					select {
					case <-ltFillCh:
					case <-time.After(30 * time.Millisecond):
					}
				}
				cancelLtOrderByCOISync(sym, poCoi)
				liveMu.Lock()
				recordLtTxLocked(time.Now().UnixMilli())
				liveMu.Unlock()
				time.Sleep(80 * time.Millisecond)
				if rp, _ := fetchLtPosREST(sym); math.Abs(rp) < m.MinB {
					return poPx, time.Since(t0).Milliseconds()
				} else {
					curPos = rp
					qty = math.Abs(rp)
				}
			}
		}
	}

	// STAGE 1: Tightly-bounded (8bp max slip) ReduceOnly IOC sweep — NEVER 50bp/150bp!
	if rb, ra := fetchLtBookREST(m.ID); rb > 0 && ra > rb {
		lb, la = rb, ra
	}
	closeSide := "BID"
	closePx := roundTo(la*1.0008, m.PxDec)
	exitEstPx = la
	if curPos > 0 {
		closeSide = "ASK"
		closePx = roundTo(lb*0.9992, m.PxDec)
		exitEstPx = lb
	}

	liveMu.Lock()
	recordLtTxLocked(time.Now().UnixMilli())
	liveMu.Unlock()

	placeLtHedgeIOC(sym, closeSide, closePx, qty, true)

	cDeadline := time.Now().Add(950 * time.Millisecond)
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
		if !restChecked && time.Since(t0) >= 550*time.Millisecond {
			restChecked = true
			if rp, _ := fetchLtPosREST(sym); math.Abs(rp) < m.MinB {
				return exitEstPx, time.Since(t0).Milliseconds()
			}
		}
		select {
		case <-ltFillCh:
		case <-time.After(40 * time.Millisecond):
		}
	}

	// STAGE 2: Final 20bp-bounded fallback (replaces destructive 150bp market sweep):
	if remPos, _ := fetchLtPosREST(sym); math.Abs(remPos) >= m.MinB {
		lb2, la2 := fetchLtBookREST(m.ID)
		if lb2 <= 0 || la2 <= 0 {
			lb2, la2 = lb, la
		}
		rSide := "BID"
		rPx := roundTo(la2*1.0020, m.PxDec)
		if remPos > 0 {
			rSide = "ASK"
			rPx = roundTo(lb2*0.9980, m.PxDec)
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

func fetchExactLtCloseTrade(sym string, minTsMs int64) (vwapPx, realPnl float64, found bool) {
	hc := &http.Client{Timeout: 3 * time.Second}
	u := fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=15&sort_by=timestamp", BaseURL, AcctIdx)
	resp, err := ltGet(hc, u)
	if err != nil {
		return 0, 0, false
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
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
		return 0, 0, false
	}
	m := markets[sym]
	var cost, totSz, sumPnl float64
	for _, t := range tr.Trades {
		ts := t.Timestamp
		if ts > 0 && ts < 100000000000 {
			ts *= 1000
		}
		if ts < minTsMs-3000 {
			continue
		}
		if t.Sym != sym && t.MarketID != m.ID {
			continue
		}
		var pnlStr string
		if t.BidAcct == AcctIdx {
			pnlStr = t.BidPnl
		} else if t.AskAcct == AcctIdx {
			pnlStr = t.AskPnl
		} else {
			continue
		}
		if strings.TrimSpace(pnlStr) == "" {
			continue // opening fill, not closing fill
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
		vwapPx = cost / totSz
	}
	return vwapPx, sumPnl, found
}

func refreshLtRestBBOIfDue(sym string, last *time.Time, minInterval time.Duration) (lb, la float64) {
	st := mktStates[sym]
	m := markets[sym]
	if st != nil {
		st.mu.Lock()
		lb, la = st.ltBid, st.ltAsk
		st.mu.Unlock()
	}
	if time.Since(*last) >= minInterval {
		*last = time.Now()
		if rb, ra := fetchLtBookREST(m.ID); rb > 0 && ra > rb {
			lb, la = rb, ra
			if st != nil {
				st.mu.Lock()
				st.bookBids = map[string]float64{strconv.FormatFloat(rb, 'f', m.PxDec, 64): math.Max(st.ltBidSz, m.MinB*5)}
				st.bookAsks = map[string]float64{strconv.FormatFloat(ra, 'f', m.PxDec, 64): math.Max(st.ltAskSz, m.MinB*5)}
				st.ltBid, st.ltAsk = rb, ra
				st.lastBookTS = time.Now()
				st.mu.Unlock()
			}
		}
	}
	return lb, la
}

func finalizeSlotClose(rt *LiveSlotRuntime, peerExitPx, peerExitFeeBp float64, execType, reason string, exitLatMs int64, now time.Time) {
	closeStartMs := time.Now().UnixMilli()
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
	entryFeeUsd := rt.EntryFeeUsd
	if entryFeeUsd <= 0 && rt.EntryPeerFee > 0 {
		entryFeeUsd = (qty * rt.EntryPeerPx * fx) * (rt.EntryPeerFee / 10000.0)
	}
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
	// PENNY-EXACT EXCHANGE RECONCILIATION: Pull exact zkLighter fill price & realized PnL directly from /api/v1/trades!
	time.Sleep(220 * time.Millisecond)
	if exPx, exLtPnl, okEx := fetchExactLtCloseTrade(rt.Sym, closeStartMs); okEx {
		ltExitPx = exPx
		ltGross = exLtPnl
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
		// Book BOTH entryFeeUsd + exitFeeUsd (totalFeeUsd) and grossTotal ONLY when the round-trip closes so 0/0 closed ALWAYS shows $0.0000!
		acct.RealizedGross = roundTo(acct.RealizedGross+grossTotal, 4)
		acct.FeesPaid = roundTo(acct.FeesPaid+totalFeeUsd, 4)
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
			acct.GrossWinUsd = roundTo(acct.GrossWinUsd+netUsd, 4)
		} else if netUsd < 0 {
			acct.GrossLossUsd = roundTo(acct.GrossLossUsd+math.Abs(netUsd), 4)
		}
		acct.RecentTripPnls = append(acct.RecentTripPnls, roundTo(netUsd, 4))
		if len(acct.RecentTripPnls) > 12 {
			acct.RecentTripPnls = append([]float64(nil), acct.RecentTripPnls[len(acct.RecentTripPnls)-12:]...)
		}
		if acct.GrossLossUsd > 1e-6 {
			acct.ProfitFactor = roundTo(acct.GrossWinUsd/acct.GrossLossUsd, 2)
		} else if acct.GrossWinUsd > 0 {
			acct.ProfitFactor = 99.0
		} else {
			acct.ProfitFactor = 0.0
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
	rt.EntryPeerOid = 0
	rt.ExitPeerOid = 0
	rt.HedgedQty = 0
	rt.EntryFeeUsd = 0
	rt.CoolUntil = now.Add(3 * time.Second)
	liveMu.Unlock()

	recordStratClose(rt.TradeID, rt.StratID,
		fmt.Sprintf("LT_%s_%s+%s_%s", rt.Sym, rt.LtSide, rt.PeerVenue, rt.PeerSide),
		rt.Sym, notional, holdSec, netBps, netUsd, peerGross-totalFeeUsd, ltGross, totalFeeUsd, reason)
}

func stepLiveSlot(rt *LiveSlotRuntime, now time.Time) {
	liveMu.Lock()
	acct := liveStrats[rt.StratID]
	halted := acct != nil && (acct.Halted || acct.Tier <= 0)
	clipUsd := rt.ClipUsd
	liveMu.Unlock()
	if acct == nil {
		return
	}

	// -------------------------------------------------------------------------
	// STATE 1A: PEER_ENTRY_OPEN (S10 / S1 Flipped Maker-First: 0% GTX Maker on BN_USDC / ASTER_RWA -> 0% Lighter IOC!)
	// -------------------------------------------------------------------------
	if rt.State == "PEER_ENTRY_OPEN" {
		m := markets[rt.Sym]
		peerSym, qDec, _, _, _, _, mFeeBp, tFeeBp, pBid, pAsk, _, jumpBps, fx, _, _, okP := getPeerSpecAndBook(rt.PeerVenue, rt.Sym, now)
		if !okP {
			peerSym = rt.PeerSym
			fx = 1.0
		}
		lb, la := refreshLtRestBBOIfDue(rt.Sym, &rt.LastLtRestBBO, 1000*time.Millisecond)

		var status string
		var execQ, avgP float64
		var polled, okQ bool
		if rt.PeerVenue == "BN_USDC" {
			status, execQ, avgP, polled, okQ = queryBnOrder(peerSym, rt.EntryPeerOid, rt.LastOrdPoll, false)
		} else {
			status, execQ, avgP, polled, okQ = queryAsterOrder(peerSym, rt.EntryPeerOid, rt.LastOrdPoll, false)
		}
		if polled {
			rt.LastOrdPoll = now
		}
		if !okQ {
			return
		}
		if avgP <= 0 {
			avgP = rt.LtLimitPx
		}

		// If partially or fully filled >= MinB, cancel any remainder and immediately fire 0% Lighter IOC hedge (with 850ms fallback)!
		if status == "FILLED" || (execQ >= m.MinB && (status == "PARTIALLY_FILLED" || now.Sub(rt.PlacedAt).Seconds() >= rt.MaxRestSec || halted)) {
			if status != "FILLED" {
				_, cQty, cPx := cancelPeerOrder(rt.PeerVenue, peerSym, rt.EntryPeerOid)
				if cQty > execQ {
					execQ = cQty
				}
				if cPx > 0 {
					avgP = cPx
				}
			}
			liveMu.Lock()
			rt.State = "HEDGING_LT" // Locks rt.Sym against 30s orphan sweeper while 385ms Lighter IOC settles!
			recordLtTxLocked(now.UnixMilli())
			liveMu.Unlock()

			rb, ra := fetchLtBookREST(m.ID)
			if rb <= 0 || ra <= rb {
				rb, ra = lb, la
			}
			var iocPx, ltSigPx float64
			if rt.LtSide == "ASK" {
				ltSigPx = rb
				iocPx = roundTo(rb*0.9988, m.PxDec) // 12bp bounded IOC
			} else {
				ltSigPx = ra
				iocPx = roundTo(ra*1.0012, m.PxDec)
			}
			tIoc0 := time.Now()
			placeLtHedgeIOC(rt.Sym, rt.LtSide, iocPx, execQ, false)
			ltPos, ltAvg := waitLtPositionFill(rt.Sym, m.MinB, 850*time.Millisecond)
			ltLatMs := time.Since(tIoc0).Milliseconds()

			// 850ms Unhedged Fallback: if Lighter IOC missed due to speed-bump book move, immediately flatten BN/Aster leg (<8ms)!
			if math.Abs(ltPos) < m.MinB {
				flatSide := "SELL"
				if rt.PeerSide == "SELL" {
					flatSide = "BUY"
				}
				_, apFlat, _, _ := executePeerMarketOrder(rt.PeerVenue, peerSym, flatSide, execQ, qDec, true)
				flatPnl := 0.0
				if apFlat > 0 {
					if rt.PeerSide == "BUY" {
						flatPnl = (apFlat - avgP) * fx * execQ
					} else {
						flatPnl = (avgP - apFlat) * fx * execQ
					}
				}
				flatFee := (execQ * math.Max(apFlat, avgP) * fx) * (tFeeBp / 10000.0)
				liveMu.Lock()
				acct.MakerFills++
				acct.TakerFills++
				acct.RealizedGross = roundTo(acct.RealizedGross+flatPnl, 4)
				acct.FeesPaid = roundTo(acct.FeesPaid+flatFee, 4)
				acct.Cash = roundTo(acct.StartCash+acct.RealizedGross-acct.FeesPaid+acct.FundingUsd, 4)
				rt.State = "IDLE"
				rt.Sym = ""
				rt.PeerSym = ""
				rt.EntryPeerOid = 0
				rt.CoolUntil = time.Now().Add(8 * time.Second)
				liveMu.Unlock()
				fmt.Printf("[S10_IOC_MISS_FALLBACK sym=%s execQ=%.4f flatPnl=$%+.4f fee=$%.4f]\n", rt.Sym, execQ, flatPnl, flatFee)
				return
			}

			hedgedQ := math.Min(execQ, math.Abs(ltPos))
			if execQ-hedgedQ >= m.MinB {
				flatSide := "SELL"
				if rt.PeerSide == "SELL" {
					flatSide = "BUY"
				}
				_, _, _, _ = executePeerMarketOrder(rt.PeerVenue, peerSym, flatSide, roundTo(execQ-hedgedQ, qDec), qDec, true)
			}
			if ltAvg <= 0 {
				ltAvg = ltSigPx
			}
			notional := hedgedQ * ltAvg
			entryFeeUsd := (hedgedQ * avgP * fx) * (mFeeBp / 10000.0) // 0.00bp!
			ltSideHuman := "BUY"
			if rt.LtSide == "ASK" {
				ltSideHuman = "SELL"
			}
			tid := time.Now().UnixMilli()
			liveMu.Lock()
			acct.MakerFills++
			acct.TakerFills++
			acct.GrossVolumeUsd = roundTo(acct.GrossVolumeUsd+notional*2.0, 2)
			appendLiveFillLocked(LiveFillAudit{
				TsMs:      tid,
				StratID:   rt.StratID,
				Venue:     rt.PeerVenue,
				Symbol:    peerSym,
				Side:      rt.PeerSide,
				ExecType:  "PEER_GTX_0FEE_MAKER",
				FillPx:    avgP,
				SignalPx:  rt.LtLimitPx,
				Qty:       hedgedQ,
				Notional:  roundTo(hedgedQ*avgP*fx, 2),
				FeeUsd:    roundTo(entryFeeUsd, 4),
				LatencyMs: now.Sub(rt.PlacedAt).Milliseconds(),
				Tag:       "S10_MAKER_FIRST",
			})
			appendLiveFillLocked(LiveFillAudit{
				TsMs:      tid,
				StratID:   rt.StratID,
				Venue:     "LT_STD",
				Symbol:    rt.Sym,
				Side:      ltSideHuman,
				ExecType:  "LT_IOC_0FEE_HEDGE",
				FillPx:    ltAvg,
				SignalPx:  ltSigPx,
				Qty:       hedgedQ,
				Notional:  roundTo(notional, 2),
				FeeUsd:    0.0,
				LatencyMs: ltLatMs,
				Tag:       "LT_HEDGE",
			})
			rt.State = "HEDGED"
			rt.HedgedQty = hedgedQ
			rt.EntryLtPx = ltAvg
			rt.EntryPeerPx = avgP
			rt.EntryFx = fx
			rt.EntryPeerFee = mFeeBp // 0.0bp!
			rt.EntryFeeUsd = roundTo(entryFeeUsd, 4)
			rt.EntryPeerOid = 0
			rt.OpenedAt = time.Now()
			rt.TradeID = tid
			liveMu.Unlock()

			lockedEdgeBp := ((ltAvg - avgP*fx) / (avgP * fx) * 10000.0) - mFeeBp
			if rt.LtSide == "BID" {
				lockedEdgeBp = ((avgP*fx - ltAvg) / (avgP * fx) * 10000.0) - mFeeBp
			}
			recordStratOpen(tid, rt.StratID,
				fmt.Sprintf("MAKER_FIRST_%s_%s+LT_%s", peerSym, rt.PeerSide, rt.LtSide),
				rt.Sym, notional, lockedEdgeBp,
				fmt.Sprintf("MakerFirst Peer0pct@%.5f+LtIOC@%.5f(%dms)", avgP, ltAvg, ltLatMs))
			return
		}

		if status == "CANCELED" || status == "EXPIRED" {
			liveMu.Lock()
			rt.State = "IDLE"
			rt.Sym = ""
			rt.PeerSym = ""
			rt.EntryPeerOid = 0
			rt.CoolUntil = now.Add(1500 * time.Millisecond)
			liveMu.Unlock()
			return
		}

		// Cancel resting Peer GTX Maker if raw spread compressed below fee+2.0bp or timed out:
		rawEdgeNow := 0.0
		if lb > 0 && la > lb && avgP > 0 {
			if rt.PeerSide == "BUY" {
				rawEdgeNow = ((lb - avgP*fx) / (avgP * fx)) * 10000.0
			} else {
				rawEdgeNow = ((avgP*fx - la) / (avgP * fx)) * 10000.0
			}
		}
		refPeer := pBid
		if rt.PeerSide == "SELL" {
			refPeer = pAsk
		}
		driftBp := 0.0
		if refPeer > 0 {
			driftBp = math.Abs(rt.LtLimitPx-refPeer) / refPeer * 10000.0
		}
		if halted || now.Sub(rt.PlacedAt).Seconds() >= rt.MaxRestSec || rawEdgeNow < 2.0 || driftBp > rt.CancelTolBp || math.Abs(jumpBps) > 2.2 {
			cSt, cQty, cPx := cancelPeerOrder(rt.PeerVenue, peerSym, rt.EntryPeerOid)
			liveMu.Lock()
			acct.MakerCancelled++
			liveMu.Unlock()
			if cSt == "FILLED" || cQty >= m.MinB {
				if cPx > 0 {
					rt.LtLimitPx = cPx
				}
				return // next tick catches fill or sweeps cleanly
			}
			liveMu.Lock()
			rt.State = "IDLE"
			rt.Sym = ""
			rt.PeerSym = ""
			rt.EntryPeerOid = 0
			rt.CoolUntil = now.Add(1500 * time.Millisecond)
			liveMu.Unlock()
		}
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
			// Hold entryFeeUsd inside Open Hedged Basis (rt.EntryFeeUsd) until round-trip close so 0/0 closed trips ALWAYS reads $0.0000!
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
				Realized:  0.0,
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
			rt.EntryFeeUsd = roundTo(entryFeeUsd, 4)
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

		// If CancelSent is already in flight (wait 1.5s past speed-bump + zkLighter settlement so late fills NEVER escape unhedged!):
		if rt.CancelSent {
			if now.Sub(rt.CancelAt) >= 1500*time.Millisecond {
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
		// Refresh live Lighter REST BBO every 1.2s while HEDGED so stale WS ghost levels can NEVER trigger fake convergence!
		lb, la := refreshLtRestBBOIfDue(rt.Sym, &rt.LastLtRestBBO, 1200*time.Millisecond)
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

		// Anti-Drift & Liquidation Guard: auto-flatten if single leg drifts >= 10%, strategy demoted/halted, or venue margin >= 65%:
		legDrift := 0.0
		if rt.EntryLtPx > 0 {
			ltMid := 0.5 * (lb + la)
			legDrift = math.Abs(ltMid-rt.EntryLtPx) / rt.EntryLtPx
		}
		dnMu.Lock()
		bnAvailChk := dnSharedAvail
		bnTotChk := bnTotalMargin
		dnMu.Unlock()
		asterMu.Lock()
		asAvailChk := asterAvail
		asTotChk := asterWallet.MarginBalance
		asterMu.Unlock()
		marginStress := (rt.PeerVenue == "BN_USDC" && bnTotChk > 10 && bnAvailChk/bnTotChk < 0.35) ||
			(rt.PeerVenue != "BN_USDC" && asTotChk > 10 && asAvailChk/asTotChk < 0.35)

		if halted || legDrift >= 0.10 || marginStress {
			reasonTag := "TIER_DEMOTE_OR_DRIFT_FLUSH"
			if legDrift >= 0.10 {
				reasonTag = "LEG_DRIFT_10PCT_GUARD"
			} else if marginStress {
				reasonTag = "VENUE_MARGIN_65PCT_GUARD"
			}
			_, ap, latMs, okM := executePeerMarketOrder(rt.PeerVenue, peerSym, closePeerSide, rt.HedgedQty, qDec, true)
			if okM {
				if ap <= 0 {
					ap = 0.5 * (pBid + pAsk)
				}
				finalizeSlotClose(rt, ap, tFeeBp, "TAKER_HALT_FLUSH", reasonTag, latMs, now)
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
			// Final live Lighter REST BBO confirmation right before placing Peer GTX exit:
			m := markets[rt.Sym]
			if rb, ra := fetchLtBookREST(m.ID); rb > 0 && ra > rb {
				lb, la = rb, ra
				if rt.PeerSide == "BUY" {
					combMakerNetBp = (((exitMakerPx-rt.EntryPeerPx)*fx)+(rt.EntryLtPx-la))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
				} else {
					combMakerNetBp = (((rt.EntryPeerPx-exitMakerPx)*fx)+(lb-rt.EntryLtPx))/rt.EntryLtPx*10000.0 - rtMakerFeeBp
				}
			}
			if combMakerNetBp >= reqNetBp {
				if oid, okE := placePeerMakerExit(rt.PeerVenue, peerSym, closePeerSide, rt.HedgedQty, exitMakerPx, qDec, pxDec); okE {
					liveMu.Lock()
					acct.MakerPlaced++
					rt.State = "PEER_EXIT_OPEN"
					rt.ExitPeerOid = oid
					rt.ExitPeerPx = exitMakerPx
					rt.ExitPlaced = now
					rt.LastOrdPoll = time.Time{}
					rt.LastLtRestBBO = now
					liveMu.Unlock()
				}
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
		// Keep Lighter BBO fresh via REST every 1.0s while Peer Maker exit is resting on the book!
		lb, la := refreshLtRestBBOIfDue(rt.Sym, &rt.LastLtRestBBO, 1000*time.Millisecond)
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
		rt.TakerGrabBp = rt.PendingGrabBp
		rt.PendingVenue = ""
		rt.PendingSyms = nil
		rt.PendingGrabBp = 0
		rt.LastSwapAt = now
		fmt.Printf("[IDLE_SAFE_ROTATE slot=%s newVenue=%s grab=%.1fbp syms=%v note=%s]\n",
			rt.SlotKey, rt.PeerVenue, rt.TakerGrabBp, rt.Symbols, rt.RotatedNote)
	}
	liveMu.Unlock()

	if halted || clipUsd < 9.5 || now.Before(rt.CoolUntil) || now.UnixMilli() < quotaCoolUntil.Load() {
		return
	}

	// Enforce per-venue <= 3.0x leverage & <= 60% single-wallet margin cap before opening any new slot:
	ltEqNow := float64(eqNow.Load()) / 1e6
	if ltEqNow <= 0 {
		ltEqNow = 90.0
	}
	dnMu.Lock()
	bnAvail := dnSharedAvail
	bnTot := bnTotalMargin
	if bnTot <= 0 {
		bnTot = 82.0
	}
	dnMu.Unlock()
	asterMu.Lock()
	asAvail := asterAvail
	asTot := asterWallet.MarginBalance
	if asTot <= 0 {
		asTot = 49.9
	}
	asterMu.Unlock()

	liveMu.Lock()
	ltNot, bnNot, asNot := venueActiveNotionalLocked(rt.PeerVenue)
	canQuote := canSendLtTxLocked(now.UnixMilli(), false)
	liveMu.Unlock()

	if ltNot+clipUsd > math.Min(ltEqNow*2.8, 165.0) {
		return
	}
	if rt.PeerVenue == "BN_USDC" {
		if bnAvail < math.Max(14.0, bnTot*0.40) || bnNot+clipUsd > math.Min(bnTot*2.8, 155.0) {
			return
		}
	} else {
		if asAvail < math.Max(10.0, asTot*0.40) || asNot+clipUsd > math.Min(asTot*2.5, 95.0) {
			return
		}
	}
	if !rt.PeerMakerFirst && !canQuote {
		return
	}

	// -------------------------------------------------------------------------
	// S9 PURE CROSSED-BOOK TAKER-GRAB MODE (rt.TakerGrabBp > 0):
	// Fires 0% Lighter IOC + <8ms Peer Taker ONLY when true raw crossed spread >= TakerGrabBp!
	// -------------------------------------------------------------------------
	if rt.TakerGrabBp > 0 {
		if !canQuote {
			return
		}
		gSym, gPeerSym, gLtSide, gPeerSide := "", "", "", ""
		gCrossBp, gLtSigPx, gQty := -999.0, 0.0, 0.0
		gQDec, gTFeeBp, gFx := 0, 4.0, 1.0

		for _, sym := range rt.Symbols {
			if isSymUsedByAnySlot(sym, rt.SlotKey) {
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
			peerSym, qDec, _, step, _, minNot, _, tFeeBp, pBid, pAsk, _, jumpBps, fx, warmTicks, bookAge, okP := getPeerSpecAndBook(rt.PeerVenue, sym, now)
			if !okP || warmTicks < 8 || bookAge > 8*time.Second || math.Abs(jumpBps) > 1.8 {
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
			qty := roundTo(math.Floor((clipUsd/ltMid)/commonStep+1e-9)*commonStep, qtyDec)
			if qty*ltMid < 10.5 && (qty+commonStep)*ltMid <= clipUsd+3.5 {
				qty = roundTo(qty+commonStep, qtyDec)
			}
			if qty < m.MinB || qty*ltMid < math.Max(10.5, minNot) {
				continue
			}

			// STRICT POSITIVE CROSSED RAW SPREAD ONLY (Bid_A - Ask_B >= TakerGrabBp):
			rawCrossShort := ((lb - pAskUsd) / pAskUsd) * 10000.0
			if rawCrossShort >= rt.TakerGrabBp && rawCrossShort >= tFeeBp+4.0 && rawCrossShort > gCrossBp && peerTopDepthUsd(rt.PeerVenue, peerSym, "BUY") >= clipUsd*2.5 {
				gCrossBp = rawCrossShort
				gSym, gPeerSym = sym, peerSym
				gLtSide, gPeerSide = "ASK", "BUY"
				gLtSigPx, gQty = lb, qty
				gQDec, gTFeeBp, gFx = qDec, tFeeBp, fx
			}

			rawCrossLong := ((pBidUsd - la) / pBidUsd) * 10000.0
			if rawCrossLong >= rt.TakerGrabBp && rawCrossLong >= tFeeBp+4.0 && rawCrossLong > gCrossBp && peerTopDepthUsd(rt.PeerVenue, peerSym, "SELL") >= clipUsd*2.5 {
				gCrossBp = rawCrossLong
				gSym, gPeerSym = sym, peerSym
				gLtSide, gPeerSide = "BID", "SELL"
				gLtSigPx, gQty = la, qty
				gQDec, gTFeeBp, gFx = qDec, tFeeBp, fx
			}
		}

		if gSym == "" || gQty <= 0 {
			return
		}

		// Confirm live Lighter REST BBO still has true crossed raw spread >= TakerGrabBp right before firing IOC:
		m := markets[gSym]
		rb, ra := fetchLtBookREST(m.ID)
		if rb <= 0 || ra <= rb {
			return
		}
		_, qDecChk, _, stepChk, _, _, _, tFeeChk, pBidChk, pAskChk, _, _, fxChk, _, _, okChk := getPeerSpecAndBook(rt.PeerVenue, gSym, now)
		if !okChk {
			return
		}
		gQDec, gTFeeBp, gFx = qDecChk, tFeeChk, fxChk
		var iocLimitPx, peerSigPx float64
		if gLtSide == "ASK" {
			gLtSigPx = rb
			peerSigPx = pAskChk
			rawCS := ((rb - pAskChk*fxChk) / (pAskChk * fxChk)) * 10000.0
			gCrossBp = rawCS
			if rawCS < rt.TakerGrabBp || rawCS < tFeeChk+3.5 {
				return
			}
			iocLimitPx = roundTo(rb*0.9988, m.PxDec)
		} else {
			gLtSigPx = ra
			peerSigPx = pBidChk
			rawCL := ((pBidChk*fxChk - ra) / (pBidChk * fxChk)) * 10000.0
			gCrossBp = rawCL
			if rawCL < rt.TakerGrabBp || rawCL < tFeeChk+3.5 {
				return
			}
			iocLimitPx = roundTo(ra*1.0012, m.PxDec)
		}

		// Pre-set Peer leverage before firing Lighter IOC so hedge fires in <8ms upon fill!
		if rt.PeerVenue == "BN_USDC" {
			ensureBnLeverage(gPeerSym)
		} else {
			lev := 5
			if rt.PeerVenue == "ASTER_RWA" {
				lev = 8
			}
			ensureAsterLeverage(gPeerSym, lev)
		}

		// Claim symbol lock while IOC is in flight across 305ms Lighter speed-bump:
		liveMu.Lock()
		if !canSendLtTxLocked(now.UnixMilli(), false) {
			liveMu.Unlock()
			return
		}
		recordLtTxLocked(now.UnixMilli())
		rt.Sym = gSym
		liveMu.Unlock()

		tIoc0 := time.Now()
		placeLtHedgeIOC(gSym, gLtSide, iocLimitPx, gQty, false)
		ltPos, ltAvg := waitLtPositionFill(gSym, m.MinB, 950*time.Millisecond)
		ltIocLatMs := time.Since(tIoc0).Milliseconds()
		if math.Abs(ltPos) < m.MinB {
			liveMu.Lock()
			rt.Sym = ""
			rt.CoolUntil = time.Now().Add(2500 * time.Millisecond)
			liveMu.Unlock()
			return
		}
		fillQty := math.Abs(ltPos)
		if ltAvg <= 0 {
			ltAvg = gLtSigPx
		}
		hQty := fillQty
		if stepChk > 0 {
			hQty = roundTo(math.Floor(fillQty/stepChk+1e-9)*stepChk, gQDec)
		}
		if hQty <= 0 {
			_, _ = closeLtLegGuaranteedSync(gSym, ltPos, rb, ra)
			liveMu.Lock()
			rt.Sym = ""
			rt.CoolUntil = time.Now().Add(5 * time.Second)
			liveMu.Unlock()
			return
		}

		fq, ap, latMs, okH := executePeerMarketOrder(rt.PeerVenue, gPeerSym, gPeerSide, hQty, gQDec, false)
		if !okH || fq <= 0 {
			_, _ = closeLtLegGuaranteedSync(gSym, ltPos, rb, ra)
			liveMu.Lock()
			rt.Sym = ""
			rt.CoolUntil = time.Now().Add(10 * time.Second)
			liveMu.Unlock()
			return
		}
		if ap <= 0 {
			ap = peerSigPx
		}
		notional := fq * ltAvg
		entryFeeUsd := (fq * ap * gFx) * (gTFeeBp / 10000.0)
		slipBp := 0.0
		if peerSigPx > 0 {
			if gPeerSide == "BUY" {
				slipBp = ((ap - peerSigPx) / peerSigPx) * 10000.0
			} else {
				slipBp = ((peerSigPx - ap) / peerSigPx) * 10000.0
			}
		}
		ltSideHuman := "BUY"
		if gLtSide == "ASK" {
			ltSideHuman = "SELL"
		}
		tid := time.Now().UnixMilli()
		liveMu.Lock()
		acct.TakerFills += 2
		acct.GrossVolumeUsd = roundTo(acct.GrossVolumeUsd+notional*2.0, 2)
		appendLiveFillLocked(LiveFillAudit{
			TsMs:      tid,
			StratID:   rt.StratID,
			Venue:     "LT_STD",
			Symbol:    gSym,
			Side:      ltSideHuman,
			ExecType:  "LT_IOC_0FEE_GRAB",
			FillPx:    ltAvg,
			SignalPx:  gLtSigPx,
			Qty:       fq,
			Notional:  roundTo(notional, 2),
			FeeUsd:    0.0,
			Realized:  0.0,
			LatencyMs: ltIocLatMs,
			Tag:       "S9_TAKER_GRAB",
		})
		appendLiveFillLocked(LiveFillAudit{
			TsMs:      tid,
			StratID:   rt.StratID,
			Venue:     rt.PeerVenue,
			Symbol:    gPeerSym,
			Side:      gPeerSide,
			ExecType:  "TOKYO_TAKER_HEDGE",
			FillPx:    ap,
			SignalPx:  peerSigPx,
			Qty:       fq,
			Notional:  roundTo(fq*ap*gFx, 2),
			FeeUsd:    roundTo(entryFeeUsd, 4),
			Realized:  0.0,
			SlipBp:    roundTo(slipBp, 2),
			LatencyMs: latMs,
			Tag:       "PEER_HEDGE",
		})
		rt.State = "HEDGED"
		rt.Sym = gSym
		rt.PeerSym = gPeerSym
		rt.LtSide = gLtSide
		rt.PeerSide = gPeerSide
		rt.HedgedQty = fq
		rt.EntryLtPx = ltAvg
		rt.EntryPeerPx = ap
		rt.EntryFx = gFx
		rt.EntryPeerFee = gTFeeBp
		rt.EntryFeeUsd = roundTo(entryFeeUsd, 4)
		rt.OpenedAt = time.Now()
		rt.TradeID = tid
		liveMu.Unlock()

		lockedEdgeBp := ((ltAvg - ap*gFx) / (ap * gFx) * 10000.0) - gTFeeBp
		if gLtSide == "BID" {
			lockedEdgeBp = ((ap*gFx - ltAvg) / (ap * gFx) * 10000.0) - gTFeeBp
		}
		recordStratOpen(tid, rt.StratID,
			fmt.Sprintf("S9_GRAB_%s_%s+%s_%s", gSym, gLtSide, rt.PeerVenue, gPeerSide),
			gSym, notional, lockedEdgeBp,
			fmt.Sprintf("S9Grab Lt0pct@%.5f(%dms)+Peer@%.5f(%dms)", ltAvg, ltIocLatMs, ap, latMs))
		return
	}

	bestSym, bestPeerSym, bestLtSide, bestPeerSide := "", "", "", ""
	bestDisloc, bestLtPx, bestPeerMakerPx, bestQty := -999.0, 0.0, 0.0, 0.0
	bestQDec, bestPxDec := 0, 0

	for _, sym := range rt.Symbols {
		switch sym {
		case "PRL", "BB", "ARM", "OPENAI", "ZEC":
			continue
		}
		if isSymUsedByAnySlot(sym, rt.SlotKey) {
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
		if ltSp > 10.0 {
			continue
		}

		peerSym, qDec, pxDec, step, _, minNot, _, tFeeBp, pBid, pAsk, emaBps, jumpBps, fx, warmTicks, bookAge, okP := getPeerSpecAndBook(rt.PeerVenue, sym, now)
		if !okP || warmTicks < 8 || bookAge > 8*time.Second || math.Abs(jumpBps) > 1.8 {
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
		qty := roundTo(math.Floor((clipUsd/ltMid)/commonStep+1e-9)*commonStep, qtyDec)
		if qty*ltMid < 10.5 && (qty+commonStep)*ltMid <= clipUsd+3.5 {
			qty = roundTo(qty+commonStep, qtyDec)
		}
		if qty < m.MinB || qty*ltMid < math.Max(10.5, minNot) {
			continue
		}

		if rt.PeerMakerFirst {
			// S10 / S1 Flipped Maker-First (0.00bp entry fee!):
			// Option A: BUY GTX Maker on Peer @ pBid, SELL IOC Hedge on Lighter @ lb (strictly lb > pBidUsd by >= MinRawEdgeBp!)
			rawBuyPeerBp := ((lb - pBidUsd) / pBidUsd) * 10000.0
			disBuyPeerBp := rawBuyPeerBp - emaBps
			if rawBuyPeerBp >= rt.MinRawEdgeBp && (rawBuyPeerBp >= rt.EntryDisloc || disBuyPeerBp >= rt.EntryDisloc) && rawBuyPeerBp > bestDisloc {
				bestDisloc = rawBuyPeerBp
				bestSym, bestPeerSym = sym, peerSym
				bestPeerSide, bestLtSide = "BUY", "ASK"
				bestPeerMakerPx, bestLtPx, bestQty = pBid, lb, qty
				bestQDec, bestPxDec = qDec, pxDec
			}
			// Option B: SELL GTX Maker on Peer @ pAsk, BUY IOC Hedge on Lighter @ la (strictly pAskUsd > la by >= MinRawEdgeBp!)
			rawSellPeerBp := ((pAskUsd - la) / pAskUsd) * 10000.0
			disSellPeerBp := rawSellPeerBp + emaBps
			if rawSellPeerBp >= rt.MinRawEdgeBp && (rawSellPeerBp >= rt.EntryDisloc || disSellPeerBp >= rt.EntryDisloc) && rawSellPeerBp > bestDisloc {
				bestDisloc = rawSellPeerBp
				bestSym, bestPeerSym = sym, peerSym
				bestPeerSide, bestLtSide = "SELL", "BID"
				bestPeerMakerPx, bestLtPx, bestQty = pAsk, la, qty
				bestQDec, bestPxDec = qDec, pxDec
			}
			continue
		}

		minRaw := tFeeBp + rt.MinRawEdgeBp
		if rt.MinRawEdgeBp <= 0 {
			minRaw = tFeeBp + 2.5
		}
		// Candidate A: SELL PostOnly Maker on Lighter @ la, BUY Taker Hedge on Peer @ pAsk (strictly la > pAskUsd!)
		rawShortBp := ((la - pAskUsd) / pAskUsd) * 10000.0
		shortDislocBp := rawShortBp - emaBps
		if rawShortBp >= minRaw && (rawShortBp >= rt.EntryDisloc || shortDislocBp >= rt.EntryDisloc) && rawShortBp > bestDisloc && peerTopDepthUsd(rt.PeerVenue, peerSym, "BUY") >= clipUsd*2.5 {
			bestDisloc = rawShortBp
			bestSym, bestPeerSym = sym, peerSym
			bestLtSide, bestPeerSide = "ASK", "BUY"
			bestLtPx, bestQty = la, qty
		}

		// Candidate B: BUY PostOnly Maker on Lighter @ lb, SELL Taker Hedge on Peer @ pBid (strictly pBidUsd > lb!)
		rawLongBp := ((pBidUsd - lb) / pBidUsd) * 10000.0
		longDislocBp := rawLongBp + emaBps
		if rawLongBp >= minRaw && (rawLongBp >= rt.EntryDisloc || longDislocBp >= rt.EntryDisloc) && rawLongBp > bestDisloc && peerTopDepthUsd(rt.PeerVenue, peerSym, "SELL") >= clipUsd*2.5 {
			bestDisloc = rawLongBp
			bestSym, bestPeerSym = sym, peerSym
			bestLtSide, bestPeerSide = "BID", "SELL"
			bestLtPx, bestQty = lb, qty
		}
	}

	if bestSym == "" || bestQty <= 0 {
		return
	}

	// Verify live Lighter REST BBO right before placing Maker order so stale WS levels never cross/underprice entry:
	if rb, ra := fetchLtBookREST(markets[bestSym].ID); rb > 0 && ra > rb {
		_, qDecChk, pxDecChk, _, _, _, _, tFeeChk, pBidChk, pAskChk, _, _, fxChk, _, _, okChk := getPeerSpecAndBook(rt.PeerVenue, bestSym, now)
		if !okChk {
			return
		}
		if rt.PeerMakerFirst {
			bestQDec, bestPxDec = qDecChk, pxDecChk
			if bestPeerSide == "BUY" {
				bestPeerMakerPx = roundTo(pBidChk, pxDecChk)
				rawBP := ((rb - bestPeerMakerPx*fxChk) / (bestPeerMakerPx * fxChk)) * 10000.0
				bestDisloc = rawBP
				if rawBP < rt.MinRawEdgeBp {
					return
				}
			} else {
				bestPeerMakerPx = roundTo(pAskChk, pxDecChk)
				rawSP := ((bestPeerMakerPx*fxChk - ra) / (bestPeerMakerPx * fxChk)) * 10000.0
				bestDisloc = rawSP
				if rawSP < rt.MinRawEdgeBp {
					return
				}
			}
		} else {
			minRawChk := tFeeChk + math.Max(rt.MinRawEdgeBp, 2.5)
			if bestLtSide == "ASK" {
				bestLtPx = ra
				rawS := ((ra - pAskChk*fxChk) / (pAskChk * fxChk)) * 10000.0
				bestDisloc = rawS
				if rawS < minRawChk {
					return
				}
			} else {
				bestLtPx = rb
				rawL := ((pBidChk*fxChk - rb) / (pBidChk * fxChk)) * 10000.0
				bestDisloc = rawL
				if rawL < minRawChk {
					return
				}
			}
		}
	}

	// Pre-set leverage BEFORE resting Maker:
	if rt.PeerVenue == "BN_USDC" {
		ensureBnLeverage(bestPeerSym)
	} else {
		lev := 5
		if rt.PeerVenue == "ASTER_RWA" {
			lev = 5
		}
		ensureAsterLeverage(bestPeerSym, lev)
	}

	if rt.PeerMakerFirst {
		// Lock rt.Sym BEFORE resting Peer GTX Maker so no other slot or sweeper races on the same symbol!
		liveMu.Lock()
		rt.Sym = bestSym
		liveMu.Unlock()

		oid, placedP := placePeerMakerOrder(rt.PeerVenue, bestPeerSym, bestPeerSide, bestQty, bestPeerMakerPx, bestQDec, bestPxDec, false)
		if !placedP {
			liveMu.Lock()
			rt.Sym = ""
			rt.CoolUntil = now.Add(2 * time.Second)
			liveMu.Unlock()
			return
		}
		liveMu.Lock()
		acct.MakerPlaced++
		rt.State = "PEER_ENTRY_OPEN"
		rt.Sym = bestSym
		rt.PeerSym = bestPeerSym
		rt.PeerSide = bestPeerSide
		rt.LtSide = bestLtSide
		rt.EntryPeerOid = oid
		rt.LtLimitPx = bestPeerMakerPx
		rt.LtOrderQty = bestQty
		rt.PlacedAt = now
		rt.LastOrdPoll = time.Time{}
		rt.LastLtRestBBO = now
		liveMu.Unlock()

		fmt.Printf("[LIVE_PEER_MAKER_QUOTE strat=%s peer=%s %s qty=%.4f @ %.5f lt=%s raw=%+.2fbp]\n",
			rt.StratID, bestPeerSym, bestPeerSide, bestQty, bestPeerMakerPx, bestLtSide, bestDisloc)
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

	fmt.Printf("[LIVE_QUOTE strat=%s sym=%s lt=%s qty=%.4f @ %.5f peer=%s raw=%+.2fbp]\n",
		rt.StratID, bestSym, bestLtSide, bestQty, bestLtPx, bestPeerSym, bestDisloc)
}

func writeLiveArenaTelemetry(now time.Time) {
	nowMs := now.UnixMilli()
	upSec := math.Max(1.0, float64(nowMs-liveBootMs)/1000.0)
	upHr := upSec / 3600.0
	denomHr := math.Max(upHr, 0.05) // 3-min floor to prevent early-second extrapolation spikes

	ltEq := float64(eqNow.Load()) / 1e6
	ltCol := float64(ltCollateral.Load()) / 1e6
	if ltCol <= 0 {
		ltCol = ltEq
	}
	dnMu.Lock()
	bnMargin := bnTotalMargin
	bnWallet := bnTotalWallet
	if bnMargin <= 0 {
		bnMargin = dnSharedAvail
	}
	dnMu.Unlock()
	asterMu.Lock()
	asMargin := asterWallet.MarginBalance
	asWallet := asterWallet.WalletBalance
	asterMu.Unlock()

	realWallet3 := 0.0
	realEq3 := 0.0
	realWalletDelta := 0.0
	realEqDelta := 0.0
	if ltCol > 10 && bnWallet > 50 && asWallet > 10 {
		realWallet3 = roundTo(ltCol+bnWallet+asWallet, 4)
		realEq3 = roundTo(ltEq+bnMargin+asMargin, 4)
		realWalletDelta = roundTo(realWallet3-Baseline3VenueUSD, 4)
		realEqDelta = roundTo(realEq3-Baseline3VenueUSD, 4)
	}

	liveMu.Lock()
	txLastMin := countLtTxLastMinLocked(nowMs)
	outStrats := make([]*LiveStratAccount, 0, len(liveStratOrder))
	combinedNet := 0.0
	bestID := ""
	bestHr := -1e9
	bestScaled489 := 0.0

	for _, id := range liveStratOrder {
		acct := liveStrats[id]
		rt0 := liveSlots[id+":0"]
		if acct == nil || rt0 == nil {
			continue
		}

		upnl := 0.0
		snaps := []LivePosView{}
		activeMakers := 0
		var activeSyms []string
		var stateParts []string
		primaryState := "IDLE"
		rotNote := rt0.RotatedNote

		for sub := 0; sub < 2; sub++ {
			rt := liveSlots[fmt.Sprintf("%s:%d", id, sub)]
			if rt == nil {
				continue
			}
			if rt.RotatedNote != "" {
				rotNote = rt.RotatedNote
			}
			if rt.State == "LT_ENTRY_OPEN" || rt.State == "PEER_ENTRY_OPEN" || rt.State == "PEER_EXIT_OPEN" {
				activeMakers++
			}
			if rt.State != "IDLE" {
				if primaryState == "IDLE" || rt.State == "HEDGED" || rt.State == "PEER_EXIT_OPEN" {
					primaryState = rt.State
				}
				if rt.Sym != "" {
					activeSyms = append(activeSyms, rt.Sym)
					stateParts = append(stateParts, fmt.Sprintf("%s(%s)", rt.State, rt.Sym))
				} else {
					stateParts = append(stateParts, rt.State)
				}
			}
			if (rt.State == "HEDGED" || rt.State == "PEER_EXIT_OPEN") && rt.HedgedQty > 0 {
				// Deduct opening peer fee inside Open Hedged Basis until close so 0/0 Closed Signal stays $0.0000!
				upnl -= rt.EntryFeeUsd
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

		// Strict -$8.00 Total-Equity Max Drawdown Demotion to Tier 0 (Forward Testing Only) — NO win-rate gating, NO single-tail overreaction!
		if acct.Tier > 0 && acct.MaxDrawdownUsd >= MaxDemoteDrawdownUSD {
			acct.Tier = 0
			acct.TierChangedMs = nowMs
			acct.TierBaseTrips = acct.RoundTrips
			acct.HaltReason = fmt.Sprintf("Demoted to Tier 0: MaxDD $%.2f >= $%.2f", acct.MaxDrawdownUsd, MaxDemoteDrawdownUSD)
			applyTierClipLocked(acct)
			fmt.Printf("[TIER_DEMOTE id=%s maxDD=$%.2f -> Tier 0 Forward Testing]\n", id, acct.MaxDrawdownUsd)
		}

		if len(acct.EquityCurve) == 0 || nowMs-acct.EquityCurve[len(acct.EquityCurve)-1].TsMs >= 30000 {
			acct.EquityCurve = append(acct.EquityCurve, LiveEqPoint{TsMs: nowMs, Cash: acct.Cash, Eq: acct.Equity})
			if len(acct.EquityCurve) > 540 { // 4.5h rolling history
				acct.EquityCurve = append([]LiveEqPoint(nil), acct.EquityCurve[len(acct.EquityCurve)-540:]...)
			}
		}

		// Govern strictly on Total Equity $/hr (Closed + Open Basis) & Profit Factor:
		acct.HourlyRunRate = roundTo(netStrat/denomHr, 3)
		acct.RealizedHourly = roundTo(realStrat/denomHr, 3)
		roll4h := acct.HourlyRunRate
		targetTs := nowMs - 45*60*1000
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
				roll4h = roundTo((acct.Equity-refPt.Eq)/dtHr, 3)
			}
		}
		if acct.RoundTrips == 0 && len(snaps) == 0 {
			acct.RealizedHourly = 0.0
			roll4h = 0.0
		}
		acct.Rolling4hHourly = roll4h
		sigHr := roll4h
		if acct.Tier <= 0 {
			sigHr = acct.Forward45mHr
			acct.Scaled489Hourly = roundTo(acct.Forward45mHr, 2)
		} else {
			acct.Scaled489Hourly = roundTo(sigHr*rt0.ScaleMult489, 2)
		}
		acct.ActiveMakers = activeMakers
		acct.PosSnap = snaps
		acct.State = primaryState
		acct.ActiveSym = strings.Join(activeSyms, ",")
		acct.RotatedNote = rotNote
		stateDisp := acct.TierLabel
		if len(stateParts) > 0 {
			stateDisp = acct.TierLabel + " [" + strings.Join(stateParts, "+") + "]"
		}
		if acct.Halted {
			acct.StatusNote = "HALTED: " + acct.HaltReason
		} else {
			acct.StatusNote = fmt.Sprintf("%s | Closed $%+.3f | Open $%+.3f | PF %.2f | MaxDD $%.2f/$8.00 | Fwd45m $%+.2f/hr",
				stateDisp, realStrat, upnl, acct.ProfitFactor, acct.MaxDrawdownUsd, acct.Forward45mHr)
		}

		if acct.Scaled489Hourly > bestScaled489 {
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

	// Global -$6.50 ($300.33 Perp floor) circuit breaker verified against BOTH strategy ledger AND true 3-venue wallet equity!
	if (combinedNet <= GlobalStopLossUSD || (realEq3 > 100 && realEqDelta <= GlobalStopLossUSD)) && breakerTripped.CompareAndSwap(false, true) {
		go flattenAndExitPause(fmt.Sprintf("STOP-LOSS Global Micro-Arena PnL=$%+.2f (WalletDelta=$%+.2f) <= $%.2f", combinedNet, realEqDelta, GlobalStopLossUSD))
	}

	// Build live dislocation radar across all 4 strategy symbol sets (deduplicated by symbol+peerVenue):
	spreads := make([]LiveSpreadSnap, 0, 26)
	seenRadar := map[string]bool{}
	for _, id := range liveStratOrder {
		rt := liveSlots[id+":0"]
		if rt == nil {
			continue
		}
		for _, sym := range rt.Symbols {
			rk := sym + ":" + rt.PeerVenue
			if seenRadar[rk] {
				continue
			}
			seenRadar[rk] = true
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
	slotSnaps := make([]SavedSlotSnap, 0, len(liveSlotKeys))
	for _, sk := range liveSlotKeys {
		if rt := liveSlots[sk]; rt != nil {
			slotSnaps = append(slotSnaps, SavedSlotSnap{
				SlotKey:      rt.SlotKey,
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
				EntryFeeUsd:  rt.EntryFeeUsd,
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
		Version:             LiveArenaVer,
		BootTsMs:            liveBootMs,
		NowTsMs:             nowMs,
		UptimeSec:           roundTo(upSec, 1),
		UptimeHours:         roundTo(upHr, 3),
		Paused:              remotePaused.Load(),
		LtTxLastMin:         txLastMin,
		TotalCapitalUsd:     roundTo(ltEq+bnMargin+asMargin, 2),
		LighterEqUsd:        roundTo(ltEq, 2),
		BinanceMarginUsd:    roundTo(bnMargin, 2),
		AsterMarginUsd:      roundTo(asMargin, 2),
		Real3VenueWalletUsd: realWallet3,
		Real3VenueEquityUsd: realEq3,
		RealWalletDeltaUsd:  realWalletDelta,
		RealEquityDeltaUsd:  realEqDelta,
		CombinedNetUsd:      roundTo(combinedNet, 4),
		CombinedHourly:      roundTo(combinedNet/denomHr, 3),
		Scaled489BestHr:     bestScaled489,
		BestStratID:         bestID,
		BestHourlyUsd:       roundTo(bestHr, 3),
		Strategies:          outStrats,
		RecentFills:         revFills,
		MarketSpreads:       spreads,
		SlotSnapshots:       slotSnaps,
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
			ID               string  `json:"id"`
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
	type fwdStat struct {
		hr45  float64
		pf    float64
		maxDD float64
		trips int64
	}
	fMap := map[string]fwdStat{}
	for _, s := range fa.Strategies {
		r := s.Rolling45mHourly
		if r == 0 && fa.UptimeHours >= 0.25 {
			r = s.HourlyRunRate
		}
		fMap[s.ID] = fwdStat{hr45: r, pf: s.ProfitFactor, maxDD: s.MaxDrawdownUsd, trips: s.RoundTrips}
	}

	nowMs := time.Now().UnixMilli()
	liveMu.Lock()
	defer liveMu.Unlock()

	for _, id := range liveStratOrder {
		acct := liveStrats[id]
		if acct == nil {
			continue
		}
		fs := fMap[acct.ForwardStratID]
		acct.Forward45mHr = roundTo(fs.hr45, 2)
		acct.ForwardPF = roundTo(fs.pf, 2)
		acct.ForwardTrips = fs.trips

		if acct.Halted {
			continue
		}
		// Demotion Rules: MaxDrawdown >= $8.00 OR Rolling 6-Trip ProfitFactor < 0.95 (never single-outlier demotion!):
		if acct.Tier > 0 {
			if acct.MaxDrawdownUsd >= MaxDemoteDrawdownUSD {
				acct.Tier = 0
				acct.TierChangedMs = nowMs
				acct.TierBaseTrips = acct.RoundTrips
				acct.HaltReason = fmt.Sprintf("Demoted: MaxDD $%.2f >= $8.00", acct.MaxDrawdownUsd)
				applyTierClipLocked(acct)
				continue
			}
			if len(acct.RecentTripPnls) >= 6 {
				wSum, lSum := 0.0, 0.0
				for _, p := range acct.RecentTripPnls[len(acct.RecentTripPnls)-6:] {
					if p > 0 {
						wSum += p
					} else if p < 0 {
						lSum += math.Abs(p)
					}
				}
				roll6PF := 99.0
				if lSum > 1e-6 {
					roll6PF = wSum / lSum
				}
				if roll6PF < 0.95 && (acct.Equity-acct.StartCash) < -1.00 {
					acct.Tier = 0
					acct.TierChangedMs = nowMs
					acct.TierBaseTrips = acct.RoundTrips
					acct.HaltReason = fmt.Sprintf("Demoted: 6-Trip PF %.2f < 0.95", roll6PF)
					applyTierClipLocked(acct)
					continue
				}
			}
		}

		// Tier Graduation Rules (governed strictly by Total-Equity $/hr & Profit Factor; only promote when slots are IDLE or ready):
		dwellMin := float64(nowMs-acct.TierChangedMs) / 60000.0
		tripsInTier := acct.RoundTrips - acct.TierBaseTrips
		netGain := acct.Equity - acct.StartCash

		switch acct.Tier {
		case 0:
			// Tier 0 -> Tier 1 ($10 Micro): 45m+ Forward Lab uptime, Total Equity $/hr >= $5.00, PF >= 1.35, >= 5 Trips, MaxDD < $6.00
			if fa.UptimeHours >= 0.75 && fs.hr45 >= 5.00 && fs.pf >= 1.35 && fs.trips >= 5 && fs.maxDD < 6.00 && dwellMin >= 15.0 {
				acct.Tier = 1
				acct.TierChangedMs = nowMs
				acct.TierBaseTrips = acct.RoundTrips
				acct.MaxDrawdownUsd = 0.0
				acct.PeakEquity = acct.Equity
				acct.HaltReason = ""
				applyTierClipLocked(acct)
				fmt.Printf("[TIER_PROMOTE id=%s -> Tier 1 ($10) | Fwd45m=$%+.2f/hr PF=%.2f Trips=%d]\n", id, fs.hr45, fs.pf, fs.trips)
			}
		case 1:
			// Tier 1 -> Tier 2 (2x $25 Medium): 30m+ in Tier 1, >= 4 Real Trips, Total Net > $0, Real PF >= 1.30
			if dwellMin >= 30.0 && tripsInTier >= 4 && netGain > 0.0 && acct.ProfitFactor >= 1.30 && acct.MaxDrawdownUsd < 4.00 {
				acct.Tier = 2
				acct.TierChangedMs = nowMs
				acct.TierBaseTrips = acct.RoundTrips
				applyTierClipLocked(acct)
				fmt.Printf("[TIER_PROMOTE id=%s -> Tier 2 (2x $25) | RealPF=%.2f Net=$%+.3f]\n", id, acct.ProfitFactor, netGain)
			}
		case 2:
			// Tier 2 -> Tier 3 (2x $55 BN / 2x $35 RWA Full Safe Cap): 45m+ in Tier 2, >= 6 Real Trips, Real PF >= 1.40
			if dwellMin >= 45.0 && tripsInTier >= 6 && netGain > 0.25 && acct.ProfitFactor >= 1.40 && acct.MaxDrawdownUsd < 5.00 {
				acct.Tier = 3
				acct.TierChangedMs = nowMs
				acct.TierBaseTrips = acct.RoundTrips
				applyTierClipLocked(acct)
				fmt.Printf("[TIER_PROMOTE id=%s -> Tier 3 (2x $%.0f) | RealPF=%.2f Net=$%+.3f]\n", id, acct.Tier3ClipUsd, acct.ProfitFactor, netGain)
			}
		}
	}
}

func sweepUntrackedLtOrphansSync() {
	if remotePaused.Load() || breakerTripped.Load() {
		return
	}
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := ltGet(hc, fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx))
	if err != nil {
		return
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var res struct {
		Accounts []struct {
			Positions []struct {
				MarketID int64  `json:"market_id"`
				Position string `json:"position"`
				Sign     int    `json:"sign"`
			} `json:"positions"`
		} `json:"accounts"`
	}
	if json.Unmarshal(body, &res) != nil || len(res.Accounts) == 0 {
		return
	}
	for _, p := range res.Accounts[0].Positions {
		sym, ok := idToSym[p.MarketID]
		if !ok {
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
		liveMu.Lock()
		tracked := false
		for _, rt := range liveSlots {
			if rt != nil && rt.State != "IDLE" && rt.Sym == sym {
				tracked = true
				break
			}
		}
		liveMu.Unlock()
		if !tracked {
			lb, la := fetchLtBookREST(m.ID)
			fmt.Printf("[ORPHAN_SWEEP] Flattening untracked Lighter position %s sz=%.4f cleanly!\n", sym, sz)
			_, _ = closeLtLegGuaranteedSync(sym, sz, lb, la)
		}
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
	sweepUntrackedLtOrphansSync()
	evaluateForwardArenaIdleSafeRotation()
	writeLiveArenaTelemetry(time.Now())

	// Keep TLS TCP pools to fapi.binance.com, fapi.asterdex.com, and mainnet.zklighter.elliot.ai warm every 4s (<8ms hedge):
	go func() {
		t := time.NewTicker(4 * time.Second)
		defer t.Stop()
		for {
			<-t.C
			if remotePaused.Load() {
				return
			}
			if r1, e1 := bnHTTP.Get("https://fapi.binance.com/fapi/v1/ping"); e1 == nil {
				_, _ = io.Copy(io.Discard, r1.Body)
				r1.Body.Close()
			}
			if r2, e2 := bnHTTP.Get(AsterBaseURL + "/fapi/v3/ping"); e2 == nil {
				_, _ = io.Copy(io.Discard, r2.Body)
				r2.Body.Close()
			}
			if r3, e3 := txHTTP.Get(BaseURL + "/api/v1/time"); e3 == nil {
				_, _ = io.Copy(io.Discard, r3.Body)
				r3.Body.Close()
			}
		}
	}()

	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	lastMarginPoll := time.Now()
	lastRotCheck := time.Now()
	lastOrphanCheck := time.Now()
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
		if now.Sub(lastOrphanCheck) >= 30*time.Second {
			lastOrphanCheck = now
			go sweepUntrackedLtOrphansSync()
		}
		if now.Sub(lastRotCheck) >= 60*time.Second {
			lastRotCheck = now
			go evaluateForwardArenaIdleSafeRotation()
		}
		for _, sk := range liveSlotKeys {
			if remotePaused.Load() || breakerTripped.Load() {
				return
			}
			if rt := liveSlots[sk]; rt != nil {
				stepLiveSlot(rt, now)
			}
		}
		if now.Sub(lastTelWrite) >= 1000*time.Millisecond {
			lastTelWrite = now
			writeLiveArenaTelemetry(now)
		}
	}
}
