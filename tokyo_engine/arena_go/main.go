package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	StartBudgetUSD = 489.12 // Full $489.12 benchmark capital (<= 3.0x gross leverage cap; real wallets untouched!)
	TargetHourly   = 5.00
	ArenaOutPath   = "/tmp/forward_arena.json"
	ArenaStatePath = "/opt/crypto-arb-bot/forward_arena_state.json"
	ArenaVer       = "ARENA-v6.3-PARITY"

	VenueBnUsdc   = "BN_USDC"
	VenueBnUsdt   = "BN_USDT"
	VenueLtStd    = "LT_STD"
	VenueLtPrem   = "LT_PREM"
	VenueAsterC   = "ASTER_C"   // AsterDEX Crypto: 0% Maker, 4.0bp Taker, live-measured Tokyo RTT + L2 depth20
	VenueAsterRWA = "ASTER_RWA" // AsterDEX RWA Equities/Commodities: 0% Maker, 1.25bp Taker!
	VenuePoly     = "POLY"
)

var (
	liveBnRttMs    atomic.Int64 // Live measured Tokyo HTTP RTT to fapi.binance.com
	liveAsterRttMs atomic.Int64 // Live measured Tokyo HTTP RTT to fapi.asterdex.com
	liveLtRttMs    atomic.Int64 // Live measured Tokyo HTTP RTT to mainnet.zklighter.elliot.ai
	liveLtPubLagMs atomic.Int64 // Live measured zkLighter WS trade timestamp -> EC2 arrival lag!
)

type VenuePhysics struct {
	PlaceLatMs  int64
	CancelLatMs int64
	TakerLatMs  int64
	MakerFeeBp  float64
	TakerFeeBp  float64
	MaxTxPerMin int
}

var venuePhys = map[string]VenuePhysics{
	VenueBnUsdc:   {PlaceLatMs: 4, CancelLatMs: 8, TakerLatMs: 4, MakerFeeBp: 0.0, TakerFeeBp: 4.0, MaxTxPerMin: 1200},
	VenueBnUsdt:   {PlaceLatMs: 4, CancelLatMs: 8, TakerLatMs: 4, MakerFeeBp: 2.0, TakerFeeBp: 5.0, MaxTxPerMin: 1200},
	VenueLtStd:    {PlaceLatMs: 5, CancelLatMs: 305, TakerLatMs: 305, MakerFeeBp: 0.0, TakerFeeBp: 0.0, MaxTxPerMin: 56},
	VenueLtPrem:   {PlaceLatMs: 5, CancelLatMs: 5, TakerLatMs: 145, MakerFeeBp: 0.30, TakerFeeBp: 2.00, MaxTxPerMin: 3800},
	VenueAsterC:   {PlaceLatMs: 6, CancelLatMs: 10, TakerLatMs: 8, MakerFeeBp: 0.0, TakerFeeBp: 4.0, MaxTxPerMin: 1200},
	VenueAsterRWA: {PlaceLatMs: 6, CancelLatMs: 10, TakerLatMs: 8, MakerFeeBp: 0.0, TakerFeeBp: 1.25, MaxTxPerMin: 1200},
	VenuePoly:     {PlaceLatMs: 60, CancelLatMs: 65, TakerLatMs: 65, MakerFeeBp: -0.50, TakerFeeBp: 6.0, MaxTxPerMin: 300},
}

type LtMarketSpec struct {
	Symbol        string
	MarketID      int64
	PriceDecimals int
	SizeDecimals  int
	MinBase       float64
	MinQuote      float64
	MaxLev        float64
	DailyTrades   int64
	BnUsdcSym     string
	BnUsdtSym     string
	BnPriceDec    int
	BnQtyDec      int
	BnTick        float64
	BnStep        float64
	BnMinNot      float64
}

type BnUsdcSpec struct {
	BaseSym  string
	UsdcSym  string
	UsdtSym  string
	PriceDec int
	QtyDec   int
	TickSize float64
	StepSize float64
	MinNot   float64
}

type BookLevel struct {
	Price float64
	Size  float64
}

type LiveBook struct {
	Venue     string
	Symbol    string
	Bid       float64
	BidSz     float64
	Ask       float64
	AskSz     float64
	Bids      []BookLevel // sorted desc
	Asks      []BookLevel // sorted asc
	UpdatedMs int64
}

type PriceSample struct {
	TsMs int64
	Mid  float64
}

type ForceLiqEvent struct {
	Symbol string
	Side   string // "SELL" means long liquidated (bearish dump wick), "BUY" means short liquidated (pump wick)
	Price  float64
	Qty    float64
	Usd    float64
	TsMs   int64
}

type SimMakerOrder struct {
	ID                int64
	StratID           string
	Tag               string
	Venue             string
	Symbol            string
	Side              string // "BUY" or "SELL"
	LimitPx           float64
	OrigQty           float64
	RemQty            float64
	QueueAheadUsd     float64
	InitQueueAheadUsd float64
	PlacedMs          int64
	ReadyMs           int64
	Activated         bool
	CancelReqMs       int64
	CancelEffMs       int64
	ReduceOnly        bool
}

type SimTakerOrder struct {
	ID         int64
	StratID    string
	Tag        string
	Venue      string
	Symbol     string
	Side       string // "BUY" or "SELL"
	Qty        float64
	SignalPx   float64
	PlacedMs   int64
	ExecAtMs   int64
	MaxSlipBp  float64
	ReduceOnly bool
}

type Position struct {
	Venue     string  `json:"venue"`
	Symbol    string  `json:"symbol"`
	Qty       float64 `json:"qty"` // >0 long, <0 short
	AvgPx     float64 `json:"avgPx"`
	EntryFee  float64 `json:"entryFee,omitempty"`
	OpenedMs  int64   `json:"openedMs"`
	UpdatedMs int64   `json:"updatedMs"`
	Tag       string  `json:"tag"`
}

type FillAudit struct {
	TsMs       int64   `json:"ts"`
	StratID    string  `json:"strat"`
	Venue      string  `json:"venue"`
	Symbol     string  `json:"sym"`
	Side       string  `json:"side"`
	ExecType   string  `json:"type"` // MAKER_QUEUE, MAKER_CROSS, PICKOFF_AFTER_CANCEL, TAKER_BOOKWALK
	FillPx     float64 `json:"px"`
	SignalPx   float64 `json:"sigPx"`
	Qty        float64 `json:"qty"`
	Notional   float64 `json:"usd"`
	FeeUsd     float64 `json:"fee"`
	Realized   float64 `json:"realized"`
	SlipBp     float64 `json:"slipBp"`
	QueueInit  float64 `json:"qInitUsd"`
	LatencyMs  int64   `json:"latMs"`
	Tag        string  `json:"tag"`
}

type EquityPoint struct {
	TsMs      int64   `json:"ts"`
	Equity    float64 `json:"eq"`
	MidEq     float64 `json:"midEq,omitempty"`
	Cash      float64 `json:"cash,omitempty"`
}

type ClosedTripRecord struct {
	TsMs     int64   `json:"ts"`
	StratID  string  `json:"strat"`
	Coin     string  `json:"coin"`
	Legs     string  `json:"legs"`
	Notional float64 `json:"usd"`
	HoldSec  float64 `json:"holdS"`
	FeeUsd   float64 `json:"fee"`
	NetUsd   float64 `json:"net"`
	Reason   string  `json:"reason"`
}

type StrategyAccount struct {
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Desc            string               `json:"desc"`
	VenueDesc       string               `json:"venueDesc"`
	MaxLevCap       float64              `json:"maxLevCap"`
	StartCash       float64              `json:"startCash"`
	Cash            float64              `json:"cash"` // StartCash + RealizedPnL - FeesPaid + FundingUsd
	ClosedNetUsd    float64              `json:"closedNetUsd"`
	WithdrawableUsd float64              `json:"withdrawableUsd"`
	RealizedGross   float64              `json:"realizedGross"`
	FeesPaid        float64              `json:"feesPaid"`
	FundingUsd      float64              `json:"fundingUsd"`
	UnrealizedPnL   float64              `json:"unrealizedPnl"`
	Equity          float64              `json:"equity"`
	PeakEquity      float64              `json:"peakEquity"`
	MaxDrawdownUsd  float64              `json:"maxDrawdownUsd"`
	HourlyRunRate    float64              `json:"hourlyRunRate"`
	RealizedHourly   float64              `json:"realizedHourly"`
	Rolling15mHourly float64              `json:"rolling15mHourly"`
	Rolling45mHourly float64              `json:"rolling45mHourly"`
	Rolling2hHourly  float64              `json:"rolling2hHourly"`
	Rolling4hHourly  float64              `json:"rolling4hHourly"`
	Rolling24hHourly float64              `json:"rolling24hHourly"`
	StratBootMs     int64                `json:"stratBootMs,omitempty"`
	MakerPlaced     int64                `json:"makerPlaced"`
	MakerCancelled  int64                `json:"makerCancelled"`
	MakerFills      int64                `json:"makerFills"`
	PickoffFills    int64                `json:"pickoffFills"`
	TakerFills      int64                `json:"takerFills"`
	RoundTrips      int64                `json:"roundTrips"`
	WinTrips        int64                `json:"winTrips"`
	GrossWinUsd     float64              `json:"grossWinUsd"`
	GrossLossUsd    float64              `json:"grossLossUsd"`
	ProfitFactor    float64              `json:"profitFactor"`
	RecentTripPnls  []float64            `json:"recentTripPnls,omitempty"`
	GrossVolumeUsd  float64              `json:"grossVolumeUsd"`
	TxTimestamps    []int64              `json:"-"`
	Positions       map[string]*Position `json:"-"`
	PendingPairPnl  map[string]float64   `json:"-"`
	PosSnap         []Position           `json:"positions"`
	ActiveMakers    int                  `json:"activeMakers"`
	EquityCurve     []EquityPoint        `json:"equityCurve"`
	StatusNote      string               `json:"statusNote"`
}

type ArenaTelemetry struct {
	Version        string             `json:"version"`
	BootTsMs       int64              `json:"bootTsMs"`
	NowTsMs        int64              `json:"nowTsMs"`
	UptimeSec      float64            `json:"uptimeSec"`
	UptimeHours    float64            `json:"uptimeHours"`
	TargetHourly   float64            `json:"targetHourlyUsd"`
	BudgetPerStrat float64            `json:"budgetPerStratUsd"`
	BestStratID    string             `json:"bestStratId"`
	BestHourlyUsd  float64            `json:"bestHourlyUsd"`
	GoalMet        bool               `json:"goalMet"`
	FeedStats      map[string]int64   `json:"feedStats"`
	MarketSpreads  []MarketSpreadSnap `json:"marketSpreads"`
	Strategies     []*StrategyAccount `json:"strategies"`
	RecentFills    []FillAudit        `json:"recentFills"`
	ClosedTrips    []ClosedTripRecord `json:"closedTrips,omitempty"`
}

type MarketSpreadSnap struct {
	Symbol      string  `json:"sym"`
	LtBid       float64 `json:"ltBid"`
	LtAsk       float64 `json:"ltAsk"`
	LtSpreadBp  float64 `json:"ltSpdBp"`
	BnBid       float64 `json:"bnBid"`
	BnAsk       float64 `json:"bnAsk"`
	BnSpreadBp  float64 `json:"bnSpdBp"`
	BasisBp     float64 `json:"basisBp"`
	Fund1hBp    float64 `json:"fund1hBp"`
	DailyTrades int64   `json:"dailyTrades"`
}

var (
	mu            sync.Mutex
	bootMs        = time.Now().UnixMilli()
	ordSeq        atomic.Int64
	ltSpecs       = map[string]*LtMarketSpec{}
	bnUsdcSpecs   = map[string]*BnUsdcSpec{} // key: UsdcSym (e.g. WLDUSDC)
	asterSpecs    = map[string]*BnUsdcSpec{} // key: Aster symbol (e.g. NVDAUSDT, NVDAUSD1, LITUSDT)
	ltIDToSym     = map[int64]string{}
	ltRawBids     = map[string]map[string]float64{}
	ltRawAsks     = map[string]map[string]float64{}
	books         = map[string]*LiveBook{} // key: venue + ":" + symbol
	midHist       = map[string][]PriceSample{}
	ltFund1hBp    = map[string]float64{} // hourly funding rate in bps (+ means longs pay shorts)
	bnFund1hBp    = map[string]float64{}
	liqRecent     = map[string]*ForceLiqEvent{}
	makerOrders   = map[int64]*SimMakerOrder{}
	takerOrders   = map[int64]*SimTakerOrder{}
	strats        = map[string]*StrategyAccount{}
	stratOrder    = []string{}
	recentFills   = []FillAudit{}
	closedTrips   = []ClosedTripRecord{}
	feedCounters  = map[string]*atomic.Int64{}
	dnBasisEMA    = map[string]float64{}
	stratCoolMap  = map[string]int64{} // key: stratID+":"+sym -> coolUntilMs
	httpCli       = &http.Client{
		Timeout: 6 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
	}
)

var trackedLtSymbols = []string{
	"VVV", "DRV", "XTEN", "PRL", "VARIATIONAL", "LIT", "ANT", "XMR", "IBM",
	"HYPE", "ENA", "NEAR", "UNI", "WLD", "AAVE", "SUI", "ARB", "LINK", "DOGE",
	"1000PEPE", "XRP", "AVAX", "PENGU", "TRUMP", "TIA", "WLFI", "KAITO", "CRV", "ETHFI",
	"SPY", "US500", "QQQ", "US100", "AAPL", "NVDA", "TSLA", "AMZN", "META", "COIN", "MSTR", "BTC", "ETH", "SOL",
}

func incFeed(name string) {
	if c, ok := feedCounters[name]; ok {
		c.Add(1)
	}
}

func bookKey(venue, sym string) string {
	return venue + ":" + sym
}

func posKey(venue, sym string) string {
	return venue + ":" + sym
}

func roundTo(v float64, dec int) float64 {
	if dec < 0 {
		dec = 0
	}
	p := math.Pow10(dec)
	return math.Round(v*p) / p
}

func floorTo(v float64, dec int) float64 {
	if dec < 0 {
		dec = 0
	}
	p := math.Pow10(dec)
	return math.Floor(v*p+1e-9) / p
}

func ceilTo(v float64, dec int) float64 {
	if dec < 0 {
		dec = 0
	}
	p := math.Pow10(dec)
	return math.Ceil(v*p-1e-9) / p
}

func getBookLocked(venue, sym string) *LiveBook {
	k := bookKey(venue, sym)
	b := books[k]
	if b == nil {
		b = &LiveBook{Venue: venue, Symbol: sym}
		books[k] = b
	}
	return b
}

func recordMidLocked(key string, mid float64, nowMs int64) {
	if mid <= 0 {
		return
	}
	arr := midHist[key]
	if len(arr) > 0 && nowMs-arr[len(arr)-1].TsMs < 80 {
		arr[len(arr)-1].Mid = mid
		return
	}
	arr = append(arr, PriceSample{TsMs: nowMs, Mid: mid})
	cutoff := nowMs - 15000
	idx := 0
	for idx < len(arr) && arr[idx].TsMs < cutoff {
		idx++
	}
	if idx > 0 {
		arr = append([]PriceSample(nil), arr[idx:]...)
	}
	midHist[key] = arr
}

func midReturnBpLocked(key string, lookbackMs int64, nowMs int64) (float64, bool) {
	arr := midHist[key]
	if len(arr) < 2 {
		return 0, false
	}
	cur := arr[len(arr)-1]
	if nowMs-cur.TsMs > 3000 || cur.Mid <= 0 {
		return 0, false
	}
	target := nowMs - lookbackMs
	ref := arr[0]
	found := false
	for i := len(arr) - 1; i >= 0; i-- {
		if arr[i].TsMs <= target {
			ref = arr[i]
			found = true
			break
		}
	}
	if !found {
		if cur.TsMs-arr[0].TsMs < lookbackMs/2 {
			return 0, false
		}
		ref = arr[0]
	}
	if ref.Mid <= 0 {
		return 0, false
	}
	return ((cur.Mid - ref.Mid) / ref.Mid) * 10000.0, true
}

func stratPrefix(stratID string) string {
	if idx := strings.IndexByte(stratID, '_'); idx > 0 {
		return stratID[:idx]
	}
	return stratID
}

func countOpenPairsLocked(s *StrategyAccount) int {
	if s == nil {
		return 0
	}
	seen := map[string]bool{}
	for _, p := range s.Positions {
		if math.Abs(p.Qty) > 1e-8 {
			seen[symBase(p.Symbol)] = true
		}
	}
	// Also count in-flight entry Taker orders (e.g. S9 385ms IOC speed-bump) so we NEVER exceed MaxOpenPairs (3x $240 = <=3.0x)!
	for _, to := range takerOrders {
		if to.StratID == s.ID && !to.ReduceOnly && !strings.Contains(to.Tag, "_HEDGE") && !strings.Contains(to.Tag, "_CLOSE") {
			seen[symBase(to.Symbol)] = true
		}
	}
	return len(seen)
}

func l2TopDepthUsdLocked(venue, sym, side string) float64 {
	b := books[bookKey(venueBookPrefix(venue), sym)]
	if b == nil || b.Bid <= 0 || b.Ask <= 0 {
		return 0
	}
	if side == "BUY" {
		if len(b.Asks) > 0 {
			tot := 0.0
			lim := b.Ask * 1.0008
			for _, a := range b.Asks {
				if a.Price > lim {
					break
				}
				tot += a.Price * a.Size
			}
			return tot
		}
		return b.Ask * b.AskSz
	}
	if len(b.Bids) > 0 {
		tot := 0.0
		lim := b.Bid * 0.9992
		for _, bd := range b.Bids {
			if bd.Price < lim {
				break
			}
			tot += bd.Price * bd.Size
		}
		return tot
	}
	return b.Bid * b.BidSz
}

func initStrategies() {
	defs := []*StrategyAccount{
		{
			ID:        "S10_BN_USDC_MAKER_FIRST",
			Name:      "0% BN USDC-M Maker -> 0% Lighter IOC (#1 Leader, 0bp Fee)",
			Desc:      "Rests 0% GTX Maker on liquid BN USDC-M (>=5.0bp positive raw spread) -> 0% Lighter #770015 IOC hedge -> 0% BN Maker exit (true 0.00bp 4-leg fee!).",
			VenueDesc: "BN_USDC 0% M -> LT_STD 0% IOC",
			MaxLevCap: 3.0,
		},
		{
			ID:        "S9_PURE_CROSSED_TAKER",
			Name:      "Pure Crossed-Book Taker Grab (Bid_A - Ask_B >= +8.5bp)",
			Desc:      "Fires instant 0% Lighter IOC + <8ms BN USDC-M Taker ONLY when true raw crossed spread >= +8.5bp (>=+4.5bp net locked at entry! 3x $240 slots).",
			VenueDesc: "LT_STD 0% IOC + BN_USDC (>=8.5bp)",
			MaxLevCap: 3.0,
		},
		{
			ID:        "S2_LT_BN_FAST_ROTATE",
			Name:      "0% Lighter Maker -> BN USDC-M Fast Rotate (4.00bp Fee)",
			Desc:      "0% Lighter #770015 Maker (>=6.5bp positive raw spread) -> BN USDC-M hedge -> 0% BN Maker exit with 6m step-down & 15m time-stop (3x $240 slots).",
			VenueDesc: "LT_STD 0% M + BN_USDC 0% M Exit",
			MaxLevCap: 3.0,
		},
		{
			ID:        "S7_BN_HIGH_CONV_RAW",
			Name:      "High-Conviction >=8.5bp Raw Spread (LT <-> BN USDC-M)",
			Desc:      "Pure USDC-to-USDC high-conviction >=8.5bp positive raw spread Lighter Maker <-> BN USDC-M with zero USDT FX drift (3x $240 slots).",
			VenueDesc: "LT_STD <-> BN_USDC (>=8.5bp Raw)",
			MaxLevCap: 3.0,
		},
	}

	nowInitMs := time.Now().UnixMilli()
	for _, s := range defs {
		s.StartCash = StartBudgetUSD
		s.StratBootMs = nowInitMs
		s.Cash = StartBudgetUSD
		s.Equity = StartBudgetUSD
		s.PeakEquity = StartBudgetUSD
		s.Positions = map[string]*Position{}
		s.PendingPairPnl = map[string]float64{}
		strats[s.ID] = s
		stratOrder = append(stratOrder, s.ID)
	}
}

func canSpendTxLocked(s *StrategyAccount, venue string, nowMs int64) bool {
	vp := venuePhys[venue]
	cutoff := nowMs - 60000
	idx := 0
	for idx < len(s.TxTimestamps) && s.TxTimestamps[idx] < cutoff {
		idx++
	}
	if idx > 0 {
		s.TxTimestamps = append([]int64(nil), s.TxTimestamps[idx:]...)
	}
	if len(s.TxTimestamps) >= vp.MaxTxPerMin {
		return false
	}
	s.TxTimestamps = append(s.TxTimestamps, nowMs)
	return true
}

func stratGrossNotionalLocked(s *StrategyAccount) float64 {
	gross := 0.0
	for _, p := range s.Positions {
		if math.Abs(p.Qty) <= 1e-9 {
			continue
		}
		mark := markPriceLocked(p.Venue, p.Symbol, p.AvgPx)
		gross += math.Abs(p.Qty) * mark
	}
	return gross
}

func venueBookPrefix(venue string) string {
	if venue == VenueLtStd || venue == VenueLtPrem {
		return "LT"
	}
	if venue == VenueAsterC || venue == VenueAsterRWA {
		return "ASTER"
	}
	return venue
}

func hasPendingTakerLocked(stratID, venue, sym string) bool {
	vp := venueBookPrefix(venue)
	for _, to := range takerOrders {
		if to.StratID == stratID && (venue == "" || venueBookPrefix(to.Venue) == vp) && (sym == "" || to.Symbol == sym) {
			return true
		}
	}
	return false
}

func markPriceLocked(venue, sym string, fallback float64) float64 {
	b := books[bookKey(venueBookPrefix(venue), sym)]
	if b != nil && b.Bid > 0 && b.Ask > 0 {
		return 0.5 * (b.Bid + b.Ask)
	}
	return fallback
}

func queueAtLimitLocked(venue, sym, side string, limitPx float64) (float64, bool) {
	b := books[bookKey(venueBookPrefix(venue), sym)]
	if b == nil || b.Bid <= 0 || b.Ask <= 0 {
		return 0, false
	}
	// Post-only check: reject if crossing current opposite BBO
	if side == "BUY" && limitPx >= b.Ask-1e-12 {
		return 0, false
	}
	if side == "SELL" && limitPx <= b.Bid+1e-12 {
		return 0, false
	}
	qUsd := 0.0
	if side == "BUY" {
		if len(b.Bids) > 0 {
			for _, lv := range b.Bids {
				if lv.Price >= limitPx-1e-9 {
					qUsd += lv.Price * lv.Size
				}
			}
		} else if math.Abs(limitPx-b.Bid)/b.Bid < 0.00015 {
			qUsd = b.Bid * b.BidSz
		}
	} else {
		if len(b.Asks) > 0 {
			for _, lv := range b.Asks {
				if lv.Price <= limitPx+1e-9 {
					qUsd += lv.Price * lv.Size
				}
			}
		} else if math.Abs(limitPx-b.Ask)/b.Ask < 0.00015 {
			qUsd = b.Ask * b.AskSz
		}
	}
	return qUsd, true
}

func liveMeasuredLatMs(venue, op string) int64 {
	bnRtt := liveBnRttMs.Load()
	if bnRtt <= 0 {
		bnRtt = 8
	}
	asRtt := liveAsterRttMs.Load()
	if asRtt <= 0 {
		asRtt = 14
	}
	ltRtt := liveLtRttMs.Load()
	if ltRtt <= 0 {
		ltRtt = 85
	}
	switch venue {
	case VenueBnUsdc, VenueBnUsdt:
		return bnRtt + 2 // real HMAC sign + Tokyo TCP dispatch
	case VenueAsterC, VenueAsterRWA:
		return asRtt + 4 // real EIP-712 keccak256 sign + Tokyo TCP dispatch
	case VenueLtStd:
		if op == "PLACE" {
			return ltRtt // 0ms Maker speed-bump, only wire RTT to zkLighter sequencer
		}
		return 305 + ltRtt // 300ms Standard speed-bump + real wire RTT to zkLighter sequencer
	case VenueLtPrem:
		if op == "PLACE" || op == "CANCEL" {
			return ltRtt // 0ms Prem Maker/Cancel speed-bump + real wire RTT
		}
		return 150 + ltRtt // 150ms Prem Taker speed-bump + real wire RTT
	}
	return venuePhys[venue].TakerLatMs
}

func liveFillNotificationLagMs(venue string) int64 {
	switch venue {
	case VenueLtStd, VenueLtPrem:
		lag := liveLtPubLagMs.Load()
		if lag <= 0 {
			lag = liveLtRttMs.Load()
		}
		if lag < 25 {
			lag = 25
		}
		if lag > 1500 {
			lag = 1500
		}
		return lag
	case VenueAsterC, VenueAsterRWA:
		r := liveAsterRttMs.Load()
		if r <= 0 {
			return 8
		}
		return r / 2
	case VenueBnUsdc, VenueBnUsdt:
		r := liveBnRttMs.Load()
		if r <= 0 {
			return 4
		}
		return r / 2
	}
	return 5
}

func placeMakerLocked(stratID, venue, sym, side string, limitPx, qty float64, reduceOnly bool, tag string, nowMs int64) int64 {
	s := strats[stratID]
	if s == nil || qty <= 0 || limitPx <= 0 {
		return 0
	}
	if s.Equity < 120.0 && !reduceOnly {
		return 0
	}
	if !reduceOnly {
		gross := stratGrossNotionalLocked(s) + qty*limitPx
		if gross > s.Equity*s.MaxLevCap {
			return 0
		}
	}
	qUsd, ok := queueAtLimitLocked(venue, sym, side, limitPx)
	if !ok {
		return 0
	}
	if !canSpendTxLocked(s, venue, nowMs) {
		return 0
	}
	id := ordSeq.Add(1)
	makerOrders[id] = &SimMakerOrder{
		ID:                id,
		StratID:           stratID,
		Tag:               tag,
		Venue:             venue,
		Symbol:            sym,
		Side:              side,
		LimitPx:           limitPx,
		OrigQty:           qty,
		RemQty:            qty,
		QueueAheadUsd:     qUsd,
		InitQueueAheadUsd: qUsd,
		PlacedMs:          nowMs,
		ReadyMs:           nowMs + liveMeasuredLatMs(venue, "PLACE"),
		ReduceOnly:        reduceOnly,
	}
	s.MakerPlaced++
	return id
}

func requestCancelMakerLocked(id int64, nowMs int64) {
	o := makerOrders[id]
	if o == nil || o.CancelReqMs > 0 {
		return
	}
	s := strats[o.StratID]
	if s == nil {
		delete(makerOrders, id)
		return
	}
	if !canSpendTxLocked(s, o.Venue, nowMs) {
		return
	}
	o.CancelReqMs = nowMs
	o.CancelEffMs = nowMs + liveMeasuredLatMs(o.Venue, "CANCEL")
	s.MakerCancelled++
}

func placeTakerLocked(stratID, venue, sym, side string, qty, signalPx, maxSlipBp float64, reduceOnly bool, tag string, nowMs int64) int64 {
	s := strats[stratID]
	if s == nil || qty <= 0 || signalPx <= 0 {
		return 0
	}
	isHedge := strings.Contains(tag, "_HEDGE") || strings.Contains(tag, "_CLOSE")
	if !reduceOnly && !isHedge {
		gross := stratGrossNotionalLocked(s) + qty*signalPx
		if gross > s.Equity*(s.MaxLevCap+0.5) {
			return 0
		}
	}
	if !isHedge && !canSpendTxLocked(s, venue, nowMs) {
		return 0
	}
	extraFillLag := int64(0)
	if isHedge {
		// Wait for real Maker-fill WS notification propagation before Taker hedge/close can even be signed!
		if strings.HasSuffix(tag, "_CLOSE") {
			if venue == VenueLtStd || venue == VenueLtPrem {
				extraFillLag = liveFillNotificationLagMs(VenueBnUsdc)
			} else {
				extraFillLag = liveFillNotificationLagMs(VenueLtStd)
			}
		} else {
			if venue == VenueLtStd || venue == VenueLtPrem {
				extraFillLag = liveFillNotificationLagMs(VenueBnUsdc)
			} else {
				extraFillLag = liveFillNotificationLagMs(VenueLtStd)
			}
		}
	}
	id := ordSeq.Add(1)
	takerOrders[id] = &SimTakerOrder{
		ID:         id,
		StratID:    stratID,
		Tag:        tag,
		Venue:      venue,
		Symbol:     sym,
		Side:       side,
		Qty:        qty,
		SignalPx:   signalPx,
		PlacedMs:   nowMs,
		ExecAtMs:   nowMs + extraFillLag + liveMeasuredLatMs(venue, "TAKER"),
		MaxSlipBp:  maxSlipBp,
		ReduceOnly: reduceOnly,
	}
	return id
}

func symBase(sym string) string {
	s := strings.TrimSuffix(sym, "USDC")
	s = strings.TrimSuffix(s, "USDT")
	s = strings.TrimSuffix(s, "USD1")
	return s
}

func applyFillLocked(stratID, venue, sym, side, execType string, fillPx, sigPx, qty, feeBp, qInit float64, latMs int64, reduceOnly bool, tag string, nowMs int64) {
	s := strats[stratID]
	if s == nil || qty <= 1e-9 || fillPx <= 0 {
		return
	}
	if s.PendingPairPnl == nil {
		s.PendingPairPnl = map[string]float64{}
	}
	pk := posKey(venue, sym)
	p := s.Positions[pk]
	if reduceOnly {
		if p == nil || math.Abs(p.Qty) <= 1e-9 {
			return
		}
		if (p.Qty > 0 && side == "BUY") || (p.Qty < 0 && side == "SELL") {
			return
		}
		if qty > math.Abs(p.Qty) {
			qty = math.Abs(p.Qty)
		}
	}

	signedQty := qty
	if side == "SELL" {
		signedQty = -qty
	}
	notional := qty * fillPx
	feeUsd := notional * (feeBp / 10000.0) // negative feeBp = rebate!

	realizedDelta := 0.0
	closedLeg := false
	deferredEntryFee := 0.0
	releasedEntryFee := 0.0
	if p == nil {
		p = &Position{
			Venue:     venue,
			Symbol:    sym,
			Qty:       signedQty,
			AvgPx:     fillPx,
			EntryFee:  feeUsd,
			OpenedMs:  nowMs,
			UpdatedMs: nowMs,
			Tag:       tag,
		}
		s.Positions[pk] = p
		deferredEntryFee = feeUsd
	} else if (p.Qty > 0 && signedQty > 0) || (p.Qty < 0 && signedQty < 0) {
		newAbs := math.Abs(p.Qty) + qty
		p.AvgPx = (math.Abs(p.Qty)*p.AvgPx + qty*fillPx) / newAbs
		p.Qty += signedQty
		p.EntryFee += feeUsd
		p.UpdatedMs = nowMs
		deferredEntryFee = feeUsd
	} else {
		closeQty := math.Min(math.Abs(p.Qty), qty)
		if p.Qty > 0 {
			realizedDelta = closeQty * (fillPx - p.AvgPx)
		} else {
			realizedDelta = closeQty * (p.AvgPx - fillPx)
		}
		remSigned := p.Qty + signedQty
		if math.Abs(remSigned) <= 1e-8 {
			releasedEntryFee = p.EntryFee
			delete(s.Positions, pk)
			closedLeg = true
		} else if (p.Qty > 0 && remSigned > 0) || (p.Qty < 0 && remSigned < 0) {
			frac := closeQty / math.Max(math.Abs(p.Qty), 1e-9)
			releasedEntryFee = p.EntryFee * frac
			p.EntryFee -= releasedEntryFee
			p.Qty = remSigned
			p.UpdatedMs = nowMs
		} else {
			releasedEntryFee = p.EntryFee
			p.Qty = remSigned
			p.AvgPx = fillPx
			p.EntryFee = 0
			p.OpenedMs = nowMs
			p.UpdatedMs = nowMs
			p.Tag = tag
			closedLeg = true
		}
	}

	bookedFee := (feeUsd - deferredEntryFee) + releasedEntryFee
	s.RealizedGross += realizedDelta
	s.FeesPaid += bookedFee
	s.Cash = s.StartCash + s.RealizedGross - s.FeesPaid + s.FundingUsd
	s.GrossVolumeUsd += notional
	if strings.HasPrefix(execType, "MAKER") || strings.HasPrefix(execType, "PICKOFF") {
		s.MakerFills++
		if execType == "PICKOFF_AFTER_CANCEL" {
			s.PickoffFills++
		}
	} else {
		s.TakerFills++
	}

	// Count 1 RoundTrip, WinTrip & ProfitFactor ONLY when BOTH hedged legs of the base symbol (or stat-arb pair) are 100% flat!
	if closedLeg {
		groupKey := symBase(sym)
		legNet := realizedDelta - bookedFee
		stillOpen := false
		for _, op := range s.Positions {
			if math.Abs(op.Qty) > 1e-8 {
				ob := symBase(op.Symbol)
				if ob == groupKey {
					stillOpen = true
					break
				}
			}
		}
		if stillOpen {
			s.PendingPairPnl[groupKey] += legNet
		} else {
			combTripNet := s.PendingPairPnl[groupKey] + legNet
			delete(s.PendingPairPnl, groupKey)
			s.RoundTrips++
			if combTripNet > 0 {
				s.WinTrips++
				s.GrossWinUsd += combTripNet
			} else if combTripNet < 0 {
				s.GrossLossUsd += math.Abs(combTripNet)
			}
			s.RecentTripPnls = append(s.RecentTripPnls, roundTo(combTripNet, 4))
			if len(s.RecentTripPnls) > 12 {
				s.RecentTripPnls = append([]float64(nil), s.RecentTripPnls[len(s.RecentTripPnls)-12:]...)
			}
			closedTrips = append(closedTrips, ClosedTripRecord{
				TsMs:     nowMs,
				StratID:  stratID,
				Coin:     groupKey,
				Legs:     venue + " " + side,
				Notional: roundTo(notional, 2),
				FeeUsd:   roundTo(feeUsd, 4),
				NetUsd:   roundTo(combTripNet, 4),
				Reason:   tag,
			})
			if len(closedTrips) > 300 {
				closedTrips = append([]ClosedTripRecord(nil), closedTrips[len(closedTrips)-300:]...)
			}
			if s.GrossLossUsd > 1e-6 {
				s.ProfitFactor = roundTo(s.GrossWinUsd/s.GrossLossUsd, 2)
			} else if s.GrossWinUsd > 0 {
				s.ProfitFactor = 99.0
			} else {
				s.ProfitFactor = 0.0
			}
		}
	}

	slipBp := 0.0
	if sigPx > 0 {
		if side == "BUY" {
			slipBp = ((fillPx - sigPx) / sigPx) * 10000.0
		} else {
			slipBp = ((sigPx - fillPx) / sigPx) * 10000.0
		}
	}

	fa := FillAudit{
		TsMs:      nowMs,
		StratID:   stratID,
		Venue:     venue,
		Symbol:    sym,
		Side:      side,
		ExecType:  execType,
		FillPx:    fillPx,
		SignalPx:  sigPx,
		Qty:       qty,
		Notional:  roundTo(notional, 2),
		FeeUsd:    roundTo(feeUsd, 4),
		Realized:  roundTo(realizedDelta-feeUsd, 4),
		SlipBp:    roundTo(slipBp, 2),
		QueueInit: roundTo(qInit, 1),
		LatencyMs: latMs,
		Tag:       tag,
	}
	recentFills = append(recentFills, fa)
	if len(recentFills) > 120 {
		recentFills = append([]FillAudit(nil), recentFills[len(recentFills)-120:]...)
	}

	onStrategyFillHookLocked(stratID, venue, sym, side, fillPx, qty, tag, nowMs)
}

// processLiveTradeLocked evaluates EVERY real market trade print against resting Maker orders!
// sellerAggressor = true means a market SELL hit resting BUY bids (Binance m=true, Lighter is_maker_ask=false).
func processLiveTradeLocked(feedVenue, sym string, tradePx, tradeQty float64, sellerAggressor bool, nowMs int64) {
	if tradePx <= 0 || tradeQty <= 0 {
		return
	}
	tradeUsd := tradePx * tradeQty

	for id, o := range makerOrders {
		if venueBookPrefix(o.Venue) != feedVenue || o.Symbol != sym {
			continue
		}
		if o.CancelReqMs > 0 && nowMs >= o.CancelEffMs {
			delete(makerOrders, id)
			continue
		}
		if nowMs < o.ReadyMs {
			continue
		}

		vp := venuePhys[o.Venue]
		crossedStrictly := false
		exactTouch := false
		if o.Side == "BUY" {
			if tradePx < o.LimitPx-1e-10 {
				crossedStrictly = true
			} else if sellerAggressor && math.Abs(tradePx-o.LimitPx) <= math.Max(1e-9, o.LimitPx*0.000015) {
				exactTouch = true
			}
		} else {
			if tradePx > o.LimitPx+1e-10 {
				crossedStrictly = true
			} else if !sellerAggressor && math.Abs(tradePx-o.LimitPx) <= math.Max(1e-9, o.LimitPx*0.000015) {
				exactTouch = true
			}
		}

		if !crossedStrictly && !exactTouch {
			continue
		}

		fillQty := 0.0
		execType := "MAKER_QUEUE"
		if crossedStrictly {
			fillQty = o.RemQty
			execType = "MAKER_CROSS"
		} else if exactTouch {
			if o.QueueAheadUsd > tradeUsd {
				o.QueueAheadUsd -= tradeUsd
				continue
			}
			availUsd := tradeUsd - o.QueueAheadUsd
			o.QueueAheadUsd = 0
			availQty := availUsd / o.LimitPx
			if availQty <= 1e-9 {
				continue
			}
			fillQty = math.Min(o.RemQty, availQty)
		}

		if fillQty <= 1e-9 {
			continue
		}
		if o.CancelReqMs > 0 && nowMs < o.CancelEffMs {
			execType = "PICKOFF_AFTER_CANCEL"
		}
		lat := nowMs - o.PlacedMs
		o.RemQty -= fillQty
		if o.RemQty <= 1e-8 {
			delete(makerOrders, id)
		}
		applyFillLocked(o.StratID, o.Venue, o.Symbol, o.Side, execType, o.LimitPx, o.LimitPx, fillQty, vp.MakerFeeBp, o.InitQueueAheadUsd, lat, o.ReduceOnly, o.Tag, nowMs)
	}
}

func processDueTakersLocked(nowMs int64) {
	for id, to := range takerOrders {
		if nowMs < to.ExecAtMs {
			continue
		}
		delete(takerOrders, id)

		b := books[bookKey(venueBookPrefix(to.Venue), to.Symbol)]
		if b == nil || b.Bid <= 0 || b.Ask <= 0 || nowMs-b.UpdatedMs > 5000 {
			continue
		}

		vp := venuePhys[to.Venue]
		rem := to.Qty
		cost := 0.0
		filled := 0.0

		if to.Side == "BUY" {
			maxPx := to.SignalPx * (1.0 + to.MaxSlipBp/10000.0)
			if len(b.Asks) > 0 {
				for _, a := range b.Asks {
					if a.Price > maxPx || rem <= 1e-9 {
						break
					}
					take := math.Min(rem, a.Size)
					cost += take * a.Price
					filled += take
					rem -= take
				}
			} else if b.Ask <= maxPx {
				take := math.Min(rem, b.AskSz) // NEVER invent infinite size when L2 isn't loaded yet!
				cost += take * b.Ask
				filled += take
			}
		} else {
			minPx := to.SignalPx * (1.0 - to.MaxSlipBp/10000.0)
			if len(b.Bids) > 0 {
				for _, bd := range b.Bids {
					if bd.Price < minPx || rem <= 1e-9 {
						break
					}
					take := math.Min(rem, bd.Size)
					cost += take * bd.Price
					filled += take
					rem -= take
				}
			} else if b.Bid >= minPx {
				take := math.Min(rem, b.BidSz)
				cost += take * b.Bid
				filled += take
			}
		}

		if filled > 1e-9 {
			vwap := cost / filled
			applyFillLocked(to.StratID, to.Venue, to.Symbol, to.Side, "TAKER_BOOKWALK", vwap, to.SignalPx, filled, vp.TakerFeeBp, 0, nowMs-to.PlacedMs, to.ReduceOnly, to.Tag, nowMs)
		}
	}

	// Expire completed cancels & reject GTX/PostOnly Maker orders if book crossed while in wire transit!
	for id, mo := range makerOrders {
		if mo.CancelReqMs > 0 && nowMs >= mo.CancelEffMs {
			delete(makerOrders, id)
			continue
		}
		if !mo.Activated && nowMs >= mo.ReadyMs {
			b := books[bookKey(venueBookPrefix(mo.Venue), mo.Symbol)]
			if b != nil && b.Bid > 0 && b.Ask > 0 {
				if (mo.Side == "BUY" && mo.LimitPx >= b.Ask-1e-12) || (mo.Side == "SELL" && mo.LimitPx <= b.Bid+1e-12) {
					// Real GTX / PostOnly rejection at matching engine arrival!
					if st := strats[mo.StratID]; st != nil {
						st.MakerCancelled++
					}
					delete(makerOrders, id)
					continue
				}
			}
			mo.Activated = true
		}
	}
}

func roundForVenueSymLocked(venue, sym string, qty float64) float64 {
	if venue == VenueLtStd || venue == VenueLtPrem {
		if sp := ltSpecs[sym]; sp != nil {
			return roundTo(qty, sp.SizeDecimals)
		}
	} else if venue == VenueBnUsdc || venue == VenueBnUsdt {
		base := strings.TrimSuffix(strings.TrimSuffix(sym, "USDC"), "USDT")
		if bs := bnUsdcSpecs[base+"USDC"]; bs != nil && venue == VenueBnUsdc {
			return roundTo(qty, bs.QtyDec)
		}
		if sp := ltSpecs[base]; sp != nil && sp.BnQtyDec >= 0 {
			return roundTo(qty, sp.BnQtyDec)
		}
	} else if venue == VenueAsterC || venue == VenueAsterRWA {
		if as := asterSpecs[sym]; as != nil {
			return roundTo(qty, as.QtyDec)
		}
	}
	return roundTo(qty, 3)
}

// onStrategyFillHookLocked handles uniform hedge/close firing for ALL Maker legs tagged "E|<v>|<s>|<pk>" or "X|<v>|<s>|<pk>"!
func onStrategyFillHookLocked(stratID, venue, sym, side string, fillPx, qty float64, tag string, nowMs int64) {
	isEntry := strings.HasPrefix(tag, "E|")
	isExit := strings.HasPrefix(tag, "X|")
	if !isEntry && !isExit {
		return
	}
	parts := strings.Split(tag, "|")
	if len(parts) < 3 {
		return
	}
	peerVenue := parts[1]
	peerSym := parts[2]
	hedgeSide := "SELL"
	if side == "SELL" {
		hedgeSide = "BUY"
	}

	pb := books[bookKey(venueBookPrefix(peerVenue), peerSym)]
	sigPx := fillPx
	if pb != nil && pb.Bid > 0 && pb.Ask > 0 {
		if hedgeSide == "BUY" {
			sigPx = pb.Ask
		} else {
			sigPx = pb.Bid
		}
	}
	// Dollar-match cross-asset pairs (S8 BTC/ETH etc.) when peerSym base differs from sym!
	rawQty := qty
	if len(parts) >= 4 && parts[3] == "DM" && sigPx > 0 {
		rawQty = (qty * fillPx) / sigPx
	}
	hQty := roundForVenueSymLocked(peerVenue, peerSym, rawQty)
	if hQty <= 0 {
		return
	}
	hTag := stratPrefix(stratID) + "_HEDGE"
	if isExit {
		hTag = stratPrefix(stratID) + "_CLOSE"
	}
	maxSlip := 18.0
	if peerVenue == VenueLtStd || peerVenue == VenueLtPrem {
		maxSlip = 25.0 // 25bp 0% Lighter IOC hedge/close bound matching live_arena.go (eliminates false IOC_MISS_FLAT aborts!)
	}
	placeTakerLocked(stratID, peerVenue, peerSym, hedgeSide, hQty, sigPx, maxSlip, isExit, hTag, nowMs)
}

func cancelStaleOrRefreshMakerLocked(stratID, venue, sym, side string, targetPx, qty, cancelTolBp float64, reduceOnly bool, tag string, maxAgeMs int64, nowMs int64) {
	var existing *SimMakerOrder
	for _, mo := range makerOrders {
		if mo.StratID == stratID && mo.Venue == venue && mo.Symbol == sym && mo.Side == side && mo.CancelReqMs == 0 {
			existing = mo
			break
		}
	}
	if existing != nil {
		diffBp := math.Abs(existing.LimitPx-targetPx) / targetPx * 10000.0
		if diffBp <= cancelTolBp && nowMs-existing.PlacedMs < maxAgeMs {
			return
		}
		requestCancelMakerLocked(existing.ID, nowMs)
		return
	}
	if qty > 0 && targetPx > 0 {
		placeMakerLocked(stratID, venue, sym, side, targetPx, qty, reduceOnly, tag, nowMs)
	}
}

func cancelAllMakersForSymLocked(stratID, venue, sym string, nowMs int64) {
	for _, mo := range makerOrders {
		if mo.StratID == stratID && mo.Venue == venue && mo.Symbol == sym && mo.CancelReqMs == 0 {
			requestCancelMakerLocked(mo.ID, nowMs)
		}
	}
}

// ============================================================================
// STRATEGY EVALUATION LOOP (Runs every 50ms on live Tokyo feeds)
// ============================================================================

func usdtFxLocked(venue string) float64 {
	if venue != VenueBnUsdt && venue != VenueAsterC && venue != VenueAsterRWA {
		return 1.0
	}
	bu := books[bookKey(VenueBnUsdt, "BTCUSDT")]
	bc := books[bookKey(VenueBnUsdc, "BTCUSDC")]
	if bu != nil && bc != nil && bu.Bid > 0 && bc.Bid > 0 {
		fx := (0.5 * (bu.Bid + bu.Ask)) / (0.5 * (bc.Bid + bc.Ask))
		if fx > 0.985 && fx < 1.015 {
			return fx
		}
	}
	return 1.0005
}

type PairArbCfg struct {
	StratID         string
	MakerVenue      string // e.g. VenueLtStd, VenueBnUsdc, VenueAsterRWA
	MakerSym        string
	HedgeVenue      string // e.g. VenueBnUsdc, VenueAsterC, VenueAsterRWA, VenueLtStd
	HedgeSym        string
	SlotUsd         float64
	MaxOpenPairs    int
	MinRawEdgeBp    float64 // STRICT positive raw price spread required above 4-leg fees (SellPx > BuyPx!)
	EntryDislocBp   float64 // dislocation vs EMA fair basis required to quote Maker entry
	TakerGrabMinBp  float64 // if >0, fires instant Taker IOC on MakerVenue ONLY when true raw crossed spread >= TakerGrabMinBp!
	NetTargetBp     float64 // required net profit above all 4-leg fees for Maker exit
	DecayAfterMs    int64
	MaxHoldMs       int64
	StopLossBp      float64
	MinPeerDepthMul float64
	ExitOnMaker     bool // true for S1/S10: exit Maker rests on BN/Aster (0%) -> 0% LT_STD IOC close = true 0.00bp 4-leg fee!
	CancelTolBp     float64
	MaxAgeMs        int64
	CarryBias       int // 0=both sides, -1=only SHORT MakerSym / LONG HedgeSym (positive carry!)
	FxAdjMaker      bool
	FxAdjHedge      bool
}

func runCrossVenuePairArbLocked(cfg PairArbCfg, commonQtyDec int, minBase float64, nowMs int64) {
	acct := strats[cfg.StratID]
	if acct == nil {
		return
	}
	bM := books[bookKey(venueBookPrefix(cfg.MakerVenue), cfg.MakerSym)]
	bH := books[bookKey(venueBookPrefix(cfg.HedgeVenue), cfg.HedgeSym)]
	if bM == nil || bH == nil || bM.Bid <= 0 || bM.Ask <= bM.Bid || bH.Bid <= 0 || bH.Ask <= bH.Bid {
		return
	}
	if nowMs-bM.UpdatedMs > 2500 || nowMs-bH.UpdatedMs > 2500 {
		return
	}
	fxM, fxH := 1.0, 1.0
	if cfg.FxAdjMaker {
		fxM = usdtFxLocked(cfg.MakerVenue)
	}
	if cfg.FxAdjHedge {
		fxH = usdtFxLocked(cfg.HedgeVenue)
	}
	mBidUsd := bM.Bid / fxM
	mAskUsd := bM.Ask / fxM
	mMidUsd := 0.5 * (mBidUsd + mAskUsd)
	hBidUsd := bH.Bid / fxH
	hAskUsd := bH.Ask / fxH
	hMidUsd := 0.5 * (hBidUsd + hAskUsd)

	mSpdBp := ((bM.Ask - bM.Bid) / (0.5 * (bM.Bid + bM.Ask))) * 10000.0
	hSpdBp := ((bH.Ask - bH.Bid) / (0.5 * (bH.Bid + bH.Ask))) * 10000.0

	emaKey := cfg.StratID + ":" + cfg.MakerSym + ":" + cfg.HedgeSym
	rawBasisBp := ((mMidUsd - hMidUsd) / hMidUsd) * 10000.0
	if _, ok := dnBasisEMA[emaKey]; !ok {
		dnBasisEMA[emaKey] = rawBasisBp
	} else {
		dnBasisEMA[emaKey] = dnBasisEMA[emaKey]*0.996 + rawBasisBp*0.004
	}
	eqBasisBp := dnBasisEMA[emaKey]

	posM := acct.Positions[posKey(cfg.MakerVenue, cfg.MakerSym)]
	posH := acct.Positions[posKey(cfg.HedgeVenue, cfg.HedgeSym)]
	pfx := stratPrefix(cfg.StratID)

	// STRICT IN-FLIGHT GUARD: never double-hedge or declare orphan while a Taker leg is executing!
	pendingM := hasPendingTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym)
	pendingH := hasPendingTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym)

	var rtFeeBp float64
	if cfg.ExitOnMaker {
		rtFeeBp = venuePhys[cfg.MakerVenue].MakerFeeBp + venuePhys[cfg.HedgeVenue].TakerFeeBp +
			venuePhys[cfg.MakerVenue].MakerFeeBp + venuePhys[cfg.HedgeVenue].TakerFeeBp
	} else {
		rtFeeBp = venuePhys[cfg.MakerVenue].MakerFeeBp + venuePhys[cfg.HedgeVenue].TakerFeeBp +
			venuePhys[cfg.HedgeVenue].MakerFeeBp + venuePhys[cfg.MakerVenue].TakerFeeBp
	}
	if cfg.TakerGrabMinBp > 0 {
		rtFeeBp = venuePhys[cfg.MakerVenue].TakerFeeBp + venuePhys[cfg.HedgeVenue].TakerFeeBp +
			venuePhys[cfg.HedgeVenue].MakerFeeBp + venuePhys[cfg.MakerVenue].TakerFeeBp
	}

	if posM != nil && math.Abs(posM.Qty) > 1e-8 {
		for _, mo := range makerOrders {
			if mo.StratID == cfg.StratID && mo.Venue == cfg.MakerVenue && mo.Symbol == cfg.MakerSym && strings.HasPrefix(mo.Tag, "E|") && mo.CancelReqMs == 0 {
				requestCancelMakerLocked(mo.ID, nowMs)
			}
		}
		if pendingM || pendingH {
			return
		}
		if posH == nil || math.Abs(posH.Qty) <= 1e-8 {
			if math.Abs(posM.Qty)*mMidUsd < 8.0 && nowMs-posM.OpenedMs > 850 {
				cSide, px := "SELL", bM.Bid
				if posM.Qty < 0 {
					cSide, px = "BUY", bM.Ask
				}
				placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, cSide, math.Abs(posM.Qty), px, 25.0, true, pfx+"_DUST_CLOSE", nowMs)
				return
			}
			// 1450ms Unhedged Fallback: wait past full bnRtt + 305ms Lighter speed-bump + ltRtt before declaring IOC miss!
			if cfg.HedgeVenue == VenueLtStd && nowMs-posM.OpenedMs > 1450 {
				cSide, px := "SELL", bM.Bid
				if posM.Qty < 0 {
					cSide, px = "BUY", bM.Ask
				}
				placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, cSide, math.Abs(posM.Qty), px, 25.0, true, pfx+"_IOC_MISS_FLAT", nowMs)
				stratCoolMap[cfg.StratID+":COOL:"+cfg.MakerSym] = nowMs + 8000
				return
			}
			if nowMs-posM.OpenedMs > 1350 {
				hSide, px := "BUY", bH.Ask
				if posM.Qty > 0 {
					hSide, px = "SELL", bH.Bid
				}
				hQty := roundTo(math.Abs(posM.Qty), commonQtyDec)
				if hQty > 0 {
					placeTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, hSide, hQty, px, 20.0, false, pfx+"_HEDGE", nowMs)
				}
			}
			return
		}

		// Manage hedged pair exit — give 1:1 hedged delta-neutral spreads ample breathing room to draw down & converge naturally!
		qty := math.Min(math.Abs(posM.Qty), math.Abs(posH.Qty))
		heldMs := nowMs - posM.OpenedMs
		decayMs := cfg.DecayAfterMs
		if decayMs <= 0 {
			decayMs = 900000 // 15m before gently stepping down target
		}
		reqNetBp := cfg.NetTargetBp
		if heldMs >= decayMs && heldMs < decayMs*2 {
			reqNetBp = math.Max(1.0, cfg.NetTargetBp*0.5)
		} else if heldMs >= decayMs*2 {
			reqNetBp = 0.4
		}

		// Emergency-only basis safety net (default -75bp / ~10% structural break, NOT tight -18bp noise stop!):
		var takerExitBp float64
		if posH.Qty > 0 {
			takerExitBp = ((hBidUsd - posH.AvgPx/fxH) / (posH.AvgPx / fxH) * 10000.0) + ((posM.AvgPx/fxM - mAskUsd) / (posM.AvgPx / fxM) * 10000.0)
		} else {
			takerExitBp = ((posH.AvgPx/fxH - hAskUsd) / (posH.AvgPx / fxH) * 10000.0) + ((mBidUsd - posM.AvgPx/fxM) / (posM.AvgPx / fxM) * 10000.0)
		}
		slBp := cfg.StopLossBp
		if slBp <= 0 {
			slBp = 75.0
		}
		if heldMs >= 60000 && takerExitBp <= -slBp {
			cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
			cancelAllMakersForSymLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, nowMs)
			sM, pxM := "SELL", bM.Bid
			if posM.Qty < 0 {
				sM, pxM = "BUY", bM.Ask
			}
			sH, pxH := "SELL", bH.Bid
			if posH.Qty < 0 {
				sH, pxH = "BUY", bH.Ask
			}
			placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, sM, math.Abs(posM.Qty), pxM, 25.0, true, pfx+"_SL_CLOSE", nowMs)
			placeTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, sH, math.Abs(posH.Qty), pxH, 25.0, true, pfx+"_SL_CLOSE", nowMs)
			stratCoolMap[cfg.StratID+":COOL:"+cfg.MakerSym] = nowMs + 25000
			return
		}

		if cfg.ExitOnMaker {
			// S1 / S10: Rest 0% GTX Maker on MakerVenue (BN_USDC / ASTER_RWA) -> 0% LT_STD IOC close!
			exitTag := "X|" + cfg.HedgeVenue + "|" + cfg.HedgeSym + "|EX"
			if posM.Qty > 0 {
				// Long Maker, Short Hedge -> rest SELL Maker at bM.Ask, close BUY on HedgeVenue at bH.Ask!
				exitGrossBp := ((mAskUsd - posM.AvgPx/fxM) / (posM.AvgPx / fxM) * 10000.0) + ((posH.AvgPx/fxH - hAskUsd) / (posH.AvgPx / fxH) * 10000.0)
				if exitGrossBp >= rtFeeBp+reqNetBp {
					cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "SELL", bM.Ask, qty, 1.2, true, exitTag, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
				}
			} else {
				exitGrossBp := ((posM.AvgPx/fxM - mBidUsd) / (posM.AvgPx / fxM) * 10000.0) + ((hBidUsd - posH.AvgPx/fxH) / (posH.AvgPx / fxH) * 10000.0)
				if exitGrossBp >= rtFeeBp+reqNetBp {
					cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "BUY", bM.Bid, qty, 1.2, true, exitTag, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
				}
			}
			return
		}

		exitTag := "X|" + cfg.MakerVenue + "|" + cfg.MakerSym + "|EX"
		if posH.Qty > 0 {
			exitGrossBp := ((hAskUsd - posH.AvgPx/fxH) / (posH.AvgPx / fxH) * 10000.0) + ((posM.AvgPx/fxM - mAskUsd) / (posM.AvgPx / fxM) * 10000.0)
			if exitGrossBp >= rtFeeBp+reqNetBp {
				cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, "SELL", bH.Ask, qty, 1.2, true, exitTag, 15000, nowMs)
			} else {
				cancelAllMakersForSymLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, nowMs)
			}
		} else {
			exitGrossBp := ((posH.AvgPx/fxH - hBidUsd) / (posH.AvgPx / fxH) * 10000.0) + ((mBidUsd - posM.AvgPx/fxM) / (posM.AvgPx / fxM) * 10000.0)
			if exitGrossBp >= rtFeeBp+reqNetBp {
				cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, "BUY", bH.Bid, qty, 1.2, true, exitTag, 15000, nowMs)
			} else {
				cancelAllMakersForSymLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, nowMs)
			}
		}
		return
	}

	if posH != nil && math.Abs(posH.Qty) > 1e-8 {
		if !pendingM && !pendingH && nowMs-posH.OpenedMs > 1200 {
			side, px := "SELL", bH.Bid
			if posH.Qty < 0 {
				side, px = "BUY", bH.Ask
			}
			placeTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, side, math.Abs(posH.Qty), px, 25.0, true, pfx+"_ORPHAN_CLOSE", nowMs)
		}
		return
	}

	if nowMs < stratCoolMap[cfg.StratID+":COOL:"+cfg.MakerSym] {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		return
	}

	maxPairs := cfg.MaxOpenPairs
	if maxPairs <= 0 {
		maxPairs = 3
	}
	if countOpenPairsLocked(acct) >= maxPairs {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		return
	}

	if mSpdBp > 10.0 || hSpdBp > 8.5 {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		return
	}

	hRet400, _ := midReturnBpLocked(bookKey(venueBookPrefix(cfg.HedgeVenue), cfg.HedgeSym), 400, nowMs)
	if math.Abs(hRet400) > 1.8 {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		return
	}

	// Exact Live commonStep Lot-Size Rounding Parity:
	stepM := math.Pow10(-commonQtyDec)
	stepH := math.Pow10(-commonQtyDec)
	minNot := 10.5
	if sp := ltSpecs[symBase(cfg.MakerSym)]; sp != nil {
		ltStep := math.Pow10(-sp.SizeDecimals)
		if cfg.MakerVenue == VenueLtStd || cfg.MakerVenue == VenueLtPrem {
			stepM = ltStep
		} else {
			stepH = ltStep
		}
		if sp.MinQuote > minNot {
			minNot = sp.MinQuote + 0.5
		}
	}
	if bs := bnUsdcSpecs[cfg.MakerSym]; bs != nil && bs.StepSize > 0 {
		stepM = bs.StepSize
	} else if as := asterSpecs[cfg.MakerSym]; as != nil && as.StepSize > 0 {
		stepM = as.StepSize
	}
	if bs := bnUsdcSpecs[cfg.HedgeSym]; bs != nil && bs.StepSize > 0 {
		stepH = bs.StepSize
	} else if as := asterSpecs[cfg.HedgeSym]; as != nil && as.StepSize > 0 {
		stepH = as.StepSize
	}
	commonStep := math.Max(stepM, stepH)
	qty := roundTo(math.Floor((cfg.SlotUsd/mMidUsd)/commonStep+1e-9)*commonStep, commonQtyDec)
	if qty*mMidUsd < minNot && (qty+commonStep)*mMidUsd <= cfg.SlotUsd+4.5 {
		qty = roundTo(qty+commonStep, commonQtyDec)
	}
	if qty < minBase || qty*mMidUsd < minNot {
		return
	}
	depthMul := cfg.MinPeerDepthMul
	if depthMul <= 0 {
		depthMul = 2.5
	}

	// CRITICAL FIX: Compute BOTH true raw price spread (SellPx - BuyPx > 0) AND EMA dislocation!
	rawShortBp := ((mAskUsd - hAskUsd) / hAskUsd) * 10000.0 // SELL Maker @ mAsk, BUY Hedge @ hAsk
	rawLongBp := ((hBidUsd - mBidUsd) / hBidUsd) * 10000.0  // BUY Maker @ mBid, SELL Hedge @ hBid
	shortDislocBp := rawShortBp - eqBasisBp
	longDislocBp := rawLongBp + eqBasisBp
	minRawBp := rtFeeBp + cfg.MinRawEdgeBp
	if cfg.MinRawEdgeBp <= 0 {
		minRawBp = rtFeeBp + 2.0
	}
	entryTag := "E|" + cfg.HedgeVenue + "|" + cfg.HedgeSym + "|EN"

	// Pure Crossed-Book Taker-Grab mode (S9): ONLY fires when true crossed raw spread >= TakerGrabMinBp!
	if cfg.TakerGrabMinBp > 0 {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		coolKey := cfg.StratID + ":TG:" + cfg.MakerSym
		if nowMs < stratCoolMap[coolKey] {
			return
		}
		rawCrossShortBp := ((mBidUsd - hAskUsd) / hAskUsd) * 10000.0
		rawCrossLongBp := ((hBidUsd - mAskUsd) / hBidUsd) * 10000.0
		if rawCrossShortBp >= cfg.TakerGrabMinBp && cfg.CarryBias <= 0 && l2TopDepthUsdLocked(cfg.HedgeVenue, cfg.HedgeSym, "BUY") >= cfg.SlotUsd*depthMul {
			if placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "SELL", qty, bM.Bid, 10.0, false, entryTag, nowMs) > 0 {
				stratCoolMap[coolKey] = nowMs + 3000
			}
		} else if rawCrossLongBp >= cfg.TakerGrabMinBp && cfg.CarryBias >= 0 && l2TopDepthUsdLocked(cfg.HedgeVenue, cfg.HedgeSym, "SELL") >= cfg.SlotUsd*depthMul {
			if placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "BUY", qty, bM.Ask, 10.0, false, entryTag, nowMs) > 0 {
				stratCoolMap[coolKey] = nowMs + 3000
			}
		}
		return
	}

	canShort := rawShortBp >= minRawBp && (rawShortBp >= cfg.EntryDislocBp || shortDislocBp >= cfg.EntryDislocBp) &&
		cfg.CarryBias <= 0 && l2TopDepthUsdLocked(cfg.HedgeVenue, cfg.HedgeSym, "BUY") >= cfg.SlotUsd*depthMul
	canLong := rawLongBp >= minRawBp && (rawLongBp >= cfg.EntryDislocBp || longDislocBp >= cfg.EntryDislocBp) &&
		cfg.CarryBias >= 0 && l2TopDepthUsdLocked(cfg.HedgeVenue, cfg.HedgeSym, "SELL") >= cfg.SlotUsd*depthMul

	if canShort {
		cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "SELL", bM.Ask, qty, cfg.CancelTolBp, false, entryTag, cfg.MaxAgeMs, nowMs)
	} else if canLong {
		cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "BUY", bM.Bid, qty, cfg.CancelTolBp, false, entryTag, cfg.MaxAgeMs, nowMs)
	} else {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
	}
}

func evalStrategiesLocked(nowMs int64) {
	bnLiquidSyms := []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "TRUMP", "TIA", "CRV", "ETHFI", "LINK", "DOGE", "XRP"}

	for _, sym := range bnLiquidSyms {
		sp := ltSpecs[sym]
		if sp == nil || sp.BnUsdcSym == "" || (sp.DailyTrades > 0 && sp.DailyTrades < 2500) {
			continue
		}
		dec := sp.SizeDecimals
		if sp.BnQtyDec < dec {
			dec = sp.BnQtyDec
		}

		// 1. S10: 0% BN USDC-M GTX Maker -> 0% Lighter IOC Hedge -> 0% BN Maker Exit (0.00bp 4-leg fee!)
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:         "S10_BN_USDC_MAKER_FIRST",
			MakerVenue:      VenueBnUsdc,
			MakerSym:        sp.BnUsdcSym,
			HedgeVenue:      VenueLtStd,
			HedgeSym:        sym,
			SlotUsd:         240.0,
			MaxOpenPairs:    3,
			MinRawEdgeBp:    5.0,
			EntryDislocBp:   5.0,
			NetTargetBp:     2.5,
			DecayAfterMs:    480000,
			MaxHoldMs:       900000,
			StopLossBp:      75.0,
			MinPeerDepthMul: 2.5,
			ExitOnMaker:     true,
			CancelTolBp:     1.5,
			MaxAgeMs:        20000,
		}, dec, sp.MinBase, nowMs)

		// 2. S9: Pure Crossed-Book Taker Grab (Bid_A - Ask_B >= +8.5bp -> >= +4.5bp Net Locked at Entry!)
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:         "S9_PURE_CROSSED_TAKER",
			MakerVenue:      VenueLtStd,
			MakerSym:        sym,
			HedgeVenue:      VenueBnUsdc,
			HedgeSym:        sp.BnUsdcSym,
			SlotUsd:         240.0,
			MaxOpenPairs:    3,
			TakerGrabMinBp:  8.5,
			NetTargetBp:     2.5,
			DecayAfterMs:    420000,
			MaxHoldMs:       900000,
			StopLossBp:      75.0,
			MinPeerDepthMul: 2.5,
		}, dec, sp.MinBase, nowMs)

		// 3. S2: 0% Lighter Std Maker (>=6.5bp raw) -> 4bp BN USDC-M Hedge -> 0% BN Maker Exit (Fast 6m Decay)
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:         "S2_LT_BN_FAST_ROTATE",
			MakerVenue:      VenueLtStd,
			MakerSym:        sym,
			HedgeVenue:      VenueBnUsdc,
			HedgeSym:        sp.BnUsdcSym,
			SlotUsd:         240.0,
			MaxOpenPairs:    3,
			MinRawEdgeBp:    2.5,
			EntryDislocBp:   6.5,
			NetTargetBp:     2.5,
			DecayAfterMs:    360000,
			MaxHoldMs:       900000,
			StopLossBp:      75.0,
			MinPeerDepthMul: 2.5,
			CancelTolBp:     2.5,
			MaxAgeMs:        30000,
		}, dec, sp.MinBase, nowMs)

		// 4. S7: High-Conviction >=8.5bp Positive Raw Spread Lighter <-> BN USDC-M
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:         "S7_BN_HIGH_CONV_RAW",
			MakerVenue:      VenueLtStd,
			MakerSym:        sym,
			HedgeVenue:      VenueBnUsdc,
			HedgeSym:        sp.BnUsdcSym,
			SlotUsd:         240.0,
			MaxOpenPairs:    3,
			MinRawEdgeBp:    4.5,
			EntryDislocBp:   8.5,
			NetTargetBp:     3.5,
			DecayAfterMs:    480000,
			MaxHoldMs:       900000,
			StopLossBp:      75.0,
			MinPeerDepthMul: 2.5,
			CancelTolBp:     2.5,
			MaxAgeMs:        35000,
		}, dec, sp.MinBase, nowMs)
	}
}

// settleHourlyFundingLocked applies true hourly Lighter & periodic Binance funding at top of hour!
func settleHourlyFundingLocked(nowMs int64) {
	for _, s := range strats {
		for _, p := range s.Positions {
			if math.Abs(p.Qty) <= 1e-9 {
				continue
			}
			mark := markPriceLocked(p.Venue, p.Symbol, p.AvgPx)
			notional := p.Qty * mark // >0 long, <0 short
			if p.Venue == VenueLtStd || p.Venue == VenueLtPrem {
				rBp := ltFund1hBp[p.Symbol] // >0 means longs pay shorts
				s.FundingUsd -= notional * (rBp / 10000.0)
			} else if p.Venue == VenueBnUsdc || p.Venue == VenueBnUsdt {
				// Apply 1h pro-rata equivalent or top-of-8h settlement
				rBp := bnFund1hBp[p.Symbol]
				s.FundingUsd -= notional * (rBp / 10000.0)
			}
				s.FundingUsd -= notional * (rBp / 10000.0)
			}
		}
		s.Cash = s.StartCash + s.RealizedGross - s.FeesPaid + s.FundingUsd
	}
}

func updateMarkToMarketAndWriteTelemetry() {
	mu.Lock()
	nowMs := time.Now().UnixMilli()
	upHours := math.Max((float64(nowMs-bootMs))/3600000.0, 1.0/3600.0)

	makerCntByStrat := map[string]int{}
	for _, mo := range makerOrders {
		if mo.CancelReqMs == 0 {
			makerCntByStrat[mo.StratID]++
		}
	}

	bestID := ""
	bestHr := -1e9
	outStrats := make([]*StrategyAccount, 0, len(stratOrder))

	for _, id := range stratOrder {
		s := strats[id]
		rawMarkUpnl := 0.0
		rawMidUpnl := 0.0
		openEntryFees := 0.0
		openExitFees := 0.0
		snaps := make([]Position, 0, len(s.Positions))
		for _, p := range s.Positions {
			if math.Abs(p.Qty) <= 1e-9 {
				continue
			}
			b := books[bookKey(venueBookPrefix(p.Venue), p.Symbol)]
			mark := p.AvgPx
			midMark := p.AvgPx
			if b != nil && b.Bid > 0 && b.Ask > 0 {
				midMark = 0.5 * (b.Bid + b.Ask)
				if p.Qty > 0 {
					mark = b.Bid // executable liquidation Bid!
				} else {
					mark = b.Ask // executable liquidation Ask!
				}
			}
			tFeeBp := 0.0
			if vp, ok := venuePhys[p.Venue]; ok {
				tFeeBp = vp.TakerFeeBp
			}
			openEntryFees += p.EntryFee
			openExitFees += (math.Abs(p.Qty) * mark) * (tFeeBp / 10000.0)
			if p.Qty > 0 {
				rawMarkUpnl += math.Abs(p.Qty) * (mark - p.AvgPx)
				rawMidUpnl += math.Abs(p.Qty) * (midMark - p.AvgPx)
			} else {
				rawMarkUpnl += math.Abs(p.Qty) * (p.AvgPx - mark)
				rawMidUpnl += math.Abs(p.Qty) * (p.AvgPx - midMark)
			}
			snaps = append(snaps, *p)
		}

		// Exact Net-Withdrawable Accounting (Zero double-deduction of open EntryFee; ClosedNetUsd = pure closed trips only!):
		s.Cash = roundTo(s.StartCash+s.RealizedGross-s.FeesPaid+s.FundingUsd, 4)
		s.ClosedNetUsd = roundTo((s.Cash-s.StartCash)+openEntryFees, 4)
		s.UnrealizedPnL = roundTo(rawMarkUpnl-openEntryFees-openExitFees, 4)
		s.Equity = roundTo(s.StartCash+s.ClosedNetUsd+s.UnrealizedPnL, 4)
		s.WithdrawableUsd = s.Equity
		sigUpnl := math.Min(0.0, rawMidUpnl-openEntryFees)
		if s.Equity > s.PeakEquity {
			s.PeakEquity = s.Equity
		}
		dd := s.PeakEquity - s.Equity
		if dd > s.MaxDrawdownUsd {
			s.MaxDrawdownUsd = roundTo(dd, 4)
		}
		netGain := s.ClosedNetUsd
		realGain := s.ClosedNetUsd
		stratStart := bootMs
		if s.StratBootMs > bootMs {
			stratStart = s.StratBootMs
		}
		stratUpHr := math.Max(float64(nowMs-stratStart)/3600000.0, 1.0/3600.0)
		// NOISE-FREE CLOSED-TRADES-ONLY RUN RATE: Minimum 15m (0.25h) time denominator; zero open unrealized basis noise!
		denomHr := math.Max(stratUpHr, 0.25)
		closedCashNow := roundTo(s.StartCash+s.ClosedNetUsd, 4)
		s.HourlyRunRate = roundTo(netGain/denomHr, 3)
		s.RealizedHourly = roundTo(realGain/denomHr, 3)
		s.ActiveMakers = makerCntByStrat[id]
		s.PosSnap = snaps

		if len(s.EquityCurve) == 0 || nowMs-s.EquityCurve[len(s.EquityCurve)-1].TsMs >= 30000 {
			s.EquityCurve = append(s.EquityCurve, EquityPoint{TsMs: nowMs, Equity: s.Equity, MidEq: closedCashNow, Cash: closedCashNow})
			if len(s.EquityCurve) > 2940 { // 2940 * 30s = 24.5 hours full rolling window!
				s.EquityCurve = append([]EquityPoint(nil), s.EquityCurve[len(s.EquityCurve)-2940:]...)
			}
		}

		calcRoll := func(winMs int64, minDtHr float64) float64 {
			if len(s.EquityCurve) < 2 {
				return s.RealizedHourly
			}
			tgt := nowMs - winMs
			refPt := s.EquityCurve[0]
			for _, pt := range s.EquityCurve {
				if pt.TsMs <= tgt {
					refPt = pt
				} else {
					break
				}
			}
			dtHr := math.Max(float64(nowMs-refPt.TsMs)/3600000.0, minDtHr)
			refClosed := refPt.Cash
			if refClosed <= 0 {
				refClosed = s.StartCash
			}
			return roundTo((closedCashNow-refClosed)/dtHr, 3)
		}

		s.Rolling15mHourly = calcRoll(15*60*1000, 0.25)
		s.Rolling45mHourly = calcRoll(45*60*1000, 0.25)
		s.Rolling2hHourly = calcRoll(2*3600*1000, 0.50)
		s.Rolling4hHourly = calcRoll(4*3600*1000, 0.50)
		s.Rolling24hHourly = calcRoll(24*3600*1000, 0.50)
		// NOISE GUARD: Do NOT show noisy $/hr before at least 1 round-trip closes (unless holding a real drawdown < -$1.00)!
		if s.RoundTrips == 0 && sigUpnl > -1.00 {
			s.HourlyRunRate = 0
			s.RealizedHourly = 0
			s.Rolling15mHourly = 0
			s.Rolling45mHourly = 0
			s.Rolling2hHourly = 0
			s.Rolling4hHourly = 0
			s.Rolling24hHourly = 0
		}

		s.StatusNote = fmt.Sprintf("Closed $%+.3f | Open $%+.3f | PF %.2f | Trips %d",
			realGain, s.UnrealizedPnL, s.ProfitFactor, s.RoundTrips)

		if s.Rolling45mHourly > bestHr || (bestID == "" && s.RealizedHourly > bestHr) {
			bestHr = s.Rolling45mHourly
			bestID = id
		}
		outStrats = append(outStrats, s)
	}

	// Build market spread snapshot
	spreads := make([]MarketSpreadSnap, 0, 22)
	for _, sym := range []string{"VVV", "DRV", "PRL", "VARIATIONAL", "LIT", "XMR", "UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "AVAX", "PENGU", "AAPL", "QQQ", "SPY", "TSLA", "NVDA"} {
		spec := ltSpecs[sym]
		lb := books[bookKey("LT", sym)]
		if spec == nil || lb == nil || lb.Bid <= 0 || lb.Ask <= 0 {
			continue
		}
		ltMid := 0.5 * (lb.Bid + lb.Ask)
		ltSpd := ((lb.Ask - lb.Bid) / ltMid) * 10000.0
		var bnBid, bnAsk, bnSpd, basis float64
		bnVenue := ""
		bnKey := ""
		if spec.BnUsdcSym != "" {
			bnVenue = VenueBnUsdc
			bnKey = bookKey(VenueBnUsdc, spec.BnUsdcSym)
		} else if spec.BnUsdtSym != "" {
			bnVenue = VenueBnUsdt
			bnKey = bookKey(VenueBnUsdt, spec.BnUsdtSym)
		}
		if bb := books[bnKey]; bb != nil && bb.Bid > 0 && bb.Ask > 0 {
			bnBid, bnAsk = bb.Bid, bb.Ask
			bnMid := (0.5 * (bnBid + bnAsk)) / usdtFxLocked(bnVenue)
			bnSpd = ((bnAsk - bnBid) / (0.5 * (bnBid + bnAsk))) * 10000.0
			basis = ((ltMid - bnMid) / bnMid) * 10000.0
		}
		spreads = append(spreads, MarketSpreadSnap{
			Symbol:      sym,
			LtBid:       lb.Bid,
			LtAsk:       lb.Ask,
			LtSpreadBp:  roundTo(ltSpd, 2),
			BnBid:       bnBid,
			BnAsk:       bnAsk,
			BnSpreadBp:  roundTo(bnSpd, 2),
			BasisBp:     roundTo(basis, 2),
			Fund1hBp:    roundTo(ltFund1hBp[sym], 2),
			DailyTrades: spec.DailyTrades,
		})
	}

	fStats := map[string]int64{}
	for k, c := range feedCounters {
		fStats[k] = c.Load()
	}

	// Reverse recent fills so newest is first
	revFills := make([]FillAudit, len(recentFills))
	for i := range recentFills {
		revFills[i] = recentFills[len(recentFills)-1-i]
	}
	revClosed := make([]ClosedTripRecord, len(closedTrips))
	for i := range closedTrips {
		revClosed[i] = closedTrips[len(closedTrips)-1-i]
	}

	goalMet := false
	for _, s := range outStrats {
		if upHours >= 2.0 && s.HourlyRunRate >= TargetHourly && s.RealizedHourly >= TargetHourly && (s.MakerFills+s.TakerFills) >= 20 {
			goalMet = true
		}
	}

	tel := ArenaTelemetry{
		Version:        ArenaVer,
		BootTsMs:       bootMs,
		NowTsMs:        nowMs,
		UptimeSec:      roundTo(float64(nowMs-bootMs)/1000.0, 1),
		UptimeHours:    roundTo(upHours, 3),
		TargetHourly:   TargetHourly,
		BudgetPerStrat: StartBudgetUSD,
		BestStratID:    bestID,
		BestHourlyUsd:  roundTo(bestHr, 3),
		GoalMet:        goalMet,
		FeedStats:      fStats,
		MarketSpreads:  spreads,
		Strategies:     outStrats,
		RecentFills:    revFills,
		ClosedTrips:    revClosed,
	}

	raw, err := json.Marshal(tel)
	shouldSaveState := (nowMs - lastStateSaveMs) >= 10000
	if shouldSaveState && err == nil {
		lastStateSaveMs = nowMs
	}
	mu.Unlock()
	if err == nil {
		tmp := ArenaOutPath + ".tmp"
		if os.WriteFile(tmp, raw, 0644) == nil {
			_ = os.Rename(tmp, ArenaOutPath)
		}
		if shouldSaveState {
			stTmp := ArenaStatePath + ".tmp"
			if os.WriteFile(stTmp, raw, 0644) == nil {
				_ = os.Rename(stTmp, ArenaStatePath)
			}
		}
	}
}

var lastStateSaveMs int64

func loadCheckpoint() {
	raw, err := os.ReadFile(ArenaStatePath)
	if err != nil {
		return
	}
	var tel ArenaTelemetry
	// Clean zero-history cutover to ARENA-v6.3-PARITY (fresh 0m 00s clock & $489.12 clean slate!):
	if json.Unmarshal(raw, &tel) != nil || !strings.HasPrefix(tel.Version, "ARENA-v6.3") || tel.BootTsMs <= 0 {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	bootMs = tel.BootTsMs
	for _, saved := range tel.Strategies {
		s := strats[saved.ID]
		if s == nil {
			continue
		}
		s.Cash = saved.Cash
		s.RealizedGross = saved.RealizedGross
		s.FeesPaid = saved.FeesPaid
		s.FundingUsd = saved.FundingUsd
		s.UnrealizedPnL = saved.UnrealizedPnL
		s.Equity = saved.Equity
		s.PeakEquity = saved.PeakEquity
		s.MaxDrawdownUsd = saved.MaxDrawdownUsd
		s.MakerPlaced = saved.MakerPlaced
		s.MakerCancelled = saved.MakerCancelled
		s.MakerFills = saved.MakerFills
		s.PickoffFills = saved.PickoffFills
		s.TakerFills = saved.TakerFills
		s.RoundTrips = saved.RoundTrips
		s.WinTrips = saved.WinTrips
		s.GrossWinUsd = saved.GrossWinUsd
		s.GrossLossUsd = saved.GrossLossUsd
		s.ProfitFactor = saved.ProfitFactor
		s.RecentTripPnls = saved.RecentTripPnls
		s.GrossVolumeUsd = saved.GrossVolumeUsd
		s.EquityCurve = saved.EquityCurve
		if saved.StratBootMs > 0 {
			s.StratBootMs = saved.StratBootMs
		} else {
			s.StratBootMs = tel.BootTsMs
		}
		s.Positions = map[string]*Position{}
		for _, p := range saved.PosSnap {
			pc := p
			s.Positions[posKey(p.Venue, p.Symbol)] = &pc
		}
	}
	for i := len(tel.RecentFills) - 1; i >= 0; i-- {
		if strats[tel.RecentFills[i].StratID] != nil {
			recentFills = append(recentFills, tel.RecentFills[i])
		}
	}
	for i := len(tel.ClosedTrips) - 1; i >= 0; i-- {
		if strats[tel.ClosedTrips[i].StratID] != nil {
			closedTrips = append(closedTrips, tel.ClosedTrips[i])
		}
	}
	fmt.Printf("[%s] Restored live checkpoint from %s (uptime %.1fm)\n", ArenaVer, ArenaStatePath, float64(time.Now().UnixMilli()-bootMs)/60000.0)
}

// ============================================================================
// PUBLIC READ-ONLY EXCHANGE METADATA & LIVE WEBSOCKET FEEDS
// ============================================================================

func loadMarketSpecs() error {
	// 1. Fetch Binance Futures symbols + exact tick/step precision
	bnUsdc := map[string]bool{}
	bnUsdt := map[string]bool{}
	type bnPrec struct {
		pDec   int
		qDec   int
		tick   float64
		step   float64
		minNot float64
	}
	bnPrecMap := map[string]bnPrec{}
	if resp, err := httpCli.Get("https://fapi.binance.com/fapi/v1/exchangeInfo"); err == nil {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		var ex struct {
			Symbols []struct {
				Symbol            string `json:"symbol"`
				Status            string `json:"status"`
				ContractType      string `json:"contractType"`
				PricePrecision    int    `json:"pricePrecision"`
				QuantityPrecision int    `json:"quantityPrecision"`
				Filters           []struct {
					FilterType string `json:"filterType"`
					TickSize   string `json:"tickSize"`
					StepSize   string `json:"stepSize"`
					Notional   string `json:"notional"`
				} `json:"filters"`
			} `json:"symbols"`
		}
		if json.Unmarshal(body, &ex) == nil {
			for _, s := range ex.Symbols {
				if s.Status == "TRADING" && s.ContractType == "PERPETUAL" {
					tk, stp, mn := 0.0, 0.0, 5.5
					pDec := s.PricePrecision
					for _, f := range s.Filters {
						if f.FilterType == "PRICE_FILTER" {
							tk, _ = strconv.ParseFloat(f.TickSize, 64)
							if tk > 0 {
								pDec = int(math.Round(-math.Log10(tk)))
								if pDec < 0 {
									pDec = 0
								}
							}
						} else if f.FilterType == "LOT_SIZE" {
							stp, _ = strconv.ParseFloat(f.StepSize, 64)
						} else if f.FilterType == "MIN_NOTIONAL" {
							if v, e := strconv.ParseFloat(f.Notional, 64); e == nil && v > 0 {
								mn = v + 0.5
							}
						}
					}
					bnPrecMap[s.Symbol] = bnPrec{pDec: pDec, qDec: s.QuantityPrecision, tick: tk, step: stp, minNot: mn}
					if strings.HasSuffix(s.Symbol, "USDC") {
						bnUsdc[strings.TrimSuffix(s.Symbol, "USDC")] = true
					} else if strings.HasSuffix(s.Symbol, "USDT") {
						bnUsdt[strings.TrimSuffix(s.Symbol, "USDT")] = true
					}
				}
			}
		}
	}

	mu.Lock()
	for base := range bnUsdc {
		if base == "ZEC" {
			continue // ban toxic ZEC
		}
		uSym := base + "USDC"
		pr := bnPrecMap[uSym]
		usdtS := ""
		if bnUsdt[base] {
			usdtS = base + "USDT"
		}
		bnUsdcSpecs[uSym] = &BnUsdcSpec{
			BaseSym:  base,
			UsdcSym:  uSym,
			UsdtSym:  usdtS,
			PriceDec: pr.pDec,
			QtyDec:   pr.qDec,
			TickSize: pr.tick,
			StepSize: pr.step,
			MinNot:   pr.minNot,
		}
	}
	mu.Unlock()

	// 2. Fetch Lighter orderBookDetails
	resp, err := httpCli.Get("https://mainnet.zklighter.elliot.ai/api/v1/orderBookDetails")
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	var obd struct {
		Details []struct {
			Symbol             string  `json:"symbol"`
			MarketID           int64   `json:"market_id"`
			Status             string  `json:"status"`
			MinBaseAmount      string  `json:"min_base_amount"`
			MinQuoteAmount     string  `json:"min_quote_amount"`
			PriceDecimals      int     `json:"price_decimals"`
			SizeDecimals       int     `json:"size_decimals"`
			MinInitMarginFrac  float64 `json:"min_initial_margin_fraction"`
			DailyTradesCount   int64   `json:"daily_trades_count"`
		} `json:"order_book_details"`
	}
	if err := json.Unmarshal(body, &obd); err != nil {
		return err
	}

	want := map[string]bool{}
	for _, s := range trackedLtSymbols {
		want[s] = true
	}

	mu.Lock()
	for _, d := range obd.Details {
		if d.Status != "active" || !want[d.Symbol] {
			continue
		}
		minBase, _ := strconv.ParseFloat(d.MinBaseAmount, 64)
		minQuote, _ := strconv.ParseFloat(d.MinQuoteAmount, 64)
		maxLev := 5.0
		if d.MinInitMarginFrac > 0 {
			maxLev = 10000.0 / d.MinInitMarginFrac
		}
		sp := &LtMarketSpec{
			Symbol:        d.Symbol,
			MarketID:      d.MarketID,
			PriceDecimals: d.PriceDecimals,
			SizeDecimals:  d.SizeDecimals,
			MinBase:       minBase,
			MinQuote:      minQuote,
			MaxLev:        maxLev,
			DailyTrades:   d.DailyTradesCount,
		}
		// Ticker collision guard: PRL, BB, ARM, OPENAI on Binance are different assets!
		if d.Symbol != "PRL" && d.Symbol != "BB" && d.Symbol != "ARM" && d.Symbol != "OPENAI" {
			if bnUsdc[d.Symbol] {
				sp.BnUsdcSym = d.Symbol + "USDC"
				if pr, ok := bnPrecMap[sp.BnUsdcSym]; ok {
					sp.BnPriceDec, sp.BnQtyDec, sp.BnTick, sp.BnStep, sp.BnMinNot = pr.pDec, pr.qDec, pr.tick, pr.step, pr.minNot
				}
			}
			if bnUsdt[d.Symbol] {
				sp.BnUsdtSym = d.Symbol + "USDT"
				if sp.BnUsdcSym == "" {
					if pr, ok := bnPrecMap[sp.BnUsdtSym]; ok {
						sp.BnPriceDec, sp.BnQtyDec, sp.BnTick = pr.pDec, pr.qDec, pr.tick
					}
				}
			}
		}
		ltSpecs[d.Symbol] = sp
		ltIDToSym[d.MarketID] = d.Symbol
		ltRawBids[d.Symbol] = map[string]float64{}
		ltRawAsks[d.Symbol] = map[string]float64{}
	}
	mu.Unlock()

	// 3. Fetch AsterDEX Futures / RWA symbols & exact tick/step precision
	if aResp, aErr := httpCli.Get("https://fapi.asterdex.com/fapi/v1/exchangeInfo"); aErr == nil {
		aBody, _ := io.ReadAll(aResp.Body)
		_ = aResp.Body.Close()
		var aex struct {
			Symbols []struct {
				Symbol            string `json:"symbol"`
				BaseAsset         string `json:"baseAsset"`
				Status            string `json:"status"`
				PricePrecision    int    `json:"pricePrecision"`
				QuantityPrecision int    `json:"quantityPrecision"`
				Filters           []struct {
					FilterType string `json:"filterType"`
					TickSize   string `json:"tickSize"`
					StepSize   string `json:"stepSize"`
				} `json:"filters"`
			} `json:"symbols"`
		}
		if json.Unmarshal(aBody, &aex) == nil {
			mu.Lock()
			for _, s := range aex.Symbols {
				if s.Status != "TRADING" {
					continue
				}
				tk, stp := 0.0, 0.0
				pDec := s.PricePrecision
				for _, f := range s.Filters {
					if f.FilterType == "PRICE_FILTER" {
						tk, _ = strconv.ParseFloat(f.TickSize, 64)
						if tk > 0 {
							pDec = int(math.Round(-math.Log10(tk)))
							if pDec < 0 {
								pDec = 0
							}
						}
					} else if f.FilterType == "LOT_SIZE" {
						stp, _ = strconv.ParseFloat(f.StepSize, 64)
					}
				}
				asterSpecs[s.Symbol] = &BnUsdcSpec{
					BaseSym:  s.BaseAsset,
					UsdcSym:  s.Symbol,
					PriceDec: pDec,
					QtyDec:   s.QuantityPrecision,
					TickSize: tk,
					StepSize: stp,
				}
			}
			mu.Unlock()
		}
	}

	fmt.Printf("[%s] Loaded %d Lighter + %d AsterDEX markets into Tokyo Forward Arena\n", ArenaVer, len(ltSpecs), len(asterSpecs))
	return nil
}

func runFundingPoller() {
	for {
		if resp, err := httpCli.Get("https://mainnet.zklighter.elliot.ai/api/v1/funding-rates"); err == nil {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var fr struct {
				Rates []struct {
					Exchange string  `json:"exchange"`
					Symbol   string  `json:"symbol"`
					Rate     float64 `json:"rate"` // 8h decimal rate (e.g. 0.0018 = 18bp/8h = 2.25bp/1h)
				} `json:"funding_rates"`
			}
			if json.Unmarshal(body, &fr) == nil {
				mu.Lock()
				for _, r := range fr.Rates {
					if r.Exchange == "lighter" {
						ltFund1hBp[r.Symbol] = (r.Rate / 8.0) * 10000.0
					} else if r.Exchange == "binance" {
						bnFund1hBp[r.Symbol+"USDT"] = (r.Rate / 8.0) * 10000.0
						bnFund1hBp[r.Symbol+"USDC"] = (r.Rate / 8.0) * 10000.0
					}
				}
				mu.Unlock()
			}
		}
		time.Sleep(60 * time.Second)
	}
}

func recomputeLtBookLocked(sym string, nowMs int64) {
	rb := ltRawBids[sym]
	ra := ltRawAsks[sym]
	bids := make([]BookLevel, 0, len(rb))
	for ps, sz := range rb {
		if sz <= 0 {
			continue
		}
		p, err := strconv.ParseFloat(ps, 64)
		if err == nil && p > 0 {
			bids = append(bids, BookLevel{Price: p, Size: sz})
		}
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	if len(bids) > 15 {
		bids = bids[:15]
	}

	asks := make([]BookLevel, 0, len(ra))
	for ps, sz := range ra {
		if sz <= 0 {
			continue
		}
		p, err := strconv.ParseFloat(ps, 64)
		if err == nil && p > 0 {
			asks = append(asks, BookLevel{Price: p, Size: sz})
		}
	}
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	if len(asks) > 15 {
		asks = asks[:15]
	}

	lb := getBookLocked("LT", sym)
	lb.Bids = bids
	lb.Asks = asks
	if len(bids) > 0 && len(asks) > 0 && asks[0].Price > bids[0].Price {
		lb.Bid = bids[0].Price
		lb.BidSz = bids[0].Size
		lb.Ask = asks[0].Price
		lb.AskSz = asks[0].Size
		lb.UpdatedMs = nowMs
		recordMidLocked("LT:"+sym, 0.5*(lb.Bid+lb.Ask), nowMs)
	}
}

func runLighterWS() {
	type obLevel struct {
		Price string `json:"price"`
		Size  string `json:"size"`
	}
	type trItem struct {
		MarketID   int64  `json:"market_id"`
		Price      string `json:"price"`
		Size       string `json:"size"`
		IsMakerAsk bool   `json:"is_maker_ask"`
		Timestamp  int64  `json:"timestamp"`
	}
	type wsMsg struct {
		Type      string `json:"type"`
		Channel   string `json:"channel"`
		OrderBook struct {
			Bids []obLevel `json:"bids"`
			Asks []obLevel `json:"asks"`
		} `json:"order_book"`
		Trades []trItem `json:"trades"`
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial("wss://mainnet.zklighter.elliot.ai/stream", nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		mu.Lock()
		ids := make([]int64, 0, len(ltIDToSym))
		for id := range ltIDToSym {
			ids = append(ids, id)
		}
		mu.Unlock()

		for _, id := range ids {
			_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", id)})
			_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("trade/%d", id)})
			time.Sleep(12 * time.Millisecond)
		}

		for {
			_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var m wsMsg
			if json.Unmarshal(raw, &m) != nil {
				continue
			}
			if m.Type == "ping" {
				_ = conn.WriteJSON(map[string]string{"type": "pong"})
				continue
			}

			nowMs := time.Now().UnixMilli()
			if strings.HasPrefix(m.Channel, "order_book:") {
				incFeed("ltBook")
				id, _ := strconv.ParseInt(strings.TrimPrefix(m.Channel, "order_book:"), 10, 64)
				mu.Lock()
				sym := ltIDToSym[id]
				if sym != "" {
					if strings.Contains(m.Type, "subscribed") {
						ltRawBids[sym] = map[string]float64{}
						ltRawAsks[sym] = map[string]float64{}
					}
					rb := ltRawBids[sym]
					ra := ltRawAsks[sym]
					for _, b := range m.OrderBook.Bids {
						sz, _ := strconv.ParseFloat(b.Size, 64)
						if sz <= 0 {
							delete(rb, b.Price)
						} else {
							rb[b.Price] = sz
							if bp, e := strconv.ParseFloat(b.Price, 64); e == nil {
								for apStr := range ra {
									if ap, e2 := strconv.ParseFloat(apStr, 64); e2 == nil && ap <= bp {
										delete(ra, apStr)
									}
								}
							}
						}
					}
					for _, a := range m.OrderBook.Asks {
						sz, _ := strconv.ParseFloat(a.Size, 64)
						if sz <= 0 {
							delete(ra, a.Price)
						} else {
							ra[a.Price] = sz
							if ap, e := strconv.ParseFloat(a.Price, 64); e == nil {
								for bpStr := range rb {
									if bp, e2 := strconv.ParseFloat(bpStr, 64); e2 == nil && bp >= ap {
										delete(rb, bpStr)
									}
								}
							}
						}
					}
					recomputeLtBookLocked(sym, nowMs)
				}
				mu.Unlock()
			} else if strings.HasPrefix(m.Channel, "trade:") && m.Type == "update/trade" {
				// CRITICAL: ONLY process LIVE "update/trade" prints, NEVER historical "subscribed/trade"!
				incFeed("ltTrade")
				mu.Lock()
				for _, tr := range m.Trades {
					sym := ltIDToSym[tr.MarketID]
					if sym == "" {
						continue
					}
					trTs := tr.Timestamp
					if trTs > 0 && trTs < 100000000000 {
						trTs *= 1000
					}
					if trTs > 0 && nowMs >= trTs && nowMs-trTs < 5000 {
						prev := liveLtPubLagMs.Load()
						lag := nowMs - trTs
						if prev <= 0 {
							liveLtPubLagMs.Store(lag)
						} else {
							liveLtPubLagMs.Store((prev*7 + lag*3) / 10)
						}
					}
					px, _ := strconv.ParseFloat(tr.Price, 64)
					sz, _ := strconv.ParseFloat(tr.Size, 64)
					// is_maker_ask == false => resting Maker was a BID => incoming Taker was a SELLER!
					sellerAgg := !tr.IsMakerAsk
					processLiveTradeLocked("LT", sym, px, sz, sellerAgg, nowMs)
				}
				mu.Unlock()
			}
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runBinanceBookAndLiqWS() {
	go func() {
		liURL := "wss://fstream.binance.com/market/stream?streams=!forceOrder@arr"
		for {
			conn, _, err := websocket.DefaultDialer.Dial(liURL, nil)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			for {
				_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
				_, raw, err := conn.ReadMessage()
				if err != nil {
					break
				}
				var env struct {
					Data struct {
						Order struct {
							Sym  string `json:"s"`
							Side string `json:"S"`
							Px   string `json:"ap"`
							Qty  string `json:"q"`
						} `json:"o"`
					} `json:"data"`
				}
				if json.Unmarshal(raw, &env) == nil && env.Data.Order.Sym != "" {
					px, _ := strconv.ParseFloat(env.Data.Order.Px, 64)
					qty, _ := strconv.ParseFloat(env.Data.Order.Qty, 64)
					if px > 0 && qty > 0 {
						incFeed("bnLiq")
						nowMs := time.Now().UnixMilli()
						mu.Lock()
						liqRecent[env.Data.Order.Sym] = &ForceLiqEvent{
							Symbol: env.Data.Order.Sym,
							Side:   env.Data.Order.Side,
							Price:  px,
							Qty:    qty,
							Usd:    px * qty,
							TsMs:   nowMs,
						}
						mu.Unlock()
					}
				}
			}
			_ = conn.Close()
			time.Sleep(time.Second)
		}
	}()

	url := "wss://fstream.binance.com/public/stream?streams=!bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		for {
			_ = conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var env struct {
				Stream string          `json:"stream"`
				Data   json.RawMessage `json:"data"`
			}
			if json.Unmarshal(raw, &env) != nil {
				continue
			}
			nowMs := time.Now().UnixMilli()
			if env.Stream == "!bookTicker" {
				var bt struct {
					Sym   string `json:"s"`
					Bid   string `json:"b"`
					BidSz string `json:"B"`
					Ask   string `json:"a"`
					AskSz string `json:"A"`
				}
				if json.Unmarshal(env.Data, &bt) == nil {
					venue := ""
					if strings.HasSuffix(bt.Sym, "USDC") {
						venue = VenueBnUsdc
					} else if strings.HasSuffix(bt.Sym, "USDT") {
						venue = VenueBnUsdt
					} else {
						continue
					}
					bid, _ := strconv.ParseFloat(bt.Bid, 64)
					bsz, _ := strconv.ParseFloat(bt.BidSz, 64)
					ask, _ := strconv.ParseFloat(bt.Ask, 64)
					asz, _ := strconv.ParseFloat(bt.AskSz, 64)
					if bid > 0 && ask > bid {
						incFeed("bnBook")
						mu.Lock()
						b := getBookLocked(venue, bt.Sym)
						b.Bid, b.BidSz, b.Ask, b.AskSz, b.UpdatedMs = bid, bsz, ask, asz, nowMs
						recordMidLocked(bookKey(venue, bt.Sym), 0.5*(bid+ask), nowMs)
						mu.Unlock()
					}
				}
			}
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runBinanceAggTradeShard(streams []string) {
	url := "wss://fstream.binance.com/market/stream?streams=" + strings.Join(streams, "/")
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		conn.SetPingHandler(func(appData string) error {
			_ = conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
			return nil
		})
		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var env struct {
				Data struct {
					Sym        string `json:"s"`
					Px         string `json:"p"`
					Qty        string `json:"q"`
					BuyerMaker bool   `json:"m"` // true => resting BUY was maker => seller aggressor!
				} `json:"data"`
			}
			if json.Unmarshal(raw, &env) != nil || env.Data.Sym == "" {
				continue
			}
			px, _ := strconv.ParseFloat(env.Data.Px, 64)
			qty, _ := strconv.ParseFloat(env.Data.Qty, 64)
			venue := VenueBnUsdt
			if strings.HasSuffix(env.Data.Sym, "USDC") {
				venue = VenueBnUsdc
			}
			incFeed("bnTrade")
			nowMs := time.Now().UnixMilli()
			mu.Lock()
			processLiveTradeLocked(venue, env.Data.Sym, px, qty, env.Data.BuyerMaker, nowMs)
			mu.Unlock()
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runBinanceAggTradeWS() {
	mu.Lock()
	streams := []string{}
	seen := map[string]bool{}
	for _, sp := range ltSpecs {
		for _, bs := range []string{sp.BnUsdcSym, sp.BnUsdtSym} {
			if bs != "" && !seen[bs] {
				seen[bs] = true
				streams = append(streams, strings.ToLower(bs)+"@aggTrade")
			}
		}
	}
	for _, bs := range bnUsdcSpecs {
		for _, s := range []string{bs.UsdcSym, bs.UsdtSym} {
			if s != "" && !seen[s] {
				seen[s] = true
				streams = append(streams, strings.ToLower(s)+"@aggTrade")
			}
		}
	}
	mu.Unlock()
	if len(streams) == 0 {
		return
	}
	for i := 0; i < len(streams); i += 18 {
		end := i + 18
		if end > len(streams) {
			end = len(streams)
		}
		chunk := append([]string(nil), streams[i:end]...)
		go runBinanceAggTradeShard(chunk)
		time.Sleep(150 * time.Millisecond)
	}
}

func applyDepthLevelsLocked(venueKey, sym string, rawBids, rawAsks [][]string, nowMs int64) {
	bids := make([]BookLevel, 0, len(rawBids))
	for _, row := range rawBids {
		if len(row) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(row[0], 64)
		q, _ := strconv.ParseFloat(row[1], 64)
		if p > 0 && q > 0 {
			bids = append(bids, BookLevel{Price: p, Size: q})
		}
	}
	asks := make([]BookLevel, 0, len(rawAsks))
	for _, row := range rawAsks {
		if len(row) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(row[0], 64)
		q, _ := strconv.ParseFloat(row[1], 64)
		if p > 0 && q > 0 {
			asks = append(asks, BookLevel{Price: p, Size: q})
		}
	}
	if len(bids) == 0 || len(asks) == 0 || asks[0].Price <= bids[0].Price {
		return
	}
	b := getBookLocked(venueKey, sym)
	b.Bids = bids
	b.Asks = asks
	b.Bid = bids[0].Price
	b.BidSz = bids[0].Size
	b.Ask = asks[0].Price
	b.AskSz = asks[0].Size
	b.UpdatedMs = nowMs
	recordMidLocked(bookKey(venueKey, sym), 0.5*(b.Bid+b.Ask), nowMs)
}

func runDepth20Shard(baseWs string, isAster bool, streams []string) {
	url := baseWs + strings.Join(streams, "/")
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		conn.SetPingHandler(func(appData string) error {
			_ = conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
			return nil
		})
		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var env struct {
				Stream string `json:"stream"`
				Data   struct {
					Sym  string     `json:"s"`
					Bids [][]string `json:"b"`
					Asks [][]string `json:"a"`
					BidsAlt [][]string `json:"bids"`
					AsksAlt [][]string `json:"asks"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &env) != nil {
				continue
			}
			sym := env.Data.Sym
			if sym == "" && strings.Contains(env.Stream, "@") {
				sym = strings.ToUpper(strings.Split(env.Stream, "@")[0])
			}
			if sym == "" {
				continue
			}
			bRows := env.Data.Bids
			if len(bRows) == 0 {
				bRows = env.Data.BidsAlt
			}
			aRows := env.Data.Asks
			if len(aRows) == 0 {
				aRows = env.Data.AsksAlt
			}
			if len(bRows) == 0 || len(aRows) == 0 {
				continue
			}
			nowMs := time.Now().UnixMilli()
			if isAster {
				incFeed("asterL2")
				mu.Lock()
				applyDepthLevelsLocked("ASTER", sym, bRows, aRows, nowMs)
				mu.Unlock()
			} else {
				venueKey := VenueBnUsdt
				if strings.HasSuffix(sym, "USDC") {
					venueKey = VenueBnUsdc
				}
				incFeed("bnL2")
				mu.Lock()
				applyDepthLevelsLocked(venueKey, sym, bRows, aRows, nowMs)
				mu.Unlock()
			}
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runBinanceDepth20WS() {
	mu.Lock()
	streams := []string{}
	seen := map[string]bool{}
	for _, sp := range ltSpecs {
		for _, bs := range []string{sp.BnUsdcSym, sp.BnUsdtSym} {
			if bs != "" && !seen[bs] {
				seen[bs] = true
				streams = append(streams, strings.ToLower(bs)+"@depth20@100ms")
			}
		}
	}
	for _, bs := range bnUsdcSpecs {
		for _, s := range []string{bs.UsdcSym, bs.UsdtSym} {
			if s != "" && !seen[s] {
				seen[s] = true
				streams = append(streams, strings.ToLower(s)+"@depth20@100ms")
			}
		}
	}
	mu.Unlock()
	for i := 0; i < len(streams); i += 18 {
		end := i + 18
		if end > len(streams) {
			end = len(streams)
		}
		go runDepth20Shard("wss://fstream.binance.com/stream?streams=", false, append([]string(nil), streams[i:end]...))
		time.Sleep(150 * time.Millisecond)
	}
}

func runLiveVenueRttProber() {
	measure := func(url string, dst *atomic.Int64) {
		t0 := time.Now()
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := httpCli.Do(req)
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 200 {
			rtt := time.Since(t0).Milliseconds()
			if rtt > 0 && rtt < 2500 {
				prev := dst.Load()
				if prev <= 0 {
					dst.Store(rtt)
				} else {
					dst.Store((prev*7 + rtt*3) / 10)
				}
			}
		}
	}
	// Warm TCP+TLS pools first so we measure true warm keep-alive order RTT (not cold handshake!)
	measure("https://fapi.binance.com/fapi/v1/ping", &liveBnRttMs)
	measure("https://fapi.asterdex.com/fapi/v3/ping", &liveAsterRttMs)
	measure("https://mainnet.zklighter.elliot.ai/", &liveLtRttMs)
	for {
		measure("https://fapi.binance.com/fapi/v1/ping", &liveBnRttMs)
		measure("https://fapi.asterdex.com/fapi/v3/ping", &liveAsterRttMs)
		measure("https://mainnet.zklighter.elliot.ai/", &liveLtRttMs)
		mu.Lock()
		if feedCounters["bnRttMs"] != nil {
			feedCounters["bnRttMs"].Store(liveBnRttMs.Load())
			feedCounters["asterRttMs"].Store(liveAsterRttMs.Load())
			feedCounters["ltRttMs"].Store(liveLtRttMs.Load())
			feedCounters["ltPubLagMs"].Store(liveLtPubLagMs.Load())
		}
		mu.Unlock()
		time.Sleep(4 * time.Second)
	}
}

func main() {
	debug.SetMemoryLimit(240 << 20) // Hard 240 MiB ceiling so Tokyo EC2 never OOM-kills arena_go
	debug.SetGCPercent(40)

	for _, k := range []string{"ltBook", "ltTrade", "bnBook", "bnL2", "bnTrade", "bnLiq", "hlMids", "asterBook", "asterL2", "asterTrade", "bnRttMs", "asterRttMs", "ltRttMs", "ltPubLagMs"} {
		feedCounters[k] = &atomic.Int64{}
	}
	initStrategies()
	loadCheckpoint()
	for attempt := 0; attempt < 10; attempt++ {
		if err := loadMarketSpecs(); err == nil && len(ltSpecs) > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		lastStateSaveMs = 0
		updateMarkToMarketAndWriteTelemetry()
		os.Exit(0)
	}()

	go runLiveVenueRttProber()
	go runFundingPoller()
	go runLighterWS()
	go runBinanceBookAndLiqWS()
	go runBinanceDepth20WS()
	go runBinanceAggTradeWS()

	fmt.Printf("[%s] Live realistic-fill simulation loop started (100%% read-only public feeds, $%.2f virtual budget/strat)\n", ArenaVer, StartBudgetUSD)

	evalTicker := time.NewTicker(50 * time.Millisecond)
	telTicker := time.NewTicker(2 * time.Second)
	lastFundHour := time.Now().UTC().Unix() / 3600

	for {
		select {
		case <-evalTicker.C:
			nowMs := time.Now().UnixMilli()
			curHr := (nowMs / 1000) / 3600
			mu.Lock()
			processDueTakersLocked(nowMs)
			if nowMs-bootMs > 4000 {
				evalStrategiesLocked(nowMs)
			}
			if curHr > lastFundHour {
				lastFundHour = curHr
				settleHourlyFundingLocked(nowMs)
			}
			mu.Unlock()
		case <-telTicker.C:
			updateMarkToMarketAndWriteTelemetry()
		}
	}
}
