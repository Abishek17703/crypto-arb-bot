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
	StartBudgetUSD = 489.12 // Exact real capital ($91.69 Lighter + $397.43 Binance)
	TargetHourly   = 5.00
	ArenaOutPath   = "/tmp/forward_arena.json"
	ArenaStatePath = "/opt/crypto-arb-bot/forward_arena_state.json"
	ArenaVer       = "ARENA-v4.2-ROLLING-4H"

	VenueBnUsdc   = "BN_USDC"
	VenueBnUsdt   = "BN_USDT"
	VenueLtStd    = "LT_STD"
	VenueLtPrem   = "LT_PREM"
	VenueAsterC   = "ASTER_C"   // AsterDEX Crypto: 0% Maker, 4.0bp Taker, 6ms/10ms/8ms Tokyo
	VenueAsterRWA = "ASTER_RWA" // AsterDEX RWA Equities/Commodities: 0% Maker, 1.25bp Taker!
	VenuePoly     = "POLY"
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
}

type BnUsdcSpec struct {
	BaseSym  string
	UsdcSym  string
	UsdtSym  string
	PriceDec int
	QtyDec   int
	TickSize float64
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
	TsMs   int64   `json:"ts"`
	Equity float64 `json:"eq"`
	Cash   float64 `json:"cash,omitempty"`
}

type StrategyAccount struct {
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Desc            string               `json:"desc"`
	VenueDesc       string               `json:"venueDesc"`
	MaxLevCap       float64              `json:"maxLevCap"`
	StartCash       float64              `json:"startCash"`
	Cash            float64              `json:"cash"` // StartCash + RealizedPnL - FeesPaid + FundingUsd
	RealizedGross   float64              `json:"realizedGross"`
	FeesPaid        float64              `json:"feesPaid"`
	FundingUsd      float64              `json:"fundingUsd"`
	UnrealizedPnL   float64              `json:"unrealizedPnl"`
	Equity          float64              `json:"equity"`
	PeakEquity      float64              `json:"peakEquity"`
	MaxDrawdownUsd  float64              `json:"maxDrawdownUsd"`
	HourlyRunRate   float64              `json:"hourlyRunRate"`
	RealizedHourly  float64              `json:"realizedHourly"`
	Rolling4hHourly float64              `json:"rolling4hHourly"`
	MakerPlaced     int64                `json:"makerPlaced"`
	MakerCancelled  int64                `json:"makerCancelled"`
	MakerFills      int64                `json:"makerFills"`
	PickoffFills    int64                `json:"pickoffFills"`
	TakerFills      int64                `json:"takerFills"`
	RoundTrips      int64                `json:"roundTrips"`
	WinTrips        int64                `json:"winTrips"`
	GrossVolumeUsd  float64              `json:"grossVolumeUsd"`
	TxTimestamps    []int64              `json:"-"`
	Positions       map[string]*Position `json:"-"`
	PosSnap         []Position           `json:"positions"`
	ActiveMakers    int                  `json:"activeMakers"`
	EquityCurve     []EquityPoint        `json:"equityCurve"`
	StatusNote      string               `json:"statusNote"`
}

type PolyMarket struct {
	Slug        string
	Question    string
	YesTokenID  string
	NoTokenID   string
	YesBid      float64
	YesBidSz    float64
	YesAsk      float64
	YesAskSz    float64
	NoBid       float64
	NoBidSz     float64
	NoAsk       float64
	NoAskSz     float64
	LastTradeMs int64
	UpdatedMs   int64
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
	hlMids        = map[string]float64{}
	hlUpdatedMs   int64
	ltFund1hBp    = map[string]float64{} // hourly funding rate in bps (+ means longs pay shorts)
	bnFund1hBp    = map[string]float64{}
	liqRecent     = map[string]*ForceLiqEvent{}
	polyMarkets   = map[string]*PolyMarket{}
	makerOrders   = map[int64]*SimMakerOrder{}
	takerOrders   = map[int64]*SimTakerOrder{}
	strats        = map[string]*StrategyAccount{}
	stratOrder    = []string{}
	recentFills   = []FillAudit{}
	feedCounters  = map[string]*atomic.Int64{}
	twinRatioEMA  = map[string]float64{}
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

func initStrategies() {
	defs := []*StrategyAccount{
		{
			ID:        "S1_LT_PREM_BN_DN_CLEAN",
			Name:      "Clean LT Prem Maker + 0% BN USDC Maker Exit",
			Desc:      "Bug-fixed Reversed DN: 0.30bp LT Prem Maker -> 4ms BN USDC-M hedge; exits via 0%-fee BN USDC-M Maker -> LT Prem close with strict in-flight taker tracking (zero duplicate hedges!).",
			VenueDesc: "LT_PREM M + BN_USDC 0% M Exit",
			MaxLevCap: 7.5,
		},
		{
			ID:        "S2_LT_STD_ZERO_FEE_DN",
			Name:      "0%-Fee Lighter Std (#770015) + 0% BN USDC Arb",
			Desc:      "Exact #770015 Standard tier (0% LT M/T, 305ms speed-bump) + 0%-Maker BN USDC-M exit (= ONLY 4.00bp total 4-leg fee!). Strict in-flight guard fixes v3 duplicate-hedge leak.",
			VenueDesc: "LT_STD 0% Fee + BN_USDC 0% Exit",
			MaxLevCap: 7.5,
		},
		{
			ID:        "S3_LT_ASTER_CRYPTO_ARB",
			Name:      "Lighter (0%) <-> AsterDEX (0% Maker) Altcoin Arb",
			Desc:      "Arbs wide alts (LIT, VVV, XMR, HYPE, ENA, NEAR, UNI, AAVE, AVAX, SUI, ARB, WLD) between 0% Lighter Std & Tokyo-hosted AsterDEX (0% Maker / 4bp Taker = only 4.0bp 4-leg fee!).",
			VenueDesc: "LT_STD 0% + ASTER_C 0% Maker",
			MaxLevCap: 6.5,
		},
		{
			ID:        "S4_ASTER_LT_BN_RWA_15X",
			Name:      "20x Tokenized Equity Arb (Aster 1.25bp RWA <-> LT)",
			Desc:      "Trades AAPL, NVDA, TSLA, QQQ, SPY, AMZN, META, COIN, MSTR, GOOGL, AMD, INTC, XAU between Lighter Prem (0.30bp M) & Aster RWA (0% M / 1.25bp T = ONLY 3.55bp 4-leg fee!) at 12x hedged.",
			VenueDesc: "ASTER_RWA 1.25bp + LT_PREM (12x)",
			MaxLevCap: 14.0,
		},
		{
			ID:        "S5_LT_STD_MARGIN_STRICT",
			Name:      "Margin-Strict Real-$489 Portfolio (LT Std + BN + Aster)",
			Desc:      "Exact real-account deployable blueprint: combines top-2 winners (S2+S3) capped strictly at <=3.6x leverage ($1,750 max/side) matching our exact #770015 0%-fee account limits.",
			VenueDesc: "LT_STD 0% + BN_USDC / ASTER (<=3.6x)",
			MaxLevCap: 4.5,
		},
		{
			ID:        "S6_FUNDING_CARRY_SCALP",
			Name:      "Positive-Carry Basis Scalp (VVV 207% APR, XMR, CRCL)",
			Desc:      "ONLY enters the positive hourly funding leg (Short LT / Long Hedge on VVV +2.4bp/h, XMR +1.1bp/h, MSTR +1.5bp/h, CRCL +1.3bp/h, COIN +0.9bp/h) — collects carry while waiting for convergence.",
			VenueDesc: "Positive-Carry DN + 8bp Target",
			MaxLevCap: 6.5,
		},
		{
			ID:        "S7_HIGH_CONV_LT_STD_DN",
			Name:      "High-Conviction >=7bp Lighter Std <-> BN USDC Arb",
			Desc:      "Same proven 4.0bp total-fee S2 architecture, filtered strictly to >=7.0bp dislocations with larger $780 clips for higher win rate & lower drawdown.",
			VenueDesc: "LT_STD 0% + BN_USDC (>=7bp)",
			MaxLevCap: 7.5,
		},
		{
			ID:        "S8_BN_USDC_PAIR_STATARB",
			Name:      "0%-Maker Binance USDC-M Cointegrated Pair Stat-Arb",
			Desc:      "Fixed exit-order non-churn + -40bp divergence stop! Beta-neutral Z-score mean-reversion on BTC/ETH, SOL/AVAX, UNI/AAVE, NEAR/SUI, ARB/WLD, ENA/HYPE at 10x.",
			VenueDesc: "BN_USDC 0% Pairs (-40bp SL)",
			MaxLevCap: 10.0,
		},
	}

	for _, s := range defs {
		s.StartCash = StartBudgetUSD
		s.Cash = StartBudgetUSD
		s.Equity = StartBudgetUSD
		s.PeakEquity = StartBudgetUSD
		s.Positions = map[string]*Position{}
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
	vp := venuePhys[venue]
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
		ReadyMs:           nowMs + vp.PlaceLatMs,
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
	vp := venuePhys[o.Venue]
	o.CancelReqMs = nowMs
	o.CancelEffMs = nowMs + vp.CancelLatMs
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
	vp := venuePhys[venue]
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
		ExecAtMs:   nowMs + vp.TakerLatMs,
		MaxSlipBp:  maxSlipBp,
		ReduceOnly: reduceOnly,
	}
	return id
}

func applyFillLocked(stratID, venue, sym, side, execType string, fillPx, sigPx, qty, feeBp, qInit float64, latMs int64, reduceOnly bool, tag string, nowMs int64) {
	s := strats[stratID]
	if s == nil || qty <= 1e-9 || fillPx <= 0 {
		return
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
	closedTrip := false
	if p == nil {
		p = &Position{
			Venue:     venue,
			Symbol:    sym,
			Qty:       signedQty,
			AvgPx:     fillPx,
			OpenedMs:  nowMs,
			UpdatedMs: nowMs,
			Tag:       tag,
		}
		s.Positions[pk] = p
	} else if (p.Qty > 0 && signedQty > 0) || (p.Qty < 0 && signedQty < 0) {
		newAbs := math.Abs(p.Qty) + qty
		p.AvgPx = (math.Abs(p.Qty)*p.AvgPx + qty*fillPx) / newAbs
		p.Qty += signedQty
		p.UpdatedMs = nowMs
	} else {
		closeQty := math.Min(math.Abs(p.Qty), qty)
		if p.Qty > 0 {
			realizedDelta = closeQty * (fillPx - p.AvgPx)
		} else {
			realizedDelta = closeQty * (p.AvgPx - fillPx)
		}
		remSigned := p.Qty + signedQty
		if math.Abs(remSigned) <= 1e-8 {
			delete(s.Positions, pk)
			closedTrip = true
		} else if (p.Qty > 0 && remSigned > 0) || (p.Qty < 0 && remSigned < 0) {
			p.Qty = remSigned
			p.UpdatedMs = nowMs
		} else {
			p.Qty = remSigned
			p.AvgPx = fillPx
			p.OpenedMs = nowMs
			p.UpdatedMs = nowMs
			p.Tag = tag
			closedTrip = true
		}
	}

	s.RealizedGross += realizedDelta
	s.FeesPaid += feeUsd
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
	if closedTrip {
		s.RoundTrips++
		if realizedDelta-feeUsd > 0 {
			s.WinTrips++
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
				take := math.Min(rem, math.Max(b.AskSz, to.Qty))
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
				take := math.Min(rem, math.Max(b.BidSz, to.Qty))
				cost += take * b.Bid
				filled += take
			}
		}

		if filled > 1e-9 {
			vwap := cost / filled
			applyFillLocked(to.StratID, to.Venue, to.Symbol, to.Side, "TAKER_BOOKWALK", vwap, to.SignalPx, filled, vp.TakerFeeBp, 0, nowMs-to.PlacedMs, to.ReduceOnly, to.Tag, nowMs)
		}
	}

	// Expire completed cancels
	for id, mo := range makerOrders {
		if mo.CancelReqMs > 0 && nowMs >= mo.CancelEffMs {
			delete(makerOrders, id)
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
	hTag := stratID[:2] + "_HEDGE"
	if isExit {
		hTag = stratID[:2] + "_CLOSE"
	}
	placeTakerLocked(stratID, peerVenue, peerSym, hedgeSide, hQty, sigPx, 25.0, isExit, hTag, nowMs)
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
	if venue != VenueBnUsdt {
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
	return 1.0015
}

type PairArbCfg struct {
	StratID       string
	MakerVenue    string // e.g. VenueLtPrem, VenueLtStd, VenueAsterRWA
	MakerSym      string
	HedgeVenue    string // e.g. VenueBnUsdc, VenueAsterC, VenueAsterRWA
	HedgeSym      string
	SlotUsd       float64
	EntryDislocBp float64 // dislocation vs EMA fair basis required to quote Maker entry
	NetTargetBp   float64 // required net profit above all 4-leg fees for Maker exit
	MaxHoldMs     int64
	CancelTolBp   float64
	MaxAgeMs      int64
	CarryBias     int // 0=both sides, -1=only SHORT MakerSym / LONG HedgeSym (positive carry!)
	FxAdjHedge    bool
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
	fx := 1.0
	if cfg.FxAdjHedge {
		fx = usdtFxLocked(cfg.HedgeVenue)
	}
	mMid := 0.5 * (bM.Bid + bM.Ask)
	hBidUsd := bH.Bid / fx
	hAskUsd := bH.Ask / fx
	hMidUsd := 0.5 * (hBidUsd + hAskUsd)

	emaKey := cfg.StratID + ":" + cfg.MakerSym + ":" + cfg.HedgeSym
	rawBasisBp := ((mMid - hMidUsd) / hMidUsd) * 10000.0
	if _, ok := dnBasisEMA[emaKey]; !ok {
		dnBasisEMA[emaKey] = rawBasisBp
	} else {
		dnBasisEMA[emaKey] = dnBasisEMA[emaKey]*0.995 + rawBasisBp*0.005
	}
	eqBasisBp := dnBasisEMA[emaKey]

	posM := acct.Positions[posKey(cfg.MakerVenue, cfg.MakerSym)]
	posH := acct.Positions[posKey(cfg.HedgeVenue, cfg.HedgeSym)]

	// STRICT IN-FLIGHT GUARD: never double-hedge or declare orphan while a Taker leg is executing!
	pendingM := hasPendingTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym)
	pendingH := hasPendingTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym)

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
			if math.Abs(posM.Qty)*mMid < 8.0 && nowMs-posM.OpenedMs > 900 {
				// Auto-sweep tiny rounding dust (<$8) cleanly!
				cSide := "SELL"
				px := bM.Bid
				if posM.Qty < 0 {
					cSide = "BUY"
					px = bM.Ask
				}
				placeTakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, cSide, math.Abs(posM.Qty), px, 25.0, true, cfg.StratID[:2]+"_DUST_CLOSE", nowMs)
				return
			}
			if nowMs-posM.OpenedMs > 900 {
				hSide := "BUY"
				px := bH.Ask
				if posM.Qty > 0 {
					hSide = "SELL"
					px = bH.Bid
				}
				hQty := roundTo(math.Abs(posM.Qty), commonQtyDec)
				if hQty > 0 {
					placeTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, hSide, hQty, px, 25.0, false, cfg.StratID[:2]+"_HEDGE", nowMs)
				}
			}
			return
		}

		// Manage hedged pair exit via Maker on the 0%-fee HedgeVenue -> Taker close on MakerVenue!
		qty := math.Abs(posH.Qty)
		rtFeeBp := venuePhys[cfg.MakerVenue].MakerFeeBp + venuePhys[cfg.HedgeVenue].TakerFeeBp +
			venuePhys[cfg.HedgeVenue].MakerFeeBp + venuePhys[cfg.MakerVenue].TakerFeeBp

		exitTag := "X|" + cfg.MakerVenue + "|" + cfg.MakerSym + "|EX"
		if posH.Qty > 0 {
			// Long Hedge, Short Maker -> rest SELL Maker on HedgeVenue at bH.Ask, close BUY on MakerVenue!
			exitGrossBp := ((bH.Ask - posH.AvgPx) / posH.AvgPx * 10000.0) + ((posM.AvgPx - bM.Ask) / posM.AvgPx * 10000.0)
			if exitGrossBp >= rtFeeBp+cfg.NetTargetBp || (cfg.MaxHoldMs > 0 && nowMs-posM.OpenedMs > cfg.MaxHoldMs && exitGrossBp >= rtFeeBp+0.3) {
				cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, "SELL", bH.Ask, qty, 1.2, true, exitTag, 15000, nowMs)
			} else {
				cancelAllMakersForSymLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, nowMs)
			}
		} else {
			exitGrossBp := ((posH.AvgPx - bH.Bid) / posH.AvgPx * 10000.0) + ((bM.Bid - posM.AvgPx) / posM.AvgPx * 10000.0)
			if exitGrossBp >= rtFeeBp+cfg.NetTargetBp || (cfg.MaxHoldMs > 0 && nowMs-posM.OpenedMs > cfg.MaxHoldMs && exitGrossBp >= rtFeeBp+0.3) {
				cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, "BUY", bH.Bid, qty, 1.2, true, exitTag, 15000, nowMs)
			} else {
				cancelAllMakersForSymLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, nowMs)
			}
		}
		return
	}

	if posH != nil && math.Abs(posH.Qty) > 1e-8 {
		if !pendingM && !pendingH && nowMs-posH.OpenedMs > 1200 {
			side := "SELL"
			px := bH.Bid
			if posH.Qty < 0 {
				side = "BUY"
				px = bH.Ask
			}
			placeTakerLocked(cfg.StratID, cfg.HedgeVenue, cfg.HedgeSym, side, math.Abs(posH.Qty), px, 25.0, true, cfg.StratID[:2]+"_ORPHAN_CLOSE", nowMs)
		}
		return
	}

	hRet400, _ := midReturnBpLocked(bookKey(venueBookPrefix(cfg.HedgeVenue), cfg.HedgeSym), 400, nowMs)
	if math.Abs(hRet400) > 1.8 {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
		return
	}

	qty := roundTo(cfg.SlotUsd/mMid, commonQtyDec)
	if qty < minBase || qty*mMid < 20.0 {
		return
	}
	shortDislocBp := (((bM.Ask - hAskUsd) / hAskUsd) * 10000.0) - eqBasisBp
	longDislocBp := (((hBidUsd - bM.Bid) / hBidUsd) * 10000.0) + eqBasisBp
	entryTag := "E|" + cfg.HedgeVenue + "|" + cfg.HedgeSym + "|EN"

	if shortDislocBp >= cfg.EntryDislocBp && cfg.CarryBias <= 0 {
		cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "SELL", bM.Ask, qty, cfg.CancelTolBp, false, entryTag, cfg.MaxAgeMs, nowMs)
	} else if longDislocBp >= cfg.EntryDislocBp && cfg.CarryBias >= 0 {
		cancelStaleOrRefreshMakerLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, "BUY", bM.Bid, qty, cfg.CancelTolBp, false, entryTag, cfg.MaxAgeMs, nowMs)
	} else {
		cancelAllMakersForSymLocked(cfg.StratID, cfg.MakerVenue, cfg.MakerSym, nowMs)
	}
}

func evalStrategiesLocked(nowMs int64) {
	// ------------------------------------------------------------------------
	// S1: Clean LT Prem Maker + 0% BN USDC-M Maker Exit (Bug-Fixed v1/v3!)
	// ------------------------------------------------------------------------
	s1Syms := []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "PENGU", "TRUMP", "TIA", "WLFI", "KAITO", "CRV", "ETHFI", "LINK", "DOGE", "XRP"}
	for _, sym := range s1Syms {
		sp := ltSpecs[sym]
		if sp == nil || sp.BnUsdcSym == "" {
			continue
		}
		dec := sp.SizeDecimals
		if sp.BnQtyDec < dec {
			dec = sp.BnQtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S1_LT_PREM_BN_DN_CLEAN",
			MakerVenue:    VenueLtPrem,
			MakerSym:      sym,
			HedgeVenue:    VenueBnUsdc,
			HedgeSym:      sp.BnUsdcSym,
			SlotUsd:       650.0,
			EntryDislocBp: 6.2,
			NetTargetBp:   2.2,
			MaxHoldMs:     900000,
			CancelTolBp:   1.0,
			MaxAgeMs:      20000,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S2: 0%-Fee Lighter Standard (#770015) + 0%-Fee BN USDC-M Arb (4bp Total RT Fee!)
	// ------------------------------------------------------------------------
	for _, sym := range s1Syms {
		sp := ltSpecs[sym]
		if sp == nil || sp.BnUsdcSym == "" {
			continue
		}
		dec := sp.SizeDecimals
		if sp.BnQtyDec < dec {
			dec = sp.BnQtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S2_LT_STD_ZERO_FEE_DN",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    VenueBnUsdc,
			HedgeSym:      sp.BnUsdcSym,
			SlotUsd:       650.0,
			EntryDislocBp: 4.8,
			NetTargetBp:   2.2,
			MaxHoldMs:     900000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S3: Lighter (0% Std) <-> AsterDEX (0% Maker / 4bp Taker) Crypto Arb!
	// ------------------------------------------------------------------------
	s3Crypto := []string{"LIT", "VVV", "XMR", "HYPE", "ENA", "NEAR", "UNI", "AAVE", "AVAX", "SUI", "ARB", "WLD"}
	for _, sym := range s3Crypto {
		sp := ltSpecs[sym]
		as := asterSpecs[sym+"USDT"]
		if sp == nil || as == nil {
			continue
		}
		dec := sp.SizeDecimals
		if as.QtyDec < dec {
			dec = as.QtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S3_LT_ASTER_CRYPTO_ARB",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    VenueAsterC,
			HedgeSym:      sym + "USDT",
			SlotUsd:       580.0,
			EntryDislocBp: 5.5,
			NetTargetBp:   2.2,
			MaxHoldMs:     900000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
			FxAdjHedge:    true,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S4: 20x Tokenized Equity Arb (Aster 1.25bp RWA <-> Lighter Prem 0.30bp M)
	// Total 4-leg fee = 0.30 + 1.25 + 0.00 + 2.00 = ONLY 3.55bp at 12x hedged!
	// ------------------------------------------------------------------------
	s4Rwa := []string{"NVDA", "AAPL", "TSLA", "QQQ", "SPY", "AMZN", "META", "COIN", "MSTR", "GOOGL", "AMD", "INTC"}
	for _, sym := range s4Rwa {
		sp := ltSpecs[sym]
		as := asterSpecs[sym+"USDT"]
		if sp == nil || as == nil {
			continue
		}
		dec := sp.SizeDecimals
		if as.QtyDec < dec {
			dec = as.QtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S4_ASTER_LT_BN_RWA_15X",
			MakerVenue:    VenueLtPrem,
			MakerSym:      sym,
			HedgeVenue:    VenueAsterRWA,
			HedgeSym:      sym + "USDT",
			SlotUsd:       1100.0,
			EntryDislocBp: 5.0,
			NetTargetBp:   2.0,
			MaxHoldMs:     900000,
			CancelTolBp:   1.2,
			MaxAgeMs:      25000,
			FxAdjHedge:    true,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S5: Margin-Strict Real-$489.12 Portfolio (LT Std + BN_USDC / ASTER_C)
	// Hard-capped <= $1,750 per side (max 2-3 pairs) matching #770015 margins!
	// ------------------------------------------------------------------------
	for _, sym := range s1Syms {
		sp := ltSpecs[sym]
		if sp == nil || sp.BnUsdcSym == "" {
			continue
		}
		dec := sp.SizeDecimals
		if sp.BnQtyDec < dec {
			dec = sp.BnQtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S5_LT_STD_MARGIN_STRICT",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    VenueBnUsdc,
			HedgeSym:      sp.BnUsdcSym,
			SlotUsd:       550.0,
			EntryDislocBp: 5.5,
			NetTargetBp:   2.3,
			MaxHoldMs:     900000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
		}, dec, sp.MinBase, nowMs)
	}
	for _, sym := range []string{"VVV", "XMR", "LIT"} {
		sp := ltSpecs[sym]
		as := asterSpecs[sym+"USDT"]
		if sp == nil || as == nil {
			continue
		}
		dec := sp.SizeDecimals
		if as.QtyDec < dec {
			dec = as.QtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S5_LT_STD_MARGIN_STRICT",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    VenueAsterC,
			HedgeSym:      sym + "USDT",
			SlotUsd:       520.0,
			EntryDislocBp: 6.2,
			NetTargetBp:   2.4,
			MaxHoldMs:     900000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
			FxAdjHedge:    true,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S6: Positive-Carry Basis Scalp (ONLY enters positive hourly funding side!)
	// VVV (+2.36bp/h), MSTR (+1.51bp/h), CRCL (+1.27bp/h), XMR (+1.14bp/h), COIN (+0.86bp/h)
	// ------------------------------------------------------------------------
	s6Carry := []string{"VVV", "XMR", "MSTR", "CRCL", "COIN", "LIT"}
	for _, sym := range s6Carry {
		sp := ltSpecs[sym]
		if sp == nil {
			continue
		}
		hv := VenueAsterC
		hs := sym + "USDT"
		as := asterSpecs[hs]
		dec := sp.SizeDecimals
		if as == nil {
			continue
		}
		if sym == "MSTR" || sym == "COIN" || sym == "CRCL" {
			hv = VenueAsterRWA
		}
		if as.QtyDec < dec {
			dec = as.QtyDec
		}
		fundDiff := ltFund1hBp[sym] - bnFund1hBp[sym+"USDT"]
		if fundDiff < 0.35 {
			continue
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S6_FUNDING_CARRY_SCALP",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    hv,
			HedgeSym:      hs,
			SlotUsd:       700.0,
			EntryDislocBp: 4.0, // Positive carry means time is our friend — hold calmly!
			NetTargetBp:   2.5,
			MaxHoldMs:     3600000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
			CarryBias:     -1, // ONLY Short Lighter / Long Aster
			FxAdjHedge:    true,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S7: High-Conviction >=7.0bp Lighter Std <-> BN USDC-M Arb ($780 clips)
	// ------------------------------------------------------------------------
	for _, sym := range s1Syms {
		sp := ltSpecs[sym]
		if sp == nil || sp.BnUsdcSym == "" {
			continue
		}
		dec := sp.SizeDecimals
		if sp.BnQtyDec < dec {
			dec = sp.BnQtyDec
		}
		runCrossVenuePairArbLocked(PairArbCfg{
			StratID:       "S7_HIGH_CONV_LT_STD_DN",
			MakerVenue:    VenueLtStd,
			MakerSym:      sym,
			HedgeVenue:    VenueBnUsdc,
			HedgeSym:      sp.BnUsdcSym,
			SlotUsd:       780.0,
			EntryDislocBp: 7.0,
			NetTargetBp:   2.5,
			MaxHoldMs:     900000,
			CancelTolBp:   2.5,
			MaxAgeMs:      45000,
		}, dec, sp.MinBase, nowMs)
	}

	// ------------------------------------------------------------------------
	// S8: 0%-Maker Binance USDC-M Cointegrated Pair Stat-Arb (Fixed Exit + -40bp SL!)
	// ------------------------------------------------------------------------
	s8Pairs := [][2]string{
		{"BTCUSDC", "ETHUSDC"},
		{"SOLUSDC", "AVAXUSDC"},
		{"UNIUSDC", "AAVEUSDC"},
		{"NEARUSDC", "SUIUSDC"},
		{"ARBUSDC", "WLDUSDC"},
		{"ENAUSDC", "HYPEUSDC"},
	}
	s8Acct := strats["S8_BN_USDC_PAIR_STATARB"]
	for _, pr := range s8Pairs {
		sA, sB := pr[0], pr[1]
		bsA, bsB := bnUsdcSpecs[sA], bnUsdcSpecs[sB]
		bA, bB := books[bookKey(VenueBnUsdc, sA)], books[bookKey(VenueBnUsdc, sB)]
		if bsA == nil || bsB == nil || bA == nil || bB == nil || bA.Bid <= 0 || bB.Bid <= 0 {
			continue
		}
		if nowMs-bA.UpdatedMs > 2000 || nowMs-bB.UpdatedMs > 2000 {
			continue
		}
		midA := 0.5 * (bA.Bid + bA.Ask)
		midB := 0.5 * (bB.Bid + bB.Ask)
		ratio := midA / midB
		pk := sA + "/" + sB
		if twinRatioEMA[pk] <= 0 {
			twinRatioEMA[pk] = ratio
			continue
		}
		twinRatioEMA[pk] = twinRatioEMA[pk]*0.996 + ratio*0.004
		devBp := ((ratio - twinRatioEMA[pk]) / twinRatioEMA[pk]) * 10000.0

		posA := s8Acct.Positions[posKey(VenueBnUsdc, sA)]
		posB := s8Acct.Positions[posKey(VenueBnUsdc, sB)]
		pendA := hasPendingTakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA)
		pendB := hasPendingTakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sB)

		if posA != nil && math.Abs(posA.Qty) > 1e-8 {
			// Cancel ONLY entry orders on sA — NEVER cancel our X| exit Maker order!
			for _, mo := range makerOrders {
				if mo.StratID == "S8_BN_USDC_PAIR_STATARB" && mo.Venue == VenueBnUsdc && mo.Symbol == sA && strings.HasPrefix(mo.Tag, "E|") && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
			if pendA || pendB || posB == nil || math.Abs(posB.Qty) <= 1e-8 {
				continue
			}
			legUsd := math.Abs(posA.Qty) * posA.AvgPx
			pnlUsd := 0.0
			if posA.Qty > 0 {
				pnlUsd += math.Abs(posA.Qty) * (bA.Bid - posA.AvgPx)
			} else {
				pnlUsd += math.Abs(posA.Qty) * (posA.AvgPx - bA.Ask)
			}
			if posB.Qty > 0 {
				pnlUsd += math.Abs(posB.Qty) * (bB.Bid - posB.AvgPx)
			} else {
				pnlUsd += math.Abs(posB.Qty) * (posB.AvgPx - bB.Ask)
			}
			pnlBp := (pnlUsd / math.Max(legUsd, 1.0)) * 10000.0
			if pnlBp <= -40.0 {
				// Correlation-break emergency Taker flatten of both legs!
				cancelAllMakersForSymLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, nowMs)
				cSideA, pxA := "SELL", bA.Bid
				if posA.Qty < 0 {
					cSideA, pxA = "BUY", bA.Ask
				}
				cSideB, pxB := "SELL", bB.Bid
				if posB.Qty < 0 {
					cSideB, pxB = "BUY", bB.Ask
				}
				placeTakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, cSideA, math.Abs(posA.Qty), pxA, 20.0, true, "S8_SL_A", nowMs)
				placeTakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sB, cSideB, math.Abs(posB.Qty), pxB, 20.0, true, "S8_SL_B", nowMs)
				stratCoolMap["S8:"+pk] = nowMs + 60000
				continue
			}
			if pnlBp >= 10.5 || (nowMs-posA.OpenedMs > 900000 && pnlBp >= 4.5) {
				exitTag := "X|" + VenueBnUsdc + "|" + sB + "|DM"
				if posA.Qty > 0 {
					cancelStaleOrRefreshMakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, "SELL", bA.Ask, math.Abs(posA.Qty), 1.5, true, exitTag, 15000, nowMs)
				} else {
					cancelStaleOrRefreshMakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, "BUY", bA.Bid, math.Abs(posA.Qty), 1.5, true, exitTag, 15000, nowMs)
				}
			} else {
				cancelAllMakersForSymLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, nowMs)
			}
			continue
		}

		if posB != nil && math.Abs(posB.Qty) > 1e-8 && !pendA && !pendB && nowMs-posB.OpenedMs > 1200 {
			side := "SELL"
			px := bB.Bid
			if posB.Qty < 0 {
				side = "BUY"
				px = bB.Ask
			}
			placeTakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sB, side, math.Abs(posB.Qty), px, 20.0, true, "S8_ORPHAN_CLOSE", nowMs)
			continue
		}

		if nowMs < stratCoolMap["S8:"+pk] {
			continue
		}
		qtyA := roundTo(900.0/midA, bsA.QtyDec)
		if qtyA*midA < 25.0 {
			continue
		}
		entryTag := "E|" + VenueBnUsdc + "|" + sB + "|DM"
		if devBp >= 11.5 {
			cancelStaleOrRefreshMakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, "SELL", bA.Ask, qtyA, 1.5, false, entryTag, 20000, nowMs)
		} else if devBp <= -11.5 {
			cancelStaleOrRefreshMakerLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, "BUY", bA.Bid, qtyA, 1.5, false, entryTag, 20000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S8_BN_USDC_PAIR_STATARB", VenueBnUsdc, sA, nowMs)
		}
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
			} else if p.Venue == VenueAsterC || p.Venue == VenueAsterRWA {
				rBp := bnFund1hBp[p.Symbol]
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
		upnl := 0.0
		snaps := make([]Position, 0, len(s.Positions))
		for _, p := range s.Positions {
			if math.Abs(p.Qty) <= 1e-9 {
				continue
			}
			b := books[bookKey(venueBookPrefix(p.Venue), p.Symbol)]
			mark := p.AvgPx
			if b != nil && b.Bid > 0 && b.Ask > 0 {
				if p.Qty > 0 {
					mark = b.Bid // conservative liquidation mark!
				} else {
					mark = b.Ask
				}
			}
			if p.Qty > 0 {
				upnl += math.Abs(p.Qty) * (mark - p.AvgPx)
			} else {
				upnl += math.Abs(p.Qty) * (p.AvgPx - mark)
			}
			snaps = append(snaps, *p)
		}

		s.UnrealizedPnL = roundTo(upnl, 4)
		s.Cash = roundTo(s.StartCash+s.RealizedGross-s.FeesPaid+s.FundingUsd, 4)
		s.Equity = roundTo(s.Cash+upnl, 4)
		if s.Equity > s.PeakEquity {
			s.PeakEquity = s.Equity
		}
		dd := s.PeakEquity - s.Equity
		if dd > s.MaxDrawdownUsd {
			s.MaxDrawdownUsd = roundTo(dd, 4)
		}
		netGain := s.Equity - s.StartCash
		realGain := s.Cash - s.StartCash
		// Avoid wild extrapolation in first 3 minutes
		denomHr := math.Max(upHours, 0.05)
		s.HourlyRunRate = roundTo(netGain/denomHr, 3)
		s.RealizedHourly = roundTo(realGain/denomHr, 3)
		s.ActiveMakers = makerCntByStrat[id]
		s.PosSnap = snaps

		if len(s.EquityCurve) == 0 || nowMs-s.EquityCurve[len(s.EquityCurve)-1].TsMs >= 30000 {
			s.EquityCurve = append(s.EquityCurve, EquityPoint{TsMs: nowMs, Equity: s.Equity, Cash: s.Cash})
			if len(s.EquityCurve) > 540 { // 540 * 30s = 4.5 hours full rolling window!
				s.EquityCurve = append([]EquityPoint(nil), s.EquityCurve[len(s.EquityCurve)-540:]...)
			}
		}

		// Compute true 4-hour rolling $/hr from EquityCurve (falls back smoothly to all-time if <4h collected):
		roll4h := s.HourlyRunRate
		targetTs := nowMs - 4*3600*1000
		if len(s.EquityCurve) >= 2 {
			refPt := s.EquityCurve[0]
			for _, pt := range s.EquityCurve {
				if pt.TsMs <= targetTs {
					refPt = pt
				} else {
					break
				}
			}
			dtHr := float64(nowMs-refPt.TsMs) / 3600000.0
			if dtHr >= 0.50 {
				roll4h = roundTo((s.Equity-refPt.Equity)/dtHr, 3)
			}
		}
		s.Rolling4hHourly = roll4h

		s.StatusNote = fmt.Sprintf("Net $%+.2f ($%+.2f/hr) | Fills:%dM/%dT (Pickoff:%d) | Pos:%d | Resting:%d",
			netGain, s.HourlyRunRate, s.MakerFills, s.TakerFills, s.PickoffFills, len(snaps), s.ActiveMakers)

		if s.HourlyRunRate > bestHr {
			bestHr = s.HourlyRunRate
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
	if json.Unmarshal(raw, &tel) != nil || !strings.HasPrefix(tel.Version, "ARENA-v4.") || tel.BootTsMs <= 0 {
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
		s.GrossVolumeUsd = saved.GrossVolumeUsd
		s.EquityCurve = saved.EquityCurve
		s.Positions = map[string]*Position{}
		for _, p := range saved.PosSnap {
			pc := p
			s.Positions[posKey(p.Venue, p.Symbol)] = &pc
		}
	}
	if len(tel.RecentFills) > 0 {
		recentFills = make([]FillAudit, len(tel.RecentFills))
		for i := range tel.RecentFills {
			recentFills[i] = tel.RecentFills[len(tel.RecentFills)-1-i]
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
		pDec int
		qDec int
		tick float64
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
				} `json:"filters"`
			} `json:"symbols"`
		}
		if json.Unmarshal(body, &ex) == nil {
			for _, s := range ex.Symbols {
				if s.Status == "TRADING" && s.ContractType == "PERPETUAL" {
					tk := 0.0
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
						}
					}
					bnPrecMap[s.Symbol] = bnPrec{pDec: pDec, qDec: s.QuantityPrecision, tick: tk}
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
					sp.BnPriceDec, sp.BnQtyDec, sp.BnTick = pr.pDec, pr.qDec, pr.tick
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
				} `json:"filters"`
			} `json:"symbols"`
		}
		if json.Unmarshal(aBody, &aex) == nil {
			mu.Lock()
			for _, s := range aex.Symbols {
				if s.Status != "TRADING" {
					continue
				}
				tk := 0.0
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
					}
				}
				asterSpecs[s.Symbol] = &BnUsdcSpec{
					BaseSym:  s.BaseAsset,
					UsdcSym:  s.Symbol,
					PriceDec: pDec,
					QtyDec:   s.QuantityPrecision,
					TickSize: tk,
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

func runHyperliquidWS() {
	for {
		conn, _, err := websocket.DefaultDialer.Dial("wss://api.hyperliquid.xyz/ws", nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		_ = conn.WriteJSON(map[string]any{
			"method":       "subscribe",
			"subscription": map[string]string{"type": "allMids"},
		})
		for {
			_ = conn.SetReadDeadline(time.Now().Add(25 * time.Second))
			_, raw, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var msg struct {
				Channel string `json:"channel"`
				Data    struct {
					Mids map[string]string `json:"mids"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &msg) == nil && msg.Channel == "allMids" {
				incFeed("hlMids")
				nowMs := time.Now().UnixMilli()
				mu.Lock()
				hlUpdatedMs = nowMs
				for k, v := range msg.Data.Mids {
					px, _ := strconv.ParseFloat(v, 64)
					if px > 0 {
						hlMids[k] = px
						recordMidLocked("HL:"+k, px, nowMs)
					}
				}
				mu.Unlock()
			}
		}
		_ = conn.Close()
		time.Sleep(2 * time.Second)
	}
}

func runPolymarketPoller() {
	// Discover active high-volume CLOB binary markets and poll live orderbooks + trades
	lastDisc := int64(0)
	lastTradePrice := map[string]float64{}
	for {
		nowMs := time.Now().UnixMilli()
		if nowMs-lastDisc > 300000 {
			if resp, err := httpCli.Get("https://gamma-api.polymarket.com/markets?active=true&closed=false&limit=12&order=volume24hr&ascending=false"); err == nil {
				body, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				var arr []struct {
					Slug          string `json:"slug"`
					Question      string `json:"question"`
					ClobTokenIds  string `json:"clobTokenIds"`
					EnableBook    bool   `json:"enableOrderBook"`
				}
				if json.Unmarshal(body, &arr) == nil {
					mu.Lock()
					kept := 0
					for _, m := range arr {
						if !m.EnableBook || m.Slug == "" || kept >= 5 {
							continue
						}
						var toks []string
						if json.Unmarshal([]byte(m.ClobTokenIds), &toks) == nil && len(toks) >= 2 {
							polyMarkets[m.Slug] = &PolyMarket{
								Slug:       m.Slug,
								Question:   m.Question,
								YesTokenID: toks[0],
								NoTokenID:  toks[1],
							}
							kept++
						}
					}
					mu.Unlock()
					lastDisc = nowMs
				}
			}
		}

		mu.Lock()
		slugs := make([]*PolyMarket, 0, len(polyMarkets))
		for _, pm := range polyMarkets {
			slugs = append(slugs, pm)
		}
		mu.Unlock()

		for _, pm := range slugs {
			resp, err := httpCli.Get("https://clob.polymarket.com/book?token_id=" + pm.YesTokenID)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			var ob struct {
				Bids []struct {
					Price string `json:"price"`
					Size  string `json:"size"`
				} `json:"bids"`
				Asks []struct {
					Price string `json:"price"`
					Size  string `json:"size"`
				} `json:"asks"`
				LastTradePrice string `json:"last_trade_price"`
			}
			if json.Unmarshal(body, &ob) != nil || len(ob.Bids) == 0 || len(ob.Asks) == 0 {
				continue
			}
			// Polymarket bids are sorted ascending or descending depending on endpoint; find true max bid and min ask!
			bestBid, bestBidSz := 0.0, 0.0
			bids := make([]BookLevel, 0, len(ob.Bids))
			for _, b := range ob.Bids {
				p, _ := strconv.ParseFloat(b.Price, 64)
				s, _ := strconv.ParseFloat(b.Size, 64)
				if p > 0 && s > 0 {
					bids = append(bids, BookLevel{Price: p, Size: s})
					if p > bestBid {
						bestBid, bestBidSz = p, s
					}
				}
			}
			bestAsk, bestAskSz := 999.0, 0.0
			asks := make([]BookLevel, 0, len(ob.Asks))
			for _, a := range ob.Asks {
				p, _ := strconv.ParseFloat(a.Price, 64)
				s, _ := strconv.ParseFloat(a.Size, 64)
				if p > 0 && s > 0 {
					asks = append(asks, BookLevel{Price: p, Size: s})
					if p < bestAsk {
						bestAsk, bestAskSz = p, s
					}
				}
			}
			if bestBid <= 0 || bestAsk >= 1.0 || bestAsk <= bestBid {
				continue
			}
			sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
			sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })

			ltp, _ := strconv.ParseFloat(ob.LastTradePrice, 64)
			incFeed("polyBook")
			tNow := time.Now().UnixMilli()

			mu.Lock()
			pm.YesBid, pm.YesBidSz, pm.YesAsk, pm.YesAskSz, pm.UpdatedMs = bestBid, bestBidSz, bestAsk, bestAskSz, tNow
			bk := getBookLocked(VenuePoly, pm.Slug)
			bk.Bid, bk.BidSz, bk.Ask, bk.AskSz, bk.Bids, bk.Asks, bk.UpdatedMs = bestBid, bestBidSz, bestAsk, bestAskSz, bids, asks, tNow
			prevLtp := lastTradePrice[pm.Slug]
			if ltp > 0 && prevLtp > 0 && math.Abs(ltp-prevLtp) > 1e-6 {
				// Live trade print detected on Polymarket CLOB!
				sellerAgg := ltp <= bestBid+0.0005
				processLiveTradeLocked(VenuePoly, pm.Slug, ltp, 60.0, sellerAgg, tNow)
			}
			if ltp > 0 {
				lastTradePrice[pm.Slug] = ltp
			}
			mu.Unlock()
			time.Sleep(250 * time.Millisecond)
		}
		time.Sleep(2500 * time.Millisecond)
	}
}

func runAsterBookWS() {
	url := "wss://fstream.asterdex.com/stream?streams=!bookTicker"
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
				Data struct {
					Sym   string `json:"s"`
					Bid   string `json:"b"`
					BidSz string `json:"B"`
					Ask   string `json:"a"`
					AskSz string `json:"A"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &env) != nil || env.Data.Sym == "" {
				continue
			}
			bid, _ := strconv.ParseFloat(env.Data.Bid, 64)
			bsz, _ := strconv.ParseFloat(env.Data.BidSz, 64)
			ask, _ := strconv.ParseFloat(env.Data.Ask, 64)
			asz, _ := strconv.ParseFloat(env.Data.AskSz, 64)
			if bid > 0 && ask > bid {
				incFeed("asterBook")
				nowMs := time.Now().UnixMilli()
				mu.Lock()
				b := getBookLocked("ASTER", env.Data.Sym)
				b.Bid, b.BidSz, b.Ask, b.AskSz, b.UpdatedMs = bid, bsz, ask, asz, nowMs
				recordMidLocked("ASTER:"+env.Data.Sym, 0.5*(bid+ask), nowMs)
				mu.Unlock()
			}
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runAsterAggTradeShard(streams []string) {
	url := "wss://fstream.asterdex.com/stream?streams=" + strings.Join(streams, "/")
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
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
					BuyerMaker bool   `json:"m"`
				} `json:"data"`
			}
			if json.Unmarshal(raw, &env) != nil || env.Data.Sym == "" {
				continue
			}
			px, _ := strconv.ParseFloat(env.Data.Px, 64)
			qty, _ := strconv.ParseFloat(env.Data.Qty, 64)
			incFeed("asterTrade")
			nowMs := time.Now().UnixMilli()
			mu.Lock()
			processLiveTradeLocked("ASTER", env.Data.Sym, px, qty, env.Data.BuyerMaker, nowMs)
			mu.Unlock()
		}
		_ = conn.Close()
		time.Sleep(time.Second)
	}
}

func runAsterAggTradeWS() {
	asterSyms := []string{
		"LITUSDT", "VVVUSDT", "XMRUSDT", "HYPEUSDT", "ENAUSDT", "NEARUSDT", "UNIUSDT", "AAVEUSDT",
		"AVAXUSDT", "SUIUSDT", "ARBUSDT", "WLDUSDT",
		"NVDAUSDT", "AAPLUSDT", "TSLAUSDT", "QQQUSDT", "SPYUSDT", "AMZNUSDT", "METAUSDT",
		"COINUSDT", "MSTRUSDT", "GOOGLUSDT", "AMDUSDT", "INTCUSDT", "XAUUSDT", "XAGUSDT",
		"XAUUSD1", "METAUSD1", "QQQUSD1", "NVDAUSD1", "XAGUSD1",
	}
	streams := []string{}
	mu.Lock()
	for _, s := range asterSyms {
		if asterSpecs[s] != nil {
			streams = append(streams, strings.ToLower(s)+"@aggTrade")
		}
	}
	mu.Unlock()
	if len(streams) == 0 {
		return
	}
	for i := 0; i < len(streams); i += 16 {
		end := i + 16
		if end > len(streams) {
			end = len(streams)
		}
		go runAsterAggTradeShard(append([]string(nil), streams[i:end]...))
		time.Sleep(120 * time.Millisecond)
	}
}

func main() {
	debug.SetMemoryLimit(240 << 20) // Hard 240 MiB ceiling so Tokyo EC2 never OOM-kills arena_go
	debug.SetGCPercent(40)

	for _, k := range []string{"ltBook", "ltTrade", "bnBook", "bnTrade", "bnLiq", "hlMids", "asterBook", "asterTrade"} {
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

	go runFundingPoller()
	go runLighterWS()
	go runBinanceBookAndLiqWS()
	go runBinanceAggTradeWS()
	go runHyperliquidWS()
	go runAsterBookWS()
	go runAsterAggTradeWS()

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
