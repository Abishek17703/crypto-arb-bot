package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	StartBudgetUSD = 489.12 // Exact real capital ($91.69 Lighter + $397.43 Binance)
	TargetHourly   = 5.00
	ArenaOutPath   = "/tmp/forward_arena.json"
	ArenaStatePath = "/opt/crypto-arb-bot/forward_arena_state.json"
	ArenaVer       = "ARENA-v3.0-TOKYO-ZEROFEEMM"

	VenueBnUsdc = "BN_USDC"
	VenueBnUsdt = "BN_USDT"
	VenueLtStd  = "LT_STD"
	VenueLtPrem = "LT_PREM"
	VenuePoly   = "POLY"
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
	VenueBnUsdc: {PlaceLatMs: 4, CancelLatMs: 8, TakerLatMs: 4, MakerFeeBp: 0.0, TakerFeeBp: 4.0, MaxTxPerMin: 1200},
	VenueBnUsdt: {PlaceLatMs: 4, CancelLatMs: 8, TakerLatMs: 4, MakerFeeBp: 2.0, TakerFeeBp: 5.0, MaxTxPerMin: 1200},
	VenueLtStd:  {PlaceLatMs: 5, CancelLatMs: 305, TakerLatMs: 305, MakerFeeBp: 0.0, TakerFeeBp: 0.0, MaxTxPerMin: 56},
	VenueLtPrem: {PlaceLatMs: 5, CancelLatMs: 5, TakerLatMs: 145, MakerFeeBp: 0.30, TakerFeeBp: 2.00, MaxTxPerMin: 3800},
	VenuePoly:   {PlaceLatMs: 60, CancelLatMs: 65, TakerLatMs: 65, MakerFeeBp: -0.50, TakerFeeBp: 6.0, MaxTxPerMin: 300},
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
			ID:        "S1_LT_PREM_LEADLAG_MM",
			Name:      "Lighter Premium Lead-Lag Maker (0ms Cancel)",
			Desc:      "Quotes 0.30bp Maker on wide Lighter markets (VVV, LIT, XMR, HYPE, ENA, NEAR, UNI, WLD, AAVE, AVAX, PENGU, IBM) anchored to <1.5ms Tokyo Binance WS; cancels in 5ms before retail takers hit.",
			VenueDesc: "LT_PREM (0.30bp M / 5ms cancel)",
			MaxLevCap: 6.0,
		},
		{
			ID:        "S2_BN_USDC_WIDE_MM",
			Name:      "Binance USDC-M 0%-Fee Lead-Lag Market Maker",
			Desc:      "Quotes 0%-fee Maker on wide-spread Binance USDC-M perps (NEAR 7bp, WLD 9bp, ARB 4bp, TIA 9bp, BOME 11bp, KAITO 13bp, PNUT 12bp, PENGU 7bp, WLFI 7bp, BONK 10bp) anchored to <1.5ms USDT-M lead with 8ms cancel!",
			VenueDesc: "BN_USDC (0% M / 8ms cancel)",
			MaxLevCap: 6.5,
		},
		{
			ID:        "S3_LT_PREM_BN_DN",
			Name:      "All-Maker Cross-Venue Arb: LT Prem Entry + BN USDC 0% Exit",
			Desc:      "Rests 0.30bp Maker on Lighter Premium (5ms cancel) -> 4ms BN USDC-M hedge; unwinds via 0%-fee Binance USDC-M Maker -> LT Prem close (cuts round-trip fee drag in half!).",
			VenueDesc: "LT_PREM M + BN_USDC 0% M Exit",
			MaxLevCap: 7.5,
		},
		{
			ID:        "S4_BN_USDC_MEAN_REV",
			Name:      "Binance USDC-M vs USDT-M Micro-Dislocation Maker",
			Desc:      "Rests 0%-fee Maker on Binance USDC-M when USDC-M temporarily deviates >=3.8bp from FX-adjusted USDT-M fair value across 28 liquid pairs, exiting via 0%-Maker on convergence.",
			VenueDesc: "BN_USDC 0% Maker In & Out",
			MaxLevCap: 6.5,
		},
		{
			ID:        "S5_BN_LIQ_WICK_CATCHER",
			Name:      "Binance ForceOrder Liquidation Cascade Absorber",
			Desc:      "Proven 100% win rate in v2.1! Catches live !forceOrder@arr liquidation cascades + exhaustion wicks across all 35 Binance USDC-M perps via 0%-fee Maker bids/asks.",
			VenueDesc: "BN_USDC 0% Maker (All 35 Perps)",
			MaxLevCap: 6.5,
		},
		{
			ID:        "S6_LT_PREM_ULTRA_MM",
			Name:      "Lighter Premium Ultra-Wide MM (DRV, PRL, VARIATIONAL, VVV)",
			Desc:      "Upgrades wide-spread Lighter perps (PRL 23bp, DRV 24bp, VARIATIONAL 16bp, VVV 15bp, IBM 7bp) from 305ms Standard tier to 5ms-cancel Premium tier (0.30bp Maker) to eliminate cancel pickoffs!",
			VenueDesc: "LT_PREM (0.30bp M / 5ms cancel)",
			MaxLevCap: 4.5,
		},
		{
			ID:        "S7_TRIPLE_TAPE_MOMENTUM",
			Name:      "Tokyo Triple-Tape Synchronized Breakout Join",
			Desc:      "Joins 0%-fee Binance USDC-M BBO when Binance USDT-M + Hyperliquid + Lighter align on impulse breakouts, exiting via 0%-Maker take-profit.",
			VenueDesc: "BN_USDC 0% Maker / 4bp Stop",
			MaxLevCap: 5.5,
		},
		{
			ID:        "S8_LT_STD_ZERO_FEE_DN",
			Name:      "0%-Fee Lighter Standard (#770015) + 0%-Maker BN USDC Arb",
			Desc:      "Exact #770015 Standard account tier (0% LT Maker/Taker + 305ms speed-bump) paired with Binance USDC-M (4ms hedge + 0%-Maker exit = ONLY 4.0bp total 4-leg fee!).",
			VenueDesc: "LT_STD 0% Fee + BN_USDC 0% Exit",
			MaxLevCap: 7.0,
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

func markPriceLocked(venue, sym string, fallback float64) float64 {
	lv := venue
	if venue == VenueLtStd || venue == VenueLtPrem {
		lv = "LT"
	}
	b := books[bookKey(lv, sym)]
	if b != nil && b.Bid > 0 && b.Ask > 0 {
		return 0.5 * (b.Bid + b.Ask)
	}
	return fallback
}

func queueAtLimitLocked(venue, sym, side string, limitPx float64) (float64, bool) {
	lv := venue
	if venue == VenueLtStd || venue == VenueLtPrem {
		lv = "LT"
	}
	b := books[bookKey(lv, sym)]
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
		matchVenue := false
		if feedVenue == "LT" && (o.Venue == VenueLtStd || o.Venue == VenueLtPrem) {
			matchVenue = true
		} else if o.Venue == feedVenue {
			matchVenue = true
		}
		if !matchVenue || o.Symbol != sym {
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

		lv := to.Venue
		if to.Venue == VenueLtStd || to.Venue == VenueLtPrem {
			lv = "LT"
		}
		b := books[bookKey(lv, to.Symbol)]
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

// onStrategyFillHookLocked triggers immediate cross-venue hedges when a Maker leg fills!
func onStrategyFillHookLocked(stratID, venue, sym, side string, fillPx, qty float64, tag string, nowMs int64) {
	if (stratID == "S3_LT_PREM_BN_DN" && venue == VenueLtPrem) ||
		(stratID == "S8_LT_STD_ZERO_FEE_DN" && venue == VenueLtStd) {
		spec := ltSpecs[sym]
		if spec != nil && (strings.Contains(tag, "_ENTRY") || strings.Contains(tag, "_MAKER_EXIT")) {
			hedgeSide := "SELL"
			if side == "SELL" {
				hedgeSide = "BUY"
			}
			bnVenue := VenueBnUsdc
			bnSym := spec.BnUsdcSym
			if bnSym == "" {
				bnVenue = VenueBnUsdt
				bnSym = spec.BnUsdtSym
			}
			if bnSym != "" {
				bb := books[bookKey(bnVenue, bnSym)]
				sigPx := fillPx
				if bb != nil && bb.Bid > 0 && bb.Ask > 0 {
					if hedgeSide == "BUY" {
						sigPx = bb.Ask
					} else {
						sigPx = bb.Bid
					}
				}
				hQty := qty
				if spec.BnQtyDec >= 0 {
					hQty = roundTo(qty, spec.BnQtyDec)
				}
				if hQty > 0 {
					isReduce := strings.Contains(tag, "_MAKER_EXIT")
					placeTakerLocked(stratID, bnVenue, bnSym, hedgeSide, hQty, sigPx, 20.0, isReduce, stratID[:2]+"_BN_HEDGE", nowMs)
				}
			}
			return
		}
	}

	if (stratID == "S3_LT_PREM_BN_DN" || stratID == "S8_LT_STD_ZERO_FEE_DN") && venue == VenueBnUsdc && strings.Contains(tag, "_BN_MAKER_EXIT:") {
		parts := strings.Split(tag, ":")
		if len(parts) == 2 {
			ltSym := parts[1]
			hedgeSide := "SELL"
			if side == "SELL" {
				hedgeSide = "BUY"
			}
			lb := books[bookKey("LT", ltSym)]
			sigPx := fillPx
			if lb != nil && lb.Bid > 0 && lb.Ask > 0 {
				if hedgeSide == "BUY" {
					sigPx = lb.Ask
				} else {
					sigPx = lb.Bid
				}
			}
			ltVenue := VenueLtStd
			if stratID == "S3_LT_PREM_BN_DN" {
				ltVenue = VenueLtPrem
			}
			hQty := qty
			if sp := ltSpecs[ltSym]; sp != nil {
				hQty = roundTo(qty, sp.SizeDecimals)
			}
			placeTakerLocked(stratID, ltVenue, ltSym, hedgeSide, hQty, sigPx, 25.0, true, stratID[:2]+"_LT_CLOSE", nowMs)
		}
		return
	}
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

func evalStrategiesLocked(nowMs int64) {
	btcMove1s, _ := midReturnBpLocked("BN_USDT:BTCUSDT", 1000, nowMs)

	// ------------------------------------------------------------------------
	// S1: Lighter Premium Lead-Lag Market Maker (0.30bp Maker, 5ms cancel)
	// Proven +$3.32/hr (+$4.16/hr realized) in v2.1! Expanded with IBM + tighter skew!
	// ------------------------------------------------------------------------
	s1Syms := []string{"VVV", "IBM", "LIT", "XMR", "HYPE", "ENA", "NEAR", "UNI", "WLD", "AAVE", "AVAX", "PENGU"}
	for _, sym := range s1Syms {
		spec := ltSpecs[sym]
		lb := books[bookKey("LT", sym)]
		if spec == nil || lb == nil || lb.Bid <= 0 || lb.Ask <= lb.Bid || nowMs-lb.UpdatedMs > 2500 {
			continue
		}
		bnVenue := VenueBnUsdc
		bnSym := spec.BnUsdcSym
		if bnSym == "" {
			bnVenue = VenueBnUsdt
			bnSym = spec.BnUsdtSym
		}
		if bnSym == "" {
			continue
		}
		bnKey := bookKey(bnVenue, bnSym)
		bb := books[bnKey]
		if bb == nil || bb.Bid <= 0 || bb.Ask <= bb.Bid || nowMs-bb.UpdatedMs > 2000 {
			continue
		}

		bnRet500, _ := midReturnBpLocked(bnKey, 500, nowMs)
		ltMid := 0.5 * (lb.Bid + lb.Ask)
		bnMid := (0.5 * (bb.Bid + bb.Ask)) / usdtFxLocked(bnVenue)
		ltSpdBp := ((lb.Ask - lb.Bid) / ltMid) * 10000.0

		pos := strats["S1_LT_PREM_LEADLAG_MM"].Positions[posKey(VenueLtPrem, sym)]
		invUsd := 0.0
		if pos != nil {
			invUsd = pos.Qty * ltMid
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((ltMid - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - ltMid) / pos.AvgPx) * 10000.0
			}
			if upnlBp < -28.0 {
				cancelAllMakersForSymLocked("S1_LT_PREM_LEADLAG_MM", VenueLtPrem, sym, nowMs)
				closeSide := "SELL"
				sigPx := lb.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					sigPx = lb.Ask
				}
				placeTakerLocked("S1_LT_PREM_LEADLAG_MM", VenueLtPrem, sym, closeSide, math.Abs(pos.Qty), sigPx, 25.0, true, "S1_SL_EXIT", nowMs)
				stratCoolMap["S1:"+sym] = nowMs + 15000
				continue
			}
		}

		if nowMs < stratCoolMap["S1:"+sym] {
			cancelAllMakersForSymLocked("S1_LT_PREM_LEADLAG_MM", VenueLtPrem, sym, nowMs)
			continue
		}

		if math.Abs(bnRet500) > 1.4 || math.Abs(btcMove1s) > 2.6 {
			if bnRet500 < -1.4 || btcMove1s < -2.6 {
				for _, mo := range makerOrders {
					if mo.StratID == "S1_LT_PREM_LEADLAG_MM" && mo.Symbol == sym && mo.Side == "BUY" && mo.CancelReqMs == 0 {
						requestCancelMakerLocked(mo.ID, nowMs)
					}
				}
			}
			if bnRet500 > 1.4 || btcMove1s > 2.6 {
				for _, mo := range makerOrders {
					if mo.StratID == "S1_LT_PREM_LEADLAG_MM" && mo.Symbol == sym && mo.Side == "SELL" && mo.CancelReqMs == 0 {
						requestCancelMakerLocked(mo.ID, nowMs)
					}
				}
			}
			continue
		}

		emaKey := "S1B:" + sym
		curRatio := ltMid / bnMid
		if dnBasisEMA[emaKey] <= 0 {
			dnBasisEMA[emaKey] = curRatio
		} else {
			dnBasisEMA[emaKey] = dnBasisEMA[emaKey]*0.992 + curRatio*0.008
		}
		fairMid := bnMid * dnBasisEMA[emaKey]
		halfSpdBp := math.Max(3.0, math.Min(ltSpdBp*0.45, 14.0))
		invSkewBp := -(invUsd / 100.0) * 3.4

		bidPx := floorTo(fairMid*(1.0+(-halfSpdBp+invSkewBp)/10000.0), spec.PriceDecimals)
		askPx := ceilTo(fairMid*(1.0+(halfSpdBp+invSkewBp)/10000.0), spec.PriceDecimals)
		if bidPx >= lb.Ask {
			bidPx = floorTo(lb.Ask-math.Pow10(-spec.PriceDecimals), spec.PriceDecimals)
		}
		if askPx <= lb.Bid {
			askPx = ceilTo(lb.Bid+math.Pow10(-spec.PriceDecimals), spec.PriceDecimals)
		}

		slotUsd := 340.0
		qty := roundTo(slotUsd/ltMid, spec.SizeDecimals)
		if qty < spec.MinBase {
			continue
		}

		if invUsd < 420.0 && (ltSpdBp >= 4.2 || invUsd < -20.0) {
			cancelStaleOrRefreshMakerLocked("S1_LT_PREM_LEADLAG_MM", VenueLtPrem, sym, "BUY", bidPx, qty, 1.2, invUsd < -20.0, "S1_MM_BID", 25000, nowMs)
		} else {
			for _, mo := range makerOrders {
				if mo.StratID == "S1_LT_PREM_LEADLAG_MM" && mo.Symbol == sym && mo.Side == "BUY" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}

		if invUsd > -420.0 && (ltSpdBp >= 4.2 || invUsd > 20.0) {
			cancelStaleOrRefreshMakerLocked("S1_LT_PREM_LEADLAG_MM", VenueLtPrem, sym, "SELL", askPx, qty, 1.2, invUsd > 20.0, "S1_MM_ASK", 25000, nowMs)
		} else {
			for _, mo := range makerOrders {
				if mo.StratID == "S1_LT_PREM_LEADLAG_MM" && mo.Symbol == sym && mo.Side == "SELL" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// S2: Binance USDC-M 0%-Fee Lead-Lag Market Maker!
	// Quotes 0%-fee Maker on multi-tick wide Binance USDC-M perps anchored to
	// <1.5ms Tokyo USDT-M lead with 8ms cancel! Zero exchange fees on both legs!
	// ------------------------------------------------------------------------
	s2UsdcSyms := []string{
		"NEARUSDC", "WLDUSDC", "ARBUSDC", "TIAUSDC", "BOMEUSDC", "KAITOUSDC",
		"PNUTUSDC", "PENGUUSDC", "WLFIUSDC", "1000BONKUSDC", "FILUSDC", "ETHFIUSDC",
		"AVAXUSDC", "AAVEUSDC", "HBARUSDC", "ENAUSDC", "LINKUSDC",
	}
	s2Acct := strats["S2_BN_USDC_WIDE_MM"]
	fxNow := usdtFxLocked(VenueBnUsdt)
	for _, uSym := range s2UsdcSyms {
		bs := bnUsdcSpecs[uSym]
		if bs == nil || bs.UsdtSym == "" {
			continue
		}
		bc := books[bookKey(VenueBnUsdc, uSym)]
		bu := books[bookKey(VenueBnUsdt, bs.UsdtSym)]
		if bc == nil || bu == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || bu.Bid <= 0 || bu.Ask <= bu.Bid {
			continue
		}
		if nowMs-bc.UpdatedMs > 1800 || nowMs-bu.UpdatedMs > 1800 {
			continue
		}
		midC := 0.5 * (bc.Bid + bc.Ask)
		spdBp := ((bc.Ask - bc.Bid) / midC) * 10000.0
		fairC := (0.5 * (bu.Bid + bu.Ask)) / fxNow
		uRet400, _ := midReturnBpLocked(bookKey(VenueBnUsdt, bs.UsdtSym), 400, nowMs)

		pos := s2Acct.Positions[posKey(VenueBnUsdc, uSym)]
		invUsd := 0.0
		if pos != nil {
			invUsd = pos.Qty * midC
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((midC - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - midC) / pos.AvgPx) * 10000.0
			}
			if upnlBp <= -18.0 || (nowMs-pos.OpenedMs > 90000 && upnlBp <= -4.0) {
				cancelAllMakersForSymLocked("S2_BN_USDC_WIDE_MM", VenueBnUsdc, uSym, nowMs)
				closeSide := "SELL"
				px := bc.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					px = bc.Ask
				}
				placeTakerLocked("S2_BN_USDC_WIDE_MM", VenueBnUsdc, uSym, closeSide, math.Abs(pos.Qty), px, 15.0, true, "S2_SL_EXIT", nowMs)
				stratCoolMap["S2:"+uSym] = nowMs + 15000
				continue
			}
		}

		if nowMs < stratCoolMap["S2:"+uSym] {
			cancelAllMakersForSymLocked("S2_BN_USDC_WIDE_MM", VenueBnUsdc, uSym, nowMs)
			continue
		}

		// 8ms cancel shield when USDT-M lead moves >= 1.2bp!
		if uRet400 <= -1.2 || btcMove1s <= -2.2 {
			for _, mo := range makerOrders {
				if mo.StratID == "S2_BN_USDC_WIDE_MM" && mo.Symbol == uSym && mo.Side == "BUY" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
		if uRet400 >= 1.2 || btcMove1s >= 2.2 {
			for _, mo := range makerOrders {
				if mo.StratID == "S2_BN_USDC_WIDE_MM" && mo.Symbol == uSym && mo.Side == "SELL" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
		if math.Abs(uRet400) >= 1.2 || math.Abs(btcMove1s) >= 2.2 {
			continue
		}

		halfSpdBp := math.Max(1.8, math.Min(spdBp*0.48, 8.0))
		invSkewBp := -(invUsd / 100.0) * 1.8
		bidPx := floorTo(fairC*(1.0+(-halfSpdBp+invSkewBp)/10000.0), bs.PriceDec)
		askPx := ceilTo(fairC*(1.0+(halfSpdBp+invSkewBp)/10000.0), bs.PriceDec)
		if bs.TickSize > 0 {
			if bidPx >= bc.Ask {
				bidPx = roundTo(bc.Ask-bs.TickSize, bs.PriceDec)
			}
			if askPx <= bc.Bid {
				askPx = roundTo(bc.Bid+bs.TickSize, bs.PriceDec)
			}
		}

		slotUsd := 320.0
		qty := roundTo(slotUsd/midC, bs.QtyDec)
		if qty*midC < 12.0 {
			continue
		}

		if invUsd < 360.0 && (spdBp >= 2.8 || invUsd < -15.0) {
			cancelStaleOrRefreshMakerLocked("S2_BN_USDC_WIDE_MM", VenueBnUsdc, uSym, "BUY", bidPx, qty, 0.9, invUsd < -15.0, "S2_USDC_BID", 20000, nowMs)
		} else {
			for _, mo := range makerOrders {
				if mo.StratID == "S2_BN_USDC_WIDE_MM" && mo.Symbol == uSym && mo.Side == "BUY" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
		if invUsd > -360.0 && (spdBp >= 2.8 || invUsd > 15.0) {
			cancelStaleOrRefreshMakerLocked("S2_BN_USDC_WIDE_MM", VenueBnUsdc, uSym, "SELL", askPx, qty, 0.9, invUsd > 15.0, "S2_USDC_ASK", 20000, nowMs)
		} else {
			for _, mo := range makerOrders {
				if mo.StratID == "S2_BN_USDC_WIDE_MM" && mo.Symbol == uSym && mo.Side == "SELL" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
	}

	// ------------------------------------------------------------------------
	// S3: All-Maker-Exit Cross-Venue Delta-Neutral (USDC-M ONLY!)
	// Entry: LT Prem Maker (0.30bp) -> 4ms BN USDC-M Taker (4.0bp) = 4.30bp
	// Exit: BN USDC-M Maker (0.00bp) -> LT Prem Taker (2.0bp) = 2.00bp!
	// Total 4-leg fee = ONLY 6.30bp, NO timeout Taker dump!
	// ------------------------------------------------------------------------
	s3Syms := []string{
		"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB",
		"AVAX", "PENGU", "TRUMP", "TIA", "WLFI", "KAITO", "CRV", "ETHFI", "LINK", "DOGE", "XRP",
	}
	s3Acct := strats["S3_LT_PREM_BN_DN"]
	for _, sym := range s3Syms {
		spec := ltSpecs[sym]
		lb := books[bookKey("LT", sym)]
		if spec == nil || spec.BnUsdcSym == "" || lb == nil || lb.Bid <= 0 || lb.Ask <= lb.Bid || nowMs-lb.UpdatedMs > 2500 {
			continue
		}
		bc := books[bookKey(VenueBnUsdc, spec.BnUsdcSym)]
		if bc == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || nowMs-bc.UpdatedMs > 2000 {
			continue
		}
		bnMid := 0.5 * (bc.Bid + bc.Ask)
		ltMid := 0.5 * (lb.Bid + lb.Ask)

		emaKey := "DN:" + sym
		rawBasisBp := ((ltMid - bnMid) / bnMid) * 10000.0
		if _, ok := dnBasisEMA[emaKey]; !ok {
			dnBasisEMA[emaKey] = rawBasisBp
		} else {
			dnBasisEMA[emaKey] = dnBasisEMA[emaKey]*0.995 + rawBasisBp*0.005
		}
		eqBasisBp := dnBasisEMA[emaKey]

		ltPos := s3Acct.Positions[posKey(VenueLtPrem, sym)]
		bnPos := s3Acct.Positions[posKey(VenueBnUsdc, spec.BnUsdcSym)]

		if ltPos != nil && math.Abs(ltPos.Qty) > 1e-8 {
			for _, mo := range makerOrders {
				if mo.StratID == "S3_LT_PREM_BN_DN" && mo.Venue == VenueLtPrem && mo.Symbol == sym && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
			// Ensure BN hedge leg exists!
			if bnPos == nil || math.Abs(bnPos.Qty) <= 1e-8 {
				if nowMs-ltPos.OpenedMs > 800 {
					hSide := "BUY"
					px := bc.Ask
					if ltPos.Qty > 0 {
						hSide = "SELL"
						px = bc.Bid
					}
					hQty := roundTo(math.Abs(ltPos.Qty), spec.BnQtyDec)
					if hQty > 0 {
						placeTakerLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, hSide, hQty, px, 20.0, false, "S3_BN_HEDGE", nowMs)
					}
				}
				continue
			}

			qty := math.Abs(bnPos.Qty)
			if bnPos.Qty > 0 {
				// Long BN, Short LT -> rest 0%-fee BN SELL Maker at bc.Ask when net edge >= 8.5bp (6.3bp fees + 2.2bp net)!
				exitGrossBp := ((bc.Ask - bnPos.AvgPx) / bnPos.AvgPx * 10000.0) + ((ltPos.AvgPx - lb.Ask) / ltPos.AvgPx * 10000.0)
				if exitGrossBp >= 8.5 || (nowMs-ltPos.OpenedMs > 900000 && exitGrossBp >= 6.4) {
					cancelStaleOrRefreshMakerLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, "SELL", bc.Ask, qty, 1.2, true, "S3_BN_MAKER_EXIT:"+sym, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, nowMs)
				}
			} else {
				exitGrossBp := ((bnPos.AvgPx - bc.Bid) / bnPos.AvgPx * 10000.0) + ((lb.Bid - ltPos.AvgPx) / ltPos.AvgPx * 10000.0)
				if exitGrossBp >= 8.5 || (nowMs-ltPos.OpenedMs > 900000 && exitGrossBp >= 6.4) {
					cancelStaleOrRefreshMakerLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, "BUY", bc.Bid, qty, 1.2, true, "S3_BN_MAKER_EXIT:"+sym, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, nowMs)
				}
			}
			continue
		}

		if bnPos != nil && math.Abs(bnPos.Qty) > 1e-8 && nowMs-bnPos.OpenedMs > 1200 {
			side := "SELL"
			px := bc.Bid
			if bnPos.Qty < 0 {
				side = "BUY"
				px = bc.Ask
			}
			placeTakerLocked("S3_LT_PREM_BN_DN", VenueBnUsdc, spec.BnUsdcSym, side, math.Abs(bnPos.Qty), px, 20.0, true, "S3_ORPHAN_CLEAN", nowMs)
			continue
		}

		bnRet400, _ := midReturnBpLocked(bookKey(VenueBnUsdc, spec.BnUsdcSym), 400, nowMs)
		if math.Abs(bnRet400) > 1.8 {
			cancelAllMakersForSymLocked("S3_LT_PREM_BN_DN", VenueLtPrem, sym, nowMs)
			continue
		}

		shortDislocBp := (((lb.Ask - bc.Ask) / bc.Ask) * 10000.0) - eqBasisBp
		longDislocBp := (((bc.Bid - lb.Bid) / bc.Bid) * 10000.0) + eqBasisBp
		// Use common precision so LT & BN quantities match 100%!
		commonDec := spec.SizeDecimals
		if spec.BnQtyDec < commonDec {
			commonDec = spec.BnQtyDec
		}
		qty := roundTo(560.0/ltMid, commonDec)
		if qty < spec.MinBase || qty*ltMid < 20.0 {
			continue
		}

		if shortDislocBp >= 6.8 {
			cancelStaleOrRefreshMakerLocked("S3_LT_PREM_BN_DN", VenueLtPrem, sym, "SELL", lb.Ask, qty, 1.0, false, "S3_ENTRY_SHORT_LT", 20000, nowMs)
		} else if longDislocBp >= 6.8 {
			cancelStaleOrRefreshMakerLocked("S3_LT_PREM_BN_DN", VenueLtPrem, sym, "BUY", lb.Bid, qty, 1.0, false, "S3_ENTRY_LONG_LT", 20000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S3_LT_PREM_BN_DN", VenueLtPrem, sym, nowMs)
		}
	}

	// ------------------------------------------------------------------------
	// S4: Binance USDC-M vs USDT-M Micro-Dislocation 0%-Fee Maker!
	// Rests 0%-Maker on USDC-M when USDC-M deviates >= 3.5bp from FX-adjusted USDT-M!
	// ------------------------------------------------------------------------
	s4Acct := strats["S4_BN_USDC_MEAN_REV"]
	for uSym, bs := range bnUsdcSpecs {
		if bs.UsdtSym == "" {
			continue
		}
		bc := books[bookKey(VenueBnUsdc, uSym)]
		bu := books[bookKey(VenueBnUsdt, bs.UsdtSym)]
		if bc == nil || bu == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || bu.Bid <= 0 || bu.Ask <= bu.Bid {
			continue
		}
		if nowMs-bc.UpdatedMs > 1500 || nowMs-bu.UpdatedMs > 1500 {
			continue
		}
		midC := 0.5 * (bc.Bid + bc.Ask)
		fairC := (0.5 * (bu.Bid + bu.Ask)) / fxNow

		pos := s4Acct.Positions[posKey(VenueBnUsdc, uSym)]
		if pos != nil && math.Abs(pos.Qty) > 1e-8 {
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((midC - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - midC) / pos.AvgPx) * 10000.0
			}
			if upnlBp <= -14.0 || (nowMs-pos.OpenedMs > 60000 && upnlBp <= -3.0) {
				cancelAllMakersForSymLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, nowMs)
				closeSide := "SELL"
				px := bc.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					px = bc.Ask
				}
				placeTakerLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, closeSide, math.Abs(pos.Qty), px, 15.0, true, "S4_STOP_EXIT", nowMs)
				stratCoolMap["S4:"+uSym] = nowMs + 12000
				continue
			}
			if pos.Qty > 0 {
				tpPx := ceilTo(math.Max(bc.Ask, pos.AvgPx*1.00045), bs.PriceDec)
				cancelStaleOrRefreshMakerLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, "SELL", tpPx, math.Abs(pos.Qty), 1.0, true, "S4_REV_TP", 12000, nowMs)
			} else {
				tpPx := floorTo(math.Min(bc.Bid, pos.AvgPx*0.99955), bs.PriceDec)
				cancelStaleOrRefreshMakerLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, "BUY", tpPx, math.Abs(pos.Qty), 1.0, true, "S4_REV_TP", 12000, nowMs)
			}
			continue
		}

		if nowMs < stratCoolMap["S4:"+uSym] {
			continue
		}
		uRet300, _ := midReturnBpLocked(bookKey(VenueBnUsdt, bs.UsdtSym), 300, nowMs)
		if math.Abs(uRet300) > 1.8 {
			cancelAllMakersForSymLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, nowMs)
			continue
		}

		// Is USDC-M Bid cheap by >= 3.6bp vs fair USDT-M, or Ask rich by >= 3.6bp?
		cheapBp := ((fairC - bc.Bid) / fairC) * 10000.0
		richBp := ((bc.Ask - fairC) / fairC) * 10000.0
		qty := roundTo(450.0/midC, bs.QtyDec)
		if qty*midC < 15.0 {
			continue
		}
		if cheapBp >= 3.8 {
			cancelStaleOrRefreshMakerLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, "BUY", bc.Bid, qty, 1.0, false, "S4_BUY_CHEAP", 8000, nowMs)
		} else if richBp >= 3.8 {
			cancelStaleOrRefreshMakerLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, "SELL", bc.Ask, qty, 1.0, false, "S4_SELL_RICH", 8000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S4_BN_USDC_MEAN_REV", VenueBnUsdc, uSym, nowMs)
		}
	}

	// ------------------------------------------------------------------------
	// S5: Binance ForceOrder Liquidation Cascade Absorber (All 39 USDC-M Perps!)
	// Proven 100% win rate (+$2.68/hr) in v2.1; expanded to all 39 USDC-M pairs!
	// ------------------------------------------------------------------------
	s5Acct := strats["S5_BN_LIQ_WICK_CATCHER"]
	for uSym, bs := range bnUsdcSpecs {
		bc := books[bookKey(VenueBnUsdc, uSym)]
		if bc == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || nowMs-bc.UpdatedMs > 2000 {
			continue
		}
		midC := 0.5 * (bc.Bid + bc.Ask)
		pos := s5Acct.Positions[posKey(VenueBnUsdc, uSym)]
		if pos != nil && math.Abs(pos.Qty) > 1e-8 {
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((midC - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - midC) / pos.AvgPx) * 10000.0
			}
			if upnlBp <= -22.0 || nowMs-pos.OpenedMs > 45000 {
				cancelAllMakersForSymLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, nowMs)
				closeSide := "SELL"
				px := bc.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					px = bc.Ask
				}
				placeTakerLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, closeSide, math.Abs(pos.Qty), px, 20.0, true, "S5_STOP_EXIT", nowMs)
				stratCoolMap["S5:"+uSym] = nowMs + 15000
				continue
			}
			if pos.Qty > 0 {
				tpPx := ceilTo(math.Max(bc.Ask, pos.AvgPx*1.00095), bs.PriceDec)
				cancelStaleOrRefreshMakerLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, "SELL", tpPx, math.Abs(pos.Qty), 1.5, true, "S5_BOUNCE_TP", 15000, nowMs)
			} else {
				tpPx := floorTo(math.Min(bc.Bid, pos.AvgPx*0.99905), bs.PriceDec)
				cancelStaleOrRefreshMakerLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, "BUY", tpPx, math.Abs(pos.Qty), 1.5, true, "S5_BOUNCE_TP", 15000, nowMs)
			}
			continue
		}

		if nowMs < stratCoolMap["S5:"+uSym] {
			continue
		}

		liq := liqRecent[bs.UsdtSym]
		if liq == nil {
			liq = liqRecent[uSym]
		}
		ret2500, okR := midReturnBpLocked(bookKey(VenueBnUsdc, uSym), 2500, nowMs)
		hasLiq := liq != nil && (nowMs-liq.TsMs <= 4000) && liq.Usd >= 4000
		if !okR {
			continue
		}

		qty := roundTo(520.0/midC, bs.QtyDec)
		if qty*midC < 15.0 {
			continue
		}

		if (hasLiq && liq.Side == "SELL" && ret2500 <= -11.0) || ret2500 <= -18.0 {
			catchBid := floorTo(bc.Bid*(1.0-0.00015), bs.PriceDec)
			cancelStaleOrRefreshMakerLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, "BUY", catchBid, qty, 2.0, false, "S5_CATCH_DUMP", 4000, nowMs)
		} else if (hasLiq && liq.Side == "BUY" && ret2500 >= 11.0) || ret2500 >= 18.0 {
			catchAsk := ceilTo(bc.Ask*(1.0+0.00015), bs.PriceDec)
			cancelStaleOrRefreshMakerLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, "SELL", catchAsk, qty, 2.0, false, "S5_CATCH_PUMP", 4000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S5_BN_LIQ_WICK_CATCHER", VenueBnUsdc, uSym, nowMs)
		}
	}

	// ------------------------------------------------------------------------
	// S6: Lighter Premium Ultra-Wide MM (DRV 24bp, PRL 23bp, VARIATIONAL 16bp, VVV 15bp, IBM 7bp)
	// Upgrades ultra-wide Lighter markets to 5ms-cancel Premium tier (0.30bp Maker)!
	// ------------------------------------------------------------------------
	s6WideSyms := []string{"DRV", "PRL", "VARIATIONAL", "VVV", "IBM"}
	for _, sym := range s6WideSyms {
		spec := ltSpecs[sym]
		lb := books[bookKey("LT", sym)]
		if spec == nil || lb == nil || lb.Bid <= 0 || lb.Ask <= lb.Bid || nowMs-lb.UpdatedMs > 3000 {
			continue
		}
		ltMid := 0.5 * (lb.Bid + lb.Ask)
		ltSpdBp := ((lb.Ask - lb.Bid) / ltMid) * 10000.0
		ltRet600, _ := midReturnBpLocked("LT:"+sym, 600, nowMs)

		pos := strats["S6_LT_PREM_ULTRA_MM"].Positions[posKey(VenueLtPrem, sym)]
		invUsd := 0.0
		if pos != nil {
			invUsd = pos.Qty * ltMid
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((ltMid - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - ltMid) / pos.AvgPx) * 10000.0
			}
			if upnlBp < -38.0 {
				cancelAllMakersForSymLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, nowMs)
				closeSide := "SELL"
				sigPx := lb.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					sigPx = lb.Ask
				}
				placeTakerLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, closeSide, math.Abs(pos.Qty), sigPx, 35.0, true, "S6_SL_EXIT", nowMs)
				stratCoolMap["S6:"+sym] = nowMs + 20000
				continue
			}
		}
		if nowMs < stratCoolMap["S6:"+sym] {
			cancelAllMakersForSymLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, nowMs)
			continue
		}

		// 5ms cancel shield on sudden Lighter tape or BTC jump!
		if ltRet600 <= -3.5 || btcMove1s <= -3.0 {
			for _, mo := range makerOrders {
				if mo.StratID == "S6_LT_PREM_ULTRA_MM" && mo.Symbol == sym && mo.Side == "BUY" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
		if ltRet600 >= 3.5 || btcMove1s >= 3.0 {
			for _, mo := range makerOrders {
				if mo.StratID == "S6_LT_PREM_ULTRA_MM" && mo.Symbol == sym && mo.Side == "SELL" && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
		}
		if math.Abs(ltRet600) >= 3.5 || math.Abs(btcMove1s) >= 3.0 {
			continue
		}
		if ltSpdBp < 6.0 && math.Abs(invUsd) < 15.0 {
			cancelAllMakersForSymLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, nowMs)
			continue
		}

		totSz := lb.BidSz + lb.AskSz
		micro := ltMid
		if totSz > 0 {
			micro = (lb.Bid*lb.AskSz + lb.Ask*lb.BidSz) / totSz
		}
		halfSpdBp := math.Max(4.5, ltSpdBp*0.44)
		invSkewBp := -(invUsd / 100.0) * 4.5

		bidPx := floorTo(micro*(1.0+(-halfSpdBp+invSkewBp)/10000.0), spec.PriceDecimals)
		askPx := ceilTo(micro*(1.0+(halfSpdBp+invSkewBp)/10000.0), spec.PriceDecimals)
		if bidPx >= lb.Ask {
			bidPx = floorTo(lb.Ask-math.Pow10(-spec.PriceDecimals), spec.PriceDecimals)
		}
		if askPx <= lb.Bid {
			askPx = ceilTo(lb.Bid+math.Pow10(-spec.PriceDecimals), spec.PriceDecimals)
		}

		qty := roundTo(280.0/ltMid, spec.SizeDecimals)
		if qty < spec.MinBase {
			continue
		}
		if invUsd < 340.0 {
			cancelStaleOrRefreshMakerLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, "BUY", bidPx, qty, 1.5, invUsd < -20.0, "S6_ULTRA_BID", 25000, nowMs)
		}
		if invUsd > -340.0 {
			cancelStaleOrRefreshMakerLocked("S6_LT_PREM_ULTRA_MM", VenueLtPrem, sym, "SELL", askPx, qty, 1.5, invUsd > 20.0, "S6_ULTRA_ASK", 25000, nowMs)
		}
	}

	// ------------------------------------------------------------------------
	// S7: Tokyo Triple-Tape Synchronized Breakout Join (0%-Maker BN USDC-M)
	// ------------------------------------------------------------------------
	s7Acct := strats["S7_TRIPLE_TAPE_MOMENTUM"]
	for _, sym := range s3Syms {
		spec := ltSpecs[sym]
		if spec == nil || spec.BnUsdcSym == "" || spec.BnUsdtSym == "" {
			continue
		}
		bc := books[bookKey(VenueBnUsdc, spec.BnUsdcSym)]
		if bc == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || nowMs-bc.UpdatedMs > 1500 {
			continue
		}
		midC := 0.5 * (bc.Bid + bc.Ask)
		pos := s7Acct.Positions[posKey(VenueBnUsdc, spec.BnUsdcSym)]
		if pos != nil && math.Abs(pos.Qty) > 1e-8 {
			upnlBp := 0.0
			if pos.Qty > 0 {
				upnlBp = ((midC - pos.AvgPx) / pos.AvgPx) * 10000.0
			} else {
				upnlBp = ((pos.AvgPx - midC) / pos.AvgPx) * 10000.0
			}
			if upnlBp <= -10.0 || nowMs-pos.OpenedMs > 30000 {
				cancelAllMakersForSymLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, nowMs)
				closeSide := "SELL"
				px := bc.Bid
				if pos.Qty < 0 {
					closeSide = "BUY"
					px = bc.Ask
				}
				placeTakerLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, closeSide, math.Abs(pos.Qty), px, 15.0, true, "S7_STOP_EXIT", nowMs)
				stratCoolMap["S7:"+sym] = nowMs + 10000
				continue
			}
			if pos.Qty > 0 {
				tpPx := ceilTo(math.Max(bc.Ask, pos.AvgPx*1.0007), spec.BnPriceDec)
				cancelStaleOrRefreshMakerLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, "SELL", tpPx, math.Abs(pos.Qty), 1.5, true, "S7_MAKER_TP", 10000, nowMs)
			} else {
				tpPx := floorTo(math.Min(bc.Bid, pos.AvgPx*0.9993), spec.BnPriceDec)
				cancelStaleOrRefreshMakerLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, "BUY", tpPx, math.Abs(pos.Qty), 1.5, true, "S7_MAKER_TP", 10000, nowMs)
			}
			continue
		}

		if nowMs < stratCoolMap["S7:"+sym] {
			continue
		}

		rUsdt, ok1 := midReturnBpLocked(bookKey(VenueBnUsdt, spec.BnUsdtSym), 800, nowMs)
		rLt, ok2 := midReturnBpLocked(bookKey("LT", sym), 800, nowMs)
		rHl, ok3 := midReturnBpLocked("HL:"+sym, 1200, nowMs)
		if !ok1 || !ok2 {
			continue
		}
		qty := roundTo(450.0/midC, spec.BnQtyDec)
		if qty*midC < 15.0 {
			continue
		}
		if rUsdt >= 6.0 && rLt >= 4.5 && (!ok3 || rHl >= 3.5) {
			cancelStaleOrRefreshMakerLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, "BUY", bc.Bid, qty, 1.5, false, "S7_JOIN_UP", 3000, nowMs)
		} else if rUsdt <= -6.0 && rLt <= -4.5 && (!ok3 || rHl <= -3.5) {
			cancelStaleOrRefreshMakerLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, "SELL", bc.Ask, qty, 1.5, false, "S7_JOIN_DN", 3000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S7_TRIPLE_TAPE_MOMENTUM", VenueBnUsdc, spec.BnUsdcSym, nowMs)
		}
	}

	// ------------------------------------------------------------------------
	// S8: 0%-Fee Lighter Standard (#770015) + 0%-Maker Binance USDC-M Arb!
	// Proven +$1.35/hr in v2.1; fixed guaranteed hedge & removed 5m Taker dump!
	// Total 4-leg Exchange Fee = ONLY 4.00bp!
	// ------------------------------------------------------------------------
	s8Syms := []string{"UNI", "HYPE", "ENA", "NEAR", "WLD", "AAVE", "SUI", "ARB", "AVAX", "PENGU", "TRUMP", "TIA", "WLFI", "KAITO", "CRV"}
	s8Acct := strats["S8_LT_STD_ZERO_FEE_DN"]
	for _, sym := range s8Syms {
		spec := ltSpecs[sym]
		lb := books[bookKey("LT", sym)]
		if spec == nil || spec.BnUsdcSym == "" || lb == nil || lb.Bid <= 0 || lb.Ask <= lb.Bid || nowMs-lb.UpdatedMs > 2500 {
			continue
		}
		bc := books[bookKey(VenueBnUsdc, spec.BnUsdcSym)]
		if bc == nil || bc.Bid <= 0 || bc.Ask <= bc.Bid || nowMs-bc.UpdatedMs > 2000 {
			continue
		}
		eqBasis := dnBasisEMA["DN:"+sym]
		ltMid := 0.5 * (lb.Bid + lb.Ask)

		ltPos := s8Acct.Positions[posKey(VenueLtStd, sym)]
		bnPos := s8Acct.Positions[posKey(VenueBnUsdc, spec.BnUsdcSym)]
		if ltPos != nil && math.Abs(ltPos.Qty) > 1e-8 {
			for _, mo := range makerOrders {
				if mo.StratID == "S8_LT_STD_ZERO_FEE_DN" && mo.Venue == VenueLtStd && mo.Symbol == sym && mo.CancelReqMs == 0 {
					requestCancelMakerLocked(mo.ID, nowMs)
				}
			}
			// Guarantee BN hedge leg exists!
			if bnPos == nil || math.Abs(bnPos.Qty) <= 1e-8 {
				if nowMs-ltPos.OpenedMs > 800 {
					hSide := "BUY"
					px := bc.Ask
					if ltPos.Qty > 0 {
						hSide = "SELL"
						px = bc.Bid
					}
					hQty := roundTo(math.Abs(ltPos.Qty), spec.BnQtyDec)
					if hQty > 0 {
						placeTakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, hSide, hQty, px, 20.0, false, "S8_BN_HEDGE", nowMs)
					}
				}
				continue
			}

			qty := math.Abs(bnPos.Qty)
			if bnPos.Qty > 0 {
				exitGrossBp := ((bc.Ask - bnPos.AvgPx) / bnPos.AvgPx * 10000.0) + ((ltPos.AvgPx - lb.Ask) / ltPos.AvgPx * 10000.0)
				if exitGrossBp >= 6.8 || (nowMs-ltPos.OpenedMs > 900000 && exitGrossBp >= 4.2) {
					cancelStaleOrRefreshMakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, "SELL", bc.Ask, qty, 1.5, true, "S8_BN_MAKER_EXIT:"+sym, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, nowMs)
				}
			} else {
				exitGrossBp := ((bnPos.AvgPx - bc.Bid) / bnPos.AvgPx * 10000.0) + ((lb.Bid - ltPos.AvgPx) / ltPos.AvgPx * 10000.0)
				if exitGrossBp >= 6.8 || (nowMs-ltPos.OpenedMs > 900000 && exitGrossBp >= 4.2) {
					cancelStaleOrRefreshMakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, "BUY", bc.Bid, qty, 1.5, true, "S8_BN_MAKER_EXIT:"+sym, 15000, nowMs)
				} else {
					cancelAllMakersForSymLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, nowMs)
				}
			}
			continue
		}

		if bnPos != nil && math.Abs(bnPos.Qty) > 1e-8 && nowMs-bnPos.OpenedMs > 1500 {
			side := "SELL"
			px := bc.Bid
			if bnPos.Qty < 0 {
				side = "BUY"
				px = bc.Ask
			}
			placeTakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueBnUsdc, spec.BnUsdcSym, side, math.Abs(bnPos.Qty), px, 20.0, true, "S8_ORPHAN_CLEAN", nowMs)
			continue
		}

		commonDec := spec.SizeDecimals
		if spec.BnQtyDec < commonDec {
			commonDec = spec.BnQtyDec
		}
		qty := roundTo(560.0/ltMid, commonDec)
		if qty < spec.MinBase || qty*ltMid < 20.0 {
			continue
		}
		shortDisloc := (((lb.Ask - bc.Ask) / bc.Ask) * 10000.0) - eqBasis
		longDisloc := (((bc.Bid - lb.Bid) / bc.Bid) * 10000.0) + eqBasis
		if shortDisloc >= 5.5 {
			cancelStaleOrRefreshMakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueLtStd, sym, "SELL", lb.Ask, qty, 2.5, false, "S8_ENTRY_SHORT_LT", 45000, nowMs)
		} else if longDisloc >= 5.5 {
			cancelStaleOrRefreshMakerLocked("S8_LT_STD_ZERO_FEE_DN", VenueLtStd, sym, "BUY", lb.Bid, qty, 2.5, false, "S8_ENTRY_LONG_LT", 45000, nowMs)
		} else {
			cancelAllMakersForSymLocked("S8_LT_STD_ZERO_FEE_DN", VenueLtStd, sym, nowMs)
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
			bKey := p.Venue
			if p.Venue == VenueLtStd || p.Venue == VenueLtPrem {
				bKey = "LT"
			}
			b := books[bookKey(bKey, p.Symbol)]
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

		if len(s.EquityCurve) == 0 || nowMs-s.EquityCurve[len(s.EquityCurve)-1].TsMs >= 15000 {
			s.EquityCurve = append(s.EquityCurve, EquityPoint{TsMs: nowMs, Equity: s.Equity})
			if len(s.EquityCurve) > 240 {
				s.EquityCurve = append([]EquityPoint(nil), s.EquityCurve[len(s.EquityCurve)-240:]...)
			}
		}

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
	mu.Unlock()
	if err == nil {
		tmp := ArenaOutPath + ".tmp"
		if os.WriteFile(tmp, raw, 0644) == nil {
			_ = os.Rename(tmp, ArenaOutPath)
		}
	}
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
	fmt.Printf("[%s] Loaded %d Lighter markets into Tokyo Forward Arena\n", ArenaVer, len(ltSpecs))
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

func main() {
	for _, k := range []string{"ltBook", "ltTrade", "bnBook", "bnTrade", "bnLiq", "hlMids", "polyBook"} {
		feedCounters[k] = &atomic.Int64{}
	}
	initStrategies()
	for attempt := 0; attempt < 10; attempt++ {
		if err := loadMarketSpecs(); err == nil && len(ltSpecs) > 0 {
			break
		}
		time.Sleep(2 * time.Second)
	}

	go runFundingPoller()
	go runLighterWS()
	go runBinanceBookAndLiqWS()
	go runBinanceAggTradeWS()
	go runHyperliquidWS()
	go runPolymarketPoller()

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
