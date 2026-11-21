package main

import (
	"bytes"
	"compress/gzip"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elliottech/lighter-go/client"
	lthttp "github.com/elliottech/lighter-go/client/http"
	"github.com/elliottech/lighter-go/types"
	"github.com/gorilla/websocket"
)

const (
	BaseURL      = "https://mainnet.zklighter.elliot.ai"
	WsURL        = "wss://mainnet.zklighter.elliot.ai/stream"
	AcctIdx      = int64(770015)
	KeyIdx       = uint8(4)
	ChainID      = uint32(304)
	PrivKey      = "56491130b44362b137b0a3b46dc63d647865876f11ff1613c857f946ec8612f6e4f216d065f4695a"
	ClipUSD      = 55.00
	EdgeBps      = 7.5  // Unused legacy constant
	CancelBps    = 2.5
	ObiLimit     = 0.35
	StopEqUSD    = 83.50
)

type Market struct {
	Sym        string
	ID         int64
	SzDec      int
	PxDec      int
	MinB       float64
	AllowEntry bool
	ClipUSD    float64
	EdgeBps    float64
}

var markets = map[string]Market{
	"ETH":   {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020, AllowEntry: true, ClipUSD: -1.0, EdgeBps: 5.0},
	"BTC":   {Sym: "BTC", ID: 1, SzDec: 5, PxDec: 1, MinB: 0.00007, AllowEntry: true, ClipUSD: -1.0, EdgeBps: 5.0},
	"SOL":   {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055, AllowEntry: true, ClipUSD: -1.0, EdgeBps: 7.0},
	"XRP":   {Sym: "XRP", ID: 7, SzDec: 0, PxDec: 6, MinB: 4.0, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.4},
	"HYPE":  {Sym: "HYPE", ID: 24, SzDec: 2, PxDec: 4, MinB: 0.07, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 6.5},
	"BNB":   {Sym: "BNB", ID: 25, SzDec: 2, PxDec: 4, MinB: 0.01, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.4},
	"PAXG":  {Sym: "PAXG", ID: 48, SzDec: 4, PxDec: 2, MinB: 0.0015, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 7.5},
	"ZEC":   {Sym: "ZEC", ID: 90, SzDec: 3, PxDec: 3, MinB: 0.004, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 7.5},
	"XAU":   {Sym: "XAU", ID: 92, SzDec: 4, PxDec: 2, MinB: 0.0015, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 7.5},
	"NVDA":  {Sym: "NVDA", ID: 110, SzDec: 3, PxDec: 3, MinB: 0.025, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.0},
	"AMZN":  {Sym: "AMZN", ID: 114, SzDec: 3, PxDec: 3, MinB: 0.0200, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 4.0},
	"GOOGL": {Sym: "GOOGL", ID: 116, SzDec: 4, PxDec: 2, MinB: 0.0150, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 4.0},
	"META":  {Sym: "META", ID: 117, SzDec: 4, PxDec: 2, MinB: 0.0080, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 4.2},
	"SPY":   {Sym: "SPY", ID: 128, SzDec: 4, PxDec: 2, MinB: 0.0070, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 5.0},
	"QQQ":   {Sym: "QQQ", ID: 129, SzDec: 4, PxDec: 2, MinB: 0.0075, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 4.0},
	"MU":    {Sym: "MU", ID: 164, SzDec: 4, PxDec: 2, MinB: 0.0060, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 6.5},
	"US500": {Sym: "US500", ID: 180, SzDec: 5, PxDec: 1, MinB: 0.00070, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 5.0},
	"US100": {Sym: "US100", ID: 181, SzDec: 6, PxDec: 0, MinB: 0.00020, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 4.0},
	"SOXL":  {Sym: "SOXL", ID: 197, SzDec: 4, PxDec: 2, MinB: 0.0500, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 9.5},
}
var evergreenSyms = []string{}
var bootTS = time.Now()
var v87BootMS = bootTS.UnixMilli()
var idToSym = map[int64]string{0: "ETH", 1: "BTC", 2: "SOL", 7: "XRP", 24: "HYPE", 25: "BNB", 48: "PAXG", 90: "ZEC", 92: "XAU", 110: "NVDA", 114: "AMZN", 116: "GOOGL", 117: "META", 128: "SPY", 129: "QQQ", 164: "MU", 180: "US500", 181: "US100", 197: "SOXL"}

func isUSRthUTC(t time.Time) bool {
	u := t.UTC()
	wd := u.Weekday()
	if wd < time.Monday || wd > time.Friday {
		return false
	}
	mins := u.Hour()*60 + u.Minute()
	return mins >= (13*60+25) && mins < 21*60
}

func isOffHoursUTC(t time.Time) bool {
	u := t.UTC()
	wd := u.Weekday()
	if wd == time.Saturday {
		return true
	}
	mins := u.Hour()*60 + u.Minute()
	if wd == time.Sunday {
		return mins < 22*60
	}
	return mins < (13*60+25) || mins >= (21*60+5)
}

func isGoldActiveUTC(t time.Time) bool {
	u := t.UTC()
	wd := u.Weekday()
	mins := u.Hour()*60 + u.Minute()
	if wd == time.Saturday {
		return false
	}
	if wd == time.Sunday {
		return mins >= 22*60+5
	}
	if wd == time.Friday {
		return mins < 21*60
	}
	return true
}

func isTradFiSym(sym string) bool {
	switch sym {
	case "NVDA", "AMZN", "GOOGL", "META", "MU", "SOXL", "SPY", "QQQ", "US500", "US100",
		"SPCX", "AAPL", "TSLA", "MSFT", "INTC", "AMD", "COIN", "HOOD", "MSTR", "PLTR",
		"CRCL", "NBIS", "SNDK", "IREN", "OURA", "STONK", "ASELS", "TUR30",
		"DRAM", "SKHY", "SMSN", "TSM", "ASML", "ARM", "AVGO", "NFLX", "ORCL", "UBER",
		"DIS", "BA", "BABA", "NIO", "GOLD", "SILVER", "OIL", "WTI", "BRENT",
		"XAU", "XAG", "PAXG", "USDJPY", "USDCHF", "USDCAD", "EURUSD", "GBPUSD":
		return true
	}
	return false
}

func isSymTradeable24hOrOpen(sym string, now time.Time) bool {
	if !isTradFiSym(sym) {
		return true
	}
	if sym == "XAU" || sym == "XAG" || sym == "PAXG" {
		return isGoldActiveUTC(now)
	}
	return isUSRthUTC(now)
}

type ActiveOrder struct {
	COI     int64
	Px      float64
	BaseInt int64
	RefMid  float64
	IsClose bool
	IsIOC   bool
	TS      time.Time
}

type RangeBucket struct {
	slot int64
	hi   float64
	lo   float64
}

type MarketState struct {
	mu          sync.Mutex
	bnMid       float64
	bnBid       float64
	bnAsk       float64
	bnMicro     float64
	bnObi       float64
	fastObi     float64
	rawObi      float64
	emaUltra    float64
	emaFast     float64
	emaEq       float64
	emaSlow     float64
	ema5m       float64
	emaMacro    float64
	lastEmaTS     time.Time
	basisBps       float64
	lastBasisTS    time.Time
	basisWarmTicks int
	emaSpreadBps   float64
	rangeBuckets  [6]RangeBucket
	range60sBps   float64
	minSlot       [128]int64
	minPx         [128]float64
	minHi         [128]float64
	minLo         [128]float64
	minCl         [128]float64
	minVol        [128]float64
	lastClosedMin int64
	lastSigMin    int64
	ltBidDepthUSD float64
	ltAskDepthUSD float64
	shDir         int
	shEntry       float64
	shOpenTS      time.Time
	shadowBps     float64
	lastShadowTS  time.Time
	dropCoolUntil time.Time
	pumpCoolUntil time.Time
	bookBids    map[string]float64
	bookAsks    map[string]float64
	ltBid       float64
	ltBidSz     float64
	ltAsk       float64
	ltAskSz     float64
	lastBookTS  time.Time
	lastRecOBMS int64
	pos         float64
	avgEntry    float64
	posOpenTS     time.Time
	lastFillTS    time.Time
	stopCoolUntil time.Time
	trendMode     bool
	entryWasSnipe bool
	lastEntryDir  int
	makerActive   bool
	peakFavBps    float64
	dynClipUSD    float64
	dynEdgeBps      float64
	entryCool       time.Duration
	rollPnl         float64
	activeOrds      map[string]*ActiveOrder
	lastOrdTS       map[string]time.Time
	lastCancelTS    map[string]time.Time
	netMicroUSD     atomic.Int64
	pendBidMicroUSD atomic.Int64
	pendAskMicroUSD atomic.Int64
	rollPnlMicro    atomic.Int64
}

func newMarketState(initClip float64) *MarketState {
	return &MarketState{
		bookBids:     map[string]float64{},
		bookAsks:     map[string]float64{},
		activeOrds:   map[string]*ActiveOrder{},
		lastOrdTS:    map[string]time.Time{},
		lastCancelTS: map[string]time.Time{},
		entryCool:    2 * time.Second,
		dynClipUSD:   initClip,
	}
}

type TwinState struct {
	Name           string
	ThinSym        string
	DeepSym        string
	AlwaysActive   bool
	MinGapBps      float64
	MaxCapUSD      float64
	WfLB           int
	WfZ            float64
	HourlyMean     float64
	HourlyStd      float64
	FundDiffBps    float64
	WfReady        bool
	LastWfSync     time.Time
	BidEdgeBps     float64
	AskEdgeBps     float64
	FallbackRatio  float64
	EmaRatio       float64
	LastEmaTS      time.Time
	WarmTicks        int
	EntryTS          time.Time
	EntryEqUSD       float64
	UnhedgedSince    time.Time
	AccruedFundBps   float64
	LastFundAccrueTS time.Time
	WasHedged        bool
	LastDualSendTS   time.Time
	LastHedgeTS      time.Time
	CoolUntil        time.Time
	LastLog          time.Time
	TradeID          int64
	LegUSD           float64
}

type CommonAsset struct {
	Sym      string
	BnSym    string
	MarketID int64
	SzDec    int
	PxDec    int
	MinB     float64
	LtVol24h float64
	BnVol24h float64
}

type CarbonOrder struct {
	ID         int64   `json:"id"`
	Sym        string  `json:"sym"`
	Side       string  `json:"side"`
	USD        float64 `json:"usd"`
	SlBps      float64 `json:"slBps"`
	TpBps      float64 `json:"tpBps"`
	MaxHoldSec int64   `json:"maxHoldSec"`
	Strat      string  `json:"strat"`
}

type CarbonTune struct {
	ID        int64   `json:"id"`
	RiskUSD   float64 `json:"riskUsd"`
	MinScore  int     `json:"minScore"`
	MinLagBps float64 `json:"minLagBps"`
	BeAtR     float64 `json:"beAtR"`
	AllowLag  bool    `json:"allowLag"`
	AllowMom  bool    `json:"allowMom"`
	Note      string  `json:"note"`
}

type CarbonPos struct {
	ID         int64
	Sym        string
	Side       string
	USD        float64
	SlBps      float64
	TpBps      float64
	MaxHoldSec int64
	SentAt     time.Time
	EntryPx    float64
	TpSent     bool
	BeArmed    bool
	Closing     bool
	StratTag    string
	CloseReason string
	LastPnLBps  float64
	PeakFavBps  float64
	LastCloseAt time.Time
}

type StratStat struct {
	Wins   int
	Losses int
	PnlUSD float64
}

type ActiveOpenPos struct {
	ID          int64
	Mode        string // "BN_LAG_ARB" or "TRI_EQUALIZE"
	SymA        string // Primary symbol (or LONG leg for TRI_EQUALIZE)
	SideA       string // "LONG" / "SHORT"
	SymB        string // SHORT leg for TRI_EQUALIZE
	LegUSD      float64
	EdgeBps     float64
	Mult        float64
	TpBps       float64
	SlBps       float64
	SentAt      time.Time
	EntryPxA    float64
	EntryPxB    float64
	TpSent      bool
	BeArmed     bool
	Closing     bool
	CloseReason string
	CloseBps    float64
	CloseUSD    float64
	CoolSec     int64
	LastPnlBps  float64
	LastPnlUSD  float64
	LastCloseAt time.Time
}

var (
	mktStates = map[string]*MarketState{
		"BTC": newMarketState(-1.0), "ETH": newMarketState(-1.0),
		"SOL": newMarketState(-1.0), "XRP": newMarketState(-1.0), "HYPE": newMarketState(-1.0),
		"BNB": newMarketState(-1.0), "PAXG": newMarketState(240.0), "ZEC": newMarketState(-1.0), "XAU": newMarketState(240.0), "NVDA": newMarketState(-1.0),
		"AMZN": newMarketState(-1.0), "GOOGL": newMarketState(-1.0), "META": newMarketState(-1.0),
		"SPY": newMarketState(240.0), "US500": newMarketState(240.0), "QQQ": newMarketState(240.0), "US100": newMarketState(240.0),
		"MU": newMarketState(-1.0), "SOXL": newMarketState(-1.0),
	}
	twins = []*TwinState{
		{Name: "PAXG/XAU", ThinSym: "PAXG", DeepSym: "XAU", AlwaysActive: false, MinGapBps: 7.5, MaxCapUSD: 1400.0, WfLB: 48, WfZ: 2.2, BidEdgeBps: 7.5, AskEdgeBps: 7.5, FallbackRatio: 1.0000},
		{Name: "US500/SPY", ThinSym: "US500", DeepSym: "SPY", AlwaysActive: false, MinGapBps: 5.0, MaxCapUSD: 4000.0, WfLB: 48, WfZ: 2.2, BidEdgeBps: 5.0, AskEdgeBps: 5.0, FallbackRatio: 10.002},
		{Name: "US100/QQQ", ThinSym: "US100", DeepSym: "QQQ", AlwaysActive: false, MinGapBps: 5.0, MaxCapUSD: 4000.0, WfLB: 48, WfZ: 2.2, BidEdgeBps: 5.0, AskEdgeBps: 5.0, FallbackRatio: 41.15},
	}
	pairMu                  sync.Mutex
	pairMarketActive        atomic.Bool
	probationForce          atomic.Bool
	remotePaused            atomic.Bool
	breakerTripped          atomic.Bool
	l2TicksRecorded         atomic.Int64
	l2RecCh                 = make(chan string, 4096)
	enableAutoTwins         = false
	commonList              []*CommonAsset
	carbonMu                sync.Mutex
	activeCarbon            *CarbonPos
	lastCarbonOrderID       int64
	carbonScanSeq           atomic.Int64
	lastStopMS              atomic.Int64
	sessionRealizedMicroUSD atomic.Int64
	sessionNetWins          atomic.Int64
	openMu                  sync.Mutex
	activeOpen              *ActiveOpenPos
	stratCoolUntilMS        atomic.Int64
	awaitingReview          atomic.Bool
	lastCarbonTuneID        int64
	tuneMu                  sync.Mutex
	curTune                 = CarbonTune{RiskUSD: 0.08, MinScore: 7, MinLagBps: 3.0, BeAtR: 0.45, AllowLag: false, AllowMom: false, Note: "v93_zero_fee_dn_arb"}
	fastArbCh               = make(chan string, 64)
	bnEthBtcMu              sync.Mutex
	bnEthBtcBid             float64
	bnEthBtcAsk             float64
	bnEthBtcTS              time.Time
	journalMu               sync.Mutex
	stratMu                 sync.Mutex
	stratStats              = map[string]*StratStat{
		"BN_LAG_ARB":    {},
		"TRI_EQUALIZE":  {},
		"TWIN_EQUALIZE": {},
		"BN_MOM_SNIPE":  {},
		"ZERO_FEE_DN":   {},
	}
	snapMu      sync.Mutex
	snapLagDesc = "none"
	snapLagEdge float64
	snapTriDesc = "none"
	snapTriEdge float64
	snapMomDesc = "none"
	snapMomSc   int
	streakWins           atomic.Int64
	globalEntryLockUntil atomic.Int64
	rateMu    sync.Mutex
	writeHist []time.Time
	eqNow     atomic.Int64
	logMu     sync.Mutex
	lastLog   = time.Time{}

	txCli    *client.TxClient
	nonceSeq atomic.Int64
	coiSeq   atomic.Int64
	txMu     sync.Mutex

	wsMu       sync.Mutex
	ltConn     *websocket.Conn
	subbedMids = map[int64]bool{}
	txWsMu     sync.Mutex
	txConn      *websocket.Conn
	cancelQueue = make(chan func(), 256)
	txQueue     = make(chan func(), 256)
	useWsSend   = true
)

func setConnNoDelay(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	u := conn.UnderlyingConn()
	if tc, ok := u.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
	} else if tlc, ok := u.(*tls.Conn); ok {
		if tc, ok2 := tlc.NetConn().(*net.TCPConn); ok2 {
			_ = tc.SetNoDelay(true)
			_ = tc.SetKeepAlive(true)
		}
	}
}

func runTxWorker() {
	runtime.LockOSThread()
	for {
		select {
		case fn := <-cancelQueue:
			fn()
		default:
			select {
			case fn := <-cancelQueue:
				fn()
			case fn := <-txQueue:
				fn()
			}
		}
	}
}

func enqueueCancel(fn func()) {
	select {
	case cancelQueue <- fn:
	default:
		go fn()
	}
}

func enqueueTx(fn func()) {
	select {
	case txQueue <- fn:
	default:
		go fn()
	}
}

var quotaCoolUntil atomic.Int64

func globalDirCapUSD() float64 {
	return 1800.0
}

func houseMoneyClipUSD() float64 {
	eq := float64(eqNow.Load()) / 1e6
	if eq >= 96.00 {
		return 320.0
	}
	if eq >= 95.00 {
		return 240.0
	}
	return 160.0
}

func takeWriteSlotTier(tier int) bool {
	now := time.Now()
	if tier == 0 && now.UnixMilli() < quotaCoolUntil.Load() {
		return false
	}
	rateMu.Lock()
	defer rateMu.Unlock()
	cut := now.Add(-60 * time.Second)
	idx := 0
	for idx < len(writeHist) && writeHist[idx].Before(cut) {
		idx++
	}
	if idx > 0 {
		writeHist = writeHist[idx:]
	}
	limit := 30 // Tier 0: entries (strictly below Lighter's 40 req/min L1Address cap!)
	if tier == 1 {
		limit = 35 // Tier 1: TP closes & modifies
	} else if tier >= 2 {
		limit = 39 // Tier 2: Emergency SL & Hedge Bailouts
	}
	if len(writeHist) >= limit {
		return false
	}
	writeHist = append(writeHist, now)
	return true
}

func takeWriteSlot(isPriority bool) bool {
	if isPriority {
		return takeWriteSlotTier(1)
	}
	return takeWriteSlotTier(0)
}

func anyOtherOpenPos(selfSym string) bool {
	for sym, st := range mktStates {
		if sym == selfSym {
			continue
		}
		if math.Abs(float64(st.netMicroUSD.Load()))/1e6 >= 5.0 {
			return true
		}
	}
	return false
}

func otherSameDirPendingUSD(selfSym string, checkLong bool) float64 {
	var sum int64
	for sym, st := range mktStates {
		if sym == selfSym {
			continue
		}
		if checkLong {
			sum += st.pendBidMicroUSD.Load()
		} else {
			sum += st.pendAskMicroUSD.Load()
		}
	}
	return float64(sum) / 1e6
}

func cancelAllOtherEntryQuotes(filledSym string) {
	for sym, st := range mktStates {
		if sym == filledSym {
			continue
		}
		if st.pendBidMicroUSD.Load() > 0 || st.pendAskMicroUSD.Load() > 0 {
			go func(s string) {
				oSt := mktStates[s]
				oSt.mu.Lock()
				for _, sd := range []string{"BID", "ASK"} {
					if ao := oSt.activeOrds[sd]; ao != nil && !ao.IsClose {
						cancelSide(s, sd) // L2CancelOrder is 100% quota-free & 0.7ms fast!
					}
				}
				oSt.mu.Unlock()
			}(sym)
		}
	}
}

func preemptWeakerQuote(leaderSym, side string) {
	mySt := mktStates[leaderSym]
	if mySt == nil {
		return
	}
	myPnl := mySt.rollPnlMicro.Load()
	checkLong := side == "BID"
	// Only preempt weaker same-side quote if combined pending would exceed globalDirCapUSD()!
	totPend := float64(mySt.pendBidMicroUSD.Load())/1e6 + otherSameDirPendingUSD(leaderSym, true)
	if !checkLong {
		totPend = float64(mySt.pendAskMicroUSD.Load())/1e6 + otherSameDirPendingUSD(leaderSym, false)
	}
	if totPend <= globalDirCapUSD()*1.02 {
		return
	}
	for sym, st := range mktStates {
		if sym == leaderSym {
			continue
		}
		otherPnl := st.rollPnlMicro.Load()
		isWeaker := otherPnl < myPnl || (otherPnl == myPnl && leaderSym < sym)
		if !isWeaker {
			continue
		}
		pend := st.pendBidMicroUSD.Load()
		if side == "ASK" {
			pend = st.pendAskMicroUSD.Load()
		}
		if pend > 0 {
			go func(s string) {
				oSt := mktStates[s]
				oSt.mu.Lock()
				if ao := oSt.activeOrds[side]; ao != nil && !ao.IsClose {
					cancelSide(s, side) // Quota-free L2CancelOrder!
				}
				oSt.mu.Unlock()
			}(sym)
		}
	}
}

func totalNetUSD() float64 {
	var sum int64
	for _, st := range mktStates {
		sum += st.netMicroUSD.Load()
	}
	return float64(sum) / 1e6
}

func roundTo(v float64, dec int) float64 {
	p := math.Pow10(dec)
	return math.Round(v*p) / p
}

func nextOpts() *types.TransactOpts {
	n := nonceSeq.Add(1) - 1
	exp := time.Now().Add(10 * time.Minute).UnixMilli()
	return &types.TransactOpts{Nonce: &n, ExpiredAt: exp}
}

var txHTTP = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
		MaxIdleConnsPerHost: 50,
		IdleConnTimeout:     90 * time.Second,
	},
}

func sendSignedTx(id string, dbg string, build func(*types.TransactOpts) (uint8, string, error)) error {
	txMu.Lock()
	defer txMu.Unlock()
	t0 := time.Now()
	txType, txInfo, err := build(nextOpts())
	if err != nil {
		return err
	}
	if useWsSend {
		txWsMu.Lock()
		tc := txConn
		var wsErr error
		if tc != nil {
			wsErr = tc.WriteJSON(map[string]any{
				"type": "jsonapi/sendtx",
				"data": map[string]any{
					"id":      id,
					"tx_type": txType,
					"tx_info": json.RawMessage(txInfo),
				},
			})
		}
		txWsMu.Unlock()
		if tc != nil && wsErr == nil {
			fmt.Printf("[WS_TX %s %s] %dus\n", id, dbg, time.Since(t0).Microseconds())
			return nil
		}
		wsMu.Lock()
		c := ltConn
		if c != nil {
			wsErr = c.WriteJSON(map[string]any{
				"type": "jsonapi/sendtx",
				"data": map[string]any{
					"id":      id,
					"tx_type": txType,
					"tx_info": json.RawMessage(txInfo),
				},
			})
		}
		wsMu.Unlock()
		if c != nil && wsErr == nil {
			fmt.Printf("[WS_TX %s %s] %dus\n", id, dbg, time.Since(t0).Microseconds())
			return nil
		}
	}
	form := url.Values{}
	form.Set("tx_type", strconv.Itoa(int(txType)))
	form.Set("tx_info", txInfo)
	resp, err := txHTTP.PostForm(BaseURL+"/api/v1/sendTx", form)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	ms := time.Since(t0).Milliseconds()
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"code":200`) {
		fmt.Printf("[SENDTX %s %s %dms] status=%d body=%s\n", id, dbg, ms, resp.StatusCode, string(body))
		if resp.StatusCode == 429 {
			quotaCoolUntil.Store(time.Now().Add(5 * time.Second).UnixMilli())
		}
		if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
			nonceSeq.Store(n)
		}
		return fmt.Errorf("sendTx failed")
	}
	fmt.Printf("[TX_OK %s %s] %dms\n", id, dbg, ms)
	return nil
}

func cancelAll() {
	_ = sendSignedTx("ca", "cancelAll", func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCancelAllOrdersTransaction(&types.CancelAllOrdersTxReq{TimeInForce: 0, Time: 0}, opts)
		if e != nil {
			return 0, "", e
		}
		s, e := ti.GetTxInfo()
		return ti.GetTxType(), s, e
	})
	for _, st := range mktStates {
		st.mu.Lock()
		st.activeOrds = map[string]*ActiveOrder{}
		st.pendBidMicroUSD.Store(0)
		st.pendAskMicroUSD.Store(0)
		st.mu.Unlock()
	}
}

func cancelSide(sym, side string) {
	st := mktStates[sym]
	ao, ok := st.activeOrds[side]
	if !ok {
		return
	}
	now := time.Now()
	st.lastCancelTS[side] = now
	if !ao.IsClose {
		st.lastOrdTS[side] = now // Block immediate re-entry until Lighter ZK position state confirms!
	}
	delete(st.activeOrds, side)
	if side == "BID" {
		st.pendBidMicroUSD.Store(0)
	} else {
		st.pendAskMicroUSD.Store(0)
	}
	m := markets[sym]
	coi, mid := ao.COI, m.ID
	enqueueCancel(func() {
		_ = sendSignedTx(fmt.Sprintf("c_%d", coi), sym+"_"+side, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: mid, Index: coi}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
	})
}

func cancelDistantForSnipe(sym, side string) bool {
	st := mktStates[sym]
	ao, ok := st.activeOrds[side]
	if !ok || ao == nil {
		return true
	}
	if ao.IsClose || ao.IsIOC {
		return false
	}
	isSafeDist := (side == "BID" && st.ltBid >= ao.Px*(1.0+1.0/10000.0)) || (side == "ASK" && st.ltAsk <= ao.Px*(1.0-1.0/10000.0))
	if !isSafeDist {
		return false
	}
	delete(st.activeOrds, side)
	if side == "BID" {
		st.pendBidMicroUSD.Store(0)
	} else {
		st.pendAskMicroUSD.Store(0)
	}
	m := markets[sym]
	coi, mid := ao.COI, m.ID
	enqueueCancel(func() {
		_ = sendSignedTx(fmt.Sprintf("cs_%d", coi), sym+"_"+side, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: mid, Index: coi}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
	})
	return true
}

func modifySide(sym, side string, newPxF, midF float64) {
	st := mktStates[sym]
	ao, ok := st.activeOrds[side]
	if !ok {
		return
	}
	m := markets[sym]
	pxInt := uint32(math.Round(newPxF * math.Pow10(m.PxDec)))
	ao.Px = newPxF
	ao.RefMid = midF
	ao.TS = time.Now()
	coi, mid, baseInt := ao.COI, m.ID, ao.BaseInt
	enqueueTx(func() {
		err := sendSignedTx(fmt.Sprintf("m_%d", coi), fmt.Sprintf("%s_%s px=%.4f", sym, side, newPxF), func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetModifyOrderTransaction(&types.ModifyOrderTxReq{
				MarketIndex: mid,
				Index:       coi,
				BaseAmount:  baseInt,
				Price:       pxInt,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
		if err != nil {
			st.mu.Lock()
			delete(st.activeOrds, side)
	if side == "BID" {
		st.pendBidMicroUSD.Store(0)
	} else {
		st.pendAskMicroUSD.Store(0)
	}
			st.mu.Unlock()
		}
	})
}

func placeSide(sym, side string, pxF, midF, customSz float64, isClose, isIOC bool) {
	if !isClose && remotePaused.Load() {
		return
	}
	st := mktStates[sym]
	m := markets[sym]
	baseF := customSz
	if baseF <= 0 {
		clip := st.dynClipUSD
		if clip <= 0 {
			clip = 95.0
		}
		if !isClose && isIOC {
			twoSidedUSD := math.Min(st.ltBidDepthUSD, st.ltAskDepthUSD)
			if 0.35*twoSidedUSD < 100.0 {
				return
			}
			clip = math.Min(clip, 0.35*twoSidedUSD)
		}
		if !isClose {
			// Enforce strict single-trade global directional cap across Quote-Two same-side orders!
			otherPend := otherSameDirPendingUSD(sym, side == "BID")
			rem := globalDirCapUSD() - otherPend
			if rem < 11.5 {
				return
			}
			if clip > rem {
				clip = rem
			}
		}
		if !isClose {
			if time.Now().UnixMilli() < globalEntryLockUntil.Load() && isIOC {
				return
			}
			st.entryWasSnipe = isIOC
			if side == "BID" {
				st.lastEntryDir = 1
			} else {
				st.lastEntryDir = -1
			}
			if isIOC {
				globalEntryLockUntil.Store(time.Now().Add(3500 * time.Millisecond).UnixMilli())
			}
		}
		baseF = math.Max(m.MinB, clip/pxF)
	}
	baseInt := int64(math.Round(baseF * math.Pow10(m.SzDec)))
	if !isClose {
		baseInt = int64(math.Ceil(baseF * math.Pow10(m.SzDec)))
	}
	if baseInt <= 0 {
		return
	}
	pxInt := uint32(math.Round(pxF * math.Pow10(m.PxDec)))
	coi := coiSeq.Add(1)
	var isAsk, redOnly uint8
	if side == "ASK" {
		isAsk = 1
	}
	if isClose {
		redOnly = 1
	}
	st.activeOrds[side] = &ActiveOrder{COI: coi, Px: pxF, BaseInt: baseInt, RefMid: midF, IsClose: isClose, IsIOC: isIOC, TS: time.Now()}
	if !isClose && !isIOC {
		ordMicro := int64(baseF * pxF * 1e6)
		if side == "BID" {
			st.pendBidMicroUSD.Store(ordMicro)
		} else {
			st.pendAskMicroUSD.Store(ordMicro)
		}
		preemptWeakerQuote(sym, side)
	} else if !isClose && isIOC {
		preemptWeakerQuote(sym, side)
	}
	enqueueTx(func() {
		var ordType uint8 = 0
		var tif uint8 = 2
		if isClose {
			tif = 1 // GoodTillTime on closes: crosses immediately at 0% fee if target already reached!
		}
		exp := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
		if isIOC {
			ordType = 1 // MARKET
			tif = 0     // IOC
			exp = 0
		}
		dbg := fmt.Sprintf("%s_%s px=%.4f ro=%d ioc=%v", sym, side, pxF, redOnly, isIOC)
		err := sendSignedTx(fmt.Sprintf("o_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      m.ID,
				ClientOrderIndex: coi,
				BaseAmount:       baseInt,
				Price:            pxInt,
				IsAsk:            isAsk,
				Type:             ordType,
				TimeInForce:      tif,
				ReduceOnly:       redOnly,
				OrderExpiry:      exp,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
		if err != nil {
			st.mu.Lock()
			delete(st.activeOrds, side)
	if side == "BID" {
		st.pendBidMicroUSD.Store(0)
	} else {
		st.pendAskMicroUSD.Store(0)
	}
			st.mu.Unlock()
		}
	})
}

func recomputeBBO(st *MarketState) {
	bestB, bestBSz, bestA, bestASz := 0.0, 0.0, 0.0, 0.0
	for ps, sz := range st.bookBids {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if p > bestB {
				bestB, bestBSz = p, sz
			}
		}
	}
	for ps, sz := range st.bookAsks {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if bestA == 0 || p < bestA {
				bestA, bestASz = p, sz
			}
		}
	}
	st.ltBid, st.ltBidSz = bestB, bestBSz
	st.ltAsk, st.ltAskSz = bestA, bestASz
	bidDepth, askDepth := 0.0, 0.0
	if bestB > 0 {
		for ps, sz := range st.bookBids {
			if sz > 0 {
				p, _ := strconv.ParseFloat(ps, 64)
				if p > 0 && (bestB-p)/bestB*10000.0 <= 0.85 {
					bidDepth += p * sz
				}
			}
		}
	}
	if bestA > 0 {
		for ps, sz := range st.bookAsks {
			if sz > 0 {
				p, _ := strconv.ParseFloat(ps, 64)
				if p > 0 && (p-bestA)/bestA*10000.0 <= 0.85 {
					askDepth += p * sz
				}
			}
		}
	}
	st.ltBidDepthUSD = bidDepth
	st.ltAskDepthUSD = askDepth
}

func isDislocSym(sym string) bool {
	_ = sym
	return false
}

func dislocParams(sym string, now time.Time) (bool, float64, float64, float64, float64, float64, float64) {
	switch sym {
	case "GOOGL":
		return isOffHoursUTC(now), 4.0, 3.0, 5.2, 16.0, 1.5, 2.8
	case "AMZN":
		return isOffHoursUTC(now), 4.0, 3.0, 5.2, 16.0, 1.5, 2.8
	case "META":
		return isOffHoursUTC(now), 4.2, 3.2, 5.5, 20.0, 1.6, 2.8
	case "SOXL":
		return isOffHoursUTC(now), 9.5, 6.8, 9.0, 26.0, 2.5, 5.5
	case "MU":
		return isOffHoursUTC(now), 6.5, 5.2, 6.8, 18.0, 2.0, 3.5
	case "ZEC":
		return true, 7.5, 5.6, 7.5, 20.0, 2.2, 3.8
	case "HYPE":
		return true, 6.5, 5.0, 6.8, 16.0, 2.0, 3.5
	default:
		return false, 11.5, 6.8, 15.5, 0.0, 3.0, 1.25
	}
}

func closeStepBps(sym string, st *MarketState) float64 {
	_ = st
	if isDislocSym(sym) {
		_, _, tp, _, _, _, _ := dislocParams(sym, time.Now())
		return tp
	}
	return 80.0
}

func stepRatchetFloor(peakFavBps, lockFloor float64) float64 {
	if peakFavBps >= 8.5 {
		return math.Max(lockFloor, peakFavBps*0.65)
	}
	return lockFloor
}

func retMinBps(st *MarketState, curMin, minsAgo int64, mid, fallbackBps float64) float64 {
	target := curMin - minsAgo
	idx := int(((target % 128) + 128) % 128)
	if st.minSlot[idx] == target {
		ref := st.minCl[idx]
		if ref <= 0 {
			ref = st.minPx[idx]
		}
		if ref > 0 {
			return (mid - ref) / ref * 10000.0
		}
	}
	return fallbackBps
}

func closedTrend60mBps(sym string) (float64, bool) {
	st := mktStates[sym]
	if st == nil {
		return 0.0, false
	}
	st.mu.Lock()
	cm := st.lastClosedMin
	var clNow, cl60 float64
	ok := false
	if cm > 60 {
		idxNow := int(((cm % 128) + 128) % 128)
		idx60 := int((((cm - 60) % 128) + 128) % 128)
		if st.minSlot[idxNow] == cm && st.minSlot[idx60] == cm-60 && st.minCl[idxNow] > 0 && st.minCl[idx60] > 0 {
			clNow = st.minCl[idxNow]
			cl60 = st.minCl[idx60]
			ok = true
		}
	}
	st.mu.Unlock()
	if !ok {
		return 0.0, false
	}
	return (clNow - cl60) / cl60 * 10000.0, true
}

func v79BreakoutSignal(st *MarketState, now time.Time, mid, selfTr60, otherTr60 float64, trOk bool) (bool, bool, int64) {
	if !trOk || mid <= 0 || st.lastClosedMin <= 0 || st.lastSigMin >= st.lastClosedMin {
		return false, false, 0
	}
	cm := st.lastClosedMin
	secSinceClose := now.Unix() - (cm+1)*60
	if secSinceClose < -2 || secSinceClose > 12 {
		return false, false, 0
	}
	mi := int(((cm % 128) + 128) % 128)
	if st.minSlot[mi] != cm {
		return false, false, 0
	}
	o0, c, v := st.minPx[mi], st.minCl[mi], st.minVol[mi]
	if o0 <= 0 || c <= 0 {
		return false, false, 0
	}
	hi18, lo18 := 0.0, 0.0
	cnt18 := 0
	for m := cm - 18; m <= cm-1; m++ {
		idx := int(((m % 128) + 128) % 128)
		if st.minSlot[idx] == m && st.minHi[idx] > 0 && st.minLo[idx] > 0 {
			if cnt18 == 0 || st.minHi[idx] > hi18 {
				hi18 = st.minHi[idx]
			}
			if cnt18 == 0 || st.minLo[idx] < lo18 {
				lo18 = st.minLo[idx]
			}
			cnt18++
		}
	}
	if cnt18 < 15 {
		return false, false, 0
	}
	hi60, lo60 := 0.0, 0.0
	cnt60 := 0
	for m := cm - 60; m <= cm; m++ {
		idx := int(((m % 128) + 128) % 128)
		if st.minSlot[idx] == m && st.minHi[idx] > 0 && st.minLo[idx] > 0 {
			if cnt60 == 0 || st.minHi[idx] > hi60 {
				hi60 = st.minHi[idx]
			}
			if cnt60 == 0 || st.minLo[idx] < lo60 {
				lo60 = st.minLo[idx]
			}
			cnt60++
		}
	}
	if cnt60 < 45 || (hi60-lo60)/c*10000.0 < 40.0 {
		return false, false, 0
	}
	sumV := 0.0
	cntV := 0
	for m := cm - 20; m <= cm-1; m++ {
		idx := int(((m % 128) + 128) % 128)
		if st.minSlot[idx] == m && st.minVol[idx] > 0 {
			sumV += st.minVol[idx]
			cntV++
		}
	}
	if cntV < 15 {
		return false, false, 0
	}
	avgV := sumV / float64(cntV)
	if avgV <= 0 || v < avgV*1.80 {
		return false, false, 0
	}
	body := (c - o0) / o0 * 10000.0
	drift := (mid - c) / c * 10000.0
	buy := c > hi18 && selfTr60 >= 18.0 && otherTr60 >= 5.0 && body >= 2.0 && drift >= -3.0 && drift <= 10.0
	sell := c < lo18 && selfTr60 <= -18.0 && otherTr60 <= -5.0 && body <= -2.0 && drift <= 3.0 && drift >= -10.0
	return buy, sell, cm
}

func updateShadowPaper(st *MarketState, now time.Time, lb, la, fairLt, dEdge, dTP, dSL float64, dActive bool) {
	if st.lastShadowTS.IsZero() {
		st.lastShadowTS = now
	} else {
		sdt := math.Min(10.0, math.Max(0.0, now.Sub(st.lastShadowTS).Seconds()))
		st.lastShadowTS = now
		st.shadowBps *= math.Exp(-sdt / 1500.0) // 25m half-life across low & high volume!
	}
	if st.shDir == 0 {
		if dActive && fairLt > 0 {
			if lb <= fairLt*(1.0-dEdge/10000.0) {
				st.shDir = 1
				st.shEntry = fairLt * (1.0 - dEdge/10000.0)
				st.shOpenTS = now
			} else if la >= fairLt*(1.0+dEdge/10000.0) {
				st.shDir = -1
				st.shEntry = fairLt * (1.0 + dEdge/10000.0)
				st.shOpenTS = now
			}
		}
		return
	}
	age := now.Sub(st.shOpenTS).Seconds()
	curSL := dSL
	if age >= 180.0 {
		curSL = dSL * 0.72
	}
	if st.shDir == 1 {
		pnlBps := (lb - st.shEntry) / st.shEntry * 10000.0
		if la >= st.shEntry*(1.0+dTP/10000.0) {
			st.shadowBps += dTP
			st.shDir = 0
		} else if (age >= 35.0 && pnlBps <= -curSL) || pnlBps <= -24.0 || age >= 480.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	} else {
		pnlBps := (st.shEntry - la) / st.shEntry * 10000.0
		if lb <= st.shEntry*(1.0-dTP/10000.0) {
			st.shadowBps += dTP
			st.shDir = 0
		} else if (age >= 35.0 && pnlBps <= -curSL) || pnlBps <= -24.0 || age >= 480.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	}
}

func manageCoin(sym string) {
	if !isDislocSym(sym) {
		return
	}
	btcTr60, btcTrOk := closedTrend60mBps("BTC")
	ethTr60, ethTrOk := closedTrend60mBps("ETH")
	st := mktStates[sym]
	st.mu.Lock()
	defer st.mu.Unlock()
	mid := st.bnMid
	micro := st.bnMicro
	if micro <= 0 {
		micro = mid
	}
	obi := st.bnObi
	fObi := st.fastObi
	lb, la := st.ltBid, st.ltAsk
	if mid <= 0 || lb <= 0 || la <= 0 || lb >= la {
		return
	}
	pos := st.pos
	posUSD := pos * mid
	st.netMicroUSD.Store(int64(posUSD * 1e6))
	netUSD := totalNetUSD()
	m := markets[sym]
	tick := math.Pow10(-m.PxDec)
	now := time.Now()

	ef, es, eu, em, eeq, e5m := st.emaFast, st.emaSlow, st.emaUltra, st.emaMacro, st.emaEq, st.ema5m
	trendBps, impBps, vel3sBps, mom5mBps, macroTrendBps := 0.0, 0.0, 0.0, 0.0, 0.0
	if ef > 0 && es > 0 {
		trendBps = (ef - es) / es * 10000.0
		impBps = (mid - ef) / ef * 10000.0
	}
	if e5m > 0 {
		mom5mBps = (mid - e5m) / e5m * 10000.0
	}
	if em > 0 {
		macroTrendBps = (mid - em) / em * 10000.0
	}
	curMin := now.Unix() / 60
	ret3mBps := retMinBps(st, curMin, 3, mid, trendBps)
	ret5mBps := retMinBps(st, curMin, 5, mid, mom5mBps)
	ret15mBps := retMinBps(st, curMin, 15, mid, macroTrendBps)
	if eu > 0 {
		vel3sBps = (mid - eu) / eu * 10000.0
		coolVel := 5.5
		if sym == "SOL" || sym == "XRP" || sym == "BNB" || sym == "ZEC" {
			coolVel = 7.0
		} else if sym == "NVDA" {
			coolVel = 6.5
		} else if sym == "HYPE" {
			coolVel = 8.0
		}
		if vel3sBps <= -coolVel {
			st.dropCoolUntil = now.Add(12 * time.Second)
		}
		if vel3sBps >= coolVel {
			st.pumpCoolUntil = now.Add(12 * time.Second)
		}
	}
	ltMid := 0.5 * (lb + la)
	spBps := (la - lb) / ltMid * 10000.0
	instBasis := (ltMid - mid) / mid * 10000.0
	isDisloc := isDislocSym(sym)
	dActive, dEdge, dTP, dSL, dMaxRng, dPickoff, dMaxSp := dislocParams(sym, now)
	basisMaxSp := 1.25
	if isDisloc {
		basisMaxSp = dMaxSp
	} else if sym == "XRP" || sym == "BNB" || sym == "NVDA" {
		basisMaxSp = 2.20
	}
	if st.lastBasisTS.IsZero() {
		st.emaSpreadBps = spBps
		st.lastBasisTS = now
		if spBps <= basisMaxSp {
			st.basisBps = instBasis
			st.basisWarmTicks = 1
		}
	} else {
		bdt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastBasisTS).Seconds()))
		st.lastBasisTS = now
		if spBps <= basisMaxSp {
			if st.basisWarmTicks == 0 {
				st.basisBps = instBasis
				st.basisWarmTicks = 1
			} else if st.basisWarmTicks < 15 {
				st.basisBps += (1.0 - math.Exp(-bdt/5.0)) * (instBasis - st.basisBps)
				st.basisWarmTicks++
			} else {
				st.basisBps += (1.0 - math.Exp(-bdt/45.0)) * (instBasis - st.basisBps) // Tight 45s basis anchor!
			}
		}
		st.emaSpreadBps += (1.0 - math.Exp(-bdt/45.0)) * (spBps - st.emaSpreadBps)
	}
	refFair := micro
	if isDisloc || sym == "SOL" || sym == "XRP" || sym == "BNB" || sym == "NVDA" {
		refFair = mid
	}
	fairLt := refFair * (1.0 + st.basisBps/10000.0)
	fairEq := fairLt
	if eeq > 0 {
		fairEq = eeq * (1.0 + st.basisBps/10000.0)
	}
	bidAnchor := math.Min(fairLt, fairEq) // Fundamental Law #1: NEVER ratchet Bid above 90s equilibrium!
	askAnchor := math.Max(fairLt, fairEq) // Fundamental Law #1: NEVER ratchet Ask below 90s equilibrium!

	// Rolling 60s Realized High-Low Range (All-Weather Low/Medium/High/Cascade Detector)
	curSlot := now.Unix() / 10
	minLo, maxHi := mid, mid
	for i := 0; i < 6; i++ {
		b := st.rangeBuckets[i]
		if b.slot > 0 && curSlot-b.slot < 6 {
			if b.hi > maxHi {
				maxHi = b.hi
			}
			if b.lo < minLo {
				minLo = b.lo
			}
		}
	}
	st.range60sBps = (maxHi - minLo) / mid * 10000.0
	preferBid := (fairLt-lb) >= (la-fairLt) || obi >= 0

	// Fundamental Law #2: ETH owns Calm (<15bps, 91.1% WR); BTC owns Active (9.5-21bps) + Sniper (8.5-26bps, 84.6% WR)
	volSafe := false
	baseEdge := 4.6
	if sym == "ETH" {
		volSafe = st.range60sBps >= 2.8 && st.range60sBps <= 15.0 && spBps <= 1.20
		baseEdge = 4.8
		if (preferBid && macroTrendBps >= 2.0 && trendBps >= 0.5 && obi >= 0.15) || (!preferBid && macroTrendBps <= -2.0 && trendBps <= -0.5 && obi <= -0.15) {
			baseEdge = 4.4
		}
	} else {
		volSafe = st.range60sBps >= 5.5 && st.range60sBps <= 22.0
		baseEdge = 4.5
		if (preferBid && macroTrendBps >= 2.0 && trendBps >= 0.5 && obi >= 0.15) || (!preferBid && macroTrendBps <= -2.0 && trendBps <= -0.5 && obi <= -0.15) {
			baseEdge = 4.1
		}
	}
	volSafeSnipe := st.range60sBps >= 8.5 && st.range60sBps <= 26.0
	edge := baseEdge
	if st.range60sBps > 13.0 {
		edge = baseEdge + (st.range60sBps-13.0)*0.22
	}
	snipeMaxSp := 0.95
	snipeThresh := 2.9
	minEdgeGuarantee := 1.9
	momLo, momHi := -0.5, 11.0
	macLo, macHi := -2.0, 95.0
	trLo, trHi := 0.0, 7.5
	impMax := 4.5
	velLo, velHi := 0.40, 5.0
	obiMin := 0.22
	if sym == "BTC" && st.range60sBps > 18.0 {
		snipeThresh = 3.4
	} else if sym == "ETH" {
		volSafeSnipe = st.range60sBps >= 6.5 && st.range60sBps <= 30.0
		snipeMaxSp = 0.85
		snipeThresh = 3.5
		minEdgeGuarantee = 2.0
		momLo, momHi = -0.5, 11.0
		macLo, macHi = -2.0, 95.0
		trLo, trHi = 0.0, 8.0
		impMax = 5.5
		velLo, velHi = 0.45, 5.5
		obiMin = 0.18
	} else if sym == "SOL" || sym == "XRP" || sym == "BNB" || sym == "ZEC" {
		volSafeSnipe = st.range60sBps >= 7.0 && st.range60sBps <= 36.0
		snipeMaxSp = 1.20
		if sym == "XRP" || sym == "BNB" || sym == "ZEC" {
			snipeMaxSp = 1.30
		}
		snipeThresh = 4.6
		minEdgeGuarantee = 2.5
		momLo, momHi = -1.0, 15.0
		macLo, macHi = -3.0, 115.0
		trLo, trHi = 0.0, 14.0
		impMax = 10.0
		velLo, velHi = 0.65, 7.0
		obiMin = -1.0
	} else if sym == "NVDA" {
		volSafeSnipe = st.range60sBps >= 3.5 && st.range60sBps <= 32.0
		snipeMaxSp = 1.50
		snipeThresh = 4.4
		minEdgeGuarantee = 2.4
		momLo, momHi = -1.0, 12.0
		macLo, macHi = -3.0, 95.0
		trLo, trHi = 0.0, 12.0
		impMax = 9.0
		velLo, velHi = 0.45, 6.0
		obiMin = -1.0
	} else if sym == "HYPE" {
		volSafeSnipe = st.range60sBps >= 7.5 && st.range60sBps <= 38.0
		snipeMaxSp = 1.40
		snipeThresh = 5.5
		minEdgeGuarantee = 2.8
		momLo, momHi = -1.0, 16.0
		macLo, macHi = -3.0, 125.0
		trLo, trHi = 0.0, 16.0
		impMax = 11.5
		velLo, velHi = 0.75, 8.0
		obiMin = -1.0
	}
	minHr := now.Minute()
	clockPause := minHr == 59 || minHr == 0 || minHr == 30
	if sym == "BTC" || sym == "ETH" {
		if minHr >= 58 || minHr <= 2 || (minHr >= 29 && minHr <= 32) {
			edge += 1.4
			snipeThresh += 0.8
		}
	}

	holdOneLocked := anyOtherOpenPos(sym) || now.UnixMilli() < globalEntryLockUntil.Load()
	dirCap := globalDirCapUSD()
	otherPendBid := otherSameDirPendingUSD(sym, true)
	otherPendAsk := otherSameDirPendingUSD(sym, false)
	coinActive := m.AllowEntry && st.dynClipUSD >= 0 && !holdOneLocked && st.basisWarmTicks >= 15 && !remotePaused.Load()
	makerAllowed := false // Directional passive Makers permanently OFF; pure single-leg True-Green-Lock snipes only!
	inCool := now.Before(st.stopCoolUntil) || now.Sub(st.lastFillTS) < 20*time.Second
	inCoolBuy := now.Before(st.stopCoolUntil) || now.Sub(st.lastFillTS) < 2*time.Second || (st.lastEntryDir == 1 && now.Sub(st.lastFillTS) < 18*time.Second)
	inCoolSell := now.Before(st.stopCoolUntil) || now.Sub(st.lastFillTS) < 2*time.Second || (st.lastEntryDir == -1 && now.Sub(st.lastFillTS) < 18*time.Second)

	bidAllowed := volSafe && !clockPause && macroTrendBps >= -48.0 && macroTrendBps <= 95.0 && mom5mBps >= -12.0 && mom5mBps <= 22.0 && trendBps >= -4.0 && trendBps <= 8.5
	askAllowed := volSafe && !clockPause && macroTrendBps <= 48.0 && macroTrendBps >= -95.0 && mom5mBps <= 12.0 && mom5mBps >= -22.0 && trendBps <= 4.0 && trendBps >= -8.5

	updateShadowPaper(st, now, lb, la, fairLt, dEdge, dTP, dSL, dActive && isDisloc && spBps <= dMaxSp)

	hi5, lo5 := mid, mid
	for mm := curMin - 5; mm <= curMin; mm++ {
		idx := int(((mm % 128) + 128) % 128)
		if st.minSlot[idx] == mm && st.minHi[idx] > 0 && st.minLo[idx] > 0 {
			if st.minHi[idx] > hi5 {
				hi5 = st.minHi[idx]
			}
			if st.minLo[idx] < lo5 {
				lo5 = st.minLo[idx]
			}
		}
	}
	rng5mBps := (hi5 - lo5) / mid * 10000.0
	effMaxRng := dMaxRng
	if st.dynClipUSD <= 25.0 {
		effMaxRng = dMaxRng * 0.90 // Probation trades CALMER books, never 1.45x choppier!
	}
	calmOk := isDisloc && dActive && coinActive && !inCool && spBps <= dMaxSp && rng5mBps <= effMaxRng && st.range60sBps <= effMaxRng && math.Abs(ret15mBps) <= effMaxRng*1.8 && math.Abs(vel3sBps) < 3.8 && math.Abs(obi) <= 0.60
	bidSafe := false
	askSafe := false
	if isDisloc {
		edge = dEdge
		bidAnchor = math.Min(fairLt, fairEq) // Fundamental Law: NEVER ratchet Bid above 90s equilibrium!
		askAnchor = math.Max(fairLt, fairEq) // Fundamental Law: NEVER ratchet Ask below 90s equilibrium!
		bidSafe = calmOk && now.After(st.dropCoolUntil) && (dirCap-otherPendBid) >= 12.0
		askSafe = calmOk && now.After(st.pumpCoolUntil) && (dirCap-otherPendAsk) >= 12.0
	}
	keepBid := isDisloc && dActive && !holdOneLocked && !remotePaused.Load() && !inCool && now.After(st.dropCoolUntil) && rng5mBps <= effMaxRng*1.05 && math.Abs(vel3sBps) < 4.5
	keepAsk := isDisloc && dActive && !holdOneLocked && !remotePaused.Load() && !inCool && now.After(st.pumpCoolUntil) && rng5mBps <= effMaxRng*1.05 && math.Abs(vel3sBps) < 4.5

	_ = bidAllowed
	_ = askAllowed
	_ = makerAllowed
	_ = fObi
	_ = impBps
	_ = minEdgeGuarantee
	_ = snipeMaxSp
	_ = snipeThresh
	_ = volSafeSnipe
	_ = momLo
	_ = momHi
	_ = macLo
	_ = macHi
	_ = trLo
	_ = trHi
	_ = impMax
	_ = velLo
	_ = velHi
	_ = obiMin
	_ = inCoolBuy
	_ = inCoolSell
	_ = ret3mBps
	_ = ret5mBps
	_ = ret15mBps
	_ = btcTr60
	_ = btcTrOk
	_ = ethTr60
	_ = ethTrOk
	waveBuy, waveSell, sigMin := false, false, int64(0)
	_ = waveBuy
	_ = waveSell
	btcTrendBuy := false
	btcTrendSell := false

	// 1. Manage resting orders: ModifyOrder (1 write) or Quota-Free 0ms L2CancelOrder!
	pickoffBuf := dPickoff
	modWait := 15000 * time.Millisecond
	modMove := 3.0
	for _, side := range []string{"BID", "ASK"} {
		ao := st.activeOrds[side]
		if ao == nil {
			continue
		}
		moveBps := math.Abs(mid-ao.RefMid) / ao.RefMid * 10000.0
		ageS := now.Sub(ao.TS).Seconds()
		if ao.IsIOC {
			if ageS >= 2.0 {
				delete(st.activeOrds, side)
				if side == "BID" {
					st.pendBidMicroUSD.Store(0)
				} else {
					st.pendAskMicroUSD.Store(0)
				}
			}
			continue
		}
		tooClose := (side == "BID" && ao.Px >= la) || (side == "ASK" && ao.Px <= lb)
		if !ao.IsClose {
			aboutToGetPickedOff := (side == "BID" && fairLt <= ao.Px*(1.0+pickoffBuf/10000.0)) || (side == "ASK" && fairLt >= ao.Px*(1.0-pickoffBuf/10000.0))
			safeNow := !aboutToGetPickedOff && ((side == "BID" && (keepBid || btcTrendBuy)) || (side == "ASK" && (keepAsk || btcTrendSell)))
			if !safeNow {
				cancelSide(sym, side) // L2CancelOrder is 100% quota-free & unthrottled!
			} else if (moveBps >= modMove || ageS >= 35.0 || tooClose) && now.Sub(st.lastOrdTS[side]) >= modWait && takeWriteSlot(false) {
				newPx := roundTo(math.Min(lb, bidAnchor*(1.0-edge/10000.0)), m.PxDec)
				if side == "ASK" {
					newPx = roundTo(math.Max(la, askAnchor*(1.0+edge/10000.0)), m.PxDec)
				}
				if side == "BID" && newPx >= la {
					newPx = roundTo(la-tick, m.PxDec)
				}
				if side == "ASK" && newPx <= lb {
					newPx = roundTo(lb+tick, m.PxDec)
				}
				st.lastOrdTS[side] = now
				modifySide(sym, side, newPx, mid)
			}
		}
	}

	ae := st.avgEntry
	stepBps := closeStepBps(sym, st)
	holdS := 0.0
	if !st.posOpenTS.IsZero() {
		holdS = now.Sub(st.posOpenTS).Seconds()
	}

	slInit, lockPeak, lockFloor := dSL, math.Max(8.5, dTP*1.45), math.Max(5.5, dTP*1.05)

	// 2. Closing orders: v84 1:1 Payoff Convergence (+dTP), 3m Stalled Cut, 8m Scratch, -dSL Stop
	if pos <= -m.MinB && ae > 0 {
		curTkShort := (ae - la) / ae * 10000.0
		if curTkShort > st.peakFavBps {
			st.peakFavBps = curTkShort
		}
		trailFloor := stepRatchetFloor(st.peakFavBps, lockFloor)
		greenLockShort := st.peakFavBps >= lockPeak && ((curTkShort <= trailFloor && spBps <= 6.0) || curTkShort <= trailFloor-0.8)
		earlyCutShort := st.peakFavBps < dTP*0.50 && holdS >= 180.0 && curTkShort <= -(dSL*0.72) && spBps <= 5.5
		hardSlShort := (holdS >= 35.0 && ((curTkShort <= -slInit && spBps <= 6.0) || curTkShort <= -(slInit+1.5))) || curTkShort <= -24.0
		slHitShort := hardSlShort || earlyCutShort || greenLockShort
		defBailShort := false
		timeScratchShort := holdS >= 480.0
		closeShortPx := roundTo(la*1.0005, m.PxDec)
		if greenLockShort {
			closeShortPx = roundTo(la*1.0001+tick, m.PxDec)
		}
		if posUSD > -10.10 {
			if (curTkShort >= stepBps || slHitShort || defBailShort || timeScratchShort) && st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 2*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["BID"] = now
				if (hardSlShort || defBailShort) && !greenLockShort {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(240 * time.Second)
				} else if earlyCutShort && !greenLockShort {
					st.stopCoolUntil = now.Add(120 * time.Second)
				}
				placeSide(sym, "BID", closeShortPx, mid, math.Abs(pos), true, true)
			}
		} else if slHitShort || defBailShort || timeScratchShort {
			if ao := st.activeOrds["BID"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "BID")
			}
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 350*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["BID"] = now
				if (hardSlShort || defBailShort) && !greenLockShort {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(240 * time.Second)
				} else if earlyCutShort && !greenLockShort {
					st.stopCoolUntil = now.Add(120 * time.Second)
				}
				placeSide(sym, "BID", closeShortPx, mid, math.Abs(pos), true, true)
			}
		} else {
			wantBid := roundTo(ae*(1.0-stepBps/10000.0), m.PxDec)
			if ao := st.activeOrds["BID"]; ao != nil && ao.IsClose && !ao.IsIOC && math.Abs(ao.Px-wantBid)/ae*10000.0 >= 0.45 && now.Sub(st.lastOrdTS["BID"]) >= 1500*time.Millisecond && takeWriteSlot(true) {
				st.lastOrdTS["BID"] = now
				modifySide(sym, "BID", wantBid, mid)
			} else if ao == nil && now.Sub(st.lastOrdTS["BID"]) >= 250*time.Millisecond && takeWriteSlot(true) {
				st.lastOrdTS["BID"] = now
				placeSide(sym, "BID", wantBid, mid, math.Abs(pos), true, false)
			}
		}
	} else if math.Abs(posUSD) < 1.0 && (bidSafe || btcTrendBuy) && now.Sub(st.lastCancelTS["BID"]) >= 2500*time.Millisecond {
		st.trendMode = false
		if btcTrendBuy {
			snipeBidPx := roundTo(la*1.00015+tick, m.PxDec)
			if snipeBidPx >= la && cancelDistantForSnipe(sym, "BID") {
				if st.activeOrds["ASK"] != nil && !cancelDistantForSnipe(sym, "ASK") {
					cancelSide(sym, "ASK")
				}
				if st.activeOrds["BID"] == nil && st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 1800*time.Millisecond && takeWriteSlot(false) {
					st.lastOrdTS["BID"] = now
					st.lastSigMin = sigMin
					placeSide(sym, "BID", snipeBidPx, mid, 0, false, true)
				}
			}
		} else if st.activeOrds["ASK"] == nil && (preferBid || !askSafe) {
			wantBid := roundTo(math.Min(lb, bidAnchor*(1.0-edge/10000.0)), m.PxDec)
			if wantBid >= la {
				wantBid = roundTo(la-tick, m.PxDec)
			}
			eCool := st.entryCool
			if eCool < 2000*time.Millisecond {
				eCool = 2000 * time.Millisecond
			}
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= eCool && takeWriteSlot(false) {
				st.lastOrdTS["BID"] = now
				placeSide(sym, "BID", wantBid, mid, 0, false, false)
			}
		}
	}

	if pos >= m.MinB && ae > 0 {
		curTkLong := (lb - ae) / ae * 10000.0
		if curTkLong > st.peakFavBps {
			st.peakFavBps = curTkLong
		}
		trailFloorL := stepRatchetFloor(st.peakFavBps, lockFloor)
		greenLockLong := st.peakFavBps >= lockPeak && ((curTkLong <= trailFloorL && spBps <= 6.0) || curTkLong <= trailFloorL-0.8)
		earlyCutLong := st.peakFavBps < dTP*0.50 && holdS >= 180.0 && curTkLong <= -(dSL*0.72) && spBps <= 5.5
		hardSlLong := (holdS >= 35.0 && ((curTkLong <= -slInit && spBps <= 6.0) || curTkLong <= -(slInit+1.5))) || curTkLong <= -24.0
		slHitLong := hardSlLong || earlyCutLong || greenLockLong
		defBailLong := false
		timeScratchLong := holdS >= 480.0
		closeLongPx := roundTo(lb*0.9995, m.PxDec)
		if greenLockLong {
			closeLongPx = roundTo(lb*0.9999-tick, m.PxDec)
		}
		if posUSD < 10.10 {
			if (curTkLong >= stepBps || slHitLong || defBailLong || timeScratchLong) && st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 2*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["ASK"] = now
				if (hardSlLong || defBailLong) && !greenLockLong {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(240 * time.Second)
				} else if earlyCutLong && !greenLockLong {
					st.stopCoolUntil = now.Add(120 * time.Second)
				}
				placeSide(sym, "ASK", closeLongPx, mid, math.Abs(pos), true, true)
			}
		} else if slHitLong || defBailLong || timeScratchLong {
			if ao := st.activeOrds["ASK"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "ASK")
			}
			if st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 350*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["ASK"] = now
				if (hardSlLong || defBailLong) && !greenLockLong {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(240 * time.Second)
				} else if earlyCutLong && !greenLockLong {
					st.stopCoolUntil = now.Add(120 * time.Second)
				}
				placeSide(sym, "ASK", closeLongPx, mid, math.Abs(pos), true, true)
			}
		} else {
			wantAsk := roundTo(ae*(1.0+stepBps/10000.0), m.PxDec)
			if ao := st.activeOrds["ASK"]; ao != nil && ao.IsClose && !ao.IsIOC && math.Abs(ao.Px-wantAsk)/ae*10000.0 >= 0.45 && now.Sub(st.lastOrdTS["ASK"]) >= 1500*time.Millisecond && takeWriteSlot(true) {
				st.lastOrdTS["ASK"] = now
				modifySide(sym, "ASK", wantAsk, mid)
			} else if ao == nil && now.Sub(st.lastOrdTS["ASK"]) >= 250*time.Millisecond && takeWriteSlot(true) {
				st.lastOrdTS["ASK"] = now
				placeSide(sym, "ASK", wantAsk, mid, math.Abs(pos), true, false)
			}
		}
	} else if math.Abs(posUSD) < 1.0 && (askSafe || btcTrendSell) && now.Sub(st.lastCancelTS["ASK"]) >= 2500*time.Millisecond {
		st.trendMode = false
		if btcTrendSell {
			snipeAskPx := roundTo(lb*0.99985-tick, m.PxDec)
			if snipeAskPx <= lb && cancelDistantForSnipe(sym, "ASK") {
				if st.activeOrds["BID"] != nil && !cancelDistantForSnipe(sym, "BID") {
					cancelSide(sym, "BID")
				}
				if st.activeOrds["ASK"] == nil && st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 1800*time.Millisecond && takeWriteSlot(false) {
					st.lastOrdTS["ASK"] = now
					st.lastSigMin = sigMin
					placeSide(sym, "ASK", snipeAskPx, mid, 0, false, true)
				}
			}
		} else if st.activeOrds["BID"] == nil && (!preferBid || !bidSafe) {
			wantAsk := roundTo(math.Max(la, askAnchor*(1.0+edge/10000.0)), m.PxDec)
			if wantAsk <= lb {
				wantAsk = roundTo(lb+tick, m.PxDec)
			}
			eCool := st.entryCool
			if eCool < 2000*time.Millisecond {
				eCool = 2000 * time.Millisecond
			}
			if st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= eCool && takeWriteSlot(false) {
				st.lastOrdTS["ASK"] = now
				placeSide(sym, "ASK", wantAsk, mid, 0, false, false)
			}
		}
	}

	logMu.Lock()
	shouldLog := now.Sub(lastLog) >= 25*time.Second
	if shouldLog {
		lastLog = now
	}
	logMu.Unlock()
	if shouldLog {
		fmt.Printf("[%s] GO-TOKYO v86 LIVE | Calm RWA Stocks (GOOGL/AMZN/META) + 35s Wick Shield + 45s Basis | Eq=$%.4f | %s Pos=%.5f | Clip=$%.0f | Net=$%.2f | Rng60s=%.1f | Sp=%.2f | Strk=%d | OBI=%.2f\n",
			now.Format("15:04:05"), float64(eqNow.Load())/1e6, sym, pos, st.dynClipUSD, netUSD, st.range60sBps, spBps, streakWins.Load(), obi)
	}
}

func rebalanceFromTrades(tb []byte) {
	var resp struct {
		Trades []struct {
			MarketID int64  `json:"market_id"`
			Symbol   string `json:"market_symbol"`
			BidID    int64  `json:"bid_account_id"`
			AskID    int64  `json:"ask_account_id"`
			BidPnl   string `json:"bid_account_pnl"`
			AskPnl   string `json:"ask_account_pnl"`
			TS       int64  `json:"timestamp"`
		} `json:"trades"`
	}
	if json.Unmarshal(tb, &resp) != nil {
		return
	}
	nowMS := time.Now().UnixMilli()
	allSyms := []string{"BTC", "XAU", "PAXG", "ETH", "SOL", "XRP", "HYPE", "BNB", "ZEC", "NVDA", "AMZN", "GOOGL", "META", "MU", "SOXL", "SPY", "US500", "QQQ", "US100"}
	pnl := map[string]float64{}
	wins := map[string]float64{}
	closes := map[string]float64{}
	hadLoss := map[string]bool{}
	lastWasLoss := map[string]bool{}
	consecWins := map[string]int{}
	totalWins := map[string]int{}
	for _, s := range allSyms {
		pnl[s] = 0
		wins[s] = 0
		closes[s] = 0
	}
	type closedFill struct {
		sym  string
		tsMS int64
		pnl  float64
	}
	twinGroup := func(s string) string {
		if s == "PAXG" || s == "XAU" {
			return "PAXG/XAU"
		}
		if s == "US500" || s == "SPY" {
			return "US500/SPY"
		}
		return ""
	}
	var fills []closedFill
	var bootPairFills []closedFill
	for _, t := range resp.Trades {
		tsMS := t.TS
		if tsMS > 0 && tsMS < 100000000000 {
			tsMS *= 1000
		}
		if tsMS < 1795229500000 {
			continue
		}
		s := t.Symbol
		if s == "" {
			s = idToSym[t.MarketID]
		}
		if _, ok := mktStates[s]; !ok {
			continue
		}
		pStr := ""
		if t.BidID == AcctIdx {
			pStr = t.BidPnl
		} else if t.AskID == AcctIdx {
			pStr = t.AskPnl
		}
		if pStr == "" {
			continue
		}
		realized, err := strconv.ParseFloat(pStr, 64)
		if err != nil || math.Abs(realized) < 0.0001 {
			continue
		}
		if _, ok := pnl[s]; ok {
			fills = append(fills, closedFill{sym: s, tsMS: tsMS, pnl: realized})
		}
		if tsMS >= v87BootMS-30000 {
			grp := twinGroup(s)
			if grp == "" {
				grp = s
			}
			bootPairFills = append(bootPairFills, closedFill{sym: grp, tsMS: tsMS, pnl: realized})
		}
	}
	sort.Slice(bootPairFills, func(i, j int) bool { return bootPairFills[i].tsMS < bootPairFills[j].tsMS })
	var pairRTs []closedFill
	for _, f := range bootPairFills {
		n := len(pairRTs)
		if n > 0 && f.tsMS-pairRTs[n-1].tsMS <= 15000 {
			pairRTs[n-1].pnl += f.pnl
			pairRTs[n-1].tsMS = f.tsMS
		} else {
			pairRTs = append(pairRTs, f)
		}
	}
	consecPairLosses := 0
	sessionPairPnl := 0.0
	var netWins int64
	hasOpenPos := anyOpenAcctPos()
	for _, prt := range pairRTs {
		if nowMS-prt.tsMS < 8000 && hasOpenPos {
			continue
		}
		sessionPairPnl += prt.pnl
		if prt.pnl < -0.10 {
			consecPairLosses++
			netWins--
		} else if prt.pnl > 0.005 {
			consecPairLosses = 0
			netWins++
		}
	}
	sessionRealizedMicroUSD.Store(int64(sessionPairPnl * 1e6))
	sessionNetWins.Store(netWins)
	if !breakerTripped.Load() && (consecPairLosses >= 3 || sessionPairPnl <= -0.55) {
		breakerTripped.Store(true)
		_ = os.WriteFile("/tmp/lighter_pause.flag", []byte("1"), 0644)
		reason := fmt.Sprintf("AUTO-BREAKER losses=%d pnl=$%.4f", consecPairLosses, sessionPairPnl)
		fmt.Printf("[%s] %s — halting safely!\n", time.Now().Format("15:04:05"), reason)
		go flattenAndExitPause(reason)
		return
	}
	sort.Slice(fills, func(i, j int) bool { return fills[i].tsMS < fills[j].tsMS })
	var rts []closedFill
	for _, f := range fills {
		n := len(rts)
		if n > 0 && rts[n-1].sym == f.sym && f.tsMS-rts[n-1].tsMS <= 3000 {
			rts[n-1].pnl += f.pnl
			rts[n-1].tsMS = f.tsMS
		} else {
			rts = append(rts, f)
		}
	}
	streak := 0
	inCaution := false
	for _, rt := range rts {
		if rt.pnl < 0 {
			hadLoss[rt.sym] = true
			lastWasLoss[rt.sym] = true
			consecWins[rt.sym] = 0
			if isDislocSym(rt.sym) {
				streak = 0
				if float64(nowMS-rt.tsMS)/60000.0 <= 12.0 {
					inCaution = true
				}
			}
		} else {
			lastWasLoss[rt.sym] = false
			consecWins[rt.sym]++
			totalWins[rt.sym]++
			if isDislocSym(rt.sym) && rt.tsMS > lastStopMS.Load() {
				streak++
				inCaution = false
			}
		}
		ageMin := math.Max(0.0, float64(nowMS-rt.tsMS)/60000.0)
		w := math.Exp(-ageMin / 180.0) // 3h half-life: losses NEVER evaporate in 25m!
		pnl[rt.sym] += rt.pnl * w
		closes[rt.sym] += w
		if rt.pnl > 0 {
			wins[rt.sym] += w
		}
	}
	probationForce.Store(inCaution)
	streakWins.Store(int64(streak))
	curEq := float64(eqNow.Load()) / 1e6
	if curEq < 50.0 {
		curEq = 100.0
	}
	evScores := map[string]float64{}
	topSym := ""
	topScore := -1e9
	for _, s := range evergreenSyms {
		st := mktStates[s]
		st.mu.Lock()
		sh := st.shadowBps
		st.mu.Unlock()
		wr := 0.85
		if closes[s] > 0.05 {
			wr = wins[s] / closes[s]
		}
		sc := pnl[s]*100.0 + sh*0.45 + (wr-0.85)*2.0
		evScores[s] = sc
		if consecWins[s] >= 3 && pnl[s] >= 0.05 && !lastWasLoss[s] && sc >= 2.5 && sc > topScore {
			topScore = sc
			topSym = s
		}
	}
	now := time.Now()
	for _, sym := range []string{"BTC", "XAU", "ETH", "SOL", "XRP", "HYPE", "BNB", "ZEC", "NVDA", "AMZN", "GOOGL", "META", "MU", "SOXL"} {
		st := mktStates[sym]
		dAct, dEdg, _, _, _, _, _ := dislocParams(sym, now)
		clip := -1.0
		if isDislocSym(sym) && dAct {
			if lastWasLoss[sym] || (hadLoss[sym] && consecWins[sym] < 2) || pnl[sym] < -0.005 || evScores[sym] <= -3.5 {
				clip = 22.0 // Locked probation until 2 straight wins AND positive 3h net PnL!
			} else if sym == topSym {
				clip = 260.0 // #1 Top Winner (3+ straight wins)
				if curEq >= 96.5 {
					clip = 320.0
				}
			} else if consecWins[sym] >= 2 && pnl[sym] >= 0.025 {
				clip = 180.0 // Proven Winner (2+ straight wins)
				if curEq >= 95.5 {
					clip = 220.0
				}
			} else if consecWins[sym] >= 1 && pnl[sym] > 0 {
				clip = 95.0 // 1-Win Validated
			} else {
				clip = 40.0 // Unproven Explorer (0 trades yet)
			}
		}
		st.mu.Lock()
		prevClip := st.dynClipUSD
		st.dynClipUSD = clip
		st.makerActive = dAct && clip > 0
		st.dynEdgeBps = dEdg
		st.entryCool = 2000 * time.Millisecond
		st.rollPnl = pnl[sym]
		if math.Abs(prevClip-clip) >= 8.0 && math.Abs(st.pos) < 1e-6 {
			for _, sd := range []string{"BID", "ASK"} {
				if ao := st.activeOrds[sd]; ao != nil && !ao.IsClose {
					cancelSide(sym, sd) // Immediately cancel stale-sized resting quotes!
				}
			}
		}
		st.mu.Unlock()
		st.rollPnlMicro.Store(int64(evScores[sym] * 1e6))
	}
	for _, sym := range []string{"SPY", "US500", "QQQ", "US100"} {
		if st := mktStates[sym]; st != nil {
			st.rollPnlMicro.Store(int64(pnl[sym] * 1e6))
		}
	}
}

func syncAccountLoop() {
	hc := &http.Client{Timeout: 6 * time.Second}
	url := fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx)
	trUrl := fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=100&sort_by=timestamp", BaseURL, AcctIdx)
	loopCnt := 0
	for {
		loopCnt++
		if loopCnt%2 == 1 {
			if trResp, e := hc.Get(trUrl); e == nil {
				tb, _ := io.ReadAll(trResp.Body)
				trResp.Body.Close()
				if len(tb) > 20 && strings.Contains(string(tb), `"trades"`) {
					_ = os.WriteFile("/tmp/lighter_trades.json.tmp", tb, 0644)
					_ = os.Rename("/tmp/lighter_trades.json.tmp", "/tmp/lighter_trades.json")
					rebalanceFromTrades(tb)
				}
			}
		}
		resp, err := hc.Get(url)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var res struct {
				Accounts []struct {
					TotalAssetValue string `json:"total_asset_value"`
					Collateral      string `json:"collateral"`
					Positions       []struct {
						MarketID       int64  `json:"market_id"`
						Position       string `json:"position"`
						Sign           int    `json:"sign"`
						AvgEntryPrice  string `json:"avg_entry_price"`
						OpenOrderCount int    `json:"open_order_count"`
					} `json:"positions"`
				} `json:"accounts"`
			}
			if json.Unmarshal(body, &res) == nil && len(res.Accounts) > 0 {
				var rawObj struct {
					Accounts []json.RawMessage `json:"accounts"`
				}
				if json.Unmarshal(body, &rawObj) == nil && len(rawObj.Accounts) > 0 {
					_ = os.WriteFile("/tmp/lighter_acct.json.tmp", rawObj.Accounts[0], 0644)
					_ = os.Rename("/tmp/lighter_acct.json.tmp", "/tmp/lighter_acct.json")
				}
				a := res.Accounts[0]
				eq, _ := strconv.ParseFloat(a.TotalAssetValue, 64)
				if eq <= 0 {
					eq, _ = strconv.ParseFloat(a.Collateral, 64)
				}
				orphanLeak := false
				if eq > 0 {
					eqNow.Store(int64(eq * 1e6))
				}
				for _, p := range a.Positions {
					if sym, ok := idToSym[p.MarketID]; ok {
						st := mktStates[sym]
					if st == nil {
						continue
					}
						sz, _ := strconv.ParseFloat(p.Position, 64)
						if p.Sign < 0 && sz > 0 {
							sz = -sz
						}
						st.mu.Lock()
						prevPos := st.pos
						st.pos = sz
						ae, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
						st.avgEntry = ae
						refPx := st.bnMid
						if refPx <= 0 && st.ltBid > 0 && st.ltAsk > 0 {
							refPx = 0.5 * (st.ltBid + st.ltAsk)
						} else if refPx <= 0 {
							refPx = ae
						}
						if refPx > 0 {
							st.netMicroUSD.Store(int64(sz * refPx * 1e6))
						}
						if math.Abs(sz) > 1e-6 && math.Abs(prevPos) < 1e-6 {
							st.posOpenTS = time.Now()
							st.peakFavBps = 0
						} else if math.Abs(sz) < 1e-6 {
							st.posOpenTS = time.Time{}
							st.trendMode = false
							st.peakFavBps = 0
							st.entryWasSnipe = false
						}
						if p.OpenOrderCount > 2 {
							orphanLeak = true
						}
						if math.Abs(sz-prevPos) > 1e-6 {
							st.lastFillTS = time.Now()
						}
						if p.OpenOrderCount == 0 {
							for _, s := range []string{"BID", "ASK"} {
								if ao := st.activeOrds[s]; ao != nil && time.Since(ao.TS) > 2*time.Second {
									delete(st.activeOrds, s)
									if s == "BID" {
										st.pendBidMicroUSD.Store(0)
									} else {
										st.pendAskMicroUSD.Store(0)
									}
								}
							}
						} else if math.Abs(sz) >= markets[sym].MinB {
							for _, s := range []string{"BID", "ASK"} {
								if ao := st.activeOrds[s]; ao != nil && !ao.IsClose && !ao.IsIOC {
									cancelSide(sym, s) // Never delete live entry orders from RAM without cancelling on Lighter!
								}
							}
						}
						st.mu.Unlock()
					}
				}
				if orphanLeak {
					if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
						nonceSeq.Store(n)
					}
					cancelAll()
				}
				manageAllTwins()
				if eq >= 5000.00 {
					fmt.Printf("[GOAL-REACHED] Equity $%.4f >= $5000.00, locking profits and halting!\n", eq)
					_ = os.WriteFile("/tmp/lighter_pause.flag", []byte("1"), 0644)
					flattenAndExitPause("GOAL >= $5000.00 REACHED")
				}
				if eq > 0 && eq <= StopEqUSD {
					fmt.Printf("[STOP-LOSS] Equity $%.4f <= $%.2f, closing & halting!\n", eq, StopEqUSD)
					useWsSend = false
					cancelAll()
					for sym, st := range mktStates {
						st.mu.Lock()
						sz, lb, la := st.pos, st.ltBid, st.ltAsk
						st.mu.Unlock()
						if math.Abs(sz) >= markets[sym].MinB {
							m := markets[sym]
							px := la * 1.01
							var isAsk uint8 = 0
							if sz > 0 {
								isAsk = 1
								px = lb * 0.99
							}
							if px > 0 {
								_ = sendSignedTx("flat_"+sym, sym, func(opts *types.TransactOpts) (uint8, string, error) {
									ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
										MarketIndex:      m.ID,
										ClientOrderIndex: coiSeq.Add(1),
										BaseAmount:       int64(math.Round(math.Abs(sz) * math.Pow10(m.SzDec))),
										Price:            uint32(math.Round(px * math.Pow10(m.PxDec))),
										IsAsk:            isAsk,
										Type:             1, // MARKET
										TimeInForce:      0, // IOC
										ReduceOnly:       1,
										OrderExpiry:      0,
									}, opts)
									if e != nil {
										return 0, "", e
									}
									s, e := ti.GetTxInfo()
									return ti.GetTxType(), s, e
								})
							}
						}
					}
					time.Sleep(1 * time.Second)
					os.Exit(0)
				}
			}
		}
		time.Sleep(1200 * time.Millisecond)
	}
}

func runTxWS() {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WsURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		txWsMu.Lock()
		txConn = conn
		txWsMu.Unlock()
		done := make(chan struct{})
		go func(c *websocket.Conn) {
			t := time.NewTicker(15 * time.Second)
			defer t.Stop()
			for {
				select {
				case <-done:
					return
				case <-t.C:
					txWsMu.Lock()
					_ = c.WriteControl(websocket.PingMessage, nil, time.Now().Add(2*time.Second))
					txWsMu.Unlock()
				}
			}
		}(conn)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			sMsg := string(msg)
			if strings.Contains(sMsg, `"error"`) || (strings.Contains(sMsg, `"code":`) && !strings.Contains(sMsg, `"code":200`)) {
				fmt.Printf("[WS_RESP] %s\n", sMsg)
				if strings.Contains(sMsg, "429") || strings.Contains(strings.ToLower(sMsg), "quota") || strings.Contains(strings.ToLower(sMsg), "rate") {
					quotaCoolUntil.Store(time.Now().Add(8 * time.Second).UnixMilli())
				}
				if strings.Contains(sMsg, "21501") || strings.Contains(sMsg, "invalid tx") {
					useWsSend = false
				}
				if strings.Contains(sMsg, "invalid nonce") || strings.Contains(sMsg, `"error"`) {
					go func() {
						if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
							nonceSeq.Store(n)
						}
					}()
				}
			}
		}
		close(done)
		txWsMu.Lock()
		if txConn == conn {
			txConn = nil
		}
		txWsMu.Unlock()
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func isTwinSym(sym string) bool {
	return sym == "PAXG" || sym == "XAU" || sym == "SPY" || sym == "US500" || sym == "QQQ" || sym == "US100"
}

func pushL2Rec(line string) {
	select {
	case l2RecCh <- line:
	default:
	}
}

func runLtWS() {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WsURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		wsMu.Lock()
		ltConn = conn
		subbedMids = map[int64]bool{}
		for _, m := range markets {
			if m.AllowEntry {
				_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
				if m.Sym == "BTC" || m.Sym == "ETH" || m.Sym == "SOL" || m.Sym == "PAXG" || m.Sym == "XAU" || m.Sym == "SPY" || m.Sym == "US500" {
					_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("trade/%d", m.ID)})
				}
				subbedMids[m.ID] = true
			}
		}
		wsMu.Unlock()
		for _, st := range mktStates {
			st.mu.Lock()
			st.bookBids = map[string]float64{}
			st.bookAsks = map[string]float64{}
			st.ltBid = 0
			st.ltAsk = 0
			st.mu.Unlock()
		}
		if tok, err := txCli.GetAuthToken(time.Now().Add(6 * time.Hour)); err == nil {
			_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("account_all/%d", AcctIdx), "auth": tok})
		}

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			sMsg := string(msg)
			if !strings.Contains(sMsg, "order_book") && !strings.Contains(sMsg, "account_all") && !strings.Contains(sMsg, `"channel":"trade:`) {
				if strings.Contains(sMsg, `"error"`) || (strings.Contains(sMsg, `"code":`) && !strings.Contains(sMsg, `"code":200`)) {
					fmt.Printf("[WS_RESP] %s\n", sMsg)
					if strings.Contains(sMsg, "21501") || strings.Contains(sMsg, "invalid tx") {
						useWsSend = false // Auto-fallback to HTTP/2 sendTx if jsonapi/sendtx format rejected
					}
					if strings.Contains(sMsg, "invalid nonce") || strings.Contains(sMsg, `"error"`) {
						go func() {
							if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
								nonceSeq.Store(n)
							}
						}()
					}
				}
			}
			var m struct {
				Type      string `json:"type"`
				Channel   string `json:"channel"`
				OrderBook struct {
					Bids []struct {
						Price string `json:"price"`
						Size  string `json:"size"`
					} `json:"bids"`
					Asks []struct {
						Price string `json:"price"`
						Size  string `json:"size"`
					} `json:"asks"`
				} `json:"order_book"`
			}
			if json.Unmarshal(msg, &m) != nil {
				continue
			}
			if strings.HasPrefix(m.Channel, "trade:") {
				idStr := strings.TrimPrefix(m.Channel, "trade:")
				midID, _ := strconv.ParseInt(idStr, 10, 64)
				if sym, ok := idToSym[midID]; ok && !strings.HasPrefix(m.Type, "subscribed") {
					var trEv struct {
						Trades []struct {
							Price      string `json:"price"`
							Size       string `json:"size"`
							IsMakerAsk bool   `json:"is_maker_ask"`
							TS         int64  `json:"timestamp"`
						} `json:"trades"`
					}
					if json.Unmarshal(msg, &trEv) == nil {
						nowMS := time.Now().UnixMilli()
						for _, tr := range trEv.Trades {
							sd := "B"
							if tr.IsMakerAsk {
								sd = "S"
							}
							tMS := tr.TS
							if tMS <= 0 {
								tMS = nowMS
							}
							pushL2Rec(fmt.Sprintf(`{"t":%d,"k":"tr","s":%q,"p":%s,"sz":%s,"sd":%q}`, tMS, sym, tr.Price, tr.Size, sd))
						}
					}
				}
				continue
			}
			if strings.HasPrefix(m.Channel, "account_all:") {
				var accEv struct {
					Positions map[string]struct {
						Position      string `json:"position"`
						Sign          int    `json:"sign"`
						AvgEntryPrice string `json:"avg_entry_price"`
					} `json:"positions"`
				}
				if json.Unmarshal(msg, &accEv) == nil && len(accEv.Positions) > 0 {
					var touched []string
					for midStr, p := range accEv.Positions {
						midID, _ := strconv.ParseInt(midStr, 10, 64)
						if sym, ok := idToSym[midID]; ok {
							st := mktStates[sym]
					if st == nil {
						continue
					}
							sz, _ := strconv.ParseFloat(p.Position, 64)
							if p.Sign < 0 && sz > 0 {
								sz = -sz
							}
							st.mu.Lock()
							prev := st.pos
							st.pos = sz
							if ae, e := strconv.ParseFloat(p.AvgEntryPrice, 64); e == nil && ae > 0 {
								st.avgEntry = ae
							}
							refPx := st.bnMid
							if refPx <= 0 && st.ltBid > 0 && st.ltAsk > 0 {
								refPx = 0.5 * (st.ltBid + st.ltAsk)
							} else if refPx <= 0 {
								refPx = st.avgEntry
							}
							if refPx > 0 {
								st.netMicroUSD.Store(int64(sz * refPx * 1e6))
							}
							if math.Abs(sz) > 1e-6 && math.Abs(prev) < 1e-6 {
								st.posOpenTS = time.Now()
								st.peakFavBps = 0
								globalEntryLockUntil.Store(time.Now().Add(3500 * time.Millisecond).UnixMilli())
								cancelAllOtherEntryQuotes(sym) // Hold-One: 0.7ms quota-free cancel of all other coins' resting quotes!
							} else if math.Abs(sz) < 1e-6 {
								st.posOpenTS = time.Time{}
								st.trendMode = false
								st.peakFavBps = 0
								st.entryWasSnipe = false
							}
							if math.Abs(sz-prev) > 1e-6 {
								st.lastFillTS = time.Now()
								// NEVER silently delete non-close entry orders from RAM: always send quota-free L2CancelOrder
								// (harmless 0ms no-op if already 100% filled, cancels partial remainders AND eliminates double-buy races!)
								for _, sd := range []string{"BID", "ASK"} {
									if ao := st.activeOrds[sd]; ao != nil {
										if !ao.IsClose {
											if !ao.IsIOC {
												cancelSide(sym, sd)
											} else {
												delete(st.activeOrds, sd)
											}
										} else if math.Abs(sz) < 1e-6 {
											if !ao.IsIOC {
												cancelSide(sym, sd)
											} else {
												delete(st.activeOrds, sd)
											}
										}
									}
								}
								touched = append(touched, sym)
							}
							st.mu.Unlock()
						}
					}
					for _, sym := range touched {
						if enableAutoTwins && isTwinSym(sym) {
							manageAllTwins()
						}
						if isActiveOpenSym(sym) {
							manageActiveOpenStrat()
						}
						if isActiveCarbonSym(sym) {
							manageCarbonBracket()
						}
					}
				}
			} else if strings.HasPrefix(m.Channel, "order_book:") {
				idStr := strings.TrimPrefix(m.Channel, "order_book:")
				midID, _ := strconv.ParseInt(idStr, 10, 64)
				if sym, ok := idToSym[midID]; ok {
					st := mktStates[sym]
					if st == nil {
						continue
					}
					st.mu.Lock()
					prevB, prevA := st.ltBid, st.ltAsk
					needRecompute := strings.Contains(m.Type, "subscribed")
					if needRecompute {
						st.bookBids = map[string]float64{}
						st.bookAsks = map[string]float64{}
						st.ltBid, st.ltBidSz = 0, 0
						st.ltAsk, st.ltAskSz = 0, 0
					}
					for _, b := range m.OrderBook.Bids {
						sz, _ := strconv.ParseFloat(b.Size, 64)
						bp, err := strconv.ParseFloat(b.Price, 64)
						if sz <= 0 {
							delete(st.bookBids, b.Price)
							if err == nil && bp >= st.ltBid {
								needRecompute = true
							}
						} else {
							st.bookBids[b.Price] = sz
							if err == nil {
								if bp >= st.ltBid {
									st.ltBid, st.ltBidSz = bp, sz
								}
								if st.ltAsk > 0 && bp >= st.ltAsk {
									for apStr := range st.bookAsks {
										if ap, e2 := strconv.ParseFloat(apStr, 64); e2 == nil && ap <= bp {
											delete(st.bookAsks, apStr)
										}
									}
									needRecompute = true
								}
							}
						}
					}
					for _, a := range m.OrderBook.Asks {
						sz, _ := strconv.ParseFloat(a.Size, 64)
						ap, err := strconv.ParseFloat(a.Price, 64)
						if sz <= 0 {
							delete(st.bookAsks, a.Price)
							if err == nil && ap <= st.ltAsk {
								needRecompute = true
							}
						} else {
							st.bookAsks[a.Price] = sz
							if err == nil {
								if st.ltAsk == 0 || ap <= st.ltAsk {
									st.ltAsk, st.ltAskSz = ap, sz
								}
								if st.ltBid > 0 && ap <= st.ltBid {
									for bpStr := range st.bookBids {
										if bp, e2 := strconv.ParseFloat(bpStr, 64); e2 == nil && bp >= ap {
											delete(st.bookBids, bpStr)
										}
									}
									needRecompute = true
								}
							}
						}
					}
					nowOB := time.Now()
					nowMS := nowOB.UnixMilli()
					sampleRec := st.ltBid > 0 && st.ltAsk > st.ltBid && nowMS-st.lastRecOBMS >= 100
					if needRecompute || sampleRec {
						recomputeBBO(st)
					}
					if st.ltBid > 0 && st.ltAsk > 0 {
						st.lastBookTS = nowOB
					}
					bboChanged := st.ltBid != prevB || st.ltAsk != prevA
					if sampleRec && (bboChanged || nowMS-st.lastRecOBMS >= 500) {
						st.lastRecOBMS = nowMS
						pushL2Rec(fmt.Sprintf(`{"t":%d,"k":"ob","s":%q,"b":%.6g,"bs":%.6g,"a":%.6g,"as":%.6g,"bd":%.1f,"ad":%.1f}`,
							nowMS, sym, st.ltBid, st.ltBidSz, st.ltAsk, st.ltAskSz, st.ltBidDepthUSD, st.ltAskDepthUSD))
					}
					st.mu.Unlock()
					if enableAutoTwins && isTwinSym(sym) && bboChanged {
						manageAllTwins()
					}
					if bboChanged && isActiveOpenSym(sym) {
						manageActiveOpenStrat()
					}
					if bboChanged && isActiveCarbonSym(sym) {
						manageCarbonBracket()
					}
				}
			}
		}
		wsMu.Lock()
		ltConn = nil
		wsMu.Unlock()
		_ = conn.Close()
		time.Sleep(1 * time.Second)
	}
}

func runBnGuardWS() {
	url := "wss://fstream.binance.com/stream?streams=paxgusdt@bookTicker/btcusdt@bookTicker/ethusdt@bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var env struct {
				Data struct {
					S  string `json:"s"`
					B  string `json:"b"`
					BQ string `json:"B"`
					A  string `json:"a"`
					AQ string `json:"A"`
				} `json:"data"`
			}
			if json.Unmarshal(msg, &env) != nil || env.Data.S == "" {
				continue
			}
			sym := strings.TrimSuffix(env.Data.S, "USDT")
			st := mktStates[sym]
			if st == nil {
				continue
			}
			b, _ := strconv.ParseFloat(env.Data.B, 64)
			a, _ := strconv.ParseFloat(env.Data.A, 64)
			if b > 0 && a > 0 {
				mid := 0.5 * (b + a)
				now := time.Now()
				st.mu.Lock()
				st.bnMid = mid
				if st.emaUltra == 0 || st.lastEmaTS.IsZero() {
					st.emaUltra = mid
				} else {
					dt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastEmaTS).Seconds()))
					st.emaUltra += (1.0 - math.Exp(-dt/3.0)) * (mid - st.emaUltra)
				}
				st.lastEmaTS = now
				st.mu.Unlock()
			}
		}
		_ = conn.Close()
		time.Sleep(1 * time.Second)
	}
}

func bnBasisGuardOK(tw *TwinState, tMid float64, now time.Time) (bool, bool, float64) {
	btcSt := mktStates["BTC"]
	var btcBasisBps float64
	btcFresh := false
	btcShock := false
	if btcSt != nil {
		btcSt.mu.Lock()
		bnB, euB, tsB := btcSt.bnMid, btcSt.emaUltra, btcSt.lastEmaTS
		ltB, ltA := btcSt.ltBid, btcSt.ltAsk
		btcSt.mu.Unlock()
		if bnB > 0 && !tsB.IsZero() && now.Sub(tsB) < 15*time.Second {
			btcFresh = true
			if euB > 0 && math.Abs(bnB-euB)/euB*10000.0 >= 8.0 {
				btcShock = true
			}
			if ltB > 0 && ltA > ltB {
				btcBasisBps = (0.5*(ltB+ltA) - bnB) / bnB * 10000.0
			}
		}
	}
	if btcShock {
		return false, false, btcBasisBps
	}
	if tw.ThinSym == "PAXG" {
		paxgSt := mktStates["PAXG"]
		if paxgSt == nil {
			return false, false, 0.0
		}
		paxgSt.mu.Lock()
		bnP, tsP := paxgSt.bnMid, paxgSt.lastEmaTS
		paxgSt.mu.Unlock()
		if bnP <= 0 || tsP.IsZero() || now.Sub(tsP) >= 15*time.Second || tMid <= 0 {
			return false, false, 0.0
		}
		diffBps := (tMid - bnP) / bnP * 10000.0
		return diffBps <= 2.0, diffBps >= -2.0, diffBps
	}
	if !btcFresh {
		return false, false, 0.0
	}
	ok := math.Abs(btcBasisBps) < 2.0
	return ok, ok, btcBasisBps
}

func runBnWS() {
	url := "wss://fstream.binance.com/stream?streams=googlusdt@bookTicker/googlusdt@kline_1m/amznusdt@bookTicker/amznusdt@kline_1m/metausdt@bookTicker/metausdt@kline_1m/soxlusdt@bookTicker/soxlusdt@kline_1m/muusdt@bookTicker/muusdt@kline_1m/zecusdt@bookTicker/zecusdt@kline_1m/hypeusdt@bookTicker/hypeusdt@kline_1m"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var env struct {
				Stream string `json:"stream"`
				Data   struct {
					S  string `json:"s"`
					B  string `json:"b"`
					BQ string `json:"B"`
					A  string `json:"a"`
					AQ string `json:"A"`
					K  struct {
						T int64  `json:"t"`
						O string `json:"o"`
						H string `json:"h"`
						L string `json:"l"`
						C string `json:"c"`
						V string `json:"v"`
						X bool   `json:"x"`
					} `json:"k"`
				} `json:"data"`
			}
			if json.Unmarshal(msg, &env) != nil || env.Data.S == "" {
				continue
			}
			sym := strings.TrimSuffix(env.Data.S, "USDT")
			st := mktStates[sym]
			if st == nil {
				continue
			}
			if strings.HasSuffix(env.Stream, "@kline_1m") {
				op, _ := strconv.ParseFloat(env.Data.K.O, 64)
				hi, _ := strconv.ParseFloat(env.Data.K.H, 64)
				lo, _ := strconv.ParseFloat(env.Data.K.L, 64)
				cl, _ := strconv.ParseFloat(env.Data.K.C, 64)
				vol, _ := strconv.ParseFloat(env.Data.K.V, 64)
				mSlot := env.Data.K.T / 60000
				if mSlot > 0 && op > 0 && cl > 0 {
					mIdx := int(((mSlot % 128) + 128) % 128)
					st.mu.Lock()
					st.minSlot[mIdx] = mSlot
					st.minPx[mIdx] = op
					st.minHi[mIdx] = hi
					st.minLo[mIdx] = lo
					st.minCl[mIdx] = cl
					st.minVol[mIdx] = vol
					closedNow := false
					if env.Data.K.X {
						if mSlot > st.lastClosedMin {
							st.lastClosedMin = mSlot
						}
						closedNow = st.bnMid > 0 && (st.dynClipUSD >= 0 || math.Abs(st.pos) > 1e-6)
					}
					st.mu.Unlock()
					if closedNow {
						manageCoin(sym)
					}
				}
				continue
			}
			b, _ := strconv.ParseFloat(env.Data.B, 64)
			bq, _ := strconv.ParseFloat(env.Data.BQ, 64)
			a, _ := strconv.ParseFloat(env.Data.A, 64)
			aq, _ := strconv.ParseFloat(env.Data.AQ, 64)
			if b > 0 && a > 0 && bq > 0 && aq > 0 {
				mVal := 0.5 * (b + a)
				microVal := (b*aq + a*bq) / (bq + aq)
				obiVal := (bq - aq) / (bq + aq)
				now := time.Now()
				st.mu.Lock()
				st.bnMid = mVal
				st.bnMicro = microVal
				st.rawObi = obiVal
				st.netMicroUSD.Store(int64(st.pos * mVal * 1e6))
				// Update 60s realized volatility bucket (6 x 10s sliding slots)
				slot := now.Unix() / 10
				idx := int(slot % 6)
				if st.rangeBuckets[idx].slot != slot {
					st.rangeBuckets[idx] = RangeBucket{slot: slot, hi: mVal, lo: mVal}
				} else {
					if mVal > st.rangeBuckets[idx].hi {
						st.rangeBuckets[idx].hi = mVal
					}
					if mVal < st.rangeBuckets[idx].lo {
						st.rangeBuckets[idx].lo = mVal
					}
				}
				if st.emaFast == 0 {
					st.emaUltra = mVal
					st.emaFast = mVal
					st.emaEq = mVal
					st.emaSlow = mVal
					st.ema5m = mVal
					st.emaMacro = mVal
					st.fastObi = obiVal
					st.bnObi = obiVal
					st.lastEmaTS = now
				} else {
					dt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastEmaTS).Seconds()))
					st.lastEmaTS = now
					st.emaUltra += (1.0 - math.Exp(-dt/3.0)) * (mVal - st.emaUltra)
					st.emaFast += (1.0 - math.Exp(-dt/20.0)) * (mVal - st.emaFast)
					if st.emaEq == 0 {
						st.emaEq = mVal
					} else {
						st.emaEq += (1.0 - math.Exp(-dt/90.0)) * (mVal - st.emaEq)
					}
					st.emaSlow += (1.0 - math.Exp(-dt/180.0)) * (mVal - st.emaSlow)
					if st.ema5m == 0 {
						st.ema5m = mVal
					} else {
						st.ema5m += (1.0 - math.Exp(-dt/300.0)) * (mVal - st.ema5m)
					}
					if st.emaMacro == 0 {
						st.emaMacro = mVal
					} else {
						st.emaMacro += (1.0 - math.Exp(-dt/900.0)) * (mVal - st.emaMacro)
					}
					st.fastObi += (1.0 - math.Exp(-dt/2.0)) * (obiVal - st.fastObi)
					st.bnObi += (1.0 - math.Exp(-dt/8.0)) * (obiVal - st.bnObi) // Stable 8s OBI prevents 1ms queue churn!
				}
				// CPU saver: run 0ms full tick on active Champion/open pos, 100ms throttled tick on Shadow coins
				shouldRun := st.dynClipUSD >= 0 || math.Abs(st.pos) > 1e-6 || now.Sub(st.lastShadowTS) >= 100*time.Millisecond
				st.mu.Unlock()
				if shouldRun {
					manageCoin(sym)
				}
			}
		}
		_ = conn.Close()
		time.Sleep(1 * time.Second)
	}
}

func seedBinanceHistory() {
	hc := &http.Client{Timeout: 3 * time.Second}
	now := time.Now()
	slot := now.Unix() / 10
	idx := int(slot % 6)
	for _, sym := range []string{"GOOGL", "AMZN", "META", "SOXL", "MU", "ZEC", "HYPE"} {
		st := mktStates[sym]
		if st == nil {
			continue
		}
		resp, err := hc.Get("https://fapi.binance.com/fapi/v1/klines?symbol=" + sym + "USDT&interval=1m&limit=90")
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var kl [][]json.RawMessage
		if json.Unmarshal(body, &kl) != nil || len(kl) == 0 {
			continue
		}
		type minSeed struct {
			slot int64
			px   float64
			hi   float64
			lo   float64
			cl   float64
			vol  float64
		}
		var seeds []minSeed
		var ef, es, em, eu, eeq, e5m, lastC, lastHi, lastLo float64
		for _, k := range kl {
			if len(k) < 6 {
				continue
			}
			var openMS int64
			var oStr, hStr, lStr, cStr, vStr string
			_ = json.Unmarshal(k[0], &openMS)
			_ = json.Unmarshal(k[1], &oStr)
			_ = json.Unmarshal(k[2], &hStr)
			_ = json.Unmarshal(k[3], &lStr)
			_ = json.Unmarshal(k[4], &cStr)
			_ = json.Unmarshal(k[5], &vStr)
			op, _ := strconv.ParseFloat(oStr, 64)
			hi, _ := strconv.ParseFloat(hStr, 64)
			lo, _ := strconv.ParseFloat(lStr, 64)
			c, _ := strconv.ParseFloat(cStr, 64)
			vol, _ := strconv.ParseFloat(vStr, 64)
			if c <= 0 {
				continue
			}
			if openMS > 0 && op > 0 {
				seeds = append(seeds, minSeed{slot: openMS / 60000, px: op, hi: hi, lo: lo, cl: c, vol: vol})
			}
			lastC, lastHi, lastLo = c, hi, lo
			if ef == 0 {
				ef, es, em, eu, eeq, e5m = c, c, c, c, c, c
			} else {
				eu += (1.0 - math.Exp(-60.0/3.0)) * (c - eu)
				ef += (1.0 - math.Exp(-60.0/20.0)) * (c - ef)
				eeq += (1.0 - math.Exp(-60.0/90.0)) * (c - eeq)
				es += (1.0 - math.Exp(-60.0/180.0)) * (c - es)
				e5m += (1.0 - math.Exp(-60.0/300.0)) * (c - e5m)
				em += (1.0 - math.Exp(-60.0/900.0)) * (c - em)
			}
		}
		if lastC > 0 {
			st.mu.Lock()
			for _, ms := range seeds {
				mi := int(((ms.slot % 128) + 128) % 128)
				st.minSlot[mi] = ms.slot
				st.minPx[mi] = ms.px
				st.minHi[mi] = ms.hi
				st.minLo[mi] = ms.lo
				st.minCl[mi] = ms.cl
				st.minVol[mi] = ms.vol
			}
			if len(seeds) >= 2 {
				st.lastClosedMin = seeds[len(seeds)-2].slot
				st.lastSigMin = st.lastClosedMin
			}
			curM := now.Unix() / 60
			curMIdx := int(((curM % 128) + 128) % 128)
			if st.minSlot[curMIdx] != curM {
				st.minSlot[curMIdx] = curM
				st.minPx[curMIdx] = lastC
				st.minHi[curMIdx] = lastHi
				st.minLo[curMIdx] = lastLo
				st.minCl[curMIdx] = lastC
			}
			st.emaUltra = eu
			st.emaFast = ef
			st.emaEq = eeq
			st.emaSlow = es
			st.ema5m = e5m
			st.emaMacro = em
			st.bnMid = lastC
			st.bnMicro = lastC
			st.lastEmaTS = now
			if lastHi > 0 && lastLo > 0 {
				st.rangeBuckets[idx] = RangeBucket{slot: slot, hi: lastHi, lo: lastLo}
				st.range60sBps = (lastHi - lastLo) / lastC * 10000.0
			}
			st.mu.Unlock()
		}
	}
}

func cancelPairSide(sym, side string) {
	st := mktStates[sym]
	if st == nil {
		return
	}
	st.mu.Lock()
	cancelSide(sym, side)
	st.mu.Unlock()
}

func modifyPairSide(sym, side string, newPxF, midF float64) {
	st := mktStates[sym]
	if st == nil {
		return
	}
	st.mu.Lock()
	st.lastOrdTS[side] = time.Now()
	modifySide(sym, side, newPxF, midF)
	st.mu.Unlock()
}

func placePairOrder(sym, side string, pxF, midF, baseF float64, isClose, isIOC bool) {
	st := mktStates[sym]
	m, ok := markets[sym]
	if st == nil || !ok || pxF <= 0 || baseF <= 0 {
		return
	}
	if !isClose {
		baseF = math.Max(m.MinB, baseF)
	}
	baseInt := int64(math.Round(baseF * math.Pow10(m.SzDec)))
	if !isClose && !isIOC {
		baseInt = int64(math.Ceil(baseF * math.Pow10(m.SzDec)))
	}
	if baseInt <= 0 {
		return
	}
	pxInt := uint32(math.Round(pxF * math.Pow10(m.PxDec)))
	coi := coiSeq.Add(1)
	var isAsk, redOnly uint8
	if side == "ASK" {
		isAsk = 1
	}
	if isClose {
		redOnly = 1
	}
	now := time.Now()
	st.mu.Lock()
	st.lastOrdTS[side] = now
	st.activeOrds[side] = &ActiveOrder{COI: coi, Px: pxF, BaseInt: baseInt, RefMid: midF, IsClose: isClose, IsIOC: isIOC, TS: now}
	if !isClose && !isIOC {
		ordMicro := int64(baseF * pxF * 1e6)
		if side == "BID" {
			st.pendBidMicroUSD.Store(ordMicro)
		} else {
			st.pendAskMicroUSD.Store(ordMicro)
		}
	}
	st.mu.Unlock()

	enqueueTx(func() {
		var ordType uint8 = 0
		var tif uint8 = 2
		if isClose {
			tif = 1 // GoodTillTime on Maker closes: crosses immediately at 0% fee if already converged!
		}
		exp := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
		if isIOC {
			ordType = 1 // MARKET
			tif = 0     // IOC
			exp = 0
		}
		dbg := fmt.Sprintf("PAIR_%s_%s px=%.2f sz=%.6f ro=%d ioc=%v", sym, side, pxF, baseF, redOnly, isIOC)
		err := sendSignedTx(fmt.Sprintf("po_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      m.ID,
				ClientOrderIndex: coi,
				BaseAmount:       baseInt,
				Price:            pxInt,
				IsAsk:            isAsk,
				Type:             ordType,
				TimeInForce:      tif,
				ReduceOnly:       redOnly,
				OrderExpiry:      exp,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
		if err != nil {
			st.mu.Lock()
			delete(st.activeOrds, side)
			if side == "BID" {
				st.pendBidMicroUSD.Store(0)
			} else {
				st.pendAskMicroUSD.Store(0)
			}
			st.mu.Unlock()
		}
	})
}

func runTwinWalkForwardLoop() {
	t := time.NewTicker(15 * time.Minute)
	defer t.Stop()
	for range t.C {
		syncTwinWalkForward()
	}
}

func syncTwinWalkForward() {
	hc := &http.Client{Timeout: 8 * time.Second}
	fetch1hCandles := func(mid int64, startS, endS int64) map[int64]float64 {
		out := map[int64]float64{}
		cur := endS
		for cur > startS {
			st := cur - 480*3600
			if st < startS {
				st = startS
			}
			time.Sleep(220 * time.Millisecond)
			u := fmt.Sprintf("%s/api/v1/candles?market_id=%d&resolution=1h&start_timestamp=%d&end_timestamp=%d&count_back=500", BaseURL, mid, st, cur)
			req, err := http.NewRequest("GET", u, nil)
			if err != nil {
				break
			}
			req.Header.Set("User-Agent", "Mozilla/5.0")
			resp, err := hc.Do(req)
			if err != nil {
				cur = st - 3600
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var d struct {
				C []struct {
					T int64   `json:"t"`
					C float64 `json:"c"`
				} `json:"c"`
			}
			if json.Unmarshal(body, &d) != nil || len(d.C) == 0 {
				cur = st - 3600
				continue
			}
			minT := cur
			for _, bar := range d.C {
				t := bar.T
				if t > 1e11 {
					t /= 1000
				}
				if t < minT {
					minT = t
				}
				if bar.C > 0 {
					out[t] = bar.C
				}
			}
			nextCur := st
			if minT < nextCur {
				nextCur = minT
			}
			cur = nextCur - 3600
		}
		return out
	}
	fetch1hFundings := func(mid int64, startS, endS int64) map[int64]float64 {
		out := map[int64]float64{}
		cur := endS
		for cur > startS {
			st := cur - 550*3600
			if st < startS {
				st = startS
			}
			time.Sleep(220 * time.Millisecond)
			u := fmt.Sprintf("%s/api/v1/fundings?market_id=%d&resolution=1h&start_timestamp=%d&end_timestamp=%d&count_back=600", BaseURL, mid, st, cur)
			req, err := http.NewRequest("GET", u, nil)
			if err != nil {
				break
			}
			req.Header.Set("User-Agent", "Mozilla/5.0")
			resp, err := hc.Do(req)
			if err != nil {
				cur = st - 3600
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var d struct {
				Fundings []struct {
					Timestamp int64  `json:"timestamp"`
					Rate      string `json:"rate"`
					Direction string `json:"direction"`
				} `json:"fundings"`
			}
			if json.Unmarshal(body, &d) != nil || len(d.Fundings) == 0 {
				cur = st - 3600
				continue
			}
			minT := cur
			for _, f := range d.Fundings {
				t := f.Timestamp
				if t > 1e11 {
					t /= 1000
				}
				if t < minT {
					minT = t
				}
				r, _ := strconv.ParseFloat(f.Rate, 64)
				r /= 100.0
				if f.Direction != "long" {
					r = -r
				}
				out[t] = r
			}
			nextCur := st
			if minT < nextCur {
				nextCur = minT
			}
			cur = nextCur - 3600
		}
		return out
	}

	nowH := (time.Now().Unix()/3600 - 1) * 3600
	start32d := nowH - 32*86400
	for _, tw := range twins {
		tM, dM := markets[tw.ThinSym], markets[tw.DeepSym]
		tc, dc := fetch1hCandles(tM.ID, start32d, nowH), fetch1hCandles(dM.ID, start32d, nowH)
		tf, df := fetch1hFundings(tM.ID, start32d, nowH), fetch1hFundings(dM.ID, start32d, nowH)
		var ts []int64
		for t, tv := range tc {
			if dv, ok := dc[t]; ok && tv > 0 && dv > 0 {
				ts = append(ts, t)
			}
		}
		sort.Slice(ts, func(i, j int) bool { return ts[i] < ts[j] })
		if len(ts) < 60 {
			fmt.Printf("[WF_SEED] %s SKIP insufficient 1h bars (%d)\n", tw.Name, len(ts))
			continue
		}
		ratios := make([]float64, len(ts))
		for i, t := range ts {
			ratios[i] = tc[t] / dc[t]
		}
		bestLB := 48
		bestZ := 2.2
		bestSR := -999.0
		trCut := nowH - 28*86400
		for _, lb := range []int{24, 48} {
			for _, zTh := range []float64{1.8, 2.2, 2.6} {
				dayPnl := map[int64]float64{}
				inPos := false
				posDir := 0.0
				entryR := 0.0
				entryT := int64(0)
				accFund := 0.0
				for i := lb; i < len(ts); i++ {
					t := ts[i]
					if t < trCut {
						continue
					}
					sum, sum2 := 0.0, 0.0
					for k := i - lb; k < i; k++ {
						sum += ratios[k]
						sum2 += ratios[k] * ratios[k]
					}
					mu := sum / float64(lb)
					varR := math.Max(1e-16, sum2/float64(lb)-mu*mu)
					sd := math.Max(mu*0.0002, math.Sqrt(varR))
					r := ratios[i]
					z := (r - mu) / sd
					fDiff := (tf[t] - df[t]) * 10000.0
					dk := t / 86400
					if _, exists := dayPnl[dk]; !exists {
						dayPnl[dk] = 0.0
					}
					if inPos {
						accFund += -posDir * (tf[t] - df[t]) * 600.0
						nbps := posDir*(r/entryR-1.0)*10000.0 - 2.4
						if posDir*z >= -0.1 || nbps <= -20.0 || t-entryT >= 16*3600 {
							dayPnl[dk] += nbps/10000.0*600.0 + accFund
							inPos = false
						}
					} else {
						rthOK := isUSRthUTC(time.Unix(t, 0))
						if tw.Name == "PAXG/XAU" {
							rthOK = isGoldActiveUTC(time.Unix(t, 0))
						}
						if rthOK && math.Abs((r/mu-1.0)*10000.0) >= tw.MinGapBps {
							if z <= -zTh && fDiff <= 2.0 {
								inPos, posDir, entryR, entryT, accFund = true, 1.0, r, t, 0.0
							} else if z >= zTh && fDiff >= -2.0 {
								inPos, posDir, entryR, entryT, accFund = true, -1.0, r, t, 0.0
							}
						}
					}
				}
				if len(dayPnl) >= 10 {
					nD := float64(len(dayPnl))
					sP := 0.0
					for _, v := range dayPnl {
						sP += v
					}
					mD := sP / nD
					vD := 0.0
					for _, v := range dayPnl {
						d := v - mD
						vD += d * d
					}
					sr := mD / math.Max(1e-6, math.Sqrt(vD/nD))
					if sr > bestSR {
						bestSR = sr
						bestLB = lb
						bestZ = zTh
					}
				}
			}
		}
		if len(ratios) <= bestLB {
			bestLB = 24
		}
		sum, sum2 := 0.0, 0.0
		for i := len(ratios) - bestLB; i < len(ratios); i++ {
			sum += ratios[i]
			sum2 += ratios[i] * ratios[i]
		}
		mu := sum / float64(bestLB)
		sd := math.Max(mu*0.00025, math.Sqrt(math.Max(1e-16, sum2/float64(bestLB)-mu*mu)))
		var lastFT, lastFD float64
		var maxFT, maxFD int64
		for ft, fv := range tf {
			if ft > maxFT {
				maxFT, lastFT = ft, fv
			}
		}
		for fd, fv := range df {
			if fd > maxFD {
				maxFD, lastFD = fd, fv
			}
		}
		fDiffBps := (lastFT - lastFD) * 10000.0
		pairMu.Lock()
		tw.WfLB = bestLB
		tw.WfZ = bestZ
		tw.HourlyMean = mu
		tw.HourlyStd = sd
		tw.FundDiffBps = fDiffBps
		tw.WfReady = true
		tw.LastWfSync = time.Now()
		tw.EmaRatio = mu
		tw.LastEmaTS = time.Now()
		tw.WarmTicks = 12
		pairMu.Unlock()
		fmt.Printf("[WF_SEED] %s lb=%dh zTh=%.1f mean=%.6f stdBps=%.2f fDiff=%+.2fbps/h (%d bars)\n",
			tw.Name, bestLB, bestZ, mu, sd/mu*10000.0, fDiffBps, len(ts))
	}
}

func fireTwinIOC(tw *TwinState, thinSide string, thinPx, thinMid, thinSz float64, deepSide string, deepPx, deepMid, deepSz float64, isClose bool) {
	tSt, dSt := mktStates[tw.ThinSym], mktStates[tw.DeepSym]
	tM, dM := markets[tw.ThinSym], markets[tw.DeepSym]
	if tSt == nil || dSt == nil || thinPx <= 0 || deepPx <= 0 || thinSz <= 0 || deepSz <= 0 {
		return
	}
	if !isClose {
		thinSz = math.Max(tM.MinB, thinSz)
		deepSz = math.Max(dM.MinB, deepSz)
	}
	tBaseInt := int64(math.Round(thinSz * math.Pow10(tM.SzDec)))
	dBaseInt := int64(math.Round(deepSz * math.Pow10(dM.SzDec)))
	if tBaseInt <= 0 || dBaseInt <= 0 {
		return
	}
	tPxInt := uint32(math.Round(thinPx * math.Pow10(tM.PxDec)))
	dPxInt := uint32(math.Round(deepPx * math.Pow10(dM.PxDec)))
	var tAsk, dAsk, redOnly uint8
	if thinSide == "ASK" {
		tAsk = 1
	}
	if deepSide == "ASK" {
		dAsk = 1
	}
	if isClose {
		redOnly = 1
	}
	coiT := coiSeq.Add(1)
	coiD := coiSeq.Add(1)
	now := time.Now()

	tSt.mu.Lock()
	tSt.lastOrdTS[thinSide] = now
	tSt.activeOrds[thinSide] = &ActiveOrder{COI: coiT, Px: thinPx, BaseInt: tBaseInt, RefMid: thinMid, IsClose: isClose, IsIOC: true, TS: now}
	tSt.mu.Unlock()

	dSt.mu.Lock()
	dSt.lastOrdTS[deepSide] = now
	dSt.activeOrds[deepSide] = &ActiveOrder{COI: coiD, Px: deepPx, BaseInt: dBaseInt, RefMid: deepMid, IsClose: isClose, IsIOC: true, TS: now}
	dSt.mu.Unlock()

	enqueueTx(func() {
		txMu.Lock()
		defer txMu.Unlock()
		t0 := time.Now()
		tiT, errT := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      tM.ID,
			ClientOrderIndex: coiT,
			BaseAmount:       tBaseInt,
			Price:            tPxInt,
			IsAsk:            tAsk,
			Type:             1,
			TimeInForce:      0,
			ReduceOnly:       redOnly,
			OrderExpiry:      0,
		}, nextOpts())
		if errT != nil {
			fmt.Printf("[TWIN_BATCH_SIGN_ERR_T] %v\n", errT)
			return
		}
		infoT, errT := tiT.GetTxInfo()
		if errT != nil {
			fmt.Printf("[TWIN_BATCH_INFO_ERR_T] %v\n", errT)
			return
		}
		tiD, errD := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      dM.ID,
			ClientOrderIndex: coiD,
			BaseAmount:       dBaseInt,
			Price:            dPxInt,
			IsAsk:            dAsk,
			Type:             1,
			TimeInForce:      0,
			ReduceOnly:       redOnly,
			OrderExpiry:      0,
		}, nextOpts())
		if errD != nil {
			fmt.Printf("[TWIN_BATCH_SIGN_ERR_D] %v\n", errD)
			return
		}
		infoD, errD := tiD.GetTxInfo()
		if errD != nil {
			fmt.Printf("[TWIN_BATCH_INFO_ERR_D] %v\n", errD)
			return
		}

		idT := fmt.Sprintf("dt_%d", coiT)
		idD := fmt.Sprintf("dd_%d", coiD)
		if useWsSend {
			txWsMu.Lock()
			tc := txConn
			var wsErrT, wsErrD error
			if tc != nil {
				wsErrT = tc.WriteJSON(map[string]any{
					"type": "jsonapi/sendtx",
					"data": map[string]any{"id": idT, "tx_type": tiT.GetTxType(), "tx_info": json.RawMessage(infoT)},
				})
				if wsErrT == nil {
					wsErrD = tc.WriteJSON(map[string]any{
						"type": "jsonapi/sendtx",
						"data": map[string]any{"id": idD, "tx_type": tiD.GetTxType(), "tx_info": json.RawMessage(infoD)},
					})
				}
			}
			txWsMu.Unlock()
			if tc != nil && wsErrT == nil && wsErrD == nil {
				fmt.Printf("[WS_TWIN_BATCH %s_%s/%s_%s px=%.2f/%.2f sz=%.5f/%.5f ro=%d] %dus\n",
					tw.ThinSym, thinSide, tw.DeepSym, deepSide, thinPx, deepPx, thinSz, deepSz, redOnly, time.Since(t0).Microseconds())
				return
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		postLeg := func(lid string, tt uint8, tiStr string) {
			defer wg.Done()
			form := url.Values{}
			form.Set("tx_type", strconv.Itoa(int(tt)))
			form.Set("tx_info", tiStr)
			if resp, e := txHTTP.PostForm(BaseURL+"/api/v1/sendTx", form); e == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || !strings.Contains(string(b), `"code":200`) {
					fmt.Printf("[HTTP_TWIN_ERR %s] %d %s\n", lid, resp.StatusCode, string(b))
				}
			}
		}
		go postLeg(idT, tiT.GetTxType(), infoT)
		go postLeg(idD, tiD.GetTxType(), infoD)
		wg.Wait()
		fmt.Printf("[HTTP_TWIN_BATCH %s/%s ro=%d] %dms\n", tw.ThinSym, tw.DeepSym, redOnly, time.Since(t0).Milliseconds())
	})
}

func manageAllTwins() {
	if !enableAutoTwins {
		return
	}
	pairMu.Lock()
	defer pairMu.Unlock()
	anyActive := false
	for _, tw := range twins {
		if manageTwin(tw) {
			anyActive = true
		}
	}
	pairMarketActive.Store(anyActive)
}

func manageTwin(tw *TwinState) bool {
	tSt, dSt := mktStates[tw.ThinSym], mktStates[tw.DeepSym]
	tM, dM := markets[tw.ThinSym], markets[tw.DeepSym]
	if tSt == nil || dSt == nil {
		return false
	}
	now := time.Now()

	tSt.mu.Lock()
	for _, sd := range []string{"BID", "ASK"} {
		if ao := tSt.activeOrds[sd]; ao != nil && ao.IsIOC && now.Sub(ao.TS).Seconds() >= 2.0 {
			delete(tSt.activeOrds, sd)
		}
	}
	tb, tbSz, ta, taSz, tBidD, tAskD, tPos, tAE, tBookTS := tSt.ltBid, tSt.ltBidSz, tSt.ltAsk, tSt.ltAskSz, tSt.ltBidDepthUSD, tSt.ltAskDepthUSD, tSt.pos, tSt.avgEntry, tSt.lastBookTS
	tBidOrd, tAskOrd := tSt.activeOrds["BID"], tSt.activeOrds["ASK"]
	tLastBid, tLastAsk := tSt.lastOrdTS["BID"], tSt.lastOrdTS["ASK"]
	tSt.mu.Unlock()

	dSt.mu.Lock()
	for _, sd := range []string{"BID", "ASK"} {
		if ao := dSt.activeOrds[sd]; ao != nil && ao.IsIOC && now.Sub(ao.TS).Seconds() >= 2.0 {
			delete(dSt.activeOrds, sd)
		}
	}
	db, dbSz, da, daSz, dBidD, dAskD, dPos, dAE, dBookTS := dSt.ltBid, dSt.ltBidSz, dSt.ltAsk, dSt.ltAskSz, dSt.ltBidDepthUSD, dSt.ltAskDepthUSD, dSt.pos, dSt.avgEntry, dSt.lastBookTS
	dSt.mu.Unlock()

	cancelThinEntries := func() {
		if tBidOrd != nil && !tBidOrd.IsClose && !tBidOrd.IsIOC {
			cancelPairSide(tw.ThinSym, "BID")
		}
		if tAskOrd != nil && !tAskOrd.IsClose && !tAskOrd.IsIOC {
			cancelPairSide(tw.ThinSym, "ASK")
		}
	}

	validBooks := tb > 0 && ta > tb && db > 0 && da > db
	booksFresh := validBooks && !tBookTS.IsZero() && !dBookTS.IsZero() && now.Sub(tBookTS) < 20*time.Second && now.Sub(dBookTS) < 20*time.Second
	if !validBooks {
		return false
	}
	tMid, dMid := 0.5*(tb+ta), 0.5*(db+da)
	tSpBps, dSpBps := (ta-tb)/tMid*10000.0, (da-db)/dMid*10000.0
	tSt.netMicroUSD.Store(int64(tPos * tMid * 1e6))
	dSt.netMicroUSD.Store(int64(dPos * dMid * 1e6))

	refMean := tw.EmaRatio
	if tw.WfReady && tw.HourlyMean > 0 {
		refMean = tw.HourlyMean
	} else if refMean <= 0 {
		refMean = tw.FallbackRatio
	}
	refStd := math.Max(refMean*0.00025, tw.HourlyStd)
	instR := tMid / dMid
	liveZ := (instR - refMean) / refStd
	devBps := (instR/refMean - 1.0) * 10000.0

	curEq := float64(eqNow.Load()) / 1e6
	if curEq < 50.0 {
		curEq = 93.79
	}

	tUSD, dUSD := tPos*tMid, dPos*dMid
	netDelta := tUSD + dUSD
	if math.Abs(netDelta) < 11.5 {
		tw.UnhedgedSince = time.Time{}
	} else if tw.UnhedgedSince.IsZero() {
		tw.UnhedgedSince = now
	}
	if math.Abs(tPos) < tM.MinB && math.Abs(dPos) < dM.MinB {
		if tw.WasHedged && tw.TradeID > 0 {
			holdS := 0.0
			if !tw.EntryTS.IsZero() {
				holdS = now.Sub(tw.EntryTS).Seconds()
			}
			estBps := math.Max(2.5, tw.MinGapBps*0.5)
			recordStratClose(tw.TradeID, "TWIN_EQUALIZE", tw.Name, tw.LegUSD, holdS, estBps, tw.LegUSD*estBps/10000.0, "FLAT_LIMIT_TP")
			tw.TradeID = 0
		}
		tw.WasHedged = false
		tw.EntryTS = time.Time{}
		tw.EntryEqUSD = 0
		tw.AccruedFundBps = 0
		tw.LastFundAccrueTS = time.Time{}
		tw.TradeID = 0
	} else if math.Abs(tUSD) >= 11.0 && math.Abs(dUSD) >= 11.0 && tUSD*dUSD < 0 {
		tw.WasHedged = true
		if tw.EntryTS.IsZero() {
			tw.EntryTS = now
			tw.EntryEqUSD = curEq
			tw.AccruedFundBps = 0
			tw.LastFundAccrueTS = now
		}
	}

	netCarryBpsHr := 0.0
	if tPos > tM.MinB {
		netCarryBpsHr = -tw.FundDiffBps
	} else if tPos < -tM.MinB {
		netCarryBpsHr = tw.FundDiffBps
	}
	if tw.WasHedged && !tw.LastFundAccrueTS.IsZero() {
		dtH := math.Min(0.25, math.Max(0.0, now.Sub(tw.LastFundAccrueTS).Hours()))
		tw.LastFundAccrueTS = now
		tw.AccruedFundBps += netCarryBpsHr * dtH
	}

	pairAllowed := isUSRthUTC(now)
	if tw.Name == "PAXG/XAU" {
		pairAllowed = isGoldActiveUTC(now)
	}
	bnLongOk, bnShortOk, bnDiffBps := bnBasisGuardOK(tw, tMid, now)
	if pairAllowed && now.Sub(tw.LastLog) >= 30*time.Second {
		tw.LastLog = now
		fmt.Printf("[%s] GO-TOKYO v90.0 TWIN | %s Eq=$%.4f | Z=%+.2f/%.1f dev=%+.1fbps bnDiff=%+.2fbps(L=%v/S=%v) netCarry=%+.2fbps/h | Pos=%.4f($%.0f)/%.4f($%.0f) | Sp=%.2f/%.2f | L2Rec=%d\n",
			now.Format("15:04:05"), tw.Name, float64(eqNow.Load())/1e6, liveZ, tw.WfZ, devBps, bnDiffBps, bnLongOk, bnShortOk, netCarryBpsHr, tPos, tUSD, dPos, dUSD, tSpBps, dSpBps, l2TicksRecorded.Load())
	}

	// STEP A: <=500ms Asymmetric Fill Guard (Cancel + Immediate ReduceOnly Market Flatten on any lone open leg!)
	if math.Abs(netDelta) >= 11.5 {
		unhedgedMS := now.Sub(tw.UnhedgedSince).Milliseconds()
		if unhedgedMS >= 450 && now.Sub(tw.LastHedgeTS) >= 450*time.Millisecond && takeWriteSlotTier(2) {
			tw.LastHedgeTS = now
			tw.LastDualSendTS = now
			tw.CoolUntil = now.Add(45 * time.Second)
			if tw.TradeID > 0 {
				recordStratClose(tw.TradeID, "TWIN_EQUALIZE", tw.Name, tw.LegUSD, float64(unhedgedMS)/1000.0, -2.0, -tw.LegUSD*0.0002, "ASYM_LEG_GUARD")
				tw.TradeID = 0
			}
			cancelThinEntries()
			fmt.Printf("[%s] [TWIN-UNHEDGED-500MS] %s flattening lone leg after %dms (tPos=%.5f dPos=%.5f)\n",
				now.Format("15:04:05"), tw.Name, unhedgedMS, tPos, dPos)
			if math.Abs(tPos) >= tM.MinB {
				if tPos > 0 {
					placePairOrder(tw.ThinSym, "ASK", roundTo(tb*0.9992, tM.PxDec), tMid, math.Abs(tPos), true, true)
				} else {
					placePairOrder(tw.ThinSym, "BID", roundTo(ta*1.0008, tM.PxDec), tMid, math.Abs(tPos), true, true)
				}
			}
			if math.Abs(dPos) >= dM.MinB {
				if dPos > 0 {
					placePairOrder(tw.DeepSym, "ASK", roundTo(db*0.9992, dM.PxDec), dMid, math.Abs(dPos), true, true)
				} else {
					placePairOrder(tw.DeepSym, "BID", roundTo(da*1.0008, dM.PxDec), dMid, math.Abs(dPos), true, true)
				}
			}
		}
		return true
	}

	if now.Sub(tw.LastDualSendTS) < 450*time.Millisecond {
		return pairAllowed
	}

	// STEP B: Structural Decoupling (|Z|>=3.5 / 1.25% Equity SL), Funding Bleed Guard & 1h-16h Convergence Exit
	if math.Abs(tUSD) >= 11.0 && math.Abs(dUSD) >= 11.0 && tUSD*dUSD < 0 {
		cancelThinEntries()
		if tAE <= 0 || dAE <= 0 {
			return true
		}
		ageS := now.Sub(tw.EntryTS).Seconds()
		var execNetBps, availExitUSD float64
		if tPos > 0 {
			execNetBps = ((tb-tAE)/tAE + (dAE-da)/dAE) * 10000.0
			availExitUSD = 0.80 * math.Min(math.Max(tb*tbSz, tBidD), math.Max(da*daSz, dAskD))
		} else {
			execNetBps = ((tAE-ta)/tAE + (db-dAE)/dAE) * 10000.0
			availExitUSD = 0.80 * math.Min(math.Max(ta*taSz, tAskD), math.Max(db*dbSz, dBidD))
		}
		totalNetBps := execNetBps + tw.AccruedFundBps
		tradePnlUSD := (totalNetBps / 10000.0) * math.Abs(tUSD)
		maxLossUSD := 0.0125 * math.Max(85.0, tw.EntryEqUSD)
		zDecoupled := (tPos > 0 && liveZ <= -3.50) || (tPos < 0 && liveZ >= 3.50)
		fundBleedExit := (netCarryBpsHr <= -1.50 && ageS >= 1800.0) || (tw.AccruedFundBps <= -4.5 && execNetBps >= -1.0)
		tightStopBook := tSpBps <= 5.0 && dSpBps <= 3.0
		hardEmergency := math.Abs(liveZ) >= 4.20 || tradePnlUSD <= -maxLossUSD*1.15
		stopTrig := (tightStopBook && (zDecoupled || tradePnlUSD <= -maxLossUSD || execNetBps <= -20.0 || ageS >= 57600.0 || fundBleedExit)) || hardEmergency
		if stopTrig && takeWriteSlotTier(2) {
			fmt.Printf("[%s] [TWIN-SL-EXIT] %s z=%.2f pnl=$%.3f(max=-$%.2f) bps=%.1f netCarry=%.2f accFund=%.2f age=%.0fs\n",
				now.Format("15:04:05"), tw.Name, liveZ, tradePnlUSD, maxLossUSD, execNetBps, netCarryBpsHr, tw.AccruedFundBps, ageS)
			if tw.TradeID > 0 {
				recordStratClose(tw.TradeID, "TWIN_EQUALIZE", tw.Name, tw.LegUSD, ageS, totalNetBps, tradePnlUSD, "SL_OR_DECOUPLE")
				tw.TradeID = 0
			}
			if tBidOrd != nil && tBidOrd.IsClose && !tBidOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "BID")
			}
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "ASK")
			}
			tw.LastDualSendTS = now
			tw.CoolUntil = now.Add(45 * time.Second)
			if availExitUSD >= 22.0 {
				tCl, dCl := math.Abs(tPos), math.Abs(dPos)
				if availExitUSD < math.Abs(tUSD)-14.0 {
					frac := availExitUSD / math.Abs(tUSD)
					tCl = roundTo(tCl*frac, tM.SzDec)
					dCl = roundTo(dCl*frac, dM.SzDec)
				}
				if tPos > 0 {
					fireTwinIOC(tw, "ASK", roundTo(tb*0.9992, tM.PxDec), tMid, tCl, "BID", roundTo(da*1.0008, dM.PxDec), dMid, dCl, true)
				} else {
					fireTwinIOC(tw, "BID", roundTo(ta*1.0008, tM.PxDec), tMid, tCl, "ASK", roundTo(db*0.9992, dM.PxDec), dMid, dCl, true)
				}
			} else if tPos > 0 {
				placePairOrder(tw.ThinSym, "ASK", roundTo(tb*0.9992, tM.PxDec), tMid, math.Abs(tPos), true, true)
			} else {
				placePairOrder(tw.ThinSym, "BID", roundTo(ta*1.0008, tM.PxDec), tMid, math.Abs(tPos), true, true)
			}
			return true
		}

		converged := (tPos > 0 && liveZ >= -0.10) || (tPos < 0 && liveZ <= 0.10) || execNetBps >= 5.5
		if converged && execNetBps >= 2.20 && availExitUSD >= math.Abs(tUSD)*0.95 && takeWriteSlot(true) {
			if tw.TradeID > 0 {
				recordStratClose(tw.TradeID, "TWIN_EQUALIZE", tw.Name, tw.LegUSD, ageS, totalNetBps, tradePnlUSD, "CONVERGED")
				tw.TradeID = 0
			}
			if tBidOrd != nil && tBidOrd.IsClose && !tBidOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "BID")
			}
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "ASK")
			}
			tw.LastDualSendTS = now
			tw.CoolUntil = now.Add(45 * time.Second)
			if tPos > 0 {
				fireTwinIOC(tw, "ASK", roundTo(tb*0.99992, tM.PxDec), tMid, math.Abs(tPos), "BID", roundTo(da*1.00008, dM.PxDec), dMid, math.Abs(dPos), true)
			} else {
				fireTwinIOC(tw, "BID", roundTo(ta*1.00008, tM.PxDec), tMid, math.Abs(tPos), "ASK", roundTo(db*0.99992, dM.PxDec), dMid, math.Abs(dPos), true)
			}
			return true
		}

		halfRev := (tPos > 0 && liveZ >= -0.50) || (tPos < 0 && liveZ <= 0.50)
		if !halfRev {
			if tBidOrd != nil && tBidOrd.IsClose && !tBidOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "BID")
			}
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "ASK")
			}
			return true
		}
		if tPos > 0 {
			minProfitAsk := tAE * (1.0 + 2.4/10000.0 - (dAE-da)/dAE)
			wantAsk := math.Max(ta, math.Max(roundTo(da*refMean*(1.0+0.6/10000.0), tM.PxDec), roundTo(minProfitAsk, tM.PxDec)))
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				if math.Abs(tAskOrd.Px-wantAsk)/wantAsk*10000.0 >= 2.0 && now.Sub(tLastAsk) >= 4000*time.Millisecond && takeWriteSlot(true) {
					modifyPairSide(tw.ThinSym, "ASK", wantAsk, tMid)
				}
			} else if tAskOrd == nil && now.Sub(tLastAsk) >= 400*time.Millisecond && takeWriteSlot(true) {
				placePairOrder(tw.ThinSym, "ASK", wantAsk, tMid, math.Abs(tPos), true, false)
			}
		} else {
			maxProfitBid := tAE * (1.0 - 2.4/10000.0 + (db-dAE)/dAE)
			wantBid := math.Min(tb, math.Min(roundTo(db*refMean*(1.0-0.6/10000.0), tM.PxDec), roundTo(maxProfitBid, tM.PxDec)))
			if tBidOrd != nil && tBidOrd.IsClose && !tBidOrd.IsIOC {
				if math.Abs(tBidOrd.Px-wantBid)/wantBid*10000.0 >= 2.0 && now.Sub(tLastBid) >= 4000*time.Millisecond && takeWriteSlot(true) {
					modifyPairSide(tw.ThinSym, "BID", wantBid, tMid)
				}
			} else if tBidOrd == nil && now.Sub(tLastBid) >= 400*time.Millisecond && takeWriteSlot(true) {
				placePairOrder(tw.ThinSym, "BID", wantBid, tMid, math.Abs(tPos), true, false)
			}
		}
		return true
	}

	// STEP C: Simultaneous Dual-IOC Entry w/ Binance <2bps Basis Guard + Max 0.75bps/h Funding Filter
	otherBusy := false
	for sym, st := range mktStates {
		if sym == tw.ThinSym || sym == tw.DeepSym {
			continue
		}
		if math.Abs(float64(st.netMicroUSD.Load()))/1e6 >= 5.0 {
			otherBusy = true
			break
		}
	}
	deepBBO := math.Min(math.Max(db*dbSz, dBidD), math.Max(da*daSz, dAskD))
	deepBookSafe := tw.WfReady && booksFresh && now.After(tw.CoolUntil) && dSpBps <= 2.2 && tSpBps <= 4.5 && deepBBO >= 350.0 && refMean > 0
	if !pairAllowed || otherBusy || isOpenOrCarbonBusy() || remotePaused.Load() || breakerTripped.Load() || !deepBookSafe {
		cancelThinEntries()
		return pairAllowed
	}

	mult, _ := calcCapMult()
	legUSD := math.Min(tw.MaxCapUSD, math.Min(curEq*3.5, math.Max(160.0, curEq*2.4*mult)))
	legUSD = math.Min(legUSD, 0.35*deepBBO)
	if legUSD < 100.0 {
		cancelThinEntries()
		return pairAllowed
	}

	sigLong := bnLongOk && liveZ <= -tw.WfZ && devBps <= -tw.MinGapBps && tw.FundDiffBps <= 0.75
	sigShort := bnShortOk && liveZ >= tw.WfZ && devBps >= tw.MinGapBps && tw.FundDiffBps >= -0.75
	cancelThinEntries()

	if sigLong {
		iocLongEdge := (db*refMean - ta) / ta * 10000.0
		liqLong := math.Min(math.Max(ta*taSz, tAskD), math.Max(db*dbSz, dBidD))
		if iocLongEdge >= tw.MinGapBps-2.0 && liqLong >= legUSD*1.15 && takeWriteSlot(false) {
			tw.LastDualSendTS = now
			tw.TradeID = coiSeq.Add(1)
			tw.LegUSD = legUSD
			recordStratOpen(tw.TradeID, "TWIN_EQUALIZE", tw.Name, legUSD, mult, iocLongEdge, fmt.Sprintf("z=%+.2f dev=%+.1fbp", liveZ, devBps))
			tPx := roundTo(ta*1.00012, tM.PxDec)
			dPx := roundTo(db*0.99988, dM.PxDec)
			tSz := roundTo(legUSD/ta, tM.SzDec)
			dSz := roundTo(legUSD/db, dM.SzDec)
			fireTwinIOC(tw, "BID", tPx, tMid, tSz, "ASK", dPx, dMid, dSz, false)
		}
	} else if sigShort {
		iocShortEdge := (tb - da*refMean) / tb * 10000.0
		liqShort := math.Min(math.Max(tb*tbSz, tBidD), math.Max(da*daSz, dAskD))
		if iocShortEdge >= tw.MinGapBps-2.0 && liqShort >= legUSD*1.15 && takeWriteSlot(false) {
			tw.LastDualSendTS = now
			tw.TradeID = coiSeq.Add(1)
			tw.LegUSD = legUSD
			recordStratOpen(tw.TradeID, "TWIN_EQUALIZE", tw.Name, legUSD, mult, iocShortEdge, fmt.Sprintf("z=%+.2f dev=%+.1fbp", liveZ, devBps))
			tPx := roundTo(tb*0.99988, tM.PxDec)
			dPx := roundTo(da*1.00012, dM.PxDec)
			tSz := roundTo(legUSD/tb, tM.SzDec)
			dSz := roundTo(legUSD/da, dM.SzDec)
			fireTwinIOC(tw, "ASK", tPx, tMid, tSz, "BID", dPx, dMid, dSz, false)
		}
	}
	return true
}

func runPairHeartbeat() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		manageAllTwins()
	}
}

func runL2TickRecorder() {
	dir := "/opt/crypto-arb-bot/l2_archive"
	if os.MkdirAll(dir, 0755) != nil {
		dir = "/tmp/l2_archive"
		_ = os.MkdirAll(dir, 0755)
	}
	var curDay string
	var f *os.File
	var gw *gzip.Writer
	openDay := func(day string) {
		if gw != nil {
			_ = gw.Close()
			gw = nil
		}
		if f != nil {
			_ = f.Close()
			f = nil
		}
		p := filepath.Join(dir, "l2_"+day+".jsonl.gz")
		nf, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return
		}
		f = nf
		gw = gzip.NewWriter(nf)
		curDay = day
	}
	pruneOld := func() {
		ents, err := os.ReadDir(dir)
		if err != nil {
			return
		}
		type finfo struct {
			name string
			sz   int64
		}
		var list []finfo
		var tot int64
		for _, e := range ents {
			if strings.HasPrefix(e.Name(), "l2_") && strings.HasSuffix(e.Name(), ".jsonl.gz") {
				if info, err2 := e.Info(); err2 == nil {
					list = append(list, finfo{name: e.Name(), sz: info.Size()})
					tot += info.Size()
				}
			}
		}
		sort.Slice(list, func(i, j int) bool { return list[i].name < list[j].name })
		for tot > 350*1024*1024 && len(list) > 1 {
			_ = os.Remove(filepath.Join(dir, list[0].name))
			tot -= list[0].sz
			list = list[1:]
		}
	}
	flushTick := time.NewTicker(10 * time.Second)
	defer flushTick.Stop()
	lastLogR := time.Now()
	pruneCnt := 0
	for {
		select {
		case line := <-l2RecCh:
			day := time.Now().UTC().Format("2006-01-02")
			if day != curDay || gw == nil {
				openDay(day)
			}
			if gw != nil {
				_, _ = gw.Write([]byte(line + "\n"))
				n := l2TicksRecorded.Add(1)
				if n%5000 == 0 || time.Since(lastLogR) >= 10*time.Minute {
					lastLogR = time.Now()
					fmt.Printf("[L2_REC] dir=%s day=%s ticks=%d\n", dir, curDay, n)
				}
			}
		case <-flushTick.C:
			if gw != nil {
				_ = gw.Flush()
			}
			if f != nil {
				_ = f.Sync()
			}
			pruneCnt++
			if pruneCnt%60 == 0 {
				pruneOld()
			}
		}
	}
}

func triggerS3Pause(reason string) {
	_ = os.WriteFile("/tmp/lighter_pause.flag", []byte("1"), 0644)
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Get(fmt.Sprintf("https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json?_t=%d", time.Now().UnixMilli()))
	if err != nil {
		return
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var tel struct {
		PauseControlUrl string `json:"pauseControlUrl"`
	}
	if json.Unmarshal(b, &tel) != nil || tel.PauseControlUrl == "" {
		return
	}
	payload, _ := json.Marshal(map[string]any{"paused": true, "ts": time.Now().UnixMilli(), "reason": reason})
	req, err := http.NewRequest("PUT", tel.PauseControlUrl, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "text/plain")
	if pr, e2 := hc.Do(req); e2 == nil {
		pr.Body.Close()
	}
}

func flattenAndExitPause(reason string) {
	if !remotePaused.CompareAndSwap(false, true) {
		return
	}
	fmt.Printf("[INSTANT-PAUSE] %s — canceling orders & flattening in <1s!\n", reason)
	if strings.HasPrefix(reason, "AUTO-BREAKER") || strings.HasPrefix(reason, "GOAL") || strings.HasPrefix(reason, "STOP-LOSS") {
		triggerS3Pause(reason)
	}
	flattenBnDnAndSweep()
	useWsSend = false
	cancelAll()
	for sym, st := range mktStates {
		st.mu.Lock()
		sz, lb, la := st.pos, st.ltBid, st.ltAsk
		st.mu.Unlock()
		if math.Abs(sz) >= markets[sym].MinB {
			m := markets[sym]
			px := la * 1.01
			var isAsk uint8 = 0
			if sz > 0 {
				isAsk = 1
				px = lb * 0.99
			}
			if px > 0 {
				_ = sendSignedTx("pause_flat_"+sym, sym, func(opts *types.TransactOpts) (uint8, string, error) {
					ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
						MarketIndex:      m.ID,
						ClientOrderIndex: coiSeq.Add(1),
						BaseAmount:       int64(math.Round(math.Abs(sz) * math.Pow10(m.SzDec))),
						Price:            uint32(math.Round(px * math.Pow10(m.PxDec))),
						IsAsk:            isAsk,
						Type:             1,
						TimeInForce:      0,
						ReduceOnly:       1,
						OrderExpiry:      0,
					}, opts)
					if e != nil {
						return 0, "", e
					}
					s, e := ti.GetTxInfo()
					return ti.GetTxType(), s, e
				})
			}
		}
	}
	time.Sleep(300 * time.Millisecond)
	os.Exit(0)
}

func runPauseWatchdog() {
	hc := &http.Client{Timeout: 1500 * time.Millisecond}
	s3Ticks := 0
	for {
		if _, err := os.Stat("/tmp/lighter_pause.flag"); err == nil {
			flattenAndExitPause("/tmp/lighter_pause.flag")
			return
		}
		s3Ticks++
		if s3Ticks%5 == 0 && time.Since(bootTS) >= 45*time.Second {
			if resp, err := hc.Get(fmt.Sprintf("https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json?t=%d", time.Now().UnixMilli())); err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var tel struct {
					TS int64 `json:"ts"`
				}
				if json.Unmarshal(b, &tel) == nil && tel.TS > v87BootMS+5000 && strings.Contains(string(b), `"userPaused":true`) {
					flattenAndExitPause("S3 userPaused=true")
					return
				}
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
}

func parseAnyFloat(raw json.RawMessage) float64 {
	s := strings.TrimSpace(string(raw))
	s = strings.Trim(s, `"`)
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func discoverCommonAssets() {
	hc := &http.Client{Timeout: 8 * time.Second}
	fetchURL := func(u string) ([]byte, error) {
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := hc.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		return io.ReadAll(resp.Body)
	}
	ltB, err1 := fetchURL(BaseURL + "/api/v1/orderBookDetails")
	bnB, err2 := fetchURL("https://fapi.binance.com/fapi/v1/ticker/24hr")
	if err1 != nil || err2 != nil {
		fmt.Printf("[DISCOVER_ERR] lt=%v bn=%v\n", err1, err2)
		return
	}
	var ltRes struct {
		Details []struct {
			Symbol   string          `json:"symbol"`
			MarketID int64           `json:"market_id"`
			Status   string          `json:"status"`
			SzDec    int             `json:"size_decimals"`
			PxDec    int             `json:"price_decimals"`
			MinB     json.RawMessage `json:"min_base_amount"`
			DailyVol json.RawMessage `json:"daily_quote_token_volume"`
		} `json:"order_book_details"`
	}
	var bnArr []struct {
		Symbol      string `json:"symbol"`
		QuoteVolume string `json:"quoteVolume"`
	}
	if json.Unmarshal(ltB, &ltRes) != nil || json.Unmarshal(bnB, &bnArr) != nil {
		fmt.Println("[DISCOVER_PARSE_ERR]")
		return
	}
	bnVol := map[string]float64{}
	for _, x := range bnArr {
		if strings.HasSuffix(x.Symbol, "USDT") {
			bnVol[x.Symbol], _ = strconv.ParseFloat(x.QuoteVolume, 64)
		}
	}
	var found []*CommonAsset
	for _, m := range ltRes.Details {
		if m.Status != "active" {
			continue
		}
		ltV := parseAnyFloat(m.DailyVol)
		bsym := m.Symbol + "USDT"
		bnV := bnVol[bsym]
		if ltV >= 25000.0 && bnV >= 1000000.0 {
			minB := math.Max(1e-6, parseAnyFloat(m.MinB))
			found = append(found, &CommonAsset{
				Sym:      m.Symbol,
				BnSym:    bsym,
				MarketID: m.MarketID,
				SzDec:    m.SzDec,
				PxDec:    m.PxDec,
				MinB:     minB,
				LtVol24h: ltV,
				BnVol24h: bnV,
			})
		}
	}
	sort.Slice(found, func(i, j int) bool { return found[i].LtVol24h > found[j].LtVol24h })
	for sym, m := range markets {
		m.AllowEntry = sym == "XAU" || sym == "PAXG" || sym == "BTC" || sym == "ETH" || sym == "SOL" || sym == "SPY" || sym == "US500" || sym == "QQQ" || sym == "US100"
		markets[sym] = m
	}
	for i, ca := range found {
		allowWS := i < 60 || ca.Sym == "PAXG" || ca.Sym == "XAU" || ca.Sym == "SPY" || ca.Sym == "US500" || ca.Sym == "QQQ" || ca.Sym == "US100"
		curM, exists := markets[ca.Sym]
		if exists {
			allowWS = allowWS || curM.AllowEntry
		}
		markets[ca.Sym] = Market{
			Sym:        ca.Sym,
			ID:         ca.MarketID,
			SzDec:      ca.SzDec,
			PxDec:      ca.PxDec,
			MinB:       ca.MinB,
			AllowEntry: allowWS,
			ClipUSD:    -1.0,
			EdgeBps:    5.0,
		}
		idToSym[ca.MarketID] = ca.Sym
		if mktStates[ca.Sym] == nil {
			mktStates[ca.Sym] = newMarketState(-1.0)
		}
	}
	commonList = found
	topSym := "none"
	var top45Min float64
	if len(found) > 0 {
		topSym = found[0].Sym
		idx := len(found) - 1
		if idx > 44 {
			idx = 44
		}
		top45Min = found[idx].LtVol24h
	}
	fmt.Printf("[DISCOVER] common=%d top1=%s top45_minVol=$%.0f\n", len(commonList), topSym, top45Min)
}

func ensureLtSubscribed(mid int64) {
	wsMu.Lock()
	defer wsMu.Unlock()
	if subbedMids[mid] || ltConn == nil {
		return
	}
	if err := ltConn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", mid)}); err == nil {
		subbedMids[mid] = true
	}
}

func runBnAllBookTickerWS() {
	wsURL := "wss://fstream.binance.com/ws/!bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var ev struct {
				S  string `json:"s"`
				B  string `json:"b"`
				BQ string `json:"B"`
				A  string `json:"a"`
				AQ string `json:"A"`
			}
			if json.Unmarshal(msg, &ev) != nil {
				continue
			}
			b, _ := strconv.ParseFloat(ev.B, 64)
			a, _ := strconv.ParseFloat(ev.A, 64)
			if b <= 0 || a <= 0 {
				continue
			}
			if ev.S == "ETHBTC" {
				bnEthBtcMu.Lock()
				bnEthBtcBid = b
				bnEthBtcAsk = a
				bnEthBtcTS = time.Now()
				bnEthBtcMu.Unlock()
				continue
			}
			if strings.HasSuffix(ev.S, "USDC") {
				bq, _ := strconv.ParseFloat(ev.BQ, 64)
				aq, _ := strconv.ParseFloat(ev.AQ, 64)
				onBnUsdcBookTick(strings.TrimSuffix(ev.S, "USDC"), b, a, bq, aq)
				continue
			}
			if !strings.HasSuffix(ev.S, "USDT") {
				continue
			}
			sym := strings.TrimSuffix(ev.S, "USDT")
			st := mktStates[sym]
			if st == nil {
				continue
			}
			mid := 0.5 * (b + a)
			now := time.Now()
			var imp3s float64
			st.mu.Lock()
			st.bnBid = b
			st.bnAsk = a
			st.bnMid = mid
			if st.emaFast == 0 || st.lastEmaTS.IsZero() {
				st.emaUltra = mid
				st.emaFast = mid
				st.emaSlow = mid
			} else {
				dt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastEmaTS).Seconds()))
				st.emaUltra += (1.0 - math.Exp(-dt/3.0)) * (mid - st.emaUltra)
				st.emaFast += (1.0 - math.Exp(-dt/20.0)) * (mid - st.emaFast)
				st.emaSlow += (1.0 - math.Exp(-dt/180.0)) * (mid - st.emaSlow)
				if st.emaUltra > 0 {
					imp3s = (mid - st.emaUltra) / st.emaUltra * 10000.0
				}
			}
			st.lastEmaTS = now
			if st.ltBid > 0 && st.ltAsk > st.ltBid && now.Sub(st.lastBookTS) < 10*time.Second {
				ltMid := 0.5 * (st.ltBid + st.ltAsk)
				ltSp := (st.ltAsk - st.ltBid) / ltMid * 10000.0
				rawBasis := (ltMid - mid) / mid * 10000.0
				if ltSp <= 3.5 && math.Abs(imp3s) <= 5.0 && (st.basisWarmTicks == 0 || now.Sub(st.lastBasisTS) >= 900*time.Millisecond) {
					if st.basisWarmTicks == 0 {
						st.basisBps = rawBasis
					} else {
						dtB := math.Min(10.0, math.Max(0.2, now.Sub(st.lastBasisTS).Seconds()))
						st.basisBps += (1.0 - math.Exp(-dtB/180.0)) * (rawBasis - st.basisBps)
					}
					st.lastBasisTS = now
					st.basisWarmTicks++
				}
			}
			st.mu.Unlock()
			if math.Abs(imp3s) >= 2.0 {
				select {
				case fastArbCh <- sym:
				default:
				}
			}
		}
		_ = conn.Close()
		time.Sleep(1 * time.Second)
	}
}

func anyOpenAcctPos() bool {
	for _, st := range mktStates {
		if math.Abs(float64(st.netMicroUSD.Load()))/1e6 >= 4.0 {
			return true
		}
	}
	return false
}

func isActiveCarbonSym(sym string) bool {
	carbonMu.Lock()
	defer carbonMu.Unlock()
	return activeCarbon != nil && activeCarbon.Sym == sym
}

func fetchLtBookREST(mid int64) (float64, float64, float64, float64) {
	hc := &http.Client{Timeout: 4 * time.Second}
	u := fmt.Sprintf("%s/api/v1/orderBookOrders?market_id=%d&limit=10", BaseURL, mid)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, 0, 0, 0
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := hc.Do(req)
	if err != nil {
		return 0, 0, 0, 0
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var ob struct {
		Bids []struct {
			Price string `json:"price"`
			Rem   string `json:"remaining_base_amount"`
		} `json:"bids"`
		Asks []struct {
			Price string `json:"price"`
			Rem   string `json:"remaining_base_amount"`
		} `json:"asks"`
	}
	if json.Unmarshal(body, &ob) != nil || len(ob.Bids) == 0 || len(ob.Asks) == 0 {
		return 0, 0, 0, 0
	}
	bb, _ := strconv.ParseFloat(ob.Bids[0].Price, 64)
	ba, _ := strconv.ParseFloat(ob.Asks[0].Price, 64)
	if bb <= 0 || ba <= bb {
		return 0, 0, 0, 0
	}
	midPx := 0.5 * (bb + ba)
	sp := (ba - bb) / midPx * 10000.0
	var bd, ad float64
	for _, x := range ob.Bids {
		p, _ := strconv.ParseFloat(x.Price, 64)
		sz, _ := strconv.ParseFloat(x.Rem, 64)
		if p > 0 && sz > 0 && (bb-p)/bb*10000.0 <= 2.0 {
			bd += p * sz
		}
	}
	for _, x := range ob.Asks {
		p, _ := strconv.ParseFloat(x.Price, 64)
		sz, _ := strconv.ParseFloat(x.Rem, 64)
		if p > 0 && sz > 0 && (p-ba)/ba*10000.0 <= 2.0 {
			ad += p * sz
		}
	}
	return bb, ba, sp, math.Min(bd, ad)
}

func runCarbonScanner() {
	time.Sleep(6 * time.Second)
	scanTick := 0
	for {
		scanTick++
		if !isOpenOrCarbonBusy() && !anyOpenAcctPos() && !remotePaused.Load() && !breakerTripped.Load() && len(commonList) > 0 {
			type candScore struct {
				ca     *CommonAsset
				rank   float64
				bnBid  float64
				bnAsk  float64
				bnMid  float64
				imp20s float64
				imp3m  float64
			}
			now := time.Now()
			var stage1 []candScore
			for _, ca := range commonList {
				if ca.LtVol24h < 800000.0 || ca.BnVol24h < 20000000.0 || !isSymTradeable24hOrOpen(ca.Sym, now) {
					continue
				}
				st := mktStates[ca.Sym]
				if st == nil {
					continue
				}
				st.mu.Lock()
				bm, bb, ba, ef, es, ts := st.bnMid, st.bnBid, st.bnAsk, st.emaFast, st.emaSlow, st.lastEmaTS
				lb, la, ldep := st.ltBid, st.ltAsk, math.Min(st.ltBidDepthUSD, st.ltAskDepthUSD)
				ltFresh := !st.lastBookTS.IsZero() && now.Sub(st.lastBookTS) < 20*time.Second && lb > 0 && la > lb
				st.mu.Unlock()
				if bm <= 0 || bb <= 0 || ba <= bb || ef <= 0 || es <= 0 || now.Sub(ts) > 25*time.Second || !ltFresh {
					continue
				}
				ltSp := (la - lb) / (0.5 * (lb + la)) * 10000.0
				bnSp := (ba - bb) / bm * 10000.0
				unitDiff := math.Abs(0.5*(lb+la)-bm) / bm * 10000.0
				if ltSp > 3.0 || bnSp > 2.0 || unitDiff > 15.0 || ldep < 200.0 {
					continue
				}
				i20 := (bm - ef) / ef * 10000.0
				i3m := (bm - es) / es * 10000.0
				rk := math.Abs(i20) + 0.5*math.Abs(i3m)
				if ca.LtVol24h >= 5000000.0 {
					rk += 2.0
				}
				stage1 = append(stage1, candScore{ca: ca, rank: rk, bnBid: bb, bnAsk: ba, bnMid: bm, imp20s: i20, imp3m: i3m})
			}
			sort.Slice(stage1, func(i, j int) bool { return stage1[i].rank > stage1[j].rank })
			if len(stage1) > 5 {
				stage1 = stage1[:5]
			}
			type scanRes struct {
				sym    string
				side   string
				score  int
				slBps  float64
				tpBps  float64
				ltSp   float64
				diff   float64
				r5m    float64
				r15m   float64
				r1h    float64
				atr    float64
				cvd    float64
				volR   float64
				oi15m  float64
				topLS  float64
				globLS float64
				fund   float64
				obi    float64
				depth  float64
			}
			results := make([]scanRes, len(stage1))
			var wg sync.WaitGroup
			for idx, c := range stage1 {
				wg.Add(1)
				go func(ix int, cs candScore) {
					defer wg.Done()
					hc := &http.Client{Timeout: 5 * time.Second}
					gJSON := func(u string, out any) bool {
						req, err := http.NewRequest("GET", u, nil)
						if err != nil {
							return false
						}
						req.Header.Set("User-Agent", "Mozilla/5.0")
						resp, err := hc.Do(req)
						if err != nil {
							return false
						}
						b, _ := io.ReadAll(resp.Body)
						resp.Body.Close()
						return json.Unmarshal(b, out) == nil
					}
					var lb, la, lsp, ldep float64
					if st := mktStates[cs.ca.Sym]; st != nil {
						st.mu.Lock()
						if !st.lastBookTS.IsZero() && time.Since(st.lastBookTS) < 10*time.Second && st.ltBid > 0 && st.ltAsk > st.ltBid {
							lb, la = st.ltBid, st.ltAsk
							lsp = (la - lb) / (0.5 * (lb + la)) * 10000.0
							ldep = math.Min(st.ltBidDepthUSD, st.ltAskDepthUSD)
						}
						st.mu.Unlock()
					}
					if lb <= 0 || ldep < 100 {
						lb, la, lsp, ldep = fetchLtBookREST(cs.ca.MarketID)
					}

					var kl [][]json.RawMessage
					var r5m, r15m, r1h, atr5m, cvdPct, volR float64
					if gJSON(fmt.Sprintf("https://fapi.binance.com/fapi/v1/klines?symbol=%s&interval=5m&limit=21", cs.ca.BnSym), &kl) && len(kl) >= 14 {
						n := len(kl)
						cLast := parseAnyFloat(kl[n-1][4])
						c1 := parseAnyFloat(kl[n-2][4])
						c3 := parseAnyFloat(kl[n-4][4])
						c12 := parseAnyFloat(kl[n-13][4])
						if c1 > 0 {
							r5m = (cLast - c1) / c1 * 10000.0
						}
						if c3 > 0 {
							r15m = (cLast - c3) / c3 * 10000.0
						}
						if c12 > 0 {
							r1h = (cLast - c12) / c12 * 10000.0
						}
						var sumRng float64
						for k := n - 12; k < n; k++ {
							hi := parseAnyFloat(kl[k][2])
							lo := parseAnyFloat(kl[k][3])
							cl := parseAnyFloat(kl[k][4])
							if cl > 0 {
								sumRng += (hi - lo) / cl * 10000.0
							}
						}
						atr5m = sumRng / 12.0
						qv := parseAnyFloat(kl[n-1][7])
						tbq := parseAnyFloat(kl[n-1][10])
						if qv > 0 {
							cvdPct = (2.0*tbq - qv) / qv * 100.0
						}
						var sumQ float64
						cntQ := 0
						for k := 0; k < n-1; k++ {
							sumQ += parseAnyFloat(kl[k][7])
							cntQ++
						}
						if cntQ > 0 && sumQ > 0 {
							volR = qv / (sumQ / float64(cntQ))
						}
					}

					var oi []struct {
						Val string `json:"sumOpenInterestValue"`
					}
					var oi15m float64
					if gJSON(fmt.Sprintf("https://fapi.binance.com/futures/data/openInterestHist?symbol=%s&period=5m&limit=4", cs.ca.BnSym), &oi) && len(oi) >= 2 {
						v0, _ := strconv.ParseFloat(oi[0].Val, 64)
						vN, _ := strconv.ParseFloat(oi[len(oi)-1].Val, 64)
						if v0 > 0 {
							oi15m = (vN - v0) / v0 * 100.0
						}
					}

					var topArr, glbArr []struct {
						Ratio string `json:"longShortRatio"`
					}
					var topLS, globLS float64
					if gJSON(fmt.Sprintf("https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol=%s&period=5m&limit=1", cs.ca.BnSym), &topArr) && len(topArr) > 0 {
						topLS, _ = strconv.ParseFloat(topArr[0].Ratio, 64)
					}
					if gJSON(fmt.Sprintf("https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=%s&period=5m&limit=1", cs.ca.BnSym), &glbArr) && len(glbArr) > 0 {
						globLS, _ = strconv.ParseFloat(glbArr[0].Ratio, 64)
					}

					var prm struct {
						LastFundingRate string `json:"lastFundingRate"`
					}
					var fundBps float64
					if gJSON(fmt.Sprintf("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=%s", cs.ca.BnSym), &prm) {
						fr, _ := strconv.ParseFloat(prm.LastFundingRate, 64)
						fundBps = fr * 10000.0
					}

					var dep struct {
						Bids [][]string `json:"bids"`
						Asks [][]string `json:"asks"`
					}
					var bnObi float64
					if gJSON(fmt.Sprintf("https://fapi.binance.com/fapi/v1/depth?symbol=%s&limit=10", cs.ca.BnSym), &dep) {
						var sb, sa float64
						for _, b := range dep.Bids {
							if len(b) >= 2 {
								q, _ := strconv.ParseFloat(b[1], 64)
								sb += q
							}
						}
						for _, a := range dep.Asks {
							if len(a) >= 2 {
								q, _ := strconv.ParseFloat(a[1], 64)
								sa += q
							}
						}
						if sb+sa > 0 {
							bnObi = (sb - sa) / (sb + sa)
						}
					}

					scL, scS := 0, 0
					if r15m > 8.0 && r1h > 5.0 {
						scL += 2
					}
					if r15m < -8.0 && r1h < -5.0 {
						scS += 2
					}
					if cvdPct > 12.0 {
						scL += 2
					} else if cvdPct < -12.0 {
						scS += 2
					}
					if volR >= 1.3 {
						scL++
						scS++
					}
					if oi15m > 0.15 {
						if r15m > 0 {
							scL += 2
						} else if r15m < 0 {
							scS += 2
						}
					}
					if topLS > globLS*1.05 || topLS > 1.25 {
						scL++
					}
					if globLS > topLS*1.05 && topLS < 1.15 {
						scS++
					}
					if fundBps <= 1.2 {
						scL++
					}
					if fundBps >= -1.2 {
						scS++
					}
					if bnObi > 0.10 {
						scL++
					} else if bnObi < -0.10 {
						scS++
					}

					bas := 0.0
					if st := mktStates[cs.ca.Sym]; st != nil {
						st.mu.Lock()
						if st.basisWarmTicks >= 10 {
							bas = st.basisBps
						}
						st.mu.Unlock()
					}
					rawDiffL := (la - cs.bnAsk) / cs.bnAsk * 10000.0
					rawDiffS := (cs.bnBid - lb) / cs.bnBid * 10000.0
					adjDiffL := (la - cs.bnAsk*(1.0+bas/10000.0)) / cs.bnAsk * 10000.0
					adjDiffS := (cs.bnBid*(1.0+bas/10000.0) - lb) / cs.bnBid * 10000.0
					diffL := math.Min(rawDiffL, adjDiffL)
					diffS := math.Min(rawDiffS, adjDiffS)
					if lb <= 0 || la <= lb || lsp > 3.0 || ldep < 200.0 {
						scL, scS = 0, 0
					}
					if diffL > 3.0 {
						scL = 0
					}
					if diffS > 3.0 {
						scS = 0
					}

					side := "LONG"
					sc := scL
					overpayDiff := diffL
					if scS > scL {
						side = "SHORT"
						sc = scS
						overpayDiff = diffS
					}
					slB := math.Min(45.0, math.Max(18.0, atr5m*0.75))
					tpB := math.Round(slB * 2.5)
					results[ix] = scanRes{
						sym: cs.ca.Sym, side: side, score: sc, slBps: math.Round(slB), tpBps: tpB,
						ltSp: lsp, diff: overpayDiff, r5m: r5m, r15m: r15m, r1h: r1h, atr: atr5m,
						cvd: cvdPct, volR: volR, oi15m: oi15m, topLS: topLS, globLS: globLS,
						fund: fundBps, obi: bnObi, depth: ldep,
					}
				}(idx, c)
			}
			wg.Wait()
			sort.Slice(results, func(i, j int) bool { return results[i].score > results[j].score })
			seq := carbonScanSeq.Add(1)
			var parts []string
			for k := 0; k < len(results) && k < 3; k++ {
				r := results[k]
				parts = append(parts, fmt.Sprintf("%d) %s %s sc=%d (1R=%.0f/TP=%.0f ltSp=%.2fbp diff=%+.2fbp r5=%.0f r15=%.0f r1h=%.0f atr=%.0f cvd=%+.0f%% v=%.1fx oi=%+.2f%% top=%.2f glb=%.2f f=%.2fbp obi=%.2f)",
					k+1, r.sym, r.side, r.score, r.slBps, r.tpBps, r.ltSp, r.diff, r.r5m, r.r15m, r.r1h, r.atr, r.cvd, r.volR, r.oi15m, r.topLS, r.globLS, r.fund, r.obi))
			}
			if len(parts) > 0 {
				fmt.Printf("[CARBON_SCAN seq=%d com=%d] %s\n", seq, len(commonList), strings.Join(parts, " | "))
				topR := results[0]
				snapMu.Lock()
				snapMomDesc = topR.sym + " " + topR.side
				snapMomSc = topR.score
				lDesc, lEdge := snapLagDesc, snapLagEdge
				snapMu.Unlock()
				tn := getTune()
				fmt.Printf("[STRAT_SCAN seq=%d gate=%v] LAG_BEST=%s %+.2fbp/%.1f | MOM_BEST=%s %s sc=%d/%d\n",
					seq, awaitingReview.Load(), lDesc, lEdge, tn.MinLagBps, topR.sym, topR.side, topR.score, tn.MinScore)
				if tn.AllowMom && canOpenNewStrat() {
					firedMom := false
					if topR.score >= tn.MinScore && topR.ltSp <= 1.8 && topR.diff <= 0.6 && topR.volR >= 0.85 && topR.depth >= 150.0 {
						alignOK := (topR.side == "LONG" && topR.r5m >= 3.0 && topR.r15m >= 8.0 && topR.r15m <= 65.0 && topR.cvd >= 5.0 && topR.obi >= -0.08) ||
							(topR.side == "SHORT" && topR.r5m <= -3.0 && topR.r15m <= -8.0 && topR.r15m >= -65.0 && topR.cvd <= -5.0 && topR.obi <= 0.08)
						if alignOK {
							sl := math.Min(32.0, math.Max(18.0, topR.slBps))
							tp := math.Round(sl * 1.6)
							usd := math.Min(165.0, math.Max(14.0, (tn.RiskUSD*10000.0)/sl))
							startAutoCarbonSnipe(topR.sym, topR.side, "ADAPT_MOM", usd, sl, tp, topR.diff)
							firedMom = true
						}
					}
					if !firedMom {
						for _, r := range results {
							if math.Abs(r.r1h) > 55.0 || r.ltSp > 1.5 || r.depth < 200.0 {
								continue
							}
							if r.r5m <= -16.0 && r.r5m >= -42.0 && r.obi >= 0.28 {
								sl := math.Min(26.0, math.Max(18.0, r.slBps))
								tp := math.Round(sl * 1.4)
								usd := math.Min(165.0, math.Max(14.0, (tn.RiskUSD*10000.0)/sl))
								startAutoCarbonSnipe(r.sym, "LONG", "OBI_MEAN_REV", usd, sl, tp, r.diff)
								break
							} else if r.r5m >= 16.0 && r.r5m <= 42.0 && r.obi <= -0.28 {
								sl := math.Min(26.0, math.Max(18.0, r.slBps))
								tp := math.Round(sl * 1.4)
								usd := math.Min(165.0, math.Max(14.0, (tn.RiskUSD*10000.0)/sl))
								startAutoCarbonSnipe(r.sym, "SHORT", "OBI_MEAN_REV", usd, sl, tp, r.diff)
								break
							}
						}
					}
				}
			}
		}
		if scanTick%2 == 0 {
			printStratBoard()
		}
		time.Sleep(15 * time.Second)
	}
}

func hmacSHA256Raw(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}

func sha256HexStr(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func readPauseControlJSON() ([]byte, error) {
	hc := &http.Client{Timeout: 2500 * time.Millisecond}
	tokReq, _ := http.NewRequest("PUT", "http://169.254.169.254/latest/api/token", nil)
	tokReq.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	if tokResp, err := hc.Do(tokReq); err == nil {
		tokB, _ := io.ReadAll(tokResp.Body)
		tokResp.Body.Close()
		tok := strings.TrimSpace(string(tokB))
		if tok != "" {
			roleReq, _ := http.NewRequest("GET", "http://169.254.169.254/latest/meta-data/iam/security-credentials/", nil)
			roleReq.Header.Set("X-aws-ec2-metadata-token", tok)
			if rResp, e2 := hc.Do(roleReq); e2 == nil {
				rB, _ := io.ReadAll(rResp.Body)
				rResp.Body.Close()
				role := strings.TrimSpace(strings.Split(string(rB), "\n")[0])
				if role != "" {
					cReq, _ := http.NewRequest("GET", "http://169.254.169.254/latest/meta-data/iam/security-credentials/"+role, nil)
					cReq.Header.Set("X-aws-ec2-metadata-token", tok)
					if cResp, e3 := hc.Do(cReq); e3 == nil {
						cb, _ := io.ReadAll(cResp.Body)
						cResp.Body.Close()
						var creds struct {
							AccessKeyId     string `json:"AccessKeyId"`
							SecretAccessKey string `json:"SecretAccessKey"`
							Token           string `json:"Token"`
						}
						if json.Unmarshal(cb, &creds) == nil && creds.AccessKeyId != "" {
							host := "crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com"
							nowU := time.Now().UTC()
							amzDate := nowU.Format("20060102T150405Z")
							dateStamp := amzDate[:8]
							payloadHash := sha256HexStr("")
							canonHdrs := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\nx-amz-security-token:%s\n",
								host, payloadHash, amzDate, creds.Token)
							signedHdrs := "host;x-amz-content-sha256;x-amz-date;x-amz-security-token"
							canonReq := "GET\n/pause-control.json\n\n" + canonHdrs + "\n" + signedHdrs + "\n" + payloadHash
							scope := dateStamp + "/ap-northeast-1/s3/aws4_request"
							strToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256HexStr(canonReq)
							kDate := hmacSHA256Raw([]byte("AWS4"+creds.SecretAccessKey), dateStamp)
							kReg := hmacSHA256Raw(kDate, "ap-northeast-1")
							kSvc := hmacSHA256Raw(kReg, "s3")
							kSig := hmacSHA256Raw(kSvc, "aws4_request")
							sig := hex.EncodeToString(hmacSHA256Raw(kSig, strToSign))
							auth := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
								creds.AccessKeyId, scope, signedHdrs, sig)
							s3Req, _ := http.NewRequest("GET", "https://"+host+"/pause-control.json", nil)
							s3Req.Header.Set("x-amz-date", amzDate)
							s3Req.Header.Set("x-amz-content-sha256", payloadHash)
							s3Req.Header.Set("x-amz-security-token", creds.Token)
							s3Req.Header.Set("Authorization", auth)
							if s3Resp, e4 := hc.Do(s3Req); e4 == nil {
								body, _ := io.ReadAll(s3Resp.Body)
								s3Resp.Body.Close()
								if s3Resp.StatusCode == 200 {
									return body, nil
								}
							}
						}
					}
				}
			}
		}
	}
	return exec.Command("aws", "s3", "cp", "s3://crypto-arb-bot-tokyo-bin-767579108352/pause-control.json", "-").Output()
}

func manageCarbonBracket() {
	carbonMu.Lock()
	cp := activeCarbon
	if cp == nil {
		carbonMu.Unlock()
		return
	}
	cpCopy := *cp
	carbonMu.Unlock()

	st := mktStates[cpCopy.Sym]
	m, ok := markets[cpCopy.Sym]
	if st == nil || !ok {
		return
	}
	now := time.Now()
	st.mu.Lock()
	lb, la, pos, ae := st.ltBid, st.ltAsk, st.pos, st.avgEntry
	closeSide := "ASK"
	if cpCopy.Side == "SHORT" {
		closeSide = "BID"
	}
	closeOrd := st.activeOrds[closeSide]
	st.mu.Unlock()

	if math.Abs(pos) < m.MinB {
		if cpCopy.TpSent {
			reasonOut := cpCopy.CloseReason
			pnlOut := cpCopy.LastPnLBps
			if reasonOut == "" {
				reasonOut = "LIMIT_TP"
				pnlOut = cpCopy.TpBps
			}
			tag := cpCopy.StratTag
			if tag == "" {
				tag = "BN_MOM_SNIPE"
			}
			holdS := now.Sub(cpCopy.SentAt).Seconds()
			recordStratClose(cpCopy.ID, tag, cpCopy.Sym+"_"+cpCopy.Side, cpCopy.USD, holdS, pnlOut, cpCopy.USD*pnlOut/10000.0, reasonOut)
			fmt.Printf("[CARBON_CLOSED id=%d sym=%s side=%s reason=%s]\n", cpCopy.ID, cpCopy.Sym, cpCopy.Side, reasonOut)
			if pnlOut < 0 {
				stratCoolUntilMS.Store(time.Now().Add(35 * time.Second).UnixMilli())
			} else {
				stratCoolUntilMS.Store(time.Now().Add(10 * time.Second).UnixMilli())
			}
			carbonMu.Lock()
			if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
				activeCarbon = nil
			}
			carbonMu.Unlock()
		} else if now.Sub(cpCopy.SentAt) >= 3*time.Second {
			fmt.Printf("[CARBON_MISSED id=%d sym=%s reason=ENTRY_IOC_NO_FILL]\n", cpCopy.ID, cpCopy.Sym)
			carbonMu.Lock()
			if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
				activeCarbon = nil
			}
			carbonMu.Unlock()
		}
		return
	}

	entry := cpCopy.EntryPx
	if ae > 0 {
		entry = ae
	}
	if entry <= 0 || lb <= 0 || la <= lb {
		return
	}
	tMid := 0.5 * (lb + la)

	if cpCopy.Closing {
		if now.Sub(cpCopy.LastCloseAt) >= 1200*time.Millisecond && takeWriteSlotTier(2) {
			if closeOrd != nil && !closeOrd.IsIOC {
				cancelPairSide(cpCopy.Sym, closeSide)
			}
			exitPx := roundTo(lb*0.995, m.PxDec)
			if cpCopy.Side == "SHORT" {
				exitPx = roundTo(la*1.005, m.PxDec)
			}
			carbonMu.Lock()
			if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
				activeCarbon.LastCloseAt = now
			}
			carbonMu.Unlock()
			placePairOrder(cpCopy.Sym, closeSide, exitPx, tMid, math.Abs(pos), true, true)
		}
		return
	}

	if !cpCopy.TpSent {
		tpPx := entry * (1.0 + cpCopy.TpBps/10000.0)
		if cpCopy.Side == "SHORT" {
			tpPx = entry * (1.0 - cpCopy.TpBps/10000.0)
		}
		tpPx = roundTo(tpPx, m.PxDec)
		carbonMu.Lock()
		if activeCarbon != nil && activeCarbon.ID == cpCopy.ID && !activeCarbon.TpSent {
			activeCarbon.EntryPx = entry
			activeCarbon.TpSent = true
		}
		carbonMu.Unlock()
		if takeWriteSlot(true) {
			placePairOrder(cpCopy.Sym, closeSide, tpPx, tMid, math.Abs(pos), true, false)
			fmt.Printf("[CARBON_OPEN id=%d sym=%s side=%s entry=%.5f tp=%.5f(+%.0fbps) sl=-%.0fbps]\n",
				cpCopy.ID, cpCopy.Sym, cpCopy.Side, entry, tpPx, cpCopy.TpBps, cpCopy.SlBps)
		}
		return
	}

	var pnlBps float64
	if cpCopy.Side == "LONG" {
		pnlBps = (lb - entry) / entry * 10000.0
	} else {
		pnlBps = (entry - la) / entry * 10000.0
	}
	peak := cpCopy.PeakFavBps
	if pnlBps > peak {
		peak = pnlBps
	}
	carbonMu.Lock()
	if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
		activeCarbon.LastPnLBps = pnlBps
		if peak > activeCarbon.PeakFavBps {
			activeCarbon.PeakFavBps = peak
		}
	}
	carbonMu.Unlock()
	tn := getTune()
	if !cpCopy.BeArmed && peak >= math.Min(10.0, cpCopy.SlBps*tn.BeAtR) {
		carbonMu.Lock()
		if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
			activeCarbon.BeArmed = true
		}
		carbonMu.Unlock()
		cpCopy.BeArmed = true
		fmt.Printf("[CARBON_BE id=%d sym=%s peak=%.1fbp locked=+3.5bps]\n", cpCopy.ID, cpCopy.Sym, peak)
	}
	slThresh := -cpCopy.SlBps
	if peak >= 35.0 {
		slThresh = peak - 7.0
	} else if peak >= 26.0 {
		slThresh = 18.0
	} else if peak >= 18.0 {
		slThresh = 10.0
	} else if cpCopy.BeArmed {
		slThresh = 3.5
	}
	ageS := int64(now.Sub(cpCopy.SentAt).Seconds())
	reason := ""
	if pnlBps <= slThresh {
		if cpCopy.BeArmed {
			reason = "BE_STOP"
		} else {
			reason = "SL"
		}
	} else if cpCopy.MaxHoldSec > 0 && ageS >= cpCopy.MaxHoldSec {
		reason = "MAX_HOLD"
	}
	if reason != "" && (!cpCopy.Closing || now.Sub(cpCopy.SentAt) >= 2*time.Second) && takeWriteSlotTier(2) {
		if closeOrd != nil && !closeOrd.IsIOC {
			cancelPairSide(cpCopy.Sym, closeSide)
		}
		exitPx := roundTo(lb*0.996, m.PxDec)
		if cpCopy.Side == "SHORT" {
			exitPx = roundTo(la*1.004, m.PxDec)
		}
		carbonMu.Lock()
		if activeCarbon != nil && activeCarbon.ID == cpCopy.ID {
			activeCarbon.Closing = true
			activeCarbon.LastCloseAt = now
			activeCarbon.CloseReason = reason
			activeCarbon.LastPnLBps = pnlBps
		}
		carbonMu.Unlock()
		placePairOrder(cpCopy.Sym, closeSide, exitPx, tMid, math.Abs(pos), true, true)
		fmt.Printf("[CARBON_CLOSED id=%d sym=%s side=%s reason=%s pnlBps=%.1f]\n",
			cpCopy.ID, cpCopy.Sym, cpCopy.Side, reason, pnlBps)
	}
}

func runCarbonCommandLoop() {
	cmdTick := time.NewTicker(800 * time.Millisecond)
	brkTick := time.NewTicker(150 * time.Millisecond)
	defer cmdTick.Stop()
	defer brkTick.Stop()
	for {
		select {
		case <-brkTick.C:
			manageCarbonBracket()
		case <-cmdTick.C:
			raw, err := readPauseControlJSON()
			if err != nil || len(raw) == 0 {
				continue
			}
			var pc struct {
				Paused      bool         `json:"paused"`
				CarbonOrder *CarbonOrder `json:"carbonOrder"`
				CarbonTune  *CarbonTune  `json:"carbonTune"`
			}
			if json.Unmarshal(raw, &pc) != nil {
				continue
			}
			nowMS := time.Now().UnixMilli()
			if !pc.Paused && pc.CarbonTune != nil {
				ct := pc.CarbonTune
				if ct.ID > lastCarbonTuneID && nowMS-ct.ID <= 120000 {
					lastCarbonTuneID = ct.ID
					nt := CarbonTune{
						ID:        ct.ID,
						RiskUSD:   math.Min(0.60, math.Max(0.04, ct.RiskUSD)),
						MinScore:  int(math.Min(10, math.Max(6, float64(ct.MinScore)))),
						MinLagBps: math.Min(8.0, math.Max(2.5, ct.MinLagBps)),
						BeAtR:     math.Min(1.50, math.Max(0.25, ct.BeAtR)),
						AllowLag:  ct.AllowLag,
						AllowMom:  ct.AllowMom,
						Note:      strings.TrimSpace(ct.Note),
					}
					tuneMu.Lock()
					curTune = nt
					tuneMu.Unlock()
					awaitingReview.Store(false)
					fmt.Printf("[STRAT_SESSION id=%d risk=$%.2f minSc=%d minLag=%.1fbp be=%.2fR lag=%v mom=%v note=%q]\n",
						nt.ID, nt.RiskUSD, nt.MinScore, nt.MinLagBps, nt.BeAtR, nt.AllowLag, nt.AllowMom, nt.Note)
					appendStratJournal(map[string]any{
						"ts":   time.Now().UTC().Format(time.RFC3339),
						"ev":   "SESSION",
						"id":   nt.ID,
						"tune": nt,
					})
				}
			}
			if pc.Paused || pc.CarbonOrder == nil {
				continue
			}
			co := pc.CarbonOrder
			if co.ID <= lastCarbonOrderID || nowMS-co.ID > 90000 {
				continue
			}
			lastCarbonOrderID = co.ID
			if isOpenOrCarbonBusy() || anyOpenAcctPos() || remotePaused.Load() || breakerTripped.Load() {
				fmt.Printf("[CARBON_REJECT id=%d sym=%s reason=BUSY_OR_PAUSED]\n", co.ID, co.Sym)
				continue
			}
			sym := strings.ToUpper(strings.TrimSpace(co.Sym))
			side := strings.ToUpper(strings.TrimSpace(co.Side))
			m, ok := markets[sym]
			st := mktStates[sym]
			if !ok || st == nil || (side != "LONG" && side != "SHORT") {
				fmt.Printf("[CARBON_REJECT id=%d sym=%s reason=UNKNOWN_MKT]\n", co.ID, sym)
				continue
			}
			ensureLtSubscribed(m.ID)
			st.mu.Lock()
			lb, la, bnB, bnA, bnM, bnTS, bas, wTicks := st.ltBid, st.ltAsk, st.bnBid, st.bnAsk, st.bnMid, st.lastEmaTS, st.basisBps, st.basisWarmTicks
			st.mu.Unlock()
			if lb <= 0 || la <= lb {
				lb, la, _, _ = fetchLtBookREST(m.ID)
			}
			if lb <= 0 || la <= lb || bnM <= 0 || time.Since(bnTS) > 15*time.Second {
				fmt.Printf("[CARBON_REJECT id=%d sym=%s reason=STALE_OR_MISSING_BBO]\n", co.ID, sym)
				continue
			}
			spBps := (la - lb) / (0.5 * (lb + la)) * 10000.0
			if spBps > 3.5 {
				fmt.Printf("[CARBON_REJECT id=%d sym=%s reason=WIDE_SPREAD_%.2fbps]\n", co.ID, sym, spBps)
				continue
			}
			rawOver := (la - bnA) / bnA * 10000.0
			if side == "SHORT" {
				rawOver = (bnB - lb) / bnB * 10000.0
			}
			overpayBps := rawOver
			if wTicks >= 10 {
				if side == "LONG" {
					overpayBps = math.Min(rawOver, rawOver-bas)
				} else {
					overpayBps = math.Min(rawOver, rawOver+bas)
				}
			}
			if overpayBps > 3.0 {
				fmt.Printf("[CARBON_REJECT id=%d sym=%s side=%s reason=OVERPAY_%.2fbps]\n", co.ID, sym, side, overpayBps)
				continue
			}
			usd := math.Min(240.0, math.Max(11.5, co.USD))
			slBps := math.Min(55.0, math.Max(8.0, co.SlBps))
			tpBps := co.TpBps
			if tpBps < 5.0 {
				tpBps = math.Round(slBps * 2.5)
			}
			maxHold := co.MaxHoldSec
			if maxHold <= 0 {
				maxHold = 900
			}
			ordSide := "BID"
			entryPx := roundTo(la*1.00035, m.PxDec)
			refPx := la
			if side == "SHORT" {
				ordSide = "ASK"
				entryPx = roundTo(lb*0.99965, m.PxDec)
				refPx = lb
			}
			stratTag := strings.ToUpper(strings.TrimSpace(co.Strat))
			if stratTag == "" {
				stratTag = "BN_MOM_SNIPE"
			}
			stratMu.Lock()
			if stratStats[stratTag] == nil {
				stratStats[stratTag] = &StratStat{}
			}
			stratMu.Unlock()
			sz := math.Max(m.MinB, roundTo(usd/refPx, m.SzDec))
			carbonMu.Lock()
			activeCarbon = &CarbonPos{
				ID: co.ID, Sym: sym, Side: side, USD: usd, SlBps: slBps, TpBps: tpBps,
				MaxHoldSec: maxHold, SentAt: time.Now(), EntryPx: refPx, StratTag: stratTag,
			}
			carbonMu.Unlock()
			recordStratOpen(co.ID, stratTag, sym+"_"+side, usd, usd/12.0, -overpayBps,
				fmt.Sprintf("carbon_pick sl=-%.0f tp=+%.0f hold=%ds", slBps, tpBps, maxHold))
			placePairOrder(sym, ordSide, entryPx, 0.5*(lb+la), sz, false, true)
			fmt.Printf("[CARBON_EXEC id=%d sym=%s side=%s strat=%s usd=%.2f px=%.5f overpay=%+.2fbp sl=-%.0f tp=+%.0f hold=%ds]\n",
				co.ID, sym, side, stratTag, usd, refPx, overpayBps, slBps, tpBps, maxHold)
		}
	}
}

func appendStratJournal(ev map[string]any) {
	b, err := json.Marshal(ev)
	if err != nil {
		return
	}
	journalMu.Lock()
	defer journalMu.Unlock()
	dir := "/opt/crypto-arb-bot/l2_archive"
	if os.MkdirAll(dir, 0755) != nil {
		dir = "/tmp/l2_archive"
		_ = os.MkdirAll(dir, 0755)
	}
	f, err := os.OpenFile(filepath.Join(dir, "strategy_journal.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	_, _ = f.Write(append(b, '\n'))
	_ = f.Close()
}

func getTune() CarbonTune {
	tuneMu.Lock()
	defer tuneMu.Unlock()
	return curTune
}

func calcCapMult() (float64, float64) {
	curEq := float64(eqNow.Load()) / 1e6
	if curEq < 50.0 {
		curEq = 93.7854
	}
	gain := math.Max(0.0, curEq-93.7854) + math.Max(0.0, float64(sessionRealizedMicroUSD.Load())/1e6)
	nw := float64(sessionNetWins.Load())
	mult := 1.00 + math.Max(0.0, nw)*0.12 + gain*0.20
	if probationForce.Load() || nw < 0 {
		mult = 0.70
	}
	mult = math.Min(2.20, math.Max(0.70, mult))
	return mult, curEq
}

func recordStratOpen(id int64, strat, legs string, usd, mult, edgeBps float64, detail string) {
	fmt.Printf("[STRAT_OPEN id=%d strat=%s legs=%q usd=$%.2f mult=%.2fx edge=%+.2fbp %s]\n",
		id, strat, legs, usd, mult, edgeBps, detail)
	appendStratJournal(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339),
		"ev":      "OPEN",
		"id":      id,
		"strat":   strat,
		"legs":    legs,
		"usd":     math.Round(usd*100) / 100,
		"mult":    math.Round(mult*100) / 100,
		"edgeBps": math.Round(edgeBps*100) / 100,
		"detail":  detail,
	})
}

func recordStratClose(id int64, strat, legs string, usd, holdSec, pnlBps, pnlUSD float64, reason string) {
	stratMu.Lock()
	st := stratStats[strat]
	if st == nil {
		st = &StratStat{}
		stratStats[strat] = st
	}
	if pnlUSD > 0.0005 {
		st.Wins++
	} else if pnlUSD < -0.0005 {
		st.Losses++
	}
	st.PnlUSD += pnlUSD
	stratMu.Unlock()
	awaitingReview.Store(true)
	fmt.Printf("[STRAT_CLOSE id=%d strat=%s legs=%q usd=$%.2f hold=%.1fs pnl=%+.2fbp($%+.4f) reason=%s]\n",
		id, strat, legs, usd, holdSec, pnlBps, pnlUSD, reason)
	fmt.Printf("[STRAT_REVIEW_DUE id=%d strat=%s legs=%q pnl=%+.2fbp($%+.4f) reason=%s eq=$%.4f]\n",
		id, strat, legs, pnlBps, pnlUSD, reason, float64(eqNow.Load())/1e6)
	appendStratJournal(map[string]any{
		"ts":      time.Now().UTC().Format(time.RFC3339),
		"ev":      "CLOSE",
		"id":      id,
		"strat":   strat,
		"legs":    legs,
		"usd":     math.Round(usd*100) / 100,
		"holdSec": math.Round(holdSec*10) / 10,
		"pnlBps":  math.Round(pnlBps*100) / 100,
		"pnlUSD":  math.Round(pnlUSD*10000) / 10000,
		"reason":  reason,
	})
}

func printStratBoard() {
	upM := math.Max(0.05, time.Since(bootTS).Minutes())
	mult, curEq := calcCapMult()
	stratMu.Lock()
	lag := *stratStats["BN_LAG_ARB"]
	dn := *stratStats["ZERO_FEE_DN"]
	tw := *stratStats["TWIN_EQUALIZE"]
	mom := *stratStats["BN_MOM_SNIPE"]
	stratMu.Unlock()
	totPnl := lag.PnlUSD + dn.PnlUSD + tw.PnlUSD + mom.PnlUSD
	realPnl := float64(sessionRealizedMicroUSD.Load()) / 1e6
	if math.Abs(realPnl) > math.Abs(totPnl) && dn.Wins+dn.Losses == 0 {
		totPnl = realPnl
	}
	rateH := totPnl / (upM / 60.0)
	fmt.Printf("[STRAT_BOARD up=%.1fm eq=$%.4f mult=%.2fx rate=$%+.2f/h] ZERO_FEE_DN:%dW/%dL($%+.4f) | BN_LAG_ARB:%dW/%dL($%+.3f) | BN_MOM_SNIPE:%dW/%dL($%+.3f)\n",
		upM, curEq, mult, rateH,
		dn.Wins, dn.Losses, dn.PnlUSD,
		lag.Wins, lag.Losses, lag.PnlUSD,
		mom.Wins, mom.Losses, mom.PnlUSD)
}

func isOpenOrCarbonBusy() bool {
	openMu.Lock()
	bo := activeOpen != nil
	openMu.Unlock()
	carbonMu.Lock()
	bc := activeCarbon != nil
	carbonMu.Unlock()
	return bo || bc || dnBusyFlag.Load()
}

func isActiveOpenSym(sym string) bool {
	openMu.Lock()
	defer openMu.Unlock()
	return activeOpen != nil && (activeOpen.SymA == sym || activeOpen.SymB == sym)
}

func canOpenNewStrat() bool {
	if awaitingReview.Load() || remotePaused.Load() || breakerTripped.Load() || pairMarketActive.Load() || anyOpenAcctPos() || isOpenOrCarbonBusy() {
		return false
	}
	if time.Now().UnixMilli() < stratCoolUntilMS.Load() {
		return false
	}
	pairMu.Lock()
	now := time.Now()
	for _, tw := range twins {
		if now.Sub(tw.LastDualSendTS) < 2*time.Second || tw.WasHedged {
			pairMu.Unlock()
			return false
		}
	}
	pairMu.Unlock()
	return true
}

func fireDualIOC(tag, symA, sideA string, pxA, midA, szA float64, symB, sideB string, pxB, midB, szB float64, isClose bool) {
	stA, stB := mktStates[symA], mktStates[symB]
	mA, okA := markets[symA]
	mB, okB := markets[symB]
	if stA == nil || stB == nil || !okA || !okB || pxA <= 0 || pxB <= 0 || szA <= 0 || szB <= 0 {
		return
	}
	if !isClose {
		szA = math.Max(mA.MinB, szA)
		szB = math.Max(mB.MinB, szB)
	}
	aBaseInt := int64(math.Round(szA * math.Pow10(mA.SzDec)))
	bBaseInt := int64(math.Round(szB * math.Pow10(mB.SzDec)))
	if aBaseInt <= 0 || bBaseInt <= 0 {
		return
	}
	aPxInt := uint32(math.Round(pxA * math.Pow10(mA.PxDec)))
	bPxInt := uint32(math.Round(pxB * math.Pow10(mB.PxDec)))
	var askA, askB, redOnly uint8
	if sideA == "ASK" {
		askA = 1
	}
	if sideB == "ASK" {
		askB = 1
	}
	if isClose {
		redOnly = 1
	}
	coiA := coiSeq.Add(1)
	coiB := coiSeq.Add(1)
	now := time.Now()

	stA.mu.Lock()
	stA.lastOrdTS[sideA] = now
	stA.activeOrds[sideA] = &ActiveOrder{COI: coiA, Px: pxA, BaseInt: aBaseInt, RefMid: midA, IsClose: isClose, IsIOC: true, TS: now}
	stA.mu.Unlock()

	stB.mu.Lock()
	stB.lastOrdTS[sideB] = now
	stB.activeOrds[sideB] = &ActiveOrder{COI: coiB, Px: pxB, BaseInt: bBaseInt, RefMid: midB, IsClose: isClose, IsIOC: true, TS: now}
	stB.mu.Unlock()

	enqueueTx(func() {
		txMu.Lock()
		defer txMu.Unlock()
		t0 := time.Now()
		tiA, errA := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      mA.ID,
			ClientOrderIndex: coiA,
			BaseAmount:       aBaseInt,
			Price:            aPxInt,
			IsAsk:            askA,
			Type:             1,
			TimeInForce:      0,
			ReduceOnly:       redOnly,
			OrderExpiry:      0,
		}, nextOpts())
		if errA != nil {
			fmt.Printf("[%s_SIGN_ERR_A] %v\n", tag, errA)
			return
		}
		infoA, errA := tiA.GetTxInfo()
		if errA != nil {
			fmt.Printf("[%s_INFO_ERR_A] %v\n", tag, errA)
			return
		}
		tiB, errB := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex:      mB.ID,
			ClientOrderIndex: coiB,
			BaseAmount:       bBaseInt,
			Price:            bPxInt,
			IsAsk:            askB,
			Type:             1,
			TimeInForce:      0,
			ReduceOnly:       redOnly,
			OrderExpiry:      0,
		}, nextOpts())
		if errB != nil {
			fmt.Printf("[%s_SIGN_ERR_B] %v\n", tag, errB)
			return
		}
		infoB, errB := tiB.GetTxInfo()
		if errB != nil {
			fmt.Printf("[%s_INFO_ERR_B] %v\n", tag, errB)
			return
		}

		idA := fmt.Sprintf("da_%d", coiA)
		idB := fmt.Sprintf("db_%d", coiB)
		if useWsSend {
			txWsMu.Lock()
			tc := txConn
			var wsErrA, wsErrB error
			if tc != nil {
				wsErrA = tc.WriteJSON(map[string]any{
					"type": "jsonapi/sendtx",
					"data": map[string]any{"id": idA, "tx_type": tiA.GetTxType(), "tx_info": json.RawMessage(infoA)},
				})
				if wsErrA == nil {
					wsErrB = tc.WriteJSON(map[string]any{
						"type": "jsonapi/sendtx",
						"data": map[string]any{"id": idB, "tx_type": tiB.GetTxType(), "tx_info": json.RawMessage(infoB)},
					})
				}
			}
			txWsMu.Unlock()
			if tc != nil && wsErrA == nil && wsErrB == nil {
				fmt.Printf("[WS_%s %s_%s/%s_%s px=%.4f/%.4f sz=%.5f/%.5f ro=%d] %dus\n",
					tag, symA, sideA, symB, sideB, pxA, pxB, szA, szB, redOnly, time.Since(t0).Microseconds())
				return
			}
		}

		var wg sync.WaitGroup
		wg.Add(2)
		postLeg := func(lid string, tt uint8, tiStr string) {
			defer wg.Done()
			form := url.Values{}
			form.Set("tx_type", strconv.Itoa(int(tt)))
			form.Set("tx_info", tiStr)
			if resp, e := txHTTP.PostForm(BaseURL+"/api/v1/sendTx", form); e == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 || !strings.Contains(string(b), `"code":200`) {
					fmt.Printf("[HTTP_%s_ERR %s] %d %s\n", tag, lid, resp.StatusCode, string(b))
				}
			}
		}
		go postLeg(idA, tiA.GetTxType(), infoA)
		go postLeg(idB, tiB.GetTxType(), infoB)
		wg.Wait()
		fmt.Printf("[HTTP_%s %s/%s ro=%d] %dms\n", tag, symA, symB, redOnly, time.Since(t0).Milliseconds())
	})
}

func startAutoCarbonSnipe(sym, side, stratTag string, usd, slBps, tpBps, diffBps float64) {
	m, ok := markets[sym]
	st := mktStates[sym]
	if !ok || st == nil || !canOpenNewStrat() {
		return
	}
	ensureLtSubscribed(m.ID)
	st.mu.Lock()
	lb, la, lbSz, laSz := st.ltBid, st.ltAsk, st.ltBidSz, st.ltAskSz
	st.mu.Unlock()
	if lb <= 0 || la <= lb {
		return
	}
	if (la-lb)/(0.5*(lb+la))*10000.0 > 2.0 {
		return
	}
	usd = math.Min(165.0, math.Max(14.0, usd))
	topBBO := la * laSz
	ordSide := "BID"
	entryPx := roundTo(la*1.00035, m.PxDec)
	refPx := la
	if side == "SHORT" {
		topBBO = lb * lbSz
		ordSide = "ASK"
		entryPx = roundTo(lb*0.99965, m.PxDec)
		refPx = lb
	}
	if topBBO < usd*1.15 || !takeWriteSlot(false) {
		return
	}
	sl := math.Min(34.0, math.Max(16.0, slBps))
	tp := math.Max(18.0, tpBps)
	if stratTag == "" {
		stratTag = "ADAPT_MOM"
	}
	stratMu.Lock()
	if stratStats[stratTag] == nil {
		stratStats[stratTag] = &StratStat{}
	}
	stratMu.Unlock()
	sz := math.Max(m.MinB, roundTo(usd/refPx, m.SzDec))
	coID := coiSeq.Add(1)
	carbonMu.Lock()
	activeCarbon = &CarbonPos{
		ID: coID, Sym: sym, Side: side, USD: usd, SlBps: sl, TpBps: tp,
		MaxHoldSec: 720, SentAt: time.Now(), EntryPx: refPx, StratTag: stratTag,
	}
	carbonMu.Unlock()
	recordStratOpen(coID, stratTag, sym+"_"+side, usd, usd/24.0, -diffBps, fmt.Sprintf("auto_1leg sl=-%.0f tp=+%.0f", sl, tp))
	placePairOrder(sym, ordSide, entryPx, 0.5*(lb+la), sz, false, true)
}

func evaluateFastStrategies() {
	if remotePaused.Load() || breakerTripped.Load() || len(commonList) == 0 {
		return
	}
	now := time.Now()
	bnEthBtcMu.Lock()
	ebBid, ebAsk, ebTS := bnEthBtcBid, bnEthBtcAsk, bnEthBtcTS
	bnEthBtcMu.Unlock()
	ethBtcFresh := ebBid > 0 && ebAsk >= ebBid && now.Sub(ebTS) < 8*time.Second

	type fastCand struct {
		ca         *CommonAsset
		lb, la     float64
		topBidUSD  float64
		topAskUSD  float64
		ltSp       float64
		depth      float64
		longAlpha  float64
		shortAlpha float64
		imp3s      float64
		imp20s     float64
	}
	var cands []fastCand
	bestLagDesc := "none"
	bestLagEdge := -999.0
	bestLagSym := ""
	bestLagSide := ""
	bestLagIdx := -1

	for _, ca := range commonList {
		if ca.LtVol24h < 500000.0 || ca.BnVol24h < 15000000.0 || !isSymTradeable24hOrOpen(ca.Sym, now) {
			continue
		}
		st := mktStates[ca.Sym]
		if st == nil {
			continue
		}
		st.mu.Lock()
		lb, la, lbSz, laSz, ldep := st.ltBid, st.ltAsk, st.ltBidSz, st.ltAskSz, math.Min(st.ltBidDepthUSD, st.ltAskDepthUSD)
		bb, ba, bm, eu, ef, bas := st.bnBid, st.bnAsk, st.bnMid, st.emaUltra, st.emaFast, st.basisBps
		ltAge, bnAge, wTicks := now.Sub(st.lastBookTS), now.Sub(st.lastEmaTS), st.basisWarmTicks
		st.mu.Unlock()

		if wTicks < 25 || ltAge > 8*time.Second || bnAge > 5*time.Second || lb <= 0 || la <= lb || bb <= 0 || ba <= bb || bm <= 0 || eu <= 0 || ef <= 0 {
			continue
		}
		ltMid := 0.5 * (lb + la)
		ltSp := (la - lb) / ltMid * 10000.0
		bnSp := (ba - bb) / (0.5 * (bb + ba)) * 10000.0
		if ltSp > 2.2 || bnSp > 1.8 || ldep < 180.0 {
			continue
		}
		fairBid := bb * (1.0 + bas/10000.0)
		fairAsk := ba * (1.0 + bas/10000.0)
		laEdge := (fairBid - la) / la * 10000.0
		saEdge := (lb - fairAsk) / lb * 10000.0
		i3 := (bm - eu) / eu * 10000.0
		i20 := (bm - ef) / ef * 10000.0
		fc := fastCand{
			ca: ca, lb: lb, la: la, topBidUSD: lb * lbSz, topAskUSD: la * laSz,
			ltSp: ltSp, depth: ldep, longAlpha: laEdge, shortAlpha: saEdge, imp3s: i3, imp20s: i20,
		}
		cands = append(cands, fc)
		idx := len(cands) - 1
		if laEdge > bestLagEdge {
			bestLagEdge = laEdge
			bestLagSym = ca.Sym
			bestLagSide = "LONG"
			bestLagDesc = ca.Sym + "_LONG"
			bestLagIdx = idx
		}
		if saEdge > bestLagEdge {
			bestLagEdge = saEdge
			bestLagSym = ca.Sym
			bestLagSide = "SHORT"
			bestLagDesc = ca.Sym + "_SHORT"
			bestLagIdx = idx
		}
	}

	bestTriDesc := "none"
	bestTriEdge := -999.0

	for i := range cands {
		ci := &cands[i]
		if ci.ca.LtVol24h < 1200000.0 || ci.ltSp > 1.8 {
			continue
		}
		for j := range cands {
			if i == j {
				continue
			}
			cj := &cands[j]
			if cj.ca.LtVol24h < 1200000.0 || cj.ltSp > 1.8 {
				continue
			}
			edge := ci.longAlpha + cj.shortAlpha
			if ethBtcFresh {
				if ci.ca.Sym == "ETH" && cj.ca.Sym == "BTC" {
					ebEdge := (ebBid - ci.la/cj.lb) / ebBid * 10000.0
					if ebEdge < -1.5 {
						continue
					}
					if ebEdge > 0 {
						edge += math.Min(1.5, 0.5*ebEdge)
					}
				} else if ci.ca.Sym == "BTC" && cj.ca.Sym == "ETH" {
					fairBE := 1.0 / ebAsk
					beEdge := (fairBE - ci.la/cj.lb) / fairBE * 10000.0
					if beEdge < -1.5 {
						continue
					}
					if beEdge > 0 {
						edge += math.Min(1.5, 0.5*beEdge)
					}
				}
			}
			if edge > bestTriEdge {
				bestTriEdge = edge
				bestTriDesc = ci.ca.Sym + "/" + cj.ca.Sym
			}
		}
	}
	if bestLagEdge < -900 {
		bestLagEdge = 0
	}
	if bestTriEdge < -900 {
		bestTriEdge = 0
	}

	snapMu.Lock()
	snapLagDesc = bestLagDesc
	snapLagEdge = bestLagEdge
	snapTriDesc = bestTriDesc
	snapTriEdge = bestTriEdge
	snapMu.Unlock()

	tn := getTune()
	if !tn.AllowLag || !canOpenNewStrat() || bestLagIdx < 0 || bestLagEdge < tn.MinLagBps {
		return
	}
	lagCand := cands[bestLagIdx]
	if lagCand.ca.LtVol24h < 1200000.0 || lagCand.ltSp > 1.8 {
		return
	}
	dirOK := (bestLagSide == "LONG" && lagCand.imp3s >= 2.5 && lagCand.imp20s >= 3.0 && lagCand.imp3s <= 35.0) ||
		(bestLagSide == "SHORT" && lagCand.imp3s <= -2.5 && lagCand.imp20s <= -3.0 && lagCand.imp3s >= -35.0)
	slBps := 16.0
	tpBps := math.Min(18.0, math.Max(8.0, bestLagEdge+4.0))
	usd := math.Min(165.0, math.Max(14.0, (tn.RiskUSD*10000.0)/slBps))
	usd = math.Min(usd, 0.35*lagCand.depth)
	topOK := (bestLagSide == "LONG" && lagCand.topAskUSD >= usd*1.2) || (bestLagSide == "SHORT" && lagCand.topBidUSD >= usd*1.2)
	if !dirOK || !topOK || usd < 13.5 || lagCand.depth < usd*1.4 || !takeWriteSlot(false) {
		return
	}
	m, ok := markets[bestLagSym]
	if !ok {
		return
	}
	ensureLtSubscribed(m.ID)
	ordSide := "BID"
	entryPx := roundTo(lagCand.la*1.00035, m.PxDec)
	refPx := lagCand.la
	if bestLagSide == "SHORT" {
		ordSide = "ASK"
		entryPx = roundTo(lagCand.lb*0.99965, m.PxDec)
		refPx = lagCand.lb
	}
	sz := math.Max(m.MinB, roundTo(usd/refPx, m.SzDec))
	oid := coiSeq.Add(1)
	openMu.Lock()
	activeOpen = &ActiveOpenPos{
		ID: oid, Mode: "BN_LAG_ARB", SymA: bestLagSym, SideA: bestLagSide,
		LegUSD: usd, EdgeBps: bestLagEdge, Mult: usd / 83.0, TpBps: tpBps, SlBps: slBps,
		SentAt: now, EntryPxA: refPx,
	}
	openMu.Unlock()
	recordStratOpen(oid, "BN_LAG_ARB", bestLagSym+"_"+bestLagSide, usd, usd/83.0, bestLagEdge,
		fmt.Sprintf("imp3s=%+.2f imp20s=%+.2f sl=-%.0f tp=+%.1f", lagCand.imp3s, lagCand.imp20s, slBps, tpBps))
	placePairOrder(bestLagSym, ordSide, entryPx, 0.5*(lagCand.lb+lagCand.la), sz, false, true)
}

func manageActiveOpenStrat() {
	openMu.Lock()
	if activeOpen == nil {
		openMu.Unlock()
		return
	}
	ap := *activeOpen
	openMu.Unlock()

	now := time.Now()
	age := now.Sub(ap.SentAt)

	if ap.Mode == "TRI_EQUALIZE" {
		stA, stB := mktStates[ap.SymA], mktStates[ap.SymB]
		mA, okA := markets[ap.SymA]
		mB, okB := markets[ap.SymB]
		if stA == nil || stB == nil || !okA || !okB {
			return
		}
		stA.mu.Lock()
		lbA, laA, posA, aeA, bbA, baA, basA := stA.ltBid, stA.ltAsk, stA.pos, stA.avgEntry, stA.bnBid, stA.bnAsk, stA.basisBps
		stA.mu.Unlock()
		stB.mu.Lock()
		lbB, laB, posB, aeB, bbB, baB, basB := stB.ltBid, stB.ltAsk, stB.pos, stB.avgEntry, stB.bnBid, stB.bnAsk, stB.basisBps
		stB.mu.Unlock()
		if lbA <= 0 || laA <= lbA || lbB <= 0 || laB <= lbB {
			return
		}
		midA, midB := 0.5*(lbA+laA), 0.5*(lbB+laB)
		flatA := math.Abs(posA) < mA.MinB
		flatB := math.Abs(posB) < mB.MinB
		usdA := math.Abs(posA) * midA
		usdB := math.Abs(posB) * midB
		legs := ap.SymA + "_L/" + ap.SymB + "_S"

		if flatA && flatB {
			if ap.Closing {
				recordStratClose(ap.ID, "TRI_EQUALIZE", legs, ap.LegUSD, age.Seconds(), ap.CloseBps, ap.CloseUSD, ap.CloseReason)
				cool := ap.CoolSec
				if cool <= 0 {
					cool = 10
				}
				stratCoolUntilMS.Store(now.Add(time.Duration(cool) * time.Second).UnixMilli())
				openMu.Lock()
				if activeOpen != nil && activeOpen.ID == ap.ID {
					activeOpen = nil
				}
				openMu.Unlock()
			} else if age >= 2500*time.Millisecond {
				fmt.Printf("[STRAT_MISS id=%d strat=TRI_EQUALIZE legs=%q reason=BOTH_IOC_NO_FILL]\n", ap.ID, legs)
				stratCoolUntilMS.Store(now.Add(15 * time.Second).UnixMilli())
				openMu.Lock()
				if activeOpen != nil && activeOpen.ID == ap.ID {
					activeOpen = nil
				}
				openMu.Unlock()
			}
			return
		}

		if ap.Closing {
			if now.Sub(ap.LastCloseAt) >= 1200*time.Millisecond && takeWriteSlotTier(2) {
				openMu.Lock()
				if activeOpen != nil && activeOpen.ID == ap.ID {
					activeOpen.LastCloseAt = now
				}
				openMu.Unlock()
				if !flatA {
					if posA > 0 {
						placePairOrder(ap.SymA, "ASK", roundTo(lbA*0.995, mA.PxDec), midA, math.Abs(posA), true, true)
					} else {
						placePairOrder(ap.SymA, "BID", roundTo(laA*1.005, mA.PxDec), midA, math.Abs(posA), true, true)
					}
				}
				if !flatB {
					if posB > 0 {
						placePairOrder(ap.SymB, "ASK", roundTo(lbB*0.995, mB.PxDec), midB, math.Abs(posB), true, true)
					} else {
						placePairOrder(ap.SymB, "BID", roundTo(laB*1.005, mB.PxDec), midB, math.Abs(posB), true, true)
					}
				}
			}
			return
		}

		// <=550ms Asymmetric Fill Guard: never hold 1 naked leg!
		if age >= 550*time.Millisecond && ((usdA >= 8.0 && usdB < 5.0) || (usdB >= 8.0 && usdA < 5.0) || math.Abs(posA*midA+posB*midB) >= math.Max(20.0, ap.LegUSD*0.45)) {
			if takeWriteSlotTier(2) {
				if !flatA {
					if posA > 0 {
						placePairOrder(ap.SymA, "ASK", roundTo(lbA*0.995, mA.PxDec), midA, math.Abs(posA), true, true)
					} else {
						placePairOrder(ap.SymA, "BID", roundTo(laA*1.005, mA.PxDec), midA, math.Abs(posA), true, true)
					}
				}
				if !flatB {
					if posB > 0 {
						placePairOrder(ap.SymB, "ASK", roundTo(lbB*0.995, mB.PxDec), midB, math.Abs(posB), true, true)
					} else {
						placePairOrder(ap.SymB, "BID", roundTo(laB*1.005, mB.PxDec), midB, math.Abs(posB), true, true)
					}
				}
				openMu.Lock()
				if activeOpen != nil && activeOpen.ID == ap.ID {
					activeOpen.Closing = true
					activeOpen.LastCloseAt = now
					activeOpen.CloseReason = "ASYM_LEG_GUARD"
					activeOpen.CloseBps = -2.0
					activeOpen.CloseUSD = -math.Max(usdA, usdB) * 0.0002
					activeOpen.CoolSec = 45
				}
				openMu.Unlock()
			}
			return
		}

		if usdA < 5.0 || usdB < 5.0 {
			return
		}
		if aeA <= 0 {
			aeA = ap.EntryPxA
		}
		if aeB <= 0 {
			aeB = ap.EntryPxB
		}
		if aeA <= 0 || aeB <= 0 {
			return
		}
		retA := (lbA - aeA) / aeA * 10000.0
		retB := (aeB - laB) / aeB * 10000.0
		execNetBps := retA + retB
		pnlUSD := (retA/10000.0)*(math.Abs(posA)*aeA) + (retB/10000.0)*(math.Abs(posB)*aeB)

		remGap := 2.0
		if bbA > 0 && baA > bbA && bbB > 0 && baB > bbB {
			fMidA := 0.5 * (bbA + baA) * (1.0 + basA/10000.0)
			fMidB := 0.5 * (bbB + baB) * (1.0 + basB/10000.0)
			remGap = (fMidA-midA)/fMidA*10000.0 + (midB-fMidB)/fMidB*10000.0
		}

		openMu.Lock()
		if activeOpen != nil && activeOpen.ID == ap.ID {
			activeOpen.LastPnlBps = execNetBps
			activeOpen.LastPnlUSD = pnlUSD
		}
		openMu.Unlock()

		reason := ""
		cool := int64(10)
		if (execNetBps >= 2.8 && remGap <= 1.0) || execNetBps >= 5.2 {
			reason = "EQUALIZED_TP"
		} else if execNetBps <= -ap.SlBps {
			reason = "SL"
			cool = 35
		} else if age >= 240*time.Second && execNetBps >= 0.8 {
			reason = "TIME_PROFIT"
		} else if age >= 420*time.Second {
			reason = "MAX_HOLD"
		}

		if reason != "" && takeWriteSlotTier(2) {
			openMu.Lock()
			if activeOpen != nil && activeOpen.ID == ap.ID {
				activeOpen.Closing = true
				activeOpen.LastCloseAt = now
				activeOpen.CloseReason = reason
				activeOpen.CloseBps = execNetBps
				activeOpen.CloseUSD = pnlUSD
				activeOpen.CoolSec = cool
			}
			openMu.Unlock()
			fireDualIOC("TRI_CLOSE", ap.SymA, "ASK", roundTo(lbA*0.995, mA.PxDec), midA, math.Abs(posA),
				ap.SymB, "BID", roundTo(laB*1.005, mB.PxDec), midB, math.Abs(posB), true)
		}
		return
	}

	// BN_LAG_ARB (1-leg management)
	st := mktStates[ap.SymA]
	m, ok := markets[ap.SymA]
	if st == nil || !ok {
		return
	}
	st.mu.Lock()
	lb, la, pos, ae, bb, ba, bas := st.ltBid, st.ltAsk, st.pos, st.avgEntry, st.bnBid, st.bnAsk, st.basisBps
	closeSide := "ASK"
	if ap.SideA == "SHORT" {
		closeSide = "BID"
	}
	closeOrd := st.activeOrds[closeSide]
	st.mu.Unlock()
	legs := ap.SymA + "_" + ap.SideA

	if math.Abs(pos) < m.MinB {
		if ap.TpSent || ap.Closing {
			reasonOut := ap.CloseReason
			bpsOut := ap.LastPnlBps
			usdOut := ap.LastPnlUSD
			cool := ap.CoolSec
			if reasonOut == "" {
				reasonOut = "LIMIT_TP"
				bpsOut = ap.TpBps
				usdOut = ap.LegUSD * ap.TpBps / 10000.0
				cool = 10
			} else if ap.CloseUSD != 0 {
				bpsOut = ap.CloseBps
				usdOut = ap.CloseUSD
			}
			recordStratClose(ap.ID, "BN_LAG_ARB", legs, ap.LegUSD, age.Seconds(), bpsOut, usdOut, reasonOut)
			if cool <= 0 {
				cool = 10
			}
			stratCoolUntilMS.Store(now.Add(time.Duration(cool) * time.Second).UnixMilli())
			openMu.Lock()
			if activeOpen != nil && activeOpen.ID == ap.ID {
				activeOpen = nil
			}
			openMu.Unlock()
		} else if age >= 2500*time.Millisecond {
			fmt.Printf("[STRAT_MISS id=%d strat=BN_LAG_ARB legs=%q reason=IOC_NO_FILL]\n", ap.ID, legs)
			stratCoolUntilMS.Store(now.Add(8 * time.Second).UnixMilli())
			openMu.Lock()
			if activeOpen != nil && activeOpen.ID == ap.ID {
				activeOpen = nil
			}
			openMu.Unlock()
		}
		return
	}

	entry := ap.EntryPxA
	if ae > 0 {
		entry = ae
	}
	if entry <= 0 || lb <= 0 || la <= lb {
		return
	}
	tMid := 0.5 * (lb + la)

	if ap.Closing {
		if now.Sub(ap.LastCloseAt) >= 1200*time.Millisecond && takeWriteSlotTier(2) {
			if closeOrd != nil && !closeOrd.IsIOC {
				cancelPairSide(ap.SymA, closeSide)
			}
			exitPx := roundTo(lb*0.995, m.PxDec)
			if ap.SideA == "SHORT" {
				exitPx = roundTo(la*1.005, m.PxDec)
			}
			openMu.Lock()
			if activeOpen != nil && activeOpen.ID == ap.ID {
				activeOpen.LastCloseAt = now
			}
			openMu.Unlock()
			placePairOrder(ap.SymA, closeSide, exitPx, tMid, math.Abs(pos), true, true)
		}
		return
	}

	if !ap.TpSent {
		tpPx := entry * (1.0 + ap.TpBps/10000.0)
		if ap.SideA == "SHORT" {
			tpPx = entry * (1.0 - ap.TpBps/10000.0)
		}
		tpPx = roundTo(tpPx, m.PxDec)
		openMu.Lock()
		if activeOpen != nil && activeOpen.ID == ap.ID && !activeOpen.TpSent {
			activeOpen.EntryPxA = entry
			activeOpen.TpSent = true
		}
		openMu.Unlock()
		if takeWriteSlot(true) {
			placePairOrder(ap.SymA, closeSide, tpPx, tMid, math.Abs(pos), true, false)
			fmt.Printf("[STRAT_TP_RESTED id=%d sym=%s tp=%.5f(+%.1fbp)]\n", ap.ID, ap.SymA, tpPx, ap.TpBps)
		}
		return
	}

	var pnlBps float64
	if ap.SideA == "LONG" {
		pnlBps = (lb - entry) / entry * 10000.0
	} else {
		pnlBps = (entry - la) / entry * 10000.0
	}
	posUSD := math.Abs(pos) * entry
	pnlUSD := posUSD * pnlBps / 10000.0

	remStale := 2.0
	if bb > 0 && ba > bb {
		if ap.SideA == "LONG" {
			fairBid := bb * (1.0 + bas/10000.0)
			remStale = (fairBid - lb) / lb * 10000.0
		} else {
			fairAsk := ba * (1.0 + bas/10000.0)
			remStale = (la - fairAsk) / la * 10000.0
		}
	}

	openMu.Lock()
	if activeOpen != nil && activeOpen.ID == ap.ID {
		activeOpen.LastPnlBps = pnlBps
		activeOpen.LastPnlUSD = pnlUSD
		if !ap.BeArmed && pnlBps >= 3.8 {
			activeOpen.BeArmed = true
			ap.BeArmed = true
		}
	}
	openMu.Unlock()

	slThresh := -ap.SlBps
	if ap.BeArmed {
		slThresh = 1.8
	}
	reason := ""
	cool := int64(10)
	if pnlBps >= 3.4 && remStale <= 0.6 {
		reason = "EQUALIZED_TP"
	} else if pnlBps <= slThresh {
		if ap.BeArmed {
			reason = "BE_STOP"
		} else {
			reason = "SL"
			cool = 35
		}
	} else if age >= 90*time.Second {
		reason = "MAX_HOLD"
	}

	if reason != "" && takeWriteSlotTier(2) {
		if closeOrd != nil && !closeOrd.IsIOC {
			cancelPairSide(ap.SymA, closeSide)
		}
		exitPx := roundTo(lb*0.995, m.PxDec)
		if ap.SideA == "SHORT" {
			exitPx = roundTo(la*1.005, m.PxDec)
		}
		openMu.Lock()
		if activeOpen != nil && activeOpen.ID == ap.ID {
			activeOpen.Closing = true
			activeOpen.LastCloseAt = now
			activeOpen.CloseReason = reason
			activeOpen.CloseBps = pnlBps
			activeOpen.CloseUSD = pnlUSD
			activeOpen.CoolSec = cool
		}
		openMu.Unlock()
		placePairOrder(ap.SymA, closeSide, exitPx, tMid, math.Abs(pos), true, true)
	}
}

func runOpenStratLoop() {
	time.Sleep(8 * time.Second)
	evalTick := time.NewTicker(450 * time.Millisecond)
	mgrTick := time.NewTicker(100 * time.Millisecond)
	defer evalTick.Stop()
	defer mgrTick.Stop()
	lastEval := time.Time{}
	for {
		select {
		case <-fastArbCh:
			for len(fastArbCh) > 0 {
				<-fastArbCh
			}
			if time.Since(lastEval) >= 120*time.Millisecond {
				lastEval = time.Now()
				evaluateFastStrategies()
			}
		case <-evalTick.C:
			if time.Since(lastEval) >= 400*time.Millisecond {
				lastEval = time.Now()
				evaluateFastStrategies()
			}
		case <-mgrTick.C:
			manageActiveOpenStrat()
		}
	}
}

const (
	BnApiKey = "I4IImhi629VlyPD28pnIkveLZsAwH5yf0COTtmJQuYiGFICACdCS03HEXz2OCti4"
	BnApiSec = "ng4mncCOkk5DvRJxPARevMGoDACQBEpzWBqh5XJ8TlEYCxR0TWpSp8vKVitg3U4D"
)

type DnSpec struct {
	Sym     string
	BnSym   string
	QDec    int
	PxDec   int
	Step    float64
	Tick    float64
	MinNot  float64
}

type DnBook struct {
	Bid       float64
	Ask       float64
	BidQ      float64
	AskQ      float64
	TS        time.Time
	EmaBps    float64
	WarmTicks int
	LastEmaTS time.Time
}

var (
	dnSpecs = []DnSpec{
		{Sym: "SOL", BnSym: "SOLUSDC", QDec: 2, PxDec: 2, Step: 0.01, Tick: 0.01, MinNot: 5.5},
		{Sym: "ETH", BnSym: "ETHUSDC", QDec: 3, PxDec: 2, Step: 0.001, Tick: 0.01, MinNot: 20.5},
		{Sym: "XRP", BnSym: "XRPUSDC", QDec: 1, PxDec: 4, Step: 0.1, Tick: 0.0001, MinNot: 5.5},
		{Sym: "DOGE", BnSym: "DOGEUSDC", QDec: 0, PxDec: 5, Step: 1.0, Tick: 0.00001, MinNot: 5.5},
		{Sym: "SUI", BnSym: "SUIUSDC", QDec: 1, PxDec: 4, Step: 0.1, Tick: 0.0001, MinNot: 5.5},
		{Sym: "LINK", BnSym: "LINKUSDC", QDec: 2, PxDec: 3, Step: 0.01, Tick: 0.001, MinNot: 5.5},
		{Sym: "AVAX", BnSym: "AVAXUSDC", QDec: 2, PxDec: 3, Step: 0.01, Tick: 0.001, MinNot: 5.5},
		{Sym: "ENA", BnSym: "ENAUSDC", QDec: 0, PxDec: 5, Step: 1.0, Tick: 0.00001, MinNot: 5.5},
		{Sym: "WLD", BnSym: "WLDUSDC", QDec: 1, PxDec: 4, Step: 0.1, Tick: 0.0001, MinNot: 5.5},
		{Sym: "ZEC", BnSym: "ZECUSDC", QDec: 3, PxDec: 2, Step: 0.001, Tick: 0.01, MinNot: 5.5},
		{Sym: "HYPE", BnSym: "HYPEUSDC", QDec: 2, PxDec: 3, Step: 0.01, Tick: 0.001, MinNot: 5.5},
		{Sym: "ADA", BnSym: "ADAUSDC", QDec: 1, PxDec: 4, Step: 0.1, Tick: 0.0001, MinNot: 5.5},
		{Sym: "NEAR", BnSym: "NEARUSDC", QDec: 0, PxDec: 3, Step: 1.0, Tick: 0.001, MinNot: 5.5},
		{Sym: "ARB", BnSym: "ARBUSDC", QDec: 1, PxDec: 5, Step: 0.1, Tick: 0.00001, MinNot: 5.5},
		{Sym: "LTC", BnSym: "LTCUSDC", QDec: 3, PxDec: 2, Step: 0.001, Tick: 0.01, MinNot: 5.5},
	}
	dnSpecMap  = map[string]DnSpec{}
	dnMu       sync.Mutex
	dnBooks    = map[string]*DnBook{}
	dnBusyFlag atomic.Bool
	bnHTTP     = &http.Client{
		Timeout: 3500 * time.Millisecond,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	dnActiveSym string
	dnBnSide    string // "BUY" or "SELL" on Binance USDC-M
	dnLtSide    string // "ASK" or "BID" on Lighter
	dnEntryOid  int64
	dnExitOid   int64
	dnQty       float64
	dnEntryBnPx float64
	dnEntryLtPx float64
	dnState     string // "IDLE", "BN_ENTRY_OPEN", "HEDGED", "BN_EXIT_OPEN"
	dnTradeID   int64
	dnOpenedAt  time.Time
	dnRefLtPx   float64
	dnPnlSumUSD float64
)

func bnSignReq(method, baseHost, path string, params url.Values) (int, []byte, error) {
	if params == nil {
		params = url.Values{}
	}
	params.Set("timestamp", strconv.FormatInt(time.Now().UnixMilli(), 10))
	params.Set("recvWindow", "5000")
	qs := params.Encode()
	sig := hex.EncodeToString(hmacSHA256Raw([]byte(BnApiSec), qs))
	fullURL := fmt.Sprintf("https://%s%s?%s&signature=%s", baseHost, path, qs, sig)
	req, err := http.NewRequest(method, fullURL, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("X-MBX-APIKEY", BnApiKey)
	resp, err := bnHTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, b, nil
}

func onBnUsdcBookTick(sym string, b, a, bq, aq float64) {
	if _, ok := dnSpecMap[sym]; !ok {
		return
	}
	st := mktStates[sym]
	var ltB, ltA float64
	var ltTS time.Time
	if st != nil {
		st.mu.Lock()
		ltB, ltA, ltTS = st.ltBid, st.ltAsk, st.lastBookTS
		st.mu.Unlock()
	}
	now := time.Now()
	dnMu.Lock()
	db := dnBooks[sym]
	if db == nil {
		db = &DnBook{}
		dnBooks[sym] = db
	}
	db.Bid, db.Ask, db.BidQ, db.AskQ, db.TS = b, a, bq, aq, now
	if ltB > 0 && ltA > ltB && now.Sub(ltTS) < 8*time.Second {
		ltMid := 0.5 * (ltB + ltA)
		bnMid := 0.5 * (b + a)
		ltSp := (ltA - ltB) / ltMid * 10000.0
		bnSp := (a - b) / bnMid * 10000.0
		rawBps := (ltMid - bnMid) / bnMid * 10000.0
		if ltSp <= 6.0 && bnSp <= 5.0 && (db.WarmTicks == 0 || now.Sub(db.LastEmaTS) >= 800*time.Millisecond) {
			if db.WarmTicks == 0 {
				db.EmaBps = rawBps
			} else if dnState == "IDLE" {
				dt := math.Min(8.0, math.Max(0.2, now.Sub(db.LastEmaTS).Seconds()))
				db.EmaBps += (1.0 - math.Exp(-dt/90.0)) * (rawBps - db.EmaBps)
			}
			db.LastEmaTS = now
			db.WarmTicks++
		}
	}
	dnMu.Unlock()
}

func setupBnUsdcMargin() bool {
	for _, sp := range dnSpecs {
		dnSpecMap[sp.Sym] = sp
		if m, ok := markets[sp.Sym]; ok {
			ensureLtSubscribed(m.ID)
		}
	}
	// 1. Check existing UMFUTURE USDC balance:
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/balance", nil)
	if err != nil || sc != 200 {
		fmt.Printf("[DN_BOOT_ERR] fapi balance status=%d err=%v body=%s\n", sc, err, string(body))
		return false
	}
	var bals []struct {
		Asset string `json:"asset"`
		Bal   string `json:"balance"`
		Avail string `json:"availableBalance"`
	}
	_ = json.Unmarshal(body, &bals)
	usdcBal := 0.0
	for _, x := range bals {
		if x.Asset == "USDC" || x.Asset == "USDT" {
			av, _ := strconv.ParseFloat(x.Avail, 64)
			if x.Asset == "USDC" {
				usdcBal = av
			}
		}
	}
	// 2a. Also ensure $45 USDT is in UMFUTURE for high-yield USDT-M hourly funding arbs (e.g. MSTR +28bp!):
	var usdtFut float64
	for _, x := range bals {
		if x.Asset == "USDT" {
			usdtFut, _ = strconv.ParseFloat(x.Avail, 64)
		}
	}
	if usdtFut < 35.0 {
		tU := url.Values{}
		tU.Set("type", "MAIN_UMFUTURE")
		tU.Set("asset", "USDT")
		tU.Set("amount", "45.00")
		tSc, tB, _ := bnSignReq("POST", "api.binance.com", "/sapi/v1/asset/transfer", tU)
		fmt.Printf("[DN_USDT_TRANSFER] moved $45.00 USDT MAIN->UMFUTURE status=%d resp=%s\n", tSc, string(tB))
		time.Sleep(200 * time.Millisecond)
	}
	// 2. If UMFUTURE USDC < $28, convert $20 Spot USDT -> Spot USDC and transfer $32 USDC to UMFUTURE:
	if usdcBal < 28.0 {
		qP := url.Values{}
		qP.Set("fromAsset", "USDT")
		qP.Set("toAsset", "USDC")
		qP.Set("fromAmount", "20.00")
		qP.Set("walletType", "SPOT")
		if qSc, qB, qE := bnSignReq("POST", "api.binance.com", "/sapi/v1/convert/getQuote", qP); qE == nil && qSc == 200 {
			var qr struct {
				QuoteID string `json:"quoteId"`
			}
			if json.Unmarshal(qB, &qr) == nil && qr.QuoteID != "" {
				aP := url.Values{}
				aP.Set("quoteId", qr.QuoteID)
				aP.Set("walletType", "SPOT")
				_, _, _ = bnSignReq("POST", "api.binance.com", "/sapi/v1/convert/acceptQuote", aP)
				time.Sleep(250 * time.Millisecond)
			}
		} else {
			mP := url.Values{}
			mP.Set("symbol", "USDCUSDT")
			mP.Set("side", "BUY")
			mP.Set("type", "MARKET")
			mP.Set("quoteOrderQty", "20.00")
			_, _, _ = bnSignReq("POST", "api.binance.com", "/api/v3/order", mP)
			time.Sleep(250 * time.Millisecond)
		}
		// Check Spot USDC free and transfer up to $32 USDC -> UMFUTURE:
		if sSc, sB, sE := bnSignReq("GET", "api.binance.com", "/api/v3/account", nil); sE == nil && sSc == 200 {
			var spAcc struct {
				Balances []struct {
					Asset string `json:"asset"`
					Free  string `json:"free"`
				} `json:"balances"`
			}
			if json.Unmarshal(sB, &spAcc) == nil {
				for _, sb := range spAcc.Balances {
					if sb.Asset == "USDC" {
						fv, _ := strconv.ParseFloat(sb.Free, 64)
						trAmt := math.Floor(math.Min(32.0, fv-1.0)*100.0) / 100.0
						if trAmt >= 10.0 {
							tP := url.Values{}
							tP.Set("type", "MAIN_UMFUTURE")
							tP.Set("asset", "USDC")
							tP.Set("amount", fmt.Sprintf("%.2f", trAmt))
							tSc, tB, _ := bnSignReq("POST", "api.binance.com", "/sapi/v1/asset/transfer", tP)
							fmt.Printf("[DN_FUND_TRANSFER] moved $%.2f USDC MAIN->UMFUTURE status=%d resp=%s\n", trAmt, tSc, string(tB))
							time.Sleep(250 * time.Millisecond)
						}
					}
				}
			}
		}
		if sc2, b2, e2 := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/balance", nil); e2 == nil && sc2 == 200 {
			_ = json.Unmarshal(b2, &bals)
			for _, x := range bals {
				if x.Asset == "USDC" {
					usdcBal, _ = strconv.ParseFloat(x.Avail, 64)
				}
			}
		}
	}
	// Ensure One-Way Position Mode & 5x leverage on active DN symbols:
	pmP := url.Values{}
	pmP.Set("dualSidePosition", "false")
	_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/positionSide/dual", pmP)
	for _, sp := range []string{"SOLUSDC", "ETHUSDC", "XRPUSDC", "HYPEUSDC", "ZECUSDC", "SUIUSDC", "MSTRUSDT", "COINUSDT", "CRCLUSDT"} {
		lvP := url.Values{}
		lvP.Set("symbol", sp)
		lvP.Set("leverage", "5")
		_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/leverage", lvP)
	}
	fmt.Printf("[DN_READY] Binance UMFUTURE 0%%-Maker USDC Avail=$%.2f + USDT Funding Ready across %d pairs!\n", usdcBal, len(dnSpecs))
	return usdcBal >= 12.0 || usdtFut >= 25.0
}

func flattenBnDnAndSweep() {
	dnMu.Lock()
	sym := dnActiveSym
	dnState = "IDLE"
	dnActiveSym = ""
	dnMu.Unlock()
	dnBusyFlag.Store(false)

	// Cancel open UMFUTURE orders on all DN pairs or active sym:
	symsToClean := []string{"SOLUSDC", "ETHUSDC", "XRPUSDC", "DOGEUSDC", "SUIUSDC", "LINKUSDC", "AVAXUSDC", "ENAUSDC", "WLDUSDC", "ZECUSDC", "HYPEUSDC", "ADAUSDC", "NEARUSDC", "ARBUSDC", "LTCUSDC"}
	if sym != "" {
		if sp, ok := dnSpecMap[sym]; ok {
			p := url.Values{}
			p.Set("symbol", sp.BnSym)
			_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", p)
		}
	}
	// Close any open UMFUTURE positions:
	if sc, b, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/positionRisk", nil); err == nil && sc == 200 {
		var prs []struct {
			Symbol string `json:"symbol"`
			Amt    string `json:"positionAmt"`
		}
		if json.Unmarshal(b, &prs) == nil {
			for _, pr := range prs {
				amt, _ := strconv.ParseFloat(pr.Amt, 64)
				if math.Abs(amt) > 1e-6 {
					p := url.Values{}
					p.Set("symbol", pr.Symbol)
					_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", p)
					cSide := "SELL"
					if amt < 0 {
						cSide = "BUY"
					}
					qDec := 3
					if pr.Symbol == "MSTRUSDT" || pr.Symbol == "COINUSDT" || pr.Symbol == "CRCLUSDT" {
						qDec = 2
					}
					for _, s := range dnSpecs {
						if s.BnSym == pr.Symbol {
							qDec = s.QDec
						}
					}
					cp := url.Values{}
					cp.Set("symbol", pr.Symbol)
					cp.Set("side", cSide)
					cp.Set("type", "MARKET")
					cp.Set("reduceOnly", "true")
					cp.Set("quantity", strconv.FormatFloat(math.Abs(amt), 'f', qDec, 64))
					_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", cp)
				}
			}
		}
	}
	_ = symsToClean
	// Sweep UMFUTURE USDC & USDT back to Spot on pause so halt_all_orders_and_repay sees 100% clean Spot!
	if sc, b, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/balance", nil); err == nil && sc == 200 {
		var bals []struct {
			Asset string `json:"asset"`
			Avail string `json:"availableBalance"`
		}
		if json.Unmarshal(b, &bals) == nil {
			for _, x := range bals {
				if x.Asset == "USDC" || x.Asset == "USDT" {
					av, _ := strconv.ParseFloat(x.Avail, 64)
					sw := math.Floor(av*100.0) / 100.0
					if sw >= 0.50 {
						tp := url.Values{}
						tp.Set("type", "UMFUTURE_MAIN")
						tp.Set("asset", x.Asset)
						tp.Set("amount", fmt.Sprintf("%.2f", sw))
						_, _, _ = bnSignReq("POST", "api.binance.com", "/sapi/v1/asset/transfer", tp)
					}
				}
			}
		}
	}
}

func placeLtHedgeIOC(sym, side string, px, mid, qty float64, isClose bool) {
	m, ok := markets[sym]
	st := mktStates[sym]
	if !ok || st == nil || qty <= 0 || px <= 0 {
		return
	}
	placePairOrder(sym, side, roundTo(px, m.PxDec), mid, qty, isClose, true)
}

func runHourlyFundingSnipe() {
	// Dedicated :00 Hourly Double-Paid Funding Arb Sniper (e.g. MSTR: Short Binance +26bp / Long Lighter +1.7bp = +28bp!)
	var snipedHour int64 = -1
	for {
		time.Sleep(1500 * time.Millisecond)
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now().UTC()
		curHourID := now.Unix() / 3600
		minInHr := now.Minute()
		secInMin := now.Second()
		// Trigger window: :50:00 to :58:45 UTC before the top of the hour!
		if minInHr < 50 || (minInHr == 58 && secInMin > 45) || minInHr == 59 || snipedHour == curHourID {
			continue
		}
		if awaitingReview.Load() || anyOpenAcctPos() || dnBusyFlag.Load() {
			continue
		}
		nextHr := (now.Hour() + 1) % 24
		// Check MSTR (id=122) live funding + live books:
		m, ok := markets["MSTR"]
		st := mktStates["MSTR"]
		if !ok || st == nil {
			continue
		}
		ensureLtSubscribed(m.ID)
		// Fetch live Binance MSTRUSDT premiumIndex & bookTicker + Lighter funding-rates:
		hc := &http.Client{Timeout: 3 * time.Second}
		prResp, e1 := hc.Get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=MSTRUSDT")
		bkResp, e2 := hc.Get("https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=MSTRUSDT")
		frResp, e3 := hc.Get(BaseURL + "/api/v1/funding-rates")
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		prB, _ := io.ReadAll(prResp.Body); prResp.Body.Close()
		bkB, _ := io.ReadAll(bkResp.Body); bkResp.Body.Close()
		frB, _ := io.ReadAll(frResp.Body); frResp.Body.Close()
		var pr struct {
			Rate string `json:"lastFundingRate"`
		}
		var bk struct {
			Bid string `json:"bidPrice"`
			Ask string `json:"askPrice"`
		}
		var fr struct {
			Rates []struct {
				ID   int64   `json:"market_id"`
				Exch string  `json:"exchange"`
				Rate float64 `json:"rate"`
			} `json:"funding_rates"`
		}
		if json.Unmarshal(prB, &pr) != nil || json.Unmarshal(bkB, &bk) != nil || json.Unmarshal(frB, &fr) != nil {
			continue
		}
		bnR, _ := strconv.ParseFloat(pr.Rate, 64)
		bnBps := 0.0
		if nextHr%8 == 0 { // MSTRUSDT settles at 00:00, 08:00, 16:00 UTC!
			bnBps = bnR * 10000.0
		}
		ltBps := 0.0
		for _, r := range fr.Rates {
			if r.ID == 122 && r.Exch == "lighter" {
				ltBps = (r.Rate / 8.0) * 10000.0
			}
		}
		netFundBps := bnBps - ltBps // Positive means SHORT Binance + LONG Lighter collects netFundBps!
		bb, _ := strconv.ParseFloat(bk.Bid, 64)
		ba, _ := strconv.ParseFloat(bk.Ask, 64)
		lb, la, _, laUSD := fetchLtBookREST(m.ID)
		if bb <= 0 || ba <= 0 || lb <= 0 || la <= lb {
			continue
		}
		entrySlipBps := (la - bb) / bb * 10000.0 // Buy Lt @ Ask, Short Bn @ Bid (or Maker Ask)
		// Require net funding after 8bp conservative round-trip Binance fees + entry spread >= +10.0 bps!
		if netFundBps-entrySlipBps-8.0 < 10.0 || laUSD < 180.0 {
			continue
		}
		snipedHour = curHourID
		dnBusyFlag.Store(true)

		// Size $150 hedged notional (~1.86 MSTR: safe 3.3x on $45 Binance USDT & 1.6x on $93 Lighter!):
		qty := math.Floor((150.0/la)/0.01) * 0.01
		if qty < 0.25 {
			dnBusyFlag.Store(false)
			continue
		}
		tid := time.Now().UnixMilli()
		// 1) Open SHORT on Binance MSTRUSDT first (try GTX Maker @ Ask for 2.5s, fallback MARKET so we never miss :00!):
		bnFillPx := bb
		p := url.Values{}
		p.Set("symbol", "MSTRUSDT")
		p.Set("side", "SELL")
		p.Set("type", "LIMIT")
		p.Set("timeInForce", "GTX")
		p.Set("quantity", fmt.Sprintf("%.2f", qty))
		p.Set("price", fmt.Sprintf("%.2f", ba))
		sc, ob, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
		filledBn := false
		if err == nil && sc == 200 {
			var or struct {
				OrderID int64  `json:"orderId"`
				Status  string `json:"status"`
			}
			if json.Unmarshal(ob, &or) == nil && or.OrderID > 0 && or.Status != "EXPIRED" {
				for w := 0; w < 10; w++ {
					time.Sleep(250 * time.Millisecond)
					qp := url.Values{}
					qp.Set("symbol", "MSTRUSDT")
					qp.Set("orderId", strconv.FormatInt(or.OrderID, 10))
					if gSc, gB, gE := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp); gE == nil && gSc == 200 {
						var goi struct {
							Status string `json:"status"`
							AvgPx  string `json:"avgPrice"`
						}
						_ = json.Unmarshal(gB, &goi)
						if goi.Status == "FILLED" {
							filledBn = true
							bnFillPx, _ = strconv.ParseFloat(goi.AvgPx, 64)
							if bnFillPx <= 0 {
								bnFillPx = ba
							}
							break
						}
					}
				}
				if !filledBn {
					dp := url.Values{}
					dp.Set("symbol", "MSTRUSDT")
					dp.Set("orderId", strconv.FormatInt(or.OrderID, 10))
					_, dB, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", dp)
					var dc struct {
						Status string `json:"status"`
					}
					_ = json.Unmarshal(dB, &dc)
					if dc.Status == "FILLED" {
						filledBn = true
						bnFillPx = ba
					}
				}
			}
		}
		if !filledBn {
			mp := url.Values{}
			mp.Set("symbol", "MSTRUSDT")
			mp.Set("side", "SELL")
			mp.Set("type", "MARKET")
			mp.Set("quantity", fmt.Sprintf("%.2f", qty))
			mSc, mB, mE := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
			if mE != nil || mSc != 200 {
				fmt.Printf("[DN_FUND_ERR] Binance MSTRUSDT entry failed sc=%d body=%s\n", mSc, string(mB))
				dnBusyFlag.Store(false)
				continue
			}
			var mo struct {
				AvgPx string `json:"avgPrice"`
			}
			_ = json.Unmarshal(mB, &mo)
			bnFillPx, _ = strconv.ParseFloat(mo.AvgPx, 64)
			if bnFillPx <= 0 {
				bnFillPx = bb
			}
		}
		// 2) Immediately hedge LONG exact qty on 0%-fee Lighter!
		lb2, la2, _, _ := fetchLtBookREST(m.ID)
		if la2 <= 0 {
			la2 = la
		}
		placeLtHedgeIOC("MSTR", "BID", roundTo(la2*1.004, m.PxDec), 0.5*(lb2+la2), qty, false)
		recordStratOpen(tid, "ZERO_FEE_DN", "FUND_16Z_MSTR_LONG_LT_SHORT_BN", qty*bnFillPx, 1.0, netFundBps, fmt.Sprintf("bnFill=%.2f ltAsk=%.2f", bnFillPx, la2))
		fmt.Printf("[DN_FUND_LOCKED id=%d] Bought %.2f MSTR on Lighter @ ~$%.2f + Shorted %.2f MSTR on Binance @ $%.2f | Waiting for :00:08 UTC payout (+%.2fbp)!\n",
			tid, qty, la2, qty, bnFillPx, netFundBps)

		// 3) Hold delta-neutral across :00:00 UTC until :00:10 UTC (so BOTH venues credit funding!), then unwind cleanly!
		targetUnwind := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 59, 59, 0, time.UTC).Add(11 * time.Second)
		for time.Now().UTC().Before(targetUnwind) {
			if remotePaused.Load() {
				dnBusyFlag.Store(false)
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
		// 4) Unwind BOTH legs after :00:10 UTC funding credit!
		cp := url.Values{}
		cp.Set("symbol", "MSTRUSDT")
		cp.Set("side", "BUY")
		cp.Set("type", "MARKET")
		cp.Set("reduceOnly", "true")
		cp.Set("quantity", fmt.Sprintf("%.2f", qty))
		_, cB, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", cp)
		var co struct {
			AvgPx string `json:"avgPrice"`
		}
		_ = json.Unmarshal(cB, &co)
		bnExitPx, _ := strconv.ParseFloat(co.AvgPx, 64)
		lb3, la3, _, _ := fetchLtBookREST(m.ID)
		if bnExitPx <= 0 && ba > 0 {
			bnExitPx = lb3
		}
		st.mu.Lock()
		ltPos, ltAe := st.pos, st.avgEntry
		st.mu.Unlock()
		if ltAe <= 0 {
			ltAe = la2
		}
		closeQty := qty
		if math.Abs(ltPos) >= m.MinB {
			closeQty = math.Abs(ltPos)
		}
		placeLtHedgeIOC("MSTR", "ASK", roundTo(lb3*0.995, m.PxDec), 0.5*(lb3+la3), closeQty, true)
		pricePnlUSD := (bnFillPx-bnExitPx)*qty + (lb3-ltAe)*qty
		fundUSD := (qty * bnFillPx) * (netFundBps / 10000.0)
		bnFeesUSD := (qty * bnFillPx) * 0.0006 // ~6bp blended maker/taker fee
		netUSD := pricePnlUSD + fundUSD - bnFeesUSD
		netBps := netUSD / (qty * bnFillPx) * 10000.0
		recordStratClose(tid, "ZERO_FEE_DN", "FUND_16Z_MSTR_LONG_LT_SHORT_BN", qty*bnFillPx, time.Since(now).Seconds(), netBps, netUSD, "HOURLY_FUND_CAPTURED")
		fmt.Printf("[DN_FUND_DONE id=%d] Closed MSTR Hedge | PricePnL=$%+.4f + Funding=$%+.4f - Fees=$%.4f => NET=$%+.4f (%+.2fbp)!\n",
			tid, pricePnlUSD, fundUSD, bnFeesUSD, netUSD, netBps)
		dnBusyFlag.Store(false)
	}
}

func runZeroFeeDnLoop() {
	time.Sleep(5 * time.Second)
	if remotePaused.Load() || !setupBnUsdcMargin() {
		return
	}
	go runHourlyFundingSnipe()
	dnMu.Lock()
	dnState = "IDLE"
	dnMu.Unlock()

	tick := time.NewTicker(120 * time.Millisecond)
	defer tick.Stop()
	lastScanLog := time.Now()
	var lastOrderPoll time.Time

	for range tick.C {
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now()
		dnMu.Lock()
		stNow := dnState
		actSym := dnActiveSym
		dnMu.Unlock()

		if stNow == "IDLE" {
			if awaitingReview.Load() || anyOpenAcctPos() || now.UnixMilli() < stratCoolUntilMS.Load() {
				continue
			}
			bestSym := ""
			bestBnSide := ""
			bestLtSide := ""
			bestEdge := -999.0
			bestBnPx := 0.0
			bestLtPx := 0.0
			bestEma := 0.0

			for _, sp := range dnSpecs {
				m, ok := markets[sp.Sym]
				st := mktStates[sp.Sym]
				if !ok || st == nil {
					continue
				}
				st.mu.Lock()
				lb, la, lbSz, laSz, lTS := st.ltBid, st.ltAsk, st.ltBidSz, st.ltAskSz, st.lastBookTS
				st.mu.Unlock()
				if lb <= 0 || la <= lb || now.Sub(lTS) > 5*time.Second {
					continue
				}
				dnMu.Lock()
				db := dnBooks[sp.Sym]
				var bb, ba, ema float64
				var wTicks int
				var bTS time.Time
				if db != nil {
					bb, ba, ema, wTicks, bTS = db.Bid, db.Ask, db.EmaBps, db.WarmTicks, db.TS
				}
				dnMu.Unlock()
				if bb <= 0 || ba <= bb || wTicks < 12 || now.Sub(bTS) > 4*time.Second {
					continue
				}
				ltSp := (la - lb) / lb * 10000.0
				bnSp := (ba - bb) / bb * 10000.0
				if ltSp > 3.2 || bnSp > 3.2 {
					continue
				}
				// Case A: Lighter spikes ABOVE dynamic EMA -> BUY Maker on Binance USDC-M @ Bid (or Ask-tick) + SELL Taker on Lighter @ Bid!
				bnBuyPx := bb
				if ba-bb >= sp.Tick*1.9 {
					bnBuyPx = roundTo(ba-sp.Tick, sp.PxDec)
				}
				basisA := (lb - bnBuyPx) / bnBuyPx * 10000.0
				edgeA := basisA - ema
				if edgeA > bestEdge && lb*lbSz >= 45.0 {
					bestEdge = edgeA
					bestSym = sp.Sym
					bestBnSide = "BUY"
					bestLtSide = "ASK"
					bestBnPx = bnBuyPx
					bestLtPx = lb
					bestEma = ema
				}
				// Case B: Lighter dips BELOW dynamic EMA -> SELL Maker on Binance USDC-M @ Ask (or Bid+tick) + BUY Taker on Lighter @ Ask!
				bnSellPx := ba
				if ba-bb >= sp.Tick*1.9 {
					bnSellPx = roundTo(bb+sp.Tick, sp.PxDec)
				}
				basisB := (la - bnSellPx) / bnSellPx * 10000.0
				edgeB := ema - basisB
				if edgeB > bestEdge && la*laSz >= 45.0 {
					bestEdge = edgeB
					bestSym = sp.Sym
					bestBnSide = "SELL"
					bestLtSide = "BID"
					bestBnPx = bnSellPx
					bestLtPx = la
					bestEma = ema
				}
				_ = m
			}

			if now.Sub(lastScanLog) >= 12*time.Second && bestSym != "" {
				lastScanLog = now
				fmt.Printf("[DN_SCAN] best=%s bn=%s edge=%+.2fbp (ema=%+.2fbp) bnPx=%.5f ltPx=%.5f\n",
					bestSym, bestBnSide, bestEdge, bestEma, bestBnPx, bestLtPx)
			}

			// Enter when 0%-fee dynamic basis dislocation >= +2.20 bps!
			if bestSym != "" && bestEdge >= 2.20 {
				sp := dnSpecMap[bestSym]
				m := markets[bestSym]
				// Step common quantity for ~$25.00 clip (honoring both Binance Step and Lighter SzDec + $11 min):
				commonStep := math.Max(sp.Step, math.Pow10(-m.SzDec))
				rawQty := math.Floor((25.0/bestBnPx)/commonStep+1e-9) * commonStep
				qtyDec := sp.QDec
				if m.SzDec < qtyDec {
					qtyDec = m.SzDec
				}
				qty := roundTo(rawQty, qtyDec)
				if qty*bestBnPx < math.Max(12.0, sp.MinNot) || qty < m.MinB {
					continue
				}
				p := url.Values{}
				p.Set("symbol", sp.BnSym)
				p.Set("side", bestBnSide)
				p.Set("type", "LIMIT")
				p.Set("timeInForce", "GTX") // Strictly 0.00% Maker PostOnly!
				p.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
				p.Set("price", strconv.FormatFloat(bestBnPx, 'f', sp.PxDec, 64))
				sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
				if err == nil && sc == 200 {
					var or struct {
						OrderID int64  `json:"orderId"`
						Status  string `json:"status"`
					}
					if json.Unmarshal(body, &or) == nil && or.OrderID > 0 && or.Status != "EXPIRED" && or.Status != "CANCELED" {
						tid := now.UnixMilli()
						dnMu.Lock()
						dnState = "BN_ENTRY_OPEN"
						dnActiveSym = bestSym
						dnBnSide = bestBnSide
						dnLtSide = bestLtSide
						dnEntryOid = or.OrderID
						dnQty = qty
						dnEntryBnPx = bestBnPx
						dnRefLtPx = bestLtPx
						dnOpenedAt = now
						dnTradeID = tid
						dnMu.Unlock()
						dnBusyFlag.Store(true)
						lastOrderPoll = time.Time{}
						fmt.Printf("[DN_MAKER_RESTED id=%d sym=%s bn=%s qty=%.4f @ %.5f edge=%+.2fbp (0%% Maker)]\n",
							tid, bestSym, bestBnSide, qty, bestBnPx, bestEdge)
					}
				}
			}

		} else if stNow == "BN_ENTRY_OPEN" {
			sp := dnSpecMap[actSym]
			m := markets[actSym]
			st := mktStates[actSym]
			st.mu.Lock()
			lb, la := st.ltBid, st.ltAsk
			st.mu.Unlock()
			dnMu.Lock()
			db := dnBooks[actSym]
			ema := 0.0
			if db != nil {
				ema = db.EmaBps
			}
			bSide, lSide, oid, ePx, refLt, openT, tid := dnBnSide, dnLtSide, dnEntryOid, dnEntryBnPx, dnRefLtPx, dnOpenedAt, dnTradeID
			dnMu.Unlock()

			// Check if Lighter edge faded or moved against our resting Maker before fill (<0.6 bps remaining or >12s stale):
			curEdge := 0.0
			ltMoved := false
			if bSide == "BUY" {
				curEdge = ((lb - ePx) / ePx * 10000.0) - ema
				ltMoved = lb < refLt*(1.0-0.8/10000.0)
			} else {
				curEdge = ema - ((la - ePx) / ePx * 10000.0)
				ltMoved = la > refLt*(1.0+0.8/10000.0)
			}
			needCancel := ltMoved || curEdge < 0.65 || now.Sub(openT) >= 12*time.Second
			if !needCancel && now.Sub(lastOrderPoll) < 160*time.Millisecond {
				continue
			}
			lastOrderPoll = now
			qp := url.Values{}
			qp.Set("symbol", sp.BnSym)
			qp.Set("orderId", strconv.FormatInt(oid, 10))
			sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp)
			if err != nil || sc != 200 {
				continue
			}
			var o struct {
				Status  string `json:"status"`
				ExecQty string `json:"executedQty"`
				AvgPx   string `json:"avgPrice"`
			}
			if json.Unmarshal(body, &o) != nil {
				continue
			}
			exQ, _ := strconv.ParseFloat(o.ExecQty, 64)
			avgP, _ := strconv.ParseFloat(o.AvgPx, 64)
			if avgP <= 0 {
				avgP = ePx
			}

			if o.Status == "FILLED" || (needCancel && exQ*avgP >= 10.5) {
				if o.Status != "FILLED" {
					_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
					var co struct {
						ExecQty string `json:"executedQty"`
					}
					if json.Unmarshal(cb, &co) == nil {
						if q2, e2 := strconv.ParseFloat(co.ExecQty, 64); e2 == nil && q2 > exQ {
							exQ = q2
						}
					}
				}
				tMid := 0.5 * (lb + la)
				ltIocPx := roundTo(lb*0.997, m.PxDec)
				ltRefFill := lb
				if lSide == "BID" {
					ltIocPx = roundTo(la*1.003, m.PxDec)
					ltRefFill = la
				}
				placeLtHedgeIOC(actSym, lSide, ltIocPx, tMid, exQ, false)
				dnMu.Lock()
				dnState = "HEDGED"
				dnQty = exQ
				dnEntryBnPx = avgP
				dnEntryLtPx = ltRefFill
				dnOpenedAt = now
				dnMu.Unlock()
				recordStratOpen(tid, "ZERO_FEE_DN", fmt.Sprintf("BN_%s_%s+LT_%s", actSym, bSide, lSide), exQ*avgP, 1.0, curEdge, "0pct_Maker_x_0pct_Lighter")
			} else if needCancel || o.Status == "CANCELED" || o.Status == "EXPIRED" {
				if o.Status == "NEW" || o.Status == "PARTIALLY_FILLED" {
					_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
					var co struct {
						ExecQty string `json:"executedQty"`
					}
					if json.Unmarshal(cb, &co) == nil {
						exQ, _ = strconv.ParseFloat(co.ExecQty, 64)
					}
				}
				if exQ > 0 {
					// Tiny sub-$10 partial fill: flatten immediately on Binance
					cSide := "SELL"
					if bSide == "SELL" {
						cSide = "BUY"
					}
					mp := url.Values{}
					mp.Set("symbol", sp.BnSym)
					mp.Set("side", cSide)
					mp.Set("type", "MARKET")
					mp.Set("reduceOnly", "true")
					mp.Set("quantity", strconv.FormatFloat(exQ, 'f', sp.QDec, 64))
					_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
				}
				dnMu.Lock()
				dnState = "IDLE"
				dnActiveSym = ""
				dnMu.Unlock()
				dnBusyFlag.Store(false)
			}

		} else if stNow == "HEDGED" {
			sp := dnSpecMap[actSym]
			m := markets[actSym]
			st := mktStates[actSym]
			st.mu.Lock()
			lb, la, ltPos, ltAvg := st.ltBid, st.ltAsk, st.pos, st.avgEntry
			st.mu.Unlock()
			dnMu.Lock()
			bSide, lSide, qty, eBn, eLt, openT, tid := dnBnSide, dnLtSide, dnQty, dnEntryBnPx, dnEntryLtPx, dnOpenedAt, dnTradeID
			db := dnBooks[actSym]
			var bb, ba float64
			if db != nil {
				bb, ba = db.Bid, db.Ask
			}
			dnMu.Unlock()

			// Safety guard: if Lighter IOC didn't fill within 2.5s, retry Lighter IOC once or flatten Binance cleanly!
			if math.Abs(ltPos) < m.MinB && now.Sub(openT) >= 2500*time.Millisecond {
				cSide := "SELL"
				if bSide == "SELL" {
					cSide = "BUY"
				}
				mp := url.Values{}
				mp.Set("symbol", sp.BnSym)
				mp.Set("side", cSide)
				mp.Set("type", "MARKET")
				mp.Set("reduceOnly", "true")
				mp.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
				_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
				fmt.Printf("[DN_UNHEDGED_BAIL id=%d sym=%s] Lighter leg missed; flattened Binance safely.\n", tid, actSym)
				dnMu.Lock()
				dnState = "IDLE"
				dnActiveSym = ""
				dnMu.Unlock()
				dnBusyFlag.Store(false)
				stratCoolUntilMS.Store(now.Add(10 * time.Second).UnixMilli())
				continue
			}
			if ltAvg > 0 {
				eLt = ltAvg
			}
			if bb <= 0 || ba <= bb || lb <= 0 || la <= lb {
				continue
			}

			// Compute live combined 2-leg PnL in bps (0% Maker Exit on Binance + 0% Taker Exit on Lighter = 0.00 bps fees!):
			var closeBnSide, closeLtSide string
			var makerExitBnPx, takerExitLtPx, combPnlBps float64
			if bSide == "BUY" {
				// Long Bn @ eBn, Short Lt @ eLt -> Exit: SELL Maker Bn @ Ask (or Bid+tick), BUY Taker Lt @ la
				closeBnSide = "SELL"
				closeLtSide = "BID"
				makerExitBnPx = ba
				if ba-bb >= sp.Tick*1.9 {
					makerExitBnPx = roundTo(bb+sp.Tick, sp.PxDec)
				}
				takerExitLtPx = la
				combPnlBps = ((makerExitBnPx - eBn) + (eLt - takerExitLtPx)) / eBn * 10000.0
			} else {
				// Short Bn @ eBn, Long Lt @ eLt -> Exit: BUY Maker Bn @ Bid (or Ask-tick), SELL Taker Lt @ lb
				closeBnSide = "BUY"
				closeLtSide = "ASK"
				makerExitBnPx = bb
				if ba-bb >= sp.Tick*1.9 {
					makerExitBnPx = roundTo(ba-sp.Tick, sp.PxDec)
				}
				takerExitLtPx = lb
				combPnlBps = ((eBn - makerExitBnPx) + (takerExitLtPx - eLt)) / eBn * 10000.0
			}

			holdS := now.Sub(openT).Seconds()
			// Take profit at >= +1.80 bps (100% pure net profit at 0.00% fees!), scratch >= +0.30 bps after 75s, or strict stop <= -6.5 bps / 240s max hold:
			wantExit := combPnlBps >= 1.80 || (holdS >= 75.0 && combPnlBps >= 0.30)
			emergencyExit := combPnlBps <= -6.50 || holdS >= 240.0

			if emergencyExit {
				// Immediate dual-close (Binance MARKET + Lighter IOC)
				mp := url.Values{}
				mp.Set("symbol", sp.BnSym)
				mp.Set("side", closeBnSide)
				mp.Set("type", "MARKET")
				mp.Set("reduceOnly", "true")
				mp.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
				_, mb, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
				var mo struct {
					AvgPx string `json:"avgPrice"`
				}
				_ = json.Unmarshal(mb, &mo)
				bnExPx, _ := strconv.ParseFloat(mo.AvgPx, 64)
				if bnExPx <= 0 {
					bnExPx = 0.5 * (bb + ba)
				}
				ltClosePx := roundTo(la*1.004, m.PxDec)
				if closeLtSide == "ASK" {
					ltClosePx = roundTo(lb*0.996, m.PxDec)
				}
				placeLtHedgeIOC(actSym, closeLtSide, ltClosePx, 0.5*(lb+la), math.Abs(ltPos), true)
				// Subtract 4 bps Binance taker fee on emergency exit
				realBps := combPnlBps - 4.0
				realUSD := (qty * eBn) * (realBps / 10000.0)
				recordStratClose(tid, "ZERO_FEE_DN", fmt.Sprintf("BN_%s_%s+LT_%s", actSym, bSide, lSide), qty*eBn, holdS, realBps, realUSD, "DN_TIME_OR_SL")
				dnMu.Lock()
				dnState = "IDLE"
				dnActiveSym = ""
				dnMu.Unlock()
				dnBusyFlag.Store(false)
				stratCoolUntilMS.Store(now.Add(15 * time.Second).UnixMilli())
				_ = lSide
			} else if wantExit {
				p := url.Values{}
				p.Set("symbol", sp.BnSym)
				p.Set("side", closeBnSide)
				p.Set("type", "LIMIT")
				p.Set("timeInForce", "GTX")
				p.Set("reduceOnly", "true")
				p.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
				p.Set("price", strconv.FormatFloat(makerExitBnPx, 'f', sp.PxDec, 64))
				sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
				if err == nil && sc == 200 {
					var or struct {
						OrderID int64  `json:"orderId"`
						Status  string `json:"status"`
					}
					if json.Unmarshal(body, &or) == nil && or.OrderID > 0 && or.Status != "EXPIRED" {
						dnMu.Lock()
						dnState = "BN_EXIT_OPEN"
						dnExitOid = or.OrderID
						dnRefLtPx = takerExitLtPx
						dnMu.Unlock()
						lastOrderPoll = time.Time{}
						fmt.Printf("[DN_EXIT_RESTED id=%d sym=%s side=%s @ %.5f estNet=%+.2fbp (0%% Fee)]\n",
							tid, actSym, closeBnSide, makerExitBnPx, combPnlBps)
					}
				}
			}

		} else if stNow == "BN_EXIT_OPEN" {
			sp := dnSpecMap[actSym]
			m := markets[actSym]
			st := mktStates[actSym]
			st.mu.Lock()
			lb, la, ltPos, ltAvg := st.ltBid, st.ltAsk, st.pos, st.avgEntry
			st.mu.Unlock()
			dnMu.Lock()
			bSide, lSide, exOid, qty, eBn, eLt, openT, tid := dnBnSide, dnLtSide, dnExitOid, dnQty, dnEntryBnPx, dnEntryLtPx, dnOpenedAt, dnTradeID
			dnMu.Unlock()
			if ltAvg > 0 {
				eLt = ltAvg
			}
			if now.Sub(lastOrderPoll) < 150*time.Millisecond {
				continue
			}
			lastOrderPoll = now
			qp := url.Values{}
			qp.Set("symbol", sp.BnSym)
			qp.Set("orderId", strconv.FormatInt(exOid, 10))
			sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp)
			if err != nil || sc != 200 {
				continue
			}
			var o struct {
				Status  string `json:"status"`
				ExecQty string `json:"executedQty"`
				AvgPx   string `json:"avgPrice"`
				Price   string `json:"price"`
			}
			if json.Unmarshal(body, &o) != nil {
				continue
			}
			exP, _ := strconv.ParseFloat(o.AvgPx, 64)
			if exP <= 0 {
				exP, _ = strconv.ParseFloat(o.Price, 64)
			}
			if o.Status == "FILLED" {
				closeLtSide := "BID"
				ltClosePx := roundTo(la*1.004, m.PxDec)
				ltExitEst := la
				if bSide == "SELL" {
					closeLtSide = "ASK"
					ltClosePx = roundTo(lb*0.996, m.PxDec)
					ltExitEst = lb
				}
				if math.Abs(ltPos) >= m.MinB {
					placeLtHedgeIOC(actSym, closeLtSide, ltClosePx, 0.5*(lb+la), math.Abs(ltPos), true)
				}
				var realBps float64
				if bSide == "BUY" {
					realBps = ((exP - eBn) + (eLt - ltExitEst)) / eBn * 10000.0
				} else {
					realBps = ((eBn - exP) + (ltExitEst - eLt)) / eBn * 10000.0
				}
				realUSD := (qty * eBn) * (realBps / 10000.0)
				holdS := now.Sub(openT).Seconds()
				recordStratClose(tid, "ZERO_FEE_DN", fmt.Sprintf("BN_%s_%s+LT_%s", actSym, bSide, lSide), qty*eBn, holdS, realBps, realUSD, "ZERO_FEE_TP")
				dnMu.Lock()
				dnPnlSumUSD += realUSD
				dnState = "IDLE"
				dnActiveSym = ""
				dnMu.Unlock()
				dnBusyFlag.Store(false)
				stratCoolUntilMS.Store(now.Add(6 * time.Second).UnixMilli())
			} else if o.Status == "CANCELED" || o.Status == "EXPIRED" {
				dnMu.Lock()
				dnState = "HEDGED"
				dnMu.Unlock()
			} else {
				// Re-check if combined PnL dropped below +0.5 bps while resting exit; if so, pull and return to HEDGED
				var curComb float64
				if bSide == "BUY" {
					curComb = ((exP - eBn) + (eLt - la)) / eBn * 10000.0
				} else {
					curComb = ((eBn - exP) + (lb - eLt)) / eBn * 10000.0
				}
				if curComb < 0.40 {
					_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
					var co struct {
						Status string `json:"status"`
					}
					_ = json.Unmarshal(cb, &co)
					if co.Status == "FILLED" {
						closeLtSide := "BID"
						ltClosePx := roundTo(la*1.004, m.PxDec)
						ltExitEst := la
						if bSide == "SELL" {
							closeLtSide = "ASK"
							ltClosePx = roundTo(lb*0.996, m.PxDec)
							ltExitEst = lb
						}
						if math.Abs(ltPos) >= m.MinB {
							placeLtHedgeIOC(actSym, closeLtSide, ltClosePx, 0.5*(lb+la), math.Abs(ltPos), true)
						}
						var realBps float64
						if bSide == "BUY" {
							realBps = ((exP - eBn) + (eLt - ltExitEst)) / eBn * 10000.0
						} else {
							realBps = ((eBn - exP) + (ltExitEst - eLt)) / eBn * 10000.0
						}
						realUSD := (qty * eBn) * (realBps / 10000.0)
						recordStratClose(tid, "ZERO_FEE_DN", fmt.Sprintf("BN_%s_%s+LT_%s", actSym, bSide, lSide), qty*eBn, now.Sub(openT).Seconds(), realBps, realUSD, "ZERO_FEE_TP_RACE")
						dnMu.Lock()
						dnPnlSumUSD += realUSD
						dnState = "IDLE"
						dnActiveSym = ""
						dnMu.Unlock()
						dnBusyFlag.Store(false)
						stratCoolUntilMS.Store(now.Add(6 * time.Second).UnixMilli())
					} else {
						dnMu.Lock()
						dnState = "HEDGED"
						dnMu.Unlock()
					}
				}
			}
		}
	}
}

func main() {
	if _, err := os.Stat("/tmp/lighter_pause.flag"); err == nil {
		fmt.Println("[PAUSED] /tmp/lighter_pause.flag active; exiting.")
		os.Exit(0)
	}
	hc := lthttp.NewClient(BaseURL)
	var err error
	txCli, err = client.NewTxClient(hc, PrivKey, AcctIdx, KeyIdx, ChainID)
	if err != nil {
		fmt.Printf("NewTxClient ERR: %v\n", err)
		os.Exit(1)
	}
	n, err := hc.GetNextNonce(AcctIdx, KeyIdx)
	if err != nil {
		fmt.Printf("GetNextNonce ERR: %v\n", err)
		os.Exit(1)
	}
	nonceSeq.Store(n)
	coiSeq.Store(time.Now().UnixMilli() % 10000000)
	probationForce.Store(false)
	discoverCommonAssets()
	for _, st := range mktStates {
		st.dynClipUSD = -1.0
		st.makerActive = false
	}

	go runTxWorker()
	go runTxWS()
	go runL2TickRecorder()
	go runBnAllBookTickerWS()
	go runLtWS()
	go runPauseWatchdog()
	time.Sleep(1500 * time.Millisecond)
	cancelAll()
	if trResp, e := (&http.Client{Timeout: 5 * time.Second}).Get(fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=100&sort_by=timestamp", BaseURL, AcctIdx)); e == nil {
		tb, _ := io.ReadAll(trResp.Body)
		trResp.Body.Close()
		if len(tb) > 20 && strings.Contains(string(tb), `"trades"`) {
			rebalanceFromTrades(tb)
		}
	}
	go runTwinWalkForwardLoop()
	go runPairHeartbeat()
	go runOpenStratLoop()
	go runCarbonScanner()
	go runCarbonCommandLoop()
	go runZeroFeeDnLoop()
	fmt.Printf("GO-TOKYO v93.0 LIVE | 0.00%%-Fee Dual-Venue Delta-Neutral Arb (Binance USDC-M GTX <-> Lighter 0%%) | Stop=$%.2f\n", StopEqUSD)
	go syncAccountLoop()
	select {}
}
