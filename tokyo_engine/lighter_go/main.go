package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
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
	EdgeBps      = 6.0  // Symmetric 6.0 bps entry around basis-adjusted fairLt
	CancelBps    = 2.2  // 2.2 bps modify threshold (preserves queue-front priority, avoids churn)
	ObiLimit     = 0.35 // Block sustained toxic-flow side when 8s-smoothed |OBI| > 0.35
	StopEqUSD    = 93.50
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
	"ETH":   {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.0},
	"BTC":   {Sym: "BTC", ID: 1, SzDec: 5, PxDec: 1, MinB: 0.00007, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.0},
	"SOL":   {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 6.0},
	"XRP":   {Sym: "XRP", ID: 7, SzDec: 0, PxDec: 6, MinB: 4.0, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.4},
	"HYPE":  {Sym: "HYPE", ID: 24, SzDec: 2, PxDec: 4, MinB: 0.07, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 4.5},
	"BNB":   {Sym: "BNB", ID: 25, SzDec: 2, PxDec: 4, MinB: 0.01, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.4},
	"ZEC":   {Sym: "ZEC", ID: 90, SzDec: 3, PxDec: 3, MinB: 0.004, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 6.0},
	"XAU":   {Sym: "XAU", ID: 92, SzDec: 4, PxDec: 2, MinB: 0.0015, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 5.0},
	"NVDA":  {Sym: "NVDA", ID: 110, SzDec: 3, PxDec: 3, MinB: 0.025, AllowEntry: false, ClipUSD: -1.0, EdgeBps: 5.0},
	"SPY":   {Sym: "SPY", ID: 128, SzDec: 4, PxDec: 2, MinB: 0.0070, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 4.0},
	"QQQ":   {Sym: "QQQ", ID: 129, SzDec: 4, PxDec: 2, MinB: 0.0075, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 4.0},
	"MU":    {Sym: "MU", ID: 164, SzDec: 4, PxDec: 2, MinB: 0.0060, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 4.5},
	"US500": {Sym: "US500", ID: 180, SzDec: 5, PxDec: 1, MinB: 0.00070, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 4.0},
	"US100": {Sym: "US100", ID: 181, SzDec: 6, PxDec: 0, MinB: 0.00020, AllowEntry: true, ClipUSD: 240.0, EdgeBps: 4.0},
	"SOXL":  {Sym: "SOXL", ID: 197, SzDec: 4, PxDec: 2, MinB: 0.0500, AllowEntry: true, ClipUSD: 95.0, EdgeBps: 8.5},
}
var evergreenSyms = []string{"SOXL", "MU", "ZEC", "HYPE", "SOL", "XAU"}
var bootTS = time.Now()
var idToSym = map[int64]string{0: "ETH", 1: "BTC", 2: "SOL", 7: "XRP", 24: "HYPE", 25: "BNB", 90: "ZEC", 92: "XAU", 110: "NVDA", 128: "SPY", 129: "QQQ", 164: "MU", 180: "US500", 181: "US100", 197: "SOXL"}

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
	Name            string
	ThinSym         string
	DeepSym         string
	AlwaysActive    bool
	BidEdgeBps      float64
	AskEdgeBps      float64
	FallbackRatio   float64
	EmaRatio        float64
	LastEmaTS       time.Time
	WarmTicks       int
	EntryTS         time.Time
	WasHedged       bool
	LastDualSendTS  time.Time
	LastHedgeTS     time.Time
	CoolUntil       time.Time
	LastLog         time.Time
}

var (
	mktStates = map[string]*MarketState{
		"BTC": newMarketState(-1.0), "ETH": newMarketState(-1.0),
		"SOL": newMarketState(95.0), "XRP": newMarketState(-1.0), "HYPE": newMarketState(95.0),
		"BNB": newMarketState(-1.0), "ZEC": newMarketState(95.0), "XAU": newMarketState(95.0), "NVDA": newMarketState(-1.0),
		"SPY": newMarketState(220.0), "US500": newMarketState(220.0), "QQQ": newMarketState(220.0), "US100": newMarketState(220.0),
		"MU": newMarketState(95.0), "SOXL": newMarketState(95.0),
	}
	twins = []*TwinState{
		{Name: "US500/SPY", ThinSym: "US500", DeepSym: "SPY", AlwaysActive: false, BidEdgeBps: 3.6, AskEdgeBps: 4.4, FallbackRatio: 10.002},
		{Name: "US100/QQQ", ThinSym: "US100", DeepSym: "QQQ", AlwaysActive: false, BidEdgeBps: 4.2, AskEdgeBps: 4.2, FallbackRatio: 41.032},
	}
	pairMu           sync.Mutex
	pairMarketActive atomic.Bool
	probationForce       atomic.Bool
	remotePaused         atomic.Bool
	lastStopMS           atomic.Int64
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

	wsMu      sync.Mutex
	ltConn    *websocket.Conn
	txWsMu    sync.Mutex
	txConn    *websocket.Conn
	txQueue   = make(chan func(), 256)
	useWsSend = true
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
	for fn := range txQueue {
		fn()
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
			time.Sleep(5 * time.Second)
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
	enqueueTx(func() {
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
	enqueueTx(func() {
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
	return sym == "SOXL" || sym == "MU" || sym == "ZEC" || sym == "HYPE" || sym == "SOL" || sym == "XAU"
}

func dislocParams(sym string, now time.Time) (bool, float64, float64, float64, float64, float64, float64) {
	switch sym {
	case "SOXL":
		return isOffHoursUTC(now), 8.5, 5.5, 14.0, 28.0, 2.2, 5.5
	case "MU":
		return isOffHoursUTC(now), 4.5, 3.2, 10.0, 18.0, 1.5, 3.5
	case "ZEC":
		return true, 6.0, 4.2, 12.0, 22.0, 1.8, 3.8
	case "HYPE":
		return true, 4.5, 3.2, 10.0, 18.0, 1.5, 3.5
	case "SOL":
		return true, 6.0, 4.2, 12.0, 22.0, 1.8, 2.5
	case "XAU":
		return true, 5.0, 3.4, 10.5, 16.0, 1.5, 2.5
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
	if peakFavBps >= 5.5 {
		return math.Max(lockFloor, peakFavBps*0.55)
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
	if st.shDir == 1 {
		pnlBps := (lb - st.shEntry) / st.shEntry * 10000.0
		if la >= st.shEntry*(1.0+dTP/10000.0) {
			st.shadowBps += dTP
			st.shDir = 0
		} else if (age >= 15.0 && pnlBps <= -dSL) || pnlBps <= -24.0 || age >= 480.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	} else {
		pnlBps := (st.shEntry - la) / st.shEntry * 10000.0
		if lb <= st.shEntry*(1.0-dTP/10000.0) {
			st.shadowBps += dTP
			st.shDir = 0
		} else if (age >= 15.0 && pnlBps <= -dSL) || pnlBps <= -24.0 || age >= 480.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	}
}

func manageCoin(sym string) {
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
				st.basisBps += (1.0 - math.Exp(-bdt/300.0)) * (instBasis - st.basisBps) // Slow 5m basis anchor!
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
	if st.dynClipUSD <= 95.0 {
		effMaxRng = dMaxRng * 1.45 // Probes test both low-vol & high-vol; winners stay in calm bands!
	}
	calmOk := isDisloc && dActive && coinActive && !inCool && spBps <= dMaxSp && rng5mBps <= effMaxRng && st.range60sBps <= effMaxRng*0.85 && math.Abs(ret15mBps) <= effMaxRng*2.0 && math.Abs(vel3sBps) < 4.2 && math.Abs(obi) <= 0.70
	bidSafe := false
	askSafe := false
	if isDisloc {
		edge = dEdge
		bidAnchor = fairLt
		askAnchor = fairLt
		bidSafe = calmOk && (dirCap-otherPendBid) >= 12.0
		askSafe = calmOk && (dirCap-otherPendAsk) >= 12.0
	}
	keepBid := isDisloc && dActive && !holdOneLocked && !remotePaused.Load() && !inCool && rng5mBps <= effMaxRng*1.20 && math.Abs(vel3sBps) < 5.5
	keepAsk := keepBid

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

	slInit, lockPeak, lockFloor := dSL, dTP*0.85, dTP*0.45

	// 2. Closing orders: v82 Multi-Book Dislocation Convergence (+dTP), 5m Stalled Cut, 8m Scratch, -dSL Stop
	if pos <= -m.MinB && ae > 0 {
		curTkShort := (ae - la) / ae * 10000.0
		if curTkShort > st.peakFavBps {
			st.peakFavBps = curTkShort
		}
		trailFloor := stepRatchetFloor(st.peakFavBps, lockFloor)
		greenShortPx := roundTo(math.Min(ae-tick, ae*(1.0-math.Max(0.25, trailFloor-0.15)/10000.0)), m.PxDec)
		greenLockShort := st.peakFavBps >= lockPeak && ((curTkShort <= trailFloor && spBps <= 6.0) || curTkShort <= trailFloor-0.8)
		earlyCutShort := st.peakFavBps < dTP*0.45 && holdS >= 300.0 && curTkShort <= -(dSL*0.60) && spBps <= 5.5
		hardSlShort := (holdS >= 15.0 && ((curTkShort <= -slInit && spBps <= 6.0) || curTkShort <= -(slInit+1.5))) || curTkShort <= -24.0
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
			if st.peakFavBps >= lockPeak && curTkShort < 0.20 {
				wantBid = greenShortPx // Only tighten to green scratch if curTk pulled back after real peak!
			}
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
		greenLongPx := roundTo(math.Max(ae+tick, ae*(1.0+math.Max(0.25, trailFloorL-0.15)/10000.0)), m.PxDec)
		greenLockLong := st.peakFavBps >= lockPeak && ((curTkLong <= trailFloorL && spBps <= 6.0) || curTkLong <= trailFloorL-0.8)
		earlyCutLong := st.peakFavBps < dTP*0.45 && holdS >= 300.0 && curTkLong <= -(dSL*0.60) && spBps <= 5.5
		hardSlLong := (holdS >= 15.0 && ((curTkLong <= -slInit && spBps <= 6.0) || curTkLong <= -(slInit+1.5))) || curTkLong <= -24.0
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
			if st.peakFavBps >= lockPeak && curTkLong < 0.20 {
				wantAsk = greenLongPx // Only tighten to green scratch if curTk pulled back after real peak!
			}
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
		fmt.Printf("[%s] GO-TOKYO v83 LIVE | Auto-Rebalanced 6-Book Maker ($22->$95->$220->$360) + 15s Wick Shield | Eq=$%.4f | %s Pos=%.5f | Clip=$%.0f | Net=$%.2f | Rng60s=%.1f | Sp=%.2f | Strk=%d | OBI=%.2f\n",
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
	pnl := map[string]float64{"BTC": 0, "XAU": 0, "ETH": 0, "SOL": 0, "XRP": 0, "HYPE": 0, "BNB": 0, "ZEC": 0, "NVDA": 0, "MU": 0, "SOXL": 0, "SPY": 0, "US500": 0, "QQQ": 0, "US100": 0}
	wins := map[string]float64{"BTC": 0, "XAU": 0, "ETH": 0, "SOL": 0, "XRP": 0, "HYPE": 0, "BNB": 0, "ZEC": 0, "NVDA": 0, "MU": 0, "SOXL": 0, "SPY": 0, "US500": 0, "QQQ": 0, "US100": 0}
	closes := map[string]float64{"BTC": 0, "XAU": 0, "ETH": 0, "SOL": 0, "XRP": 0, "HYPE": 0, "BNB": 0, "ZEC": 0, "NVDA": 0, "MU": 0, "SOXL": 0, "SPY": 0, "US500": 0, "QQQ": 0, "US100": 0}
	latestDecided := false
	inCaution := probationForce.Load()
	streak := 0
	streakBroken := false
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
		if _, ok := pnl[s]; !ok {
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
		if err != nil || math.Abs(realized) < 0.002 {
			continue
		}
		if !streakBroken {
			if realized <= -0.045 {
				streakBroken = true
			} else if realized >= 0.045 && tsMS > lastStopMS.Load() {
				streak++
			}
		}
		if !latestDecided && math.Abs(realized) >= 0.045 {
			if realized <= -0.045 && float64(nowMS-tsMS)/60000.0 <= 12.0 {
				inCaution = true
				probationForce.Store(true)
				streak = 0
				streakBroken = true
				latestDecided = true
			} else if realized >= 0.045 && tsMS > lastStopMS.Load() {
				inCaution = false
				probationForce.Store(false)
				latestDecided = true
			} else if realized <= -0.045 {
				latestDecided = true
			}
		}
		ageMin := math.Max(0.0, float64(nowMS-tsMS)/60000.0)
		w := math.Exp(-ageMin / 25.0)
		pnl[s] += realized * w
		closes[s] += w
		if realized > 0 {
			wins[s] += w
		}
	}
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
		if sc >= 2.5 && pnl[s] >= 0.04 && sc > topScore {
			topScore = sc
			topSym = s
		}
	}
	_ = inCaution
	now := time.Now()
	for _, sym := range []string{"BTC", "XAU", "ETH", "SOL", "XRP", "HYPE", "BNB", "ZEC", "NVDA", "MU", "SOXL"} {
		st := mktStates[sym]
		dAct, dEdg, _, _, _, _, _ := dislocParams(sym, now)
		clip := -1.0
		if isDislocSym(sym) && dAct {
			if pnl[sym] <= -0.04 || evScores[sym] <= -3.5 {
				clip = 22.0 // Loser / Choppy probation ($22 probe, ~$0.025 risk)
			} else if sym == topSym {
				clip = 300.0 // #1 Top Winner scaled budget
				if curEq >= 96.5 {
					clip = 360.0
				}
			} else if pnl[sym] >= 0.05 || (evScores[sym] >= 3.0 && pnl[sym] >= 0) {
				clip = 220.0 // Proven Winner
				if curEq >= 95.5 {
					clip = 260.0
				}
			} else {
				clip = 95.0 // Neutral Explorer (tests low & high vol safely)
			}
		}
		st.mu.Lock()
		st.dynClipUSD = clip
		st.makerActive = dAct && clip > 0
		st.dynEdgeBps = dEdg
		st.entryCool = 2000 * time.Millisecond
		st.rollPnl = pnl[sym]
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
								if ao := st.activeOrds[s]; ao != nil && !ao.IsClose {
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
				if eq >= 100.08 {
					fmt.Printf("[GOAL-REACHED] Equity $%.4f >= $100.08, locking $100+ and halting!\n", eq)
					_ = os.WriteFile("/tmp/lighter_pause.flag", []byte("1"), 0644)
					flattenAndExitPause("GOAL >= $100.08 REACHED")
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
		wsMu.Unlock()
		for _, st := range mktStates {
			st.mu.Lock()
			st.bookBids = map[string]float64{}
			st.bookAsks = map[string]float64{}
			st.ltBid = 0
			st.ltAsk = 0
			st.mu.Unlock()
		}

		for _, m := range markets {
			if m.AllowEntry {
				_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
			}
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
			if !strings.Contains(sMsg, "order_book") && !strings.Contains(sMsg, "account_all") {
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
						if sym == "SPY" || sym == "US500" || sym == "QQQ" || sym == "US100" {
							manageAllTwins()
						} else {
							manageCoin(sym)
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
					if needRecompute {
						recomputeBBO(st)
					}
					if st.ltBid > 0 && st.ltAsk > 0 {
						st.lastBookTS = time.Now()
					}
					bboChanged := st.ltBid != prevB || st.ltAsk != prevA
					shouldRun := bboChanged && st.bnMid > 0 && (st.dynClipUSD >= 0 || math.Abs(st.pos) > 1e-6 || time.Since(st.lastShadowTS) >= 100*time.Millisecond)
					st.mu.Unlock()
					if sym == "SPY" || sym == "US500" || sym == "QQQ" || sym == "US100" {
						if bboChanged {
							manageAllTwins()
						}
					} else if shouldRun {
						manageCoin(sym)
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

func runBnWS() {
	url := "wss://fstream.binance.com/stream?streams=soxlusdt@bookTicker/soxlusdt@kline_1m/muusdt@bookTicker/muusdt@kline_1m/zecusdt@bookTicker/zecusdt@kline_1m/hypeusdt@bookTicker/hypeusdt@kline_1m/solusdt@bookTicker/solusdt@kline_1m/xauusdt@bookTicker/xauusdt@kline_1m"
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
	for _, sym := range []string{"SOXL", "MU", "ZEC", "HYPE", "SOL", "XAU"} {
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

func seedPairHistory() {
	hc := &http.Client{Timeout: 5 * time.Second}
	type trPt struct {
		ts int64
		px float64
	}
	fetchTr := func(mid int64) []trPt {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/trades?market_id=%d&limit=100&sort_by=timestamp", BaseURL, mid), nil)
		if err != nil {
			return nil
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := hc.Do(req)
		if err != nil {
			return nil
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var d struct {
			Trades []struct {
				TS    int64  `json:"timestamp"`
				Price string `json:"price"`
			} `json:"trades"`
		}
		if json.Unmarshal(body, &d) != nil {
			return nil
		}
		var out []trPt
		for _, t := range d.Trades {
			p, _ := strconv.ParseFloat(t.Price, 64)
			if p > 0 && t.TS > 0 {
				out = append(out, trPt{ts: t.TS, px: p})
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].ts < out[j].ts })
		return out
	}
	fetchBookMid := func(mid int64) float64 {
		req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/orderBookOrders?market_id=%d&limit=3", BaseURL, mid), nil)
		if err != nil {
			return 0
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")
		resp, err := hc.Do(req)
		if err != nil {
			return 0
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var ob struct {
			Bids []struct {
				Price string `json:"price"`
			} `json:"bids"`
			Asks []struct {
				Price string `json:"price"`
			} `json:"asks"`
		}
		if json.Unmarshal(body, &ob) != nil || len(ob.Bids) == 0 || len(ob.Asks) == 0 {
			return 0
		}
		bb, _ := strconv.ParseFloat(ob.Bids[0].Price, 64)
		ba, _ := strconv.ParseFloat(ob.Asks[0].Price, 64)
		if bb > 0 && ba > bb {
			return 0.5 * (bb + ba)
		}
		return 0
	}
	for _, tw := range twins {
		tM, dM := markets[tw.ThinSym], markets[tw.DeepSym]
		tPts, dPts := fetchTr(tM.ID), fetchTr(dM.ID)
		var ratios []float64
		if len(tPts) >= 5 && len(dPts) >= 5 {
			di := 0
			for _, tp := range tPts {
				for di+1 < len(dPts) && math.Abs(float64(dPts[di+1].ts-tp.ts)) <= math.Abs(float64(dPts[di].ts-tp.ts)) {
					di++
				}
				if math.Abs(float64(dPts[di].ts-tp.ts)) <= 12000 && dPts[di].px > 0 {
					ratios = append(ratios, tp.px/dPts[di].px)
				}
			}
		}
		med := tw.FallbackRatio
		warm := 6
		src := "fallback"
		if len(ratios) >= 5 {
			sort.Float64s(ratios)
			med = ratios[len(ratios)/2]
			warm = 12
			src = fmt.Sprintf("%d prints", len(ratios))
		} else {
			tm, dm := fetchBookMid(tM.ID), fetchBookMid(dM.ID)
			if tm > 0 && dm > 0 {
				med = tm / dm
				warm = 8
				src = "live_bbo"
			}
		}
		pairMu.Lock()
		tw.EmaRatio = med
		tw.LastEmaTS = time.Now()
		tw.WarmTicks = warm
		pairMu.Unlock()
		fmt.Printf("[TWIN_SEED] %s ratio=%.6f (%s)\n", tw.Name, med, src)
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
		dbgT := fmt.Sprintf("TWIN_%s_%s px=%.2f sz=%.6f ro=%d", tw.ThinSym, thinSide, thinPx, thinSz, redOnly)
		_ = sendSignedTx(fmt.Sprintf("dt_%d", coiT), dbgT, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      tM.ID,
				ClientOrderIndex: coiT,
				BaseAmount:       tBaseInt,
				Price:            tPxInt,
				IsAsk:            tAsk,
				Type:             1,
				TimeInForce:      0,
				ReduceOnly:       redOnly,
				OrderExpiry:      0,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
		dbgD := fmt.Sprintf("TWIN_%s_%s px=%.2f sz=%.6f ro=%d", tw.DeepSym, deepSide, deepPx, deepSz, redOnly)
		_ = sendSignedTx(fmt.Sprintf("dd_%d", coiD), dbgD, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      dM.ID,
				ClientOrderIndex: coiD,
				BaseAmount:       dBaseInt,
				Price:            dPxInt,
				IsAsk:            dAsk,
				Type:             1,
				TimeInForce:      0,
				ReduceOnly:       redOnly,
				OrderExpiry:      0,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
	})
}

func manageAllTwins() {
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
	tb, tbSz, ta, taSz, tPos, tAE, tBookTS := tSt.ltBid, tSt.ltBidSz, tSt.ltAsk, tSt.ltAskSz, tSt.pos, tSt.avgEntry, tSt.lastBookTS
	tBidOrd, tAskOrd := tSt.activeOrds["BID"], tSt.activeOrds["ASK"]
	tLastBid, tLastAsk := tSt.lastOrdTS["BID"], tSt.lastOrdTS["ASK"]
	tSt.mu.Unlock()

	dSt.mu.Lock()
	for _, sd := range []string{"BID", "ASK"} {
		if ao := dSt.activeOrds[sd]; ao != nil && ao.IsIOC && now.Sub(ao.TS).Seconds() >= 2.0 {
			delete(dSt.activeOrds, sd)
		}
	}
	db, dbSz, da, daSz, dPos, dAE, dBookTS := dSt.ltBid, dSt.ltBidSz, dSt.ltAsk, dSt.ltAskSz, dSt.pos, dSt.avgEntry, dSt.lastBookTS
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

	if booksFresh && dSpBps <= 2.2 && tSpBps <= 5.0 {
		instR := tMid / dMid
		if tw.EmaRatio <= 0 {
			tw.EmaRatio = instR
			tw.LastEmaTS = now
		} else if math.Abs(instR-tw.EmaRatio)/tw.EmaRatio*10000.0 < 35.0 {
			dt := math.Min(5.0, math.Max(0.001, now.Sub(tw.LastEmaTS).Seconds()))
			tw.LastEmaTS = now
			tau := 600.0
			if math.Abs(tPos) >= tM.MinB || math.Abs(dPos) >= dM.MinB {
				tau = 1800.0
			}
			tw.EmaRatio += (1.0 - math.Exp(-dt/tau)) * (instR - tw.EmaRatio)
			tw.WarmTicks++
		}
	}

	tUSD, dUSD := tPos*tMid, dPos*dMid
	netDelta := tUSD + dUSD
	if math.Abs(tPos) < tM.MinB && math.Abs(dPos) < dM.MinB {
		tw.WasHedged = false
		tw.EntryTS = time.Time{}
	} else if math.Abs(tUSD) >= 11.0 && math.Abs(dUSD) >= 11.0 && tUSD*dUSD < 0 {
		tw.WasHedged = true
		if tw.EntryTS.IsZero() {
			tw.EntryTS = now
		}
	}

	edgeLongThin, edgeShortThin := 0.0, 0.0
	if tw.EmaRatio > 0 {
		edgeLongThin = (db*tw.EmaRatio - ta) / ta * 10000.0
		edgeShortThin = (tb - da*tw.EmaRatio) / tb * 10000.0
	}
	pairAllowed := tw.AlwaysActive || !isOffHoursUTC(now)

	if pairAllowed && now.Sub(tw.LastLog) >= 30*time.Second {
		tw.LastLog = now
		fmt.Printf("[%s] GO-TOKYO v80 %s Eq=$%.4f | Thin=%.5f($%.1f) Deep=%.4f($%.1f) | Sp=%.2f/%.2f | Edge=%.2f/%.2f | DeepBBO=$%0.f/%0.f\n",
			now.Format("15:04:05"), tw.Name, float64(eqNow.Load())/1e6, tPos, tUSD, dPos, dUSD, tSpBps, dSpBps, edgeLongThin, edgeShortThin, db*dbSz, da*daSz)
	}

	if now.Sub(tw.LastDualSendTS) < 1100*time.Millisecond {
		return pairAllowed
	}

	// STEP A: Immediate 0.8ms Delta Hedge on Deep Leg OR Finish 1-Leg Exit
	if math.Abs(netDelta) >= 11.5 && dSpBps <= 4.5 && tSpBps <= 6.5 {
		if tw.WasHedged {
			if math.Abs(tPos) < tM.MinB && math.Abs(dPos) >= dM.MinB && now.Sub(tw.LastHedgeTS) >= 900*time.Millisecond && takeWriteSlotTier(2) {
				tw.LastHedgeTS = now
				tw.CoolUntil = now.Add(45 * time.Second)
				if dPos > 0 {
					placePairOrder(tw.DeepSym, "ASK", roundTo(db*0.9994, dM.PxDec), dMid, math.Abs(dPos), true, true)
				} else {
					placePairOrder(tw.DeepSym, "BID", roundTo(da*1.0006, dM.PxDec), dMid, math.Abs(dPos), true, true)
				}
			} else if math.Abs(dPos) < dM.MinB && math.Abs(tPos) >= tM.MinB && now.Sub(tw.LastHedgeTS) >= 900*time.Millisecond && takeWriteSlotTier(2) {
				tw.LastHedgeTS = now
				tw.CoolUntil = now.Add(45 * time.Second)
				if tPos > 0 {
					placePairOrder(tw.ThinSym, "ASK", roundTo(tb*0.9994, tM.PxDec), tMid, math.Abs(tPos), true, true)
				} else {
					placePairOrder(tw.ThinSym, "BID", roundTo(ta*1.0006, tM.PxDec), tMid, math.Abs(tPos), true, true)
				}
			}
			return true
		}
		if math.Abs(tUSD) >= 11.0 && math.Abs(dPos) < dM.MinB {
			cancelThinEntries()
			if now.Sub(tw.LastHedgeTS) >= 900*time.Millisecond && takeWriteSlotTier(2) {
				tw.LastHedgeTS = now
				hedgeSz := roundTo(math.Abs(tUSD)/dMid, dM.SzDec)
				if tPos > 0 {
					placePairOrder(tw.DeepSym, "ASK", roundTo(db*0.9994, dM.PxDec), dMid, hedgeSz, false, true)
				} else {
					placePairOrder(tw.DeepSym, "BID", roundTo(da*1.0006, dM.PxDec), dMid, hedgeSz, false, true)
				}
			}
		} else if math.Abs(dUSD) >= 11.0 && math.Abs(tPos) < tM.MinB {
			if now.Sub(tw.LastHedgeTS) >= 900*time.Millisecond && takeWriteSlotTier(2) {
				tw.LastHedgeTS = now
				if dPos > 0 {
					placePairOrder(tw.DeepSym, "ASK", roundTo(db*0.9994, dM.PxDec), dMid, math.Abs(dPos), true, true)
				} else {
					placePairOrder(tw.DeepSym, "BID", roundTo(da*1.0006, dM.PxDec), dMid, math.Abs(dPos), true, true)
				}
			}
		}
		return true
	}

	// STEP B: Hedged Twin Convergence Exit (Locked-Green Dual IOC OR Maker-First Unwind)
	if math.Abs(tUSD) >= 11.0 && math.Abs(dUSD) >= 11.0 && tUSD*dUSD < 0 {
		cancelThinEntries()
		if tAE <= 0 || dAE <= 0 {
			return true
		}
		ageS := now.Sub(tw.EntryTS).Seconds()
		var execNetBps, availExitUSD float64
		if tPos > 0 {
			execNetBps = ((tb-tAE)/tAE + (dAE-da)/dAE) * 10000.0
			availExitUSD = 0.80 * math.Min(tb*tbSz, da*daSz)
		} else {
			execNetBps = ((tAE-ta)/tAE + (db-dAE)/dAE) * 10000.0
			availExitUSD = 0.80 * math.Min(ta*taSz, db*dbSz)
		}
		allowCarry := tPos > 0 && tw.ThinSym == "US500" && execNetBps > -6.0
		tightStopBook := tSpBps <= 3.0 && dSpBps <= 2.0
		if ((execNetBps <= -18.0 && tightStopBook) || (ageS >= 1800.0 && !allowCarry && tightStopBook)) && takeWriteSlotTier(2) {
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
					fireTwinIOC(tw, "ASK", roundTo(tb*0.9994, tM.PxDec), tMid, tCl, "BID", roundTo(da*1.0006, dM.PxDec), dMid, dCl, true)
				} else {
					fireTwinIOC(tw, "BID", roundTo(ta*1.0006, tM.PxDec), tMid, tCl, "ASK", roundTo(db*0.9994, dM.PxDec), dMid, dCl, true)
				}
			} else if tPos > 0 {
				placePairOrder(tw.ThinSym, "ASK", roundTo(tb*0.9994, tM.PxDec), tMid, math.Abs(tPos), true, true)
			} else {
				placePairOrder(tw.ThinSym, "BID", roundTo(ta*1.0006, tM.PxDec), tMid, math.Abs(tPos), true, true)
			}
			return true
		}
		if execNetBps >= 1.50 && availExitUSD >= math.Abs(tUSD)*0.95 && takeWriteSlot(true) {
			if tBidOrd != nil && tBidOrd.IsClose && !tBidOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "BID")
			}
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				cancelPairSide(tw.ThinSym, "ASK")
			}
			tw.LastDualSendTS = now
			tw.CoolUntil = now.Add(45 * time.Second)
			if tPos > 0 {
				fireTwinIOC(tw, "ASK", tb, tMid, math.Abs(tPos), "BID", da, dMid, math.Abs(dPos), true)
			} else {
				fireTwinIOC(tw, "BID", ta, tMid, math.Abs(tPos), "ASK", db, dMid, math.Abs(dPos), true)
			}
			return true
		}
		// Zero-Slippage Maker-First Unwind on ThinSym
		if tPos > 0 {
			wantAsk := math.Max(ta, roundTo(da*tw.EmaRatio*(1.0+0.6/10000.0), tM.PxDec))
			if tAskOrd != nil && tAskOrd.IsClose && !tAskOrd.IsIOC {
				if math.Abs(tAskOrd.Px-wantAsk)/wantAsk*10000.0 >= 2.0 && now.Sub(tLastAsk) >= 4000*time.Millisecond && takeWriteSlot(true) {
					modifyPairSide(tw.ThinSym, "ASK", wantAsk, tMid)
				}
			} else if tAskOrd == nil && now.Sub(tLastAsk) >= 400*time.Millisecond && takeWriteSlot(true) {
				placePairOrder(tw.ThinSym, "ASK", wantAsk, tMid, math.Abs(tPos), true, false)
			}
		} else {
			wantBid := math.Min(tb, roundTo(db*tw.EmaRatio*(1.0-0.6/10000.0), tM.PxDec))
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

	// STEP C: Zero-1-Legging Maker-First Entry on ThinSym
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
	deepBookSafe := booksFresh && now.After(tw.CoolUntil) && dSpBps <= 1.85 && tSpBps <= 6.0 && db*dbSz >= 800.0 && da*daSz >= 800.0 && tw.EmaRatio > 0 && tw.WarmTicks >= 5
	if !pairAllowed || otherBusy || remotePaused.Load() || !deepBookSafe {
		cancelThinEntries()
		return pairAllowed
	}

	tickThin := math.Pow10(-tM.PxDec)
	wantBid := roundTo(da*tw.EmaRatio*(1.0-tw.BidEdgeBps/10000.0), tM.PxDec)
	if wantBid >= ta {
		wantBid = roundTo(ta-tickThin, tM.PxDec)
	}
	wantAsk := roundTo(db*tw.EmaRatio*(1.0+tw.AskEdgeBps/10000.0), tM.PxDec)
	if wantAsk <= tb {
		wantAsk = roundTo(tb+tickThin, tM.PxDec)
	}
	preferBid := (tb - wantBid) <= (wantAsk - ta)

	if tBidOrd != nil && !tBidOrd.IsClose && !tBidOrd.IsIOC {
		liveEdge := (db*tw.EmaRatio - tBidOrd.Px) / tBidOrd.Px * 10000.0
		if liveEdge < 2.0 || tBidOrd.Px >= ta || (!preferBid && (wantAsk-ta)*1.5 < (tb-wantBid)) {
			cancelPairSide(tw.ThinSym, "BID")
		} else if math.Abs(tBidOrd.Px-wantBid)/wantBid*10000.0 >= 2.5 && now.Sub(tLastBid) >= 4500*time.Millisecond && takeWriteSlot(false) {
			modifyPairSide(tw.ThinSym, "BID", wantBid, tMid)
		}
		return true
	}
	if tAskOrd != nil && !tAskOrd.IsClose && !tAskOrd.IsIOC {
		liveEdge := (tAskOrd.Px - da*tw.EmaRatio) / tAskOrd.Px * 10000.0
		if liveEdge < 2.0 || tAskOrd.Px <= tb || (preferBid && (tb-wantBid)*1.5 < (wantAsk-ta)) {
			cancelPairSide(tw.ThinSym, "ASK")
		} else if math.Abs(tAskOrd.Px-wantAsk)/wantAsk*10000.0 >= 2.5 && now.Sub(tLastAsk) >= 4500*time.Millisecond && takeWriteSlot(false) {
			modifyPairSide(tw.ThinSym, "ASK", wantAsk, tMid)
		}
		return true
	}

	pairPnl := float64(tSt.rollPnlMicro.Load()+dSt.rollPnlMicro.Load()) / 1e6
	legUSD := 220.0
	if pairPnl <= -0.08 {
		legUSD = 60.0
	} else if pairPnl >= 0.10 {
		legUSD = 340.0
	}
	if preferBid {
		bidEdge := (db*tw.EmaRatio - wantBid) / wantBid * 10000.0
		if bidEdge >= 2.2 && now.Sub(tLastBid) >= 3000*time.Millisecond && takeWriteSlot(false) {
			sz := roundTo(legUSD/wantBid, tM.SzDec)
			placePairOrder(tw.ThinSym, "BID", wantBid, tMid, sz, false, false)
		}
	} else {
		askEdge := (wantAsk - da*tw.EmaRatio) / wantAsk * 10000.0
		if askEdge >= 2.2 && now.Sub(tLastAsk) >= 3000*time.Millisecond && takeWriteSlot(false) {
			sz := roundTo(legUSD/wantAsk, tM.SzDec)
			placePairOrder(tw.ThinSym, "ASK", wantAsk, tMid, sz, false, false)
		}
	}
	return true
}

func runPairHeartbeat() {
	t := time.NewTicker(350 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		manageAllTwins()
	}
}

func flattenAndExitPause(reason string) {
	if !remotePaused.CompareAndSwap(false, true) {
		return
	}
	fmt.Printf("[INSTANT-PAUSE] %s — canceling orders & flattening in <1s!\n", reason)
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
		if s3Ticks%5 == 0 {
			if resp, err := hc.Get(fmt.Sprintf("https://crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com/live-telemetry.json?t=%d", time.Now().UnixMilli())); err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				if strings.Contains(string(b), `"userPaused":true`) {
					flattenAndExitPause("S3 userPaused=true")
					return
				}
			}
		}
		time.Sleep(400 * time.Millisecond)
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
	mktStates["BTC"].dynClipUSD = -1.0
	mktStates["XAU"].dynClipUSD = -1.0
	mktStates["ETH"].dynClipUSD = -1.0
	mktStates["SOL"].dynClipUSD = -1.0
	mktStates["XRP"].dynClipUSD = -1.0
	mktStates["HYPE"].dynClipUSD = -1.0
	mktStates["BNB"].dynClipUSD = -1.0
	mktStates["ZEC"].dynClipUSD = -1.0
	mktStates["NVDA"].dynClipUSD = -1.0
	nowBoot := time.Now()
	for _, s := range []string{"BTC", "XAU", "ETH", "SOL", "XRP", "HYPE", "BNB", "ZEC", "NVDA", "MU", "SOXL"} {
		dAct, _, _, _, _, _, _ := dislocParams(s, nowBoot)
		mktStates[s].dynClipUSD = -1.0
		mktStates[s].makerActive = false
		if isDislocSym(s) && dAct {
			mktStates[s].dynClipUSD = 95.0
			mktStates[s].makerActive = true
		}
	}

	go runTxWorker()
	go runTxWS()
	go runLtWS()
	go runPauseWatchdog()
	time.Sleep(1500 * time.Millisecond)
	cancelAll()
	seedBinanceHistory()
	seedPairHistory()
	go runPairHeartbeat()
	fmt.Printf("GO-TOKYO v83 LIVE | Auto-Rebalanced 6-Book Maker ($22->$95->$220->$360) + 15s Wick Shield | Stop=$%.2f\n", StopEqUSD)
	go syncAccountLoop()
	runBnWS()
}
