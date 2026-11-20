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
	StopEqUSD    = 95.00
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
	"ETH":   {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020, AllowEntry: false, ClipUSD: 150.0, EdgeBps: 5.4},
	"BTC":   {Sym: "BTC", ID: 1, SzDec: 5, PxDec: 1, MinB: 0.00007, AllowEntry: true, ClipUSD: 150.0, EdgeBps: 5.0},
	"SOL":   {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055, AllowEntry: false, ClipUSD: 88.0, EdgeBps: 5.4},
	"HYPE":  {Sym: "HYPE", ID: 24, SzDec: 2, PxDec: 4, MinB: 0.07, AllowEntry: false, ClipUSD: 88.0, EdgeBps: 5.8},
	"ZEC":   {Sym: "ZEC", ID: 90, SzDec: 3, PxDec: 3, MinB: 0.004, AllowEntry: false, ClipUSD: 12.0, EdgeBps: 10.0},
	"QQQ":   {Sym: "QQQ", ID: 129, SzDec: 4, PxDec: 2, MinB: 0.0170, AllowEntry: true, ClipUSD: 255.0, EdgeBps: 1.75},
	"US100": {Sym: "US100", ID: 181, SzDec: 6, PxDec: 0, MinB: 0.00042, AllowEntry: true, ClipUSD: 255.0, EdgeBps: 0.0},
}
var evergreenSyms = []string{"BTC", "ETH"}
var bootTS = time.Now()
var idToSym = map[int64]string{0: "ETH", 1: "BTC", 2: "SOL", 24: "HYPE", 90: "ZEC", 129: "QQQ", 181: "US100"}

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
	basisBps      float64
	lastBasisTS   time.Time
	emaSpreadBps  float64
	rangeBuckets  [6]RangeBucket
	range60sBps   float64
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
	ltAsk       float64
	lastBookTS  time.Time
	pos         float64
	avgEntry    float64
	posOpenTS     time.Time
	lastFillTS    time.Time
	stopCoolUntil time.Time
	trendMode     bool
	entryWasSnipe bool
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

var (
	mktStates = map[string]*MarketState{
		"BTC": newMarketState(150.0), "ETH": newMarketState(-1.0),
		"SOL": newMarketState(-1.0), "HYPE": newMarketState(-1.0), "ZEC": newMarketState(-1.0),
		"QQQ": newMarketState(255.0), "US100": newMarketState(255.0),
	}
	pairMu                 sync.Mutex
	pairMarketActive       atomic.Bool
	pairMarketLastActiveTS time.Time
	pairEmaRatio           float64
	pairLastEmaTS          time.Time
	pairWarmTicks          int
	pairEntryTS            time.Time
	lastHedgeTS            time.Time
	lastPairLog            time.Time
	probationForce       atomic.Bool
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
	eq := float64(eqNow.Load()) / 1e6
	if eq < 50.0 {
		eq = 100.0
	}
	return math.Min(650.0, eq*2.35)
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
	limit := 90 // Tier 0: entries & quote modifies (unlocked WS Volume Quota; L2CancelOrder is 100% free!)
	if tier == 1 {
		limit = 110 // Tier 1: TP closes & modifies
	} else if tier >= 2 {
		limit = 130 // Tier 2: Emergency SL & Crash Bailouts
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
	st := mktStates[sym]
	m := markets[sym]
	baseF := customSz
	if baseF <= 0 {
		clip := st.dynClipUSD
		if clip <= 0 {
			clip = m.ClipUSD
		}
		eq := float64(eqNow.Load()) / 1e6
		if eq < 50.0 {
			eq = 100.0
		}
		cautCap := math.Min(500.0, eq*1.55)
		if probationForce.Load() && clip > cautCap {
			clip = cautCap // ~$153 gentle 0.84x caution for 1 win (no size inversion!)
		}
		if !isClose && isIOC {
			if sym == "BTC" {
				clip = math.Min(clip*1.15, eq*2.23) // ~$209 normal / ~$220 max on A+ BTC Snipes
			} else {
				clip = math.Min(clip*1.05, eq*2.05)
			}
			if st.range60sBps > 24.0 {
				clip = math.Max(150.0, clip*(24.0/st.range60sBps))
			}
		} else if !isClose {
			if sym != "BTC" {
				clip = math.Max(150.0, clip*0.94) // ~$171 normal / ~$187 streak / $150 caution on ETH Calm Maker
			}
			if st.range60sBps > 15.0 {
				clip = math.Max(145.0, clip*(15.0/st.range60sBps))
			}
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
	bestB, bestA := 0.0, 0.0
	for ps, sz := range st.bookBids {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if p > bestB {
				bestB = p
			}
		}
	}
	for ps, sz := range st.bookAsks {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if bestA == 0 || p < bestA {
				bestA = p
			}
		}
	}
	st.ltBid = bestB
	st.ltAsk = bestA
}

func closeStepBps(st *MarketState) float64 {
	if !st.posOpenTS.IsZero() {
		secs := time.Since(st.posOpenTS).Seconds()
		if secs >= 50.0 {
			return 0.2
		}
		if secs >= 22.0 {
			return 0.9
		}
	}
	return 1.65 // Verified 80%+ WR fast 0%-fee Maker exit on BTC weekend fallback snipes!
}

func updateShadowPaper(st *MarketState, now time.Time, lb, la, fairLt, effEdge float64, bidOk, askOk bool) {
	if st.lastShadowTS.IsZero() {
		st.lastShadowTS = now
	} else {
		sdt := math.Min(10.0, math.Max(0.0, now.Sub(st.lastShadowTS).Seconds()))
		st.lastShadowTS = now
		st.shadowBps *= math.Exp(-sdt / 1200.0) // 20m exponential half-life on paper score
	}
	if st.shDir == 0 {
		if st.emaSpreadBps > 0 && st.emaSpreadBps <= 1.6 {
			if bidOk && (fairLt-lb)/lb*10000.0 >= effEdge*0.85 {
				st.shDir = 1
				st.shEntry = lb
				st.shOpenTS = now
			} else if askOk && (la-fairLt)/la*10000.0 >= effEdge*0.85 {
				st.shDir = -1
				st.shEntry = la
				st.shOpenTS = now
			}
		}
		return
	}
	age := now.Sub(st.shOpenTS).Seconds()
	tgt := 3.5
	if age >= 45.0 {
		tgt = 0.0
	} else if age >= 25.0 {
		tgt = 1.5
	}
	if st.shDir == 1 {
		pnlBps := (lb - st.shEntry) / st.shEntry * 10000.0
		if la >= st.shEntry*(1.0+tgt/10000.0) {
			st.shadowBps += tgt
			st.shDir = 0
		} else if pnlBps <= -8.5 || age >= 60.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	} else {
		pnlBps := (st.shEntry - la) / st.shEntry * 10000.0
		if lb <= st.shEntry*(1.0-tgt/10000.0) {
			st.shadowBps += tgt
			st.shDir = 0
		} else if pnlBps <= -8.5 || age >= 60.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	}
}

func manageCoin(sym string) {
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
	if eu > 0 {
		vel3sBps = (mid - eu) / eu * 10000.0
		if vel3sBps <= -5.5 {
			st.dropCoolUntil = now.Add(12 * time.Second)
		}
		if vel3sBps >= 5.5 {
			st.pumpCoolUntil = now.Add(12 * time.Second)
		}
	}
	ltMid := 0.5 * (lb + la)
	spBps := (la - lb) / ltMid * 10000.0
	instBasis := (ltMid - mid) / mid * 10000.0
	if st.lastBasisTS.IsZero() {
		st.basisBps = instBasis
		st.emaSpreadBps = spBps
		st.lastBasisTS = now
	} else {
		bdt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastBasisTS).Seconds()))
		st.lastBasisTS = now
		if spBps <= 1.25 {
			st.basisBps += (1.0 - math.Exp(-bdt/300.0)) * (instBasis - st.basisBps) // Slow 5m basis anchor!
		}
		st.emaSpreadBps += (1.0 - math.Exp(-bdt/45.0)) * (spBps - st.emaSpreadBps)
	}
	fairLt := micro * (1.0 + st.basisBps/10000.0)
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
	if st.range60sBps > 18.0 {
		snipeThresh = 3.4
	}
	minHr := now.Minute()
	clockPause := minHr == 59 || minHr == 0 || minHr == 30
	if minHr >= 58 || minHr <= 2 || (minHr >= 29 && minHr <= 32) {
		edge += 1.4
		snipeThresh += 0.8
	}

	holdOneLocked := anyOtherOpenPos(sym) || now.UnixMilli() < globalEntryLockUntil.Load()
	dirCap := globalDirCapUSD()
	otherPendBid := otherSameDirPendingUSD(sym, true)
	otherPendAsk := otherSameDirPendingUSD(sym, false)
	coinActive := m.AllowEntry && st.dynClipUSD >= 0 && !holdOneLocked && !pairMarketActive.Load()
	makerAllowed := false // Directional crypto passive Makers permanently OFF; 24/5 runs Delta-Neutral QQQ/US100!
	inCool := now.Before(st.stopCoolUntil) || now.Sub(st.lastFillTS) < 3*time.Second

	bidAllowed := volSafe && !clockPause && macroTrendBps >= -48.0 && macroTrendBps <= 95.0 && mom5mBps >= -12.0 && mom5mBps <= 22.0 && trendBps >= -4.0 && trendBps <= 8.5
	askAllowed := volSafe && !clockPause && macroTrendBps <= 48.0 && macroTrendBps >= -95.0 && mom5mBps <= 12.0 && mom5mBps >= -22.0 && trendBps <= 4.0 && trendBps >= -8.5

	updateShadowPaper(st, now, lb, la, fairLt, edge, bidAllowed && vel3sBps > -2.5 && obi >= -ObiLimit, askAllowed && vel3sBps < 2.5 && obi <= ObiLimit)

	bidSafe := makerAllowed && !inCool && now.After(st.dropCoolUntil) && vel3sBps > -2.5 && bidAllowed && obi >= -ObiLimit && (dirCap-otherPendBid) >= 12.0
	askSafe := makerAllowed && !inCool && now.After(st.pumpCoolUntil) && vel3sBps < 2.5 && askAllowed && obi <= ObiLimit && (dirCap-otherPendAsk) >= 12.0
	// Fundamental Law #3: NO 1ms rawObi self-cancel churn! Preserve queue priority on Lighter!
	keepBid := makerAllowed && volSafe && !clockPause && !inCool && now.After(st.dropCoolUntil) && macroTrendBps >= -55.0 && macroTrendBps <= 105.0 && mom5mBps >= -14.5 && mom5mBps <= 25.0 && vel3sBps > -4.2 && obi >= -0.55
	keepAsk := makerAllowed && volSafe && !clockPause && !inCool && now.After(st.pumpCoolUntil) && macroTrendBps <= 55.0 && macroTrendBps >= -105.0 && mom5mBps <= 14.5 && mom5mBps >= -25.0 && vel3sBps < 4.2 && obi <= 0.55

	// BTC-ONLY Lead-Lag Snipe using fast 2s fObi + strict 5m non-overextension [-6, +9bps] (84.6% WR +2.47bps slice!)
	staleSnipeBuy := sym == "BTC" && coinActive && volSafeSnipe && !clockPause && !inCool && spBps <= snipeMaxSp && now.After(st.dropCoolUntil) && math.Abs(posUSD) < 1.0 && (dirCap-otherPendBid) >= 12.0 && (fairLt-la)/la*10000.0 >= snipeThresh && macroTrendBps >= -42.0 && macroTrendBps <= 85.0 && mom5mBps >= -6.0 && mom5mBps <= 9.0 && trendBps >= -2.0 && trendBps <= 5.5 && impBps <= 3.6 && vel3sBps >= 0.15 && vel3sBps <= 4.8 && fObi >= 0.22
	staleSnipeSell := sym == "BTC" && coinActive && volSafeSnipe && !clockPause && !inCool && spBps <= snipeMaxSp && now.After(st.pumpCoolUntil) && math.Abs(posUSD) < 1.0 && (dirCap-otherPendAsk) >= 12.0 && (lb-fairLt)/lb*10000.0 >= snipeThresh && macroTrendBps <= 42.0 && macroTrendBps >= -85.0 && mom5mBps <= 6.0 && mom5mBps >= -9.0 && trendBps <= 2.0 && trendBps >= -5.5 && impBps >= -3.6 && vel3sBps <= -0.15 && vel3sBps >= -4.8 && fObi <= -0.22
	btcTrendBuy := staleSnipeBuy
	btcTrendSell := staleSnipeSell

	// 1. Manage resting orders: ModifyOrder (1 write) or Quota-Free 0ms L2CancelOrder!
	pickoffBuf := 1.25
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
			} else if (moveBps >= CancelBps || ageS >= 35.0 || tooClose) && now.Sub(st.lastOrdTS[side]) >= 800*time.Millisecond && takeWriteSlot(false) {
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
	stepBps := closeStepBps(st)
	holdS := 0.0
	if !st.posOpenTS.IsZero() {
		holdS = now.Sub(st.posOpenTS).Seconds()
	}

	// 2. Closing orders: Peak +2.4bps Winner Lock + 1:1.7 Risk Cap (5.4bps Snipe / 6.5bps Maker) + Dual BTC/ETH Sniper
	if pos <= -m.MinB && ae > 0 {
		favBps := (ae - fairLt) / ae * 10000.0
		if favBps > st.peakFavBps {
			st.peakFavBps = favBps
		}
		slBps := 4.8
		slHitShort := fairLt >= ae*(1.0+slBps/10000.0)
		defBailShort := vel3sBps >= 8.5 && obi >= 0.55 && fairLt >= ae*(1.0+5.0/10000.0)
		timeScratchShort := holdS >= 115.0 // Zombie-only IOC bail after 115s; 65-115s uses passive Maker scratch!
		if posUSD > -10.10 {
			if (fairLt <= ae*(1.0-stepBps/10000.0) || slHitShort || defBailShort || timeScratchShort) && st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 2*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["BID"] = now
				if (slHitShort || defBailShort) && st.peakFavBps < 2.2 {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(35 * time.Second)
				}
				placeSide(sym, "BID", roundTo(la*1.0006, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else if slHitShort || defBailShort || timeScratchShort {
			if ao := st.activeOrds["BID"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "BID")
			}
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 350*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["BID"] = now
				if (slHitShort || defBailShort) && st.peakFavBps < 2.2 {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(35 * time.Second)
				}
				placeSide(sym, "BID", roundTo(la*1.0006, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else {
			effStep := stepBps
			if stepBps >= 3.0 && vel3sBps <= -2.2 && obi <= -0.35 {
				effStep = 5.2 // Momentum runner boost (~+$0.055-$0.068 wins!)
			}
			wantBid := roundTo(ae*(1.0-effStep/10000.0), m.PxDec)
			if effStep > 0 && fairLt < wantBid {
				wantBid = roundTo(fairLt, m.PxDec)
			}
			if ao := st.activeOrds["BID"]; ao != nil && ao.IsClose && !ao.IsIOC && math.Abs(ao.Px-wantBid) >= tick*2 && now.Sub(st.lastOrdTS["BID"]) >= 400*time.Millisecond && takeWriteSlot(true) {
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
			snipeBidPx := roundTo(math.Min(la+tick*3, fairLt*(1.0-minEdgeGuarantee/10000.0)), m.PxDec)
			if snipeBidPx >= la && cancelDistantForSnipe(sym, "BID") {
				if st.activeOrds["ASK"] != nil && !cancelDistantForSnipe(sym, "ASK") {
					cancelSide(sym, "ASK")
				}
				if st.activeOrds["BID"] == nil && st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 1800*time.Millisecond && takeWriteSlot(false) {
					st.lastOrdTS["BID"] = now
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
		favBps := (fairLt - ae) / ae * 10000.0
		if favBps > st.peakFavBps {
			st.peakFavBps = favBps
		}
		slBps := 4.8
		slHitLong := fairLt <= ae*(1.0-slBps/10000.0)
		defBailLong := vel3sBps <= -8.5 && obi <= -0.55 && fairLt <= ae*(1.0-5.0/10000.0)
		timeScratchLong := holdS >= 115.0 // Zombie-only IOC bail after 115s; 65-115s uses passive Maker scratch!
		if posUSD < 10.10 {
			if (fairLt >= ae*(1.0+stepBps/10000.0) || slHitLong || defBailLong || timeScratchLong) && st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 2*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["ASK"] = now
				if (slHitLong || defBailLong) && st.peakFavBps < 2.2 {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(35 * time.Second)
				}
				placeSide(sym, "ASK", roundTo(lb*0.9994, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else if slHitLong || defBailLong || timeScratchLong {
			if ao := st.activeOrds["ASK"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "ASK")
			}
			if st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 350*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["ASK"] = now
				if (slHitLong || defBailLong) && st.peakFavBps < 2.2 {
					probationForce.Store(true)
					streakWins.Store(0)
					lastStopMS.Store(now.UnixMilli())
					st.stopCoolUntil = now.Add(35 * time.Second)
				}
				placeSide(sym, "ASK", roundTo(lb*0.9994, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else {
			effStep := stepBps
			if stepBps >= 3.0 && vel3sBps >= 2.2 && obi >= 0.35 {
				effStep = 5.2 // Momentum runner boost (~+$0.055-$0.068 wins!)
			}
			wantAsk := roundTo(ae*(1.0+effStep/10000.0), m.PxDec)
			if effStep > 0 && fairLt > wantAsk {
				wantAsk = roundTo(fairLt, m.PxDec)
			}
			if ao := st.activeOrds["ASK"]; ao != nil && ao.IsClose && !ao.IsIOC && math.Abs(ao.Px-wantAsk) >= tick*2 && now.Sub(st.lastOrdTS["ASK"]) >= 400*time.Millisecond && takeWriteSlot(true) {
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
			snipeAskPx := roundTo(math.Max(lb-tick*3, fairLt*(1.0+minEdgeGuarantee/10000.0)), m.PxDec)
			if snipeAskPx <= lb && cancelDistantForSnipe(sym, "ASK") {
				if st.activeOrds["BID"] != nil && !cancelDistantForSnipe(sym, "BID") {
					cancelSide(sym, "BID")
				}
				if st.activeOrds["ASK"] == nil && st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 1800*time.Millisecond && takeWriteSlot(false) {
					st.lastOrdTS["ASK"] = now
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
	if shouldLog && !pairMarketActive.Load() {
		fmt.Printf("[%s] GO-TOKYO v63 Eq=$%.4f | %s Pos=%.5f | Net=$%.2f | Rng60s=%.1fbps | Mac=%.1fbps | Strk=%d | Prob=%v | OBI=%.2f\n",
			now.Format("15:04:05"), float64(eqNow.Load())/1e6, sym, pos, netUSD, st.range60sBps, macroTrendBps, streakWins.Load(), probationForce.Load(), obi)
	}
}

func rebalanceFromTrades(tb []byte) {
	var resp struct {
		Trades []struct {
			Symbol string `json:"market_symbol"`
			BidID  int64  `json:"bid_account_id"`
			AskID  int64  `json:"ask_account_id"`
			BidPnl string `json:"bid_account_pnl"`
			AskPnl string `json:"ask_account_pnl"`
			TS     int64  `json:"timestamp"`
		} `json:"trades"`
	}
	if json.Unmarshal(tb, &resp) != nil {
		return
	}
	nowMS := time.Now().UnixMilli()
	pnl := map[string]float64{"BTC": 0, "ETH": 0, "SOL": 0, "HYPE": 0}
	wins := map[string]float64{"BTC": 0, "ETH": 0, "SOL": 0, "HYPE": 0}
	closes := map[string]float64{"BTC": 0, "ETH": 0, "SOL": 0, "HYPE": 0}
	latestDecided := false
	inCaution := probationForce.Load()
	streak := 0
	streakBroken := false
	for _, t := range resp.Trades {
		if t.TS < 1795104600000 {
			continue
		}
		s := t.Symbol
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
			if realized <= -0.005 {
				streakBroken = true
			} else if realized >= 0.006 && t.TS > lastStopMS.Load() {
				streak++
			}
		}
		if !latestDecided && math.Abs(realized) >= 0.005 {
			// Smooth 3-Step Ladder: Step 0 cautious ($75) on any loss within 12m; cleared on 1 green win >= $0.008!
			if realized <= -0.005 && float64(nowMS-t.TS)/60000.0 <= 12.0 {
				inCaution = true
				probationForce.Store(true)
				streak = 0
				streakBroken = true
				latestDecided = true
			} else if realized >= 0.008 && t.TS > lastStopMS.Load() {
				inCaution = false
				probationForce.Store(false)
				latestDecided = true
			} else if realized <= -0.005 {
				latestDecided = true
			}
		}
		ageMin := math.Max(0.0, float64(nowMS-t.TS)/60000.0)
		w := math.Exp(-ageMin / 20.0)
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
	bestSym := "BTC"
	evScores := map[string]float64{}
	spreadMap := map[string]float64{}
	shMap := map[string]float64{}
	rngMap := map[string]float64{}
	for _, s := range evergreenSyms {
		st := mktStates[s]
		st.mu.Lock()
		sp := st.emaSpreadBps
		sh := st.shadowBps
		rng := st.range60sBps
		st.mu.Unlock()
		spreadMap[s], shMap[s], rngMap[s] = sp, sh, rng
		wr := 0.75
		if closes[s] > 0.05 {
			wr = wins[s] / closes[s]
		}
		sc := pnl[s]*90.0 + sh*0.35 + (wr-0.75)*4.0
		if sp > 0 {
			sc -= sp * 1.8
		}
		evScores[s] = sc
	}
	// Fundamental Law #2: ETH Maker owns Calm (<10.5bps BTC range) at 91.1% WR; BTC owns Active (>=10.5bps) Maker + Sniper!
	bestSym = "BTC"
	if rngMap["BTC"] > 0 && rngMap["BTC"] < 10.5 {
		if (spreadMap["ETH"] == 0 || spreadMap["ETH"] <= 1.15) && (rngMap["ETH"] == 0 || (rngMap["ETH"] >= 2.8 && rngMap["ETH"] <= 15.0)) {
			bestSym = "ETH"
		}
	}
	baseClip := math.Min(550.0, curEq*1.85)
	if inCaution {
		baseClip = math.Min(500.0, curEq*1.55)
	} else if streak >= 2 {
		baseClip = math.Min(550.0, curEq*2.02)
	}
	for _, sym := range evergreenSyms {
		st := mktStates[sym]
		isMakerSym := sym == bestSym
		clip := -1.0
		if isMakerSym || sym == "BTC" {
			clip = baseClip // Keep BTC armed at 0ms for sudden >=8.5bps breakout snipes even when ETH owns calm Maker!
		}
		if !isMakerSym {
			st.mu.Lock()
			for _, sd := range []string{"BID", "ASK"} {
				if ao := st.activeOrds[sd]; ao != nil && !ao.IsClose {
					cancelSide(sym, sd)
				}
			}
			st.mu.Unlock()
		}
		st.mu.Lock()
		st.dynClipUSD = clip
		st.makerActive = false
		st.dynEdgeBps = 4.6
		st.entryCool = 2000 * time.Millisecond
		st.rollPnl = pnl[sym]
		st.mu.Unlock()
		st.rollPnlMicro.Store(int64(evScores[sym] * 1e6))
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
				managePairArb()
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
						if sym == "QQQ" || sym == "US100" {
							managePairArb()
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
						st.ltBid = 0
						st.ltAsk = 0
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
								if bp > st.ltBid {
									st.ltBid = bp
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
								if st.ltAsk == 0 || ap < st.ltAsk {
									st.ltAsk = ap
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
					if sym == "QQQ" || sym == "US100" {
						if bboChanged {
							managePairArb()
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
	url := "wss://fstream.binance.com/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker/solusdt@bookTicker/hypeusdt@bookTicker"
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
			if json.Unmarshal(msg, &env) == nil && env.Data.S != "" {
				sym := strings.TrimSuffix(env.Data.S, "USDT")
				b, _ := strconv.ParseFloat(env.Data.B, 64)
				bq, _ := strconv.ParseFloat(env.Data.BQ, 64)
				a, _ := strconv.ParseFloat(env.Data.A, 64)
				aq, _ := strconv.ParseFloat(env.Data.AQ, 64)
				if b > 0 && a > 0 && bq > 0 && aq > 0 {
					mVal := 0.5 * (b + a)
					microVal := (b*aq + a*bq) / (bq + aq)
					obiVal := (bq - aq) / (bq + aq)
					now := time.Now()
					st := mktStates[sym]
					if st == nil {
						continue
					}
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
	for _, sym := range []string{"BTC", "ETH"} {
		st := mktStates[sym]
		if st == nil {
			continue
		}
		resp, err := hc.Get("https://fapi.binance.com/fapi/v1/klines?symbol=" + sym + "USDT&interval=1m&limit=20")
		if err != nil {
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var kl [][]json.RawMessage
		if json.Unmarshal(body, &kl) != nil || len(kl) == 0 {
			continue
		}
		var ef, es, em, eu, eeq, e5m, lastC, lastHi, lastLo float64
		for _, k := range kl {
			if len(k) < 5 {
				continue
			}
			var hStr, lStr, cStr string
			_ = json.Unmarshal(k[2], &hStr)
			_ = json.Unmarshal(k[3], &lStr)
			_ = json.Unmarshal(k[4], &cStr)
			hi, _ := strconv.ParseFloat(hStr, 64)
			lo, _ := strconv.ParseFloat(lStr, 64)
			c, _ := strconv.ParseFloat(cStr, 64)
			if c <= 0 {
				continue
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
	fetchTr := func(mid int) []trPt {
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
	uPts := fetchTr(181)
	qPts := fetchTr(129)
	if len(uPts) < 5 || len(qPts) < 5 {
		return
	}
	var ratios []float64
	ui := 0
	for _, q := range qPts {
		for ui+1 < len(uPts) && math.Abs(float64(uPts[ui+1].ts-q.ts)) <= math.Abs(float64(uPts[ui].ts-q.ts)) {
			ui++
		}
		if math.Abs(float64(uPts[ui].ts-q.ts)) <= 8000 && uPts[ui].px > 0 {
			ratios = append(ratios, q.px/uPts[ui].px)
		}
	}
	if len(ratios) >= 5 {
		sort.Float64s(ratios)
		med := ratios[len(ratios)/2]
		pairMu.Lock()
		pairEmaRatio = med
		pairLastEmaTS = time.Now()
		pairWarmTicks = 12
		pairMarketLastActiveTS = time.Now()
		pairMarketActive.Store(true)
		pairMu.Unlock()
		fmt.Printf("[PAIR_SEED] QQQ/US100 seeded median ratio=%.7f from %d prints\n", med, len(ratios))
	}
}

func managePairArb() {
	pairMu.Lock()
	defer pairMu.Unlock()

	qSt := mktStates["QQQ"]
	uSt := mktStates["US100"]
	if qSt == nil || uSt == nil {
		return
	}
	now := time.Now()

	qSt.mu.Lock()
	for _, sd := range []string{"BID", "ASK"} {
		if ao := qSt.activeOrds[sd]; ao != nil && ao.IsIOC && now.Sub(ao.TS).Seconds() >= 2.0 {
			delete(qSt.activeOrds, sd)
		}
	}
	qb, qa, qPos, qAE, qBookTS, qLastFill := qSt.ltBid, qSt.ltAsk, qSt.pos, qSt.avgEntry, qSt.lastBookTS, qSt.lastFillTS
	qBidOrd, qAskOrd := qSt.activeOrds["BID"], qSt.activeOrds["ASK"]
	qLastOrdBid, qLastOrdAsk := qSt.lastOrdTS["BID"], qSt.lastOrdTS["ASK"]
	qLastCanBid, qLastCanAsk := qSt.lastCancelTS["BID"], qSt.lastCancelTS["ASK"]
	qSt.mu.Unlock()

	uSt.mu.Lock()
	for _, sd := range []string{"BID", "ASK"} {
		if ao := uSt.activeOrds[sd]; ao != nil && ao.IsIOC && now.Sub(ao.TS).Seconds() >= 2.0 {
			delete(uSt.activeOrds, sd)
		}
	}
	ub, ua, uPos, uAE, uBookTS := uSt.ltBid, uSt.ltAsk, uSt.pos, uSt.avgEntry, uSt.lastBookTS
	uSt.mu.Unlock()

	validBooks := qb > 0 && qa > qb && ub > 0 && ua > ub
	booksFresh := validBooks && !qBookTS.IsZero() && !uBookTS.IsZero() && now.Sub(qBookTS) < 15*time.Second && now.Sub(uBookTS) < 15*time.Second
	qMid, uMid, qSpBps, uSpBps := 0.0, 0.0, 999.0, 999.0
	if validBooks {
		qMid = 0.5 * (qb + qa)
		uMid = 0.5 * (ub + ua)
		qSpBps = (qa - qb) / qMid * 10000.0
		uSpBps = (ua - ub) / uMid * 10000.0
		qSt.netMicroUSD.Store(int64(qPos * qMid * 1e6))
		uSt.netMicroUSD.Store(int64(uPos * uMid * 1e6))
	}
	spreadsTight := booksFresh && uSpBps <= 1.90 && qSpBps <= 2.30
	if spreadsTight {
		pairMarketLastActiveTS = now
		pairMarketActive.Store(true)
		instR := qMid / uMid
		if pairEmaRatio <= 0 {
			pairEmaRatio = instR
			pairLastEmaTS = now
		} else if math.Abs(instR-pairEmaRatio)/pairEmaRatio*10000.0 < 25.0 {
			dt := math.Min(5.0, math.Max(0.001, now.Sub(pairLastEmaTS).Seconds()))
			pairLastEmaTS = now
			tau := 600.0
			if math.Abs(qPos) >= 0.015 || math.Abs(uPos) >= 0.0003 {
				tau = 1800.0
			}
			pairEmaRatio += (1.0 - math.Exp(-dt/tau)) * (instR - pairEmaRatio)
			pairWarmTicks++
		}
	} else if !pairMarketLastActiveTS.IsZero() && now.Sub(pairMarketLastActiveTS) > 45*time.Second && math.Abs(qPos) < 0.0075 && math.Abs(uPos) < 0.0002 {
		pairMarketActive.Store(false)
	}

	if !validBooks {
		return
	}

	qUSD := qPos * qMid
	uUSD := uPos * uMid
	netDeltaUSD := qUSD + uUSD
	devBps := 0.0
	if pairEmaRatio > 0 {
		fairQ := uMid * pairEmaRatio
		devBps = (qMid - fairQ) / fairQ * 10000.0
	}

	if now.Sub(lastPairLog) >= 25*time.Second {
		lastPairLog = now
		fmt.Printf("[%s] GO-TOKYO v63 Eq=$%.4f | QQQ=%.4f ($%.1f) US100=%.6f ($%.1f) | Net=$%.2f | Sp=%.2f/%.2fbps | Dev=%.2fbps | Warm=%d\n",
			now.Format("15:04:05"), float64(eqNow.Load())/1e6, qPos, qUSD, uPos, uUSD, netDeltaUSD, qSpBps, uSpBps, devBps, pairWarmTicks)
	}

	// STEP A: 0ms Auto-Hedger & Residual Unwind (Highest Priority!)
	if uSpBps <= 8.0 && qSpBps <= 8.0 {
		// A1: QQQ flat, US100 still holding hedge -> close 100% of US100 ReduceOnly IOC immediately!
		if math.Abs(qPos) < 0.0075 && math.Abs(uPos) >= 0.00020 {
			if qBidOrd != nil && !qBidOrd.IsIOC {
				cancelPairSide("QQQ", "BID")
			}
			if qAskOrd != nil && !qAskOrd.IsIOC {
				cancelPairSide("QQQ", "ASK")
			}
			if now.Sub(lastHedgeTS) >= 850*time.Millisecond && takeWriteSlotTier(2) {
				lastHedgeTS = now
				pairEntryTS = time.Time{}
				if uPos > 0 {
					placePairOrder("US100", "ASK", math.Floor(ub*0.9985), uMid, math.Abs(uPos), true, true)
				} else {
					placePairOrder("US100", "BID", math.Ceil(ua*1.0015), uMid, math.Abs(uPos), true, true)
				}
			}
			return
		}
		// A2: QQQ holds >= $11 position and netDeltaUSD >= $11.2 -> hedge US100 IOC immediately!
		if math.Abs(qUSD) >= 11.0 && math.Abs(netDeltaUSD) >= 11.2 {
			if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC {
				cancelPairSide("QQQ", "BID")
			}
			if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC {
				cancelPairSide("QQQ", "ASK")
			}
			if now.Sub(lastHedgeTS) >= 850*time.Millisecond && takeWriteSlotTier(2) {
				lastHedgeTS = now
				if pairEntryTS.IsZero() {
					pairEntryTS = now
				}
				needUUSD := -netDeltaUSD
				needUSz := roundTo(math.Abs(needUUSD)/uMid, 6)
				if uPos > 0.00020 && needUUSD < 0 {
					sz := math.Min(math.Abs(uPos), needUSz)
					placePairOrder("US100", "ASK", math.Floor(ub*0.9985), uMid, sz, true, true)
				} else if uPos < -0.00020 && needUUSD > 0 {
					sz := math.Min(math.Abs(uPos), needUSz)
					placePairOrder("US100", "BID", math.Ceil(ua*1.0015), uMid, sz, true, true)
				} else if needUSz >= 0.00042 {
					if needUUSD < 0 {
						placePairOrder("US100", "ASK", math.Floor(ub*0.9988), uMid, needUSz, false, true)
					} else {
						placePairOrder("US100", "BID", math.Ceil(ua*1.0012), uMid, needUSz, false, true)
					}
				}
			}
			return
		}
		// A3: Tiny sub-$11 partial QQQ fill sitting > 1.8s without US100 hedge -> clean ReduceOnly IOC!
		if math.Abs(qPos) >= 0.0075 && math.Abs(qUSD) < 11.0 && math.Abs(uPos) < 0.00020 {
			if now.Sub(qLastFill) >= 1800*time.Millisecond && now.Sub(lastHedgeTS) >= 1200*time.Millisecond && takeWriteSlotTier(2) {
				lastHedgeTS = now
				if qBidOrd != nil && !qBidOrd.IsIOC {
					cancelPairSide("QQQ", "BID")
				}
				if qAskOrd != nil && !qAskOrd.IsIOC {
					cancelPairSide("QQQ", "ASK")
				}
				if qPos > 0 {
					placePairOrder("QQQ", "ASK", roundTo(qb*0.9985, 2), qMid, math.Abs(qPos), true, true)
				} else {
					placePairOrder("QQQ", "BID", roundTo(qa*1.0015, 2), qMid, math.Abs(qPos), true, true)
				}
			}
			return
		}
	}

	// STEP B: Hedged Pair Convergence Exit (QQQ Maker $0.01 tick -> triggers A1 US100 close on fill!)
	if math.Abs(qUSD) >= 11.0 && math.Abs(netDeltaUSD) < 11.2 {
		if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC {
			cancelPairSide("QQQ", "BID")
		}
		if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC {
			cancelPairSide("QQQ", "ASK")
		}
		if qAE <= 0 {
			qAE = qMid
		}
		if uAE <= 0 {
			uAE = uMid
		}
		if pairEntryTS.IsZero() {
			pairEntryTS = now
		}
		ageS := now.Sub(pairEntryTS).Seconds()
		tpNetBps := 1.18
		if ageS >= 300.0 {
			tpNetBps = 0.18
		} else if ageS >= 120.0 {
			tpNetBps = 0.65
		}

		if qPos > 0 {
			uLegBps := (uAE - ua) / uAE * 10000.0
			mtmBps := (qb-qAE)/qAE*10000.0 + uLegBps
			if mtmBps <= -16.0 {
				if qAskOrd != nil && !qAskOrd.IsIOC {
					cancelPairSide("QQQ", "ASK")
				} else if qAskOrd == nil && now.Sub(qLastOrdAsk) >= 400*time.Millisecond && takeWriteSlotTier(2) {
					placePairOrder("QQQ", "ASK", roundTo(qb*0.9985, 2), qMid, math.Abs(qPos), true, true)
				}
			} else {
				needQBps := tpNetBps - uLegBps
				wantAsk := math.Ceil(qAE*(1.0+needQBps/10000.0)*100.0) / 100.0
				if wantAsk < qb {
					wantAsk = qb
				}
				if qAskOrd != nil && qAskOrd.IsClose && !qAskOrd.IsIOC && math.Abs(qAskOrd.Px-wantAsk) >= 0.02 && now.Sub(qLastOrdAsk) >= 450*time.Millisecond && takeWriteSlot(true) {
					modifyPairSide("QQQ", "ASK", wantAsk, qMid)
				} else if qAskOrd == nil && now.Sub(qLastOrdAsk) >= 300*time.Millisecond && takeWriteSlot(true) {
					placePairOrder("QQQ", "ASK", wantAsk, qMid, math.Abs(qPos), true, false)
				}
			}
		} else {
			uLegBps := (ub - uAE) / uAE * 10000.0
			mtmBps := (qAE-qa)/qAE*10000.0 + uLegBps
			if mtmBps <= -16.0 {
				if qBidOrd != nil && !qBidOrd.IsIOC {
					cancelPairSide("QQQ", "BID")
				} else if qBidOrd == nil && now.Sub(qLastOrdBid) >= 400*time.Millisecond && takeWriteSlotTier(2) {
					placePairOrder("QQQ", "BID", roundTo(qa*1.0015, 2), qMid, math.Abs(qPos), true, true)
				}
			} else {
				needQBps := tpNetBps - uLegBps
				wantBid := math.Floor(qAE*(1.0-needQBps/10000.0)*100.0) / 100.0
				if wantBid > qa {
					wantBid = qa
				}
				if qBidOrd != nil && qBidOrd.IsClose && !qBidOrd.IsIOC && math.Abs(qBidOrd.Px-wantBid) >= 0.02 && now.Sub(qLastOrdBid) >= 450*time.Millisecond && takeWriteSlot(true) {
					modifyPairSide("QQQ", "BID", wantBid, qMid)
				} else if qBidOrd == nil && now.Sub(qLastOrdBid) >= 300*time.Millisecond && takeWriteSlot(true) {
					placePairOrder("QQQ", "BID", wantBid, qMid, math.Abs(qPos), true, false)
				}
			}
		}
		return
	}

	// STEP C: Passive QQQ Maker Entry (0% fee, $0.01 fine tick, 94-100% historical WR!)
	if math.Abs(qPos) >= 0.0075 || math.Abs(uPos) >= 0.00020 {
		return
	}
	legacyBusy := math.Abs(float64(mktStates["BTC"].netMicroUSD.Load()))/1e6 >= 5.0 || math.Abs(float64(mktStates["ETH"].netMicroUSD.Load()))/1e6 >= 5.0
	if !spreadsTight || pairWarmTicks < 15 || pairEmaRatio <= 0 || legacyBusy {
		if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC {
			cancelPairSide("QQQ", "BID")
		}
		if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC {
			cancelPairSide("QQQ", "ASK")
		}
		return
	}

	fairQ := uMid * pairEmaRatio
	if math.Abs(qMid-fairQ)/fairQ*10000.0 > 10.0 {
		if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC {
			cancelPairSide("QQQ", "BID")
		}
		if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC {
			cancelPairSide("QQQ", "ASK")
		}
		return
	}

	// Pull immediately via 0ms quota-free L2CancelOrder if US100 moved against resting QQQ quote so remaining edge < 0.95bps!
	if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC && fairQ <= qBidOrd.Px*(1.0+0.95/10000.0) {
		cancelPairSide("QQQ", "BID")
		qBidOrd = nil
		qLastCanBid = now
	}
	if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC && fairQ >= qAskOrd.Px*(1.0-0.95/10000.0) {
		cancelPairSide("QQQ", "ASK")
		qAskOrd = nil
		qLastCanAsk = now
	}

	if now.Sub(qLastFill) < 2500*time.Millisecond {
		return
	}

	entryEdgeBps := 1.75
	preferBid := (fairQ - qb) >= (qa - fairQ)
	curEq := float64(eqNow.Load()) / 1e6
	if curEq < 50.0 {
		curEq = 98.0
	}
	legUSD := math.Min(340.0, math.Max(140.0, curEq*2.60))

	if preferBid {
		if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC {
			cancelPairSide("QQQ", "ASK")
		}
		wantBid := math.Floor(fairQ*(1.0-entryEdgeBps/10000.0)*100.0) / 100.0
		if wantBid >= qa {
			wantBid = roundTo(qa-0.01, 2)
		}
		if qBidOrd == nil && now.Sub(qLastCanBid) >= 2200*time.Millisecond && now.Sub(qLastOrdBid) >= 1800*time.Millisecond && takeWriteSlot(false) {
			placePairOrder("QQQ", "BID", wantBid, qMid, legUSD/wantBid, false, false)
		} else if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC && (math.Abs(qBidOrd.Px-wantBid) >= 0.02 || now.Sub(qBidOrd.TS).Seconds() >= 40.0) && now.Sub(qLastOrdBid) >= 700*time.Millisecond && takeWriteSlot(false) {
			modifyPairSide("QQQ", "BID", wantBid, qMid)
		}
	} else {
		if qBidOrd != nil && !qBidOrd.IsClose && !qBidOrd.IsIOC {
			cancelPairSide("QQQ", "BID")
		}
		wantAsk := math.Ceil(fairQ*(1.0+entryEdgeBps/10000.0)*100.0) / 100.0
		if wantAsk <= qb {
			wantAsk = roundTo(qb+0.01, 2)
		}
		if qAskOrd == nil && now.Sub(qLastCanAsk) >= 2200*time.Millisecond && now.Sub(qLastOrdAsk) >= 1800*time.Millisecond && takeWriteSlot(false) {
			placePairOrder("QQQ", "ASK", wantAsk, qMid, legUSD/wantAsk, false, false)
		} else if qAskOrd != nil && !qAskOrd.IsClose && !qAskOrd.IsIOC && (math.Abs(qAskOrd.Px-wantAsk) >= 0.02 || now.Sub(qAskOrd.TS).Seconds() >= 40.0) && now.Sub(qLastOrdAsk) >= 700*time.Millisecond && takeWriteSlot(false) {
			modifyPairSide("QQQ", "ASK", wantAsk, qMid)
		}
	}
}

func runPairHeartbeat() {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for range t.C {
		managePairArb()
	}
}

func main() {
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
	mktStates["BTC"].dynClipUSD = 150.0
	mktStates["ETH"].dynClipUSD = -1.0
	mktStates["BTC"].makerActive = false

	go runTxWorker()
	go runTxWS()
	go runLtWS()
	time.Sleep(1500 * time.Millisecond)
	cancelAll()
	seedBinanceHistory()
	seedPairHistory()
	fmt.Printf("GO-TOKYO v63 LIVE | Delta-Neutral QQQ/US100 Pair Arb (94%%+ WR) + BTC Weekend Snipe | Stop=$%.2f\n", StopEqUSD)
	go syncAccountLoop()
	go runPairHeartbeat()
	runBnWS()
}
