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
	CancelBps    = 1.5  // 1.5 bps anti-trend cancel shield (avoids 1-tick thrashing)
	ObiLimit     = 0.35 // Block sustained toxic-flow side when 3s-smoothed |OBI| > 0.35
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
	"ETH":  {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020, AllowEntry: true, ClipUSD: 90.0, EdgeBps: 4.5},
	"BTC":  {Sym: "BTC", ID: 1, SzDec: 5, PxDec: 1, MinB: 0.00007, AllowEntry: true, ClipUSD: 90.0, EdgeBps: 3.8},
	"SOL":  {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055, AllowEntry: true, ClipUSD: 90.0, EdgeBps: 4.5},
	"HYPE": {Sym: "HYPE", ID: 24, SzDec: 2, PxDec: 4, MinB: 0.07, AllowEntry: true, ClipUSD: 90.0, EdgeBps: 5.0},
	"ZEC":  {Sym: "ZEC", ID: 90, SzDec: 3, PxDec: 3, MinB: 0.004, AllowEntry: false, ClipUSD: 12.0, EdgeBps: 10.0},
}
var evergreenSyms = []string{"BTC", "ETH", "SOL", "HYPE"}
var bootTS = time.Now()
var idToSym = map[int64]string{0: "ETH", 1: "BTC", 2: "SOL", 24: "HYPE", 90: "ZEC"}

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
	rawObi      float64
	emaUltra      float64
	emaFast       float64
	emaSlow       float64
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
	pos         float64
	avgEntry    float64
	posOpenTS     time.Time
	lastFillTS    time.Time
	stopCoolUntil time.Time
	trendMode     bool
	dynClipUSD      float64
	dynEdgeBps      float64
	entryCool       time.Duration
	rollPnl         float64
	activeOrds      map[string]*ActiveOrder
	lastOrdTS       map[string]time.Time
	netMicroUSD     atomic.Int64
	pendBidMicroUSD atomic.Int64
	pendAskMicroUSD atomic.Int64
	rollPnlMicro    atomic.Int64
}

func newMarketState(initClip float64) *MarketState {
	return &MarketState{
		bookBids:   map[string]float64{},
		bookAsks:   map[string]float64{},
		activeOrds: map[string]*ActiveOrder{},
		lastOrdTS:  map[string]time.Time{},
		entryCool:  2 * time.Second,
		dynClipUSD: initClip,
	}
}

var (
	mktStates = map[string]*MarketState{
		"BTC": newMarketState(90.0), "ETH": newMarketState(-1.0),
		"SOL": newMarketState(-1.0), "HYPE": newMarketState(-1.0), "ZEC": newMarketState(-1.0),
	}
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

func takeWriteSlotTier(tier int) bool {
	rateMu.Lock()
	defer rateMu.Unlock()
	now := time.Now()
	cut := now.Add(-60 * time.Second)
	idx := 0
	for idx < len(writeHist) && writeHist[idx].Before(cut) {
		idx++
	}
	if idx > 0 {
		writeHist = writeHist[idx:]
	}
	limit := 33 // Tier 0: entries & quote modifies
	if tier == 1 {
		limit = 37 // Tier 1: TP closes & cancels
	} else if tier >= 2 {
		limit = 40 // Tier 2: Emergency SL & Crash Bailouts (never starved!)
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

func otherDirExposureUSD(selfSym string, checkLong bool) float64 {
	mySt := mktStates[selfSym]
	myPnl := int64(0)
	if mySt != nil {
		myPnl = mySt.rollPnlMicro.Load()
	}
	var sum int64
	for sym, st := range mktStates {
		if sym == selfSym {
			continue
		}
		n := st.netMicroUSD.Load()
		otherPnl := st.rollPnlMicro.Load()
		isOtherStronger := otherPnl > myPnl || (otherPnl == myPnl && sym < selfSym)
		if checkLong {
			if n > 0 {
				sum += n
			}
			if isOtherStronger {
				sum += st.pendBidMicroUSD.Load()
			}
		} else {
			if n < 0 {
				sum += -n
			}
			if isOtherStronger {
				sum += st.pendAskMicroUSD.Load()
			}
		}
	}
	return float64(sum) / 1e6
}

func preemptWeakerQuote(leaderSym, side string) {
	mySt := mktStates[leaderSym]
	if mySt == nil {
		return
	}
	myPnl := mySt.rollPnlMicro.Load()
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
				if ao := oSt.activeOrds[side]; ao != nil && !ao.IsClose && takeWriteSlot(true) {
					cancelSide(s, side)
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
		if st.range60sBps > 9.5 {
			clip = math.Max(12.0, clip*(9.5/st.range60sBps)) // Avellaneda-Stoikov inverse volatility sizing!
		}
		if !isClose && isIOC {
			clip = math.Max(12.0, clip*0.65) // Shield #5: 0.65x cap on Taker Snipes
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
		if secs >= 45.0 {
			return -1.2 // Shield #1: exit before >45s zombie tail!
		}
		if secs >= 30.0 {
			return 0.0 // Exact breakeven at 30s
		}
		if secs >= 18.0 {
			return 1.4 // Fast green step-down at 18s
		}
	}
	if st.range60sBps > 0 && st.range60sBps < 3.5 {
		return 3.0 // Low-volume quiet-hour snap target
	}
	return 3.5
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
	if age >= 30.0 {
		tgt = 0.0
	} else if age >= 18.0 {
		tgt = 1.4
	}
	if st.shDir == 1 {
		pnlBps := (lb - st.shEntry) / st.shEntry * 10000.0
		if la >= st.shEntry*(1.0+tgt/10000.0) {
			st.shadowBps += tgt
			st.shDir = 0
		} else if pnlBps <= -8.5 || age >= 45.0 {
			st.shadowBps += pnlBps
			st.shDir = 0
		}
	} else {
		pnlBps := (st.shEntry - la) / st.shEntry * 10000.0
		if lb <= st.shEntry*(1.0-tgt/10000.0) {
			st.shadowBps += tgt
			st.shDir = 0
		} else if pnlBps <= -8.5 || age >= 45.0 {
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

	ef, es, eu := st.emaFast, st.emaSlow, st.emaUltra
	trendBps, impBps, vel3sBps := 0.0, 0.0, 0.0
	if ef > 0 && es > 0 {
		trendBps = (ef - es) / es * 10000.0
		impBps = (mid - ef) / ef * 10000.0
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
		st.basisBps += (1.0 - math.Exp(-bdt/60.0)) * (instBasis - st.basisBps)
		st.emaSpreadBps += (1.0 - math.Exp(-bdt/45.0)) * (spBps - st.emaSpreadBps)
	}
	fairLt := micro * (1.0 + st.basisBps/10000.0)

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
	volSafe := st.range60sBps <= 18.0 // Shield #3: pause new entries during >18bps 1m flash cascades!

	// All-Weather Avellaneda-Stoikov Edge + Clock-Boundary Cushion (:58-:02 & :29-:32)
	baseEdge := st.dynEdgeBps
	if baseEdge <= 0 {
		baseEdge = m.EdgeBps
	}
	edge := baseEdge
	if st.range60sBps < 4.5 {
		edge = math.Min(baseEdge, 3.8) // Low-volume regime: tight 3.8bps Maker fills
	} else if st.range60sBps <= 9.5 {
		edge = 3.8 + (st.range60sBps-4.5)*0.36 // Medium-volume regime: 3.8 -> 5.6bps
	} else {
		edge = 5.6 + (st.range60sBps-9.5)*0.22 // High-volume regime: 5.6 -> 7.5bps
	}
	snipeThresh := 3.8
	if st.range60sBps > 8.0 {
		snipeThresh = 4.5
	}
	minHr := now.Minute()
	if minHr >= 58 || minHr <= 2 || (minHr >= 29 && minHr <= 32) {
		edge += 1.4 // Shield #4: institutional :00/:30 TWAP rollover cushion
		snipeThresh += 1.0
	}

	netCap := 15.0
	otherLong := otherDirExposureUSD(sym, true)
	otherShort := otherDirExposureUSD(sym, false)
	coinActive := m.AllowEntry && st.dynClipUSD >= 0
	inCool := now.Before(st.stopCoolUntil) || now.Sub(st.lastFillTS) < 4*time.Second
	bidAllowed := volSafe && trendBps >= -2.2 && trendBps <= 5.0 && impBps >= -2.5 && impBps <= 3.5
	askAllowed := volSafe && trendBps <= 2.2 && trendBps >= -5.0 && impBps <= 2.5 && impBps >= -3.5

	// Update 24/7 Zero-Risk Virtual Paper Engine on ALL 4 coins before coinActive filter!
	updateShadowPaper(st, now, lb, la, fairLt, edge, bidAllowed && vel3sBps > -2.0 && obi >= -ObiLimit, askAllowed && vel3sBps < 2.0 && obi <= ObiLimit)

	bidSafe := coinActive && !inCool && now.After(st.dropCoolUntil) && vel3sBps > -2.0 && bidAllowed && obi >= -ObiLimit && otherLong < netCap
	askSafe := coinActive && !inCool && now.After(st.pumpCoolUntil) && vel3sBps < 2.0 && askAllowed && obi <= ObiLimit && otherShort < netCap
	keepBid := coinActive && volSafe && !inCool && now.After(st.dropCoolUntil) && trendBps >= -3.5 && trendBps <= 5.5 && vel3sBps > -3.5 && obi >= -0.58 && st.rawObi > -0.78 && otherLong < netCap
	keepAsk := coinActive && volSafe && !inCool && now.After(st.pumpCoolUntil) && trendBps <= 3.5 && trendBps >= -5.5 && vel3sBps < 3.5 && obi <= 0.58 && st.rawObi < 0.78 && otherShort < netCap

	staleSnipeBuy := coinActive && volSafe && !inCool && now.After(st.dropCoolUntil) && math.Abs(posUSD) < 1.0 && otherLong < netCap && (fairLt-la)/la*10000.0 >= snipeThresh && trendBps >= -2.2 && trendBps <= 5.0 && vel3sBps > -0.8 && obi >= 0.22
	staleSnipeSell := coinActive && volSafe && !inCool && now.After(st.pumpCoolUntil) && math.Abs(posUSD) < 1.0 && otherShort < netCap && (lb-fairLt)/lb*10000.0 >= snipeThresh && trendBps <= 2.2 && trendBps >= -5.0 && vel3sBps < 0.8 && obi <= -0.22
	btcTrendBuy := staleSnipeBuy
	btcTrendSell := staleSnipeSell

	// 1. Manage resting orders: ModifyOrder (1 write) or 0ms Tier-2 Anti-Pickoff Cancel
	pickoffBuf := math.Max(1.4, edge*0.38)
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
				if (aboutToGetPickedOff && takeWriteSlotTier(2)) || (!aboutToGetPickedOff && takeWriteSlot(true)) {
					cancelSide(sym, side)
				}
			} else if (moveBps >= CancelBps || ageS >= 35.0 || tooClose) && now.Sub(st.lastOrdTS[side]) >= 250*time.Millisecond && takeWriteSlot(false) {
				newPx := roundTo(math.Min(lb, fairLt*(1.0-edge/10000.0)), m.PxDec)
				if side == "ASK" {
					newPx = roundTo(math.Max(la, fairLt*(1.0+edge/10000.0)), m.PxDec)
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

	// 2. Closing orders: Shield #1 (<45s Alpha Half-Life Exit) + 9.5bps Room-to-Breathe (6.5bps after 35s)
	if pos <= -m.MinB && ae > 0 {
		slBps := 9.5
		if holdS >= 35.0 {
			slBps = 6.5
		}
		slHitShort := fairLt >= ae*(1.0+slBps/10000.0)
		defBailShort := vel3sBps >= 8.0 && fairLt >= ae*(1.0+6.0/10000.0)
		timeScratchShort := holdS >= 60.0 && la <= ae*(1.0+5.0/10000.0)
		if posUSD > -10.10 {
			if (fairLt <= ae*(1.0-stepBps/10000.0) || slHitShort || defBailShort || timeScratchShort) && st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 3*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["BID"] = now
				if slHitShort || defBailShort {
					st.stopCoolUntil = now.Add(150 * time.Second)
				}
				placeSide(sym, "BID", roundTo(la*1.0015, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else if slHitShort || defBailShort || timeScratchShort {
			if ao := st.activeOrds["BID"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "BID")
			}
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 400*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["BID"] = now
				if slHitShort || defBailShort {
					st.stopCoolUntil = now.Add(150 * time.Second)
				}
				placeSide(sym, "BID", roundTo(la*1.0015, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else {
			effStep := stepBps
			if stepBps > 0 && vel3sBps <= -2.5 && obi <= -0.35 {
				effStep = 6.5
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
	} else if math.Abs(posUSD) < 1.0 && (bidSafe || btcTrendBuy) {
		st.trendMode = false
		if btcTrendBuy {
			snipeBidPx := roundTo(math.Max(la, fairLt*(1.0-1.5/10000.0)), m.PxDec)
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= 1500*time.Millisecond && takeWriteSlot(false) {
				st.lastOrdTS["BID"] = now
				placeSide(sym, "BID", snipeBidPx, mid, 0, false, true)
			}
		} else {
			wantBid := roundTo(math.Min(lb, fairLt*(1.0-edge/10000.0)), m.PxDec)
			if wantBid >= la {
				wantBid = roundTo(la-tick, m.PxDec)
			}
			eCool := st.entryCool
			if eCool < 1500*time.Millisecond {
				eCool = 1500 * time.Millisecond
			}
			if st.activeOrds["BID"] == nil && now.Sub(st.lastOrdTS["BID"]) >= eCool && takeWriteSlot(false) {
				st.lastOrdTS["BID"] = now
				placeSide(sym, "BID", wantBid, mid, 0, false, false)
			}
		}
	}

	if pos >= m.MinB && ae > 0 {
		slBps := 9.5
		if holdS >= 35.0 {
			slBps = 6.5
		}
		slHitLong := fairLt <= ae*(1.0-slBps/10000.0)
		defBailLong := vel3sBps <= -8.0 && fairLt <= ae*(1.0-6.0/10000.0)
		timeScratchLong := holdS >= 60.0 && lb >= ae*(1.0-5.0/10000.0)
		if posUSD < 10.10 {
			if (fairLt >= ae*(1.0+stepBps/10000.0) || slHitLong || defBailLong || timeScratchLong) && st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 3*time.Second && takeWriteSlot(true) {
				st.lastOrdTS["ASK"] = now
				if slHitLong || defBailLong {
					st.stopCoolUntil = now.Add(150 * time.Second)
				}
				placeSide(sym, "ASK", roundTo(lb*0.9985, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else if slHitLong || defBailLong || timeScratchLong {
			if ao := st.activeOrds["ASK"]; ao != nil && !ao.IsIOC {
				cancelSide(sym, "ASK")
			}
			if st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 400*time.Millisecond && takeWriteSlotTier(2) {
				st.lastOrdTS["ASK"] = now
				if slHitLong || defBailLong {
					st.stopCoolUntil = now.Add(150 * time.Second)
				}
				placeSide(sym, "ASK", roundTo(lb*0.9985, m.PxDec), mid, math.Abs(pos), true, true)
			}
		} else {
			effStep := stepBps
			if stepBps > 0 && vel3sBps >= 2.5 && obi >= 0.35 {
				effStep = 6.5
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
	} else if math.Abs(posUSD) < 1.0 && (askSafe || btcTrendSell) {
		st.trendMode = false
		if btcTrendSell {
			snipeAskPx := roundTo(math.Min(lb, fairLt*(1.0+1.5/10000.0)), m.PxDec)
			if st.activeOrds["ASK"] == nil && now.Sub(st.lastOrdTS["ASK"]) >= 1500*time.Millisecond && takeWriteSlot(false) {
				st.lastOrdTS["ASK"] = now
				placeSide(sym, "ASK", snipeAskPx, mid, 0, false, true)
			}
		} else {
			wantAsk := roundTo(math.Max(la, fairLt*(1.0+edge/10000.0)), m.PxDec)
			if wantAsk <= lb {
				wantAsk = roundTo(lb+tick, m.PxDec)
			}
			eCool := st.entryCool
			if eCool < 1500*time.Millisecond {
				eCool = 1500 * time.Millisecond
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
		fmt.Printf("[%s] GO-TOKYO v51 Eq=$%.4f | %s Pos=%.5f | Net=$%.2f | Rng60s=%.1fbps | Sh=%.1f | OBI=%.2f\n",
			now.Format("15:04:05"), float64(eqNow.Load())/1e6, sym, pos, netUSD, st.range60sBps, st.shadowBps, obi)
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
	latestCloseSeen := false
	inProbation := false
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
		if err != nil || math.Abs(realized) < 1e-8 {
			continue
		}
		if !latestCloseSeen {
			latestCloseSeen = true
			// Shield #2: Aftershock Probation ($20 clip after any >$0.015 loss until 1 green win clears it!)
			if realized < -0.015 && float64(nowMS-t.TS)/60000.0 <= 12.0 {
				inProbation = true
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
	curEq := float64(eqNow.Load()) / 1e6
	if curEq < 50.0 {
		curEq = 100.0
	}
	uptimeMin := time.Since(bootTS).Minutes()
	btcWarm := 1.2 * math.Exp(-uptimeMin/8.0) // Fades smoothly over 8m as paper scores warm up
	bestSym := ""
	bestScore := -999.0
	evScores := map[string]float64{}
	for _, s := range evergreenSyms {
		st := mktStates[s]
		st.mu.Lock()
		sp := st.emaSpreadBps
		sh := st.shadowBps
		rng := st.range60sBps
		st.mu.Unlock()
		wr := 0.75
		if closes[s] > 0.05 {
			wr = wins[s] / closes[s]
		}
		sc := pnl[s]*110.0 + sh*0.30 + (wr-0.75)*4.0
		if s == "BTC" {
			sc += btcWarm
		}
		if sp > 0 {
			sc -= sp * 1.6
		}
		evScores[s] = sc
		// Evergreen qualification: live spread <= 1.6bps (or warming up on BTC) and not in active cascade
		spreadOk := (sp > 0 && sp <= 1.6) || (sp == 0 && s == "BTC")
		if spreadOk && rng <= 18.0 && sc > bestScore {
			bestScore = sc
			bestSym = s
		}
	}
	baseClip := math.Min(550.0, curEq*0.92) // Smooth auto-compounding up to $550 clip on $500+ equity
	if inProbation {
		baseClip = 20.0 // Shield #2: $20 probation size until next green win!
	}
	for _, sym := range evergreenSyms {
		st := mktStates[sym]
		clip := -1.0
		if sym == bestSym && bestScore > -4.0 {
			clip = baseClip
		}
		st.mu.Lock()
		st.dynClipUSD = clip
		st.dynEdgeBps = 3.8
		st.entryCool = 2 * time.Second
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
		if loopCnt%3 == 1 {
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
						if st.bnMid > 0 {
							st.netMicroUSD.Store(int64(sz * st.bnMid * 1e6))
						}
						if math.Abs(sz) > 1e-6 && math.Abs(prevPos) < 1e-6 {
							st.posOpenTS = time.Now()
						} else if math.Abs(sz) < 1e-6 {
							st.posOpenTS = time.Time{}
							st.trendMode = false
						}
						if p.OpenOrderCount > 2 {
							orphanLeak = true
						}
						if p.OpenOrderCount == 0 || math.Abs(sz-prevPos) > 1e-6 {
							if math.Abs(sz-prevPos) > 1e-6 {
								st.lastFillTS = time.Now()
							}
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
		time.Sleep(2 * time.Second)
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
							if st.bnMid > 0 {
								st.netMicroUSD.Store(int64(sz * st.bnMid * 1e6))
							}
							if math.Abs(sz) > 1e-6 && math.Abs(prev) < 1e-6 {
								st.posOpenTS = time.Now()
							} else if math.Abs(sz) < 1e-6 {
								st.posOpenTS = time.Time{}
								st.trendMode = false
							}
							if math.Abs(sz-prev) > 1e-6 {
								st.lastFillTS = time.Now()
								if sz > prev {
									if ao := st.activeOrds["BID"]; ao != nil && !ao.IsClose {
										ordBase := float64(ao.BaseInt) * math.Pow10(-markets[sym].SzDec)
										if (sz-prev) < ordBase*0.95 && !ao.IsIOC {
											cancelSide(sym, "BID") // Cancel remaining partial entry so no ghost order stays on Lighter!
										} else {
											delete(st.activeOrds, "BID")
											st.pendBidMicroUSD.Store(0)
										}
									} else {
										delete(st.activeOrds, "BID")
										st.pendBidMicroUSD.Store(0)
									}
									if ao := st.activeOrds["ASK"]; ao != nil && !ao.IsClose {
										cancelSide(sym, "ASK")
									}
								} else {
									if ao := st.activeOrds["ASK"]; ao != nil && !ao.IsClose {
										ordBase := float64(ao.BaseInt) * math.Pow10(-markets[sym].SzDec)
										if (prev-sz) < ordBase*0.95 && !ao.IsIOC {
											cancelSide(sym, "ASK") // Cancel remaining partial entry so no ghost order stays on Lighter!
										} else {
											delete(st.activeOrds, "ASK")
											st.pendAskMicroUSD.Store(0)
										}
									} else {
										delete(st.activeOrds, "ASK")
										st.pendAskMicroUSD.Store(0)
									}
									if ao := st.activeOrds["BID"]; ao != nil && !ao.IsClose {
										cancelSide(sym, "BID")
									}
								}
								touched = append(touched, sym)
							}
							st.mu.Unlock()
						}
					}
					for _, sym := range touched {
						manageCoin(sym)
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
					bboChanged := st.ltBid != prevB || st.ltAsk != prevA
					shouldRun := bboChanged && st.bnMid > 0 && (st.dynClipUSD >= 0 || math.Abs(st.pos) > 1e-6 || time.Since(st.lastShadowTS) >= 100*time.Millisecond)
					st.mu.Unlock()
					if shouldRun {
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
						st.emaSlow = mVal
						st.bnObi = obiVal
						st.lastEmaTS = now
					} else {
						dt := math.Min(5.0, math.Max(0.001, now.Sub(st.lastEmaTS).Seconds()))
						st.lastEmaTS = now
						st.emaUltra += (1.0 - math.Exp(-dt/3.0)) * (mVal - st.emaUltra)
						st.emaFast += (1.0 - math.Exp(-dt/20.0)) * (mVal - st.emaFast)
						st.emaSlow += (1.0 - math.Exp(-dt/180.0)) * (mVal - st.emaSlow)
						st.bnObi += (1.0 - math.Exp(-dt/3.0)) * (obiVal - st.bnObi)
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

	go runTxWorker()
	go runTxWS()
	go runLtWS()
	time.Sleep(1500 * time.Millisecond)
	cancelAll()
	fmt.Printf("GO-TOKYO v3 LIVE | ETH+BTC | Microprice+OBI | Breakeven StepDown | ModifyOrder+WS_TX | Stop=$%.2f\n", StopEqUSD)
	go syncAccountLoop()
	runBnWS()
}
