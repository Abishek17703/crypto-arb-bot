package main

import (
	"bytes"
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
	BotVersion = "v98.0"
	BaseURL    = "https://mainnet.zklighter.elliot.ai"
	WsURL      = "wss://mainnet.zklighter.elliot.ai/stream"
	AcctIdx    = int64(770015)
	KeyIdx     = uint8(4)
	ChainID    = uint32(304)
	PrivKey    = "56491130b44362b137b0a3b46dc63d647865876f11ff1613c857f946ec8612f6e4f216d065f4695a"
	StopEqUSD  = 83.50

	BnApiKey = "I4IImhi629VlyPD28pnIkveLZsAwH5yf0COTtmJQuYiGFICACdCS03HEXz2OCti4"
	BnApiSec = "ng4mncCOkk5DvRJxPARevMGoDACQBEpzWBqh5XJ8TlEYCxR0TWpSp8vKVitg3U4D"

	S3Bucket      = "crypto-arb-bot-tokyo-bin-767579108352"
	S3Host        = "crypto-arb-bot-tokyo-bin-767579108352.s3.ap-northeast-1.amazonaws.com"
	PauseFlagPath = "/tmp/lighter_pause.flag"
	DnHistPrimary = "/opt/crypto-arb-bot/dn_trades_history.json"
	DnHistBackup  = "/tmp/dn_trades_history.json"
)

type Market struct {
	Sym   string
	ID    int64
	SzDec int
	PxDec int
	MinB  float64
}

type ActiveOrder struct {
	COI     int64
	Px      float64
	BaseInt int64
	IsClose bool
	IsIOC   bool
	TS      time.Time
}

type MarketState struct {
	mu           sync.Mutex
	bookBids     map[string]float64
	bookAsks     map[string]float64
	ltBid        float64
	ltBidSz      float64
	ltAsk        float64
	ltAskSz      float64
	lastBookTS   time.Time
	pos          float64
	avgEntry     float64
	lastFillTS   time.Time
	activeOrds   map[string]*ActiveOrder
	lastOrdTS    map[string]time.Time
	lastCancelTS map[string]time.Time
}

type DnSpec struct {
	Sym        string
	BnSym      string
	Quote      string // "USDC" (0% Maker) or "USDT" (2bp Maker)
	MakerFeeBp float64
	TakerFeeBp float64
	QDec       int
	PxDec      int
	Step       float64
	Tick       float64
	MinNot     float64
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

type DnSlot struct {
	State      string // "IDLE", "BN_ENTRY_OPEN", "HEDGED", "BN_EXIT_OPEN"
	Spec       DnSpec
	BnSide     string // "BUY" or "SELL"
	LtSide     string // "ASK" or "BID"
	EntryOid   int64
	ExitOid    int64
	Qty        float64
	EntryBnPx  float64
	EntryLtPx  float64
	EntryFx    float64
	EntryFeeBp float64
	TradeID    int64
	OpenedAt   time.Time
	RefLtPx    float64
	LastPoll   time.Time
}

type DnTradeRecord struct {
	ID       int64   `json:"id"`
	OpenTS   int64   `json:"openTs"`
	CloseTS  int64   `json:"closeTs"`
	Strat    string  `json:"strat"`
	Legs     string  `json:"legs"`
	Coin     string  `json:"coin"`
	Usd      float64 `json:"usd"`
	HoldS    float64 `json:"holdS"`
	PnlBps   float64 `json:"pnlBps"`
	PnlUsd   float64 `json:"pnlUsd"`
	BnLegUsd float64 `json:"bnLegUsd"`
	LtLegUsd float64 `json:"ltLegUsd"`
	FeeUsd   float64 `json:"feeUsd"`
	EdgeBps  float64 `json:"edgeBps"`
	Status   string  `json:"status"`
	Reason   string  `json:"reason"`
}

var (
	markets   = map[string]Market{}
	idToSym   = map[int64]string{}
	mktStates = map[string]*MarketState{}

	txCli       *client.TxClient
	nonceSeq    atomic.Int64
	coiSeq      atomic.Int64
	txMu        sync.Mutex
	useWsSend   = true
	cancelQueue = make(chan func(), 256)
	txQueue     = make(chan func(), 256)

	wsMu       sync.Mutex
	ltConn     *websocket.Conn
	subbedMids = map[int64]bool{}
	txWsMu     sync.Mutex
	txConn     *websocket.Conn

	bootTS             = time.Now()
	bootMS             = time.Now().UnixMilli()
	eqNow              atomic.Int64
	remotePaused       atomic.Bool
	breakerTripped     atomic.Bool
	quotaCoolUntil     atomic.Int64
	stratCoolUntilMS   atomic.Int64
	dnFundingActive    atomic.Bool
	dnBusyFlag         atomic.Bool

	dnMu          sync.Mutex
	dnSpecs       []DnSpec
	dnByBnSym     = map[string]DnSpec{}
	dnBooks       = map[string]*DnBook{}
	dnBlockedSym  = map[string]bool{}
	dnLevSet      = map[string]bool{}
	dnFillCh      = make(chan string, 32)
	dnSharedAvail float64
	dnUsdcAvail   float64
	dnUsdtAvail   float64
	dnSlots       = [3]DnSlot{{State: "IDLE"}, {State: "IDLE"}, {State: "IDLE"}}

	histMu        sync.Mutex
	dnTradeHist   []DnTradeRecord
	activeHistPath string

	txHTTP = &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	bnHTTP = &http.Client{
		Timeout: 3500 * time.Millisecond,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
	}
)

func newMarketState() *MarketState {
	return &MarketState{
		bookBids:     map[string]float64{},
		bookAsks:     map[string]float64{},
		activeOrds:   map[string]*ActiveOrder{},
		lastOrdTS:    map[string]time.Time{},
		lastCancelTS: map[string]time.Time{},
	}
}

func roundTo(v float64, dec int) float64 {
	p := math.Pow10(dec)
	return math.Round(v*p) / p
}

func parseAnyFloat(raw json.RawMessage) float64 {
	if len(raw) == 0 {
		return 0
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return f
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
		return v
	}
	return 0
}

func countStepDecimals(s string) int {
	s = strings.TrimRight(s, "0")
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		return len(s) - idx - 1
	}
	return 0
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

// ---------- Persistent Trade Ledger (/opt/crypto-arb-bot/dn_trades_history.json) ----------

func loadDnHistory() {
	histMu.Lock()
	defer histMu.Unlock()
	for _, p := range []string{DnHistPrimary, DnHistBackup} {
		b, err := os.ReadFile(p)
		if err == nil && len(b) > 2 {
			var rows []DnTradeRecord
			if json.Unmarshal(b, &rows) == nil {
				if len(rows) > 200 {
					rows = rows[len(rows)-200:]
				}
				dnTradeHist = rows
				activeHistPath = p
				return
			}
		}
	}
	activeHistPath = DnHistPrimary
}

func saveDnHistoryLocked() {
	if len(dnTradeHist) > 200 {
		dnTradeHist = dnTradeHist[len(dnTradeHist)-200:]
	}
	b, err := json.Marshal(dnTradeHist)
	if err != nil {
		return
	}
	targets := []string{DnHistPrimary, DnHistBackup}
	if activeHistPath == DnHistBackup {
		targets = []string{DnHistBackup}
	}
	for _, p := range targets {
		tmp := p + ".tmp"
		if os.WriteFile(tmp, b, 0644) == nil && os.Rename(tmp, p) == nil {
			activeHistPath = p
			return
		}
	}
}

func recordStratOpen(id int64, strat, legs, coin string, usd, edgeBps float64, detail string) {
	fmt.Printf("[STRAT_OPEN id=%d strat=%s legs=%q usd=$%.2f mult=1.00x edge=%+.2fbp %s]\n",
		id, strat, legs, usd, edgeBps, detail)
	histMu.Lock()
	defer histMu.Unlock()
	rec := DnTradeRecord{
		ID:      id,
		OpenTS:  time.Now().UnixMilli(),
		Strat:   strat,
		Legs:    legs,
		Coin:    coin,
		Usd:     math.Round(usd*100) / 100,
		EdgeBps: math.Round(edgeBps*100) / 100,
		Status:  "OPEN",
		Reason:  detail,
	}
	replaced := false
	for i := range dnTradeHist {
		if dnTradeHist[i].ID == id {
			dnTradeHist[i] = rec
			replaced = true
			break
		}
	}
	if !replaced {
		dnTradeHist = append(dnTradeHist, rec)
	}
	saveDnHistoryLocked()
}

func recordStratClose(id int64, strat, legs, coin string, usd, holdSec, pnlBps, pnlUSD, bnLegUSD, ltLegUSD, feeUSD float64, reason string) {
	fmt.Printf("[STRAT_CLOSE id=%d strat=%s legs=%q usd=$%.2f hold=%.1fs pnl=%+.2fbp($%+.4f) reason=%s]\n",
		id, strat, legs, usd, holdSec, pnlBps, pnlUSD, reason)
	fmt.Printf("[DN_LEGS id=%d coin=%s bn=%+.4f lt=%+.4f fee=$%.4f]\n",
		id, coin, bnLegUSD, ltLegUSD, feeUSD)

	histMu.Lock()
	defer histMu.Unlock()
	nowMS := time.Now().UnixMilli()
	updated := false
	for i := range dnTradeHist {
		if dnTradeHist[i].ID == id {
			dnTradeHist[i].CloseTS = nowMS
			dnTradeHist[i].HoldS = math.Round(holdSec*10) / 10
			dnTradeHist[i].Usd = math.Round(usd*100) / 100
			dnTradeHist[i].PnlBps = math.Round(pnlBps*100) / 100
			dnTradeHist[i].PnlUsd = math.Round(pnlUSD*10000) / 10000
			dnTradeHist[i].BnLegUsd = math.Round(bnLegUSD*10000) / 10000
			dnTradeHist[i].LtLegUsd = math.Round(ltLegUSD*10000) / 10000
			dnTradeHist[i].FeeUsd = math.Round(feeUSD*10000) / 10000
			dnTradeHist[i].Status = "CLOSED"
			dnTradeHist[i].Reason = reason
			updated = true
			break
		}
	}
	if !updated {
		dnTradeHist = append(dnTradeHist, DnTradeRecord{
			ID:       id,
			OpenTS:   nowMS - int64(holdSec*1000),
			CloseTS:  nowMS,
			Strat:    strat,
			Legs:     legs,
			Coin:     coin,
			Usd:      math.Round(usd*100) / 100,
			HoldS:    math.Round(holdSec*10) / 10,
			PnlBps:   math.Round(pnlBps*100) / 100,
			PnlUsd:   math.Round(pnlUSD*10000) / 10000,
			BnLegUsd: math.Round(bnLegUSD*10000) / 10000,
			LtLegUsd: math.Round(ltLegUSD*10000) / 10000,
			FeeUsd:   math.Round(feeUSD*10000) / 10000,
			Status:   "CLOSED",
			Reason:   reason,
		})
	}
	saveDnHistoryLocked()
}

// ---------- Lighter Tx Pipeline ----------

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

func nextOpts() *types.TransactOpts {
	n := nonceSeq.Add(1) - 1
	exp := time.Now().Add(10 * time.Minute).UnixMilli()
	return &types.TransactOpts{Nonce: &n, ExpiredAt: exp}
}

func sendSignedTx(id, dbg string, build func(*types.TransactOpts) (uint8, string, error)) error {
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
		st.mu.Unlock()
	}
}

func cancelSide(sym, side string) {
	st := mktStates[sym]
	if st == nil {
		return
	}
	ao, ok := st.activeOrds[side]
	if !ok {
		return
	}
	now := time.Now()
	st.lastCancelTS[side] = now
	if !ao.IsClose {
		st.lastOrdTS[side] = now
	}
	delete(st.activeOrds, side)
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

func placeLtHedgeIOC(sym, side string, px, qty float64, isClose bool) {
	m, ok := markets[sym]
	st := mktStates[sym]
	if !ok || st == nil || px <= 0 || qty <= 0 {
		return
	}
	if !isClose {
		qty = math.Max(m.MinB, qty)
	}
	baseInt := int64(math.Round(qty * math.Pow10(m.SzDec)))
	if baseInt <= 0 {
		return
	}
	pxF := roundTo(px, m.PxDec)
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
	st.activeOrds[side] = &ActiveOrder{COI: coi, Px: pxF, BaseInt: baseInt, IsClose: isClose, IsIOC: true, TS: now}
	st.mu.Unlock()

	enqueueTx(func() {
		dbg := fmt.Sprintf("LT_%s_%s px=%.4f sz=%.5f ro=%d", sym, side, pxF, qty, redOnly)
		err := sendSignedTx(fmt.Sprintf("lt_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      m.ID,
				ClientOrderIndex: coi,
				BaseAmount:       baseInt,
				Price:            pxInt,
				IsAsk:            isAsk,
				Type:             1, // MARKET
				TimeInForce:      0, // IOC
				ReduceOnly:       redOnly,
				OrderExpiry:      0,
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
			st.mu.Unlock()
		}
	})
}

// ---------- Market Discovery & WebSockets ----------

func discoverCommonAssets() {
	hc := &http.Client{Timeout: 8 * time.Second}
	req, err := http.NewRequest("GET", BaseURL+"/api/v1/orderBookDetails", nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := hc.Do(req)
	if err != nil {
		fmt.Printf("[DISCOVER_ERR] %v\n", err)
		return
	}
	ltB, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

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
	if json.Unmarshal(ltB, &ltRes) != nil {
		fmt.Println("[DISCOVER_PARSE_ERR]")
		return
	}
	cnt := 0
	for _, m := range ltRes.Details {
		if m.Status != "active" {
			continue
		}
		minB := math.Max(1e-6, parseAnyFloat(m.MinB))
		markets[m.Symbol] = Market{
			Sym:   m.Symbol,
			ID:    m.MarketID,
			SzDec: m.SzDec,
			PxDec: m.PxDec,
			MinB:  minB,
		}
		idToSym[m.MarketID] = m.Symbol
		if mktStates[m.Symbol] == nil {
			mktStates[m.Symbol] = newMarketState()
		}
		cnt++
	}
	fmt.Printf("[DISCOVER] loaded %d active Lighter markets dynamically\n", cnt)
}

func autoDiscoverAllCommonDnPairs() {
	hc := &http.Client{Timeout: 8 * time.Second}
	resp, err := hc.Get("https://fapi.binance.com/fapi/v1/exchangeInfo")
	if err != nil {
		fmt.Printf("[DN_DISCOVER_ERR] %v\n", err)
		return
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var ex struct {
		Symbols []struct {
			Symbol     string `json:"symbol"`
			Status     string `json:"status"`
			BaseAsset  string `json:"baseAsset"`
			QuoteAsset string `json:"quoteAsset"`
			Filters    []struct {
				FilterType string `json:"filterType"`
				StepSize   string `json:"stepSize"`
				TickSize   string `json:"tickSize"`
				Notional   string `json:"notional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if json.Unmarshal(b, &ex) != nil {
		fmt.Println("[DN_DISCOVER_PARSE_ERR]")
		return
	}
	var discovered []DnSpec
	byBn := map[string]DnSpec{}
	usdcCnt, usdtCnt := 0, 0
	for _, s := range ex.Symbols {
		if s.Status != "TRADING" || (s.QuoteAsset != "USDC" && s.QuoteAsset != "USDT") {
			continue
		}
		m, ok := markets[s.BaseAsset]
		if !ok {
			continue
		}
		var step, tick, minNot float64 = 0.001, 0.001, 5.5
		qDec, pxDec := 3, 3
		for _, f := range s.Filters {
			switch f.FilterType {
			case "LOT_SIZE":
				step, _ = strconv.ParseFloat(f.StepSize, 64)
				qDec = countStepDecimals(f.StepSize)
			case "PRICE_FILTER":
				tick, _ = strconv.ParseFloat(f.TickSize, 64)
				pxDec = countStepDecimals(f.TickSize)
			case "MIN_NOTIONAL":
				if mn, e2 := strconv.ParseFloat(f.Notional, 64); e2 == nil && mn > 0 {
					minNot = mn + 0.5
				}
			}
		}
		if step <= 0 || tick <= 0 {
			continue
		}
		mFee, tFee := 2.0, 5.0
		if s.QuoteAsset == "USDC" {
			mFee, tFee = 0.0, 4.0
			usdcCnt++
		} else {
			usdtCnt++
		}
		sp := DnSpec{
			Sym:        s.BaseAsset,
			BnSym:      s.Symbol,
			Quote:      s.QuoteAsset,
			MakerFeeBp: mFee,
			TakerFeeBp: tFee,
			QDec:       qDec,
			PxDec:      pxDec,
			Step:       step,
			Tick:       tick,
			MinNot:     minNot,
		}
		discovered = append(discovered, sp)
		byBn[s.Symbol] = sp
		ensureLtSubscribed(m.ID)
	}
	dnMu.Lock()
	dnSpecs = discovered
	dnByBnSym = byBn
	dnMu.Unlock()
	fmt.Printf("[DN_AUTO_DISCOVERED] %d total common contracts (%d ZERO-FEE USDC-M + %d USDT-M) — 0 hardcoded lists!\n",
		len(discovered), usdcCnt, usdtCnt)
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
		dnMu.Lock()
		dnSnap := append([]DnSpec(nil), dnSpecs...)
		dnMu.Unlock()

		wsMu.Lock()
		ltConn = conn
		subbedMids = map[int64]bool{}
		for _, sp := range dnSnap {
			if m, ok := markets[sp.Sym]; ok && !subbedMids[m.ID] {
				_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
				subbedMids[m.ID] = true
			}
		}
		for _, base := range []string{"BTC", "ETH", "SOL"} {
			if m, ok := markets[base]; ok && !subbedMids[m.ID] {
				_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
				subbedMids[m.ID] = true
			}
		}
		wsMu.Unlock()

		for _, st := range mktStates {
			st.mu.Lock()
			st.bookBids = map[string]float64{}
			st.bookAsks = map[string]float64{}
			st.ltBid, st.ltBidSz = 0, 0
			st.ltAsk, st.ltAskSz = 0, 0
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
			if !strings.Contains(sMsg, "order_book") && !strings.Contains(sMsg, "account_all") {
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
							if math.Abs(sz-prev) > 1e-6 {
								st.lastFillTS = time.Now()
								for _, sd := range []string{"BID", "ASK"} {
									if ao := st.activeOrds[sd]; ao != nil {
										if !ao.IsClose || math.Abs(sz) < 1e-6 {
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
						notifyDnFillIfSym(sym)
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
					st.mu.Unlock()
				}
			}
		}
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func onBnAnyBookTick(bnSym, baseSym string, b, a, bq, aq float64) {
	dnMu.Lock()
	_, exists := dnByBnSym[bnSym]
	if !exists && bnSym != "BTCUSDT" && bnSym != "BTCUSDC" {
		dnMu.Unlock()
		return
	}
	dnMu.Unlock()

	st := mktStates[baseSym]
	var ltB, ltA float64
	var ltTS time.Time
	if st != nil {
		st.mu.Lock()
		ltB, ltA, ltTS = st.ltBid, st.ltAsk, st.lastBookTS
		st.mu.Unlock()
	}
	now := time.Now()
	dnMu.Lock()
	db := dnBooks[bnSym]
	if db == nil {
		db = &DnBook{}
		dnBooks[bnSym] = db
	}
	db.Bid, db.Ask, db.BidQ, db.AskQ, db.TS = b, a, bq, aq, now
	if ltB > 0 && ltA > ltB && now.Sub(ltTS) < 8*time.Second {
		ltMid := 0.5 * (ltB + ltA)
		bnMid := 0.5 * (b + a)
		ltSp := (ltA - ltB) / ltMid * 10000.0
		bnSp := (a - b) / bnMid * 10000.0
		rawBps := (ltMid - bnMid) / bnMid * 10000.0
		if ltSp <= 8.0 && bnSp <= 6.0 && (db.WarmTicks == 0 || now.Sub(db.LastEmaTS) >= 800*time.Millisecond) {
			if db.WarmTicks == 0 {
				db.EmaBps = rawBps
			} else if !dnBusyFlag.Load() {
				dt := math.Min(8.0, math.Max(0.2, now.Sub(db.LastEmaTS).Seconds()))
				db.EmaBps += (1.0 - math.Exp(-dt/90.0)) * (rawBps - db.EmaBps)
			}
			db.LastEmaTS = now
			db.WarmTicks++
		}
	}
	dnMu.Unlock()
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
			bq, _ := strconv.ParseFloat(ev.BQ, 64)
			aq, _ := strconv.ParseFloat(ev.AQ, 64)
			if strings.HasSuffix(ev.S, "USDC") {
				onBnAnyBookTick(ev.S, strings.TrimSuffix(ev.S, "USDC"), b, a, bq, aq)
			} else if strings.HasSuffix(ev.S, "USDT") {
				onBnAnyBookTick(ev.S, strings.TrimSuffix(ev.S, "USDT"), b, a, bq, aq)
			}
		}
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func syncAccountLoop() {
	hc := &http.Client{Timeout: 6 * time.Second}
	accURL := fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx)
	trURL := fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=100&sort_by=timestamp", BaseURL, AcctIdx)
	loopCnt := 0
	for {
		loopCnt++
		if loopCnt%2 == 1 {
			if trResp, e := hc.Get(trURL); e == nil {
				tb, _ := io.ReadAll(trResp.Body)
				trResp.Body.Close()
				if len(tb) > 20 && strings.Contains(string(tb), `"trades"`) {
					_ = os.WriteFile("/tmp/lighter_trades.json.tmp", tb, 0644)
					_ = os.Rename("/tmp/lighter_trades.json.tmp", "/tmp/lighter_trades.json")
				}
			}
		}
		if resp, err := hc.Get(accURL); err == nil {
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
				if eq > 0 {
					eqNow.Store(int64(eq * 1e6))
					if eq < StopEqUSD && breakerTripped.CompareAndSwap(false, true) {
						go flattenAndExitPause(fmt.Sprintf("STOP-LOSS Lighter eq=$%.2f < $%.2f", eq, StopEqUSD))
					}
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
						if ae > 0 {
							st.avgEntry = ae
						}
						if math.Abs(sz-prevPos) > 1e-6 {
							st.lastFillTS = time.Now()
						}
						if p.OpenOrderCount == 0 {
							for _, s := range []string{"BID", "ASK"} {
								if ao := st.activeOrds[s]; ao != nil && time.Since(ao.TS) > 2*time.Second {
									delete(st.activeOrds, s)
								}
							}
						}
						st.mu.Unlock()
					}
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
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
	return bb, ba, bd, ad
}

func anyOpenAcctPos() bool {
	for sym, m := range markets {
		if st := mktStates[sym]; st != nil {
			st.mu.Lock()
			p := math.Abs(st.pos)
			st.mu.Unlock()
			if p >= m.MinB {
				return true
			}
		}
	}
	return false
}

// ---------- Pause & Flatten Guards ----------

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
							nowU := time.Now().UTC()
							amzDate := nowU.Format("20060102T150405Z")
							dateStamp := amzDate[:8]
							payloadHash := sha256HexStr("")
							canonHdrs := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\nx-amz-security-token:%s\n",
								S3Host, payloadHash, amzDate, creds.Token)
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
							s3Req, _ := http.NewRequest("GET", "https://"+S3Host+"/pause-control.json", nil)
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
	return exec.Command("aws", "s3", "cp", "s3://"+S3Bucket+"/pause-control.json", "-").Output()
}

func triggerS3Pause(reason string) {
	_ = os.WriteFile(PauseFlagPath, []byte("1"), 0644)
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Get(fmt.Sprintf("https://%s/live-telemetry.json?_t=%d", S3Host, time.Now().UnixMilli()))
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

func flattenAllLtPositionsSync(prefix string) {
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := hc.Get(fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx))
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
		var lb, la float64
		if st := mktStates[sym]; st != nil {
			st.mu.Lock()
			lb, la = st.ltBid, st.ltAsk
			st.mu.Unlock()
		}
		if lb <= 0 || la <= 0 {
			lb, la, _, _ = fetchLtBookREST(m.ID)
		}
		px := la * 1.015
		var isAsk uint8
		if sz > 0 {
			isAsk = 1
			px = lb * 0.985
		}
		if px > 0 {
			_ = sendSignedTx(prefix+sym, sym, func(opts *types.TransactOpts) (uint8, string, error) {
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
			fmt.Printf("[LT_FLAT_SYNC sym=%s sz=%.5f px=%.4f]\n", sym, sz, px)
		}
	}
}

func flattenAndExitPause(reason string) {
	if !remotePaused.CompareAndSwap(false, true) {
		return
	}
	fmt.Printf("[INSTANT-PAUSE] %s — canceling orders & flattening BOTH venues!\n", reason)
	if strings.HasPrefix(reason, "STOP-LOSS") {
		triggerS3Pause(reason)
	}
	useWsSend = false
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		cancelAll()
		flattenAllLtPositionsSync("pause_flat_")
	}()
	go func() {
		defer wg.Done()
		flattenBnDnAndSweep()
	}()
	wg.Wait()
	emitBnAuditLine()
	time.Sleep(150 * time.Millisecond)
	os.Exit(0)
}

func runPauseWatchdog() {
	hc := &http.Client{Timeout: 1500 * time.Millisecond}
	s3Ticks := 0
	for {
		if _, err := os.Stat(PauseFlagPath); err == nil {
			flattenAndExitPause(PauseFlagPath)
			return
		}
		s3Ticks++
		if s3Ticks%5 == 0 && time.Since(bootTS) >= 45*time.Second {
			if resp, err := hc.Get(fmt.Sprintf("https://%s/live-telemetry.json?t=%d", S3Host, time.Now().UnixMilli())); err == nil {
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var tel struct {
					TS int64 `json:"ts"`
				}
				if json.Unmarshal(b, &tel) == nil && tel.TS > bootMS+5000 && strings.Contains(string(b), `"userPaused":true`) {
					flattenAndExitPause("S3 userPaused=true")
					return
				}
			}
		}
		time.Sleep(400 * time.Millisecond)
	}
}

// ---------- Binance Futures Helpers & Multi-Assets Margin ----------

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

func ensureBnLeverage(bnSym string) {
	dnMu.Lock()
	already := dnLevSet[bnSym]
	if !already {
		dnLevSet[bnSym] = true
	}
	dnMu.Unlock()
	if !already {
		lvP := url.Values{}
		lvP.Set("symbol", bnSym)
		lvP.Set("leverage", "5")
		_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/leverage", lvP)
	}
}

func fetchBnAccountMargin() (sharedAvail, usdcAvail, usdtAvail float64, ok bool) {
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/account", nil)
	if err != nil || sc != 200 {
		return 0, 0, 0, false
	}
	var acc struct {
		AvailableBalance string `json:"availableBalance"`
		Assets           []struct {
			Asset            string `json:"asset"`
			AvailableBalance string `json:"availableBalance"`
			WalletBalance    string `json:"walletBalance"`
		} `json:"assets"`
	}
	if json.Unmarshal(body, &acc) != nil {
		return 0, 0, 0, false
	}
	sharedAvail, _ = strconv.ParseFloat(acc.AvailableBalance, 64)
	for _, a := range acc.Assets {
		wb, _ := strconv.ParseFloat(a.WalletBalance, 64)
		if a.Asset == "USDC" {
			usdcAvail = wb
		} else if a.Asset == "USDT" {
			usdtAvail = wb
		}
	}
	dnMu.Lock()
	dnSharedAvail = sharedAvail
	dnUsdcAvail = usdcAvail
	dnUsdtAvail = usdtAvail
	dnMu.Unlock()
	return sharedAvail, usdcAvail, usdtAvail, true
}

func setupBnUsdcMargin() bool {
	if ooSc, ooB, ooE := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/openOrders", nil); ooE == nil && ooSc == 200 {
		var oos []struct {
			Symbol string `json:"symbol"`
		}
		if json.Unmarshal(ooB, &oos) == nil {
			canceledSyms := map[string]bool{}
			for _, oo := range oos {
				if oo.Symbol != "" && !canceledSyms[oo.Symbol] {
					canceledSyms[oo.Symbol] = true
					cp := url.Values{}
					cp.Set("symbol", oo.Symbol)
					_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", cp)
				}
			}
		}
	}
	pmP := url.Values{}
	pmP.Set("dualSidePosition", "false")
	_, _, _ = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/positionSide/dual", pmP)
	maP := url.Values{}
	maP.Set("multiAssetsMargin", "true")
	maSc, maB, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/multiAssetsMargin", maP)
	fmt.Printf("[DN_MULTI_ASSET] status=%d body=%s\n", maSc, string(maB))

	sharedAvail, usdcBal, usdtBal, ok := fetchBnAccountMargin()
	if !ok {
		fmt.Println("[DN_BOOT_ERR] /fapi/v2/account failed")
		return false
	}
	fmt.Printf("[DN_READY] Binance Multi-Asset sharedAvail=$%.2f (USDC wallet=$%.2f USDT wallet=$%.2f) across %d contracts!\n",
		sharedAvail, usdcBal, usdtBal, len(dnSpecs))
	return sharedAvail >= 20.0
}

func emitBnAuditLine() {
	nowU := time.Now().UTC()
	ip := url.Values{}
	ip.Set("startTime", strconv.FormatInt(nowU.Add(-72*time.Hour).UnixMilli(), 10))
	ip.Set("limit", "1000")
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/income", ip)
	var realPnl, comm, fund float64
	var cnt int
	if err == nil && sc == 200 {
		var rows []struct {
			Type   string `json:"incomeType"`
			Income string `json:"income"`
		}
		if json.Unmarshal(body, &rows) == nil {
			for _, r := range rows {
				v, _ := strconv.ParseFloat(r.Income, 64)
				switch r.Type {
				case "REALIZED_PNL":
					realPnl += v
					cnt++
				case "COMMISSION":
					comm += v
				case "FUNDING_FEE":
					fund += v
				}
			}
		}
	}
	var upnl float64
	if sc2, b2, e2 := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/account", nil); e2 == nil && sc2 == 200 {
		var acc struct {
			TotalUnPnl string `json:"totalUnrealizedProfit"`
		}
		if json.Unmarshal(b2, &acc) == nil {
			upnl, _ = strconv.ParseFloat(acc.TotalUnPnl, 64)
		}
	}
	var walletUsd float64
	wp := url.Values{}
	wp.Set("quoteAsset", "USDT")
	if sc3, b3, e3 := bnSignReq("GET", "api.binance.com", "/sapi/v1/asset/wallet/balance", wp); e3 == nil && sc3 == 200 {
		var wrows []struct {
			Activate bool   `json:"activate"`
			Balance  string `json:"balance"`
		}
		if json.Unmarshal(b3, &wrows) == nil {
			for _, wr := range wrows {
				if wr.Activate {
					v, _ := strconv.ParseFloat(wr.Balance, 64)
					walletUsd += v
				}
			}
		}
	}
	netBn := realPnl + comm + fund + upnl
	fmt.Printf("[BN_AUDIT real=%+.4f fee=%+.4f fund=%+.4f upnl=%+.4f net=%+.4f trades=%d wallet=%.4f]\n",
		realPnl, comm, fund, upnl, netBn, cnt, walletUsd)
}

func flattenBnDnAndSweep() {
	dnMu.Lock()
	var activeSyms []string
	for i := range dnSlots {
		if dnSlots[i].Spec.BnSym != "" {
			activeSyms = append(activeSyms, dnSlots[i].Spec.BnSym)
		}
		dnSlots[i] = DnSlot{State: "IDLE"}
	}
	dnMu.Unlock()
	dnFundingActive.Store(false)
	dnBusyFlag.Store(false)

	var oWg sync.WaitGroup
	canceledMap := map[string]bool{}
	for _, sym := range activeSyms {
		canceledMap[sym] = true
		oWg.Add(1)
		go func(s string) {
			defer oWg.Done()
			p := url.Values{}
			p.Set("symbol", s)
			_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", p)
		}(sym)
	}
	if ooSc, ooB, ooE := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/openOrders", nil); ooE == nil && ooSc == 200 {
		var oos []struct {
			Symbol string `json:"symbol"`
		}
		if json.Unmarshal(ooB, &oos) == nil {
			for _, oo := range oos {
				if oo.Symbol != "" && !canceledMap[oo.Symbol] {
					canceledMap[oo.Symbol] = true
					oWg.Add(1)
					go func(s string) {
						defer oWg.Done()
						cp := url.Values{}
						cp.Set("symbol", s)
						_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", cp)
					}(oo.Symbol)
				}
			}
		}
	}
	oWg.Wait()

	if sc, b, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/positionRisk", nil); err == nil && sc == 200 {
		var prs []struct {
			Symbol string `json:"symbol"`
			Amt    string `json:"positionAmt"`
		}
		if json.Unmarshal(b, &prs) == nil {
			var pWg sync.WaitGroup
			for _, pr := range prs {
				amt, _ := strconv.ParseFloat(pr.Amt, 64)
				if math.Abs(amt) > 1e-6 {
					pWg.Add(1)
					go func(sym, amtStr string, aVal float64) {
						defer pWg.Done()
						if !canceledMap[sym] {
							p := url.Values{}
							p.Set("symbol", sym)
							_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", p)
						}
						cSide := "SELL"
						if aVal < 0 {
							cSide = "BUY"
						}
						rawQ := strings.TrimPrefix(strings.TrimSpace(amtStr), "-")
						if strings.Contains(rawQ, ".") {
							rawQ = strings.TrimRight(rawQ, "0")
							rawQ = strings.TrimRight(rawQ, ".")
						}
						cp := url.Values{}
						cp.Set("symbol", sym)
						cp.Set("side", cSide)
						cp.Set("type", "MARKET")
						cp.Set("reduceOnly", "true")
						cp.Set("quantity", rawQ)
						fSc, fB, fE := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", cp)
						if fE != nil || fSc != 200 {
							dnMu.Lock()
							sp, ok := dnByBnSym[sym]
							dnMu.Unlock()
							if ok {
								cp.Set("quantity", strconv.FormatFloat(math.Abs(aVal), 'f', sp.QDec, 64))
								fSc, fB, fE = bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", cp)
							}
						}
						fmt.Printf("[BN_FLAT_SYNC sym=%s side=%s qty=%s sc=%d err=%v body=%s]\n",
							sym, cSide, cp.Get("quantity"), fSc, fE, string(fB))
					}(pr.Symbol, pr.Amt, amt)
				}
			}
			pWg.Wait()
		}
	}
}

// ---------- Core 3-Slot 0%-Fee USDC-M Engine + Hourly Funding Snipe ----------

func dnSlotBusyCountLocked() int {
	cnt := 0
	for _, s := range dnSlots {
		if s.State != "" && s.State != "IDLE" {
			cnt++
		}
	}
	return cnt
}

func dnSymActiveLocked(sym string) bool {
	for _, s := range dnSlots {
		if s.State != "" && s.State != "IDLE" && s.Spec.Sym == sym {
			return true
		}
	}
	return false
}

func idleDnSlotIdxLocked() int {
	for i, s := range dnSlots {
		if s.State == "" || s.State == "IDLE" {
			return i
		}
	}
	return -1
}

func notifyDnFillIfSym(sym string) {
	dnMu.Lock()
	active := dnSymActiveLocked(sym)
	dnMu.Unlock()
	if active {
		select {
		case dnFillCh <- sym:
		default:
		}
	}
}

func dnQuoteFxToUsdt(quote string) (float64, bool) {
	if quote != "USDC" {
		return 1.0, true
	}
	dnMu.Lock()
	bu := dnBooks["BTCUSDT"]
	bc := dnBooks["BTCUSDC"]
	var uMid, cMid float64
	var uTS, cTS time.Time
	if bu != nil && bu.Bid > 0 && bu.Ask >= bu.Bid {
		uMid, uTS = 0.5*(bu.Bid+bu.Ask), bu.TS
	}
	if bc != nil && bc.Bid > 0 && bc.Ask >= bc.Bid {
		cMid, cTS = 0.5*(bc.Bid+bc.Ask), bc.TS
	}
	dnMu.Unlock()
	if uMid <= 0 || cMid <= 0 || time.Since(uTS) > 4*time.Second || time.Since(cTS) > 4*time.Second {
		return 0, false
	}
	fx := uMid / cMid
	if fx < 0.98 || fx > 1.02 {
		return 0, false
	}
	return fx, true
}

func computeDnLegPnls(bnSide string, entryBn, exitBn, entryLt, exitLt, fx, qty, entryFeeBp, exitFeeBp float64) (bnLegUsd, ltLegUsd, feeUsd, netUsd, netBps float64) {
	eUsd := entryBn * fx
	notional := qty * eUsd
	if notional <= 0 {
		return 0, 0, 0, 0, 0
	}
	feeUsd = notional * ((entryFeeBp + exitFeeBp) / 10000.0)
	if bnSide == "BUY" {
		bnLegUsd = (exitBn-entryBn)*fx*qty - feeUsd
		ltLegUsd = (entryLt - exitLt) * qty
	} else {
		bnLegUsd = (entryBn-exitBn)*fx*qty - feeUsd
		ltLegUsd = (exitLt - entryLt) * qty
	}
	netUsd = bnLegUsd + ltLegUsd
	netBps = netUsd / notional * 10000.0
	return bnLegUsd, ltLegUsd, feeUsd, netUsd, netBps
}

func closeDnSlot(idx int, sl DnSlot, bnExPx, ltPos, lb, la, fx, exitFeeBp float64, reason string, now time.Time) {
	actSp := sl.Spec
	m := markets[actSp.Sym]
	closeLtSide, ltClosePx, ltExitEst := "BID", roundTo(la*1.004, m.PxDec), la
	if sl.BnSide == "SELL" {
		closeLtSide, ltClosePx, ltExitEst = "ASK", roundTo(lb*0.996, m.PxDec), lb
	}
	closeQty := math.Abs(ltPos)
	if closeQty < m.MinB {
		closeQty = sl.Qty
	}
	if closeQty >= m.MinB {
		placeLtHedgeIOC(actSp.Sym, closeLtSide, ltClosePx, closeQty, true)
		time.Sleep(80 * time.Millisecond)
	}
	eLt := sl.EntryLtPx
	if eLt <= 0 {
		eLt = ltExitEst
	}
	bnLeg, ltLeg, feeUsd, realUSD, realBps := computeDnLegPnls(
		sl.BnSide, sl.EntryBnPx, bnExPx, eLt, ltExitEst, fx, sl.Qty, sl.EntryFeeBp, exitFeeBp)
	eUsd := sl.EntryBnPx * fx
	recordStratClose(sl.TradeID, "ZERO_FEE_DN",
		fmt.Sprintf("BN_%s_%s+LT_%s", actSp.BnSym, sl.BnSide, sl.LtSide),
		actSp.Sym, sl.Qty*eUsd, now.Sub(sl.OpenedAt).Seconds(),
		realBps, realUSD, bnLeg, ltLeg, feeUsd, reason)
	emitBnAuditLine()
	dnMu.Lock()
	dnSlots[idx] = DnSlot{State: "IDLE"}
	dnBusyFlag.Store(dnSlotBusyCountLocked() > 0 || dnFundingActive.Load())
	dnMu.Unlock()
}

func processActiveDnSlot(idx int, now time.Time) {
	dnMu.Lock()
	sl := dnSlots[idx]
	dnMu.Unlock()
	if sl.State == "" || sl.State == "IDLE" {
		return
	}
	actSp := sl.Spec
	m := markets[actSp.Sym]
	st := mktStates[actSp.Sym]
	if st == nil {
		return
	}
	st.mu.Lock()
	lb, la, ltPos, ltAvg := st.ltBid, st.ltAsk, st.pos, st.avgEntry
	st.mu.Unlock()

	eFx := sl.EntryFx
	if eFx <= 0 {
		eFx = 1.0
	}
	fx, okFx := dnQuoteFxToUsdt(actSp.Quote)
	if !okFx || fx <= 0 {
		fx = eFx
	}
	rtMakerFeeBp := sl.EntryFeeBp + actSp.MakerFeeBp
	rtMixedFeeBp := sl.EntryFeeBp + actSp.TakerFeeBp

	if sl.State == "BN_ENTRY_OPEN" {
		eUsd := sl.EntryBnPx * fx
		curNetEdge := (lb-eUsd)/eUsd*10000.0 - rtMakerFeeBp
		ltMoved := lb < sl.RefLtPx*(1.0-0.6/10000.0)
		if sl.BnSide == "SELL" {
			curNetEdge = (eUsd-la)/eUsd*10000.0 - rtMakerFeeBp
			ltMoved = la > sl.RefLtPx*(1.0+0.6/10000.0)
		}
		needCancel := ltMoved || curNetEdge < 1.20 || now.Sub(sl.OpenedAt) >= 8*time.Second
		if !needCancel && now.Sub(sl.LastPoll) < 90*time.Millisecond {
			return
		}
		dnMu.Lock()
		dnSlots[idx].LastPoll = now
		dnMu.Unlock()
		qp := url.Values{}
		qp.Set("symbol", actSp.BnSym)
		qp.Set("orderId", strconv.FormatInt(sl.EntryOid, 10))
		sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp)
		if err != nil || sc != 200 {
			return
		}
		var o struct {
			Status  string `json:"status"`
			ExecQty string `json:"executedQty"`
			AvgPx   string `json:"avgPrice"`
		}
		if json.Unmarshal(body, &o) != nil {
			return
		}
		exQ, _ := strconv.ParseFloat(o.ExecQty, 64)
		avgP, _ := strconv.ParseFloat(o.AvgPx, 64)
		if avgP <= 0 {
			avgP = sl.EntryBnPx
		}
		if o.Status == "FILLED" || (needCancel && exQ*(avgP*fx) >= actSp.MinNot && exQ >= m.MinB) {
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
			ltIocPx, ltRefFill := roundTo(lb*0.996, m.PxDec), lb
			if sl.LtSide == "BID" {
				ltIocPx, ltRefFill = roundTo(la*1.004, m.PxDec), la
			}
			placeLtHedgeIOC(actSp.Sym, sl.LtSide, ltIocPx, exQ, false)
			time.Sleep(80 * time.Millisecond)
			st.mu.Lock()
			if st.avgEntry > 0 {
				ltRefFill = st.avgEntry
			}
			st.mu.Unlock()
			avgUsd := avgP * fx
			lockedBps := (ltRefFill-avgUsd)/avgUsd*10000.0 - rtMakerFeeBp
			if sl.BnSide == "SELL" {
				lockedBps = (avgUsd-ltRefFill)/avgUsd*10000.0 - rtMakerFeeBp
			}
			dnMu.Lock()
			dnSlots[idx].State = "HEDGED"
			dnSlots[idx].Qty = exQ
			dnSlots[idx].EntryBnPx = avgP
			dnSlots[idx].EntryLtPx = ltRefFill
			dnSlots[idx].EntryFx = fx
			dnSlots[idx].OpenedAt = time.Now()
			dnMu.Unlock()
			recordStratOpen(sl.TradeID, "ZERO_FEE_DN",
				fmt.Sprintf("BN_%s_%s+LT_%s", actSp.BnSym, sl.BnSide, sl.LtSide),
				actSp.Sym, exQ*avgUsd, lockedBps,
				fmt.Sprintf("Slot%d 0pctFee Fx%.5f BnMaker@%.5f+Lt0pct@%.5f", idx+1, fx, avgP, ltRefFill))
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
			if exQ >= m.MinB {
				tMid := 0.5 * (lb + la)
				ltIocPx := roundTo(lb*0.996, m.PxDec)
				if sl.LtSide == "BID" {
					ltIocPx = roundTo(la*1.004, m.PxDec)
				}
				placeLtHedgeIOC(actSp.Sym, sl.LtSide, ltIocPx, exQ, false)
				dnMu.Lock()
				dnSlots[idx].State = "HEDGED"
				dnSlots[idx].Qty = exQ
				dnSlots[idx].EntryBnPx = avgP
				dnSlots[idx].EntryLtPx = tMid
				dnSlots[idx].EntryFx = fx
				dnSlots[idx].OpenedAt = time.Now()
				dnMu.Unlock()
				recordStratOpen(sl.TradeID, "ZERO_FEE_DN",
					fmt.Sprintf("BN_%s_%s+LT_%s", actSp.BnSym, sl.BnSide, sl.LtSide),
					actSp.Sym, exQ*(avgP*fx), curNetEdge, fmt.Sprintf("Slot%d Partial_Hedged", idx+1))
			} else {
				dnMu.Lock()
				dnSlots[idx] = DnSlot{State: "IDLE"}
				dnBusyFlag.Store(dnSlotBusyCountLocked() > 0 || dnFundingActive.Load())
				dnMu.Unlock()
			}
		}
		return
	}

	eLt := sl.EntryLtPx
	if ltAvg > 0 {
		eLt = ltAvg
		sl.EntryLtPx = ltAvg
	}
	eUsd := sl.EntryBnPx * eFx

	if sl.State == "HEDGED" {
		dnMu.Lock()
		db := dnBooks[actSp.BnSym]
		var bb, ba float64
		if db != nil {
			bb, ba = db.Bid, db.Ask
		}
		dnMu.Unlock()
		if bb <= 0 || ba <= bb || lb <= 0 || la <= lb {
			return
		}
		var closeBnSide string
		var makerExitBnPx, takerExitLtPx, combNetMakerBps, combTakerNetBps float64
		if sl.BnSide == "BUY" {
			closeBnSide = "SELL"
			makerExitBnPx = ba
			if ba-bb >= actSp.Tick*1.9 {
				makerExitBnPx = roundTo(bb+actSp.Tick, actSp.PxDec)
			}
			takerExitLtPx = la
			combNetMakerBps = (((makerExitBnPx-sl.EntryBnPx)*fx)+(eLt-takerExitLtPx))/eUsd*10000.0 - rtMakerFeeBp
			combTakerNetBps = (((bb-sl.EntryBnPx)*fx)+(eLt-la))/eUsd*10000.0 - rtMixedFeeBp
		} else {
			closeBnSide = "BUY"
			makerExitBnPx = bb
			if ba-bb >= actSp.Tick*1.9 {
				makerExitBnPx = roundTo(ba-actSp.Tick, actSp.PxDec)
			}
			takerExitLtPx = lb
			combNetMakerBps = (((sl.EntryBnPx-makerExitBnPx)*fx)+(takerExitLtPx-eLt))/eUsd*10000.0 - rtMakerFeeBp
			combTakerNetBps = (((sl.EntryBnPx-ba)*fx)+(lb-eLt))/eUsd*10000.0 - rtMixedFeeBp
		}
		holdS := now.Sub(sl.OpenedAt).Seconds()
		utcNow := now.UTC()
		preHourFlush := (utcNow.Minute() == 58 && utcNow.Second() >= 45) || utcNow.Minute() == 59
		// Pure 0% Maker convergence exit (NO 20s/-14bp taker SL); only catastrophic -45bp / pre-hour / 10m backstop uses market close!
		wantExit := combNetMakerBps >= 1.80 ||
			(holdS >= 45.0 && combNetMakerBps >= 0.60) ||
			(holdS >= 150.0 && combNetMakerBps >= 0.10) ||
			(holdS >= 360.0 && combNetMakerBps >= -1.50)
		catastrophicSL := holdS >= 30.0 && combTakerNetBps <= -45.0
		emergencyExit := preHourFlush || catastrophicSL || holdS >= 600.0

		if emergencyExit {
			mp := url.Values{}
			mp.Set("symbol", actSp.BnSym)
			mp.Set("side", closeBnSide)
			mp.Set("type", "MARKET")
			mp.Set("reduceOnly", "true")
			mp.Set("quantity", strconv.FormatFloat(sl.Qty, 'f', actSp.QDec, 64))
			_, mb, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
			var mo struct {
				AvgPx string `json:"avgPrice"`
			}
			_ = json.Unmarshal(mb, &mo)
			bnExPx, _ := strconv.ParseFloat(mo.AvgPx, 64)
			if bnExPx <= 0 {
				bnExPx = 0.5 * (bb + ba)
			}
			exReason := "DN_CATASTROPHIC_SL"
			if preHourFlush {
				exReason = "PRE_HOUR_FUND_GUARD"
			} else if holdS >= 600.0 {
				exReason = "DN_MAX_HOLD_10M"
			}
			closeDnSlot(idx, sl, bnExPx, ltPos, lb, la, fx, actSp.TakerFeeBp, exReason, now)
			stratCoolUntilMS.Store(now.Add(8 * time.Second).UnixMilli())
		} else if wantExit {
			p := url.Values{}
			p.Set("symbol", actSp.BnSym)
			p.Set("side", closeBnSide)
			p.Set("type", "LIMIT")
			p.Set("timeInForce", "GTX")
			p.Set("reduceOnly", "true")
			p.Set("quantity", strconv.FormatFloat(sl.Qty, 'f', actSp.QDec, 64))
			p.Set("price", strconv.FormatFloat(makerExitBnPx, 'f', actSp.PxDec, 64))
			sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
			if err == nil && sc == 200 {
				var or struct {
					OrderID int64  `json:"orderId"`
					Status  string `json:"status"`
				}
				if json.Unmarshal(body, &or) == nil && or.OrderID > 0 && or.Status != "EXPIRED" && or.Status != "CANCELED" {
					dnMu.Lock()
					dnSlots[idx].State = "BN_EXIT_OPEN"
					dnSlots[idx].ExitOid = or.OrderID
					dnSlots[idx].RefLtPx = takerExitLtPx
					dnSlots[idx].LastPoll = time.Time{}
					dnMu.Unlock()
					fmt.Printf("[DN_EXIT_RESTED id=%d slot=%d sym=%s side=%s @ %.5f estNet=%+.2fbp fee=$0.00]\n",
						sl.TradeID, idx+1, actSp.BnSym, closeBnSide, makerExitBnPx, combNetMakerBps)
				}
			}
		}
		return
	}

	if sl.State == "BN_EXIT_OPEN" {
		if now.Sub(sl.LastPoll) < 90*time.Millisecond {
			return
		}
		dnMu.Lock()
		dnSlots[idx].LastPoll = now
		dnMu.Unlock()
		qp := url.Values{}
		qp.Set("symbol", actSp.BnSym)
		qp.Set("orderId", strconv.FormatInt(sl.ExitOid, 10))
		sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp)
		if err != nil || sc != 200 {
			return
		}
		var o struct {
			Status string `json:"status"`
			AvgPx  string `json:"avgPrice"`
			Price  string `json:"price"`
		}
		if json.Unmarshal(body, &o) != nil {
			return
		}
		exP, _ := strconv.ParseFloat(o.AvgPx, 64)
		if exP <= 0 {
			exP, _ = strconv.ParseFloat(o.Price, 64)
		}
		if o.Status == "FILLED" {
			closeDnSlot(idx, sl, exP, ltPos, lb, la, fx, actSp.MakerFeeBp, "ZERO_FEE_4LEG_TP", now)
			stratCoolUntilMS.Store(now.Add(2 * time.Second).UnixMilli())
		} else if o.Status == "CANCELED" || o.Status == "EXPIRED" {
			dnMu.Lock()
			dnSlots[idx].State = "HEDGED"
			dnMu.Unlock()
		} else {
			curComb := (((exP-sl.EntryBnPx)*fx)+(eLt-la))/eUsd*10000.0 - rtMakerFeeBp
			if sl.BnSide == "SELL" {
				curComb = (((sl.EntryBnPx-exP)*fx)+(lb-eLt))/eUsd*10000.0 - rtMakerFeeBp
			}
			utcNow := now.UTC()
			preHourCancel := (utcNow.Minute() == 58 && utcNow.Second() >= 45) || utcNow.Minute() == 59
			if preHourCancel || curComb < -0.50 {
				_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
				var co struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal(cb, &co)
				if co.Status == "FILLED" {
					closeDnSlot(idx, sl, exP, ltPos, lb, la, fx, actSp.MakerFeeBp, "ZERO_FEE_4LEG_TP", now)
					stratCoolUntilMS.Store(now.Add(2 * time.Second).UnixMilli())
				} else {
					dnMu.Lock()
					dnSlots[idx].State = "HEDGED"
					dnMu.Unlock()
				}
			}
		}
	}
}

func scanAndOpenDnSlot(idleIdx int, now time.Time, sharedAvail float64, specsSnap []DnSpec, lastScanLog *time.Time) {
	if sharedAvail < 20.0 {
		return
	}
	var bestSp DnSpec
	bestBnSide, bestLtSide := "", ""
	bestNetEdge, bestBnPx, bestLtPx, bestFx := -999.0, 0.0, 0.0, 1.0

	for _, sp := range specsSnap {
		if sp.Quote != "USDC" {
			continue
		}
		fx, okFx := dnQuoteFxToUsdt(sp.Quote)
		if !okFx {
			continue
		}
		dnMu.Lock()
		blocked := dnBlockedSym[sp.BnSym]
		symActive := dnSymActiveLocked(sp.Sym)
		db := dnBooks[sp.BnSym]
		var bb, ba float64
		var wTicks int
		var bTS time.Time
		if db != nil {
			bb, ba, wTicks, bTS = db.Bid, db.Ask, db.WarmTicks, db.TS
		}
		dnMu.Unlock()
		if blocked || symActive || bb <= 0 || ba <= bb || wTicks < 2 || now.Sub(bTS) > 4*time.Second {
			continue
		}
		m, okM := markets[sp.Sym]
		st := mktStates[sp.Sym]
		if !okM || st == nil {
			continue
		}
		st.mu.Lock()
		lb, la, lbSz, laSz, ltPos, lOrds, lTS := st.ltBid, st.ltAsk, st.ltBidSz, st.ltAskSz, st.pos, len(st.activeOrds), st.lastBookTS
		st.mu.Unlock()
		if math.Abs(ltPos) >= m.MinB || lOrds > 0 || lb <= 0 || la <= lb || now.Sub(lTS) > 5*time.Second {
			continue
		}
		ltSp := (la - lb) / lb * 10000.0
		bnSp := (ba - bb) / bb * 10000.0
		if ltSp > 5.0 || bnSp > 3.5 {
			continue
		}

		bnBuyPx := bb
		if ba-bb >= sp.Tick*1.9 {
			bnBuyPx = roundTo(ba-sp.Tick, sp.PxDec)
		}
		bnBuyUsd := bnBuyPx * fx
		netEdgeA := (lb - bnBuyUsd) / bnBuyUsd * 10000.0
		if netEdgeA > bestNetEdge && lb*lbSz >= 120.0 {
			bestNetEdge, bestSp, bestBnSide, bestLtSide, bestBnPx, bestLtPx, bestFx =
				netEdgeA, sp, "BUY", "ASK", bnBuyPx, lb, fx
		}

		bnSellPx := ba
		if ba-bb >= sp.Tick*1.9 {
			bnSellPx = roundTo(bb+sp.Tick, sp.PxDec)
		}
		bnSellUsd := bnSellPx * fx
		netEdgeB := (bnSellUsd - la) / bnSellUsd * 10000.0
		if netEdgeB > bestNetEdge && la*laSz >= 120.0 {
			bestNetEdge, bestSp, bestBnSide, bestLtSide, bestBnPx, bestLtPx, bestFx =
				netEdgeB, sp, "SELL", "BID", bnSellPx, la, fx
		}
	}

	if now.Sub(*lastScanLog) >= 12*time.Second && bestSp.BnSym != "" {
		*lastScanLog = now
		fmt.Printf("[DN_SCAN] best=%s bnMaker=%s->ltZeroFee=%s netAfterFee=%+.2fbp (fx=%.5f fee=0.0bp clip=$55) bnPx=%.5f ltPx=%.5f\n",
			bestSp.BnSym, bestBnSide, bestLtSide, bestNetEdge, bestFx, bestBnPx, bestLtPx)
	}
	if bestSp.BnSym == "" || bestNetEdge < 2.50 {
		return
	}
	m := markets[bestSp.Sym]
	ensureLtSubscribed(m.ID)
	ensureBnLeverage(bestSp.BnSym)
	commonStep := math.Max(bestSp.Step, math.Pow10(-m.SzDec))
	rawQty := math.Floor((55.0/(bestBnPx*bestFx))/commonStep+1e-9) * commonStep
	qtyDec := bestSp.QDec
	if m.SzDec < qtyDec {
		qtyDec = m.SzDec
	}
	qty := roundTo(rawQty, qtyDec)
	if qty*(bestBnPx*bestFx) < math.Max(12.0, bestSp.MinNot) || qty < m.MinB {
		return
	}
	p := url.Values{}
	p.Set("symbol", bestSp.BnSym)
	p.Set("side", bestBnSide)
	p.Set("type", "LIMIT")
	p.Set("timeInForce", "GTX")
	p.Set("quantity", strconv.FormatFloat(qty, 'f', bestSp.QDec, 64))
	p.Set("price", strconv.FormatFloat(bestBnPx, 'f', bestSp.PxDec, 64))
	sc, body, err := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", p)
	if err == nil && sc == 200 {
		var or struct {
			OrderID int64  `json:"orderId"`
			Status  string `json:"status"`
		}
		if json.Unmarshal(body, &or) == nil && or.OrderID > 0 && or.Status != "EXPIRED" && or.Status != "CANCELED" {
			tid := now.UnixMilli()
			dnMu.Lock()
			dnSlots[idleIdx] = DnSlot{
				State: "BN_ENTRY_OPEN", Spec: bestSp, BnSide: bestBnSide, LtSide: bestLtSide,
				EntryOid: or.OrderID, Qty: qty, EntryBnPx: bestBnPx, EntryFx: bestFx,
				EntryFeeBp: bestSp.MakerFeeBp, TradeID: tid, OpenedAt: now, RefLtPx: bestLtPx,
			}
			dnBusyFlag.Store(true)
			dnMu.Unlock()
			fmt.Printf("[DN_ZERO_FEE_MAKER id=%d slot=%d sym=%s bn=%s qty=%.4f @ %.5f netEdge=%+.2fbp fx=%.5f fee=0.0bp]\n",
				tid, idleIdx+1, bestSp.BnSym, bestBnSide, qty, bestBnPx, bestNetEdge, bestFx)
		}
	} else if strings.Contains(string(body), "-4411") {
		dnMu.Lock()
		dnBlockedSym[bestSp.BnSym] = true
		dnMu.Unlock()
	}
}

func runHourlyFundingSnipe() {
	var snipedHour int64 = -1
	for {
		time.Sleep(2 * time.Second)
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now().UTC()
		curHourID := now.Unix() / 3600
		minInHr := now.Minute()
		secInMin := now.Second()
		if minInHr < 51 || (minInHr == 58 && secInMin > 45) || minInHr == 59 || snipedHour == curHourID {
			continue
		}
		if anyOpenAcctPos() || dnBusyFlag.Load() {
			continue
		}
		nextHr := (now.Hour() + 1) % 24
		hc := &http.Client{Timeout: 4 * time.Second}
		prResp, e1 := hc.Get("https://fapi.binance.com/fapi/v1/premiumIndex")
		fiResp, e2 := hc.Get("https://fapi.binance.com/fapi/v1/fundingInfo")
		frResp, e3 := hc.Get(BaseURL + "/api/v1/funding-rates")
		if e1 != nil || e2 != nil || e3 != nil {
			continue
		}
		prB, _ := io.ReadAll(prResp.Body)
		prResp.Body.Close()
		fiB, _ := io.ReadAll(fiResp.Body)
		fiResp.Body.Close()
		frB, _ := io.ReadAll(frResp.Body)
		frResp.Body.Close()

		var prs []struct {
			Symbol string `json:"symbol"`
			Rate   string `json:"lastFundingRate"`
		}
		var fis []struct {
			Symbol string `json:"symbol"`
			Hours  int    `json:"fundingIntervalHours"`
		}
		var fr struct {
			Rates []struct {
				Sym  string  `json:"symbol"`
				Exch string  `json:"exchange"`
				Rate float64 `json:"rate"`
			} `json:"funding_rates"`
		}
		if json.Unmarshal(prB, &prs) != nil || json.Unmarshal(fiB, &fis) != nil || json.Unmarshal(frB, &fr) != nil {
			continue
		}
		bnRate := map[string]float64{}
		for _, p := range prs {
			r, _ := strconv.ParseFloat(p.Rate, 64)
			bnRate[p.Symbol] = r * 10000.0
		}
		bnIv := map[string]int{}
		for _, f := range fis {
			if f.Hours > 0 {
				bnIv[f.Symbol] = f.Hours
			}
		}
		ltRate := map[string]float64{}
		for _, r := range fr.Rates {
			if r.Exch == "lighter" {
				ltRate[r.Sym] = (r.Rate / 8.0) * 10000.0
			}
		}

		type fundCand struct {
			sp      DnSpec
			netBps  float64
			shortBn bool
		}
		var cands []fundCand
		dnMu.Lock()
		specsSnap := append([]DnSpec(nil), dnSpecs...)
		dnMu.Unlock()
		for _, sp := range specsSnap {
			dnMu.Lock()
			blocked := dnBlockedSym[sp.BnSym]
			dnMu.Unlock()
			if blocked {
				continue
			}
			ltBps, okL := ltRate[sp.Sym]
			if !okL {
				continue
			}
			iv := bnIv[sp.BnSym]
			if iv <= 0 {
				iv = 8
			}
			bBps := 0.0
			if nextHr%iv == 0 {
				bBps = bnRate[sp.BnSym]
			}
			diff := bBps - ltBps
			rtFees := sp.TakerFeeBp * 2.0
			if math.Abs(diff)-rtFees >= 10.0 {
				cands = append(cands, fundCand{sp: sp, netBps: math.Abs(diff), shortBn: diff > 0})
			}
		}
		sort.Slice(cands, func(i, j int) bool { return cands[i].netBps > cands[j].netBps })
		if len(cands) == 0 {
			continue
		}

		for _, c := range cands {
			sp := c.sp
			m, ok := markets[sp.Sym]
			st := mktStates[sp.Sym]
			if !ok || st == nil {
				continue
			}
			ensureLtSubscribed(m.ID)
			ensureBnLeverage(sp.BnSym)
			dnMu.Lock()
			db := dnBooks[sp.BnSym]
			var bb, ba float64
			if db != nil {
				bb, ba = db.Bid, db.Ask
			}
			dnMu.Unlock()
			lb, la, bd, ad := fetchLtBookREST(m.ID)
			if bb <= 0 || ba <= bb || lb <= 0 || la <= lb {
				continue
			}
			bnSide, ltSide := "SELL", "BID"
			ltRef := la
			entrySlipBps := (la - bb) / bb * 10000.0
			depthOK := ad
			if !c.shortBn {
				bnSide, ltSide = "BUY", "ASK"
				ltRef = lb
				entrySlipBps = (ba - lb) / lb * 10000.0
				depthOK = bd
			}
			if c.netBps-entrySlipBps-(sp.TakerFeeBp*2.0) < 8.0 || depthOK < 120.0 {
				continue
			}
			commonStep := math.Max(sp.Step, math.Pow10(-m.SzDec))
			rawQty := math.Floor((90.0/ltRef)/commonStep+1e-9) * commonStep
			qtyDec := sp.QDec
			if m.SzDec < qtyDec {
				qtyDec = m.SzDec
			}
			qty := roundTo(rawQty, qtyDec)
			if qty*ltRef < math.Max(12.0, sp.MinNot) || qty < m.MinB {
				continue
			}

			dnFundingActive.Store(true)
			dnBusyFlag.Store(true)
			mp := url.Values{}
			mp.Set("symbol", sp.BnSym)
			mp.Set("side", bnSide)
			mp.Set("type", "MARKET")
			mp.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
			mSc, mB, mE := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
			if mE != nil || mSc != 200 {
				fmt.Printf("[DN_FUND_SKIP sym=%s] status=%d body=%s\n", sp.BnSym, mSc, string(mB))
				if strings.Contains(string(mB), "-4411") {
					dnMu.Lock()
					dnBlockedSym[sp.BnSym] = true
					dnMu.Unlock()
				}
				dnFundingActive.Store(false)
				dnMu.Lock()
				dnBusyFlag.Store(dnSlotBusyCountLocked() > 0)
				dnMu.Unlock()
				continue
			}
			snipedHour = curHourID
			var mo struct {
				AvgPx string `json:"avgPrice"`
			}
			_ = json.Unmarshal(mB, &mo)
			bnFillPx, _ := strconv.ParseFloat(mo.AvgPx, 64)
			if bnFillPx <= 0 {
				bnFillPx = 0.5 * (bb + ba)
			}
			ltIocPx := roundTo(la*1.004, m.PxDec)
			if ltSide == "ASK" {
				ltIocPx = roundTo(lb*0.996, m.PxDec)
			}
			placeLtHedgeIOC(sp.Sym, ltSide, ltIocPx, qty, false)
			tid := time.Now().UnixMilli()
			recordStratOpen(tid, "ZERO_FEE_DN",
				fmt.Sprintf("HOURLY_FUND_%s_BN_%s+LT_%s", sp.Sym, bnSide, ltSide),
				sp.Sym, qty*bnFillPx, c.netBps, "Hourly_Funding_Lock")

			targetUnwind := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 59, 59, 0, time.UTC).Add(11 * time.Second)
			for time.Now().UTC().Before(targetUnwind) {
				if remotePaused.Load() {
					dnFundingActive.Store(false)
					dnMu.Lock()
					dnBusyFlag.Store(dnSlotBusyCountLocked() > 0)
					dnMu.Unlock()
					return
				}
				time.Sleep(500 * time.Millisecond)
			}
			closeBn, closeLt := "BUY", "ASK"
			if !c.shortBn {
				closeBn, closeLt = "SELL", "BID"
			}
			cp := url.Values{}
			cp.Set("symbol", sp.BnSym)
			cp.Set("side", closeBn)
			cp.Set("type", "MARKET")
			cp.Set("reduceOnly", "true")
			cp.Set("quantity", strconv.FormatFloat(qty, 'f', sp.QDec, 64))
			_, cB, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", cp)
			var co struct {
				AvgPx string `json:"avgPrice"`
			}
			_ = json.Unmarshal(cB, &co)
			bnExitPx, _ := strconv.ParseFloat(co.AvgPx, 64)
			lb3, la3, _, _ := fetchLtBookREST(m.ID)
			if bnExitPx <= 0 {
				bnExitPx = 0.5 * (lb3 + la3)
			}
			st.mu.Lock()
			ltPos, ltAe := st.pos, st.avgEntry
			st.mu.Unlock()
			if ltAe <= 0 {
				ltAe = ltRef
			}
			closeQty := qty
			if math.Abs(ltPos) >= m.MinB {
				closeQty = math.Abs(ltPos)
			}
			ltExIoc, ltExitRef := roundTo(lb3*0.995, m.PxDec), lb3
			if closeLt == "BID" {
				ltExIoc, ltExitRef = roundTo(la3*1.005, m.PxDec), la3
			}
			placeLtHedgeIOC(sp.Sym, closeLt, ltExIoc, closeQty, true)
			bnLeg, ltLeg, feeUsd, _, _ := computeDnLegPnls(bnSide, bnFillPx, bnExitPx, ltAe, ltExitRef, 1.0, qty, sp.TakerFeeBp, sp.TakerFeeBp)
			fundUSD := (qty * bnFillPx) * (c.netBps / 10000.0)
			netUSD := bnLeg + ltLeg + fundUSD
			netBps := netUSD / (qty * bnFillPx) * 10000.0
			recordStratClose(tid, "ZERO_FEE_DN",
				fmt.Sprintf("HOURLY_FUND_%s_BN_%s+LT_%s", sp.Sym, bnSide, ltSide),
				sp.Sym, qty*bnFillPx, time.Since(now).Seconds(),
				netBps, netUSD, bnLeg+fundUSD, ltLeg, feeUsd, "HOURLY_FUND_CAPTURED")
			emitBnAuditLine()
			dnFundingActive.Store(false)
			dnMu.Lock()
			dnBusyFlag.Store(dnSlotBusyCountLocked() > 0)
			dnMu.Unlock()
			break
		}
	}
}

func runZeroFeeDnLoop() {
	time.Sleep(5 * time.Second)
	if remotePaused.Load() || !setupBnUsdcMargin() {
		return
	}
	emitBnAuditLine()
	go runHourlyFundingSnipe()

	dnMu.Lock()
	for i := range dnSlots {
		dnSlots[i] = DnSlot{State: "IDLE"}
	}
	dnMu.Unlock()

	tick := time.NewTicker(80 * time.Millisecond)
	defer tick.Stop()
	lastScanLog := time.Now()
	lastMarginPoll := time.Now()

	for {
		select {
		case <-dnFillCh:
			for len(dnFillCh) > 0 {
				<-dnFillCh
			}
		case <-tick.C:
		}
		if remotePaused.Load() || breakerTripped.Load() {
			return
		}
		now := time.Now()
		if now.Sub(lastMarginPoll) >= 5*time.Second {
			lastMarginPoll = now
			go func() { _, _, _, _ = fetchBnAccountMargin() }()
		}
		for i := range dnSlots {
			processActiveDnSlot(i, now)
		}
		dnMu.Lock()
		idleIdx := idleDnSlotIdxLocked()
		specsSnap := dnSpecs
		unpolledOpen := 0
		for _, s := range dnSlots {
			if s.State != "" && s.State != "IDLE" && s.OpenedAt.After(lastMarginPoll) {
				unpolledOpen++
			}
		}
		sharedAvail := dnSharedAvail - float64(unpolledOpen)*11.0
		dnMu.Unlock()
		utcNow := now.UTC()
		if idleIdx >= 0 && !dnFundingActive.Load() &&
			now.UnixMilli() >= stratCoolUntilMS.Load() && utcNow.Minute() < 54 &&
			!(utcNow.Minute() == 0 && utcNow.Second() < 10) {
			scanAndOpenDnSlot(idleIdx, now, sharedAvail, specsSnap, &lastScanLog)
		}
	}
}

func main() {
	if _, err := os.Stat(PauseFlagPath); err == nil {
		fmt.Println("[PAUSED] /tmp/lighter_pause.flag active; exiting.")
		os.Exit(0)
	}
	loadDnHistory()

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

	discoverCommonAssets()
	autoDiscoverAllCommonDnPairs()

	go runTxWorker()
	go runTxWS()
	go runBnAllBookTickerWS()
	go runLtWS()
	go runPauseWatchdog()
	time.Sleep(1500 * time.Millisecond)

	cancelAll()
	flattenAllLtPositionsSync("boot_flat_")
	flattenBnDnAndSweep()
	emitBnAuditLine()

	if rawPC, ePC := readPauseControlJSON(); ePC == nil && len(rawPC) > 0 {
		var pcCheck struct {
			Paused bool `json:"paused"`
		}
		if json.Unmarshal(rawPC, &pcCheck) == nil && pcCheck.Paused {
			_ = os.WriteFile(PauseFlagPath, []byte("1"), 0644)
			fmt.Println("[PAUSED] Remote pause-control.json paused=true; staying halted.")
			os.Exit(0)
		}
	}

	go runZeroFeeDnLoop()
	fmt.Printf("GO-TOKYO %s LIVE | CLEAN 3-SLOT 0%%-FEE USDC ARB | Stop=$%.2f\n", BotVersion, StopEqUSD)
	go syncAccountLoop()
	select {}
}
