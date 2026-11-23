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
	"runtime/debug"
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
	BotVersion = "v107.4"
	BaseURL    = "https://mainnet.zklighter.elliot.ai"
	WsURL      = "wss://mainnet.zklighter.elliot.ai/stream"
	AcctIdx    = int64(770015)
	KeyIdx     = uint8(4)
	ChainID    = uint32(304)
	PrivKey    = "56491130b44362b137b0a3b46dc63d647865876f11ff1613c857f946ec8612f6e4f216d065f4695a"
	StopEqUSD  = 83.50
	ClipUSD    = 40.00

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

type MarketState struct {
	mu           sync.Mutex
	bookBids     map[string]float64
	bookAsks     map[string]float64
	ltBid        float64
	ltBidSz      float64
	ltAsk        float64
	ltAskSz      float64
	ltBidBandUsd float64
	ltAskBandUsd float64
	lastBookTS   time.Time
	pos          float64
	avgEntry     float64
	lastFillTS   time.Time
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
	Bid         float64
	Ask         float64
	BidQ        float64
	AskQ        float64
	TS          time.Time
	EmaBps      float64
	WarmTicks   int
	LastEmaTS   time.Time
	SnapMid     float64
	SnapTS      time.Time
	LastJumpBps float64
}

type BnOrderState struct {
	Status    string
	ExecQty   float64
	AvgPx     float64
	UpdatedAt time.Time
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
	TradeID     int64
	OpenedAt    time.Time
	RefLtPx     float64
	RefUsdtMid  float64
	RefBtcMid   float64
	EntryEmaBps float64
	LastPoll    time.Time
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
	cancelQueue = make(chan func(), 128)
	txQueue     = make(chan func(), 128)

	wsMu       sync.Mutex
	ltConn     *websocket.Conn
	subbedMids = map[int64]bool{}
	txWsMu     sync.Mutex
	txConn     *websocket.Conn

	bootTS           = time.Now()
	bootMS           = time.Now().UnixMilli()
	eqNow            atomic.Int64
	remotePaused     atomic.Bool
	breakerTripped   atomic.Bool
	quotaCoolUntil   atomic.Int64
	stratCoolUntilMS atomic.Int64

	engineWakeCh = make(chan struct{}, 1)
	ltFillCh     = make(chan string, 16)

	dnMu          sync.Mutex
	dnSpecs       []DnSpec
	dnByBnSym     = map[string]DnSpec{}
	dnUsdcBySym   = map[string]bool{}
	dnBooks       = map[string]*DnBook{}
	dnBlockedSym  = map[string]bool{}
	dnLevSet      = map[string]bool{}
	dnSharedAvail float64
	bnTotalWallet float64
	bnTotalMargin float64
	ltCollateral  atomic.Int64
	activeSlot    = DnSlot{State: "IDLE"}

	bnOrdMu  sync.Mutex
	bnOrdMap = map[int64]BnOrderState{}

	fundMu           sync.Mutex
	bnFundIvCache    = map[string]int{}
	lastFundCheckHr  int64 = -1
	lastFundCheckSym string
	lastFundAdverse  bool

	histMu         sync.Mutex
	dnTradeHist    []DnTradeRecord
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
		lastOrdTS:    map[string]time.Time{},
		lastCancelTS: map[string]time.Time{},
	}
}

func wakeEngine() {
	select {
	case engineWakeCh <- struct{}{}:
	default:
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
	st.mu.Lock()
	st.lastOrdTS[side] = time.Now()
	st.mu.Unlock()

	enqueueTx(func() {
		dbg := fmt.Sprintf("LT_%s_%s px=%.4f sz=%.5f ro=%d", sym, side, pxF, qty, redOnly)
		_ = sendSignedTx(fmt.Sprintf("lt_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
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
	})
}

// ---------- Dynamic Market Discovery & Feeds ----------

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
	usdcSyms := map[string]bool{}
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
			usdcSyms[s.BaseAsset] = true
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
		if s.QuoteAsset == "USDC" {
			ensureLtSubscribed(m.ID)
		}
	}
	dnMu.Lock()
	dnSpecs = discovered
	dnByBnSym = byBn
	dnUsdcBySym = usdcSyms
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
	bidBand, askBand := 0.0, 0.0
	if bestB > 0 {
		minB := bestB * (1.0 - 0.00015)
		for ps, sz := range st.bookBids {
			if sz > 0 {
				if p, err := strconv.ParseFloat(ps, 64); err == nil && p >= minB {
					bidBand += p * sz
				}
			}
		}
	}
	if bestA > 0 {
		maxA := bestA * (1.0 + 0.00015)
		for ps, sz := range st.bookAsks {
			if sz > 0 {
				if p, err := strconv.ParseFloat(ps, 64); err == nil && p <= maxA {
					askBand += p * sz
				}
			}
		}
	}
	st.ltBid, st.ltBidSz, st.ltBidBandUsd = bestB, bestBSz, bidBand
	st.ltAsk, st.ltAskSz, st.ltAskBandUsd = bestA, bestASz, askBand
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
		subbedMids = map[int64]bool{}
		wantActiveSyms := []string{
			"BTC", "ETH", "SOL",
			"UNI", "HYPE", "ENA", "NEAR", "SUI", "ARB",
			"VVV", "XMR", "LIT",
			"NVDA", "AAPL", "TSLA", "QQQ", "SPY", "META", "COIN", "MSTR",
			"AAVE", "AVAX", "WLD", "LINK", "DOGE", "XRP", "TIA", "PENGU", "TRUMP",
			"KAITO", "CRV", "ETHFI", "WLFI",
		}
		var midsToSub []int64
		for _, sym := range wantActiveSyms {
			if m, ok := markets[sym]; ok && !subbedMids[m.ID] {
				subbedMids[m.ID] = true
				midsToSub = append(midsToSub, m.ID)
			}
		}
		wsMu.Unlock()

		for _, st := range mktStates {
			st.mu.Lock()
			st.bookBids = map[string]float64{}
			st.bookAsks = map[string]float64{}
			st.ltBid, st.ltBidSz, st.ltBidBandUsd = 0, 0, 0
			st.ltAsk, st.ltAskSz, st.ltAskBandUsd = 0, 0, 0
			st.lastBookTS = time.Time{}
			st.mu.Unlock()
		}
		// Subscribe in background so ReadMessage() starts immediately and consumes every initial snapshot + ping!
		go func(c *websocket.Conn, ids []int64) {
			for _, mid := range ids {
				wsMu.Lock()
				alive := (ltConn == c)
				if alive {
					_ = c.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", mid)})
				}
				wsMu.Unlock()
				if !alive {
					return
				}
				time.Sleep(12 * time.Millisecond)
			}
			if tok, err := txCli.GetAuthToken(time.Now().Add(6 * time.Hour)); err == nil {
				wsMu.Lock()
				if ltConn == c {
					_ = c.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("account_all/%d", AcctIdx), "auth": tok})
				}
				wsMu.Unlock()
			}
		}(conn, midsToSub)

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
			if m.Type == "ping" {
				wsMu.Lock()
				if ltConn == conn {
					_ = conn.WriteJSON(map[string]string{"type": "pong"})
				}
				wsMu.Unlock()
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
							}
							st.mu.Unlock()
							if math.Abs(sz-prev) > 1e-6 {
								select {
								case ltFillCh <- sym:
								default:
								}
								wakeEngine()
							}
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
					if strings.Contains(m.Type, "subscribed") {
						st.bookBids = map[string]float64{}
						st.bookAsks = map[string]float64{}
						st.ltBid, st.ltBidSz, st.ltBidBandUsd = 0, 0, 0
						st.ltAsk, st.ltAskSz, st.ltAskBandUsd = 0, 0, 0
					}
					for _, b := range m.OrderBook.Bids {
						sz, _ := strconv.ParseFloat(b.Size, 64)
						bp, err := strconv.ParseFloat(b.Price, 64)
						if sz <= 0 {
							delete(st.bookBids, b.Price)
						} else {
							st.bookBids[b.Price] = sz
							if err == nil && st.ltAsk > 0 && bp >= st.ltAsk {
								for apStr := range st.bookAsks {
									if ap, e2 := strconv.ParseFloat(apStr, 64); e2 == nil && ap <= bp {
										delete(st.bookAsks, apStr)
									}
								}
							}
						}
					}
					for _, a := range m.OrderBook.Asks {
						sz, _ := strconv.ParseFloat(a.Size, 64)
						ap, err := strconv.ParseFloat(a.Price, 64)
						if sz <= 0 {
							delete(st.bookAsks, a.Price)
						} else {
							st.bookAsks[a.Price] = sz
							if err == nil && st.ltBid > 0 && ap <= st.ltBid {
								for bpStr := range st.bookBids {
									if bp, e2 := strconv.ParseFloat(bpStr, 64); e2 == nil && bp >= ap {
										delete(st.bookBids, bpStr)
									}
								}
							}
						}
					}
					recomputeBBO(st)
					if st.ltBid > 0 && st.ltAsk > 0 {
						st.lastBookTS = time.Now()
					}
					changed := st.ltBid != prevB || st.ltAsk != prevA
					st.mu.Unlock()
					if changed {
						dnMu.Lock()
						isUsdc := dnUsdcBySym[sym]
						dnMu.Unlock()
						asterMu.Lock()
						_, isAster := asterSpecs[sym+"USDT"]
						asterMu.Unlock()
						if isUsdc || isAster {
							wakeEngine()
						}
					}
				}
			}
		}
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func dnQuoteFxToUsdtLocked(quote string, now time.Time) (float64, bool) {
	if quote != "USDC" {
		return 1.0, true
	}
	bu := dnBooks["BTCUSDT"]
	bc := dnBooks["BTCUSDC"]
	if bu == nil || bc == nil || bu.Bid <= 0 || bc.Bid <= 0 || bu.Ask < bu.Bid || bc.Ask < bc.Bid {
		return 0, false
	}
	if now.Sub(bu.TS) > 4*time.Second || now.Sub(bc.TS) > 4*time.Second {
		return 0, false
	}
	fx := (0.5 * (bu.Bid + bu.Ask)) / (0.5 * (bc.Bid + bc.Ask))
	if fx < 0.98 || fx > 1.02 {
		return 0, false
	}
	return fx, true
}

func onBnAnyBookTick(bnSym, baseSym string, b, a, bq, aq float64) {
	dnMu.Lock()
	sp, exists := dnByBnSym[bnSym]
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
	bnMid := 0.5 * (b + a)
	isUsdc := strings.HasSuffix(bnSym, "USDC")

	dnMu.Lock()
	db := dnBooks[bnSym]
	if db == nil {
		db = &DnBook{SnapMid: bnMid, SnapTS: now}
		dnBooks[bnSym] = db
	}
	db.Bid, db.Ask, db.BidQ, db.AskQ, db.TS = b, a, bq, aq, now

	// Track rolling 250ms jump bps to detect toxic Binance impulse spikes before quoting:
	if db.SnapMid <= 0 {
		db.SnapMid, db.SnapTS = bnMid, now
	} else {
		db.LastJumpBps = (bnMid - db.SnapMid) / db.SnapMid * 10000.0
		if now.Sub(db.SnapTS) >= 250*time.Millisecond {
			db.SnapMid, db.SnapTS = bnMid, now
		}
	}

	// Continuously update 90s EWMA equilibrium basis (LtMid - BnMidUsd) regardless of active slot state:
	if (isUsdc || bnSym == "BTCUSDT") && ltB > 0 && ltA > ltB && now.Sub(ltTS) < 20*time.Second {
		quote := "USDT"
		if exists {
			quote = sp.Quote
		} else if isUsdc {
			quote = "USDC"
		}
		if fx, okFx := dnQuoteFxToUsdtLocked(quote, now); okFx {
			bnMidUsd := bnMid * fx
			ltMid := 0.5 * (ltB + ltA)
			ltSp := (ltA - ltB) / ltMid * 10000.0
			bnSp := (a - b) / bnMid * 10000.0
			rawBasis := (ltMid - bnMidUsd) / bnMidUsd * 10000.0
			if ltSp <= 28.0 && bnSp <= 20.0 && (db.WarmTicks == 0 || now.Sub(db.LastEmaTS) >= 500*time.Millisecond) {
				if db.WarmTicks == 0 {
					db.EmaBps = rawBasis
				} else {
					dt := math.Min(8.0, math.Max(0.2, now.Sub(db.LastEmaTS).Seconds()))
					db.EmaBps += (1.0 - math.Exp(-dt/90.0)) * (rawBasis - db.EmaBps)
				}
				db.LastEmaTS = now
				db.WarmTicks++
			}
		}
	}
	leadJump := math.Abs(db.LastJumpBps)
	dnMu.Unlock()

	if isUsdc || bnSym == "BTCUSDT" || (strings.HasSuffix(bnSym, "USDT") && leadJump >= 0.8) {
		wakeEngine()
	}
}

func leadMidAndJumps(sym string, now time.Time) (usdtMid, btcMid, usdtJumpBps, btcJumpBps float64) {
	dnMu.Lock()
	defer dnMu.Unlock()
	if ud := dnBooks[sym+"USDT"]; ud != nil && ud.Bid > 0 && ud.Ask >= ud.Bid && now.Sub(ud.TS) < 4*time.Second {
		usdtMid = 0.5 * (ud.Bid + ud.Ask)
		usdtJumpBps = ud.LastJumpBps
	}
	if bd := dnBooks["BTCUSDT"]; bd != nil && bd.Bid > 0 && bd.Ask >= bd.Bid && now.Sub(bd.TS) < 4*time.Second {
		btcMid = 0.5 * (bd.Bid + bd.Ask)
		btcJumpBps = bd.LastJumpBps
	}
	return usdtMid, btcMid, usdtJumpBps, btcJumpBps
}

func hasAdverseLeadMove(orderBnSide string, refUsdtMid, refBtcMid, curUsdtMid, curBtcMid float64) bool {
	uMove, bMove := 0.0, 0.0
	if refUsdtMid > 0 && curUsdtMid > 0 {
		uMove = (curUsdtMid - refUsdtMid) / refUsdtMid * 10000.0
	}
	if refBtcMid > 0 && curBtcMid > 0 {
		bMove = (curBtcMid - refBtcMid) / refBtcMid * 10000.0
	}
	if orderBnSide == "BUY" {
		return uMove <= -1.0 || bMove <= -0.8
	}
	return uMove >= 1.0 || bMove >= 0.8
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

// ---------- <1.5ms Binance Futures User Data WS (listenKey) ----------

func bnUserStreamReq(method string) (int, []byte, error) {
	req, err := http.NewRequest(method, "https://fapi.binance.com/fapi/v1/listenKey", nil)
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

func runBnUserWS() {
	for {
		sc, body, err := bnUserStreamReq("POST")
		if err != nil || sc != 200 {
			time.Sleep(2 * time.Second)
			continue
		}
		var lk struct {
			ListenKey string `json:"listenKey"`
		}
		if json.Unmarshal(body, &lk) != nil || lk.ListenKey == "" {
			time.Sleep(2 * time.Second)
			continue
		}
		conn, _, err := websocket.DefaultDialer.Dial("wss://fstream.binance.com/ws/"+lk.ListenKey, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		fmt.Println("[BN_USER_WS] Connected (<1.5ms ORDER_TRADE_UPDATE fill push active)")

		stopRenew := make(chan struct{})
		go func() {
			t := time.NewTicker(25 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-stopRenew:
					return
				case <-t.C:
					_, _, _ = bnUserStreamReq("PUT")
				}
			}
		}()

		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			var ev struct {
				E string `json:"e"`
				O struct {
					ID     int64  `json:"i"`
					Status string `json:"X"`
					CumQty string `json:"z"`
					AvgPx  string `json:"ap"`
				} `json:"o"`
			}
			if json.Unmarshal(msg, &ev) != nil {
				continue
			}
			if ev.E == "listenKeyExpired" {
				break
			}
			if ev.E == "ORDER_TRADE_UPDATE" && ev.O.ID > 0 {
				eq, _ := strconv.ParseFloat(ev.O.CumQty, 64)
				ap, _ := strconv.ParseFloat(ev.O.AvgPx, 64)
				bnOrdMu.Lock()
				if len(bnOrdMap) > 400 {
					for k, v := range bnOrdMap {
						if time.Since(v.UpdatedAt) > 10*time.Minute {
							delete(bnOrdMap, k)
						}
					}
				}
				bnOrdMap[ev.O.ID] = BnOrderState{
					Status:    ev.O.Status,
					ExecQty:   eq,
					AvgPx:     ap,
					UpdatedAt: time.Now(),
				}
				bnOrdMu.Unlock()
				wakeEngine()
			}
		}
		close(stopRenew)
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func ltGet(hc *http.Client, u string) (*http.Response, error) {
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	return hc.Do(req)
}

func syncAccountLoop() {
	hc := &http.Client{Timeout: 6 * time.Second}
	accURL := fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx)
	trURL := fmt.Sprintf("%s/api/v1/trades?account_index=%d&limit=100&sort_by=timestamp", BaseURL, AcctIdx)
	loopCnt := 0
	for {
		loopCnt++
		if loopCnt%2 == 1 {
			if trResp, e := ltGet(hc, trURL); e == nil {
				tb, _ := io.ReadAll(trResp.Body)
				trResp.Body.Close()
				if len(tb) > 20 && strings.Contains(string(tb), `"trades"`) {
					_ = os.WriteFile("/tmp/lighter_trades.json.tmp", tb, 0644)
					_ = os.Rename("/tmp/lighter_trades.json.tmp", "/tmp/lighter_trades.json")
				}
			}
		}
		if resp, err := ltGet(hc, accURL); err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var res struct {
				Accounts []struct {
					TotalAssetValue string `json:"total_asset_value"`
					Collateral      string `json:"collateral"`
					Positions       []struct {
						MarketID      int64  `json:"market_id"`
						Position      string `json:"position"`
						Sign          int    `json:"sign"`
						AvgEntryPrice string `json:"avg_entry_price"`
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
				col, _ := strconv.ParseFloat(a.Collateral, 64)
				if col > 0 {
					ltCollateral.Store(int64(col * 1e6))
				}
				eq, _ := strconv.ParseFloat(a.TotalAssetValue, 64)
				if eq <= 0 {
					eq = col
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
						st.mu.Unlock()
					}
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func fetchLtBookREST(mid int64) (float64, float64) {
	hc := &http.Client{Timeout: 4 * time.Second}
	u := fmt.Sprintf("%s/api/v1/orderBookOrders?market_id=%d&limit=5", BaseURL, mid)
	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		return 0, 0
	}
	req.Header.Set("User-Agent", "Mozilla/5.0")
	resp, err := hc.Do(req)
	if err != nil {
		return 0, 0
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
		return 0, 0
	}
	bb, _ := strconv.ParseFloat(ob.Bids[0].Price, 64)
	ba, _ := strconv.ParseFloat(ob.Asks[0].Price, 64)
	return bb, ba
}

func fetchLtPosREST(sym string) (float64, float64) {
	m, ok := markets[sym]
	if !ok {
		return 0, 0
	}
	hc := &http.Client{Timeout: 3 * time.Second}
	resp, err := ltGet(hc, fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx))
	if err != nil {
		return 0, 0
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var res struct {
		Accounts []struct {
			Positions []struct {
				MarketID      int64  `json:"market_id"`
				Position      string `json:"position"`
				Sign          int    `json:"sign"`
				AvgEntryPrice string `json:"avg_entry_price"`
			} `json:"positions"`
		} `json:"accounts"`
	}
	if json.Unmarshal(body, &res) != nil || len(res.Accounts) == 0 {
		return 0, 0
	}
	for _, p := range res.Accounts[0].Positions {
		if p.MarketID == m.ID {
			sz, _ := strconv.ParseFloat(p.Position, 64)
			if p.Sign < 0 && sz > 0 {
				sz = -sz
			}
			ae, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
			if st := mktStates[sym]; st != nil {
				st.mu.Lock()
				st.pos = sz
				if ae > 0 {
					st.avgEntry = ae
				}
				st.mu.Unlock()
			}
			return sz, ae
		}
	}
	return 0, 0
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
		var lb, la float64
		if st := mktStates[sym]; st != nil {
			st.mu.Lock()
			lb, la = st.ltBid, st.ltAsk
			st.mu.Unlock()
		}
		if lb <= 0 || la <= 0 {
			lb, la = fetchLtBookREST(m.ID)
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

func flattenBnDnAndSweep() {
	dnMu.Lock()
	activeSym := activeSlot.Spec.BnSym
	activeSlot = DnSlot{State: "IDLE"}
	dnMu.Unlock()

	var oWg sync.WaitGroup
	canceledMap := map[string]bool{}
	if activeSym != "" {
		canceledMap[activeSym] = true
		oWg.Add(1)
		go func(s string) {
			defer oWg.Done()
			p := url.Values{}
			p.Set("symbol", s)
			_, _, _ = bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/allOpenOrders", p)
		}(activeSym)
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
	wg.Add(3)
	go func() {
		defer wg.Done()
		cancelAll()
		flattenAllLtPositionsSync("pause_flat_")
	}()
	go func() {
		defer wg.Done()
		flattenBnDnAndSweep()
	}()
	go func() {
		defer wg.Done()
		flattenAsterAllSync()
	}()
	wg.Wait()
	if liveInitDone.Load() {
		writeLiveArenaTelemetry(time.Now())
	}
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

// ---------- Binance Futures REST Helpers & Margin ----------

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

func fetchBnAccountMargin() (sharedAvail, usdcBal, usdtBal float64, ok bool) {
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v2/account", nil)
	if err != nil || sc != 200 {
		return 0, 0, 0, false
	}
	var acc struct {
		AvailableBalance string `json:"availableBalance"`
		Assets           []struct {
			Asset         string `json:"asset"`
			WalletBalance string `json:"walletBalance"`
			MarginBalance string `json:"marginBalance"`
		} `json:"assets"`
	}
	if json.Unmarshal(body, &acc) != nil {
		return 0, 0, 0, false
	}
	sharedAvail, _ = strconv.ParseFloat(acc.AvailableBalance, 64)
	var usdcM, usdtM float64
	for _, a := range acc.Assets {
		wb, _ := strconv.ParseFloat(a.WalletBalance, 64)
		mb, _ := strconv.ParseFloat(a.MarginBalance, 64)
		if a.Asset == "USDC" {
			usdcBal = wb
			usdcM = mb
		} else if a.Asset == "USDT" {
			usdtBal = wb
			usdtM = mb
		}
	}
	dnMu.Lock()
	dnSharedAvail = sharedAvail
	bnTotalWallet = usdcBal + usdtBal
	bnTotalMargin = usdcM + usdtM
	dnMu.Unlock()
	return sharedAvail, usdcBal, usdtBal, true
}

func setupBnUsdcMargin() bool {
	if !hasAdoptedLivePositions() {
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
	fmt.Printf("[DN_READY] KISS USDC-M sharedAvail=$%.2f (USDC=$%.2f USDT=$%.2f) clip=$%.0f\n",
		sharedAvail, usdcBal, usdtBal, ClipUSD)
	return sharedAvail >= 15.0
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

// ---------- Core KISS Strategy Engine ----------

func checkAdverseHourlyFunding(sp DnSpec, bnSide string) bool {
	nowU := time.Now().UTC()
	curHr := nowU.Unix() / 3600

	fundMu.Lock()
	if lastFundCheckHr == curHr && lastFundCheckSym == sp.BnSym {
		res := lastFundAdverse
		fundMu.Unlock()
		return res
	}
	fundMu.Unlock()

	hc := &http.Client{Timeout: 3 * time.Second}
	prResp, e1 := hc.Get("https://fapi.binance.com/fapi/v1/premiumIndex?symbol=" + sp.BnSym)
	frResp, e2 := hc.Get(BaseURL + "/api/v1/funding-rates")
	if e1 != nil || e2 != nil {
		return false
	}
	prB, _ := io.ReadAll(prResp.Body)
	prResp.Body.Close()
	frB, _ := io.ReadAll(frResp.Body)
	frResp.Body.Close()

	fundMu.Lock()
	needIv := len(bnFundIvCache) == 0
	fundMu.Unlock()
	if needIv {
		if fiResp, e3 := hc.Get("https://fapi.binance.com/fapi/v1/fundingInfo"); e3 == nil {
			fiB, _ := io.ReadAll(fiResp.Body)
			fiResp.Body.Close()
			var fis []struct {
				Symbol string `json:"symbol"`
				Hours  int    `json:"fundingIntervalHours"`
			}
			if json.Unmarshal(fiB, &fis) == nil {
				fundMu.Lock()
				for _, f := range fis {
					if f.Hours > 0 {
						bnFundIvCache[f.Symbol] = f.Hours
					}
				}
				fundMu.Unlock()
			}
		}
	}

	var pr struct {
		Rate string `json:"lastFundingRate"`
	}
	var fr struct {
		Rates []struct {
			Sym  string  `json:"symbol"`
			Exch string  `json:"exchange"`
			Rate float64 `json:"rate"`
		} `json:"funding_rates"`
	}
	if json.Unmarshal(prB, &pr) != nil || json.Unmarshal(frB, &fr) != nil {
		return false
	}

	rawBnRate, _ := strconv.ParseFloat(pr.Rate, 64)
	bnRateBps := 0.0
	fundMu.Lock()
	iv := bnFundIvCache[sp.BnSym]
	fundMu.Unlock()
	if iv <= 0 {
		iv = 8
	}
	nextHr := (nowU.Hour() + 1) % 24
	if nextHr%iv == 0 {
		bnRateBps = rawBnRate * 10000.0
	}

	ltRateBps := 0.0
	for _, r := range fr.Rates {
		if r.Exch == "lighter" && r.Sym == sp.Sym {
			ltRateBps = (r.Rate / 8.0) * 10000.0
			break
		}
	}

	// Net funding collected across :00: SELL Binance (short) + Long Lighter collects bnRateBps - ltRateBps
	netFundBps := bnRateBps - ltRateBps
	if bnSide == "BUY" {
		netFundBps = -bnRateBps + ltRateBps
	}
	adverse := netFundBps < -1.50
	fundMu.Lock()
	lastFundCheckHr = curHr
	lastFundCheckSym = sp.BnSym
	lastFundAdverse = adverse
	fundMu.Unlock()

	fmt.Printf("[FUND_CHK sym=%s side=%s bn=%.2fbp lt=%.2fbp net=%+.2fbp adverse=%v]\n",
		sp.BnSym, bnSide, bnRateBps, ltRateBps, netFundBps, adverse)
	return adverse
}

func queryBnOrder(symbol string, oid int64, lastPoll time.Time, forceREST bool) (status string, execQty, avgPx float64, polledREST, ok bool) {
	bnOrdMu.Lock()
	ws, hasWS := bnOrdMap[oid]
	bnOrdMu.Unlock()
	if hasWS && !forceREST {
		if ws.Status == "FILLED" || ws.Status == "CANCELED" || ws.Status == "EXPIRED" {
			return ws.Status, ws.ExecQty, ws.AvgPx, false, true
		}
		if time.Since(lastPoll) < 1500*time.Millisecond {
			return ws.Status, ws.ExecQty, ws.AvgPx, false, true
		}
	}
	if !forceREST && time.Since(lastPoll) < 1500*time.Millisecond {
		return "", 0, 0, false, false
	}
	qp := url.Values{}
	qp.Set("symbol", symbol)
	qp.Set("orderId", strconv.FormatInt(oid, 10))
	sc, body, err := bnSignReq("GET", "fapi.binance.com", "/fapi/v1/order", qp)
	if err != nil || sc != 200 {
		return "", 0, 0, true, false
	}
	var o struct {
		Status  string `json:"status"`
		ExecQty string `json:"executedQty"`
		AvgPx   string `json:"avgPrice"`
		Price   string `json:"price"`
	}
	if json.Unmarshal(body, &o) != nil {
		return "", 0, 0, true, false
	}
	exQ, _ := strconv.ParseFloat(o.ExecQty, 64)
	avgP, _ := strconv.ParseFloat(o.AvgPx, 64)
	if avgP <= 0 {
		avgP, _ = strconv.ParseFloat(o.Price, 64)
	}
	return o.Status, exQ, avgP, true, true
}

func waitLtPositionFill(sym string, minNeed float64, waitDur time.Duration) (float64, float64) {
	t0 := time.Now()
	deadline := t0.Add(waitDur)
	st := mktStates[sym]
	rest450Done, rest850Done := false, false
	for time.Now().Before(deadline) {
		if st != nil {
			st.mu.Lock()
			pos, ae := st.pos, st.avgEntry
			st.mu.Unlock()
			if math.Abs(pos) >= minNeed {
				return pos, ae
			}
		}
		el := time.Since(t0)
		if !rest450Done && el >= 450*time.Millisecond {
			rest450Done = true
			if p, ae := fetchLtPosREST(sym); math.Abs(p) >= minNeed {
				return p, ae
			}
		}
		if !rest850Done && el >= 850*time.Millisecond {
			rest850Done = true
			if p, ae := fetchLtPosREST(sym); math.Abs(p) >= minNeed {
				return p, ae
			}
		}
		select {
		case <-ltFillCh:
		case <-time.After(45 * time.Millisecond):
		}
	}
	return fetchLtPosREST(sym)
}

func verifyAndHedgeLt(sp DnSpec, sl DnSlot, exQ, bnAvgP, fx float64) (hedged bool, ltEntryPx, filledQty float64) {
	m := markets[sp.Sym]
	st := mktStates[sp.Sym]
	if st == nil {
		return false, 0, 0
	}
	st.mu.Lock()
	lb, la := st.ltBid, st.ltAsk
	st.mu.Unlock()
	if lb <= 0 || la <= 0 {
		lb, la = fetchLtBookREST(m.ID)
	}

	// Attempt 1: 12bp buffer IOC + 1200ms confirmation
	px1, ref1 := roundTo(lb*0.9988, m.PxDec), lb
	if sl.LtSide == "BID" {
		px1, ref1 = roundTo(la*1.0012, m.PxDec), la
	}
	placeLtHedgeIOC(sp.Sym, sl.LtSide, px1, exQ, false)
	pos, ae := waitLtPositionFill(sp.Sym, m.MinB, 1200*time.Millisecond)
	if math.Abs(pos) >= m.MinB {
		if ae <= 0 {
			ae = ref1
		}
		return true, ae, math.Abs(pos)
	}

	// Attempt 2: wider 20bp buffer IOC retry + 1200ms confirmation
	st.mu.Lock()
	lb2, la2 := st.ltBid, st.ltAsk
	st.mu.Unlock()
	if lb2 <= 0 || la2 <= 0 {
		lb2, la2 = fetchLtBookREST(m.ID)
	}
	if lb2 <= 0 || la2 <= 0 {
		lb2, la2 = lb, la
	}
	px2, ref2 := roundTo(lb2*0.9980, m.PxDec), lb2
	if sl.LtSide == "BID" {
		px2, ref2 = roundTo(la2*1.0020, m.PxDec), la2
	}
	placeLtHedgeIOC(sp.Sym, sl.LtSide, px2, exQ, false)
	pos, ae = waitLtPositionFill(sp.Sym, m.MinB, 1200*time.Millisecond)
	if math.Abs(pos) >= m.MinB {
		if ae <= 0 {
			ae = ref2
		}
		return true, ae, math.Abs(pos)
	}

	// Both Lighter IOC attempts failed: immediately flatten the Binance leg (Zero Unhedged Tolerance!)
	unwindSide := "SELL"
	if sl.BnSide == "SELL" {
		unwindSide = "BUY"
	}
	mp := url.Values{}
	mp.Set("symbol", sp.BnSym)
	mp.Set("side", unwindSide)
	mp.Set("type", "MARKET")
	mp.Set("reduceOnly", "true")
	mp.Set("quantity", strconv.FormatFloat(exQ, 'f', sp.QDec, 64))
	_, mb, _ := bnSignReq("POST", "fapi.binance.com", "/fapi/v1/order", mp)
	var mo struct {
		AvgPx string `json:"avgPrice"`
	}
	_ = json.Unmarshal(mb, &mo)
	unPx, _ := strconv.ParseFloat(mo.AvgPx, 64)
	if unPx <= 0 {
		unPx = bnAvgP
	}
	time.Sleep(900 * time.Millisecond)
	if latePos, _ := fetchLtPosREST(sp.Sym); math.Abs(latePos) >= m.MinB {
		closeSide := "BID"
		if latePos > 0 {
			closeSide = "ASK"
		}
		lb3, la3 := fetchLtBookREST(m.ID)
		if lb3 <= 0 || la3 <= 0 {
			lb3, la3 = lb2, la2
		}
		closePx := roundTo(la3*1.01, m.PxDec)
		if closeSide == "ASK" {
			closePx = roundTo(lb3*0.99, m.PxDec)
		}
		placeLtHedgeIOC(sp.Sym, closeSide, closePx, math.Abs(latePos), true)
		fmt.Printf("[POST_ABORT_LT_SWEEP sym=%s side=%s qty=%.5f px=%.4f]\n",
			sp.Sym, closeSide, math.Abs(latePos), closePx)
	}
	bnLeg, _, feeUsd, netUsd, netBps := computeDnLegPnls(
		sl.BnSide, bnAvgP, unPx, bnAvgP*fx, bnAvgP*fx, fx, exQ, sl.EntryFeeBp, sp.TakerFeeBp)
	fmt.Printf("[UNHEDGED_ABORT id=%d sym=%s] Lighter failed 2x IOC; unwound Binance %s exQ=%.4f net=$%+.4f\n",
		sl.TradeID, sp.BnSym, unwindSide, exQ, netUsd)
	recordStratClose(sl.TradeID, "ZERO_FEE_DN",
		fmt.Sprintf("BN_%s_%s+LT_%s", sp.BnSym, sl.BnSide, sl.LtSide),
		sp.Sym, exQ*(bnAvgP*fx), time.Since(sl.OpenedAt).Seconds(),
		netBps, netUsd, bnLeg, 0.0, feeUsd, "UNHEDGED_ABORT")
	emitBnAuditLine()
	return false, 0, 0
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

func closeActiveSlot(sl DnSlot, bnExPx, ltPos, lb, la, fx, exitFeeBp float64, reason string, now time.Time) {
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
		stClose := mktStates[actSp.Sym]
		c0 := time.Now()
		cDeadline := c0.Add(1000 * time.Millisecond)
		rest550Done := false
		for time.Now().Before(cDeadline) {
			if stClose != nil {
				stClose.mu.Lock()
				curPos := stClose.pos
				stClose.mu.Unlock()
				if math.Abs(curPos) < m.MinB {
					break
				}
			}
			if !rest550Done && time.Since(c0) >= 550*time.Millisecond {
				rest550Done = true
				if rp, _ := fetchLtPosREST(actSp.Sym); math.Abs(rp) < m.MinB {
					break
				}
			}
			select {
			case <-ltFillCh:
			case <-time.After(50 * time.Millisecond):
			}
		}
		if curPos, _ := fetchLtPosREST(actSp.Sym); math.Abs(curPos) >= m.MinB {
			retrySide := "BID"
			if curPos > 0 {
				retrySide = "ASK"
			}
			lbR, laR := fetchLtBookREST(m.ID)
			if lbR <= 0 || laR <= 0 {
				lbR, laR = lb, la
			}
			retryPx := roundTo(laR*1.01, m.PxDec)
			if retrySide == "ASK" {
				retryPx = roundTo(lbR*0.99, m.PxDec)
			}
			placeLtHedgeIOC(actSp.Sym, retrySide, retryPx, math.Abs(curPos), true)
			time.Sleep(600 * time.Millisecond)
		}
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
	activeSlot = DnSlot{State: "IDLE"}
	dnMu.Unlock()
}

func processActiveSlot(now time.Time) {
	dnMu.Lock()
	sl := activeSlot
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
	lb, la, lbBand, laBand, ltPos, ltAvg := st.ltBid, st.ltAsk, st.ltBidBandUsd, st.ltAskBandUsd, st.pos, st.avgEntry
	st.mu.Unlock()

	eFx := sl.EntryFx
	if eFx <= 0 {
		eFx = 1.0
	}
	dnMu.Lock()
	fx, okFx := dnQuoteFxToUsdtLocked(actSp.Quote, now)
	dnMu.Unlock()
	if !okFx || fx <= 0 {
		fx = eFx
	}
	rtMakerFeeBp := sl.EntryFeeBp + actSp.MakerFeeBp
	rtMixedFeeBp := sl.EntryFeeBp + actSp.TakerFeeBp

	if sl.State == "BN_ENTRY_OPEN" {
		eUsd := sl.EntryBnPx * fx
		emaBps := sl.EntryEmaBps
		dnMu.Lock()
		if db := dnBooks[actSp.BnSym]; db != nil && db.WarmTicks >= 15 {
			emaBps = db.EmaBps
		}
		dnMu.Unlock()
		curNetEdge := (lb-eUsd)/eUsd*10000.0 - rtMakerFeeBp
		curDisloc := curNetEdge - emaBps
		ltMoved := lb < sl.RefLtPx*(1.0-2.5/10000.0)
		ltHedgeDepth := lbBand
		if sl.BnSide == "SELL" {
			curNetEdge = (eUsd-la)/eUsd*10000.0 - rtMakerFeeBp
			curDisloc = curNetEdge + emaBps
			ltMoved = la > sl.RefLtPx*(1.0+2.5/10000.0)
			ltHedgeDepth = laBand
		}
		curUsdtMid, curBtcMid, _, _ := leadMidAndJumps(actSp.Sym, now)
		leadAdverse := hasAdverseLeadMove(sl.BnSide, sl.RefUsdtMid, sl.RefBtcMid, curUsdtMid, curBtcMid)
		needCancel := leadAdverse || ltMoved || ltHedgeDepth < 60.0 || curNetEdge < 2.00 || curDisloc < 5.00 || now.Sub(sl.OpenedAt) >= 8*time.Second
		status, exQ, avgP, polled, ok := queryBnOrder(actSp.BnSym, sl.EntryOid, sl.LastPoll, needCancel)
		if polled {
			dnMu.Lock()
			activeSlot.LastPoll = now
			dnMu.Unlock()
		}
		if !ok {
			return
		}
		if avgP <= 0 {
			avgP = sl.EntryBnPx
		}
		if status == "FILLED" || (needCancel && exQ*(avgP*fx) >= actSp.MinNot && exQ >= m.MinB) {
			if status != "FILLED" {
				qp := url.Values{}
				qp.Set("symbol", actSp.BnSym)
				qp.Set("orderId", strconv.FormatInt(sl.EntryOid, 10))
				_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
				var co struct {
					ExecQty string `json:"executedQty"`
					AvgPx   string `json:"avgPrice"`
				}
				if json.Unmarshal(cb, &co) == nil {
					if q2, e2 := strconv.ParseFloat(co.ExecQty, 64); e2 == nil && q2 > exQ {
						exQ = q2
					}
					if p2, e3 := strconv.ParseFloat(co.AvgPx, 64); e3 == nil && p2 > 0 {
						avgP = p2
					}
				}
			}
			hedged, ltRefFill, filledQ := verifyAndHedgeLt(actSp, sl, exQ, avgP, fx)
			if !hedged {
				dnMu.Lock()
				activeSlot = DnSlot{State: "IDLE"}
				dnMu.Unlock()
				stratCoolUntilMS.Store(time.Now().Add(8 * time.Second).UnixMilli())
				return
			}
			useQty := math.Min(exQ, filledQ)
			if useQty < m.MinB {
				useQty = exQ
			}
			avgUsd := avgP * fx
			lockedBps := (ltRefFill-avgUsd)/avgUsd*10000.0 - rtMakerFeeBp
			if sl.BnSide == "SELL" {
				lockedBps = (avgUsd-ltRefFill)/avgUsd*10000.0 - rtMakerFeeBp
			}
			dnMu.Lock()
			activeSlot.State = "HEDGED"
			activeSlot.Qty = useQty
			activeSlot.EntryBnPx = avgP
			activeSlot.EntryLtPx = ltRefFill
			activeSlot.EntryFx = fx
			activeSlot.OpenedAt = time.Now()
			activeSlot.LastPoll = time.Time{}
			dnMu.Unlock()
			recordStratOpen(sl.TradeID, "ZERO_FEE_DN",
				fmt.Sprintf("BN_%s_%s+LT_%s", actSp.BnSym, sl.BnSide, sl.LtSide),
				actSp.Sym, useQty*avgUsd, lockedBps,
				fmt.Sprintf("KISS 0pctFee Fx%.5f BnMaker@%.5f+Lt0pct@%.5f", fx, avgP, ltRefFill))
		} else if needCancel || status == "CANCELED" || status == "EXPIRED" {
			if status == "NEW" || status == "PARTIALLY_FILLED" {
				qp := url.Values{}
				qp.Set("symbol", actSp.BnSym)
				qp.Set("orderId", strconv.FormatInt(sl.EntryOid, 10))
				_, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
				var co struct {
					ExecQty string `json:"executedQty"`
					AvgPx   string `json:"avgPrice"`
				}
				if json.Unmarshal(cb, &co) == nil {
					if q2, e2 := strconv.ParseFloat(co.ExecQty, 64); e2 == nil {
						exQ = q2
					}
					if p2, e3 := strconv.ParseFloat(co.AvgPx, 64); e3 == nil && p2 > 0 {
						avgP = p2
					}
				}
			}
			if exQ >= m.MinB {
				hedged, ltRefFill, filledQ := verifyAndHedgeLt(actSp, sl, exQ, avgP, fx)
				if !hedged {
					dnMu.Lock()
					activeSlot = DnSlot{State: "IDLE"}
					dnMu.Unlock()
					stratCoolUntilMS.Store(time.Now().Add(8 * time.Second).UnixMilli())
					return
				}
				useQty := math.Min(exQ, filledQ)
				if useQty < m.MinB {
					useQty = exQ
				}
				avgUsd := avgP * fx
				dnMu.Lock()
				activeSlot.State = "HEDGED"
				activeSlot.Qty = useQty
				activeSlot.EntryBnPx = avgP
				activeSlot.EntryLtPx = ltRefFill
				activeSlot.EntryFx = fx
				activeSlot.OpenedAt = time.Now()
				activeSlot.LastPoll = time.Time{}
				dnMu.Unlock()
				recordStratOpen(sl.TradeID, "ZERO_FEE_DN",
					fmt.Sprintf("BN_%s_%s+LT_%s", actSp.BnSym, sl.BnSide, sl.LtSide),
					actSp.Sym, useQty*avgUsd, curNetEdge, "KISS Partial_Hedged")
			} else {
				dnMu.Lock()
				activeSlot = DnSlot{State: "IDLE"}
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
		var makerExitBnPx, takerExitLtPx, combNetMakerBps, combTakerNetBps, ltExitDepthUsd float64
		if sl.BnSide == "BUY" {
			closeBnSide = "SELL"
			minSell := math.Ceil((bb*(1.0+0.00015))/actSp.Tick-1e-9) * actSp.Tick
			makerExitBnPx = roundTo(math.Max(ba, minSell), actSp.PxDec)
			takerExitLtPx = la
			ltExitDepthUsd = laBand
			combNetMakerBps = (((makerExitBnPx-sl.EntryBnPx)*fx)+(eLt-takerExitLtPx))/eUsd*10000.0 - rtMakerFeeBp
			combTakerNetBps = (((bb-sl.EntryBnPx)*fx)+(eLt-la))/eUsd*10000.0 - rtMixedFeeBp
		} else {
			closeBnSide = "BUY"
			maxBuy := math.Floor((ba*(1.0-0.00015))/actSp.Tick+1e-9) * actSp.Tick
			makerExitBnPx = roundTo(math.Min(bb, maxBuy), actSp.PxDec)
			takerExitLtPx = lb
			ltExitDepthUsd = lbBand
			combNetMakerBps = (((sl.EntryBnPx-makerExitBnPx)*fx)+(takerExitLtPx-eLt))/eUsd*10000.0 - rtMakerFeeBp
			combTakerNetBps = (((sl.EntryBnPx-ba)*fx)+(lb-eLt))/eUsd*10000.0 - rtMixedFeeBp
		}
		holdS := now.Sub(sl.OpenedAt).Seconds()
		utcNow := now.UTC()
		adverseFundFlush := utcNow.Minute() == 58 && utcNow.Second() >= 30 && checkAdverseHourlyFunding(actSp, sl.BnSide)
		catastrophicSL := holdS >= 30.0 && combTakerNetBps <= -35.0
		curUsdtMid, curBtcMid, usdtJump, btcJump := leadMidAndJumps(actSp.Sym, now)
		// Patient 0%-fee Maker convergence with recessed exit quotes & lead-market calmness:
		wantExit := ltExitDepthUsd >= 100.0 && math.Abs(usdtJump) <= 1.5 && math.Abs(btcJump) <= 1.2 &&
			((holdS < 120.0 && combNetMakerBps >= 3.50) ||
				(holdS >= 120.0 && holdS < 300.0 && combNetMakerBps >= 2.50) ||
				(holdS >= 300.0 && combNetMakerBps >= 1.50))

		if adverseFundFlush || catastrophicSL {
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
			if adverseFundFlush {
				exReason = "PRE_HOUR_FUND_GUARD"
			}
			closeActiveSlot(sl, bnExPx, ltPos, lb, la, fx, actSp.TakerFeeBp, exReason, now)
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
					activeSlot.State = "BN_EXIT_OPEN"
					activeSlot.ExitOid = or.OrderID
					activeSlot.RefLtPx = takerExitLtPx
					activeSlot.RefUsdtMid = curUsdtMid
					activeSlot.RefBtcMid = curBtcMid
					activeSlot.LastPoll = time.Time{}
					dnMu.Unlock()
					fmt.Printf("[DN_EXIT_RESTED id=%d sym=%s side=%s @ %.5f estNet=%+.2fbp fee=$0.00]\n",
						sl.TradeID, actSp.BnSym, closeBnSide, makerExitBnPx, combNetMakerBps)
				}
			}
		}
		return
	}

	if sl.State == "BN_EXIT_OPEN" {
		status, _, exP, polled, ok := queryBnOrder(actSp.BnSym, sl.ExitOid, sl.LastPoll, false)
		if polled {
			dnMu.Lock()
			activeSlot.LastPoll = now
			dnMu.Unlock()
		}
		if !ok {
			return
		}
		if exP <= 0 {
			dnMu.Lock()
			if db := dnBooks[actSp.BnSym]; db != nil {
				exP = 0.5 * (db.Bid + db.Ask)
			}
			dnMu.Unlock()
		}
		if status == "FILLED" {
			closeActiveSlot(sl, exP, ltPos, lb, la, fx, actSp.MakerFeeBp, "ZERO_FEE_4LEG_TP", now)
			stratCoolUntilMS.Store(now.Add(2 * time.Second).UnixMilli())
		} else if status == "CANCELED" || status == "EXPIRED" {
			dnMu.Lock()
			activeSlot.State = "HEDGED"
			dnMu.Unlock()
		} else {
			curComb := (((exP-sl.EntryBnPx)*fx)+(eLt-la))/eUsd*10000.0 - rtMakerFeeBp
			closeBnSide := "SELL"
			ltExitDepthUsd := laBand
			if sl.BnSide == "SELL" {
				curComb = (((sl.EntryBnPx-exP)*fx)+(lb-eLt))/eUsd*10000.0 - rtMakerFeeBp
				closeBnSide = "BUY"
				ltExitDepthUsd = lbBand
			}
			holdS := now.Sub(sl.OpenedAt).Seconds()
			minKeepBps := 2.00
			if holdS >= 120.0 && holdS < 300.0 {
				minKeepBps = 1.20
			} else if holdS >= 300.0 {
				minKeepBps = 0.50
			}
			curUsdtMid, curBtcMid, _, _ := leadMidAndJumps(actSp.Sym, now)
			leadAdverse := hasAdverseLeadMove(closeBnSide, sl.RefUsdtMid, sl.RefBtcMid, curUsdtMid, curBtcMid)
			utcNow := now.UTC()
			adverseCancel := utcNow.Minute() == 58 && utcNow.Second() >= 30 && checkAdverseHourlyFunding(actSp, sl.BnSide)
			if adverseCancel || leadAdverse || ltExitDepthUsd < 60.0 || curComb < minKeepBps {
				qp := url.Values{}
				qp.Set("symbol", actSp.BnSym)
				qp.Set("orderId", strconv.FormatInt(sl.ExitOid, 10))
				sc, cb, _ := bnSignReq("DELETE", "fapi.binance.com", "/fapi/v1/order", qp)
				var co struct {
					Status  string `json:"status"`
					ExecQty string `json:"executedQty"`
					AvgPx   string `json:"avgPrice"`
				}
				_ = json.Unmarshal(cb, &co)
				if sc != 200 {
					if st2, eq2, ap2, _, ok2 := queryBnOrder(actSp.BnSym, sl.ExitOid, time.Time{}, true); ok2 {
						co.Status = st2
						co.ExecQty = strconv.FormatFloat(eq2, 'f', 8, 64)
						if ap2 > 0 {
							co.AvgPx = strconv.FormatFloat(ap2, 'f', 8, 64)
						}
					}
				}
				if co.Status == "FILLED" {
					if cp, e2 := strconv.ParseFloat(co.AvgPx, 64); e2 == nil && cp > 0 {
						exP = cp
					}
					closeActiveSlot(sl, exP, ltPos, lb, la, fx, actSp.MakerFeeBp, "ZERO_FEE_4LEG_TP", now)
					stratCoolUntilMS.Store(now.Add(2 * time.Second).UnixMilli())
				} else {
					exQ, _ := strconv.ParseFloat(co.ExecQty, 64)
					if exQ >= m.MinB && exQ < sl.Qty-m.MinB*0.5 {
						closeLtSide, ltClosePx := "BID", roundTo(la*1.004, m.PxDec)
						if sl.BnSide == "SELL" {
							closeLtSide, ltClosePx = "ASK", roundTo(lb*0.996, m.PxDec)
						}
						placeLtHedgeIOC(actSp.Sym, closeLtSide, ltClosePx, exQ, true)
						remQ := roundTo(sl.Qty-exQ, actSp.QDec)
						if remQ >= m.MinB {
							dnMu.Lock()
							activeSlot.Qty = remQ
							activeSlot.State = "HEDGED"
							dnMu.Unlock()
							return
						}
					} else if exQ >= sl.Qty-m.MinB*0.5 {
						if cp, e2 := strconv.ParseFloat(co.AvgPx, 64); e2 == nil && cp > 0 {
							exP = cp
						}
						closeActiveSlot(sl, exP, ltPos, lb, la, fx, actSp.MakerFeeBp, "ZERO_FEE_4LEG_TP", now)
						stratCoolUntilMS.Store(now.Add(2 * time.Second).UnixMilli())
						return
					}
					dnMu.Lock()
					activeSlot.State = "HEDGED"
					dnMu.Unlock()
				}
			}
		}
	}
}

func scanAndOpenSlot(now time.Time, sharedAvail float64, specsSnap []DnSpec, lastScanLog *time.Time) {
	if sharedAvail < 15.0 {
		return
	}
	var bestSp DnSpec
	bestBnSide, bestLtSide := "", ""
	bestRawEdge, bestDisloc, bestBnPx, bestLtPx, bestFx, bestUsdtMid, bestBtcMid, bestEmaBps := -999.0, -999.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0

	for _, sp := range specsSnap {
		if sp.Quote != "USDC" {
			continue
		}
		switch sp.Sym {
		case "BTC", "ETH", "SOL", "XRP", "BNB", "ZEC":
			continue
		}
		dnMu.Lock()
		fx, okFx := dnQuoteFxToUsdtLocked(sp.Quote, now)
		blocked := dnBlockedSym[sp.BnSym]
		db := dnBooks[sp.BnSym]
		var bb, ba, emaBps, jumpBps float64
		var wTicks int
		var bTS time.Time
		if db != nil {
			bb, ba = db.Bid, db.Ask
			emaBps, wTicks, bTS, jumpBps = db.EmaBps, db.WarmTicks, db.TS, db.LastJumpBps
		}
		dnMu.Unlock()
		if !okFx || blocked || bb <= 0 || ba <= bb || wTicks < 15 || now.Sub(bTS) > 15*time.Second || math.Abs(emaBps) > 25.0 || math.Abs(jumpBps) > 1.8 {
			continue
		}
		uMid, bMid, uJump, bJump := leadMidAndJumps(sp.Sym, now)
		if math.Abs(uJump) > 1.5 || math.Abs(bJump) > 1.2 {
			continue
		}
		m, okM := markets[sp.Sym]
		st := mktStates[sp.Sym]
		if !okM || st == nil {
			continue
		}
		st.mu.Lock()
		lb, la, lbBand, laBand, ltPos, lTS := st.ltBid, st.ltAsk, st.ltBidBandUsd, st.ltAskBandUsd, st.pos, st.lastBookTS
		st.mu.Unlock()
		if math.Abs(ltPos) >= m.MinB || lb <= 0 || la <= lb || now.Sub(lTS) > 15*time.Second {
			continue
		}
		ltSp := (la - lb) / lb * 10000.0
		bnSp := (ba - bb) / bb * 10000.0
		if ltSp > 7.0 || bnSp > 5.5 {
			continue
		}

		// Side A: BUY Maker Binance + SELL IOC Lighter (recessed >=1.5bp from BnAsk, 1.5bp LT band depth >= $100)
		maxBuy := math.Floor((ba*(1.0-0.00015))/sp.Tick+1e-9) * sp.Tick
		bnBuyPx := roundTo(math.Min(bb, maxBuy), sp.PxDec)
		bnBuyUsd := bnBuyPx * fx
		bboBuyUsd := bb * fx
		rawEdgeA := (lb - bnBuyUsd) / bnBuyUsd * 10000.0
		dislocA := rawEdgeA - emaBps
		bboDislocA := ((lb - bboBuyUsd) / bboBuyUsd * 10000.0) - emaBps
		if lbBand >= 100.0 && bboDislocA >= 5.50 && rawEdgeA >= 3.00 && dislocA >= 6.50 && dislocA > bestDisloc {
			bestDisloc, bestRawEdge, bestSp, bestBnSide, bestLtSide, bestBnPx, bestLtPx, bestFx, bestUsdtMid, bestBtcMid, bestEmaBps =
				dislocA, rawEdgeA, sp, "BUY", "ASK", bnBuyPx, lb, fx, uMid, bMid, emaBps
		}

		// Side B: SELL Maker Binance + BUY IOC Lighter (recessed >=1.5bp from BnBid, 1.5bp LT band depth >= $100)
		minSell := math.Ceil((bb*(1.0+0.00015))/sp.Tick-1e-9) * sp.Tick
		bnSellPx := roundTo(math.Max(ba, minSell), sp.PxDec)
		bnSellUsd := bnSellPx * fx
		bboSellUsd := ba * fx
		rawEdgeB := (bnSellUsd - la) / bnSellUsd * 10000.0
		dislocB := rawEdgeB + emaBps
		bboDislocB := ((bboSellUsd - la) / bboSellUsd * 10000.0) + emaBps
		if laBand >= 100.0 && bboDislocB >= 5.50 && rawEdgeB >= 3.00 && dislocB >= 6.50 && dislocB > bestDisloc {
			bestDisloc, bestRawEdge, bestSp, bestBnSide, bestLtSide, bestBnPx, bestLtPx, bestFx, bestUsdtMid, bestBtcMid, bestEmaBps =
				dislocB, rawEdgeB, sp, "SELL", "BID", bnSellPx, la, fx, uMid, bMid, emaBps
		}
	}

	if now.Sub(*lastScanLog) >= 12*time.Second && bestSp.BnSym != "" {
		*lastScanLog = now
		fmt.Printf("[DN_SCAN] best=%s bnMaker=%s->ltZeroFee=%s netAfterFee=%+.2fbp (disloc=%+.2fbp fee=0.0bp clip=$%.0f) bnPx=%.5f ltPx=%.5f\n",
			bestSp.BnSym, bestBnSide, bestLtSide, bestRawEdge, bestDisloc, ClipUSD, bestBnPx, bestLtPx)
	}
	if bestSp.BnSym == "" || bestRawEdge < 3.00 || bestDisloc < 6.50 {
		return
	}
	m := markets[bestSp.Sym]
	ensureLtSubscribed(m.ID)
	ensureBnLeverage(bestSp.BnSym)
	commonStep := math.Max(bestSp.Step, math.Pow10(-m.SzDec))
	rawQty := math.Floor((ClipUSD/(bestBnPx*bestFx))/commonStep+1e-9) * commonStep
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
			activeSlot = DnSlot{
				State: "BN_ENTRY_OPEN", Spec: bestSp, BnSide: bestBnSide, LtSide: bestLtSide,
				EntryOid: or.OrderID, Qty: qty, EntryBnPx: bestBnPx, EntryFx: bestFx,
				EntryFeeBp: bestSp.MakerFeeBp, TradeID: tid, OpenedAt: now, RefLtPx: bestLtPx,
				RefUsdtMid: bestUsdtMid, RefBtcMid: bestBtcMid, EntryEmaBps: bestEmaBps,
			}
			dnMu.Unlock()
			fmt.Printf("[DN_ZERO_FEE_MAKER id=%d sym=%s bn=%s qty=%.4f @ %.5f raw=%+.2fbp disloc=%+.2fbp fee=0.0bp]\n",
				tid, bestSp.BnSym, bestBnSide, qty, bestBnPx, bestRawEdge, bestDisloc)
		}
	} else if strings.Contains(string(body), "-4411") {
		dnMu.Lock()
		dnBlockedSym[bestSp.BnSym] = true
		dnMu.Unlock()
	}
}

func runZeroFeeKISS() {
	time.Sleep(5 * time.Second)
	if remotePaused.Load() || !setupBnUsdcMargin() {
		return
	}
	emitBnAuditLine()
	dnMu.Lock()
	activeSlot = DnSlot{State: "IDLE"}
	dnMu.Unlock()

	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	lastScanLog := time.Now()
	lastMarginPoll := time.Now()

	for {
		select {
		case <-engineWakeCh:
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
		processActiveSlot(now)
		dnMu.Lock()
		isIdle := activeSlot.State == "" || activeSlot.State == "IDLE"
		specsSnap := dnSpecs
		sharedAvail := dnSharedAvail
		dnMu.Unlock()
		if isIdle && now.UnixMilli() >= stratCoolUntilMS.Load() {
			scanAndOpenSlot(now, sharedAvail, specsSnap, &lastScanLog)
		}
	}
}

func main() {
	debug.SetMemoryLimit(200 << 20)
	debug.SetGCPercent(40)

	if err := initAsterEIP712(); err != nil {
		fmt.Printf("[ASTER_INIT_ERR] %v\n", err)
	}
	initLiveMicroArena()
	_, _, _ = fetchAsterAccountMargin()
	writeLiveArenaTelemetry(time.Now())

	if _, err := os.Stat(PauseFlagPath); err == nil {
		remotePaused.Store(true)
		writeLiveArenaTelemetry(time.Now())
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
	discoverAsterMarkets()

	go runTxWorker()
	go runTxWS()
	go runBnAllBookTickerWS()
	go runAsterBookTickerWS()
	go runLtWS()
	go runPauseWatchdog()
	time.Sleep(1500 * time.Millisecond)

	cancelAll() // Cancel resting Lighter Maker entry orders only; keep adopted hedged positions untouched!
	if !hasAdoptedLivePositions() {
		flattenAllLtPositionsSync("boot_flat_")
		flattenBnDnAndSweep()
		flattenAsterAllSync()
	} else {
		fmt.Println("[HOT_RELOAD] Adopted active 1:1 hedged positions seamlessly without flattening or pausing!")
	}
	emitBnAuditLine()

	if rawPC, ePC := readPauseControlJSON(); ePC == nil && len(rawPC) > 0 {
		var pcCheck struct {
			Paused bool `json:"paused"`
		}
		if json.Unmarshal(rawPC, &pcCheck) == nil && pcCheck.Paused {
			remotePaused.Store(true)
			writeLiveArenaTelemetry(time.Now())
			_ = os.WriteFile(PauseFlagPath, []byte("1"), 0644)
			fmt.Println("[PAUSED] Remote pause-control.json paused=true; staying halted.")
			os.Exit(0)
		}
	}

	go runBnUserWS()
	go runAsterUserWS()
	go runLiveMicroArenaLoop()
	fmt.Printf("GO-TOKYO %s LIVE | 4-STRATEGY MICRO-CAPITAL ARENA | Lighter+Binance+AsterDEX | Stop=$%.2f\n", BotVersion, StopEqUSD)
	go syncAccountLoop()
	select {}
}
