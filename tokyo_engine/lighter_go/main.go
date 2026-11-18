package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
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
	ClipUSD      = 10.60
	EdgeBps      = 5.0
	CloseEdgeBps = 2.5
	CancelBps    = 1.0
	ImpulseBps   = 2.0
	StopEqUSD    = 14.50
)

type Market struct {
	Sym   string
	ID    int64
	SzDec int
	PxDec int
	MinB  float64
}

var markets = map[string]Market{
	"ETH": {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020},
	"SOL": {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055},
}
var idToSym = map[int64]string{0: "ETH", 2: "SOL"}

type ActiveOrder struct {
	COI    int64
	Px     float64
	RefMid float64
	TS     time.Time
}

var (
	mu         sync.Mutex
	bnMid      = map[string]float64{}
	bnEma      = map[string]float64{}
	ltBid      = map[string]float64{}
	ltAsk      = map[string]float64{}
	positions  = map[string]float64{}
	avgEntry   = map[string]float64{}
	activeOrds = map[string]*ActiveOrder{} // key: sym+"_"+side
	lastOrdTS  = map[string]time.Time{}
	eqNow      = 14.98
	lastLog    = time.Time{}

	txCli    *client.TxClient
	nonceSeq atomic.Int64
	coiSeq   atomic.Int64
	txMu     sync.Mutex

	wsMu   sync.Mutex
	ltConn *websocket.Conn
)

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
	// Fast-path: send directly over already-open TCP_NODELAY WebSocket (<1ms non-blocking write!)
	wsMu.Lock()
	c := ltConn
	var wsErr error
	if c != nil {
		wsErr = c.WriteJSON(map[string]any{
			"type": "jsonapi/sendtx",
			"data": map[string]any{"id": id, "tx_type": txType, "tx_info": txInfo},
		})
	}
	wsMu.Unlock()
	if c != nil && wsErr == nil {
		fmt.Printf("[WS_TX %s %s] %dus\n", id, dbg, time.Since(t0).Microseconds())
		return nil
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
		return fmt.Errorf("sendTx failed: %s", string(body))
	}
	fmt.Printf("[TX_OK %s %s] %dms\n", id, dbg, ms)
	return nil
}

func cancelAll() {
	err := sendSignedTx("ca", "cancelAll", func(opts *types.TransactOpts) (uint8, string, error) {
		ti, e := txCli.GetCancelAllOrdersTransaction(&types.CancelAllOrdersTxReq{TimeInForce: 0, Time: 0}, opts)
		if e != nil {
			return 0, "", e
		}
		s, e := ti.GetTxInfo()
		return ti.GetTxType(), s, e
	})
	if err != nil {
		fmt.Printf("[CANCEL_ALL ERR] %v\n", err)
	}
	mu.Lock()
	activeOrds = map[string]*ActiveOrder{}
	mu.Unlock()
}

func cancelSide(sym, side string) {
	key := sym + "_" + side
	ao, ok := activeOrds[key]
	if !ok {
		return
	}
	delete(activeOrds, key)
	m := markets[sym]
	go func(coi int64, mid int64) {
		_ = sendSignedTx(fmt.Sprintf("c_%d", coi), sym+"_"+side, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: mid, Index: coi}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
	}(ao.COI, m.ID)
}

func placeSide(sym, side string, pxF, midF float64) {
	m := markets[sym]
	baseF := math.Max(m.MinB, ClipUSD/pxF)
	baseInt := int64(math.Ceil(baseF * math.Pow10(m.SzDec)))
	pxInt := uint32(math.Round(pxF * math.Pow10(m.PxDec)))
	coi := coiSeq.Add(1)
	var isAsk uint8
	if side == "ASK" {
		isAsk = 1
	}
	activeOrds[sym+"_"+side] = &ActiveOrder{COI: coi, Px: pxF, RefMid: midF, TS: time.Now()}
	go func() {
		exp := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
		dbg := fmt.Sprintf("%s_%s px=%.4f", sym, side, pxF)
		err := sendSignedTx(fmt.Sprintf("o_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      m.ID,
				ClientOrderIndex: coi,
				BaseAmount:       baseInt,
				Price:            pxInt,
				IsAsk:            isAsk,
				Type:             0,
				TimeInForce:      2,
				OrderExpiry:      exp,
			}, opts)
			if e != nil {
				return 0, "", e
			}
			s, e := ti.GetTxInfo()
			return ti.GetTxType(), s, e
		})
		if err != nil {
			mu.Lock()
			delete(activeOrds, sym+"_"+side)
			mu.Unlock()
		}
	}()
}

func manageCoin(sym string) {
	mu.Lock()
	defer mu.Unlock()
	mid := bnMid[sym]
	lb, la := ltBid[sym], ltAsk[sym]
	if mid <= 0 || lb <= 0 || la <= 0 {
		return
	}
	posUSD := positions[sym] * mid

	for _, side := range []string{"BID", "ASK"} {
		ao := activeOrds[sym+"_"+side]
		if ao != nil {
			isClose := (side == "BID" && posUSD <= -5.0) || (side == "ASK" && posUSD >= 5.0)
			moveBps := math.Abs(mid-ao.RefMid) / ao.RefMid * 10000.0
			ageS := time.Since(ao.TS).Seconds()
			tooClose := (side == "BID" && ao.Px >= la) || (side == "ASK" && ao.Px <= lb)
			if (!isClose && (moveBps >= CancelBps || ageS >= 45.0)) || tooClose {
				cancelSide(sym, side)
			}
		}
	}

	ema := bnEma[sym]
	impBps := 0.0
	if ema > 0 {
		impBps = (mid - ema) / ema * 10000.0
	}
	allowBid := posUSD < 5.0 && impBps >= -ImpulseBps
	allowAsk := posUSD > -5.0 && impBps <= ImpulseBps
	ae := avgEntry[sym]
	m := markets[sym]
	tick := math.Pow10(-m.PxDec)

	// Sanity-check Lighter BBO (WS delta can occasionally send deep level; fall back to mid if >0.5% away)
	saneLB := lb
	if math.Abs(lb-mid)/mid > 0.005 {
		saneLB = mid
	}
	saneLA := la
	if math.Abs(la-mid)/mid > 0.005 {
		saneLA = mid
	}

	wantBid := roundTo(math.Min(saneLB, mid*(1.0-EdgeBps/10000.0)), m.PxDec)
	if posUSD <= -5.0 && ae > 0 {
		pBid := roundTo(math.Min(saneLB, ae*(1.0-CloseEdgeBps/10000.0)), m.PxDec)
		wantBid = pBid
	}
	wantAsk := roundTo(math.Max(saneLA, mid*(1.0+EdgeBps/10000.0)), m.PxDec)
	if posUSD >= 5.0 && ae > 0 {
		pAsk := roundTo(math.Max(saneLA, ae*(1.0+CloseEdgeBps/10000.0)), m.PxDec)
		wantAsk = pAsk
	}
	if wantBid >= saneLA {
		wantBid = roundTo(saneLA-tick, m.PxDec)
	}
	if wantAsk <= saneLB {
		wantAsk = roundTo(saneLB+tick, m.PxDec)
	}

	now := time.Now()
	if allowBid && activeOrds[sym+"_BID"] == nil && now.Sub(lastOrdTS[sym+"_BID"]) >= 5*time.Second {
		lastOrdTS[sym+"_BID"] = now
		placeSide(sym, "BID", wantBid, mid)
	}
	if allowAsk && activeOrds[sym+"_ASK"] == nil && now.Sub(lastOrdTS[sym+"_ASK"]) >= 5*time.Second {
		lastOrdTS[sym+"_ASK"] = now
		placeSide(sym, "ASK", wantAsk, mid)
	}

	if now.Sub(lastLog) >= 20*time.Second {
		lastLog = now
		fmt.Printf("[%s] GO-TOKYO Eq=$%.4f | Pos=%v | Active=%d\n", now.Format("15:04:05"), eqNow, positions, len(activeOrds))
	}
}

func syncAccountLoop() {
	hc := &http.Client{Timeout: 6 * time.Second}
	url := fmt.Sprintf("%s/api/v1/account?by=index&value=%d", BaseURL, AcctIdx)
	for {
		resp, err := hc.Get(url)
		if err == nil {
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
				eq, _ := strconv.ParseFloat(a.TotalAssetValue, 64)
				if eq <= 0 {
					eq, _ = strconv.ParseFloat(a.Collateral, 64)
				}
				mu.Lock()
				if eq > 0 {
					eqNow = eq
				}
				for _, p := range a.Positions {
					if sym, ok := idToSym[p.MarketID]; ok {
						sz, _ := strconv.ParseFloat(p.Position, 64)
						if p.Sign < 0 && sz > 0 {
							sz = -sz
						}
						positions[sym] = sz
						ae, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
						avgEntry[sym] = ae
					}
				}
				mu.Unlock()
				if eq > 0 && eq <= StopEqUSD {
					fmt.Printf("[STOP-LOSS] Equity $%.4f <= $%.2f, halting!\n", eq, StopEqUSD)
					cancelAll()
					time.Sleep(1 * time.Second)
					os.Exit(0)
				}
			}
		}
		time.Sleep(2500 * time.Millisecond)
	}
}

func runLtWS() {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WsURL, nil)
		if err != nil {
			fmt.Printf("[LT WS DIAL ERR] %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if tc, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		wsMu.Lock()
		ltConn = conn
		wsMu.Unlock()
		for _, m := range markets {
			_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
		}
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if !strings.Contains(string(msg), "order_book") && (strings.Contains(string(msg), `"error"`) || (strings.Contains(string(msg), `"code":`) && !strings.Contains(string(msg), `"code":200`))) {
				fmt.Printf("[WS_RESP] %s\n", string(msg))
				if strings.Contains(string(msg), "invalid nonce") {
					if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
						nonceSeq.Store(n)
					}
				}
			}
			var m struct {
				Channel   string `json:"channel"`
				OrderBook struct {
					Bids []struct{ Price string `json:"price"` } `json:"bids"`
					Asks []struct{ Price string `json:"price"` } `json:"asks"`
				} `json:"order_book"`
			}
			if json.Unmarshal(msg, &m) == nil && strings.HasPrefix(m.Channel, "order_book:") {
				idStr := strings.TrimPrefix(m.Channel, "order_book:")
				midID, _ := strconv.ParseInt(idStr, 10, 64)
				if sym, ok := idToSym[midID]; ok && len(m.OrderBook.Bids) > 0 && len(m.OrderBook.Asks) > 0 {
					bp, _ := strconv.ParseFloat(m.OrderBook.Bids[0].Price, 64)
					ap, _ := strconv.ParseFloat(m.OrderBook.Asks[0].Price, 64)
					mu.Lock()
					ltBid[sym] = bp
					ltAsk[sym] = ap
					mu.Unlock()
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
	url := "wss://fstream.binance.com/stream?streams=ethusdt@bookTicker/solusdt@bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			fmt.Printf("[BN WS DIAL ERR] %v\n", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if tc, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
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
				a, _ := strconv.ParseFloat(env.Data.A, 64)
				if b > 0 && a > 0 {
					mVal := 0.5 * (b + a)
					mu.Lock()
					bnMid[sym] = mVal
					if bnEma[sym] == 0 {
						bnEma[sym] = mVal
					} else {
						bnEma[sym] = bnEma[sym]*0.95 + mVal*0.05
					}
					mu.Unlock()
					manageCoin(sym)
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

	go runLtWS()
	time.Sleep(1500 * time.Millisecond)
	cancelAll()
	fmt.Printf("GO-TOKYO Lighter Bot LIVE | Acct #%d | Nonce=%d | Clip=$%.2f | Edge=%.1fbps | Stop=$%.2f\n", AcctIdx, n, ClipUSD, EdgeBps, StopEqUSD)
	go syncAccountLoop()
	runBnWS()
}
