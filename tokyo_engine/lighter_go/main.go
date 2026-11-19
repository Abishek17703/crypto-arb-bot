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
	EdgeBps      = 7.5 // High-selectivity 7.5 bps entry around microprice
	CancelBps    = 1.0 // Instant 1.0 bps anti-trend cancel shield
	ObiLimit     = 0.45 // Block toxic-flow side when |OBI| > 0.45
	StopEqUSD    = 14.50
)

type Market struct {
	Sym        string
	ID         int64
	SzDec      int
	PxDec      int
	MinB       float64
	AllowEntry bool
}

var markets = map[string]Market{
	"ETH": {Sym: "ETH", ID: 0, SzDec: 4, PxDec: 2, MinB: 0.0020, AllowEntry: true},
	"BTC": {Sym: "BTC", ID: 1, SzDec: 5, PxDec: 1, MinB: 0.00007, AllowEntry: true},
	"SOL": {Sym: "SOL", ID: 2, SzDec: 3, PxDec: 3, MinB: 0.055, AllowEntry: false}, // Close existing only
}
var idToSym = map[int64]string{0: "ETH", 1: "BTC", 2: "SOL"}

type ActiveOrder struct {
	COI     int64
	Px      float64
	BaseInt int64
	RefMid  float64
	IsClose bool
	TS      time.Time
}

var (
	mu         sync.Mutex
	bnMid      = map[string]float64{}
	bnMicro    = map[string]float64{}
	bnObi      = map[string]float64{}
	emaFast    = map[string]float64{} // 20s EMA
	emaSlow    = map[string]float64{} // 180s EMA
	lastEmaTS  = map[string]time.Time{}
	bookBids   = map[string]map[string]float64{"ETH": {}, "BTC": {}, "SOL": {}}
	bookAsks   = map[string]map[string]float64{"ETH": {}, "BTC": {}, "SOL": {}}
	ltBid      = map[string]float64{}
	ltAsk      = map[string]float64{}
	positions  = map[string]float64{}
	avgEntry   = map[string]float64{}
	posOpenTS  = map[string]time.Time{}
	trendMode  = map[string]bool{} // True if BTC entered via 2:1 trend-start
	activeOrds = map[string]*ActiveOrder{}
	lastOrdTS  = map[string]time.Time{}
	eqNow      = 15.03
	lastLog    = time.Time{}

	txCli    *client.TxClient
	nonceSeq atomic.Int64
	coiSeq   atomic.Int64
	txMu     sync.Mutex

	wsMu      sync.Mutex
	ltConn    *websocket.Conn
	useWsSend = true
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
	if useWsSend {
		wsMu.Lock()
		c := ltConn
		var wsErr error
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

func modifySide(sym, side string, newPxF, midF float64) {
	key := sym + "_" + side
	ao, ok := activeOrds[key]
	if !ok {
		return
	}
	m := markets[sym]
	pxInt := uint32(math.Round(newPxF * math.Pow10(m.PxDec)))
	ao.Px = newPxF
	ao.RefMid = midF
	ao.TS = time.Now()
	go func(coi int64, mid, baseInt int64) {
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
			mu.Lock()
			delete(activeOrds, key)
			mu.Unlock()
		}
	}(ao.COI, m.ID, ao.BaseInt)
}

func placeSide(sym, side string, pxF, midF, customSz float64, isClose bool) {
	m := markets[sym]
	baseF := customSz
	if baseF <= 0 {
		baseF = math.Max(m.MinB, ClipUSD/pxF)
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
	activeOrds[sym+"_"+side] = &ActiveOrder{COI: coi, Px: pxF, BaseInt: baseInt, RefMid: midF, IsClose: isClose, TS: time.Now()}
	go func() {
		exp := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
		dbg := fmt.Sprintf("%s_%s px=%.4f ro=%d", sym, side, pxF, redOnly)
		err := sendSignedTx(fmt.Sprintf("o_%d", coi), dbg, func(opts *types.TransactOpts) (uint8, string, error) {
			ti, e := txCli.GetCreateOrderTransaction(&types.CreateOrderTxReq{
				MarketIndex:      m.ID,
				ClientOrderIndex: coi,
				BaseAmount:       baseInt,
				Price:            pxInt,
				IsAsk:            isAsk,
				Type:             0,
				TimeInForce:      2,
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
			mu.Lock()
			delete(activeOrds, sym+"_"+side)
			mu.Unlock()
		}
	}()
}

func recomputeBBO(sym string) {
	bestB, bestA := 0.0, 0.0
	for ps, sz := range bookBids[sym] {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if p > bestB {
				bestB = p
			}
		}
	}
	for ps, sz := range bookAsks[sym] {
		if sz > 0 {
			p, _ := strconv.ParseFloat(ps, 64)
			if bestA == 0 || p < bestA {
				bestA = p
			}
		}
	}
	if bestB > 0 {
		ltBid[sym] = bestB
	}
	if bestA > 0 {
		ltAsk[sym] = bestA
	}
}

func closeStepBps(sym string) float64 {
	if trendMode[sym] {
		return 24.0 // 2:1 trend-start target on BTC
	}
	if t0, ok := posOpenTS[sym]; ok && !t0.IsZero() {
		mins := time.Since(t0).Minutes()
		if mins >= 30.0 {
			return 0.0 // Breakeven unstick after 30m
		}
		if mins >= 15.0 {
			return 0.5
		}
	}
	return 2.0
}

func manageCoin(sym string) {
	mu.Lock()
	defer mu.Unlock()
	mid := bnMid[sym]
	micro := bnMicro[sym]
	if micro <= 0 {
		micro = mid
	}
	obi := bnObi[sym]
	lb, la := ltBid[sym], ltAsk[sym]
	if mid <= 0 || lb <= 0 || la <= 0 || lb >= la {
		return
	}
	pos := positions[sym]
	posUSD := pos * mid
	totalNetUSD := positions["ETH"]*bnMid["ETH"] + positions["BTC"]*bnMid["BTC"] + positions["SOL"]*bnMid["SOL"]
	m := markets[sym]
	tick := math.Pow10(-m.PxDec)
	now := time.Now()

	ef, es := emaFast[sym], emaSlow[sym]
	trendBps, impBps := 0.0, 0.0
	if ef > 0 && es > 0 {
		trendBps = (ef - es) / es * 10000.0
		impBps = (mid - ef) / ef * 10000.0
	}
	// High-quality entry filters: Anti-trend shield + OBI toxic-flow filter + net delta cap
	bidSafe := m.AllowEntry && trendBps >= -1.5 && impBps >= -1.5 && obi >= -ObiLimit && totalNetUSD < 6.0
	askSafe := m.AllowEntry && trendBps <= 1.5 && impBps <= 1.5 && obi <= ObiLimit && totalNetUSD > -6.0

	// BTC Trend-Start breakout override (only when flat & strong impulse + OBI confirm)
	btcTrendBuy := sym == "BTC" && math.Abs(posUSD) < 5.0 && impBps >= 12.0 && trendBps > 0 && obi >= 0.25
	btcTrendSell := sym == "BTC" && math.Abs(posUSD) < 5.0 && impBps <= -12.0 && trendBps < 0 && obi <= -0.25

	// 1. Manage resting orders: ModifyOrder (1 write) or Cancel on toxic flow
	for _, side := range []string{"BID", "ASK"} {
		ao := activeOrds[sym+"_"+side]
		if ao == nil {
			continue
		}
		moveBps := math.Abs(mid-ao.RefMid) / ao.RefMid * 10000.0
		ageS := now.Sub(ao.TS).Seconds()
		tooClose := (side == "BID" && ao.Px >= la) || (side == "ASK" && ao.Px <= lb)
		if !ao.IsClose {
			safeNow := (side == "BID" && (bidSafe || btcTrendBuy)) || (side == "ASK" && (askSafe || btcTrendSell))
			if !safeNow {
				cancelSide(sym, side)
			} else if (moveBps >= CancelBps || ageS >= 35.0 || tooClose) && now.Sub(lastOrdTS[sym+"_"+side]) >= 3*time.Second {
				newPx := roundTo(math.Min(lb, micro*(1.0-EdgeBps/10000.0)), m.PxDec)
				if side == "ASK" {
					newPx = roundTo(math.Max(la, micro*(1.0+EdgeBps/10000.0)), m.PxDec)
				}
				if side == "BID" && newPx >= la {
					newPx = roundTo(la-tick, m.PxDec)
				}
				if side == "ASK" && newPx <= lb {
					newPx = roundTo(lb+tick, m.PxDec)
				}
				lastOrdTS[sym+"_"+side] = now
				modifySide(sym, side, newPx, mid)
			}
		} else if tooClose {
			cancelSide(sym, side)
		}
	}

	ae := avgEntry[sym]
	stepBps := closeStepBps(sym)

	// 2. Closing orders (with time-based breakeven step-down, or 2:1 BTC trend SL/TP)
	if posUSD <= -5.0 && ae > 0 {
		if trendMode[sym] && mid >= ae*(1.0+12.0/10000.0) {
			// BTC trend-start -12 bps stop-out
			if activeOrds[sym+"_BID"] == nil && now.Sub(lastOrdTS[sym+"_BID"]) >= 3*time.Second {
				lastOrdTS[sym+"_BID"] = now
				placeSide(sym, "BID", roundTo(lb, m.PxDec), mid, math.Abs(pos), true)
			}
		} else {
			wantBid := roundTo(math.Min(lb, ae*(1.0-stepBps/10000.0)), m.PxDec)
			if wantBid >= la {
				wantBid = roundTo(la-tick, m.PxDec)
			}
			if ao := activeOrds[sym+"_BID"]; ao != nil && ao.IsClose && ao.Px < wantBid-tick*2 && now.Sub(lastOrdTS[sym+"_BID"]) >= 10*time.Second {
				lastOrdTS[sym+"_BID"] = now
				modifySide(sym, "BID", wantBid, mid)
			} else if ao == nil && now.Sub(lastOrdTS[sym+"_BID"]) >= 4*time.Second {
				lastOrdTS[sym+"_BID"] = now
				placeSide(sym, "BID", wantBid, mid, math.Abs(pos), true)
			}
		}
	} else if posUSD < 5.0 && (bidSafe || btcTrendBuy) {
		eBps := EdgeBps
		if btcTrendBuy {
			eBps = 1.0
			trendMode[sym] = true
		} else {
			trendMode[sym] = false
		}
		wantBid := roundTo(math.Min(lb, micro*(1.0-eBps/10000.0)), m.PxDec)
		if wantBid >= la {
			wantBid = roundTo(la-tick, m.PxDec)
		}
		if activeOrds[sym+"_BID"] == nil && now.Sub(lastOrdTS[sym+"_BID"]) >= 5*time.Second {
			lastOrdTS[sym+"_BID"] = now
			placeSide(sym, "BID", wantBid, mid, 0, false)
		}
	}

	if posUSD >= 5.0 && ae > 0 {
		if trendMode[sym] && mid <= ae*(1.0-12.0/10000.0) {
			if activeOrds[sym+"_ASK"] == nil && now.Sub(lastOrdTS[sym+"_ASK"]) >= 3*time.Second {
				lastOrdTS[sym+"_ASK"] = now
				placeSide(sym, "ASK", roundTo(la, m.PxDec), mid, math.Abs(pos), true)
			}
		} else {
			wantAsk := roundTo(math.Max(la, ae*(1.0+stepBps/10000.0)), m.PxDec)
			if wantAsk <= lb {
				wantAsk = roundTo(lb+tick, m.PxDec)
			}
			if ao := activeOrds[sym+"_ASK"]; ao != nil && ao.IsClose && ao.Px > wantAsk+tick*2 && now.Sub(lastOrdTS[sym+"_ASK"]) >= 10*time.Second {
				lastOrdTS[sym+"_ASK"] = now
				modifySide(sym, "ASK", wantAsk, mid)
			} else if ao == nil && now.Sub(lastOrdTS[sym+"_ASK"]) >= 4*time.Second {
				lastOrdTS[sym+"_ASK"] = now
				placeSide(sym, "ASK", wantAsk, mid, math.Abs(pos), true)
			}
		}
	} else if posUSD > -5.0 && (askSafe || btcTrendSell) {
		eBps := EdgeBps
		if btcTrendSell {
			eBps = 1.0
			trendMode[sym] = true
		} else {
			trendMode[sym] = false
		}
		wantAsk := roundTo(math.Max(la, micro*(1.0+eBps/10000.0)), m.PxDec)
		if wantAsk <= lb {
			wantAsk = roundTo(lb+tick, m.PxDec)
		}
		if activeOrds[sym+"_ASK"] == nil && now.Sub(lastOrdTS[sym+"_ASK"]) >= 5*time.Second {
			lastOrdTS[sym+"_ASK"] = now
			placeSide(sym, "ASK", wantAsk, mid, 0, false)
		}
	}

	if now.Sub(lastLog) >= 25*time.Second {
		lastLog = now
		fmt.Printf("[%s] GO-TOKYO v3 Eq=$%.4f | Pos=%v | Active=%d | OBI_E=%.2f OBI_B=%.2f\n",
			now.Format("15:04:05"), eqNow, positions, len(activeOrds), bnObi["ETH"], bnObi["BTC"])
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
						prevPos := positions[sym]
						positions[sym] = sz
						ae, _ := strconv.ParseFloat(p.AvgEntryPrice, 64)
						avgEntry[sym] = ae
						if math.Abs(sz) > 1e-6 && math.Abs(prevPos) < 1e-6 {
							posOpenTS[sym] = time.Now()
						} else if math.Abs(sz) < 1e-6 {
							posOpenTS[sym] = time.Time{}
							trendMode[sym] = false
						}
						if p.OpenOrderCount == 0 || math.Abs(sz-prevPos) > 1e-6 {
							for _, s := range []string{"BID", "ASK"} {
								if ao := activeOrds[sym+"_"+s]; ao != nil && time.Since(ao.TS) > 2*time.Second {
									delete(activeOrds, sym+"_"+s)
								}
							}
						}
					}
				}
				mu.Unlock()
				if eq > 0 && eq <= StopEqUSD {
					fmt.Printf("[STOP-LOSS] Equity $%.4f <= $%.2f, halting!\n", eq, StopEqUSD)
					useWsSend = false
					cancelAll()
					time.Sleep(1 * time.Second)
					os.Exit(0)
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
}

func runLtWS() {
	for {
		conn, _, err := websocket.DefaultDialer.Dial(WsURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		if tc, ok := conn.UnderlyingConn().(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		wsMu.Lock()
		ltConn = conn
		wsMu.Unlock()
		mu.Lock()
		bookBids = map[string]map[string]float64{"ETH": {}, "BTC": {}, "SOL": {}}
		bookAsks = map[string]map[string]float64{"ETH": {}, "BTC": {}, "SOL": {}}
		mu.Unlock()

		for _, m := range markets {
			_ = conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("order_book/%d", m.ID)})
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
					if strings.Contains(sMsg, "invalid nonce") {
						if n, e2 := txCli.HTTP().GetNextNonce(AcctIdx, KeyIdx); e2 == nil {
							nonceSeq.Store(n)
						}
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
			if json.Unmarshal(msg, &m) == nil && strings.HasPrefix(m.Channel, "order_book:") {
				idStr := strings.TrimPrefix(m.Channel, "order_book:")
				midID, _ := strconv.ParseInt(idStr, 10, 64)
				if sym, ok := idToSym[midID]; ok {
					mu.Lock()
					if strings.Contains(m.Type, "subscribed") {
						bookBids[sym] = map[string]float64{}
						bookAsks[sym] = map[string]float64{}
					}
					for _, b := range m.OrderBook.Bids {
						sz, _ := strconv.ParseFloat(b.Size, 64)
						if sz <= 0 {
							delete(bookBids[sym], b.Price)
						} else {
							bookBids[sym][b.Price] = sz
						}
					}
					for _, a := range m.OrderBook.Asks {
						sz, _ := strconv.ParseFloat(a.Size, 64)
						if sz <= 0 {
							delete(bookAsks[sym], a.Price)
						} else {
							bookAsks[sym][a.Price] = sz
						}
					}
					recomputeBBO(sym)
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
	url := "wss://fstream.binance.com/stream?streams=ethusdt@bookTicker/btcusdt@bookTicker/solusdt@bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
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
				bq, _ := strconv.ParseFloat(env.Data.BQ, 64)
				a, _ := strconv.ParseFloat(env.Data.A, 64)
				aq, _ := strconv.ParseFloat(env.Data.AQ, 64)
				if b > 0 && a > 0 && bq > 0 && aq > 0 {
					mVal := 0.5 * (b + a)
					microVal := (b*aq + a*bq) / (bq + aq)
					obiVal := (bq - aq) / (bq + aq)
					now := time.Now()
					mu.Lock()
					bnMid[sym] = mVal
					bnMicro[sym] = microVal
					bnObi[sym] = obiVal
					if emaFast[sym] == 0 {
						emaFast[sym] = mVal
						emaSlow[sym] = mVal
						lastEmaTS[sym] = now
					} else {
						dt := math.Min(5.0, math.Max(0.001, now.Sub(lastEmaTS[sym]).Seconds()))
						lastEmaTS[sym] = now
						emaFast[sym] += (1.0 - math.Exp(-dt/20.0)) * (mVal - emaFast[sym])
						emaSlow[sym] += (1.0 - math.Exp(-dt/180.0)) * (mVal - emaSlow[sym])
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
	fmt.Printf("GO-TOKYO v3 LIVE | ETH+BTC | Microprice+OBI | Breakeven StepDown | ModifyOrder+WS_TX | Stop=$%.2f\n", StopEqUSD)
	go syncAccountLoop()
	runBnWS()
}
