package main

import (
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	ethcrypto "github.com/ethereum/go-ethereum/crypto"
	"github.com/gorilla/websocket"
)

const (
	AsterBaseURL      = "https://fapi.asterdex.com"
	AsterMainUser     = "0x07d8B4181cE22Bf284fD6E2E0dFD9a8e8eE97B94"
	AsterSignerAddr   = "0x0F788144755A526D85f6B18635054E0e8b2a4033"
	AsterPrivKeyHex   = "05b38ba01cf41d875a2d5d82b22d3a87136c84edc50505bbe6911e2a444263eb"
	AsterChainID      = uint64(1666)
	AsterAcctOutPath  = "/tmp/aster_acct.json"
	LiveArenaOutPath  = "/tmp/live_arena.json"
	LiveArenaSavePath = "/opt/crypto-arb-bot/live_arena_state.json"
)

type AsterSpec struct {
	Sym        string
	AsterSym   string
	IsRWA      bool
	MakerFeeBp float64
	TakerFeeBp float64
	QDec       int
	PxDec      int
	Step       float64
	Tick       float64
	MinNot     float64
}

type AsterWalletSnap struct {
	TsMs             int64   `json:"ts"`
	WalletBalance    float64 `json:"walletBalance"`
	MarginBalance    float64 `json:"marginBalance"`
	AvailableBalance float64 `json:"availableBalance"`
	UnrealizedProfit float64 `json:"unrealizedProfit"`
}

var (
	asterPrivKey     *ecdsa.PrivateKey
	asterDomainSep   []byte
	asterMsgTypeHash []byte
	asterNonceSeq    atomic.Int64
	asterTimeOffset  atomic.Int64 // serverMs - localMs

	asterMu      sync.Mutex
	asterSpecs   = map[string]AsterSpec{} // key: AsterSym (e.g. NVDAUSDT)
	asterBooks   = map[string]*DnBook{}   // key: AsterSym
	asterLevSet  = map[string]bool{}
	asterAvail   float64
	asterWallet  AsterWalletSnap
	asterOrdMu   sync.Mutex
	asterOrdMap  = map[int64]BnOrderState{}
)

func initAsterEIP712() error {
	pk, err := ethcrypto.HexToECDSA(AsterPrivKeyHex)
	if err != nil {
		return err
	}
	asterPrivKey = pk

	domainTypeHash := ethcrypto.Keccak256([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	nameHash := ethcrypto.Keccak256([]byte("AsterSignTransaction"))
	verHash := ethcrypto.Keccak256([]byte("1"))
	chainBuf := make([]byte, 32)
	binary.BigEndian.PutUint64(chainBuf[24:], AsterChainID)
	contractBuf := make([]byte, 32)

	buf := make([]byte, 0, 160)
	buf = append(buf, domainTypeHash...)
	buf = append(buf, nameHash...)
	buf = append(buf, verHash...)
	buf = append(buf, chainBuf...)
	buf = append(buf, contractBuf...)
	asterDomainSep = ethcrypto.Keccak256(buf)
	asterMsgTypeHash = ethcrypto.Keccak256([]byte("Message(string msg)"))

	syncAsterServerTime()
	return nil
}

func syncAsterServerTime() {
	resp, err := bnHTTP.Get(AsterBaseURL + "/fapi/v3/time")
	if err != nil {
		return
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var st struct {
		ServerTime int64 `json:"serverTime"`
	}
	if json.Unmarshal(b, &st) == nil && st.ServerTime > 0 {
		asterTimeOffset.Store(st.ServerTime - time.Now().UnixMilli())
	}
}

func nextAsterNonceStr() string {
	nowUs := (time.Now().UnixMilli() + asterTimeOffset.Load()) * 1000
	for {
		prev := asterNonceSeq.Load()
		next := nowUs
		if next <= prev {
			next = prev + 1
		}
		if asterNonceSeq.CompareAndSwap(prev, next) {
			return strconv.FormatInt(next, 10)
		}
	}
}

func signAsterQueryEIP712(qs string) (string, error) {
	msgHash := ethcrypto.Keccak256([]byte(qs))
	sb := make([]byte, 0, 64)
	sb = append(sb, asterMsgTypeHash...)
	sb = append(sb, msgHash...)
	structHash := ethcrypto.Keccak256(sb)

	db := make([]byte, 0, 66)
	db = append(db, 0x19, 0x01)
	db = append(db, asterDomainSep...)
	db = append(db, structHash...)
	digest := ethcrypto.Keccak256(db)

	sig, err := ethcrypto.Sign(digest, asterPrivKey)
	if err != nil {
		return "", err
	}
	if sig[64] < 27 {
		sig[64] += 27
	}
	return "0x" + hex.EncodeToString(sig), nil
}

func asterSignReq(method, path string, params url.Values) (int, []byte, error) {
	if asterPrivKey == nil {
		return 0, nil, fmt.Errorf("aster key nil")
	}
	if params == nil {
		params = url.Values{}
	}
	params.Set("nonce", nextAsterNonceStr())
	params.Set("signer", AsterSignerAddr)
	params.Set("user", AsterMainUser)
	qs := params.Encode() // alphabetically sorted & URL-encoded
	sig, err := signAsterQueryEIP712(qs)
	if err != nil {
		return 0, nil, err
	}
	signedPayload := qs + "&signature=" + sig

	var req *http.Request
	if method == "GET" || method == "DELETE" {
		req, err = http.NewRequest(method, AsterBaseURL+path+"?"+signedPayload, nil)
	} else {
		req, err = http.NewRequest(method, AsterBaseURL+path, strings.NewReader(signedPayload))
		if err == nil {
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	}
	if err != nil {
		return 0, nil, err
	}
	resp, err := bnHTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp.StatusCode, b, nil
}

func discoverAsterMarkets() {
	resp, err := bnHTTP.Get(AsterBaseURL + "/fapi/v3/exchangeInfo")
	if err != nil {
		fmt.Printf("[ASTER_DISCOVER_ERR] %v\n", err)
		return
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	var ex struct {
		Symbols []struct {
			Symbol    string `json:"symbol"`
			Status    string `json:"status"`
			BaseAsset string `json:"baseAsset"`
			Filters   []struct {
				FilterType string `json:"filterType"`
				StepSize   string `json:"stepSize"`
				TickSize   string `json:"tickSize"`
				Notional   string `json:"notional"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if json.Unmarshal(b, &ex) != nil {
		return
	}

	rwaBases := map[string]bool{
		"NVDA": true, "AAPL": true, "TSLA": true, "QQQ": true, "SPY": true,
		"AMZN": true, "META": true, "COIN": true, "MSTR": true, "GOOGL": true,
		"AMD": true, "INTC": true, "XAU": true, "HOOD": true, "PLTR": true,
	}
	loaded := 0
	for _, s := range ex.Symbols {
		if s.Status != "TRADING" || !strings.HasSuffix(s.Symbol, "USDT") {
			continue
		}
		m, okM := markets[s.BaseAsset]
		if !okM {
			continue
		}
		var step, tick, minNot float64 = 0.01, 0.01, 5.5
		qDec, pxDec := 2, 2
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
		isRWA := rwaBases[s.BaseAsset]
		tFee := 4.0
		if isRWA {
			tFee = 1.25
		}
		sp := AsterSpec{
			Sym:        s.BaseAsset,
			AsterSym:   s.Symbol,
			IsRWA:      isRWA,
			MakerFeeBp: 0.0,
			TakerFeeBp: tFee,
			QDec:       qDec,
			PxDec:      pxDec,
			Step:       step,
			Tick:       tick,
			MinNot:     minNot,
		}
		asterMu.Lock()
		asterSpecs[s.Symbol] = sp
		asterMu.Unlock()
		ensureLtSubscribed(m.ID)
		loaded++
	}
	fmt.Printf("[ASTER_DISCOVER] Loaded %d common Lighter<->AsterDEX contracts (0%% Maker / 1.25bp RWA / 4bp Crypto)\n", loaded)
}

func fetchAsterAccountMargin() (avail, marginBal float64, ok bool) {
	sc, body, err := asterSignReq("GET", "/fapi/v3/account", nil)
	if err != nil || sc != 200 {
		return 0, 0, false
	}
	var acc struct {
		TotalWalletBalance string `json:"totalWalletBalance"`
		TotalMarginBalance string `json:"totalMarginBalance"`
		AvailableBalance   string `json:"availableBalance"`
		TotalUnPnl         string `json:"totalUnrealizedProfit"`
	}
	if json.Unmarshal(body, &acc) != nil {
		return 0, 0, false
	}
	wb, _ := strconv.ParseFloat(acc.TotalWalletBalance, 64)
	mb, _ := strconv.ParseFloat(acc.TotalMarginBalance, 64)
	av, _ := strconv.ParseFloat(acc.AvailableBalance, 64)
	up, _ := strconv.ParseFloat(acc.TotalUnPnl, 64)

	snap := AsterWalletSnap{
		TsMs:             time.Now().UnixMilli(),
		WalletBalance:    roundTo(wb, 4),
		MarginBalance:    roundTo(mb, 4),
		AvailableBalance: roundTo(av, 4),
		UnrealizedProfit: roundTo(up, 4),
	}
	asterMu.Lock()
	asterAvail = av
	asterWallet = snap
	asterMu.Unlock()

	if jb, e2 := json.Marshal(snap); e2 == nil {
		_ = os.WriteFile(AsterAcctOutPath+".tmp", jb, 0644)
		_ = os.Rename(AsterAcctOutPath+".tmp", AsterAcctOutPath)
	}
	return av, mb, true
}

func ensureAsterLeverage(asterSym string, lev int) {
	asterMu.Lock()
	done := asterLevSet[asterSym]
	asterMu.Unlock()
	if !done {
		p := url.Values{}
		p.Set("symbol", asterSym)
		p.Set("leverage", strconv.Itoa(lev))
		sc, _, err := asterSignReq("POST", "/fapi/v3/leverage", p)
		if err == nil && sc == 200 {
			asterMu.Lock()
			asterLevSet[asterSym] = true
			asterMu.Unlock()
		}
	}
}

func flattenAsterAllSync() {
	if sc, ooB, err := asterSignReq("GET", "/fapi/v3/openOrders", nil); err == nil && sc == 200 {
		var oos []struct {
			Symbol string `json:"symbol"`
		}
		if json.Unmarshal(ooB, &oos) == nil {
			seen := map[string]bool{}
			for _, oo := range oos {
				if oo.Symbol != "" && !seen[oo.Symbol] {
					seen[oo.Symbol] = true
					cp := url.Values{}
					cp.Set("symbol", oo.Symbol)
					_, _, _ = asterSignReq("DELETE", "/fapi/v3/allOpenOrders", cp)
				}
			}
		}
	}
	if sc, prB, err := asterSignReq("GET", "/fapi/v3/positionRisk", nil); err == nil && sc == 200 {
		var prs []struct {
			Symbol string `json:"symbol"`
			Amt    string `json:"positionAmt"`
		}
		if json.Unmarshal(prB, &prs) == nil {
			for _, pr := range prs {
				amt, _ := strconv.ParseFloat(pr.Amt, 64)
				if math.Abs(amt) > 1e-6 {
					cp := url.Values{}
					cp.Set("symbol", pr.Symbol)
					_, _, _ = asterSignReq("DELETE", "/fapi/v3/allOpenOrders", cp)
					cSide := "SELL"
					if amt < 0 {
						cSide = "BUY"
					}
					rawQ := strings.TrimPrefix(strings.TrimSpace(pr.Amt), "-")
					if strings.Contains(rawQ, ".") {
						rawQ = strings.TrimRight(strings.TrimRight(rawQ, "0"), ".")
					}
					mp := url.Values{}
					mp.Set("symbol", pr.Symbol)
					mp.Set("side", cSide)
					mp.Set("type", "MARKET")
					mp.Set("reduceOnly", "true")
					mp.Set("quantity", rawQ)
					fSc, fB, fE := asterSignReq("POST", "/fapi/v3/order", mp)
					fmt.Printf("[ASTER_FLAT_SYNC sym=%s side=%s qty=%s sc=%d err=%v body=%s]\n",
						pr.Symbol, cSide, rawQ, fSc, fE, string(fB))
				}
			}
		}
	}
}

func runAsterBookTickerWS() {
	wsURL := "wss://fstream.asterdex.com/stream?streams=!bookTicker"
	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(25 * time.Second))
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
			b, _ := strconv.ParseFloat(env.Data.B, 64)
			a, _ := strconv.ParseFloat(env.Data.A, 64)
			if b <= 0 || a <= b {
				continue
			}
			bq, _ := strconv.ParseFloat(env.Data.BQ, 64)
			aq, _ := strconv.ParseFloat(env.Data.AQ, 64)

			asterMu.Lock()
			sp, tracked := asterSpecs[env.Data.S]
			if !tracked {
				asterMu.Unlock()
				continue
			}
			now := time.Now()
			mid := 0.5 * (b + a)
			db := asterBooks[env.Data.S]
			if db == nil {
				db = &DnBook{SnapMid: mid, SnapTS: now}
				asterBooks[env.Data.S] = db
			}
			db.Bid, db.Ask, db.BidQ, db.AskQ, db.TS = b, a, bq, aq, now
			if db.SnapMid <= 0 {
				db.SnapMid, db.SnapTS = mid, now
			} else {
				db.LastJumpBps = (mid - db.SnapMid) / db.SnapMid * 10000.0
				if now.Sub(db.SnapTS) >= 250*time.Millisecond {
					db.SnapMid, db.SnapTS = mid, now
				}
			}
			asterMu.Unlock()

			// Update rolling 90s EWMA equilibrium basis vs Lighter:
			if st := mktStates[sp.Sym]; st != nil {
				st.mu.Lock()
				ltB, ltA, ltTS := st.ltBid, st.ltAsk, st.lastBookTS
				st.mu.Unlock()
				if ltB > 0 && ltA > ltB && now.Sub(ltTS) < 20*time.Second {
					dnMu.Lock()
					fx, okFx := dnQuoteFxToUsdtLocked("USDT", now)
					dnMu.Unlock()
					if !okFx || fx <= 0 {
						fx = 1.001
					}
					asMidUsd := mid / fx
					ltMid := 0.5 * (ltB + ltA)
					ltSp := (ltA - ltB) / ltMid * 10000.0
					asSp := (a - b) / mid * 10000.0
					rawBasis := (ltMid - asMidUsd) / asMidUsd * 10000.0
					asterMu.Lock()
					if ltSp <= 25.0 && asSp <= 18.0 && (db.WarmTicks == 0 || now.Sub(db.LastEmaTS) >= 500*time.Millisecond) {
						if db.WarmTicks == 0 {
							db.EmaBps = rawBasis
						} else {
							dt := math.Min(8.0, math.Max(0.2, now.Sub(db.LastEmaTS).Seconds()))
							db.EmaBps += (1.0 - math.Exp(-dt/90.0)) * (rawBasis - db.EmaBps)
						}
						db.LastEmaTS = now
						db.WarmTicks++
					}
					asterMu.Unlock()
				}
			}
			wakeEngine()
		}
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func runAsterUserWS() {
	for {
		sc, body, err := asterSignReq("POST", "/fapi/v3/listenKey", nil)
		if err != nil || sc != 200 {
			time.Sleep(3 * time.Second)
			continue
		}
		var lk struct {
			ListenKey string `json:"listenKey"`
		}
		if json.Unmarshal(body, &lk) != nil || lk.ListenKey == "" {
			time.Sleep(3 * time.Second)
			continue
		}
		conn, _, err := websocket.DefaultDialer.Dial("wss://fstream.asterdex.com/ws/"+lk.ListenKey, nil)
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}
		setConnNoDelay(conn)
		fmt.Println("[ASTER_USER_WS] Connected (real-time ORDER_TRADE_UPDATE active)")

		stopRenew := make(chan struct{})
		go func() {
			t := time.NewTicker(25 * time.Minute)
			defer t.Stop()
			for {
				select {
				case <-stopRenew:
					return
				case <-t.C:
					_, _, _ = asterSignReq("PUT", "/fapi/v3/listenKey", nil)
					syncAsterServerTime()
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
				asterOrdMu.Lock()
				if len(asterOrdMap) > 400 {
					for k, v := range asterOrdMap {
						if time.Since(v.UpdatedAt) > 10*time.Minute {
							delete(asterOrdMap, k)
						}
					}
				}
				asterOrdMap[ev.O.ID] = BnOrderState{
					Status:    ev.O.Status,
					ExecQty:   eq,
					AvgPx:     ap,
					UpdatedAt: time.Now(),
				}
				asterOrdMu.Unlock()
				wakeEngine()
			}
		}
		close(stopRenew)
		_ = conn.Close()
		time.Sleep(500 * time.Millisecond)
	}
}

func queryAsterOrder(symbol string, oid int64, lastPoll time.Time, forceREST bool) (status string, execQty, avgPx float64, polledREST, ok bool) {
	asterOrdMu.Lock()
	ws, hasWS := asterOrdMap[oid]
	asterOrdMu.Unlock()
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
	sc, body, err := asterSignReq("GET", "/fapi/v3/order", qp)
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
