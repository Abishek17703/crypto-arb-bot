package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"time"
)

const (
	CarryMicroVer      = "CARRY-v1.0-MICRO-3X25"
	CarryClipUSD       = 25.0
	CarryHardStopUSD   = -4.00
	CarryMinEntryAPR   = 20.0
	CarryMaxAdverseBp  = 25.0
	CarryFlipExitAPR   = -5.0
	CarryBasisStopBps  = 150.0
)

var carryTargets = []struct {
	Sym   string
	BnSym string
}{
	{Sym: "MRNA", BnSym: "MRNAUSDT"},
	{Sym: "WLFI", BnSym: "WLFIUSDC"},
	{Sym: "ZEC", BnSym: "ZECUSDC"},
}

type carrySlot struct {
	Sym         string
	BnSym       string
	Open        bool
	Dir         string // "LONG_LT" or "SHORT_LT"
	Qty         float64
	LtEntryPx   float64
	BnEntryPx   float64
	OpenedAt    time.Time
	EntryApr    float64
	FlipStrikes int
	LastTry     time.Time
}

func fetchLiveFundingDiffs() (ltAprMap map[string]float64, bnAprMap map[string]float64, ok bool) {
	ltAprMap = map[string]float64{}
	bnAprMap = map[string]float64{}
	hc := &http.Client{Timeout: 8 * time.Second}

	// 1. Lighter funding rates (8h equiv rate -> hourly = rate/8 -> APR = rate/8 * 24 * 365 * 100)
	resp1, err := hc.Get(BaseURL + "/api/v1/funding-rates")
	if err != nil {
		return nil, nil, false
	}
	b1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	var ltFr struct {
		FundingRates []struct {
			Exchange string  `json:"exchange"`
			Symbol   string  `json:"symbol"`
			Rate     float64 `json:"rate"`
		} `json:"funding_rates"`
	}
	if json.Unmarshal(b1, &ltFr) != nil {
		return nil, nil, false
	}
	for _, x := range ltFr.FundingRates {
		if x.Exchange == "lighter" {
			ltAprMap[x.Symbol] = (x.Rate / 8.0) * 24.0 * 365.0 * 100.0
		}
	}

	// 2. Binance funding intervals (/fapi/v1/fundingInfo)
	bnInt := map[string]float64{}
	if resp2, err2 := hc.Get("https://fapi.binance.com/fapi/v1/fundingInfo"); err2 == nil {
		b2, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		var fi []struct {
			Symbol               string  `json:"symbol"`
			FundingIntervalHours float64 `json:"fundingIntervalHours"`
		}
		if json.Unmarshal(b2, &fi) == nil {
			for _, x := range fi {
				if x.FundingIntervalHours > 0 {
					bnInt[x.Symbol] = x.FundingIntervalHours
				}
			}
		}
	}

	// 3. Binance current funding rates (/fapi/v1/premiumIndex)
	resp3, err3 := hc.Get("https://fapi.binance.com/fapi/v1/premiumIndex")
	if err3 != nil {
		return nil, nil, false
	}
	b3, _ := io.ReadAll(resp3.Body)
	resp3.Body.Close()
	var pi []struct {
		Symbol          string `json:"symbol"`
		LastFundingRate string `json:"lastFundingRate"`
	}
	if json.Unmarshal(b3, &pi) != nil {
		return nil, nil, false
	}
	for _, x := range pi {
		r, _ := strconv.ParseFloat(x.LastFundingRate, 64)
		h := bnInt[x.Symbol]
		if h <= 0 {
			h = 8.0
		}
		bnAprMap[x.Symbol] = (r / h) * 24.0 * 365.0 * 100.0
	}
	return ltAprMap, bnAprMap, true
}

func fetchBnBookRESTSingle(bnSym string) (bid, ask float64, ok bool) {
	hc := &http.Client{Timeout: 5 * time.Second}
	resp, err := hc.Get("https://fapi.binance.com/fapi/v1/ticker/bookTicker?symbol=" + bnSym)
	if err != nil {
		return 0, 0, false
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var bk struct {
		BidPrice string `json:"bidPrice"`
		AskPrice string `json:"askPrice"`
	}
	if json.Unmarshal(b, &bk) != nil {
		return 0, 0, false
	}
	bid, _ = strconv.ParseFloat(bk.BidPrice, 64)
	ask, _ = strconv.ParseFloat(bk.AskPrice, 64)
	return bid, ask, bid > 0 && ask > bid
}

func syncCarryToLiveSlots(slots []*carrySlot) {
	liveMu.Lock()
	liveAcct.ID = "CARRY_MICRO_3X25"
	liveAcct.ForwardStratID = "CARRY_MICRO_3X25"
	liveAcct.Regime = "FUNDING CARRY"
	liveAcct.Name = "Micro Funding Carry ($25x3: MRNA, WLFI, ZEC)"
	liveAcct.Desc = "1:1 hedged Lighter 0%-fee <-> Binance funding carry micro test ($25/coin, -$4 hard stop)."
	liveAcct.VenueDesc = "LT 0% IOC + BN Hedge | Micro Carry"
	liveAcct.Tier = 1
	liveAcct.TierLabel = "MICRO $25x3"
	liveAcct.ClipUsd = CarryClipUSD
	for i := 0; i < MaxLiveSlots; i++ {
		if i < len(slots) && slots[i].Open {
			cs := slots[i]
			sl := liveSlots[i]
			sl.State = "HEDGED"
			sl.Sym = cs.Sym
			sl.PeerSym = cs.BnSym
			if cs.Dir == "LONG_LT" {
				sl.LtSide = "BID"
				sl.PeerSide = "SELL"
			} else {
				sl.LtSide = "ASK"
				sl.PeerSide = "BUY"
			}
			sl.HedgedQty = cs.Qty
			sl.EntryLtPx = cs.LtEntryPx
			sl.EntryPeerPx = cs.BnEntryPx
			sl.EntryFx = 1.0
			sl.OpenedAt = cs.OpenedAt
		} else {
			sl := liveSlots[i]
			sl.State = "IDLE"
			sl.Sym = ""
			sl.PeerSym = ""
			sl.HedgedQty = 0
		}
	}
	liveMu.Unlock()
	writeLiveArenaTelemetry(time.Now())
}

func runMicroCarryTestLoop() {
	fmt.Printf("[%s] Booting safe micro funding-carry test ($%.0f x 3: MRNA, WLFI, ZEC | Stop=$%.2f)\n",
		CarryMicroVer, CarryClipUSD, CarryHardStopUSD)

	slots := make([]*carrySlot, len(carryTargets))
	for i, t := range carryTargets {
		slots[i] = &carrySlot{Sym: t.Sym, BnSym: t.BnSym}
	}

	// Capture starting 3-venue flat equity baseline for hard stop
	_, _, _, _ = fetchBnAccountMargin()
	bootLtEq := 0.0
	if snap, ok := fetchLtAccountCached(2*time.Second, 3); ok && snap != nil {
		bootLtEq, _ = strconv.ParseFloat(snap.TotalAssetValue, 64)
	}
	dnMu.Lock()
	bootBnEq := bnTotalMargin
	if bootBnEq <= 0 {
		bootBnEq = bnTotalWallet
	}
	dnMu.Unlock()
	bootTotalEq := bootLtEq + bootBnEq
	fmt.Printf("[%s] Boot 2-venue equity baseline: LT=$%.2f + BN=$%.2f = $%.2f (hard stop at $%.2f)\n",
		CarryMicroVer, bootLtEq, bootBnEq, bootTotalEq, bootTotalEq+CarryHardStopUSD)

	var lastFundCheck time.Time
	ltAprMap := map[string]float64{}
	bnAprMap := map[string]float64{}

	for {
		if remotePaused.Load() {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		now := time.Now()
		// Refresh live funding rates immediately on boot and every 5 minutes
		if lastFundCheck.IsZero() || now.Sub(lastFundCheck) >= 5*time.Minute {
			if ltm, bnm, ok := fetchLiveFundingDiffs(); ok {
				ltAprMap, bnAprMap = ltm, bnm
				lastFundCheck = now
				fmt.Printf("[%s] Funding refresh: MRNA(LT=%+.1f%% BN=%+.1f%%) WLFI(LT=%+.1f%% BN=%+.1f%%) ZEC(LT=%+.1f%% BN=%+.1f%%)\n",
					CarryMicroVer,
					ltAprMap["MRNA"], bnAprMap["MRNAUSDT"],
					ltAprMap["WLFI"], bnAprMap["WLFIUSDT"],
					ltAprMap["ZEC"], bnAprMap["ZECUSDT"])
			}
		}

		// Hard Equity Stop Check (-$4.00)
		if bootTotalEq > 100.0 {
			_, _, _, okBn := fetchBnAccountMargin()
			if snap, okLt := fetchLtAccountCached(5*time.Second, 2); okLt && snap != nil && okBn {
				dnMu.Lock()
				curBn := bnTotalMargin
				if curBn <= 0 {
					curBn = bnTotalWallet
				}
				dnMu.Unlock()
				ltEqNow, _ := strconv.ParseFloat(snap.TotalAssetValue, 64)
				curEq := ltEqNow + curBn
				dd := curEq - bootTotalEq
				if dd <= CarryHardStopUSD {
					fmt.Printf("[%s] HARD STOP HIT: cur=$%.2f vs boot=$%.2f (delta=$%+.2f <= $%.2f) -> FLATTENING ALL!\n",
						CarryMicroVer, curEq, bootTotalEq, dd, CarryHardStopUSD)
					flattenAndExitPause("STOP-LOSS CARRY")
					return
				}
			}
		}

		for _, cs := range slots {
			if remotePaused.Load() {
				break
			}
			m, okM := markets[cs.Sym]
			dnMu.Lock()
			sp, okSp := dnByBnSym[cs.BnSym]
			dnMu.Unlock()
			if !okM || !okSp {
				fmt.Printf("[%s] Skipping %s/%s: metadata missing (okM=%v okSp=%v)\n", CarryMicroVer, cs.Sym, cs.BnSym, okM, okSp)
				continue
			}

			ltApr := ltAprMap[cs.Sym]
			bnApr, okBnFr := bnAprMap[cs.Sym+"USDT"]
			if !okBnFr {
				bnApr = bnAprMap[cs.BnSym]
			}
			diffApr := ltApr - bnApr // >0 => SHORT_LT+LONG_BN; <0 => LONG_LT+SHORT_BN

			if !cs.Open {
				if now.Sub(cs.LastTry) < 20*time.Second {
					continue
				}
				cs.LastTry = now
				if math.Abs(diffApr) < CarryMinEntryAPR {
					fmt.Printf("[%s] %s funding spread %.1f%% < %.0f%% min; waiting.\n", CarryMicroVer, cs.Sym, math.Abs(diffApr), CarryMinEntryAPR)
					continue
				}

				// Verify flat on Lighter first
				if rp, _, okP := fetchLtPosRESTChecked(cs.Sym); !okP || math.Abs(rp) >= m.MinB {
					if okP && math.Abs(rp) >= m.MinB {
						fmt.Printf("[%s] %s already has live LT pos %.5f; adopting check needed.\n", CarryMicroVer, cs.Sym, rp)
					}
					continue
				}

				rb, ra := fetchLtBookREST(m.ID)
				bb, ba, okB := fetchBnBookRESTSingle(cs.BnSym)
				if rb <= 0 || ra <= rb || !okB {
					continue
				}
				ltMid := 0.5 * (rb + ra)
				bnMid := 0.5 * (bb + ba)
				if math.Abs(ltMid-bnMid)/bnMid > 0.025 {
					continue
				}
				if (ra-rb)/ltMid*10000.0 > 35.0 {
					fmt.Printf("[%s] %s LT spread too wide (%.1fbp); waiting.\n", CarryMicroVer, cs.Sym, (ra-rb)/ltMid*10000.0)
					continue
				}

				wantDir := "LONG_LT"
				ltSide := "BID"
				entryCrossBp := ((bb - ra) / ra) * 10000.0
				iocPx := roundTo(ra*1.0035, m.PxDec)
				if diffApr > 0 {
					wantDir = "SHORT_LT"
					ltSide = "ASK"
					entryCrossBp = ((rb - ba) / ba) * 10000.0
					iocPx = roundTo(rb*0.9965, m.PxDec)
				}
				if entryCrossBp < -CarryMaxAdverseBp {
					fmt.Printf("[%s] %s entryBasis=%+.1fbp worse than -%.0fbp cap; waiting.\n", CarryMicroVer, cs.Sym, entryCrossBp, CarryMaxAdverseBp)
					continue
				}

				// Compute exact shared step quantity for ~$25
				cStep := math.Max(sp.Step, math.Pow10(-m.SzDec))
				qD := sp.QDec
				if m.SzDec < qD {
					qD = m.SzDec
				}
				qty := roundTo(math.Floor((CarryClipUSD/ltMid)/cStep+1e-9)*cStep, qD)
				if qty < m.MinB || qty*ltMid < math.Max(11.0, sp.MinNot) {
					qty = roundTo(math.Ceil((math.Max(12.0, sp.MinNot)/ltMid)/cStep)*cStep, qD)
				}

				ensureBnLeverage(cs.BnSym)
				fmt.Printf("[%s] OPENING %s %s qty=%.*f ($%.2f) diffAPR=%.1f%% entryBasis=%+.1fbp (LT=%.5f/%.5f BN=%.5f/%.5f)\n",
					CarryMicroVer, cs.Sym, wantDir, qD, qty, qty*ltMid, math.Abs(diffApr), entryCrossBp, rb, ra, bb, ba)

				coi, sent := placeLtHedgeIOC(cs.Sym, ltSide, iocPx, qty, false)
				if !sent {
					continue
				}
				ltPos, ltAvg := waitLtPositionFill(cs.Sym, coi, m.MinB, 4500*time.Millisecond)
				if math.Abs(ltPos) < m.MinB {
					fmt.Printf("[%s] %s LT IOC did not fill; will retry.\n", CarryMicroVer, cs.Sym)
					continue
				}
				if ltAvg <= 0 {
					ltAvg = ltMid
				}
				// Derive exact Binance hedge direction from ACTUAL filled Lighter position sign!
				actualDir := "LONG_LT"
				actualBnSide := "SELL"
				if ltPos < 0 {
					actualDir = "SHORT_LT"
					actualBnSide = "BUY"
				}
				hQty := roundTo(math.Floor(math.Abs(ltPos)/sp.Step+1e-9)*sp.Step, sp.QDec)
				fq, ap, latMs, okH := executeBnMarketOrder(cs.BnSym, actualBnSide, hQty, sp.QDec, false)
				if !okH || fq <= 0 {
					fmt.Printf("[%s] %s BN hedge FAILED -> immediately closing LT pos %.5f!\n", CarryMicroVer, cs.Sym, ltPos)
					_, _ = closeLtLegSync(cs.Sym, ltPos, rb, ra)
					continue
				}

				cs.Open = true
				cs.Dir = actualDir
				cs.Qty = fq
				cs.LtEntryPx = ltAvg
				cs.BnEntryPx = ap
				cs.OpenedAt = time.Now()
				cs.EntryApr = math.Abs(diffApr)
				cs.FlipStrikes = 0
				fmt.Printf("[%s] HEDGED %s %s qty=%.*f | LT@%.5f + BN(%s)@%.5f (%dms) | NetAPR=%.1f%%\n",
					CarryMicroVer, cs.Sym, cs.Dir, sp.QDec, fq, ltAvg, cs.BnSym, ap, latMs, cs.EntryApr)
				syncCarryToLiveSlots(slots)
			} else {
				// Monitor open hedged carry position
				rb, ra := fetchLtBookREST(m.ID)
				bb, ba, okB := fetchBnBookRESTSingle(cs.BnSym)
				if rb > 0 && ra > rb && okB {
					var basisBps float64
					if cs.Dir == "LONG_LT" {
						basisBps = ((rb-cs.LtEntryPx)/cs.LtEntryPx + (cs.BnEntryPx-ba)/cs.BnEntryPx) * 10000.0
					} else {
						basisBps = ((cs.LtEntryPx-ra)/cs.LtEntryPx + (bb-cs.BnEntryPx)/cs.BnEntryPx) * 10000.0
					}
					myApr := -diffApr
					if cs.Dir == "SHORT_LT" {
						myApr = diffApr
					}
					needExit := false
					reason := ""
					if basisBps <= -CarryBasisStopBps {
						needExit = true
						reason = fmt.Sprintf("BASIS_STOP(%.1fbp)", basisBps)
					} else if myApr <= CarryFlipExitAPR {
						cs.FlipStrikes++
						if cs.FlipStrikes >= 20 { // ~5 minutes of flipped funding
							needExit = true
							reason = fmt.Sprintf("FUNDING_FLIP(%.1f%%)", myApr)
						}
					} else {
						cs.FlipStrikes = 0
					}

					if needExit {
						fmt.Printf("[%s] CLOSING %s %s reason=%s basis=%+.1fbp apr=%+.1f%%\n",
							CarryMicroVer, cs.Sym, cs.Dir, reason, basisBps, myApr)
						ltSigned := cs.Qty
						bnCloseSide := "BUY"
						if cs.Dir == "SHORT_LT" {
							ltSigned = -cs.Qty
							bnCloseSide = "SELL"
						}
						_, _ = closeLtLegSync(cs.Sym, ltSigned, rb, ra)
						_, _, _, _ = executeBnMarketOrder(cs.BnSym, bnCloseSide, cs.Qty, sp.QDec, true)
						cs.Open = false
						cs.LastTry = time.Now().Add(5 * time.Minute)
						syncCarryToLiveSlots(slots)
					}
				}
			}
			time.Sleep(400 * time.Millisecond)
		}

		// Periodic status log & telemetry refresh every 15s
		syncCarryToLiveSlots(slots)
		time.Sleep(15 * time.Second)
	}
}
