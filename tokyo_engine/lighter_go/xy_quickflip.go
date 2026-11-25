package main

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

const (
	XYQuickFlipVer     = "XY-v1.0-QUICKFLIP-3X26"
	XYClipUSD          = 26.0
	XYHardStopUSD      = -4.00
	XYMinEntryCrossBp  = 8.5   // Fresh REST cross >= +8.5bp beyond live USDT/USDC equilibrium (+4.5bp net above 4bp fee!)
	XYExitStartBp      = 4.5   // Target +4.5bp net early
	XYExitFloorBp      = 2.5   // Never exit below +2.5bp buffer (absorbs Lighter 300ms-2s IOC exit lag!)
	XYEmergencyStopBp  = 75.0  // Wide hedged basis emergency stop (never tight 35bp!)
	XYMaxBnJumpBp      = 1.2   // Skip unstable 1s Binance jumps
	XYMinBnDepthMult   = 3.0   // Require >= 3x clip ($78) top-of-book depth on Binance
)

var xySymbols = []string{"ETH", "SOL", "XRP"}

type xySlot struct {
	Idx          int
	State        string // IDLE | HEDGED | BN_EXIT_OPEN
	Sym          string
	BnSym        string
	LtSide       string // BID (Long LT) | ASK (Short LT)
	BnSide       string // SELL (Short BN) | BUY (Long BN)
	Qty          float64
	LtEntryPx    float64
	BnEntryPx    float64
	EntryFeeUsd  float64
	EntryCrossBp float64
	OpenedAt     time.Time
	TradeID      int64
	ExitBnOid    int64
	ExitBnPx     float64
	ExitPlaced   time.Time
	LastRestChk  time.Time
	CoolUntil    time.Time
}

func syncXYToTelemetry(slots []*xySlot) {
	liveMu.Lock()
	liveAcct.ID = "XY_QUICKFLIP_3X26"
	liveAcct.ForwardStratID = "P2_CROSSED_BOOK_GRAB"
	liveAcct.Regime = "PEAK VOL"
	liveAcct.Name = "X+Y Quick-Flip Crossed-Book IOC ($26x3: ETH/SOL/XRP)"
	liveAcct.Desc = "Pure 0%-fee Lighter IOC + <8ms BN USDC-M hedge (>=+8.5bp REST cross beyond fxEq) + 0% BN Maker exit (>=+2.5bp REST buffer)."
	liveAcct.VenueDesc = "LT 0% IOC -> BN USDC-M (4bp RT)"
	liveAcct.Tier = 1
	liveAcct.TierLabel = "MICRO $26x3"
	liveAcct.ClipUsd = XYClipUSD
	for i := 0; i < MaxLiveSlots; i++ {
		sl := liveSlots[i]
		if i < len(slots) && slots[i].State != "IDLE" {
			xs := slots[i]
			sl.State = xs.State
			sl.Sym = xs.Sym
			sl.PeerSym = xs.BnSym
			sl.LtSide = xs.LtSide
			sl.PeerSide = xs.BnSide
			sl.HedgedQty = xs.Qty
			sl.EntryLtPx = xs.LtEntryPx
			sl.EntryPeerPx = xs.BnEntryPx
			sl.EntryFx = 1.0
			sl.EntryFeeUsd = xs.EntryFeeUsd
			sl.OpenedAt = xs.OpenedAt
			sl.TradeID = xs.TradeID
			sl.ExitPeerOid = xs.ExitBnOid
			sl.ExitPeerPx = xs.ExitBnPx
		} else {
			sl.State = "IDLE"
			sl.Sym = ""
			sl.PeerSym = ""
			sl.HedgedQty = 0
			sl.ExitPeerOid = 0
		}
	}
	liveMu.Unlock()
	writeLiveArenaTelemetry(time.Now())
}

func xySymActive(slots []*xySlot, sym string) bool {
	for _, s := range slots {
		if s.State != "IDLE" && s.Sym == sym {
			return true
		}
	}
	return false
}

func runXYQuickFlipLoop() {
	fmt.Printf("[%s] Booting pure X+Y Quick-Flip IOC bot ($%.0f x 3: ETH, SOL, XRP | Entry>=+%.1fbp beyond fxEq | Exit>=+%.1fbp REST | Stop=$%.2f)\n",
		XYQuickFlipVer, XYClipUSD, XYMinEntryCrossBp, XYExitFloorBp, XYHardStopUSD)

	// Reset live account stats cleanly for this micro profitability test
	liveMu.Lock()
	liveAcct.ClosedNetUsd = 0
	liveAcct.RealizedGross = 0
	liveAcct.FeesPaid = 0
	liveAcct.RoundTrips = 0
	liveAcct.Wins = 0
	liveAcct.Losses = 0
	liveAcct.GrossVolumeUsd = 0
	liveAcct.MakerFills = 0
	liveAcct.TakerFills = 0
	liveMu.Unlock()

	slots := make([]*xySlot, MaxLiveSlots)
	for i := 0; i < MaxLiveSlots; i++ {
		slots[i] = &xySlot{Idx: i, State: "IDLE"}
	}

	// Capture exact flat 2-venue equity baseline for -$4.00 hard stop
	bootBnEq := 0.0
	for k := 0; k < 4 && bootBnEq <= 0; k++ {
		if _, _, _, ok := fetchBnAccountMargin(); ok {
			dnMu.Lock()
			bootBnEq = bnTotalMargin
			if bootBnEq <= 0 {
				bootBnEq = bnTotalWallet
			}
			dnMu.Unlock()
		}
		if bootBnEq <= 0 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	bootLtEq := 0.0
	if snap, ok := fetchLtAccountCached(2*time.Second, 3); ok && snap != nil {
		bootLtEq, _ = strconv.ParseFloat(snap.TotalAssetValue, 64)
	}
	bootTotalEq := bootLtEq + bootBnEq
	fmt.Printf("[%s] Flat baseline: LT=$%.2f + BN=$%.2f = $%.2f (hard stop at $%.2f)\n",
		XYQuickFlipVer, bootLtEq, bootBnEq, bootTotalEq, bootTotalEq+XYHardStopUSD)

	syncXYToTelemetry(slots)
	lastStopChk := time.Now()
	lastTelSync := time.Now()
	lastScanLog := time.Now()

	for {
		if remotePaused.Load() {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		now := time.Now()

		// 1. Real 2-venue equity hard stop check every 10s
		if bootTotalEq > 100.0 && now.Sub(lastStopChk) >= 10*time.Second {
			lastStopChk = now
			_, _, _, okBn := fetchBnAccountMargin()
			if snap, okLt := fetchLtAccountCached(4*time.Second, 2); okLt && snap != nil && okBn {
				dnMu.Lock()
				curBn := bnTotalMargin
				if curBn <= 0 {
					curBn = bnTotalWallet
				}
				dnMu.Unlock()
				ltNow, _ := strconv.ParseFloat(snap.TotalAssetValue, 64)
				dd := (ltNow + curBn) - bootTotalEq
				if dd <= XYHardStopUSD {
					fmt.Printf("[%s] HARD STOP HIT: cur=$%.2f vs boot=$%.2f (delta=$%+.2f) -> FLATTENING ALL!\n",
						XYQuickFlipVer, ltNow+curBn, bootTotalEq, dd)
					flattenAndExitPause("STOP-LOSS XY")
					return
				}
			}
		}

		// 2. Manage open slots first (Y — REST-Verified Buffered 0% BN Maker Exit + 0% LT IOC Close)
		for _, xs := range slots {
			if remotePaused.Load() {
				break
			}
			if xs.State == "IDLE" {
				continue
			}
			m, okM := markets[xs.Sym]
			if !okM {
				continue
			}
			_, qDec, pxDec, _, _, pBid, pAsk, _, _, _, _, age, okB := getBnUsdcBook(xs.Sym, now)
			if !okB || age > 8*time.Second || pBid <= 0 || pAsk <= pBid {
				continue
			}

			if xs.State == "HEDGED" {
				// Poll FRESH Lighter REST book at most once per 1.5s per open slot (ZERO stale WS exits!)
				if now.Sub(xs.LastRestChk) < 1500*time.Millisecond {
					continue
				}
				xs.LastRestChk = now
				rb, ra := fetchLtBookREST(m.ID)
				if rb <= 0 || ra <= rb {
					continue
				}
				// Re-read freshest Binance USDC-M book right after Lighter REST returns
				_, _, _, _, _, pBid, pAsk, _, _, _, _, _, okB2 := getBnUsdcBook(xs.Sym, time.Now())
				if !okB2 || pBid <= 0 || pAsk <= pBid {
					continue
				}

				// Compute true fx = 1.0 USDC exit P&L (0% BN GTX Maker exit + 0% LT IOC close = 4.0bp total fee!)
				var grossBp, exitBnPx float64
				var bnCloseSide string
				if xs.LtSide == "BID" {
					// Long LT / Short BN -> Buy back BN at pBid (0% GTX Maker) + Sell LT at rb (0% IOC)
					bnCloseSide = "BUY"
					exitBnPx = pBid
					grossBp = ((rb-xs.LtEntryPx)/xs.LtEntryPx + (xs.BnEntryPx-pBid)/xs.BnEntryPx) * 10000.0
				} else {
					// Short LT / Long BN -> Sell BN at pAsk (0% GTX Maker) + Buy LT at ra (0% IOC)
					bnCloseSide = "SELL"
					exitBnPx = pAsk
					grossBp = ((xs.LtEntryPx-ra)/xs.LtEntryPx + (pAsk-xs.BnEntryPx)/xs.BnEntryPx) * 10000.0
				}
				netBp := grossBp - 4.0
				heldSec := time.Since(xs.OpenedAt).Seconds()

				// Emergency wide 75bp hedge-break stop
				if netBp <= -XYEmergencyStopBp {
					fmt.Printf("[%s] EMERGENCY STOP slot=%d %s net=%+.1fbp -> market closing BOTH legs!\n",
						XYQuickFlipVer, xs.Idx, xs.Sym, netBp)
					ltSigned := xs.Qty
					if xs.LtSide == "ASK" {
						ltSigned = -xs.Qty
					}
					ltClosePx, _ := closeLtLegSync(xs.Sym, ltSigned, rb, ra)
					_, bnClosePx, _, _ := executeBnMarketOrder(xs.BnSym, bnCloseSide, xs.Qty, qDec, true)
					recordXYClose(xs, ltClosePx, bnClosePx, 4.0, "EMERGENCY_75BP")
					syncXYToTelemetry(slots)
					continue
				}

				// Dynamic buffered target: +4.5bp early -> decays gently over 180s -> HARD FLOOR +2.5bp!
				reqNetBp := XYExitStartBp - (heldSec/180.0)*(XYExitStartBp-XYExitFloorBp)
				if reqNetBp < XYExitFloorBp {
					reqNetBp = XYExitFloorBp
				}
				if netBp >= reqNetBp {
					oid, placed := placeBnMakerExit(xs.BnSym, bnCloseSide, xs.Qty, exitBnPx, qDec, pxDec)
					if placed && oid > 0 {
						xs.State = "BN_EXIT_OPEN"
						xs.ExitBnOid = oid
						xs.ExitBnPx = exitBnPx
						xs.ExitPlaced = time.Now()
						fmt.Printf("[%s] Y_MAKER_EXIT slot=%d %s %s qty=%.4f @ %.5f (REST net=%+.2fbp >= +%.2fbp)\n",
							XYQuickFlipVer, xs.Idx, xs.Sym, bnCloseSide, xs.Qty, exitBnPx, netBp, reqNetBp)
						syncXYToTelemetry(slots)
					}
				}
			} else if xs.State == "BN_EXIT_OPEN" {
				// Poll Binance GTX Maker exit fill status via existing UserWS/REST helper
				stStr, filledQty, avgPx, _, okQ := queryBnOrder(xs.BnSym, xs.ExitBnOid, xs.LastRestChk, time.Since(xs.LastRestChk) >= 1200*time.Millisecond)
				if !okQ {
					continue
				}
				if stStr == "FILLED" && filledQty > 0 {
					rb, ra := fetchLtBookREST(m.ID)
					ltSigned := xs.Qty
					if xs.LtSide == "ASK" {
						ltSigned = -xs.Qty
					}
					ltClosePx, _ := closeLtLegSync(xs.Sym, ltSigned, rb, ra)
					recordXYClose(xs, ltClosePx, avgPx, 0.0, "XY_0FEE_MAKER_TP")
					syncXYToTelemetry(slots)
					continue
				}
				// If order expired/canceled or drifted off BBO after 3s, cancel and return to HEDGED
				drifted := (xs.LtSide == "BID" && pBid > xs.ExitBnPx*1.00005) || (xs.LtSide == "ASK" && pAsk < xs.ExitBnPx*0.99995)
				if stStr == "CANCELED" || stStr == "EXPIRED" || stStr == "REJECTED" || (time.Since(xs.ExitPlaced) >= 3*time.Second && drifted) || time.Since(xs.ExitPlaced) >= 12*time.Second {
					if stStr == "NEW" || stStr == "PARTIALLY_FILLED" {
						_, fq2, ap2 := cancelBnOrder(xs.BnSym, xs.ExitBnOid)
						if fq2 >= m.MinB {
							rb, ra := fetchLtBookREST(m.ID)
							ltSigned := fq2
							if xs.LtSide == "ASK" {
								ltSigned = -fq2
							}
							ltClosePx, _ := closeLtLegSync(xs.Sym, ltSigned, rb, ra)
							recordXYClose(xs, ltClosePx, ap2, 0.0, "XY_PARTIAL_MAKER_TP")
							syncXYToTelemetry(slots)
							continue
						}
					}
					xs.State = "HEDGED"
					xs.ExitBnOid = 0
				}
			}
		}

		// 3. Scan for new X entries on IDLE slots (max 1 entry per loop tick, ZERO resting LT Maker orders!)
		var idleSlot *xySlot
		for _, xs := range slots {
			if xs.State == "IDLE" && now.After(xs.CoolUntil) {
				idleSlot = xs
				break
			}
		}

		if idleSlot != nil {
			liveMu.Lock()
			txCount := countLtTxLastMinLocked(now.UnixMilli())
			liveMu.Unlock()

			if txCount < MaxLtTxPerMin-6 {
				fxEq := getUsdtUsdcEquilFx(now)
				bestCross := -999.0
				bestSym, bestBnSym, bestLtSide := "", "", ""
				bestQty, bestStep := 0.0, 0.0
				bestQDec := 3

				for _, sym := range xySymbols {
					if xySymActive(slots, sym) {
						continue
					}
					m, okM := markets[sym]
					st := mktStates[sym]
					if !okM || st == nil {
						continue
					}
					st.mu.Lock()
					lb, la, ltPos, lTS := st.ltBid, st.ltAsk, st.pos, st.lastBookTS
					st.mu.Unlock()
					if math.Abs(ltPos) >= m.MinB || lb <= 0 || la <= lb || now.Sub(lTS) > 6*time.Second {
						continue
					}
					bnSym, qDec, _, step, minNot, pBid, pAsk, _, jumpBps, _, warm, age, okB := getBnUsdcBook(sym, now)
					if !okB || warm < 6 || age > 5*time.Second || math.Abs(jumpBps) > XYMaxBnJumpBp {
						continue
					}
					cStep := math.Max(step, math.Pow10(-m.SzDec))
					qD := qDec
					if m.SzDec < qD {
						qD = m.SzDec
					}
					ltMid := 0.5 * (lb + la)
					qty := roundTo(math.Floor((XYClipUSD/ltMid)/cStep+1e-9)*cStep, qD)
					if qty < m.MinB || qty*ltMid < math.Max(11.0, minNot) {
						continue
					}

					// Equilibrium-anchored crossed-book check!
					// Long LT (buy la) + Short BN (sell pBid): crossed when pBid*fxEq > la
					cLong := ((pBid*fxEq - la) / (pBid * fxEq)) * 10000.0
					if cLong >= XYMinEntryCrossBp && cLong > bestCross && bnTopDepthUsd(bnSym, "SELL") >= XYClipUSD*XYMinBnDepthMult {
						bestCross, bestSym, bestBnSym, bestLtSide, bestQty, bestStep, bestQDec = cLong, sym, bnSym, "BID", qty, step, qDec
					}
					// Short LT (sell lb) + Long BN (buy pAsk): crossed when lb > pAsk*fxEq
					cShort := ((lb - pAsk*fxEq) / (pAsk * fxEq)) * 10000.0
					if cShort >= XYMinEntryCrossBp && cShort > bestCross && bnTopDepthUsd(bnSym, "BUY") >= XYClipUSD*XYMinBnDepthMult {
						bestCross, bestSym, bestBnSym, bestLtSide, bestQty, bestStep, bestQDec = cShort, sym, bnSym, "ASK", qty, step, qDec
					}
				}

				if bestSym != "" {
					// MANDATORY FRESH REST CONFIRMATION BEFORE FIRING!
					m := markets[bestSym]
					rb, ra := fetchLtBookREST(m.ID)
					_, _, _, _, _, pBidC, pAskC, _, jumpC, _, _, _, okC := getBnUsdcBook(bestSym, time.Now())
					fxEqC := getUsdtUsdcEquilFx(time.Now())
					confirmed := false
					confCross := 0.0
					iocPx := 0.0
					if rb > 0 && ra > rb && okC && math.Abs(jumpC) <= XYMaxBnJumpBp {
						if bestLtSide == "BID" {
							confCross = ((pBidC*fxEqC - ra) / (pBidC * fxEqC)) * 10000.0
							iocPx = roundTo(ra*1.0012, m.PxDec)
							confirmed = confCross >= XYMinEntryCrossBp
						} else {
							confCross = ((rb - pAskC*fxEqC) / (pAskC * fxEqC)) * 10000.0
							iocPx = roundTo(rb*0.9988, m.PxDec)
							confirmed = confCross >= XYMinEntryCrossBp
						}
					}
					if confirmed {
						if rpPre, _, okPre := fetchLtPosRESTChecked(bestSym); okPre && math.Abs(rpPre) < m.MinB {
							ensureBnLeverage(bestBnSym)
							liveMu.Lock()
							recordLtTxLocked(time.Now().UnixMilli())
							liveMu.Unlock()
							coi, sent := placeLtHedgeIOC(bestSym, bestLtSide, iocPx, bestQty, false)
							var ltPos, ltAvg float64
							if sent {
								ltPos, ltAvg = waitLtPositionFill(bestSym, coi, m.MinB, 4200*time.Millisecond)
							}
							if math.Abs(ltPos) >= m.MinB {
								actualLtSide, actualBnSide := "BID", "SELL"
								if ltPos < 0 {
									actualLtSide, actualBnSide = "ASK", "BUY"
								}
								if ltAvg <= 0 {
									if actualLtSide == "BID" {
										ltAvg = ra
									} else {
										ltAvg = rb
									}
								}
								hQty := roundTo(math.Floor(math.Abs(ltPos)/bestStep+1e-9)*bestStep, bestQDec)
								fq, ap, latMs, okH := executeBnMarketOrder(bestBnSym, actualBnSide, hQty, bestQDec, false)
								if !okH || fq <= 0 {
									_, _ = closeLtLegSync(bestSym, ltPos, rb, ra)
									idleSlot.CoolUntil = time.Now().Add(8 * time.Second)
								} else {
									feeUsd := fq * ap * 0.0004
									tid := time.Now().UnixMilli()
									idleSlot.State = "HEDGED"
									idleSlot.Sym = bestSym
									idleSlot.BnSym = bestBnSym
									idleSlot.LtSide = actualLtSide
									idleSlot.BnSide = actualBnSide
									idleSlot.Qty = fq
									idleSlot.LtEntryPx = ltAvg
									idleSlot.BnEntryPx = ap
									idleSlot.EntryFeeUsd = feeUsd
									idleSlot.EntryCrossBp = confCross
									idleSlot.OpenedAt = time.Now()
									idleSlot.TradeID = tid
									liveMu.Lock()
									liveAcct.TakerFills++
									liveAcct.GrossVolumeUsd = roundTo(liveAcct.GrossVolumeUsd+fq*ap*2, 2)
									liveMu.Unlock()
									fmt.Printf("[%s] X_OPEN slot=%d %s LT_%s@%.5f + BN_%s@%.5f qty=%.4f ($%.2f) RESTcross=%+.2fbp (%dms)\n",
										XYQuickFlipVer, idleSlot.Idx, bestSym, actualLtSide, ltAvg, actualBnSide, ap, fq, fq*ap, confCross, latMs)
									recordStratOpen(tid, "XY_QUICKFLIP_3X26", fmt.Sprintf("LT_%s_%s+BN_%s", bestSym, actualLtSide, actualBnSide), bestSym, fq*ap, confCross, fmt.Sprintf("XY REST %+.1fbp", confCross))
									syncXYToTelemetry(slots)
								}
							} else {
								idleSlot.CoolUntil = time.Now().Add(3 * time.Second)
							}
						}
					} else {
						idleSlot.CoolUntil = time.Now().Add(1500 * time.Millisecond)
					}
				}
			}
		}

		// 4. Periodic heartbeat log (every 60s) & telemetry sync (every 3s)
		if now.Sub(lastScanLog) >= 60*time.Second {
			lastScanLog = now
			fx := getUsdtUsdcEquilFx(now)
			openCnt := 0
			for _, xs := range slots {
				if xs.State != "IDLE" {
					openCnt++
				}
			}
			liveMu.Lock()
			cNet, tr, w := liveAcct.ClosedNetUsd, liveAcct.RoundTrips, liveAcct.Wins
			liveMu.Unlock()
			fmt.Printf("[%s] Heartbeat | fxEq=%.5f | open=%d/3 | closed=%d (%dW) net=$%+.4f\n",
				XYQuickFlipVer, fx, openCnt, tr, w, cNet)
		}
		if now.Sub(lastTelSync) >= 3*time.Second {
			lastTelSync = now
			syncXYToTelemetry(slots)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func recordXYClose(xs *xySlot, ltExitPx, bnExitPx, exitFeeBps float64, reason string) {
	if ltExitPx <= 0 {
		ltExitPx = xs.LtEntryPx
	}
	if bnExitPx <= 0 {
		bnExitPx = xs.BnEntryPx
	}
	var ltPnl, bnPnl float64
	if xs.LtSide == "BID" {
		ltPnl = xs.Qty * (ltExitPx - xs.LtEntryPx)
		bnPnl = xs.Qty * (xs.BnEntryPx - bnExitPx)
	} else {
		ltPnl = xs.Qty * (xs.LtEntryPx - ltExitPx)
		bnPnl = xs.Qty * (bnExitPx - xs.BnEntryPx)
	}
	exitFeeUsd := xs.Qty * bnExitPx * (exitFeeBps / 10000.0)
	totFee := xs.EntryFeeUsd + exitFeeUsd
	gross := ltPnl + bnPnl
	net := gross - totFee
	notional := xs.Qty * xs.BnEntryPx
	netBps := 0.0
	if notional > 0 {
		netBps = (net / notional) * 10000.0
	}
	holdS := time.Since(xs.OpenedAt).Seconds()

	liveMu.Lock()
	liveAcct.RoundTrips++
	if net > 0 {
		liveAcct.Wins++
	} else {
		liveAcct.Losses++
	}
	liveAcct.RealizedGross = roundTo(liveAcct.RealizedGross+gross, 6)
	liveAcct.FeesPaid = roundTo(liveAcct.FeesPaid+totFee, 6)
	liveAcct.ClosedNetUsd = roundTo(liveAcct.ClosedNetUsd+net, 6)
	liveMu.Unlock()

	fmt.Printf("[%s] Y_CLOSE slot=%d %s %s | gross=$%+.4f fee=$%.4f NET=$%+.4f (%+.2fbp) hold=%.0fs | LT@%.5f->%.5f BN@%.5f->%.5f\n",
		XYQuickFlipVer, xs.Idx, xs.Sym, reason, gross, totFee, net, netBps, holdS, xs.LtEntryPx, ltExitPx, xs.BnEntryPx, bnExitPx)
	legs := fmt.Sprintf("LT_%s_%s+BN_%s", xs.Sym, xs.LtSide, xs.BnSide)
	recordStratClose(xs.TradeID, "XY_QUICKFLIP_3X26", legs, xs.Sym, notional, holdS, netBps, net, bnPnl, ltPnl, totFee, reason)

	xs.State = "IDLE"
	xs.Sym = ""
	xs.BnSym = ""
	xs.Qty = 0
	xs.ExitBnOid = 0
	xs.CoolUntil = time.Now().Add(4 * time.Second)
}
