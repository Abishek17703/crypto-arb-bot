package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var baseCoins = []string{
	"BTC", "ETH", "SOL", "XRP", "DOGE",
	"NEAR", "UNI", "1000PEPE", "BNB", "HYPE",
}

type wsEnvelope struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type rawBookTicker struct {
	EventTime int64  `json:"E"`
	TransTime int64  `json:"T"`
	Symbol    string `json:"s"`
	BidPx     string `json:"b"`
	BidQty    string `json:"B"`
	AskPx     string `json:"a"`
	AskQty    string `json:"A"`
}

type rawAggTrade struct {
	EventTime    int64  `json:"E"`
	TradeTime    int64  `json:"T"`
	Symbol       string `json:"s"`
	Price        string `json:"p"`
	Qty          string `json:"q"`
	IsBuyerMaker bool   `json:"m"`
}

type tickBookRecord struct {
	Rx int64   `json:"r"`
	Ev int64   `json:"e"`
	Ty string  `json:"t"`
	Sy string  `json:"s"`
	Bp float64 `json:"bp"`
	Bq float64 `json:"bq"`
	Ap float64 `json:"ap"`
	Aq float64 `json:"aq"`
}

type tickTradeRecord struct {
	Rx int64   `json:"r"`
	Ev int64   `json:"e"`
	Ty string  `json:"t"`
	Sy string  `json:"s"`
	Px float64 `json:"p"`
	Qy float64 `json:"q"`
	Mk bool    `json:"m"`
}

type collectorStatus struct {
	Version         string   `json:"version"`
	StartedAtMs     int64    `json:"startedAtMs"`
	UpdatedAtMs     int64    `json:"updatedAtMs"`
	UptimeSec       int64    `json:"uptimeSec"`
	EventsTotal     int64    `json:"eventsTotal"`
	BookTicks       int64    `json:"bookTicks"`
	AggTrades       int64    `json:"aggTrades"`
	EventsPerSec    float64  `json:"eventsPerSec"`
	AvgWsLagMs      float64  `json:"avgWsLagMs"`
	Reconnects      int64    `json:"reconnects"`
	LastErr         string   `json:"lastErr,omitempty"`
	CompletedChunks int64    `json:"completedChunks"`
	ActiveFile      string   `json:"activeFile"`
	LastEventRxMs   int64    `json:"lastEventRxMs"`
	RotateSec       int      `json:"rotateSec"`
	Symbols         []string `json:"symbols"`
}

type chunkWriter struct {
	mu          sync.Mutex
	dir         string
	rotateSec   int
	retainHours int
	file        *os.File
	buf         *bufio.Writer
	gz          *gzip.Writer
	tmpPath     string
	finalName   string
	openedAt    time.Time
	chunksDone  atomic.Int64
	linesInFile int64
}

func newChunkWriter(dir string, rotateSec, retainHours int) (*chunkWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cw := &chunkWriter{
		dir:         dir,
		rotateSec:   rotateSec,
		retainHours: retainHours,
	}
	if err := cw.openNewLocked(time.Now().UTC()); err != nil {
		return nil, err
	}
	return cw, nil
}

func (cw *chunkWriter) openNewLocked(now time.Time) error {
	name := fmt.Sprintf("ticks_%s.ndjson.gz", now.Format("20060102_150405"))
	tmp := filepath.Join(cw.dir, name+".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256*1024)
	gz, err := gzip.NewWriterLevel(bw, gzip.BestSpeed)
	if err != nil {
		_ = f.Close()
		return err
	}
	cw.file = f
	cw.buf = bw
	cw.gz = gz
	cw.tmpPath = tmp
	cw.finalName = name
	cw.openedAt = now
	cw.linesInFile = 0
	return nil
}

func (cw *chunkWriter) finalizeCurrentLocked() {
	if cw.file == nil {
		return
	}
	_ = cw.gz.Close()
	_ = cw.buf.Flush()
	_ = cw.file.Close()
	if cw.linesInFile > 0 {
		finalPath := filepath.Join(cw.dir, cw.finalName)
		if err := os.Rename(cw.tmpPath, finalPath); err == nil {
			cw.chunksDone.Add(1)
			fmt.Printf("[TICK_COLLECTOR] finalized %s (%d lines)\n", cw.finalName, cw.linesInFile)
		} else {
			fmt.Printf("[TICK_COLLECTOR] rename err: %v\n", err)
		}
	} else {
		_ = os.Remove(cw.tmpPath)
	}
	cw.file = nil
	cw.buf = nil
	cw.gz = nil
	cw.pruneOldLocked()
}

func (cw *chunkWriter) pruneOldLocked() {
	entries, err := os.ReadDir(cw.dir)
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-time.Duration(cw.retainHours) * time.Hour)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ndjson.gz") {
			continue
		}
		info, err := e.Info()
		if err == nil && info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(cw.dir, e.Name()))
		}
	}
}

func (cw *chunkWriter) WriteLine(line []byte) error {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	now := time.Now().UTC()
	if cw.file == nil || now.Sub(cw.openedAt) >= time.Duration(cw.rotateSec)*time.Second {
		cw.finalizeCurrentLocked()
		if err := cw.openNewLocked(now); err != nil {
			return err
		}
	}
	if _, err := cw.gz.Write(line); err != nil {
		return err
	}
	if _, err := cw.gz.Write([]byte{'\n'}); err != nil {
		return err
	}
	cw.linesInFile++
	return nil
}

func (cw *chunkWriter) RotateIfDue() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	now := time.Now().UTC()
	if cw.file != nil && now.Sub(cw.openedAt) >= time.Duration(cw.rotateSec)*time.Second {
		cw.finalizeCurrentLocked()
		_ = cw.openNewLocked(now)
	}
}

func (cw *chunkWriter) ActiveFile() string {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	return cw.finalName
}

func (cw *chunkWriter) Close() {
	cw.mu.Lock()
	defer cw.mu.Unlock()
	cw.finalizeCurrentLocked()
}

func envInt(key string, def int) int {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func buildSymbolsAndURLs() ([]string, string, string) {
	var syms []string
	var bookStreams []string
	var tradeStreams []string
	for _, base := range baseCoins {
		for _, quote := range []string{"USDC", "USDT"} {
			sym := base + quote
			syms = append(syms, sym)
			low := strings.ToLower(sym)
			bookStreams = append(bookStreams, low+"@bookTicker")
			tradeStreams = append(tradeStreams, low+"@aggTrade")
		}
	}
	sort.Strings(syms)
	bookURL := "wss://fstream.binance.com/public/stream?streams=" + strings.Join(bookStreams, "/")
	tradeURL := "wss://fstream.binance.com/market/stream?streams=" + strings.Join(tradeStreams, "/")
	return syms, bookURL, tradeURL
}

func parseF(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

type metrics struct {
	eventsTotal atomic.Int64
	bookTicks   atomic.Int64
	aggTrades   atomic.Int64
	reconnects  atomic.Int64
	lastEventRx atomic.Int64
	lagSumMs    atomic.Int64
	lagCount    atomic.Int64
	errMu       sync.Mutex
	lastErr     string
}

func (m *metrics) setErr(s string) {
	m.errMu.Lock()
	m.lastErr = s
	m.errMu.Unlock()
}

func (m *metrics) getErr() string {
	m.errMu.Lock()
	defer m.errMu.Unlock()
	return m.lastErr
}

func runStreamWorker(name, wsURL string, cw *chunkWriter, m *metrics, stopCh <-chan struct{}) {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	backoff := time.Second
	for {
		select {
		case <-stopCh:
			return
		default:
		}

		conn, _, err := dialer.Dial(wsURL, nil)
		if err != nil {
			m.setErr(name + " dial: " + err.Error())
			m.reconnects.Add(1)
			select {
			case <-stopCh:
				return
			case <-time.After(backoff):
			}
			if backoff < 10*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		fmt.Printf("[TICK_COLLECTOR] %s connected\n", name)

		conn.SetPingHandler(func(appData string) error {
			return conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(5*time.Second))
		})

		doneRead := make(chan struct{})
		go func() {
			select {
			case <-stopCh:
				_ = conn.Close()
			case <-doneRead:
			}
		}()

		for {
			_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, msg, err := conn.ReadMessage()
			if err != nil {
				m.setErr(name + " read: " + err.Error())
				m.reconnects.Add(1)
				break
			}
			rxMs := time.Now().UnixMilli()
			var env wsEnvelope
			if err := json.Unmarshal(msg, &env); err != nil {
				continue
			}
			if strings.HasSuffix(env.Stream, "@bookTicker") {
				var bt rawBookTicker
				if json.Unmarshal(env.Data, &bt) != nil {
					continue
				}
				evMs := bt.EventTime
				if evMs == 0 {
					evMs = bt.TransTime
				}
				rec := tickBookRecord{
					Rx: rxMs,
					Ev: evMs,
					Ty: "b",
					Sy: bt.Symbol,
					Bp: parseF(bt.BidPx),
					Bq: parseF(bt.BidQty),
					Ap: parseF(bt.AskPx),
					Aq: parseF(bt.AskQty),
				}
				if b, err := json.Marshal(rec); err == nil {
					_ = cw.WriteLine(b)
					m.eventsTotal.Add(1)
					m.bookTicks.Add(1)
					m.lastEventRx.Store(rxMs)
					if evMs > 0 {
						lag := rxMs - evMs
						if lag >= -500 && lag <= 5000 {
							m.lagSumMs.Add(lag)
							m.lagCount.Add(1)
						}
					}
				}
			} else if strings.HasSuffix(env.Stream, "@aggTrade") {
				var at rawAggTrade
				if json.Unmarshal(env.Data, &at) != nil {
					continue
				}
				evMs := at.EventTime
				if evMs == 0 {
					evMs = at.TradeTime
				}
				rec := tickTradeRecord{
					Rx: rxMs,
					Ev: evMs,
					Ty: "t",
					Sy: at.Symbol,
					Px: parseF(at.Price),
					Qy: parseF(at.Qty),
					Mk: at.IsBuyerMaker,
				}
				if b, err := json.Marshal(rec); err == nil {
					_ = cw.WriteLine(b)
					m.eventsTotal.Add(1)
					m.aggTrades.Add(1)
					m.lastEventRx.Store(rxMs)
					if evMs > 0 {
						lag := rxMs - evMs
						if lag >= -500 && lag <= 5000 {
							m.lagSumMs.Add(lag)
							m.lagCount.Add(1)
						}
					}
				}
			}
		}
		close(doneRead)
		_ = conn.Close()
	}
}

func main() {
	tickDir := strings.TrimSpace(os.Getenv("TICK_DIR"))
	if tickDir == "" {
		tickDir = "/opt/crypto-arb-bot/ticks"
	}
	rotateSec := envInt("ROTATE_SEC", 300)
	retainHours := envInt("RETAIN_HOURS", 72)

	cw, err := newChunkWriter(tickDir, rotateSec, retainHours)
	if err != nil {
		fmt.Printf("[TICK_COLLECTOR] init failed: %v\n", err)
		os.Exit(1)
	}

	syms, bookURL, tradeURL := buildSymbolsAndURLs()
	startedAt := time.Now()
	m := &metrics{}

	stopCh := make(chan struct{})
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("[TICK_COLLECTOR] shutdown signal received, finalizing chunk...")
		close(stopCh)
	}()

	go func() {
		statusPath := filepath.Join(tickDir, "collector_status.json")
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				cw.RotateIfDue()
				up := time.Since(startedAt).Seconds()
				if up < 1 {
					up = 1
				}
				tot := m.eventsTotal.Load()
				lc := m.lagCount.Load()
				avgLag := 0.0
				if lc > 0 {
					avgLag = float64(m.lagSumMs.Load()) / float64(lc)
				}
				st := collectorStatus{
					Version:         "TICK-v1.1-SPLIT-WS",
					StartedAtMs:     startedAt.UnixMilli(),
					UpdatedAtMs:     time.Now().UnixMilli(),
					UptimeSec:       int64(up),
					EventsTotal:     tot,
					BookTicks:       m.bookTicks.Load(),
					AggTrades:       m.aggTrades.Load(),
					EventsPerSec:    float64(tot) / up,
					AvgWsLagMs:      avgLag,
					Reconnects:      m.reconnects.Load(),
					LastErr:         m.getErr(),
					CompletedChunks: cw.chunksDone.Load(),
					ActiveFile:      cw.ActiveFile(),
					LastEventRxMs:   m.lastEventRx.Load(),
					RotateSec:       rotateSec,
					Symbols:         syms,
				}
				if b, err := json.MarshalIndent(st, "", "  "); err == nil {
					tmp := statusPath + ".tmp"
					if os.WriteFile(tmp, b, 0o644) == nil {
						_ = os.Rename(tmp, statusPath)
					}
				}
			}
		}
	}()

	fmt.Printf("[TICK_COLLECTOR] starting v1.1 dir=%s rotate=%ds symbols=%d\n", tickDir, rotateSec, len(syms))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		runStreamWorker("BOOK", bookURL, cw, m, stopCh)
	}()
	go func() {
		defer wg.Done()
		runStreamWorker("TRADE", tradeURL, cw, m, stopCh)
	}()

	wg.Wait()
	cw.Close()
}
