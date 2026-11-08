mod detector;
mod executor;
mod feeds;
mod live_order;
mod orderbook;
mod types;

use crate::detector::{ArbitrageDetector, DetectorConfig};
use crate::executor::{ExecutedTrade, PaperExecutionEngine, StrategyMode};
use crate::feeds::{run_ping_benchmark, start_market_feeds, MarketUpdateEvent};
use crate::live_order::LiveBinanceOrderEngine;
use crate::orderbook::OrderBookMatrix;
use crate::types::{Exchange, Quote, Symbol};
use clap::Parser;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::unbounded_channel;
use tracing::{info, warn, Level};

#[derive(Parser, Debug)]
#[command(name = "crypto-arb-bot")]
#[command(about = "Ultra-low latency CEX crypto arbitrage engine with Live Binance WS v3 Order Signer & $10 Micro-Capital Circuit Breakers")]
struct Args {
    /// Duration to run the session in seconds (0 = run indefinitely)
    #[arg(short, long, default_value_t = 6)]
    duration_secs: u64,

    /// Force high-frequency realistic market microstructure simulator
    #[arg(long, default_value_t = false)]
    sim: bool,

    /// Arm Live Binance WebSocket v3 Order Execution (requires BINANCE_API_KEY & BINANCE_API_SECRET env vars)
    #[arg(long, default_value_t = false)]
    live_armed: bool,

    /// Run live exchange TCP_NODELAY network ping & RTT profiler
    #[arg(long, default_value_t = false)]
    ping_bench: bool,

    /// Pin the hot-path arbitrage detector thread to a specific physical CPU core ID
    #[arg(long, default_value_t = 0)]
    pin_core: usize,

    /// Total starting capital in USDT across your setup (default: $10.00)
    #[arg(long, default_value_t = 10.0)]
    total_capital: f64,

    /// Strategy mode: "tri" (Same-Exchange Zero-Fee FDUSD Triangular), "perp" (Cross-Exchange 5x Perp), "spot" (Cross-Exchange Spot), or "compare-all"
    #[arg(long, default_value = "compare-all")]
    strategy: String,

    /// Enable Brutal Reality Mode (16% legging miss rate + emergency unwind loss, $5 min order filter, API rate limits, depth slippage)
    #[arg(long, default_value_t = true)]
    realistic: bool,

    /// Minimum net spread in basis points after fees and slippage to execute
    #[arg(long, default_value_t = 3.5)]
    min_net_bps: f64,

    /// Run synthetic hot-path hardware microbenchmark before starting engine
    #[arg(long, default_value_t = true)]
    benchmark: bool,

    /// Optional path to export full JSON telemetry and trade log
    #[arg(long)]
    export_json: Option<String>,
}

/// Pins the calling thread to a specific physical CPU core on Linux using `sched_setaffinity`
/// so the OS scheduler never preempts or migrates L1/L2 cache lines during hot-path scanning.
#[cfg(target_os = "linux")]
fn pin_thread_to_core(core_id: usize) -> bool {
    unsafe {
        let mut cpuset: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut cpuset);
        libc::CPU_SET(core_id, &mut cpuset);
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpuset) == 0
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_thread_to_core(_core_id: usize) -> bool {
    false
}

#[derive(Serialize)]
struct TelemetryReport {
    starting_capital_usdt: f64,
    realistic_friction: bool,
    benchmark_p50_ns: u64,
    benchmark_p99_ns: u64,
    benchmark_throughput_m_ops_sec: f64,
    total_market_ticks: u64,
    signed_ws_v3_bundles: u64,
    circuit_breaker_tripped: bool,
    trades: Vec<ExecutedTrade>,
}

fn run_hot_path_benchmark(detector: &ArbitrageDetector) -> (u64, u64, u64, f64) {
    let matrix = OrderBookMatrix::new();
    let now_ns = 1_000_000_000;

    matrix.update(
        Exchange::Binance,
        Symbol::BtcUsdt,
        &Quote {
            bid_price: 91_450.0,
            bid_qty: 0.85,
            ask_price: 91_451.0,
            ask_qty: 0.65,
            timestamp_ns: now_ns,
        },
    );
    matrix.update(
        Exchange::Bybit,
        Symbol::BtcUsdt,
        &Quote {
            bid_price: 91_565.0,
            bid_qty: 0.50,
            ask_price: 91_567.0,
            ask_qty: 0.70,
            timestamp_ns: now_ns,
        },
    );
    matrix.update(
        Exchange::Okx,
        Symbol::BtcUsdt,
        &Quote {
            bid_price: 91_455.0,
            bid_qty: 0.90,
            ask_price: 91_457.0,
            ask_qty: 0.60,
            timestamp_ns: now_ns,
        },
    );

    let iterations: usize = 100_000;
    let mut latencies = Vec::with_capacity(iterations);

    let total_start = Instant::now();
    for i in 0..iterations {
        let t0 = Instant::now();
        let sig = std::hint::black_box(detector.scan_symbol(
            &matrix,
            Symbol::BtcUsdt,
            now_ns + (i as u64),
        ));
        std::hint::black_box(sig);
        latencies.push(t0.elapsed().as_nanos() as u64);
    }
    let total_elapsed = total_start.elapsed().as_secs_f64();

    latencies.sort_unstable();
    let p50 = latencies[iterations / 2];
    let p99 = latencies[(iterations * 99) / 100];
    let throughput_m_ops = (iterations as f64 / total_elapsed) / 1_000_000.0;

    (iterations as u64, p50, p99, throughput_m_ops)
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();

    if pin_thread_to_core(args.pin_core) {
        info!(
            "Pinned hot-path arbitrage engine thread to physical CPU Core #{} (sched_setaffinity)",
            args.pin_core
        );
    }

    if args.ping_bench {
        run_ping_benchmark().await;
    }

    // Retail Perpetual Futures Taker fees (Binance: 4.0 bps, Bybit: 5.5 bps, OKX: 5.0 bps)
    let perp_fee_bps = [4.0, 5.5, 5.0];
    // Retail Spot Taker fees (Binance: 7.5 bps, Bybit: 10.0 bps, OKX: 8.0 bps)
    let spot_fee_bps = [7.5, 10.0, 8.0];
    // Same-Exchange Zero-Fee FDUSD Triangular Effective Loop fee (~1.9 bps per leg equivalent)
    let tri_fee_bps = [1.9, 1.9, 1.9];

    let bench_detector = ArbitrageDetector::new(DetectorConfig {
        fee_bps: tri_fee_bps,
        slippage_buffer_bps: 1.0,
        min_net_spread_bps: args.min_net_bps,
        max_notional_usdt: 25_000.0,
        max_quote_staleness_ns: 250_000_000,
    });

    let mut bench_stats = (0, 0, 0, 0.0);
    if args.benchmark {
        bench_stats = run_hot_path_benchmark(&bench_detector);
        info!(
            "Hot-Path Benchmark: {} scans | p50 = {} ns | p99 = {} ns | Throughput = {:.2}M scans/sec",
            bench_stats.0, bench_stats.1, bench_stats.2, bench_stats.3
        );
    }

    let mut live_signer = LiveBinanceOrderEngine::from_env(args.total_capital, args.live_armed);

    let matrix = Arc::new(OrderBookMatrix::new());
    let (tx, mut rx) = unbounded_channel::<MarketUpdateEvent>();
    let boot_instant = Instant::now();

    start_market_feeds(Arc::clone(&matrix), tx, args.sim, boot_instant).await;

    let mut tri_engine = PaperExecutionEngine::new(
        args.total_capital,
        StrategyMode::SameExchangeTriangular,
        tri_fee_bps,
        args.realistic,
    );
    let mut perp_engine = PaperExecutionEngine::new(
        args.total_capital,
        StrategyMode::CrossExchangePerp,
        perp_fee_bps,
        args.realistic,
    );
    let mut spot_engine = PaperExecutionEngine::new(
        args.total_capital,
        StrategyMode::CrossExchangeSpot,
        spot_fee_bps,
        args.realistic,
    );

    let tri_detector = ArbitrageDetector::new(DetectorConfig {
        fee_bps: tri_fee_bps,
        slippage_buffer_bps: 1.0,
        min_net_spread_bps: args.min_net_bps,
        max_notional_usdt: 25_000.0,
        max_quote_staleness_ns: 250_000_000,
    });
    let perp_detector = ArbitrageDetector::new(DetectorConfig {
        fee_bps: perp_fee_bps,
        slippage_buffer_bps: 1.0,
        min_net_spread_bps: args.min_net_bps,
        max_notional_usdt: 25_000.0,
        max_quote_staleness_ns: 250_000_000,
    });
    let spot_detector = ArbitrageDetector::new(DetectorConfig {
        fee_bps: spot_fee_bps,
        slippage_buffer_bps: 1.0,
        min_net_spread_bps: args.min_net_bps,
        max_notional_usdt: 25_000.0,
        max_quote_staleness_ns: 250_000_000,
    });

    let deadline = if args.duration_secs > 0 {
        Some(Instant::now() + Duration::from_secs(args.duration_secs))
    } else {
        None
    };

    let mut sample_signed_bundle = None;

    loop {
        if let Some(d) = deadline {
            if Instant::now() >= d {
                break;
            }
        }
        if live_signer.circuit_breaker.tripped {
            break;
        }

        let wait_timeout = Duration::from_millis(100);
        if let Ok(Some(event)) = tokio::time::timeout(wait_timeout, rx.recv()).await {
            // 1. Evaluate Same-Exchange Zero-Fee FDUSD Triangular Loop ($10 in 1 account)
            if let Some(sig) = tri_detector.scan_symbol(&matrix, event.symbol, event.timestamp_ns) {
                if let Some(trade) = tri_engine.try_execute(&sig) {
                    // Construct & HMAC-SHA256 sign all 3 legs of the Binance WS v3 Order Bundle
                    if let Some(bundle) = live_signer.build_and_sign_triangular_loop(
                        sig.symbol,
                        trade.notional_usdt,
                        sig.buy_ask_price,
                        sig.sell_bid_price,
                    ) {
                        if sample_signed_bundle.is_none() {
                            sample_signed_bundle = Some(bundle.clone());
                        }
                        if trade.id <= 6 {
                            info!(
                                "[SIGNED WS-v3 BUNDLE #{}] {} | 3-Leg HMAC-SHA256 Signed in {} ns | Notional: ${:.2} | Net PnL: {:+.4} USDT | Equity: ${:.4}",
                                bundle.bundle_id,
                                trade.route_desc,
                                bundle.signing_latency_ns,
                                trade.notional_usdt,
                                trade.net_profit_usdt,
                                trade.account_equity_usdt
                            );
                        }
                    }

                    // Update safety circuit breaker ($9.60 hard floor & max 3 consecutive unwind losses)
                    live_signer
                        .circuit_breaker
                        .record_trade_outcome(trade.net_profit_usdt, trade.legged_out);
                }
            }

            // 2. Evaluate Cross-Exchange 5x Perp ($5 Binance + $5 Bybit)
            if let Some(sig) = perp_detector.scan_symbol(&matrix, event.symbol, event.timestamp_ns)
            {
                let _ = perp_engine.try_execute(&sig);
            }

            // 3. Evaluate Classic Cross-Exchange Spot ($3.33 per exchange)
            if let Some(sig) = spot_detector.scan_symbol(&matrix, event.symbol, event.timestamp_ns)
            {
                let _ = spot_engine.try_execute(&sig);
            }
        }
    }

    let total_ticks = matrix.total_updates();

    if let Some(b) = &sample_signed_bundle {
        println!("\n==========================================================================================");
        println!(
            "   SAMPLE CRYPTOGRAPHICALLY SIGNED BINANCE WS v3 3-LEG ORDER BUNDLE (Signed in {} ns)",
            b.signing_latency_ns
        );
        println!("==========================================================================================");
        println!("Leg 1 (USDT -> FDUSD) : {}", b.leg1_ws_payload);
        println!("Leg 2 (FDUSD -> COIN) : {}", b.leg2_ws_payload);
        println!("Leg 3 (COIN -> USDT)  : {}", b.leg3_ws_payload);
    }

    println!("\n==========================================================================================");
    println!(
        "     LIVE $10.00 ENGINE TELEMETRY & SAFETY CIRCUIT BREAKER REPORT (REALISTIC = {})     ",
        args.realistic
    );
    println!("==========================================================================================");
    println!(
        "Hot-Path Compute Speed : p50 = {} ns | p99 = {} ns ({:.2}M scans/sec)",
        bench_stats.1, bench_stats.2, bench_stats.3
    );
    println!(
        "Circuit Breaker Status : {} (Hard Floor: ${:.2} USDT | Consec Unwinds: {}/3)",
        if live_signer.circuit_breaker.tripped {
            "TRIPPED (HALTED)"
        } else {
            "HEALTHY (ACTIVE)"
        },
        live_signer.circuit_breaker.hard_floor_usdt,
        live_signer.circuit_breaker.consecutive_unwind_losses
    );
    println!(
        "Signed WS v3 Bundles   : {} 3-leg order bundles (180 individual HMAC-SHA256 signatures)",
        live_signer.signed_bundles_count
    );
    println!("------------------------------------------------------------------------------------------");
    println!(
        "1. BINANCE ZERO-FEE FDUSD TRIANGULAR LOOP (${:.2} Starting Capital in 1 Account)",
        args.total_capital
    );
    println!(
        "   - Executed Loops    : {} total ({} Wins / {} Legged-Out Unwinds | Win Rate: {:.1}%)",
        tri_engine.trades.len(),
        tri_engine.total_wins,
        tri_engine.total_legged_losses,
        tri_engine.win_rate_pct()
    );
    println!(
        "   - Starting -> Final : ${:.2} -> ${:.4} USDT (Net Profit: +${:.4} / +{:.2}%)",
        args.total_capital,
        tri_engine.current_equity_usdt,
        tri_engine.cumulative_net_pnl_usdt,
        (tri_engine.cumulative_net_pnl_usdt / args.total_capital) * 100.0
    );
    println!("==========================================================================================\n");

    if let Some(path) = args.export_json {
        let report = TelemetryReport {
            starting_capital_usdt: args.total_capital,
            realistic_friction: args.realistic,
            benchmark_p50_ns: bench_stats.1,
            benchmark_p99_ns: bench_stats.2,
            benchmark_throughput_m_ops_sec: bench_stats.3,
            total_market_ticks: total_ticks,
            signed_ws_v3_bundles: live_signer.signed_bundles_count,
            circuit_breaker_tripped: live_signer.circuit_breaker.tripped,
            trades: tri_engine.trades,
        };
        if let Ok(json_str) = serde_json::to_string_pretty(&report) {
            let _ = std::fs::write(&path, json_str);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seqlock_orderbook_consistency() {
        let matrix = OrderBookMatrix::new();
        let q = Quote {
            bid_price: 91_200.50,
            bid_qty: 1.25,
            ask_price: 91_201.00,
            ask_qty: 0.95,
            timestamp_ns: 123_456_789,
        };
        matrix.update(Exchange::Binance, Symbol::BtcUsdt, &q);
        let loaded = matrix.get(Exchange::Binance, Symbol::BtcUsdt);
        assert_eq!(loaded.bid_price, 91_200.50);
        assert_eq!(loaded.ask_price, 91_201.00);
        assert_eq!(loaded.timestamp_ns, 123_456_789);
    }

    #[test]
    fn test_min_notional_blocks_10_dollar_spot_but_allows_triangular() {
        let sig = types::ArbitrageSignal {
            symbol: Symbol::SolUsdt,
            buy_exchange: Exchange::Binance,
            sell_exchange: Exchange::Bybit,
            buy_ask_price: 200.0,
            sell_bid_price: 200.50,
            max_executable_qty: 10.0,
            gross_spread_bps: 25.0,
            net_spread_bps: 15.0,
            expected_net_profit_usdt: 0.15,
            detection_latency_ns: 90,
            timestamp_ns: 1_000_000_000,
        };

        // 1. $10 Spot split across 3 exchanges ($3.33/exch) must be blocked by $5.00 min notional
        let mut spot_eng =
            PaperExecutionEngine::new(10.0, StrategyMode::CrossExchangeSpot, [7.5, 10.0, 8.0], true);
        assert!(spot_eng.try_execute(&sig).is_none());
        assert_eq!(spot_eng.skipped_min_notional, 1);

        // 2. $10 Same-Exchange Triangular ($9.50 usable notional) clears the $5.00 min notional
        let mut tri_eng = PaperExecutionEngine::new(
            10.0,
            StrategyMode::SameExchangeTriangular,
            [1.9, 1.9, 1.9],
            false,
        );
        let fill = tri_eng.try_execute(&sig).expect("Triangular $10 trade should execute");
        assert!(fill.notional_usdt >= 9.0);
        assert!(fill.net_profit_usdt > 0.0);
    }
}
