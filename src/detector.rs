use crate::orderbook::OrderBookMatrix;
use crate::types::{ArbitrageSignal, Exchange, Symbol, NUM_EXCHANGES};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct DetectorConfig {
    /// Taker fees in basis points indexed by Exchange.
    pub fee_bps: [f64; NUM_EXCHANGES],
    /// Estimated slippage buffer in basis points per trade leg.
    pub slippage_buffer_bps: f64,
    /// Minimum net spread in basis points after fees and slippage required to fire a signal.
    pub min_net_spread_bps: f64,
    /// Maximum notional trade size in USDT per opportunity.
    pub max_notional_usdt: f64,
    /// Maximum allowed age difference between quotes in nanoseconds (prevents trading on stale legs).
    pub max_quote_staleness_ns: u64,
}

impl Default for DetectorConfig {
    fn default() -> Self {
        Self {
            fee_bps: [
                Exchange::Binance.default_taker_fee_bps(),
                Exchange::Bybit.default_taker_fee_bps(),
                Exchange::Okx.default_taker_fee_bps(),
            ],
            slippage_buffer_bps: 1.5,
            min_net_spread_bps: 2.0,
            max_notional_usdt: 25_000.0,
            max_quote_staleness_ns: 250_000_000, // 250ms max staleness
        }
    }
}

pub struct ArbitrageDetector {
    config: DetectorConfig,
}

impl ArbitrageDetector {
    pub fn new(config: DetectorConfig) -> Self {
        Self { config }
    }

    /// Evaluates all cross-exchange pairs for the updated `Symbol` in O(E^2) = 6 comparisons.
    /// Designed to execute in < 100 nanoseconds on the hot path.
    #[inline(always)]
    pub fn scan_symbol(
        &self,
        matrix: &OrderBookMatrix,
        symbol: Symbol,
        now_ns: u64,
    ) -> Option<ArbitrageSignal> {
        let start = Instant::now();
        let quotes = matrix.get_symbol_row(symbol);

        let mut best_signal: Option<ArbitrageSignal> = None;

        for buy_idx in 0..NUM_EXCHANGES {
            let buy_quote = &quotes[buy_idx];
            if !buy_quote.is_valid() {
                continue;
            }
            if now_ns.saturating_sub(buy_quote.timestamp_ns) > self.config.max_quote_staleness_ns {
                continue;
            }

            for sell_idx in 0..NUM_EXCHANGES {
                if buy_idx == sell_idx {
                    continue;
                }
                let sell_quote = &quotes[sell_idx];
                if !sell_quote.is_valid() {
                    continue;
                }
                if now_ns.saturating_sub(sell_quote.timestamp_ns)
                    > self.config.max_quote_staleness_ns
                {
                    continue;
                }

                // Spatial arbitrage condition: Sell Exchange Bid > Buy Exchange Ask
                if sell_quote.bid_price <= buy_quote.ask_price {
                    continue;
                }

                let gross_spread_bps = ((sell_quote.bid_price - buy_quote.ask_price)
                    / buy_quote.ask_price)
                    * 10_000.0;

                let total_cost_bps = self.config.fee_bps[buy_idx]
                    + self.config.fee_bps[sell_idx]
                    + self.config.slippage_buffer_bps;

                let net_spread_bps = gross_spread_bps - total_cost_bps;

                if net_spread_bps >= self.config.min_net_spread_bps {
                    // Constrain executable quantity by top-of-book liquidity on both legs and max notional cap
                    let book_executable_qty = buy_quote.ask_qty.min(sell_quote.bid_qty);
                    let max_qty_by_notional = self.config.max_notional_usdt / buy_quote.ask_price;
                    let executable_qty = book_executable_qty.min(max_qty_by_notional);

                    if executable_qty <= 0.0 {
                        continue;
                    }

                    let notional_usdt = executable_qty * buy_quote.ask_price;
                    let expected_net_profit_usdt = notional_usdt * (net_spread_bps / 10_000.0);

                    let should_replace = match &best_signal {
                        Some(prev) => expected_net_profit_usdt > prev.expected_net_profit_usdt,
                        None => true,
                    };

                    if should_replace {
                        let detection_latency_ns = start.elapsed().as_nanos() as u64;
                        best_signal = Some(ArbitrageSignal {
                            symbol,
                            buy_exchange: Exchange::ALL[buy_idx],
                            sell_exchange: Exchange::ALL[sell_idx],
                            buy_ask_price: buy_quote.ask_price,
                            sell_bid_price: sell_quote.bid_price,
                            max_executable_qty: executable_qty,
                            gross_spread_bps,
                            net_spread_bps,
                            expected_net_profit_usdt,
                            detection_latency_ns,
                            timestamp_ns: now_ns,
                        });
                    }
                }
            }
        }

        best_signal
    }

    /// Ultra-fast HFT Orderbook Imbalance (OBI) + Cross-Exchange Lead-Lag Micro-Scalp Scanner
    /// for Binance Zero-Fee FDUSD Pairs (`BTCFDUSD`, `ETHFDUSD`, `SOLFDUSD` — `0.00%` Maker Fee).
    #[inline(always)]
    pub fn scan_hft_obi_scalp(
        &self,
        matrix: &OrderBookMatrix,
        symbol: Symbol,
        now_ns: u64,
    ) -> Option<ArbitrageSignal> {
        let start = Instant::now();
        let quotes = matrix.get_symbol_row(symbol);
        let binance_q = &quotes[Exchange::Binance.index()];
        if !binance_q.is_valid()
            || now_ns.saturating_sub(binance_q.timestamp_ns) > self.config.max_quote_staleness_ns
        {
            return None;
        }

        let total_top_qty = binance_q.bid_qty + binance_q.ask_qty;
        if total_top_qty <= 0.0 {
            return None;
        }

        // 1. Orderbook Imbalance (OBI) in [-1.0, +1.0]: > +0.32 means > 2:1 bid pressure
        let obi = ((binance_q.bid_qty - binance_q.ask_qty) / total_top_qty).abs();

        // 2. Lead-Lag Fair Mid from OKX / Bybit vs Binance Mid
        let binance_mid = (binance_q.bid_price + binance_q.ask_price) * 0.5;
        let okx_q = &quotes[Exchange::Okx.index()];
        let lead_lag_bps = if okx_q.is_valid() {
            let okx_mid = (okx_q.bid_price + okx_q.ask_price) * 0.5;
            ((okx_mid - binance_mid).abs() / binance_mid) * 10_000.0
        } else {
            0.0
        };

        // Combined micro-edge in basis points on 0.00% Maker Fee FDUSD book
        let micro_edge_bps = (obi * 2.4) + (lead_lag_bps * 0.45);
        if micro_edge_bps < 1.15 {
            return None;
        }

        let exec_qty = binance_q
            .ask_qty
            .min(binance_q.bid_qty)
            .min(self.config.max_notional_usdt / binance_q.ask_price);
        if exec_qty <= 0.0 {
            return None;
        }

        let notional_usdt = exec_qty * binance_q.ask_price;
        let expected_net_profit_usdt = notional_usdt * (micro_edge_bps / 10_000.0);

        Some(ArbitrageSignal {
            symbol,
            buy_exchange: Exchange::Binance,
            sell_exchange: Exchange::Binance,
            buy_ask_price: binance_q.bid_price,
            sell_bid_price: binance_q.ask_price,
            max_executable_qty: exec_qty,
            gross_spread_bps: micro_edge_bps,
            net_spread_bps: micro_edge_bps,
            expected_net_profit_usdt,
            detection_latency_ns: start.elapsed().as_nanos() as u64,
            timestamp_ns: now_ns,
        })
    }
}
