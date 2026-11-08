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
}
