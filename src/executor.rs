use crate::types::{ArbitrageSignal, Exchange, Symbol, NUM_EXCHANGES, NUM_SYMBOLS};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum StrategyMode {
    /// Same-Exchange Triangular Loop (e.g., USDT -> FDUSD -> SOL -> USDT on Binance)
    /// Ideal for $10 micro-capital because 100% of capital sits in 1 exchange account.
    SameExchangeTriangular,
    /// True HFT Zero-Fee FDUSD Orderbook Imbalance (OBI) + Lead-Lag Maker Scalper (`0.00%` fee + 2s Time-Stop Scratch)
    ZeroFeeHftObiScalp,
    /// Cross-Exchange Perpetual Futures Delta-Neutral Arb ($5 Binance + $5 Bybit with 5x leverage)
    CrossExchangePerp,
    /// Classic Pre-Funded Spot Cross-Exchange Arb (requires >= $300+ capital)
    CrossExchangeSpot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum FeeTier {
    /// Standard Retail Spot (7.5 - 10.0 bps per leg)
    RetailSpot,
    /// Standard Retail Perpetual Futures (3.5 - 5.0 bps per leg)
    RetailPerp,
    /// Binance FDUSD Zero-Fee Promo Loop (0 bps on FDUSD legs, 7.5 bps on USDT leg -> ~7.5 bps total for 3 legs, or 0 bps Maker)
    ZeroFeeFdusdLoop,
    /// Institutional VIP (3.0 bps per leg)
    Vip,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutedTrade {
    pub id: u64,
    pub strategy: &'static str,
    pub symbol: Symbol,
    pub route_desc: String,
    pub notional_usdt: f64,
    pub gross_spread_bps: f64,
    pub net_spread_bps: f64,
    pub fees_paid_usdt: f64,
    pub net_profit_usdt: f64,
    pub legged_out: bool,
    pub account_equity_usdt: f64,
    pub detection_latency_ns: u64,
}

/// Brutal Reality Execution Engine.
pub struct PaperExecutionEngine {
    pub mode: StrategyMode,
    pub realistic_friction: bool,
    pub starting_capital_usdt: f64,
    pub leverage: f64,
    /// Total live account equity in USDT
    pub current_equity_usdt: f64,
    /// Per-exchange collateral for Cross-Exchange modes
    pub usdt_balances: [f64; NUM_EXCHANGES],
    pub asset_balances: [[f64; NUM_EXCHANGES]; NUM_SYMBOLS],
    pub fee_bps: [f64; NUM_EXCHANGES],
    last_exec_ns: [[[u64; NUM_EXCHANGES]; NUM_EXCHANGES]; NUM_SYMBOLS],
    pub cooldown_ns: u64,
    /// Rate limiter: timestamps of recent order submissions
    order_timestamps_ns: Vec<u64>,
    pub max_orders_per_sec: usize,
    pub min_order_notional_usdt: f64,
    pub leg_miss_probability: f64,
    rng: StdRng,
    pub trades: Vec<ExecutedTrade>,
    pub cumulative_net_pnl_usdt: f64,
    pub cumulative_gross_pnl_usdt: f64,
    pub cumulative_fees_usdt: f64,
    pub cumulative_volume_usdt: f64,
    pub total_wins: u64,
    pub total_legged_losses: u64,
    pub skipped_min_notional: u64,
    pub skipped_rate_limit: u64,
    pub skipped_cooldown: u64,
}

impl PaperExecutionEngine {
    pub fn new(
        starting_capital_usdt: f64,
        mode: StrategyMode,
        fee_bps: [f64; NUM_EXCHANGES],
        realistic_friction: bool,
    ) -> Self {
        let leverage = match mode {
            StrategyMode::CrossExchangePerp => 5.0,
            _ => 1.0,
        };

        let usdt_balances = match mode {
            StrategyMode::SameExchangeTriangular | StrategyMode::ZeroFeeHftObiScalp => {
                [starting_capital_usdt, 0.0, 0.0]
            }
            StrategyMode::CrossExchangePerp => [
                starting_capital_usdt / 2.0,
                starting_capital_usdt / 2.0,
                0.0,
            ],
            StrategyMode::CrossExchangeSpot => [
                starting_capital_usdt / 3.0,
                starting_capital_usdt / 3.0,
                starting_capital_usdt / 3.0,
            ],
        };

        let mut asset_balances = [[0.0; NUM_EXCHANGES]; NUM_SYMBOLS];
        if mode == StrategyMode::CrossExchangeSpot {
            let per_ex = starting_capital_usdt / 6.0;
            for ex in 0..NUM_EXCHANGES {
                asset_balances[Symbol::BtcUsdt.index()][ex] = per_ex / 91_400.0;
                asset_balances[Symbol::EthUsdt.index()][ex] = per_ex / 3_180.0;
                asset_balances[Symbol::SolUsdt.index()][ex] = per_ex / 214.0;
            }
        }

        let cooldown_ns = match mode {
            StrategyMode::ZeroFeeHftObiScalp => 25_000_000, // 25ms HFT micro-scalp cadence
            _ => 65_000_000,
        };

        Self {
            mode,
            realistic_friction,
            starting_capital_usdt,
            leverage,
            current_equity_usdt: starting_capital_usdt,
            usdt_balances,
            asset_balances,
            fee_bps,
            last_exec_ns: [[[0; NUM_EXCHANGES]; NUM_EXCHANGES]; NUM_SYMBOLS],
            cooldown_ns,
            order_timestamps_ns: Vec::with_capacity(256),
            max_orders_per_sec: if realistic_friction { 18 } else { 1000 },
            min_order_notional_usdt: 5.0,
            leg_miss_probability: if realistic_friction { 0.16 } else { 0.0 },
            rng: StdRng::seed_from_u64(99_2026),
            trades: Vec::with_capacity(1024),
            cumulative_net_pnl_usdt: 0.0,
            cumulative_gross_pnl_usdt: 0.0,
            cumulative_fees_usdt: 0.0,
            cumulative_volume_usdt: 0.0,
            total_wins: 0,
            total_legged_losses: 0,
            skipped_min_notional: 0,
            skipped_rate_limit: 0,
            skipped_cooldown: 0,
        }
    }

    pub fn try_execute(&mut self, signal: &ArbitrageSignal) -> Option<ExecutedTrade> {
        let sym_idx = signal.symbol.index();
        let buy_idx = signal.buy_exchange.index();
        let sell_idx = signal.sell_exchange.index();

        let last_ns = self.last_exec_ns[sym_idx][buy_idx][sell_idx];
        if last_ns > 0 && signal.timestamp_ns.saturating_sub(last_ns) < self.cooldown_ns {
            self.skipped_cooldown += 1;
            return None;
        }

        let one_sec_ns = 1_000_000_000;
        self.order_timestamps_ns
            .retain(|&ts| signal.timestamp_ns.saturating_sub(ts) < one_sec_ns);
        if self.order_timestamps_ns.len() >= self.max_orders_per_sec {
            self.skipped_rate_limit += 1;
            return None;
        }

        let (usable_notional_usdt, route_desc, strategy_label, round_trip_fee_bps) = match self.mode
        {
            StrategyMode::SameExchangeTriangular => {
                let notional = (self.current_equity_usdt * 0.95)
                    .min(signal.max_executable_qty * signal.buy_ask_price);
                let desc = format!(
                    "Binance Loop: USDT -> FDUSD -> {} -> USDT",
                    signal.symbol.as_str()
                );
                (notional, desc, "SameExch-Triangular", 3.75)
            }
            StrategyMode::ZeroFeeHftObiScalp => {
                let notional = (self.current_equity_usdt * 0.95)
                    .min(signal.max_executable_qty * signal.buy_ask_price);
                let pair = match signal.symbol {
                    Symbol::BtcUsdt => "BTCFDUSD",
                    Symbol::EthUsdt => "ETHFDUSD",
                    Symbol::SolUsdt => "SOLFDUSD",
                };
                let desc = format!("Binance HFT 0% OBI Scalp: {} LIMIT_MAKER", pair);
                // 0.00% Maker Fee on both Buy and Sell on Binance FDUSD pairs
                (notional, desc, "ZeroFee-HFT-OBI-Scalp", 0.0)
            }
            StrategyMode::CrossExchangePerp => {
                if buy_idx == 2 || sell_idx == 2 {
                    return None;
                }
                let avail_margin = self.usdt_balances[buy_idx].min(self.usdt_balances[sell_idx]);
                let max_buying_power = avail_margin * self.leverage * 0.90;
                let notional =
                    max_buying_power.min(signal.max_executable_qty * signal.buy_ask_price);
                let desc = format!(
                    "Perp 5x: LONG {} @ ${:.2} / SHORT {} @ ${:.2}",
                    signal.buy_exchange,
                    signal.buy_ask_price,
                    signal.sell_exchange,
                    signal.sell_bid_price
                );
                let fee = self.fee_bps[buy_idx] + self.fee_bps[sell_idx];
                (notional, desc, "CrossExch-Perp-5x", fee)
            }
            StrategyMode::CrossExchangeSpot => {
                let avail_usdt = self.usdt_balances[buy_idx] * 0.95;
                let avail_base_usdt =
                    self.asset_balances[sym_idx][sell_idx] * signal.buy_ask_price * 0.95;
                let notional = avail_usdt
                    .min(avail_base_usdt)
                    .min(signal.max_executable_qty * signal.buy_ask_price);
                let desc = format!(
                    "Spot: BUY {} @ ${:.2} -> SELL {} @ ${:.2}",
                    signal.buy_exchange,
                    signal.buy_ask_price,
                    signal.sell_exchange,
                    signal.sell_bid_price
                );
                let fee = self.fee_bps[buy_idx] + self.fee_bps[sell_idx];
                (notional, desc, "CrossExch-Spot", fee)
            }
        };

        if usable_notional_usdt < self.min_order_notional_usdt {
            self.skipped_min_notional += 1;
            return None;
        }

        self.order_timestamps_ns.push(signal.timestamp_ns);
        self.last_exec_ns[sym_idx][buy_idx][sell_idx] = signal.timestamp_ns;

        let legged_out =
            self.realistic_friction && self.rng.gen_bool(self.leg_miss_probability);

        let (gross_profit_usdt, total_fees_usdt, net_profit_usdt, effective_net_bps) = if legged_out
        {
            if self.mode == StrategyMode::ZeroFeeHftObiScalp {
                // 2-Second Time-Stop Scratch on Zero-Fee FDUSD pair: 0.00% fee, ~0.65 bps 1-tick scratch
                let scratch_bps = 0.65;
                let scratch_loss = -(usable_notional_usdt * (scratch_bps / 10_000.0));
                self.total_legged_losses += 1;
                (scratch_loss, 0.0, scratch_loss, -scratch_bps)
            } else {
                let unwind_fee_bps = round_trip_fee_bps * 0.65;
                let adverse_slip_bps = 1.80;
                let total_loss_bps = unwind_fee_bps + adverse_slip_bps;
                let fees_usdt = usable_notional_usdt * (unwind_fee_bps / 10_000.0);
                let slip_loss_usdt = usable_notional_usdt * (adverse_slip_bps / 10_000.0);
                let net_loss = -(fees_usdt + slip_loss_usdt);
                self.total_legged_losses += 1;
                (-slip_loss_usdt, fees_usdt, net_loss, -total_loss_bps)
            }
        } else {
            let depth_slippage_bps = if self.mode == StrategyMode::ZeroFeeHftObiScalp {
                0.20 // Passive LIMIT_MAKER fill has near-zero slippage
            } else if self.realistic_friction {
                0.85
            } else {
                0.30
            };
            let gross_usdt = usable_notional_usdt * (signal.gross_spread_bps / 10_000.0);
            let fees_usdt =
                usable_notional_usdt * ((round_trip_fee_bps + depth_slippage_bps) / 10_000.0);
            let net_usdt = gross_usdt - fees_usdt;
            let net_bps = signal.gross_spread_bps - round_trip_fee_bps - depth_slippage_bps;

            if net_usdt <= 0.0 {
                return None;
            }
            self.total_wins += 1;
            (gross_usdt, fees_usdt, net_usdt, net_bps)
        };

        self.current_equity_usdt += net_profit_usdt;
        match self.mode {
            StrategyMode::SameExchangeTriangular | StrategyMode::ZeroFeeHftObiScalp => {
                self.usdt_balances[0] = self.current_equity_usdt;
            }
            StrategyMode::CrossExchangePerp => {
                self.usdt_balances[buy_idx] += net_profit_usdt * 0.5;
                self.usdt_balances[sell_idx] += net_profit_usdt * 0.5;
            }
            StrategyMode::CrossExchangeSpot => {
                self.usdt_balances[buy_idx] += net_profit_usdt * 0.5;
                self.usdt_balances[sell_idx] += net_profit_usdt * 0.5;
            }
        }

        self.cumulative_gross_pnl_usdt += gross_profit_usdt;
        self.cumulative_fees_usdt += total_fees_usdt;
        self.cumulative_net_pnl_usdt += net_profit_usdt;
        self.cumulative_volume_usdt += usable_notional_usdt * 2.0;

        let trade = ExecutedTrade {
            id: (self.trades.len() + 1) as u64,
            strategy: strategy_label,
            symbol: signal.symbol,
            route_desc,
            notional_usdt: usable_notional_usdt,
            gross_spread_bps: signal.gross_spread_bps,
            net_spread_bps: effective_net_bps,
            fees_paid_usdt: total_fees_usdt,
            net_profit_usdt,
            legged_out,
            account_equity_usdt: self.current_equity_usdt,
            detection_latency_ns: signal.detection_latency_ns,
        };

        self.trades.push(trade.clone());
        Some(trade)
    }

    pub fn win_rate_pct(&self) -> f64 {
        let total = self.total_wins + self.total_legged_losses;
        if total == 0 {
            0.0
        } else {
            (self.total_wins as f64 / total as f64) * 100.0
        }
    }
}
