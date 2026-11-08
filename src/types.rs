use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported exchanges indexed from 0..NUM_EXCHANGES for zero-overhead array indexing.
pub const NUM_EXCHANGES: usize = 3;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Exchange {
    Binance = 0,
    Bybit = 1,
    Okx = 2,
}

impl Exchange {
    pub const ALL: [Exchange; NUM_EXCHANGES] = [Exchange::Binance, Exchange::Bybit, Exchange::Okx];

    #[inline(always)]
    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Exchange::Binance => "Binance",
            Exchange::Bybit => "Bybit",
            Exchange::Okx => "OKX",
        }
    }

    /// Default taker fee in basis points (1 bp = 0.01% = 0.0001).
    /// VIP / market-maker tiers can be configured via CLI.
    #[inline(always)]
    pub const fn default_taker_fee_bps(self) -> f64 {
        match self {
            Exchange::Binance => 7.5, // 0.075% with BNB discount
            Exchange::Bybit => 10.0,  // 0.100% standard spot taker
            Exchange::Okx => 8.0,     // 0.080% standard spot taker
        }
    }
}

impl fmt::Display for Exchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Supported trading pairs indexed from 0..NUM_SYMBOLS for lock-free matrix lookups.
pub const NUM_SYMBOLS: usize = 3;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Symbol {
    BtcUsdt = 0,
    EthUsdt = 1,
    SolUsdt = 2,
}

impl Symbol {
    pub const ALL: [Symbol; NUM_SYMBOLS] = [Symbol::BtcUsdt, Symbol::EthUsdt, Symbol::SolUsdt];

    #[inline(always)]
    pub const fn index(self) -> usize {
        self as usize
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC/USDT",
            Symbol::EthUsdt => "ETH/USDT",
            Symbol::SolUsdt => "SOL/USDT",
        }
    }

    pub const fn base_asset(self) -> &'static str {
        match self {
            Symbol::BtcUsdt => "BTC",
            Symbol::EthUsdt => "ETH",
            Symbol::SolUsdt => "SOL",
        }
    }

    pub fn from_exchange_symbol(s: &str) -> Option<Self> {
        let normalized = s.to_ascii_uppercase().replace(['-', '_', '/'], "");
        match normalized.as_str() {
            "BTCUSDT" => Some(Symbol::BtcUsdt),
            "ETHUSDT" => Some(Symbol::EthUsdt),
            "SOLUSDT" => Some(Symbol::SolUsdt),
            _ => None,
        }
    }
}

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Best Bid & Offer (Top-of-Book) quote snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub struct Quote {
    pub bid_price: f64,
    pub bid_qty: f64,
    pub ask_price: f64,
    pub ask_qty: f64,
    /// Monotonic nanosecond timestamp when the quote was received locally.
    pub timestamp_ns: u64,
}

impl Quote {
    #[inline(always)]
    pub fn is_valid(&self) -> bool {
        self.bid_price > 0.0
            && self.ask_price > 0.0
            && self.ask_price >= self.bid_price
            && self.bid_qty > 0.0
            && self.ask_qty > 0.0
    }
}

/// Detected cross-exchange arbitrage signal ready for immediate execution.
#[derive(Debug, Clone)]
pub struct ArbitrageSignal {
    pub symbol: Symbol,
    pub buy_exchange: Exchange,
    pub sell_exchange: Exchange,
    pub buy_ask_price: f64,
    pub sell_bid_price: f64,
    pub max_executable_qty: f64,
    pub gross_spread_bps: f64,
    pub net_spread_bps: f64,
    pub expected_net_profit_usdt: f64,
    pub detection_latency_ns: u64,
    pub timestamp_ns: u64,
}
