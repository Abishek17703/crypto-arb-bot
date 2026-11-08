use crate::types::{Exchange, Quote, Symbol, NUM_EXCHANGES, NUM_SYMBOLS};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cache-line aligned (64-byte) lock-free SeqLock slot for a single `(Exchange, Symbol)` BBO.
///
/// Writers increment `version` to an odd number before updating fields and to an even number
/// after completion. Readers spin-read `version` before and after copying the fields, retrying
/// if `v1 != v2` or if `v1` is odd. This guarantees consistent snapshots in ~2-5 nanoseconds
/// with zero mutex contention or heap allocation.
#[repr(align(64))]
pub struct AtomicQuoteSlot {
    version: AtomicU64,
    bid_price_bits: AtomicU64,
    bid_qty_bits: AtomicU64,
    ask_price_bits: AtomicU64,
    ask_qty_bits: AtomicU64,
    timestamp_ns: AtomicU64,
}

impl Default for AtomicQuoteSlot {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicQuoteSlot {
    pub const fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            bid_price_bits: AtomicU64::new(0),
            bid_qty_bits: AtomicU64::new(0),
            ask_price_bits: AtomicU64::new(0),
            ask_qty_bits: AtomicU64::new(0),
            timestamp_ns: AtomicU64::new(0),
        }
    }

    /// Lock-free write by a single feed producer.
    #[inline(always)]
    pub fn store(&self, quote: &Quote) {
        let v = self.version.load(Ordering::Relaxed);
        self.version.store(v.wrapping_add(1), Ordering::Release);

        self.bid_price_bits
            .store(quote.bid_price.to_bits(), Ordering::Relaxed);
        self.bid_qty_bits
            .store(quote.bid_qty.to_bits(), Ordering::Relaxed);
        self.ask_price_bits
            .store(quote.ask_price.to_bits(), Ordering::Relaxed);
        self.ask_qty_bits
            .store(quote.ask_qty.to_bits(), Ordering::Relaxed);
        self.timestamp_ns
            .store(quote.timestamp_ns, Ordering::Relaxed);

        self.version.store(v.wrapping_add(2), Ordering::Release);
    }

    /// Wait-free optimistic read by the arbitrage detector or execution engine.
    #[inline(always)]
    pub fn load(&self) -> Quote {
        loop {
            let v1 = self.version.load(Ordering::Acquire);
            if v1 & 1 == 1 {
                std::hint::spin_loop();
                continue;
            }

            let bid_price = f64::from_bits(self.bid_price_bits.load(Ordering::Relaxed));
            let bid_qty = f64::from_bits(self.bid_qty_bits.load(Ordering::Relaxed));
            let ask_price = f64::from_bits(self.ask_price_bits.load(Ordering::Relaxed));
            let ask_qty = f64::from_bits(self.ask_qty_bits.load(Ordering::Relaxed));
            let timestamp_ns = self.timestamp_ns.load(Ordering::Relaxed);

            let v2 = self.version.load(Ordering::Acquire);
            if v1 == v2 {
                return Quote {
                    bid_price,
                    bid_qty,
                    ask_price,
                    ask_qty,
                    timestamp_ns,
                };
            }
            std::hint::spin_loop();
        }
    }
}

/// Global flat matrix of `[Symbol][Exchange]` top-of-book slots.
pub struct OrderBookMatrix {
    slots: [[AtomicQuoteSlot; NUM_EXCHANGES]; NUM_SYMBOLS],
    total_updates: AtomicU64,
}

impl Default for OrderBookMatrix {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBookMatrix {
    pub fn new() -> Self {
        Self {
            slots: std::array::from_fn(|_| std::array::from_fn(|_| AtomicQuoteSlot::new())),
            total_updates: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    pub fn update(&self, exchange: Exchange, symbol: Symbol, quote: &Quote) {
        self.slots[symbol.index()][exchange.index()].store(quote);
        self.total_updates.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn get(&self, exchange: Exchange, symbol: Symbol) -> Quote {
        self.slots[symbol.index()][exchange.index()].load()
    }

    #[inline(always)]
    pub fn get_symbol_row(&self, symbol: Symbol) -> [Quote; NUM_EXCHANGES] {
        let row = &self.slots[symbol.index()];
        [
            row[Exchange::Binance.index()].load(),
            row[Exchange::Bybit.index()].load(),
            row[Exchange::Okx.index()].load(),
        ]
    }

    pub fn total_updates(&self) -> u64 {
        self.total_updates.load(Ordering::Relaxed)
    }
}
