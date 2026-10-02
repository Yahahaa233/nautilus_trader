//! Exact historical market deques. These are source state, never fresh observations.
use ahash::AHashMap;
use anyhow::{Context, Result, ensure};
use nautilus_model::{
    data::{
        Bar, BarType, FundingRateUpdate, IndexPriceUpdate, MarkPriceUpdate, QuoteTick, TradeTick,
    },
    enums::AggregationSource,
    instruments::{Instrument, InstrumentAny},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

use super::{Cache, bounded::BoundedVecDeque, config::CacheConfig};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstrumentHistory {
    instrument: InstrumentAny,
    quotes: Option<Vec<QuoteTick>>,
    trades: Option<Vec<TradeTick>>,
    marks: Option<Vec<MarkPriceUpdate>>,
    index_prices: Option<Vec<IndexPriceUpdate>>,
    funding: Option<Vec<FundingRateUpdate>>,
    bars: Vec<BarHistory>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BarHistory {
    #[serde(rename = "type")]
    bar_type: BarType,
    bars: Option<Vec<Bar>>,
}

fn deque<T: Copy>(items: Vec<T>, capacity: usize) -> Result<BoundedVecDeque<T>> {
    ensure!(
        items.len() <= capacity,
        "source market history exceeds original capacity"
    );
    let mut result = BoundedVecDeque::new(capacity);
    // Cache exports the actual newest-first deque. Reverse insertion, not sorting,
    // is the exact inverse of push_front; equal timestamps retain original order.
    for value in items.into_iter().rev() {
        result.push_front(value);
    }
    Ok(result)
}

impl Cache {
    /// Actual source configuration must be captured alongside the market histories.
    #[must_use]
    pub const fn native_market_checkpoint_config(&self) -> &CacheConfig {
        &self.config
    }

    /// Exports all represented market histories in their actual deque order.
    /// # Errors
    /// Refuses histories whose instrument is absent from the native instrument roster.
    pub fn native_market_checkpoint(&self) -> Result<Value> {
        let mut ids = self
            .instrument_ids(None)
            .into_iter()
            .copied()
            .collect::<Vec<_>>();
        ids.sort();
        let roster: BTreeSet<_> = ids.iter().copied().collect();
        ensure!(
            self.quotes
                .keys()
                .chain(self.trades.keys())
                .chain(self.mark_prices.keys())
                .chain(self.index_prices.keys())
                .chain(self.funding_rates.keys())
                .all(|id| roster.contains(id))
                && self
                    .bars
                    .keys()
                    .all(|kind| roster.contains(&kind.instrument_id())),
            "source market history references an unregistered instrument"
        );
        let mut instruments = Vec::new();
        for id in ids {
            let mut kinds = self
                .bar_types(Some(&id), None, AggregationSource::External)
                .into_iter()
                .copied()
                .collect::<Vec<_>>();
            kinds.extend(
                self.bar_types(Some(&id), None, AggregationSource::Internal)
                    .into_iter()
                    .copied(),
            );
            kinds.sort_by_key(ToString::to_string);
            instruments.push(InstrumentHistory {
                instrument: self
                    .instrument(&id)
                    .context("source instrument missing")?
                    .clone(),
                quotes: self.quotes(&id),
                trades: self.trades(&id),
                marks: self.mark_prices(&id),
                index_prices: self.index_prices(&id),
                funding: self.funding_rates(&id),
                bars: kinds
                    .into_iter()
                    .map(|bar_type| BarHistory {
                        bar_type,
                        bars: self.bars(&bar_type),
                    })
                    .collect(),
            });
        }
        Ok(serde_json::to_value(instruments)?)
    }

    /// Installs historical deques into an empty, isolated in-memory market cache.
    /// The node's reader-issued source/frontier must authorize this operation;
    /// this low-level cache operation creates no permission or freshness proof.
    /// # Errors
    /// Rejects changed/missing configuration, instruments, noncanonical payloads,
    /// existing target histories or any source history exceeding its original capacity.
    pub fn restore_native_market_checkpoint(
        &mut self,
        config: &Value,
        source: &Value,
    ) -> Result<()> {
        ensure!(
            !self.has_backing(),
            "market restoration cannot write through a persistence backing"
        );
        let original: CacheConfig = serde_json::from_value(config.clone())
            .context("source market cache config missing or invalid")?;
        ensure!(
            serde_json::to_value(&original)? == *config && original == self.config,
            "original market cache configuration differs"
        );
        ensure!(
            self.quotes.is_empty()
                && self.trades.is_empty()
                && self.mark_prices.is_empty()
                && self.index_prices.is_empty()
                && self.funding_rates.is_empty()
                && self.bars.is_empty(),
            "target market histories are not empty"
        );
        let histories: Vec<InstrumentHistory> =
            serde_json::from_value(source.clone()).context("source market histories invalid")?;
        ensure!(
            serde_json::to_value(&histories)? == *source,
            "source market history is not canonical"
        );
        let mut ids = self
            .instrument_ids(None)
            .into_iter()
            .copied()
            .collect::<Vec<_>>();
        ids.sort();
        ensure!(
            histories
                .iter()
                .map(|h| h.instrument.id())
                .eq(ids.iter().copied()),
            "source market instrument roster or order differs"
        );
        // Prepare every map before changing the actual shared cache. No add_* call
        // may evict, publish, persist or reinterpret a historical ordering here.
        let mut quotes = AHashMap::new();
        let mut trades = AHashMap::new();
        let mut marks = AHashMap::new();
        let mut indexes = AHashMap::new();
        let mut funding = AHashMap::new();
        let mut bars = AHashMap::new();
        for history in histories {
            let id = history.instrument.id();
            ensure!(
                serde_json::to_value(self.instrument(&id).context("target instrument absent")?)?
                    == serde_json::to_value(&history.instrument)?,
                "source market instrument specification differs"
            );
            macro_rules! history {
                ($items:expr, $map:ident) => {
                    if let Some(items) = $items {
                        ensure!(
                            items.iter().all(|item| item.instrument_id == id),
                            "market history instrument identity differs"
                        );
                        $map.insert(id, deque(items, original.tick_capacity)?);
                    }
                };
            }
            history!(history.quotes, quotes);
            history!(history.trades, trades);
            history!(history.marks, marks);
            history!(history.index_prices, indexes);
            history!(history.funding, funding);
            let mut kinds = BTreeSet::new();
            let mut previous_kind = None;
            for group in history.bars {
                let name = group.bar_type.to_string();
                ensure!(
                    group.bar_type.instrument_id() == id
                        && kinds.insert(name.clone())
                        && previous_kind
                            .as_ref()
                            .is_none_or(|previous| previous < &name),
                    "bar history owner or original type order differs"
                );
                previous_kind = Some(name);
                let items = group.bars.context("source bar history deque missing")?;
                ensure!(
                    items.iter().all(|bar| bar.bar_type == group.bar_type),
                    "bar history type identity differs"
                );
                bars.insert(group.bar_type, deque(items, original.bar_capacity)?);
            }
        }
        self.quotes = quotes;
        self.trades = trades;
        self.mark_prices = marks;
        self.index_prices = indexes;
        self.funding_rates = funding;
        self.bars = bars;
        ensure!(
            self.native_market_checkpoint()? == *source,
            "restored market history differs from original deque bytes"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::stubs::{quote_ethusdt_binance, stub_trade_ethusdt_buy},
        instruments::stubs::crypto_perpetual_ethusdt,
        types::Price,
    };
    use rust_decimal::Decimal;

    fn empty() -> Cache {
        let mut cache = Cache::new(
            Some(CacheConfig {
                tick_capacity: 3,
                bar_capacity: 2,
                ..Default::default()
            }),
            None,
        );
        cache
            .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
            .unwrap();
        cache
    }

    fn append(cache: &mut Cache, ordinal: u64) {
        let id = crypto_perpetual_ethusdt().id();
        // Deliberately equal timestamps: deque order must not be inferred by sorting.
        let time = UnixNanos::from(100);
        let price = Price::new(1_000.0 + ordinal as f64, 2);
        let mut quote = quote_ethusdt_binance();
        quote.instrument_id = id;
        quote.bid_price = price;
        quote.ts_event = time;
        quote.ts_init = time;
        cache.add_quote(quote).unwrap();
        let mut trade = stub_trade_ethusdt_buy();
        trade.instrument_id = id;
        trade.price = price;
        trade.ts_event = time;
        trade.ts_init = time;
        cache.add_trade(trade).unwrap();
        cache
            .add_mark_price(MarkPriceUpdate::new(id, price, time, time))
            .unwrap();
        cache
            .add_index_price(IndexPriceUpdate::new(id, price, time, time))
            .unwrap();
        cache
            .add_funding_rate(FundingRateUpdate::new(
                id,
                Decimal::new(ordinal as i64, 4),
                Some(480),
                Some(UnixNanos::from(200)),
                time,
                time,
            ))
            .unwrap();
        let mut bar = Bar::default();
        bar.bar_type = BarType::from("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL");
        bar.open = price;
        bar.high = price;
        bar.low = price;
        bar.close = price;
        bar.ts_event = time;
        bar.ts_init = time;
        cache.add_bar(bar).unwrap();
    }

    #[test]
    fn native_market_checkpoint_all_six_histories_preserve_order_and_capacity() {
        let mut source = empty();
        for n in 1..=4 {
            append(&mut source, n);
        }
        let config = serde_json::to_value(source.native_market_checkpoint_config()).unwrap();
        let histories = source.native_market_checkpoint().unwrap();
        assert_eq!(histories[0]["quotes"].as_array().unwrap().len(), 3);
        assert_eq!(histories[0]["bars"][0]["bars"].as_array().unwrap().len(), 2);
        let mut target = empty();
        target
            .restore_native_market_checkpoint(&config, &histories)
            .unwrap();
        assert_eq!(target.native_market_checkpoint().unwrap(), histories);
        for n in 5..=6 {
            append(&mut source, n);
            append(&mut target, n);
            assert_eq!(
                target.native_market_checkpoint().unwrap(),
                source.native_market_checkpoint().unwrap()
            );
        }
        assert!(
            target
                .restore_native_market_checkpoint(&config, &histories)
                .is_err()
        );
    }

    #[test]
    fn native_market_checkpoint_rejects_changed_config_identity_or_over_capacity() {
        let mut source = empty();
        append(&mut source, 1);
        let config = serde_json::to_value(source.native_market_checkpoint_config()).unwrap();
        let histories = source.native_market_checkpoint().unwrap();
        let mut changed_config = config.clone();
        changed_config["tick_capacity"] = 4.into();
        let mut target = empty();
        let before = target.native_market_checkpoint().unwrap();
        assert!(
            target
                .restore_native_market_checkpoint(&changed_config, &histories)
                .is_err()
        );
        let mut missing = config.clone();
        missing.as_object_mut().unwrap().remove("tick_capacity");
        assert!(
            target
                .restore_native_market_checkpoint(&missing, &histories)
                .is_err()
        );
        let mut changed = histories.clone();
        changed[0]["quotes"][0]["instrument_id"] = "OTHER.BINANCE".into();
        assert!(
            target
                .restore_native_market_checkpoint(&config, &changed)
                .is_err()
        );
        let mut changed_spec = histories.clone();
        changed_spec[0]["instrument"]["price_precision"] = 8.into();
        assert!(
            target
                .restore_native_market_checkpoint(&config, &changed_spec)
                .is_err()
        );
        let mut oversized = histories.clone();
        let quote = oversized[0]["quotes"][0].clone();
        oversized[0]["quotes"] = serde_json::json!([quote, quote, quote, quote]);
        assert!(
            target
                .restore_native_market_checkpoint(&config, &oversized)
                .is_err()
        );
        assert_eq!(target.native_market_checkpoint().unwrap(), before);
    }
}
