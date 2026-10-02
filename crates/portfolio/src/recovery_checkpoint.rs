// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.

//! The actual Portfolio cut, including calculation caches and original samples.
//! AccountsManager owns only the already-installed cache/clock references; its
//! business data lives in that cache and in the fields sealed below.
use std::sync::Arc;

use anyhow::{Context, Result, ensure};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use super::{Portfolio, PortfolioState, SNAPSHOT_BUFFER_CAP};

fn sorted_map<'a, K: Serialize + 'a, V: Serialize + 'a>(
    entries: impl Iterator<Item = (&'a K, &'a V)>,
) -> Result<Value> {
    let mut rows = entries
        .map(|(key, value)| Ok((serde_json::to_value(key)?, serde_json::to_value(value)?)))
        .collect::<Result<Vec<(Value, Value)>>>()?;
    rows.sort_by_cached_key(|(key, _)| key.to_string());
    Ok(serde_json::to_value(rows)?)
}

fn sorted_set<'a, K: Serialize + 'a>(items: impl Iterator<Item = &'a K>) -> Result<Value> {
    let mut rows = items
        .map(serde_json::to_value)
        .collect::<serde_json::Result<Vec<_>>>()?;
    rows.sort_by_cached_key(Value::to_string);
    Ok(Value::Array(rows))
}

fn decode<T: DeserializeOwned>(object: &Value, name: &str) -> Result<T> {
    serde_json::from_value(
        object
            .get(name)
            .with_context(|| format!("Portfolio field missing: {name}"))?
            .clone(),
    )
    .with_context(|| format!("invalid original Portfolio field: {name}"))
}

impl Portfolio {
    fn checkpoint_for_state(&self, state: &PortfolioState) -> Result<Value> {
        ensure!(
            state.analyzer.statistics.len() == state.native_statistic_factories.len()
                && state.analyzer.statistics.iter().all(|(name, factory)| state
                    .native_statistic_factories
                    .get(name)
                    .is_some_and(|original| Arc::ptr_eq(factory, original))),
            "custom Portfolio statistic factory recovery unsupported"
        );
        // f64 JSON must not silently turn non-finite historical returns into null.
        ensure!(
            state
                .analyzer
                .realized_pnls
                .values()
                .chain(state.analyzer.recorded_realized_pnls.values())
                .flat_map(|rows| rows.iter())
                .all(|row| row.2.is_finite())
                && state
                    .analyzer
                    .position_returns
                    .values()
                    .chain(state.analyzer.portfolio_returns.values())
                    .chain(state.analyzer.returns.values())
                    .all(|value| value.is_finite()),
            "Portfolio analysis contains non-finite historical values"
        );
        let analyzer = &state.analyzer;
        let mut account_ids = self
            .cache
            .try_borrow()?
            .accounts_all_owned()
            .iter()
            .map(nautilus_model::accounts::AccountAny::id)
            .collect::<Vec<_>>();
        account_ids.sort_by_key(ToString::to_string);
        let mut missing_prices = state
            .venues_missing_price
            .iter()
            .map(|(venue, accounts)| {
                let mut rows = accounts
                    .iter()
                    .map(|(account, instruments)| {
                        Ok((
                            serde_json::to_value(account)?,
                            sorted_set(instruments.iter())?,
                        ))
                    })
                    .collect::<Result<Vec<(Value, Value)>>>()?;
                rows.sort_by_cached_key(|(account, _)| account.to_string());
                Ok((serde_json::to_value(venue)?, serde_json::to_value(rows)?))
            })
            .collect::<Result<Vec<(Value, Value)>>>()?;
        missing_prices.sort_by_cached_key(|(venue, _)| venue.to_string());
        let mut value = json!({
            "schema": "NautilusPortfolioCheckpoint.v1",
            "configuration": self.config,
            "registered_accounts": account_ids,
            "analyzer": {
                "factory_profile": "native_default_portfolio_analyzer.v1",
                "statistics": sorted_set(analyzer.statistics.keys())?,
                "account_balances_starting": analyzer.account_balances_starting.iter().collect::<Vec<_>>(),
                "account_balances": analyzer.account_balances.iter().collect::<Vec<_>>(),
                "positions": analyzer.positions,
                "realized_pnls": sorted_map(analyzer.realized_pnls.iter())?,
                "recorded_realized_pnls": sorted_map(analyzer.recorded_realized_pnls.iter())?,
                "position_returns": analyzer.position_returns.iter().collect::<Vec<_>>(),
                "portfolio_returns": analyzer.portfolio_returns.iter().collect::<Vec<_>>(),
                "returns": analyzer.returns.iter().collect::<Vec<_>>()
            },
            // Preserve IndexMap order; hash collections below have canonical key order.
            "unrealized_pnls": state.unrealized_pnls.iter().collect::<Vec<_>>(),
            "realized_pnls": state.realized_pnls.iter().collect::<Vec<_>>(),
            "net_positions": state.net_positions.iter().collect::<Vec<_>>(),
            "initialized": state.initialized,
            "min_account_state_logging_interval_ns": state.min_account_state_logging_interval_ns,
            "equity_curve_finalized": state.equity_curve_finalized,
            "venues_missing_price": missing_prices
        });
        macro_rules! maps {
            ($($field:ident),+ $(,)?) => { $(
                value[stringify!($field)] = sorted_map(state.$field.iter())?;
            )+ };
        }
        macro_rules! sets {
            ($($field:ident),+ $(,)?) => { $(
                value[stringify!($field)] = sorted_set(state.$field.iter())?;
            )+ };
        }
        maps!(
            snapshot_sum_per_position,
            snapshot_last_per_position,
            snapshot_processed_counts,
            snapshot_processed_revisions,
            snapshot_account_ids,
            bar_close_prices,
            last_prices,
            last_xrates,
            last_account_state_log_ts,
            account_open_positions,
            portfolio_snapshots
        );
        sets!(
            recorded_closed_position_cycles,
            snapshot_currency_mismatches,
            snapshot_aggregation_overflows,
            pending_calcs,
            stale_prices,
            stale_xrates,
            equity_curve_accounts,
            pre_position_fill_events
        );
        Ok(value)
    }

    /// Seals all supported native Portfolio history at the current actual cut.
    /// Original curve timestamps/event IDs and calculation caches are retained.
    /// # Errors
    /// Rejects borrowed state, custom statistic factories or non-finite analysis.
    pub fn running_checkpoint_state(&self) -> Result<Value> {
        let state = self.inner.try_borrow()?;
        self.checkpoint_for_state(&state)
    }

    /// Installs a complete original cut into this actual Portfolio, without
    /// callbacks, sampling, timers or new event IDs. Native caller admission and
    /// verified source/frontier are checked by LiveNode, separately from JSON.
    /// # Errors
    /// Rejects missing/unknown fields, changed config/accounts/factory, duplicate
    /// rows and corrupt or oversized original series. Installs only after full
    /// canonical round-trip validation, retaining the actual cache/clock owners.
    pub fn restore_running_checkpoint_state(&mut self, value: &Value) -> Result<()> {
        ensure!(
            value["schema"] == "NautilusPortfolioCheckpoint.v1",
            "Portfolio source checkpoint profile missing or unsupported"
        );
        let current = self.running_checkpoint_state()?;
        ensure!(
            value["configuration"] == current["configuration"]
                && value["registered_accounts"] == current["registered_accounts"]
                && value["analyzer"]["factory_profile"] == current["analyzer"]["factory_profile"]
                && value["analyzer"]["statistics"] == current["analyzer"]["statistics"],
            "Portfolio source configuration/account/factory differs from actual target"
        );
        let mut prepared =
            PortfolioState::new(self.clock.clone(), self.cache.clone(), &self.config);
        prepared.native_statistic_factories =
            self.inner.try_borrow()?.native_statistic_factories.clone();
        prepared.analyzer.statistics = prepared.native_statistic_factories.clone();
        macro_rules! maps {
            ($($field:ident),+ $(,)?) => { $(
                prepared.$field = decode::<Vec<_>>(value, stringify!($field))?.into_iter().collect();
            )+ };
        }
        macro_rules! sets {
            ($($field:ident),+ $(,)?) => { $(
                prepared.$field = decode::<Vec<_>>(value, stringify!($field))?.into_iter().collect();
            )+ };
        }
        maps!(
            unrealized_pnls,
            realized_pnls,
            net_positions,
            snapshot_sum_per_position,
            snapshot_last_per_position,
            snapshot_processed_counts,
            snapshot_processed_revisions,
            snapshot_account_ids,
            bar_close_prices,
            last_prices,
            last_xrates,
            last_account_state_log_ts,
            account_open_positions,
            portfolio_snapshots
        );
        sets!(
            recorded_closed_position_cycles,
            snapshot_currency_mismatches,
            snapshot_aggregation_overflows,
            pending_calcs,
            stale_prices,
            stale_xrates,
            equity_curve_accounts,
            pre_position_fill_events
        );
        prepared.initialized = decode(value, "initialized")?;
        prepared.min_account_state_logging_interval_ns =
            decode(value, "min_account_state_logging_interval_ns")?;
        prepared.equity_curve_finalized = decode(value, "equity_curve_finalized")?;
        type MissingPrices = Vec<(
            nautilus_model::identifiers::Venue,
            Vec<(
                Option<nautilus_model::identifiers::AccountId>,
                Vec<nautilus_model::identifiers::InstrumentId>,
            )>,
        )>;
        prepared.venues_missing_price = decode::<MissingPrices>(value, "venues_missing_price")?
            .into_iter()
            .map(|(venue, rows)| {
                (
                    venue,
                    rows.into_iter()
                        .map(|(account, instruments)| (account, instruments.into_iter().collect()))
                        .collect(),
                )
            })
            .collect();
        let analyzer = value
            .get("analyzer")
            .context("Portfolio analysis history absent")?;
        macro_rules! analyzer_maps {
            ($($field:ident),+ $(,)?) => { $(
                prepared.analyzer.$field = decode::<Vec<_>>(analyzer, stringify!($field))?.into_iter().collect();
            )+ };
        }
        analyzer_maps!(
            account_balances_starting,
            account_balances,
            realized_pnls,
            recorded_realized_pnls,
            position_returns,
            portfolio_returns,
            returns
        );
        prepared.analyzer.positions = decode(analyzer, "positions")?;
        let registered: Vec<nautilus_model::identifiers::AccountId> =
            decode(value, "registered_accounts")?;
        ensure!(
            prepared
                .equity_curve_accounts
                .iter()
                .all(|id| registered.contains(id))
                && prepared
                    .portfolio_snapshots
                    .iter()
                    .all(|(id, series)| registered.contains(id)
                        && series.len() <= SNAPSHOT_BUFFER_CAP
                        && series.iter().all(|sample| sample.account_id == *id)),
            "Portfolio series/account association or original buffer bound invalid"
        );
        ensure!(
            self.checkpoint_for_state(&prepared)? == *value,
            "Portfolio checkpoint is not a complete canonical original state"
        );
        *self.inner.try_borrow_mut()? = prepared;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, TestClock},
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        accounts::AccountAny,
        enums::PositionSide,
        identifiers::{AccountId, InstrumentId},
        types::Price,
    };
    use rstest::rstest;
    use std::{cell::RefCell, rc::Rc};

    fn source() -> (Portfolio, Value) {
        let account = AccountAny::default();
        let event = account.last_event().unwrap();
        let concrete = Rc::new(RefCell::new(TestClock::new()));
        let clock: Rc<RefCell<dyn Clock>> = concrete.clone();
        let cache = Rc::new(RefCell::new(Cache::default()));
        cache.borrow_mut().add_account(account).unwrap();
        let mut portfolio = Portfolio::new(clock, cache, None);
        // Actual account callback records the registration baseline and installs
        // the original factory; the actual overdue clock event records midnight.
        portfolio.update_account(&event);
        let day = UnixNanos::from(86_400_000_000_000);
        let events = concrete.borrow_mut().advance_time(day, true);
        let handlers = concrete.borrow().match_handlers(events);
        assert_eq!(handlers.len(), 1);
        for handler in handlers {
            handler.callback.call(handler.event);
        }
        portfolio.inner.borrow_mut().last_prices.insert(
            (InstrumentId::from("ETHUSDT.BINANCE"), PositionSide::Long),
            Price::from("123.45"),
        );
        let checkpoint = portfolio.running_checkpoint_state().unwrap();
        (portfolio, checkpoint)
    }

    fn target(source: &Portfolio) -> Portfolio {
        let clock: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        for account in source.cache.borrow().accounts_all_owned() {
            cache.borrow_mut().add_account(account).unwrap();
        }
        Portfolio::new(clock, cache, None)
    }

    #[rstest]
    fn portfolio_checkpoint_preserves_original_series_and_calculation_history() {
        let (source, checkpoint) = source();
        let account = source.cache.borrow().accounts_all_owned()[0].id();
        let original = serde_json::to_value(source.snapshots(&account)).unwrap();
        assert_eq!(original.as_array().unwrap().len(), 2);
        assert_eq!(original[0]["ts_event"], 0);
        assert_eq!(original[1]["ts_event"], 86_400_000_000_000_u64);
        let mut target = target(&source);
        assert!(target.snapshots(&account).is_empty());
        target
            .restore_running_checkpoint_state(&checkpoint)
            .unwrap();
        assert_eq!(target.running_checkpoint_state().unwrap(), checkpoint);
        assert_eq!(
            serde_json::to_value(target.snapshots(&account)).unwrap(),
            original
        );
        assert_eq!(
            target.inner.borrow().last_prices
                [&(InstrumentId::from("ETHUSDT.BINANCE"), PositionSide::Long)],
            Price::from("123.45")
        );
        assert!(
            target.clock.borrow().timer_names().is_empty(),
            "state restore must not sample or arm timers"
        );
    }

    #[rstest]
    #[case("missing")]
    #[case("configuration")]
    #[case("account")]
    #[case("series_account")]
    #[case("duplicate_series")]
    #[case("factory")]
    fn portfolio_checkpoint_rejects_missing_or_changed_source_without_install(
        #[case] change: &str,
    ) {
        let (source, mut checkpoint) = source();
        let mut target = target(&source);
        let before = target.running_checkpoint_state().unwrap();
        match change {
            "missing" => {
                checkpoint
                    .as_object_mut()
                    .unwrap()
                    .remove("portfolio_snapshots");
            }
            "configuration" => checkpoint["configuration"]["equity_curve"] = false.into(),
            "account" => checkpoint["registered_accounts"] = json!([AccountId::from("OTHER-001")]),
            "series_account" => {
                checkpoint["portfolio_snapshots"][0][1][0]["account_id"] = "OTHER-001".into()
            }
            "duplicate_series" => {
                let duplicate = checkpoint["portfolio_snapshots"][0].clone();
                checkpoint["portfolio_snapshots"]
                    .as_array_mut()
                    .unwrap()
                    .push(duplicate);
            }
            "factory" => {
                checkpoint["analyzer"]["factory_profile"] = "caller_declared_default".into()
            }
            _ => unreachable!(),
        }
        assert!(
            target
                .restore_running_checkpoint_state(&checkpoint)
                .is_err()
        );
        assert_eq!(target.running_checkpoint_state().unwrap(), before);
        assert!(target.clock.borrow().timer_names().is_empty());
    }
}
