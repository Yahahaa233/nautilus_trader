// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Actual EXTERNAL-bar engine inventory; no internal pipeline is inferred empty.
use super::{BarType, ClientId, Context, DataEngine, DataEngineConfig, Venue};
use anyhow::{Result, ensure};
use std::collections::BTreeMap;
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    profile: String,
    configuration: DataEngineConfig,
    clients: Vec<ClientId>,
    default_client_id: Option<ClientId>,
    routing: Vec<(Venue, ClientId)>,
    subsystem_counts: BTreeMap<String, u64>,
    registered_stateless_helpers: BTreeMap<String, bool>,
    command_count: u64,
    data_count: u64,
    request_count: u64,
    response_count: u64,
    msgbus_priority: u32,
}
impl DataEngine {
    /// Projects every owned pipeline family from actual engine state.
    /// The restricted external-bar profile refuses unsupported/private pipelines.
    fn checkpoint_subsystem_counts(&self) -> Result<BTreeMap<String, u64>> {
        let mut counts = BTreeMap::new();
        macro_rules! count {
            ($field:ident) => {
                counts.insert(stringify!($field).into(), self.$field.len() as u64);
            };
        }
        counts.insert(
            "subscriptions_external".into(),
            self.subscriptions_external.iter().count() as u64,
        );
        count!(external_clients);
        count!(book_intervals);
        count!(book_snapshot_counts);
        count!(book_snapshot_sources);
        count!(book_deltas_counts);
        count!(book_depth_counts);
        count!(book_updaters);
        count!(book_subscriptions);
        count!(book_subscription_owners);
        count!(book_snapshotters);
        count!(bar_aggregators);
        count!(bar_aggregator_handlers);
        count!(subscriptions_bar_aggregation);
        count!(request_bar_aggregations);
        count!(request_pipeline_parent_request);
        count!(request_pipeline_n_components);
        count!(request_pipeline_parent_request_id);
        count!(request_pipeline_responses);
        count!(time_range_pipeline_requests);
        count!(time_range_pipeline_parent_request_id);
        count!(parent_join_request_id);
        count!(pending_join_requests);
        count!(continuous_future_requests);
        count!(continuous_future_subscriptions);
        counts.insert(
            "continuous_future_roller".into(),
            u64::from(self.continuous_future_roller.is_some()),
        );
        count!(spread_quote_states);
        count!(option_chain_managers);
        count!(option_chain_instrument_index);
        counts.insert(
            "deferred_cmd_queue".into(),
            self.deferred_cmd_queue.try_borrow()?.len() as u64,
        );
        counts.insert(
            "option_chain_bootstrapper".into(),
            u64::from(self.option_chain_bootstrapper.is_some()),
        );
        count!(pending_option_chain_requests);
        count!(option_chain_greeks_bootstraps);
        count!(synthetic_quote_feeds);
        count!(synthetic_trade_feeds);
        count!(subscribed_synthetic_quotes);
        count!(subscribed_synthetic_trades);
        count!(buffered_deltas_map);
        count!(deltas_frame);
        #[cfg(feature = "streaming")]
        count!(catalogs);
        #[cfg(feature = "defi")]
        {
            count!(pool_updaters);
            count!(pool_snapshot_pending);
            count!(pool_event_buffers);
        }
        Ok(counts)
    }
    /// Returns typed replay data only when actual owned internal pipelines are
    /// empty. A nonempty pipeline yields its exact family/count, never zero.
    ///
    /// # Errors
    /// Returns an error if the current native configuration, subscriptions, routes, or codec state cannot be encoded.
    pub fn running_checkpoint_state(&self) -> Result<serde_json::Value> {
        let counts = self.checkpoint_subsystem_counts()?;
        let mut registered_stateless_helpers = BTreeMap::new();
        // These two resident helpers own only a WeakCell<DataEngine>. Their
        // actual mutable state is in the corresponding engine maps above.
        macro_rules! stateless_helper {
            ($field:ident) => {{
                let registered = if let Some(helper) = &self.$field {
                    let actual = helper
                        .engine
                        .upgrade()
                        .context("native stateless helper owner disappeared")?;
                    ensure!(
                        std::ptr::eq(actual.as_ref().as_ptr(), self),
                        "native stateless helper is bound to another engine"
                    );
                    true
                } else {
                    false
                };
                registered_stateless_helpers.insert(stringify!($field).into(), registered);
            }};
        }
        stateless_helper!(continuous_future_roller);
        stateless_helper!(option_chain_bootstrapper);
        for (family, count) in &counts {
            if registered_stateless_helpers.contains_key(family) {
                ensure!(
                    *count == u64::from(registered_stateless_helpers[family]),
                    "native resident helper inventory differs"
                );
                continue;
            }
            ensure!(
                *count == 0,
                "unsupported native DataEngine pipeline {family}: {count}"
            );
        }
        for client in self.clients.values() {
            let state = client.running_checkpoint_state()?;
            for bar in state["subscriptions_bars"]
                .as_array()
                .context("actual facade bar inventory missing")?
            {
                let bar: BarType = serde_json::from_value(bar.clone())?;
                ensure!(
                    bar.is_externally_aggregated(),
                    "native internal bar subscription is unsupported"
                );
            }
        }
        Ok(serde_json::to_value(Snapshot {
            profile: "native_data_engine_external_bars_no_internal_pipelines.v2".into(),
            configuration: self.config.clone(),
            clients: self.clients.keys().copied().collect(),
            default_client_id: self.default_client_id,
            routing: self
                .routing_map
                .iter()
                .map(|(venue, id)| (*venue, *id))
                .collect(),
            subsystem_counts: counts,
            registered_stateless_helpers,
            command_count: self.command_count,
            data_count: self.data_count,
            request_count: self.request_count,
            response_count: self.response_count,
            msgbus_priority: self.msgbus_priority,
        })?)
    }
    /// Installs counters only into the same actual routing/configuration before
    /// tail replay. Internal request/aggregation pipelines have no supported path.
    ///
    /// # Errors
    /// Returns an error for invalid or changed source configuration, subscriptions, routes, or nonempty actual state.
    pub fn restore_running_checkpoint_state(&mut self, source: &serde_json::Value) -> Result<()> {
        let mut source: Snapshot = serde_json::from_value(source.clone())?;
        if source.profile == "native_data_engine_external_bars_no_internal_pipelines.v1" {
            // The original profile supports only external bars. New book owners
            // are incompatible with active old expansions; retain original bytes
            // and translate only verified empty unsupported families in memory.
            for (old, new) in [
                ("book_depth10_counts", "book_depth_counts"),
                ("book_deltas_parent_expansions", "book_subscriptions"),
                ("book_depth10_parent_expansions", "book_subscription_owners"),
            ] {
                ensure!(
                    source.subsystem_counts.remove(old) == Some(0),
                    "legacy native DataEngine unsupported pipeline is absent or active: {old}"
                );
                ensure!(
                    source.subsystem_counts.insert(new.into(), 0).is_none(),
                    "legacy native DataEngine contains a duplicate new pipeline: {new}"
                );
            }
            #[cfg(feature = "defi")]
            ensure!(
                source.subsystem_counts.remove("pool_updaters_pending") == Some(0),
                "legacy native DataEngine pending pool updater is absent or active"
            );
            source.profile = "native_data_engine_external_bars_no_internal_pipelines.v2".into();
        }
        ensure!(
            !self.checkpoint_restored,
            "native DataEngine checkpoint already installed"
        );
        let current: Snapshot = serde_json::from_value(self.running_checkpoint_state()?)?;
        ensure!(
            source.profile == current.profile
                && serde_json::to_value(&source.configuration)?
                    == serde_json::to_value(&current.configuration)?
                && source.clients == current.clients
                && source.default_client_id == current.default_client_id
                && source.routing == current.routing
                && source.subsystem_counts == current.subsystem_counts
                && source.registered_stateless_helpers == current.registered_stateless_helpers
                && source.msgbus_priority == current.msgbus_priority,
            "actual native DataEngine route/configuration or complete pipeline profile differs"
        );
        ensure!(
            current.command_count == 0
                && current.data_count == 0
                && current.request_count == 0
                && current.response_count == 0,
            "native DataEngine already processed source-tail inputs"
        );
        self.command_count = source.command_count;
        self.data_count = source.data_count;
        self.request_count = source.request_count;
        self.response_count = source.response_count;
        self.checkpoint_restored = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        messages::data::{RequestBars, RequestCommand},
    };
    use nautilus_core::{UUID4, UnixNanos};

    use super::*;
    use crate::engine::futures::ContinuousFutureRoller;
    fn engine() -> DataEngine {
        DataEngine::new(
            Rc::new(RefCell::new(nautilus_common::clock::VirtualClock::new())),
            Rc::new(RefCell::new(Cache::default())),
            None,
        )
    }
    #[test]
    fn checkpoint_actual_external_pipeline_restores_counters_and_rejects_live_join_and_partial_source()
     {
        let mut source = engine();
        source.command_count = 11;
        source.data_count = 42;
        source.request_count = 7;
        source.response_count = 7;
        let saved = source.running_checkpoint_state().unwrap();
        assert_eq!(saved["subsystem_counts"]["request_pipeline_responses"], 0);
        assert_eq!(saved["subsystem_counts"]["bar_aggregators"], 0);
        assert_eq!(saved["subsystem_counts"]["deferred_cmd_queue"], 0);
        let mut target = engine();
        target.restore_running_checkpoint_state(&saved).unwrap();
        assert_eq!(target.running_checkpoint_state().unwrap(), saved);
        assert!(target.restore_running_checkpoint_state(&saved).is_err());
        let mut broken = saved.clone();
        broken["subsystem_counts"]
            .as_object_mut()
            .unwrap()
            .remove("request_pipeline_responses");
        assert!(engine().restore_running_checkpoint_state(&broken).is_err());
        let request = RequestCommand::Bars(RequestBars::new(
            BarType::from("ETHUSDT.BINANCE-1-MINUTE-LAST-EXTERNAL"),
            None,
            None,
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
            None,
        ));
        // Actual native multi-leg pipeline, not a caller-provided count.
        source.new_request_pipeline(request, 2);
        let error = source.running_checkpoint_state().unwrap_err();
        assert!(
            error.to_string().contains("request_pipeline_n_components")
                || error
                    .to_string()
                    .contains("request_pipeline_parent_request")
        );
    }

    #[test]
    fn checkpoint_registered_resident_helpers_bind_actual_engine_without_inventing_pipeline_state()
    {
        let source = Rc::new(RefCell::new(engine()));
        DataEngine::register_msgbus_handlers(&source);
        let saved = source.borrow().running_checkpoint_state().unwrap();
        assert_eq!(saved["subsystem_counts"]["continuous_future_roller"], 1);
        assert_eq!(saved["subsystem_counts"]["option_chain_bootstrapper"], 1);
        let target = Rc::new(RefCell::new(engine()));
        DataEngine::register_msgbus_handlers(&target);
        target
            .borrow_mut()
            .restore_running_checkpoint_state(&saved)
            .unwrap();
        assert_eq!(target.borrow().running_checkpoint_state().unwrap(), saved);
        let other = Rc::new(RefCell::new(engine()));
        target.borrow_mut().continuous_future_roller =
            Some(Rc::new(ContinuousFutureRoller::new(&other)));
        assert!(
            target
                .borrow()
                .running_checkpoint_state()
                .unwrap_err()
                .to_string()
                .contains("another engine")
        );
    }
    #[rstest::rstest]
    fn original_empty_external_bar_v1_inventory_reads_without_mutating_source() {
        let mut source = engine();
        source.command_count = 11;
        source.data_count = 42;
        let current = source.running_checkpoint_state().unwrap();
        let mut original = current.clone();
        original["profile"] = "native_data_engine_external_bars_no_internal_pipelines.v1".into();
        let counts = original["subsystem_counts"].as_object_mut().unwrap();
        for (old, new) in [
            ("book_depth10_counts", "book_depth_counts"),
            ("book_deltas_parent_expansions", "book_subscriptions"),
            ("book_depth10_parent_expansions", "book_subscription_owners"),
        ] {
            let value = counts.remove(new).unwrap();
            counts.insert(old.into(), value);
        }
        #[cfg(feature = "defi")]
        counts.insert("pool_updaters_pending".into(), 0.into());
        let bytes = serde_json::to_vec(&original).unwrap();
        let mut target = engine();
        target.restore_running_checkpoint_state(&original).unwrap();
        assert_eq!(target.running_checkpoint_state().unwrap(), current);
        assert_eq!(serde_json::to_vec(&original).unwrap(), bytes);
        original["subsystem_counts"]["book_deltas_parent_expansions"] = 1.into();
        assert!(
            engine()
                .restore_running_checkpoint_state(&original)
                .is_err()
        );
    }
}
