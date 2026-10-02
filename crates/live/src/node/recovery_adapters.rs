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

//! Installs actual native/adaptor state from the completed same-node frontier.
use super::{LiveNode, NodeState};
use anyhow::{Result, ensure};
use nautilus_common::live::dst;
use std::collections::{BTreeMap, BTreeSet};
impl LiveNode {
    /// Installs complete supported native engine state at the source checkpoint
    /// frontier before replaying later inputs. Actual offline elapsed time ages
    /// native recency; it cannot refresh approval/account facts or replay queues.
    pub fn restore_registered_engine_checkpoint(
        &mut self,
        manager: &serde_json::Value,
        data_engine: &serde_json::Value,
        source_capture_ns: u64,
        watermark: &crate::runner_recovery::RunnerRecoveryWatermark,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle
                && !self.handle.should_stop()
                && self.recovery_requires_release
                && self.recovery_native_frontier.as_ref() == Some(watermark)
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && self.recovery_engine_source.is_none(),
            "native engine restore requires same installed source checkpoint frontier"
        );
        let actual_now = self.kernel.clock.try_borrow()?.timestamp_ns().as_u64();
        ensure!(
            source_capture_ns > 0 && manager["captured_at_ns"].as_u64() == Some(source_capture_ns),
            "native manager capture is not the source checkpoint time"
        );
        let target_at = dst::time::Instant::now();
        let downtime = actual_now
            .checked_sub(source_capture_ns)
            .ok_or_else(|| anyhow::anyhow!("native source capture is in the future"))?;
        let input=self.native_mutation_input("recovery.engine_state_install",&serde_json::json!({"watermark":watermark,"manager":manager,"data_engine":data_engine,"source_capture_ns":source_capture_ns,"actual_now_ns":actual_now}))?;
        let guard = self.begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &input)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let mut prepared = self.exec_manager.clone();
            prepared.restore_checkpoint_inventory_at(manager, downtime, target_at)?;
            #[cfg(feature = "native-tail-replay")]
            {
                self.recovery_engine_time = Some((target_at, actual_now));
            }
            let mut data = self.kernel.data_engine.try_borrow_mut()?;
            data.restore_running_checkpoint_state(data_engine)?;
            self.exec_manager = prepared;
            self.recovery_engine_source = Some(
                serde_json::json!({"watermark":watermark,"source_capture_ns":source_capture_ns}),
            );
            Ok(())
        }))
        .map_err(|_| anyhow::anyhow!("native engine restoration panicked"))
        .and_then(|result| result);
        if let Err(error) = &result {
            self.fail_recovery_observation(&format!("native engine restore failed: {error:#}"));
        }
        result?;
        if let Some(guard) = guard {
            self.finish_node_dispatch(guard)?;
        }
        Ok(())
    }

    /// Installs supported source state into actual registered fresh adapters.
    /// JSON is replay data; the installed private native frontier remains the
    /// authority for restoration. Fresh UID/account/market proofs are separate.
    pub fn restore_registered_adapter_checkpoint(
        &mut self,
        adapters: &BTreeMap<String, serde_json::Value>,
        data_client_state: &BTreeMap<String, serde_json::Value>,
        watermark: &crate::runner_recovery::RunnerRecoveryWatermark,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle
                && !self.handle.should_stop()
                && self.recovery_requires_release
                && self.recovery_native_frontier.as_ref() == Some(watermark)
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && self.recovery_adapter_source.is_none(),
            "adapter restoration requires completed same paused native frontier"
        );
        let input=self.native_mutation_input("recovery.adapter_state_install",&serde_json::json!({"watermark":watermark,"adapters":adapters,"data_client_state":data_client_state}))?;
        let guard = self.begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &input)?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let mut data = self.kernel.data_engine.try_borrow_mut()?;
            let mut execution = self.kernel.exec_engine.try_borrow_mut()?;
            ensure!(
                execution.get_external_client_ids().is_empty()
                    && self
                        .config
                        .data_engine
                        .external_clients
                        .as_ref()
                        .is_none_or(Vec::is_empty),
                "external adapters have no restore inventory"
            );
            let data_ids = data
                .get_clients()
                .iter()
                .map(|client| format!("data:{}", client.client_id))
                .collect::<BTreeSet<_>>();
            let ids = data_ids
                .iter()
                .cloned()
                .chain(
                    execution
                        .get_all_clients()
                        .iter()
                        .map(|client| format!("execution:{}", client.client_id())),
                )
                .collect::<BTreeSet<_>>();
            ensure!(
                ids == adapters.keys().cloned().collect()
                    && data_ids == data_client_state.keys().cloned().collect(),
                "actual registered adapter/native subscription identities differ from source"
            );
            for client in data.get_clients_mut() {
                let key = format!("data:{}", client.client_id);
                client.restore_running_checkpoint(&adapters[&key])?;
                client.restore_running_checkpoint_state(&data_client_state[&key])?;
            }
            for client in execution.get_clients_mut() {
                client.restore_running_checkpoint(
                    &adapters[&format!("execution:{}", client.client_id)],
                )?;
            }
            Ok(())
        }))
        .map_err(|_| anyhow::anyhow!("native adapter restoration panicked"))
        .and_then(|result| result);
        if let Err(error) = &result {
            self.fail_recovery_observation(&format!("adapter restoration failed: {error:#}"));
        }
        result?;
        self.recovery_adapter_source = Some(adapters.clone());
        if let Some(guard) = guard {
            self.finish_node_dispatch(guard)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver},
        node::NodeDispatchObserver,
    };
    use nautilus_common::{cache::Cache, enums::Environment};
    use nautilus_model::{
        enums::TradingState,
        identifiers::{ClientOrderId, TraderId},
    };
    use nautilus_system::trader::Trader;
    fn node() -> LiveNode {
        let mut node = LiveNode::builder(TraderId::from("ENGINE-SOURCE"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .build()
            .unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("engine-restore".into(), |_| Ok(())).unwrap(),
            |source, phase, input| {
                let native = input
                    .downcast_ref::<super::super::NativeMutationInput>()
                    .ok_or_else(|| anyhow::anyhow!("unexpected source"))?;
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload: native.canonical_payload()?,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        node
    }
    #[test]
    fn checkpoint_node_engine_restore_requires_real_cache_components_and_same_installed_frontier() {
        let source = node();
        let manager = source
            .exec_manager
            .checkpoint_inventory(nautilus_common::live::dst::time::Instant::now())
            .unwrap();
        let data = source
            .kernel
            .data_engine
            .borrow()
            .running_checkpoint_state()
            .unwrap();
        let captured = manager["captured_at_ns"].as_u64().unwrap();
        let watermark = crate::runner_recovery::RunnerRecoveryWatermark {
            recovery_id: "source-cut".into(),
            checkpoint_sequence: 8,
            dispatch_watermark: 12,
        };
        let mut target = node();
        assert!(
            target
                .restore_registered_engine_checkpoint(&manager, &data, captured, &watermark)
                .is_err()
        );
        target
            .kernel
            .risk_engine
            .borrow_mut()
            .set_trading_state(TradingState::Halted);
        target.restore_native_cache(Cache::default()).unwrap();
        let components = Trader::collect_component_state(&target.kernel.trader).unwrap();
        target.restore_component_state(&components).unwrap();
        let registry = crate::runner_recovery::RunnerRecoveryCodecRegistry::new([])
            .seal()
            .unwrap();
        target
            .replay_recovery_events(&watermark, &[], &registry, |_, _| Ok(()))
            .unwrap();
        let mut different = watermark.clone();
        different.dispatch_watermark += 1;
        assert!(
            target
                .restore_registered_engine_checkpoint(&manager, &data, captured, &different)
                .is_err()
        );
        target
            .restore_registered_engine_checkpoint(&manager, &data, captured, &watermark)
            .unwrap();
        assert!(
            target
                .restore_registered_engine_checkpoint(&manager, &data, captured, &watermark)
                .is_err()
        );
        // Actual manager remains usable for later recovered tail inputs.
        target
            .exec_manager
            .register_inflight(ClientOrderId::from("TAIL-AFTER-CUT"));
        assert!(
            target
                .exec_manager
                .checkpoint_inventory(nautilus_common::live::dst::time::Instant::now())
                .unwrap()["order_inflight_checks"]
                .as_array()
                .unwrap()
                .len()
                == 1
        );
    }
}
