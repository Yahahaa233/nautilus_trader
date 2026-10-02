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
use std::collections::{BTreeMap, BTreeSet};
impl LiveNode {
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
            guard.complete()?;
        }
        Ok(())
    }
}
