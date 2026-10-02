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

//! Preserves order/fee/cumulative-fill state before a new authenticated session.

use super::*;
use anyhow::{Result, ensure};
use std::sync::atomic::Ordering;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    profile: String,
    account_id: AccountId,
    client_id: ClientId,
    configuration: serde_json::Value,
    request_tasks: serde_json::Value,
    sessions: serde_json::Value,
    private_socket: serde_json::Value,
    business_socket: serde_json::Value,
    http: serde_json::Value,
    dispatch: serde_json::Value,
    private_stream: crate::checkpoint::ExecutionStreamState,
    business_stream: crate::checkpoint::ExecutionStreamState,
    execution_authorized: bool,
}
impl OKXExecutionClient {
    pub(super) fn checkpoint_configuration(&self) -> Result<serde_json::Value> {
        let mut config = self.config.clone();
        config.api_key = None;
        config.api_secret = None;
        config.api_passphrase = None;
        config.proxy_url = None;
        Ok(serde_json::to_value(config)?)
    }
    pub(super) fn restore_checkpoint_source(
        &mut self,
        inventory: &serde_json::Value,
    ) -> Result<()> {
        self.verify_paused_recovery_inventory()?;
        let source: Inventory = serde_json::from_value(inventory.clone())?;
        ensure!(
            source.profile == "okx_connected_execution_quiescent_retained_inputs.v1"
                && source.account_id == self.core.account_id
                && source.client_id == self.core.client_id
                && source.configuration == self.checkpoint_configuration()?
                && !source.execution_authorized
                && source.request_tasks["owned_tasks"].as_u64() == Some(0)
                && source.sessions["owned_tasks"].as_u64() == Some(2),
            "OKX execution source identity/profile differs"
        );
        // Partial failure is a permanent failed installation, not a fresh retry.
        self.recovery_pristine.store(false, Ordering::Release);
        self.ws_dispatch_state
            .restore_checkpoint(&source.dispatch)?;
        self.http_client.restore_running_checkpoint(&source.http)?;
        self.ws_private
            .restore_running_checkpoint(&source.private_socket)?;
        self.ws_business
            .restore_running_checkpoint(&source.business_socket)?;
        *self.private_stream_state.lock() = source.private_stream;
        *self.business_stream_state.lock() = source.business_stream;
        Ok(())
    }
}
