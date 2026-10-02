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

//! Actual OKX no-book adapter restoration before fresh connection.

use super::*;
use anyhow::{Result, ensure};
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Inventory {
    profile: String,
    client_id: ClientId,
    configuration: serde_json::Value,
    tasks: serde_json::Value,
    public_socket: serde_json::Value,
    business_socket: Option<serde_json::Value>,
    http: serde_json::Value,
    instruments: AHashMap<Ustr, InstrumentAny>,
    instrument_write_sequence: u64,
    book_pipeline: String,
    index_ticker_map: AHashMap<Ustr, AHashSet<Ustr>>,
    option_greeks_subscriptions: AHashMap<InstrumentId, AHashSet<OKXGreeksType>>,
    option_summary_family_subscriptions: AHashMap<Ustr, usize>,
    public_stream: serde_json::Value,
    business_stream: serde_json::Value,
    execution_authorized: bool,
}
impl OKXDataClient {
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
            source.profile == "okx_connected_data_quiescent_retained_inputs_no_books.v1"
                && source.client_id == self.client_id
                && !source.execution_authorized
                && source.configuration == self.checkpoint_configuration()?
                && source.book_pipeline == "verified_empty_unsupported_active"
                && source.business_socket.is_some() == self.ws_business.is_some(),
            "OKX data source identity/profile differs"
        );
        let expected_tasks = 1
            + usize::from(self.ws_business.is_some())
            + usize::from(
                self.config.book_stale_check_interval_secs > 0
                    && self.config.book_stale_threshold_secs > 0,
            )
            + usize::from(self.config.update_instruments_interval_mins > 0);
        ensure!(
            source.tasks["owned_tasks"].as_u64() == Some(expected_tasks as u64),
            "unsupported source data tasks"
        );
        let public = crate::checkpoint::DataStreamState::restore(&source.public_stream)?;
        let business = crate::checkpoint::DataStreamState::restore(&source.business_stream)?;
        // A failed partial installation can never be retried as a pristine adapter.
        self.recovery_pristine.store(false, Ordering::Release);
        self.http_client.restore_running_checkpoint(&source.http)?;
        self.ws_public
            .as_mut()
            .context("fresh public socket missing")?
            .restore_running_checkpoint(&source.public_socket)?;
        if let (Some(socket), Some(inventory)) = (&mut self.ws_business, &source.business_socket) {
            socket.restore_running_checkpoint(inventory)?;
        }
        self.instruments_by_symbol
            .rcu(|m| *m = source.instruments.clone());
        self.instrument_update_lock
            .write_seq
            .store(source.instrument_write_sequence, Ordering::SeqCst);
        self.index_ticker_map
            .rcu(|m| *m = source.index_ticker_map.clone());
        self.option_greeks_subs
            .rcu(|m| *m = source.option_greeks_subscriptions.clone());
        *self.option_summary_family_subs.lock() = source.option_summary_family_subscriptions;
        *self.public_stream_state.lock() = public;
        *self.business_stream_state.lock() = business;
        Ok(())
    }
}
