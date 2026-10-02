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

//! Captures/restores actual native subscription ownership alongside adapter state.
use super::*;
use anyhow::{Result, ensure};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Active {
    command: SubscribeCommand,
    acquisitions: Vec<nautilus_core::UUID4>,
    owners: usize,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    profile: String,
    client_id: ClientId,
    venue: Option<Venue>,
    handles_book_deltas: bool,
    handles_book_snapshots: bool,
    active: Vec<Active>,
    subscriptions_custom: Vec<DataType>,
    subscriptions_book_deltas: Vec<InstrumentId>,
    subscriptions_book_depth10: Vec<InstrumentId>,
    subscriptions_quotes: Vec<InstrumentId>,
    subscriptions_trades: Vec<InstrumentId>,
    subscriptions_bars: Vec<BarType>,
    subscriptions_instrument_status: Vec<InstrumentId>,
    subscriptions_instrument_close: Vec<InstrumentId>,
    subscriptions_instrument: Vec<InstrumentId>,
    subscriptions_instrument_venue: Vec<Venue>,
    subscriptions_mark_prices: Vec<InstrumentId>,
    subscriptions_index_prices: Vec<InstrumentId>,
    subscriptions_funding_rates: Vec<InstrumentId>,
    subscriptions_option_greeks: Vec<InstrumentId>,
}
fn ordered<T: Clone + Serialize>(values: &AHashSet<T>) -> Result<Vec<T>> {
    let mut keyed = values
        .iter()
        .map(|v| Ok((serde_json::to_string(v)?, v.clone())))
        .collect::<Result<Vec<_>>>()?;
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(keyed.into_iter().map(|(_, v)| v).collect())
}
fn unique<T: Eq + std::hash::Hash>(values: Vec<T>) -> Result<AHashSet<T>> {
    let len = values.len();
    let set = values.into_iter().collect::<AHashSet<T>>();
    ensure!(
        len == set.len() && len <= 1_000_000,
        "duplicate or oversized native subscriptions"
    );
    Ok(set)
}
impl DataClientAdapter {
    pub fn running_checkpoint_state(&self) -> Result<serde_json::Value> {
        #[cfg(feature = "defi")]
        ensure!(
            self.subscriptions_active_defi.iter().next().is_none(),
            "native DeFi subscription checkpoint unsupported"
        );
        let mut active = self
            .subscriptions_active
            .iter()
            .map(|(_, value)| {
                Ok(Active {
                    command: value.command.clone(),
                    acquisitions: ordered(&value.acquisitions)?,
                    owners: value.owners,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        active.sort_by_key(|value| value.command.command_id().to_string());
        let snapshot = Snapshot {
            profile: "native_data_client_subscription_owners.v1".into(),
            client_id: self.client_id,
            venue: self.venue,
            handles_book_deltas: self.handles_book_deltas,
            handles_book_snapshots: self.handles_book_snapshots,
            active,
            subscriptions_custom: ordered(&self.subscriptions_custom)?,
            subscriptions_book_deltas: ordered(&self.subscriptions_book_deltas)?,
            subscriptions_book_depth10: ordered(&self.subscriptions_book_depth10)?,
            subscriptions_quotes: ordered(&self.subscriptions_quotes)?,
            subscriptions_trades: ordered(&self.subscriptions_trades)?,
            subscriptions_bars: ordered(&self.subscriptions_bars)?,
            subscriptions_instrument_status: ordered(&self.subscriptions_instrument_status)?,
            subscriptions_instrument_close: ordered(&self.subscriptions_instrument_close)?,
            subscriptions_instrument: ordered(&self.subscriptions_instrument)?,
            subscriptions_instrument_venue: ordered(&self.subscriptions_instrument_venue)?,
            subscriptions_mark_prices: ordered(&self.subscriptions_mark_prices)?,
            subscriptions_index_prices: ordered(&self.subscriptions_index_prices)?,
            subscriptions_funding_rates: ordered(&self.subscriptions_funding_rates)?,
            subscriptions_option_greeks: ordered(&self.subscriptions_option_greeks)?,
        };
        Ok(serde_json::to_value(snapshot)?)
    }
    pub fn restore_running_checkpoint_state(&mut self, source: &serde_json::Value) -> Result<()> {
        let snapshot: Snapshot = serde_json::from_value(source.clone())?;
        ensure!(
            snapshot.profile == "native_data_client_subscription_owners.v1"
                && snapshot.client_id == self.client_id
                && snapshot.venue == self.venue
                && snapshot.handles_book_deltas == self.handles_book_deltas
                && snapshot.handles_book_snapshots == self.handles_book_snapshots,
            "native client facade identity/profile differs"
        );
        ensure!(
            self.subscriptions_active.iter().next().is_none(),
            "native facade subscription registry already used"
        );
        ensure!(
            self.subscriptions_custom.is_empty(),
            "native facade subscription state already used"
        );
        let custom = unique(snapshot.subscriptions_custom)?;
        ensure!(
            self.subscriptions_book_deltas.is_empty(),
            "native facade subscription state already used"
        );
        let book_deltas = unique(snapshot.subscriptions_book_deltas)?;
        ensure!(
            self.subscriptions_book_depth10.is_empty(),
            "native facade subscription state already used"
        );
        let book_depth10 = unique(snapshot.subscriptions_book_depth10)?;
        ensure!(
            self.subscriptions_quotes.is_empty(),
            "native facade subscription state already used"
        );
        let quotes = unique(snapshot.subscriptions_quotes)?;
        ensure!(
            self.subscriptions_trades.is_empty(),
            "native facade subscription state already used"
        );
        let trades = unique(snapshot.subscriptions_trades)?;
        ensure!(
            self.subscriptions_bars.is_empty(),
            "native facade subscription state already used"
        );
        let bars = unique(snapshot.subscriptions_bars)?;
        ensure!(
            self.subscriptions_instrument_status.is_empty(),
            "native facade subscription state already used"
        );
        let instrument_status = unique(snapshot.subscriptions_instrument_status)?;
        ensure!(
            self.subscriptions_instrument_close.is_empty(),
            "native facade subscription state already used"
        );
        let instrument_close = unique(snapshot.subscriptions_instrument_close)?;
        ensure!(
            self.subscriptions_instrument.is_empty(),
            "native facade subscription state already used"
        );
        let instrument = unique(snapshot.subscriptions_instrument)?;
        ensure!(
            self.subscriptions_instrument_venue.is_empty(),
            "native facade subscription state already used"
        );
        let instrument_venue = unique(snapshot.subscriptions_instrument_venue)?;
        ensure!(
            self.subscriptions_mark_prices.is_empty(),
            "native facade subscription state already used"
        );
        let mark_prices = unique(snapshot.subscriptions_mark_prices)?;
        ensure!(
            self.subscriptions_index_prices.is_empty(),
            "native facade subscription state already used"
        );
        let index_prices = unique(snapshot.subscriptions_index_prices)?;
        ensure!(
            self.subscriptions_funding_rates.is_empty(),
            "native facade subscription state already used"
        );
        let funding_rates = unique(snapshot.subscriptions_funding_rates)?;
        ensure!(
            self.subscriptions_option_greeks.is_empty(),
            "native facade subscription state already used"
        );
        let option_greeks = unique(snapshot.subscriptions_option_greeks)?;
        let entries = snapshot
            .active
            .into_iter()
            .map(|active| {
                Ok((
                    SubscriptionKey::from_subscribe(&active.command),
                    active.command,
                    unique(active.acquisitions)?,
                    active.owners,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        self.subscriptions_active.restore_entries(entries)?;
        self.subscriptions_custom = custom;
        self.subscriptions_book_deltas = book_deltas;
        self.subscriptions_book_depth10 = book_depth10;
        self.subscriptions_quotes = quotes;
        self.subscriptions_trades = trades;
        self.subscriptions_bars = bars;
        self.subscriptions_instrument_status = instrument_status;
        self.subscriptions_instrument_close = instrument_close;
        self.subscriptions_instrument = instrument;
        self.subscriptions_instrument_venue = instrument_venue;
        self.subscriptions_mark_prices = mark_prices;
        self.subscriptions_index_prices = index_prices;
        self.subscriptions_funding_rates = funding_rates;
        self.subscriptions_option_greeks = option_greeks;
        Ok(())
    }
}
