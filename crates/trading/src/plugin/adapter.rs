// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Native adapter resources belong to the same checkpoint as the guest state.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use nautilus_common::messages::data::SubscribeCommand;
use nautilus_model::{data::BarType, identifiers::InstrumentId};
use serde::{Deserialize, Serialize};

use super::{PluginScope, wire};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PendingHistory {
    pub(super) request_id: String,
    pub(super) request: wire::HistoryRequest,
}

impl PendingHistory {
    pub(super) fn id(&self) -> Result<nautilus_core::UUID4> {
        self.request_id.parse().map_err(anyhow::Error::msg)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdapterState {
    schema_version: u32,
    pub(super) subscribed_bars: BTreeSet<BarType>,
    pub(super) subscribed_quotes: BTreeSet<InstrumentId>,
    pub(super) subscribed_trades: BTreeSet<InstrumentId>,
    pub(super) pending_history: Option<PendingHistory>,
    pub(super) retained_subscriptions: Vec<SubscribeCommand>,
}

impl Default for AdapterState {
    fn default() -> Self {
        Self {
            schema_version: 4,
            subscribed_bars: BTreeSet::new(),
            subscribed_quotes: BTreeSet::new(),
            subscribed_trades: BTreeSet::new(),
            pending_history: None,
            retained_subscriptions: Vec::new(),
        }
    }
}

impl AdapterState {
    pub(super) fn decode(raw: &[u8], scope: &PluginScope) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Legacy {
            schema_version: u32,
            subscribed_bars: BTreeSet<BarType>,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyData {
            schema_version: u32,
            subscribed_bars: BTreeSet<BarType>,
            subscribed_quotes: BTreeSet<InstrumentId>,
            subscribed_trades: BTreeSet<InstrumentId>,
        }
        let header: serde_json::Value = serde_json::from_slice(raw)?;
        let state = if header["schema_version"] == 1 {
            let legacy: Legacy = serde_json::from_slice(raw)?;
            ensure!(
                legacy.schema_version == 1
                    && scope.quote_instruments.is_empty()
                    && scope.trade_instruments.is_empty()
                    && scope.max_history_bars == 0
                    && legacy.subscribed_bars.is_empty(),
                "legacy native adapter lacks declared data/history resources"
            );
            Self {
                subscribed_bars: legacy.subscribed_bars,
                ..Self::default()
            }
        } else if header["schema_version"] == 2 {
            let legacy: LegacyData = serde_json::from_slice(raw)?;
            ensure!(
                legacy.schema_version == 2
                    && scope.max_history_bars == 0
                    && legacy.subscribed_bars.is_empty()
                    && legacy.subscribed_quotes.is_empty()
                    && legacy.subscribed_trades.is_empty(),
                "legacy native adapter lacks pending history resources"
            );
            Self {
                subscribed_bars: legacy.subscribed_bars,
                subscribed_quotes: legacy.subscribed_quotes,
                subscribed_trades: legacy.subscribed_trades,
                ..Self::default()
            }
        } else {
            ensure!(
                header.get("pending_history").is_some()
                    && header.get("retained_subscriptions").is_some(),
                "native history/subscription command inventory is absent"
            );
            serde_json::from_slice(raw)?
        };
        state.validate(scope)?;
        Ok(state)
    }

    pub(super) fn validate(&self, scope: &PluginScope) -> Result<()> {
        ensure!(
            self.schema_version == 4,
            "unknown native plugin adapter state"
        );
        let mut bars = BTreeSet::new();
        let mut quotes = BTreeSet::new();
        let mut trades = BTreeSet::new();
        for command in &self.retained_subscriptions {
            let (client, venue, instrument, params, correlation, unique) = match command {
                SubscribeCommand::Bars(value) => (
                    value.client_id,
                    value.venue,
                    value.bar_type.instrument_id(),
                    &value.params,
                    value.correlation_id,
                    bars.insert(value.bar_type),
                ),
                SubscribeCommand::Quotes(value) => (
                    value.client_id,
                    value.venue,
                    value.instrument_id,
                    &value.params,
                    value.correlation_id,
                    quotes.insert(value.instrument_id),
                ),
                SubscribeCommand::Trades(value) => (
                    value.client_id,
                    value.venue,
                    value.instrument_id,
                    &value.params,
                    value.correlation_id,
                    trades.insert(value.instrument_id),
                ),
                _ => anyhow::bail!("unknown retained native subscription family"),
            };
            ensure!(
                unique
                    && client == Some(scope.data_client_id)
                    && venue == Some(instrument.venue)
                    && scope.instruments.contains(&instrument)
                    && params.is_none()
                    && correlation.is_none(),
                "original native subscription command differs from its admitted scope"
            );
        }
        ensure!(
            bars == self.subscribed_bars
                && quotes == self.subscribed_quotes
                && trades == self.subscribed_trades,
            "native subscriptions differ from their actual retained command inventory"
        );
        if let Some(pending) = &self.pending_history {
            let id: nautilus_core::UUID4 = pending.id()?;
            ensure!(
                id.to_string() == pending.request_id
                    && pending.request.schema_version == wire::VERSION
                    && scope.bar_types.contains(&pending.request.bar_type)
                    && pending.request.limit > 0
                    && pending.request.limit as usize <= scope.max_history_bars
                    && pending.request.start_ns <= pending.request.end_ns,
                "pending native history differs from the admitted scope"
            );
        }
        ensure!(
            self.subscribed_quotes.is_subset(&scope.quote_instruments)
                && self.subscribed_trades.is_subset(&scope.trade_instruments),
            "restored native quote/trade resources exceed the admitted scope"
        );
        ensure!(
            self.subscribed_bars.iter().all(|bar| {
                scope.bar_types.contains(bar) && scope.instruments.contains(&bar.instrument_id())
            }),
            "restored plugin subscriptions differ from the admitted scope"
        );
        Ok(())
    }
}
