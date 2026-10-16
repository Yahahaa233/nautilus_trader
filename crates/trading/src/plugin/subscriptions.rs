// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Native data commands and local resource inventory share one adapter.

use anyhow::{Result, ensure};
use nautilus_common::actor::DataActor;
use nautilus_plugin::strategy::HostOperation;

use super::{PluginStrategy, require_version, wire};

impl PluginStrategy {
    pub(super) fn data_subscription(
        &mut self,
        operation: HostOperation,
        input: &[u8],
    ) -> Result<()> {
        let header: serde_json::Value = serde_json::from_slice(input)?;
        let request = if header.get("kind").is_some() {
            serde_json::from_slice(input)?
        } else {
            let legacy: wire::Subscription = serde_json::from_slice(input)?;
            require_version(legacy.schema_version)?;
            wire::DataSubscription::Bar {
                schema_version: 2,
                bar_type: legacy.bar_type,
            }
        };
        let subscribe = operation == HostOperation::Subscribe;
        match request {
            wire::DataSubscription::Bar {
                schema_version,
                bar_type,
            } => {
                ensure!(
                    schema_version == 2 && self.scope.bar_types.contains(&bar_type),
                    "bar subscription is outside the admitted declaration"
                );
                self.require_instrument(bar_type.instrument_id())?;
                if subscribe {
                    self.subscribe_bars(bar_type, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_bars.insert(bar_type);
                } else {
                    self.unsubscribe_bars(bar_type, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_bars.remove(&bar_type);
                }
            }
            wire::DataSubscription::Quote {
                schema_version,
                instrument_id,
            } => {
                ensure!(
                    schema_version == 2 && self.scope.quote_instruments.contains(&instrument_id),
                    "quote subscription is outside the admitted declaration"
                );
                self.require_instrument(instrument_id)?;
                if subscribe {
                    self.subscribe_quotes(instrument_id, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_quotes.insert(instrument_id);
                } else {
                    self.unsubscribe_quotes(instrument_id, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_quotes.remove(&instrument_id);
                }
            }
            wire::DataSubscription::Trade {
                schema_version,
                instrument_id,
            } => {
                ensure!(
                    schema_version == 2 && self.scope.trade_instruments.contains(&instrument_id),
                    "trade subscription is outside the admitted declaration"
                );
                self.require_instrument(instrument_id)?;
                if subscribe {
                    self.subscribe_trades(instrument_id, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_trades.insert(instrument_id);
                } else {
                    self.unsubscribe_trades(instrument_id, Some(self.scope.data_client_id), None);
                    self.adapter.subscribed_trades.remove(&instrument_id);
                }
            }
        }
        Ok(())
    }
}
