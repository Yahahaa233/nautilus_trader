// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Read-only native facts. Account selection never falls back to a venue.
//! Native floating indicator/average values retain their original bit pattern;
//! quantities, money and prices retain their exact native serialization.

use anyhow::{Context, Result, ensure};
use nautilus_common::actor::DataActorNative;
use nautilus_plugin::strategy::HostOperation;
use serde_json::json;

use super::{PluginStrategy, require_version, wire};
use crate::strategy::Strategy;

impl PluginStrategy {
    pub(super) fn readonly(&self, operation: HostOperation, input: &[u8]) -> Result<Vec<u8>> {
        match operation {
            HostOperation::Clock => {
                ensure!(input.is_empty(), "clock request must be empty");
                self.clock_facts()
            }
            HostOperation::Query => {
                let request: wire::OrderId = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                Ok(serde_json::to_vec(
                    &self.owned_order(request.client_order_id)?,
                )?)
            }
            HostOperation::Instrument | HostOperation::Portfolio => {
                let request: wire::InstrumentRequest = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                self.require_instrument(request.instrument_id)?;
                let cache = self.try_cache_ref()?;
                if operation == HostOperation::Instrument {
                    let instrument = cache
                        .instrument(&request.instrument_id)
                        .context("admitted native instrument facts unavailable")?;
                    return Ok(serde_json::to_vec(
                        &json!({"schema_version":wire::VERSION,"instrument":instrument}),
                    )?);
                }
                // A client registration, if present, must agree with the exact
                // deployment account. No default venue account is consulted.
                if let Some(account) = cache.account_id_for_client(&self.scope.client_id) {
                    ensure!(
                        *account == self.scope.account_id,
                        "native client belongs to another account"
                    );
                }
                let account = cache
                    .account(&self.scope.account_id)
                    .context("admitted native account facts unavailable")?;
                let account_state = account
                    .last_event()
                    .context("native account has no source state")?;
                ensure!(
                    account_state.balances.len() <= 256 && account_state.margins.len() <= 256,
                    "native account fact inventory exceeds fixed bounds"
                );
                let owner = self.strategy_id().context("plugin owner is unregistered")?;
                let positions = cache.positions_open_refs(
                    None,
                    Some(&request.instrument_id),
                    Some(&owner),
                    Some(&self.scope.account_id),
                    None,
                );
                ensure!(
                    positions.len() <= 1024,
                    "native position fact inventory exceeds fixed bounds"
                );
                let mut facts = Vec::with_capacity(positions.len());
                for position in positions {
                    ensure!(
                        position.commissions.len() <= 16,
                        "native position commission inventory exceeds fixed bounds"
                    );
                    facts.push(json!({"position_id":position.id,"strategy_id":position.strategy_id,
                        "instrument_id":position.instrument_id,"account_id":position.account_id,
                        "entry":position.entry,"side":position.side,"quantity":position.quantity,
                        "peak_quantity":position.peak_qty,"average_open_price_bits":position.avg_px_open.to_bits(),
                        "average_close_price_bits":position.avg_px_close.map(f64::to_bits),
                        "realized_pnl":position.realized_pnl,"commissions":position.commissions,
                        "ts_opened":position.ts_opened,"ts_last":position.ts_last}));
                }
                Ok(serde_json::to_vec(
                    &json!({"schema_version":wire::VERSION,"client_id":self.scope.client_id,
                    "data_client_id":self.scope.data_client_id,"account_id":self.scope.account_id,"strategy_id":owner,"instrument_id":request.instrument_id,
                    "account_state":account_state,"open_positions":facts}),
                )?)
            }
            _ => anyhow::bail!("mutation is unavailable during native state handoff"),
        }
    }
}
