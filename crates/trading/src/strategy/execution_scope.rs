// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Immutable native routing restriction for a host-admitted strategy. It owns
//! no execution client, order inventory, permission, timer or economic ledger.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use nautilus_common::cache::Cache;
use nautilus_model::{
    identifiers::{AccountId, ClientId, InstrumentId, TraderId},
    orders::{Order, OrderAny},
    position::Position,
};

#[derive(Clone, Debug)]
pub(crate) struct StrategyExecutionScope {
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub instruments: BTreeSet<InstrumentId>,
}

impl StrategyExecutionScope {
    pub(crate) fn client_for(
        &self,
        instrument: InstrumentId,
        requested: Option<ClientId>,
    ) -> Result<ClientId> {
        ensure!(
            self.instruments.contains(&instrument)
                && requested.is_none_or(|id| id == self.client_id),
            "native strategy operation exceeds its admitted instrument/client scope"
        );
        Ok(self.client_id)
    }

    pub(crate) fn owns_order(&self, cache: &Cache, order: &OrderAny, trader_id: TraderId) -> bool {
        order.trader_id() == trader_id
            && self.instruments.contains(&order.instrument_id())
            && order.account_id().is_none_or(|id| id == self.account_id)
            && cache.client_id(&order.client_order_id()).copied() == Some(self.client_id)
    }

    pub(crate) fn owns_position(&self, position: &Position, trader_id: TraderId) -> bool {
        position.trader_id == trader_id
            && position.account_id == self.account_id
            && self.instruments.contains(&position.instrument_id)
    }
}
