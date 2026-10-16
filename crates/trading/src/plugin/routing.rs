// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Callback admission preserves native ownership before entering algorithm code.

use anyhow::{Result, ensure};
use nautilus_model::events::{OrderEventAny, PositionEvent};
use nautilus_plugin::PluginErrorCode;

use super::PluginStrategy;
use crate::strategy::Strategy;

impl PluginStrategy {
    pub(super) fn check_order_route(&self, event: &OrderEventAny) -> Result<()> {
        ensure!(
            Some(event.strategy_id()) == self.strategy_id(),
            "native order callback belongs to another strategy"
        );
        self.require_instrument(event.instrument_id())?;
        ensure!(
            event
                .account_id()
                .is_none_or(|account| account == self.scope.account_id),
            "native order callback belongs to another account"
        );
        Ok(())
    }

    pub(super) fn check_position_route(&self, event: &PositionEvent) -> Result<()> {
        let owner = match event {
            PositionEvent::PositionOpened(value) => value.strategy_id,
            PositionEvent::PositionChanged(value) => value.strategy_id,
            PositionEvent::PositionClosed(value) => value.strategy_id,
            PositionEvent::PositionAdjusted(value) => value.strategy_id,
        };
        ensure!(
            Some(owner) == self.strategy_id() && event.account_id() == self.scope.account_id,
            "native position callback belongs to another strategy/account"
        );
        self.require_instrument(event.instrument_id())
    }

    pub(super) fn retain_route_result(&mut self, result: Result<()>) -> Result<()> {
        if let Err(error) = &result {
            self.guest
                .try_borrow_mut()?
                .retain_native_failure(PluginErrorCode::InvalidArgument, &format!("{error:#}"))
                .map_err(|failure| {
                    anyhow::anyhow!(
                        "native callback refusal could not retain its first failure: {}",
                        failure.message_string()
                    )
                })?;
            self.fault_callback(error);
        }
        result
    }
}
