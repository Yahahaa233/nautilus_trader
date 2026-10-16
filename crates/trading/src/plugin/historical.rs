// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Original-object callback replay. The Live reader owns source verification;
//! this adapter owns its native local bindings and the actual guest callbacks.

use std::{any::Any, fmt::Debug, rc::Rc};

use anyhow::{Context, Result, ensure};
use indexmap::IndexMap;

use nautilus_common::{
    actor::DataActor,
    messages::data::BarsResponse,
    recovery_trace::{
        NativeComponentLifecycle,
        historical::{
            HistoricalInputBoundary, HistoricalReplayPreparation, pending_callback_route,
        },
    },
    timer::TimeEvent,
};
use nautilus_model::{
    data::{Bar, QuoteTick, TradeTick},
    events::{OrderEventAny, PositionEvent},
    identifiers::StrategyId,
};

use super::PluginStrategy;

/// Implemented by the native Live Journal reader integration. Trading cannot
/// depend on event-store (event-store already depends on system/trading).
/// Installing a verifier grants no current execution or source fabrication.
pub trait PluginHistoricalVerifier: Debug {
    /// Binds this owner and complete private state to the actual reader cut.
    ///
    /// # Errors
    /// Refuses an unrelated/unverified source, owner or incomplete state.
    fn prepare(
        &self,
        boundary: &HistoricalReplayPreparation<'_>,
        owner: &str,
        current: &IndexMap<String, Vec<u8>>,
    ) -> Result<()>;

    /// Checks the exact reader root and whole Begin, including original route.
    ///
    /// # Errors
    /// Refuses changed source/root/Begin/owner, or a failed verification session.
    fn verify_input(&self, boundary: &HistoricalInputBoundary<'_>, owner: &str) -> Result<()>;
}

impl PluginStrategy {
    /// Attaches the actual native reader verifier before registration or use.
    ///
    /// # Errors
    /// Refuses replacement, already registered instances or started recovery.
    pub fn set_historical_verifier(
        &mut self,
        verifier: Rc<dyn PluginHistoricalVerifier>,
    ) -> Result<()> {
        ensure!(
            self.historical_verifier.is_none()
                && self.core.strategy_id().is_some()
                && self.core.trader_id().is_none()
                && !self.historical_prepared
                && !self.historical_failed,
            "native plugin recovery verifier cannot be replaced"
        );
        self.historical_verifier = Some(verifier);
        Ok(())
    }

    pub(super) fn historical_snapshot(&self) -> Result<IndexMap<String, Vec<u8>>> {
        ensure!(
            !self.historical_failed,
            "native plugin historical recovery failed"
        );
        let mut state = DataActor::on_save(self)?;
        state.insert(
            "plugin.native_framework".into(),
            serde_json::to_vec(&self.core.native_framework_checkpoint()?.value()?)?,
        );
        Ok(state)
    }

    pub(super) fn prepare_historical(
        &mut self,
        boundary: &HistoricalReplayPreparation<'_>,
    ) -> Result<()> {
        let result = (|| {
            ensure!(
                !self.historical_prepared && !self.historical_failed,
                "native plugin historical bindings already used or failed"
            );
            let owner = self.core.strategy_id().context("plugin owner absent")?;
            self.historical_verifier
                .as_ref()
                .context("native plugin reader verifier absent")?
                .prepare(boundary, owner.as_str(), &self.historical_snapshot()?)?;
            for bar in self.adapter.subscribed_bars.clone() {
                self.restore_native_local_bars(bar, boundary)?;
            }
            for instrument in self.adapter.subscribed_quotes.clone() {
                self.restore_native_local_quotes(instrument, boundary)?;
            }
            for instrument in self.adapter.subscribed_trades.clone() {
                self.restore_native_local_trades(instrument, boundary)?;
            }
            if self.callback_failure().is_none()
                && let Some(pending) = &self.adapter.pending_history
            {
                self.restore_native_local_bars_response(pending.id()?, boundary)?;
            }
            for command in self.adapter.retained_subscriptions.clone() {
                nautilus_common::actor::DataActorNative::core_mut(self)
                    .restore_native_data_subscription_command(command, boundary)?;
            }
            self.restored_resources_unprepared = false;
            self.historical_prepared = true;
            Ok(())
        })();
        if result.is_err() {
            self.historical_failed = true;
        }
        result
    }

    pub(super) fn historical_input(
        &mut self,
        boundary: &HistoricalInputBoundary<'_>,
        input: &dyn Any,
    ) -> Result<()> {
        let result = (|| {
            ensure!(
                self.historical_prepared && !self.historical_failed,
                "native plugin historical input has no prepared owner"
            );
            let owner = self.core.strategy_id().context("plugin owner absent")?;
            self.historical_verifier
                .as_ref()
                .context("native plugin reader verifier absent")?
                .verify_input(boundary, owner.as_str())?;
            self.dispatch_historical_input(boundary, input)?;
            ensure!(!self.historical_failed, "original plugin cleanup failed");
            // A guest may return an ordinary error just as in the original
            // callback. SDK route/clock/output verification errors still fail
            // the actual replay instead of being converted to callback success.
            pending_callback_route()?;
            Ok(())
        })();
        if result.is_err() {
            self.historical_failed = true;
        }
        result
    }

    fn dispatch_historical_input(
        &mut self,
        boundary: &HistoricalInputBoundary<'_>,
        input: &dyn Any,
    ) -> Result<()> {
        let running =
            boundary.original_component_state()? == nautilus_common::enums::ComponentState::Running;
        let outcome = if let Some(response) = input.downcast_ref::<BarsResponse>() {
            DataActor::on_historical_bars_response(self, response)
        } else if let Some(lifecycle) = input.downcast_ref::<NativeComponentLifecycle>() {
            ensure!(
                Some(lifecycle.component_id.as_str())
                    == self.core.strategy_id().as_ref().map(StrategyId::as_str)
                    && lifecycle.action == "stop",
                "unsupported original plugin lifecycle"
            );
            DataActor::on_stop(self)
        } else if !running
            && (input.is::<Bar>()
                || input.is::<QuoteTick>()
                || input.is::<TradeTick>()
                || input.is::<TimeEvent>()
                || input.is::<OrderEventAny>()
                || input.is::<PositionEvent>())
        {
            return Ok(());
        } else if let Some(bar) = input.downcast_ref::<Bar>() {
            DataActor::on_bar(self, bar)
        } else if let Some(quote) = input.downcast_ref::<QuoteTick>() {
            DataActor::on_quote(self, quote)
        } else if let Some(trade) = input.downcast_ref::<TradeTick>() {
            DataActor::on_trade(self, trade)
        } else if let Some(event) = input.downcast_ref::<TimeEvent>() {
            DataActor::on_time_event(self, event)
        } else if let Some(event) = input.downcast_ref::<OrderEventAny>() {
            self.event("order", event)
        } else if let Some(event) = input.downcast_ref::<PositionEvent>() {
            // The native position wrapper does not call the aggregate callback
            // for PositionAdjusted, and applies no plugin-specific prelude.
            if !running || matches!(event, PositionEvent::PositionAdjusted(_)) {
                return Ok(());
            }
            self.event("position", event)
        } else {
            anyhow::bail!(
                "unsupported original plugin callback {}",
                boundary.route().kind
            )
        };
        // The native DataActor/Strategy wrapper logs callback errors and keeps
        // their real terminal effect. Source Complete compares the entire state;
        // verifier/route/unsupported-input errors above always propagate.
        if let Err(error) = outcome {
            log::error!("Original plugin callback returned: {error:#}");
        }
        Ok(())
    }
}
