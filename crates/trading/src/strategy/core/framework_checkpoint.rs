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

//! Explicit actual `StrategyCore` profile. No clock read, UUID generation, event
//! dispatch, timer reset, transport call, or execution permission is performed.
use super::{
    ActorId, Cache, ClientOrderId, Debug, StrategyConfig, StrategyCore, StrategyId, TraderId,
    UUID4, Ustr,
};
use anyhow::{Context, Result, ensure};
use nautilus_model::{enums::TimeInForce, orders::Order};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrameworkState {
    schema: String,
    strategy_id: StrategyId,
    actor_id: ActorId,
    trader_id: TraderId,
    configuration: Value,
    gtd_timers: BTreeMap<ClientOrderId, String>,
    gtd_expiry_ns: BTreeMap<ClientOrderId, u64>,
    managed_time_event_last_id: Option<UUID4>,
    is_exiting: bool,
    pending_stop: bool,
    market_exit_attempts: u64,
    market_exit_timer_name: String,
    market_exit_tag: String,
    client_order_id_count: usize,
    order_list_id_count: usize,
    order_manager: Value,
}

/// Actual SDK-issued read-only framework facts. No Deserialize or caller
/// constructor is exposed. Possessing this value never authorizes submission.
#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct NativeStrategyFrameworkCheckpoint(FrameworkState);

impl NativeStrategyFrameworkCheckpoint {
    /// Serializes these exact actual facts without recreating a restore capability.
    ///
    /// # Errors
    /// Returns an error if the original framework state cannot be serialized.
    pub fn value(&self) -> Result<Value> {
        Ok(serde_json::to_value(&self.0)?)
    }
    /// Exact actual object comparison, with no time/random draw or mutation.
    ///
    /// # Errors
    /// Returns an error if this actual strategy object differs from its frozen framework checkpoint.
    pub fn verify_current(&self, core: &StrategyCore) -> Result<()> {
        ensure!(
            core.native_framework_checkpoint()?.0 == self.0,
            "actual strategy framework changed from its checkpoint"
        );
        Ok(())
    }
    /// Shape/cache validation only; this never reconstructs a restore receipt.
    ///
    /// # Errors
    /// Returns an error for malformed source state or an invalid configuration, identity, cache association, or framework profile.
    pub fn validate_serialized(value: &Value, cache: &Cache) -> Result<()> {
        let state: FrameworkState = serde_json::from_value(value.clone())?;
        state.validate(cache)
    }
    /// Joins the original Core state to this same owner's frozen schedules.
    ///
    /// # Errors
    /// Returns an error if source timers or callback identities do not match the original strategy owner.
    pub fn verify_timer_inventory(&self, inventory: &Value) -> Result<()> {
        self.0.verify_timers(inventory)
    }
}

impl FrameworkState {
    fn validate(&self, cache: &Cache) -> Result<()> {
        let configuration: StrategyConfig = serde_json::from_value(self.configuration.clone())?;
        configuration.validate()?;
        ensure!(
            self.schema == "NautilusStrategyFrameworkCheckpoint.v1"
                && self.actor_id.as_str() == self.strategy_id.as_str()
                && self.market_exit_timer_name == format!("MARKET_EXIT_CHECK:{}", self.strategy_id)
                && self.market_exit_tag == "MARKET_EXIT"
                && self.order_manager["profile"] == "actual_empty_order_manager.v1"
                && self.order_manager["active_local"] == false
                && self.order_manager["submit_order_commands"] == 0
                && self.order_manager["oto_target_quantities"] == 0,
            "unsupported strategy native framework profile"
        );
        ensure!(
            self.gtd_timers.keys().eq(self.gtd_expiry_ns.keys())
                && (self.gtd_timers.is_empty() || configuration.manage_gtd_expiry),
            "native GTD ownership/expiry keys or configured manager differ"
        );
        for (id, name) in &self.gtd_timers {
            let order = cache.order(id).context("native GTD source order absent")?;
            ensure!(
                name == &format!("GTD-EXPIRY:{id}")
                    && order.strategy_id() == self.strategy_id
                    && order.trader_id() == self.trader_id
                    && order.time_in_force() == TimeInForce::Gtd
                    && order.expire_time().map(|time| time.as_u64())
                        == self.gtd_expiry_ns.get(id).copied(),
                "native GTD order/owner/expiry differs"
            );
        }
        Ok(())
    }
    fn verify_timers(&self, inventory: &Value) -> Result<()> {
        let rows = inventory["timers"]
            .as_array()
            .context("native framework owner schedules absent")?;
        let mut names = BTreeSet::new();
        for row in rows {
            let name = row["name"].as_str().context("native timer name absent")?;
            ensure!(names.insert(name), "duplicate native framework timer name");
        }
        let owner_timer = |name: &str| -> Result<&Value> {
            let row = rows
                .iter()
                .find(|row| row["name"] == name)
                .context("native framework timer schedule absent")?;
            ensure!(
                row["callback_kind"] == "registered_owner_thread"
                    && row["callback_source"] == "registered_clock_default.v1"
                    && row["binding"]["binding_id"]
                        .as_u64()
                        .is_some_and(|id| id > 0)
                    && row["binding"]["owner_thread_only"] == true
                    && matches!(row["status"].as_str(), Some("active" | "exhausted")),
                "native framework timer owner/callback differs"
            );
            Ok(row)
        };
        if self.is_exiting {
            let timer = owner_timer(&self.market_exit_timer_name)?;
            let configuration: StrategyConfig = serde_json::from_value(self.configuration.clone())?;
            ensure!(
                timer["interval_ns"].as_u64()
                    == configuration.market_exit_interval_ms.checked_mul(1_000_000),
                "native market exit timer interval differs"
            );
        }
        for (id, name) in &self.gtd_timers {
            let row = owner_timer(name)?;
            let expiry = self
                .gtd_expiry_ns
                .get(id)
                .context("native GTD expiry missing")?;
            ensure!(
                row["stop_time_ns"].as_u64() == Some(*expiry)
                    && row["next_time_ns"].as_u64() == Some(*expiry),
                "native GTD timer/cache expiry differs"
            );
        }
        Ok(())
    }
}

impl StrategyCore {
    /// Reads the limited complete Core profile from this actual registered object.
    /// This never samples time, generates an ID, dispatches, or changes lifecycle.
    ///
    /// # Errors
    /// Returns an error if the registered strategy, native order managers, or timer bindings cannot establish a complete framework snapshot.
    pub fn native_framework_checkpoint(&self) -> Result<NativeStrategyFrameworkCheckpoint> {
        let factory = self
            .order_factory
            .as_ref()
            .context("native strategy factory absent")?
            .try_borrow()?;
        let cache_rc = self.cache_rc();
        let cache = cache_rc
            .try_borrow()
            .context("native framework cache busy")?;
        let gtd_expiry_ns = self
            .gtd_timers
            .keys()
            .map(|id| {
                Ok((
                    *id,
                    cache
                        .order(id)
                        .context("native GTD source order absent")?
                        .expire_time()
                        .context("native GTD source expiry absent")?
                        .as_u64(),
                ))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let state = FrameworkState {
            schema: "NautilusStrategyFrameworkCheckpoint.v1".into(),
            strategy_id: self.strategy_id.context("native strategy not registered")?,
            actor_id: self.actor_id(),
            trader_id: self.trader_id().context("native strategy trader absent")?,
            configuration: serde_json::to_value(&self.config)?,
            gtd_timers: self
                .gtd_timers
                .iter()
                .map(|(id, name)| (*id, name.to_string()))
                .collect(),
            gtd_expiry_ns,
            managed_time_event_last_id: self.managed_time_event_last_id,
            is_exiting: self.is_exiting,
            pending_stop: self.pending_stop,
            market_exit_attempts: self.market_exit_attempts,
            market_exit_timer_name: self.market_exit_timer_name.to_string(),
            market_exit_tag: self.market_exit_tag.to_string(),
            client_order_id_count: factory.client_order_id_count(),
            order_list_id_count: factory.order_list_id_count(),
            order_manager: self
                .order_manager
                .as_ref()
                .context("native strategy order manager absent")?
                .empty_native_framework_checkpoint()?,
        };
        state.validate(&cache)?;
        Ok(NativeStrategyFrameworkCheckpoint(state))
    }

    /// Internal kernel handoff: the public LiveNode entry point supplies only
    /// reader-verified cut facts. This validates all identity/config/cache/timer
    /// facts before changing any field; it never starts the physical component.
    /// Factory counters remain owned by the business load callback, then undergo
    /// the same complete comparison. No clear/unfence/grant operation exists.
    #[doc(hidden)]
    pub fn restore_native_framework_checkpoint(
        &mut self,
        value: &Value,
        owner_timers: &Value,
    ) -> Result<NativeStrategyFrameworkCheckpoint> {
        ensure!(
            self.native_framework_restore.is_none(),
            "native strategy framework installed twice"
        );
        ensure!(
            self.actor.state() == nautilus_common::enums::ComponentState::Ready,
            "native framework restore requires a fresh physical Ready strategy"
        );
        let current = self.native_framework_checkpoint()?;
        ensure!(
            !current.0.is_exiting
                && !current.0.pending_stop
                && current.0.market_exit_attempts == 0
                && current.0.gtd_timers.is_empty()
                && current.0.managed_time_event_last_id.is_none()
                && current.0.client_order_id_count == 0
                && current.0.order_list_id_count == 0,
            "native framework target has already processed business state"
        );
        let original: FrameworkState = serde_json::from_value(value.clone())?;
        let cache_rc = self.cache_rc();
        let cache = cache_rc
            .try_borrow()
            .context("native framework restore cache busy")?;
        original.validate(&cache)?;
        original.verify_timers(owner_timers)?;
        ensure!(
            original.strategy_id == current.0.strategy_id
                && original.actor_id == current.0.actor_id
                && original.trader_id == current.0.trader_id
                && original.configuration == current.0.configuration
                && original.market_exit_timer_name == current.0.market_exit_timer_name
                && original.market_exit_tag == current.0.market_exit_tag
                && original.order_manager == current.0.order_manager,
            "native framework configuration/registered identity differs"
        );
        self.gtd_timers = original
            .gtd_timers
            .iter()
            .map(|(id, name)| (*id, Ustr::from(name.as_str())))
            .collect();
        self.managed_time_event_last_id = original.managed_time_event_last_id;
        self.is_exiting = original.is_exiting;
        self.pending_stop = original.pending_stop;
        self.market_exit_attempts = original.market_exit_attempts;
        self.market_exit_timer_name = Ustr::from(original.market_exit_timer_name.as_str());
        self.market_exit_tag = Ustr::from(original.market_exit_tag.as_str());
        let receipt = NativeStrategyFrameworkCheckpoint(original);
        self.native_framework_restore = Some(receipt.clone());
        Ok(receipt)
    }
}

/// Strict timer join for actual SDK framework facts; not a restore constructor.
pub(super) fn validate_native_framework_timers(value: &Value, inventory: &Value) -> Result<()> {
    let state: FrameworkState = serde_json::from_value(value.clone())?;
    state.verify_timers(inventory)
}
