// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Encoded guest callbacks backed by the actual native strategy core.
//! Loading/publication and business admission precede construction. This
//! adapter creates no runner, venue client, order manager or account ledger.

use std::{cell::RefCell, collections::BTreeSet, rc::Rc};

use anyhow::{Context, Result, ensure};
use indexmap::IndexMap;
use nautilus_common::{actor::DataActor, component::Component, timer::TimeEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    events::{OrderEventAny, PositionEvent},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId},
    orders::{Order, OrderAny},
};
use nautilus_plugin::{
    PluginError,
    strategy::{
        CallbackKind, HostOperation,
        instance::{GuestInstance, NativeFailureRecorder},
    },
};
use serde::{Deserialize, Serialize};

use crate::strategy::{Strategy, StrategyConfig, StrategyCore};

mod adapter;
mod facts;
#[cfg(feature = "live")]
pub mod historical;
mod history_requests;
mod managed_frame;
pub use managed_frame::managed_frame_signal_name;
pub mod managed;
mod orders;
mod routing;
mod subscriptions;

/// Explicit native routing and bounds, supplied by the server's admitted
/// deployment. Guests cannot select another client, account or instrument.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginScope {
    pub client_id: ClientId,
    /// Public data routing is separate from account execution routing.
    pub data_client_id: ClientId,
    pub account_id: AccountId,
    pub instruments: BTreeSet<InstrumentId>,
    pub bar_types: Vec<BarType>,
    pub order_types: BTreeSet<PluginOrderType>,
    pub max_timers: usize,
    /// Shared bound for cached bars and any explicitly allowed native request.
    /// Zero disables both operations. A nonzero value does not allow networking.
    pub max_history_bars: usize,
    /// Fixed application policy; network history is separate from cached warmup.
    pub allow_history_requests: bool,
    pub quote_instruments: BTreeSet<InstrumentId>,
    pub trade_instruments: BTreeSet<InstrumentId>,
    pub state_schema_version: u32,
    pub descriptor_sha256: String,
    pub configuration_sha256: String,
}

/// Order construction currently implemented by the encoded native facade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PluginOrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
}

/// Shared encoded requests do not require algorithms to depend on a Core.
pub use nautilus_plugin::strategy::wire;

/// Uses the same native registration, component, clock, cache and execution
/// facades as an ordinary compiled strategy. Native framework recovery still
/// captures this object's strategy state and timers at the owner's cut.
#[derive(Debug)]
pub struct PluginStrategy {
    core: StrategyCore,
    guest: Rc<RefCell<GuestInstance>>,
    native_failure: NativeFailureRecorder,
    scope: PluginScope,
    adapter: adapter::AdapterState,
    managed_policy: Option<managed::ManagedPolicyBinding>,
    history_reissue_after_restore: bool,
    restored_resources_unprepared: bool,
    #[cfg(feature = "live")]
    historical_verifier: Option<Rc<dyn historical::PluginHistoricalVerifier>>,
    #[cfg(feature = "live")]
    historical_prepared: bool,
    #[cfg(feature = "live")]
    historical_failed: bool,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointIdentity {
    schema_version: u32,
    descriptor_sha256: String,
    configuration_sha256: String,
    /// Old experimental identities remain readable, but lack the routing proof
    /// required to resume under the complete scope. Never infer that proof.
    #[serde(default)]
    native_scope: Option<PluginScope>,
    #[serde(default)]
    managed_policy_identity: Option<managed::ManagedPolicyIdentity>,
    #[serde(default)]
    trader_id: Option<nautilus_model::identifiers::TraderId>,
    #[serde(default)]
    strategy_id: Option<nautilus_model::identifiers::StrategyId>,
}

/// Terminal native adapter state, deliberately distinct from algorithm state.
/// It records that no resumable algorithm state can be asserted after failure.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FailureCheckpoint {
    schema: String,
    code: u32,
    message: String,
}

impl FailureCheckpoint {
    fn code(&self) -> Result<nautilus_plugin::PluginErrorCode> {
        use nautilus_plugin::PluginErrorCode;
        ensure!(
            self.schema == "NautilusStrategyComponentFailure.v1",
            "unknown terminal failure schema"
        );
        Ok(match self.code {
            1 => PluginErrorCode::Generic,
            2 => PluginErrorCode::Panic,
            3 => PluginErrorCode::InvalidArgument,
            4 => PluginErrorCode::NotImplemented,
            5 => PluginErrorCode::AbiMismatch,
            6 => PluginErrorCode::SerializationFailed,
            _ => anyhow::bail!("invalid terminal failure code"),
        })
    }
}

impl PluginStrategy {
    fn adapter_checkpoint(&self) -> Result<adapter::AdapterState> {
        let mut state = self.adapter.clone();
        if !self.restored_resources_unprepared {
            state.retained_subscriptions = nautilus_common::actor::DataActorNative::core(self)
                .native_data_subscription_commands()?;
        }
        state.validate(&self.scope)?;
        Ok(state)
    }

    fn checkpoint_identity(&self) -> CheckpointIdentity {
        CheckpointIdentity {
            schema_version: self.scope.state_schema_version,
            descriptor_sha256: self.scope.descriptor_sha256.clone(),
            configuration_sha256: self.scope.configuration_sha256.clone(),
            native_scope: Some(self.scope.clone()),
            managed_policy_identity: self
                .managed_policy
                .as_ref()
                .map(|binding| binding.identity.clone()),
            trader_id: self.core.trader_id(),
            strategy_id: self.core.strategy_id(),
        }
    }

    fn terminal_checkpoint(&self) -> Result<Option<IndexMap<String, Vec<u8>>>> {
        let Some((code, message)) = self.callback_failure() else {
            return Ok(None);
        };
        Ok(Some(IndexMap::from([
            (
                "plugin.failure".into(),
                serde_json::to_vec(&FailureCheckpoint {
                    schema: "NautilusStrategyComponentFailure.v1".into(),
                    code: code as u32,
                    message,
                })?,
            ),
            (
                "plugin.identity".into(),
                serde_json::to_vec(&self.checkpoint_identity())?,
            ),
            (
                "plugin.adapter".into(),
                serde_json::to_vec(&self.adapter_checkpoint()?)?,
            ),
        ])))
    }

    fn clock_facts(&self) -> Result<Vec<u8>> {
        let clock = self.clock();
        let prefix = format!(
            "plugin:{}:",
            self.strategy_id().context("plugin is unregistered")?
        );
        let timers = clock
            .timer_names()
            .into_iter()
            .filter_map(|name| {
                name.strip_prefix(&prefix).map(|logical| {
                    serde_json::json!({
                        "name":logical,
                        "native_name":name,
                        "next_time_ns":clock.next_time_ns(&name).map(|time| time.as_u64()),
                    })
                })
            })
            .collect::<Vec<_>>();
        Ok(serde_json::to_vec(&serde_json::json!({
            "schema_version":wire::VERSION,
            "timestamp_ns":clock.timestamp_ns().as_u64(),
            "timers":timers,
        }))?)
    }
    /// Assembles an already-admitted guest without starting or authorizing it.
    ///
    /// # Errors
    ///
    /// Refuses missing routing, resource or state identities and invalid native
    /// configuration. Business execution interception is installed by the caller.
    pub fn new(config: StrategyConfig, guest: GuestInstance, scope: PluginScope) -> Result<Self> {
        config.validate()?;
        ensure!(
            config.strategy_id.is_some(),
            "plugin requires an explicit native strategy ID"
        );
        ensure!(
            !scope.instruments.is_empty(),
            "plugin has no admitted instruments"
        );
        ensure!(
            scope.state_schema_version > 0,
            "plugin state schema is absent"
        );
        ensure!(
            scope.max_history_bars <= 50_000,
            "native history request capacity exceeds the host limit"
        );
        ensure!(
            scope.quote_instruments.is_subset(&scope.instruments)
                && scope.trade_instruments.is_subset(&scope.instruments),
            "native data capabilities exceed admitted instruments"
        );
        for hash in [&scope.descriptor_sha256, &scope.configuration_sha256] {
            ensure!(
                hash.len() == 64
                    && hash
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
                "invalid plugin state identity hash"
            );
        }
        ensure!(
            !scope.order_types.is_empty(),
            "plugin has no admitted order types"
        );
        for bar in &scope.bar_types {
            ensure!(
                scope.instruments.contains(&bar.instrument_id()),
                "bar is outside admitted instruments"
            );
        }
        let mut core = StrategyCore::new_checked(config)?;
        core.execution_scope = Some(crate::strategy::execution_scope::StrategyExecutionScope {
            account_id: scope.account_id,
            client_id: scope.client_id,
            instruments: scope.instruments.clone(),
        });
        Ok(Self {
            core,
            native_failure: guest.native_failure_recorder(),
            guest: Rc::new(RefCell::new(guest)),
            scope,
            adapter: adapter::AdapterState::default(),
            managed_policy: None,
            history_reissue_after_restore: false,
            restored_resources_unprepared: false,
            #[cfg(feature = "live")]
            historical_verifier: None,
            #[cfg(feature = "live")]
            historical_prepared: false,
            #[cfg(feature = "live")]
            historical_failed: false,
        })
    }

    fn invoke(&mut self, kind: CallbackKind, payload: &[u8]) -> Result<Vec<u8>> {
        let guest = Rc::clone(&self.guest);
        let version = self.scope.state_schema_version;
        let failure = self.native_failure.clone();
        let mut native = |operation, input: &[u8]| {
            let result = if matches!(kind, CallbackKind::Save | CallbackKind::Restore) {
                self.readonly(operation, input)
            } else {
                self.dispatch(operation, input)
            };
            result.map_err(|error| {
                if let Some(fatal) = error.downcast_ref::<managed::NativeManagedFailure>() {
                    PluginError::new(fatal.code, &fatal.message)
                } else {
                    PluginError::generic(format!("{error:#}"))
                }
            })
        };
        let mut guest = guest.try_borrow_mut().map_err(|error| {
            let message = format!("native callback reentered its active guest: {error}");
            let retained = failure.retain(
                nautilus_plugin::PluginErrorCode::SerializationFailed,
                &message,
            );
            debug_assert!(retained.is_ok());
            anyhow::anyhow!(message)
        })?;
        guest
            .invoke(kind, version, payload, &mut native)
            .map_err(|error| anyhow::anyhow!("plugin callback: {}", error.message_string()))
    }

    fn event(&mut self, kind: &str, event: impl Serialize) -> Result<()> {
        let payload = serde_json::to_vec(
            &serde_json::json!({"schema_version":wire::VERSION,"kind":kind,"event":event}),
        )?;
        match self.invoke(CallbackKind::Event, &payload) {
            Ok(_) => Ok(()),
            Err(error) => {
                self.fault_callback(&error);
                Err(error)
            }
        }
    }

    /// Original callback failure for native observation. Reading it grants no
    /// recovery, execution or state reset authority.
    #[must_use]
    pub fn callback_failure(&self) -> Option<(nautilus_plugin::PluginErrorCode, String)> {
        self.native_failure
            .first_failure()
            .map(|(code, message)| (code, message.to_owned()))
    }

    fn require_instrument(&self, instrument: InstrumentId) -> Result<()> {
        ensure!(
            self.scope.instruments.contains(&instrument),
            "instrument is outside the admitted plugin scope"
        );
        Ok(())
    }

    fn timer_name(&self, logical: &str) -> Result<String> {
        ensure!(
            !logical.is_empty()
                && logical.len() <= 128
                && logical
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "invalid plugin timer name"
        );
        Ok(format!(
            "plugin:{}:{logical}",
            self.strategy_id().context("plugin is unregistered")?
        ))
    }

    fn owned_order(&self, id: ClientOrderId) -> Result<OrderAny> {
        let cache = self.cache();
        let order = cache.order(&id).context("plugin order is absent")?;
        ensure!(
            Some(order.strategy_id()) == self.strategy_id()
                && Some(order.trader_id()) == self.core.trader_id(),
            "order belongs to another strategy or trader"
        );
        ensure!(
            cache.client_id(&id) == Some(self.scope.client_id),
            "order belongs to another or unbound native client"
        );
        self.require_instrument(order.instrument_id())?;
        if let Some(account) = order.account_id() {
            ensure!(
                account == self.scope.account_id,
                "order belongs to another account"
            );
        }
        Ok(order)
    }

    fn dispatch(&mut self, operation: HostOperation, input: &[u8]) -> Result<Vec<u8>> {
        match operation {
            HostOperation::Clock
            | HostOperation::Instrument
            | HostOperation::Portfolio
            | HostOperation::Query
            | HostOperation::CachedBars => self.readonly(operation, input),
            HostOperation::ManagedOrderFacts => self.managed_order_facts(input),
            HostOperation::RequestHistory => self.request_history(input),
            HostOperation::Log => {
                let request: wire::LogRecord = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                ensure!(
                    !request.message.is_empty()
                        && request.message.len() <= 4096
                        && !request.message.chars().any(char::is_control),
                    "native log message is empty, oversized or contains control characters"
                );
                let owner = self.strategy_id().context("plugin owner is unregistered")?;
                match request.level {
                    wire::LogLevel::Debug => log::debug!(
                        "Plugin {owner} account={} client={}: {}",
                        self.scope.account_id,
                        self.scope.client_id,
                        request.message
                    ),
                    wire::LogLevel::Info => log::info!(
                        "Plugin {owner} account={} client={}: {}",
                        self.scope.account_id,
                        self.scope.client_id,
                        request.message
                    ),
                    wire::LogLevel::Warn => log::warn!(
                        "Plugin {owner} account={} client={}: {}",
                        self.scope.account_id,
                        self.scope.client_id,
                        request.message
                    ),
                    wire::LogLevel::Error => log::error!(
                        "Plugin {owner} account={} client={}: {}",
                        self.scope.account_id,
                        self.scope.client_id,
                        request.message
                    ),
                }
                Ok(Vec::new())
            }
            HostOperation::Subscribe | HostOperation::Unsubscribe => {
                self.data_subscription(operation, input)?;
                Ok(Vec::new())
            }
            HostOperation::SetTimer => {
                let request: wire::Timer = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                let name = self.timer_name(&request.name)?;
                let prefix = format!(
                    "plugin:{}:",
                    self.strategy_id().context("plugin is unregistered")?
                );
                let timers = self.clock().timer_names();
                ensure!(
                    !timers.iter().any(|existing| existing == &name),
                    "plugin timer already exists"
                );
                ensure!(
                    timers
                        .iter()
                        .filter(|name| name.starts_with(&prefix))
                        .count()
                        < self.scope.max_timers,
                    "plugin timer capacity exceeded"
                );
                self.clock().set_time_alert_ns(
                    &name,
                    UnixNanos::from(request.alert_at_ns),
                    None,
                    Some(false),
                )?;
                Ok(Vec::new())
            }
            HostOperation::CancelTimer => {
                let request: wire::TimerName = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                self.clock().cancel_timer(&self.timer_name(&request.name)?);
                Ok(Vec::new())
            }
            HostOperation::Submit => self.submit_encoded(input),
            HostOperation::ManagedSubmit => self.managed_submit(input),
            HostOperation::ManagedSubmitBatch => self.managed_submit_batch(input),
            HostOperation::ManagedMarketExit => {
                let request: wire::ManagedMarketExit = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                ensure!(
                    self.managed_policy.is_some()
                        && self.core.execution_scope.is_some()
                        && self.scope.order_types.contains(&PluginOrderType::Market)
                        && self.core.config.market_exit_reduce_only
                        && self.state() == nautilus_common::enums::ComponentState::Running,
                    "managed native exit requires the installed scoped owner and a running reduce-only framework"
                );
                self.market_exit()?;
                Ok(serde_json::to_vec(&serde_json::json!({
                    "schema_version":1,"native_framework_requested":true,
                    "is_exiting":self.core.is_exiting,
                    "execution_authorized":false
                }))?)
            }
            HostOperation::Cancel => {
                let request: wire::OrderId = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                self.owned_order(request.client_order_id)?;
                self.cancel_order(request.client_order_id, Some(self.scope.client_id), None)?;
                Ok(Vec::new())
            }
            HostOperation::Modify => {
                let request: wire::Modify = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                self.owned_order(request.client_order_id)?;
                self.modify_order(
                    request.client_order_id,
                    request.quantity,
                    request.price,
                    request.trigger_price,
                    Some(self.scope.client_id),
                    None,
                )?;
                Ok(Vec::new())
            }
        }
    }

    fn release_algorithm_resources(&mut self, retire_history: bool) -> Result<()> {
        let prefix = format!(
            "plugin:{}:",
            self.strategy_id().context("plugin is unregistered")?
        );
        let clock = self.clock();
        for name in clock.timer_names() {
            if name.starts_with(&prefix) {
                clock.cancel_timer(&name);
            }
        }
        drop(clock);
        self.release_managed_frame()?;
        self.release_subscriptions();
        self.adapter.subscribed_bars.clear();
        self.adapter.subscribed_quotes.clear();
        self.adapter.subscribed_trades.clear();
        self.adapter.retained_subscriptions.clear();
        self.restored_resources_unprepared = false;
        if retire_history {
            if let Some(pending) = &self.adapter.pending_history {
                self.retire_native_bars_response(pending.id()?)?;
            }
            self.adapter.pending_history = None;
            self.history_reissue_after_restore = false;
        }
        Ok(())
    }

    fn fault_callback(&mut self, error: &anyhow::Error) {
        if self.native_failure.first_failure().is_none() {
            let message = format!("{error:#}");
            if let Err(failure) = self
                .native_failure
                .retain(nautilus_plugin::PluginErrorCode::Generic, &message)
            {
                log::error!(
                    "Cannot retain original native callback failure: {}",
                    failure.message_string()
                );
            }
        }
        log::error!("Plugin callback failed: {error:#}");
        #[cfg(feature = "live")]
        if nautilus_common::recovery_trace::historical::active() {
            // The original failure is latched in the guest; the target remains
            // physically Ready while source callback effects are verified.
            // Replay the actual cleanup, never a Ready -> Fault transition.
            if let Err(cleanup) = DataActor::on_fault(self) {
                self.historical_failed = true;
                log::error!("Historical plugin cleanup failed: {cleanup:#}; original: {error:#}");
            }
            self.release_subscriptions();
            return;
        }
        if self.is_faulted() {
            return;
        }
        if let Err(fault) = self.fault() {
            log::error!("Native plugin fault transition failed: {fault:#}; original: {error:#}");
        }
    }
}

fn require_version(version: u32) -> Result<()> {
    ensure!(
        version == wire::VERSION,
        "unknown encoded native strategy protocol"
    );
    Ok(())
}

crate::nautilus_strategy!(PluginStrategy, {
    fn defer_order_and_position_events() -> bool {
        true
    }
    #[cfg(feature = "live")]
    fn on_native_framework_checkpoint(
        &self,
        framework: &crate::strategy::NativeStrategyFrameworkCheckpoint,
    ) -> Result<IndexMap<String, Vec<u8>>> {
        framework.verify_current(&self.core)?;
        self.on_save()
    }

    #[cfg(feature = "live")]
    fn on_native_framework_checkpoint_load(
        &mut self,
        state: IndexMap<String, Vec<u8>>,
        framework: &crate::strategy::NativeStrategyFrameworkCheckpoint,
    ) -> Result<()> {
        // The native kernel has already installed this exact owner's frozen
        // framework. Factory counters belong to this native adapter, never to
        // algorithm state or a guest-supplied order identifier.
        let facts = framework.value()?;
        let count = |field: &str| -> Result<usize> {
            Ok(usize::try_from(
                facts[field]
                    .as_u64()
                    .context("native framework factory count missing")?,
            )?)
        };
        let client_count = count("client_order_id_count")?;
        let list_count = count("order_list_id_count")?;
        {
            let factory = crate::strategy::StrategyNative::order_factory(self);
            ensure!(
                factory.client_order_id_count() == 0 && factory.order_list_id_count() == 0,
                "native plugin factory target has already been used"
            );
        }
        self.on_load(state)?;
        {
            let mut factory = crate::strategy::StrategyNative::order_factory(self);
            factory.set_client_order_id_count(client_count);
            factory.set_order_list_id_count(list_count);
        }
        framework.verify_current(&self.core)
    }

    fn on_order_event(&mut self, event: OrderEventAny) {
        let scope = self.check_order_route(&event);
        if let Err(error) = self
            .retain_route_result(scope)
            .and_then(|()| self.event("order", event))
        {
            log::error!("Plugin order callback: {error:#}");
        }
    }
    fn on_position_event(&mut self, event: PositionEvent) {
        let scope = self.check_position_route(&event);
        if let Err(error) = self
            .retain_route_result(scope)
            .and_then(|()| self.event("position", event))
        {
            log::error!("Plugin position callback: {error:#}");
        }
    }
});

impl DataActor for PluginStrategy {
    fn defer_signal_events() -> bool {
        true
    }

    #[cfg(feature = "live")]
    fn native_pending_responses(
        &self,
    ) -> Result<Vec<nautilus_common::msgbus::NativePendingResponse>> {
        let Some(pending) = &self.adapter.pending_history else {
            return Ok(Vec::new());
        };
        ensure!(
            self.callback_failure().is_none(),
            "failed guest retains unsupported pending response"
        );
        self.adapter_checkpoint()?;
        let owner = self
            .core
            .strategy_id()
            .context("native response owner absent")?
            .to_string();
        let id = pending.id()?;
        Ok(vec![nautilus_common::msgbus::NativePendingResponse {
            handler_id: format!("native.bars_response:{owner}:{id}"),
            owner,
            correlation_id: id,
        }])
    }

    #[cfg(feature = "live")]
    fn prepare_native_recovery(
        &mut self,
        boundary: &nautilus_common::recovery_trace::historical::HistoricalReplayPreparation<'_>,
    ) -> Result<()> {
        self.prepare_historical(boundary)
    }

    #[cfg(feature = "live")]
    fn on_native_recovery_state(&self) -> Result<IndexMap<String, Vec<u8>>> {
        self.historical_snapshot()
    }

    #[cfg(feature = "live")]
    fn on_native_recovery_input(
        &mut self,
        boundary: &nautilus_common::recovery_trace::historical::HistoricalInputBoundary<'_>,
        input: &dyn std::any::Any,
    ) -> Result<()> {
        self.historical_input(boundary, input)
    }

    fn start_inhibited(&self) -> bool {
        if self.restored_resources_unprepared
            && (!self.adapter.subscribed_bars.is_empty()
                || !self.adapter.subscribed_quotes.is_empty()
                || !self.adapter.subscribed_trades.is_empty()
                || self.adapter.pending_history.is_some()
                || self.adapter.managed_frame_subscribed)
        {
            return true;
        }
        #[cfg(feature = "live")]
        if self.historical_failed {
            return true;
        }
        self.guest.borrow().is_faulted()
    }

    fn on_start(&mut self) -> Result<()> {
        ensure!(
            !self.restored_resources_unprepared
                || (self.adapter.subscribed_bars.is_empty()
                    && self.adapter.subscribed_quotes.is_empty()
                    && self.adapter.subscribed_trades.is_empty()
                    && self.adapter.pending_history.is_none()
                    && !self.adapter.managed_frame_subscribed),
            "native data resources require original reader preparation"
        );
        self.restored_resources_unprepared = false;
        if self.history_reissue_after_restore && self.adapter.pending_history.is_some() {
            #[cfg(feature = "live")]
            ensure!(
                self.historical_prepared && !self.historical_failed,
                "pending history cannot restart without original reader preparation"
            );
            #[cfg(not(feature = "live"))]
            anyhow::bail!("pending history requires native reader recovery");
            #[cfg(feature = "live")]
            {
                self.send_pending_history()?;
                self.history_reissue_after_restore = false;
            }
        }
        self.subscribe_managed_frame()?;
        self.invoke(CallbackKind::Start, b"").map(|_| ())
    }
    fn on_stop(&mut self) -> Result<()> {
        let callback = self.invoke(CallbackKind::Stop, b"native_stop").map(|_| ());
        let cleanup = self.release_algorithm_resources(false);
        if let Err(first) = callback {
            if let Err(secondary) = cleanup {
                log::error!(
                    "Native cleanup failed: {secondary:#}; original stop failure: {first:#}"
                );
            }
            return Err(first);
        }
        cleanup
    }
    fn on_resume(&mut self) -> Result<()> {
        self.invoke(CallbackKind::Resume, b"").map(|_| ())
    }
    fn on_signal(&mut self, signal: &nautilus_common::signal::Signal) -> Result<()> {
        self.managed_frame_event(signal)
    }
    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        let valid = self.scope.bar_types.contains(&bar.bar_type);
        self.retain_route_result(if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "native bar callback exceeds the admitted route"
            ))
        })?;
        self.event("bar", bar)
    }
    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        let valid = self.scope.quote_instruments.contains(&quote.instrument_id);
        self.retain_route_result(if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "native quote callback exceeds the admitted route"
            ))
        })?;
        self.event("quote", quote)
    }
    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        let valid = self.scope.trade_instruments.contains(&trade.instrument_id);
        self.retain_route_result(if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "native trade callback exceeds the admitted route"
            ))
        })?;
        self.event("trade", trade)
    }
    fn on_historical_bars_response(
        &mut self,
        response: &nautilus_common::messages::data::BarsResponse,
    ) -> Result<()> {
        self.history_response(response)
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> Result<()> {
        let valid = bars.len() <= self.scope.max_history_bars
            && bars
                .iter()
                .all(|bar| self.scope.bar_types.contains(&bar.bar_type));
        self.retain_route_result(if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "native history callback exceeds the admitted route or bound"
            ))
        })?;
        self.event("historical_bars", bars)
    }
    fn on_time_event(&mut self, event: &TimeEvent) -> Result<()> {
        // The native framework already owns and processes expiry/market-exit
        // timers. Its exact consumed event must not become a guest timer or
        // spend the guest's timer budget. Changed/foreign events still enter
        // the original strict guest scope check below.
        if self.core.managed_framework_event.as_ref() == Some(event) {
            return Ok(());
        }
        let valid = self
            .strategy_id()
            .is_some_and(|owner| event.name.as_str().starts_with(&format!("plugin:{owner}:")));
        self.retain_route_result(if valid {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "native timer callback belongs to another owner"
            ))
        })?;
        self.event("timer", serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id.to_string(),"ts_event":event.ts_event.as_u64(),"ts_init":event.ts_init.as_u64()}))
    }

    fn on_fault(&mut self) -> Result<()> {
        self.release_algorithm_resources(true)
    }

    fn on_dispose(&mut self) -> Result<()> {
        self.release_algorithm_resources(true)
    }

    fn on_save(&self) -> Result<IndexMap<String, Vec<u8>>> {
        if let Some(checkpoint) = self.terminal_checkpoint()? {
            // Never call a failed algorithm again or assert that its partial
            // opaque state is resumable. Native cache/ledger/framework facts
            // remain with their common checkpoint owners.
            return Ok(checkpoint);
        }
        let mut readonly = |operation, input: &[u8]| {
            self.readonly(operation, input)
                .map_err(|error| PluginError::generic(format!("{error:#}")))
        };
        let result = {
            let mut guest = self.guest.try_borrow_mut()?;
            guest.invoke(
                CallbackKind::Save,
                self.scope.state_schema_version,
                b"",
                &mut readonly,
            )
        };
        let payload = match result {
            Ok(payload) => payload,
            Err(e) => {
                log::error!(
                    "Plugin Save retained terminal failure: {}",
                    e.message_string()
                );
                // First Save failure must already be represented in this cut,
                // without returning unvalidated bytes or waiting for a retry.
                return self
                    .terminal_checkpoint()?
                    .context("guest Save failed without its original failure latch");
            }
        };
        Ok(IndexMap::from([
            ("plugin.state".into(), payload),
            (
                "plugin.identity".into(),
                serde_json::to_vec(&self.checkpoint_identity())?,
            ),
            (
                "plugin.adapter".into(),
                serde_json::to_vec(&self.adapter_checkpoint()?)?,
            ),
        ]))
    }

    fn on_load(&mut self, mut state: IndexMap<String, Vec<u8>>) -> Result<()> {
        ensure!(
            matches!(state.len(), 2 | 3),
            "incomplete or unknown plugin checkpoint members"
        );
        let adapter = if let Some(raw) = state.swap_remove("plugin.adapter") {
            adapter::AdapterState::decode(&raw, &self.scope)?
        } else {
            // ABI-1 timer-only checkpoints predate adapter inventories. Their
            // empty declared bar domain proves that no bar route can be lost.
            // Other legacy checkpoints require a separately qualified migration.
            ensure!(
                self.scope.bar_types.is_empty()
                    && self.scope.quote_instruments.is_empty()
                    && self.scope.trade_instruments.is_empty()
                    && self.scope.max_history_bars == 0,
                "legacy plugin checkpoint lacks its native subscription inventory"
            );
            adapter::AdapterState::default()
        };
        ensure!(
            !adapter.managed_frame_subscribed || self.managed_frames_enabled(),
            "original application frame has no matching native managed policy"
        );
        ensure!(state.len() == 2, "unknown plugin checkpoint member");
        let identity: CheckpointIdentity = serde_json::from_slice(
            &state
                .swap_remove("plugin.identity")
                .context("plugin checkpoint identity missing")?,
        )?;
        ensure!(
            identity == self.checkpoint_identity(),
            "plugin checkpoint identity differs from admitted deployment"
        );
        if let Some(failure) = state.swap_remove("plugin.failure") {
            let failure: FailureCheckpoint = serde_json::from_slice(&failure)?;
            self.guest
                .try_borrow_mut()?
                .retain_checkpoint_failure(failure.code()?, &failure.message)
                .map_err(|error| {
                    anyhow::anyhow!("plugin terminal restore: {}", error.message_string())
                })?;
            self.adapter = adapter;
            self.restored_resources_unprepared = true;
            return Ok(());
        }
        let payload = state
            .swap_remove("plugin.state")
            .context("plugin checkpoint state missing")?;
        self.invoke(CallbackKind::Restore, &payload)?;
        self.history_reissue_after_restore = adapter.pending_history.is_some();
        self.adapter = adapter;
        self.restored_resources_unprepared = true;
        Ok(())
    }
}
