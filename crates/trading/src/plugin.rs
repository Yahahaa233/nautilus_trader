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
    enums::{OrderSide, TimeInForce},
    events::{OrderEventAny, PositionEvent},
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use nautilus_plugin::{
    PluginError,
    strategy::{CallbackKind, HostOperation, instance::GuestInstance},
};
use serde::{Deserialize, Serialize};

use crate::strategy::{Strategy, StrategyConfig, StrategyCore};

/// Explicit native routing and bounds, supplied by the server's admitted
/// deployment. Guests cannot select another client, account or instrument.
#[derive(Clone, Debug)]
pub struct PluginScope {
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub instruments: BTreeSet<InstrumentId>,
    pub bar_types: Vec<BarType>,
    pub order_types: BTreeSet<PluginOrderType>,
    pub max_timers: usize,
    pub state_schema_version: u32,
    pub descriptor_sha256: String,
    pub configuration_sha256: String,
}

/// Order construction currently implemented by the encoded native facade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PluginOrderType {
    Market,
    Limit,
}

/// Supported wire messages are intentionally explicit. An unsupported host
/// operation returns an error; it is never treated as an empty success.
pub mod wire {
    use super::{
        BarType, ClientOrderId, Deserialize, InstrumentId, OrderSide, Price, Quantity, Serialize,
    };

    /// First encoded native strategy protocol.
    pub const VERSION: u32 = 1;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Subscription {
        pub schema_version: u32,
        pub bar_type: BarType,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Timer {
        pub schema_version: u32,
        pub name: String,
        pub alert_at_ns: u64,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct TimerName {
        pub schema_version: u32,
        pub name: String,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(tag = "order_type", rename_all = "snake_case", deny_unknown_fields)]
    pub enum Submit {
        Market {
            schema_version: u32,
            instrument_id: InstrumentId,
            side: OrderSide,
            quantity: Quantity,
            reduce_only: bool,
        },
        Limit {
            schema_version: u32,
            instrument_id: InstrumentId,
            side: OrderSide,
            quantity: Quantity,
            price: Price,
            reduce_only: bool,
            post_only: bool,
        },
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct OrderId {
        pub schema_version: u32,
        pub client_order_id: ClientOrderId,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct Modify {
        pub schema_version: u32,
        pub client_order_id: ClientOrderId,
        pub quantity: Option<Quantity>,
        pub price: Option<Price>,
        pub trigger_price: Option<Price>,
    }
}

/// Uses the same native registration, component, clock, cache and execution
/// facades as an ordinary compiled strategy. Native framework recovery still
/// captures this object's strategy state and timers at the owner's cut.
#[derive(Debug)]
pub struct PluginStrategy {
    core: StrategyCore,
    guest: Rc<RefCell<GuestInstance>>,
    scope: PluginScope,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointIdentity {
    schema_version: u32,
    descriptor_sha256: String,
    configuration_sha256: String,
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
    fn checkpoint_identity(&self) -> CheckpointIdentity {
        CheckpointIdentity {
            schema_version: self.scope.state_schema_version,
            descriptor_sha256: self.scope.descriptor_sha256.clone(),
            configuration_sha256: self.scope.configuration_sha256.clone(),
        }
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
        Ok(Self {
            core: StrategyCore::new_checked(config)?,
            guest: Rc::new(RefCell::new(guest)),
            scope,
        })
    }

    fn invoke(&mut self, kind: CallbackKind, payload: &[u8]) -> Result<Vec<u8>> {
        let guest = Rc::clone(&self.guest);
        let version = self.scope.state_schema_version;
        let mut native = |operation, input: &[u8]| {
            if matches!(kind, CallbackKind::Save | CallbackKind::Restore)
                && operation != HostOperation::Clock
            {
                return Err(PluginError::generic(
                    "state handoff has no mutating native capability",
                ));
            }
            self.dispatch(operation, input)
                .map_err(|error| PluginError::generic(format!("{error:#}")))
        };
        guest
            .try_borrow_mut()?
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
        self.guest
            .borrow()
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
        let order = self.cache().order(&id).context("plugin order is absent")?;
        ensure!(
            Some(order.strategy_id()) == self.strategy_id(),
            "order belongs to another strategy"
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
            HostOperation::Clock => {
                ensure!(input.is_empty(), "clock request must be empty");
                self.clock_facts()
            }
            HostOperation::Subscribe | HostOperation::Unsubscribe => {
                let request: wire::Subscription = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                self.require_instrument(request.bar_type.instrument_id())?;
                ensure!(
                    self.scope.bar_types.contains(&request.bar_type),
                    "bar subscription is outside the admitted declaration"
                );
                if operation == HostOperation::Subscribe {
                    self.subscribe_bars(request.bar_type, Some(self.scope.client_id), None);
                } else {
                    self.unsubscribe_bars(request.bar_type, Some(self.scope.client_id), None);
                }
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
            HostOperation::Submit => {
                let request: wire::Submit = serde_json::from_slice(input)?;
                let order = match request {
                    wire::Submit::Market {
                        schema_version,
                        instrument_id,
                        side,
                        quantity,
                        reduce_only,
                    } => {
                        ensure!(
                            self.scope.order_types.contains(&PluginOrderType::Market),
                            "market order capability was not admitted"
                        );
                        require_version(schema_version)?;
                        self.require_instrument(instrument_id)?;
                        self.order().market(
                            instrument_id,
                            side,
                            quantity,
                            Some(TimeInForce::Gtc),
                            Some(reduce_only),
                            Some(false),
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                    wire::Submit::Limit {
                        schema_version,
                        instrument_id,
                        side,
                        quantity,
                        price,
                        reduce_only,
                        post_only,
                    } => {
                        ensure!(
                            self.scope.order_types.contains(&PluginOrderType::Limit),
                            "limit order capability was not admitted"
                        );
                        require_version(schema_version)?;
                        self.require_instrument(instrument_id)?;
                        self.order().limit(
                            instrument_id,
                            side,
                            quantity,
                            price,
                            Some(TimeInForce::Gtc),
                            None,
                            Some(post_only),
                            Some(reduce_only),
                            Some(false),
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                            None,
                        )
                    }
                };
                let id = order.client_order_id();
                // Native submission keeps the application's interceptor and the
                // final adapter fence; no direct execution client is exposed.
                self.submit_order(order, None, Some(self.scope.client_id), None)?;
                Ok(serde_json::to_vec(&wire::OrderId {
                    schema_version: wire::VERSION,
                    client_order_id: id,
                })?)
            }
            HostOperation::Cancel | HostOperation::Query => {
                let request: wire::OrderId = serde_json::from_slice(input)?;
                require_version(request.schema_version)?;
                let order = self.owned_order(request.client_order_id)?;
                if operation == HostOperation::Query {
                    return Ok(serde_json::to_vec(&order)?);
                }
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
            _ => anyhow::bail!("host operation is not implemented by this native bridge"),
        }
    }

    fn fault_callback(&mut self, error: &anyhow::Error) {
        log::error!("Plugin callback failed: {error:#}");
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
        if let Err(error) = self.event("order", event) {
            log::error!("Plugin order callback: {error:#}");
        }
    }
    fn on_position_event(&mut self, event: PositionEvent) {
        if let Err(error) = self.event("position", event) {
            log::error!("Plugin position callback: {error:#}");
        }
    }
});

impl DataActor for PluginStrategy {
    fn start_inhibited(&self) -> bool {
        self.guest.borrow().is_faulted()
    }

    fn on_start(&mut self) -> Result<()> {
        self.invoke(CallbackKind::Start, b"").map(|_| ())
    }
    fn on_stop(&mut self) -> Result<()> {
        self.invoke(CallbackKind::Stop, b"native_stop").map(|_| ())
    }
    fn on_resume(&mut self) -> Result<()> {
        self.invoke(CallbackKind::Resume, b"").map(|_| ())
    }
    fn on_bar(&mut self, bar: &Bar) -> Result<()> {
        self.event("bar", bar)
    }
    fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
        self.event("quote", quote)
    }
    fn on_trade(&mut self, trade: &TradeTick) -> Result<()> {
        self.event("trade", trade)
    }
    fn on_historical_bars(&mut self, bars: &[Bar]) -> Result<()> {
        self.event("historical_bars", bars)
    }
    fn on_time_event(&mut self, event: &TimeEvent) -> Result<()> {
        self.event("timer", serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id.to_string(),"ts_event":event.ts_event.as_u64(),"ts_init":event.ts_init.as_u64()}))
    }

    fn on_fault(&mut self) -> Result<()> {
        // A failed guest cannot perform cleanup through another guest callback.
        // Release only this actor's admitted native timers. Component::fault
        // releases the actor's subscriptions; financial state remains native.
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
        Ok(())
    }

    fn on_save(&self) -> Result<IndexMap<String, Vec<u8>>> {
        if let Some((code, message)) = self.callback_failure() {
            // Never call a failed algorithm again or assert that its partial
            // opaque state is resumable. Native cache/ledger/framework facts
            // remain with their common checkpoint owners.
            return Ok(IndexMap::from([
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
            ]));
        }
        let mut readonly = |operation, input: &[u8]| {
            if operation != HostOperation::Clock || !input.is_empty() {
                return Err(PluginError::generic(
                    "Save has no mutating native capability",
                ));
            }
            self.clock_facts()
                .map_err(|error| PluginError::generic(error.to_string()))
        };
        let payload = self
            .guest
            .try_borrow_mut()?
            .invoke(
                CallbackKind::Save,
                self.scope.state_schema_version,
                b"",
                &mut readonly,
            )
            .map_err(|error| anyhow::anyhow!("plugin save: {}", error.message_string()))?;
        Ok(IndexMap::from([
            ("plugin.state".into(), payload),
            (
                "plugin.identity".into(),
                serde_json::to_vec(&self.checkpoint_identity())?,
            ),
        ]))
    }

    fn on_load(&mut self, mut state: IndexMap<String, Vec<u8>>) -> Result<()> {
        ensure!(
            state.len() == 2,
            "incomplete or unknown plugin checkpoint members"
        );
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
            return Ok(());
        }
        let payload = state
            .swap_remove("plugin.state")
            .context("plugin checkpoint state missing")?;
        self.invoke(CallbackKind::Restore, &payload)?;
        Ok(())
    }
}
