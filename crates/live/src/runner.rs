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

//! Async event loop runner for live and sandbox trading nodes.
//!
//! `AsyncRunner` owns seven tokio mpsc channel pairs plus a shutdown
//! signal channel. Construction creates the channels without side
//! effects. The sender halves are placed into thread-local storage
//! via [`AsyncRunner::bind_senders`] so that adapters and engine
//! components can resolve them through the `get_*_sender()` accessors
//! in `nautilus_common::runner` and `nautilus_common::live::runner`.
//!
//! Channel pairs:
//!
//! - **Time events**: timer callbacks dispatched by the clock.
//! - **System events**: system notifications handled by the live node.
//! - **System commands**: control requests handled by the live node.
//! - **Execution events**: fills, order updates, and account state from
//!   execution clients to the execution engine.
//! - **Trading commands**: deferred order actions routed to their direct endpoint.
//! - **Data events**: market data from adapters to the data engine.
//! - **Data commands**: subscribe/unsubscribe requests to data clients.
//!
//! Both `AsyncRunner::run` and `LiveNode::run` use a `biased;` select with
//! system and execution branches polled ahead of data branches. Within each
//! channel pair, events are polled before commands.
//!
//! The runner can drive the event loop in two ways:
//!
//! - **Standalone**: call [`AsyncRunner::run`], which binds senders and
//!   enters a `tokio::select!` loop internally.
//! - **Integrated**: call [`AsyncRunner::take_channels`] to extract the
//!   receivers and run the `select!` loop directly inside `LiveNode::run`,
//!   where it is interleaved with startup, reconciliation, and shutdown
//!   phases.
//!
//! # Invariants
//!
//! - `bind_senders` must be called before any code that reads from TLS.
//!   This includes adapter constructors, clock initialization, and
//!   execution client start methods. Every path from construction to
//!   the event loop must bind before the first TLS read.
//! - The event loop and all TLS consumers must execute on the same
//!   thread. Senders are cloneable and `Send`, but the `RefCell`-backed
//!   TLS slots are not accessible from other threads.
//! - Only one runner at a time should own the TLS slots on a given
//!   thread. `bind_senders` overwrites any existing TLS contents on the
//!   thread, so the last caller wins.

#[cfg(feature = "native-tail-replay")]
use anyhow::Context;
use nautilus_common::live::ingress::{FrozenIngress, IngressGate, IngressSender};
use nautilus_common::recovery_trace::NativeInputSource;
use std::{
    fmt::Debug,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_common::{
    live::{
        dispatch::DispatchMessage,
        runner::{
            replace_data_event_sender, replace_exec_event_sender, replace_system_command_sender,
            replace_system_event_sender,
        },
        sender::{DispatchSender, EventSender},
    },
    messages::{
        DataEvent, ExecutionEvent, ExecutionReport, SystemCommand, SystemEvent, data::DataCommand,
        execution::TradingCommand,
    },
    msgbus::{self, MessagingSwitchboard},
    runner::{
        DataCommandSender, TimeEventMessage, TimeEventSender, TradingCommandMessage,
        TradingCommandSender, replace_data_cmd_sender, replace_exec_cmd_sender,
        replace_time_event_sender,
    },
};
use nautilus_model::events::OrderEventAny;

use crate::dispatch::drain_callbacks;
#[cfg(feature = "node")]
use crate::node::{LiveNodeHandle, NodeState};

/// Asynchronous implementation of `DataCommandSender` for live environments.
#[derive(Debug)]
pub struct AsyncDataCommandSender {
    cmd_tx: DispatchSender<DataCommand>,
    #[cfg(feature = "node")]
    node_handle: Option<LiveNodeHandle>,
}

impl AsyncDataCommandSender {
    /// Creates a sender owned by the calling thread.
    ///
    /// Construct it on the runtime thread so command sends capture callback roots.
    #[must_use]
    pub fn new(cmd_tx: impl Into<DispatchSender<DataCommand>>) -> Self {
        let cmd_tx = cmd_tx.into();
        Self {
            cmd_tx,
            #[cfg(feature = "node")]
            node_handle: None,
        }
    }
}

impl DataCommandSender for AsyncDataCommandSender {
    fn invalidate_snapshot(&self) {
        self.cmd_tx.invalidate_snapshot();
    }

    fn execute(&self, command: DataCommand) {
        if let Err(e) = self.cmd_tx.send(command) {
            // Disposal releases retained subscriptions after the node drops its receivers
            #[cfg(feature = "node")]
            if self
                .node_handle
                .as_ref()
                .is_some_and(|handle| handle.state() == NodeState::Stopped)
            {
                return;
            }

            log::error!("Failed to send data command: {e}");
        }
    }
}

/// Asynchronous implementation of `TimeEventSender` for live environments.
#[derive(Debug, Clone)]
pub struct AsyncTimeEventSender {
    time_tx: DispatchSender<TimeEventMessage>,
}

impl AsyncTimeEventSender {
    /// Creates a sender owned by the calling runtime thread.
    #[must_use]
    pub fn new(time_tx: impl Into<DispatchSender<TimeEventMessage>>) -> Self {
        Self {
            time_tx: time_tx.into(),
        }
    }
}

impl TimeEventSender for AsyncTimeEventSender {
    fn invalidate_snapshot(&self) {
        self.time_tx.invalidate_snapshot();
    }

    fn send(&self, message: TimeEventMessage) {
        if let Err(e) = self.time_tx.send(message) {
            log::error!("Failed to send time event message: {e}");
        }
    }
}

/// Asynchronous implementation of `TradingCommandSender` for live environments.
#[derive(Debug)]
pub struct AsyncTradingCommandSender {
    cmd_tx: DispatchSender<TradingCommandMessage>,
}

impl AsyncTradingCommandSender {
    /// Creates a sender owned by the calling thread.
    ///
    /// Construct it on the runtime thread so command sends capture callback roots.
    #[must_use]
    pub fn new(cmd_tx: impl Into<DispatchSender<TradingCommandMessage>>) -> Self {
        Self {
            cmd_tx: cmd_tx.into(),
        }
    }
}

impl TradingCommandSender for AsyncTradingCommandSender {
    fn invalidate_snapshot(&self) {
        self.cmd_tx.invalidate_snapshot();
    }

    fn execute(&self, message: TradingCommandMessage) {
        if let Err(e) = self.cmd_tx.send(message) {
            log::error!("Failed to send trading command: {e}");
        }
    }
}

#[path = "runner_snapshot.rs"]
mod snapshot;
#[cfg(feature = "node")]
pub(crate) use snapshot::Retained;
pub use snapshot::SnapshotReceiver;

pub trait Runner {
    fn run(&mut self);
}

/// Channel receivers for the async event loop.
///
/// These can be extracted from `AsyncRunner` via `take_channels()` to drive
/// the event loop directly on the same thread as the msgbus endpoints.
#[derive(Debug)]
pub struct AsyncRunnerChannels {
    pub time_evt_rx: SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
    pub system_evt_rx: SnapshotReceiver<DispatchMessage<SystemEvent>>,
    pub system_cmd_rx: SnapshotReceiver<DispatchMessage<SystemCommand>>,
    pub exec_evt_rx: SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
    pub exec_cmd_rx: SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
    pub data_evt_rx: SnapshotReceiver<DispatchMessage<DataEvent>>,
    pub data_cmd_rx: SnapshotReceiver<DispatchMessage<DataCommand>>,
}

#[cfg(feature = "node")]
#[allow(
    clippy::large_enum_variant,
    reason = "runner events are consumed immediately; boxing would add routing allocations"
)]
pub(crate) enum PendingRunnerEvent {
    TimeEvent(DispatchMessage<TimeEventMessage>),
    SystemEvent(DispatchMessage<SystemEvent>),
    SystemCommand(DispatchMessage<SystemCommand>),
    ExecEvent(DispatchMessage<ExecutionEvent>),
    ExecCommand(DispatchMessage<TradingCommandMessage>),
    DataEvent(DispatchMessage<DataEvent>),
    DataCommand(DispatchMessage<DataCommand>),
}

pub struct AsyncRunner {
    ingress: IngressGate,
    channels: AsyncRunnerChannels,
    time_evt_tx: IngressSender<DispatchMessage<TimeEventMessage>>,
    system_evt_tx: IngressSender<DispatchMessage<SystemEvent>>,
    system_cmd_tx: IngressSender<DispatchMessage<SystemCommand>>,
    signal_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    signal_tx: tokio::sync::mpsc::UnboundedSender<()>,
    exec_evt_tx: IngressSender<DispatchMessage<ExecutionEvent>>,
    exec_cmd_tx: IngressSender<DispatchMessage<TradingCommandMessage>>,
    data_evt_tx: IngressSender<DispatchMessage<DataEvent>>,
    data_cmd_tx: IngressSender<DispatchMessage<DataCommand>>,
    recovery_handoff_available: Arc<AtomicBool>,
    recovery_progress: crate::runner_recovery::RunnerRecoveryProgressHandle,
}

/// Handle for stopping the `AsyncRunner` from another context.
#[derive(Clone, Debug)]
pub struct AsyncRunnerHandle {
    ingress: IngressGate,
    signal_tx: tokio::sync::mpsc::UnboundedSender<()>,
}

impl AsyncRunnerHandle {
    /// Signals the runner to stop.
    pub fn stop(&self) {
        self.ingress.stop_snapshot_admission();
        if let Err(e) = self.signal_tx.send(()) {
            log::error!("Failed to send shutdown signal: {e}");
        }
    }
}

impl Default for AsyncRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl Debug for AsyncRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AsyncRunner)).finish()
    }
}

/// Borrowed receiver inventory owned by the running node loop. This cannot be
/// constructed by an application and cannot replace the actual channel receivers.
#[cfg(all(feature = "node", feature = "dispatch-observer"))]
pub(crate) struct RunningReceivers<'a> {
    pub time_events: &'a mut SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
    pub system_events: &'a mut SnapshotReceiver<DispatchMessage<SystemEvent>>,
    pub system_commands: &'a mut SnapshotReceiver<DispatchMessage<SystemCommand>>,
    pub execution_events: &'a mut SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
    pub execution_commands: &'a mut SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
    pub data_events: &'a mut SnapshotReceiver<DispatchMessage<DataEvent>>,
    pub data_commands: &'a mut SnapshotReceiver<DispatchMessage<DataCommand>>,
}

#[cfg(all(feature = "node", feature = "dispatch-observer"))]
impl RunningReceivers<'_> {
    pub(crate) fn callback_ancestry(
        &self,
    ) -> anyhow::Result<crate::runner_recovery::RunnerCallbackAncestry> {
        let mut contexts = Vec::new();
        let mut receipts = Vec::new();
        macro_rules! capture {
            ($field:ident) => {
                #[cfg(feature = "native-tail-replay")]
                let members = self.$field.pending().zip(self.$field.pending_receipts());
                #[cfg(not(feature = "native-tail-replay"))]
                let members = self.$field.pending().map(|message| {
                    (
                        message,
                        None::<&nautilus_common::recovery_trace::NativeIngressReceipt>,
                    )
                });
                for (message, receipt) in members {
                    contexts.push(message.checkpoint_context());
                    receipts.push(receipt.cloned());
                }
            };
        }
        capture!(time_events);
        capture!(system_events);
        capture!(system_commands);
        capture!(execution_events);
        capture!(execution_commands);
        capture!(data_events);
        capture!(data_commands);
        let state = nautilus_common::actor::capture_channel_contexts(&contexts)?;
        Ok(crate::runner_recovery::RunnerCallbackAncestry {
            profile: crate::runner_recovery::CALLBACK_ANCESTRY_PROFILE.into(),
            receipts,
            state,
        })
    }

    pub(crate) fn reborrow(&mut self) -> RunningReceivers<'_> {
        RunningReceivers {
            time_events: self.time_events,
            system_events: self.system_events,
            system_commands: self.system_commands,
            execution_events: self.execution_events,
            execution_commands: self.execution_commands,
            data_events: self.data_events,
            data_commands: self.data_commands,
        }
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn prepare_recovered_prefix_release(&mut self) -> anyhow::Result<()> {
        self.time_events.prepare_recovered_prefix_release()?;
        self.system_events.prepare_recovered_prefix_release()?;
        self.system_commands.prepare_recovered_prefix_release()?;
        self.execution_events.prepare_recovered_prefix_release()?;
        self.execution_commands.prepare_recovered_prefix_release()?;
        self.data_events.prepare_recovered_prefix_release()?;
        self.data_commands.prepare_recovered_prefix_release()?;
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn release_recovered_prefix(&mut self) {
        self.time_events.release_recovered_prefix();
        self.system_events.release_recovered_prefix();
        self.system_commands.release_recovered_prefix();
        self.execution_events.release_recovered_prefix();
        self.execution_commands.release_recovered_prefix();
        self.data_events.release_recovered_prefix();
        self.data_commands.release_recovered_prefix();
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn native_pending_receipts(
        &self,
    ) -> anyhow::Result<Vec<nautilus_common::recovery_trace::NativeIngressReceipt>> {
        let mut pending = Vec::new();
        macro_rules! receipts {
            ($field:ident) => {
                for receipt in self.$field.pending_receipts() {
                    pending.push(
                        receipt
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "actual native pending member has no source receipt"
                                )
                            })?
                            .clone(),
                    );
                }
            };
        }
        receipts!(time_events);
        receipts!(system_events);
        receipts!(system_commands);
        receipts!(execution_events);
        receipts!(execution_commands);
        receipts!(data_events);
        receipts!(data_commands);
        Ok(pending)
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn native_pending_inputs(
        &self,
        encode: &dyn Fn(
            nautilus_common::recovery_trace::NativeInputSource,
            &dyn std::any::Any,
        ) -> anyhow::Result<serde_json::Value>,
    ) -> anyhow::Result<Vec<nautilus_common::recovery_trace::NativePendingInput>> {
        use nautilus_common::recovery_trace::{NativeInputSource, NativePendingInput};
        let mut pending = Vec::new();
        macro_rules! capture {
            ($field:ident, $source:ident) => {
                for (message, receipt) in self.$field.pending().zip(self.$field.pending_receipts()) {
                    let receipt = receipt.context("actual staged input has no native receipt")?.clone();
                    anyhow::ensure!(receipt.input_source == NativeInputSource::$source, "native staged channel changed");
                    let any: &dyn std::any::Any = message.message();
                    let callback_binding = any.downcast_ref::<TimeEventMessage>().map(TimeEventMessage::native_input_callback_binding);
                    let timer_event = any.downcast_ref::<TimeEventMessage>().map(|message| {
                        let event = message.event(); serde_json::json!({"name":event.name,"event_id":event.event_id,"ts_event":event.ts_event,"ts_init":event.ts_init})
                    });
                    pending.push(NativePendingInput { receipt, payload: encode(NativeInputSource::$source, any)?, callback_binding, timer_event });
                }
            };
        }
        capture!(time_events, Time);
        capture!(system_events, SystemEvent);
        capture!(system_commands, SystemCommand);
        capture!(execution_events, ExecutionEvent);
        capture!(execution_commands, TradingCommand);
        capture!(data_events, DataEvent);
        capture!(data_commands, DataCommand);
        Ok(pending)
    }
    pub(crate) fn snapshot(
        &mut self,
        ingress: &IngressGate,
        guard: &FrozenIngress,
        registry: &crate::runner_recovery::RunnerRecoveryCodecRegistry,
    ) -> anyhow::Result<crate::runner_recovery::RunnerPendingSnapshot> {
        anyhow::ensure!(guard.belongs_to(ingress), "foreign running ingress guard");
        guard.verify()?;
        anyhow::ensure!(registry.is_sealed(), "capture registry is not sealed");
        let mut entries = Vec::new();
        macro_rules! capture {
            ($field:ident, $variant:ident) => {
                self.$field.stage()?;
                for (ordinal, message) in self.$field.pending().enumerate() {
                    guard.verify()?;
                    entries.push(registry.encode(
                        crate::runner_recovery::RunnerRecoveryEventRef::$variant(message.message()),
                        u64::try_from(ordinal)?,
                    )?);
                    guard.verify()?;
                }
            };
        }
        capture!(time_events, TimeEvent);
        capture!(system_events, SystemEvent);
        capture!(system_commands, SystemCommand);
        capture!(execution_events, ExecutionEvent);
        capture!(execution_commands, ExecutionCommand);
        capture!(data_events, DataEvent);
        capture!(data_commands, DataCommand);
        guard.verify()?;
        let callback_ancestry = Some(self.callback_ancestry()?);
        guard.verify()?;
        Ok(crate::runner_recovery::RunnerPendingSnapshot {
            entries,
            callback_ancestry,
        })
    }
}

impl AsyncRunner {
    /// Creates a new [`AsyncRunner`] instance.
    ///
    /// Creates channels but does not bind senders to thread-local storage.
    /// Call [`bind_senders`](Self::bind_senders) before creating clients that
    /// read from TLS, and again before entering the event loop.
    #[must_use]
    #[rustfmt::skip]
    pub fn new() -> Self {
        use tokio::sync::mpsc::unbounded_channel; // tokio-import-ok

        let ingress = IngressGate::new();
        let (time_evt_tx, time_events) =
            ingress.native_channel::<DispatchMessage<TimeEventMessage>>(NativeInputSource::Time);
        let (system_evt_tx, system_events) =
            ingress.native_channel::<DispatchMessage<SystemEvent>>(NativeInputSource::SystemEvent);
        let (system_cmd_tx, system_commands) =
            ingress.native_channel::<DispatchMessage<SystemCommand>>(NativeInputSource::SystemCommand);
        let (signal_tx, signal_rx) = unbounded_channel::<()>();
        let (exec_evt_tx, execution_events) =
            ingress.native_channel::<DispatchMessage<ExecutionEvent>>(NativeInputSource::ExecutionEvent);
        let (exec_cmd_tx, execution_commands) =
            ingress.native_channel::<DispatchMessage<TradingCommandMessage>>(NativeInputSource::TradingCommand);
        let (data_evt_tx, data_events) =
            ingress.native_channel::<DispatchMessage<DataEvent>>(NativeInputSource::DataEvent);
        let (data_cmd_tx, data_commands) =
            ingress.native_channel::<DispatchMessage<DataCommand>>(NativeInputSource::DataCommand);
        let recovery_progress = crate::runner_recovery::RunnerRecoveryProgressHandle::new();

        Self {
            ingress,
            channels: AsyncRunnerChannels {
                time_evt_rx: time_events.into(),
                system_evt_rx: system_events.into(),
                system_cmd_rx: system_commands.into(),
                exec_evt_rx: execution_events.into(),
                exec_cmd_rx: execution_commands.into(),
                data_evt_rx: data_events.into(),
                data_cmd_rx: data_commands.into(),
            },
            time_evt_tx,
            system_evt_tx,
            system_cmd_tx,
            signal_rx,
            signal_tx,
            exec_evt_tx,
            exec_cmd_tx,
            data_evt_tx,
            data_cmd_tx,
            recovery_handoff_available: Arc::new(AtomicBool::new(true)),
            recovery_progress,
        }
    }

    #[cfg(feature = "node")]
    pub(crate) fn ingress_gate(&self) -> IngressGate {
        self.ingress.clone()
    }

    /// Checks that admission has not failed or entered a snapshot freeze.
    ///
    /// # Errors
    /// Returns an error after a rejected frozen send or abandoned snapshot.
    pub fn verify_ingress(&self) -> anyhow::Result<()> {
        self.ingress.verify_open()
    }

    /// Freezes admission through all clones of this runner's seven senders.
    /// This is only a producer barrier, not a complete component checkpoint.
    /// A send attempted during collection invalidates the guard and the gate.
    /// The caller must finish the guard after successful collection; dropping
    /// it without finishing leaves admission failed closed.
    ///
    /// # Errors
    /// Refuses an unhealthy or already frozen gate.
    pub fn freeze_ingress(&self) -> anyhow::Result<FrozenIngress> {
        self.ingress.freeze()
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn install_native_retained_input(
        &mut self,
        guard: &FrozenIngress,
        event: crate::runner_recovery::RunnerRecoveryEvent,
        receipt: nautilus_common::recovery_trace::NativeIngressReceipt,
        context: Option<nautilus_common::actor::RestoredCallbackContext>,
    ) -> anyhow::Result<()> {
        use crate::runner_recovery::RunnerRecoveryEvent;
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign retained ingress gate"
        );
        guard.verify()?;
        match event {
            RunnerRecoveryEvent::TimeEvent(message)
                if receipt.input_source == NativeInputSource::Time =>
            {
                self.channels.time_evt_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::SystemEvent(message)
                if receipt.input_source == NativeInputSource::SystemEvent =>
            {
                self.channels.system_evt_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::SystemCommand(message)
                if receipt.input_source == NativeInputSource::SystemCommand =>
            {
                self.channels.system_cmd_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::ExecutionEvent(message)
                if receipt.input_source == NativeInputSource::ExecutionEvent =>
            {
                self.channels.exec_evt_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::ExecutionCommand(message)
                if receipt.input_source == NativeInputSource::TradingCommand =>
            {
                self.channels.exec_cmd_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::DataEvent(message)
                if receipt.input_source == NativeInputSource::DataEvent =>
            {
                self.channels.data_evt_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            RunnerRecoveryEvent::DataCommand(message)
                if receipt.input_source == NativeInputSource::DataCommand =>
            {
                self.channels.data_cmd_rx.append_native_retained(
                    DispatchMessage::from_checkpoint_context(message, context),
                    receipt,
                )?;
            }
            _ => anyhow::bail!("retained original input decoder changed its channel"),
        }
        guard.verify()
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn bind_historical_source_frontiers(
        &mut self,
        guard: &FrozenIngress,
        frontiers: &[(
            nautilus_common::recovery_trace::NativeInputSource,
            nautilus_core::UUID4,
            u64,
        )],
    ) -> anyhow::Result<()> {
        use nautilus_common::recovery_trace::NativeInputSource as N;
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign historical source frontier gate"
        );
        guard.verify()?;
        for &(source, channel, ordinal) in frontiers {
            macro_rules! bind {
                ($field:ident) => {
                    self.channels
                        .$field
                        .bind_historical_source_frontier(guard, channel, ordinal)?
                };
            }
            match source {
                N::Time => bind!(time_evt_rx),
                N::SystemEvent => bind!(system_evt_rx),
                N::SystemCommand => bind!(system_cmd_rx),
                N::ExecutionEvent => bind!(exec_evt_rx),
                N::TradingCommand => bind!(exec_cmd_rx),
                N::DataEvent => bind!(data_evt_rx),
                N::DataCommand => bind!(data_cmd_rx),
                _ => anyhow::bail!("historical frontier is not a physical runner channel"),
            }
        }
        guard.verify()
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn install_historical_external_input(
        &mut self,
        guard: &FrozenIngress,
        event: crate::runner_recovery::RunnerRecoveryEvent,
        receipt: nautilus_common::recovery_trace::NativeIngressReceipt,
    ) -> anyhow::Result<()> {
        use crate::runner_recovery::RunnerRecoveryEvent as E;
        use nautilus_common::recovery_trace::NativeInputSource as N;
        anyhow::ensure!(
            guard.belongs_to(&self.ingress) && receipt.caused_by.is_none(),
            "historical external input is foreign or claims an unprocessed native parent"
        );
        guard.verify()?;
        macro_rules! append {
            ($field:ident, $message:ident) => {
                self.channels.$field.append_historical_external(
                    guard,
                    DispatchMessage::from($message),
                    receipt,
                )?
            };
        }
        match event {
            E::TimeEvent(message) if receipt.input_source == N::Time => {
                append!(time_evt_rx, message);
            }
            E::SystemEvent(message) if receipt.input_source == N::SystemEvent => {
                append!(system_evt_rx, message);
            }
            E::SystemCommand(message) if receipt.input_source == N::SystemCommand => {
                append!(system_cmd_rx, message);
            }
            E::ExecutionEvent(message) if receipt.input_source == N::ExecutionEvent => {
                append!(exec_evt_rx, message);
            }
            E::ExecutionCommand(message) if receipt.input_source == N::TradingCommand => {
                append!(exec_cmd_rx, message);
            }
            E::DataEvent(message) if receipt.input_source == N::DataEvent => {
                append!(data_evt_rx, message);
            }
            E::DataCommand(message) if receipt.input_source == N::DataCommand => {
                append!(data_cmd_rx, message);
            }
            _ => anyhow::bail!("historical external source decoder changed its channel"),
        }
        guard.verify()
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn take_historical_native_input(
        &mut self,
        guard: &FrozenIngress,
        receipt: &nautilus_common::recovery_trace::NativeIngressReceipt,
    ) -> anyhow::Result<Option<PendingRunnerEvent>> {
        use nautilus_common::recovery_trace::NativeInputSource as N;
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign historical source dequeue gate"
        );
        guard.verify()?;
        macro_rules! take {
            ($field:ident, $variant:ident) => {
                self.channels
                    .$field
                    .take_historical_front(receipt)?
                    .map(|entry| PendingRunnerEvent::$variant(entry.activate()))
            };
        }
        let event = match receipt.input_source {
            N::Time => take!(time_evt_rx, TimeEvent),
            N::SystemEvent => take!(system_evt_rx, SystemEvent),
            N::SystemCommand => take!(system_cmd_rx, SystemCommand),
            N::ExecutionEvent => take!(exec_evt_rx, ExecEvent),
            N::TradingCommand => take!(exec_cmd_rx, ExecCommand),
            N::DataEvent => take!(data_evt_rx, DataEvent),
            N::DataCommand => take!(data_cmd_rx, DataCommand),
            _ => anyhow::bail!("historical source is not a physical runner channel"),
        };
        guard.verify()?;
        Ok(event)
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn historical_pending_inputs(
        &mut self,
        guard: &FrozenIngress,
        encode: &dyn Fn(
            nautilus_common::recovery_trace::NativeInputSource,
            &dyn std::any::Any,
        ) -> anyhow::Result<serde_json::Value>,
    ) -> anyhow::Result<(
        Vec<nautilus_common::recovery_trace::NativePendingInput>,
        crate::runner_recovery::RunnerCallbackAncestry,
    )> {
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign historical FIFO inventory gate"
        );
        guard.verify()?;
        let receivers = RunningReceivers {
            time_events: &mut self.channels.time_evt_rx,
            system_events: &mut self.channels.system_evt_rx,
            system_commands: &mut self.channels.system_cmd_rx,
            execution_events: &mut self.channels.exec_evt_rx,
            execution_commands: &mut self.channels.exec_cmd_rx,
            data_events: &mut self.channels.data_evt_rx,
            data_commands: &mut self.channels.data_cmd_rx,
        };
        receivers.time_events.hold_historical_pending()?;
        receivers.system_events.hold_historical_pending()?;
        receivers.system_commands.hold_historical_pending()?;
        receivers.execution_events.hold_historical_pending()?;
        receivers.execution_commands.hold_historical_pending()?;
        receivers.data_events.hold_historical_pending()?;
        receivers.data_commands.hold_historical_pending()?;
        let inputs = receivers.native_pending_inputs(encode)?;
        let ancestry = receivers.callback_ancestry()?;
        guard.verify()?;
        Ok((inputs, ancestry))
    }

    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn verify_native_callback_ancestry(
        &mut self,
        guard: &FrozenIngress,
        expected: &crate::runner_recovery::RunnerCallbackAncestry,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign callback restore gate"
        );
        guard.verify()?;
        let receivers = RunningReceivers {
            time_events: &mut self.channels.time_evt_rx,
            system_events: &mut self.channels.system_evt_rx,
            system_commands: &mut self.channels.system_cmd_rx,
            execution_events: &mut self.channels.exec_evt_rx,
            execution_commands: &mut self.channels.exec_cmd_rx,
            data_events: &mut self.channels.data_evt_rx,
            data_commands: &mut self.channels.data_cmd_rx,
        };
        anyhow::ensure!(
            receivers.callback_ancestry()? == *expected,
            "restored shared callback chains or physical receipts differ from source"
        );
        guard.verify()
    }

    /// Captures actual retained messages under this runner's live ingress guard.
    /// Messages remain in their original channel FIFO, including on codec failure.
    /// This covers seven queues only, not timers, component internals, or global order.
    ///
    /// # Errors
    /// Refuses foreign/invalid guards, extracted receivers, and unsupported codecs.
    /// Any failure after staging starts invalidates the ingress boundary.
    pub fn snapshot_pending(
        &mut self,
        guard: &FrozenIngress,
        registry: &crate::runner_recovery::RunnerRecoveryCodecRegistry,
    ) -> anyhow::Result<crate::runner_recovery::RunnerPendingSnapshot> {
        anyhow::ensure!(
            guard.belongs_to(&self.ingress),
            "foreign runner ingress guard"
        );
        guard.verify()?;
        anyhow::ensure!(
            self.recovery_handoff_available.load(Ordering::Acquire),
            "runner receivers no longer available for paused capture"
        );
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            anyhow::ensure!(registry.is_sealed(), "capture registry is not sealed");
            let mut entries = Vec::new();
            let mut contexts = Vec::new();
            let mut receipts = Vec::new();
            macro_rules! capture {
                ($field:ident, $variant:ident) => {
                    self.channels.$field.stage()?;
                    #[cfg(feature = "native-tail-replay")]
                    let members = self
                        .channels
                        .$field
                        .pending()
                        .zip(self.channels.$field.pending_receipts());
                    #[cfg(not(feature = "native-tail-replay"))]
                    let members = self.channels.$field.pending().map(|message| {
                        (
                            message,
                            None::<&nautilus_common::recovery_trace::NativeIngressReceipt>,
                        )
                    });
                    for (ordinal, (message, receipt)) in members.enumerate() {
                        guard.verify()?;
                        entries.push(registry.encode(
                            crate::runner_recovery::RunnerRecoveryEventRef::$variant(
                                message.message(),
                            ),
                            u64::try_from(ordinal)?,
                        )?);
                        contexts.push(message.checkpoint_context());
                        receipts.push(receipt.cloned());
                        guard.verify()?;
                    }
                };
            }
            capture!(time_evt_rx, TimeEvent);
            capture!(system_evt_rx, SystemEvent);
            capture!(system_cmd_rx, SystemCommand);
            capture!(exec_evt_rx, ExecutionEvent);
            capture!(exec_cmd_rx, ExecutionCommand);
            capture!(data_evt_rx, DataEvent);
            capture!(data_cmd_rx, DataCommand);
            guard.verify()?;
            let callback_ancestry = Some(crate::runner_recovery::RunnerCallbackAncestry {
                profile: crate::runner_recovery::CALLBACK_ANCESTRY_PROFILE.into(),
                receipts,
                state: nautilus_common::actor::capture_channel_contexts(&contexts)?,
            });
            guard.verify()?;
            Ok(crate::runner_recovery::RunnerPendingSnapshot {
                entries,
                callback_ancestry,
            })
        }));
        match outcome {
            Ok(Ok(snapshot)) => Ok(snapshot),
            Ok(Err(error)) => {
                self.ingress.invalidate();
                Err(error)
            }
            Err(panic) => {
                self.ingress.invalidate();
                std::panic::resume_unwind(panic)
            }
        }
    }

    /// Binds this runner's channel senders to thread-local storage.
    ///
    /// Call before creating clients that read from TLS (e.g., in the builder),
    /// and again before entering the event loop to reclaim ownership if another
    /// runner was constructed on this thread in the interim.
    pub fn bind_senders(&self) {
        self.bind_senders_with_data_sender(AsyncDataCommandSender::new(self.data_cmd_tx.clone()));
    }

    #[cfg(feature = "node")]
    pub(crate) fn bind_senders_for_node(&self, handle: LiveNodeHandle) {
        self.bind_senders_with_data_sender(AsyncDataCommandSender {
            cmd_tx: self.data_cmd_tx.clone().into(),
            node_handle: Some(handle),
        });
    }

    #[rustfmt::skip]
    fn bind_senders_with_data_sender(&self, sender: AsyncDataCommandSender) {
        replace_time_event_sender(Arc::new(AsyncTimeEventSender::new(self.time_evt_tx.clone())));
        replace_system_event_sender(EventSender::new(self.system_evt_tx.clone()));
        replace_system_command_sender(DispatchSender::new(self.system_cmd_tx.clone()));
        replace_exec_event_sender(EventSender::new(self.exec_evt_tx.clone()));
        replace_exec_cmd_sender(Arc::new(AsyncTradingCommandSender::new(self.exec_cmd_tx.clone())));
        replace_data_event_sender(EventSender::new(self.data_evt_tx.clone()));
        replace_data_cmd_sender(Arc::new(sender));
    }

    /// Read-only per-channel counts. Publishers can race this observation; it is not a fence.
    pub(crate) fn pending_queue_counts(
        &self,
    ) -> std::collections::BTreeMap<crate::runner_recovery::RunnerRecoveryChannel, usize> {
        use crate::runner_recovery::RunnerRecoveryChannel as Channel;
        std::collections::BTreeMap::from([
            (Channel::TimeEvent, self.channels.time_evt_rx.len()),
            (Channel::SystemEvent, self.channels.system_evt_rx.len()),
            (Channel::SystemCommand, self.channels.system_cmd_rx.len()),
            (Channel::ExecutionEvent, self.channels.exec_evt_rx.len()),
            (Channel::ExecutionCommand, self.channels.exec_cmd_rx.len()),
            (Channel::DataEvent, self.channels.data_evt_rx.len()),
            (Channel::DataCommand, self.channels.data_cmd_rx.len()),
        ])
    }

    /// Returns a pre-start handoff into this runner's internal channels.
    ///
    /// # Errors
    /// Returns an error after the runner has entered its loop or its receivers
    /// have been extracted.
    pub fn recovery_handoff(
        &self,
    ) -> anyhow::Result<crate::runner_recovery::RunnerRecoveryHandoff> {
        crate::runner_recovery::RunnerRecoveryHandoff::from_runner(self)
    }

    /// Returns the shared bookkeeping handle for a pre-start recovery
    /// handoff.
    ///
    /// Capture this handle before `take_channels()` consumes the runner. The
    /// live node can acknowledge dequeue and successful processing through
    /// the handle after startup; those acknowledgements remain independent of
    /// the sender's sent watermark and never grant execution authority.
    #[must_use]
    pub fn recovery_progress(&self) -> crate::runner_recovery::RunnerRecoveryProgressHandle {
        self.recovery_progress.clone()
    }

    pub(crate) fn recovery_progress_handle(
        &self,
    ) -> crate::runner_recovery::RunnerRecoveryProgressHandle {
        self.recovery_progress.clone()
    }

    /// Closes any outstanding recovery handoff handles.
    ///
    /// Hosts which use `LiveNode::start`, whose channels remain owned by the
    /// node after startup, must call this before handing control to the live
    /// trader.
    pub fn close_recovery_handoff(&self) {
        self.recovery_handoff_available
            .store(false, Ordering::Release);
    }

    pub(crate) fn recovery_handoff_available(&self) -> &Arc<AtomicBool> {
        &self.recovery_handoff_available
    }

    pub(crate) fn time_event_sender_clone(
        &self,
    ) -> IngressSender<DispatchMessage<TimeEventMessage>> {
        self.time_evt_tx.clone()
    }

    pub(crate) fn system_event_sender_clone(&self) -> IngressSender<DispatchMessage<SystemEvent>> {
        self.system_evt_tx.clone()
    }

    pub(crate) fn system_command_sender_clone(
        &self,
    ) -> IngressSender<DispatchMessage<SystemCommand>> {
        self.system_cmd_tx.clone()
    }

    pub(crate) fn execution_event_sender_clone(
        &self,
    ) -> IngressSender<DispatchMessage<ExecutionEvent>> {
        self.exec_evt_tx.clone()
    }

    pub(crate) fn execution_command_sender_clone(
        &self,
    ) -> IngressSender<DispatchMessage<TradingCommandMessage>> {
        self.exec_cmd_tx.clone()
    }

    pub(crate) fn data_event_sender_clone(&self) -> IngressSender<DispatchMessage<DataEvent>> {
        self.data_evt_tx.clone()
    }

    pub(crate) fn data_command_sender_clone(&self) -> IngressSender<DispatchMessage<DataCommand>> {
        self.data_cmd_tx.clone()
    }

    /// Stops the runner with an internal shutdown signal.
    pub fn stop(&self) {
        self.ingress.stop_snapshot_admission();
        if let Err(e) = self.signal_tx.send(()) {
            log::error!("Failed to send shutdown signal: {e}");
        }
    }

    /// Returns a handle that can be used to stop the runner from another context.
    #[must_use]
    pub fn handle(&self) -> AsyncRunnerHandle {
        AsyncRunnerHandle {
            ingress: self.ingress.clone(),
            signal_tx: self.signal_tx.clone(),
        }
    }

    /// Consumes the runner and returns the channel receivers for direct event loop driving.
    ///
    /// This is used when the event loop needs to run on the same thread as the msgbus
    /// endpoints (which use thread-local storage).
    #[must_use]
    pub fn take_channels(self) -> AsyncRunnerChannels {
        self.ingress.invalidate_if_frozen();
        self.close_recovery_handoff();
        self.channels
    }

    #[cfg(feature = "node")]
    pub(crate) fn startup_channels_mut(&mut self) -> &mut AsyncRunnerChannels {
        self.ingress.invalidate_if_frozen();
        &mut self.channels
    }

    /// Flushes all pending data events and commands from the channels.
    ///
    /// Loops until both data channels are empty, processing each item
    /// into the cache immediately. Used in `start()` where channels are
    /// not extracted.
    pub fn flush_pending_data(&mut self) {
        self.ingress.invalidate_if_frozen();
        let mut total = 0;

        loop {
            let mut progressed = false;

            // Events drain before commands here even though the runtime select
            // prefers the opposite for everything-else: `LiveNode::start()`
            // calls this after `connect_data_clients()` to push queued
            // `DataEvent::Instrument` items into the cache. A pending
            // subscription command (e.g. `SubscribeBars`) processed before the
            // matching instrument lands would be rejected by the data engine.
            while let Ok(evt) = self.channels.data_evt_rx.try_recv() {
                Self::dispatch_data_event(evt);
                progressed = true;
                total += 1;
            }

            while let Ok(cmd) = self.channels.data_cmd_rx.try_recv() {
                Self::handle_data_command(cmd);
                progressed = true;
                total += 1;
            }

            if !progressed {
                break;
            }
        }

        if total > 0 {
            log::debug!("Flushed {total} pending data events/commands");
        }
    }

    #[cfg(feature = "node")]
    #[cfg(test)]
    pub(crate) fn drain_pending_system_events(&mut self) -> Vec<DispatchMessage<SystemEvent>> {
        self.ingress.invalidate_if_frozen();
        let mut events = Vec::new();

        while let Ok(event) = self.channels.system_evt_rx.try_recv() {
            events.push(event);
        }

        events
    }

    #[cfg(feature = "node")]
    #[cfg(test)]
    pub(crate) fn drain_pending_system_commands(&mut self) -> Vec<DispatchMessage<SystemCommand>> {
        self.ingress.invalidate_if_frozen();
        let mut commands = Vec::new();

        while let Ok(command) = self.channels.system_cmd_rx.try_recv() {
            commands.push(command);
        }

        commands
    }

    /// Runs the async runner event loop.
    ///
    /// This method processes time, system, execution, and data events in an async loop.
    /// It will run until a signal is received or the event streams are closed.
    ///
    /// # Errors
    ///
    /// Returns an error if callback dispatch fails. Pending channel messages and the failure latch
    /// are retained.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.ingress.invalidate_if_frozen();
        self.close_recovery_handoff();
        self.bind_senders();

        log::info!("AsyncRunner starting");

        loop {
            let callbacks_pending = drain_callbacks().await?;

            tokio::select! {
                biased;

                Some(()) = self.signal_rx.recv() => {
                    log::info!("AsyncRunner received signal, shutting down");
                    return Ok(());
                },
                () = std::future::ready(()), if callbacks_pending => {},
                Some(handler) = self.channels.time_evt_rx.recv() => {
                    let _ = Self::handle_time_event(handler);
                },
                Some(event) = self.channels.system_evt_rx.recv() => {
                    log::error!("System event {event} requires the LiveNode runner");
                },
                Some(command) = self.channels.system_cmd_rx.recv() => {
                    log::error!("System command {command} requires the LiveNode runner");
                },
                Some(evt) = self.channels.exec_evt_rx.recv() => {
                    Self::dispatch_exec_event(evt);
                },
                Some(cmd) = self.channels.exec_cmd_rx.recv() => {
                    Self::handle_trading_command(cmd);
                },
                Some(evt) = self.channels.data_evt_rx.recv() => {
                    Self::dispatch_data_event(evt);
                },
                Some(cmd) = self.channels.data_cmd_rx.recv() => {
                    Self::handle_data_command(cmd);
                },
                else => {
                    log::debug!("AsyncRunner all channels closed, exiting");
                    return Ok(());
                }
            };
        }
    }

    /// Handles a time event under its originating callback root.
    ///
    /// # Panics
    ///
    /// Panics if a rooted message is processed outside its owner thread.
    #[inline]
    #[must_use]
    pub fn handle_time_event(message: impl Into<DispatchMessage<TimeEventMessage>>) -> bool {
        message.into().dispatch(TimeEventMessage::dispatch)
    }

    /// Handles a data command under its captured callback root by sending to the `DataEngine`.
    ///
    /// # Panics
    ///
    /// Panics if a rooted command is processed outside its owner thread.
    #[inline]
    pub fn handle_data_command(cmd: impl Into<DispatchMessage<DataCommand>>) {
        cmd.into().dispatch(|cmd| {
            msgbus::send_data_command(MessagingSwitchboard::data_engine_execute(), cmd);
        });
    }

    /// Dispatches a received data event under its originating callback root.
    ///
    /// # Panics
    ///
    /// Panics if a rooted message is processed outside its owner thread.
    pub fn dispatch_data_event(event: DispatchMessage<DataEvent>) {
        event.dispatch(Self::handle_data_event);
    }

    /// Dispatches a received execution event under its originating callback root.
    ///
    /// # Panics
    ///
    /// Panics if a rooted message is processed outside its owner thread.
    pub fn dispatch_exec_event(event: DispatchMessage<ExecutionEvent>) {
        event.dispatch(Self::handle_exec_event);
    }

    /// Handles a data event by sending to the appropriate `DataEngine` endpoint.
    #[inline]
    pub fn handle_data_event(event: DataEvent) {
        match event {
            DataEvent::Data(data) => {
                msgbus::send_data(MessagingSwitchboard::data_engine_process_data(), data);
            }
            DataEvent::Instrument(data) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &data);
            }
            DataEvent::Response(resp) => {
                msgbus::send_data_response(MessagingSwitchboard::data_engine_response(), resp);
            }
            DataEvent::FundingRate(funding_rate) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &funding_rate);
            }
            DataEvent::InstrumentStatus(status) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &status);
            }
            DataEvent::OptionGreeks(greeks) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &greeks);
            }
            #[cfg(feature = "defi")]
            DataEvent::DeFi(data) => {
                msgbus::send_defi_data(MessagingSwitchboard::data_engine_process_defi_data(), data);
            }
            #[cfg(feature = "defi")]
            DataEvent::PoolSnapshotResponse(response) => {
                msgbus::send_any(MessagingSwitchboard::data_engine_process(), &response);
            }
            #[cfg(not(feature = "defi"))]
            #[allow(
                unreachable_patterns,
                reason = "DeFi variants can exist without this crate's defi feature"
            )]
            other => log::error!(
                "Cannot handle {other} data event, nautilus-live built without its `defi` feature"
            ),
        }
    }

    /// Dispatches an internal execution command directly to the execution engine.
    #[inline]
    pub fn handle_exec_command(cmd: TradingCommand) {
        msgbus::send_trading_command(MessagingSwitchboard::exec_engine_execute(), cmd);
    }

    /// Dispatches a trading command and its deferred children under their captured callback roots.
    ///
    /// # Panics
    ///
    /// Panics if a rooted command is processed outside its owner thread.
    #[inline]
    pub fn handle_trading_command(message: impl Into<DispatchMessage<TradingCommandMessage>>) {
        message.into().dispatch_trading(|_| {});
    }

    /// Handles an execution event by sending to the appropriate engine endpoint.
    #[inline]
    pub fn handle_exec_event(event: ExecutionEvent) {
        match event {
            ExecutionEvent::Order(order_event) => {
                msgbus::send_order_event(MessagingSwitchboard::exec_engine_process(), order_event);
            }
            ExecutionEvent::OrderSubmittedBatch(batch) => {
                for submitted in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Submitted(submitted),
                    );
                }
            }
            ExecutionEvent::OrderAcceptedBatch(batch) => {
                for accepted in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Accepted(accepted),
                    );
                }
            }
            ExecutionEvent::OrderCanceledBatch(batch) => {
                for canceled in batch {
                    msgbus::send_order_event(
                        MessagingSwitchboard::exec_engine_process(),
                        OrderEventAny::Canceled(canceled),
                    );
                }
            }
            ExecutionEvent::Report(report) => {
                Self::handle_exec_report(report);
            }
            ExecutionEvent::Account(ref account) => {
                msgbus::send_account_state(
                    MessagingSwitchboard::portfolio_update_account(),
                    account,
                );
            }
        }
    }

    #[inline]
    pub fn handle_exec_report(report: ExecutionReport) {
        let endpoint = MessagingSwitchboard::exec_engine_reconcile_execution_report();
        msgbus::send_execution_report(endpoint, report);
    }
}

#[cfg(feature = "node")]
impl AsyncRunner {
    pub(crate) fn poll_pending(&mut self, mut process: impl FnMut(PendingRunnerEvent)) -> usize {
        self.ingress.invalidate_if_frozen();
        self.bind_senders();

        let pending = (
            self.channels.time_evt_rx.len(),
            self.channels.system_evt_rx.len(),
            self.channels.system_cmd_rx.len(),
            self.channels.exec_evt_rx.len(),
            self.channels.exec_cmd_rx.len(),
            self.channels.data_evt_rx.len(),
            self.channels.data_cmd_rx.len(),
        );

        let mut processed = 0;
        processed += poll_channel(
            &mut self.channels.time_evt_rx,
            pending.0,
            PendingRunnerEvent::TimeEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.system_evt_rx,
            pending.1,
            PendingRunnerEvent::SystemEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.system_cmd_rx,
            pending.2,
            PendingRunnerEvent::SystemCommand,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.exec_evt_rx,
            pending.3,
            PendingRunnerEvent::ExecEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.exec_cmd_rx,
            pending.4,
            PendingRunnerEvent::ExecCommand,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.data_evt_rx,
            pending.5,
            PendingRunnerEvent::DataEvent,
            &mut process,
        );
        processed += poll_channel(
            &mut self.channels.data_cmd_rx,
            pending.6,
            PendingRunnerEvent::DataCommand,
            &mut process,
        );
        processed
    }

    pub(crate) async fn recv(&mut self) -> Option<PendingRunnerEvent> {
        self.ingress.invalidate_if_frozen();
        tokio::select! {
            biased;

            Some(message) = self.channels.time_evt_rx.recv() => {
                Some(PendingRunnerEvent::TimeEvent(message))
            }
            Some(event) = self.channels.system_evt_rx.recv() => {
                Some(PendingRunnerEvent::SystemEvent(event))
            }
            Some(command) = self.channels.system_cmd_rx.recv() => {
                Some(PendingRunnerEvent::SystemCommand(command))
            }
            Some(event) = self.channels.exec_evt_rx.recv() => {
                Some(PendingRunnerEvent::ExecEvent(event))
            }
            Some(command) = self.channels.exec_cmd_rx.recv() => {
                Some(PendingRunnerEvent::ExecCommand(command))
            }
            Some(event) = self.channels.data_evt_rx.recv() => {
                Some(PendingRunnerEvent::DataEvent(event))
            }
            Some(command) = self.channels.data_cmd_rx.recv() => {
                Some(PendingRunnerEvent::DataCommand(command))
            }
            else => None,
        }
    }
}

#[cfg(feature = "node")]
fn poll_channel<T>(
    receiver: &mut SnapshotReceiver<T>,
    pending: usize,
    event: impl Fn(T) -> PendingRunnerEvent,
    process: &mut impl FnMut(PendingRunnerEvent),
) -> usize {
    let mut processed = 0;

    for _ in 0..pending {
        let Ok(message) = receiver.try_recv() else {
            break;
        };

        process(event(message));
        processed += 1;
    }

    processed
}

#[cfg(test)]
mod tests {
    #[cfg(not(all(feature = "simulation", madsim)))]
    use std::num::NonZeroU64;
    use std::{cell::RefCell, rc::Rc, sync::Arc, thread, time::Duration};

    #[cfg(not(all(feature = "simulation", madsim)))]
    use nautilus_common::live::LiveTimer;
    use nautilus_common::{
        actor,
        cache::Cache,
        clock::VirtualClock,
        live::{
            dispatch::DispatchMessage,
            runner::{
                get_data_event_sender, get_exec_event_sender, get_system_command_sender,
                get_system_event_sender, try_get_system_command_sender,
                try_get_system_event_sender,
            },
        },
        messages::{
            ExecutionEvent, ExecutionReport,
            data::{SubscribeCommand, SubscribeCustomData},
            execution::{CancelAllOrders, QueryAccount, TradingCommand},
            system::{ReconnectSocket, SocketState, SocketStateChange},
        },
        msgbus::{TypedIntoHandler, stubs::get_typed_into_message_saving_handler},
        runner::{
            SyncTradingCommandSender, TimeEventMessage, drain_trading_cmd_queue,
            get_data_cmd_sender, get_time_event_sender, get_trading_cmd_sender,
            replace_exec_cmd_sender, try_get_time_event_sender, try_get_trading_cmd_sender,
        },
        timer::{TimeEvent, TimeEventCallback},
    };
    #[cfg(not(all(feature = "simulation", madsim)))]
    use nautilus_core::time::get_atomic_clock_realtime;
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_execution::engine::ExecutionEngine;
    use nautilus_model::{
        data::{Data, DataType, quote::QuoteTick},
        enums::{
            AccountType, LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSide,
            TimeInForce,
        },
        events::{
            OrderAcceptedBatch, OrderCanceledBatch, OrderEvent, OrderEventAny, OrderSubmittedBatch,
            account::state::AccountState,
            order::spec::{OrderAcceptedSpec, OrderCanceledSpec, OrderSubmittedSpec},
        },
        identifiers::{
            AccountId, ClientId, ClientOrderId, InstrumentId, PositionId, StrategyId, TradeId,
            TraderId, Venue, VenueOrderId,
        },
        reports::{FillReport, OrderStatusReport, PositionStatusReport},
        types::{Money, Price, Quantity},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;

    #[test]
    fn ingress_freeze_covers_retained_runner_clones_and_stop() {
        let runner = AsyncRunner::new();
        assert!(runner.time_evt_tx.belongs_to(&runner.ingress));
        assert!(runner.system_evt_tx.belongs_to(&runner.ingress));
        assert!(runner.system_cmd_tx.belongs_to(&runner.ingress));
        assert!(runner.exec_evt_tx.belongs_to(&runner.ingress));
        assert!(runner.exec_cmd_tx.belongs_to(&runner.ingress));
        assert!(runner.data_evt_tx.belongs_to(&runner.ingress));
        assert!(runner.data_cmd_tx.belongs_to(&runner.ingress));
        runner.bind_senders();
        let sender = get_data_event_sender();
        sender
            .send(DataEvent::Data(Data::Quote(test_quote())))
            .unwrap();
        let frozen = runner.freeze_ingress().unwrap();
        assert!(runner.verify_ingress().is_err());
        assert!(
            sender
                .send(DataEvent::Data(Data::Quote(test_quote())))
                .is_err()
        );
        assert!(frozen.finish().is_err());
        assert_eq!(runner.channels.data_evt_rx.len(), 1);
        assert!(runner.verify_ingress().is_err());

        let runner = AsyncRunner::new();
        let frozen = runner.freeze_ingress().unwrap();
        runner.handle().stop();
        assert!(frozen.finish().is_err());
        let runner = AsyncRunner::new();
        runner.stop();
        runner
            .system_evt_tx
            .send(test_system_event().into())
            .unwrap();
        assert_eq!(runner.channels.system_evt_rx.len(), 1);
    }

    #[test]
    fn ingress_freeze_is_invalidated_by_receiver_extraction() {
        let runner = AsyncRunner::new();
        let frozen = runner.freeze_ingress().unwrap();
        let _channels = runner.take_channels();
        assert!(frozen.finish().is_err());
    }

    #[cfg(feature = "node")]
    #[tokio::test]
    async fn ingress_freeze_is_invalidated_by_receiver_consumption() {
        let mut runner = AsyncRunner::new();
        runner
            .system_evt_tx
            .send(test_system_event().into())
            .unwrap();
        let frozen = runner.freeze_ingress().unwrap();
        assert!(matches!(
            runner.recv().await,
            Some(PendingRunnerEvent::SystemEvent(_))
        ));
        assert!(frozen.finish().is_err());
    }

    #[test]
    fn ingress_stop_prevents_later_snapshot_but_allows_shutdown_messages() {
        let runner = AsyncRunner::new();
        runner.handle().stop();
        assert!(runner.freeze_ingress().is_err());
        runner
            .system_evt_tx
            .send(test_system_event().into())
            .unwrap();
        assert_eq!(runner.channels.system_evt_rx.len(), 1);
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn test_runner_callback_failure_stops_before_next_command(#[case] before_run: bool) {
        actor::clear_callbacks().unwrap();
        let mut runner = AsyncRunner::new();
        let received = Rc::new(RefCell::new(Vec::new()));
        let observed = received.clone();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::risk_engine_execute(),
            TypedIntoHandler::from(move |command: TradingCommand| {
                observed.borrow_mut().push(command.ts_init());
                crate::dispatch::tests::latch_callback_failure();
            }),
        );

        let sender = AsyncTradingCommandSender::new(runner.exec_cmd_tx.clone());
        let seed_endpoint = ustr::Ustr::from("test.callback-root-seed");
        msgbus::register_trading_command_endpoint(
            seed_endpoint.into(),
            TypedIntoHandler::from(move |command: TradingCommand| {
                sender.execute(TradingCommandMessage::new(
                    MessagingSwitchboard::risk_engine_execute(),
                    command,
                ));
            }),
        );

        for timestamp in [17, 23] {
            SyncTradingCommandSender.execute(TradingCommandMessage::new(
                seed_endpoint.into(),
                TradingCommand::QueryAccount(QueryAccount::new(
                    "TRADER-001".into(),
                    None,
                    "SIM-001".into(),
                    UUID4::new(),
                    timestamp.into(),
                    None,
                    None,
                )),
            ));
        }

        drain_trading_cmd_queue();
        let first = runner.channels.exec_cmd_rx.try_recv().unwrap();
        let second = runner.channels.exec_cmd_rx.try_recv().unwrap();
        assert!(first.is_rooted());
        assert!(second.is_rooted());
        runner.exec_cmd_tx.send(first).unwrap();
        runner.exec_cmd_tx.send(second).unwrap();

        if before_run {
            crate::dispatch::tests::latch_callback_failure();
        }

        let error = runner.run().await.unwrap_err();

        assert_eq!(
            error.downcast_ref::<actor::CallbackDispatchError>(),
            Some(&actor::CallbackDispatchError::DeliveryUnwound)
        );
        assert_eq!(
            *received.borrow(),
            if before_run {
                vec![]
            } else {
                vec![UnixNanos::from(17)]
            }
        );
        assert_eq!(
            runner.channels.exec_cmd_rx.len(),
            if before_run { 2 } else { 1 }
        );
        assert!(!runner.exec_cmd_tx.is_closed());
        assert_eq!(
            actor::callback_failure(),
            Some(actor::CallbackDispatchError::DeliveryUnwound)
        );
        drop(runner);
        assert_eq!(actor::clear_callbacks(), Ok(()));
    }

    #[tokio::test]
    async fn test_runner_stop_preserves_messages_and_channel_only_scheduling() {
        let mut runner = AsyncRunner::new();
        let received = Rc::new(RefCell::new(Vec::new()));
        let stop = runner.signal_tx.clone();

        for timestamp in 1..=65 {
            let received = received.clone();
            let stop = stop.clone();
            runner
                .time_evt_tx
                .send(
                    TimeEventMessage::new(
                        TimeEvent::new(
                            "runner-resume".into(),
                            UUID4::new(),
                            timestamp.into(),
                            timestamp.into(),
                        ),
                        TimeEventCallback::RustLocal(Rc::new(move |_| {
                            received.borrow_mut().push(timestamp);
                            if timestamp == 65 {
                                stop.send(()).unwrap();
                            }
                        })),
                    )
                    .into(),
                )
                .unwrap();
        }

        stop.send(()).unwrap();
        runner.run().await.unwrap();

        assert!(received.borrow().is_empty());
        assert_eq!(runner.channels.time_evt_rx.len(), 65);
        assert!(!runner.time_evt_tx.is_closed());

        let (result, at_yield) = tokio::join!(biased;
            runner.run(),
            async { received.borrow().clone() },
        );
        result.unwrap();

        let expected: Vec<u64> = (1..=65).collect();
        assert_eq!(at_yield, expected);
        assert_eq!(*received.borrow(), expected);
        assert_eq!(runner.channels.time_evt_rx.len(), 0);
        assert!(!runner.time_evt_tx.is_closed());
    }

    // Test fixture for creating test quotes
    fn test_quote() -> QuoteTick {
        QuoteTick {
            instrument_id: InstrumentId::from("EUR/USD.SIM"),
            bid_price: Price::from("1.10000"),
            ask_price: Price::from("1.10001"),
            bid_size: Quantity::from(1_000_000),
            ask_size: Quantity::from(1_000_000),
            ts_event: UnixNanos::default(),
            ts_init: UnixNanos::default(),
        }
    }

    fn test_system_event() -> SystemEvent {
        SystemEvent::SocketState(SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            Ustr::from("binance-futures-market-streams"),
            SocketState::Connected,
        ))
    }

    fn test_system_command() -> SystemCommand {
        SystemCommand::ReconnectSocket(ReconnectSocket::new(
            TraderId::from("TRADER-001"),
            ClientId::from("POLYMARKET"),
            Ustr::from("polymarket-market-streams"),
            UnixNanos::from(3),
        ))
    }

    #[derive(Debug)]
    struct SnapshotCodec {
        calls: std::cell::Cell<usize>,
        fail_second: bool,
        panic_second: bool,
    }
    impl SnapshotCodec {
        fn assert_no_injected_panic(&self) {
            assert!(!self.panic_second, "injected codec panic");
        }
    }
    impl crate::runner_recovery::RunnerRecoveryCodec for SnapshotCodec {
        fn channel(&self) -> crate::runner_recovery::RunnerRecoveryChannel {
            crate::runner_recovery::RunnerRecoveryChannel::SystemEvent
        }
        fn codec_id(&self) -> &'static str {
            "snapshot.test.v1"
        }
        fn decode(
            &self,
            _: &crate::runner_recovery::RunnerRecoveryEnvelope,
        ) -> anyhow::Result<crate::runner_recovery::RunnerRecoveryEvent> {
            anyhow::bail!("encoding fixture only")
        }
        fn encode(
            &self,
            event: crate::runner_recovery::RunnerRecoveryEventRef<'_>,
        ) -> anyhow::Result<serde_json::Value> {
            let call = self.calls.get() + 1;
            self.calls.set(call);
            if call == 2 {
                self.assert_no_injected_panic();
                anyhow::ensure!(!self.fail_second, "injected unsupported second message");
            }
            Ok(serde_json::json!(format!("{event:?}")))
        }
    }
    fn snapshot_registry(
        fail_second: bool,
        panic_second: bool,
    ) -> crate::runner_recovery::RunnerRecoveryCodecRegistry {
        use crate::runner_recovery::{RunnerRecoveryChannel, RunnerRecoveryCodecRegistry};
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::SystemEvent]);
        registry
            .register(SnapshotCodec {
                calls: std::cell::Cell::new(0),
                fail_second,
                panic_second,
            })
            .unwrap();
        registry.seal().unwrap()
    }
    #[test]
    fn queue_snapshot_repeated_capture_and_errors_preserve_original_fifo() {
        for (fail, panic) in [(false, false), (true, false), (false, true)] {
            let mut runner = AsyncRunner::new();
            let first = test_system_event();
            let second = SystemEvent::SocketState(SocketStateChange::new(
                ClientId::from("OTHER"),
                None,
                Ustr::from("different-stream"),
                SocketState::Connected,
            ));
            runner.system_evt_tx.send(first.into()).unwrap();
            runner.system_evt_tx.send(second.into()).unwrap();
            let guard = runner.freeze_ingress().unwrap();
            let registry = snapshot_registry(fail, panic);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runner.snapshot_pending(&guard, &registry)
            }));
            if panic {
                assert!(outcome.is_err());
            } else if fail {
                assert!(outcome.unwrap().is_err());
            } else {
                let first_snapshot = outcome.unwrap().unwrap();
                let second_snapshot = runner.snapshot_pending(&guard, &registry).unwrap();
                assert_eq!(first_snapshot, second_snapshot);
                assert_eq!(first_snapshot.entries()[0].channel_ordinal, 0);
                assert_eq!(first_snapshot.entries()[1].channel_ordinal, 1);
                guard.verify().unwrap();
            }
            assert_eq!(
                runner.channels.system_evt_rx.try_recv().unwrap().message(),
                &first
            );
            assert_eq!(
                runner.channels.system_evt_rx.try_recv().unwrap().message(),
                &second
            );
            assert!(runner.channels.system_evt_rx.try_recv().is_err());
        }
    }
    #[cfg(feature = "native-tail-replay")]
    #[test]
    fn actual_native_fifo_restores_shared_official_callback_chain_and_same_source_receipts() {
        use crate::runner_recovery::RunnerRecoveryEvent;
        use nautilus_common::{live::sender::EventSender, msgbus};
        actor::clear_callbacks().unwrap();
        let mut source = AsyncRunner::new();
        let first = test_system_event();
        let second = SystemEvent::SocketState(SocketStateChange::new(
            ClientId::from("SECOND"),
            None,
            Ustr::from("source-second-stream"),
            SocketState::Connected,
        ));
        let sender = EventSender::<SystemEvent>::new(source.system_evt_tx.clone());
        let handler = msgbus::ShareableMessageHandler::from_typed(move |_: &u8| {
            sender.send(first).unwrap();
            sender.send(second).unwrap();
        });
        msgbus::subscribe_any("runner.actual.callback.ancestry".into(), handler, None);
        msgbus::publish_any("runner.actual.callback.ancestry".into(), &1_u8);
        assert!(actor::callbacks_pending());
        let guard = source.freeze_ingress().unwrap();
        let snapshot = source
            .snapshot_pending(&guard, &snapshot_registry(false, false))
            .unwrap();
        let ancestry = snapshot.callback_ancestry().unwrap().clone();
        assert_eq!(ancestry.state.entry_chains, vec![Some(0), Some(0)]);
        let receipts = ancestry
            .receipts
            .iter()
            .map(|receipt| receipt.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(receipts[0].channel_id, receipts[1].channel_id);
        assert!(receipts[0].channel_ordinal < receipts[1].channel_ordinal);
        let original = serde_json::to_vec(&snapshot).unwrap();
        guard.finish().unwrap();
        drop(source);
        assert!(!actor::callbacks_pending());
        let mut target = AsyncRunner::new();
        let frozen = target.freeze_ingress().unwrap();
        let mut bank = actor::restore_channel_contexts(&ancestry.state).unwrap();
        for (index, message) in [first, second].into_iter().enumerate() {
            target
                .install_native_retained_input(
                    &frozen,
                    RunnerRecoveryEvent::SystemEvent(message),
                    receipts[index].clone(),
                    bank.take(index).unwrap(),
                )
                .unwrap();
        }
        drop(bank);
        target
            .verify_native_callback_ancestry(&frozen, &ancestry)
            .unwrap();
        let restored = target
            .snapshot_pending(&frozen, &snapshot_registry(false, false))
            .unwrap();
        assert_eq!(serde_json::to_vec(&restored).unwrap(), original);
        assert!(actor::callbacks_pending());
        assert!(target.channels.system_evt_rx.try_recv().is_err());
        assert!(target.channels.system_evt_rx.dispatch_is_empty());
        target
            .channels
            .system_evt_rx
            .prepare_recovered_prefix_release()
            .unwrap();
        target.channels.system_evt_rx.release_recovered_prefix();
        frozen.finish().unwrap();
        for expected in [first, second] {
            let message = target.channels.system_evt_rx.try_recv().unwrap();
            assert!(message.is_rooted());
            assert_eq!(message.dispatch(|event| event), expected);
        }
        assert!(target.channels.system_evt_rx.try_recv().is_err());
        assert!(!actor::callbacks_pending());
        actor::clear_callbacks().unwrap();
    }
    #[test]
    fn queue_snapshot_rejects_timer_without_consuming_callback_or_other_queues() {
        let mut runner = AsyncRunner::new();
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_count = count.clone();
        let event = TimeEvent::new(
            Ustr::from("snapshot-timer"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        runner
            .time_evt_tx
            .send(
                TimeEventMessage::new(
                    event,
                    TimeEventCallback::from(move |_: TimeEvent| {
                        callback_count.fetch_add(1, Ordering::SeqCst);
                    }),
                )
                .into(),
            )
            .unwrap();
        runner
            .system_evt_tx
            .send(test_system_event().into())
            .unwrap();
        let guard = runner.freeze_ingress().unwrap();
        assert!(
            runner
                .snapshot_pending(&guard, &snapshot_registry(false, false))
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(
            runner
                .channels
                .time_evt_rx
                .try_recv()
                .unwrap()
                .dispatch(nautilus_common::runner::TimeEventMessage::dispatch)
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(runner.channels.time_evt_rx.try_recv().is_err());
        assert_eq!(
            runner.channels.system_evt_rx.try_recv().unwrap().message(),
            &test_system_event()
        );
    }
    #[test]
    fn queue_snapshot_refuses_foreign_or_stopped_boundary() {
        let mut runner = AsyncRunner::new();
        runner
            .system_evt_tx
            .send(test_system_event().into())
            .unwrap();
        let other = IngressGate::new();
        let foreign = other.freeze().unwrap();
        assert!(
            runner
                .snapshot_pending(&foreign, &snapshot_registry(false, false))
                .is_err()
        );
        foreign.finish().unwrap();
        let guard = runner.freeze_ingress().unwrap();
        runner.stop();
        assert!(
            runner
                .snapshot_pending(&guard, &snapshot_registry(false, false))
                .is_err()
        );
        assert_eq!(
            runner.channels.system_evt_rx.try_recv().unwrap().message(),
            &test_system_event()
        );
    }

    // Test fixture to create AsyncRunner with manual channels.
    // Sender halves are dummies (not connected to the test receivers) since
    // these tests exercise the event loop, not TLS binding.
    fn create_test_runner(
        time_events: SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
        data_events: SnapshotReceiver<DispatchMessage<DataEvent>>,
        data_commands: SnapshotReceiver<DispatchMessage<DataCommand>>,
        execution_events: SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
        execution_commands: SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
        signal_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
        signal_tx: tokio::sync::mpsc::UnboundedSender<()>,
    ) -> AsyncRunner {
        let ingress = IngressGate::new();
        let (time_evt_tx, _) = ingress.channel();
        let (system_evt_tx, system_events) = ingress.channel();
        let (system_cmd_tx, system_commands) = ingress.channel();
        let (data_evt_tx, _) = ingress.channel();
        let (data_cmd_tx, _) = ingress.channel();
        let (exec_evt_tx, _) = ingress.channel();
        let (exec_cmd_tx, _) = ingress.channel();

        AsyncRunner {
            ingress,
            channels: AsyncRunnerChannels {
                time_evt_rx: time_events,
                system_evt_rx: system_events.into(),
                system_cmd_rx: system_commands.into(),
                exec_evt_rx: execution_events,
                exec_cmd_rx: execution_commands,
                data_evt_rx: data_events,
                data_cmd_rx: data_commands,
            },
            time_evt_tx,
            system_evt_tx,
            system_cmd_tx,
            exec_evt_tx,
            exec_cmd_tx,
            data_evt_tx,
            data_cmd_tx,
            signal_rx,
            signal_tx,
            recovery_handoff_available: Arc::new(AtomicBool::new(true)),
            recovery_progress: crate::runner_recovery::RunnerRecoveryProgressHandle::new(),
        }
    }

    #[cfg(feature = "node")]
    #[rstest]
    fn test_poll_pending_processes_entry_snapshot_across_channels() {
        let (time_evt_tx, time_events) = tokio::sync::mpsc::unbounded_channel();
        let (data_evt_tx, data_events) = tokio::sync::mpsc::unbounded_channel();
        let (data_cmd_tx, data_commands) = tokio::sync::mpsc::unbounded_channel();
        let (exec_evt_tx, execution_events) = tokio::sync::mpsc::unbounded_channel();
        let (exec_cmd_tx, execution_commands) = tokio::sync::mpsc::unbounded_channel();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();

        let time_event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        time_evt_tx
            .send(
                (TimeEventMessage::new(time_event, TimeEventCallback::from(|_: TimeEvent| {})))
                    .into(),
            )
            .unwrap();
        exec_evt_tx
            .send(
                (ExecutionEvent::Order(OrderEventAny::Submitted(
                    OrderSubmittedSpec::builder()
                        .client_order_id(ClientOrderId::from("O-POLL-001"))
                        .build(),
                )))
                .into(),
            )
            .unwrap();
        exec_cmd_tx
            .send(
                TradingCommandMessage::new(
                    MessagingSwitchboard::exec_engine_execute(),
                    TradingCommand::CancelAllOrders(CancelAllOrders::new(
                        TraderId::from("TRADER-001"),
                        None,
                        StrategyId::from("S-POLL-001"),
                        InstrumentId::from("EUR/USD.SIM"),
                        Some(OrderSide::Buy),
                        UUID4::new(),
                        UnixNanos::from(3),
                        None,
                        None,
                    )),
                )
                .into(),
            )
            .unwrap();
        data_evt_tx
            .send((DataEvent::Data(Data::Quote(test_quote()))).into())
            .unwrap();
        data_cmd_tx
            .send(
                DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
                    client_id: Some(ClientId::from("POLL")),
                    venue: None,
                    data_type: DataType::new("QuoteTick", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::from(4),
                    correlation_id: None,
                    params: None,
                }))
                .into(),
            )
            .unwrap();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx,
        );
        runner.bind_senders();
        get_system_command_sender()
            .send(test_system_command())
            .unwrap();
        get_system_event_sender().send(test_system_event()).unwrap();
        get_system_event_sender().send(test_system_event()).unwrap();
        let mut processed_by_channel = [0; 7];
        let mut processed_order = Vec::new();

        let first = runner.poll_pending(|event| match event {
            PendingRunnerEvent::TimeEvent(_) => {
                processed_by_channel[0] += 1;
                processed_order.push("time");
            }
            PendingRunnerEvent::SystemEvent(_) => {
                processed_by_channel[1] += 1;
                processed_order.push("system_event");
            }
            PendingRunnerEvent::SystemCommand(_) => {
                processed_by_channel[2] += 1;
                processed_order.push("system_command");
            }
            PendingRunnerEvent::ExecEvent(_) => {
                processed_by_channel[3] += 1;
                processed_order.push("exec_event");
            }
            PendingRunnerEvent::ExecCommand(_) => {
                processed_by_channel[4] += 1;
                processed_order.push("exec_command");
            }
            PendingRunnerEvent::DataEvent(_) => {
                processed_by_channel[5] += 1;
                processed_order.push("data_event");
                data_evt_tx
                    .send((DataEvent::Data(Data::Quote(test_quote()))).into())
                    .unwrap();
            }
            PendingRunnerEvent::DataCommand(_) => {
                processed_by_channel[6] += 1;
                processed_order.push("data_command");
            }
        });

        let second = runner.poll_pending(|event| match event {
            PendingRunnerEvent::DataEvent(_) => {
                processed_by_channel[5] += 1;
                processed_order.push("data_event");
            }
            _ => panic!("Unexpected runner event"),
        });

        assert_eq!(first, 8);
        assert_eq!(second, 1);
        assert_eq!(processed_by_channel, [1, 2, 1, 1, 1, 2, 1]);
        assert_eq!(
            processed_order,
            [
                "time",
                "system_event",
                "system_event",
                "system_command",
                "exec_event",
                "exec_command",
                "data_event",
                "data_command",
                "data_event",
            ]
        );
    }

    #[cfg(feature = "node")]
    #[tokio::test]
    async fn test_recv_processes_system_event_before_command() {
        let (_time_evt_tx, time_events) = tokio::sync::mpsc::unbounded_channel();
        let (_data_evt_tx, data_events) = tokio::sync::mpsc::unbounded_channel();
        let (_data_cmd_tx, data_commands) = tokio::sync::mpsc::unbounded_channel();
        let (_exec_evt_tx, execution_events) = tokio::sync::mpsc::unbounded_channel();
        let (_exec_cmd_tx, execution_commands) = tokio::sync::mpsc::unbounded_channel();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx,
        );

        runner
            .system_cmd_tx
            .send((test_system_command()).into())
            .unwrap();
        runner
            .system_evt_tx
            .send((test_system_event()).into())
            .unwrap();

        assert!(matches!(
            runner.recv().await,
            Some(PendingRunnerEvent::SystemEvent(_))
        ));
        assert!(matches!(
            runner.recv().await,
            Some(PendingRunnerEvent::SystemCommand(_))
        ));
    }

    #[rstest]
    fn test_async_data_command_sender_creation() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let sender = AsyncDataCommandSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncDataCommandSender"));
    }

    #[cfg(feature = "node")]
    #[rstest]
    fn test_data_command_sender_shutdown_logging() {
        struct ErrorCapture(std::sync::Mutex<Vec<String>>);

        impl log::Log for ErrorCapture {
            fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
                metadata.level() == log::Level::Error
                    && metadata.target() == "nautilus_live::runner"
            }

            fn log(&self, record: &log::Record<'_>) {
                if self.enabled(record.metadata()) {
                    self.0.lock().unwrap().push(record.args().to_string());
                }
            }

            fn flush(&self) {}
        }

        static ERRORS: ErrorCapture = ErrorCapture(std::sync::Mutex::new(Vec::new()));
        log::set_logger(&ERRORS).unwrap();
        log::set_max_level(log::LevelFilter::Error);

        let runner = AsyncRunner::new();
        let handle = LiveNodeHandle::new();
        handle.set_starting();
        runner.bind_senders_for_node(handle.clone());
        let sender = get_data_cmd_sender();
        let mut channels = runner.take_channels();

        let command = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("TEST")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }));

        // Stopping and final draining must still deliver commands while the receiver is alive
        for stopped in [false, true] {
            if stopped {
                handle.set_stopped();
            } else {
                handle.set_shutting_down();
            }

            sender.execute(command.clone());
            assert_eq!(
                channels
                    .data_cmd_rx
                    .try_recv()
                    .unwrap()
                    .dispatch(|command| command),
                command
            );
        }

        drop(channels);
        sender.execute(command.clone());
        assert_eq!(*ERRORS.0.lock().unwrap(), Vec::<String>::new());

        // A stopped previous node must not hide an unexpected closure in its replacement
        let runner = AsyncRunner::new();
        let handle = LiveNodeHandle::new();
        runner.bind_senders_for_node(handle.clone());
        let sender = get_data_cmd_sender();
        drop(runner);

        for shutting_down in [false, true] {
            if shutting_down {
                handle.set_shutting_down();
            } else {
                handle.set_starting();
            }

            sender.execute(command.clone());
        }

        assert_eq!(
            *ERRORS.0.lock().unwrap(),
            vec!["Failed to send data command: channel closed"; 2],
        );

        ERRORS.0.lock().unwrap().clear();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        drop(rx);
        AsyncDataCommandSender::new(tx).execute(command);
        assert_eq!(
            *ERRORS.0.lock().unwrap(),
            vec!["Failed to send data command: channel closed"],
        );
    }

    #[rstest]
    fn test_async_time_event_sender_creation() {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let sender = AsyncTimeEventSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncTimeEventSender"));
    }

    #[tokio::test]
    async fn test_async_data_command_sender_execute() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let sender = AsyncDataCommandSender::new(tx);

        let command = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("TEST")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }));

        sender.execute(command.clone());

        let received = rx.recv().await.unwrap().dispatch(|command| command);
        match (received, command) {
            (
                DataCommand::Subscribe(SubscribeCommand::Data(r)),
                DataCommand::Subscribe(SubscribeCommand::Data(c)),
            ) => {
                assert_eq!(r.client_id, c.client_id);
                assert_eq!(r.data_type, c.data_type);
            }
            _ => panic!("Command mismatch"),
        }
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_async_command_senders_capture_owner_root(#[case] rooted: bool) {
        let runner = AsyncRunner::new();
        runner.bind_senders();
        let mut channels = runner.take_channels();

        let data = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("ROOT")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::from(7),
            correlation_id: None,
            params: None,
        }));

        let trading = TradingCommand::CancelAllOrders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("ROOT-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(OrderSide::Sell),
            UUID4::new(),
            UnixNanos::from(11),
            None,
            None,
        ));
        let expected_data = data.clone();
        let expected_trading = trading.clone();
        let trigger = trading.clone();

        let send = move || {
            get_data_cmd_sender().execute(data.clone());
            get_trading_cmd_sender().execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                trading.clone(),
            ));
        };

        if rooted {
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(move |_| send()),
            );
            SyncTradingCommandSender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                trigger,
            ));
            drain_trading_cmd_queue();
        } else {
            send();
        }

        let data = channels.data_cmd_rx.try_recv().unwrap();
        let trading = channels.exec_cmd_rx.try_recv().unwrap();

        let results = thread::spawn(move || {
            let data = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                data.dispatch(|command| command)
            }));

            let trading = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                trading.dispatch(|message| (message.endpoint(), message.command().clone()))
            }));

            (data, trading)
        })
        .join()
        .unwrap();

        if rooted {
            for error in [results.0.unwrap_err(), results.1.unwrap_err()] {
                let message = error.downcast_ref::<String>().unwrap();
                assert!(message.contains("command context dispatched outside its owner thread"));
            }
        } else {
            assert_eq!(results.0.unwrap(), expected_data);
            assert_eq!(
                results.1.unwrap(),
                (
                    MessagingSwitchboard::exec_engine_execute(),
                    expected_trading
                ),
            );
        }

        assert!(channels.data_cmd_rx.is_empty());
        assert!(channels.exec_cmd_rx.is_empty());
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_event_senders_capture_owner_root(
        #[case] rooted: bool,
        #[values(false, true)] dispatch: bool,
    ) {
        let runner = AsyncRunner::new();
        runner.bind_senders();
        let mut channels = runner.take_channels();
        let expected_quote = test_quote();
        let expected_order = OrderSubmittedSpec::builder()
            .client_order_id(ClientOrderId::from("EVENT-017"))
            .build();
        let order = expected_order.clone();

        let send = move || {
            get_data_event_sender()
                .send(DataEvent::Data(Data::Quote(expected_quote)))
                .unwrap();
            get_exec_event_sender()
                .send(ExecutionEvent::Order(OrderEventAny::Submitted(
                    order.clone(),
                )))
                .unwrap();
        };

        if rooted {
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(move |_| send()),
            );
            SyncTradingCommandSender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::QueryAccount(QueryAccount::new(
                    "TRADER-001".into(),
                    None,
                    "SIM-001".into(),
                    UUID4::new(),
                    19.into(),
                    None,
                    None,
                )),
            ));
            drain_trading_cmd_queue();
        } else {
            send();
        }

        if dispatch {
            msgbus::register_data_endpoint(
                MessagingSwitchboard::data_engine_process_data(),
                TypedIntoHandler::from(move |data| {
                    assert_eq!(data, Data::Quote(expected_quote));
                    get_data_event_sender().send(DataEvent::Data(data)).unwrap();
                }),
            );

            let order = expected_order.clone();
            msgbus::register_order_event_endpoint(
                MessagingSwitchboard::exec_engine_process(),
                TypedIntoHandler::from(move |event| {
                    assert_eq!(event, OrderEventAny::Submitted(order.clone()));
                    get_exec_event_sender()
                        .send(ExecutionEvent::Order(event))
                        .unwrap();
                }),
            );

            AsyncRunner::dispatch_data_event(channels.data_evt_rx.try_recv().unwrap());
            AsyncRunner::dispatch_exec_event(channels.exec_evt_rx.try_recv().unwrap());
        }

        let data = channels.data_evt_rx.try_recv().unwrap();
        let exec = channels.exec_evt_rx.try_recv().unwrap();

        let (data, exec) = thread::spawn(move || {
            (
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    data.dispatch(|event| event)
                })),
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    exec.dispatch(|event| event)
                })),
            )
        })
        .join()
        .unwrap();

        if rooted || dispatch {
            for error in [data.unwrap_err(), exec.unwrap_err()] {
                assert!(
                    error
                        .downcast_ref::<String>()
                        .unwrap()
                        .contains("command context dispatched outside its owner thread")
                );
            }
        } else {
            let DataEvent::Data(Data::Quote(quote)) = data.unwrap() else {
                panic!("expected quote")
            };

            let ExecutionEvent::Order(OrderEventAny::Submitted(order)) = exec.unwrap() else {
                panic!("expected submitted order")
            };

            assert_eq!(quote, expected_quote);
            assert_eq!(order, expected_order);
        }

        assert!(channels.data_evt_rx.is_empty());
        assert!(channels.exec_evt_rx.is_empty());
    }

    #[rstest]
    fn test_system_and_time_senders_capture_owner_root(#[values(false, true)] rooted: bool) {
        let runner = AsyncRunner::new();
        runner.bind_senders();
        let mut channels = runner.take_channels();
        let event = TimeEvent::new("system-time".into(), UUID4::new(), 17.into(), 23.into());
        let observed = Rc::new(RefCell::new(Vec::new()));
        let received = observed.clone();
        let expected_event = event.clone();

        let callback = TimeEventCallback::RustLocal(Rc::new(move |event| {
            received.borrow_mut().push(event);
            get_system_command_sender()
                .send(test_system_command())
                .unwrap();
        }));

        let send = move || {
            get_system_event_sender().send(test_system_event()).unwrap();
            get_system_command_sender()
                .send(test_system_command())
                .unwrap();
            get_time_event_sender().send(TimeEventMessage::new(event.clone(), callback.clone()));
        };

        if rooted {
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(move |_| send()),
            );
            SyncTradingCommandSender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::QueryAccount(QueryAccount::new(
                    "TRADER-001".into(),
                    None,
                    "SIM-001".into(),
                    UUID4::new(),
                    31.into(),
                    None,
                    None,
                )),
            ));
            drain_trading_cmd_queue();
        } else {
            send();
        }

        let system_event = channels.system_evt_rx.try_recv().unwrap();
        let system_command = channels.system_cmd_rx.try_recv().unwrap();
        let time_event = channels.time_evt_rx.try_recv().unwrap();
        assert_eq!(system_event.is_rooted(), rooted);
        assert_eq!(system_command.is_rooted(), rooted);
        assert_eq!(time_event.is_rooted(), rooted);
        assert!(AsyncRunner::handle_time_event(time_event));
        let child = channels.system_cmd_rx.try_recv().unwrap();

        assert_eq!(system_event.dispatch(|event| event), test_system_event());
        assert_eq!(
            system_command.dispatch(|command| command),
            test_system_command()
        );
        assert_eq!(*observed.borrow(), vec![expected_event]);
        assert!(child.is_rooted());
        assert_eq!(child.dispatch(|command| command), test_system_command());
        assert!(channels.time_evt_rx.is_empty());
        assert!(channels.system_evt_rx.is_empty());
        assert!(channels.system_cmd_rx.is_empty());
    }

    #[cfg(not(all(feature = "simulation", madsim)))]
    #[tokio::test]
    async fn test_live_timer_started_under_root_emits_independent_event() {
        let runner = AsyncRunner::new();
        runner.bind_senders();
        let mut channels = runner.take_channels();
        let now = get_atomic_clock_realtime().get_time_ns();
        let name = Ustr::from("ROOTED_TIMER");
        let owner = thread::current().id();
        let observed = Rc::new(RefCell::new(Vec::new()));
        let received = observed.clone();

        let callback = TimeEventCallback::RustLocal(Rc::new(move |event| {
            received
                .borrow_mut()
                .push((event.name, event.ts_event, thread::current().id()));
        }));

        let timer = Rc::new(RefCell::new(None));
        let started = timer.clone();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::risk_engine_execute(),
            TypedIntoHandler::from(move |_| {
                get_system_command_sender()
                    .send(test_system_command())
                    .unwrap();

                let mut timer = LiveTimer::new(
                    name,
                    NonZeroU64::new(1_000_000).unwrap(),
                    now,
                    Some(now),
                    callback.clone(),
                    true,
                    Some(get_time_event_sender()),
                );
                timer.start();
                *started.borrow_mut() = Some(timer);
            }),
        );

        SyncTradingCommandSender.execute(TradingCommandMessage::new(
            MessagingSwitchboard::risk_engine_execute(),
            TradingCommand::QueryAccount(QueryAccount::new(
                "TRADER-001".into(),
                None,
                "SIM-001".into(),
                UUID4::new(),
                31.into(),
                None,
                None,
            )),
        ));
        drain_trading_cmd_queue();
        let witness = channels.system_cmd_rx.try_recv().unwrap();
        let message = tokio::time::timeout(Duration::from_secs(2), channels.time_evt_rx.recv())
            .await
            .unwrap()
            .unwrap();
        timer.borrow_mut().take().unwrap().cancel();

        assert!(witness.is_rooted());
        assert!(!message.is_rooted());
        assert!(observed.borrow().is_empty());
        assert!(AsyncRunner::handle_time_event(message));
        assert_eq!(*observed.borrow(), vec![(name, now, owner)]);
        assert!(channels.time_evt_rx.is_empty());
    }

    #[tokio::test]
    async fn test_async_time_event_sender_send() {
        let (tx, mut rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let sender = AsyncTimeEventSender::new(tx);

        let event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let callback = TimeEventCallback::from(|_: TimeEvent| {});
        let message = TimeEventMessage::new(event, callback);

        sender.send(message);

        assert!(rx.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_runner_shutdown_signal() {
        // Create runner with manual channels to avoid global state
        let (_data_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        // Start runner
        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Send shutdown signal
        signal_tx.send(()).unwrap();

        // Runner should stop quickly
        let result = tokio::time::timeout(Duration::from_millis(100), runner_handle).await;
        assert!(result.is_ok(), "Runner should stop on signal");
    }

    #[tokio::test]
    async fn test_runner_closes_on_channel_drop() {
        let (data_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        // Start runner
        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        drop(data_tx);

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.send(()).ok();

        // Runner should stop when channels close or on signal
        let result = tokio::time::timeout(Duration::from_millis(200), runner_handle).await;
        assert!(
            result.is_ok(),
            "Runner should stop when channels close or on signal"
        );
    }

    #[tokio::test]
    async fn test_concurrent_event_sending() {
        let (data_evt_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_data_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_evt_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        // Setup runner
        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        // Spawn multiple concurrent senders
        let mut handles = vec![];

        for _ in 0..5 {
            let tx_clone = data_evt_tx.clone();

            let handle = tokio::spawn(async move {
                for _ in 0..20 {
                    let quote = test_quote();
                    tx_clone
                        .send(DataEvent::Data(Data::Quote(quote)).into())
                        .unwrap();
                    tokio::task::yield_now().await;
                }
            });

            handles.push(handle);
        }

        // Start runner in background
        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Wait for all senders
        for handle in handles {
            handle.await.unwrap();
        }

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.send(()).unwrap();

        let _ = tokio::time::timeout(Duration::from_millis(200), runner_handle).await;
    }

    #[rstest]
    #[case(10)]
    #[case(100)]
    #[case(1000)]
    fn test_channel_send_performance(#[case] count: usize) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
        let quote = test_quote();

        // Send events
        for _ in 0..count {
            tx.send(DataEvent::Data(Data::Quote(quote))).unwrap();
        }

        // Verify all received
        let mut received = 0;

        while rx.try_recv().is_ok() {
            received += 1;
        }

        assert_eq!(received, count);
    }

    #[rstest]
    fn test_async_trading_command_sender_creation() {
        let (tx, _rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let sender = AsyncTradingCommandSender::new(tx);
        assert!(format!("{sender:?}").contains("AsyncTradingCommandSender"));
    }

    #[rstest]
    fn test_async_trading_command_sender_preserves_target_endpoints() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let (risk_handler, risk_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "RiskEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                risk_handler,
            );
            let (exec_handler, exec_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "ExecEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::exec_engine_execute(),
                exec_handler,
            );

            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
            let sender = AsyncTradingCommandSender::new(tx);
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("RISK-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    Some(OrderSide::Buy),
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("EXEC-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    Some(OrderSide::Sell),
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());
            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            let risk_commands = risk_saving_handler.get_messages();
            let exec_commands = exec_saving_handler.get_messages();
            assert!(rx.try_recv().is_err());
            assert_eq!(risk_commands.len(), 1);
            assert_eq!(
                risk_commands[0].strategy_id(),
                Some(StrategyId::from("RISK-001"))
            );
            assert_eq!(exec_commands.len(), 1);
            assert_eq!(
                exec_commands[0].strategy_id(),
                Some(StrategyId::from("EXEC-001"))
            );
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_async_runner_preserves_deferred_follow_up_order() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let clock = Rc::new(RefCell::new(VirtualClock::new()));
            let cache = Rc::new(RefCell::new(Cache::default()));
            let exec_engine = Rc::new(RefCell::new(ExecutionEngine::new(clock, cache, None)));
            ExecutionEngine::register_msgbus_handlers(&exec_engine);
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(|command: TradingCommand| {
                    msgbus::send_trading_command(
                        MessagingSwitchboard::exec_engine_queue_execute(),
                        command,
                    );
                }),
            );

            let (exec_handler, exec_saving_handler) =
                get_typed_into_message_saving_handler::<TradingCommand>(Some(Ustr::from(
                    "ExecEngine.execute",
                )));
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::exec_engine_execute(),
                exec_handler,
            );

            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
            let sender = Arc::new(AsyncTradingCommandSender::new(tx));
            replace_exec_cmd_sender(sender.clone());
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::risk_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("FIRST-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    Some(OrderSide::Buy),
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));
            sender.execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("SECOND-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    Some(OrderSide::Sell),
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                )),
            ));

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());
            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            let commands = exec_saving_handler.get_messages();
            let strategy_ids = commands
                .iter()
                .map(TradingCommand::strategy_id)
                .collect::<Vec<_>>();
            assert!(rx.try_recv().is_err());
            assert_eq!(commands.len(), 2);
            assert_eq!(
                strategy_ids,
                vec![
                    Some(StrategyId::from("FIRST-001")),
                    Some(StrategyId::from("SECOND-001"))
                ]
            );
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_async_runner_dispatches_deferred_exec_command_once() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let clock = Rc::new(RefCell::new(VirtualClock::new()));
            let cache = Rc::new(RefCell::new(Cache::default()));
            let exec_engine = Rc::new(RefCell::new(ExecutionEngine::new(clock, cache, None)));
            ExecutionEngine::register_msgbus_handlers(&exec_engine);

            let (tx, mut rx) =
                tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
            replace_exec_cmd_sender(Arc::new(AsyncTradingCommandSender::new(tx)));
            let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
                TraderId::from("TRADER-001"),
                None,
                StrategyId::from("EXEC-001"),
                InstrumentId::from("EUR/USD.SIM"),
                Some(OrderSide::Buy),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            ));

            msgbus::send_trading_command(
                MessagingSwitchboard::exec_engine_queue_execute(),
                command,
            );
            assert_eq!(exec_engine.borrow().command_count(), 0);

            AsyncRunner::handle_trading_command(rx.try_recv().unwrap());

            assert!(rx.try_recv().is_err());
            assert_eq!(exec_engine.borrow().command_count(), 1);
        })
        .join()
        .unwrap();
    }

    #[tokio::test]
    async fn test_runner_processes_trading_commands() {
        let (_data_evt_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_data_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_evt_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
            TraderId::from("TRADER-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(OrderSide::Buy),
            UUID4::new(),
            UnixNanos::default(),
            None,
            None, // correlation_id
        ));
        exec_cmd_tx
            .send(
                TradingCommandMessage::new(MessagingSwitchboard::exec_engine_execute(), command)
                    .into(),
            )
            .unwrap();

        tokio::task::yield_now().await;
        signal_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner_handle).await;
        assert!(result.is_ok(), "Runner should process command and stop");
    }

    #[tokio::test]
    async fn test_runner_processes_multiple_trading_commands() {
        let (_data_evt_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_data_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_evt_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        for i in 0..10 {
            let strategy_id = format!("S-{i:03}");
            let command = TradingCommand::CancelAllOrders(CancelAllOrders::new(
                TraderId::from("TRADER-001"),
                None,
                StrategyId::from(strategy_id.as_str()),
                InstrumentId::from("EUR/USD.SIM"),
                Some(OrderSide::Buy),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None, // correlation_id
            ));
            exec_cmd_tx
                .send(
                    TradingCommandMessage::new(
                        MessagingSwitchboard::exec_engine_execute(),
                        command,
                    )
                    .into(),
                )
                .unwrap();
        }

        tokio::task::yield_now().await;
        signal_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner_handle).await;
        assert!(
            result.is_ok(),
            "Runner should process all commands and stop"
        );
    }

    #[tokio::test]
    async fn test_execution_event_order_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let event = OrderSubmittedSpec::builder()
            .client_order_id(ClientOrderId::from("O-001"))
            .build();

        tx.send(ExecutionEvent::Order(OrderEventAny::Submitted(event)))
            .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Order(OrderEventAny::Submitted(e)) => {
                assert_eq!(e.client_order_id(), ClientOrderId::from("O-001"));
            }
            _ => panic!("Expected OrderSubmitted event"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_order_status_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let report = OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(ClientOrderId::from("O-001")),
            VenueOrderId::from("V-001"),
            OrderSide::Buy.into(),
            OrderType::Market,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from(100_000),
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            None,
        );

        tx.send(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Order(r)) => {
                assert_eq!(r.venue_order_id.as_str(), "V-001");
                assert_eq!(r.order_status, OrderStatus::Accepted);
            }
            _ => panic!("Expected OrderStatusReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_fill() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let report = FillReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            VenueOrderId::from("V-001"),
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from(100_000),
            Price::from("1.10000"),
            Money::from("10 USD"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-001")),
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );

        tx.send(ExecutionEvent::Report(ExecutionReport::Fill(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Fill(r)) => {
                assert_eq!(r.venue_order_id.as_str(), "V-001");
                assert_eq!(r.trade_id.to_string(), "T-001");
            }
            _ => panic!("Expected FillReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_report_position() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let report = PositionStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            PositionSide::Long,
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
            Some(PositionId::from("P-001")),
            None,
        );

        tx.send(ExecutionEvent::Report(ExecutionReport::Position(Box::new(
            report,
        ))))
        .unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Report(ExecutionReport::Position(r)) => {
                assert_eq!(r.venue_position_id.unwrap().as_str(), "P-001");
            }
            _ => panic!("Expected PositionStatusReport"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_account() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let account_state = AccountState::new(
            AccountId::from("SIM-001"),
            AccountType::Cash,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );

        tx.send(ExecutionEvent::Account(account_state)).unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::Account(r) => {
                assert_eq!(r.account_id.as_str(), "SIM-001");
            }
            _ => panic!("Expected AccountState"),
        }
    }

    #[tokio::test]
    async fn test_runner_stop_method() {
        let (_data_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Use stop via signal_tx directly
        signal_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(100), runner_handle).await;
        assert!(result.is_ok(), "Runner should stop when stop() is called");
    }

    #[tokio::test]
    async fn test_all_event_types_integration() {
        let (data_evt_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (data_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (time_evt_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Send data event
        let quote = test_quote();
        data_evt_tx
            .send((DataEvent::Data(Data::Quote(quote))).into())
            .unwrap();

        // Send data command
        let command = DataCommand::Subscribe(SubscribeCommand::Data(SubscribeCustomData {
            client_id: Some(ClientId::from("TEST")),
            venue: None,
            data_type: DataType::new("QuoteTick", None, None),
            command_id: UUID4::new(),
            ts_init: UnixNanos::default(),
            correlation_id: None,
            params: None,
        }));

        data_cmd_tx.send(command.into()).unwrap();

        // Send time event
        let event = TimeEvent::new(
            Ustr::from("test"),
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
        );
        let callback = TimeEventCallback::from(|_: TimeEvent| {});
        let message = TimeEventMessage::new(event, callback);
        time_evt_tx.send((message).into()).unwrap();

        // Send execution order event
        let order_event = OrderSubmittedSpec::builder()
            .client_order_id(ClientOrderId::from("O-001"))
            .build();
        exec_evt_tx
            .send((ExecutionEvent::Order(OrderEventAny::Submitted(order_event))).into())
            .unwrap();

        // Send execution report (OrderStatus)
        let order_status = OrderStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            Some(ClientOrderId::from("O-001")),
            VenueOrderId::from("V-001"),
            OrderSide::Buy.into(),
            OrderType::Market,
            TimeInForce::Gtc,
            OrderStatus::Accepted,
            Quantity::from(100_000),
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            UnixNanos::from(3),
            None,
        );
        exec_evt_tx
            .send((ExecutionEvent::Report(ExecutionReport::Order(Box::new(order_status)))).into())
            .unwrap();

        // Send execution report (Fill)
        let fill = FillReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            VenueOrderId::from("V-001"),
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from(100_000),
            Price::from("1.10000"),
            Money::from("10 USD"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-001")),
            None,
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );
        exec_evt_tx
            .send((ExecutionEvent::Report(ExecutionReport::Fill(Box::new(fill)))).into())
            .unwrap();

        // Send execution report (Position)
        let position = PositionStatusReport::new(
            AccountId::from("SIM-001"),
            InstrumentId::from("EUR/USD.SIM"),
            PositionSide::Long,
            Quantity::from(100_000),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
            Some(PositionId::from("P-001")),
            None,
        );
        exec_evt_tx
            .send((ExecutionEvent::Report(ExecutionReport::Position(Box::new(position)))).into())
            .unwrap();

        // Send account event
        let account_state = AccountState::new(
            AccountId::from("SIM-001"),
            AccountType::Cash,
            vec![],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::from(1),
            UnixNanos::from(2),
            None,
        );
        exec_evt_tx
            .send((ExecutionEvent::Account(account_state)).into())
            .unwrap();

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        signal_tx.send(()).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), runner_handle).await;
        assert!(
            result.is_ok(),
            "Runner should process all event types and stop cleanly"
        );
    }

    #[tokio::test]
    async fn test_runner_handle_stops_runner() {
        let (_data_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        // Get handle before moving runner
        let handle = runner.handle();

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Use handle to stop
        handle.stop();

        let result = tokio::time::timeout(Duration::from_millis(100), runner_handle).await;
        assert!(result.is_ok(), "Runner should stop via handle");
    }

    #[tokio::test]
    async fn test_runner_handle_is_cloneable() {
        let (signal_tx, _signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let handle = AsyncRunnerHandle {
            signal_tx,
            ingress: IngressGate::new(),
        };

        let handle2 = handle.clone();

        // Both handles should be able to send stop signals
        assert!(handle.signal_tx.send(()).is_ok());
        assert!(handle2.signal_tx.send(()).is_ok());
    }

    #[tokio::test]
    async fn test_runner_processes_events_before_stop() {
        let (data_evt_tx, data_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let (_cmd_tx, data_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let (_time_tx, time_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let (_exec_evt_tx, execution_events) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let (_exec_cmd_tx, execution_commands) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let (signal_tx, signal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

        let mut runner = create_test_runner(
            time_events.into(),
            data_events.into(),
            data_commands.into(),
            execution_events.into(),
            execution_commands.into(),
            signal_rx,
            signal_tx.clone(),
        );

        let handle = runner.handle();

        // Send events before starting runner
        for _ in 0..10 {
            let quote = test_quote();
            data_evt_tx
                .send((DataEvent::Data(Data::Quote(quote))).into())
                .unwrap();
        }

        let runner_handle = tokio::spawn(async move {
            runner.run().await.unwrap();
        });

        // Yield to let runner enter event loop before stop signal
        tokio::task::yield_now().await;
        handle.stop();

        let result = tokio::time::timeout(Duration::from_millis(200), runner_handle).await;
        assert!(result.is_ok(), "Runner should process events and stop");
    }

    #[rstest]
    fn test_new_does_not_bind_tls() {
        std::thread::spawn(|| {
            let _runner = AsyncRunner::new();
            assert!(try_get_time_event_sender().is_none());
            assert!(try_get_system_command_sender().is_none());
            assert!(try_get_system_event_sender().is_none());
            assert!(try_get_trading_cmd_sender().is_none());
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_bind_senders_routes_to_runner_channels() {
        std::thread::spawn(|| {
            let mut runner = AsyncRunner::new();
            runner.bind_senders();

            get_data_cmd_sender().execute(DataCommand::Subscribe(SubscribeCommand::Data(
                SubscribeCustomData {
                    client_id: Some(ClientId::from("TEST")),
                    venue: None,
                    data_type: DataType::new("test", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::default(),
                    correlation_id: None,
                    params: None,
                },
            )));

            assert!(runner.channels.data_cmd_rx.try_recv().is_ok());

            get_trading_cmd_sender().execute(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                TradingCommand::CancelAllOrders(CancelAllOrders::new(
                    TraderId::from("TRADER-001"),
                    None,
                    StrategyId::from("S-001"),
                    InstrumentId::from("EUR/USD.SIM"),
                    Some(OrderSide::Buy),
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None, // correlation_id
                )),
            ));
            assert!(runner.channels.exec_cmd_rx.try_recv().is_ok());

            let event = TimeEvent::new(
                Ustr::from("test"),
                UUID4::new(),
                UnixNanos::from(1),
                UnixNanos::from(2),
            );
            let callback = TimeEventCallback::from(|_: TimeEvent| {});
            get_time_event_sender().send(TimeEventMessage::new(event, callback));
            assert!(runner.channels.time_evt_rx.try_recv().is_ok());

            get_system_event_sender().send(test_system_event()).unwrap();
            assert_eq!(
                runner
                    .channels
                    .system_evt_rx
                    .try_recv()
                    .unwrap()
                    .dispatch(|value| value),
                test_system_event()
            );

            get_system_command_sender()
                .send(test_system_command())
                .unwrap();
            assert_eq!(
                runner
                    .channels
                    .system_cmd_rx
                    .try_recv()
                    .unwrap()
                    .dispatch(|value| value),
                test_system_command()
            );

            get_data_event_sender()
                .send(DataEvent::Data(Data::Quote(test_quote())))
                .unwrap();
            assert!(runner.channels.data_evt_rx.try_recv().is_ok());

            let account = AccountState::new(
                AccountId::from("SIM-001"),
                AccountType::Cash,
                vec![],
                vec![],
                true,
                UUID4::new(),
                UnixNanos::from(1),
                UnixNanos::from(2),
                None,
            );
            get_exec_event_sender()
                .send(ExecutionEvent::Account(account))
                .unwrap();
            assert!(runner.channels.exec_evt_rx.try_recv().is_ok());
        })
        .join()
        .unwrap();
    }

    #[cfg(feature = "node")]
    #[rstest]
    fn test_drain_pending_system_events_keeps_data_events_separate() {
        std::thread::spawn(|| {
            let mut runner = AsyncRunner::new();
            runner.bind_senders();
            let system_event = test_system_event();

            get_system_event_sender().send(system_event).unwrap();
            get_data_event_sender()
                .send(DataEvent::Data(Data::Quote(test_quote())))
                .unwrap();

            let system_events = runner
                .drain_pending_system_events()
                .into_iter()
                .map(|message| message.dispatch(|value| value))
                .collect::<Vec<_>>();

            assert_eq!(system_events, vec![system_event]);
            assert!(runner.channels.system_evt_rx.try_recv().is_err());
            assert!(runner.channels.data_evt_rx.try_recv().is_ok());
        })
        .join()
        .unwrap();
    }

    #[cfg(feature = "node")]
    #[rstest]
    fn test_drain_pending_system_commands_keeps_events_separate() {
        std::thread::spawn(|| {
            let mut runner = AsyncRunner::new();
            runner.bind_senders();
            let system_command = test_system_command();

            get_system_command_sender().send(system_command).unwrap();
            get_system_event_sender().send(test_system_event()).unwrap();

            let system_commands = runner
                .drain_pending_system_commands()
                .into_iter()
                .map(|message| message.dispatch(|value| value))
                .collect::<Vec<_>>();

            assert_eq!(system_commands, vec![system_command]);
            assert!(runner.channels.system_cmd_rx.try_recv().is_err());
            assert!(runner.channels.system_evt_rx.try_recv().is_ok());
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_bind_senders_reclaims_tls_from_previous_runner() {
        std::thread::spawn(|| {
            let mut runner1 = AsyncRunner::new();
            runner1.bind_senders();

            let mut runner2 = AsyncRunner::new();
            runner2.bind_senders();

            get_data_cmd_sender().execute(DataCommand::Subscribe(SubscribeCommand::Data(
                SubscribeCustomData {
                    client_id: Some(ClientId::from("TEST")),
                    venue: None,
                    data_type: DataType::new("test", None, None),
                    command_id: UUID4::new(),
                    ts_init: UnixNanos::default(),
                    correlation_id: None,
                    params: None,
                },
            )));

            assert!(runner2.channels.data_cmd_rx.try_recv().is_ok());
            assert!(runner1.channels.data_cmd_rx.try_recv().is_err());
        })
        .join()
        .unwrap();
    }

    #[tokio::test]
    async fn test_execution_event_order_submitted_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let events = vec![
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderSubmittedBatch::new(events);
        tx.send(ExecutionEvent::OrderSubmittedBatch(batch)).unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderSubmittedBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderSubmittedBatch event"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_order_accepted_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let events = vec![
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderAcceptedBatch::new(events);
        tx.send(ExecutionEvent::OrderAcceptedBatch(batch)).unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderAcceptedBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderAcceptedBatch event"),
        }
    }

    #[tokio::test]
    async fn test_execution_event_order_canceled_batch_channel() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ExecutionEvent>();

        let events = vec![
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        let batch = OrderCanceledBatch::new(events);
        tx.send(ExecutionEvent::OrderCanceledBatch(batch)).unwrap();

        let received = rx.recv().await.unwrap();
        match received {
            ExecutionEvent::OrderCanceledBatch(b) => {
                assert_eq!(b.len(), 2);
                assert_eq!(b.events[0].client_order_id, ClientOrderId::from("O-001"));
                assert_eq!(b.events[1].client_order_id, ClientOrderId::from("O-002"));
            }
            _ => panic!("Expected OrderCanceledBatch event"),
        }
    }

    #[cfg(feature = "defi")]
    #[rstest]
    fn test_handle_data_event_routes_pool_snapshot_response_to_data_engine() {
        use nautilus_common::{
            messages::defi::PoolSnapshotResponse, msgbus::ShareableMessageHandler,
        };
        use nautilus_model::defi::{
            data::block::BlockPosition,
            pool_analysis::snapshot::{PoolAnalytics, PoolSnapshot, PoolState},
        };

        let received = Rc::new(RefCell::new(Vec::new()));
        let sink = received.clone();
        msgbus::register_any(
            MessagingSwitchboard::data_engine_process(),
            ShareableMessageHandler::from_typed(move |response: &PoolSnapshotResponse| {
                sink.borrow_mut().push(response.correlation_id);
            }),
        );

        let correlation_id = UUID4::new();

        let snapshot = PoolSnapshot::new(
            InstrumentId::from("0x11b815efB8f581194ae79006d24E0d814B7697F6.Arbitrum:UniswapV3"),
            PoolState::default(),
            Vec::new(),
            Vec::new(),
            PoolAnalytics::default(),
            BlockPosition::new(1000, "0x0".to_string(), 0, 0),
            UnixNanos::default(),
            UnixNanos::default(),
        );

        AsyncRunner::handle_data_event(DataEvent::PoolSnapshotResponse(PoolSnapshotResponse::new(
            correlation_id,
            snapshot,
        )));

        assert_eq!(*received.borrow(), vec![correlation_id]);
    }
}
