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

//! Live trading node built on a single-threaded tokio event loop.
//!
//! The node owns system lifecycle and the event loop. Its reconciliation module schedules checks,
//! manages report futures and deadlines, and dispatches results. The execution manager owns
//! reconciliation state, discrepancy decisions, and individual reconciliation operations.
//!
//! `LiveNode::run()` drives the system through a `tokio::select!` loop that
//! multiplexes data events, execution events, trading commands, timers, and
//! periodic maintenance tasks (reconciliation, purge, prune, audit).
//!
//! # Threading model
//!
//! The core types (`ExecutionManager`, `ExecutionEngine`, `Cache`) use
//! `Rc<RefCell<..>>` and are `!Send`. All access happens on the same thread.
//! Pending report futures can retain client borrows while other select branches run.
//! The client facade defers instrument updates until those borrows are released;
//! completion and cancellation paths flush the deferred updates. Single-threaded
//! execution does not by itself prevent conflicting borrows or reentrant callbacks.
//!
//! # Startup sequencing
//!
//! Startup connects clients in two phases so that instruments are in the
//! cache before execution clients read them:
//!
//! 1. Connect data clients (instruments arrive as buffered `DataEvent`s).
//! 2. Flush all pending data events and commands into the cache via
//!    the original Node dispatch handlers, retaining each dequeue receipt
//!    and whole batch until its engine borrow is available.
//! 3. Connect execution clients (`load_instruments_from_cache` now finds
//!    populated instruments).
//! 4. Drain remaining events, then run reconciliation.
//!
//! Both `run()` (integrated event loop) and `start()` (manual lifecycle)
//! follow this sequence.
//!
//! # Reconciliation
//!
//! Continuous inflight, open-order, and position checks run on independent intervals. The
//! shared maintenance timer in the select loop dispatches reconciliation at
//! the minimum enabled interval. Each dispatch the handler checks which
//! sub-checks are due based on elapsed nanoseconds and schedules their work.
//! Continuous checks do not await venue HTTP in the select loop: open-order
//! and position checks poll bulk venue report futures from the loop.
//!
//! # Maintenance dispatcher
//!
//! Six periodic tasks share a single coarse `maintenance_timer`:
//!
//! - reconciliation (inflight, open, position sub-checks)
//! - purge closed orders
//! - purge closed positions
//! - purge account events
//! - own-books audit
//! - recent-fills cache prune
//!
//! The runner wakes one timer per loop iteration regardless of how many
//! maintenance tasks are configured. Each task tracks its own
//! `next_fire: Instant` and the dispatcher fires the bodies whose deadline
//! has passed, rescheduling `next = now + interval` (equivalent to
//! `MissedTickBehavior::Delay`). Disabled tasks anchor on a far-future
//! `next` that never trips.
//!
//! The 100ms timer cadence is the effective floor for any maintenance
//! interval. Configured intervals below 100ms (the config types allow
//! `inflight_check_interval_ms` and `own_books_audit_interval_secs` smaller)
//! become eligible on the next maintenance tick. Event processing and runtime
//! scheduling can delay dispatch further; the timer does not guarantee a maximum delay.
//! When maintenance and runner inputs are both ready, one maintenance root
//! alternates with one actual runner input. This bounds priority in both
//! directions even when synchronous checkpoint persistence exceeds 100ms.
//! Ready runner channels also rotate in bounded cyclic order; a permanent
//! timer backlog cannot prevent original execution/data FIFO progress.

use std::{
    any::Any,
    cell::RefCell,
    fmt::Debug,
    rc::{Rc, Weak},
    time::Duration,
};

#[cfg(feature = "dispatch-observer")]
use std::collections::VecDeque;

use anyhow::Context;
use nautilus_common::{
    actor::{self, Actor, DataActor, DataActorNative},
    cache::{
        Cache,
        database::{CacheDatabaseAdapter, CacheDatabaseFactory},
    },
    component::Component,
    enums::{Environment, LogColor},
    live::{dispatch::DispatchMessage, dst},
    log_info,
    messages::{
        DataEvent, ExecutionEvent, ExecutionReport, SystemCommand, SystemEvent,
        data::DataCommand,
        execution::TradingCommand,
        system::{QueueStateChanged, ReconnectSocket, SocketStateChange, SocketStateChanged},
    },
    msgbus::{self, BusMessage, MessagingSwitchboard},
    runner::{SystemChannel, TimeEventMessage, TradingCommandMessage},
};
use nautilus_core::{
    UUID4,
    datetime::{mins_to_secs, secs_to_nanos_unchecked},
};
#[cfg(test)]
use nautilus_model::reports::OrderStatusReport;
use nautilus_model::{
    events::OrderEventAny,
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::Order,
};
use nautilus_network::mode::ReconnectRequestOutcome;
#[cfg(any(feature = "dispatch-observer", feature = "python"))]
use nautilus_system::trader::Trader;
use nautilus_system::{config::NautilusKernelConfig, kernel::NautilusKernel};
use nautilus_trading::{
    ExecutionAlgorithm, ExecutionAlgorithmNative,
    strategy::{Strategy, StrategyNative},
};
use tabled::{builder::Builder, settings::Style};

use crate::{
    dispatch::drain_callbacks,
    execution::{
        client::LiveExecutionClient,
        manager::{ExecutionManager, ExecutionManagerConfig, TargetedOrderReportResult},
    },
    runner::{AsyncRunner, AsyncRunnerChannels, PendingRunnerEvent, SnapshotReceiver},
    runner_recovery::RunnerRecoveryHandoff,
    socket::{SocketReconnectLookup, SocketReconnectRegistry},
};

pub mod builder;
pub mod config;

#[cfg(feature = "plugin")]
pub mod plugin;
#[cfg(all(feature = "strategy-plugin", feature = "native-tail-replay"))]
pub mod plugin_recovery;

#[cfg(feature = "dispatch-observer")]
mod dispatch;
#[cfg(feature = "dispatch-observer")]
pub use dispatch::NodeDispatchObserver;

mod metrics;
mod mutation;
mod queue;
mod reconciliation;
mod startup;
pub use mutation::NativeMutationInput;
use startup::{StartupPending, buffer_startup_events};
#[cfg(feature = "dispatch-observer")]
mod recovery;
#[cfg(feature = "dispatch-observer")]
mod recovery_quiescence;
#[cfg(feature = "dispatch-observer")]
pub use recovery_quiescence::{EmptyBootstrapRecoveryReceipt, PausedRecoveryInventory};
#[cfg(feature = "dispatch-observer")]
mod recovery_observation;
#[cfg(feature = "dispatch-observer")]
mod recovery_timers;
#[cfg(feature = "dispatch-observer")]
pub use recovery_observation::{
    RecoveryObservationBoundary, RecoveryReleaseBoundary, RecoveryStartupBoundary,
};
#[cfg(feature = "dispatch-observer")]
pub use recovery_timers::{RetainedRecoveryTimerHandoff, RetainedRecoveryTimerInput};
#[cfg(feature = "dispatch-observer")]
mod recovery_adapters;
#[cfg(feature = "dispatch-observer")]
mod running_checkpoint;
#[cfg(feature = "dispatch-observer")]
pub use running_checkpoint::{
    RunningCheckpointBoundary, RunningCheckpointInventory, RunningCheckpointSchedule,
};
#[cfg(feature = "native-tail-replay")]
mod native_tail;
#[cfg(feature = "native-tail-replay")]
pub use native_tail::NativeTailReplayReceipt;
mod state;

use builder::ExternalMessageBusIngress;
pub use builder::LiveNodeBuilder;
use config::{LiveNodeConfig, PluginConfig, validate_live_environment};
pub use metrics::{RunnerChannelMetricsSnapshot, RunnerMetricsDelta, RunnerMetricsSnapshot};
use metrics::{RunnerChannelQueueDepths, RunnerMetrics};
use queue::{QueueMonitor, QueueStateTransition};
use reconciliation::{
    OpenOrderReportResult, OpenOrderReportTask, PositionReportTask, PositionReportTaskResult,
    ReconciliationCheckIntervals, ReconciliationCheckState, ReportTaskOutcome,
    TargetedOrderReportTask,
};
use state::{EngineConnectionStatus, RunningTransition};
pub use state::{
    LiveNodeHandle, NodeRunMode, NodeState, StartupReconciliationObservation,
    StartupReconciliationPhase,
};

/// Dispatches the run loop performs before yielding to the executor.
///
/// A saturated channel keeps every select branch ready, so the loop would otherwise never return
/// `Pending`. Under a host event loop that starves the adapter I/O tasks feeding those channels,
/// which shows up as lapsed heartbeats and reconnects rather than as backpressure.
const DISPATCHES_PER_YIELD: usize = 64;

thread_local! {
    static NODE_THREAD_OWNER: RefCell<Weak<()>> = const { RefCell::new(Weak::new()) };
}

type StreamProcessorCallback = dyn Fn(&dyn Any, &serde_json::Value) -> anyhow::Result<()> + 'static;

struct StreamProcessor(Box<StreamProcessorCallback>);

impl Debug for StreamProcessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(StreamProcessor)).finish()
    }
}

/// High-level abstraction for a live Nautilus system node.
///
/// Provides a simplified interface for running live systems
/// with automatic client management and lifecycle handling.
///
/// Only one live node may exist on a thread at a time. Drop the node before building another,
/// including after disposal, because retained nodes can still access thread-local messaging.
/// Concurrent nodes in one process remain unsupported, even on separate threads.
#[derive(Debug)]
pub struct LiveNode {
    kernel: NautilusKernel,
    #[cfg(feature = "dispatch-observer")]
    dispatch_observer: Option<NodeDispatchObserver>,
    #[cfg(feature = "dispatch-observer")]
    recovery_dispatch_queue: VecDeque<crate::dispatch::DispatchInput>,
    recovery_requires_release: bool,
    #[cfg(feature = "dispatch-observer")]
    recovery_native_frontier: Option<crate::runner_recovery::RunnerRecoveryWatermark>,
    #[cfg(feature = "dispatch-observer")]
    recovery_cache_installed: bool,
    #[cfg(feature = "dispatch-observer")]
    recovery_restored_components: Option<(Vec<String>, Vec<String>)>,
    #[cfg(feature = "dispatch-observer")]
    recovery_empty_bootstrap: Option<EmptyBootstrapRecoveryReceipt>,
    #[cfg(feature = "dispatch-observer")]
    running_checkpoint: Option<running_checkpoint::RunningCheckpointRegistration>,
    #[cfg(feature = "dispatch-observer")]
    terminal_checkpoint: Option<running_checkpoint::TerminalCheckpointReceipt>,
    #[cfg(feature = "dispatch-observer")]
    recovery_observation: Option<recovery_observation::RecoveryObservationRegistration>,
    #[cfg(feature = "dispatch-observer")]
    recovery_timers: Option<recovery_timers::RetainedNodeTimers>,
    #[cfg(feature = "dispatch-observer")]
    recovery_adapter_source: Option<std::collections::BTreeMap<String, serde_json::Value>>,
    #[cfg(feature = "dispatch-observer")]
    recovery_engine_source: Option<serde_json::Value>,
    #[cfg(feature = "dispatch-observer")]
    recovery_portfolio_source: Option<serde_json::Value>,
    #[cfg(feature = "native-tail-replay")]
    recovery_market_source: Option<native_tail::InstalledNativeMarketCut>,
    #[cfg(feature = "native-tail-replay")]
    historical_replay: Option<nautilus_event_store::native_trace::NativeHistoricalRootReplay>,
    #[cfg(feature = "native-tail-replay")]
    historical_ingress: Option<nautilus_common::live::ingress::FrozenIngress>,
    #[cfg(feature = "native-tail-replay")]
    historical_timer_admissions: Vec<(String, u64, bool, nautilus_common::timer::TimeEvent, u64)>,
    #[cfg(feature = "native-tail-replay")]
    recovery_engine_time: Option<(dst::time::Instant, u64)>,
    #[cfg(feature = "native-tail-replay")]
    native_report_contexts:
        std::cell::RefCell<std::collections::BTreeMap<String, serde_json::Value>>,
    runner: Option<AsyncRunner>,
    config: LiveNodeConfig,
    handle: LiveNodeHandle,
    exec_manager: ExecutionManager,
    exec_clients: Vec<LiveExecutionClient>,
    socket_registry: SocketReconnectRegistry,
    cache_database_factory: Option<Box<dyn CacheDatabaseFactory>>,
    external_msgbus: Option<ExternalMessageBusIngress>,
    stream_processors: Vec<StreamProcessor>,
    shutdown_deadline: Option<dst::time::Instant>,
    contain_event_store_failure: bool,
    #[cfg(feature = "dispatch-observer")]
    dispatch_failure: std::cell::Cell<bool>,
    #[cfg(feature = "plugin")]
    plugins: plugin::NodePlugins,
    _thread_owner: Rc<()>,
}

// The token guard spans the entire synchronous implementation, including descendants.
macro_rules! begin_node_dispatch {
    ($node:expr, $source:ident, $input:expr $(, $fallback:expr)?) => {{
        #[cfg(feature = "dispatch-observer")]
        {
            match $node.begin_node_dispatch(crate::dispatch::DispatchSource::$source, $input) {
                Ok(guard) => guard,
                Err(error) => {
                    log::error!("Dispatch begin failed: {error:#}");
                    $node.fail_native_dispatch(&format!("dispatch begin failed: {error:#}"));
                    return $($fallback)?;
                }
            }
        }
        #[cfg(not(feature = "dispatch-observer"))]
        {
            // Keep the call site and its receiver observable in the uninstrumented
            // build without creating a unit binding that Clippy rejects.
            let _ = &$node;
            None::<()>
        }
    }};
}
macro_rules! complete_node_dispatch {
    ($node:expr, $guard:expr) => {{
        #[cfg(feature = "dispatch-observer")]
        if let Some(guard) = $guard {
            if let Err(error) = $node.finish_node_dispatch(guard) {
                log::error!("Dispatch completion failed: {error:#}");
                $node.fail_native_dispatch(&format!("dispatch completion failed: {error:#}"));
            }
        }
        #[cfg(not(feature = "dispatch-observer"))]
        {
            let _ = $guard;
        }
    }};
}

impl LiveNode {
    /// Opens this fresh node's actual Journal before its first native root and
    /// binds the recorder issued by that same store. Registered components stay
    /// unstarted and native trading stays Halted; normal start consumes the same
    /// owned Journal continuation exactly once.
    #[cfg(feature = "native-tail-replay")]
    #[allow(clippy::too_many_arguments)]
    ///
    /// # Errors
    /// Returns an error unless the idle node, original Journal, witnesses, and source profiles can establish one owned native trace.
    pub fn prepare_owned_native_trace<
        R: Into<nautilus_common::recovery_trace::NativeReadWitnesses>,
    >(
        &mut self,
        logical_run: String,
        configuration_digest: String,
        codec_profile: String,
        registered_profile_digest: String,
        witnesses: impl Fn(crate::dispatch::DispatchSource, &str, &dyn Any) -> anyhow::Result<R>
        + 'static,
        verify_witnesses: impl Fn(
            &[nautilus_common::recovery_trace::NativeReadWitness],
        ) -> anyhow::Result<()>
        + 'static,
    ) -> anyhow::Result<nautilus_event_store::native_trace::NativeTraceRecorder> {
        anyhow::ensure!(
            self.state() == NodeState::Idle
                && !self.handle.should_stop()
                && !self.recovery_requires_release
                && self.dispatch_observer.is_some(),
            "fresh owned native trace requires an unused node and strict observer"
        );
        self.kernel
            .risk_engine
            .try_borrow_mut()?
            .set_trading_state(nautilus_model::enums::TradingState::Halted);
        self.kernel.open_event_store_for_paused_recovery()?;
        let store = self
            .kernel
            .event_store()
            .context("actual native source Journal missing")?;
        let source = nautilus_common::recovery_trace::NativeTraceSource {
            schema_version: 1,
            node_instance: self.kernel.instance_id(),
            process_incarnation: nautilus_common::recovery_trace::native_event_uuid(),
            journal_run: store
                .run_id()
                .context("actual native source Journal not opened")?
                .to_owned(),
            logical_run,
            configuration_digest,
            codec_profile,
            registered_profile_digest,
        };
        let owned_handle = store.native_trace_handle(source);
        let result = (|| {
            let handle = owned_handle?;
            let recorder = handle
                .downcast::<nautilus_event_store::native_trace::NativeTraceRecorder>()
                .map_err(|_| {
                    anyhow::anyhow!("owned Journal returned an unsupported native trace type")
                })?;
            self.set_native_recovery_trace((*recorder).clone(), witnesses, verify_witnesses)?;
            Ok((*recorder).clone())
        })();
        if let Err(error) = &result {
            self.fail_native_dispatch(&format!("owned native trace preparation failed: {error:#}"));
        }
        result
    }

    /// Binds actual Journal tracing before any native startup or runner dispatch.
    /// All unsupported source read dependencies must fail in the witness collector.
    /// This does not authorize submissions or replace the ordinary current observation phase.
    ///
    /// # Errors
    /// Refuses a changed installation, absent observer, non-idle node or a prior source root.
    #[cfg(feature = "native-tail-replay")]
    pub fn set_native_recovery_trace<
        R: Into<nautilus_common::recovery_trace::NativeReadWitnesses>,
    >(
        &mut self,
        recorder: nautilus_event_store::native_trace::NativeTraceRecorder,
        witnesses: impl Fn(crate::dispatch::DispatchSource, &str, &dyn Any) -> anyhow::Result<R>
        + 'static,
        verify_witnesses: impl Fn(
            &[nautilus_common::recovery_trace::NativeReadWitness],
        ) -> anyhow::Result<()>
        + 'static,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "native source trace requires a fresh idle node"
        );
        anyhow::ensure!(
            self.config.queue_monitor.is_none(),
            "native history queue-monitor state profile is not installed"
        );
        let source = recorder.source()?;
        anyhow::ensure!(
            source.node_instance == self.kernel.instance_id()
                && self.kernel.event_store().and_then(|store| store.run_id())
                    == Some(source.journal_run.as_str()),
            "native trace is not the actual node Journal installation"
        );
        let actual_risk = self.kernel.risk_engine.clone();
        let actual_clock = self.kernel.clock.clone();
        let actual_trader = self.kernel.trader.clone();
        self.dispatch_observer.as_mut()
            .ok_or_else(|| anyhow::anyhow!("native trace requires the node dispatch observer"))?
            .attach_native_trace(recorder, move |source, phase, input| {
                let mut found: nautilus_common::recovery_trace::NativeReadWitnesses =
                    witnesses(source, phase, input)?.into();
                anyhow::ensure!(!found.iter().any(|witness| witness.component_id.starts_with("native:")),
                    "caller cannot substitute the native risk witness");
                found.push(nautilus_common::recovery_trace::NativeReadWitness {
                    component_id: "native:risk_engine".into(), profile: "native_risk_business_state.v1".into(),
                    source_version: "native_actual_registered_engine.v1".into(),
                    payload: serde_json::json!({"trading_state":actual_risk.try_borrow()?.trading_state()}),
                });
                let admission = {
                    let trader = actual_trader.try_borrow()?;
                    trader.actor_ids().into_iter().map(|id| id.inner())
                        .chain(trader.strategy_ids().into_iter().map(|id| id.inner()))
                        .map(|id| (id.to_string(),
                            serde_json::json!({
                                "admitted": nautilus_common::actor::recovery_observation::callback_admitted(&id),
                                "observer": nautilus_common::actor::recovery_observation::is_observer(&id),
                            })))
                        .collect::<std::collections::BTreeMap<_,_>>()
                };
                found.push(nautilus_common::recovery_trace::NativeReadWitness {
                    component_id: "native:callback_admission".into(),
                    profile: "actual_registered_callback_admission.v1".into(),
                    source_version: "native_actual_registered_admission.v1".into(),
                    payload: serde_json::json!({"components":admission}),
                });
                let (timers, timer_capture_ns) = recovery_quiescence::with_registered_timer_inventory_mode(
                    &actual_clock, &actual_trader, false, |_, inventory, verify, _| {
                        verify()?;
                        let captured_at_ns = u64::try_from(nautilus_core::time::duration_since_unix_epoch().as_nanos()).context("native Unix timestamp exceeds receipt range")?;
                        Ok((inventory.clone(), captured_at_ns))
                    })?;
                let mut timer_input = serde_json::Value::Null;
                if source == crate::dispatch::DispatchSource::Time {
                    let message = input.downcast_ref::<TimeEventMessage>().context("actual timer message absent")?;
                    let binding = message.checkpoint_callback_binding();
                    let id = binding["binding_id"].as_u64().context("historical timer is not an owner callback")?;
                    let active_owners = timers.iter().filter(|(_, inventory)| inventory["timers"].as_array()
                        .is_some_and(|entries| entries.iter().any(|entry| entry["binding"]["binding_id"].as_u64() == Some(id))))
                        .map(|(owner, _)| owner.clone()).collect::<Vec<_>>();
                    let clock_id = message.checkpoint_owner_clock_id()
                        .context("actual queued timer lease has no original owner Clock")?;
                    let owners = timers.iter().filter(|(_, inventory)| {
                        inventory["native_clock_id"] == serde_json::json!(clock_id)
                    }).map(|(owner, _)| owner.clone()).collect::<Vec<_>>();
                    anyhow::ensure!(owners.len() == 1 && (active_owners.is_empty() || active_owners == owners),
                        "actual timer owner binding is missing, ambiguous or differs from its original lease");
                    let event = message.event();
                    timer_input = serde_json::json!({"owner":owners[0],"binding":binding,
                        "event":{"name":event.name,"event_id":event.event_id,"ts_event":event.ts_event,"ts_init":event.ts_init}});
                }
                found.push(nautilus_common::recovery_trace::NativeReadWitness {
                    component_id: "native:timers".into(), profile: "actual_registered_owner_timer_input.v1".into(),
                    source_version: "native_actual_registered_clock.v1".into(),
                    payload: serde_json::json!({"inventory":timers,"input":timer_input,"captured_at_ns":timer_capture_ns}),
                });
                Ok(found)
            }, move |found| {
                let external = found.iter().filter(|witness| !witness.component_id.starts_with("native:")).cloned().collect::<Vec<_>>();
                verify_witnesses(&external)
            })?;
        self.bind_native_ingress_codec()
    }

    #[cfg(feature = "native-tail-replay")]
    fn bind_native_ingress_codec(&self) -> anyhow::Result<()> {
        let encoder = self
            .dispatch_observer
            .as_ref()
            .context("native derived ingress observer absent")?
            .weak_native_ingress_codec();
        nautilus_common::recovery_trace::set_native_ingress_encoder(encoder);
        Ok(())
    }

    #[cfg(feature = "dispatch-observer")]
    fn finish_node_dispatch(&self, guard: dispatch::NodeDispatchGuard) -> anyhow::Result<()> {
        #[cfg(feature = "native-tail-replay")]
        if guard.requires_native_effects() {
            // Descendants keep this original source scope through delivery.
            // All handler-local actor/engine/cache borrows have ended here.
            nautilus_common::actor::settle_native_input_callbacks()?;
            // A failed historical business hook must be returned before reading
            // another Clock or sampling effects. Sampling is not error recovery.
            if self.historical_replay.is_some() {
                nautilus_common::recovery_trace::historical::pending_callback_route()?;
            }
            if let Some(replay) = &self.historical_replay {
                let expected = replay.expected_effects()?;
                let scheduled = serde_json::from_value(expected["registered_timers"].clone())?;
                self.recovery_timers
                    .as_ref()
                    .context("historical native owner clocks missing")?
                    .refresh_historical_dispatch(&scheduled)?;
                self.recovery_timers
                    .as_ref()
                    .context("historical native owner clocks missing")?
                    .admit_historical_timers_after_dispatch(
                        &replay.expected_begin()?,
                        &scheduled,
                        &self.historical_timer_admissions,
                        expected["timer_capture_ns"]
                            .as_u64()
                            .context("original timer boundary absent")?,
                    )?;
            }
            let effects = self.collect_native_trace_effects()?;
            return guard.complete_native(effects);
        }
        guard.complete()
    }

    #[cfg(feature = "dispatch-observer")]
    fn collect_recovery_checkpoint_components(
        &self,
    ) -> anyhow::Result<nautilus_system::trader::CollectedComponentState> {
        #[cfg(feature = "native-tail-replay")]
        {
            Trader::collect_native_component_state(&self.kernel.trader)
        }
        #[cfg(not(feature = "native-tail-replay"))]
        {
            Trader::collect_component_state(&self.kernel.trader)
        }
    }

    /// Restores only Reader-verified framework state into this actual Idle node.
    /// All source data remain untrusted until actual registered configuration,
    /// native cache and same-owner source schedules have been checked. The
    /// physical lifecycle and submission gates remain paused.
    #[cfg(feature = "native-tail-replay")]
    ///
    /// # Errors
    /// Returns an error if the node lifecycle, original component configuration, cache, or timer source cannot be validated.
    pub fn restore_native_component_state(
        &mut self,
        state: &nautilus_system::trader::CollectedComponentState,
        trace: &nautilus_event_store::native_trace::VerifiedNativeTrace,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "native component restore requires Idle"
        );
        anyhow::ensure!(
            self.recovery_restored_components.is_none() && self.recovery_empty_bootstrap.is_none(),
            "native component source already installed"
        );
        let framework = trace
            .cut()
            .native_effects
            .get("strategy_framework")
            .context("reader source cut lacks native strategy framework")?;
        self.recovery_requires_release = true;
        Trader::restore_native_component_state(
            self.kernel.trader(),
            state,
            framework,
            &trace.cut().registered_timers,
        )?;
        self.recovery_restored_components = Some((
            state.actors.keys().map(ToString::to_string).collect(),
            state.strategies.keys().map(ToString::to_string).collect(),
        ));
        Ok(())
    }

    #[cfg(feature = "native-tail-replay")]
    fn collect_native_trace_effects(&self) -> anyhow::Result<serde_json::Value> {
        recovery_quiescence::with_registered_timer_inventory_mode(
            &self.kernel.clock,
            &self.kernel.trader,
            false,
            |_, timers, verify, _| {
                let timer_capture_ns = if let Some(replay) = &self.historical_replay {
                    replay.expected_effects()?["timer_capture_ns"]
                        .as_u64()
                        .context("source timer capture absent")?
                } else {
                    u64::try_from(nautilus_core::time::duration_since_unix_epoch().as_nanos())
                        .context("native Unix timestamp exceeds receipt range")?
                };
                let (manager_at, manager_capture_elapsed) =
                    if let Some(replay) = &self.historical_replay {
                        replay.effects_capture_instant()?
                    } else if let Some(trace) = self
                        .dispatch_observer
                        .as_ref()
                        .and_then(NodeDispatchObserver::native_trace)
                    {
                        let (at, elapsed) = trace.effects_capture_instant()?;
                        (at, Some(elapsed))
                    } else {
                        (dst::time::Instant::now(), None)
                    };
                let cache = self.kernel.cache.try_borrow()?;
                let mut orders = cache
                    .orders(None, None, None, None, None)
                    .iter()
                    .map(|order| (**order).clone())
                    .collect::<Vec<_>>();
                let mut positions = cache
                    .positions(None, None, None, None, None)
                    .iter()
                    .map(|position| (**position).clone())
                    .collect::<Vec<_>>();
                let mut accounts = cache.accounts_all_owned();
                orders.sort_by_key(nautilus_model::orders::Order::client_order_id);
                positions.sort_by_key(|position| position.id);
                accounts.sort_by_key(nautilus_model::accounts::AccountAny::id);
                let market = cache.native_market_checkpoint()?;
                let market_config = cache.native_market_checkpoint_config();
                let components = Trader::collect_native_recovery_state(&self.kernel.trader)?;
                let strategy_framework =
                    Trader::collect_native_strategy_framework(&self.kernel.trader)?;
                let data = self
                    .kernel
                    .data_engine
                    .try_borrow()?
                    .running_checkpoint_state()?;
                let manager = self.exec_manager.trace_effects_inventory(manager_at)?;
                let portfolio = self
                    .kernel
                    .portfolio
                    .try_borrow()?
                    .running_checkpoint_state()?;
                verify()?;
                let mut effects = serde_json::json!({"schema":"NautilusNativeTraceEffects.v1","timer_capture_ns":timer_capture_ns,
                    "orders":orders, "positions":positions, "accounts":accounts,"market_cache":market,"market_cache_config":market_config,
                    "components":components, "strategy_framework":strategy_framework, "data_engine":data, "execution_manager":manager, "portfolio":portfolio,
                    "registered_timers":if self.historical_replay.is_some() {
                        self.recovery_timers.as_ref().context("historical owner timer installation missing")?.historical_inventory()?
                    } else { timers.clone() }, "report_contexts": &*self.native_report_contexts.try_borrow()?,
                    "risk_state":nautilus_common::recovery_trace::native_risk_state(self.kernel.risk_engine.try_borrow()?.trading_state())});
                if let Some(elapsed) = manager_capture_elapsed {
                    effects["manager_effects_capture_process_elapsed_ns"] =
                        serde_json::Value::from(elapsed);
                }
                Ok(effects)
            },
        )
    }
    /// Installs partial dispatch instrumentation before startup; no recovery authority.
    /// # Errors
    /// Rejects duplicate installation or any non-idle node.
    #[cfg(feature = "dispatch-observer")]
    pub fn set_dispatch_observer(&mut self, observer: NodeDispatchObserver) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle && self.dispatch_observer.is_none(),
            "dispatch observer requires a fresh idle node"
        );
        self.dispatch_observer = Some(observer);
        Ok(())
    }

    /// Returns the latest completed-root proof from the installed observer.
    /// A missing observer yields `None`; the proof never grants recovery or
    /// execution authority and becomes invalid after the next input.
    ///
    /// # Errors
    /// Rejects reentrant access or a failed observer run.
    #[cfg(feature = "dispatch-observer")]
    pub fn dispatch_completion_proof(
        &self,
    ) -> anyhow::Result<Option<crate::dispatch::DispatchCompletionProof>> {
        self.dispatch_observer
            .as_ref()
            .map_or(Ok(None), NodeDispatchObserver::completion_proof)
    }

    /// Runs a caller-owned recovery callback inside the same observed dispatch
    /// boundary used by live ingress. The node must remain idle: this method
    /// provides the durable begin/complete envelope and callback ordering, but
    /// it does not start the node, create a queue, or grant execution access.
    ///
    /// A callback error rejects the input and stops the node handle. A
    /// completion or observer failure also stops the handle so a caller cannot
    /// accidentally continue with an invalid dispatch proof.
    ///
    /// # Errors
    ///
    /// Rejects a non-idle node, a node without the opt-in observer, observer
    /// begin/complete failures, or the caller callback failure.
    #[cfg(feature = "dispatch-observer")]
    pub fn with_recovery_dispatch<T, F>(
        &mut self,
        input: crate::dispatch::DispatchInput,
        apply: F,
    ) -> anyhow::Result<T>
    where
        F: FnOnce(&mut Self) -> anyhow::Result<T>,
    {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "recovery dispatch requires a fresh idle node"
        );
        anyhow::ensure!(
            !self.handle.should_stop(),
            "recovery node is stopped or poisoned"
        );
        self.recovery_requires_release = true;
        let observer = self
            .dispatch_observer
            .clone()
            .context("recovery dispatch requires an installed dispatch observer")?;
        let guard = match observer.begin_input(input) {
            Ok(guard) => guard,
            Err(error) => {
                self.fail_native_dispatch(&format!("recovery dispatch begin failed: {error:#}"));
                return Err(error).context("recovery dispatch begin failed");
            }
        };

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| apply(self)));
        let result = match result {
            Ok(result) => result,
            Err(panic) => {
                self.fail_native_dispatch("recovery dispatch callback panicked");
                drop(guard);
                std::panic::resume_unwind(panic);
            }
        };
        match result {
            Ok(value) => {
                if let Err(error) = guard.complete() {
                    self.fail_native_dispatch(&format!(
                        "recovery dispatch completion failed: {error:#}"
                    ));
                    return Err(error).context("recovery dispatch completion failed");
                }
                Ok(value)
            }
            Err(error) => {
                self.fail_native_dispatch(&format!("recovery dispatch callback failed: {error:#}"));
                if let Err(abort_error) = guard.rejected() {
                    return Err(error).context(format!(
                        "recovery dispatch callback failed and abort could not be recorded: {abort_error:#}"
                    ));
                }
                Err(error)
            }
        }
    }

    /// Queues one already encoded recovery input for ordered handoff to this
    /// node. Queuing is deliberately separate from dispatch: the caller can
    /// stage the complete sealed inventory before any business callback runs,
    /// and [`Self::drain_recovery_dispatch`] consumes it in FIFO order.
    ///
    /// The node must remain fresh and idle. A queued input is not considered
    /// acknowledged until the drain method enters its observed begin/complete
    /// boundary. Starting or disposing a node with queued recovery inputs is
    /// rejected so a pending envelope cannot be silently lost.
    ///
    /// # Errors
    ///
    /// Rejects a non-idle or stop-requested node, a node without the opt-in
    /// observer, or an empty dispatch phase.
    #[cfg(feature = "dispatch-observer")]
    pub fn enqueue_recovery_dispatch(
        &mut self,
        input: crate::dispatch::DispatchInput,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "recovery dispatch queue requires a fresh idle node"
        );
        anyhow::ensure!(
            self.dispatch_observer.is_some(),
            "recovery dispatch queue requires an installed dispatch observer"
        );
        anyhow::ensure!(!input.phase.trim().is_empty(), "empty dispatch phase");
        self.recovery_dispatch_queue.push_back(input);
        Ok(())
    }

    /// Returns the number of staged recovery inputs awaiting dispatch.
    #[cfg(feature = "dispatch-observer")]
    #[must_use]
    pub fn recovery_dispatch_queue_len(&self) -> usize {
        self.recovery_dispatch_queue.len()
    }

    /// Drains staged recovery inputs through the same observed dispatch
    /// boundary used by live ingress.
    ///
    /// Inputs are offered strictly FIFO. On callback or observer failure the
    /// current input is put back at the front, all later inputs remain staged,
    /// and the node is stopped by [`Self::with_recovery_dispatch`]. The caller
    /// can therefore inspect the exact unprocessed suffix instead of losing it
    /// to a partially consumed in-memory queue.
    ///
    /// # Errors
    ///
    /// Rejects a non-idle node, a missing observer, or the first failed input.
    #[cfg(feature = "dispatch-observer")]
    pub fn drain_recovery_dispatch<F>(&mut self, mut apply: F) -> anyhow::Result<usize>
    where
        F: FnMut(&mut Self, crate::dispatch::DispatchInput) -> anyhow::Result<()>,
    {
        anyhow::ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "recovery dispatch drain requires a fresh idle node"
        );
        anyhow::ensure!(
            self.dispatch_observer.is_some(),
            "recovery dispatch drain requires an installed dispatch observer"
        );

        let mut drained = 0;
        while let Some(input) = self.recovery_dispatch_queue.pop_front() {
            let callback_input = input.clone();
            let result =
                self.with_recovery_dispatch(input.clone(), |node| apply(node, callback_input));
            match result {
                Ok(()) => drained += 1,
                Err(error) => {
                    self.recovery_dispatch_queue.push_front(input);
                    return Err(error).context("recovery dispatch queue drain failed");
                }
            }
        }
        Ok(drained)
    }

    /// Exposes machine-readable observer coverage and failure state to the host
    /// so an incomplete matrix cannot be mistaken for a healthy node.
    ///
    /// # Errors
    /// Rejects reentrant access while reading observer state.
    #[cfg(feature = "dispatch-observer")]
    pub fn dispatch_coverage(&self) -> anyhow::Result<serde_json::Value> {
        self.dispatch_observer.as_ref().map_or_else(
            || Ok(serde_json::json!({"installed": false})),
            NodeDispatchObserver::coverage,
        )
    }

    #[cfg(feature = "dispatch-observer")]
    fn begin_node_dispatch(
        &self,
        source: crate::dispatch::DispatchSource,
        input: &dyn std::any::Any,
    ) -> anyhow::Result<Option<dispatch::NodeDispatchGuard>> {
        #[cfg(feature = "native-tail-replay")]
        if let Some(replay) = &self.historical_replay {
            let begin = replay.next_begin()?;
            let nautilus_common::recovery_trace::NativeTraceRecord::Begin {
                phase, receipt, ..
            } = &begin
            else {
                anyhow::bail!("historical native input missing Begin")
            };
            let observer = self
                .dispatch_observer
                .as_ref()
                .context("historical native observer absent")?;
            let encoded = observer.encode_historical_source(source, phase, input)?;
            let native_source = dispatch::native_source(source)?;
            let historical = replay.begin(native_source, &encoded.payload)?;
            if let Some(expected) = &receipt.ingress
                && observer.native_trace().is_some()
            {
                // Actual child tracing enables receipt publication on dequeue.
                // Consume that same original receipt before another envelope can
                // replace it. Reader-only replay already verifies the frozen FIFO.
                let actual = nautilus_common::recovery_trace::scope::take_ingress(native_source)?;
                anyhow::ensure!(
                    actual.as_ref() == Some(expected),
                    "historical actual dequeue receipt differs from its original Begin"
                );
            }
            nautilus_common::recovery_trace::historical::bind_current_native_queue_retention(
                self.historical_ingress
                    .as_ref()
                    .context("historical native ingress owner absent")?,
            )?;
            let mut guard = observer.begin_input(crate::dispatch::DispatchInput {
                source: crate::dispatch::DispatchSource::Replay,
                phase: "verified_native_tail".into(),
                payload: serde_json::to_value(&begin)?,
                batch_index: None,
            })?;
            guard.attach_historical(historical)?;
            return Ok(Some(guard));
        }
        let phase = format!("{:?}", self.state());
        self.dispatch_observer
            .as_ref()
            .map(|observer| observer.begin(source, &phase, input))
            .transpose()
    }

    /// All instrumentation failures close the same actual instance before any
    /// application callback. Stop alone is not containment or durable retention.
    #[cfg(feature = "dispatch-observer")]
    fn fail_native_dispatch(&self, reason: &str) {
        self.dispatch_failure.set(true);
        if let Err(error) = self.kernel.contain_native_failure(reason) {
            log::error!("Native failure retention refused: {error:#}");
        }
        if let Some(registration) = &self.recovery_observation {
            let fence = registration.fence.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fence(reason)));
        }
        if let Some(registration) = &self.running_checkpoint {
            let fence = registration.fence.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fence(reason)));
        }
        self.handle.stop();
    }

    fn note_dispatch_gap(&self, reason: &str) -> bool {
        #[cfg(feature = "dispatch-observer")]
        if let Some(observer) = &self.dispatch_observer
            && let Err(error) = observer.uncovered(reason)
        {
            log::error!("Dispatch coverage recording failed: {error:#}");
            self.fail_native_dispatch(&format!("dispatch coverage failed: {error:#}"));
            return false;
        }
        #[cfg(not(feature = "dispatch-observer"))]
        {
            let _ = (self, reason);
        }
        true
    }

    /// Returns whether the kernel's injected event store has entered its fail-stop state.
    ///
    /// Event-store write failures can happen while the node is otherwise idle, so the running
    /// loop must poll this state independently of business-message traffic. A halted store is
    /// never treated as a recoverable transient condition by the live node.
    #[cfg(feature = "dispatch-observer")]
    fn is_native_historical_dispatch(&self) -> bool {
        #[cfg(feature = "native-tail-replay")]
        {
            self.historical_replay.is_some()
        }
        #[cfg(not(feature = "native-tail-replay"))]
        {
            false
        }
    }

    fn event_store_halted(&self) -> bool {
        self.kernel
            .event_store()
            .is_some_and(nautilus_system::KernelEventStore::is_halted)
    }

    /// Keeps connections and remediation dispatch alive after a persistence halt.
    /// Requires an installed synchronous submission guard before opting in.
    ///
    /// # Errors
    /// Returns an error when the journal or submission guard is absent.
    pub fn enable_event_store_failure_containment(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.kernel.event_store().is_some(),
            "event store required for containment"
        );
        anyhow::ensure!(
            self.kernel.exec_engine.borrow().has_submission_guard(),
            "submission guard required for containment"
        );
        self.contain_event_store_failure = true;
        Ok(())
    }

    /// A running but persistence-failed node is not a healthy trading runtime.
    #[must_use]
    pub fn persistence_degraded(&self) -> bool {
        self.contain_event_store_failure && self.event_store_halted()
    }

    #[allow(clippy::unused_unit)]
    fn process_external_message(&self, message: &BusMessage) {
        let guard = begin_node_dispatch!(self, ExternalMessage, message, ());
        self.process_external_msgbus_message(message);
        complete_node_dispatch!(self, guard);
    }

    fn process_time_event(&self, message: impl Into<DispatchMessage<TimeEventMessage>>) -> bool {
        match self.process_time_event_result(message) {
            Ok(dispatched) => dispatched,
            Err(error) => {
                log::error!("Timer dispatch completion failed: {error:#}");
                #[cfg(feature = "dispatch-observer")]
                self.fail_native_dispatch(&format!("timer completion failed: {error:#}"));
                false
            }
        }
    }

    #[allow(clippy::unused_unit)]
    fn process_time_event_result(
        &self,
        message: impl Into<DispatchMessage<TimeEventMessage>>,
    ) -> anyhow::Result<bool> {
        message
            .into()
            .dispatch(|message| self.process_time_event_unobserved_result(message))
    }
    #[cfg_attr(
        not(feature = "dispatch-observer"),
        expect(
            clippy::unnecessary_wraps,
            reason = "Native dispatch observer completion can fail; both feature modes retain the same typed completion contract"
        )
    )]
    fn process_time_event_unobserved_result(
        &self,
        message: TimeEventMessage,
    ) -> anyhow::Result<bool> {
        #[cfg(feature = "dispatch-observer")]
        let retained_event_id = message.event().event_id;
        #[cfg(feature = "dispatch-observer")]
        let cleanup = message.checkpoint_callback_binding()["kind"] == "registered_cleanup";
        #[cfg(feature = "dispatch-observer")]
        if self.state() == NodeState::Observing
            && let Err(error) = self.verify_observation_timer_admission(&message)
        {
            self.fail_recovery_observation(&format!("{error:#}"));
            return Ok(false);
        }
        #[cfg(feature = "dispatch-observer")]
        if let Some(timers) = &self.recovery_timers
            && !self.is_native_historical_dispatch()
            && let Err(error) = timers.received(&message)
        {
            self.fail_recovery_observation(&format!("{error:#}"));
            return Ok(false);
        }
        let guard = begin_node_dispatch!(self, Time, &message, Ok(false));
        let dispatched = message.dispatch();
        #[cfg(feature = "dispatch-observer")]
        if !dispatched && !cleanup {
            if let Some(guard) = guard {
                let _ = guard.rejected();
                self.fail_native_dispatch("registered timer dispatch was rejected");
            }
            return Ok(false);
        }
        #[cfg(feature = "dispatch-observer")]
        {
            if let Some(guard) = guard {
                self.finish_node_dispatch(guard)?;
            }
            if let Some(timers) = &self.recovery_timers
                && !self.is_native_historical_dispatch()
                && let Err(error) = timers.processed(retained_event_id)
            {
                self.fail_recovery_observation(&format!("{error:#}"));
                return Ok(false);
            }
        }
        #[cfg(not(feature = "dispatch-observer"))]
        complete_node_dispatch!(self, guard);
        Ok(dispatched)
    }
    #[allow(clippy::unused_unit)]
    fn process_data_event(&self, event: impl Into<DispatchMessage<DataEvent>>) {
        event.into().dispatch(|event| {
            let guard = begin_node_dispatch!(self, DataEvent, &event, ());
            AsyncRunner::handle_data_event(event);
            complete_node_dispatch!(self, guard);
        });
    }
    #[allow(clippy::unused_unit)]
    fn process_data_command(&self, command: impl Into<DispatchMessage<DataCommand>>) {
        command.into().dispatch(|command| {
            let guard = begin_node_dispatch!(self, DataCommand, &command, ());
            msgbus::send_data_command(MessagingSwitchboard::data_engine_execute(), command);
            complete_node_dispatch!(self, guard);
        });
    }

    /// Creates a new `LiveNode` from builder components.
    ///
    /// This is an internal constructor used by `LiveNodeBuilder`.
    #[must_use]
    #[allow(
        clippy::too_many_arguments,
        reason = "builder components have distinct lifecycle roles"
    )]
    pub(crate) fn new_from_builder(
        kernel: NautilusKernel,
        runner: AsyncRunner,
        config: LiveNodeConfig,
        mut exec_manager: ExecutionManager,
        exec_clients: Vec<LiveExecutionClient>,
        socket_registry: SocketReconnectRegistry,
        cache_database_factory: Option<Box<dyn CacheDatabaseFactory>>,
        external_msgbus: Option<ExternalMessageBusIngress>,
        thread_owner: Rc<()>,
    ) -> Self {
        for client in &exec_clients {
            exec_manager.register_submission_retention(client);
        }

        Self {
            kernel,
            #[cfg(feature = "dispatch-observer")]
            dispatch_observer: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_dispatch_queue: VecDeque::new(),
            recovery_requires_release: false,
            #[cfg(feature = "dispatch-observer")]
            recovery_native_frontier: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_cache_installed: false,
            #[cfg(feature = "dispatch-observer")]
            recovery_restored_components: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_empty_bootstrap: None,
            #[cfg(feature = "dispatch-observer")]
            running_checkpoint: None,
            #[cfg(feature = "dispatch-observer")]
            terminal_checkpoint: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_observation: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_timers: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_adapter_source: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_engine_source: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_portfolio_source: None,
            #[cfg(feature = "native-tail-replay")]
            recovery_market_source: None,
            #[cfg(feature = "native-tail-replay")]
            historical_replay: None,
            #[cfg(feature = "native-tail-replay")]
            historical_ingress: None,
            #[cfg(feature = "native-tail-replay")]
            historical_timer_admissions: Vec::new(),
            #[cfg(feature = "native-tail-replay")]
            recovery_engine_time: None,
            #[cfg(feature = "native-tail-replay")]
            native_report_contexts: std::cell::RefCell::new(std::collections::BTreeMap::new()),
            handle: LiveNodeHandle::with_ingress(runner.ingress_gate()),
            runner: Some(runner),
            config,
            exec_manager,
            exec_clients,
            socket_registry,
            cache_database_factory,
            external_msgbus,
            stream_processors: Vec::new(),
            shutdown_deadline: None,
            contain_event_store_failure: false,
            #[cfg(feature = "dispatch-observer")]
            dispatch_failure: std::cell::Cell::new(false),
            #[cfg(feature = "plugin")]
            plugins: plugin::NodePlugins,
            _thread_owner: thread_owner,
        }
    }

    /// Creates a new [`LiveNodeBuilder`] for fluent configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment is invalid for live trading.
    pub fn builder(
        trader_id: TraderId,
        environment: Environment,
    ) -> anyhow::Result<LiveNodeBuilder> {
        LiveNodeBuilder::new(trader_id, environment)
    }

    /// Creates a new [`LiveNode`] directly from a kernel name and optional configuration.
    ///
    /// This is a convenience method for creating a live node with a pre-configured
    /// kernel configuration, bypassing the builder pattern. If no config is provided,
    /// a default configuration will be used.
    ///
    /// # Errors
    ///
    /// Returns an error if kernel construction fails or another live node exists or is being
    /// built on this thread.
    pub fn build(name: String, config: Option<LiveNodeConfig>) -> anyhow::Result<Self> {
        let thread_owner = Self::acquire_thread()?;
        let config = config.unwrap_or_default();
        validate_live_environment(config.environment())?;

        config.validate_runtime_support()?;

        if config.event_store.is_some() {
            anyhow::bail!(
                "LiveNodeConfig.event_store is set but LiveNode::build cannot install a factory; \
                 use LiveNodeBuilder::with_event_store(...) instead"
            );
        }

        let runner = AsyncRunner::new();
        runner.bind_senders();

        let kernel = NautilusKernel::new(name, config.clone())?;
        #[cfg(feature = "python")]
        if let Some(controller) = config.controller.as_ref() {
            Trader::add_controller_from_importable_config(&kernel.trader, controller)?;
        }

        #[cfg(not(feature = "python"))]
        if let Some(controller) = config.controller.as_ref() {
            anyhow::bail!(
                "LiveNodeConfig.controller for importable controller '{}' requires the python feature",
                controller.controller_path
            );
        }

        let exec_manager_config =
            ExecutionManagerConfig::from(&config.exec_engine).with_trader_id(config.trader_id);

        let exec_manager = ExecutionManager::new(
            kernel.clock.clone(),
            kernel.cache.clone(),
            exec_manager_config,
        )?;

        let node = Self {
            kernel,
            #[cfg(feature = "dispatch-observer")]
            dispatch_observer: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_dispatch_queue: VecDeque::new(),
            recovery_requires_release: false,
            #[cfg(feature = "dispatch-observer")]
            recovery_native_frontier: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_cache_installed: false,
            #[cfg(feature = "dispatch-observer")]
            recovery_restored_components: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_empty_bootstrap: None,
            #[cfg(feature = "dispatch-observer")]
            running_checkpoint: None,
            #[cfg(feature = "dispatch-observer")]
            terminal_checkpoint: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_observation: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_timers: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_adapter_source: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_engine_source: None,
            #[cfg(feature = "dispatch-observer")]
            recovery_portfolio_source: None,
            #[cfg(feature = "native-tail-replay")]
            recovery_market_source: None,
            #[cfg(feature = "native-tail-replay")]
            historical_replay: None,
            #[cfg(feature = "native-tail-replay")]
            historical_ingress: None,
            #[cfg(feature = "native-tail-replay")]
            historical_timer_admissions: Vec::new(),
            #[cfg(feature = "native-tail-replay")]
            recovery_engine_time: None,
            #[cfg(feature = "native-tail-replay")]
            native_report_contexts: std::cell::RefCell::new(std::collections::BTreeMap::new()),
            handle: LiveNodeHandle::with_ingress(runner.ingress_gate()),
            runner: Some(runner),
            config,
            exec_manager,
            exec_clients: Vec::new(),
            socket_registry: SocketReconnectRegistry::default(),
            cache_database_factory: None,
            external_msgbus: None,
            stream_processors: Vec::new(),
            shutdown_deadline: None,
            contain_event_store_failure: false,
            #[cfg(feature = "dispatch-observer")]
            dispatch_failure: std::cell::Cell::new(false),
            #[cfg(feature = "plugin")]
            plugins: plugin::NodePlugins,
            _thread_owner: thread_owner,
        };

        node.load_configured_plugins()?;

        log::info!("LiveNode built successfully with kernel config");

        Ok(node)
    }

    fn acquire_thread() -> anyhow::Result<Rc<()>> {
        NODE_THREAD_OWNER.with(|slot| {
            let mut owner = slot.borrow_mut();
            anyhow::ensure!(
                owner.upgrade().is_none(),
                "A LiveNode already exists or is being built on this thread; drop it before building another"
            );
            let token = Rc::new(());
            *owner = Rc::downgrade(&token);
            Ok(token)
        })
    }

    /// Loads and registers plug-ins declared on the node config.
    ///
    /// # Errors
    ///
    /// Returns an error if plug-ins are configured without host-side support.
    pub(crate) fn load_configured_plugins(&self) -> anyhow::Result<()> {
        if self.config.plugins.is_empty() {
            return Ok(());
        }

        anyhow::bail!(
            "LiveNodeConfig.plugins requires host-side plug-in support; nautilus-plugin is the guest SDK only"
        )
    }

    /// Loads and registers one plug-in instance.
    ///
    /// # Errors
    ///
    /// Returns an error if called. Dynamic plug-in hosting lives in the host-side integration.
    #[expect(
        clippy::needless_pass_by_value,
        reason = "signature mirrors the host-enabled API"
    )]
    pub fn add_plugin(&mut self, config: PluginConfig) -> anyhow::Result<()> {
        #[cfg(feature = "plugin")]
        config.validate_runtime_support(self.config.plugins.len())?;
        #[cfg(not(feature = "plugin"))]
        let _ = config;

        anyhow::bail!(
            "LiveNode::add_plugin requires host-side plug-in support; nautilus-plugin is the guest SDK only"
        )
    }

    /// Returns a thread-safe handle to control this node.
    #[must_use]
    pub fn handle(&self) -> LiveNodeHandle {
        self.handle.clone()
    }

    /// Adds a callback for supported typed external messages.
    ///
    /// While [`run`](Self::run) or [`run_with_mode`](Self::run_with_mode) services external ingress,
    /// the node invokes processors in registration order before normal inbound streaming filters
    /// for JSON or `MessagePack` payloads when [`msgbus::BusPayloadType::is_typed_message`] returns
    /// `true`. Other encodings are skipped with a warning. Each callback receives the decoded
    /// concrete value as [`Any`], allowing it to downcast to the concrete payload type. External
    /// egress is suppressed while the processors run, so synchronous publications remain local.
    pub fn add_stream_processor<F>(&mut self, callback: F)
    where
        F: Fn(&dyn Any) + 'static,
    {
        self.add_stream_processor_with_mapping(move |message, _| {
            callback(message);
            Ok(())
        });
    }

    pub(crate) fn add_stream_processor_with_mapping<F>(&mut self, callback: F)
    where
        F: Fn(&dyn Any, &serde_json::Value) -> anyhow::Result<()> + 'static,
    {
        self.stream_processors
            .push(StreamProcessor(Box::new(callback)));
    }

    /// Returns a pre-start handoff into this node's real runner channels.
    ///
    /// The handoff is available only while the node is idle and still owns its
    /// runner. `run_with_mode` closes it when channel receivers are extracted;
    /// `start` closes it immediately because that lifecycle intentionally does
    /// not service runner channels after startup.
    ///
    /// # Errors
    /// Returns an error when the node is not idle or its runner has already
    /// been consumed.
    pub fn recovery_runner_handoff(&self) -> anyhow::Result<RunnerRecoveryHandoff> {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "recovery runner handoff requires an idle live node"
        );
        self.runner
            .as_ref()
            .context("recovery runner handoff unavailable after runner consumption")?
            .recovery_handoff()
    }

    /// Observes all seven normal runner queues without consuming messages.
    /// Counts are sampled sequentially and do not establish producer quiescence.
    ///
    /// # Errors
    /// Refuses non-idle nodes or unavailable receivers instead of reporting zero.
    pub fn recovery_runner_queue_counts(
        &self,
    ) -> anyhow::Result<
        std::collections::BTreeMap<crate::runner_recovery::RunnerRecoveryChannel, usize>,
    > {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "recovery queue observation requires an idle node"
        );
        Ok(self
            .runner
            .as_ref()
            .context("recovery queue receivers unavailable")?
            .pending_queue_counts())
    }

    /// Holds actual runner ingress frozen across synchronous component capture.
    /// The snapshot covers seven native queues only; it grants no execution or
    /// complete timer, external-bus, or component-internal inventory guarantee.
    ///
    /// # Errors
    /// Refuses non-paused recovery, active dispatch, connected clients, pending
    /// recovery callbacks, or invalidated admission. Capture failure poisons the node.
    #[cfg(feature = "dispatch-observer")]
    pub fn with_paused_recovery_queue_snapshot<T>(
        &mut self,
        registry: &crate::runner_recovery::RunnerRecoveryCodecRegistry,
        collect: impl FnOnce(&crate::runner_recovery::RunnerPendingSnapshot) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        self.with_paused_recovery_queue_checkpoint(registry, collect, Ok)
    }

    /// Collects component state, validates the frozen boundary, then persists it.
    /// `persist` must perform only durable IO, never callbacks or message delivery.
    /// The boundary is revalidated after persistence; failed output remains unaccepted.
    ///
    /// # Errors
    /// Refuses invalid recovery boundaries and collection/persistence failures.
    /// Every failure after freezing invalidates admission and stops the node.
    #[cfg(feature = "dispatch-observer")]
    pub fn with_paused_recovery_queue_checkpoint<C, T>(
        &mut self,
        registry: &crate::runner_recovery::RunnerRecoveryCodecRegistry,
        collect: impl FnOnce(&crate::runner_recovery::RunnerPendingSnapshot) -> anyhow::Result<C>,
        persist: impl FnOnce(C) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        self.with_paused_recovery_inventory_checkpoint(
            registry,
            |snapshot, _| collect(snapshot),
            persist,
        )
    }

    /// Captures actual native queue, timer, synchronous bus and adapter inventory.
    /// Collection and persistence execute under the same paused admission guards.
    ///
    /// # Errors
    /// Refuses unsupported or busy inventory, invalidated boundaries, and callback errors.
    #[cfg(feature = "dispatch-observer")]
    pub fn with_paused_recovery_inventory_checkpoint<C, T>(
        &mut self,
        registry: &crate::runner_recovery::RunnerRecoveryCodecRegistry,
        collect: impl FnOnce(
            &crate::runner_recovery::RunnerPendingSnapshot,
            &PausedRecoveryInventory,
        ) -> anyhow::Result<C>,
        persist: impl FnOnce(C) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let check = |node: &Self| -> anyhow::Result<()> {
            anyhow::ensure!(
                node.state() == NodeState::Idle
                    && !node.handle.should_stop()
                    && node.recovery_requires_release
                    && (node.recovery_native_frontier.is_some()
                        || node.recovery_empty_bootstrap.is_some()),
                "queue capture requires completed paused native recovery"
            );
            anyhow::ensure!(
                node.recovery_dispatch_queue.is_empty(),
                "recovery callbacks remain queued"
            );
            anyhow::ensure!(
                node.kernel.risk_engine.try_borrow()?.trading_state()
                    == nautilus_model::enums::TradingState::Halted,
                "queue capture requires halted admission"
            );
            anyhow::ensure!(
                node.kernel.data_engine.try_borrow()?.check_disconnected()
                    && node.kernel.exec_engine.try_borrow()?.check_disconnected(),
                "queue capture requires disconnected clients"
            );
            anyhow::ensure!(
                node.config
                    .data_engine
                    .external_clients
                    .as_ref()
                    .is_none_or(Vec::is_empty),
                "external data clients have no paused inventory proof"
            );
            anyhow::ensure!(
                node.external_msgbus.is_none(),
                "external message bus ingress inventory unsupported"
            );
            let data = node.kernel.data_engine.try_borrow()?;
            let execution = node.kernel.exec_engine.try_borrow()?;
            anyhow::ensure!(
                execution.get_external_client_ids().is_empty(),
                "external execution clients have no paused inventory proof"
            );
            for client in data.get_clients() {
                client.get_client().verify_paused_recovery_inventory()?;
            }
            for client in execution.get_all_clients() {
                client.verify_paused_recovery_inventory()?;
            }
            Ok(())
        };
        check(self)?;
        let observer = self
            .dispatch_observer
            .clone()
            .context("queue capture requires observer")?;
        let coverage = observer.coverage()?;
        anyhow::ensure!(
            coverage["active_depth"].as_u64() == Some(0) && coverage["failure"].is_null(),
            "queue capture requires idle healthy observer"
        );
        let proof = observer.completion_proof()?;
        anyhow::ensure!(
            coverage["completed_root"].as_u64() == Some(0) || proof.is_some(),
            "queue capture requires an uninvalidated completion frontier"
        );
        let ingress = self
            .runner
            .as_ref()
            .context("runner receivers unavailable")?
            .ingress_gate();
        let guard = ingress.freeze()?;
        let kernel_clock = self.kernel.clock.clone();
        let trader = self.kernel.trader.clone();
        let data_engine = self.kernel.data_engine.clone();
        let exec_engine = self.kernel.exec_engine.clone();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let data_inventory = data_engine.try_borrow()?;
            let execution_inventory = exec_engine.try_borrow()?;
            recovery_quiescence::with_registered_timer_inventory(
                &kernel_clock,
                &trader,
                |timer_counts| {
                    nautilus_common::msgbus::with_local_only_recovery_inventory(|bus_inventory| {
                        nautilus_common::runner::with_empty_sync_command_queues(|| {
                            check(self)?;
                            let snapshot = self
                                .runner
                                .as_mut()
                                .context("runner receivers unavailable")?
                                .snapshot_pending(&guard, registry)?;
                            let runner_counts = self
                                .runner
                                .as_ref()
                                .context("runner missing")?
                                .pending_queue_counts()
                                .into_iter()
                                .map(|(channel, count)| {
                                    Ok((
                                        serde_json::to_value(channel)?
                                            .as_str()
                                            .context("invalid channel name")?
                                            .to_owned(),
                                        count as u64,
                                    ))
                                })
                                .collect::<anyhow::Result<_>>()?;
                            let (data_count, trading_count) =
                                nautilus_common::runner::synchronous_command_queue_counts()?;
                            let mut adapter_profiles = std::collections::BTreeMap::new();
                            for client in data_inventory.get_clients() {
                                let client = client.get_client();
                                adapter_profiles.insert(
                                    format!("data:{}", client.client_id()),
                                    client.paused_recovery_inventory_profile()?.to_owned(),
                                );
                            }
                            for client in execution_inventory.get_all_clients() {
                                adapter_profiles.insert(
                                    format!("execution:{}", client.client_id()),
                                    client.paused_recovery_inventory_profile()?.to_owned(),
                                );
                            }
                            let inventory = PausedRecoveryInventory {
                                runner_counts,
                                timer_counts: timer_counts.clone(),
                                synchronous_queue_counts: std::collections::BTreeMap::from([
                                    ("data_command".to_owned(), data_count as u64),
                                    ("trading_command".to_owned(), trading_count as u64),
                                ]),
                                adapter_profiles,
                                message_bus_mode: "local_only",
                            };
                            let value = collect(&snapshot, &inventory)?;
                            check(self)?;
                            guard.verify()?;
                            bus_inventory.verify()?;
                            anyhow::ensure!(
                                observer.coverage()? == coverage,
                                "observer changed during queue capture"
                            );
                            if let Some(proof) = &proof {
                                proof.verify()?;
                            }
                            let value = persist(value)?;
                            check(self)?;
                            guard.verify()?;
                            bus_inventory.verify()?;
                            anyhow::ensure!(
                                observer.coverage()? == coverage,
                                "observer changed during persistence"
                            );
                            if let Some(proof) = &proof {
                                proof.verify()?;
                            }
                            guard.finish()?;
                            Ok(value)
                        })
                    })
                },
            )
        }));
        match outcome {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => {
                ingress.invalidate();
                self.handle.stop();
                Err(error)
            }
            Err(panic) => {
                ingress.invalidate();
                self.handle.stop();
                std::panic::resume_unwind(panic)
            }
        }
    }

    fn ensure_recovery_start_permitted(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovery_requires_release,
            "recovered node requires independently verified recovery release; startup denied"
        );
        if let Some(runner) = &self.runner {
            runner.verify_ingress()?;
            let progress = runner.recovery_progress_handle().completion_watermark()?;
            anyhow::ensure!(
                progress.watermark.is_none() && progress.sent_watermark.is_none(),
                "runner recovery handoff requires independently verified recovery release; startup denied"
            );
        }
        Ok(())
    }

    /// Starts the live node without entering a select loop.
    ///
    /// Connects clients, runs reconciliation, and starts the trader, but does
    /// not consume the runner or drive channel receivers, so channel traffic arriving after
    /// startup is never serviced. This is a building block for tests and embedding, not a
    /// lifecycle: use [`run`](Self::run) or [`run_with_mode`](Self::run_with_mode) to run a node.
    ///
    /// # Errors
    ///
    /// Returns an error if startup fails.
    pub async fn start(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.kernel.trader.borrow().requires_callback_loop(),
            "queued strategy events require LiveNode::run or run_with_mode; standalone start has no callback loop"
        );
        self.ensure_recovery_start_permitted()?;
        if self.state().is_running() {
            anyhow::bail!("Already running");
        }
        #[cfg(feature = "dispatch-observer")]
        anyhow::ensure!(
            self.recovery_dispatch_queue.is_empty(),
            "cannot start with undrained recovery dispatch queue"
        );

        if let Some(runner) = self.runner.as_ref() {
            runner.close_recovery_handoff();
        }

        if self.external_msgbus.is_some() {
            log::warn!(
                "External message bus ingress is configured but LiveNode::start() does not service it; use LiveNode::run()"
            );
        }

        #[cfg(feature = "dispatch-observer")]
        let startup_input = self.native_lifecycle_input("startup.connect_reconcile_start")?;
        let startup_guard = begin_node_dispatch!(
            self,
            Lifecycle,
            &startup_input,
            Err(anyhow::anyhow!("startup lifecycle dispatch begin failed"))
        );
        self.prepare_cache().await?;

        if let Some(runner) = self.runner.as_ref() {
            runner.bind_senders_for_node(self.handle.clone());
        }

        self.handle.set_starting();

        self.kernel.reset_shutdown_flag();
        self.kernel.start_async().await;

        if self.event_store_halted() {
            return self
                .abort_startup("Event-store persistence halted during startup")
                .await;
        }

        if self.kernel.is_event_store_replay() {
            log::info!(
                "Event-store replay loaded; skipping live client connection and reconciliation",
            );

            if !self.finish_startup_replay().await? {
                return Ok(());
            }

            return Ok(());
        }

        if self.kernel.is_event_store_replay_configured() {
            self.abort_startup("Event-store replay did not start")
                .await?;
            return Ok(());
        }

        let connection_deadline = dst::time::Instant::now() + self.config.timeout_connection;

        // Connect data clients first and flush instrument events into cache
        if let Err(e) = self.connect_data_phase(connection_deadline).await {
            return self
                .abort_startup_with_error("Data client connection timed out", e)
                .await;
        }

        let mut pending = StartupPending::default();
        if let Err(error) = self.flush_installed_startup(&mut pending, true) {
            #[cfg(feature = "dispatch-observer")]
            drop(startup_guard);
            return self
                .abort_startup_with_error("Startup data dispatch failed", error)
                .await;
        }

        if let Err(e) = self.connect_exec_clients(connection_deadline).await {
            return self
                .abort_startup_with_error("Execution client connection timed out", e)
                .await;
        }

        if let Some(reason) = self.startup_abort_reason() {
            self.abort_startup(reason).await?;
            return Ok(());
        }

        let connection_status = match self
            .await_startup_engines_connected(&mut pending, None, connection_deadline)
            .await
        {
            Ok(status) => status,
            Err(error) => {
                #[cfg(feature = "dispatch-observer")]
                drop(startup_guard);
                return self
                    .abort_startup_with_error("Startup dispatch failed", error)
                    .await;
            }
        };
        match connection_status {
            EngineConnectionStatus::Connected => {}
            EngineConnectionStatus::TimedOut => {
                return self
                    .abort_startup_with_error(
                        "Engine readiness timed out",
                        anyhow::anyhow!("readiness timeout while waiting for engine connections"),
                    )
                    .await;
            }
            EngineConnectionStatus::StopRequested => {
                self.abort_startup("Stop signal received during startup")
                    .await?;
                return Ok(());
            }
            EngineConnectionStatus::ShutdownRequested => {
                self.abort_startup("Shutdown signal received during startup")
                    .await?;
                return Ok(());
            }
        }

        if let Err(e) = self.perform_startup_reconciliation().await {
            if let Err(finalize_err) = self.abort_startup("Startup reconciliation failed").await {
                anyhow::bail!(
                    "startup reconciliation failed: {e}; failed to finalize startup abort: {finalize_err}"
                );
            }

            return Err(e);
        }

        if let Some(reason) = self.startup_abort_reason() {
            self.abort_startup(reason).await?;
            return Ok(());
        }

        if let Err(e) = self.kernel.start_trader() {
            return self.abort_after_trader_start_failure(e).await;
        }

        #[cfg(feature = "plugin")]
        if let Err(e) = self.plugins.start_controllers() {
            return self.abort_after_trader_start_failure(e).await;
        }

        if let Err(error) = self.process_startup_system(&mut pending) {
            #[cfg(feature = "dispatch-observer")]
            drop(startup_guard);
            return self
                .abort_startup_with_error("Startup system dispatch failed", error)
                .await;
        }

        if !self.finish_startup_trader(None).await? {
            return Ok(());
        }

        complete_node_dispatch!(self, startup_guard);
        self.check_startup_dispatch()?;
        Ok(())
    }

    /// Stop the live node.
    ///
    /// This method stops the trader, waits for the configured grace period to allow
    /// residual events to be processed, then finalizes the shutdown sequence.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown fails or retained submissions remain unresolved at the end
    /// of `delay_post_stop`. Teardown still completes; subsequent events cannot clear this result.
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        if !self.state().is_running() {
            anyhow::bail!("Not running");
        }

        self.handle.set_shutting_down();

        #[cfg(feature = "plugin")]
        let controller_stop_result = self.plugins.stop_controllers();
        #[cfg(not(feature = "plugin"))]
        let controller_stop_result: anyhow::Result<()> = Ok(());

        self.kernel.stop_trader();
        let delay = self.kernel.delay_post_stop();
        log::info!("Awaiting residual events ({delay:?})...");

        let residual_events = self.process_runner_for(delay).await;
        if residual_events > 0 {
            log::debug!("Processed {residual_events} residual events during shutdown");
        }

        let stop_result = self.finalize_stop().await;
        let drained_events = self.drain_runner_pending();
        if drained_events > 0 {
            log::info!("Drained {drained_events} remaining events during shutdown");
        }

        match (controller_stop_result, stop_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(controller_err), Ok(())) => Err(controller_err),
            (Ok(()), Err(stop_err)) => Err(stop_err),
            (Err(controller_err), Err(stop_err)) => {
                log::error!("Error stopping plug-in controllers: {controller_err}");
                Err(stop_err)
            }
        }
    }

    /// Disposes the live node kernel and releases resources.
    ///
    /// Discards any retained runner messages and attempts callback cleanup. Logs latched callback
    /// failures and cleanup rejection; externally retained work can prevent clearing.
    pub fn dispose(&mut self) {
        self.close_external_ingress();
        self.handle.set_stopped();
        self.kernel.dispose();
        self.note_dispatch_gap("shutdown_lifecycle");
        drop(self.runner.take());

        if let Some(e) = actor::callback_failure() {
            log::error!("Callback dispatch failed before disposal cleanup: {e}");
        }

        if let Err(e) = actor::clear_callbacks() {
            log::error!("Failed to clear callback dispatch during disposal: {e}");
        }
    }

    async fn process_runner_for(&mut self, duration: Duration) -> usize {
        let Some(mut runner) = self.runner.take() else {
            dst::time::sleep(duration).await;
            return 0;
        };

        runner.bind_senders_for_node(self.handle.clone());
        let deadline = dst::time::Instant::now() + duration;
        let mut processed = 0;

        loop {
            tokio::select! {
                biased;

                () = dst::time::sleep_until(deadline) => break,
                event = runner.recv() => {
                    let Some(event) = event else {
                        dst::time::sleep_until(deadline).await;
                        break;
                    };

                    self.process_runner_event(event);
                    processed += 1;
                }
            }
        }

        self.runner = Some(runner);
        processed
    }

    fn drain_runner_pending(&mut self) -> usize {
        let Some(mut runner) = self.runner.take() else {
            return 0;
        };

        let processed = runner.poll_pending(|event| self.process_runner_event(event));
        self.runner = Some(runner);
        processed
    }

    fn process_runner_event(&mut self, event: PendingRunnerEvent) {
        match event {
            PendingRunnerEvent::TimeEvent(message) => {
                let _ = self.process_time_event(message);
            }
            PendingRunnerEvent::SystemEvent(event) => {
                event.dispatch(|event| self.process_system_event(event));
            }
            PendingRunnerEvent::SystemCommand(command) => {
                command.dispatch(|command| self.process_system_command(command));
            }
            PendingRunnerEvent::ExecEvent(event) => {
                event.dispatch(|event| self.process_exec_event(event));
            }
            PendingRunnerEvent::ExecCommand(command) => self.process_exec_command(command),
            PendingRunnerEvent::DataEvent(event) => self.process_data_event(event),
            PendingRunnerEvent::DataCommand(command) => self.process_data_command(command),
        }
    }

    #[cfg(test)]
    fn process_system_events(&self, events: Vec<DispatchMessage<SystemEvent>>) {
        for event in events {
            event.dispatch(|event| self.process_system_event(event));
        }
    }

    #[cfg(test)]
    fn process_system_commands(&self, commands: Vec<DispatchMessage<SystemCommand>>) {
        for command in commands {
            command.dispatch(|command| self.process_system_command(command));
        }
    }

    #[allow(clippy::unused_unit)]
    fn process_system_command(&self, command: SystemCommand) {
        let guard = begin_node_dispatch!(self, SystemCommand, &command, ());
        self.process_system_command_unobserved(command);
        complete_node_dispatch!(self, guard);
    }

    fn process_system_command_unobserved(&self, command: SystemCommand) {
        match command {
            SystemCommand::ReconnectSocket(command) => {
                self.process_socket_reconnect(command);
            }
        }
    }

    fn process_socket_reconnect(&self, command: ReconnectSocket) {
        let mut actual = SocketReconnectDispatchOutcome::Accepted;
        let result = nautilus_common::recovery_trace::native_transport(
            command.client_id.as_str(),
            "socket.request_reconnect",
            &serde_json::json!({"trader_id":command.trader_id,"client_id":command.client_id,
                "endpoint":command.endpoint,"ts_init":command.ts_init}),
            || {
                actual = if command.trader_id == self.config.trader_id {
                    Self::request_socket_reconnect(
                        self.socket_registry
                            .get(command.client_id, command.endpoint),
                    )
                } else {
                    SocketReconnectDispatchOutcome::InvalidTrader
                };
                if actual == SocketReconnectDispatchOutcome::Accepted {
                    Ok(())
                } else {
                    anyhow::bail!("native_socket_outcome:{actual:?}")
                }
            },
        );
        let outcome = match result {
            Ok(()) => SocketReconnectDispatchOutcome::Accepted,
            Err(error) if nautilus_common::recovery_trace::historical_active() => {
                // The original rejection is preserved as a local disposition. No handle is invoked.
                log::warn!("Historical socket command kept its original rejection: {error:#}");
                return;
            }
            Err(_) => actual,
        };

        if outcome == SocketReconnectDispatchOutcome::Accepted {
            log::info!(
                "Requested socket reconnect for client {} endpoint {}",
                command.client_id,
                command.endpoint
            );
        } else {
            log::warn!(
                "Rejected socket reconnect request for client {} endpoint {}: {outcome:?}",
                command.client_id,
                command.endpoint
            );
        }
    }

    fn request_socket_reconnect(lookup: SocketReconnectLookup) -> SocketReconnectDispatchOutcome {
        match lookup {
            SocketReconnectLookup::Handle(handle) => match handle.request_reconnect() {
                ReconnectRequestOutcome::Accepted => SocketReconnectDispatchOutcome::Accepted,
                ReconnectRequestOutcome::AlreadyReconnecting => {
                    SocketReconnectDispatchOutcome::AlreadyReconnecting
                }
                ReconnectRequestOutcome::Disconnected => {
                    SocketReconnectDispatchOutcome::Disconnected
                }
                ReconnectRequestOutcome::Closed => SocketReconnectDispatchOutcome::Closed,
                ReconnectRequestOutcome::Unsupported => SocketReconnectDispatchOutcome::Unsupported,
            },
            SocketReconnectLookup::ClientNotFound => SocketReconnectDispatchOutcome::UnknownClient,
            SocketReconnectLookup::Unsupported => SocketReconnectDispatchOutcome::Unsupported,
            SocketReconnectLookup::EndpointNotFound => {
                SocketReconnectDispatchOutcome::UnknownEndpoint
            }
            SocketReconnectLookup::AmbiguousEndpoint => {
                SocketReconnectDispatchOutcome::AmbiguousEndpoint
            }
        }
    }

    #[allow(clippy::unused_unit)]
    fn process_system_event(&self, event: SystemEvent) {
        let guard = begin_node_dispatch!(self, SystemEvent, &event, ());
        self.process_system_event_unobserved(event);
        complete_node_dispatch!(self, guard);
    }

    fn process_system_event_unobserved(&self, event: SystemEvent) {
        match event {
            SystemEvent::SocketState(change) => self.publish_socket_state_change(change),
        }
    }

    fn publish_socket_state_change(&self, change: SocketStateChange) {
        let timestamp = self.kernel.generate_timestamp_ns();

        let event = SocketStateChanged::new(
            self.config.trader_id,
            change.client_id,
            change.venue,
            change.endpoint,
            change.state,
            nautilus_common::recovery_trace::native_event_uuid(),
            timestamp,
            timestamp,
        );

        msgbus::publish_any(
            MessagingSwitchboard::socket_state_changed_topic(
                event.client_id,
                event.endpoint.as_str(),
            ),
            event.as_any(),
        );
    }

    /// Awaits engine clients to connect with timeout.
    ///
    /// Returns the final connection wait status.
    #[cfg(test)]
    async fn await_engines_connected(
        &self,
        deadline: dst::time::Instant,
    ) -> EngineConnectionStatus {
        log::info!(
            "Awaiting engine connections ({:?} timeout)...",
            self.config.timeout_connection
        );

        let interval = Duration::from_millis(100);

        loop {
            if self.handle.should_stop() {
                log::warn!("Stop signal received, aborting connection wait");
                return EngineConnectionStatus::StopRequested;
            }

            if self.kernel.is_shutdown_requested() {
                log::warn!("Shutdown signal received, aborting connection wait");
                return EngineConnectionStatus::ShutdownRequested;
            }

            if self.kernel.check_engines_connected() {
                log::info!("All engine clients connected");
                return EngineConnectionStatus::Connected;
            }

            let now = dst::time::Instant::now();
            if now >= deadline {
                break;
            }

            dst::time::sleep(interval.min(deadline - now)).await;
        }

        self.log_connection_status();
        EngineConnectionStatus::TimedOut
    }

    /// Awaits engine clients to disconnect with timeout.
    ///
    /// Returns an error with client status on timeout.
    async fn await_engines_disconnected(&self, deadline: dst::time::Instant) -> anyhow::Result<()> {
        log::info!(
            "Awaiting engine disconnections ({:?} timeout)...",
            self.config.timeout_disconnection
        );

        let timeout = self.config.timeout_disconnection;
        let interval = Duration::from_millis(100);

        loop {
            if self.kernel.check_engines_disconnected() {
                log::info!("All engine clients disconnected");
                return Ok(());
            }

            let now = dst::time::Instant::now();
            if now >= deadline {
                break;
            }

            dst::time::sleep(interval.min(deadline - now)).await;
        }

        log::error!(
            "Timed out ({:?}) waiting for engines to disconnect\n\
             DataEngine.check_disconnected() == {}\n\
             ExecEngine.check_disconnected() == {}",
            timeout,
            self.kernel.data_engine().check_disconnected(),
            self.kernel.exec_engine().borrow().check_disconnected(),
        );
        anyhow::bail!("disconnect readiness timeout while waiting for engine disconnections")
    }

    fn log_connection_status(&self) {
        let data_status = self.kernel.data_client_connection_status();
        let exec_status = self.kernel.exec_client_connection_status();

        let mut rows: Vec<ClientStatus> = Vec::new();

        for (client_id, connected) in data_status {
            rows.push(ClientStatus {
                client: client_id.to_string(),
                client_type: "Data",
                connected,
            });
        }

        for (client_id, connected) in exec_status {
            rows.push(ClientStatus {
                client: client_id.to_string(),
                client_type: "Execution",
                connected,
            });
        }

        let table = render_client_statuses(rows);

        log::warn!(
            "Timed out ({:?}) waiting for engines to connect\n\n{table}\n\n\
             DataEngine.check_connected() == {}\n\
             ExecEngine.check_connected() == {}",
            self.config.timeout_connection,
            self.kernel.data_engine().check_connected(),
            self.kernel.exec_engine().borrow().check_connected(),
        );
    }

    /// Performs startup reconciliation to align internal state with venue state.
    ///
    /// This method queries each execution client for mass status (orders, fills, positions)
    /// and reconciles any discrepancies with the local cache state.
    ///
    /// # Errors
    ///
    /// Returns an error if reconciliation fails or times out.
    async fn perform_startup_reconciliation(&mut self) -> anyhow::Result<()> {
        #[cfg(feature = "dispatch-observer")]
        let reconciliation_input = self.native_lifecycle_input("startup.reconciliation")?;
        let reconciliation_guard = begin_node_dispatch!(
            self,
            Reconciliation,
            &reconciliation_input,
            Err(anyhow::anyhow!(
                "startup reconciliation dispatch begin failed"
            ))
        );
        let clients = self.kernel.exec_engine.borrow().client_ids().len();
        self.handle
            .observe_startup_reconciliation(StartupReconciliationObservation {
                phase: StartupReconciliationPhase::Processing,
                observed_at_ns: self.kernel.clock.borrow().timestamp_ns().as_u64(),
                clients,
                reason: None,
            });
        let result = self.perform_startup_reconciliation_inner().await;
        self.handle
            .observe_startup_reconciliation(StartupReconciliationObservation {
                phase: if result.is_err() {
                    StartupReconciliationPhase::Failed
                } else if !self.config.exec_engine.reconciliation {
                    StartupReconciliationPhase::Disabled
                } else {
                    StartupReconciliationPhase::Processed
                },
                observed_at_ns: self.kernel.clock.borrow().timestamp_ns().as_u64(),
                clients,
                reason: result
                    .as_ref()
                    .err()
                    .map(|error| error.to_string().chars().take(1024).collect()),
            });
        if result.is_ok() {
            complete_node_dispatch!(self, reconciliation_guard);
        }
        result
    }

    #[expect(clippy::await_holding_refcell_ref)]
    async fn perform_startup_reconciliation_inner(&mut self) -> anyhow::Result<()> {
        if !self.config.exec_engine.reconciliation {
            log::info!("Startup reconciliation disabled");
            self.kernel
                .portfolio
                .borrow_mut()
                .initialize_wallet_orders()?;
            return Ok(());
        }

        log_info!(
            "Starting execution state reconciliation...",
            color = LogColor::Blue
        );

        let lookback_mins = self
            .config
            .exec_engine
            .reconciliation_lookback_mins
            .map(u64::from);

        let timeout = self.config.timeout_reconciliation;
        let start = dst::time::Instant::now();
        let client_ids = self.kernel.exec_engine.borrow().client_ids();

        for client_id in client_ids {
            let elapsed = start.elapsed();
            if elapsed >= timeout {
                anyhow::bail!("Startup reconciliation timeout reached");
            }

            let remaining = timeout
                .checked_sub(elapsed)
                .expect("elapsed checked against reconciliation timeout");

            log_info!(
                "Requesting mass status from {}...",
                client_id,
                color = LogColor::Blue
            );

            let mass_status_result = dst::time::timeout(remaining, async {
                self.kernel
                    .exec_engine
                    .borrow_mut()
                    .generate_mass_status(&client_id, lookback_mins)
                    .await
            })
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "Startup reconciliation timeout reached while requesting mass status from {client_id}"
                )
            })?;

            match mass_status_result {
                Ok(Some(mass_status)) => {
                    #[cfg(feature = "dispatch-observer")]
                    let report_input = self.native_mutation_input(
                        "query.startup_mass_status",
                        &(&client_id, &mass_status),
                    )?;
                    let report_guard = begin_node_dispatch!(
                        self,
                        QueryResult,
                        &report_input,
                        Err(anyhow::anyhow!("startup mass status dispatch begin failed"))
                    );
                    anyhow::ensure!(
                        mass_status.reports_complete(),
                        "Startup reconciliation requires complete mass status from {client_id}"
                    );
                    log_info!(
                        "Reconciling ExecutionMassStatus for {}",
                        client_id,
                        color = LogColor::Blue
                    );

                    let result = self
                        .exec_manager
                        .reconcile_execution_mass_status(&mass_status, &self.kernel.exec_engine);

                    anyhow::ensure!(
                        result.unresolved.is_empty(),
                        "Startup reconciliation unresolved for {client_id}: {:?}",
                        result.unresolved
                    );

                    anyhow::ensure!(
                        self.kernel
                            .exec_engine
                            .borrow()
                            .get_client(&client_id)
                            .is_some(),
                        "Execution client {client_id} disappeared during startup reconciliation",
                    );

                    anyhow::ensure!(
                        result.unresolved_positions.is_empty(),
                        "Unresolved positions during startup reconciliation for {client_id}: {}",
                        result.unresolved_positions.join("; "),
                    );

                    if result.events.is_empty() {
                        log_info!(
                            "Reconciliation for {} succeeded",
                            client_id,
                            color = LogColor::Blue
                        );
                    } else {
                        log::info!(
                            color = LogColor::Blue as u8;
                            "Reconciliation for {} processed {} events",
                            client_id,
                            result.events.len()
                        );
                    }

                    // Register external orders with execution clients for tracking
                    if !result.external_orders.is_empty() {
                        let exec_engine = self.kernel.exec_engine.borrow();

                        let source_client = exec_engine.get_client(&client_id).ok_or_else(|| {
                            anyhow::anyhow!(
                                "Execution client {client_id} disappeared during startup reconciliation"
                            )
                        })?;

                        for external in result.external_orders {
                            source_client.register_external_order(
                                external.client_order_id,
                                external.venue_order_id,
                                external.instrument_id,
                                external.strategy_id,
                                external.ts_init,
                            );
                        }
                    }
                    complete_node_dispatch!(self, report_guard);
                }
                Ok(None) => {
                    anyhow::bail!(
                        "Startup reconciliation requires mass status from {client_id}; no report received"
                    );
                }
                Err(e) => {
                    return Err(e).context(format!("Failed to get mass status from {client_id}"));
                }
            }
        }

        self.kernel.portfolio.borrow_mut().initialize_orders();
        self.kernel.portfolio.borrow_mut().initialize_positions();
        self.kernel
            .portfolio
            .borrow_mut()
            .initialize_wallet_orders()?;

        let elapsed_secs = start.elapsed().as_secs_f64();
        log_info!(
            "Startup reconciliation completed in {:.2}s",
            elapsed_secs,
            color = LogColor::Blue
        );

        Ok(())
    }

    /// Run the live node with automatic shutdown handling.
    ///
    /// This method starts the node, runs indefinitely, and handles graceful shutdown
    /// on interrupt signals.
    ///
    /// # Thread Safety
    ///
    /// The event loop runs directly on the current thread (not spawned) because the
    /// msgbus uses thread-local storage. Endpoints registered by the kernel are only
    /// accessible from the same thread.
    ///
    /// # Shutdown Sequence
    ///
    /// 1. Signal received (SIGINT, SIGTERM, or handle stop).
    /// 2. Trader components stopped (triggers order cancellations, etc.).
    /// 3. Event loop continues processing residual events for the configured grace period.
    /// 4. Kernel finalized, clients disconnected, remaining events drained.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start, encounters a runtime error, or shuts down
    /// with retained submissions unresolved at the end of `delay_post_stop`.
    pub async fn run(&mut self) -> anyhow::Result<()> {
        self.run_with_mode(NodeRunMode::Owned).await
    }

    /// Run the live node under the given mode.
    ///
    /// [`NodeRunMode::Hosted`] leaves signal handling to the host application. Every other
    /// responsibility, including maintenance, reconciliation, external ingress, and the shutdown
    /// sequence, is identical across modes so that hosted and owned nodes cannot diverge.
    ///
    /// # Errors
    ///
    /// Returns an error if the node fails to start, encounters a runtime error, or shuts down
    /// with retained submissions unresolved at the end of `delay_post_stop`.
    pub async fn run_with_mode(&mut self, mode: NodeRunMode) -> anyhow::Result<()> {
        #[cfg(feature = "dispatch-observer")]
        let observing_recovery =
            self.recovery_requires_release && self.recovery_observation.is_some();
        #[cfg(not(feature = "dispatch-observer"))]
        let observing_recovery = false;
        if observing_recovery {
            #[cfg(feature = "dispatch-observer")]
            self.install_recovery_observation_admission()?;
        } else {
            self.ensure_recovery_start_permitted()?;
        }
        if self.state().is_running() {
            anyhow::bail!("Already running");
        }

        if self.runner.is_none() {
            anyhow::bail!("Runner already consumed - run() called twice");
        }
        #[cfg(feature = "dispatch-observer")]
        anyhow::ensure!(
            self.recovery_dispatch_queue.is_empty(),
            "cannot run with undrained recovery dispatch queue"
        );

        #[cfg(feature = "dispatch-observer")]
        let startup_input = self.native_lifecycle_input("startup.connect_reconcile_start")?;
        let startup_guard = begin_node_dispatch!(
            self,
            Lifecycle,
            &startup_input,
            Err(anyhow::anyhow!("startup lifecycle dispatch begin failed"))
        );

        // Authenticated recovery cache must not be flushed/reloaded at observation startup.
        if !observing_recovery {
            self.prepare_cache().await?;
        }

        let Some(runner) = self.runner.take() else {
            anyhow::bail!("Runner already consumed - run() called twice");
        };

        runner.bind_senders_for_node(self.handle.clone());

        let AsyncRunnerChannels {
            mut time_evt_rx,
            mut system_evt_rx,
            mut system_cmd_rx,
            mut exec_evt_rx,
            mut exec_cmd_rx,
            mut data_evt_rx,
            mut data_cmd_rx,
        } = runner.take_channels();

        log::info!("Event loop starting");

        self.handle.set_starting();

        self.kernel.reset_shutdown_flag();
        self.kernel.start_async().await;

        if self.event_store_halted() {
            let result = self
                .abort_startup("Event-store persistence halted during startup")
                .await;
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
            log::info!("Event loop stopped");
            return result;
        }

        if self.kernel.is_event_store_replay() {
            log::info!(
                "Event-store replay loaded; skipping live client connection and reconciliation",
            );

            if !self.finish_startup_replay().await? {
                return Ok(());
            }

            return Ok(());
        }

        if self.kernel.is_event_store_replay_configured() {
            self.abort_startup("Event-store replay did not start")
                .await?;
            return Ok(());
        }

        let mut external_msgbus_rx = match self.take_external_ingress_receiver() {
            Ok(rx) => rx,
            Err(e) => {
                let result = self
                    .abort_startup("External message bus ingress failed to start")
                    .await;
                self.drain_channels(
                    &mut time_evt_rx,
                    &mut system_evt_rx,
                    &mut system_cmd_rx,
                    &mut exec_evt_rx,
                    &mut exec_cmd_rx,
                    &mut data_evt_rx,
                    &mut data_cmd_rx,
                );
                log::info!("Event loop stopped");

                if let Err(finalize_err) = result {
                    anyhow::bail!(
                        "failed to start external message bus ingress: {e}; failed to finalize startup abort: {finalize_err}"
                    );
                }

                return Err(e);
            }
        };

        let stop_handle = self.handle.clone();
        let mut pending = StartupPending::default();
        let connection_deadline = dst::time::Instant::now() + self.config.timeout_connection;
        let mut startup_receivers = RunnerReceivers {
            time_evt: &mut time_evt_rx,
            system_evt: &mut system_evt_rx,
            system_cmd: &mut system_cmd_rx,
            exec_evt: &mut exec_evt_rx,
            exec_cmd: &mut exec_cmd_rx,
            data_evt: &mut data_evt_rx,
            data_cmd: &mut data_cmd_rx,
        };

        // The connect future owns an engine borrow. Dequeues retain their original
        // receipts until that borrow ends, then actual Node handlers run.
        let data_connect_result = buffer_startup_events(
            self.connect_data_phase(connection_deadline),
            &mut pending,
            &mut startup_receivers,
        )
        .await;
        if let Err(error) = data_connect_result {
            #[cfg(feature = "dispatch-observer")]
            drop(startup_guard);
            return self
                .abort_startup_with_error("Data client connection timed out", error)
                .await;
        }
        if let Err(error) = self.flush_startup_data(&mut pending, &mut startup_receivers) {
            #[cfg(feature = "dispatch-observer")]
            drop(startup_guard);
            return self
                .abort_startup_with_error("Startup data dispatch failed", error)
                .await;
        }

        let exec_connect_result = buffer_startup_events(
            self.connect_exec_clients(connection_deadline),
            &mut pending,
            &mut startup_receivers,
        )
        .await;
        let engine_connection_result = match exec_connect_result {
            Ok(()) => {
                self.await_startup_engines_connected(
                    &mut pending,
                    Some(&mut startup_receivers),
                    connection_deadline,
                )
                .await
            }
            Err(error) => Err(error),
        };

        let engine_connection_status = match engine_connection_result {
            Ok(status) => status,
            Err(e) => {
                #[cfg(feature = "dispatch-observer")]
                drop(startup_guard);
                let result = self
                    .abort_startup_with_error("Execution client connection timed out", e)
                    .await;
                self.drain_channels(
                    &mut time_evt_rx,
                    &mut system_evt_rx,
                    &mut system_cmd_rx,
                    &mut exec_evt_rx,
                    &mut exec_cmd_rx,
                    &mut data_evt_rx,
                    &mut data_cmd_rx,
                );
                log::info!("Event loop stopped");
                return result;
            }
        };

        if engine_connection_status == EngineConnectionStatus::TimedOut {
            let result = self
                .abort_startup_with_error(
                    "Engine readiness timed out",
                    anyhow::anyhow!("readiness timeout while waiting for engine connections"),
                )
                .await;
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
            log::info!("Event loop stopped");
            return result;
        }

        if let Some(reason) = engine_connection_status
            .abort_reason()
            .or_else(|| self.startup_abort_reason())
        {
            self.abort_startup(reason).await?;
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
            log::info!("Event loop stopped");
            return Ok(());
        }

        debug_assert_eq!(engine_connection_status, EngineConnectionStatus::Connected);

        // Run reconciliation now that instruments are in cache and start trader
        if let Err(e) = self.perform_startup_reconciliation().await {
            let result = self.abort_startup("Startup reconciliation failed").await;
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
            log::info!("Event loop stopped");

            if let Err(finalize_err) = result {
                anyhow::bail!(
                    "startup reconciliation failed: {e}; failed to finalize startup abort: {finalize_err}"
                );
            }

            return Err(e);
        }

        if let Some(reason) = self.startup_abort_reason() {
            let result = self.abort_startup(reason).await;
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
            log::info!("Event loop stopped");
            return result;
        }

        if observing_recovery {
            #[cfg(feature = "dispatch-observer")]
            if let Err(error) = self.begin_recovery_input_observation() {
                return self
                    .abort_startup_with_error("Recovery input observation failed", error)
                    .await;
            }
            if let Err(error) = self.process_startup_system(&mut pending) {
                #[cfg(feature = "dispatch-observer")]
                drop(startup_guard);
                return self
                    .abort_startup_with_error("Startup system dispatch failed", error)
                    .await;
            }
        } else {
            if let Err(e) = self.kernel.start_trader() {
                let result = self.abort_after_trader_start_failure(e).await;
                self.drain_channels(
                    &mut time_evt_rx,
                    &mut system_evt_rx,
                    &mut system_cmd_rx,
                    &mut exec_evt_rx,
                    &mut exec_cmd_rx,
                    &mut data_evt_rx,
                    &mut data_cmd_rx,
                );
                log::info!("Event loop stopped");
                return result;
            }

            #[cfg(feature = "plugin")]
            if let Err(e) = self.plugins.start_controllers() {
                let result = self.abort_after_trader_start_failure(e).await;
                self.drain_channels(
                    &mut time_evt_rx,
                    &mut system_evt_rx,
                    &mut system_cmd_rx,
                    &mut exec_evt_rx,
                    &mut exec_cmd_rx,
                    &mut data_evt_rx,
                    &mut data_cmd_rx,
                );
                log::info!("Event loop stopped");
                return result;
            }

            if let Err(error) = self.process_startup_system(&mut pending) {
                #[cfg(feature = "dispatch-observer")]
                drop(startup_guard);
                return self
                    .abort_startup_with_error("Startup system dispatch failed", error)
                    .await;
            }

            let finish_result = {
                let mut receivers = RunnerReceivers {
                    time_evt: &mut time_evt_rx,
                    system_evt: &mut system_evt_rx,
                    system_cmd: &mut system_cmd_rx,
                    data_evt: &mut data_evt_rx,
                    data_cmd: &mut data_cmd_rx,
                    exec_evt: &mut exec_evt_rx,
                    exec_cmd: &mut exec_cmd_rx,
                };

                self.finish_startup_trader(Some(&mut receivers)).await
            };

            match finish_result {
                Ok(true) => {}
                result => {
                    log::info!("Event loop stopped");
                    return result.map(|_| ());
                }
            }
        }

        complete_node_dispatch!(self, startup_guard);

        let exec_config = &self.config.exec_engine;
        let inflight_interval =
            Duration::from_millis(u64::from(exec_config.inflight_check_interval_ms));

        let open_interval = exec_config
            .open_check_interval_secs
            .filter(|&s| s > 0.0)
            .map_or(Duration::ZERO, |secs| {
                Duration::from_nanos(secs_to_nanos_unchecked(secs))
            });

        let position_interval = exec_config
            .position_check_interval_secs
            .filter(|&s| s > 0.0)
            .map_or(Duration::ZERO, |secs| {
                Duration::from_nanos(secs_to_nanos_unchecked(secs))
            });

        let has_clients = !self
            .kernel
            .exec_engine
            .borrow()
            .get_all_clients()
            .is_empty();
        let recon_enabled = has_clients
            && (!inflight_interval.is_zero()
                || !open_interval.is_zero()
                || !position_interval.is_zero());

        let recon_min_interval = if recon_enabled {
            let mut intervals = Vec::new();

            if !inflight_interval.is_zero() {
                intervals.push(inflight_interval);
            }

            if !open_interval.is_zero() {
                intervals.push(open_interval);
            }

            if !position_interval.is_zero() {
                intervals.push(position_interval);
            }

            intervals
                .into_iter()
                .min()
                .unwrap_or(Duration::from_secs(1))
        } else {
            Duration::from_secs(1) // Unused, timer won't fire
        };

        // `reconciliation_startup_delay_secs` is a post-reconciliation grace period:
        // startup reconciliation has already completed above, and this delay offsets
        // the first periodic tick to let the system stabilize before continuous checks
        // begin.
        let startup_delay = if self.config.exec_engine.reconciliation {
            Duration::from_secs_f64(exec_config.reconciliation_startup_delay_secs)
        } else {
            Duration::ZERO
        };

        let recon_start = dst::time::Instant::now() + startup_delay;

        let mut last_inflight_check = dst::time::Instant::now();
        let mut last_open_check = last_inflight_check;
        let mut last_position_check = last_inflight_check;

        // Per-task `(interval, next_fire)` schedules dispatched by the
        // shared `maintenance_timer` below. See module docs for rationale.
        let far_future = Duration::from_hours(24 * 365 * 100);

        let make_schedule = |opt_dur: Option<Duration>| -> (Duration, dst::time::Instant) {
            let dur = opt_dur.unwrap_or(far_future);
            (dur, recon_start + dur)
        };

        let (recon_interval, mut recon_next) = make_schedule(if recon_enabled {
            Some(recon_min_interval)
        } else {
            None
        });

        let (purge_orders_interval, mut purge_orders_next) = make_schedule(
            exec_config
                .purge_closed_orders_interval_mins
                .filter(|&m| m > 0)
                .map(|m| Duration::from_secs(mins_to_secs(u64::from(m)))),
        );

        let (purge_positions_interval, mut purge_positions_next) = make_schedule(
            exec_config
                .purge_closed_positions_interval_mins
                .filter(|&m| m > 0)
                .map(|m| Duration::from_secs(mins_to_secs(u64::from(m)))),
        );

        let (purge_account_interval, mut purge_account_next) = make_schedule(
            exec_config
                .purge_account_events_interval_mins
                .filter(|&m| m > 0)
                .map(|m| Duration::from_secs(mins_to_secs(u64::from(m)))),
        );

        let (own_books_interval, mut own_books_next) = make_schedule(
            exec_config
                .own_books_audit_interval_secs
                .filter(|&s| s > 0.0)
                .map(Duration::from_secs_f64),
        );

        let (prune_fills_interval, mut prune_fills_next) =
            make_schedule(Some(Duration::from_mins(1)));

        let mut maintenance_timer = dst::time::interval(Duration::from_millis(100));
        maintenance_timer.set_missed_tick_behavior(dst::time::MissedTickBehavior::Skip);

        // Stop-check timer is not subject to the reconciliation startup delay,
        // so shutdown signals remain responsive from the moment the node reaches
        // `Running`. Set `MissedTickBehavior::Skip` so backlog ticks do not fire
        // a burst after the select arm was suspended by other branches.
        let mut stop_check_timer = dst::time::interval(Duration::from_millis(100));
        stop_check_timer.set_missed_tick_behavior(dst::time::MissedTickBehavior::Skip);

        // Running phase: runs until shutdown deadline expires
        let mut residual_events = 0usize;
        let mut open_order_report_task: Option<OpenOrderReportTask> = None;
        let mut targeted_order_report_task: Option<TargetedOrderReportTask> = None;
        let mut position_report_task: Option<PositionReportTask> = None;

        // A hosted node never installs signal handlers, so these futures stay pending and their
        // listeners are never registered. Both arms resolve to the same type as the real listeners.
        let owns_signals = mode.owns_signals();

        let ctrl_c = async move {
            if owns_signals {
                dst::signal::ctrl_c().await
            } else {
                std::future::pending::<std::io::Result<()>>().await
            }
        };

        let terminate = async move {
            if owns_signals {
                dst::signal::terminate().await
            } else {
                std::future::pending::<std::io::Result<()>>().await
            }
        };

        tokio::pin!(ctrl_c);
        tokio::pin!(terminate);

        let metrics = self.handle.metrics.clone();
        let metrics_start = dst::time::Instant::now();
        metrics.reset();

        let mut queue_monitor = self
            .config
            .queue_monitor
            .as_ref()
            .map(|config| QueueMonitor::new(config, metrics.snapshot()));
        let mut dispatches_since_yield = 0usize;
        let mut last_dispatch_was_maintenance = false;
        let mut next_runner_channel = 0usize;
        #[cfg(feature = "dispatch-observer")]
        let mut checkpoint_error = None;

        let dispatch_result: anyhow::Result<()> = loop {
            if let Err(error) = self.handle.verify_ingress_health() {
                #[cfg(feature = "dispatch-observer")]
                self.fail_native_dispatch(&format!("native ingress producer failed: {error:#}"));
                #[cfg(not(feature = "dispatch-observer"))]
                {
                    let _ = self.kernel.contain_native_failure(&format!(
                        "native ingress producer failed: {error:#}"
                    ));
                    self.handle.stop();
                }
                break Err(error);
            }
            let callbacks_pending = match drain_callbacks().await {
                Ok(pending) => pending,
                Err(error) => {
                    #[cfg(feature = "dispatch-observer")]
                    self.fail_native_dispatch(&format!("callback dispatch failed: {error}"));
                    let _ = self
                        .kernel
                        .prohibit_event_store_seal(&format!("callback dispatch failed: {error}"));
                    if self.state() == NodeState::Running {
                        self.initiate_shutdown();
                    }
                    log::warn!(
                        "Skipping residual events and final buffered dispatch after callback failure"
                    );
                    break Err(error.into());
                }
            };
            let shutdown_deadline = self.shutdown_deadline;
            let is_shutting_down = self.state() == NodeState::ShuttingDown;
            let is_running = matches!(self.state(), NodeState::Running | NodeState::Observing);
            #[cfg(feature = "dispatch-observer")]
            let checkpoint_pending_deadline = self.running_checkpoint_pending_response_deadline();
            #[cfg(not(feature = "dispatch-observer"))]
            let checkpoint_pending_deadline: Option<dst::time::Instant> = None;
            // A slow synchronous checkpoint can leave every next maintenance
            // tick ready. Preserve its priority, but never select it twice in
            // succession while an actual runner input is already queued.
            // Reading queue length neither dequeues nor changes FIFO/receipts.
            // Snapshot only readiness; each selected receiver still dequeues
            // its own original FIFO message/receipt in the normal branch.
            // A continuously ready fast timer must not starve native command,
            // report/data input or another channel. Empty snapshots leave all
            // receivers armed so a newly arriving message wakes the loop.
            let runner_channels_ready = [
                !time_evt_rx.dispatch_is_empty(),
                !system_evt_rx.dispatch_is_empty(),
                !system_cmd_rx.dispatch_is_empty(),
                !exec_evt_rx.dispatch_is_empty(),
                !exec_cmd_rx.dispatch_is_empty(),
                external_msgbus_rx
                    .as_ref()
                    .is_some_and(|rx| !rx.is_empty() || rx.is_closed()),
                !data_evt_rx.dispatch_is_empty(),
                !data_cmd_rx.dispatch_is_empty(),
            ];
            let selected_runner_channel =
                next_ready_runner_channel(&runner_channels_ready, next_runner_channel);
            let runner_input_ready = selected_runner_channel.is_some();
            let maintenance_can_dispatch = !last_dispatch_was_maintenance || !runner_input_ready;

            tokio::select! {
                biased;

                () = async {
                    match checkpoint_pending_deadline {
                        Some(deadline) => dst::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if is_running && checkpoint_pending_deadline.is_some() => {
                    #[cfg(feature = "dispatch-observer")]
                    if let Err(error) = self.expire_running_checkpoint_pending_responses() {
                        checkpoint_error = Some(error);
                        break Ok(());
                    }
                }

                // Signal branches first so they are always checked
                result = &mut ctrl_c, if is_running => {
                    match result {
                        Ok(()) => log::info!("Received SIGINT, shutting down"),
                        Err(e) => log::error!("Failed to listen for SIGINT: {e}"),
                    }
                    self.initiate_shutdown();
                }
                result = &mut terminate, if is_running => {
                    match result {
                        Ok(()) => log::info!("Received SIGTERM, shutting down"),
                        Err(e) => log::error!("Failed to listen for SIGTERM: {e}"),
                    }
                    self.initiate_shutdown();
                }
                _ = stop_check_timer.tick(), if is_running => {
                    if stop_handle.should_stop() {
                        log::info!("Received stop signal from handle");
                        self.initiate_shutdown();
                    } else if self.kernel.is_shutdown_requested() {
                        log::info!("Received ShutdownSystem command, shutting down");
                        self.initiate_shutdown();
                    } else if self.event_store_halted() {
                        if self.contain_event_store_failure {
                            let engine = self.kernel.exec_engine.borrow();
                            if !engine.submissions_fenced() {
                                log::error!("Event-store persistence halted; submissions fenced, remediation connections retained");
                                engine.fence_submissions();
                            }
                        } else {
                            log::error!("Event-store persistence halted; shutting down");
                            self.initiate_shutdown();
                        }
                    }
                }
                () = async {
                    match shutdown_deadline {
                        Some(deadline) => dst::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                }, if self.state() == NodeState::ShuttingDown => {
                    break Ok(());
                }
                () = std::future::ready(()), if callbacks_pending => {},
                result = async {
                    match open_order_report_task.as_mut() {
                        Some(task) => task.future.as_mut().await,
                        None => std::future::pending::<ReportTaskOutcome<OpenOrderReportResult>>().await,
                    }
                }, if open_order_report_task.is_some() => {
                    #[cfg(feature = "dispatch-observer")]
                    let query_input = self.native_mutation_input("query.open_order", &result)?;
                    let query_guard = begin_node_dispatch!(self, QueryResult, &query_input, Err(anyhow::anyhow!("query dispatch begin failed")));
                    let maintenance_start = dst::time::Instant::now();

                    drop(open_order_report_task.take());

                    targeted_order_report_task = self.apply_open_order_report_outcome(result)?;
                    complete_node_dispatch!(self, query_guard);
                    record_runner_maintenance(&metrics, maintenance_start, metrics_start);
                }
                result = async {
                    match targeted_order_report_task.as_mut() {
                        Some(task) => task.future.as_mut().await,
                        None => std::future::pending::<ReportTaskOutcome<Vec<TargetedOrderReportResult>>>().await,
                    }
                }, if targeted_order_report_task.is_some() => {
                    #[cfg(feature = "dispatch-observer")]
                    let query_input = self.native_mutation_input("query.targeted", &(&result, targeted_order_report_task.as_ref().map(|task| &task.planned_client_order_ids)))?;
                    let query_guard = begin_node_dispatch!(self, QueryResult, &query_input, Err(anyhow::anyhow!("query dispatch begin failed")));
                    let maintenance_start = dst::time::Instant::now();

                    let planned_client_order_ids = targeted_order_report_task
                        .as_ref()
                        .map(|task| task.planned_client_order_ids.clone())
                        .unwrap_or_default();
                    drop(targeted_order_report_task.take());

                    self.apply_targeted_report_outcome(result, &planned_client_order_ids)?;
                    complete_node_dispatch!(self, query_guard);
                    record_runner_maintenance(&metrics, maintenance_start, metrics_start);
                }
                result = async {
                    match position_report_task.as_mut() {
                        Some(task) => task.future.as_mut().await,
                        None => std::future::pending::<ReportTaskOutcome<PositionReportTaskResult>>().await,
                    }
                }, if position_report_task.is_some() => {
                    #[cfg(feature = "dispatch-observer")]
                    let query_input = self.native_mutation_input("query.position", &result)?;
                    let query_guard = begin_node_dispatch!(self, QueryResult, &query_input, Err(anyhow::anyhow!("query dispatch begin failed")));
                    let maintenance_start = dst::time::Instant::now();

                    drop(position_report_task.take());

                    position_report_task = self.apply_position_report_outcome(result)?;
                    complete_node_dispatch!(self, query_guard);
                    record_runner_maintenance(&metrics, maintenance_start, metrics_start);
                }

                // Bound both directions: maintenance keeps priority after each
                // runner input, then yields one turn to any already queued input.
                _ = maintenance_timer.tick(), if is_running && maintenance_can_dispatch => {
                    last_dispatch_was_maintenance = true;
                    let maintenance_now = dst::time::Instant::now();
                    #[cfg(feature = "dispatch-observer")]
                    let maintenance_input = self.native_mutation_input("maintenance.tick", &serde_json::json!({
                        "reconciliation_due":recon_enabled && maintenance_now >= recon_next,
                        "inflight_check_due":reconciliation::reconciliation_check_due(maintenance_now,last_inflight_check,inflight_interval),
                        "open_check_due":reconciliation::reconciliation_check_due(maintenance_now,last_open_check,open_interval),
                        "position_check_due":reconciliation::reconciliation_check_due(maintenance_now,last_position_check,position_interval),
                        "position_before_open":last_position_check < last_open_check,
                        "purge_orders_due":maintenance_now >= purge_orders_next,
                        "purge_positions_due":maintenance_now >= purge_positions_next,
                        "purge_account_due":maintenance_now >= purge_account_next,
                        "own_books_due":maintenance_now >= own_books_next,
                        "prune_fills_due":maintenance_now >= prune_fills_next,
                        "report_tasks":{"open_order":open_order_report_task.is_some(),"targeted_order":targeted_order_report_task.is_some(),"position":position_report_task.is_some()},
                        "queue_depths":RunnerChannelQueueDepths::from_receivers(&time_evt_rx,&exec_evt_rx,&exec_cmd_rx,&data_evt_rx,&data_cmd_rx),
                    }))?;
                    let maintenance_guard = begin_node_dispatch!(self, Maintenance, &maintenance_input, Err(anyhow::anyhow!("maintenance dispatch begin failed")));
                    let maintenance_start = dst::time::Instant::now();
                    metrics.publish_queue_depths(
                        RunnerChannelQueueDepths::from_receivers(
                            &time_evt_rx,
                            &exec_evt_rx,
                            &exec_cmd_rx,
                            &data_evt_rx,
                            &data_cmd_rx,
                        ),
                        metrics_start.elapsed(),
                    );

                    if let Some(queue_monitor) = queue_monitor.as_mut() {
                        let transitions = queue_monitor.evaluate(metrics.snapshot());
                        self.publish_queue_state_transitions(&transitions);
                    }

                    let now = maintenance_now;

                    if recon_enabled && now >= recon_next {
                        let recon_intervals = ReconciliationCheckIntervals {
                            inflight: inflight_interval,
                            open: open_interval,
                            position: position_interval,
                        };
                        let mut recon_state = ReconciliationCheckState {
                            last_inflight_check: &mut last_inflight_check,
                            last_open_check: &mut last_open_check,
                            last_position_check: &mut last_position_check,
                            open_order_report_task: &mut open_order_report_task,
                            targeted_order_report_task: &mut targeted_order_report_task,
                            position_report_task: &mut position_report_task,
                        };

                        self.run_reconciliation_checks(
                            now,
                            recon_intervals,
                            &mut recon_state,
                        );

                        recon_next = dst::time::Instant::now() + recon_interval;
                    }

                    if now >= purge_orders_next {
                        self.exec_manager.purge_closed_orders();
                        purge_orders_next = now + purge_orders_interval;
                    }

                    if now >= purge_positions_next {
                        self.exec_manager.purge_closed_positions();
                        purge_positions_next = now + purge_positions_interval;
                    }

                    if now >= purge_account_next {
                        self.exec_manager.purge_account_events();
                        purge_account_next = now + purge_account_interval;
                    }

                    if now >= own_books_next {
                        self.kernel.cache().borrow_mut().audit_own_order_books();
                        own_books_next = now + own_books_interval;
                    }

                    if now >= prune_fills_next {
                        self.exec_manager.prune_recent_fills_cache(60.0);
                        self.exec_manager.prune_processed_fills();
                        self.exec_manager.prune_order_local_activity();
                        prune_fills_next = now + prune_fills_interval;
                    }

                    complete_node_dispatch!(self, maintenance_guard);
                    record_runner_maintenance(&metrics, maintenance_start, metrics_start);
                }

                // Event processing branches. Exec commands and events are
                // ordered ahead of data events so a strategy action (cancel,
                // submit, etc.) is not delayed behind a market data backlog
                // when the biased select polls receivers each iteration.
                Some(handler) = time_evt_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 0) => {
                    next_runner_channel = 1 % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let dispatch_start = dst::time::Instant::now();
                    let dispatched = self.process_time_event(handler);

                    if dispatched && is_shutting_down {
                        log::debug!("Residual time event");
                        residual_events += 1;
                    }

                    if dispatched {
                        record_runner_dispatch(
                            &metrics,
                            SystemChannel::TimeEvents,
                            dispatch_start,
                            metrics_start,
                        );
                    }
                }
                Some(event) = system_evt_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 1) => {
                    next_runner_channel = (1 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    if is_shutting_down {
                        log::debug!("Residual system event: {event}");
                        residual_events += 1;
                    }
                    event.dispatch(|event| self.process_system_event(event));
                }
                Some(command) = system_cmd_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 2) => {
                    next_runner_channel = (2 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    if is_shutting_down {
                        log::debug!("Residual system command: {command}");
                        residual_events += 1;
                    }
                    command.dispatch(|command| self.process_system_command(command));
                }
                Some(evt) = exec_evt_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 3) => {
                    next_runner_channel = (3 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let dispatch_start = dst::time::Instant::now();

                    if is_shutting_down {
                        log::debug!("Residual exec event: {evt}");
                        residual_events += 1;
                    }

                    evt.dispatch(|evt| self.process_exec_event(evt));
                    record_runner_dispatch(
                        &metrics,
                        SystemChannel::ExecEvents,
                        dispatch_start,
                        metrics_start,
                    );
                }
                Some(cmd) = exec_cmd_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 4) => {
                    next_runner_channel = (4 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let dispatch_start = dst::time::Instant::now();

                    if is_shutting_down {
                        log::debug!("Residual exec command: {cmd}");
                        residual_events += 1;
                    }

                    self.process_exec_command(cmd);
                    record_runner_dispatch(
                        &metrics,
                        SystemChannel::ExecCommands,
                        dispatch_start,
                        metrics_start,
                    );
                }
                message = recv_external_msgbus_message(&mut external_msgbus_rx), if selected_runner_channel.is_none_or(|selected| selected == 5) => {
                    next_runner_channel = (5 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let external_msgbus_start = dst::time::Instant::now();

                    match message {
                        Some(message) => {
                            if is_shutting_down {
                                log::debug!("Residual external message bus message: {message}");
                                residual_events += 1;
                            }
                            self.process_external_message(&message);
                        }
                        None => {
                            if !self.note_dispatch_gap("external_ingress_closed") {
                                break Ok(());
                            }
                            log::info!("External message bus ingress closed");
                            external_msgbus_rx = None;
                            self.close_external_ingress();
                        }
                    }

                    record_runner_external_msgbus(
                        &metrics,
                        external_msgbus_start,
                        metrics_start,
                    );
                }
                Some(evt) = data_evt_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 6) => {
                    next_runner_channel = (6 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let dispatch_start = dst::time::Instant::now();

                    if is_shutting_down {
                        log::debug!("Residual data event: {evt:?}");
                        residual_events += 1;
                    }
                    self.process_data_event(evt);
                    record_runner_dispatch(
                        &metrics,
                        SystemChannel::DataEvents,
                        dispatch_start,
                        metrics_start,
                    );
                }
                Some(cmd) = data_cmd_rx.recv(), if selected_runner_channel.is_none_or(|selected| selected == 7) => {
                    next_runner_channel = (7 + 1) % runner_channels_ready.len();
                    last_dispatch_was_maintenance = false;
                    let dispatch_start = dst::time::Instant::now();

                    if is_shutting_down {
                        log::debug!("Residual data command: {cmd:?}");
                        residual_events += 1;
                    }
                    self.process_data_command(cmd);
                    record_runner_dispatch(
                        &metrics,
                        SystemChannel::DataCommands,
                        dispatch_start,
                        metrics_start,
                    );
                }
            }

            #[cfg(feature = "dispatch-observer")]
            let checkpoint_started = dst::time::Instant::now();
            #[cfg(feature = "dispatch-observer")]
            if let Err(error) = self.observation_after_completed_root(
                crate::runner::RunningReceivers {
                    time_events: &mut time_evt_rx,
                    system_events: &mut system_evt_rx,
                    system_commands: &mut system_cmd_rx,
                    execution_events: &mut exec_evt_rx,
                    execution_commands: &mut exec_cmd_rx,
                    data_events: &mut data_evt_rx,
                    data_commands: &mut data_cmd_rx,
                },
                open_order_report_task.is_some()
                    || targeted_order_report_task.is_some()
                    || position_report_task.is_some(),
            ) {
                checkpoint_error = Some(error);
                break Ok(());
            }

            #[cfg(feature = "dispatch-observer")]
            let checkpoint_occupied_tick =
                checkpoint_started.elapsed() >= Duration::from_millis(100);
            #[cfg(not(feature = "dispatch-observer"))]
            let checkpoint_occupied_tick = false;

            dispatches_since_yield += 1;
            if checkpoint_occupied_tick {
                dispatches_since_yield = 0;
                // All capture guards are released before this await. A merely
                // ready task/yield_now does not guarantee a current-thread
                // runtime polls its timer driver. Await one timer quantum after
                // a synchronous capture consumed an entire maintenance tick;
                // heartbeat, stop and existing deadline timers must progress.
                dst::time::sleep(Duration::from_millis(1)).await;
            } else if dispatches_since_yield >= DISPATCHES_PER_YIELD {
                dispatches_since_yield = 0;
                tokio::task::yield_now().await;
            }
        };

        if residual_events > 0 {
            log::debug!("Processed {residual_events} residual events during shutdown");
        }

        self.cancel_report_tasks(
            &mut open_order_report_task,
            &mut targeted_order_report_task,
            &mut position_report_task,
        );
        drop(external_msgbus_rx.take());
        let _ = self.kernel.cache().borrow().check_residuals();

        #[cfg(feature = "dispatch-observer")]
        if self.dispatch_failure.get() && checkpoint_error.is_none() {
            checkpoint_error = Some(anyhow::anyhow!(
                "native dispatch failed: run remains unsealed"
            ));
        }
        #[cfg(feature = "dispatch-observer")]
        if dispatch_result.is_ok()
            && checkpoint_error.is_none()
            && self.running_checkpoint.is_some()
        {
            let terminal_result =
                self.final_checkpoint_before_stop(crate::runner::RunningReceivers {
                    time_events: &mut time_evt_rx,
                    system_events: &mut system_evt_rx,
                    system_commands: &mut system_cmd_rx,
                    execution_events: &mut exec_evt_rx,
                    execution_commands: &mut exec_cmd_rx,
                    data_events: &mut data_evt_rx,
                    data_commands: &mut data_cmd_rx,
                });
            if let Err(error) = terminal_result {
                checkpoint_error = Some(error);
            }
        }
        #[cfg(feature = "dispatch-observer")]
        let may_seal = checkpoint_error.is_none() && dispatch_result.is_ok();
        #[cfg(feature = "dispatch-observer")]
        if let Some(error) = &checkpoint_error
            && let Err(retain_error) = self.kernel.prohibit_event_store_seal(&format!("{error:#}"))
        {
            log::error!("Failed to establish event-store Drop retention: {retain_error:#}");
        }
        #[cfg(not(feature = "dispatch-observer"))]
        let may_seal = dispatch_result.is_ok();
        let stop_result = self.disconnect_stop().await;
        #[cfg(feature = "dispatch-observer")]
        let terminal_cut = self.terminal_checkpoint.is_some();
        #[cfg(not(feature = "dispatch-observer"))]
        let terminal_cut = false;
        // No-checkpoint clients stay open until their transports have joined.
        // Their last emitted reports must be processed before seal as well.
        if !terminal_cut && dispatch_result.is_ok() {
            self.drain_channels(
                &mut time_evt_rx,
                &mut system_evt_rx,
                &mut system_cmd_rx,
                &mut exec_evt_rx,
                &mut exec_cmd_rx,
                &mut data_evt_rx,
                &mut data_cmd_rx,
            );
        }
        let stop_result = match stop_result {
            Ok(()) if may_seal => self.finish_stop_seal().await,
            Ok(()) => {
                self.kernel.finish_stop_without_seal().await?;
                Err(anyhow::anyhow!(
                    "failed final checkpoint: run remains unsealed"
                ))
            }
            Err(error) => {
                let _ = self.kernel.prohibit_event_store_seal(&format!("{error:#}"));
                let _ = self.kernel.finish_stop_without_seal().await;
                Err(error)
            }
        };
        self.handle.set_stopped();
        if let Err(error) = dispatch_result {
            if let Err(stop_error) = stop_result {
                let diagnostic = format!("{error}; {stop_error}");
                return Err(error.context(diagnostic));
            }
            return Err(error);
        }

        log::info!("Event loop stopped");

        #[cfg(feature = "dispatch-observer")]
        if let Some(error) = checkpoint_error {
            return Err(error);
        }

        stop_result
    }

    fn publish_queue_state_transitions(&self, transitions: &[QueueStateTransition]) {
        for transition in transitions {
            let topic = MessagingSwitchboard::queue_state_changed_topic(transition.channel);
            let timestamp = self.kernel.generate_timestamp_ns();

            let event = QueueStateChanged::new(
                self.config.trader_id,
                transition.channel,
                transition.condition,
                transition.state,
                transition.queue_depth,
                transition.mean_dispatch_ns,
                nautilus_common::recovery_trace::native_event_uuid(),
                timestamp,
                timestamp,
            );

            msgbus::publish_any(topic, event.as_any());
        }
    }

    #[expect(
        clippy::await_holding_refcell_ref,
        reason = "cache loading is serialized before the single-threaded live node starts"
    )]
    async fn prepare_cache(&mut self) -> anyhow::Result<()> {
        self.install_cache_database().await?;

        let cache = self.kernel.cache();
        if !cache.borrow().has_backing() {
            return Ok(());
        }

        if self
            .config
            .cache
            .as_ref()
            .is_some_and(|config| config.flush_on_start)
        {
            cache
                .borrow_mut()
                .flush_db()
                .context("Failed to flush persistent cache")?;
            return Ok(());
        }

        if self.config.exec_engine.load_cache {
            self.kernel
                .exec_engine()
                .borrow_mut()
                .load_cache()
                .await
                .context("Failed to load persistent cache")?;
        }

        Ok(())
    }

    /// Returns whether a cache database backing is configured but not yet installed.
    #[must_use]
    pub const fn has_pending_cache_database(&self) -> bool {
        self.cache_database_factory.is_some()
    }

    /// Constructs the configured cache database backing and installs it on the kernel cache.
    ///
    /// Construction is deferred to startup so the adapter is built inside the async runtime rather
    /// than by blocking the synchronous builder, and so the connection opens only when the node runs.
    async fn install_cache_database(&mut self) -> anyhow::Result<()> {
        let Some(factory) = self.cache_database_factory.as_ref() else {
            return Ok(());
        };

        let config = self.config.cache.clone().unwrap_or_default();

        // Cleared only after a successful install, so a failed startup can be retried rather than
        // silently starting without the backing the caller asked for.
        let database = factory
            .create(self.config.trader_id, self.kernel.instance_id, config)
            .await
            .context("failed to create cache database backing")?;
        self.kernel.cache().borrow_mut().set_database(database);
        self.cache_database_factory = None;

        Ok(())
    }

    fn take_external_ingress_receiver(
        &mut self,
    ) -> anyhow::Result<Option<tokio::sync::mpsc::Receiver<BusMessage>>> {
        let Some(external_ingress) = self.external_msgbus.as_mut() else {
            return Ok(None);
        };

        let receiver = external_ingress.take_receiver()?;
        log::info!("External message bus ingress started");
        Ok(Some(receiver))
    }

    fn republish_external_msgbus_message(message: &BusMessage) -> bool {
        if let Err(e) = msgbus::republish_external_message(message) {
            log::error!(
                "Failed to republish external message bus topic '{}': {e:#}",
                message.topic
            );
            return false;
        }
        true
    }

    fn process_external_msgbus_message(&self, message: &BusMessage) {
        if self.stream_processors.is_empty() {
            Self::republish_external_msgbus_message(message);
            return;
        }

        let mut process = |value: &dyn Any, mapping: &serde_json::Value| {
            for processor in &self.stream_processors {
                (processor.0)(value, mapping)?;
            }

            Ok(())
        };

        if let Err(e) = msgbus::process_external_typed_message(message, &mut process) {
            log::error!(
                "Failed to process external message bus topic '{}': {e:#}",
                message.topic
            );
        }
    }

    fn close_external_ingress(&mut self) {
        if let Some(external_ingress) = self.external_msgbus.as_mut()
            && !external_ingress.is_closed()
        {
            external_ingress.close();
        }
    }

    #[allow(clippy::unused_unit)]
    fn process_reconciliation_events(&mut self, events: &[OrderEventAny]) {
        #[cfg(feature = "dispatch-observer")]
        let owned = events.to_vec();
        let guard = begin_node_dispatch!(self, Reconciliation, &owned, ());
        self.process_reconciliation_events_unobserved(events);
        complete_node_dispatch!(self, guard);
    }

    fn process_reconciliation_events_unobserved(&mut self, events: &[OrderEventAny]) {
        if events.is_empty() {
            self.publish_submission_recovery_exhaustions();
            return;
        }

        log::info!(
            "Processing {} reconciliation event{}",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

        for event in events {
            self.exec_manager
                .record_local_activity(event.client_order_id());
            if let OrderEventAny::Filled(fill) = event {
                self.exec_manager
                    .record_position_activity(fill.instrument_id, fill.account_id);
            }

            self.kernel.exec_engine.borrow_mut().process(event);
            self.exec_manager
                .confirm_submission_outcome(&event.client_order_id());
            if let OrderEventAny::Filled(fill) = event {
                self.exec_manager.commit_recent_fill_if_applied(fill);
            }
        }

        self.publish_submission_recovery_exhaustions();
    }

    fn publish_submission_recovery_exhaustions(&mut self) {
        for diagnostic in self.exec_manager.take_submission_recovery_exhaustions() {
            log::warn!(
                "Submission recovery exhausted for {}: {:?} after {} checks; submission remains unresolved pending venue evidence",
                diagnostic.client_order_id,
                diagnostic.source,
                diagnostic.retry_count,
            );
            msgbus::publish_any(
                MessagingSwitchboard::submission_recovery_exhausted_topic(),
                &diagnostic,
            );
        }
    }

    #[allow(clippy::unused_unit)]
    fn process_exec_event(&mut self, event: ExecutionEvent) {
        let guard = begin_node_dispatch!(self, ExecutionEvent, &event, ());
        let dispatched = self.process_exec_event_unobserved(event);
        #[cfg(feature = "dispatch-observer")]
        if !dispatched {
            if let Some(guard) = guard {
                let _ = guard.rejected();
                self.fail_native_dispatch("native execution event was rejected");
            }
            return;
        }
        #[cfg(not(feature = "dispatch-observer"))]
        let _ = dispatched;
        complete_node_dispatch!(self, guard);
    }

    fn process_exec_event_unobserved(&mut self, event: ExecutionEvent) -> bool {
        let clear_closed_orders = matches!(
            &event,
            ExecutionEvent::Order(_) | ExecutionEvent::OrderCanceledBatch(_)
        );
        let closed_report_id = match &event {
            ExecutionEvent::Report(
                ExecutionReport::Order(report) | ExecutionReport::OrderWithFills(report, _),
            ) if report.order_status.is_closed() => report.client_order_id,
            _ => None,
        };
        let Some(order_ids) = self.observe_exec_event_before_dispatch(&event) else {
            return false;
        };

        self.dispatch_exec_event_and_commit_fill(event);

        for client_order_id in &order_ids {
            self.exec_manager
                .confirm_submission_outcome(client_order_id);
            let is_closed = self
                .kernel
                .cache()
                .borrow()
                .order(client_order_id)
                .is_some_and(|order| order.is_closed());

            if is_closed && (clear_closed_orders || closed_report_id == Some(*client_order_id)) {
                self.exec_manager
                    .clear_recon_tracking(client_order_id, true);
            }
        }
        true
    }

    #[allow(clippy::unused_unit)]
    fn process_exec_command(&mut self, message: impl Into<DispatchMessage<TradingCommandMessage>>) {
        let message = message.into();
        let guard = begin_node_dispatch!(self, ExecutionCommand, message.message(), ());
        let admitted = message.dispatch_trading_with_admission(|message| {
            if message.endpoint() == MessagingSwitchboard::exec_engine_execute() {
                self.observe_exec_command_before_dispatch(message.command())
            } else {
                Ok(true)
            }
        });
        if let Err(error) = admitted {
            let reason = format!("execution command admission failed: {error:#}");
            log::error!("{reason}");
            self.kernel.exec_engine.borrow().fence_submissions();
            #[cfg(feature = "dispatch-observer")]
            self.fail_native_dispatch(&reason);
            #[cfg(not(feature = "dispatch-observer"))]
            {
                if let Err(error) = self.kernel.contain_native_failure(&reason) {
                    log::error!("Execution admission failure retention refused: {error:#}");
                }
                self.handle.stop();
            }
            // Dropping the uncompleted native guard preserves the failure;
            // an admission error must never publish a Completed input.
            return;
        }
        complete_node_dispatch!(self, guard);
    }

    /// Dispatches a normal-ingress execution event, then commits a direct
    /// `OrderFilled` to the recent-fills dedup cache only once it is present on
    /// its canonical order.
    ///
    /// The fill candidate is captured from `&evt` BEFORE the value is moved into
    /// [`AsyncRunner::handle_exec_event`]; the gated commit runs AFTER dispatch,
    /// so a fill the execution engine rejects (unknown order, invalid
    /// transition) is never marked and its later `Fill` report stays eligible.
    fn dispatch_exec_event_and_commit_fill(&mut self, evt: ExecutionEvent) {
        let recent_fill_candidate = match &evt {
            ExecutionEvent::Order(OrderEventAny::Filled(fill)) => Some(fill.clone()),
            _ => None,
        };

        AsyncRunner::handle_exec_event(evt);

        if let Some(fill) = &recent_fill_candidate {
            self.exec_manager.commit_recent_fill_if_applied(fill);
        }
    }

    async fn connect_data_phase(&mut self, deadline: dst::time::Instant) -> anyhow::Result<()> {
        // A zero remaining budget still admits an immediately-ready connect (an
        // empty/ready client set completes on the first poll); a pending connect
        // fails closed on that same poll. This keeps a zero `timeout_connection`
        // - a supported "do not wait, but allow ready work" configuration -
        // working, while still bounding a hung connect.
        let remaining = deadline.saturating_duration_since(dst::time::Instant::now());
        dst::time::timeout(remaining, self.kernel.connect_data_clients())
            .await
            .map_err(|_| anyhow::anyhow!("data-connect timeout"))
    }

    async fn connect_exec_clients(&mut self, deadline: dst::time::Instant) -> anyhow::Result<()> {
        let remaining = deadline.saturating_duration_since(dst::time::Instant::now());
        dst::time::timeout(remaining, self.kernel.connect_exec_clients())
            .await
            .map_err(|_| anyhow::anyhow!("exec-connect timeout"))
    }

    fn startup_abort_reason(&self) -> Option<&'static str> {
        if self.handle.should_stop() {
            Some("Stop signal received during startup")
        } else if self.kernel.is_shutdown_requested() {
            Some("Shutdown signal received during startup")
        } else {
            None
        }
    }

    async fn finish_startup_replay(&mut self) -> anyhow::Result<bool> {
        match self.handle.try_set_running() {
            RunningTransition::Entered => Ok(true),
            RunningTransition::StopRequested => {
                self.abort_startup("Stop signal received during startup")
                    .await?;
                Ok(false)
            }
            RunningTransition::Invalid(control) => {
                self.abort_startup_with_error(
                    "Invalid lifecycle state during startup",
                    anyhow::anyhow!(
                        "Invalid LiveNode control state {control:#04x} while entering Running"
                    ),
                )
                .await?;
                Ok(false)
            }
        }
    }

    async fn finish_startup_trader(
        &mut self,
        receivers: Option<&mut RunnerReceivers<'_>>,
    ) -> anyhow::Result<bool> {
        match self.handle.try_set_running() {
            RunningTransition::Entered => Ok(true),
            RunningTransition::StopRequested => {
                self.abort_started_trader("Stop signal received during startup", receivers)
                    .await?;
                Ok(false)
            }
            RunningTransition::Invalid(control) => {
                let state_err = anyhow::anyhow!(
                    "Invalid LiveNode control state {control:#04x} while entering Running"
                );

                match self
                    .abort_started_trader("Invalid lifecycle state during startup", receivers)
                    .await
                {
                    Ok(()) => Err(state_err),
                    Err(finalize_err) => {
                        anyhow::bail!(
                            "{state_err}; failed to finalize startup abort: {finalize_err}"
                        )
                    }
                }
            }
        }
    }

    async fn abort_startup(&mut self, reason: &str) -> anyhow::Result<()> {
        log::info!("{reason}, aborting startup");
        self.handle.set_shutting_down();
        self.finalize_stop().await
    }

    async fn abort_startup_with_error(
        &mut self,
        reason: &str,
        startup_err: anyhow::Error,
    ) -> anyhow::Result<()> {
        match self.abort_startup(reason).await {
            Ok(()) => Err(startup_err),
            Err(finalize_err) => {
                anyhow::bail!("{startup_err}; failed to finalize startup abort: {finalize_err}")
            }
        }
    }

    async fn abort_started_trader(
        &mut self,
        reason: &str,
        mut receivers: Option<&mut RunnerReceivers<'_>>,
    ) -> anyhow::Result<()> {
        log::info!("{reason}, aborting startup");
        self.handle.set_shutting_down();

        #[cfg(feature = "plugin")]
        let controller_stop_result = self.plugins.stop_controllers();
        #[cfg(not(feature = "plugin"))]
        let controller_stop_result: anyhow::Result<()> = Ok(());

        let trader_stop_result = self.kernel.stop_trader_after_start_failure();
        let delay = self.kernel.delay_post_stop();
        log::info!("Awaiting residual events ({delay:?})...");

        let residual_events = match receivers.as_mut() {
            Some(receivers) => self.process_receivers_for(delay, receivers).await,
            None => self.process_runner_for(delay).await,
        };

        if residual_events > 0 {
            log::debug!("Processed {residual_events} residual events during shutdown");
        }

        let finalize_result = self.finalize_stop().await;

        if let Some(receivers) = receivers {
            self.drain_channels(
                receivers.time_evt,
                receivers.system_evt,
                receivers.system_cmd,
                receivers.exec_evt,
                receivers.exec_cmd,
                receivers.data_evt,
                receivers.data_cmd,
            );
        } else {
            let drained_events = self.drain_runner_pending();
            if drained_events > 0 {
                log::info!("Drained {drained_events} remaining events during shutdown");
            }
        }

        let mut errors = Vec::new();

        if let Err(e) = controller_stop_result {
            errors.push(format!("Failed to stop plug-in controllers: {e}"));
        }

        if let Err(e) = trader_stop_result {
            errors.push(format!("Failed to stop trader: {e}"));
        }

        if let Err(e) = finalize_result {
            errors.push(format!("Failed to finalize startup abort: {e}"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("{}", errors.join("; "))
        }
    }

    async fn process_receivers_for(
        &mut self,
        duration: Duration,
        receivers: &mut RunnerReceivers<'_>,
    ) -> usize {
        let deadline = dst::time::Instant::now() + duration;
        let mut processed = 0;

        loop {
            tokio::select! {
                biased;

                () = dst::time::sleep_until(deadline) => break,
                Some(message) = receivers.time_evt.recv() => {
                    let _ = self.process_time_event(message);
                    processed += 1;
                }
                Some(event) = receivers.system_evt.recv() => {
                    event.dispatch(|event| self.process_system_event(event));
                    processed += 1;
                }
                Some(command) = receivers.system_cmd.recv() => {
                    command.dispatch(|command| self.process_system_command(command));
                    processed += 1;
                }
                Some(event) = receivers.exec_evt.recv() => {
                    event.dispatch(|event| self.process_exec_event(event));
                    processed += 1;
                }
                Some(command) = receivers.exec_cmd.recv() => {
                    self.process_exec_command(command);
                    processed += 1;
                }
                Some(event) = receivers.data_evt.recv() => {
                    self.process_data_event(event);
                    processed += 1;
                }
                Some(command) = receivers.data_cmd.recv() => {
                    self.process_data_command(command);
                    processed += 1;
                }
            }
        }

        processed
    }

    async fn abort_after_trader_start_failure(
        &mut self,
        start_err: anyhow::Error,
    ) -> anyhow::Result<()> {
        log::info!("Trader startup failed, aborting startup");
        self.handle.set_shutting_down();
        let stop_result = self.kernel.stop_trader_after_start_failure();
        let finalize_result = self.finalize_stop().await;

        match (stop_result, finalize_result) {
            (Ok(()), Ok(())) => Err(start_err),
            (Err(stop_err), Ok(())) => anyhow::bail!(
                "Failed during trader startup: {start_err}; failed to stop partial trader start: \
                 {stop_err}"
            ),
            (Ok(()), Err(finalize_err)) => anyhow::bail!(
                "Failed during trader startup: {start_err}; failed to finalize startup abort: \
                 {finalize_err}"
            ),
            (Err(stop_err), Err(finalize_err)) => anyhow::bail!(
                "Failed during trader startup: {start_err}; failed to stop partial trader start: \
                 {stop_err}; failed to finalize startup abort: {finalize_err}"
            ),
        }
    }

    fn initiate_shutdown(&mut self) {
        // Fence before lifecycle encoding, persistence and stop callbacks can
        // await or spawn work. Historical stop replay invokes Trader's replay
        // handler directly and does not enter this current-process boundary.
        self.handle.fence_transmissions();
        #[cfg(feature = "dispatch-observer")]
        let input = match self.native_lifecycle_input("stop.trader") {
            Ok(input) => input,
            Err(error) => {
                log::error!("Stop lifecycle input failed: {error:#}");
                #[cfg(feature = "dispatch-observer")]
                self.fail_native_dispatch(&format!("stop lifecycle encoding failed: {error:#}"));
                #[cfg(not(feature = "dispatch-observer"))]
                self.handle.stop();
                return;
            }
        };
        let guard = begin_node_dispatch!(self, Lifecycle, &input);
        #[cfg(feature = "plugin")]
        if let Err(e) = self.plugins.stop_controllers() {
            log::error!("Error stopping plug-in controllers: {e}");
        }

        self.kernel.stop_trader();
        let delay = self.kernel.delay_post_stop();
        log::info!("Awaiting residual events ({delay:?})...");

        self.shutdown_deadline = Some(dst::time::Instant::now() + delay);
        self.handle.set_shutting_down();
        complete_node_dispatch!(self, guard);
    }

    async fn finalize_stop(&mut self) -> anyhow::Result<()> {
        if let Err(error) = self.disconnect_stop().await {
            // A failed disconnect can never certify the final boundary, but
            // must still stop engines and timers before exposing Stopped.
            let mut errors = vec![error.to_string()];
            if let Err(retention_error) =
                self.kernel.prohibit_event_store_seal(&format!("{error:#}"))
            {
                errors.push(format!(
                    "failed to retain unsealed shutdown: {retention_error:#}"
                ));
            }
            if let Err(cleanup_error) = self.kernel.finish_stop_without_seal().await {
                errors.push(format!(
                    "failed while cleaning up shutdown: {cleanup_error:#}"
                ));
            }
            self.handle.set_stopped();
            anyhow::bail!("{}", errors.join("; "));
        }
        self.drain_runner_pending();
        #[cfg(feature = "dispatch-observer")]
        if self.running_checkpoint.is_some() && self.terminal_checkpoint.is_none() {
            let _ = self
                .kernel
                .prohibit_event_store_seal("shutdown without persisted terminal boundary");
            self.kernel.finish_stop_without_seal().await?;
            self.handle.set_stopped();
            anyhow::bail!("shutdown has no persisted terminal boundary; run remains unsealed");
        }
        self.finish_stop_seal().await?;
        self.handle.set_stopped();
        Ok(())
    }
    async fn disconnect_stop(&mut self) -> anyhow::Result<()> {
        #[cfg(feature = "dispatch-observer")]
        let terminal = self.terminal_checkpoint.is_some();
        #[cfg(not(feature = "dispatch-observer"))]
        let terminal = false;
        let input = self.native_lifecycle_input("stop.disconnect_finalize")?;
        #[cfg(feature = "dispatch-observer")]
        let guard = if terminal {
            None
        } else {
            match self.begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &input) {
                Ok(guard) => guard,
                Err(error) => {
                    log::error!("Shutdown observer failed; preserving unsealed run: {error:#}");
                    let _ = self.kernel.prohibit_event_store_seal(&format!("{error:#}"));
                    None
                }
            }
        };
        #[cfg(not(feature = "dispatch-observer"))]
        let guard = {
            let _ = (input, terminal);
            None::<()>
        };
        let unresolved_submissions = self.exec_manager.unresolved_submission_ids();
        self.close_external_ingress();

        let timeout = self.config.timeout_disconnection;
        let deadline = dst::time::Instant::now() + timeout;

        let mut errors = Vec::new();
        let (report_result, disconnect_result) = futures_util::join!(
            dst::time::timeout(
                timeout,
                Self::finish_report_tasks(&self.exec_clients, &mut errors)
            ),
            dst::time::timeout(timeout, self.kernel.disconnect_clients()),
        );

        if report_result.is_err() {
            errors.push("report collection shutdown timeout".to_string());
        }

        let disconnect_result = match disconnect_result {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!(
                "disconnect timeout while disconnecting clients"
            )),
        };

        if let Err(ref e) = disconnect_result {
            log::error!("Error disconnecting clients: {e}");
        }

        let readiness_result = self.await_engines_disconnected(deadline).await;

        if !unresolved_submissions.is_empty() {
            errors.push(format!(
                "Submission recovery incomplete at shutdown: unresolved client order IDs {unresolved_submissions:?}"
            ));
        }

        if let Err(e) = disconnect_result {
            errors.push(e.to_string());
        }

        if let Err(e) = readiness_result {
            errors.push(format!("failed while awaiting engine disconnection: {e}"));
        }

        if self.event_store_halted() {
            errors.push("event-store persistence halted".into());
        }

        if errors.is_empty() {
            complete_node_dispatch!(self, guard);
            Ok(())
        } else {
            anyhow::bail!("{}", errors.join("; "))
        }
    }

    async fn finish_stop_seal(&mut self) -> anyhow::Result<()> {
        #[cfg(feature = "dispatch-observer")]
        if let Some(receipt) = &self.terminal_checkpoint {
            let proof = self
                .dispatch_observer
                .as_ref()
                .context("terminal observer missing")?
                .completed_root_boundary_proof()?
                .context("terminal completed root missing")?;
            anyhow::ensure!(
                receipt.node_instance == self.kernel.instance_id
                    && receipt.root_sequence == proof.root_sequence(),
                "terminal frontier changed before seal"
            );
            proof.verify()?;
        }
        anyhow::ensure!(
            !self.event_store_halted(),
            "event store halted before Stop seal"
        );
        self.kernel
            .finalize_stop()
            .await
            .map_err(|error| anyhow::anyhow!("failed while finalizing kernel shutdown: {error:#}"))
    }

    #[cfg(feature = "dispatch-observer")]
    fn record_shutdown_discarded(
        &self,
        source: crate::dispatch::DispatchSource,
        input: &dyn std::any::Any,
    ) -> anyhow::Result<()> {
        if let Some(observer) = &self.dispatch_observer {
            #[cfg(feature = "native-tail-replay")]
            if observer.native_trace().is_some() {
                // The native Journal has no verified discarded-input disposition.
                // Keep the original dequeued receipt and all later FIFO members;
                // containment must precede encoding or advancing either protocol.
                let reason = "native trace shutdown discard disposition unsupported";
                self.fail_native_dispatch(reason);
                anyhow::bail!(reason);
            }
            observer.record_discarded(source, &format!("{:?}", self.state()), input)?;
        }
        Ok(())
    }

    #[allow(clippy::unused_unit)]
    #[expect(
        clippy::too_many_arguments,
        reason = "shutdown drains each runner receiver without changing channel ownership"
    )]
    fn drain_channels(
        &self,
        time_evt_rx: &mut SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
        system_evt_rx: &mut SnapshotReceiver<DispatchMessage<SystemEvent>>,
        system_cmd_rx: &mut SnapshotReceiver<DispatchMessage<SystemCommand>>,
        exec_evt_rx: &mut SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
        exec_cmd_rx: &mut SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
        data_evt_rx: &mut SnapshotReceiver<DispatchMessage<DataEvent>>,
        data_cmd_rx: &mut SnapshotReceiver<DispatchMessage<DataCommand>>,
    ) {
        let mut drained = 0;

        while let Ok(handler) = time_evt_rx.try_recv() {
            let _ = self.process_time_event(handler);
            drained += 1;
        }

        while let Ok(event) = system_evt_rx.try_recv() {
            #[cfg(feature = "dispatch-observer")]
            if let Err(error) = self.record_shutdown_discarded(
                crate::dispatch::DispatchSource::SystemEvent,
                event.message(),
            ) {
                self.fail_native_dispatch(&format!("shutdown discard recording failed: {error:#}"));
                return;
            }
            #[cfg(not(feature = "dispatch-observer"))]
            let _ = event;
            // Reconnection and other system mutations are no longer admissible
            // after the final runner boundary. Retain the discarded input only.
            drained += 1;
        }
        while let Ok(command) = system_cmd_rx.try_recv() {
            #[cfg(feature = "dispatch-observer")]
            if let Err(error) = self.record_shutdown_discarded(
                crate::dispatch::DispatchSource::SystemCommand,
                command.message(),
            ) {
                self.fail_native_dispatch(&format!("shutdown discard recording failed: {error:#}"));
                return;
            }
            #[cfg(not(feature = "dispatch-observer"))]
            let _ = command;
            drained += 1;
        }

        while let Ok(evt) = data_evt_rx.try_recv() {
            self.process_data_event(evt);
            drained += 1;
        }

        while let Ok(cmd) = data_cmd_rx.try_recv() {
            self.process_data_command(cmd);
            drained += 1;
        }

        while let Ok(evt) = exec_evt_rx.try_recv() {
            evt.dispatch(|evt| {
                let guard = begin_node_dispatch!(self, ExecutionEvent, &evt, ());
                AsyncRunner::handle_exec_event(evt);
                complete_node_dispatch!(self, guard);
            });
            drained += 1;
        }

        while let Ok(cmd) = exec_cmd_rx.try_recv() {
            let guard = begin_node_dispatch!(self, ExecutionCommand, cmd.message(), ());
            AsyncRunner::handle_trading_command(cmd);
            complete_node_dispatch!(self, guard);
            drained += 1;
        }

        if drained > 0 {
            log::info!("Drained {drained} remaining events during shutdown");
        }
    }

    fn observe_exec_event_before_dispatch(
        &mut self,
        evt: &ExecutionEvent,
    ) -> Option<Vec<ClientOrderId>> {
        let mut order_ids = Vec::new();

        match evt {
            ExecutionEvent::Order(order_evt) => {
                self.exec_manager.observe_order_event(order_evt);
                order_ids.push(order_evt.client_order_id());
            }
            ExecutionEvent::OrderSubmittedBatch(batch) => {
                for submitted in &batch.events {
                    self.exec_manager
                        .record_local_activity(submitted.client_order_id);
                }
            }
            ExecutionEvent::OrderAcceptedBatch(batch) => {
                for accepted in &batch.events {
                    self.exec_manager
                        .observe_order_event(&OrderEventAny::Accepted(*accepted));
                    order_ids.push(accepted.client_order_id);
                }
            }
            ExecutionEvent::OrderCanceledBatch(batch) => {
                for canceled in &batch.events {
                    self.exec_manager
                        .observe_order_event(&OrderEventAny::Canceled(*canceled));
                    order_ids.push(canceled.client_order_id);
                }
            }
            ExecutionEvent::Report(report) => {
                if let ExecutionReport::Fill(fill_report) = report
                    && self.exec_manager.is_fill_recently_processed(
                        fill_report.account_id,
                        fill_report.instrument_id,
                        fill_report.trade_id,
                    )
                {
                    log::debug!(
                        "Skipping recently processed fill report: {}",
                        fill_report.trade_id,
                    );
                    return None;
                }

                self.exec_manager.observe_execution_report(report);

                if let Some(client_order_id) = self.order_report_client_order_id(report) {
                    order_ids.push(client_order_id);
                }
            }
            ExecutionEvent::Account(_) => {}
        }

        Some(order_ids)
    }

    fn order_report_client_order_id(&self, report: &ExecutionReport) -> Option<ClientOrderId> {
        match report {
            ExecutionReport::Order(order_report)
            | ExecutionReport::OrderWithFills(order_report, _) => {
                order_report.client_order_id.or_else(|| {
                    self.kernel
                        .cache
                        .borrow()
                        .client_order_id(&order_report.venue_order_id)
                        .copied()
                })
            }
            ExecutionReport::Fill(fill_report) => fill_report.client_order_id.or_else(|| {
                self.kernel
                    .cache
                    .borrow()
                    .client_order_id(&fill_report.venue_order_id)
                    .copied()
            }),
            _ => None,
        }
    }

    fn observe_exec_command_before_dispatch(
        &mut self,
        cmd: &TradingCommand,
    ) -> anyhow::Result<bool> {
        let submission_client_id = if matches!(
            cmd,
            TradingCommand::SubmitOrder(_) | TradingCommand::SubmitOrderList(_)
        ) {
            self.kernel
                .exec_engine
                .borrow()
                .find_client_for_command(cmd)
                .map(|client| client.client_id())
        } else {
            None
        };

        match cmd {
            TradingCommand::SubmitOrder(submit) => {
                self.exec_manager.register_submission(
                    &submit.order_init,
                    submission_client_id.or(submit.client_id),
                );
            }
            TradingCommand::SubmitOrderList(submit) => {
                for order_init in &submit.order_inits {
                    self.exec_manager
                        .register_submission(order_init, submission_client_id.or(submit.client_id));
                }
            }
            TradingCommand::ModifyOrder(modify) => {
                return self
                    .exec_manager
                    .register_modify_inflight(modify.client_order_id, modify.command_id);
            }
            TradingCommand::ModifyOrders(modify) => {
                let admissions = modify
                    .modifies
                    .iter()
                    .map(|child| {
                        self.exec_manager
                            .register_modify_inflight(child.client_order_id, child.command_id)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let any_new = admissions.contains(&true);
                anyhow::ensure!(
                    !any_new || !admissions.contains(&false),
                    "mixed duplicate modify batch cannot change its protected original payload"
                );
                return Ok(any_new);
            }
            TradingCommand::CancelOrder(cancel) => {
                return self
                    .exec_manager
                    .register_cancel_inflight(cancel.client_order_id, cancel.command_id);
            }
            TradingCommand::CancelOrders(cancel) => {
                let admissions = cancel
                    .cancels
                    .iter()
                    .map(|child| {
                        self.exec_manager
                            .register_cancel_inflight(child.client_order_id, child.command_id)
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let any_new = admissions.contains(&true);
                anyhow::ensure!(
                    !any_new || !admissions.contains(&false),
                    "mixed duplicate cancel batch cannot change its protected original payload"
                );
                return Ok(any_new);
            }
            _ => {}
        }
        Ok(true)
    }

    /// Gets the node's environment.
    #[must_use]
    pub fn environment(&self) -> Environment {
        self.kernel.environment()
    }

    /// Gets a reference to the underlying kernel.
    #[must_use]
    pub const fn kernel(&self) -> &NautilusKernel {
        &self.kernel
    }

    /// Returns the actual immutable node configuration used by this instance.
    #[must_use]
    pub const fn config(&self) -> &LiveNodeConfig {
        &self.config
    }

    /// Gets an exclusive reference to the underlying kernel.
    #[must_use]
    pub const fn kernel_mut(&mut self) -> &mut NautilusKernel {
        &mut self.kernel
    }

    /// Gets the node's trader ID.
    #[must_use]
    pub fn trader_id(&self) -> TraderId {
        self.kernel.trader_id()
    }

    /// Gets the node's instance ID.
    #[must_use]
    pub const fn instance_id(&self) -> UUID4 {
        self.kernel.instance_id()
    }

    /// Returns the current node state.
    #[must_use]
    pub fn state(&self) -> NodeState {
        self.handle.state()
    }

    /// Checks if the live node is currently running.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.state().is_running()
    }

    /// Sets the cache database adapter for persistence.
    ///
    /// This allows setting a database adapter (e.g., PostgreSQL, Redis) after the node
    /// is built but before it starts running. The database adapter is used to persist
    /// cache data for recovery and state management.
    ///
    /// # Errors
    ///
    /// Returns an error if the node is already running.
    pub fn set_cache_database(
        &mut self,
        database: Box<dyn CacheDatabaseAdapter>,
    ) -> anyhow::Result<()> {
        if self.state() != NodeState::Idle {
            anyhow::bail!(
                "Cannot set cache database while node is running, set it before running the node"
            );
        }

        self.kernel.cache().borrow_mut().set_database(database);
        Ok(())
    }

    /// Returns the execution manager.
    #[must_use]
    pub fn exec_manager(&self) -> &ExecutionManager {
        &self.exec_manager
    }

    /// Returns a mutable reference to the execution manager.
    #[must_use]
    pub fn exec_manager_mut(&mut self) -> &mut ExecutionManager {
        &mut self.exec_manager
    }

    /// Adds an actor to the trader.
    ///
    /// This method provides a high-level interface for adding actors to the underlying
    /// trader without requiring direct access to the kernel. Actors should be added
    /// after the node is built but before starting the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The trader is not in a valid state for adding components.
    /// - An actor with the same ID is already registered.
    /// - The node is currently running.
    pub fn add_actor<T>(&mut self, actor: T) -> anyhow::Result<()>
    where
        T: DataActor + DataActorNative + Component + Actor + 'static,
    {
        if self.state() != NodeState::Idle {
            anyhow::bail!(
                "Cannot add actor while node is running, add actors before running the node"
            );
        }

        self.kernel.trader.borrow_mut().add_actor(actor)
    }

    /// Adds an actor to the live node using a factory function.
    ///
    /// The factory function is called at registration time to create the actor,
    /// avoiding cloning issues with non-cloneable actor types.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The node is currently running.
    /// - The factory function fails to create the actor.
    /// - The underlying trader registration fails.
    pub fn add_actor_from_factory<F, T>(&mut self, factory: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> anyhow::Result<T>,
        T: DataActor + DataActorNative + Component + Actor + 'static,
    {
        if self.state() != NodeState::Idle {
            anyhow::bail!(
                "Cannot add actor while node is running, add actors before running the node"
            );
        }

        self.kernel
            .trader
            .borrow_mut()
            .add_actor_from_factory(factory)
    }

    /// Adds a strategy to the trader.
    ///
    /// Strategies are registered in both the component registry (for lifecycle management)
    /// and the actor registry (for data callbacks via msgbus).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The node is currently running.
    /// - A strategy with the same ID is already registered.
    /// - The configured external order instrument IDs repeat an instrument or the cache already
    ///   contains a requested claim.
    /// - The strategy configures one or more external order instrument IDs and the cache is already
    ///   borrowed.
    /// - The strategy configures an OMS type override and the execution engine is already borrowed.
    pub fn add_strategy<T>(&mut self, mut strategy: T) -> anyhow::Result<()>
    where
        T: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        if self.state() != NodeState::Idle {
            anyhow::bail!(
                "Cannot add strategy while node is running, add strategies before running the node"
            );
        }

        // Capture strategy-owned values before adding the strategy, which moves it
        let strategy_id = self
            .kernel
            .trader
            .borrow()
            .prepare_strategy_for_registration(&mut strategy)?;
        let oms_type = StrategyNative::strategy_core(&strategy).config.oms_type;
        let instrument_ids = strategy.external_order_instrument_ids().unwrap_or_default();

        if !instrument_ids.is_empty() {
            self.register_external_order_claims(strategy_id, &instrument_ids)?;
        }

        let mut exec_engine = match oms_type
            .map(|_| {
                self.kernel
                    .exec_engine
                    .try_borrow_mut()
                    .map_err(|e| anyhow::anyhow!("Cannot register OMS type: {e}"))
            })
            .transpose()
        {
            Ok(exec_engine) => exec_engine,
            Err(e) => {
                if !instrument_ids.is_empty()
                    && let Err(rollback_error) =
                        self.rollback_external_order_claims(strategy_id, &instrument_ids)
                {
                    anyhow::bail!(
                        "{e}; failed to roll back external order claims for {strategy_id}: {rollback_error}"
                    );
                }

                return Err(e);
            }
        };

        if let Err(add_error) = self.kernel.trader.borrow_mut().add_strategy(strategy) {
            drop(exec_engine);

            if !instrument_ids.is_empty()
                && let Err(rollback_error) =
                    self.rollback_external_order_claims(strategy_id, &instrument_ids)
            {
                anyhow::bail!(
                    "Failed to add strategy {strategy_id}: {add_error}; failed to roll back external order claims: {rollback_error}"
                );
            }

            return Err(add_error);
        }

        if let Some(exec_engine) = &mut exec_engine
            && let Some(oms_type) = oms_type
        {
            exec_engine.register_oms_type(strategy_id, oms_type);
        }

        Ok(())
    }

    /// Registers external order claims in the shared cache.
    ///
    /// It can be called while the node is idle, after manual [`start`](Self::start) returns, or
    /// after the node stops. A running strategy can update its own claims through
    /// `Strategy::set_external_order_instrument_ids`.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache is already borrowed, the request repeats an instrument, or any
    /// requested instrument already has a claim. The cache remains unchanged.
    pub fn register_external_order_claims(
        &self,
        strategy_id: StrategyId,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<()> {
        self.kernel
            .cache
            .try_borrow_mut()
            .map_err(|e| anyhow::anyhow!("Cannot register external order claims: {e}"))?
            .register_external_order_claims(strategy_id, instrument_ids)?;

        if !instrument_ids.is_empty() {
            log::info!("Registered external order claims for {strategy_id}: {instrument_ids:?}");
        }

        Ok(())
    }

    /// Deregisters all external order claims owned by `strategy_id` from the shared cache.
    ///
    /// The operation is synchronous and can be called while the node is idle, after manual
    /// [`start`](Self::start) returns, or after the node stops. It cannot be called while
    /// [`run`](Self::run) or [`run_with_mode`](Self::run_with_mode) owns the node.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache is already borrowed.
    pub fn deregister_external_order_claims(&self, strategy_id: StrategyId) -> anyhow::Result<()> {
        self.kernel
            .cache
            .try_borrow_mut()
            .map_err(|e| anyhow::anyhow!("Cannot deregister external order claims: {e}"))?
            .set_external_order_claims(strategy_id, &[])?;

        Ok(())
    }

    /// Restores actor and strategy callback state into the freshly assembled
    /// node while it is still idle.
    ///
    /// This is a component-state operation only.  The caller remains
    /// responsible for restoring native cache/portfolio state, replaying
    /// queues at a common dispatch boundary, reconciling the venue, and
    /// obtaining an independent execution authorization before starting.
    /// A callback failure leaves the node unsuitable for start; callers should
    /// discard it and preserve the source checkpoint.
    ///
    /// # Errors
    ///
    /// Rejects a non-idle node, mismatched component identities/order, or a
    /// component callback failure.
    pub fn restore_component_state(
        &mut self,
        state: &nautilus_system::trader::CollectedComponentState,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "Cannot restore component state after node startup has begun"
        );
        self.recovery_requires_release = true;
        #[cfg(feature = "dispatch-observer")]
        {
            anyhow::ensure!(
                self.recovery_empty_bootstrap.is_none(),
                "empty recovery already completed"
            );
            self.recovery_restored_components = None;
        }
        nautilus_system::trader::Trader::restore_component_state(self.kernel.trader(), state)?;
        #[cfg(feature = "dispatch-observer")]
        {
            self.recovery_restored_components = Some((
                state.actors.keys().map(ToString::to_string).collect(),
                state.strategies.keys().map(ToString::to_string).collect(),
            ));
        }
        Ok(())
    }

    /// Installs an isolated, validated native cache into a freshly assembled idle node.
    ///
    /// This operation preserves the node's shared cache handle so the portfolio and engines
    /// observe the same cache object. It accepts only a cache without a persistence adapter and
    /// rejects a target that already contains native state or has a database waiting to be
    /// installed on startup. The caller must still restore component state, replay pending
    /// inputs at a common boundary, reconcile the venue and obtain an independent execution
    /// authorization before starting the node.
    ///
    /// # Errors
    ///
    /// Rejects a non-idle node, a cache with persistence backing, or a target cache that already
    /// contains instruments, accounts, orders or positions.
    pub fn restore_native_cache(&mut self, cache: Cache) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.state() == NodeState::Idle,
            "Cannot restore native cache after node startup has begun"
        );
        anyhow::ensure!(
            !self.has_pending_cache_database(),
            "Cannot restore native cache while a cache database is pending installation"
        );
        anyhow::ensure!(
            !self.kernel.load_state() && !self.kernel.is_event_store_replay_configured(),
            "Cannot restore native cache while startup state loading or event-store replay is configured"
        );
        anyhow::ensure!(
            !cache.has_backing(),
            "Native handoff cache must not have a persistence backing"
        );

        let cache_handle = self.kernel.cache();
        let mut target = cache_handle.try_borrow_mut().map_err(|error| {
            anyhow::anyhow!("Cannot borrow target cache for native handoff: {error}")
        })?;
        anyhow::ensure!(
            !target.has_backing()
                && target.instrument_ids(None).is_empty()
                && target.accounts_all_owned().is_empty()
                && target.orders_total_count(None, None, None, None, None) == 0
                && target.positions_total_count(None, None, None, None, None) == 0,
            "Target cache already contains native state or a persistence backing"
        );
        self.recovery_requires_release = true;
        *target = cache;
        #[cfg(feature = "dispatch-observer")]
        {
            self.recovery_cache_installed = true;
        }
        Ok(())
    }

    pub(crate) fn rollback_external_order_claims(
        &self,
        strategy_id: StrategyId,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<()> {
        let mut cache = self
            .kernel
            .cache
            .try_borrow_mut()
            .map_err(|e| anyhow::anyhow!("Cannot roll back external order claims: {e}"))?;
        let retained: Vec<_> = cache
            .external_order_claim_instrument_ids(Some(strategy_id))
            .into_iter()
            .filter(|instrument_id| !instrument_ids.contains(instrument_id))
            .collect();
        cache.set_external_order_claims(strategy_id, &retained)
    }

    /// Adds an execution algorithm to the trader.
    ///
    /// Execution algorithms are registered in both the component registry (for lifecycle
    /// management) and the actor registry (for data callbacks via msgbus).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The node is currently running.
    /// - An execution algorithm with the same ID is already registered.
    pub fn add_exec_algorithm<T>(&mut self, exec_algorithm: T) -> anyhow::Result<()>
    where
        T: ExecutionAlgorithm + ExecutionAlgorithmNative + Component + Debug + 'static,
    {
        if self.state() != NodeState::Idle {
            anyhow::bail!(
                "Cannot add exec algorithm while node is running, add exec algorithms before running the node"
            );
        }

        self.kernel
            .trader
            .borrow_mut()
            .add_exec_algorithm(exec_algorithm)
    }

    // Runs reconciliation sub-checks, each gated by its own interval.
    // Continuous checks only schedule venue work; they do not await venue I/O
    // in the event loop.
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SocketReconnectDispatchOutcome {
    Accepted,
    AlreadyReconnecting,
    Disconnected,
    Closed,
    Unsupported,
    InvalidTrader,
    UnknownClient,
    UnknownEndpoint,
    AmbiguousEndpoint,
}

fn record_runner_dispatch(
    metrics: &RunnerMetrics,
    channel: SystemChannel,
    dispatch_start: dst::time::Instant,
    metrics_start: dst::time::Instant,
) {
    let dispatch_end = dst::time::Instant::now();
    metrics.record_dispatch(
        channel,
        dispatch_end.duration_since(dispatch_start),
        dispatch_end.duration_since(metrics_start),
    );
}

fn record_runner_maintenance(
    metrics: &RunnerMetrics,
    work_start: dst::time::Instant,
    metrics_start: dst::time::Instant,
) {
    let work_end = dst::time::Instant::now();
    metrics.record_maintenance(
        work_end.duration_since(work_start),
        work_end.duration_since(metrics_start),
    );
}

fn record_runner_external_msgbus(
    metrics: &RunnerMetrics,
    work_start: dst::time::Instant,
    metrics_start: dst::time::Instant,
) {
    let work_end = dst::time::Instant::now();
    metrics.record_external_msgbus(
        work_end.duration_since(work_start),
        work_end.duration_since(metrics_start),
    );
}

/// Select one ready channel in bounded cyclic order without receiving,
/// moving, sorting or discarding any original input. Signals/deadlines remain
/// ahead of this selection, and maintenance retains its existing alternation.
fn next_ready_runner_channel<const N: usize>(ready: &[bool; N], next: usize) -> Option<usize> {
    ready
        .iter()
        .enumerate()
        .skip(next)
        .chain(ready.iter().enumerate().take(next))
        .find_map(|(index, ready)| ready.then_some(index))
}

async fn recv_external_msgbus_message(
    rx: &mut Option<tokio::sync::mpsc::Receiver<BusMessage>>,
) -> Option<BusMessage> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending::<Option<BusMessage>>().await,
    }
}

struct RunnerReceivers<'a> {
    time_evt: &'a mut SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
    system_evt: &'a mut SnapshotReceiver<DispatchMessage<SystemEvent>>,
    system_cmd: &'a mut SnapshotReceiver<DispatchMessage<SystemCommand>>,
    exec_evt: &'a mut SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
    exec_cmd: &'a mut SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
    data_evt: &'a mut SnapshotReceiver<DispatchMessage<DataEvent>>,
    data_cmd: &'a mut SnapshotReceiver<DispatchMessage<DataCommand>>,
}

/// Flushes data events and commands from both `pending` and the channel receivers
/// into the cache, looping until no progress is made.
///
/// This covers inputs still queued when a biased startup select completes its
/// connection future before capturing those inputs into `pending`.
#[cfg(test)]
fn flush_pending_data(
    pending: &mut PendingEvents,
    data_evt_rx: &mut SnapshotReceiver<DispatchMessage<DataEvent>>,
    data_cmd_rx: &mut SnapshotReceiver<DispatchMessage<DataCommand>>,
) {
    loop {
        let mut progressed = pending.drain_data();

        while let Ok(evt) = data_evt_rx.try_recv() {
            AsyncRunner::dispatch_data_event(evt);
            progressed = true;
        }

        while let Ok(cmd) = data_cmd_rx.try_recv() {
            AsyncRunner::handle_data_command(cmd);
            progressed = true;
        }

        if !progressed {
            break;
        }
    }
}

/// Flushes all channel receivers into `pending`, then drains everything.
///
/// Unlike [`flush_pending_data`] this is a single pass, not a drain-until-quiet
/// loop. Sufficient for phase 2 where the goal is to capture items the biased
/// select did not poll before the connect future resolved.
#[expect(
    clippy::too_many_arguments,
    reason = "all runner receivers are drained together"
)]
#[cfg(test)]
fn flush_all_pending(
    pending: &mut PendingEvents,
    time_evt_rx: &mut SnapshotReceiver<DispatchMessage<TimeEventMessage>>,
    system_evt_rx: &mut SnapshotReceiver<DispatchMessage<SystemEvent>>,
    system_cmd_rx: &mut SnapshotReceiver<DispatchMessage<SystemCommand>>,
    exec_evt_rx: &mut SnapshotReceiver<DispatchMessage<ExecutionEvent>>,
    exec_cmd_rx: &mut SnapshotReceiver<DispatchMessage<TradingCommandMessage>>,
    data_evt_rx: &mut SnapshotReceiver<DispatchMessage<DataEvent>>,
    data_cmd_rx: &mut SnapshotReceiver<DispatchMessage<DataCommand>>,
) {
    // Flush channel receivers into pending
    while let Ok(handler) = time_evt_rx.try_recv() {
        let _ = AsyncRunner::handle_time_event(handler);
    }

    while let Ok(event) = system_evt_rx.try_recv() {
        pending.system_events.push(event);
    }

    while let Ok(command) = system_cmd_rx.try_recv() {
        pending.system_commands.push(command);
    }

    while let Ok(evt) = data_evt_rx.try_recv() {
        pending.data_evts.push(evt);
    }

    while let Ok(cmd) = data_cmd_rx.try_recv() {
        pending.data_cmds.push(cmd);
    }

    while let Ok(evt) = exec_evt_rx.try_recv() {
        pending.push_exec_event(evt);
    }

    while let Ok(cmd) = exec_cmd_rx.try_recv() {
        pending.exec_cmds.push(cmd);
    }

    pending.drain();
}

#[cfg(test)]
#[derive(Default)]
struct PendingEvents {
    system_events: Vec<DispatchMessage<SystemEvent>>,
    system_commands: Vec<DispatchMessage<SystemCommand>>,
    data_evts: Vec<DispatchMessage<DataEvent>>,
    data_cmds: Vec<DispatchMessage<DataCommand>>,
    exec_reports: Vec<DispatchMessage<ExecutionReport>>,
    order_evts: Vec<DispatchMessage<OrderEventAny>>,
    exec_cmds: Vec<DispatchMessage<TradingCommandMessage>>,
}

#[cfg(test)]
impl PendingEvents {
    fn is_empty(&self) -> bool {
        self.system_events.is_empty()
            && self.system_commands.is_empty()
            && self.data_evts.is_empty()
            && self.data_cmds.is_empty()
            && self.exec_reports.is_empty()
            && self.order_evts.is_empty()
            && self.exec_cmds.is_empty()
    }

    /// Drains only data events and commands into the cache.
    ///
    /// Returns `true` if any events or commands were drained.
    fn drain_data(&mut self) -> bool {
        let total = self.data_evts.len() + self.data_cmds.len();

        if total > 0 {
            log::debug!(
                "Draining {total} data events/commands into cache \
                 (data_evts={}, data_cmds={})",
                self.data_evts.len(),
                self.data_cmds.len(),
            );
        }

        for evt in self.data_evts.drain(..) {
            AsyncRunner::dispatch_data_event(evt);
        }

        for cmd in self.data_cmds.drain(..) {
            AsyncRunner::handle_data_command(cmd);
        }

        total > 0
    }

    /// Drains all remaining pending events.
    fn drain(&mut self) {
        let total = self.data_evts.len()
            + self.data_cmds.len()
            + self.exec_reports.len()
            + self.order_evts.len()
            + self.exec_cmds.len();

        if total > 0 {
            log::debug!(
                "Processing {total} events/commands queued during startup \
                 (data_evts={}, data_cmds={}, exec_reports={}, order_evts={}, exec_cmds={})",
                self.data_evts.len(),
                self.data_cmds.len(),
                self.exec_reports.len(),
                self.order_evts.len(),
                self.exec_cmds.len()
            );
        }

        for evt in self.data_evts.drain(..) {
            AsyncRunner::dispatch_data_event(evt);
        }

        for cmd in self.data_cmds.drain(..) {
            AsyncRunner::handle_data_command(cmd);
        }

        for report in self.exec_reports.drain(..) {
            report
                .dispatch(|report| AsyncRunner::handle_exec_event(ExecutionEvent::Report(report)));
        }

        for evt in self.order_evts.drain(..) {
            evt.dispatch(|evt| AsyncRunner::handle_exec_event(ExecutionEvent::Order(evt)));
        }

        for cmd in self.exec_cmds.drain(..) {
            AsyncRunner::handle_trading_command(cmd);
        }
    }

    fn push_exec_event(&mut self, event: DispatchMessage<ExecutionEvent>) {
        let rooted = event.is_rooted();
        event.dispatch(|event| {
            let owner = std::thread::current().id();

            let order = |event| {
                if rooted {
                    DispatchMessage::new(event, owner)
                } else {
                    DispatchMessage::from(event)
                }
            };

            // Account events are safe to process immediately. Reports and orders need
            // ExecEngine access, which may conflict with the driven startup future's borrow.
            match event {
                ExecutionEvent::Account(_) => AsyncRunner::handle_exec_event(event),
                ExecutionEvent::Report(report) => self.exec_reports.push(if rooted {
                    DispatchMessage::new(report, owner)
                } else {
                    report.into()
                }),
                ExecutionEvent::Order(event) => self.order_evts.push(order(event)),
                ExecutionEvent::OrderSubmittedBatch(batch) => {
                    for event in batch {
                        self.order_evts.push(order(OrderEventAny::Submitted(event)));
                    }
                }
                ExecutionEvent::OrderAcceptedBatch(batch) => {
                    for event in batch {
                        self.order_evts.push(order(OrderEventAny::Accepted(event)));
                    }
                }
                ExecutionEvent::OrderCanceledBatch(batch) => {
                    for event in batch {
                        self.order_evts.push(order(OrderEventAny::Canceled(event)));
                    }
                }
            }
        });
    }

    fn take_system_events(&mut self) -> Vec<DispatchMessage<SystemEvent>> {
        std::mem::take(&mut self.system_events)
    }

    fn take_system_commands(&mut self) -> Vec<DispatchMessage<SystemCommand>> {
        std::mem::take(&mut self.system_commands)
    }
}

struct ClientStatus {
    client: String,
    client_type: &'static str,
    connected: bool,
}

fn render_client_statuses(rows: Vec<ClientStatus>) -> String {
    let mut builder = Builder::with_capacity(rows.len() + 1, 3);
    builder.push_record(["Client", "Type", "Connected"]);

    for row in rows {
        builder.push_record([
            row.client,
            row.client_type.to_string(),
            row.connected.to_string(),
        ]);
    }

    builder.build().with(Style::rounded()).to_string()
}

#[cfg(test)]
mod tests {
    mod shutdown;

    use std::{
        cell::{Cell, RefCell},
        fmt::Debug,
        rc::Rc,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use crate::runner_recovery::{RunnerRecoveryCodecRegistry, RunnerRecoveryEnvelope};
    use bytes::Bytes;
    use indexmap::{IndexMap, IndexSet};
    use log::{Level, LevelFilter, Log, Metadata, Record};
    #[cfg(feature = "python")]
    use nautilus_common::runner::{
        SyncDataCommandSender, replace_data_cmd_sender, replace_exec_cmd_sender,
    };
    use nautilus_common::{
        actor::{
            self, CallbackDispatchError, DataActor, DataActorCore, data_actor::DataActorConfig,
            registry::get_actor_unchecked,
        },
        cache::Cache,
        clients::ExecutionClient,
        clock::{Clock, VirtualClock},
        enums::SerializationEncoding,
        live::{
            runner::{get_data_event_sender, get_exec_event_sender, get_system_event_sender},
            sender::DispatchSender,
        },
        logging::{logger::LoggerConfig, logging_sync_to_disk, writer::FileWriterConfig},
        messages::{
            data::{SubscribeCommand, SubscribeQuotes},
            execution::{
                GenerateFillReports, GenerateOrderStatusReport, GenerateOrderStatusReports,
                GeneratePositionStatusReports, QueryAccount, SubmitOrder, SubmitOrderList,
                TradingCommand,
            },
            system::{
                QueueCondition, QueueState, ReconnectSocket, SocketState, SocketStateChanged,
            },
        },
        msgbus::{
            self, BusMessage, BusPayloadType, MessageBusBacking, MessageBusBackingFactory,
            MessageBusConfig, MessageBusExternalEgress, MessageBusExternalIngress,
            MessagingSwitchboard, ShareableMessageHandler, TypedHandler, TypedIntoHandler,
        },
        nautilus_actor,
        runner::{SyncTradingCommandSender, TradingCommandSender},
        testing::wait_until_async,
        timer::{TimeEvent, TimeEventCallback},
    };
    use nautilus_core::{Params, UUID4, UnixNanos};
    use nautilus_execution::{
        engine::{ExecutionEngine, SnapshotAnchorer, stubs::StubExecutionClient},
        reconciliation::{RECONCILIATION_ORDER_TAG, create_inferred_fill_for_qty},
    };
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        data::QuoteTick,
        enums::{
            AccountType, LiquiditySide, OmsType, OrderSide, OrderStatus, OrderType, PositionSide,
            TimeInForce,
        },
        events::{
            AccountState, OrderAccepted, OrderAcceptedBatch, OrderFilled, OrderUpdated,
            order::spec::{
                OrderAcceptedSpec, OrderPendingCancelSpec, OrderPendingUpdateSpec, OrderUpdatedSpec,
            },
        },
        identifiers::{
            AccountId, ActorId, ClientId, InstrumentId, OrderListId, PositionId, StrategyId,
            TradeId, TraderId, Venue, VenueOrderId,
        },
        instruments::{
            Instrument, InstrumentAny,
            stubs::{crypto_perpetual_ethusdt, currency_pair_btcusdt},
        },
        orders::{
            Order, OrderAny, OrderList, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderEventStubs},
        },
        reports::{FillReport, PositionStatusReport},
        types::{AccountBalance, Currency, MarginBalance, Money, Price, Quantity},
    };
    use nautilus_system::{
        KernelEventStore, RegisteredComponents, event_store::EventStoreConfig,
        trader::CollectedComponentState,
    };
    use nautilus_testkit::{
        cache::TestCacheDatabaseControl,
        components::{StateActor, StateStrategy},
    };
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{config::StrategyConfig, core::StrategyCore},
    };
    use parking_lot::Mutex;
    use rstest::*;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use ustr::Ustr;

    use super::{
        reconciliation::{
            PositionFillReportResult, PositionReportResult, reconciliation_check_due,
            request_position_fill_reports,
        },
        *,
    };
    use crate::{
        execution::{
            client::LiveExecutionClient,
            manager::{PositionFillReportQuery, ReportClientCoverage},
            submission::{
                SubmissionRecoveryExhausted, SubmissionRecoveryPolicy, SubmissionRecoverySource,
            },
        },
        socket::SocketControl,
    };

    struct ExternalIngressLogCapture {
        messages: Mutex<Vec<String>>,
    }

    static EXTERNAL_INGRESS_LOG_CAPTURE: ExternalIngressLogCapture = ExternalIngressLogCapture {
        messages: Mutex::new(Vec::new()),
    };

    #[derive(Debug)]
    enum FillReportClientOutcome {
        Reports(Vec<FillReport>),
        Failure,
    }

    #[derive(Debug)]
    struct FillReportClient {
        client_id: ClientId,
        account_id: AccountId,
        venue: Venue,
        outcome: FillReportClientOutcome,
        commands: Rc<RefCell<Vec<GenerateFillReports>>>,
    }

    #[async_trait::async_trait(?Send)]
    impl ExecutionClient for FillReportClient {
        fn is_connected(&self) -> bool {
            true
        }

        fn client_id(&self) -> ClientId {
            self.client_id
        }

        fn account_id(&self) -> AccountId {
            self.account_id
        }

        fn venue(&self) -> Venue {
            self.venue
        }

        fn oms_type(&self) -> OmsType {
            OmsType::Netting
        }

        fn get_account(&self) -> Option<AccountAny> {
            None
        }

        fn generate_account_state(
            &self,
            _balances: Vec<AccountBalance>,
            _margins: Vec<MarginBalance>,
            _reported: bool,
            _ts_event: UnixNanos,
            _info: Option<Params>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn start(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn stop(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn generate_order_status_reports(
            &self,
            _cmd: &GenerateOrderStatusReports,
        ) -> anyhow::Result<Vec<OrderStatusReport>> {
            match self.outcome {
                FillReportClientOutcome::Failure => anyhow::bail!("order reports unavailable"),
                FillReportClientOutcome::Reports(_) => Ok(Vec::new()),
            }
        }

        async fn generate_position_status_reports(
            &self,
            _cmd: &GeneratePositionStatusReports,
        ) -> anyhow::Result<Vec<PositionStatusReport>> {
            match self.outcome {
                FillReportClientOutcome::Failure => anyhow::bail!("position reports unavailable"),
                FillReportClientOutcome::Reports(_) => Ok(Vec::new()),
            }
        }

        async fn generate_fill_reports(
            &self,
            cmd: GenerateFillReports,
        ) -> anyhow::Result<Vec<FillReport>> {
            self.commands.borrow_mut().push(cmd);

            match &self.outcome {
                FillReportClientOutcome::Reports(reports) => Ok(reports.clone()),
                FillReportClientOutcome::Failure => anyhow::bail!("fill reports unavailable"),
            }
        }
    }

    #[derive(Debug)]
    struct OrderReportClient {
        client_id: ClientId,
        report: OrderStatusReport,
        targeted: bool,
        queries: Rc<RefCell<Vec<&'static str>>>,
    }

    #[async_trait::async_trait(?Send)]
    impl ExecutionClient for OrderReportClient {
        fn is_connected(&self) -> bool {
            true
        }

        fn client_id(&self) -> ClientId {
            self.client_id
        }

        fn account_id(&self) -> AccountId {
            self.report.account_id
        }

        fn venue(&self) -> Venue {
            self.report.instrument_id.venue
        }

        fn oms_type(&self) -> OmsType {
            OmsType::Netting
        }

        fn get_account(&self) -> Option<AccountAny> {
            None
        }

        fn generate_account_state(
            &self,
            _balances: Vec<AccountBalance>,
            _margins: Vec<MarginBalance>,
            _reported: bool,
            _ts_event: UnixNanos,
            _info: Option<Params>,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn start(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        fn stop(&mut self) -> anyhow::Result<()> {
            Ok(())
        }

        async fn generate_order_status_reports(
            &self,
            _command: &GenerateOrderStatusReports,
        ) -> anyhow::Result<Vec<OrderStatusReport>> {
            self.queries.borrow_mut().push("bulk");
            Ok(if self.targeted {
                Vec::new()
            } else {
                vec![self.report.clone()]
            })
        }

        async fn generate_order_status_report(
            &self,
            _command: &GenerateOrderStatusReport,
        ) -> anyhow::Result<Option<OrderStatusReport>> {
            self.queries.borrow_mut().push("targeted");
            Ok(Some(self.report.clone()))
        }
    }

    #[derive(Debug)]
    struct StartupSocketActor {
        core: DataActorCore,
        received: Rc<RefCell<Vec<SocketStateChanged>>>,
    }

    impl StartupSocketActor {
        fn new(received: Rc<RefCell<Vec<SocketStateChanged>>>) -> Self {
            Self {
                core: DataActorCore::new(DataActorConfig {
                    actor_id: Some(ActorId::from("SOCKET-STARTUP-ACTOR")),
                    ..Default::default()
                }),
                received,
            }
        }
    }

    impl DataActor for StartupSocketActor {
        fn on_start(&mut self) -> anyhow::Result<()> {
            self.subscribe_socket_state(None, None, None);
            Ok(())
        }

        fn on_socket_state(&mut self, event: &SocketStateChanged) -> anyhow::Result<()> {
            self.received.borrow_mut().push(event.clone());
            Ok(())
        }
    }

    nautilus_actor!(StartupSocketActor);

    impl Log for ExternalIngressLogCapture {
        fn enabled(&self, metadata: &Metadata<'_>) -> bool {
            metadata.level() == Level::Error && metadata.target() == "nautilus_live::node"
        }

        fn log(&self, record: &Record<'_>) {
            if self.enabled(record.metadata()) {
                self.messages.lock().push(record.args().to_string());
            }
        }

        fn flush(&self) {}
    }

    #[derive(Debug)]
    struct FailingTimerActor {
        core: DataActorCore,
        received: Rc<RefCell<Vec<u64>>>,
    }

    nautilus_actor!(FailingTimerActor);

    impl DataActor for FailingTimerActor {
        fn on_start(&mut self) -> anyhow::Result<()> {
            for timestamp in [17, 23] {
                let received = self.received.clone();

                let callback = TimeEventCallback::RustLocal(Rc::new(move |_| {
                    received.borrow_mut().push(timestamp);

                    if timestamp == 17 {
                        crate::dispatch::tests::latch_callback_failure();
                    }
                }));

                nautilus_common::runner::get_time_event_sender().send(TimeEventMessage::new(
                    TimeEvent::new(
                        "callback-failure".into(),
                        UUID4::new(),
                        timestamp.into(),
                        timestamp.into(),
                    ),
                    callback,
                ));
            }

            Ok(())
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_callback_failure_stops_later_live_events(
        #[values(false, true)] incomplete_submission: bool,
    ) {
        actor::clear_callbacks().unwrap();

        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                submission_recovery_policy: SubmissionRecoveryPolicy::RetainUnresolved,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("CallbackFailureNode".to_string(), Some(config)).unwrap();

        if incomplete_submission {
            let order = OrderTestBuilder::new(OrderType::Market)
                .client_order_id(ClientOrderId::from("O-CALLBACK-UNRESOLVED"))
                .instrument_id(InstrumentId::from("EUR/USD.SIM"))
                .quantity(Quantity::from("1"))
                .build();
            node.exec_manager
                .register_submission(order.init_event(), None);
        }
        let received = Rc::new(RefCell::new(Vec::new()));
        node.add_actor(FailingTimerActor {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("CALLBACK-FAILURE")),
                ..Default::default()
            }),
            received: received.clone(),
        })
        .unwrap();

        let result = node.run_with_mode(NodeRunMode::Hosted).await;

        let error = result.unwrap_err();
        assert!(error.to_string().contains("Callback delivery unwound"));
        if incomplete_submission {
            assert!(error.to_string().contains("O-CALLBACK-UNRESOLVED"));
        }
        let state = node.state();
        let trader_stopped = node.kernel.trader.borrow().is_stopped();
        let failure = actor::callback_failure();
        node.dispose();

        assert_eq!(
            error.downcast_ref::<CallbackDispatchError>(),
            Some(&CallbackDispatchError::DeliveryUnwound)
        );
        assert_eq!(*received.borrow(), [17]);
        assert_eq!(state, NodeState::Stopped);
        assert!(trader_stopped);
        assert_eq!(failure, Some(CallbackDispatchError::DeliveryUnwound));
        assert_eq!(actor::callback_failure(), None);
        assert_eq!(actor::clear_callbacks(), Ok(()));
    }

    #[rstest]
    fn test_render_client_statuses() {
        let rows = vec![
            ClientStatus {
                client: "BINANCE".to_string(),
                client_type: "Data",
                connected: true,
            },
            ClientStatus {
                client: "SIM".to_string(),
                client_type: "Execution",
                connected: false,
            },
        ];

        let output = render_client_statuses(rows);
        let expected = "╭─────────┬───────────┬───────────╮\n\
│ Client  │ Type      │ Connected │\n\
├─────────┼───────────┼───────────┤\n\
│ BINANCE │ Data      │ true      │\n\
│ SIM     │ Execution │ false     │\n\
╰─────────┴───────────┴───────────╯";

        assert_eq!(output, expected);
    }

    #[rstest]
    fn test_republish_external_msgbus_message_logs_topic_and_error_chain() {
        log::set_logger(&EXTERNAL_INGRESS_LOG_CAPTURE).expect("test logger already installed");
        log::set_max_level(LevelFilter::Error);
        EXTERNAL_INGRESS_LOG_CAPTURE.messages.lock().clear();
        let message = BusMessage::with_str_topic(
            "data.quotes.AUDUSD.SIM*",
            BusPayloadType::Custom(Ustr::from("UnregisteredCustomData")),
            Bytes::new(),
            SerializationEncoding::Json,
        );

        LiveNode::republish_external_msgbus_message(&message);

        assert_eq!(
            *EXTERNAL_INGRESS_LOG_CAPTURE.messages.lock(),
            vec![
                "Failed to republish external message bus topic 'data.quotes.AUDUSD.SIM*': invalid \
                 external message topic: Topic `value` contained invalid characters, was \
                 data.quotes.AUDUSD.SIM*"
                    .to_string()
            ],
        );
    }

    #[rstest]
    #[case(None, 2)]
    #[case(Some(SystemChannel::DataEvents), 1)]
    #[case(Some(SystemChannel::ExecCommands), 1)]
    fn test_publish_queue_state_transitions_reaches_typed_subscriber(
        #[case] channel: Option<SystemChannel>,
        #[case] expected_count: usize,
    ) {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("QUEUE-001"),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let node = LiveNode::build("QueuePublicationNode".to_string(), Some(config)).unwrap();
        let received = Rc::new(RefCell::new(Vec::<QueueStateChanged>::new()));

        let handler = ShareableMessageHandler::from_typed({
            let received = received.clone();
            move |event: &QueueStateChanged| received.borrow_mut().push(event.clone())
        });

        msgbus::subscribe_any(
            MessagingSwitchboard::queue_state_changed_pattern(channel),
            handler,
            None,
        );

        let transitions = [
            QueueStateTransition {
                channel: SystemChannel::DataEvents,
                condition: QueueCondition::Backlogged,
                state: QueueState::Triggered,
                queue_depth: 17,
                mean_dispatch_ns: 23,
            },
            QueueStateTransition {
                channel: SystemChannel::ExecCommands,
                condition: QueueCondition::Slow,
                state: QueueState::Triggered,
                queue_depth: 17,
                mean_dispatch_ns: 23,
            },
        ];

        node.publish_queue_state_transitions(&transitions);

        let events = received.borrow();
        assert_eq!(events.len(), expected_count);

        let expected = transitions
            .into_iter()
            .filter(|transition| channel.is_none_or(|channel| channel == transition.channel));

        for (event, transition) in events.iter().zip(expected) {
            assert_eq!(event.trader_id, TraderId::from("QUEUE-001"));
            assert_eq!(event.channel, transition.channel);
            assert_eq!(event.condition, transition.condition);
            assert_eq!(event.state, transition.state);
            assert_eq!(event.queue_depth, transition.queue_depth);
            assert_eq!(event.mean_dispatch_ns, transition.mean_dispatch_ns);
            assert_ne!(event.event_id, UUID4::default());
            assert_ne!(event.ts_event, UnixNanos::default());
            assert_eq!(event.ts_init, event.ts_event);
        }

        if channel.is_none() {
            assert_ne!(events[0].event_id, events[1].event_id);
        }

        drop(events);
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    #[case(None, None)]
    #[case(Some(ClientId::from("BINANCE")), None)]
    #[case(None, Some("binance-futures-market-streams"))]
    #[case(
        Some(ClientId::from("BINANCE")),
        Some("binance-futures-market-streams")
    )]
    fn test_process_socket_state_change_reaches_typed_subscriber(
        #[case] client_id: Option<ClientId>,
        #[case] endpoint: Option<&str>,
    ) {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("SOCKET-001"),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let node = LiveNode::build("SocketPublicationNode".to_string(), Some(config)).unwrap();
        let received = Rc::new(RefCell::new(Vec::<SocketStateChanged>::new()));

        let handler = ShareableMessageHandler::from_typed({
            let received = received.clone();
            move |event: &SocketStateChanged| received.borrow_mut().push(event.clone())
        });

        msgbus::subscribe_any(
            MessagingSwitchboard::socket_state_changed_pattern(client_id, endpoint),
            handler,
            None,
        );

        let change = SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            Ustr::from("binance-futures-market-streams"),
            SocketState::Disconnected,
        );

        node.process_system_event(SystemEvent::SocketState(change));

        let events = received.borrow();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trader_id, TraderId::from("SOCKET-001"));
        assert_eq!(events[0].client_id, ClientId::from("BINANCE"));
        assert_eq!(events[0].venue, Some(Venue::from("BINANCE")));
        assert_eq!(
            events[0].endpoint,
            Ustr::from("binance-futures-market-streams")
        );
        assert_eq!(events[0].state, SocketState::Disconnected);
        assert_ne!(events[0].event_id, UUID4::default());
        assert_ne!(events[0].ts_event, UnixNanos::default());
        assert_eq!(events[0].ts_init, events[0].ts_event);
        drop(events);
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    #[case::accepted(
        ReconnectRequestOutcome::Accepted,
        SocketReconnectDispatchOutcome::Accepted
    )]
    #[case::already_reconnecting(
        ReconnectRequestOutcome::AlreadyReconnecting,
        SocketReconnectDispatchOutcome::AlreadyReconnecting
    )]
    #[case::disconnected(
        ReconnectRequestOutcome::Disconnected,
        SocketReconnectDispatchOutcome::Disconnected
    )]
    #[case::closed(
        ReconnectRequestOutcome::Closed,
        SocketReconnectDispatchOutcome::Closed
    )]
    #[case::unsupported(
        ReconnectRequestOutcome::Unsupported,
        SocketReconnectDispatchOutcome::Unsupported
    )]
    fn test_request_socket_reconnect_maps_transport_outcome(
        #[case] transport: ReconnectRequestOutcome,
        #[case] expected: SocketReconnectDispatchOutcome,
    ) {
        let registry = SocketReconnectRegistry::default();
        let client_id = ClientId::from("TEST");
        let endpoint = Ustr::from("test-streams");
        let control = SocketControl::with_registry(client_id, None, endpoint, &registry);
        let _sink = control.sink();
        control.register(move || transport);

        let outcome = LiveNode::request_socket_reconnect(registry.get(client_id, endpoint));

        assert_eq!(outcome, expected);
    }

    #[rstest]
    #[case::client_not_found(
        SocketReconnectLookup::ClientNotFound,
        SocketReconnectDispatchOutcome::UnknownClient
    )]
    #[case::unsupported(
        SocketReconnectLookup::Unsupported,
        SocketReconnectDispatchOutcome::Unsupported
    )]
    #[case::endpoint_not_found(
        SocketReconnectLookup::EndpointNotFound,
        SocketReconnectDispatchOutcome::UnknownEndpoint
    )]
    #[case::ambiguous(
        SocketReconnectLookup::AmbiguousEndpoint,
        SocketReconnectDispatchOutcome::AmbiguousEndpoint
    )]
    fn test_request_socket_reconnect_maps_lookup_failure(
        #[case] lookup: SocketReconnectLookup,
        #[case] expected: SocketReconnectDispatchOutcome,
    ) {
        assert_eq!(LiveNode::request_socket_reconnect(lookup), expected);
    }

    #[rstest]
    fn test_system_dispatch_restores_context(#[values(false, true)] startup: bool) {
        let trader_id = TraderId::from("SOCKET-001");

        let config = LiveNodeConfig {
            trader_id,
            ..Default::default()
        };

        let mut node = LiveNode::build("SystemDispatchNode".to_string(), Some(config)).unwrap();
        let client_id = ClientId::from("TEST");
        let endpoint = Ustr::from("test-streams");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<u32>>();
        let sender = DispatchSender::new(tx);
        let events = sender.clone();
        msgbus::subscribe_any(
            MessagingSwitchboard::socket_state_changed_pattern(
                Some(client_id),
                Some(endpoint.as_str()),
            ),
            ShareableMessageHandler::from_typed(move |_: &SocketStateChanged| {
                events.send(17u32).unwrap();
            }),
            None,
        );

        let control =
            SocketControl::with_registry(client_id, None, endpoint, &node.socket_registry);
        let _sink = control.sink();
        control.register(move || {
            sender.send(23u32).unwrap();
            ReconnectRequestOutcome::Accepted
        });

        let event = SystemEvent::SocketState(SocketStateChange::new(
            client_id,
            None,
            endpoint,
            SocketState::Connected,
        ));
        let command = SystemCommand::ReconnectSocket(ReconnectSocket::new(
            trader_id,
            client_id,
            endpoint,
            31.into(),
        ));

        if startup {
            let mut pending = PendingEvents::default();
            pending.system_events.push(event.into());
            pending.system_commands.push(command.into());
            node.process_system_events(pending.take_system_events());
            node.process_system_commands(pending.take_system_commands());
            assert!(pending.is_empty());
        } else {
            node.process_runner_event(PendingRunnerEvent::SystemEvent(event.into()));
            node.process_runner_event(PendingRunnerEvent::SystemCommand(command.into()));
        }

        let event_child = rx.try_recv().unwrap();
        let command_child = rx.try_recv().unwrap();
        assert!(event_child.is_rooted());
        assert!(command_child.is_rooted());
        assert_eq!(event_child.dispatch(|value| value), 17);
        assert_eq!(command_child.dispatch(|value| value), 23);
        assert!(rx.is_empty());
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    fn test_process_socket_reconnect_routes_only_matching_trader() {
        let trader_id = TraderId::from("SOCKET-001");

        let config = LiveNodeConfig {
            trader_id,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let node = LiveNode::build("SocketReconnectNode".to_string(), Some(config)).unwrap();
        let client_id = ClientId::from("TEST");
        let endpoint = Ustr::from("test-streams");
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let request_count = Arc::clone(&requests);
        let control =
            SocketControl::with_registry(client_id, None, endpoint, &node.socket_registry);
        let _sink = control.sink();
        control.register(move || {
            request_count.fetch_add(1, Ordering::SeqCst);
            ReconnectRequestOutcome::Accepted
        });

        node.process_system_command(SystemCommand::ReconnectSocket(ReconnectSocket::new(
            TraderId::from("OTHER-001"),
            client_id,
            endpoint,
            UnixNanos::default(),
        )));
        assert_eq!(requests.load(Ordering::SeqCst), 0);

        node.process_system_command(SystemCommand::ReconnectSocket(ReconnectSocket::new(
            trader_id,
            client_id,
            endpoint,
            UnixNanos::default(),
        )));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn paused_ingress_actual_node_handle_invalidates_runner_boundary() {
        let mut node = LiveNode::build("IngressStop".to_string(), None).unwrap();
        let frozen = node.runner.as_ref().unwrap().freeze_ingress().unwrap();
        node.handle().stop();
        assert!(node.handle().should_stop());
        assert!(frozen.finish().is_err());
        assert!(node.ensure_recovery_start_permitted().is_err());
        node.dispose();
        drop(node);

        let mut node = LiveNode::builder(TraderId::from("INGRESS-002"), Environment::Sandbox)
            .unwrap()
            .with_name("IngressBuilderStop")
            .build()
            .unwrap();
        let frozen = node.runner.as_ref().unwrap().freeze_ingress().unwrap();
        node.handle().stop();
        assert!(frozen.finish().is_err());
        node.dispose();
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_closes_retained_recovery_runner_handoff() {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("RECOVERY-HANDOFF-START-001"),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };
        let mut node = LiveNode::build("RecoveryHandoffStart".to_string(), Some(config)).unwrap();
        let mut handoff = node.recovery_runner_handoff().unwrap();
        let registry = RunnerRecoveryCodecRegistry::new([]).seal().unwrap();

        node.start().await.unwrap();
        assert!(
            handoff
                .enqueue_batch(std::iter::empty::<RunnerRecoveryEnvelope>(), &registry)
                .is_err()
        );
        assert!(node.recovery_runner_handoff().is_err());
        node.stop().await.unwrap();
        node.dispose();
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_publishes_socket_change_after_actor_subscribes() {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("SOCKET-STARTUP-001"),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("SocketStartupNode".to_string(), Some(config)).unwrap();
        let received = Rc::new(RefCell::new(Vec::new()));
        node.add_actor(StartupSocketActor::new(Rc::clone(&received)))
            .unwrap();
        node.runner.as_ref().unwrap().bind_senders();

        let change = SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            Ustr::from("binance-futures-market-streams"),
            SocketState::Connected,
        );
        get_system_event_sender()
            .send(SystemEvent::SocketState(change))
            .unwrap();

        node.start().await.unwrap();

        {
            let events = received.borrow();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].trader_id, TraderId::from("SOCKET-STARTUP-001"));
            assert_eq!(events[0].client_id, change.client_id);
            assert_eq!(events[0].venue, change.venue);
            assert_eq!(events[0].endpoint, change.endpoint);
            assert_eq!(events[0].state, change.state);
            assert_ne!(events[0].event_id, UUID4::default());
            assert_ne!(events[0].ts_event, UnixNanos::default());
            assert_eq!(events[0].ts_init, events[0].ts_event);
        }

        node.stop().await.unwrap();
        node.dispose();
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_publishes_queue_state_after_dispatch_sample() {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("QUEUE-RUN-001"),
            queue_monitor: Some(crate::config::QueueMonitorConfig {
                queue_depth_trigger: usize::MAX,
                queue_depth_clear: 0,
                mean_dispatch_ns_trigger: 1,
                mean_dispatch_ns_clear: 0,
            }),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("QueueMonitorRunNode".to_string(), Some(config)).unwrap();
        let handle = node.handle();
        let received = Rc::new(RefCell::new(Vec::<QueueStateChanged>::new()));

        let handler = ShareableMessageHandler::from_typed({
            let received = received.clone();
            let stop_handle = handle.clone();

            move |event: &QueueStateChanged| {
                received.borrow_mut().push(event.clone());
                stop_handle.stop();
            }
        });

        msgbus::subscribe_any(
            MessagingSwitchboard::queue_state_changed_pattern(None),
            handler,
            None,
        );
        let drive_handle = handle.clone();

        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async move {
                wait_until_async(
                    || async { drive_handle.is_running() },
                    Duration::from_secs(2),
                )
                .await;
                get_data_event_sender().send(stub_data_event()).unwrap();
            };

            let (run_result, ()) = tokio::join!(run, drive);
            run_result
        })
        .await;

        assert!(
            result.is_ok(),
            "queue state event should arrive before timeout"
        );
        assert!(result.unwrap().is_ok(), "run() should succeed");
        let events = received.borrow();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].trader_id, TraderId::from("QUEUE-RUN-001"));
        assert_eq!(events[0].channel, SystemChannel::DataEvents);
        assert_eq!(events[0].condition, QueueCondition::Slow);
        assert_eq!(events[0].state, QueueState::Triggered);
        assert_eq!(events[0].queue_depth, 0);
        assert!(events[0].mean_dispatch_ns > 0);
        assert_ne!(events[0].event_id, UUID4::default());
        assert_ne!(events[0].ts_event, UnixNanos::default());
        assert_eq!(events[0].ts_init, events[0].ts_event);
        drop(events);
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    fn test_observe_exec_event_before_dispatch_skips_recent_fill_report() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("FillSkipNode".to_string(), Some(config)).unwrap();
        let event = stub_exec_event();
        let account_id = AccountId::from("TEST-001");
        let instrument_id = InstrumentId::from("TEST.VENUE");
        let trade_id = TradeId::from("T-001");

        let close_ids = node.observe_exec_event_before_dispatch(&event);
        assert_eq!(close_ids, Some(Vec::new()));
        assert!(
            !node
                .exec_manager
                .is_fill_recently_processed(account_id, instrument_id, trade_id)
        );

        node.exec_manager
            .mark_fill_processed(account_id, instrument_id, trade_id);

        let close_ids = node.observe_exec_event_before_dispatch(&event);
        assert_eq!(close_ids, None);
    }

    #[rstest]
    #[case(false, false, OrderStatus::Canceled, 1)]
    #[case(false, true, OrderStatus::Accepted, 0)]
    #[case(true, false, OrderStatus::Canceled, 1)]
    #[case(true, true, OrderStatus::Accepted, 0)]
    fn test_process_exec_event_clears_terminal_activity_only_after_cached_order_closes(
        #[case] with_fills: bool,
        #[case] superseded: bool,
        #[case] expected_status: OrderStatus,
        #[case] expected_query_count: usize,
    ) {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                open_check_threshold_ms: 5_000,
                single_order_query_delay_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("TerminalReportNode".to_string(), Some(config)).unwrap();
        let client_order_id = ClientOrderId::from("O-TERMINAL-REPORT");
        let old_venue_order_id = VenueOrderId::from("V-TERMINAL-REPORT-OLD");
        let new_venue_order_id = VenueOrderId::from("V-TERMINAL-REPORT-NEW");
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("TEST");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let account = AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::from("0 USDT"),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                nautilus_common::recovery_trace::native_event_uuid(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        ));
        node.kernel.cache.borrow_mut().add_account(account).unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument_id,
            client_order_id,
            old_venue_order_id,
        );

        if superseded {
            let order = node
                .kernel
                .cache
                .borrow()
                .order_owned(&client_order_id)
                .unwrap();
            let pending_update = OrderPendingUpdateSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .venue_order_id(old_venue_order_id)
                .build();
            node.kernel
                .cache
                .borrow_mut()
                .update_order(&OrderEventAny::PendingUpdate(pending_update))
                .unwrap();
            let order = node
                .kernel
                .cache
                .borrow()
                .order_owned(&client_order_id)
                .unwrap();
            let updated = OrderUpdatedSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .quantity(order.quantity())
                .venue_order_id(new_venue_order_id)
                .account_id(account_id)
                .build();
            node.kernel
                .cache
                .borrow_mut()
                .update_order(&OrderEventAny::Updated(updated))
                .unwrap();
        }

        let report = OrderStatusReport::new(
            account_id,
            instrument_id,
            Some(client_order_id),
            old_venue_order_id,
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::Canceled,
            Quantity::from("10.0"),
            Quantity::from("0.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(2_000),
            UnixNanos::from(3_000),
            None,
        );

        let report = if with_fills {
            ExecutionReport::OrderWithFills(Box::new(report), Vec::new())
        } else {
            ExecutionReport::Order(Box::new(report))
        };

        let event = ExecutionEvent::Report(report);

        node.process_exec_event(event);

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();

        let expected_venue_order_id = if superseded {
            new_venue_order_id
        } else {
            old_venue_order_id
        };

        assert_eq!(order.status(), expected_status);
        assert_eq!(order.venue_order_id(), Some(expected_venue_order_id));

        if !superseded {
            let replacement = OrderTestBuilder::new(OrderType::Limit)
                .client_order_id(client_order_id)
                .instrument_id(instrument_id)
                .quantity(Quantity::from("20.0"))
                .price(Price::from("200.0"))
                .build();
            let submitted = TestOrderEventStubs::submitted(&replacement, account_id);
            node.kernel
                .cache
                .borrow_mut()
                .add_order(replacement, None, Some(client_id), true)
                .unwrap();
            let replacement = node
                .kernel
                .cache
                .borrow_mut()
                .update_order(&submitted)
                .unwrap();
            let accepted =
                TestOrderEventStubs::accepted(&replacement, account_id, old_venue_order_id);
            node.kernel
                .cache
                .borrow_mut()
                .update_order(&accepted)
                .unwrap();
        }

        assert_eq!(
            node.exec_manager.check_open_order_queries().len(),
            expected_query_count,
        );
    }

    #[rstest]
    fn test_rejected_direct_fill_stays_eligible_for_later_report() {
        let (mut node, mut fill_event, _) = recent_fill_test_fixture("RejectedDirectFillNode");

        let OrderEventAny::Filled(fill) = &mut fill_event else {
            unreachable!();
        };

        fill.client_order_id = ClientOrderId::from("O-UNKNOWN");
        fill.venue_order_id = VenueOrderId::from("V-UNKNOWN");
        let fill = fill.clone();
        let report_event = fill_report_event(&fill);
        let event = ExecutionEvent::Order(OrderEventAny::Filled(fill.clone()));

        assert!(node.observe_exec_event_before_dispatch(&event).is_some());
        let marked_before_dispatch = is_recent_fill(&node, &fill);

        node.dispatch_exec_event_and_commit_fill(event);

        let marked_after_dispatch = is_recent_fill(&node, &fill);
        let later_report_is_eligible = node
            .observe_exec_event_before_dispatch(&report_event)
            .is_some();
        assert_eq!(
            (
                marked_before_dispatch,
                marked_after_dispatch,
                later_report_is_eligible,
            ),
            (false, false, true),
        );
    }

    #[rstest]
    fn test_applied_direct_fill_commits_and_skips_later_report() {
        let (mut node, fill_event, _) = recent_fill_test_fixture("AppliedDirectFillNode");

        let OrderEventAny::Filled(fill) = &fill_event else {
            unreachable!();
        };

        let fill = fill.clone();
        let report_event = fill_report_event(&fill);
        let event = ExecutionEvent::Order(fill_event);

        assert!(node.observe_exec_event_before_dispatch(&event).is_some());
        assert!(!is_recent_fill(&node, &fill));

        node.dispatch_exec_event_and_commit_fill(event);

        assert!(is_recent_fill(&node, &fill));
        assert_eq!(node.observe_exec_event_before_dispatch(&report_event), None);
    }

    #[rstest]
    fn test_canonical_duplicate_fill_counts_as_applied() {
        let (mut node, fill_event, _) = recent_fill_test_fixture("DuplicateDirectFillNode");

        let OrderEventAny::Filled(fill) = &fill_event else {
            unreachable!();
        };

        let mut fill = fill.clone();
        node.kernel
            .cache
            .borrow_mut()
            .update_order(&fill_event)
            .unwrap();
        fill.client_order_id = ClientOrderId::from("O-DUPLICATE-UNKNOWN");

        node.exec_manager.commit_recent_fill_if_applied(&fill);

        assert!(is_recent_fill(&node, &fill));
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn test_continuous_reconciliation_commits_only_applied_fill(#[case] applied: bool) {
        let (mut node, mut fill_event, _) = recent_fill_test_fixture(if applied {
            "AppliedContinuousFillNode"
        } else {
            "RejectedContinuousFillNode"
        });

        if !applied {
            let OrderEventAny::Filled(fill) = &mut fill_event else {
                unreachable!();
            };

            fill.client_order_id = ClientOrderId::from("O-CONTINUOUS-UNKNOWN");
            fill.venue_order_id = VenueOrderId::from("V-CONTINUOUS-UNKNOWN");
        }

        let OrderEventAny::Filled(fill) = &fill_event else {
            unreachable!();
        };

        let fill = fill.clone();

        node.process_reconciliation_events(&[fill_event]);

        assert_eq!(is_recent_fill(&node, &fill), applied);
    }

    #[rstest]
    fn test_recent_fill_commit_requires_account_and_instrument_match() {
        let (mut node, fill_event, _) = recent_fill_test_fixture("MismatchedDirectFillNode");
        node.kernel
            .cache
            .borrow_mut()
            .update_order(&fill_event)
            .unwrap();

        let OrderEventAny::Filled(fill) = fill_event else {
            unreachable!();
        };

        let mut account_mismatch = fill.clone();
        account_mismatch.account_id = AccountId::from("OTHER-001");
        let mut instrument_mismatch = fill;
        instrument_mismatch.instrument_id = InstrumentId::from("OTHER.VENUE");

        node.exec_manager
            .commit_recent_fill_if_applied(&account_mismatch);
        node.exec_manager
            .commit_recent_fill_if_applied(&instrument_mismatch);

        assert!(!is_recent_fill(&node, &account_mismatch));
        assert!(!is_recent_fill(&node, &instrument_mismatch));
    }

    #[rstest]
    fn test_applied_inferred_fill_remains_recently_processed() {
        let (mut node, _, instrument) = recent_fill_test_fixture("InferredFillNode");
        let client_order_id = ClientOrderId::from("O-RECENT-FILL");
        let venue_order_id = VenueOrderId::from("V-RECENT-FILL");
        let account_id = AccountId::from("TEST-001");
        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let report = OrderStatusReport::new(
            account_id,
            instrument.id(),
            Some(client_order_id),
            venue_order_id,
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0"),
            Quantity::from("1.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            None,
        )
        .with_avg_px(dec!(100.0));
        let inferred = create_inferred_fill_for_qty(
            &order,
            &report,
            &account_id,
            &instrument,
            Quantity::from("1.0"),
            UnixNanos::from(1_000),
            None,
        )
        .unwrap();

        let OrderEventAny::Filled(fill) = &inferred else {
            unreachable!();
        };

        let fill = fill.clone();

        node.process_reconciliation_events(&[inferred]);

        assert!(fill.reconciliation);
        assert!(is_recent_fill(&node, &fill));
    }

    #[rstest]
    #[tokio::test]
    async fn test_request_position_fill_reports_keeps_failed_query_unsuccessful() {
        let client_id = ClientId::from("POSITION-FILLS");
        let account_id = AccountId::from("POSITION-FILLS-001");
        let instrument_id = crypto_perpetual_ethusdt().id();
        let commands = Rc::new(RefCell::new(Vec::new()));

        let client = LiveExecutionClient::new(Box::new(FillReportClient {
            client_id,
            account_id,
            venue: instrument_id.venue,
            outcome: FillReportClientOutcome::Failure,
            commands: commands.clone(),
        }));

        let command = GenerateFillReports::new(
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::from(2_000),
            Some(instrument_id),
            None,
            Some(UnixNanos::from(1_000)),
            Some(UnixNanos::from(2_000)),
            None,
            None,
        );

        let result = request_position_fill_reports(
            vec![client],
            vec![PositionFillReportQuery {
                key: (instrument_id, account_id),
                client_id,
                command: command.clone(),
            }],
        )
        .await;

        assert_eq!(*commands.borrow(), vec![command]);
        assert!(result.successful_keys.is_empty());
        assert!(result.reports.is_empty());
    }

    #[rstest]
    #[tokio::test]
    async fn test_request_position_fill_reports_accepts_scoped_response() {
        let client_id = ClientId::from("POSITION-FILLS");
        let account_id = AccountId::from("POSITION-FILLS-001");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();

        let report_b = FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::from("V-POSITION-FILLS"),
            TradeId::from("T-POSITION-FILLS-B"),
            OrderSide::Buy,
            Quantity::from("1.0"),
            Price::from("100.0"),
            Money::zero(instrument.quote_currency()),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-POSITION-FILLS")),
            None,
            UnixNanos::from(1_500),
            UnixNanos::from(2_000),
            None,
        );
        let mut report_a = report_b.clone();
        report_a.trade_id = TradeId::from("T-POSITION-FILLS-A");
        let commands = Rc::new(RefCell::new(Vec::new()));

        let client = LiveExecutionClient::new(Box::new(FillReportClient {
            client_id,
            account_id,
            venue: instrument_id.venue,
            outcome: FillReportClientOutcome::Reports(vec![report_b.clone(), report_a.clone()]),
            commands,
        }));

        let command = GenerateFillReports::new(
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::from(2_000),
            Some(instrument_id),
            None,
            Some(UnixNanos::from(1_000)),
            Some(UnixNanos::from(2_000)),
            None,
            None,
        );
        let key = (instrument_id, account_id);

        let result = request_position_fill_reports(
            vec![client],
            vec![PositionFillReportQuery {
                key,
                client_id,
                command,
            }],
        )
        .await;

        assert_eq!(result.successful_keys, IndexSet::from([key]));
        assert_eq!(
            result.reports,
            IndexMap::from([(key, vec![report_a, report_b])])
        );
    }

    #[rstest]
    #[case("OTHER-001", "ETHUSDT-PERP.BINANCE", 1_500, "1.0")]
    #[case("POSITION-FILLS-001", "BTCUSDT-PERP.BINANCE", 1_500, "1.0")]
    #[case("POSITION-FILLS-001", "ETHUSDT-PERP.BINANCE", 1_500, "0.0")]
    #[tokio::test]
    async fn test_request_position_fill_reports_rejects_out_of_scope_response(
        #[case] report_account: &str,
        #[case] report_instrument: &str,
        #[case] ts_event: u64,
        #[case] quantity: &str,
    ) {
        let client_id = ClientId::from("POSITION-FILLS");
        let account_id = AccountId::from("POSITION-FILLS-001");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();

        let report = FillReport::new(
            AccountId::from(report_account),
            InstrumentId::from(report_instrument),
            VenueOrderId::from("V-POSITION-FILLS"),
            TradeId::from("T-POSITION-FILLS"),
            OrderSide::Buy,
            Quantity::from(quantity),
            Price::from("100.0"),
            Money::zero(instrument.quote_currency()),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-POSITION-FILLS")),
            None,
            UnixNanos::from(ts_event),
            UnixNanos::from(2_000),
            None,
        );

        let client = LiveExecutionClient::new(Box::new(FillReportClient {
            client_id,
            account_id,
            venue: instrument_id.venue,
            outcome: FillReportClientOutcome::Reports(vec![report]),
            commands: Rc::new(RefCell::new(Vec::new())),
        }));

        let command = GenerateFillReports::new(
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::from(2_000),
            Some(instrument_id),
            None,
            Some(UnixNanos::from(1_000)),
            Some(UnixNanos::from(2_000)),
            None,
            None,
        );
        let key = (instrument_id, account_id);

        let result = request_position_fill_reports(
            vec![client],
            vec![PositionFillReportQuery {
                key,
                client_id,
                command,
            }],
        )
        .await;

        assert!(result.successful_keys.is_empty());
        assert!(result.reports.is_empty());
    }

    #[rstest]
    #[case(999)]
    #[case(2_001)]
    #[tokio::test]
    async fn test_request_position_fill_reports_filters_time_window_superset(
        #[case] ts_event: u64,
    ) {
        let client_id = ClientId::from("POSITION-FILLS");
        let account_id = AccountId::from("POSITION-FILLS-001");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();

        let report = FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::from("V-POSITION-FILLS"),
            TradeId::from("T-POSITION-FILLS"),
            OrderSide::Buy,
            Quantity::from("1.0"),
            Price::from("100.0"),
            Money::zero(instrument.quote_currency()),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-POSITION-FILLS")),
            None,
            UnixNanos::from(ts_event),
            UnixNanos::from(2_000),
            None,
        );

        let client = LiveExecutionClient::new(Box::new(FillReportClient {
            client_id,
            account_id,
            venue: instrument_id.venue,
            outcome: FillReportClientOutcome::Reports(vec![report]),
            commands: Rc::new(RefCell::new(Vec::new())),
        }));

        let command = GenerateFillReports::new(
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::from(2_000),
            Some(instrument_id),
            None,
            Some(UnixNanos::from(1_000)),
            Some(UnixNanos::from(2_000)),
            None,
            None,
        );
        let key = (instrument_id, account_id);

        let result = request_position_fill_reports(
            vec![client],
            vec![PositionFillReportQuery {
                key,
                client_id,
                command,
            }],
        )
        .await;

        assert_eq!(result.successful_keys, IndexSet::from([key]));
        assert_eq!(result.reports, IndexMap::from([(key, Vec::new())]));
    }

    #[rstest]
    #[case::query_error(false)]
    #[case::missing_client(true)]
    #[tokio::test]
    async fn test_position_fill_collection_discards_partial_failure(
        #[case] missing_client: bool,
        #[values(false, true)] failure_first: bool,
        position_fill_query: (FillReport, PositionFillReportQuery),
    ) {
        let (report, query) = position_fill_query;
        let failed_client_id = ClientId::from("FAILED-FILLS");
        let healthy_key = (report.instrument_id, AccountId::from("HEALTHY-001"));
        let mut healthy_report = report.clone();
        healthy_report.account_id = healthy_key.1;
        let healthy_client_id = ClientId::from("HEALTHY-FILLS");
        let mut clients = vec![
            LiveExecutionClient::new(Box::new(FillReportClient {
                client_id: query.client_id,
                account_id: query.key.1,
                venue: query.key.0.venue,
                outcome: FillReportClientOutcome::Reports(vec![report]),
                commands: Rc::new(RefCell::new(Vec::new())),
            })),
            LiveExecutionClient::new(Box::new(FillReportClient {
                client_id: healthy_client_id,
                account_id: healthy_key.1,
                venue: healthy_key.0.venue,
                outcome: FillReportClientOutcome::Reports(vec![healthy_report.clone()]),
                commands: Rc::new(RefCell::new(Vec::new())),
            })),
        ];

        if !missing_client {
            clients.push(LiveExecutionClient::new(Box::new(FillReportClient {
                client_id: failed_client_id,
                account_id: query.key.1,
                venue: query.key.0.venue,
                outcome: FillReportClientOutcome::Failure,
                commands: Rc::new(RefCell::new(Vec::new())),
            })));
        }

        let failed_query = PositionFillReportQuery {
            client_id: failed_client_id,
            key: query.key,
            command: query.command.clone(),
        };

        let healthy_query = PositionFillReportQuery {
            key: healthy_key,
            client_id: healthy_client_id,
            command: query.command.clone(),
        };

        let mut queries = vec![query, failed_query];

        if failure_first {
            queries.reverse();
        }

        queries.push(healthy_query);

        let result = request_position_fill_reports(clients, queries).await;

        assert_eq!(result.successful_keys, IndexSet::from([healthy_key]));
        assert_eq!(
            result.reports,
            IndexMap::from([(healthy_key, vec![healthy_report])])
        );
    }

    #[rstest]
    #[case::same_economics("metadata", true)]
    #[case::venue_order("venue_order", false)]
    #[case::side("side", false)]
    #[case::quantity("quantity", false)]
    #[case::price("price", false)]
    #[case::commission("commission", false)]
    #[case::liquidity("liquidity", false)]
    #[case::average("average", false)]
    #[case::event_time("event_time", false)]
    #[case::client_order("client_order", false)]
    #[case::position("position", false)]
    #[tokio::test]
    async fn test_position_fill_collection_validates_duplicate_economics(
        #[case] changed: &str,
        #[case] accepted: bool,
        position_fill_query: (FillReport, PositionFillReportQuery),
    ) {
        let (report, query) = position_fill_query;
        let mut duplicate = report.clone();

        match changed {
            "metadata" => {
                duplicate.report_id = UUID4::new();
                duplicate.ts_init = UnixNanos::from(3_000);
            }
            "venue_order" => duplicate.venue_order_id = VenueOrderId::from("OTHER-ORDER"),
            "side" => duplicate.order_side = OrderSide::Sell,
            "quantity" => duplicate.last_qty = Quantity::from("2.0"),
            "price" => duplicate.last_px = Price::from("101.0"),
            "commission" => duplicate.commission = Money::from("0.25 USDT"),
            "liquidity" => duplicate.liquidity_side = LiquiditySide::Maker,
            "average" => duplicate.avg_px = Some(dec!(100.5)),
            "event_time" => duplicate.ts_event = UnixNanos::from(1_501),
            "client_order" => duplicate.client_order_id = None,
            "position" => duplicate.venue_position_id = Some(PositionId::from("OTHER-POSITION")),
            _ => unreachable!(),
        }

        let key = query.key;

        let client = LiveExecutionClient::new(Box::new(FillReportClient {
            client_id: query.client_id,
            account_id: key.1,
            venue: key.0.venue,
            outcome: FillReportClientOutcome::Reports(vec![report.clone(), duplicate]),
            commands: Rc::new(RefCell::new(Vec::new())),
        }));

        let result = request_position_fill_reports(vec![client], vec![query]).await;

        if accepted {
            assert_eq!(result.successful_keys, IndexSet::from([key]));
            assert_eq!(result.reports, IndexMap::from([(key, vec![report])]));
        } else {
            assert!(result.successful_keys.is_empty());
            assert!(result.reports.is_empty());
        }
    }

    #[fixture]
    fn position_fill_query() -> (FillReport, PositionFillReportQuery) {
        let account_id = AccountId::from("POSITION-FILLS-001");
        let instrument_id = crypto_perpetual_ethusdt().id();

        let report = FillReport::new(
            account_id,
            instrument_id,
            VenueOrderId::from("V-POSITION-FILLS"),
            TradeId::from("T-POSITION-FILLS"),
            OrderSide::Buy,
            Quantity::from("1.0"),
            Price::from("100.0"),
            Money::from("0.10 USDT"),
            LiquiditySide::Taker,
            Some(ClientOrderId::from("O-POSITION-FILLS")),
            None,
            UnixNanos::from(1_500),
            UnixNanos::from(2_000),
            None,
        );

        let query = PositionFillReportQuery {
            key: (instrument_id, account_id),
            client_id: ClientId::from("POSITION-FILLS"),
            command: GenerateFillReports::new(
                UUID4::new(),
                UnixNanos::from(2_000),
                Some(instrument_id),
                None,
                Some(UnixNanos::from(1_000)),
                Some(UnixNanos::from(2_000)),
                None,
                None,
            ),
        };

        (report, query)
    }

    #[rstest]
    #[case::failed_query(false)]
    #[case::local_activity(true)]
    fn test_position_fallback_preserves_deferred_venue_only_retries(#[case] local_activity: bool) {
        let (mut node, report, _) =
            position_fill_test_fixture("PositionRetryRetentionNode", Quantity::from("1.0"));
        let active_key = (report.instrument_id, report.account_id);
        let deferred_key = (report.instrument_id, AccountId::from("SECOND-001"));
        let deferred_reports = vec![
            PositionStatusReport::new(
                deferred_key.1,
                deferred_key.0,
                PositionSide::Long,
                Quantity::from("2.0"),
                UnixNanos::from(1_000),
                UnixNanos::from(1_000),
                None,
                None,
                Some(dec!(100.0)),
            ),
            PositionStatusReport::new(
                deferred_key.1,
                deferred_key.0,
                PositionSide::Short,
                Quantity::from("1.0"),
                UnixNanos::from(1_000),
                UnixNanos::from(1_000),
                None,
                None,
                Some(dec!(100.0)),
            ),
        ];
        let mut check = node.exec_manager.prepare_position_report_check(
            nautilus_common::recovery_trace::native_event_uuid(),
            &[],
        );
        check.client_coverage.clear();
        let events = node.exec_manager.reconcile_position_reports(
            &check,
            deferred_reports.clone(),
            &IndexSet::new(),
            &IndexSet::new(),
        );
        assert!(events.is_empty());
        assert_eq!(
            node.exec_manager.position_recon_retry_count(&deferred_key),
            1
        );

        let mut position_result = position_report_result(&node, report);
        position_result.reports.extend(deferred_reports);
        position_result.check.client_coverage.insert(
            deferred_key,
            ReportClientCoverage::Resolved(IndexSet::from([ClientId::from("SECOND")])),
        );
        position_result
            .check
            .activity_revisions
            .insert(deferred_key, 0);
        position_result
            .queried_clients
            .insert(ClientId::from("SECOND"));
        let mut successful_keys = IndexSet::from([active_key]);

        if local_activity {
            successful_keys.insert(deferred_key);
            node.exec_manager
                .record_position_activity(deferred_key.0, deferred_key.1);
        }

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(active_key, Vec::new())]),
            successful_keys,
        });

        assert_eq!(
            node.exec_manager.position_recon_retry_count(&deferred_key),
            1
        );
        {
            let cache = node.kernel.cache.borrow();
            let positions =
                cache.positions_open(None, Some(&active_key.0), None, Some(&active_key.1), None);
            assert_eq!(
                positions
                    .iter()
                    .map(|position| position.quantity)
                    .collect::<Vec<_>>(),
                vec![Quantity::from("1.0"), Quantity::from("1.0")],
            );
            assert_eq!(
                cache
                    .positions_open(
                        None,
                        Some(&deferred_key.0),
                        None,
                        Some(&deferred_key.1),
                        None,
                    )
                    .len(),
                0
            );
        }

        let mut fresh_check = node.exec_manager.prepare_position_report_check(
            nautilus_common::recovery_trace::native_event_uuid(),
            &[],
        );
        node.exec_manager.plan_position_fill_reports(
            &mut fresh_check,
            &[],
            &IndexSet::new(),
            &IndexSet::new(),
            &[],
        );

        assert_eq!(
            node.exec_manager.position_recon_retry_count(&deferred_key),
            0
        );
    }

    #[rstest]
    fn test_position_fill_report_result_applies_authoritative_fill_without_synthetic_order() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("AuthoritativePositionFillNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let revision = node.exec_manager.position_activity_revision(&key);
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(
            positions
                .iter()
                .map(|position| position.quantity)
                .sum::<Quantity>(),
            Quantity::from("2.0")
        );
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
        drop(positions);
        drop(cache);
        assert!(
            node.exec_manager
                .position_contains_fill_report(&fill_report)
        );
        assert_eq!(
            node.exec_manager.position_activity_revision(&key),
            revision + 1
        );
    }

    #[rstest]
    fn test_position_fill_report_match_ignores_adapter_timestamp_source() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("PositionFillTimestampNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let mut rest_report = fill_report;
        rest_report.ts_event = UnixNanos::from(2_000);

        assert!(
            node.exec_manager
                .position_contains_fill_report(&rest_report)
        );
    }

    #[rstest]
    fn test_position_fill_report_result_falls_back_when_order_contains_inferred_fill() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("InferredPositionFillNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let client_order_id = fill_report.client_order_id.unwrap();
        apply_inferred_position_fill(&mut node, &fill_report);

        let venue_report = PositionStatusReport::new(
            key.1,
            key.0,
            PositionSide::Long,
            Quantity::from("3.0"),
            venue_report.ts_last,
            venue_report.ts_init,
            None,
            None,
            venue_report.avg_px_open,
        );
        let mut later_fill_report = fill_report.clone();
        later_fill_report.trade_id = TradeId::from("T-POSITION-AUTHORITATIVE-LATER");
        later_fill_report.ts_event = UnixNanos::from(1_001);
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone(), later_fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let order = cache.order(&client_order_id).unwrap();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(order.filled_qty(), Quantity::from("2.0"));
        assert!(!order.trade_ids().contains(&&fill_report.trade_id));
        assert!(!order.trade_ids().contains(&&later_fill_report.trade_id));
        assert_eq!(
            positions
                .iter()
                .map(|position| position.quantity)
                .sum::<Quantity>(),
            Quantity::from("3.0")
        );
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            2
        );
    }

    #[rstest]
    fn test_position_fill_report_result_falls_back_when_reconciled_position_includes_fill() {
        let (mut node, _, _) =
            position_fill_test_fixture("ReconciledPositionFillNode", Quantity::from("1.0"));
        let account_id = AccountId::from("TEST-001");
        let instrument = InstrumentAny::CurrencyPair(currency_pair_btcusdt());
        let key = (instrument.id(), account_id);
        let commission = Money::zero(instrument.quote_currency());
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();

        let order = OrderTestBuilder::new(OrderType::Market)
            .strategy_id(StrategyId::external())
            .instrument_id(instrument.id())
            .client_order_id(ClientOrderId::from("O-RECONCILED"))
            .quantity(Quantity::from("1.000000"))
            .tags(vec![Ustr::from(RECONCILIATION_ORDER_TAG)])
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, account_id);
        let accepted =
            TestOrderEventStubs::accepted(&order, account_id, VenueOrderId::from("V-RECONCILED"));
        node.kernel
            .cache
            .borrow_mut()
            .add_order(
                order.clone(),
                None,
                Some(ClientId::from("POSITION-FILLS")),
                false,
            )
            .unwrap();
        node.process_reconciliation_events(&[submitted, accepted]);
        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&order.client_order_id())
            .unwrap();
        let opening_fill = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("T-RECONCILED")),
            None,
            Some(Price::from("100.00")),
            Some(Quantity::from("1.000000")),
            Some(LiquiditySide::Taker),
            Some(commission),
            Some(UnixNanos::from(2_000)),
            Some(account_id),
        );
        node.process_reconciliation_events(&[opening_fill]);

        // A real fill between the position report query and reconciliation stays off the position
        let held_fill_report = FillReport::new(
            account_id,
            instrument.id(),
            VenueOrderId::from("V-HELD"),
            TradeId::from("T-HELD"),
            OrderSide::Buy,
            Quantity::from("1.000000"),
            Price::from("100.00"),
            commission,
            LiquiditySide::Taker,
            None,
            None,
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            None,
        );
        node.kernel
            .exec_engine
            .borrow_mut()
            .reconcile_fill_report(&held_fill_report);

        let venue_report = PositionStatusReport::new(
            account_id,
            instrument.id(),
            PositionSide::Long,
            Quantity::from("2.000000"),
            UnixNanos::from(3_000),
            UnixNanos::from(3_000),
            None,
            None,
            Some(dec!(100.00)),
        );
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![held_fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let held_order = cache.order(&ClientOrderId::from("V-HELD")).unwrap();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(held_order.filled_qty(), Quantity::from("1.000000"));
        assert_eq!(
            positions
                .iter()
                .map(|position| position.quantity)
                .sum::<Quantity>(),
            Quantity::from("2.000000")
        );
        assert!(
            positions
                .iter()
                .all(|position| !position.trade_ids.contains(&held_fill_report.trade_id))
        );
    }

    #[rstest]
    fn test_position_fill_report_validates_hedge_identity_before_inferred_fallback() {
        let (mut node, _, mut fill_report) =
            position_fill_test_fixture("InferredHedgeIdentityNode", Quantity::from("1.0"));
        apply_inferred_position_fill(&mut node, &fill_report);
        let conflicting_position_id = PositionId::from("P-POSITION-CONFLICT");
        fill_report.venue_position_id = Some(conflicting_position_id);

        let error = node
            .exec_manager
            .prepare_position_fill_report(&mut fill_report, &[])
            .unwrap_err();

        assert!(
            error.to_string().contains(&format!(
                "position ID {conflicting_position_id} conflicts with cached order position"
            )),
            "{error:#}"
        );
    }

    #[rstest]
    #[case::not_applied_exactly("100.0", "O-POSITION-FILLS")]
    #[case::preparation_error("1.0", "O-POSITION-CONFLICT")]
    fn test_position_fill_report_result_falls_back_after_unapplied_fill_expires(
        #[case] authoritative_qty: &str,
        #[case] client_order_id: &str,
    ) {
        let (mut node, venue_report, mut fill_report) = position_fill_test_fixture(
            "UnappliedPositionFillNode",
            Quantity::from(authoritative_qty),
        );
        fill_report.client_order_id = Some(ClientOrderId::from(client_order_id));
        let reports = [fill_report.clone()];

        let quantities = (0..2)
            .map(|_| position_quantity_after_check(&mut node, &venue_report, &reports))
            .collect::<Vec<_>>();

        assert_eq!(
            quantities,
            vec![Quantity::from("1.0"), Quantity::from("2.0")]
        );
        assert!(
            !node
                .exec_manager
                .position_contains_fill_report(&fill_report)
        );
    }

    #[rstest]
    fn test_position_fill_report_result_expires_one_unapplied_fill_per_check() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("UnappliedPositionFillsNode", Quantity::from("100.0"));
        let mut later_fill_report = fill_report.clone();
        later_fill_report.trade_id = TradeId::from("T-POSITION-AUTHORITATIVE-LATER");
        later_fill_report.ts_event = UnixNanos::from(1_001);
        let reports = [fill_report.clone(), later_fill_report.clone()];

        let quantities = (0..3)
            .map(|_| position_quantity_after_check(&mut node, &venue_report, &reports))
            .collect::<Vec<_>>();

        assert_eq!(
            quantities,
            vec![
                Quantity::from("1.0"),
                Quantity::from("1.0"),
                Quantity::from("2.0"),
            ]
        );
        assert!(
            !node
                .exec_manager
                .position_contains_fill_report(&fill_report)
        );
        assert!(
            !node
                .exec_manager
                .position_contains_fill_report(&later_fill_report)
        );
    }

    #[rstest]
    #[case::within_grace(60_000, true)]
    #[case::after_grace(0, false)]
    fn test_position_fill_report_result_retries_refused_fill_only_within_grace(
        #[case] position_check_threshold_ms: u32,
        #[case] expected_applied: bool,
    ) {
        let (mut node, venue_report, fill_report) = position_fill_test_fixture_with_threshold(
            "TransientPositionFillNode",
            Quantity::from("1.0"),
            position_check_threshold_ms,
        );
        let mut conflicting_report = fill_report.clone();
        conflicting_report.client_order_id = Some(ClientOrderId::from("O-POSITION-CONFLICT"));

        let quantities = [conflicting_report, fill_report.clone()]
            .into_iter()
            .map(|report| position_quantity_after_check(&mut node, &venue_report, &[report]))
            .collect::<Vec<_>>();

        assert_eq!(
            quantities,
            vec![Quantity::from("1.0"), Quantity::from("2.0")]
        );
        assert_eq!(
            node.exec_manager
                .position_contains_fill_report(&fill_report),
            expected_applied
        );
    }

    #[rstest]
    fn test_position_fill_report_result_synthesizes_only_residual_after_fresh_report() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("ResidualPositionFillNode", Quantity::from("0.5"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let first_result = position_report_result(&node, venue_report.clone());

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result: first_result,
            reports: IndexMap::from([(key, vec![fill_report])]),
            successful_keys: IndexSet::from([key]),
        });

        let fresh_result = position_report_result(&node, venue_report);
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result: fresh_result,
            reports: IndexMap::from([(key, Vec::new())]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(
            positions
                .iter()
                .map(|position| position.quantity)
                .sum::<Quantity>(),
            Quantity::from("2.0")
        );
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            2
        );
    }

    #[rstest]
    #[case::below_limit(63)]
    #[case::at_limit(64)]
    #[case::above_limit(65)]
    fn test_position_fill_dispatch_limit_resumes_without_duplicate_economics(#[case] count: usize) {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("PositionFillLimitNode", Quantity::from("0.1"));
        let expected_quantity = dec!(1) + Decimal::new(i64::try_from(count).unwrap(), 1);

        let venue_report = PositionStatusReport::new(
            venue_report.account_id,
            venue_report.instrument_id,
            PositionSide::Long,
            Quantity::from_decimal_dp(expected_quantity, 1).unwrap(),
            venue_report.ts_last,
            venue_report.ts_init,
            None,
            None,
            venue_report.avg_px_open,
        );
        let key = (venue_report.instrument_id, venue_report.account_id);
        let client_order_id = fill_report.client_order_id.unwrap();

        let reports = (0..count)
            .map(|index| {
                let mut report = fill_report.clone();
                report.trade_id = TradeId::from(format!("T-LIMIT-{index:03}"));
                report.ts_event = UnixNanos::from(1_000 + index as u64);
                report
            })
            .collect::<Vec<_>>();

        let position_result = position_report_result(&node, venue_report.clone());

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, reports.clone())]),
            successful_keys: IndexSet::from([key]),
        });

        {
            let cache = node.kernel.cache.borrow();
            let order = cache.order(&client_order_id).unwrap();
            let applied = count.min(64);
            assert_eq!(
                order.filled_qty().as_decimal(),
                dec!(1) + Decimal::new(i64::try_from(applied).unwrap(), 1)
            );
            assert_eq!(order.trade_ids().len(), applied + 1);

            for (index, report) in reports.iter().enumerate() {
                assert_eq!(
                    order.trade_ids().contains(&&report.trade_id),
                    index < applied
                );
            }

            assert_eq!(
                cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
                1
            );
        }

        // A fresh cycle replays the full response; only the deferred fill may change exposure
        let position_result = position_report_result(&node, venue_report);
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, reports)]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let order = cache.order(&client_order_id).unwrap();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(order.filled_qty().as_decimal(), expected_quantity);
        assert_eq!(order.trade_ids().len(), count + 1);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity.as_decimal(), expected_quantity);
        assert_eq!(positions[0].side, PositionSide::Long);
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
    }

    #[rstest]
    #[case::client_order(0)]
    #[case::venue_order(1)]
    #[case::side(2)]
    #[case::position(3)]
    fn test_position_fill_conflict_blocks_remaining_fills_and_fallback(#[case] conflict: u8) {
        let (mut node, venue_report, valid_report) =
            position_fill_test_fixture("ConflictingPositionFillNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let client_order_id = valid_report.client_order_id.unwrap();
        let revision = node.exec_manager.position_activity_revision(&key);
        let position_result = position_report_result(&node, venue_report);
        let mut conflicting_report = valid_report.clone();
        conflicting_report.trade_id = TradeId::from("T-CONFLICTING-FILL");

        match conflict {
            0 => conflicting_report.client_order_id = Some(ClientOrderId::from("O-CONFLICT")),
            1 => conflicting_report.venue_order_id = VenueOrderId::from("V-CONFLICT"),
            2 => conflicting_report.order_side = OrderSide::Sell,
            3 => conflicting_report.venue_position_id = Some(PositionId::from("P-CONFLICT")),
            _ => unreachable!(),
        }

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![conflicting_report, valid_report])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let order = cache.order(&client_order_id).unwrap();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(order.filled_qty(), Quantity::from("1.0"));
        assert_eq!(
            order.trade_ids(),
            vec![&TradeId::from("T-POSITION-INITIAL")]
        );
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Quantity::from("1.0"));
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
        assert_eq!(node.exec_manager.position_activity_revision(&key), revision);
    }

    #[rstest]
    fn test_position_fill_report_failure_does_not_trigger_synthetic_fallback() {
        let (mut node, venue_report, _) =
            position_fill_test_fixture("FailedPositionFillNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::new(),
            successful_keys: IndexSet::new(),
        });

        let cache = node.kernel.cache.borrow();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Quantity::from("1.0"));
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
    }

    #[rstest]
    fn test_position_fill_report_result_falls_back_for_unattributable_hedge_fill() {
        let (mut node, mut venue_report, mut fill_report) =
            position_fill_test_fixture("UnattributedHedgeFillNode", Quantity::from("1.0"));
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_oms_type(StrategyId::from("EXTERNAL"), OmsType::Hedging);
        let key = (venue_report.instrument_id, venue_report.account_id);

        let position_id = {
            let cache = node.kernel.cache.borrow();
            let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
            assert_eq!(positions.len(), 1);
            positions[0].id
        };

        venue_report.venue_position_id = Some(position_id);
        fill_report.client_order_id = None;
        fill_report.venue_order_id = VenueOrderId::from("V-POSITION-EXTERNAL");
        let position_result = position_report_result(&node, venue_report);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].id, position_id);
        assert_eq!(positions[0].quantity, Quantity::from("2.0"));
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            2
        );
        drop(positions);
        drop(cache);
        assert!(
            !node
                .exec_manager
                .position_contains_fill_report(&fill_report)
        );
    }

    #[rstest]
    fn test_bundled_fill_report_invalidates_prepared_position_reconciliation() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("BundledPositionActivityNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let client_order_id = fill_report.client_order_id.unwrap();
        let position_result = position_report_result(&node, venue_report);
        let revision = node.exec_manager.position_activity_revision(&key);

        let order_report = OrderStatusReport::new(
            key.1,
            key.0,
            Some(client_order_id),
            fill_report.venue_order_id,
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0"),
            Quantity::from("2.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            UnixNanos::from(2_000),
            None,
        );
        let event = ExecutionEvent::Report(ExecutionReport::OrderWithFills(
            Box::new(order_report),
            vec![fill_report.clone()],
        ));

        assert_eq!(
            node.observe_exec_event_before_dispatch(&event),
            Some(vec![client_order_id])
        );

        assert_eq!(
            node.exec_manager.position_activity_revision(&key),
            revision + 1
        );
        assert!(
            !node
                .exec_manager
                .position_report_check_is_current(&position_result.check, &key)
        );

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let order = cache.order(&client_order_id).unwrap();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(order.filled_qty(), Quantity::from("1.0"));
        assert!(!order.trade_ids().contains(&&fill_report.trade_id));
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Quantity::from("1.0"));
        assert_eq!(positions[0].side, PositionSide::Long);
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
    }

    #[rstest]
    fn test_position_fill_report_result_defers_after_local_position_activity() {
        let (mut node, venue_report, fill_report) =
            position_fill_test_fixture("StalePositionFillNode", Quantity::from("1.0"));
        let key = (venue_report.instrument_id, venue_report.account_id);
        let position_result = position_report_result(&node, venue_report);
        node.exec_manager.record_position_activity(key.0, key.1);

        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let cache = node.kernel.cache.borrow();
        let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].quantity, Quantity::from("1.0"));
        assert_eq!(
            cache.orders_total_count(None, Some(&key.0), None, Some(&key.1), None),
            1
        );
        drop(positions);
        drop(cache);
        assert!(
            !node
                .exec_manager
                .position_contains_fill_report(&fill_report)
        );
    }

    #[rstest]
    fn test_observe_exec_event_before_dispatch_accepted_batch_stamps_local_activity() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                open_check_threshold_ms: 5_000,
                single_order_query_delay_ms: 0,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("AcceptedBatchNode".to_string(), Some(config)).unwrap();
        let account_id = AccountId::from("TEST-ACCEPTED-BATCH-001");
        let client_id = ClientId::from("TEST-ACCEPTED-BATCH");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let client_order_id = ClientOrderId::from("O-ACCEPTED-BATCH");
        let venue_order_id = VenueOrderId::from("V-ACCEPTED-BATCH");

        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument_id,
            client_order_id,
            venue_order_id,
        );

        assert_eq!(node.exec_manager.check_open_order_queries().len(), 1);

        let accepted = OrderAcceptedSpec::builder()
            .instrument_id(instrument_id)
            .client_order_id(client_order_id)
            .venue_order_id(venue_order_id)
            .account_id(account_id)
            .build();
        let event = ExecutionEvent::OrderAcceptedBatch(OrderAcceptedBatch::new(vec![accepted]));

        let close_ids = node.observe_exec_event_before_dispatch(&event);

        assert_eq!(close_ids, Some(vec![client_order_id]));
        assert!(node.exec_manager.check_open_order_queries().is_empty());
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_batch_cancel_command_registers_each_child_for_inflight_timeout() {
        use nautilus_common::messages::execution::{BatchCancelOrders, CancelOrder};
        use nautilus_model::{events::OrderPendingCancel, identifiers::ClientOrderId};

        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 1,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("BatchCancelNode".to_string(), Some(config)).unwrap();
        let trader_id = TraderId::from("TESTER-001");
        let strategy_id = StrategyId::from("S-BATCH-CANCEL");
        let account_id = AccountId::from("TEST-001");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let child_ids = [
            ClientOrderId::from("O-BATCH-CANCEL-1"),
            ClientOrderId::from("O-BATCH-CANCEL-2"),
        ];
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();

        for client_order_id in child_ids {
            let order = OrderTestBuilder::new(OrderType::Limit)
                .trader_id(trader_id)
                .strategy_id(strategy_id)
                .client_order_id(client_order_id)
                .instrument_id(instrument_id)
                .quantity(Quantity::from("10.0"))
                .price(Price::from("100.0"))
                .build();
            let venue_order_id = VenueOrderId::from(format!("V-{client_order_id}").as_str());
            // Model the production batch-cancel path: each child is accepted, then
            // moved to PendingCancel, so an inflight timeout must emit a Canceled
            // event (not a Submitted-order rejection).
            let submitted = TestOrderEventStubs::submitted(&order, account_id);
            let accepted = TestOrderEventStubs::accepted(&order, account_id, venue_order_id);
            let pending_cancel = OrderEventAny::PendingCancel(OrderPendingCancel::new(
                trader_id,
                strategy_id,
                instrument_id,
                client_order_id,
                Some(account_id),
                nautilus_common::recovery_trace::native_event_uuid(),
                UnixNanos::default(),
                UnixNanos::default(),
                false,
                Some(venue_order_id),
            ));
            let mut cache = node.kernel.cache.borrow_mut();
            cache.add_order(order, None, None, false).unwrap();
            cache.update_order(&submitted).unwrap();
            cache.update_order(&accepted).unwrap();
            cache.update_order(&pending_cancel).unwrap();
        }

        let cancels = child_ids
            .into_iter()
            .map(|client_order_id| {
                CancelOrder::new(
                    trader_id,
                    None,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    None,
                    nautilus_common::recovery_trace::native_event_uuid(),
                    UnixNanos::default(),
                    None,
                    None,
                )
            })
            .collect();

        let command = TradingCommand::CancelOrders(BatchCancelOrders::new(
            trader_id,
            None,
            strategy_id,
            instrument_id,
            cancels,
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            None,
            None,
        ));

        assert!(node.observe_exec_command_before_dispatch(&command).unwrap());
        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();
        let timed_out_ids = result
            .events
            .iter()
            .map(OrderEventAny::client_order_id)
            .collect::<IndexSet<_>>();

        assert_eq!(timed_out_ids, IndexSet::from(child_ids));
        assert_eq!(result.events.len(), child_ids.len());
        assert!(
            result
                .events
                .iter()
                .all(|event| matches!(event, OrderEventAny::Canceled(_))),
            "batch-cancel children must time out as Canceled events",
        );
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_risk_bound_command_does_not_register_inflight() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("RiskBoundNode".to_string(), Some(config)).unwrap();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::risk_engine_execute(),
            TypedIntoHandler::from(|_: TradingCommand| {}),
        );
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(node.trader_id())
            .strategy_id(StrategyId::from("S-RISK-DENIED"))
            .instrument_id(instrument_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.000"))
            .price(Price::from("100.00"))
            .build();
        let client_order_id = order.client_order_id();

        {
            let mut cache = node.kernel.cache.borrow_mut();
            cache
                .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
                .unwrap();
            cache.add_order(order.clone(), None, None, false).unwrap();
        }

        let submit_order = SubmitOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            instrument_id,
            client_order_id,
            order.init_event().clone(),
            None,
            None,
            None,
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            None,
        );
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::risk_engine_execute(),
            TradingCommand::SubmitOrder(submit_order),
        ));

        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();
        let status = node
            .kernel
            .cache
            .borrow()
            .order(&client_order_id)
            .unwrap()
            .status();

        assert_eq!(status, OrderStatus::Initialized);
        assert_eq!(
            node.exec_manager.recon_check_retry_count(&client_order_id),
            0
        );
        assert!(result.events.is_empty());
        assert!(result.queries.is_empty());
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_risk_approved_command_registers_inflight() {
        let config = LiveNodeConfig {
            risk_engine: crate::config::LiveRiskEngineConfig {
                bypass: true,
                ..Default::default()
            },
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("RiskApprovedNode".to_string(), Some(config)).unwrap();
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            TypedIntoHandler::from(|_: TradingCommand| {}),
        );
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(node.trader_id())
            .strategy_id(StrategyId::from("S-RISK-APPROVED"))
            .instrument_id(instrument_id)
            .side(OrderSide::Buy)
            .quantity(Quantity::from("1.000"))
            .price(Price::from("100.00"))
            .build();
        let client_order_id = order.client_order_id();

        {
            let mut cache = node.kernel.cache.borrow_mut();
            cache
                .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
                .unwrap();
            cache.add_order(order.clone(), None, None, false).unwrap();
        }

        let submit_order = SubmitOrder::new(
            order.trader_id(),
            None,
            order.strategy_id(),
            instrument_id,
            client_order_id,
            order.init_event().clone(),
            None,
            None,
            None,
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            None,
        );
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::risk_engine_execute(),
            TradingCommand::SubmitOrder(submit_order),
        ));

        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();

        let [TradingCommand::QueryOrder(query)] = result.queries.as_slice() else {
            panic!("expected one query order command");
        };

        assert_eq!(query.client_order_id, client_order_id);
        assert_eq!(
            node.exec_manager.recon_check_retry_count(&client_order_id),
            1
        );
        assert!(result.events.is_empty());
    }

    #[rstest]
    fn test_live_node_builder_clock_factory_drives_kernel_clock() {
        let calls = Rc::new(Cell::new(0usize));
        let calls_in_factory = calls.clone();
        let sentinel = UnixNanos::from(123_456_789_u64);

        let node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_clock_factory(move || {
                calls_in_factory.set(calls_in_factory.get() + 1);
                let mut clock = VirtualClock::new();
                clock.advance_time(sentinel, true);
                Rc::new(RefCell::new(clock)) as Rc<RefCell<dyn Clock>>
            })
            .build()
            .unwrap();

        assert_eq!(node.kernel().clock().borrow().timestamp_ns(), sentinel);
        assert_eq!(calls.get(), 1);
    }

    #[derive(Debug)]
    struct ReplayKernelEventStore {
        fail_restore: bool,
    }

    impl KernelEventStore for ReplayKernelEventStore {
        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            if self.fail_restore {
                anyhow::bail!("replay restore failed");
            }

            Ok(())
        }

        fn open(
            &mut self,
            _instance_id: UUID4,
            _components: &RegisteredComponents,
            _environment: Environment,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
            None
        }

        fn seal(&mut self, _ts_init: UnixNanos) {}

        fn run_id(&self) -> Option<&str> {
            Some("replay-child")
        }

        fn parent_run_id(&self) -> Option<&str> {
            Some("seed-run")
        }

        fn is_event_store_replay_configured(&self) -> bool {
            true
        }

        fn is_halted(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct HaltableKernelEventStore {
        halted: Rc<Cell<bool>>,
    }

    impl KernelEventStore for HaltableKernelEventStore {
        fn restore_parent_cache(
            &mut self,
            _instance_id: UUID4,
            _cache: &mut Cache,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn open(
            &mut self,
            _instance_id: UUID4,
            _components: &RegisteredComponents,
            _environment: Environment,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn snapshot_anchorer(&self) -> Option<SnapshotAnchorer> {
            None
        }

        fn seal(&mut self, _ts_init: UnixNanos) {}

        fn run_id(&self) -> Option<&str> {
            Some("haltable-run")
        }

        fn parent_run_id(&self) -> Option<&str> {
            None
        }

        fn is_halted(&self) -> bool {
            self.halted.get()
        }
    }

    #[derive(Debug)]
    struct TestStrategy {
        core: StrategyCore,
    }

    impl TestStrategy {
        fn new(config: StrategyConfig) -> Self {
            Self {
                core: StrategyCore::new(config),
            }
        }
    }

    impl DataActor for TestStrategy {}

    nautilus_strategy!(TestStrategy, {
        fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
            self.core.config.external_order_instrument_ids.clone()
        }
    });

    fn live_node_with_replay_store(fail_restore: bool) -> LiveNode {
        // load_state must be true: the kernel rejects event-store replay otherwise,
        // and LiveNodeConfig defaults it to false.
        let builder = LiveNodeBuilder::new(TraderId::default(), Environment::Live)
            .unwrap()
            .with_exec_engine_config(crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            })
            .with_load_state(true)
            .with_name("TestKernel")
            .with_event_store(move |_instance_id: UUID4, _clock: Rc<RefCell<dyn Clock>>| {
                Ok(Box::new(ReplayKernelEventStore { fail_restore }) as Box<dyn KernelEventStore>)
            });

        builder.build().unwrap()
    }

    fn live_node_with_haltable_store(halted: Rc<Cell<bool>>) -> LiveNode {
        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_disconnection: Duration::from_millis(100),
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };
        LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_event_store(move |_instance_id, _clock| {
                Ok(Box::new(HaltableKernelEventStore { halted }) as Box<dyn KernelEventStore>)
            })
            .build()
            .unwrap()
    }

    #[rstest]
    fn test_add_strategy_registers_external_order_claims_with_manager_and_engine() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_timeout_connection(1)
            .build()
            .unwrap();
        let instrument_id = InstrumentId::from("AUDUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");

        node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            external_order_instrument_ids: Some(vec![instrument_id]),
            ..Default::default()
        }))
        .unwrap();

        assert_eq!(
            node.exec_manager.get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );

        {
            let exec_engine = node.kernel().exec_engine.borrow();
            assert_eq!(
                exec_engine.get_external_order_claim(&instrument_id),
                Some(strategy_id)
            );
        }
    }

    #[rstest]
    fn test_register_external_order_claims_after_build_is_visible_to_manager_and_engine() {
        let node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let instrument_id = InstrumentId::from("AUDUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");

        node.register_external_order_claims(strategy_id, &[instrument_id])
            .unwrap();

        assert_eq!(
            node.exec_manager.get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );
        assert_eq!(
            node.kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_register_external_order_claims_while_running_is_visible_to_manager_and_engine() {
        let mut node = live_node_with_replay_store(false);
        let instrument_id = InstrumentId::from("AUDUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");

        node.start().await.unwrap();
        assert_eq!(node.state(), NodeState::Running);

        node.register_external_order_claims(strategy_id, &[instrument_id])
            .unwrap();

        assert_eq!(
            node.exec_manager.get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );
        assert_eq!(
            node.kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );
    }

    #[rstest]
    fn test_register_external_order_claims_conflicting_batch_leaves_new_claims_absent() {
        let node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let existing_instrument = InstrumentId::from("AUDUSD.SIM");
        let new_instruments = [
            InstrumentId::from("EURUSD.SIM"),
            InstrumentId::from("GBPUSD.SIM"),
        ];
        let existing_strategy_id = StrategyId::from("CLAIMS-001");
        let new_strategy_id = StrategyId::from("CLAIMS-002");
        node.register_external_order_claims(existing_strategy_id, &[existing_instrument])
            .unwrap();

        let result = node.register_external_order_claims(
            new_strategy_id,
            &[new_instruments[0], existing_instrument, new_instruments[1]],
        );

        assert!(result.is_err());
        assert_eq!(
            node.exec_manager
                .get_external_order_claim(&existing_instrument),
            Some(existing_strategy_id)
        );

        for instrument_id in new_instruments {
            assert_eq!(
                node.exec_manager.get_external_order_claim(&instrument_id),
                None
            );
            assert_eq!(
                node.kernel
                    .exec_engine
                    .borrow()
                    .get_external_order_claim(&instrument_id),
                None
            );
        }
    }

    #[rstest]
    fn test_deregister_external_order_claims_allows_successor_to_claim() {
        let node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let instruments = [
            InstrumentId::from("AUDUSD.SIM"),
            InstrumentId::from("EURUSD.SIM"),
        ];
        let first_strategy_id = StrategyId::from("CLAIMS-001");
        let successor_strategy_id = StrategyId::from("CLAIMS-002");
        node.register_external_order_claims(first_strategy_id, &instruments)
            .unwrap();

        node.deregister_external_order_claims(first_strategy_id)
            .unwrap();
        node.register_external_order_claims(successor_strategy_id, &instruments)
            .unwrap();

        for instrument_id in instruments {
            assert_eq!(
                node.exec_manager.get_external_order_claim(&instrument_id),
                Some(successor_strategy_id)
            );
            assert_eq!(
                node.kernel
                    .exec_engine
                    .borrow()
                    .get_external_order_claim(&instrument_id),
                Some(successor_strategy_id)
            );
        }
    }

    #[rstest]
    fn test_deregister_external_order_claims_without_claims_is_idempotent() {
        let node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let strategy_id = StrategyId::from("CLAIMS-001");

        node.deregister_external_order_claims(strategy_id).unwrap();
        node.deregister_external_order_claims(strategy_id).unwrap();
    }

    #[rstest]
    fn test_add_strategy_rejects_duplicate_external_order_claim_without_overwriting() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_timeout_connection(1)
            .build()
            .unwrap();
        let instrument_id = InstrumentId::from("AUDUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");
        let duplicate_strategy_id = StrategyId::from("OTHER-002");

        node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            external_order_instrument_ids: Some(vec![instrument_id]),
            ..Default::default()
        }))
        .unwrap();

        let result = node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(duplicate_strategy_id),
            external_order_instrument_ids: Some(vec![instrument_id]),
            ..Default::default()
        }));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already exists for CLAIMS-001")
        );
        assert_eq!(
            node.exec_manager.get_external_order_claim(&instrument_id),
            Some(strategy_id)
        );

        {
            let exec_engine = node.kernel().exec_engine.borrow();
            assert_eq!(
                exec_engine.get_external_order_claim(&instrument_id),
                Some(strategy_id)
            );
        }
    }

    #[rstest]
    fn test_add_strategy_rejects_repeated_external_order_claim_without_registering() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_timeout_connection(1)
            .build()
            .unwrap();
        let instrument_id = InstrumentId::from("AUDUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");

        let result = node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            external_order_instrument_ids: Some(vec![instrument_id, instrument_id]),
            ..Default::default()
        }));

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("appears more than once for CLAIMS-001")
        );
        assert_eq!(
            node.exec_manager.get_external_order_claim(&instrument_id),
            None
        );

        {
            let exec_engine = node.kernel().exec_engine.borrow();
            assert_eq!(exec_engine.get_external_order_claim(&instrument_id), None);
        }
    }

    #[rstest]
    fn test_add_strategy_failure_restores_external_order_claims() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_timeout_connection(1)
            .build()
            .unwrap();
        let existing_instrument_id = InstrumentId::from("AUDUSD.SIM");
        let configured_instrument_id = InstrumentId::from("EURUSD.SIM");
        let strategy_id = StrategyId::from("CLAIMS-001");
        node.register_external_order_claims(strategy_id, &[existing_instrument_id])
            .unwrap();

        let mut strategy = TestStrategy::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            external_order_instrument_ids: Some(vec![configured_instrument_id]),
            ..Default::default()
        });

        strategy
            .core
            .register(
                node.trader_id(),
                node.kernel.clock(),
                node.kernel.cache.clone(),
                node.kernel.portfolio.clone(),
            )
            .unwrap();

        let result = node.add_strategy(strategy);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("already registered with trader")
        );
        assert_eq!(
            node.exec_manager
                .get_external_order_claim(&existing_instrument_id),
            Some(strategy_id)
        );
        assert_eq!(
            node.kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&existing_instrument_id),
            Some(strategy_id)
        );
        assert_eq!(
            node.exec_manager
                .get_external_order_claim(&configured_instrument_id),
            None
        );
        assert_eq!(
            node.kernel
                .exec_engine
                .borrow()
                .get_external_order_claim(&configured_instrument_id),
            None
        );
    }

    #[rstest]
    fn test_add_strategy_without_claims_or_oms_type_does_not_require_engine_borrow() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let exec_engine = node.kernel.exec_engine.clone();
        let _engine_borrow = exec_engine.borrow_mut();

        node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("NOCLAIMS-001")),
            ..Default::default()
        }))
        .unwrap();
    }

    #[rstest]
    fn test_add_strategy_registers_configured_hedging_oms_type() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_timeout_connection(1)
            .build()
            .unwrap();
        let strategy_id = StrategyId::from("FUNDING_ARBITRAGE-001");

        node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(strategy_id),
            oms_type: Some(OmsType::Hedging),
            ..Default::default()
        }))
        .unwrap();

        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let client_id = ClientId::from("STUB");

        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                AccountId::from("TEST-ACCOUNT"),
                instrument_id.venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();

        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(node.trader_id())
            .strategy_id(strategy_id)
            .instrument_id(instrument_id)
            .quantity(Quantity::from("1.000"))
            .build();
        let position_id = PositionId::new("CUSTOM-POSITION-001");

        node.kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), Some(position_id), Some(client_id), true)
            .unwrap();

        let submit_order = SubmitOrder::new(
            order.trader_id(),
            Some(client_id),
            strategy_id,
            instrument_id,
            order.client_order_id(),
            order.init_event().clone(),
            order.exec_algorithm_id(),
            Some(position_id),
            None,
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            None,
        );

        node.kernel
            .exec_engine
            .borrow()
            .execute(TradingCommand::SubmitOrder(submit_order));

        let exec_engine = node.kernel.exec_engine.borrow();
        let cache = exec_engine.cache().borrow();
        let cached_order = cache
            .order(&order.client_order_id())
            .expect("Order should be cached");

        assert_eq!(cached_order.status(), OrderStatus::Initialized);
    }

    #[cfg(all(feature = "simulation", madsim))]
    async fn advance_clock(d: Duration) {
        madsim::time::advance(d);
        madsim::task::yield_now().await;
    }

    #[cfg(not(all(feature = "simulation", madsim)))]
    async fn advance_clock(d: Duration) {
        tokio::time::advance(d).await;
    }

    #[rstest]
    #[case::single_default(SubmissionRecoveryPolicy::ResolveLocally, false, None, false)]
    #[case::list_default(SubmissionRecoveryPolicy::ResolveLocally, true, None, false)]
    #[case::single_diagnostics(SubmissionRecoveryPolicy::RetainUnresolved, false, None, false)]
    #[case::list_diagnostics(SubmissionRecoveryPolicy::RetainUnresolved, true, None, false)]
    #[case::unapplied_cancel_default(
        SubmissionRecoveryPolicy::ResolveLocally,
        false,
        Some(OrderStatus::PendingCancel),
        false
    )]
    #[case::unapplied_modify_default(
        SubmissionRecoveryPolicy::ResolveLocally,
        false,
        Some(OrderStatus::PendingUpdate),
        false
    )]
    #[case::unapplied_cancel_diagnostics(
        SubmissionRecoveryPolicy::RetainUnresolved,
        false,
        Some(OrderStatus::PendingCancel),
        false
    )]
    #[case::unapplied_modify_diagnostics(
        SubmissionRecoveryPolicy::RetainUnresolved,
        false,
        Some(OrderStatus::PendingUpdate),
        false
    )]
    #[case::first_fill_cancel_default(
        SubmissionRecoveryPolicy::ResolveLocally,
        false,
        Some(OrderStatus::PendingCancel),
        true
    )]
    #[case::first_fill_modify_default(
        SubmissionRecoveryPolicy::ResolveLocally,
        false,
        Some(OrderStatus::PendingUpdate),
        true
    )]
    #[case::first_fill_cancel_diagnostics(
        SubmissionRecoveryPolicy::RetainUnresolved,
        false,
        Some(OrderStatus::PendingCancel),
        true
    )]
    #[case::first_fill_modify_diagnostics(
        SubmissionRecoveryPolicy::RetainUnresolved,
        false,
        Some(OrderStatus::PendingUpdate),
        true
    )]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_registry_publishes_after_native_resolution(
        #[case] policy: SubmissionRecoveryPolicy,
        #[case] order_list: bool,
        #[case] pending_status: Option<OrderStatus>,
        #[case] first_fill: bool,
    ) {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut node = LiveNode::build("SubmissionRegistryNode".to_string(), Some(config)).unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let client_id = ClientId::from("TEST-SUBMISSION");
        let account_id = AccountId::from("TEST-001");
        let strategy_id = StrategyId::from("SUBMISSION-001");
        let account = AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::from("0 USDT"),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        ));
        node.kernel.cache.borrow_mut().add_account(account).unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let orders: Vec<_> = (0..if order_list { 2 } else { 1 })
            .map(|index| {
                OrderTestBuilder::new(OrderType::Limit)
                    .trader_id(node.trader_id())
                    .strategy_id(strategy_id)
                    .client_order_id(ClientOrderId::from(
                        format!("O-SUBMISSION-{index}").as_str(),
                    ))
                    .instrument_id(instrument.id())
                    .quantity(Quantity::from("1.000"))
                    .price(Price::from("100.00"))
                    .build()
            })
            .collect();
        let command = if order_list {
            TradingCommand::SubmitOrderList(SubmitOrderList::new(
                node.trader_id(),
                Some(client_id),
                strategy_id,
                OrderList::new(
                    OrderListId::from("OL-SUBMISSION"),
                    instrument.id(),
                    strategy_id,
                    orders.iter().map(Order::client_order_id).collect(),
                    UnixNanos::default(),
                ),
                orders
                    .iter()
                    .map(|order| order.init_event().clone())
                    .collect(),
                None,
                None,
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
            ))
        } else {
            TradingCommand::SubmitOrder(SubmitOrder::from_order(
                &orders[0],
                node.trader_id(),
                Some(client_id),
                None,
                UUID4::new(),
                UnixNanos::default(),
            ))
        };
        assert!(node.observe_exec_command_before_dispatch(&command).unwrap());

        for order in &orders {
            let submitted = TestOrderEventStubs::submitted(order, account_id);
            node.kernel
                .cache
                .borrow_mut()
                .add_order(order.clone(), None, Some(client_id), false)
                .unwrap();
            node.process_exec_event(ExecutionEvent::Order(submitted));
        }

        let expected_status = match pending_status {
            Some(status) if !first_fill => status,
            Some(_) => OrderStatus::Canceled,
            None if policy == SubmissionRecoveryPolicy::RetainUnresolved => OrderStatus::Submitted,
            None => OrderStatus::Rejected,
        };
        let diagnostics = Rc::new(RefCell::new(Vec::new()));
        let handler = ShareableMessageHandler::from_typed({
            let diagnostics = diagnostics.clone();
            let cache = node.kernel.cache.clone();
            move |diagnostic: &SubmissionRecoveryExhausted| {
                let cache = cache.borrow_mut();
                let order = cache.order(&diagnostic.client_order_id).unwrap();
                assert_eq!(order.status(), expected_status);
                assert!(diagnostic.ts_event >= order.ts_last());
                diagnostics.borrow_mut().push(diagnostic.clone());
            }
        });
        let topic = MessagingSwitchboard::submission_recovery_exhausted_topic();
        msgbus::subscribe_any(topic.into(), handler.clone(), None);
        let queries = Rc::new(RefCell::new(Vec::new()));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            TypedIntoHandler::from({
                let queries = queries.clone();
                move |command: TradingCommand| queries.borrow_mut().push(command)
            }),
        );

        let mut last_inflight_check = dst::time::Instant::now();
        let mut last_open_check = last_inflight_check;
        let mut last_position_check = last_inflight_check;
        let mut open_order_report_task = None;
        let mut targeted_order_report_task = None;
        let mut position_report_task = None;

        for check in 1..=3 {
            advance_clock(Duration::from_millis(101)).await;
            node.run_reconciliation_checks(
                dst::time::Instant::now(),
                ReconciliationCheckIntervals {
                    inflight: Duration::from_millis(100),
                    open: Duration::ZERO,
                    position: Duration::ZERO,
                },
                &mut ReconciliationCheckState {
                    last_inflight_check: &mut last_inflight_check,
                    last_open_check: &mut last_open_check,
                    last_position_check: &mut last_position_check,
                    open_order_report_task: &mut open_order_report_task,
                    targeted_order_report_task: &mut targeted_order_report_task,
                    position_report_task: &mut position_report_task,
                },
            );

            if check == 1 {
                assert!(diagnostics.borrow().is_empty());

                if let Some(pending_status) = pending_status {
                    let order = &orders[0];
                    let pending = if pending_status == OrderStatus::PendingCancel {
                        OrderEventAny::PendingCancel(
                            OrderPendingCancelSpec::builder()
                                .trader_id(node.trader_id())
                                .strategy_id(strategy_id)
                                .instrument_id(instrument.id())
                                .client_order_id(order.client_order_id())
                                .account_id(account_id)
                                .build(),
                        )
                    } else {
                        OrderEventAny::PendingUpdate(
                            OrderPendingUpdateSpec::builder()
                                .trader_id(node.trader_id())
                                .strategy_id(strategy_id)
                                .instrument_id(instrument.id())
                                .client_order_id(order.client_order_id())
                                .account_id(account_id)
                                .build(),
                        )
                    };
                    node.process_exec_event(ExecutionEvent::Order(pending));
                    node.exec_manager.register_inflight(order.client_order_id());
                    if first_fill {
                        let cached = node
                            .kernel
                            .cache
                            .borrow()
                            .order(&order.client_order_id())
                            .unwrap()
                            .clone();
                        let OrderEventAny::Filled(mut fill) =
                            OrderFilledTestBuilder::new(&cached, &instrument)
                                .last_qty(Quantity::from("0.400"))
                                .last_px(Price::from("100.00"))
                                .commission(Money::from("0 USDT"))
                                .without_position_id()
                                .build()
                        else {
                            unreachable!()
                        };
                        fill.venue_order_id = VenueOrderId::from("V-SUBMISSION");
                        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(fill)));
                    } else {
                        let report = OrderStatusReport::new(
                            account_id,
                            instrument.id(),
                            Some(order.client_order_id()),
                            VenueOrderId::from("V-SUBMISSION"),
                            Some(order.order_side()),
                            order.order_type(),
                            order.time_in_force(),
                            OrderStatus::Submitted,
                            order.quantity(),
                            Quantity::zero(3),
                            UnixNanos::default(),
                            UnixNanos::default(),
                            UnixNanos::default(),
                            None,
                        )
                        .with_price(Price::from("100.00"));
                        node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(
                            Box::new(report),
                        )));
                    }
                    let cache = node.kernel.cache.borrow();
                    let cached = cache.order(&order.client_order_id()).unwrap();
                    assert_eq!(cached.status(), pending_status);
                    assert_eq!(
                        cached.filled_qty(),
                        if first_fill {
                            Quantity::from("0.400")
                        } else {
                            Quantity::zero(3)
                        }
                    );
                    assert!(
                        !cached
                            .events()
                            .iter()
                            .any(|event| matches!(event, OrderEventAny::Accepted(_)))
                    );
                }
            }
        }

        assert_eq!(
            queries.borrow().len(),
            orders.len()
                + usize::from(first_fill && policy == SubmissionRecoveryPolicy::ResolveLocally)
        );

        for (command, order) in queries.borrow().iter().zip(orders.iter().cycle()) {
            assert!(matches!(command, TradingCommand::QueryOrder(query)
                if query.client_order_id == order.client_order_id()
                && query.client_id == Some(client_id)));
            assert_eq!(
                node.kernel
                    .cache
                    .borrow()
                    .order(&order.client_order_id())
                    .unwrap()
                    .status(),
                if first_fill && policy == SubmissionRecoveryPolicy::RetainUnresolved {
                    pending_status.unwrap()
                } else {
                    expected_status
                }
            );
        }
        let diagnostics = diagnostics.borrow();
        assert_eq!(
            diagnostics.len(),
            if policy == SubmissionRecoveryPolicy::RetainUnresolved && pending_status.is_none() {
                orders.len()
            } else {
                0
            }
        );

        for (diagnostic, order) in diagnostics.iter().zip(&orders) {
            assert_eq!(diagnostic.trader_id, node.trader_id());
            assert_eq!(diagnostic.client_id, Some(client_id));
            assert_eq!(diagnostic.strategy_id, strategy_id);
            assert_eq!(diagnostic.instrument_id, instrument.id());
            assert_eq!(diagnostic.client_order_id, order.client_order_id());
            assert_eq!(diagnostic.source, SubmissionRecoverySource::Inflight);
            assert_eq!(diagnostic.retry_count, 2);
        }
        msgbus::unsubscribe_any(topic.into(), &handler);
        ExecutionEngine::register_msgbus_handlers(&node.kernel.exec_engine);
    }

    #[derive(Debug)]
    struct SubmissionCallbackStrategy {
        core: StrategyCore,
        cancel: bool,
        batch: bool,
        callback: &'static str,
        client_id: ClientId,
    }

    impl DataActor for SubmissionCallbackStrategy {}

    impl SubmissionCallbackStrategy {
        fn queue_command(&mut self, client_order_id: ClientOrderId) {
            let current_price = self.cache().order(&client_order_id).unwrap().price();
            let modify_price = if current_price == Some(Price::from("101.00")) {
                Price::from("102.00")
            } else {
                Price::from("101.00")
            };
            match (self.cancel, self.batch) {
                (true, true) => {
                    self.cancel_orders(vec![client_order_id], Some(self.client_id), None)
                        .unwrap();
                }
                (true, false) => {
                    self.cancel_order(client_order_id, Some(self.client_id), None)
                        .unwrap();
                }
                (false, true) => {
                    self.modify_orders(
                        vec![(client_order_id, None, Some(modify_price), None)],
                        Some(self.client_id),
                        None,
                    )
                    .unwrap();
                }
                (false, false) => self
                    .modify_order(
                        client_order_id,
                        None,
                        Some(modify_price),
                        None,
                        Some(self.client_id),
                        None,
                    )
                    .unwrap(),
            }
        }
    }

    nautilus_strategy!(SubmissionCallbackStrategy, {
        fn on_order_accepted(&mut self, event: OrderAccepted) {
            if self.callback == "accepted" {
                self.queue_command(event.client_order_id);
            }
        }

        fn on_order_updated(&mut self, event: OrderUpdated) {
            if self.callback == "updated" {
                self.queue_command(event.client_order_id);
            }
        }
    });

    #[rstest]
    #[case::native(false, "accepted")]
    #[case::report(true, "accepted")]
    #[case::updated(false, "updated")]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_registry_ack_callback_starts_command_budget_at_dispatch(
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
        #[values(false, true)] cancel: bool,
        #[values(false, true)] batch: bool,
        #[case] report: bool,
        #[case] callback: &'static str,
    ) {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            ..Default::default()
        };
        let mut node = LiveNode::build("SubmissionCallbackNode".to_string(), Some(config)).unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account_id = AccountId::from("TEST-001");
        let strategy_id = StrategyId::from("CALLBACK-001");
        let client_id = ClientId::from("TEST-CALLBACK");
        let account = AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::from("0 USDT"),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        ));
        node.kernel.cache.borrow_mut().add_account(account).unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        node.add_strategy(SubmissionCallbackStrategy {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(strategy_id),
                ..Default::default()
            }),
            cancel,
            batch,
            callback,
            client_id,
        })
        .unwrap();
        node.kernel.trader.borrow_mut().initialize().unwrap();
        node.kernel.start_trader().unwrap();
        let runner = node.runner.take().unwrap();
        runner.bind_senders_for_node(node.handle.clone());
        let mut channels = runner.take_channels();
        let order = OrderTestBuilder::new(OrderType::Limit)
            .trader_id(node.trader_id())
            .strategy_id(strategy_id)
            .client_order_id(ClientOrderId::from("O-CALLBACK"))
            .instrument_id(instrument.id())
            .quantity(Quantity::from("1.000"))
            .price(Price::from("100.00"))
            .build();
        let client_order_id = order.client_order_id();
        let submit = TradingCommand::SubmitOrder(SubmitOrder::from_order(
            &order,
            node.trader_id(),
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
        ));
        assert!(node.observe_exec_command_before_dispatch(&submit).unwrap());
        node.kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id), false)
            .unwrap();
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::submitted(
            &order, account_id,
        )));
        advance_clock(Duration::from_millis(101)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), 1);
        assert!(first.events.is_empty());

        advance_clock(Duration::from_millis(101)).await;
        let accepted =
            TestOrderEventStubs::accepted(&order, account_id, VenueOrderId::from("V-CALLBACK"));

        if callback == "updated" {
            let mut update = OrderUpdatedSpec::builder()
                .trader_id(node.trader_id())
                .strategy_id(strategy_id)
                .instrument_id(instrument.id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .quantity(order.quantity())
                .price(order.price().unwrap())
                .build();
            update.venue_order_id = Some(VenueOrderId::from("V-CALLBACK"));
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(update)));
        } else if report {
            let accepted = OrderStatusReport::new(
                account_id,
                instrument.id(),
                Some(client_order_id),
                VenueOrderId::from("V-CALLBACK"),
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Accepted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(Price::from("100.00"));
            node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                accepted,
            ))));
        } else {
            node.process_exec_event(ExecutionEvent::Order(accepted));
        }

        let pending_status = if cancel {
            OrderStatus::PendingCancel
        } else {
            OrderStatus::PendingUpdate
        };
        assert_eq!(channels.exec_cmd_rx.len(), 1);
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            pending_status
        );

        // Maintenance may win the runner selection before the queued command dispatches
        for _ in 0..3 {
            advance_clock(Duration::from_millis(101)).await;
            let maintenance = node.exec_manager.check_inflight_orders();
            assert!(maintenance.events.is_empty());
            assert!(maintenance.queries.is_empty());
            assert!(
                node.exec_manager
                    .take_submission_recovery_exhaustions()
                    .is_empty()
            );
        }

        let command = channels.exec_cmd_rx.try_recv().unwrap();
        let mutation_command_id = venue_evidence_command_id(command.message().command());
        node.process_exec_command(command);
        assert!(channels.exec_cmd_rx.is_empty());
        let immediate = node.exec_manager.check_inflight_orders();
        assert!(immediate.events.is_empty());
        assert!(immediate.queries.is_empty());
        advance_clock(Duration::from_millis(101)).await;
        let first_command_check = node.exec_manager.check_inflight_orders();
        assert!(first_command_check.events.is_empty());
        assert_eq!(first_command_check.queries.len(), 1);
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());
        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            assert!(exhausted.events.is_empty());
        } else {
            assert!(
                matches!(&exhausted.events[..], [OrderEventAny::Canceled(event)]
                if event.client_order_id == client_order_id)
            );
        }
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            assert_retained_mutation_requires_matching_venue_outcome(
                &mut node,
                &order,
                mutation_command_id,
                cancel,
                pending_status,
                if cancel {
                    OrderStatus::Canceled
                } else if callback == "updated" {
                    OrderStatus::PendingUpdate
                } else {
                    OrderStatus::Accepted
                },
                (account_id, VenueOrderId::from("V-CALLBACK")),
            );
            if callback == "updated" && !cancel {
                // Applying M1's real outcome queues M2, but does not dispatch it.
                assert_eq!(channels.exec_cmd_rx.len(), 1);
                let next = channels.exec_cmd_rx.try_recv().unwrap();
                assert_ne!(
                    venue_evidence_command_id(next.message().command()),
                    mutation_command_id
                );
                let next_price = match next.message().command() {
                    TradingCommand::ModifyOrder(command) => command.price,
                    TradingCommand::ModifyOrders(command) => command.modifies[0].price,
                    _ => panic!("updated callback must queue a real modify command"),
                };
                assert_eq!(next_price, Some(Price::from("102.00")));
                assert!(node.exec_manager.unresolved_submission_ids().is_empty());
            }
        } else {
            node.process_reconciliation_events(&exhausted.events);
            assert_eq!(
                node.kernel
                    .cache
                    .borrow()
                    .order(&client_order_id)
                    .unwrap()
                    .status(),
                OrderStatus::Canceled
            );
        }
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_reconciliation_check_due_uses_monotonic_elapsed_time() {
        let last = dst::time::Instant::now();
        let interval = Duration::from_millis(100);

        assert!(!reconciliation_check_due(last, last, Duration::ZERO));
        assert!(!reconciliation_check_due(last, last, interval));

        advance_clock(Duration::from_millis(99)).await;
        let before_interval = dst::time::Instant::now();
        assert!(!reconciliation_check_due(before_interval, last, interval));

        advance_clock(Duration::from_millis(1)).await;
        let at_interval = dst::time::Instant::now();
        assert!(reconciliation_check_due(at_interval, last, interval));

        assert!(!reconciliation_check_due(last, at_interval, interval));
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_run_reconciliation_checks_does_not_publish_open_order_queries() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                open_check_interval_secs: Some(1.0),
                position_check_interval_secs: Some(1.0),
                max_single_order_queries_per_cycle: 5,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node =
            LiveNode::build("ReconciliationFallbackNode".to_string(), Some(config)).unwrap();
        let client_id = ClientId::from("TEST-QUERY");
        let account_id = AccountId::from("TEST-QUERY-001");

        let trading_commands = Rc::new(RefCell::new(Vec::new()));
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_execute(),
            TypedIntoHandler::from({
                let trading_commands = trading_commands.clone();
                move |command: TradingCommand| {
                    trading_commands.borrow_mut().push(command);
                }
            }),
        );

        let venue_order_id = VenueOrderId::from("V-NODE-QUERY-001");
        let instrument = crypto_perpetual_ethusdt();
        let instrument_id = instrument.id();
        let client_order_id = ClientOrderId::from("O-NODE-QUERY-001");

        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(instrument))
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument_id,
            client_order_id,
            venue_order_id,
        );

        let last = dst::time::Instant::now();
        advance_clock(Duration::from_nanos(1)).await;
        let now = dst::time::Instant::now();
        let mut last_inflight_check = last;
        let mut last_open_check = last;
        let mut last_position_check = last;
        let mut open_order_report_task = None;
        let mut targeted_order_report_task = None;
        let mut position_report_task = None;

        node.run_reconciliation_checks(
            now,
            ReconciliationCheckIntervals {
                inflight: Duration::ZERO,
                open: Duration::from_nanos(1),
                position: Duration::ZERO,
            },
            &mut ReconciliationCheckState {
                last_inflight_check: &mut last_inflight_check,
                last_open_check: &mut last_open_check,
                last_position_check: &mut last_position_check,
                open_order_report_task: &mut open_order_report_task,
                targeted_order_report_task: &mut targeted_order_report_task,
                position_report_task: &mut position_report_task,
            },
        );

        let commands = trading_commands.borrow();

        assert!(commands.is_empty());
        assert!(open_order_report_task.is_none());
        assert!(targeted_order_report_task.is_none());
        assert!(position_report_task.is_none());

        ExecutionEngine::register_msgbus_handlers(&node.kernel.exec_engine);
    }

    #[rstest]
    #[tokio::test]
    async fn test_recurring_report_failure_preserves_client_coverage(
        #[values(false, true)] positions: bool,
        #[values(false, true)] failure_first: bool,
    ) {
        let mut node = LiveNode::build("ReportFailureNode".to_string(), None).unwrap();
        let failed_id = ClientId::from("FAILED");
        let healthy_id = ClientId::from("HEALTHY");

        for (client_id, outcome) in [
            (failed_id, FillReportClientOutcome::Failure),
            (healthy_id, FillReportClientOutcome::Reports(Vec::new())),
        ] {
            node.exec_clients
                .push(LiveExecutionClient::new(Box::new(FillReportClient {
                    client_id,
                    account_id: AccountId::from("TEST-001"),
                    venue: Venue::from("BINANCE"),
                    outcome,
                    commands: Rc::new(RefCell::new(Vec::new())),
                })));
        }

        if !failure_first {
            node.exec_clients.reverse();
        }

        let last = dst::time::Instant::now();
        let now = last + Duration::from_secs(1);
        let mut last_inflight = last;
        let mut last_open = last;
        let mut last_position = last;
        let mut open_task = None;
        let mut targeted_task = None;
        let mut position_task = None;

        node.run_reconciliation_checks(
            now,
            ReconciliationCheckIntervals {
                inflight: Duration::ZERO,
                open: if positions {
                    Duration::ZERO
                } else {
                    Duration::from_secs(1)
                },
                position: if positions {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                },
            },
            &mut ReconciliationCheckState {
                last_inflight_check: &mut last_inflight,
                last_open_check: &mut last_open,
                last_position_check: &mut last_position,
                open_order_report_task: &mut open_task,
                targeted_order_report_task: &mut targeted_task,
                position_report_task: &mut position_task,
            },
        );

        let (queried_clients, failed_clients) = if positions {
            let ReportTaskOutcome::Completed(PositionReportTaskResult::Positions(result)) =
                position_task.unwrap().future.await
            else {
                panic!("position collection should complete despite one client failure");
            };

            assert!(result.reports.is_empty());
            assert!(open_task.is_none());
            (result.queried_clients, result.failed_clients)
        } else {
            let ReportTaskOutcome::Completed(result) = open_task.unwrap().future.await else {
                panic!("order collection should complete despite one client failure");
            };

            assert!(result.reports.is_empty());
            assert!(position_task.is_none());
            (result.queried_clients, result.failed_clients)
        };

        assert_eq!(queried_clients, IndexSet::from([failed_id, healthy_id]));
        assert_eq!(failed_clients, IndexSet::from([failed_id]));
        assert!(targeted_task.is_none());
    }

    #[rstest]
    #[case::inflight(true, false)]
    #[case::open(false, false)]
    #[case::positions(false, true)]
    #[tokio::test]
    async fn test_reconciliation_shutdown_preserves_pending_checks(
        #[case] inflight: bool,
        #[case] positions: bool,
    ) {
        let mut node = LiveNode::build("ShutdownChecksNode".to_string(), None).unwrap();
        node.handle.set_shutting_down();
        let last = dst::time::Instant::now();
        let now = last + Duration::from_secs(1);
        let mut last_inflight = last;
        let mut last_open = last;
        let mut last_position = last;
        let mut open_task = None;
        let mut targeted_task = None;
        let mut position_task = None;

        node.run_reconciliation_checks(
            now,
            ReconciliationCheckIntervals {
                inflight: if inflight {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                },
                open: if !inflight && !positions {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                },
                position: if positions {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                },
            },
            &mut ReconciliationCheckState {
                last_inflight_check: &mut last_inflight,
                last_open_check: &mut last_open,
                last_position_check: &mut last_position,
                open_order_report_task: &mut open_task,
                targeted_order_report_task: &mut targeted_task,
                position_report_task: &mut position_task,
            },
        );

        assert_eq!(
            (last_inflight, last_open, last_position),
            (last, last, last)
        );
        assert!(open_task.is_none());
        assert!(targeted_task.is_none());
        assert!(position_task.is_none());
    }

    fn insert_accepted_limit_order_in_node(
        node: &LiveNode,
        account_id: AccountId,
        client_id: ClientId,
        instrument_id: InstrumentId,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .client_order_id(client_order_id)
            .instrument_id(instrument_id)
            .quantity(Quantity::from("10.0"))
            .price(Price::from("100.0"))
            .build();
        let submitted = TestOrderEventStubs::submitted(&order, account_id);
        node.kernel
            .cache
            .borrow_mut()
            .add_order(order, None, Some(client_id), false)
            .unwrap();
        let order = node
            .kernel
            .cache
            .borrow_mut()
            .update_order(&submitted)
            .unwrap();
        let accepted = TestOrderEventStubs::accepted(&order, account_id, venue_order_id);
        node.kernel
            .cache
            .borrow_mut()
            .update_order(&accepted)
            .unwrap();
    }

    fn recent_fill_test_fixture(name: &str) -> (LiveNode, OrderEventAny, InstrumentAny) {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let node = LiveNode::build(name.to_string(), Some(config)).unwrap();
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("TEST-RECENT-FILL");
        let client_order_id = ClientOrderId::from("O-RECENT-FILL");
        let venue_order_id = VenueOrderId::from("V-RECENT-FILL");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument.id(),
            client_order_id,
            venue_order_id,
        );
        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let fill = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("T-RECENT-FILL")),
            None,
            Some(Price::from("100.0")),
            Some(Quantity::from("1.0")),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(account_id),
        );

        (node, fill, instrument)
    }

    fn apply_inferred_position_fill(node: &mut LiveNode, fill_report: &FillReport) {
        let client_order_id = fill_report.client_order_id.unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let order_report = OrderStatusReport::new(
            fill_report.account_id,
            fill_report.instrument_id,
            Some(client_order_id),
            fill_report.venue_order_id,
            OrderSide::Buy.into(),
            OrderType::Limit,
            TimeInForce::Gtc,
            OrderStatus::PartiallyFilled,
            Quantity::from("10.0"),
            Quantity::from("2.0"),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            None,
        )
        .with_avg_px(dec!(100.0));
        let inferred = create_inferred_fill_for_qty(
            &order,
            &order_report,
            &fill_report.account_id,
            &instrument,
            Quantity::from("1.0"),
            UnixNanos::from(1_000),
            None,
        )
        .unwrap();

        node.process_reconciliation_events(&[inferred]);
    }

    #[rstest]
    #[case::rejected(false, "10.0", "10.0", "1.0")]
    #[case::dust(true, "9.004", "10.004", "10.004")]
    #[case::price_improvement(true, "12.5", "13.5", "13.5")]
    fn test_position_check_overfill_raises_before_fill_when_client_allows(
        #[case] allow: bool,
        #[case] fill_qty: &str,
        #[case] expected_qty: &str,
        #[case] expected_filled: &str,
    ) {
        let (mut node, venue_report, mut fill_report) =
            position_fill_test_fixture("PositionOverfillRaise", Quantity::from(fill_qty));
        fill_report.commission = Money::from("1.25 USDT");
        let key = (fill_report.instrument_id, fill_report.account_id);

        let client = StubExecutionClient::new(
            ClientId::from("POSITION-FILLS"),
            AccountId::from("TEST-001"),
            fill_report.instrument_id.venue,
            OmsType::Netting,
            None,
        );

        let client = if allow {
            client.with_allows_reconciliation_overfill()
        } else {
            client
        };

        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(LiveExecutionClient::new(Box::new(client))))
            .unwrap();

        let position_result = position_report_result(&node, venue_report);
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fill_report.clone()])]),
            successful_keys: IndexSet::from([key]),
        });

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from("O-POSITION-FILLS"))
            .unwrap();
        assert_eq!(order.quantity(), Quantity::from(expected_qty));
        assert_eq!(order.filled_qty(), Quantity::from(expected_filled));
        let applied = order
            .trade_ids()
            .iter()
            .any(|trade_id| trade_id.as_str() == "T-POSITION-AUTHORITATIVE");
        assert_eq!(applied, allow);
        let revision = node.exec_manager.position_activity_revision(&key);

        if !allow {
            return;
        }

        assert_eq!(revision, 2);

        {
            let cache = node.kernel.cache.borrow();
            let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
            assert_eq!(positions.len(), 1);
            assert_eq!(positions[0].quantity, Quantity::from(expected_qty));
            assert_eq!(positions[0].commissions(), vec![Money::from("1.25 USDT")]);
            assert_eq!(positions[0].trade_ids.len(), 2);
        }

        let replay_report = position_report_result(
            &node,
            PositionStatusReport::new(
                key.1,
                key.0,
                PositionSide::Long,
                Quantity::from(expected_qty),
                UnixNanos::from(2_000),
                UnixNanos::from(2_000),
                None,
                None,
                Some(dec!(100.0)),
            ),
        );
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result: replay_report,
            reports: IndexMap::from([(key, vec![fill_report])]),
            successful_keys: IndexSet::from([key]),
        });

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from("O-POSITION-FILLS"))
            .unwrap();
        assert_eq!(order.quantity(), Quantity::from(expected_qty));
        assert_eq!(order.filled_qty(), Quantity::from(expected_filled));
        assert_eq!(node.exec_manager.position_activity_revision(&key), revision);
    }

    #[rstest]
    fn test_position_check_applies_fitting_fill_then_overfill_in_one_cycle() {
        let (mut node, venue_report, mut overfill) =
            position_fill_test_fixture("PositionSplitOverfill", Quantity::from("2.0"));
        overfill.commission = Money::from("0.20 USDT");
        let key = (overfill.instrument_id, overfill.account_id);
        let client = StubExecutionClient::new(
            ClientId::from("POSITION-FILLS"),
            AccountId::from("TEST-001"),
            overfill.instrument_id.venue,
            OmsType::Netting,
            None,
        )
        .with_allows_reconciliation_overfill();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(LiveExecutionClient::new(Box::new(client))))
            .unwrap();

        let mut fitting = overfill.clone();
        fitting.trade_id = TradeId::from("T-POSITION-FITTING");
        fitting.last_qty = Quantity::from("8.0");
        fitting.commission = Money::from("0.80 USDT");
        let position_result = position_report_result(&node, venue_report);
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![fitting, overfill])]),
            successful_keys: IndexSet::from([key]),
        });

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from("O-POSITION-FILLS"))
            .unwrap();
        assert_eq!(order.quantity(), Quantity::from("11.0"));
        assert_eq!(order.filled_qty(), Quantity::from("11.0"));
        assert_eq!(order.trade_ids().len(), 3);
        assert_eq!(node.exec_manager.position_activity_revision(&key), 3);
        {
            let cache = node.kernel.cache.borrow();
            let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
            assert_eq!(positions.len(), 1);
            assert_eq!(positions[0].quantity, Quantity::from("11.0"));
            assert_eq!(positions[0].commissions(), vec![Money::from("1.00 USDT")]);
        }
    }

    #[rstest]
    fn test_position_check_second_overfill_does_not_raise_filled_order() {
        let (mut node, venue_report, mut first) =
            position_fill_test_fixture("PositionSecondOverfill", Quantity::from("12.0"));
        first.commission = Money::from("1.20 USDT");
        let key = (first.instrument_id, first.account_id);
        let client = StubExecutionClient::new(
            ClientId::from("POSITION-FILLS"),
            AccountId::from("TEST-001"),
            first.instrument_id.venue,
            OmsType::Netting,
            None,
        )
        .with_allows_reconciliation_overfill();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(LiveExecutionClient::new(Box::new(client))))
            .unwrap();

        let mut second = first.clone();
        second.trade_id = TradeId::from("T-POSITION-LATER");
        second.last_qty = Quantity::from("1.0");
        second.commission = Money::from("0.10 USDT");
        second.ts_event = UnixNanos::from(1_001);
        let position_result = position_report_result(&node, venue_report);
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, vec![first, second])]),
            successful_keys: IndexSet::from([key]),
        });

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&ClientOrderId::from("O-POSITION-FILLS"))
            .unwrap();
        assert_eq!(order.quantity(), Quantity::from("13.0"));
        assert_eq!(order.filled_qty(), Quantity::from("13.0"));
        assert_eq!(order.status(), OrderStatus::Filled);
        assert_eq!(order.trade_ids().len(), 2);
        assert!(
            order
                .trade_ids()
                .iter()
                .all(|trade_id| trade_id.as_str() != "T-POSITION-LATER")
        );
        {
            let cache = node.kernel.cache.borrow();
            let positions = cache.positions_open(None, Some(&key.0), None, Some(&key.1), None);
            assert_eq!(positions.len(), 1);
            assert_eq!(positions[0].quantity, Quantity::from("13.0"));
            assert_eq!(positions[0].commissions(), vec![Money::from("1.20 USDT")]);
        }
    }

    #[rstest]
    fn test_allow_overfills_applies_fill_without_raising_quantity() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                allow_overfills: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let node = LiveNode::build("AllowOverfillNoRaise".to_string(), Some(config)).unwrap();
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("POSITION-FILLS");
        let client_order_id = ClientOrderId::from("O-ALLOW");
        let venue_order_id = VenueOrderId::from("V-ALLOW");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument.id(),
            client_order_id,
            venue_order_id,
        );
        let client = StubExecutionClient::new(
            client_id,
            account_id,
            instrument.id().venue,
            OmsType::Netting,
            None,
        )
        .with_allows_reconciliation_overfill();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(LiveExecutionClient::new(Box::new(client))))
            .unwrap();

        let fill = FillReport::new(
            account_id,
            instrument.id(),
            venue_order_id,
            TradeId::from("T-ALLOW"),
            OrderSide::Buy,
            Quantity::from("12.0"),
            Price::from("100.0"),
            Money::from("1.00 USDT"),
            LiquiditySide::Taker,
            Some(client_order_id),
            None,
            UnixNanos::from(1_000),
            UnixNanos::from(1_000),
            None,
        );
        node.kernel
            .exec_engine
            .borrow_mut()
            .reconcile_fill_report(&fill);

        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        assert_eq!(order.quantity(), Quantity::from("10.0"));
        assert_eq!(order.filled_qty(), Quantity::from("12.0"));
        assert_eq!(order.trade_ids().len(), 1);
    }

    fn position_fill_test_fixture(
        name: &str,
        authoritative_qty: Quantity,
    ) -> (LiveNode, PositionStatusReport, FillReport) {
        position_fill_test_fixture_with_threshold(name, authoritative_qty, 0)
    }

    fn position_fill_test_fixture_with_threshold(
        name: &str,
        authoritative_qty: Quantity,
        position_check_threshold_ms: u32,
    ) -> (LiveNode, PositionStatusReport, FillReport) {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                position_check_threshold_ms,
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build(name.to_string(), Some(config)).unwrap();
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("POSITION-FILLS");
        let client_order_id = ClientOrderId::from("O-POSITION-FILLS");
        let venue_order_id = VenueOrderId::from("V-POSITION-FILLS");
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account = AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::from("0 USDT"),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                nautilus_common::recovery_trace::native_event_uuid(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        ));
        node.kernel.cache.borrow_mut().add_account(account).unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        insert_accepted_limit_order_in_node(
            &node,
            account_id,
            client_id,
            instrument.id(),
            client_order_id,
            venue_order_id,
        );
        let order = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let mut initial_fill = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("T-POSITION-INITIAL")),
            None,
            Some(Price::from("100.0")),
            Some(Quantity::from("1.0")),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(account_id),
        );

        let OrderEventAny::Filled(fill) = &mut initial_fill else {
            unreachable!();
        };

        fill.commission = Some(Money::zero(instrument.quote_currency()));
        node.process_reconciliation_events(&[initial_fill]);

        let ts_event = UnixNanos::from(1_000);

        let fill_report = FillReport::new(
            account_id,
            instrument.id(),
            venue_order_id,
            TradeId::from("T-POSITION-AUTHORITATIVE"),
            OrderSide::Buy,
            authoritative_qty,
            Price::from("100.0"),
            Money::zero(instrument.quote_currency()),
            LiquiditySide::Taker,
            Some(client_order_id),
            None,
            ts_event,
            ts_event,
            None,
        );

        let venue_report = PositionStatusReport::new(
            account_id,
            instrument.id(),
            PositionSide::Long,
            Quantity::from("2.0"),
            ts_event,
            ts_event,
            None,
            None,
            Some(dec!(100.0)),
        );

        (node, venue_report, fill_report)
    }

    fn position_report_result(
        node: &LiveNode,
        report: PositionStatusReport,
    ) -> PositionReportResult {
        let client_id = ClientId::from("POSITION-FILLS");
        let key = (report.instrument_id, report.account_id);
        let mut check = node.exec_manager.prepare_position_report_check(
            nautilus_common::recovery_trace::native_event_uuid(),
            &[],
        );
        check.client_coverage.insert(
            key,
            ReportClientCoverage::Resolved(IndexSet::from([client_id])),
        );

        PositionReportResult {
            check,
            reports: vec![report],
            queried_clients: IndexSet::from([client_id]),
            failed_clients: IndexSet::new(),
        }
    }

    fn position_quantity_after_check(
        node: &mut LiveNode,
        venue_report: &PositionStatusReport,
        reports: &[FillReport],
    ) -> Quantity {
        let key = (venue_report.instrument_id, venue_report.account_id);
        let position_result = position_report_result(node, venue_report.clone());
        node.handle_position_fill_report_result(PositionFillReportResult {
            position_result,
            reports: IndexMap::from([(key, reports.to_vec())]),
            successful_keys: IndexSet::from([key]),
        });

        node.kernel
            .cache
            .borrow()
            .positions_open(None, Some(&key.0), None, Some(&key.1), None)
            .iter()
            .map(|position| position.quantity)
            .sum()
    }

    fn fill_report_event(fill: &OrderFilled) -> ExecutionEvent {
        ExecutionEvent::Report(ExecutionReport::Fill(Box::new(FillReport::new(
            fill.account_id,
            fill.instrument_id,
            fill.venue_order_id,
            fill.trade_id,
            fill.order_side,
            fill.last_qty,
            fill.last_px,
            fill.commission
                .unwrap_or_else(|| Money::zero(fill.currency)),
            fill.liquidity_side,
            Some(fill.client_order_id),
            fill.position_id,
            fill.ts_event,
            fill.ts_init,
            None,
        ))))
    }

    fn is_recent_fill(node: &LiveNode, fill: &OrderFilled) -> bool {
        node.exec_manager.is_fill_recently_processed(
            fill.account_id,
            fill.instrument_id,
            fill.trade_id,
        )
    }

    #[rstest]
    #[case(0, NodeState::Idle)]
    #[case(1, NodeState::Starting)]
    #[case(2, NodeState::Running)]
    #[case(3, NodeState::ShuttingDown)]
    #[case(4, NodeState::Stopped)]
    #[case(5, NodeState::Observing)]
    fn test_node_state_from_u8_valid(#[case] value: u8, #[case] expected: NodeState) {
        assert_eq!(NodeState::from_u8(value), expected);
    }

    #[rstest]
    #[case(6)]
    #[case(255)]
    #[should_panic(expected = "Invalid NodeState value")]
    fn test_node_state_from_u8_invalid_panics(#[case] value: u8) {
        let _ = NodeState::from_u8(value);
    }

    #[rstest]
    fn test_node_state_roundtrip() {
        for state in [
            NodeState::Idle,
            NodeState::Starting,
            NodeState::Running,
            NodeState::ShuttingDown,
            NodeState::Stopped,
            NodeState::Observing,
        ] {
            assert_eq!(NodeState::from_u8(state.as_u8()), state);
        }
    }

    #[rstest]
    fn test_node_state_is_running_only_for_running() {
        assert!(!NodeState::Idle.is_running());
        assert!(!NodeState::Starting.is_running());
        assert!(NodeState::Running.is_running());
        assert!(!NodeState::ShuttingDown.is_running());
        assert!(!NodeState::Stopped.is_running());
        assert!(!NodeState::Observing.is_running());
    }

    #[rstest]
    #[tokio::test]
    async fn test_await_engines_connected_returns_stop_requested() {
        let node = LiveNode::build("TestNode".to_string(), None).unwrap();
        let handle = node.handle();

        handle.stop();

        let deadline = dst::time::Instant::now() + Duration::from_secs(1);
        let status = node.await_engines_connected(deadline).await;

        assert_eq!(status, EngineConnectionStatus::StopRequested);
        assert!(handle.should_stop());
    }

    #[rstest]
    #[tokio::test]
    async fn test_await_engines_connected_returns_shutdown_requested() {
        let node = LiveNode::build("TestNode".to_string(), None).unwrap();

        node.kernel().shutdown_flag().set(true);

        let deadline = dst::time::Instant::now() + Duration::from_secs(1);
        let status = node.await_engines_connected(deadline).await;

        assert_eq!(status, EngineConnectionStatus::ShutdownRequested);
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_stop_request_aborts_startup_without_running() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_disconnection: Duration::from_millis(50),
            ..Default::default()
        };

        let mut node = LiveNode::build("TestNode".to_string(), Some(config)).unwrap();
        let handle = node.handle();

        handle.stop();
        node.start().await.unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(handle.should_stop());
        assert!(!handle.is_running());
    }

    #[rstest]
    #[case(None, false)]
    #[case(Some(false), false)]
    #[case(Some(true), false)]
    #[case(Some(true), true)]
    #[tokio::test]
    async fn test_startup_reconciliation_requires_complete_mass_status(
        #[case] complete: Option<bool>,
        #[case] missing_instrument: bool,
    ) {
        struct MassStatusClient(Option<bool>, bool);
        #[async_trait::async_trait(?Send)]
        impl nautilus_common::clients::ExecutionClient for MassStatusClient {
            fn generate_account_state(
                &self,
                _balances: Vec<AccountBalance>,
                _margins: Vec<MarginBalance>,
                _reported: bool,
                _ts_event: UnixNanos,
                _params: Option<Params>,
            ) -> anyhow::Result<()> {
                anyhow::bail!("account mutation is outside this reconciliation fixture")
            }
            fn is_connected(&self) -> bool {
                true
            }
            fn client_id(&self) -> ClientId {
                ClientId::from("STUB")
            }
            fn account_id(&self) -> AccountId {
                AccountId::from("TEST-ACCOUNT")
            }
            fn venue(&self) -> Venue {
                Venue::from("TEST")
            }
            fn oms_type(&self) -> OmsType {
                OmsType::Netting
            }
            fn get_account(&self) -> Option<AccountAny> {
                None
            }
            fn start(&mut self) -> anyhow::Result<()> {
                Ok(())
            }
            fn stop(&mut self) -> anyhow::Result<()> {
                Ok(())
            }
            async fn generate_mass_status(
                &self,
                _lookback_mins: Option<u64>,
            ) -> anyhow::Result<Option<nautilus_model::reports::ExecutionMassStatus>> {
                Ok(self.0.map(|complete| {
                    let mut report = nautilus_model::reports::ExecutionMassStatus::new(
                        self.client_id(),
                        self.account_id(),
                        self.venue(),
                        UnixNanos::from(1),
                        None,
                    );
                    report.set_report_window(None, complete);
                    if self.1 {
                        report.add_order_reports(vec![
                            nautilus_model::reports::OrderStatusReport::new(
                                self.account_id(),
                                InstrumentId::from("UNKNOWN.TEST"),
                                None,
                                VenueOrderId::from("V-UNKNOWN"),
                                Some(OrderSide::Buy),
                                OrderType::Market,
                                TimeInForce::Gtc,
                                OrderStatus::Accepted,
                                Quantity::from("1"),
                                Quantity::from("0"),
                                UnixNanos::from(1),
                                UnixNanos::from(1),
                                UnixNanos::from(1),
                                None,
                            ),
                        ]);
                    }
                    report
                }))
            }
        }
        let mut node = LiveNode::build("MissingMassStatusNode".to_string(), None).unwrap();
        assert!(node.handle().startup_reconciliation().is_none());
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(MassStatusClient(complete, missing_instrument)))
            .unwrap();
        let result = node.perform_startup_reconciliation().await;
        if missing_instrument {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("Startup reconciliation unresolved for STUB"));
            assert!(error.contains("instrument_not_in_cache"));
        } else if complete == Some(true) {
            result.unwrap();
        } else {
            let error = result.unwrap_err().to_string();
            assert!(error.contains("Startup reconciliation requires") && error.contains("STUB"));
        }
        assert!(!node.handle().is_running());
        let observation = node.handle().startup_reconciliation().unwrap();
        assert_eq!(observation.clients, 1);
        assert!(observation.observed_at_ns > 0);
        assert_eq!(
            observation.phase,
            if complete == Some(true) && !missing_instrument {
                StartupReconciliationPhase::Processed
            } else {
                StartupReconciliationPhase::Failed
            }
        );
        assert_eq!(
            node.handle.clone().startup_reconciliation(),
            Some(observation)
        );
        node.handle.set_starting();
        assert!(node.handle().startup_reconciliation().is_none());
    }

    #[rstest]
    #[tokio::test(start_paused = true)]
    async fn test_stop_processes_residual_exec_event_during_grace_period() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::from_millis(20),
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("TestNode".to_string(), Some(config)).unwrap();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(InstrumentId::from("EUR/USD.SIM"))
            .quantity(Quantity::from("2"))
            .build();
        let client_order_id = order.client_order_id();
        let submitted = TestOrderEventStubs::submitted(&order, AccountId::from("POLL-STOP-001"));

        node.kernel
            .cache()
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();

        node.start().await.unwrap();
        let exec_event_sender = get_exec_event_sender();

        let send_residual = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1)).await;
            exec_event_sender
                .send(ExecutionEvent::Order(submitted))
                .unwrap();
        });

        node.stop().await.unwrap();
        send_residual.await.unwrap();

        assert_eq!(
            node.kernel
                .cache()
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );

        node.dispose();
    }

    #[rstest]
    #[tokio::test]
    async fn test_live_state_persistence_loads_before_start_and_saves_after_stop() {
        let actor_id = ActorId::from("LIVE-STATE-ACTOR");
        let strategy_id = StrategyId::from("LIVE-STATE-STRATEGY-001");
        let actor_load = IndexMap::from([("actor-load".to_string(), b"actor-loaded".to_vec())]);
        let strategy_load =
            IndexMap::from([("strategy-load".to_string(), b"strategy-loaded".to_vec())]);
        let actor_save = IndexMap::from([("actor-save".to_string(), b"actor-saved".to_vec())]);
        let strategy_save =
            IndexMap::from([("strategy-save".to_string(), b"strategy-saved".to_vec())]);
        let (database, control) = TestCacheDatabaseControl::create();
        control.set_actor_state(actor_id, &actor_load);
        control.set_strategy_state(strategy_id, &strategy_load);

        let config = LiveNodeConfig {
            load_state: true,
            save_state: true,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("StatePersistenceNode".to_string(), Some(config)).unwrap();
        node.set_cache_database(Box::new(database)).unwrap();
        node.add_actor(StateActor::new(
            actor_id,
            control.clone(),
            actor_save.clone(),
        ))
        .unwrap();
        node.add_strategy(StateStrategy::new(
            strategy_id,
            control.clone(),
            strategy_save.clone(),
        ))
        .unwrap();

        node.start().await.unwrap();
        node.stop().await.unwrap();
        node.dispose();

        assert_eq!(
            control.events(),
            vec![
                "actor.load:LIVE-STATE-ACTOR",
                "actor.on_load",
                "strategy.load:LIVE-STATE-STRATEGY-001",
                "strategy.on_load",
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "actor.update:LIVE-STATE-ACTOR",
                "strategy.on_save",
                "strategy.update:LIVE-STATE-STRATEGY-001",
                "database.close",
            ]
        );
        assert_eq!(control.actor_state(&actor_id), Some(actor_save));
        assert_eq!(control.strategy_state(&strategy_id), Some(strategy_save));
        assert_eq!(node.state(), NodeState::Stopped);
    }

    #[rstest]
    fn test_restore_component_state_loads_registered_strategy_while_idle() {
        let strategy_id = StrategyId::from("LIVE-RESTORE-STRATEGY-001");
        let control = TestCacheDatabaseControl::default();
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.add_strategy(StateStrategy::new(
            strategy_id,
            control.clone(),
            IndexMap::new(),
        ))
        .unwrap();

        let payload = IndexMap::from([("strategy-load".to_string(), b"restored".to_vec())]);
        let state = CollectedComponentState {
            actors: IndexMap::new(),
            strategies: IndexMap::from([(strategy_id, payload.clone())]),
        };
        node.restore_component_state(&state).unwrap();

        let registered = get_actor_unchecked::<StateStrategy>(&strategy_id.inner());
        assert_eq!(registered.state_load(), Some(&payload));
        drop(registered);
        assert!(control.events().contains(&"strategy.on_load".to_string()));
        node.dispose();
    }

    #[rstest]
    fn test_restore_native_cache_replaces_empty_idle_cache() {
        let mut source = Cache::default();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        source.add_instrument(instrument).unwrap();

        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.restore_native_cache(source).unwrap();
        assert!(
            node.kernel()
                .cache
                .borrow()
                .instrument(&instrument_id)
                .is_some()
        );
        node.dispose();
    }

    #[tokio::test]
    async fn test_recovered_cache_cannot_enter_either_startup_path() {
        let mut node = LiveNode::builder(TraderId::from("RECOVERY-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.restore_native_cache(Cache::default()).unwrap();
        assert!(
            node.start()
                .await
                .unwrap_err()
                .to_string()
                .contains("recovery release")
        );
        assert!(
            node.run()
                .await
                .unwrap_err()
                .to_string()
                .contains("recovery release")
        );
        assert_eq!(node.state(), NodeState::Idle);
        node.dispose();
    }

    #[tokio::test]
    async fn test_bound_runner_recovery_cannot_be_started_as_a_fresh_node() {
        let mut node = LiveNode::builder(TraderId::from("RECOVERY-002"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.recovery_runner_handoff()
            .unwrap()
            .bind_watermark("recovery", 1, 1)
            .unwrap();
        assert!(
            node.start()
                .await
                .unwrap_err()
                .to_string()
                .contains("recovery release")
        );
        assert!(
            node.run()
                .await
                .unwrap_err()
                .to_string()
                .contains("recovery release")
        );
        assert_eq!(node.state(), NodeState::Idle);
        node.dispose();
    }

    #[rstest]
    fn test_restore_native_cache_rejects_nonempty_target() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        node.kernel_mut()
            .cache
            .borrow_mut()
            .add_instrument(instrument)
            .unwrap();

        assert!(node.restore_native_cache(Cache::default()).is_err());
        node.dispose();
    }

    #[rstest]
    fn test_restore_native_cache_rejects_startup_state_loading() {
        let mut node = LiveNode::builder(TraderId::from("TESTER-001"), Environment::Sandbox)
            .unwrap()
            .with_load_state(true)
            .with_reconciliation(false)
            .build()
            .unwrap();

        assert!(node.restore_native_cache(Cache::default()).is_err());
        node.dispose();
    }

    #[rstest]
    #[tokio::test]
    async fn test_live_state_persistence_reports_callback_errors_after_shutdown() {
        let actor_id = ActorId::from("LIVE-FAIL-SAVE-ACTOR");
        let strategy_id = StrategyId::from("LIVE-FAIL-SAVE-STRATEGY-001");
        let (database, control) = TestCacheDatabaseControl::create();

        let config = LiveNodeConfig {
            save_state: true,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node =
            LiveNode::build("StatePersistenceErrorNode".to_string(), Some(config)).unwrap();
        node.set_cache_database(Box::new(database)).unwrap();
        node.add_actor(
            StateActor::new(actor_id, control.clone(), IndexMap::new()).with_fail_save(),
        )
        .unwrap();
        node.add_strategy(
            StateStrategy::new(strategy_id, control.clone(), IndexMap::new()).with_fail_save(),
        )
        .unwrap();

        node.start().await.unwrap();
        let error = node.stop().await.unwrap_err();
        node.dispose();

        assert_eq!(
            error.to_string(),
            "failed while finalizing kernel shutdown: Failed to save component state: actor \
             LIVE-FAIL-SAVE-ACTOR callback: test actor on_save failure; strategy \
             LIVE-FAIL-SAVE-STRATEGY-001 callback: test strategy on_save failure"
        );
        assert_eq!(
            control.events(),
            vec![
                "actor.on_start",
                "strategy.on_start",
                "actor.on_stop",
                "strategy.on_stop",
                "actor.on_save",
                "strategy.on_save",
                "database.close",
            ]
        );
        assert_eq!(node.state(), NodeState::Stopped);
    }

    #[rstest]
    #[tokio::test]
    async fn test_stop_drains_queued_exec_event_after_zero_grace() {
        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_disconnection: Duration::ZERO,
            delay_post_stop: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node = LiveNode::build("TestNode".to_string(), Some(config)).unwrap();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(InstrumentId::from("GBP/USD.SIM"))
            .quantity(Quantity::from("3"))
            .build();
        let client_order_id = order.client_order_id();
        let submitted = TestOrderEventStubs::submitted(&order, AccountId::from("POLL-DRAIN-001"));

        node.kernel
            .cache()
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();

        node.start().await.unwrap();
        get_exec_event_sender()
            .send(ExecutionEvent::Order(submitted))
            .unwrap();

        node.stop().await.unwrap();

        assert_eq!(
            node.kernel
                .cache()
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );

        node.dispose();
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_event_store_replay_skips_live_connections() {
        let mut node = live_node_with_replay_store(false);
        let handle = node.handle();

        node.start().await.unwrap();

        assert_eq!(handle.state(), NodeState::Running);
        assert!(handle.is_running());
        assert!(node.kernel.is_event_store_replay());
        assert!(node.runner.is_some());
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_event_store_replay_preserves_stop_request() {
        let mut node = live_node_with_replay_store(false);
        let handle = node.handle();
        handle.stop();

        node.start().await.unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(handle.should_stop());
        assert!(node.kernel.is_event_store_replay());
        assert!(node.runner.is_some());
    }

    #[rstest]
    #[tokio::test]
    async fn test_start_event_store_replay_config_failure_aborts_startup() {
        let mut node = live_node_with_replay_store(true);
        let handle = node.handle();

        node.start().await.unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(!handle.is_running());
        assert!(node.kernel.is_event_store_replay_configured());
        assert!(!node.kernel.is_event_store_replay());
        assert!(node.runner.is_some());
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_event_store_replay_consumes_runner_and_stops_before_connections() {
        let mut node = live_node_with_replay_store(false);
        let handle = node.handle();

        node.run().await.unwrap();

        assert_eq!(handle.state(), NodeState::Running);
        assert!(handle.is_running());
        assert!(node.kernel.is_event_store_replay());
        assert!(node.runner.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_event_store_replay_preserves_stop_request() {
        let mut node = live_node_with_replay_store(false);
        let handle = node.handle();
        handle.stop();

        node.run().await.unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(handle.should_stop());
        assert!(node.kernel.is_event_store_replay());
        assert!(node.runner.is_none());
    }

    #[rstest]
    #[tokio::test]
    async fn test_run_event_store_replay_config_failure_aborts_startup() {
        let mut node = live_node_with_replay_store(true);
        let handle = node.handle();

        node.run().await.unwrap();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(!handle.is_running());
        assert!(node.kernel.is_event_store_replay_configured());
        assert!(!node.kernel.is_event_store_replay());
        assert!(node.runner.is_none());
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_stops_and_reports_when_event_store_halts_while_idle() {
        let halted = Rc::new(Cell::new(false));
        let mut node = live_node_with_haltable_store(halted.clone());
        let handle = node.handle();

        tokio::time::timeout(Duration::from_secs(30), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(10)).await;
                halted.set(true);
                wait_until_async(
                    || async { handle.state() == NodeState::Stopped },
                    Duration::from_secs(10),
                )
                .await;
            };

            tokio::select! {
                biased;

                () = drive => {
                    let error = run.await.expect_err("halted event store should fail the run");
                    assert!(error
                        .to_string()
                        .contains("event-store persistence halted"));
                }
                result = &mut run => {
                    assert!(halted.get(), "node stopped before the event store was halted");
                    let error = result.expect_err("halted event store should fail the run");
                    assert!(error
                        .to_string()
                        .contains("event-store persistence halted"));
                }
            }
        })
        .await
        .expect("event-store halt should stop the idle node before timeout");

        assert_eq!(handle.state(), NodeState::Stopped);
        node.dispose();
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_event_store_containment_retains_running_loop_until_explicit_stop() {
        let halted = Rc::new(Cell::new(false));
        let mut node = live_node_with_haltable_store(halted.clone());
        assert!(node.enable_event_store_failure_containment().is_err());
        let engine = node.kernel.exec_engine.clone();
        let health = halted.clone();
        engine
            .borrow_mut()
            .set_submission_guard(Some(Rc::new(move |_| {
                anyhow::ensure!(!health.get(), "injected persistence failure");
                Ok(())
            })));
        node.enable_event_store_failure_containment().unwrap();
        let handle = node.handle();
        tokio::time::timeout(Duration::from_secs(15), async {
            let run = node.run();
            tokio::pin!(run);
            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(5)).await;
                halted.set(true);
                wait_until_async(|| async { engine.borrow().submissions_fenced() }, Duration::from_secs(5)).await;
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert!(handle.is_running(), "containment must retain the runner");
                handle.stop();
            };
            tokio::select! {
                biased;
                () = drive => {
                    assert!(run.await.unwrap_err().to_string().contains("event-store persistence halted"));
                }
                result = &mut run => panic!("contained node exited before explicit stop: {result:?}"),
            }
        }).await.unwrap();
        assert!(node.persistence_degraded());
        assert_eq!(handle.state(), NodeState::Stopped);
        node.dispose();
    }

    #[rstest]
    fn test_build_rejects_event_store_config_without_factory() {
        let config = LiveNodeConfig {
            event_store: Some(EventStoreConfig::default()),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let err = LiveNodeBuilder::from_config(config)
            .expect("builder")
            .build()
            .expect_err("should reject event_store config without factory");

        assert!(
            err.to_string().contains("with_event_store"),
            "error message should mention with_event_store, was: {err}"
        );
    }

    #[rstest]
    fn test_direct_build_rejects_event_store_config() {
        let config = LiveNodeConfig {
            event_store: Some(EventStoreConfig::default()),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            ..Default::default()
        };

        let err = LiveNode::build("TestNode".to_string(), Some(config))
            .expect_err("LiveNode::build should reject event_store config");

        assert!(
            err.to_string().contains("with_event_store"),
            "error message should mention with_event_store, was: {err}"
        );
    }

    #[rstest]
    fn test_dispose_before_start_is_idempotent() {
        let mut node = LiveNode::build("TestNode".to_string(), None).unwrap();
        node.add_strategy(TestStrategy::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("DISPOSAL-001")),
            ..Default::default()
        }))
        .unwrap();

        node.dispose();
        node.dispose();

        assert!(node.kernel.trader().borrow().is_disposed());
        assert_eq!(node.kernel.trader().borrow().component_count(), 0);
        assert_eq!(node.state(), NodeState::Stopped);
    }

    #[rstest]
    fn test_dispose_releases_retained_callback_roots(
        #[values(false, true)] fatal: bool,
        #[values(false, true)] external: bool,
    ) {
        actor::clear_callbacks().unwrap();
        let directory =
            std::env::temp_dir().join(format!("nautilus-callback-disposal-{}", UUID4::new()));

        let config = LiveNodeConfig {
            logging: LoggerConfig {
                fileout_level: LevelFilter::Info,
                file_config: Some(FileWriterConfig {
                    directory: Some(directory.to_str().unwrap().to_string()),
                    file_name: Some("disposal".to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            },
            ..Default::default()
        };

        let mut node = LiveNode::build("CallbackDisposalNode".to_string(), Some(config)).unwrap();
        let received = Rc::new(RefCell::new(Vec::new()));
        let observed = received.clone();

        let retained = DispatchMessage::from(()).dispatch(|()| {
            nautilus_common::runner::get_time_event_sender().send(TimeEventMessage::new(
                TimeEvent::new("disposal".into(), UUID4::new(), 17.into(), 23.into()),
                TimeEventCallback::RustLocal(Rc::new(move |_| {
                    observed.borrow_mut().push("delivered");
                })),
            ));

            external.then(|| DispatchMessage::new((), std::thread::current().id()))
        });

        if let Some(retained) = &retained {
            assert!(retained.is_rooted());
        }

        assert_eq!(actor::clear_callbacks(), Err(CallbackDispatchError::Active));

        if fatal {
            crate::dispatch::tests::latch_callback_failure();
        }

        node.dispose();

        assert!(node.runner.is_none());
        assert!(received.borrow().is_empty());
        assert_eq!(Rc::strong_count(&received), 1);
        assert_eq!(
            actor::callback_failure(),
            (fatal && external).then_some(CallbackDispatchError::DeliveryUnwound)
        );
        assert_eq!(
            actor::clear_callbacks(),
            if external {
                Err(CallbackDispatchError::Active)
            } else {
                Ok(())
            }
        );

        logging_sync_to_disk().unwrap();
        let output = std::fs::read_to_string(directory.join("disposal.log")).unwrap();
        drop(retained);
        node.dispose();

        assert_eq!(actor::callback_failure(), None);
        assert_eq!(actor::clear_callbacks(), Ok(()));
        drop(node);
        std::fs::remove_dir_all(directory).unwrap();

        let errors: Vec<_> = output
            .lines()
            .filter(|line| line.contains("[ERROR]"))
            .filter_map(|line| {
                line.split_once(".nautilus_live::node: ")
                    .map(|(_, text)| text)
            })
            .collect();

        let mut expected = Vec::new();

        if fatal {
            expected.push(
                "Callback dispatch failed before disposal cleanup: Callback delivery unwound",
            );
        }

        if external {
            expected.push(
                "Failed to clear callback dispatch during disposal: Callback work or access is still active",
            );
        }

        assert_eq!(errors, expected);
    }

    #[tokio::test]
    async fn test_dispose_releases_stop_generated_callback_roots() {
        actor::clear_callbacks().unwrap();

        let config = LiveNodeConfig {
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            timeout_connection: Duration::ZERO,
            timeout_reconciliation: Duration::ZERO,
            timeout_portfolio: Duration::ZERO,
            timeout_shutdown: Duration::ZERO,
            ..Default::default()
        };

        let mut node =
            LiveNode::build("StopCallbackDisposalNode".to_string(), Some(config)).unwrap();
        let received = Rc::new(RefCell::new(Vec::new()));
        node.add_actor(StopCallbackActor {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(ActorId::from("STOP-CALLBACK")),
                ..Default::default()
            }),
            received: received.clone(),
        })
        .unwrap();

        node.start().await.unwrap();

        assert!(node.kernel.trader().borrow().is_running());
        assert_eq!(actor::clear_callbacks(), Ok(()));

        node.dispose();

        assert_eq!(*received.borrow(), ["stop", "queued"]);
        assert_eq!(Rc::strong_count(&received), 1);
        assert!(node.runner.is_none());
        assert!(node.kernel.trader().borrow().is_disposed());
        assert_eq!(node.state(), NodeState::Stopped);
        assert_eq!(actor::clear_callbacks(), Ok(()));
    }

    #[derive(Debug)]
    struct StopCallbackActor {
        core: DataActorCore,
        received: Rc<RefCell<Vec<&'static str>>>,
    }

    nautilus_actor!(StopCallbackActor);

    impl DataActor for StopCallbackActor {
        fn on_stop(&mut self) -> anyhow::Result<()> {
            self.received.borrow_mut().push("stop");
            let received = self.received.clone();
            DispatchMessage::from(()).dispatch(|()| {
                nautilus_common::runner::get_time_event_sender().send(TimeEventMessage::new(
                    TimeEvent::new("stop-disposal".into(), UUID4::new(), 31.into(), 37.into()),
                    TimeEventCallback::RustLocal(Rc::new(move |_| {
                        received.borrow_mut().push("delivered");
                    })),
                ));
            });

            let clear_result = actor::clear_callbacks();
            anyhow::ensure!(
                clear_result == Err(CallbackDispatchError::Active),
                "Expected active callback roots during stop, received {clear_result:?}"
            );
            self.received.borrow_mut().push("queued");
            Ok(())
        }
    }

    #[rstest]
    fn test_handle_initial_state() {
        let handle = LiveNodeHandle::new();

        assert_eq!(handle.state(), NodeState::Idle);
        assert!(!handle.should_stop());
        assert!(!handle.is_running());
    }

    #[rstest]
    fn test_handle_initial_metrics_snapshot_is_zero() {
        let handle = LiveNodeHandle::new();

        assert_eq!(handle.metrics_snapshot(), RunnerMetricsSnapshot::default());
    }

    #[rstest]
    fn test_record_runner_dispatch_updates_selected_channel() {
        let metrics = RunnerMetrics::default();
        let dispatch_start = dst::time::Instant::now();
        let metrics_start = dispatch_start
            .checked_sub(Duration::from_micros(1))
            .expect("test instant should support a one-microsecond lookback");

        record_runner_dispatch(
            &metrics,
            SystemChannel::DataCommands,
            dispatch_start,
            metrics_start,
        );
        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.time_events.dispatched, 0);
        assert_eq!(snapshot.exec_events.dispatched, 0);
        assert_eq!(snapshot.exec_commands.dispatched, 0);
        assert_eq!(snapshot.data_events.dispatched, 0);
        assert_eq!(snapshot.data_commands.dispatched, 1);
        assert_eq!(
            snapshot.data_commands.last_dispatch_at_ns,
            snapshot.elapsed_ns
        );
        assert_eq!(
            snapshot.data_commands.dispatch_busy_ns,
            snapshot.dispatch_busy_ns
        );
        assert!(snapshot.dispatch_busy_ns < snapshot.elapsed_ns);
    }

    #[rstest]
    fn test_handle_stop_sets_flag() {
        let handle = LiveNodeHandle::new();

        handle.stop();

        assert!(handle.should_stop());
    }

    #[rstest]
    fn test_handle_stop_blocks_running_transition() {
        let handle = LiveNodeHandle::new();
        handle.set_starting();
        handle.stop();

        let transition = handle.try_set_running();

        assert_eq!(transition, RunningTransition::StopRequested);
        assert_eq!(handle.state(), NodeState::Starting);
        assert!(handle.should_stop());
        assert!(!handle.is_running());
    }

    #[rstest]
    fn test_handle_stop_after_running_transition_remains_pending() {
        let handle = LiveNodeHandle::new();
        handle.set_starting();

        let transition = handle.try_set_running();
        handle.stop();

        assert_eq!(transition, RunningTransition::Entered);
        assert_eq!(handle.state(), NodeState::Running);
        assert!(handle.should_stop());
        assert!(handle.is_running());
    }

    #[rstest]
    fn test_handle_node_state_transitions() {
        let handle = LiveNodeHandle::new();
        assert_eq!(handle.state(), NodeState::Idle);

        handle.set_starting();
        assert_eq!(handle.state(), NodeState::Starting);
        assert!(!handle.is_running());

        assert_eq!(handle.try_set_running(), RunningTransition::Entered);
        assert_eq!(handle.state(), NodeState::Running);
        assert!(handle.is_running());

        handle.set_shutting_down();
        assert_eq!(handle.state(), NodeState::ShuttingDown);
        assert!(!handle.is_running());

        handle.set_stopped();
        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(!handle.is_running());
    }

    #[rstest]
    fn test_handle_clone_shares_state_bidirectionally() {
        let handle1 = LiveNodeHandle::new();
        let handle2 = handle1.clone();

        handle1.set_starting();
        let transition = handle2.try_set_running();
        handle1.stop();

        assert_eq!(transition, RunningTransition::Entered);
        assert_eq!(handle1.state(), NodeState::Running);
        assert!(handle2.should_stop());
    }

    #[rstest]
    fn test_handle_stop_flag_survives_non_running_state_changes() {
        let handle = LiveNodeHandle::new();

        handle.set_starting();
        handle.stop();
        handle.set_shutting_down();
        handle.set_stopped();

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(handle.should_stop());
    }

    #[rstest]
    fn test_builder_creation() {
        let result = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox);

        assert!(result.is_ok());
    }

    #[rstest]
    fn test_stream_processor_receives_unregistered_typed_payload() {
        let mut node = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .build()
            .unwrap();
        let command = SubscribeCommand::Quotes(SubscribeQuotes::new(
            InstrumentId::from("AUD/USD.SIM"),
            Some(ClientId::from("EXTERNAL")),
            Some(Venue::from("SIM")),
            UUID4::from("00000000-0000-4000-8000-000000000001"),
            UnixNanos::from(1),
            None,
            None,
        ));
        let expected = serde_json::to_value(&command).unwrap();
        let received = Rc::new(RefCell::new(Vec::new()));
        let steps = Rc::new(RefCell::new(Vec::new()));
        let received_processor = received.clone();
        let first_steps = steps.clone();
        node.add_stream_processor(move |message| {
            let command = message
                .downcast_ref::<SubscribeCommand>()
                .expect("processor must receive the decoded concrete command");
            received_processor
                .borrow_mut()
                .push(serde_json::to_value(command).unwrap());
            first_steps.borrow_mut().push(1);
        });

        let second_steps = steps.clone();
        node.add_stream_processor(move |_| second_steps.borrow_mut().push(2));
        let republished = Rc::new(RefCell::new(Vec::new()));
        let republished_handler = republished.clone();
        let subscriber_steps = steps.clone();
        msgbus::subscribe_any(
            "external.test".into(),
            ShareableMessageHandler::from_typed(move |command: &SubscribeCommand| {
                republished_handler
                    .borrow_mut()
                    .push(serde_json::to_value(command).unwrap());
                subscriber_steps.borrow_mut().push(3);
            }),
            None,
        );

        let message = BusMessage::with_str_topic(
            "external.test",
            BusPayloadType::SubscribeCommand,
            Bytes::from(serde_json::to_vec(&command).unwrap()),
            SerializationEncoding::Json,
        );

        assert!(
            !msgbus::get_message_bus()
                .borrow()
                .is_streaming_type(BusPayloadType::SubscribeCommand)
        );
        node.process_external_msgbus_message(&message);

        assert_eq!(*received.borrow(), vec![expected.clone()]);
        assert_eq!(*steps.borrow(), vec![1, 2]);
        assert!(republished.borrow().is_empty());

        received.borrow_mut().clear();
        steps.borrow_mut().clear();
        msgbus::get_message_bus()
            .borrow_mut()
            .add_streaming_type(BusPayloadType::SubscribeCommand);
        node.process_external_msgbus_message(&message);

        assert_eq!(*received.borrow(), vec![expected.clone()]);
        assert_eq!(*republished.borrow(), vec![expected]);
        assert_eq!(*steps.borrow(), vec![1, 2, 3]);
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    fn test_builder_rejects_backtest() {
        let result = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Backtest);

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Backtest"));
    }

    #[rstest]
    fn test_builder_accepts_live_environment() {
        let result = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Live);

        assert!(result.is_ok());
    }

    #[rstest]
    fn test_builder_accepts_sandbox_environment() {
        let result = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox);

        assert!(result.is_ok());
    }

    #[rstest]
    fn test_builder_fluent_api_chaining() {
        let builder = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Live)
            .unwrap()
            .with_name("TestNode")
            .with_instance_id(nautilus_common::recovery_trace::native_event_uuid())
            .with_load_state(false)
            .with_save_state(true)
            .with_timeout_connection(30)
            .with_timeout_reconciliation(60)
            .with_reconciliation(true)
            .with_reconciliation_lookback_mins(120)
            .with_timeout_portfolio(10)
            .with_timeout_disconnection_secs(5)
            .with_delay_post_stop_secs(3)
            .with_delay_shutdown_secs(10);

        assert_eq!(builder.name(), "TestNode");
    }

    #[rstest]
    fn test_builder_with_external_msgbus_egress_uses_configured_encoding() {
        let (external_egress, publications, closed) = CapturingExternalEgress::new();

        let msgbus_config = MessageBusConfig {
            encoding: SerializationEncoding::Json,
            ..Default::default()
        };

        let node = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_msgbus_config(msgbus_config)
            .with_external_msgbus_egress(Box::new(external_egress))
            .build()
            .expect("node builds with external message bus egress");
        let quote = QuoteTick::default();

        msgbus::publish_quote("data.quotes.TEST".into(), &quote);

        let publications = publications.borrow();
        assert_eq!(publications.len(), 1);
        assert_eq!(publications[0].topic, "data.quotes.TEST");
        assert_eq!(
            serde_json::from_slice::<QuoteTick>(&publications[0].payload)
                .expect("JSON payload must decode as QuoteTick"),
            quote
        );
        drop(publications);

        msgbus::get_message_bus().borrow_mut().dispose();
        assert!(closed.get());
        drop(node);
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_builder_with_external_msgbus_factory_installs_egress_and_ingress() {
        let quote = QuoteTick::default();
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let publications = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let factory = CapturingBackingFactory::new(publications.clone(), closed.clone(), Some(rx));

        let msgbus_config = MessageBusConfig {
            external_streams: Some(vec!["stream".to_string()]),
            ..Default::default()
        };

        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            msgbus: Some(msgbus_config),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            timeout_connection: Duration::from_millis(500),
            timeout_disconnection: Duration::from_millis(500),
            ..Default::default()
        };

        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_external_msgbus_factory(Box::new(factory))
            .build()
            .expect("node builds with external message bus factory");

        msgbus::publish_quote("data.quotes.TEST".into(), &quote);
        {
            let publications = publications.lock();
            assert_eq!(publications.len(), 1);
            assert_eq!(publications[0].topic, "data.quotes.TEST");
            assert_eq!(
                serde_json::from_slice::<QuoteTick>(&publications[0].payload)
                    .expect("JSON payload must decode as QuoteTick"),
                quote
            );
        }

        let received = Rc::new(RefCell::new(Vec::<QuoteTick>::new()));
        let handle = node.handle();

        // Stopping from `drive` rather than here, so the node cannot finish `run` before the
        // republished quote is observed.
        let handler = TypedHandler::from({
            let received = received.clone();
            move |quote: &QuoteTick| {
                received.borrow_mut().push(*quote);
            }
        });

        msgbus::subscribe_quotes("data.quotes.*".into(), handler, None);
        msgbus::get_message_bus()
            .borrow_mut()
            .add_streaming_type(BusPayloadType::QuoteTick);

        let payload =
            Bytes::from(serde_json::to_vec(&quote).expect("QuoteTick should serialize as JSON"));
        let message = BusMessage::with_str_topic(
            "data.quotes.TEST",
            BusPayloadType::QuoteTick,
            payload,
            SerializationEncoding::Json,
        );

        tokio::time::timeout(Duration::from_secs(30), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(10)).await;

                tx.send(message)
                    .await
                    .expect("external ingress receiver should be open");

                wait_until_async(
                    || async { received.borrow().len() == 1 },
                    Duration::from_secs(10),
                )
                .await;
                assert_eq!(*received.borrow(), vec![quote]);
                handle.stop();
            };

            tokio::select! {
                biased;

                () = drive => {}
                result = &mut run => {
                    panic!("node stopped before factory ingress was republished: {result:?}");
                }
            }

            run.await.expect("node should stop cleanly");
        })
        .await
        .expect("live node should republish factory ingress and stop before timeout");

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(closed.load(Ordering::Relaxed));
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_builder_with_external_msgbus_factory_without_streams_runs_without_ingress() {
        let quote = QuoteTick::default();
        let publications = Arc::new(Mutex::new(Vec::new()));
        let closed = Arc::new(AtomicBool::new(false));
        let factory = CapturingBackingFactory::new(publications.clone(), closed.clone(), None);

        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            msgbus: Some(MessageBusConfig::default()),
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            timeout_connection: Duration::from_millis(500),
            timeout_disconnection: Duration::from_millis(500),
            ..Default::default()
        };

        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_external_msgbus_factory(Box::new(factory))
            .build()
            .expect("node builds with egress-only message bus factory");
        let handle = node.handle();

        msgbus::publish_quote("data.quotes.TEST".into(), &quote);
        {
            let publications = publications.lock();
            assert_eq!(publications.len(), 1);
            assert_eq!(publications[0].topic, "data.quotes.TEST");
        }

        tokio::time::timeout(Duration::from_secs(30), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(10)).await;
                handle.stop();
            };

            tokio::select! {
                biased;

                () = drive => {}
                result = &mut run => {
                    panic!("node stopped before egress-only factory run was observed: {result:?}");
                }
            }

            run.await.expect("node should stop cleanly");
        })
        .await
        .expect("live node should run without external ingress before timeout");

        assert_eq!(handle.state(), NodeState::Stopped);
        msgbus::get_message_bus().borrow_mut().dispose();
        assert!(closed.load(Ordering::Relaxed));
    }

    #[rstest]
    fn test_builder_with_external_msgbus_factory_rejects_injected_surfaces() {
        let (external_egress, _publications, _closed) = CapturingExternalEgress::new();

        let egress_factory = CapturingBackingFactory::new(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicBool::new(false)),
            None,
        );
        let egress_error = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_external_msgbus_factory(Box::new(egress_factory))
            .with_external_msgbus_egress(Box::new(external_egress))
            .build()
            .expect_err("builder should reject factory plus injected egress");

        assert!(
            egress_error
                .to_string()
                .contains("cannot be combined with injected egress or ingress")
        );

        let (_tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);

        let ingress_factory = CapturingBackingFactory::new(
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicBool::new(false)),
            None,
        );
        let ingress = CapturingExternalIngress::new(rx, Rc::new(Cell::new(false)));
        let ingress_error = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_external_msgbus_factory(Box::new(ingress_factory))
            .with_external_ingress(Box::new(ingress))
            .build()
            .expect_err("builder should reject factory plus injected ingress");

        assert!(
            ingress_error
                .to_string()
                .contains("cannot be combined with injected egress or ingress")
        );
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_republishes_external_ingress_on_local_msgbus() {
        let quote = QuoteTick::default();
        let received = Rc::new(RefCell::new(Vec::<QuoteTick>::new()));
        let payload =
            Bytes::from(serde_json::to_vec(&quote).expect("QuoteTick should serialize as JSON"));
        let message = BusMessage::with_str_topic(
            "data.quotes.TEST",
            BusPayloadType::QuoteTick,
            payload,
            SerializationEncoding::Json,
        );
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let closed = Rc::new(Cell::new(false));
        let ingress = CapturingExternalIngress::new(rx, closed.clone());

        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            timeout_connection: Duration::from_millis(500),
            timeout_disconnection: Duration::from_millis(500),
            ..Default::default()
        };

        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_external_ingress(Box::new(ingress))
            .build()
            .expect("node builds with external message bus ingress");
        let handle = node.handle();

        let handler = TypedHandler::from({
            let received = received.clone();
            move |quote: &QuoteTick| {
                received.borrow_mut().push(*quote);
            }
        });

        msgbus::subscribe_quotes("data.quotes.*".into(), handler, None);
        msgbus::get_message_bus()
            .borrow_mut()
            .add_streaming_type(BusPayloadType::QuoteTick);

        tokio::time::timeout(Duration::from_secs(30), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(10)).await;

                tx.send(message)
                    .await
                    .expect("external ingress receiver should be open");

                wait_until_async(
                    || async { received.borrow().len() == 1 },
                    Duration::from_secs(10),
                )
                .await;
                assert_eq!(*received.borrow(), vec![quote]);
                handle.stop();
            };

            tokio::select! {
                biased;

                () = drive => {}
                result = &mut run => {
                    panic!("node stopped before external message was republished: {result:?}");
                }
            }

            run.await.expect("node should stop cleanly");
        })
        .await
        .expect("live node should republish ingress and stop before timeout");

        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(closed.get());
        msgbus::get_message_bus().borrow_mut().dispose();
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_closes_external_ingress_when_receiver_closes() {
        let (tx, rx) = tokio::sync::mpsc::channel::<BusMessage>(1);
        let closed = Rc::new(Cell::new(false));
        let ingress = CapturingExternalIngress::new(rx, closed.clone());

        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            timeout_connection: Duration::from_millis(500),
            timeout_disconnection: Duration::from_millis(500),
            ..Default::default()
        };

        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_external_ingress(Box::new(ingress))
            .build()
            .expect("node builds with external message bus ingress");
        let handle = node.handle();

        tokio::time::timeout(Duration::from_secs(30), async {
            let run = node.run();
            tokio::pin!(run);

            let drive = async {
                wait_until_async(|| async { handle.is_running() }, Duration::from_secs(10)).await;

                drop(tx);

                wait_until_async(|| async { closed.get() }, Duration::from_secs(10)).await;
                assert!(
                    handle.is_running(),
                    "node should keep running after ingress closes"
                );
                handle.stop();
            };

            tokio::select! {
                biased;

                () = drive => {}
                result = &mut run => {
                    panic!("node stopped before ingress close was observed: {result:?}");
                }
            }

            run.await.expect("node should stop cleanly");
        })
        .await
        .expect("live node should close ingress and stop before timeout");

        assert_eq!(handle.state(), NodeState::Stopped);
    }

    #[rstest]
    #[tokio::test(flavor = "current_thread")]
    async fn test_run_aborts_startup_when_external_ingress_receiver_unavailable() {
        let closed = Rc::new(Cell::new(false));
        let ingress = FailingExternalIngress::new(closed.clone());

        let config = LiveNodeConfig {
            environment: Environment::Sandbox,
            exec_engine: crate::config::LiveExecutionEngineConfig {
                reconciliation: false,
                ..Default::default()
            },
            delay_post_stop: Duration::ZERO,
            timeout_connection: Duration::from_millis(500),
            timeout_disconnection: Duration::from_millis(500),
            ..Default::default()
        };

        let mut node = LiveNodeBuilder::from_config(config)
            .unwrap()
            .with_external_ingress(Box::new(ingress))
            .build()
            .expect("node builds with external message bus ingress");
        let handle = node.handle();

        let err = node.run().await.expect_err("run should fail");

        assert!(
            err.to_string()
                .contains("external ingress receiver unavailable")
        );
        assert_eq!(handle.state(), NodeState::Stopped);
        assert!(closed.get());
    }

    #[rstest]
    fn test_node_build_rejects_existing_node_preserving_account_delivery(
        #[values(false, true)] first_builder: bool,
        #[values(false, true)] second_builder: bool,
    ) {
        let config = LiveNodeConfig {
            trader_id: TraderId::from("PROBE-001"),
            ..Default::default()
        };

        let mut node = if first_builder {
            LiveNodeBuilder::from_config(config)
                .unwrap()
                .build()
                .unwrap()
        } else {
            LiveNode::build("Original".to_string(), Some(config)).unwrap()
        };

        let bus = msgbus::get_message_bus();

        let ExecutionEvent::Account(before) = stub_account_event() else {
            unreachable!()
        };

        get_exec_event_sender()
            .send(ExecutionEvent::Account(before.clone()))
            .unwrap();
        assert_eq!(node.drain_runner_pending(), 1);
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .account(&before.account_id)
                .unwrap()
                .last_event(),
            Some(before)
        );

        let result = if second_builder {
            LiveNodeBuilder::from_config(LiveNodeConfig::default())
                .unwrap()
                .build()
        } else {
            LiveNode::build("Extra".to_string(), None)
        };

        assert_eq!(
            result.unwrap_err().to_string(),
            "A LiveNode already exists or is being built on this thread; drop it before building another"
        );

        let ExecutionEvent::Account(after) = stub_account_event() else {
            unreachable!()
        };

        get_exec_event_sender()
            .send(ExecutionEvent::Account(after.clone()))
            .unwrap();
        assert_eq!(node.drain_runner_pending(), 1);
        assert_eq!(node.trader_id(), TraderId::from("PROBE-001"));
        assert!(Rc::ptr_eq(&bus, &msgbus::get_message_bus()));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .account(&after.account_id)
                .unwrap()
                .last_event(),
            Some(after)
        );
    }

    #[rstest]
    fn test_node_build_rejects_reentry_and_releases_thread_after_factory_failure() {
        let result = LiveNodeBuilder::new(TraderId::default(), Environment::Live)
            .unwrap()
            .with_event_store(|_instance_id: UUID4, _clock: Rc<RefCell<dyn Clock>>| {
                let error = LiveNode::build("Reentrant".to_string(), None).unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains("already exists or is being built")
                );
                anyhow::bail!("Event store construction failed")
            })
            .build();

        assert_eq!(
            result.unwrap_err().to_string(),
            "Event store construction failed"
        );

        let node = LiveNode::build("Retry".to_string(), None).unwrap();
        assert_eq!(node.state(), NodeState::Idle);
    }

    #[rstest]
    fn test_node_build_allows_separate_threads() {
        let node = LiveNode::build("First".to_string(), None).unwrap();

        let other_state = std::thread::spawn(|| {
            let other = LiveNode::build("OtherThread".to_string(), None).unwrap();
            other.state()
        })
        .join()
        .unwrap();

        assert_eq!(node.state(), NodeState::Idle);
        assert_eq!(other_state, NodeState::Idle);
    }

    #[rstest]
    fn test_node_build_releases_thread_after_drop() {
        let mut node = LiveNode::build("First".to_string(), None).unwrap();
        node.dispose();
        assert!(LiveNode::build("WhileRetained".to_string(), None).is_err());
        drop(node);

        let replacement = LiveNode::build("Replacement".to_string(), None).unwrap();
        assert_eq!(replacement.state(), NodeState::Idle);
    }

    #[rstest]
    fn test_node_build_releases_thread_after_failure(#[values(false, true)] builder: bool) {
        let config = LiveNodeConfig {
            event_store: Some(EventStoreConfig::default()),
            ..Default::default()
        };

        let result = if builder {
            LiveNodeBuilder::from_config(config).unwrap().build()
        } else {
            LiveNode::build("Failure".to_string(), Some(config))
        };

        assert!(result.unwrap_err().to_string().contains("factory"));

        let node = LiveNode::build("Retry".to_string(), None).unwrap();
        assert_eq!(node.state(), NodeState::Idle);
    }

    #[cfg(feature = "python")]
    #[rstest]
    fn test_node_build_and_initial_state() {
        let node = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_name("TestNode")
            .build()
            .unwrap();

        assert_eq!(node.state(), NodeState::Idle);
        assert!(!node.is_running());
        assert_eq!(node.environment(), Environment::Sandbox);
        assert_eq!(node.trader_id(), TraderId::from("TRADER-001"));
    }

    #[cfg(feature = "python")]
    #[rstest]
    fn test_node_build_replaces_stale_runner_senders() {
        replace_data_cmd_sender(Arc::new(SyncDataCommandSender));
        replace_exec_cmd_sender(Arc::new(SyncTradingCommandSender));

        let first = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_name("FirstNode")
            .build()
            .unwrap();

        assert_eq!(first.state(), NodeState::Idle);
        drop(first);

        let second = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_name("SecondNode")
            .build()
            .unwrap();

        assert_eq!(second.state(), NodeState::Idle);
        assert!(!second.is_running());
    }

    #[cfg(feature = "python")]
    #[rstest]
    fn test_node_handle_reflects_node_state() {
        let node = LiveNode::builder(TraderId::from("TRADER-001"), Environment::Sandbox)
            .unwrap()
            .with_name("TestNode")
            .build()
            .unwrap();

        let handle = node.handle();

        assert_eq!(handle.state(), NodeState::Idle);
        assert!(!handle.is_running());
    }

    #[rstest]
    fn test_pending_drain_data_returns_false_when_empty() {
        let mut pending = PendingEvents::default();

        assert!(!pending.drain_data());
    }

    #[rstest]
    fn test_pending_drain_data_returns_true_when_non_empty() {
        use nautilus_model::instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt};

        let mut pending = PendingEvents::default();
        pending.data_evts.push(
            DataEvent::Instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
                .into(),
        );

        assert!(pending.drain_data());
        assert!(pending.data_evts.is_empty());
    }

    fn stub_data_event() -> DataEvent {
        use nautilus_model::instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt};

        DataEvent::Instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
    }

    fn stub_data_command() -> DataCommand {
        use nautilus_common::messages::data::{SubscribeCommand, subscribe::SubscribeInstruments};
        use nautilus_core::UnixNanos;
        use nautilus_model::identifiers::Venue;

        DataCommand::Subscribe(SubscribeCommand::Instruments(SubscribeInstruments::new(
            None,
            Venue::from("TEST"),
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            None,
            None,
        )))
    }

    fn stub_system_command() -> SystemCommand {
        SystemCommand::ReconnectSocket(ReconnectSocket::new(
            TraderId::from("TRADER-001"),
            ClientId::from("POLYMARKET"),
            Ustr::from("polymarket-market-streams"),
            UnixNanos::default(),
        ))
    }

    #[rstest]
    fn test_flush_pending_data_drains_events_and_commands() {
        let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut evt_rx = SnapshotReceiver::from(evt_rx);
        let (cmd_tx, cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut cmd_rx = SnapshotReceiver::from(cmd_rx);

        let mut pending = PendingEvents::default();

        // Pre-load pending (items captured by the select loop)
        pending.data_evts.push((stub_data_event()).into());
        pending.data_cmds.push(stub_data_command().into());

        // Pre-load channels (items missed by the select loop)
        evt_tx.send((stub_data_event()).into()).unwrap();
        cmd_tx.send(stub_data_command().into()).unwrap();

        flush_pending_data(&mut pending, &mut evt_rx, &mut cmd_rx);

        assert!(pending.data_evts.is_empty());
        assert!(pending.data_cmds.is_empty());
        assert!(evt_rx.try_recv().is_err());
        assert!(cmd_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_flush_pending_data_drains_mixed_sources() {
        let (evt_tx, evt_rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut evt_rx = SnapshotReceiver::from(evt_rx);
        let (cmd_tx, cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut cmd_rx = SnapshotReceiver::from(cmd_rx);

        let mut pending = PendingEvents::default();

        // First pass: pending has an event, channel has a command
        pending.data_evts.push((stub_data_event()).into());
        cmd_tx.send(stub_data_command().into()).unwrap();

        // Second pass: channel has items that simulate arrival during first drain
        evt_tx.send((stub_data_event()).into()).unwrap();
        evt_tx.send((stub_data_event()).into()).unwrap();
        cmd_tx.send(stub_data_command().into()).unwrap();

        flush_pending_data(&mut pending, &mut evt_rx, &mut cmd_rx);

        assert!(pending.data_evts.is_empty());
        assert!(pending.data_cmds.is_empty());
        assert!(evt_rx.try_recv().is_err());
        assert!(cmd_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_pending_system_events_stay_separate_from_data() {
        let mut pending = PendingEvents::default();

        let change = SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            ustr::Ustr::from("binance-futures-market-streams"),
            SocketState::Connected,
        );

        pending
            .system_events
            .push(SystemEvent::SocketState(change).into());
        let system_events = pending
            .take_system_events()
            .into_iter()
            .map(|message| message.dispatch(|value| value))
            .collect::<Vec<_>>();

        assert_eq!(system_events, vec![SystemEvent::SocketState(change)]);
        assert!(pending.is_empty());
    }

    #[rstest]
    fn test_pending_system_commands_stay_separate_from_data() {
        let mut pending = PendingEvents::default();
        let command = stub_system_command();

        pending.system_commands.push(command.into());
        let system_commands = pending
            .take_system_commands()
            .into_iter()
            .map(|message| message.dispatch(|value| value))
            .collect::<Vec<_>>();

        assert_eq!(system_commands, vec![command]);
        assert!(pending.is_empty());
    }

    fn stub_time_event_handler() -> TimeEventMessage {
        use std::rc::Rc;

        use nautilus_common::{
            runner::TimeEventMessage,
            timer::{TimeEvent, TimeEventCallback},
        };
        use nautilus_core::UnixNanos;
        use ustr::Ustr;

        TimeEventMessage::new(
            TimeEvent::new(
                Ustr::from("test-timer"),
                nautilus_common::recovery_trace::native_event_uuid(),
                UnixNanos::default(),
                UnixNanos::default(),
            ),
            TimeEventCallback::RustLocal(Rc::new(|_| {})),
        )
    }

    fn stub_trading_command_message() -> TradingCommandMessage {
        use nautilus_common::messages::execution::query::QueryAccount;
        use nautilus_core::UnixNanos;
        use nautilus_model::identifiers::AccountId;

        TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            TradingCommand::QueryAccount(QueryAccount::new(
                TraderId::from("TESTER-001"),
                None,
                AccountId::from("TEST-001"),
                nautilus_common::recovery_trace::native_event_uuid(),
                UnixNanos::default(),
                None,
                None, // correlation_id
            )),
        )
    }

    fn stub_exec_event() -> ExecutionEvent {
        use nautilus_model::{
            enums::{LiquiditySide, OrderSide},
            identifiers::{AccountId, InstrumentId, TradeId, VenueOrderId},
            reports::FillReport,
            types::{Money, Price, Quantity},
        };

        ExecutionEvent::Report(ExecutionReport::Fill(Box::new(FillReport::new(
            AccountId::from("TEST-001"),
            InstrumentId::from("TEST.VENUE"),
            VenueOrderId::from("V-001"),
            TradeId::from("T-001"),
            OrderSide::Buy,
            Quantity::from("1.0"),
            Price::from("100.0"),
            Money::from("0.01 USD"),
            LiquiditySide::Maker,
            None,
            None,
            nautilus_core::UnixNanos::default(),
            nautilus_core::UnixNanos::default(),
            None,
        ))))
    }

    #[rstest]
    fn test_flush_all_pending_drains_buffered_channels() {
        let (time_tx, time_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let mut time_rx = SnapshotReceiver::from(time_rx);
        let (system_evt_tx, system_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemEvent>>();
        let mut system_evt_rx = SnapshotReceiver::from(system_evt_rx);
        let (system_cmd_tx, system_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemCommand>>();
        let mut system_cmd_rx = SnapshotReceiver::from(system_cmd_rx);
        let (data_evt_tx, data_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut data_evt_rx = SnapshotReceiver::from(data_evt_rx);
        let (data_cmd_tx, data_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut data_cmd_rx = SnapshotReceiver::from(data_cmd_rx);
        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);
        let (exec_cmd_tx, exec_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let mut exec_cmd_rx = SnapshotReceiver::from(exec_cmd_rx);

        let mut pending = PendingEvents::default();

        // Pre-load pending with data items
        pending.data_evts.push((stub_data_event()).into());
        pending.data_cmds.push(stub_data_command().into());

        // Pre-load all channel types
        time_tx.send((stub_time_event_handler()).into()).unwrap();

        let change = SocketStateChange::new(
            ClientId::from("BINANCE"),
            Some(Venue::from("BINANCE")),
            Ustr::from("binance-futures-market-streams"),
            SocketState::Connected,
        );
        system_evt_tx
            .send((SystemEvent::SocketState(change)).into())
            .unwrap();
        system_cmd_tx.send((stub_system_command()).into()).unwrap();
        data_evt_tx.send((stub_data_event()).into()).unwrap();
        data_cmd_tx.send(stub_data_command().into()).unwrap();
        exec_evt_tx.send((stub_exec_event()).into()).unwrap();
        exec_cmd_tx
            .send(stub_trading_command_message().into())
            .unwrap();

        flush_all_pending(
            &mut pending,
            &mut time_rx,
            &mut system_evt_rx,
            &mut system_cmd_rx,
            &mut exec_evt_rx,
            &mut exec_cmd_rx,
            &mut data_evt_rx,
            &mut data_cmd_rx,
        );

        let system_events = pending
            .take_system_events()
            .into_iter()
            .map(|message| message.dispatch(|value| value))
            .collect::<Vec<_>>();
        let system_commands = pending
            .take_system_commands()
            .into_iter()
            .map(|message| message.dispatch(|value| value))
            .collect::<Vec<_>>();
        assert_eq!(system_events, vec![SystemEvent::SocketState(change)]);
        assert_eq!(system_commands, vec![stub_system_command()]);
        assert!(pending.data_evts.is_empty());
        assert!(pending.data_cmds.is_empty());
        assert!(pending.exec_reports.is_empty());
        assert!(pending.exec_cmds.is_empty());
        assert!(pending.order_evts.is_empty());
        assert!(time_rx.try_recv().is_err());
        assert!(system_evt_rx.try_recv().is_err());
        assert!(system_cmd_rx.try_recv().is_err());
        assert!(data_evt_rx.try_recv().is_err());
        assert!(data_cmd_rx.try_recv().is_err());
        assert!(exec_evt_rx.try_recv().is_err());
        assert!(exec_cmd_rx.try_recv().is_err());
    }

    fn stub_order_event() -> ExecutionEvent {
        use nautilus_model::events::order::spec::OrderSubmittedSpec;

        ExecutionEvent::Order(OrderEventAny::Submitted(
            OrderSubmittedSpec::builder().build(),
        ))
    }

    fn stub_account_event() -> ExecutionEvent {
        use nautilus_core::UnixNanos;
        use nautilus_model::{
            enums::AccountType, events::account::state::AccountState, identifiers::AccountId,
        };

        ExecutionEvent::Account(AccountState::new(
            AccountId::from("TEST-001"),
            AccountType::Cash,
            vec![],
            vec![],
            true,
            nautilus_common::recovery_trace::native_event_uuid(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        ))
    }

    #[rstest]
    fn test_flush_all_pending_routes_order_event_to_order_evts() {
        let (_time_tx, time_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let mut time_rx = SnapshotReceiver::from(time_rx);
        let (_system_evt_tx, system_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemEvent>>();
        let mut system_evt_rx = SnapshotReceiver::from(system_evt_rx);
        let (_system_cmd_tx, system_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemCommand>>();
        let mut system_cmd_rx = SnapshotReceiver::from(system_cmd_rx);
        let (_data_evt_tx, data_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut data_evt_rx = SnapshotReceiver::from(data_evt_rx);
        let (_data_cmd_tx, data_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut data_cmd_rx = SnapshotReceiver::from(data_cmd_rx);
        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);
        let (_exec_cmd_tx, exec_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let mut exec_cmd_rx = SnapshotReceiver::from(exec_cmd_rx);

        let mut pending = PendingEvents::default();

        exec_evt_tx.send((stub_order_event()).into()).unwrap();
        exec_evt_tx.send((stub_exec_event()).into()).unwrap();

        flush_all_pending(
            &mut pending,
            &mut time_rx,
            &mut system_evt_rx,
            &mut system_cmd_rx,
            &mut exec_evt_rx,
            &mut exec_cmd_rx,
            &mut data_evt_rx,
            &mut data_cmd_rx,
        );

        // Both order and report events are drained by pending.drain()
        assert!(pending.order_evts.is_empty());
        assert!(pending.exec_reports.is_empty());
        assert!(exec_evt_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_flush_all_pending_routes_account_event_immediately() {
        let (_time_tx, time_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let mut time_rx = SnapshotReceiver::from(time_rx);
        let (_system_evt_tx, system_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemEvent>>();
        let mut system_evt_rx = SnapshotReceiver::from(system_evt_rx);
        let (_system_cmd_tx, system_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemCommand>>();
        let mut system_cmd_rx = SnapshotReceiver::from(system_cmd_rx);
        let (_data_evt_tx, data_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut data_evt_rx = SnapshotReceiver::from(data_evt_rx);
        let (_data_cmd_tx, data_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut data_cmd_rx = SnapshotReceiver::from(data_cmd_rx);
        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);
        let (_exec_cmd_tx, exec_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let mut exec_cmd_rx = SnapshotReceiver::from(exec_cmd_rx);

        let mut pending = PendingEvents::default();

        exec_evt_tx.send((stub_account_event()).into()).unwrap();

        flush_all_pending(
            &mut pending,
            &mut time_rx,
            &mut system_evt_rx,
            &mut system_cmd_rx,
            &mut exec_evt_rx,
            &mut exec_cmd_rx,
            &mut data_evt_rx,
            &mut data_cmd_rx,
        );

        // Account events are forwarded immediately, never buffered in pending
        assert!(pending.exec_reports.is_empty());
        assert!(pending.order_evts.is_empty());
        assert!(pending.exec_cmds.is_empty());
        assert!(exec_evt_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_pending_is_empty_when_default() {
        let pending = PendingEvents::default();

        assert!(pending.is_empty());
    }

    #[rstest]
    fn test_pending_is_empty_false_with_data_evt() {
        let mut pending = PendingEvents::default();
        pending.data_evts.push((stub_data_event()).into());

        assert!(!pending.is_empty());
    }

    #[rstest]
    fn test_pending_is_empty_false_with_data_cmd() {
        let mut pending = PendingEvents::default();
        pending.data_cmds.push(stub_data_command().into());

        assert!(!pending.is_empty());
    }

    #[rstest]
    fn test_pending_is_empty_false_with_exec_cmd() {
        let mut pending = PendingEvents::default();
        pending
            .exec_cmds
            .push(stub_trading_command_message().into());

        assert!(!pending.is_empty());
    }

    #[rstest]
    fn test_pending_drain_preserves_trading_command_target() {
        std::thread::spawn(|| {
            msgbus::get_message_bus().borrow_mut().dispose();
            let risk_commands = Rc::new(RefCell::new(Vec::new()));
            let exec_commands = Rc::new(RefCell::new(Vec::new()));

            let risk_commands_handler = risk_commands.clone();
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(move |command: TradingCommand| {
                    risk_commands_handler.borrow_mut().push(command);
                }),
            );

            let exec_commands_handler = exec_commands.clone();
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::exec_engine_execute(),
                TypedIntoHandler::from(move |command: TradingCommand| {
                    exec_commands_handler.borrow_mut().push(command);
                }),
            );

            let mut pending = PendingEvents::default();
            pending.exec_cmds.push(
                TradingCommandMessage::new(
                    MessagingSwitchboard::risk_engine_execute(),
                    TradingCommand::QueryAccount(QueryAccount::new(
                        TraderId::from("TESTER-001"),
                        None,
                        AccountId::from("TEST-001"),
                        nautilus_common::recovery_trace::native_event_uuid(),
                        UnixNanos::default(),
                        None,
                        None,
                    )),
                )
                .into(),
            );

            pending.drain();

            assert!(pending.is_empty());
            assert_eq!(risk_commands.borrow().len(), 1);
            assert!(matches!(
                &risk_commands.borrow()[0],
                TradingCommand::QueryAccount(_)
            ));
            assert_eq!(exec_commands.borrow().as_slice(), &[]);
        })
        .join()
        .unwrap();
    }

    #[rstest]
    fn test_pending_is_empty_false_with_exec_report() {
        let mut pending = PendingEvents::default();

        if let ExecutionEvent::Report(report) = stub_exec_event() {
            pending.exec_reports.push((report).into());
        }

        assert!(!pending.is_empty());
    }

    #[rstest]
    fn test_pending_is_empty_false_with_order_evt() {
        let mut pending = PendingEvents::default();

        if let ExecutionEvent::Order(order_evt) = stub_order_event() {
            pending.order_evts.push((order_evt).into());
        }

        assert!(!pending.is_empty());
    }

    fn stub_submitted_batch_event() -> ExecutionEvent {
        use nautilus_model::{
            events::{OrderSubmittedBatch, order::spec::OrderSubmittedSpec},
            identifiers::ClientOrderId,
        };

        let events = vec![
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderSubmittedSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        ExecutionEvent::OrderSubmittedBatch(OrderSubmittedBatch::new(events))
    }

    fn stub_canceled_batch_event() -> ExecutionEvent {
        use nautilus_model::{
            events::{OrderCanceledBatch, order::spec::OrderCanceledSpec},
            identifiers::ClientOrderId,
        };

        let events = vec![
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-001"))
                .build(),
            OrderCanceledSpec::builder()
                .client_order_id(ClientOrderId::from("O-002"))
                .build(),
        ];

        ExecutionEvent::OrderCanceledBatch(OrderCanceledBatch::new(events))
    }

    #[rstest]
    fn test_flush_all_pending_buffers_submitted_batch_as_individual_events() {
        let (_time_tx, time_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let mut time_rx = SnapshotReceiver::from(time_rx);
        let (_system_evt_tx, system_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemEvent>>();
        let mut system_evt_rx = SnapshotReceiver::from(system_evt_rx);
        let (_system_cmd_tx, system_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemCommand>>();
        let mut system_cmd_rx = SnapshotReceiver::from(system_cmd_rx);
        let (_data_evt_tx, data_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut data_evt_rx = SnapshotReceiver::from(data_evt_rx);
        let (_data_cmd_tx, data_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut data_cmd_rx = SnapshotReceiver::from(data_cmd_rx);
        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);
        let (_exec_cmd_tx, exec_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let mut exec_cmd_rx = SnapshotReceiver::from(exec_cmd_rx);

        let mut pending = PendingEvents::default();

        exec_evt_tx
            .send((stub_submitted_batch_event()).into())
            .unwrap();

        flush_all_pending(
            &mut pending,
            &mut time_rx,
            &mut system_evt_rx,
            &mut system_cmd_rx,
            &mut exec_evt_rx,
            &mut exec_cmd_rx,
            &mut data_evt_rx,
            &mut data_cmd_rx,
        );

        // Batch should be unpacked into individual Submitted events then drained
        assert!(pending.order_evts.is_empty());
        assert!(exec_evt_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_flush_all_pending_buffers_canceled_batch_as_individual_events() {
        let (_time_tx, time_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TimeEventMessage>>();
        let mut time_rx = SnapshotReceiver::from(time_rx);
        let (_system_evt_tx, system_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemEvent>>();
        let mut system_evt_rx = SnapshotReceiver::from(system_evt_rx);
        let (_system_cmd_tx, system_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<SystemCommand>>();
        let mut system_cmd_rx = SnapshotReceiver::from(system_cmd_rx);
        let (_data_evt_tx, data_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataEvent>>();
        let mut data_evt_rx = SnapshotReceiver::from(data_evt_rx);
        let (_data_cmd_tx, data_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<DataCommand>>();
        let mut data_cmd_rx = SnapshotReceiver::from(data_cmd_rx);
        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);
        let (_exec_cmd_tx, exec_cmd_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<TradingCommandMessage>>();
        let mut exec_cmd_rx = SnapshotReceiver::from(exec_cmd_rx);

        let mut pending = PendingEvents::default();

        exec_evt_tx
            .send((stub_canceled_batch_event()).into())
            .unwrap();

        flush_all_pending(
            &mut pending,
            &mut time_rx,
            &mut system_evt_rx,
            &mut system_cmd_rx,
            &mut exec_evt_rx,
            &mut exec_cmd_rx,
            &mut data_evt_rx,
            &mut data_cmd_rx,
        );

        // Batch should be unpacked into individual Canceled events then drained
        assert!(pending.order_evts.is_empty());
        assert!(exec_evt_rx.try_recv().is_err());
    }

    #[rstest]
    fn test_flush_all_pending_expands_batch_into_order_evts_before_drain() {
        use nautilus_model::identifiers::ClientOrderId;

        let (exec_evt_tx, exec_evt_rx) =
            tokio::sync::mpsc::unbounded_channel::<DispatchMessage<ExecutionEvent>>();
        let mut exec_evt_rx = SnapshotReceiver::from(exec_evt_rx);

        exec_evt_tx
            .send((stub_canceled_batch_event()).into())
            .unwrap();

        let mut pending = PendingEvents::default();

        while let Ok(evt) = exec_evt_rx.try_recv() {
            pending.push_exec_event(evt);
        }

        let events: Vec<_> = pending
            .order_evts
            .drain(..)
            .map(|event| event.dispatch(|event| event))
            .collect();
        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], OrderEventAny::Canceled(c) if c.client_order_id == ClientOrderId::from("O-001"))
        );
        assert!(
            matches!(&events[1], OrderEventAny::Canceled(c) if c.client_order_id == ClientOrderId::from("O-002"))
        );
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_confirmation_prevents_duplicate_tracking(
        #[values(false, true)] report_confirmation: bool,
        #[values(false, true)] exhausted: bool,
    ) {
        let (mut node, order, _) = venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingUpdate(
            OrderPendingUpdateSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .build(),
        )));
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);

        if exhausted {
            advance_clock(Duration::from_millis(101)).await;
            let result = node.exec_manager.check_inflight_orders();
            assert!(result.events.is_empty());
            assert!(result.queries.is_empty());
            assert_eq!(
                node.exec_manager
                    .take_submission_recovery_exhaustions()
                    .len(),
                1
            );
        }

        if report_confirmation {
            node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                OrderStatusReport::new(
                    account_id,
                    order.instrument_id(),
                    Some(client_order_id),
                    venue_order_id,
                    Some(order.order_side()),
                    order.order_type(),
                    order.time_in_force(),
                    OrderStatus::Submitted,
                    order.quantity(),
                    Quantity::zero(3),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    None,
                )
                .with_price(order.price().unwrap()),
            ))));
            node.exec_manager
                .register_submission(order.init_event(), Some(ClientId::from("OTHER")));
            advance_clock(Duration::from_millis(101)).await;
            let result = node.exec_manager.check_inflight_orders();
            assert!(result.queries.is_empty());
            assert!(result.events.is_empty());
        }

        let mut update = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .account_id(account_id)
            .quantity(order.quantity())
            .price(order.price().unwrap())
            .build();

        if !report_confirmation {
            update.venue_order_id = Some(venue_order_id);
        }
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(update)));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );

        for _ in 0..3 {
            node.exec_manager
                .register_submission(order.init_event(), Some(ClientId::from("OTHER")));
            advance_clock(Duration::from_millis(101)).await;
            let result = node.exec_manager.check_inflight_orders();
            assert!(result.queries.is_empty());
            assert!(result.events.is_empty());
            assert!(
                node.exec_manager
                    .take_submission_recovery_exhaustions()
                    .is_empty()
            );
        }
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_report_confirmation_preserves_later_command_budget(
        #[values(false, true)] cancel: bool,
        #[values(false, true)] later_fill: bool,
        #[values(false, true)] repeated_report: bool,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node(policy);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        let initial_command = venue_evidence_command(&order, false, false);
        let initial_command_id = venue_evidence_command_id(&initial_command);
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingUpdate(
            OrderPendingUpdateSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .build(),
        )));
        assert!(
            node.exec_manager
                .register_modify_inflight(client_order_id, initial_command_id)
                .unwrap()
        );
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);
        let report = ExecutionReport::Order(Box::new(
            OrderStatusReport::new(
                account_id,
                order.instrument_id(),
                Some(client_order_id),
                venue_order_id,
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(order.price().unwrap()),
        ));
        node.process_exec_event(ExecutionEvent::Report(report.clone()));
        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();
        assert!(result.events.is_empty());
        assert!(result.queries.is_empty());

        let mut initial_updated = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .account_id(account_id)
            .quantity(order.quantity())
            .price(order.price().unwrap())
            .build();
        initial_updated.causation_id = Some(initial_command_id);
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(
            initial_updated,
        )));
        let mutation_command = venue_evidence_command(&order, cancel, false);
        let mutation_command_id = venue_evidence_command_id(&mutation_command);
        let pending = if cancel {
            OrderEventAny::PendingCancel(
                OrderPendingCancelSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        } else {
            OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        };
        node.process_exec_event(ExecutionEvent::Order(pending));
        if cancel {
            assert!(
                node.exec_manager
                    .register_cancel_inflight(client_order_id, mutation_command_id)
                    .unwrap()
            );
        } else {
            assert!(
                node.exec_manager
                    .register_modify_inflight(client_order_id, mutation_command_id)
                    .unwrap()
            );
        }

        if repeated_report {
            node.process_exec_event(ExecutionEvent::Report(report));
        }
        advance_clock(Duration::from_millis(50)).await;

        if later_fill {
            let cached = node
                .kernel
                .cache
                .borrow()
                .order_owned(&client_order_id)
                .unwrap();
            let OrderEventAny::Filled(mut fill) = OrderFilledTestBuilder::new(&cached, &instrument)
                .last_qty(Quantity::from("0.400"))
                .last_px(order.price().unwrap())
                .commission(Money::from("0 USDT"))
                .without_position_id()
                .build()
            else {
                unreachable!()
            };
            fill.venue_order_id = venue_order_id;
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(fill)));
        }
        let early = node.exec_manager.check_inflight_orders();
        assert!(early.events.is_empty());
        assert!(early.queries.is_empty());
        advance_clock(Duration::from_millis(51)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(
            first.queries.len(),
            usize::from(!repeated_report || policy == SubmissionRecoveryPolicy::RetainUnresolved)
        );
        assert!(first.events.is_empty());
        advance_clock(Duration::from_millis(101)).await;
        let second = node.exec_manager.check_inflight_orders();
        assert!(second.queries.is_empty());
        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            assert!(second.events.is_empty());
            let cached = node
                .kernel
                .cache
                .borrow()
                .order_owned(&client_order_id)
                .unwrap();
            assert_eq!(
                cached.filled_qty(),
                if later_fill {
                    Quantity::from("0.400")
                } else {
                    Quantity::zero(3)
                }
            );
            assert_eq!(
                cached.previous_status(),
                Some(if later_fill {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Submitted
                })
            );
            assert_retained_mutation_requires_matching_venue_outcome(
                &mut node,
                &order,
                mutation_command_id,
                cancel,
                if cancel {
                    OrderStatus::PendingCancel
                } else {
                    OrderStatus::PendingUpdate
                },
                if cancel {
                    OrderStatus::Canceled
                } else if later_fill {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Submitted
                },
                (account_id, venue_order_id),
            );
        } else if repeated_report {
            assert!(second.events.is_empty());
        } else {
            assert!(
                matches!(&second.events[..], [OrderEventAny::Canceled(event)] if event.client_order_id == client_order_id)
            );
        }
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_confirmed_submission_command_dispatch_starts_own_budget(
        #[values(false, true)] report_confirmation: bool,
        #[values(false, true)] cancel: bool,
        #[values(false, true)] batch: bool,
        #[values(false, true)] pending_before_dispatch: bool,
        #[values(false, true)] missing_order_check: bool,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node_with_config(
            crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                open_check_threshold_ms: 5_000,
                open_check_open_only: !missing_order_check,
                open_check_lookback_mins: None,
                open_check_missing_retries: 5,
                single_order_query_delay_ms: 0,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            OrderType::Limit,
            true,
        );
        let client_order_id = order.client_order_id();
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();

        // Consume part of the original submission budget before venue confirmation
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);

        if report_confirmation {
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )));
            node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                OrderStatusReport::new(
                    account_id,
                    order.instrument_id(),
                    Some(client_order_id),
                    venue_order_id,
                    Some(order.order_side()),
                    order.order_type(),
                    order.time_in_force(),
                    OrderStatus::Submitted,
                    order.quantity(),
                    Quantity::zero(3),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    UnixNanos::default(),
                    None,
                )
                .with_price(order.price().unwrap()),
            ))));
        }

        let mut update = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .account_id(account_id)
            .quantity(order.quantity())
            .price(order.price().unwrap())
            .build();

        if !report_confirmation {
            update.venue_order_id = Some(venue_order_id);
        }
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(update)));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Submitted,
        );
        advance_clock(Duration::from_millis(101)).await;
        let confirmed = node.exec_manager.check_inflight_orders();
        assert!(confirmed.queries.is_empty());
        assert!(confirmed.events.is_empty());

        let pending = if cancel {
            OrderEventAny::PendingCancel(
                OrderPendingCancelSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        } else {
            OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        };
        let command = venue_evidence_command(&order, cancel, batch);
        let mutation_command_id = venue_evidence_command_id(&command);

        if pending_before_dispatch {
            node.process_exec_event(ExecutionEvent::Order(pending.clone()));
        }
        node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            command,
        )));
        advance_clock(Duration::from_millis(50)).await;

        if missing_order_check {
            let engine = node.kernel.exec_engine.borrow();
            let clients = engine.get_all_clients();
            let check = node
                .exec_manager
                .prepare_open_order_report_check(UUID4::new(), &clients);
            assert_eq!(check.filtered_orders.len(), 1);
            let missing = node.exec_manager.reconcile_open_order_reports(
                &check,
                Vec::new(),
                &IndexSet::from([client_id]),
                &IndexSet::new(),
                &clients,
            );
            assert!(missing.events.is_empty());
            assert!(missing.targeted_queries.is_empty());
            assert_eq!(
                node.exec_manager.recon_check_retry_count(&client_order_id),
                1
            );
        }
        let early = node.exec_manager.check_inflight_orders();
        assert!(early.queries.is_empty());
        assert!(early.events.is_empty());

        if !pending_before_dispatch {
            // The queued pending event must not be needed to register the command,
            // nor shift its timeout start from dispatch to event application.
            assert_eq!(
                node.kernel
                    .cache
                    .borrow()
                    .order(&client_order_id)
                    .unwrap()
                    .status(),
                OrderStatus::Submitted,
            );
            node.process_exec_event(ExecutionEvent::Order(pending));
        }

        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            node.exec_manager
                .register_submission(order.init_event(), Some(client_id));
        }
        advance_clock(Duration::from_millis(51)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), 1);
        assert!(
            matches!(&first.queries[0], TradingCommand::QueryOrder(query)
            if query.client_order_id == client_order_id)
        );
        assert!(first.events.is_empty());
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());
        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            assert!(exhausted.events.is_empty());
        } else {
            assert!(
                matches!(&exhausted.events[..], [OrderEventAny::Canceled(event)]
                    if event.client_order_id == client_order_id)
            );
        }
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
        if policy == SubmissionRecoveryPolicy::RetainUnresolved {
            assert_retained_mutation_requires_matching_venue_outcome(
                &mut node,
                &order,
                mutation_command_id,
                cancel,
                if cancel {
                    OrderStatus::PendingCancel
                } else {
                    OrderStatus::PendingUpdate
                },
                if cancel {
                    OrderStatus::Canceled
                } else {
                    OrderStatus::Submitted
                },
                (account_id, venue_order_id),
            );
        } else {
            node.process_reconciliation_events(&exhausted.events);
            assert_eq!(
                node.kernel
                    .cache
                    .borrow()
                    .order(&client_order_id)
                    .unwrap()
                    .status(),
                OrderStatus::Canceled,
            );
        }
    }

    #[rstest]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_command_report_cleanup_matches_default(
        #[values(OrderStatus::Submitted, OrderStatus::PartiallyFilled)] report_status: OrderStatus,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node(policy);
        let client_order_id = order.client_order_id();
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::accepted(
            &order,
            account_id,
            venue_order_id,
        )));
        node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            venue_evidence_command(&order, true, false),
        )));
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingCancel(
            OrderPendingCancelSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .venue_order_id(venue_order_id)
                .build(),
        )));
        advance_clock(Duration::from_millis(101)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), 1);
        assert!(first.events.is_empty());

        let filled_qty = if report_status == OrderStatus::PartiallyFilled {
            Quantity::from("0.400")
        } else {
            Quantity::zero(3)
        };

        let report = OrderStatusReport::new(
            account_id,
            order.instrument_id(),
            Some(client_order_id),
            venue_order_id,
            Some(order.order_side()),
            order.order_type(),
            order.time_in_force(),
            report_status,
            order.quantity(),
            filled_qty,
            UnixNanos::default(),
            UnixNanos::default(),
            UnixNanos::default(),
            None,
        )
        .with_price(order.price().unwrap())
        .with_avg_px(dec!(100.00));
        node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
            report,
        ))));
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();

        assert!(exhausted.queries.is_empty());
        assert!(exhausted.events.is_empty());
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
        let cache = node.kernel.cache.borrow();
        let cached = cache.order_ref(&client_order_id).unwrap();
        assert_eq!(cached.status(), OrderStatus::PendingCancel);
        assert_eq!(cached.filled_qty(), filled_qty);
    }

    #[rstest]
    #[case::accepted_modify(false, "accepted")]
    #[case::accepted_cancel(true, "accepted")]
    #[case::accepted_batch_modify(false, "accepted_batch")]
    #[case::accepted_batch_cancel(true, "accepted_batch")]
    #[case::updated_cancel(true, "updated")]
    #[case::triggered_cancel(true, "triggered")]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_acknowledgement_retires_overlapping_command_recovery(
        #[case] cancel: bool,
        #[case] evidence: &str,
        #[values(false, true)] batch: bool,
        #[values(false, true)] report_first: bool,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        use nautilus_model::events::order::spec::OrderTriggeredSpec;

        let triggered = evidence == "triggered";

        let order_type = if triggered {
            OrderType::StopLimit
        } else {
            OrderType::Limit
        };
        let (mut node, order, instrument) = venue_evidence_node_with_order_type(policy, order_type);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                ClientId::from("TEST-EVIDENCE"),
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);

        if triggered || report_first {
            // Evidence can overtake a different, queued command intent
            let previous_pending = if cancel {
                OrderEventAny::PendingUpdate(
                    OrderPendingUpdateSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            } else {
                OrderEventAny::PendingCancel(
                    OrderPendingCancelSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            };
            node.process_exec_event(ExecutionEvent::Order(previous_pending));
        }
        advance_clock(Duration::from_millis(10)).await;
        node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            venue_evidence_command(&order, cancel, batch),
        )));

        if report_first {
            let report = OrderStatusReport::new(
                account_id,
                instrument.id(),
                Some(client_order_id),
                venue_order_id,
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(order.price().unwrap());
            node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                report,
            ))));
            assert!(
                node.kernel
                    .cache
                    .borrow()
                    .order(&client_order_id)
                    .unwrap()
                    .venue_order_id()
                    .is_none()
            );
        }
        advance_clock(Duration::from_millis(40)).await;
        let acknowledgement = if triggered {
            OrderEventAny::Triggered(
                OrderTriggeredSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .venue_order_id(venue_order_id)
                    .build(),
            )
        } else if evidence == "updated" {
            OrderEventAny::Updated(
                OrderUpdatedSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .venue_order_id(venue_order_id)
                    .quantity(order.quantity())
                    .price(order.price().unwrap())
                    .build(),
            )
        } else {
            TestOrderEventStubs::accepted(&order, account_id, venue_order_id)
        };

        if evidence == "accepted_batch" {
            let OrderEventAny::Accepted(accepted) = acknowledgement else {
                unreachable!()
            };
            node.process_exec_event(ExecutionEvent::OrderAcceptedBatch(OrderAcceptedBatch::new(
                vec![accepted],
            )));
        } else {
            node.process_exec_event(ExecutionEvent::Order(acknowledgement));
        }
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            match evidence {
                "triggered" => OrderStatus::Triggered,
                "updated" => OrderStatus::Submitted,
                _ => OrderStatus::Accepted,
            }
        );
        advance_clock(Duration::from_millis(20)).await;
        let pending = if cancel {
            OrderEventAny::PendingCancel(
                OrderPendingCancelSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        } else {
            OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )
        };
        node.process_exec_event(ExecutionEvent::Order(pending));
        let early = node.exec_manager.check_inflight_orders();
        assert!(early.queries.is_empty());
        assert!(early.events.is_empty());
        advance_clock(Duration::from_millis(41)).await;
        let first = node.exec_manager.check_inflight_orders();
        // The default does not clear inflight tracking on Triggered events.
        let recover_command =
            triggered && !report_first && policy == SubmissionRecoveryPolicy::ResolveLocally;
        assert_eq!(first.queries.len(), usize::from(recover_command));
        assert!(first.events.is_empty());
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );

        if recover_command {
            assert!(
                matches!(&exhausted.events[..], [OrderEventAny::Canceled(event)]
                if event.client_order_id == client_order_id)
            );
        } else {
            assert!(exhausted.events.is_empty());
        }
    }

    #[rstest]
    #[case::submission_only(None, false)]
    #[case::modify(Some(false), false)]
    #[case::cancel(Some(true), false)]
    #[case::batch_modify(Some(false), true)]
    #[case::batch_cancel(Some(true), true)]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_unconfirmed_submission_bulk_confirmation_before_registration(
        #[case] cancel: Option<bool>,
        #[case] batch: bool,
        #[values(false, true)] missing_order_check: bool,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node_with_config(
            crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                open_check_threshold_ms: 1,
                open_check_open_only: false,
                open_check_lookback_mins: None,
                open_check_missing_retries: 5,
                single_order_query_delay_ms: 0,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            OrderType::Limit,
            false,
        );
        let client_order_id = order.client_order_id();
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account_id = AccountId::from("TEST-001");
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        let queries = Rc::new(RefCell::new(Vec::new()));
        let client = OrderReportClient {
            client_id,
            report: OrderStatusReport::new(
                account_id,
                instrument.id(),
                Some(client_order_id),
                VenueOrderId::from("V-VENUE-EVIDENCE"),
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(order.price().unwrap()),
            targeted: false,
            queries: queries.clone(),
        };
        advance_clock(Duration::from_millis(2)).await;
        let events = node.exec_manager.check_open_orders(&[&client]).await;
        assert!(events.is_empty());
        assert_eq!(*queries.borrow(), vec!["bulk"]);
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
        advance_clock(Duration::from_millis(2)).await;

        if missing_order_check {
            let engine = node.kernel.exec_engine.borrow();
            let clients = engine.get_all_clients();
            let check = node
                .exec_manager
                .prepare_open_order_report_check(UUID4::new(), &clients);
            assert_eq!(check.filtered_orders.len(), 1);
            let missing = node.exec_manager.reconcile_open_order_reports(
                &check,
                Vec::new(),
                &IndexSet::from([client_id]),
                &IndexSet::new(),
                &clients,
            );
            assert!(missing.events.is_empty());
            assert!(missing.targeted_queries.is_empty());
            assert_eq!(
                node.exec_manager.recon_check_retry_count(&client_order_id),
                1
            );
        }
        node.exec_manager
            .register_submission(order.init_event(), Some(client_id));
        advance_clock(Duration::from_millis(101)).await;
        let duplicate = node.exec_manager.check_inflight_orders();
        assert!(duplicate.events.is_empty());
        assert_eq!(
            duplicate.queries.len(),
            usize::from(policy == SubmissionRecoveryPolicy::ResolveLocally)
        );

        let mutation_command_id = cancel.map(|cancel| {
            let command = venue_evidence_command(&order, cancel, batch);
            let mutation_command_id = venue_evidence_command_id(&command);
            node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                command,
            )));
            let pending = if cancel {
                OrderEventAny::PendingCancel(
                    OrderPendingCancelSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            } else {
                OrderEventAny::PendingUpdate(
                    OrderPendingUpdateSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            };
            node.process_exec_event(ExecutionEvent::Order(pending));
            mutation_command_id
        });
        advance_clock(Duration::from_millis(101)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), usize::from(cancel.is_some()));

        if cancel.is_none() && policy == SubmissionRecoveryPolicy::ResolveLocally {
            assert!(matches!(&first.events[..], [OrderEventAny::Rejected(event)]
                if event.client_order_id == client_order_id));
        } else {
            assert!(first.events.is_empty());
        }
        node.process_reconciliation_events(&first.events);
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());

        if let Some(cancel) = cancel {
            if policy == SubmissionRecoveryPolicy::RetainUnresolved {
                assert!(exhausted.events.is_empty());
                assert_retained_mutation_requires_matching_venue_outcome(
                    &mut node,
                    &order,
                    mutation_command_id.unwrap(),
                    cancel,
                    if cancel {
                        OrderStatus::PendingCancel
                    } else {
                        OrderStatus::PendingUpdate
                    },
                    if cancel {
                        OrderStatus::Canceled
                    } else {
                        OrderStatus::Submitted
                    },
                    (client.report.account_id, client.report.venue_order_id),
                );
            } else {
                assert!(
                    matches!(&exhausted.events[..], [OrderEventAny::Canceled(event)]
                    if event.client_order_id == client_order_id)
                );
            }
        } else {
            assert!(exhausted.events.is_empty());
        }
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    #[rstest]
    #[case::submission_only(None, false)]
    #[case::modify(Some(false), false)]
    #[case::cancel(Some(true), false)]
    #[case::batch_modify(Some(false), true)]
    #[case::batch_cancel(Some(true), true)]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_unconfirmed_submission_query_report_confirms_before_exhaustion(
        #[case] cancel: Option<bool>,
        #[case] batch: bool,
        #[values(false, true)] targeted: bool,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node_with_config(
            crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                open_check_threshold_ms: 1,
                open_check_open_only: false,
                open_check_lookback_mins: None,
                open_check_missing_retries: 1,
                single_order_query_delay_ms: 0,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            OrderType::Limit,
            true,
        );
        let client_order_id = order.client_order_id();
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account_id = AccountId::from("TEST-001");
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);
        advance_clock(Duration::from_millis(10)).await;

        let mutation_command_id = cancel.map(|cancel| {
            let command = venue_evidence_command(&order, cancel, batch);
            let mutation_command_id = venue_evidence_command_id(&command);
            node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                command,
            )));
            let pending = if cancel {
                OrderEventAny::PendingCancel(
                    OrderPendingCancelSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            } else {
                OrderEventAny::PendingUpdate(
                    OrderPendingUpdateSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            };
            node.process_exec_event(ExecutionEvent::Order(pending));
            mutation_command_id
        });
        let queries = Rc::new(RefCell::new(Vec::new()));
        let client = OrderReportClient {
            client_id,
            report: OrderStatusReport::new(
                account_id,
                order.instrument_id(),
                Some(client_order_id),
                VenueOrderId::from("V-VENUE-EVIDENCE"),
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(order.price().unwrap()),
            targeted,
            queries: queries.clone(),
        };
        advance_clock(Duration::from_millis(40)).await;
        let events = node.exec_manager.check_open_orders(&[&client]).await;
        assert!(events.is_empty());
        assert_eq!(
            *queries.borrow(),
            if targeted {
                vec!["bulk", "targeted"]
            } else {
                vec!["bulk"]
            }
        );
        node.process_reconciliation_events(&events);
        advance_clock(Duration::from_millis(61)).await;
        let first = node.exec_manager.check_inflight_orders();
        let recover_command =
            cancel.is_some() && policy == SubmissionRecoveryPolicy::ResolveLocally;
        assert_eq!(first.queries.len(), usize::from(recover_command));

        if cancel.is_none() && policy == SubmissionRecoveryPolicy::ResolveLocally {
            assert!(matches!(&first.events[..], [OrderEventAny::Rejected(event)]
                if event.client_order_id == client_order_id));
        } else {
            assert!(first.events.is_empty());
        }
        node.process_reconciliation_events(&first.events);
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());

        if recover_command {
            assert!(
                matches!(&exhausted.events[..], [OrderEventAny::Canceled(event)]
                if event.client_order_id == client_order_id)
            );
        } else {
            assert!(exhausted.events.is_empty());
        }
        if policy == SubmissionRecoveryPolicy::RetainUnresolved
            && let Some(cancel) = cancel
        {
            assert_retained_mutation_requires_matching_venue_outcome(
                &mut node,
                &order,
                mutation_command_id.unwrap(),
                cancel,
                if cancel {
                    OrderStatus::PendingCancel
                } else {
                    OrderStatus::PendingUpdate
                },
                if cancel {
                    OrderStatus::Canceled
                } else {
                    OrderStatus::Submitted
                },
                (client.report.account_id, client.report.venue_order_id),
            );
        }
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    #[rstest]
    #[case::local_update_before_pending("updated")]
    #[case::modify_rejected_after_pending("modify_rejected")]
    #[case::cancel_rejected_after_pending("cancel_rejected")]
    #[case::modify_rejected_after_cancel_pending("modify_rejected_after_cancel_pending")]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_unconfirmed_submission_completed_command_does_not_revive_budget(
        #[case] completion: &str,
        #[values(
            SubmissionRecoveryPolicy::ResolveLocally,
            SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: SubmissionRecoveryPolicy,
    ) {
        use nautilus_model::events::order::spec::{
            OrderCancelRejectedSpec, OrderModifyRejectedSpec,
        };

        let (mut node, order, instrument) = venue_evidence_node(policy);
        let client_order_id = order.client_order_id();
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account_id = AccountId::from("TEST-001");
        let cancel = completion == "cancel_rejected";
        let later_cancel_pending = completion == "modify_rejected_after_cancel_pending";
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                client_id,
                account_id,
                instrument.id().venue,
                OmsType::Netting,
                None,
            )))
            .unwrap();
        let command = venue_evidence_command(&order, cancel, false);
        let command_id = venue_evidence_command_id(&command);
        node.process_exec_command(DispatchMessage::from(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            command,
        )));
        advance_clock(Duration::from_millis(10)).await;

        if completion != "updated" {
            let pending = if cancel {
                OrderEventAny::PendingCancel(
                    OrderPendingCancelSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            } else {
                OrderEventAny::PendingUpdate(
                    OrderPendingUpdateSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            };
            node.process_exec_event(ExecutionEvent::Order(pending));
        }

        if later_cancel_pending {
            // The cancel is pending in native state, but has not dispatched
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingCancel(
                OrderPendingCancelSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )));
        }
        advance_clock(Duration::from_millis(10)).await;
        let mut completed = match completion {
            "updated" => OrderEventAny::Updated(
                OrderUpdatedSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .quantity(order.quantity())
                    .price(Price::from("101.00"))
                    .build(),
            ),
            "modify_rejected" | "modify_rejected_after_cancel_pending" => {
                OrderEventAny::ModifyRejected(
                    OrderModifyRejectedSpec::builder()
                        .trader_id(order.trader_id())
                        .strategy_id(order.strategy_id())
                        .instrument_id(order.instrument_id())
                        .client_order_id(client_order_id)
                        .account_id(account_id)
                        .build(),
                )
            }
            "cancel_rejected" => OrderEventAny::CancelRejected(
                OrderCancelRejectedSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            ),
            _ => unreachable!(),
        };
        match &mut completed {
            OrderEventAny::Updated(event) => event.causation_id = Some(command_id),
            OrderEventAny::ModifyRejected(event) => event.causation_id = Some(command_id),
            OrderEventAny::CancelRejected(event) => event.causation_id = Some(command_id),
            _ => unreachable!(),
        }
        node.process_exec_event(ExecutionEvent::Order(completed));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            if later_cancel_pending {
                OrderStatus::PendingCancel
            } else {
                OrderStatus::Submitted
            }
        );

        // A newer pending event is queued, but that command has not dispatched
        advance_clock(Duration::from_millis(10)).await;

        if !later_cancel_pending {
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )));
        }
        advance_clock(Duration::from_millis(10)).await;
        let cached = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let OrderEventAny::Filled(mut fill) = OrderFilledTestBuilder::new(&cached, &instrument)
            .last_qty(Quantity::from("0.400"))
            .last_px(order.price().unwrap())
            .commission(Money::from("0 USDT"))
            .without_position_id()
            .build()
        else {
            unreachable!()
        };
        fill.venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Filled(fill)));

        for _ in 0..3 {
            advance_clock(Duration::from_millis(101)).await;
            let result = node.exec_manager.check_inflight_orders();
            assert!(result.queries.is_empty());
            assert!(result.events.is_empty());
            assert!(
                node.exec_manager
                    .take_submission_recovery_exhaustions()
                    .is_empty()
            );
        }
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            if later_cancel_pending {
                OrderStatus::PendingCancel
            } else {
                OrderStatus::PendingUpdate
            }
        );
    }

    #[rstest]
    #[case::wrong_account("account")]
    #[case::wrong_instrument("instrument")]
    #[case::conflicting_report_venue("report_venue")]
    #[case::unapplied_venue_update("update_venue")]
    #[case::local_update("local_update")]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_submission_confirmation_requires_matching_venue_evidence(#[case] invalid: &str) {
        let (mut node, order, _) = venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");

        if invalid.ends_with("venue") {
            insert_accepted_limit_order_in_node(
                &node,
                account_id,
                ClientId::from("TEST-EVIDENCE"),
                order.instrument_id(),
                ClientOrderId::from("O-OTHER"),
                venue_order_id,
            );
        }
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingCancel(
            OrderPendingCancelSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .build(),
        )));
        advance_clock(Duration::from_millis(101)).await;
        assert_eq!(node.exec_manager.check_inflight_orders().queries.len(), 1);

        if invalid == "update_venue" || invalid == "local_update" {
            let mut updated = OrderUpdatedSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .quantity(order.quantity())
                .price(order.price().unwrap())
                .build();

            if invalid == "update_venue" {
                updated.venue_order_id = Some(venue_order_id);
            }
            node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(updated)));
        } else {
            let report = OrderStatusReport::new(
                if invalid == "account" {
                    AccountId::from("OTHER-001")
                } else {
                    account_id
                },
                if invalid == "instrument" {
                    InstrumentId::from("OTHER.BINANCE")
                } else {
                    order.instrument_id()
                },
                Some(client_order_id),
                venue_order_id,
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(order.price().unwrap());
            node.process_exec_event(ExecutionEvent::Report(ExecutionReport::Order(Box::new(
                report,
            ))));
        }
        assert!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .venue_order_id()
                .is_none()
        );
        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();
        assert!(result.events.is_empty());
        assert!(result.queries.is_empty());
        let cached = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        assert_eq!(
            cached.status(),
            if invalid == "local_update" {
                OrderStatus::Submitted
            } else {
                OrderStatus::PendingCancel
            }
        );
        let diagnostics = node.exec_manager.take_submission_recovery_exhaustions();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].client_order_id, client_order_id);
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_untyped_inflight_compatibility_requires_terminal_venue_outcome() {
        let (mut node, order, _) = venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-UNKNOWN-COMPATIBILITY");
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::accepted(
            &order,
            account_id,
            venue_order_id,
        )));
        assert!(node.exec_manager.unresolved_submission_ids().is_empty());
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::PendingUpdate(
            OrderPendingUpdateSpec::builder()
                .trader_id(order.trader_id())
                .strategy_id(order.strategy_id())
                .instrument_id(order.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .venue_order_id(venue_order_id)
                .build(),
        )));
        // The compatibility entry point deliberately cannot assert a dispatch kind.
        node.exec_manager.register_inflight(client_order_id);
        advance_clock(Duration::from_millis(101)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), 1);
        assert!(first.events.is_empty());

        let mut updated = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(client_order_id)
            .account_id(account_id)
            .quantity(order.quantity())
            .price(Price::from("101.00"))
            .build();
        updated.venue_order_id = Some(venue_order_id);
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(updated)));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Accepted
        );
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![client_order_id]
        );
        advance_clock(Duration::from_millis(101)).await;
        let exhausted = node.exec_manager.check_inflight_orders();
        assert!(exhausted.queries.is_empty());
        assert!(exhausted.events.is_empty());
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![client_order_id]
        );
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );

        let cached = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::canceled(
            &cached,
            account_id,
            Some(venue_order_id),
        )));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Canceled
        );
        assert!(node.exec_manager.unresolved_submission_ids().is_empty());
        advance_clock(Duration::from_millis(101)).await;
        let confirmed = node.exec_manager.check_inflight_orders();
        assert!(confirmed.events.is_empty());
        assert!(confirmed.queries.is_empty());
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    fn assert_retained_mutation_requires_matching_venue_outcome(
        node: &mut LiveNode,
        order: &OrderAny,
        command_id: UUID4,
        cancel: bool,
        expected_status: OrderStatus,
        expected_after: OrderStatus,
        venue_identity: (AccountId, VenueOrderId),
    ) {
        let client_order_id = order.client_order_id();
        let cached = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        assert_eq!(cached.status(), expected_status);
        // This is the same unresolved inventory consumed by disconnect_stop:
        // query exhaustion or a repeated submission acknowledgement cannot permit sealing.
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![client_order_id]
        );
        let unchanged = node.exec_manager.check_inflight_orders();
        assert!(unchanged.events.is_empty());
        assert!(unchanged.queries.is_empty());
        assert_eq!(
            serde_json::to_value(
                node.kernel
                    .cache
                    .borrow()
                    .order_owned(&client_order_id)
                    .unwrap()
            )
            .unwrap(),
            serde_json::to_value(&cached).unwrap()
        );
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![client_order_id]
        );

        // A report can establish venue identity without changing the cached order.
        // The caller supplies that actual fixture event/report identity, not a
        // synthesized fallback for a missing cached venue ID.
        let (account_id, venue_order_id) = venue_identity;
        assert_eq!(cached.account_id(), Some(account_id));
        if let Some(cached_venue_id) = cached.venue_order_id() {
            assert_eq!(cached_venue_id, venue_order_id);
        }
        let outcome = if cancel {
            TestOrderEventStubs::canceled(&cached, account_id, Some(venue_order_id))
        } else {
            let mut updated = OrderUpdatedSpec::builder()
                .trader_id(cached.trader_id())
                .strategy_id(cached.strategy_id())
                .instrument_id(cached.instrument_id())
                .client_order_id(client_order_id)
                .account_id(account_id)
                .quantity(cached.quantity())
                .price(Price::from("101.00"))
                .build();
            updated.venue_order_id = Some(venue_order_id);
            updated.causation_id = Some(command_id);
            OrderEventAny::Updated(updated)
        };
        node.process_exec_event(ExecutionEvent::Order(outcome));
        let resolved = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        assert_eq!(resolved.status(), expected_after);
        assert_eq!(resolved.filled_qty(), cached.filled_qty());
        assert!(node.exec_manager.unresolved_submission_ids().is_empty());
        let confirmed = node.exec_manager.check_inflight_orders();
        assert!(confirmed.events.is_empty());
        assert!(confirmed.queries.is_empty());
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    #[rstest]
    #[case::single(false, false)]
    #[case::batch(true, false)]
    #[case::deferred(false, true)]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_mutation_admission_does_not_forward_duplicate_uuid(
        #[case] batch: bool,
        #[case] deferred: bool,
    ) {
        let (mut node, order, instrument) =
            venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client = StubExecutionClient::new(
            ClientId::from("TEST-EVIDENCE"),
            AccountId::from("TEST-001"),
            instrument.id().venue,
            OmsType::Netting,
            None,
        );
        let sends = client.modified_order_ids();
        advance_clock(Duration::from_millis(1)).await;
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(client))
            .unwrap();
        let first = venue_evidence_command(&order, false, batch);
        let first_id = venue_evidence_command_id(&first);
        assert!(sends.borrow().is_empty());
        if deferred {
            use nautilus_common::{msgbus, msgbus::TypedIntoHandler, runner::capture_trading_cmd};
            let child = first.clone();
            msgbus::register_trading_command_endpoint(
                "ADMISSION.DEFERRED".into(),
                TypedIntoHandler::from_with_id(
                    "node-admission-deferred",
                    move |_: TradingCommand| {
                        for _ in 0..2 {
                            capture_trading_cmd(TradingCommandMessage::new(
                                MessagingSwitchboard::exec_engine_execute(),
                                child.clone(),
                            ));
                        }
                    },
                ),
            );
            node.process_exec_command(TradingCommandMessage::new(
                "ADMISSION.DEFERRED".into(),
                first.clone(),
            ));
        } else {
            node.process_exec_command(TradingCommandMessage::new(
                MessagingSwitchboard::exec_engine_execute(),
                first.clone(),
            ));
        }
        assert_eq!(sends.borrow().len(), 1);
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            first,
        ));
        assert_eq!(sends.borrow().len(), 1);

        let second = venue_evidence_command(&order, false, batch);
        let second_id = venue_evidence_command_id(&second);
        assert_ne!(second_id, first_id);
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            second,
        ));
        assert_eq!(sends.borrow().len(), 2);
        let mut updated = OrderUpdatedSpec::builder()
            .trader_id(order.trader_id())
            .strategy_id(order.strategy_id())
            .instrument_id(order.instrument_id())
            .client_order_id(order.client_order_id())
            .account_id(AccountId::from("TEST-001"))
            .quantity(order.quantity())
            .price(Price::from("101.00"))
            .build();
        updated.venue_order_id = Some(VenueOrderId::from("V-VENUE-EVIDENCE"));
        updated.causation_id = Some(second_id);
        node.process_exec_event(ExecutionEvent::Order(OrderEventAny::Updated(updated)));
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![order.client_order_id()]
        );
        assert_retained_mutation_requires_matching_venue_outcome(
            &mut node,
            &order,
            first_id,
            false,
            OrderStatus::Submitted,
            OrderStatus::Submitted,
            (updated.account_id.unwrap(), updated.venue_order_id.unwrap()),
        );
        assert_eq!(sends.borrow().len(), 2);
    }

    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_mixed_duplicate_batch_is_fenced_before_any_forward() {
        let (mut node, order, instrument) =
            venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client = StubExecutionClient::new(
            ClientId::from("TEST-EVIDENCE"),
            AccountId::from("TEST-001"),
            instrument.id().venue,
            OmsType::Netting,
            None,
        );
        let sends = client.modified_order_ids();
        advance_clock(Duration::from_millis(1)).await;
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(client))
            .unwrap();
        let first = venue_evidence_command(&order, false, true);
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            first.clone(),
        ));
        assert_eq!(sends.borrow().len(), 1);
        let TradingCommand::ModifyOrders(mut mixed) = first else {
            unreachable!("original fixture must be a batch");
        };
        let TradingCommand::ModifyOrder(second) = venue_evidence_command(&order, false, false)
        else {
            unreachable!("original fixture must be a single mutation");
        };
        mixed.modifies.push(second);
        node.process_exec_command(TradingCommandMessage::new(
            MessagingSwitchboard::exec_engine_execute(),
            TradingCommand::ModifyOrders(mixed),
        ));
        assert_eq!(sends.borrow().len(), 1);
        assert!(node.kernel.exec_engine.borrow().submissions_fenced());
        assert!(node.kernel.is_shutdown_requested());
        assert_eq!(
            node.exec_manager.unresolved_submission_ids(),
            vec![order.client_order_id()]
        );
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Submitted
        );
    }

    fn venue_evidence_command_id(command: &TradingCommand) -> UUID4 {
        match command {
            TradingCommand::ModifyOrder(command) => command.command_id,
            TradingCommand::ModifyOrders(command) => command.modifies.first().unwrap().command_id,
            TradingCommand::CancelOrder(command) => command.command_id,
            TradingCommand::CancelOrders(command) => command.cancels.first().unwrap().command_id,
            _ => panic!("fixture requires an actual mutation command"),
        }
    }

    fn venue_evidence_command(order: &OrderAny, cancel: bool, batch: bool) -> TradingCommand {
        use nautilus_common::messages::execution::{
            BatchCancelOrders, BatchModifyOrders, CancelOrder, ModifyOrder,
        };

        let client_id = ClientId::from("TEST-EVIDENCE");
        let client_order_id = order.client_order_id();
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");

        if cancel {
            let cancel = CancelOrder::new(
                order.trader_id(),
                Some(client_id),
                order.strategy_id(),
                order.instrument_id(),
                client_order_id,
                Some(venue_order_id),
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            );

            if batch {
                TradingCommand::CancelOrders(BatchCancelOrders::new(
                    order.trader_id(),
                    Some(client_id),
                    order.strategy_id(),
                    order.instrument_id(),
                    vec![cancel],
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                ))
            } else {
                TradingCommand::CancelOrder(cancel)
            }
        } else {
            let modify = ModifyOrder::new(
                order.trader_id(),
                Some(client_id),
                order.strategy_id(),
                order.instrument_id(),
                client_order_id,
                Some(venue_order_id),
                None,
                Some(Price::from("101.00")),
                None,
                UUID4::new(),
                UnixNanos::default(),
                None,
                None,
            );

            if batch {
                TradingCommand::ModifyOrders(BatchModifyOrders::new(
                    order.trader_id(),
                    Some(client_id),
                    order.strategy_id(),
                    order.instrument_id(),
                    vec![modify],
                    UUID4::new(),
                    UnixNanos::default(),
                    None,
                    None,
                ))
            } else {
                TradingCommand::ModifyOrder(modify)
            }
        }
    }

    fn venue_evidence_node(
        policy: crate::execution::submission::SubmissionRecoveryPolicy,
    ) -> (LiveNode, OrderAny, InstrumentAny) {
        venue_evidence_node_with_order_type(policy, OrderType::Limit)
    }

    fn venue_evidence_node_with_order_type(
        policy: crate::execution::submission::SubmissionRecoveryPolicy,
        order_type: OrderType,
    ) -> (LiveNode, OrderAny, InstrumentAny) {
        venue_evidence_node_with_config(
            crate::config::LiveExecutionEngineConfig {
                reconciliation: true,
                inflight_check_threshold_ms: 100,
                inflight_check_retries: 2,
                open_check_threshold_ms: 5_000,
                single_order_query_delay_ms: 0,
                submission_recovery_policy: policy,
                ..Default::default()
            },
            order_type,
            true,
        )
    }

    fn venue_evidence_node_with_config(
        exec_engine: crate::config::LiveExecutionEngineConfig,
        order_type: OrderType,
        register_submission: bool,
    ) -> (LiveNode, OrderAny, InstrumentAny) {
        let config = LiveNodeConfig {
            exec_engine,
            ..Default::default()
        };
        let mut node = LiveNode::build("VenueEvidenceNode".to_string(), Some(config)).unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("TEST-EVIDENCE");
        let account = AccountAny::Margin(MarginAccount::new(
            AccountState::new(
                account_id,
                AccountType::Margin,
                vec![AccountBalance::new(
                    Money::from("1000000 USDT"),
                    Money::from("0 USDT"),
                    Money::from("1000000 USDT"),
                )],
                Vec::new(),
                true,
                UUID4::new(),
                UnixNanos::default(),
                UnixNanos::default(),
                Some(Currency::USDT()),
            ),
            true,
        ));
        node.kernel.cache.borrow_mut().add_account(account).unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(instrument.clone())
            .unwrap();
        let mut builder = OrderTestBuilder::new(order_type);
        builder
            .trader_id(node.trader_id())
            .client_order_id(ClientOrderId::from("O-VENUE-EVIDENCE"))
            .instrument_id(instrument.id())
            .quantity(Quantity::from("1.000"))
            .price(Price::from("100.00"));

        if order_type == OrderType::StopLimit {
            builder.trigger_price(Price::from("110.00"));
        }
        let order = builder.build();
        let command = TradingCommand::SubmitOrder(SubmitOrder::from_order(
            &order,
            node.trader_id(),
            Some(client_id),
            None,
            UUID4::new(),
            UnixNanos::default(),
        ));

        if register_submission {
            assert!(node.observe_exec_command_before_dispatch(&command).unwrap());
        }
        node.kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id), false)
            .unwrap();
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::submitted(
            &order, account_id,
        )));
        (node, order, instrument)
    }

    #[rstest]
    #[case::partial("0.400", OrderStatus::PartiallyFilled)]
    #[case::full("1.000", OrderStatus::Filled)]
    #[tokio::test(start_paused = true)]
    async fn test_retained_submission_resolves_late_fill(
        #[case] quantity: &str,
        #[case] expected_status: OrderStatus,
    ) {
        let (mut node, order, instrument) =
            venue_evidence_node(SubmissionRecoveryPolicy::RetainUnresolved);
        let client_order_id = order.client_order_id();

        for _ in 0..2 {
            advance_clock(Duration::from_millis(101)).await;
            assert!(node.exec_manager.check_inflight_orders().events.is_empty());
        }

        assert_eq!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .len(),
            1
        );
        let fill = TestOrderEventStubs::filled(
            &order,
            &instrument,
            Some(TradeId::from("T-LATE-RETAINED")),
            None,
            Some(Price::from("100.00")),
            Some(Quantity::from(quantity)),
            Some(LiquiditySide::Taker),
            None,
            None,
            Some(AccountId::from("TEST-001")),
        );
        node.process_exec_event(ExecutionEvent::Order(fill));
        advance_clock(Duration::from_millis(101)).await;
        let result = node.exec_manager.check_inflight_orders();
        let cache = node.kernel.cache.borrow();
        let cached = cache.order_ref(&client_order_id).unwrap();
        assert_eq!(cached.status(), expected_status);
        assert_eq!(cached.filled_qty(), Quantity::from(quantity));
        assert_eq!(cached.trade_ids(), vec![&TradeId::from("T-LATE-RETAINED")]);
        assert!(result.events.is_empty());
        assert!(result.queries.is_empty());
        assert!(
            node.exec_manager
                .take_submission_recovery_exhaustions()
                .is_empty()
        );
    }

    #[rstest]
    #[case::submitted_report("order")]
    #[case::submitted_report_with_fills("order_with_fills")]
    #[case::venue_update("updated")]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_venue_evidence_stops_submission_timeout(
        #[case] evidence: &str,
        #[values(
            OrderStatus::Submitted,
            OrderStatus::PendingCancel,
            OrderStatus::PendingUpdate
        )]
        status: OrderStatus,
        #[values(
            crate::execution::submission::SubmissionRecoveryPolicy::ResolveLocally,
            crate::execution::submission::SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: crate::execution::submission::SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, _) = venue_evidence_node(policy);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        let pending = match status {
            OrderStatus::PendingCancel => Some(OrderEventAny::PendingCancel(
                OrderPendingCancelSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )),
            OrderStatus::PendingUpdate => Some(OrderEventAny::PendingUpdate(
                OrderPendingUpdateSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .build(),
            )),
            _ => None,
        };

        if let Some(pending) = pending {
            node.process_exec_event(ExecutionEvent::Order(pending));
        }
        advance_clock(Duration::from_millis(101)).await;
        let first = node.exec_manager.check_inflight_orders();
        assert_eq!(first.queries.len(), 1);
        assert!(first.events.is_empty());
        advance_clock(Duration::from_millis(94)).await;

        let event = if evidence == "updated" {
            ExecutionEvent::Order(OrderEventAny::Updated(
                OrderUpdatedSpec::builder()
                    .trader_id(order.trader_id())
                    .strategy_id(order.strategy_id())
                    .instrument_id(order.instrument_id())
                    .client_order_id(client_order_id)
                    .account_id(account_id)
                    .venue_order_id(venue_order_id)
                    .quantity(order.quantity())
                    .price(Price::from("101.00"))
                    .build(),
            ))
        } else {
            let report = OrderStatusReport::new(
                account_id,
                order.instrument_id(),
                Some(client_order_id),
                venue_order_id,
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                OrderStatus::Submitted,
                order.quantity(),
                Quantity::zero(3),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(Price::from("100.00"));
            ExecutionEvent::Report(if evidence == "order" {
                ExecutionReport::Order(Box::new(report))
            } else {
                ExecutionReport::OrderWithFills(Box::new(report), Vec::new())
            })
        };
        node.process_exec_event(event);
        let expected_status = if evidence == "updated" {
            OrderStatus::Submitted
        } else if status == OrderStatus::Submitted {
            OrderStatus::Accepted
        } else {
            status
        };
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            expected_status,
        );

        for _ in 0..3 {
            advance_clock(Duration::from_millis(101)).await;
            let result = node.exec_manager.check_inflight_orders();
            assert!(result.queries.is_empty());
            assert!(result.events.is_empty());
        }
        assert_eq!(
            node.exec_manager.recon_check_retry_count(&client_order_id),
            0
        );
        assert!(node.exec_manager.check_open_order_queries().is_empty());
        advance_clock(Duration::from_secs(5)).await;
        assert_eq!(node.exec_manager.check_open_order_queries().len(), 1);
    }

    #[rstest]
    #[case::fill("fill", false)]
    #[case::terminal_order("order", true)]
    #[case::terminal_order_with_fills("order_with_fills", true)]
    #[case::working_order_for_closed_cache("order", false)]
    #[case::working_order_with_fills_for_closed_cache("order_with_fills", false)]
    #[cfg_attr(
        not(all(feature = "simulation", madsim)),
        tokio::test(start_paused = true)
    )]
    #[cfg_attr(all(feature = "simulation", madsim), madsim::test)]
    async fn test_report_cleanup_preserves_original_settling_predicate(
        #[case] report_kind: &str,
        #[case] terminal_report: bool,
        #[values(false, true)] explicit_client_id: bool,
        #[values(
            crate::execution::submission::SubmissionRecoveryPolicy::ResolveLocally,
            crate::execution::submission::SubmissionRecoveryPolicy::RetainUnresolved
        )]
        policy: crate::execution::submission::SubmissionRecoveryPolicy,
    ) {
        let (mut node, order, instrument) = venue_evidence_node(policy);
        let client_order_id = order.client_order_id();
        let account_id = AccountId::from("TEST-001");
        let client_id = ClientId::from("TEST-EVIDENCE");
        let venue_order_id = VenueOrderId::from("V-VENUE-EVIDENCE");
        node.process_exec_event(ExecutionEvent::Order(TestOrderEventStubs::accepted(
            &order,
            account_id,
            venue_order_id,
        )));
        let accepted = node
            .kernel
            .cache
            .borrow()
            .order_owned(&client_order_id)
            .unwrap();
        let OrderEventAny::Filled(fill) = OrderFilledTestBuilder::new(&accepted, &instrument)
            .last_qty(accepted.quantity())
            .last_px(Price::from("100.00"))
            .commission(Money::from("0 USDT"))
            .without_position_id()
            .build()
        else {
            unreachable!()
        };
        let mut fill_report = FillReport::new(
            account_id,
            instrument.id(),
            venue_order_id,
            fill.trade_id,
            fill.order_side,
            fill.last_qty,
            fill.last_px,
            fill.commission.unwrap(),
            fill.liquidity_side,
            Some(client_order_id),
            None,
            fill.ts_event,
            fill.ts_init,
            None,
        );

        if !explicit_client_id {
            fill_report.client_order_id = None;
        }
        let event = if report_kind == "fill" {
            ExecutionReport::Fill(Box::new(fill_report))
        } else {
            if !terminal_report {
                node.kernel
                    .cache
                    .borrow_mut()
                    .update_order(&OrderEventAny::Filled(fill))
                    .unwrap();
            }
            let report = OrderStatusReport::new(
                account_id,
                instrument.id(),
                explicit_client_id.then_some(client_order_id),
                venue_order_id,
                Some(order.order_side()),
                order.order_type(),
                order.time_in_force(),
                if terminal_report {
                    OrderStatus::Filled
                } else {
                    OrderStatus::Accepted
                },
                order.quantity(),
                order.quantity(),
                UnixNanos::default(),
                UnixNanos::default(),
                UnixNanos::default(),
                None,
            )
            .with_price(Price::from("100.00"))
            .with_avg_px(dec!(100.00));

            if report_kind == "order" {
                ExecutionReport::Order(Box::new(report))
            } else {
                ExecutionReport::OrderWithFills(Box::new(report), vec![fill_report])
            }
        };
        node.process_exec_event(ExecutionEvent::Report(event));
        assert_eq!(
            node.kernel
                .cache
                .borrow()
                .order(&client_order_id)
                .unwrap()
                .status(),
            OrderStatus::Filled
        );

        // Replace only cached state, without an observer stamp, to expose the existing grace gate
        node.kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, Some(client_id), true)
            .unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .update_order(&TestOrderEventStubs::submitted(&order, account_id))
            .unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .update_order(&TestOrderEventStubs::accepted(
                &order,
                account_id,
                venue_order_id,
            ))
            .unwrap();
        let should_clear = terminal_report && explicit_client_id;
        assert_eq!(
            node.exec_manager.check_open_order_queries().len(),
            usize::from(should_clear)
        );

        if !should_clear {
            advance_clock(Duration::from_secs(5)).await;
            assert_eq!(node.exec_manager.check_open_order_queries().len(), 1);
        }
    }

    #[rstest]
    #[case(stub_order_event, 1)]
    #[case(stub_submitted_batch_event, 2)]
    #[case(stub_accepted_batch_event, 2)]
    #[case(stub_canceled_batch_event, 2)]
    fn test_pending_events_preserve_roots_when_splitting_batches(
        #[values(false, true)] rooted: bool,
        #[case] make_event: fn() -> ExecutionEvent,
        #[case] orders: usize,
    ) {
        let runner = AsyncRunner::new();
        runner.bind_senders();
        let mut channels = runner.take_channels();

        let send = move || {
            get_data_event_sender().send(stub_data_event()).unwrap();
            get_exec_event_sender().send(stub_exec_event()).unwrap();
            get_exec_event_sender().send(make_event()).unwrap();
        };

        if rooted {
            msgbus::register_trading_command_endpoint(
                MessagingSwitchboard::risk_engine_execute(),
                TypedIntoHandler::from(move |_| send()),
            );
            TradingCommandSender::execute(
                &SyncTradingCommandSender,
                TradingCommandMessage::new(
                    MessagingSwitchboard::risk_engine_execute(),
                    TradingCommand::QueryAccount(QueryAccount::new(
                        "TRADER-001".into(),
                        None,
                        "SIM-001".into(),
                        UUID4::new(),
                        37.into(),
                        None,
                        None,
                    )),
                ),
            );
            nautilus_common::runner::drain_trading_cmd_queue();
        } else {
            send();
        }

        let mut pending = PendingEvents::default();
        pending
            .data_evts
            .push(channels.data_evt_rx.try_recv().unwrap());
        while let Ok(event) = channels.exec_evt_rx.try_recv() {
            pending.push_exec_event(event);
        }

        assert_eq!(pending.data_evts.len(), 1);
        assert_eq!(pending.exec_reports.len(), 1);
        assert_eq!(pending.order_evts.len(), orders);
        assert_eq!(pending.data_evts[0].is_rooted(), rooted);
        assert_eq!(pending.exec_reports[0].is_rooted(), rooted);
        assert!(
            pending
                .order_evts
                .iter()
                .all(|event| event.is_rooted() == rooted)
        );

        let results = std::thread::spawn(move || {
            let mut results = Vec::new();

            for event in pending.data_evts {
                results.push(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || event.dispatch(drop),
                )));
            }

            for event in pending.exec_reports {
                results.push(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || event.dispatch(drop),
                )));
            }

            for event in pending.order_evts {
                results.push(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || event.dispatch(drop),
                )));
            }

            results
        })
        .join()
        .unwrap();

        assert_eq!(results.len(), orders + 2);

        for result in results {
            if rooted {
                let error = result.unwrap_err();
                assert!(
                    error
                        .downcast_ref::<String>()
                        .unwrap()
                        .contains("command context dispatched outside its owner thread")
                );
            } else {
                result.unwrap();
            }
        }
    }

    fn stub_accepted_batch_event() -> ExecutionEvent {
        ExecutionEvent::OrderAcceptedBatch(OrderAcceptedBatch::new(vec![
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-017"))
                .build(),
            OrderAcceptedSpec::builder()
                .client_order_id(ClientOrderId::from("O-023"))
                .build(),
        ]))
    }

    #[derive(Debug)]
    struct CapturedEgressMessage {
        topic: String,
        payload: Bytes,
    }

    type CapturedEgressMessages = Rc<RefCell<Vec<CapturedEgressMessage>>>;
    type SharedClosed = Rc<Cell<bool>>;

    #[derive(Debug)]
    struct CapturingExternalIngress {
        rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
        closed: SharedClosed,
    }

    impl CapturingExternalIngress {
        fn new(rx: tokio::sync::mpsc::Receiver<BusMessage>, closed: SharedClosed) -> Self {
            Self {
                rx: Some(rx),
                closed,
            }
        }
    }

    impl MessageBusExternalIngress for CapturingExternalIngress {
        fn is_closed(&self) -> bool {
            self.closed.get()
        }

        fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
            self.rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("external ingress receiver already taken"))
        }

        fn close(&mut self) {
            self.closed.set(true);
        }
    }

    #[derive(Debug)]
    struct FailingExternalIngress {
        closed: SharedClosed,
    }

    impl FailingExternalIngress {
        fn new(closed: SharedClosed) -> Self {
            Self { closed }
        }
    }

    impl MessageBusExternalIngress for FailingExternalIngress {
        fn is_closed(&self) -> bool {
            self.closed.get()
        }

        fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
            anyhow::bail!("external ingress receiver unavailable")
        }

        fn close(&mut self) {
            self.closed.set(true);
        }
    }

    struct CapturingExternalEgress {
        publications: CapturedEgressMessages,
        closed: SharedClosed,
    }

    impl CapturingExternalEgress {
        fn new() -> (Self, CapturedEgressMessages, SharedClosed) {
            let publications = Rc::new(RefCell::new(Vec::new()));
            let closed = Rc::new(Cell::new(false));
            (
                Self {
                    publications: publications.clone(),
                    closed: closed.clone(),
                },
                publications,
                closed,
            )
        }
    }

    impl MessageBusExternalEgress for CapturingExternalEgress {
        fn is_closed(&self) -> bool {
            self.closed.get()
        }

        fn publish(&self, message: BusMessage) {
            self.publications.borrow_mut().push(CapturedEgressMessage {
                topic: message.topic.to_string(),
                payload: message.payload,
            });
        }

        fn close(&mut self) {
            self.closed.set(true);
        }
    }

    struct CapturingBackingFactory {
        publications: Arc<Mutex<Vec<CapturedEgressMessage>>>,
        closed: Arc<AtomicBool>,
        rx: Mutex<Option<tokio::sync::mpsc::Receiver<BusMessage>>>,
    }

    impl CapturingBackingFactory {
        fn new(
            publications: Arc<Mutex<Vec<CapturedEgressMessage>>>,
            closed: Arc<AtomicBool>,
            rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
        ) -> Self {
            Self {
                publications,
                closed,
                rx: Mutex::new(rx),
            }
        }
    }

    impl Debug for CapturingBackingFactory {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct(stringify!(CapturingBackingFactory))
                .finish_non_exhaustive()
        }
    }

    impl MessageBusBackingFactory for CapturingBackingFactory {
        fn create(
            &self,
            _trader_id: TraderId,
            _instance_id: UUID4,
            _config: MessageBusConfig,
        ) -> anyhow::Result<Box<dyn MessageBusBacking>> {
            let rx = self.rx.lock().take();
            Ok(Box::new(CapturingBacking {
                publications: self.publications.clone(),
                closed: self.closed.clone(),
                rx,
            }))
        }
    }

    struct CapturingBacking {
        publications: Arc<Mutex<Vec<CapturedEgressMessage>>>,
        closed: Arc<AtomicBool>,
        rx: Option<tokio::sync::mpsc::Receiver<BusMessage>>,
    }

    impl MessageBusBacking for CapturingBacking {
        fn is_closed(&self) -> bool {
            self.closed.load(Ordering::Relaxed)
        }

        fn publish(&self, message: BusMessage) {
            self.publications.lock().push(CapturedEgressMessage {
                topic: message.topic.to_string(),
                payload: message.payload,
            });
        }

        fn take_receiver(&mut self) -> anyhow::Result<tokio::sync::mpsc::Receiver<BusMessage>> {
            self.rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("external ingress receiver unavailable"))
        }

        fn close(&mut self) {
            self.closed.store(true, Ordering::Relaxed);
        }
    }
    #[cfg(feature = "dispatch-observer")]
    include!("dispatch_tests.rs");
}
