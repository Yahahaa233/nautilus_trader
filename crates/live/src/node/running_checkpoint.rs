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

//! Node-owned synchronous capture at an actual completed running dispatch root.

use std::{any::Any, collections::BTreeMap, fmt::Debug, rc::Rc, time::Duration};

use anyhow::{Context, Result, ensure};
use nautilus_common::{cache::Cache, live::dst};
use nautilus_system::trader::{CollectedComponentState, Trader};

use super::{LiveNode, NodeState, recovery_quiescence};
use crate::{
    dispatch::DispatchCompletionProof,
    runner::RunningReceivers,
    runner_recovery::{RunnerPendingSnapshot, RunnerRecoveryCodecRegistry},
};

/// Explicit capture cadence; no implicit disk-write interval is installed.
#[derive(Clone, Copy, Debug)]
pub enum RunningCheckpointSchedule {
    /// Collect only after a handle explicitly requests the next completed root.
    Requested,
    /// Check this elapsed interval after completed roots, also accepting requests.
    Interval(Duration),
    /// Useful for callers intentionally persisting every completed root.
    EveryCompletedRoot,
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use nautilus_common::{enums::Environment, messages::DataEvent, timer::TimeEventCallback};
    use nautilus_core::DurationNanos;
    use nautilus_model::{
        identifiers::TraderId,
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
    };
    use rstest::rstest;

    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{NodeDispatchObserver, NodeRunMode},
    };

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test(flavor = "current_thread")]
    async fn actual_hosted_running_root_captures_active_timer_and_fences_failed_writer(
        #[case] fail_writer: bool,
    ) {
        let mut node = LiveNode::builder(
            TraderId::from("RUNNING-CHECKPOINT-TEST"),
            Environment::Sandbox,
        )
        .unwrap()
        .with_reconciliation(false)
        .with_delay_shutdown_secs(0)
        .with_delay_post_stop_secs(0)
        .build()
        .unwrap();
        let observer = DispatchObserver::new("running-checkpoint".into(), |_| Ok(())).unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(observer, |source, phase, _| {
            Ok(DispatchInput {
                source,
                phase: phase.into(),
                payload: serde_json::Value::Null,
                batch_index: None,
            })
        }))
        .unwrap();
        let clock = node.kernel.clock.clone();
        let start = clock.borrow().timestamp_ns();
        clock
            .borrow_mut()
            .set_timer_ns(
                "checkpoint-owned-timer",
                DurationNanos::new(5_000_000_000),
                Some(start),
                None,
                Some(TimeEventCallback::RustLocal(Rc::new(|_| {}))),
                Some(false),
                Some(false),
            )
            .unwrap();
        let writes = Rc::new(Cell::new(0));
        let written = writes.clone();
        let fences = Rc::new(Cell::new(0));
        let fenced = fences.clone();
        node.set_running_checkpoint_handler(
            Rc::new(
                RunnerRecoveryCodecRegistry::new(std::iter::empty())
                    .seal()
                    .unwrap(),
            ),
            RunningCheckpointSchedule::Requested,
            |boundary| {
                boundary.completion_proof().verify()?;
                ensure!(
                    boundary.completion_proof().root_sequence() > 0,
                    "root missing"
                );
                let inventory = serde_json::to_value(boundary.inventory())?;
                ensure!(
                    inventory["node_state"] == "running",
                    "not an actual Running boundary"
                );
                ensure!(
                    inventory["timer_counts"]["kernel"].as_u64() == Some(1),
                    "actual active timer not captured"
                );
                ensure!(
                    inventory["timers"]["kernel"]["timers"][0]["name"] == "checkpoint-owned-timer",
                    "native schedule missing"
                );
                Ok(boundary.completion_proof().root_sequence())
            },
            move |_| {
                written.set(written.get() + 1);
                if fail_writer {
                    anyhow::bail!("durable writer injected failure")
                }
                Ok(())
            },
            move |_| fenced.set(fenced.get() + 1),
        )
        .unwrap();
        let handle = node.handle();
        let driving_handle = handle.clone();
        let driver = async {
            while !driving_handle.is_running() {
                tokio::task::yield_now().await;
            }
            assert_eq!(
                writes.get(),
                0,
                "startup must not manufacture a completed root"
            );
            driving_handle.request_running_checkpoint();
            nautilus_common::live::runner::get_data_event_sender()
                .send(DataEvent::Instrument(InstrumentAny::CryptoPerpetual(
                    crypto_perpetual_ethusdt(),
                )))
                .unwrap();
            if !fail_writer {
                while writes.get() == 0 {
                    tokio::task::yield_now().await;
                }
                driving_handle.stop();
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(node.run_with_mode(NodeRunMode::Hosted), driver)
        })
        .await
        .unwrap_or_else(|_| panic!("running boundary timed out: state={:?}, stop={}, metrics={:?}, coverage={:?}, request={}", node.state(), handle.should_stop(), handle.metrics_snapshot(), node.dispatch_observer.as_ref().unwrap().coverage(), handle.checkpoint_requested()));
        assert_eq!(writes.get(), 1);
        assert_eq!(fences.get(), u32::from(fail_writer));
        assert_eq!(node.state(), NodeState::Stopped);
        assert_eq!(result.is_err(), fail_writer);
        if fail_writer {
            assert!(node.kernel.exec_engine.borrow().submissions_fenced());
        }
    }
}

/// Actual inventory collected by the native node while its guards are held.
/// It is evidence of the stated profile, never recovery/execution permission.
#[derive(Debug, serde::Serialize)]
pub struct RunningCheckpointInventory {
    profile: &'static str,
    node_state: &'static str,
    runner_counts: BTreeMap<String, u64>,
    timer_counts: BTreeMap<String, u64>,
    timers: BTreeMap<String, serde_json::Value>,
    synchronous_queue_counts: BTreeMap<String, u64>,
    adapters: BTreeMap<String, serde_json::Value>,
    execution_manager: serde_json::Value,
    dispatch_coverage: serde_json::Value,
    startup_reconciliation: Option<super::StartupReconciliationObservation>,
    node_instance_id: nautilus_core::UUID4,
    captured_at_ns: u64,
    recovery_frontier: Option<crate::runner_recovery::RunnerRecoveryWatermark>,
    empty_bootstrap: Option<super::EmptyBootstrapRecoveryReceipt>,
    message_bus_mode: &'static str,
    execution_authorized: bool,
}

impl RunningCheckpointInventory {
    #[must_use]
    pub const fn startup_reconciliation(&self) -> Option<&super::StartupReconciliationObservation> {
        self.startup_reconciliation.as_ref()
    }
    #[must_use]
    pub const fn node_instance_id(&self) -> nautilus_core::UUID4 {
        self.node_instance_id
    }
    #[must_use]
    pub const fn captured_at_ns(&self) -> u64 {
        self.captured_at_ns
    }
    #[must_use]
    pub fn recovery_frontier(&self) -> Option<&crate::runner_recovery::RunnerRecoveryWatermark> {
        self.recovery_frontier.as_ref()
    }
    #[must_use]
    pub const fn empty_bootstrap(&self) -> Option<&super::EmptyBootstrapRecoveryReceipt> {
        self.empty_bootstrap.as_ref()
    }
    #[must_use]
    pub const fn dispatch_coverage(&self) -> &serde_json::Value {
        &self.dispatch_coverage
    }
    #[must_use]
    pub const fn registered_timers(&self) -> &BTreeMap<String, serde_json::Value> {
        &self.timers
    }
}

/// Sealed borrowed boundary. Applications cannot construct or retain this value.
/// Its proof, seven queues, cache and component state share one native freeze.
pub struct RunningCheckpointBoundary<'a> {
    proof: &'a DispatchCompletionProof,
    pending: &'a RunnerPendingSnapshot,
    inventory: &'a RunningCheckpointInventory,
    cache: &'a Cache,
    components: &'a CollectedComponentState,
    verify_frozen: &'a dyn Fn() -> Result<()>,
}

impl Debug for RunningCheckpointBoundary<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningCheckpointBoundary")
            .field("proof", &self.proof)
            .field("pending", &self.pending)
            .field("inventory", &self.inventory)
            .finish_non_exhaustive()
    }
}

impl RunningCheckpointBoundary<'_> {
    /// Rechecks actual native registrations, adapters, timers, queues and same
    /// completed root while this borrowed boundary's freeze remains held.
    ///
    /// # Errors
    /// Refuses any changed or failed part of the actual native boundary.
    pub fn verify(&self) -> Result<()> {
        (self.verify_frozen)()
    }

    #[must_use]
    pub const fn completion_proof(&self) -> &DispatchCompletionProof {
        self.proof
    }
    #[must_use]
    pub const fn pending(&self) -> &RunnerPendingSnapshot {
        self.pending
    }
    #[must_use]
    pub const fn inventory(&self) -> &RunningCheckpointInventory {
        self.inventory
    }
    #[must_use]
    pub const fn cache(&self) -> &Cache {
        self.cache
    }
    #[must_use]
    pub const fn components(&self) -> &CollectedComponentState {
        self.components
    }
}

pub(super) type Collect = dyn Fn(&RunningCheckpointBoundary<'_>) -> Result<Box<dyn Any>>;
pub(super) type Persist = dyn Fn(&RunningCheckpointBoundary<'_>, Box<dyn Any>) -> Result<()>;
pub(super) type Fence = dyn Fn(&str);

pub(super) struct RunningCheckpointRegistration {
    pub(super) registry: Rc<RunnerRecoveryCodecRegistry>,
    pub(super) schedule: RunningCheckpointSchedule,
    pub(super) collect: Rc<Collect>,
    pub(super) persist: Rc<Persist>,
    pub(super) fence: Rc<Fence>,
    pub(super) last_root: u64,
    pub(super) last_request: u64,
    pub(super) last_capture: dst::time::Instant,
}

impl Debug for RunningCheckpointRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningCheckpointRegistration")
            .field("schedule", &self.schedule)
            .field("last_root", &self.last_root)
            .finish_non_exhaustive()
    }
}

impl LiveNode {
    /// Registers a node-thread collector and durable writer before running.
    /// The node verifies all guards between collection and persistence and again
    /// afterwards. A failure fences native submissions and caller-owned fences
    /// before requesting shutdown; an already written bundle is not acknowledged.
    ///
    /// # Errors
    /// Refuses late/duplicate registration, unsealed codecs or a zero interval.
    pub fn set_running_checkpoint_handler<T: 'static>(
        &mut self,
        registry: Rc<RunnerRecoveryCodecRegistry>,
        schedule: RunningCheckpointSchedule,
        collect: impl Fn(&RunningCheckpointBoundary<'_>) -> Result<T> + 'static,
        persist: impl Fn(T) -> Result<()> + 'static,
        fence: impl Fn(&str) + 'static,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "checkpoint registration requires idle node"
        );
        ensure!(
            self.running_checkpoint.is_none(),
            "checkpoint handler already registered"
        );
        ensure!(
            self.dispatch_observer.is_some(),
            "checkpoint requires dispatch observer"
        );
        ensure!(
            registry.is_sealed(),
            "checkpoint requires sealed runner codecs"
        );
        if let RunningCheckpointSchedule::Interval(interval) = schedule {
            ensure!(
                !interval.is_zero(),
                "checkpoint interval must be explicit and positive"
            );
        }
        self.running_checkpoint = Some(RunningCheckpointRegistration {
            registry,
            schedule,
            collect: Rc::new(move |boundary| Ok(Box::new(collect(boundary)?) as Box<dyn Any>)),
            persist: Rc::new(move |_, value| {
                let value = value
                    .downcast::<T>()
                    .map_err(|_| anyhow::anyhow!("checkpoint payload type changed"))?;
                persist(*value)
            }),
            fence: Rc::new(fence),
            last_root: 0,
            last_request: 0,
            last_capture: dst::time::Instant::now(),
        });
        Ok(())
    }

    pub(super) fn checkpoint_after_completed_root(
        &mut self,
        mut receivers: RunningReceivers<'_>,
        pending_report_tasks: bool,
    ) -> Result<()> {
        let Some(registration) = self.running_checkpoint.as_ref() else {
            return Ok(());
        };
        if !matches!(self.state(), NodeState::Running | NodeState::Observing)
            || self.handle.should_stop()
        {
            return Ok(());
        }
        let observer = self
            .dispatch_observer
            .as_ref()
            .context("checkpoint observer missing")?
            .clone();
        let Some(proof) = observer.completed_root_boundary_proof()? else {
            return Ok(());
        };
        if proof.root_sequence() <= registration.last_root {
            return Ok(());
        }
        let now = dst::time::Instant::now();
        let request_sequence = self.handle.checkpoint_requested();
        let due = request_sequence > registration.last_request
            || match registration.schedule {
                RunningCheckpointSchedule::Requested => false,
                RunningCheckpointSchedule::Interval(interval) => {
                    now.duration_since(registration.last_capture) >= interval
                }
                RunningCheckpointSchedule::EveryCompletedRoot => true,
            };
        // Even when not due, a root is considered once. Timer/maintenance ticks
        // cannot invent a completed dispatch or reuse its completion frontier.
        self.running_checkpoint.as_mut().unwrap().last_root = proof.root_sequence();
        if !due {
            return Ok(());
        }
        let registration = self.running_checkpoint.as_ref().unwrap();
        let (registry, collect, persist, fence) = (
            registration.registry.clone(),
            registration.collect.clone(),
            registration.persist.clone(),
            registration.fence.clone(),
        );
        let ingress = self.handle.ingress_gate();
        let capture_state = self.state();
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            ensure!(
                !pending_report_tasks,
                "venue reconciliation HTTP is in flight"
            );
            ensure!(
                self.external_msgbus.is_none(),
                "external message-bus inventory unsupported"
            );
            ensure!(
                self.stream_processors.is_empty(),
                "stream processor private state unsupported"
            );
            ensure!(
                self.recovery_dispatch_queue.is_empty(),
                "recovery callbacks remain queued"
            );
            ensure!(
                self.config
                    .data_engine
                    .external_clients
                    .as_ref()
                    .is_none_or(Vec::is_empty),
                "external data client inventory unsupported"
            );
            let data = self.kernel.data_engine.try_borrow()?;
            let execution = self.kernel.exec_engine.try_borrow()?;
            let risk = self.kernel.risk_engine.try_borrow()?;
            let _portfolio = self.kernel.portfolio.try_borrow()?;
            ensure!(
                execution.get_external_client_ids().is_empty(),
                "external execution inventory unsupported"
            );
            ensure!(
                self.kernel
                    .trader
                    .try_borrow()?
                    .exec_algorithm_ids()
                    .is_empty(),
                "execution algorithm private state unsupported"
            );
            let mut adapters = Vec::new();
            for client in data.get_clients() {
                let client = client.get_client();
                adapters.push((
                    format!("data:{}", client.client_id()),
                    client.freeze_running_checkpoint()?,
                ));
            }
            for client in execution.get_all_clients() {
                adapters.push((
                    format!("execution:{}", client.client_id()),
                    client.freeze_running_checkpoint()?,
                ));
            }
            // Adapter admission is frozen first, so incoming venue messages
            // remain in their owned input queues rather than hitting a closed runner.
            recovery_quiescence::with_running_registered_timer_inventory(
                &self.kernel.clock,
                &self.kernel.trader,
                |timers, timer_state, verify_timers, _| {
                    let guard = ingress.freeze()?;
                    let coverage = observer.coverage()?;
                    proof.verify()?;
                    let cache_rc = self.kernel.cache();
                    let cache = cache_rc.try_borrow()?;
                    // Cache containers contain separately mutable order/position/account
                    // cells. Holding the outer cache alone does not freeze those cells.
                    let _orders = cache.orders_refs(None, None, None, None, None);
                    let _positions = cache.positions_refs(None, None, None, None, None);
                    let accounts = cache.accounts_all_owned();
                    let _accounts = accounts
                        .iter()
                        .map(|account| {
                            cache
                                .account_ref(&account.id())
                                .context("account disappeared during checkpoint")
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let components = Trader::collect_component_state(&self.kernel.trader)?;
                    let manager = self.exec_manager.checkpoint_inventory(now)?;
                    let engine_counts = (
                        execution.command_count(),
                        execution.event_count(),
                        execution.report_count(),
                        execution.submissions_fenced(),
                        risk.trading_state(),
                    );
                    nautilus_common::msgbus::with_local_only_recovery_inventory(|bus| {
                        nautilus_common::runner::with_empty_sync_command_queues(|| {
                            let pending = receivers.snapshot(&ingress, &guard, &registry)?;
                            let mut runner_counts = [
                                "time_event",
                                "system_event",
                                "system_command",
                                "execution_event",
                                "execution_command",
                                "data_event",
                                "data_command",
                            ]
                            .into_iter()
                            .map(|name| (name.to_owned(), 0))
                            .collect::<BTreeMap<_, u64>>();
                            for entry in &pending.entries {
                                let name = serde_json::to_value(entry.channel)?
                                    .as_str()
                                    .context("invalid runner channel")?
                                    .to_owned();
                                *runner_counts.entry(name).or_insert(0) += 1;
                            }
                            let inventory = RunningCheckpointInventory {
                                profile: "running_completed_root_local_bus_registered_live_timers.v1",
                                node_state: if capture_state == NodeState::Observing {
                                    "observing"
                                } else {
                                    "running"
                                },
                                runner_counts,
                                timer_counts: timers.clone(),
                                timers: timer_state.clone(),
                                synchronous_queue_counts: BTreeMap::from([
                                    ("data_command".into(), 0),
                                    ("trading_command".into(), 0),
                                ]),
                                adapters: adapters
                                    .iter()
                                    .map(|(id, guard)| (id.clone(), guard.inventory().clone()))
                                    .collect(),
                                execution_manager: manager.clone(),
                                dispatch_coverage: coverage.clone(),
                                startup_reconciliation: self.handle.startup_reconciliation(),
                                node_instance_id: self.kernel.instance_id,
                                captured_at_ns: self
                                    .kernel
                                    .clock
                                    .try_borrow()?
                                    .timestamp_ns()
                                    .as_u64(),
                                recovery_frontier: self.recovery_native_frontier.clone(),
                                empty_bootstrap: self.recovery_empty_bootstrap.clone(),
                                message_bus_mode: "local_only",
                                execution_authorized: false,
                            };
                            let verify = || -> Result<()> {
                                ensure!(
                                    self.state() == capture_state && !self.handle.should_stop(),
                                    "node lifecycle changed during checkpoint"
                                );
                                verify_timers()?;
                                guard.verify()?;
                                bus.verify()?;
                                proof.verify()?;
                                ensure!(
                                    observer.coverage()? == coverage,
                                    "dispatch inventory changed during checkpoint"
                                );
                                ensure!(
                                    Trader::collect_component_state(&self.kernel.trader)?
                                        == components,
                                    "component state changed during checkpoint"
                                );
                                ensure!(
                                    self.exec_manager.checkpoint_inventory(now)? == manager,
                                    "reconciliation state changed during checkpoint"
                                );
                                ensure!(
                                    (
                                        execution.command_count(),
                                        execution.event_count(),
                                        execution.report_count(),
                                        execution.submissions_fenced(),
                                        risk.trading_state()
                                    ) == engine_counts,
                                    "native execution/risk state changed during checkpoint"
                                );
                                for (_, adapter) in &adapters {
                                    adapter.verify()?;
                                }
                                Ok(())
                            };
                            verify()?;
                            let boundary = RunningCheckpointBoundary {
                                proof: &proof,
                                pending: &pending,
                                inventory: &inventory,
                                cache: &cache,
                                components: &components,
                                verify_frozen: &verify,
                            };
                            let value = collect(&boundary)?;
                            verify()?;
                            persist(&boundary, value)?;
                            verify()?;
                            guard.finish()?;
                            Ok(())
                        })
                    })
                },
            )?;
            // Reopen adapters only after the runner is ready. No await occurs
            // anywhere in the node-thread collect/persist/revalidation interval.
            for (_, adapter) in adapters {
                adapter.finish()?;
            }
            Ok(())
        }));
        match outcome {
            Ok(Ok(())) => {
                let registration = self.running_checkpoint.as_mut().unwrap();
                registration.last_capture = now;
                registration.last_request = request_sequence;
                Ok(())
            }
            outcome => {
                // Failure is monotonic, including codec, persistence and caught
                // callback panic. Do not continue on a possibly written checkpoint.
                self.kernel.exec_engine.borrow().fence_submissions();
                ingress.invalidate();
                let error = match outcome {
                    Ok(Err(error)) => error,
                    Err(_) => anyhow::anyhow!("running checkpoint callback panicked"),
                    Ok(Ok(())) => unreachable!(),
                };
                let reason = format!("{error:#}");
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fence(&reason)));
                self.handle.stop();
                Err(error)
            }
        }
    }
}
