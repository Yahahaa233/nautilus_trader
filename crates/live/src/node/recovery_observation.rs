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

//! Native input-only observation of the same recovered node and sealed startup.

use std::{any::Any, cell::Cell, fmt::Debug, rc::Rc};

use anyhow::{Context, Result, ensure};
use nautilus_common::{actor::recovery_observation::RecoveryObservationAdmission, live::dst};
use nautilus_execution::engine::RecoveryObservationExecutionGate;
use nautilus_model::{enums::TradingState, identifiers::ActorId};

use super::{
    LiveNode, NodeState, RunningCheckpointBoundary, RunningCheckpointSchedule,
    StartupReconciliationPhase, running_checkpoint::RunningCheckpointRegistration,
    state::RunningTransition,
};
use crate::{runner::RunningReceivers, runner_recovery::RunnerRecoveryCodecRegistry};

/// Borrowed actual native boundary. It is neither constructed nor deserialized by
/// the application. The current-root proof does not certify historical coverage.
#[derive(Debug)]
pub struct RecoveryObservationBoundary<'a> {
    native: &'a RunningCheckpointBoundary<'a>,
    observation_started_at_ns: u64,
    observer_ids: &'a [ActorId],
}

impl RecoveryObservationBoundary<'_> {
    #[must_use]
    pub const fn native(&self) -> &RunningCheckpointBoundary<'_> {
        self.native
    }
    #[must_use]
    pub const fn observation_started_at_ns(&self) -> u64 {
        self.observation_started_at_ns
    }
    #[must_use]
    pub const fn observer_ids(&self) -> &[ActorId] {
        self.observer_ids
    }
}

/// Before-trader-start handoff while the same native input freeze and cache
/// borrows remain held. Only caller-owned, privately reconciled position release
/// is allowed here; native component history must still remain unchanged.
#[derive(Debug)]
pub struct RecoveryStartupBoundary<'a> {
    observation: &'a RecoveryObservationBoundary<'a>,
}

impl RecoveryStartupBoundary<'_> {
    #[must_use]
    pub const fn observation(&self) -> &RecoveryObservationBoundary<'_> {
        self.observation
    }
}

/// Sealed final startup review. During this callback all registered actor clock
/// read views are suspended and actual current time is visible. Producer gates,
/// native cache cells, component registrations and execution admission stay held.
/// This lifecycle boundary is not a new completed-root checkpoint or a grant.
#[derive(Debug)]
pub struct RecoveryReleaseBoundary<'a> {
    cache: &'a nautilus_common::cache::Cache,
    components: &'a nautilus_system::trader::CollectedComponentState,
    node_instance_id: nautilus_core::UUID4,
    recovery_frontier: &'a Option<crate::runner_recovery::RunnerRecoveryWatermark>,
    empty_bootstrap: &'a Option<super::EmptyBootstrapRecoveryReceipt>,
}
impl RecoveryReleaseBoundary<'_> {
    #[must_use]
    pub fn actual_now_ns(&self) -> u64 {
        nautilus_core::time::get_atomic_clock_realtime()
            .get_time_ns()
            .as_u64()
    }
    #[must_use]
    pub const fn cache(&self) -> &nautilus_common::cache::Cache {
        self.cache
    }
    #[must_use]
    pub const fn components(&self) -> &nautilus_system::trader::CollectedComponentState {
        self.components
    }
    #[must_use]
    pub const fn node_instance_id(&self) -> nautilus_core::UUID4 {
        self.node_instance_id
    }
    #[must_use]
    pub fn recovery_frontier(&self) -> Option<&crate::runner_recovery::RunnerRecoveryWatermark> {
        self.recovery_frontier.as_ref()
    }
    #[must_use]
    pub const fn empty_bootstrap(&self) -> Option<&super::EmptyBootstrapRecoveryReceipt> {
        self.empty_bootstrap.as_ref()
    }
}

type Round = dyn Fn() -> Result<()>;
type Attest = dyn Fn(&RecoveryObservationBoundary<'_>) -> Result<Option<Box<dyn Any>>>;
type Arm = dyn Fn(&RecoveryObservationBoundary<'_>, Box<dyn Any>) -> Result<Box<dyn Any>>;
type BeforeStart = dyn Fn(&RecoveryStartupBoundary<'_>, Box<dyn Any>) -> Result<()>;

pub(super) struct RecoveryObservationRegistration {
    observer_ids: Vec<ActorId>,
    registry: Rc<RunnerRecoveryCodecRegistry>,
    schedule: RunningCheckpointSchedule,
    begin: Rc<Round>,
    observe_round: Rc<Round>,
    attest: Rc<Attest>,
    arm: Rc<Arm>,
    before_start: Rc<BeforeStart>,
    validate_release: Rc<dyn Fn(&RecoveryReleaseBoundary<'_>) -> Result<()>>,
    fence: Rc<dyn Fn(&str)>,
    admission: Option<RecoveryObservationAdmission>,
    execution_gate: Option<RecoveryObservationExecutionGate>,
    suspended_checkpoint: Option<RunningCheckpointRegistration>,
    ready: Rc<Cell<bool>>,
}

impl Debug for RecoveryObservationRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoveryObservationRegistration")
            .field("observer_ids", &self.observer_ids)
            .field("schedule", &self.schedule)
            .field("ready", &self.ready.get())
            .finish_non_exhaustive()
    }
}

impl LiveNode {
    /// Registers actual observer actors and private proof handoff before run.
    /// No callbacks or execution permission are created by registration.
    ///
    /// `observe_round` updates actual risk observations before freezing. `attest`
    /// returns None while fresh facts are missing. `arm` consumes a private
    /// application receipt and may only arm transient component startup branches.
    /// `before_trader_start` may perform a privately reconciled caller-owned
    /// position release while native cache/component state remains immutable.
    /// Ordinary restored actor/strategy on_start runs once afterwards under the
    /// native execution phase gate. All errors permanently fence execution.
    ///
    /// # Errors
    /// Refuses incomplete native recovery, strategies declared as observers,
    /// duplicates, unknown actors, unsupported ingress, unsealed codecs and an
    /// implicit/zero interval. No disconnected or flat-only fallback is installed.
    #[allow(
        clippy::too_many_arguments,
        reason = "each callback marks a distinct verified lifecycle boundary"
    )]
    pub fn set_recovery_observation_handler<A: 'static, S: 'static>(
        &mut self,
        observer_ids: Vec<ActorId>,
        registry: Rc<RunnerRecoveryCodecRegistry>,
        schedule: RunningCheckpointSchedule,
        begin: impl Fn() -> Result<()> + 'static,
        observe_round: impl Fn() -> Result<()> + 'static,
        attest: impl Fn(&RecoveryObservationBoundary<'_>) -> Result<Option<A>> + 'static,
        arm: impl Fn(&RecoveryObservationBoundary<'_>, A) -> Result<S> + 'static,
        before_trader_start: impl Fn(&RecoveryStartupBoundary<'_>, S) -> Result<()> + 'static,
        validate_release: impl Fn(&RecoveryReleaseBoundary<'_>) -> Result<()> + 'static,
        fence: impl Fn(&str) + 'static,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "observation registration requires idle node"
        );
        ensure!(
            self.recovery_observation.is_none(),
            "observation already registered"
        );
        self.verify_recovery_observation_source()?;
        ensure!(
            registry.is_sealed(),
            "observation requires sealed actual runner codecs"
        );
        if let RunningCheckpointSchedule::Interval(interval) = schedule {
            ensure!(
                !interval.is_zero(),
                "observation interval must be explicit and positive"
            );
        }
        let trader = self.kernel.trader.try_borrow()?;
        let registered = trader.actor_ids();
        let ids = observer_ids
            .iter()
            .map(ToString::to_string)
            .collect::<std::collections::BTreeSet<_>>();
        ensure!(
            !ids.is_empty() && ids.len() == observer_ids.len(),
            "observation IDs empty or duplicated"
        );
        ensure!(
            observer_ids.iter().all(|id| registered.contains(id)),
            "observer is not an actual registered actor"
        );
        ensure!(
            trader
                .strategy_ids()
                .iter()
                .all(|id| !ids.contains(&id.to_string())),
            "strategy cannot be an observation actor"
        );
        drop(trader);
        self.recovery_observation = Some(RecoveryObservationRegistration {
            observer_ids,
            registry,
            schedule,
            begin: Rc::new(begin),
            observe_round: Rc::new(observe_round),
            attest: Rc::new(move |boundary| {
                Ok(attest(boundary)?.map(|value| Box::new(value) as Box<dyn Any>))
            }),
            arm: Rc::new(move |boundary, value| {
                let value = value
                    .downcast::<A>()
                    .map_err(|_| anyhow::anyhow!("observation receipt type changed"))?;
                Ok(Box::new(arm(boundary, *value)?) as Box<dyn Any>)
            }),
            before_start: Rc::new(move |boundary, value| {
                let value = value
                    .downcast::<S>()
                    .map_err(|_| anyhow::anyhow!("startup receipt type changed"))?;
                before_trader_start(boundary, *value)
            }),
            validate_release: Rc::new(validate_release),
            fence: Rc::new(fence),
            admission: None,
            execution_gate: None,
            suspended_checkpoint: None,
            ready: Rc::new(Cell::new(false)),
        });
        Ok(())
    }

    pub(super) fn verify_recovery_observation_source(&self) -> Result<()> {
        ensure!(
            self.recovery_requires_release
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && (self.recovery_native_frontier.is_some()
                    || self.recovery_empty_bootstrap.is_some())
                && self.recovery_dispatch_queue.is_empty(),
            "observation requires completed same-node recovery handoffs"
        );
        ensure!(
            self.dispatch_observer.is_some()
                && self.external_msgbus.is_none()
                && self.stream_processors.is_empty(),
            "observation source ingress unsupported"
        );
        ensure!(
            !self.kernel.load_state()
                && !self.config.exec_engine.load_cache
                && self
                    .config
                    .cache
                    .as_ref()
                    .is_none_or(|config| !config.flush_on_start),
            "observation must not reload a second database state over recovery"
        );
        ensure!(
            self.kernel.risk_engine.try_borrow()?.trading_state() == TradingState::Halted,
            "recovery observation requires native risk Halted"
        );
        ensure!(
            !self.kernel.exec_engine.try_borrow()?.submissions_fenced(),
            "permanent native failure fence cannot be released by observation"
        );
        if self.recovery_native_frontier.is_some() {
            let clients = self.kernel.data_engine.try_borrow()?.get_clients().len()
                + self
                    .kernel
                    .exec_engine
                    .try_borrow()?
                    .get_all_clients()
                    .len();
            ensure!(
                clients == 0 || self.recovery_adapter_source.is_some(),
                "restored actual clients require source adapter and native subscription inventory"
            );
        }
        ensure!(
            self.kernel
                .trader
                .try_borrow()?
                .exec_algorithm_ids()
                .is_empty(),
            "observation execution algorithm lifecycle unsupported"
        );
        Ok(())
    }

    pub(super) fn install_recovery_observation_admission(&mut self) -> Result<()> {
        self.verify_recovery_observation_source()?;
        let registration = self
            .recovery_observation
            .as_mut()
            .context("observation registration missing")?;
        let ids = registration
            .observer_ids
            .iter()
            .map(|id| id.inner())
            .collect::<Vec<_>>();
        registration.admission = Some(RecoveryObservationAdmission::enter(&ids)?);
        registration.execution_gate = Some(
            self.kernel
                .exec_engine
                .try_borrow()?
                .begin_recovery_observation()?,
        );
        Ok(())
    }

    pub(super) fn begin_recovery_input_observation(&mut self) -> Result<()> {
        ensure!(
            self.handle
                .startup_reconciliation()
                .is_some_and(|value| value.phase == StartupReconciliationPhase::Processed),
            "observation requires actual processed startup reconciliation"
        );
        let mut registration = self
            .recovery_observation
            .take()
            .context("observation registration missing")?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            registration
                .admission
                .as_ref()
                .context("observation admission missing")?
                .verify()?;
            registration
                .execution_gate
                .as_ref()
                .context("observation execution gate missing")?
                .verify()?;
            let started_at_ns = self.kernel.clock.try_borrow()?.timestamp_ns().as_u64();
            if let Some(timers) = self.recovery_timers.as_mut() {
                timers.resume_observers(&registration.observer_ids)?;
            }
            (registration.begin)()?;
            ensure!(
                matches!(self.handle.try_set_observing(), RunningTransition::Entered),
                "observation startup lifecycle changed"
            );
            let ids = registration.observer_ids.clone();
            let (attest, arm, before_start, ready) = (
                registration.attest.clone(),
                registration.arm.clone(),
                registration.before_start.clone(),
                registration.ready.clone(),
            );
            let collect_ids = ids.clone();
            registration.suspended_checkpoint = self.running_checkpoint.take();
            self.running_checkpoint = Some(RunningCheckpointRegistration {
                registry: registration.registry.clone(),
                schedule: registration.schedule,
                collect: Rc::new(move |native| {
                    let boundary = RecoveryObservationBoundary {
                        native,
                        observation_started_at_ns: started_at_ns,
                        observer_ids: &collect_ids,
                    };
                    Ok(Box::new(attest(&boundary)?) as Box<dyn Any>)
                }),
                persist: Rc::new(move |native, value| {
                    let value = value
                        .downcast::<Option<Box<dyn Any>>>()
                        .map_err(|_| anyhow::anyhow!("observation attestation type changed"))?;
                    let Some(value) = *value else {
                        return Ok(());
                    };
                    let observation = RecoveryObservationBoundary {
                        native,
                        observation_started_at_ns: started_at_ns,
                        observer_ids: &ids,
                    };
                    let startup_receipt = arm(&observation, value)?;
                    native.verify()?;
                    before_start(
                        &RecoveryStartupBoundary {
                            observation: &observation,
                        },
                        startup_receipt,
                    )?;
                    native.verify()?;
                    ready.set(true);
                    Ok(())
                }),
                fence: registration.fence.clone(),
                last_root: 0,
                last_request: 0,
                last_capture: dst::time::Instant::now(),
            });
            Ok(())
        }))
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "recovery observation lifecycle callback panicked"
            ))
        });
        self.recovery_observation = Some(registration);
        if let Err(error) = &result {
            self.fail_recovery_observation(&format!("{error:#}"));
        }
        result
    }

    pub(super) fn observation_after_completed_root(
        &mut self,
        mut receivers: RunningReceivers<'_>,
        pending_report_tasks: bool,
    ) -> Result<()> {
        if self.state() != NodeState::Observing {
            return self.checkpoint_after_completed_root(receivers, pending_report_tasks);
        }
        if pending_report_tasks || self.handle.should_stop() {
            return Ok(());
        }
        let Some(proof) = self
            .dispatch_observer
            .as_ref()
            .context("observation dispatch observer missing")?
            .completed_root_boundary_proof()?
        else {
            return Ok(());
        };
        if proof.root_sequence()
            <= self
                .running_checkpoint
                .as_ref()
                .context("observation capture registration missing")?
                .last_root
        {
            return Ok(());
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let registration = self
                .recovery_observation
                .as_ref()
                .context("observation registration missing")?;
            registration
                .admission
                .as_ref()
                .context("observation admission missing")?
                .verify()?;
            registration
                .execution_gate
                .as_ref()
                .context("observation execution gate missing")?
                .verify()?;
            // Risk observation is allowed to update actual peak/day state here,
            // before the immutable component boundary is taken.
            (registration.observe_round)()?;
            ensure!(
                self.kernel.risk_engine.try_borrow()?.trading_state() == TradingState::Halted,
                "observation round changed native risk admission"
            );
            self.checkpoint_after_completed_root(receivers.reborrow(), pending_report_tasks)?;
            if self.recovery_observation.as_ref().unwrap().ready.get() {
                self.complete_recovery_observation_startup(&mut receivers)?;
            }
            Ok(())
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("recovery observation callback panicked")));
        if let Err(error) = &result {
            self.fail_recovery_observation(&format!("{error:#}"));
        }
        result
    }

    fn complete_recovery_observation_startup(
        &mut self,
        receivers: &mut RunningReceivers<'_>,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Observing && !self.handle.should_stop(),
            "recovery startup lifecycle changed"
        );
        let mut registration = self
            .recovery_observation
            .take()
            .context("observation registration missing")?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            registration
                .admission
                .as_ref()
                .context("observation admission missing")?
                .verify()?;
            registration
                .execution_gate
                .as_ref()
                .context("observation execution gate missing")?
                .verify()?;
            // This is synchronous: no input callback is serviced between the
            // checked handoff and restored on_start. All venue writes remain gated.
            let lifecycle = self.native_lifecycle_input("recovery.restored_trader_start")?;
            let lifecycle_guard =
                self.begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &lifecycle)?;
            self.kernel.start_trader_after_recovery_observation()?;
            if let Some(guard) = lifecycle_guard {
                guard.complete()?;
            }
            self.validate_recovery_startup_freshness(&registration)?;
            ensure!(
                !self.handle.should_stop(),
                "recovery startup stop requested"
            );
            registration.execution_gate.as_ref().unwrap().verify()?;
            ensure!(
                matches!(
                    self.handle.try_release_observing(),
                    RunningTransition::Entered
                ),
                "sealed recovery transition failed"
            );
            self.handoff_recovered_timers(receivers)?;
            registration.execution_gate.take().unwrap().finish()?;
            registration.admission.take().unwrap().finish()?;
            self.recovery_requires_release = false;
            self.running_checkpoint = registration.suspended_checkpoint.take();
            Ok(())
        }))
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "recovery observation lifecycle callback panicked"
            ))
        });
        self.recovery_observation = Some(registration);
        result
    }

    fn validate_recovery_startup_freshness(
        &self,
        registration: &RecoveryObservationRegistration,
    ) -> Result<()> {
        let dispatch = self
            .dispatch_observer
            .as_ref()
            .context("release dispatch observer missing")?;
        let generation = dispatch
            .completed_root_boundary_proof()?
            .context("release actual lifecycle completion missing")?;
        generation.verify()?;
        super::recovery_quiescence::with_running_registered_timer_inventory(
            &self.kernel.clock,
            &self.kernel.trader,
            |_, _, verify_timers, with_actual_reads| {
                let cache_rc = self.kernel.cache();
                let cache = cache_rc.try_borrow()?;
                let _orders = cache.orders_refs(None, None, None, None, None);
                let _positions = cache.positions_refs(None, None, None, None, None);
                let accounts = cache.accounts_all_owned();
                let _accounts = accounts
                    .iter()
                    .map(|account| {
                        cache
                            .account_ref(&account.id())
                            .context("release account disappeared")
                    })
                    .collect::<Result<Vec<_>>>()?;
                let components =
                    nautilus_system::trader::Trader::collect_component_state(&self.kernel.trader)?;
                let boundary = RecoveryReleaseBoundary {
                    cache: &cache,
                    components: &components,
                    node_instance_id: self.kernel.instance_id,
                    recovery_frontier: &self.recovery_native_frontier,
                    empty_bootstrap: &self.recovery_empty_bootstrap,
                };
                generation.verify()?;
                // The callback sees actual actor Clock now, even if an earlier
                // durable write took longer than the evidence's valid lifetime.
                with_actual_reads(&|| (registration.validate_release)(&boundary))?;
                verify_timers()?;
                generation.verify()?;
                ensure!(
                    nautilus_system::trader::Trader::collect_component_state(&self.kernel.trader)?
                        == components,
                    "final release validation mutated native component history"
                );
                registration
                    .execution_gate
                    .as_ref()
                    .context("release phase gate missing")?
                    .verify()?;
                registration
                    .admission
                    .as_ref()
                    .context("release callback admission missing")?
                    .verify()
            },
        )
    }

    pub(super) fn verify_observation_timer_admission(
        &self,
        message: &nautilus_common::runner::TimeEventMessage,
    ) -> Result<()> {
        let binding = message.checkpoint_callback_binding();
        ensure!(
            binding["owner_thread_matches"].as_bool() == Some(true)
                && matches!(
                    binding["kind"].as_str(),
                    Some("registered_owner_thread" | "registered_cleanup")
                ),
            "observation timer has no actual native owner binding"
        );
        let binding_id = binding["binding_id"]
            .as_u64()
            .context("timer binding ID missing")?;
        let name = message.event().name.as_str();
        let registration = self
            .recovery_observation
            .as_ref()
            .context("observation registration missing")?;
        super::recovery_quiescence::with_running_registered_timer_inventory(
            &self.kernel.clock,
            &self.kernel.trader,
            |_, inventories, verify, _| {
                let mut owners = Vec::new();
                for (owner, inventory) in inventories {
                    if let Some(timers) = inventory["timers"].as_array() {
                        for timer in timers {
                            if timer["name"].as_str() == Some(name)
                                && timer["binding"]["binding_id"].as_u64() == Some(binding_id)
                            {
                                owners.push(owner);
                            }
                        }
                    }
                }
                ensure!(
                    owners.len() == 1,
                    "timer does not bind exactly one actual registered clock"
                );
                ensure!(
                    registration
                        .observer_ids
                        .iter()
                        .any(|id| owners[0] == &format!("component:{id}")),
                    "non-observer timer callback remains closed"
                );
                verify()
            },
        )
    }

    pub(super) fn fail_recovery_observation(&self, reason: &str) {
        self.kernel.exec_engine.borrow().fence_submissions();
        if let Some(registration) = &self.recovery_observation {
            let fence = registration.fence.clone();
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fence(reason)));
        }
        self.handle.stop();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use indexmap::IndexMap;
    use nautilus_common::{
        actor::data_actor::DataActorConfig,
        actor::{DataActor, DataActorCore, DataActorNative, registry::get_actor_unchecked},
        cache::Cache,
        enums::Environment,
        messages::DataEvent,
        nautilus_actor,
        timer::TimeEventCallback,
    };
    use nautilus_core::{DurationNanos, UnixNanos};
    use nautilus_model::{
        data::{Data, QuoteTick},
        identifiers::TraderId,
        instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
        types::{Price, Quantity},
    };
    use nautilus_system::trader::Trader;
    use rstest::rstest;

    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{NodeDispatchObserver, NodeRunMode},
    };

    #[derive(Debug)]
    struct NativeObserver {
        core: DataActorCore,
        history: Rc<Cell<u64>>,
        latest_quote_ns: Rc<Cell<u64>>,
        armed: Rc<Cell<bool>>,
        starts: Rc<Cell<u64>>,
    }

    impl DataActor for NativeObserver {
        fn on_save(&self) -> Result<IndexMap<String, Vec<u8>>> {
            // Represents production Guardian's actual clock-derived captured_at.
            Ok(IndexMap::from([
                ("history".into(), self.history.get().to_le_bytes().to_vec()),
                (
                    "captured_at_ns".into(),
                    self.clock_rc()
                        .borrow()
                        .timestamp_ns()
                        .as_u64()
                        .to_le_bytes()
                        .to_vec(),
                ),
            ]))
        }
        fn on_load(&mut self, state: IndexMap<String, Vec<u8>>) -> Result<()> {
            self.history
                .set(u64::from_le_bytes(state["history"].as_slice().try_into()?));
            Ok(())
        }
        fn on_start(&mut self) -> Result<()> {
            ensure!(
                self.armed.replace(false),
                "normal restored startup cannot bypass private arm"
            );
            ensure!(
                self.history.get() == 8,
                "restored observer history was reset"
            );
            self.starts.set(self.starts.get() + 1);
            Ok(())
        }
        fn on_quote(&mut self, quote: &QuoteTick) -> Result<()> {
            self.latest_quote_ns.set(quote.ts_init.as_u64());
            self.history.set(self.history.get() + 1);
            Ok(())
        }
    }
    nautilus_actor!(NativeObserver);

    // These receipts cannot be sourced from deserialized readiness JSON.
    struct ObservedReceipt {
        quote_ns: u64,
        history: u64,
    }
    struct StartupReceipt {
        quote_ns: u64,
    }

    #[derive(Debug)]
    struct ActualObservationQueueCodec(crate::runner_recovery::RunnerRecoveryChannel);
    impl crate::runner_recovery::RunnerRecoveryCodec for ActualObservationQueueCodec {
        fn channel(&self) -> crate::runner_recovery::RunnerRecoveryChannel {
            self.0
        }
        fn codec_id(&self) -> &str {
            match self.0 {
                crate::runner_recovery::RunnerRecoveryChannel::DataEvent => {
                    "actual-observation-quote.v1"
                }
                _ => "actual-observation-subscription.v1",
            }
        }
        fn encode(
            &self,
            event: crate::runner_recovery::RunnerRecoveryEventRef<'_>,
        ) -> Result<serde_json::Value> {
            use crate::runner_recovery::RunnerRecoveryEventRef;
            match event {
                RunnerRecoveryEventRef::DataEvent(DataEvent::Data(Data::Quote(quote))) => {
                    Ok(serde_json::to_value(quote)?)
                }
                RunnerRecoveryEventRef::DataCommand(
                    nautilus_common::messages::data::DataCommand::Subscribe(command),
                ) => Ok(serde_json::json!({"Subscribe":command})),
                _ => anyhow::bail!("unsupported actual observation test input"),
            }
        }
        fn decode(
            &self,
            _: &crate::runner_recovery::RunnerRecoveryEnvelope,
        ) -> Result<crate::runner_recovery::RunnerRecoveryEvent> {
            anyhow::bail!("this actual-source test registry is capture-only")
        }
    }

    fn actual_observation_queue_registry() -> RunnerRecoveryCodecRegistry {
        use crate::runner_recovery::RunnerRecoveryChannel;
        let mut registry = RunnerRecoveryCodecRegistry::new([
            RunnerRecoveryChannel::DataEvent,
            RunnerRecoveryChannel::DataCommand,
        ]);
        registry
            .register(ActualObservationQueueCodec(
                RunnerRecoveryChannel::DataEvent,
            ))
            .unwrap();
        registry
            .register(ActualObservationQueueCodec(
                RunnerRecoveryChannel::DataCommand,
            ))
            .unwrap();
        registry.seal().unwrap()
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test(flavor = "current_thread")]
    async fn recovery_observation_actual_input_preserves_history_and_rechecks_expired_facts(
        #[case] expire_during_durable_release: bool,
    ) {
        let actor_id = ActorId::from("OBSERVATION-RISK-ACTOR");
        let mut node = LiveNode::builder(TraderId::from("OBSERVATION-TEST"), Environment::Sandbox)
            .unwrap()
            .with_exec_engine_config(crate::config::LiveExecutionEngineConfig {
                load_cache: false,
                ..Default::default()
            })
            .with_delay_shutdown_secs(0)
            .with_delay_post_stop_secs(0)
            .build()
            .unwrap();
        let history = Rc::new(Cell::new(7));
        let quote_ns = Rc::new(Cell::new(0));
        let armed = Rc::new(Cell::new(false));
        let starts = Rc::new(Cell::new(0));
        node.add_actor(NativeObserver {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(actor_id),
                ..Default::default()
            }),
            history: history.clone(),
            latest_quote_ns: quote_ns.clone(),
            armed: armed.clone(),
            starts: starts.clone(),
        })
        .unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("observation-source".into(), |_| Ok(())).unwrap(),
            |source, phase, _| {
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload: serde_json::Value::Null,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        node.kernel
            .risk_engine
            .borrow_mut()
            .set_trading_state(TradingState::Halted);
        let instrument = audusd_sim();
        let instrument_id = instrument.id();
        let mut cache = Cache::default();
        cache
            .add_instrument(InstrumentAny::CurrencyPair(instrument))
            .unwrap();
        node.restore_native_cache(cache).unwrap();
        let state = Trader::collect_component_state(node.kernel.trader()).unwrap();
        node.restore_component_state(&state).unwrap();
        node.complete_empty_bootstrap_recovery(
            "observation-source",
            1,
            &"a".repeat(64),
            node.kernel.instance_id,
        )
        .unwrap();
        assert!(node.ensure_recovery_start_permitted().is_err());
        let observer_clock = node
            .kernel
            .trader
            .borrow()
            .registered_component_clocks()
            .unwrap()
            .into_iter()
            .find(|(id, _)| id.inner() == actor_id.inner())
            .unwrap()
            .1;
        let deadline_ns = Rc::new(Cell::new(0));
        let released = Rc::new(Cell::new(0));
        let fences = Rc::new(Cell::new(0));
        let attest_quote = quote_ns.clone();
        let attest_history = history.clone();
        let arm_quote = quote_ns.clone();
        let arm_history = history.clone();
        let before_quote = quote_ns.clone();
        let release_count = released.clone();
        let final_deadline = deadline_ns.clone();
        let final_clock = observer_clock.clone();
        let fenced = fences.clone();
        node.set_recovery_observation_handler(
            vec![actor_id],
            Rc::new(actual_observation_queue_registry()),
            RunningCheckpointSchedule::EveryCompletedRoot,
            move || {
                let mut actor = get_actor_unchecked::<NativeObserver>(&actor_id.inner());
                actor.subscribe_quotes(instrument_id, None, None);
                let mut clock = actor.clock_mut();
                let start = clock.timestamp_ns();
                clock.set_timer_ns(
                    "same-name-allowed-per-owner",
                    DurationNanos::new(1_000_000_000),
                    Some(start),
                    None,
                    Some(TimeEventCallback::RustLocal(Rc::new(|_| {}))),
                    Some(false),
                    Some(false),
                )?;
                Ok(())
            },
            || Ok(()),
            move |boundary| {
                boundary.native().verify()?;
                ensure!(
                    boundary
                        .native()
                        .inventory()
                        .startup_reconciliation()
                        .unwrap()
                        .phase
                        == StartupReconciliationPhase::Processed,
                    "actual reconciliation missing"
                );
                let quote_ns = attest_quote.get();
                if quote_ns == 0 {
                    return Ok(None);
                }
                ensure!(
                    quote_ns >= boundary.observation_started_at_ns(),
                    "restored stale quote used as observation"
                );
                Ok(Some(ObservedReceipt {
                    quote_ns,
                    history: attest_history.get(),
                }))
            },
            move |boundary, receipt| {
                boundary.native().verify()?;
                ensure!(
                    receipt.quote_ns == arm_quote.get() && receipt.history == arm_history.get(),
                    "private observation changed"
                );
                armed.set(true);
                Ok(StartupReceipt {
                    quote_ns: receipt.quote_ns,
                })
            },
            move |boundary, receipt| {
                boundary.observation().native().verify()?;
                ensure!(
                    receipt.quote_ns == before_quote.get(),
                    "startup receipt changed"
                );
                release_count.set(release_count.get() + 1);
                if expire_during_durable_release {
                    std::thread::sleep(Duration::from_millis(80));
                }
                Ok(())
            },
            move |boundary| {
                let now = boundary.actual_now_ns();
                ensure!(
                    final_clock.borrow().timestamp_ns().as_u64() >= now,
                    "actual actor clock remained in capture read view"
                );
                ensure!(
                    now <= final_deadline.get(),
                    "actual observations expired during durable release"
                );
                Ok(())
            },
            move |_| fenced.set(fenced.get() + 1),
        )
        .unwrap();
        let handle = node.handle();
        let driving_handle = handle.clone();
        let observed_start = starts.clone();
        let clock = node.kernel.clock.clone();
        let driver = async {
            while driving_handle.state() != NodeState::Observing {
                tokio::task::yield_now().await;
            }
            assert_eq!(observed_start.get(), 0, "trader started before observation");
            while driving_handle.metrics_snapshot().data_commands.dispatched == 0 {
                tokio::task::yield_now().await;
            }
            let now = clock.borrow().timestamp_ns();
            deadline_ns.set(
                now.as_u64()
                    + if expire_during_durable_release {
                        30_000_000
                    } else {
                        5_000_000_000
                    },
            );
            nautilus_common::live::runner::get_data_event_sender()
                .send(DataEvent::Data(Data::Quote(QuoteTick::new(
                    instrument_id,
                    Price::from("0.80000"),
                    Price::from("0.80001"),
                    Quantity::from(10),
                    Quantity::from(11),
                    now,
                    UnixNanos::new(now.as_u64()),
                ))))
                .unwrap();
            if !expire_during_durable_release {
                while !driving_handle.is_running() {
                    tokio::task::yield_now().await;
                }
                driving_handle.stop();
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(node.run_with_mode(NodeRunMode::Hosted), driver)
        })
        .await
        .expect("native observation lifecycle did not finish");
        assert_eq!(history.get(), 8);
        assert_eq!(released.get(), 1);
        assert_eq!(starts.get(), 1);
        assert_eq!(result.is_err(), expire_during_durable_release);
        assert_eq!(
            node.kernel.exec_engine.borrow().submissions_fenced(),
            expire_during_durable_release
        );
        assert_eq!(
            node.kernel
                .exec_engine
                .borrow()
                .recovery_observation_fenced(),
            expire_during_durable_release
        );
        assert_eq!(fences.get() > 0, expire_during_durable_release);
        assert_eq!(
            node.recovery_requires_release,
            expire_during_durable_release
        );
    }
}
