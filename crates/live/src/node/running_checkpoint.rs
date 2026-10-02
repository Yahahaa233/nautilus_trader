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
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    };
    use rstest::rstest;

    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{NodeDispatchObserver, NodeRunMode},
    };

    #[derive(Debug)]
    struct PendingQuotesActor {
        core: nautilus_common::actor::DataActorCore,
        request: Rc<Cell<Option<nautilus_core::UUID4>>>,
        responses: Rc<Cell<usize>>,
        delay_response: bool,
    }
    impl nautilus_common::actor::DataActor for PendingQuotesActor {
        fn on_start(&mut self) -> Result<()> {
            let request = self.request_quotes(
                crypto_perpetual_ethusdt().id(),
                None,
                None,
                None,
                None,
                None,
            )?;
            self.request.set(Some(request));
            Ok(())
        }
        fn on_historical_quotes(&mut self, _: &[nautilus_model::data::QuoteTick]) -> Result<()> {
            if self.delay_response {
                // Real callback processing crosses the configured deadline while
                // the select-loop cannot poll its ready expiration arm.
                std::thread::sleep(Duration::from_millis(100));
            }
            self.responses.set(self.responses.get() + 1);
            Ok(())
        }
    }
    nautilus_common::nautilus_actor!(PendingQuotesActor);

    #[derive(Debug)]
    struct CheckpointHeartbeatActor {
        core: nautilus_common::actor::DataActorCore,
        heartbeats: Rc<Cell<usize>>,
    }
    impl nautilus_common::actor::DataActor for CheckpointHeartbeatActor {
        fn on_start(&mut self) -> Result<()> {
            self.clock().set_timer(
                "CHECKPOINT-FAIRNESS-HEARTBEAT",
                Duration::from_millis(10),
                None,
                None,
                None,
                Some(false),
                Some(false),
            )?;
            // The actual LiveClock producer runs on its real background runtime.
            // Keep original startup dispatch active while it queues a backlog;
            // no test-created callback/message replaces this registered owner.
            std::thread::sleep(Duration::from_millis(60));
            Ok(())
        }
        fn on_time_event(&mut self, event: &nautilus_common::timer::TimeEvent) -> Result<()> {
            ensure!(
                event.name.as_str() == "CHECKPOINT-FAIRNESS-HEARTBEAT",
                "wrong timer owner"
            );
            self.heartbeats.set(self.heartbeats.get() + 1);
            Ok(())
        }
    }
    nautilus_common::nautilus_actor!(CheckpointHeartbeatActor);

    #[derive(Debug)]
    struct ActualHeartbeatQueueCodec;
    impl crate::runner_recovery::RunnerRecoveryCodec for ActualHeartbeatQueueCodec {
        fn channel(&self) -> crate::runner_recovery::RunnerRecoveryChannel {
            crate::runner_recovery::RunnerRecoveryChannel::TimeEvent
        }
        fn codec_id(&self) -> &str {
            "actual-registered-heartbeat-capture.v1"
        }
        fn encode(
            &self,
            event: crate::runner_recovery::RunnerRecoveryEventRef<'_>,
        ) -> Result<serde_json::Value> {
            let crate::runner_recovery::RunnerRecoveryEventRef::TimeEvent(message) = event else {
                anyhow::bail!("expected actual timer message");
            };
            let event = message.event();
            Ok(
                serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id,
                "ts_event":event.ts_event,"ts_init":event.ts_init,
                "callback":message.checkpoint_callback_binding()}),
            )
        }
        fn decode(
            &self,
            _: &crate::runner_recovery::RunnerRecoveryEnvelope,
        ) -> Result<crate::runner_recovery::RunnerRecoveryEvent> {
            anyhow::bail!("actual fairness source registry is capture-only")
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn checkpoint_slow_persistence_bounds_maintenance_and_registered_heartbeat_priority() {
        use nautilus_common::actor::{DataActorConfig, DataActorCore};

        let trace = Rc::new(std::cell::RefCell::new(Vec::new()));
        let retained = Rc::new(Cell::new(false));
        let heartbeats = Rc::new(Cell::new(0));
        let mut node =
            LiveNode::builder(TraderId::from("CHECKPOINT-FAIRNESS"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .with_delay_post_stop_secs(0)
                .with_delay_shutdown_secs(1)
                .with_event_store({
                    let trace = trace.clone();
                    let retained = retained.clone();
                    move |_, _| Ok(Box::new(TerminalStore { trace, retained }))
                })
                .build()
                .unwrap();
        node.add_actor(CheckpointHeartbeatActor {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some("CHECKPOINT-HEARTBEAT-ACTOR".into()),
                ..Default::default()
            }),
            heartbeats: heartbeats.clone(),
        })
        .unwrap();
        let selected = Rc::new(std::cell::RefCell::new(Vec::new()));
        let selected_input = selected.clone();
        let maintenance_roots = Rc::new(Cell::new(0));
        let maintenance = maintenance_roots.clone();
        let handle = node.handle();
        let stop = handle.clone();
        let selected_roots = Rc::new(std::cell::RefCell::new(Vec::new()));
        let actual_roots = selected_roots.clone();
        let root_handle = handle.clone();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("slow-checkpoint-heartbeat".into(), move |record| {
                if let crate::dispatch::DispatchRecord::Begin {
                    root_sequence,
                    parent_input_sequence: None,
                    input,
                    ..
                } = record
                    && !root_handle.should_stop()
                    && matches!(
                        input.source,
                        crate::dispatch::DispatchSource::Maintenance
                            | crate::dispatch::DispatchSource::Time
                    )
                {
                    actual_roots.borrow_mut().push(*root_sequence);
                }
                Ok(())
            })
            .unwrap(),
            move |source, phase, input| {
                let payload = if let Some(native) =
                    input.downcast_ref::<super::super::NativeMutationInput>()
                {
                    if source == crate::dispatch::DispatchSource::Maintenance {
                        selected_input.borrow_mut().push("maintenance");
                        maintenance.set(maintenance.get() + 1);
                        // Bound the actual loop even if the old scheduling bug
                        // regresses; stop does not modify a frozen checkpoint.
                        if maintenance.get() == 5 {
                            stop.stop();
                        }
                    }
                    native.canonical_payload()?
                } else if let Some(message) =
                    input.downcast_ref::<nautilus_common::runner::TimeEventMessage>()
                {
                    selected_input.borrow_mut().push("heartbeat");
                    let event = message.event();
                    serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id,
                        "ts_event":event.ts_event,"ts_init":event.ts_init,
                        "callback":message.checkpoint_callback_binding()})
                } else {
                    anyhow::bail!("unsupported actual fairness input");
                };
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        let mut registry = RunnerRecoveryCodecRegistry::new([
            crate::runner_recovery::RunnerRecoveryChannel::TimeEvent,
        ]);
        registry
            .register_owner_bound_timer_codec(ActualHeartbeatQueueCodec)
            .unwrap();
        let captures = Rc::new(std::cell::RefCell::new(Vec::new()));
        let persisted = captures.clone();
        let retained_fence = retained.clone();
        node.set_running_checkpoint_handler(
            Rc::new(registry.seal().unwrap()),
            RunningCheckpointSchedule::EveryCompletedRoot,
            |boundary| {
                boundary.verify()?;
                Ok((
                    boundary.completion_proof().root_sequence(),
                    boundary.inventory().is_terminal_cut(),
                ))
            },
            move |capture| {
                // Every selected root still gets its original synchronous
                // persistence. Its cost makes maintenance continuously ready.
                std::thread::sleep(Duration::from_millis(130));
                persisted.borrow_mut().push(capture);
                Ok(())
            },
            move |_| retained_fence.set(true),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            node.run_with_mode(NodeRunMode::Hosted),
        )
        .await
        .expect("actual heartbeat fairness bounded run")
        .unwrap();
        let selected = selected.borrow();
        let maintenance_indices = selected
            .iter()
            .enumerate()
            .filter_map(|(index, kind)| (*kind == "maintenance").then_some(index))
            .collect::<Vec<_>>();
        assert_eq!(maintenance_indices.len(), 5, "{selected:?}");
        assert!(
            maintenance_indices
                .windows(2)
                .all(|indices| selected[indices[0] + 1..indices[1]].contains(&"heartbeat")),
            "ready heartbeat starved by continuously due maintenance: {selected:?}"
        );
        assert!(
            heartbeats.get() >= 4,
            "original actor heartbeat callback did not run"
        );
        let captures = captures.borrow();
        for root in selected_roots.borrow().iter() {
            assert!(
                captures
                    .iter()
                    .any(|(captured, terminal)| captured == root && !terminal),
                "selected original root {root} lost its synchronous checkpoint: {captures:?}"
            );
        }
        assert!(
            captures.windows(2).all(|cuts| cuts[0].0 < cuts[1].0),
            "cut roots did not advance"
        );
        assert!(captures.last().is_some_and(|(_, terminal)| *terminal));
        assert!(!retained.get());
        assert!(trace.borrow().contains(&"seal"));
        assert!(!node.kernel.exec_engine.borrow().submissions_fenced());
    }

    #[rstest]
    #[case::actual_response(false, false, false, false)]
    #[case::deadline_without_another_input(true, false, false, false)]
    #[case::terminal_pending(false, true, false, false)]
    #[case::changed_ingress_at_persistence(false, false, true, false)]
    #[case::late_response_must_not_clear_expired_budget(false, false, false, true)]
    #[tokio::test(flavor = "current_thread")]
    async fn checkpoint_pending_request_defers_real_cut_then_captures_or_retains_failure(
        #[case] never_respond: bool,
        #[case] stop_pending: bool,
        #[case] change_during_write: bool,
        #[case] late_response: bool,
    ) {
        use nautilus_common::{
            actor::{DataActorConfig, DataActorCore},
            messages::{
                DataResponse,
                data::{DataCommand, QuotesResponse, RequestCommand},
            },
            msgbus,
        };
        let trace = Rc::new(std::cell::RefCell::new(Vec::new()));
        let retained = Rc::new(Cell::new(false));
        let request = Rc::new(Cell::new(None));
        let responses = Rc::new(Cell::new(0));
        let mut node =
            LiveNode::builder(TraderId::from("PENDING-CHECKPOINT"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .with_delay_post_stop_secs(0)
                .with_delay_shutdown_secs(0)
                .with_event_store({
                    let trace = trace.clone();
                    let retained = retained.clone();
                    move |_, _| Ok(Box::new(TerminalStore { trace, retained }))
                })
                .build()
                .unwrap();
        let encoded_request = Rc::new(Cell::new(None));
        let recorded = encoded_request.clone();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("pending-request-cut".into(), |_| Ok(())).unwrap(),
            move |source, phase, input| {
                let payload = if let Some(native) =
                    input.downcast_ref::<super::super::NativeMutationInput>()
                {
                    native.canonical_payload()?
                } else if let Some(DataCommand::Request(RequestCommand::Quotes(value))) =
                    input.downcast_ref::<DataCommand>()
                {
                    recorded.set(Some(value.request_id));
                    serde_json::to_value(value)?
                } else if let Some(DataEvent::Instrument(value)) = input.downcast_ref::<DataEvent>()
                {
                    serde_json::to_value(value)?
                } else if let Some(DataEvent::Response(DataResponse::Quotes(value))) =
                    input.downcast_ref::<DataEvent>()
                {
                    serde_json::to_value(value)?
                } else {
                    anyhow::bail!("unsupported actual request test input");
                };
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        node.add_actor(PendingQuotesActor {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some("PENDING-QUOTES-ACTOR".into()),
                ..Default::default()
            }),
            request: request.clone(),
            responses: responses.clone(),
            delay_response: late_response,
        })
        .unwrap();
        let mut registry = RunnerRecoveryCodecRegistry::new([
            crate::runner_recovery::RunnerRecoveryChannel::DataEvent,
        ]);
        registry.register(ActualInstrumentQueueCodec).unwrap();
        let writes = Rc::new(Cell::new(0));
        let written = writes.clone();
        let ready_responses = responses.clone();
        let retained_fence = retained.clone();
        let handle = node.handle();
        node.set_running_checkpoint_handler(
            Rc::new(registry.seal().unwrap()),
            RunningCheckpointSchedule::Requested,
            move |boundary| {
                ensure!(
                    ready_responses.get() == 1,
                    "original registered actor response was not processed"
                );
                boundary.verify()?;
                Ok(boundary.inventory().is_terminal_cut())
            },
            move |terminal| {
                written.set(written.get() + 1);
                if change_during_write && !terminal {
                    ensure!(
                        nautilus_common::live::runner::get_data_event_sender()
                            .send(DataEvent::Instrument(InstrumentAny::CryptoPerpetual(
                                crypto_perpetual_ethusdt()
                            )))
                            .is_err(),
                        "changed actual input was admitted during checkpoint"
                    );
                }
                Ok(())
            },
            move |_| retained_fence.set(true),
        )
        .unwrap();
        assert!(
            node.set_running_checkpoint_pending_response_timeout(Duration::ZERO)
                .is_err()
        );
        node.set_running_checkpoint_pending_response_timeout(if never_respond || late_response {
            Duration::from_millis(50)
        } else {
            Duration::from_secs(2)
        })
        .unwrap();
        assert!(
            node.set_running_checkpoint_pending_response_timeout(Duration::from_secs(2))
                .is_err()
        );
        let drive = async {
            while !handle.is_running() {
                tokio::task::yield_now().await;
            }
            let correlation = request.get().expect("actual actor request missing");
            assert_eq!(encoded_request.get(), Some(correlation));
            assert_eq!(
                msgbus::local_only_recovery_readiness().unwrap(),
                msgbus::LocalRecoveryReadiness::PendingResponses { count: 1 }
            );
            handle.request_running_checkpoint();
            nautilus_common::live::runner::get_data_event_sender()
                .send(DataEvent::Instrument(InstrumentAny::CryptoPerpetual(
                    crypto_perpetual_ethusdt(),
                )))
                .unwrap();
            // Let the real completed Instrument root attempt the requested cut.
            tokio::time::sleep(Duration::from_millis(10)).await;
            assert_eq!(writes.get(), 0);
            assert!(!retained.get());
            assert_eq!(responses.get(), 0);
            assert!(
                msgbus::get_message_bus()
                    .borrow()
                    .get_response_handler(&correlation)
                    .is_some()
            );
            if stop_pending {
                handle.stop();
            } else if !never_respond {
                nautilus_common::live::runner::get_data_event_sender()
                    .send(DataEvent::Response(DataResponse::Quotes(QuotesResponse {
                        correlation_id: correlation,
                        client_id: "SIM".into(),
                        instrument_id: crypto_perpetual_ethusdt().id(),
                        data: vec![],
                        start: None,
                        end: None,
                        ts_init: 0.into(),
                        params: None,
                    })))
                    .unwrap();
                while writes.get() == 0 && !handle.should_stop() {
                    tokio::task::yield_now().await;
                }
                if !change_during_write && !late_response {
                    handle.stop();
                }
            }
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(node.run_with_mode(NodeRunMode::Hosted), drive)
        })
        .await
        .expect("actual request checkpoint bounded run");
        let failed = never_respond || stop_pending || change_during_write || late_response;
        assert_eq!(result.is_err(), failed, "{result:?}");
        assert_eq!(retained.get(), failed);
        assert_eq!(
            responses.get(),
            usize::from(!never_respond && !stop_pending)
        );
        if failed {
            assert!(node.kernel.exec_engine.borrow().submissions_fenced());
            node.kernel.dispose();
            assert!(!trace.borrow().contains(&"seal"));
        } else {
            assert_eq!(writes.get(), 2);
            assert!(trace.borrow().contains(&"seal"));
        }
        if never_respond || late_response {
            assert!(
                format!("{:#}", result.as_ref().unwrap_err())
                    .contains("checkpoint pending responses deadline expired")
            );
        }
        if stop_pending {
            assert!(
                format!("{:#}", result.as_ref().unwrap_err())
                    .contains("terminal checkpoint message bus responses remain pending")
            );
        }
    }

    #[derive(Debug)]
    struct ActualInstrumentQueueCodec;
    impl crate::runner_recovery::RunnerRecoveryCodec for ActualInstrumentQueueCodec {
        fn channel(&self) -> crate::runner_recovery::RunnerRecoveryChannel {
            crate::runner_recovery::RunnerRecoveryChannel::DataEvent
        }
        fn codec_id(&self) -> &str {
            "actual-native-instrument.v1"
        }
        fn encode(
            &self,
            event: crate::runner_recovery::RunnerRecoveryEventRef<'_>,
        ) -> Result<serde_json::Value> {
            match event {
                crate::runner_recovery::RunnerRecoveryEventRef::DataEvent(
                    DataEvent::Instrument(instrument),
                ) => Ok(serde_json::to_value(instrument)?),
                _ => anyhow::bail!("unsupported actual instrument test input"),
            }
        }
        fn decode(
            &self,
            envelope: &crate::runner_recovery::RunnerRecoveryEnvelope,
        ) -> Result<crate::runner_recovery::RunnerRecoveryEvent> {
            Ok(crate::runner_recovery::RunnerRecoveryEvent::DataEvent(
                DataEvent::Instrument(serde_json::from_value(envelope.payload.clone())?),
            ))
        }
    }

    #[derive(Debug)]
    struct TerminalStore {
        trace: Rc<std::cell::RefCell<Vec<&'static str>>>,
        retained: Rc<Cell<bool>>,
    }
    impl nautilus_system::event_store::KernelEventStore for TerminalStore {
        fn restore_parent_cache(&mut self, _: nautilus_core::UUID4, _: &mut Cache) -> Result<()> {
            Ok(())
        }
        fn open(
            &mut self,
            _: nautilus_core::UUID4,
            _: &nautilus_system::event_store::RegisteredComponents,
            _: Environment,
        ) -> Result<()> {
            self.trace.borrow_mut().push("open");
            Ok(())
        }
        fn snapshot_anchorer(&self) -> Option<nautilus_execution::engine::SnapshotAnchorer> {
            None
        }
        fn seal(&mut self, _: nautilus_core::UnixNanos) {
            assert!(
                !self.retained.get(),
                "failed run must never receive normal seal"
            );
            self.trace.borrow_mut().push("seal");
        }
        fn failure_retention(
            &self,
        ) -> Option<nautilus_system::event_store::EventStoreFailureRetention> {
            let retained = self.retained.clone();
            Some(
                nautilus_system::event_store::EventStoreFailureRetention::new(move |_| {
                    retained.set(true);
                    Ok(())
                }),
            )
        }
        fn retain_unsealed(&mut self, reason: &str) -> Result<()> {
            ensure!(!reason.is_empty(), "failure reason missing");
            self.retained.set(true);
            self.trace.borrow_mut().push("retain");
            Ok(())
        }
        fn run_id(&self) -> Option<&str> {
            Some("terminal-actual-loop")
        }
        fn parent_run_id(&self) -> Option<&str> {
            None
        }
        fn is_halted(&self) -> bool {
            self.retained.get()
        }
    }

    #[rstest]
    #[case(false, false)]
    #[case(true, false)]
    #[case(false, true)]
    #[tokio::test(flavor = "current_thread")]
    async fn checkpoint_actual_stop_late_input_precedes_final_cut_and_seal_or_retains_failure(
        #[case] fail_writer: bool,
        #[case] change_during_write: bool,
    ) {
        let trace = Rc::new(std::cell::RefCell::new(Vec::new()));
        let retained = Rc::new(Cell::new(false));
        let store = TerminalStore {
            trace: trace.clone(),
            retained: retained.clone(),
        };
        let mut node =
            LiveNode::builder(TraderId::from("TERMINAL-CHECKPOINT"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .with_delay_post_stop_secs(0)
                .with_delay_shutdown_secs(0)
                .with_event_store(move |_, _| Ok(Box::new(store)))
                .build()
                .unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("terminal-cut".into(), |_| Ok(())).unwrap(),
            |source, phase, input| {
                let payload = if let Some(native) =
                    input.downcast_ref::<super::super::NativeMutationInput>()
                {
                    native.canonical_payload()?
                } else if let Some(DataEvent::Instrument(input)) = input.downcast_ref::<DataEvent>()
                {
                    serde_json::to_value(input)?
                } else {
                    anyhow::bail!("unexpected terminal actual input")
                };
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        let mut registry = RunnerRecoveryCodecRegistry::new([
            crate::runner_recovery::RunnerRecoveryChannel::DataEvent,
        ]);
        registry.register(ActualInstrumentQueueCodec).unwrap();
        let observed = Rc::new(Cell::new(false));
        let observed_clone = observed.clone();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let persisted = trace.clone();
        let handle = node.handle();
        let mutate = handle.clone();
        let fences = Rc::new(Cell::new(0));
        let fenced = fences.clone();
        node.set_running_checkpoint_handler(
            Rc::new(registry.seal().unwrap()),
            RunningCheckpointSchedule::Requested,
            move |boundary| {
                ensure!(
                    boundary.inventory().is_terminal_cut(),
                    "interval substituted for final cut"
                );
                ensure!(
                    boundary.cache().instrument(&id).is_some(),
                    "last accepted native instrument missing"
                );
                ensure!(
                    boundary.pending().entries.is_empty(),
                    "dequeued final input was not processed"
                );
                boundary.completion_proof().verify()?;
                observed_clone.set(true);
                Ok(())
            },
            move |_| {
                persisted.borrow_mut().push("terminal_checkpoint");
                if change_during_write {
                    mutate.stop();
                }
                ensure!(!fail_writer, "injected terminal durable writer failure");
                Ok(())
            },
            move |_| fenced.set(fenced.get() + 1),
        )
        .unwrap();
        let driver = async {
            while !handle.is_running() {
                tokio::task::yield_now().await;
            }
            handle.stop();
            nautilus_common::live::runner::get_data_event_sender()
                .send(DataEvent::Instrument(instrument))
                .unwrap();
        };
        let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(node.run_with_mode(NodeRunMode::Hosted), driver)
        })
        .await
        .expect("actual terminal run timed out");
        assert!(observed.get());
        let failed = fail_writer || change_during_write;
        assert_eq!(result.is_err(), failed);
        assert_eq!(fences.get(), u32::from(failed));
        assert_eq!(retained.get(), failed);
        assert_eq!(
            trace
                .borrow()
                .iter()
                .filter(|value| **value == "seal")
                .count(),
            usize::from(!failed)
        );
        if !failed {
            assert_eq!(*trace.borrow(), vec!["open", "terminal_checkpoint", "seal"]);
            assert!(node.handle.ingress_gate().verify_open().is_err());
        }
        if failed {
            assert!(node.kernel.exec_engine.borrow().submissions_fenced());
            node.kernel.dispose();
            assert!(
                !trace.borrow().contains(&"seal"),
                "dispose must not promote failure"
            );
        }
    }

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
        let mut registry = RunnerRecoveryCodecRegistry::new([
            crate::runner_recovery::RunnerRecoveryChannel::DataEvent,
        ]);
        registry.register(ActualInstrumentQueueCodec).unwrap();
        node.set_running_checkpoint_handler(
            Rc::new(registry.seal().unwrap()),
            RunningCheckpointSchedule::Requested,
            |boundary| {
                boundary.completion_proof().verify()?;
                ensure!(
                    boundary.completion_proof().root_sequence() > 0,
                    "root missing"
                );
                let inventory = serde_json::to_value(boundary.inventory())?;
                ensure!(
                    inventory["node_state"] == "running" || boundary.inventory().is_terminal_cut(),
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
                "requested capture must wait for an explicit request"
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
        assert_eq!(writes.get(), if fail_writer { 1 } else { 2 });
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
    retained_recovery_timers: Option<serde_json::Value>,
    restored_adapters: Option<BTreeMap<String, serde_json::Value>>,
    data_client_state: BTreeMap<String, serde_json::Value>,
    data_engine: serde_json::Value,
    portfolio: serde_json::Value,
    message_bus_mode: &'static str,
    execution_authorized: bool,
}

impl RunningCheckpointInventory {
    #[must_use]
    pub const fn execution_manager(&self) -> &serde_json::Value {
        &self.execution_manager
    }
    #[must_use]
    pub const fn data_engine(&self) -> &serde_json::Value {
        &self.data_engine
    }
    #[must_use]
    pub const fn portfolio(&self) -> &serde_json::Value {
        &self.portfolio
    }
    /// True only for the final cut which permanently closes actual admission.
    #[must_use]
    pub fn is_terminal_cut(&self) -> bool {
        self.node_state == "shutting_down"
    }
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
    #[must_use]
    pub const fn retained_recovery_timers(&self) -> Option<&serde_json::Value> {
        self.retained_recovery_timers.as_ref()
    }
    #[must_use]
    pub const fn restored_adapters(&self) -> Option<&BTreeMap<String, serde_json::Value>> {
        self.restored_adapters.as_ref()
    }
    #[must_use]
    pub const fn adapters(&self) -> &BTreeMap<String, serde_json::Value> {
        &self.adapters
    }
    #[must_use]
    pub const fn data_client_state(&self) -> &BTreeMap<String, serde_json::Value> {
        &self.data_client_state
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
    #[cfg(feature = "native-tail-replay")]
    native_trace_cut: Option<&'a nautilus_event_store::native_trace::NativeTraceCheckpointCut>,
    #[cfg(feature = "native-tail-replay")]
    native_trace: Option<&'a nautilus_event_store::native_trace::NativeTraceRecorder>,
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
    /// Actual source cut generated while all native queue/adapter/timer guards are held.
    #[cfg(feature = "native-tail-replay")]
    #[must_use]
    pub fn native_trace_cut(
        &self,
    ) -> Option<&nautilus_event_store::native_trace::NativeTraceCheckpointCut> {
        self.native_trace_cut
    }
    /// Persists one complete host artifact binding in the actual source Journal at this cut.
    /// # Errors
    /// Refuses an absent trace, changed boundary or failed actual durable acknowledgment.
    #[cfg(feature = "native-tail-replay")]
    pub fn persist_native_checkpoint(
        &self,
        host_checkpoint: serde_json::Value,
    ) -> Result<nautilus_event_store::writer::DurableEntryAcknowledgment> {
        self.verify()?;
        let ack = self
            .native_trace
            .context("native trace not installed")?
            .persist_checkpoint(
                self.native_trace_cut.context("native cut absent")?,
                host_checkpoint,
            )?;
        self.verify()?;
        Ok(ack)
    }

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

#[derive(Debug)]
pub(super) struct PendingResponseCapture {
    first_root: u64,
    request_sequence: u64,
    count: usize,
    deadline: dst::time::Instant,
}

enum CaptureDisposition {
    Persisted,
    PendingResponses(usize),
}

pub(super) struct RunningCheckpointRegistration {
    pub(super) registry: Rc<RunnerRecoveryCodecRegistry>,
    pub(super) schedule: RunningCheckpointSchedule,
    pub(super) collect: Rc<Collect>,
    pub(super) persist: Rc<Persist>,
    pub(super) fence: Rc<Fence>,
    pub(super) last_root: u64,
    pub(super) last_request: u64,
    pub(super) last_capture: dst::time::Instant,
    pub(super) pending_response_timeout: Option<Duration>,
    pub(super) pending_responses: Option<PendingResponseCapture>,
}

impl Debug for RunningCheckpointRegistration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunningCheckpointRegistration")
            .field("schedule", &self.schedule)
            .field("last_root", &self.last_root)
            .finish_non_exhaustive()
    }
}

/// Same-process proof of a persisted final cut. No deserialization or setter.
#[derive(Debug)]
pub(super) struct TerminalCheckpointReceipt {
    pub(super) node_instance: nautilus_core::UUID4,
    pub(super) root_sequence: u64,
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
            pending_response_timeout: None,
            pending_responses: None,
        });
        Ok(())
    }

    /// Explicitly bounds deferral of due nonterminal cuts with local responses pending.
    /// The original request/cadence stays due; only a later completed root may capture.
    /// Without this opt-in, the original fail-closed behavior is retained.
    ///
    /// # Errors
    /// Requires an idle registered collector and an explicit positive representable timeout.
    pub fn set_running_checkpoint_pending_response_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "checkpoint timeout requires idle node"
        );
        ensure!(
            !timeout.is_zero() && dst::time::Instant::now().checked_add(timeout).is_some(),
            "checkpoint pending timeout must be explicit and positive"
        );
        let registration = self
            .running_checkpoint
            .as_mut()
            .context("checkpoint handler missing")?;
        ensure!(
            registration.pending_response_timeout.is_none(),
            "checkpoint pending timeout already configured"
        );
        registration.pending_response_timeout = Some(timeout);
        Ok(())
    }

    pub(super) fn running_checkpoint_pending_response_deadline(
        &self,
    ) -> Option<dst::time::Instant> {
        self.running_checkpoint
            .as_ref()?
            .pending_responses
            .as_ref()
            .map(|pending| pending.deadline)
    }

    fn verify_running_checkpoint_pending_response_deadline(&self) -> Result<()> {
        let Some(registration) = &self.running_checkpoint else {
            return Ok(());
        };
        let Some(pending) = &registration.pending_responses else {
            return Ok(());
        };
        if dst::time::Instant::now() < pending.deadline {
            return Ok(());
        }
        let reason = format!(
            "checkpoint pending responses deadline expired: count={}, first_root={}, request_sequence={}",
            pending.count, pending.first_root, pending.request_sequence
        );
        anyhow::bail!(reason)
    }

    pub(super) fn expire_running_checkpoint_pending_responses(&self) -> Result<()> {
        if let Err(error) = self.verify_running_checkpoint_pending_response_deadline() {
            self.fail_native_dispatch(&format!("{error:#}"));
            return Err(error);
        }
        Ok(())
    }

    /// Runs only on the original runloop after trader stop and residual grace.
    /// Every actually dequeued input still gets the ordinary begin/complete path.
    pub(super) fn final_checkpoint_before_stop(
        &mut self,
        receivers: RunningReceivers<'_>,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::ShuttingDown && self.running_checkpoint.is_some(),
            "final checkpoint requires shutting-down original run"
        );
        let result = (|| -> Result<()> {
            let mut processed = 0usize;
            loop {
                let mut progress = false;
                macro_rules! take {
                    ($field:ident, $apply:expr) => {
                        while let Ok(input) = receivers.$field.try_recv() {
                            processed += 1;
                            ensure!(processed <= 100_000, "final drain input bound exceeded");
                            progress = true;
                            ($apply)(self, input)?;
                        }
                    };
                }
                take!(time_evt_rx, |node: &mut Self, input| -> Result<()> {
                    ensure!(
                        node.process_time_event(input),
                        "final native timer dispatch rejected"
                    );
                    Ok(())
                });
                take!(system_evt_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_system_event(input);
                    Ok(())
                });
                take!(system_cmd_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_system_command(input);
                    Ok(())
                });
                take!(exec_evt_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_exec_event(input);
                    Ok(())
                });
                take!(exec_cmd_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_exec_command(input);
                    Ok(())
                });
                take!(data_evt_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_data_event(input);
                    Ok(())
                });
                take!(data_cmd_rx, |node: &mut Self, input| -> Result<()> {
                    node.process_data_command(input);
                    Ok(())
                });
                if !progress {
                    break;
                }
            }
            let input = self.native_lifecycle_input("stop.final_admission_cut")?;
            let guard = self
                .begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &input)?
                .context("final cut observer missing")?;
            self.finish_node_dispatch(guard)?;
            self.checkpoint_completed_root_mode(receivers, false, true)?;
            ensure!(
                self.terminal_checkpoint.is_some(),
                "final cut was not persisted"
            );
            Ok(())
        })();
        if let Err(ref error) = result {
            self.kernel.exec_engine.borrow().fence_submissions();
            self.handle.ingress_gate().invalidate();
            if let Some(registration) = &self.running_checkpoint {
                let reason = format!("final stop checkpoint: {error:#}");
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (registration.fence)(&reason)
                }));
            }
        }
        result
    }

    pub(super) fn checkpoint_after_completed_root(
        &mut self,
        receivers: RunningReceivers<'_>,
        pending_report_tasks: bool,
    ) -> Result<()> {
        self.checkpoint_completed_root_mode(receivers, pending_report_tasks, false)
    }
    pub(super) fn checkpoint_completed_root_mode(
        &mut self,
        mut receivers: RunningReceivers<'_>,
        pending_report_tasks: bool,
        terminal: bool,
    ) -> Result<()> {
        let Some(registration) = self.running_checkpoint.as_ref() else {
            return Ok(());
        };
        self.expire_running_checkpoint_pending_responses()?;
        if terminal {
            ensure!(
                self.state() == NodeState::ShuttingDown && self.terminal_checkpoint.is_none(),
                "terminal capture requires original shutting-down run"
            );
        } else if !matches!(self.state(), NodeState::Running | NodeState::Observing)
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
            ensure!(!terminal, "terminal cut requires new completed root");
            return Ok(());
        }
        let now = dst::time::Instant::now();
        let request_sequence = self.handle.checkpoint_requested();
        let due = terminal
            || request_sequence > registration.last_request
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
        let capture = || -> Result<CaptureDisposition> {
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
            let data_engine_state = data.running_checkpoint_state()?;
            let portfolio_state = self
                .kernel
                .portfolio
                .try_borrow()?
                .running_checkpoint_state()?;
            // No adapter/runner/timer freeze has begun. Pending correlations are
            // a typed local-bus observation, never an error-text retry heuristic.
            proof.verify()?;
            if let nautilus_common::msgbus::LocalRecoveryReadiness::PendingResponses { count } =
                nautilus_common::msgbus::local_only_recovery_readiness()?
            {
                ensure!(
                    !terminal,
                    "terminal checkpoint message bus responses remain pending"
                );
                ensure!(
                    self.running_checkpoint
                        .as_ref()
                        .unwrap()
                        .pending_response_timeout
                        .is_some(),
                    "message bus responses remain pending without explicit checkpoint timeout"
                );
                return Ok(CaptureDisposition::PendingResponses(count));
            }
            let mut adapters = Vec::new();
            let data_client_state = data
                .get_clients()
                .iter()
                .map(|client| {
                    Ok((
                        format!("data:{}", client.client_id),
                        client.running_checkpoint_state()?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
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
            recovery_quiescence::with_registered_timer_inventory_mode(
                &self.kernel.clock,
                &self.kernel.trader,
                terminal,
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
                            let mut pending = receivers.snapshot(&ingress, &guard, &registry)?;
                            let retained_timers = self
                                .recovery_timers
                                .as_ref()
                                .map(|timers| timers.inventory(&registry))
                                .transpose()?;
                            if let Some(timers) = &self.recovery_timers {
                                let mut retained = timers.pending_entries(&registry)?;
                                let count = retained.len() as u64;
                                for entry in &mut pending.entries {
                                    if entry.channel
                                        == crate::runner_recovery::RunnerRecoveryChannel::TimeEvent
                                    {
                                        entry.channel_ordinal = entry
                                            .channel_ordinal
                                            .checked_add(count)
                                            .context("timer FIFO ordinal exhausted")?;
                                    }
                                }
                                retained.append(&mut pending.entries);
                                pending.entries = retained;
                            }
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
                                profile: if terminal {
                                    "terminal_completed_root_closed_admission.v1"
                                } else {
                                    "running_completed_root_local_bus_registered_live_timers.v1"
                                },
                                node_state: if terminal {
                                    "shutting_down"
                                } else if capture_state == NodeState::Observing {
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
                                retained_recovery_timers: retained_timers.clone(),
                                restored_adapters: self.recovery_adapter_source.clone(),
                                data_client_state: data_client_state.clone(),
                                data_engine: data_engine_state.clone(),
                                portfolio: portfolio_state.clone(),
                                message_bus_mode: "local_only",
                                execution_authorized: false,
                            };
                            let verify = || -> Result<()> {
                                self.verify_running_checkpoint_pending_response_deadline()?;
                                ensure!(
                                    self.state() == capture_state
                                        && (terminal || !self.handle.should_stop()),
                                    "node lifecycle changed during checkpoint"
                                );
                                ensure!(
                                    self.recovery_timers
                                        .as_ref()
                                        .map(|timers| timers.inventory(&registry))
                                        .transpose()?
                                        == retained_timers,
                                    "retained native timer handoff changed during checkpoint"
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
                                    data.running_checkpoint_state()? == data_engine_state,
                                    "actual native DataEngine internal inventory changed"
                                );
                                ensure!(
                                    self.kernel
                                        .portfolio
                                        .try_borrow()?
                                        .running_checkpoint_state()?
                                        == portfolio_state,
                                    "actual native Portfolio history changed during checkpoint"
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
                            #[cfg(feature = "native-tail-replay")]
                            let native_trace_cut = observer.native_trace().map(|trace| {
                                ensure!(self.recovery_timers.is_none(),
                                    "legacy retained timers lack original causal receipts");
                                let receipts = receivers.native_pending_receipts()?;
                                let value = serde_json::to_value(&inventory)?;
                                let digest = nautilus_event_store::native_trace::native_inventory_digest(&value)?;
                                let mut cut = trace.checkpoint_cut_at(proof.root_sequence(), proof.input_sequence(), digest, receipts, now, inventory.captured_at_ns)?;
                                cut.pending_inputs = receivers.native_pending_inputs(&|source, input| {
                                    Ok(observer.encode_historical_source(crate::node::dispatch::source_dispatch(source)?, "native_enqueue", input)?.payload)
                                })?;
                                cut.registered_timers = inventory.timers.clone();
                                cut.native_effects["registered_timers"] = serde_json::to_value(&inventory.timers)?;
                                cut.native_effects["execution_manager"] = self.exec_manager.trace_effects_inventory(now)?;
                                cut.native_effects["portfolio"] = portfolio_state.clone();
                                if cut.native_effects.get("manager_effects_capture_process_elapsed_ns").is_some() {
                                    cut.native_effects["manager_effects_capture_process_elapsed_ns"] = cut.captured_process_elapsed_ns.into();
                                }
                                cut.native_effects["timer_capture_ns"] = inventory.captured_at_ns.into();
                                ensure!(cut.pending_inputs.iter().map(|input| &input.receipt).eq(cut.pending.iter()),
                                    "actual staged source payload/receipt inventory differs");
                                Ok(cut)
                            }).transpose()?;
                            let boundary = RunningCheckpointBoundary {
                                #[cfg(feature = "native-tail-replay")]
                                native_trace_cut: native_trace_cut.as_ref(),
                                #[cfg(feature = "native-tail-replay")]
                                native_trace: observer.native_trace(),
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
                            if terminal {
                                guard.finish_terminal()?;
                            } else {
                                guard.finish()?;
                            }
                            Ok(())
                        })
                    })
                },
            )?;
            // Reopen adapters only after the runner is ready. No await occurs
            // anywhere in the node-thread collect/persist/revalidation interval.
            for (_, adapter) in adapters {
                if terminal {
                    adapter.finish_terminal()?;
                } else {
                    adapter.finish()?;
                }
            }
            Ok(CaptureDisposition::Persisted)
        };
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(capture));
        match outcome {
            Ok(Ok(CaptureDisposition::PendingResponses(count))) => {
                let registration = self.running_checkpoint.as_mut().unwrap();
                if let Some(pending) = &mut registration.pending_responses {
                    pending.count = count;
                } else {
                    let timeout = registration
                        .pending_response_timeout
                        .context("checkpoint timeout missing")?;
                    registration.pending_responses = Some(PendingResponseCapture {
                        first_root: proof.root_sequence(),
                        request_sequence,
                        count,
                        deadline: now
                            .checked_add(timeout)
                            .context("checkpoint timeout overflow")?,
                    });
                }
                // Do not update last_capture/last_request, persist an old cut,
                // discard correlations, or reuse this completed root.
                Ok(())
            }
            Ok(Ok(CaptureDisposition::Persisted)) => {
                self.expire_running_checkpoint_pending_responses()?;
                let registration = self.running_checkpoint.as_mut().unwrap();
                registration.last_capture = now;
                registration.last_request = request_sequence;
                registration.pending_responses = None;
                if terminal {
                    self.terminal_checkpoint = Some(TerminalCheckpointReceipt {
                        node_instance: self.kernel.instance_id,
                        root_sequence: proof.root_sequence(),
                    });
                }
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
                    Ok(Ok(_)) => unreachable!(),
                };
                let reason = format!("{error:#}");
                if !terminal {
                    let _ =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| fence(&reason)));
                }
                self.handle.stop();
                Err(error)
            }
        }
    }
}
