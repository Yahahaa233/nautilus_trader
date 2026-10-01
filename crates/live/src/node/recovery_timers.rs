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

//! Actual owner-clock timer restoration and retained pending callback handoff.

use super::{LiveNode, NodeState};
use crate::{
    runner::{RunningReceivers, SnapshotReceiver},
    runner_recovery::{
        RunnerPendingEntry, RunnerRecoveryCodecRegistry, RunnerRecoveryEventRef,
        RunnerRecoveryWatermark,
    },
};
use anyhow::{Context, Result, ensure};
use nautilus_common::{clock::RestoredTimerCheckpoint, runner::TimeEventMessage, timer::TimeEvent};
use nautilus_core::{UUID4, UnixNanos};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    rc::Rc,
};

/// Exact source callback event and its actual owner-clock binding. Authority is
/// provided by this node's completed native recovery frontier, not this DTO.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedRecoveryTimerInput {
    pub owner: String,
    pub source_binding_id: u64,
    pub channel_ordinal: u64,
    pub name: String,
    pub event_id: UUID4,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub cleanup: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
struct TimerProgress {
    watermark: RunnerRecoveryWatermark,
    node_instance_id: UUID4,
    events: BTreeMap<String, RetainedTimerProgressEvent>,
}
#[derive(Debug, Clone, serde::Serialize)]
struct RetainedTimerProgressEvent {
    phase: &'static str,
    expected_event: serde_json::Value,
    actual_callback_binding: serde_json::Value,
}
fn event_identity(message: &TimeEventMessage) -> serde_json::Value {
    let event = message.event();
    serde_json::json!({"name":event.name,"event_id":event.event_id,"ts_event":event.ts_event,"ts_init":event.ts_init})
}
/// Private same-node handoff progress. Retention/queueing is not processing.
#[derive(Debug, Clone)]
pub struct RetainedRecoveryTimerHandoff(Rc<RefCell<TimerProgress>>);
impl RetainedRecoveryTimerHandoff {
    pub fn progress(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&*self.0.try_borrow()?)?)
    }
}
#[derive(Debug)]
pub(super) struct RetainedNodeTimers {
    pub(super) clocks: BTreeMap<String, Option<Box<dyn RestoredTimerCheckpoint>>>,
    source: BTreeMap<String, serde_json::Value>,
    source_pending: Vec<RetainedRecoveryTimerInput>,
    pending: VecDeque<TimeEventMessage>,
    progress: RetainedRecoveryTimerHandoff,
}
impl RetainedNodeTimers {
    pub(super) fn verify(&self) -> Result<()> {
        for clock in self.clocks.values().flatten() {
            clock.verify()?;
        }
        Ok(())
    }
    pub(super) fn inventory(
        &self,
        registry: &RunnerRecoveryCodecRegistry,
    ) -> Result<serde_json::Value> {
        self.verify()?;
        let actual = self
            .pending
            .iter()
            .enumerate()
            .map(|(i, m)| registry.encode(RunnerRecoveryEventRef::TimeEvent(m), i as u64))
            .collect::<Result<Vec<_>>>()?;
        Ok(
            serde_json::json!({"profile":"registered_owner_clock_retained_timers.v1", "source":self.source,
            "source_pending":self.source_pending,"actual_retained_pending":actual,"progress":self.progress.progress()?,
            "execution_authorized":false}),
        )
    }
    pub(super) fn pending_entries(
        &self,
        registry: &RunnerRecoveryCodecRegistry,
    ) -> Result<Vec<RunnerPendingEntry>> {
        self.verify()?;
        self.pending
            .iter()
            .enumerate()
            .map(|(i, m)| registry.encode(RunnerRecoveryEventRef::TimeEvent(m), i as u64))
            .collect()
    }
    pub(super) fn resume_observers(
        &mut self,
        observer_ids: &[nautilus_model::identifiers::ActorId],
    ) -> Result<()> {
        self.verify()?;
        for id in observer_ids {
            if let Some(receipt) = self
                .clocks
                .get_mut(&format!("component:{id}"))
                .and_then(Option::take)
            {
                receipt.resume()?;
            }
        }
        Ok(())
    }
    pub(super) fn handoff_after_start(
        &mut self,
        receiver: &mut SnapshotReceiver<TimeEventMessage>,
    ) -> Result<()> {
        self.verify()?;
        // Reserve before moving any owned callback. Old source events form the
        // actual prefix and therefore precede new current-process time events.
        receiver.prepend_retained(&mut self.pending)?;
        for state in self.progress.0.try_borrow_mut()?.events.values_mut() {
            ensure!(state.phase == "retained", "timer handoff already attempted");
            state.phase = "queued";
        }
        for receipt in self.clocks.values_mut().filter_map(Option::take) {
            receipt.resume()?;
        }
        Ok(())
    }
    pub(super) fn received(&self, message: &TimeEventMessage) -> Result<()> {
        let id = message.event().event_id;
        if let Some(state) = self
            .progress
            .0
            .try_borrow_mut()?
            .events
            .get_mut(&id.to_string())
        {
            ensure!(
                state.phase == "queued",
                "retained timer receive duplicate or out of order"
            );
            ensure!(
                state.expected_event == event_identity(message)
                    && state.actual_callback_binding == message.checkpoint_callback_binding(),
                "retained timer source headers or actual callback binding changed"
            );
            state.phase = "received";
        }
        Ok(())
    }
    pub(super) fn processed(&self, id: UUID4) -> Result<()> {
        if let Some(state) = self
            .progress
            .0
            .try_borrow_mut()?
            .events
            .get_mut(&id.to_string())
        {
            ensure!(
                state.phase == "received",
                "retained timer processing without actual receive"
            );
            state.phase = "processed";
        }
        Ok(())
    }
}
impl LiveNode {
    /// Rebuilds exact actual owner-clock schedules and retains callbacks until
    /// sealed recovered startup. The source frontier must already be installed
    /// by native cache/component replay. No queue ACK is a processed ACK.
    pub fn restore_registered_timer_checkpoint(
        &mut self,
        source: BTreeMap<String, serde_json::Value>,
        pending: Vec<RetainedRecoveryTimerInput>,
        watermark: &RunnerRecoveryWatermark,
    ) -> Result<RetainedRecoveryTimerHandoff> {
        ensure!(
            self.state() == NodeState::Idle
                && !self.handle.should_stop()
                && self.recovery_requires_release
                && self.recovery_native_frontier.as_ref() == Some(watermark)
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && self.recovery_timers.is_none(),
            "timer restoration requires this completed paused native source frontier"
        );
        let mut clocks = BTreeMap::from([("kernel".to_owned(), self.kernel.clock.clone())]);
        for (id, clock) in self
            .kernel
            .trader
            .try_borrow()?
            .registered_component_clocks()?
        {
            ensure!(
                clocks.insert(format!("component:{id}"), clock).is_none(),
                "duplicate native clock owner"
            );
        }
        ensure!(
            clocks.keys().eq(source.keys()),
            "source timer owners differ from actual registered clocks"
        );
        let mut clock_addresses = BTreeSet::new();
        for clock in clocks.values() {
            ensure!(
                clock_addresses.insert(Rc::as_ptr(clock) as *const () as usize),
                "aliased owner clocks do not have a unique recovery callback contract"
            );
        }
        let mut ids = BTreeSet::new();
        for (ordinal, input) in pending.iter().enumerate() {
            ensure!(
                input.channel_ordinal == ordinal as u64
                    && ids.insert(input.event_id.to_string())
                    && source.contains_key(&input.owner),
                "invalid retained timer source FIFO or owner"
            );
        }
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<_> {
            let mut restored = BTreeMap::new();
            for (owner, clock) in clocks {
                let receipt = clock
                    .try_borrow_mut()?
                    .restore_running_timer_checkpoint(&source[&owner])?;
                restored.insert(owner, Some(receipt));
            }
            let mut messages = VecDeque::new();
            for input in &pending {
                let event = TimeEvent::new(
                    input.name.as_str().into(),
                    input.event_id,
                    input.ts_event,
                    input.ts_init,
                );
                messages.push_back(
                    restored[&input.owner]
                        .as_ref()
                        .context("native clock receipt missing")?
                        .restore_message(event, input.source_binding_id, input.cleanup)?,
                );
            }
            let progress = RetainedRecoveryTimerHandoff(Rc::new(RefCell::new(TimerProgress {
                watermark: watermark.clone(),
                node_instance_id: self.kernel.instance_id,
                events: messages
                    .iter()
                    .map(|message| {
                        (
                            message.event().event_id.to_string(),
                            RetainedTimerProgressEvent {
                                phase: "retained",
                                expected_event: event_identity(message),
                                actual_callback_binding: message.checkpoint_callback_binding(),
                            },
                        )
                    })
                    .collect(),
            })));
            let node_timers = RetainedNodeTimers {
                clocks: restored,
                source,
                source_pending: pending,
                pending: messages,
                progress: progress.clone(),
            };
            node_timers.verify()?;
            Ok((node_timers, progress))
        }))
        .unwrap_or_else(|_| Err(anyhow::anyhow!("native timer restoration panicked")));
        match outcome {
            Ok((timers, receipt)) => {
                self.recovery_timers = Some(timers);
                Ok(receipt)
            }
            Err(error) => {
                self.kernel.exec_engine.borrow().fence_submissions();
                self.handle.stop();
                Err(error)
            }
        }
    }
    pub(super) fn handoff_recovered_timers(
        &mut self,
        receivers: &mut RunningReceivers<'_>,
    ) -> Result<()> {
        if let Some(timers) = self.recovery_timers.as_mut() {
            timers.handoff_after_start(receivers.time_evt_rx)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::timer::TimeEventCallback;
    #[test]
    fn checkpoint_retained_callback_fifo_cannot_ack_a_different_actual_callback() {
        let actual_now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
        let event = TimeEvent::new("owned".into(), UUID4::new(), actual_now, actual_now);
        let calls = Rc::new(std::cell::Cell::new(0));
        let called = calls.clone();
        let message = TimeEventMessage::new(
            event.clone(),
            TimeEventCallback::RustLocal(Rc::new(move |_| called.set(called.get() + 1))),
        );
        let progress = RetainedRecoveryTimerHandoff(Rc::new(RefCell::new(TimerProgress {
            watermark: RunnerRecoveryWatermark {
                recovery_id: "callback-test".into(),
                checkpoint_sequence: 1,
                dispatch_watermark: 1,
            },
            node_instance_id: UUID4::new(),
            events: BTreeMap::from([(
                event.event_id.to_string(),
                RetainedTimerProgressEvent {
                    phase: "retained",
                    expected_event: event_identity(&message),
                    actual_callback_binding: message.checkpoint_callback_binding(),
                },
            )]),
        })));
        let mut timers = RetainedNodeTimers {
            clocks: BTreeMap::new(),
            source: BTreeMap::new(),
            source_pending: Vec::new(),
            pending: VecDeque::from([message]),
            progress: progress.clone(),
        };
        let (tail_sender, tail_receiver) = tokio::sync::mpsc::unbounded_channel();
        tail_sender
            .send(TimeEventMessage::new(
                TimeEvent::new("tail".into(), UUID4::new(), actual_now, actual_now),
                TimeEventCallback::RustLocal(Rc::new(|_| {})),
            ))
            .unwrap();
        let mut receiver = SnapshotReceiver::from(tail_receiver);
        timers.handoff_after_start(&mut receiver).unwrap();
        assert_eq!(
            progress.progress().unwrap()["events"][event.event_id.to_string()]["phase"],
            "queued"
        );
        let forged =
            TimeEventMessage::new(event.clone(), TimeEventCallback::RustLocal(Rc::new(|_| {})));
        assert!(timers.received(&forged).is_err());
        assert_eq!(
            progress.progress().unwrap()["events"][event.event_id.to_string()]["phase"],
            "queued"
        );
        let actual = receiver.try_recv().unwrap();
        assert_eq!(actual.event(), &event);
        timers.received(&actual).unwrap();
        assert_eq!(
            progress.progress().unwrap()["events"][event.event_id.to_string()]["phase"],
            "received"
        );
        assert!(actual.dispatch());
        timers.processed(event.event_id).unwrap();
        assert_eq!(calls.get(), 1);
        assert_eq!(
            progress.progress().unwrap()["events"][event.event_id.to_string()]["phase"],
            "processed"
        );
        assert_eq!(receiver.try_recv().unwrap().event().name.as_str(), "tail");
    }
}
