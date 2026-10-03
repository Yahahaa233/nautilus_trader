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
#[cfg(feature = "native-tail-replay")]
use std::collections::HashSet;
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
    pub(super) clocks: BTreeMap<String, Option<RefCell<Box<dyn RestoredTimerCheckpoint>>>>,
    #[cfg(feature = "native-tail-replay")]
    actual_clocks: BTreeMap<String, Rc<RefCell<dyn nautilus_common::clock::Clock>>>,
    source: BTreeMap<String, serde_json::Value>,
    source_pending: Vec<RetainedRecoveryTimerInput>,
    pending: RefCell<VecDeque<TimeEventMessage>>,
    #[cfg(feature = "native-tail-replay")]
    historical_admitted: RefCell<HashSet<UUID4>>,
    progress: RetainedRecoveryTimerHandoff,
}
impl RetainedNodeTimers {
    pub(super) fn verify(&self) -> Result<()> {
        for clock in self.clocks.values().flatten() {
            clock.try_borrow()?.verify()?;
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
            .try_borrow()?
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
            .try_borrow()?
            .iter()
            .enumerate()
            .map(|(i, m)| registry.encode(RunnerRecoveryEventRef::TimeEvent(m), i as u64))
            .collect()
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn historical_inventory(&self) -> Result<BTreeMap<String, serde_json::Value>> {
        self.clocks
            .iter()
            .map(|(owner, receipt)| {
                Ok((
                    owner.clone(),
                    receipt
                        .as_ref()
                        .context("historical timer producer was already resumed")?
                        .try_borrow()?
                        .historical_inventory()?,
                ))
            })
            .collect()
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn refresh_historical_dispatch(
        &self,
        expected: &BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        ensure!(
            self.clocks.keys().eq(expected.keys()),
            "historical native clock owners changed"
        );
        for (owner, receipt) in &self.clocks {
            receipt
                .as_ref()
                .context("historical timer owner already resumed")?
                .try_borrow_mut()?
                .refresh_from_historical_dispatch(
                    &*self.actual_clocks[owner].try_borrow()?,
                    &expected[owner],
                )?;
        }
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn apply_historical_inventory(
        &self,
        source: &BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        ensure!(
            self.clocks.keys().eq(source.keys()),
            "historical registered timer owner changed"
        );
        for (owner, receipt) in &self.clocks {
            receipt
                .as_ref()
                .context("historical timer producer was already resumed")?
                .try_borrow()?
                .apply_historical_inventory(&source[owner])?;
        }
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn admit_historical_timers(
        &self,
        before: &BTreeMap<String, serde_json::Value>,
        admissions: &[(String, u64, bool, TimeEvent, u64)],
        captured_at_ns: u64,
    ) -> Result<()> {
        let mut pending = self.pending.try_borrow_mut()?;
        let mut admitted = self.historical_admitted.try_borrow_mut()?;
        let mut incoming = BTreeMap::<String, Vec<(TimeEvent, u64, bool)>>::new();
        let mut incoming_order = Vec::new();
        let mut incoming_ids = HashSet::new();
        for (owner, binding, cleanup, event, accepted_at) in admissions {
            if *accepted_at > captured_at_ns || admitted.contains(&event.event_id) {
                continue;
            }
            if let Some(message) = pending
                .iter()
                .find(|message| message.event().event_id == event.event_id)
            {
                ensure!(
                    message.event() == event
                        && message.native_input_callback_binding()["binding_id"].as_u64()
                            == Some(*binding)
                        && (message.native_input_callback_binding()["kind"]
                            == "registered_cleanup")
                            == *cleanup,
                    "retained original timer admission changed headers or binding"
                );
                admitted.insert(event.event_id);
                continue;
            }
            ensure!(
                incoming_ids.insert(event.event_id),
                "duplicate original timer admission UUID"
            );
            incoming_order.push((event.clone(), *binding, *cleanup));
            incoming
                .entry(owner.clone())
                .or_default()
                .push((event.clone(), *binding, *cleanup));
        }
        ensure!(
            incoming.keys().all(|owner| before.contains_key(owner)),
            "historical timer admission has unknown owner"
        );
        let mut materialized = std::collections::HashMap::new();
        for (owner, inventory) in before {
            let receipt = self
                .clocks
                .get(owner)
                .context("historical timer owner changed")?
                .as_ref()
                .context("historical timer owner resumed")?
                .try_borrow()?;
            let messages = receipt.admit_historical_messages(
                inventory,
                &incoming.remove(owner).unwrap_or_default(),
            )?;
            for message in messages {
                ensure!(
                    materialized
                        .insert(message.event().event_id, message)
                        .is_none(),
                    "duplicate actual owner timer admission UUID"
                );
            }
        }
        // Owner grouping is only a paused lease-acquisition detail. The actual
        // retained Time channel must keep the original cross-owner input FIFO.
        for (event, binding, cleanup) in incoming_order {
            let message = materialized
                .remove(&event.event_id)
                .context("original owner timer message was not materialized")?;
            ensure!(
                message.event() == &event
                    && message.native_input_callback_binding()["binding_id"].as_u64()
                        == Some(binding)
                    && (message.native_input_callback_binding()["kind"] == "registered_cleanup")
                        == cleanup,
                "actual owner timer admission changed original input headers or binding"
            );
            admitted.insert(event.event_id);
            pending.push_back(message);
        }
        ensure!(
            materialized.is_empty(),
            "unarchived actual owner timer admission"
        );
        ensure!(
            self.historical_inventory()? == *before,
            "historical native timer inventory contains an unarchived callback admission"
        );
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn take_historical_retained(&self, id: UUID4) -> Result<TimeEventMessage> {
        let mut pending = self.pending.try_borrow_mut()?;
        ensure!(
            pending
                .front()
                .is_some_and(|message| message.event().event_id == id),
            "sealed original timer input is not the next retained FIFO message"
        );
        let message = pending
            .pop_front()
            .context("original retained timer disappeared")?;
        self.progress
            .0
            .try_borrow_mut()?
            .events
            .remove(&id.to_string());
        Ok(message)
    }
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn historical_time_message(
        &mut self,
        witness: &nautilus_common::recovery_trace::NativeReadWitness,
    ) -> Result<TimeEventMessage> {
        ensure!(
            witness.profile == "actual_registered_owner_timer_input.v1"
                && witness.source_version == "native_actual_registered_clock.v1",
            "unknown historical owner timer witness"
        );
        let input = &witness.payload["input"];
        let owner = input["owner"]
            .as_str()
            .context("historical timer owner absent")?;
        let binding = input["binding"]["binding_id"]
            .as_u64()
            .context("historical timer binding absent")?;
        let cleanup = match input["binding"]["kind"].as_str() {
            Some("registered_owner_thread") => false,
            Some("registered_cleanup") => true,
            _ => anyhow::bail!("historical non-owner callback unsupported"),
        };
        let event = TimeEvent::new(
            input["event"]["name"]
                .as_str()
                .context("historical timer name absent")?
                .into(),
            serde_json::from_value(input["event"]["event_id"].clone())?,
            serde_json::from_value(input["event"]["ts_event"].clone())?,
            serde_json::from_value(input["event"]["ts_init"].clone())?,
        );
        let before: BTreeMap<String, serde_json::Value> =
            serde_json::from_value(witness.payload["inventory"].clone())?;
        self.historical_admitted
            .try_borrow_mut()?
            .insert(event.event_id);
        let mut pending = self.pending.try_borrow_mut()?;
        let retained = pending
            .iter()
            .position(|message| message.event().event_id == event.event_id);
        if let Some(index) = retained {
            ensure!(
                index == 0,
                "historical timer input changed original retained FIFO"
            );
            let message = pending
                .remove(index)
                .context("retained source event disappeared")?;
            ensure!(
                message.event() == &event
                    && message.native_input_callback_binding() == input["binding"],
                "retained timer headers or original owner binding changed"
            );
            self.apply_historical_inventory(&before)?;
            self.progress
                .0
                .try_borrow_mut()?
                .events
                .remove(&event.event_id.to_string());
            return Ok(message);
        }
        // Preserve an actual queued lease even if the source producer closed
        // after reserving its terminal fire. Closure is never undone.
        let mut messages = self
            .clocks
            .get(owner)
            .context("historical timer owner absent")?
            .as_ref()
            .context("historical timer receipt absent")?
            .try_borrow()?
            .admit_historical_messages(&before[owner], &[(event, binding, cleanup)])?;
        ensure!(
            messages.len() == 1,
            "original timer admission count differs"
        );
        Ok(messages.remove(0))
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
                receipt.into_inner().resume()?;
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
        receiver.prepend_retained(self.pending.get_mut())?;
        for state in self.progress.0.try_borrow_mut()?.events.values_mut() {
            ensure!(state.phase == "retained", "timer handoff already attempted");
            state.phase = "queued";
        }
        for receipt in self.clocks.values_mut().filter_map(Option::take) {
            receipt.into_inner().resume()?;
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
            self.kernel
                .portfolio
                .try_borrow_mut()?
                .prepare_equity_curve_timer_recovery(&source["kernel"])?;
            let mut restored = BTreeMap::new();
            for (owner, clock) in &clocks {
                let receipt = clock
                    .try_borrow_mut()?
                    .restore_running_timer_checkpoint(&source[owner])?;
                restored.insert(owner.clone(), Some(RefCell::new(receipt)));
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
                        .try_borrow()?
                        .restore_message(event, input.source_binding_id, input.cleanup)?,
                );
            }
            // Keep the original global FIFO above. Only after every actual
            // queued lease is acquired may a source terminal token close.
            // The complete source counts reject any missing/unarchived lease.
            for (owner, receipt) in &restored {
                receipt
                    .as_ref()
                    .context("native clock receipt missing")?
                    .try_borrow()?
                    .admit_historical_messages(&source[owner], &[])?;
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
                #[cfg(feature = "native-tail-replay")]
                actual_clocks: clocks,
                source,
                source_pending: pending,
                pending: RefCell::new(messages),
                #[cfg(feature = "native-tail-replay")]
                historical_admitted: RefCell::new(HashSet::new()),
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
    #[cfg(feature = "native-tail-replay")]
    #[rstest::rstest]
    #[case("valid")]
    #[case("wrong_front")]
    #[case("changed_binding")]
    #[case("missing_lease")]
    fn checkpoint_actual_cross_owner_timer_admission_keeps_original_fifo(#[case] fault: &str) {
        use crate::runner_recovery::{
            RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryEnvelope, RunnerRecoveryEvent,
        };
        use nautilus_common::{clock::Clock, live::clock::LiveClock, runner::TimeEventSender};
        use nautilus_core::DurationNanos;
        use std::{
            sync::{Arc, mpsc},
            time::Duration,
        };

        #[derive(Debug)]
        struct SourceSender(mpsc::Sender<(TimeEventMessage, u64)>);
        impl TimeEventSender for SourceSender {
            fn send(&self, message: TimeEventMessage) {
                let accepted = nautilus_core::time::duration_since_unix_epoch().as_nanos() as u64;
                self.0.send((message, accepted)).unwrap();
            }
        }
        #[derive(Debug)]
        struct ActualTimerCodec;
        impl RunnerRecoveryCodec for ActualTimerCodec {
            fn channel(&self) -> RunnerRecoveryChannel {
                RunnerRecoveryChannel::TimeEvent
            }
            fn codec_id(&self) -> &str {
                "actual_cross_owner_retained_timer.v1"
            }
            fn encode(&self, input: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
                let RunnerRecoveryEventRef::TimeEvent(message) = input else {
                    anyhow::bail!("only actual owner timer messages supported")
                };
                Ok(serde_json::json!({"event":event_identity(message),
                    "binding":message.native_input_callback_binding()}))
            }
            fn decode(&self, _: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
                anyhow::bail!("borrowed actual owner capture is not an executable JSON callback")
            }
        }
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::TimeEvent]);
        registry
            .register_owner_bound_timer_codec(ActualTimerCodec)
            .unwrap();
        let registry = registry.seal().unwrap();
        let (source_tx, source_rx) = mpsc::channel();
        let mut source_clocks = Vec::new();
        let mut source_messages = Vec::new();
        let mut before = BTreeMap::new();
        let mut admissions = Vec::new();
        // Actual Source producers deliver Z then A. Sorting owner IDs would
        // reverse this single Time channel's real FIFO.
        for owner in ["owner-Z", "owner-A"] {
            let mut clock = LiveClock::new(Some(Arc::new(SourceSender(source_tx.clone()))));
            clock.register_default_handler(TimeEventCallback::RustLocal(Rc::new(|_| {})));
            let due = clock.timestamp_ns() + DurationNanos::from_millis(20);
            clock
                .set_time_alert_ns(owner, due, None, Some(false))
                .unwrap();
            let (message, accepted) = source_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let freeze = clock.freeze_running_timer_checkpoint().unwrap();
            before.insert(owner.to_owned(), freeze.inventory().clone());
            freeze.finish().unwrap();
            let binding = message.checkpoint_callback_binding()["binding_id"]
                .as_u64()
                .unwrap();
            admissions.push((
                owner.to_owned(),
                binding,
                false,
                message.event().clone(),
                accepted,
            ));
            source_messages.push(message);
            source_clocks.push(clock);
        }
        let original_ids = source_messages
            .iter()
            .map(|m| m.event().event_id)
            .collect::<Vec<_>>();
        let calls = Rc::new(RefCell::new(Vec::new()));
        let mut actual_clocks = BTreeMap::new();
        let mut receipts = BTreeMap::new();
        let (target_tx, target_rx) = mpsc::channel();
        for owner in ["owner-Z", "owner-A"] {
            let mut clock = LiveClock::new(Some(Arc::new(SourceSender(target_tx.clone()))));
            let actual_calls = calls.clone();
            clock.register_default_handler(TimeEventCallback::RustLocal(Rc::new(move |event| {
                actual_calls
                    .borrow_mut()
                    .push((owner.to_owned(), event.event_id));
            })));
            let receipt = clock
                .restore_running_timer_checkpoint(&before[owner])
                .unwrap();
            receipts.insert(owner.to_owned(), Some(RefCell::new(receipt)));
            let actual: Rc<RefCell<dyn Clock>> = Rc::new(RefCell::new(clock));
            actual_clocks.insert(owner.to_owned(), actual);
        }
        let progress = RetainedRecoveryTimerHandoff(Rc::new(RefCell::new(TimerProgress {
            watermark: RunnerRecoveryWatermark {
                recovery_id: "actual-cross-owner".into(),
                checkpoint_sequence: 1,
                dispatch_watermark: 1,
            },
            node_instance_id: UUID4::new(),
            events: BTreeMap::new(),
        })));
        let mut timers = RetainedNodeTimers {
            clocks: receipts,
            actual_clocks,
            source: before.clone(),
            source_pending: Vec::new(),
            pending: RefCell::new(VecDeque::new()),
            historical_admitted: RefCell::new(HashSet::new()),
            progress,
        };
        let captured = nautilus_core::time::duration_since_unix_epoch().as_nanos() as u64;
        if fault == "changed_binding" {
            admissions[1].1 += 9999;
        }
        if fault == "missing_lease" {
            before.get_mut("owner-A").unwrap()["timers"][0]["binding"]["state"] =
                (1u64 << 63).into();
        }
        let result = timers.admit_historical_timers(&before, &admissions, captured);
        if matches!(fault, "changed_binding" | "missing_lease") {
            assert!(result.is_err());
            assert!(
                timers.pending.borrow().is_empty(),
                "a partial owner batch cannot publish a queue prefix"
            );
            assert!(calls.borrow().is_empty());
            return;
        }
        result.unwrap();
        let entries = timers.pending_entries(&registry).unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|e| e.channel_ordinal)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(
                entry.payload["event"],
                event_identity(&source_messages[index])
            );
            assert_eq!(
                entry.payload["binding"],
                source_messages[index].checkpoint_callback_binding()
            );
        }
        if fault == "wrong_front" {
            assert!(timers.take_historical_retained(original_ids[1]).is_err());
            assert_eq!(
                timers.pending_entries(&registry).unwrap(),
                entries,
                "wrong original UUID cannot remove a later owner's input"
            );
        }
        let (_current_tx, current_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut receiver = SnapshotReceiver::from(current_rx);
        timers.handoff_after_start(&mut receiver).unwrap();
        for original in &source_messages {
            let message = receiver.try_recv().unwrap();
            assert_eq!(message.event(), original.event());
            assert_eq!(
                message.native_input_callback_binding(),
                original.checkpoint_callback_binding()
            );
            assert!(message.dispatch());
        }
        assert_eq!(
            *calls.borrow(),
            vec![
                ("owner-Z".to_owned(), original_ids[0]),
                ("owner-A".to_owned(), original_ids[1])
            ]
        );
        assert!(receiver.try_recv().is_err());
        assert!(
            target_rx.try_recv().is_err(),
            "source terminal messages must not regenerate"
        );
    }

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
            #[cfg(feature = "native-tail-replay")]
            actual_clocks: BTreeMap::new(),
            source: BTreeMap::new(),
            source_pending: Vec::new(),
            pending: RefCell::new(VecDeque::from([message])),
            #[cfg(feature = "native-tail-replay")]
            historical_admitted: RefCell::new(HashSet::new()),
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
