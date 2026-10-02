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

//! Live clock implementation using Tokio for real-time operations.

use std::{collections::BTreeMap, ops::Deref, sync::Arc};

use anyhow::Context;
use nautilus_core::{
    AtomicTime, DurationNanos, UnixNanos, correctness::check_predicate_true,
    time::get_atomic_clock_realtime,
};
use ustr::Ustr;

use super::timer::LiveTimer;
use crate::{
    clock::{
        CallbackRegistry, Clock, PortfolioEquityCurveTimerCallback, replace_existing_timer,
        validate_and_prepare_time_alert, validate_and_prepare_timer,
    },
    runner::{TimeEventSender, purge_closed_time_event_callbacks, try_get_time_event_sender},
    timer::{TimeEventCallback, create_valid_interval},
};

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TimerRestoreSpec {
    name: String,
    interval_ns: u64,
    start_time_ns: UnixNanos,
    stop_time_ns: Option<UnixNanos>,
    next_time_ns: u64,
    fire_immediately: bool,
    status: String,
    callback_kind: String,
    callback_source: String,
    #[serde(default)]
    callback_profile: Option<serde_json::Value>,
    binding: serde_json::Value,
    execution_authorized: bool,
}

struct LiveRestoredTimers {
    actual_clock_id: nautilus_core::UUID4,
    clock_id: Option<nautilus_core::UUID4>,
    pause: super::checkpoint::PausedCallbacks,
    source: serde_json::Value,
    tokens: BTreeMap<u64, (String, u64, u64, crate::runner::TimeEventCallbackToken)>,
    inspectors: Vec<TimerInspector>,
    updaters: BTreeMap<String, Box<dyn Fn(u64, bool) -> anyhow::Result<()>>>,
    restored: std::cell::RefCell<serde_json::Value>,
}
impl std::fmt::Debug for LiveRestoredTimers {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveRestoredTimers")
            .field("source", &self.source)
            .finish_non_exhaustive()
    }
}
impl crate::clock::RestoredTimerCheckpoint for LiveRestoredTimers {
    fn source_inventory(&self) -> &serde_json::Value {
        &self.source
    }
    fn verify(&self) -> anyhow::Result<()> {
        self.pause.verify()?;
        anyhow::ensure!(
            running_timer_inventory(&self.inspectors, self.clock_id)?
                == *self.restored.try_borrow()?,
            "restored native timer schedules changed while retained"
        );
        Ok(())
    }
    fn restore_message(
        &self,
        event: crate::timer::TimeEvent,
        source_binding_id: u64,
        cleanup: bool,
    ) -> anyhow::Result<crate::runner::TimeEventMessage> {
        self.verify()?;
        let (name, interval, _, token) = self.tokens.get(&source_binding_id).ok_or_else(|| {
            anyhow::anyhow!("source timer callback binding is not in this actual clock")
        })?;
        let inventory = self.historical_inventory()?;
        let next = inventory["timers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|timer| timer["name"].as_str() == Some(name))
            .unwrap()["next_time_ns"]
            .as_u64()
            .unwrap();
        anyhow::ensure!(
            event.name.as_str() == name
                && event.ts_event.as_u64() <= next
                && (next - event.ts_event.as_u64()) % interval == 0,
            "pending time event does not match its source owner schedule"
        );
        let lease = token
            .acquire()
            .ok_or_else(|| anyhow::anyhow!("restored timer callback closed"))?;
        // Acquiring this actual queued message increments the same native token
        // lease count. Only this known change becomes the next retained state.
        *self.restored.try_borrow_mut()? =
            running_timer_inventory(&self.inspectors, self.clock_id)?;
        Ok(if cleanup {
            crate::runner::TimeEventMessage::cleanup(event, lease)
        } else {
            crate::runner::TimeEventMessage::registered(event, lease)
        })
    }
    fn historical_inventory(&self) -> anyhow::Result<serde_json::Value> {
        self.pause.verify()?;
        let mut actual = running_timer_inventory(&self.inspectors, self.clock_id)?;
        for timer in actual["timers"]
            .as_array_mut()
            .ok_or_else(|| anyhow::anyhow!("timer inventory absent"))?
        {
            let name = timer["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("timer name absent"))?;
            let (source_id, (_, _, _, token)) = self
                .tokens
                .iter()
                .find(|(_, (registered, _, _, _))| registered == name)
                .ok_or_else(|| anyhow::anyhow!("actual timer has no retained source binding"))?;
            anyhow::ensure!(
                timer["binding"] == token.checkpoint_inventory(),
                "actual timer callback was replaced"
            );
            timer["binding"]["binding_id"] = (*source_id).into();
        }
        Ok(actual)
    }
    fn apply_historical_inventory(&self, desired: &serde_json::Value) -> anyhow::Result<()> {
        self.pause.verify()?;
        let actual = self.historical_inventory()?;
        let mut unchanged = desired.clone();
        let desired_timers = desired["timers"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("historical timers absent"))?;
        let actual_timers = actual["timers"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("actual timers absent"))?;
        anyhow::ensure!(
            desired_timers.len() == actual_timers.len(),
            "historical timer registration changed"
        );
        let mut updates = Vec::new();
        for (desired_timer, actual_timer) in desired_timers.iter().zip(actual_timers) {
            let name = desired_timer["name"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("historical timer name absent"))?;
            let next = desired_timer["next_time_ns"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("historical next deadline absent"))?;
            let exhausted = match desired_timer["status"].as_str() {
                Some("active") => false,
                Some("exhausted") => true,
                _ => anyhow::bail!("historical timer status unsupported"),
            };
            let replacement = unchanged["timers"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .find(|timer| timer["name"].as_str() == Some(name))
                .unwrap();
            replacement["next_time_ns"] = actual_timer["next_time_ns"].clone();
            replacement["status"] = actual_timer["status"].clone();
            anyhow::ensure!(
                self.updaters.contains_key(name),
                "historical timer owner changed"
            );
            updates.push((name.to_owned(), next, exhausted));
        }
        anyhow::ensure!(
            unchanged == actual,
            "historical timer configuration, owner or pending callback count changed"
        );
        for (name, next, exhausted) in updates {
            self.updaters[&name](next, exhausted)?;
        }
        *self.restored.try_borrow_mut()? =
            running_timer_inventory(&self.inspectors, self.clock_id)?;
        anyhow::ensure!(
            self.historical_inventory()? == *desired,
            "actual historical timer advancement disagrees"
        );
        Ok(())
    }
    fn refresh_after_historical_dispatch(&self) -> anyhow::Result<()> {
        self.pause.verify()?;
        *self.restored.try_borrow_mut()? =
            running_timer_inventory(&self.inspectors, self.clock_id)?;
        Ok(())
    }
    fn refresh_from_historical_dispatch(
        &mut self,
        clock: &dyn Clock,
        expected: &serde_json::Value,
    ) -> anyhow::Result<()> {
        self.pause.verify()?;
        anyhow::ensure!(
            crate::recovery_trace::historical::active(),
            "timer refresh requires original native dispatch"
        );
        let clock = clock
            .as_any()
            .downcast_ref::<LiveClock>()
            .ok_or_else(|| anyhow::anyhow!("historical actual owner is not LiveClock"))?;
        anyhow::ensure!(
            clock.native_clock_id == self.actual_clock_id
                && clock.restored_clock_id == self.clock_id,
            "historical actual clock owner changed"
        );
        let inspectors = clock
            .timers
            .values()
            .map(LiveTimer::checkpoint_inspector)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let mut actual = running_timer_inventory(&inspectors, self.clock_id)?;
        let original = expected["timers"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("original timer registration absent"))?;
        let rows = actual["timers"].as_array_mut().unwrap();
        anyhow::ensure!(
            rows.len() == original.len(),
            "original handler timer registration differs"
        );
        let mut tokens = BTreeMap::new();
        for (row, source) in rows.iter_mut().zip(original) {
            let name = row["name"].as_str().unwrap().to_owned();
            clock.resolve_recovery_callback(
                &name.as_str().into(),
                row["callback_source"]
                    .as_str()
                    .context("actual timer callback source missing")?,
                row.get("callback_profile"),
            )?;
            anyhow::ensure!(
                source["name"] == row["name"]
                    && row["callback_source"] == source["callback_source"]
                    && row.get("callback_profile") == source.get("callback_profile"),
                "historical timer callback owner/profile changed"
            );
            let source_id = source["binding"]["binding_id"]
                .as_u64()
                .ok_or_else(|| anyhow::anyhow!("original timer binding absent"))?;
            let token = clock.timers[&Ustr::from(&name)].registered_checkpoint_token()?;
            // Existing native bindings stay bound to the same source ID. A
            // replacement is allowed only after its actual old token closed.
            if let Some((old_id, (_, _, _, old_token))) = self
                .tokens
                .iter()
                .find(|(_, (registered, _, _, _))| registered == &name)
            {
                if old_token.checkpoint_inventory()["binding_id"] == row["binding"]["binding_id"] {
                    anyhow::ensure!(
                        *old_id == source_id,
                        "original timer binding was reassigned"
                    );
                } else {
                    anyhow::ensure!(
                        old_token.checkpoint_inventory()["state"]
                            .as_u64()
                            .is_some_and(|state| state & (1u64 << 63) != 0),
                        "original timer was replaced before actual cancellation"
                    );
                }
            }
            row["binding"]["binding_id"] = source_id.into();
            // Producer-derived advancement is checked/applied separately from
            // source receipt admissions; all registration/configuration bytes
            // must already come from the actual handler's timer.
            let mut registration = source.clone();
            registration["next_time_ns"] = row["next_time_ns"].clone();
            registration["status"] = row["status"].clone();
            registration["binding"]["state"] = row["binding"]["state"].clone();
            anyhow::ensure!(
                registration == *row,
                "actual historical timer configuration differs"
            );
            anyhow::ensure!(
                tokens
                    .insert(
                        source_id,
                        (
                            name,
                            row["interval_ns"].as_u64().unwrap(),
                            row["next_time_ns"].as_u64().unwrap(),
                            token
                        )
                    )
                    .is_none(),
                "duplicate historical timer source binding"
            );
        }
        for (id, (name, _, _, token)) in &self.tokens {
            if !tokens.contains_key(id) {
                anyhow::ensure!(
                    token.checkpoint_inventory()["state"]
                        .as_u64()
                        .is_some_and(|state| state & (1u64 << 63) != 0),
                    "historical timer disappeared without actual cancellation: {name}"
                );
            }
        }
        self.updaters = clock
            .timers
            .iter()
            .map(|(name, timer)| Ok((name.to_string(), timer.historical_schedule_updater()?)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        self.inspectors = inspectors;
        self.tokens = tokens;
        *self.restored.try_borrow_mut()? =
            running_timer_inventory(&self.inspectors, self.clock_id)?;
        Ok(())
    }
    fn resume(self: Box<Self>) -> anyhow::Result<()> {
        self.verify()?;
        self.pause.finish()
    }
}

/// A real-time clock which uses system time.
///
/// Timestamps are guaranteed to be unique and monotonically increasing.
///
/// # Threading
///
/// The clock holds thread-local runtime state and must remain on its originating thread.
#[derive(Debug)]
pub struct LiveClock {
    native_clock_id: nautilus_core::UUID4,
    restored_clock_id: Option<nautilus_core::UUID4>,
    checkpoint_gate: super::checkpoint::CheckpointGate,
    checkpoint_read_time: std::rc::Rc<std::cell::Cell<Option<UnixNanos>>>,
    time: &'static AtomicTime,
    timers: BTreeMap<Ustr, LiveTimer>,
    callbacks: CallbackRegistry,
    portfolio_callback_profiles: BTreeMap<Ustr, serde_json::Value>,
    sender: Option<Arc<dyn TimeEventSender>>,
    sender_deferred: bool,
}

impl LiveClock {
    /// Creates a new [`LiveClock`] instance.
    #[must_use]
    pub fn new(sender: Option<Arc<dyn TimeEventSender>>) -> Self {
        Self {
            native_clock_id: nautilus_core::UUID4::new(),
            restored_clock_id: None,
            checkpoint_gate: Default::default(),
            checkpoint_read_time: Default::default(),
            time: get_atomic_clock_realtime(),
            timers: BTreeMap::new(),
            callbacks: CallbackRegistry::new(),
            portfolio_callback_profiles: BTreeMap::new(),
            sender,
            sender_deferred: false,
        }
    }

    fn clear_expired_timers(&mut self) {
        self.timers.retain(|_, timer| !timer.is_expired());
        purge_closed_time_event_callbacks();
    }

    fn replace_existing_timer_if_needed(&mut self, name: &Ustr) {
        replace_existing_timer(&mut self.timers, name);
    }

    fn resolve_recovery_callback(
        &self,
        name: &Ustr,
        source: &str,
        profile: Option<&serde_json::Value>,
    ) -> anyhow::Result<(TimeEventCallback, &'static str)> {
        let (callback, source) = match source {
            "registered_clock_default.v1" => {
                anyhow::ensure!(
                    profile.is_none(),
                    "default callback cannot inherit a named factory"
                );
                (
                    self.callbacks
                        .default_handler()
                        .context("actual registered owner default timer callback missing")?,
                    "registered_clock_default.v1",
                )
            }
            PortfolioEquityCurveTimerCallback::SOURCE => {
                let registered = self
                    .portfolio_callback_profiles
                    .get(name)
                    .context("actual Portfolio callback factory/account absent")?;
                anyhow::ensure!(
                    Some(registered) == profile,
                    "Portfolio callback factory/account/configuration changed"
                );
                (
                    self.callbacks
                        .get_callback(name)
                        .context("actual Portfolio callback absent")?,
                    PortfolioEquityCurveTimerCallback::SOURCE,
                )
            }
            _ => anyhow::bail!("source timer callback/owner restoration contract unsupported"),
        };
        anyhow::ensure!(
            callback.is_local(),
            "timer restore requires actual owner-thread callback"
        );
        Ok((callback, source))
    }
}

impl Default for LiveClock {
    /// Creates a new default [`LiveClock`] instance.
    ///
    /// Uses `try_get_time_event_sender()` to allow creation before channels are initialized.
    fn default() -> Self {
        let mut clock = Self::new(try_get_time_event_sender());
        clock.sender_deferred = clock.sender.is_none();
        clock
    }
}

type TimerInspector = Box<dyn Fn() -> anyhow::Result<serde_json::Value>>;

struct LiveClockCheckpoint {
    clock_id: Option<nautilus_core::UUID4>,
    frozen: super::checkpoint::FrozenCallbacks,
    inventory: serde_json::Value,
    inspectors: Vec<TimerInspector>,
    read_time: std::rc::Rc<std::cell::Cell<Option<UnixNanos>>>,
    captured_time: UnixNanos,
}
impl std::fmt::Debug for LiveClockCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LiveClockCheckpoint")
            .field("inventory", &self.inventory)
            .finish()
    }
}
fn running_timer_inventory(
    inspectors: &[TimerInspector],
    clock_id: Option<nautilus_core::UUID4>,
) -> anyhow::Result<serde_json::Value> {
    let timers = inspectors
        .iter()
        .map(|inspect| inspect())
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut value =
        serde_json::json!({"profile":"live_clock_frozen_registered_schedules.v1","timers":timers});
    if let Some(clock_id) = clock_id {
        value["native_clock_id"] = serde_json::to_value(clock_id)?;
    }
    Ok(value)
}
impl crate::clock::TimerCheckpoint for LiveClockCheckpoint {
    fn inventory(&self) -> &serde_json::Value {
        &self.inventory
    }
    fn verify(&self) -> anyhow::Result<()> {
        self.frozen.verify()?;
        anyhow::ensure!(
            self.read_time.get() == Some(self.captured_time),
            "clock checkpoint read view changed"
        );
        anyhow::ensure!(
            running_timer_inventory(&self.inspectors, self.clock_id)? == self.inventory,
            "registered timer state changed during checkpoint"
        );
        self.frozen.verify()
    }
    fn pause_read_view(&self) -> anyhow::Result<()> {
        self.verify()?;
        self.read_time.set(None);
        Ok(())
    }
    fn resume_read_view(&self) -> anyhow::Result<()> {
        self.frozen.verify()?;
        anyhow::ensure!(
            self.read_time.get().is_none(),
            "actual clock read scope changed"
        );
        self.read_time.set(Some(self.captured_time));
        self.verify()
    }
    fn finish(self: Box<Self>) -> anyhow::Result<()> {
        self.verify()?;
        let read_time = self.read_time.clone();
        self.frozen.finish()?;
        // Do not set or rewind AtomicTime. Readers resume the actual real clock.
        read_time.set(None);
        Ok(())
    }
    fn finish_terminal(self: Box<Self>) -> anyhow::Result<()> {
        self.verify()?;
        let read_time = self.read_time.clone();
        self.frozen.finish_terminal()?;
        read_time.set(None);
        Ok(())
    }
}

impl Deref for LiveClock {
    type Target = AtomicTime;

    fn deref(&self) -> &Self::Target {
        self.time
    }
}

impl Clock for LiveClock {
    fn register_portfolio_equity_curve_callback(
        &mut self,
        binding: PortfolioEquityCurveTimerCallback,
    ) -> anyhow::Result<bool> {
        anyhow::ensure!(
            binding.callback.is_local(),
            "Portfolio callback is not owner-thread local"
        );
        if let Some(previous) = self.portfolio_callback_profiles.get(&binding.name) {
            anyhow::ensure!(
                previous == &binding.profile,
                "Portfolio callback registration source changed"
            );
        }
        self.callbacks
            .register_callback(binding.name, binding.callback);
        self.portfolio_callback_profiles
            .insert(binding.name, binding.profile);
        Ok(true)
    }
    fn restore_running_timer_checkpoint(
        &mut self,
        inventory: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn crate::clock::RestoredTimerCheckpoint>> {
        anyhow::ensure!(
            self.timers.is_empty() && self.checkpoint_read_time.get().is_none(),
            "timer restoration requires an empty actual clock outside a capture"
        );
        anyhow::ensure!(
            inventory.get("profile").and_then(|v| v.as_str())
                == Some("live_clock_frozen_registered_schedules.v1"),
            "unsupported native timer source profile"
        );
        let entries = inventory
            .get("timers")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow::anyhow!("source timer entries missing"))?;
        let specs = entries
            .iter()
            .cloned()
            .map(serde_json::from_value::<TimerRestoreSpec>)
            .collect::<Result<Vec<_>, _>>()?;
        let mut names = std::collections::BTreeSet::new();
        let mut bindings = std::collections::BTreeSet::new();
        for spec in &specs {
            anyhow::ensure!(
                !spec.name.trim().is_empty()
                    && names.insert(spec.name.clone())
                    && spec.interval_ns > 0
                    && spec.next_time_ns > 0
                    && !spec.execution_authorized,
                "invalid or duplicate source timer schedule"
            );
            anyhow::ensure!(
                spec.callback_kind == "registered_owner_thread"
                    && matches!(spec.status.as_str(), "active" | "exhausted"),
                "source timer callback/owner restoration contract unsupported"
            );
            self.resolve_recovery_callback(
                &spec.name.as_str().into(),
                &spec.callback_source,
                spec.callback_profile.as_ref(),
            )?;
            let id = spec
                .binding
                .get("binding_id")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow::anyhow!("source callback binding missing"))?;
            anyhow::ensure!(
                id > 0
                    && bindings.insert(id)
                    && spec
                        .binding
                        .get("owner_thread_only")
                        .and_then(|v| v.as_bool())
                        == Some(true),
                "source callback owner binding invalid"
            );
            if spec.status == "active"
                && let Some(stop) = spec.stop_time_ns
            {
                anyhow::ensure!(
                    spec.next_time_ns <= stop.as_u64(),
                    "active source timer follows stop bound"
                );
            }
        }
        let restored_clock_id = inventory
            .get("native_clock_id")
            .cloned()
            .map(serde_json::from_value)
            .transpose()?;
        let sender = self.resolve_time_event_sender();
        anyhow::ensure!(
            specs.is_empty() || sender.is_some(),
            "actual timer event sender missing"
        );
        let pause = self.checkpoint_gate.pause_producers()?;
        let mut tokens = BTreeMap::new();
        for spec in specs {
            let (callback, callback_source) = self.resolve_recovery_callback(
                &spec.name.as_str().into(),
                &spec.callback_source,
                spec.callback_profile.as_ref(),
            )?;
            let mut timer = LiveTimer::new(
                Ustr::from(spec.name.as_str()),
                std::num::NonZeroU64::new(spec.interval_ns).unwrap(),
                spec.start_time_ns,
                spec.stop_time_ns,
                callback,
                spec.fire_immediately,
                sender.clone(),
            )
            .with_callback_source(callback_source)
            .with_callback_profile(spec.callback_profile)
            .with_checkpoint_gate(self.checkpoint_gate.clone());
            timer.start_restored(
                UnixNanos::from(spec.next_time_ns),
                spec.status == "exhausted",
            );
            let id = spec.binding["binding_id"].as_u64().unwrap();
            tokens.insert(
                id,
                (
                    spec.name.clone(),
                    spec.interval_ns,
                    spec.next_time_ns,
                    timer.registered_checkpoint_token()?,
                ),
            );
            self.timers.insert(Ustr::from(spec.name.as_str()), timer);
        }
        let updaters = self
            .timers
            .iter()
            .map(|(name, timer)| Ok((name.to_string(), timer.historical_schedule_updater()?)))
            .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
        let inspectors = self
            .timers
            .values()
            .map(LiveTimer::checkpoint_inspector)
            .collect::<anyhow::Result<Vec<_>>>()?;
        self.restored_clock_id = restored_clock_id;
        let restored = running_timer_inventory(&inspectors, restored_clock_id)?;
        Ok(Box::new(LiveRestoredTimers {
            actual_clock_id: self.native_clock_id,
            clock_id: restored_clock_id,
            pause,
            source: inventory.clone(),
            tokens,
            inspectors,
            updaters,
            restored: std::cell::RefCell::new(restored),
        }))
    }
    fn freeze_running_timer_checkpoint(
        &self,
    ) -> anyhow::Result<Box<dyn crate::clock::TimerCheckpoint>> {
        let frozen = self.checkpoint_gate.freeze()?;
        anyhow::ensure!(
            self.checkpoint_read_time.get().is_none(),
            "clock checkpoint read view already active"
        );
        let captured_time = self.time.get_time_ns();
        self.checkpoint_read_time.set(Some(captured_time));
        let inspectors = self
            .timers
            .values()
            .map(LiveTimer::checkpoint_inspector)
            .collect::<anyhow::Result<Vec<_>>>()?;
        let clock_id = Some(self.restored_clock_id.unwrap_or(self.native_clock_id));
        let inventory = running_timer_inventory(&inspectors, clock_id)?;
        frozen.verify()?;
        Ok(Box::new(LiveClockCheckpoint {
            clock_id,
            frozen,
            inventory,
            inspectors,
            read_time: self.checkpoint_read_time.clone(),
            captured_time,
        }))
    }

    fn timestamp_ns(&self) -> UnixNanos {
        let actual = self
            .checkpoint_read_time
            .get()
            .unwrap_or_else(|| self.time.get_time_ns());
        UnixNanos::from(crate::recovery_trace::native_clock_read(
            self.restored_clock_id.unwrap_or(self.native_clock_id),
            "timestamp_ns",
            actual.as_u64(),
        ))
    }

    fn timestamp_us(&self) -> u64 {
        let actual = self
            .checkpoint_read_time
            .get()
            .map_or_else(|| self.time.get_time_us(), |time| time.as_u64() / 1_000);
        crate::recovery_trace::native_clock_read(
            self.restored_clock_id.unwrap_or(self.native_clock_id),
            "timestamp_us",
            actual,
        )
    }

    fn timestamp_ms(&self) -> u64 {
        let actual = self
            .checkpoint_read_time
            .get()
            .map_or_else(|| self.time.get_time_ms(), |time| time.as_u64() / 1_000_000);
        crate::recovery_trace::native_clock_read(
            self.restored_clock_id.unwrap_or(self.native_clock_id),
            "timestamp_ms",
            actual,
        )
    }

    fn timestamp(&self) -> f64 {
        let actual = self.checkpoint_read_time.get().map_or_else(
            || self.time.get_time(),
            |time| time.as_u64() as f64 / 1_000_000_000.0,
        );
        crate::recovery_trace::native_clock_read(
            self.restored_clock_id.unwrap_or(self.native_clock_id),
            "timestamp",
            actual,
        )
    }

    fn timer_names(&self) -> Vec<&str> {
        self.timers
            .iter()
            .filter(|(_, timer)| !timer.is_expired())
            .map(|(k, _)| k.as_str())
            .collect()
    }

    fn timer_count(&self) -> usize {
        self.timers
            .iter()
            .filter(|(_, timer)| !timer.is_expired())
            .count()
    }

    fn timer_exists(&self, name: &Ustr) -> bool {
        self.timers
            .get(name)
            .is_some_and(|timer| !timer.is_expired())
    }

    fn register_default_handler(&mut self, handler: TimeEventCallback) {
        self.callbacks.register_default_handler(handler);
    }

    fn cancel_default_handler(&mut self) {
        self.callbacks.cancel_default_handler();
    }

    fn cancel_callbacks(&mut self) {
        self.callbacks.clear();
        self.portfolio_callback_profiles.clear();
    }

    fn set_time_alert_ns(
        &mut self,
        name: &str,
        alert_time_ns: UnixNanos,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
    ) -> anyhow::Result<()> {
        // Timer decisions use the same recorded owner clock read as callbacks.
        let ts_now = Clock::timestamp_ns(self);
        let (name, alert_time_ns) =
            validate_and_prepare_time_alert(name, alert_time_ns, allow_past, ts_now)?;

        check_predicate_true(
            callback.is_some() | self.callbacks.has_any_callback(&name),
            "No callbacks provided",
        )?;

        self.replace_existing_timer_if_needed(&name);

        if callback.is_some() {
            self.portfolio_callback_profiles.remove(&name);
        }
        let callback_profile = self.portfolio_callback_profiles.get(&name).cloned();
        let callback_source = if callback_profile.is_some() {
            PortfolioEquityCurveTimerCallback::SOURCE
        } else {
            self.callbacks.callback_source(&name, callback.is_some())
        };
        let callback = if let Some(callback) = callback {
            self.callbacks.register_callback(name, callback.clone());
            callback
        } else {
            self.callbacks
                .get_callback(&name)
                .expect("Callback should exist")
        };

        // Safe to calculate interval now that we've ensured alert_time_ns >= ts_now
        let interval_ns = create_valid_interval(alert_time_ns - ts_now);
        let fire_immediately = alert_time_ns == ts_now;
        let sender = self.resolve_time_event_sender();

        let mut timer = LiveTimer::new(
            name,
            interval_ns,
            ts_now,
            Some(alert_time_ns),
            callback,
            fire_immediately,
            sender,
        )
        .with_checkpoint_gate(self.checkpoint_gate.clone())
        .with_callback_source(callback_source)
        .with_callback_profile(callback_profile);

        timer.start();

        self.clear_expired_timers();
        self.timers.insert(name, timer);

        Ok(())
    }

    fn set_timer_ns(
        &mut self,
        name: &str,
        interval_ns: DurationNanos,
        start_time_ns: Option<UnixNanos>,
        stop_time_ns: Option<UnixNanos>,
        callback: Option<TimeEventCallback>,
        allow_past: Option<bool>,
        fire_immediately: Option<bool>,
    ) -> anyhow::Result<()> {
        // Timer decisions use the same recorded owner clock read as callbacks.
        let ts_now = Clock::timestamp_ns(self);
        let (name, start_time_ns, stop_time_ns, _allow_past, fire_immediately) =
            validate_and_prepare_timer(
                name,
                interval_ns,
                start_time_ns,
                stop_time_ns,
                allow_past,
                fire_immediately,
                ts_now,
            )?;

        check_predicate_true(
            callback.is_some() | self.callbacks.has_any_callback(&name),
            "No callbacks provided",
        )?;

        self.replace_existing_timer_if_needed(&name);

        if callback.is_some() {
            self.portfolio_callback_profiles.remove(&name);
        }
        let callback_profile = self.portfolio_callback_profiles.get(&name).cloned();
        let callback_source = if callback_profile.is_some() {
            PortfolioEquityCurveTimerCallback::SOURCE
        } else {
            self.callbacks.callback_source(&name, callback.is_some())
        };
        let callback = if let Some(callback) = callback {
            self.callbacks.register_callback(name, callback.clone());
            callback
        } else {
            self.callbacks
                .get_callback(&name)
                .expect("Callback should exist")
        };

        let interval_ns = create_valid_interval(interval_ns);
        let sender = self.resolve_time_event_sender();

        let mut timer = LiveTimer::new(
            name,
            interval_ns,
            start_time_ns,
            stop_time_ns,
            callback,
            fire_immediately,
            sender,
        )
        .with_checkpoint_gate(self.checkpoint_gate.clone())
        .with_callback_source(callback_source)
        .with_callback_profile(callback_profile);
        timer.start();

        self.clear_expired_timers();
        self.timers.insert(name, timer);

        Ok(())
    }

    fn next_time_ns(&self, name: &str) -> Option<UnixNanos> {
        self.timers
            .get(&Ustr::from(name))
            .filter(|timer| !timer.is_expired())
            .map(LiveTimer::next_time_ns)
    }

    fn cancel_timer(&mut self, name: &str) {
        let timer = self.timers.remove(&Ustr::from(name));
        if let Some(mut timer) = timer {
            timer.cancel();
        }
    }

    fn cancel_timers(&mut self) {
        for timer in &mut self.timers.values_mut() {
            timer.cancel();
        }

        self.timers.clear();
    }

    fn reset(&mut self) {
        self.cancel_timers();
        self.callbacks.clear();
    }
}

impl LiveClock {
    fn resolve_time_event_sender(&mut self) -> Option<Arc<dyn TimeEventSender>> {
        if self.sender.is_none() && self.sender_deferred {
            self.sender = try_get_time_event_sender();
        }

        self.sender.clone()
    }
}

#[cfg(test)]
#[cfg(not(all(feature = "simulation", madsim)))]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
            mpsc,
        },
        time::Duration,
    };

    use nautilus_core::{DurationNanos, UnixNanos, time::get_atomic_clock_realtime};
    use parking_lot::Mutex;
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        clock::Clock,
        runner::{TimeEventMessage, TimeEventSender, replace_time_event_sender},
        testing::wait_until,
        timer::{TimeEvent, TimeEventCallback},
    };

    #[derive(Debug)]
    struct CollectingSender {
        events: Arc<Mutex<Vec<(TimeEvent, UnixNanos)>>>,
    }

    impl CollectingSender {
        fn new(events: Arc<Mutex<Vec<(TimeEvent, UnixNanos)>>>) -> Self {
            Self { events }
        }
    }

    impl TimeEventSender for CollectingSender {
        fn send(&self, message: TimeEventMessage) {
            let now_ns = get_atomic_clock_realtime().get_time_ns();
            let event = message.event().clone();
            message.dispatch();
            self.events.lock().push((event, now_ns));
        }
    }

    #[derive(Debug)]
    struct PausingCollectingSender {
        collector: CollectingSender,
        paused_tx: mpsc::Sender<()>,
        release_rx: Mutex<mpsc::Receiver<()>>,
        pause_once: AtomicBool,
    }

    impl PausingCollectingSender {
        fn new(
            events: Arc<Mutex<Vec<(TimeEvent, UnixNanos)>>>,
        ) -> (Arc<Self>, mpsc::Receiver<()>, mpsc::Sender<()>) {
            let (paused_tx, paused_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            let sender = Arc::new(Self {
                collector: CollectingSender::new(events),
                paused_tx,
                release_rx: Mutex::new(release_rx),
                pause_once: AtomicBool::new(true),
            });
            (sender, paused_rx, release_tx)
        }
    }

    impl TimeEventSender for PausingCollectingSender {
        fn send(&self, message: TimeEventMessage) {
            self.collector.send(message);

            if self.pause_once.swap(false, Ordering::SeqCst) {
                self.paused_tx.send(()).expect("timer send should pause");
                self.release_rx
                    .lock()
                    .recv()
                    .expect("timer send should release");
            }
        }
    }

    fn wait_for_events(
        events: &Arc<Mutex<Vec<(TimeEvent, UnixNanos)>>>,
        target: usize,
        timeout: Duration,
    ) {
        wait_until(|| events.lock().len() >= target, timeout);
    }

    #[rstest]
    fn test_live_clock_timer_replacement_cancels_previous_task() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (sender, paused_rx, release_tx) = PausingCollectingSender::new(Arc::clone(&events));

        let mut clock = LiveClock::new(Some(sender));
        clock.register_default_handler(TimeEventCallback::from(|_| {}));

        let fast_interval = DurationNanos::from_millis(10);
        clock
            .set_timer_ns("replace", fast_interval, None, None, None, None, None)
            .unwrap();

        paused_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("fast timer send should pause");
        events.lock().clear();

        let slow_interval = DurationNanos::from_millis(30);
        clock
            .set_timer_ns("replace", slow_interval, None, None, None, None, None)
            .unwrap();
        release_tx.send(()).expect("fast timer send should release");

        wait_for_events(&events, 3, Duration::from_secs(2));

        let snapshot = events.lock().clone();
        let diffs: Vec<DurationNanos> = snapshot
            .array_windows()
            .map(|[a, b]| b.0.ts_event - a.0.ts_event)
            .collect();

        assert!(!diffs.is_empty());
        for diff in diffs {
            assert_eq!(diff, slow_interval);
        }

        clock.cancel_timers();
    }

    #[derive(Debug)]
    struct CheckpointQueuedSender(mpsc::Sender<TimeEventMessage>);
    impl TimeEventSender for CheckpointQueuedSender {
        fn send(&self, message: TimeEventMessage) {
            self.0
                .send(message)
                .expect("checkpoint timer receiver alive");
        }
    }

    #[rstest]
    fn active_registered_timer_checkpoint_retains_nominal_event_until_release() {
        let (sender, receiver) = mpsc::channel();
        let callback_count = std::rc::Rc::new(std::cell::Cell::new(0));
        let called = callback_count.clone();
        let mut clock = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(sender))));
        let due = clock.timestamp_ns() + DurationNanos::from_millis(25);
        clock
            .set_time_alert_ns(
                "checkpoint-owned-alert",
                due,
                Some(TimeEventCallback::RustLocal(std::rc::Rc::new(move |_| {
                    called.set(called.get() + 1)
                }))),
                None,
            )
            .unwrap();
        let guard = clock.freeze_running_timer_checkpoint().unwrap();
        let read_time = clock.timestamp_ns();
        assert_eq!(
            guard.inventory()["timers"][0]["callback_kind"],
            "registered_owner_thread"
        );
        std::thread::sleep(Duration::from_millis(60));
        assert!(
            receiver.try_recv().is_err(),
            "frozen actual timer emitted a callback"
        );
        guard.verify().unwrap();
        assert_eq!(
            clock.timestamp_ns(),
            read_time,
            "component save reads must share the actual boundary time"
        );
        assert!(
            get_atomic_clock_realtime().get_time_ns() > read_time,
            "underlying actual clock was not advanced by the read view"
        );
        assert_eq!(callback_count.get(), 0);
        guard.pause_read_view().unwrap();
        assert!(
            clock.timestamp_ns() > read_time,
            "independent freshness checks must see actual now"
        );
        assert!(
            receiver.try_recv().is_err(),
            "actual time reads must not open timer producers"
        );
        guard.resume_read_view().unwrap();
        assert_eq!(
            clock.timestamp_ns(),
            read_time,
            "only capture reads resume the same boundary view"
        );
        guard.verify().unwrap();
        guard.finish().unwrap();
        assert!(
            clock.timestamp_ns() > read_time,
            "release must resume actual time without rollback"
        );
        let event = receiver.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(event.event().ts_event, due);
        event.dispatch();
        assert_eq!(callback_count.get(), 1);
        clock.cancel_timers();
    }

    #[rstest]
    #[case(false, false)]
    #[case(true, false)]
    #[case(false, true)]
    #[case(true, true)]
    fn actual_checkpoint_timer_restore_preserves_overdue_frontier_and_owner_callback_fifo(
        #[case] advance_history: bool,
        #[case] portfolio_named: bool,
    ) {
        let (source_tx, source_rx) = mpsc::channel();
        let mut source = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(source_tx))));
        let account = nautilus_model::identifiers::AccountId::from("SOURCE-001");
        let configuration = serde_json::json!({"equity_curve":true,"bar_updates":true});
        let source_callback = TimeEventCallback::RustLocal(std::rc::Rc::new(|_| {}));
        let name = if portfolio_named {
            source
                .register_portfolio_equity_curve_callback(
                    PortfolioEquityCurveTimerCallback::with_counter_for_test(
                        account,
                        &configuration,
                        source_callback,
                    )
                    .unwrap(),
                )
                .unwrap();
            "portfolio_equity_curve.SOURCE-001"
        } else {
            source.register_default_handler(source_callback);
            "same-name"
        };
        source
            .set_timer_ns(
                name,
                DurationNanos::from_millis(100),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
        let source_message = source_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let source_event = source_message.event().clone();
        let source_binding = source_message.checkpoint_callback_binding()["binding_id"]
            .as_u64()
            .unwrap();
        let freeze = source.freeze_running_timer_checkpoint().unwrap();
        let inventory = freeze.inventory().clone();
        let nominal_next = inventory["timers"][0]["next_time_ns"].as_u64().unwrap();
        freeze.finish().unwrap();
        source.cancel_timers();

        let (restored_tx, restored_rx) = mpsc::channel();
        let count = std::rc::Rc::new(std::cell::Cell::new(0));
        let called = count.clone();
        let mut restored =
            LiveClock::new(Some(Arc::new(CheckpointQueuedSender(restored_tx.clone()))));
        let target_callback =
            TimeEventCallback::RustLocal(std::rc::Rc::new(move |_| called.set(called.get() + 1)));
        if portfolio_named {
            restored
                .register_portfolio_equity_curve_callback(
                    PortfolioEquityCurveTimerCallback::with_counter_for_test(
                        account,
                        &configuration,
                        target_callback,
                    )
                    .unwrap(),
                )
                .unwrap();
        } else {
            restored.register_default_handler(target_callback);
        }
        let receipt = restored
            .restore_running_timer_checkpoint(&inventory)
            .unwrap();
        std::thread::sleep(Duration::from_millis(220));
        assert!(restored.timestamp_ns().as_u64() > nominal_next);
        assert!(
            restored_rx.try_recv().is_err(),
            "restored producer must remain paused"
        );
        assert_eq!(
            restored.next_time_ns(name).unwrap().as_u64(),
            nominal_next,
            "recovery must not clamp an overdue source frontier to now"
        );
        let frozen = restored.freeze_running_timer_checkpoint().unwrap();
        frozen.verify().unwrap();
        receipt.verify().unwrap();
        frozen.finish().unwrap();
        assert!(
            receipt
                .restore_message(source_event.clone(), source_binding + 9999, false)
                .is_err()
        );
        let pending = receipt
            .restore_message(source_event.clone(), source_binding, false)
            .unwrap();
        assert_ne!(
            pending.checkpoint_callback_binding()["binding_id"]
                .as_u64()
                .unwrap(),
            source_binding
        );
        restored_tx.send(pending).unwrap();
        let expected_next = if advance_history {
            let mut advanced = receipt.historical_inventory().unwrap();
            let timer = &mut advanced["timers"][0];
            let next = nominal_next + timer["interval_ns"].as_u64().unwrap() * 4;
            timer["next_time_ns"] = next.into();
            receipt.apply_historical_inventory(&advanced).unwrap();
            receipt.verify().unwrap();
            next
        } else {
            nominal_next
        };
        receipt.resume().unwrap();
        let first = restored_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(first.event(), &source_event);
        assert!(first.dispatch());
        let next = restored_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(next.event().ts_event.as_u64(), expected_next);
        assert!(next.dispatch());
        assert_eq!(count.get(), 2);
        restored.cancel_timers();
        drop(source_message);
    }

    #[rstest]
    fn checkpoint_portfolio_equity_curve_rejects_unregistered_native_callback_factory() {
        let result = PortfolioEquityCurveTimerCallback::new(
            nautilus_model::identifiers::AccountId::from("SOURCE-001"),
            &serde_json::json!({"equity_curve":true}),
            std::rc::Rc::new(|_: crate::timer::TimeEvent| {}),
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("original private native factory")
        );
    }

    #[rstest]
    #[case("account")]
    #[case("configuration")]
    #[case("factory")]
    #[case("legacy_unknown")]
    fn checkpoint_portfolio_equity_curve_timer_rejects_changed_or_unknown_factory(
        #[case] changed: &str,
    ) {
        let account = nautilus_model::identifiers::AccountId::from("SOURCE-001");
        let configuration = serde_json::json!({"equity_curve":true});
        let (sender, _) = mpsc::channel();
        let mut source = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(sender))));
        source
            .register_portfolio_equity_curve_callback(
                PortfolioEquityCurveTimerCallback::with_counter_for_test(
                    account,
                    &configuration,
                    TimeEventCallback::RustLocal(std::rc::Rc::new(|_| {})),
                )
                .unwrap(),
            )
            .unwrap();
        source
            .set_timer_ns(
                "portfolio_equity_curve.SOURCE-001",
                DurationNanos::from_secs(60),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
        let frozen = source.freeze_running_timer_checkpoint().unwrap();
        let mut inventory = frozen.inventory().clone();
        frozen.finish().unwrap();
        source.cancel_timers();
        let (sender, _) = mpsc::channel();
        let mut target = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(sender))));
        target
            .register_portfolio_equity_curve_callback(
                PortfolioEquityCurveTimerCallback::with_counter_for_test(
                    account,
                    &configuration,
                    TimeEventCallback::RustLocal(std::rc::Rc::new(|_| {})),
                )
                .unwrap(),
            )
            .unwrap();
        let row = &mut inventory["timers"][0];
        match changed {
            "account" => row["callback_profile"]["account_id"] = "OTHER-001".into(),
            "configuration" => {
                row["callback_profile"]["configuration"]["equity_curve"] = false.into()
            }
            "factory" => row["callback_profile"]["factory"] = "unregistered_factory".into(),
            "legacy_unknown" => {
                row["callback_source"] = "named_or_explicit_unsupported".into();
                row.as_object_mut().unwrap().remove("callback_profile");
            }
            _ => unreachable!(),
        }
        let error = target
            .restore_running_timer_checkpoint(&inventory)
            .unwrap_err();
        assert!(error.to_string().contains(if changed == "legacy_unknown" {
            "restoration contract unsupported"
        } else {
            "factory/account/configuration changed"
        }));
        assert_eq!(target.timer_count(), 0);
    }

    #[rstest]
    fn checkpoint_timer_restore_rejects_unregistered_or_changed_callback_contract() {
        let (sender, _) = mpsc::channel();
        let mut source = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(sender))));
        source.register_default_handler(TimeEventCallback::RustLocal(std::rc::Rc::new(|_| {})));
        source
            .set_timer_ns(
                "future",
                DurationNanos::from_secs(60),
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();
        let frozen = source.freeze_running_timer_checkpoint().unwrap();
        let inventory = frozen.inventory().clone();
        frozen.finish().unwrap();
        source.cancel_timers();
        let (sender, _) = mpsc::channel();
        let mut target = LiveClock::new(Some(Arc::new(CheckpointQueuedSender(sender))));
        assert!(target.restore_running_timer_checkpoint(&inventory).is_err());
        target.register_default_handler(TimeEventCallback::RustLocal(std::rc::Rc::new(|_| {})));
        let mut changed = inventory;
        changed["timers"][0]["callback_source"] = serde_json::json!("unknown-source");
        assert!(target.restore_running_timer_checkpoint(&changed).is_err());
        assert_eq!(target.timer_count(), 0);
    }

    #[rstest]
    fn test_live_clock_time_alert_persists_callback() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sender = Arc::new(CollectingSender::new(Arc::clone(&events)));

        let mut clock = LiveClock::new(Some(sender));
        clock.register_default_handler(TimeEventCallback::from(|_| {}));

        let now = clock.timestamp_ns();
        let alert_time = now + DurationNanos::from_mins(1);

        clock
            .set_time_alert_ns("alert-callback", alert_time, None, None)
            .unwrap();

        assert!(
            clock
                .callbacks
                .has_any_callback(&Ustr::from("alert-callback"))
        );

        clock.cancel_timers();
    }

    #[rstest]
    fn test_default_live_clock_resolves_sender_after_initialization() {
        std::thread::spawn(|| {
            let events = Arc::new(Mutex::new(Vec::new()));
            let sender = Arc::new(CollectingSender::new(Arc::clone(&events)));
            let mut clock = LiveClock::default();
            assert!(clock.sender.is_none());

            replace_time_event_sender(sender);
            let mut explicit_senderless = LiveClock::new(None);
            assert!(explicit_senderless.resolve_time_event_sender().is_none());

            let alert_time = clock.timestamp_ns();
            clock
                .set_time_alert_ns(
                    "late-sender",
                    alert_time,
                    Some(TimeEventCallback::from(|_| {})),
                    None,
                )
                .unwrap();
            wait_for_events(&events, 1, Duration::from_secs(2));

            assert!(clock.sender.is_some());
            let events = events.lock();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].0.name, Ustr::from("late-sender"));
        })
        .join()
        .expect("live clock sender test thread should join");
    }

    #[rstest]
    fn test_live_clock_reset_stops_active_timers() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (sender, paused_rx, release_tx) = PausingCollectingSender::new(Arc::clone(&events));

        let mut clock = LiveClock::new(Some(sender.clone()));

        clock
            .set_timer_ns(
                "reset-test",
                DurationNanos::from_millis(15),
                None,
                None,
                Some(TimeEventCallback::from(|_| {})),
                None,
                None,
            )
            .unwrap();

        paused_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timer send should pause");

        assert_eq!(events.lock().len(), 1);
        assert_eq!(clock.timer_count(), 1);

        clock.reset();
        release_tx.send(()).expect("timer send should release");

        // Only the test and clock retain the sender after the canceled task exits
        wait_until(|| Arc::strong_count(&sender) == 2, Duration::from_secs(2));

        assert_eq!(events.lock().len(), 1);
        assert_eq!(clock.timer_count(), 0);
        assert!(clock.timer_names().is_empty());
        assert!(!clock.callbacks.has_any_callback(&Ustr::from("reset-test")));
    }

    #[rstest]
    fn test_live_clock_timer_exists_consistent_after_expiry() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let (sender, paused_rx, release_tx) = PausingCollectingSender::new(Arc::clone(&events));

        let mut clock = LiveClock::new(Some(sender));
        clock.register_default_handler(TimeEventCallback::from(|_| {}));

        let name = Ustr::from("expiring");
        let interval_ns = DurationNanos::from_millis(10);
        let start_time = clock.timestamp_ns();
        let stop_time = start_time + DurationNanos::from_millis(30);

        clock
            .set_timer_ns(
                name.as_str(),
                interval_ns,
                Some(start_time),
                Some(stop_time),
                None,
                None,
                None,
            )
            .unwrap();

        paused_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timer send should pause");

        assert!(clock.timer_exists(&name));
        release_tx.send(()).expect("timer send should release");

        // Wait for the timer task to run past its stop time and finish
        wait_until(|| clock.timer_count() == 0, Duration::from_secs(2));

        // An expired timer is purged only lazily on the next set/cancel call,
        // so the entry still sits in the map; the introspection surfaces
        // must nevertheless agree it is gone
        assert!(clock.timers.contains_key(&name));
        assert!(!clock.timer_exists(&name));
        assert_eq!(clock.timer_count(), 0);
        assert!(clock.timer_names().is_empty());
        assert!(clock.next_time_ns(name.as_str()).is_none());
    }

    #[rstest]
    fn test_live_clock_failed_set_time_alert_ns_preserves_existing_timer() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let sender = Arc::new(CollectingSender::new(Arc::clone(&events)));

        // No default handler registered
        let mut clock = LiveClock::new(Some(sender));

        let now = clock.timestamp_ns();
        let alert_time = now + DurationNanos::from_mins(1);

        clock
            .set_time_alert_ns(
                "alert",
                alert_time,
                Some(TimeEventCallback::from(|_| {})),
                None,
            )
            .unwrap();
        assert_eq!(clock.next_time_ns("alert"), Some(alert_time));

        // Callbacks released (e.g. partial component teardown) while the alert still lives
        clock.cancel_callbacks();

        // Rescheduling without a callback fails the predicate check; the error
        // return must not have destroyed the previously scheduled alert
        let err = clock
            .set_time_alert_ns("alert", alert_time + DurationNanos::new(1000), None, None)
            .unwrap_err();
        assert!(
            err.to_string().contains("No callbacks provided"),
            "unexpected error: {err}"
        );
        assert!(clock.timer_exists(&Ustr::from("alert")));
        assert_eq!(clock.next_time_ns("alert"), Some(alert_time));

        clock.cancel_timers();
    }
}
