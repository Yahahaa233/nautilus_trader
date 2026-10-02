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

//! Node-owned original-input replay. Outputs are expectations, never a second cache delta.

use super::{
    LiveNode, NativeMutationInput, NodeState,
    reconciliation::{
        OpenOrderReportResult, OpenOrderReportTask, PositionReportTask, PositionReportTaskResult,
        ReportTaskOutcome, TargetedOrderReportTask,
    },
};
use crate::{
    dispatch::DispatchSource,
    execution::manager::TargetedOrderReportResult,
    runner_recovery::{RunnerRecoveryEvent, RunnerRecoveryWatermark},
};
use anyhow::{Context, Result, ensure};
use nautilus_common::recovery_trace::{
    NativeInputSource, NativePendingInput, NativeTraceRecord, NativeTraceSource,
};
use nautilus_event_store::native_trace::{
    NativeHistoricalRootReplay, VerifiedNativeRoot, VerifiedNativeTrace,
};
use nautilus_system::trader::Trader;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// Native progress from actual handler comparisons. This is neither Deserialize
/// nor an execution/recovery-release permit. Pending source tasks stay explicit.
#[derive(Debug)]
pub struct NativeTailReplayReceipt {
    source: NativeTraceSource,
    target_instance: nautilus_core::UUID4,
    original_cut: RunnerRecoveryWatermark,
    final_root: u64,
    final_input: u64,
    journal_end_sequence: u64,
    pending_report_contexts: serde_json::Value,
    final_watermark: RunnerRecoveryWatermark,
    retained_inputs: Vec<NativePendingInput>,
}
impl NativeTailReplayReceipt {
    #[must_use]
    pub const fn source(&self) -> &NativeTraceSource {
        &self.source
    }
    #[must_use]
    pub const fn target_instance(&self) -> nautilus_core::UUID4 {
        self.target_instance
    }
    #[must_use]
    pub const fn original_cut(&self) -> &RunnerRecoveryWatermark {
        &self.original_cut
    }
    #[must_use]
    pub const fn final_watermark(&self) -> &RunnerRecoveryWatermark {
        &self.final_watermark
    }
    pub fn retained_inputs(&self) -> &[NativePendingInput] {
        &self.retained_inputs
    }
    #[must_use]
    pub const fn final_root(&self) -> u64 {
        self.final_root
    }
    #[must_use]
    pub const fn final_input(&self) -> u64 {
        self.final_input
    }
    #[must_use]
    pub const fn journal_end_sequence(&self) -> u64 {
        self.journal_end_sequence
    }
    #[must_use]
    pub const fn pending_report_contexts(&self) -> &serde_json::Value {
        &self.pending_report_contexts
    }
}

#[derive(Default)]
struct HistoricalReports {
    open: Option<OpenOrderReportTask>,
    targeted: Option<TargetedOrderReportTask>,
    position: Option<PositionReportTask>,
}

impl LiveNode {
    /// Replays source roots through this node's actual registered objects and native handlers.
    /// The strict decoder is checked by the same source encoder before each dispatch.
    /// No future is polled: original HTTP results apply their original preparation,
    /// and transport wrappers consume original local dispositions without contacting a venue.
    ///
    /// Requires the same installed native cache/component/engine cut. Original timers
    /// must use an owner-bound native decoder, never a callback guessed from an event name.
    /// Any panic, source gap, callback mismatch or changed output permanently fences
    /// the actual engine and retains the same child Journal unsealed.
    pub fn replay_native_tail(
        &mut self,
        trace: &VerifiedNativeTrace,
        cut: &RunnerRecoveryWatermark,
        mut decode: impl FnMut(&VerifiedNativeRoot, &NativeTraceRecord) -> Result<RunnerRecoveryEvent>,
        mut decode_pending: impl FnMut(
            &VerifiedNativeTrace,
            &NativePendingInput,
        ) -> Result<RunnerRecoveryEvent>,
    ) -> Result<NativeTailReplayReceipt> {
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            self.replay_native_tail_inner(trace, cut, &mut decode, &mut decode_pending)
        }))
        .map_err(|_| anyhow::anyhow!("native tail replay panicked"))
        .and_then(|result| result);
        self.historical_replay = None;
        self.historical_timer_admissions.clear();
        if let Err(error) = &outcome {
            self.fail_native_dispatch(&format!("native tail replay failed: {error:#}"));
        }
        outcome
    }

    fn replay_native_tail_inner(
        &mut self,
        trace: &VerifiedNativeTrace,
        cut: &RunnerRecoveryWatermark,
        decode: &mut impl FnMut(&VerifiedNativeRoot, &NativeTraceRecord) -> Result<RunnerRecoveryEvent>,
        decode_pending: &mut impl FnMut(
            &VerifiedNativeTrace,
            &NativePendingInput,
        ) -> Result<RunnerRecoveryEvent>,
    ) -> Result<NativeTailReplayReceipt> {
        ensure!(
            self.state() == NodeState::Idle
                && self.recovery_requires_release
                && !self.handle.should_stop()
                && !self.dispatch_failure.get()
                && self.historical_replay.is_none()
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && self.recovery_engine_source.is_some()
                && self.recovery_native_frontier.as_ref() == Some(cut),
            "native tail requires the same complete paused source installation"
        );
        let child_store = self
            .kernel
            .event_store()
            .context("native history requires the actual owned child Journal")?;
        ensure!(
            child_store.run_id().is_some()
                && !child_store.is_halted()
                && child_store.parent_run_id() == Some(trace.source().journal_run.as_str()),
            "native history child Journal is not bound to the verified original source"
        );
        ensure!(
            trace.incomplete_suffix().is_empty(),
            "native tail has an incomplete original root"
        );
        ensure!(
            trace.cut().captured_at_ns
                == self.recovery_engine_source.as_ref().unwrap()["source_capture_ns"]
                    .as_u64()
                    .unwrap_or(0)
                && trace.cut().last_input_sequence == cut.dispatch_watermark,
            "native tail source cut does not bind the installed native frontier"
        );
        ensure!(
            trace.initial_cut_sealed(),
            "native initial cut was not sealed by the actual source barrier"
        );
        ensure!(
            trace.cut().native_effects.get("portfolio").is_some()
                && self.recovery_portfolio_source.as_ref()
                    == trace.cut().native_effects.get("portfolio"),
            "native source Portfolio cut was not completely installed"
        );
        let final_cut = trace.final_cut()?;
        self.historical_timer_admissions = original_timer_admissions(trace)?;
        ensure!(
            self.native_report_contexts.try_borrow()?.is_empty(),
            "native source cut cannot inherit unknown live query tasks"
        );
        let engine = self.kernel.exec_engine.try_borrow()?;
        ensure!(
            !engine.submissions_fenced() && engine.get_external_client_ids().is_empty(),
            "native history cannot run under permanent failure or external execution forwarding"
        );
        drop(engine);
        let (at, wall) = self
            .recovery_engine_time
            .context("native manager restoration timeline missing")?;
        let downtime = wall
            .checked_sub(trace.cut().captured_at_ns)
            .context("native cut is in target future")?;
        let mapped_cut = at
            .checked_sub(std::time::Duration::from_nanos(downtime))
            .context("native cut predates monotonic range")?;
        let runner = self
            .runner
            .as_ref()
            .context("native history runner missing")?;
        runner.bind_senders_for_node(self.handle.clone());
        self.bind_native_ingress_codec()?;
        ensure!(
            runner
                .pending_queue_counts()
                .values()
                .all(|count| *count == 0),
            "native history has unowned target ingress; retain source pending until replay completes"
        );
        let frozen = runner.freeze_ingress()?;
        Trader::prepare_native_recovery(&self.kernel.trader, trace)?;
        self.verify_native_cut_business_state(trace, mapped_cut)?;

        let mut reports = HistoricalReports::default();
        let mut final_root = trace.cut().completed_root;
        let mut final_input = trace.cut().last_input_sequence;
        for root in trace.roots() {
            let replay = NativeHistoricalRootReplay::with_timeline(
                root,
                mapped_cut,
                trace.cut().captured_process_elapsed_ns,
            );
            let begin = replay.next_begin()?;
            let NativeTraceRecord::Begin { read_witnesses, .. } = &begin else {
                unreachable!()
            };
            let witness = read_witnesses
                .iter()
                .find(|witness| witness.component_id == "native:timers")
                .context("original registered clock witness absent")?;
            let before = serde_json::from_value(witness.payload["inventory"].clone())?;
            self.admit_historical_native_timers(
                &before,
                witness.payload["captured_at_ns"]
                    .as_u64()
                    .context("original timer capture absent")?,
            )?;
            self.historical_replay = Some(replay.clone());
            self.dispatch_historical_source(root, &begin, decode, &mut reports)?;
            ensure!(
                !self.dispatch_failure.get() && !self.handle.should_stop(),
                "native historical dispatch failed"
            );
            let receipt = replay.finish()?;
            final_root = receipt.root_sequence();
            final_input = receipt.final_input_sequence();
            self.historical_replay = None;
            frozen.verify()?;
        }
        // Unresolved original requests are evidence, never a processed acknowledgement.
        let pending_report_contexts =
            serde_json::to_value(&*self.native_report_contexts.try_borrow()?)?;
        drop(reports); // Never poll/reissue these historical futures after restoration.
        self.admit_historical_native_timers(
            &final_cut.registered_timers,
            final_cut.captured_at_ns,
        )?;
        for original in trace.final_pending()? {
            let event = if original.receipt.input_source == NativeInputSource::Time {
                let timers = self
                    .recovery_timers
                    .as_mut()
                    .context("final source owner timers missing")?;
                let event_id: nautilus_core::UUID4 = serde_json::from_value(
                    original
                        .timer_event
                        .as_ref()
                        .context("final source timer headers absent")?["event_id"]
                        .clone(),
                )?;
                RunnerRecoveryEvent::TimeEvent(timers.take_historical_retained(event_id)?)
            } else {
                decode_pending(trace, original)?
            };
            let (source, any) = original_event_ref(&event);
            ensure!(
                source == original.receipt.input_source,
                "final source decoder changed its channel"
            );
            let encoded = self
                .dispatch_observer
                .as_ref()
                .context("final source codec absent")?
                .encode_historical_source(
                    crate::node::dispatch::source_dispatch(source)?,
                    "native_enqueue",
                    any,
                )?;
            ensure!(
                encoded.payload == original.payload,
                "final source decoder changed the original input"
            );
            self.runner
                .as_mut()
                .context("final original FIFO receiver missing")?
                .install_native_retained_input(&frozen, event, original.receipt.clone())?;
        }
        frozen.finish()?;
        let mut final_watermark = cut.clone();
        final_watermark.dispatch_watermark = final_input;
        self.recovery_native_frontier = Some(final_watermark.clone());
        self.recovery_engine_source
            .as_mut()
            .context("native engine source lost")?["watermark"] =
            serde_json::to_value(&final_watermark)?;
        Ok(NativeTailReplayReceipt {
            source: trace.source().clone(),
            target_instance: self.kernel.instance_id(),
            original_cut: cut.clone(),
            final_root,
            final_input,
            journal_end_sequence: trace.end_sequence(),
            pending_report_contexts,
            final_watermark,
            retained_inputs: trace.final_pending()?.to_vec(),
        })
    }

    fn dispatch_historical_source(
        &mut self,
        root: &VerifiedNativeRoot,
        begin: &NativeTraceRecord,
        decode: &mut impl FnMut(&VerifiedNativeRoot, &NativeTraceRecord) -> Result<RunnerRecoveryEvent>,
        reports: &mut HistoricalReports,
    ) -> Result<()> {
        let NativeTraceRecord::Begin {
            input_source,
            payload,
            ..
        } = begin
        else {
            anyhow::bail!("original source Begin absent")
        };
        match input_source {
            NativeInputSource::Time => {
                let NativeTraceRecord::Begin { read_witnesses, .. } = begin else {
                    unreachable!()
                };
                let witnesses = read_witnesses
                    .iter()
                    .filter(|witness| witness.component_id == "native:timers")
                    .collect::<Vec<_>>();
                ensure!(
                    witnesses.len() == 1,
                    "original native timer witness missing or duplicated"
                );
                let message = self
                    .recovery_timers
                    .as_mut()
                    .context("original owner timers not installed")?
                    .historical_time_message(witnesses[0])?;
                ensure!(
                    self.process_time_event(message),
                    "original owner callback rejected historical event"
                );
            }
            NativeInputSource::SystemEvent
            | NativeInputSource::SystemCommand
            | NativeInputSource::ExecutionEvent
            | NativeInputSource::TradingCommand
            | NativeInputSource::DataEvent
            | NativeInputSource::DataCommand => match decode(root, begin)? {
                RunnerRecoveryEvent::SystemEvent(event)
                    if *input_source == NativeInputSource::SystemEvent =>
                {
                    self.process_system_event(event)
                }
                RunnerRecoveryEvent::SystemCommand(command)
                    if *input_source == NativeInputSource::SystemCommand =>
                {
                    self.process_system_command(command)
                }
                RunnerRecoveryEvent::ExecutionEvent(event)
                    if *input_source == NativeInputSource::ExecutionEvent =>
                {
                    self.process_exec_event(event)
                }
                RunnerRecoveryEvent::ExecutionCommand(command)
                    if *input_source == NativeInputSource::TradingCommand =>
                {
                    self.process_exec_command(command)
                }
                RunnerRecoveryEvent::DataEvent(event)
                    if *input_source == NativeInputSource::DataEvent =>
                {
                    self.process_data_event(event)
                }
                RunnerRecoveryEvent::DataCommand(command)
                    if *input_source == NativeInputSource::DataCommand =>
                {
                    self.process_data_command(command)
                }
                _ => anyhow::bail!("native source decoder changed its channel"),
            },
            NativeInputSource::QueryResult => {
                let input = NativeMutationInput::from_verified(root, begin)?;
                let guard = self
                    .begin_node_dispatch(DispatchSource::QueryResult, &input)?
                    .context("historical query guard missing")?;
                match input.kind() {
                    "query.open_order" => {
                        ensure!(
                            reports.open.take().is_some(),
                            "original open query preparation missing"
                        );
                        let result: ReportTaskOutcome<OpenOrderReportResult> =
                            serde_json::from_value(input.payload().clone())?;
                        reports.targeted = self.apply_open_order_report_outcome(result)?;
                    }
                    "query.targeted" => {
                        let task = reports
                            .targeted
                            .take()
                            .context("original targeted preparation missing")?;
                        let (result, planned): (
                            ReportTaskOutcome<Vec<TargetedOrderReportResult>>,
                            Option<Vec<nautilus_model::identifiers::ClientOrderId>>,
                        ) = serde_json::from_value(input.payload().clone())?;
                        ensure!(
                            planned.as_deref() == Some(task.planned_client_order_ids.as_slice()),
                            "targeted query owner drifted"
                        );
                        self.apply_targeted_report_outcome(result, &task.planned_client_order_ids)?;
                    }
                    "query.position" => {
                        ensure!(
                            reports.position.take().is_some(),
                            "original position preparation missing"
                        );
                        let result: ReportTaskOutcome<PositionReportTaskResult> =
                            serde_json::from_value(input.payload().clone())?;
                        reports.position = self.apply_position_report_outcome(result)?;
                    }
                    _ => anyhow::bail!("unsupported native historical query: {}", input.kind()),
                }
                self.finish_node_dispatch(guard)?;
            }
            NativeInputSource::Maintenance => {
                let input = NativeMutationInput::from_verified(root, begin)?;
                ensure!(
                    input.kind() == "maintenance.tick",
                    "unknown original maintenance input"
                );
                let guard = self
                    .begin_node_dispatch(DispatchSource::Maintenance, &input)?
                    .context("historical maintenance guard missing")?;
                self.apply_historical_maintenance(&input, reports)?;
                self.finish_node_dispatch(guard)?;
            }
            NativeInputSource::Reconciliation => {
                // Real synchronous producer uses the same Vec<OrderEventAny> encoder and native handler.
                let events: Vec<nautilus_model::events::OrderEventAny> =
                    serde_json::from_value(payload.clone())?;
                self.process_reconciliation_events(&events);
            }
            NativeInputSource::Lifecycle => {
                let input = NativeMutationInput::from_verified(root, begin)?;
                let guard = self
                    .begin_node_dispatch(DispatchSource::Lifecycle, &input)?
                    .context("historical lifecycle guard absent")?;
                match input.kind() {
                    "stop.trader" => Trader::replay_native_lifecycle(&self.kernel.trader)?,
                    "stop.final_admission_cut" => { /* Pure original boundary marker: target admission stays frozen. */
                    }
                    _ => anyhow::bail!(
                        "unsupported original lifecycle transition: {}",
                        input.kind()
                    ),
                }
                self.finish_node_dispatch(guard)?;
            }
            NativeInputSource::ExternalMessage => {
                anyhow::bail!(
                    "external source requires an explicit registered historical bus profile"
                );
            }
        }
        Ok(())
    }

    fn verify_native_cut_business_state(
        &self,
        trace: &VerifiedNativeTrace,
        source_at: nautilus_common::live::dst::time::Instant,
    ) -> Result<()> {
        let actual = self.collect_native_trace_effects()?;
        let expected = &trace.cut().native_effects;
        for name in [
            "orders",
            "positions",
            "accounts",
            "market_cache",
            "components",
            "data_engine",
            "report_contexts",
            "portfolio",
        ] {
            ensure!(
                expected.get(name).is_some() && actual[name] == expected[name],
                "installed native source cut business state differs: {name}"
            );
        }
        let mut manager = self.exec_manager.trace_effects_inventory(source_at)?;
        manager["captured_at_ns"] = trace.cut().captured_at_ns.into();
        ensure!(
            manager == expected["execution_manager"],
            "installed native manager history differs at the original source cut"
        );
        ensure!(
            self.recovery_timers
                .as_ref()
                .context("source native timer installation missing")?
                .historical_inventory()?
                == trace.cut().registered_timers,
            "installed owner timer source cut differs"
        );
        Ok(())
    }

    pub(super) fn admit_historical_native_timers(
        &self,
        inventory: &std::collections::BTreeMap<String, serde_json::Value>,
        captured_at_ns: u64,
    ) -> Result<()> {
        self.recovery_timers
            .as_ref()
            .context("native original owner timers not installed")?
            .admit_historical_timers(inventory, &self.historical_timer_admissions, captured_at_ns)
    }

    fn apply_historical_maintenance(
        &mut self,
        input: &NativeMutationInput,
        reports: &mut HistoricalReports,
    ) -> Result<()> {
        let flag = |name: &str| {
            input.payload()[name]
                .as_bool()
                .with_context(|| format!("native maintenance decision absent: {name}"))
        };
        ensure!(
            input.payload()["report_tasks"]["open_order"].as_bool() == Some(reports.open.is_some())
                && input.payload()["report_tasks"]["targeted_order"].as_bool()
                    == Some(reports.targeted.is_some())
                && input.payload()["report_tasks"]["position"].as_bool()
                    == Some(reports.position.is_some()),
            "original maintenance pending ownership changed"
        );
        if flag("reconciliation_due")? {
            if flag("inflight_check_due")? {
                let result = self.exec_manager.check_inflight_orders();
                self.process_reconciliation_events(&result.events);
                for command in result.queries {
                    crate::runner::AsyncRunner::handle_exec_command(command);
                }
            }
            let open = flag("open_check_due")?;
            let position = flag("position_check_due")?;
            if reports.open.is_none() && reports.targeted.is_none() && reports.position.is_none() {
                if position && (!open || flag("position_before_open")?) {
                    reports.position = self.start_position_report_check();
                } else if open {
                    reports.open = self.start_open_order_report_check();
                }
            }
        }
        if flag("purge_orders_due")? {
            self.exec_manager.purge_closed_orders();
        }
        if flag("purge_positions_due")? {
            self.exec_manager.purge_closed_positions();
        }
        if flag("purge_account_due")? {
            self.exec_manager.purge_account_events();
        }
        if flag("own_books_due")? {
            self.kernel.cache.try_borrow_mut()?.audit_own_order_books();
        }
        if flag("prune_fills_due")? {
            self.exec_manager.prune_recent_fills_cache(60.0);
            self.exec_manager.prune_processed_fills();
            self.exec_manager.prune_order_local_activity();
        }
        Ok(())
    }
}

fn original_event_ref(event: &RunnerRecoveryEvent) -> (NativeInputSource, &dyn std::any::Any) {
    match event {
        RunnerRecoveryEvent::TimeEvent(value) => (NativeInputSource::Time, value),
        RunnerRecoveryEvent::SystemEvent(value) => (NativeInputSource::SystemEvent, value),
        RunnerRecoveryEvent::SystemCommand(value) => (NativeInputSource::SystemCommand, value),
        RunnerRecoveryEvent::ExecutionEvent(value) => (NativeInputSource::ExecutionEvent, value),
        RunnerRecoveryEvent::ExecutionCommand(value) => (NativeInputSource::TradingCommand, value),
        RunnerRecoveryEvent::DataEvent(value) => (NativeInputSource::DataEvent, value),
        RunnerRecoveryEvent::DataCommand(value) => (NativeInputSource::DataCommand, value),
    }
}

fn original_timer_admissions(
    trace: &VerifiedNativeTrace,
) -> Result<Vec<(String, u64, bool, nautilus_common::timer::TimeEvent, u64)>> {
    use nautilus_common::timer::TimeEvent;
    let parse = |owner: String,
                 binding: &serde_json::Value,
                 event: &serde_json::Value,
                 accepted: u64|
     -> Result<_> {
        let cleanup = match binding["kind"].as_str() {
            Some("registered_owner_thread") => false,
            Some("registered_cleanup") => true,
            _ => anyhow::bail!("original timer owner callback kind unsupported"),
        };
        Ok((
            owner,
            binding["binding_id"]
                .as_u64()
                .context("original timer binding absent")?,
            cleanup,
            TimeEvent::new(
                event["name"]
                    .as_str()
                    .context("original timer name absent")?
                    .into(),
                serde_json::from_value(event["event_id"].clone())?,
                serde_json::from_value(event["ts_event"].clone())?,
                serde_json::from_value(event["ts_init"].clone())?,
            ),
            accepted,
        ))
    };
    let mut admissions = Vec::new();
    for root in trace.roots() {
        for record in root.inputs() {
            if let NativeTraceRecord::Begin {
                input_source: NativeInputSource::Time,
                receipt,
                read_witnesses,
                ..
            } = record
            {
                let input = &read_witnesses
                    .iter()
                    .find(|witness| witness.component_id == "native:timers")
                    .context("original native owner witness missing")?
                    .payload["input"];
                admissions.push(parse(
                    input["owner"]
                        .as_str()
                        .context("original timer owner absent")?
                        .into(),
                    &input["binding"],
                    &input["event"],
                    receipt
                        .ingress
                        .as_ref()
                        .context("original timer ingress receipt missing")?
                        .accepted_wall_ns,
                )?);
            }
        }
    }
    let cut = trace.final_cut()?;
    for pending in &cut.pending_inputs {
        if pending.receipt.input_source != NativeInputSource::Time {
            continue;
        }
        let binding = pending
            .callback_binding
            .as_ref()
            .context("original final callback binding absent")?;
        let id = binding["binding_id"]
            .as_u64()
            .context("original final callback ID absent")?;
        let owners = cut
            .registered_timers
            .iter()
            .filter(|(_, inventory)| {
                inventory["timers"].as_array().is_some_and(|entries| {
                    entries
                        .iter()
                        .any(|entry| entry["binding"]["binding_id"].as_u64() == Some(id))
                })
            })
            .map(|(owner, _)| owner.clone())
            .collect::<Vec<_>>();
        ensure!(
            owners.len() == 1,
            "original final timer owner missing or ambiguous"
        );
        admissions.push(parse(
            owners[0].clone(),
            binding,
            pending
                .timer_event
                .as_ref()
                .context("original final timer headers absent")?,
            pending.receipt.accepted_wall_ns,
        )?);
    }
    admissions.sort_by_key(|(_, _, _, _, accepted)| *accepted);
    let mut ids = std::collections::HashSet::new();
    ensure!(
        admissions
            .iter()
            .all(|(_, _, _, event, _)| ids.insert(event.event_id)),
        "duplicate original timer event identity"
    );
    Ok(admissions)
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{NodeDispatchObserver, NodeRunMode, RunningCheckpointSchedule},
        runner_recovery::{
            RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryCodecRegistry,
            RunnerRecoveryEnvelope, RunnerRecoveryEventRef,
        },
    };
    use nautilus_common::{cache::Cache, enums::Environment, messages::DataEvent};
    use nautilus_event_store::{
        EventStoreReader, backend::RedbBackend, kernel::EventStoreLifecycle,
    };
    use nautilus_model::{
        identifiers::TraderId,
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    };
    use nautilus_system::event_store::EventStoreConfig;
    use rstest::rstest;
    use std::{cell::RefCell, rc::Rc, time::Duration};

    #[derive(Debug)]
    struct InstrumentCodec;
    impl RunnerRecoveryCodec for InstrumentCodec {
        fn channel(&self) -> RunnerRecoveryChannel {
            RunnerRecoveryChannel::DataEvent
        }
        fn codec_id(&self) -> &str {
            "actual_native_instrument_tail.v1"
        }
        fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
            match event {
                RunnerRecoveryEventRef::DataEvent(DataEvent::Instrument(value)) => {
                    Ok(serde_json::to_value(value)?)
                }
                _ => anyhow::bail!("unsupported actual instrument queue member"),
            }
        }
        fn decode(&self, input: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            Ok(RunnerRecoveryEvent::DataEvent(DataEvent::Instrument(
                serde_json::from_value(input.payload.clone())?,
            )))
        }
    }
    pub(super) fn actual_node(
        name: &str,
        directory: std::path::PathBuf,
        parent: Option<(
            std::path::PathBuf,
            nautilus_core::UUID4,
            String,
            u64,
            String,
        )>,
    ) -> LiveNode {
        let config = EventStoreConfig {
            base_dir: directory,
            ..Default::default()
        };
        let mut node = LiveNode::builder(TraderId::from("NATIVE-TAIL-001"), Environment::Live)
            .unwrap()
            .with_name(name)
            .with_load_state(false)
            .with_reconciliation(false)
            .with_exec_engine_config(crate::config::LiveExecutionEngineConfig {
                load_cache: false,
                reconciliation: false,
                ..Default::default()
            })
            .with_delay_shutdown_secs(0)
            .with_event_store(move |instance, clock| {
                let mut store = EventStoreLifecycle::boot(Some(config), instance, clock)?;
                if let Some((directory, source_instance, source_run, watermark, fingerprint)) =
                    parent
                {
                    store.bind_external_parent(
                        &directory,
                        source_instance,
                        &source_run,
                        watermark,
                        &fingerprint,
                    )?;
                }
                Ok(Box::new(store))
            })
            .build()
            .unwrap();
        node.config.timeout_shutdown = Duration::ZERO;
        node.config.delay_post_stop = Duration::ZERO;
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new(name.into(), |_| Ok(())).unwrap(),
            |source, phase, input| {
                let payload = if let Some(value) = input.downcast_ref::<NativeMutationInput>() {
                    value.canonical_payload()?
                } else if let Some(DataEvent::Instrument(value)) = input.downcast_ref::<DataEvent>()
                {
                    serde_json::to_value(value)?
                } else if let Some(value) = input.downcast_ref::<DataEvent>() {
                    super::framework_tests::data_payload(value)?
                } else if let Some(value) = input.downcast_ref::<nautilus_common::messages::data::DataCommand>() {
                    match value {
                        nautilus_common::messages::data::DataCommand::Subscribe(value) => serde_json::json!({"Subscribe":value}),
                        _ => anyhow::bail!("unknown actual test data command"),
                    }
                } else if let Some(value) = input.downcast_ref::<nautilus_common::messages::ExecutionEvent>() {
                    use nautilus_common::messages::ExecutionEvent;
                    match value {
                        ExecutionEvent::Order(value) => serde_json::to_value(value)?,
                        ExecutionEvent::Account(value) => serde_json::json!({"Account":value}),
                        ExecutionEvent::OrderSubmittedBatch(value) => serde_json::json!({"SubmittedBatch":value.events}),
                        ExecutionEvent::OrderAcceptedBatch(value) => serde_json::json!({"AcceptedBatch":value.events}),
                        _ => anyhow::bail!("unknown actual test execution event"),
                    }
                } else if let Some(command) = input.downcast_ref::<nautilus_common::runner::TradingCommandMessage>() {
                    serde_json::json!({"endpoint":command.endpoint().to_string(),"command":command.command()})
                } else if let Some(events) =
                    input.downcast_ref::<Vec<nautilus_model::events::OrderEventAny>>()
                {
                    serde_json::to_value(events)?
                } else {
                    anyhow::bail!("unknown actual test native input")
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
        node
    }
    fn registry() -> Rc<RunnerRecoveryCodecRegistry> {
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::DataEvent]);
        registry.register(InstrumentCodec).unwrap();
        Rc::new(registry.seal().unwrap())
    }

    /// Uses two actual LiveNodes, a real source runloop, durable redb Journal,
    /// native checkpoint barriers and original DataEngine handlers. No venue or
    /// DTO completion fixture supplies the source root.
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test(flavor = "current_thread")]
    async fn actual_running_native_tail_replays_instrument_and_refuses_changed_original_input(
        #[case] changed: bool,
    ) {
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir)
            .join(format!("native-tail-test-{}", nautilus_core::UUID4::new()));
        std::fs::create_dir_all(&root).unwrap();
        let mut source = actual_node("native-source", root.join("source"), None);
        let source_instance = source.kernel.instance_id();
        let trace = source
            .prepare_owned_native_trace(
                "native-source-business-run".into(),
                nautilus_event_store::native_trace::native_inventory_digest(
                    &serde_json::to_value(&source.config).unwrap(),
                )
                .unwrap(),
                "actual_native_instrument_tail.v1".into(),
                "actual_empty_registered_components.v1".into(),
                |_, _, _| Ok(Vec::new()),
                |_| Ok(()),
            )
            .unwrap();
        let cuts = Rc::new(RefCell::new(Vec::<(
            nautilus_event_store::native_trace::NativeTraceCheckpointCut,
            serde_json::Value,
            nautilus_system::trader::CollectedComponentState,
        )>::new()));
        let initial = Rc::new(tokio::sync::Notify::new());
        let progressed = Rc::new(tokio::sync::Notify::new());
        let saved = cuts.clone();
        let initial_cut = initial.clone();
        let second_cut = progressed.clone();
        source
            .set_running_checkpoint_handler(
                registry(),
                RunningCheckpointSchedule::EveryCompletedRoot,
                move |boundary| {
                    boundary.verify()?;
                    let cut = boundary
                        .native_trace_cut()
                        .context("actual source trace cut absent")?
                        .clone();
                    boundary.persist_native_checkpoint(
                        serde_json::json!({"native_source_cut":cut.native_inventory_digest}),
                    )?;
                    saved.try_borrow_mut()?.push((
                        cut,
                        serde_json::to_value(boundary.inventory())?,
                        boundary.components().clone(),
                    ));
                    if saved.borrow().len() == 1 {
                        initial_cut.notify_one();
                    } else {
                        second_cut.notify_one();
                    }
                    Ok(())
                },
                |_| Ok(()),
                |_| {},
            )
            .unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let sender = source.runner.as_ref().unwrap().data_event_sender_clone();
        let handle = source.handle();
        let produced = async move {
            initial.notified().await;
            sender.send(DataEvent::Instrument(instrument)).unwrap();
            progressed.notified().await;
            handle.stop();
        };
        // Return a source run error immediately even if no completed cut was
        // produced. A waiting producer cannot hide that failure indefinitely.
        let mut producer = std::pin::pin!(produced);
        let outcome = tokio::time::timeout(Duration::from_secs(15), async {
            let mut running = std::pin::pin!(source.run_with_mode(NodeRunMode::Hosted));
            tokio::select! {
                result = &mut running => result,
                () = &mut producer => running.await,
            }
        })
        .await
        .expect("actual source run did not reach its cut/stop within 15s");
        outcome.unwrap();
        assert!(source.kernel.cache.borrow().instrument(&id).is_some());
        let source_identity = trace.source().unwrap();
        let first = cuts.borrow().first().unwrap().clone();
        assert!(
            first.0.native_effects["market_cache"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        source.dispose();
        drop(source);
        let reader = EventStoreReader::new(
            RedbBackend::open_sealed(
                root.join("source"),
                &source_instance.to_string(),
                &source_identity.journal_run,
            )
            .unwrap(),
        );
        let verified = reader
            .verify_native_tail(&source_identity, &first.0, reader.high_watermark().unwrap())
            .unwrap();
        assert!(
            verified
                .roots()
                .iter()
                .any(|root| root.inputs().iter().any(|row| matches!(
                    row,
                    NativeTraceRecord::Begin {
                        input_source: NativeInputSource::DataEvent,
                        ..
                    }
                )))
        );
        assert!(verified.final_pending().unwrap().is_empty());
        let source_high = reader.high_watermark().unwrap();
        let fingerprint = EventStoreLifecycle::sealed_run_fingerprint(
            &root.join("source"),
            source_instance,
            &source_identity.journal_run,
            source_high,
        )
        .unwrap();
        let mut target = actual_node(
            "native-target",
            root.join("target"),
            Some((
                root.join("source"),
                source_instance,
                source_identity.journal_run.clone(),
                source_high,
                fingerprint,
            )),
        );
        target.restore_native_cache(Cache::default()).unwrap();
        target.restore_component_state(&first.2).unwrap();
        target
            .kernel
            .risk_engine
            .borrow_mut()
            .set_trading_state(nautilus_model::enums::TradingState::Halted);
        target
            .kernel
            .open_event_store_for_paused_recovery()
            .unwrap();
        let watermark = RunnerRecoveryWatermark {
            recovery_id: "actual-source-cut".into(),
            checkpoint_sequence: first.0.prefix.sequence,
            dispatch_watermark: first.0.last_input_sequence,
        };
        target
            .replay_recovery_events(&watermark, &[], &registry(), |_, _| Ok(()))
            .unwrap();
        target
            .restore_registered_engine_checkpoint(
                &first.1["execution_manager"],
                &first.1["data_engine"],
                first.0.captured_at_ns,
                &watermark,
            )
            .unwrap();
        target
            .restore_registered_portfolio_checkpoint(
                &first.0.native_effects["portfolio"],
                &watermark,
            )
            .unwrap();
        target
            .restore_registered_timer_checkpoint(
                first.0.registered_timers.clone(),
                Vec::new(),
                &watermark,
            )
            .unwrap();
        let result = target.replay_native_tail(
            &verified,
            &watermark,
            |_, begin| {
                let NativeTraceRecord::Begin { payload, .. } = begin else {
                    unreachable!()
                };
                let mut original: InstrumentAny = serde_json::from_value(payload.clone())?;
                if changed {
                    original = InstrumentAny::CryptoPerpetual(
                        nautilus_model::instruments::stubs::xbtusd_bitmex(),
                    );
                }
                Ok(RunnerRecoveryEvent::DataEvent(DataEvent::Instrument(
                    original,
                )))
            },
            |_, _| anyhow::bail!("no original pending input expected"),
        );
        if changed {
            assert!(result.is_err());
            assert!(target.kernel.exec_engine.borrow().submissions_fenced());
            assert!(target.event_store_halted());
        } else {
            let receipt = result.unwrap();
            assert!(receipt.final_input() > watermark.dispatch_watermark);
            assert!(receipt.retained_inputs().is_empty());
            assert!(target.kernel.cache.borrow().instrument(&id).is_some());
            assert_eq!(target.state(), NodeState::Idle);
            assert_eq!(
                target.kernel.risk_engine.borrow().trading_state(),
                nautilus_model::enums::TradingState::Halted
            );
            assert!(target.recovery_requires_release);
        }
        target
            .kernel
            .prohibit_event_store_seal("test retains child until authenticated observation")
            .unwrap();
        target.dispose();
        drop(target);
        drop(reader);
        drop(trace);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "native_tail_framework_tests.rs"]
mod framework_tests;

#[cfg(test)]
#[path = "native_tail_uuid_tests.rs"]
mod uuid_tests;
