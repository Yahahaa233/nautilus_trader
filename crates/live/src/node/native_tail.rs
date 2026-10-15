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
    trace_continuation: Option<nautilus_event_store::native_trace::NativeReplayContinuationReceipt>,
}
impl NativeTailReplayReceipt {
    /// Same actual child writer audit after verified replay. Never current execution authority.
    #[must_use]
    pub const fn trace_continuation(
        &self,
    ) -> Option<&nautilus_event_store::native_trace::NativeReplayContinuationReceipt> {
        self.trace_continuation.as_ref()
    }
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
    #[must_use]
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

#[derive(Debug)]
pub(super) struct InstalledNativeMarketCut {
    watermark: RunnerRecoveryWatermark,
    source: NativeTraceSource,
    prefix: nautilus_event_store::writer::DurableJournalPrefix,
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
    ///
    /// # Errors
    /// Returns an error if original inputs, effects, or retained queues cannot be restored and verified; failures keep the native admission fence closed.
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
        .map_err(|panic| {
            let reason = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload");
            anyhow::anyhow!("native tail replay panicked: {reason}")
        })
        .and_then(|result| result);
        self.historical_replay = None;
        // An incomplete historical transaction must poison its same physical ingress.
        self.historical_ingress = None;
        self.historical_timer_admissions.clear();
        if let Err(error) = &outcome {
            self.fail_native_dispatch(&format!("native tail replay failed: {error:#}"));
        }
        outcome
    }

    /// Installs the original market deques from a reader-issued source trace.
    /// This is historical cache state, not market freshness or execution authority.
    /// Install it once after cache/components/frontier and before engines/timers.
    /// # Errors
    /// Refuses an unsealed/changed source cut, unknown original cache capacity,
    /// changed instruments/configuration, populated target histories or repeat use.
    pub fn restore_native_market_checkpoint(
        &mut self,
        trace: &VerifiedNativeTrace,
        watermark: &RunnerRecoveryWatermark,
    ) -> Result<()> {
        ensure!(
            self.state() == NodeState::Idle
                && self.recovery_requires_release
                && !self.handle.should_stop()
                && !self.dispatch_failure.get()
                && self.historical_replay.is_none()
                && self.historical_ingress.is_none()
                && self.recovery_cache_installed
                && self.recovery_restored_components.is_some()
                && self.recovery_native_frontier.as_ref() == Some(watermark)
                && self.recovery_market_source.is_none()
                && self.recovery_engine_source.is_none()
                && self.recovery_timers.is_none(),
            "market restore requires the same idle source cut before engines/timers"
        );
        ensure!(
            trace.initial_cut_sealed()
                && trace.cut().last_input_sequence == watermark.dispatch_watermark,
            "market source is not the verified installed checkpoint cut"
        );
        let config = trace
            .cut()
            .native_effects
            .get("market_cache_config")
            .context("original market cache configuration/capacity absent")?;
        let market = trace
            .cut()
            .native_effects
            .get("market_cache")
            .context("original market cache histories absent")?;
        let input = self.native_mutation_input(
            "recovery.market_cache_install",
            &serde_json::json!({"watermark":watermark,"source":trace.source(),
                "source_prefix":trace.cut().prefix,"market_cache_config":config,"market_cache":market}),
        )?;
        let guard = self.begin_node_dispatch(crate::dispatch::DispatchSource::Lifecycle, &input)?;
        let outcome = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
            self.kernel
                .cache
                .try_borrow_mut()?
                .restore_native_market_checkpoint(config, market)?;
            self.recovery_market_source = Some(InstalledNativeMarketCut {
                watermark: watermark.clone(),
                source: trace.source().clone(),
                prefix: trace.cut().prefix.clone(),
            });
            Ok(())
        }))
        .map_err(|_| anyhow::anyhow!("native market restoration panicked"))
        .and_then(|result| result);
        if let Err(error) = &outcome {
            self.fail_native_dispatch(&format!("native market restoration failed: {error:#}"));
        }
        outcome?;
        if let Some(guard) = guard {
            self.finish_node_dispatch(guard)?;
        }
        Ok(())
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
        if let Some(installed) = &self.recovery_market_source {
            ensure!(
                installed.watermark == *cut
                    && installed.source == *trace.source()
                    && installed.prefix == trace.cut().prefix,
                "native market installation belongs to a different verified source cut"
            );
        }
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
        self.historical_ingress = Some(runner.freeze_ingress()?);
        Trader::prepare_native_recovery(&self.kernel.trader, trace)?;
        self.verify_native_cut_business_state(trace, mapped_cut)?;
        // Restore the original callback owner/frontier before the first historical
        // input can publish or allocate another command context.
        let initial = &trace.cut().pending_inputs;
        let initial_ancestry = read_callback_ancestry(trace.cut(), initial)?;
        self.admit_historical_native_timers(
            &trace.cut().registered_timers,
            trace.cut().captured_at_ns,
        )?;
        let mut decoded = Vec::with_capacity(initial.len());
        for original in initial {
            decoded.push((
                self.decode_original_pending(trace, original, decode_pending)?,
                original.receipt.clone(),
            ));
        }
        let mut contexts = initial_ancestry
            .as_ref()
            .map(|ancestry| nautilus_common::actor::restore_channel_contexts(&ancestry.state))
            .transpose()?;
        for (index, (event, receipt)) in decoded.into_iter().enumerate() {
            let context = contexts
                .as_mut()
                .map(|bank| bank.take(index))
                .transpose()?
                .flatten();
            self.runner
                .as_mut()
                .context("initial source FIFO receiver missing")?
                .install_native_retained_input(
                    self.historical_ingress
                        .as_ref()
                        .context("initial source ingress missing")?,
                    event,
                    receipt,
                    context,
                )?;
        }
        drop(contexts);
        let frontiers = original_channel_frontiers(trace)?;
        self.runner
            .as_mut()
            .context("initial source FIFO receiver missing")?
            .bind_historical_source_frontiers(
                self.historical_ingress
                    .as_ref()
                    .context("initial source ingress missing")?,
                &frontiers,
            )?;
        if let Some(ancestry) = initial_ancestry.as_ref() {
            self.runner
                .as_mut()
                .context("initial source FIFO receiver missing")?
                .verify_native_callback_ancestry(
                    self.historical_ingress
                        .as_ref()
                        .context("initial source ingress missing")?,
                    ancestry,
                )?;
        }

        let mut historical_receipts = Vec::new();
        historical_receipts.try_reserve(trace.roots().len())?;
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
            historical_receipts.push(receipt);
            self.historical_replay = None;
            self.historical_ingress
                .as_ref()
                .context("historical native ingress missing")?
                .verify()?;
        }
        // Unresolved original requests are evidence, never a processed acknowledgement.
        let pending_report_contexts =
            serde_json::to_value(&*self.native_report_contexts.try_borrow()?)?;
        drop(reports); // Never poll/reissue these historical futures after restoration.
        self.admit_historical_native_timers(
            &final_cut.registered_timers,
            final_cut.captured_at_ns,
        )?;
        let originals = trace.final_pending()?;
        let callback_ancestry = read_callback_ancestry(final_cut, originals)?;
        let (actual, _) = self.actual_historical_pending()?;
        let mut matched = 0;
        for (index, original) in originals.iter().enumerate() {
            if actual
                .get(matched)
                .is_some_and(|input| input.receipt == original.receipt)
            {
                ensure!(
                    actual[matched] == *original,
                    "actual retained domain or original callback lease changed"
                );
                matched += 1;
                continue;
            }
            ensure!(actual.get(matched).is_none_or(|input| input.receipt.input_source != original.receipt.input_source),
                "sealed final source FIFO would overtake an actual native message");
            let ancestry = callback_ancestry
                .as_ref()
                .context("final external source needs explicit callback ancestry")?;
            ensure!(
                original.receipt.caused_by.is_none()
                    && ancestry.state.entry_chains[index].is_none()
                    && ancestry.state.entry_context_ids[index].is_none(),
                "missing derived original FIFO input cannot be manufactured from final source metadata"
            );
            let event = self.decode_original_pending(trace, original, decode_pending)?;
            self.runner
                .as_mut()
                .context("final external source FIFO missing")?
                .install_historical_external_input(
                    self.historical_ingress
                        .as_ref()
                        .context("final source ingress missing")?,
                    event,
                    original.receipt.clone(),
                )?;
        }
        ensure!(
            matched == actual.len(),
            "actual historical FIFO has inputs absent from the sealed source"
        );
        let (actual, actual_ancestry) = self.actual_historical_pending()?;
        ensure!(
            actual == originals,
            "actual historical final FIFO differs from the sealed original source"
        );
        if let Some(expected) = callback_ancestry.as_ref() {
            ensure!(
                actual_ancestry == *expected,
                "actual historical callback ancestry or owner frontier differs from the sealed source"
            );
        }
        self.historical_ingress
            .as_ref()
            .context("historical native ingress missing")?
            .verify()?;
        let trace_continuation = if let Some(observer) = &self.dispatch_observer
            && let Some(recorder) = observer.native_trace()
        {
            let prepared = recorder.prepare_historical_continuation(trace, &historical_receipts)?;
            let completed = observer
                .completed_root_boundary_proof()?
                .context("historical replay child observer has no completed boundary")?;
            completed.verify()?;
            ensure!(
                prepared.completed_dispatch()
                    == Some((completed.root_sequence(), completed.input_sequence())),
                "verified historical continuation differs from actual child dispatch"
            );
            let acknowledgment = prepared.commit()?;
            completed.verify()?;
            self.historical_ingress
                .as_ref()
                .context("historical native ingress missing")?
                .verify()?;
            Some(acknowledgment)
        } else {
            None
        };
        self.historical_ingress
            .take()
            .context("historical native ingress disappeared")?
            .finish()?;
        let mut final_watermark = cut.clone();
        final_watermark.dispatch_watermark = final_input;
        self.recovery_native_frontier = Some(final_watermark.clone());
        self.recovery_engine_source
            .as_mut()
            .context("native engine source lost")?["watermark"] =
            serde_json::to_value(&final_watermark)?;
        if let Some(audit) = &trace_continuation {
            let recorder = self
                .dispatch_observer
                .as_ref()
                .and_then(|observer| observer.native_trace())
                .context("native timer continuation lost its owned child recorder")?;
            self.recovery_timers
                .as_mut()
                .context("native timer continuation lost original owner timers")?
                .bind_native_continuation(trace, &final_watermark, recorder, audit)?;
        }
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
            trace_continuation,
        })
    }

    fn actual_historical_pending(
        &mut self,
    ) -> Result<(
        Vec<NativePendingInput>,
        crate::runner_recovery::RunnerCallbackAncestry,
    )> {
        let observer = self
            .dispatch_observer
            .as_ref()
            .context("historical FIFO codec absent")?;
        self.runner
            .as_mut()
            .context("historical FIFO receiver absent")?
            .historical_pending_inputs(
                self.historical_ingress
                    .as_ref()
                    .context("historical ingress owner absent")?,
                &|source, input| {
                    Ok(observer
                        .encode_historical_source(
                            super::dispatch::source_dispatch(source),
                            "native_enqueue",
                            input,
                        )?
                        .payload)
                },
            )
    }

    fn decode_original_pending(
        &self,
        trace: &VerifiedNativeTrace,
        original: &NativePendingInput,
        decode_pending: &mut impl FnMut(
            &VerifiedNativeTrace,
            &NativePendingInput,
        ) -> Result<RunnerRecoveryEvent>,
    ) -> Result<RunnerRecoveryEvent> {
        let event = if original.receipt.input_source == NativeInputSource::Time {
            let event_id = serde_json::from_value(
                original
                    .timer_event
                    .as_ref()
                    .context("original source timer headers absent")?["event_id"]
                    .clone(),
            )?;
            RunnerRecoveryEvent::TimeEvent(
                self.recovery_timers
                    .as_ref()
                    .context("original source owner timers missing")?
                    .take_historical_retained(event_id)?,
            )
        } else {
            decode_pending(trace, original)?
        };
        let (source, any) = original_event_ref(&event);
        ensure!(
            source == original.receipt.input_source,
            "original source decoder changed its channel"
        );
        let encoded = self
            .dispatch_observer
            .as_ref()
            .context("original source codec absent")?
            .encode_historical_source(
                super::dispatch::source_dispatch(source),
                "native_enqueue",
                any,
            )?;
        ensure!(
            encoded.payload == original.payload,
            "original source decoder changed its domain payload"
        );
        if let RunnerRecoveryEvent::TimeEvent(message) = &event {
            let time = message.event();
            ensure!(
                original.callback_binding.as_ref()
                    == Some(&message.native_input_callback_binding())
                    && original.timer_event.as_ref()
                        == Some(
                            &serde_json::json!({"name":time.name,"event_id":time.event_id,"ts_event":time.ts_event,"ts_init":time.ts_init})
                        ),
                "original source timer decoder changed its owner lease or event headers"
            );
        }
        Ok(event)
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
            NativeInputSource::Time
            | NativeInputSource::SystemEvent
            | NativeInputSource::SystemCommand
            | NativeInputSource::ExecutionEvent
            | NativeInputSource::TradingCommand
            | NativeInputSource::DataEvent
            | NativeInputSource::DataCommand => {
                use crate::runner::PendingRunnerEvent;
                let NativeTraceRecord::Begin { receipt, .. } = begin else {
                    unreachable!()
                };
                let ingress = receipt
                    .ingress
                    .as_ref()
                    .context("original physical Begin has no ingress receipt")?;
                let mut event = self
                    .runner
                    .as_mut()
                    .context("historical physical FIFO missing")?
                    .take_historical_native_input(
                        self.historical_ingress
                            .as_ref()
                            .context("historical ingress owner missing")?,
                        ingress,
                    )?;
                if event.is_none() {
                    ensure!(
                        ingress.caused_by.is_none(),
                        "derived original Begin is missing its actual retained native envelope"
                    );
                    let decoded = if *input_source == NativeInputSource::Time {
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
                        RunnerRecoveryEvent::TimeEvent(
                            self.recovery_timers
                                .as_mut()
                                .context("original owner timers not installed")?
                                .historical_time_message(witnesses[0])?,
                        )
                    } else {
                        decode(root, begin)?
                    };
                    self.runner
                        .as_mut()
                        .context("external original FIFO missing")?
                        .install_historical_external_input(
                            self.historical_ingress
                                .as_ref()
                                .context("historical ingress owner missing")?,
                            decoded,
                            ingress.clone(),
                        )?;
                    event = self
                        .runner
                        .as_mut()
                        .context("external original FIFO missing")?
                        .take_historical_native_input(
                            self.historical_ingress
                                .as_ref()
                                .context("historical ingress owner missing")?,
                            ingress,
                        )?;
                }
                match event.context("original native FIFO envelope disappeared")? {
                    PendingRunnerEvent::TimeEvent(message) => ensure!(
                        self.process_time_event_result(message)?,
                        "original owner callback rejected historical event"
                    ),
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
            "market_cache_config",
            "components",
            "strategy_framework",
            "data_engine",
            "report_contexts",
            "portfolio",
        ] {
            ensure!(
                expected.get(name).is_some() && actual[name] == expected[name],
                "installed native source cut business state differs: {name}"
            );
        }
        let owner_timers = self
            .recovery_timers
            .as_ref()
            .context("source native timer installation missing")?
            .historical_inventory()?;
        nautilus_system::trader::Trader::verify_native_strategy_framework_timers(
            &self.kernel.trader,
            &expected["strategy_framework"],
            &owner_timers,
        )?;
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

fn original_channel_frontiers(
    trace: &VerifiedNativeTrace,
) -> Result<Vec<(NativeInputSource, nautilus_core::UUID4, u64)>> {
    use std::collections::HashMap;
    // Per-channel original ordinal sequence only; this is not a global dispatch order.
    let mut first = HashMap::<NativeInputSource, (nautilus_core::UUID4, u64)>::new();
    let mut last = HashMap::<NativeInputSource, u64>::new();
    let mut initial_last = HashMap::<NativeInputSource, u64>::new();
    let mut seen = HashMap::<
        nautilus_core::UUID4,
        nautilus_common::recovery_trace::NativeIngressReceipt,
    >::new();
    let mut observe =
        |receipt: &nautilus_common::recovery_trace::NativeIngressReceipt| -> Result<()> {
            ensure!(
                receipt.channel_ordinal > 0,
                "original source channel frontier is not physically numbered"
            );
            if let Some(previous) = seen.get(&receipt.message_id) {
                ensure!(
                    previous == receipt,
                    "original source message receipt changed between cut and Begin"
                );
                return Ok(());
            }
            if let Some((channel, _)) = first.get(&receipt.input_source) {
                ensure!(
                    *channel == receipt.channel_id
                        && last[&receipt.input_source].checked_add(1)
                            == Some(receipt.channel_ordinal),
                    "original source channel frontier is foreign or has an ordinal gap"
                );
            } else {
                first.insert(
                    receipt.input_source,
                    (receipt.channel_id, receipt.channel_ordinal),
                );
            }
            last.insert(receipt.input_source, receipt.channel_ordinal);
            seen.insert(receipt.message_id, receipt.clone());
            Ok(())
        };
    for pending in &trace.cut().pending_inputs {
        observe(&pending.receipt)?;
        initial_last.insert(
            pending.receipt.input_source,
            pending.receipt.channel_ordinal,
        );
    }
    for root in trace.roots() {
        for record in root.inputs() {
            if let NativeTraceRecord::Begin { receipt, .. } = record
                && let Some(ingress) = &receipt.ingress
            {
                observe(ingress)?;
            }
        }
    }
    for pending in trace.final_pending()? {
        observe(&pending.receipt)?;
    }
    let mut result = Vec::with_capacity(first.len());
    for (source, (channel, first_ordinal)) in first {
        let next = initial_last
            .get(&source)
            .map_or(Ok(first_ordinal), |ordinal| {
                ordinal
                    .checked_add(1)
                    .context("initial source frontier exhausted")
            })?;
        result.push((source, channel, next));
    }
    Ok(result)
}

fn read_callback_ancestry(
    cut: &nautilus_event_store::native_trace::NativeTraceCheckpointCut,
    originals: &[NativePendingInput],
) -> Result<Option<crate::runner_recovery::RunnerCallbackAncestry>> {
    ensure!(
        originals
            .iter()
            .map(|input| &input.receipt)
            .eq(cut.pending.iter()),
        "original source sidecar input inventory differs from its sealed physical receipt table"
    );
    let profile = cut.native_effects.get("callback_ancestry_profile");
    let metadata = cut.native_effects.get("callback_ancestry");
    match (profile, metadata) {
        // Historical producers predate owner callback ancestry. Their original
        // independent-input profile remains readable without rewriting bytes.
        (None, None) => {
            ensure!(
                originals.is_empty(),
                "legacy original pending FIFO callback ancestry is unsupported without its source sidecar"
            );
            Ok(None)
        }
        (Some(profile), Some(metadata)) => {
            ensure!(
                profile.as_str() == Some(crate::runner_recovery::CALLBACK_ANCESTRY_PROFILE),
                "unsupported original physical FIFO callback ancestry profile"
            );
            let ancestry: crate::runner_recovery::RunnerCallbackAncestry =
                serde_json::from_value(metadata.clone())?;
            ancestry.state.validate()?;
            ensure!(
                ancestry.profile == crate::runner_recovery::CALLBACK_ANCESTRY_PROFILE
                    && ancestry
                        .receipts
                        .iter()
                        .map(Option::as_ref)
                        .eq(originals.iter().map(|input| Some(&input.receipt)))
                    && ancestry.state.entry_chains.len() == originals.len()
                    && ancestry.state.entry_context_ids.len() == originals.len(),
                "original callback ancestry does not bind the exact retained physical FIFO"
            );
            Ok(Some(ancestry))
        }
        _ => anyhow::bail!("original physical FIFO callback ancestry sidecar missing"),
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

type OriginalTimerAdmission = (String, u64, bool, nautilus_common::timer::TimeEvent, u64);

fn original_timer_admissions(trace: &VerifiedNativeTrace) -> Result<Vec<OriginalTimerAdmission>> {
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
    let mut ingress_receipts = Vec::new();
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
                let ingress = receipt
                    .ingress
                    .as_ref()
                    .context("original timer ingress receipt missing")?;
                ingress_receipts.push(ingress.clone());
                admissions.push(parse(
                    input["owner"]
                        .as_str()
                        .context("original timer owner absent")?
                        .into(),
                    &input["binding"],
                    &input["event"],
                    ingress.accepted_wall_ns,
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
        ingress_receipts.push(pending.receipt.clone());
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
    // Verified Begin/input order, followed by the sealed final pending prefix,
    // is the original physical Time-channel order. Producer wall samples occur
    // before concurrent sends and can invert by microseconds; they only govern
    // whether a lease existed at a sampled boundary, never its FIFO position.
    verify_original_timer_channel_fifo(&ingress_receipts)?;
    let mut ids = std::collections::HashSet::new();
    ensure!(
        admissions
            .iter()
            .all(|(_, _, _, event, _)| ids.insert(event.event_id)),
        "duplicate original timer event identity"
    );
    Ok(admissions)
}

fn verify_original_timer_channel_fifo(
    receipts: &[nautilus_common::recovery_trace::NativeIngressReceipt],
) -> Result<()> {
    let mut last = std::collections::HashMap::new();
    for receipt in receipts {
        ensure!(
            receipt.input_source == NativeInputSource::Time && receipt.channel_ordinal > 0,
            "original timer admission has no physical Time-channel ordinal"
        );
        if let Some(previous) = last.insert(receipt.channel_id, receipt.channel_ordinal) {
            ensure!(
                previous.checked_add(1) == Some(receipt.channel_ordinal),
                "original timer admission changed physical channel FIFO"
            );
        }
    }
    Ok(())
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

    #[test]
    #[ignore = "requires the immutable actual Capital40 sealed Redb and original cut"]
    fn actual_verified_timer_fifo_rejects_changed_ordinal_and_keeps_inverted_wall_samples() {
        let root = std::path::PathBuf::from(
            std::env::var("METIS_TEST_ORIGINAL_TIMER_FIFO_SOURCE_PATH")
                .expect("original sealed timer source path is required"),
        );
        assert!(root.is_absolute());
        let envelope: serde_json::Value =
            serde_json::from_slice(&std::fs::read(root.join("source-composite.json")).unwrap())
                .unwrap();
        let body = &envelope["body"];
        let cut: nautilus_event_store::native_trace::NativeTraceCheckpointCut =
            serde_json::from_value(
                body["payload"]["components"]["components"]["native_trace_cut"].clone(),
            )
            .unwrap();
        assert!(
            cut.pending_inputs.is_empty(),
            "the actual original cut is empty"
        );
        let reader = EventStoreReader::new(
            RedbBackend::open_sealed(
                root.join("new-journal"),
                &cut.source.node_instance.to_string(),
                &cut.source.journal_run,
            )
            .unwrap(),
        );
        let trace = reader
            .verify_native_tail(&cut.source, &cut, reader.high_watermark().unwrap())
            .unwrap();
        assert!(trace.initial_cut_sealed());
        let mut receipts = Vec::new();
        let mut ids = Vec::new();
        for original in trace.roots() {
            for record in original.inputs() {
                if let NativeTraceRecord::Begin {
                    input_source: NativeInputSource::Time,
                    receipt,
                    read_witnesses,
                    ..
                } = record
                {
                    let input = &read_witnesses
                        .iter()
                        .find(|w| w.component_id == "native:timers")
                        .unwrap()
                        .payload["input"];
                    receipts.push(receipt.ingress.as_ref().unwrap().clone());
                    ids.push(
                        serde_json::from_value::<nautilus_core::UUID4>(
                            input["event"]["event_id"].clone(),
                        )
                        .unwrap(),
                    );
                }
            }
        }
        for pending in trace.final_pending().unwrap() {
            if pending.receipt.input_source == NativeInputSource::Time {
                receipts.push(pending.receipt.clone());
                ids.push(
                    serde_json::from_value::<nautilus_core::UUID4>(
                        pending.timer_event.as_ref().unwrap()["event_id"].clone(),
                    )
                    .unwrap(),
                );
            }
        }
        assert!(
            receipts
                .windows(2)
                .any(|pair| pair[0].channel_id == pair[1].channel_id
                    && pair[0].accepted_wall_ns > pair[1].accepted_wall_ns),
            "the real source must exercise producer-sample inversion"
        );
        let ordered = original_timer_admissions(&trace).unwrap();
        assert_eq!(
            ordered.iter().map(|v| v.3.event_id).collect::<Vec<_>>(),
            ids
        );
        verify_original_timer_channel_fifo(&receipts).unwrap();
        let mut changed = receipts.clone();
        changed[1].channel_ordinal = changed[0].channel_ordinal;
        assert!(verify_original_timer_channel_fifo(&changed).is_err());
        changed[1].channel_ordinal = receipts[1].channel_ordinal.checked_add(1).unwrap();
        assert!(verify_original_timer_channel_fifo(&changed).is_err());
        changed[1].input_source = NativeInputSource::DataEvent;
        assert!(verify_original_timer_channel_fifo(&changed).is_err());
    }

    #[derive(Debug)]
    struct InstrumentCodec;
    impl RunnerRecoveryCodec for InstrumentCodec {
        fn channel(&self) -> RunnerRecoveryChannel {
            RunnerRecoveryChannel::DataEvent
        }
        fn codec_id(&self) -> &'static str {
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
            .with_delay_post_stop_secs(0)
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
        assert_eq!(node.kernel.config.timeout_shutdown(), Duration::ZERO);
        assert_eq!(node.kernel.delay_post_stop(), Duration::ZERO);
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
                } else if let Some(message) = input.downcast_ref::<nautilus_common::runner::TimeEventMessage>() {
                    let actual = message.checkpoint_callback_binding();
                    anyhow::ensure!(
                        actual["owner_thread_matches"].as_bool() == Some(true)
                            && matches!(actual["kind"].as_str(),
                                Some("registered_owner_thread" | "registered_cleanup"))
                            && actual["binding_id"].as_u64().is_some_and(|id| id > 0),
                        "actual observer timer has no owner-bound dispatch lease"
                    );
                    let event = message.event();
                    serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id,
                        "ts_event":event.ts_event,"ts_init":event.ts_init,
                        "callback":message.native_input_callback_binding()})
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

    /// Uses two actual `LiveNode`s, a real source runloop, durable redb Journal,
    /// native checkpoint barriers and original `DataEngine` handlers. No venue or
    /// DTO completion fixture supplies the source root.
    #[rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test(flavor = "current_thread")]
    async fn actual_running_native_tail_replays_instrument_and_refuses_changed_original_input(
        #[case] changed: bool,
    ) {
        let root = std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(std::env::temp_dir, std::path::PathBuf::from)
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
                |()| Ok(()),
                |_| {},
            )
            .unwrap();
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let sender = source.runner.as_ref().unwrap().data_event_sender_clone();
        let handle = source.handle();
        let produced = async move {
            initial.notified().await;
            sender
                .send(DataEvent::Instrument(instrument).into())
                .unwrap();
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
        // Receipt/callback scopes belong to one actual node owner thread and
        // cannot be reset or reused by a second source installation. The child
        // reads and verifies the original sealed Journal on its own owner.
        let child_root = root.clone();
        std::thread::spawn(move || {
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            executor.block_on(async move {
                let root = child_root;
                let reader = EventStoreReader::new(
                    RedbBackend::open_sealed(
                        root.join("source"),
                        &source_instance.to_string(),
                        &source_identity.journal_run,
                    )
                    .unwrap(),
                );
                let verified = reader
                    .verify_native_tail(
                        &source_identity,
                        &first.0,
                        reader.high_watermark().unwrap(),
                    )
                    .unwrap();
                assert!(verified.roots().iter().any(|root| root.inputs().iter().any(
                    |row| matches!(
                        row,
                        NativeTraceRecord::Begin {
                            input_source: NativeInputSource::DataEvent,
                            ..
                        }
                    )
                )));
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
                let child_source = NativeTraceSource {
                    node_instance: target.kernel.instance_id(),
                    process_incarnation: nautilus_core::UUID4::new(),
                    journal_run: target
                        .kernel
                        .event_store()
                        .unwrap()
                        .run_id()
                        .unwrap()
                        .to_owned(),
                    ..source_identity.clone()
                };
                let child_trace = target
                    .kernel
                    .event_store()
                    .unwrap()
                    .native_trace_handle(child_source.clone())
                    .unwrap()
                    .downcast::<nautilus_event_store::native_trace::NativeTraceRecorder>()
                    .unwrap();
                target
                    .set_native_recovery_trace(
                        child_trace.as_ref().clone(),
                        |_, _, _| Ok(vec![]),
                        |_| Ok(()),
                    )
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
                let legacy_error = target
                    .recovery_timers
                    .as_ref()
                    .unwrap()
                    .verify_native_continuation(
                        &child_trace,
                        &watermark,
                        target.instance_id(),
                        true,
                        &[],
                    )
                    .unwrap_err();
                assert!(legacy_error.to_string().contains("legacy retained timers"));
                let result = target.replay_native_tail(
                    &verified,
                    &watermark,
                    |_, begin| {
                        let NativeTraceRecord::Begin { payload, .. } = begin else {
                            unreachable!()
                        };
                        let mut original: InstrumentAny = serde_json::from_value(payload.clone())?;
                        if changed {
                            // Official removed the BitMEX stub; its BTC inverse
                            // perpetual equivalent remains distinct from this
                            // source's linear ETH payload, so mismatch must fail.
                            original = InstrumentAny::CryptoPerpetual(
                                nautilus_model::instruments::stubs::btcusd_bybit(),
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
                    let audit = receipt
                        .trace_continuation()
                        .expect("actual child trace continuation absent");
                    assert_eq!(audit.child(), &child_source);
                    assert_eq!(audit.original(), verified.source());
                    assert!(audit.acknowledgment().sequence() > 0);
                    let timers = target.recovery_timers.as_ref().unwrap();
                    // Original owner clocks remain restored; only this actual reader/child
                    // audit admits their causal inventory to a subsequent native cut.
                    timers
                        .verify_native_continuation(
                            &child_trace,
                            receipt.final_watermark(),
                            target.instance_id(),
                            true,
                            receipt.retained_inputs(),
                        )
                        .unwrap();
                    let mut wrong_frontier = receipt.final_watermark().clone();
                    wrong_frontier.dispatch_watermark += 1;
                    assert!(
                        timers
                            .verify_native_continuation(
                                &child_trace,
                                &wrong_frontier,
                                target.instance_id(),
                                true,
                                receipt.retained_inputs(),
                            )
                            .is_err()
                    );
                    assert!(
                        timers
                            .verify_native_continuation(
                                &child_trace,
                                receipt.final_watermark(),
                                nautilus_core::UUID4::new(),
                                true,
                                receipt.retained_inputs(),
                            )
                            .is_err()
                    );
                    // Continue through the actual original receiver and native DataEngine.
                    target
                        .runner
                        .as_ref()
                        .unwrap()
                        .data_event_sender_clone()
                        .send(
                            DataEvent::Instrument(InstrumentAny::CryptoPerpetual(
                                nautilus_model::instruments::stubs::btcusd_bybit(),
                            ))
                            .into(),
                        )
                        .unwrap();
                    let fresh = target
                        .runner
                        .as_mut()
                        .unwrap()
                        .startup_channels_mut()
                        .data_evt_rx
                        .try_recv()
                        .unwrap();
                    target.process_data_event(fresh);
                    assert!(!target.dispatch_failure.get());
                    let completion = target
                        .dispatch_observer
                        .as_ref()
                        .unwrap()
                        .completed_root_boundary_proof()
                        .unwrap()
                        .unwrap();
                    completion.verify().unwrap();
                    assert!(completion.input_sequence() > audit.last_dispatch_input());
                    let frozen = target.runner.as_ref().unwrap().freeze_ingress().unwrap();
                    assert!(
                        target
                            .runner
                            .as_ref()
                            .unwrap()
                            .pending_queue_counts()
                            .values()
                            .all(|count| *count == 0)
                    );
                    target
                        .recovery_timers
                        .as_ref()
                        .unwrap()
                        .verify_native_continuation(
                            &child_trace,
                            receipt.final_watermark(),
                            target.instance_id(),
                            true,
                            &[],
                        )
                        .unwrap();
                    let fresh_cut = child_trace
                        .checkpoint_cut(
                            completion.root_sequence(),
                            completion.input_sequence(),
                            "actual-child-fresh-native-inventory".into(),
                            vec![],
                        )
                        .unwrap();
                    child_trace
                        .persist_checkpoint(&fresh_cut, serde_json::Value::Null)
                        .unwrap();
                    frozen.finish().unwrap();
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
            });
        })
        .join()
        .expect("actual native child owner worker failed");
        drop(trace);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[derive(Debug)]
    struct GuardCommandCodec;
    impl RunnerRecoveryCodec for GuardCommandCodec {
        fn channel(&self) -> RunnerRecoveryChannel {
            RunnerRecoveryChannel::ExecutionCommand
        }
        fn codec_id(&self) -> &'static str {
            "actual_registered_submission_guard_tail.v1"
        }
        fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
            let RunnerRecoveryEventRef::ExecutionCommand(command) = event else {
                anyhow::bail!("registered guard source has a different channel");
            };
            Ok(
                serde_json::json!({"endpoint":command.endpoint().to_string(),
                "command":command.command()}),
            )
        }
        fn decode(&self, input: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            Ok(RunnerRecoveryEvent::ExecutionCommand(
                nautilus_common::runner::TradingCommandMessage::new(
                    input.payload["endpoint"]
                        .as_str()
                        .context("guard command endpoint absent")?
                        .into(),
                    serde_json::from_value(input.payload["command"].clone())?,
                ),
            ))
        }
    }

    fn guard_command_registry() -> Rc<RunnerRecoveryCodecRegistry> {
        let mut registry =
            RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::ExecutionCommand]);
        registry.register(GuardCommandCodec).unwrap();
        Rc::new(registry.seal().unwrap())
    }

    // The guard is invoked through the real registered ExecutionEngine path.
    // Only its actual native_transport outcome is replayed. The source's local
    // approval branch is not a CQS grant; target phase gates remain closed.
    fn install_original_guard(
        node: &LiveNode,
        approved: bool,
    ) -> (Rc<std::cell::Cell<usize>>, Rc<std::cell::Cell<usize>>) {
        let guard_calls = Rc::new(std::cell::Cell::new(0));
        let current_calls = Rc::new(std::cell::Cell::new(0));
        let calls = guard_calls.clone();
        let current = current_calls.clone();
        node.kernel
            .exec_engine
            .borrow_mut()
            .set_submission_guard(Some(Rc::new(move |command| {
                calls.set(calls.get() + 1);
                nautilus_common::recovery_trace::native_transport(
                    "ACTUAL-REGISTERED-GUARD",
                    "execution.root_guard",
                    command,
                    || {
                        current.set(current.get() + 1);
                        anyhow::ensure!(approved, "node execution not activated");
                        Ok(())
                    },
                )
            })));
        (guard_calls, current_calls)
    }

    async fn run_actual_original_guard_tail(approved: bool) {
        use nautilus_common::{
            messages::execution::{SubmitOrder, TradingCommand},
            msgbus::MessagingSwitchboard,
            runner::TradingCommandMessage,
        };
        use nautilus_core::UUID4;
        use nautilus_model::{
            enums::{OrderStatus, OrderType},
            identifiers::{ClientOrderId, StrategyId},
            orders::{Order, OrderTestBuilder},
            types::Quantity,
        };
        let directory = std::env::var_os("CARGO_TARGET_DIR")
            .map_or_else(std::env::temp_dir, std::path::PathBuf::from)
            .join(format!("original-guard-tail-{}", UUID4::new()));
        std::fs::create_dir_all(&directory).unwrap();
        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(TraderId::from("NATIVE-TAIL-001"))
            .strategy_id(StrategyId::from("GUARD-TAIL-001"))
            .instrument_id(crypto_perpetual_ethusdt().id())
            .client_order_id(ClientOrderId::from("O-ORIGINAL-GUARD-001"))
            .quantity(Quantity::from(1))
            .build();
        let mut source = actual_node("guard-source", directory.join("source"), None);
        source
            .kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
        let (source_guard, source_current) = install_original_guard(&source, approved);
        let instance = source.kernel.instance_id();
        let recorder = source
            .prepare_owned_native_trace(
                "actual-guard-business-run".into(),
                nautilus_event_store::native_trace::native_inventory_digest(
                    &serde_json::to_value(&source.config).unwrap(),
                )
                .unwrap(),
                "actual_registered_submission_guard_tail.v1".into(),
                "actual_empty_registered_components.v1".into(),
                |_, _, _| Ok(vec![]),
                |_| Ok(()),
            )
            .unwrap();
        let cuts = Rc::new(RefCell::new(vec![]));
        let initial = Rc::new(tokio::sync::Notify::new());
        let progress = Rc::new(tokio::sync::Notify::new());
        let saved = cuts.clone();
        let ready = initial.clone();
        let advanced = progress.clone();
        source
            .set_running_checkpoint_handler(
                guard_command_registry(),
                RunningCheckpointSchedule::EveryCompletedRoot,
                move |boundary| {
                    boundary.verify()?;
                    let cut = boundary
                        .native_trace_cut()
                        .context("actual guard source cut absent")?
                        .clone();
                    boundary
                        .persist_native_checkpoint(serde_json::json!({"registered_guard":true}))?;
                    saved.borrow_mut().push((
                        cut,
                        serde_json::to_value(boundary.inventory())?,
                        boundary.components().clone(),
                    ));
                    if saved.borrow().len() == 1 {
                        ready.notify_one();
                    } else {
                        advanced.notify_one();
                    }
                    Ok(())
                },
                |()| Ok(()),
                |_| {},
            )
            .unwrap();
        let command = TradingCommand::SubmitOrder(SubmitOrder::from_order(
            &order,
            order.trader_id(),
            None,
            None,
            UUID4::new(),
            nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
        ));
        let sender = source
            .runner
            .as_ref()
            .unwrap()
            .execution_command_sender_clone();
        let stop = source.handle();
        let command_calls = source_guard.clone();
        let produced = async move {
            initial.notified().await;
            sender
                .send(
                    TradingCommandMessage::new(
                        MessagingSwitchboard::exec_engine_execute(),
                        command,
                    )
                    .into(),
                )
                .unwrap();
            loop {
                progress.notified().await;
                if command_calls.get() == 1 {
                    break;
                }
            }
            stop.stop();
        };
        let mut producer = std::pin::pin!(produced);
        tokio::time::timeout(Duration::from_secs(15), async {
            let mut running = std::pin::pin!(source.run_with_mode(NodeRunMode::Hosted));
            tokio::select! { result = &mut running => result, () = &mut producer => running.await }
        })
        .await
        .expect("actual guard source did not complete within 15s")
        .unwrap();
        assert_eq!(source_guard.get(), 1);
        assert_eq!(source_current.get(), 1);
        assert!(source.kernel.exec_engine.borrow().client_ids().is_empty());
        {
            let cache = source.kernel.cache.borrow();
            let original = cache.order(&order.client_order_id()).unwrap();
            let nautilus_model::events::OrderEventAny::Denied(denied) = original.last_event()
            else {
                panic!("actual guard source did not record an OrderDenied terminal");
            };
            if approved {
                assert!(denied.reason.as_str().starts_with("NO_EXECUTION_CLIENT"));
            } else {
                assert_eq!(
                    denied.reason.as_str(),
                    "Execution persistence/authorization barrier failed"
                );
            }
        }
        let original_order = serde_json::to_value(
            source
                .kernel
                .cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .clone(),
        )
        .unwrap();
        assert_eq!(
            source
                .kernel
                .cache
                .borrow()
                .order(&order.client_order_id())
                .unwrap()
                .status(),
            OrderStatus::Denied
        );
        let first = cuts.borrow().first().unwrap().clone();
        let identity = recorder.source().unwrap();
        source.dispose();
        drop(source);

        // A new node owner reads the actual sealed Journal, never a public
        // Begin DTO or synthetic transport receipt standing in for that source.
        let child_directory = directory.clone();
        std::thread::spawn(move || {
            let executor = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            executor.block_on(async move {
                let reader = EventStoreReader::new(
                    RedbBackend::open_sealed(
                        child_directory.join("source"),
                        &instance.to_string(),
                        &identity.journal_run,
                    )
                    .unwrap(),
                );
                let high = reader.high_watermark().unwrap();
                let verified = reader
                    .verify_native_tail(&identity, &first.0, high)
                    .unwrap();
                let originals: Vec<_> = verified
                    .roots()
                    .iter()
                    .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
                    .filter_map(|input| match input {
                        NativeTraceRecord::Complete { transports, .. } => Some(transports),
                        _ => None,
                    })
                    .flatten()
                    .filter(|receipt| receipt.operation == "execution.root_guard")
                    .collect();
                assert_eq!(originals.len(), 1);
                assert_eq!(
                    originals[0].outcome,
                    if approved {
                        Ok(())
                    } else {
                        Err("node execution not activated".into())
                    }
                );
                let fingerprint = EventStoreLifecycle::sealed_run_fingerprint(
                    &child_directory.join("source"),
                    instance,
                    &identity.journal_run,
                    high,
                )
                .unwrap();
                let mut target = actual_node(
                    "guard-target",
                    child_directory.join("target"),
                    Some((
                        child_directory.join("source"),
                        instance,
                        identity.journal_run.clone(),
                        high,
                        fingerprint,
                    )),
                );
                let mut cache = Cache::default();
                cache.add_order(order.clone(), None, None, false).unwrap();
                target.restore_native_cache(cache).unwrap();
                target.restore_component_state(&first.2).unwrap();
                target
                    .kernel
                    .risk_engine
                    .borrow_mut()
                    .set_trading_state(nautilus_model::enums::TradingState::Halted);
                let (target_guard, target_current) = install_original_guard(&target, approved);
                let observation = target
                    .kernel
                    .exec_engine
                    .borrow()
                    .begin_recovery_observation()
                    .unwrap();
                target
                    .kernel
                    .open_event_store_for_paused_recovery()
                    .unwrap();
                let watermark = RunnerRecoveryWatermark {
                    recovery_id: "actual-original-registered-guard-cut".into(),
                    checkpoint_sequence: first.0.prefix.sequence,
                    dispatch_watermark: first.0.last_input_sequence,
                };
                target
                    .replay_recovery_events(&watermark, &[], &guard_command_registry(), |_, _| {
                        Ok(())
                    })
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
                        vec![],
                        &watermark,
                    )
                    .unwrap();
                target
                    .replay_native_tail(
                        &verified,
                        &watermark,
                        |_, begin| {
                            let NativeTraceRecord::Begin {
                                input_source: NativeInputSource::TradingCommand,
                                payload,
                                ..
                            } = begin
                            else {
                                anyhow::bail!("actual guard source decoder changed channel");
                            };
                            Ok(RunnerRecoveryEvent::ExecutionCommand(
                                TradingCommandMessage::new(
                                    payload["endpoint"]
                                        .as_str()
                                        .context("actual guard endpoint absent")?
                                        .into(),
                                    serde_json::from_value(payload["command"].clone())?,
                                ),
                            ))
                        },
                        |_, _| anyhow::bail!("actual guard source has no retained input"),
                    )
                    .unwrap();
                assert_eq!(
                    target_guard.get(),
                    1,
                    "historical path skipped the actual registered guard"
                );
                assert_eq!(
                    target_current.get(),
                    0,
                    "historical guard invoked current transport/permission closure"
                );
                assert_eq!(
                    serde_json::to_value(
                        target
                            .kernel
                            .cache
                            .borrow()
                            .order(&order.client_order_id())
                            .unwrap()
                            .clone()
                    )
                    .unwrap(),
                    original_order
                );
                assert!(
                    target
                        .kernel
                        .exec_engine
                        .borrow()
                        .recovery_observation_fenced()
                );
                assert!(!target.kernel.exec_engine.borrow().submissions_fenced());
                assert!(target.kernel.exec_engine.borrow().client_ids().is_empty());
                assert!(target.recovery_requires_release);
                assert_eq!(target.state(), NodeState::Idle);
                assert_eq!(
                    target.kernel.risk_engine.borrow().trading_state(),
                    nautilus_model::enums::TradingState::Halted
                );
                // After the original scope ends, even the successful old guard
                // cannot authorize a current command or open the phase gate.
                let current_command = TradingCommand::SubmitOrder(SubmitOrder::from_order(
                    &order,
                    order.trader_id(),
                    None,
                    None,
                    UUID4::new(),
                    nautilus_core::time::get_atomic_clock_realtime().get_time_ns(),
                ));
                target.kernel.exec_engine.borrow().execute(current_command);
                assert_eq!(target_guard.get(), 1);
                assert_eq!(target_current.get(), 0);
                assert!(
                    target
                        .kernel
                        .exec_engine
                        .borrow()
                        .recovery_observation_fenced()
                );
                target
                    .kernel
                    .prohibit_event_store_seal(
                        "actual guard replay retains child without authenticated release",
                    )
                    .unwrap();
                drop(observation);
                // Dropping the unfinished phase capability retains the closed
                // observation gate; it does not invent a permanent failure.
                assert!(
                    target
                        .kernel
                        .exec_engine
                        .borrow()
                        .recovery_observation_fenced()
                );
                target.kernel.exec_engine.borrow().failure_fence().latch();
                assert!(target.kernel.exec_engine.borrow().submissions_fenced());
                target.dispose();
                drop(target);
                drop(reader);
            });
        })
        .join()
        .expect("actual original guard child owner failed");
        drop(recorder);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actual_reader_guard_denial_replays_original_terminal_before_client_routing() {
        Box::pin(run_actual_original_guard_tail(false)).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actual_reader_approved_guard_keeps_current_observation_fence_and_transport_closed() {
        Box::pin(run_actual_original_guard_tail(true)).await;
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "native_tail_framework_tests.rs"]
mod framework_tests;

#[cfg(test)]
#[path = "native_tail_uuid_tests.rs"]
mod uuid_tests;
