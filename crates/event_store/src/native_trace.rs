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

//! Durable native input/output causality in the actual Journal.
//!
//! Readers issue immutable source proofs. Neither serialized cuts nor successful local
//! trace completion issue a recovery release or establish external order acceptance.

use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
    sync::{Arc, Weak},
};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use nautilus_common::{
    live::dst,
    recovery_trace::{
        NativeIngressReceipt, NativeInputSource, NativeProcessingReceipt, NativeReadWitness,
        NativeTraceRecord, NativeTraceSource,
        scope::{self, NativeCaptureScope},
    },
};
use nautilus_core::{UnixNanos, time::nanos_since_unix_epoch};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ustr::Ustr;

use crate::{
    EventStore, EventStoreEntry, EventStoreReader, Headers,
    kernel::HaltSignal,
    writer::{
        DurableEntryAcknowledgment, DurableJournalPrefix, EntryDraft, EventStoreWriter, HaltReason,
    },
};

pub const NATIVE_TRACE_PAYLOAD_TYPE: &str = "NativeRecoveryTrace.v1";
pub const NATIVE_CHECKPOINT_PAYLOAD_TYPE: &str = "NativeRecoveryCheckpoint.v1";

/// Explicit non-economic checkpoint metadata in the source Journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTraceCheckpointRecord {
    pub cut: NativeTraceCheckpointCut,
    pub host_checkpoint: Value,
}

/// Canonical digest of the actual native inventory collected inside the frozen boundary.
/// # Errors
/// Refuses an unserializable inventory.
pub fn native_inventory_digest(inventory: &Value) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"nautilus-native-checkpoint-inventory/v1");
    hasher.update(&serde_json::to_vec(inventory)?);
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Debug)]
struct State {
    source: NativeTraceSource,
    writer: Weak<EventStoreWriter>,
    halt: HaltSignal,
    process_start: dst::time::Instant,
    next_input: u64,
    next_root: u64,
    stack: Vec<(u64, u64)>,
    completed: Option<(u64, u64)>,
    last_acknowledgment: Option<DurableEntryAcknowledgment>,
    failure: Option<String>,
    last_native_effects: Option<Value>,
}

/// A handle issued from an actual open lifecycle, bound to its writer and native run.
#[derive(Clone, Debug)]
pub struct NativeTraceRecorder(Rc<RefCell<State>>);

impl NativeTraceRecorder {
    pub(crate) fn new(
        source: NativeTraceSource,
        writer: &Arc<EventStoreWriter>,
        halt: HaltSignal,
    ) -> Result<Self> {
        source.validate()?;
        ensure!(!halt.is_halted(), "native trace Journal halted");
        Ok(Self(Rc::new(RefCell::new(State {
            source,
            writer: Arc::downgrade(writer),
            halt,
            process_start: dst::time::Instant::now(),
            next_input: 1,
            next_root: 1,
            stack: Vec::new(),
            completed: None,
            last_acknowledgment: None,
            failure: None,
            last_native_effects: None,
        }))))
    }

    /// Returns the actual lifecycle's source binding.
    ///
    /// # Errors
    /// Refuses reentrancy or a failed source.
    pub fn source(&self) -> Result<NativeTraceSource> {
        let state = self
            .0
            .try_borrow()
            .context("native trace recorder reentered")?;
        state.check()?;
        Ok(state.source.clone())
    }

    /// Acknowledges the exact original native input before synchronous effects run.
    /// Callers must use a real node producer, not an output row or cache replay closure.
    ///
    /// # Errors
    /// Refuses noncontiguous frontiers, mismatched nesting, missing ingress evidence,
    /// invalid dependencies or failed actual append acknowledgments.
    #[expect(
        clippy::too_many_arguments,
        reason = "native trace retains the complete dispatch identity"
    )]
    pub fn begin_native(
        &self,
        root: u64,
        input: u64,
        parent: Option<u64>,
        input_source: NativeInputSource,
        phase: &str,
        payload: Value,
        read_witnesses: Vec<NativeReadWitness>,
    ) -> Result<NativeTraceGuard> {
        ensure!(
            !phase.is_empty() && phase.len() <= 128,
            "invalid native trace phase"
        );
        let mut state = self
            .0
            .try_borrow_mut()
            .context("native trace recorder reentered")?;
        state.check()?;
        ensure!(
            input == state.next_input,
            "native trace input sequence mismatch"
        );
        match state.stack.last() {
            Some((previous_root, previous_input)) => ensure!(
                *previous_root == root && Some(*previous_input) == parent,
                "native trace stack parent mismatch"
            ),
            None => ensure!(
                root == state.next_root && parent.is_none(),
                "native trace root sequence mismatch"
            ),
        }
        let next_input = input
            .checked_add(1)
            .context("native trace input exhausted")?;
        let next_root = if parent.is_none() {
            root.checked_add(1).context("native trace root exhausted")?
        } else {
            state.next_root
        };
        let ingress = scope::take_ingress(input_source)?;
        if let Some(receipt) = &ingress {
            ensure!(
                receipt.channel_ordinal > 0,
                "native ingress ordinal unavailable"
            );
        }
        let activity_at = dst::time::Instant::now();
        let receipt = state.receipt(activity_at, ingress)?;
        let mut dependencies = BTreeSet::new();
        for witness in &read_witnesses {
            ensure!(
                !witness.component_id.is_empty()
                    && !witness.profile.is_empty()
                    && !witness.source_version.is_empty()
                    && dependencies.insert((&witness.component_id, &witness.profile)),
                "invalid or duplicate native read witness"
            );
        }
        let record = NativeTraceRecord::Begin {
            source: state.source.clone(),
            root_sequence: root,
            input_sequence: input,
            stack_parent: parent,
            input_source,
            phase: phase.into(),
            receipt,
            payload,
            read_witnesses,
        };
        state.append(&record)?;
        let capture = NativeCaptureScope::enter(&state.source, root, input, parent, activity_at)?;
        state.next_input = next_input;
        state.next_root = next_root;
        state.stack.push((root, input));
        Ok(NativeTraceGuard {
            recorder: self.clone(),
            root,
            input,
            capture: Some(capture),
            completed: false,
        })
    }

    /// Fingerprints the actual Journal prefix at this completed native source boundary.
    /// The host still has to persist and revalidate its actual composite inventory.
    ///
    /// # Errors
    /// Refuses active/failed source work, mismatched roots, absent receipt inventory
    /// or unavailable actual backend prefix acknowledgments.
    pub fn checkpoint_cut(
        &self,
        root: u64,
        input: u64,
        native_inventory_digest: String,
        pending: Vec<NativeIngressReceipt>,
    ) -> Result<NativeTraceCheckpointCut> {
        self.checkpoint_cut_at(
            root,
            input,
            native_inventory_digest,
            pending,
            dst::time::Instant::now(),
            nanos_since_unix_epoch(),
        )
    }
    /// Captures the same native monotonic reference used for manager ages.
    #[allow(clippy::too_many_arguments)]
    pub fn checkpoint_cut_at(
        &self,
        root: u64,
        input: u64,
        native_inventory_digest: String,
        pending: Vec<NativeIngressReceipt>,
        at: dst::time::Instant,
        captured_at_ns: u64,
    ) -> Result<NativeTraceCheckpointCut> {
        let state = self
            .0
            .try_borrow()
            .context("native trace recorder reentered")?;
        state.check()?;
        ensure!(
            state.stack.is_empty() && state.completed == Some((root, input)),
            "native trace cut is not the same completed root"
        );
        ensure!(
            !native_inventory_digest.is_empty(),
            "native trace cut inventory absent"
        );
        let mut messages = BTreeSet::new();
        for receipt in &pending {
            ensure!(
                receipt.channel_ordinal > 0 && messages.insert(receipt.message_id.to_string()),
                "invalid native pending receipt inventory"
            );
        }
        let prefix = state.writer()?.durable_prefix()?;
        ensure!(
            prefix.run_id == state.source.journal_run,
            "native trace cut writer changed"
        );
        let ack = state
            .last_acknowledgment
            .as_ref()
            .context("native trace acknowledgment absent")?;
        ensure!(
            prefix.sequence >= ack.sequence(),
            "native trace cut precedes completion"
        );
        Ok(NativeTraceCheckpointCut {
            schema_version: 1,
            source: state.source.clone(),
            completed_root: root,
            completed_input: input,
            last_input_sequence: state.next_input - 1,
            last_trace_sequence: ack.sequence(),
            last_trace_entry_hash: ack.entry_hash().to_hex(),
            prefix,
            captured_at_ns,
            captured_process_elapsed_ns: u64::try_from(
                at.checked_duration_since(state.process_start)
                    .context("native cut process clock moved backwards")?
                    .as_nanos(),
            )?,
            native_inventory_digest,
            pending,
            pending_inputs: Vec::new(),
            registered_timers: BTreeMap::new(),
            native_effects: state
                .last_native_effects
                .clone()
                .context("native completed effects absent")?,
        })
    }

    /// Appends the host checkpoint as explicit metadata at the same frozen native cut.
    /// The host must place its complete immutable artifact bindings in this record;
    /// subsequent readers do not classify arbitrary unjoined rows as checkpoints.
    ///
    /// # Errors
    /// Refuses a substituted cut, source progress, interleaved Journal output or lost ACK.
    pub fn persist_checkpoint(
        &self,
        cut: &NativeTraceCheckpointCut,
        host_checkpoint: Value,
    ) -> Result<DurableEntryAcknowledgment> {
        let mut state = self
            .0
            .try_borrow_mut()
            .context("native checkpoint reentered")?;
        state.check()?;
        ensure!(
            state.stack.is_empty()
                && cut.source == state.source
                && state.completed == Some((cut.completed_root, cut.completed_input))
                && state.next_input.checked_sub(1) == Some(cut.last_input_sequence),
            "native checkpoint source frontier changed"
        );
        let writer = state.writer()?;
        ensure!(
            writer.durable_prefix()? == cut.prefix,
            "Journal changed after native cut"
        );
        let previous = state
            .last_acknowledgment
            .as_ref()
            .context("native completion ACK absent")?;
        ensure!(
            previous.sequence() == cut.last_trace_sequence
                && previous.entry_hash().to_hex() == cut.last_trace_entry_hash,
            "native completion ACK changed"
        );
        let record = NativeTraceCheckpointRecord {
            cut: cut.clone(),
            host_checkpoint,
        };
        let payload = Bytes::from(rmp_serde::to_vec_named(&record)?);
        state.failure = Some("native checkpoint append interrupted".into());
        let outcome = writer.append_durable(EntryDraft::without_indices(
            Headers::empty(),
            "run.recovery.native_checkpoint".into(),
            Ustr::from(NATIVE_CHECKPOINT_PAYLOAD_TYPE),
            payload,
            UnixNanos::from(nanos_since_unix_epoch()),
        ));
        match outcome {
            Ok(ack) => {
                state.failure = None;
                Ok(ack)
            }
            Err(error) => {
                state.halt.callback()(HaltReason::ExternalPersistence(error.to_string()));
                Err(error.into())
            }
        }
    }

    /// Permanently records a source failure and prevents later successful completions/seals.
    pub fn fail(&self, reason: &str) {
        if let Ok(mut state) = self.0.try_borrow_mut() {
            state.failure.get_or_insert_with(|| reason.into());
            state.halt.callback()(HaltReason::ExternalPersistence(reason.into()));
        }
    }
}

impl State {
    fn check(&self) -> Result<()> {
        ensure!(
            self.failure.is_none() && !self.halt.is_halted(),
            "native trace source failed: {:?}",
            self.failure
        );
        Ok(())
    }
    fn writer(&self) -> Result<Arc<EventStoreWriter>> {
        self.writer.upgrade().context("native trace writer closed")
    }
    fn receipt(
        &self,
        instant: dst::time::Instant,
        ingress: Option<NativeIngressReceipt>,
    ) -> Result<NativeProcessingReceipt> {
        let process_elapsed_ns = instant
            .checked_duration_since(self.process_start)
            .context("native source monotonic clock moved backwards")?
            .as_nanos()
            .try_into()?;
        let wall_ns = nanos_since_unix_epoch();
        if let Some(ingress) = &ingress {
            ensure!(
                ingress.accepted_wall_ns <= wall_ns,
                "native source wall clock moved backwards"
            );
        }
        Ok(NativeProcessingReceipt {
            wall_ns,
            process_elapsed_ns,
            ingress,
        })
    }
    fn append(&mut self, record: &NativeTraceRecord) -> Result<()> {
        self.check()?;
        // A panic or lost acknowledgment permanently poisons this source
        self.failure = Some("native trace durable append interrupted".into());
        let payload = Bytes::from(rmp_serde::to_vec_named(record)?);
        let writer = self.writer()?;
        let ack = writer.append_durable(EntryDraft::without_indices(
            Headers::empty(),
            "run.recovery.native_trace".into(),
            Ustr::from(NATIVE_TRACE_PAYLOAD_TYPE),
            payload,
            UnixNanos::from(nanos_since_unix_epoch()),
        ));
        match ack {
            Ok(ack) => {
                ensure!(
                    self.last_acknowledgment
                        .as_ref()
                        .is_none_or(|previous| ack.sequence() > previous.sequence()),
                    "native trace acknowledgment did not advance"
                );
                self.last_acknowledgment = Some(ack);
                self.failure = None;
                Ok(())
            }
            Err(e) => {
                self.halt.callback()(HaltReason::ExternalPersistence(e.to_string()));
                Err(e.into())
            }
        }
    }
}

/// Owns one source input until all synchronous effects and the Complete row commit.
#[derive(Debug)]
pub struct NativeTraceGuard {
    recorder: NativeTraceRecorder,
    root: u64,
    input: u64,
    capture: Option<NativeCaptureScope>,
    completed: bool,
}
impl NativeTraceGuard {
    /// Returns the same actual writer-bound failure latch for downstream projection checks.
    #[must_use]
    pub fn recorder_handle(&self) -> NativeTraceRecorder {
        self.recorder.clone()
    }

    /// Acknowledges the actual native effects after their synchronous Journal captures.
    ///
    /// # Errors
    /// Refuses missing/changed scopes or any failed durable append.
    pub fn complete(mut self, native_effects: Value) -> Result<()> {
        let output_count = self
            .capture
            .as_ref()
            .context("native capture scope absent")?
            .output_count()?;
        let mut state = self
            .recorder
            .0
            .try_borrow_mut()
            .context("native trace recorder reentered")?;
        state.check()?;
        ensure!(
            state.stack.last() == Some(&(self.root, self.input)),
            "native trace completion out of order"
        );
        let receipt = state.receipt(dst::time::Instant::now(), None)?;
        let completed_effects = native_effects.clone();
        let record = NativeTraceRecord::Complete {
            source: state.source.clone(),
            root_sequence: self.root,
            input_sequence: self.input,
            receipt,
            output_count,
            native_effects,
            transports: self
                .capture
                .as_ref()
                .context("native source capture absent")?
                .transports()?,
            uuid_draws: self
                .capture
                .as_ref()
                .context("native source capture absent")?
                .uuid_draws()?,
            clock_reads: self
                .capture
                .as_ref()
                .context("native source capture absent")?
                .clock_reads()?,
            queued_outputs: self
                .capture
                .as_ref()
                .context("native source capture absent")?
                .queued_outputs()?,
            callbacks: self
                .capture
                .as_ref()
                .context("native source capture absent")?
                .callbacks()?,
        };
        state.append(&record)?;
        self.capture
            .take()
            .context("native capture scope absent")?
            .finish()?;
        state.stack.pop();
        if state.stack.is_empty() {
            state.completed = Some((self.root, self.input));
            state.last_native_effects = Some(completed_effects);
        }
        self.completed = true;
        Ok(())
    }
}
impl Drop for NativeTraceGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.recorder
                .fail("native input aborted before durable completion");
        }
    }
}

/// Source fields persisted by the native cut. Reading this DTO alone is not a replay proof.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTraceCheckpointCut {
    pub schema_version: u32,
    pub source: NativeTraceSource,
    pub completed_root: u64,
    pub completed_input: u64,
    pub last_input_sequence: u64,
    pub last_trace_sequence: u64,
    pub last_trace_entry_hash: String,
    pub prefix: DurableJournalPrefix,
    pub captured_at_ns: u64,
    pub captured_process_elapsed_ns: u64,
    pub native_inventory_digest: String,
    pub pending: Vec<NativeIngressReceipt>,
    #[serde(default)]
    pub pending_inputs: Vec<nautilus_common::recovery_trace::NativePendingInput>,
    #[serde(default)]
    pub registered_timers: BTreeMap<String, Value>,
    #[serde(default)]
    pub native_effects: Value,
}

/// A complete source root issued only by the actual Journal reader.
#[derive(Clone, Debug)]
pub struct VerifiedNativeRoot {
    source: NativeTraceSource,
    root_sequence: u64,
    inputs: Vec<NativeTraceRecord>,
    entries: Vec<EventStoreEntry>,
}
impl VerifiedNativeRoot {
    #[must_use]
    pub const fn source(&self) -> &NativeTraceSource {
        &self.source
    }
    #[must_use]
    pub const fn root_sequence(&self) -> u64 {
        self.root_sequence
    }
    /// Returns original verified source records; none refer to current authority.
    #[must_use]
    pub fn inputs(&self) -> &[NativeTraceRecord] {
        &self.inputs
    }
    /// Builds exact source output expectations for one input from this reader's
    /// verified native closure. Current child capture metadata is never substituted.
    pub fn historical_outputs(
        &self,
        input_sequence: u64,
    ) -> Result<Vec<nautilus_common::recovery_trace::NativeHistoricalOutput>> {
        let mut outputs = Vec::new();
        for entry in &self.entries {
            let Some(origin) = &entry.headers.native_origin else {
                continue;
            };
            if origin.input_sequence != input_sequence {
                continue;
            }
            ensure!(
                origin.source == self.source
                    && origin.root_sequence == self.root_sequence
                    && origin.output_ordinal == outputs.len() as u64 + 1,
                "native verified output closure changed"
            );
            let mut headers = entry.headers.clone();
            headers.native_origin = None;
            outputs.push(nautilus_common::recovery_trace::NativeHistoricalOutput {
                source_entry_sequence: entry.seq,
                source_entry_hash: entry.entry_hash.to_hex(),
                topic: entry.topic.as_str().into(),
                payload_type: entry.payload_type.as_str().into(),
                payload: entry.payload.to_vec(),
                semantic_headers: serde_json::to_value(headers)?,
            });
        }
        Ok(outputs)
    }

    /// Returns the original Journal closure, including domain receipt wrappers.
    #[must_use]
    pub fn entries(&self) -> &[EventStoreEntry] {
        &self.entries
    }
}

/// Verified source roots and any explicit incomplete suffix. An incomplete suffix
/// cannot be relabeled a completed root or forgotten during replay.
#[derive(Debug)]
pub struct VerifiedNativeTrace {
    source: NativeTraceSource,
    cut: NativeTraceCheckpointCut,
    roots: Vec<VerifiedNativeRoot>,
    incomplete_suffix: Vec<EventStoreEntry>,
    end_sequence: u64,
    final_cut: Option<NativeTraceCheckpointCut>,
    initial_cut_sealed: bool,
}
impl VerifiedNativeTrace {
    #[must_use]
    pub const fn source(&self) -> &NativeTraceSource {
        &self.source
    }
    #[must_use]
    pub const fn cut(&self) -> &NativeTraceCheckpointCut {
        &self.cut
    }
    #[must_use]
    pub fn roots(&self) -> &[VerifiedNativeRoot] {
        &self.roots
    }
    #[must_use]
    pub fn incomplete_suffix(&self) -> &[EventStoreEntry] {
        &self.incomplete_suffix
    }
    #[must_use]
    pub const fn end_sequence(&self) -> u64 {
        self.end_sequence
    }
    /// Only an actual final native checkpoint seals the remaining physical FIFO.
    /// No last completed root alone proves an empty source queue.
    pub fn final_pending(&self) -> Result<&[nautilus_common::recovery_trace::NativePendingInput]> {
        Ok(&self.final_cut()?.pending_inputs)
    }
    #[must_use]
    pub const fn initial_cut_sealed(&self) -> bool {
        self.initial_cut_sealed
    }
    pub fn final_cut(&self) -> Result<&NativeTraceCheckpointCut> {
        self.final_cut
            .as_ref()
            .context("native tail final physical FIFO was not sealed")
    }
}

/// Owner-thread replay of the exact Begin/Complete tree issued by the native reader.
/// It controls historical callbacks and comparisons only, never native admission.
#[derive(Clone, Debug)]
pub struct NativeHistoricalRootReplay(Rc<RefCell<HistoricalRootState>>);
#[derive(Debug)]
struct HistoricalRootState {
    root: Rc<VerifiedNativeRoot>,
    next: usize,
    stack: Vec<u64>,
    failed: bool,
    timeline: Option<(dst::time::Instant, u64)>,
}
impl NativeHistoricalRootReplay {
    /// Creates a replay tree from the actual verified Journal closure.
    #[must_use]
    pub fn new(root: &VerifiedNativeRoot) -> Self {
        Self(Rc::new(RefCell::new(HistoricalRootState {
            root: Rc::new(root.clone()),
            next: 0,
            stack: Vec::new(),
            failed: false,
            timeline: None,
        })))
    }
    /// Maps original source receipts into the same target monotonic origin used
    /// to install manager ages. Real downtime is retained; no wall clock is reset.
    #[must_use]
    pub fn with_timeline(
        root: &VerifiedNativeRoot,
        mapped_cut: dst::time::Instant,
        source_cut_elapsed_ns: u64,
    ) -> Self {
        let replay = Self::new(root);
        replay.0.borrow_mut().timeline = Some((mapped_cut, source_cut_elapsed_ns));
        replay
    }

    /// Returns the next original source Begin. A pending Complete cannot be skipped.
    pub fn next_begin(&self) -> Result<NativeTraceRecord> {
        let state = self
            .0
            .try_borrow()
            .context("historical native tree reentered")?;
        ensure!(!state.failed, "historical native tree failed");
        let record = state
            .root
            .inputs
            .get(state.next)
            .context("historical native tree exhausted")?;
        ensure!(
            matches!(record, NativeTraceRecord::Begin { .. }),
            "historical native child/completion order changed"
        );
        Ok(record.clone())
    }
    /// Returns the expected effects for the currently executing original input.
    pub fn expected_effects(&self) -> Result<Value> {
        let state = self
            .0
            .try_borrow()
            .context("historical native tree reentered")?;
        let input = *state
            .stack
            .last()
            .context("historical native input absent")?;
        state
            .root
            .inputs
            .iter()
            .find_map(|record| match record {
                NativeTraceRecord::Complete {
                    input_sequence,
                    native_effects,
                    ..
                } if *input_sequence == input => Some(native_effects.clone()),
                _ => None,
            })
            .context("historical native source Complete missing")
    }
    /// Enters only the next exact original source input, including actual nested children.
    /// # Errors
    /// Refuses a changed source family/payload, skipped child or failed source closure.
    pub fn begin(
        &self,
        kind: NativeInputSource,
        payload: &Value,
    ) -> Result<NativeHistoricalInputGuard> {
        let mut state = self
            .0
            .try_borrow_mut()
            .context("historical native tree reentered")?;
        ensure!(!state.failed, "historical native tree failed");
        let begin = state
            .root
            .inputs
            .get(state.next)
            .context("historical native tree exhausted")?
            .clone();
        let NativeTraceRecord::Begin {
            input_sequence,
            input_source,
            payload: expected,
            stack_parent,
            ..
        } = &begin
        else {
            anyhow::bail!("historical native child/completion order changed")
        };
        ensure!(
            *input_source == kind
                && expected == payload
                && *stack_parent == state.stack.last().copied(),
            "historical native input family/payload/parent differs"
        );
        let input = *input_sequence;
        let complete = state
            .root
            .inputs
            .iter()
            .find(|record| {
                matches!(record,
            NativeTraceRecord::Complete { input_sequence, .. } if *input_sequence == input)
            })
            .context("historical native source Complete missing")?;
        let NativeTraceRecord::Complete {
            callbacks,
            transports,
            uuid_draws,
            clock_reads,
            queued_outputs,
            ..
        } = complete
        else {
            unreachable!()
        };
        let scope =
            nautilus_common::recovery_trace::historical::HistoricalInputScope::enter_with_outputs_at(
                state.root.clone(),
                begin.clone(),
                callbacks.clone(),
                transports.clone(),
                uuid_draws.clone(),
                clock_reads.clone(),
                state.root.historical_outputs(input)?,
                state.timeline.map(|(at, source_elapsed)| -> Result<dst::time::Instant> {
                    let NativeTraceRecord::Begin { receipt, .. } = &begin else { unreachable!() };
                    let delta = receipt.process_elapsed_ns.checked_sub(source_elapsed)
                        .context("historical input precedes source cut process boundary")?;
                    at.checked_add(std::time::Duration::from_nanos(delta)).context("historical receipt exceeds monotonic range")
                }).transpose()?,
                queued_outputs.clone(),
            )?;
        state.stack.push(input);
        state.next += 1;
        Ok(NativeHistoricalInputGuard {
            replay: self.clone(),
            input,
            scope: Some(scope),
            completed: false,
        })
    }
    /// Verifies the entire native root, with no unresolved original child or output.
    pub fn finish(&self) -> Result<NativeHistoricalRootReceipt> {
        let state = self
            .0
            .try_borrow()
            .context("historical native tree reentered")?;
        ensure!(
            !state.failed && state.stack.is_empty() && state.next == state.root.inputs.len(),
            "historical native root is incomplete"
        );
        Ok(NativeHistoricalRootReceipt {
            source: state.root.source.clone(),
            root: state.root.root_sequence,
            final_input: state
                .root
                .inputs
                .iter()
                .filter_map(|record| match record {
                    NativeTraceRecord::Begin { input_sequence, .. } => Some(*input_sequence),
                    _ => None,
                })
                .max()
                .context("historical native source root empty")?,
        })
    }
}
/// Original source progress, issued only after actual handler effect comparisons.
/// It is not Deserialize and has no current execution capability.
#[derive(Debug)]
pub struct NativeHistoricalRootReceipt {
    source: NativeTraceSource,
    root: u64,
    final_input: u64,
}
impl NativeHistoricalRootReceipt {
    #[must_use]
    pub const fn source(&self) -> &NativeTraceSource {
        &self.source
    }
    #[must_use]
    pub const fn root_sequence(&self) -> u64 {
        self.root
    }
    #[must_use]
    pub const fn final_input_sequence(&self) -> u64 {
        self.final_input
    }
}
#[must_use]
#[derive(Debug)]
pub struct NativeHistoricalInputGuard {
    replay: NativeHistoricalRootReplay,
    input: u64,
    scope: Option<nautilus_common::recovery_trace::historical::HistoricalInputScope>,
    completed: bool,
}
impl NativeHistoricalInputGuard {
    /// Compares actual original-object/native effects before accepting Complete.
    pub fn complete(mut self, actual_effects: Value) -> Result<()> {
        let mut state = self
            .replay
            .0
            .try_borrow_mut()
            .context("historical native tree reentered")?;
        ensure!(
            !state.failed && state.stack.last() == Some(&self.input),
            "historical native completion order changed"
        );
        let record = state
            .root
            .inputs
            .get(state.next)
            .context("historical native Complete missing")?;
        let NativeTraceRecord::Complete {
            input_sequence,
            native_effects,
            ..
        } = record
        else {
            anyhow::bail!("historical native original child was not processed");
        };
        ensure!(
            *input_sequence == self.input,
            "historical native Complete belongs to another input"
        );
        self.scope
            .take()
            .context("historical callback scope absent")?
            .finish()?;
        if native_effects != &actual_effects {
            // Report only bounded, fixed SDK field names. Actual account/business
            // values and arbitrary witness keys must not leak into error logs.
            let mut changed = [
                "schema",
                "timer_capture_ns",
                "orders",
                "positions",
                "accounts",
                "market_cache",
                "components",
                "data_engine",
                "execution_manager",
                "registered_timers",
                "report_contexts",
                "risk_state",
            ]
            .into_iter()
            .filter(|key| native_effects.get(*key) != actual_effects.get(*key))
            .map(str::to_owned)
            .collect::<Vec<_>>();
            if native_effects.get("execution_manager") != actual_effects.get("execution_manager") {
                for key in [
                    "schema",
                    "configuration",
                    "captured_at_ns",
                    "monotonic_offsets",
                    "order_activity",
                    "order_inflight_checks",
                    "order_query_recency",
                    "order_query_pending",
                    "order_recon_retries",
                    "order_coverage_unresolved",
                    "order_coverage_warnings",
                    "order_lookback_warnings",
                    "fills_processed",
                    "fills_recent",
                    "position_activity",
                    "position_activity_revisions",
                    "position_recon",
                    "position_recon_tolerances",
                ] {
                    if native_effects["execution_manager"].get(key)
                        != actual_effects["execution_manager"].get(key)
                    {
                        changed.push(format!("execution_manager.{key}"));
                    }
                }
            }
            anyhow::bail!(
                "historical native effects differ at input {}: {}",
                self.input,
                if changed.is_empty() {
                    "other source profile field".into()
                } else {
                    changed.join(",")
                }
            );
        }
        state.stack.pop();
        state.next += 1;
        self.completed = true;
        Ok(())
    }
}
impl Drop for NativeHistoricalInputGuard {
    fn drop(&mut self) {
        if !self.completed {
            if let Ok(mut state) = self.replay.0.try_borrow_mut() {
                state.failed = true;
            }
        }
    }
}

#[derive(Debug)]
struct InputCheck {
    input: u64,
    root: u64,
    begin_receipt: NativeProcessingReceipt,
    outputs: u64,
}

impl<B: EventStore> EventStoreReader<B> {
    fn verified_native_prefix(
        &self,
        source: &NativeTraceSource,
        end: u64,
    ) -> Result<DurableJournalPrefix> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"nautilus-native-journal-prefix/v1");
        hasher.update(&(source.journal_run.len() as u64).to_be_bytes());
        hasher.update(source.journal_run.as_bytes());
        hasher.update(&end.to_be_bytes());
        for sequence in 1..=end {
            let entry = self
                .scan_seq(sequence)?
                .context("native prefix row absent")?;
            ensure!(
                entry.seq == sequence && entry.recompute_hash() == entry.entry_hash,
                "native prefix hash mismatch"
            );
            hasher.update(entry.entry_hash.as_bytes());
        }
        Ok(DurableJournalPrefix {
            run_id: source.journal_run.clone(),
            sequence: end,
            entry_hash_digest: hasher.finalize().to_hex().to_string(),
        })
    }

    /// Verifies exact source bytes, original receipt times, nesting, causal joins and effects.
    ///
    /// # Errors
    /// Refuses stale/mismatched cuts, gaps, tampering, unjoined output rows, unsupported
    /// source records, duplicate FIFO evidence and false completion. A real interrupted
    /// final root remains explicitly incomplete in the returned source object.
    pub fn verify_native_tail(
        &self,
        expected: &NativeTraceSource,
        cut: &NativeTraceCheckpointCut,
        end: u64,
    ) -> Result<VerifiedNativeTrace> {
        expected.validate()?;
        ensure!(
            cut.schema_version == 1
                && cut.source == *expected
                && cut.prefix.run_id == expected.journal_run,
            "native trace source/cut mismatch"
        );
        let manifest = self.manifest()?;
        ensure!(
            manifest.run_id == expected.journal_run
                && manifest.instance_id == expected.node_instance.to_string(),
            "native trace manifest identity mismatch"
        );
        let watermark = self.high_watermark()?;
        ensure!(
            cut.prefix.sequence > 0 && cut.prefix.sequence <= end && end <= watermark,
            "native trace bounds unavailable"
        );
        let mut prefix_hash = blake3::Hasher::new();
        prefix_hash.update(b"nautilus-native-journal-prefix/v1");
        prefix_hash.update(&(expected.journal_run.len() as u64).to_be_bytes());
        prefix_hash.update(expected.journal_run.as_bytes());
        prefix_hash.update(&cut.prefix.sequence.to_be_bytes());
        let mut known_inputs = BTreeMap::new();
        let mut prefix_channel_ordinals = BTreeMap::<String, u64>::new();
        let mut derived_outputs =
            BTreeMap::<String, nautilus_common::recovery_trace::NativeQueuedOutput>::new();
        for sequence in 1..=cut.prefix.sequence {
            let entry = self
                .scan_seq(sequence)?
                .context("native prefix row absent")?;
            ensure!(
                entry.seq == sequence && entry.recompute_hash() == entry.entry_hash,
                "native prefix hash mismatch"
            );
            prefix_hash.update(entry.entry_hash.as_bytes());
            if entry.payload_type.as_str() == NATIVE_TRACE_PAYLOAD_TYPE {
                let record: NativeTraceRecord = rmp_serde::from_slice(&entry.payload)?;
                if let NativeTraceRecord::Complete {
                    source,
                    root_sequence,
                    input_sequence,
                    queued_outputs,
                    ..
                } = &record
                {
                    ensure!(source == expected, "native prefix queued source differs");
                    for output in queued_outputs {
                        ensure!(
                            output.receipt.caused_by
                                == Some(nautilus_common::recovery_trace::NativeInputCause {
                                    process_incarnation: expected.process_incarnation,
                                    journal_run: expected.journal_run.clone(),
                                    root_sequence: *root_sequence,
                                    input_sequence: *input_sequence,
                                })
                                && output.receipt.channel_ordinal == 0,
                            "native prefix derived ingress cause differs"
                        );
                        ensure!(
                            derived_outputs
                                .insert(output.receipt.message_id.to_string(), output.clone())
                                .is_none(),
                            "native prefix duplicate derived admission"
                        );
                    }
                }
                if let NativeTraceRecord::Begin {
                    source,
                    root_sequence,
                    input_sequence,
                    receipt,
                    ..
                } = record
                {
                    if let Some(ingress) = &receipt.ingress {
                        if let Some(previous) = prefix_channel_ordinals
                            .insert(ingress.channel_id.to_string(), ingress.channel_ordinal)
                        {
                            ensure!(
                                previous.checked_add(1) == Some(ingress.channel_ordinal),
                                "native prefix FIFO gap"
                            );
                        }
                    }
                    ensure!(
                        source == *expected,
                        "native prefix producer identity mismatch"
                    );
                    ensure!(
                        known_inputs.insert(input_sequence, root_sequence).is_none(),
                        "native prefix duplicate input"
                    );
                }
            }
        }
        ensure!(
            prefix_hash.finalize().to_hex().as_str() == cut.prefix.entry_hash_digest,
            "native trace prefix changed"
        );
        let cut_entry = self
            .scan_seq(cut.last_trace_sequence)?
            .context("native cut completion absent")?;
        ensure!(
            cut.last_trace_sequence <= cut.prefix.sequence
                && cut_entry.entry_hash.to_hex() == cut.last_trace_entry_hash
                && cut_entry.payload_type.as_str() == NATIVE_TRACE_PAYLOAD_TYPE,
            "native cut completion mismatch"
        );
        let completion: NativeTraceRecord = rmp_serde::from_slice(&cut_entry.payload)?;
        ensure!(
            matches!(completion, NativeTraceRecord::Complete { source, root_sequence, input_sequence, .. }
            if source == *expected && root_sequence == cut.completed_root && input_sequence == cut.completed_input),
            "native cut frontier mismatch"
        );
        ensure!(
            cut.last_input_sequence >= cut.completed_input
                && known_inputs.keys().next_back() == Some(&cut.last_input_sequence),
            "native cut latest input mismatch"
        );
        let mut next_input = cut
            .last_input_sequence
            .checked_add(1)
            .context("native tail input exhausted")?;
        let mut next_root = cut
            .completed_root
            .checked_add(1)
            .context("native tail root exhausted")?;
        let mut stack = Vec::<InputCheck>::new();
        let mut root_entries = Vec::new();
        let mut root_records = Vec::new();
        let mut roots = Vec::new();
        let mut seen_messages = BTreeSet::new();
        let mut channel_ordinals = prefix_channel_ordinals;
        let mut pending_cut = BTreeMap::new();
        for receipt in &cut.pending {
            ensure!(
                pending_cut
                    .insert(receipt.message_id.to_string(), receipt.clone())
                    .is_none(),
                "native cut duplicate pending receipt"
            );
        }
        let mut final_cut = None;
        let mut initial_cut_sealed = false;
        let mut last_processing_ns = cut.captured_process_elapsed_ns;
        for sequence in cut.prefix.sequence + 1..=end {
            let entry = self.scan_seq(sequence)?.context("native tail row absent")?;
            ensure!(
                entry.seq == sequence && entry.recompute_hash() == entry.entry_hash,
                "native tail hash mismatch"
            );
            if entry.payload_type.as_str() == "RunEnded" {
                ensure!(
                    stack.is_empty() && sequence == end && entry.headers.native_origin.is_none(),
                    "native tail premature RunEnded"
                );
                continue;
            }
            if entry.payload_type.as_str() == NATIVE_CHECKPOINT_PAYLOAD_TYPE {
                ensure!(
                    stack.is_empty() && entry.headers.native_origin.is_none(),
                    "checkpoint metadata inside active native input"
                );
                let metadata: NativeTraceCheckpointRecord = rmp_serde::from_slice(&entry.payload)?;
                if sequence == cut.prefix.sequence + 1 {
                    ensure!(
                        metadata.cut == *cut,
                        "initial native cut differs from its actual durable checkpoint"
                    );
                    initial_cut_sealed = true;
                }

                ensure!(
                    metadata.cut.source == *expected
                        && metadata.cut.completed_root.checked_add(1) == Some(next_root)
                        && metadata.cut.last_input_sequence.checked_add(1) == Some(next_input)
                        && metadata.cut.prefix.sequence.checked_add(1) == Some(sequence),
                    "native checkpoint metadata frontier mismatch"
                );
                let actual = self.verified_native_prefix(expected, metadata.cut.prefix.sequence)?;
                ensure!(
                    actual == metadata.cut.prefix,
                    "native checkpoint metadata prefix changed"
                );
                ensure!(
                    metadata.cut.pending_inputs.len() == metadata.cut.pending.len(),
                    "native checkpoint pending payload coverage absent"
                );
                let mut final_ids = BTreeSet::new();
                for (input, receipt) in metadata
                    .cut
                    .pending_inputs
                    .iter()
                    .zip(&metadata.cut.pending)
                {
                    ensure!(
                        input.receipt == *receipt
                            && final_ids.insert(receipt.message_id.to_string())
                            && !seen_messages.contains(&receipt.message_id.to_string()),
                        "native checkpoint pending was changed or already processed"
                    );
                    if let Some(original) = derived_outputs.get(&receipt.message_id.to_string()) {
                        let mut origin = original.receipt.clone();
                        origin.channel_ordinal = receipt.channel_ordinal;
                        ensure!(
                            original.accepted
                                && origin == *receipt
                                && original.payload == input.payload,
                            "native final derived FIFO differs from actual admission"
                        );
                    }
                }
                final_cut = Some(metadata.cut);
                continue;
            }
            if entry.payload_type.as_str() == NATIVE_TRACE_PAYLOAD_TYPE {
                ensure!(
                    entry.headers.native_origin.is_none(),
                    "native trace row cannot be a derived output"
                );
                let record: NativeTraceRecord = rmp_serde::from_slice(&entry.payload)?;
                match &record {
                    NativeTraceRecord::Begin {
                        source,
                        root_sequence,
                        input_sequence,
                        stack_parent,
                        input_source,
                        receipt,
                        payload,
                        ..
                    } => {
                        final_cut = None;
                        ensure!(
                            source == expected && *input_sequence == next_input,
                            "native tail input identity/sequence mismatch"
                        );
                        match stack.last() {
                            Some(parent) => ensure!(
                                *root_sequence == parent.root
                                    && *stack_parent == Some(parent.input),
                                "native tail stack parent mismatch"
                            ),
                            None => ensure!(
                                *root_sequence == next_root && stack_parent.is_none(),
                                "native tail root mismatch"
                            ),
                        }
                        ensure!(
                            receipt.process_elapsed_ns >= last_processing_ns,
                            "native processing clock moved backwards"
                        );
                        last_processing_ns = receipt.process_elapsed_ns;
                        if let Some(ingress) = &receipt.ingress {
                            if let Some(original) =
                                pending_cut.remove(&ingress.message_id.to_string())
                            {
                                ensure!(
                                    original == *ingress,
                                    "native cut pending receipt was substituted"
                                );
                            }
                            if let Some(cause) = &ingress.caused_by {
                                let admitted = derived_outputs
                                    .get(&ingress.message_id.to_string())
                                    .context(
                                        "derived native input has no original admission output",
                                    )?;
                                let mut original = admitted.receipt.clone();
                                original.channel_ordinal = ingress.channel_ordinal;
                                ensure!(
                                    admitted.accepted
                                        && original == *ingress
                                        && admitted.payload == *payload
                                        && known_inputs.get(&cause.input_sequence)
                                            == Some(&cause.root_sequence),
                                    "derived native input was changed or originally rejected"
                                );
                            }
                            ensure!(
                                ingress.input_source == *input_source
                                    && ingress.channel_ordinal > 0
                                    && ingress.accepted_wall_ns <= receipt.wall_ns
                                    && seen_messages.insert(ingress.message_id.to_string()),
                                "native ingress identity/time mismatch"
                            );
                            if let Some(previous) = channel_ordinals
                                .insert(ingress.channel_id.to_string(), ingress.channel_ordinal)
                            {
                                ensure!(
                                    previous.checked_add(1) == Some(ingress.channel_ordinal),
                                    "native channel FIFO gap"
                                );
                            }
                            if let Some(cause) = &ingress.caused_by {
                                ensure!(
                                    cause.process_incarnation == expected.process_incarnation
                                        && cause.journal_run == expected.journal_run
                                        && known_inputs.get(&cause.input_sequence)
                                            == Some(&cause.root_sequence),
                                    "native queued cause absent or foreign"
                                );
                            }
                        }
                        ensure!(
                            known_inputs
                                .insert(*input_sequence, *root_sequence)
                                .is_none(),
                            "native input replay duplicate"
                        );
                        stack.push(InputCheck {
                            input: *input_sequence,
                            root: *root_sequence,
                            begin_receipt: receipt.clone(),
                            outputs: 0,
                        });
                        next_input = next_input
                            .checked_add(1)
                            .context("native input exhausted")?;
                    }
                    NativeTraceRecord::Complete {
                        source,
                        root_sequence,
                        input_sequence,
                        receipt,
                        output_count,
                        queued_outputs,
                        ..
                    } => {
                        for output in queued_outputs {
                            ensure!(
                                output.receipt.caused_by
                                    == Some(nautilus_common::recovery_trace::NativeInputCause {
                                        process_incarnation: expected.process_incarnation,
                                        journal_run: expected.journal_run.clone(),
                                        root_sequence: *root_sequence,
                                        input_sequence: *input_sequence,
                                    })
                                    && output.receipt.channel_ordinal == 0
                                    && output.receipt.accepted_wall_ns <= receipt.wall_ns,
                                "native derived admission has changed cause or time"
                            );
                            ensure!(
                                derived_outputs
                                    .insert(output.receipt.message_id.to_string(), output.clone())
                                    .is_none(),
                                "duplicate native derived admission identity"
                            );
                        }
                        let input = stack.pop().context("native Complete without Begin")?;
                        ensure!(
                            source == expected
                                && input.input == *input_sequence
                                && input.root == *root_sequence
                                && input.outputs == *output_count
                                && receipt.ingress.is_none()
                                && receipt.wall_ns >= input.begin_receipt.wall_ns
                                && receipt.process_elapsed_ns >= last_processing_ns,
                            "native Complete identity/effect/time mismatch"
                        );
                        last_processing_ns = receipt.process_elapsed_ns;
                        if stack.is_empty() {
                            next_root =
                                next_root.checked_add(1).context("native root exhausted")?;
                        }
                    }
                    NativeTraceRecord::Abort { .. } | NativeTraceRecord::Uncovered { .. } => {
                        anyhow::bail!(
                            "native source aborted or uncovered; reconciliation required"
                        );
                    }
                }
                root_records.push(record);
            } else {
                let origin = entry
                    .headers
                    .native_origin
                    .as_ref()
                    .context("native tail mutation row has no causal join")?;
                let parent = stack.iter().rev().nth(1).map(|frame| frame.input);
                let input = stack
                    .last_mut()
                    .context("native output outside source scope")?;
                ensure!(
                    origin.source == *expected
                        && origin.input_sequence == input.input
                        && origin.root_sequence == input.root
                        && origin.stack_parent == parent
                        && input.outputs.checked_add(1) == Some(origin.output_ordinal),
                    "native output causal/ordinal mismatch"
                );
                input.outputs = origin.output_ordinal;
            }
            root_entries.push(entry);
            if stack.is_empty() {
                ensure!(
                    !root_records.is_empty(),
                    "native root missing source records"
                );
                roots.push(VerifiedNativeRoot {
                    source: expected.clone(),
                    root_sequence: next_root - 1,
                    inputs: std::mem::take(&mut root_records),
                    entries: std::mem::take(&mut root_entries),
                });
            }
        }
        ensure!(
            self.high_watermark()? == watermark,
            "native source changed during tail verification"
        );
        Ok(VerifiedNativeTrace {
            source: expected.clone(),
            cut: cut.clone(),
            roots,
            incomplete_suffix: root_entries,
            end_sequence: end,
            final_cut,
            initial_cut_sealed,
        })
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::*;
    use crate::{
        backend::{AppendEntry, IndexKind, MemoryBackend, ScanDirection},
        capture::{BusCaptureAdapter, EncodedPayload, EncoderRegistry},
        error::EventStoreError,
        manifest::{RegisteredComponents, RunManifest, RunStatus},
        snapshot::SnapshotAnchor,
        writer::WriterConfig,
    };
    use indexmap::IndexMap;
    use nautilus_common::recovery_trace::{NativeInputSource, historical::HistoricalInputScope};
    use nautilus_core::{UUID4, time::get_atomic_clock_realtime};
    use parking_lot::Mutex;
    use rstest::rstest;
    use std::time::Duration;
    #[derive(Debug, Clone)]
    struct SharedTraceMemory(Arc<Mutex<MemoryBackend>>);

    impl EventStore for SharedTraceMemory {
        fn open_run(&mut self, manifest: RunManifest) -> Result<(), EventStoreError> {
            self.0.lock().open_run(manifest)
        }

        fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
            self.0.lock().append_batch(entries)
        }

        fn scan_range(
            &self,
            from: u64,
            to: u64,
            direction: ScanDirection,
        ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
            self.0.lock().scan_range(from, to, direction)
        }

        fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
            self.0.lock().scan_seq(seq)
        }

        fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
            self.0.lock().lookup(kind, key)
        }

        fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
            self.0.lock().iter_index_keys(kind)
        }

        fn record_snapshot_anchor(
            &mut self,
            anchor: SnapshotAnchor,
        ) -> Result<(), EventStoreError> {
            self.0.lock().record_snapshot_anchor(anchor)
        }

        fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
            self.0.lock().latest_snapshot_anchor()
        }

        fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
            self.0.lock().seal(status)
        }

        fn manifest(&self) -> Result<RunManifest, EventStoreError> {
            self.0.lock().manifest()
        }

        fn high_watermark(&self) -> Result<u64, EventStoreError> {
            self.0.lock().high_watermark()
        }
    }

    fn actual_journal() -> (
        NativeTraceRecorder,
        Arc<EventStoreWriter>,
        SharedTraceMemory,
        NativeTraceSource,
        HaltSignal,
    ) {
        let source = NativeTraceSource {
            schema_version: 1,
            node_instance: UUID4::new(),
            process_incarnation: UUID4::new(),
            journal_run: "actual-native-trace".into(),
            logical_run: "logical".into(),
            configuration_digest: "config".into(),
            codec_profile: "native-test.v1".into(),
            registered_profile_digest: "registered".into(),
        };
        let mut backend = SharedTraceMemory(Arc::new(Mutex::new(MemoryBackend::new())));
        backend
            .open_run(RunManifest {
                run_id: source.journal_run.clone(),
                parent_run_id: None,
                instance_id: source.node_instance.to_string(),
                binary_hash: "actual-test-writer".into(),
                schema_version: 1,
                crate_versions: "test".into(),
                feature_flags: vec!["live".into()],
                adapter_versions: IndexMap::new(),
                config_hash: "config".into(),
                registered_components: RegisteredComponents::default(),
                seed: None,
                start_ts_init: UnixNanos::from(1),
                end_ts_init: None,
                high_watermark: 0,
                status: RunStatus::Running,
            })
            .unwrap();
        let halt = HaltSignal::new();
        let writer = Arc::new(
            EventStoreWriter::spawn(
                Box::new(backend.clone()),
                get_atomic_clock_realtime(),
                halt.callback(),
                WriterConfig {
                    max_batch_entries: 1,
                    max_batch_latency: Duration::from_millis(2),
                    ..WriterConfig::default()
                },
            )
            .unwrap(),
        );
        let trace = NativeTraceRecorder::new(source.clone(), &writer, halt.clone()).unwrap();
        (trace, writer, backend, source, halt)
    }
    fn cut(trace: &NativeTraceRecorder) -> NativeTraceCheckpointCut {
        trace
            .begin_native(
                1,
                1,
                None,
                NativeInputSource::Lifecycle,
                "source",
                serde_json::json!({"action":"start"}),
                vec![],
            )
            .unwrap()
            .complete(serde_json::json!({"state":"started"}))
            .unwrap();
        trace
            .checkpoint_cut(1, 1, "actual-inventory".into(), vec![])
            .unwrap()
    }

    #[rstest]
    fn native_trace_actual_writer_preserves_nested_causal_economic_closure_and_checkpoint_metadata()
    {
        let (trace, writer, backend, source, _) = actual_journal();
        let cut = cut(&trace);
        let checkpoint = trace
            .persist_checkpoint(
                &cut,
                serde_json::json!({"artifact_sha256":"bound-host-artifact"}),
            )
            .unwrap();
        assert_eq!(checkpoint.sequence(), cut.prefix.sequence + 1);
        assert!(
            trace
                .persist_checkpoint(&cut, serde_json::Value::Null)
                .is_err()
        );
        let root = trace
            .begin_native(
                2,
                2,
                None,
                NativeInputSource::Maintenance,
                "running",
                serde_json::json!({"due":true}),
                vec![],
            )
            .unwrap();
        let child = trace
            .begin_native(
                2,
                3,
                Some(2),
                NativeInputSource::QueryResult,
                "running",
                serde_json::json!({"original_result":17}),
                vec![],
            )
            .unwrap();
        let mut registry = EncoderRegistry::new();
        registry.register::<u64, _>(Ustr::from("NativeTestEconomicEvent.v1"), |value| {
            Ok(EncodedPayload::without_indices(Bytes::from(
                rmp_serde::to_vec_named(value).unwrap(),
            )))
        });
        let adapter = BusCaptureAdapter::new(
            writer.clone(),
            Arc::new(registry),
            HaltSignal::new().callback(),
        );
        assert!(
            adapter
                .capture(
                    "native.economic".into(),
                    &17u64,
                    Headers::empty(),
                    UnixNanos::from(3)
                )
                .unwrap()
        );
        child.complete(serde_json::json!({"economics":17})).unwrap();
        root.complete(serde_json::json!({"maintained":true}))
            .unwrap();
        writer.flush().unwrap();
        let reader = EventStoreReader::new(backend);
        let verified = reader
            .verify_native_tail(&source, &cut, writer.high_watermark())
            .unwrap();
        assert!(verified.incomplete_suffix().is_empty());
        assert_eq!(verified.roots().len(), 1);
        let root = &verified.roots()[0];
        assert_eq!(root.root_sequence(), 2);
        assert_eq!(root.inputs().len(), 4);
        let economics = root
            .entries()
            .iter()
            .find(|entry| entry.payload_type.as_str() == "NativeTestEconomicEvent.v1")
            .unwrap();
        let origin = economics.headers.native_origin.as_ref().unwrap();
        assert_eq!(
            (
                origin.root_sequence,
                origin.input_sequence,
                origin.stack_parent,
                origin.output_ordinal
            ),
            (2, 3, Some(2), 1)
        );
    }

    #[rstest]
    fn native_trace_interrupted_real_output_retains_incomplete_root_and_halts_source() {
        let (trace, writer, backend, source, halt) = actual_journal();
        let cut = cut(&trace);
        let root = trace
            .begin_native(
                2,
                2,
                None,
                NativeInputSource::Reconciliation,
                "running",
                serde_json::json!({"economic_input":23}),
                vec![],
            )
            .unwrap();
        writer
            .append_durable(EntryDraft::without_indices(
                Headers {
                    native_origin: scope::next_output_origin().unwrap(),
                    ..Headers::empty()
                },
                "native.economic".into(),
                Ustr::from("NativeTestEconomicEvent.v1"),
                Bytes::from_static(b"23"),
                UnixNanos::from(3),
            ))
            .unwrap();
        drop(root);
        assert!(halt.is_halted());
        let verified = EventStoreReader::new(backend)
            .verify_native_tail(&source, &cut, writer.high_watermark())
            .unwrap();
        assert!(verified.roots().is_empty());
        assert_eq!(verified.incomplete_suffix().len(), 2);
        assert!(trace.checkpoint_cut(1, 1, "same".into(), vec![]).is_err());
    }

    #[rstest]
    fn native_trace_unjoined_output_is_rejected_even_with_valid_row_hash() {
        let (trace, writer, backend, source, _) = actual_journal();
        let cut = cut(&trace);
        writer
            .append_durable(EntryDraft::without_indices(
                Headers::empty(),
                "native.economic".into(),
                Ustr::from("NativeTestEconomicEvent.v1"),
                Bytes::from_static(b"31"),
                UnixNanos::from(3),
            ))
            .unwrap();
        assert!(
            EventStoreReader::new(backend)
                .verify_native_tail(&source, &cut, writer.high_watermark())
                .is_err()
        );
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn native_trace_reader_proof_drives_original_uuid_and_transport_without_resend(
        #[case] changed_request: bool,
    ) {
        let (trace, writer, backend, source, _) = actual_journal();
        let cut = cut(&trace);
        let guard = trace
            .begin_native(
                2,
                2,
                None,
                NativeInputSource::Maintenance,
                "running",
                serde_json::json!({"prepare":"query"}),
                vec![],
            )
            .unwrap();
        let original_uuid = nautilus_common::recovery_trace::native_event_uuid();
        let actual_wire_calls = std::cell::Cell::new(0);
        nautilus_common::recovery_trace::native_transport(
            "actual-client",
            "query",
            &serde_json::json!({"order":11}),
            || {
                actual_wire_calls.set(actual_wire_calls.get() + 1);
                Ok(())
            },
        )
        .unwrap();
        guard
            .complete(serde_json::json!({"query_pending":true}))
            .unwrap();
        let verified = EventStoreReader::new(backend)
            .verify_native_tail(&source, &cut, writer.high_watermark())
            .unwrap();
        let root = Rc::new(verified.roots()[0].clone());
        let begin = root.inputs()[0].clone();
        let (callbacks, transports, uuid_draws) = match &root.inputs()[1] {
            NativeTraceRecord::Complete {
                callbacks,
                transports,
                uuid_draws,
                ..
            } => (callbacks.clone(), transports.clone(), uuid_draws.clone()),
            _ => panic!("source complete absent"),
        };
        let historical =
            HistoricalInputScope::enter(root, begin, callbacks, transports, uuid_draws).unwrap();
        assert_eq!(
            nautilus_common::recovery_trace::native_event_uuid(),
            original_uuid
        );
        let request = serde_json::json!({"order": if changed_request {12} else {11}});
        let result = nautilus_common::recovery_trace::native_transport(
            "actual-client",
            "query",
            &request,
            || {
                actual_wire_calls.set(actual_wire_calls.get() + 1);
                Ok(())
            },
        );
        assert_eq!(result.is_err(), changed_request);
        assert_eq!(historical.finish().is_err(), changed_request);
        assert_eq!(
            actual_wire_calls.get(),
            1,
            "archived command must never be sent again"
        );
    }
    #[rstest]
    #[case(false)]
    #[case(true)]
    fn verified_native_output_replay_does_not_append_second_economic_row(
        #[case] change_payload: bool,
    ) {
        let (trace, writer, backend, source, _) = actual_journal();
        let cut = cut(&trace);
        let guard = trace
            .begin_native(
                2,
                2,
                None,
                NativeInputSource::Maintenance,
                "running",
                serde_json::json!({"due": true}),
                vec![],
            )
            .unwrap();
        let mut registry = EncoderRegistry::new();
        registry.register::<u64, _>("NativeEconomicTest.v1".into(), |value| {
            Ok(EncodedPayload {
                payload: bytes::Bytes::from(rmp_serde::to_vec(value).unwrap()),
                index_keys: vec![],
                payload_type: None,
            })
        });
        let adapter = BusCaptureAdapter::new(
            writer.clone(),
            Arc::new(registry),
            HaltSignal::new().callback(),
        );
        adapter
            .capture(
                "native.economic".into(),
                &17u64,
                Headers::empty(),
                UnixNanos::from(1),
            )
            .unwrap();
        guard
            .complete(serde_json::json!({"economic_total":17}))
            .unwrap();
        let reader = EventStoreReader::new(backend);
        let verified = reader
            .verify_native_tail(&source, &cut, writer.high_watermark())
            .unwrap();
        let root = Rc::new(verified.roots()[0].clone());
        let begin = root.inputs()[0].clone();
        let complete = root.inputs().last().unwrap();
        let NativeTraceRecord::Complete {
            callbacks,
            transports,
            uuid_draws,
            clock_reads,
            ..
        } = complete
        else {
            panic!("missing Complete")
        };
        let scope = HistoricalInputScope::enter_with_outputs(
            root.clone(),
            begin,
            callbacks.clone(),
            transports.clone(),
            uuid_draws.clone(),
            clock_reads.clone(),
            root.historical_outputs(2).unwrap(),
        )
        .unwrap();
        let before = writer.high_watermark();
        let outcome = adapter.capture(
            "native.economic".into(),
            &(if change_payload { 18u64 } else { 17u64 }),
            Headers::empty(),
            UnixNanos::from(999999),
        );
        assert_eq!(outcome.is_err(), change_payload);
        assert_eq!(scope.finish().is_err(), change_payload);
        writer.flush().unwrap();
        assert_eq!(
            writer.high_watermark(),
            before,
            "historical capture cannot duplicate economics"
        );
        drop(writer);
    }

    #[rstest]
    fn verified_native_clock_reads_preserve_source_time_and_restore_actual_reads() {
        use nautilus_common::{clock::Clock, live::clock::LiveClock};
        let (trace, writer, backend, source, _) = actual_journal();
        let cut = cut(&trace);
        let clock = LiveClock::default();
        let guard = trace
            .begin_native(
                2,
                2,
                None,
                NativeInputSource::Maintenance,
                "running",
                serde_json::json!({"due":true}),
                vec![],
            )
            .unwrap();
        let original = clock.timestamp_ns();
        guard
            .complete(serde_json::json!({"source_time":original}))
            .unwrap();
        let reader = EventStoreReader::new(backend);
        let verified = reader
            .verify_native_tail(&source, &cut, writer.high_watermark())
            .unwrap();
        let root = Rc::new(verified.roots()[0].clone());
        let NativeTraceRecord::Complete {
            callbacks,
            transports,
            uuid_draws,
            clock_reads,
            ..
        } = root.inputs().last().unwrap()
        else {
            panic!("missing Complete")
        };
        let scope = HistoricalInputScope::enter_with_clocks(
            root.clone(),
            root.inputs()[0].clone(),
            callbacks.clone(),
            transports.clone(),
            uuid_draws.clone(),
            clock_reads.clone(),
        )
        .unwrap();
        assert_eq!(clock.timestamp_ns(), original);
        scope.finish().unwrap();
        assert!(
            clock.timestamp_ns() > original,
            "historical read view cannot roll back actual time"
        );
        drop(writer);
    }
}
