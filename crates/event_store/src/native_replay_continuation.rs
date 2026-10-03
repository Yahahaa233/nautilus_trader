//! Child Journal audit of verified historical replay; never a second economic input.
use super::*;
use nautilus_common::recovery_trace::NativePendingInput;

pub const NATIVE_REPLAY_CONTINUATION_PAYLOAD_TYPE: &str = "NativeRecoveryReplayContinuation.v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRootAudit {
    original_root: u64,
    original_first_input: u64,
    original_last_input: u64,
    original_entry_digest: String,
    input_count: u64,
    child_root: u64,
    child_first_input: u64,
    child_last_input: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayContinuationRecord {
    schema_version: u32,
    child: NativeTraceSource,
    original: NativeTraceSource,
    original_cut_prefix: DurableJournalPrefix,
    original_cut_root: u64,
    original_cut_input: u64,
    original_end_prefix: DurableJournalPrefix,
    original_final_root: u64,
    original_final_input: u64,
    child_prefix: DurableJournalPrefix,
    child_first_root: u64,
    child_first_input: u64,
    roots: Vec<ReplayRootAudit>,
    retained: Vec<NativePendingInput>,
    execution_authorized: bool,
}

fn verified_root_identity(root: &VerifiedNativeRoot) -> Result<(u64, u64, u64, String)> {
    let mut first = None;
    let mut last = 0;
    let mut count = 0u64;
    for record in &root.inputs {
        if let NativeTraceRecord::Begin { input_sequence, .. } = record {
            first.get_or_insert(*input_sequence);
            last = last.max(*input_sequence);
            count = count
                .checked_add(1)
                .context("historical input count exhausted")?;
        }
    }
    let first = first.context("verified historical root empty")?;
    ensure!(
        last.checked_sub(first).and_then(|n| n.checked_add(1)) == Some(count),
        "verified historical root inputs are not contiguous"
    );
    let mut hash = blake3::Hasher::new();
    hash.update(b"nautilus-native-original-replay-root/v1");
    for entry in &root.entries {
        hash.update(&entry.seq.to_be_bytes());
        hash.update(entry.entry_hash.as_bytes());
    }
    Ok((first, last, count, hash.finalize().to_hex().to_string()))
}

/// Prepared only from reader-issued roots and completed original callback/effect receipts.
/// Its target frontier is derived from this recorder's actual cursor, never caller counters.
#[must_use]
#[derive(Debug)]
pub struct PreparedNativeReplayContinuation {
    recorder: NativeTraceRecorder,
    record: ReplayContinuationRecord,
    next_root: u64,
    next_input: u64,
    completed_dispatch: Option<(u64, u64)>,
    committed: bool,
}
impl PreparedNativeReplayContinuation {
    /// Expected actual child observer completion; not a source completion or release.
    #[must_use]
    pub const fn completed_dispatch(&self) -> Option<(u64, u64)> {
        self.completed_dispatch
    }
    /// Persists this same owned writer's audit before allowing fresh native capture.
    /// # Errors
    /// Refuses a changed cursor/prefix, repeated continuation, or a lost durable ACK.
    pub fn commit(mut self) -> Result<NativeReplayContinuationReceipt> {
        let mut state = self
            .recorder
            .0
            .try_borrow_mut()
            .context("native replay continuation reentered")?;
        state.check()?;
        ensure!(
            state.stack.is_empty()
                && !state.replay_continued
                && state.next_root == self.record.child_first_root
                && state.next_input == self.record.child_first_input,
            "native replay continuation cursor changed"
        );
        let writer = state.writer()?;
        ensure!(
            writer.durable_prefix()? == self.record.child_prefix,
            "native replay continuation child prefix changed"
        );
        state.failure = Some("native replay continuation append interrupted".into());
        let result = writer.append_durable(EntryDraft::without_indices(
            Headers::empty(),
            "run.recovery.native_replay_continuation".into(),
            Ustr::from(NATIVE_REPLAY_CONTINUATION_PAYLOAD_TYPE),
            Bytes::from(rmp_serde::to_vec_named(&self.record)?),
            UnixNanos::from(nanos_since_unix_epoch()),
        ));
        let acknowledgment = match result {
            Ok(ack) => ack,
            Err(error) => {
                state.halt.callback()(HaltReason::ExternalPersistence(error.to_string()));
                return Err(error.into());
            }
        };
        state.next_root = self.next_root;
        state.next_input = self.next_input;
        state.replay_continued = true;
        state.failure = None;
        // No source Complete or native effects are fabricated. Only the next real
        // fresh input can supply a source cut at the advanced observer frontier.
        self.committed = true;
        Ok(NativeReplayContinuationReceipt {
            child: self.record.child.clone(),
            original: self.record.original.clone(),
            acknowledgment,
            completed_dispatch: self.completed_dispatch,
            last_dispatch_input: self.next_input - 1,
            recorder: self.recorder.clone(),
            original_cut_prefix: self.record.original_cut_prefix.clone(),
            original_end_prefix: self.record.original_end_prefix.clone(),
            original_cut_input: self.record.original_cut_input,
            original_final_root: self.record.original_final_root,
            original_final_input: self.record.original_final_input,
        })
    }
}
impl Drop for PreparedNativeReplayContinuation {
    fn drop(&mut self) {
        if !self.committed {
            self.recorder
                .fail("verified native replay continuation was not durably completed");
        }
    }
}

/// Actual non-economic child audit acknowledgment. No Deserialize or public constructor.
#[derive(Debug, Clone)]
pub struct NativeReplayContinuationReceipt {
    child: NativeTraceSource,
    original: NativeTraceSource,
    acknowledgment: DurableEntryAcknowledgment,
    completed_dispatch: Option<(u64, u64)>,
    last_dispatch_input: u64,
    recorder: NativeTraceRecorder,
    original_cut_prefix: DurableJournalPrefix,
    original_end_prefix: DurableJournalPrefix,
    original_cut_input: u64,
    original_final_root: u64,
    original_final_input: u64,
}
impl NativeReplayContinuationReceipt {
    /// Rechecks the exact reader-issued original cut and sealed final native frontier.
    /// # Errors
    /// Refuses another cut/tail, including one from the same logical or physical run.
    pub fn verify_original_trace(&self, trace: &VerifiedNativeTrace) -> Result<()> {
        let final_cut = trace.final_cut()?;
        ensure!(
            trace.initial_cut_sealed()
                && trace.incomplete_suffix().is_empty()
                && *trace.source() == self.original
                && trace.cut().prefix == self.original_cut_prefix
                && trace.cut().last_input_sequence == self.original_cut_input
                && trace.end_prefix == self.original_end_prefix
                && final_cut.completed_root == self.original_final_root
                && final_cut.last_input_sequence == self.original_final_input,
            "native replay continuation original reader cut or sealed tail changed"
        );
        Ok(())
    }

    /// Revalidates this actual audit against the same owning recorder and healthy writer.
    /// This receipt cannot authorize a replacement recorder, even with equal source DTOs.
    /// # Errors
    /// Refuses a foreign recorder, failed owner, replaced run or lost durable audit prefix.
    pub fn verify_recorder(&self, recorder: &NativeTraceRecorder) -> Result<()> {
        ensure!(
            Rc::ptr_eq(&self.recorder.0, &recorder.0),
            "native replay continuation belongs to a different owned recorder"
        );
        let state = recorder
            .0
            .try_borrow()
            .context("native continuation verification reentered")?;
        state.check()?;
        ensure!(
            state.source == self.child
                && state.parent_run.as_deref() == Some(self.original.journal_run.as_str())
                && state.replay_continued
                && state.stack.is_empty()
                && state.next_input > self.last_dispatch_input,
            "native replay continuation child cursor or original binding changed"
        );
        let prefix = state.writer()?.durable_prefix()?;
        ensure!(
            prefix.run_id == self.child.journal_run
                && prefix.sequence >= self.acknowledgment.sequence(),
            "native replay continuation durable audit is no longer owned"
        );
        Ok(())
    }

    #[must_use]
    pub const fn child(&self) -> &NativeTraceSource {
        &self.child
    }
    #[must_use]
    pub const fn original(&self) -> &NativeTraceSource {
        &self.original
    }
    #[must_use]
    pub const fn acknowledgment(&self) -> &DurableEntryAcknowledgment {
        &self.acknowledgment
    }
    #[must_use]
    pub const fn completed_dispatch(&self) -> Option<(u64, u64)> {
        self.completed_dispatch
    }
    #[must_use]
    pub const fn last_dispatch_input(&self) -> u64 {
        self.last_dispatch_input
    }
}

impl NativeTraceRecorder {
    /// Joins actual historical handler receipts to this same child writer's fresh cursor.
    /// # Errors
    /// Refuses unsealed/incomplete/foreign source roots, substituted receipts or repeated joins.
    pub fn prepare_historical_continuation(
        &self,
        trace: &VerifiedNativeTrace,
        receipts: &[NativeHistoricalRootReceipt],
    ) -> Result<PreparedNativeReplayContinuation> {
        let state = self
            .0
            .try_borrow()
            .context("native replay continuation reentered")?;
        state.check()?;
        ensure!(
            state.stack.is_empty()
                && !state.replay_continued
                && state.parent_run.as_deref() == Some(trace.source.journal_run.as_str())
                && state.source.journal_run != trace.source.journal_run
                && state.source.logical_run == trace.source.logical_run
                && state.source.configuration_digest == trace.source.configuration_digest
                && state.source.codec_profile == trace.source.codec_profile
                && state.source.registered_profile_digest == trace.source.registered_profile_digest,
            "native replay continuation has no same owned child/original binding"
        );
        ensure!(
            trace.initial_cut_sealed
                && trace.incomplete_suffix.is_empty()
                && trace.roots.len() == receipts.len(),
            "native replay continuation source incomplete"
        );
        let final_cut = trace.final_cut()?;
        let mut next_root = state.next_root;
        let mut next_input = state.next_input;
        let mut completed_dispatch = state.completed;
        let mut roots = Vec::new();
        roots.try_reserve(receipts.len())?;
        for (root, receipt) in trace.roots.iter().zip(receipts) {
            let (first, last, count, digest) = verified_root_identity(root)?;
            ensure!(
                receipt.source == trace.source
                    && receipt.root == root.root_sequence
                    && receipt.final_input == last
                    && verified_root_identity(&receipt.verified_root)?
                        == (first, last, count, digest.clone()),
                "native replay continuation historical receipt substituted"
            );
            let last_child_input = next_input
                .checked_add(count - 1)
                .context("child replay input exhausted")?;
            roots.push(ReplayRootAudit {
                original_root: root.root_sequence,
                original_first_input: first,
                original_last_input: last,
                original_entry_digest: digest,
                input_count: count,
                child_root: next_root,
                child_first_input: next_input,
                child_last_input: last_child_input,
            });
            completed_dispatch = Some((next_root, next_input));
            next_root = next_root
                .checked_add(1)
                .context("child replay root exhausted")?;
            next_input = last_child_input
                .checked_add(1)
                .context("child replay input exhausted")?;
        }
        let child_prefix = state.writer()?.durable_prefix()?;
        ensure!(
            child_prefix.run_id == state.source.journal_run,
            "native child writer identity changed"
        );
        Ok(PreparedNativeReplayContinuation {
            recorder: self.clone(),
            next_root,
            next_input,
            completed_dispatch,
            committed: false,
            record: ReplayContinuationRecord {
                schema_version: 1,
                child: state.source.clone(),
                original: trace.source.clone(),
                original_cut_prefix: trace.cut.prefix.clone(),
                original_cut_root: trace.cut.completed_root,
                original_cut_input: trace.cut.last_input_sequence,
                original_end_prefix: trace.end_prefix.clone(),
                original_final_root: final_cut.completed_root,
                original_final_input: final_cut.last_input_sequence,
                child_prefix,
                child_first_root: state.next_root,
                child_first_input: state.next_input,
                roots,
                retained: trace.final_pending()?.to_vec(),
                execution_authorized: false,
            },
        })
    }
}

#[derive(Debug, Default)]
pub(super) struct PrefixContinuations {
    continued: bool,
    next_root: u64,
    next_input: u64,
    stack: Vec<(u64, u64)>,
    retained: BTreeMap<String, NativePendingInput>,
}
impl PrefixContinuations {
    pub(super) fn new() -> Self {
        Self {
            next_root: 1,
            next_input: 1,
            ..Default::default()
        }
    }
    pub(super) fn observe_trace(&mut self, record: &NativeTraceRecord) -> Result<()> {
        match record {
            NativeTraceRecord::Begin {
                root_sequence,
                input_sequence,
                stack_parent,
                ..
            } => {
                ensure!(
                    *input_sequence == self.next_input,
                    "native prefix input gap outside verified replay audit"
                );
                if let Some((root, input)) = self.stack.last() {
                    ensure!(
                        *root == *root_sequence && Some(*input) == *stack_parent,
                        "native prefix nesting differs"
                    );
                } else {
                    ensure!(
                        *root_sequence == self.next_root && stack_parent.is_none(),
                        "native prefix root gap outside verified replay audit"
                    );
                    self.next_root = self
                        .next_root
                        .checked_add(1)
                        .context("native prefix root exhausted")?;
                }
                self.next_input = self
                    .next_input
                    .checked_add(1)
                    .context("native prefix input exhausted")?;
                self.stack.push((*root_sequence, *input_sequence));
            }
            NativeTraceRecord::Complete {
                root_sequence,
                input_sequence,
                ..
            } => {
                ensure!(
                    self.stack.pop() == Some((*root_sequence, *input_sequence)),
                    "native prefix completion differs"
                );
            }
            NativeTraceRecord::Abort { .. } | NativeTraceRecord::Uncovered { .. } => {
                anyhow::bail!("native prefix failed before cut")
            }
        }
        Ok(())
    }
    pub(super) fn observe_audit(
        &mut self,
        entry: &EventStoreEntry,
        expected: &NativeTraceSource,
        parent_run: Option<&str>,
        actual_prefix: DurableJournalPrefix,
    ) -> Result<()> {
        ensure!(
            !self.continued && self.stack.is_empty() && entry.headers.native_origin.is_none(),
            "native replay audit repeated or inside native source input"
        );
        let record: ReplayContinuationRecord = rmp_serde::from_slice(&entry.payload)?;
        record.child.validate()?;
        record.original.validate()?;
        ensure!(
            record.schema_version == 1
                && !record.execution_authorized
                && record.child == *expected
                && parent_run == Some(record.original.journal_run.as_str())
                && record.child.journal_run != record.original.journal_run
                && record.child.logical_run == record.original.logical_run
                && record.child.configuration_digest == record.original.configuration_digest
                && record.child.codec_profile == record.original.codec_profile
                && record.child.registered_profile_digest
                    == record.original.registered_profile_digest
                && record.child_prefix == actual_prefix
                && record.child_prefix.sequence.checked_add(1) == Some(entry.seq)
                && record.original_cut_prefix.run_id == record.original.journal_run
                && record.original_end_prefix.run_id == record.original.journal_run
                && record.original_end_prefix.sequence > record.original_cut_prefix.sequence
                && record.child_first_root == self.next_root
                && record.child_first_input == self.next_input,
            "native replay audit original/child/prefix binding differs"
        );
        let mut source_root = record.original_cut_root;
        let mut source_input = record.original_cut_input;
        for root in &record.roots {
            ensure!(
                source_root.checked_add(1) == Some(root.original_root)
                    && source_input.checked_add(1) == Some(root.original_first_input)
                    && root.input_count > 0
                    && root.original_first_input.checked_add(root.input_count - 1)
                        == Some(root.original_last_input)
                    && root.original_entry_digest.len() == 64
                    && root
                        .original_entry_digest
                        .bytes()
                        .all(|c| c.is_ascii_hexdigit())
                    && root.child_root == self.next_root
                    && root.child_first_input == self.next_input
                    && root.child_first_input.checked_add(root.input_count - 1)
                        == Some(root.child_last_input),
                "native replay audit progress differs"
            );
            source_root = root.original_root;
            source_input = root.original_last_input;
            self.next_root = self
                .next_root
                .checked_add(1)
                .context("native replay audit root exhausted")?;
            self.next_input = root
                .child_last_input
                .checked_add(1)
                .context("native replay audit input exhausted")?;
        }
        ensure!(
            source_root == record.original_final_root
                && source_input == record.original_final_input,
            "native replay audit final original frontier differs"
        );
        for pending in record.retained {
            let receipt = &pending.receipt;
            ensure!(
                receipt.channel_ordinal > 0
                    && self
                        .retained
                        .insert(receipt.message_id.to_string(), pending.clone())
                        .is_none(),
                "native replay audit duplicate/invalid retained FIFO"
            );
            if let Some(cause) = &receipt.caused_by {
                ensure!(
                    cause.journal_run == record.original.journal_run
                        && cause.process_incarnation == record.original.process_incarnation
                        && cause.root_sequence <= source_root
                        && cause.input_sequence <= source_input
                        && cause.root_sequence > 0
                        && cause.input_sequence > 0,
                    "native replay audit retained cause is foreign"
                );
            }
        }
        self.continued = true;
        Ok(())
    }
    pub(super) fn verify_retained_inventory(&self, pending: &[NativePendingInput]) -> Result<()> {
        for original in self.retained.values() {
            ensure!(
                pending.iter().any(|input| input == original),
                "native replay continuation retained inventory was lost or substituted"
            );
        }
        Ok(())
    }
    pub(super) fn consume_retained(
        &mut self,
        receipt: &NativeIngressReceipt,
        payload: &Value,
    ) -> Result<bool> {
        let Some(original) = self.retained.remove(&receipt.message_id.to_string()) else {
            return Ok(false);
        };
        ensure!(
            original.receipt == *receipt && original.payload == *payload,
            "native replay continuation retained input was substituted"
        );
        Ok(true)
    }
}
