//! Explicit recovery handoff contract for the live runner channels.
//!
//! The runner owns typed, in-process channels while recovery evidence is
//! durable data.  This module is the narrow boundary between those two
//! worlds: a caller supplies a validated JSON envelope and a registered,
//! channel-specific codec reconstructs one typed runner message.  The
//! resulting message is sent to the same channel receiver used by the live
//! event loop.
//!
//! This boundary deliberately stops before business recovery.  It does not
//! apply a message, persist a side effect, reconcile a venue, or authorize
//! execution.  A durable journal and the live node remain responsible for
//! those decisions.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Context, Result, ensure};
use nautilus_common::{
    messages::{DataEvent, ExecutionEvent, SystemCommand, SystemEvent, data::DataCommand},
    runner::{TimeEventMessage, TradingCommandMessage},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::runner::AsyncRunner;

/// Current wire schema for a recovery input sent to a runner channel.
pub const RUNNER_RECOVERY_ENVELOPE_SCHEMA_VERSION: u16 = 1;

/// The concrete internal channel which receives a decoded recovery input.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerRecoveryChannel {
    TimeEvent,
    SystemEvent,
    SystemCommand,
    ExecutionEvent,
    ExecutionCommand,
    DataEvent,
    DataCommand,
}

/// A durable, typed-channel recovery envelope.
///
/// The payload is intentionally opaque to this crate.  A registered codec
/// must validate its shape and reconstruct the matching concrete message.
/// The envelope metadata provides the ordering and scope checks which every
/// codec receives before it can enter an internal runner channel.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRecoveryEnvelope {
    pub schema_version: u16,
    pub recovery_id: String,
    pub checkpoint_sequence: u64,
    pub dispatch_sequence: u64,
    pub parent_dispatch_sequence: Option<u64>,
    pub channel: RunnerRecoveryChannel,
    pub codec_id: String,
    pub phase: String,
    pub payload: Value,
}

/// The composite checkpoint boundary which a pre-start runner handoff is
/// allowed to extend.
///
/// Binding this value before the first send gives the runner channel handoff a
/// single dispatch frontier shared with the composite recovery coordinator.
/// It is an ordering fence only: it does not prove that a message was
/// processed, persisted, reconciled or authorized for execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRecoveryWatermark {
    pub recovery_id: String,
    pub checkpoint_sequence: u64,
    pub dispatch_watermark: u64,
}

impl RunnerRecoveryWatermark {
    fn validate(&self) -> Result<()> {
        ensure!(
            valid_text(&self.recovery_id, 128),
            "runner recovery watermark id must be non-empty, NUL-free, and at most 128 bytes"
        );
        ensure!(
            self.checkpoint_sequence > 0,
            "runner recovery watermark checkpoint sequence must be positive"
        );
        ensure!(
            self.dispatch_watermark > 0,
            "runner recovery watermark dispatch sequence must be positive"
        );
        Ok(())
    }
}

/// The three independently observable boundaries of a pre-start recovery
/// handoff.
///
/// `sent_watermark` advances only after an item was accepted by the real
/// runner channel sender. `received_watermark` advances only after the live
/// event loop explicitly acknowledges dequeueing that same item, and
/// `processed_watermark` advances only after the node explicitly acknowledges
/// business handling. A later boundary never implies an earlier one for an
/// item that is out of order, and none of these fields grants execution
/// authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunnerRecoveryCompletionWatermark {
    /// The composite checkpoint boundary, when the handoff was bound to one.
    pub watermark: Option<RunnerRecoveryWatermark>,
    /// The greatest dispatch sequence accepted by a channel sender.
    pub sent_watermark: Option<u64>,
    /// The greatest contiguous sent sequence explicitly acknowledged as received.
    pub received_watermark: Option<u64>,
    /// The greatest contiguous sent sequence explicitly acknowledged as processed.
    pub processed_watermark: Option<u64>,
}

/// Explicit identity binding for one recovery input which was accepted by a
/// handoff sender.
///
/// The ordinary runner channels carry only their existing typed payloads, so
/// a receiver cannot infer this binding from a dequeued value. Obtaining a
/// binding therefore does not prove that the corresponding item was received
/// or processed. A host must establish that correspondence separately, then
/// call [`RunnerRecoveryDispatchBinding::acknowledge_received`] at dequeue and
/// [`RunnerRecoveryDispatchBinding::acknowledge_processed`] after successful
/// node handling. This type prevents the host from changing the bound
/// sequence or channel while keeping that missing identity evidence explicit.
#[derive(Clone, Debug)]
pub struct RunnerRecoveryDispatchBinding {
    progress: RunnerRecoveryProgressHandle,
    recovery_id: String,
    checkpoint_sequence: u64,
    dispatch_sequence: u64,
    channel: RunnerRecoveryChannel,
}

impl RunnerRecoveryDispatchBinding {
    /// Returns the recovery run identity carried by this binding.
    #[must_use]
    pub fn recovery_id(&self) -> &str {
        &self.recovery_id
    }

    /// Returns the composite checkpoint sequence carried by this binding.
    #[must_use]
    pub const fn checkpoint_sequence(&self) -> u64 {
        self.checkpoint_sequence
    }

    /// Returns the dispatch sequence carried by this binding.
    #[must_use]
    pub const fn dispatch_sequence(&self) -> u64 {
        self.dispatch_sequence
    }

    /// Returns the typed channel carried by this binding.
    #[must_use]
    pub const fn channel(&self) -> RunnerRecoveryChannel {
        self.channel
    }

    /// Records explicit evidence that the bound recovery input was dequeued.
    ///
    /// This method does not inspect a channel and cannot prove the caller's
    /// identity mapping. It only validates the immutable binding against the
    /// handoff ledger and advances the received watermark when the caller has
    /// already established that mapping.
    ///
    /// # Errors
    /// Returns an error for a duplicate acknowledgement, a stale or unknown
    /// binding, or a poisoned progress ledger.
    pub fn acknowledge_received(&self) -> Result<RunnerRecoveryCompletionWatermark> {
        self.progress
            .acknowledge_received(self.dispatch_sequence, self.channel)
    }

    /// Records explicit evidence that the bound recovery input finished node
    /// processing successfully.
    ///
    /// The received acknowledgement must happen first. This method only
    /// advances recovery bookkeeping and never authorizes execution.
    ///
    /// # Errors
    /// Returns an error when the input was not acknowledged as received, the
    /// binding is stale or unknown, the acknowledgement is duplicated, or the
    /// progress ledger was poisoned.
    pub fn acknowledge_processed(&self) -> Result<RunnerRecoveryCompletionWatermark> {
        self.progress
            .acknowledge_processed(self.dispatch_sequence, self.channel)
    }
}

/// A thread-safe progress handle shared by a handoff and its runner.
///
/// The live node should retain this handle before it consumes the runner,
/// call [`RunnerRecoveryProgressHandle::acknowledge_received`] immediately
/// after dequeuing a recovery input, and call
/// [`RunnerRecoveryProgressHandle::acknowledge_processed`] only after the node's
/// processing path returns successfully. The handle is bookkeeping only; it
/// has no method that authorizes execution.
#[derive(Clone, Debug)]
pub struct RunnerRecoveryProgressHandle {
    ledger: Arc<RunnerRecoveryLedger>,
}

#[derive(Debug, Default)]
struct RunnerRecoveryLedger {
    state: Mutex<RunnerRecoveryLedgerState>,
}

#[derive(Debug, Default)]
struct RunnerRecoveryLedgerState {
    watermark: Option<RunnerRecoveryWatermark>,
    sent_channels: BTreeMap<u64, RunnerRecoveryChannel>,
    received: BTreeSet<u64>,
    processed: BTreeSet<u64>,
    sent_watermark: Option<u64>,
    received_watermark: Option<u64>,
    processed_watermark: Option<u64>,
}

impl RunnerRecoveryProgressHandle {
    pub(crate) fn new() -> Self {
        Self {
            ledger: Arc::new(RunnerRecoveryLedger::default()),
        }
    }

    /// Returns a point-in-time snapshot of sent, received and processed
    /// progress. The three watermarks are intentionally separate.
    ///
    /// # Errors
    /// Returns an error if the progress ledger was poisoned by a panic in
    /// another thread.
    pub fn completion_watermark(&self) -> Result<RunnerRecoveryCompletionWatermark> {
        self.ledger.snapshot()
    }

    /// Returns an immutable identity binding for one sent input.
    ///
    /// A binding is available only for a handoff with a bound composite
    /// watermark. It is a scoped acknowledgement capability, not evidence
    /// that the untagged channel receiver has dequeued the item.
    ///
    /// # Errors
    /// Returns an error when the sequence was not sent, the channel does not
    /// match, no composite watermark was bound, or the ledger was poisoned.
    pub fn binding_for(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryDispatchBinding> {
        self.ledger
            .binding_for(self.clone(), dispatch_sequence, channel)
    }

    /// Acknowledges that a sent recovery input was dequeued by the live
    /// runner. Dequeueing alone does not advance the processed watermark.
    ///
    /// # Errors
    /// Returns an error for an unknown sequence, a channel mismatch, a
    /// duplicate acknowledgement, or a poisoned progress ledger.
    pub fn acknowledge_received(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        self.ledger.acknowledge_received(dispatch_sequence, channel)
    }

    /// Acknowledges that the live node completed handling a previously
    /// received recovery input. This is the only operation that advances the
    /// processed watermark.
    ///
    /// # Errors
    /// Returns an error when the input was not sent or received, the channel
    /// does not match, the acknowledgement is duplicated, or the progress
    /// ledger was poisoned.
    pub fn acknowledge_processed(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        self.ledger
            .acknowledge_processed(dispatch_sequence, channel)
    }

    fn bind_watermark(&self, watermark: RunnerRecoveryWatermark) -> Result<()> {
        self.ledger.bind_watermark(watermark)
    }

    fn send_and_record<F>(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
        send: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        self.ledger
            .send_and_record(dispatch_sequence, channel, send)
    }
}

impl RunnerRecoveryLedger {
    fn lock(&self) -> Result<MutexGuard<'_, RunnerRecoveryLedgerState>> {
        self.state
            .lock()
            .map_err(|_| anyhow::anyhow!("runner recovery progress ledger is poisoned"))
    }

    fn snapshot(&self) -> Result<RunnerRecoveryCompletionWatermark> {
        Ok(self.lock()?.snapshot())
    }

    fn binding_for(
        &self,
        progress: RunnerRecoveryProgressHandle,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryDispatchBinding> {
        let state = self.lock()?;
        Self::ensure_sent_channel(&state, dispatch_sequence, channel)?;
        let watermark = state
            .watermark
            .as_ref()
            .context("runner recovery dispatch binding requires a bound composite watermark")?;
        Ok(RunnerRecoveryDispatchBinding {
            progress,
            recovery_id: watermark.recovery_id.clone(),
            checkpoint_sequence: watermark.checkpoint_sequence,
            dispatch_sequence,
            channel,
        })
    }

    fn bind_watermark(&self, watermark: RunnerRecoveryWatermark) -> Result<()> {
        let mut state = self.lock()?;
        if let Some(existing) = &state.watermark {
            ensure!(
                existing == &watermark,
                "runner recovery progress watermark binding differs from the existing boundary"
            );
            return Ok(());
        }
        ensure!(
            state.sent_channels.is_empty(),
            "runner recovery progress watermark must be bound before the first sent input"
        );
        state.watermark = Some(watermark);
        Ok(())
    }

    fn send_and_record<F>(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
        send: F,
    ) -> Result<()>
    where
        F: FnOnce() -> Result<()>,
    {
        let mut state = self.lock()?;
        state.validate_sent(dispatch_sequence)?;
        send()?;
        // `validate_sent` ran while the ledger lock was held, and the send is
        // non-blocking. Therefore this insertion cannot race another handoff
        // and cannot turn a successful channel send into an untracked input.
        let previous = state.sent_channels.insert(dispatch_sequence, channel);
        debug_assert!(previous.is_none());
        state.sent_watermark = Some(dispatch_sequence);
        Ok(())
    }

    fn acknowledge_received(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        let mut state = self.lock()?;
        Self::ensure_sent_channel(&state, dispatch_sequence, channel)?;
        ensure!(
            state.received.insert(dispatch_sequence),
            "runner recovery dispatch sequence {dispatch_sequence} was already acknowledged as received"
        );
        let received_watermark = state.contiguous_watermark(&state.received);
        state.received_watermark = received_watermark;
        Ok(state.snapshot())
    }

    fn acknowledge_processed(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        let mut state = self.lock()?;
        Self::ensure_sent_channel(&state, dispatch_sequence, channel)?;
        ensure!(
            state.received.contains(&dispatch_sequence),
            "runner recovery dispatch sequence {dispatch_sequence} must be acknowledged as received before processed"
        );
        ensure!(
            state.processed.insert(dispatch_sequence),
            "runner recovery dispatch sequence {dispatch_sequence} was already acknowledged as processed"
        );
        let processed_watermark = state.contiguous_watermark(&state.processed);
        state.processed_watermark = processed_watermark;
        Ok(state.snapshot())
    }

    fn ensure_sent_channel(
        state: &RunnerRecoveryLedgerState,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<()> {
        let sent_channel = state
            .sent_channels
            .get(&dispatch_sequence)
            .copied()
            .with_context(|| {
                format!("runner recovery dispatch sequence {dispatch_sequence} was not sent")
            })?;
        ensure!(
            sent_channel == channel,
            "runner recovery dispatch sequence {dispatch_sequence} belongs to {sent_channel:?}, not {channel:?}"
        );
        Ok(())
    }
}

impl RunnerRecoveryLedgerState {
    fn validate_sent(&self, dispatch_sequence: u64) -> Result<()> {
        ensure!(
            dispatch_sequence > 0,
            "runner recovery sent dispatch sequence must be positive"
        );
        if let Some(watermark) = &self.watermark {
            ensure!(
                dispatch_sequence > watermark.dispatch_watermark,
                "runner recovery sent dispatch sequence must be after the bound watermark"
            );
        }
        if let Some(previous) = self.sent_watermark {
            ensure!(
                dispatch_sequence > previous,
                "runner recovery sent dispatch sequence {dispatch_sequence} is not after {previous}"
            );
        }
        Ok(())
    }
}

impl RunnerRecoveryLedgerState {
    fn contiguous_watermark(&self, acknowledged: &BTreeSet<u64>) -> Option<u64> {
        let mut watermark = None;
        for sequence in self.sent_channels.keys() {
            if !acknowledged.contains(sequence) {
                break;
            }
            watermark = Some(*sequence);
        }
        watermark
    }

    fn snapshot(&self) -> RunnerRecoveryCompletionWatermark {
        RunnerRecoveryCompletionWatermark {
            watermark: self.watermark.clone(),
            sent_watermark: self.sent_watermark,
            received_watermark: self.received_watermark,
            processed_watermark: self.processed_watermark,
        }
    }
}

impl RunnerRecoveryEnvelope {
    /// Constructs and validates one recovery envelope.
    ///
    /// # Errors
    /// Returns an error when metadata is empty, out of range, or the payload
    /// is not a JSON object.
    #[allow(
        clippy::too_many_arguments,
        reason = "The wire constructor keeps every durable envelope field explicit for auditability."
    )]
    pub fn new(
        recovery_id: impl Into<String>,
        checkpoint_sequence: u64,
        dispatch_sequence: u64,
        parent_dispatch_sequence: Option<u64>,
        channel: RunnerRecoveryChannel,
        codec_id: impl Into<String>,
        phase: impl Into<String>,
        payload: Value,
    ) -> Result<Self> {
        let envelope = Self {
            schema_version: RUNNER_RECOVERY_ENVELOPE_SCHEMA_VERSION,
            recovery_id: recovery_id.into(),
            checkpoint_sequence,
            dispatch_sequence,
            parent_dispatch_sequence,
            channel,
            codec_id: codec_id.into(),
            phase: phase.into(),
            payload,
        };
        envelope.validate()?;
        Ok(envelope)
    }

    /// Validates scope, ordering metadata, and the durable payload shape.
    ///
    /// # Errors
    /// Returns an error for an unsupported schema, invalid identifiers,
    /// non-positive sequence values, an invalid parent relation, or a
    /// non-object payload.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema_version == RUNNER_RECOVERY_ENVELOPE_SCHEMA_VERSION,
            "unsupported runner recovery envelope schema {}",
            self.schema_version
        );
        ensure!(
            valid_text(&self.recovery_id, 128),
            "runner recovery id must be non-empty, NUL-free, and at most 128 bytes"
        );
        ensure!(
            self.checkpoint_sequence > 0,
            "runner recovery checkpoint sequence must be positive"
        );
        ensure!(
            self.dispatch_sequence > 0,
            "runner recovery dispatch sequence must be positive"
        );
        if let Some(parent) = self.parent_dispatch_sequence {
            ensure!(
                parent > 0 && parent < self.dispatch_sequence,
                "runner recovery parent dispatch sequence must precede the input"
            );
        }
        ensure!(
            valid_text(&self.phase, 128),
            "runner recovery phase must be non-empty, NUL-free, and at most 128 bytes"
        );
        ensure!(
            valid_text(&self.codec_id, 128),
            "runner recovery codec id must be non-empty, NUL-free, and at most 128 bytes"
        );
        ensure!(
            self.payload.is_object(),
            "runner recovery payload must be a JSON object"
        );
        Ok(())
    }

    /// Serializes the validated envelope to JSON bytes.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    pub fn encode_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).context("serialize runner recovery envelope")
    }

    /// Parses and validates an envelope from JSON bytes.
    ///
    /// # Errors
    /// Returns an error for malformed JSON, unknown fields, or invalid
    /// envelope metadata.
    pub fn decode_json(bytes: &[u8]) -> Result<Self> {
        let envelope: Self =
            serde_json::from_slice(bytes).context("deserialize runner recovery envelope")?;
        envelope.validate()?;
        Ok(envelope)
    }
}

fn valid_text(value: &str, max_bytes: usize) -> bool {
    !value.is_empty() && value.len() <= max_bytes && !value.contains('\0')
}

/// A typed message decoded from a recovery envelope.
#[derive(Debug)]
#[allow(
    clippy::large_enum_variant,
    reason = "The enum carries the concrete messages already owned by the runner; boxing would add an allocation before channel handoff."
)]
pub enum RunnerRecoveryEvent {
    TimeEvent(TimeEventMessage),
    SystemEvent(SystemEvent),
    SystemCommand(SystemCommand),
    ExecutionEvent(ExecutionEvent),
    ExecutionCommand(TradingCommandMessage),
    DataEvent(DataEvent),
    DataCommand(DataCommand),
}

impl RunnerRecoveryEvent {
    /// Returns the internal channel for this typed event.
    #[must_use]
    pub const fn channel(&self) -> RunnerRecoveryChannel {
        match self {
            Self::TimeEvent(_) => RunnerRecoveryChannel::TimeEvent,
            Self::SystemEvent(_) => RunnerRecoveryChannel::SystemEvent,
            Self::SystemCommand(_) => RunnerRecoveryChannel::SystemCommand,
            Self::ExecutionEvent(_) => RunnerRecoveryChannel::ExecutionEvent,
            Self::ExecutionCommand(_) => RunnerRecoveryChannel::ExecutionCommand,
            Self::DataEvent(_) => RunnerRecoveryChannel::DataEvent,
            Self::DataCommand(_) => RunnerRecoveryChannel::DataCommand,
        }
    }
}

/// A channel-specific decoder supplied by the application.
///
/// Implementations are process-local and must reject payloads they do not
/// understand.  The trait has no authorization method by design.
pub trait RunnerRecoveryCodec: std::fmt::Debug {
    /// Declares the only channel this codec may populate.
    fn channel(&self) -> RunnerRecoveryChannel;

    /// Declares the stable codec identity/version accepted by this registry.
    fn codec_id(&self) -> &str;

    /// Decodes one validated envelope into a matching typed runner event.
    ///
    /// # Errors
    /// Implementations must return an error for unsupported payloads or
    /// metadata.  Returning an event for another channel is rejected by the
    /// registry.
    fn decode(&self, envelope: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent>;
}

/// Explicit registration table for recovery codecs.
///
/// A registry must be created with the channels it is intended to support,
/// populated exactly once, and sealed before an envelope can be decoded.  A
/// missing registration never falls back to a generic callback or silently
/// enters a channel.
pub struct RunnerRecoveryCodecRegistry {
    required: BTreeSet<RunnerRecoveryChannel>,
    codecs: BTreeMap<RunnerRecoveryChannel, Box<dyn RunnerRecoveryCodec>>,
    sealed: bool,
}

impl std::fmt::Debug for RunnerRecoveryCodecRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunnerRecoveryCodecRegistry")
            .field("required", &self.required)
            .field("registered", &self.codecs.keys().collect::<Vec<_>>())
            .field("sealed", &self.sealed)
            .finish()
    }
}

impl RunnerRecoveryCodecRegistry {
    /// Creates a registry with an explicit allow-list of channels.
    #[must_use]
    pub fn new<I>(required: I) -> Self
    where
        I: IntoIterator<Item = RunnerRecoveryChannel>,
    {
        Self {
            required: required.into_iter().collect(),
            codecs: BTreeMap::new(),
            sealed: false,
        }
    }

    /// Registers one codec for its declared channel.
    ///
    /// # Errors
    /// Returns an error after sealing, for a channel outside the allow-list,
    /// or for a duplicate registration.
    pub fn register<C>(&mut self, codec: C) -> Result<()>
    where
        C: RunnerRecoveryCodec + 'static,
    {
        ensure!(!self.sealed, "runner recovery codec registry is sealed");
        let channel = codec.channel();
        ensure!(
            valid_text(codec.codec_id(), 128),
            "runner recovery codec id must be non-empty, NUL-free, and at most 128 bytes"
        );
        ensure!(
            self.required.contains(&channel),
            "runner recovery codec channel {channel:?} was not declared"
        );
        ensure!(
            !self.codecs.contains_key(&channel),
            "runner recovery codec for {channel:?} is already registered"
        );
        self.codecs.insert(channel, Box::new(codec));
        Ok(())
    }

    /// Seals the registry after all declared codecs are installed.
    ///
    /// # Errors
    /// Returns an error when one or more declared channels are missing.
    pub fn seal(mut self) -> Result<Self> {
        let registered: BTreeSet<_> = self.codecs.keys().copied().collect();
        let missing: Vec<_> = self.required.difference(&registered).copied().collect();
        ensure!(
            missing.is_empty(),
            "runner recovery codec registry missing channels: {missing:?}"
        );
        self.sealed = true;
        Ok(self)
    }

    /// Returns whether all registrations are finalized.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.sealed
    }

    /// Decodes one envelope through its explicitly registered codec.
    ///
    /// # Errors
    /// Returns an error for an unsealed registry, malformed envelope, missing
    /// codec, codec failure, or a channel mismatch.
    pub fn decode(&self, envelope: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
        ensure!(self.sealed, "runner recovery codec registry is not sealed");
        envelope.validate()?;
        let codec = self
            .codecs
            .get(&envelope.channel)
            .with_context(|| format!("no recovery codec registered for {:?}", envelope.channel))?;
        ensure!(
            envelope.codec_id == codec.codec_id(),
            "runner recovery codec id {:?} does not match registered codec {:?}",
            envelope.codec_id,
            codec.codec_id()
        );
        let event = codec.decode(envelope)?;
        ensure!(
            event.channel() == envelope.channel,
            "recovery codec returned {:?} for {:?}",
            event.channel(),
            envelope.channel
        );
        Ok(event)
    }
}

/// A pre-start handoff into the runner's real internal mpsc channels.
///
/// The handoff is invalidated when the runner enters its loop or its channel
/// receivers are extracted.  This prevents a retained recovery handle from
/// injecting messages into an already-running node through an out-of-band
/// path.  Duplicate/idempotent business effects remain the journal and node's
/// responsibility.
#[derive(Debug)]
pub struct RunnerRecoveryHandoff {
    available: Arc<AtomicBool>,
    progress: RunnerRecoveryProgressHandle,
    time_evt_tx: tokio::sync::mpsc::UnboundedSender<TimeEventMessage>,
    system_evt_tx: tokio::sync::mpsc::UnboundedSender<SystemEvent>,
    system_cmd_tx: tokio::sync::mpsc::UnboundedSender<SystemCommand>,
    exec_evt_tx: tokio::sync::mpsc::UnboundedSender<ExecutionEvent>,
    exec_cmd_tx: tokio::sync::mpsc::UnboundedSender<TradingCommandMessage>,
    data_evt_tx: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    data_cmd_tx: tokio::sync::mpsc::UnboundedSender<DataCommand>,
    last_dispatch_sequence: Option<u64>,
    watermark: Option<RunnerRecoveryWatermark>,
}

#[derive(Debug)]
struct RunnerRecoverySenders {
    time_event: tokio::sync::mpsc::UnboundedSender<TimeEventMessage>,
    system_event: tokio::sync::mpsc::UnboundedSender<SystemEvent>,
    system_command: tokio::sync::mpsc::UnboundedSender<SystemCommand>,
    execution_event: tokio::sync::mpsc::UnboundedSender<ExecutionEvent>,
    execution_command: tokio::sync::mpsc::UnboundedSender<TradingCommandMessage>,
    data_event: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    data_command: tokio::sync::mpsc::UnboundedSender<DataCommand>,
}

impl RunnerRecoveryHandoff {
    fn new(
        available: Arc<AtomicBool>,
        progress: RunnerRecoveryProgressHandle,
        senders: RunnerRecoverySenders,
    ) -> Self {
        Self {
            available,
            progress,
            time_evt_tx: senders.time_event,
            system_evt_tx: senders.system_event,
            system_cmd_tx: senders.system_command,
            exec_evt_tx: senders.execution_event,
            exec_cmd_tx: senders.execution_command,
            data_evt_tx: senders.data_event,
            data_cmd_tx: senders.data_command,
            last_dispatch_sequence: None,
            watermark: None,
        }
    }

    /// Binds this handoff to one composite recovery checkpoint before any
    /// envelope is sent.
    ///
    /// Every subsequent envelope must carry the same recovery id and
    /// checkpoint sequence, and its dispatch sequence must be strictly after
    /// the supplied composite watermark. A handoff may be bound only before
    /// its first send; repeated binding to the identical value is accepted so
    /// an orchestrator can make the boundary explicit at each retry decision.
    ///
    /// This method only establishes a shared ordering boundary. It never
    /// authorizes execution or asserts that a receiver processed a message.
    ///
    /// # Errors
    /// Returns an error when the handoff is closed, the binding is malformed,
    /// a different binding is requested, or messages were already sent before
    /// the first binding.
    pub fn bind_watermark(
        &mut self,
        recovery_id: impl Into<String>,
        checkpoint_sequence: u64,
        dispatch_watermark: u64,
    ) -> Result<()> {
        self.ensure_available()?;
        let binding = RunnerRecoveryWatermark {
            recovery_id: recovery_id.into(),
            checkpoint_sequence,
            dispatch_watermark,
        };
        binding.validate()?;
        if let Some(existing) = &self.watermark {
            ensure!(
                existing == &binding,
                "runner recovery watermark binding differs from the existing boundary"
            );
            return Ok(());
        }
        ensure!(
            self.last_dispatch_sequence.is_none(),
            "runner recovery watermark must be bound before the first envelope is sent"
        );
        self.progress.bind_watermark(binding.clone())?;
        self.watermark = Some(binding);
        Ok(())
    }

    /// Returns the composite boundary bound to this handoff, if one was set.
    #[must_use]
    pub fn watermark(&self) -> Option<&RunnerRecoveryWatermark> {
        self.watermark.as_ref()
    }

    /// Returns the shared sent/received/processed recovery progress handle.
    ///
    /// Hosts should retain this handle before starting or consuming the
    /// runner, then pass the dispatch sequence and channel observed by the
    /// live processing path to the explicit acknowledgement methods. Closing
    /// the pre-start handoff does not invalidate this bookkeeping handle.
    #[must_use]
    pub fn progress(&self) -> RunnerRecoveryProgressHandle {
        self.progress.clone()
    }

    /// Returns an immutable identity binding for one sent recovery input.
    ///
    /// This is the explicit bridge a host may retain while it correlates a
    /// typed channel value with its durable recovery envelope. The runner
    /// cannot create that correlation from the existing channel payload, so
    /// this method does not claim that the input was received or processed.
    ///
    /// # Errors
    /// Returns an error when the sequence was not sent, the channel does not
    /// match, no composite watermark was bound, or the progress ledger was
    /// poisoned.
    pub fn binding_for(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryDispatchBinding> {
        self.progress.binding_for(dispatch_sequence, channel)
    }

    /// Returns a point-in-time sent/received/processed progress snapshot.
    ///
    /// # Errors
    /// Returns an error if the shared progress ledger was poisoned.
    pub fn completion_watermark(&self) -> Result<RunnerRecoveryCompletionWatermark> {
        self.progress.completion_watermark()
    }

    /// Records that a sent recovery input was dequeued by the live runner.
    /// This does not mark the input processed.
    ///
    /// # Errors
    /// Returns an error for an unknown sequence, channel mismatch, duplicate
    /// acknowledgement, or a poisoned progress ledger.
    pub fn acknowledge_received(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        self.progress
            .acknowledge_received(dispatch_sequence, channel)
    }

    /// Records that the live node completed handling a received recovery
    /// input. This method has no execution-authority effect.
    ///
    /// # Errors
    /// Returns an error when the input was not sent or received, the channel
    /// does not match, the acknowledgement is duplicated, or the progress
    /// ledger was poisoned.
    pub fn acknowledge_processed(
        &self,
        dispatch_sequence: u64,
        channel: RunnerRecoveryChannel,
    ) -> Result<RunnerRecoveryCompletionWatermark> {
        self.progress
            .acknowledge_processed(dispatch_sequence, channel)
    }

    /// Enqueues one decoded recovery input into the corresponding real runner
    /// channel.
    ///
    /// # Errors
    /// Returns an error if the handoff is closed, the registry is incomplete,
    /// the sequence is stale/out of order, decoding fails, or the receiver is
    /// closed.  A failed send does not advance the sequence frontier.
    pub fn enqueue(
        &mut self,
        envelope: &RunnerRecoveryEnvelope,
        registry: &RunnerRecoveryCodecRegistry,
    ) -> Result<()> {
        self.ensure_available()?;
        envelope.validate()?;
        self.ensure_watermark(envelope)?;
        self.ensure_sequence(envelope.dispatch_sequence)?;
        let channel = envelope.channel;
        let event = registry.decode(envelope)?;
        self.progress
            .send_and_record(envelope.dispatch_sequence, channel, || self.send(event))?;
        self.last_dispatch_sequence = Some(envelope.dispatch_sequence);
        Ok(())
    }

    /// Preflights and enqueues a FIFO batch into the runner channels.
    ///
    /// Every envelope is validated and decoded before the first send.  If a
    /// receiver closes during sending, the returned error identifies the
    /// channel boundary; the sequence frontier reports the successfully sent
    /// prefix and the caller must retry only the remaining suffix.
    ///
    /// # Errors
    /// Returns an error if preflight fails, an envelope is out of order, the
    /// handoff closes, or a typed channel receiver is closed.
    pub fn enqueue_batch<I>(
        &mut self,
        envelopes: I,
        registry: &RunnerRecoveryCodecRegistry,
    ) -> Result<usize>
    where
        I: IntoIterator<Item = RunnerRecoveryEnvelope>,
    {
        self.ensure_available()?;
        let mut staged = Vec::new();
        let mut frontier = self.last_dispatch_sequence;
        for envelope in envelopes {
            envelope.validate()?;
            self.ensure_watermark_at(&envelope, frontier)?;
            if let Some(previous) = frontier {
                ensure!(
                    envelope.dispatch_sequence > previous,
                    "runner recovery dispatch sequence {} is not after {}",
                    envelope.dispatch_sequence,
                    previous
                );
            }
            let event = registry.decode(&envelope)?;
            staged.push((envelope.dispatch_sequence, envelope.channel, event));
            frontier = Some(envelope.dispatch_sequence);
        }

        let mut sent = 0;
        for (sequence, channel, event) in staged {
            self.ensure_available()?;
            self.progress
                .send_and_record(sequence, channel, || self.send(event))?;
            self.last_dispatch_sequence = Some(sequence);
            sent += 1;
        }
        Ok(sent)
    }

    /// Returns the last sequence successfully sent by this handoff.
    #[must_use]
    pub const fn last_dispatch_sequence(&self) -> Option<u64> {
        self.last_dispatch_sequence
    }

    fn ensure_available(&self) -> Result<()> {
        ensure!(
            self.available.load(Ordering::Acquire),
            "runner recovery handoff is closed after runner startup or channel extraction"
        );
        Ok(())
    }

    fn ensure_sequence(&self, sequence: u64) -> Result<()> {
        if let Some(previous) = self.last_dispatch_sequence {
            ensure!(
                sequence > previous,
                "runner recovery dispatch sequence {sequence} is not after {previous}"
            );
        }
        Ok(())
    }

    fn ensure_watermark(&self, envelope: &RunnerRecoveryEnvelope) -> Result<()> {
        self.ensure_watermark_at(envelope, self.last_dispatch_sequence)
    }

    fn ensure_watermark_at(
        &self,
        envelope: &RunnerRecoveryEnvelope,
        frontier: Option<u64>,
    ) -> Result<()> {
        if let Some(binding) = &self.watermark {
            ensure!(
                envelope.recovery_id == binding.recovery_id,
                "runner recovery envelope id does not match the bound recovery"
            );
            ensure!(
                envelope.checkpoint_sequence == binding.checkpoint_sequence,
                "runner recovery envelope checkpoint sequence does not match the bound checkpoint"
            );
            ensure!(
                envelope.dispatch_sequence > binding.dispatch_watermark,
                "runner recovery envelope dispatch sequence must be after the bound watermark"
            );
            let expected = frontier
                .unwrap_or(binding.dispatch_watermark)
                .checked_add(1)
                .context("runner recovery dispatch watermark exhausted")?;
            ensure!(
                envelope.dispatch_sequence == expected,
                "runner recovery envelope dispatch sequence {} does not extend watermark contiguously from {}",
                envelope.dispatch_sequence,
                expected - 1
            );
        }
        Ok(())
    }

    fn send(&self, event: RunnerRecoveryEvent) -> Result<()> {
        match event {
            RunnerRecoveryEvent::TimeEvent(message) => self
                .time_evt_tx
                .send(message)
                .map_err(|_| anyhow::anyhow!("runner time-event receiver is closed")),
            RunnerRecoveryEvent::SystemEvent(event) => self
                .system_evt_tx
                .send(event)
                .map_err(|_| anyhow::anyhow!("runner system-event receiver is closed")),
            RunnerRecoveryEvent::SystemCommand(command) => self
                .system_cmd_tx
                .send(command)
                .map_err(|_| anyhow::anyhow!("runner system-command receiver is closed")),
            RunnerRecoveryEvent::ExecutionEvent(event) => self
                .exec_evt_tx
                .send(event)
                .map_err(|_| anyhow::anyhow!("runner execution-event receiver is closed")),
            RunnerRecoveryEvent::ExecutionCommand(command) => self
                .exec_cmd_tx
                .send(command)
                .map_err(|_| anyhow::anyhow!("runner execution-command receiver is closed")),
            RunnerRecoveryEvent::DataEvent(event) => self
                .data_evt_tx
                .send(event)
                .map_err(|_| anyhow::anyhow!("runner data-event receiver is closed")),
            RunnerRecoveryEvent::DataCommand(command) => self
                .data_cmd_tx
                .send(command)
                .map_err(|_| anyhow::anyhow!("runner data-command receiver is closed")),
        }
    }
}

impl RunnerRecoveryHandoff {
    pub(crate) fn from_runner(runner: &AsyncRunner) -> Result<Self> {
        ensure!(
            runner.recovery_handoff_available().load(Ordering::Acquire),
            "runner recovery handoff is closed after runner startup or channel extraction"
        );
        Ok(Self::new(
            runner.recovery_handoff_available().clone(),
            runner.recovery_progress_handle(),
            RunnerRecoverySenders {
                time_event: runner.time_event_sender_clone(),
                system_event: runner.system_event_sender_clone(),
                system_command: runner.system_command_sender_clone(),
                execution_event: runner.execution_event_sender_clone(),
                execution_command: runner.execution_command_sender_clone(),
                data_event: runner.data_event_sender_clone(),
                data_command: runner.data_command_sender_clone(),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_common::messages::system::ReconnectSocket;
    use nautilus_core::UnixNanos;
    use nautilus_model::identifiers::{ClientId, TraderId};
    use ustr::Ustr;

    #[derive(Debug)]
    struct SystemCommandFixtureCodec;

    impl RunnerRecoveryCodec for SystemCommandFixtureCodec {
        fn channel(&self) -> RunnerRecoveryChannel {
            RunnerRecoveryChannel::SystemCommand
        }

        fn codec_id(&self) -> &'static str {
            "system.reconnect_socket.v1"
        }

        fn decode(&self, envelope: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            let endpoint = envelope
                .payload
                .get("endpoint")
                .and_then(Value::as_str)
                .context("fixture endpoint")?;
            Ok(RunnerRecoveryEvent::SystemCommand(
                SystemCommand::ReconnectSocket(ReconnectSocket::new(
                    TraderId::from("TRADER-RECOVERY"),
                    ClientId::from("CLIENT-RECOVERY"),
                    Ustr::from(endpoint),
                    UnixNanos::from(envelope.dispatch_sequence),
                )),
            ))
        }
    }

    fn registry() -> RunnerRecoveryCodecRegistry {
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::SystemCommand]);
        registry.register(SystemCommandFixtureCodec).unwrap();
        registry.seal().unwrap()
    }

    fn envelope(sequence: u64, endpoint: &str) -> RunnerRecoveryEnvelope {
        RunnerRecoveryEnvelope::new(
            "recovery-fixture",
            7,
            sequence,
            sequence.checked_sub(1).filter(|parent| *parent > 0),
            RunnerRecoveryChannel::SystemCommand,
            "system.reconnect_socket.v1",
            "paused_recovery",
            serde_json::json!({"endpoint": endpoint}),
        )
        .unwrap()
    }

    #[test]
    fn envelope_json_roundtrip_rejects_unknown_fields_and_invalid_shape() {
        let original = envelope(2, "endpoint-2");
        let encoded = original.encode_json().unwrap();
        assert_eq!(
            RunnerRecoveryEnvelope::decode_json(&encoded).unwrap(),
            original
        );

        let mut value: Value = serde_json::from_slice(&encoded).unwrap();
        value["unexpected"] = Value::Bool(true);
        assert!(RunnerRecoveryEnvelope::decode_json(&serde_json::to_vec(&value).unwrap()).is_err());

        let mut invalid = original;
        invalid.payload = Value::Null;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn registry_requires_explicit_sealed_registration_and_matching_channel() {
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::SystemCommand]);
        assert!(registry.decode(&envelope(1, "endpoint-1")).is_err());
        registry.register(SystemCommandFixtureCodec).unwrap();
        let registry = registry.seal().unwrap();
        assert!(registry.is_sealed());
        let event = registry.decode(&envelope(1, "endpoint-1")).unwrap();
        assert_eq!(event.channel(), RunnerRecoveryChannel::SystemCommand);
    }

    #[test]
    fn handoff_sends_fifo_into_the_real_runner_channel_and_closes_on_extraction() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        let registry = registry();
        let sent = handoff
            .enqueue_batch(
                [envelope(1, "endpoint-1"), envelope(2, "endpoint-2")],
                &registry,
            )
            .unwrap();
        assert_eq!(sent, 2);
        assert_eq!(handoff.last_dispatch_sequence(), Some(2));

        let mut channels = runner.take_channels();
        let first = channels.system_cmd_rx.try_recv().unwrap();
        let second = channels.system_cmd_rx.try_recv().unwrap();
        let SystemCommand::ReconnectSocket(first) = first;
        let SystemCommand::ReconnectSocket(second) = second;
        assert_eq!(first.endpoint, Ustr::from("endpoint-1"));
        assert_eq!(second.endpoint, Ustr::from("endpoint-2"));
        assert!(
            handoff
                .enqueue(&envelope(3, "endpoint-3"), &registry)
                .is_err()
        );
    }

    #[test]
    fn handoff_rejects_duplicate_or_out_of_order_sequences_before_send() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        let registry = registry();
        handoff
            .enqueue(&envelope(3, "endpoint-3"), &registry)
            .unwrap();
        assert!(
            handoff
                .enqueue(&envelope(3, "endpoint-3"), &registry)
                .is_err()
        );
        assert!(
            handoff
                .enqueue(&envelope(2, "endpoint-2"), &registry)
                .is_err()
        );
        assert_eq!(handoff.last_dispatch_sequence(), Some(3));
    }

    #[test]
    fn handoff_binds_envelopes_to_one_composite_watermark() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        let registry = registry();
        handoff.bind_watermark("recovery-fixture", 7, 3).unwrap();
        assert_eq!(
            handoff.watermark(),
            Some(&RunnerRecoveryWatermark {
                recovery_id: "recovery-fixture".into(),
                checkpoint_sequence: 7,
                dispatch_watermark: 3,
            })
        );

        // The composite checkpoint itself is not replayed into the runner;
        // every queued input must extend its dispatch frontier.
        assert!(
            handoff
                .enqueue(&envelope(1, "before-checkpoint"), &registry)
                .is_err()
        );

        let valid = RunnerRecoveryEnvelope::new(
            "recovery-fixture",
            7,
            4,
            Some(3),
            RunnerRecoveryChannel::SystemCommand,
            "system.reconnect_socket.v1",
            "paused_recovery",
            serde_json::json!({"endpoint": "after-checkpoint"}),
        )
        .unwrap();
        handoff.enqueue(&valid, &registry).unwrap();

        let mismatched_id = RunnerRecoveryEnvelope::new(
            "other-recovery",
            7,
            5,
            Some(4),
            RunnerRecoveryChannel::SystemCommand,
            "system.reconnect_socket.v1",
            "paused_recovery",
            serde_json::json!({"endpoint": "wrong-id"}),
        )
        .unwrap();
        assert!(handoff.enqueue(&mismatched_id, &registry).is_err());
        assert!(handoff.bind_watermark("other-recovery", 7, 3).is_err());
    }

    #[test]
    fn progress_separates_sent_received_and_processed_watermarks() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        let registry = registry();
        handoff.bind_watermark("recovery-fixture", 7, 3).unwrap();
        handoff
            .enqueue_batch(
                [envelope(4, "endpoint-4"), envelope(5, "endpoint-5")],
                &registry,
            )
            .unwrap();

        let progress = runner.recovery_progress();
        let sent = progress.completion_watermark().unwrap();
        assert_eq!(sent.sent_watermark, Some(5));
        assert_eq!(sent.received_watermark, None);
        assert_eq!(sent.processed_watermark, None);

        // Processing cannot be acknowledged before dequeueing. A later
        // sequence can be received and processed first, but the reported
        // watermarks remain at the greatest contiguous prefix.
        assert!(
            progress
                .acknowledge_processed(4, RunnerRecoveryChannel::SystemCommand)
                .is_err()
        );
        let received_later = progress
            .acknowledge_received(5, RunnerRecoveryChannel::SystemCommand)
            .unwrap();
        assert_eq!(received_later.received_watermark, None);
        let processed_later = progress
            .acknowledge_processed(5, RunnerRecoveryChannel::SystemCommand)
            .unwrap();
        assert_eq!(processed_later.processed_watermark, None);

        let received_first = progress
            .acknowledge_received(4, RunnerRecoveryChannel::SystemCommand)
            .unwrap();
        assert_eq!(received_first.received_watermark, Some(5));
        let processed_first = progress
            .acknowledge_processed(4, RunnerRecoveryChannel::SystemCommand)
            .unwrap();
        assert_eq!(processed_first.sent_watermark, Some(5));
        assert_eq!(processed_first.received_watermark, Some(5));
        assert_eq!(processed_first.processed_watermark, Some(5));
        assert!(
            progress
                .acknowledge_processed(4, RunnerRecoveryChannel::SystemCommand)
                .is_err()
        );
    }

    #[test]
    fn binding_requires_sent_identity_and_keeps_channel_correlation_explicit() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        let registry = registry();

        assert!(
            handoff
                .binding_for(4, RunnerRecoveryChannel::SystemCommand)
                .is_err()
        );
        handoff.bind_watermark("recovery-fixture", 7, 3).unwrap();
        handoff
            .enqueue(&envelope(4, "endpoint-4"), &registry)
            .unwrap();

        let binding = handoff
            .binding_for(4, RunnerRecoveryChannel::SystemCommand)
            .unwrap();
        assert_eq!(binding.recovery_id(), "recovery-fixture");
        assert_eq!(binding.checkpoint_sequence(), 7);
        assert_eq!(binding.dispatch_sequence(), 4);
        assert_eq!(binding.channel(), RunnerRecoveryChannel::SystemCommand);
        assert!(
            handoff
                .binding_for(4, RunnerRecoveryChannel::DataCommand)
                .is_err()
        );

        // A binding is not a dequeue proof. The caller must invoke these only
        // after independently correlating the untagged channel value.
        let sent = handoff.completion_watermark().expect("progress snapshot");
        assert_eq!(sent.sent_watermark, Some(4));
        assert_eq!(sent.received_watermark, None);
        assert_eq!(sent.processed_watermark, None);
        let mut channels = runner.take_channels();
        let SystemCommand::ReconnectSocket(received_command) =
            channels.system_cmd_rx.try_recv().unwrap();
        assert_eq!(received_command.endpoint, Ustr::from("endpoint-4"));
        let received = binding.acknowledge_received().unwrap();
        assert_eq!(received.received_watermark, Some(4));
        let processed = binding.acknowledge_processed().unwrap();
        assert_eq!(processed.processed_watermark, Some(4));
    }

    #[test]
    fn runner_close_recovery_handoff_invalidates_retained_handles() {
        let runner = AsyncRunner::new();
        let mut handoff = runner.recovery_handoff().unwrap();
        runner.close_recovery_handoff();
        assert!(
            handoff
                .enqueue(&envelope(1, "endpoint-1"), &registry())
                .is_err()
        );
    }
}
