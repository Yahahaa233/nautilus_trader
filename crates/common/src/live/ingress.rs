//! A shared, fail-closed boundary for all live runner ingress channels.
use super::dst;
use crate::recovery_trace::{NativeIngressReceipt, NativeInputSource, scope};
use nautilus_core::{UUID4, time::duration_since_unix_epoch};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::sync::mpsc::{
    UnboundedReceiver, UnboundedSender,
    error::{SendError, TryRecvError},
};

/// The message and its actual ingress evidence remain one physical queue entry.
#[derive(Debug)]
pub struct NativeIngressMessage<T> {
    message: T,
    receipt: NativeIngressReceipt,
    historical_epoch: Option<u64>,
}
impl<T> NativeIngressMessage<T> {
    /// Returns the original message and its inseparable native receipt.
    #[must_use]
    pub fn into_parts(self) -> (T, NativeIngressReceipt) {
        (self.message, self.receipt)
    }
}

#[derive(Debug)]
struct TraceChannel {
    id: UUID4,
    source: NativeInputSource,
    anchor_wall_ns: u64,
    anchor_instant: dst::time::Instant,
    historical: Mutex<Option<HistoricalChannelBinding>>,
}

#[derive(Debug)]
struct HistoricalChannelBinding {
    channel_id: UUID4,
    epoch: u64,
}

#[derive(Debug)]
struct HistoricalSourceFrontier {
    channel_id: UUID4,
    epoch: u64,
    next_ordinal: Option<u64>,
}

#[derive(Debug)]
enum SenderKind<T> {
    Raw(UnboundedSender<T>),
    Native(UnboundedSender<NativeIngressMessage<T>>, Arc<TraceChannel>),
}

/// The actual native receiver numbers messages in physical FIFO order, including
/// staged checkpoint prefixes. Producers never hold a mutex across arbitrary wakers.
#[derive(Debug)]
pub struct NativeIngressReceiver<T> {
    receiver: UnboundedReceiver<NativeIngressMessage<T>>,
    next_ordinal: Option<u64>,
    gate: IngressGate,
    trace: Arc<TraceChannel>,
    historical: Option<HistoricalSourceFrontier>,
}
impl<T> NativeIngressReceiver<T> {
    fn number(&mut self, mut message: NativeIngressMessage<T>) -> NativeIngressMessage<T> {
        if let Some(epoch) = message.historical_epoch {
            let retention = HistoricalQueueRetention {
                gate: self.gate.clone(),
                epoch,
            };
            let valid = retention.verify().is_ok()
                && self.historical.as_ref().is_some_and(|frontier| {
                    frontier.epoch == epoch
                        && frontier.channel_id == message.receipt.channel_id
                        && message.receipt.input_source == self.trace.source
                        && message.receipt.channel_ordinal == 0
                        && frontier.next_ordinal.is_some()
                });
            if !valid {
                self.gate.fail("historical native FIFO frontier changed");
                return message;
            }
            let frontier = self.historical.as_mut().expect("verified source frontier");
            message.receipt.channel_ordinal =
                frontier.next_ordinal.expect("verified source ordinal");
            frontier.next_ordinal = frontier.next_ordinal.and_then(|value| value.checked_add(1));
            if frontier.next_ordinal.is_none() {
                self.gate.fail("historical native FIFO ordinal exhausted");
            }
            return message;
        }
        message.receipt.channel_ordinal = self.next_ordinal.unwrap_or(0);
        self.next_ordinal = self.next_ordinal.and_then(|value| value.checked_add(1));
        if self.next_ordinal.is_none() {
            self.gate.invalidate();
        }
        message
    }

    /// Binds a reader-verified source frontier to this empty frozen native channel once.
    /// The caller must prove the original channel and next ordinal from the admitted cut.
    /// This does not authorize a source, a callback, or execution.
    ///
    /// # Errors
    /// Refuses foreign or unhealthy epochs, occupied channels, and repeated bindings.
    #[doc(hidden)]
    pub fn bind_historical_source_frontier(
        &mut self,
        frozen: &FrozenIngress,
        channel_id: UUID4,
        next_ordinal: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            frozen.belongs_to(&self.gate),
            "historical FIFO gate differs"
        );
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        verify_retention_state(&state, frozen.epoch)?;
        anyhow::ensure!(
            self.receiver.is_empty() && !self.receiver.is_closed(),
            "historical FIFO is not empty and open"
        );
        anyhow::ensure!(
            self.historical.is_none() && next_ordinal > 0,
            "historical FIFO frontier is repeated or absent"
        );
        let mut binding = self
            .trace
            .historical
            .lock()
            .map_err(|_| anyhow::anyhow!("historical channel mutex poisoned"))?;
        anyhow::ensure!(binding.is_none(), "historical native channel already bound");
        *binding = Some(HistoricalChannelBinding {
            channel_id,
            epoch: frozen.epoch,
        });
        self.historical = Some(HistoricalSourceFrontier {
            channel_id,
            epoch: frozen.epoch,
            next_ordinal: Some(next_ordinal),
        });
        Ok(())
    }

    /// Advances this empty native channel for one reader-admitted independent source input.
    /// The reader must verify the exact original receipt and held FIFO before calling.
    ///
    /// # Errors
    /// Refuses derived, foreign, duplicate, discontinuous, or exhausted source receipts.
    #[doc(hidden)]
    pub fn advance_historical_source_frontier(
        &mut self,
        frozen: &FrozenIngress,
        receipt: &NativeIngressReceipt,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            frozen.belongs_to(&self.gate),
            "historical FIFO gate differs"
        );
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        verify_retention_state(&state, frozen.epoch)?;
        anyhow::ensure!(
            self.receiver.is_empty() && !self.receiver.is_closed(),
            "historical FIFO contains an unconsumed native entry"
        );
        let frontier = self
            .historical
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("historical FIFO source frontier absent"))?;
        anyhow::ensure!(
            frontier.epoch == frozen.epoch
                && frontier.channel_id == receipt.channel_id
                && receipt.input_source == self.trace.source
                && receipt.caused_by.is_none()
                && frontier.next_ordinal == Some(receipt.channel_ordinal),
            "historical independent FIFO receipt changed or is discontinuous"
        );
        let next = receipt
            .channel_ordinal
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("historical FIFO ordinal exhausted"))?;
        let mut binding = self
            .trace
            .historical
            .lock()
            .map_err(|_| anyhow::anyhow!("historical channel mutex poisoned"))?;
        let binding = binding
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("historical native channel binding absent"))?;
        anyhow::ensure!(
            binding.epoch == frozen.epoch && binding.channel_id == receipt.channel_id,
            "historical independent FIFO identity repeated or changed"
        );
        frontier.next_ordinal = Some(next);
        Ok(())
    }
    /// Returns the real number of channel-resident messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.receiver.len()
    }
    /// Returns whether the actual receiver is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }
    /// Closes future admission without dropping queued evidence.
    pub fn close(&mut self) {
        self.receiver.close();
    }
    /// Returns whether the actual channel is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }
    /// Receives and numbers the oldest physical entry.
    ///
    /// # Errors
    /// Returns the actual channel empty/disconnected error.
    pub fn try_recv(&mut self) -> Result<NativeIngressMessage<T>, TryRecvError> {
        let message = self.receiver.try_recv()?;
        Ok(self.number(message))
    }
    /// Polls the actual oldest physical entry without inventing an enqueue acknowledgment.
    pub fn poll_recv(
        &mut self,
        context: &mut Context<'_>,
    ) -> Poll<Option<NativeIngressMessage<T>>> {
        match self.receiver.poll_recv(context) {
            Poll::Ready(Some(message)) => Poll::Ready(Some(self.number(message))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
impl<T> Clone for SenderKind<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Raw(sender) => Self::Raw(sender.clone()),
            Self::Native(sender, trace) => Self::Native(sender.clone(), trace.clone()),
        }
    }
}

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    in_flight: u64,
    frozen: bool,
    snapshot_stopped: bool,
    poisoned: bool,
    first_failure: Option<String>,
    terminal: bool,
}

/// Clones share one admission boundary. No lock is held while a snapshot runs.
#[derive(Clone, Debug, Default)]
pub struct IngressGate(Arc<Mutex<State>>);

impl IngressGate {
    /// Creates an independent, open ingress boundary.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a channel whose retained sender clones all share this gate.
    #[must_use]
    pub fn channel<T>(&self) -> (IngressSender<T>, UnboundedReceiver<T>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        (
            IngressSender {
                sender: SenderKind::Raw(sender),
                gate: self.clone(),
            },
            receiver,
        )
    }

    /// Creates an owned channel that preserves actual admission time and FIFO identity.
    /// Retained sender clones share admission; the actual receiver assigns physical FIFO ordinals.
    #[must_use]
    ///
    /// # Panics
    /// Panics if the current Unix timestamp exceeds the native receipt range.
    pub fn native_channel<T>(
        &self,
        source: NativeInputSource,
    ) -> (IngressSender<T>, NativeIngressReceiver<T>) {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let trace = TraceChannel {
            id: UUID4::new(),
            source,
            anchor_wall_ns: duration_since_unix_epoch()
                .as_nanos()
                .try_into()
                .expect("current Unix time exceeds native receipt range"),
            anchor_instant: dst::time::Instant::now(),
            historical: Mutex::new(None),
        };
        let trace = Arc::new(trace);
        (
            IngressSender {
                sender: SenderKind::Native(sender, trace.clone()),
                gate: self.clone(),
            },
            NativeIngressReceiver {
                receiver,
                next_ordinal: Some(1),
                gate: self.clone(),
                trace,
                historical: None,
            },
        )
    }

    /// Returns whether both handles refer to the same admission boundary.
    #[must_use]
    pub fn same_gate(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    /// Verifies boundary health without requiring open admission.
    ///
    /// # Errors
    /// Returns an error for an invalidated boundary or poisoned mutex.
    pub fn verify(&self) -> anyhow::Result<()> {
        let state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        anyhow::ensure!(
            !state.poisoned,
            "ingress invalidated: {}",
            state.first_failure.as_deref().unwrap_or("boundary failure")
        );
        Ok(())
    }

    /// Verifies healthy, open admission in a single critical section.
    ///
    /// # Errors
    /// Returns an error if frozen, invalidated, or the mutex is poisoned.
    pub fn verify_open(&self) -> anyhow::Result<()> {
        let state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        anyhow::ensure!(
            !state.poisoned && !state.frozen && !state.terminal,
            "ingress is not open and healthy: {}",
            state.first_failure.as_deref().unwrap_or("boundary failure")
        );
        Ok(())
    }

    /// Latches the first producer failure on this same owned admission boundary.
    /// It cannot clear failure, renew a source timestamp, or create a new input.
    pub fn fail(&self, reason: &str) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.poisoned = true;
        if state.first_failure.is_none() {
            state.first_failure = Some(reason.chars().take(256).collect());
        }
    }

    /// Permanently rejects subsequent sends and invalidates any active capture.
    pub fn invalidate(&self) {
        // Preserve invalidation even if an enqueue panicked while holding the mutex.
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.poisoned = true;
    }

    /// A stop request invalidates an in-progress capture but allows ordinary
    /// shutdown queues to drain when no capture is in progress.
    pub fn invalidate_if_frozen(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.frozen {
            state.poisoned = true;
        }
    }

    /// Permanently closes snapshot admission while preserving shutdown sends.
    /// An active capture is invalidated before the caller signals a stop.
    pub fn stop_snapshot_admission(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.snapshot_stopped = true;
        if state.frozen {
            state.poisoned = true;
        }
    }

    /// Linearizes a node lifecycle atomic update against freeze and completion.
    ///
    /// The closure must perform only short atomic state operations. It must not
    /// send messages, invoke callbacks or other user code, or reenter this gate.
    /// A frozen or poisoned boundary stays invalid, but a stop update still runs.
    /// This operation does not grant execution permission.
    pub fn with_lifecycle_transition<T>(&self, update: impl FnOnce() -> T) -> T {
        let mut state = match self.0.lock() {
            Ok(state) => state,
            Err(error) => {
                let mut state = error.into_inner();
                state.poisoned = true;
                state
            }
        };
        if state.frozen {
            state.poisoned = true;
        }
        update()
    }

    /// Establishes an exclusive snapshot epoch without retaining the mutex.
    ///
    /// # Errors
    /// Rejects poisoned or already frozen boundaries, exhausted epochs, and
    /// in-flight enqueues. In-flight rejection does not invalidate admission.
    /// Nested freezes invalidate the original capture.
    pub fn freeze(&self) -> anyhow::Result<FrozenIngress> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        anyhow::ensure!(
            !state.poisoned && !state.terminal,
            "ingress invalidated or terminal"
        );
        if state.frozen {
            state.poisoned = true;
            anyhow::bail!("ingress already frozen");
        }
        anyhow::ensure!(state.in_flight == 0, "ingress enqueue is in flight");
        anyhow::ensure!(!state.snapshot_stopped, "snapshot admission stopped");
        let Some(epoch) = state.epoch.checked_add(1) else {
            state.poisoned = true;
            anyhow::bail!("ingress epoch exhausted");
        };
        state.epoch = epoch;
        state.frozen = true;
        Ok(FrozenIngress {
            gate: self.clone(),
            epoch,
            finished: false,
        })
    }
}

/// A private sender prevents retained clones from bypassing the gate.
#[derive(Debug)]
pub struct IngressSender<T> {
    sender: SenderKind<T>,
    gate: IngressGate,
}
impl<T> Clone for IngressSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            gate: self.gate.clone(),
        }
    }
}
impl<T> From<UnboundedSender<T>> for IngressSender<T> {
    /// Compatibility for independent channels. A runner must create its channels
    /// with its own gate; wrapping an external channel does not prove ownership.
    fn from(sender: UnboundedSender<T>) -> Self {
        Self {
            sender: SenderKind::Raw(sender),
            gate: IngressGate::new(),
        }
    }
}
impl<T> IngressSender<T> {
    /// Sends only through this channel's admission boundary.
    ///
    /// # Errors
    /// Returns the original message when frozen, poisoned, or disconnected.
    /// A send attempted during capture invalidates that capture.
    pub fn send(&self, message: T) -> Result<(), SendError<T>>
    where
        T: 'static,
    {
        // History verifies source bytes before admission. Only an explicit frozen
        // source binding retains the original envelope in this same physical FIFO.
        let trace_active = scope::capture_active() || crate::recovery_trace::historical::active();
        let encoded = if trace_active {
            let SenderKind::Native(_, trace) = &self.sender else {
                crate::recovery_trace::historical_failure(
                    "historical/source derived send used an unowned raw channel",
                );
                return Err(SendError(message));
            };
            let payload = match crate::recovery_trace::encode_native_ingress(trace.source, &message)
            {
                Ok(payload) => payload,
                Err(error) => {
                    crate::recovery_trace::historical_failure(&format!(
                        "native derived codec failed: {error:#}"
                    ));
                    return Err(SendError(message));
                }
            };
            match crate::recovery_trace::historical::queued(trace.source, &payload) {
                Ok(Some(crate::recovery_trace::historical::Queued::Verified(true))) => {
                    return Ok(());
                }
                Ok(Some(crate::recovery_trace::historical::Queued::Verified(false))) => {
                    return Err(SendError(message));
                }
                Ok(Some(crate::recovery_trace::historical::Queued::Retain {
                    receipt,
                    retention,
                })) => return self.retain_historical(message, *receipt, &retention),
                Ok(None) => Some(payload),
                Err(error) => {
                    crate::recovery_trace::historical_failure(&format!(
                        "native derived queue differs: {error:#}"
                    ));
                    return Err(SendError(message));
                }
            }
        } else {
            None
        };
        let mut derived = None;
        {
            let Ok(mut state) = self.gate.0.lock() else {
                return Err(SendError(message));
            };
            if state.terminal {
                return Err(SendError(message));
            }
            if state.frozen || state.poisoned {
                state.poisoned = true;
                return Err(SendError(message));
            }
            let Some(in_flight) = state.in_flight.checked_add(1) else {
                state.poisoned = true;
                return Err(SendError(message));
            };
            state.in_flight = in_flight;
        }
        // Tokio may synchronously invoke an arbitrary receiver waker. The
        // permit prevents freeze without holding a mutex across that callback.
        let permit = EnqueuePermit {
            gate: &self.gate,
            completed: false,
        };
        let result = match &self.sender {
            SenderKind::Raw(sender) => sender.send(message),
            SenderKind::Native(sender, trace) => {
                let wall_ns = u64::try_from(duration_since_unix_epoch().as_nanos());
                let elapsed_ns = u64::try_from(trace.anchor_instant.elapsed().as_nanos());
                let (Ok(wall_ns), Ok(elapsed_ns)) = (wall_ns, elapsed_ns) else {
                    self.gate.invalidate();
                    return Err(SendError(message));
                };
                let receipt = NativeIngressReceipt {
                    message_id: UUID4::new(),
                    channel_id: trace.id,
                    channel_ordinal: 0,
                    input_source: trace.source,
                    clock_anchor_wall_ns: trace.anchor_wall_ns,
                    accepted_elapsed_ns: elapsed_ns,
                    accepted_wall_ns: wall_ns,
                    caused_by: scope::current_cause(),
                };
                if let Some(payload) = encoded {
                    derived = Some(crate::recovery_trace::NativeQueuedOutput {
                        receipt: receipt.clone(),
                        payload,
                        accepted: false,
                    });
                }
                match sender.send(NativeIngressMessage {
                    message,
                    receipt,
                    historical_epoch: None,
                }) {
                    Ok(()) => Ok(()),
                    Err(e) => Err(SendError(e.0.message)),
                }
            }
        };
        if let Some(mut derived) = derived {
            derived.accepted = result.is_ok();
            if let Err(error) = scope::note_queued_output(derived) {
                self.gate.invalidate();
                crate::recovery_trace::historical_failure(&format!(
                    "native queued source receipt failed: {error:#}"
                ));
            }
        }
        permit.finish(result.is_ok());
        result
    }

    fn retain_historical(
        &self,
        message: T,
        receipt: NativeIngressReceipt,
        retention: &HistoricalQueueRetention,
    ) -> Result<(), SendError<T>> {
        let SenderKind::Native(sender, trace) = &self.sender else {
            crate::recovery_trace::historical_failure(
                "historical retention requires the native channel",
            );
            return Err(SendError(message));
        };
        let binding_matches = trace.historical.lock().is_ok_and(|binding| {
            binding.as_ref().is_some_and(|binding| {
                binding.epoch == retention.epoch
                    && binding.channel_id == receipt.channel_id
                    && receipt.input_source == trace.source
                    && receipt.channel_ordinal == 0
            })
        });
        if !binding_matches {
            crate::recovery_trace::historical_failure(
                "historical retention native source channel differs",
            );
            retention
                .gate
                .fail("historical retention native source channel differs");
            return Err(SendError(message));
        }
        let permit = match retention.acquire(&self.gate) {
            Ok(permit) => permit,
            Err(e) => {
                crate::recovery_trace::historical_failure(&format!(
                    "historical retention gate refused: {e:#}"
                ));
                retention.gate.fail("historical retention gate refused");
                return Err(SendError(message));
            }
        };
        let result = sender
            .send(NativeIngressMessage {
                message,
                receipt,
                historical_epoch: Some(retention.epoch),
            })
            .map_err(|e| SendError(e.0.message));
        permit.finish(result.is_ok());
        if result.is_err() || retention.verify().is_err() {
            crate::recovery_trace::historical_failure("historical accepted native enqueue failed");
        }
        result
    }
    /// Permanently closes this sender's original admission boundary after a
    /// producer capture failure. The owner observes the same first-fault reason.
    pub fn fail(&self, reason: &str) {
        self.gate.fail(reason);
    }

    /// Invalidates an active capture when this sender is replaced or rebound.
    /// Ordinary replacement leaves open admission usable.
    pub fn invalidate_snapshot(&self) {
        self.gate.invalidate_if_frozen();
    }

    /// Returns whether this handle belongs to the supplied boundary.
    #[must_use]
    pub fn belongs_to(&self, gate: &IngressGate) -> bool {
        self.gate.same_gate(gate)
    }
    /// Compares the underlying channel without exposing a raw sender.
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        match (&self.sender, &other.sender) {
            (SenderKind::Raw(a), SenderKind::Raw(b)) => a.same_channel(b),
            (SenderKind::Native(a, _), SenderKind::Native(b, _)) => a.same_channel(b),
            _ => false,
        }
    }
    /// Returns whether the receiver is closed; this does not prove gate health.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        match &self.sender {
            SenderKind::Raw(sender) => sender.is_closed(),
            SenderKind::Native(sender, _) => sender.is_closed(),
        }
    }
    /// Waits for receiver closure, independently of admission state.
    pub async fn closed(&self) {
        match &self.sender {
            SenderKind::Raw(sender) => sender.closed().await,
            SenderKind::Native(sender, _) => sender.closed().await,
        }
    }
}

struct EnqueuePermit<'a> {
    gate: &'a IngressGate,
    completed: bool,
}
impl EnqueuePermit<'_> {
    fn finish(mut self, accepted: bool) {
        let mut state = self
            .gate
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight -= 1;
        if !accepted {
            state.poisoned = true;
        }
        self.completed = true;
    }
}
impl Drop for EnqueuePermit<'_> {
    fn drop(&mut self) {
        if !self.completed {
            let mut state = self
                .gate
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_flight -= 1;
            state.poisoned = true;
        }
    }
}

fn verify_retention_state(state: &State, epoch: u64) -> anyhow::Result<()> {
    anyhow::ensure!(
        !state.poisoned
            && !state.terminal
            && !state.snapshot_stopped
            && state.frozen
            && state.in_flight == 0
            && state.epoch == epoch,
        "historical native queue epoch is not frozen and healthy"
    );
    Ok(())
}

/// Private proof of the actual owner's frozen epoch, never serialized admission.
#[derive(Clone, Debug)]
pub(crate) struct HistoricalQueueRetention {
    gate: IngressGate,
    epoch: u64,
}
impl HistoricalQueueRetention {
    pub(crate) fn fail(&self, reason: &str) {
        self.gate.fail(reason);
    }
    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        verify_retention_state(&state, self.epoch)
    }
    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        self.gate.same_gate(&other.gate) && self.epoch == other.epoch
    }
    fn acquire<'a>(&self, gate: &'a IngressGate) -> anyhow::Result<EnqueuePermit<'a>> {
        anyhow::ensure!(self.gate.same_gate(gate), "historical sender gate differs");
        let mut state = gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        verify_retention_state(&state, self.epoch)?;
        state.in_flight = state
            .in_flight
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("historical enqueue count exhausted"))?;
        Ok(EnqueuePermit {
            gate,
            completed: false,
        })
    }
}

/// Completion must be explicit; abandonment invalidates subsequent admission.
#[derive(Debug)]
#[must_use = "a frozen ingress boundary must be explicitly verified and finished"]
pub struct FrozenIngress {
    gate: IngressGate,
    epoch: u64,
    finished: bool,
}
impl FrozenIngress {
    pub(crate) fn historical_queue_retention(&self) -> anyhow::Result<HistoricalQueueRetention> {
        let retention = HistoricalQueueRetention {
            gate: self.gate.clone(),
            epoch: self.epoch,
        };
        retention.verify()?;
        Ok(retention)
    }
    /// Returns whether this handle belongs to the supplied boundary.
    #[must_use]
    pub fn belongs_to(&self, gate: &IngressGate) -> bool {
        self.gate.same_gate(gate)
    }
    /// Revalidates this exact epoch and absence of rejected sends.
    ///
    /// # Errors
    /// Rejects invalidation, epoch mismatch, or a poisoned mutex.
    pub fn verify(&self) -> anyhow::Result<()> {
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        anyhow::ensure!(
            !state.poisoned && state.frozen && state.in_flight == 0 && state.epoch == self.epoch,
            "frozen ingress boundary invalidated"
        );
        Ok(())
    }
    /// Ends a successful terminal capture without reopening any retained sender.
    /// # Errors
    /// Refuses rejected sends, changed epochs and in-flight enqueues.
    pub fn finish_terminal(mut self) -> anyhow::Result<()> {
        self.verify()?;
        let mut state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
        anyhow::ensure!(
            !state.poisoned && state.frozen && state.in_flight == 0 && state.epoch == self.epoch,
            "terminal ingress boundary changed"
        );
        state.terminal = true;
        state.frozen = false;
        state.snapshot_stopped = true;
        self.finished = true;
        Ok(())
    }
    /// Revalidates and releases the boundary atomically.
    ///
    /// # Errors
    /// Rejects any invalidated boundary; failed completion remains fail-closed.
    pub fn finish(mut self) -> anyhow::Result<()> {
        {
            let mut state = self
                .gate
                .0
                .lock()
                .map_err(|_| anyhow::anyhow!("ingress mutex poisoned"))?;
            anyhow::ensure!(
                !state.poisoned
                    && state.frozen
                    && state.in_flight == 0
                    && state.epoch == self.epoch,
                "frozen ingress boundary invalidated"
            );
            state.frozen = false;
        }
        self.finished = true;
        Ok(())
    }
}
impl Drop for FrozenIngress {
    fn drop(&mut self) {
        if !self.finished {
            self.gate.invalidate();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_terminal_ingress_retains_prefix_and_rejects_reopening() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        sender.send(17).unwrap();
        let frozen = gate.freeze().unwrap();
        frozen.finish_terminal().unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 17);
        assert!(sender.send(18).is_err());
        assert!(gate.verify_open().is_err());
        assert!(gate.freeze().is_err());
        gate.verify().unwrap();
    }

    #[test]
    fn clone_preserves_gate_without_clone_message_bound() {
        struct Message(u8);
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let cloned = sender.clone();
        assert!(cloned.belongs_to(&gate));
        assert!(sender.same_channel(&cloned));
        cloned.send(Message(7)).ok().unwrap();
        assert_eq!(receiver.try_recv().unwrap().0, 7);
        let frozen = gate.freeze().unwrap();
        frozen.verify().unwrap();
        assert!(gate.verify_open().is_err());
        frozen.finish().unwrap();
        gate.verify_open().unwrap();
        sender.send(Message(8)).ok().unwrap();
        assert_eq!(receiver.try_recv().unwrap().0, 8);
    }
    #[test]
    fn rejected_reentrant_send_retains_message_and_poisons_boundary() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        sender.send(1).unwrap();
        let frozen = gate.freeze().unwrap();
        let cloned_sender = sender.clone();
        assert_eq!(cloned_sender.send(2).unwrap_err().0, 2);
        assert!(frozen.verify().is_err());
        assert!(frozen.finish().is_err());
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert!(receiver.try_recv().is_err());
        assert_eq!(sender.send(3).unwrap_err().0, 3);
    }
    #[test]
    fn abandoned_and_nested_freezes_poison() {
        let gate = IngressGate::new();
        drop(gate.freeze().unwrap());
        assert!(gate.verify().is_err());
        let gate = IngressGate::new();
        let frozen = gate.freeze().unwrap();
        assert!(gate.freeze().is_err());
        assert!(frozen.finish().is_err());
    }
    #[test]
    fn cross_thread_send_race_has_no_unaccounted_accepted_message() {
        for _ in 0..32 {
            let gate = IngressGate::new();
            let (sender, mut receiver) = gate.channel();
            let barrier = Arc::new(std::sync::Barrier::new(2));
            let other = barrier.clone();
            let worker = std::thread::spawn(move || {
                other.wait();
                sender.send(42)
            });
            barrier.wait();
            let frozen = gate.freeze();
            let sent = worker.join().unwrap();
            if frozen.is_err() {
                sent.unwrap();
                assert_eq!(receiver.try_recv().unwrap(), 42);
                gate.freeze().unwrap().finish().unwrap();
                continue;
            }
            let frozen = frozen.unwrap();
            match sent {
                Ok(()) => {
                    assert_eq!(receiver.try_recv().unwrap(), 42);
                    frozen.finish().unwrap();
                }
                Err(error) => {
                    assert_eq!(error.0, 42);
                    assert!(receiver.try_recv().is_err());
                    assert!(frozen.finish().is_err());
                }
            }
        }
    }
    #[test]
    fn stop_invalidation_preserves_normal_shutdown_but_rejects_frozen_capture() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        gate.invalidate_if_frozen();
        sender.send(1).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 1);
        let frozen = gate.freeze().unwrap();
        gate.invalidate_if_frozen();
        assert!(frozen.verify().is_err());
        assert!(frozen.finish().is_err());
    }

    #[test]
    fn mutex_panic_refuses_admission_and_capture() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let inner = gate.clone();
        assert!(
            std::thread::spawn(move || {
                let _lock = inner.0.lock().unwrap();
                panic!("injected ingress failure");
            })
            .join()
            .is_err()
        );
        assert_eq!(sender.send(4).unwrap_err().0, 4);
        assert!(receiver.try_recv().is_err());
        assert!(gate.freeze().is_err());
        gate.invalidate();
        assert!(gate.verify().is_err());
    }

    #[test]
    fn synchronous_waker_can_reenter_send_without_deadlock() {
        struct ReentrantWake(IngressSender<u8>);
        impl std::task::Wake for ReentrantWake {
            fn wake(self: Arc<Self>) {
                self.0.send(2).unwrap();
            }
        }
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let waker = std::task::Waker::from(Arc::new(ReentrantWake(sender.clone())));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(receiver.poll_recv(&mut context).is_pending());
        sender.send(1).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert_eq!(receiver.try_recv().unwrap(), 2);
        gate.freeze().unwrap().finish().unwrap();
    }

    #[test]
    fn freeze_refuses_until_enqueue_and_synchronous_wake_complete() {
        struct BlockingWake {
            entered: std::sync::mpsc::SyncSender<()>,
            resume: Mutex<std::sync::mpsc::Receiver<()>>,
        }
        impl std::task::Wake for BlockingWake {
            fn wake(self: Arc<Self>) {
                self.entered.send(()).unwrap();
                self.resume.lock().unwrap().recv().unwrap();
            }
        }
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(1);
        let waker = std::task::Waker::from(Arc::new(BlockingWake {
            entered: entered_tx,
            resume: Mutex::new(resume_rx),
        }));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(receiver.poll_recv(&mut context).is_pending());
        let worker = std::thread::spawn(move || sender.send(42));
        entered_rx.recv().unwrap();
        assert!(gate.freeze().is_err());
        gate.verify_open().unwrap();
        resume_tx.send(()).unwrap();
        worker.join().unwrap().unwrap();
        let frozen = gate.freeze().unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 42);
        frozen.finish().unwrap();
    }

    #[test]
    fn producer_first_failure_closes_original_gate_but_legitimate_freeze_is_healthy() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let frozen = gate.freeze().unwrap();
        gate.verify().unwrap();
        assert!(gate.verify_open().is_err());
        frozen.finish().unwrap();
        sender.send(1).unwrap();
        sender.fail("actual original ACK persistence failed");
        let first = gate.verify().unwrap_err().to_string();
        sender.fail("later callback symptom");
        assert_eq!(gate.verify().unwrap_err().to_string(), first);
        assert!(first.contains("actual original ACK persistence failed"));
        assert!(sender.send(2).is_err());
        assert!(gate.freeze().is_err());
        assert_eq!(receiver.try_recv().unwrap(), 1);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn panicking_receiver_waker_invalidates_admitted_send() {
        struct PanicWake;
        impl std::task::Wake for PanicWake {
            fn wake(self: Arc<Self>) {
                panic!("injected receiver waker panic");
            }
        }
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        let waker = std::task::Waker::from(Arc::new(PanicWake));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(receiver.poll_recv(&mut context).is_pending());
        assert!(std::thread::spawn(move || sender.send(42)).join().is_err());
        assert!(gate.verify_open().is_err());
        assert!(gate.freeze().is_err());
        assert_eq!(receiver.try_recv().unwrap(), 42);
    }

    #[test]
    fn stopped_snapshot_admission_preserves_shutdown_sends() {
        let gate = IngressGate::new();
        let (sender, mut receiver) = gate.channel();
        gate.stop_snapshot_admission();
        assert!(gate.freeze().is_err());
        gate.verify_open().unwrap();
        sender.send(1).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 1);
        let gate = IngressGate::new();
        let frozen = gate.freeze().unwrap();
        gate.stop_snapshot_admission();
        assert!(frozen.finish().is_err());
    }

    #[test]
    fn lifecycle_atomic_updates_invalidate_only_active_capture() {
        use std::sync::atomic::{AtomicU8, Ordering};
        let gate = IngressGate::new();
        let state = AtomicU8::new(0);
        assert_eq!(
            gate.with_lifecycle_transition(|| state.fetch_or(1, Ordering::AcqRel)),
            0
        );
        assert_eq!(state.load(Ordering::Acquire), 1);
        gate.verify_open().unwrap();
        let frozen = gate.freeze().unwrap();
        assert_eq!(
            gate.with_lifecycle_transition(|| state.fetch_or(2, Ordering::AcqRel)),
            1
        );
        assert_eq!(state.load(Ordering::Acquire), 3);
        assert!(frozen.verify().is_err());
        assert!(frozen.finish().is_err());
        gate.with_lifecycle_transition(|| state.fetch_or(4, Ordering::AcqRel));
        assert_eq!(state.load(Ordering::Acquire), 7);
        assert!(gate.verify().is_err());
    }

    #[test]
    fn lifecycle_stop_still_updates_after_mutex_poison() {
        use std::sync::atomic::{AtomicU8, Ordering};
        let gate = IngressGate::new();
        let state = AtomicU8::new(0);
        let other = gate.clone();
        assert!(
            std::thread::spawn(move || {
                let _lock = other.0.lock().unwrap();
                panic!("injected mutex failure");
            })
            .join()
            .is_err()
        );
        gate.with_lifecycle_transition(|| state.fetch_or(1, Ordering::AcqRel));
        assert_eq!(state.load(Ordering::Acquire), 1);
        assert!(gate.verify().is_err());
    }

    #[test]
    fn explicit_invalidation_and_closed_receiver_fail_closed() {
        let gate = IngressGate::new();
        let (sender, receiver) = gate.channel::<u8>();
        assert!(!sender.belongs_to(&IngressGate::new()));
        drop(receiver);
        assert!(sender.is_closed());
        assert_eq!(sender.send(9).unwrap_err().0, 9);
        assert!(gate.freeze().is_err());
        let gate = IngressGate::new();
        let frozen = gate.freeze().unwrap();
        gate.invalidate();
        assert!(frozen.finish().is_err());
    }
}

#[cfg(test)]
mod historical_tests {
    use std::rc::Rc;

    use serde_json::json;

    use super::*;
    use crate::{
        actor::{PublicationScope, callbacks_pending, capture_channel_contexts, clear_callbacks},
        live::{dispatch::DispatchMessage, sender::EventSender},
        recovery_trace::{
            NativeProcessingReceipt, NativeQueuedOutput, NativeTraceRecord, NativeTraceSource,
            historical::{HistoricalInputScope, bind_current_native_queue_retention},
            scope::NativeCaptureScope,
            set_native_ingress_encoder,
        },
    };

    fn install_codec() {
        set_native_ingress_encoder(|source, message| {
            anyhow::ensure!(
                source == NativeInputSource::SystemEvent,
                "unsupported fixture source"
            );
            let value = message
                .downcast_ref::<DispatchMessage<u64>>()
                .map(|message| *message.message())
                .or_else(|| message.downcast_ref::<u64>().copied())
                .ok_or_else(|| anyhow::anyhow!("unsupported fixture payload"))?;
            Ok(json!(value))
        });
    }

    fn original_outputs() -> (
        NativeTraceRecord,
        Vec<NativeQueuedOutput>,
        Vec<NativeIngressReceipt>,
    ) {
        install_codec();
        clear_callbacks().unwrap();
        let source = NativeTraceSource {
            schema_version: 1,
            node_instance: UUID4::new(),
            process_incarnation: UUID4::new(),
            journal_run: "actual-native-history-fixture".into(),
            logical_run: "actual-native-history-fixture".into(),
            configuration_digest: "fixture-config".into(),
            codec_profile: "fixture-native.v1".into(),
            registered_profile_digest: "fixture-roster".into(),
        };
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let capture =
            NativeCaptureScope::enter(&source, 11, 12, None, dst::time::Instant::now()).unwrap();
        {
            let _publication = PublicationScope::enter();
            sender.send(10).unwrap();
            sender.send(11).unwrap();
        }
        let outputs = capture.queued_outputs().unwrap();
        assert_eq!(outputs.len(), 2);
        assert!(
            outputs
                .iter()
                .all(|output| output.accepted && output.receipt.channel_ordinal == 0)
        );
        let originals: Vec<_> = (0..2).map(|_| receiver.try_recv().unwrap()).collect();
        let ancestry = capture_channel_contexts(
            &originals
                .iter()
                .map(|message| message.message.checkpoint_context())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(ancestry.entry_chains, [Some(0), Some(0)]);
        let receipts = originals
            .into_iter()
            .map(|message| message.into_parts().1)
            .collect();
        capture.finish().unwrap();
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();
        let begin = NativeTraceRecord::Begin {
            source,
            root_sequence: 11,
            input_sequence: 12,
            stack_parent: None,
            input_source: NativeInputSource::Maintenance,
            phase: "original-running".into(),
            receipt: NativeProcessingReceipt {
                wall_ns: 100,
                process_elapsed_ns: 100,
                ingress: None,
            },
            payload: json!({"actual_source_fixture": true}),
            read_witnesses: Vec::new(),
        };
        (begin, outputs, receipts)
    }

    fn enter(begin: NativeTraceRecord, outputs: Vec<NativeQueuedOutput>) -> HistoricalInputScope {
        HistoricalInputScope::enter_with_outputs_at(
            Rc::new(()),
            begin,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            None,
            outputs,
        )
        .unwrap()
    }

    #[rstest::rstest]
    fn historical_native_retention_keeps_actual_shared_context_receipts_and_target_identity() {
        let (begin, outputs, original_receipts) = original_outputs();
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, original_receipts[0].channel_id, 1)
            .unwrap();
        assert!(
            receiver
                .bind_historical_source_frontier(&frozen, original_receipts[0].channel_id, 1)
                .is_err()
        );
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        bind_current_native_queue_retention(&frozen).unwrap();
        {
            let _publication = PublicationScope::enter();
            sender.send(10).unwrap();
            sender.send(11).unwrap();
        }
        assert_eq!(receiver.len(), 2);
        history.finish().unwrap();
        let retained: Vec<_> = (0..2).map(|_| receiver.try_recv().unwrap()).collect();
        let ancestry = capture_channel_contexts(
            &retained
                .iter()
                .map(|message| message.message.checkpoint_context())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(ancestry.entry_chains, [Some(0), Some(0)]);
        assert_eq!(ancestry.chain_delivered, [0]);
        assert_eq!(
            retained
                .iter()
                .map(|message| message.receipt.clone())
                .collect::<Vec<_>>(),
            original_receipts
        );
        assert_eq!(
            retained
                .iter()
                .map(|message| *message.message.message())
                .collect::<Vec<_>>(),
            [10, 11]
        );
        drop(retained);
        assert!(!callbacks_pending());
        frozen.verify().unwrap();
        frozen.finish().unwrap();
        sender.send(12).unwrap();
        let (message, receipt) = receiver.try_recv().unwrap().into_parts();
        assert!(!message.is_rooted());
        assert_eq!(receipt.channel_ordinal, 1);
        assert_ne!(receipt.channel_id, original_receipts[0].channel_id);
        assert!(receipt.caused_by.is_none());
        drop(message);
        clear_callbacks().unwrap();
    }

    #[rstest::rstest]
    fn historical_unbound_scope_preserves_verification_only_and_refuses_late_binding() {
        let (begin, outputs, _) = original_outputs();
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        let history = enter(begin, outputs);
        sender.send(10).unwrap();
        assert!(history.bind_native_queue_retention(&frozen).is_err());
        sender.send(11).unwrap();
        history.finish().unwrap();
        assert!(receiver.try_recv().is_err());
        frozen.finish().unwrap();
    }

    #[rstest::rstest]
    #[case("raw")]
    #[case("foreign_gate")]
    #[case("channel")]
    #[case("payload")]
    #[case("cause")]
    #[case("codec")]
    #[case("closed")]
    #[case("failed")]
    #[case("stale_epoch")]
    fn historical_retention_refuses_unowned_changed_failed_and_stale_admission(#[case] mode: &str) {
        let (begin, mut outputs, originals) = original_outputs();
        outputs.truncate(1);
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, originals[0].channel_id, 1)
            .unwrap();
        if mode == "cause" {
            outputs[0].receipt.caused_by = None;
        }
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        let (sender, raw_receiver, foreign_receiver, foreign_frozen) = if mode == "raw" {
            let (raw, rx) = tokio::sync::mpsc::unbounded_channel::<DispatchMessage<u64>>();
            (EventSender::<u64>::new(raw), Some(rx), None, None)
        } else if mode == "foreign_gate" {
            let other = IngressGate::new();
            let (other_native, mut rx) =
                other.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
            let other_frozen = other.freeze().unwrap();
            rx.bind_historical_source_frontier(&other_frozen, originals[0].channel_id, 1)
                .unwrap();
            (
                EventSender::<u64>::new(other_native),
                None,
                Some(rx),
                Some(other_frozen),
            )
        } else if mode == "channel" {
            let (other_native, rx) =
                gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
            (EventSender::<u64>::new(other_native), None, Some(rx), None)
        } else {
            (EventSender::<u64>::new(native), None, None, None)
        };
        if mode == "closed" {
            receiver.close();
        }
        if mode == "failed" {
            gate.fail("original producer failure");
        }
        let frozen = if mode == "stale_epoch" {
            frozen.finish().unwrap();
            Some(gate.freeze().unwrap())
        } else {
            Some(frozen)
        };
        if mode == "codec" {
            set_native_ingress_encoder(|_, _| {
                anyhow::bail!("unsupported historical fixture codec")
            });
        }
        let value = if mode == "payload" { 99 } else { 10 };
        let error = sender.send(value).unwrap_err();
        assert_eq!(*error.0.message(), value);
        drop(error);
        assert!(receiver.try_recv().is_err());
        assert!(history.finish().is_err());
        assert!(gate.verify().is_err());
        if mode == "failed" {
            assert!(
                gate.verify()
                    .unwrap_err()
                    .to_string()
                    .contains("original producer failure")
            );
        }
        drop(raw_receiver);
        drop(foreign_receiver);
        drop(foreign_frozen);
        drop(frozen);
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();
    }

    #[rstest::rstest]
    fn historical_independent_frontier_advances_once_and_cannot_cover_resident_derived_entry() {
        let (begin, outputs, originals) = original_outputs();
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, originals[0].channel_id, 4)
            .unwrap();
        let mut independent = originals[0].clone();
        independent.channel_ordinal = 4;
        independent.caused_by = None;
        let other_gate = IngressGate::new();
        let other_frozen = other_gate.freeze().unwrap();
        assert!(
            receiver
                .advance_historical_source_frontier(&other_frozen, &independent)
                .is_err()
        );
        other_frozen.finish().unwrap();
        let mut foreign = independent.clone();
        foreign.channel_id = UUID4::new();
        assert!(
            receiver
                .advance_historical_source_frontier(&frozen, &foreign)
                .is_err()
        );
        let mut gap = independent.clone();
        gap.channel_ordinal = 5;
        assert!(
            receiver
                .advance_historical_source_frontier(&frozen, &gap)
                .is_err()
        );
        let mut derived = independent.clone();
        derived.caused_by = originals[0].caused_by.clone();
        assert!(
            receiver
                .advance_historical_source_frontier(&frozen, &derived)
                .is_err()
        );
        receiver
            .advance_historical_source_frontier(&frozen, &independent)
            .unwrap();
        assert!(
            receiver
                .advance_historical_source_frontier(&frozen, &independent)
                .is_err()
        );
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        sender.send(10).unwrap();
        independent.channel_ordinal = 5;
        assert!(
            receiver
                .advance_historical_source_frontier(&frozen, &independent)
                .is_err()
        );
        let first = receiver.try_recv().unwrap();
        assert_eq!(first.receipt.channel_ordinal, 5);
        sender.send(11).unwrap();
        assert_eq!(receiver.try_recv().unwrap().receipt.channel_ordinal, 6);
        history.finish().unwrap();
        drop(first);
        frozen.finish().unwrap();
        clear_callbacks().unwrap();
    }

    #[rstest::rstest]
    fn historical_original_rejected_output_is_not_retained_or_successfully_rewritten() {
        let (begin, mut outputs, originals) = original_outputs();
        outputs.truncate(1);
        outputs[0].accepted = false;
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, originals[0].channel_id, 1)
            .unwrap();
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        assert_eq!(*sender.send(10).unwrap_err().0.message(), 10);
        history.finish().unwrap();
        assert!(receiver.try_recv().is_err());
        frozen.finish().unwrap();
    }

    #[rstest::rstest]
    fn historical_frontier_binding_refuses_occupied_foreign_and_zero_or_terminal_boundaries() {
        let gate = IngressGate::new();
        let (sender, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        sender.send(DispatchMessage::from(10)).unwrap();
        let frozen = gate.freeze().unwrap();
        assert!(
            receiver
                .bind_historical_source_frontier(&frozen, UUID4::new(), 1)
                .is_err()
        );
        drop(receiver.try_recv().unwrap());
        let other = IngressGate::new();
        let foreign = other.freeze().unwrap();
        assert!(
            receiver
                .bind_historical_source_frontier(&foreign, UUID4::new(), 1)
                .is_err()
        );
        foreign.finish().unwrap();
        assert!(
            receiver
                .bind_historical_source_frontier(&frozen, UUID4::new(), 0)
                .is_err()
        );
        let epoch = frozen.epoch;
        frozen.finish_terminal().unwrap();
        let stale = HistoricalQueueRetention { gate, epoch };
        assert!(stale.verify().is_err());
        clear_callbacks().unwrap();
    }

    #[rstest::rstest]
    fn historical_retention_waker_observes_permit_without_mutex_or_inflight_rebinding() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct InspectWake {
            gate: IngressGate,
            epoch: u64,
            observed: Arc<AtomicBool>,
        }
        impl std::task::Wake for InspectWake {
            fn wake(self: Arc<Self>) {
                let state = self.gate.0.lock().unwrap();
                assert_eq!(state.in_flight, 1);
                assert!(state.frozen && !state.poisoned);
                drop(state);
                assert!(
                    HistoricalQueueRetention {
                        gate: self.gate.clone(),
                        epoch: self.epoch
                    }
                    .verify()
                    .is_err()
                );
                self.observed.store(true, Ordering::Release);
            }
        }
        let (begin, mut outputs, originals) = original_outputs();
        outputs.truncate(1);
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, originals[0].channel_id, 1)
            .unwrap();
        let observed = Arc::new(AtomicBool::new(false));
        let waker = std::task::Waker::from(Arc::new(InspectWake {
            gate,
            epoch: frozen.epoch,
            observed: observed.clone(),
        }));
        let mut context = std::task::Context::from_waker(&waker);
        assert!(receiver.poll_recv(&mut context).is_pending());
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        sender.send(10).unwrap();
        assert!(observed.load(Ordering::Acquire));
        history.finish().unwrap();
        assert_eq!(receiver.try_recv().unwrap().receipt, originals[0]);
        frozen.verify().unwrap();
        frozen.finish().unwrap();
        clear_callbacks().unwrap();
    }

    #[rstest::rstest]
    fn historical_incomplete_scope_fences_gate_without_dropping_accepted_native_evidence() {
        let (begin, outputs, originals) = original_outputs();
        let accepted = outputs[0].receipt.clone();
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        let frozen = gate.freeze().unwrap();
        receiver
            .bind_historical_source_frontier(&frozen, originals[0].channel_id, 1)
            .unwrap();
        let history = enter(begin, outputs);
        history.bind_native_queue_retention(&frozen).unwrap();
        {
            let _publication = PublicationScope::enter();
            sender.send(10).unwrap();
        }
        assert_eq!(receiver.len(), 1);
        assert!(history.finish().is_err());
        assert!(gate.verify().is_err());
        let (message, receipt) = receiver.try_recv().unwrap().into_parts();
        assert_eq!(receipt, accepted);
        assert!(message.is_rooted());
        assert_eq!(*message.message(), 10);
        assert!(frozen.finish().is_err());
        drop(message);
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();
    }
}
