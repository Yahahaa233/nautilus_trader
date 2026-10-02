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
}
impl<T> NativeIngressReceiver<T> {
    fn number(&mut self, mut message: NativeIngressMessage<T>) -> NativeIngressMessage<T> {
        message.receipt.channel_ordinal = self.next_ordinal.unwrap_or(0);
        self.next_ordinal = self.next_ordinal.and_then(|value| value.checked_add(1));
        if self.next_ordinal.is_none() {
            self.gate.invalidate();
        }
        message
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
        };
        (
            IngressSender {
                sender: SenderKind::Native(sender, Arc::new(trace)),
                gate: self.clone(),
            },
            NativeIngressReceiver {
                receiver,
                next_ordinal: Some(1),
                gate: self.clone(),
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
        anyhow::ensure!(!state.poisoned, "ingress invalidated");
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
            "ingress is not open and healthy"
        );
        Ok(())
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
        // History verifies the original intent before the admission mutex; it must
        // not create a second physical FIFO entry while the real runner is frozen.
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
                Ok(Some(true)) => return Ok(()),
                Ok(Some(false)) => return Err(SendError(message)),
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
                match sender.send(NativeIngressMessage { message, receipt }) {
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

/// Completion must be explicit; abandonment invalidates subsequent admission.
#[derive(Debug)]
#[must_use = "a frozen ingress boundary must be explicitly verified and finished"]
pub struct FrozenIngress {
    gate: IngressGate,
    epoch: u64,
    finished: bool,
}
impl FrozenIngress {
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
        assert_eq!(sender.clone().send(2).unwrap_err().0, 2);
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
