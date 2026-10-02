//! Explicit opt-in node instrumentation. Coverage gaps remain machine-visible.
use crate::dispatch::{
    DispatchAbort, DispatchCompletionProof, DispatchInput, DispatchObserver, DispatchSource,
    DispatchToken,
};
use anyhow::{Context, Result};
use std::{any::Any, cell::RefCell, collections::BTreeSet, rc::Rc};

type Encoder = dyn Fn(DispatchSource, &str, &dyn Any) -> Result<DispatchInput>;

/// Partial live ingress instrumentation; this type cannot grant recovery permission.
#[derive(Clone)]
pub struct NodeDispatchObserver {
    observer: DispatchObserver,
    encoder: Rc<Encoder>,
    uncovered: Rc<RefCell<BTreeSet<String>>>,
    failure: Rc<RefCell<Option<String>>>,
    #[cfg(feature = "native-tail-replay")]
    native_trace: Option<nautilus_event_store::native_trace::NativeTraceRecorder>,
    #[cfg(feature = "native-tail-replay")]
    verify_read_witnesses:
        Option<Rc<dyn Fn(&[nautilus_common::recovery_trace::NativeReadWitness]) -> Result<()>>>,
    #[cfg(feature = "native-tail-replay")]
    read_witnesses: Option<
        Rc<
            dyn Fn(
                DispatchSource,
                &str,
                &dyn Any,
            ) -> Result<Vec<nautilus_common::recovery_trace::NativeReadWitness>>,
        >,
    >,
}
impl std::fmt::Debug for NodeDispatchObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDispatchObserver")
            .field("observer", &self.observer)
            .finish_non_exhaustive()
    }
}
impl NodeDispatchObserver {
    /// Uses an application codec; unsupported inputs must fail, never use Debug as replay data.
    pub fn new(
        observer: DispatchObserver,
        encoder: impl Fn(DispatchSource, &str, &dyn Any) -> Result<DispatchInput> + 'static,
    ) -> Self {
        Self {
            observer,
            encoder: Rc::new(encoder),
            failure: Rc::new(RefCell::new(None)),
            #[cfg(feature = "native-tail-replay")]
            native_trace: None,
            #[cfg(feature = "native-tail-replay")]
            verify_read_witnesses: None,
            #[cfg(feature = "native-tail-replay")]
            read_witnesses: None,
            uncovered: Rc::new(RefCell::new(
                [
                    "startup_buffering_and_flush",
                    "final_drain",
                    "http_query_results",
                    "maintenance",
                    "external_ingress_close",
                    "lifecycle",
                    "pending_queues_and_timer_callbacks",
                ]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            )),
        }
    }

    /// Returns a cloneable node-thread handle for free startup, drain and
    /// maintenance functions. Clones share the same protocol and failure
    /// latch; they do not create an independent dispatch sequence.
    #[must_use]
    pub fn handle(&self) -> Self {
        self.clone()
    }

    /// Returns unresolved coverage requirements. Empty queues are never inferred.
    /// # Errors
    /// Rejects reentrant access.
    pub fn coverage(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({"scope":"live_dispatch_instrumentation",
            "uncovered": &*self.uncovered.try_borrow().context("coverage reentered")?,
            "completed_root":self.observer.completed_root()?, "active_depth":self.observer.depth()?,
            "queue_state_collected":false, "coverage_complete":false,
            "failure": &*self.failure.try_borrow().context("failure state reentered")?,
            "execution_authorized":false}))
    }

    /// Returns the latest observer proof only at an idle node boundary. The
    /// proof remains tied to this observer instance and must be revalidated by
    /// a composite checkpoint coordinator before accepting a watermark.
    ///
    /// # Errors
    /// Rejects reentrant access or a failed observer run.
    pub fn completion_proof(&self) -> Result<Option<DispatchCompletionProof>> {
        self.observer.completion_proof()
    }

    /// Current-root cut only; unresolved historical coverage remains in `coverage`.
    /// # Errors
    /// Refuses changed, active or failed dispatch boundaries.
    pub fn completed_root_boundary_proof(&self) -> Result<Option<DispatchCompletionProof>> {
        self.observer.completed_root_boundary_proof()
    }

    /// Records a coverage gap for a path that has not yet got a replay codec.
    /// This is durable evidence of an uncovered path, never a successful
    /// dispatch or a recovery permit.
    ///
    /// # Errors
    /// Rejects an empty reason, reentrant access or a failed observer run.
    pub fn record_uncovered(&self, reason: impl Into<String>) -> Result<()> {
        let reason = reason.into();
        self.observer.record_uncovered(reason.clone())?;
        self.uncovered
            .try_borrow_mut()
            .context("coverage reentered")?
            .insert(reason);
        Ok(())
    }

    /// Encodes and persists an input that was received but discarded before
    /// native processing. This is intended for shutdown drains and other
    /// free functions that cannot construct a `NodeDispatchGuard` because no
    /// business mutation will run.
    ///
    /// # Errors
    /// Rejects an unsupported or mismatched codec result, reentrant access, or
    /// a failed observer run.
    pub fn record_discarded(
        &self,
        source: DispatchSource,
        phase: &str,
        input: &dyn Any,
    ) -> Result<()> {
        self.mark_begin()?;
        let envelope = (self.encoder)(source, phase, input)?;
        anyhow::ensure!(
            envelope.source == source && envelope.phase == phase,
            "dispatch codec source/phase mismatch"
        );
        self.observer.record_discarded(envelope)?;
        *self.failure.borrow_mut() = None;
        Ok(())
    }

    pub(super) fn uncovered(&self, reason: &str) -> Result<()> {
        self.record_uncovered(reason.to_owned())
    }
    pub(super) fn begin(
        &self,
        source: DispatchSource,
        phase: &str,
        input: &dyn Any,
    ) -> Result<NodeDispatchGuard> {
        self.mark_begin()?;
        let envelope = (self.encoder)(source, phase, input)?;
        anyhow::ensure!(
            envelope.source == source && envelope.phase == phase,
            "dispatch codec source/phase mismatch"
        );
        #[cfg(feature = "native-tail-replay")]
        if let Some(trace) = &self.native_trace {
            let source = native_source(source)?;
            let witnesses = self
                .read_witnesses
                .as_ref()
                .context("native trace source witness collector absent")?(
                envelope.source,
                phase,
                input,
            )?;
            let payload = envelope.payload.clone();
            let mut guard = self.finish_begin(envelope)?;
            let token = guard
                .token
                .as_ref()
                .context("native dispatch token absent")?;
            guard.native_trace = Some(trace.begin_native(
                token.root_sequence(),
                token.input_sequence(),
                token.parent_input_sequence(),
                source,
                phase,
                payload,
                witnesses.clone(),
            )?);
            let verify = self
                .verify_read_witnesses
                .clone()
                .context("native read witness verifier absent")?;
            guard.verify_read_witnesses = Some(Rc::new(move || verify(&witnesses)));
            return Ok(guard);
        }
        self.finish_begin(envelope)
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn encode_historical_source(
        &self,
        source: DispatchSource,
        phase: &str,
        input: &dyn Any,
    ) -> Result<DispatchInput> {
        let encoded = (self.encoder)(source, phase, input)?;
        anyhow::ensure!(
            encoded.source == source && encoded.phase == phase,
            "historical native codec source/phase mismatch"
        );
        Ok(encoded)
    }

    /// Begins a dispatch from an already encoded durable envelope. This is
    /// the entry point for startup/free functions that cannot borrow a
    /// `LiveNode` mutably while an async future is being driven.
    ///
    /// # Errors
    /// Rejects a halted observer, malformed envelope, reentrancy, or a failed
    /// durable acknowledgment before the caller may mutate business state.
    pub fn begin_input(&self, input: DispatchInput) -> Result<NodeDispatchGuard> {
        self.mark_begin()?;
        self.finish_begin(input)
    }

    fn mark_begin(&self) -> Result<()> {
        let mut failure = self
            .failure
            .try_borrow_mut()
            .context("dispatch codec reentered")?;
        anyhow::ensure!(failure.is_none(), "dispatch codec halted: {:?}", *failure);
        *failure = Some("dispatch codec incomplete".into());
        Ok(())
    }

    fn finish_begin(&self, input: DispatchInput) -> Result<NodeDispatchGuard> {
        let token = self.observer.begin(input)?;
        *self.failure.borrow_mut() = None;
        Ok(NodeDispatchGuard {
            observer: self.observer.clone(),
            token: Some(token),
            #[cfg(feature = "native-tail-replay")]
            native_trace: None,
            #[cfg(feature = "native-tail-replay")]
            verify_read_witnesses: None,
            #[cfg(feature = "native-tail-replay")]
            historical: None,
        })
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn attach_native_trace(
        &mut self,
        trace: nautilus_event_store::native_trace::NativeTraceRecorder,
        witnesses: impl Fn(
            DispatchSource,
            &str,
            &dyn Any,
        ) -> Result<Vec<nautilus_common::recovery_trace::NativeReadWitness>>
        + 'static,
        verify_witnesses: impl Fn(&[nautilus_common::recovery_trace::NativeReadWitness]) -> Result<()>
        + 'static,
    ) -> Result<()> {
        anyhow::ensure!(
            self.native_trace.is_none()
                && self.observer.depth()? == 0
                && self.observer.completed_root()? == 0,
            "native source tracing must precede actual dispatch"
        );
        nautilus_common::recovery_trace::scope::enable_ingress_receipts()?;
        self.native_trace = Some(trace);
        self.read_witnesses = Some(Rc::new(witnesses));
        self.verify_read_witnesses = Some(Rc::new(verify_witnesses));
        Ok(())
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn native_trace(
        &self,
    ) -> Option<&nautilus_event_store::native_trace::NativeTraceRecorder> {
        self.native_trace.as_ref()
    }
}

#[cfg(feature = "native-tail-replay")]
pub(super) fn source_dispatch(
    source: nautilus_common::recovery_trace::NativeInputSource,
) -> Result<DispatchSource> {
    use nautilus_common::recovery_trace::NativeInputSource as N;
    Ok(match source {
        N::Time => DispatchSource::Time,
        N::SystemEvent => DispatchSource::SystemEvent,
        N::SystemCommand => DispatchSource::SystemCommand,
        N::ExecutionEvent => DispatchSource::ExecutionEvent,
        N::TradingCommand => DispatchSource::ExecutionCommand,
        N::DataEvent => DispatchSource::DataEvent,
        N::DataCommand => DispatchSource::DataCommand,
        N::ExternalMessage => DispatchSource::ExternalMessage,
        N::QueryResult => DispatchSource::QueryResult,
        N::Maintenance => DispatchSource::Maintenance,
        N::Lifecycle => DispatchSource::Lifecycle,
        N::Reconciliation => DispatchSource::Reconciliation,
    })
}
#[cfg(feature = "native-tail-replay")]
pub(super) fn native_source(
    source: DispatchSource,
) -> Result<nautilus_common::recovery_trace::NativeInputSource> {
    use nautilus_common::recovery_trace::NativeInputSource as Native;
    Ok(match source {
        DispatchSource::Time => Native::Time,
        DispatchSource::SystemEvent => Native::SystemEvent,
        DispatchSource::SystemCommand => Native::SystemCommand,
        DispatchSource::ExecutionEvent => Native::ExecutionEvent,
        DispatchSource::ExecutionCommand => Native::TradingCommand,
        DispatchSource::DataEvent => Native::DataEvent,
        DispatchSource::DataCommand => Native::DataCommand,
        DispatchSource::ExternalMessage => Native::ExternalMessage,
        DispatchSource::QueryResult => Native::QueryResult,
        DispatchSource::Maintenance => Native::Maintenance,
        DispatchSource::Lifecycle => Native::Lifecycle,
        DispatchSource::Reconciliation => Native::Reconciliation,
        DispatchSource::Replay => {
            anyhow::bail!("caller Replay is not an original native source producer")
        }
    })
}

/// RAII guard for one acknowledged node dispatch. Dropping an unfinished guard
/// records cancellation (or failure while unwinding) and poisons the run.
#[must_use]
pub struct NodeDispatchGuard {
    observer: DispatchObserver,
    token: Option<DispatchToken>,
    #[cfg(feature = "native-tail-replay")]
    native_trace: Option<nautilus_event_store::native_trace::NativeTraceGuard>,
    #[cfg(feature = "native-tail-replay")]
    verify_read_witnesses: Option<Rc<dyn Fn() -> Result<()>>>,
    #[cfg(feature = "native-tail-replay")]
    historical: Option<nautilus_event_store::native_trace::NativeHistoricalInputGuard>,
}
impl std::fmt::Debug for NodeDispatchGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeDispatchGuard")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}
impl NodeDispatchGuard {
    /// Marks the input complete after all synchronous descendants and state
    /// updates have returned.
    pub fn complete(mut self) -> Result<()> {
        #[cfg(feature = "native-tail-replay")]
        anyhow::ensure!(
            self.native_trace.is_none() && self.historical.is_none(),
            "actual native effects required for source completion"
        );
        self.observer
            .complete(self.token.as_ref().context("missing dispatch token")?)?;
        self.token = None;
        Ok(())
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn requires_native_effects(&self) -> bool {
        self.native_trace.is_some() || self.historical.is_some()
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn complete_native(mut self, native_effects: serde_json::Value) -> Result<()> {
        if let Some(historical) = self.historical.take() {
            historical.complete(native_effects)?;
            self.observer.complete(
                self.token
                    .as_ref()
                    .context("missing replay dispatch token")?,
            )?;
            self.token = None;
            return Ok(());
        }
        let guard = self
            .native_trace
            .take()
            .context("native source guard absent")?;
        let recorder = guard.recorder_handle();
        let result = (|| {
            self.verify_read_witnesses
                .as_ref()
                .context("native read witness verifier absent")?()?;
            guard.complete(native_effects)?;
            self.observer
                .complete(self.token.as_ref().context("missing dispatch token")?)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.token = None;
                Ok(())
            }
            Err(error) => {
                recorder.fail(&format!(
                    "native source/projection completion failed: {error:#}"
                ));
                Err(error)
            }
        }
    }

    #[cfg(feature = "native-tail-replay")]
    pub(super) fn attach_historical(
        &mut self,
        guard: nautilus_event_store::native_trace::NativeHistoricalInputGuard,
    ) -> Result<()> {
        anyhow::ensure!(
            self.historical.is_none() && self.native_trace.is_none(),
            "native historical dispatch already attached"
        );
        self.historical = Some(guard);
        Ok(())
    }

    /// Marks the input as rejected and permanently poisons this run.
    pub fn rejected(self) -> Result<()> {
        self.abort(DispatchAbort::Rejected)
    }

    /// Marks an input as discarded (for example, during a final shutdown
    /// drain) and permanently poisons this run.
    pub fn discarded(self) -> Result<()> {
        self.abort(DispatchAbort::Discarded)
    }

    /// Records an explicit non-success disposition for this input.
    pub fn abort(mut self, reason: DispatchAbort) -> Result<()> {
        self.observer.abort(
            self.token.as_ref().context("missing dispatch token")?,
            reason,
        )?;
        self.token = None;
        Ok(())
    }
}
impl Drop for NodeDispatchGuard {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self.observer.abort(
                &token,
                if std::thread::panicking() {
                    DispatchAbort::Failed
                } else {
                    DispatchAbort::Cancelled
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (NodeDispatchObserver, DispatchObserver) {
        let protocol = DispatchObserver::new("test".into(), |_| Ok(())).unwrap();
        (
            NodeDispatchObserver::new(protocol.clone(), |source, phase, _| {
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload: Value::Null,
                    batch_index: None,
                })
            }),
            protocol,
        )
    }
    use serde_json::Value;
    #[test]
    fn guard_drop_cancels_and_nested_completion_stays_ordered() {
        let (observer, protocol) = fixture();
        let root = observer
            .begin(DispatchSource::Time, "running", &())
            .unwrap();
        let child = observer
            .begin(DispatchSource::ExecutionCommand, "running", &())
            .unwrap();
        child.complete().unwrap();
        assert_eq!(protocol.completed_root().unwrap(), 0);
        drop(root);
        assert!(
            observer
                .begin(DispatchSource::Time, "running", &())
                .is_err()
        );
    }
    #[test]
    fn codec_failure_does_not_enter_protocol_or_execute() {
        let protocol = DispatchObserver::new("test".into(), |_| Ok(())).unwrap();
        let observer = NodeDispatchObserver::new(protocol.clone(), |_, _, _| {
            anyhow::bail!("unsupported codec")
        });
        assert!(
            observer
                .begin(DispatchSource::Time, "running", &())
                .is_err()
        );
        assert_eq!(protocol.depth().unwrap(), 0);
        assert!(observer.coverage().unwrap()["failure"].is_string());
    }

    #[test]
    fn free_function_handle_accepts_encoded_input_and_records_disposition() {
        let (observer, protocol) = fixture();
        let handle = observer.handle();
        let guard = handle
            .begin_input(DispatchInput {
                source: DispatchSource::Maintenance,
                phase: "running".into(),
                payload: serde_json::json!({"task": "audit"}),
                batch_index: Some(3),
            })
            .unwrap();
        guard.complete().unwrap();
        assert_eq!(protocol.completed_root().unwrap(), 1);

        let guard = observer
            .begin_input(DispatchInput {
                source: DispatchSource::Lifecycle,
                phase: "stopping".into(),
                payload: serde_json::Value::Null,
                batch_index: None,
            })
            .unwrap();
        guard.discarded().unwrap();
        assert!(observer.completion_proof().is_err());
    }

    #[test]
    fn free_function_discard_uses_the_registered_codec() {
        let (observer, protocol) = fixture();
        observer
            .record_discarded(
                DispatchSource::SystemEvent,
                "stopping",
                &serde_json::json!({"socket":"closed"}),
            )
            .unwrap();
        assert_eq!(protocol.completed_root().unwrap(), 0);
        assert!(protocol.completion_proof().unwrap().is_none());
    }
}
