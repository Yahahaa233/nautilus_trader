//! Opt-in outer-dispatch protocol, not a live-node recovery implementation.
//! A durable sink must acknowledge records before begin/complete returns. This
//! module neither writes storage nor establishes ingress coverage by itself.
use anyhow::{Context, Result, ensure};
use nautilus_core::UUID4;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{cell::RefCell, rc::Rc};

/// All mutation-bearing ingress families; each live call site needs explicit wiring.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DispatchSource {
    Time,
    SystemEvent,
    SystemCommand,
    ExecutionEvent,
    ExecutionCommand,
    DataEvent,
    DataCommand,
    ExternalMessage,
    QueryResult,
    Maintenance,
    Lifecycle,
    Reconciliation,
    Replay,
}

/// Durable, application-provided input envelope. Payload completeness remains
/// the codec owner's responsibility; opaque callback pointers are not replay data.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DispatchInput {
    pub source: DispatchSource,
    pub phase: String,
    pub payload: Value,
    pub batch_index: Option<u64>,
}

/// Unforgeable process-local dispatch handle; clones refer to the same operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DispatchToken {
    observer: UUID4,
    input_sequence: u64,
    root_sequence: u64,
    parent_input_sequence: Option<u64>,
}
impl DispatchToken {
    #[must_use]
    pub fn input_sequence(&self) -> u64 {
        self.input_sequence
    }
    #[must_use]
    pub fn root_sequence(&self) -> u64 {
        self.root_sequence
    }
    #[must_use]
    pub fn parent_input_sequence(&self) -> Option<u64> {
        self.parent_input_sequence
    }
}

/// A process-local proof that one outermost input was durably completed.
///
/// The observer handle is kept private inside the proof.  Callers can only
/// obtain a proof from [`DispatchObserver::completion_proof`], and
/// [`Self::verify`] rechecks that the same observer is still idle and has not
/// advanced or failed since the proof was issued.  The proof is evidence for
/// a boundary only; it does not grant recovery or execution authority.
#[derive(Clone, Debug)]
pub struct DispatchCompletionProof {
    observer: DispatchObserver,
    observer_id: UUID4,
    run_id: String,
    input_sequence: u64,
    root_sequence: u64,
    current_root_only: bool,
}

impl DispatchCompletionProof {
    /// True for a current-root cut; historical gaps remain unresolved.
    #[must_use]
    pub fn current_root_only(&self) -> bool {
        self.current_root_only
    }

    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    #[must_use]
    pub fn input_sequence(&self) -> u64 {
        self.input_sequence
    }

    #[must_use]
    pub fn root_sequence(&self) -> u64 {
        self.root_sequence
    }

    /// Revalidates the proof against its original observer instance.
    ///
    /// # Errors
    /// Rejects a foreign, stale, invalidated, active or failed observer state.
    pub fn verify(&self) -> Result<()> {
        self.observer.verify_completion(self)
    }
}

/// Explicit non-success outcomes; none can advance the root completion frontier.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum DispatchAbort {
    Failed,
    Cancelled,
    Rejected,
    Discarded,
}

/// Records consumed by a synchronously acknowledged durable sink.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "record")]
pub enum DispatchRecord {
    /// A mutation-bearing path was observed without a complete replay codec.
    Uncovered { run_id: String, reason: String },
    /// An encoded input was received but deliberately discarded before native
    /// processing (for example, during a final shutdown drain).
    Discarded {
        run_id: String,
        input_sequence: u64,
        root_sequence: u64,
        input: DispatchInput,
    },
    Begin {
        run_id: String,
        input_sequence: u64,
        root_sequence: u64,
        parent_input_sequence: Option<u64>,
        input: DispatchInput,
    },
    Complete {
        run_id: String,
        input_sequence: u64,
        root_sequence: u64,
        outermost: bool,
    },
    Abort {
        run_id: String,
        input_sequence: u64,
        reason: DispatchAbort,
    },
}

type Sink = Box<dyn FnMut(&DispatchRecord) -> Result<()>>;
struct State {
    id: UUID4,
    run_id: String,
    next_input: u64,
    next_root: u64,
    completed_root: u64,
    completed_input: Option<u64>,
    proof_invalidated: bool,
    boundary_invalidated: bool,
    stack: Vec<DispatchToken>,
    failure: Option<String>,
    sink: Sink,
}

/// Cloneable node-thread handle. Clones share depth, sequence, sink and failure latch.
#[derive(Clone)]
pub struct DispatchObserver(Rc<RefCell<State>>);
impl std::fmt::Debug for DispatchObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchObserver").finish_non_exhaustive()
    }
}
impl DispatchObserver {
    /// Creates a fresh protocol instance. A previous run must not reuse its counter.
    /// # Errors
    /// Rejects an empty run identity.
    pub fn new(
        run_id: String,
        sink: impl FnMut(&DispatchRecord) -> Result<()> + 'static,
    ) -> Result<Self> {
        ensure!(!run_id.trim().is_empty(), "empty dispatch run identity");
        Ok(Self(Rc::new(RefCell::new(State {
            id: UUID4::new(),
            run_id,
            next_input: 1,
            next_root: 1,
            completed_root: 0,
            completed_input: None,
            proof_invalidated: false,
            boundary_invalidated: false,
            stack: Vec::new(),
            failure: None,
            sink: Box::new(sink),
        }))))
    }

    /// Acknowledges input before the caller may mutate business state.
    /// # Errors
    /// Rejects reentrancy, invalid phase, sequence exhaustion, or a failed sink.
    pub fn begin(&self, input: DispatchInput) -> Result<DispatchToken> {
        ensure!(!input.phase.trim().is_empty(), "empty dispatch phase");
        let mut state = self
            .0
            .try_borrow_mut()
            .context("dispatch observer reentered")?;
        state.check()?;
        let parent = state.stack.last();
        let token = DispatchToken {
            observer: state.id,
            input_sequence: state.next_input,
            root_sequence: parent.map_or(state.next_root, |p| p.root_sequence),
            parent_input_sequence: parent.map(|p| p.input_sequence),
        };
        let next_input = state
            .next_input
            .checked_add(1)
            .context("input sequence exhausted")?;
        let next_root = if parent.is_none() {
            state
                .next_root
                .checked_add(1)
                .context("root sequence exhausted")?
        } else {
            state.next_root
        };
        let record = DispatchRecord::Begin {
            run_id: state.run_id.clone(),
            input_sequence: token.input_sequence,
            root_sequence: token.root_sequence,
            parent_input_sequence: token.parent_input_sequence,
            input,
        };
        state.write(&record)?;
        state.next_input = next_input;
        state.next_root = next_root;
        state.stack.push(token.clone());
        Ok(token)
    }

    /// Acknowledges completion only after all synchronous descendants have returned.
    /// Queued commands/timers remain outside this protocol and prevent recovery claims.
    /// # Errors
    /// Rejects foreign, stale or out-of-order tokens and failed acknowledgments.
    pub fn complete(&self, token: &DispatchToken) -> Result<()> {
        let mut state = self
            .0
            .try_borrow_mut()
            .context("dispatch observer reentered")?;
        state.validate(token)?;
        let outermost = state.stack.len() == 1;
        let record = DispatchRecord::Complete {
            run_id: state.run_id.clone(),
            input_sequence: token.input_sequence,
            root_sequence: token.root_sequence,
            outermost,
        };
        state.write(&record)?;
        state.stack.pop();
        if outermost {
            state.completed_root = token.root_sequence;
            state.completed_input = Some(token.input_sequence);
            state.boundary_invalidated = false;
        }
        Ok(())
    }

    /// Records interruption and permanently poisons this run's protocol.
    /// # Errors
    /// Rejects invalid tokens, reentrancy or failed durable acknowledgment.
    pub fn abort(&self, token: &DispatchToken, reason: DispatchAbort) -> Result<()> {
        let mut state = self
            .0
            .try_borrow_mut()
            .context("dispatch observer reentered")?;
        state.validate(token)?;
        let record = DispatchRecord::Abort {
            run_id: state.run_id.clone(),
            input_sequence: token.input_sequence,
            reason,
        };
        state.failure = Some(format!("dispatch {:?} at {}", reason, token.input_sequence));
        state.write(&record)
    }

    /// Persists an explicit coverage gap; this is not a successful input dispatch.
    /// # Errors
    /// Rejects empty reasons, reentrancy or a failed durable sink.
    pub fn record_uncovered(&self, reason: String) -> Result<()> {
        ensure!(!reason.trim().is_empty(), "empty coverage reason");
        let mut state = self
            .0
            .try_borrow_mut()
            .context("dispatch observer reentered")?;
        state.check()?;
        let record = DispatchRecord::Uncovered {
            run_id: state.run_id.clone(),
            reason,
        };
        state.write(&record)?;
        state.proof_invalidated = true;
        state.boundary_invalidated = true;
        Ok(())
    }

    /// Persists an encoded input that was received but discarded before
    /// processing. Discarded inputs consume a sequence and invalidate any
    /// earlier completion proof, while allowing the remaining shutdown queue
    /// to be recorded item by item.
    ///
    /// # Errors
    /// Rejects reentrancy, active nested dispatch, sequence exhaustion or a
    /// failed durable acknowledgment.
    pub fn record_discarded(&self, input: DispatchInput) -> Result<()> {
        ensure!(!input.phase.trim().is_empty(), "empty dispatch phase");
        let mut state = self
            .0
            .try_borrow_mut()
            .context("dispatch observer reentered")?;
        state.check()?;
        ensure!(
            state.stack.is_empty(),
            "cannot discard during active dispatch"
        );
        let input_sequence = state.next_input;
        let root_sequence = state.next_root;
        let next_input = input_sequence
            .checked_add(1)
            .context("input sequence exhausted")?;
        let next_root = root_sequence
            .checked_add(1)
            .context("root sequence exhausted")?;
        let record = DispatchRecord::Discarded {
            run_id: state.run_id.clone(),
            input_sequence,
            root_sequence,
            input,
        };
        state.write(&record)?;
        state.next_input = next_input;
        state.next_root = next_root;
        state.proof_invalidated = true;
        state.boundary_invalidated = true;
        Ok(())
    }

    /// Returns only durably completed roots, not a queue-empty or recovery permit.
    /// # Errors
    /// Rejects reentrant access.
    pub fn completed_root(&self) -> Result<u64> {
        Ok(self
            .0
            .try_borrow()
            .context("dispatch observer reentered")?
            .completed_root)
    }

    /// Returns a proof for the latest completed outermost input only when the
    /// observer is idle.  An active child, later input, sink failure or abort
    /// makes the proof unavailable or invalid.
    ///
    /// # Errors
    /// Rejects reentrant access or a failed observer run.
    pub fn completion_proof(&self) -> Result<Option<DispatchCompletionProof>> {
        let state = self.0.try_borrow().context("dispatch observer reentered")?;
        state.check()?;
        if !state.stack.is_empty() {
            return Ok(None);
        }
        if state.proof_invalidated {
            return Ok(None);
        }
        Ok(state
            .completed_input
            .map(|input_sequence| DispatchCompletionProof {
                observer: self.clone(),
                observer_id: state.id,
                run_id: state.run_id.clone(),
                input_sequence,
                root_sequence: state.completed_root,
                current_root_only: false,
            }))
    }

    /// Returns evidence for only the latest actually completed root. This does
    /// not repair historical coverage or infer prior queue/callback state.
    /// A checkpoint caller must capture the complete native inventory separately.
    ///
    /// # Errors
    /// Refuses active callbacks, discarded/uncovered work after the latest root,
    /// reentrancy or a failed durable observer.
    pub fn completed_root_boundary_proof(&self) -> Result<Option<DispatchCompletionProof>> {
        let state = self.0.try_borrow().context("dispatch observer reentered")?;
        state.check()?;
        if !state.stack.is_empty() || state.boundary_invalidated {
            return Ok(None);
        }
        Ok(state
            .completed_input
            .map(|input_sequence| DispatchCompletionProof {
                observer: self.clone(),
                observer_id: state.id,
                run_id: state.run_id.clone(),
                input_sequence,
                root_sequence: state.completed_root,
                current_root_only: true,
            }))
    }

    fn verify_completion(&self, proof: &DispatchCompletionProof) -> Result<()> {
        let state = self.0.try_borrow().context("dispatch observer reentered")?;
        state.verify_completion(proof)
    }
    /// Returns current synchronous nesting depth.
    /// # Errors
    /// Rejects reentrant access.
    pub fn depth(&self) -> Result<usize> {
        Ok(self
            .0
            .try_borrow()
            .context("dispatch observer reentered")?
            .stack
            .len())
    }
}
impl State {
    fn check(&self) -> Result<()> {
        ensure!(
            self.failure.is_none(),
            "dispatch halted: {:?}",
            self.failure
        );
        Ok(())
    }
    fn validate(&self, token: &DispatchToken) -> Result<()> {
        self.check()?;
        ensure!(
            token.observer == self.id && self.stack.last() == Some(token),
            "foreign, completed or out-of-order dispatch token"
        );
        Ok(())
    }

    fn verify_completion(&self, proof: &DispatchCompletionProof) -> Result<()> {
        self.check()?;
        ensure!(
            proof.observer_id == self.id,
            "foreign dispatch completion proof"
        );
        ensure!(
            self.stack.is_empty(),
            "dispatch observer still has active inputs"
        );
        ensure!(
            self.completed_root == proof.root_sequence,
            "stale dispatch completion proof"
        );
        ensure!(
            self.completed_input == Some(proof.input_sequence),
            "dispatch input proof mismatch"
        );
        ensure!(
            !self.boundary_invalidated && (proof.current_root_only || !self.proof_invalidated),
            "dispatch completion proof invalidated"
        );
        ensure!(proof.run_id == self.run_id, "dispatch run proof mismatch");
        Ok(())
    }
    fn write(&mut self, record: &DispatchRecord) -> Result<()> {
        // A panicking sink must leave the run poisoned even if its caller catches unwind.
        let previous = self.failure.take();
        self.failure = Some("dispatch sink acknowledgment interrupted".into());
        if let Err(error) = (self.sink)(record) {
            self.failure = Some(format!("dispatch sink: {error:#}"));
            return Err(error);
        }
        self.failure = previous;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn input() -> DispatchInput {
        DispatchInput {
            source: DispatchSource::ExecutionEvent,
            phase: "running".into(),
            payload: serde_json::json!({"event":"fixture"}),
            batch_index: None,
        }
    }

    #[test]
    fn completed_root_cut_preserves_historical_gap_and_never_reuses_a_stale_boundary() {
        let observer = DispatchObserver::new("cut-only".into(), |_| Ok(())).unwrap();
        observer.record_uncovered("startup".into()).unwrap();
        assert!(observer.completed_root_boundary_proof().unwrap().is_none());
        let root = observer.begin(input()).unwrap();
        observer.complete(&root).unwrap();
        assert!(observer.completion_proof().unwrap().is_none());
        let cut = observer.completed_root_boundary_proof().unwrap().unwrap();
        assert!(cut.current_root_only());
        cut.verify().unwrap();
        observer.record_uncovered("maintenance".into()).unwrap();
        assert!(cut.verify().is_err());
        assert!(observer.completed_root_boundary_proof().unwrap().is_none());
        let next = observer.begin(input()).unwrap();
        observer.complete(&next).unwrap();
        assert!(observer.completion_proof().unwrap().is_none());
        assert!(cut.verify().is_err());
        observer
            .completed_root_boundary_proof()
            .unwrap()
            .unwrap()
            .verify()
            .unwrap();
    }

    #[test]
    fn nested_dispatch_preserves_parent_and_advances_only_completed_roots() {
        let records = Rc::new(RefCell::new(Vec::new()));
        let output = records.clone();
        let observer = DispatchObserver::new("run".into(), move |record| {
            output.borrow_mut().push(record.clone());
            Ok(())
        })
        .unwrap();
        let clone = observer.clone();
        let root = observer.begin(input()).unwrap();
        let child = clone.begin(input()).unwrap();
        assert_eq!(child.parent_input_sequence(), Some(root.input_sequence()));
        assert_eq!(child.root_sequence(), root.root_sequence());
        assert_eq!(clone.depth().unwrap(), 2);
        assert!(observer.complete(&root).is_err());
        clone.complete(&child).unwrap();
        assert_eq!(observer.completed_root().unwrap(), 0);
        observer.complete(&root).unwrap();
        assert_eq!(observer.completed_root().unwrap(), 1);
        let next = observer.begin(input()).unwrap();
        assert_eq!(next.input_sequence(), 3);
        assert_eq!(next.root_sequence(), 2);
        observer.complete(&next).unwrap();
        assert_eq!(observer.completed_root().unwrap(), 2);
        assert_eq!(records.borrow().len(), 6);
    }

    #[test]
    fn failure_before_begin_or_completion_cannot_advance_or_restart() {
        let observer =
            DispatchObserver::new("run".into(), |_| anyhow::bail!("begin failed")).unwrap();
        assert!(observer.begin(input()).is_err());
        assert_eq!(observer.depth().unwrap(), 0);
        assert_eq!(observer.completed_root().unwrap(), 0);
        assert!(observer.begin(input()).is_err());
        let observer = DispatchObserver::new("run".into(), |record| {
            if matches!(record, DispatchRecord::Complete { .. }) {
                anyhow::bail!("complete failed");
            }
            Ok(())
        })
        .unwrap();
        let root = observer.begin(input()).unwrap();
        assert!(observer.complete(&root).is_err());
        assert_eq!(observer.completed_root().unwrap(), 0);
        assert_eq!(observer.depth().unwrap(), 1);
        assert!(observer.begin(input()).is_err());
    }

    #[test]
    fn cancelled_duplicate_and_foreign_tokens_cannot_complete() {
        let observer = DispatchObserver::new("run".into(), |_| Ok(())).unwrap();
        let token = observer.begin(input()).unwrap();
        let other = DispatchObserver::new("run".into(), |_| Ok(())).unwrap();
        assert!(other.complete(&token).is_err());
        observer.complete(&token).unwrap();
        assert!(observer.complete(&token).is_err());
        let token = observer.begin(input()).unwrap();
        observer.abort(&token, DispatchAbort::Cancelled).unwrap();
        assert!(observer.complete(&token).is_err());
        assert!(observer.begin(input()).is_err());
        assert_eq!(observer.completed_root().unwrap(), 1);
    }

    #[test]
    fn panicking_sink_remains_poisoned_after_caught_unwind() {
        let observer = DispatchObserver::new("run".into(), |_| panic!("interrupted sink")).unwrap();
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer.begin(input())));
        assert!(result.is_err());
        assert_eq!(observer.completed_root().unwrap(), 0);
        assert!(observer.begin(input()).is_err());
    }

    #[test]
    fn completion_proof_is_idle_and_stale_after_next_input() {
        let observer = DispatchObserver::new("run".into(), |_| Ok(())).unwrap();
        let token = observer.begin(input()).unwrap();
        observer.complete(&token).unwrap();
        let proof = observer.completion_proof().unwrap().unwrap();
        assert_eq!(proof.run_id(), "run");
        assert_eq!(proof.input_sequence(), 1);
        assert_eq!(proof.root_sequence(), 1);
        proof.verify().unwrap();
        let next = observer.begin(input()).unwrap();
        assert!(observer.completion_proof().unwrap().is_none());
        observer.complete(&next).unwrap();
        assert!(proof.verify().is_err());
    }

    #[test]
    fn discarded_input_is_durable_and_invalidates_completion_proof() {
        let records = Rc::new(RefCell::new(Vec::new()));
        let output = records.clone();
        let observer = DispatchObserver::new("run".into(), move |record| {
            output.borrow_mut().push(record.clone());
            Ok(())
        })
        .unwrap();
        let completed = observer.begin(input()).unwrap();
        observer.complete(&completed).unwrap();
        let proof = observer.completion_proof().unwrap().unwrap();

        observer
            .record_discarded(DispatchInput {
                source: DispatchSource::SystemCommand,
                phase: "stopping".into(),
                payload: serde_json::json!({"reason":"shutdown"}),
                batch_index: Some(2),
            })
            .unwrap();

        assert!(proof.verify().is_err());
        assert!(observer.completion_proof().unwrap().is_none());
        assert!(records.borrow().iter().any(|record| matches!(
            record,
            DispatchRecord::Discarded {
                input_sequence: 2,
                root_sequence: 2,
                input,
                ..
            } if input.source == DispatchSource::SystemCommand
                && input.phase == "stopping"
        )));

        let next = observer.begin(input()).unwrap();
        assert_eq!(next.input_sequence(), 3);
        observer.complete(&next).unwrap();
        assert!(observer.completion_proof().unwrap().is_none());
    }
}
