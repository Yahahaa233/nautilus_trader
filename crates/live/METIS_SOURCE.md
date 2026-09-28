# Managed source and opt-in dispatch protocol

Source: NautilusTrader v2.0.0rc4, a0400251110653b6d8ae6a9b5b89c4543fa85a2d,
crates/live. Package identity and license are retained.

The optional dispatch-observer feature exposes a cloneable node-thread observer,
private process-local tokens and serializable begin/complete/abort records. A caller
provides a synchronous durable sink. Nested inputs retain parent/root sequences;
only acknowledged outer completion advances the root frontier. Errors, aborts and
sink unwind poison the run. Foreign, stale and out-of-order tokens are rejected.
Encoded inputs that are received but never processed can be persisted as explicit
`Discarded` records; they consume a sequence and invalidate completion proofs while
allowing a shutdown queue to be recorded item by item.

The original protocol-only batch preserved default live behavior and was followed
by optional node instrumentation described below; complete ingress coverage and
input codecs remain required for recovery contract section 17.

Startup reconciliation now refuses an execution client returning no mass-status
report or declaring incomplete report coverage. An absent report is not an empty, successfully reconciled account. The
existing startup error/abort paths handle this refusal before starting the trader;
disabling reconciliation remains an explicit separate configuration path. This
change does not establish account reconciliation completeness or execution
authorization. Regression: `test_startup_reconciliation_requires_complete_mass_status`.

The mass-status reconciliation result now retains known unresolved processing
reasons: unavailable execution clients, missing instruments, venue-order index
failures, failed orphan-fill materialization, and historical fills not applied.
LiveNode refuses startup reconciliation when these reasons are present, retaining
already-processed events for investigation rather than claiming rollback. An empty
reason list does not prove full account reconciliation: filtering, report coverage,
and account-level invariants remain separate requirements. Tests cover the manager
result and the node's rejection of a complete report containing an unknown instrument.

`LiveNodeHandle::startup_reconciliation` exposes a read-only, shared observation of
native startup processing, with original clock time, client count, phase and bounded
failure reason. Beginning a new startup clears the previous observation. Processed
is deliberately not named reconciled or authorized: callers must preserve the
distinction between native processing and complete account/runtime permission evidence.
The node now exposes a bounded recovery FIFO through
`LiveNode::enqueue_recovery_dispatch` and
`LiveNode::drain_recovery_dispatch`. It stages already encoded envelopes before
the first callback, drains them in FIFO order, and preserves the failed input
and unprocessed suffix. This is a recovery handoff queue, not the internal live
runner channels; a completed root is still not proof of empty runner queues,
full recovery, historical completeness or execution permission. Tokens cannot
be serialized into a cross-process recovery permit.

The optional recovery handoff in `runner_recovery` is a separate pre-start
boundary. A sealed `RunnerRecoveryCodecRegistry` must explicitly decode each
validated envelope into one of the seven concrete runner message types; a
`RunnerRecoveryHandoff` then sends the typed event to the runner's original
mpsc channel in dispatch order. It can bind one composite recovery identity,
checkpoint sequence and dispatch watermark before sending, and then requires
the envelope sequence to extend that watermark contiguously. The handle is
closed when the runner starts or its receivers are extracted. This proves
channel ownership and lifecycle fencing only. The composite recovery object
now exposes `replay_pending_into_runner`, which performs the ordered pending
inventory checks and calls this handoff; codecs remain application-owned and
the resulting receipt only proves channel enqueue. The boundary still does
not provide production codec registration, observed processing completion,
durable business idempotency, venue reconciliation or execution
authorization.

`LiveNode::restore_component_state` is an explicit idle-only handoff for the
actor/strategy callback payload produced by `Trader::collect_component_state`.
It requires exact component identity/order and leaves native cache, portfolio,
execution algorithms, queues, timers, venue reconciliation and execution
authorization to separate recovery contracts.

`LiveNode::restore_native_cache` is a separate idle-only handoff for an isolated
cache reconstructed by a caller. It rejects persistence-backed or already
populated target/source caches, pending database installation, startup state
loading and event-store replay, then replaces the shared cache contents while
preserving the node's cache handle. This does not restore cache configuration,
portfolio/algorithm internals, queue state, fresh market observations, venue
reconciliation or execution authority. A later composite checkpoint in a
verified journal tail must be loaded as a new boundary first; a caller must
discard the node on any later recovery failure.

`LiveNode::with_recovery_dispatch` is the single-input form of the same
idle-only boundary. The FIFO methods use it for each staged envelope and record
the durable `DispatchInput` begin/complete pair around the callback, stopping
the node on callback, begin, abort or completion failure. They do not decode a
Nautilus message, move data into the internal runner channels, start the node,
or grant execution permission; the caller remains responsible for the concrete
message and its durable idempotency store.

Unit test filters: `dispatch` (protocol plus node/guard tests, requires
`--features dispatch-observer`). The upstream logger regression is best run in
its own process because it installs a global test logger.
Removal requires equivalent upstream protocol and ingress wiring acceptance.

## Node instrumentation batch

`NodeDispatchObserver` now accepts an explicit input codec and can be installed
on an idle LiveNode. Main select, runner polling and delayed receivers use common
time/data/system/execution wrappers. Execution begins before pre-observation and
completes after recent-fill dedup and terminal tracking. Reconciliation has its own
source kind; final drain preserves its original raw execution semantics. RAII
records dropped guards as Cancelled/Failed, while non-dispatched inputs are Rejected.
System inputs found in the final drain are encoded as per-input `Discarded` records
when the configured codec supports them; failed encoding stops the node.

Incomplete paths emit durable `Uncovered` dispositions: startup buffering/flush,
HTTP query result application, maintenance, external ingress and shutdown lifecycle.
These paths do not claim complete replay payloads. Coverage() always reports
coverage_complete=false and queue_state_collected=false, with explicit reasons.
The source enum alone is not evidence that all inputs have codecs. Default nodes
have no observer; production composite checkpoints and replay remain unimplemented.
When a kernel event store is configured, LiveNode checks `is_halted` immediately
after startup and on the 100 ms stop-check timer; a fail-stop store requests node
shutdown and `finalize_stop` preserves the persistence error. This supervision
does not make the incomplete dispatch matrix or composite recovery restorable.

## 2026-09-13 ordered native event replay and startup fencing

`LiveNode::replay_recovery_events` decodes a complete identity-bound, contiguous
batch before mutation and processes execution/data events in one FIFO through
actual native handlers. Direct order events must be present verbatim on their
canonical order before observer Complete; a conflicting duplicate cannot be
accepted by a no-op caller verifier. The application still supplies additional
business postconditions. Only an isolated, client-free, Halted idle node is
accepted. Commands, timers, transport callbacks and derived runner queues are
outside this event-only protocol.

Native/cache and component restore, recovery callbacks, and bound/enqueued
legacy runner handoffs now block both start and run before startup side effects.
There is deliberately no release bypass. Callback panic also stops the node and
poisons the observer; no later recovery callback may run on that instance.
Full composite restoration, strategy progression while paused, independent venue
reconciliation and an authenticated execution release remain unimplemented or
unverified. Event replay tests must not be described as production recovery.

Continuation: submitted/accepted/canceled batches require canonical retention of
every child before batch Complete. Account receipts compare complete serialized
payloads because upstream AccountState equality checks identity only. A conflicting
balance with the same event ID cannot obtain a receipt. Raw ExecutionReport inputs
are rejected during whole-batch preflight until reconciliation and derived-event
recovery are implemented. Differential tests include close/reversal commissions,
realized PnL and archived position cycles across each restart boundary.
# Persistence failure containment

An owner may enable event-store failure containment only after installing a
synchronous execution submission guard. The running loop then fences submissions
permanently on a store halt and retains remediation dispatch until an explicit
shutdown. Startup still refuses an already failed store. The default remains
shutdown on persistence halt for owners that do not opt in. `persistence_degraded`
distinguishes this condition from healthy operation; normal shutdown still reports
the persistence failure. This is not trading recovery authorization.

# Execution-client attachment after isolated recovery

A completed native event frontier (including a validated empty batch) may install
one disconnected execution client through its factory while the node remains
idle, startup-blocked and risk-halted. Registration uses the native engine,
socket registry, instrument subscription and reconciliation tolerance paths.
Existing clients, incomplete/failed recovery and connected factory results are
refused; a connected result is stopped before refusal. Factory/registration
errors or unwind poison the node. Attachment never clears the recovery release
fence or connects the venue. Metis tests cover empty/nonempty replay, duplicate
attachment, factory error/panic and connected-result refusal.
