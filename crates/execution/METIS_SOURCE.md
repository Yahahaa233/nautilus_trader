> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

# Managed execution source

Pinned upstream identity and per-file source digests are in `../manifest.json`.

Metis adds an optional synchronous `SubmissionGuard` at the execution engine's
dispatch boundary. Submission, list submission, modification and batch modification
must pass the guard before local or external client routing. Cancellation and query
commands remain available. A rejected submission emits the existing denial event
when its order is cached. A refused modification restores the cached pending-update
state through OrderModifyRejected and publishes the existing strategy/instrument
notification. The local reason and causation ID distinguish it from a venue reply;
uncached or identity-mismatched orders are not mutated. Single and batch
modifications are never sent to a client after refusal.

The Node event-store integration installs a synchronous journal intent write and
flush. The execution engine does not itself own that persistence implementation,
authorize recovery, keep the node alive, or prove venue cancellation acceptance.
Those lifecycle and capture requirements remain separately owned by Node.

The engine also exposes a permanent submission fence for persistence-failure
containment. Removing or replacing the guard cannot clear that fence. It blocks
submission and modification while allowing cancellation and query routing.
