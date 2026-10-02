# Running checkpoint implementation record

Base: `7cc4e32d0ed272314811b47141a1ef0307979245`, HTTPS
`https://github.com/Yahahaa233/nautilus_trader.git`. Independent source checkout:
`/Users/y/.codex/worktrees/metis-running-checkpoint-fork`.

| Trigger | Pinned behavior | Necessary change / current implementation |
| --- | --- | --- |
| Checkpoint while `LiveNode::run` owns a Running node | Idle-only paused API has no access to the seven receivers extracted by the run loop | Node-owned `set_running_checkpoint_handler`, explicit cadence/request, actual completed-root proof and borrowed real receivers; collect / verify / persist / verify under one synchronous freeze |
| Connected OKX private/public/business inputs | Session tasks own hidden merge/dedup maps; pending operations are not equivalent to empty node queues | Shared actual stream state; whole-request admission leases; callback poll/output leases; retained raw FIFO prefix; actual socket request/command/outbox/session checks; replay state retained in inventory |
| Active Guardian or strategy clock timers | Paused inventory requires timer count zero; producer tasks can emit into closed runner ingress | Freeze actual registered LiveClock timer producers before runner admission; collect interval/start/stop/next/state/callback binding; retain ready ticks until release |
| Pending native timer callback | Registry unconditionally rejects TimeEvent encoding | Default rejection retained; explicit owner-bound timer codec registration requires the application's actual component/clock/timer resolver. Callback IDs are process evidence, never deserialized executable authority |
| Restored original node needs fresh account/data/Guardian observations | `ensure_recovery_start_permitted` denies starting the node; normal startup starts trader callbacks | Node-owned `set_recovery_observation_handler` connects actual clients/reconciliation while only registered observer actors receive callbacks. A same-instance/frontier frozen attestation arms private component receipts, executes the private pre-trader release, starts restored components once and checks actual-now facts before clearing the reversible phase gate |
| Guardian `on_save` reads a continuously advancing LiveClock | Two legitimate snapshots have different capture timestamps | Scoped LiveClock read views hold capture time during frozen state reads without changing AtomicTime. Actual reads during final freshness validation temporarily leave the view; all producer gates and cache/component checks remain held |
| Callback mutates cache or lifecycle during persistence | Outer Cache borrow alone does not protect independently mutable account/order/position cells | Hold each cell, engines, portfolio, clocks, trader registration, local bus and synchronous queue inventory; revalidate proof, risk/execution counters, components, manager and adapter/timer gates before and after writing |

The inventory is restricted evidence and grants no recovery or trading permission.
Unknown adapters, external message buses, execution algorithms, senderless timers,
active TestClock timers and OKX order-book recovery pipelines currently refuse.
No unsupported profile is reported complete. Production timer codec/restore,
consumer observation integration, consumer source pin migration and end-to-end acceptance remain
separate required work. No real venue submission is part of this verification.

Independent-fork scoped verification completed through the consumer's actual
`cargo_target_guard.py` global admission and `cargo_fork_command.py` with
`fork_mode=false` (not consumer source-governance acceptance). All `METIS_BUILD_*`
source injections were removed. Products remained on PSSD in
`/Volumes/My PSSD/CQS/target/trading-checkpoint-fork`.

The final combined nextest batch ran 44 tests: 44 passed, 5715 outside the scoped
filter skipped, wrapper exit 0. This includes actual Hosted Running dispatch with
an active registered native timer; durable-writer failure fencing; actual active
OKX public/business loopback WebSockets with real frames arriving during the
freeze and FIFO quote delivery only after release; callback admission; reversible
execution observation admission; and historical-gap/stale-proof rejection.
The successful Running test uses explicit zero test shutdown/post-stop delays,
not an altered production timeout. The socket instrument comes from actual HTTP
metadata and matches the actual subscription and frames.

The current-root proof explicitly reports `current_root_only=true`; historical
coverage gaps remain in the inventory and the original history-wide completion
proof still refuses them. This new cut does not certify original replay coverage.

Authoritative log and source-before/after record:
`/Volumes/My PSSD/CQS/trading-checkpoint-validation/running-tests-6/`.
Log SHA256: `c82cb1bb7c67233e77894eccf587fa63651fb9bffc8593826d903da8888a9fcc`.
Tracked diff and untracked new-source hashes were unchanged during the wrapper.
Observed peak compiler/test tree RSS was 2500880 KiB with two jobs.

Normal native observation/startup/stop lifecycle integration, supported timer and
adapter state restoration, and consumer source migration are not demonstrated by
this batch. They remain required before S3 or the original Demo scope is complete.

The next combined batch ran 67 tests: 67 passed, 5695 outside the filter skipped,
wrapper exit 0. It includes a real LiveNode with registered LiveClock receiving
actual quote input, observer state/history advancing before the freeze, private
attestation and arm, pre-trader release, exactly one restored `on_start`, and
actual-now validation before the observation phase gate opens. A second case
delays the pre-trader release past the fact deadline: final real-time validation
rejects and retains both the phase gate and permanent failure fence. No venue
client/account acceptance is claimed by these SDK lifecycle tests.

The compiled callback order is:
`set_recovery_observation_handler(observer_ids, registry, schedule, begin,
observe_round, attest, arm, before_trader_start, validate_release, fence)`.
`attest` returns an optional private application value, `arm` consumes it under
the same native freeze, and `before_trader_start` consumes the armed value before
ordinary trader startup. The sealed observation/startup boundaries expose the
actual `RunningCheckpointBoundary`; the release boundary exposes cache,
components, node identity, recovery frontier and actual current time. None is
deserializable authorization. The execution phase gate is distinct from the
irreversible failure latch and never grants a Metis submission approval.

Authoritative record:
`/Volumes/My PSSD/CQS/trading-checkpoint-validation/observation-tests-1/`.
Log SHA256: `2823e6cf7599cf9bbb0149a9a263682c2d79b6fc618d3ae786447e7c7df29997`.
All tested tracked/untracked source hashes remained unchanged during the wrapper;
observed peak tree RSS was 2533024 KiB with two jobs. Actual cross-process timer
and adapter restoration, final Stop drain/checkpoint/seal, the remaining native
mutation producer roots and consumer integration still need their own positive
and adversarial verification before S3 is complete.


The next timer/mutation batch ran 73 tests: 73 passed, 5695 outside the scoped
filter skipped, final wrapper exit 0. LiveClock restore preserves the source
next deadline even when it is now in the past, binds the actual registered
current-process default Rust callback, pauses producers, and retains source
events ahead of current-process events. Unknown explicit callback sources are
refused. The node-owned handoff distinguishes retained, queued, received and
processed; only actual callback dispatch completion marks processed.

The compiled public entry is
`restore_registered_timer_checkpoint(source, pending, watermark)` returning
`RetainedRecoveryTimerHandoff`. Source owners are `kernel` and `component:<id>`;
the exact owner set must match actual registered clocks. `pending` carries the
source owner, callback binding, FIFO ordinal and full TimeEvent headers. It is
not executable authority. The completed native recovery frontier must already
be installed. Observer clock producers resume during observation; strategy
clock producers and source pending callbacks remain retained until sealed
restored startup, then move as the actual runner prefix. Consumer lifecycle
hooks must preserve existing restored timer schedules instead of replacing
their original next deadline. These SDK tests do not prove the consumer's
original source artifact or full node timer recovery integration.

Actual startup, startup reconciliation, HTTP query completion, maintenance and
restored trader startup producers now emit `NativeMutationInput` before state
mutation. The source codec can downcast this native-only type and serialize
`canonical_payload()`. The payload includes its fixed schema, producer kind,
actual node instance and actual wall time; it is Serialize-only and excludes
full configuration/credential contents. Existing reconciliation batches still
carry their actual typed order events. No Debug/no-op fallback closes coverage.
The tests include real embedded startup roots and durable input-writer rejection
before kernel startup. Full historical mutation replay and remaining runtime
state restoration are still required; this work does not erase older gaps.

Authoritative record:
`/Volumes/My PSSD/CQS/trading-checkpoint-validation/timer-mutations-5/`.
Log SHA256: `c65f3c50abe1a3948dddd1eb41374122faffa6205d32a60729acb2b22e2ac357`.
Tracked diff and both new module hashes were unchanged during the wrapper.
Observed peak tree RSS was 2580528 KiB with two jobs. Active OKX restoration,
remaining native state/replay integration, the final Stop drain/checkpoint/seal
and consumer pin/migration are not certified by this scoped batch.


The adapter/terminal-Stop batch ran 83 tests: 83 passed, 6344 outside the scoped
filter skipped, final wrapper exit 0. This includes actual HTTP metadata and
public/business WebSocket sessions: old retained economic frames remain ahead
of new-session economic arrivals, while only current-session control ACKs can
confirm fresh subscriptions. Old login/ACK frames, configuration drift,
duplicate source bindings and repeated restore are refused. Native acquisition
and owner counts are restored into the actual DataClientAdapter registry.
The private execution dispatch deduplication/lifecycle state is restored and
verified in SDK tests; independent fresh UID/account facts remain mandatory.
The loopback data tests do not establish a live/private-account venue acceptance.

The compiled installation entry is:
`LiveNode::restore_registered_adapter_checkpoint(&adapters,
&data_client_state, &watermark) -> Result<()>`. It requires Idle, the same already
installed native recovery frontier, restored cache/components, actual registered
client identities and fresh supported adapters. It runs a typed native lifecycle
root and installs real adapter/facade state once. No JSON inventory is execution
authority. `RunningCheckpointInventory::adapters()` and `data_client_state()`
expose those source maps. `RecoveryReleaseBoundary::empty_bootstrap()` now exposes
the actual sealed native receipt, not a reconstructed readiness value.

A configured original run performs its final seven-channel drain and a new
completed native lifecycle root, then uses the same collect/persist/verify
handler regardless of interval/request cadence. `inventory.is_terminal_cut()`
identifies `terminal_completed_root_closed_admission.v1`; actual pre-cut raw or
native pending inputs remain serialized, so the terminal pending count is not
assumed zero. The successful freeze permanently closes actual adapter,
registered timer and runner admission without reopening callback producers.
Inputs arriving after that cut cannot become old-run business callbacks. Owned
transports are disconnected/joined before the kernel seal. Tests assert a real
late native input is in the final cache before checkpoint/normal seal, and an
actual active OKX terminal cut retains its raw prefix and joins both session
tasks without publishing that retained input after the cut.

A writer error or mutation during final persistence invokes the private failure
fence, prohibits finalize/dispose normal seal and retains an unsealed run. The
actual event-store adapter must implement
`KernelEventStore::retain_unsealed(&mut self, reason: &str) -> Result<()>` to latch
its real writer/session failure and suppress any implementation Drop seal. The
default refuses this contract. `NautilusKernel::prohibit_event_store_seal` latches
native failure; successful disconnect is never a substitute for final evidence.
Unknown adapters or timer implementations refuse terminal gate closure.

Authoritative record:
`/Volumes/My PSSD/CQS/trading-checkpoint-validation/adapter-stop-6/`.
Log SHA256: `6c8762f7ae3b96418968bdd2fd32bc2e8762b670840ddb44df1cef6f9dc10d2d`.
Tracked pre/post diff SHA256:
`8cd5eda8963c26ff2e7dfa651438f586984fe9972ba2745329334dceac02e3df`;
all five untracked module hashes also remained unchanged. Peak tree RSS was
2676800 KiB with two jobs. Failed earlier compile and context-regression records
are retained. These tests certify the specified independent fork slices, not
consumer governance, original Demo, a private venue account or complete S3.
Historical coverage/replay, native manager/data-pipeline state, the exact
consumer caller and final approved source migration still require actual closure.


The native-manager/DataEngine batch ran 90 tests: 90 passed, 7081 outside the
scoped filter skipped, final wrapper exit 0. This seven-crate check also includes
the actual EventStoreLifecycle writer hook, with redb evidence that neither an
explicit seal nor Drop writes RunEnded/Ended after a failed terminal boundary.
The first batch's test-enum compile failure remains recorded separately.

`LiveNode::restore_registered_engine_checkpoint(&manager, &data_engine,
source_capture_ns, &watermark) -> Result<()>` now installs real native state.
It requires Idle, Halted recovery, the same already installed cut frontier,
restored cache/components, unused native engines, and one installation. The
manager schema is `NautilusExecutionManagerCheckpoint.v2`, with exact native
configuration and `captured_at_ns`. Actual ordered inflight/retry state, fill
recency/deduplication, order/position revisions and reconciliation shapes are
restored. Real offline elapsed wall time is added to stored monotonic ages;
restart cannot refresh native recency. Duplicate, oversized, future/overflow,
changed-configuration and pending-query sources refuse restoration. The source
is recorded through the actual native lifecycle observer before mutation.
`RunningCheckpointInventory::execution_manager()` and `data_engine()` expose
these actual source projections.

DataEngine uses `native_data_engine_external_bars_no_internal_pipelines.v1`.
Every owned aggregation, book, request/join/time-range, continuous-future,
option/Greek, synthetic, buffered, deferred and feature-specific pipeline is
projected from its actual private container. Nonempty unsupported families fail
with their concrete family/count. Resident continuous-future/option helpers own
only WeakCell routing; their actual same-engine binding is verified separately,
while their mutable request/subscription maps remain fully checked. Routing,
client order, configuration, complete family set and counters are restored once
before any tail processing. Missing families or an actual live request pipeline
are rejected rather than represented as empty.

Consumer ordering is cut-only cache -> restored components -> authenticated
same-cut native frontier -> engine cut installation -> actual native typed tail
replay -> real adapter attachment/restoration -> sealed Observation. Retained
owner-bound source timers may be installed at the cut and remain unprocessed
until restored startup. Their queued/received/processed proof is separate from
the completed native event frontier. A terminal cut can verify a RunEnded-only
suffix; nonterminal cache-only tail application does not update ExecutionManager
or DataEngine and is not a complete native restoration path. Historical mutation
replay/coverage and exact consumer/venue acceptance remain explicit work.

Authoritative record:
`/Volumes/My PSSD/CQS/trading-checkpoint-validation/manager-data-engine-2/`.
Log SHA256: `996c79a7c1170c30c726f31190a704b13d2485dc98eafde3649ea1fc38a8761f`.
Tracked pre/post diff SHA256:
`cc71ea0cbc05807afa71c51f782ce3922778b6dcae67e00a28d7008fef0a9a2a`;
both new module hashes remained unchanged. Peak tree RSS was 3193712 KiB with two
jobs. This explanatory documentation was appended only after successful tests;
it is not part of the tested Rust-source diff. The batch proves the restricted
independent-fork APIs, not full S3, original Demo or a private venue grant.
