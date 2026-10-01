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
