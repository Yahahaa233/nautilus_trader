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
| Restored original node needs fresh account/data/Guardian observations | `ensure_recovery_start_permitted` denies starting the node; normal startup starts trader callbacks | Observation-only startup and sealed same-instance/frontier release are still required; no unrestricted bool setter or arbitrary ready JSON is introduced |
| Callback mutates cache or lifecycle during persistence | Outer Cache borrow alone does not protect independently mutable account/order/position cells | Hold each cell, engines, portfolio, clocks, trader registration, local bus and synchronous queue inventory; revalidate proof, risk/execution counters, components, manager and adapter/timer gates before and after writing |

The inventory is restricted evidence and grants no recovery or trading permission.
Unknown adapters, external message buses, execution algorithms, senderless timers,
active TestClock timers and OKX order-book recovery pipelines currently refuse.
No unsupported profile is reported complete. Production timer codec/restore,
observation release, consumer source pin migration and end-to-end acceptance remain
separate required work. No real venue submission is part of this verification.

Source parsing / targeted rustfmt completed. Governed compile/test results are pending;
the shared node feature check owns the current build budget. Build products stay on PSSD.
