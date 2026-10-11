# nautilus-live

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-live)](https://docs.rs/nautilus-live/latest/nautilus_live/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-live.svg)](https://crates.io/crates/nautilus-live)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

Live system node for [NautilusTrader](https://nautilustrader.io).

The `nautilus-live` crate provides high-level abstractions and infrastructure for running live trading
systems, including data streaming, execution management, and system lifecycle handling.
It builds on top of the system kernel to provide simplified interfaces for live deployment:

- `LiveNode` High-level abstraction for live system nodes.
- `LiveNodeConfig` Configuration for live node deployment.
- `AsyncRunner` for managing system real-time data flow.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Feature flags

This crate provides feature flags to control source code inclusion during compilation:

- `defi`: Enables DeFi (Decentralized Finance) support.
- `examples`: Enables example strategies and testkit support for live nodes.
- `extension-module`: Builds as a Python extension module.
- `fuzz`: Provides shared libFuzzer integration for adapter fuzz binaries.
- `node` (default): Enables the full live node, builder, config, and execution manager.
- `plugin` (default): Keeps compatibility stubs for plug-in config validation.
- `python`: Enables Python bindings from [PyO3](https://pyo3.rs) and auto-enables `node` and
  `streaming`.
- `simulation`: Enables deterministic simulation testing with
  [MadSim](https://crates.io/crates/madsim).
- `streaming`: Enables the `nautilus-persistence` dependency for streaming configuration and
  requires `node`.
- `test-support`: Enables shared execution harnesses for adapter integration tests.

## Documentation

See [the docs](https://docs.rs/nautilus-live) for more detailed usage.

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

Use of this software is subject to the [Disclaimer](https://nautilustrader.io/legal/disclaimer/).

<img src="https://github.com/nautechsystems/nautilus_trader/raw/develop/assets/nautilus-logo-white.png" alt="logo" width="300" height="auto"/>

© 2015-2026 Nautech Systems Pty Ltd. All rights reserved.

## Managed Metis extension

### Current bounded native profile

The current managed batch selects official `develop`
`021f9d6642c1fbaff7590b5609ebc59d0ed9ee32` (Rust 1.99). The contract table and limits
below describe earlier protocol/handoff batches and retain their historical scope.
With `native-tail-replay`, an actual owned Journal and explicitly registered
codecs/component/adapter/timer profiles, the current node also captures completed
roots with seven physical native FIFOs, original receipts and shared callback
ancestry, then replays a verified sealed original tail through actual handlers and
compares the resulting final cut. See `RunningCheckpointBoundary` and
`LiveNode::replay_native_tail`.

The legacy observer's `coverage_complete` and `queue_state_collected` remain false;
only the separately verified native inventory proves its bounded capture. Missing
codecs/owner profiles, unknown active or failed callbacks, in-flight venue queries,
external forwarding/private pipelines and unsealed original tails are refused.
Legacy nonempty pending FIFO lacking its original ancestry sidecar is unsupported.
Execution observation and transport fences stay closed during history; SDK tests
do not establish CQS recovery readiness, account reconciliation, authorization or
venue acceptance. The current facts supersede only the appendix's general
"queue state is not collected" description for this explicitly admitted profile.

| Local contract | Scope and regression entry | Removal criterion |
|---|---|---|
| Explicit dispatch begin/complete/abort protocol | Opt-in `dispatch-observer`; `dispatch::tests` checks nesting, ordered tokens, durable failure and aborts | Equivalent upstream protocol plus complete ingress wiring and failure tests |
| Idle recovery dispatch boundary and FIFO handoff | `LiveNode::with_recovery_dispatch` plus `enqueue_recovery_dispatch`/`drain_recovery_dispatch` stage and drain caller-owned envelopes in observed FIFO order, preserving a failed suffix | Observed completion must still be tied to the composite/runner watermark; durable idempotency and recovery authorization remain separate |
| Pre-start runner-channel recovery handoff | `RunnerRecoveryHandoff` with sealed `RunnerRecoveryCodecRegistry` decodes validated envelopes into the seven real runner mpsc channels, optionally binds a composite identity/checkpoint/dispatch watermark with contiguous sequencing, and closes retained handles after startup or receiver extraction; `PausedCompositeRecovery::replay_pending_into_runner` supplies ordered composite inventory checks and a channel-only receipt | Production codec registration, observed processing completion, durable idempotency, venue reconciliation and recovery authorization |

Source revision and limits are recorded in [METIS_SOURCE.md](METIS_SOURCE.md).
The observer is optionally connected to synchronous LiveNode dispatch paths. Remaining
startup, query-result, maintenance, external and lifecycle paths emit Uncovered
dispositions; queue state is not collected and coverage remains incomplete. Nodes
without an observer retain upstream behavior. Tests do not establish composite
checkpoint or recovery acceptance.
