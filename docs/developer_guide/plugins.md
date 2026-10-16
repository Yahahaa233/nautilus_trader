# Plugins

The `nautilus-plugin` crate defines the artifact contract for NautilusTrader plug-ins: an
independently compiled Rust `cdylib` that identifies itself with a versioned manifest and exchanges
values across a C-ABI boundary. The original artifact metadata and boundary primitives remain
available independently. This managed fork also provides an opt-in encoded strategy component and a
trusted GNU library loader, described below. They are independent of the private Nautilus Pro host;
the legacy generic Live plug-in entry does not acquire strategy loading support implicitly.

:::warning
The plug-in ABI is early alpha and the contract is unstable. Pin plug-in builds to the matching
`nautilus-plugin` version.
:::

## Artifact contract

A plug-in is a Rust `cdylib` that exports a single entry symbol, `nautilus_plugin_init`. The
`nautilus_plugin!` macro generates that symbol together with the static manifest carrying the build
identity:

```rust
nautilus_plugin::nautilus_plugin! {
    name: "example-plugin",
    vendor: "Nautech",
    version: env!("CARGO_PKG_VERSION"),
}
```

`name` and `version` are required, `vendor` defaults to an empty string. Invoke the macro once per
artifact at module scope, set `crate-type = ["cdylib"]` in the artifact's `Cargo.toml`, and depend on
the matching `nautilus-plugin` version.

## Manifest compatibility

`nautilus_plugin_init` takes an opaque host pointer and returns a `PluginManifest` held in
process-lifetime storage, or null when the host pointer is null or the call panics.
`PluginManifest::validate` checks the invariants a host relies on before registration. It reports
every structural problem it finds and fails when:

- `abi_version` does not equal `NAUTILUS_PLUGIN_ABI_VERSION`, or `build_id.schema_version` does not
  equal `PLUGIN_BUILD_ID_VERSION`.
- `plugin_name` or `plugin_version` is empty.
- Any manifest string is malformed: a null pointer with non-zero length, or bytes that are not valid
  UTF-8.
- `build_id.precision_mode` or `build_id.fixed_precision` differs from the host build.

Precision is validated because it changes model type layout across the boundary. The remaining build
identity fields (`nautilus-plugin` version, `rustc` version, target triple, and build profile) are
diagnostic.

## Boundary rules

Only the `#[repr(C)]` types in `nautilus_plugin::boundary`, and `#[repr(C)]` types built from them,
may appear in a signature that crosses the boundary. `String`, `Vec`, and `Box<dyn Trait>` rely on
Rust's unstable ABI and must never cross it. Panics must not unwind across the boundary either; the
generated entry symbol catches them and returns null.

Under the current ABI, plug-ins are not unloaded, so manifest storage stays valid for the life of the
process.

See [the crate docs](https://docs.rs/nautilus-plugin) for the boundary and manifest types.

## Managed encoded strategy components

The fork's `nautilus_plugin::strategy` module adds a separate executable strategy ABI, version 1.
It does not change metadata ABI 1 or the experimental native-value component ABI 0. An algorithm
implements every `Algorithm` method: pure validation and creation, Start, Event, immutable Save,
versioned Restore, Pause, Resume and Stop. `nautilus_strategy!(AlgorithmType)` generates
`nautilus_strategy_init_v1` and the producer-owned disposal thunk. The metadata macro is still
required once per library.

The executable table has an ABI/size prefix, actual build and precision fields, panic/allocation
policies and an embedded encoded descriptor. The native instance refuses incompatible table sizes,
compiler, target, profile or precision and requires exact admitted descriptor bytes before creation.
The application publisher must also establish the SDK source revision, configuration/state schemas,
artifact bytes and algorithm qualification. Metadata equality alone grants no execution permission.
In particular, a library hash or a structurally valid ELF is not a publication trust certificate.

Both the executable host and algorithm must actually use `panic=unwind`. Generated thunks contain
ordinary Rust panics and preserve their original category/message. The SDK's general release profile
uses `panic=abort`; a component producer must explicitly select and record an unwind profile for
both sides. An abort build is refused before algorithm validation/creation. No allocator-owned Rust
container or trait object crosses the ABI: bounded bytes are copied and freed by the producing
side. A `GuestInstance` owns one opaque algorithm allocation and retains its first callback failure.
Save or Restore cannot clear that failure.

Creating an owning guest also requires a host-owned `StateValidator` bound to the admitted
descriptor and schema. Restore validates the complete original bytes before any guest callback;
Save validates the returned bytes before exposing them to checkpoint owners. The validator has no
mutable host capability. Its refusal or contained panic uses the same first-failure latch as an
algorithm callback, so another Restore or a runtime callback cannot clear it. The application must
provide its real state contract; a permissive validator is not production qualification.

### Native host access and lifecycle

`Host` is borrowed, non-cloneable and non-Send. Each callback receives a process-unique instance and
generation lease. The issuing thread must resolve that exact active scope before touching the
native facade. Expired, cross-instance, cross-thread, nested and reentrant calls fail. Save/Restore
receive `ReadOnlyHost`; native dispatch also refuses mutating requests during state handoff.

`nautilus_trading::plugin::PluginStrategy` uses one real `StrategyCore`, native component state,
cache, clock, order factory and submission interception. It adds no runner, venue client, order
manager or ledger. Supported encoded host operations currently cover clock/timer facts, declared
bar subscriptions, bounded one-shot timers, market/limit construction and submission, and
strategy/account/instrument-scoped cached order query/modify/cancel. Unsupported operations return
an error, including history/portfolio/instrument operations not yet implemented by this bridge.
An application's capability admission must reject declarations that require those missing paths.
Encoded request types live in `nautilus_plugin::strategy::wire` and are re-exported by the native
adapter. An algorithm therefore needs no dependency on `StrategyCore` to use the shared protocol.

Native market, historical-bar, order, position and timer callbacks enter the encoded Event hook.
A reported event failure or contained panic faults that actual actor, retains the first guest error,
releases its own timers/subscriptions and leaves unrelated actors with their own native state.
Save produces algorithm bytes plus configuration/descriptor/schema identity; Restore checks that
identity. A failed guest instead produces a distinct bounded terminal-failure record, without
claiming that its partially mutated opaque state is resumable. Only an unused physical guest can
retain that failure during restore; native startup inhibition keeps it inactive while unrelated
components start normally. Explicit native starts also refuse an inhibited component. This
restriction never grants execution authority or clears the original error.
If the first failure occurs inside Save or its state validator, that same component collection
returns the distinct terminal-failure record immediately. Invalid state bytes are not returned,
and the caller does not need another Save attempt to preserve the original failure.

A two-process component-state test does not prove a complete order/portfolio/risk/timer checkpoint
or production recovery qualification. Those remain requirements of the application's common
native checkpoint and journal path.

### GNU library admission and lifetime

The `host` feature exposes `LibraryRegistry` on x86-64 GNU Linux. Its unsafe `load_admitted` entry
requires an already-reviewed managed publication and constructor/dependency provenance. It refuses
root callers, loader environment overrides, writable or untrusted source files, incorrect SHA-256,
oversized/malformed ELF layouts, executable images or W+X mappings, search-path/audit/filter overrides and
unknown direct runtime dependencies before `dlopen` can run a constructor. Rust runtime initializers
may be present; recording them is not proof that arbitrary constructors are safe.

The loader copies the admitted bytes into a sealed executable memfd and uses immediate local
binding. Both the memfd descriptor number and loader handle remain pinned until process exit;
releasing the registry does not unload code still referenced by callbacks, producer deallocators or
metadata. Loaded/failed library identities share a fixed registry capacity. Trusted native code is
not a malicious-code sandbox: process supervision must handle memory corruption, native aborts and
unbounded blocking.

The `contract_counter`, `contract_interval` and `strategy_contract_host` examples are private
non-financial conformance fixtures. They use one actual LiveNode and native clocks, with no venue
clients and an interceptor that rejects every attempted submit. Their descriptors explicitly mark
them as research-only fixtures. They do not supply the application's complete strategy publisher,
BacktestID binding, execution authority or production acceptance.
