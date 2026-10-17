# Plugins

The `nautilus-plugin` crate defines the artifact contract for NautilusTrader plug-ins: an
independently compiled Rust `cdylib` that identifies itself with a versioned manifest and exchanges
values across a C-ABI boundary. The original artifact metadata and boundary primitives remain
available independently. This managed fork also provides an opt-in encoded strategy component and a
trusted GNU/Darwin library loader, described below. They are independent of the private Nautilus Pro host;
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
manager or ledger. Encoded host operations cover clock/timer facts, declared bars/quotes/trades,
bounded cached warmup and historical-bar requests, native market/limit/stop construction, scoped
cached facts and order query/modify/cancel, and managed single/batch/bracket submission through the
application's installed policy. Unsupported operations return an error. Capability admission must
match the paths, limits and policy actually installed by the application.
Encoded request types live in `nautilus_plugin::strategy::wire` and are re-exported by the native
adapter. An algorithm therefore needs no dependency on `StrategyCore` to use the shared protocol.

Ordinary order operations require the original cache's exact client binding in addition to native
strategy, trader, account and instrument ownership. New requests may supply bounded unique
algorithm tags; they cannot supply the native `MARKET_EXIT` tag. Bracket entry/take-profit/stop-loss
tags remain attached to their actual native legs, including the native factory's defaults when
none are supplied.

Managed original-order observations distinguish a pending economic amendment from an actual
native liquidation reduction verified by the installed application owner. A smaller cached
position, an algorithm tag or a rejection string is not a liquidation proof. The bridge only
encodes the owner's scoped observation; it cannot create or alter the native proof.

`ManagedMarketExit` accepts only its schema version. A Running component with an installed managed
policy and reduce-only native exit configuration may request the existing native market-exit
framework. The immutable host scope restricts its original order cancellation, position closing
and timer retries to the admitted account, client and instruments. It creates no guest-supplied
scope, second timer, entry allocation or execution permission; original native submission
interception and transport controls still apply. A repeated request does not restart the loop,
and an initialized local close remains pending rather than being duplicated by the next check.

Native market, historical-bar, order, position and timer callbacks enter the encoded Event hook.
A reported event failure or contained panic faults that actual actor, retains the first guest error,
releases its own timers/subscriptions and leaves unrelated actors with their own native state.
Save produces algorithm bytes, configuration/descriptor/schema identity and the adapter's actual
subscription inventory; Restore checks those complete fields before use. Legacy component-only
checkpoints without a subscription inventory are accepted only when their declared bar domain is
empty. Other upgrades require an explicit qualified migration. A failed guest instead produces a
distinct bounded terminal-failure record, without
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

### Reader-verified historical callbacks

With `native-tail-replay` and `strategy-plugin`, the Live owner supplies
`node::plugin_recovery::ReaderVerifiedPluginRecovery` before registering a `PluginStrategy`.
Trading depends only on its verifier interface; it does not add an event-store dependency cycle.
Preparation requires the actual Journal reader's `VerifiedNativeTrace` and compares this owner's
whole private state, native factory/framework facts and restored subscription inventory with its
original cut. Local route installation does not call ordinary Start or contact a venue.

Every historical callback verifies the actual `VerifiedNativeRoot`, source identity, root inventory,
whole original Begin and component owner. The native driver still owns clock/UUID/output/queue
comparison and suppresses transport. Historical plugin error/panic cleanup keeps the target
physically Ready, retains the original guest failure and inhibits later startup; it does not attempt
an invalid Ready-to-Fault physical transition. Missing/unrelated proofs, changed state and unsupported
input profiles fail recovery and retain the failure. A verified historical cut never creates a
current execution grant or clears a process transmission veto.

The private `strategy_contract_host --history-source/--history-restore` fixture uses real sealed
Journals and different PIDs for normal, original Generic and original Panic scenarios. Actual exact
state/decision/UUID/clock/routing comparisons and source/configuration/state refusals are bounded
conformance evidence. They do not qualify nonflat portfolios, venue protection, economic ledgers or
an application's production recovery integration.

### Platform library admission and lifetime

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

On ARM64 Darwin, the same `strategy::library_loader` path verifies the Mach-O layout, explicit
system dependency set and actual OS signature admission. The host supplies a private directory.
The loader creates a named readonly copy, closes its writable descriptor, checks its inode and exact
bytes, protects the directory and loads via its retained descriptor. The named file, descriptor and
loader handle remain live until process exit. Darwin signing policy requires named backing; this is
trusted same-UID host custody and does not claim GNU memfd sealing or adversarial native-code isolation.

Both platform parsers bound virtual mappings and reject overlapping/unbounded images. Each host
registry conservatively charges copied bytes and mapped images across successful and failed loads.
The host retains one registry for the process lifetime; dropping it does not recycle resident capacity.

The `contract_counter`, `contract_interval` and `strategy_contract_host` examples are private
non-financial conformance fixtures. They use one actual LiveNode and native clocks, with no venue
clients and an interceptor that rejects every attempted submit. Their descriptors explicitly mark
them as research-only fixtures. They do not supply the application's complete strategy publisher,
BacktestID binding, execution authority or production acceptance.
`strategy_contract_research_host` runs the same two dynamic algorithms in one actual BacktestEngine
with virtual clocks and the same native submission refusal. It is also conformance evidence, not
trading eligibility or a completed research report.

### Native framework timer handoff

`strategy::route_time_event` remains the owner of native GTD expiry and market-exit checks.
After processing an owned event it retains that exact `TimeEvent` at the callback boundary.
`PluginStrategy` consumes only this original framework handoff; it does not deliver an internal
framework check as an encoded guest timer or charge it to the guest timer allowance. A changed
name, UUID, scheduled time or arrival time cannot reuse the original handoff. All other encoded
timer callbacks retain the existing guest namespace checks and normal guest dispatch.

The handoff is reset at each native timer dispatch and is not serialized. It is callback routing
metadata, not a timer, recovery proof, financial policy or execution permission. Native framework
state, timer effects and historical verification remain with their existing owners. This does not
by itself qualify an application's full financial/native recovery cut.
