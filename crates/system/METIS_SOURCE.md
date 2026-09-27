> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

# Managed source and component-state collection

Source: NautilusTrader v2.0.0rc4, commit a0400251110653b6d8ae6a9b5b89c4543fa85a2d,
upstream directory crates/system. Package name and upstream license are preserved.

Metis adds Trader::collect_component_state, Trader::restore_component_state and
CollectedComponentState. Collection invokes registered actor and strategy on_save
callbacks without requiring a backing database and without writing cache/database
state. Empty results remain keyed by registered identity. Callback errors return no
partial DTO. Registration is checked before and after collection, and the trader
RefCell is not held during callbacks.

Restoration accepts only a non-active trader whose registered actor and strategy
identities and order exactly match the payload. All identities are checked before
callbacks run; a callback failure is returned and the caller must discard the node.
The API loads callback payloads only. It does not restore cache, portfolio,
execution algorithms, queues, timers or risk permission, and it cannot authorize
recovery by itself.

This API does not establish a common event boundary, persist a checkpoint, collect
execution-algorithm or private framework state, serialize queues/timers, or authorize
recovery. The caller must supply those contracts. Existing save_state behavior is
unchanged. Removal requires equivalent upstream API and the collection regressions.

Tests: `component_collection_tests` covers collection, identity rejection, active
trader rejection and real callback restoration; the managed vendor test target
passes these cases.
Cargo.toml uses the enclosing managed vendor workspace. Root and vendor lockfiles,
patch mappings, manifest hashes and dependency checks are integrated centrally.
