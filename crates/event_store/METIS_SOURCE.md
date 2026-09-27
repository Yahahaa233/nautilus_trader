> Current fork base: NautilusTrader v2.0.0rc5, upstream commit 1b0a49d2792a9432a3aca3fcb617ce7a630d905e.
> Imported from Metis vendor manifest; earlier versions below are historical provenance.

# Managed upstream event-store correction

Source: NautilusTrader v2.0.0rc4, a0400251110653b6d8ae6a9b5b89c4543fa85a2d.

Registered encoder failures now fail-stop the capture adapter and signal the native lifecycle with `HaltReason::CaptureEncoding`. Subsequent dispatches are blocked; normal lifecycle sealing and drop leave the failed run for crash recovery. Unregistered types remain outside the capture contract.

Regression: adapter error/next-hop tests and Metis real-Redb lifecycle tests. Remove this override when upstream provides equivalent fail-stop semantics and these regressions pass unchanged. This does not acknowledge durable writes or authorize Metis state recovery.

The two `test_data/upstream-cache` files are exact pinned common cache sources used by the upstream recovery classification test. They are test fixtures, not an additional runtime override. Inventory refresh checks them against the pinned checkout.

Session snapshot callbacks observe the shared halt signal, including callbacks retained before a capture failure; direct session close also refuses halted runs. Regression proves no new recovery anchor or RunEnded is written and the predecessor recovers as CrashedRecovered.

`flush()` is a synchronous FIFO durability acknowledgment for writer, session and lifecycle callers. It drains earlier entries and returns the committed watermark without sealing or creating a cache snapshot anchor. Queue/acknowledgment timeouts share the existing control-request fail-stop path. Closed/absent/capture-halted sessions reject confirmation. Metis business checkpoints must not masquerade as native cache snapshot blobs.

`EventStoreLifecycle::capture_and_flush` supports caller-owned business messages without bus dispatch. It preserves registered headers, requires an actual captured entry, and confirms durability through the session; missing encoders and duplicate identities never return a successful receipt.

Owners can abort required external persistence via a distinct `ExternalPersistence` halt reason. Metis uses this for file-checkpoint failures and unwinds so an incomplete business checkpoint cannot leave a normally sealed native journal.
