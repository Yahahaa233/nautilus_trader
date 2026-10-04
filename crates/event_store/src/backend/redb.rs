// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! redb-backed [`EventStore`] implementation.
//!
//! One redb file per run at `<base>/<instance_id>/<run_id>.redb`. Every commit uses
//! [`Durability::Immediate`] so a crashed writer never leaves the in-flight tail visible
//! after reopen, and the high-watermark only advances after a durable acknowledgement.

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::{
    cell::Cell,
    fmt::Debug,
    fs,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::ThreadId,
    time::SystemTime,
};

use nautilus_core::UnixNanos;
use redb::{
    CommitError, Database, DatabaseError, Durability, ReadOnlyDatabase, ReadTransaction,
    ReadableDatabase, ReadableTable, StorageBackend, StorageError, TableDefinition, TableError,
    TransactionError, WriteTransaction, backends::FileBackend,
};

use crate::{
    backend::{AppendEntry, EventStore, IndexKey, IndexKind, ScanDirection},
    codec,
    entry::EventStoreEntry,
    error::EventStoreError,
    format,
    hash::EntryHash,
    manifest::{RunManifest, RunStatus},
    snapshot::{SnapshotAnchor, validate_new_anchor},
};

const ENTRIES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
const MANIFEST_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("manifest");
const CLIENT_ORDER_INDEX: TableDefinition<&str, u64> = TableDefinition::new("client_order_id_idx");
const VENUE_ORDER_INDEX: TableDefinition<&str, u64> = TableDefinition::new("venue_order_id_idx");
const SNAPSHOT_ANCHOR_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshot_anchor");

const MANIFEST_KEY: &str = "current";
const SNAPSHOT_ANCHOR_KEY: &str = "latest";

/// On-disk [`EventStore`] backed by a per-run [`redb`] file.
///
/// One backend instance owns at most one open run at a time. Opening a fresh run creates
/// `<base>/<instance_id>/<run_id>.redb` and writes the manifest with status
/// [`RunStatus::Running`] before returning. Reopening a path whose manifest is still
/// [`RunStatus::Running`] returns [`EventStoreError::CrashedPredecessor`]; the caller seals
/// it as [`RunStatus::CrashedRecovered`] (or [`RunStatus::Quarantined`]) and then opens a new
/// run, mirroring the in-memory backend's contract.
#[derive(Debug)]
pub struct RedbBackend {
    base_dir: PathBuf,
    state: Option<RunState>,
}

#[derive(Debug)]
struct RunState {
    db: RunDatabase,
    manifest: RunManifest,
    high_watermark: u64,
    max_ts_init: UnixNanos,
    file_path: PathBuf,
    append_only_prefix: Option<VerifiedAppendOnlyPrefix>,
    prefix_failed: Cell<bool>,
    #[cfg(test)]
    verified_suffix_reads: u64,
}

/// Available only for a fresh run owned by this database handle. Existing files
/// never inherit these process-local verified hashes from an earlier writer.
#[derive(Debug)]
struct VerifiedAppendOnlyPrefix {
    run_id: String,
    hashes: Vec<EntryHash>,
    generation: FileGeneration,
    writes: Arc<StorageWriteEpoch>,
    verified_write_generation: u64,
}

/// Tracks the actual owned redb backend writes, independently of filesystem
/// timestamp granularity. A legal operation owns one current-thread permit;
/// writes outside it remain permanently visible even if a later legal commit
/// would otherwise refresh file metadata. This adds no old-row reads.
#[derive(Debug, Default)]
struct StorageWriteEpoch {
    generation: AtomicU64,
    failed: AtomicBool,
    armed: AtomicBool,
    owner: Mutex<Option<ThreadId>>,
}

impl StorageWriteEpoch {
    fn record_mutation(&self) -> io::Result<()> {
        let owner = self.owner.lock().map_err(|_| {
            self.failed.store(true, Ordering::SeqCst);
            io::Error::other("native storage ownership lock poisoned")
        })?;
        if self.armed.load(Ordering::SeqCst) && *owner != Some(std::thread::current().id()) {
            // Do not replace redb's I/O semantics: let the actual write happen,
            // but never permit any following cached-prefix proof or ACK.
            self.failed.store(true, Ordering::SeqCst);
        }
        self.generation
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |value| {
                value.checked_add(1)
            })
            .map_err(|_| {
                self.failed.store(true, Ordering::SeqCst);
                io::Error::other("native storage generation exhausted")
            })?;
        Ok(())
    }

    fn snapshot(&self) -> Result<u64, EventStoreError> {
        let before = self.generation.load(Ordering::SeqCst);
        if self.failed.load(Ordering::SeqCst) || before != self.generation.load(Ordering::SeqCst) {
            return Err(EventStoreError::Backend(
                "native storage writes changed or failed".into(),
            ));
        }
        Ok(before)
    }

    fn begin_owned(self: &Arc<Self>, expected: u64) -> Result<StorageWritePermit, EventStoreError> {
        let mut owner = self.owner.lock().map_err(|_| {
            self.failed.store(true, Ordering::SeqCst);
            EventStoreError::Backend("native storage ownership lock poisoned".into())
        })?;
        if owner.is_some() || self.snapshot()? != expected {
            self.failed.store(true, Ordering::SeqCst);
            return Err(EventStoreError::Backend(
                "native storage write ownership changed".into(),
            ));
        }
        *owner = Some(std::thread::current().id());
        Ok(StorageWritePermit(self.clone()))
    }

    fn check_owned(&self) -> Result<(), EventStoreError> {
        let owner = self.owner.lock().map_err(|_| {
            self.failed.store(true, Ordering::SeqCst);
            EventStoreError::Backend("native storage ownership lock poisoned".into())
        })?;
        if *owner != Some(std::thread::current().id()) {
            self.failed.store(true, Ordering::SeqCst);
            return Err(EventStoreError::Backend(
                "native storage refresh lacks owned write".into(),
            ));
        }
        Ok(())
    }
}

struct StorageWritePermit(Arc<StorageWriteEpoch>);
impl Drop for StorageWritePermit {
    fn drop(&mut self) {
        match self.0.owner.lock() {
            Ok(mut owner) => *owner = None,
            Err(_) => self.0.failed.store(true, Ordering::SeqCst),
        }
    }
}

#[derive(Debug)]
struct OwnedStorageBackend {
    inner: FileBackend,
    epoch: Arc<StorageWriteEpoch>,
}
impl StorageBackend for OwnedStorageBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.epoch.record_mutation()?;
        let result = self.inner.set_len(len);
        if result.is_err() {
            self.epoch.failed.store(true, Ordering::SeqCst);
        }
        result
    }
    fn sync_data(&self) -> io::Result<()> {
        let result = self.inner.sync_data();
        if result.is_err() {
            self.epoch.failed.store(true, Ordering::SeqCst);
        }
        result
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        // Advance before delegation, including failed or partially written I/O.
        self.epoch.record_mutation()?;
        let result = self.inner.write(offset, data);
        if result.is_err() {
            self.epoch.failed.store(true, Ordering::SeqCst);
        }
        result
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct FileGeneration {
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    ctime: (i64, i64),
}

impl FileGeneration {
    fn read(path: &Path) -> Result<Self, EventStoreError> {
        let metadata = fs::symlink_metadata(path)
            .map_err(|e| EventStoreError::Backend(format!("native prefix file metadata: {e}")))?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(EventStoreError::Backend(
                "native prefix file identity changed".into(),
            ));
        }
        Ok(Self {
            len: metadata.len(),
            modified: metadata.modified().map_err(|e| {
                EventStoreError::Backend(format!("native prefix modification time: {e}"))
            })?,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            ctime: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

impl RunState {
    fn check_prefix_owner(&self) -> Result<(), EventStoreError> {
        if self.prefix_failed.get() {
            return Err(EventStoreError::Backend(
                "native prefix verification previously failed".into(),
            ));
        }
        let result = (|| {
            if let Some(prefix) = &self.append_only_prefix {
                let before = prefix.writes.snapshot()?;
                if before != prefix.verified_write_generation
                    || prefix.run_id != self.manifest.run_id
                    || u64::try_from(prefix.hashes.len()).ok() != Some(self.high_watermark)
                    || prefix.generation != FileGeneration::read(&self.file_path)?
                    || before != prefix.writes.snapshot()?
                {
                    return Err(EventStoreError::Backend(
                        "native prefix owner or generation changed".into(),
                    ));
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.prefix_failed.set(true);
        }
        result
    }

    fn begin_owned_write(&self) -> Result<Option<StorageWritePermit>, EventStoreError> {
        let result = (|| {
            self.check_prefix_owner()?;
            self.append_only_prefix
                .as_ref()
                .map(|prefix| prefix.writes.begin_owned(prefix.verified_write_generation))
                .transpose()
        })();
        if result.is_err() {
            self.prefix_failed.set(true);
        }
        result
    }

    fn refresh_prefix_generation(&mut self) -> Result<(), EventStoreError> {
        let result = (|| {
            if let Some(prefix) = &mut self.append_only_prefix {
                prefix.writes.check_owned()?;
                let before = prefix.writes.snapshot()?;
                let generation = FileGeneration::read(&self.file_path)?;
                if before != prefix.writes.snapshot()? {
                    return Err(EventStoreError::Backend(
                        "native storage changed during refresh".into(),
                    ));
                }
                prefix.generation = generation;
                prefix.verified_write_generation = before;
            }
            Ok(())
        })();
        if result.is_err() {
            self.prefix_failed.set(true);
        }
        result
    }
}

enum RunDatabase {
    ReadWrite(Database),
    ReadOnly(ReadOnlyDatabase),
}

impl Debug for RunDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadWrite(_) => f.write_str("RunDatabase::ReadWrite"),
            Self::ReadOnly(_) => f.write_str("RunDatabase::ReadOnly"),
        }
    }
}

impl RunDatabase {
    fn readable(&self) -> &dyn ReadableDatabase {
        match self {
            Self::ReadWrite(db) => db,
            Self::ReadOnly(db) => db,
        }
    }

    fn read_write(&self) -> Result<&Database, EventStoreError> {
        match self {
            Self::ReadWrite(db) => Ok(db),
            Self::ReadOnly(_) => Err(EventStoreError::Closed),
        }
    }

    fn begin_read(&self) -> Result<ReadTransaction, EventStoreError> {
        self.readable().begin_read().map_err(map_transaction_err)
    }
}

impl RedbBackend {
    /// Creates a new [`RedbBackend`] rooted at `base_dir`.
    ///
    /// The backend creates `<base_dir>/<instance_id>/` lazily on the first
    /// [`EventStore::open_run`] call.
    #[must_use]
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
            state: None,
        }
    }

    /// Returns the directory the backend writes run files to for `instance_id`.
    #[must_use]
    pub fn run_dir(&self, instance_id: &str) -> PathBuf {
        self.base_dir.join(instance_id)
    }

    /// Returns the on-disk path the backend uses for `(instance_id, run_id)`.
    #[must_use]
    pub fn run_path(&self, instance_id: &str, run_id: &str) -> PathBuf {
        self.run_dir(instance_id).join(format!("{run_id}.redb"))
    }

    /// Returns the path of the currently open run file.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when no run is open.
    pub fn current_path(&self) -> Result<&Path, EventStoreError> {
        Ok(self.state()?.file_path.as_path())
    }

    /// Opens the sealed run file at `<base>/<instance_id>/<run_id>.redb` for read-only replay.
    ///
    /// # Design
    ///
    /// The standard [`EventStore::open_run`] path rejects sealed files: that is the
    /// crash-recovery guard, a successor must not silently reopen a predecessor's log
    /// without going through seal. Event-store replay is the legitimate case for touching
    /// a sealed file, so the reader uses this constructor instead.
    ///
    /// The shared [`EventStore`] trait is held intentionally narrow and is locked by
    /// design; adding a sealed-open method to it would force the in-memory backend
    /// (whose sealed runs stay readable in place without a reopen step) to carry a
    /// useless second entry point, and would conflate the writer's open-or-recover
    /// lifecycle with the reader's pure read-only path. The sealed-open path therefore
    /// lives as a backend-specific constructor: each backend adds the entry points it
    /// actually needs. The resulting [`RedbBackend`] still implements [`EventStore`],
    /// so the reader composes over the locked trait without pulling in writer-only
    /// methods. [`crate::backend::MemoryBackend`] has no equivalent constructor: a
    /// sealed in-memory run keeps its state accessible to any reader holding the
    /// backend instance, and the reader receives that instance directly.
    ///
    /// The returned backend holds a read-only database handle, rejects
    /// [`EventStore::append_batch`] with [`EventStoreError::Closed`] (the manifest is
    /// already sealed), and exposes every read path: [`EventStore::scan_range`],
    /// [`EventStore::scan_seq`], [`EventStore::lookup`], and [`EventStore::manifest`].
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the run file does not exist or its
    /// status is not a sealed terminal state (use [`EventStore::open_run`] for that
    /// path); [`EventStoreError::Corrupted`] when the run file lacks a manifest or
    /// fails to decode.
    pub fn open_sealed(
        base_dir: impl Into<PathBuf>,
        instance_id: &str,
        run_id: &str,
    ) -> Result<Self, EventStoreError> {
        let base = base_dir.into();
        let path = base.join(instance_id).join(format!("{run_id}.redb"));
        Self::open_sealed_path(base, path)
    }

    /// Opens a sealed redb run file directly by path for read-only replay or verification.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the run file does not exist or its
    /// status is not a sealed terminal state (use [`EventStore::open_run`] for that
    /// path); [`EventStoreError::Corrupted`] when the run file lacks a manifest or
    /// fails to decode.
    pub fn open_sealed_file(path: impl Into<PathBuf>) -> Result<Self, EventStoreError> {
        let path = path.into();
        let base = path
            .parent()
            .and_then(Path::parent)
            .map_or_else(PathBuf::new, Path::to_path_buf);
        Self::open_sealed_path(base, path)
    }

    fn open_sealed_path(base: PathBuf, path: PathBuf) -> Result<Self, EventStoreError> {
        if !path.exists() {
            return Err(EventStoreError::Backend(format!(
                "no run file at {}",
                path.display()
            )));
        }

        let db = ReadOnlyDatabase::open(&path).map_err(map_read_only_database_err)?;
        format::verify_store_format(&db)?;
        let manifest = Self::read_manifest(&db)?.ok_or_else(|| {
            EventStoreError::Corrupted(format!(
                "missing manifest in run file at {}",
                path.display()
            ))
        })?;

        if !manifest.is_sealed() {
            return Err(EventStoreError::Backend(format!(
                "run file at {} is not sealed, status was {:?}",
                path.display(),
                manifest.status,
            )));
        }
        let (high_watermark, max_ts_init) = Self::compute_progress(&db)?;

        Ok(Self {
            base_dir: base,
            state: Some(RunState {
                db: RunDatabase::ReadOnly(db),
                manifest,
                high_watermark,
                max_ts_init,
                file_path: path,
                append_only_prefix: None,
                prefix_failed: Cell::new(false),
                #[cfg(test)]
                verified_suffix_reads: 0,
            }),
        })
    }

    /// Lists the manifests of every run file under `<base_dir>/<instance_id>/*.redb`.
    ///
    /// Used by the reader for forensics navigation across runs without requiring an
    /// active backend instance per run. The result is sorted by `start_ts_init` so
    /// chronologically-newer runs appear last.
    ///
    /// Opens each run file with a read-only database handle. A run file whose process
    /// died hard (kill, OOM, power loss) lacks redb's allocator-state table and refuses
    /// the read-only open; the listing falls back to a writable open, which performs
    /// redb's repair pass and leaves the file readable again. Files that still cannot
    /// be opened or that lack a manifest are skipped with a logged error so one
    /// current-format damaged file cannot block recovery or retention over the healthy
    /// runs; such files never become recovery parents or reclaim candidates and are
    /// left in place for manual inspection. Unsupported store formats are returned as
    /// errors rather than skipped because they require operator action.
    ///
    /// # Errors
    ///
    /// Returns [`EventStoreError::Backend`] when the directory iterator fails, or
    /// [`EventStoreError::Corrupted`] when a run file uses an unsupported store format.
    pub fn list_runs(
        base_dir: &Path,
        instance_id: &str,
    ) -> Result<Vec<RunManifest>, EventStoreError> {
        let dir = base_dir.join(instance_id);
        let entries = match fs::read_dir(&dir) {
            Ok(it) => it,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => {
                return Err(EventStoreError::Backend(format!(
                    "read_dir {}: {e}",
                    dir.display()
                )));
            }
        };

        let mut manifests = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|e| {
                EventStoreError::Backend(format!("read_dir entry in {}: {e}", dir.display()))
            })?;
            let path = entry.path();

            if !is_run_file(&path) {
                continue;
            }

            match Self::read_run_manifest(&path) {
                Ok(manifest) => manifests.push(manifest),
                Err(e) if format::is_unsupported_store_format(&e) => return Err(e),
                Err(e) => {
                    log::error!("Skipping unreadable run file {}: {e}", path.display());
                }
            }
        }
        // Break start-time ties on the run id so parent selection and retention stay
        // deterministic: stable sort alone preserves the platform-dependent `read_dir`
        // order.
        manifests.sort_by(|a, b| {
            a.start_ts_init
                .cmp(&b.start_ts_init)
                .then_with(|| a.run_id.cmp(&b.run_id))
        });
        Ok(manifests)
    }

    fn read_run_manifest(path: &Path) -> Result<RunManifest, EventStoreError> {
        let manifest = match ReadOnlyDatabase::open(path) {
            Ok(db) => {
                Self::verify_listed_store_format(&db, path)?;
                Self::read_manifest(&db)?
            }
            // Each durable commit deletes redb's allocator-state table and only a clean
            // `Database::drop` rewrites it, so a hard-killed process leaves a file the
            // read-only open refuses. A writable open repairs it for future opens.
            Err(DatabaseError::RepairAborted) => {
                log::warn!(
                    "Run file {} was not shut down cleanly, repairing",
                    path.display()
                );
                let db = Database::open(path).map_err(map_database_err)?;
                Self::verify_listed_store_format(&db, path)?;
                Self::read_manifest(&db)?
            }
            Err(e) => return Err(map_read_only_database_err(e)),
        };
        manifest.ok_or_else(|| missing_manifest(path))
    }

    fn verify_listed_store_format<D: ReadableDatabase + ?Sized>(
        db: &D,
        path: &Path,
    ) -> Result<(), EventStoreError> {
        match format::verify_store_format(db) {
            Ok(()) => Ok(()),
            Err(e) if format::is_missing_store_format(&e) && !Self::manifest_row_exists(db)? => {
                Err(missing_manifest(path))
            }
            Err(e) => Err(e),
        }
    }

    fn manifest_row_exists<D: ReadableDatabase + ?Sized>(db: &D) -> Result<bool, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = match txn.open_table(MANIFEST_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(false),
            Err(e) => return Err(map_table_err(e)),
        };

        table
            .get(MANIFEST_KEY)
            .map(|value| value.is_some())
            .map_err(map_storage_err)
    }

    fn state(&self) -> Result<&RunState, EventStoreError> {
        self.state
            .as_ref()
            .ok_or_else(|| EventStoreError::Backend("no run open".to_string()))
    }

    fn state_mut(&mut self) -> Result<&mut RunState, EventStoreError> {
        self.state
            .as_mut()
            .ok_or_else(|| EventStoreError::Backend("no run open".to_string()))
    }

    fn initialize_fresh(db: &Database, manifest: &RunManifest) -> Result<(), EventStoreError> {
        let txn = begin_immediate_write(db)?;
        {
            txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
            txn.open_table(CLIENT_ORDER_INDEX).map_err(map_table_err)?;
            txn.open_table(VENUE_ORDER_INDEX).map_err(map_table_err)?;
            txn.open_table(SNAPSHOT_ANCHOR_TABLE)
                .map_err(map_table_err)?;
        }
        format::write_store_format(&txn)?;
        insert_run_manifest(&txn, manifest)?;
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    fn write_manifest(db: &Database, manifest: &RunManifest) -> Result<(), EventStoreError> {
        let txn = begin_immediate_write(db)?;
        insert_run_manifest(&txn, manifest)?;
        txn.commit().map_err(map_commit_err)?;
        Ok(())
    }

    fn read_manifest<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<Option<RunManifest>, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(MANIFEST_TABLE).map_err(map_table_err)?;
        let Some(value) = table.get(MANIFEST_KEY).map_err(map_storage_err)? else {
            return Ok(None);
        };
        let bytes = value.value();
        let manifest = codec::decode_from_slice::<RunManifest>(bytes)
            .map_err(|e| EventStoreError::Corrupted(format!("decode manifest: {e}")))?;
        Ok(Some(manifest))
    }

    fn read_snapshot_anchor<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = match txn.open_table(SNAPSHOT_ANCHOR_TABLE) {
            Ok(table) => table,
            Err(TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(map_table_err(e)),
        };
        let Some(value) = table.get(SNAPSHOT_ANCHOR_KEY).map_err(map_storage_err)? else {
            return Ok(None);
        };
        let bytes = value.value();
        let anchor = codec::decode_from_slice::<SnapshotAnchor>(bytes)
            .map_err(|e| EventStoreError::Corrupted(format!("decode snapshot anchor: {e}")))?;
        Ok(Some(anchor))
    }

    fn compute_progress<D: ReadableDatabase + ?Sized>(
        db: &D,
    ) -> Result<(u64, UnixNanos), EventStoreError> {
        let txn = db.begin_read().map_err(map_transaction_err)?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;

        let Some((last_key, _)) = table.last().map_err(map_storage_err)? else {
            return Ok((0, UnixNanos::default()));
        };
        let high_watermark = last_key.value();

        // Walk the entry table once to recover the maximum `ts_init`. Memory.rs tracks this
        // across appends; on crash recovery we have nothing to fall back on, so we recompute
        // it from the durable rows. An undecodable row must not make the run unopenable:
        // max ts_init is best-effort, and the corruption itself surfaces on the scan paths,
        // where the recovery sweep quarantines the run.
        let mut max_ts = UnixNanos::default();
        let iter = table.iter().map_err(map_storage_err)?;

        for row in iter {
            let (key, value) = row.map_err(map_storage_err)?;
            let bytes = value.value();

            match codec::decode_from_slice::<EventStoreEntry>(bytes) {
                Ok(entry) => {
                    if entry.ts_init > max_ts {
                        max_ts = entry.ts_init;
                    }
                }
                Err(e) => {
                    log::error!("Undecodable entry at seq {} on load: {e}", key.value());
                }
            }
        }

        Ok((high_watermark, max_ts))
    }
}

impl EventStore for RedbBackend {
    fn open_run(&mut self, mut manifest: RunManifest) -> Result<(), EventStoreError> {
        if let Some(state) = &self.state {
            if matches!(state.db, RunDatabase::ReadOnly(_)) {
                return Err(EventStoreError::Closed);
            }

            if !state.manifest.is_sealed() {
                return Err(EventStoreError::CrashedPredecessor);
            }
        }

        let dir = self.run_dir(&manifest.instance_id);
        fs::create_dir_all(&dir).map_err(|e| {
            let msg = format!("create dir {}: {e}", dir.display());

            if is_disk_pressure(e.kind()) {
                EventStoreError::Disk(msg)
            } else {
                EventStoreError::Backend(msg)
            }
        })?;
        let path = self.run_path(&manifest.instance_id, &manifest.run_id);
        let path_existed = path.exists();

        let writes = Arc::new(StorageWriteEpoch::default());
        // Same FileBackend, OpenOptions, exclusive lock and builder defaults as
        // Database::create; only actual write/resize ownership is instrumented.
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| map_database_err(e.into()))?;
        let storage = OwnedStorageBackend {
            inner: FileBackend::new(file).map_err(map_database_err)?,
            epoch: writes.clone(),
        };
        let db = Database::builder()
            .create_with_backend(storage)
            .map_err(map_database_err)?;

        if path_existed {
            format::verify_store_format(&db)?;
            let on_disk = Self::read_manifest(&db)?.ok_or_else(|| {
                EventStoreError::Corrupted(format!(
                    "missing manifest in existing run file at {}",
                    path.display()
                ))
            })?;

            if !matches!(on_disk.status, RunStatus::Running) {
                return Err(EventStoreError::Backend(format!(
                    "run file at {} already sealed, status was {:?}",
                    path.display(),
                    on_disk.status
                )));
            }

            let (high_watermark, max_ts_init) = Self::compute_progress(&db)?;
            let mut recovered = on_disk;
            recovered.high_watermark = high_watermark;
            self.state = Some(RunState {
                db: RunDatabase::ReadWrite(db),
                manifest: recovered,
                high_watermark,
                max_ts_init,
                file_path: path,
                append_only_prefix: None,
                prefix_failed: Cell::new(false),
                #[cfg(test)]
                verified_suffix_reads: 0,
            });
            return Err(EventStoreError::CrashedPredecessor);
        }

        manifest.status = RunStatus::Running;
        manifest.end_ts_init = None;
        manifest.high_watermark = 0;
        Self::initialize_fresh(&db, &manifest)?;

        // File identity/generation is part of this optimization's ownership proof.
        // Platforms without the native inode contract retain complete scans.
        #[cfg(unix)]
        let append_only_prefix = {
            writes.armed.store(true, Ordering::SeqCst);
            Some(VerifiedAppendOnlyPrefix {
                run_id: manifest.run_id.clone(),
                hashes: Vec::new(),
                generation: FileGeneration::read(&path)?,
                verified_write_generation: writes.snapshot()?,
                writes,
            })
        };
        #[cfg(not(unix))]
        let append_only_prefix = None;

        self.state = Some(RunState {
            db: RunDatabase::ReadWrite(db),
            manifest,
            high_watermark: 0,
            max_ts_init: UnixNanos::default(),
            file_path: path,
            append_only_prefix,
            prefix_failed: Cell::new(false),
            #[cfg(test)]
            verified_suffix_reads: 0,
        });
        Ok(())
    }

    fn append_batch(&mut self, entries: &[AppendEntry]) -> Result<u64, EventStoreError> {
        let state = self.state_mut()?;

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        state.check_prefix_owner()?;
        let _storage_write = state.begin_owned_write()?;

        if entries.is_empty() {
            return Ok(state.high_watermark);
        }

        let entry_count = u64::try_from(entries.len())
            .map_err(|_| EventStoreError::Backend("native prefix batch count overflow".into()))?;
        let first = state
            .high_watermark
            .checked_add(1)
            .ok_or_else(|| EventStoreError::Backend("native prefix sequence exhausted".into()))?;
        state
            .high_watermark
            .checked_add(entry_count)
            .ok_or_else(|| {
                EventStoreError::Backend("native prefix batch sequence exhausted".into())
            })?;
        for (offset, append) in entries.iter().enumerate() {
            let offset = u64::try_from(offset)
                .map_err(|_| EventStoreError::Backend("native prefix offset overflow".into()))?;
            let expected = first.checked_add(offset).ok_or_else(|| {
                EventStoreError::Backend("native prefix sequence offset exhausted".into())
            })?;
            if append.entry.seq != expected {
                // Atomically rejected: surface the durable high-watermark, not the within-batch
                // validation cursor, so callers that resync from this value never skip entries
                // that were never committed.
                return Err(EventStoreError::OutOfOrder {
                    high_watermark: state.high_watermark,
                    seq: append.entry.seq,
                });
            }
        }

        if let Some(prefix) = &mut state.append_only_prefix {
            prefix.hashes.try_reserve(entries.len()).map_err(|e| {
                state.prefix_failed.set(true);
                EventStoreError::Backend(format!("native prefix capacity unavailable: {e}"))
            })?;
        }

        let encoded: Vec<Vec<u8>> = entries
            .iter()
            .map(|append| {
                codec::encode_to_vec(&append.entry).map_err(|e| {
                    EventStoreError::Backend(format!("encode entry seq={}: {e}", append.entry.seq))
                })
            })
            .collect::<Result<_, _>>()?;

        let db = state.db.read_write()?;
        let txn = begin_immediate_write(db)?;
        {
            let mut entries_table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
            let mut client_table = txn.open_table(CLIENT_ORDER_INDEX).map_err(map_table_err)?;
            let mut venue_table = txn.open_table(VENUE_ORDER_INDEX).map_err(map_table_err)?;

            for (append, bytes) in entries.iter().zip(encoded.iter()) {
                entries_table
                    .insert(append.entry.seq, bytes.as_slice())
                    .map_err(map_storage_err)?;

                for IndexKey { kind, key } in &append.index_keys {
                    let table = match kind {
                        IndexKind::ClientOrderId => &mut client_table,
                        IndexKind::VenueOrderId => &mut venue_table,
                    };
                    let already = table.get(key.as_str()).map_err(map_storage_err)?.is_some();

                    if !already {
                        table
                            .insert(key.as_str(), append.entry.seq)
                            .map_err(map_storage_err)?;
                    }
                }
            }
        }
        txn.commit().map_err(map_commit_err)?;

        let mut max_ts = state.max_ts_init;
        let mut new_hwm = state.high_watermark;

        for append in entries {
            if append.entry.ts_init > max_ts {
                max_ts = append.entry.ts_init;
            }
            new_hwm = append.entry.seq;
        }
        state.high_watermark = new_hwm;
        state.max_ts_init = max_ts;
        state.manifest.high_watermark = new_hwm;

        if state.append_only_prefix.is_some() {
            // No acknowledgment or cached hash is issued from the pre-commit draft.
            // Read the exact committed suffix once in one database transaction.
            let result = (|| {
                let txn = state.db.begin_read()?;
                let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
                let mut hashes = Vec::new();
                hashes.try_reserve(entries.len()).map_err(|e| {
                    EventStoreError::Backend(format!("native suffix capacity unavailable: {e}"))
                })?;
                for (append, expected_bytes) in entries.iter().zip(&encoded) {
                    let value = table
                        .get(append.entry.seq)
                        .map_err(map_storage_err)?
                        .ok_or_else(|| {
                            EventStoreError::Backend("native committed suffix absent".into())
                        })?;
                    let bytes = value.value();
                    if bytes != expected_bytes.as_slice() {
                        return Err(EventStoreError::Backend(
                            "native committed suffix bytes changed".into(),
                        ));
                    }
                    let actual =
                        codec::decode_from_slice::<EventStoreEntry>(bytes).map_err(|e| {
                            EventStoreError::Corrupted(format!("native committed suffix: {e}"))
                        })?;
                    check_embedded_seq(append.entry.seq, &actual)?;
                    if actual.recompute_hash() != actual.entry_hash {
                        return Err(EventStoreError::HashMismatch { seq: actual.seq });
                    }
                    hashes.push(actual.entry_hash);
                }
                Ok(hashes)
            })();
            match result {
                Ok(hashes) => {
                    #[cfg(test)]
                    {
                        state.verified_suffix_reads = state
                            .verified_suffix_reads
                            .checked_add(entry_count)
                            .ok_or_else(|| {
                                EventStoreError::Backend(
                                    "native suffix read counter exhausted".into(),
                                )
                            })?;
                    }
                    if let Some(prefix) = &mut state.append_only_prefix {
                        prefix.hashes.extend(hashes);
                    } else {
                        state.prefix_failed.set(true);
                        return Err(EventStoreError::Backend(
                            "native prefix owner disappeared".into(),
                        ));
                    }
                    state.refresh_prefix_generation()?;
                }
                Err(error) => {
                    state.prefix_failed.set(true);
                    return Err(error);
                }
            }
        }

        Ok(new_hwm)
    }

    fn scan_range(
        &self,
        from: u64,
        to: u64,
        direction: ScanDirection,
    ) -> Result<Vec<EventStoreEntry>, EventStoreError> {
        let state = self.state()?;

        if from > to || from == 0 || state.high_watermark == 0 {
            return Ok(Vec::new());
        }

        let lo = from;
        let hi = to.min(state.high_watermark);

        if lo > hi {
            return Ok(Vec::new());
        }

        let txn = state.db.begin_read()?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;

        // hi is capped to high_watermark above, so every seq in [lo, hi] is supposed to be
        // present. redb iterates only existing keys, so a missing row inside this range
        // means a committed sequence has been lost (corruption, external tampering); we
        // surface Gap rather than silently shortening the result.
        let mut out = Vec::new();
        let mut expected = lo;
        let iter = table.range(lo..=hi).map_err(map_storage_err)?;

        for row in iter {
            let (k, v) = row.map_err(map_storage_err)?;
            let seq = k.value();

            if seq != expected {
                return Err(EventStoreError::Gap {
                    prev: expected.saturating_sub(1),
                    next: seq,
                    missing: expected,
                });
            }
            let bytes = v.value();
            let entry = codec::decode_from_slice::<EventStoreEntry>(bytes)
                .map_err(|e| EventStoreError::Corrupted(format!("decode entry seq={seq}: {e}")))?;

            check_embedded_seq(seq, &entry)?;

            if entry.recompute_hash() != entry.entry_hash {
                return Err(EventStoreError::HashMismatch { seq });
            }
            out.push(entry);
            expected = seq + 1;
        }

        if expected <= hi {
            return Err(EventStoreError::Gap {
                prev: expected.saturating_sub(1),
                next: hi + 1,
                missing: expected,
            });
        }

        if matches!(direction, ScanDirection::Reverse) {
            out.reverse();
        }
        Ok(out)
    }

    fn scan_seq(&self, seq: u64) -> Result<Option<EventStoreEntry>, EventStoreError> {
        let state = self.state()?;

        if seq == 0 || seq > state.high_watermark {
            return Ok(None);
        }

        let txn = state.db.begin_read()?;
        let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
        let Some(value) = table.get(seq).map_err(map_storage_err)? else {
            // seq is inside the watermark per the guard above, so the row must exist;
            // its absence is a committed-but-missing entry.
            return Err(EventStoreError::Gap {
                prev: seq.saturating_sub(1),
                next: seq + 1,
                missing: seq,
            });
        };

        let bytes = value.value();
        let entry = codec::decode_from_slice::<EventStoreEntry>(bytes)
            .map_err(|e| EventStoreError::Corrupted(format!("decode entry seq={seq}: {e}")))?;

        check_embedded_seq(seq, &entry)?;

        if entry.recompute_hash() != entry.entry_hash {
            return Err(EventStoreError::HashMismatch { seq });
        }
        Ok(Some(entry))
    }

    fn verified_append_only_entry_hashes(&self) -> Result<Option<&[EntryHash]>, EventStoreError> {
        let state = self.state()?;
        state.check_prefix_owner()?;
        let Some(prefix) = &state.append_only_prefix else {
            return Ok(None);
        };
        let result = (|| {
            let manifest = Self::read_manifest(state.db.readable())?
                .ok_or_else(|| EventStoreError::Backend("native prefix manifest absent".into()))?;
            if manifest.run_id != prefix.run_id || manifest.status != state.manifest.status {
                return Err(EventStoreError::Backend(
                    "native prefix durable run changed".into(),
                ));
            }
            let txn = state.db.begin_read()?;
            let table = txn.open_table(ENTRIES_TABLE).map_err(map_table_err)?;
            let actual_hwm = table
                .last()
                .map_err(map_storage_err)?
                .map_or(0, |(key, _)| key.value());
            if actual_hwm != state.high_watermark {
                return Err(EventStoreError::Backend(
                    "native prefix durable watermark changed".into(),
                ));
            }
            // The expected generation is checked again after the actual backend reads.
            state.check_prefix_owner()?;
            Ok(Some(prefix.hashes.as_slice()))
        })();
        if result.is_err() {
            state.prefix_failed.set(true);
        }
        result
    }

    fn lookup(&self, kind: IndexKind, key: &str) -> Result<Option<u64>, EventStoreError> {
        let state = self.state()?;
        let txn = state.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        let value = table.get(key).map_err(map_storage_err)?;
        Ok(value.map(|v| v.value()))
    }

    fn iter_index_keys(&self, kind: IndexKind) -> Result<Vec<(String, u64)>, EventStoreError> {
        let state = self.state()?;
        let txn = state.db.begin_read()?;
        let definition = match kind {
            IndexKind::ClientOrderId => CLIENT_ORDER_INDEX,
            IndexKind::VenueOrderId => VENUE_ORDER_INDEX,
        };
        let table = txn.open_table(definition).map_err(map_table_err)?;
        let iter = table.iter().map_err(map_storage_err)?;
        let mut out = Vec::new();

        for row in iter {
            let (k, v) = row.map_err(map_storage_err)?;
            out.push((k.value().to_string(), v.value()));
        }
        Ok(out)
    }

    fn record_snapshot_anchor(&mut self, anchor: SnapshotAnchor) -> Result<(), EventStoreError> {
        let state = self.state_mut()?;

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        state.check_prefix_owner()?;
        let _storage_write = state.begin_owned_write()?;

        let latest = Self::read_snapshot_anchor(state.db.readable())?;
        validate_new_anchor(&anchor, state.high_watermark, latest.as_ref())?;

        let bytes = codec::encode_to_vec(&anchor)
            .map_err(|e| EventStoreError::Backend(format!("encode snapshot anchor: {e}")))?;
        let db = state.db.read_write()?;
        let txn = begin_immediate_write(db)?;
        {
            let mut table = txn
                .open_table(SNAPSHOT_ANCHOR_TABLE)
                .map_err(map_table_err)?;
            table
                .insert(SNAPSHOT_ANCHOR_KEY, bytes.as_slice())
                .map_err(map_storage_err)?;
        }
        txn.commit().map_err(map_commit_err)?;
        state.refresh_prefix_generation()?;
        Ok(())
    }

    fn latest_snapshot_anchor(&self) -> Result<Option<SnapshotAnchor>, EventStoreError> {
        Self::read_snapshot_anchor(self.state()?.db.readable())
    }

    fn seal(&mut self, status: RunStatus) -> Result<(), EventStoreError> {
        let state = self.state_mut()?;
        state.check_prefix_owner()?;
        let _storage_write = state.begin_owned_write()?;

        // Running is not a terminal state; accepting it would leave `is_sealed()` returning
        // false while the seal call returned Ok, so subsequent appends would not see Closed.
        if matches!(status, RunStatus::Running) {
            return Err(EventStoreError::Backend(
                "seal status must be a terminal state, was Running".to_string(),
            ));
        }

        if state.manifest.is_sealed() {
            return Err(EventStoreError::Closed);
        }

        let mut updated = state.manifest.clone();
        updated.status = status;
        updated.high_watermark = state.high_watermark;

        if state.high_watermark > 0 {
            updated.end_ts_init = Some(state.max_ts_init);
        }

        Self::write_manifest(state.db.read_write()?, &updated)?;
        state.refresh_prefix_generation()?;
        state.manifest = updated;
        // Sealed readers independently scan every original payload; no live cache
        // is exported or reused as a reader-issued proof.
        state.append_only_prefix = None;
        Ok(())
    }

    fn manifest(&self) -> Result<RunManifest, EventStoreError> {
        Ok(self.state()?.manifest.clone())
    }

    fn high_watermark(&self) -> Result<u64, EventStoreError> {
        Ok(self.state()?.high_watermark)
    }
}

fn missing_manifest(path: &Path) -> EventStoreError {
    EventStoreError::Corrupted(format!(
        "missing manifest in run file at {}",
        path.display()
    ))
}

// The entry hash covers the embedded seq, not the table key, so a moved or
// duplicated row still hashes correctly; both read paths refuse it here.
fn check_embedded_seq(seq: u64, entry: &EventStoreEntry) -> Result<(), EventStoreError> {
    if entry.seq != seq {
        return Err(EventStoreError::SeqMismatch {
            table_key: seq,
            embedded_seq: entry.seq,
        });
    }
    Ok(())
}

fn begin_immediate_write(db: &Database) -> Result<WriteTransaction, EventStoreError> {
    let mut txn = db.begin_write().map_err(map_transaction_err)?;
    txn.set_durability(Durability::Immediate)
        .map_err(|e| EventStoreError::Backend(format!("set durability: {e}")))?;
    Ok(txn)
}

fn insert_run_manifest(
    txn: &WriteTransaction,
    manifest: &RunManifest,
) -> Result<(), EventStoreError> {
    let bytes = codec::encode_to_vec(manifest)
        .map_err(|e| EventStoreError::Backend(format!("encode manifest: {e}")))?;
    let mut table = txn.open_table(MANIFEST_TABLE).map_err(map_table_err)?;
    table
        .insert(MANIFEST_KEY, bytes.as_slice())
        .map_err(map_storage_err)?;
    Ok(())
}

fn map_storage_err(err: StorageError) -> EventStoreError {
    match err {
        StorageError::Io(io_err) if is_disk_pressure(io_err.kind()) => {
            EventStoreError::Disk(io_err.to_string())
        }
        StorageError::Corrupted(msg) => EventStoreError::Corrupted(msg),
        other => EventStoreError::Backend(other.to_string()),
    }
}

// `EventStoreError::Disk` documents ENOSPC and `RLIMIT_FSIZE` as its targets. On the
// stable toolchain `ENOSPC` surfaces as `StorageFull`, `RLIMIT_FSIZE`/`EFBIG` as
// `FileTooLarge`, and `EDQUOT` as `QuotaExceeded`; the kernel halt path keys off
// `Disk`, so all three must classify the same way.
fn is_disk_pressure(kind: ErrorKind) -> bool {
    matches!(
        kind,
        ErrorKind::FileTooLarge | ErrorKind::StorageFull | ErrorKind::QuotaExceeded
    )
}

fn map_database_err(err: DatabaseError) -> EventStoreError {
    match err {
        DatabaseError::RepairAborted => EventStoreError::Corrupted(
            "database requires repair and cannot be verified read-only".to_string(),
        ),
        DatabaseError::UpgradeRequired(version) => EventStoreError::Corrupted(format!(
            "database file format version {version} requires manual upgrade",
        )),
        DatabaseError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

fn map_read_only_database_err(err: DatabaseError) -> EventStoreError {
    match err {
        DatabaseError::Storage(StorageError::Io(io_err)) if is_corrupt_read(io_err.kind()) => {
            EventStoreError::Corrupted(format!("read-only open failed: {io_err}"))
        }
        other => map_database_err(other),
    }
}

fn is_corrupt_read(kind: ErrorKind) -> bool {
    matches!(kind, ErrorKind::UnexpectedEof | ErrorKind::InvalidData)
}

fn map_table_err(err: TableError) -> EventStoreError {
    // Mirror redb's own classification: schema-shape failures (missing table, type
    // mismatch, definition drift) are structural corruption, not generic backend
    // errors. Programmer-error variants (`TableAlreadyOpen`, `TableExists`) stay
    // Backend so they surface as bugs rather than quarantine triggers.
    match err {
        TableError::Storage(storage) => map_storage_err(storage),
        TableError::TableDoesNotExist(_)
        | TableError::TableTypeMismatch { .. }
        | TableError::TableIsMultimap(_)
        | TableError::TableIsNotMultimap(_)
        | TableError::TypeDefinitionChanged { .. } => EventStoreError::Corrupted(err.to_string()),
        other => EventStoreError::Backend(other.to_string()),
    }
}

fn map_commit_err(err: CommitError) -> EventStoreError {
    match err {
        CommitError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

fn is_run_file(path: &Path) -> bool {
    path.extension().and_then(|s| s.to_str()) == Some("redb")
        && path
            .file_name()
            .and_then(|s| s.to_str())
            .is_none_or(|name| !name.ends_with(".markers.redb"))
}

fn map_transaction_err(err: TransactionError) -> EventStoreError {
    match err {
        TransactionError::Storage(storage) => map_storage_err(storage),
        other => EventStoreError::Backend(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use tempfile::TempDir;

    use super::*;

    #[cfg(unix)]
    fn prefix_manifest(run_id: &str) -> RunManifest {
        RunManifest {
            run_id: run_id.into(),
            parent_run_id: None,
            instance_id: "prefix-owner".into(),
            binary_hash: "native-binary".into(),
            schema_version: 1,
            crate_versions: "native-source".into(),
            feature_flags: Vec::new(),
            adapter_versions: Default::default(),
            config_hash: "frozen-config".into(),
            registered_components: Default::default(),
            seed: None,
            start_ts_init: UnixNanos::from(0),
            end_ts_init: None,
            high_watermark: 0,
            status: RunStatus::Running,
        }
    }

    #[cfg(unix)]
    fn prefix_entry(seq: u64) -> AppendEntry {
        let headers = crate::headers::Headers::empty();
        let topic: crate::entry::Topic = "events.native.prefix".into();
        let payload_type = ustr::Ustr::from("NativePrefixOriginal.v1");
        let payload =
            bytes::Bytes::from(vec![u8::try_from(seq).expect("small fixture"); 128 * 1024]);
        let ts = UnixNanos::from(seq);
        let hash = crate::hash::compute_entry_hash(
            seq,
            ts,
            ts,
            topic.as_ref(),
            payload_type.as_str(),
            &payload,
            &headers,
        );
        AppendEntry::without_indices(EventStoreEntry::new(
            hash,
            seq,
            headers,
            topic,
            payload_type,
            payload,
            ts,
            ts,
        ))
    }

    #[cfg(unix)]
    fn original_full_prefix(backend: &dyn EventStore) -> crate::writer::DurableJournalPrefix {
        let run_id = backend.manifest().expect("manifest").run_id;
        let sequence = backend.high_watermark().expect("high watermark");
        let mut digest = blake3::Hasher::new();
        digest.update(b"nautilus-native-journal-prefix/v1");
        digest.update(&(run_id.len() as u64).to_be_bytes());
        digest.update(run_id.as_bytes());
        digest.update(&sequence.to_be_bytes());
        for seq in 1..=sequence {
            let row = backend
                .scan_seq(seq)
                .expect("full original read")
                .expect("row");
            assert_eq!(row.recompute_hash(), row.entry_hash);
            digest.update(row.entry_hash.as_bytes());
        }
        crate::writer::DurableJournalPrefix {
            run_id,
            sequence,
            entry_hash_digest: digest.finalize().to_hex().to_string(),
        }
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_real_redb_reads_only_committed_suffix_and_preserves_v1_digest() {
        let tmp = TempDir::new().expect("tempdir");
        let mut backend = RedbBackend::new(tmp.path());
        backend
            .open_run(prefix_manifest("multi-batch"))
            .expect("open");
        assert_eq!(
            crate::writer::read_durable_prefix(&backend).expect("empty prefix"),
            original_full_prefix(&backend)
        );
        for batch in [
            vec![prefix_entry(1), prefix_entry(2)],
            vec![prefix_entry(3)],
            vec![prefix_entry(4), prefix_entry(5)],
        ] {
            backend.append_batch(&batch).expect("actual durable commit");
            let watermark = backend.high_watermark().expect("hwm");
            for _ in 0..4 {
                assert_eq!(
                    crate::writer::read_durable_prefix(&backend).expect("same cut prefix"),
                    original_full_prefix(&backend)
                );
            }
            assert_eq!(
                backend.state().expect("state").verified_suffix_reads,
                watermark,
                "prefix requests must not reread old payloads"
            );
            backend
                .record_snapshot_anchor(SnapshotAnchor::new(watermark, "real-cut", "cut-hash"))
                .expect("anchor");
            assert_eq!(
                crate::writer::read_durable_prefix(&backend).expect("anchor generation"),
                original_full_prefix(&backend)
            );
        }
        let expected = original_full_prefix(&backend);
        backend.seal(RunStatus::Ended).expect("seal");
        assert!(
            backend
                .verified_append_only_entry_hashes()
                .expect("sealed fallback")
                .is_none()
        );
        drop(backend);
        let reader = RedbBackend::open_sealed(tmp.path(), "prefix-owner", "multi-batch")
            .expect("sealed original");
        assert!(
            reader
                .verified_append_only_entry_hashes()
                .expect("reopened fallback")
                .is_none()
        );
        assert_eq!(
            crate::writer::read_durable_prefix(&reader).expect("all original rows verified again"),
            expected
        );
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_existing_redb_never_inherits_process_cache() {
        let tmp = TempDir::new().expect("tempdir");
        let expected = {
            let mut original = RedbBackend::new(tmp.path());
            original.open_run(prefix_manifest("crashed")).expect("open");
            original
                .append_batch(&[prefix_entry(1), prefix_entry(2)])
                .expect("commit");
            original_full_prefix(&original)
        };
        let mut reopened = RedbBackend::new(tmp.path());
        assert!(matches!(
            reopened.open_run(prefix_manifest("crashed")),
            Err(EventStoreError::CrashedPredecessor)
        ));
        assert!(
            reopened
                .verified_append_only_entry_hashes()
                .expect("existing fallback")
                .is_none()
        );
        assert_eq!(
            crate::writer::read_durable_prefix(&reopened).expect("full existing rows"),
            expected
        );
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_old_payload_change_cannot_be_hidden_by_legal_commit_anchor_or_seal() {
        for action in ["append", "anchor", "seal", "prefix"] {
            let tmp = TempDir::new().expect("tempdir");
            let mut backend = RedbBackend::new(tmp.path());
            backend.open_run(prefix_manifest(action)).expect("open");
            backend
                .append_batch(&[prefix_entry(1)])
                .expect("original commit");
            let mut tampered = prefix_entry(1).entry;
            tampered.payload = bytes::Bytes::from_static(b"changed old payload");
            let bytes = codec::encode_to_vec(&tampered).expect("encode changed actual row");
            // Deliberately bypass the sole legal append path using the owned test
            // database handle. Its unexpected generation must be refused before
            // any later legal commit could refresh the expected generation.
            {
                let state = backend.state().expect("state");
                let txn =
                    begin_immediate_write(state.db.read_write().expect("owned db")).expect("write");
                {
                    let mut table = txn.open_table(ENTRIES_TABLE).expect("table");
                    table
                        .insert(1, bytes.as_slice())
                        .expect("old payload change");
                }
                txn.commit().expect("actual unexpected commit");
            }
            let result = match action {
                "append" => backend.append_batch(&[prefix_entry(2)]).map(|_| ()),
                "anchor" => backend.record_snapshot_anchor(SnapshotAnchor::new(1, "cut", "hash")),
                "seal" => backend.seal(RunStatus::Ended),
                _ => crate::writer::read_durable_prefix(&backend).map(|_| ()),
            };
            assert!(result.is_err(), "{action} must reject changed old storage");
            assert!(backend.state().expect("state").prefix_failed.get());
            assert!(
                backend.append_batch(&[prefix_entry(2)]).is_err(),
                "failure must remain latched"
            );
            assert_eq!(backend.high_watermark().expect("hwm"), 1);
            assert_eq!(
                backend.manifest().expect("manifest").status,
                RunStatus::Running
            );
            assert!(matches!(
                backend.scan_seq(1),
                Err(EventStoreError::HashMismatch { seq: 1 })
            ));
        }
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_same_length_rehashed_old_payload_is_not_laundered_by_owned_commit() {
        for action in ["append", "anchor", "seal", "prefix"] {
            let tmp = TempDir::new().expect("tempdir");
            let mut backend = RedbBackend::new(tmp.path());
            backend.open_run(prefix_manifest(action)).expect("open");
            backend
                .append_batch(&[prefix_entry(1)])
                .expect("original commit");
            let mut changed = prefix_entry(1).entry;
            changed.payload = bytes::Bytes::from(vec![0x7f; changed.payload.len()]);
            changed.entry_hash = changed.recompute_hash();
            let encoded = codec::encode_to_vec(&changed).expect("valid same-length changed row");
            {
                let state = backend.state().expect("state");
                let transaction = begin_immediate_write(state.db.read_write().expect("db"))
                    .expect("unexpected actual write transaction");
                {
                    let mut table = transaction.open_table(ENTRIES_TABLE).expect("table");
                    let original = table.get(1).expect("old row").expect("existing");
                    assert_eq!(original.value().len(), encoded.len());
                    drop(original);
                    table
                        .insert(1, encoded.as_slice())
                        .expect("replace same length");
                }
                transaction
                    .commit()
                    .expect("actual unexpected durable commit");
            }
            assert_eq!(
                backend.scan_seq(1).expect("independent self-hash scan"),
                Some(changed)
            );
            let result = match action {
                "append" => backend.append_batch(&[prefix_entry(2)]).map(|_| ()),
                "anchor" => backend.record_snapshot_anchor(SnapshotAnchor::new(1, "cut", "hash")),
                "seal" => backend.seal(RunStatus::Ended),
                _ => crate::writer::read_durable_prefix(&backend).map(|_| ()),
            };
            let error = result.expect_err("valid rehash must not certify changed old bytes");
            assert!(
                error
                    .to_string()
                    .contains("native storage writes changed or failed")
            );
            assert!(backend.state().expect("state").prefix_failed.get());
            assert_eq!(backend.high_watermark().expect("hwm"), 1);
            assert_eq!(
                backend.manifest().expect("manifest").status,
                RunStatus::Running
            );
            assert!(backend.append_batch(&[prefix_entry(2)]).is_err());
        }
    }

    #[test]
    fn native_storage_write_epoch_rejects_concurrent_unowned_io_before_refresh() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("owned-io");
        fs::write(&path, b"original").expect("owned file");
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect("file");
        let epoch = Arc::new(StorageWriteEpoch::default());
        epoch.armed.store(true, Ordering::SeqCst);
        let storage = Arc::new(OwnedStorageBackend {
            inner: FileBackend::new(file).expect("same locked FileBackend"),
            epoch: epoch.clone(),
        });
        let _permit = epoch.begin_owned(0).expect("legal current-thread scope");
        let concurrent = storage.clone();
        std::thread::spawn(move || concurrent.write(0, b"changed!"))
            .join()
            .expect("owned thread")
            .expect("actual other-thread IO");
        let mut actual = [0; 8];
        storage.read(0, &mut actual).expect("actual storage read");
        assert_eq!(&actual, b"changed!");
        assert_eq!(epoch.generation.load(Ordering::SeqCst), 1);
        assert!(
            epoch.snapshot().is_err(),
            "legal scope cannot launder unowned write"
        );
    }

    #[test]
    fn native_storage_epoch_overflow_refuses_io_and_failed_resize_write_remain_visible() {
        let tmp = TempDir::new().expect("tempdir");
        let path = tmp.path().join("overflow-io");
        fs::write(&path, b"original").expect("owned file");
        let epoch = Arc::new(StorageWriteEpoch::default());
        epoch.generation.store(u64::MAX, Ordering::SeqCst);
        let storage = OwnedStorageBackend {
            inner: FileBackend::new(
                fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(&path)
                    .expect("file"),
            )
            .expect("locked FileBackend"),
            epoch: epoch.clone(),
        };
        assert!(storage.write(0, b"changed!").is_err());
        assert!(storage.set_len(0).is_err());
        assert_eq!(fs::read(&path).expect("original remains"), b"original");
        assert!(epoch.snapshot().is_err());
        drop(storage);
        let readonly = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("read-only descriptor");
        let failed = Arc::new(StorageWriteEpoch::default());
        let storage = OwnedStorageBackend {
            inner: FileBackend::new(readonly).expect("same FileBackend lock"),
            epoch: failed.clone(),
        };
        assert!(storage.write(0, b"changed!").is_err());
        assert!(storage.set_len(0).is_err());
        assert_eq!(
            failed.generation.load(Ordering::SeqCst),
            2,
            "failed/partial IO advances before delegation, never an invisible write"
        );
        assert!(failed.snapshot().is_err());
        assert_eq!(fs::read(&path).expect("original remains"), b"original");
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_bad_committed_suffix_never_mints_ack_or_advances_verified_hashes() {
        let tmp = TempDir::new().expect("tempdir");
        let mut backend = RedbBackend::new(tmp.path());
        backend
            .open_run(prefix_manifest("bad-suffix"))
            .expect("open");
        backend
            .append_batch(&[prefix_entry(1)])
            .expect("first commit");
        let mut invalid = prefix_entry(2);
        invalid.entry.payload = bytes::Bytes::from_static(b"bad committed checksum");
        assert!(matches!(
            backend.append_batch(&[invalid]),
            Err(EventStoreError::HashMismatch { seq: 2 })
        ));
        assert_eq!(
            backend
                .high_watermark()
                .expect("committed but not acknowledged"),
            2
        );
        assert_eq!(
            backend
                .state()
                .expect("state")
                .append_only_prefix
                .as_ref()
                .expect("cache")
                .hashes
                .len(),
            1
        );
        assert!(crate::writer::read_durable_prefix(&backend).is_err());
        assert!(backend.append_batch(&[prefix_entry(3)]).is_err());
        assert!(backend.seal(RunStatus::Ended).is_err());
        assert_eq!(
            backend.manifest().expect("retained").status,
            RunStatus::Running
        );
    }

    #[test]
    #[cfg(unix)]
    fn native_prefix_unknown_backend_keeps_full_original_payload_verification() {
        let mut backend = crate::backend::MemoryBackend::new();
        backend.open_run(prefix_manifest("unknown")).expect("open");
        backend.append_batch(&[prefix_entry(1)]).expect("commit");
        assert!(
            backend
                .verified_append_only_entry_hashes()
                .expect("no opt in")
                .is_none()
        );
        assert_eq!(
            crate::writer::read_durable_prefix(&backend).expect("full original prefix"),
            original_full_prefix(&backend)
        );
        let mut invalid = prefix_entry(2);
        invalid.entry.payload = bytes::Bytes::from_static(b"invalid unknown source");
        backend
            .append_batch(&[invalid])
            .expect("unknown backend retains its original append contract");
        assert!(
            crate::writer::read_durable_prefix(&backend).is_err(),
            "default backend cannot bypass payload verification"
        );
    }

    #[test]
    #[cfg(all(unix, not(madsim)))]
    fn native_prefix_failed_durable_suffix_fences_actual_writer_and_retains_unsealed_redb() {
        let tmp = TempDir::new().expect("tempdir");
        let mut backend = RedbBackend::new(tmp.path());
        backend
            .open_run(prefix_manifest("failed-writer"))
            .expect("open");
        let path = backend.current_path().expect("path").to_path_buf();
        let mut invalid = prefix_entry(1);
        invalid.entry.payload = bytes::Bytes::from_static(b"changed before durable suffix read");
        assert!(backend.append_batch(&[invalid]).is_err());
        let halt = crate::kernel::HaltSignal::new();
        let writer = crate::writer::EventStoreWriter::spawn(
            Box::new(backend),
            nautilus_core::time::get_atomic_clock_realtime(),
            halt.callback(),
            crate::writer::WriterConfig::default(),
        )
        .expect("actual owner writer");
        assert_eq!(
            crate::writer::WriterConfig::default().halt_threshold,
            std::time::Duration::from_millis(250)
        );
        assert!(
            writer.flush().is_err(),
            "an empty flush must not acknowledge failed integrity"
        );
        assert!(halt.is_halted());
        assert!(writer.durable_prefix().is_err());
        drop(writer);
        assert_eq!(
            RedbBackend::read_run_manifest(&path)
                .expect("original manifest")
                .status,
            RunStatus::Running
        );
    }

    fn raw_run_path(base: &Path, run_id: &str) -> PathBuf {
        let dir = base.join("trader-001");
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir.join(format!("{run_id}.redb"))
    }

    fn create_pre_codec_run_file(path: &Path) {
        let entries: TableDefinition<u64, &[u8]> = TableDefinition::new("entries");
        let manifest: TableDefinition<&str, &[u8]> = TableDefinition::new("manifest");
        let db = Database::create(path).expect("create redb");
        let txn = db.begin_write().expect("begin write");
        {
            txn.open_table(entries).expect("open entries");
            let mut table = txn.open_table(manifest).expect("open manifest");
            table
                .insert("current", b"old-format".as_slice())
                .expect("insert");
        }
        txn.commit().expect("commit");
    }

    #[rstest]
    fn read_run_manifest_rejects_store_without_format_marker() {
        let tmp = TempDir::new().expect("tempdir");
        let path = raw_run_path(tmp.path(), "run-old-format");
        create_pre_codec_run_file(&path);

        let err = RedbBackend::read_run_manifest(&path).expect_err("must reject old format");

        match err {
            EventStoreError::Corrupted(msg) => {
                assert!(msg.contains("regenerated"), "msg was: {msg}");
            }
            other => panic!("expected Corrupted, was {other:?}"),
        }
    }

    #[rstest]
    #[case::file_too_large(ErrorKind::FileTooLarge, true)]
    #[case::storage_full(ErrorKind::StorageFull, true)]
    #[case::quota_exceeded(ErrorKind::QuotaExceeded, true)]
    #[case::other(ErrorKind::Other, false)]
    #[case::not_found(ErrorKind::NotFound, false)]
    #[case::permission_denied(ErrorKind::PermissionDenied, false)]
    #[case::interrupted(ErrorKind::Interrupted, false)]
    fn is_disk_pressure_matches_documented_kinds(#[case] kind: ErrorKind, #[case] expected: bool) {
        assert_eq!(is_disk_pressure(kind), expected);
    }

    #[rstest]
    fn map_storage_err_classifies_disk_pressure_as_disk() {
        let io_err = std::io::Error::from(ErrorKind::StorageFull);
        let mapped = map_storage_err(StorageError::Io(io_err));

        match mapped {
            EventStoreError::Disk(_) => {}
            other => panic!("expected Disk, was {other:?}"),
        }
    }

    #[rstest]
    fn map_storage_err_classifies_corrupted_as_corrupted() {
        let mapped = map_storage_err(StorageError::Corrupted("boom".to_string()));

        match mapped {
            EventStoreError::Corrupted(msg) => assert!(msg.contains("boom")),
            other => panic!("expected Corrupted, was {other:?}"),
        }
    }

    #[rstest]
    fn map_storage_err_falls_back_to_backend_for_unrelated_io() {
        let io_err = std::io::Error::from(ErrorKind::PermissionDenied);
        let mapped = map_storage_err(StorageError::Io(io_err));

        match mapped {
            EventStoreError::Backend(_) => {}
            other => panic!("expected Backend, was {other:?}"),
        }
    }
}
