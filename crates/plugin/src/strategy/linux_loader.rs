// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! GNU loader for already-published trusted algorithms. Hash/ELF checks and
//! sealed backing bytes precede constructors. Handles and descriptor numbers
//! are retained for process lifetime: reusing a `/proc/self/fd/N` pathname could
//! otherwise make the dynamic loader return an older library under that name.

use std::{
    collections::BTreeMap,
    ffi::{CStr, CString, c_void},
    fs::File,
    io::Write,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{FileExt, MetadataExt},
    },
    ptr::NonNull,
};

use sha2::{Digest, Sha256};

use super::{
    STRATEGY_INIT_SYMBOL, StrategyApi,
    elf::ElfLibrary,
    instance::{CallbackLimits, GuestInstance},
};
use crate::{HostVTable, PluginError, PluginErrorCode, PluginInitFn, PluginManifest};

static METADATA_TOKEN: u8 = 0;
type StrategyInitFn = unsafe extern "C" fn() -> *const StrategyApi;

#[derive(Debug)]
struct Pinned {
    _handle: NonNull<c_void>,
    _backing: &'static File,
    api: *const StrategyApi,
    metadata: &'static PluginManifest,
    elf: ElfLibrary,
}

/// A library whose allocation functions and callbacks remain live until exit.
#[derive(Debug, Clone, Copy)]
pub struct LoadedLibrary {
    pinned: &'static Pinned,
}

impl LoadedLibrary {
    /// Returns the actual loaded metadata, without granting execution authority.
    #[must_use]
    pub const fn metadata(&self) -> &'static PluginManifest {
        self.pinned.metadata
    }

    /// Returns the pre-constructor ELF dependency facts.
    #[must_use]
    pub const fn elf(&self) -> &ElfLibrary {
        &self.pinned.elf
    }

    /// Creates one instance after the business owner has admitted its exact
    /// descriptor, typed configuration, qualification and resource limits.
    ///
    /// # Errors
    ///
    /// Refuses incompatible metadata, invalid configuration or guest failure.
    pub fn create_instance(
        &self,
        descriptor: &[u8],
        configuration: &[u8],
        limits: CallbackLimits,
    ) -> Result<GuestInstance, PluginError> {
        // SAFETY: the trusted publication contract and pinned loaded library
        // establish the process-long executable table's layout and lifetime.
        unsafe { GuestInstance::create(self.pinned.api, descriptor, configuration, limits) }
    }
}

/// Owned by the one native host. Capacity comes from its fixed server profile,
/// never from a guest manifest. Libraries are shared across configured instances.
#[derive(Debug)]
pub struct LibraryRegistry {
    libraries: BTreeMap<[u8; 32], LoadedLibrary>,
    failures: BTreeMap<[u8; 32], (PluginErrorCode, String)>,
    maximum_libraries: usize,
    maximum_library_bytes: usize,
}

fn invalid(message: impl AsRef<str>) -> PluginError {
    PluginError::new(PluginErrorCode::InvalidArgument, message)
}

fn io_error(error: &std::io::Error) -> PluginError {
    PluginError::generic(error.to_string())
}

impl LibraryRegistry {
    /// Constructs the host-owned bounded library registry.
    ///
    /// # Errors
    ///
    /// Refuses zero or above-transport capacities.
    pub fn new(
        maximum_libraries: usize,
        maximum_library_bytes: usize,
    ) -> Result<Self, PluginError> {
        if maximum_libraries == 0
            || maximum_libraries > 1024
            || !(64..=536_870_912).contains(&maximum_library_bytes)
        {
            return Err(invalid("invalid native library capacity"));
        }
        Ok(Self {
            libraries: BTreeMap::new(),
            failures: BTreeMap::new(),
            maximum_libraries,
            maximum_library_bytes,
        })
    }

    /// Loads the exact admitted bytes under the unprivileged trading identity.
    /// No library is loaded by an administrator or a metadata inspection command.
    ///
    /// # Errors
    ///
    /// Refuses privileged callers, modified/untrusted files, loader overrides,
    /// incompatible ELF, capacity, sealing or dynamic-loader failures.
    ///
    /// # Safety
    ///
    /// The caller has verified a managed publication, source, producer receipt,
    /// constructor conformance and dependency provenance for `expected_sha256`.
    /// The library is trusted native code implementing the documented generated
    /// metadata/component tables. Hash equality alone does not confer this trust.
    pub unsafe fn load_admitted(
        &mut self,
        source: &File,
        expected_sha256: [u8; 32],
        publisher_uid: u32,
    ) -> Result<LoadedLibrary, PluginError> {
        // SAFETY: geteuid has no pointer or ownership preconditions
        if unsafe { libc::geteuid() } == 0 {
            return Err(invalid(
                "administrator processes cannot load algorithm code",
            ));
        }
        if std::env::vars_os().any(|(name, _)| {
            let name = name.to_string_lossy();
            name.starts_with("LD_") || name == "GLIBC_TUNABLES"
        }) {
            return Err(invalid("native algorithm loader environment is overridden"));
        }
        if let Some(library) = self.libraries.get(&expected_sha256) {
            return Ok(*library);
        }
        if let Some((code, message)) = self.failures.get(&expected_sha256) {
            return Err(PluginError::new(*code, message));
        }
        if self.libraries.len() + self.failures.len() >= self.maximum_libraries {
            return Err(invalid("native library capacity exceeded"));
        }
        // SAFETY: the admitted publisher contract is inherited from this call;
        // successful and failed loads are both bounded and never retried here.
        let result = unsafe {
            load_file(
                source,
                expected_sha256,
                publisher_uid,
                self.maximum_library_bytes,
            )
        };
        match &result {
            Ok(library) => {
                self.libraries.insert(expected_sha256, *library);
            }
            Err(error) => {
                self.failures
                    .insert(expected_sha256, (error.code, error.message_string()));
            }
        }
        result
    }
}

unsafe fn load_file(
    source: &File,
    expected_sha256: [u8; 32],
    publisher_uid: u32,
    maximum_library_bytes: usize,
) -> Result<LoadedLibrary, PluginError> {
    let metadata = source.metadata().map_err(|error| io_error(&error))?;
    if !metadata.is_file()
        || metadata.uid() != publisher_uid
        || metadata.mode() & 0o222 != 0
        || metadata.len()
            > u64::try_from(maximum_library_bytes)
                .map_err(|_| invalid("native library capacity overflow"))?
    {
        return Err(invalid(
            "native library publication file is untrusted or oversized",
        ));
    }
    let length =
        usize::try_from(metadata.len()).map_err(|_| invalid("native library size overflow"))?;
    let mut bytes = vec![0; length];
    let mut read = 0;
    while read < bytes.len() {
        let offset =
            u64::try_from(read).map_err(|_| invalid("native library read offset overflow"))?;
        let count = source
            .read_at(&mut bytes[read..], offset)
            .map_err(|error| io_error(&error))?;
        if count == 0 {
            return Err(invalid("native library shrank while reading"));
        }
        read += count;
    }
    let actual: [u8; 32] = Sha256::digest(&bytes).into();
    if actual != expected_sha256 {
        return Err(invalid("native library differs from admitted SHA-256"));
    }
    let elf = ElfLibrary::inspect(&bytes)?;
    let backing = sealed_backing(&bytes)?;
    let path = CString::new(format!("/proc/self/fd/{}", backing.as_raw_fd()))
        .map_err(|_| invalid("invalid sealed library descriptor"))?;
    // SAFETY: the path is a live sealed FD to pre-inspected trusted bytes;
    // flags request immediate local binding and permanent loader retention.
    let handle = unsafe {
        libc::dlopen(
            path.as_ptr(),
            libc::RTLD_NOW | libc::RTLD_LOCAL | libc::RTLD_NODELETE,
        )
    };
    let handle = NonNull::new(handle).ok_or_else(loader_error)?;

    // Pin the backing FD immediately, including later entry/metadata failure.
    // Constructor-retained references cannot be invalidated by failure cleanup.
    let backing = Box::leak(Box::new(backing));
    // SAFETY: live dlopen handle and a NUL-terminated metadata symbol
    let metadata_symbol = unsafe { libc::dlsym(handle.as_ptr(), c"nautilus_plugin_init".as_ptr()) };
    if metadata_symbol.is_null() {
        return Err(loader_error());
    }
    // SAFETY: the managed publisher guarantees the generated entry signature
    let metadata_init: PluginInitFn = unsafe { std::mem::transmute(metadata_symbol) };
    // SAFETY: HostVTable is an opaque zero-sized metadata token, not an
    // executable service table or an execution-authority object.
    let metadata = unsafe { metadata_init((&raw const METADATA_TOKEN).cast::<HostVTable>()) };
    // SAFETY: the generated entry returns valid process-lifetime metadata
    let metadata =
        unsafe { metadata.as_ref() }.ok_or_else(|| invalid("null loaded plugin metadata"))?;
    metadata
        .validate()
        .map_err(|error| invalid(error.to_string()))?;
    let symbol = CStr::from_bytes_with_nul(STRATEGY_INIT_SYMBOL)
        .map_err(|_| invalid("invalid strategy entry symbol"))?;
    // SAFETY: live pinned handle and a NUL-terminated strategy symbol
    let executable = unsafe { libc::dlsym(handle.as_ptr(), symbol.as_ptr()) };
    if executable.is_null() {
        return Err(loader_error());
    }
    // SAFETY: the admitted producer guarantees the versioned entry signature
    let init: StrategyInitFn = unsafe { std::mem::transmute(executable) };
    // SAFETY: trusted generated entry in a permanently pinned library
    let api = unsafe { init() };
    if api.is_null() {
        return Err(invalid("null loaded executable strategy table"));
    }
    let pinned = Box::leak(Box::new(Pinned {
        _handle: handle,
        _backing: backing,
        api,
        metadata,
        elf,
    }));
    let library = LoadedLibrary { pinned };
    Ok(library)
}

fn sealed_backing(bytes: &[u8]) -> Result<File, PluginError> {
    // SAFETY: constant name/flags; the successful FD is immediately owned by File
    let descriptor = unsafe {
        libc::memfd_create(
            c"nautilus-algorithm".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING | libc::MFD_EXEC,
        )
    };
    if descriptor < 0 {
        return Err(io_error(&std::io::Error::last_os_error()));
    }
    // SAFETY: descriptor is newly created and has exactly one Rust owner
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    file.write_all(bytes).map_err(|error| io_error(&error))?;
    let seals = libc::F_SEAL_WRITE | libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_SEAL;
    // SAFETY: fcntl operates on the owned memfd using integer seal flags
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) } != 0 {
        return Err(io_error(&std::io::Error::last_os_error()));
    }
    // SAFETY: F_GET_SEALS reads the seal mask of the owned memfd
    let actual = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if actual < 0 || actual & seals != seals {
        return Err(invalid("native library backing is not fully sealed"));
    }
    Ok(file)
}

fn loader_error() -> PluginError {
    // SAFETY: dlerror returns null or a live thread-local C error string
    let message = unsafe { libc::dlerror() };
    if message.is_null() {
        return PluginError::generic("native dynamic loader failed");
    }
    // SAFETY: the loader error string remains live until the next loader call
    PluginError::generic(unsafe { CStr::from_ptr(message) }.to_string_lossy())
}
