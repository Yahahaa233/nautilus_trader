// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Platform loader for already-published trusted algorithms. Hash, image and
//! dependency checks precede constructors. GNU uses sealed memfd bytes; Darwin
//! needs private named signed backing and trusted same-UID host custody.
//! Handles, backing files and descriptor numbers remain pinned until exit.

use std::{
    collections::BTreeMap,
    ffi::{CStr, CString, c_void},
    fs::File,
    os::{
        fd::AsRawFd,
        unix::fs::{FileExt, MetadataExt},
    },
    ptr::NonNull,
};

use sha2::{Digest, Sha256};

use super::{
    STRATEGY_INIT_SYMBOL, StrategyApi,
    instance::{CallbackLimits, GuestInstance},
};
use crate::{HostVTable, PluginError, PluginErrorCode, PluginInitFn, PluginManifest};

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
#[path = "library_loader/gnu.rs"]
mod backend;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[path = "library_loader/darwin.rs"]
mod backend;
#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
use super::elf::ElfLibrary as PlatformLibrary;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
use super::macho::MachLibrary as PlatformLibrary;

static METADATA_TOKEN: u8 = 0;
type StrategyInitFn = unsafe extern "C" fn() -> *const StrategyApi;

#[derive(Debug)]
struct Pinned {
    _handle: NonNull<c_void>,
    _backing: &'static backend::Backing,
    api: *const StrategyApi,
    metadata: &'static PluginManifest,
    facts: PlatformLibrary,
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
    #[cfg(target_os = "linux")]
    pub const fn elf(&self) -> &PlatformLibrary {
        &self.pinned.facts
    }
    /// Returns pre-constructor Mach-O layout and dependency facts.
    #[cfg(target_os = "macos")]
    #[must_use]
    pub const fn macho(&self) -> &PlatformLibrary {
        &self.pinned.facts
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
        state_validator: Box<dyn super::instance::StateValidator>,
    ) -> Result<GuestInstance, PluginError> {
        // SAFETY: the trusted publication contract and pinned loaded library
        // establish the process-long executable table's layout and lifetime.
        unsafe {
            GuestInstance::create(
                self.pinned.api,
                descriptor,
                configuration,
                limits,
                state_validator,
            )
        }
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
    remaining_library_bytes: usize,
    #[cfg(target_os = "macos")]
    directory: backend::LibraryDirectory,
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
    /// Refuses zero or above-transport capacities. The byte budget covers both
    /// copied files and declared mapped images across successful/failed loads.
    /// Darwin additionally requires an existing private host storage directory.
    pub fn new(
        maximum_libraries: usize,
        maximum_library_bytes: usize,
        #[cfg(target_os = "macos")] backing_directory: &std::path::Path,
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
            remaining_library_bytes: maximum_library_bytes,
            #[cfg(target_os = "macos")]
            directory: backend::LibraryDirectory::new(backing_directory)?,
        })
    }

    /// Returns this host registry's conservative unspent byte budget.
    /// The server retains one registry for the process lifetime: dropping it
    /// never unloads code and must not be used to recycle resident capacity.
    #[must_use]
    pub const fn remaining_library_bytes(&self) -> usize {
        self.remaining_library_bytes
    }

    /// Loads the exact admitted bytes under the unprivileged host identity.
    /// No library is loaded by an administrator or a metadata inspection command.
    ///
    /// # Errors
    ///
    /// Refuses privileged callers, modified/untrusted files, loader overrides,
    /// incompatible image/dependencies, capacity, backing or dynamic-loader failures.
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
            name.starts_with("LD_") || name.starts_with("DYLD_") || name == "GLIBC_TUNABLES"
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
                &mut self.remaining_library_bytes,
                #[cfg(target_os = "macos")]
                &self.directory,
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
    remaining_library_bytes: &mut usize,
    #[cfg(target_os = "macos")] directory: &backend::LibraryDirectory,
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
    reserve_bytes(remaining_library_bytes, length)?;
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
    let facts = PlatformLibrary::inspect(&bytes)?;
    reserve_bytes(
        remaining_library_bytes,
        usize::try_from(facts.mapped_bytes).map_err(|_| invalid("native mapped size overflow"))?,
    )?;
    let backing = backend::prepare(
        &bytes,
        #[cfg(target_os = "macos")]
        directory,
    )?;
    let path = CString::new(format!(
        "{}/{}",
        backend::FD_DIRECTORY,
        backing.file().as_raw_fd()
    ))
    .map_err(|_| invalid("invalid sealed library descriptor"))?;
    let flags = libc::RTLD_NOW | libc::RTLD_LOCAL | libc::RTLD_NODELETE;
    #[cfg(target_os = "macos")]
    let flags = flags | libc::RTLD_FIRST;
    // SAFETY: the path names a live protected FD to pre-inspected trusted bytes;
    // flags request immediate local binding and permanent loader retention.
    let handle = unsafe { libc::dlopen(path.as_ptr(), flags) };
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
        facts,
    }));
    let library = LoadedLibrary { pinned };
    Ok(library)
}

fn reserve_bytes(remaining: &mut usize, bytes: usize) -> Result<(), PluginError> {
    *remaining = remaining
        .checked_sub(bytes)
        .ok_or_else(|| invalid("native library resident byte capacity exceeded"))?;
    Ok(())
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
