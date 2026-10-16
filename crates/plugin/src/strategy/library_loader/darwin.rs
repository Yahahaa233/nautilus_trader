// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Named private Darwin backing. dyld's signature policy refuses an unlinked
//! image, so this cannot promise GNU memfd sealing. Native source, publisher
//! and same-UID host custody are trusted; no malicious-code sandbox is claimed.

use std::{
    ffi::CString,
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::fs::{MetadataExt, OpenOptionsExt},
    },
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{invalid, io_error};
use crate::PluginError;

pub(super) const FD_DIRECTORY: &str = "/dev/fd";
static NAME_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_name(prefix: &str) -> Result<CString, PluginError> {
    let id = NAME_COUNTER
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| invalid("native backing identity exhausted"))?;
    // SAFETY: getpid has no pointer or ownership preconditions
    let pid = unsafe { libc::getpid() };
    CString::new(format!("{prefix}.{pid}.{id}"))
        .map_err(|_| invalid("native backing name is invalid"))
}

fn open_at(directory: &File, name: &CString, flags: i32, mode: u16) -> Result<File, PluginError> {
    // SAFETY: live directory/name and checked integer flags; only a successful
    // new descriptor is transferred to File ownership.
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            libc::c_uint::from(mode),
        )
    };
    if descriptor < 0 {
        return Err(io_error(&std::io::Error::last_os_error()));
    }
    // SAFETY: this successful descriptor has exactly one Rust owner
    Ok(unsafe { File::from_raw_fd(descriptor) })
}

fn mode(file: &File, value: u16) -> Result<(), PluginError> {
    // SAFETY: the caller owns this live descriptor; no pathname is followed
    if unsafe { libc::fchmod(file.as_raw_fd(), value) } != 0 {
        return Err(io_error(&std::io::Error::last_os_error()));
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct LibraryDirectory(File);

impl LibraryDirectory {
    pub(super) fn new(parent: &Path) -> Result<Self, PluginError> {
        let parent = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(parent)
            .map_err(|e| io_error(&e))?;
        let metadata = parent.metadata().map_err(|e| io_error(&e))?;
        // SAFETY: geteuid has no pointer or ownership preconditions
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err(invalid(
                "Darwin backing parent is not private host-owned storage",
            ));
        }
        let name = next_name("nautilus-algorithms")?;
        // SAFETY: the opened parent and generated basename remain live
        if unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
            return Err(io_error(&std::io::Error::last_os_error()));
        }
        let directory = open_at(&parent, &name, libc::O_RDONLY | libc::O_DIRECTORY, 0)?;
        mode(&directory, 0o500)?;
        Ok(Self(directory))
    }
}

#[derive(Debug)]
pub(super) struct Backing {
    file: File,
    // Loaded and failed post-constructor images retain their descriptor and
    // protected directory for process lifetime, even after registry release.
    _directory: File,
}

impl Backing {
    pub(super) fn file(&self) -> &File {
        &self.file
    }
}

pub(super) fn prepare(bytes: &[u8], directory: &LibraryDirectory) -> Result<Backing, PluginError> {
    mode(&directory.0, 0o700)?;
    let copied = (|| {
        let name = next_name("algorithm")?;
        let mut writable = open_at(
            &directory.0,
            &name,
            libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            0o600,
        )?;
        writable.write_all(bytes).map_err(|e| io_error(&e))?;
        writable.sync_all().map_err(|e| io_error(&e))?;
        mode(&writable, 0o444)?;
        let mut readonly = open_at(&directory.0, &name, libc::O_RDONLY, 0)?;
        let original = writable.metadata().map_err(|e| io_error(&e))?;
        let actual = readonly.metadata().map_err(|e| io_error(&e))?;
        if original.dev() != actual.dev()
            || original.ino() != actual.ino()
            || actual.mode() & 0o222 != 0
            || actual.len() != bytes.len() as u64
            || actual.nlink() != 1
        {
            return Err(invalid("Darwin backing inode differs from copied bytes"));
        }
        drop(writable);
        let mut observed = vec![0; bytes.len()];
        readonly
            .read_exact(&mut observed)
            .map_err(|e| io_error(&e))?;
        if observed != bytes {
            return Err(invalid("Darwin backing bytes changed during publication"));
        }
        readonly
            .seek(SeekFrom::Start(0))
            .map_err(|e| io_error(&e))?;
        directory.0.sync_all().map_err(|e| io_error(&e))?;
        Ok(Backing {
            file: readonly,
            _directory: directory.0.try_clone().map_err(|e| io_error(&e))?,
        })
    })();
    // This result is checked before dlopen. Keep original failed copies too;
    // an active protected directory is never implicitly removed by this SDK.
    mode(&directory.0, 0o500)?;
    copied
}
