// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Sealed GNU backing; the descriptor is retained with its loaded image.
use super::{invalid, io_error};
use crate::PluginError;
use std::{
    fs::File,
    io::Write,
    os::fd::{AsRawFd, FromRawFd},
};
pub(super) const FD_DIRECTORY: &str = "/proc/self/fd";
#[derive(Debug)]
pub(super) struct Backing(File);
impl Backing {
    pub(super) fn file(&self) -> &File {
        &self.0
    }
}

pub(super) fn prepare(bytes: &[u8]) -> Result<Backing, PluginError> {
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
    Ok(Backing(file))
}
