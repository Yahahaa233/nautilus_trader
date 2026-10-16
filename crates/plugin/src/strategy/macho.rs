// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Bounded ARM64 Mach-O admission before dynamic-library constructors.
//! OS signature validation and trusted source publication remain mandatory.

use crate::{PluginError, PluginErrorCode};
use std::collections::BTreeSet;

const MAX_IMAGE_BYTES: u64 = 536_870_912;
// Exact dependencies observed in the governed Rust algorithm artifacts and
// verified against the selected Apple SDK's system stubs. A system-looking
// prefix alone never admits another image or a user-controlled search path.
const SYSTEM_LIBRARIES: &[&str] = &[
    "/usr/lib/libSystem.B.dylib",
    "/usr/lib/libiconv.2.dylib",
    "/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation",
];

#[cfg(test)]
mod tests;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MachLibrary {
    pub dependencies: BTreeSet<String>,
    pub mapped_bytes: u64,
    pub has_initializers: bool,
    pub has_code_signature: bool,
}

fn invalid(message: &str) -> PluginError {
    PluginError::new(PluginErrorCode::InvalidArgument, message)
}

struct Bytes<'a>(&'a [u8]);
impl<'a> Bytes<'a> {
    fn range(&self, offset: u64, length: u64) -> Result<&'a [u8], PluginError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("Mach-O range overflow"))?;
        let start = usize::try_from(offset).map_err(|_| invalid("Mach-O offset overflow"))?;
        let end = usize::try_from(end).map_err(|_| invalid("Mach-O range overflow"))?;
        self.0
            .get(start..end)
            .ok_or_else(|| invalid("Mach-O range exceeds artifact bytes"))
    }
    fn u32(&self, offset: u64) -> Result<u32, PluginError> {
        Ok(u32::from_le_bytes(
            self.range(offset, 4)?
                .try_into()
                .map_err(|_| invalid("Mach-O u32"))?,
        ))
    }
    fn u64(&self, offset: u64) -> Result<u64, PluginError> {
        Ok(u64::from_le_bytes(
            self.range(offset, 8)?
                .try_into()
                .map_err(|_| invalid("Mach-O u64"))?,
        ))
    }
}

#[derive(Default)]
struct ImageSegments {
    ranges: Vec<(u64, u64)>,
    mapped_bytes: u64,
    executable: bool,
    has_initializers: bool,
}
impl ImageSegments {
    fn read(&mut self, data: &Bytes<'_>, bytes: &Bytes<'_>) -> Result<(), PluginError> {
        bytes.range(0, 72)?;
        let address = bytes.u64(24)?;
        let memory = bytes.u64(32)?;
        let file_offset = bytes.u64(40)?;
        let file_size = bytes.u64(48)?;
        let maximum = bytes.u32(56)?;
        let initial = bytes.u32(60)?;
        let sections = u64::from(bytes.u32(64)?);
        if initial & !maximum != 0
            || initial & !7 != 0
            || initial & 6 == 6
            || file_size > memory
            || sections > 4096
            || 72 + sections * 80 != bytes.0.len() as u64
        {
            return Err(invalid("invalid or writable executable Mach-O segment"));
        }
        data.range(file_offset, file_size)?;
        let limit = address
            .checked_add(memory)
            .ok_or_else(|| invalid("Mach-O address overflow"))?;
        if memory > 0 {
            if self
                .ranges
                .iter()
                .any(|(start, end)| address < *end && *start < limit)
            {
                return Err(invalid("overlapping Mach-O virtual segments"));
            }
            self.ranges.push((address, limit));
        }
        self.mapped_bytes = self
            .mapped_bytes
            .checked_add(memory)
            .ok_or_else(|| invalid("Mach-O memory overflow"))?;
        if self.mapped_bytes > MAX_IMAGE_BYTES {
            return Err(invalid("Mach-O mapped image exceeds capacity"));
        }
        self.executable |= initial & 4 != 0;
        for section in 0..sections {
            let offset = 72 + section * 80;
            let section_address = bytes.u64(offset + 32)?;
            let section_size = bytes.u64(offset + 40)?;
            let section_end = section_address
                .checked_add(section_size)
                .ok_or_else(|| invalid("Mach-O section overflow"))?;
            if section_address < address || section_end > limit {
                return Err(invalid("Mach-O section exceeds segment"));
            }
            self.has_initializers |= matches!(bytes.u32(offset + 64)? & 0xff, 0x9 | 0xa | 0x16);
        }
        Ok(())
    }
    fn span(&self) -> u64 {
        self.ranges
            .iter()
            .map(|(_, end)| *end)
            .max()
            .unwrap_or(0)
            .saturating_sub(
                self.ranges
                    .iter()
                    .map(|(start, _)| *start)
                    .min()
                    .unwrap_or(0),
            )
    }
}

fn command_name<'a>(bytes: &Bytes<'a>) -> Result<&'a str, PluginError> {
    bytes.range(0, 24)?;
    let offset = u64::from(bytes.u32(8)?);
    let length = bytes.0.len() as u64;
    if offset < 24 || offset >= length {
        return Err(invalid("invalid Mach-O name offset"));
    }
    let value = bytes.range(offset, length - offset)?;
    let nul = value
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| invalid("unterminated Mach-O name"))?;
    if nul == 0 || nul > 1024 {
        return Err(invalid("invalid Mach-O name length"));
    }
    std::str::from_utf8(&value[..nul]).map_err(|_| invalid("invalid Mach-O name text"))
}

impl MachLibrary {
    /// Inspects the complete declared image and direct dependency domain.
    ///
    /// # Errors
    /// Refuses wrong architecture/type, malformed commands, writable executable
    /// mappings, oversized images, search overrides and foreign dylibs.
    pub fn inspect(raw: &[u8]) -> Result<Self, PluginError> {
        let data = Bytes(raw);
        if data.u32(0)? != 0xfeed_facf
            || data.u32(4)? != 0x0100_000c
            || data.u32(8)? != 0
            || data.u32(12)? != 6
            || data.u32(28)? != 0
        {
            return Err(invalid(
                "expected ARM64 little-endian Mach-O dynamic library",
            ));
        }
        let flags = data.u32(24)?;
        if flags & (0x100 | 0x2_0000) != 0 || flags & 0x80 == 0 {
            return Err(invalid(
                "Mach-O flat namespace or executable stack is not admitted",
            ));
        }
        let count = data.u32(16)?;
        let size = u64::from(data.u32(20)?);
        if count == 0 || count > 4096 || size > 1_048_576 {
            return Err(invalid("invalid Mach-O command capacity"));
        }
        data.range(32, size)?;
        let end = 32 + size;
        let mut cursor = 32;
        let mut dependencies = BTreeSet::new();
        let mut image = ImageSegments::default();
        let mut signatures = 0;
        let mut identifiers = 0;
        for _ in 0..count {
            let command = data.u32(cursor)?;
            let length = u64::from(data.u32(cursor + 4)?);
            if length < 8 || !length.is_multiple_of(8) || length > end.saturating_sub(cursor) {
                return Err(invalid("invalid Mach-O command extent"));
            }
            let bytes = Bytes(data.range(cursor, length)?);
            match command {
                0x19 => image.read(&data, &bytes)?,
                0xc | 0x8000_0018 | 0x8000_001f | 0x8000_0023 | 0x20 => {
                    let name = command_name(&bytes)?;
                    if !SYSTEM_LIBRARIES.contains(&name) {
                        return Err(invalid("unadmitted Mach-O runtime dependency"));
                    }
                    dependencies.insert(name.into());
                }
                0x1d => {
                    bytes.range(0, 16)?;
                    let offset = u64::from(bytes.u32(8)?);
                    let size = u64::from(bytes.u32(12)?);
                    if size == 0 {
                        return Err(invalid("empty Mach-O signature"));
                    }
                    data.range(offset, size)?;
                    signatures += 1;
                }
                0xd => {
                    command_name(&bytes)?;
                    identifiers += 1;
                }
                0x1a => {
                    bytes.range(0, 72)?;
                    image.has_initializers = true;
                }
                0x8000_001c | 0x27 | 0xe | 0x8000_0028 => {
                    return Err(invalid(
                        "Mach-O loader override or executable entry is not admitted",
                    ));
                }
                // dyld interprets these only after the image/dependency domain
                // and the actual system code-signing policy admit the file.
                0x2 | 0xb | 0x1b | 0x32 | 0x2a | 0x26 | 0x29 | 0x1e | 0x22 | 0x8000_0022
                | 0x8000_0033 | 0x8000_0034 | 0x24 | 0x2e | 0x31 => {}
                _ => return Err(invalid("unsupported Mach-O load command")),
            }
            cursor += length;
        }
        if cursor != end
            || signatures != 1
            || identifiers != 1
            || !image.executable
            || image.mapped_bytes == 0
            || image.span() > MAX_IMAGE_BYTES
        {
            return Err(invalid("incomplete Mach-O image or signature inventory"));
        }
        Ok(Self {
            dependencies,
            mapped_bytes: image.mapped_bytes,
            has_initializers: image.has_initializers,
            has_code_signature: true,
        })
    }
}
