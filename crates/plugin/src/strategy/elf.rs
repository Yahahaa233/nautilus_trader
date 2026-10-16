// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Bounded x86-64 GNU library inspection, performed before any constructor.
//! A valid ELF layout is not a trust or publication certificate.

use std::collections::BTreeSet;

use crate::{PluginError, PluginErrorCode};

#[cfg(test)]
mod tests;

/// Loader dependencies admitted by the first GNU host profile. Native
/// strategies otherwise use the encoded host facade, not extra shared runtimes.
const SYSTEM_LIBRARIES: &[&str] = &[
    "libc.so.6",
    "libm.so.6",
    "libgcc_s.so.1",
    "libdl.so.2",
    "libpthread.so.0",
    "librt.so.1",
    "ld-linux-x86-64.so.2",
];

/// Pre-load facts used in the artifact receipt and actual loader check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElfLibrary {
    pub dependencies: BTreeSet<String>,
    pub mapped_bytes: u64,
    pub has_initializers: bool,
    pub has_relro: bool,
}

fn invalid(message: &str) -> PluginError {
    PluginError::new(PluginErrorCode::InvalidArgument, message)
}

struct Bytes<'a>(&'a [u8]);

impl<'a> Bytes<'a> {
    fn range(&self, offset: u64, length: u64) -> Result<&'a [u8], PluginError> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid("ELF range overflow"))?;
        let start = usize::try_from(offset).map_err(|_| invalid("ELF offset overflow"))?;
        let end = usize::try_from(end).map_err(|_| invalid("ELF range overflow"))?;
        self.0
            .get(start..end)
            .ok_or_else(|| invalid("ELF range exceeds artifact bytes"))
    }
    fn u16(&self, offset: u64) -> Result<u16, PluginError> {
        Ok(u16::from_le_bytes(
            self.range(offset, 2)?
                .try_into()
                .map_err(|_| invalid("ELF u16"))?,
        ))
    }
    fn u32(&self, offset: u64) -> Result<u32, PluginError> {
        Ok(u32::from_le_bytes(
            self.range(offset, 4)?
                .try_into()
                .map_err(|_| invalid("ELF u32"))?,
        ))
    }
    fn u64(&self, offset: u64) -> Result<u64, PluginError> {
        Ok(u64::from_le_bytes(
            self.range(offset, 8)?
                .try_into()
                .map_err(|_| invalid("ELF u64"))?,
        ))
    }
}

struct ProgramImage {
    loads: Vec<(u64, u64, u64)>,
    dynamic: (u64, u64),
    relro: bool,
    mapped_bytes: u64,
}

fn validate_virtual_span(virtual_ranges: &[(u64, u64)]) -> Result<(), PluginError> {
    let span = virtual_ranges
        .iter()
        .map(|(_, end)| *end)
        .max()
        .unwrap_or(0)
        .saturating_sub(
            virtual_ranges
                .iter()
                .map(|(start, _)| *start)
                .min()
                .unwrap_or(0),
        );
    if span > 536_870_912 {
        return Err(invalid("ELF virtual image span exceeds capacity"));
    }
    Ok(())
}

impl ProgramImage {
    fn read(data: &Bytes<'_>) -> Result<Self, PluginError> {
        if data.range(0, 7)? != b"\x7fELF\x02\x01\x01"
            || !matches!(data.range(7, 1)?[0], 0 | 3)
            || data.u16(16)? != 3
            || data.u16(18)? != 62
            || data.u32(20)? != 1
            || data.u16(52)? != 64
            || data.u16(54)? != 56
        {
            return Err(invalid("expected x86-64 little-endian ELF shared library"));
        }
        let count = data.u16(56)?;
        let offset = data.u64(32)?;
        if count == 0 || count > 256 {
            return Err(invalid("invalid ELF program header count"));
        }
        data.range(offset, u64::from(count) * 56)?;
        let mut loads = Vec::new();
        let mut dynamic = None;
        let mut stack = false;
        let mut relro = false;
        let mut mapped_bytes = 0u64;
        let mut virtual_ranges = Vec::new();
        for index in 0..count {
            let header = offset + u64::from(index) * 56;
            let kind = data.u32(header)?;
            let flags = data.u32(header + 4)?;
            let file_offset = data.u64(header + 8)?;
            let address = data.u64(header + 16)?;
            let size = data.u64(header + 32)?;
            let memory_size = data.u64(header + 40)?;
            if size > memory_size {
                return Err(invalid("ELF segment exceeds memory size"));
            }
            data.range(file_offset, size)?;
            match kind {
                1 => {
                    if flags & 3 == 3 {
                        return Err(invalid("writable executable ELF mapping"));
                    }
                    let end = address
                        .checked_add(memory_size)
                        .ok_or_else(|| invalid("ELF virtual range overflow"))?;
                    if memory_size > 0 {
                        if virtual_ranges
                            .iter()
                            .any(|(start, limit)| address < *limit && *start < end)
                        {
                            return Err(invalid("overlapping ELF load segments"));
                        }
                        virtual_ranges.push((address, end));
                    }
                    mapped_bytes = mapped_bytes
                        .checked_add(memory_size)
                        .ok_or_else(|| invalid("ELF memory capacity overflow"))?;
                    if mapped_bytes > 536_870_912 {
                        return Err(invalid("ELF mapped image exceeds capacity"));
                    }
                    loads.push((address, file_offset, size));
                }
                2 => {
                    if dynamic.replace((file_offset, size)).is_some() {
                        return Err(invalid("multiple ELF dynamic segments"));
                    }
                }
                3 => {
                    return Err(invalid(
                        "ELF interpreter marks an executable, not an algorithm library",
                    ));
                }
                0x6474_e551 => {
                    if flags & 1 != 0 {
                        return Err(invalid("executable ELF stack"));
                    }
                    stack = true;
                }
                0x6474_e552 => relro = true,
                _ => {}
            }
        }
        if !stack {
            return Err(invalid("ELF does not declare a non-executable stack"));
        }
        validate_virtual_span(&virtual_ranges)?;
        Ok(Self {
            loads,
            dynamic: dynamic.ok_or_else(|| invalid("ELF dynamic table missing"))?,
            relro,
            mapped_bytes,
        })
    }
}

impl ElfLibrary {
    /// Inspects ELF program headers and dynamic dependencies before `dlopen`.
    ///
    /// # Errors
    ///
    /// Refuses another platform, an executable, oversized/malformed tables,
    /// writable executable mappings, executable stack, loader path overrides,
    /// auditing/filter dependencies or an undeclared system library.
    pub fn inspect(bytes: &[u8]) -> Result<Self, PluginError> {
        let data = Bytes(bytes);
        let image = ProgramImage::read(&data)?;
        let (dynamic_offset, dynamic_size) = image.dynamic;
        if dynamic_size == 0 || dynamic_size % 16 != 0 || dynamic_size / 16 > 4096 {
            return Err(invalid("invalid ELF dynamic table size"));
        }
        let mut terminated = false;
        let mut string_table = None;
        let mut string_size = None;
        let mut needed = Vec::new();
        let mut initializers = false;
        for index in 0..dynamic_size / 16 {
            let entry = dynamic_offset + index * 16;
            let tag = data.u64(entry)?;
            let value = data.u64(entry + 8)?;
            match tag {
                0 => {
                    terminated = true;
                    break;
                }
                1 => needed.push(value),
                5 => {
                    if string_table.replace(value).is_some() {
                        return Err(invalid("duplicate ELF string table"));
                    }
                }
                10 => {
                    if string_size.replace(value).is_some() {
                        return Err(invalid("duplicate ELF string table size"));
                    }
                }
                12 | 25 | 32 => initializers |= value != 0,
                15 | 29 | 0x6fff_fefa | 0x6fff_fefb | 0x6fff_fefc | 0x7fff_fffd | 0x7fff_ffff => {
                    return Err(invalid(
                        "ELF contains a loader path, audit or filter override",
                    ));
                }
                0x6fff_fffb if value & 0x0800_0000 != 0 => {
                    return Err(invalid("ELF PIE executable is not an algorithm library"));
                }
                _ => {}
            }
        }
        if !terminated || needed.len() > 64 {
            return Err(invalid(
                "unterminated or excessive ELF dynamic dependencies",
            ));
        }
        let address = string_table.ok_or_else(|| invalid("ELF dynamic string table absent"))?;
        let size = string_size.ok_or_else(|| invalid("ELF dynamic string size absent"))?;
        if size == 0 || size > 1_048_576 {
            return Err(invalid("ELF dynamic strings exceed bound"));
        }
        let ranges = image
            .loads
            .iter()
            .filter_map(|(start, offset, available)| {
                address
                    .checked_sub(*start)
                    .filter(|delta| *delta < *available)
                    .and_then(|delta| {
                        delta
                            .checked_add(size)
                            .filter(|end| *end <= *available)
                            .map(|_| *offset + delta)
                    })
            })
            .collect::<Vec<_>>();
        if ranges.len() != 1 {
            return Err(invalid("ambiguous or unmapped ELF dynamic strings"));
        }
        let strings = data.range(ranges[0], size)?;
        let mut dependencies = BTreeSet::new();
        for offset in needed {
            let offset =
                usize::try_from(offset).map_err(|_| invalid("ELF dependency offset overflow"))?;
            let rest = strings
                .get(offset..)
                .ok_or_else(|| invalid("ELF dependency exceeds string table"))?;
            let length = rest
                .iter()
                .position(|byte| *byte == 0)
                .filter(|length| *length > 0 && *length <= 256)
                .ok_or_else(|| invalid("invalid ELF dependency name"))?;
            let name = std::str::from_utf8(&rest[..length])
                .map_err(|_| invalid("invalid ELF dependency UTF-8"))?;
            if !SYSTEM_LIBRARIES.contains(&name) || !dependencies.insert(name.to_owned()) {
                return Err(invalid("undeclared or duplicate ELF runtime dependency"));
            }
        }
        Ok(Self {
            dependencies,
            mapped_bytes: image.mapped_bytes,
            has_initializers: initializers,
            has_relro: image.relro,
        })
    }
}
