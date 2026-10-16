// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use super::*;

fn put16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}
fn put32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn put64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn fixture() -> Vec<u8> {
    let mut bytes = vec![0; 1024];
    bytes[..7].copy_from_slice(b"\x7fELF\x02\x01\x01");
    put16(&mut bytes, 16, 3);
    put16(&mut bytes, 18, 62);
    put32(&mut bytes, 20, 1);
    put64(&mut bytes, 32, 64);
    put16(&mut bytes, 52, 64);
    put16(&mut bytes, 54, 56);
    put16(&mut bytes, 56, 4);
    for (index, (kind, flags, offset, address, size)) in [
        (1, 4, 0, 0x0010_0000, 1024),
        (2, 6, 0x200, 0x0010_0200, 64),
        (0x6474_e551, 6, 0, 0, 0),
        (0x6474_e552, 4, 0x200, 0x0010_0200, 64),
    ]
    .into_iter()
    .enumerate()
    {
        let h = 64 + index * 56;
        put32(&mut bytes, h, kind);
        put32(&mut bytes, h + 4, flags);
        put64(&mut bytes, h + 8, offset);
        put64(&mut bytes, h + 16, address);
        put64(&mut bytes, h + 32, size);
        put64(&mut bytes, h + 40, size);
    }
    for (index, (tag, value)) in [(1, 0), (5, 0x0010_0300), (10, 10), (0, 0)]
        .into_iter()
        .enumerate()
    {
        put64(&mut bytes, 0x200 + index * 16, tag);
        put64(&mut bytes, 0x208 + index * 16, value);
    }
    bytes[0x300..0x30a].copy_from_slice(b"libc.so.6\0");
    bytes
}

#[test]
fn elf_reads_exact_dependency_and_nonexecuting_layout_facts() {
    let facts = ElfLibrary::inspect(&fixture()).unwrap();
    assert_eq!(facts.dependencies, BTreeSet::from(["libc.so.6".to_owned()]));
    assert!(facts.has_relro);
    assert!(!facts.has_initializers);
}

#[test]
fn elf_refuses_loader_overrides_wrong_platform_writable_code_and_bad_ranges() {
    for change in 0..12 {
        let mut bytes = fixture();
        match change {
            0 => bytes[4] = 1,
            1 => put16(&mut bytes, 16, 2),
            2 => put16(&mut bytes, 18, 183),
            3 => put16(&mut bytes, 56, 257),
            4 => put32(&mut bytes, 64 + 4, 7),
            5 => put32(&mut bytes, 64 + 2 * 56 + 4, 7),
            6 => put64(&mut bytes, 32, u64::MAX),
            7 => put64(&mut bytes, 0x200, 15),
            8 => put64(&mut bytes, 0x200, 0x6fff_fefc),
            9 => put64(&mut bytes, 0x208, 11),
            10 => {
                put64(&mut bytes, 0x200, 0x6fff_fffb);
                put64(&mut bytes, 0x208, 0x0800_0000);
            }
            11 => bytes[0x300] = b'/',
            _ => unreachable!(),
        }
        assert!(
            ElfLibrary::inspect(&bytes).is_err(),
            "accepted mutation {change}"
        );
    }
}

#[test]
fn elf_all_truncated_prefixes_fail_without_pointer_reads() {
    let bytes = fixture();
    for end in 0..bytes.len() {
        assert!(
            ElfLibrary::inspect(&bytes[..end]).is_err(),
            "accepted prefix {end}"
        );
    }
}
