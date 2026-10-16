// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use super::*;

fn put32(raw: &mut [u8], offset: usize, value: u32) {
    raw[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}
fn put64(raw: &mut [u8], offset: usize, value: u64) {
    raw[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn fixture() -> Vec<u8> {
    // Format-only bytes, never executable code or a valid signing claim.
    let mut raw = vec![0; 512];
    for (offset, value) in [
        (0, 0xfeed_facf),
        (4, 0x0100_000c),
        (12, 6),
        (16, 4),
        (20, 192),
        (24, 0x85),
        (32, 0x19),
        (36, 72),
        (88, 5),
        (92, 5),
        (104, 0xc),
        (108, 56),
        (112, 24),
        (160, 0xd),
        (164, 48),
        (168, 24),
        (208, 0x1d),
        (212, 16),
        (216, 480),
        (220, 16),
    ] {
        put32(&mut raw, offset, value);
    }
    put64(&mut raw, 64, 4096);
    put64(&mut raw, 80, 512);
    let dependency = b"/usr/lib/libSystem.B.dylib\0";
    raw[128..128 + dependency.len()].copy_from_slice(dependency);
    let identifier = b"libcontract.dylib\0";
    raw[184..184 + identifier.len()].copy_from_slice(identifier);
    raw
}

#[test]
fn macho_reads_bounded_image_and_signature_presence_without_loading() {
    let facts = MachLibrary::inspect(&fixture()).unwrap();
    assert_eq!(
        facts.dependencies,
        BTreeSet::from(["/usr/lib/libSystem.B.dylib".into()])
    );
    assert_eq!(facts.mapped_bytes, 4096);
    assert!(facts.has_code_signature);
    assert!(!facts.has_initializers);
}

fn dependency_fixture(name: &str) -> Vec<u8> {
    let mut raw = fixture();
    let size = (24 + name.len() + 1).next_multiple_of(8);
    let mut command = vec![0; size];
    put32(&mut command, 0, 0xc);
    put32(&mut command, 4, u32::try_from(size).unwrap());
    put32(&mut command, 8, 24);
    command[24..24 + name.len()].copy_from_slice(name.as_bytes());
    raw.splice(104..160, command);
    let delta = i64::try_from(size).unwrap() - 56;
    let adjusted = |value: i64| u32::try_from(value + delta).unwrap();
    put32(&mut raw, 20, adjusted(192));
    put64(&mut raw, 80, u64::from(adjusted(512)));
    put32(
        &mut raw,
        usize::try_from(216 + delta).unwrap(),
        adjusted(480),
    );
    raw
}

#[test]
fn macho_admits_exact_actual_rust_system_dependencies_and_refuses_lookalikes() {
    for name in SYSTEM_LIBRARIES {
        let facts = MachLibrary::inspect(&dependency_fixture(name)).unwrap();
        assert_eq!(facts.dependencies, BTreeSet::from([(*name).into()]));
    }
    for name in [
        "/tmp/libSystem.B.dylib",
        "/usr/lib/libSystem.B.dylib.extra",
        "@rpath/libiconv.2.dylib",
        "/System/Library/Frameworks/CoreFoundation.framework/Versions/A/Foreign",
    ] {
        assert!(MachLibrary::inspect(&dependency_fixture(name)).is_err());
    }
}

#[test]
fn macho_refuses_truncation_before_any_native_code() {
    let raw = fixture();
    for length in 0..raw.len() {
        assert!(
            MachLibrary::inspect(&raw[..length]).is_err(),
            "length={length}"
        );
    }
}

#[test]
fn macho_refuses_foreign_images_loader_overrides_and_unbounded_segments() {
    for change in 0..13 {
        let mut raw = fixture();
        match change {
            0 => put32(&mut raw, 4, 0x0100_0007),
            1 => put32(&mut raw, 12, 2),
            2 => put32(&mut raw, 24, 0x185),
            3 => put32(&mut raw, 24, 0x2_0085),
            4 => put32(&mut raw, 16, 4097),
            5 => put32(&mut raw, 36, 0),
            6 => put32(&mut raw, 92, 7),
            7 => put64(&mut raw, 64, MAX_IMAGE_BYTES + 1),
            8 => put32(&mut raw, 104, 0x8000_001c),
            9 => raw[128] = b'@',
            10 => put32(&mut raw, 208, 0x27),
            11 => put32(&mut raw, 216, u32::MAX),
            12 => put32(&mut raw, 168, 0),
            _ => unreachable!(),
        }
        assert!(MachLibrary::inspect(&raw).is_err(), "change={change}");
    }
}
