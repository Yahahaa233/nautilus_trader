// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Allocator-local, panic-contained executable thunks.

use std::{
    ffi::c_void,
    panic::{AssertUnwindSafe, catch_unwind},
};

use super::{
    Algorithm, CallResult, CallbackKind, Host, HostLease, MAX_PAYLOAD_BYTES, ReadOnlyHost,
};
use crate::{
    PluginError, PluginErrorCode, Slice,
    panic::{drop_payload, panic_message},
};

fn guarded(f: impl FnOnce() -> Result<Vec<u8>, PluginError>) -> CallResult {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(bytes)) if bytes.len() <= MAX_PAYLOAD_BYTES => CallResult::success(bytes),
        Ok(Ok(_)) => CallResult::failure(PluginError::new(
            PluginErrorCode::InvalidArgument,
            "component output exceeds transport bound",
        )),
        Ok(Err(error)) => CallResult::failure(error),
        Err(payload) => {
            let message = panic_message(payload.as_ref());
            drop_payload(payload);
            CallResult::failure(PluginError::panic(message))
        }
    }
}

unsafe fn bytes(value: Slice<'_, u8>) -> Result<&[u8], PluginError> {
    if value.len > MAX_PAYLOAD_BYTES || (value.len > 0 && value.ptr.is_null()) {
        return Err(PluginError::new(
            PluginErrorCode::InvalidArgument,
            "invalid component input buffer",
        ));
    }
    // SAFETY: the caller supplies live borrowed bytes; pointer and length were checked above
    Ok(unsafe { value.as_slice() })
}

/// Checks the pure algorithm-specific configuration.
///
/// # Safety
///
/// Nonempty input points to live storage for the duration of this call.
pub unsafe extern "C" fn validate<A: Algorithm>(input: Slice<'_, u8>) -> CallResult {
    guarded(|| {
        // SAFETY: inherited live input contract
        A::validate(unsafe { bytes(input)? })?;
        Ok(Vec::new())
    })
}

/// Creates an algorithm instance using the producer's allocator.
///
/// # Safety
///
/// Input is live; `output` points to writable pointer-sized storage. The host
/// does not reuse or dereference the resulting opaque instance itself.
pub unsafe extern "C" fn create<A: Algorithm>(
    input: Slice<'_, u8>,
    output: *mut *mut c_void,
) -> CallResult {
    guarded(|| {
        if output.is_null() {
            return Err(PluginError::new(
                PluginErrorCode::InvalidArgument,
                "null create output",
            ));
        }
        // SAFETY: inherited writable output contract, initialized even on failure
        unsafe {
            *output = std::ptr::null_mut();
        }
        // SAFETY: inherited live input contract
        let input = unsafe { bytes(input)? };
        A::validate(input)?;
        let algorithm = A::create(input)?;
        // SAFETY: output is live; the opaque handle is disposed by this producer only
        unsafe {
            *output = Box::into_raw(Box::new(algorithm)).cast();
        }
        Ok(Vec::new())
    })
}

/// Dispatches the exact lifecycle callback without unwinding across C.
///
/// # Safety
///
/// Handle comes from this algorithm's create thunk, is not disposed, and has
/// exclusive synchronous access. Input and the host function remain live.
pub unsafe extern "C" fn callback<A: Algorithm>(
    handle: *mut c_void,
    kind: u32,
    version: u32,
    input: Slice<'_, u8>,
    lease: HostLease,
) -> CallResult {
    guarded(|| {
        if handle.is_null() || lease.instance == 0 || lease.generation == 0 {
            return Err(PluginError::new(
                PluginErrorCode::InvalidArgument,
                "null instance or invalid callback lease",
            ));
        }
        let kind = CallbackKind::from_tag(kind).ok_or_else(|| {
            PluginError::new(PluginErrorCode::InvalidArgument, "unknown callback tag")
        })?;
        // SAFETY: inherited exclusive live handle and input contracts
        let algorithm = unsafe { &mut *handle.cast::<A>() };
        // SAFETY: inherited live input contract
        let input = unsafe { bytes(input)? };
        let mut host = Host::new(lease);
        match kind {
            CallbackKind::Start => algorithm.start(&mut host)?,
            CallbackKind::Event => algorithm.on_event(input, &mut host)?,
            CallbackKind::Save => return algorithm.save(&mut ReadOnlyHost::new(lease)),
            CallbackKind::Restore => {
                algorithm.restore(version, input, &mut ReadOnlyHost::new(lease))?;
            }
            CallbackKind::Pause => algorithm.pause(&mut host)?,
            CallbackKind::Resume => algorithm.resume(&mut host)?,
            CallbackKind::Stop => algorithm.stop(input, &mut host)?,
        }
        Ok(Vec::new())
    })
}

/// Releases an instance exactly once using the producer's allocator.
///
/// # Safety
///
/// Handle is null or comes from this algorithm's create thunk. It is no longer
/// in use and is never called or disposed again, including after a drop error.
pub unsafe extern "C" fn dispose<A: Algorithm>(handle: *mut c_void) -> CallResult {
    guarded(|| {
        if !handle.is_null() {
            // SAFETY: inherited exclusive ownership contract
            unsafe {
                drop(Box::from_raw(handle.cast::<A>()));
            }
        }
        Ok(Vec::new())
    })
}
