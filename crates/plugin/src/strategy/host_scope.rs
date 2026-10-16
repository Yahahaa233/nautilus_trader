// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Thread-local dispatch leases that expire when the native callback returns.
//! Guest code never receives a pointer to a native `StrategyCore` or this stack.

use std::{
    cell::RefCell,
    ffi::c_void,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{CallResult, HostLease, HostOperation, MAX_PAYLOAD_BYTES};
use crate::{PluginError, PluginErrorCode, Slice};

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

type NativeDispatch = unsafe fn(*mut c_void, HostOperation, &[u8]) -> Result<Vec<u8>, PluginError>;

struct Active {
    instance: u64,
    generation: u64,
    context: *mut c_void,
    dispatch: NativeDispatch,
}

thread_local! {
    static ACTIVE: RefCell<Option<Active>> = const { RefCell::new(None) };
}

/// Process-unique native instance identity; IDs are never reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstanceId(u64);

fn next(counter: &AtomicU64) -> Result<u64, PluginError> {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| invalid("component lease identity exhausted"))
}

fn invalid(message: &str) -> PluginError {
    PluginError::new(PluginErrorCode::InvalidArgument, message)
}

impl InstanceId {
    /// Allocates a new native instance identity.
    ///
    /// # Errors
    ///
    /// Refuses exhaustion rather than recycling an earlier instance ID.
    pub fn allocate() -> Result<Self, PluginError> {
        next(&NEXT_INSTANCE).map(Self)
    }
}

struct Scope;
impl Drop for Scope {
    fn drop(&mut self) {
        ACTIVE.with(|active| {
            // Native dispatch cannot retain a RefCell borrow beyond a call,
            // including unwinding, so the scope always clears its pointer.
            *active.borrow_mut() = None;
        });
    }
}

/// Invokes one guest callback with exclusive access to a native facade.
/// The issuing thread and exact generation are checked before dereferencing
/// callback-local state. Nested scopes are refused, including cross-instance
/// reentry during a synchronous native host operation.
///
/// # Errors
///
/// Refuses nesting or identity exhaustion before invoking guest code.
pub fn with_host_scope<F, R>(
    instance: InstanceId,
    handler: &mut F,
    invoke: impl FnOnce(HostLease) -> R,
) -> Result<R, PluginError>
where
    F: FnMut(HostOperation, &[u8]) -> Result<Vec<u8>, PluginError>,
{
    unsafe fn dispatch<F>(
        context: *mut c_void,
        operation: HostOperation,
        input: &[u8],
    ) -> Result<Vec<u8>, PluginError>
    where
        F: FnMut(HostOperation, &[u8]) -> Result<Vec<u8>, PluginError>,
    {
        // SAFETY: the matching scope owns exclusive access to the live F;
        // the registry is cleared before that scope can return or unwind.
        unsafe { (&mut *context.cast::<F>())(operation, input) }
    }
    let generation = next(&NEXT_GENERATION)?;
    ACTIVE.with(|active| {
        let mut active = active
            .try_borrow_mut()
            .map_err(|_| invalid("nested native host dispatch"))?;
        if active.is_some() {
            return Err(invalid("nested component callback"));
        }
        *active = Some(Active {
            instance: instance.0,
            generation,
            context: std::ptr::from_mut(handler).cast(),
            dispatch: dispatch::<F>,
        });
        Ok(())
    })?;
    let _scope = Scope;
    Ok(invoke(HostLease {
        instance: instance.0,
        generation,
        call,
    }))
}

unsafe extern "C" fn call(
    instance: u64,
    generation: u64,
    operation: u32,
    input: Slice<'_, u8>,
) -> CallResult {
    let result = crate::panic::guard(|| {
        if input.len > MAX_PAYLOAD_BYTES || (input.len > 0 && input.ptr.is_null()) {
            return Err(invalid("invalid host input buffer"));
        }
        let operation = HostOperation::from_tag(operation)
            .ok_or_else(|| invalid("unknown native host operation"))?;
        ACTIVE.with(|active| {
            // Hold the exclusive borrow through native dispatch. Reentrant
            // guest calls fail without dereferencing the active native owner.
            let mut active = active
                .try_borrow_mut()
                .map_err(|_| invalid("reentrant native host call"))?;
            let active = active
                .as_mut()
                .ok_or_else(|| invalid("expired or cross-thread host lease"))?;
            if active.instance != instance || active.generation != generation {
                return Err(invalid("cross-instance or expired host lease"));
            }
            // SAFETY: only this synchronous matching scope can access its
            // live facade; input belongs to the caller for this call only.
            let input = unsafe { input.as_slice() };
            // SAFETY: the matching scope exclusively owns its live facade
            let output = unsafe { (active.dispatch)(active.context, operation, input) }?;
            if output.len() > MAX_PAYLOAD_BYTES {
                return Err(invalid("host output exceeds transport bound"));
            }
            Ok(output)
        })
    })
    .into_result();
    match result {
        Ok(bytes) => CallResult::success(bytes),
        Err(error) => CallResult::failure(error),
    }
}
