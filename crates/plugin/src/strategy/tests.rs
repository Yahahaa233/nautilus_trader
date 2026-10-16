// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::{
    ffi::c_void,
    sync::atomic::{AtomicUsize, Ordering},
};

use super::{
    host_scope::{InstanceId, with_host_scope},
    *,
};

fn limits() -> instance::CallbackLimits {
    instance::CallbackLimits {
        configuration_bytes: 128,
        state_bytes: 128,
        event_bytes: 128,
    }
}

#[test]
fn owning_instance_refuses_mismatched_abi_build_descriptor_and_invalid_limits() {
    for mutation in 0..7 {
        let mut api = StrategyApi::for_algorithm::<Counter>();
        let mut expected = Counter::DESCRIPTOR_JSON.as_bytes();
        let mut capacity = limits();
        match mutation {
            0 => api.header.abi_version = 0,
            1 => api.header.struct_size -= 1,
            2 => api.build.rustc_version = BorrowedStr::from_str("different compiler"),
            3 => expected = b"different descriptor",
            4 => capacity.state_bytes = 0,
            5 => api.panic_mode = 2,
            6 => api.allocation_mode = 0,
            _ => unreachable!(),
        }
        let api = Box::leak(Box::new(api));
        // SAFETY: the readable table is deliberately incompatible, but all
        // producer functions and storage remain valid for process lifetime.
        assert!(
            unsafe { instance::GuestInstance::create(api, expected, b"counter-v1", capacity) }
                .is_err()
        );
    }
}

#[test]
fn owning_instance_latches_original_panic_and_cannot_restore_it_away() {
    let api = Box::leak(Box::new(StrategyApi::for_algorithm::<Counter>()));
    // SAFETY: static executable table and guest thunks have process lifetime
    let mut guest = unsafe {
        instance::GuestInstance::create(
            api,
            Counter::DESCRIPTOR_JSON.as_bytes(),
            b"counter-v1",
            limits(),
        )
    }
    .unwrap();
    let mut facade = |_, _: &[u8]| Ok(Vec::new());
    guest
        .invoke(CallbackKind::Event, 1, b"event", &mut facade)
        .unwrap();
    assert_eq!(
        guest
            .invoke(CallbackKind::Save, 1, b"", &mut facade)
            .unwrap(),
        1u64.to_le_bytes()
    );
    let error = guest
        .invoke(CallbackKind::Event, 1, b"panic", &mut facade)
        .unwrap_err();
    assert_eq!(error.code, PluginErrorCode::Panic);
    assert_eq!(error.message_string(), "counter callback original failure");
    assert!(guest.is_faulted());
    let error = guest
        .invoke(CallbackKind::Restore, 1, &0u64.to_le_bytes(), &mut facade)
        .unwrap_err();
    assert_eq!(error.code, PluginErrorCode::Panic);
    assert_eq!(error.message_string(), "counter callback original failure");
}

static DISPOSED: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug)]
struct Counter(u64);

fn apply_counter_event(counter: &mut u64, event: &[u8]) {
    match event {
        b"panic" => panic!("counter callback original failure"),
        _ => *counter += 1,
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        DISPOSED.fetch_add(1, Ordering::SeqCst);
    }
}

impl Algorithm for Counter {
    const DESCRIPTOR_JSON: &'static str = r#"{"algorithm_id":"counter"}"#;
    fn validate(configuration: &[u8]) -> Result<(), PluginError> {
        if configuration == b"counter-v1" {
            Ok(())
        } else {
            Err(PluginError::generic("invalid counter configuration"))
        }
    }
    fn create(_: &[u8]) -> Result<Self, PluginError> {
        Ok(Self(0))
    }
    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        host.call(HostOperation::Clock, b"")?;
        Ok(())
    }
    fn on_event(&mut self, event: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        apply_counter_event(&mut self.0, event);
        Ok(())
    }
    fn save(&self, _: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        Ok(self.0.to_le_bytes().to_vec())
    }
    fn restore(
        &mut self,
        version: u32,
        state: &[u8],
        _: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError> {
        if version != 1 || state.len() != 8 {
            return Err(PluginError::generic("invalid counter state"));
        }
        self.0 = u64::from_le_bytes(state.try_into().unwrap());
        Ok(())
    }
    fn pause(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn resume(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn stop(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
}

fn result(response: CallResult) -> Result<Vec<u8>, PluginError> {
    // SAFETY: the result is producer-owned live storage from the tested thunk
    unsafe { response.copy_result(MAX_PAYLOAD_BYTES) }
}

#[test]
fn encoded_guest_lifecycle_keeps_full_state_and_producer_disposal() {
    let api = StrategyApi::for_algorithm::<Counter>();
    let instance = InstanceId::allocate().unwrap();
    let mut handle: *mut c_void = std::ptr::null_mut();
    // SAFETY: configuration and output storage remain live for this call
    result(unsafe { (api.create)(Slice::from_slice(b"counter-v1"), &raw mut handle) }).unwrap();
    assert!(!handle.is_null());
    let mut calls = Vec::new();
    let mut facade = |operation, _: &[u8]| {
        calls.push(operation);
        Ok(Vec::new())
    };
    for kind in [
        CallbackKind::Start,
        CallbackKind::Event,
        CallbackKind::Pause,
        CallbackKind::Resume,
    ] {
        let output = with_host_scope(instance, &mut facade, |lease| {
            // SAFETY: the created handle is exclusively held until disposal
            unsafe { (api.callback)(handle, kind.tag(), 1, Slice::empty(), lease) }
        })
        .unwrap();
        result(output).unwrap();
    }
    let saved = with_host_scope(instance, &mut facade, |lease| {
        // SAFETY: the handle is live and exclusively held
        unsafe { (api.callback)(handle, CallbackKind::Save.tag(), 1, Slice::empty(), lease) }
    })
    .unwrap();
    assert_eq!(result(saved).unwrap(), 1u64.to_le_bytes());
    let state = 27u64.to_le_bytes();
    let restored = with_host_scope(instance, &mut facade, |lease| {
        // SAFETY: both handle and state remain live for this call
        unsafe {
            (api.callback)(
                handle,
                CallbackKind::Restore.tag(),
                1,
                Slice::from_slice(&state),
                lease,
            )
        }
    })
    .unwrap();
    result(restored).unwrap();
    let saved = with_host_scope(instance, &mut facade, |lease| {
        // SAFETY: the handle is live and exclusively held
        unsafe { (api.callback)(handle, CallbackKind::Save.tag(), 1, Slice::empty(), lease) }
    })
    .unwrap();
    assert_eq!(result(saved).unwrap(), state);
    assert_eq!(calls, vec![HostOperation::Clock]);
    let before = DISPOSED.load(Ordering::SeqCst);
    // SAFETY: this producer releases its handle exactly once
    result(unsafe { (api.dispose)(handle) }).unwrap();
    assert_eq!(DISPOSED.load(Ordering::SeqCst), before + 1);
}

#[test]
fn callback_panic_keeps_original_error_and_does_not_unwind_across_abi() {
    let api = StrategyApi::for_algorithm::<Counter>();
    let mut handle = std::ptr::null_mut();
    // SAFETY: arguments remain live and the output is writable
    result(unsafe { (api.create)(Slice::from_slice(b"counter-v1"), &raw mut handle) }).unwrap();
    let mut facade = |_, _: &[u8]| Ok(Vec::new());
    let output = with_host_scope(InstanceId::allocate().unwrap(), &mut facade, |lease| {
        // SAFETY: arguments remain live and the handle is exclusively owned
        unsafe {
            (api.callback)(
                handle,
                CallbackKind::Event.tag(),
                1,
                Slice::from_slice(b"panic"),
                lease,
            )
        }
    })
    .unwrap();
    let error = result(output).unwrap_err();
    assert_eq!(error.code, PluginErrorCode::Panic);
    assert_eq!(error.message_string(), "counter callback original failure");
    // SAFETY: failed callback does not change the allocation's producer
    result(unsafe { (api.dispose)(handle) }).unwrap();
}

#[test]
fn expired_cross_thread_cross_instance_and_nested_leases_never_call_native_owner() {
    let instance = InstanceId::allocate().unwrap();
    let mut calls = 0;
    let mut facade = |_, _: &[u8]| {
        calls += 1;
        Ok(Vec::new())
    };
    let lease = with_host_scope(instance, &mut facade, |lease| {
        // SAFETY: only a valid empty borrowed buffer is passed to each entry
        assert!(
            result(unsafe {
                (lease.call)(lease.instance + 1, lease.generation, 1, Slice::empty())
            })
            .is_err()
        );
        // SAFETY: the lease is deliberately transported to a different thread;
        // that thread must refuse it before looking at callback-local pointers.
        assert!(
            std::thread::spawn(move || result(unsafe {
                (lease.call)(lease.instance, lease.generation, 1, Slice::empty())
            })
            .is_err())
            .join()
            .unwrap()
        );
        let mut nested = |_, _: &[u8]| Ok(Vec::new());
        assert!(with_host_scope(instance, &mut nested, |_| ()).is_err());
        lease
    })
    .unwrap();
    // SAFETY: this expired numeric lease contains no dangling storage pointer
    assert!(
        result(unsafe { (lease.call)(lease.instance, lease.generation, 1, Slice::empty()) })
            .is_err()
    );
    with_host_scope(instance, &mut facade, |current| {
        assert_ne!(current.generation, lease.generation);
        // SAFETY: stale generation must be refused before native access
        assert!(
            result(unsafe { (lease.call)(lease.instance, lease.generation, 1, Slice::empty()) })
                .is_err()
        );
    })
    .unwrap();
    assert_eq!(calls, 0);
}

#[test]
fn scope_is_removed_when_the_invoker_unwinds() {
    let instance = InstanceId::allocate().unwrap();
    let mut facade = |_, _: &[u8]| Ok(Vec::new());
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_host_scope(instance, &mut facade, |_| panic!("invoker failure"))
    }));
    assert!(panic.is_err());
    assert!(with_host_scope(instance, &mut facade, |_| ()).is_ok());
}

#[test]
fn native_host_panics_and_reentrant_calls_are_contained() {
    let instance = InstanceId::allocate().unwrap();
    let mut panic_facade =
        |_, _: &[u8]| -> Result<Vec<u8>, PluginError> { panic!("native facade failure") };
    let output = with_host_scope(instance, &mut panic_facade, |lease| {
        // SAFETY: scope and empty input are live
        unsafe { (lease.call)(lease.instance, lease.generation, 1, Slice::empty()) }
    })
    .unwrap();
    let error = result(output).unwrap_err();
    assert_eq!(error.code, PluginErrorCode::Panic);
    assert_eq!(error.message_string(), "native facade failure");
    let lease_slot = std::cell::Cell::new(None::<HostLease>);
    let mut reentrant = |_, _: &[u8]| {
        let lease = lease_slot.get().unwrap();
        // SAFETY: the active native owner is already mutably borrowed and
        // must refuse a recursive call before dereferencing it a second time.
        result(unsafe { (lease.call)(lease.instance, lease.generation, 1, Slice::empty()) })
    };
    let output = with_host_scope(instance, &mut reentrant, |lease| {
        lease_slot.set(Some(lease));
        // SAFETY: scope and empty input are live
        unsafe { (lease.call)(lease.instance, lease.generation, 1, Slice::empty()) }
    })
    .unwrap();
    assert!(
        result(output)
            .unwrap_err()
            .message_string()
            .contains("reentrant")
    );
}

#[test]
fn invalid_buffer_tag_and_configuration_are_refused_before_guest_access() {
    let api = StrategyApi::for_algorithm::<Counter>();
    let mut output = std::ptr::null_mut();
    // SAFETY: the storage is valid, even though the configuration is unsupported
    assert!(result(unsafe { (api.create)(Slice::from_slice(b"wrong"), &raw mut output) }).is_err());
    assert!(output.is_null());
    let mut invalid = Slice::empty();
    invalid.len = MAX_PAYLOAD_BYTES + 1;
    // SAFETY: length is refused before any pointer read
    assert!(result(unsafe { (api.validate)(invalid) }).is_err());
    let instance = InstanceId::allocate().unwrap();
    let mut calls = 0;
    let mut facade = |_, _: &[u8]| {
        calls += 1;
        Ok(Vec::new())
    };
    with_host_scope(instance, &mut facade, |lease| {
        // SAFETY: unknown operation is refused before native access
        assert!(
            result(unsafe {
                (lease.call)(lease.instance, lease.generation, u32::MAX, Slice::empty())
            })
            .is_err()
        );
    })
    .unwrap();
    assert_eq!(calls, 0);
}

#[test]
fn terminal_checkpoint_failure_only_restricts_an_unused_guest() {
    let api = Box::leak(Box::new(StrategyApi::for_algorithm::<Counter>()));
    let create = || {
        // SAFETY: generated static table and producer storage remain live.
        unsafe {
            instance::GuestInstance::create(
                api,
                Counter::DESCRIPTOR_JSON.as_bytes(),
                b"counter-v1",
                limits(),
            )
        }
        .unwrap()
    };
    let mut guest = create();
    assert!(
        guest
            .retain_checkpoint_failure(PluginErrorCode::Ok, "not a failure")
            .is_err()
    );
    assert!(
        guest
            .retain_checkpoint_failure(PluginErrorCode::Panic, &"x".repeat(MAX_ERROR_BYTES + 1))
            .is_err()
    );
    guest
        .retain_checkpoint_failure(PluginErrorCode::Panic, "original terminal panic")
        .unwrap();
    let mut facade = |_, _: &[u8]| Ok(Vec::new());
    for kind in [
        CallbackKind::Start,
        CallbackKind::Event,
        CallbackKind::Save,
        CallbackKind::Restore,
        CallbackKind::Resume,
    ] {
        let failure = guest.invoke(kind, 1, b"", &mut facade).unwrap_err();
        assert_eq!(failure.code, PluginErrorCode::Panic);
        assert_eq!(failure.message_string(), "original terminal panic");
    }
    assert!(
        guest
            .retain_checkpoint_failure(PluginErrorCode::Generic, "replacement")
            .is_err()
    );
    assert_eq!(
        guest.first_failure(),
        Some((PluginErrorCode::Panic, "original terminal panic"))
    );
    let mut used = create();
    used.invoke(CallbackKind::Event, 1, b"event", &mut facade)
        .unwrap();
    assert!(
        used.retain_checkpoint_failure(PluginErrorCode::Panic, "old failure")
            .is_err()
    );
    assert!(!used.is_faulted());
    assert_eq!(
        used.invoke(CallbackKind::Save, 1, b"", &mut facade)
            .unwrap(),
        1u64.to_le_bytes()
    );
}

#[test]
fn owning_guest_restore_precedes_every_runtime_callback_and_cannot_repeat() {
    let api = Box::leak(Box::new(StrategyApi::for_algorithm::<Counter>()));
    // SAFETY: static generated table and producer storage remain live.
    let mut guest = unsafe {
        instance::GuestInstance::create(
            api,
            Counter::DESCRIPTOR_JSON.as_bytes(),
            b"counter-v1",
            limits(),
        )
    }
    .unwrap();
    let mut facade = |_, _: &[u8]| Ok(Vec::new());
    assert_eq!(
        guest
            .invoke(CallbackKind::Save, 1, b"", &mut facade)
            .unwrap(),
        0u64.to_le_bytes()
    );
    guest
        .invoke(CallbackKind::Restore, 1, &7u64.to_le_bytes(), &mut facade)
        .unwrap();
    guest
        .invoke(CallbackKind::Event, 1, b"event", &mut facade)
        .unwrap();
    assert_eq!(
        guest
            .invoke(CallbackKind::Save, 1, b"", &mut facade)
            .unwrap(),
        8u64.to_le_bytes()
    );
    let failure = guest
        .invoke(CallbackKind::Restore, 1, &0u64.to_le_bytes(), &mut facade)
        .unwrap_err();
    assert_eq!(failure.code, PluginErrorCode::InvalidArgument);
    assert!(failure.message_string().contains("unused physical guest"));
    assert!(
        guest
            .invoke(CallbackKind::Start, 1, b"", &mut facade)
            .is_err()
    );
}
