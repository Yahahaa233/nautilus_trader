// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Host-side ownership of an executable guest. Native component state and the
//! runner remain with the engine; this object owns only the guest allocation
//! and a first-failure latch. There is no second execution lifecycle.

use std::{ffi::c_void, ptr::NonNull};

use super::{
    CallbackKind, HostOperation, MAX_PAYLOAD_BYTES, STRATEGY_ABI_VERSION, StrategyApi,
    StrategyApiHeader,
    host_scope::{InstanceId, with_host_scope},
};
use crate::{BorrowedStr, PluginBuildId, PluginError, PluginErrorCode, Slice};

/// Declared limits already checked against the server's fixed capacity.
#[derive(Clone, Copy, Debug)]
pub struct CallbackLimits {
    pub configuration_bytes: usize,
    pub state_bytes: usize,
    pub event_bytes: usize,
}

/// Host-owned state contract bound to the admitted executable descriptor.
/// Validation receives bytes only, without a mutable native host capability.
/// Every owning Save and Restore uses this contract, including recovery calls.
pub trait StateValidator: std::fmt::Debug {
    /// Validates the exact declared version and complete algorithm state.
    ///
    /// # Errors
    ///
    /// Refuses incomplete, unknown, oversized or incompatible state values.
    fn validate(&self, version: u32, state: &[u8]) -> Result<(), PluginError>;
}

/// Retains the first original callback error while the native owner handles
/// fault, stop and recovery. A faulted guest cannot silently resume callbacks.
#[derive(Debug)]
pub struct GuestInstance {
    api: &'static StrategyApi,
    handle: NonNull<c_void>,
    instance: InstanceId,
    limits: CallbackLimits,
    failure: Option<(PluginErrorCode, String)>,
    callbacks_used: bool,
    state_validator: Box<dyn StateValidator>,
}

fn invalid(message: &str) -> PluginError {
    PluginError::new(PluginErrorCode::InvalidArgument, message)
}

unsafe fn text(value: BorrowedStr<'_>) -> Result<&str, PluginError> {
    if value.len > 1_048_576 || (value.len > 0 && value.ptr.is_null()) {
        return Err(invalid("invalid component metadata buffer"));
    }
    // SAFETY: the loader pins the producing library for process lifetime
    unsafe { value.try_as_str() }.map_err(|_| invalid("invalid component metadata UTF-8"))
}

impl GuestInstance {
    /// Validates the full table/build/descriptor before constructing a guest.
    /// Metadata inspection must precede this call in the loader, because loading
    /// a native library can execute constructors before its entry point.
    ///
    /// # Errors
    ///
    /// Refuses incompatible tables/builds, mismatched descriptors, invalid
    /// configuration, oversized buffers or a failed guest constructor.
    ///
    /// # Safety
    ///
    /// `api` points to at least a readable header from a trusted loaded library.
    /// If its header declares this exact table size, the full table is valid.
    /// The library and all dependencies remain loaded until process exit.
    /// Functions uphold the documented C ABI and allocation contracts.
    pub unsafe fn create(
        api: *const StrategyApi,
        descriptor: &[u8],
        configuration: &[u8],
        limits: CallbackLimits,
        state_validator: Box<dyn StateValidator>,
    ) -> Result<Self, PluginError> {
        if api.is_null() {
            return Err(invalid("null executable strategy API"));
        }
        // SAFETY: the loader ensures this readable prefix is present
        let header = unsafe { api.cast::<StrategyApiHeader>().read() };
        if header.abi_version != STRATEGY_ABI_VERSION
            || header.struct_size as usize != size_of::<StrategyApi>()
        {
            return Err(PluginError::new(
                PluginErrorCode::AbiMismatch,
                "incompatible executable strategy ABI or table size",
            ));
        }
        // SAFETY: exact header match and the trusted loader contract establish
        // the full table's layout and process-long library lifetime.
        let api = unsafe { &*api };
        if !cfg!(panic = "unwind")
            || api.panic_mode != super::PANIC_CONTAINED_UNWIND
            || api.allocation_mode != super::ALLOCATION_PRODUCER_RELEASES
        {
            return Err(PluginError::new(
                PluginErrorCode::AbiMismatch,
                "incompatible panic containment or producer allocation policy",
            ));
        }
        let host = PluginBuildId::current();
        if api.build.schema_version != host.schema_version
            || api.build.fixed_precision != host.fixed_precision
        {
            return Err(PluginError::new(
                PluginErrorCode::AbiMismatch,
                "incompatible strategy build schema or precision",
            ));
        }
        for (guest, host) in [
            (
                api.build.nautilus_plugin_version,
                host.nautilus_plugin_version,
            ),
            (api.build.rustc_version, host.rustc_version),
            (api.build.target_triple, host.target_triple),
            (api.build.build_profile, host.build_profile),
            (api.build.precision_mode, host.precision_mode),
        ] {
            // SAFETY: both producer strings remain live for process lifetime
            let guest = unsafe { text(guest)? };
            // SAFETY: the host's static build strings remain live
            let host = unsafe { text(host)? };
            if guest.is_empty() || guest != host {
                return Err(PluginError::new(
                    PluginErrorCode::AbiMismatch,
                    "strategy differs from the actual host build",
                ));
            }
        }
        // SAFETY: the producing library remains pinned
        let declared = unsafe { text(api.descriptor_json)? };
        if descriptor.is_empty() || declared.as_bytes() != descriptor {
            return Err(invalid(
                "strategy descriptor differs from admitted publication",
            ));
        }
        if limits.configuration_bytes == 0
            || limits.state_bytes == 0
            || limits.event_bytes == 0
            || limits.configuration_bytes > MAX_PAYLOAD_BYTES
            || limits.state_bytes > MAX_PAYLOAD_BYTES
            || limits.event_bytes > MAX_PAYLOAD_BYTES
            || configuration.len() > limits.configuration_bytes
        {
            return Err(invalid("invalid component resource limits"));
        }
        // SAFETY: configuration is a live bounded byte slice
        let response = unsafe { (api.validate)(Slice::from_slice(configuration)) };
        // SAFETY: producer owns the result until this copy releases it
        let output = unsafe { response.copy_result(MAX_PAYLOAD_BYTES) }?;
        if !output.is_empty() {
            return Err(invalid("validation returned an unexpected payload"));
        }
        let instance = InstanceId::allocate()?;
        let mut handle = std::ptr::null_mut();
        // SAFETY: input/output storage remains live for the synchronous constructor
        let response = unsafe { (api.create)(Slice::from_slice(configuration), &raw mut handle) };
        // SAFETY: producer owns the result until this copy releases it
        let output = unsafe { response.copy_result(MAX_PAYLOAD_BYTES) }?;
        let handle = NonNull::new(handle)
            .ok_or_else(|| invalid("strategy constructor returned a null instance"))?;
        let mut instance = Self {
            api,
            handle,
            instance,
            limits,
            failure: None,
            callbacks_used: false,
            state_validator,
        };
        if !output.is_empty() {
            instance.failure = Some((
                PluginErrorCode::InvalidArgument,
                "constructor returned an unexpected payload".into(),
            ));
            return Err(invalid("constructor returned an unexpected payload"));
        }
        Ok(instance)
    }

    /// Invokes a guest from an actual native component callback.
    ///
    /// # Errors
    ///
    /// Retains and returns the original first failure. No later callback can
    /// clear it; recovery constructs a new instance from the durable native cut.
    pub fn invoke<F>(
        &mut self,
        kind: CallbackKind,
        version: u32,
        input: &[u8],
        facade: &mut F,
    ) -> Result<Vec<u8>, PluginError>
    where
        F: FnMut(HostOperation, &[u8]) -> Result<Vec<u8>, PluginError>,
    {
        if let Some((code, message)) = &self.failure {
            return Err(PluginError::new(*code, message));
        }
        let already_used = self.callbacks_used;
        // A successful immutable Save can inspect the fresh target without
        // consuming its one Restore. Runtime callbacks and Restore cannot.
        self.callbacks_used |= kind != CallbackKind::Save;
        let bound = match kind {
            CallbackKind::Restore => self.limits.state_bytes,
            CallbackKind::Event => self.limits.event_bytes,
            CallbackKind::Stop => 1024,
            _ => 0,
        };
        let mut native_failure: Option<(PluginErrorCode, String)> = None;
        let mut watched_facade = |operation, payload: &[u8]| {
            if let Some((code, message)) = &native_failure {
                return Err(PluginError::new(*code, message));
            }
            let result = crate::panic::guard(|| facade(operation, payload)).into_result();
            if let Err(error) = &result
                && matches!(
                    error.code,
                    PluginErrorCode::Panic | PluginErrorCode::SerializationFailed
                )
            {
                native_failure = Some((error.code, error.message_string()));
            }
            result
        };
        let result = if kind == CallbackKind::Restore && already_used {
            Err(invalid(
                "algorithm Restore requires an unused physical guest",
            ))
        } else if input.len() > bound {
            Err(invalid("callback input exceeds the declared bound"))
        } else {
            let admitted = if kind == CallbackKind::Restore {
                self.validate_state(version, input)
            } else {
                Ok(())
            };
            admitted
                .and_then(|()| {
                    with_host_scope(self.instance, &mut watched_facade, |lease| {
                        // SAFETY: self owns the live guest and its pinned API;
                        // the current scope holds exclusive native facade access.
                        let response = unsafe {
                            (self.api.callback)(
                                self.handle.as_ptr(),
                                kind.tag(),
                                version,
                                Slice::from_slice(input),
                                lease,
                            )
                        };
                        let bound = if kind == CallbackKind::Save {
                            self.limits.state_bytes
                        } else {
                            0
                        };
                        // SAFETY: response is producer-owned live storage
                        unsafe { response.copy_result(bound) }
                    })
                    .and_then(std::convert::identity)
                })
                .and_then(|output| {
                    if kind == CallbackKind::Save {
                        self.validate_state(version, &output)?;
                    }
                    Ok(output)
                })
        };
        // A producer cannot swallow a native panic or durability failure and
        // continue host effects. This happened before any later producer error.
        let result = if let Some((code, message)) = native_failure {
            Err(PluginError::new(code, &message))
        } else {
            result
        };
        if let Err(error) = &result {
            self.failure = Some((error.code, error.message_string()));
        }
        result
    }

    fn validate_state(&self, version: u32, state: &[u8]) -> Result<(), PluginError> {
        crate::panic::guard(|| self.state_validator.validate(version, state))
            .into_result()
            .map_err(|e| {
                if e.code == PluginErrorCode::Ok
                    || e.message_string().len() > super::MAX_ERROR_BYTES
                {
                    invalid("state validator returned an invalid failure")
                } else {
                    e
                }
            })
    }

    /// Latches an actual native admission failure before entering a guest callback.
    /// A used guest remains failed; later failures cannot replace the first one.
    /// This grants neither Restore nor checkpoint authority.
    /// # Errors
    /// Refuses success codes or unbounded diagnostic text without clearing any failure.
    pub fn retain_native_failure(
        &mut self,
        code: PluginErrorCode,
        message: &str,
    ) -> Result<(), PluginError> {
        if code == PluginErrorCode::Ok || message.len() > super::MAX_ERROR_BYTES {
            return Err(invalid("native failure requires bounded original error"));
        }
        if self.failure.is_none() {
            self.failure = Some((code, message.to_owned()));
        }
        self.callbacks_used = true;
        Ok(())
    }

    /// Reinstates only a terminal failure from a checked checkpoint into an
    /// unused physical guest. It never calls the algorithm or enables startup.
    ///
    /// # Errors
    ///
    /// Refuses a used/faulted guest, success code or oversized failure text.
    pub fn retain_checkpoint_failure(
        &mut self,
        code: PluginErrorCode,
        message: &str,
    ) -> Result<(), PluginError> {
        if self.callbacks_used
            || self.failure.is_some()
            || code == PluginErrorCode::Ok
            || message.len() > super::MAX_ERROR_BYTES
        {
            return Err(invalid(
                "terminal failure requires an unused guest and bounded original error",
            ));
        }
        self.failure = Some((code, message.to_owned()));
        self.callbacks_used = true;
        Ok(())
    }

    /// Returns the original callback failure without clearing or replacing it.
    #[must_use]
    pub fn first_failure(&self) -> Option<(PluginErrorCode, &str)> {
        self.failure
            .as_ref()
            .map(|(code, message)| (*code, message.as_str()))
    }

    /// Returns whether the native owner must recover or dispose this instance.
    #[must_use]
    pub const fn is_faulted(&self) -> bool {
        self.failure.is_some()
    }
}

impl Drop for GuestInstance {
    fn drop(&mut self) {
        crate::panic::guard_drop("strategy_instance_dispose", || {
            // SAFETY: this object is the sole owner; the library remains pinned.
            // A fault cannot change which producer must release its allocation.
            let response = unsafe { (self.api.dispose)(self.handle.as_ptr()) };
            // SAFETY: producer owns its final response and the library remains loaded
            if let Err(error) = unsafe { response.copy_result(MAX_PAYLOAD_BYTES) } {
                log::error!("Strategy disposal failed: {}", error.message_string());
            }
        });
    }
}
