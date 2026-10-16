// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Versioned executable strategy boundary, separate from experimental ABI 0.
//!
//! Native model values are encoded by the host; Rust layouts never cross this
//! boundary. The guest owns its instance and every returned buffer. The host
//! keeps the library loaded for the process lifetime, including error buffers
//! and any callback functions retained by the native engine.

#![allow(unsafe_code)]

use std::{ffi::c_void, marker::PhantomData};

use crate::{BorrowedStr, OwnedBytes, PluginBuildId, PluginError, PluginErrorCode, Slice};

#[cfg(feature = "host")]
pub mod elf;
pub mod guest;
pub mod host_scope;
pub mod instance;
#[cfg(all(
    feature = "host",
    target_os = "linux",
    target_env = "gnu",
    target_arch = "x86_64"
))]
pub mod linux_loader;
pub mod wire;

#[cfg(test)]
mod tests;

/// First version of the encoded strategy component ABI.
pub const STRATEGY_ABI_VERSION: u32 = 1;
/// Panic behavior required by contained guest and native facade callbacks.
pub const PANIC_CONTAINED_UNWIND: u32 = 1;
/// Every boundary allocation is released by its actual producer.
pub const ALLOCATION_PRODUCER_RELEASES: u32 = 1;
/// Entry symbol, independent of the metadata-only entry point.
pub const STRATEGY_INIT_SYMBOL: &[u8] = b"nautilus_strategy_init_v1\0";
/// Absolute transport bound; a host applies the smaller declared resource limits too.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
/// Error detail has its own bound, including callbacks with no success payload.
pub const MAX_ERROR_BYTES: usize = 65_536;

/// Numeric tags are validated before converting to Rust enums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CallbackKind {
    Start,
    Event,
    Save,
    Restore,
    Pause,
    Resume,
    Stop,
}

impl CallbackKind {
    /// Returns the fixed ABI tag.
    #[must_use]
    pub const fn tag(self) -> u32 {
        match self {
            Self::Start => 1,
            Self::Event => 2,
            Self::Save => 3,
            Self::Restore => 4,
            Self::Pause => 5,
            Self::Resume => 6,
            Self::Stop => 7,
        }
    }

    /// Parses a fixed ABI tag.
    #[must_use]
    pub const fn from_tag(tag: u32) -> Option<Self> {
        match tag {
            1 => Some(Self::Start),
            2 => Some(Self::Event),
            3 => Some(Self::Save),
            4 => Some(Self::Restore),
            5 => Some(Self::Pause),
            6 => Some(Self::Resume),
            7 => Some(Self::Stop),
            _ => None,
        }
    }
}

/// Operations are dispatched to the native `StrategyCore` facade, never to a
/// guest-owned venue client, runner, clock or economic ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostOperation {
    Clock,
    Instrument,
    Portfolio,
    Subscribe,
    Unsubscribe,
    RequestHistory,
    SetTimer,
    CancelTimer,
    Submit,
    Modify,
    Cancel,
    Query,
    Log,
}

impl HostOperation {
    /// Returns the fixed ABI tag.
    #[must_use]
    pub const fn tag(self) -> u32 {
        match self {
            Self::Clock => 1,
            Self::Instrument => 2,
            Self::Portfolio => 3,
            Self::Subscribe => 4,
            Self::Unsubscribe => 5,
            Self::RequestHistory => 6,
            Self::SetTimer => 7,
            Self::CancelTimer => 8,
            Self::Submit => 9,
            Self::Modify => 10,
            Self::Cancel => 11,
            Self::Query => 12,
            Self::Log => 13,
        }
    }

    /// Parses a fixed ABI tag.
    #[must_use]
    pub const fn from_tag(tag: u32) -> Option<Self> {
        match tag {
            1 => Some(Self::Clock),
            2 => Some(Self::Instrument),
            3 => Some(Self::Portfolio),
            4 => Some(Self::Subscribe),
            5 => Some(Self::Unsubscribe),
            6 => Some(Self::RequestHistory),
            7 => Some(Self::SetTimer),
            8 => Some(Self::CancelTimer),
            9 => Some(Self::Submit),
            10 => Some(Self::Modify),
            11 => Some(Self::Cancel),
            12 => Some(Self::Query),
            13 => Some(Self::Log),
            _ => None,
        }
    }
}

/// No Rust enum discriminant is accepted from a foreign library. Zero means
/// success; nonzero values use the numeric `PluginErrorCode` categories.
#[repr(C)]
#[must_use]
pub struct CallResult {
    pub status: u32,
    pub payload: OwnedBytes,
}

impl std::fmt::Debug for CallResult {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CallResult")
            .field("status", &self.status)
            .field("payload_bytes", &self.payload.len)
            .finish_non_exhaustive()
    }
}

impl CallResult {
    /// Constructs a producer-owned response.
    pub fn success(payload: Vec<u8>) -> Self {
        Self {
            status: 0,
            payload: OwnedBytes::from_vec(payload),
        }
    }

    /// Constructs a producer-owned error response.
    pub fn failure(error: PluginError) -> Self {
        Self {
            status: error.code as u32,
            payload: error.message,
        }
    }

    /// Copies a bounded response before the producer releases its buffer.
    ///
    /// # Errors
    ///
    /// Returns the original error or refuses invalid/beyond-bound buffers.
    ///
    /// # Safety
    ///
    /// The producer must supply live storage and its own valid deallocator.
    pub unsafe fn copy_result(mut self, bound: usize) -> Result<Vec<u8>, PluginError> {
        let bound = if self.status == 0 {
            bound
        } else {
            MAX_ERROR_BYTES
        };
        if self.payload.len > self.payload.cap
            || (self.payload.cap > 0
                && (self.payload.ptr.is_null() || self.payload.drop_fn.is_none()))
        {
            // A malformed allocation description cannot safely be given back
            // to its deallocator, retain it rather than guessing a Rust layout.
            self.payload.drop_fn = None;
            return Err(PluginError::new(
                PluginErrorCode::InvalidArgument,
                "invalid component response buffer",
            ));
        }
        if self.payload.len > bound {
            return Err(PluginError::new(
                PluginErrorCode::InvalidArgument,
                "component response exceeds bound",
            ));
        }
        // SAFETY: the caller ensures live producer storage; length was bounded above
        let bytes = unsafe { self.payload.as_bytes() }.to_vec();
        if self.status == 0 {
            Ok(bytes)
        } else {
            let code = match self.status {
                1 => PluginErrorCode::Generic,
                2 => PluginErrorCode::Panic,
                3 => PluginErrorCode::InvalidArgument,
                4 => PluginErrorCode::NotImplemented,
                5 => PluginErrorCode::AbiMismatch,
                6 => PluginErrorCode::SerializationFailed,
                _ => {
                    return Err(PluginError::new(
                        PluginErrorCode::InvalidArgument,
                        "unknown component error code",
                    ));
                }
            };
            Err(PluginError::new(code, String::from_utf8_lossy(&bytes)))
        }
    }
}

/// Callback-scoped capability. No pointer to temporary host storage escapes.
/// A host resolves both numbers only on the issuing thread and while that
/// exact callback is active. Numbers must never be recycled within a process.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HostLease {
    pub instance: u64,
    pub generation: u64,
    pub call: unsafe extern "C" fn(u64, u64, u32, Slice<'_, u8>) -> CallResult,
}

/// Borrowed host facade passed to a guest method. It cannot escape through a
/// safe guest implementation, be cloned or be sent to a background thread.
#[derive(Debug)]
pub struct Host<'a> {
    lease: HostLease,
    _lifetime: PhantomData<&'a mut ()>,
    _not_send: PhantomData<std::rc::Rc<()>>,
}

impl Host<'_> {
    pub(crate) const fn new(lease: HostLease) -> Self {
        Self {
            lease,
            _lifetime: PhantomData,
            _not_send: PhantomData,
        }
    }

    /// Calls a declared native facade using a bounded encoded message.
    ///
    /// # Errors
    ///
    /// Returns the native failure, including expired/cross-scope lease refusal.
    pub fn call(
        &mut self,
        operation: HostOperation,
        payload: &[u8],
    ) -> Result<Vec<u8>, PluginError> {
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(PluginError::new(
                PluginErrorCode::InvalidArgument,
                "host call exceeds transport bound",
            ));
        }
        // SAFETY: the host supplies a process-lifetime function. Its registry
        // validates the lease before accessing any callback-local native state.
        let response = unsafe {
            (self.lease.call)(
                self.lease.instance,
                self.lease.generation,
                operation.tag(),
                Slice::from_slice(payload),
            )
        };
        // SAFETY: the trusted host returned producer-owned storage
        unsafe { response.copy_result(MAX_PAYLOAD_BYTES) }
    }
}

/// Operations that cannot mutate subscriptions, timers, orders or execution
/// authority. The native owner may restrict these further during a snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadOperation {
    Clock,
    Instrument,
    Portfolio,
    Order,
}

/// Read-only callback capability used by Save/Restore. Safe algorithm code
/// cannot obtain an order, timer or subscription mutation facade from it.
///
/// ```compile_fail,E0308
/// use nautilus_plugin::{PluginError, strategy::{HostOperation, ReadOnlyHost}};
/// fn submit_during_save(host: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
///     host.call(HostOperation::Submit, b"{}")
/// }
/// ```
#[derive(Debug)]
pub struct ReadOnlyHost<'a> {
    host: Host<'a>,
}

impl ReadOnlyHost<'_> {
    pub(crate) const fn new(lease: HostLease) -> Self {
        Self {
            host: Host::new(lease),
        }
    }

    /// Calls a read-only native facade under the same expiring lease.
    ///
    /// # Errors
    ///
    /// Returns the original native error or a scoped capability refusal.
    pub fn call(
        &mut self,
        operation: ReadOperation,
        payload: &[u8],
    ) -> Result<Vec<u8>, PluginError> {
        let operation = match operation {
            ReadOperation::Clock => HostOperation::Clock,
            ReadOperation::Instrument => HostOperation::Instrument,
            ReadOperation::Portfolio => HostOperation::Portfolio,
            ReadOperation::Order => HostOperation::Query,
        };
        self.host.call(operation, payload)
    }
}

/// Required algorithm code contract. Every lifecycle method is explicit;
/// there are no default success implementations or inferred recovery support.
pub trait Algorithm: Sized + 'static {
    /// Frozen algorithm identity, capabilities and schema hashes, as JSON.
    const DESCRIPTOR_JSON: &'static str;

    /// Checks the algorithm-specific configuration without host side effects.
    ///
    /// # Errors
    ///
    /// Returns an error when the configuration is unsupported.
    fn validate(configuration: &[u8]) -> Result<(), PluginError>;
    /// Creates pure algorithm state, without a host capability.
    ///
    /// # Errors
    ///
    /// Returns an error when creation fails.
    fn create(configuration: &[u8]) -> Result<Self, PluginError>;
    /// Starts native subscriptions and warmup.
    ///
    /// # Errors
    ///
    /// Returns the original startup failure.
    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError>;
    /// Handles one versioned event in native dispatch order.
    ///
    /// # Errors
    ///
    /// Returns the original event failure.
    fn on_event(&mut self, event: &[u8], host: &mut Host<'_>) -> Result<(), PluginError>;
    /// Saves complete algorithm state at the native owner's requested cut.
    ///
    /// # Errors
    ///
    /// Returns the original snapshot failure.
    fn save(&self, host: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError>;
    /// Restores a schema-versioned complete algorithm state.
    ///
    /// # Errors
    ///
    /// Returns an error for an unsupported or invalid state.
    fn restore(
        &mut self,
        version: u32,
        state: &[u8],
        host: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError>;
    /// Applies the server's paused intent.
    ///
    /// # Errors
    ///
    /// Returns the original pause failure.
    fn pause(&mut self, host: &mut Host<'_>) -> Result<(), PluginError>;
    /// Resumes only under the host's current eligibility.
    ///
    /// # Errors
    ///
    /// Returns the original resume failure.
    fn resume(&mut self, host: &mut Host<'_>) -> Result<(), PluginError>;
    /// Stops without discarding pending orders or economic identity.
    ///
    /// # Errors
    ///
    /// Returns the original stop failure.
    fn stop(&mut self, reason: &[u8], host: &mut Host<'_>) -> Result<(), PluginError>;
}

/// Prefix is read before the host accesses the remaining table.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct StrategyApiHeader {
    pub abi_version: u32,
    pub struct_size: u32,
}

/// Stable C-shaped executable table. Only encoded bytes, integer tags and an
/// opaque guest instance handle cross the boundary.
#[repr(C)]
#[derive(Debug)]
pub struct StrategyApi {
    pub header: StrategyApiHeader,
    pub panic_mode: u32,
    pub allocation_mode: u32,
    pub build: PluginBuildId,
    pub descriptor_json: BorrowedStr<'static>,
    pub validate: unsafe extern "C" fn(Slice<'_, u8>) -> CallResult,
    pub create: unsafe extern "C" fn(Slice<'_, u8>, *mut *mut c_void) -> CallResult,
    pub callback:
        unsafe extern "C" fn(*mut c_void, u32, u32, Slice<'_, u8>, HostLease) -> CallResult,
    pub dispose: unsafe extern "C" fn(*mut c_void) -> CallResult,
}

impl StrategyApi {
    /// Builds the guest table from a compile-time complete algorithm.
    #[must_use]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the fixed ABI table size is const-asserted to fit u32"
    )]
    pub const fn for_algorithm<A: Algorithm>() -> Self {
        const {
            assert!(size_of::<Self>() <= u32::MAX as usize);
        }
        Self {
            header: StrategyApiHeader {
                abi_version: STRATEGY_ABI_VERSION,
                struct_size: size_of::<Self>() as u32,
            },
            panic_mode: if cfg!(panic = "unwind") {
                PANIC_CONTAINED_UNWIND
            } else {
                2
            },
            allocation_mode: ALLOCATION_PRODUCER_RELEASES,
            build: PluginBuildId::current(),
            descriptor_json: BorrowedStr::from_str(A::DESCRIPTOR_JSON),
            validate: guest::validate::<A>,
            create: guest::create::<A>,
            callback: guest::callback::<A>,
            dispose: guest::dispose::<A>,
        }
    }
}

/// Emits the versioned executable entry point for one algorithm cdylib.
/// The existing metadata entry point is exported separately with `nautilus_plugin!`.
#[macro_export]
macro_rules! nautilus_strategy {
    ($algorithm:ty) => {
        const _: () = {
            static API: $crate::strategy::StrategyApi =
                $crate::strategy::StrategyApi::for_algorithm::<$algorithm>();
            #[unsafe(no_mangle)]
            pub extern "C" fn nautilus_strategy_init_v1() -> *const $crate::strategy::StrategyApi {
                &raw const API
            }
        };
    };
}
