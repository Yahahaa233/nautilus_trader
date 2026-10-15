// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Process-private veto of admitted commands before transport entry.
//!
//! This fence never grants trading authority. Native admission remains the
//! caller's responsibility. A successful check defines entry into transport;
//! later revocation cannot retract a request already handed to that transport.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

/// Owner of an irreversible native failure bit. It can only add a veto.
#[derive(Clone, Debug, Default)]
pub struct TransmissionVetoSource(Arc<AtomicBool>);

/// Read-only failure observation carried to transport; it cannot reset or
/// independently latch its owner's failure state, and is never serializable.
#[derive(Clone, Debug)]
pub struct TransmissionVeto(Arc<AtomicBool>);

impl TransmissionVetoSource {
    /// Permanently records failure for every observer of this source.
    pub fn latch(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Returns whether this owner has irreversibly recorded failure.
    #[must_use]
    pub fn is_latched(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Issues a read-only observer bound to this exact failure source.
    #[must_use]
    pub fn observer(&self) -> TransmissionVeto {
        TransmissionVeto(Arc::clone(&self.0))
    }
}

impl TransmissionVeto {
    /// Observes failure without a mutation or reset capability.
    #[must_use]
    pub fn is_latched(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug, Default)]
struct State {
    revision: u64,
    revoked: bool,
    veto: Option<TransmissionVeto>,
}

/// Shared control generation for one execution process, never serializable.
#[derive(Clone, Debug, Default)]
pub struct TransmissionGate(Arc<Mutex<State>>);

/// Frozen admission generation carried through asynchronous native queues.
#[derive(Clone, Debug)]
pub struct TransmissionPermit {
    gate: TransmissionGate,
    revision: Option<u64>,
}

impl TransmissionGate {
    /// Capture this generation; poisoned or revoked gates always refuse entry.
    #[must_use]
    pub fn capture(&self) -> TransmissionPermit {
        TransmissionPermit {
            gate: self.clone(),
            revision: self
                .0
                .lock()
                .ok()
                .filter(|s| !s.revoked && s.veto.as_ref().is_none_or(|v| !v.is_latched()))
                .map(|s| s.revision),
        }
    }

    /// Bind this process's native failure observation before admission.
    ///
    /// This adds a restriction and invalidates pre-binding tickets; it never
    /// grants execution. Rebinding the same source is idempotent. A different
    /// source, poisoned gate or revision exhaustion permanently refuses entry.
    #[must_use]
    pub fn bind_veto(&self, veto: TransmissionVeto) -> bool {
        let Ok(mut state) = self.0.lock() else {
            return false;
        };
        if let Some(bound) = &state.veto {
            if Arc::ptr_eq(&bound.0, &veto.0) {
                return true;
            }
            state.revoked = true;
            return false;
        }
        let Some(next) = state.revision.checked_add(1) else {
            state.revoked = true;
            return false;
        };
        state.revision = next;
        state.veto = Some(veto);
        true
    }

    /// Invalidate queued admissions without reviving any earlier generation.
    pub fn invalidate(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(next) = state.revision.checked_add(1) {
            state.revision = next;
        } else {
            state.revoked = true;
        }
    }

    /// Permanently refuse both captured and future admissions in this process.
    pub fn revoke(&self) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .revoked = true;
    }
}

impl TransmissionPermit {
    /// Check immediately before native transport entry, after asynchronous waits.
    /// Already entered requests retain their original delivery uncertainty.
    #[must_use]
    pub fn can_transmit(&self) -> bool {
        self.gate.0.lock().is_ok_and(|state| {
            !state.revoked
                && Some(state.revision) == self.revision
                && state.veto.as_ref().is_none_or(|v| !v.is_latched())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queued_generation_cannot_revive_after_invalidate_or_revoke() {
        let gate = TransmissionGate::default();
        let old = gate.capture();
        assert!(old.can_transmit());
        gate.invalidate();
        assert!(!old.can_transmit());
        let current = gate.capture();
        assert!(current.can_transmit());
        let revoker = gate.clone();
        std::thread::spawn(move || revoker.revoke()).join().unwrap();
        gate.invalidate();
        assert!(!old.can_transmit());
        assert!(!current.can_transmit());
        assert!(!gate.capture().can_transmit());
    }

    #[test]
    fn native_failure_veto_refuses_queued_and_future_permits_without_control_poll() {
        let gate = TransmissionGate::default();
        let before_binding = gate.capture();
        let source = TransmissionVetoSource::default();
        let observer = source.observer();
        assert!(gate.bind_veto(observer.clone()));
        assert!(!before_binding.can_transmit());
        let queued = gate.capture();
        assert!(queued.can_transmit());
        std::thread::spawn(move || source.latch()).join().unwrap();
        assert!(!queued.can_transmit());
        assert!(!gate.capture().can_transmit());
        gate.invalidate();
        assert!(gate.bind_veto(observer));
        assert!(!queued.can_transmit());
        assert!(!gate.capture().can_transmit());
    }

    #[test]
    fn replacing_native_failure_source_permanently_refuses_gate() {
        let gate = TransmissionGate::default();
        assert!(gate.bind_veto(TransmissionVetoSource::default().observer()));
        let queued = gate.capture();
        assert!(queued.can_transmit());
        assert!(!gate.bind_veto(TransmissionVetoSource::default().observer()));
        gate.invalidate();
        assert!(!queued.can_transmit());
        assert!(!gate.capture().can_transmit());
    }

    #[test]
    fn failure_before_binding_never_admits_transport() {
        let source = TransmissionVetoSource::default();
        source.latch();
        let gate = TransmissionGate::default();
        assert!(gate.bind_veto(source.observer()));
        gate.invalidate();
        assert!(!gate.capture().can_transmit());
    }
}
