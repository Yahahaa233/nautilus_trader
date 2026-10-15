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
use std::sync::{Arc, Mutex};

#[derive(Debug, Default)]
struct State {
    revision: u64,
    revoked: bool,
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
                .filter(|s| !s.revoked)
                .map(|s| s.revision),
        }
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
        self.gate
            .0
            .lock()
            .is_ok_and(|state| !state.revoked && Some(state.revision) == self.revision)
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
}
