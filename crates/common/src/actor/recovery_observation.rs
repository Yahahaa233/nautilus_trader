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

//! Owner-thread callback admission for a node's read-only recovery observation.

use std::{cell::RefCell, collections::BTreeSet, marker::PhantomData, rc::Rc};

use anyhow::{Context, Result, ensure};
use ustr::Ustr;

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    observers: Option<BTreeSet<String>>,
    failed: bool,
}

thread_local! {
    static ADMISSION: RefCell<State> = RefCell::new(State::default());
}

/// A node-owned, non-serializable admission capability. Unknown components are
/// denied while it is held. Dropping an unfinished capability remains closed.
#[derive(Debug)]
pub struct RecoveryObservationAdmission {
    epoch: u64,
    finished: bool,
    owner: PhantomData<Rc<()>>,
}

impl RecoveryObservationAdmission {
    /// Installs the already validated native observer actor registration set.
    /// It does not start actors, invoke callbacks or grant venue execution.
    ///
    /// # Errors
    /// Refuses overlapping phases, duplicates, empty IDs and failed admission.
    pub fn enter(observer_ids: &[Ustr]) -> Result<Self> {
        let observers = observer_ids
            .iter()
            .map(ToString::to_string)
            .collect::<BTreeSet<_>>();
        ensure!(
            !observers.is_empty() && observers.len() == observer_ids.len(),
            "invalid observation registration"
        );
        ensure!(
            observers.iter().all(|id| !id.trim().is_empty()),
            "empty observer ID"
        );
        ADMISSION.with(|state| {
            let mut state = state
                .try_borrow_mut()
                .context("observation admission reentered")?;
            ensure!(
                state.observers.is_none() && !state.failed,
                "observation admission unavailable"
            );
            state.epoch = state
                .epoch
                .checked_add(1)
                .context("observation epoch exhausted")?;
            state.observers = Some(observers);
            Ok(Self {
                epoch: state.epoch,
                finished: false,
                owner: PhantomData,
            })
        })
    }

    /// # Errors
    /// Rejects a replaced, released, reentered or abandoned phase.
    pub fn verify(&self) -> Result<()> {
        ADMISSION.with(|state| {
            let state = state
                .try_borrow()
                .context("observation admission reentered")?;
            ensure!(
                state.epoch == self.epoch
                    && state.observers.is_some()
                    && !state.failed
                    && !self.finished,
                "observation admission changed"
            );
            Ok(())
        })
    }

    /// Only the owner that holds this capability can finish this exact phase.
    /// The native node must verify its sealed observation release first.
    ///
    /// # Errors
    /// Refuses changed or failed admission instead of reopening callbacks.
    pub fn finish(mut self) -> Result<()> {
        self.verify()?;
        ADMISSION.with(|state| state.borrow_mut().observers = None);
        self.finished = true;
        Ok(())
    }
}

impl Drop for RecoveryObservationAdmission {
    fn drop(&mut self) {
        if !self.finished {
            let _ = ADMISSION.try_with(|state| {
                if let Ok(mut state) = state.try_borrow_mut() {
                    state.failed = true;
                }
            });
        }
    }
}

/// Native callback entry points must check this before indicators, historical
/// responses, lifecycle-related order handling or application callbacks mutate.
#[must_use]
pub fn callback_admitted(component_id: &Ustr) -> bool {
    ADMISSION
        .try_with(|state| {
            state
                .try_borrow()
                .map(|state| {
                    !state.failed
                        && state
                            .observers
                            .as_ref()
                            .is_none_or(|ids| ids.contains(component_id.as_str()))
                })
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

/// Allows explicitly registered observer actors to receive actual input while
/// ordinary restored components remain in their non-running lifecycle state.
#[must_use]
pub fn is_observer(component_id: &Ustr) -> bool {
    ADMISSION
        .try_with(|state| {
            state
                .try_borrow()
                .map(|state| {
                    !state.failed
                        && state
                            .observers
                            .as_ref()
                            .is_some_and(|ids| ids.contains(component_id.as_str()))
                })
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn observation_callback_admission_keeps_strategy_closed_until_exact_release() {
        let guardian = Ustr::from("guardian-observer");
        let strategy = Ustr::from("restored-strategy");
        let guard = RecoveryObservationAdmission::enter(&[guardian]).unwrap();
        assert!(callback_admitted(&guardian));
        assert!(is_observer(&guardian));
        assert!(!callback_admitted(&strategy));
        assert!(!is_observer(&strategy));
        assert!(RecoveryObservationAdmission::enter(&[strategy]).is_err());
        guard.verify().unwrap();
        guard.finish().unwrap();
        assert!(callback_admitted(&strategy));
        assert!(!is_observer(&guardian));
    }
}
