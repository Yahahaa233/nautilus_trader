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

//! Linearizes adapter polling and network request admission at a checkpoint.
//! Frozen input futures are not polled: their real messages remain owned and
//! are delivered in order after release. An in-flight request refuses freezing.

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use anyhow::{Result, ensure};

#[derive(Debug, Default)]
struct State {
    epoch: u64,
    active: usize,
    frozen: bool,
    poisoned: bool,
    waiters: Vec<Waker>,
}

/// Shared by the actual producer tasks and their native adapter owner.
#[derive(Clone, Debug, Default)]
pub struct CheckpointGate(Arc<Mutex<State>>);

impl CheckpointGate {
    /// Holds admission for the entire network operation, including retries and
    /// awaiting the response. Starting a request while frozen fails closed.
    ///
    /// # Errors
    /// Refuses an unhealthy gate, frozen admission or counter exhaustion.
    pub fn enter_request(&self) -> Result<CallbackLease> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        if state.frozen {
            state.poisoned = true;
        }
        ensure!(
            !state.frozen && !state.poisoned,
            "adapter request admission frozen or failed"
        );
        state.active = state
            .active
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("callback count exhausted"))?;
        Ok(CallbackLease(self.clone()))
    }

    /// Freezes only when no request or callback is executing. Nothing is drained,
    /// cancelled or cleared to manufacture an empty inventory.
    ///
    /// # Errors
    /// Refuses overlapping freezes, active work or a previously failed boundary.
    pub fn freeze(&self) -> Result<FrozenCallbacks> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        ensure!(
            !state.frozen && !state.poisoned,
            "adapter checkpoint gate unavailable"
        );
        ensure!(
            state.active == 0,
            "adapter requests or callbacks remain in flight"
        );
        state.epoch = state
            .epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("checkpoint epoch exhausted"))?;
        state.frozen = true;
        Ok(FrozenCallbacks {
            gate: self.clone(),
            epoch: state.epoch,
            finished: false,
        })
    }

    fn poll_callback<F: Future>(
        &self,
        cx: &mut Context<'_>,
        future: Pin<&mut F>,
    ) -> Poll<(F::Output, CallbackLease)> {
        let lease = {
            let mut state = self.0.lock().expect("checkpoint gate mutex poisoned");
            // A failed boundary never resumes producer callbacks. Shutdown owns
            // task cancellation; waking here cannot reopen failed admission.
            if state.frozen || state.poisoned {
                if !state
                    .waiters
                    .iter()
                    .any(|waker| waker.will_wake(cx.waker()))
                {
                    state.waiters.push(cx.waker().clone());
                }
                return Poll::Pending;
            }
            state.active = state
                .active
                .checked_add(1)
                .expect("callback count exhausted");
            CallbackLease(self.clone())
        };
        match future.poll(cx) {
            Poll::Ready(value) => Poll::Ready((value, lease)),
            Poll::Pending => {
                drop(lease);
                Poll::Pending
            }
        }
    }

    /// Returns a lease with a ready input. The caller retains it through all
    /// derived state changes and output emission, not just the receiver poll.
    pub async fn callback<F: Future>(&self, future: F) -> (F::Output, CallbackLease) {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| self.poll_callback(cx, future.as_mut())).await
    }
}

/// Owned callback/request admission retained until all output work is complete.
#[derive(Debug)]
pub struct CallbackLease(CheckpointGate);

impl Drop for CallbackLease {
    fn drop(&mut self) {
        let mut state = self.0.0.lock().expect("checkpoint gate mutex poisoned");
        if std::thread::panicking() {
            state.poisoned = true;
        }
        state.active = state
            .active
            .checked_sub(1)
            .expect("callback lease underflow");
    }
}

/// One actual adapter gate freeze. Dropping without finishing never reopens it.
#[derive(Debug)]
pub struct FrozenCallbacks {
    gate: CheckpointGate,
    epoch: u64,
    finished: bool,
}

impl FrozenCallbacks {
    /// # Errors
    /// Refuses changed epochs, active work or rejected frozen requests.
    pub fn verify(&self) -> Result<()> {
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        ensure!(
            state.frozen && !state.poisoned && state.epoch == self.epoch && state.active == 0,
            "adapter checkpoint boundary changed or failed"
        );
        Ok(())
    }

    /// # Errors
    /// Refuses a changed/failed boundary instead of resuming its input callbacks.
    pub fn finish(mut self) -> Result<()> {
        self.verify()?;
        let waiters = {
            let mut state = self
                .gate
                .0
                .lock()
                .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
            ensure!(
                state.frozen && !state.poisoned && state.epoch == self.epoch && state.active == 0,
                "adapter checkpoint changed before release"
            );
            state.frozen = false;
            self.finished = true;
            std::mem::take(&mut state.waiters)
        };
        for waker in waiters {
            waker.wake();
        }
        Ok(())
    }
}

impl Drop for FrozenCallbacks {
    fn drop(&mut self) {
        if !self.finished {
            let mut state = self.gate.0.lock().expect("checkpoint gate mutex poisoned");
            state.poisoned = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    fn request_and_abandoned_boundary_fail_closed() {
        let gate = CheckpointGate::default();
        let lease = gate.enter_request().unwrap();
        assert!(gate.freeze().is_err());
        drop(lease);
        let freeze = gate.freeze().unwrap();
        assert!(gate.enter_request().is_err());
        assert!(freeze.verify().is_err());
        drop(freeze);
        assert!(gate.enter_request().is_err());
        assert!(gate.freeze().is_err());
    }

    #[tokio::test]
    async fn frozen_actual_receiver_retains_input_and_output_lease() {
        let gate = CheckpointGate::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(41).unwrap();
        tx.send(42).unwrap();
        let freeze = gate.freeze().unwrap();
        let mut callback = Box::pin(gate.callback(rx.recv()));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(callback.as_mut().poll(cx).is_pending())).await
        );
        freeze.verify().unwrap();
        freeze.finish().unwrap();
        let (value, lease) = callback.await;
        assert_eq!(value, Some(41));
        assert!(gate.freeze().is_err());
        drop(lease);
        let (value, lease) = gate.callback(rx.recv()).await;
        assert_eq!(value, Some(42));
        drop(lease);
        gate.freeze().unwrap().finish().unwrap();
    }
}
