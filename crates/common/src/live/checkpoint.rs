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
    paused: bool,
    pause_epoch: u64,
    poisoned: bool,
    terminal: bool,
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
            !state.frozen && !state.paused && !state.poisoned && !state.terminal,
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
            !state.frozen && !state.poisoned && !state.terminal,
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

    /// Retains actual producer futures across recovery without holding a capture
    /// freeze. A checkpoint can still freeze and verify this paused producer.
    /// Only the returned same-gate capability can resume it.
    ///
    /// # Errors
    /// Rejects active work, overlapping pauses/freezes or a failed producer.
    pub fn pause_producers(&self) -> Result<PausedCallbacks> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        ensure!(
            !state.paused
                && !state.frozen
                && !state.poisoned
                && !state.terminal
                && state.active == 0,
            "producer recovery pause unavailable"
        );
        state.pause_epoch = state
            .pause_epoch
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("pause epoch exhausted"))?;
        state.paused = true;
        Ok(PausedCallbacks {
            gate: self.clone(),
            epoch: state.pause_epoch,
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
            if state.frozen || state.paused || state.poisoned || state.terminal {
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

    /// Linearizes a registered timer's synchronous poll, schedule publication and
    /// queue send against freeze. Only native registered timer workers use this
    /// section: network requests and arbitrary callbacks keep their full active
    /// leases and continue to refuse a concurrent freeze.
    pub(crate) async fn registered_timer_publication<R>(
        &self,
        mut publish: impl FnMut(&mut Context<'_>) -> Poll<R>,
    ) -> R {
        std::future::poll_fn(|cx| {
            let mut state = self.0.lock().expect("checkpoint gate mutex poisoned");
            if state.frozen || state.paused || state.poisoned || state.terminal {
                if !state
                    .waiters
                    .iter()
                    .any(|waker| waker.will_wake(cx.waker()))
                {
                    state.waiters.push(cx.waker().clone());
                }
                return Poll::Pending;
            }
            // Freeze obtains this same mutex after the actual poll/publication
            // has returned. No lease is exposed across a worker scheduling gap.
            // A panic poisons the mutex and leaves admission permanently closed.
            publish(cx)
        })
        .await
    }

    /// Returns a lease with a ready input. The caller retains it through all
    /// derived state changes and output emission, not just the receiver poll.
    pub async fn callback<F: Future>(&self, future: F) -> (F::Output, CallbackLease) {
        let mut future = std::pin::pin!(future);
        std::future::poll_fn(|cx| self.poll_callback(cx, future.as_mut())).await
    }
}

/// Non-deserializable producer pause, independent of checkpoint epochs.
#[derive(Debug)]
pub struct PausedCallbacks {
    gate: CheckpointGate,
    epoch: u64,
    finished: bool,
}
impl PausedCallbacks {
    /// # Errors
    /// Rejects a changed, busy or failed native producer.
    pub fn verify(&self) -> Result<()> {
        let state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        ensure!(
            state.paused && state.pause_epoch == self.epoch && !state.poisoned && state.active == 0,
            "recovery producer pause changed or failed"
        );
        Ok(())
    }
    /// Resumes only this actual paused producer. This is not trading admission.
    /// # Errors
    /// Rejects an outstanding capture freeze, changed state or permanent failure.
    pub fn finish(mut self) -> Result<()> {
        self.verify()?;
        let waiters = {
            let mut state = self
                .gate
                .0
                .lock()
                .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
            ensure!(!state.frozen, "producer capture freeze remains active");
            state.paused = false;
            self.finished = true;
            std::mem::take(&mut state.waiters)
        };
        for waker in waiters {
            waker.wake();
        }
        Ok(())
    }
}
impl Drop for PausedCallbacks {
    fn drop(&mut self) {
        if !self.finished {
            self.gate
                .0
                .lock()
                .expect("checkpoint gate mutex poisoned")
                .poisoned = true;
        }
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

    /// Ends the same verified freeze with admission permanently closed. This
    /// healthy terminal cut cannot be reopened by a pause or another checkpoint.
    /// Producer futures retain all pre-cut input; shutdown owns their join.
    /// # Errors
    /// Refuses changed or failed freezes.
    pub fn finish_terminal(mut self) -> Result<()> {
        self.verify()?;
        let mut state = self
            .gate
            .0
            .lock()
            .map_err(|_| anyhow::anyhow!("checkpoint gate mutex poisoned"))?;
        ensure!(
            state.frozen && !state.poisoned && state.epoch == self.epoch && state.active == 0,
            "terminal adapter boundary changed"
        );
        state.terminal = true;
        state.frozen = false;
        self.finished = true;
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
    async fn checkpoint_terminal_cut_never_reopens_actual_producer_or_request() {
        let gate = CheckpointGate::default();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(71).unwrap();
        let frozen = gate.freeze().unwrap();
        frozen.finish_terminal().unwrap();
        assert!(gate.enter_request().is_err());
        assert!(gate.freeze().is_err());
        assert!(gate.pause_producers().is_err());
        let mut future = Box::pin(gate.callback(rx.recv()));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
        );
        drop(future);
        assert_eq!(rx.len(), 1);
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

    #[tokio::test]
    async fn recovery_pause_allows_real_capture_and_retains_future_until_private_resume() {
        let gate = CheckpointGate::default();
        let pause = gate.pause_producers().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        tx.send(31).unwrap();
        let mut callback = Box::pin(gate.callback(rx.recv()));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(callback.as_mut().poll(cx).is_pending())).await
        );
        let frozen = gate.freeze().unwrap();
        pause.verify().unwrap();
        frozen.finish().unwrap();
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(callback.as_mut().poll(cx).is_pending())).await
        );
        assert!(gate.enter_request().is_err());
        pause.finish().unwrap();
        let (value, lease) = callback.await;
        assert_eq!(value, Some(31));
        drop(lease);
        gate.freeze().unwrap().finish().unwrap();
    }
}
