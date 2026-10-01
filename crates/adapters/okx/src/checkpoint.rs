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

//! Actual retained socket input and stream state used by the OKX running barrier.

use std::{
    collections::VecDeque,
    fmt::Debug,
    sync::Arc,
    task::{Context, Poll},
};

use ahash::AHashMap;
use anyhow::{Result, ensure};
use nautilus_common::{
    cache::quote::QuoteCache, clients::RunningAdapterCheckpoint, live::checkpoint::FrozenCallbacks,
};
use nautilus_model::{
    identifiers::ClientOrderId,
    types::{Money, Quantity},
};
use parking_lot::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, error::TryRecvError};
use tokio_tungstenite::tungstenite::Message;
use ustr::Ustr;

use crate::websocket::parse::OrderStateSnapshot;

#[derive(Debug)]
struct Inbox<T> {
    prefix: VecDeque<T>,
    receiver: UnboundedReceiver<T>,
}

/// Shared inspection ownership does not create a second consumer: only the
/// adapter's handler/stream polls it. Snapshot staging retains the original FIFO.
#[derive(Debug)]
pub(crate) struct RetainedInbox<T>(Arc<Mutex<Inbox<T>>>);

impl<T> Clone for RetainedInbox<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T> From<UnboundedReceiver<T>> for RetainedInbox<T> {
    fn from(receiver: UnboundedReceiver<T>) -> Self {
        Self::new(receiver)
    }
}

impl<T> RetainedInbox<T> {
    pub(crate) fn new(receiver: UnboundedReceiver<T>) -> Self {
        Self(Arc::new(Mutex::new(Inbox {
            prefix: VecDeque::new(),
            receiver,
        })))
    }
    pub(crate) fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut inbox = self.0.lock();
        if let Some(value) = inbox.prefix.pop_front() {
            return Poll::Ready(Some(value));
        }
        inbox.receiver.poll_recv(cx)
    }
    pub(crate) async fn recv(&self) -> Option<T> {
        std::future::poll_fn(|cx| self.poll_recv(cx)).await
    }
    pub(crate) fn is_empty(&self) -> bool {
        let inbox = self.0.lock();
        inbox.prefix.is_empty() && inbox.receiver.is_empty()
    }
    // Captures only the pre-cut retained prefix. Messages arriving after this
    // staging remain in the actual receiver as post-cut inputs; they are never
    // consumed, discarded or treated as already dispatched by this checkpoint.
    fn stage(&self) -> Result<()> {
        let mut inbox = self.0.lock();
        let count = inbox.receiver.len();
        for _ in 0..count {
            match inbox.receiver.try_recv() {
                Ok(value) => inbox.prefix.push_back(value),
                Err(TryRecvError::Empty) => anyhow::bail!("socket inventory changed while staging"),
                Err(TryRecvError::Disconnected) => {
                    anyhow::bail!("socket receiver disconnected during staging")
                }
            }
        }
        Ok(())
    }
}

impl RetainedInbox<Message> {
    pub(crate) fn freeze_raw_prefix(&self) -> Result<serde_json::Value> {
        self.stage()?;
        self.raw_prefix()
    }
    pub(crate) fn raw_prefix(&self) -> Result<serde_json::Value> {
        let inbox = self.0.lock();
        let frames = inbox
            .prefix
            .iter()
            .map(|message| -> Result<serde_json::Value> {
                Ok(match message {
                    Message::Text(text) => serde_json::json!({"kind":"text","body":text.as_str()}),
                    Message::Binary(bytes) => {
                        serde_json::json!({"kind":"binary","body":bytes.as_ref()})
                    }
                    Message::Ping(bytes) => {
                        serde_json::json!({"kind":"ping","body":bytes.as_ref()})
                    }
                    Message::Pong(bytes) => {
                        serde_json::json!({"kind":"pong","body":bytes.as_ref()})
                    }
                    Message::Close(_) => {
                        anyhow::bail!("socket close frame prevents healthy checkpoint")
                    }
                    Message::Frame(_) => {
                        anyhow::bail!("unparsed socket frame checkpoint unsupported")
                    }
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(
            serde_json::json!({"schema":"OKXRetainedRawInput.v1","fifo":frames,
            "cut":"retained_prefix_before_checkpoint_new_arrivals_remain_post_cut"}),
        )
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct ExecutionStreamState {
    pub(crate) fee_cache: AHashMap<Ustr, Money>,
    pub(crate) filled_qty_cache: AHashMap<Ustr, Quantity>,
    pub(crate) order_state_cache: AHashMap<ClientOrderId, OrderStateSnapshot>,
}

#[derive(Debug)]
pub(crate) struct DataStreamState {
    pub(crate) quotes: QuoteCache,
    pub(crate) funding: AHashMap<Ustr, (Ustr, u64)>,
}
impl Default for DataStreamState {
    fn default() -> Self {
        Self {
            quotes: QuoteCache::new(),
            funding: AHashMap::new(),
        }
    }
}
impl DataStreamState {
    pub(crate) fn snapshot(&self) -> serde_json::Value {
        serde_json::json!({"quotes":self.quotes.checkpoint_entries(),"funding":self.funding})
    }
}

pub(crate) struct OKXCheckpointGuard {
    frozen: FrozenCallbacks,
    inventory: serde_json::Value,
    inspect: Box<dyn Fn() -> Result<serde_json::Value>>,
}
impl Debug for OKXCheckpointGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OKXCheckpointGuard")
            .field("inventory", &self.inventory)
            .finish_non_exhaustive()
    }
}
impl OKXCheckpointGuard {
    pub(crate) fn new(
        frozen: FrozenCallbacks,
        inspect: impl Fn() -> Result<serde_json::Value> + 'static,
    ) -> Result<Self> {
        frozen.verify()?;
        let inventory = inspect()?;
        frozen.verify()?;
        Ok(Self {
            frozen,
            inventory,
            inspect: Box::new(inspect),
        })
    }
}
impl RunningAdapterCheckpoint for OKXCheckpointGuard {
    fn inventory(&self) -> &serde_json::Value {
        &self.inventory
    }
    fn verify(&self) -> Result<()> {
        self.frozen.verify()?;
        ensure!(
            (self.inspect)()? == self.inventory,
            "actual OKX adapter inventory changed during checkpoint"
        );
        self.frozen.verify()
    }
    fn finish(self: Box<Self>) -> Result<()> {
        self.verify()?;
        self.frozen.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn raw_snapshot_retains_pre_cut_and_post_cut_fifo() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let inbox = RetainedInbox::new(rx);
        tx.send(Message::Text("before".into())).unwrap();
        let captured = inbox.freeze_raw_prefix().unwrap();
        tx.send(Message::Text("after".into())).unwrap();
        assert_eq!(captured, inbox.raw_prefix().unwrap());
        assert_eq!(inbox.recv().await, Some(Message::Text("before".into())));
        assert_eq!(inbox.recv().await, Some(Message::Text("after".into())));
    }
}
