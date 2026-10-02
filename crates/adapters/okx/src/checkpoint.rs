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
use anyhow::{Context as _, Result, ensure};
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
    pub(crate) fn len(&self) -> usize {
        let inbox = self.0.lock();
        inbox.prefix.len() + inbox.receiver.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(crate) fn prepend(&self, prefix: &mut VecDeque<T>) -> Result<()> {
        let mut inbox = self.0.lock();
        inbox.prefix.try_reserve(prefix.len())?;
        while let Some(value) = prefix.pop_back() {
            inbox.prefix.push_front(value);
        }
        Ok(())
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

#[derive(Debug, Default)]
struct RecoveryRawState {
    source: VecDeque<Message>,
    current: VecDeque<Message>,
    released: bool,
    failed: bool,
    current_bytes: usize,
}

/// Old data never enters the fresh session's authentication/subscription path.
/// The actual reader retains new economic input while current control ACKs run.
#[derive(Debug)]
pub(crate) struct RecoveryRawPrefix(Mutex<RecoveryRawState>);
impl RecoveryRawPrefix {
    pub(crate) fn new(raw: &serde_json::Value) -> Result<Self> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            schema: String,
            fifo: Vec<Frame>,
            cut: String,
        }
        #[derive(serde::Deserialize)]
        #[serde(
            tag = "kind",
            content = "body",
            rename_all = "lowercase",
            deny_unknown_fields
        )]
        enum Frame {
            Text(String),
            Binary(Vec<u8>),
            Ping(Vec<u8>),
            Pong(Vec<u8>),
        }
        let raw: Raw = serde_json::from_value(raw.clone())?;
        ensure!(
            raw.schema == "OKXRetainedRawInput.v1"
                && raw.cut == "retained_prefix_before_checkpoint_new_arrivals_remain_post_cut"
                && raw.fifo.len() <= 100_000,
            "unsupported retained raw input schema or bound"
        );
        let mut source = VecDeque::new();
        let mut bytes = 0usize;
        for frame in raw.fifo {
            let message = match frame {
                Frame::Text(text) => Message::Text(text.into()),
                Frame::Binary(bytes) => Message::Binary(bytes.into()),
                Frame::Ping(bytes) => Message::Ping(bytes.into()),
                Frame::Pong(bytes) => Message::Pong(bytes.into()),
            };
            bytes = bytes
                .checked_add(message.len())
                .context("raw input byte bound overflow")?;
            ensure!(
                bytes <= 64 * 1024 * 1024,
                "retained raw input byte bound exceeded"
            );
            ensure!(
                Self::economic_input(&message)?,
                "historical socket control input cannot authenticate or confirm a fresh session"
            );
            source.push_back(message);
        }
        Ok(Self(Mutex::new(RecoveryRawState {
            source,
            ..Default::default()
        })))
    }
    fn economic_input(message: &Message) -> Result<bool> {
        let value: serde_json::Value = match message {
            Message::Text(text) if text.as_str() == "pong" || text.as_str() == "ping" => {
                return Ok(false);
            }
            Message::Text(text) => serde_json::from_str(text.as_str())?,
            Message::Binary(bytes) => serde_json::from_slice(bytes)?,
            Message::Ping(_) | Message::Pong(_) => return Ok(false),
            Message::Close(_) | Message::Frame(_) => {
                anyhow::bail!("closed or unparsed recovery socket input")
            }
        };
        Ok(value.get("arg").is_some()
            && value.get("data").is_some()
            && value.get("event").is_none()
            && value.get("op").is_none())
    }
    pub(crate) fn retain_current(&self, message: Message) -> Result<Option<Message>> {
        let mut state = self.0.lock();
        ensure!(!state.failed, "socket recovery input failed");
        if state.released {
            return Ok(Some(message));
        }
        match Self::economic_input(&message) {
            Ok(true) => {
                ensure!(
                    state.current.len() < 100_000,
                    "fresh socket recovery input bound exceeded"
                );
                state.current_bytes = state
                    .current_bytes
                    .checked_add(message.len())
                    .context("fresh raw byte bound overflow")?;
                ensure!(
                    state.current_bytes <= 64 * 1024 * 1024,
                    "fresh socket recovery byte bound exceeded"
                );
                state.current.push_back(message);
                Ok(None)
            }
            Ok(false) => Ok(Some(message)),
            Err(error) => {
                state.failed = true;
                Err(error)
            }
        }
    }
    pub(crate) fn release(&self, inbox: &RetainedInbox<Message>) -> Result<()> {
        let mut state = self.0.lock();
        ensure!(
            !state.failed && !state.released,
            "socket recovery input release invalid"
        );
        let current = std::mem::take(&mut state.current);
        state.source.try_reserve(current.len())?;
        state.source.extend(current);
        inbox.prepend(&mut state.source)?;
        state.released = true;
        Ok(())
    }
    pub(crate) fn verify_released(&self) -> Result<()> {
        let state = self.0.lock();
        ensure!(
            state.released && !state.failed && state.source.is_empty() && state.current.is_empty(),
            "fresh socket recovery handshake/input handoff is incomplete"
        );
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
    pub(crate) fn restore(source: &serde_json::Value) -> Result<Self> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Snapshot {
            quotes: Vec<nautilus_model::data::QuoteTick>,
            funding: AHashMap<Ustr, (Ustr, u64)>,
        }
        let source: Snapshot = serde_json::from_value(source.clone())?;
        let mut state = Self::default();
        for quote in source.quotes {
            ensure!(
                state.quotes.insert(quote.instrument_id, quote).is_none(),
                "duplicate source merge quote"
            );
        }
        state.funding = source.funding;
        Ok(state)
    }
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
    fn finish_terminal(self: Box<Self>) -> Result<()> {
        self.verify()?;
        self.frozen.finish_terminal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn checkpoint_restore_raw_input_cannot_supply_fresh_auth_and_keeps_fifo() {
        let source = serde_json::json!({"schema":"OKXRetainedRawInput.v1","cut":"retained_prefix_before_checkpoint_new_arrivals_remain_post_cut",
            "fifo":[{"kind":"text","body":"{\"arg\":{\"channel\":\"bbo-tbt\"},\"data\":[1]}"}]});
        let recovery = RecoveryRawPrefix::new(&source).unwrap();
        let login = Message::Text("{\"event\":\"login\",\"code\":\"0\"}".into());
        assert_eq!(recovery.retain_current(login.clone()).unwrap(), Some(login));
        assert!(
            recovery
                .retain_current(Message::Text(
                    "{\"arg\":{\"channel\":\"bbo-tbt\"},\"data\":[2]}".into()
                ))
                .unwrap()
                .is_none()
        );
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let inbox = RetainedInbox::new(rx);
        tx.send(Message::Text(
            "{\"arg\":{\"channel\":\"bbo-tbt\"},\"data\":[3]}".into(),
        ))
        .unwrap();
        recovery.release(&inbox).unwrap();
        recovery.verify_released().unwrap();
        for expected in [1, 2, 3] {
            let message = inbox.recv().await.unwrap();
            let Message::Text(text) = message else {
                panic!("text FIFO");
            };
            assert_eq!(
                serde_json::from_str::<serde_json::Value>(&text).unwrap()["data"][0],
                expected
            );
        }
        let mut forged = source;
        forged["fifo"] =
            serde_json::json!([{"kind":"text","body":"{\"event\":\"login\",\"code\":\"0\"}"}]);
        assert!(RecoveryRawPrefix::new(&forged).is_err());
    }
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
