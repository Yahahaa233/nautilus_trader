//! Owned queue prefixes preserve non-cloneable messages during borrowed encoding.
use nautilus_common::{
    live::ingress::NativeIngressReceiver,
    recovery_trace::{NativeIngressReceipt, scope},
};
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};
use tokio::sync::mpsc::{UnboundedReceiver, error::TryRecvError};

#[derive(Debug)]
struct Retained<T> {
    message: T,
    receipt: Option<NativeIngressReceipt>,
}

#[derive(Debug)]
enum Receiver<T> {
    Raw(UnboundedReceiver<T>),
    Native(NativeIngressReceiver<T>),
}
impl<T> Receiver<T> {
    fn len(&self) -> usize {
        match self {
            Self::Raw(receiver) => receiver.len(),
            Self::Native(receiver) => receiver.len(),
        }
    }
    fn close(&mut self) {
        match self {
            Self::Raw(receiver) => receiver.close(),
            Self::Native(receiver) => receiver.close(),
        }
    }
    fn is_closed(&self) -> bool {
        match self {
            Self::Raw(receiver) => receiver.is_closed(),
            Self::Native(receiver) => receiver.is_closed(),
        }
    }
    fn try_recv(&mut self) -> Result<Retained<T>, TryRecvError> {
        match self {
            Self::Raw(receiver) => receiver.try_recv().map(|message| Retained {
                message,
                receipt: None,
            }),
            Self::Native(receiver) => receiver.try_recv().map(|entry| {
                let (message, receipt) = entry.into_parts();
                Retained {
                    message,
                    receipt: Some(receipt),
                }
            }),
        }
    }
    fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<Retained<T>>> {
        match self {
            Self::Raw(receiver) => receiver.poll_recv(context).map(|message| {
                message.map(|message| Retained {
                    message,
                    receipt: None,
                })
            }),
            Self::Native(receiver) => receiver.poll_recv(context).map(|message| {
                message.map(|entry| {
                    let (message, receipt) = entry.into_parts();
                    Retained {
                        message,
                        receipt: Some(receipt),
                    }
                })
            }),
        }
    }
}

/// A receiver that retains staged messages and always consumes its prefix first.
/// There is deliberately no method to extract the underlying receiver alone.
#[derive(Debug)]
pub struct SnapshotReceiver<T> {
    prefix: VecDeque<Retained<T>>,
    receiver: Receiver<T>,
}
impl<T> From<UnboundedReceiver<T>> for SnapshotReceiver<T> {
    fn from(receiver: UnboundedReceiver<T>) -> Self {
        Self {
            prefix: VecDeque::new(),
            receiver: Receiver::Raw(receiver),
        }
    }
}
impl<T> From<NativeIngressReceiver<T>> for SnapshotReceiver<T> {
    fn from(receiver: NativeIngressReceiver<T>) -> Self {
        Self {
            prefix: VecDeque::new(),
            receiver: Receiver::Native(receiver),
        }
    }
}
impl<T> SnapshotReceiver<T> {
    /// Returns the total number of retained and channel-resident messages.
    #[must_use]
    pub fn len(&self) -> usize {
        self.prefix.len() + self.receiver.len()
    }
    /// Returns whether both storage locations are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.prefix.is_empty() && self.receiver.len() == 0
    }
    /// Closes further channel admission while preserving retained messages.
    pub fn close(&mut self) {
        self.receiver.close();
    }
    /// Returns whether the underlying channel is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.receiver.is_closed()
    }
    /// Receives the oldest message without waiting.
    ///
    /// # Errors
    /// Returns the underlying empty/disconnected error when no prefix remains.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let entry = match self.prefix.pop_front() {
            Some(message) => Ok(message),
            None => self.receiver.try_recv(),
        }?;
        scope::received_ingress(entry.receipt);
        Ok(entry.message)
    }
    /// Waits for the oldest message, consuming retained messages first.
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }
    /// Polls for the oldest retained or channel-resident message.
    pub fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        let message = match self.prefix.pop_front() {
            Some(message) => Poll::Ready(Some(message)),
            None => self.receiver.poll_recv(context),
        };
        message.map(|message| {
            message.map(|entry| {
                scope::received_ingress(entry.receipt);
                entry.message
            })
        })
    }
    pub(super) fn stage(&mut self) -> anyhow::Result<()> {
        // Reserve before consuming: an allocation failure cannot drop a moved message.
        while self.receiver.len() > 0 {
            self.prefix.try_reserve(1)?;
            match self.receiver.try_recv() {
                Ok(message) => self.prefix.push_back(message),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn stage_for_test(&mut self) -> anyhow::Result<()> {
        self.stage()
    }

    pub(super) fn pending(&self) -> impl Iterator<Item = &T> {
        self.prefix.iter().map(|entry| &entry.message)
    }
    /// Returns original evidence for each actual staged FIFO member.
    /// Raw compatibility channels explicitly return absent receipts.
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn pending_receipts(&self) -> impl Iterator<Item = Option<&NativeIngressReceipt>> {
        self.prefix.iter().map(|entry| entry.receipt.as_ref())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn append_native_retained(
        &mut self,
        message: T,
        receipt: NativeIngressReceipt,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.receiver.len() == 0,
            "unowned actual input appeared during native handoff"
        );
        if let Some(previous) = self.prefix.back().and_then(|entry| entry.receipt.as_ref()) {
            anyhow::ensure!(
                previous.channel_id == receipt.channel_id
                    && previous.channel_ordinal.checked_add(1) == Some(receipt.channel_ordinal),
                "restored native FIFO is not contiguous"
            );
        }
        self.prefix.try_reserve(1)?;
        self.prefix.push_back(Retained {
            message,
            receipt: Some(receipt),
        });
        Ok(())
    }
    pub(crate) fn prepend_retained(&mut self, retained: &mut VecDeque<T>) -> anyhow::Result<()> {
        self.prefix.try_reserve(retained.len())?;
        while let Some(message) = retained.pop_back() {
            self.prefix.push_front(Retained {
                message,
                receipt: None,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn staged_prefix_precedes_new_tail_across_all_consumption_apis() {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut receiver = SnapshotReceiver::from(receiver);
        sender.send("first").unwrap();
        sender.send("second").unwrap();
        receiver.stage().unwrap();
        receiver.stage().unwrap();
        sender.send("third").unwrap();
        assert_eq!(receiver.len(), 3);
        assert_eq!(receiver.try_recv().unwrap(), "first");
        receiver.stage().unwrap();
        assert_eq!(receiver.recv().await, Some("second"));
        receiver.close();
        assert_eq!(
            std::future::poll_fn(|cx| receiver.poll_recv(cx)).await,
            Some("third")
        );
        assert_eq!(receiver.recv().await, None);
        assert!(receiver.is_empty());
    }

    #[test]
    fn codec_unwind_cannot_drop_noncloneable_retained_messages() {
        struct Owned(std::rc::Rc<std::cell::Cell<usize>>);
        impl Drop for Owned {
            fn drop(&mut self) {
                self.0.set(self.0.get() + 1);
            }
        }
        let drops = std::rc::Rc::new(std::cell::Cell::new(0));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut receiver = SnapshotReceiver::from(receiver);
        sender.send(Owned(drops.clone())).ok().unwrap();
        receiver.stage().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _borrowed = receiver.pending().next().unwrap();
                panic!("injected encoder panic");
            }))
            .is_err()
        );
        assert_eq!(drops.get(), 0);
        drop(receiver.try_recv().unwrap());
        assert_eq!(drops.get(), 1);
    }
}
