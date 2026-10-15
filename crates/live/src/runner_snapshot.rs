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
pub(crate) struct Retained<T> {
    message: T,
    receipt: Option<NativeIngressReceipt>,
}
impl<T> Retained<T> {
    /// Publishes the original dequeue evidence only when its message is processed.
    /// Buffering owns both members and cannot overwrite an active dispatch receipt.
    pub(crate) fn activate(self) -> T {
        scope::received_ingress(self.receipt);
        self.message
    }
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
    // Installed only by the reader-issued native tail while the actual runner is
    // frozen. Its original receipts stay visible to capture, but cannot be
    // dispatched by startup or Observing before the private fresh release.
    #[cfg(feature = "native-tail-replay")]
    recovered_prefix: VecDeque<Retained<T>>,
    #[cfg(feature = "native-tail-replay")]
    recovered_prefix_released: bool,
    receiver: Receiver<T>,
}
impl<T> From<UnboundedReceiver<T>> for SnapshotReceiver<T> {
    fn from(receiver: UnboundedReceiver<T>) -> Self {
        Self {
            prefix: VecDeque::new(),
            #[cfg(feature = "native-tail-replay")]
            recovered_prefix: VecDeque::new(),
            #[cfg(feature = "native-tail-replay")]
            recovered_prefix_released: false,
            receiver: Receiver::Raw(receiver),
        }
    }
}
impl<T> From<NativeIngressReceiver<T>> for SnapshotReceiver<T> {
    fn from(receiver: NativeIngressReceiver<T>) -> Self {
        Self {
            prefix: VecDeque::new(),
            #[cfg(feature = "native-tail-replay")]
            recovered_prefix: VecDeque::new(),
            #[cfg(feature = "native-tail-replay")]
            recovered_prefix_released: false,
            receiver: Receiver::Native(receiver),
        }
    }
}
impl<T> SnapshotReceiver<T> {
    /// Returns the total number of retained and channel-resident messages.
    #[must_use]
    pub fn len(&self) -> usize {
        #[cfg(feature = "native-tail-replay")]
        let recovered = self.recovered_prefix.len();
        #[cfg(not(feature = "native-tail-replay"))]
        let recovered = 0;
        recovered + self.prefix.len() + self.receiver.len()
    }
    /// Returns whether both storage locations are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Actual dispatch readiness excludes the privately held historical prefix.
    /// Inventory emptiness remains separate and includes all retained messages.
    #[cfg(feature = "node")]
    pub(crate) fn dispatch_is_empty(&self) -> bool {
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
        self.try_recv_retained().map(Retained::activate)
    }
    pub(crate) fn try_recv_retained(&mut self) -> Result<Retained<T>, TryRecvError> {
        match self.prefix.pop_front() {
            Some(message) => Ok(message),
            None => self.receiver.try_recv(),
        }
    }
    #[cfg(feature = "node")]
    pub(crate) async fn recv_retained(&mut self) -> Option<Retained<T>> {
        std::future::poll_fn(|context| self.poll_recv_retained(context)).await
    }
    /// Waits for the oldest message, consuming retained messages first.
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }
    /// Polls for the oldest retained or channel-resident message.
    pub fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        self.poll_recv_retained(context)
            .map(|entry| entry.map(Retained::activate))
    }
    fn poll_recv_retained(&mut self, context: &mut Context<'_>) -> Poll<Option<Retained<T>>> {
        match self.prefix.pop_front() {
            Some(message) => Poll::Ready(Some(message)),
            None => self.receiver.poll_recv(context),
        }
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
        #[cfg(feature = "native-tail-replay")]
        let retained = self.recovered_prefix.iter().chain(self.prefix.iter());
        #[cfg(not(feature = "native-tail-replay"))]
        let retained = self.prefix.iter();
        retained.map(|entry| &entry.message)
    }
    /// Returns original evidence for each actual staged FIFO member.
    /// Raw compatibility channels explicitly return absent receipts.
    #[cfg(feature = "native-tail-replay")]
    pub(super) fn pending_receipts(&self) -> impl Iterator<Item = Option<&NativeIngressReceipt>> {
        self.recovered_prefix
            .iter()
            .chain(self.prefix.iter())
            .map(|entry| entry.receipt.as_ref())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn append_native_retained(
        &mut self,
        message: T,
        receipt: NativeIngressReceipt,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovered_prefix_released && self.prefix.is_empty() && self.receiver.len() == 0,
            "unowned actual input appeared during native handoff or its release already occurred"
        );
        if let Some(previous) = self
            .recovered_prefix
            .back()
            .and_then(|entry| entry.receipt.as_ref())
        {
            anyhow::ensure!(
                previous.channel_id == receipt.channel_id
                    && previous.channel_ordinal.checked_add(1) == Some(receipt.channel_ordinal),
                "restored native FIFO is not contiguous"
            );
        }
        self.recovered_prefix.try_reserve(1)?;
        self.recovered_prefix.push_back(Retained {
            message,
            receipt: Some(receipt),
        });
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn bind_historical_source_frontier(
        &mut self,
        guard: &nautilus_common::live::ingress::FrozenIngress,
        source_channel_id: nautilus_core::UUID4,
        next_ordinal: u64,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovered_prefix_released && self.prefix.is_empty(),
            "historical source frontier requires the unreleased source prefix"
        );
        match &mut self.receiver {
            Receiver::Native(receiver) => {
                receiver.bind_historical_source_frontier(guard, source_channel_id, next_ordinal)
            }
            Receiver::Raw(_) => {
                anyhow::bail!("historical source requires the owned native receiver")
            }
        }
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn append_historical_external(
        &mut self,
        guard: &nautilus_common::live::ingress::FrozenIngress,
        message: T,
        receipt: NativeIngressReceipt,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovered_prefix_released && self.receiver.len() == 0,
            "historical external admission cannot overtake a physical native input"
        );
        if let Some(previous) = self.prefix.back().or_else(|| self.recovered_prefix.back()) {
            anyhow::ensure!(
                previous.receipt.as_ref().is_some_and(|previous| {
                    previous.channel_id == receipt.channel_id
                        && previous.channel_ordinal.checked_add(1) == Some(receipt.channel_ordinal)
                }),
                "historical external admission changed original channel FIFO"
            );
        }
        self.prefix.try_reserve(1)?;
        match &mut self.receiver {
            Receiver::Native(receiver) => {
                receiver.advance_historical_source_frontier(guard, &receipt)?;
            }
            Receiver::Raw(_) => {
                anyhow::bail!("historical external input requires native receipt ownership")
            }
        }
        self.prefix.push_back(Retained {
            message,
            receipt: Some(receipt),
        });
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn take_historical_front(
        &mut self,
        expected: &NativeIngressReceipt,
    ) -> anyhow::Result<Option<Retained<T>>> {
        anyhow::ensure!(
            !self.recovered_prefix_released,
            "historical source FIFO already released"
        );
        // Stage only the next physical entry. A later external source admission
        // cannot silently change the ordinals of already resident native entries.
        if self.recovered_prefix.is_empty() && self.prefix.is_empty() && self.receiver.len() > 0 {
            self.prefix.try_reserve(1)?;
            self.prefix.push_back(self.receiver.try_recv()?);
        }
        let Some(front) = self
            .recovered_prefix
            .front()
            .or_else(|| self.prefix.front())
        else {
            return Ok(None);
        };
        anyhow::ensure!(
            front.receipt.as_ref() == Some(expected),
            "historical Begin is not the next original physical FIFO receipt"
        );
        Ok(if self.recovered_prefix.is_empty() {
            self.prefix.pop_front()
        } else {
            self.recovered_prefix.pop_front()
        })
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn hold_historical_pending(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovered_prefix_released,
            "historical pending FIFO already released"
        );
        self.stage()?;
        self.recovered_prefix.try_reserve(self.prefix.len())?;
        // The initial source prefix precedes actual historical descendants.
        // All remain unavailable to ordinary startup until the same fresh release.
        self.recovered_prefix.append(&mut self.prefix);
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn prepare_recovered_prefix_release(&mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.recovered_prefix_released,
            "native retained release already occurred"
        );
        self.prefix.try_reserve(self.recovered_prefix.len())?;
        Ok(())
    }
    #[cfg(feature = "native-tail-replay")]
    pub(crate) fn release_recovered_prefix(&mut self) {
        // All seven receivers reserve before the fresh lifecycle commits. No
        // allocation or fallible operation can split this ordered handoff.
        while let Some(entry) = self.recovered_prefix.pop_back() {
            self.prefix.push_front(entry);
        }
        self.recovered_prefix_released = true;
    }
    #[cfg(all(feature = "node", feature = "dispatch-observer"))]
    pub(crate) fn prepend_retained<U: Into<T>>(
        &mut self,
        retained: &mut VecDeque<U>,
    ) -> anyhow::Result<()> {
        self.prefix.try_reserve(retained.len())?;
        while let Some(message) = retained.pop_back() {
            self.prefix.push_front(Retained {
                message: message.into(),
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

    #[cfg(feature = "native-tail-replay")]
    #[test]
    fn recovered_native_prefix_is_retained_until_private_release_and_precedes_fresh_tail() {
        use nautilus_common::{live::ingress::IngressGate, recovery_trace::NativeInputSource};
        let parent = IngressGate::new();
        let (original_sender, mut original_receiver) =
            parent.native_channel::<&'static str>(NativeInputSource::TradingCommand);
        original_sender.send("original-first").unwrap();
        original_sender.send("original-second").unwrap();
        let (first, first_receipt) = original_receiver.try_recv().unwrap().into_parts();
        let (second, second_receipt) = original_receiver.try_recv().unwrap().into_parts();
        let original_receipts = vec![first_receipt.clone(), second_receipt.clone()];
        let child = IngressGate::new();
        let (current_sender, current_receiver) =
            child.native_channel::<&'static str>(NativeInputSource::TradingCommand);
        let mut receiver = SnapshotReceiver::from(current_receiver);
        receiver
            .append_native_retained(first, first_receipt)
            .unwrap();
        receiver
            .append_native_retained(second, second_receipt)
            .unwrap();
        assert!(!receiver.is_empty());
        assert!(receiver.dispatch_is_empty());
        assert!(matches!(
            receiver.try_recv_retained(),
            Err(TryRecvError::Empty)
        ));
        assert_eq!(
            receiver.pending().copied().collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(
            receiver
                .pending_receipts()
                .map(|r| r.unwrap().clone())
                .collect::<Vec<_>>(),
            original_receipts
        );
        current_sender.send("fresh-observation").unwrap();
        let fresh = receiver.try_recv_retained().unwrap();
        assert_eq!(fresh.message, "fresh-observation");
        assert_ne!(
            fresh.receipt.unwrap().channel_id,
            original_receipts[0].channel_id
        );
        assert_eq!(receiver.len(), 2);
        current_sender.send("later-current").unwrap();
        receiver.stage().unwrap();
        receiver.prepare_recovered_prefix_release().unwrap();
        receiver.release_recovered_prefix();
        let original = receiver.try_recv_retained().unwrap();
        assert_eq!(
            (original.message, original.receipt.as_ref()),
            (first, Some(&original_receipts[0]))
        );
        let original = receiver.try_recv_retained().unwrap();
        assert_eq!(
            (original.message, original.receipt.as_ref()),
            (second, Some(&original_receipts[1]))
        );
        assert_eq!(
            receiver.try_recv_retained().unwrap().message,
            "later-current"
        );
        assert!(receiver.prepare_recovered_prefix_release().is_err());
        assert!(
            receiver
                .append_native_retained(first, original_receipts[0].clone())
                .is_err()
        );
    }

    #[cfg(feature = "native-tail-replay")]
    #[test]
    fn failed_or_closed_startup_preserves_original_recovered_receipts_without_dispatch() {
        use nautilus_common::{live::ingress::IngressGate, recovery_trace::NativeInputSource};
        let parent = IngressGate::new();
        let (sender, mut original_receiver) =
            parent.native_channel::<String>(NativeInputSource::TradingCommand);
        sender
            .send("uncertain-original-command".to_owned())
            .unwrap();
        let (message, receipt) = original_receiver.try_recv().unwrap().into_parts();
        let original = receipt.clone();
        let child = IngressGate::new();
        let (_, current) = child.native_channel::<String>(NativeInputSource::TradingCommand);
        let mut receiver = SnapshotReceiver::from(current);
        receiver.append_native_retained(message, receipt).unwrap();
        receiver.close();
        assert!(matches!(
            receiver.try_recv_retained(),
            Err(TryRecvError::Disconnected)
        ));
        receiver.stage().unwrap();
        assert_eq!(receiver.len(), 1);
        assert_eq!(
            receiver.pending().next().unwrap(),
            "uncertain-original-command"
        );
        assert_eq!(receiver.pending_receipts().next().unwrap(), Some(&original));
        assert!(receiver.dispatch_is_empty());
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
