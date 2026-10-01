//! Owned queue prefixes preserve non-cloneable messages during borrowed encoding.
use std::{
    collections::VecDeque,
    task::{Context, Poll},
};
use tokio::sync::mpsc::{UnboundedReceiver, error::TryRecvError};

/// A receiver that retains staged messages and always consumes its prefix first.
/// There is deliberately no method to extract the underlying receiver alone.
#[derive(Debug)]
pub struct SnapshotReceiver<T> {
    prefix: VecDeque<T>,
    receiver: UnboundedReceiver<T>,
}
impl<T> From<UnboundedReceiver<T>> for SnapshotReceiver<T> {
    fn from(receiver: UnboundedReceiver<T>) -> Self {
        Self {
            prefix: VecDeque::new(),
            receiver,
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
        self.prefix.is_empty() && self.receiver.is_empty()
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
        match self.prefix.pop_front() {
            Some(message) => Ok(message),
            None => self.receiver.try_recv(),
        }
    }
    /// Waits for the oldest message, consuming retained messages first.
    pub async fn recv(&mut self) -> Option<T> {
        std::future::poll_fn(|context| self.poll_recv(context)).await
    }
    /// Polls for the oldest retained or channel-resident message.
    pub fn poll_recv(&mut self, context: &mut Context<'_>) -> Poll<Option<T>> {
        match self.prefix.pop_front() {
            Some(message) => Poll::Ready(Some(message)),
            None => self.receiver.poll_recv(context),
        }
    }
    pub(super) fn stage(&mut self) -> anyhow::Result<()> {
        // Reserve before consuming: an allocation failure cannot drop a moved message.
        while !self.receiver.is_empty() {
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
        self.prefix.iter()
    }
    pub(crate) fn prepend_retained(&mut self, retained: &mut VecDeque<T>) -> anyhow::Result<()> {
        self.prefix.try_reserve(retained.len())?;
        while let Some(message) = retained.pop_back() {
            self.prefix.push_front(message);
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
