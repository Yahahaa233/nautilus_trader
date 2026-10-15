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

//! Message senders for standalone clients and live runtime dispatch.

use std::thread::{self, ThreadId};

use super::{dispatch::DispatchMessage, ingress::IngressSender};

/// A message sender which preserves callback ancestry when bound to a live runtime.
///
/// Sends on the runtime owner thread capture its active root. Sends on other threads
/// and sends without an active root are independent ingress. Conversion from a plain Tokio sender supports standalone
/// clients whose receivers consume domain events directly, without callback tracking.
#[derive(Debug)]
pub struct DispatchSender<T> {
    channel: DispatchChannel<T>,
}

/// A sender for data, execution, and system events.
pub type EventSender<T> = DispatchSender<T>;

impl<T> DispatchSender<T> {
    /// Binds a dispatch channel to the calling runtime thread.
    #[must_use]
    pub fn new(sender: impl Into<Self>) -> Self {
        sender.into()
    }

    // panics-doc-ok
    /// Sends a message, preserving the active owner-thread callback root.
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver is closed. The error contains the undelivered envelope.
    ///
    /// # Panics
    ///
    /// Panics if the owner exhausts its channel context IDs.
    pub fn send(
        &self,
        message: T,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<DispatchMessage<T>>>
    where
        T: 'static,
    {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender
                .send(message)
                .map_err(|e| tokio::sync::mpsc::error::SendError(e.0.into())),
            DispatchChannel::Managed { sender, owner } => {
                sender.send(DispatchMessage::new(message, *owner))
            }
            DispatchChannel::Dispatch { sender, owner } => {
                sender.send(DispatchMessage::new(message, *owner))
            }
        }
    }

    /// Returns whether the receiver has closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.is_closed(),
            DispatchChannel::Dispatch { sender, .. } => sender.is_closed(),
            DispatchChannel::Managed { sender, .. } => sender.is_closed(),
        }
    }

    /// Returns whether both senders target the same channel.
    #[must_use]
    pub fn same_channel(&self, other: &Self) -> bool {
        match (&self.channel, &other.channel) {
            (DispatchChannel::Plain(left), DispatchChannel::Plain(right)) => {
                left.same_channel(right)
            }
            (
                DispatchChannel::Dispatch { sender: left, .. },
                DispatchChannel::Dispatch { sender: right, .. },
            ) => left.same_channel(right),
            (
                DispatchChannel::Managed { sender: left, .. },
                DispatchChannel::Managed { sender: right, .. },
            ) => left.same_channel(right),
            _ => false,
        }
    }
    /// Waits for receiver closure without changing admission state.
    pub async fn closed(&self) {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.closed().await,
            DispatchChannel::Dispatch { sender, .. } => sender.closed().await,
            DispatchChannel::Managed { sender, .. } => sender.closed().await,
        }
    }

    /// Invalidates a retained checkpoint when its bound sender is replaced.
    pub fn invalidate_snapshot(&self) {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.invalidate_snapshot(),
            DispatchChannel::Managed { sender, .. } | DispatchChannel::Dispatch { sender, .. } => {
                sender.invalidate_snapshot();
            }
        }
    }
    /// Permanently fails the same original producer admission boundary.
    pub fn fail(&self, reason: &str) {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.fail(reason),
            DispatchChannel::Managed { sender, .. } | DispatchChannel::Dispatch { sender, .. } => {
                sender.fail(reason);
            }
        }
    }

    /// Proves ownership of this sender's actual admission boundary.
    #[must_use]
    pub fn belongs_to(&self, gate: &super::ingress::IngressGate) -> bool {
        match &self.channel {
            DispatchChannel::Plain(sender) => sender.belongs_to(gate),
            DispatchChannel::Managed { sender, .. } | DispatchChannel::Dispatch { sender, .. } => {
                sender.belongs_to(gate)
            }
        }
    }
}

impl<T> Clone for DispatchSender<T> {
    fn clone(&self) -> Self {
        let channel = match &self.channel {
            DispatchChannel::Plain(sender) => DispatchChannel::Plain(sender.clone()),
            DispatchChannel::Managed { sender, owner } => DispatchChannel::Managed {
                sender: sender.clone(),
                owner: *owner,
            },
            DispatchChannel::Dispatch { sender, owner } => DispatchChannel::Dispatch {
                sender: sender.clone(),
                owner: *owner,
            },
        };

        Self { channel }
    }
}

impl<T> From<tokio::sync::mpsc::UnboundedSender<T>> for DispatchSender<T> {
    fn from(sender: tokio::sync::mpsc::UnboundedSender<T>) -> Self {
        Self {
            channel: DispatchChannel::Plain(sender.into()),
        }
    }
}

impl<T> From<tokio::sync::mpsc::UnboundedSender<DispatchMessage<T>>> for DispatchSender<T> {
    fn from(sender: tokio::sync::mpsc::UnboundedSender<DispatchMessage<T>>) -> Self {
        Self {
            channel: DispatchChannel::Dispatch {
                sender: sender.into(),
                owner: thread::current().id(),
            },
        }
    }
}

impl<T> From<IngressSender<DispatchMessage<T>>> for DispatchSender<T> {
    fn from(sender: IngressSender<DispatchMessage<T>>) -> Self {
        Self {
            channel: DispatchChannel::Managed {
                sender,
                owner: thread::current().id(),
            },
        }
    }
}

#[derive(Debug)]
enum DispatchChannel<T> {
    Plain(IngressSender<T>),
    Managed {
        sender: IngressSender<DispatchMessage<T>>,
        owner: ThreadId,
    },
    Dispatch {
        sender: IngressSender<DispatchMessage<T>>,
        owner: ThreadId,
    },
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{live::ingress::IngressGate, recovery_trace::NativeInputSource};

    #[rstest]
    fn managed_dispatch_keeps_native_receipt_and_original_freeze_boundary() {
        let gate = IngressGate::new();
        let (native, mut receiver) =
            gate.native_channel::<DispatchMessage<u64>>(NativeInputSource::SystemEvent);
        let sender = EventSender::<u64>::new(native);
        assert!(sender.belongs_to(&gate));
        sender.send(7).unwrap();
        let (envelope, receipt) = receiver.try_recv().unwrap().into_parts();
        assert_eq!(receipt.input_source, NativeInputSource::SystemEvent);
        assert_eq!(*envelope.message(), 7);
        assert_eq!(envelope.dispatch(|value| value), 7);
        let frozen = gate.freeze().unwrap();
        let cloned_sender = sender.clone();
        assert!(cloned_sender.send(8).is_err());
        assert!(sender.send(9).is_err());
        assert!(receiver.try_recv().is_err());
        assert!(frozen.finish().is_err());
    }

    #[rstest]
    fn standalone_plain_sender_retains_independent_events_and_latched_failure() {
        // This standalone fixture receives the plain domain value.
        let (native, mut receiver) = tokio::sync::mpsc::unbounded_channel::<u64>();
        let sender: EventSender<u64> = native.into();
        sender.send(7).unwrap();
        assert_eq!(receiver.try_recv().unwrap(), 7);
        sender.fail("actual standalone producer failed");
        let cloned_sender = sender.clone();
        assert!(cloned_sender.send(8).is_err());
        assert!(sender.send(9).is_err());
        assert!(receiver.try_recv().is_err());
    }
}
