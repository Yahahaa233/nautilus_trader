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

//! Exact callback ancestry for caller-frozen native channel messages.

use std::{marker::PhantomData, rc::Rc, sync::Arc, thread};

use ahash::{AHashMap, AHashSet};
use serde::{Deserialize, Serialize};

use super::{
    CHAIN_BYTES, COMMAND_CONTEXTS, Chain, ChainContext, CommandContexts, DISPATCH, DispatchError,
    Dispatcher, MAX_CHAIN, MAX_KNOWN_BYTES, SendChainContext, Slot,
};

const SCHEMA: &str = "NautilusCallbackAncestry.v1";

/// An opaque borrow of the actual context still attached to a channel message.
#[derive(Debug)]
pub struct CallbackContextRef<'a> {
    context: &'a SendChainContext,
    marker: PhantomData<Rc<()>>,
}

impl<'a> CallbackContextRef<'a> {
    pub(crate) const fn new(context: &'a SendChainContext) -> Self {
        Self {
            context,
            marker: PhantomData,
        }
    }
}

/// Ancestry aligned with a caller's exact original channel receipt inventory.
///
/// Chain ordinals follow first occurrence in that inventory; they imply no global FIFO.
/// The native owner must independently bind this state to its admitted cut and receipts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbackAncestryState {
    pub schema: String,
    pub entry_chains: Vec<Option<u32>>,
    pub entry_context_ids: Vec<Option<u64>>,
    pub chain_delivered: Vec<u64>,
    pub publication_sequence: u64,
    pub next_context_id: u64,
    pub pending_capacity: u64,
}

impl CallbackAncestryState {
    /// Validates the canonical inventory and original callback resource limits.
    ///
    /// # Errors
    /// Returns an invalid-checkpoint error for malformed or unsupported state.
    pub fn validate(&self) -> Result<(), DispatchError> {
        if self.schema != SCHEMA
            || self.entry_chains.len() != self.entry_context_ids.len()
            || self.chain_delivered.len() > self.entry_chains.len()
            || self
                .chain_delivered
                .iter()
                .any(|&count| count > MAX_CHAIN as u64)
        {
            return Err(DispatchError::InvalidCheckpoint);
        }

        let capacity =
            usize::try_from(self.pending_capacity).map_err(|_| DispatchError::InvalidCheckpoint)?;
        let queue_bytes = capacity
            .checked_mul(size_of::<((u64, u64), Rc<Slot>)>())
            .ok_or(DispatchError::InvalidCheckpoint)?;
        let chain_bytes = self
            .chain_delivered
            .len()
            .checked_mul(CHAIN_BYTES)
            .and_then(|bytes| bytes.checked_add(queue_bytes))
            .ok_or(DispatchError::InvalidCheckpoint)?;
        if chain_bytes > MAX_KNOWN_BYTES {
            return Err(DispatchError::InvalidCheckpoint);
        }

        let mut ids = AHashSet::new();
        let mut seen_chains = AHashSet::new();
        for (&chain, &id) in self.entry_chains.iter().zip(&self.entry_context_ids) {
            match (chain, id) {
                (None, None) => {}
                (Some(chain), Some(id)) => {
                    let ordinal = chain as usize;
                    if id == 0
                        || id > self.next_context_id
                        || !ids.insert(id)
                        || ordinal >= self.chain_delivered.len()
                        || (seen_chains.insert(chain) && ordinal != seen_chains.len() - 1)
                    {
                        return Err(DispatchError::InvalidCheckpoint);
                    }
                }
                _ => return Err(DispatchError::InvalidCheckpoint),
            }
        }
        if seen_chains.len() != self.chain_delivered.len() {
            return Err(DispatchError::InvalidCheckpoint);
        }
        Ok(())
    }
}

/// A single-use restored context; it cannot be cloned or detached from its owner ticket.
#[derive(Debug)]
pub struct RestoredCallbackContext(SendChainContext);

impl RestoredCallbackContext {
    pub(crate) fn into_context(self) -> SendChainContext {
        self.0
    }
}

/// Restored contexts aligned with every original inventory entry, including independent inputs.
#[derive(Debug)]
struct RestoredContextSlot(Option<RestoredCallbackContext>);

#[derive(Debug)]
pub struct RestoredChannelContexts {
    entries: Vec<Option<RestoredContextSlot>>,
    marker: PhantomData<Rc<()>>,
}

impl RestoredChannelContexts {
    /// Transfers an entry once to the corresponding decoded native channel message.
    ///
    /// # Errors
    /// Returns an invalid-checkpoint error for an absent or already transferred entry.
    pub fn take(&mut self, index: usize) -> Result<Option<RestoredCallbackContext>, DispatchError> {
        self.entries
            .get_mut(index)
            .and_then(Option::take)
            .map(|slot| slot.0)
            .ok_or(DispatchError::InvalidCheckpoint)
    }
}

fn collect_releases() -> Result<(), DispatchError> {
    COMMAND_CONTEXTS
        .try_with(|contexts| {
            let mut contexts = contexts
                .try_borrow_mut()
                .map_err(|_| DispatchError::Active)?;
            if let Some(contexts) = contexts.as_mut() {
                while let Ok(id) = contexts.releases.try_recv() {
                    contexts.pending.remove(&id);
                }
            }
            Ok(())
        })
        .map_err(|_| DispatchError::Active)?
}

fn require_inactive(state: &Dispatcher) -> Result<(), DispatchError> {
    if let Some(e) = state.error {
        return Err(e);
    }
    if state.current.is_some()
        || state.publication.is_some()
        || state.draining
        || state.clearing
        || state.depth != 0
        || state.chain_depth != 0
        || !state.pending.is_empty()
        || state.accounting.count.get() != 0
        || state.accounting.reservations.get() != 0
        || super::super::access::is_active()
    {
        return Err(DispatchError::Active);
    }
    Ok(())
}

/// Captures only contexts proved to belong to every caller-frozen physical native FIFO.
///
/// The borrow preserves the original messages, IDs, accounting, and shared chain budgets.
///
/// # Errors
/// Refuses incomplete, duplicate, foreign, or stale tickets and all unaccounted actor work.
pub fn capture_channel_contexts(
    entries: &[Option<CallbackContextRef<'_>>],
) -> Result<CallbackAncestryState, DispatchError> {
    collect_releases()?;
    DISPATCH
        .try_with(|state| {
            let state = state.try_borrow().map_err(|_| DispatchError::Active)?;
            require_inactive(&state)?;
            COMMAND_CONTEXTS
                .try_with(|contexts| {
                    let contexts = contexts.try_borrow().map_err(|_| DispatchError::Active)?;
                    let contexts = contexts.as_ref();
                    let mut ids = AHashSet::new();
                    let mut chains = AHashMap::new();
                    let mut checkpoint = CallbackAncestryState {
                        schema: SCHEMA.to_owned(),
                        entry_chains: Vec::with_capacity(entries.len()),
                        entry_context_ids: Vec::with_capacity(entries.len()),
                        chain_delivered: Vec::new(),
                        publication_sequence: state.sequence,
                        next_context_id: contexts.map_or(0, |contexts| contexts.next),
                        pending_capacity: state.pending.capacity() as u64,
                    };
                    for entry in entries {
                        let Some(ticket) = entry else {
                            checkpoint.entry_chains.push(None);
                            checkpoint.entry_context_ids.push(None);
                            continue;
                        };
                        if !ticket.context.is_owner()
                            || !ids.insert(ticket.context.id)
                            || contexts.is_none_or(|contexts| {
                                !Arc::ptr_eq(&ticket.context.released, &contexts.released)
                            })
                        {
                            return Err(DispatchError::InvalidCheckpoint);
                        }
                        let context = contexts
                            .and_then(|contexts| contexts.pending.get(&ticket.context.id))
                            .ok_or(DispatchError::InvalidCheckpoint)?;
                        let chain = context
                            .chain
                            .as_ref()
                            .ok_or(DispatchError::InvalidCheckpoint)?;
                        if !Rc::ptr_eq(&chain.accounting, &state.accounting) {
                            return Err(DispatchError::InvalidCheckpoint);
                        }
                        let next = u32::try_from(chains.len())
                            .map_err(|_| DispatchError::InvalidCheckpoint)?;
                        let ordinal = *chains.entry(Rc::as_ptr(chain)).or_insert_with(|| {
                            checkpoint
                                .chain_delivered
                                .push(chain.delivered.get() as u64);
                            next
                        });
                        checkpoint.entry_chains.push(Some(ordinal));
                        checkpoint.entry_context_ids.push(Some(ticket.context.id));
                    }
                    if ids.len() != contexts.map_or(0, |contexts| contexts.pending.len())
                        || state.accounting.contexts.get() != ids.len()
                        || Some(state.accounting.bytes.get())
                            != chains.len().checked_mul(CHAIN_BYTES)
                    {
                        return Err(DispatchError::Active);
                    }
                    checkpoint.validate()?;
                    Ok(checkpoint)
                })
                .map_err(|_| DispatchError::Active)?
        })
        .map_err(|_| DispatchError::Active)?
}

#[cfg(all(test, feature = "live"))]
mod tests {
    use std::cell::RefCell;

    use rstest::rstest;

    use super::*;
    use crate::{
        actor::{callbacks_pending, clear_callbacks, drain_callbacks},
        live::{dispatch::DispatchMessage, sender::DispatchSender},
    };

    fn send_pair(value: &mut (DispatchSender<u32>, u32)) -> bool {
        value.0.send(value.1).unwrap();
        value.0.send(value.1 + 1).unwrap();
        true
    }

    fn record(value: &mut Rc<RefCell<Vec<u32>>>) -> bool {
        value.borrow_mut().push(1);
        true
    }

    fn capture(messages: &[DispatchMessage<u32>]) -> CallbackAncestryState {
        capture_channel_contexts(
            &messages
                .iter()
                .map(DispatchMessage::checkpoint_context)
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    // Copy the data while consuming every original message before restoring the
    // callback bank; a lazy iterator would keep live original leases during restore.
    fn consume_message_values(messages: Vec<DispatchMessage<u32>>) -> Vec<u32> {
        messages
            .into_iter()
            .map(|message| *message.message())
            .collect()
    }

    fn rooted_message(value: u32) -> DispatchMessage<u32> {
        let _publication = super::super::PublicationScope::enter();
        DispatchMessage::new(value, thread::current().id())
    }

    #[rstest]
    fn actual_channel_ancestry_checkpoint_preserves_shared_chain_and_delivery_budget() {
        clear_callbacks().unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sender = DispatchSender::new(tx);
        super::super::reserve(0)
            .unwrap()
            .commit((sender.clone(), 10), send_pair);
        assert_eq!(drain_callbacks(1), Ok(false));
        super::super::reserve(0)
            .unwrap()
            .commit((sender, 20), send_pair);
        assert_eq!(drain_callbacks(1), Ok(false));
        let messages = vec![
            rx.try_recv().unwrap(),
            DispatchMessage::from(99),
            rx.try_recv().unwrap(),
            rx.try_recv().unwrap(),
            rx.try_recv().unwrap(),
        ];
        let snapshot = capture(&messages);
        assert_eq!(
            snapshot.entry_chains,
            [Some(0), None, Some(0), Some(1), Some(1)]
        );
        assert_eq!(snapshot.chain_delivered, [1, 1]);
        assert!(callbacks_pending());
        assert!(matches!(
            restore_channel_contexts(&snapshot),
            Err(DispatchError::Active)
        ));
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let decoded: CallbackAncestryState = serde_json::from_slice(&bytes).unwrap();
        let values = consume_message_values(messages);
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();

        let mut bank = restore_channel_contexts(&decoded).unwrap();
        let mut restored: Vec<_> = values
            .into_iter()
            .enumerate()
            .map(|(index, message)| {
                DispatchMessage::from_checkpoint_context(message, bank.take(index).unwrap())
            })
            .collect();
        assert!(matches!(
            bank.take(1),
            Err(DispatchError::InvalidCheckpoint)
        ));
        assert_eq!(capture(&restored), snapshot);
        let received = Rc::new(RefCell::new(Vec::new()));
        restored.remove(0).dispatch(|value| {
            assert_eq!(value, 10);
            super::super::reserve(0)
                .unwrap()
                .commit(received.clone(), record);
        });
        assert_eq!(drain_callbacks(1), Ok(false));
        assert_eq!(capture(&restored).chain_delivered, [2, 1]);
        assert_eq!(*received.borrow(), [1]);
        drop(restored);
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();
    }

    #[rstest]
    fn actual_channel_ancestry_capture_rejects_missing_duplicate_and_private_actor_work() {
        clear_callbacks().unwrap();
        let message = rooted_message(7);
        let snapshot = capture(std::slice::from_ref(&message));
        assert_eq!(capture_channel_contexts(&[]), Err(DispatchError::Active));
        assert_eq!(
            capture_channel_contexts(&[message.checkpoint_context(), message.checkpoint_context()]),
            Err(DispatchError::InvalidCheckpoint)
        );
        let retained = super::super::retain(0).unwrap();
        assert_eq!(
            capture_channel_contexts(&[message.checkpoint_context()]),
            Err(DispatchError::Active)
        );
        drop(retained);
        let reservation = super::super::reserve::<u32>(0).unwrap();
        assert_eq!(
            capture_channel_contexts(&[message.checkpoint_context()]),
            Err(DispatchError::Active)
        );
        drop(reservation);
        drain_callbacks(1).unwrap();
        {
            let _publication = super::super::PublicationScope::enter();
            assert_eq!(
                capture_channel_contexts(&[message.checkpoint_context()]),
                Err(DispatchError::Active)
            );
        }
        assert_eq!(
            capture(std::slice::from_ref(&message)).entry_context_ids,
            snapshot.entry_context_ids
        );
        super::super::record_failure(DispatchError::Runaway);
        assert_eq!(
            capture_channel_contexts(&[message.checkpoint_context()]),
            Err(DispatchError::Runaway)
        );
        assert!(matches!(
            restore_channel_contexts(&snapshot),
            Err(DispatchError::Runaway)
        ));
        drop(message);
        clear_callbacks().unwrap();
    }

    #[rstest]
    #[case::duplicate_id(0)]
    #[case::missing_id(1)]
    #[case::noncanonical_chain(2)]
    #[case::exhausted_budget(3)]
    #[case::unaccounted_chain(4)]
    #[case::excess_storage(5)]
    #[case::future_id(6)]
    fn actual_channel_ancestry_restore_refuses_corrupt_state_without_target_mutation(
        #[case] corruption: usize,
    ) {
        clear_callbacks().unwrap();
        let first = rooted_message(1);
        let second = rooted_message(2);
        let mut snapshot = capture(&[first, second]);
        clear_callbacks().unwrap();
        let before = capture_channel_contexts(&[]).unwrap();
        match corruption {
            0 => snapshot.entry_context_ids[1] = snapshot.entry_context_ids[0],
            1 => snapshot.entry_context_ids[0] = None,
            2 => snapshot.entry_chains.swap(0, 1),
            3 => snapshot.chain_delivered[0] = MAX_CHAIN as u64 + 1,
            4 => snapshot.chain_delivered.push(0),
            5 => snapshot.pending_capacity = MAX_KNOWN_BYTES as u64,
            6 => snapshot.next_context_id = 0,
            _ => unreachable!(),
        }
        assert!(matches!(
            restore_channel_contexts(&snapshot),
            Err(DispatchError::InvalidCheckpoint)
        ));
        assert_eq!(capture_channel_contexts(&[]).unwrap(), before);
        clear_callbacks().unwrap();
    }

    #[rstest]
    fn actual_channel_ancestry_restore_keeps_runaway_limit_and_bank_cleanup() {
        clear_callbacks().unwrap();
        let messages = vec![rooted_message(1), DispatchMessage::from(2)];
        let mut snapshot = capture(&messages);
        drop(messages);
        clear_callbacks().unwrap();
        snapshot.chain_delivered[0] = MAX_CHAIN as u64;
        let mut bank = restore_channel_contexts(&snapshot).unwrap();
        let message = DispatchMessage::from_checkpoint_context(1, bank.take(0).unwrap());
        let received = Rc::new(RefCell::new(Vec::new()));
        message.dispatch(|_| {
            super::super::reserve(0)
                .unwrap()
                .commit(received.clone(), record);
        });
        assert_eq!(drain_callbacks(1), Err(DispatchError::Runaway));
        assert!(received.borrow().is_empty());
        drop(bank);
        clear_callbacks().unwrap();

        let mut bank = restore_channel_contexts(&snapshot).unwrap();
        let token = bank.take(0).unwrap();
        std::thread::spawn(move || drop(token)).join().unwrap();
        assert!(!callbacks_pending());
        drop(bank);
        clear_callbacks().unwrap();
    }

    #[rstest]
    fn actual_channel_ancestry_restore_isolates_old_taken_ticket_release_epoch(
        #[values(false, true)] foreign_drop: bool,
    ) {
        clear_callbacks().unwrap();
        let message = rooted_message(1);
        let snapshot = capture(std::slice::from_ref(&message));
        drop(message.checkpoint_context().unwrap().context.take());
        clear_callbacks().unwrap();
        let mut bank = restore_channel_contexts(&snapshot).unwrap();
        let restored = DispatchMessage::from_checkpoint_context(1, bank.take(0).unwrap());
        assert_eq!(
            capture_channel_contexts(&[message.checkpoint_context()]),
            Err(DispatchError::InvalidCheckpoint)
        );
        if foreign_drop {
            std::thread::spawn(move || drop(message)).join().unwrap();
        } else {
            drop(message);
        }
        assert_eq!(capture(std::slice::from_ref(&restored)), snapshot);
        drop(restored);
        drop(bank);
        assert!(!callbacks_pending());
        clear_callbacks().unwrap();
    }

    #[rstest]
    fn actual_channel_ancestry_capture_refuses_foreign_ticket_and_checked_actor_access() {
        clear_callbacks().unwrap();
        let foreign = std::thread::spawn(|| rooted_message(8)).join().unwrap();
        assert_eq!(
            capture_channel_contexts(&[foreign.checkpoint_context()]),
            Err(DispatchError::InvalidCheckpoint)
        );
        drop(foreign);
        let access = super::super::super::access::AllocationGuard::acquire(Rc::new(
            std::cell::UnsafeCell::new(0_u32),
        ))
        .unwrap();
        assert_eq!(capture_channel_contexts(&[]), Err(DispatchError::Active));
        drop(access);
        assert!(capture_channel_contexts(&[]).is_ok());
        clear_callbacks().unwrap();
    }

    #[rstest]
    fn actual_channel_ancestry_capture_refuses_direct_context_outside_physical_fifo() {
        clear_callbacks().unwrap();
        let message = rooted_message(1);
        let context = message.dispatch(|_| ChainContext::capture());
        assert_eq!(capture_channel_contexts(&[]), Err(DispatchError::Active));
        drop(context);
        assert!(capture_channel_contexts(&[]).is_ok());
        clear_callbacks().unwrap();
    }
}

/// Restores shared ancestry into an entirely quiescent owner dispatcher.
///
/// All validation and allocation finish before either thread-local owner is replaced.
/// The returned single-use tickets must be attached to the exact admitted native receipts.
///
/// # Errors
/// Refuses malformed state, failed or active target dispatchers, and unsupported allocations.
pub fn restore_channel_contexts(
    checkpoint: &CallbackAncestryState,
) -> Result<RestoredChannelContexts, DispatchError> {
    checkpoint.validate()?;
    collect_releases()?;
    DISPATCH
        .try_with(|state| {
            let mut state = state.try_borrow_mut().map_err(|_| DispatchError::Active)?;
            require_inactive(&state)?;
            COMMAND_CONTEXTS
                .try_with(|contexts| {
                    let mut contexts = contexts
                        .try_borrow_mut()
                        .map_err(|_| DispatchError::Active)?;
                    if state.accounting.contexts.get() != 0
                        || state.accounting.bytes.get() != 0
                        || contexts
                            .as_ref()
                            .is_some_and(|contexts| !contexts.pending.is_empty())
                    {
                        return Err(DispatchError::Active);
                    }

                    let mut restored = Dispatcher {
                        sequence: checkpoint.publication_sequence,
                        ..Dispatcher::default()
                    };
                    let capacity = usize::try_from(checkpoint.pending_capacity)
                        .map_err(|_| DispatchError::InvalidCheckpoint)?;
                    restored
                        .pending
                        .try_reserve_exact(capacity)
                        .map_err(|_| DispatchError::Overflow)?;
                    if restored.pending.capacity() != capacity {
                        return Err(DispatchError::InvalidCheckpoint);
                    }
                    let mut restored_contexts = CommandContexts {
                        next: checkpoint.next_context_id,
                        ..CommandContexts::default()
                    };
                    let rooted = checkpoint.entry_context_ids.iter().flatten().count();
                    restored_contexts
                        .pending
                        .try_reserve(rooted)
                        .map_err(|_| DispatchError::Overflow)?;
                    let mut chains = Vec::new();
                    chains
                        .try_reserve_exact(checkpoint.chain_delivered.len())
                        .map_err(|_| DispatchError::Overflow)?;
                    let mut entries = Vec::new();
                    entries
                        .try_reserve_exact(checkpoint.entry_chains.len())
                        .map_err(|_| DispatchError::Overflow)?;
                    for &delivered in &checkpoint.chain_delivered {
                        restored
                            .accounting
                            .bytes
                            .set(restored.accounting.bytes.get() + CHAIN_BYTES);
                        chains.push(Rc::new(Chain {
                            delivered: std::cell::Cell::new(delivered as usize),
                            accounting: restored.accounting.clone(),
                        }));
                    }
                    for (&ordinal, &id) in checkpoint
                        .entry_chains
                        .iter()
                        .zip(&checkpoint.entry_context_ids)
                    {
                        let token = match (ordinal, id) {
                            (Some(ordinal), Some(id)) => {
                                restored
                                    .accounting
                                    .contexts
                                    .set(restored.accounting.contexts.get() + 1);
                                restored_contexts.pending.insert(
                                    id,
                                    ChainContext {
                                        chain: Some(chains[ordinal as usize].clone()),
                                    },
                                );
                                Some(RestoredCallbackContext(SendChainContext {
                                    id,
                                    owner: thread::current().id(),
                                    released: restored_contexts.released.clone(),
                                }))
                            }
                            (None, None) => None,
                            _ => unreachable!("checkpoint has been validated"),
                        };
                        entries.push(Some(RestoredContextSlot(token)));
                    }
                    drop(chains);
                    *state = restored;
                    *contexts = Some(restored_contexts);
                    Ok(RestoredChannelContexts {
                        entries,
                        marker: PhantomData,
                    })
                })
                .map_err(|_| DispatchError::Active)?
        })
        .map_err(|_| DispatchError::Active)?
}
