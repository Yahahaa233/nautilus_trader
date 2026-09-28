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

//! In-memory message bus for intra-process communication.
//!
//! # Messaging patterns
//!
//! - **Point-to-point**: Send messages to named endpoints via `send_*` functions.
//! - **Pub/sub**: Publish messages to topics via `publish_*`, subscribers receive
//!   all messages matching their pattern.
//! - **Request/response**: Register correlation IDs for response sequence tracking.
//!
//! # Architecture
//!
//! The bus uses thread-local storage for single-threaded async runtimes. Each
//! thread gets its own `MessageBus` instance, avoiding synchronization overhead.
//!
//! Two routing mechanisms serve different needs:
//!
//! - **Typed routing** (`publish_quote`, `subscribe_quotes`): Zero-cost dispatch
//!   for known types. Handlers receive `&T` directly with no runtime type checking.
//! - **Any-based routing** (`publish_any`, `subscribe_any`): Flexible dispatch for
//!   custom types and Python interop. Handlers receive `&dyn Any`.
//!
//! See [`core`] module documentation for design decisions and performance details.

pub mod config;
pub mod core;
pub mod matching;
pub mod message;
pub mod mstr;
pub mod stubs;
pub mod switchboard;
pub mod typed_endpoints;
pub mod typed_handler;
pub mod typed_router;

pub(crate) mod external;

mod api;
mod backing;

use std::{
    any::Any,
    cell::{Cell, RefCell},
    rc::Rc,
};

use nautilus_core::UUID4;
#[cfg(feature = "defi")]
use nautilus_model::defi::{Block, Pool, PoolFeeCollect, PoolFlash, PoolLiquidityUpdate, PoolSwap};
use nautilus_model::{
    data::{
        Bar, FundingRateUpdate, GreeksData, IndexPriceUpdate, MarkPriceUpdate, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick,
        option_chain::{OptionChainSlice, OptionGreeks},
    },
    events::{AccountState, OrderEventAny, PortfolioSnapshot, PositionEvent},
    instruments::InstrumentAny,
    orderbook::OrderBook,
};
use smallvec::SmallVec;

#[cfg(feature = "live")]
pub use self::backing::{
    MessageBusExternalIngress, MessageBusExternalReceiver, external_io_from_backing,
};
pub use self::{
    api::*,
    backing::{
        MessageBusBacking, MessageBusBackingFactory, MessageBusExternalEgress,
        external_egress_from_backing,
    },
    config::MessageBusConfig,
    core::{MessageBus, Subscription},
    message::{BusMessage, BusPayloadCategory, BusPayloadType},
    mstr::{Endpoint, MStr, Pattern, Topic},
    switchboard::MessagingSwitchboard,
    typed_endpoints::{EndpointMap, IntoEndpointMap},
    typed_handler::{
        CallbackHandler, Handler, IntoHandler, ShareableMessageHandler, TypedHandler,
        TypedIntoHandler,
    },
    typed_router::{TopicRouter, TypedSubscription},
};
use crate::timer::TimeEvent;

/// Inline capacity for handler buffers before heap allocation.
pub(super) const HANDLER_BUFFER_CAP: usize = 64;

// MessageBus is designed for single-threaded use within each async runtime.
// Thread-local storage ensures each thread gets its own instance, eliminating
// the need for unsafe Send/Sync implementations.
//
// Handler buffers provide zero-allocation publish on hot paths.
// Each buffer stores up to 64 handlers inline before spilling to heap.
// Publish functions use move-out/move-back to avoid holding RefCell borrows
// during handler calls (enabling re-entrant publishes).
thread_local! {
    pub(super) static MESSAGE_BUS: RefCell<Option<Rc<RefCell<MessageBus>>>> = const { RefCell::new(None) };
    pub(super) static HAS_EXTERNAL_EGRESS: Cell<bool> = const { Cell::new(false) };
    pub(super) static SUPPRESS_EXTERNAL_DEPTH: Cell<u32> = const { Cell::new(0) };

    pub(super) static ANY_HANDLERS: RefCell<SmallVec<[ShareableMessageHandler; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    pub(super) static DELTAS_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDeltas>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static DEPTH10_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDepth10>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static BOOK_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBook>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static QUOTE_HANDLERS: RefCell<SmallVec<[TypedHandler<QuoteTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static TRADE_HANDLERS: RefCell<SmallVec<[TypedHandler<TradeTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static BAR_HANDLERS: RefCell<SmallVec<[TypedHandler<Bar>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static MARK_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<MarkPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static INDEX_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<IndexPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static FUNDING_RATE_HANDLERS: RefCell<SmallVec<[TypedHandler<FundingRateUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static GREEKS_HANDLERS: RefCell<SmallVec<[TypedHandler<GreeksData>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static OPTION_GREEKS_HANDLERS: RefCell<SmallVec<[TypedHandler<OptionGreeks>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static OPTION_CHAIN_HANDLERS: RefCell<SmallVec<[TypedHandler<OptionChainSlice>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static ACCOUNT_STATE_HANDLERS: RefCell<SmallVec<[TypedHandler<AccountState>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static PORTFOLIO_SNAPSHOT_HANDLERS: RefCell<SmallVec<[TypedHandler<PortfolioSnapshot>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static ORDER_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderEventAny>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static POSITION_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<PositionEvent>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    pub(super) static INSTRUMENT_HANDLERS: RefCell<SmallVec<[TypedHandler<InstrumentAny>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    #[cfg(feature = "defi")]
    pub(super) static DEFI_BLOCK_HANDLERS: RefCell<SmallVec<[TypedHandler<Block>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_POOL_HANDLERS: RefCell<SmallVec<[TypedHandler<Pool>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_SWAP_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolSwap>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_LIQUIDITY_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolLiquidityUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_COLLECT_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFeeCollect>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    pub(super) static DEFI_FLASH_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFlash>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
}

/// Guard that prevents republished external messages from being forwarded again.
#[derive(Debug)]
pub struct SuppressExternalGuard;

impl SuppressExternalGuard {
    #[must_use]
    pub fn new() -> Self {
        SUPPRESS_EXTERNAL_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        Self
    }
}

impl Default for SuppressExternalGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SuppressExternalGuard {
    fn drop(&mut self) {
        SUPPRESS_EXTERNAL_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Sets the thread-local message bus, replacing any existing one.
pub fn set_message_bus(msgbus: Rc<RefCell<MessageBus>>) {
    reject_recovery_dispatch();
    HAS_EXTERNAL_EGRESS.with(|flag| flag.set(msgbus.borrow().has_external_egress()));
    MESSAGE_BUS.with(|bus| {
        *bus.borrow_mut() = Some(msgbus);
    });
}

/// Gets the thread-local message bus.
///
/// If no message bus has been set for this thread, a default one is created and initialized.
pub fn get_message_bus() -> Rc<RefCell<MessageBus>> {
    if let Some(bus) = try_get_message_bus() {
        return bus;
    }
    MESSAGE_BUS.with(|bus| {
        let mut slot = bus.borrow_mut();
        let rc = slot.get_or_insert_with(|| Rc::new(RefCell::new(MessageBus::default())));
        rc.clone()
    })
}

/// Returns the thread-local message bus if one has been set for this thread.
pub fn try_get_message_bus() -> Option<Rc<RefCell<MessageBus>>> {
    MESSAGE_BUS.with(|bus| bus.borrow().clone())
}

/// Observes dispatched bus traffic for the durable event store.
///
/// The bus invokes the registered tap (when present) before each publish, send, or
/// correlation response fanout, so subscribers cannot observe a message that has not
/// yet been handed to the tap. The tap callback runs on the engine thread and must be
/// cheap; it must not re-enter the bus (the bus is single-threaded and the call site
/// holds no live borrow of the bus, so any re-entrant publish would deadlock through
/// downstream `RefCell::borrow_mut` calls inside the registered tap).
pub trait BusTap: 'static {
    /// Invoked before a publish fanout dispatches to subscribers on `topic`.
    fn on_publish(&self, topic: MStr<Topic>, message: &dyn Any);

    /// Invoked before a send dispatch reaches the endpoint handler.
    fn on_send(&self, endpoint: MStr<Endpoint>, message: &dyn Any);

    /// Invoked before a correlation response dispatch reaches the response handler.
    fn on_response(&self, _correlation_id: &UUID4, _message: &dyn Any) {}
}

thread_local! {
    pub(super) static BUS_TAP: RefCell<Option<Rc<dyn BusTap>>> = const { RefCell::new(None) };
}

/// Registers `tap` as the thread-local bus tap, replacing any previously installed tap.
///
/// The tap fires before each publish, send, and correlation response fanout. Callers
/// are responsible for clearing the tap on shutdown via [`clear_bus_tap`] so a stale
/// adapter does not outlive the writer it captures into.
pub fn set_bus_tap(tap: Rc<dyn BusTap>) {
    reject_recovery_dispatch();
    BUS_TAP.with(|slot| {
        *slot.borrow_mut() = Some(tap);
    });
}

/// Clears the registered bus tap on this thread.
///
/// A no-op when no tap is installed.
pub fn clear_bus_tap() {
    reject_recovery_dispatch();
    BUS_TAP.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[inline]
pub(super) fn dispatch_tap_publish(topic: MStr<Topic>, message: &dyn Any) {
    reject_recovery_dispatch();
    // Clone the Rc so the cell borrow is released before the tap runs. The tap is
    // single-threaded with the bus; a re-entrant `set_bus_tap` during dispatch would
    // otherwise panic on RefCell.
    let tap = BUS_TAP.with(|slot| slot.borrow().clone());
    if let Some(tap) = tap {
        tap.on_publish(topic, message);
    }
}

#[inline]
pub(super) fn dispatch_tap_send(endpoint: MStr<Endpoint>, message: &dyn Any) {
    reject_recovery_dispatch();
    let tap = BUS_TAP.with(|slot| slot.borrow().clone());
    if let Some(tap) = tap {
        tap.on_send(endpoint, message);
    }
}

#[inline]
pub(super) fn dispatch_tap_response(correlation_id: &UUID4, message: &dyn Any) {
    reject_recovery_dispatch();
    let tap = BUS_TAP.with(|slot| slot.borrow().clone());
    if let Some(tap) = tap {
        tap.on_response(correlation_id, message);
    }
}

#[inline]
pub(crate) fn dispatch_tap_time_event(event: &TimeEvent) {
    dispatch_tap_publish(MessagingSwitchboard::time_event_topic(), event);
}

thread_local! {
    static BUS_DISPATCH_DEPTH: Cell<usize> = const { Cell::new(0) };
    static RECOVERY_CAPTURE_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static RECOVERY_CAPTURE_FAILED: Cell<bool> = const { Cell::new(false) };
}

/// Borrowed admission guard for a local-only, synchronous bus capture.
/// Only the inventory callback can obtain this guard; it grants no dispatch permission.
#[derive(Debug)]
pub struct LocalOnlyRecoveryInventory {
    _private: (),
}
impl LocalOnlyRecoveryInventory {
    /// Confirms no bus dispatch was attempted during the capture.
    ///
    /// # Errors
    /// Refuses an inactive boundary or any attempted publish/send/response.
    pub fn verify(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            RECOVERY_CAPTURE_ACTIVE.get() && !RECOVERY_CAPTURE_FAILED.get(),
            "message bus capture invalidated by dispatch"
        );
        Ok(())
    }
}
impl Drop for LocalOnlyRecoveryInventory {
    fn drop(&mut self) {
        RECOVERY_CAPTURE_ACTIVE.set(false);
    }
}
// Held by each central dispatch function through its full handler fanout.
#[derive(Debug)]
pub(super) struct BusDispatchScope;
impl BusDispatchScope {
    pub(super) fn enter() -> Self {
        reject_recovery_dispatch();
        BUS_DISPATCH_DEPTH.set(
            BUS_DISPATCH_DEPTH
                .get()
                .checked_add(1)
                .expect("message bus dispatch depth overflow"),
        );
        Self
    }
}
impl Drop for BusDispatchScope {
    fn drop(&mut self) {
        BUS_DISPATCH_DEPTH.set(BUS_DISPATCH_DEPTH.get() - 1);
    }
}

fn reject_recovery_dispatch() {
    if RECOVERY_CAPTURE_ACTIVE.get() {
        RECOVERY_CAPTURE_FAILED.set(true);
        panic!("message bus dispatch during paused recovery capture");
    }
}

/// Captures only a local bus with no external backing or outstanding responses.
/// Retains immutable bus and TLS-slot borrows, rejecting registry mutation and replacement.
/// Every dispatch attempt invalidates the guard even when its panic is caught.
///
/// # Errors
/// Refuses missing/busy buses, external I/O, outstanding responses, nested capture,
/// invalidated boundaries, and callback failures.
///
/// # Panics
/// Propagates callback panics. Dispatch and bus mutation during capture panic.
pub fn with_local_only_recovery_inventory<T>(
    capture: impl FnOnce(&LocalOnlyRecoveryInventory) -> anyhow::Result<T>,
) -> anyhow::Result<T> {
    MESSAGE_BUS.with(|slot| {
        let slot = slot.try_borrow()?;
        let bus = slot
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("message bus missing"))?;
        let bus = bus.try_borrow()?;
        bus.verify_local_only_recovery_inventory()?;
        anyhow::ensure!(
            BUS_DISPATCH_DEPTH.get() == 0,
            "message bus dispatch remains active"
        );
        anyhow::ensure!(!RECOVERY_CAPTURE_ACTIVE.get(), "nested message bus capture");
        RECOVERY_CAPTURE_FAILED.set(false);
        RECOVERY_CAPTURE_ACTIVE.set(true);
        let guard = LocalOnlyRecoveryInventory { _private: () };
        let result = capture(&guard);
        guard.verify()?;
        bus.verify_local_only_recovery_inventory()?;
        result
    })
}

#[cfg(test)]
mod recovery_inventory_tests {
    use super::*;
    #[test]
    fn local_only_inventory_rejects_reentry_from_actual_handler() {
        #[derive(Debug)]
        struct CaptureHandler(Rc<Cell<bool>>);
        impl Handler<dyn Any> for CaptureHandler {
            fn id(&self) -> ustr::Ustr {
                ustr::Ustr::from("capture-handler")
            }
            fn handle(&self, _: &dyn Any) {
                self.0
                    .set(with_local_only_recovery_inventory(|_| Ok(())).is_err());
            }
        }
        std::thread::spawn(|| {
            set_message_bus(Rc::new(RefCell::new(MessageBus::default())));
            let refused = Rc::new(Cell::new(false));
            register_any(
                "capture-handler".into(),
                typed_handler::shareable_handler(Rc::new(CaptureHandler(refused.clone()))),
            );
            send_any("capture-handler".into(), &());
            assert!(refused.get());
            with_local_only_recovery_inventory(|guard| guard.verify()).unwrap();
        })
        .join()
        .unwrap();
    }

    #[test]
    fn local_only_inventory_rejects_dispatch_replacement_and_pending_without_consuming() {
        std::thread::spawn(|| {
            let bus = Rc::new(RefCell::new(MessageBus::default()));
            set_message_bus(bus.clone());
            with_local_only_recovery_inventory(|guard| {
                assert!(Rc::ptr_eq(&get_message_bus(), &bus));
                assert!(bus.try_borrow_mut().is_err());
                guard.verify()
            })
            .unwrap();
            for operation in 0..4 {
                let result = with_local_only_recovery_inventory(|guard| {
                    let failed =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                            || match operation {
                                0 => dispatch_tap_publish(MStr::from("capture"), &()),
                                1 => dispatch_tap_send(MStr::from("capture"), &()),
                                2 => dispatch_tap_response(&UUID4::new(), &()),
                                _ => set_message_bus(Rc::new(RefCell::new(MessageBus::default()))),
                            },
                        ));
                    assert!(failed.is_err());
                    assert!(guard.verify().is_err());
                    Ok(())
                });
                assert!(result.is_err());
                assert!(Rc::ptr_eq(&get_message_bus(), &bus));
            }
            bus.borrow_mut().has_backing = true;
            assert!(with_local_only_recovery_inventory(|_| Ok(())).is_err());
            bus.borrow_mut().has_backing = false;
            let correlation = UUID4::new();
            bus.borrow_mut()
                .register_response_handler(&correlation, stubs::get_stub_shareable_handler(None))
                .unwrap();
            assert!(with_local_only_recovery_inventory(|_| Ok(())).is_err());
            assert!(bus.borrow().get_response_handler(&correlation).is_some());
        })
        .join()
        .unwrap();
    }
}
