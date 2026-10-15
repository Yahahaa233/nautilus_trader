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

// Actual registered original-object replay, including SDK framework effects.
use super::tests::actual_node;
use super::*;
use crate::{
    node::{NodeRunMode, RunningCheckpointSchedule},
    runner_recovery::{
        RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryCodecRegistry,
        RunnerRecoveryEnvelope, RunnerRecoveryEventRef,
    },
};
use indexmap::IndexMap;
use nautilus_common::{
    actor::{
        DataActor, DataActorNative, indicators::ActorIndicator, registry::get_actor_unchecked,
    },
    cache::Cache,
    component::Component,
    enums::ComponentState,
    messages::{
        DataEvent, ExecutionEvent,
        data::{BarsResponse, DataResponse, QuotesResponse},
    },
    msgbus::{self, ShareableMessageHandler, TypedHandler, switchboard},
    recovery_trace::{
        NativeComponentLifecycle,
        historical::{HistoricalInputBoundary, HistoricalReplayPreparation},
    },
    runner::TradingCommandMessage,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_event_store::{EventStoreReader, backend::RedbBackend, kernel::EventStoreLifecycle};
use nautilus_model::{
    data::{Bar, BarType, Data, QuoteTick, TradeTick},
    enums::{OrderSide, OrderStatus, OrderType, TimeInForce},
    events::{
        OrderAccepted, OrderCanceled, OrderDenied, OrderEventAny, OrderPendingCancel,
        OrderSubmitted,
    },
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderAny, OrderTestBuilder},
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore, StrategyNative},
};
use rstest::rstest;
use std::{
    any::Any,
    cell::{Cell, RefCell},
    rc::Rc,
    time::Duration,
};

const STRATEGY: &str = "FrameworkTail-001";
const ORDER: &str = "O-FRAMEWORK-GTD-001";
fn instrument_id() -> InstrumentId {
    crypto_perpetual_ethusdt().id()
}
fn bar_type() -> BarType {
    BarType::from("ETHUSDT-PERP.BINANCE-1-MINUTE-LAST-EXTERNAL")
}

// A real indicator registered in DataActorCore, with separate nonempty state for
// every data kind. The business callbacks verify that the SDK updated it once.
#[derive(Debug)]
struct CountingIndicator {
    counts: Cell<[u64; 3]>,
}
impl ActorIndicator for CountingIndicator {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).cast::<()>() as usize
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn initialized(&self) -> Result<bool> {
        Ok(true)
    }
    fn handle_quote(&self, _: &QuoteTick) -> Result<()> {
        self.bump(0);
        Ok(())
    }
    fn handle_trade(&self, _: &TradeTick) -> Result<()> {
        self.bump(1);
        Ok(())
    }
    fn handle_bar(&self, _: &Bar) -> Result<()> {
        self.bump(2);
        Ok(())
    }
}
impl CountingIndicator {
    fn bump(&self, index: usize) {
        let mut n = self.counts.get();
        n[index] += 1;
        self.counts.set(n);
    }
}
#[derive(Clone, Copy, Debug)]
struct TimerCompletion {
    market_exit: bool,
    gtd: bool,
}

#[derive(Clone, Copy, Debug)]
struct FrameworkBehavior {
    manage_stop: bool,
    completion: TimerCompletion,
    fail_historical_stop: bool,
}

struct FrameworkScenario {
    changed: bool,
    historical_responses: bool,
    behavior: FrameworkBehavior,
}

impl FrameworkScenario {
    fn managed_market_exit() -> Self {
        Self {
            changed: false,
            historical_responses: false,
            behavior: FrameworkBehavior {
                manage_stop: true,
                completion: TimerCompletion {
                    market_exit: true,
                    gtd: false,
                },
                fail_historical_stop: false,
            },
        }
    }
}

#[derive(Debug)]
struct FrameworkStrategy {
    core: StrategyCore,
    indicator: Rc<CountingIndicator>,
    callbacks: [u64; 3],
    denied: u64,
    stopped: u64,
    created_gtd: u64,
    post_market_exits: u64,
    time_events: u64,
    completion: TimerCompletion,
    gtd_order_events: Vec<OrderEventAny>,
    fail_historical_stop: bool,
    historical_clock_fault: String,
    bindings: bool,
    cancelled_timer_backlog: Option<CancelledTimerBacklog>,
}
impl FrameworkStrategy {
    fn with_timer_completion(behavior: FrameworkBehavior) -> Self {
        let FrameworkBehavior {
            manage_stop,
            completion,
            fail_historical_stop,
        } = behavior;
        let mut value = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                manage_gtd_expiry: true,
                manage_contingent_orders: true,
                manage_stop,
                // The complete case has real outstanding source exposure until
                // its original terminal receipt; the original timer must expire.
                market_exit_interval_ms: if completion.market_exit {
                    100
                } else {
                    3_600_000
                },
                ..Default::default()
            }),
            indicator: Rc::new(CountingIndicator {
                counts: Cell::new([2, 3, 4]),
            }),
            callbacks: [2, 3, 4],
            denied: 7,
            stopped: 0,
            created_gtd: 0,
            post_market_exits: 0,
            time_events: 0,
            completion,
            gtd_order_events: Vec::new(),
            fail_historical_stop,
            historical_clock_fault: String::new(),
            bindings: false,
            cancelled_timer_backlog: None,
        };
        let indicator = value.indicator.clone();
        value
            .core_mut()
            .register_indicator_for_quote_ticks(instrument_id(), indicator.clone());
        value
            .core_mut()
            .register_indicator_for_trade_ticks(instrument_id(), indicator.clone());
        value
            .core_mut()
            .register_indicator_for_bars(bar_type(), indicator);
        value
    }
    fn bind_original_handlers(&mut self) {
        assert!(!self.bindings, "local original bindings installed twice");
        let id = self.actor_id().inner();
        msgbus::subscribe_quotes(
            switchboard::get_quotes_topic(instrument_id()).into(),
            TypedHandler::from(move |q: &QuoteTick| {
                get_actor_unchecked::<Self>(&id).handle_quote(q);
            }),
            None,
        );
        msgbus::subscribe_trades(
            switchboard::get_trades_topic(instrument_id()).into(),
            TypedHandler::from(move |t: &TradeTick| {
                get_actor_unchecked::<Self>(&id).handle_trade(t);
            }),
            None,
        );
        msgbus::subscribe_bars(
            switchboard::get_bars_topic(bar_type()).into(),
            TypedHandler::from(move |b: &Bar| get_actor_unchecked::<Self>(&id).handle_bar(b)),
            None,
        );
        self.bindings = true;
    }
    fn business_data(&mut self, index: usize) -> Result<()> {
        self.callbacks[index] += 1;
        ensure!(
            self.indicator.counts.get() == self.callbacks,
            "indicator prelude did not run exactly once"
        );
        Ok(())
    }
    fn snapshot(&self) -> Result<IndexMap<String, Vec<u8>>> {
        Ok(IndexMap::from([(
            "business.v1".into(),
            serde_json::to_vec(&serde_json::json!({
                "indicators":self.indicator.counts.get(), "callbacks":self.callbacks,
                "denied":self.denied,"stopped":self.stopped,"created_gtd":self.created_gtd,
                "post_market_exits":self.post_market_exits,"time_events":self.time_events,
                "gtd_order_events":self.gtd_order_events,
                "exiting":self.is_exiting(),
                "exit_timer_present":self.clock_rc().borrow().timer_names().iter().any(|n| n==&format!("MARKET_EXIT_CHECK:{STRATEGY}")),
                "gtd_clock_present":self.clock_rc().borrow().timer_names().iter().any(|n| n==&format!("GTD-EXPIRY:{ORDER}")),
            }))?,
        )]))
    }
}
impl DataActor for FrameworkStrategy {
    fn on_start(&mut self) -> Result<()> {
        self.bind_original_handlers();
        Ok(())
    }
    fn on_stop(&mut self) -> Result<()> {
        ensure!(
            !self.is_exiting(),
            "business stop ran before actual strategy stop cleanup"
        );
        ensure!(
            !self
                .clock_rc()
                .borrow()
                .timer_names()
                .iter()
                .any(|n| n == &format!("MARKET_EXIT_CHECK:{STRATEGY}")),
            "business stop retained original market-exit timer"
        );
        self.stopped += 1;
        if self.fail_historical_stop {
            anyhow::bail!("original managed business stop callback failed");
        }
        Ok(())
    }
    fn on_save(&self) -> Result<IndexMap<String, Vec<u8>>> {
        self.snapshot()
    }
    fn on_load(&mut self, state: IndexMap<String, Vec<u8>>) -> Result<()> {
        let value: serde_json::Value = serde_json::from_slice(&state["business.v1"])?;
        self.indicator
            .counts
            .set(serde_json::from_value(value["indicators"].clone())?);
        self.callbacks = serde_json::from_value(value["callbacks"].clone())?;
        self.denied = value["denied"].as_u64().context("denied missing")?;
        self.stopped = value["stopped"].as_u64().context("stopped missing")?;
        self.created_gtd = value["created_gtd"].as_u64().context("created missing")?;
        self.post_market_exits = value["post_market_exits"]
            .as_u64()
            .context("post exit missing")?;
        self.time_events = value["time_events"]
            .as_u64()
            .context("time events missing")?;
        self.gtd_order_events = serde_json::from_value(value["gtd_order_events"].clone())?;
        ensure!(
            value["exiting"] == false && value["exit_timer_present"] == false,
            "fixture initial cut already has a market exit"
        );
        ensure!(
            value["gtd_clock_present"] == false,
            "initial fixture cannot inherit an uninstalled GTD map"
        );
        Ok(())
    }
    fn on_quote(&mut self, _: &QuoteTick) -> Result<()> {
        self.business_data(0)?;
        if self.completion.gtd {
            let now = self.clock_rc().borrow().timestamp_ns();
            let order = self.order_factory().limit(
                instrument_id(),
                OrderSide::Buy,
                Quantity::from("1.000"),
                Price::from("1000.00"),
                Some(TimeInForce::Gtd),
                Some(UnixNanos::from(now.as_u64() + 1_000_000_000)),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some(ClientOrderId::from(ORDER)),
            );
            // This is the original factory's actual initialized order. Later
            // Submitted and Accepted traverse the real native execution FIFO.
            self.cache_rc()
                .borrow_mut()
                .add_order(order, None, None, false)?;
            return Ok(());
        }
        if self.completion.market_exit {
            self.market_exit()?;
            ensure!(
                !Strategy::stop(self),
                "original managed stop did not defer to its real timer"
            );
            return Ok(());
        }
        let order = self
            .cache_ref()
            .order(&ClientOrderId::from(ORDER))
            .context("original GTD order absent")?
            .clone();
        self.set_gtd_expiry(&order)?;
        ensure!(
            self.has_gtd_expiry_timer(&ClientOrderId::from(ORDER)),
            "GTD framework timer was not created"
        );
        self.created_gtd += 1;
        self.market_exit()?;
        ensure!(
            self.is_exiting(),
            "original default market exit did not preserve Running admission"
        );
        Ok(())
    }
    fn on_time_event(&mut self, event: &nautilus_common::timer::TimeEvent) -> Result<()> {
        if self.completion.gtd {
            ensure!(
                event.name.as_str() == format!("GTD-EXPIRY:{ORDER}"),
                "wrong actual GTD timer owner"
            );
            ensure!(
                !self.has_gtd_expiry_timer(&ClientOrderId::from(ORDER)),
                "original GTD framework did not clear its map before business callback"
            );
            let cache = self.cache_ref();
            let order = cache
                .order(&ClientOrderId::from(ORDER))
                .context("actual GTD order missing")?;
            ensure!(
                order.status() == OrderStatus::PendingCancel
                    && self
                        .gtd_order_events
                        .iter()
                        .any(|event| matches!(event, OrderEventAny::PendingCancel(_))),
                "original expiry did not prepare its real pending cancellation first"
            );
            drop(order);
            drop(cache);
            self.time_events += 1;
            return Ok(());
        }
        ensure!(
            self.completion.market_exit
                && event.name.as_str() == format!("MARKET_EXIT_CHECK:{STRATEGY}"),
            "unexpected actual framework timer callback"
        );
        if self.stopped == 0 {
            ensure!(
                self.post_market_exits == 0 && self.is_exiting(),
                "unsettled managed exit lost its original pending supervision"
            );
        } else {
            ensure!(
                self.post_market_exits == 1 && self.stopped == 1 && !self.is_exiting(),
                "managed framework did not finish before original business timer callback"
            );
        }
        self.time_events += 1;
        Ok(())
    }
    fn on_trade(&mut self, _: &TradeTick) -> Result<()> {
        self.business_data(1)
    }
    fn on_bar(&mut self, _: &Bar) -> Result<()> {
        self.business_data(2)
    }
    fn prepare_native_recovery(
        &mut self,
        boundary: &HistoricalReplayPreparation<'_>,
    ) -> Result<()> {
        ensure!(
            boundary
                .verified_source::<nautilus_event_store::native_trace::VerifiedNativeTrace>()
                .is_some(),
            "unverified preparation"
        );
        self.bind_original_handlers();
        Ok(())
    }
    fn on_native_recovery_state(&self) -> Result<IndexMap<String, Vec<u8>>> {
        self.snapshot()
    }
    fn on_native_recovery_input(
        &mut self,
        boundary: &HistoricalInputBoundary<'_>,
        input: &dyn Any,
    ) -> Result<()> {
        ensure!(
            boundary.verified_source::<VerifiedNativeRoot>().is_some(),
            "unverified original root"
        );
        ensure!(
            self.state() == ComponentState::Ready,
            "historical business callback opened physical lifecycle"
        );
        if let Some(q) = input.downcast_ref::<QuoteTick>() {
            if self.historical_clock_fault == "foreign_owner" {
                // A real different clock cannot borrow this registered owner's
                // original read tape, even when its returned type is identical.
                let foreign = nautilus_common::live::clock::LiveClock::default();
                let _ = nautilus_common::clock::Clock::timestamp_ns(&foreign);
            }
            self.on_quote(q)?;
            if self.historical_clock_fault == "missing_read" {
                // The actual original creation consumed its exact start draw.
                // A further unrecorded real owner read has no target-now fallback.
                let _ = self.clock_rc().borrow().timestamp_ns();
            }
            Ok(())
        } else if let Some(t) = input.downcast_ref::<TradeTick>() {
            self.on_trade(t)
        } else if let Some(b) = input.downcast_ref::<Bar>() {
            self.on_bar(b)
        } else if let Some(event) = input.downcast_ref::<nautilus_common::timer::TimeEvent>() {
            DataActor::on_time_event(self, event)
        } else if let Some(event) = input.downcast_ref::<OrderEventAny>() {
            self.on_order_event(event.clone());
            Ok(())
        } else if let Some(event) = input.downcast_ref::<NativeComponentLifecycle>() {
            match event.action.as_str() {
                "stop" => DataActor::on_stop(self),
                _ => anyhow::bail!("unknown original lifecycle"),
            }
        } else {
            anyhow::bail!("unsupported original framework business input")
        }
    }
}
nautilus_strategy!(FrameworkStrategy, {
    fn post_market_exit(&mut self) {
        self.post_market_exits += 1;
    }
    fn on_order_event(&mut self, event: OrderEventAny) {
        if self.completion.market_exit {
            // market_exit prepares PendingCancel synchronously before its
            // queued CancelOrder. Each business receipt must occur once, in
            // its real source order; a duplicate or unrelated event is fatal.
            assert!(matches!(
                (self.gtd_order_events.len(), &event),
                (0, OrderEventAny::Submitted(_))
                    | (1, OrderEventAny::Accepted(_))
                    | (2, OrderEventAny::PendingCancel(_))
                    | (3, OrderEventAny::Canceled(_))
            ));
            assert_eq!(event.client_order_id(), ClientOrderId::from(ORDER));
            assert_eq!(event.instrument_id(), instrument_id());
            assert_eq!(event.strategy_id(), StrategyId::from(STRATEGY));
            if matches!(event, OrderEventAny::Canceled(_))
                && let Some(backlog) = self.cancelled_timer_backlog.take()
            {
                // The actual terminal handler owns this point before another
                // timer can dispatch; a later Notify wake can observe Stopped.
                assert_eq!(self.state(), ComponentState::Running);
                assert_eq!(
                    self.cache_ref()
                        .order(&ClientOrderId::from(ORDER))
                        .unwrap()
                        .status(),
                    OrderStatus::Canceled,
                );
                let mut publications = backlog.sender.publications.lock().unwrap();
                let already_published = publications.len();
                while publications.len() < already_published + 2 {
                    let remaining = Duration::from_secs(20)
                        .checked_sub(backlog.source_started.elapsed())
                        .expect("actual timer backlog exceeded original Source budget");
                    let (next, waited) = backlog
                        .sender
                        .arrived
                        .wait_timeout(publications, remaining)
                        .unwrap();
                    publications = next;
                    assert!(
                        !waited.timed_out() || publications.len() >= already_published + 2,
                        "actual native timer publication did not reach the original Source budget"
                    );
                }
                backlog
                    .stopped_timer_ids
                    .borrow_mut()
                    .extend_from_slice(&publications[already_published + 1..]);
            }
            self.gtd_order_events.push(event);
            return;
        }
        if self.completion.gtd {
            if matches!(event, OrderEventAny::Accepted(_)) {
                let order = self
                    .cache_ref()
                    .order(&ClientOrderId::from(ORDER))
                    .expect("original accepted GTD order absent")
                    .clone();
                self.set_gtd_expiry(&order)
                    .expect("actual default GTD timer not created");
                self.created_gtd += 1;
            }
            self.gtd_order_events.push(event);
            return;
        }
        assert!(matches!(event, OrderEventAny::Denied(_)));
        assert!(
            !self.has_gtd_expiry_timer(&ClientOrderId::from(ORDER)),
            "terminal framework did not clear GTD before business callback"
        );
        assert!(
            !self
                .clock_rc()
                .borrow()
                .timer_names()
                .iter()
                .any(|n| n == &format!("GTD-EXPIRY:{ORDER}"))
        );
        self.denied += 1;
    }
});

pub(super) fn data_payload(value: &DataEvent) -> Result<serde_json::Value> {
    match value {
        DataEvent::Data(Data::Quote(v)) => Ok(serde_json::json!({"Quote":v})),
        DataEvent::Data(Data::Trade(v)) => Ok(serde_json::json!({"Trade":v})),
        DataEvent::Data(Data::Bar(v)) => Ok(serde_json::json!({"Bar":v})),
        _ => anyhow::bail!("unsupported framework data input"),
    }
}
fn decode_data(value: &serde_json::Value) -> Result<DataEvent> {
    if let Some(v) = value.get("Quote") {
        Ok(DataEvent::Data(Data::Quote(serde_json::from_value(
            v.clone(),
        )?)))
    } else if let Some(v) = value.get("Trade") {
        Ok(DataEvent::Data(Data::Trade(serde_json::from_value(
            v.clone(),
        )?)))
    } else if let Some(v) = value.get("Bar") {
        Ok(DataEvent::Data(Data::Bar(serde_json::from_value(
            v.clone(),
        )?)))
    } else {
        anyhow::bail!("unknown data payload")
    }
}
#[derive(Debug)]
struct FrameworkCodec(RunnerRecoveryChannel);
impl RunnerRecoveryCodec for FrameworkCodec {
    fn channel(&self) -> RunnerRecoveryChannel {
        self.0
    }
    fn codec_id(&self) -> &'static str {
        "actual_registered_framework_tail.v1"
    }
    fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
        match event {
            RunnerRecoveryEventRef::DataEvent(v) => data_payload(v),
            RunnerRecoveryEventRef::ExecutionCommand(message) => Ok(serde_json::json!({
                "endpoint":message.endpoint().to_string(), "command":message.command()
            })),
            RunnerRecoveryEventRef::TimeEvent(message) => {
                let actual = message.checkpoint_callback_binding();
                ensure!(
                    actual["owner_thread_matches"].as_bool() == Some(true)
                        && matches!(
                            actual["kind"].as_str(),
                            Some("registered_owner_thread" | "registered_cleanup")
                        )
                        && actual["binding_id"].as_u64().is_some_and(|id| id > 0),
                    "native framework timer has no actual owner-bound lease"
                );
                let event = message.event();
                Ok(
                    serde_json::json!({"name":event.name.to_string(),"event_id":event.event_id,
                    "ts_event":event.ts_event,"ts_init":event.ts_init,
                    "callback":message.native_input_callback_binding()}),
                )
            }
            RunnerRecoveryEventRef::ExecutionEvent(ExecutionEvent::Order(v)) => {
                Ok(serde_json::to_value(v)?)
            }
            _ => anyhow::bail!("unknown framework queue member"),
        }
    }
    fn decode(&self, input: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
        match self.0 {
            RunnerRecoveryChannel::DataEvent => {
                Ok(RunnerRecoveryEvent::DataEvent(decode_data(&input.payload)?))
            }
            RunnerRecoveryChannel::ExecutionEvent => Ok(RunnerRecoveryEvent::ExecutionEvent(
                ExecutionEvent::Order(serde_json::from_value(input.payload.clone())?),
            )),
            RunnerRecoveryChannel::ExecutionCommand => Ok(RunnerRecoveryEvent::ExecutionCommand(
                TradingCommandMessage::new(
                    input.payload["endpoint"]
                        .as_str()
                        .context("command endpoint missing")?
                        .into(),
                    serde_json::from_value(input.payload["command"].clone())?,
                ),
            )),
            _ => anyhow::bail!("unknown framework queue channel"),
        }
    }
}
fn registry() -> Rc<RunnerRecoveryCodecRegistry> {
    let channels = [
        RunnerRecoveryChannel::DataEvent,
        RunnerRecoveryChannel::ExecutionEvent,
        RunnerRecoveryChannel::ExecutionCommand,
        RunnerRecoveryChannel::TimeEvent,
    ];
    let mut value = RunnerRecoveryCodecRegistry::new(channels);
    for channel in channels {
        if channel == RunnerRecoveryChannel::TimeEvent {
            value
                .register_owner_bound_timer_codec(FrameworkCodec(channel))
                .unwrap();
        } else {
            value.register(FrameworkCodec(channel)).unwrap();
        }
    }
    Rc::new(value.seal().unwrap())
}
fn install(
    node: &mut LiveNode,
    order: &OrderAny,
    source: bool,
    behavior: FrameworkBehavior,
    historical_clock_fault: &str,
) {
    let complete_gtd = behavior.completion.gtd;
    let mut strategy = FrameworkStrategy::with_timer_completion(behavior);
    if !source {
        strategy.historical_clock_fault = historical_clock_fault.into();
    }
    node.add_strategy(strategy).unwrap();
    if source {
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
            .unwrap();
        if !complete_gtd {
            node.kernel
                .cache
                .borrow_mut()
                .add_order(order.clone(), None, None, false)
                .unwrap();
        }
    }
}

// Observe the actual registered LiveTimer publication only after its original
// native sender has accepted it. The owner runloop can then wait for a real
// admitted backlog; no message, lease or Ingress receipt is held outside it.
#[derive(Debug)]
struct QueuedMarketExitTimerSender {
    original: std::sync::Arc<dyn nautilus_common::runner::TimeEventSender>,
    publications: std::sync::Mutex<Vec<UUID4>>,
    arrived: std::sync::Condvar,
}
#[derive(Debug)]
struct CancelledTimerBacklog {
    sender: std::sync::Arc<QueuedMarketExitTimerSender>,
    source_started: std::time::Instant,
    stopped_timer_ids: Rc<RefCell<Vec<UUID4>>>,
}
impl nautilus_common::runner::TimeEventSender for QueuedMarketExitTimerSender {
    fn invalidate_snapshot(&self) {
        self.original.invalidate_snapshot();
    }
    fn send(&self, message: nautilus_common::runner::TimeEventMessage) {
        if message.event().name.as_str() == format!("MARKET_EXIT_CHECK:{STRATEGY}") {
            let event_id = message.event().event_id;
            self.original.send(message);
            self.publications.lock().unwrap().push(event_id);
            self.arrived.notify_all();
        } else {
            self.original.send(message);
        }
    }
}

#[rstest]
#[case(false, false, false, false, false, false, "")]
#[case(true, false, false, false, false, false, "")]
#[case(false, true, false, false, false, false, "")]
#[case(true, true, false, false, false, false, "")]
#[case(false, false, true, false, false, false, "")]
#[case(true, false, true, false, false, false, "")]
#[case(false, false, true, true, false, false, "")]
#[case(true, false, true, true, false, false, "")]
#[case(false, false, true, true, true, false, "")]
#[case(false, false, false, false, false, true, "")]
#[case(true, false, false, false, false, true, "")]
#[case(false, false, true, true, false, false, "missing_read")]
#[case(false, false, true, true, false, false, "foreign_owner")]
#[tokio::test(flavor = "current_thread")]
async fn actual_registered_native_tail_framework_effects_and_changed_input(
    #[case] changed: bool,
    #[case] historical_responses: bool,
    #[case] manage_stop: bool,
    #[case] complete_market_exit: bool,
    #[case] fail_historical_stop: bool,
    #[case] complete_gtd: bool,
    #[case] historical_clock_fault: &str,
) {
    Box::pin(run_actual_registered_framework_tail(
        FrameworkScenario {
            changed,
            historical_responses,
            behavior: FrameworkBehavior {
                manage_stop,
                completion: TimerCompletion {
                    market_exit: complete_market_exit,
                    gtd: complete_gtd,
                },
                fail_historical_stop,
            },
        },
        (historical_clock_fault, false),
    ))
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_cancelled_queued_default_timer_after_stop_keeps_original_reader_route() {
    Box::pin(run_actual_registered_framework_tail(
        FrameworkScenario::managed_market_exit(),
        ("", true),
    ))
    .await;
}

// These are original Reader inputs and their actual native Ingress receipts,
// not elapsed-time estimates of how many ticks should have fired.
fn assert_original_market_timer_admission_boundary(trace: &VerifiedNativeTrace) {
    assert!(
        trace.cut().registered_timers.values().all(|inventory| {
            inventory["timers"]
                .as_array()
                .unwrap()
                .iter()
                .all(|timer| timer["name"] != format!("MARKET_EXIT_CHECK:{STRATEGY}"))
        }),
        "the market exit timer must be created after this original cut"
    );
    let mut original = Vec::new();
    for root in trace.roots() {
        for input in root.inputs() {
            let NativeTraceRecord::Begin {
                input_source: NativeInputSource::Time,
                input_sequence,
                receipt,
                read_witnesses,
                ..
            } = input
            else {
                continue;
            };
            let witness = read_witnesses
                .iter()
                .find(|witness| witness.component_id == "native:timers")
                .unwrap();
            let timer = &witness.payload["input"];
            if timer["event"]["name"] != format!("MARKET_EXIT_CHECK:{STRATEGY}") {
                continue;
            }
            original.push((
                timer["owner"].as_str().unwrap().to_owned(),
                timer["binding"]["binding_id"].as_u64().unwrap(),
                *input_sequence,
                receipt.ingress.as_ref().unwrap().accepted_wall_ns,
                timer["event"]["event_id"].as_str().unwrap().to_owned(),
            ));
        }
    }
    assert!(
        !original.is_empty(),
        "Source never admitted its real market exit timer"
    );
    let mut seen = std::collections::HashSet::new();
    assert!(original.iter().all(|(_, _, _, _, id)| seen.insert(id)));
    let mut checked = 0;
    for root in trace.roots() {
        for input in root.inputs() {
            let NativeTraceRecord::Begin {
                input_sequence,
                read_witnesses,
                ..
            } = input
            else {
                continue;
            };
            let witness = read_witnesses
                .iter()
                .find(|witness| witness.component_id == "native:timers")
                .unwrap();
            let captured_at_ns = witness.payload["captured_at_ns"].as_u64().unwrap();
            for (owner, inventory) in witness.payload["inventory"].as_object().unwrap() {
                for timer in inventory["timers"].as_array().unwrap() {
                    if timer["name"] != format!("MARKET_EXIT_CHECK:{STRATEGY}") {
                        continue;
                    }
                    let binding = timer["binding"]["binding_id"].as_u64().unwrap();
                    let accepted = original
                        .iter()
                        .filter(|(o, b, _, at, _)| {
                            o == owner && *b == binding && *at <= captured_at_ns
                        })
                        .count();
                    let previously_dispatched = original
                        .iter()
                        .filter(|(o, b, sequence, _, _)| {
                            o == owner && *b == binding && *sequence < *input_sequence
                        })
                        .count();
                    let expected = accepted.checked_sub(previously_dispatched).unwrap();
                    assert_eq!(
                        timer["binding"]["state"].as_u64().unwrap() & !(1u64 << 63),
                        expected as u64,
                        "original Begin timer snapshot and capture timestamp cross an actual admission: root={} input={} owner={owner} binding={binding}",
                        root.root_sequence(),
                        input_sequence,
                    );
                    checked += 1;
                }
            }
        }
    }
    assert!(
        checked > 0,
        "Source has no original live market exit snapshot"
    );
}

// A publisher observation used only to check the actual freeze. The original
// native Ingress receipts in the Reader caller below remain the admission proof.
#[derive(Debug)]
struct TimerSnapshotPublicationSender {
    delivered: std::sync::mpsc::Sender<(u64, nautilus_common::runner::TimeEventMessage)>,
}
impl nautilus_common::runner::TimeEventSender for TimerSnapshotPublicationSender {
    fn send(&self, message: nautilus_common::runner::TimeEventMessage) {
        let publisher_observed_ns = nautilus_core::time::nanos_since_unix_epoch();
        self.delivered
            .send((publisher_observed_ns, message))
            .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_registered_timer_witness_capture_keeps_original_admission_boundary() {
    use nautilus_common::clock::Clock;
    let (delivered, received) = std::sync::mpsc::channel();
    let mut clock = nautilus_common::live::clock::LiveClock::new(Some(std::sync::Arc::new(
        TimerSnapshotPublicationSender { delivered },
    )));
    let called = Rc::new(Cell::new(0));
    let original_called = called.clone();
    let due = clock.timestamp_ns() + nautilus_core::DurationNanos::from_millis(100);
    clock
        .set_time_alert_ns(
            "ORIGINAL_TIMER_CAPTURE_BOUNDARY",
            due,
            Some(nautilus_common::timer::TimeEventCallback::RustLocal(
                Rc::new(move |_| {
                    original_called.set(original_called.get() + 1);
                }),
            )),
            Some(false),
        )
        .unwrap();
    let frozen = clock.freeze_running_timer_checkpoint().unwrap();
    // Cross the real deadline while the same FrozenCallbacks producer gate is
    // retained. A queued publication cannot appear before its snapshot stamp.
    assert!(matches!(
        received.recv_timeout(Duration::from_millis(150)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    frozen.verify().unwrap();
    let original_inventory = frozen.inventory().clone();
    let captured_at_ns = nautilus_core::time::nanos_since_unix_epoch();
    assert_eq!(original_inventory["timers"][0]["binding"]["state"], 0);
    assert_eq!(
        original_inventory["timers"][0]["next_time_ns"],
        due.as_u64()
    );
    frozen.finish().unwrap();
    let (publisher_observed_ns, original_message) =
        received.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(publisher_observed_ns > captured_at_ns);
    assert_eq!(original_message.event().ts_event, due);
    assert_eq!(called.get(), 0);
    assert!(original_message.dispatch());
    assert_eq!(called.get(), 1);
    clock.cancel_timers();
    // Same original-object Source / sealed Reader / Target path as the managed
    // timer fixture, with independently checked Begin snapshot/Ingress counts.
    Box::pin(run_actual_registered_framework_tail(
        FrameworkScenario::managed_market_exit(),
        ("", false),
    ))
    .await;
}

async fn run_actual_registered_framework_tail(
    scenario: FrameworkScenario,
    historical: (&str, bool),
) {
    let FrameworkScenario {
        changed,
        historical_responses,
        behavior,
    } = scenario;
    let FrameworkBehavior {
        manage_stop,
        completion:
            TimerCompletion {
                market_exit: complete_market_exit,
                gtd: complete_gtd,
            },
        fail_historical_stop,
    } = behavior;
    let (historical_clock_fault, cancelled_queued_after_stop) = historical;
    let directory = std::path::PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").unwrap())
        .join(format!("framework-tail-{}", UUID4::new()));
    std::fs::create_dir_all(&directory).unwrap();
    let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let order = OrderTestBuilder::new(OrderType::Limit)
        .trader_id(TraderId::from("NATIVE-TAIL-001"))
        .strategy_id(StrategyId::from(STRATEGY))
        .instrument_id(instrument_id())
        .client_order_id(ClientOrderId::from(ORDER))
        .quantity(Quantity::from(1))
        .price(Price::from("1000.00"))
        .time_in_force(TimeInForce::Gtd)
        .expire_time(UnixNanos::from(now.as_u64() + 60_000_000_000))
        .build();
    let mut source = actual_node("framework-source", directory.join("source"), None);
    // The real runner is bound now; the strategy's fresh component LiveClock
    // is constructed by install/add_strategy below and captures this sender.
    // The existing kernel clock is left unchanged.
    let held_timer = cancelled_queued_after_stop.then(|| {
        let held = std::sync::Arc::new(QueuedMarketExitTimerSender {
            original: nautilus_common::runner::get_time_event_sender(),
            publications: std::sync::Mutex::default(),
            arrived: std::sync::Condvar::new(),
        });
        nautilus_common::runner::replace_time_event_sender(held.clone());
        held
    });
    install(
        &mut source,
        &order,
        true,
        FrameworkBehavior {
            fail_historical_stop: false,
            ..behavior
        },
        "",
    );
    if historical_responses {
        // Actual response processing, not a cache DTO: production warmup fills
        // the shared DataEngine cache before the first completed-root cut.
        let received = Rc::new(Cell::new(0));
        for bars in [true, false] {
            let correlation_id = UUID4::new();
            let count = received.clone();
            let handler = if bars {
                ShareableMessageHandler::from_typed(move |_: &BarsResponse| {
                    count.set(count.get() + 1);
                })
            } else {
                ShareableMessageHandler::from_typed(move |_: &QuotesResponse| {
                    count.set(count.get() + 1);
                })
            };
            msgbus::get_message_bus()
                .borrow_mut()
                .register_response_handler(&correlation_id, handler)
                .unwrap();
            let time = UnixNanos::from(now.as_u64() - 1_000_000);
            let response = if bars {
                let mut first = Bar::default();
                first.bar_type = bar_type();
                first.open = Price::from("1000.00");
                first.high = first.open;
                first.low = first.open;
                first.close = first.open;
                first.volume = Quantity::from(1);
                first.ts_event = time;
                first.ts_init = time;
                let mut second = first;
                second.close = Price::from("1001.00");
                second.open = second.close;
                second.high = second.close;
                second.low = second.close;
                // Official time-bar history replaces equal-timestamp revisions.
                // This restore fixture requires two actual historical members.
                second.ts_event = UnixNanos::from(time.as_u64() + 1);
                second.ts_init = second.ts_event;
                DataResponse::Bars(BarsResponse::new(
                    correlation_id,
                    ClientId::from("BINANCE"),
                    bar_type(),
                    vec![first, second],
                    None,
                    None,
                    now,
                    None,
                ))
            } else {
                let mut first = nautilus_model::data::stubs::quote_ethusdt_binance();
                first.bid_price = Price::from("1000.00");
                first.ask_price = Price::from("1001.00");
                first.ts_event = time;
                first.ts_init = time;
                let mut second = first;
                second.bid_price = Price::from("1002.00");
                second.ask_price = Price::from("1003.00");
                DataResponse::Quotes(QuotesResponse::new(
                    correlation_id,
                    ClientId::from("BINANCE"),
                    instrument_id(),
                    vec![first, second],
                    None,
                    None,
                    now,
                    None,
                ))
            };
            source.kernel.data_engine.borrow_mut().response(response);
        }
        assert_eq!(received.get(), 2);
        assert_eq!(
            source
                .kernel
                .cache
                .borrow()
                .bars(&bar_type())
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            source
                .kernel
                .cache
                .borrow()
                .quotes(&instrument_id())
                .unwrap()
                .len(),
            2
        );
    }
    let instance = source.kernel.instance_id();
    let trace = source
        .prepare_owned_native_trace(
            "framework-business-run".into(),
            nautilus_event_store::native_trace::native_inventory_digest(
                &serde_json::to_value(&source.config).unwrap(),
            )
            .unwrap(),
            "actual_registered_framework_tail.v1".into(),
            "actual_nonempty_strategy_framework.v1".into(),
            |_, _, _| Ok(Vec::new()),
            |_| Ok(()),
        )
        .unwrap();
    let cuts = Rc::new(RefCell::new(Vec::new()));
    let ready = Rc::new(tokio::sync::Notify::new());
    let progress = Rc::new(tokio::sync::Notify::new());
    let saved = cuts.clone();
    let started = ready.clone();
    let advanced = progress.clone();
    source
        .set_running_checkpoint_handler(
            registry(),
            RunningCheckpointSchedule::EveryCompletedRoot,
            move |boundary| {
                boundary.verify()?;
                let cut = boundary
                    .native_trace_cut()
                    .context("actual trace absent")?
                    .clone();
                if saved.borrow().is_empty() {
                    let ancestry = read_callback_ancestry(&cut, &cut.pending_inputs)?
                        .context("new producer omitted its explicit callback ancestry sidecar")?;
                    assert_eq!(&ancestry, boundary.inventory().callback_ancestry());
                    let mut missing = cut.clone();
                    missing
                        .native_effects
                        .as_object_mut()
                        .unwrap()
                        .remove("callback_ancestry");
                    assert!(read_callback_ancestry(&missing, &missing.pending_inputs).is_err());
                    let mut changed = cut.clone();
                    changed.native_effects["callback_ancestry_profile"] =
                        "unsupported-profile".into();
                    assert!(read_callback_ancestry(&changed, &changed.pending_inputs).is_err());
                    let mut changed = cut.clone();
                    changed.native_effects["callback_ancestry"]["state"]["entry_chains"] =
                        serde_json::json!([null]);
                    assert!(read_callback_ancestry(&changed, &changed.pending_inputs).is_err());
                }
                boundary.persist_native_checkpoint(
                    serde_json::json!({"actual_registered_framework":true}),
                )?;
                saved.borrow_mut().push((
                    cut,
                    serde_json::to_value(boundary.inventory())?,
                    boundary.components().clone(),
                ));
                if saved.borrow().len() == 1 {
                    started.notify_one();
                } else {
                    advanced.notify_one();
                }
                Ok(())
            },
            |()| Ok(()),
            |_| {},
        )
        .unwrap();
    let data = source.runner.as_ref().unwrap().data_event_sender_clone();
    let exec = source
        .runner
        .as_ref()
        .unwrap()
        .execution_event_sender_clone();
    let handle = source.handle();
    let mut quote = nautilus_model::data::stubs::quote_ethusdt_binance();
    quote.ts_event = now;
    quote.ts_init = now;
    let mut trade = nautilus_model::data::stubs::stub_trade_ethusdt_buy();
    trade.ts_event = now;
    trade.ts_init = now;
    let bar = Bar {
        bar_type: bar_type(),
        ts_event: now,
        ts_init: now,
        ..Bar::default()
    };
    let denied = OrderEventAny::Denied(OrderDenied::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        "actual local terminal".into(),
        UUID4::new(),
        now,
        now,
    ));
    let original_cuts = cuts.clone();
    let managed_order = order.clone();
    let stopped_timer_ids = Rc::new(RefCell::new(Vec::new()));
    let stopped_timers = stopped_timer_ids.clone();
    let source_started = std::time::Instant::now();
    if let Some(sender) = &held_timer {
        let mut original =
            get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
        original.cancelled_timer_backlog = Some(CancelledTimerBacklog {
            sender: sender.clone(),
            source_started,
            stopped_timer_ids: stopped_timers,
        });
    }
    let produced = async move {
        ready.notified().await;
        if complete_gtd {
            data.send(DataEvent::Data(Data::Quote(quote)).into())
                .unwrap();
            let initialized = loop {
                progress.notified().await;
                let actual =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                let initialized = actual.cache_ref().order_owned(&ClientOrderId::from(ORDER));
                if let Some(initialized) = initialized {
                    assert_eq!(
                        actual.callbacks[0], 3,
                        "original quote did not reach its factory callback"
                    );
                    break initialized;
                }
                // A maintenance completion/old Notify permit is not an ACK for
                // the actual Quote. Wait for its real cache mutation instead.
            };
            let received = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
            let account = AccountId::from("BINANCE-001");
            for event in [
                OrderEventAny::Submitted(OrderSubmitted::new(
                    initialized.trader_id(),
                    initialized.strategy_id(),
                    initialized.instrument_id(),
                    initialized.client_order_id(),
                    account,
                    UUID4::new(),
                    received,
                    received,
                )),
                OrderEventAny::Accepted(OrderAccepted::new(
                    initialized.trader_id(),
                    initialized.strategy_id(),
                    initialized.instrument_id(),
                    initialized.client_order_id(),
                    VenueOrderId::from("V-GTD-ACTUAL"),
                    account,
                    UUID4::new(),
                    received,
                    received,
                    false,
                )),
            ] {
                exec.send(ExecutionEvent::Order(event).into()).unwrap();
                progress.notified().await;
            }
            loop {
                progress.notified().await;
                let actual =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                let expired = actual.time_events == 1;
                drop(actual);
                let no_queued_command = original_cuts.borrow().last().is_some_and(|cut| {
                    !cut.0
                        .pending
                        .iter()
                        .any(|receipt| receipt.input_source == NativeInputSource::TradingCommand)
                });
                if expired && no_queued_command {
                    break;
                }
            }
            handle.stop();
            return;
        }
        if complete_market_exit {
            // The newer managed-stop contract completes immediately when flat.
            // Establish actual outstanding native order state through the real
            // Source execution FIFO, not a pending flag or manually changed cache.
            let received = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
            let account = AccountId::from("BINANCE-001");
            for (event, status) in [
                (
                    OrderEventAny::Submitted(OrderSubmitted::new(
                        managed_order.trader_id(),
                        managed_order.strategy_id(),
                        managed_order.instrument_id(),
                        managed_order.client_order_id(),
                        account,
                        UUID4::new(),
                        received,
                        received,
                    )),
                    OrderStatus::Submitted,
                ),
                (
                    OrderEventAny::Accepted(OrderAccepted::new(
                        managed_order.trader_id(),
                        managed_order.strategy_id(),
                        managed_order.instrument_id(),
                        managed_order.client_order_id(),
                        VenueOrderId::from("V-MANAGED-ACTUAL"),
                        account,
                        UUID4::new(),
                        received,
                        received,
                        false,
                    )),
                    OrderStatus::Accepted,
                ),
            ] {
                exec.send(ExecutionEvent::Order(event).into()).unwrap();
                loop {
                    progress.notified().await;
                    let actual = get_actor_unchecked::<FrameworkStrategy>(
                        &StrategyId::from(STRATEGY).inner(),
                    );
                    let applied = actual
                        .cache_ref()
                        .order(&ClientOrderId::from(ORDER))
                        .is_some_and(|order| order.status() == status);
                    if applied {
                        break;
                    }
                }
            }
            data.send(DataEvent::Data(Data::Quote(quote)).into())
                .unwrap();
            loop {
                progress.notified().await;
                let actual =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                let prepared = actual.callbacks[0] == 3 && actual.is_exiting();
                if prepared {
                    assert_eq!(actual.stopped, 0, "unsettled managed stop completed early");
                    assert_eq!(actual.post_market_exits, 0);
                    assert_eq!(actual.gtd_order_events.len(), 3);
                    assert!(matches!(
                        actual.gtd_order_events.last(),
                        Some(OrderEventAny::PendingCancel(_))
                    ));
                    assert_eq!(
                        actual
                            .cache_ref()
                            .order(&ClientOrderId::from(ORDER))
                            .unwrap()
                            .status(),
                        OrderStatus::PendingCancel
                    );
                }
                drop(actual);
                let command_drained = original_cuts.borrow().last().is_some_and(|cut| {
                    !cut.0
                        .pending
                        .iter()
                        .any(|receipt| receipt.input_source == NativeInputSource::TradingCommand)
                });
                if prepared && command_drained {
                    // Do not race the original acknowledgement ahead of the
                    // actual queued cancel. Its verified Begin/Complete and
                    // Quote causal receipt are checked again after sealing.
                    break;
                }
            }
            let received = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
            exec.send(
                ExecutionEvent::Order(OrderEventAny::Canceled(OrderCanceled::new(
                    managed_order.trader_id(),
                    managed_order.strategy_id(),
                    managed_order.instrument_id(),
                    managed_order.client_order_id(),
                    UUID4::new(),
                    received,
                    received,
                    false,
                    Some(VenueOrderId::from("V-MANAGED-ACTUAL")),
                    Some(account),
                    None,
                )))
                .into(),
            )
            .unwrap();
            let queued_before_dispatch =
                original_cuts.borrow().last().unwrap().0.last_input_sequence;
            loop {
                progress.notified().await;
                let value =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                let finished = value.stopped == 1 && value.time_events >= 1;
                drop(value);
                if finished {
                    break;
                }
            }
            if held_timer.is_some() {
                let original =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                assert_eq!(original.state(), ComponentState::Stopped);
                assert!(!original.is_exiting());
                assert!(
                    !original
                        .clock_rc()
                        .borrow()
                        .timer_names()
                        .iter()
                        .any(|name| { name == &format!("MARKET_EXIT_CHECK:{STRATEGY}") })
                );
                let business_time_events = original.time_events;
                drop(original);
                loop {
                    progress.notified().await;
                    let drained = original_cuts.borrow().last().is_some_and(|cut| {
                        cut.0.last_input_sequence > queued_before_dispatch
                            && !cut
                                .0
                                .pending
                                .iter()
                                .any(|pending| pending.input_source == NativeInputSource::Time)
                    });
                    if drained {
                        let original = get_actor_unchecked::<FrameworkStrategy>(
                            &StrategyId::from(STRATEGY).inner(),
                        );
                        assert_eq!(original.state(), ComponentState::Stopped);
                        assert_eq!(original.time_events, business_time_events);
                        break;
                    }
                }
            }
            handle.stop();
            return;
        }
        for (index, event) in [
            DataEvent::Data(Data::Quote(quote)),
            DataEvent::Data(Data::Trade(trade)),
            DataEvent::Data(Data::Bar(bar)),
        ]
        .into_iter()
        .enumerate()
        {
            data.send(event.into()).unwrap();
            loop {
                progress.notified().await;
                let actual =
                    get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
                // Maintenance also notifies this waiter. Only the original
                // business callback proves this input reached its real owner.
                if actual.callbacks[index] == [3, 4, 5][index] {
                    assert_eq!(actual.state(), ComponentState::Running);
                    break;
                }
            }
        }
        let original_denied = denied.clone();
        exec.send(ExecutionEvent::Order(denied).into()).unwrap();
        loop {
            progress.notified().await;
            let actual =
                get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
            let cache = actual.cache_ref();
            let applied = actual.denied == 8
                && cache
                    .order(&ClientOrderId::from(ORDER))
                    .is_some_and(|order| {
                        order.status() == OrderStatus::Denied
                            && order.last_event() == &original_denied
                    });
            if applied {
                // Stop admission only after this exact original terminal event
                // and its business callback completed while the owner ran.
                assert_eq!(actual.state(), ComponentState::Running);
                break;
            }
        }
        handle.stop();
    };
    let mut producer = std::pin::pin!(produced);
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut running = std::pin::pin!(source.run_with_mode(NodeRunMode::Hosted));
        tokio::select! { result=&mut running=>result, ()=&mut producer=>running.await }
    })
    .await
    .expect("actual registered source did not stop")
    .unwrap();
    let mut actual = get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
    assert_eq!(
        actual.indicator.counts.get(),
        if complete_market_exit || complete_gtd {
            [3, 3, 4]
        } else {
            [3, 4, 5]
        }
    );
    assert_eq!(
        actual.denied,
        if complete_market_exit || complete_gtd {
            7
        } else {
            8
        }
    );
    assert_eq!(actual.created_gtd, u64::from(!complete_market_exit));
    assert_eq!(actual.stopped, 1);
    assert_eq!(
        actual.post_market_exits,
        u64::from(complete_market_exit || manage_stop)
    );
    let original_business_order_events = actual.gtd_order_events.clone();
    let original_business_time_events = actual.time_events;
    if complete_market_exit {
        assert!(original_business_time_events >= 1);
        assert_managed_business_cache(&actual);
    } else {
        assert_eq!(original_business_time_events, u64::from(complete_gtd));
    }
    assert!(!actual.is_exiting());
    assert_eq!(actual.state(), ComponentState::Stopped);
    assert!(
        !actual
            .clock_rc()
            .borrow()
            .timer_names()
            .iter()
            .any(|n| n == &format!("MARKET_EXIT_CHECK:{STRATEGY}"))
    );
    if complete_gtd {
        assert_gtd_business_cache(&mut actual);
    }
    drop(actual);
    let identity = trace.source().unwrap();
    let first = cuts.borrow()[0].clone();
    if complete_market_exit {
        let observed = cuts.borrow();
        let exiting = observed
            .iter()
            .find(|cut| cut.0.native_effects["strategy_framework"][STRATEGY]["is_exiting"] == true)
            .expect("real market_exit never reached a completed native Core cut");
        let framework = &exiting.0.native_effects["strategy_framework"][STRATEGY];
        assert_eq!(framework["pending_stop"], true);
        assert_eq!(framework["market_exit_attempts"], 0);
        assert_eq!(
            framework["market_exit_timer_name"],
            format!("MARKET_EXIT_CHECK:{STRATEGY}")
        );
        assert_eq!(framework["market_exit_tag"], "MARKET_EXIT");
        assert_eq!(framework["order_manager"]["submit_order_commands"], 0);
        assert_eq!(framework["order_manager"]["oto_target_quantities"], 0);
    }
    if historical_responses {
        assert_eq!(
            first.0.native_effects["market_cache"][0]["quotes"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            first.0.native_effects["market_cache"][0]["bars"][0]["bars"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }
    source.dispose();
    drop(source);
    let reader = EventStoreReader::new(
        RedbBackend::open_sealed(
            directory.join("source"),
            &instance.to_string(),
            &identity.journal_run,
        )
        .unwrap(),
    );
    let end = reader.high_watermark().unwrap();
    let verified = reader.verify_native_tail(&identity, &first.0, end).unwrap();
    let actual_clock_ids = first
        .0
        .registered_timers
        .values()
        .map(|inventory| {
            serde_json::from_value::<UUID4>(inventory["native_clock_id"].clone()).unwrap()
        })
        .collect::<Vec<_>>();
    let timer_start_reads = verified
        .roots()
        .iter()
        .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
        .filter_map(|input| match input {
            NativeTraceRecord::Complete { clock_reads, .. } => Some(clock_reads),
            _ => None,
        })
        .flatten()
        .filter(|read| read.operation.starts_with("live_timer.start:"))
        .collect::<Vec<_>>();
    assert!(
        !timer_start_reads.is_empty(),
        "actual timer creation lacked its synchronous source clock draw"
    );
    for read in timer_start_reads {
        assert!(
            actual_clock_ids.contains(&read.clock_id),
            "timer start draw was not owned by the actual registered clock"
        );
        assert!(
            read.value.as_u64().is_some_and(|now| now > 0),
            "timer start draw lost its exact nanos type"
        );
    }
    if cancelled_queued_after_stop {
        let event_ids = stopped_timer_ids.borrow();
        assert!(!event_ids.is_empty(), "actual queued timer was not sent");
        for event_id in event_ids.iter().copied() {
            let matches: Vec<_> = verified
                .roots()
                .iter()
                .filter(|root| {
                    root.inputs().iter().any(|input| {
                        matches!(input,
                NativeTraceRecord::Begin { input_source: NativeInputSource::Time, payload, .. }
                    if payload["event_id"] == serde_json::to_value(event_id).unwrap())
                    })
                })
                .collect();
            assert_eq!(matches.len(), 1);
            let original = matches[0]
                .inputs()
                .iter()
                .find_map(|input| {
                    if let NativeTraceRecord::Complete {
                        callbacks,
                        transports,
                        uuid_draws,
                        output_count,
                        queued_outputs,
                        clock_reads,
                        ..
                    } = input
                    {
                        Some((
                            callbacks,
                            transports,
                            uuid_draws,
                            output_count,
                            queued_outputs,
                            clock_reads,
                        ))
                    } else {
                        None
                    }
                })
                .unwrap();
            assert_eq!(original.0.len(), 1);
            assert_eq!(original.0[0].component_id, STRATEGY);
            assert_eq!(original.0[0].kind, "strategy.handle_time_event");
            assert_eq!(original.0[0].component_state, "Stopped");
            assert!(original.1.is_empty());
            assert!(original.2.is_empty());
            // The source retains the actual initial TimeEvent bus capture. No
            // business/default-framework output was executed after physical stop.
            assert_eq!(*original.3, 1);
            assert!(original.4.is_empty());
            assert_eq!(original.5.len(), 1);
            assert_eq!(original.5[0].operation, "timestamp_ns");
            assert_eq!(
                serde_json::to_value(original.5[0].clock_id).unwrap(),
                first.0.registered_timers["kernel"]["native_clock_id"]
            );
        }
    }
    if complete_market_exit {
        assert_original_market_timer_admission_boundary(&verified);
    }
    if complete_gtd {
        assert_original_gtd_timer_tail(&verified);
    } else if !complete_market_exit {
        assert!(verified.roots().iter().any(|r| r.inputs().iter().any(|i| matches!(i,NativeTraceRecord::Complete {callbacks,..} if callbacks.iter().any(|c| c.kind=="handle_order_event")))));
    } else {
        assert_original_managed_cancel_tail(&verified, &original_business_order_events);
        let original = verified
            .roots()
            .iter()
            .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
            .find_map(|input| {
                let NativeTraceRecord::Complete { callbacks, .. } = input else {
                    return None;
                };
                callbacks
                    .iter()
                    .any(|route| route.kind == "lifecycle.finalize_market_exit.default")
                    .then_some(callbacks)
            })
            .expect("actual original timer did not complete managed exit");
        assert_eq!(
            original
                .iter()
                .filter(|route| route.component_id == STRATEGY)
                .map(|route| route.kind.as_str())
                .collect::<Vec<_>>(),
            vec![
                "strategy.handle_time_event",
                "strategy.check_market_exit.default",
                "lifecycle.finalize_market_exit.default",
                "strategy.cancel_market_exit.default",
                "lifecycle.stop"
            ]
        );
        let timer_begins = verified
            .roots()
            .iter()
            .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
            .filter(|input| {
                matches!(
                    input,
                    NativeTraceRecord::Begin {
                        input_source: NativeInputSource::Time,
                        ..
                    }
                )
            })
            .count();
        assert!(
            timer_begins >= 1,
            "source never dispatched its actual timer owner"
        );
    }
    let lifecycle = verified
        .roots()
        .iter()
        .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
        .find_map(|input| {
            if let NativeTraceRecord::Complete { callbacks, .. } = input {
                callbacks
                    .iter()
                    .any(|route| route.kind == "lifecycle.strategy_stop")
                    .then_some(callbacks)
            } else {
                None
            }
        })
        .expect("original strategy stop routes absent");
    let owned_routes: Vec<_> = lifecycle
        .iter()
        .filter(|route| route.component_id == STRATEGY)
        .map(|route| route.kind.as_str())
        .collect();
    let expected = if manage_stop && complete_market_exit {
        vec!["lifecycle.strategy_stop", "lifecycle.strategy_stop.default"]
    } else if complete_gtd {
        vec![
            "lifecycle.strategy_stop",
            "lifecycle.strategy_stop.default",
            "lifecycle.stop",
        ]
    } else {
        vec![
            "lifecycle.strategy_stop",
            "lifecycle.strategy_stop.default",
            "strategy.cancel_market_exit.default",
            "lifecycle.stop",
        ]
    };
    assert_eq!(
        owned_routes, expected,
        "actual Source stop framework/business routes changed"
    );
    let fingerprint = EventStoreLifecycle::sealed_run_fingerprint(
        &directory.join("source"),
        instance,
        &identity.journal_run,
        end,
    )
    .unwrap();
    let mut target = actual_node(
        "framework-target",
        directory.join("target"),
        Some((
            directory.join("source"),
            instance,
            identity.journal_run,
            end,
            fingerprint,
        )),
    );
    let target_process_gate = nautilus_core::transmission::TransmissionGate::default();
    target
        .handle()
        .bind_transmission_gate(target_process_gate.clone())
        .unwrap();
    let target_admission = target_process_gate.capture();
    install(&mut target, &order, false, behavior, historical_clock_fault);
    let mut restored = Cache::default();
    restored
        .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
        .unwrap();
    if !complete_gtd {
        restored
            .add_order(order.clone(), None, None, false)
            .unwrap();
    }
    target.restore_native_cache(restored).unwrap();
    target
        .restore_native_component_state(&first.2, &verified)
        .unwrap();
    target
        .kernel
        .risk_engine
        .borrow_mut()
        .set_trading_state(nautilus_model::enums::TradingState::Halted);
    target
        .kernel
        .open_event_store_for_paused_recovery()
        .unwrap();
    let watermark = RunnerRecoveryWatermark {
        recovery_id: "actual-framework-source-cut".into(),
        checkpoint_sequence: first.0.prefix.sequence,
        dispatch_watermark: first.0.last_input_sequence,
    };
    target
        .replay_recovery_events(&watermark, &[], &registry(), |_, _| Ok(()))
        .unwrap();
    let mut wrong_cut = watermark.clone();
    wrong_cut.checkpoint_sequence += 1;
    assert!(
        target
            .restore_native_market_checkpoint(&verified, &wrong_cut)
            .is_err()
    );
    target
        .restore_native_market_checkpoint(&verified, &watermark)
        .unwrap();
    assert_eq!(
        target
            .kernel
            .cache
            .borrow()
            .native_market_checkpoint()
            .unwrap(),
        first.0.native_effects["market_cache"]
    );
    assert!(
        target
            .restore_native_market_checkpoint(&verified, &watermark)
            .is_err()
    );
    target
        .restore_registered_engine_checkpoint(
            &first.1["execution_manager"],
            &first.1["data_engine"],
            first.0.captured_at_ns,
            &watermark,
        )
        .unwrap();
    target
        .restore_registered_portfolio_checkpoint(&first.0.native_effects["portfolio"], &watermark)
        .unwrap();
    target
        .restore_registered_timer_checkpoint(
            first.0.registered_timers.clone(),
            Vec::new(),
            &watermark,
        )
        .unwrap();
    let result = target.replay_native_tail(
        &verified,
        &watermark,
        |_, begin| {
            let NativeTraceRecord::Begin {
                input_source,
                payload,
                ..
            } = begin
            else {
                unreachable!()
            };
            match input_source {
                NativeInputSource::DataEvent => {
                    let mut event = decode_data(payload)?;
                    if changed && let DataEvent::Data(Data::Quote(ref mut quote)) = event {
                        quote.bid_price = Price::from("999.00");
                    }
                    Ok(RunnerRecoveryEvent::DataEvent(event))
                }
                NativeInputSource::ExecutionEvent => Ok(RunnerRecoveryEvent::ExecutionEvent(
                    ExecutionEvent::Order(serde_json::from_value(payload.clone())?),
                )),
                NativeInputSource::TradingCommand => Ok(RunnerRecoveryEvent::ExecutionCommand(
                    TradingCommandMessage::new(
                        payload["endpoint"]
                            .as_str()
                            .context("original command endpoint absent")?
                            .into(),
                        serde_json::from_value(payload["command"].clone())?,
                    ),
                )),
                _ => anyhow::bail!("unsupported original registered framework channel"),
            }
        },
        |_, _| anyhow::bail!("fixture has no retained input"),
    );
    if result.is_ok() {
        assert!(
            target_admission.can_transmit(),
            "original stop lifecycle must not revoke the new target process gate"
        );
    }
    if changed || fail_historical_stop || !historical_clock_fault.is_empty() {
        assert!(result.is_err());
        if !historical_clock_fault.is_empty() {
            assert!(
                format!("{:#}", result.as_ref().unwrap_err())
                    .contains("native tail replay panicked")
            );
        }
        if fail_historical_stop {
            let message = format!("{:#}", result.unwrap_err());
            assert!(
                message.contains("original managed business stop callback failed"),
                "{message}"
            );
            let actual =
                get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
            assert_eq!(actual.post_market_exits, 1);
            assert_eq!(actual.stopped, 1);
            assert_eq!(
                actual.time_events + 1,
                original_business_time_events,
                "failed final framework stop must precede that input's business timer callback"
            );
            assert_eq!(actual.state(), ComponentState::Ready);
        }
        assert!(target.kernel.exec_engine.borrow().submissions_fenced());
        assert!(target.event_store_halted());
    } else {
        result.unwrap();
        assert_eq!(
            target
                .kernel
                .cache
                .borrow()
                .native_market_checkpoint()
                .unwrap(),
            verified.final_cut().unwrap().native_effects["market_cache"]
        );
        let mut actual =
            get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
        assert_eq!(
            actual.indicator.counts.get(),
            if complete_market_exit || complete_gtd {
                [3, 3, 4]
            } else {
                [3, 4, 5]
            }
        );
        assert_eq!(
            actual.callbacks,
            if complete_market_exit || complete_gtd {
                [3, 3, 4]
            } else {
                [3, 4, 5]
            }
        );
        assert_eq!(actual.created_gtd, u64::from(!complete_market_exit));
        assert_eq!(
            actual.denied,
            if complete_market_exit || complete_gtd {
                7
            } else {
                8
            }
        );
        assert_eq!(actual.stopped, 1);
        assert_eq!(
            actual.post_market_exits,
            u64::from(complete_market_exit || manage_stop)
        );
        assert_eq!(actual.time_events, original_business_time_events);
        assert!(!actual.is_exiting());
        assert!(
            !actual
                .clock_rc()
                .borrow()
                .timer_names()
                .iter()
                .any(|n| n == &format!("MARKET_EXIT_CHECK:{STRATEGY}"))
        );
        assert_eq!(actual.state(), ComponentState::Ready);
        if complete_gtd {
            assert_gtd_business_cache(&mut actual);
        }
        if complete_market_exit {
            assert_managed_business_cache(&actual);
            assert_eq!(actual.gtd_order_events, original_business_order_events);
        }
        drop(actual);
        assert_eq!(
            nautilus_system::trader::Trader::collect_native_strategy_framework(
                &target.kernel.trader
            )
            .unwrap(),
            verified.final_cut().unwrap().native_effects["strategy_framework"]
        );
        assert_eq!(target.state(), NodeState::Idle);
        assert_eq!(
            target.kernel.risk_engine.borrow().trading_state(),
            nautilus_model::enums::TradingState::Halted
        );
    }
    target
        .kernel
        .prohibit_event_store_seal("actual framework fixture retains child without new grant")
        .unwrap();
    target.dispose();
    drop(target);
    drop(reader);
    drop(trace);
    std::fs::remove_dir_all(directory).unwrap();
}

fn assert_gtd_business_cache(actual: &mut FrameworkStrategy) {
    assert_eq!(actual.time_events, 1);
    assert_eq!(actual.created_gtd, 1);
    assert!(!actual.has_gtd_expiry_timer(&ClientOrderId::from(ORDER)));
    let cache = actual.cache_ref();
    let order = cache
        .order(&ClientOrderId::from(ORDER))
        .expect("actual GTD economic cache missing");
    assert_eq!(order.status(), OrderStatus::PendingCancel);
    assert_eq!(
        actual
            .gtd_order_events
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Submitted(_)))
            .count(),
        1
    );
    assert_eq!(
        actual
            .gtd_order_events
            .iter()
            .filter(|event| matches!(event, OrderEventAny::Accepted(_)))
            .count(),
        1
    );
    assert_eq!(
        actual
            .gtd_order_events
            .iter()
            .filter(|event| matches!(event, OrderEventAny::PendingCancel(_)))
            .count(),
        1
    );
}

fn assert_original_gtd_timer_tail(
    verified: &nautilus_event_store::native_trace::VerifiedNativeTrace,
) {
    use nautilus_common::messages::execution::TradingCommand;
    let original = verified
        .roots()
        .iter()
        .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
        .find_map(|input| {
            let NativeTraceRecord::Complete {
                root_sequence,
                input_sequence,
                callbacks,
                ..
            } = input
            else {
                return None;
            };
            callbacks
                .iter()
                .any(|route| route.kind == "strategy.expire_gtd_order.default")
                .then_some((*root_sequence, *input_sequence, callbacks))
        })
        .expect("actual Source never dispatched the default GTD expiry");
    assert_eq!(
        original
            .2
            .iter()
            .filter(|route| route.component_id == STRATEGY)
            .map(|route| route.kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "strategy.handle_time_event",
            "strategy.expire_gtd_order.default",
            "handle_order_event"
        ]
    );
    let command = verified
        .roots()
        .iter()
        .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
        .find_map(|input| {
            let NativeTraceRecord::Begin {
                input_source: NativeInputSource::TradingCommand,
                payload,
                receipt,
                ..
            } = input
            else {
                return None;
            };
            let command: TradingCommand = serde_json::from_value(payload["command"].clone())
                .expect("original native cancel payload malformed");
            let TradingCommand::CancelOrder(command) = command else {
                return None;
            };
            assert_eq!(command.client_order_id, ClientOrderId::from(ORDER));
            let cause = receipt
                .ingress
                .as_ref()
                .and_then(|ingress| ingress.caused_by.as_ref())
                .expect("cancel command lost actual timer causal receipt");
            assert_eq!(
                (cause.root_sequence, cause.input_sequence),
                (original.0, original.1)
            );
            Some(command.command_id)
        })
        .expect("actual GTD cancel command never traversed original native execution FIFO");
    let command_was_drawn = verified
        .roots()
        .iter()
        .flat_map(nautilus_event_store::native_trace::VerifiedNativeRoot::inputs)
        .filter_map(|input| {
            let NativeTraceRecord::Complete { uuid_draws, .. } = input else {
                return None;
            };
            Some(uuid_draws)
        })
        .flatten()
        .any(|draw| draw == &command);
    assert!(
        command_was_drawn,
        "GTD cancel UUID was not an actual original draw"
    );
}

fn assert_managed_business_cache(actual: &FrameworkStrategy) {
    assert!(matches!(
        actual.gtd_order_events.as_slice(),
        [
            OrderEventAny::Submitted(_),
            OrderEventAny::Accepted(_),
            OrderEventAny::PendingCancel(_),
            OrderEventAny::Canceled(_),
        ]
    ));
    assert_eq!(
        actual
            .cache_ref()
            .order(&ClientOrderId::from(ORDER))
            .unwrap()
            .status(),
        OrderStatus::Canceled
    );
}

fn assert_original_managed_cancel_tail(
    verified: &nautilus_event_store::native_trace::VerifiedNativeTrace,
    events: &[OrderEventAny],
) {
    use nautilus_common::messages::execution::TradingCommand;
    let [
        OrderEventAny::Submitted(submitted),
        OrderEventAny::Accepted(accepted),
        OrderEventAny::PendingCancel(pending),
        OrderEventAny::Canceled(canceled),
    ] = events
    else {
        panic!("original managed event chain incomplete or reordered");
    };
    assert!(submitted.ts_event <= accepted.ts_event);
    assert!(accepted.ts_event <= pending.ts_event && pending.ts_event <= canceled.ts_event);
    let mut quote = None;
    let mut cancel = None;
    let mut canceled_input = None;
    let mut execution_events = Vec::new();
    for root in verified.roots() {
        for input in root.inputs() {
            let NativeTraceRecord::Begin {
                root_sequence,
                input_sequence,
                input_source,
                payload,
                receipt,
                ..
            } = input
            else {
                continue;
            };
            match input_source {
                NativeInputSource::DataEvent if payload.get("Quote").is_some() => {
                    assert!(quote.replace((*root_sequence, *input_sequence)).is_none());
                    let outputs = root.historical_outputs(*input_sequence).unwrap();
                    let pending_outputs = outputs
                        .iter()
                        .filter(|output| {
                            output.topic == format!("events.order.{STRATEGY}")
                                && output.payload_type == "OrderPendingCancel"
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(
                        pending_outputs.len(),
                        1,
                        "pending cancel lacks its original Journal output"
                    );
                    assert_eq!(
                        pending_outputs[0].payload.as_slice(),
                        rmp_serde::to_vec_named(pending).unwrap().as_slice(),
                        "original Journal pending cancel MessagePack bytes differ"
                    );
                    assert_eq!(
                        rmp_serde::from_slice::<OrderPendingCancel>(&pending_outputs[0].payload)
                            .unwrap(),
                        *pending
                    );
                    let complete = root
                        .inputs()
                        .iter()
                        .find_map(|record| match record {
                            NativeTraceRecord::Complete {
                                input_sequence: sequence,
                                uuid_draws,
                                ..
                            } if sequence == input_sequence => Some(uuid_draws),
                            _ => None,
                        })
                        .expect("original Quote Complete absent");
                    assert!(complete.contains(&pending.event_id));
                }
                NativeInputSource::ExecutionEvent => {
                    let event: OrderEventAny = serde_json::from_value(payload.clone()).unwrap();
                    if matches!(&event, OrderEventAny::Canceled(_)) {
                        assert!(canceled_input.replace(*input_sequence).is_none());
                        assert_eq!(event, OrderEventAny::Canceled(*canceled));
                    }
                    execution_events.push(event);
                }
                NativeInputSource::TradingCommand => {
                    let command: TradingCommand =
                        serde_json::from_value(payload["command"].clone()).unwrap();
                    let TradingCommand::CancelOrder(command) = command else {
                        panic!("unexpected managed exit command");
                    };
                    assert_eq!(command.client_order_id, ClientOrderId::from(ORDER));
                    assert_eq!(
                        command.venue_order_id,
                        Some(VenueOrderId::from("V-MANAGED-ACTUAL"))
                    );
                    let cause = receipt
                        .ingress
                        .as_ref()
                        .and_then(|ingress| ingress.caused_by.as_ref())
                        .expect("managed cancel lacks original Quote cause");
                    assert_eq!(Some((cause.root_sequence, cause.input_sequence)), quote);
                    assert!(cancel.replace(*input_sequence).is_none());
                    assert!(root.inputs().iter().any(|record| matches!(record,
                        NativeTraceRecord::Complete { input_sequence: sequence, .. }
                            if sequence == input_sequence)));
                }
                _ => {}
            }
        }
    }
    // PendingCancel is the Quote's actual synchronous output, never a second
    // synthetic ExecutionEvent. The original acknowledgment follows the real
    // queued command, and historical replay compares the same full receipts.
    assert_eq!(
        execution_events,
        vec![
            OrderEventAny::Submitted(*submitted),
            OrderEventAny::Accepted(*accepted),
            OrderEventAny::Canceled(*canceled),
        ]
    );
    let quote_input = quote.expect("original managed Quote absent").1;
    let cancel_input = cancel.expect("original managed CancelOrder FIFO input absent");
    assert!(quote_input < cancel_input && cancel_input < canceled_input.unwrap());
}
