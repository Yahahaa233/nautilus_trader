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
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_event_store::{EventStoreReader, backend::RedbBackend, kernel::EventStoreLifecycle};
use nautilus_model::{
    data::{Bar, BarType, Data, QuoteTick, TradeTick},
    enums::{OrderType, TimeInForce},
    events::{OrderDenied, OrderEventAny},
    identifiers::{ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderAny, OrderTestBuilder},
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
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
#[derive(Debug)]
struct FrameworkStrategy {
    core: StrategyCore,
    indicator: Rc<CountingIndicator>,
    callbacks: [u64; 3],
    denied: u64,
    stopped: u64,
    created_gtd: u64,
    bindings: bool,
}
impl FrameworkStrategy {
    fn new() -> Self {
        let mut value = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                manage_gtd_expiry: true,
                manage_contingent_orders: true,
                ..Default::default()
            }),
            indicator: Rc::new(CountingIndicator {
                counts: Cell::new([2, 3, 4]),
            }),
            callbacks: [2, 3, 4],
            denied: 7,
            stopped: 0,
            created_gtd: 0,
            bindings: false,
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
            .register_indicator_for_bars(bar_type(), indicator.clone());
        value
    }
    fn bind_original_handlers(&mut self) {
        assert!(!self.bindings, "local original bindings installed twice");
        let id = self.actor_id().inner();
        msgbus::subscribe_quotes(
            switchboard::get_quotes_topic(instrument_id()).into(),
            TypedHandler::from(move |q: &QuoteTick| {
                get_actor_unchecked::<Self>(&id).handle_quote(q)
            }),
            None,
        );
        msgbus::subscribe_trades(
            switchboard::get_trades_topic(instrument_id()).into(),
            TypedHandler::from(move |t: &TradeTick| {
                get_actor_unchecked::<Self>(&id).handle_trade(t)
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
        self.stopped += 1;
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
        ensure!(
            value["gtd_clock_present"] == false,
            "initial fixture cannot inherit an uninstalled GTD map"
        );
        Ok(())
    }
    fn on_quote(&mut self, _: &QuoteTick) -> Result<()> {
        self.business_data(0)?;
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
            self.on_quote(q)
        } else if let Some(t) = input.downcast_ref::<TradeTick>() {
            self.on_trade(t)
        } else if let Some(b) = input.downcast_ref::<Bar>() {
            self.on_bar(b)
        } else if let Some(event) = input.downcast_ref::<OrderEventAny>() {
            self.on_order_event(event.clone());
            Ok(())
        } else if let Some(event) = input.downcast_ref::<NativeComponentLifecycle>() {
            match event.action.as_str() {
                "lifecycle.stop" => DataActor::on_stop(self),
                "lifecycle.strategy_stop" => Ok(()),
                _ => anyhow::bail!("unknown original lifecycle"),
            }
        } else {
            anyhow::bail!("unsupported original framework business input")
        }
    }
}
nautilus_strategy!(FrameworkStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
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
    fn codec_id(&self) -> &str {
        "actual_registered_framework_tail.v1"
    }
    fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
        match event {
            RunnerRecoveryEventRef::DataEvent(v) => data_payload(v),
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
            _ => anyhow::bail!("unknown framework queue channel"),
        }
    }
}
fn registry() -> Rc<RunnerRecoveryCodecRegistry> {
    let channels = [
        RunnerRecoveryChannel::DataEvent,
        RunnerRecoveryChannel::ExecutionEvent,
    ];
    let mut value = RunnerRecoveryCodecRegistry::new(channels);
    for channel in channels {
        value.register(FrameworkCodec(channel)).unwrap();
    }
    Rc::new(value.seal().unwrap())
}
fn install(node: &mut LiveNode, order: &OrderAny, source: bool) {
    node.add_strategy(FrameworkStrategy::new()).unwrap();
    if source {
        node.kernel
            .cache
            .borrow_mut()
            .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
            .unwrap();
        node.kernel
            .cache
            .borrow_mut()
            .add_order(order.clone(), None, None, false)
            .unwrap();
    }
}

#[rstest]
#[case(false, false)]
#[case(true, false)]
#[case(false, true)]
#[case(true, true)]
#[tokio::test(flavor = "current_thread")]
async fn actual_registered_native_tail_framework_effects_and_changed_input(
    #[case] changed: bool,
    #[case] historical_responses: bool,
) {
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
    install(&mut source, &order, true);
    if historical_responses {
        // Actual response processing, not a cache DTO: production warmup fills
        // the shared DataEngine cache before the first completed-root cut.
        let received = Rc::new(Cell::new(0));
        for bars in [true, false] {
            let correlation_id = UUID4::new();
            let count = received.clone();
            let handler = if bars {
                ShareableMessageHandler::from_typed(move |_: &BarsResponse| {
                    count.set(count.get() + 1)
                })
            } else {
                ShareableMessageHandler::from_typed(move |_: &QuotesResponse| {
                    count.set(count.get() + 1)
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
            |_| Ok(()),
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
    let mut bar = Bar::default();
    bar.bar_type = bar_type();
    bar.ts_event = now;
    bar.ts_init = now;
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
    let produced = async move {
        ready.notified().await;
        for event in [
            DataEvent::Data(Data::Quote(quote)),
            DataEvent::Data(Data::Trade(trade)),
            DataEvent::Data(Data::Bar(bar)),
        ] {
            data.send(event).unwrap();
            progress.notified().await;
        }
        exec.send(ExecutionEvent::Order(denied)).unwrap();
        progress.notified().await;
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
    let actual = get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
    assert_eq!(actual.indicator.counts.get(), [3, 4, 5]);
    assert_eq!(actual.denied, 8);
    assert_eq!(actual.created_gtd, 1);
    assert_eq!(actual.stopped, 1);
    drop(actual);
    let identity = trace.source().unwrap();
    let first = cuts.borrow()[0].clone();
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
    assert!(verified.roots().iter().any(|r| r.inputs().iter().any(|i| matches!(i,NativeTraceRecord::Complete {callbacks,..} if callbacks.iter().any(|c| c.kind=="handle_order_event")))));
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
            identity.journal_run.clone(),
            end,
            fingerprint,
        )),
    );
    install(&mut target, &order, false);
    let mut restored = Cache::default();
    restored
        .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
        .unwrap();
    restored
        .add_order(order.clone(), None, None, false)
        .unwrap();
    target.restore_native_cache(restored).unwrap();
    target.restore_component_state(&first.2).unwrap();
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
                _ => anyhow::bail!("unsupported original registered framework channel"),
            }
        },
        |_, _| anyhow::bail!("fixture has no retained input"),
    );
    if changed {
        assert!(result.is_err());
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
        let actual = get_actor_unchecked::<FrameworkStrategy>(&StrategyId::from(STRATEGY).inner());
        assert_eq!(actual.indicator.counts.get(), [3, 4, 5]);
        assert_eq!(actual.callbacks, [3, 4, 5]);
        assert_eq!(actual.created_gtd, 1);
        assert_eq!(actual.denied, 8);
        assert_eq!(actual.stopped, 1);
        assert_eq!(actual.state(), ComponentState::Ready);
        drop(actual);
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
