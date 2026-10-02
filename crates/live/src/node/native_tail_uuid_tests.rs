// Actual registered strategy emissions, recorded by the native runloop and
// replayed on the same original-object routes. No venue client or wire is used.
use super::{tests::actual_node, *};
use crate::{
    node::{NodeRunMode, RunningCheckpointSchedule},
    runner_recovery::{
        RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryCodecRegistry,
        RunnerRecoveryEnvelope, RunnerRecoveryEventRef,
    },
};
use indexmap::IndexMap;
use nautilus_common::{
    actor::{DataActor, DataActorNative, registry::get_actor_unchecked},
    cache::Cache,
    component::Component,
    enums::ComponentState,
    factories::OrderEventFactory,
    live::runner::get_exec_event_sender,
    messages::{DataEvent, ExecutionEvent, execution::TradingCommand},
    msgbus::{self, TypedHandler, switchboard},
    recovery_trace::{
        NativeComponentLifecycle,
        historical::{HistoricalInputBoundary, HistoricalReplayPreparation},
    },
    runner::TradingCommandMessage,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_event_store::{EventStoreReader, backend::RedbBackend, kernel::EventStoreLifecycle};
use nautilus_model::{
    data::{Data, QuoteTick},
    enums::{AccountType, OrderSide, OrderType, TimeInForce, TradingState},
    events::{OrderAccepted, OrderEventAny, OrderSubmitted},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, VenueOrderId},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderAny, OrderTestBuilder},
    types::{Price, Quantity},
};
use nautilus_trading::{
    StrategyNative, nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rstest::rstest;
use std::{any::Any, cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};

const STRATEGY: &str = "UuidCommandTail-001";
const SEEDS: [&str; 3] = ["SEED-MODIFY-CANCEL", "SEED-BATCH-A", "SEED-BATCH-B"];
fn instrument_id() -> InstrumentId {
    crypto_perpetual_ethusdt().id()
}

#[derive(Debug)]
struct CommandStrategy {
    core: StrategyCore,
    emitted: u64,
    stopped: u64,
    orders: Vec<ClientOrderId>,
    events: Vec<OrderEventAny>,
    bindings: bool,
}
impl CommandStrategy {
    fn new() -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(STRATEGY.into()),
                use_uuid_client_order_ids: true,
                ..Default::default()
            }),
            emitted: 0,
            stopped: 0,
            orders: Vec::new(),
            events: Vec::new(),
            bindings: false,
        }
    }
    fn bind_original(&mut self) {
        assert!(!self.bindings);
        let id = self.actor_id().inner();
        msgbus::subscribe_quotes(
            switchboard::get_quotes_topic(instrument_id()).into(),
            TypedHandler::from(move |quote: &QuoteTick| {
                get_actor_unchecked::<Self>(&id).handle_quote(quote)
            }),
            None,
        );
        self.bindings = true;
    }
    fn limit(&mut self) -> OrderAny {
        self.order_factory().limit(
            instrument_id(),
            OrderSide::Buy,
            Quantity::from("1.000"),
            Price::from("1000.00"),
            Some(TimeInForce::Gtc),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }
    fn snapshot(&self) -> Result<IndexMap<String, Vec<u8>>> {
        Ok(IndexMap::from([(
            "command_business.v1".into(),
            serde_json::to_vec(&serde_json::json!({
                "emitted":self.emitted,"stopped":self.stopped,"orders":self.orders,"events":self.events,
                "order_list_count":self.order_factory_rc().borrow().order_list_id_count(),
            }))?,
        )]))
    }
}
impl DataActor for CommandStrategy {
    fn on_start(&mut self) -> Result<()> {
        self.bind_original();
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
        let v: serde_json::Value = serde_json::from_slice(&state["command_business.v1"])?;
        self.emitted = v["emitted"].as_u64().context("emitted absent")?;
        self.stopped = v["stopped"].as_u64().context("stopped absent")?;
        self.orders = serde_json::from_value(v["orders"].clone())?;
        self.events = serde_json::from_value(v["events"].clone())?;
        self.order_factory_rc()
            .borrow_mut()
            .set_order_list_id_count(
                v["order_list_count"]
                    .as_u64()
                    .context("list count absent")? as usize,
            );
        Ok(())
    }
    fn on_quote(&mut self, _: &QuoteTick) -> Result<()> {
        ensure!(self.emitted == 0, "source emission repeated");
        let single = self.limit();
        self.orders.push(single.client_order_id());
        self.submit_order(single, None, None, None)?;
        let a = self.limit();
        let b = self.limit();
        self.orders
            .extend([a.client_order_id(), b.client_order_id()]);
        self.submit_order_list(vec![a, b], None, None, None)?;
        self.modify_order(
            SEEDS[0].into(),
            Some(Quantity::from("2.000")),
            Some(Price::from("999.00")),
            None,
            None,
            None,
        )?;
        self.cancel_order(SEEDS[0].into(), None, None)?;
        self.cancel_orders(vec![SEEDS[1].into(), SEEDS[2].into()], None, None)?;
        // A local no-wire denial generated by the actual report factory and
        // processed by the real execution queue, never a fabricated venue ACK.
        let local_denied = self.limit();
        self.orders.push(local_denied.client_order_id());
        self.cache_rc()
            .borrow_mut()
            .add_order(local_denied.clone(), None, None, true)?;
        let factory = OrderEventFactory::new(
            local_denied.trader_id(),
            AccountId::from("BINANCE-001"),
            AccountType::Margin,
            None,
        );
        let event = factory.generate_order_denied(
            &local_denied,
            "actual local factory policy denial",
            self.clock().timestamp_ns(),
        );
        get_exec_event_sender().send(ExecutionEvent::Order(event))?;
        self.emitted += 1;
        Ok(())
    }
    fn prepare_native_recovery(&mut self, p: &HistoricalReplayPreparation<'_>) -> Result<()> {
        ensure!(
            p.verified_source::<nautilus_event_store::native_trace::VerifiedNativeTrace>()
                .is_some(),
            "source not verified"
        );
        self.bind_original();
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
            boundary.verified_source::<VerifiedNativeRoot>().is_some()
                && self.state() == ComponentState::Ready,
            "historical source/lifecycle changed"
        );
        if let Some(q) = input.downcast_ref::<QuoteTick>() {
            self.on_quote(q)
        } else if let Some(e) = input.downcast_ref::<OrderEventAny>() {
            self.on_order_event(e.clone());
            Ok(())
        } else if let Some(l) = input.downcast_ref::<NativeComponentLifecycle>() {
            match l.action.as_str() {
                "lifecycle.stop" => DataActor::on_stop(self),
                "lifecycle.strategy_stop" => Ok(()),
                _ => anyhow::bail!("unknown command lifecycle"),
            }
        } else {
            anyhow::bail!("unknown original command business input")
        }
    }
}
nautilus_strategy!(CommandStrategy, {
    fn on_order_event(&mut self, event: OrderEventAny) {
        self.events.push(event);
    }
});

fn command_payload(command: &TradingCommandMessage) -> serde_json::Value {
    serde_json::json!({"endpoint":command.endpoint().to_string(),"command":command.command()})
}
fn decode_command(v: &serde_json::Value) -> Result<TradingCommandMessage> {
    Ok(TradingCommandMessage::new(
        v["endpoint"].as_str().context("endpoint absent")?.into(),
        serde_json::from_value(v["command"].clone())?,
    ))
}
#[derive(Debug)]
struct CommandCodec(RunnerRecoveryChannel);
impl RunnerRecoveryCodec for CommandCodec {
    fn channel(&self) -> RunnerRecoveryChannel {
        self.0
    }
    fn codec_id(&self) -> &str {
        "actual_registered_uuid_commands.v1"
    }
    fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<serde_json::Value> {
        match event {
            RunnerRecoveryEventRef::DataEvent(v) => super::framework_tests::data_payload(v),
            RunnerRecoveryEventRef::ExecutionEvent(ExecutionEvent::Order(v)) => {
                Ok(serde_json::to_value(v)?)
            }
            RunnerRecoveryEventRef::ExecutionCommand(v) => Ok(command_payload(v)),
            _ => anyhow::bail!("unknown command queue"),
        }
    }
    fn decode(&self, input: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
        match self.0 {
            RunnerRecoveryChannel::DataEvent => {
                Ok(RunnerRecoveryEvent::DataEvent(DataEvent::Data(
                    Data::Quote(serde_json::from_value(input.payload["Quote"].clone())?),
                )))
            }
            RunnerRecoveryChannel::ExecutionEvent => Ok(RunnerRecoveryEvent::ExecutionEvent(
                ExecutionEvent::Order(serde_json::from_value(input.payload.clone())?),
            )),
            RunnerRecoveryChannel::ExecutionCommand => Ok(RunnerRecoveryEvent::ExecutionCommand(
                decode_command(&input.payload)?,
            )),
            _ => anyhow::bail!("unknown command channel"),
        }
    }
}
fn registry() -> Rc<RunnerRecoveryCodecRegistry> {
    let channels = [
        RunnerRecoveryChannel::DataEvent,
        RunnerRecoveryChannel::ExecutionEvent,
        RunnerRecoveryChannel::ExecutionCommand,
    ];
    let mut r = RunnerRecoveryCodecRegistry::new(channels);
    for c in channels {
        r.register(CommandCodec(c)).unwrap();
    }
    Rc::new(r.seal().unwrap())
}
fn seed_orders(now: UnixNanos) -> Vec<OrderAny> {
    SEEDS
        .iter()
        .map(|id| {
            let mut o = OrderTestBuilder::new(OrderType::Limit)
                .trader_id("NATIVE-TAIL-001".into())
                .strategy_id(STRATEGY.into())
                .instrument_id(instrument_id())
                .client_order_id((*id).into())
                .side(OrderSide::Buy)
                .quantity(Quantity::from("1.000"))
                .price(Price::from("1000.00"))
                .build();
            let a = AccountId::from("BINANCE-001");
            o.apply(OrderEventAny::Submitted(OrderSubmitted::new(
                o.trader_id(),
                o.strategy_id(),
                o.instrument_id(),
                o.client_order_id(),
                a,
                UUID4::new(),
                now,
                now,
            )))
            .unwrap();
            o.apply(OrderEventAny::Accepted(OrderAccepted::new(
                o.trader_id(),
                o.strategy_id(),
                o.instrument_id(),
                o.client_order_id(),
                VenueOrderId::from(*id),
                a,
                UUID4::new(),
                now,
                now,
                false,
            )))
            .unwrap();
            o
        })
        .collect()
}
fn cache(orders: &[OrderAny]) -> Cache {
    let mut cache = Cache::default();
    cache
        .add_instrument(InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt()))
        .unwrap();
    for o in orders {
        cache.add_order(o.clone(), None, None, false).unwrap();
    }
    cache
}

#[rstest]
#[case(false)]
#[case(true)]
#[tokio::test(flavor = "current_thread")]
async fn actual_registered_native_tail_uuid_commands_and_changed_input(#[case] changed: bool) {
    let directory = std::path::PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").unwrap())
        .join(format!("uuid-command-tail-{}", UUID4::new()));
    std::fs::create_dir_all(&directory).unwrap();
    let now = nautilus_core::time::get_atomic_clock_realtime().get_time_ns();
    let seeds = seed_orders(now);
    let mut source = actual_node("uuid-command-source", directory.join("source"), None);
    source.add_strategy(CommandStrategy::new()).unwrap();
    *source.kernel.cache.borrow_mut() = cache(&seeds);
    let instance = source.kernel.instance_id();
    let trace = source
        .prepare_owned_native_trace(
            "uuid-command-business-run".into(),
            nautilus_event_store::native_trace::native_inventory_digest(
                &serde_json::to_value(&source.config).unwrap(),
            )
            .unwrap(),
            "actual_registered_uuid_commands.v1".into(),
            "actual_original_strategy_commands.v1".into(),
            |_, _, _| Ok(Vec::new()),
            |_| Ok(()),
        )
        .unwrap();
    let cuts = Rc::new(RefCell::new(Vec::new()));
    let ready = Rc::new(tokio::sync::Notify::new());
    let progressed = Rc::new(tokio::sync::Notify::new());
    let saved = cuts.clone();
    let started = ready.clone();
    let advanced = progressed.clone();
    source
        .set_running_checkpoint_handler(
            registry(),
            RunningCheckpointSchedule::EveryCompletedRoot,
            move |boundary| {
                boundary.verify()?;
                let cut = boundary.native_trace_cut().context("trace absent")?.clone();
                boundary.persist_native_checkpoint(
                    serde_json::json!({"actual_registered_uuid_commands":true}),
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
    let handle = source.handle();
    let observed = cuts.clone();
    let mut quote = nautilus_model::data::stubs::quote_ethusdt_binance();
    quote.ts_event = now;
    quote.ts_init = now;
    let producer = async move {
        ready.notified().await;
        data.send(DataEvent::Data(Data::Quote(quote))).unwrap();
        loop {
            progressed.notified().await;
            if observed
                .borrow()
                .last()
                .is_some_and(|cut| cut.0.pending_inputs.is_empty())
            {
                break;
            }
        }
        handle.stop();
    };
    let mut producer = std::pin::pin!(producer);
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut run = std::pin::pin!(source.run_with_mode(NodeRunMode::Hosted));
        tokio::select! {r=&mut run=>r,()=&mut producer=>run.await}
    })
    .await
    .expect("source command runloop timed out")
    .unwrap();
    let original = get_actor_unchecked::<CommandStrategy>(&StrategyId::from(STRATEGY).inner())
        .snapshot()
        .unwrap();
    let identity = trace.source().unwrap();
    let first = cuts.borrow()[0].clone();
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
    let mut draws = BTreeSet::new();
    let mut emitted = BTreeSet::new();
    let mut command_ids = Vec::new();
    for root in verified.roots() {
        for input in root.inputs() {
            match input {
                NativeTraceRecord::Complete { uuid_draws, .. } => {
                    draws.extend(uuid_draws.iter().map(ToString::to_string))
                }
                NativeTraceRecord::Begin {
                    input_source: NativeInputSource::TradingCommand,
                    payload,
                    ..
                } => {
                    let message = decode_command(payload).unwrap();
                    let command = message.command();
                    let id = match command {
                        TradingCommand::SubmitOrder(c) => c.command_id,
                        TradingCommand::SubmitOrderList(c) => c.command_id,
                        TradingCommand::ModifyOrder(c) => c.command_id,
                        TradingCommand::CancelOrder(c) => c.command_id,
                        TradingCommand::CancelOrders(c) => c.command_id,
                        _ => panic!("unexpected actual command class"),
                    };
                    command_ids.push(id.to_string());
                    let kind = match command {
                        TradingCommand::SubmitOrder(_) => "single",
                        TradingCommand::SubmitOrderList(_) => "list",
                        TradingCommand::ModifyOrder(_) => "modify",
                        TradingCommand::CancelOrder(_) => "cancel",
                        TradingCommand::CancelOrders(_) => "batch_cancel",
                        _ => "other",
                    };
                    emitted.insert(kind);
                }
                _ => {}
            }
        }
    }
    assert!(
        ["single", "list", "modify", "cancel", "batch_cancel"]
            .iter()
            .all(|kind| emitted.contains(kind)),
        "actual source command classes absent: {emitted:?}"
    );
    assert!(
        command_ids.iter().all(|id| draws.contains(id)),
        "actual emitted command UUID was not recorded"
    );
    assert!(
        draws.len() > command_ids.len(),
        "factory and pending-report draws absent"
    );
    let original_state: serde_json::Value =
        serde_json::from_slice(&original["command_business.v1"]).unwrap();
    let source_events: Vec<OrderEventAny> =
        serde_json::from_value(original_state["events"].clone()).unwrap();
    let factory_id = source_events
        .iter()
        .find_map(|event| match event {
            OrderEventAny::Denied(e)
                if e.reason.as_str() == "actual local factory policy denial" =>
            {
                Some(e.event_id.to_string())
            }
            _ => None,
        })
        .expect("actual factory denial did not reach original strategy");
    assert!(
        draws.contains(&factory_id),
        "factory report UUID not present in original draws"
    );
    let fingerprint = EventStoreLifecycle::sealed_run_fingerprint(
        &directory.join("source"),
        instance,
        &identity.journal_run,
        end,
    )
    .unwrap();
    let mut target = actual_node(
        "uuid-command-target",
        directory.join("target"),
        Some((
            directory.join("source"),
            instance,
            identity.journal_run.clone(),
            end,
            fingerprint,
        )),
    );
    target.add_strategy(CommandStrategy::new()).unwrap();
    target.restore_native_cache(cache(&seeds)).unwrap();
    target.restore_component_state(&first.2).unwrap();
    target
        .kernel
        .risk_engine
        .borrow_mut()
        .set_trading_state(TradingState::Halted);
    target
        .kernel
        .open_event_store_for_paused_recovery()
        .unwrap();
    let watermark = RunnerRecoveryWatermark {
        recovery_id: "actual-command-source-cut".into(),
        checkpoint_sequence: first.0.prefix.sequence,
        dispatch_watermark: first.0.last_input_sequence,
    };
    target
        .replay_recovery_events(&watermark, &[], &registry(), |_, _| Ok(()))
        .unwrap();
    target
        .restore_registered_engine_checkpoint(
            &first.1["execution_manager"],
            &first.1["data_engine"],
            first.0.captured_at_ns,
            &watermark,
        )
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
                    let mut quote: QuoteTick = serde_json::from_value(payload["Quote"].clone())?;
                    if changed {
                        quote.bid_price = Price::from("999.00");
                    }
                    Ok(RunnerRecoveryEvent::DataEvent(DataEvent::Data(
                        Data::Quote(quote),
                    )))
                }
                NativeInputSource::TradingCommand => Ok(RunnerRecoveryEvent::ExecutionCommand(
                    decode_command(payload)?,
                )),
                NativeInputSource::ExecutionEvent => Ok(RunnerRecoveryEvent::ExecutionEvent(
                    ExecutionEvent::Order(serde_json::from_value(payload.clone())?),
                )),
                _ => anyhow::bail!("unsupported actual command source"),
            }
        },
        |_, _| anyhow::bail!("source left no retained input"),
    );
    if changed {
        assert!(result.is_err());
        assert!(target.kernel.exec_engine.borrow().submissions_fenced());
        assert!(target.event_store_halted());
    } else {
        result.unwrap();
        let replayed = get_actor_unchecked::<CommandStrategy>(&StrategyId::from(STRATEGY).inner());
        assert_eq!(replayed.snapshot().unwrap(), original);
        assert_eq!(replayed.state(), ComponentState::Ready);
        assert_eq!(
            target.kernel.risk_engine.borrow().trading_state(),
            TradingState::Halted
        );
        assert_eq!(target.state(), NodeState::Idle);
    }
    target
        .kernel
        .prohibit_event_store_seal("actual command replay grants no authority")
        .unwrap();
    target.dispose();
    drop(target);
    drop(reader);
    drop(trace);
    std::fs::remove_dir_all(directory).unwrap();
}
