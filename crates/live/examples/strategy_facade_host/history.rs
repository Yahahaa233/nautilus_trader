// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

mod history {
    use super::*;
    use nautilus_common::{
        cache::Cache,
        messages::data::{DataCommand, RequestCommand},
        recovery_trace::{NativeInputSource, NativeTraceRecord},
    };
    use nautilus_core::transmission::TransmissionGate;
    use nautilus_event_store::{
        EventStoreReader,
        backend::RedbBackend,
        kernel::EventStoreLifecycle,
        native_trace::{NativeTraceCheckpointCut, native_inventory_digest},
    };
    use nautilus_live::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{NativeMutationInput, NodeDispatchObserver, RunningCheckpointSchedule},
        runner_recovery::{
            RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryCodecRegistry,
            RunnerRecoveryEnvelope, RunnerRecoveryEvent, RunnerRecoveryEventRef,
            RunnerRecoveryWatermark,
        },
    };
    use nautilus_model::enums::TradingState;
    use nautilus_system::{
        event_store::EventStoreConfig,
        trader::{CollectedComponentState, Trader},
    };
    use serde::{Deserialize, Serialize};
    use std::{collections::BTreeMap, fs::OpenOptions, io::Write, path::PathBuf};

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum Scenario {
        Normal,
        Duplicate,
        Late,
        ForeignClient,
        Oversized,
        ForeignCorrelation,
    }
    impl Scenario {
        fn fault(self) -> bool {
            matches!(self, Self::ForeignClient | Self::Oversized)
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Cut {
        cut: NativeTraceCheckpointCut,
        inventory: Value,
        components: CollectedComponentState,
    }
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Source {
        schema: String,
        scenario: Scenario,
        failure: Option<Value>,
        pid: u32,
        journal: PathBuf,
        instance: UUID4,
        first: Cut,
        end: u64,
        fingerprint: String,
        instrument: InstrumentAny,
        account: AccountState,
        final_states: CollectedComponentState,
        provider: Value,
    }
    fn original(name: &str, value: &impl Serialize) -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(name)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        File::open(".")?.sync_all()?;
        Ok(())
    }
    fn data_payload(event: &DataEvent) -> Result<Value> {
        match event {
            DataEvent::Instrument(value) => Ok(json!({"Instrument":value})),
            DataEvent::Response(DataResponse::Bars(value)) => Ok(json!({"BarsResponse":value})),
            DataEvent::Data(Data::Quote(value)) => Ok(json!({"Quote":value})),
            DataEvent::Data(Data::Trade(value)) => Ok(json!({"Trade":value})),
            DataEvent::Data(Data::Bar(value)) => Ok(json!({"Bar":value})),
            _ => bail!("unsupported facade data input"),
        }
    }
    fn command_payload(command: &DataCommand) -> Result<Value> {
        match command {
            DataCommand::Subscribe(value) => Ok(json!({"Subscribe":value})),
            DataCommand::Unsubscribe(value) => Ok(json!({"Unsubscribe":value})),
            DataCommand::Request(RequestCommand::Bars(value)) => Ok(json!({"RequestBars":value})),
            _ => bail!("unsupported facade data command"),
        }
    }
    fn decode_data(value: &Value) -> Result<DataEvent> {
        if let Some(value) = value.get("Instrument") {
            return Ok(DataEvent::Instrument(serde_json::from_value(
                value.clone(),
            )?));
        }
        if let Some(value) = value.get("BarsResponse") {
            return Ok(DataEvent::Response(DataResponse::Bars(
                serde_json::from_value(value.clone())?,
            )));
        }
        if let Some(value) = value.get("Quote") {
            return Ok(DataEvent::Data(Data::Quote(serde_json::from_value(
                value.clone(),
            )?)));
        }
        if let Some(value) = value.get("Trade") {
            return Ok(DataEvent::Data(Data::Trade(serde_json::from_value(
                value.clone(),
            )?)));
        }
        if let Some(value) = value.get("Bar") {
            return Ok(DataEvent::Data(Data::Bar(serde_json::from_value(
                value.clone(),
            )?)));
        }
        bail!("unknown original facade data event")
    }
    fn decode_command(value: &Value) -> Result<DataCommand> {
        if let Some(value) = value.get("Subscribe") {
            return Ok(DataCommand::Subscribe(serde_json::from_value(
                value.clone(),
            )?));
        }
        if let Some(value) = value.get("Unsubscribe") {
            return Ok(DataCommand::Unsubscribe(serde_json::from_value(
                value.clone(),
            )?));
        }
        if let Some(value) = value.get("RequestBars") {
            return Ok(DataCommand::Request(RequestCommand::Bars(
                serde_json::from_value(value.clone())?,
            )));
        }
        bail!("unknown original facade data command")
    }
    #[derive(Debug)]
    struct Codec(RunnerRecoveryChannel);
    impl RunnerRecoveryCodec for Codec {
        fn channel(&self) -> RunnerRecoveryChannel {
            self.0
        }
        fn codec_id(&self) -> &'static str {
            "actual_native_facade_history.v1"
        }
        fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<Value> {
            match event {
                RunnerRecoveryEventRef::DataEvent(event) => data_payload(event),
                RunnerRecoveryEventRef::DataCommand(command) => command_payload(command),
                _ => bail!("unsupported facade physical channel"),
            }
        }
        fn decode(&self, envelope: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            match self.0 {
                RunnerRecoveryChannel::DataEvent => Ok(RunnerRecoveryEvent::DataEvent(
                    decode_data(&envelope.payload)?,
                )),
                RunnerRecoveryChannel::DataCommand => Ok(RunnerRecoveryEvent::DataCommand(
                    decode_command(&envelope.payload)?,
                )),
                _ => bail!("unsupported facade channel"),
            }
        }
    }
    fn registry() -> Result<Rc<RunnerRecoveryCodecRegistry>> {
        let channels = [
            RunnerRecoveryChannel::DataEvent,
            RunnerRecoveryChannel::DataCommand,
        ];
        let mut value = RunnerRecoveryCodecRegistry::new(channels);
        for channel in channels {
            value.register(Codec(channel))?;
        }
        Ok(Rc::new(value.seal()?))
    }
    fn node(
        journal: PathBuf,
        parent: Option<&Source>,
        provider: &Rc<RefCell<ProviderState>>,
        scenario: Scenario,
    ) -> Result<LiveNode> {
        let parent = parent.map(|source| {
            (
                source.journal.clone(),
                source.instance,
                source.first.cut.source.journal_run.clone(),
                source.end,
                source.fingerprint.clone(),
            )
        });
        let config = EventStoreConfig {
            base_dir: journal,
            ..Default::default()
        };
        let mut node = LiveNode::builder(TraderId::from("FACADE-TRADER"), Environment::Live)?
            .with_name("native-facade-history")
            .with_load_state(false)
            .with_reconciliation(false)
            .with_delay_post_stop_secs(u64::from(scenario == Scenario::Late))
            .with_delay_shutdown_secs(0)
            .with_event_store(move |instance, clock| {
                let mut store = EventStoreLifecycle::boot(Some(config), instance, clock)?;
                if let Some((directory, instance, run, end, fingerprint)) = parent {
                    store.bind_external_parent(&directory, instance, &run, end, &fingerprint)?;
                }
                Ok(Box::new(store))
            })
            .add_data_client(
                Some("FACADE-DATA".into()),
                Box::new(FixtureFactory(Rc::clone(provider), true)),
                Box::new(FixtureConfig),
            )?
            .build()?;
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("actual-native-facade-history".into(), |_| Ok(()))?,
            |source, phase, input| {
                let payload = if let Some(value) = input.downcast_ref::<NativeMutationInput>() {
                    value.canonical_payload()?
                } else if let Some(value) = input.downcast_ref::<DataEvent>() {
                    data_payload(value)?
                } else if let Some(value) = input.downcast_ref::<DataCommand>() {
                    command_payload(value)?
                } else {
                    bail!("unsupported external input in actual facade Journal");
                };
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload,
                    batch_index: None,
                })
            },
        ))?;
        Ok(node)
    }
    fn account() -> AccountState {
        let total = Money::from("1_000_000 USDT");
        let zero = Money::from("0 USDT");
        AccountState {
            account_id: AccountId::from("BINANCE-FIXTURE"),
            account_type: AccountType::Margin,
            base_currency: None,
            balances: vec![AccountBalance::new(total, zero, total)],
            margins: vec![],
            is_reported: false,
            event_id: UUID4::new(),
            ts_event: 0.into(),
            ts_init: 0.into(),
            info: None,
        }
    }
    fn cache(instrument: &InstrumentAny, account: &AccountState) -> Result<Cache> {
        let mut cache = Cache::default();
        cache.add_instrument(instrument.clone())?;
        cache.add_account(AccountAny::Margin(MarginAccount::new(
            account.clone(),
            true,
        )))?;
        Ok(cache)
    }
    fn response(request: &RequestBars) -> Result<BarsResponse> {
        let end = u64::try_from(
            request
                .end
                .context("original request end absent")?
                .as_nanosecond(),
        )?;
        let event = end / 1_000_000_000 * 1_000_000_000;
        let bar = Bar::new(
            request.bar_type,
            Price::from("1000.00"),
            Price::from("1001.00"),
            Price::from("999.00"),
            Price::from("1000.00"),
            Quantity::from("1.000"),
            event.into(),
            end.into(),
        );
        let start = request
            .start
            .map(|value| u64::try_from(value.as_nanosecond()))
            .transpose()?
            .map(Into::into);
        Ok(BarsResponse::new(
            request.request_id,
            ClientId::from("FACADE-DATA"),
            request.bar_type,
            vec![bar],
            start,
            Some(end.into()),
            end.into(),
            None,
        ))
    }
    async fn source(args: &[String], scenario: Scenario) -> Result<()> {
        let provider = Rc::new(RefCell::new(ProviderState::default()));
        let records = Rc::new(RefCell::new(Vec::new()));
        let journal = std::env::current_dir()?.join("journal-source");
        let mut node = node(journal.clone(), None, &provider, scenario)?;
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let account = account();
        {
            let mut cache = node.kernel().cache.borrow_mut();
            cache.add_instrument(instrument.clone())?;
            cache.add_account(AccountAny::Margin(MarginAccount::new(
                account.clone(),
                true,
            )))?;
        }
        node.add_strategy(construct_strategy(args, &records)?)?;
        let instance = node.instance_id();
        let trace = node.prepare_owned_native_trace(
            "actual-native-facade-history".into(),
            native_inventory_digest(&serde_json::to_value(node.config())?)?,
            "actual_native_facade_history.v1".into(),
            "actual_native_facade_component.v1".into(),
            |_, _, _| Ok(Vec::new()),
            |_| Ok(()),
        )?;
        let cuts = Rc::new(RefCell::new(Vec::<Cut>::new()));
        let captured = Rc::clone(&cuts);
        node.set_running_checkpoint_handler(
            registry()?,
            RunningCheckpointSchedule::Requested,
            move |boundary| {
                boundary.verify()?;
                let cut = boundary
                    .native_trace_cut()
                    .context("facade native cut absent")?
                    .clone();
                boundary
                    .persist_native_checkpoint(json!({"schema":"ActualNativeFacadeHost.v1"}))?;
                captured.borrow_mut().push(Cut {
                    cut,
                    inventory: serde_json::to_value(boundary.inventory())?,
                    components: boundary.components().clone(),
                });
                Ok(())
            },
            |()| Ok(()),
            |_| {},
        )?;
        let handle = node.handle();
        let input_instrument = instrument.clone();
        let drive = async {
            for _ in 0..1000 {
                if provider.borrow().requests.len() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            ensure!(
                provider.borrow().requests.len() == 1,
                "native pending request did not reach provider"
            );
            handle.request_running_checkpoint();
            let sender = get_data_event_sender();
            sender.send(DataEvent::Instrument(input_instrument))?;
            for _ in 0..1000 {
                if !cuts.borrow().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            ensure!(
                !cuts.borrow().is_empty(),
                "actual pending-request source cut absent"
            );
            let request: RequestBars =
                serde_json::from_value(provider.borrow().requests[0].clone())?;
            let mut original_response = response(&request)?;
            if scenario == Scenario::ForeignClient {
                original_response.client_id = ClientId::from("FOREIGN-DATA");
            }
            if scenario == Scenario::ForeignCorrelation {
                original_response.correlation_id = UUID4::new();
            }
            if scenario == Scenario::Oversized {
                original_response.data.push(original_response.data[0]);
            }
            if scenario != Scenario::Late {
                sender.send(DataEvent::Response(DataResponse::Bars(
                    original_response.clone(),
                )))?;
                if scenario == Scenario::Duplicate {
                    sender.send(DataEvent::Response(DataResponse::Bars(
                        original_response.clone(),
                    )))?;
                }
            }
            let now = nautilus_core::time::nanos_since_unix_epoch();
            let id = instrument.id();
            let bar: BarType = BAR.parse()?;
            sender.send(DataEvent::Data(Data::Quote(QuoteTick::new(
                id,
                Price::from("999.99"),
                Price::from("1000.01"),
                Quantity::from("1.000"),
                Quantity::from("1.000"),
                now.into(),
                now.into(),
            ))))?;
            sender.send(DataEvent::Data(Data::Trade(TradeTick::new(
                id,
                Price::from("1000.00"),
                Quantity::from("1.000"),
                AggressorSide::Buy,
                TradeId::from("FACADE-HISTORY-1"),
                now.into(),
                now.into(),
            ))))?;
            sender.send(DataEvent::Data(Data::Bar(Bar::new(
                bar,
                Price::from("1000.00"),
                Price::from("1001.00"),
                Price::from("999.00"),
                Price::from("1000.00"),
                Quantity::from("1.000"),
                now.into(),
                now.into(),
            ))))?;
            tokio::time::sleep(Duration::from_millis(200)).await;
            handle.stop();
            if scenario == Scenario::Late {
                tokio::time::sleep(Duration::from_millis(50)).await;
                sender.send(DataEvent::Response(DataResponse::Bars(original_response)))?;
            }
            Ok::<(), anyhow::Error>(())
        };
        let (run, drive) = tokio::join!(node.run_with_mode(NodeRunMode::Hosted), drive);
        original(
            "facade-history-source-attempt.original.json",
            &json!({"run_error":run.as_ref().err().map(|error|format!("{error:#}")),
            "drive_error":drive.as_ref().err().map(|error|format!("{error:#}")),"provider":*provider.borrow(),"captured_cuts":*cuts.borrow()}),
        )?;
        run?;
        drive?;
        let first = cuts.borrow().first().context("source cut absent")?.clone();
        let guest: Value = serde_json::from_slice(
            &first.components.strategies[&StrategyId::from("FACADE-001")]["plugin.state"],
        )?;
        ensure!(
            guest["history_request_id"].is_string()
                && guest["historical_bars"] == 0
                && first.inventory["native_pending_responses"]
                    .as_array()
                    .is_some_and(|value| value.len() == 1)
                && first.cut.pending_inputs.len() == 1
                && first.cut.pending_inputs[0].payload == json!({"Instrument":instrument}),
            "source cut does not own the original pending history"
        );
        let actor = try_get_actor_unchecked::<PluginStrategy>(&"FACADE-001".into())
            .context("source facade owner absent")?;
        let failure = actor
            .callback_failure()
            .map(|(code, message)| json!({"code":format!("{code:?}"),"message":message}));
        ensure!(
            failure.is_some() == scenario.fault(),
            "source callback fault boundary differs from the actual case"
        );
        if scenario.fault() {
            ensure!(
                failure.as_ref().unwrap()["code"] == "InvalidArgument",
                "source lost the first route failure"
            );
        }
        drop(actor);
        let final_states = Trader::collect_native_recovery_state(node.kernel().trader())?;
        let identity = trace.source().context("native facade source absent")?;
        node.dispose();
        drop(node);
        let reader = EventStoreReader::new(RedbBackend::open_sealed(
            &journal,
            &instance.to_string(),
            &identity.journal_run,
        )?);
        let end = reader.high_watermark()?;
        let verified = reader.verify_native_tail(&identity, &first.cut, end)?;
        ensure!(
            !verified.roots().is_empty() && verified.final_cut().is_ok(),
            "actual facade tail not sealed"
        );
        let source = Source {
            schema: "ActualNativeFacadeHistorySource.v1".into(),
            scenario,
            failure,
            pid: std::process::id(),
            fingerprint: EventStoreLifecycle::sealed_run_fingerprint(
                &journal,
                instance,
                &identity.journal_run,
                end,
            )?,
            journal,
            instance,
            first,
            end,
            instrument,
            account,
            final_states,
            provider: serde_json::to_value(&*provider.borrow())?,
        };
        original("facade-history-source.original.json", &source)?;
        println!(
            "{}",
            json!({"schema":"NativeFacadeHistoryProbe.v1","mode":"source","scenario":scenario,"pid":source.pid,
            "native_roots":verified.roots().len(),"owned_pending_responses":1,"financial_requests":0,"production_acceptance":false})
        );
        Ok(())
    }
    fn restore(args: &[String], original_path: &str) -> Result<()> {
        let source: Source = serde_json::from_slice(&std::fs::read(original_path)?)?;
        ensure!(
            source.schema == "ActualNativeFacadeHistorySource.v1"
                && source.pid != std::process::id(),
            "native facade requires another original process"
        );
        let reader = EventStoreReader::new(RedbBackend::open_sealed(
            &source.journal,
            &source.instance.to_string(),
            &source.first.cut.source.journal_run,
        )?);
        ensure!(reader.high_watermark()? == source.end, "source end changed");
        let verified =
            reader.verify_native_tail(&source.first.cut.source, &source.first.cut, source.end)?;
        let provider = Rc::new(RefCell::new(ProviderState::default()));
        let records = Rc::new(RefCell::new(Vec::new()));
        let mut node = node(
            std::env::current_dir()?.join("journal-target"),
            Some(&source),
            &provider,
            source.scenario,
        )?;
        let gate = TransmissionGate::default();
        node.handle().bind_transmission_gate(gate.clone())?;
        gate.revoke();
        node.add_strategy(construct_strategy(args, &records)?)?;
        let outcome = (|| -> Result<Value> {
            node.restore_native_cache(cache(&source.instrument, &source.account)?)?;
            node.restore_native_component_state(&source.first.components, &verified)?;
            node.kernel()
                .risk_engine()
                .borrow_mut()
                .set_trading_state(TradingState::Halted);
            node.kernel_mut().open_event_store_for_paused_recovery()?;
            let watermark = RunnerRecoveryWatermark {
                recovery_id: "actual-native-facade-cut".into(),
                checkpoint_sequence: source.first.cut.prefix.sequence,
                dispatch_watermark: source.first.cut.last_input_sequence,
            };
            let codecs = registry()?;
            node.replay_recovery_events(&watermark, &[], &codecs, |_, _| Ok(()))?;
            node.restore_native_market_checkpoint(&verified, &watermark)?;
            node.restore_registered_engine_checkpoint(
                &source.first.inventory["execution_manager"],
                &source.first.inventory["data_engine"],
                source.first.cut.captured_at_ns,
                &watermark,
            )?;
            let adapters: BTreeMap<String, Value> =
                serde_json::from_value(source.first.inventory["adapters"].clone())?;
            let clients: BTreeMap<String, Value> =
                serde_json::from_value(source.first.inventory["data_client_state"].clone())?;
            node.restore_registered_adapter_checkpoint(&adapters, &clients, &watermark)?;
            node.restore_registered_portfolio_checkpoint(
                &source.first.cut.native_effects["portfolio"],
                &watermark,
            )?;
            node.restore_registered_timer_checkpoint(
                source.first.cut.registered_timers.clone(),
                Vec::new(),
                &watermark,
            )?;
            let replay = node.replay_native_tail(
                &verified,
                &watermark,
                |_, begin| {
                    let NativeTraceRecord::Begin {
                        input_source,
                        payload,
                        ..
                    } = begin
                    else {
                        bail!("actual Begin absent");
                    };
                    match input_source {
                        NativeInputSource::DataEvent => {
                            Ok(RunnerRecoveryEvent::DataEvent(decode_data(payload)?))
                        }
                        NativeInputSource::DataCommand => {
                            Ok(RunnerRecoveryEvent::DataCommand(decode_command(payload)?))
                        }
                        _ => bail!("unsupported original facade root"),
                    }
                },
                |_, pending| {
                    ensure!(
                        pending.receipt.input_source == NativeInputSource::DataEvent,
                        "retained facade physical input changed channel"
                    );
                    Ok(RunnerRecoveryEvent::DataEvent(decode_data(
                        &pending.payload,
                    )?))
                },
            )?;
            let states = Trader::collect_native_recovery_state(node.kernel().trader())?;
            ensure!(
                states == source.final_states,
                "native facade complete original private state differs"
            );
            ensure!(
                !gate.capture().can_transmit()
                    && records.borrow().is_empty()
                    && provider.borrow().requests.is_empty(),
                "historical restore reissued work or acquired financial authority"
            );
            let actor = try_get_actor_unchecked::<PluginStrategy>(&"FACADE-001".into())
                .context("restored facade owner absent")?;
            let failure = actor
                .callback_failure()
                .map(|(code, message)| json!({"code":format!("{code:?}"),"message":message}));
            ensure!(
                failure == source.failure,
                "original first callback failure changed"
            );
            drop(actor);
            let state = states.strategies[&StrategyId::from("FACADE-001")]
                .get("plugin.state")
                .map(|raw| serde_json::from_slice::<Value>(raw))
                .transpose()?;
            if !source.scenario.fault() {
                let state = state
                    .as_ref()
                    .context("healthy restored guest state absent")?;
                let pending = source.scenario == Scenario::ForeignCorrelation;
                ensure!(
                    state["history_request_id"].is_null() != pending
                        && state["historical_bars"] == u64::from(!pending)
                        && state["quotes"] == 1
                        && state["trades"] == 1
                        && state["bars"] == 1,
                    "original native response was lost, completed without correlation, or applied twice"
                );
            }
            Ok(
                json!({"schema":"NativeFacadeHistoryProbe.v1","mode":"restore","pid":std::process::id(),"source_pid":source.pid,"scenario":source.scenario,"first_callback_failure":failure,
                "native_roots":verified.roots().len(),"final_root":replay.final_root(),"complete_original_state_equal":true,
                "original_pending_request_id_restored":true,"historical_request_reissued":false,"current_process_transmission_veto_closed":true,
                "state":state,"financial_requests":0,"real_venue":false,"production_acceptance":false,
                "scope":"flat fixture client with no sockets or HTTP tasks; real OKX pending request restoration remains unqualified"}),
            )
        })();
        let retained = node
            .kernel_mut()
            .prohibit_event_store_seal("native facade fixture has no current execution grant");
        node.dispose();
        original(
            "facade-history-restore-attempt.original.json",
            &json!({"error":outcome.as_ref().err().map(|error|format!("{error:#}")),"retain_error":retained.as_ref().err().map(|error|format!("{error:#}"))}),
        )?;
        retained?;
        let result = outcome?;
        original("facade-history-restored.original.json", &result)?;
        println!("{result}");
        Ok(())
    }
    pub(super) fn run(args: &[String]) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        match args.first().map(String::as_str) {
            Some(mode) if mode.starts_with("history-source") && args.len() == 5 => {
                let scenario = match mode {
                    "history-source" | "history-source-normal" => Scenario::Normal,
                    "history-source-duplicate" => Scenario::Duplicate,
                    "history-source-late" => Scenario::Late,
                    "history-source-foreign-client" => Scenario::ForeignClient,
                    "history-source-oversized" => Scenario::Oversized,
                    "history-source-foreign-correlation" => Scenario::ForeignCorrelation,
                    _ => bail!("unknown actual source scenario"),
                };
                runtime.block_on(source(&args[1..], scenario))
            }
            Some("history-restore") if args.len() == 6 => {
                runtime.block_on(async { restore(&args[1..5], &args[5]) })
            }
            _ => bail!(
                "expected history-source plus four loader arguments, or history-restore plus source receipt"
            ),
        }
    }
}
