mod history {
    // -------------------------------------------------------------------------------------------------
    //  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
    //  https://nautechsystems.io
    //
    //  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
    //  You may not use this file except in compliance with the License.
    //  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
    // -------------------------------------------------------------------------------------------------

    // A real flat, non-financial Journal/cut probe. Source and restore run in
    // different processes; no JSON object can replace the sealed Journal reader.
    use std::{cell::RefCell, fs::OpenOptions, io::Write, path::PathBuf, rc::Rc, time::Duration};

    use anyhow::{Context, Result, bail, ensure};
    use nautilus_common::{
        actor::registry::try_get_actor_unchecked, cache::Cache, component::Component,
        enums::Environment, recovery_trace::NativeTraceRecord, runner::TimeEventMessage,
    };
    use nautilus_core::{UUID4, transmission::TransmissionGate};
    use nautilus_event_store::{
        EventStoreReader,
        backend::RedbBackend,
        kernel::EventStoreLifecycle,
        native_trace::{NativeTraceCheckpointCut, VerifiedNativeTrace, native_inventory_digest},
    };
    use nautilus_live::{
        dispatch::{DispatchInput, DispatchObserver},
        node::{
            LiveNode, NativeMutationInput, NodeDispatchObserver, NodeRunMode,
            RunningCheckpointSchedule,
        },
        runner_recovery::{
            RunnerRecoveryChannel, RunnerRecoveryCodec, RunnerRecoveryCodecRegistry,
            RunnerRecoveryEnvelope, RunnerRecoveryEvent, RunnerRecoveryEventRef,
            RunnerRecoveryWatermark,
        },
    };
    use nautilus_model::{
        enums::TradingState,
        identifiers::TraderId,
        instruments::{InstrumentAny, stubs::crypto_perpetual_ethusdt},
    };
    use nautilus_plugin::PluginErrorCode;
    use nautilus_system::{
        event_store::EventStoreConfig,
        trader::{CollectedComponentState, Trader},
    };
    use nautilus_trading::plugin::PluginStrategy;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    enum HistoryMode {
        Normal,
        CacheDenied,
        EventFailure,
        EventPanic,
        BatchNormal,
        BatchEmpty,
        BatchPanic,
        BatchNativePanic,
        Bracket,
    }
    impl HistoryMode {
        fn case(self) -> &'static str {
            match self {
                Self::Normal => "native-rejected",
                Self::CacheDenied => "native-cache-denied",
                Self::EventFailure => "finish-failed",
                Self::EventPanic => "panic",
                Self::BatchNormal => "batch-normal",
                Self::BatchEmpty => "batch-empty",
                Self::BatchPanic => "batch-panic",
                Self::BatchNativePanic => "batch-native-panic",
                Self::Bracket => "bracket",
            }
        }
        fn denials(self) -> usize {
            match self {
                Self::EventPanic | Self::BatchPanic | Self::BatchEmpty => 0,
                Self::BatchNormal | Self::BatchNativePanic => 2,
                _ => 1,
            }
        }
        fn failure(self) -> Option<PluginErrorCode> {
            match self {
                Self::Normal | Self::CacheDenied | Self::BatchNormal | Self::BatchEmpty | Self::Bracket => None,
                Self::EventFailure => Some(PluginErrorCode::SerializationFailed),
                Self::EventPanic | Self::BatchPanic | Self::BatchNativePanic => Some(PluginErrorCode::Panic),
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct CapturedCut {
        cut: NativeTraceCheckpointCut,
        inventory: Value,
        components: CollectedComponentState,
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SourceReceipt {
        schema: String,
        pid: u32,
        journal: PathBuf,
        instance: UUID4,
        first: CapturedCut,
        end: u64,
        sealed_fingerprint: String,
        instrument: InstrumentAny,
        final_states: CollectedComponentState,
        effects: Captured,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<HistoryMode>,
    }
    impl SourceReceipt {
        fn managed_mode(&self) -> Result<HistoryMode> {
            match self.schema.as_str() {
                "ActualManagedNativeHistorySource.v1" => {
                    ensure!(
                        self.mode.is_none(),
                        "legacy source has foreign mode metadata"
                    );
                    Ok(HistoryMode::Normal)
                }
                "ActualManagedNativeHistorySource.v2" => {
                    self.mode.context("source mode is missing")
                }
                _ => bail!("unknown original native history receipt"),
            }
        }
    }

    fn original_json(name: &str, value: &impl Serialize) -> Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).open(name)?;
        file.write_all(&serde_json::to_vec_pretty(value)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        std::fs::File::open(".")?.sync_all()?;
        Ok(())
    }

    fn time_payload(message: &TimeEventMessage) -> Result<Value> {
        let binding = message.checkpoint_callback_binding();
        ensure!(
            binding["owner_thread_matches"] == true
                && matches!(
                    binding["kind"].as_str(),
                    Some("registered_owner_thread" | "registered_cleanup")
                )
                && binding["binding_id"].as_u64().is_some_and(|id| id > 0),
            "history probe timer has no original owner-bound callback"
        );
        let event = message.event();
        Ok(
            json!({"name":event.name.to_string(),"event_id":event.event_id,
        "ts_event":event.ts_event,"ts_init":event.ts_init,
        "callback":message.native_input_callback_binding()}),
        )
    }

    #[derive(Debug)]
    struct TimerCodec;
    impl RunnerRecoveryCodec for TimerCodec {
        fn channel(&self) -> RunnerRecoveryChannel {
            RunnerRecoveryChannel::TimeEvent
        }
        fn codec_id(&self) -> &'static str {
            "actual_managed_history_timer.v1"
        }
        fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<Value> {
            if let RunnerRecoveryEventRef::TimeEvent(message) = event {
                return time_payload(message);
            }
            bail!("history probe has no external venue queue input")
        }
        fn decode(&self, _: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            bail!("original timers require the native owner-bound decoder")
        }
    }

    fn command_payload(command: &nautilus_common::runner::TradingCommandMessage) -> Value {
        json!({"endpoint":command.endpoint().to_string(),"command":command.command()})
    }

    #[derive(Debug)]
    struct CommandCodec;
    impl RunnerRecoveryCodec for CommandCodec {
        fn channel(&self) -> RunnerRecoveryChannel { RunnerRecoveryChannel::ExecutionCommand }
        fn codec_id(&self) -> &'static str { "actual_managed_history_command.v1" }
        fn encode(&self, event: RunnerRecoveryEventRef<'_>) -> Result<Value> {
            if let RunnerRecoveryEventRef::ExecutionCommand(value) = event { return Ok(command_payload(value)); }
            bail!("managed history command codec received another channel")
        }
        fn decode(&self, input: &RunnerRecoveryEnvelope) -> Result<RunnerRecoveryEvent> {
            Ok(RunnerRecoveryEvent::ExecutionCommand(nautilus_common::runner::TradingCommandMessage::new(
                input.payload["endpoint"].as_str().context("original command endpoint absent")?.into(),
                serde_json::from_value(input.payload["command"].clone())?,
            )))
        }
    }

    fn registry() -> Result<Rc<RunnerRecoveryCodecRegistry>> {
        let mut registry = RunnerRecoveryCodecRegistry::new([RunnerRecoveryChannel::TimeEvent, RunnerRecoveryChannel::ExecutionCommand]);
        registry.register_owner_bound_timer_codec(TimerCodec)?;
        registry.register(CommandCodec)?;
        Ok(Rc::new(registry.seal()?))
    }

    fn node(journal: PathBuf, parent: Option<&SourceReceipt>) -> Result<LiveNode> {
        let parent = parent.map(|source| {
            (
                source.journal.clone(),
                source.instance,
                source.first.cut.source.journal_run.clone(),
                source.end,
                source.sealed_fingerprint.clone(),
            )
        });
        let config = EventStoreConfig {
            base_dir: journal,
            ..Default::default()
        };
        let mut node = LiveNode::builder(TraderId::from("MANAGED-TRADER"), Environment::Live)?
            .with_name("native-plugin-history")
            .with_load_state(false)
            .with_reconciliation(false)
            .with_delay_post_stop_secs(0)
            .with_delay_shutdown_secs(0)
            .with_event_store(move |instance, clock| {
                let mut store = EventStoreLifecycle::boot(Some(config), instance, clock)?;
                if let Some((directory, original_instance, run, end, fingerprint)) = parent {
                    store.bind_external_parent(
                        &directory,
                        original_instance,
                        &run,
                        end,
                        &fingerprint,
                    )?;
                }
                Ok(Box::new(store))
            })
            .build()?;
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("actual-managed-history".into(), |_| Ok(()))?,
            |source, phase, input| {
                let payload = if let Some(value) = input.downcast_ref::<NativeMutationInput>() {
                    value.canonical_payload()?
                } else if let Some(value) = input.downcast_ref::<TimeEventMessage>() {
                    time_payload(value)?
                } else if let Some(value) = input.downcast_ref::<nautilus_common::runner::TradingCommandMessage>() {
                    command_payload(value)
                } else {
                    bail!("unexpected external input in non-financial plugin history probe")
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

    fn install(
        node: &mut LiveNode,
        args: &[String],
        records: &Rc<RefCell<Captured>>,
        mode: HistoryMode,
    ) -> Result<()> {
        let case = if args[4] == "wrong-policy" {
            "preparation-refused"
        } else {
            mode.case()
        };
        node.add_strategy(construct_strategy(args, case, records)?)?;
        if mode == HistoryMode::CacheDenied {
            // Cache feedback only: no account, clients, funds or transmission.
            node.kernel().risk_engine().borrow_mut().set_trading_state(TradingState::Halted);
        }
        Ok(())
    }

    type Captures = Rc<RefCell<Vec<CapturedCut>>>;

    fn register_capture(node: &mut LiveNode) -> Result<Captures> {
        let cuts = Rc::new(RefCell::new(Vec::<CapturedCut>::new()));
        let saved = Rc::clone(&cuts);
        node.set_running_checkpoint_handler(
            registry()?,
            RunningCheckpointSchedule::EveryCompletedRoot,
            move |boundary| {
                boundary.verify()?;
                let cut = boundary
                    .native_trace_cut()
                    .context("actual native trace cut absent")?
                    .clone();
                boundary.persist_native_checkpoint(
                    json!({"schema":"ActualPluginHistoryHostCheckpoint.v1"}),
                )?;
                saved.borrow_mut().push(CapturedCut {
                    cut,
                    inventory: serde_json::to_value(boundary.inventory())?,
                    components: boundary.components().clone(),
                });
                Ok(())
            },
            |()| Ok(()),
            |_| {},
        )?;
        Ok(cuts)
    }

    async fn source(args: &[String], mode: HistoryMode) -> Result<()> {
        let journal = std::env::current_dir()?.join("journal-source");
        let mut node = node(journal.clone(), None)?;
        let records = Rc::new(RefCell::new(Captured::default()));
        install(&mut node, args, &records, mode)?;
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        node.kernel()
            .cache()
            .borrow_mut()
            .add_instrument(instrument.clone())?;
        let instance = node.instance_id();
        let trace = node.prepare_owned_native_trace(
            "actual-managed-history-run".into(),
            native_inventory_digest(&serde_json::to_value(node.config())?)?,
            "actual_managed_history_timer.v1".into(),
            "actual_managed_component.v1".into(),
            |_, _, _| Ok(Vec::new()),
            |_| Ok(()),
        )?;
        let cuts = register_capture(&mut node)?;
        let handle = node.handle();
        let recorded = Rc::clone(&records);
        let observed = Rc::clone(&cuts);
        let control = async move {
            for _ in 0..2000 {
                let expected_prepared = match mode {
                    HistoryMode::BatchNormal | HistoryMode::BatchPanic | HistoryMode::BatchNativePanic => 2,
                    HistoryMode::BatchEmpty => 0,
                    _ => 1,
                };
                let frame_complete = mode != HistoryMode::BatchEmpty ||
                    (recorded.borrow().batches.len()==1 && recorded.borrow().batch_outcomes.len()==1);
                let feedback_complete = if mode == HistoryMode::CacheDenied {
                    if let Some(actor) = try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()) {
                    actor.callback_failure().is_none() && actor.state() == nautilus_common::enums::ComponentState::Running
                        && actor.on_save().ok().and_then(|state|state.get("plugin.state").and_then(|raw|serde_json::from_slice::<Value>(raw).ok()))
                            .is_some_and(|state|state["secondary"]["order_events"].as_array().is_some_and(|events| events.len()>=2))
                    } else { false }
                } else { true };
                if frame_complete && feedback_complete && recorded.borrow().prepared.len() == expected_prepared
                    && recorded.borrow().commands.len() == mode.denials()
                    && observed.borrow().len() >= 3
                {
                    handle.stop();
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            handle.stop();
            bail!("actual history source callbacks/cuts did not complete")
        };
        let (result, control) = tokio::join!(node.run_with_mode(NodeRunMode::Hosted), control);
        result?;
        control?;
        let first = cuts
            .borrow()
            .first()
            .context("original initial cut absent")?
            .clone();
        let initial: Value = serde_json::from_slice(
            &first.components.strategies
                [&nautilus_model::identifiers::StrategyId::from("MANAGED-001")]["plugin.state"],
        )?;
        ensure!(
            initial["attempted"] == false && initial["result"].is_null(),
            "source initial cut did not precede actual guest callbacks"
        );
        ensure!(
            first.cut.pending_inputs.is_empty(),
            "this source probe requires an empty initial physical FIFO"
        );
        let final_states = Trader::collect_native_recovery_state(node.kernel().trader())?;
        let identity = trace.source().context("actual trace source absent")?;
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
            "actual source lacks its sealed final cut"
        );
        let receipt = SourceReceipt {
            schema: "ActualManagedNativeHistorySource.v2".into(),
            pid: std::process::id(),
            journal: journal.clone(),
            instance,
            first,
            end,
            sealed_fingerprint: EventStoreLifecycle::sealed_run_fingerprint(
                &journal,
                instance,
                &identity.journal_run,
                end,
            )?,
            instrument,
            final_states,
            effects: records.borrow().clone(),
            mode: Some(mode),
        };
        original_json("managed-history-source.original.json", &receipt)?;
        println!(
            "{}",
            json!({"schema":"NativeManagedHistoryProbe.v1","mode":"source","pid":receipt.pid,
        "native_roots":verified.roots().len(),"native_denials":receipt.effects.commands.len(),"managed_mode":mode,"financial_requests":0,
        "production_acceptance":false,"execution_authorized":false})
        );
        Ok(())
    }

    fn restore(args: &[String], original: &str) -> Result<()> {
        let source: SourceReceipt = serde_json::from_slice(&std::fs::read(original)?)?;
        source.managed_mode()?;
        ensure!(
            source.pid != std::process::id(),
            "native history restore requires the original different source process"
        );
        let reader = EventStoreReader::new(RedbBackend::open_sealed(
            &source.journal,
            &source.instance.to_string(),
            &source.first.cut.source.journal_run,
        )?);
        ensure!(
            reader.high_watermark()? == source.end,
            "original source journal end changed"
        );
        let verified =
            reader.verify_native_tail(&source.first.cut.source, &source.first.cut, source.end)?;
        let mut node = node(
            std::env::current_dir()?.join("journal-target"),
            Some(&source),
        )?;
        let records = Rc::new(RefCell::new(Captured::default()));
        let gate = TransmissionGate::default();
        let restored = restore_node(&mut node, args, &source, &verified, &records, &gate);
        let retained = node
            .kernel_mut()
            .prohibit_event_store_seal("native history fixture has no current execution grant");
        node.dispose();
        original_json(
            "managed-history-restore-attempt.original.json",
            &json!({
                "restore_error":restored.as_ref().err().map(|error| format!("{error:#}")),
                "retain_unsealed_error":retained.as_ref().err().map(|error| format!("{error:#}"))
            }),
        )?;
        let result = restored?;
        retained?;
        original_json("managed-history-restored.original.json", &result)?;
        println!("{result}");
        Ok(())
    }

    fn restore_node(
        node: &mut LiveNode,
        args: &[String],
        source: &SourceReceipt,
        verified: &VerifiedNativeTrace,
        records: &Rc<RefCell<Captured>>,
        gate: &TransmissionGate,
    ) -> Result<Value> {
        let prior = gate.capture();
        node.handle().bind_transmission_gate(gate.clone())?;
        gate.revoke();
        ensure!(
            !prior.can_transmit() && !gate.capture().can_transmit(),
            "target process transmission veto was not closed before replay"
        );
        let mode = source.managed_mode()?;
        install(node, args, records, mode)?;
        let mut cache = Cache::default();
        cache.add_instrument(source.instrument.clone())?;
        node.restore_native_cache(cache)?;
        node.restore_native_component_state(&source.first.components, verified)?;
        node.kernel()
            .risk_engine()
            .borrow_mut()
            .set_trading_state(TradingState::Halted);
        node.kernel_mut().open_event_store_for_paused_recovery()?;
        let watermark = RunnerRecoveryWatermark {
            recovery_id: "actual-managed-history-cut".into(),
            checkpoint_sequence: source.first.cut.prefix.sequence,
            dispatch_watermark: source.first.cut.last_input_sequence,
        };
        let codecs = registry()?;
        node.replay_recovery_events(&watermark, &[], &codecs, |_, _| Ok(()))?;
        node.restore_native_market_checkpoint(verified, &watermark)?;
        node.restore_registered_engine_checkpoint(
            &source.first.inventory["execution_manager"],
            &source.first.inventory["data_engine"],
            source.first.cut.captured_at_ns,
            &watermark,
        )?;
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
            verified,
            &watermark,
            |_, _: &NativeTraceRecord| bail!("history probe contains no external venue input"),
            |_, _| bail!("history probe contains no retained external input"),
        )?;
        let states = Trader::collect_native_recovery_state(node.kernel().trader())?;
        ensure!(
            states == source.final_states,
            "complete original guest/native private state differs"
        );
        ensure!(
            *records.borrow() == source.effects,
            "original decisions, UUIDs, clocks or routing changed"
        );
        let actor = try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into())
            .context("actual restored managed guest absent")?;
        ensure!(
            actor.callback_failure().map(|(code, _)| code) == mode.failure()
                && actor.is_start_inhibited() == mode.failure().is_some()
                && !actor.is_faulted(),
            "original guest fault or physical recovery lifecycle changed"
        );
        drop(actor);
        ensure!(
            node.kernel().risk_engine().borrow().trading_state() == TradingState::Halted
                && !prior.can_transmit()
                && !gate.capture().can_transmit(),
            "historical restore acquired current permission"
        );
        let result = json!({"schema":"NativeManagedHistoryProbe.v1","mode":"restore","pid":std::process::id(),
        "source_pid":source.pid,"native_roots":verified.roots().len(),"final_root":replay.final_root(),
        "complete_original_state_equal":true,"original_decisions_and_uuid_clock_routing_equal":true,
        "native_submission_refusals":records.borrow().commands.len(),"financial_requests":0,"execution_authorized":false,
        "managed_mode":mode,"managed_start_inhibited":mode.failure().is_some(),
        "current_process_transmission_veto_closed":true,"terminal_failure_fence":node.kernel().exec_engine().borrow().submissions_fenced(),
        "production_acceptance":false,"scope":"single managed fixture policy; no actual account, economic ledger or venue qualification"});
        Ok(result)
    }

    pub(super) fn run(args: &[String]) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        ensure!(
            args.len() == 6 || args.len() == 7,
            "expected history source/restore, four loader arguments, policy case and optional source receipt"
        );
        let mode = match args[5].as_str() {
            "native-rejected" | "wrong-policy" => HistoryMode::Normal,
            "native-cache-denied" => HistoryMode::CacheDenied,
            "finish-failed" => HistoryMode::EventFailure,
            "panic" => HistoryMode::EventPanic,
            "batch-normal" => HistoryMode::BatchNormal,
            "batch-empty" => HistoryMode::BatchEmpty,
            "batch-panic" => HistoryMode::BatchPanic,
            "batch-native-panic" => HistoryMode::BatchNativePanic,
            "bracket" => HistoryMode::Bracket,
            _ => bail!("unsupported managed history case"),
        };
        match args[0].as_str() {
            "history-source" if args.len() == 6 && args[5] != "wrong-policy" => {
                runtime.block_on(source(&args[1..6], mode))
            }
            "history-restore" if args.len() == 7 => {
                runtime.block_on(async { restore(&args[1..6], &args[6]) })
            }
            _ => bail!("invalid managed history invocation"),
        }
    }
}
