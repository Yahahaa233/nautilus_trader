// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Actual single-node managed submission probe; no exchange or economic ledger.
#[cfg(any(
    all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod supported {
    use anyhow::{Context, Result, bail, ensure};
    use nautilus_common::{
        actor::{DataActor, registry::try_get_actor_unchecked},
        component::Component,
        enums::Environment,
        messages::execution::TradingCommand,
    };
    use nautilus_live::node::{LiveNode, NodeRunMode};
    use nautilus_model::{
        enums::{ContingencyType, OrderSide, TimeInForce, TriggerType},
        identifiers::{AccountId, ClientId, StrategyId, TraderId},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::{Price, Quantity},
    };
    use nautilus_plugin::{
        PluginError,
        strategy::{
            instance::{CallbackLimits, StateValidator},
            library_loader::LibraryRegistry,
        },
    };
    use nautilus_trading::{
        plugin::{
            PluginOrderType, PluginScope, PluginStrategy,
            managed::{
                ManagedBracketRequest, ManagedOrderPlan, ManagedPolicyIdentity,
                ManagedSubmissionContext, ManagedSubmissionOutcome, ManagedSubmissionPolicy,
            },
        },
        strategy::{PreparedSubmission, StrategyConfig, StrategyNative, SubmissionInterceptor},
    };
    use serde_json::{Value, json};
    use std::{cell::RefCell, collections::BTreeSet, fs::File, rc::Rc, time::Duration};
    const DESCRIPTOR: &str = include_str!("../../plugin/examples/contract_managed.descriptor.json");
    #[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Captured {
        prepared: Vec<Value>,
        finished: Vec<Value>,
        commands: Vec<Value>,
        batches: Vec<Value>,
        batch_outcomes: Vec<Value>,
    }
    #[derive(Debug)]
    struct Policy {
        case: String,
        captured: Rc<RefCell<Captured>>,
    }
    #[derive(Debug)]
    struct BadDrop;
    impl Drop for BadDrop {
        fn drop(&mut self) {
            panic!("managed original payload destructor panic");
        }
    }
    fn original_prepare_panic() -> ! {
        panic!("original native managed preparation panic");
    }
    fn original_payload_drop_panic() -> ! {
        std::panic::panic_any(BadDrop);
    }
    impl ManagedSubmissionPolicy for Policy {
        fn prepare(
            &self,
            context: &ManagedSubmissionContext,
            raw: &[u8],
        ) -> Result<ManagedOrderPlan> {
            let request: Value = serde_json::from_slice(raw)?;
            ensure!(
                request["intent"] == json!({"kind":"entry"}),
                "private managed fixture input changed"
            );
            self.captured
                .borrow_mut()
                .prepared
                .push(serde_json::to_value(context)?);
            match self.case.as_str() {
                "preparation-refused" => bail!("original business preparation refused"),
                "panic" => original_prepare_panic(),
                "batch-panic" if self.captured.borrow().prepared.len() == 2 => {
                    original_prepare_panic()
                }
                "panic-payload-drop" => original_payload_drop_panic(),
                _ => {}
            }
            let entry = nautilus_trading::plugin::wire::SubmitRequest {
                schema_version: 2,
                order_type: nautilus_trading::plugin::wire::NativeOrderType::Market,
                instrument_id: if self.case == "foreign-scope" {
                    "BTCUSDT-PERP.BINANCE".into()
                } else {
                    context.instrument_id
                },
                side: OrderSide::Buy,
                quantity: Quantity::from(if self.case == "invalid-quantity" {
                    "0.000"
                } else {
                    "0.001"
                }),
                price: None,
                trigger_price: None,
                trigger_type: None,
                time_in_force: TimeInForce::Gtc,
                expire_time_ns: None,
                reduce_only: false,
                post_only: false,
                position_id: None,
            };
            if self.case == "bracket" {
                use nautilus_trading::plugin::wire::NativeOrderType;
                let mut take_profit = entry.clone();
                take_profit.side = OrderSide::Sell;
                take_profit.order_type = NativeOrderType::Limit;
                take_profit.price = Some(Price::from("3000.00"));
                take_profit.reduce_only = true;
                let mut stop_loss = entry.clone();
                stop_loss.side = OrderSide::Sell;
                stop_loss.order_type = NativeOrderType::StopMarket;
                stop_loss.trigger_price = Some(Price::from("1000.00"));
                stop_loss.trigger_type = Some(TriggerType::LastPrice);
                stop_loss.reduce_only = true;
                return Ok(ManagedOrderPlan::Bracket(Box::new(ManagedBracketRequest {
                    entry,
                    take_profit,
                    stop_loss,
                    contingency: ContingencyType::Ouo,
                })));
            }
            Ok(ManagedOrderPlan::Single(entry))
        }
        fn prepare_batch(
            &self,
            contexts: &[ManagedSubmissionContext],
            original: &[u8],
        ) -> Result<Vec<ManagedOrderPlan>> {
            let request: Value = serde_json::from_slice(original)?;
            self.captured
                .borrow_mut()
                .batches
                .push(json!({"contexts":contexts,"original":request}));
            let inputs = request["intents"]
                .as_array()
                .context("private batch absent")?;
            ensure!(
                inputs.len() == contexts.len(),
                "private batch identity changed"
            );
            if self.case.starts_with("batch-frame") {
                let context = contexts
                    .first()
                    .context("private frame has no original native owner")?;
                let name = nautilus_trading::plugin::managed_frame_signal_name(context.strategy_id);
                let signal=nautilus_common::signal::Signal::new(name.as_str().into(),
                    json!({"schema_version":1,"frame_owner":context.strategy_id,"frame_sequence":1}).to_string(),
                    nautilus_core::UnixNanos::from(context.timestamp_ns),
                    nautilus_core::UnixNanos::from(context.timestamp_ns));
                let data_type = nautilus_model::data::DataType::new(
                    &format!(
                        "Signal{}",
                        nautilus_core::string::conversions::title_case(&name)
                    ),
                    None,
                    None,
                );
                let data =
                    nautilus_model::data::CustomData::new(std::sync::Arc::new(signal), data_type);
                // The native callback is deliberately published while this same
                // guest's original timer and batch host call are still active.
                let topic = nautilus_common::msgbus::switchboard::get_custom_topic(&data.data_type);
                nautilus_common::msgbus::publish_any(topic, &data);
            }
            contexts
                .iter()
                .zip(inputs)
                .map(|(context, input)| self.prepare(context, &serde_json::to_vec(input)?))
                .collect()
        }
        fn finish_batch(
            &self,
            contexts: &[ManagedSubmissionContext],
            outcomes: &[ManagedSubmissionOutcome],
            original: &[u8],
        ) -> Result<()> {
            self.captured
                .borrow_mut()
                .batch_outcomes
                .push(json!({"contexts":contexts,"outcomes":outcomes}));
            // Private fixture has no economic ledger; records every actual outcome.
            ensure!(
                contexts.len() == outcomes.len(),
                "private batch outcome frame changed"
            );
            for (context, outcome) in contexts.iter().zip(outcomes) {
                self.finish(context, outcome, original)?;
            }
            Ok(())
        }
        fn finish(
            &self,
            context: &ManagedSubmissionContext,
            outcome: &ManagedSubmissionOutcome,
            _original_request: &[u8],
        ) -> Result<()> {
            self.captured
                .borrow_mut()
                .finished
                .push(json!({"context":context,"outcome":outcome}));
            if self.case == "finish-panic" {
                original_prepare_panic();
            }
            ensure!(
                self.case != "finish-failed",
                "original managed durability refusal"
            );
            Ok(())
        }
    }
    #[derive(Debug)]
    struct NoGrant {
        captured: Rc<RefCell<Captured>>,
        panic: bool,
        batch_panic: bool,
        cache_probe: bool,
    }
    struct RecordedOnly;
    impl PreparedSubmission for RecordedOnly {
        fn bind(self: Box<Self>, _: &TradingCommand) -> Result<()> {
            Ok(())
        }
    }
    impl SubmissionInterceptor for NoGrant {
        fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
            self.captured
                .borrow_mut()
                .commands
                .push(serde_json::to_value(command)?);
            if self.panic || (self.batch_panic && self.captured.borrow().commands.len() == 2) {
                original_prepare_panic();
            }
            if self.cache_probe {
                return Ok(Box::new(RecordedOnly));
            }
            bail!("managed_probe_no_financial_grant")
        }
    }
    #[derive(Debug)]
    struct Contract;
    impl StateValidator for Contract {
        fn validate(&self, version: u32, raw: &[u8]) -> Result<(), PluginError> {
            let refuse = || PluginError::generic("managed fixture state contract refused");
            let value: Value = serde_json::from_slice(raw).map_err(|_| refuse())?;
            let fields = value.as_object().ok_or_else(refuse)?;
            if version != 1
                || fields.len() != 4
                || !["attempted", "result", "error", "secondary"]
                    .iter()
                    .all(|key| fields.contains_key(*key))
                || !value["attempted"].is_boolean()
                || ["result", "error", "secondary"]
                    .iter()
                    .any(|key| !value[key].is_null() && !value[key].is_object())
            {
                return Err(refuse());
            }
            Ok(())
        }
    }
    fn hash(raw: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(raw)
            .iter()
            .map(|value| format!("{value:02x}"))
            .collect()
    }
    fn construct_strategy(
        args: &[String],
        case: &str,
        captured: &Rc<RefCell<Captured>>,
    ) -> Result<PluginStrategy> {
        ensure!(args[1].len() == 64, "invalid library digest");
        let mut digest = [0; 32];
        for (i, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&args[1][i * 2..i * 2 + 2], 16)?;
        }
        let mut registry = LibraryRegistry::new(
            1,
            64 * 1024 * 1024,
            #[cfg(target_os = "macos")]
            std::path::Path::new(&args[2]),
        )
        .map_err(|error| anyhow::anyhow!("{}", error.message_string()))?;
        // SAFETY: fixed diagnostic publisher, hash and private non-financial build.
        let library =
            unsafe { registry.load_admitted(&File::open(&args[0])?, digest, args[3].parse()?) }
                .map_err(|error| anyhow::anyhow!("{}", error.message_string()))?;
        let configuration: &[u8] = if case == "batch-frame-failure" {
            b"managed-frame-fault-v1"
        } else if case == "batch-empty" {
            b"managed-empty-batch-v1"
        } else if case.starts_with("batch-") {
            b"managed-batch-v1"
        } else if case == "bracket" {
            b"managed-bracket-v1"
        } else {
            b"managed-v1"
        };
        let guest = library
            .create_instance(
                DESCRIPTOR.as_bytes(),
                configuration,
                CallbackLimits {
                    configuration_bytes: 1024,
                    state_bytes: 16384,
                    event_bytes: 16384,
                },
                Box::new(Contract),
            )
            .map_err(|error| anyhow::anyhow!("{}", error.message_string()))?;
        drop(registry);
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let id = instrument.id();
        let mut strategy = PluginStrategy::new(
            StrategyConfig {
                strategy_id: Some(StrategyId::from("MANAGED-001")),
                manage_order_lists_locally: true,
                manage_contingent_orders: true,
                ..Default::default()
            },
            guest,
            PluginScope {
                client_id: ClientId::from("MANAGED-EXEC"),
                data_client_id: ClientId::from("MANAGED-DATA"),
                account_id: AccountId::from("BINANCE-MANAGED"),
                instruments: BTreeSet::from([id]),
                bar_types: Vec::new(),
                order_types: BTreeSet::from([
                    PluginOrderType::Market,
                    PluginOrderType::Limit,
                    PluginOrderType::StopMarket,
                ]),
                max_timers: 1,
                max_history_bars: 0,
                allow_history_requests: false,
                quote_instruments: BTreeSet::new(),
                trade_instruments: BTreeSet::new(),
                state_schema_version: 1,
                descriptor_sha256: hash(DESCRIPTOR.as_bytes()),
                configuration_sha256: hash(configuration),
            },
        )?;
        if case != "missing-policy" {
            strategy.set_managed_submission_policy(
                ManagedPolicyIdentity {
                    max_batch_size: if case.starts_with("batch-") { 2 } else { 0 },
                    policy_id: "private_fixed_no_ledger".into(),
                    schema_version: 1,
                    configuration_sha256: hash(case.as_bytes()),
                },
                Rc::new(Policy {
                    case: case.to_owned(),
                    captured: Rc::clone(captured),
                }),
            )?;
        }
        strategy
            .strategy_core_mut()
            .set_submission_interceptor(Rc::new(NoGrant {
                captured: Rc::clone(captured),
                panic: case == "native-panic",
                batch_panic: case == "batch-native-panic",
                cache_probe: case == "native-cache-denied",
            }))?;
        #[cfg(feature = "native-tail-replay")]
        strategy.set_historical_verifier(Rc::new(
            nautilus_live::node::plugin_recovery::ReaderVerifiedPluginRecovery::default(),
        ))?;
        Ok(strategy)
    }

    #[cfg(feature = "native-tail-replay")]
    include!("strategy_managed_host/history.rs");

    pub(super) fn run() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        #[cfg(feature = "native-tail-replay")]
        if args
            .first()
            .is_some_and(|value| value.starts_with("history-"))
        {
            return history::run(&args);
        }

        ensure!(
            args.len() == 5,
            "expected library hash backing publisher case"
        );
        let case = args[4].clone();
        ensure!(
            [
                "native-rejected",
                "native-cache-denied",
                "preparation-refused",
                "foreign-scope",
                "invalid-quantity",
                "panic",
                "panic-payload-drop",
                "finish-failed",
                "missing-policy",
                "native-panic",
                "finish-panic"
            ]
            .contains(&case.as_str()),
            "unknown managed fixture case"
        );
        let captured = Rc::new(RefCell::new(Captured::default()));
        let strategy = construct_strategy(&args, &case, &captured)?;
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
   let mut node=LiveNode::builder(TraderId::from("MANAGED-TRADER"),Environment::Live)?.with_reconciliation(false).with_delay_post_stop_secs(0).with_delay_shutdown_secs(0).build()?;
   node.kernel().cache.borrow_mut().add_instrument(instrument)?;node.kernel().risk_engine().borrow_mut().set_trading_state(nautilus_model::enums::TradingState::Halted);node.add_strategy(strategy)?;
   let handle=node.handle();let healthy_feedback=Rc::new(RefCell::new(None));let drive=async {let result=async {
    let mut completed=false;
    for _ in 0..1000 {
     if let Some(actor)=try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()) {
      completed=actor.callback_failure().is_some() || actor.on_save().ok().and_then(|state|state.get("plugin.state").and_then(|raw|serde_json::from_slice::<Value>(raw).ok())).is_some_and(|state|state["attempted"]==true && (case!="native-cache-denied" || state["secondary"]["order_events"].as_array().is_some_and(|events|events.len()>=2)));
     }
     if completed{break;}tokio::time::sleep(Duration::from_millis(5)).await;
    }
    if completed && case=="native-cache-denied" {
     let actor=try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()).context("positive probe actor absent")?;
     let saved=actor.on_save()?;let state:Value=serde_json::from_slice(&saved["plugin.state"])?;
     ensure!(actor.state()==nautilus_common::enums::ComponentState::Running && actor.callback_failure().is_none(),"native component faulted during actual order feedback");
     let events=state["secondary"]["order_events"].as_array().context("actual order feedback absent")?;
     let original=&state["result"]["client_order_id"];
     ensure!(original.is_string() && events.len()>=2 && events.iter().all(|event|event.as_object().is_some_and(|value|value.len()==1 && value.values().all(|value|&value["client_order_id"]==original))),"actual native callback identity changed");
     *healthy_feedback.borrow_mut()=Some(json!({"registered_actor_order_callbacks":events.len(),"native_component_running":true,"same_original_native_id":true,"original_client_order_id":original,"events":events}));
    }
    handle.stop();ensure!(completed,"managed native timer did not complete");Ok::<(),anyhow::Error>(())
   }.await;handle.stop();result};
   let (run,drive)=tokio::join!(node.run_with_mode(NodeRunMode::Hosted),drive);
   let actor=try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()).context("native managed component absent")?;
   let failure=actor.callback_failure().map(|(code,message)|json!({"code":format!("{code:?}"),"message":message}));let saved=actor.on_save();
   let state=saved.as_ref().ok().and_then(|value|value.get("plugin.state")).map(|raw|serde_json::from_slice::<Value>(raw)).transpose()?;
   let captured=captured.borrow();
   println!("{}",json!({"schema":"NativeManagedAttempt.v1","case":case,"pid":std::process::id(),"failure":failure,"state":state,"prepared":captured.prepared,"finished":captured.finished,"native_commands":captured.commands,
    "run_error":run.as_ref().err().map(|error|format!("{error:#}")),"drive_error":drive.as_ref().err().map(|error|format!("{error:#}")),"financial_requests":0,"production_acceptance":false}));
   run?;drive?;saved?;
   let fatal=matches!(case.as_str(),"panic"|"panic-payload-drop"|"finish-failed"|"native-panic"|"finish-panic");ensure!(failure.is_some()==fatal,"managed native terminal failure differs");
   let expected_commands=usize::from(matches!(case.as_str(),"native-rejected"|"native-cache-denied"|"finish-failed"|"missing-policy"|"native-panic"|"finish-panic"));ensure!(captured.commands.len()==expected_commands,"guest continued host effects after a fatal native failure");
   if fatal {ensure!(failure.as_ref().unwrap()["code"]==if case=="finish-failed"{"SerializationFailed"}else{"Panic"},"original fatal class changed");}
   else {
    let state=state.as_ref().context("healthy managed guest state absent")?;ensure!(state["attempted"]==true,"managed event not applied");
    if case=="missing-policy" {ensure!(state["error"]["code"]=="Generic" && captured.prepared.is_empty() && captured.finished.is_empty(),"missing policy created application preparation");}
    else {ensure!(state["result"]["native_outcome"]["status"]==if case=="preparation-refused"{"preparation_refused"}else if case=="native-cache-denied"{"native_accepted"}else{"native_rejected"},"unsubmitted outcome changed");}
   }
   if case!="missing-policy" {
    ensure!(captured.finished.len()==1,"the same business owner missed the original native outcome");
    if matches!(case.as_str(),"panic"|"panic-payload-drop"|"native-panic") {
     ensure!(captured.finished[0]["outcome"]["status"]=="native_unknown","panic was misclassified as conclusive non-submission");
    }
   }
   for command in &captured.commands {
    ensure!(command["order_init"]["quantity"]=="0.001" && command["client_id"]=="MANAGED-EXEC","native identity or approved quantity differs");
    if case!="missing-policy" {ensure!(command["client_order_id"]==captured.prepared[0]["client_order_id"],"native factory replaced the one application request ID");}
   }
   ensure!(node.kernel().cache.borrow().orders_total_count(None,None,None,None,None)==usize::from(case=="native-cache-denied"),"probe registered an unauthorized order");
   println!("{}",json!({"schema":"NativeManagedProbe.v1","case":case,"pid":std::process::id(),"failure":failure,"state":state,
    "prepared":captured.prepared,"finished":captured.finished,"native_commands":captured.commands,"one_native_request_id":if case=="missing-policy"{None}else{Some(true)},"single_native_live_node":true,"financial_requests":0,
    "real_venue":false,"economic_ledger_proven":false,"production_acceptance":false,"healthy_feedback":*healthy_feedback.borrow()}));drop(actor);drop(captured);node.dispose();Ok(())
  })
    }
}
fn main() -> anyhow::Result<()> {
    #[cfg(any(
        all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    ))]
    {
        supported::run()
    }
    #[cfg(not(any(
        all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"),
        all(target_os = "macos", target_arch = "aarch64")
    )))]
    {
        anyhow::bail!("unsupported native managed probe platform")
    }
}
