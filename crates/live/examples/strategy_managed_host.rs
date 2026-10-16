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
        enums::Environment,
        messages::execution::TradingCommand,
    };
    use nautilus_live::node::{LiveNode, NodeRunMode};
    use nautilus_model::{
        enums::{OrderSide, TimeInForce},
        identifiers::{AccountId, ClientId, StrategyId, TraderId},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::Quantity,
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
                ManagedPolicyIdentity, ManagedSubmissionContext, ManagedSubmissionOutcome,
                ManagedSubmissionPolicy,
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
        ) -> Result<nautilus_trading::plugin::wire::SubmitRequest> {
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
                "panic-payload-drop" => original_payload_drop_panic(),
                _ => {}
            }
            Ok(nautilus_trading::plugin::wire::SubmitRequest {
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
            })
        }
        fn finish(
            &self,
            context: &ManagedSubmissionContext,
            outcome: &ManagedSubmissionOutcome,
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
    }
    impl SubmissionInterceptor for NoGrant {
        fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
            self.captured
                .borrow_mut()
                .commands
                .push(serde_json::to_value(command)?);
            if self.panic {
                original_prepare_panic();
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
        let guest = library
            .create_instance(
                DESCRIPTOR.as_bytes(),
                b"managed-v1",
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
                ..Default::default()
            },
            guest,
            PluginScope {
                client_id: ClientId::from("MANAGED-EXEC"),
                data_client_id: ClientId::from("MANAGED-DATA"),
                account_id: AccountId::from("BINANCE-MANAGED"),
                instruments: BTreeSet::from([id]),
                bar_types: Vec::new(),
                order_types: BTreeSet::from([PluginOrderType::Market]),
                max_timers: 1,
                max_history_bars: 0,
                quote_instruments: BTreeSet::new(),
                trade_instruments: BTreeSet::new(),
                state_schema_version: 1,
                descriptor_sha256: hash(DESCRIPTOR.as_bytes()),
                configuration_sha256: hash(b"managed-v1"),
            },
        )?;
        if case != "missing-policy" {
            strategy.set_managed_submission_policy(
                ManagedPolicyIdentity {
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
   node.kernel().cache.borrow_mut().add_instrument(instrument)?;node.add_strategy(strategy)?;
   let handle=node.handle();let drive=async {
    let mut completed=false;
    for _ in 0..1000 {
     if let Some(actor)=try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()) {
      completed=actor.callback_failure().is_some() || actor.on_save().ok().and_then(|state|state.get("plugin.state").and_then(|raw|serde_json::from_slice::<Value>(raw).ok())).is_some_and(|state|state["attempted"]==true);
     }
     if completed{break;}tokio::time::sleep(Duration::from_millis(5)).await;
    }
    handle.stop();ensure!(completed,"managed native timer did not complete");Ok::<(),anyhow::Error>(())
   };
   let (run,drive)=tokio::join!(node.run_with_mode(NodeRunMode::Hosted),drive);
   let actor=try_get_actor_unchecked::<PluginStrategy>(&"MANAGED-001".into()).context("native managed component absent")?;
   let failure=actor.callback_failure().map(|(code,message)|json!({"code":format!("{code:?}"),"message":message}));let saved=actor.on_save();
   let state=saved.as_ref().ok().and_then(|value|value.get("plugin.state")).map(|raw|serde_json::from_slice::<Value>(raw)).transpose()?;
   let captured=captured.borrow();
   println!("{}",json!({"schema":"NativeManagedAttempt.v1","case":case,"pid":std::process::id(),"failure":failure,"state":state,"prepared":captured.prepared,"finished":captured.finished,"native_commands":captured.commands,
    "run_error":run.as_ref().err().map(|error|format!("{error:#}")),"drive_error":drive.as_ref().err().map(|error|format!("{error:#}")),"financial_requests":0,"production_acceptance":false}));
   run?;drive?;saved?;
   let fatal=matches!(case.as_str(),"panic"|"panic-payload-drop"|"finish-failed"|"native-panic"|"finish-panic");ensure!(failure.is_some()==fatal,"managed native terminal failure differs");
   let expected_commands=usize::from(matches!(case.as_str(),"native-rejected"|"finish-failed"|"missing-policy"|"native-panic"|"finish-panic"));ensure!(captured.commands.len()==expected_commands,"guest continued host effects after a fatal native failure");
   if fatal {ensure!(failure.as_ref().unwrap()["code"]==if case=="finish-failed"{"SerializationFailed"}else{"Panic"},"original fatal class changed");}
   else {
    let state=state.as_ref().context("healthy managed guest state absent")?;ensure!(state["attempted"]==true,"managed event not applied");
    if case=="missing-policy" {ensure!(state["error"]["code"]=="Generic" && captured.prepared.is_empty() && captured.finished.is_empty(),"missing policy created application preparation");}
    else {ensure!(state["result"]["native_outcome"]["status"]==if case=="preparation-refused"{"preparation_refused"}else{"native_rejected"},"unsubmitted outcome changed");}
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
   ensure!(node.kernel().cache.borrow().orders_total_count(None,None,None,None,None)==0,"probe registered an unauthorized order");
   println!("{}",json!({"schema":"NativeManagedProbe.v1","case":case,"pid":std::process::id(),"failure":failure,"state":state,
    "prepared":captured.prepared,"finished":captured.finished,"native_commands":captured.commands,"one_native_request_id":if case=="missing-policy"{None}else{Some(true)},"single_native_live_node":true,"financial_requests":0,
    "real_venue":false,"economic_ledger_proven":false,"production_acceptance":false}));drop(actor);drop(captured);node.dispose();Ok(())
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
