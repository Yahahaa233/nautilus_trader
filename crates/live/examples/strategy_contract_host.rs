// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Private, non-financial GNU component conformance driver. Two independent
//! cdylibs share one actual LiveNode, native clocks and submission interception.
//! This is not a Metis production qualification, account or execution grant.

#[cfg(all(
    target_os = "linux",
    target_env = "gnu",
    target_arch = "x86_64",
    feature = "native-tail-replay"
))]
#[path = "strategy_contract_host/history.rs"]
mod history;

#[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
mod gnu {
    use anyhow::{Context, Result, bail, ensure};
    use indexmap::IndexMap;
    use nautilus_common::{
        actor::{DataActor, DataActorNative, registry::try_get_actor_unchecked},
        component::Component,
        enums::Environment,
        messages::execution::TradingCommand,
    };
    use nautilus_live::node::{LiveNode, NodeRunMode};
    use nautilus_model::identifiers::{AccountId, ClientId, InstrumentId, StrategyId, TraderId};
    use nautilus_plugin::strategy::{
        instance::{CallbackLimits, StateValidator},
        linux_loader::{LibraryRegistry, LoadedLibrary},
    };
    use nautilus_trading::{
        plugin::{PluginOrderType, PluginScope, PluginStrategy},
        strategy::{
            PreparedSubmission, Strategy, StrategyConfig, StrategyNative, SubmissionInterceptor,
        },
    };
    use serde_json::{Value, json};
    use std::{cell::RefCell, collections::BTreeSet, fs::File, path::Path, rc::Rc, time::Duration};

    const COUNTER_DESCRIPTOR: &str =
        include_str!("../../plugin/examples/contract_counter.descriptor.json");
    const INTERVAL_DESCRIPTOR: &str =
        include_str!("../../plugin/examples/contract_interval.descriptor.json");

    #[derive(Debug)]
    struct FixtureStateValidator {
        counter: bool,
        reject: bool,
    }

    impl StateValidator for FixtureStateValidator {
        fn validate(&self, version: u32, raw: &[u8]) -> Result<(), nautilus_plugin::PluginError> {
            if self.reject {
                return Err(nautilus_plugin::PluginError::new(
                    nautilus_plugin::PluginErrorCode::SerializationFailed,
                    "contract_counter_original_state_refusal",
                ));
            }
            let refuse =
                || nautilus_plugin::PluginError::generic("native fixture state contract refused");
            let value: Value = serde_json::from_slice(raw).map_err(|_| refuse())?;
            let fields = value.as_object().ok_or_else(refuse)?;
            let names = if self.counter {
                ["events", "denied", "pending"]
            } else {
                ["scheduled_at", "elapsed_ns", "denied"]
            };
            if version != 1
                || fields.len() != 3
                || names.iter().any(|key| !fields.contains_key(*key))
            {
                return Err(refuse());
            }
            for name in names {
                if self.counter && name == "pending" {
                    if !value[name].is_boolean() {
                        return Err(refuse());
                    }
                } else if !value[name].is_u64() {
                    return Err(refuse());
                }
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoGrant {
        strategy: StrategyId,
        client: ClientId,
        records: Rc<RefCell<Vec<Value>>>,
    }
    impl SubmissionInterceptor for NoGrant {
        fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
            let TradingCommand::SubmitOrder(command) = command else {
                bail!("unexpected contract probe command");
            };
            ensure!(
                command.strategy_id == self.strategy && command.client_id == Some(self.client),
                "contract probe native account/strategy route changed"
            );
            ensure!(
                command.instrument_id == InstrumentId::from("ETHUSDT-PERP.BINANCE"),
                "contract probe instrument changed"
            );
            self.records
                .borrow_mut()
                .push(serde_json::to_value(command)?);
            bail!("contract_probe_no_grant")
        }
    }

    fn hash(bytes: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    fn decode_hash(value: &str) -> Result<[u8; 32]> {
        ensure!(
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "expected a canonical SHA-256"
        );
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
        }
        Ok(bytes)
    }

    fn snapshot(id: &str) -> Result<IndexMap<String, Vec<u8>>> {
        let actor = try_get_actor_unchecked::<PluginStrategy>(&id.into())
            .context("actual plugin actor missing")?;
        actor.on_save()
    }

    fn component_state(snapshot: &Value) -> &Value {
        if snapshot["schema"] == "NativeStrategyFrameworkProbe.v1" {
            &snapshot["components"]
        } else {
            snapshot
        }
    }

    fn load_original_component(
        actor: &mut PluginStrategy,
        id: &str,
        snapshot: &Value,
    ) -> Result<()> {
        let state = serde_json::from_value(component_state(snapshot)[id].clone())?;
        if snapshot["schema"] == "NativeStrategyFrameworkProbe.v1" {
            let owner = &snapshot["owners"][id];
            let receipt = actor
                .strategy_core_mut()
                .restore_native_framework_checkpoint(&owner["framework"], &owner["timers"])?;
            Strategy::on_native_framework_checkpoint_load(actor, state, &receipt)?;
        } else {
            actor.on_load(state)?;
        }
        Ok(())
    }

    fn capture_original_framework() -> Result<Value> {
        let mut owners = serde_json::Map::new();
        let mut components = serde_json::Map::new();
        for id in ["COUNTER-001", "INTERVAL-002"] {
            let actor = try_get_actor_unchecked::<PluginStrategy>(&id.into())
                .context("actual framework owner missing")?;
            let clock = DataActorNative::clock_rc(&*actor);
            let frozen = clock.borrow().freeze_running_timer_checkpoint()?;
            let framework = actor.strategy_core().native_framework_checkpoint()?;
            let state = Strategy::on_native_framework_checkpoint(&*actor, &framework)?;
            framework.verify_timer_inventory(frozen.inventory())?;
            owners.insert(
                id.into(),
                json!({"framework":framework.value()?,"timers":frozen.inventory()}),
            );
            components.insert(id.into(), serde_json::to_value(state)?);
            frozen.verify()?;
            frozen.finish_terminal()?;
        }
        let snapshot = json!({"schema":"NativeStrategyFrameworkProbe.v1","owners":owners,"components":components});
        std::fs::write(
            "component-framework-state.original.json",
            serde_json::to_vec(&snapshot)?,
        )?;
        Ok(snapshot)
    }

    pub(super) fn install_guests(
        node: &mut LiveNode,
        counter: LoadedLibrary,
        interval: LoadedLibrary,
        counter_configuration: &[u8],
        records: &Rc<RefCell<Vec<Value>>>,
        restored: Option<&Value>,
        reject_counter_state: bool,
    ) -> Result<()> {
        for (id, client, library, descriptor, configuration) in [
            (
                "COUNTER-001",
                "CONTRACT-A",
                counter,
                COUNTER_DESCRIPTOR,
                counter_configuration,
            ),
            (
                "INTERVAL-002",
                "CONTRACT-B",
                interval,
                INTERVAL_DESCRIPTOR,
                b"interval-v1".as_slice(),
            ),
        ] {
            let guest = library
                .create_instance(
                    descriptor.as_bytes(),
                    configuration,
                    CallbackLimits {
                        configuration_bytes: 128,
                        state_bytes: 4096,
                        event_bytes: 4096,
                    },
                    Box::new(FixtureStateValidator {
                        counter: id == "COUNTER-001",
                        reject: id == "COUNTER-001" && reject_counter_state,
                    }),
                )
                .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
            let strategy_id = StrategyId::from(id);
            let client_id = ClientId::from(client);
            let mut strategy = PluginStrategy::new(
                StrategyConfig {
                    strategy_id: Some(strategy_id),
                    ..Default::default()
                },
                guest,
                PluginScope {
                    client_id,
                    account_id: AccountId::from("BINANCE-CONTRACT"),
                    instruments: BTreeSet::from([InstrumentId::from("ETHUSDT-PERP.BINANCE")]),
                    bar_types: Vec::new(),
                    order_types: BTreeSet::from([PluginOrderType::Market]),
                    max_timers: 1,
                    state_schema_version: 1,
                    descriptor_sha256: hash(descriptor.as_bytes()),
                    configuration_sha256: hash(configuration),
                },
            )?;
            #[cfg(feature = "native-tail-replay")]
            strategy.set_historical_verifier(Rc::new(
                nautilus_live::node::plugin_recovery::ReaderVerifiedPluginRecovery::default(),
            ))?;
            strategy
                .strategy_core_mut()
                .set_submission_interceptor(Rc::new(NoGrant {
                    strategy: strategy_id,
                    client: client_id,
                    records: Rc::clone(records),
                }))?;
            node.add_strategy(strategy)?;
            if let Some(state) = &restored {
                let mut actor = try_get_actor_unchecked::<PluginStrategy>(&id.into())
                    .context("actual restore target absent")?;
                load_original_component(&mut actor, id, state)?;
                let inhibited = actor.is_start_inhibited();
                drop(actor);
                if inhibited {
                    ensure!(
                        node.kernel()
                            .trader()
                            .borrow()
                            .start_strategy(&strategy_id)
                            .is_err(),
                        "explicit native startup bypassed a terminal checkpoint failure"
                    );
                }
            }
        }
        Ok(())
    }

    fn verify_fault(
        node: &LiveNode,
        mode: &str,
        restored: bool,
        records: &[Value],
    ) -> Result<Value> {
        let mut actor = try_get_actor_unchecked::<PluginStrategy>(&"COUNTER-001".into())
            .context("faulted actual plugin actor missing")?;
        // In the Save-contract case this first call must retain a terminal
        // checkpoint immediately, without another callback or collection retry.
        let failure_state = actor.on_save()?;
        let native_faulted = actor.is_faulted();
        ensure!(
            if restored {
                actor.is_start_inhibited() && !actor.is_running()
            } else if mode == "--reject-save" {
                actor.is_start_inhibited()
            } else {
                actor.is_faulted()
            },
            "original or restored terminal failure did not keep its actual native actor inactive"
        );
        let start_inhibited = actor.is_start_inhibited();
        ensure!(
            start_inhibited,
            "terminal failure did not inhibit native startup"
        );
        let first = actor
            .callback_failure()
            .context("original guest fault not retained")?;
        let expected = match mode {
            "--panic-event" => "contract_counter_original_event_panic",
            "--reject-save" => "contract_counter_original_state_refusal",
            _ => "contract_counter_original_event_error",
        };
        ensure!(
            first.1.contains(expected),
            "original callback fault replaced"
        );
        ensure!(
            failure_state.contains_key("plugin.failure")
                && !failure_state.contains_key("plugin.state"),
            "terminal failure masquerades as resumable algorithm state"
        );
        let failure: Value = serde_json::from_slice(&failure_state["plugin.failure"])?;
        ensure!(
            failure["message"]
                .as_str()
                .is_some_and(|message| message.contains(expected)),
            "terminal checkpoint lost original failure"
        );
        ensure!(
            DataActor::on_resume(&mut *actor)
                .unwrap_err()
                .to_string()
                .contains(expected),
            "faulted guest resumed"
        );
        ensure!(
            actor.callback_failure() == Some(first.clone()),
            "subsequent operation replaced first failure"
        );
        drop(actor);
        let interval_actor = try_get_actor_unchecked::<PluginStrategy>(&"INTERVAL-002".into())
            .context("healthy sibling missing")?;
        ensure!(
            !interval_actor.is_faulted() && interval_actor.callback_failure().is_none(),
            "A failure was attributed to B"
        );
        let interval_saved = interval_actor.on_save()?;
        let interval_state: Value = serde_json::from_slice(&interval_saved["plugin.state"])?;
        ensure!(
            interval_state["denied"] == 1
                && if restored {
                    records.is_empty()
                } else {
                    records.len() == if mode == "--reject-save" { 2 } else { 1 }
                        && records
                            .iter()
                            .any(|record| record["client_id"] == "CONTRACT-B")
                },
            "healthy B did not complete its actual callback after A failure"
        );
        drop(interval_actor);
        let collected =
            nautilus_system::trader::Trader::collect_component_state(node.kernel().trader())?;
        ensure!(
            collected.strategies.len() == 2,
            "common native component collection lost a strategy"
        );
        std::fs::write(
            "component-failure-state.original.json",
            serde_json::to_vec(&collected.strategies)?,
        )?;
        Ok(
            json!({"schema":"NativeStrategyContractProbe.v1","pid":std::process::id(),"single_live_node":true,
                    "mode":mode,"counter_failure_code":format!("{:?}",first.0),"counter_first_failure":first.1,"counter_native_faulted":native_faulted,"counter_start_inhibited":start_inhibited,"restored_terminal_failure":restored,"common_component_checkpoint_complete":true,
                    "interval_state":interval_state,"healthy_sibling_faulted":false,"actual_native_denials":records,
                    "registry_released_before_callbacks":true,"execution_authorized":false,"financial_requests":0,"algorithm_state_only":true,"production_acceptance":false}),
        )
    }

    fn verify_saved_state(
        counter: LoadedLibrary,
        interval: LoadedLibrary,
        restored: Option<&Value>,
        records: &[Value],
    ) -> Result<Value> {
        let saved = json!({"COUNTER-001":snapshot("COUNTER-001")?,"INTERVAL-002":snapshot("INTERVAL-002")?});
        let decode = |id: &str| -> Result<Value> {
            let state: IndexMap<String, Vec<u8>> = serde_json::from_value(saved[id].clone())?;
            Ok(serde_json::from_slice(&state["plugin.state"])?)
        };
        let counter_state = decode("COUNTER-001")?;
        let interval_state = decode("INTERVAL-002")?;
        ensure!(
            counter_state["events"] == 1
                && counter_state["denied"] == 1
                && counter_state["pending"] == false,
            "actual counter state is incomplete"
        );
        ensure!(
            interval_state["elapsed_ns"]
                .as_u64()
                .is_some_and(|elapsed| elapsed >= 40_000_000)
                && interval_state["denied"] == 1,
            "actual interval state is incomplete"
        );
        ensure!(
            records.len() == if restored.is_some() { 0 } else { 2 },
            "native denial count differs from the original/restore boundary"
        );
        if let Some(original) = restored {
            ensure!(
                &saved == component_state(original),
                "restored algorithm state changed without a new event"
            );
        }
        std::fs::write(
            Path::new("algorithm-state.original.json"),
            serde_json::to_vec(&saved)?,
        )?;
        Ok(
            json!({"schema":"NativeStrategyContractProbe.v1","pid":std::process::id(),"single_live_node":true,
                "counter_state":counter_state,"interval_state":interval_state,"actual_native_denials":records,"registry_released_before_callbacks":true,
                "counter_elf":format!("{:?}",counter.elf()),"interval_elf":format!("{:?}",interval.elf()),
                "execution_authorized":false,"financial_requests":0,"algorithm_state_only":true,"production_acceptance":false}),
        )
    }

    pub(super) fn load_libraries(args: &[String]) -> Result<[LoadedLibrary; 2]> {
        ensure!(args.len() >= 4, "native fixture library arguments absent");
        let mut registry = LibraryRegistry::new(2, 64 * 1024 * 1024)
            .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        let counter = File::open(&args[0])?;
        let interval = File::open(&args[2])?;
        // SAFETY: the private VPS driver verifies complete build/source receipts,
        // root-owned immutable publication and constructor provenance first.
        let counter = unsafe { registry.load_admitted(&counter, decode_hash(&args[1])?, 0) }
            .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        // SAFETY: the same verified fixture publication applies to this image
        let interval = unsafe { registry.load_admitted(&interval, decode_hash(&args[3])?, 0) }
            .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        drop(registry);
        Ok([counter, interval])
    }

    pub(super) fn run() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        #[cfg(feature = "native-tail-replay")]
        if args
            .first()
            .is_some_and(|mode| mode.starts_with("--history-"))
        {
            return super::history::run(&args);
        }
        ensure!(
            (4..=6).contains(&args.len()),
            "expected counter-path SHA-256 interval-path SHA-256 [restored-state]"
        );
        let failure_mode = args
            .get(5)
            .or_else(|| args.get(4).filter(|mode| mode.starts_with("--")));
        ensure!(
            failure_mode.is_none_or(|mode| matches!(
                mode.as_str(),
                "--fail-event" | "--panic-event" | "--reject-save"
            )),
            "unknown component fault mode"
        );
        let counter_configuration: &[u8] = match failure_mode.map(String::as_str) {
            Some("--fail-event") => b"counter-fail-event-v1",
            Some("--panic-event") => b"counter-panic-event-v1",
            _ => b"counter-v1",
        };
        let [counter, interval] = load_libraries(&args)?;
        let records = Rc::new(RefCell::new(Vec::new()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut node = LiveNode::builder(TraderId::from("CONTRACT-001"), Environment::Live)?
                .with_reconciliation(false)
                .with_delay_post_stop_secs(0)
                .build()?;
            let restored: Option<Value> = args
                .get(4)
                .filter(|path| !path.starts_with("--"))
                .map(|path| std::fs::read(path).map(|raw| serde_json::from_slice(&raw)))
                .transpose()?
                .transpose()?;
            install_guests(
                &mut node,
                counter,
                interval,
                counter_configuration,
                &records,
                restored.as_ref(),
                failure_mode.is_some_and(|mode| mode == "--reject-save"),
            )?;
            let handle = node.handle();
            let recorded = Rc::clone(&records);
            let control = async {
                for _ in 0..1000 {
                    if handle.is_running()
                        && (restored.is_some()
                            || recorded.borrow().len()
                                == if failure_mode.is_some_and(|mode| mode != "--reject-save") {
                                    1
                                } else {
                                    2
                                })
                    {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        handle.stop();
                        return Ok::<(), anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                handle.stop();
                bail!("actual native callbacks did not reach the expected boundary")
            };
            let (result, control) = tokio::join!(node.run_with_mode(NodeRunMode::Hosted), control);
            result?;
            control?;
            let mut proof = if let Some(mode) = failure_mode {
                verify_fault(&node, mode, restored.is_some(), &records.borrow())?
            } else {
                verify_saved_state(counter, interval, restored.as_ref(), &records.borrow())?
            };
            let framework = capture_original_framework()?;
            let mut counts = serde_json::Map::new();
            for id in ["COUNTER-001", "INTERVAL-002"] {
                let actual = &framework["owners"][id]["framework"];
                if let Some(original) = restored
                    .as_ref()
                    .filter(|value| value["schema"] == "NativeStrategyFrameworkProbe.v1")
                {
                    ensure!(
                        actual == &original["owners"][id]["framework"],
                        "restored actual native framework differs from original owner"
                    );
                    ensure!(
                        framework["components"][id] == original["components"][id],
                        "native framework restore changed original component state"
                    );
                }
                counts.insert(id.into(), actual["client_order_id_count"].clone());
            }
            proof["native_factory_client_counts"] = Value::Object(counts);
            proof["native_framework_restored"] = json!(
                restored
                    .as_ref()
                    .is_some_and(|value| value["schema"] == "NativeStrategyFrameworkProbe.v1")
            );
            proof["full_native_cut_proven"] = json!(false);
            println!("{proof}");
            node.dispose();
            Ok(())
        })
    }
}

fn main() -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"))]
    {
        gnu::run()
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu", target_arch = "x86_64")))]
    {
        anyhow::bail!("this private driver executes only on the existing x86-64 GNU VPS")
    }
}
