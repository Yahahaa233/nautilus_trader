// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Private loader/BacktestEngine conformance, never a Metis production grant.

#[cfg(any(
    all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod supported {
    use anyhow::{Context, Result, bail, ensure};
    use nautilus_backtest::{
        config::{BacktestEngineConfig, SimulatedVenueConfig},
        engine::BacktestEngine,
    };
    use nautilus_common::{
        actor::{DataActor, registry::try_get_actor_unchecked},
        messages::execution::TradingCommand,
    };
    use nautilus_execution::models::fee::{FeeModelAny, MakerTakerFeeModel};
    use nautilus_model::{
        data::{Data, QuoteTick},
        enums::{AccountType, BookType, OmsType},
        identifiers::{AccountId, ClientId, InstrumentId, StrategyId, Venue},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::{Money, Price, Quantity},
    };
    use nautilus_plugin::{
        PluginError,
        strategy::{
            instance::{CallbackLimits, StateValidator},
            library_loader::{LibraryRegistry, LoadedLibrary},
        },
    };
    use nautilus_trading::{
        plugin::{PluginOrderType, PluginScope, PluginStrategy},
        strategy::{PreparedSubmission, StrategyConfig, StrategyNative, SubmissionInterceptor},
    };
    use serde_json::{Value, json};
    use std::{cell::RefCell, collections::BTreeSet, fs::File, rc::Rc};

    const COUNTER: &str = include_str!("../../plugin/examples/contract_counter.descriptor.json");
    const INTERVAL: &str = include_str!("../../plugin/examples/contract_interval.descriptor.json");

    #[derive(Debug)]
    struct StateContract {
        counter: bool,
    }
    impl StateValidator for StateContract {
        fn validate(&self, version: u32, raw: &[u8]) -> Result<(), PluginError> {
            let refuse = || PluginError::generic("research fixture state contract refused");
            let state: Value = serde_json::from_slice(raw).map_err(|_| refuse())?;
            let fields = state.as_object().ok_or_else(refuse)?;
            let names = if self.counter {
                ["events", "denied", "pending"]
            } else {
                ["scheduled_at", "elapsed_ns", "denied"]
            };
            if version != 1
                || fields.len() != 3
                || names.iter().any(|name| !fields.contains_key(*name))
            {
                return Err(refuse());
            }
            for name in names {
                if self.counter && name == "pending" {
                    if !state[name].is_boolean() {
                        return Err(refuse());
                    }
                } else if !state[name].is_u64() {
                    return Err(refuse());
                }
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoGrant(Rc<RefCell<Vec<Value>>>);
    impl SubmissionInterceptor for NoGrant {
        fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
            let TradingCommand::SubmitOrder(order) = command else {
                bail!("unexpected research conformance command");
            };
            self.0.borrow_mut().push(serde_json::to_value(order)?);
            // Both platform hosts exercise the same fixture protocol. Guests
            // recognize only this explicit refusal, never an arbitrary error.
            bail!("contract_probe_no_grant")
        }
    }

    fn hash(raw: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        Sha256::digest(raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
    fn decode_hash(raw: &str) -> Result<[u8; 32]> {
        ensure!(
            raw.len() == 64
                && raw
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "invalid research SHA-256"
        );
        let mut bytes = [0; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&raw[index * 2..index * 2 + 2], 16)?;
        }
        Ok(bytes)
    }

    fn load_libraries(args: &[String]) -> Result<([LoadedLibrary; 2], usize)> {
        let publisher: u32 = args[5].parse()?;
        #[cfg(target_os = "macos")]
        let _cache = std::path::Path::new(&args[4]);
        let mut registry = LibraryRegistry::new(
            2,
            64 * 1024 * 1024,
            #[cfg(target_os = "macos")]
            _cache,
        )
        .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        let before = registry.remaining_library_bytes();
        let counter = File::open(&args[0])?;
        let interval = File::open(&args[2])?;
        // SAFETY: the private driver verifies the exact source/build receipts,
        // publisher, readonly libraries and this non-financial fixture domain.
        let counter =
            unsafe { registry.load_admitted(&counter, decode_hash(&args[1])?, publisher) }
                .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        // SAFETY: the same verified private publication applies to this image
        let interval =
            unsafe { registry.load_admitted(&interval, decode_hash(&args[3])?, publisher) }
                .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        let remaining = registry.remaining_library_bytes();
        ensure!(
            remaining < before,
            "actual code did not consume the declared byte budget"
        );
        drop(registry);
        Ok(([counter, interval], remaining))
    }

    fn install_guests(
        engine: &mut BacktestEngine,
        libraries: [LoadedLibrary; 2],
        records: &Rc<RefCell<Vec<Value>>>,
        instrument_id: InstrumentId,
    ) -> Result<()> {
        for (id, client, library, descriptor, configuration, is_counter) in [
            (
                "COUNTER-001",
                "CONTRACT-A",
                libraries[0],
                COUNTER,
                b"counter-v1".as_slice(),
                true,
            ),
            (
                "INTERVAL-002",
                "CONTRACT-B",
                libraries[1],
                INTERVAL,
                b"interval-v1".as_slice(),
                false,
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
                    Box::new(StateContract {
                        counter: is_counter,
                    }),
                )
                .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
            let mut strategy = PluginStrategy::new(
                StrategyConfig {
                    strategy_id: Some(StrategyId::from(id)),
                    ..Default::default()
                },
                guest,
                PluginScope {
                    client_id: ClientId::from(client),
                    account_id: AccountId::from("BINANCE-CONTRACT"),
                    instruments: BTreeSet::from([instrument_id]),
                    bar_types: Vec::new(),
                    order_types: BTreeSet::from([PluginOrderType::Market]),
                    max_timers: 1,
                    state_schema_version: 1,
                    descriptor_sha256: hash(descriptor.as_bytes()),
                    configuration_sha256: hash(configuration),
                },
            )?;
            strategy
                .strategy_core_mut()
                .set_submission_interceptor(Rc::new(NoGrant(Rc::clone(records))))?;
            engine.add_strategy(strategy)?;
        }
        Ok(())
    }

    fn verify_state(engine: &BacktestEngine, records: &[Value], remaining: usize) -> Result<Value> {
        let mut states = serde_json::Map::new();
        for id in ["COUNTER-001", "INTERVAL-002"] {
            let actor = try_get_actor_unchecked::<PluginStrategy>(&id.into())
                .context("actual research strategy missing")?;
            let saved = actor.on_save()?;
            let state: Value = serde_json::from_slice(
                saved
                    .get("plugin.state")
                    .context("research algorithm failed")?,
            )?;
            ensure!(
                state["denied"] == 1,
                "actual native research submission refusal absent"
            );
            states.insert(id.into(), state);
        }
        ensure!(
            states["COUNTER-001"]["events"] == 1 && states["COUNTER-001"]["pending"] == false,
            "counter did not retain its actual native timer state"
        );
        ensure!(
            states["INTERVAL-002"]["elapsed_ns"]
                .as_u64()
                .is_some_and(|elapsed| elapsed >= 40_000_000),
            "native virtual-clock interval absent"
        );
        ensure!(
            records.len() == 2 && engine.get_result().total_orders == 0,
            "unexpected native submission/ordering boundary"
        );
        Ok(
            json!({"schema":"NativeStrategyResearchContractProbe.v1","pid":std::process::id(),"single_native_backtest_engine":true,"registry_released_before_callbacks":true,"states":states,"native_submission_refusals":records,"remaining_library_bytes":remaining,"financial_requests":0,"execution_authorized":false,"production_acceptance":false,"full_native_cut_proven":false}),
        )
    }

    pub(super) fn run() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        ensure!(
            args.len() == 6,
            "expected counter-path SHA-256 interval-path SHA-256 cache-directory publisher-uid"
        );
        let ([counter, interval], remaining) = load_libraries(&args)?;
        let records = Rc::new(RefCell::new(Vec::new()));
        let mut engine = BacktestEngine::new(BacktestEngineConfig::default())?;
        engine.add_venue(
            SimulatedVenueConfig::builder()
                .venue(Venue::from("BINANCE"))
                .oms_type(OmsType::Netting)
                .account_type(AccountType::Margin)
                .book_type(BookType::L1_MBP)
                .starting_balances(vec![Money::from("1_000_000 USDT")])
                .fee_model(FeeModelAny::MakerTaker(MakerTakerFeeModel::zero()).into())
                .build()?,
        )?;
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        engine.add_instrument(&instrument)?;
        install_guests(&mut engine, [counter, interval], &records, instrument_id)?;
        let quotes = [1_000_000_000u64, 2_000_000_000]
            .into_iter()
            .map(|time| {
                Data::Quote(QuoteTick::new(
                    instrument_id,
                    Price::from("999.99"),
                    Price::from("1000.01"),
                    Quantity::from("1.000"),
                    Quantity::from("1.000"),
                    time.into(),
                    time.into(),
                ))
            })
            .collect();
        engine.add_data(quotes, None, true, true)?;
        engine.run(None, None, None, false)?;
        let proof = verify_state(&engine, &records.borrow(), remaining)?;
        println!("{proof}");
        engine.dispose();
        Ok(())
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
        anyhow::bail!("unsupported native research conformance platform")
    }
}
