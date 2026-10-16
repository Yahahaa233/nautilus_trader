// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Actual native facade/market-client fixture, with no execution connection.

#[cfg(any(
    all(target_os = "linux", target_env = "gnu", target_arch = "x86_64"),
    all(target_os = "macos", target_arch = "aarch64")
))]
mod supported {
    use anyhow::{Context, Result, bail, ensure};
    use nautilus_common::{
        actor::{DataActor, registry::try_get_actor_unchecked},
        cache::CacheView,
        clients::DataClient,
        clock::Clock,
        enums::Environment,
        factories::{ClientConfig, DataClientFactory},
        live::{runner::get_data_event_sender, sender::EventSender},
        messages::{
            DataEvent,
            data::{
                BarsResponse, DataResponse, RequestBars, SubscribeBars, SubscribeQuotes,
                SubscribeTrades, UnsubscribeBars, UnsubscribeQuotes, UnsubscribeTrades,
            },
            execution::TradingCommand,
        },
    };
    use nautilus_core::UUID4;
    use nautilus_live::node::{LiveNode, NodeRunMode};
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        data::{Bar, BarType, Data, QuoteTick, TradeTick},
        enums::{AccountType, AggressorSide},
        events::AccountState,
        identifiers::{AccountId, ClientId, StrategyId, TradeId, TraderId, Venue},
        instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
        types::{AccountBalance, Money, Price, Quantity},
    };
    use nautilus_plugin::{
        PluginError,
        strategy::{
            instance::{CallbackLimits, StateValidator},
            library_loader::LibraryRegistry,
        },
    };
    use nautilus_trading::{
        plugin::{PluginOrderType, PluginScope, PluginStrategy},
        strategy::{PreparedSubmission, StrategyConfig, StrategyNative, SubmissionInterceptor},
    };
    use serde_json::{Value, json};
    use std::{
        any::Any,
        cell::{Cell, RefCell},
        collections::BTreeSet,
        fs::File,
        rc::Rc,
        time::Duration,
    };

    const DESCRIPTOR: &str = include_str!("../../plugin/examples/contract_facade.descriptor.json");
    const INSTRUMENT: &str = "ETHUSDT-PERP.BINANCE";
    const BAR: &str = "ETHUSDT-PERP.BINANCE-1-SECOND-LAST-EXTERNAL";

    #[derive(Debug, Default, serde::Serialize)]
    struct ProviderState {
        requests: Vec<Value>,
        subscriptions: Vec<Value>,
        unsubscriptions: Vec<Value>,
    }
    #[derive(Debug)]
    struct FixtureClient {
        state: Rc<RefCell<ProviderState>>,
        sender: EventSender<DataEvent>,
        connected: Rc<Cell<bool>>,
        admission: Rc<Cell<u8>>,
        deferred: bool,
        restored: bool,
    }
    #[async_trait::async_trait(?Send)]
    impl DataClient for FixtureClient {
        fn restore_running_checkpoint(&mut self, value: &Value) -> Result<()> {
            self.verify_paused_recovery_inventory()?;
            ensure!(
                !self.restored && value == &self.profile(),
                "fixture source profile changed or reused"
            );
            self.restored = true;
            Ok(())
        }
        fn verify_paused_recovery_inventory(&self) -> Result<()> {
            let state = self.state.borrow();
            ensure!(
                !self.connected.get()
                    && self.admission.get() == 0
                    && state.requests.is_empty()
                    && state.subscriptions.is_empty()
                    && state.unsubscriptions.is_empty(),
                "fixture adapter is not fresh"
            );
            Ok(())
        }
        fn paused_recovery_inventory_profile(&self) -> Result<&'static str> {
            self.verify_paused_recovery_inventory()?;
            Ok("native_facade_disconnected_stateless_fixture.v1")
        }
        fn freeze_running_checkpoint(
            &self,
        ) -> Result<Box<dyn nautilus_common::clients::RunningAdapterCheckpoint>> {
            ensure!(
                self.connected.get() && self.admission.get() == 0,
                "fixture adapter is not admitted"
            );
            let value = self.profile();
            let original = serde_json::to_value(&*self.state.borrow())?;
            self.admission.set(1);
            Ok(Box::new(FixtureFreeze {
                value,
                original,
                state: Rc::clone(&self.state),
                connected: Rc::clone(&self.connected),
                admission: Rc::clone(&self.admission),
                finished: false,
            }))
        }
        fn client_id(&self) -> ClientId {
            ClientId::from("FACADE-DATA")
        }
        fn venue(&self) -> Option<Venue> {
            Some(Venue::from("BINANCE"))
        }
        fn start(&mut self) -> Result<()> {
            Ok(())
        }
        fn stop(&mut self) -> Result<()> {
            Ok(())
        }
        fn reset(&mut self) -> Result<()> {
            Ok(())
        }
        fn dispose(&mut self) -> Result<()> {
            Ok(())
        }
        fn is_connected(&self) -> bool {
            self.connected.get()
        }
        fn is_disconnected(&self) -> bool {
            !self.connected.get()
        }
        async fn connect(&mut self) -> Result<()> {
            ensure!(
                self.admission.get() == 0,
                "fixture connection admission closed"
            );
            self.connected.set(true);
            Ok(())
        }
        async fn disconnect(&mut self) -> Result<()> {
            self.connected.set(false);
            Ok(())
        }
        fn subscribe_quotes(&mut self, command: SubscribeQuotes) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .subscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn subscribe_trades(&mut self, command: SubscribeTrades) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .subscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn subscribe_bars(&mut self, command: SubscribeBars) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .subscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn unsubscribe_quotes(&mut self, command: &UnsubscribeQuotes) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .unsubscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn unsubscribe_trades(&mut self, command: &UnsubscribeTrades) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .unsubscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn unsubscribe_bars(&mut self, command: &UnsubscribeBars) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            self.state
                .borrow_mut()
                .unsubscriptions
                .push(serde_json::to_value(command)?);
            Ok(())
        }
        fn request_bars(&self, request: RequestBars) -> Result<()> {
            ensure!(self.admission.get() == 0, "fixture input admission closed");
            ensure!(
                request.client_id == Some(self.client_id()) && request.bar_type.to_string() == BAR,
                "fixture provider received another source scope"
            );
            self.state
                .borrow_mut()
                .requests
                .push(serde_json::to_value(&request)?);
            if self.deferred {
                return Ok(());
            }
            let end = request.end.context("native history end absent")?;
            let time = u64::try_from(end.as_nanosecond())?;
            let event = time / 1_000_000_000 * 1_000_000_000;
            let bar = Bar::new(
                request.bar_type,
                Price::from("1000.00"),
                Price::from("1001.00"),
                Price::from("999.00"),
                Price::from("1000.00"),
                Quantity::from("1.000"),
                event.into(),
                time.into(),
            );
            let start = request
                .start
                .map(|value| u64::try_from(value.as_nanosecond()))
                .transpose()?
                .map(Into::into);
            let response = BarsResponse::new(
                request.request_id,
                self.client_id(),
                request.bar_type,
                vec![bar],
                start,
                Some(time.into()),
                time.into(),
                None,
            );
            self.sender
                .send(DataEvent::Response(DataResponse::Bars(response)))?;
            Ok(())
        }
    }

    impl FixtureClient {
        fn profile(&self) -> Value {
            // Requests are synchronous transfers to the external fixture driver.
            // This client owns no socket, HTTP future or asynchronous response task.
            json!({"profile":"native_facade_stateless_external_driver.v1","client_id":self.client_id(),
                "deferred":self.deferred,"owned_network_tasks":0,"execution_authorized":false})
        }
    }
    #[derive(Debug)]
    struct FixtureFreeze {
        value: Value,
        original: Value,
        state: Rc<RefCell<ProviderState>>,
        connected: Rc<Cell<bool>>,
        admission: Rc<Cell<u8>>,
        finished: bool,
    }
    impl nautilus_common::clients::RunningAdapterCheckpoint for FixtureFreeze {
        fn inventory(&self) -> &Value {
            &self.value
        }
        fn verify(&self) -> Result<()> {
            ensure!(
                self.connected.get()
                    && self.admission.get() == 1
                    && serde_json::to_value(&*self.state.borrow())? == self.original,
                "fixture freeze invalidated"
            );
            Ok(())
        }
        fn finish(mut self: Box<Self>) -> Result<()> {
            self.verify()?;
            self.finished = true;
            self.admission.set(0);
            Ok(())
        }
        fn finish_terminal(mut self: Box<Self>) -> Result<()> {
            self.verify()?;
            self.finished = true;
            self.admission.set(2);
            Ok(())
        }
    }
    impl Drop for FixtureFreeze {
        fn drop(&mut self) {
            if !self.finished {
                self.admission.set(2);
            }
        }
    }

    #[derive(Debug)]
    struct FixtureConfig;
    impl ClientConfig for FixtureConfig {
        fn as_any(&self) -> &dyn Any {
            self
        }
    }
    #[derive(Debug)]
    struct FixtureFactory(Rc<RefCell<ProviderState>>, bool);
    impl DataClientFactory for FixtureFactory {
        fn name(&self) -> &'static str {
            "FACADE-DATA"
        }
        fn config_type(&self) -> &'static str {
            "NativeFacadeFixtureConfig"
        }
        fn create(
            &self,
            _: &str,
            config: &dyn ClientConfig,
            _: CacheView,
            _: Rc<RefCell<dyn Clock>>,
        ) -> Result<Box<dyn DataClient>> {
            ensure!(
                config.as_any().is::<FixtureConfig>(),
                "wrong private fixture configuration"
            );
            Ok(Box::new(FixtureClient {
                state: Rc::clone(&self.0),
                sender: get_data_event_sender(),
                connected: Rc::new(Cell::new(false)),
                admission: Rc::new(Cell::new(0)),
                deferred: self.1,
                restored: false,
            }))
        }
    }

    #[derive(Debug)]
    struct Contract;
    impl StateValidator for Contract {
        fn validate(&self, version: u32, raw: &[u8]) -> Result<(), PluginError> {
            let refuse = || PluginError::generic("native facade fixture state refused");
            let state: Value = serde_json::from_slice(raw).map_err(|_| refuse())?;
            let fields = state.as_object().ok_or_else(refuse)?;
            let names = [
                "initialized",
                "subscribed",
                "quotes",
                "trades",
                "bars",
                "historical_bars",
                "history_request_id",
                "native_submission_refusals",
                "scoped_refusals",
            ];
            if version != 1
                || fields.len() != names.len()
                || names.iter().any(|name| !fields.contains_key(*name))
            {
                return Err(refuse());
            }
            if !state["initialized"].is_boolean() || !state["subscribed"].is_boolean() {
                return Err(refuse());
            }
            for name in [
                "quotes",
                "trades",
                "bars",
                "historical_bars",
                "native_submission_refusals",
                "scoped_refusals",
            ] {
                if !state[name].is_u64() {
                    return Err(refuse());
                }
            }
            if !state["history_request_id"].is_null()
                && !state["history_request_id"]
                    .as_str()
                    .is_some_and(|id| id.parse::<UUID4>().is_ok())
            {
                return Err(refuse());
            }
            Ok(())
        }
    }

    #[derive(Debug)]
    struct NoGrant(Rc<RefCell<Vec<Value>>>);
    impl SubmissionInterceptor for NoGrant {
        fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
            self.0.borrow_mut().push(serde_json::to_value(command)?);
            bail!("facade_probe_no_grant")
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
        records: &Rc<RefCell<Vec<Value>>>,
    ) -> Result<PluginStrategy> {
        ensure!(
            args.len() == 4,
            "expected library SHA-256 backing-directory publisher-uid"
        );
        ensure!(
            args[1].len() == 64
                && args[1]
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()),
            "invalid fixture library hash"
        );
        let mut digest = [0; 32];
        for (index, byte) in digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&args[1][index * 2..index * 2 + 2], 16)?;
        }
        #[cfg(target_os = "macos")]
        let backing = std::path::Path::new(&args[2]);
        let mut registry = LibraryRegistry::new(
            1,
            64 * 1024 * 1024,
            #[cfg(target_os = "macos")]
            backing,
        )
        .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        // SAFETY: the isolated private driver verifies this exact source/build,
        // immutable library, publisher and the fixed non-financial scope.
        let library =
            unsafe { registry.load_admitted(&File::open(&args[0])?, digest, args[3].parse()?) }
                .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        let configuration =
            serde_json::to_vec(&json!({"instrument_id":INSTRUMENT,"bar_type":BAR}))?;
        let guest = library
            .create_instance(
                DESCRIPTOR.as_bytes(),
                &configuration,
                CallbackLimits {
                    configuration_bytes: 4096,
                    state_bytes: 16384,
                    event_bytes: 16 * 1024 * 1024,
                },
                Box::new(Contract),
            )
            .map_err(|e| anyhow::anyhow!("{}", e.message_string()))?;
        drop(registry);
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        let bar_type: BarType = BAR.parse()?;
        let mut strategy = PluginStrategy::new(
            StrategyConfig {
                strategy_id: Some(StrategyId::from("FACADE-001")),
                ..Default::default()
            },
            guest,
            PluginScope {
                client_id: ClientId::from("FACADE-EXEC"),
                data_client_id: ClientId::from("FACADE-DATA"),
                account_id: AccountId::from("BINANCE-FIXTURE"),
                instruments: BTreeSet::from([instrument_id]),
                bar_types: vec![bar_type],
                order_types: BTreeSet::from([
                    PluginOrderType::Market,
                    PluginOrderType::Limit,
                    PluginOrderType::StopMarket,
                    PluginOrderType::StopLimit,
                ]),
                max_timers: 1,
                max_history_bars: 1,
                quote_instruments: BTreeSet::from([instrument_id]),
                trade_instruments: BTreeSet::from([instrument_id]),
                state_schema_version: 1,
                descriptor_sha256: hash(DESCRIPTOR.as_bytes()),
                configuration_sha256: hash(&configuration),
            },
        )?;
        strategy
            .strategy_core_mut()
            .set_submission_interceptor(Rc::new(NoGrant(Rc::clone(records))))?;
        #[cfg(feature = "native-tail-replay")]
        strategy.set_historical_verifier(Rc::new(
            nautilus_live::node::plugin_recovery::ReaderVerifiedPluginRecovery::default(),
        ))?;
        Ok(strategy)
    }

    #[cfg(feature = "native-tail-replay")]
    include!("strategy_facade_host/history.rs");

    pub(super) fn run() -> Result<()> {
        let args = std::env::args().skip(1).collect::<Vec<_>>();
        #[cfg(feature = "native-tail-replay")]
        if args
            .first()
            .is_some_and(|value| value.starts_with("history-"))
        {
            return history::run(&args);
        }
        let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
        let instrument_id = instrument.id();
        let bar_type: BarType = BAR.parse()?;
        let records = Rc::new(RefCell::new(Vec::new()));
        let provider = Rc::new(RefCell::new(ProviderState::default()));
        let strategy = construct_strategy(&args, &records)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let mut node=LiveNode::builder(TraderId::from("FACADE-TRADER"),Environment::Live)?
                .with_reconciliation(false).with_delay_post_stop_secs(0)
                .add_data_client(Some("FACADE-DATA".into()),Box::new(FixtureFactory(Rc::clone(&provider), false)),Box::new(FixtureConfig))?.build()?;
            let total=Money::from("1_000_000 USDT");let zero=Money::from("0 USDT");
            let account=MarginAccount::new(AccountState{account_id:AccountId::from("BINANCE-FIXTURE"),account_type:AccountType::Margin,base_currency:None,
                balances:vec![AccountBalance::new(total,zero,total)],margins:vec![],is_reported:false,event_id:UUID4::new(),ts_event:0.into(),ts_init:0.into(),info:None},true);
            {let mut cache=node.kernel().cache.borrow_mut();cache.add_instrument(instrument)?;cache.add_account(AccountAny::Margin(account))?;}
            node.add_strategy(strategy)?;let handle=node.handle();
            let feed=async {
                let mut ready=false;
                for _ in 0..1000 {
                    ready=provider.borrow().requests.len()==1;
                    if ready {break;}
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                if !ready {handle.stop();bail!("native facade did not reach its actual history provider");}
                let sender=get_data_event_sender();let now=nautilus_core::time::nanos_since_unix_epoch();
                sender.send(DataEvent::Data(Data::Quote(QuoteTick::new(instrument_id,Price::from("999.99"),Price::from("1000.01"),Quantity::from("1.000"),Quantity::from("1.000"),now.into(),now.into()))))?;
                sender.send(DataEvent::Data(Data::Trade(TradeTick::new(instrument_id,Price::from("1000.00"),Quantity::from("1.000"),AggressorSide::Buy,TradeId::from("FACADE-TRADE-1"),now.into(),now.into()))))?;
                sender.send(DataEvent::Data(Data::Bar(Bar::new(bar_type,Price::from("1000.00"),Price::from("1001.00"),Price::from("999.00"),Price::from("1000.00"),Quantity::from("1.000"),now.into(),now.into()))))?;
                tokio::time::sleep(Duration::from_millis(200)).await;handle.stop();Ok::<(),anyhow::Error>(())
            };
            let (run,feed)=tokio::join!(node.run_with_mode(NodeRunMode::Hosted),feed);
            let actor=try_get_actor_unchecked::<PluginStrategy>(&"FACADE-001".into()).context("actual facade component missing")?;
            let saved=actor.on_save();
            println!("{}",json!({"schema":"NativeFacadeAttempt.v1","pid":std::process::id(),
                "run_error":run.as_ref().err().map(|error|format!("{error:#}")),"feed_error":feed.as_ref().err().map(|error|format!("{error:#}")),
                "first_callback_failure":actor.callback_failure().map(|(code,message)|json!({"code":format!("{code:?}"),"message":message})),
                "component":saved.as_ref().ok(),"save_error":saved.as_ref().err().map(|error|format!("{error:#}")),
                "native_submission_refusals":*records.borrow(),"provider_requests":provider.borrow().requests,
                "fixture_account_facts":true,"real_venue":false,"financial_requests":0,"production_acceptance":false}));
            run?;feed?;
            let saved=saved?;
            let state:Value=serde_json::from_slice(saved.get("plugin.state").context("facade failed; retain original first failure")?)?;
            ensure!(state["native_submission_refusals"]==4 && state["scoped_refusals"]==5,"actual native facade refusal boundary differs");
            ensure!(state["quotes"]==1 && state["trades"]==1 && state["bars"]==1 && state["historical_bars"]==1 && state["history_request_id"].is_null(),"actual native market/history delivery differs");
            let provider=provider.borrow();ensure!(provider.requests.len()==1 && provider.subscriptions.len()==3 && provider.unsubscriptions.len()==3,"native provider did not receive the exact routed work");
            ensure!(records.borrow().len()==4 && node.kernel().cache.borrow().orders_total_count(None,None,None,None,None)==0,"unexpected native execution boundary");
            println!("{}",json!({"schema":"NativeFacadeContractProbe.v1","pid":std::process::id(),"state":state,"native_submission_refusals":*records.borrow(),
                "provider_requests":provider.requests,"provider_subscriptions":provider.subscriptions,"provider_unsubscriptions":provider.unsubscriptions,
                "single_native_live_node":true,"separate_data_execution_routing":true,"fixture_account_facts":true,"real_venue":false,
                "financial_requests":0,"execution_authorized":false,"full_native_cut_proven":false,"running_adapter_recovery_qualified":false,"production_acceptance":false}));
            drop(actor);drop(provider);node.dispose();Ok(())
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
        anyhow::bail!("unsupported native facade fixture platform")
    }
}
