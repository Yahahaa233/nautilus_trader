// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Actual native factory/cache/interceptor paths; no exchange or economic ledger.
use super::{
    ManagedBracketRequest, ManagedOrderPlan, ManagedPolicyIdentity, ManagedSubmissionContext,
    ManagedSubmissionOutcome, ManagedSubmissionPolicy, ManagedSubmissionStatus,
    NativeManagedFailure, classify_managed_attempt, finish_managed_single,
};
use crate::{
    plugin::{PluginOrderType, PluginScope, PluginStrategy, wire},
    strategy::{PreparedSubmission, StrategyConfig, StrategyNative, SubmissionInterceptor},
};
use anyhow::{Result, bail, ensure};
use nautilus_common::{
    cache::Cache, clock::VirtualClock, component::Component, messages::execution::TradingCommand,
};
use nautilus_model::{
    enums::{ContingencyType, OrderSide, OrderStatus, TimeInForce, TriggerType},
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_plugin::{
    PluginError, PluginErrorCode,
    strategy::{
        Algorithm, Host, ReadOnlyHost, StrategyApi,
        instance::{CallbackLimits, GuestInstance, StateValidator},
    },
};
use nautilus_portfolio::portfolio::Portfolio;
use serde_json::{Value, json};
use std::{cell::RefCell, collections::BTreeSet, rc::Rc};

#[derive(Debug)]
struct Empty;
impl Algorithm for Empty {
    const DESCRIPTOR_JSON: &'static str = "{}";
    fn validate(_: &[u8]) -> Result<(), PluginError> {
        Ok(())
    }
    fn create(_: &[u8]) -> Result<Self, PluginError> {
        Ok(Self)
    }
    fn start(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn on_event(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn save(&self, _: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        Ok(b"{}".to_vec())
    }
    fn restore(&mut self, _: u32, _: &[u8], _: &mut ReadOnlyHost<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn pause(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn resume(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn stop(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
}
#[derive(Debug)]
struct Contract;
impl StateValidator for Contract {
    fn validate(&self, version: u32, raw: &[u8]) -> Result<(), PluginError> {
        if version == 1 && raw == b"{}" {
            Ok(())
        } else {
            Err(PluginError::generic("private state differs"))
        }
    }
}
static API: StrategyApi = StrategyApi::for_algorithm::<Empty>();
fn injected_prepare_panic() -> ! {
    panic!("original portfolio preparation panic");
}
fn injected_native_panic() -> ! {
    panic!("original batch native submission panic");
}
#[derive(Debug, Default)]
struct Evidence {
    prepares: usize,
    finalizations: Vec<Vec<ManagedSubmissionOutcome>>,
    contexts: Vec<ManagedSubmissionContext>,
    commands: Vec<TradingCommand>,
    bound: usize,
}
#[derive(Debug)]
struct Policy {
    evidence: Rc<RefCell<Evidence>>,
    case: &'static str,
}
fn entry(context: &ManagedSubmissionContext) -> wire::SubmitRequest {
    wire::SubmitRequest {
        schema_version: 2,
        order_type: wire::NativeOrderType::Market,
        instrument_id: context.instrument_id,
        side: context.side,
        quantity: Quantity::from("0.001"),
        price: None,
        trigger_price: None,
        trigger_type: None,
        time_in_force: TimeInForce::Gtc,
        expire_time_ns: None,
        reduce_only: false,
        post_only: false,
        position_id: None,
    }
}
impl Policy {
    fn plan(&self, context: &ManagedSubmissionContext) -> ManagedOrderPlan {
        let mut parent = entry(context);
        if !self.case.starts_with("bracket") {
            return ManagedOrderPlan::Single(parent);
        }
        let mut take_profit = parent.clone();
        take_profit.side = OrderSide::Sell;
        take_profit.order_type = wire::NativeOrderType::Limit;
        take_profit.price = Some(Price::from("3000.00"));
        take_profit.reduce_only = true;
        let mut stop_loss = parent.clone();
        stop_loss.side = OrderSide::Sell;
        stop_loss.order_type = wire::NativeOrderType::StopMarket;
        stop_loss.trigger_price = Some(Price::from("1000.00"));
        stop_loss.trigger_type = Some(TriggerType::LastPrice);
        stop_loss.reduce_only = true;
        if self.case == "bracket-stop-market" || self.case == "bracket-stop-limit" {
            parent.order_type = if self.case == "bracket-stop-market" {
                wire::NativeOrderType::StopMarket
            } else {
                wire::NativeOrderType::StopLimit
            };
            parent.trigger_price = Some(Price::from("2000.00"));
            parent.trigger_type = Some(TriggerType::MarkPrice);
        }
        if self.case == "bracket-limit" || self.case == "bracket-stop-limit" {
            parent.order_type = if self.case == "bracket-limit" {
                wire::NativeOrderType::Limit
            } else {
                wire::NativeOrderType::StopLimit
            };
            parent.price = Some(Price::from("2001.00"));
        }
        if self.case == "bracket-foreign-child" {
            stop_loss.instrument_id = "BTCUSDT-PERP.BINANCE".into();
        }
        if self.case == "bracket-quantity-mismatch" {
            stop_loss.quantity = Quantity::from("0.002");
        }
        ManagedOrderPlan::Bracket(Box::new(ManagedBracketRequest {
            entry: parent,
            take_profit,
            stop_loss,
            contingency: ContingencyType::Ouo,
        }))
    }
}
impl ManagedSubmissionPolicy for Policy {
    fn prepare(&self, context: &ManagedSubmissionContext, _: &[u8]) -> Result<ManagedOrderPlan> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.prepares += 1;
        evidence.contexts.push(context.clone());
        drop(evidence);
        Ok(self.plan(context))
    }
    fn finish(
        &self,
        _: &ManagedSubmissionContext,
        outcome: &ManagedSubmissionOutcome,
        _: &[u8],
    ) -> Result<()> {
        self.evidence
            .borrow_mut()
            .finalizations
            .push(vec![outcome.clone()]);
        Ok(())
    }
    fn prepare_batch(
        &self,
        contexts: &[ManagedSubmissionContext],
        _: &[u8],
    ) -> Result<Vec<ManagedOrderPlan>> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.prepares += 1;
        evidence.contexts.extend_from_slice(contexts);
        drop(evidence);
        if self.case == "prepare-panic" {
            injected_prepare_panic();
        }
        if self.case == "wrong-inventory" {
            return Ok(Vec::new());
        }
        Ok(contexts
            .iter()
            .enumerate()
            .map(|(i, context)| {
                if i == 1 && self.case == "member-refused" {
                    ManagedOrderPlan::Refused {
                        reason: "original portfolio budget refusal".into(),
                    }
                } else {
                    self.plan(context)
                }
            })
            .collect())
    }
    fn finish_batch(
        &self,
        _: &[ManagedSubmissionContext],
        outcomes: &[ManagedSubmissionOutcome],
        _: &[u8],
    ) -> Result<()> {
        self.evidence
            .borrow_mut()
            .finalizations
            .push(outcomes.to_vec());
        if self.case == "finish-failed" {
            bail!("original batch writer failed");
        }
        Ok(())
    }
}
#[derive(Debug)]
struct Interceptor {
    evidence: Rc<RefCell<Evidence>>,
    case: &'static str,
}
#[derive(Debug)]
struct Prepared {
    evidence: Rc<RefCell<Evidence>>,
    fail: bool,
}
impl PreparedSubmission for Prepared {
    fn bind(self: Box<Self>, _: &TradingCommand) -> Result<()> {
        self.evidence.borrow_mut().bound += 1;
        if self.fail {
            bail!("original binding failure after native cache admission");
        }
        Ok(())
    }
}
impl SubmissionInterceptor for Interceptor {
    fn prepare(&self, command: &TradingCommand) -> Result<Box<dyn PreparedSubmission>> {
        let mut evidence = self.evidence.borrow_mut();
        evidence.commands.push(command.clone());
        let n = evidence.commands.len();
        drop(evidence);
        if self.case == "native-panic" && n == 2 {
            injected_native_panic();
        }
        if self.case.starts_with("bracket") || self.case == "all-rejected" {
            bail!("private no financial grant");
        }
        Ok(Box::new(Prepared {
            evidence: Rc::clone(&self.evidence),
            fail: self.case == "binding-failed" && n == 2,
        }))
    }
}
#[allow(unsafe_code)] // Fixed process-lifetime static ABI table; safety stated at call.
fn fixture(case: &'static str) -> Result<(PluginStrategy, Rc<RefCell<Evidence>>)> {
    // SAFETY: fixed static executable table and Rust thunks have process lifetime.
    let guest = unsafe {
        GuestInstance::create(
            &raw const API,
            b"{}",
            b"",
            CallbackLimits {
                configuration_bytes: 1024,
                state_bytes: 1024,
                event_bytes: 1024,
            },
            Box::new(Contract),
        )
    }
    .map_err(|e| anyhow::anyhow!(e.message_string()))?;
    let instrument = InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt());
    let mut strategy = PluginStrategy::new(
        StrategyConfig {
            strategy_id: Some("MANAGED-001".into()),
            manage_order_lists_locally: case != "bracket-manager-missing",
            manage_contingent_orders: true,
            ..Default::default()
        },
        guest,
        PluginScope {
            client_id: "MANAGED-EXEC".into(),
            data_client_id: "MANAGED-DATA".into(),
            account_id: "BINANCE-MANAGED".into(),
            instruments: BTreeSet::from([instrument.id()]),
            bar_types: Vec::new(),
            order_types: BTreeSet::from([
                PluginOrderType::Market,
                PluginOrderType::Limit,
                PluginOrderType::StopMarket,
                PluginOrderType::StopLimit,
            ]),
            max_timers: 0,
            max_history_bars: 0,
            allow_history_requests: false,
            quote_instruments: BTreeSet::new(),
            trade_instruments: BTreeSet::new(),
            state_schema_version: 1,
            descriptor_sha256: "a".repeat(64),
            configuration_sha256: "b".repeat(64),
        },
    )?;
    let evidence = Rc::new(RefCell::new(Evidence::default()));
    strategy.set_managed_submission_policy(
        ManagedPolicyIdentity {
            policy_id: "private_no_ledger".into(),
            schema_version: 1,
            configuration_sha256: "c".repeat(64),
            max_batch_size: if case == "batch-disabled" { 0 } else { 3 },
        },
        Rc::new(Policy {
            evidence: Rc::clone(&evidence),
            case,
        }),
    )?;
    strategy
        .core
        .set_submission_interceptor(Rc::new(Interceptor {
            evidence: Rc::clone(&evidence),
            case,
        }))?;
    let clock = Rc::new(RefCell::new(VirtualClock::new()));
    let cache = Rc::new(RefCell::new(Cache::default()));
    cache.borrow_mut().add_instrument(instrument)?;
    let portfolio = Rc::new(RefCell::new(Portfolio::new(
        clock.clone(),
        cache.clone(),
        None,
    )));
    strategy
        .core
        .register("TRADER-001".into(), clock, cache, portfolio)?;
    strategy.initialize()?;
    if case == "market-exit-cached-denial" {
        strategy.core.is_exiting = true;
    }
    Ok((strategy, evidence))
}
fn intent() -> Value {
    json!({"schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","intent":{"kind":"entry"}})
}

#[rstest::rstest]
#[case("all-accepted", 3, 3, false)]
#[case("empty", 0, 0, false)]
#[case("all-rejected", 3, 0, false)]
#[case("member-refused", 2, 2, false)]
#[case("native-panic", 2, 1, true)]
#[case("binding-failed", 2, 2, true)]
#[case("prepare-panic", 0, 0, true)]
#[case("wrong-inventory", 0, 0, false)]
#[case("finish-failed", 3, 3, true)]
#[case("market-exit-cached-denial", 0, 0, false)]
fn actual_batch_preserves_frame_identity_and_always_finalizes_once(
    #[case] case: &'static str,
    #[case] commands: usize,
    #[case] bound: usize,
    #[case] fatal: bool,
) -> Result<()> {
    let (mut strategy, evidence) = fixture(case)?;
    let count = if case == "empty" { 0 } else { 3 };
    let request = serde_json::to_vec(&json!({"schema_version":1,"intents":vec![intent();count]}))?;
    let result = strategy.managed_submit_batch(&request);
    ensure!((result.is_err()) == (fatal), "{case}: {result:?}");
    let evidence = evidence.borrow();
    ensure!(
        (evidence.prepares) == (1),
        "native managed fixture equality failed"
    );
    ensure!(
        (evidence.finalizations.len()) == (1),
        "native managed fixture equality failed"
    );
    ensure!(
        (evidence.commands.len()) == (commands),
        "native managed fixture equality failed"
    );
    ensure!(
        (evidence.bound) == (bound),
        "native managed fixture equality failed"
    );
    ensure!(
        (evidence.contexts.len()) == (count),
        "native managed fixture equality failed"
    );
    let ids = evidence
        .contexts
        .iter()
        .map(|context| context.client_order_id)
        .collect::<BTreeSet<_>>();
    ensure!(
        (ids.len()) == (count),
        "native managed fixture equality failed"
    );
    let outcomes = &evidence.finalizations[0];
    ensure!(
        (outcomes.len()) == (count),
        "native managed fixture equality failed"
    );
    for (context, outcome) in evidence.contexts.iter().zip(outcomes) {
        ensure!(
            (context.client_order_id) == (outcome.factory_order_ids[0]),
            "native managed fixture equality failed"
        );
    }
    if matches!(case, "native-panic" | "binding-failed") {
        ensure!(
            (outcomes[0].status) == (ManagedSubmissionStatus::NativeAccepted),
            "native managed fixture equality failed"
        );
        ensure!(
            (outcomes[1].status) == (ManagedSubmissionStatus::NativeUnknown),
            "native managed fixture equality failed"
        );
        ensure!(
            (outcomes[2].status) == (ManagedSubmissionStatus::NativeRejected),
            "native managed fixture equality failed"
        );
    }
    if case == "market-exit-cached-denial" {
        let cache = strategy.core.cache_rc();
        let cache = cache.borrow();
        for (context, outcome) in evidence.contexts.iter().zip(outcomes) {
            ensure!(
                outcome.status == ManagedSubmissionStatus::NativeAccepted
                    && outcome.cache_contains_order
            );
            ensure!(
                cache
                    .order(&context.client_order_id)
                    .is_some_and(|order| order.status() == OrderStatus::Denied)
            );
        }
    }
    if case == "prepare-panic" {
        ensure!(
            outcomes
                .iter()
                .all(|outcome| outcome.status == ManagedSubmissionStatus::NativeUnknown)
        );
    }
    Ok(())
}

#[test]
fn batch_refuses_foreign_scope_and_bound_before_preparing_or_allocating() -> Result<()> {
    for (case, count, foreign) in [
        ("batch-disabled", 0, false),
        ("batch-disabled", 1, false),
        ("all-accepted", 4, false),
        ("all-accepted", 1, true),
    ] {
        let (mut strategy, evidence) = fixture(case)?;
        let mut intents = vec![intent(); count];
        if foreign {
            intents[0]["instrument_id"] = json!("BTCUSDT-PERP.BINANCE");
        }
        ensure!(
            strategy
                .managed_submit_batch(&serde_json::to_vec(
                    &json!({"schema_version":1,"intents":intents})
                )?)
                .is_err()
        );
        let evidence = evidence.borrow();
        ensure!(
            evidence.prepares == 0
                && evidence.commands.is_empty()
                && evidence.finalizations.is_empty()
        );
    }
    Ok(())
}

#[test]
fn native_market_exit_denial_is_cached_and_finalized_with_actual_status() -> Result<()> {
    let (mut strategy, evidence) = fixture("market-exit-cached-denial")?;
    let result: Value =
        serde_json::from_slice(&strategy.managed_submit(&serde_json::to_vec(&intent())?)?)?;
    let evidence = evidence.borrow();
    ensure!(evidence.finalizations.len() == 1 && evidence.commands.is_empty());
    let context = &evidence.contexts[0];
    let outcome = &evidence.finalizations[0][0];
    ensure!(
        result["native_outcome"]["status"] == "native_accepted" && outcome.cache_contains_order
    );
    ensure!(
        strategy
            .core
            .cache_rc()
            .borrow()
            .order(&context.client_order_id)
            .is_some_and(|order| order.status() == OrderStatus::Denied)
    );
    Ok(())
}

#[test]
fn successful_missing_cache_result_calls_same_policy_unknown_finalizer_before_fatal() -> Result<()>
{
    let (mut strategy, evidence) = fixture("all-rejected")?;
    let id = strategy.order_factory().generate_client_order_id();
    let context = ManagedSubmissionContext {
        trader_id: strategy.core.trader_id().unwrap(),
        strategy_id: strategy.core.strategy_id().unwrap(),
        account_id: strategy.scope.account_id,
        client_id: strategy.scope.client_id,
        instrument_id: *strategy.scope.instruments.iter().next().unwrap(),
        side: OrderSide::Buy,
        client_order_id: id,
        timestamp_ns: 0,
    };
    // Exercise the production classifier/finalizer's defensive contract. No
    // actual native denial is misrepresented as an absent-cache submission.
    let (outcome, failure) =
        classify_managed_attempt(&context, true, vec![id], Vec::new(), None, Ok(Ok(())));
    ensure!(
        outcome.status == ManagedSubmissionStatus::NativeUnknown && !outcome.cache_contains_order
    );
    let raw = serde_json::to_vec(&intent())?;
    let error = finish_managed_single(
        &context,
        strategy.managed_policy.as_ref().unwrap().policy.as_ref(),
        &raw,
        &outcome,
        failure,
    )
    .unwrap_err();
    ensure!(
        error.downcast_ref::<NativeManagedFailure>().unwrap().code
            == PluginErrorCode::SerializationFailed
    );
    let evidence = evidence.borrow();
    ensure!(evidence.finalizations.len() == 1 && evidence.commands.is_empty());
    ensure!(
        evidence.finalizations[0][0].status == ManagedSubmissionStatus::NativeUnknown
            && evidence.finalizations[0][0].factory_order_ids == vec![id]
    );
    Ok(())
}

#[rstest::rstest]
#[case("bracket-market", true)]
#[case("bracket-limit", true)]
#[case("bracket-stop-market", true)]
#[case("bracket-stop-limit", true)]
#[case("bracket-foreign-child", false)]
#[case("bracket-quantity-mismatch", false)]
#[case("bracket-manager-missing", false)]
fn actual_bracket_uses_native_linked_children_and_final_interceptor(
    #[case] case: &'static str,
    #[case] native_list: bool,
) -> Result<()> {
    let (mut strategy, evidence) = fixture(case)?;
    let result: Value =
        serde_json::from_slice(&strategy.managed_submit(&serde_json::to_vec(&intent())?)?)?;
    ensure!(
        (result["native_outcome"]["status"]) == ("native_rejected"),
        "native managed fixture equality failed"
    );
    let evidence = evidence.borrow();
    ensure!(
        (evidence.finalizations.len()) == (1),
        "native managed fixture equality failed"
    );
    ensure!(
        (evidence.commands.len()) == (usize::from(native_list)),
        "native managed fixture equality failed"
    );
    if native_list {
        let TradingCommand::SubmitOrderList(command) = &evidence.commands[0] else {
            bail!("native bracket changed its command shape")
        };
        ensure!(
            (command.client_id) == (Some("MANAGED-EXEC".into())),
            "native managed fixture equality failed"
        );
        ensure!(
            (command.order_inits.len()) == (3),
            "native managed fixture equality failed"
        );
        let orders = &command.order_inits;
        ensure!(
            (orders[0].client_order_id) == (evidence.contexts[0].client_order_id),
            "native managed fixture equality failed"
        );
        ensure!(
            (orders[0].contingency_type) == (Some(ContingencyType::Oto)),
            "native managed fixture equality failed"
        );
        ensure!(
            (orders[1].parent_order_id) == (Some(orders[0].client_order_id)),
            "native managed fixture equality failed"
        );
        ensure!(
            (orders[2].parent_order_id) == (Some(orders[0].client_order_id)),
            "native managed fixture equality failed"
        );
        for child in &orders[1..] {
            ensure!(
                (child.contingency_type) == (Some(ContingencyType::Ouo)),
                "native managed fixture equality failed"
            );
            ensure!(child.reduce_only);
            ensure!(
                (child.quantity) == (orders[0].quantity),
                "native managed fixture equality failed"
            );
        }
        let outcome = &evidence.finalizations[0][0];
        ensure!(
            (outcome.factory_order_ids.len()) == (3),
            "native managed fixture equality failed"
        );
        ensure!(outcome.cached_order_ids.is_empty());
        ensure!(
            (orders[0].trigger_type)
                == (if case.contains("stop") {
                    Some(TriggerType::MarkPrice)
                } else {
                    None
                }),
            "native managed fixture equality failed"
        );
    }
    Ok(())
}

#[test]
fn admitted_cached_warmup_never_starts_external_history_or_returns_future_bars() -> Result<()> {
    use nautilus_core::UnixNanos;
    use nautilus_model::data::{Bar, BarType};
    use nautilus_plugin::strategy::HostOperation;
    let (mut strategy, _) = fixture("all-rejected")?;
    let bar_type: BarType = "ETHUSDT-PERP.BINANCE-4-HOUR-LAST-EXTERNAL".parse()?;
    strategy.scope.bar_types = vec![bar_type];
    strategy.scope.max_history_bars = 8;
    let cache = strategy.core.cache_rc();
    for timestamp in [0u64, 1] {
        cache.borrow_mut().add_bar(Bar {
            bar_type,
            ts_event: UnixNanos::from(timestamp),
            ts_init: UnixNanos::from(timestamp),
            ..Bar::default()
        })?;
    }
    let query = serde_json::to_vec(&json!({"schema_version":1,"bar_type":bar_type,"limit":8}))?;
    let response: Value =
        serde_json::from_slice(&strategy.dispatch(HostOperation::CachedBars, &query)?)?;
    ensure!(
        response["bars"]
            .as_array()
            .is_some_and(|bars| bars.len() == 1)
            && response["timestamp_ns"] == 0
            && response["network_requested"] == false
    );
    for limit in [0, 9] {
        ensure!(
            strategy
                .dispatch(
                    HostOperation::CachedBars,
                    &serde_json::to_vec(
                        &json!({"schema_version":1,"bar_type":bar_type,"limit":limit})
                    )?
                )
                .is_err()
        );
    }
    ensure!(
        strategy
            .dispatch(HostOperation::RequestHistory, b"{")
            .is_err()
    );
    ensure!(strategy.adapter.pending_history.is_none() && cache.borrow().bar_count(&bar_type) == 2);
    Ok(())
}

#[test]
fn recorded_native_fault_finalizes_cached_submission_unknown_on_original_policy() -> Result<()> {
    let (mut strategy, evidence) = fixture("all-rejected")?;
    let id = strategy.order_factory().generate_client_order_id();
    let context = ManagedSubmissionContext {
        trader_id: strategy.core.trader_id().unwrap(),
        strategy_id: strategy.core.strategy_id().unwrap(),
        account_id: strategy.scope.account_id,
        client_id: strategy.scope.client_id,
        instrument_id: *strategy.scope.instruments.iter().next().unwrap(),
        side: OrderSide::Buy,
        client_order_id: id,
        timestamp_ns: 0,
    };
    let (outcome, failure) = classify_managed_attempt(
        &context,
        true,
        vec![id],
        vec![id],
        Some((
            nautilus_plugin::PluginErrorCode::SerializationFailed,
            "original native callback fault".into(),
        )),
        Ok(Ok(())),
    );
    ensure!(
        outcome.status == ManagedSubmissionStatus::NativeUnknown && outcome.cache_contains_order
    );
    let raw = serde_json::to_vec(&intent())?;
    let error = finish_managed_single(
        &context,
        strategy.managed_policy.as_ref().unwrap().policy.as_ref(),
        &raw,
        &outcome,
        failure,
    )
    .unwrap_err();
    ensure!(error.to_string().contains("original native callback fault"));
    let evidence = evidence.borrow();
    ensure!(
        evidence.finalizations.len() == 1
            && evidence.finalizations[0][0].status == ManagedSubmissionStatus::NativeUnknown
    );
    ensure!(evidence.finalizations[0][0].factory_order_ids == vec![id]);
    Ok(())
}
