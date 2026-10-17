// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Actual native factory/cache/interceptor paths; no exchange or economic ledger.
use super::{
    ManagedBracketRequest, ManagedOrderFacts, ManagedOrderObservationContext, ManagedOrderPlan,
    ManagedPolicyIdentity, ManagedSubmissionContext, ManagedSubmissionOutcome,
    ManagedSubmissionPolicy, ManagedSubmissionStatus, NativeManagedFailure,
    classify_managed_attempt, finish_managed_single,
};
use crate::{
    plugin::{PluginOrderType, PluginScope, PluginStrategy, wire},
    strategy::{PreparedSubmission, StrategyConfig, StrategyNative, SubmissionInterceptor},
};
use anyhow::{Context, Result, bail, ensure};
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

#[derive(Debug)]
struct SwallowReadOnlyFailure(Empty);
impl Algorithm for SwallowReadOnlyFailure {
    const DESCRIPTOR_JSON: &'static str = "{}";
    fn validate(raw: &[u8]) -> Result<(), PluginError> {
        Empty::validate(raw)
    }
    fn create(raw: &[u8]) -> Result<Self, PluginError> {
        Empty::create(raw).map(Self)
    }
    fn start(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.start(h)
    }
    fn on_event(&mut self, raw: &[u8], h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.on_event(raw, h)
    }
    fn save(&self, h: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        let _original_ignored_result = h.call(
            nautilus_plugin::strategy::ReadOperation::ManagedOrderFacts,
            br#"{"schema_version":1,"client_order_id":"original-protection-1"}"#,
        );
        self.0.save(h)
    }
    fn restore(&mut self, v: u32, raw: &[u8], h: &mut ReadOnlyHost<'_>) -> Result<(), PluginError> {
        self.0.restore(v, raw, h)
    }
    fn pause(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.pause(h)
    }
    fn resume(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.resume(h)
    }
    fn stop(&mut self, raw: &[u8], h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.stop(raw, h)
    }
}
static OBSERVER_API: StrategyApi = StrategyApi::for_algorithm::<SwallowReadOnlyFailure>();

#[test]
fn readonly_managed_owner_panic_cannot_be_swallowed_during_native_save() -> Result<()> {
    let (mut strategy, evidence) = fixture("observe-save-panic")?;
    observation_order(&strategy, "owned")?;
    let original = strategy.invoke(nautilus_plugin::strategy::CallbackKind::Save, b"");
    ensure!(
        original.is_err(),
        "native save swallowed the original managed observation owner panic"
    );
    let first = strategy
        .callback_failure()
        .context("native save lost original first failure")?;
    ensure!(
        first.0 == PluginErrorCode::Panic
            && first.1.contains("original managed observation owner panic"),
        "native save changed original failure classification"
    );
    ensure!(
        strategy
            .invoke(nautilus_plugin::strategy::CallbackKind::Save, b"")
            .is_err()
            && strategy.callback_failure() == Some(first),
        "later save erased original financial owner failure"
    );
    let e = evidence.borrow();
    ensure!(
        e.observations.len() == 1
            && e.commands.is_empty()
            && e.prepares == 0
            && e.finalizations.is_empty(),
        "failed read-only save reached mutation path"
    );
    Ok(())
}
fn injected_prepare_panic() -> ! {
    panic!("original portfolio preparation panic");
}
fn injected_native_panic() -> ! {
    panic!("original batch native submission panic");
}
#[derive(Debug, Default)]
struct Evidence {
    observations: Vec<ManagedOrderObservationContext>,
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
        tags: None,
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
        if self.case == "bracket-tags" {
            parent.tags = Some(vec!["ADX_ENTRY".into()]);
            take_profit.tags = Some(vec!["ADX_BREAKOUT_PORTFOLIO_TP".into()]);
            stop_loss.tags = Some(vec!["ADX_BREAKOUT_PORTFOLIO_SL".into()]);
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
    fn observe_order(&self, context: &ManagedOrderObservationContext) -> Result<ManagedOrderFacts> {
        self.evidence
            .borrow_mut()
            .observations
            .push(context.clone());
        if self.case == "observation-error" {
            bail!("original financial observation unavailable");
        }
        if matches!(self.case, "observation-panic" | "observe-save-panic") {
            observation_panic();
        }
        Ok(ManagedOrderFacts {
            pending_exit_amendment: self.case == "observed-pending",
            native_liquidation_reduced_protection: self.case == "observed-native-liquidation",
        })
    }
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

fn observation_panic() -> ! {
    panic!("original managed observation owner panic");
}

fn observation_order(strategy: &PluginStrategy, case: &str) -> Result<()> {
    use nautilus_common::actor::DataActorNative;
    use nautilus_core::UUID4;
    use nautilus_model::{
        events::{OrderAccepted, OrderEventAny, OrderSubmitted},
        orders::{MarketOrder, OrderAny},
    };
    let strategy_id = if case == "foreign-strategy" {
        "FOREIGN-001"
    } else {
        "MANAGED-001"
    }
    .into();
    let trader = if case == "foreign-trader" {
        "FOREIGN-TRADER"
    } else {
        "TRADER-001"
    }
    .into();
    let account = if case == "foreign-account" {
        "BINANCE-FOREIGN"
    } else {
        "BINANCE-MANAGED"
    }
    .into();
    let instrument = if case == "foreign-instrument" {
        "BTCUSDT-PERP.BINANCE"
    } else {
        "ETHUSDT-PERP.BINANCE"
    }
    .into();
    let id = "original-protection-1".into();
    let order: OrderAny = MarketOrder::new_checked(
        trader,
        strategy_id,
        instrument,
        id,
        OrderSide::Sell,
        Quantity::from("0.001"),
        TimeInForce::Gtc,
        UUID4::new(),
        0.into(),
        case != "risk-entry",
        false,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )?
    .into();
    let cache = strategy.cache_rc();
    let mut cache = cache.borrow_mut();
    cache.add_order(
        order,
        None,
        if case == "unbound-client" {
            None
        } else {
            Some(
                if case == "foreign-client" {
                    "FOREIGN-EXEC"
                } else {
                    "MANAGED-EXEC"
                }
                .into(),
            )
        },
        false,
    )?;
    if case != "unbound-account" {
        cache.update_order(&OrderEventAny::Submitted(OrderSubmitted::new(
            trader,
            strategy_id,
            instrument,
            id,
            account,
            UUID4::new(),
            0.into(),
            0.into(),
        )))?;
        cache.update_order(&OrderEventAny::Accepted(OrderAccepted::new(
            trader,
            strategy_id,
            instrument,
            id,
            "original-venue-1".into(),
            account,
            UUID4::new(),
            0.into(),
            0.into(),
            false,
        )))?;
    }
    Ok(())
}

#[rstest::rstest]
fn ordinary_order_operations_refuse_foreign_client_and_trader_before_native_effects(
    #[values("foreign-client", "unbound-client", "foreign-trader")] case: &'static str,
    #[values(
        nautilus_plugin::strategy::HostOperation::Query,
        nautilus_plugin::strategy::HostOperation::Cancel,
        nautilus_plugin::strategy::HostOperation::Modify
    )]
    operation: nautilus_plugin::strategy::HostOperation,
) -> Result<()> {
    use nautilus_common::actor::DataActorNative;
    let (mut strategy, evidence) = fixture("observed-pending")?;
    observation_order(&strategy, case)?;
    let id = "original-protection-1".into();
    let cache = strategy.cache_rc();
    let before = {
        let cache = cache.borrow();
        let order = cache.order(&id).context("original native order missing")?;
        serde_json::to_value(order.as_ref())?
    };
    let count = strategy.order_factory().client_order_id_count();
    let request = if operation == nautilus_plugin::strategy::HostOperation::Modify {
        json!({"schema_version":1,"client_order_id":id,"quantity":"0.002","price":null,"trigger_price":null})
    } else {
        json!({"schema_version":1,"client_order_id":id})
    };
    let result = strategy.dispatch(operation, &serde_json::to_vec(&request)?);
    ensure!(
        result.is_err(),
        "{case} {operation:?} crossed original native ownership"
    );
    let cache = cache.borrow();
    let original = cache.order(&id).context("original order disappeared")?;
    ensure!(
        serde_json::to_value(original.as_ref())? == before
            && strategy.order_factory().client_order_id_count() == count,
        "refused foreign operation changed original order or identity"
    );
    let e = evidence.borrow();
    ensure!(
        e.prepares == 0
            && e.bound == 0
            && e.commands.is_empty()
            && e.finalizations.is_empty()
            && e.observations.is_empty(),
        "foreign operation reached the original submission owner"
    );
    Ok(())
}

#[rstest::rstest]
#[case(wire::NativeOrderType::Market)]
#[case(wire::NativeOrderType::Limit)]
#[case(wire::NativeOrderType::StopMarket)]
#[case(wire::NativeOrderType::StopLimit)]
fn ordinary_native_factory_preserves_original_algorithm_tags(
    #[case] kind: wire::NativeOrderType,
) -> Result<()> {
    let (mut strategy, evidence) = fixture("algorithm-tags")?;
    let limit = matches!(
        kind,
        wire::NativeOrderType::Limit | wire::NativeOrderType::StopLimit
    );
    let stop = matches!(
        kind,
        wire::NativeOrderType::StopMarket | wire::NativeOrderType::StopLimit
    );
    let request = wire::SubmitRequest {
        schema_version: 2,
        order_type: kind,
        instrument_id: "ETHUSDT-PERP.BINANCE".into(),
        side: OrderSide::Sell,
        quantity: Quantity::from("0.001"),
        price: limit.then(|| Price::from("3000.00")),
        trigger_price: stop.then(|| Price::from("2900.00")),
        trigger_type: stop.then_some(TriggerType::Default),
        time_in_force: TimeInForce::Gtc,
        expire_time_ns: None,
        reduce_only: true,
        post_only: false,
        position_id: None,
        tags: Some(vec![
            if stop {
                "ADX_BREAKOUT_PORTFOLIO_SL"
            } else {
                "ADX_BREAKOUT_PORTFOLIO_TP"
            }
            .into(),
        ]),
    };
    let result = strategy.submit_native_request(&request, None)?;
    let id: wire::OrderId = serde_json::from_slice(&result)?;
    let original = strategy.owned_order(id.client_order_id)?;
    let tags: Vec<_> = original
        .tags()
        .context("native tags lost")?
        .iter()
        .map(ToString::to_string)
        .collect();
    ensure!(
        Some(tags) == request.tags && original.is_reduce_only(),
        "native order annotations changed"
    );
    ensure!(
        evidence.borrow().bound == 1,
        "tagged order bypassed the original interceptor"
    );
    Ok(())
}

#[test]
fn invalid_or_framework_reserved_tags_are_refused_before_native_identity_and_interceptor()
-> Result<()> {
    let (mut strategy, evidence) = fixture("algorithm-tags")?;
    let before = strategy.order_factory().client_order_id_count();
    for tags in [
        vec![],
        vec![String::new()],
        vec!["x".repeat(129)],
        vec!["line\nbreak".into()],
        vec!["duplicate".into(), "duplicate".into()],
        (0..33).map(|n| format!("tag{n}")).collect(),
        vec!["MARKET_EXIT".into()],
    ] {
        let mut request: wire::SubmitRequest = serde_json::from_value(json!({
            "schema_version":2,"order_type":"market","instrument_id":"ETHUSDT-PERP.BINANCE",
            "side":"SELL","quantity":"0.001","time_in_force":"GTC","reduce_only":true,"post_only":false
        }))?;
        request.tags = Some(tags);
        ensure!(
            strategy.submit_native_request(&request, None).is_err(),
            "invalid tags reached native construction"
        );
    }
    ensure!(
        strategy.order_factory().client_order_id_count() == before
            && evidence.borrow().bound == 0
            && evidence.borrow().prepares == 0,
        "invalid tags consumed native identity or reached the original interceptor"
    );
    Ok(())
}

#[rstest::rstest]
#[case("observed-pending", true, false)]
#[case("observed-settled", false, false)]
#[case("observed-native-liquidation", false, true)]
fn managed_observation_uses_original_native_scope_without_generating_identity(
    #[case] case: &'static str,
    #[case] pending: bool,
    #[case] native_liquidation: bool,
) -> Result<()> {
    use nautilus_plugin::strategy::HostOperation;
    let (mut strategy, evidence) = fixture(case)?;
    observation_order(&strategy, "owned")?;
    let before = strategy.order_factory().client_order_id_count();
    let original = strategy.owned_order("original-protection-1".into())?;
    let input = br#"{"schema_version":1,"client_order_id":"original-protection-1"}"#;
    let raw = strategy.readonly(HostOperation::ManagedOrderFacts, input)?;
    let result: Value = serde_json::from_slice(&raw)?;
    ensure!(
        result["facts"]["pending_exit_amendment"] == pending
            && result["facts"]["native_liquidation_reduced_protection"] == native_liquidation
            && result["context"]["client_order_id"] == "original-protection-1"
            && result["context"]["strategy_id"] == "MANAGED-001"
            && result["context"]["account_id"] == "BINANCE-MANAGED"
            && result["context"]["client_id"] == "MANAGED-EXEC"
            && result["context"]["timestamp_ns"] == 0
            && result["execution_authorized"] == false
            && result["native_identity_generated"] == false,
        "original observation scope changed"
    );
    ensure!(
        strategy.order_factory().client_order_id_count() == before
            && strategy
                .owned_order("original-protection-1".into())?
                .events()
                == original.events(),
        "read-only observation advanced native identity or order state"
    );
    let e = evidence.borrow();
    ensure!(
        e.observations.len() == 1
            && e.prepares == 0
            && e.bound == 0
            && e.commands.is_empty()
            && e.finalizations.is_empty(),
        "observation invoked a mutation path"
    );
    Ok(())
}

#[rstest::rstest]
#[case("foreign-strategy")]
#[case("foreign-account")]
#[case("foreign-trader")]
#[case("foreign-instrument")]
#[case("risk-entry")]
#[case("unbound-account")]
#[case("foreign-client")]
#[case("unbound-client")]
fn managed_observation_refuses_foreign_or_unbound_orders_before_the_shared_owner(
    #[case] case: &'static str,
) -> Result<()> {
    let (strategy, evidence) = fixture("observed-pending")?;
    observation_order(&strategy, case)?;
    ensure!(
        strategy
            .managed_order_facts(
                br#"{"schema_version":1,"client_order_id":"original-protection-1"}"#
            )
            .is_err(),
        "{case} observed another scope"
    );
    ensure!(
        evidence.borrow().observations.is_empty(),
        "foreign observation reached economic owner"
    );
    Ok(())
}

#[rstest::rstest]
#[case("observation-error", false)]
#[case("observation-panic", true)]
fn managed_observation_preserves_original_owner_errors_without_allocating(
    #[case] case: &'static str,
    #[case] panic: bool,
) -> Result<()> {
    let (strategy, evidence) = fixture(case)?;
    observation_order(&strategy, "owned")?;
    let failure = strategy
        .managed_order_facts(br#"{"schema_version":1,"client_order_id":"original-protection-1"}"#)
        .unwrap_err();
    ensure!(
        failure.to_string().contains(if panic {
            "original managed observation owner panic"
        } else {
            "original financial observation unavailable"
        }),
        "original observation error lost"
    );
    ensure!(
        failure.downcast_ref::<NativeManagedFailure>().is_some() == panic,
        "observation panic classification lost"
    );
    let e = evidence.borrow();
    ensure!(
        e.observations.len() == 1
            && e.prepares == 0
            && e.finalizations.is_empty()
            && e.commands.is_empty(),
        "observation failure mutated funds"
    );
    Ok(())
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
            if case == "framework-timer-route" {
                &raw const TIMER_API
            } else if case == "observe-save-panic" {
                &raw const OBSERVER_API
            } else {
                &raw const API
            },
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
#[case("owned", true)]
#[case("foreign-client", false)]
#[case("unbound-client", false)]
#[case("foreign-account", false)]
#[case("foreign-trader", false)]
fn scoped_market_exit_reuses_original_native_cancel_and_timer_without_foreign_effects(
    #[case] order_scope: &'static str,
    #[case] canceled: bool,
) -> Result<()> {
    use crate::strategy::Strategy;
    use nautilus_common::{
        actor::registry::{deregister_actor, try_get_actor_unchecked},
        component::{deregister_component, register_component_actor, start_component},
        msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
        timer::TimeEventCallback,
    };
    use nautilus_plugin::strategy::HostOperation;
    let (mut strategy, evidence) = fixture("scoped-market-exit")?;
    observation_order(&strategy, order_scope)?;
    let (handler, cancellations) = get_typed_into_message_saving_handler::<TradingCommand>(None);
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::exec_engine_queue_execute(),
        handler,
    );
    strategy
        .core
        .clock_mut()
        .register_default_handler(TimeEventCallback::from(|_| {}));
    let actor_id = strategy.component_id().inner();
    register_component_actor(strategy);
    start_component(&actor_id)?;
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original native registered exit actor is missing")?;
    let request = br#"{"schema_version":1}"#;
    let result: Value =
        serde_json::from_slice(&strategy.dispatch(HostOperation::ManagedMarketExit, request)?)?;
    ensure!(
        result["native_framework_requested"] == true
            && result["is_exiting"] == true
            && result["execution_authorized"] == false
            && strategy.is_exiting(),
        "managed exit did not use the original native framework"
    );
    let commands = cancellations.get_messages();
    let count = commands.len();
    ensure!(
        count == usize::from(canceled),
        "native exit changed foreign order scope"
    );
    if canceled {
        let TradingCommand::CancelOrder(command) = &commands[0] else {
            bail!("native scoped exit did not use original cancellation");
        };
        ensure!(
            command.client_id == Some("MANAGED-EXEC".into())
                && command.client_order_id.as_str() == "original-protection-1",
            "native scoped cancellation changed original route/identity"
        );
    }
    strategy.dispatch(HostOperation::ManagedMarketExit, request)?;
    ensure!(
        cancellations.get_messages().len() == count,
        "repeated exit resent original cancellation"
    );
    ensure!(
        evidence.borrow().commands.is_empty(),
        "native cancellation entered a new entry allocation"
    );
    strategy.cancel_market_exit();
    Component::stop(&mut *strategy)?;
    drop(strategy);
    deregister_component(&actor_id);
    deregister_actor(&actor_id);
    Ok(())
}

#[test]
fn scoped_market_exit_rejects_guest_scope_and_nonrunning_requests_before_native_effects()
-> Result<()> {
    use nautilus_plugin::strategy::HostOperation;
    let (mut strategy, evidence) = fixture("scoped-market-exit")?;
    for raw in [
        br#"{"schema_version":1}"#.as_slice(),
        br#"{"schema_version":2}"#.as_slice(),
        br#"{"schema_version":1,"account_id":"FOREIGN"}"#.as_slice(),
        br#"{"schema_version":1,"client_id":"FOREIGN"}"#.as_slice(),
    ] {
        ensure!(
            strategy
                .dispatch(HostOperation::ManagedMarketExit, raw)
                .is_err(),
            "invalid scope/lifecycle started native exit"
        );
    }
    ensure!(
        !strategy.core.is_exiting && evidence.borrow().commands.is_empty(),
        "rejected exit changed the original native owner"
    );
    Ok(())
}

#[rstest::rstest]
#[case("owned", true)]
#[case("foreign-account", false)]
#[case("foreign-trader", false)]
#[case("foreign-strategy", false)]
#[case("foreign-instrument", false)]
fn scoped_market_exit_closes_only_original_positions_and_retries_on_native_timer(
    #[case] position_scope: &'static str,
    #[case] closed: bool,
) -> Result<()> {
    use crate::strategy::Strategy;
    use nautilus_common::{
        actor::registry::{deregister_actor, try_get_actor_unchecked},
        component::{deregister_component, register_component_actor, start_component},
        msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
        timer::{TimeEvent, TimeEventCallback},
    };
    use nautilus_core::UUID4;
    use nautilus_model::{
        enums::OmsType, events::order::spec::OrderFilledSpec, identifiers::PositionId,
        position::Position,
    };
    use nautilus_plugin::strategy::HostOperation;

    let (mut strategy, evidence) = fixture("scoped-market-exit")?;
    let mut instrument = crypto_perpetual_ethusdt();
    if position_scope == "foreign-instrument" {
        instrument.id = "BTCUSDT-PERP.BINANCE".into();
    }
    let instrument = InstrumentAny::CryptoPerpetual(instrument);
    let position_id = PositionId::from("original-exit-position");
    let fill = OrderFilledSpec::builder()
        .trader_id(if position_scope == "foreign-trader" {
            "FOREIGN-TRADER".into()
        } else {
            "TRADER-001".into()
        })
        .strategy_id(if position_scope == "foreign-strategy" {
            "FOREIGN-001".into()
        } else {
            "MANAGED-001".into()
        })
        .account_id(if position_scope == "foreign-account" {
            "BINANCE-FOREIGN".into()
        } else {
            "BINANCE-MANAGED".into()
        })
        .instrument_id(instrument.id())
        .client_order_id("original-position-opening-fill".into())
        .position_id(position_id)
        .last_qty(Quantity::from("0.003"))
        .last_px(Price::from("2000.00"))
        .currency(instrument.quote_currency())
        .build();
    let position = Position::new(&instrument, fill);
    strategy
        .core
        .cache_rc()
        .borrow_mut()
        .add_position_without_order(&position, OmsType::Netting)?;

    let (handler, submissions) = get_typed_into_message_saving_handler::<TradingCommand>(None);
    msgbus::register_trading_command_endpoint(
        MessagingSwitchboard::risk_engine_queue_execute(),
        handler,
    );
    strategy
        .core
        .clock_mut()
        .register_default_handler(TimeEventCallback::from(|_| {}));
    let actor_id = strategy.component_id().inner();
    register_component_actor(strategy);
    start_component(&actor_id)?;
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original native registered position owner is missing")?;
    let request = br#"{"schema_version":1}"#;
    strategy.dispatch(HostOperation::ManagedMarketExit, request)?;
    let commands = submissions.get_messages();
    ensure!(
        commands.len() == usize::from(closed),
        "native position exit affected foreign scope: {commands:?}"
    );
    let event = || {
        TimeEvent::new(
            "MARKET_EXIT_CHECK:MANAGED-001".into(),
            UUID4::new(),
            1.into(),
            1.into(),
        )
    };
    if closed {
        let TradingCommand::SubmitOrder(first) = &commands[0] else {
            bail!("native exit did not submit an original market close");
        };
        let first_order = strategy
            .core
            .cache_ref()
            .try_order_owned(&first.client_order_id)?;
        ensure!(
            first.client_id == Some("MANAGED-EXEC".into())
                && first.position_id == Some(position_id)
                && first_order.order_side() == OrderSide::Sell
                && first_order.quantity() == Quantity::from("0.003")
                && first_order.is_reduce_only()
                && first_order
                    .tags()
                    .is_some_and(|tags| { tags.len() == 1 && tags[0].as_str() == "MARKET_EXIT" }),
            "native exit changed original position, route, quantity or protective tag"
        );
        strategy.dispatch(HostOperation::ManagedMarketExit, request)?;
        strategy.check_market_exit(event());
        ensure!(
            submissions.get_messages().len() == 1 && strategy.is_exiting(),
            "initialized native close was duplicated before its outcome"
        );
        strategy.deny_order(&first_order, "private original native denial".into());
        strategy.check_market_exit(event());
        let retried = submissions.get_messages();
        ensure!(
            retried.len() == 2,
            "original native timer did not retry denied close"
        );
        let TradingCommand::SubmitOrder(second) = &retried[1] else {
            bail!("native timer retry did not use original market submit");
        };
        let second_order = strategy
            .core
            .cache_ref()
            .try_order_owned(&second.client_order_id)?;
        ensure!(
            second.client_id == first.client_id
                && second.position_id == first.position_id
                && second_order.quantity() == first_order.quantity()
                && second_order.is_reduce_only()
                && second_order.client_order_id() != first_order.client_order_id(),
            "native timer retry changed original scope or reused terminal native identity"
        );
    } else {
        strategy.check_market_exit(event());
        ensure!(
            !strategy.is_exiting() && submissions.get_messages().is_empty(),
            "foreign positions kept scoped native exit active or received commands"
        );
    }
    ensure!(
        evidence.borrow().prepares == 0,
        "native risk-reduction exit created an additional entry allocation"
    );
    ensure!(
        !strategy
            .core
            .cache_ref()
            .position(&position_id)
            .context("original native position disappeared")?
            .is_closed(),
        "no-execution fixture fabricated an actual position close"
    );
    strategy.cancel_market_exit();
    Component::stop(&mut *strategy)?;
    drop(strategy);
    deregister_component(&actor_id);
    deregister_actor(&actor_id);
    Ok(())
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
#[case("bracket-tags", true)]
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
        if case == "bracket-tags" {
            for order in orders {
                let expected = match order.order_type {
                    nautilus_model::enums::OrderType::Market => "ADX_ENTRY",
                    nautilus_model::enums::OrderType::Limit => "ADX_BREAKOUT_PORTFOLIO_TP",
                    nautilus_model::enums::OrderType::StopMarket => "ADX_BREAKOUT_PORTFOLIO_SL",
                    _ => bail!("native bracket changed the original leg type"),
                };
                ensure!(
                    order
                        .tags
                        .as_ref()
                        .is_some_and(|tags| tags.len() == 1 && tags[0].as_str() == expected),
                    "original native bracket leg annotation changed"
                );
            }
        }
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

#[test]
fn native_frame_adapter_migrates_only_explicit_legacy_no_signal_inventory() -> Result<()> {
    let (strategy, _) = fixture("all-rejected")?;
    let current = serde_json::to_value(strategy.adapter_checkpoint()?)?;
    ensure!(
        current["schema_version"] == 5 && current["managed_frame_subscribed"] == false,
        "new native frame inventory is not explicit"
    );
    let mut legacy = current.clone();
    legacy["schema_version"] = json!(4);
    legacy
        .as_object_mut()
        .unwrap()
        .remove("managed_frame_subscribed");
    let restored = crate::plugin::adapter::AdapterState::decode(
        &serde_json::to_vec(&legacy)?,
        &strategy.scope,
    )?;
    ensure!(
        serde_json::to_value(restored)? == current,
        "legacy no-signal inventory changed another resource"
    );
    let mut missing = current;
    missing
        .as_object_mut()
        .unwrap()
        .remove("managed_frame_subscribed");
    ensure!(
        crate::plugin::adapter::AdapterState::decode(
            &serde_json::to_vec(&missing)?,
            &strategy.scope
        )
        .is_err(),
        "new native checkpoint inferred a missing frame inventory"
    );
    Ok(())
}

#[test]
fn native_frame_restore_rejects_a_signal_without_its_bounded_native_policy() -> Result<()> {
    use nautilus_common::actor::DataActor;
    let (source, _) = fixture("batch-disabled")?;
    let mut saved = source.on_save()?;
    let mut adapter: Value = serde_json::from_slice(saved.get("plugin.adapter").unwrap())?;
    adapter["managed_frame_subscribed"] = json!(true);
    saved.insert("plugin.adapter".into(), serde_json::to_vec(&adapter)?);
    let (mut target, evidence) = fixture("batch-disabled")?;
    ensure!(
        target.on_load(saved).is_err(),
        "unbound native frame restored"
    );
    ensure!(
        evidence.borrow().prepares == 0 && evidence.borrow().commands.is_empty(),
        "invalid native frame restore changed policy or execution"
    );
    Ok(())
}

#[test]
fn native_frame_refuses_foreign_owner_and_oversized_payload_before_guest() -> Result<()> {
    use nautilus_common::{actor::DataActor, signal::Signal};
    use nautilus_core::UnixNanos;
    for oversized in [false, true] {
        let (mut strategy, evidence) = fixture("all-rejected")?;
        strategy.adapter.managed_frame_subscribed = true;
        let owner = if oversized {
            "MANAGED-001"
        } else {
            "FOREIGN-001"
        };
        let name = crate::plugin::managed_frame_signal_name(owner.into());
        let signal = Signal::new(
            name.into(),
            if oversized {
                "x".repeat(1_048_577)
            } else {
                "{}".into()
            },
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        ensure!(
            strategy.on_signal(&signal).is_err() && strategy.callback_failure().is_some(),
            "foreign/oversized native frame reached author code"
        );
        ensure!(
            evidence.borrow().prepares == 0 && evidence.borrow().commands.is_empty(),
            "invalid native frame changed policy or execution"
        );
    }
    Ok(())
}

#[derive(Debug)]
struct FrameworkTimerGuest(Empty);
impl Algorithm for FrameworkTimerGuest {
    const DESCRIPTOR_JSON: &'static str = "{}";
    fn validate(raw: &[u8]) -> Result<(), PluginError> {
        Empty::validate(raw)
    }
    fn create(raw: &[u8]) -> Result<Self, PluginError> {
        Empty::create(raw).map(Self)
    }
    fn start(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.start(h)
    }
    fn on_event(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Err(PluginError::generic("private guest received a timer event"))
    }
    fn save(&self, h: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        self.0.save(h)
    }
    fn restore(&mut self, v: u32, raw: &[u8], h: &mut ReadOnlyHost<'_>) -> Result<(), PluginError> {
        self.0.restore(v, raw, h)
    }
    fn pause(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.pause(h)
    }
    fn resume(&mut self, h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.resume(h)
    }
    fn stop(&mut self, raw: &[u8], h: &mut Host<'_>) -> Result<(), PluginError> {
        self.0.stop(raw, h)
    }
}
static TIMER_API: StrategyApi = StrategyApi::for_algorithm::<FrameworkTimerGuest>();

type FrameworkTimerFixture = (
    ustr::Ustr,
    nautilus_common::timer::TimeEvent,
    Rc<RefCell<Evidence>>,
);
fn dispatched_framework_timer() -> Result<FrameworkTimerFixture> {
    use crate::strategy::{Strategy, route_time_event};
    use nautilus_common::{
        actor::{DataActor, registry::try_get_actor_unchecked},
        clock::VirtualClock,
        component::{register_component_actor, start_component},
        timer::TimeEventCallback,
    };
    let (mut strategy, evidence) = fixture("framework-timer-route")?;
    let actor_id = strategy.component_id().inner();
    let results = Rc::new(RefCell::new(Vec::new()));
    let captured = results.clone();
    strategy
        .core
        .clock_mut()
        .register_default_handler(TimeEventCallback::RustLocal(Rc::new(move |event| {
            let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
                .expect("original timer actor retained");
            route_time_event(&mut *strategy, &event);
            let result =
                DataActor::on_time_event(&mut *strategy, &event).map_err(|e| format!("{e:#}"));
            captured.borrow_mut().push(result);
        })));
    register_component_actor(strategy);
    start_component(&actor_id)?;
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original clock owner missing")?;
    strategy.dispatch(
        nautilus_plugin::strategy::HostOperation::ManagedMarketExit,
        br#"{"schema_version":1}"#,
    )?;
    let name = strategy.core.market_exit_timer_name;
    let next = strategy
        .core
        .clock_mut()
        .next_time_ns(name.as_str())
        .context("original framework timer not scheduled")?;
    let handlers = {
        let mut clock = strategy.core.clock_mut();
        let clock = clock
            .as_any_mut()
            .downcast_mut::<VirtualClock>()
            .context("actual virtual clock missing")?;
        let events = clock.advance_time(next, true);
        ensure!(
            events.len() == 1 && events[0].name == name,
            "original framework clock emitted another route"
        );
        clock.match_handlers(events)
    };
    let original = handlers[0].event.clone();
    drop(strategy);
    for handler in handlers {
        handler.run();
    }
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original dispatched clock owner missing")?;
    ensure!(
        results.borrow().len() == 1 && results.borrow()[0].is_ok(),
        "original framework timer entered guest contract: {:?}",
        results.borrow()
    );
    ensure!(
        !strategy.is_exiting() && strategy.callback_failure().is_none(),
        "native exit timer left false failure or exit in progress"
    );
    ensure!(
        strategy.state() == nautilus_common::enums::ComponentState::Running
            && evidence.borrow().prepares == 0
            && evidence.borrow().commands.is_empty(),
        "empty native exit created financial effects or stopped guest"
    );
    // The same actual event is benign after its framework owner consumed it.
    DataActor::on_time_event(&mut *strategy, &original)?;
    Ok((actor_id, original, evidence))
}

#[test]
fn framework_timer_native_exit_actual_clock_does_not_enter_guest_timer_contract() -> Result<()> {
    use nautilus_common::{
        actor::registry::{deregister_actor, try_get_actor_unchecked},
        component::deregister_component,
    };
    let (actor_id, _, _) = dispatched_framework_timer()?;
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original actor missing after timer")?;
    Component::stop(&mut *strategy)?;
    drop(strategy);
    deregister_component(&actor_id);
    deregister_actor(&actor_id);
    Ok(())
}

#[rstest::rstest]
#[case("name")]
#[case("event_id")]
#[case("ts_event")]
#[case("ts_init")]
fn framework_timer_consumed_event_cannot_admit_changed_original_identity(
    #[case] field: &str,
) -> Result<()> {
    use nautilus_common::{
        actor::{
            DataActor,
            registry::{deregister_actor, try_get_actor_unchecked},
        },
        component::deregister_component,
    };
    let (actor_id, mut event, _) = dispatched_framework_timer()?;
    match field {
        "name" => event.name = "MARKET_EXIT_CHECK:FOREIGN-001".into(),
        "event_id" => event.event_id = nautilus_core::UUID4::new(),
        "ts_event" => event.ts_event = (event.ts_event.as_u64() + 1).into(),
        _ => event.ts_init = (event.ts_init.as_u64() + 1).into(),
    }
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original timer owner missing")?;
    let refused = DataActor::on_time_event(&mut *strategy, &event);
    ensure!(
        refused.is_err()
            && strategy
                .callback_failure()
                .is_some_and(|(code, message)| code == PluginErrorCode::InvalidArgument
                    && message.contains("native timer callback belongs to another owner")),
        "framework dispatch accepted a changed original timer identity"
    );
    drop(strategy);
    deregister_component(&actor_id);
    deregister_actor(&actor_id);
    Ok(())
}

#[test]
fn framework_timer_guest_owned_event_still_reaches_original_guest_contract() -> Result<()> {
    use nautilus_common::{actor::DataActor, timer::TimeEvent};
    use nautilus_common::{
        actor::registry::{deregister_actor, try_get_actor_unchecked},
        component::{deregister_component, register_component_actor, start_component},
    };
    let (strategy, _) = fixture("framework-timer-route")?;
    let actor_id = strategy.component_id().inner();
    register_component_actor(strategy);
    start_component(&actor_id)?;
    let mut strategy = try_get_actor_unchecked::<PluginStrategy>(&actor_id)
        .context("original ordinary timer actor missing")?;
    let event = TimeEvent::new(
        "plugin:MANAGED-001:original-user-timer".into(),
        nautilus_core::UUID4::new(),
        1.into(),
        1.into(),
    );
    let actual = DataActor::on_time_event(&mut *strategy, &event);
    ensure!(
        actual.is_err()
            && strategy
                .callback_failure()
                .is_some_and(|(code, message)| code == PluginErrorCode::Generic
                    && message.contains("private guest received a timer event")),
        "native framework filtering suppressed the ordinary guest timer callback"
    );
    drop(strategy);
    deregister_component(&actor_id);
    deregister_actor(&actor_id);
    Ok(())
}
