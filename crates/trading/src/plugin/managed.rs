// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! An atomic bridge to the application's existing allocation/submission owner.
//! It adds no allocator, account, runner, order state machine or economic ledger.

mod batch;
#[cfg(test)]
mod tests;

use super::{PluginStrategy, wire};
use crate::strategy::{Strategy, StrategyNative};
use anyhow::{Context, Result, ensure};
use nautilus_common::actor::DataActor;
use nautilus_model::{
    enums::OrderSide,
    identifiers::{AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId},
};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, rc::Rc};

/// Frozen application policy identity, compared before any checkpoint restore.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedPolicyIdentity {
    pub policy_id: String,
    pub schema_version: u32,
    pub configuration_sha256: String,
    /// Zero disables batches; otherwise the immutable frame bound is 1..=64.
    pub max_batch_size: u16,
}
impl ManagedPolicyIdentity {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.policy_id.is_empty()
                && self.policy_id.len() <= 128
                && self
                    .policy_id
                    .bytes()
                    .all(|value| value.is_ascii_alphanumeric() || value == b'_')
                && self.schema_version > 0,
            "invalid native managed policy identity"
        );
        ensure!(
            self.configuration_sha256.len() == 64
                && self
                    .configuration_sha256
                    .bytes()
                    .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value)),
            "invalid native managed policy configuration identity"
        );
        ensure!(
            self.max_batch_size <= 64,
            "native managed batch bound exceeds 64"
        );
        Ok(())
    }
}

/// Actual native identity for one synchronous preparation and submission.
/// The guest cannot supply account, client, strategy, clock or request identity.
#[derive(Clone, Debug, Serialize)]
pub struct ManagedSubmissionContext {
    pub trader_id: TraderId,
    pub strategy_id: StrategyId,
    pub account_id: AccountId,
    pub client_id: ClientId,
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    pub client_order_id: ClientOrderId,
    pub timestamp_ns: u64,
}

/// Native cache recording only; a recorded order can already be locally Denied.
/// Accepted never proves transmission, venue acceptance or a fill.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ManagedSubmissionStatus {
    PreparationRefused,
    NativeAccepted,
    NativeRejected,
    NativeUnknown,
}

/// Original native outcome supplied to the same application economic owner.
#[derive(Clone, Debug, Serialize)]
pub struct ManagedSubmissionOutcome {
    pub status: ManagedSubmissionStatus,
    pub cache_contains_order: bool,
    pub error: Option<String>,
    /// Exact native identities made by this operation, including linked children.
    pub factory_order_ids: Vec<ClientOrderId>,
    /// Actual current cache evidence; absence alone is never a venue conclusion.
    pub cached_order_ids: Vec<ClientOrderId>,
}

/// Host-approved order shape. The guest never supplies this native plan.
#[derive(Clone, Debug)]
pub enum ManagedOrderPlan {
    Single(wire::SubmitRequest),
    Bracket(Box<ManagedBracketRequest>),
    Refused { reason: String },
}

/// Full-quantity native OTO entry with linked OUO/OCO exits. Prices, quantity,
/// trigger types, routing and resource admission stay explicit. No extra OMS.
#[derive(Clone, Debug)]
pub struct ManagedBracketRequest {
    pub entry: wire::SubmitRequest,
    pub take_profit: wire::SubmitRequest,
    pub stop_loss: wire::SubmitRequest,
    pub contingency: nautilus_model::enums::ContingencyType,
}

/// Installed by the application before registration, never supplied by a guest.
/// Implementations use the existing shared business ledger and final interceptor.
/// Source capture/recovery of that ledger remains the application owner's duty.
pub trait ManagedSubmissionPolicy: Debug {
    /// Allocates once under the original native request ID. Returned quantities
    /// and order fields are application-approved, not guest authority.
    /// # Errors
    /// Refuses unsupported policy, unavailable account facts or business risk.
    fn prepare(
        &self,
        context: &ManagedSubmissionContext,
        original_request: &[u8],
    ) -> Result<ManagedOrderPlan>;
    /// Persists the original outcome. Unknown/cached requests keep their existing
    /// economic occupancy. Only proven unsubmitted reservations may be retired.
    /// # Errors
    /// Refuses ambiguous, unowned or non-durable economic transitions.
    fn finish(
        &self,
        context: &ManagedSubmissionContext,
        outcome: &ManagedSubmissionOutcome,
        original_request: &[u8],
    ) -> Result<()>;
    /// Allocates the complete portfolio frame once using these original native
    /// identities. Implementations must preserve their shared ledger on unwind.
    /// # Errors
    /// Refuses unsupported batches or unavailable business facts.
    fn prepare_batch(
        &self,
        _contexts: &[ManagedSubmissionContext],
        _original_request: &[u8],
    ) -> Result<Vec<ManagedOrderPlan>> {
        anyhow::bail!("native managed policy does not implement batch preparation")
    }
    /// Persists the entire original outcome frame under the same economic owner.
    /// Called once even when preparation or any native submission panics.
    /// # Errors
    /// Refuses non-durable or ambiguous financial transitions.
    fn finish_batch(
        &self,
        _contexts: &[ManagedSubmissionContext],
        _outcomes: &[ManagedSubmissionOutcome],
        _original_request: &[u8],
    ) -> Result<()> {
        anyhow::bail!("native managed policy does not implement batch finalization")
    }
}

#[derive(Debug)]
pub(super) struct ManagedPolicyBinding {
    pub(super) identity: ManagedPolicyIdentity,
    policy: Rc<dyn ManagedSubmissionPolicy>,
}

#[derive(Debug)]
pub(super) struct NativeManagedFailure {
    pub(super) code: nautilus_plugin::PluginErrorCode,
    pub(super) message: String,
}
impl std::fmt::Display for NativeManagedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for NativeManagedFailure {}
fn fatal(code: nautilus_plugin::PluginErrorCode, message: String) -> anyhow::Error {
    nautilus_plugin::panic::guard_drop("native managed original failure log", || {
        log::error!("Native managed failure: {message}");
    });
    let bound = nautilus_plugin::strategy::MAX_ERROR_BYTES;
    let mut end = message.len().min(bound.saturating_sub(64));
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let message = if message.len() > bound {
        format!(
            "{}; complete original retained in native error log",
            &message[..end]
        )
    } else {
        message
    };
    NativeManagedFailure { code, message }.into()
}

impl PluginStrategy {
    /// Binds an immutable policy identity and the same application business owner.
    /// # Errors
    /// Refuses replacement, registration/use, failed guests or invalid identities.
    pub fn set_managed_submission_policy(
        &mut self,
        identity: ManagedPolicyIdentity,
        policy: Rc<dyn ManagedSubmissionPolicy>,
    ) -> Result<()> {
        ensure!(
            self.managed_policy.is_none()
                && self.core.trader_id().is_none()
                && self.core.strategy_id().is_some()
                && !self.guest.borrow().is_faulted(),
            "native managed submission policy cannot be replaced after registration"
        );
        identity.validate()?;
        self.managed_policy = Some(ManagedPolicyBinding { identity, policy });
        Ok(())
    }
    pub(super) fn managed_submit(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        catch_managed_panic(|| self.managed_submit_inner(input))
            .map_err(|message| fatal(nautilus_plugin::PluginErrorCode::Panic, message))?
    }
    fn submit_managed_plan(
        &mut self,
        context: &ManagedSubmissionContext,
        plan: &ManagedOrderPlan,
        factory_ids: &mut Vec<ClientOrderId>,
    ) -> Result<()> {
        let entry = match plan {
            ManagedOrderPlan::Single(entry) => entry,
            ManagedOrderPlan::Bracket(bracket) => &bracket.entry,
            ManagedOrderPlan::Refused { .. } => {
                anyhow::bail!("refused managed plan has no native order")
            }
        };
        ensure!(
            entry.schema_version == 2
                && entry.instrument_id == context.instrument_id
                && entry.side == context.side,
            "native managed policy returned another order scope"
        );
        match plan {
            ManagedOrderPlan::Single(entry) => {
                self.submit_native_request(entry, Some(context.client_order_id))?;
            }
            ManagedOrderPlan::Bracket(bracket) => {
                use nautilus_model::{
                    enums::{ContingencyType, OrderType, TriggerType},
                    orders::Order,
                };
                self.validate_native_request(entry)?;
                ensure!(
                    self.core.config.manage_order_lists_locally,
                    "managed brackets require the installed native local order-list manager"
                );
                ensure!(
                    !entry.reduce_only && entry.position_id.is_none(),
                    "managed bracket parent must be an independent risk entry"
                );
                let opposite = if entry.side == OrderSide::Buy {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };
                for child in [&bracket.take_profit, &bracket.stop_loss] {
                    self.validate_native_request(child)?;
                    ensure!(
                        child.schema_version == 2
                            && child.instrument_id == entry.instrument_id
                            && child.side == opposite
                            && child.quantity == entry.quantity
                            && child.reduce_only
                            && child.position_id.is_none()
                            && child.expire_time_ns.is_none(),
                        "managed native bracket child scope or full-quantity protection differs"
                    );
                }
                ensure!(
                    bracket.take_profit.order_type == wire::NativeOrderType::Limit
                        && bracket.stop_loss.order_type == wire::NativeOrderType::StopMarket
                        && matches!(
                            bracket.contingency,
                            ContingencyType::Ouo | ContingencyType::Oco
                        ),
                    "unsupported managed native bracket shape"
                );
                let entry_type = match entry.order_type {
                    wire::NativeOrderType::Market => OrderType::Market,
                    wire::NativeOrderType::Limit => OrderType::Limit,
                    wire::NativeOrderType::StopMarket => OrderType::StopMarket,
                    wire::NativeOrderType::StopLimit => OrderType::StopLimit,
                };
                let orders = self
                    .order_factory()
                    .try_bracket()
                    .instrument_id(entry.instrument_id)
                    .order_side(entry.side)
                    .quantity(entry.quantity)
                    .entry_order_type(entry_type)
                    .maybe_entry_price(entry.price)
                    .maybe_entry_trigger_price(entry.trigger_price)
                    .entry_trigger_type(entry.trigger_type.unwrap_or(TriggerType::Default))
                    .maybe_expire_time(entry.expire_time_ns.map(nautilus_core::UnixNanos::from))
                    .time_in_force(entry.time_in_force)
                    .entry_post_only(entry.post_only)
                    .entry_client_order_id(context.client_order_id)
                    .tp_order_type(OrderType::Limit)
                    .maybe_tp_price(bracket.take_profit.price)
                    .tp_time_in_force(bracket.take_profit.time_in_force)
                    .tp_post_only(bracket.take_profit.post_only)
                    .sl_order_type(OrderType::StopMarket)
                    .maybe_sl_trigger_price(bracket.stop_loss.trigger_price)
                    .sl_trigger_type(
                        bracket
                            .stop_loss
                            .trigger_type
                            .context("managed stop trigger type absent")?,
                    )
                    .sl_time_in_force(bracket.stop_loss.time_in_force)
                    .contingency_type(bracket.contingency)
                    .call()?;
                ensure!(
                    orders.len() == 3 && orders[0].client_order_id() == context.client_order_id,
                    "native bracket changed its original parent or leg inventory"
                );
                for order in &orders {
                    let id = order.client_order_id();
                    if id != context.client_order_id {
                        factory_ids.push(id);
                    }
                }
                ensure!(
                    factory_ids
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == 3,
                    "native bracket generated duplicate identities"
                );
                self.submit_order_list(orders, None, Some(self.scope.client_id), None)?;
            }
            ManagedOrderPlan::Refused { .. } => {
                anyhow::bail!("refused managed plan has no native construction")
            }
        }
        Ok(())
    }

    fn managed_submit_inner(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        ensure!(
            input.len() <= 1_048_576,
            "native managed request exceeds its bound"
        );
        let request: wire::ManagedSubmit = serde_json::from_slice(input)?;
        ensure!(
            request.schema_version == 1,
            "unknown native managed request version"
        );
        self.require_instrument(request.instrument_id)?;
        ensure!(
            matches!(request.side, OrderSide::Buy | OrderSide::Sell),
            "native managed request has no side"
        );
        let policy = Rc::clone(
            &self
                .managed_policy
                .as_ref()
                .context("native managed business policy is not installed")?
                .policy,
        );
        let trader_id = self
            .core
            .trader_id()
            .context("native managed trader is unregistered")?;
        let strategy_id = self
            .core
            .strategy_id()
            .context("native managed strategy is unregistered")?;
        let timestamp_ns = self.clock().timestamp_ns().as_u64();
        let client_order_id = self.order_factory().generate_client_order_id();
        ensure!(
            !self.cache().order_exists(&client_order_id),
            "native managed identity already exists in cache"
        );
        let context = ManagedSubmissionContext {
            trader_id,
            strategy_id,
            account_id: self.scope.account_id,
            client_id: self.scope.client_id,
            instrument_id: request.instrument_id,
            side: request.side,
            client_order_id,
            timestamp_ns,
        };
        let mut prepared = false;
        let mut factory_order_ids = vec![client_order_id];
        let attempt = catch_managed_panic(|| -> Result<()> {
            let plan = policy.prepare(&context, input)?;
            if let ManagedOrderPlan::Refused { reason } = &plan {
                ensure!(
                    !reason.is_empty() && reason.len() <= 4096,
                    "invalid managed refusal"
                );
                anyhow::bail!("{reason}");
            }
            prepared = true;
            self.submit_managed_plan(&context, &plan, &mut factory_order_ids)
        });
        let cached_order_ids: Vec<_> = factory_order_ids
            .iter()
            .copied()
            .filter(|id| self.cache().order_exists(id))
            .collect();
        let (outcome, original_failure) = classify_managed_attempt(
            &context,
            prepared,
            factory_order_ids,
            cached_order_ids,
            self.callback_failure(),
            attempt,
        );
        finish_managed_single(&context, policy.as_ref(), input, &outcome, original_failure)
    }
}

fn classify_managed_attempt(
    context: &ManagedSubmissionContext,
    prepared: bool,
    factory_order_ids: Vec<ClientOrderId>,
    cached_order_ids: Vec<ClientOrderId>,
    retained_failure: Option<(nautilus_plugin::PluginErrorCode, String)>,
    attempt: std::result::Result<Result<()>, String>,
) -> (
    ManagedSubmissionOutcome,
    Option<(nautilus_plugin::PluginErrorCode, String)>,
) {
    let client_order_id = context.client_order_id;
    let cached = cached_order_ids.contains(&client_order_id);
    let (attempt, mut original_failure) = match attempt {
        Ok(attempt) => (attempt, retained_failure),
        Err(message) => (
            Err(anyhow::anyhow!(message.clone())),
            retained_failure.or(Some((nautilus_plugin::PluginErrorCode::Panic, message))),
        ),
    };
    let complete_cache = cached && cached_order_ids.len() == factory_order_ids.len();
    if attempt.is_ok() && !complete_cache && original_failure.is_none() {
        original_failure = Some((
            nautilus_plugin::PluginErrorCode::SerializationFailed,
            format!(
                "native managed submission succeeded without its complete original cache identities: original={factory_order_ids:?}, cached={cached_order_ids:?}"
            ),
        ));
    }
    if attempt.is_err() && !cached_order_ids.is_empty() && original_failure.is_none() {
        original_failure = Some((
            nautilus_plugin::PluginErrorCode::SerializationFailed,
            format!(
                "native managed failure after cache admission: original={factory_order_ids:?}, cached={cached_order_ids:?}"
            ),
        ));
    }
    let status = if original_failure.is_some() || (!cached_order_ids.is_empty() && attempt.is_err())
    {
        ManagedSubmissionStatus::NativeUnknown
    } else if attempt.is_ok() && complete_cache {
        ManagedSubmissionStatus::NativeAccepted
    } else if prepared {
        ManagedSubmissionStatus::NativeRejected
    } else {
        ManagedSubmissionStatus::PreparationRefused
    };
    let outcome = ManagedSubmissionOutcome {
        status,
        cache_contains_order: cached,
        error: attempt.err().map(|error| format!("{error:#}")).or_else(|| {
            original_failure
                .as_ref()
                .map(|(_, message)| message.clone())
        }),
        factory_order_ids,
        cached_order_ids,
    };
    (outcome, original_failure)
}

fn finish_managed_single(
    context: &ManagedSubmissionContext,
    policy: &dyn ManagedSubmissionPolicy,
    input: &[u8],
    outcome: &ManagedSubmissionOutcome,
    original_failure: Option<(nautilus_plugin::PluginErrorCode, String)>,
) -> Result<Vec<u8>> {
    let client_order_id = context.client_order_id;
    // Never undo unknown work or invent a new key. The application owns this
    // exact reservation and decides which transition is actually proven.
    let finalization = catch_managed_panic(|| policy.finish(context, outcome, input));
    if let Some((code, original)) = original_failure {
        let finalization = match finalization {
            Ok(Ok(())) => String::new(),
            Ok(Err(error)) => format!("; original outcome finalization failed: {error:#}"),
            Err(message) => format!("; original outcome finalization panicked: {message}"),
        };
        return Err(fatal(
            code,
            format!("{original}; original id={client_order_id}, outcome={outcome:?}{finalization}"),
        ));
    }
    match finalization {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(fatal(
                nautilus_plugin::PluginErrorCode::SerializationFailed,
                format!(
                    "native managed outcome was not durably finalized: id={client_order_id}, original={outcome:?}; finalization: {error:#}"
                ),
            ));
        }
        Err(message) => {
            return Err(fatal(
                nautilus_plugin::PluginErrorCode::Panic,
                format!(
                    "native managed outcome finalization panicked: id={client_order_id}, original={outcome:?}; {message}"
                ),
            ));
        }
    }
    Ok(serde_json::to_vec(
        &serde_json::json!({"schema_version":1,"client_order_id":client_order_id,
        "native_outcome":outcome,"venue_acceptance_proven":false}),
    )?)
}

fn catch_managed_panic<T>(
    call: impl FnOnce() -> Result<T>,
) -> std::result::Result<Result<T>, String> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(call)).map_err(|panic| {
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic payload");
        let message = format!("native managed preparation/submission panicked: {message}");
        nautilus_plugin::panic::drop_payload(panic);
        message
    })
}
