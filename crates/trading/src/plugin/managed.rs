// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! An atomic bridge to the application's existing allocation/submission owner.
//! It adds no allocator, account, runner, order state machine or economic ledger.

use super::{PluginStrategy, wire};
use crate::strategy::StrategyNative;
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

/// Native admission only. Accepted does not mean a venue accepted or filled it.
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
    ) -> Result<wire::SubmitRequest>;
    /// Persists the original outcome. Unknown/cached requests keep their existing
    /// economic occupancy. Only proven unsubmitted reservations may be retired.
    /// # Errors
    /// Refuses ambiguous, unowned or non-durable economic transitions.
    fn finish(
        &self,
        context: &ManagedSubmissionContext,
        outcome: &ManagedSubmissionOutcome,
    ) -> Result<()>;
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
        let attempt = catch_managed_panic(|| -> Result<()> {
            // Pass original bytes: the application validates its own strict typed
            // schema rather than trusting a JSON Value with duplicate members.
            let order = policy.prepare(&context, input)?;
            prepared = true;
            ensure!(
                order.schema_version == 2
                    && order.instrument_id == context.instrument_id
                    && order.side == context.side,
                "native managed policy returned another order scope"
            );
            self.submit_native_request(&order, Some(client_order_id))?;
            Ok(())
        });
        let cached = self.cache().order_exists(&client_order_id);
        // A panic may have happened after the shared owner reserved funds or
        // native work began. It is unknown even when no cache order is present.
        // Give the same owner its actual ID/context before latching the panic.
        let (attempt, original_panic) = match attempt {
            Ok(attempt) => (attempt, None),
            Err(message) => (Err(anyhow::anyhow!(message.clone())), Some(message)),
        };
        let status = match (prepared, attempt.is_ok(), cached, original_panic.is_some()) {
            (_, true, true, false) => ManagedSubmissionStatus::NativeAccepted,
            (_, _, _, true) | (_, _, true, false) => ManagedSubmissionStatus::NativeUnknown,
            (false, false, false, false) => ManagedSubmissionStatus::PreparationRefused,
            (_, false, false, false) => ManagedSubmissionStatus::NativeRejected,
            (_, true, false, false) => {
                return Err(fatal(
                    nautilus_plugin::PluginErrorCode::SerializationFailed,
                    format!(
                        "native managed submission succeeded without its original cache identity: {client_order_id}"
                    ),
                ));
            }
        };
        let outcome = ManagedSubmissionOutcome {
            status,
            cache_contains_order: cached,
            error: attempt.err().map(|error| format!("{error:#}")),
        };
        // Never undo unknown work or invent a new key. The application owns this
        // exact reservation and decides which transition is actually proven.
        let finalization = catch_managed_panic(|| policy.finish(&context, &outcome));
        if let Some(original) = original_panic {
            let finalization = match finalization {
                Ok(Ok(())) => String::new(),
                Ok(Err(error)) => format!("; original outcome finalization failed: {error:#}"),
                Err(message) => format!("; original outcome finalization panicked: {message}"),
            };
            return Err(fatal(
                nautilus_plugin::PluginErrorCode::Panic,
                format!(
                    "{original}; original id={client_order_id}, outcome={outcome:?}{finalization}"
                ),
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
