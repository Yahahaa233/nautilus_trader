// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Synchronous native submission of one application-owned portfolio frame.

use super::{
    ManagedOrderPlan, ManagedSubmissionContext, ManagedSubmissionOutcome, ManagedSubmissionStatus,
    PluginStrategy, catch_managed_panic, classify_managed_attempt, fatal, wire,
};
use crate::strategy::StrategyNative;
use anyhow::{Context, Result, ensure};
use nautilus_common::actor::DataActor;
use nautilus_model::enums::OrderSide;
use nautilus_plugin::PluginErrorCode;
use std::rc::Rc;

impl PluginStrategy {
    pub(in crate::plugin) fn managed_submit_batch(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        catch_managed_panic(|| self.managed_submit_batch_inner(input))
            .map_err(|message| fatal(PluginErrorCode::Panic, message))?
    }

    fn managed_submit_batch_inner(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        ensure!(
            input.len() <= 1_048_576,
            "native managed batch exceeds its bound"
        );
        let request: wire::ManagedSubmitBatch = serde_json::from_slice(input)?;
        let binding = self
            .managed_policy
            .as_ref()
            .context("native managed business policy is not installed")?;
        ensure!(
            request.schema_version == 1
                && binding.identity.max_batch_size > 0
                && request.intents.len() <= usize::from(binding.identity.max_batch_size),
            "native managed batch version, inventory or immutable bound differs"
        );
        let policy = Rc::clone(&binding.policy);
        // Validate every guest scope before allocating any identity or money.
        for intent in &request.intents {
            ensure!(
                intent.schema_version == 1
                    && matches!(intent.side, OrderSide::Buy | OrderSide::Sell),
                "invalid native managed batch intent"
            );
            self.require_instrument(intent.instrument_id)?;
        }
        let trader_id = self
            .core
            .trader_id()
            .context("native managed trader is unregistered")?;
        let strategy_id = self
            .core
            .strategy_id()
            .context("native managed strategy is unregistered")?;
        let timestamp_ns = self.clock().timestamp_ns().as_u64();
        let mut contexts = Vec::with_capacity(request.intents.len());
        for intent in &request.intents {
            let client_order_id = self.order_factory().generate_client_order_id();
            ensure!(
                !self.cache().order_exists(&client_order_id)
                    && contexts
                        .iter()
                        .all(|context: &ManagedSubmissionContext| context.client_order_id
                            != client_order_id),
                "native managed batch identity already exists"
            );
            contexts.push(ManagedSubmissionContext {
                trader_id,
                strategy_id,
                account_id: self.scope.account_id,
                client_id: self.scope.client_id,
                instrument_id: intent.instrument_id,
                side: intent.side,
                client_order_id,
                timestamp_ns,
            });
        }
        let mut original_failure = None;
        let mut outcomes = Vec::with_capacity(contexts.len());
        match catch_managed_panic(|| -> Result<Vec<ManagedOrderPlan>> {
            let plans = policy.prepare_batch(&contexts, input)?;
            ensure!(
                plans.len() == contexts.len(),
                "managed batch policy changed its frame inventory"
            );
            Ok(plans)
        }) {
            Ok(Ok(plans)) => {
                for (context, plan) in contexts.iter().zip(&plans) {
                    if original_failure.is_some() {
                        outcomes.push(unattempted(
                            context,
                            ManagedSubmissionStatus::NativeRejected,
                            "not submitted after an earlier uncertain native outcome".into(),
                        ));
                        continue;
                    }
                    let (outcome, failure) = self.submit_batch_member(context, plan);
                    original_failure = failure;
                    outcomes.push(outcome);
                }
            }
            Ok(Err(error)) => {
                let error = format!("managed batch preparation refused: {error:#}");
                outcomes.extend(contexts.iter().map(|context| {
                    unattempted(
                        context,
                        ManagedSubmissionStatus::PreparationRefused,
                        error.clone(),
                    )
                }));
            }
            Err(message) => {
                // Preparation may have mutated the existing ledger before panic.
                outcomes.extend(contexts.iter().map(|context| {
                    unattempted(
                        context,
                        ManagedSubmissionStatus::NativeUnknown,
                        message.clone(),
                    )
                }));
                original_failure = Some((PluginErrorCode::Panic, message));
            }
        }
        // Exactly one finalization call to the same application owner, including
        // partially submitted batches and every original identity not attempted.
        let finalization = catch_managed_panic(|| policy.finish_batch(&contexts, &outcomes, input));
        let finalization_failure = match finalization {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some((
                PluginErrorCode::SerializationFailed,
                format!("managed batch original outcome finalization failed: {error:#}"),
            )),
            Err(message) => Some((
                PluginErrorCode::Panic,
                format!("managed batch original outcome finalization panicked: {message}"),
            )),
        };
        if let Some((code, message)) = original_failure {
            return Err(fatal(
                code,
                format!(
                    "{message}; original contexts={contexts:?}; outcomes={outcomes:?}; finalization={finalization_failure:?}"
                ),
            ));
        }
        if let Some((code, message)) = finalization_failure {
            return Err(fatal(
                code,
                format!("{message}; original contexts={contexts:?}; outcomes={outcomes:?}"),
            ));
        }
        Ok(serde_json::to_vec(&serde_json::json!({
            "schema_version": 1, "timestamp_ns": timestamp_ns, "contexts": contexts, "native_outcomes": outcomes,
            "venue_acceptance_proven": false,
        }))?)
    }

    fn submit_batch_member(
        &mut self,
        context: &ManagedSubmissionContext,
        plan: &ManagedOrderPlan,
    ) -> (ManagedSubmissionOutcome, Option<(PluginErrorCode, String)>) {
        if let ManagedOrderPlan::Refused { reason } = plan {
            if reason.is_empty() || reason.len() > 4096 {
                let message = "managed batch policy returned an invalid refusal".to_string();
                return (
                    unattempted(
                        context,
                        ManagedSubmissionStatus::NativeUnknown,
                        message.clone(),
                    ),
                    Some((PluginErrorCode::SerializationFailed, message)),
                );
            }
            return (
                unattempted(
                    context,
                    ManagedSubmissionStatus::PreparationRefused,
                    reason.clone(),
                ),
                None,
            );
        }
        let mut factory_order_ids = vec![context.client_order_id];
        let attempt =
            catch_managed_panic(|| self.submit_managed_plan(context, plan, &mut factory_order_ids));
        let cached_order_ids: Vec<_> = factory_order_ids
            .iter()
            .copied()
            .filter(|id| self.cache().order_exists(id))
            .collect();
        classify_managed_attempt(
            context,
            true,
            factory_order_ids,
            cached_order_ids,
            self.callback_failure(),
            attempt,
        )
    }
}

fn unattempted(
    context: &ManagedSubmissionContext,
    status: ManagedSubmissionStatus,
    error: String,
) -> ManagedSubmissionOutcome {
    ManagedSubmissionOutcome {
        status,
        cache_contains_order: false,
        error: Some(error),
        factory_order_ids: vec![context.client_order_id],
        cached_order_ids: Vec::new(),
    }
}
