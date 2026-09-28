//! Ordered native event recovery. Commands and transport callbacks require a
//! separate reconciliation protocol and are deliberately rejected before replay.
use super::{LiveNode, NodeState};
use crate::{
    dispatch::{DispatchInput, DispatchSource},
    runner::AsyncRunner,
    runner_recovery::{
        RunnerRecoveryChannel, RunnerRecoveryCodecRegistry, RunnerRecoveryEnvelope,
        RunnerRecoveryEvent, RunnerRecoveryWatermark,
    },
};
use anyhow::{Context, Result, ensure};
use nautilus_common::messages::ExecutionEvent;
use nautilus_model::{events::OrderEventAny, orders::Order};

impl LiveNode {
    /// Installs one disconnected venue client after isolated native event recovery.
    /// This preserves the recovery startup fence and halted risk admission.
    ///
    /// # Errors
    /// Refuses incomplete recovery, active/stopped nodes, existing clients, connected
    /// factory results, or registration failure. Failed installation poisons the node.
    pub fn attach_execution_client_after_recovery(
        &mut self,
        factory: &dyn nautilus_common::factories::ExecutionClientFactory,
        config: &dyn nautilus_common::factories::ClientConfig,
    ) -> Result<()> {
        use nautilus_common::clients::ExecutionClient;
        ensure!(self.state() == NodeState::Idle && !self.handle.should_stop()
            && self.recovery_requires_release && self.recovery_native_frontier.is_some(),
            "execution client attachment requires completed paused native recovery");
        ensure!(self.exec_clients.is_empty()
            && self.kernel.exec_engine.try_borrow().context("execution engine busy")?.client_ids().is_empty(),
            "recovery execution clients already installed");
        ensure!(self.kernel.risk_engine.try_borrow().context("risk engine busy")?.trading_state()
            == nautilus_model::enums::TradingState::Halted,
            "recovery execution attachment requires halted admission");
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| -> Result<()> {
            let mut client = self.socket_registry.scope(|| factory.create(
                self.config.trader_id, factory.name(), config,
                self.kernel.cache().into(), self.kernel.clock(),
            ))?;
            if client.is_connected() {
                client.stop().context("connected recovery client could not be stopped")?;
                anyhow::bail!("recovery client factory returned a connected client");
            }
            let client = crate::execution::client::LiveExecutionClient::new(client);
            let id = client.client_id();
            let venue = client.venue();
            self.kernel.exec_engine.try_borrow_mut().context("execution engine busy")?
                .register_client(Box::new(client.clone()))?;
            self.socket_registry.register_client(id);
            nautilus_execution::engine::ExecutionEngine::subscribe_venue_instruments(
                &self.kernel.exec_engine, venue);
            self.exec_manager.set_position_reconciliation_tolerance(
                client.account_id(), client.position_reconciliation_tolerance());
            self.exec_clients.push(client);
            Ok(())
        }));
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => { self.handle.stop(); Err(error) }
            Err(panic) => { self.handle.stop(); std::panic::resume_unwind(panic) }
        }
    }

    /// Replays a frozen, contiguous batch through the actual native event handlers.
    ///
    /// The complete batch is decoded before any mutation. Envelope identity stays
    /// attached to its typed event, so biased polling of the seven normal channels
    /// cannot reorder recovery. `verify` must check business postconditions; native
    /// dispatch returning by itself is not proof that an event was accepted.
    /// Durable observer Complete is recorded only after this check succeeds.
    /// Direct and batch order events must remain in canonical order history;
    /// account events must retain their full payload (identity equality is insufficient).
    ///
    /// Commands, timers, raw execution reports and system callbacks are rejected: this event-only path
    /// cannot resolve uncertain venue submissions. Derived messages in the normal
    /// runner remain pending. The node is permanently startup-blocked after replay
    /// until an independent release implementation is available.
    ///
    /// # Errors
    /// Rejects identity/sequence/codec mismatches, a running or failed node, a
    /// non-halted risk engine, unsupported channels, or failed business checks.
    /// Failure stops the node and never processes the remaining suffix.
    ///
    /// # Panics
    /// Propagates handler or verifier panics after stopping and blocking the node.
    pub fn replay_recovery_events<F>(
        &mut self,
        boundary: &RunnerRecoveryWatermark,
        envelopes: &[RunnerRecoveryEnvelope],
        registry: &RunnerRecoveryCodecRegistry,
        mut verify: F,
    ) -> Result<u64>
    where
        F: FnMut(&Self, &RunnerRecoveryEnvelope) -> Result<()>,
    {
        ensure!(
            self.state() == NodeState::Idle && !self.handle.should_stop(),
            "native recovery requires an idle, non-failed node"
        );
        ensure!(
            self.dispatch_observer.is_some(),
            "native recovery requires durable observation"
        );
        ensure!(
            self.recovery_dispatch_queue.is_empty(),
            "another recovery queue is pending"
        );
        ensure!(
            self.exec_clients.is_empty()
                && self.kernel.exec_engine.borrow().client_ids().is_empty(),
            "native event recovery requires an isolated node without execution clients"
        );
        ensure!(
            self.kernel.risk_engine.borrow().trading_state()
                == nautilus_model::enums::TradingState::Halted,
            "native recovery requires halted order admission"
        );
        ensure!(
            !boundary.recovery_id.trim().is_empty()
                && boundary.checkpoint_sequence > 0
                && boundary.dispatch_watermark > 0,
            "invalid recovery boundary"
        );
        if let Some(previous) = &self.recovery_native_frontier {
            ensure!(
                previous == boundary,
                "recovery boundary does not extend completed prefix"
            );
        }
        let mut frontier = boundary.dispatch_watermark;
        let mut staged = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            ensure!(
                envelope.recovery_id == boundary.recovery_id
                    && envelope.checkpoint_sequence == boundary.checkpoint_sequence,
                "recovery envelope identity mismatch"
            );
            frontier = frontier
                .checked_add(1)
                .context("recovery sequence exhausted")?;
            ensure!(
                envelope.dispatch_sequence == frontier,
                "non-contiguous recovery events"
            );
            ensure!(
                matches!(
                    envelope.channel,
                    RunnerRecoveryChannel::ExecutionEvent | RunnerRecoveryChannel::DataEvent
                ),
                "commands, timers and system callbacks require independent recovery"
            );
            let event = registry.decode(envelope)?;
            ensure!(
                !matches!(
                    &event,
                    RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::Report(_))
                ),
                "execution reports require independent reconciliation and derived-event recovery"
            );
            staged.push((envelope, event));
        }
        for (envelope, event) in staged {
            let order_events: Vec<OrderEventAny> = match &event {
                RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::Order(event)) => {
                    vec![event.clone()]
                }
                RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::OrderSubmittedBatch(batch)) => {
                    batch
                        .events
                        .iter()
                        .copied()
                        .map(OrderEventAny::Submitted)
                        .collect()
                }
                RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::OrderAcceptedBatch(batch)) => {
                    batch
                        .events
                        .iter()
                        .copied()
                        .map(OrderEventAny::Accepted)
                        .collect()
                }
                RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::OrderCanceledBatch(batch)) => {
                    batch
                        .events
                        .iter()
                        .copied()
                        .map(OrderEventAny::Canceled)
                        .collect()
                }
                _ => Vec::new(),
            };
            let account_event = match &event {
                RunnerRecoveryEvent::ExecutionEvent(ExecutionEvent::Account(event)) => {
                    Some(event.clone())
                }
                _ => None,
            };
            let source = match envelope.channel {
                RunnerRecoveryChannel::ExecutionEvent => DispatchSource::ExecutionEvent,
                RunnerRecoveryChannel::DataEvent => DispatchSource::DataEvent,
                _ => unreachable!("preflight restricts recovery channels"),
            };
            // Persist the full identity-bearing envelope, not just its payload.
            let input = DispatchInput {
                source,
                phase: "native_recovery".into(),
                payload: serde_json::to_value(envelope)?,
                batch_index: None,
            };
            self.with_recovery_dispatch(input, |node| {
                match event {
                    RunnerRecoveryEvent::ExecutionEvent(event) => ensure!(
                        node.process_exec_event_unobserved(event),
                        "native execution event rejected"
                    ),
                    RunnerRecoveryEvent::DataEvent(event) => AsyncRunner::handle_data_event(event),
                    _ => anyhow::bail!("decoded recovery channel mismatch"),
                }
                ensure!(
                    !node.handle.should_stop() && !node.event_store_halted(),
                    "native recovery failed during event handling"
                );
                ensure!(
                    node.kernel.risk_engine.borrow().trading_state()
                        == nautilus_model::enums::TradingState::Halted,
                    "native recovery changed order admission"
                );
                for event in &order_events {
                    let cache = node.kernel.cache.borrow();
                    let order = cache
                        .order(&event.client_order_id())
                        .context("native handler did not retain the recovered order")?;
                    ensure!(
                        order.events().into_iter().any(|stored| stored == event),
                        "native handler did not retain the exact recovered order event"
                    );
                }
                if let Some(event) = &account_event {
                    let cache = node.kernel.cache.borrow();
                    let account = cache
                        .account(&event.account_id)
                        .context("native handler did not retain the recovered account")?;
                    // AccountState::eq compares identity only, not balances/margins.
                    let expected = serde_json::to_value(event)?;
                    let retained = account
                        .events()
                        .iter()
                        .map(serde_json::to_value)
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    ensure!(
                        retained.contains(&expected),
                        "native handler did not retain the exact recovered account event"
                    );
                }
                verify(node, envelope)
            })?;
            self.recovery_native_frontier = Some(RunnerRecoveryWatermark {
                recovery_id: boundary.recovery_id.clone(),
                checkpoint_sequence: boundary.checkpoint_sequence,
                dispatch_watermark: envelope.dispatch_sequence,
            });
        }
        if envelopes.is_empty() {
            self.recovery_requires_release = true;
            self.recovery_native_frontier = Some(boundary.clone());
        }
        Ok(frontier)
    }
}
