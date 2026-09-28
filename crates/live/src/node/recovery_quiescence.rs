//! Explicit no-timer subset for paused recovery capture.
use anyhow::{Context, Result, ensure};
use nautilus_common::clock::Clock;
use nautilus_system::trader::Trader;
use std::{any::TypeId, cell::RefCell, rc::Rc};

pub(super) fn with_registered_timer_inventory<T>(
    kernel_clock: &Rc<RefCell<dyn Clock>>,
    trader: &Rc<RefCell<Trader>>,
    capture: impl FnOnce(&std::collections::BTreeMap<String, u64>) -> Result<T>,
) -> Result<T> {
    // Retaining this borrow also prevents changing the registered clock map.
    let trader = trader
        .try_borrow()
        .context("trader busy during timer inventory")?;
    let components = trader.registered_component_clocks()?;
    let mut clocks = vec![("kernel".to_owned(), kernel_clock.clone())];
    clocks.extend(
        components
            .into_iter()
            .map(|(id, clock)| (format!("component:{id}"), clock)),
    );
    let held = clocks
        .iter()
        .map(|(id, clock)| {
            Ok((
                id,
                clock
                    .try_borrow()
                    .with_context(|| format!("clock busy: {id}"))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let verify = || -> Result<()> {
        for (id, clock) in &held {
            let kind = (**clock).type_id();
            ensure!(
                kind == TypeId::of::<nautilus_common::clock::TestClock>()
                    || kind == TypeId::of::<nautilus_common::live::clock::LiveClock>(),
                "unsupported clock implementation: {id}"
            );
            let names = clock.timer_names();
            let count = clock.timer_count();
            ensure!(count == names.len(), "timer inventory disagrees: {id}");
            ensure!(
                count == 0,
                "paused capture has unsupported active timers: {id}: {names:?}"
            );
        }
        Ok(())
    };
    verify()?;
    let counts = held
        .iter()
        .map(|(id, clock)| ((*id).clone(), clock.timer_count() as u64))
        .collect();
    let result = capture(&counts);
    verify()?;
    // Keep the trader inventory borrow alive until all capture work has ended.
    ensure!(
        trader.component_count() + 1 == clocks.len(),
        "component clocks changed during capture"
    );
    result
}

#[cfg(test)]
pub(super) fn with_empty_registered_timers<T>(
    kernel_clock: &Rc<RefCell<dyn Clock>>,
    trader: &Rc<RefCell<Trader>>,
    capture: impl FnOnce() -> Result<T>,
) -> Result<T> {
    with_registered_timer_inventory(kernel_clock, trader, |_| capture())
}

/// Actual native inventory projected while all paused capture guards are held.
/// This is evidence for the narrow supported profile, never execution permission.
#[derive(Debug, serde::Serialize)]
pub struct PausedRecoveryInventory {
    pub(super) runner_counts: std::collections::BTreeMap<String, u64>,
    pub(super) timer_counts: std::collections::BTreeMap<String, u64>,
    pub(super) synchronous_queue_counts: std::collections::BTreeMap<String, u64>,
    pub(super) adapter_profiles: std::collections::BTreeMap<String, String>,
    pub(super) message_bus_mode: &'static str,
}

/// Native acknowledgement of completed empty bootstrap handoff; never a dispatch watermark.
/// Constructed only after native cache and component restoration succeeded on this node.
#[derive(Debug, Clone, serde::Serialize)]
pub struct EmptyBootstrapRecoveryReceipt {
    recovery_id: String,
    checkpoint_sequence: u64,
    payload_sha256: String,
    source_instance_id: nautilus_core::UUID4,
}

impl super::LiveNode {
    /// Completes an empty bootstrap without fabricating an event or dispatch watermark.
    /// Source identity must already have been authenticated by the application loader.
    ///
    /// # Errors
    /// Refuses incomplete native handoffs, changed registration, active/nonempty inventory,
    /// existing completion, and malformed source binding. Failure after freezing poisons admission.
    pub fn complete_empty_bootstrap_recovery(
        &mut self,
        recovery_id: &str,
        checkpoint_sequence: u64,
        payload_sha256: &str,
        source_instance_id: nautilus_core::UUID4,
    ) -> Result<EmptyBootstrapRecoveryReceipt> {
        ensure!(
            !recovery_id.is_empty()
                && checkpoint_sequence > 0
                && payload_sha256.len() == 64
                && payload_sha256.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "invalid empty bootstrap source binding"
        );
        ensure!(
            self.state() == super::NodeState::Idle
                && !self.handle.should_stop()
                && self.recovery_requires_release
                && self.recovery_cache_installed
                && self.recovery_native_frontier.is_none()
                && self.recovery_empty_bootstrap.is_none(),
            "empty bootstrap requires completed native handoffs without prior completion"
        );
        let restored = self
            .recovery_restored_components
            .as_ref()
            .context("registered component handoff missing")?;
        let trader = self.kernel.trader.try_borrow()?;
        ensure!(
            trader
                .actor_ids()
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                == restored.0
                && trader
                    .strategy_ids()
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    == restored.1
                && trader.exec_algorithm_ids().is_empty(),
            "registered handoff identity changed"
        );
        drop(trader);
        ensure!(
            self.kernel.risk_engine.try_borrow()?.trading_state()
                == nautilus_model::enums::TradingState::Halted
                && self.recovery_dispatch_queue.is_empty()
                && self.external_msgbus.is_none(),
            "empty bootstrap is not paused and local"
        );
        let observer = self
            .dispatch_observer
            .as_ref()
            .context("empty bootstrap requires durable observer")?;
        let coverage = observer.coverage()?;
        ensure!(
            coverage["active_depth"].as_u64() == Some(0)
                && coverage["completed_root"].as_u64() == Some(0)
                && coverage["failure"].is_null(),
            "empty bootstrap cannot complete after native dispatch"
        );
        let runner = self.runner.as_ref().context("runner unavailable")?;
        let gate = runner.ingress_gate();
        let guard = gate.freeze()?;
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            with_registered_timer_inventory(&self.kernel.clock, &self.kernel.trader, |_| {
                nautilus_common::msgbus::with_local_only_recovery_inventory(|bus| {
                    nautilus_common::runner::with_empty_sync_command_queues(|| {
                        ensure!(
                            runner
                                .pending_queue_counts()
                                .values()
                                .all(|count| *count == 0),
                            "empty bootstrap runner messages remain pending"
                        );
                        ensure!(
                            self.kernel.data_engine.try_borrow()?.check_disconnected()
                                && self.kernel.exec_engine.try_borrow()?.check_disconnected(),
                            "empty bootstrap clients connected"
                        );
                        bus.verify()?;
                        guard.verify()?;
                        ensure!(!self.handle.should_stop(), "empty bootstrap stop requested");
                        guard.finish()
                    })
                })
            })
        }));
        match outcome {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                gate.invalidate();
                self.handle.stop();
                return Err(error);
            }
            Err(panic) => {
                gate.invalidate();
                self.handle.stop();
                std::panic::resume_unwind(panic);
            }
        }
        let receipt = EmptyBootstrapRecoveryReceipt {
            recovery_id: recovery_id.to_owned(),
            checkpoint_sequence,
            payload_sha256: payload_sha256.to_owned(),
            source_instance_id,
        };
        self.recovery_empty_bootstrap = Some(receipt.clone());
        Ok(receipt)
    }
}
