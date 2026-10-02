//! Explicit no-timer subset for paused recovery capture.
use anyhow::{Context, Result, ensure};
use nautilus_common::clock::Clock;
use nautilus_system::trader::Trader;
use std::{any::TypeId, cell::RefCell, rc::Rc};

/// Holds actual registered clocks and producer gates before runner ingress closes.
/// Timer futures retain scheduled ticks until release, without emitting into a
/// frozen runner or fabricating empty callback inventory.
pub(super) fn with_running_registered_timer_inventory<T>(
    kernel_clock: &Rc<RefCell<dyn Clock>>,
    trader: &Rc<RefCell<Trader>>,
    capture: impl FnOnce(
        &std::collections::BTreeMap<String, u64>,
        &std::collections::BTreeMap<String, serde_json::Value>,
        &dyn Fn() -> Result<()>,
        &dyn Fn(&dyn Fn() -> Result<()>) -> Result<()>,
    ) -> Result<T>,
) -> Result<T> {
    with_registered_timer_inventory_mode(kernel_clock, trader, false, capture)
}

pub(super) fn with_registered_timer_inventory_mode<T>(
    kernel_clock: &Rc<RefCell<dyn Clock>>,
    trader: &Rc<RefCell<Trader>>,
    terminal: bool,
    capture: impl FnOnce(
        &std::collections::BTreeMap<String, u64>,
        &std::collections::BTreeMap<String, serde_json::Value>,
        &dyn Fn() -> Result<()>,
        &dyn Fn(&dyn Fn() -> Result<()>) -> Result<()>,
    ) -> Result<T>,
) -> Result<T> {
    let trader = trader
        .try_borrow()
        .context("trader busy during running timer capture")?;
    let mut clocks = vec![("kernel".to_owned(), kernel_clock.clone())];
    clocks.extend(
        trader
            .registered_component_clocks()?
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
                    .with_context(|| format!("registered clock busy: {id}"))?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let mut guards: Vec<Box<dyn nautilus_common::clock::TimerCheckpoint>> = Vec::new();
    let mut counts = std::collections::BTreeMap::new();
    let mut inventories = std::collections::BTreeMap::new();
    let mut frozen_clocks = std::collections::BTreeMap::<usize, usize>::new();
    for (index, (id, clock)) in held.iter().enumerate() {
        let address = Rc::as_ptr(&clocks[index].1) as *const () as usize;
        if let Some(guard_index) = frozen_clocks.get(&address) {
            counts.insert((*id).clone(), clock.timer_count() as u64);
            inventories.insert((*id).clone(), guards[*guard_index].inventory().clone());
            continue;
        }
        let kind = (**clock).type_id();
        ensure!(
            kind == TypeId::of::<nautilus_common::clock::TestClock>()
                || kind == TypeId::of::<nautilus_common::live::clock::LiveClock>(),
            "unsupported running clock implementation: {id}"
        );
        ensure!(
            clock.timer_names().len() == clock.timer_count(),
            "timer names/count disagree: {id}"
        );
        let guard = clock.freeze_running_timer_checkpoint()?;
        counts.insert((*id).clone(), clock.timer_count() as u64);
        inventories.insert((*id).clone(), guard.inventory().clone());
        frozen_clocks.insert(address, guards.len());
        guards.push(guard);
    }
    let verify = || -> Result<()> {
        ensure!(
            trader.component_count() + 1 == clocks.len(),
            "registered clocks changed"
        );
        for (id, clock) in &held {
            ensure!(
                counts.get(*id) == Some(&(clock.timer_count() as u64)),
                "active timer count changed: {id}"
            );
        }
        for guard in &guards {
            guard.verify()?;
        }
        Ok(())
    };
    let with_actual_clock_reads = |read: &dyn Fn() -> Result<()>| -> Result<()> {
        verify()?;
        for guard in &guards {
            guard.pause_read_view()?;
        }
        let result = read();
        for guard in &guards {
            guard.resume_read_view()?;
        }
        verify()?;
        result
    };
    verify()?;
    let result = capture(&counts, &inventories, &verify, &with_actual_clock_reads)?;
    verify()?;
    for guard in guards {
        if terminal {
            guard.finish_terminal()?;
        } else {
            guard.finish()?;
        }
    }
    Ok(result)
}

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
    fn verify_empty_bootstrap_adapter_inventory(&self) -> Result<()> {
        ensure!(
            self.config
                .data_engine
                .external_clients
                .as_ref()
                .is_none_or(Vec::is_empty),
            "empty bootstrap external data clients have no paused inventory proof"
        );
        let data = self.kernel.data_engine.try_borrow()?;
        let execution = self.kernel.exec_engine.try_borrow()?;
        ensure!(
            execution.get_external_client_ids().is_empty(),
            "empty bootstrap external execution clients have no paused inventory proof"
        );
        ensure!(
            data.check_disconnected() && execution.check_disconnected(),
            "empty bootstrap clients connected"
        );
        for client in data.get_clients() {
            let client = client.get_client();
            client.verify_paused_recovery_inventory()?;
            client.paused_recovery_inventory_profile()?;
        }
        for client in execution.get_all_clients() {
            client.verify_paused_recovery_inventory()?;
            client.paused_recovery_inventory_profile()?;
        }
        Ok(())
    }

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
            // Retain registration borrows through guard.finish, just as child
            // checkpoint capture does. Disconnection alone is not empty inventory.
            let _data_inventory = self.kernel.data_engine.try_borrow()?;
            let _execution_inventory = self.kernel.exec_engine.try_borrow()?;
            self.verify_empty_bootstrap_adapter_inventory()?;
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
                        self.verify_empty_bootstrap_adapter_inventory()?;
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

#[cfg(test)]
mod adapter_inventory_tests {
    use nautilus_common::enums::Environment;
    use nautilus_execution::engine::stubs::StubExecutionClient;
    use nautilus_model::{
        enums::OmsType,
        identifiers::{AccountId, ClientId, TraderId, Venue},
    };

    #[tokio::test]
    async fn empty_bootstrap_rejects_disconnected_unknown_adapter_without_consuming_it() {
        let node =
            super::super::LiveNode::builder(TraderId::from("INVENTORY-001"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .build()
                .unwrap();
        node.verify_empty_bootstrap_adapter_inventory().unwrap();
        node.kernel
            .exec_engine
            .borrow_mut()
            .register_client(Box::new(StubExecutionClient::new(
                ClientId::from("UNKNOWN"),
                AccountId::from("UNKNOWN-001"),
                Venue::from("TEST"),
                OmsType::Netting,
                None,
            )))
            .unwrap();
        assert!(node.kernel.exec_engine.borrow().check_disconnected());
        let before = node.kernel.exec_engine.borrow().get_all_clients().len();
        let error = node.verify_empty_bootstrap_adapter_inventory().unwrap_err();
        assert!(error.to_string().contains("unsupported"), "{error:#}");
        assert_eq!(
            node.kernel.exec_engine.borrow().get_all_clients().len(),
            before
        );
        assert!(node.recovery_empty_bootstrap.is_none());
    }

    #[tokio::test]
    async fn empty_bootstrap_rejects_external_data_ids_and_busy_registration() {
        let mut node =
            super::super::LiveNode::builder(TraderId::from("INVENTORY-002"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .build()
                .unwrap();
        node.config.data_engine.external_clients = Some(vec![ClientId::from("EXTERNAL")]);
        assert!(
            node.verify_empty_bootstrap_adapter_inventory()
                .unwrap_err()
                .to_string()
                .contains("external data")
        );
        node.config.data_engine.external_clients = None;
        let held = node.kernel.exec_engine.borrow_mut();
        assert!(node.verify_empty_bootstrap_adapter_inventory().is_err());
        drop(held);
        node.verify_empty_bootstrap_adapter_inventory().unwrap();
        assert!(node.recovery_empty_bootstrap.is_none());
    }
}
