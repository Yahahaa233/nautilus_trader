//! Startup owns queued messages and original receipts until the engine borrow is free.
//! A dequeued batch remains one input; only the original Node handlers process it.
use super::{EngineConnectionStatus, LiveNode, RunnerReceivers};
use crate::runner::{AsyncRunnerChannels, Retained};
use nautilus_common::{
    live::dst,
    messages::{DataEvent, ExecutionEvent, SystemCommand, SystemEvent, data::DataCommand},
    runner::{TimeEventMessage, TradingCommandMessage},
};
use std::{collections::VecDeque, time::Duration};

#[derive(Default)]
pub(super) struct StartupPending {
    time: VecDeque<Retained<TimeEventMessage>>,
    system_events: VecDeque<Retained<SystemEvent>>,
    system_commands: VecDeque<Retained<SystemCommand>>,
    data_events: VecDeque<Retained<DataEvent>>,
    data_commands: VecDeque<Retained<DataCommand>>,
    execution_events: VecDeque<Retained<ExecutionEvent>>,
    execution_commands: VecDeque<Retained<TradingCommandMessage>>,
}
impl StartupPending {
    #[cfg(all(test, feature = "native-tail-replay"))]
    pub(super) fn retained_counts_for_test(&self) -> (usize, usize, usize) {
        (
            self.data_events.len(),
            self.data_commands.len(),
            self.execution_events.len(),
        )
    }

    fn stage_data(&mut self, receivers: &mut RunnerReceivers<'_>) {
        while let Ok(entry) = receivers.data_evt.try_recv_retained() {
            self.data_events.push_back(entry);
        }
        while let Ok(entry) = receivers.data_cmd.try_recv_retained() {
            self.data_commands.push_back(entry);
        }
    }
    fn stage_all(&mut self, receivers: &mut RunnerReceivers<'_>) {
        while let Ok(entry) = receivers.time_evt.try_recv_retained() {
            self.time.push_back(entry);
        }
        while let Ok(entry) = receivers.system_evt.try_recv_retained() {
            self.system_events.push_back(entry);
        }
        while let Ok(entry) = receivers.system_cmd.try_recv_retained() {
            self.system_commands.push_back(entry);
        }
        self.stage_data(receivers);
        while let Ok(entry) = receivers.exec_evt.try_recv_retained() {
            self.execution_events.push_back(entry);
        }
        while let Ok(entry) = receivers.exec_cmd.try_recv_retained() {
            self.execution_commands.push_back(entry);
        }
    }
}

/// No handler runs while the connection future owns an engine RefMut. Retained
/// dequeue evidence is published only immediately before its actual dispatch.
pub(super) async fn buffer_startup_events<F: std::future::Future>(
    future: F,
    pending: &mut StartupPending,
    receivers: &mut RunnerReceivers<'_>,
) -> F::Output {
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            Some(entry) = receivers.time_evt.recv_retained() => pending.time.push_back(entry),
            Some(entry) = receivers.system_evt.recv_retained() => pending.system_events.push_back(entry),
            Some(entry) = receivers.system_cmd.recv_retained() => pending.system_commands.push_back(entry),
            Some(entry) = receivers.data_evt.recv_retained() => pending.data_events.push_back(entry),
            Some(entry) = receivers.data_cmd.recv_retained() => pending.data_commands.push_back(entry),
            Some(entry) = receivers.exec_evt.recv_retained() => pending.execution_events.push_back(entry),
            Some(entry) = receivers.exec_cmd.recv_retained() => pending.execution_commands.push_back(entry),
        }
    }
}

impl LiveNode {
    pub(super) fn check_startup_dispatch(&self) -> anyhow::Result<()> {
        #[cfg(feature = "dispatch-observer")]
        anyhow::ensure!(
            !self.dispatch_failure.get(),
            "startup native dispatch failed"
        );
        anyhow::ensure!(!self.event_store_halted(), "startup Journal halted");
        Ok(())
    }
    fn process_startup_data(&self, pending: &mut StartupPending) -> anyhow::Result<bool> {
        let progressed = !pending.data_events.is_empty() || !pending.data_commands.is_empty();
        // Instruments must be installed before subscriptions/requests are dispatched.
        while let Some(entry) = pending.data_events.pop_front() {
            self.process_data_event(entry.activate());
            self.check_startup_dispatch()?;
        }
        while let Some(entry) = pending.data_commands.pop_front() {
            self.process_data_command(entry.activate());
            self.check_startup_dispatch()?;
        }
        Ok(progressed)
    }
    pub(super) fn flush_startup_data(
        &self,
        pending: &mut StartupPending,
        receivers: &mut RunnerReceivers<'_>,
    ) -> anyhow::Result<()> {
        loop {
            pending.stage_data(receivers);
            if !self.process_startup_data(pending)? {
                return Ok(());
            }
        }
    }
    pub(super) fn flush_startup_events(
        &mut self,
        pending: &mut StartupPending,
        receivers: &mut RunnerReceivers<'_>,
    ) -> anyhow::Result<()> {
        pending.stage_all(receivers);
        while let Some(entry) = pending.time.pop_front() {
            let _ = self.process_time_event(entry.activate());
            self.check_startup_dispatch()?;
        }
        self.process_startup_data(pending)?;
        // Account, reports, individual orders and whole native batches retain
        // their original relative FIFO order on the execution channel.
        while let Some(entry) = pending.execution_events.pop_front() {
            self.process_exec_event(entry.activate());
            self.check_startup_dispatch()?;
        }
        while let Some(entry) = pending.execution_commands.pop_front() {
            self.process_exec_command(entry.activate());
            self.check_startup_dispatch()?;
        }
        Ok(())
    }
    pub(super) fn process_startup_system(
        &self,
        pending: &mut StartupPending,
    ) -> anyhow::Result<()> {
        while let Some(entry) = pending.system_events.pop_front() {
            self.process_system_event(entry.activate());
            self.check_startup_dispatch()?;
        }
        while let Some(entry) = pending.system_commands.pop_front() {
            self.process_system_command(entry.activate());
            self.check_startup_dispatch()?;
        }
        Ok(())
    }
    pub(super) fn flush_installed_startup(
        &mut self,
        pending: &mut StartupPending,
        data_only: bool,
    ) -> anyhow::Result<()> {
        let Some(mut runner) = self.runner.take() else {
            return Ok(());
        };
        let mut receivers = RunnerReceivers::from(runner.startup_channels_mut());
        let result = if data_only {
            pending.stage_all(&mut receivers);
            self.flush_startup_data(pending, &mut receivers)
        } else {
            self.flush_startup_events(pending, &mut receivers)
        };
        self.runner = Some(runner);
        result
    }
    /// Engine readiness is checked after actual dispatch, not while the
    /// connection future still borrows the execution engine.
    pub(super) async fn await_startup_engines_connected(
        &mut self,
        pending: &mut StartupPending,
        mut receivers: Option<&mut RunnerReceivers<'_>>,
        deadline: dst::time::Instant,
    ) -> anyhow::Result<EngineConnectionStatus> {
        loop {
            if let Some(receivers) = receivers.as_deref_mut() {
                self.flush_startup_events(pending, receivers)?;
            } else {
                self.flush_installed_startup(pending, false)?;
            }
            if self.handle.should_stop() {
                return Ok(EngineConnectionStatus::StopRequested);
            }
            if self.kernel.is_shutdown_requested() {
                return Ok(EngineConnectionStatus::ShutdownRequested);
            }
            if self.kernel.check_engines_connected() {
                return Ok(EngineConnectionStatus::Connected);
            }
            let now = dst::time::Instant::now();
            if now >= deadline {
                self.log_connection_status();
                return Ok(EngineConnectionStatus::TimedOut);
            }
            let delay = Duration::from_millis(100).min(deadline - now);
            if let Some(receivers) = receivers.as_deref_mut() {
                buffer_startup_events(dst::time::sleep(delay), pending, receivers).await;
            } else {
                dst::time::sleep(delay).await;
            }
        }
    }
}
impl<'a> From<&'a mut AsyncRunnerChannels> for RunnerReceivers<'a> {
    fn from(channels: &'a mut AsyncRunnerChannels) -> Self {
        Self {
            time_evt: &mut channels.time_evt_rx,
            system_evt: &mut channels.system_evt_rx,
            system_cmd: &mut channels.system_cmd_rx,
            data_evt: &mut channels.data_evt_rx,
            data_cmd: &mut channels.data_cmd_rx,
            exec_evt: &mut channels.exec_evt_rx,
            exec_cmd: &mut channels.exec_cmd_rx,
        }
    }
}
