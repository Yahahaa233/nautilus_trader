// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Bounded application frames on the original native Signal route and owner.
//! The application still seals, validates and persists its own economic frame.

use anyhow::{Context, Result, ensure};
use nautilus_common::{actor::DataActor, signal::Signal};
use nautilus_model::identifiers::StrategyId;

use super::PluginStrategy;
use crate::strategy::Strategy;

/// Returns the exact native signal name for one managed portfolio owner.
#[must_use]
pub fn managed_frame_signal_name(strategy_id: StrategyId) -> String {
    format!("NATIVE_MANAGED_FRAME_{strategy_id}")
}

impl PluginStrategy {
    pub(super) fn managed_frames_enabled(&self) -> bool {
        self.managed_policy
            .as_ref()
            .is_some_and(|policy| policy.identity.max_batch_size > 0)
    }

    fn managed_frame_name(&self) -> Result<String> {
        ensure!(
            self.managed_frames_enabled(),
            "native application frame requires an installed bounded batch policy"
        );
        Ok(managed_frame_signal_name(
            self.strategy_id()
                .context("native managed frame owner absent")?,
        ))
    }

    pub(super) fn subscribe_managed_frame(&mut self) -> Result<()> {
        if self.managed_frames_enabled() && !self.adapter.managed_frame_subscribed {
            let name = self.managed_frame_name()?;
            self.subscribe_signal(&name, None);
            self.adapter.managed_frame_subscribed = true;
        }
        Ok(())
    }

    pub(super) fn release_managed_frame(&mut self) -> Result<()> {
        if self.adapter.managed_frame_subscribed {
            let name = self.managed_frame_name()?;
            self.unsubscribe_signal(&name);
            self.adapter.managed_frame_subscribed = false;
        }
        Ok(())
    }

    #[cfg(feature = "live")]
    pub(super) fn restore_managed_frame(
        &mut self,
        preparation: &nautilus_common::recovery_trace::historical::HistoricalReplayPreparation<'_>,
    ) -> Result<()> {
        if self.adapter.managed_frame_subscribed {
            let name = self.managed_frame_name()?;
            self.restore_native_local_signal(&name, None, preparation)?;
        }
        Ok(())
    }

    pub(super) fn managed_frame_event(&mut self, signal: &Signal) -> Result<()> {
        let route = (|| {
            ensure!(
                self.adapter.managed_frame_subscribed
                    && signal.name.as_str() == self.managed_frame_name()?
                    && !signal.value.is_empty()
                    && signal.value.len() <= 1_048_576,
                "native application frame exceeds its original owner or byte bound"
            );
            Ok(())
        })();
        self.retain_route_result(route)?;
        self.event("managed_frame", signal)
    }
}
