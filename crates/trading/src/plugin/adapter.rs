// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Native adapter resources belong to the same checkpoint as the guest state.

use std::collections::BTreeSet;

use anyhow::{Result, ensure};
use nautilus_model::data::BarType;
use serde::{Deserialize, Serialize};

use super::PluginScope;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AdapterState {
    schema_version: u32,
    pub(super) subscribed_bars: BTreeSet<BarType>,
}

impl Default for AdapterState {
    fn default() -> Self {
        Self {
            schema_version: 1,
            subscribed_bars: BTreeSet::new(),
        }
    }
}

impl AdapterState {
    pub(super) fn validate(&self, scope: &PluginScope) -> Result<()> {
        ensure!(
            self.schema_version == 1,
            "unknown native plugin adapter state"
        );
        ensure!(
            self.subscribed_bars.iter().all(|bar| {
                scope.bar_types.contains(bar) && scope.instruments.contains(&bar.instrument_id())
            }),
            "restored plugin subscriptions differ from the admitted scope"
        );
        Ok(())
    }
}
