// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Plugin history is bound to opaque objects from the actual Journal reader.
//! There is no deserializable recovery permit or current execution grant here.

use std::{cell::RefCell, collections::BTreeMap};

use anyhow::{Context, Result, ensure};
use indexmap::IndexMap;
use nautilus_common::recovery_trace::{
    NativeTraceRecord, NativeTraceSource,
    historical::{HistoricalInputBoundary, HistoricalReplayPreparation},
};
use nautilus_event_store::native_trace::{
    VerifiedNativeRoot, VerifiedNativeTrace, native_inventory_digest,
};
use nautilus_trading::plugin::historical::PluginHistoricalVerifier;

#[derive(Debug)]
struct SourceBinding {
    owner: String,
    source: NativeTraceSource,
    roots: BTreeMap<u64, String>,
}

#[derive(Debug, Default)]
struct VerificationState {
    binding: Option<SourceBinding>,
    failed: bool,
}

/// One native strategy's reader-bound historical verification session.
/// Create before registration; the actual Trader preparation supplies its cut.
/// A failed session is never reset or converted to current trading authority.
#[derive(Debug, Default)]
pub struct ReaderVerifiedPluginRecovery {
    state: RefCell<VerificationState>,
}

fn input_digest(inputs: &[NativeTraceRecord]) -> Result<String> {
    native_inventory_digest(&serde_json::to_value(inputs)?)
}

impl PluginHistoricalVerifier for ReaderVerifiedPluginRecovery {
    fn prepare(
        &self,
        boundary: &HistoricalReplayPreparation<'_>,
        owner: &str,
        current: &IndexMap<String, Vec<u8>>,
    ) -> Result<()> {
        let mut state = self.state.try_borrow_mut()?;
        let result = (|| {
            ensure!(
                !state.failed && state.binding.is_none(),
                "native plugin reader session already prepared or failed"
            );
            let trace = boundary
                .verified_source::<VerifiedNativeTrace>()
                .context("native plugin preparation is not from the Journal reader")?;
            ensure!(
                boundary.component_id() == owner
                    && trace.source() == &trace.cut().source
                    && !trace.source().logical_run.is_empty(),
                "native plugin reader cut changed source or owner"
            );
            let original = trace.cut().native_effects["components"]["strategies"]
                .get(owner)
                .context("native source cut has no original plugin state")?;
            let original: IndexMap<String, Vec<u8>> = serde_json::from_value(original.clone())?;
            ensure!(
                &original == current,
                "native plugin complete source-cut state differs"
            );
            let mut roots = BTreeMap::new();
            for root in trace.roots() {
                ensure!(
                    root.source() == trace.source()
                        && roots
                            .insert(root.root_sequence(), input_digest(root.inputs())?)
                            .is_none(),
                    "native plugin reader roots are inconsistent"
                );
            }
            state.binding = Some(SourceBinding {
                owner: owner.into(),
                source: trace.source().clone(),
                roots,
            });
            Ok(())
        })();
        if result.is_err() {
            state.failed = true;
        }
        result
    }

    fn verify_input(&self, boundary: &HistoricalInputBoundary<'_>, owner: &str) -> Result<()> {
        let mut state = self.state.try_borrow_mut()?;
        let result = (|| {
            ensure!(!state.failed, "native plugin reader verification failed");
            let binding = state
                .binding
                .as_ref()
                .context("native plugin source cut is absent")?;
            let root = boundary
                .verified_source::<VerifiedNativeRoot>()
                .context("native plugin input is not from the Journal reader")?;
            ensure!(
                binding.owner == owner
                    && boundary.route().component_id == owner
                    && root.source() == &binding.source
                    && binding.roots.get(&root.root_sequence())
                        == Some(&input_digest(root.inputs())?),
                "native plugin changed its whole reader source/root/owner"
            );
            let NativeTraceRecord::Begin {
                source,
                root_sequence,
                ..
            } = boundary.input()
            else {
                anyhow::bail!("native plugin historical input has no original Begin");
            };
            ensure!(
                source == root.source()
                    && *root_sequence == root.root_sequence()
                    && root
                        .inputs()
                        .iter()
                        .any(|original| original == boundary.input()),
                "native plugin changed the complete original Begin"
            );
            Ok(())
        })();
        if result.is_err() {
            state.failed = true;
        }
        result
    }
}
