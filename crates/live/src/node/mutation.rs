// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Canonical inputs emitted by actual native background mutation producers.

use super::LiveNode;
use anyhow::Result;
use serde::Serialize;

/// Fixed schema native input captured before its producer mutates state. Only
/// native producer constructors can create one. It grants no callback/admission
/// capability and must be journaled by the application's strict source codec.
#[derive(Debug, Clone, Serialize)]
pub struct NativeMutationInput {
    schema: &'static str,
    kind: String,
    node_instance_id: nautilus_core::UUID4,
    actual_now_ns: u64,
    payload: serde_json::Value,
}
impl NativeMutationInput {
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn canonical_payload(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(self)?)
    }
}
impl LiveNode {
    pub(super) fn native_mutation_input<T: Serialize>(
        &self,
        kind: &str,
        payload: &T,
    ) -> Result<NativeMutationInput> {
        Ok(NativeMutationInput {
            schema: "NautilusNativeMutationInput.v1",
            kind: kind.into(),
            node_instance_id: self.kernel.instance_id,
            actual_now_ns: nautilus_core::time::get_atomic_clock_realtime()
                .get_time_ns()
                .as_u64(),
            payload: serde_json::to_value(payload)?,
        })
    }
    pub(super) fn native_lifecycle_input(&self, action: &str) -> Result<NativeMutationInput> {
        self.native_mutation_input(action, &serde_json::json!({"trader_id":self.config.trader_id,"environment":self.config.environment,
            "load_state":self.config.load_state,"save_state":self.config.save_state,
            "exec_engine":self.config.exec_engine,"data_engine":self.config.data_engine,
            "state":format!("{:?}",self.state()),"recovery_requires_release":self.recovery_requires_release}))
    }
}

#[cfg(all(test, feature = "dispatch-observer"))]
mod tests {
    use super::*;
    use crate::{
        dispatch::{DispatchInput, DispatchObserver, DispatchSource},
        node::NodeDispatchObserver,
    };
    use nautilus_common::enums::Environment;
    use nautilus_model::identifiers::TraderId;
    use std::{cell::RefCell, rc::Rc};

    #[tokio::test(flavor = "current_thread")]
    async fn native_mutation_actual_startup_has_typed_lifecycle_and_reconciliation_roots() {
        let mut node =
            LiveNode::builder(TraderId::from("MUTATION-CHECKPOINT"), Environment::Sandbox)
                .unwrap()
                .with_reconciliation(false)
                .with_delay_post_stop_secs(0)
                .with_delay_shutdown_secs(0)
                .build()
                .unwrap();
        let inputs = Rc::new(RefCell::new(Vec::new()));
        let seen = inputs.clone();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("native-mutations".into(), |_| Ok(())).unwrap(),
            move |source, phase, input| {
                let native = input
                    .downcast_ref::<NativeMutationInput>()
                    .ok_or_else(|| anyhow::anyhow!("unexpected native source input"))?;
                let payload = native.canonical_payload()?;
                seen.borrow_mut()
                    .push((source, native.kind().to_owned(), payload.clone()));
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        node.start().await.unwrap();
        assert!(node.state().is_running());
        let seen = inputs.borrow();
        assert!(
            seen.iter()
                .any(|(source, kind, _)| *source == DispatchSource::Lifecycle
                    && kind == "startup.connect_reconcile_start")
        );
        assert!(seen.iter().any(
            |(source, kind, _)| *source == DispatchSource::Reconciliation
                && kind == "startup.reconciliation"
        ));
        assert!(seen.iter().all(|(_, _, value)| value["schema"]
            == "NautilusNativeMutationInput.v1"
            && value["actual_now_ns"].as_u64().unwrap() > 0
            && value["payload"].get("password").is_none()));
        let proof = node
            .dispatch_observer
            .as_ref()
            .unwrap()
            .completed_root_boundary_proof()
            .unwrap()
            .unwrap();
        proof.verify().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_mutation_writer_rejection_prevents_actual_kernel_start() {
        let mut node = LiveNode::builder(TraderId::from("MUTATION-REJECT"), Environment::Sandbox)
            .unwrap()
            .with_reconciliation(false)
            .build()
            .unwrap();
        node.set_dispatch_observer(NodeDispatchObserver::new(
            DispatchObserver::new("native-mutation-reject".into(), |_| {
                Err(anyhow::anyhow!("durable sink unavailable"))
            })
            .unwrap(),
            |source, phase, input| {
                Ok(DispatchInput {
                    source,
                    phase: phase.into(),
                    payload: input
                        .downcast_ref::<NativeMutationInput>()
                        .ok_or_else(|| anyhow::anyhow!("native input missing"))?
                        .canonical_payload()?,
                    batch_index: None,
                })
            },
        ))
        .unwrap();
        assert!(node.start().await.is_err());
        assert!(node.handle.should_stop());
        assert!(node.handle.startup_reconciliation().is_none());
    }
}
