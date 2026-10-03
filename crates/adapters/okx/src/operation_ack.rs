//! Original WebSocket operation ACK observations, independent of order state.
//! These values provide no trading permission or native terminal-state effect.
use std::{any::Any, sync::Arc};

use nautilus_common::messages::execution::SubmitOrder;
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::execution::context::OrderIdentity;
use nautilus_model::{
    data::{CustomDataTrait, HasTsInit},
    identifiers::{AccountId, TraderId},
};
use serde::{Deserialize, Serialize};

pub const SOURCE_SCHEMA: &str = "OKXOrderOperationAckSource.v1";
const MAX_SOURCE_BYTES: usize = 1_048_576;

/// Local original-request binding. It does not assign the operation ACK to a
/// venue terminal event; the full original Submit and raw frame stay separate.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitAckCommandBinding {
    pub command_id: UUID4,
    pub command_payload_json: String,
    pub request_id: String,
    pub identity: OrderIdentity,
    pub account_id: AccountId,
    /// Both fields are absent only for an actual top-level failure with no item.
    pub matched_item_index: Option<usize>,
    pub matched_item: Option<serde_json::Value>,
}

/// One actual raw-inbox operation response. Duplicate and unbound frames are
/// retained independently; neither can synthesize or amend OrderAccepted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderOperationAckSource {
    pub schema: String,
    pub trader_id: TraderId,
    pub adapter_account_id: AccountId,
    pub websocket_payload_json: String,
    /// Actual adapter raw-inbox time, before parsing and dispatcher queuing.
    pub received_ns: u64,
    pub matched_submit: Option<SubmitAckCommandBinding>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub execution_authorized: bool,
    pub order_state_effect: bool,
}
impl OrderOperationAckSource {
    /// Checks the original wire and any explicit local original-request binding.
    /// Journal custody/physical receipt verification remains the consumer's job.
    ///
    /// # Errors
    /// Returns an error for changed wire, identity, item, command, or timestamps.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema == SOURCE_SCHEMA
                && !self.execution_authorized
                && !self.order_state_effect
                && self.received_ns != 0
                && self.ts_event.as_u64() == self.received_ns
                && self.ts_init >= self.ts_event
                && self.websocket_payload_json.len() <= MAX_SOURCE_BYTES,
            "invalid original operation ACK source profile"
        );
        let frame: crate::websocket::messages::OKXWsFrame =
            serde_json::from_str(&self.websocket_payload_json)?;
        anyhow::ensure!(
            matches!(
                frame,
                crate::websocket::messages::OKXWsFrame::OrderResponse { .. }
            ),
            "original operation ACK source is not a response frame"
        );
        let raw: serde_json::Value = serde_json::from_str(&self.websocket_payload_json)?;
        if let Some(binding) = &self.matched_submit {
            let command: SubmitOrder = serde_json::from_str(&binding.command_payload_json)?;
            let validated = crate::websocket::client::PendingSubmitInfo::new(
                &command,
                binding.account_id,
                binding.identity,
            )?;
            let original_data = raw["data"]
                .as_array()
                .ok_or_else(|| anyhow::anyhow!("original ACK lacks item array"))?;
            anyhow::ensure!(
                command.command_id == binding.command_id
                    && command.trader_id == self.trader_id
                    && binding.account_id == self.adapter_account_id
                    && validated.request_id == binding.request_id
                    && raw["op"] == "order"
                    && raw["id"] == binding.request_id
                    && command.ts_init.as_u64() <= self.received_ns,
                "original operation ACK command/request identity differs"
            );
            match (binding.matched_item_index, binding.matched_item.as_ref()) {
                (Some(0), Some(item)) => anyhow::ensure!(
                    original_data.len() == 1
                        && original_data.first() == Some(item)
                        && item["clOrdId"] == binding.identity.client_order_id.as_str()
                        && item["sCode"]
                            .as_str()
                            .is_some_and(|value| !value.is_empty()),
                    "original operation ACK item differs"
                ),
                (None, None) => anyhow::ensure!(
                    original_data.is_empty()
                        && raw["code"]
                            .as_str()
                            .is_some_and(|value| !value.is_empty() && value != "0"),
                    "item-free operation ACK must retain its actual request failure"
                ),
                _ => anyhow::bail!("operation ACK item index and original value must be paired"),
            }
        }
        anyhow::ensure!(
            serde_json::to_vec(self)?.len() <= MAX_SOURCE_BYTES,
            "original operation ACK source exceeds input bound"
        );
        Ok(())
    }
}
impl HasTsInit for OrderOperationAckSource {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}
impl CustomDataTrait for OrderOperationAckSource {
    fn type_name(&self) -> &'static str {
        SOURCE_SCHEMA
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }
    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }
    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }
    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        other.as_any().downcast_ref::<Self>() == Some(self)
    }
    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let source: Self = serde_json::from_value(value)?;
        source.validate()?;
        Ok(Arc::new(source))
    }
}

/// Registers the original ACK decoder only; no authority or order state is minted.
///
/// # Errors
/// Returns an error when the data type registry has a conflicting registration.
pub fn register_source_type() -> anyhow::Result<()> {
    nautilus_model::data::registry::ensure_json_deserializer_registered(
        SOURCE_SCHEMA,
        Box::new(OrderOperationAckSource::from_json),
    )
}
