//! Original WebSocket operation ACK observations, independent of order state.
//! These values provide no trading permission or native terminal-state effect.
use std::{any::Any, sync::Arc};

use nautilus_common::messages::execution::{CancelOrder, ModifyOrder, SubmitOrder};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::execution::context::OrderIdentity;
use nautilus_model::{
    data::{CustomDataTrait, HasTsInit},
    identifiers::{AccountId, ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
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
/// retained independently; neither can synthesize or amend `OrderAccepted`.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_modify: Option<OrderOperationAckCommandBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_cancel: Option<OrderOperationAckCommandBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<OrderOperationAckConnection>,
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
        anyhow::ensure!(
            usize::from(self.matched_submit.is_some())
                + usize::from(self.matched_modify.is_some())
                + usize::from(self.matched_cancel.is_some())
                <= 1,
            "operation ACK cannot bind multiple native commands"
        );
        for (operation, binding) in [
            ("amend-order", self.matched_modify.as_ref()),
            ("cancel-order", self.matched_cancel.as_ref()),
        ] {
            if let Some(binding) = binding {
                binding.validate(operation)?;
                anyhow::ensure!(
                    binding.trader_id == self.trader_id
                        && binding.account_id == self.adapter_account_id
                        && self.connection.as_ref() == Some(&binding.connection)
                        && binding.issued_ns <= self.received_ns
                        && raw["id"] == binding.request_id
                        && raw["op"] == operation,
                    "operation ACK native command/connection/receive identity differs"
                );
                let data = raw["data"]
                    .as_array()
                    .ok_or_else(|| anyhow::anyhow!("original ACK lacks item array"))?;
                match (binding.matched_item_index, binding.matched_item.as_ref()) {
                    (Some(0), Some(item)) => anyhow::ensure!(
                        data.len() == 1
                            && data.first() == Some(item)
                            && item["clOrdId"] == binding.client_order_id.as_str()
                            && item["sCode"].as_str().is_some_and(|v| !v.is_empty())
                            && binding
                                .routed_venue_order_id
                                .is_none_or(|venue| item["ordId"].as_str() == Some(venue.as_str())),
                        "operation ACK original routed item differs"
                    ),
                    (None, None) => anyhow::ensure!(
                        data.is_empty()
                            && raw["code"]
                                .as_str()
                                .is_some_and(|v| !v.is_empty() && v != "0"),
                        "item-free management ACK must retain its actual failure"
                    ),
                    _ => anyhow::bail!("operation ACK item and index must be paired"),
                }
            }
        }
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

/// Actual transport instance and reader/writer epoch. This is evidence only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderOperationAckConnection {
    pub instance_id: UUID4,
    pub epoch: u64,
}

/// One original native Modify/Cancel and its actual routed request. No order
/// state or permission is inferred from this local request association.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderOperationAckCommandBinding {
    pub command_id: UUID4,
    pub command_payload_json: String,
    pub request_id: String,
    pub request_payload_json: String,
    pub trader_id: TraderId,
    pub account_id: AccountId,
    pub strategy_id: StrategyId,
    pub instrument_id: InstrumentId,
    pub instrument_id_code: u64,
    pub client_order_id: ClientOrderId,
    pub routed_venue_order_id: Option<VenueOrderId>,
    pub connection: OrderOperationAckConnection,
    pub issued_ns: u64,
    pub matched_item_index: Option<usize>,
    pub matched_item: Option<serde_json::Value>,
}
impl OrderOperationAckCommandBinding {
    pub(crate) fn validate(&self, operation: &str) -> anyhow::Result<()> {
        let request: serde_json::Value = serde_json::from_str(&self.request_payload_json)?;
        let args = request["args"]
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("original operation request lacks arguments"))?;
        anyhow::ensure!(
            args.len() == 1
                && request["id"] == self.request_id
                && request["op"] == operation
                && self.request_id == self.command_id.to_string().replace('-', "")
                && self.instrument_id_code != 0
                && self.issued_ns != 0
                && args[0]["instIdCode"].as_u64() == Some(self.instrument_id_code)
                && args[0]["clOrdId"].as_str() == Some(self.client_order_id.as_str())
                && args[0]["ordId"].as_str()
                    == self
                        .routed_venue_order_id
                        .as_ref()
                        .map(nautilus_model::identifiers::VenueOrderId::as_str),
            "original operation request identity or routing differs"
        );
        let (id, trader, strategy, instrument, client, ts_init) = match operation {
            "amend-order" => {
                let command: ModifyOrder = serde_json::from_str(&self.command_payload_json)?;
                let string_param = |key: &str| {
                    command
                        .params
                        .as_ref()
                        .and_then(|p| p.get(key))
                        .and_then(|v| {
                            v.as_str()
                                .map(ToOwned::to_owned)
                                .or_else(|| v.as_f64().map(|n| n.to_string()))
                        })
                };
                let px_usd = string_param("px_usd");
                let px_vol = if px_usd.is_none() {
                    string_param("px_vol")
                } else {
                    None
                };
                let px = if px_usd.is_none() && px_vol.is_none() {
                    command.price.map(|v| v.to_string())
                } else {
                    None
                };
                anyhow::ensure!(
                    command.trigger_price.is_none() && args[0]["reqId"] == self.request_id,
                    "ordinary Modify ACK cannot claim unsupported trigger or another request"
                );
                anyhow::ensure!(
                    args[0]["newSz"].as_str() == command.quantity.map(|v| v.to_string()).as_deref()
                        && args[0]["newPx"].as_str() == px.as_deref()
                        && args[0]["newPxUsd"].as_str() == px_usd.as_deref()
                        && args[0]["newPxVol"].as_str() == px_vol.as_deref()
                        && args[0]["rpiTakerAccess"].as_bool()
                            == command
                                .params
                                .as_ref()
                                .and_then(|p| p.get_bool("rpi_taker_access"))
                        && args[0]["rpiPxRound"].as_bool()
                            == command
                                .params
                                .as_ref()
                                .and_then(|p| p.get_bool("rpi_px_round")),
                    "original Modify parameters differ from routed request"
                );
                (
                    command.command_id,
                    command.trader_id,
                    command.strategy_id,
                    command.instrument_id,
                    command.client_order_id,
                    command.ts_init,
                )
            }
            "cancel-order" => {
                let command: CancelOrder = serde_json::from_str(&self.command_payload_json)?;
                (
                    command.command_id,
                    command.trader_id,
                    command.strategy_id,
                    command.instrument_id,
                    command.client_order_id,
                    command.ts_init,
                )
            }
            _ => anyhow::bail!("unsupported management operation ACK command"),
        };
        anyhow::ensure!(
            id == self.command_id
                && trader == self.trader_id
                && strategy == self.strategy_id
                && instrument == self.instrument_id
                && client == self.client_order_id
                && ts_init.as_u64() <= self.issued_ns
                && self.command_payload_json.len() + self.request_payload_json.len()
                    <= MAX_SOURCE_BYTES,
            "original management command identity/time differs"
        );
        Ok(())
    }
}

/// Private bounded request context, captured before the actual owned socket
/// send. It is persisted by the existing adapter inventory until raw ACK capture.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingOperationAck {
    pub(crate) operation: String,
    pub(crate) binding: OrderOperationAckCommandBinding,
}
pub(crate) const OPERATION_ACK_CAPACITY: usize = 10_000;
pub(crate) const OPERATION_ACK_PENDING_BYTES: usize = 64 * 1024 * 1024;
impl PendingOperationAck {
    pub(crate) fn retained_bytes(&self) -> anyhow::Result<usize> {
        self.binding
            .command_payload_json
            .len()
            .checked_add(self.binding.request_payload_json.len())
            .ok_or_else(|| anyhow::anyhow!("original operation ACK retained byte overflow"))
    }
}
pub(crate) type OperationAckContexts =
    Arc<parking_lot::Mutex<std::collections::BTreeMap<String, PendingOperationAck>>>;

#[derive(Clone, Debug)]
pub struct PreparedOperationAck {
    pub(crate) operation: String,
    pub(crate) command_payload_json: String,
    pub(crate) account_id: AccountId,
    pub(crate) instrument_id_code: u64,
    pub(crate) routed_venue_order_id: Option<VenueOrderId>,
}
impl PreparedOperationAck {
    pub(crate) fn bind_actual_send(
        &self,
        request_payload_json: &str,
        connection: OrderOperationAckConnection,
        issued_ns: u64,
    ) -> anyhow::Result<PendingOperationAck> {
        let (command_id, trader_id, strategy_id, instrument_id, client_order_id) =
            match self.operation.as_str() {
                "amend-order" => {
                    let c: ModifyOrder = serde_json::from_str(&self.command_payload_json)?;
                    (
                        c.command_id,
                        c.trader_id,
                        c.strategy_id,
                        c.instrument_id,
                        c.client_order_id,
                    )
                }
                "cancel-order" => {
                    let c: CancelOrder = serde_json::from_str(&self.command_payload_json)?;
                    (
                        c.command_id,
                        c.trader_id,
                        c.strategy_id,
                        c.instrument_id,
                        c.client_order_id,
                    )
                }
                _ => anyhow::bail!("unsupported original operation ACK request"),
            };
        let binding = OrderOperationAckCommandBinding {
            command_id,
            command_payload_json: self.command_payload_json.clone(),
            request_id: command_id.to_string().replace('-', ""),
            request_payload_json: request_payload_json.to_owned(),
            trader_id,
            account_id: self.account_id,
            strategy_id,
            instrument_id,
            instrument_id_code: self.instrument_id_code,
            client_order_id,
            routed_venue_order_id: self.routed_venue_order_id,
            connection,
            issued_ns,
            matched_item_index: None,
            matched_item: None,
        };
        binding.validate(&self.operation)?;
        Ok(PendingOperationAck {
            operation: self.operation.clone(),
            binding,
        })
    }
}

pub(crate) type OperationAckRequestIds =
    Arc<parking_lot::Mutex<std::collections::BTreeSet<String>>>;
