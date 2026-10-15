//! Immutable authenticated-adapter observations for native protective-order reports.
//! These are custom data, not order state, command acknowledgements, or authorization.
use std::{any::Any, sync::Arc};

use nautilus_common::{live::sender::EventSender, messages::DataEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomData, CustomDataTrait, Data, HasTsInit},
    identifiers::{AccountId, TraderId},
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

pub const SOURCE_SCHEMA: &str = "OKXOrderProtectionSource.v1";
const MAX_SOURCE_BYTES: usize = 1_048_576;

/// Actual signed HTTP request body and received response body. Credentials and headers
/// are deliberately absent. Times describe adapter request/receive, never venue finality.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtectionHttpSource {
    pub method: String,
    pub path: String,
    pub request_body_json: String,
    pub response_body_json: String,
    pub status: u16,
    pub request_ns: u64,
    pub received_ns: u64,
}

/// An original response/frame can arrive before or after the native order event.
/// Keeping it independently avoids changing or re-emitting that original event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderProtectionSource {
    pub schema: String,
    pub kind: String,
    pub trader_id: TraderId,
    pub adapter_account_id: AccountId,
    pub command_payload_json: Option<String>,
    pub http: Option<ProtectionHttpSource>,
    pub websocket_payload_json: Option<String>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
    pub venue_command_causation_available: bool,
    pub execution_authorized: bool,
}
impl HasTsInit for OrderProtectionSource {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}
impl CustomDataTrait for OrderProtectionSource {
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
impl OrderProtectionSource {
    /// Validates the observation profile, without assigning an order or authorizing it.
    ///
    /// # Errors
    /// Returns an error for unsupported, inconsistent, or incomplete original protection evidence.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.schema == SOURCE_SCHEMA
                && !self.execution_authorized
                && !self.venue_command_causation_available,
            "invalid protective source profile"
        );
        anyhow::ensure!(
            self.ts_event == self.ts_init,
            "protective capture timestamp changed"
        );
        match self.kind.as_str() {
            "http_order_algo" | "http_amend_algo" | "http_cancel_algo" => {
                let http = self
                    .http
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("missing HTTP original"))?;
                let path = match self.kind.as_str() {
                    "http_order_algo" => "/api/v5/trade/order-algo",
                    "http_amend_algo" => "/api/v5/trade/amend-algos",
                    _ => "/api/v5/trade/cancel-algos",
                };
                anyhow::ensure!(
                    http.method == "POST"
                        && http.path == path
                        && http.request_ns <= http.received_ns
                        && http.received_ns == self.ts_init.as_u64()
                        && (200..300).contains(&http.status)
                        && self.command_payload_json.is_some()
                        && self.websocket_payload_json.is_none(),
                    "invalid HTTP protection source"
                );
                let _: serde_json::Value = serde_json::from_str(&http.request_body_json)?;
                let _: serde_json::Value = serde_json::from_str(&http.response_body_json)?;
            }
            "orders_algo" | "orders" => {
                anyhow::ensure!(
                    self.http.is_none() && self.command_payload_json.is_none(),
                    "private stream original contains a command claim"
                );
                let wire: serde_json::Value = serde_json::from_str(
                    self.websocket_payload_json
                        .as_deref()
                        .ok_or_else(|| anyhow::anyhow!("missing private stream original"))?,
                )?;
                let channel = wire["arg"]["channel"].as_str().unwrap_or_default();
                anyhow::ensure!(
                    matches!(
                        (self.kind.as_str(), channel),
                        ("orders_algo", "orders-algo" | "algo-advance") | ("orders", "orders")
                    ) && wire["data"].is_array(),
                    "invalid private stream protection source"
                );
            }
            _ => anyhow::bail!("unknown protective source kind"),
        }
        anyhow::ensure!(
            serde_json::to_vec(self)?.len() <= MAX_SOURCE_BYTES,
            "protective source exceeds input bound"
        );
        Ok(())
    }
}

/// Shared sender installed on the node owner thread. Async HTTP/WS tasks publish to
/// the existing data ingress; they never reach the thread-local bus directly.
#[derive(Clone, Debug, Default)]
pub(crate) struct ProtectionSourceEmitter(Arc<Mutex<SourcePublicationState>>);
#[derive(Debug, Default)]
struct SourcePublicationState {
    sender: Option<EventSender<DataEvent>>,
    first_failure: Option<String>,
}
impl ProtectionSourceEmitter {
    /// Whether the actual source publisher has been bound by its runtime owner.
    pub(crate) fn is_bound(&self) -> bool {
        self.0.lock().sender.is_some()
    }

    pub(crate) fn bind(&self, sender: EventSender<DataEvent>) {
        let mut state = self.0.lock();
        if state.first_failure.is_some() {
            sender.fail("original operation ACK source persistence failed");
        }
        state.sender = Some(sender);
    }
    pub(crate) fn http(
        &self,
        trader_id: TraderId,
        account: AccountId,
        command_payload_json: String,
        http: ProtectionHttpSource,
    ) -> anyhow::Result<()> {
        let kind = match http.path.as_str() {
            "/api/v5/trade/order-algo" => "http_order_algo",
            "/api/v5/trade/amend-algos" => "http_amend_algo",
            "/api/v5/trade/cancel-algos" => "http_cancel_algo",
            _ => anyhow::bail!("unsupported protective HTTP endpoint"),
        };
        let now = UnixNanos::from(http.received_ns);
        self.emit(OrderProtectionSource {
            schema: SOURCE_SCHEMA.into(),
            kind: kind.into(),
            trader_id,
            adapter_account_id: account,
            command_payload_json: Some(command_payload_json),
            http: Some(http),
            websocket_payload_json: None,
            ts_event: now,
            ts_init: now,
            venue_command_causation_available: false,
            execution_authorized: false,
        })
    }
    pub(crate) fn wire(
        &self,
        trader_id: TraderId,
        account: AccountId,
        payload: String,
        now: UnixNanos,
    ) -> anyhow::Result<()> {
        let parsed: serde_json::Value = serde_json::from_str(&payload)?;
        let kind = match parsed["arg"]["channel"].as_str() {
            Some("orders-algo" | "algo-advance") => "orders_algo",
            Some("orders") => "orders",
            _ => anyhow::bail!("unsupported protective stream channel"),
        };
        self.emit(OrderProtectionSource {
            schema: SOURCE_SCHEMA.into(),
            kind: kind.into(),
            trader_id,
            adapter_account_id: account,
            command_payload_json: None,
            http: None,
            websocket_payload_json: Some(payload),
            ts_event: now,
            ts_init: now,
            venue_command_causation_available: false,
            execution_authorized: false,
        })
    }
    pub(crate) fn operation_ack(
        &self,
        source: crate::operation_ack::OrderOperationAckSource,
    ) -> anyhow::Result<()> {
        let result = source
            .validate()
            .and_then(|()| self.send_custom(Arc::new(source)));
        if result.is_err() {
            self.fail_operation_ack();
        }
        result
    }
    pub(crate) fn fail_operation_ack(&self) {
        let mut state = self.0.lock();
        if state.first_failure.is_none() {
            state.first_failure = Some("original operation ACK source persistence failed".into());
        }
        if let Some(sender) = &state.sender {
            sender.fail(state.first_failure.as_deref().expect("latched failure"));
        }
    }
    pub(crate) fn verify(&self) -> anyhow::Result<()> {
        let state = self.0.lock();
        anyhow::ensure!(
            state.first_failure.is_none(),
            "{}",
            state.first_failure.as_deref().unwrap_or_default()
        );
        Ok(())
    }
    fn emit(&self, source: OrderProtectionSource) -> anyhow::Result<()> {
        source.validate()?;
        self.send_custom(Arc::new(source))
    }
    fn send_custom(&self, source: Arc<dyn CustomDataTrait>) -> anyhow::Result<()> {
        let sender = {
            let state = self.0.lock();
            anyhow::ensure!(
                state.first_failure.is_none(),
                "source publisher permanently failed"
            );
            state
                .sender
                .clone()
                .ok_or_else(|| anyhow::anyhow!("protective source ingress absent"))?
        };
        sender
            .send(DataEvent::Data(Data::Custom(CustomData::from_arc(source))))
            .map_err(|_| anyhow::anyhow!("protective source ingress closed"))?;
        Ok(())
    }
}

/// Registers the protection and independent operation ACK source decoders.
/// This installs a decoder; it supplies no trading or recovery authority.
///
/// # Errors
/// Returns an error if protection or operation acknowledgment decoders conflict with the native data registry.
pub fn register_source_type() -> anyhow::Result<()> {
    nautilus_model::data::registry::ensure_json_deserializer_registered(
        SOURCE_SCHEMA,
        Box::new(OrderProtectionSource::from_json),
    )?;
    crate::operation_ack::register_source_type()
}
