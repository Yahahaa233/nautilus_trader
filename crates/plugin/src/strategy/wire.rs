// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Encoded strategy requests shared by algorithms and the native host.

use nautilus_model::{
    data::BarType,
    enums::{OrderSide, TimeInForce, TriggerType},
    identifiers::{ClientOrderId, InstrumentId, PositionId},
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

/// First encoded native strategy protocol.
pub const VERSION: u32 = 1;

/// A read-only request within this instance's admitted instrument domain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstrumentRequest {
    pub schema_version: u32,
    pub instrument_id: InstrumentId,
}

/// Reads only already cached native bars at or before the native callback clock.
/// A missing cache returns an empty array; this never falls back to HTTP/catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CachedBarsRequest {
    pub schema_version: u32,
    pub bar_type: BarType,
    pub limit: u32,
}

/// Bounded historical bars. Nanosecond bounds preserve the original native
/// time range; the host returns the actual request UUID, never a completion.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryRequest {
    pub schema_version: u32,
    pub bar_type: BarType,
    pub start_ns: u64,
    pub end_ns: u64,
    pub limit: u32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogRecord {
    pub schema_version: u32,
    pub level: LogLevel,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub schema_version: u32,
    pub bar_type: BarType,
}

/// Explicit data family; original untagged version-1 bar requests remain read.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DataSubscription {
    Bar {
        schema_version: u32,
        bar_type: BarType,
    },
    Quote {
        schema_version: u32,
        instrument_id: InstrumentId,
    },
    Trade {
        schema_version: u32,
        instrument_id: InstrumentId,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Timer {
    pub schema_version: u32,
    pub name: String,
    pub alert_at_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimerName {
    pub schema_version: u32,
    pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "order_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Submit {
    Market {
        schema_version: u32,
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        reduce_only: bool,
    },
    Limit {
        schema_version: u32,
        instrument_id: InstrumentId,
        side: OrderSide,
        quantity: Quantity,
        price: Price,
        reduce_only: bool,
        post_only: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NativeOrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
}

/// Complete version-2 native order construction. Irrelevant fields must be
/// absent; prices and quantities use the model's exact decimal encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitRequest {
    pub schema_version: u32,
    pub order_type: NativeOrderType,
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    pub quantity: Quantity,
    pub price: Option<Price>,
    pub trigger_price: Option<Price>,
    pub trigger_type: Option<TriggerType>,
    pub time_in_force: TimeInForce,
    pub expire_time_ns: Option<u64>,
    pub reduce_only: bool,
    pub post_only: bool,
    pub position_id: Option<PositionId>,
    /// Bounded algorithm annotations. Native execution/fault authority is not
    /// encoded in tags; reserved framework tags are rejected by the host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderId {
    pub schema_version: u32,
    pub client_order_id: ClientOrderId,
}

/// The guest supplies no account, client, instrument, order identity or quantity.
/// The original native framework uses the immutable host-installed scope.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedMarketExit {
    pub schema_version: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Modify {
    pub schema_version: u32,
    pub client_order_id: ClientOrderId,
    pub quantity: Option<Quantity>,
    pub price: Option<Price>,
    pub trigger_price: Option<Price>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_order_wire_preserves_exact_conditional_fields_and_rejects_unknown_scope() {
        let original = serde_json::json!({"schema_version":2,"order_type":"stop_limit",
            "instrument_id":"ETHUSDT-PERP.BINANCE","side":"SELL","quantity":"0.00000001",
            "price":"12345.67000001","trigger_price":"12345.67000002","trigger_type":"MARK_PRICE",
            "time_in_force":"GTD","expire_time_ns":u64::MAX,"reduce_only":true,"post_only":false,
            "position_id":"P-001"});
        let request: SubmitRequest = serde_json::from_value(original.clone()).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), original);
        let mut foreign = original.clone();
        foreign["account_id"] = serde_json::json!("BINANCE-OTHER");
        assert!(serde_json::from_value::<SubmitRequest>(foreign).is_err());
        for field in ["quantity", "price", "trigger_price"] {
            let mut approximate = original.clone();
            approximate[field] = serde_json::json!(0.125);
            assert!(serde_json::from_value::<SubmitRequest>(approximate).is_err());
        }
    }

    #[test]
    fn shared_wire_preserves_exact_values_and_refuses_ambiguous_requests() {
        let original = serde_json::json!({"order_type":"limit","schema_version":1,
            "instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.00000001",
            "price":"12345.67000001","reduce_only":false,"post_only":true});
        let request: Submit = serde_json::from_value(original.clone()).unwrap();
        assert_eq!(serde_json::to_value(request).unwrap(), original);
        for field in ["quantity", "price"] {
            let mut approximate = original.clone();
            approximate[field] = serde_json::json!(0.125);
            assert!(serde_json::from_value::<Submit>(approximate).is_err());
        }
        let mut unknown = original;
        unknown["another_account"] = serde_json::json!("B");
        assert!(serde_json::from_value::<Submit>(unknown).is_err());
        let timer = Timer {
            schema_version: VERSION,
            name: "exact_time".into(),
            alert_at_ns: u64::MAX,
        };
        let restored: Timer = serde_json::from_slice(&serde_json::to_vec(&timer).unwrap()).unwrap();
        assert_eq!(restored.alert_at_ns, u64::MAX);
    }
}

/// Application policy input. Context identity and quantity are never guest fields.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedSubmit {
    pub schema_version: u32,
    pub instrument_id: nautilus_model::identifiers::InstrumentId,
    pub side: nautilus_model::enums::OrderSide,
    pub intent: serde_json::Value,
}

/// One bounded portfolio intent frame. An empty frame still commits the common
/// application policy; it generates no order identities. A zero installed bound
/// disables this operation entirely. Native identities are host-generated.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedSubmitBatch {
    pub schema_version: u32,
    pub intents: Vec<ManagedSubmit>,
}

#[cfg(test)]
mod managed_tests {
    use super::*;
    #[test]
    fn managed_intent_cannot_supply_native_or_economic_authority() {
        let value = serde_json::json!({"schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","intent":{"kind":"entry"}});
        assert!(serde_json::from_value::<ManagedSubmit>(value.clone()).is_ok());
        for key in [
            "account_id",
            "client_id",
            "strategy_id",
            "run_id",
            "client_order_id",
            "quantity",
            "equity",
            "available_budget",
        ] {
            let mut changed = value.clone();
            changed[key] = serde_json::json!("forged");
            assert!(serde_json::from_value::<ManagedSubmit>(changed).is_err());
        }
        assert_eq!(
            super::super::HostOperation::from_tag(14),
            Some(super::super::HostOperation::ManagedSubmit)
        );
    }
}
