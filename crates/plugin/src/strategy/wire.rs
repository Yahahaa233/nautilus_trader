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
    enums::OrderSide,
    identifiers::{ClientOrderId, InstrumentId},
    types::{Price, Quantity},
};
use serde::{Deserialize, Serialize};

/// First encoded native strategy protocol.
pub const VERSION: u32 = 1;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Subscription {
    pub schema_version: u32,
    pub bar_type: BarType,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderId {
    pub schema_version: u32,
    pub client_order_id: ClientOrderId,
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
