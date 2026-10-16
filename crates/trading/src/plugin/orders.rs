// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Exact native construction and the existing submission interceptor/fence.

use anyhow::{Context, Result, ensure};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{enums::TimeInForce, identifiers::ClientOrderId, orders::Order};

use super::{PluginOrderType, PluginStrategy, require_version, wire};
use crate::strategy::{Strategy, StrategyNative};

fn decode_submit(input: &[u8]) -> Result<wire::SubmitRequest> {
    let header: serde_json::Value = serde_json::from_slice(input)?;
    let request = if header["schema_version"] == 1 {
        let legacy: wire::Submit = serde_json::from_slice(input)?;
        let (schema_version, instrument_id, side, quantity, reduce_only, price, post_only) =
            match legacy {
                wire::Submit::Market {
                    schema_version,
                    instrument_id,
                    side,
                    quantity,
                    reduce_only,
                } => (
                    schema_version,
                    instrument_id,
                    side,
                    quantity,
                    reduce_only,
                    None,
                    false,
                ),
                wire::Submit::Limit {
                    schema_version,
                    instrument_id,
                    side,
                    quantity,
                    price,
                    reduce_only,
                    post_only,
                } => (
                    schema_version,
                    instrument_id,
                    side,
                    quantity,
                    reduce_only,
                    Some(price),
                    post_only,
                ),
            };
        require_version(schema_version)?;
        wire::SubmitRequest {
            schema_version: 2,
            order_type: if price.is_some() {
                wire::NativeOrderType::Limit
            } else {
                wire::NativeOrderType::Market
            },
            instrument_id,
            side,
            quantity,
            price,
            trigger_price: None,
            trigger_type: None,
            time_in_force: TimeInForce::Gtc,
            expire_time_ns: None,
            reduce_only,
            post_only,
            position_id: None,
        }
    } else {
        serde_json::from_slice(input)?
    };
    ensure!(
        request.schema_version == 2,
        "unknown native order request version"
    );
    Ok(request)
}

impl PluginStrategy {
    pub(super) fn submit_encoded(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        self.submit_native_request(&decode_submit(input)?, None)
    }

    pub(super) fn submit_native_request(
        &mut self,
        request: &wire::SubmitRequest,
        client_order_id: Option<ClientOrderId>,
    ) -> Result<Vec<u8>> {
        self.require_instrument(request.instrument_id)?;
        let kind = match request.order_type {
            wire::NativeOrderType::Market => PluginOrderType::Market,
            wire::NativeOrderType::Limit => PluginOrderType::Limit,
            wire::NativeOrderType::StopMarket => PluginOrderType::StopMarket,
            wire::NativeOrderType::StopLimit => PluginOrderType::StopLimit,
        };
        ensure!(
            self.scope.order_types.contains(&kind),
            "native order capability was not admitted"
        );
        let limit = matches!(kind, PluginOrderType::Limit | PluginOrderType::StopLimit);
        let stop = matches!(
            kind,
            PluginOrderType::StopMarket | PluginOrderType::StopLimit
        );
        ensure!(
            request.price.is_some() == limit
                && request.trigger_price.is_some() == stop
                && request.trigger_type.is_some() == stop,
            "native order fields differ from the declared order type"
        );
        ensure!(
            !request.post_only || limit,
            "post-only is unavailable for native market orders"
        );
        let expiry = request.expire_time_ns.map(UnixNanos::from);
        ensure!(
            (request.time_in_force == TimeInForce::Gtd) == expiry.is_some(),
            "GTD requires its exact expiry and other time-in-force modes forbid expiry"
        );
        if let Some(expiry) = expiry {
            ensure!(
                expiry > self.clock().timestamp_ns(),
                "native expiry is not in the future"
            );
        }
        if kind == PluginOrderType::Market {
            ensure!(expiry.is_none(), "native market orders have no GTD expiry");
        }
        if let Some(id) = request.position_id {
            let cache = self.cache();
            let position = cache
                .position(&id)
                .context("native target position is absent")?;
            ensure!(
                Some(position.strategy_id) == self.strategy_id()
                    && position.account_id == self.scope.account_id
                    && position.instrument_id == request.instrument_id
                    && !position.is_closed(),
                "native target position belongs to another scope or is closed"
            );
        }
        let order = match kind {
            PluginOrderType::Market => self.order_factory().try_market(
                request.instrument_id,
                request.side,
                request.quantity,
                Some(request.time_in_force),
                Some(request.reduce_only),
                Some(false),
                None,
                None,
                None,
                client_order_id,
            )?,
            PluginOrderType::Limit => self.order_factory().try_limit(
                request.instrument_id,
                request.side,
                request.quantity,
                request.price.context("limit price absent")?,
                Some(request.time_in_force),
                expiry,
                Some(request.post_only),
                Some(request.reduce_only),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                client_order_id,
            )?,
            PluginOrderType::StopMarket => self.order_factory().try_stop_market(
                request.instrument_id,
                request.side,
                request.quantity,
                request
                    .trigger_price
                    .context("native stop trigger absent")?,
                request.trigger_type,
                Some(request.time_in_force),
                expiry,
                Some(request.reduce_only),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                client_order_id,
            )?,
            PluginOrderType::StopLimit => self.order_factory().try_stop_limit(
                request.instrument_id,
                request.side,
                request.quantity,
                request.price.context("limit price absent")?,
                request
                    .trigger_price
                    .context("native stop trigger absent")?,
                request.trigger_type,
                Some(request.time_in_force),
                expiry,
                Some(request.post_only),
                Some(request.reduce_only),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                client_order_id,
            )?,
        };
        let id = order.client_order_id();
        ensure!(
            client_order_id.is_none_or(|expected| id == expected),
            "native prepared order identity changed"
        );
        self.submit_order(order, request.position_id, Some(self.scope.client_id), None)?;
        Ok(serde_json::to_vec(&wire::OrderId {
            schema_version: wire::VERSION,
            client_order_id: id,
        })?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual_native_submit_decoder_refuses_duplicate_identity_fields_in_both_versions() {
        for raw in [
            r#"{"order_type":"market","schema_version":1,"schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","reduce_only":false}"#,
            r#"{"schema_version":2,"schema_version":2,"order_type":"market","instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","time_in_force":"GTC","reduce_only":false,"post_only":false}"#,
            r#"{"schema_version":2,"order_type":"market","instrument_id":"ETHUSDT-PERP.BINANCE","instrument_id":"BTCUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","time_in_force":"GTC","reduce_only":false,"post_only":false}"#,
        ] {
            assert!(decode_submit(raw.as_bytes()).is_err());
        }
        let legacy=br#"{"order_type":"market","schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","reduce_only":false}"#;
        let request = decode_submit(legacy).unwrap();
        assert_eq!(request.schema_version, 2);
        assert_eq!(request.time_in_force, TimeInForce::Gtc);
        assert!(
            request.price.is_none()
                && request.trigger_price.is_none()
                && request.position_id.is_none()
        );
    }
}
