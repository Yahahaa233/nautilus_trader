// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Non-financial timing-state fixture using a separate compiled algorithm.

use nautilus_plugin::{
    PluginError,
    strategy::{Algorithm, Host, HostOperation, ReadOnlyHost},
};
use serde_json::{Value, json};

#[derive(Debug)]
struct Interval {
    scheduled_at: u64,
    elapsed_ns: u64,
    denied: u64,
}
fn error(message: impl AsRef<str>) -> PluginError {
    PluginError::generic(message)
}

impl Algorithm for Interval {
    const DESCRIPTOR_JSON: &'static str = include_str!("contract_interval.descriptor.json");
    fn validate(configuration: &[u8]) -> Result<(), PluginError> {
        if configuration == b"interval-v1" {
            Ok(())
        } else {
            Err(error("unsupported interval configuration"))
        }
    }
    fn create(_: &[u8]) -> Result<Self, PluginError> {
        Ok(Self {
            scheduled_at: 0,
            elapsed_ns: 0,
            denied: 0,
        })
    }
    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        if self.elapsed_ns > 0 {
            return Ok(());
        }
        let clock: Value = serde_json::from_slice(&host.call(HostOperation::Clock, b"")?)
            .map_err(|e| error(e.to_string()))?;
        self.scheduled_at = clock["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| error("native clock missing"))?;
        let alert_at = self
            .scheduled_at
            .checked_add(40_000_000)
            .ok_or_else(|| error("interval timer overflow"))?;
        host.call(
            HostOperation::SetTimer,
            &serde_json::to_vec(
                &json!({"schema_version":1,"name":"interval","alert_at_ns":alert_at}),
            )
            .map_err(|e| error(e.to_string()))?,
        )?;
        Ok(())
    }
    fn on_event(&mut self, event: &[u8], host: &mut Host<'_>) -> Result<(), PluginError> {
        let event: Value = serde_json::from_slice(event).map_err(|e| error(e.to_string()))?;
        if event["schema_version"] != 1 || event["kind"] != "timer" || self.elapsed_ns > 0 {
            return Err(error("unexpected interval event"));
        }
        let at = event["event"]["ts_event"]
            .as_u64()
            .ok_or_else(|| error("native event timestamp missing"))?;
        self.elapsed_ns = at
            .checked_sub(self.scheduled_at)
            .filter(|v| *v > 0)
            .ok_or_else(|| error("invalid original timer interval"))?;
        let request=serde_json::to_vec(&json!({"order_type":"market","schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"SELL","quantity":"0.002","reduce_only":false})).map_err(|e|error(e.to_string()))?;
        match host.call(HostOperation::Submit, &request) {
            Err(e) if e.message_string().contains("contract_probe_no_grant") => self.denied += 1,
            Err(e) => return Err(e),
            Ok(_) => {
                return Err(error(
                    "interval unexpectedly bypassed the native submission fence",
                ));
            }
        }
        Ok(())
    }
    fn save(&self, _: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        serde_json::to_vec(&json!({"scheduled_at":self.scheduled_at,"elapsed_ns":self.elapsed_ns,"denied":self.denied})).map_err(|e|error(e.to_string()))
    }
    fn restore(
        &mut self,
        version: u32,
        state: &[u8],
        _: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError> {
        let value: Value = serde_json::from_slice(state).map_err(|e| error(e.to_string()))?;
        if version != 1 || value.as_object().is_none_or(|v| v.len() != 3) {
            return Err(error("invalid interval state schema"));
        }
        self.scheduled_at = value["scheduled_at"]
            .as_u64()
            .ok_or_else(|| error("interval source time missing"))?;
        self.elapsed_ns = value["elapsed_ns"]
            .as_u64()
            .ok_or_else(|| error("interval state missing"))?;
        self.denied = value["denied"]
            .as_u64()
            .ok_or_else(|| error("interval denial missing"))?;
        Ok(())
    }
    fn pause(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn resume(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn stop(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
}
nautilus_plugin::nautilus_plugin! { name: "native_contract_interval", vendor: "Native conformance fixture", version: "1.0.0", }
nautilus_plugin::nautilus_strategy!(Interval);
