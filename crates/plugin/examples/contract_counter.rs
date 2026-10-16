// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

//! Non-financial native ABI fixture, not a production-qualified Metis strategy.
//! It requests one native timer and observes a real native submission refusal.

use nautilus_plugin::{
    PluginError,
    strategy::{Algorithm, Host, HostOperation, ReadOnlyHost},
};
use serde_json::{Value, json};

#[derive(Debug)]
struct Counter {
    events: u64,
    denied: u64,
    pending: bool,
    failure_mode: u8,
}

fn error(message: impl AsRef<str>) -> PluginError {
    PluginError::generic(message)
}

fn inject_original_panic() -> ! {
    panic!("contract_counter_original_event_panic")
}

impl Algorithm for Counter {
    const DESCRIPTOR_JSON: &'static str = include_str!("contract_counter.descriptor.json");
    fn validate(configuration: &[u8]) -> Result<(), PluginError> {
        if [
            b"counter-v1".as_slice(),
            b"counter-fail-event-v1",
            b"counter-panic-event-v1",
        ]
        .contains(&configuration)
        {
            Ok(())
        } else {
            Err(error("unsupported counter configuration"))
        }
    }
    fn create(configuration: &[u8]) -> Result<Self, PluginError> {
        Ok(Self {
            events: 0,
            denied: 0,
            pending: false,
            failure_mode: match configuration {
                b"counter-fail-event-v1" => 1,
                b"counter-panic-event-v1" => 2,
                _ => 0,
            },
        })
    }
    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        if self.events > 0 {
            return Ok(());
        }
        let clock: Value = serde_json::from_slice(&host.call(HostOperation::Clock, b"")?)
            .map_err(|e| error(e.to_string()))?;
        let now = clock["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| error("native clock missing"))?;
        let alert_at = now
            .checked_add(20_000_000)
            .ok_or_else(|| error("counter timer overflow"))?;
        host.call(
            HostOperation::SetTimer,
            &serde_json::to_vec(
                &json!({"schema_version":1,"name":"counter","alert_at_ns":alert_at}),
            )
            .map_err(|e| error(e.to_string()))?,
        )?;
        self.pending = true;
        Ok(())
    }
    fn on_event(&mut self, event: &[u8], host: &mut Host<'_>) -> Result<(), PluginError> {
        let event: Value = serde_json::from_slice(event).map_err(|e| error(e.to_string()))?;
        if event["schema_version"] != 1 || event["kind"] != "timer" {
            return Err(error("unexpected counter native event"));
        }
        if !self.pending {
            return Err(error("duplicate counter timer"));
        }
        match self.failure_mode {
            1 => return Err(error("contract_counter_original_event_error")),
            2 => inject_original_panic(),
            _ => {}
        }
        self.pending = false;
        self.events += 1;
        let request=serde_json::to_vec(&json!({"order_type":"market","schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","reduce_only":false})).map_err(|e|error(e.to_string()))?;
        match host.call(HostOperation::Submit, &request) {
            Err(e) if e.message_string().contains("contract_probe_no_grant") => self.denied += 1,
            Err(e) => return Err(e),
            Ok(_) => {
                return Err(error(
                    "counter unexpectedly bypassed the native submission fence",
                ));
            }
        }
        Ok(())
    }
    fn save(&self, _: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        serde_json::to_vec(
            &json!({"events":self.events,"denied":self.denied,"pending":self.pending}),
        )
        .map_err(|e| error(e.to_string()))
    }
    fn restore(
        &mut self,
        version: u32,
        state: &[u8],
        _: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError> {
        let state: Value = serde_json::from_slice(state).map_err(|e| error(e.to_string()))?;
        if version != 1 || state.as_object().is_none_or(|fields| fields.len() != 3) {
            return Err(error("invalid counter state schema"));
        }
        self.events = state["events"]
            .as_u64()
            .ok_or_else(|| error("counter events missing"))?;
        self.denied = state["denied"]
            .as_u64()
            .ok_or_else(|| error("counter denial missing"))?;
        self.pending = state["pending"]
            .as_bool()
            .ok_or_else(|| error("counter timer state missing"))?;
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

nautilus_plugin::nautilus_plugin! { name: "native_contract_counter", vendor: "Native conformance fixture", version: "1.0.0", }
nautilus_plugin::nautilus_strategy!(Counter);
