// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Non-financial probe: deliberately tries to ignore a fatal native host error.
use nautilus_plugin::{
    PluginError,
    strategy::{Algorithm, Host, HostOperation, ReadOnlyHost},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    attempted: bool,
    result: Option<Value>,
    error: Option<Value>,
    secondary: Option<Value>,
}
#[derive(Debug)]
struct Managed {
    state: State,
    batch: bool,
    empty_batch: bool,
    frame_fault: bool,
}
fn error(value: impl std::fmt::Display) -> PluginError {
    PluginError::generic(value.to_string())
}
impl Algorithm for Managed {
    const DESCRIPTOR_JSON: &'static str = include_str!("contract_managed.descriptor.json");
    fn validate(raw: &[u8]) -> Result<(), PluginError> {
        if [
            b"managed-v1".as_slice(),
            b"managed-batch-v1",
            b"managed-empty-batch-v1",
            b"managed-bracket-v1",
            b"managed-frame-fault-v1",
        ]
        .contains(&raw)
        {
            Ok(())
        } else {
            Err(error("managed fixture config differs"))
        }
    }
    fn create(raw: &[u8]) -> Result<Self, PluginError> {
        Self::validate(raw)?;
        Ok(Self {
            state: State::default(),
            batch: raw == b"managed-batch-v1"
                || raw == b"managed-empty-batch-v1"
                || raw == b"managed-frame-fault-v1",
            empty_batch: raw == b"managed-empty-batch-v1",
            frame_fault: raw == b"managed-frame-fault-v1",
        })
    }
    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        if self.state.attempted {
            return Ok(());
        }
        let clock: Value =
            serde_json::from_slice(&host.call(HostOperation::Clock, b"")?).map_err(error)?;
        let at = clock["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| error("native clock absent"))?
            .checked_add(20_000_000)
            .ok_or_else(|| error("timer overflow"))?;
        host.call(
            HostOperation::SetTimer,
            &serde_json::to_vec(
                &json!({"schema_version":1,"name":"managed_once","alert_at_ns":at}),
            )
            .map_err(error)?,
        )?;
        Ok(())
    }
    fn on_event(&mut self, raw: &[u8], host: &mut Host<'_>) -> Result<(), PluginError> {
        let value: Value = serde_json::from_slice(raw).map_err(error)?;
        if value["kind"] == "managed_frame" {
            if !self.batch || !self.state.attempted {
                return Err(error("unexpected private application frame lifecycle"));
            }
            let frame: Value = serde_json::from_str(
                value["event"]["value"]
                    .as_str()
                    .ok_or_else(|| error("private frame bytes absent"))?,
            )
            .map_err(error)?;
            if frame["schema_version"] != 1
                || frame["frame_owner"] != "MANAGED-001"
                || frame["frame_sequence"] != 1
            {
                return Err(error("original private application frame changed"));
            }
            if self.frame_fault {
                return Err(error("original private managed-frame callback failure"));
            }
            let secondary = self.state.secondary.get_or_insert_with(|| json!({}));
            let events = secondary
                .as_object_mut()
                .ok_or_else(|| error("private event inventory differs"))?
                .entry("frame_events")
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .ok_or_else(|| error("private frame event inventory differs"))?;
            events.push(value["event"].clone());
            return Ok(());
        }
        if value["kind"] == "order" {
            let mut events = self
                .state
                .secondary
                .take()
                .and_then(|value| value["order_events"].as_array().cloned())
                .unwrap_or_default();
            events.push(value["event"].clone());
            self.state.secondary = Some(json!({"order_events":events}));
            return Ok(());
        }
        if value["kind"] != "timer" || self.state.attempted {
            return Err(error("managed fixture event differs or repeats"));
        }
        self.state.attempted = true;
        let intent=serde_json::to_vec(&json!({"schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","intent":{"kind":"entry"}})).map_err(error)?;
        let (operation, intent) = if self.batch {
            let one: Value = serde_json::from_slice(&intent).map_err(error)?;
            (
                HostOperation::ManagedSubmitBatch,
                serde_json::to_vec(&json!({"schema_version":1,"intents":if self.empty_batch {Vec::new()} else {vec![one.clone(),one]}}))
                    .map_err(error)?,
            )
        } else {
            (HostOperation::ManagedSubmit, intent)
        };
        match host.call(operation, &intent) {
            Ok(raw) => self.state.result = Some(serde_json::from_slice(&raw).map_err(error)?),
            Err(original) => {
                self.state.error = Some(
                    json!({"code":format!("{:?}",original.code),"message":original.message_string()}),
                );
                let raw=serde_json::to_vec(&json!({"schema_version":1,"order_type":"market","instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","quantity":"0.001","reduce_only":false})).map_err(error)?;
                self.state.secondary = Some(match host.call(HostOperation::Submit, &raw) {
                    Ok(_) => json!({"unexpected_success":true}),
                    Err(failure) => {
                        json!({"code":format!("{:?}",failure.code),"message":failure.message_string()})
                    }
                });
            }
        }
        // Fatal native errors must remain terminal even when the producer returns Ok.
        Ok(())
    }
    fn save(&self, _: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        serde_json::to_vec(&self.state).map_err(error)
    }
    fn restore(
        &mut self,
        version: u32,
        raw: &[u8],
        _: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError> {
        if version != 1 {
            return Err(error("managed state version differs"));
        }
        self.state = serde_json::from_slice(raw).map_err(error)?;
        Ok(())
    }
    fn pause(&mut self, _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
    fn resume(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        self.start(host)
    }
    fn stop(&mut self, _: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        Ok(())
    }
}
nautilus_plugin::nautilus_plugin! {name:"native_contract_managed",vendor:"Metis",version:"1.0.0",}
nautilus_plugin::nautilus_strategy!(Managed);
