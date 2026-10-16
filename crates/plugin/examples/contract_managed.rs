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
}
fn error(value: impl std::fmt::Display) -> PluginError {
    PluginError::generic(value.to_string())
}
impl Algorithm for Managed {
    const DESCRIPTOR_JSON: &'static str = include_str!("contract_managed.descriptor.json");
    fn validate(raw: &[u8]) -> Result<(), PluginError> {
        if raw == b"managed-v1" {
            Ok(())
        } else {
            Err(error("managed fixture config differs"))
        }
    }
    fn create(raw: &[u8]) -> Result<Self, PluginError> {
        Self::validate(raw)?;
        Ok(Self {
            state: State::default(),
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
        if value["kind"] != "timer" || self.state.attempted {
            return Err(error("managed fixture event differs or repeats"));
        }
        self.state.attempted = true;
        let intent=serde_json::to_vec(&json!({"schema_version":1,"instrument_id":"ETHUSDT-PERP.BINANCE","side":"BUY","intent":{"kind":"entry"}})).map_err(error)?;
        match host.call(HostOperation::ManagedSubmit, &intent) {
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
