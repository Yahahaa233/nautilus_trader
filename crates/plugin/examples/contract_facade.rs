// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! Private non-financial native facade probe. This is not a trading strategy.

use nautilus_model::{data::BarType, identifiers::InstrumentId};
use nautilus_plugin::{
    PluginError,
    strategy::{Algorithm, Host, HostOperation, ReadOnlyHost, ReadOperation},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    instrument_id: InstrumentId,
    bar_type: BarType,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    initialized: bool,
    subscribed: bool,
    quotes: u64,
    trades: u64,
    bars: u64,
    historical_bars: u64,
    history_request_id: Option<String>,
    native_submission_refusals: u64,
    scoped_refusals: u64,
}

#[derive(Debug)]
struct Facade {
    configuration: Configuration,
    state: State,
}

fn error(value: impl std::fmt::Display) -> PluginError {
    PluginError::generic(value.to_string())
}

fn encode(value: &Value) -> Result<Vec<u8>, PluginError> {
    serde_json::to_vec(value).map_err(error)
}

impl Facade {
    fn configuration(raw: &[u8]) -> Result<Configuration, PluginError> {
        let value: Configuration = serde_json::from_slice(raw).map_err(error)?;
        if value.bar_type.instrument_id() != value.instrument_id || !value.bar_type.is_standard() {
            return Err(error("facade fixture instrument/bar mismatch"));
        }
        Ok(value)
    }

    fn subscription(
        &mut self,
        host: &mut Host<'_>,
        operation: HostOperation,
    ) -> Result<(), PluginError> {
        for value in [
            json!({"kind":"quote","schema_version":2,"instrument_id":self.configuration.instrument_id}),
            json!({"kind":"trade","schema_version":2,"instrument_id":self.configuration.instrument_id}),
            json!({"kind":"bar","schema_version":2,"bar_type":self.configuration.bar_type}),
        ] {
            host.call(operation, &encode(&value)?)?;
        }
        self.state.subscribed = operation == HostOperation::Subscribe;
        Ok(())
    }

    fn refuse(
        &mut self,
        host: &mut Host<'_>,
        operation: HostOperation,
        value: &Value,
    ) -> Result<(), PluginError> {
        if host.call(operation, &encode(value)?).is_ok() {
            return Err(error(
                "actual native facade accepted an invalid fixture request",
            ));
        }
        self.state.scoped_refusals += 1;
        Ok(())
    }

    fn inspect_facts(&self, host: &mut ReadOnlyHost<'_>) -> Result<(), PluginError> {
        let request =
            encode(&json!({"schema_version":1,"instrument_id":self.configuration.instrument_id}))?;
        for operation in [ReadOperation::Instrument, ReadOperation::Portfolio] {
            let value: Value =
                serde_json::from_slice(&host.call(operation, &request)?).map_err(error)?;
            if value["schema_version"] != 1 {
                return Err(error("native read-only facts are absent"));
            }
        }
        Ok(())
    }
}

impl Algorithm for Facade {
    const DESCRIPTOR_JSON: &'static str = include_str!("contract_facade.descriptor.json");

    fn validate(raw: &[u8]) -> Result<(), PluginError> {
        Self::configuration(raw).map(|_| ())
    }

    fn create(raw: &[u8]) -> Result<Self, PluginError> {
        Ok(Self {
            configuration: Self::configuration(raw)?,
            state: State::default(),
        })
    }

    fn start(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        if !self.state.subscribed {
            self.subscription(host, HostOperation::Subscribe)?;
        }
        if self.state.initialized {
            return Ok(());
        }
        let request =
            encode(&json!({"schema_version":1,"instrument_id":self.configuration.instrument_id}))?;
        for operation in [HostOperation::Instrument, HostOperation::Portfolio] {
            host.call(operation, &request)?;
        }
        let clock: Value =
            serde_json::from_slice(&host.call(HostOperation::Clock, b"")?).map_err(error)?;
        let now = clock["timestamp_ns"]
            .as_u64()
            .ok_or_else(|| error("native facade clock is absent"))?;
        let expiry = now
            .checked_add(5_000_000_000)
            .ok_or_else(|| error("facade expiry overflow"))?;
        for kind in ["market", "limit", "stop_market", "stop_limit"] {
            let limit = matches!(kind, "limit" | "stop_limit");
            let stop = matches!(kind, "stop_market" | "stop_limit");
            let result=host.call(HostOperation::Submit,&encode(&json!({"schema_version":2,"order_type":kind,
                "instrument_id":self.configuration.instrument_id,"side":"SELL","quantity":"0.001",
                "price":if limit {Some("799.99")}else{None},"trigger_price":if stop {Some("800.00")}else{None},
                "trigger_type":if stop {Some("MARK_PRICE")}else{None},
                "time_in_force":if kind=="market"{"GTC"}else{"GTD"},
                "expire_time_ns":if kind=="market"{None}else{Some(expiry)},
                "reduce_only":stop,"post_only":false,"position_id":null}))?);
            match result {
                Err(e) if e.message_string().contains("facade_probe_no_grant") => {
                    self.state.native_submission_refusals += 1;
                }
                Err(e) => return Err(e),
                Ok(_) => {
                    return Err(error(
                        "facade fixture unexpectedly acquired a financial grant",
                    ));
                }
            }
        }
        self.refuse(
            host,
            HostOperation::Instrument,
            &json!({"schema_version":1,"instrument_id":"BTCUSDT-PERP.BINANCE"}),
        )?;
        self.refuse(host,HostOperation::Portfolio,&json!({"schema_version":1,"instrument_id":self.configuration.instrument_id,"account_id":"BINANCE-OTHER"}))?;
        self.refuse(
            host,
            HostOperation::Subscribe,
            &json!({"kind":"quote","schema_version":2,"instrument_id":"BTCUSDT-PERP.BINANCE"}),
        )?;
        self.refuse(
            host,
            HostOperation::Log,
            &json!({"schema_version":1,"level":"info","message":"invalid\nrecord"}),
        )?;
        self.refuse(host,HostOperation::Submit,&json!({"schema_version":1,"order_type":"market","instrument_id":self.configuration.instrument_id,"side":"BUY","quantity":"0.000","reduce_only":false}))?;
        let response: Value=serde_json::from_slice(&host.call(HostOperation::RequestHistory,
            &encode(&json!({"schema_version":1,"bar_type":self.configuration.bar_type,"start_ns":0,"end_ns":now,"limit":1}))?)?).map_err(error)?;
        self.state.history_request_id = Some(
            response["request_id"]
                .as_str()
                .ok_or_else(|| error("original history request ID absent"))?
                .to_owned(),
        );
        self.state.initialized = true;
        Ok(())
    }

    fn on_event(&mut self, raw: &[u8], _: &mut Host<'_>) -> Result<(), PluginError> {
        let value: Value = serde_json::from_slice(raw).map_err(error)?;
        if value["schema_version"] != 1 {
            return Err(error("unknown facade event protocol"));
        }
        match value["kind"].as_str() {
            Some("quote") => self.state.quotes += 1,
            Some("trade") => self.state.trades += 1,
            Some("bar") => self.state.bars += 1,
            Some("historical_bars_response") => {
                let response = &value["event"];
                if response["request_id"].as_str() != self.state.history_request_id.as_deref()
                    || response["completed"] != true
                {
                    return Err(error(
                        "history completion differs from the original native request",
                    ));
                }
                self.state.historical_bars += response["data"]
                    .as_array()
                    .ok_or_else(|| error("history data absent"))?
                    .len() as u64;
                self.state.history_request_id = None;
            }
            _ => return Err(error("unexpected native facade event")),
        }
        Ok(())
    }

    fn save(&self, host: &mut ReadOnlyHost<'_>) -> Result<Vec<u8>, PluginError> {
        self.inspect_facts(host)?;
        serde_json::to_vec(&self.state).map_err(error)
    }

    fn restore(
        &mut self,
        version: u32,
        raw: &[u8],
        host: &mut ReadOnlyHost<'_>,
    ) -> Result<(), PluginError> {
        if version != 1 {
            return Err(error("unsupported facade state version"));
        }
        self.inspect_facts(host)?;
        self.state = serde_json::from_slice(raw).map_err(error)?;
        Ok(())
    }

    fn pause(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        if self.state.subscribed {
            self.subscription(host, HostOperation::Unsubscribe)?;
        }
        Ok(())
    }

    fn resume(&mut self, host: &mut Host<'_>) -> Result<(), PluginError> {
        self.start(host)
    }

    fn stop(&mut self, _: &[u8], host: &mut Host<'_>) -> Result<(), PluginError> {
        self.pause(host)
    }
}

nautilus_plugin::nautilus_plugin! {name:"native_contract_facade",vendor:"Metis",version:"1.0.0",}
nautilus_plugin::nautilus_strategy!(Facade);
