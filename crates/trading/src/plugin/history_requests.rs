// -------------------------------------------------------------------------------------------------
// Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
// Licensed under the GNU Lesser General Public License Version 3.0.
// -------------------------------------------------------------------------------------------------

//! One bounded outstanding native history request, with exact original identity.

use anyhow::{Context, Result, ensure};
use nautilus_common::{actor::DataActor, messages::data::BarsResponse};

use super::{PluginStrategy, adapter::PendingHistory, require_version, wire};

impl PluginStrategy {
    pub(super) fn request_history(&mut self, input: &[u8]) -> Result<Vec<u8>> {
        ensure!(
            self.scope.allow_history_requests,
            "external historical requests are disabled by the admitted server policy"
        );
        let request: wire::HistoryRequest = serde_json::from_slice(input)?;
        require_version(request.schema_version)?;
        self.require_instrument(request.bar_type.instrument_id())?;
        ensure!(
            request.bar_type.is_standard() && self.scope.bar_types.contains(&request.bar_type),
            "historical bars are outside the admitted declaration"
        );
        ensure!(
            request.limit > 0 && request.limit as usize <= self.scope.max_history_bars,
            "historical bars request exceeds the admitted bound"
        );
        ensure!(
            request.start_ns <= request.end_ns
                && request.end_ns <= self.clock().timestamp_ns().as_u64(),
            "historical bars request has an invalid or future source range"
        );
        ensure!(
            self.adapter.pending_history.is_none(),
            "native history request is already pending"
        );
        let id = nautilus_common::recovery_trace::native_event_uuid();
        self.adapter.pending_history = Some(PendingHistory {
            request_id: id.to_string(),
            request,
        });
        if let Err(error) = self.send_pending_history() {
            // Core's fallible request API refuses before sending on Err. A
            // panic or missing response leaves the original pending identity.
            self.adapter.pending_history = None;
            return Err(error);
        }
        Ok(serde_json::to_vec(
            &serde_json::json!({"schema_version":wire::VERSION,"request_id":id.to_string(),"completion_proven":false}),
        )?)
    }

    pub(super) fn send_pending_history(&mut self) -> Result<()> {
        ensure!(
            self.scope.allow_history_requests,
            "restored external historical request is outside the admitted server policy"
        );
        let pending = self
            .adapter
            .pending_history
            .clone()
            .context("native history request is absent")?;
        let id = pending.id()?;
        let request = pending.request;
        let returned = self.request_bars_with_id(
            id,
            request.bar_type,
            Some(jiff::Timestamp::from_nanosecond(i128::from(
                request.start_ns,
            ))?),
            Some(jiff::Timestamp::from_nanosecond(i128::from(
                request.end_ns,
            ))?),
            std::num::NonZeroUsize::new(request.limit as usize),
            Some(self.scope.data_client_id),
            None,
        )?;
        ensure!(
            returned == id,
            "native history correlation identity changed"
        );
        Ok(())
    }

    pub(super) fn history_response(&mut self, response: &BarsResponse) -> Result<()> {
        let valid = (|| -> Result<()> {
            let pending = self
                .adapter
                .pending_history
                .as_ref()
                .context("native history response has no pending owner")?;
            ensure!(
                response.correlation_id.to_string() == pending.request_id
                    && response.client_id == self.scope.data_client_id
                    && response.bar_type == pending.request.bar_type,
                "native history response belongs to another request/client/bar route"
            );
            ensure!(
                response.data.len() <= pending.request.limit as usize
                    && response
                        .data
                        .iter()
                        .all(|bar| bar.bar_type == response.bar_type),
                "native history response exceeds its original bound or bar domain"
            );
            Ok(())
        })();
        self.retain_route_result(valid)?;
        // The ordinary bus already consumed this handler. Historical direct
        // dispatch retires the exact restored local handler through the same
        // owner check, so an already-completed request cannot linger.
        self.retire_native_bars_response(response.correlation_id)?;
        self.adapter.pending_history = None;
        self.history_reissue_after_restore = false;
        self.event(
            "historical_bars_response",
            serde_json::json!({
            "request_id":response.correlation_id.to_string(),"client_id":response.client_id,
            "bar_type":response.bar_type,"data":response.data,"ts_init":response.ts_init,
            "start":response.start,"end":response.end,"params":response.params,"completed":true}),
        )
    }
}
