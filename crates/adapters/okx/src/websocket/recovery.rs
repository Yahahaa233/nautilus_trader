// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Restores actual socket replay state without reusing historical session ACKs.

use super::*;
use anyhow::{Context as _, Result, ensure};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub(super) struct SocketRecoveryPlan {
    pub(super) raw: Arc<crate::checkpoint::RecoveryRawPrefix>,
    confirmed: BTreeMap<String, Vec<String>>,
    metadata: Value,
    instruments: AHashMap<Ustr, InstrumentAny>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SocketInventory {
    profile: String,
    endpoint: String,
    requires_authentication: bool,
    active: bool,
    authenticated: bool,
    tasks: Value,
    request_id_counter: u64,
    pending_orders: u64,
    pending_cancels: u64,
    pending_amends: u64,
    pending_commands: u64,
    pending_decoded_outbox: u64,
    pending_handler_messages: u64,
    confirmed_subscriptions: BTreeMap<String, Vec<String>>,
    subscription_references: BTreeMap<String, usize>,
    raw_input: Value,
    desired_instrument_type_subscriptions: BTreeMap<String, Vec<String>>,
    desired_instrument_family_subscriptions: BTreeMap<String, Vec<String>>,
    desired_instrument_id_subscriptions: BTreeMap<String, Vec<String>>,
    desired_bare_subscriptions: BTreeMap<String, bool>,
    instruments: AHashMap<Ustr, InstrumentAny>,
    inst_id_codes: AHashMap<Ustr, u64>,
    trade_quote_ccy_lists: AHashMap<Ustr, Vec<Ustr>>,
    spot_trade_quote_ccy: Option<Ustr>,
    option_greeks_subscriptions: AHashMap<InstrumentId, AHashSet<OKXGreeksType>>,
    index_pair_subscribers: BTreeMap<Ustr, usize>,
}
fn parse_map<T: std::str::FromStr + Eq + std::hash::Hash>(
    source: BTreeMap<String, Vec<String>>,
) -> Result<Vec<(OKXWsChannel, AHashSet<T>)>>
where
    T::Err: std::fmt::Display,
{
    source
        .into_iter()
        .map(|(channel, values)| {
            let channel: OKXWsChannel = channel
                .parse()
                .map_err(|e| anyhow::anyhow!("unknown source channel: {e}"))?;
            let parsed = values
                .iter()
                .map(|value| {
                    value
                        .parse()
                        .map_err(|e| anyhow::anyhow!("unknown source subscription: {e}"))
                })
                .collect::<Result<AHashSet<T>>>()?;
            ensure!(
                parsed.len() == values.len(),
                "duplicate source subscription"
            );
            Ok((channel, parsed))
        })
        .collect()
}

impl OKXWebSocketClient {
    pub(crate) fn restore_running_checkpoint(&mut self, inventory: &Value) -> Result<()> {
        self.verify_paused_recovery_inventory()?;
        ensure!(
            self.checkpoint_recovery.is_none(),
            "socket recovery already installed"
        );
        let source: SocketInventory = serde_json::from_value(inventory.clone())?;
        ensure!(
            source.profile == "okx_live_socket_retained_raw_prefix.v2"
                && source.endpoint == self.url
                && source.requires_authentication == self.credential.is_some()
                && source.active
                && (!source.requires_authentication || source.authenticated),
            "socket source endpoint/authentication profile differs"
        );
        ensure!(
            source.pending_orders == 0
                && source.pending_cancels == 0
                && source.pending_amends == 0
                && source.pending_commands == 0
                && source.pending_decoded_outbox == 0
                && source.pending_handler_messages == 0
                && source.tasks["owned_tasks"].as_u64() == Some(1)
                && source.request_id_counter > 0
                && source.request_id_counter < u64::MAX,
            "socket source contains unsupported in-flight work"
        );
        let raw = Arc::new(crate::checkpoint::RecoveryRawPrefix::new(
            &source.raw_input,
        )?);
        let types = parse_map::<OKXInstrumentType>(source.desired_instrument_type_subscriptions)?;
        let families = parse_map::<Ustr>(source.desired_instrument_family_subscriptions)?;
        let ids = parse_map::<Ustr>(source.desired_instrument_id_subscriptions)?;
        let bare = source
            .desired_bare_subscriptions
            .into_iter()
            .map(|(channel, enabled)| {
                ensure!(enabled, "inactive source bare subscription");
                Ok((channel.parse::<OKXWsChannel>()?, enabled))
            })
            .collect::<Result<Vec<_>>>()?;
        ensure!(
            source.subscription_references.len() <= 100_000
                && source
                    .subscription_references
                    .values()
                    .all(|count| *count <= 10_000),
            "source subscription reference bound exceeded"
        );
        // Build and validate all intent before mutating the actual socket owner.
        let desired = SubscriptionState::new(OKX_WS_TOPIC_DELIMITER);
        let arguments = types
            .iter()
            .flat_map(|(channel, values)| {
                values.iter().map(move |value| OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: Some(*value),
                    inst_family: None,
                    inst_id: None,
                })
            })
            .chain(families.iter().flat_map(|(channel, values)| {
                values.iter().map(move |value| OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: None,
                    inst_family: Some(*value),
                    inst_id: None,
                })
            }))
            .chain(ids.iter().flat_map(|(channel, values)| {
                values.iter().map(move |value| OKXSubscriptionArg {
                    channel: channel.clone(),
                    inst_type: None,
                    inst_family: None,
                    inst_id: Some(*value),
                })
            }))
            .chain(bare.iter().map(|(channel, _)| OKXSubscriptionArg {
                channel: channel.clone(),
                inst_type: None,
                inst_family: None,
                inst_id: None,
            }))
            .collect::<Vec<_>>();
        for arg in &arguments {
            let topic = topic_from_subscription_arg(arg);
            desired.mark_subscribe(&topic);
            desired.confirm_subscribe(&topic);
        }
        let expected = canonical_confirmed(&desired);
        let mut topics = desired.all_topics();
        topics.sort();
        ensure!(
            expected == source.confirmed_subscriptions
                && topics
                    == source
                        .subscription_references
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>(),
            "source desired/confirmed/reference subscription identities differ"
        );
        for (channel, values) in types {
            self.subscriptions_inst_type.insert(channel, values);
        }
        for (channel, values) in families {
            self.subscriptions_inst_family.insert(channel, values);
        }
        for (channel, values) in ids {
            self.subscriptions_inst_id.insert(channel, values);
        }
        for (channel, enabled) in bare {
            self.subscriptions_bare.insert(channel, enabled);
        }
        for (topic, count) in source.subscription_references {
            for _ in 0..count {
                self.subscriptions_state.add_reference(&topic);
            }
        }
        self.instruments_cache
            .rcu(|m| *m = source.instruments.clone());
        self.inst_id_code_cache
            .rcu(|m| *m = source.inst_id_codes.clone());
        self.trade_quote_ccy_lists
            .rcu(|m| *m = source.trade_quote_ccy_lists.clone());
        *self.spot_trade_quote_ccy.lock() = source.spot_trade_quote_ccy;
        self.option_greeks_subs
            .rcu(|m| *m = source.option_greeks_subscriptions.clone());
        for (pair, count) in source.index_pair_subscribers {
            self.index_pair_subscribers.insert(pair, count);
        }
        self.request_id_counter
            .store(source.request_id_counter, Ordering::Release);
        let metadata = self.checkpoint_metadata();
        self.checkpoint_recovery = Some(SocketRecoveryPlan {
            raw,
            confirmed: source.confirmed_subscriptions,
            metadata,
            instruments: source.instruments,
        });
        Ok(())
    }
    fn checkpoint_metadata(&self) -> Value {
        serde_json::json!({"inst_id_codes":&**self.inst_id_code_cache.load(),
            "trade_quote_ccy_lists":&**self.trade_quote_ccy_lists.load(),"spot_trade_quote_ccy":*self.spot_trade_quote_ccy.lock()})
    }
    pub(super) fn verify_checkpoint_metadata(&self) -> Result<()> {
        if let Some(recovery) = &self.checkpoint_recovery {
            ensure!(
                self.checkpoint_metadata() == recovery.metadata,
                "fresh instrument definitions differ from restored socket source"
            );
            let current = self.instruments_cache.load();
            ensure!(
                current.len() == recovery.instruments.len()
                    && recovery.instruments.iter().all(|(symbol, source)| current
                        .get(symbol)
                        .is_some_and(|instrument| crate::data::instrument_definitions_match(
                            source, instrument
                        ))),
                "fresh tradable instrument fields differ from restored socket source"
            );
        }
        Ok(())
    }
    pub(super) async fn finish_checkpoint_recovery_handshake(&self) -> Result<()> {
        let Some(recovery) = &self.checkpoint_recovery else {
            return Ok(());
        };
        let args = subscription_args(
            &self.subscriptions_inst_type,
            &self.subscriptions_inst_family,
            &self.subscriptions_inst_id,
            &self.subscriptions_bare,
        );
        for chunk in subscription_arg_batches(&args) {
            self.subscribe(chunk.to_vec()).await?;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                ensure!(
                    self.is_active()
                        && (!self.credential.is_some() || self.auth_tracker.is_authenticated()),
                    "fresh socket authentication or active connection changed"
                );
                if canonical_confirmed(&self.subscriptions_state) == recovery.confirmed
                    && self.subscriptions_state.pending_subscribe().is_empty()
                    && self.subscriptions_state.pending_unsubscribe().is_empty()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            recovery.raw.release(
                self.checkpoint_raw
                    .as_ref()
                    .context("actual raw socket receiver missing")?,
            )
        })
        .await
        .context("fresh socket subscription acknowledgement timed out")??;
        recovery.raw.verify_released()
    }
}
fn canonical_confirmed(state: &SubscriptionState) -> BTreeMap<String, Vec<String>> {
    state
        .confirmed()
        .iter()
        .map(|(key, values)| {
            let mut values = values.iter().map(ToString::to_string).collect::<Vec<_>>();
            values.sort();
            (key.to_string(), values)
        })
        .collect()
}
