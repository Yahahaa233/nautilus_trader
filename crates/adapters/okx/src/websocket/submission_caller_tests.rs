//! Production request builder/command queue and raw response dispatcher. All
//! frames are controlled local protocol inputs; no venue/network permissions.
use super::*;
use crate::operation_ack::OrderOperationAckSource;
use crate::websocket::{
    dispatch::{WsDispatchState, dispatch_ws_message},
    messages::{OKXOrderMsg, OKXWsFrame},
};
use nautilus_common::messages::{DataEvent, ExecutionEvent};
use nautilus_core::UUID4;
use nautilus_live::{ExecutionEventEmitter, execution::context::OrderIdentity};
use nautilus_model::data::{CustomDataTrait, Data};
use nautilus_model::{
    enums::AccountType, events::OrderEventAny, instruments::stubs::crypto_perpetual_ethusdt,
    orders::OrderTestBuilder,
};

struct ActualCaller {
    client: OKXWebSocketClient,
    dispatch: WsDispatchState,
    emitter: ExecutionEventEmitter,
    receiver: tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    data_receiver: tokio::sync::mpsc::UnboundedReceiver<DataEvent>,
    instruments: AtomicMap<Ustr, InstrumentAny>,
    wire: serde_json::Value,
    command_id: UUID4,
    identity: OrderIdentity,
}
impl ActualCaller {
    async fn new() -> anyhow::Result<Self> {
        let client = OKXWebSocketClient::default();
        let (sender, mut commands) = tokio::sync::mpsc::unbounded_channel();
        *client.cmd_tx.write().await = sender;
        let command_id = UUID4::new();
        let trader = TraderId::from("TRADER-001");
        let account = AccountId::from("OKX-001");
        let identity = OrderIdentity {
            client_order_id: "CQS-SUBMIT-ORIGINAL-1".into(),
            strategy_id: "STRATEGY-001".into(),
            instrument_id: "BTC-USDT-SWAP.OKX".into(),
            order_side: OrderSide::Buy,
            order_type: OrderType::Market,
        };
        let order = OrderTestBuilder::new(OrderType::Market)
            .trader_id(trader)
            .strategy_id(identity.strategy_id)
            .instrument_id(identity.instrument_id)
            .client_order_id(identity.client_order_id)
            .quantity("0.03".into())
            .build();
        let command = nautilus_common::messages::execution::SubmitOrder::from_order(
            &order,
            trader,
            None,
            None,
            command_id,
            get_atomic_clock_realtime().get_time_ns(),
        );
        let original = PendingSubmitInfo::new(&command, account, identity)?;
        let mut instrument = crypto_perpetual_ethusdt();
        instrument.id = identity.instrument_id;
        instrument.raw_symbol = "BTC-USDT-SWAP".into();
        instrument.base_currency = nautilus_model::types::Currency::BTC();
        let contract = InstrumentAny::CryptoPerpetual(instrument);
        client.cache_instruments(std::slice::from_ref(&contract));
        client.cache_inst_id_code(Ustr::from("BTC-USDT-SWAP"), 10459);
        client
            .submit_order_with_context(
                Some(original),
                trader,
                identity.strategy_id,
                identity.instrument_id,
                OKXTradeMode::Cross,
                identity.client_order_id,
                identity.order_side,
                identity.order_type,
                Quantity::from("0.03"),
                Some(TimeInForce::Gtc),
                None,
                None,
                Some(false),
                Some(false),
                Some(false),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        let HandlerCommand::Send {
            payload,
            request_id,
            client_order_ids,
            op,
            ..
        } = commands.try_recv()?
        else {
            anyhow::bail!("production caller did not issue a wire Send");
        };
        let wire: serde_json::Value = serde_json::from_str(&payload)?;
        anyhow::ensure!(
            wire["op"] == "order"
                && wire["id"] == command_id.to_string().replace('-', "")
                && request_id.as_deref() == wire["id"].as_str()
                && client_order_ids == vec![identity.client_order_id]
                && op == Some(super::super::enums::OKXWsOperation::Order),
            "actual wire request lost original command identity"
        );
        assert_eq!(
            client
                .pending_orders
                .get(identity.client_order_id.as_str())
                .unwrap()
                .command_id,
            Some(command_id)
        );
        let mut dispatch = WsDispatchState::with_pending_maps(
            client.pending_orders.clone(),
            client.pending_cancels.clone(),
            client.pending_amends.clone(),
        );
        dispatch.retained_submit_contexts = client.retained_submit_contexts.clone();
        dispatch
            .order_identities
            .insert(identity.client_order_id, identity);
        let instruments = AtomicMap::new();
        instruments.insert(Ustr::from("BTC-USDT-SWAP"), contract);
        let (data_sender, data_receiver) = tokio::sync::mpsc::unbounded_channel();
        dispatch.protection_sources.bind(data_sender.into());
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut emitter = ExecutionEventEmitter::new(
            get_atomic_clock_realtime(),
            trader,
            account,
            AccountType::Margin,
            None,
        );
        emitter.set_sender(sender);
        emitter.emit_order_submitted_with_causation(&order, command_id);
        Ok(Self {
            client,
            dispatch,
            emitter,
            receiver,
            data_receiver,
            instruments,
            wire,
            command_id,
            identity,
        })
    }
    fn response(&mut self, request_id: &str, s_code: &str) {
        // Exercise the actual raw frame deserializer, including op/code/data.
        let raw = serde_json::json!({"id":request_id,"op":"order","code":"0","msg":"", "data":[{
            "clOrdId":self.identity.client_order_id.as_str(),"ordId":"2497956918703120384",
            "sCode":s_code,"sMsg":if s_code=="0" {""} else {"invalid order parameters"}}]})
        .to_string();
        let frame: OKXWsFrame = serde_json::from_str(&raw).unwrap();
        let OKXWsFrame::OrderResponse {
            id,
            op,
            code,
            msg,
            data,
        } = frame
        else {
            panic!("raw response did not decode")
        };
        self.dispatch(OKXWsMessage::OrderResponse {
            id,
            op,
            code,
            msg,
            data,
        });
    }
    fn live(&mut self) {
        // A new controlled frame uses the production venue schema. No old
        // recorded financial/original receipt is edited to make a test pass.
        let raw: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/ws_orders.json")).unwrap();
        let mut row = raw["data"][0].clone();
        row["clOrdId"] = self.identity.client_order_id.to_string().into();
        row["state"] = "live".into();
        row["accFillSz"] = "0".into();
        row["fillSz"] = "0".into();
        row["fillPx"] = "".into();
        row["fillTime"] = "".into();
        row["tradeId"] = "".into();
        row["avgPx"] = "".into();
        row["fee"] = "0".into();
        row["feeCcy"] = "USDT".into();
        let message: OKXOrderMsg = serde_json::from_value(row).unwrap();
        self.dispatch(OKXWsMessage::Orders(vec![message]));
    }
    fn dispatch(&mut self, message: OKXWsMessage) {
        dispatch_ws_message(
            message,
            &self.emitter,
            &self.dispatch,
            AccountId::from("OKX-001"),
            &self.instruments,
            &mut AHashMap::new(),
            &mut AHashMap::new(),
            &mut AHashMap::new(),
            get_atomic_clock_realtime(),
        );
    }
    fn raw_ack(&self, request_id: &str, s_code: &str) -> String {
        serde_json::json!({"id":request_id,"op":"order","code":"0","msg":"",
            "inTime":"1700000000000010","outTime":"1700000000000020","data":[{
            "clOrdId":self.identity.client_order_id.as_str(),"ordId":"2497956918703120384",
            "sCode":s_code,"sMsg":if s_code=="0" {""} else {"invalid order parameters"},
            "tag":"original-extra-field"}]})
        .to_string()
    }
    async fn dispatch_raw_ack(&mut self, raw: String) {
        // Real retained raw inbox -> production handler -> original envelope ->
        // actual dispatcher -> original Custom data ingress and native events.
        let message = crate::websocket::handler::read_protection_test_frame(raw.clone()).await;
        assert!(
            matches!(&message, OKXWsMessage::OriginalOrderResponse {payload_json, received_ns, ..}
            if payload_json == &raw && *received_ns != 0)
        );
        self.dispatch(message);
    }
    fn ack_sources(&mut self) -> Vec<OrderOperationAckSource> {
        let mut values = Vec::new();
        while let Ok(event) = self.data_receiver.try_recv() {
            let DataEvent::Data(Data::Custom(source)) = event else {
                panic!("ACK used another ingress family")
            };
            values.push(
                source
                    .data
                    .as_any()
                    .downcast_ref::<OrderOperationAckSource>()
                    .expect("actual ACK source type differs")
                    .clone(),
            );
        }
        values
    }
    fn terminal(&mut self) {
        let raw: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/ws_orders.json")).unwrap();
        let mut row = raw["data"][0].clone();
        row["clOrdId"] = self.identity.client_order_id.to_string().into();
        row["state"] = "canceled".into();
        row["accFillSz"] = "0".into();
        row["fillSz"] = "0".into();
        row["fillPx"] = "".into();
        row["fillTime"] = "".into();
        row["tradeId"] = "".into();
        row["avgPx"] = "".into();
        row["fee"] = "0".into();
        row["feeCcy"] = "USDT".into();
        self.dispatch(OKXWsMessage::Orders(vec![
            serde_json::from_value(row).unwrap(),
        ]));
    }
    fn events(&mut self) -> Vec<OrderEventAny> {
        let mut events = vec![];
        while let Ok(event) = self.receiver.try_recv() {
            if let ExecutionEvent::Order(order) = event {
                events.push(order);
            }
        }
        events
    }
}

#[tokio::test]
async fn actual_submit_wire_and_early_acceptance_keep_original_causation_after_late_ack_and_restore()
-> anyhow::Result<()> {
    let mut original = ActualCaller::new().await?;
    let submitted = original.events();
    assert!(
        matches!(&submitted[..], [OrderEventAny::Submitted(event)] if event.causation_id == Some(original.command_id))
    );
    original.live();
    let events = original.events();
    assert!(
        matches!(&events[..], [OrderEventAny::Accepted(event)] if event.causation_id == Some(original.command_id))
    );
    assert!(
        original
            .client
            .retained_submit_contexts
            .get(original.identity.client_order_id.as_str())
            .unwrap()
            .acknowledged_response
            .is_none()
    );
    let OrderEventAny::Accepted(accepted) = &events[0] else {
        panic!("actual Accepted missing");
    };
    let basis = accepted
        .command_causation_basis
        .expect("original Submit association missing");
    assert_eq!(basis.kind.as_str(), "local_original_submit_identity");
    assert!(!basis.venue_command_causation_available);
    assert!(basis.acknowledgement_payload_json.is_none());
    let before: serde_json::Value =
        serde_json::from_str(basis.original_terminal_payload_json.as_str())?;
    assert!(before["causation_id"].is_null() && before["command_causation_basis"].is_null());
    assert!(
        original.dispatch.checkpoint_inventory().is_err(),
        "an outstanding actual request cannot claim a resolved cut"
    );
    let req = original.wire["id"].as_str().unwrap().to_owned();
    original.response(&req, "0");
    assert!(
        original.events().is_empty(),
        "operation ACK never emits or replaces Accepted"
    );
    let source = original.dispatch.checkpoint_inventory()?;
    let restored = WsDispatchState::default();
    restored.restore_checkpoint(&source)?;
    assert_eq!(restored.checkpoint_inventory()?, source);
    let retained = restored
        .retained_submit_contexts
        .get(original.identity.client_order_id.as_str())
        .unwrap();
    assert_eq!(retained.command_id, original.command_id);
    assert_eq!(retained.acknowledged_response.as_ref().unwrap()["id"], req);
    drop(retained);
    let mut changed = source.clone();
    changed["retained_submit_contexts"][original.identity.client_order_id.as_str()]["command_id"] =
        UUID4::new().to_string().into();
    assert!(
        WsDispatchState::default()
            .restore_checkpoint(&changed)
            .is_err(),
        "new UUID cannot reuse old request proof"
    );
    Ok(())
}

#[tokio::test]
async fn actual_submit_wrong_request_rejection_cannot_consume_original_and_ack_is_not_terminal()
-> anyhow::Result<()> {
    let mut original = ActualCaller::new().await?;
    original.events();
    original.response("not-the-original-request", "51000");
    assert!(original.events().is_empty());
    assert!(
        original
            .client
            .pending_orders
            .contains_key(original.identity.client_order_id.as_str())
    );
    let req = original.wire["id"].as_str().unwrap().to_owned();
    original.response(&req, "0");
    assert!(original.events().is_empty());
    assert!(
        !original
            .client
            .pending_orders
            .contains_key(original.identity.client_order_id.as_str())
    );
    assert!(
        original
            .client
            .retained_submit_contexts
            .contains_key(original.identity.client_order_id.as_str())
    );
    original.live();
    assert!(
        matches!(&original.events()[..], [OrderEventAny::Accepted(event)] if event.causation_id == Some(original.command_id))
    );
    Ok(())
}

#[tokio::test]
async fn actual_submit_matched_rejection_preserves_original_uuid_and_foreign_account_never_bridges()
-> anyhow::Result<()> {
    let mut rejected = ActualCaller::new().await?;
    rejected.events();
    let req = rejected.wire["id"].as_str().unwrap().to_owned();
    rejected.response(&req, "51000");
    let events = rejected.events();
    let [OrderEventAny::Rejected(event)] = &events[..] else {
        anyhow::bail!("actual matched rejection missing");
    };
    assert_eq!(event.causation_id, Some(rejected.command_id));
    let basis = event
        .command_causation_basis
        .expect("actual request proof missing");
    assert_eq!(basis.kind.as_str(), "venue_submit_req_id_rejection");
    assert!(basis.venue_command_causation_available);
    assert_eq!(basis.request_id.unwrap().as_str(), req);
    let acknowledgement: serde_json::Value =
        serde_json::from_str(basis.acknowledgement_payload_json.unwrap().as_str())?;
    assert_eq!(acknowledgement["id"], req);
    assert_eq!(acknowledgement["data"][0]["sCode"], "51000");
    let before: serde_json::Value =
        serde_json::from_str(basis.original_terminal_payload_json.as_str())?;
    assert!(before["causation_id"].is_null() && before["command_causation_basis"].is_null());
    assert!(rejected.client.retained_submit_contexts.is_empty());
    let mut foreign = ActualCaller::new().await?;
    foreign.events();
    foreign
        .client
        .retained_submit_contexts
        .get_mut(foreign.identity.client_order_id.as_str())
        .unwrap()
        .account_id = "OKX-FOREIGN".into();
    let req = foreign.wire["id"].as_str().unwrap().to_owned();
    foreign.response(&req, "0");
    assert!(
        foreign
            .client
            .pending_orders
            .contains_key(foreign.identity.client_order_id.as_str())
    );
    foreign.live();
    assert!(
        matches!(&foreign.events()[..], [OrderEventAny::Accepted(event)] if event.causation_id.is_none())
    );
    Ok(())
}

#[tokio::test]
async fn actual_raw_ack_before_accepted_is_independent_and_preserves_wire_and_receive_time()
-> anyhow::Result<()> {
    let mut caller = ActualCaller::new().await?;
    caller.events();
    let req = caller.wire["id"].as_str().unwrap().to_owned();
    let raw = caller.raw_ack(&req, "0");
    caller.dispatch_raw_ack(raw.clone()).await;
    assert!(
        caller.events().is_empty(),
        "operation success is not Accepted"
    );
    let sources = caller.ack_sources();
    let [source] = &sources[..] else {
        anyhow::bail!("exactly one raw ACK observation required")
    };
    source.validate()?;
    assert_eq!(source.websocket_payload_json, raw);
    assert!(
        source.received_ns != 0
            && source.ts_event.as_u64() == source.received_ns
            && source.ts_init >= source.ts_event
    );
    let binding = source.matched_submit.as_ref().unwrap();
    assert_eq!(binding.command_id, caller.command_id);
    assert_eq!(binding.identity, caller.identity);
    assert_eq!(binding.request_id, req);
    assert_eq!(binding.matched_item_index, Some(0));
    assert_eq!(
        binding.matched_item.as_ref().unwrap()["tag"],
        "original-extra-field"
    );
    let restored = OrderOperationAckSource::from_json(serde_json::from_str(&source.to_json()?)?)?;
    assert!(source.eq_arc(restored.as_ref()));
    caller.live();
    let events = caller.events();
    let [OrderEventAny::Accepted(accepted)] = &events[..] else {
        anyhow::bail!("actual live native Accepted missing")
    };
    assert_eq!(accepted.causation_id, Some(caller.command_id));
    assert!(
        accepted
            .command_causation_basis
            .unwrap()
            .acknowledgement_payload_json
            .is_some()
    );
    Ok(())
}

#[tokio::test]
async fn actual_raw_ack_after_accepted_never_backfills_original_event_and_exports_source()
-> anyhow::Result<()> {
    let mut caller = ActualCaller::new().await?;
    let submitted = caller.events();
    caller.live();
    let accepted = caller.events();
    let [OrderEventAny::Accepted(event)] = &accepted[..] else {
        anyhow::bail!("original actual Accepted missing")
    };
    assert!(
        event
            .command_causation_basis
            .unwrap()
            .acknowledgement_payload_json
            .is_none()
    );
    let original_event_bytes = serde_json::to_vec(&accepted)?;
    assert!(caller.dispatch.checkpoint_inventory().is_err());
    let req = caller.wire["id"].as_str().unwrap().to_owned();
    let raw = caller.raw_ack(&req, "0");
    caller.dispatch_raw_ack(raw.clone()).await;
    assert!(caller.events().is_empty());
    assert_eq!(serde_json::to_vec(&accepted)?, original_event_bytes);
    let sources = caller.ack_sources();
    let [source] = &sources[..] else {
        anyhow::bail!("late original ACK source missing")
    };
    source.validate()?;
    assert_eq!(
        source.matched_submit.as_ref().unwrap().command_id,
        caller.command_id
    );
    assert_eq!(source.websocket_payload_json, raw);
    let inventory = caller.dispatch.checkpoint_inventory()?;
    let restored = WsDispatchState::default();
    restored.restore_checkpoint(&inventory)?;
    assert_eq!(restored.checkpoint_inventory()?, inventory);
    if let Some(path) = std::env::var_os("METIS_TEST_OKX_ACK_EXPORT_PATH") {
        let path = std::path::PathBuf::from(path);
        anyhow::ensure!(
            path.is_absolute(),
            "absolute fresh ACK evidence export required"
        );
        std::fs::create_dir(&path)?;
        let value = serde_json::json!({"schema":"OKXLateOperationAckCallerEvidence.v1",
            "trader_id":caller.emitter.trader_id(),"adapter_account_id":"OKX-001",
            "submitted_events":submitted,"accepted_before_ack_events":accepted,
            "operation_ack_sources":sources,"original_wire_request":caller.wire,
            "checkpoint_inventory":inventory,"execution_authorized":false,
            "actual_raw_handler":true,"actual_native_dispatcher":true,"venue_io":false});
        std::fs::write(
            path.join("evidence.json"),
            serde_json::to_vec_pretty(&value)?,
        )?;
    }
    Ok(())
}

#[tokio::test]
async fn actual_raw_ack_duplicates_and_foreign_request_retain_observations_without_duplicate_order_effect()
-> anyhow::Result<()> {
    let mut caller = ActualCaller::new().await?;
    caller.events();
    caller
        .dispatch_raw_ack(caller.raw_ack("foreign-request", "0"))
        .await;
    let foreign = caller.ack_sources();
    assert!(foreign[0].matched_submit.is_none());
    assert!(
        caller
            .client
            .pending_orders
            .contains_key(caller.identity.client_order_id.as_str())
    );
    assert!(caller.events().is_empty());
    let req = caller.wire["id"].as_str().unwrap().to_owned();
    let raw = caller.raw_ack(&req, "0");
    caller.dispatch_raw_ack(raw.clone()).await;
    let first = caller
        .client
        .retained_submit_contexts
        .get(caller.identity.client_order_id.as_str())
        .unwrap()
        .acknowledged_response
        .clone();
    caller.dispatch_raw_ack(raw.clone()).await;
    let sources = caller.ack_sources();
    assert_eq!(sources.len(), 2);
    assert!(
        sources
            .iter()
            .all(|value| value.websocket_payload_json == raw
                && value.matched_submit.as_ref().unwrap().command_id == caller.command_id)
    );
    assert!(sources[1].received_ns >= sources[0].received_ns);
    assert_eq!(
        caller
            .client
            .retained_submit_contexts
            .get(caller.identity.client_order_id.as_str())
            .unwrap()
            .acknowledged_response,
        first
    );
    assert!(caller.events().is_empty());
    assert!(caller.client.pending_orders.is_empty());
    caller.live();
    assert!(matches!(&caller.events()[..], [OrderEventAny::Accepted(_)]));
    caller.live();
    assert!(caller.events().is_empty());
    Ok(())
}

#[tokio::test]
async fn actual_raw_ack_rejection_and_foreign_account_keep_exact_original_sources()
-> anyhow::Result<()> {
    let mut rejected = ActualCaller::new().await?;
    rejected.events();
    let req = rejected.wire["id"].as_str().unwrap().to_owned();
    rejected
        .dispatch_raw_ack(rejected.raw_ack(&req, "51000"))
        .await;
    let sources = rejected.ack_sources();
    sources[0].validate()?;
    assert_eq!(
        sources[0].matched_submit.as_ref().unwrap().command_id,
        rejected.command_id
    );
    assert_eq!(
        sources[0]
            .matched_submit
            .as_ref()
            .unwrap()
            .matched_item
            .as_ref()
            .unwrap()["sCode"],
        "51000"
    );
    assert!(
        matches!(&rejected.events()[..], [OrderEventAny::Rejected(value)] if value.causation_id == Some(rejected.command_id))
    );
    let mut foreign = ActualCaller::new().await?;
    foreign.events();
    foreign
        .client
        .retained_submit_contexts
        .get_mut(foreign.identity.client_order_id.as_str())
        .unwrap()
        .account_id = "OKX-FOREIGN".into();
    let req = foreign.wire["id"].as_str().unwrap().to_owned();
    foreign.dispatch_raw_ack(foreign.raw_ack(&req, "0")).await;
    assert!(foreign.ack_sources()[0].matched_submit.is_none());
    assert!(
        foreign
            .client
            .pending_orders
            .contains_key(foreign.identity.client_order_id.as_str())
    );
    assert!(foreign.events().is_empty());
    let mut empty = ActualCaller::new().await?;
    empty.events();
    let raw = serde_json::json!({"id":empty.wire["id"],"op":"order","code":"50000","msg":"actual top failure","data":[]}).to_string();
    empty.dispatch_raw_ack(raw.clone()).await;
    let sources = empty.ack_sources();
    sources[0].validate()?;
    assert_eq!(sources[0].websocket_payload_json, raw);
    let binding = sources[0].matched_submit.as_ref().unwrap();
    assert!(binding.matched_item.is_none() && binding.matched_item_index.is_none());
    assert!(
        empty.events().is_empty(),
        "item-free ACK cannot invent an order rejection"
    );
    assert!(empty.dispatch.checkpoint_inventory().is_err());
    Ok(())
}

#[tokio::test]
async fn actual_terminal_before_raw_ack_keeps_unresolved_request_and_retires_only_exact_ack()
-> anyhow::Result<()> {
    let mut caller = ActualCaller::new().await?;
    caller.events();
    caller.live();
    caller.events();
    caller.terminal();
    assert!(matches!(&caller.events()[..], [OrderEventAny::Canceled(_)]));
    assert!(
        caller
            .dispatch
            .contains_terminal(&caller.identity.client_order_id)
    );
    assert!(
        caller
            .client
            .pending_orders
            .contains_key(caller.identity.client_order_id.as_str())
    );
    assert!(
        caller
            .client
            .retained_submit_contexts
            .contains_key(caller.identity.client_order_id.as_str())
    );
    assert!(
        caller.dispatch.checkpoint_inventory().is_err(),
        "unresolved terminal request must not make a resolved cut"
    );
    let req = caller.wire["id"].as_str().unwrap().to_owned();
    caller.dispatch_raw_ack(caller.raw_ack(&req, "0")).await;
    let sources = caller.ack_sources();
    sources[0].validate()?;
    assert_eq!(
        sources[0].matched_submit.as_ref().unwrap().command_id,
        caller.command_id
    );
    assert!(caller.events().is_empty());
    assert!(caller.client.pending_orders.is_empty());
    assert!(caller.client.retained_submit_contexts.is_empty());
    let inventory = caller.dispatch.checkpoint_inventory()?;
    let restored = WsDispatchState::default();
    restored.restore_checkpoint(&inventory)?;
    assert_eq!(restored.checkpoint_inventory()?, inventory);
    Ok(())
}

#[tokio::test]
async fn actual_raw_ack_capture_failure_latches_original_gate_and_cannot_resolve_request()
-> anyhow::Result<()> {
    let mut caller = ActualCaller::new().await?;
    caller.events();
    let gate = nautilus_common::live::ingress::IngressGate::new();
    let (sender, receiver) = gate.channel::<DataEvent>();
    drop(receiver);
    caller.dispatch.protection_sources.bind(sender.clone());
    let req = caller.wire["id"].as_str().unwrap().to_owned();
    caller.dispatch_raw_ack(caller.raw_ack(&req, "0")).await;
    assert!(caller.events().is_empty());
    assert!(caller.dispatch.protection_sources.verify().is_err());
    let first = gate.verify().unwrap_err().to_string();
    assert!(first.contains("original operation ACK source persistence failed"));
    sender.fail("later secondary failure");
    assert_eq!(gate.verify().unwrap_err().to_string(), first);
    assert!(gate.freeze().is_err());
    assert!(caller.dispatch.checkpoint_inventory().is_err());
    assert!(
        caller
            .client
            .pending_orders
            .contains_key(caller.identity.client_order_id.as_str())
    );
    caller.live();
    assert!(
        caller.events().is_empty(),
        "failed source must not admit another business dispatch"
    );
    Ok(())
}
