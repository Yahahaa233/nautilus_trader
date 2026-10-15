//! Real command builder -> handler command -> original ACK -> native terminal event.
use super::*;
use crate::{
    common::enums::OKXOrderStatus,
    websocket::{
        dispatch::{WsDispatchState, dispatch_ws_message},
        messages::{OKXOrderMsg, OKXWsFrame},
        parse::OrderStateSnapshot,
    },
};
use ahash::AHashMap;
use nautilus_common::messages::{ExecutionEvent, execution::CancelOrder};
use nautilus_core::UUID4;
use nautilus_live::{
    ExecutionEventEmitter,
    execution::context::{OrderContext, OrderIdentity},
};
use nautilus_model::{
    enums::{AccountType, OrderSide, OrderType, TimeInForce},
    events::{OrderCanceled, OrderEventAny},
    identifiers::Symbol,
    instruments::CryptoPerpetual,
    types::{Currency, Price, Quantity},
};

fn command() -> CancelOrder {
    CancelOrder::new(
        TraderId::from("TRADER-001"),
        None,
        StrategyId::from("STRATEGY-001"),
        InstrumentId::from("BTC-USDT-SWAP.OKX"),
        ClientOrderId::from("CANCEL-ORIGINAL-001"),
        Some(VenueOrderId::from("2497956918703120384")),
        UUID4::new(),
        get_atomic_clock_realtime().get_time_ns(),
        None,
        None,
    )
}
fn instrument() -> InstrumentAny {
    CryptoPerpetual::builder()
        .instrument_id(InstrumentId::from("BTC-USDT-SWAP.OKX"))
        .raw_symbol(Symbol::from("BTC-USDT-SWAP"))
        .base_currency(Currency::BTC())
        .quote_currency(Currency::USDT())
        .settlement_currency(Currency::USDT())
        .is_inverse(false)
        .price_precision(2)
        .size_precision(8)
        .price_increment(Price::from("0.01"))
        .size_increment(Quantity::from("0.00000001"))
        .ts_event(UnixNanos::default())
        .ts_init(UnixNanos::default())
        .build()
        .unwrap()
        .into()
}
fn state(socket: &OKXWebSocketClient, cmd: &CancelOrder) -> WsDispatchState {
    let state = WsDispatchState::with_pending_maps(
        socket.pending_orders.clone(),
        socket.pending_cancels.clone(),
        socket.pending_amends.clone(),
    )
    .with_retained_cancel_contexts(socket.retained_cancel_contexts.clone());
    let identity = OrderIdentity {
        client_order_id: cmd.client_order_id,
        strategy_id: cmd.strategy_id,
        instrument_id: cmd.instrument_id,
        order_side: OrderSide::Buy,
        order_type: OrderType::Market,
    };
    state.order_identities.insert(cmd.client_order_id, identity);
    state.track_order_context(OrderContext {
        identity,
        quantity: Quantity::from("0.03"),
        price: None,
        trigger_price: None,
        trigger_type: None,
        time_in_force: TimeInForce::Gtc,
        is_post_only: false,
        is_reduce_only: false,
        is_quote_quantity: false,
    });
    state
}
async fn original_request(socket: &OKXWebSocketClient, cmd: &CancelOrder) -> String {
    socket.cache_inst_id_code(Ustr::from("BTC-USDT-SWAP"), 10459);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    *socket.cmd_tx.write().await = tx;
    socket
        .cancel_order_with_command(cmd, AccountId::from("OKX-001"))
        .await
        .unwrap();
    let HandlerCommand::Send {
        payload,
        request_id,
        client_order_ids,
        op,
        ..
    } = rx.try_recv().unwrap()
    else {
        panic!("cancel did not use original handler channel")
    };
    let request_id = request_id.unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["id"], request_id);
    assert_eq!(payload["op"], "cancel-order");
    assert_eq!(
        payload["args"][0]["clOrdId"],
        cmd.client_order_id.to_string()
    );
    assert_eq!(
        payload["args"][0]["ordId"],
        cmd.venue_order_id.unwrap().to_string()
    );
    assert_eq!(client_order_ids, vec![cmd.client_order_id]);
    assert_eq!(op, Some(super::super::enums::OKXWsOperation::CancelOrder));
    assert_eq!(request_id, cmd.command_id.to_string().replace('-', ""));
    request_id
}
fn make_emitter() -> (
    ExecutionEventEmitter,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
) {
    let mut emitter = ExecutionEventEmitter::new(
        get_atomic_clock_realtime(),
        TraderId::from("TRADER-001"),
        AccountId::from("OKX-001"),
        AccountType::Margin,
        None,
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    emitter.set_sender(tx);
    (emitter, rx)
}
fn dispatch(message: OKXWsMessage, emitter: &ExecutionEventEmitter, state: &WsDispatchState) {
    let instruments = nautilus_core::AtomicMap::new();
    instruments.insert(Ustr::from("BTC-USDT-SWAP"), instrument());
    dispatch_ws_message(
        message,
        emitter,
        state,
        AccountId::from("OKX-001"),
        AccountType::Margin,
        &instruments,
        &mut crate::websocket::parse::FeeCache::new(),
        &mut crate::websocket::parse::FilledQtyCache::new(),
        &mut AHashMap::<ClientOrderId, OrderStateSnapshot>::new(),
        get_atomic_clock_realtime(),
    );
}
fn ack(cmd: &CancelOrder, id: &str, code: &str) -> OKXWsMessage {
    let raw = serde_json::json!({"id":id,"op":"cancel-order","code":"0","msg":"", "data":[
        {"clOrdId":cmd.client_order_id,"ordId":cmd.venue_order_id,"sCode":code,"sMsg":if code=="0" {""} else {"Order does not exist"}}
    ]}).to_string();
    let frame: OKXWsFrame = serde_json::from_str(&raw).unwrap();
    let OKXWsFrame::OrderResponse {
        id,
        op,
        code,
        msg,
        data,
        ..
    } = frame
    else {
        panic!("original ACK parser did not route order response")
    };
    OKXWsMessage::OrderResponse {
        id,
        op,
        code,
        msg,
        data,
    }
}

fn terminal(cmd: &CancelOrder) -> OKXWsMessage {
    let frame: OKXWsFrame =
        serde_json::from_str(include_str!("../../test_data/ws_orders.json")).unwrap();
    let OKXWsFrame::Data { data, .. } = frame else {
        panic!("order fixture is not original data")
    };
    let mut messages: Vec<OKXOrderMsg> = serde_json::from_value(data).unwrap();
    let message = &mut messages[0];
    message.cl_ord_id = cmd.client_order_id.to_string();
    message.ord_id = Ustr::from(cmd.venue_order_id.unwrap().as_str());
    message.state = OKXOrderStatus::Canceled;
    message.acc_fill_sz = Some("0".into());
    message.fill_sz.clear();
    message.fill_px.clear();
    message.trade_id.clear();
    message.fee = Some("0".into());
    OKXWsMessage::Orders(messages)
}
fn canceled(rx: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>) -> OrderCanceled {
    let mut result = None;
    while let Ok(event) = rx.try_recv() {
        if let ExecutionEvent::Order(OrderEventAny::Canceled(event)) = event {
            assert!(result.replace(event).is_none());
        }
    }
    result.expect("actual parsed Canceled event absent")
}

#[tokio::test]
async fn actual_cancel_command_ack_terminal_preserves_local_basis_and_restores_pending_context() {
    let socket = OKXWebSocketClient::default();
    let cmd = command();
    let id = original_request(&socket, &cmd).await;
    let source = state(&socket, &cmd);
    let (emitter, mut rx) = make_emitter();
    assert!(
        source.checkpoint_inventory().is_err(),
        "unACKed cancel became a complete source"
    );
    dispatch(ack(&cmd, &id, "0"), &emitter, &source);
    assert!(
        rx.try_recv().is_err(),
        "ACK was emitted as an order terminal"
    );
    let cut = source.checkpoint_inventory().unwrap();
    let target = WsDispatchState::default();
    target.restore_checkpoint(&cut).unwrap();
    assert_eq!(target.checkpoint_inventory().unwrap(), cut);
    dispatch(terminal(&cmd), &emitter, &target);
    let event = canceled(&mut rx);
    let basis = event.command_causation_basis.unwrap();
    assert_eq!(event.causation_id, Some(cmd.command_id));
    assert_eq!(basis.command_id, cmd.command_id);
    assert_eq!(
        basis.kind,
        Ustr::from("local_unique_cancel_ack_terminal_identity")
    );
    assert!(!basis.venue_command_causation_available);
    assert_eq!(
        serde_json::from_str::<CancelOrder>(basis.command_payload_json.as_str()).unwrap(),
        cmd
    );
    let ack_json: serde_json::Value =
        serde_json::from_str(basis.acknowledgement_payload_json.unwrap().as_str()).unwrap();
    assert_eq!(ack_json["request_id"], id);
    assert_eq!(
        ack_json["matched_item"]["clOrdId"],
        cmd.client_order_id.to_string()
    );
    let original: OrderCanceled =
        serde_json::from_str(basis.original_terminal_payload_json.as_str()).unwrap();
    assert!(original.causation_id.is_none() && original.command_causation_basis.is_none());
    assert_eq!(original.client_order_id, event.client_order_id);
    assert_eq!(original.event_id, event.event_id);
    assert_eq!(original.venue_order_id, event.venue_order_id);
    assert_eq!(original.ts_event, event.ts_event);
    assert!(target.retained_cancel_contexts.is_empty());
    let mut forged = cut;
    forged["retained_cancel_contexts"][cmd.client_order_id.to_string()]["command_id"] =
        serde_json::json!(UUID4::new());
    assert!(
        WsDispatchState::default()
            .restore_checkpoint(&forged)
            .is_err()
    );
}
#[tokio::test]
async fn actual_cancel_early_terminal_wrong_ack_and_duplicate_command_never_gain_causation() {
    for wrong_ack in [false, true] {
        let socket = OKXWebSocketClient::default();
        let cmd = command();
        let _id = original_request(&socket, &cmd).await;
        let source = state(&socket, &cmd);
        let (emitter, mut rx) = make_emitter();
        let before = socket
            .retained_cancel_contexts
            .get(cmd.client_order_id.as_str())
            .unwrap()
            .clone();
        let mut duplicate = cmd.clone();
        duplicate.command_id = UUID4::new();
        assert!(
            socket
                .cancel_order_with_command(&duplicate, AccountId::from("OKX-001"))
                .await
                .is_err()
        );
        assert_eq!(
            *socket
                .retained_cancel_contexts
                .get(cmd.client_order_id.as_str())
                .unwrap(),
            before
        );
        if wrong_ack {
            dispatch(ack(&cmd, "wrong-original-request", "0"), &emitter, &source);
            assert!(source.checkpoint_inventory().is_err());
        }
        dispatch(terminal(&cmd), &emitter, &source);
        let event = canceled(&mut rx);
        assert!(event.causation_id.is_none() && event.command_causation_basis.is_none());
    }
}
#[tokio::test]
async fn actual_cancel_matched_rejection_retains_command_and_conflicting_ack_stays_uncertain() {
    let socket = OKXWebSocketClient::default();
    let cmd = command();
    let id = original_request(&socket, &cmd).await;
    let source = state(&socket, &cmd);
    let (emitter, mut rx) = make_emitter();
    dispatch(ack(&cmd, &id, "51400"), &emitter, &source);
    let ExecutionEvent::Order(OrderEventAny::CancelRejected(event)) = rx.try_recv().unwrap() else {
        panic!("actual matched cancel rejection absent")
    };
    assert_eq!(event.causation_id, Some(cmd.command_id));
    assert_eq!(event.client_order_id, cmd.client_order_id);
    let basis = event.command_causation_basis.unwrap();
    assert_eq!(basis.kind, Ustr::from("venue_cancel_req_id_rejection"));
    assert!(basis.venue_command_causation_available);
    let original: nautilus_model::events::OrderCancelRejected =
        serde_json::from_str(basis.original_terminal_payload_json.as_str()).unwrap();
    assert!(original.causation_id.is_none() && original.command_causation_basis.is_none());
    assert_eq!(original.event_id, event.event_id);
    let ack_json: serde_json::Value =
        serde_json::from_str(basis.acknowledgement_payload_json.unwrap().as_str()).unwrap();
    assert_eq!(ack_json["matched_item"]["sCode"], "51400");
    assert_eq!(
        basis.request_id.unwrap().as_str(),
        cmd.command_id.to_string().replace('-', "")
    );
    assert!(source.retained_cancel_contexts.is_empty());
    let socket = OKXWebSocketClient::default();
    let cmd = command();
    let id = original_request(&socket, &cmd).await;
    let source = state(&socket, &cmd);
    let (emitter, mut rx) = make_emitter();
    dispatch(ack(&cmd, &id, "0"), &emitter, &source);
    let original = source
        .retained_cancel_contexts
        .get(cmd.client_order_id.as_str())
        .unwrap()
        .acknowledgement
        .clone();
    let mut changed = ack(&cmd, &id, "0");
    if let OKXWsMessage::OrderResponse { msg, .. } = &mut changed {
        *msg = "conflicting response".into();
    }
    dispatch(changed, &emitter, &source);
    assert!(source.checkpoint_inventory().is_err());
    assert_eq!(
        source
            .retained_cancel_contexts
            .get(cmd.client_order_id.as_str())
            .unwrap()
            .acknowledgement,
        original
    );
    dispatch(terminal(&cmd), &emitter, &source);
    let event = canceled(&mut rx);
    assert!(event.causation_id.is_none() && event.command_causation_basis.is_none());
}
