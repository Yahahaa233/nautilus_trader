use super::*;
use nautilus_common::messages::ExecutionEvent;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    enums::{AccountType, OrderSide, OrderType},
    instruments::stubs::crypto_perpetual_ethusdt,
};

#[test]
fn amendment_acknowledgement_and_order_result_are_separate() {
    for (result, filled) in [
        ("0", false),
        ("0", true),
        ("-1", false),
        ("1", false),
        ("request_rejected", false),
    ] {
        let command = UUID4::new();
        let req_id = command.to_string().replace('-', "");
        let client_id = ClientOrderId::from("CQS-AMEND-1");
        let account = AccountId::from("OKX-001");
        let trader = TraderId::from("TRADER-001");
        let strategy = StrategyId::from("STRATEGY-001");
        let instrument_id = InstrumentId::from("ETH-USDT-SWAP.OKX");
        let state = WsDispatchState::default();
        state.order_identities.insert(
            client_id,
            OrderIdentity {
                client_order_id: client_id,
                instrument_id,
                strategy_id: strategy,
                order_side: OrderSide::Buy,
                order_type: OrderType::Limit,
            },
        );
        state.insert_accepted(client_id, "123".into());
        state.pending_amends.insert(
            client_id.to_string(),
            PendingOrderInfo {
                trader_id: trader,
                strategy_id: strategy,
                instrument_id,
                command_id: Some(command),
            },
        );
        let clock = get_atomic_clock_realtime();
        let mut emitter =
            ExecutionEventEmitter::new(clock, trader, account, AccountType::Margin, None);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let instruments = AtomicMap::new();
        let mut instrument = crypto_perpetual_ethusdt();
        instrument.id = instrument_id;
        instrument.raw_symbol = "ETH-USDT-SWAP".into();
        instruments.insert(
            Ustr::from("ETH-USDT-SWAP"),
            InstrumentAny::CryptoPerpetual(instrument),
        );
        let mut fees = AHashMap::new();
        let mut fills = AHashMap::new();
        let mut orders = AHashMap::new();
        let mut dispatch = |message| {
            dispatch_ws_message(
                message,
                &emitter,
                &state,
                account,
                &instruments,
                &mut fees,
                &mut fills,
                &mut orders,
                clock,
            )
        };
        // 请求接收成功和旧请求失败均不能完成当前修改。
        for (id, code) in [(req_id.clone(), "0"), ("old-request".into(), "51000")] {
            dispatch(OKXWsMessage::OrderResponse {
                id: Some(id),
                op: OKXWsOperation::AmendOrder,
                code: "0".into(),
                msg: "".into(),
                data: vec![
                    serde_json::json!({"sCode":code,"sMsg":"","clOrdId":client_id.as_str(),"ordId":"123"}),
                ],
            });
            assert!(receiver.try_recv().is_err());
            assert!(state.pending_amends.contains_key(client_id.as_str()));
        }
        dispatch(OKXWsMessage::SendFailed {
            request_id: req_id.clone(),
            client_order_ids: vec![client_id],
            op: Some(OKXWsOperation::AmendOrder),
            error: crate::websocket::error::OKXWsError::SendFailed("测试断连".into()),
        });
        assert!(receiver.try_recv().is_err());
        assert!(state.pending_amends.contains_key(client_id.as_str()));
        if result == "request_rejected" {
            dispatch(OKXWsMessage::OrderResponse {
                id: Some(req_id),
                op: OKXWsOperation::AmendOrder,
                code: "1".into(),
                msg: "".into(),
                data: vec![
                    serde_json::json!({"sCode":"51000","sMsg":"rejected","clOrdId":client_id.as_str(),"ordId":"123"}),
                ],
            });
            assert!(
                matches!(receiver.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::ModifyRejected(e)) if e.causation_id == Some(command))
            );
            assert!(!state.pending_amends.contains_key(client_id.as_str()));
            continue;
        }
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/ws_orders.json")).unwrap();
        let mut msg: OKXOrderMsg = serde_json::from_value(fixture["data"][0].clone()).unwrap();
        msg.cl_ord_id = client_id.as_str().into();
        msg.inst_id = "ETH-USDT-SWAP".into();
        msg.ord_id = "123".into();
        msg.req_id = Some(req_id);
        msg.amend_result = Some(result.into());
        msg.sz = "1.000".into();
        msg.px = "100.00".into();
        msg.ord_type = OKXOrderType::Limit;
        msg.state = if filled {
            OKXOrderStatus::PartiallyFilled
        } else if result == "1" {
            OKXOrderStatus::Canceled
        } else {
            OKXOrderStatus::Live
        };
        msg.acc_fill_sz = Some(if filled { "0.250" } else { "0" }.into());
        msg.fill_sz = if filled { "0.250" } else { "0" }.into();
        msg.trade_id = if filled { "trade-1" } else { "" }.into();
        dispatch(OKXWsMessage::Orders(vec![msg.clone()]));
        let mut events = Vec::new();
        while let Ok(event) = receiver.try_recv() {
            events.push(event);
        }
        let outcomes: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                ExecutionEvent::Order(OrderEventAny::Updated(e)) => Some(e.causation_id),
                ExecutionEvent::Order(OrderEventAny::ModifyRejected(e)) => Some(e.causation_id),
                _ => None,
            })
            .collect();
        assert_eq!(
            outcomes,
            vec![Some(command)],
            "{result} {filled}: {events:?}"
        );
        assert!(!state.pending_amends.contains_key(client_id.as_str()));
        if filled {
            assert!(events.len() >= 2, "成交不能被改单确认丢弃：{events:?}");
        }
        if result == "1" {
            assert!(
                events.iter().any(|event| matches!(
                    event,
                    ExecutionEvent::Order(OrderEventAny::Canceled(_))
                ))
            );
        }
        dispatch(OKXWsMessage::Orders(vec![msg]));
        while let Ok(event) = receiver.try_recv() {
            assert!(!matches!(
                event,
                ExecutionEvent::Order(OrderEventAny::Updated(_) | OrderEventAny::ModifyRejected(_))
            ));
        }
    }
}

#[test]
fn algo_amendment_correlates_result_and_preserves_pending_on_unknown() {
    for result in ["0", "-1", "", "unknown"] {
        let command = UUID4::new();
        let client_id = ClientOrderId::from("CQS-ALGO-1");
        let account = AccountId::from("OKX-001");
        let trader = TraderId::from("TRADER-001");
        let strategy = StrategyId::from("STRATEGY-001");
        let instrument_id = InstrumentId::from("ETH-USDT-SWAP.OKX");
        let state = WsDispatchState::default();
        state.pending_amends.insert(
            client_id.to_string(),
            PendingOrderInfo {
                trader_id: trader,
                strategy_id: strategy,
                instrument_id,
                command_id: Some(command),
            },
        );
        let clock = get_atomic_clock_realtime();
        let mut emitter =
            ExecutionEventEmitter::new(clock, trader, account, AccountType::Margin, None);
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        emitter.set_sender(sender);
        let instruments = AtomicMap::new();
        let mut instrument = crypto_perpetual_ethusdt();
        instrument.id = instrument_id;
        instrument.raw_symbol = "ETH-USDT-SWAP".into();
        instruments.insert(
            Ustr::from("ETH-USDT-SWAP"),
            InstrumentAny::CryptoPerpetual(instrument),
        );
        let mut fees = AHashMap::new();
        let mut fills = AHashMap::new();
        let mut orders = AHashMap::new();
        let mut dispatch = |message| {
            dispatch_ws_message(
                message,
                &emitter,
                &state,
                account,
                &instruments,
                &mut fees,
                &mut fills,
                &mut orders,
                clock,
            )
        };
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../../test_data/ws_orders_algo.json")).unwrap();
        let mut raw = fixture["data"][0].clone();
        raw["algoClOrdId"] = client_id.as_str().into();
        raw["instId"] = "ETH-USDT-SWAP".into();
        raw["reqId"] = "old-request".into();
        raw["amendResult"] = result.into();
        let decode =
            |raw| serde_json::from_value::<super::super::messages::OKXAlgoOrderMsg>(raw).unwrap();
        dispatch(OKXWsMessage::AlgoOrders(vec![decode(raw.clone())]));
        assert!(receiver.try_recv().is_err());
        assert!(state.pending_amends.contains_key(client_id.as_str()));
        raw["reqId"] = command.to_string().replace('-', "").into();
        dispatch(OKXWsMessage::AlgoOrders(vec![decode(raw.clone())]));
        match result {
            "0" => {
                let ExecutionEvent::Order(OrderEventAny::Updated(update)) =
                    receiver.try_recv().unwrap()
                else {
                    panic!("应为改单确认")
                };
                assert_eq!(update.causation_id, Some(command));
                assert_eq!(update.trigger_price.unwrap().as_f64(), 95000.0);
                assert_eq!(update.quantity.as_f64(), 0.01);
            }
            "-1" => assert!(
                matches!(receiver.try_recv().unwrap(), ExecutionEvent::Order(OrderEventAny::ModifyRejected(e)) if e.causation_id == Some(command))
            ),
            _ => assert!(receiver.try_recv().is_err()),
        }
        assert_eq!(
            state.pending_amends.contains_key(client_id.as_str()),
            !matches!(result, "0" | "-1")
        );
        dispatch(OKXWsMessage::AlgoOrders(vec![decode(raw)]));
        assert!(
            receiver.try_recv().is_err(),
            "重复通知不得产生第二次修改或无归因状态报告"
        );
    }
}
