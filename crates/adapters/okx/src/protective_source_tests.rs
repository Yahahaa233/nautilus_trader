//! Production HTTP submit + raw-inbox handler + order dispatcher. Localhost only.
use super::*;
use crate::{
    common::consts::OKX_CLIENT_ID,
    protection::OrderProtectionSource,
    websocket::{dispatch::dispatch_ws_message, handler::read_protection_test_frame},
};
use axum::{Router, body::Bytes, routing::post};
use nautilus_common::{
    cache::Cache,
    messages::{DataEvent, ExecutionEvent},
};
use nautilus_core::{UUID4, collections::AtomicMap};
use nautilus_model::{
    data::{CustomDataTrait, Data},
    enums::{OrderSide, TriggerType},
    instruments::stubs::crypto_perpetual_ethusdt,
    orders::OrderTestBuilder,
    types::{Currency, Price},
};
use serde_json::{Value, json};
use std::{cell::RefCell, rc::Rc, sync::Arc, time::Duration};
use ustr::Ustr;

fn take_sources(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<DataEvent>) -> Vec<String> {
    let mut result = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        if let DataEvent::Data(Data::Custom(source)) = event {
            result.push(
                source
                    .data
                    .as_any()
                    .downcast_ref::<OrderProtectionSource>()
                    .expect("wrong actual data source")
                    .to_json()
                    .unwrap(),
            );
        }
    }
    result
}
fn take_events(receiver: &mut tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>) -> Vec<Value> {
    let mut result = Vec::new();
    while let Ok(event) = receiver.try_recv() {
        if let ExecutionEvent::Order(event) = event {
            result.push(serde_json::to_value(event).unwrap());
        }
    }
    result
}
async fn dispatch(
    client: &OKXExecutionClient,
    wire: String,
    instruments: &AtomicMap<Ustr, InstrumentAny>,
) {
    let message = read_protection_test_frame(wire.clone()).await;
    assert!(
        matches!(&message,crate::websocket::messages::OKXWsMessage::OriginalOrderData {payload_json,..} if payload_json==&wire)
    );
    dispatch_ws_message(
        message,
        &client.emitter,
        &client.ws_dispatch_state,
        client.core.account_id,
        instruments,
        &mut AHashMap::new(),
        &mut AHashMap::new(),
        &mut AHashMap::new(),
        client.clock,
    );
}

#[tokio::test]
async fn actual_http_protection_submit_raw_trigger_and_child_fills_keep_original_sources()
-> anyhow::Result<()> {
    let requests = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let captured = requests.clone();
    let response = "{\n \"code\":\"0\",\"msg\":\"\",\"unknownOriginal\":17,\"data\":[{\"algoId\":\"PROTECTION-PARENT\",\"algoClOrdId\":\"OPROTECTION1\",\"sCode\":\"0\",\"sMsg\":\"\"}]}";
    let router = Router::new().route(
        "/api/v5/trade/order-algo",
        post(move |body: Bytes| {
            captured
                .lock()
                .push(String::from_utf8(body.to_vec()).unwrap());
            async move { ([("content-type", "application/json")], response) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let base_url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        "METIS-001".into(),
        *OKX_CLIENT_ID,
        *OKX_VENUE,
        OmsType::Netting,
        "OKX-001".into(),
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let config = OKXExecutionClientConfig {
        account_id: "OKX-001".into(),
        base_url_http: Some(base_url),
        api_key: Some("test-key".into()),
        api_secret: Some("test-secret".into()),
        api_passphrase: Some("test-passphrase".into()),
        ..Default::default()
    };
    let mut client = OKXExecutionClient::new(core, config)?;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
    client.emitter.set_sender(event_tx);
    client
        .ws_dispatch_state
        .protection_sources
        .bind(data_tx.into());
    let mut contract = crypto_perpetual_ethusdt();
    contract.id = "BTC-USDT-SWAP.OKX".into();
    contract.raw_symbol = "BTC-USDT-SWAP".into();
    contract.base_currency = Currency::BTC();
    let contract = InstrumentAny::CryptoPerpetual(contract);
    client.on_instrument(contract.clone());
    let instruments = AtomicMap::new();
    instruments.insert("BTC-USDT-SWAP".into(), contract);
    let start = client.clock.get_time_ns().as_u64();
    let order = OrderTestBuilder::new(OrderType::StopMarket)
        .trader_id("METIS-001".into())
        .strategy_id("STRATEGY-001".into())
        .instrument_id("BTC-USDT-SWAP.OKX".into())
        .client_order_id("OPROTECTION1".into())
        .side(OrderSide::Sell)
        .quantity("1".into())
        .trigger_price(Price::from("95000.00"))
        .trigger_type(TriggerType::MarkPrice)
        .reduce_only(true)
        .ts_init(start.into())
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(*OKX_CLIENT_ID), false)?;
    let command = SubmitOrder::from_order(
        &order,
        "METIS-001".into(),
        Some(*OKX_CLIENT_ID),
        None,
        UUID4::new(),
        start.into(),
    );
    client.submit_order(command.clone())?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while requests.lock().is_empty()
            || client
                .ws_dispatch_state
                .order_venue_binding("OPROTECTION1".into())
                .is_none()
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut sources = take_sources(&mut data_rx);
    let mut events = take_events(&mut event_rx);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["Submitted"]["type"], "OrderSubmitted");
    assert_eq!(sources.len(), 1);
    let first: OrderProtectionSource = serde_json::from_str(&sources[0])?;
    let http = first.http.as_ref().unwrap();
    assert_eq!(http.request_body_json, requests.lock()[0]);
    assert_eq!(http.response_body_json, response); // whitespace and unknown raw field retained
    assert!(http.request_ns <= http.received_ns && http.received_ns >= start);
    assert!(!first.execution_authorized && !first.venue_command_causation_available);
    let mut template: Value =
        serde_json::from_str(include_str!("../test_data/ws_orders_algo_states.json"))?;
    template["arg"]["uid"] = "123".into();
    let mut row = template["data"][0].clone();
    row["algoId"] = "PROTECTION-PARENT".into();
    row["algoClOrdId"] = "OPROTECTION1".into();
    row["clOrdId"] = "VENUE-GENERATED-CHILD".into();
    row["ordId"] = "".into();
    row["ordIdList"] = json!([]);
    row["sz"] = "1".into();
    row["ordType"] = "conditional".into();
    row["state"] = "live".into();
    row["tdMode"] = "cross".into();
    row["posSide"] = "net".into();
    row["slTriggerPx"] = "95000.00".into();
    row["slTriggerPxType"] = "mark".into();
    row["slOrdPx"] = "-1".into();
    row["triggerTime"] = "".into();
    let millis = || client.clock.get_time_ns().as_u64() / 1_000_000;
    // A venue millisecond event must be inside the actual nanosecond window;
    // wait for that original fixture time instead of retiming a captured event.
    while millis() * 1_000_000 < start {
        tokio::task::yield_now().await;
    }
    row["cTime"] = millis().to_string().into();
    row["uTime"] = millis().to_string().into();
    template["data"] = json!([row.clone()]);
    dispatch(&client, serde_json::to_string(&template)?, &instruments).await;
    sources.extend(take_sources(&mut data_rx));
    events.extend(take_events(&mut event_rx));
    assert!(
        events
            .iter()
            .any(|v| v["Accepted"]["type"] == "OrderAccepted")
    );
    // Real algorithm update links a unique child. No ACK supplied as a terminal.
    while millis() * 1_000_000 <= start {
        tokio::task::yield_now().await;
    }
    let trigger = millis();
    row["state"] = "effective".into();
    row["ordId"] = "PROTECTION-CHILD".into();
    row["ordIdList"] = json!(["PROTECTION-CHILD"]);
    row["triggerTime"] = trigger.to_string().into();
    row["uTime"] = trigger.to_string().into();
    template["data"] = json!([row]);
    dispatch(&client, serde_json::to_string(&template)?, &instruments).await;
    sources.extend(take_sources(&mut data_rx));
    events.extend(take_events(&mut event_rx));
    let mut child: Value = serde_json::from_str(include_str!(
        "../test_data/ws_orders_algo_child_filled_empty_cl_ord_id.json"
    ))?;
    child["arg"]["uid"] = "123".into();
    child["arg"]["instType"] = "SWAP".into();
    let mut fill = child["data"][0].clone();
    fill["instId"] = "BTC-USDT-SWAP".into();
    fill["instType"] = "SWAP".into();
    fill["ordId"] = "PROTECTION-CHILD".into();
    fill["algoId"] = "PROTECTION-PARENT".into();
    fill["linkedAlgoOrd"]["algoId"] = "PROTECTION-PARENT".into();
    fill["algoClOrdId"] = "OPROTECTION1".into();
    fill["clOrdId"] = "".into();
    fill["side"] = "sell".into();
    fill["sz"] = "1".into();
    fill["ordType"] = "market".into();
    fill["reduceOnly"] = "true".into();
    fill["feeCcy"] = "USDT".into();
    fill["fillFeeCcy"] = "USDT".into();
    for (trade, qty, cumulative, state) in [
        ("PROTECTION-TRADE1", "0.4", "0.4", "partially_filled"),
        ("PROTECTION-TRADE2", "0.6", "1", "filled"),
    ] {
        let event_ms = millis();
        fill["tradeId"] = trade.into();
        fill["fillSz"] = qty.into();
        fill["accFillSz"] = cumulative.into();
        fill["fillPx"] = "94990.00".into();
        fill["avgPx"] = "94990.00".into();
        fill["px"] = "".into();
        fill["state"] = state.into();
        fill["fillTime"] = event_ms.to_string().into();
        fill["uTime"] = event_ms.to_string().into();
        fill["fee"] = "-0.1".into();
        fill["fillFee"] = "-0.1".into();
        child["data"] = json!([fill.clone()]);
        dispatch(&client, serde_json::to_string(&child)?, &instruments).await;
        sources.extend(take_sources(&mut data_rx));
        events.extend(take_events(&mut event_rx));
    }
    let fills = events
        .iter()
        .filter_map(|v| v.get("Filled"))
        .collect::<Vec<_>>();
    assert_eq!(fills.len(), 2);
    assert_eq!(
        Decimal::from_str_exact(fills[0]["last_qty"].as_str().unwrap())?,
        Decimal::new(4, 1)
    );
    assert_eq!(fills[1]["trade_id"], "PROTECTION-TRADE2");
    assert!(
        fills
            .iter()
            .all(|v| v["venue_order_id"] == "PROTECTION-CHILD")
    );
    assert_eq!(sources.len(), 5);
    // Raw source is retained on replay/serialization without reconstructing wire JSON.
    for raw in &sources {
        let source: OrderProtectionSource = serde_json::from_str(raw)?;
        source.validate()?;
        assert_eq!(source.to_json()?, *raw);
    }
    if let Some(path) = std::env::var_os("METIS_TEST_PROTECTION_SDK_EXPORT_PATH") {
        let path = std::path::PathBuf::from(path);
        anyhow::ensure!(path.is_absolute(), "absolute fresh export required");
        std::fs::create_dir(&path)?;
        let evidence = json!({"schema":"MetisActualOKXProtectionSDKTestEvidence.v1","scope":{"environment":"demo","account_id":"123","node_account_id":"OKX-001"},
            "start_ns":start,"end_ns":client.clock.get_time_ns().as_u64()+1,"command":command,
            "events":events,"original_protection_payload_json":sources,"execution_authorized":false,"venue_acceptance":false});
        std::fs::write(path.join("evidence.json"), serde_json::to_vec(&evidence)?)?;
    }
    server.abort();
    Ok(())
}

#[tokio::test]
async fn actual_algo_cancel_http_ack_is_retained_without_fabricating_terminal() -> anyhow::Result<()>
{
    let requests = Arc::new(parking_lot::Mutex::new(Vec::<String>::new()));
    let captured = requests.clone();
    let router=Router::new().route("/api/v5/trade/order-algo",post(||async{
        axum::Json(json!({"code":"0","msg":"","data":[{"algoId":"CANCEL-PARENT","algoClOrdId":"OPROTECTIONCANCEL","sCode":"0","sMsg":""}]}))
    })).route("/api/v5/trade/cancel-algos",post(move |body:Bytes| {captured.lock().push(String::from_utf8(body.to_vec()).unwrap());async {
        axum::Json(json!({"code":"0","msg":"","data":[{"algoId":"CANCEL-PARENT","sCode":"0","sMsg":""}]}))
    }}));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let url = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move {
        axum::serve(listener, router.into_make_service())
            .await
            .unwrap();
    });
    let cache = Rc::new(RefCell::new(Cache::default()));
    let core = ExecutionClientCore::new(
        "METIS-001".into(),
        *OKX_CLIENT_ID,
        *OKX_VENUE,
        OmsType::Netting,
        "OKX-001".into(),
        AccountType::Margin,
        None,
        cache.clone(),
    );
    let mut client = OKXExecutionClient::new(
        core,
        OKXExecutionClientConfig {
            account_id: "OKX-001".into(),
            base_url_http: Some(url),
            api_key: Some("test-key".into()),
            api_secret: Some("test-secret".into()),
            api_passphrase: Some("test-passphrase".into()),
            ..Default::default()
        },
    )?;
    let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
    client.emitter.set_sender(event_tx);
    let (data_tx, mut data_rx) = tokio::sync::mpsc::unbounded_channel();
    client
        .ws_dispatch_state
        .protection_sources
        .bind(data_tx.into());
    let mut instrument = crypto_perpetual_ethusdt();
    instrument.id = "BTC-USDT-SWAP.OKX".into();
    instrument.raw_symbol = "BTC-USDT-SWAP".into();
    instrument.base_currency = Currency::BTC();
    let instrument = InstrumentAny::CryptoPerpetual(instrument);
    client.on_instrument(instrument.clone());
    let instruments = AtomicMap::new();
    instruments.insert("BTC-USDT-SWAP".into(), instrument);
    let now = client.clock.get_time_ns();
    let order = OrderTestBuilder::new(OrderType::StopMarket)
        .trader_id("METIS-001".into())
        .strategy_id("STRATEGY-001".into())
        .instrument_id("BTC-USDT-SWAP.OKX".into())
        .client_order_id("OPROTECTIONCANCEL".into())
        .side(OrderSide::Sell)
        .quantity("1".into())
        .trigger_price("95000".into())
        .reduce_only(true)
        .ts_init(now)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(*OKX_CLIENT_ID), false)?;
    client.submit_order(SubmitOrder::from_order(
        &order,
        "METIS-001".into(),
        Some(*OKX_CLIENT_ID),
        None,
        UUID4::new(),
        now,
    ))?;
    let _ = tokio::time::timeout(Duration::from_secs(5), data_rx.recv())
        .await?
        .context("actual Submit source absent")?;
    let mut frame: Value =
        serde_json::from_str(include_str!("../test_data/ws_orders_algo_states.json"))?;
    frame["arg"]["uid"] = "123".into();
    let mut row = frame["data"][0].clone();
    row["algoId"] = "CANCEL-PARENT".into();
    row["algoClOrdId"] = "OPROTECTIONCANCEL".into();
    row["clOrdId"] = "".into();
    row["ordId"] = "".into();
    row["ordIdList"] = json!([]);
    row["state"] = "live".into();
    row["sz"] = "1".into();
    row["ordType"] = "conditional".into();
    row["triggerTime"] = "".into();
    row["uTime"] = (client.clock.get_time_ns().as_u64() / 1_000_000)
        .to_string()
        .into();
    frame["data"] = json!([row.clone()]);
    dispatch(&client, serde_json::to_string(&frame)?, &instruments).await;
    let _ = take_sources(&mut data_rx);
    let _ = take_events(&mut event_rx);
    let original = CancelOrder::new(
        "METIS-001".into(),
        Some(*OKX_CLIENT_ID),
        "STRATEGY-001".into(),
        "BTC-USDT-SWAP.OKX".into(),
        "OPROTECTIONCANCEL".into(),
        None,
        UUID4::new(),
        client.clock.get_time_ns(),
        None,
        None,
    );
    client.cancel_order(original.clone())?;
    let event = tokio::time::timeout(Duration::from_secs(5), data_rx.recv())
        .await?
        .context("actual Cancel source absent")?;
    let DataEvent::Data(Data::Custom(source)) = event else {
        anyhow::bail!("actual Cancel data profile changed")
    };
    let source = source
        .data
        .as_any()
        .downcast_ref::<OrderProtectionSource>()
        .unwrap();
    assert_eq!(source.kind, "http_cancel_algo");
    assert_eq!(
        source.command_payload_json.as_deref(),
        Some(serde_json::to_string(&original)?.as_str())
    );
    assert_eq!(
        source.http.as_ref().unwrap().request_body_json,
        requests.lock()[0]
    );
    assert!(
        event_rx.try_recv().is_err(),
        "request ACK fabricated Canceled"
    );
    row["state"] = "canceled".into();
    row["uTime"] = (client.clock.get_time_ns().as_u64() / 1_000_000)
        .to_string()
        .into();
    frame["data"] = json!([row]);
    dispatch(&client, serde_json::to_string(&frame)?, &instruments).await;
    let actual = take_events(&mut event_rx);
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0]["Canceled"]["type"], "OrderCanceled");
    let actual_cancel: nautilus_model::events::OrderEventAny =
        serde_json::from_value(actual[0].clone())?;
    let nautilus_model::events::OrderEventAny::Canceled(actual_cancel) = actual_cancel else {
        anyhow::bail!("actual native cancellation variant changed")
    };
    assert!(
        actual_cancel.causation_id.is_none(),
        "venue terminal without request echo gained fabricated causation"
    );
    assert_eq!(actual[0]["Canceled"]["venue_order_id"], "CANCEL-PARENT");
    server.abort();
    Ok(())
}
