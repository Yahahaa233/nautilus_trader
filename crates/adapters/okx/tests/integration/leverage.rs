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

//! Authenticated leverage queries must work for instruments with no positions.
use std::sync::Arc;

use axum::{
    Router,
    extract::Query,
    http::{HeaderMap, Uri},
    routing::get,
};
use nautilus_okx::{
    common::{
        credential::Credential,
        enums::{OKXEnvironment, OKXMarginMode},
    },
    http::{client::OKXRawHttpClient, error::OKXHttpError, query::GetLeverageParams},
};
use rstest::rstest;
use serde_json::json;
use tokio::sync::Mutex;

#[rstest]
#[case(OKXEnvironment::Demo)]
#[case(OKXEnvironment::Live)]
#[tokio::test]
async fn test_leverage_authenticated_flat_instrument(#[case] environment: OKXEnvironment) {
    let request = Arc::new(Mutex::new(None));
    let captured = request.clone();
    let router = Router::new().route(
        "/api/v5/account/leverage-info",
        get(
            move |headers: HeaderMap,
                  uri: Uri,
                  Query(params): Query<std::collections::HashMap<String, String>>| {
                let captured = captured.clone();
                async move {
                    assert_eq!(params["instId"], "DOGE-USDT-SWAP");
                    assert_eq!(params["mgnMode"], "cross");
                    *captured.lock().await = Some((headers, uri));
                    axum::Json(json!({"code":"0","msg":"","data":[{
                        "instId":"DOGE-USDT-SWAP","mgnMode":"cross","posSide":"","lever":"15.0"
                    }]}))
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = OKXRawHttpClient::with_credentials(
        "fixture-key".into(),
        "fixture-secret".into(),
        "fixture-pass".into(),
        format!("http://{address}"),
        5,
        0,
        1,
        1,
        environment,
        None,
    )
    .unwrap();
    let rows = client
        .get_leverage_info(GetLeverageParams {
            inst_id: "DOGE-USDT-SWAP".into(),
            mgn_mode: OKXMarginMode::Cross,
        })
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].inst_id, "DOGE-USDT-SWAP");
    assert_eq!(rows[0].lever, "15.0");
    assert_eq!(rows[0].mgn_mode, OKXMarginMode::Cross);
    assert!(rows[0].pos_side.is_empty());
    let (headers, uri) = request.lock().await.clone().unwrap();
    assert_eq!(headers["ok-access-key"], "fixture-key");
    assert_eq!(headers["ok-access-passphrase"], "fixture-pass");
    assert_eq!(
        headers
            .get("x-simulated-trading")
            .map(|v| v.to_str().unwrap()),
        (environment == OKXEnvironment::Demo).then_some("1")
    );
    let credential = Credential::new(
        "fixture-key".into(),
        "fixture-secret".into(),
        "fixture-pass".into(),
    );
    assert_eq!(
        headers["ok-access-sign"],
        credential.sign_bytes(
            headers["ok-access-timestamp"].to_str().unwrap(),
            "GET",
            uri.to_string().as_str(),
            None,
        )
    );
    server.abort();
}

#[tokio::test]
async fn test_leverage_missing_credentials_is_rejected() {
    let client = OKXRawHttpClient::new(
        Some("http://127.0.0.1:1".into()),
        1,
        0,
        1,
        1,
        OKXEnvironment::Live,
        None,
    )
    .unwrap();
    assert!(matches!(
        client
            .get_leverage_info(GetLeverageParams {
                inst_id: "DOGE-USDT-SWAP".into(),
                mgn_mode: OKXMarginMode::Cross,
            })
            .await,
        Err(OKXHttpError::MissingCredentials)
    ));
}

#[rstest]
fn test_leverage_response_preserves_scope_and_rejects_missing_fields() {
    let row =
        json!({"instId":"DOGE-USDT-SWAP","mgnMode":"isolated","posSide":"short","lever":"15"});
    let parsed: nautilus_okx::http::models::OKXLeverageInfo =
        serde_json::from_value(row.clone()).unwrap();
    assert_eq!(parsed.mgn_mode, OKXMarginMode::Isolated);
    assert_eq!(parsed.pos_side, "short");
    for field in ["instId", "mgnMode", "posSide", "lever"] {
        let mut missing = row.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<nautilus_okx::http::models::OKXLeverageInfo>(missing).is_err()
        );
    }
}
