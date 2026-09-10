//! The bounded localhost REST boundary for the pinned Bitcoin SDK profile.

use crate::{model::*, websocket, Bridge, Error, Result};
use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{net::SocketAddr, str::FromStr, sync::Arc};
use swap_common::SwapDirection;
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct AppState {
    pub bridge: Arc<Bridge>,
    pub sockets: Arc<tokio::sync::Semaphore>,
}

pub fn router(bridge: Arc<Bridge>, bind: SocketAddr) -> Router {
    let state = AppState {
        bridge,
        sockets: Arc::new(tokio::sync::Semaphore::new(32)),
    };
    Router::new()
        .route("/health", get(health))
        .route("/v2/version", get(version))
        .route("/v2/nodes", get(nodes))
        .route(
            "/v2/swap/submarine",
            get(submarine_pairs).post(create_submarine),
        )
        .route("/v2/swap/reverse", get(reverse_pairs).post(create_reverse))
        .route(
            "/v2/swap/chain",
            get(|| async { Json(json!({})) }).post(unsupported),
        )
        .route("/v2/swap/{id}", get(status))
        .route("/v2/swap/submarine/{id}/transaction", get(swap_transaction))
        .route("/v2/swap/reverse/{id}/transaction", get(swap_transaction))
        .route(
            "/v2/swap/{kind}/{id}/claim",
            get(unsupported).post(unsupported),
        )
        .route("/v2/swap/{kind}/{id}/refund", post(unsupported))
        .route("/v2/chain/{asset}/fee", get(fee))
        .route("/v2/chain/{asset}/height", get(height))
        .route("/v2/chain/{asset}/transaction/{txid}", get(transaction))
        .route("/v2/chain/{asset}/transaction", post(broadcast))
        .route("/v2/ws", get(websocket::upgrade))
        .fallback(unsupported)
        .method_not_allowed_fallback(unsupported)
        .layer(DefaultBodyLimit::max(32 * 1024))
        .layer(middleware::from_fn(move |request, next| {
            local_boundary(request, next, bind)
        }))
        .with_state(state)
}

async fn local_boundary(request: axum::extract::Request, next: Next, bind: SocketAddr) -> Response {
    let host = request
        .headers()
        .get("host")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let allowed = [bind.to_string(), format!("localhost:{}", bind.port())];
    if !allowed.iter().any(|a| a == host) || request.headers().contains_key("origin") {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error":"only local clients without a browser origin are supported"})),
        )
            .into_response();
    }
    next.run(request).await
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match self {
            Error::Invalid(_) | Error::Unsupported => StatusCode::BAD_REQUEST,
            Error::Validation => StatusCode::BAD_GATEWAY,
            Error::Conflict => StatusCode::CONFLICT,
            Error::NotFound => StatusCode::NOT_FOUND,
            Error::Provider | Error::Chain => StatusCode::SERVICE_UNAVAILABLE,
            Error::Storage => StatusCode::INTERNAL_SERVER_ERROR,
            Error::Busy => StatusCode::TOO_MANY_REQUESTS,
        };
        (status, Json(json!({"error":self.to_string()}))).into_response()
    }
}

async fn health(State(state): State<AppState>) -> Result<Json<Value>> {
    state.bridge.offer().await?;
    state.bridge.chain.tip().await?;
    Ok(Json(json!({"status":"ok"})))
}

async fn version() -> Json<Value> {
    Json(
        json!({"version":env!("CARGO_PKG_VERSION"),"name":"pubky-swap-boltz","compatibility":"boltz-client-v2.12.5-bitcoin-script-path"}),
    )
}

async fn nodes(State(state): State<AppState>) -> Result<Json<Value>> {
    let offer = state.bridge.offer().await?;
    Ok(Json(match offer.lightning_node_id {
        Some(key) => json!({"BTC":{"pubky":{"publicKey":key,"uris":[]}}}),
        None => json!({"BTC":{}}),
    }))
}

async fn submarine_pairs(State(state): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(state.bridge.pairs(SwapDirection::Submarine).await?))
}
async fn reverse_pairs(State(state): State<AppState>) -> Result<Json<Value>> {
    Ok(Json(state.bridge.pairs(SwapDirection::Reverse).await?))
}

async fn create_submarine(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: std::result::Result<Json<SubmarineRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    let request = body
        .map_err(|_| Error::Invalid("invalid submarine request"))?
        .0;
    create(state.bridge, CreateRequest::Submarine(request), headers).await
}

async fn create_reverse(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: std::result::Result<Json<ReverseRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    let request = body
        .map_err(|_| Error::Invalid("invalid reverse request"))?
        .0;
    create(state.bridge, CreateRequest::Reverse(request), headers).await
}

async fn create(
    bridge: Arc<Bridge>,
    request: CreateRequest,
    headers: HeaderMap,
) -> Result<Json<Value>> {
    let key = headers
        .get("idempotency-key")
        .map(|v| {
            v.to_str()
                .map(str::to_owned)
                .map_err(|_| Error::Invalid("invalid idempotency key"))
        })
        .transpose()?;
    if key.as_ref().is_some_and(|k| k.is_empty() || k.len() > 128) {
        return Err(Error::Invalid(
            "idempotency key must contain 1 to 128 bytes",
        ));
    }
    // Admission survives an HTTP caller disconnecting after the native request was sent.
    let response = tokio::spawn(async move { bridge.create(request, key).await })
        .await
        .map_err(|_| Error::Storage)??;
    Ok(Json(response))
}

async fn status(State(state): State<AppState>, Path(id): Path<String>) -> Result<Json<SwapUpdate>> {
    Ok(Json(state.bridge.refresh(parse_id(&id)?).await?))
}
async fn swap_transaction(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(state.bridge.swap_transaction(parse_id(&id)?).await?))
}
pub(crate) fn parse_id(id: &str) -> Result<Uuid> {
    Uuid::parse_str(id).map_err(|_| Error::NotFound)
}
async fn unsupported() -> Error {
    Error::Unsupported
}
fn bitcoin_only(asset: &str) -> Result<()> {
    if asset == "BTC" {
        Ok(())
    } else {
        Err(Error::Unsupported)
    }
}

async fn fee(State(state): State<AppState>, Path(asset): Path<String>) -> Result<Json<Value>> {
    bitcoin_only(&asset)?;
    Ok(Json(json!({"fee":state.bridge.chain.fee().await?})))
}
async fn height(State(state): State<AppState>, Path(asset): Path<String>) -> Result<Json<Value>> {
    bitcoin_only(&asset)?;
    Ok(Json(json!({"height":state.bridge.chain.tip().await?})))
}

async fn transaction(
    State(state): State<AppState>,
    Path((asset, txid)): Path<(String, String)>,
) -> Result<Json<Value>> {
    bitcoin_only(&asset)?;
    let txid =
        bitcoin::Txid::from_str(&txid).map_err(|_| Error::Invalid("invalid transaction id"))?;
    let transaction = state.bridge.chain.transaction(txid).await?;
    Ok(Json(json!({"hex":transaction.hex})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BroadcastRequest {
    hex: String,
}

async fn broadcast(
    State(state): State<AppState>,
    Path(asset): Path<String>,
    body: std::result::Result<Json<BroadcastRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<Value>> {
    bitcoin_only(&asset)?;
    let body = body
        .map_err(|_| Error::Invalid("invalid broadcast request"))?
        .0;
    let bytes = hex::decode(body.hex).map_err(|_| Error::Invalid("invalid transaction"))?;
    let transaction = bitcoin::consensus::deserialize(&bytes)
        .map_err(|_| Error::Invalid("invalid transaction"))?;
    Ok(Json(
        json!({"id":state.bridge.chain.broadcast(transaction).await?}),
    ))
}
