#[path = "../examples/fixture_server.rs"]
#[allow(dead_code)]
mod fixture;

use async_trait::async_trait;
use bitcoin::Network;
use futures_util::{SinkExt, StreamExt};
use pubky_swap_boltz::{
    http,
    model::{CreateRequest, ReverseRequest},
    provider::Provider,
    service::BridgeSettings,
    store::Store,
    Bridge, Result,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use swap_common::messages::*;
use tokio::{net::TcpStream, sync::Notify, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

#[derive(Default)]
struct BlockedProvider {
    inner: fixture::FixtureProvider,
    started: Notify,
    cancelled: Notify,
}

struct SnapshotGuard<'a>(&'a Notify);

impl Drop for SnapshotGuard<'_> {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl Provider for BlockedProvider {
    fn identity(&self) -> String {
        self.inner.identity()
    }
    fn provider_key(&self) -> String {
        self.inner.provider_key()
    }
    async fn offer(&self) -> Result<SwapOffer> {
        self.inner.offer().await
    }
    async fn quote(&self, request: QuoteRequest) -> Result<Quote> {
        self.inner.quote(request).await
    }
    async fn create(&self, request: SwapRequest) -> Result<SwapAccept> {
        self.inner.create(request).await
    }
    async fn snapshot(&self, _: Uuid) -> Result<SwapStatusSnapshot> {
        let _guard = SnapshotGuard(&self.cancelled);
        self.started.notify_one();
        std::future::pending().await
    }
}

struct SocketFixture {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    provider: Arc<BlockedProvider>,
    swap_id: String,
    server: JoinHandle<()>,
}

impl Drop for SocketFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl SocketFixture {
    async fn new() -> Self {
        let provider = Arc::new(BlockedProvider::default());
        let bridge = Bridge::new(
            provider.clone(),
            Arc::new(fixture::FixtureChain),
            Store::memory("websocket-fixture").unwrap(),
            BridgeSettings {
                network: Network::Regtest,
                max_fee_bps: 1000,
                max_amount_sat: 1_000_000,
            },
        );
        let response = bridge.create(request(), None).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = http::router(bridge, address);
        let server = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let (socket, _) = connect_async(format!("ws://{address}/v2/ws"))
            .await
            .unwrap();
        Self {
            socket,
            provider,
            swap_id: response["id"].as_str().unwrap().into(),
            server,
        }
    }

    async fn command(&mut self, operation: &str) {
        self.socket
            .send(Message::Text(
                json!({"op":operation,"channel":"swap.update","args":[self.swap_id]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let response = self.next().await.into_text().unwrap();
        let response: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["event"], operation);
    }

    async fn next(&mut self) -> Message {
        tokio::time::timeout(Duration::from_secs(1), self.socket.next())
            .await
            .expect("socket remained responsive")
            .unwrap()
            .unwrap()
    }
}

fn request() -> CreateRequest {
    CreateRequest::Reverse(ReverseRequest {
        from: "BTC".into(),
        to: "BTC".into(),
        preimage_hash: hex::encode([3; 32]),
        claim_public_key: "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
            .into(),
        invoice_amount: 100_000,
        onchain_amount: 0,
        pair_hash: String::new(),
        referral_id: String::new(),
        error: String::new(),
    })
}

#[tokio::test]
async fn blocked_snapshot_does_not_block_ping_or_unsubscribe_and_is_cancelled_on_close() {
    let mut fixture = SocketFixture::new().await;
    fixture.command("subscribe").await;
    tokio::time::timeout(Duration::from_secs(1), fixture.provider.started.notified())
        .await
        .unwrap();
    fixture
        .socket
        .send(Message::Ping(b"still-alive".to_vec().into()))
        .await
        .unwrap();
    assert_eq!(
        fixture.next().await,
        Message::Pong(b"still-alive".to_vec().into())
    );
    fixture.command("unsubscribe").await;
    fixture.socket.close(None).await.unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.provider.cancelled.notified(),
    )
    .await
    .expect("snapshot worker was cancelled on close");
}

#[tokio::test]
async fn full_snapshot_queue_requests_resubscription_instead_of_growing() {
    let mut fixture = SocketFixture::new().await;
    fixture.command("subscribe").await;
    tokio::time::timeout(Duration::from_secs(1), fixture.provider.started.notified())
        .await
        .unwrap();
    for _ in 0..8 {
        fixture.command("subscribe").await;
    }
    fixture.command("subscribe").await;
    let response: Value = serde_json::from_str(&fixture.next().await.into_text().unwrap()).unwrap();
    assert_eq!(response["event"], "error");
    assert!(response["error"].as_str().unwrap().contains("resubscribe"));
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.provider.cancelled.notified(),
    )
    .await
    .expect("overloaded connection cancelled snapshot work");
}
