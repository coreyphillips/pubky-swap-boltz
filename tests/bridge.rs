#[path = "../examples/fixture_server.rs"]
#[allow(dead_code)]
mod fixture;

use async_trait::async_trait;
use bitcoin::{Network, Transaction, Txid};
use pubky_swap_boltz::{
    chain::{Chain, Observation},
    http,
    model::{CreateRequest, ReverseRequest, TransactionInfo},
    provider::Provider,
    service::BridgeSettings,
    store::Store,
    Bridge, Error, Result,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use swap_common::messages::*;
use uuid::Uuid;

fn request(hash: u8) -> CreateRequest {
    CreateRequest::Reverse(ReverseRequest {
        from: "BTC".into(),
        to: "BTC".into(),
        preimage_hash: hex::encode([hash; 32]),
        claim_public_key: "02c6047f9441ed7d6d3045406e95c07cd85c778e4b8cef3ca7abac09b95c709ee5"
            .into(),
        invoice_amount: 100_000,
        onchain_amount: 0,
        pair_hash: String::new(),
        referral_id: String::new(),
        error: String::new(),
    })
}

fn settings() -> BridgeSettings {
    BridgeSettings {
        network: Network::Regtest,
        max_fee_bps: 1000,
        max_amount_sat: 1_000_000,
    }
}

#[derive(Default)]
struct FaultProvider {
    inner: fixture::FixtureProvider,
    lose_response: AtomicBool,
    tamper: AtomicBool,
    creates: AtomicUsize,
}

#[async_trait]
impl Provider for FaultProvider {
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
        self.creates.fetch_add(1, Ordering::SeqCst);
        let mut accept = self.inner.create(request).await?;
        if self.lose_response.swap(false, Ordering::SeqCst) {
            return Err(Error::Provider);
        }
        if self.tamper.load(Ordering::SeqCst) {
            accept.onchain_amount_sat += 1;
        }
        Ok(accept)
    }
    async fn snapshot(&self, id: Uuid) -> Result<SwapStatusSnapshot> {
        self.inner.snapshot(id).await
    }
}

#[tokio::test]
async fn response_loss_recovers_the_same_native_swap_after_process_restart() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(FaultProvider::default());
    provider.lose_response.store(true, Ordering::SeqCst);
    let store = Store::open(directory.path(), "binding").unwrap();
    let bridge = Bridge::new(
        provider.clone(),
        Arc::new(fixture::FixtureChain),
        store,
        settings(),
    );
    assert!(matches!(
        bridge.create(request(1), Some("stable".into())).await,
        Err(Error::Provider)
    ));
    drop(bridge);
    let store = Store::open(directory.path(), "binding").unwrap();
    let pending = store.all().unwrap();
    assert_eq!(pending.len(), 1);
    let quote_id = pending[0].native_request.as_ref().unwrap().quote_id;
    let bridge = Bridge::new(
        provider.clone(),
        Arc::new(fixture::FixtureChain),
        store,
        settings(),
    );
    let recovered = bridge
        .create(request(1), Some("stable".into()))
        .await
        .unwrap();
    let again = bridge.create(request(1), None).await.unwrap();
    assert_eq!(recovered, again);
    assert_eq!(provider.creates.load(Ordering::SeqCst), 2);
    let native = provider
        .inner
        .create(pending[0].native_request.clone().unwrap())
        .await
        .unwrap();
    assert_eq!(native.quote_id, quote_id);
    assert_eq!(recovered["lockupAddress"], native.htlc_address);
}

#[tokio::test]
async fn tampered_provider_amount_never_becomes_a_fundable_response() {
    let provider = Arc::new(FaultProvider::default());
    provider.tamper.store(true, Ordering::SeqCst);
    let bridge = Bridge::new(
        provider,
        Arc::new(fixture::FixtureChain),
        Store::memory("binding").unwrap(),
        settings(),
    );
    assert!(matches!(
        bridge.create(request(2), None).await,
        Err(Error::Validation)
    ));
}

#[test]
fn idempotency_conflicts_and_payment_hash_reuse_are_rejected_durably() {
    let store = Store::memory("binding").unwrap();
    let original = store
        .reserve(request(3), hex::encode([3; 32]), Some("key"))
        .unwrap();
    assert_eq!(
        store
            .reserve(request(3), hex::encode([3; 32]), Some("key"))
            .unwrap()
            .id,
        original.id
    );
    assert!(matches!(
        store.reserve(request(4), hex::encode([4; 32]), Some("key")),
        Err(Error::Conflict)
    ));
    let mut changed = request(3);
    if let CreateRequest::Reverse(r) = &mut changed {
        r.invoice_amount += 1;
    }
    assert!(matches!(
        store.reserve(changed, hex::encode([3; 32]), None),
        Err(Error::Conflict)
    ));
}

#[test]
fn state_directory_is_bound_to_one_identity_provider_and_process() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), "alice|provider-a|regtest").unwrap();
    assert!(Store::open(directory.path(), "alice|provider-a|regtest").is_err());
    drop(store);
    assert!(Store::open(directory.path(), "bob|provider-a|regtest").is_err());
    assert!(Store::open(directory.path(), "alice|provider-b|regtest").is_err());
    assert!(Store::open(directory.path(), "alice|provider-a|bitcoin").is_err());
}

struct ObservedChain {
    confirmations: AtomicUsize,
    tip: AtomicUsize,
    spent: AtomicBool,
}

#[async_trait]
impl Chain for ObservedChain {
    async fn tip(&self) -> Result<u32> {
        Ok(self.tip.load(Ordering::SeqCst) as u32)
    }
    async fn fee(&self) -> Result<u64> {
        Ok(1)
    }
    async fn observe(&self, _: &SwapAccept) -> Result<Option<Observation>> {
        Ok(Some(Observation {
            confirmations: self.confirmations.load(Ordering::SeqCst) as u32,
            transaction: TransactionInfo {
                id: hex::encode([5; 32]),
                hex: "confirmed-transaction-fixture".into(),
            },
            outpoint: bitcoin::OutPoint::null(),
            spend: self.spent.load(Ordering::SeqCst).then(|| TransactionInfo {
                id: "pending-spend".into(),
                hex: String::new(),
            }),
            spend_confirmations: 0,
        }))
    }
    async fn transaction(&self, _: Txid) -> Result<TransactionInfo> {
        Err(Error::NotFound)
    }
    async fn broadcast(&self, tx: Transaction) -> Result<Txid> {
        Ok(tx.compute_txid())
    }
}

#[tokio::test]
async fn reverse_client_cannot_receive_claim_trigger_or_transaction_until_confirmed() {
    let chain = Arc::new(ObservedChain {
        confirmations: AtomicUsize::new(0),
        tip: AtomicUsize::new(100),
        spent: AtomicBool::new(false),
    });
    let bridge = Bridge::new(
        Arc::new(FaultProvider::default()),
        chain.clone(),
        Store::memory("binding").unwrap(),
        settings(),
    );
    let response = bridge.create(request(5), None).await.unwrap();
    let id = Uuid::parse_str(response["id"].as_str().unwrap()).unwrap();
    let pending = bridge.refresh(id).await.unwrap();
    assert_eq!(pending.status, "invoice.paid");
    assert!(pending.transaction.is_none());
    assert!(bridge.swap_transaction(id).await.is_err());
    chain.confirmations.store(1, Ordering::SeqCst);
    let confirmed = bridge.refresh(id).await.unwrap();
    assert_eq!(confirmed.status, "transaction.confirmed");
    assert!(confirmed.transaction.is_some());
    chain.confirmations.store(0, Ordering::SeqCst);
    let reorged = bridge.refresh(id).await.unwrap();
    assert!(reorged.transaction.is_none());
    assert_eq!(reorged.status, "invoice.paid");
    chain.confirmations.store(1, Ordering::SeqCst);
    chain.tip.store(227, Ordering::SeqCst);
    let late = bridge.refresh(id).await.unwrap();
    assert!(late.transaction.is_none());
    chain.tip.store(100, Ordering::SeqCst);
    chain.spent.store(true, Ordering::SeqCst);
    let spent = bridge.refresh(id).await.unwrap();
    assert!(spent.transaction.is_none());
}

#[tokio::test]
async fn browser_origins_and_rebinding_hosts_are_rejected() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let bridge = fixture::fixture_bridge().unwrap();
    let app = http::router(bridge, "127.0.0.1:9001".parse().unwrap());
    for (host, origin) in [
        ("attacker.example:9001", None),
        ("127.0.0.1:9001", Some("https://attacker.example")),
    ] {
        let mut request = Request::builder().uri("/v2/version").header("host", host);
        if let Some(origin) = origin {
            request = request.header("origin", origin);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
    }
}

#[tokio::test]
async fn unsupported_cooperative_body_is_never_parsed_or_polled() {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    let never = futures_util::stream::poll_fn(
        |_| -> std::task::Poll<Option<std::result::Result<axum::body::Bytes, std::io::Error>>> {
            panic!("cooperative body must never be polled")
        },
    );
    let app = http::router(
        fixture::fixture_bridge().unwrap(),
        "127.0.0.1:9001".parse().unwrap(),
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v2/swap/reverse/anything/claim")
                .header("host", "127.0.0.1:9001")
                .body(Body::from_stream(never))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
}

#[test]
fn reverse_fee_inversion_never_silently_changes_invoice_amount() {
    let mut offer = fixture::fixture_offer();
    offer.base_fee_sat = 500;
    offer.onchain_fee_sat = 770;
    offer.fee_ppm = 2000;
    for output in 10_000..20_000 {
        let invoice = pubky_swap_boltz::fees::total(&offer, output).unwrap();
        assert_eq!(
            pubky_swap_boltz::fees::reverse_output(&offer, invoice).unwrap(),
            output
        );
    }
    let skipped = pubky_swap_boltz::fees::total(&offer, 10_500).unwrap() - 1;
    assert!(pubky_swap_boltz::fees::reverse_output(&offer, skipped).is_err());
    offer.fee_ppm = u64::MAX;
    assert!(pubky_swap_boltz::fees::total(&offer, u64::MAX).is_err());
}

#[tokio::test]
async fn acceptance_recovery_keeps_original_height_binding_after_several_blocks() {
    let directory = tempfile::tempdir().unwrap();
    let provider = Arc::new(FaultProvider::default());
    provider.lose_response.store(true, Ordering::SeqCst);
    let chain = Arc::new(ObservedChain {
        confirmations: AtomicUsize::new(0),
        tip: AtomicUsize::new(100),
        spent: AtomicBool::new(false),
    });
    let bridge = Bridge::new(
        provider.clone(),
        chain.clone(),
        Store::open(directory.path(), "binding").unwrap(),
        settings(),
    );
    assert!(bridge.create(request(6), None).await.is_err());
    drop(bridge);
    chain.tip.store(110, Ordering::SeqCst);
    let bridge = Bridge::new(
        provider,
        chain,
        Store::open(directory.path(), "binding").unwrap(),
        settings(),
    );
    let response = bridge.create(request(6), None).await.unwrap();
    assert_eq!(response["timeoutBlockHeight"], 244);
}

#[tokio::test]
async fn rejected_stale_pair_hash_does_not_consume_the_payment_hash() {
    let bridge = fixture::fixture_bridge().unwrap();
    let mut stale = request(7);
    if let CreateRequest::Reverse(r) = &mut stale {
        r.pair_hash = "stale".into();
    }
    assert!(matches!(
        bridge.create(stale, None).await,
        Err(Error::Invalid(_))
    ));
    assert!(bridge.create(request(7), None).await.is_ok());
}

#[tokio::test]
async fn discovery_honors_the_local_total_amount_ceiling() {
    let settings = BridgeSettings {
        network: Network::Regtest,
        max_fee_bps: 1000,
        max_amount_sat: 50_000,
    };
    let bridge = Bridge::new(
        Arc::new(FaultProvider::default()),
        Arc::new(fixture::FixtureChain),
        Store::memory("binding").unwrap(),
        settings,
    );
    let pairs = bridge
        .pairs(swap_common::SwapDirection::Reverse)
        .await
        .unwrap();
    assert_eq!(pairs["BTC"]["BTC"]["limits"]["maximal"], 50_000);
    let mut boundary = request(8);
    if let CreateRequest::Reverse(r) = &mut boundary {
        r.invoice_amount = 50_000;
        r.pair_hash = pairs["BTC"]["BTC"]["hash"].as_str().unwrap().into();
    }
    assert!(bridge.create(boundary, None).await.is_ok());
    assert!(bridge.create(request(9), None).await.is_err());
}
