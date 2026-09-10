use super::*;
use crate::{
    model::{ReverseRequest, SubmarineRequest, TransactionInfo},
    provider::identity_from_secret,
};
use bitcoin::{
    hashes::{sha256, Hash},
    secp256k1::{Secp256k1, SecretKey},
    PublicKey,
};
use lightning_invoice::{Currency, InvoiceBuilder, PaymentSecret};
use swap_common::{
    messages::{Quote, SwapAccept, SwapScript},
    taproot::BoltzTaprootSwap,
    SwapDirection,
};
use uuid::Uuid;

fn identity() -> String {
    identity_from_secret(&[7; 32])
}
fn binding_string() -> String {
    format!("{}|{}|regtest", identity(), identity_from_secret(&[8; 32]))
}
fn key(byte: u8) -> PublicKey {
    PublicKey::new(
        SecretKey::from_slice(&[byte; 32])
            .unwrap()
            .public_key(&Secp256k1::new()),
    )
}
fn invoice(amount: u64) -> String {
    InvoiceBuilder::new(Currency::Regtest)
        .description("backup fixture".into())
        .payment_hash(sha256::Hash::from_byte_array([3; 32]))
        .payment_secret(PaymentSecret([4; 32]))
        .duration_since_epoch(std::time::Duration::from_secs(1))
        .amount_milli_satoshis(amount * 1000)
        .expiry_time(std::time::Duration::from_secs(86_400))
        .min_final_cltv_expiry_delta(240)
        .build_signed(|message| {
            Secp256k1::new()
                .sign_ecdsa_recoverable(message, &SecretKey::from_slice(&[5; 32]).unwrap())
        })
        .unwrap()
        .to_string()
}

fn sample(direction: SwapDirection) -> Store {
    let store = Store::memory(&binding_string()).unwrap();
    let request = match direction {
        SwapDirection::Submarine => CreateRequest::Submarine(SubmarineRequest {
            from: "BTC".into(),
            to: "BTC".into(),
            invoice: invoice(100_000),
            refund_public_key: key(2).to_string(),
            pair_hash: String::new(),
            referral_id: String::new(),
            error: String::new(),
        }),
        SwapDirection::Reverse => CreateRequest::Reverse(ReverseRequest {
            from: "BTC".into(),
            to: "BTC".into(),
            preimage_hash: hex::encode([3; 32]),
            claim_public_key: key(2).to_string(),
            invoice_amount: 0,
            onchain_amount: 100_000,
            pair_hash: String::new(),
            referral_id: String::new(),
            error: String::new(),
        }),
    };
    let mut record = store
        .reserve(request, hex::encode([3; 32]), Some("retry-one"))
        .unwrap();
    let quote = Quote {
        request_id: Some(Uuid::new_v4()),
        quote_id: Uuid::new_v4(),
        offer_id: Uuid::new_v4(),
        direction,
        amount_sat: 100_000,
        fee_sat: 100,
        service_fee_sat: 50,
        onchain_fee_sat: 50,
        fee_rate_sat_vb: 1,
        total_sat: 100_100,
        htlc_timeout_blocks: 144,
        required_confirmations: 1,
        valid_until_unix: 10,
        protocol_version: PROTOCOL_VERSION,
    };
    record.native_request = Some(native_request(&record, &quote, identity()));
    let (claim, refund) = match direction {
        SwapDirection::Submarine => (key(1), key(2)),
        SwapDirection::Reverse => (key(2), key(1)),
    };
    let contract = BoltzTaprootSwap::new(direction, &[3; 32], &claim, &refund, 244).unwrap();
    let accept = SwapAccept {
        script_type: SwapScript::TaprootBoltz,
        swap_tree: Some(contract.swap_tree()),
        quote_id: quote.quote_id,
        swap_id: Uuid::new_v4(),
        direction,
        htlc_script_hex: String::new(),
        htlc_address: contract.address(Network::Regtest).to_string(),
        onchain_amount_sat: if direction == SwapDirection::Reverse {
            quote.amount_sat
        } else {
            quote.total_sat
        },
        timeout_block_height: 244,
        provider_pubkey_hex: key(1).to_string(),
        invoice: if direction == SwapDirection::Reverse {
            Some(invoice(quote.total_sat))
        } else {
            None
        },
    };
    record.response = Some(creation_response(record.id, &accept).unwrap());
    record.accept = Some(accept);
    record.quote = Some(quote);
    record.admission_tip = Some(100);
    store.save(&record).unwrap();
    store
}

#[test]
fn accepted_contracts_round_trip_after_invoice_and_quote_expiry() {
    for direction in [SwapDirection::Submarine, SwapDirection::Reverse] {
        let store = sample(direction);
        let snapshot = store.export_snapshot().unwrap();
        let encoded = snapshot.to_json().unwrap();
        let decoded = StoreSnapshot::from_json(&encoded).unwrap();
        let restored = Store::memory(&binding_string()).unwrap();
        restored.validate_import(&decoded).unwrap();
        restored.import_snapshot(&decoded).unwrap();
        assert_eq!(
            restored.export_snapshot().unwrap().to_json().unwrap(),
            encoded
        );
        let record = &snapshot.swaps[0].record;
        assert_eq!(
            restored
                .reserve(
                    record.request.clone(),
                    record.payment_hash.clone(),
                    Some("retry-one")
                )
                .unwrap()
                .id,
            record.id
        );
    }
}

#[test]
fn transaction_cache_is_excluded_and_existing_observation_is_preserved() {
    let store = sample(SwapDirection::Submarine);
    let mut record = store.all().unwrap().remove(0);
    record.update.status = "transaction.claimed".into();
    record.update.transaction = Some(TransactionInfo {
        id: "cached-id".into(),
        hex: "witness-containing-preimage".into(),
    });
    store.save(&record).unwrap();
    let snapshot = store.export_snapshot().unwrap();
    assert!(!snapshot
        .to_json()
        .unwrap()
        .contains("witness-containing-preimage"));
    assert!(snapshot.swaps[0].record.update.transaction.is_none());
    store.import_snapshot(&snapshot).unwrap();
    assert_eq!(store.get(record.id).unwrap().update, record.update);
    let restored = Store::memory(&binding_string()).unwrap();
    restored.import_snapshot(&snapshot).unwrap();
    assert_eq!(
        restored.get(record.id).unwrap().update,
        SwapUpdate::initial(record.id, SwapDirection::Submarine)
    );
}

#[test]
fn unknown_secret_fields_and_transaction_payloads_are_rejected() {
    let snapshot = sample(SwapDirection::Reverse).export_snapshot().unwrap();
    for pointer in [
        "",
        "/swaps/0",
        "/swaps/0/record",
        "/swaps/0/record/quote",
        "/swaps/0/record/native_request",
        "/swaps/0/record/accept",
        "/swaps/0/record/accept/swap_tree",
        "/swaps/0/record/response",
        "/swaps/0/record/update",
    ] {
        let mut value = serde_json::to_value(&snapshot).unwrap();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("private_key".into(), serde_json::json!("secret"));
        assert!(
            StoreSnapshot::from_json(&value.to_string()).is_err(),
            "accepted unknown field at {pointer}"
        );
    }
    let mut snapshot = snapshot;
    snapshot.swaps[0].record.update.transaction = Some(TransactionInfo {
        id: "id".into(),
        hex: "secret".into(),
    });
    assert!(snapshot.validate().is_err());
}

#[test]
fn binding_fingerprints_and_duplicate_references_are_checked() {
    let snapshot = sample(SwapDirection::Reverse).export_snapshot().unwrap();
    let mut cases = Vec::new();
    let mut changed = snapshot.clone();
    changed.version += 1;
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.identity_binding = "invalid".into();
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].fingerprint = hex::encode([0; 32]);
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].record.payment_hash = hex::encode([0; 32]);
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].record.update.id = Uuid::new_v4();
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps.push(changed.swaps[0].clone());
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.idempotency.push(changed.idempotency[0].clone());
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.idempotency[0].fingerprint = hex::encode([0; 32]);
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.idempotency[0].key = "x".repeat(257);
    cases.push(changed);
    for changed in cases {
        assert!(changed.validate().is_err());
    }
    for network in ["mainnet", "testnet", "regtest", "Bitcoin", "Signet"] {
        assert!(binding(&format!("{}|{}|{network}", identity(), identity())).is_ok());
    }
}

#[test]
fn native_terms_and_response_contract_cannot_be_changed() {
    let snapshot = sample(SwapDirection::Reverse).export_snapshot().unwrap();
    let mut cases = Vec::new();
    let mut changed = snapshot.clone();
    changed.swaps[0]
        .record
        .native_request
        .as_mut()
        .unwrap()
        .client_pkarr = identity_from_secret(&[9; 32]);
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0]
        .record
        .accept
        .as_mut()
        .unwrap()
        .htlc_address = "wrong".into();
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0]
        .record
        .accept
        .as_mut()
        .unwrap()
        .onchain_amount_sat += 1;
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0]
        .record
        .accept
        .as_mut()
        .unwrap()
        .timeout_block_height = u32::MAX;
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0]
        .record
        .quote
        .as_mut()
        .unwrap()
        .service_fee_sat += 1;
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].record.response.as_mut().unwrap()["onchainAmount"] = serde_json::json!(999);
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].record.admission_tip = None;
    cases.push(changed);
    let mut changed = snapshot.clone();
    changed.swaps[0].record.quote = None;
    cases.push(changed);
    for changed in cases {
        assert!(changed.validate().is_err());
    }
}

#[test]
fn merging_preserves_admission_and_rejects_conflicting_quotes_atomically() {
    let source = sample(SwapDirection::Reverse);
    let admitted = source.export_snapshot().unwrap();
    let mut pending = admitted.clone();
    pending.swaps[0].record.accept = None;
    pending.swaps[0].record.response = None;
    pending.swaps[0].record.admission_tip = None;
    let target = Store::memory(&binding_string()).unwrap();
    target.import_snapshot(&pending).unwrap();
    target.import_snapshot(&admitted).unwrap();
    target.import_snapshot(&pending).unwrap();
    assert_eq!(
        target.export_snapshot().unwrap().to_json().unwrap(),
        admitted.to_json().unwrap()
    );
    let before = target.export_snapshot().unwrap().to_json().unwrap();
    pending.swaps[0].record.quote.as_mut().unwrap().offer_id = Uuid::new_v4();
    pending.validate().unwrap();
    assert!(matches!(
        target.validate_import(&pending),
        Err(Error::Conflict)
    ));
    assert!(matches!(
        target.import_snapshot(&pending),
        Err(Error::Conflict)
    ));
    assert_eq!(target.export_snapshot().unwrap().to_json().unwrap(), before);
    let mut foreign = admitted;
    foreign.identity_binding = format!("{}|{}|regtest", identity(), identity_from_secret(&[9; 32]));
    assert!(target.import_snapshot(&foreign).is_err());
}

#[test]
fn conflicting_retry_bindings_roll_back_all_new_records() {
    let source = sample(SwapDirection::Reverse);
    let mut incoming = source.export_snapshot().unwrap();
    let target = Store::memory(&binding_string()).unwrap();
    let mut request = incoming.swaps[0].record.request.clone();
    if let CreateRequest::Reverse(request) = &mut request {
        request.preimage_hash = hex::encode([6; 32]);
    }
    target
        .reserve(request, hex::encode([6; 32]), Some("retry-one"))
        .unwrap();
    let before = target.export_snapshot().unwrap().to_json().unwrap();
    assert!(matches!(
        target.import_snapshot(&incoming),
        Err(Error::Conflict)
    ));
    assert_eq!(target.export_snapshot().unwrap().to_json().unwrap(), before);
    incoming.idempotency[0].key = "retry-two".into();
    target.import_snapshot(&incoming).unwrap();
    assert_eq!(target.all().unwrap().len(), 2);
}

#[test]
fn directory_snapshots_honor_process_locks_and_do_not_create_missing_databases() {
    let snapshot = sample(SwapDirection::Submarine).export_snapshot().unwrap();
    let directory = tempfile::tempdir().unwrap();
    assert!(Store::snapshot_directory(directory.path()).is_err());
    assert!(!directory.path().join("swaps.sqlite3").exists());
    Store::validate_directory_import(directory.path(), &snapshot).unwrap();
    assert!(!directory.path().join("swaps.sqlite3").exists());
    Store::import_directory(directory.path(), &snapshot).unwrap();
    let store = Store::open(directory.path(), &binding_string()).unwrap();
    assert!(matches!(
        Store::snapshot_directory(directory.path()),
        Err(Error::Busy)
    ));
    assert!(matches!(
        Store::validate_directory_import(directory.path(), &snapshot),
        Err(Error::Busy)
    ));
    drop(store);
    assert_eq!(
        Store::snapshot_directory(directory.path())
            .unwrap()
            .to_json()
            .unwrap(),
        snapshot.to_json().unwrap()
    );
}

#[test]
fn corrupted_local_indices_are_not_exported_or_overwritten() {
    let store = sample(SwapDirection::Reverse);
    let snapshot = store.export_snapshot().unwrap();
    store
        .connection
        .lock()
        .unwrap()
        .execute("UPDATE swaps SET payment_hash='wrong'", [])
        .unwrap();
    assert!(store.export_snapshot().is_err());
    assert!(store.import_snapshot(&snapshot).is_err());
}

#[test]
fn oversized_json_is_rejected_before_parsing() {
    assert!(StoreSnapshot::from_json(&" ".repeat(MAX_SNAPSHOT_BYTES + 1)).is_err());
}

struct OfflineChain;
#[async_trait::async_trait]
impl crate::chain::Chain for OfflineChain {
    async fn tip(&self) -> Result<u32> {
        panic!("backup contacted chain")
    }
    async fn fee(&self) -> Result<u64> {
        panic!("backup contacted chain")
    }
    async fn observe(&self, _: &SwapAccept) -> Result<Option<crate::chain::Observation>> {
        panic!("backup contacted chain")
    }
    async fn transaction(&self, _: bitcoin::Txid) -> Result<TransactionInfo> {
        panic!("backup contacted chain")
    }
    async fn broadcast(&self, _: bitcoin::Transaction) -> Result<bitcoin::Txid> {
        panic!("backup broadcast a transaction")
    }
}

#[derive(Default)]
struct PausedProvider {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl crate::provider::Provider for PausedProvider {
    fn identity(&self) -> String {
        identity()
    }
    fn provider_key(&self) -> String {
        identity_from_secret(&[8; 32])
    }
    async fn offer(&self) -> Result<swap_common::messages::SwapOffer> {
        self.entered.notify_one();
        self.release.notified().await;
        Err(Error::Provider)
    }
    async fn quote(&self, _: QuoteRequest) -> Result<Quote> {
        panic!("backup contacted provider")
    }
    async fn create(&self, _: swap_common::messages::SwapRequest) -> Result<SwapAccept> {
        panic!("backup contacted provider")
    }
    async fn snapshot(&self, _: Uuid) -> Result<swap_common::messages::SwapStatusSnapshot> {
        panic!("backup contacted provider")
    }
}

#[tokio::test]
async fn bridge_backup_does_not_wait_for_network_and_restore_rejects_active_operations() {
    use std::sync::Arc;
    let snapshot = sample(SwapDirection::Reverse).export_snapshot().unwrap();
    let provider = Arc::new(PausedProvider::default());
    let bridge = crate::Bridge::new(
        provider.clone(),
        Arc::new(OfflineChain),
        Store::memory(&binding_string()).unwrap(),
        crate::service::BridgeSettings {
            network: Network::Regtest,
            max_fee_bps: 500,
            max_amount_sat: 1_000_000,
        },
    );
    let request = snapshot.swaps[0].record.request.clone();
    let task = tokio::spawn({
        let bridge = bridge.clone();
        async move { bridge.create(request, None).await }
    });
    provider.entered.notified().await;
    assert!(bridge.export_snapshot().unwrap().swaps.is_empty());
    bridge.validate_import_snapshot(&snapshot).unwrap();
    assert!(matches!(
        bridge.import_snapshot(&snapshot).await,
        Err(Error::Busy)
    ));
    provider.release.notify_one();
    assert!(matches!(task.await.unwrap(), Err(Error::Provider)));
    bridge.import_snapshot(&snapshot).await.unwrap();
    assert_eq!(
        bridge.export_snapshot().unwrap().to_json().unwrap(),
        snapshot.to_json().unwrap()
    );
}
