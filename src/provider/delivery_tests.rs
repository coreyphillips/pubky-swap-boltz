use super::*;
use pubky_transport::{outbox::Outbox, DeliveryFailure};

fn creation(quote_id: Uuid) -> SwapMessage {
    SwapMessage::SwapRequest(SwapRequest {
        script_type: SwapScript::TaprootBoltz,
        quote_id,
        client_pkarr: identity_from_secret(&[1; 32]),
        direction: swap_common::SwapDirection::Reverse,
        payment_hash_hex: hex::encode([2; 32]),
        client_claim_pubkey_hex: Some("key".into()),
        client_refund_pubkey_hex: None,
        invoice: None,
    })
}

fn provider_with_transport(transport: Transport, peer: String) -> PubkyProvider {
    let (commands, _) = mpsc::channel(8);
    PubkyProvider {
        identity: transport.public_key_string(),
        provider: peer,
        timeout: Duration::from_secs(1),
        commands,
        transport: Some(Arc::new(transport)),
        events: broadcast::channel(4).0,
    }
}

#[test]
fn paused_delivery_requires_explicit_retry_and_keeps_resource_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("outbox.json");
    let peer = identity_from_secret(&[2; 32]);
    let request = creation(Uuid::new_v4());
    let transport = Transport::unsigned([1; 32])
        .unwrap()
        .with_outbox(&path)
        .unwrap();
    transport
        .enqueue_with_scope(&peer, &request.delivery_scope(), &request)
        .unwrap();
    let id = transport.outbox_status().unwrap()[0].id.clone();
    drop(transport);
    let outbox = Outbox::open(&path).unwrap();
    outbox
        .mark_failed(
            &id,
            crate::validation::now(),
            DeliveryFailure::Authentication(401),
        )
        .unwrap();
    drop(outbox);
    let before: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let transport = Transport::unsigned([1; 32])
        .unwrap()
        .with_outbox(&path)
        .unwrap();
    transport
        .enqueue_with_scope(&peer, &request.delivery_scope(), &request)
        .unwrap();
    let provider = provider_with_transport(transport, peer);
    assert!(provider.delivery_status().unwrap()[0].publication_paused);
    provider
        .retry_delivery(&id, DeliveryOperation::Publication)
        .unwrap();
    let status = provider.delivery_status().unwrap();
    assert_eq!(status[0].id, id);
    assert!(!status[0].publication_paused);
    assert_eq!(
        status[0].publication_failure,
        Some(DeliveryFailure::Authentication(401))
    );
    let after: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(
        before["entries"][0]["prepared"],
        after["entries"][0]["prepared"]
    );
}

#[tokio::test]
async fn cancellation_before_send_never_reserves_creation_and_releases_journal() {
    let directory = tempfile::tempdir().unwrap();
    let identity = identity_from_secret(&[1; 32]);
    let peer = identity_from_secret(&[2; 32]);
    let journal =
        DeliveryJournal::open(Some(directory.path()), &identity, &peer, Network::Regtest).unwrap();
    let path = journal.directory.as_ref().unwrap().join("replies.sqlite3");
    let (send, receive) = mpsc::channel(8);
    let quote_id = Uuid::new_v4();
    let (reply, dropped) = oneshot::channel();
    drop(dropped);
    send.send(WorkerCommand::Request(Box::new(Command {
        request: creation(quote_id),
        key: ReplyKey::Accept(quote_id),
        reply,
        deadline: Instant::now() + Duration::from_secs(1),
    })))
    .await
    .unwrap();
    drop(send);
    inbox::run(
        Bootstrap::Secret(Zeroizing::new([1; 32])),
        None,
        peer.clone(),
        journal,
        broadcast::channel(4).0,
        receive,
    )
    .await;
    let reopened =
        DeliveryJournal::open(Some(directory.path()), &identity, &peer, Network::Regtest).unwrap();
    let connection = rusqlite::Connection::open(path).unwrap();
    let count: u64 = connection
        .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
    drop(reopened);
}

#[tokio::test]
async fn expired_queue_entry_never_reserves_creation() {
    let directory = tempfile::tempdir().unwrap();
    let identity = identity_from_secret(&[1; 32]);
    let peer = identity_from_secret(&[2; 32]);
    let journal =
        DeliveryJournal::open(Some(directory.path()), &identity, &peer, Network::Regtest).unwrap();
    let path = journal.directory.as_ref().unwrap().join("replies.sqlite3");
    let (send, receive) = mpsc::channel(8);
    let quote_id = Uuid::new_v4();
    let (reply, retained) = oneshot::channel();
    send.send(WorkerCommand::Request(Box::new(Command {
        request: creation(quote_id),
        key: ReplyKey::Accept(quote_id),
        reply,
        deadline: Instant::now(),
    })))
    .await
    .unwrap();
    drop(send);
    inbox::run(
        Bootstrap::Secret(Zeroizing::new([1; 32])),
        None,
        peer,
        journal,
        broadcast::channel(4).0,
        receive,
    )
    .await;
    assert!(retained.await.is_err());
    let connection = rusqlite::Connection::open(path).unwrap();
    let count: u64 = connection
        .query_row("SELECT COUNT(*) FROM requests", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn ambiguous_creation_intent_preserves_original_request_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let quote_id = Uuid::new_v4();
    let request = creation(quote_id);
    let journal =
        DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest).unwrap();
    journal.prepare(&request).unwrap();
    drop(journal);
    let reopened =
        DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest).unwrap();
    reopened.prepare(&request).unwrap();
    let mut changed = creation(quote_id);
    if let SwapMessage::SwapRequest(request) = &mut changed {
        request.payment_hash_hex = hex::encode([3; 32]);
    }
    assert!(matches!(reopened.prepare(&changed), Err(Error::Conflict)));
    reopened.prepare(&creation(Uuid::new_v4())).unwrap();
}
