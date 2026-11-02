use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

const SCOPE: &str = "/pub/bitkit.to/bitkit/wallet/";

fn token(owner: &str, marker: &str) -> String {
    let claims = serde_json::json!({
        "iss": owner, "client_id": "bitkit.to", "caps": [], "cnf": owner,
        "jti": marker, "iat": 0, "exp": 1,
    });
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
    format!("pubky-grant-credential-v1:{owner}:fixture:e30.{payload}.fixture")
}

fn credentials(marker: &str, scope: &str) -> session::SessionBootstrap {
    let owner = identity_from_secret(&[1; 32]);
    session::SessionBootstrap::new(
        token(&owner, marker),
        owner.clone(),
        scope.into(),
        [3; 32],
        &owner,
    )
    .unwrap()
}

fn worker(directory: &std::path::Path) -> Worker {
    let credentials = credentials("old", SCOPE);
    let peer = credentials.owner.clone();
    let journal = DeliveryJournal::open(
        Some(directory),
        &credentials.transport_identity(),
        &peer,
        Network::Regtest,
    )
    .unwrap();
    Worker {
        bootstrap: Bootstrap::Session(credentials),
        transport: None,
        peer,
        journal,
        events: broadcast::channel(4).0,
        rpc: None,
        session: None,
    }
}

fn renewal(scope: &str, timeout: Duration) -> (SessionRenewal, oneshot::Receiver<Result<()>>) {
    let (reply, receive) = oneshot::channel();
    (
        SessionRenewal {
            credentials: credentials("new", scope),
            reply,
            deadline: Instant::now() + timeout,
        },
        receive,
    )
}

fn assert_current(worker: &Worker, marker: &str, directory: &std::path::Path) {
    let Bootstrap::Session(current) = &worker.bootstrap else {
        panic!("missing session");
    };
    assert!(current.has_token(&token(&current.owner, marker)));
    assert!(matches!(
        DeliveryJournal::open(
            Some(directory),
            &current.transport_identity(),
            &worker.peer,
            Network::Regtest,
        ),
        Err(Error::Busy)
    ));
}

#[tokio::test]
async fn renewal_preserves_journal_reservation_and_emits_wakeup() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let request = SwapMessage::SwapRequest(SwapRequest {
        script_type: SwapScript::TaprootBoltz,
        quote_id: Uuid::new_v4(),
        client_pkarr: bootstrap_identity(&worker.bootstrap),
        direction: swap_common::SwapDirection::Reverse,
        payment_hash_hex: hex::encode([2; 32]),
        client_claim_pubkey_hex: Some("claim-key".into()),
        client_refund_pubkey_hex: None,
        invoice: None,
    });
    worker.journal.prepare(&request).unwrap();
    let mut events = worker.events.subscribe();
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    worker.renew_session(command).await;
    reply.await.unwrap().unwrap();
    assert_current(&worker, "new", directory.path());
    events.try_recv().unwrap();
    worker.journal.prepare(&request).unwrap();
    let mut changed = request.clone();
    let SwapMessage::SwapRequest(ref mut changed) = changed else {
        unreachable!()
    };
    changed.payment_hash_hex = hex::encode([4; 32]);
    assert!(matches!(
        worker
            .journal
            .prepare(&SwapMessage::SwapRequest(changed.clone())),
        Err(Error::Conflict)
    ));
}

#[tokio::test]
async fn failed_replacement_keeps_prior_credentials_and_lock() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    worker
        .renew_session_with(command, async { Err(Error::Provider) })
        .await;
    assert!(matches!(reply.await.unwrap(), Err(Error::Provider)));
    assert_current(&worker, "old", directory.path());
}

#[tokio::test]
async fn cancelled_queued_renewal_never_validates_or_changes_credentials() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    drop(reply);
    worker
        .renew_session_with(command, async {
            panic!("cancelled renewal must not validate")
        })
        .await;
    assert_current(&worker, "old", directory.path());
}

#[tokio::test]
async fn cancellation_during_validation_keeps_prior_credentials_and_lock() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    let (entered, entered_rx) = oneshot::channel();
    {
        let renewing = worker.renew_session_with(command, async {
            entered.send(()).unwrap();
            std::future::pending().await
        });
        tokio::pin!(renewing);
        tokio::select! {
            _ = &mut renewing => panic!("validation must wait"),
            _ = entered_rx => {}
        }
        drop(reply);
        renewing.await;
    }
    assert_current(&worker, "old", directory.path());
}

#[tokio::test]
async fn validation_timeout_keeps_prior_credentials_and_lock() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_millis(5));
    worker
        .renew_session_with(command, std::future::pending())
        .await;
    assert!(matches!(reply.await.unwrap(), Err(Error::Provider)));
    assert_current(&worker, "old", directory.path());
}

#[tokio::test]
async fn scope_change_is_rejected_before_validation() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(
        "/pub/staging.bitkit.to/bitkit/wallet/",
        Duration::from_secs(1),
    );
    worker
        .renew_session_with(command, async {
            panic!("changed binding must not validate")
        })
        .await;
    assert!(matches!(reply.await.unwrap(), Err(Error::Conflict)));
    assert_current(&worker, "old", directory.path());
}

#[tokio::test]
async fn public_renewal_reuses_worker_and_rejects_changed_wallet() {
    let directory = tempfile::tempdir().unwrap();
    let owner = identity_from_secret(&[1; 32]);
    let provider = PubkyProvider::from_session_with_storage(
        token(&owner, "old"),
        owner.clone(),
        SCOPE.into(),
        [3; 32],
        owner.clone(),
        Duration::from_secs(1),
        directory.path().into(),
        Network::Regtest,
    )
    .await
    .unwrap();
    let identity = provider.identity();
    provider
        .renew_session(token(&owner, "new"), owner.clone(), SCOPE.into(), [3; 32])
        .await
        .unwrap();
    assert_eq!(provider.identity(), identity);
    assert!(matches!(
        provider
            .renew_session(token(&owner, "new"), owner.clone(), SCOPE.into(), [4; 32])
            .await,
        Err(Error::Conflict)
    ));
    assert!(matches!(
        DeliveryJournal::open(Some(directory.path()), &identity, &owner, Network::Regtest),
        Err(Error::Busy)
    ));
    drop(provider);
    session::open_journal(directory.path(), &identity, &owner, Network::Regtest)
        .await
        .unwrap();
}

#[tokio::test]
async fn cancellation_after_commit_keeps_renewed_credentials_and_lock() {
    let directory = tempfile::tempdir().unwrap();
    let mut worker = worker(directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    worker.renew_session(command).await;
    drop(reply);
    assert_current(&worker, "new", directory.path());
    let (command, reply) = renewal(SCOPE, Duration::from_secs(1));
    worker.renew_session(command).await;
    reply.await.unwrap().unwrap();
}

#[tokio::test]
async fn root_provider_does_not_accept_session_renewal() {
    let owner = identity_from_secret(&[1; 32]);
    let provider = PubkyProvider::from_secret_key([2; 32], owner.clone(), Duration::from_secs(1))
        .await
        .unwrap();
    assert!(matches!(
        provider
            .renew_session(token(&owner, "new"), owner, SCOPE.into(), [3; 32])
            .await,
        Err(Error::Invalid(_))
    ));
}
