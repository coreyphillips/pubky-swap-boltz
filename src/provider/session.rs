//! Ring sessions authorize a device transport key without exposing the account secret.

use super::*;
use pubky_transport::session_rpc::{self, SessionClient, SessionRequest};
use std::path::Path;

const JOURNAL_REOPEN_TIMEOUT: Duration = Duration::from_secs(5);
const JOURNAL_REOPEN_INTERVAL: Duration = Duration::from_millis(25);

pub(super) async fn open_journal(
    directory: &Path,
    identity: &str,
    provider: &str,
    network: Network,
) -> Result<DeliveryJournal> {
    open_journal_until(
        directory,
        identity,
        provider,
        network,
        Instant::now() + JOURNAL_REOPEN_TIMEOUT,
        JOURNAL_REOPEN_INTERVAL,
    )
    .await
}

async fn open_journal_until(
    directory: &Path,
    identity: &str,
    provider: &str,
    network: Network,
    deadline: Instant,
    interval: Duration,
) -> Result<DeliveryJournal> {
    loop {
        match DeliveryJournal::open(Some(directory), identity, provider, network) {
            Err(Error::Busy) => {
                // The previous session worker owns this lock until its endpoint closes.
                // Yield without creating a replacement worker or changing its journal.
                let next = (Instant::now() + interval).min(deadline);
                tokio::time::sleep_until(next).await;
                if Instant::now() >= deadline {
                    return Err(Error::Busy);
                }
            }
            result => return result,
        }
    }
}

#[derive(Clone)]
pub(super) struct SessionBootstrap {
    token: Zeroizing<String>,
    pub owner: String,
    scope: String,
    transport_secret: Zeroizing<[u8; 32]>,
}

impl SessionBootstrap {
    pub fn new(
        token: String,
        owner: String,
        scope: String,
        wallet_secret: [u8; 32],
        provider: &str,
    ) -> Result<Self> {
        let wallet_secret = Zeroizing::new(wallet_secret);
        let token = Zeroizing::new(token);
        let owner = canonical_pubky(&owner)?;
        let token_owner = session_rpc::session_account_hint(&token)
            .map_err(|_| Error::Invalid("invalid Pubky grant credential"))?;
        if token_owner != owner {
            return Err(Error::Invalid("Pubky session account mismatch"));
        }
        let transport_secret = Zeroizing::new(
            session_rpc::derive_transport_secret(&wallet_secret, &owner, provider, &scope)
                .map_err(|_| Error::Validation)?,
        );
        session_rpc::authorization_path(&scope, &identity_from_secret(&transport_secret))
            .map_err(|_| Error::Invalid("invalid application swap scope"))?;
        Ok(Self {
            token,
            owner,
            scope,
            transport_secret,
        })
    }

    pub fn transport_identity(&self) -> String {
        identity_from_secret(&self.transport_secret)
    }

    #[cfg(test)]
    pub(super) fn has_token(&self, token: &str) -> bool {
        self.token.as_str() == token
    }

    pub fn same_binding(&self, other: &Self) -> bool {
        self.owner == other.owner
            && self.scope == other.scope
            && self.transport_secret == other.transport_secret
    }

    pub async fn connect_client(&self, provider: &str) -> Result<SessionClient> {
        SessionClient::connect(
            &self.token,
            &self.owner,
            &self.scope,
            provider,
            &identity_from_secret(&self.transport_secret),
        )
        .await
        .map_err(|_| Error::Provider)
    }

    pub async fn connect(&self, provider: &str) -> Result<SessionTransport> {
        let client = self.connect_client(provider).await?;
        Ok(SessionTransport {
            client,
            owner: self.owner.clone(),
            scope: self.scope.clone(),
            rpc: pubky_transport::p2p::SessionRpcClient::new(*self.transport_secret, provider)
                .await
                .map_err(|_| Error::Provider)?,
        })
    }
}

pub(super) struct SessionTransport {
    client: SessionClient,
    owner: String,
    scope: String,
    rpc: pubky_transport::p2p::SessionRpcClient,
}

impl SessionTransport {
    pub fn replace_client(&mut self, client: SessionClient) {
        self.client = client;
    }

    pub async fn exchange(&self, message: &SwapMessage, key: ReplyKey) -> Result<SwapMessage> {
        self.client.authorize().await.map_err(|_| Error::Provider)?;
        let request = SessionRequest {
            owner: self.owner.clone(),
            scope: self.scope.clone(),
            message: serde_json::to_value(message).map_err(|_| Error::Validation)?,
        };
        let bytes = self
            .rpc
            .request(&request)
            .await
            .map_err(|_| Error::Provider)?;
        let reply: SwapMessage = serde_json::from_slice(&bytes).map_err(|_| Error::Validation)?;
        validate_reply(reply, key)
    }

    pub async fn close(&self) {
        self.rpc.close().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grant_fixture(owner: &str) -> String {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        let claims = serde_json::json!({
            "iss": owner, "client_id": "bitkit.to", "caps": [], "cnf": owner,
            "jti": "fixture", "iat": 0, "exp": 1,
        });
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
        format!("pubky-grant-credential-v1:{owner}:fixture:e30.{payload}.fixture")
    }

    fn saved_creation() -> SwapMessage {
        SwapMessage::SwapRequest(SwapRequest {
            script_type: SwapScript::TaprootBoltz,
            quote_id: Uuid::new_v4(),
            client_pkarr: identity_from_secret(&[1; 32]),
            direction: swap_common::SwapDirection::Reverse,
            payment_hash_hex: hex::encode([2; 32]),
            client_claim_pubkey_hex: Some("claim-key".into()),
            client_refund_pubkey_hex: None,
            invoice: None,
        })
    }

    fn assert_creation_preserved(journal: &DeliveryJournal, request: &SwapMessage) {
        journal.prepare(request).unwrap();
        let mut changed = request.clone();
        let SwapMessage::SwapRequest(ref mut request) = changed else {
            panic!("expected creation request");
        };
        request.payment_hash_hex = hex::encode([3; 32]);
        assert!(matches!(journal.prepare(&changed), Err(Error::Conflict)));
    }

    #[tokio::test]
    async fn session_reopen_waits_for_previous_owner_and_preserves_creation() {
        let directory = tempfile::tempdir().unwrap();
        let held = DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
            .unwrap();
        let request = saved_creation();
        held.prepare(&request).unwrap();
        let reopen = open_journal_until(
            directory.path(),
            "owner",
            "peer",
            Network::Regtest,
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(1),
        );
        tokio::pin!(reopen);
        tokio::select! {
            _ = &mut reopen => panic!("reopened while the previous owner still held the lock"),
            _ = sleep(Duration::from_millis(5)) => {}
        }
        drop(held);
        let reopened = reopen.await.unwrap();
        assert_creation_preserved(&reopened, &request);
    }

    #[tokio::test]
    async fn session_reopen_timeout_preserves_previous_owner_and_creation() {
        let directory = tempfile::tempdir().unwrap();
        let held = DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
            .unwrap();
        let request = saved_creation();
        held.prepare(&request).unwrap();
        let result = open_journal_until(
            directory.path(),
            "owner",
            "peer",
            Network::Regtest,
            Instant::now() + Duration::from_millis(10),
            Duration::from_millis(1),
        )
        .await;
        assert!(matches!(result, Err(Error::Busy)));
        assert_creation_preserved(&held, &request);
        assert!(matches!(
            DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest),
            Err(Error::Busy)
        ));
        drop(held);
        let reopened = open_journal(directory.path(), "owner", "peer", Network::Regtest)
            .await
            .unwrap();
        assert_creation_preserved(&reopened, &request);
    }

    #[tokio::test]
    async fn cancelling_session_reopen_preserves_previous_owner_and_creation() {
        let directory = tempfile::tempdir().unwrap();
        let held = DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
            .unwrap();
        let request = saved_creation();
        held.prepare(&request).unwrap();
        let cancelled = tokio::time::timeout(
            Duration::from_millis(5),
            open_journal(directory.path(), "owner", "peer", Network::Regtest),
        )
        .await;
        assert!(cancelled.is_err());
        assert_creation_preserved(&held, &request);
        drop(held);
        let reopened = open_journal(directory.path(), "owner", "peer", Network::Regtest)
            .await
            .unwrap();
        assert_creation_preserved(&reopened, &request);
    }

    #[tokio::test]
    async fn session_reopen_propagates_non_busy_errors_without_retrying() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            open_journal(file.path(), "owner", "peer", Network::Regtest),
        )
        .await
        .expect("storage errors must not consume the five-second busy retry deadline");
        assert!(matches!(result, Err(Error::Storage)));
    }

    #[tokio::test]
    async fn session_bootstrap_preserves_ring_account_without_network_access() {
        let owner = identity_from_secret(&[1; 32]);
        let provider = identity_from_secret(&[2; 32]);
        let handle = PubkyProvider::from_session(
            grant_fixture(&owner),
            owner.clone(),
            "/pub/bitkit.to/bitkit/wallet/".into(),
            [3; 32],
            provider.clone(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_ne!(handle.identity(), owner);
        assert_eq!(
            handle.identity(),
            identity_from_secret(
                &session_rpc::derive_transport_secret(
                    &[3; 32],
                    &owner,
                    &provider,
                    "/pub/bitkit.to/bitkit/wallet/"
                )
                .unwrap()
            )
        );
        assert_eq!(handle.provider_key(), provider);
    }

    #[test]
    fn session_bootstrap_rejects_account_mismatch_and_invalid_scope() {
        let owner = identity_from_secret(&[1; 32]);
        let other = identity_from_secret(&[2; 32]);
        assert!(SessionBootstrap::new(
            grant_fixture(&other),
            owner.clone(),
            "/pub/bitkit.to/bitkit/wallet/".into(),
            [3; 32],
            &other
        )
        .is_err());
        assert!(SessionBootstrap::new(
            grant_fixture(&owner),
            owner,
            "/pub/".into(),
            [3; 32],
            &other
        )
        .is_err());
    }
}
