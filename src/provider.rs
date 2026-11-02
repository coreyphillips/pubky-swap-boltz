//! A dedicated Pubky inbox reuses connections and persists delivery before acknowledgement.

use crate::{Error, Result};
use async_trait::async_trait;
use bitcoin::Network;
use pubky_transport::Transport;
pub use pubky_transport::{DeliveryOperation, OutboxStatus};
use std::{path::PathBuf, sync::Arc, time::Duration};
use swap_common::messages::*;
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    time::{sleep, Instant},
};
use uuid::Uuid;
use zeroize::Zeroizing;

mod delivery;
#[cfg(test)]
mod delivery_tests;
mod inbox;
#[cfg(feature = "rendezvous")]
mod session;
use delivery::DeliveryJournal;

#[async_trait]
pub trait Provider: Send + Sync {
    fn identity(&self) -> String;
    fn provider_key(&self) -> String;
    fn subscribe_delivery(&self) -> Option<broadcast::Receiver<()>> {
        None
    }
    async fn offer(&self) -> Result<SwapOffer>;
    async fn quote(&self, request: QuoteRequest) -> Result<Quote>;
    async fn create(&self, request: SwapRequest) -> Result<SwapAccept>;
    async fn snapshot(&self, swap_id: Uuid) -> Result<SwapStatusSnapshot>;
    async fn snapshot_by_quote(&self, _quote_id: Uuid) -> Result<SwapStatusSnapshot> {
        Err(Error::Provider)
    }
}

pub struct PubkyProvider {
    identity: String,
    provider: String,
    timeout: Duration,
    commands: mpsc::Sender<WorkerCommand>,
    transport: Option<Arc<Transport>>,
    events: broadcast::Sender<()>,
}

enum WorkerCommand {
    Request(Box<Command>),
    #[cfg(feature = "rendezvous")]
    RenewSession(SessionRenewal),
}

#[cfg(feature = "rendezvous")]
struct SessionRenewal {
    credentials: session::SessionBootstrap,
    reply: oneshot::Sender<Result<()>>,
    deadline: Instant,
}

struct Command {
    request: SwapMessage,
    key: ReplyKey,
    reply: oneshot::Sender<Result<SwapMessage>>,
    deadline: Instant,
}

#[derive(Clone, Copy)]
enum ReplyKey {
    Offer(Uuid),
    Quote(Uuid),
    Accept(Uuid),
    Snapshot(Uuid),
}

impl ReplyKey {
    fn id(self) -> String {
        match self {
            Self::Offer(id) => format!("offer:{id}"),
            Self::Quote(id) => format!("quote:{id}"),
            Self::Accept(id) => format!("accept:{id}"),
            Self::Snapshot(id) => format!("snapshot:{id}"),
        }
    }
    fn matches(self, message: &SwapMessage) -> bool {
        match (self, message) {
            (Self::Offer(id), SwapMessage::Offer(o)) => o.request_id == Some(id),
            (Self::Quote(id), SwapMessage::Quote(q)) => q.request_id == Some(id),
            (Self::Accept(id), SwapMessage::SwapAccept(a)) => a.quote_id == id,
            (Self::Snapshot(id), SwapMessage::SwapStatusSnapshot(s)) => s.request_id == Some(id),
            (Self::Accept(id), SwapMessage::Reject(r)) => r.quote_id == Some(id),
            (Self::Offer(id) | Self::Quote(id) | Self::Snapshot(id), SwapMessage::Reject(r)) => {
                r.request_id == Some(id)
            }
            _ => false,
        }
    }
}

/// Normalize a provider identity before it is used in requests or persistent bindings.
pub fn canonical_pubky(value: &str) -> Result<String> {
    pubky_transport::canonical_pubky(value)
        .map_err(|_| Error::Invalid("invalid provider public key"))
}

pub fn identity_from_secret(secret: &[u8; 32]) -> String {
    pubky_transport::identity_from_secret(secret)
}

enum Bootstrap {
    Existing(Arc<Transport>),
    Secret(Zeroizing<[u8; 32]>),
    #[cfg(feature = "rendezvous")]
    Session(session::SessionBootstrap),
}

impl PubkyProvider {
    /// Open a volatile provider for callers that manage the supplied transport's journals.
    pub async fn new(
        transport: Arc<Transport>,
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        Self::start(
            Bootstrap::Existing(transport),
            provider,
            timeout,
            None,
            Network::Bitcoin,
        )
        .await
    }

    /// Restore scoped delivery state for a caller-owned transport identity.
    pub async fn new_with_storage(
        transport: Transport,
        provider: String,
        timeout: Duration,
        directory: PathBuf,
        network: Network,
    ) -> Result<Self> {
        let peer = canonical_pubky(&provider)?;
        let journal = DeliveryJournal::open(
            Some(&directory),
            &transport.public_key_string(),
            &peer,
            network,
        )?;
        let transport = attach_journals(transport, &journal)?;
        Self::spawn(
            Bootstrap::Existing(Arc::new(transport)),
            peer,
            timeout,
            journal,
        )
        .await
    }

    /// Open an identity without network access. Use the storage variant for restart recovery.
    pub async fn from_secret_key(
        secret: [u8; 32],
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        Self::start(
            Bootstrap::Secret(Zeroizing::new(secret)),
            provider,
            timeout,
            None,
            Network::Bitcoin,
        )
        .await
    }

    /// Restore protected journals separated by identity, Bitcoin network and provider.
    pub async fn from_secret_key_with_storage(
        secret: [u8; 32],
        provider: String,
        timeout: Duration,
        directory: PathBuf,
        network: Network,
    ) -> Result<Self> {
        Self::start(
            Bootstrap::Secret(Zeroizing::new(secret)),
            provider,
            timeout,
            Some(directory),
            network,
        )
        .await
    }

    /// Use a Ring-authorized session without importing the account's private key.
    #[cfg(feature = "rendezvous")]
    pub async fn from_session(
        token: String,
        owner: String,
        scope: String,
        wallet_secret: [u8; 32],
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        let credentials =
            session::SessionBootstrap::new(token, owner, scope, wallet_secret, &provider)?;
        Self::start(
            Bootstrap::Session(credentials),
            provider,
            timeout,
            None,
            Network::Bitcoin,
        )
        .await
    }

    /// Restore saved session requests and replies in the same account and network scope.
    #[cfg(feature = "rendezvous")]
    #[allow(clippy::too_many_arguments)]
    pub async fn from_session_with_storage(
        token: String,
        owner: String,
        scope: String,
        wallet_secret: [u8; 32],
        provider: String,
        timeout: Duration,
        directory: PathBuf,
        network: Network,
    ) -> Result<Self> {
        let credentials =
            session::SessionBootstrap::new(token, owner, scope, wallet_secret, &provider)?;
        let provider = canonical_pubky(&provider)?;
        let journal = session::open_journal(
            &directory,
            &credentials.transport_identity(),
            &provider,
            network,
        )
        .await?;
        Self::spawn(Bootstrap::Session(credentials), provider, timeout, journal).await
    }

    /// Renew the same session binding without interrupting its journal or RPC endpoint.
    /// Failed or cancelled validation retains the previous credentials. Once acknowledged,
    /// future requests use the renewed grant; cancellation after commit leaves it active.
    #[cfg(feature = "rendezvous")]
    pub async fn renew_session(
        &self,
        token: String,
        owner: String,
        scope: String,
        wallet_secret: [u8; 32],
    ) -> Result<()> {
        if self.transport.is_some() {
            return Err(Error::Invalid(
                "only session providers support grant renewal",
            ));
        }
        let credentials =
            session::SessionBootstrap::new(token, owner, scope, wallet_secret, &self.provider)?;
        if credentials.transport_identity() != self.identity {
            return Err(Error::Conflict);
        }
        let (reply, receive) = oneshot::channel();
        let deadline = Instant::now() + self.timeout;
        tokio::time::timeout_at(
            deadline,
            self.commands
                .send(WorkerCommand::RenewSession(SessionRenewal {
                    credentials,
                    reply,
                    deadline,
                })),
        )
        .await
        .map_err(|_| Error::Busy)?
        .map_err(|_| Error::Provider)?;
        tokio::time::timeout_at(deadline, receive)
            .await
            .map_err(|_| Error::Provider)?
            .map_err(|_| Error::Provider)?
    }

    /// Inspect sanitized publication and cleanup failure state without network access.
    pub fn delivery_status(&self) -> Result<Vec<OutboxStatus>> {
        self.transport
            .as_ref()
            .map(|transport| transport.outbox_status().map_err(|_| Error::Storage))
            .transpose()
            .map(Option::unwrap_or_default)
    }

    /// Deliberately resume one paused operation after its cause has been corrected.
    pub fn retry_delivery(&self, id: &str, operation: DeliveryOperation) -> Result<()> {
        self.transport
            .as_ref()
            .ok_or(Error::Unsupported)?
            .retry_outbox(id, operation)
            .map_err(|_| Error::Storage)?;
        let _ = self.events.send(());
        Ok(())
    }

    async fn start(
        bootstrap: Bootstrap,
        provider: String,
        timeout: Duration,
        directory: Option<PathBuf>,
        network: Network,
    ) -> Result<Self> {
        let peer = canonical_pubky(&provider)?;
        let identity = bootstrap_identity(&bootstrap);
        let journal = DeliveryJournal::open(directory.as_deref(), &identity, &peer, network)?;
        Self::spawn(bootstrap, peer, timeout, journal).await
    }

    async fn spawn(
        bootstrap: Bootstrap,
        provider: String,
        timeout: Duration,
        journal: DeliveryJournal,
    ) -> Result<Self> {
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(Error::Invalid(
                "request timeout must be positive and at most 300 seconds",
            ));
        }
        let identity = bootstrap_identity(&bootstrap);
        let transport = match &bootstrap {
            Bootstrap::Existing(transport) => Some(transport.clone()),
            Bootstrap::Secret(secret) => Some(Arc::new(attach_journals(
                Transport::unsigned(**secret).map_err(|_| Error::Provider)?,
                &journal,
            )?)),
            #[cfg(feature = "rendezvous")]
            Bootstrap::Session(_) => None,
        };
        if let Some(transport) = &transport {
            transport.pin_peer(provider.clone());
        }
        let (commands, receive) = mpsc::channel::<WorkerCommand>(8);
        let events = broadcast::channel(64).0;
        let worker_events = events.clone();
        let worker_transport = transport.clone();
        let peer = provider.clone();
        std::thread::Builder::new()
            .name("pubky-inbox".into())
            .spawn(move || {
                let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                else {
                    return;
                };
                runtime.block_on(inbox::run(
                    bootstrap,
                    worker_transport,
                    peer,
                    journal,
                    worker_events,
                    receive,
                ));
            })
            .map_err(|_| Error::Provider)?;
        Ok(Self {
            identity,
            provider,
            timeout,
            commands,
            transport,
            events,
        })
    }

    async fn request(&self, request: SwapMessage, key: ReplyKey) -> Result<SwapMessage> {
        let (reply, receive) = oneshot::channel();
        let deadline = Instant::now() + self.timeout;
        // Waiting for a queue slot consumes the same deadline and preserves the caller's request.
        tokio::time::timeout_at(
            deadline,
            self.commands.send(WorkerCommand::Request(Box::new(Command {
                request,
                key,
                reply,
                deadline,
            }))),
        )
        .await
        .map_err(|_| Error::Busy)?
        .map_err(|_| Error::Provider)?;
        tokio::time::timeout_at(deadline, receive)
            .await
            .map_err(|_| Error::Provider)?
            .map_err(|_| Error::Provider)?
    }

    async fn status_request(
        &self,
        swap_id: Option<Uuid>,
        quote_id: Option<Uuid>,
    ) -> Result<SwapStatusSnapshot> {
        let id = Uuid::new_v4();
        let message = SwapMessage::SwapStatusRequest(SwapStatusRequest {
            swap_id,
            quote_id,
            request_id: Some(id),
        });
        match self.request(message, ReplyKey::Snapshot(id)).await? {
            SwapMessage::SwapStatusSnapshot(snapshot) => Ok(snapshot),
            _ => Err(Error::Provider),
        }
    }
}

fn bootstrap_identity(bootstrap: &Bootstrap) -> String {
    match bootstrap {
        Bootstrap::Existing(transport) => transport.public_key_string(),
        Bootstrap::Secret(secret) => identity_from_secret(secret),
        #[cfg(feature = "rendezvous")]
        Bootstrap::Session(credentials) => credentials.transport_identity(),
    }
}

fn attach_journals(transport: Transport, journal: &DeliveryJournal) -> Result<Transport> {
    let Some(directory) = &journal.directory else {
        return Ok(transport);
    };
    transport
        .with_receive_journal(directory.join("inbox.json"))
        .and_then(|transport| transport.with_outbox(directory.join("outbox.json")))
        .map_err(|_| Error::Storage)
}

fn validate_reply(reply: SwapMessage, key: ReplyKey) -> Result<SwapMessage> {
    if !key.matches(&reply) {
        return Err(Error::Validation);
    }
    if let SwapMessage::Reject(rejection) = &reply {
        if matches!(key, ReplyKey::Snapshot(_)) && rejection.code.as_deref() == Some("not_found") {
            return Err(Error::NotFound);
        }
        return Err(Error::Provider);
    }
    Ok(reply)
}

#[async_trait]
impl Provider for PubkyProvider {
    fn subscribe_delivery(&self) -> Option<broadcast::Receiver<()>> {
        Some(self.events.subscribe())
    }
    fn identity(&self) -> String {
        self.identity.clone()
    }
    fn provider_key(&self) -> String {
        self.provider.clone()
    }
    async fn offer(&self) -> Result<SwapOffer> {
        let id = Uuid::new_v4();
        match self
            .request(
                SwapMessage::OfferRequest(OfferRequest {
                    request_id: Some(id),
                }),
                ReplyKey::Offer(id),
            )
            .await?
        {
            SwapMessage::Offer(o) => Ok(o),
            _ => Err(Error::Provider),
        }
    }
    async fn quote(&self, request: QuoteRequest) -> Result<Quote> {
        let id = request.request_id.ok_or(Error::Validation)?;
        match self
            .request(SwapMessage::QuoteRequest(request), ReplyKey::Quote(id))
            .await?
        {
            SwapMessage::Quote(q) => Ok(q),
            _ => Err(Error::Provider),
        }
    }
    async fn create(&self, request: SwapRequest) -> Result<SwapAccept> {
        let id = request.quote_id;
        match self
            .request(SwapMessage::SwapRequest(request), ReplyKey::Accept(id))
            .await?
        {
            SwapMessage::SwapAccept(a) => Ok(a),
            _ => Err(Error::Provider),
        }
    }
    async fn snapshot(&self, swap_id: Uuid) -> Result<SwapStatusSnapshot> {
        self.status_request(Some(swap_id), None).await
    }
    async fn snapshot_by_quote(&self, quote_id: Uuid) -> Result<SwapStatusSnapshot> {
        self.status_request(None, Some(quote_id)).await
    }
}

#[cfg(test)]
mod mobile_tests {
    use super::*;

    #[test]
    fn secret_bootstrap_can_cross_a_spawn_boundary() {
        fn assert_send<T: Send>(_: T) {}
        assert_send(PubkyProvider::from_secret_key(
            [1; 32],
            String::new(),
            Duration::from_secs(1),
        ));
    }

    #[tokio::test]
    async fn secret_bootstrap_is_available_without_a_homeserver_or_network() {
        let provider = PubkyProvider::from_secret_key(
            [1; 32],
            "pubky://q9x5sfjbpajdebk45b9jashgb86iem7rnwpmu16px3ens63xzwro".into(),
            Duration::from_secs(1),
        )
        .await
        .unwrap();
        assert_eq!(provider.identity(), identity_from_secret(&[1; 32]));
        assert_eq!(
            provider.provider_key(),
            "q9x5sfjbpajdebk45b9jashgb86iem7rnwpmu16px3ens63xzwro"
        );
    }

    #[tokio::test]
    async fn invalid_identity_fails_before_network_initialization() {
        assert!(matches!(
            PubkyProvider::from_secret_key([1; 32], "invalid".into(), Duration::from_secs(1)).await,
            Err(Error::Invalid(_))
        ));
    }
}
