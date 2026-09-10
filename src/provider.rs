//! A dedicated Pubky inbox serializes request/reply exchanges on its own runtime.

use crate::{Error, Result};
use async_trait::async_trait;
use pubky_transport::Transport;
use std::{sync::Arc, time::Duration};
use swap_common::messages::*;
use tokio::{
    sync::{mpsc, oneshot},
    time::{sleep, Instant},
};
use uuid::Uuid;
use zeroize::Zeroizing;

#[async_trait]
pub trait Provider: Send + Sync {
    fn identity(&self) -> String;
    fn provider_key(&self) -> String;
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
    commands: mpsc::Sender<Command>,
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
}

impl PubkyProvider {
    pub async fn new(
        transport: Arc<Transport>,
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        Self::start(Bootstrap::Existing(transport), provider, timeout).await
    }

    /// Open a provider handle for an already registered identity without network access.
    /// Sign-in is deferred until a request and retried after failures on the inbox runtime.
    /// With `rendezvous`, the same identity rings the provider before every request.
    pub async fn from_secret_key(
        secret: [u8; 32],
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        Self::start(Bootstrap::Secret(Zeroizing::new(secret)), provider, timeout).await
    }

    async fn start(bootstrap: Bootstrap, provider: String, timeout: Duration) -> Result<Self> {
        let provider = canonical_pubky(&provider)?;
        if timeout.is_zero() || timeout > Duration::from_secs(300) {
            return Err(Error::Invalid(
                "request timeout must be positive and at most 300 seconds",
            ));
        }
        let (commands, receive) = mpsc::channel::<Command>(8);
        let identity = match &bootstrap {
            Bootstrap::Existing(transport) => transport.public_key_string(),
            Bootstrap::Secret(secret) => identity_from_secret(secret),
        };
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
                runtime.block_on(run_inbox(bootstrap, peer, receive));
            })
            .map_err(|_| Error::Provider)?;
        Ok(Self {
            identity,
            provider,
            timeout,
            commands,
        })
    }

    async fn request(&self, request: SwapMessage, key: ReplyKey) -> Result<SwapMessage> {
        let (reply, receive) = oneshot::channel();
        let deadline = Instant::now() + self.timeout;
        self.commands
            .try_send(Command {
                request,
                key,
                reply,
                deadline,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Error::Busy,
                mpsc::error::TrySendError::Closed(_) => Error::Provider,
            })?;
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

async fn initialize_transport(bootstrap: &Bootstrap, peer: &str) -> Result<Arc<Transport>> {
    let transport = match bootstrap {
        Bootstrap::Existing(transport) => transport.clone(),
        Bootstrap::Secret(secret) => Arc::new(
            Transport::from_secret_key(**secret)
                .await
                .map_err(|_| Error::Provider)?,
        ),
    };
    transport.pin_peer(peer.to_owned());
    transport
        .mark_conversation_seen(peer)
        .await
        .map_err(|_| Error::Provider)?;
    Ok(transport)
}

async fn run_inbox(bootstrap: Bootstrap, peer: String, mut receive: mpsc::Receiver<Command>) {
    let mut transport = None;
    while let Some(mut command) = receive.recv().await {
        if command.reply.is_closed() || Instant::now() >= command.deadline {
            continue;
        }
        let operation = async {
            if transport.is_none() {
                transport = Some(initialize_transport(&bootstrap, &peer).await?);
            }
            let secret = match &bootstrap {
                Bootstrap::Secret(secret) => Some(&**secret),
                Bootstrap::Existing(_) => None,
            };
            exchange(
                transport.as_ref().ok_or(Error::Provider)?,
                &peer,
                secret,
                command.deadline,
                &command.request,
                command.key,
            )
            .await
        };
        let result = tokio::select! {
            _ = command.reply.closed() => continue,
            result = tokio::time::timeout_at(command.deadline, operation) => {
                result.unwrap_or(Err(Error::Provider))
            },
        };
        if matches!(result, Err(Error::Provider)) {
            transport = None;
        }
        let _ = command.reply.send(result);
    }
}

async fn exchange(
    transport: &Transport,
    peer: &str,
    secret: Option<&[u8; 32]>,
    deadline: Instant,
    request: &SwapMessage,
    key: ReplyKey,
) -> Result<SwapMessage> {
    #[cfg(feature = "rendezvous")]
    if let Some(secret) = secret {
        tokio::time::timeout_at(deadline, pubky_transport::p2p::ring_provider(*secret, peer))
            .await
            .map_err(|_| Error::Provider)?
            .map_err(|_| Error::Provider)?;
    }
    #[cfg(not(feature = "rendezvous"))]
    let _ = secret;
    tokio::time::timeout_at(deadline, transport.send(peer, request))
        .await
        .map_err(|_| Error::Provider)?
        .map_err(|_| Error::Provider)?;
    loop {
        let received =
            tokio::time::timeout_at(deadline, transport.receive_from::<SwapMessage>(peer))
                .await
                .map_err(|_| Error::Provider)?
                .map_err(|_| Error::Provider)?;
        if let Some(reply) = received.into_iter().find(|message| key.matches(message)) {
            if let SwapMessage::Reject(ref rejection) = reply {
                if matches!(key, ReplyKey::Snapshot(_))
                    && rejection.code.as_deref() == Some("not_found")
                {
                    return Err(Error::NotFound);
                }
                return Err(Error::Provider);
            }
            return Ok(reply);
        }
        if Instant::now() >= deadline {
            return Err(Error::Provider);
        }
        sleep(Duration::from_millis(500)).await;
    }
}

#[async_trait]
impl Provider for PubkyProvider {
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
