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

impl PubkyProvider {
    pub async fn new(
        transport: Arc<Transport>,
        provider: String,
        timeout: Duration,
    ) -> Result<Self> {
        transport.pin_peer(provider.clone());
        tokio::time::timeout(timeout, transport.mark_conversation_seen(&provider))
            .await
            .map_err(|_| Error::Provider)?
            .map_err(|_| Error::Provider)?;
        let identity = transport.public_key_string();
        let peer = provider.clone();
        let (commands, mut receive) = mpsc::channel::<Command>(8);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| Error::Provider)?;
        std::thread::Builder::new()
            .name("pubky-inbox".into())
            .spawn(move || {
                runtime.block_on(async move {
                    while let Some(command) = receive.recv().await {
                        if command.reply.is_closed() {
                            continue;
                        }
                        let result = exchange(&transport, &peer, timeout, &command).await;
                        let _ = command.reply.send(result);
                    }
                })
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
        self.commands
            .try_send(Command {
                request,
                key,
                reply,
            })
            .map_err(|_| Error::Busy)?;
        tokio::time::timeout(self.timeout, receive)
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

async fn exchange(
    transport: &Transport,
    peer: &str,
    timeout: Duration,
    command: &Command,
) -> Result<SwapMessage> {
    let deadline = Instant::now() + timeout;
    tokio::time::timeout_at(deadline, transport.send(peer, &command.request))
        .await
        .map_err(|_| Error::Provider)?
        .map_err(|_| Error::Provider)?;
    loop {
        let received =
            tokio::time::timeout_at(deadline, transport.receive_from::<SwapMessage>(peer))
                .await
                .map_err(|_| Error::Provider)?
                .map_err(|_| Error::Provider)?;
        if let Some(reply) = received.into_iter().find(|m| command.key.matches(m)) {
            if let SwapMessage::Reject(ref rejection) = reply {
                if matches!(command.key, ReplyKey::Snapshot(_))
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
