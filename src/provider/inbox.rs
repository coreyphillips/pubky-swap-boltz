//! Request deadlines include queueing, connection attempts, delivery and receipt persistence.

use super::*;

pub(super) async fn run(
    bootstrap: Bootstrap,
    transport: Option<Arc<Transport>>,
    peer: String,
    journal: DeliveryJournal,
    events: broadcast::Sender<()>,
    mut receive: mpsc::Receiver<WorkerCommand>,
) {
    #[cfg(not(feature = "rendezvous"))]
    let _ = bootstrap;
    let mut worker = Worker {
        #[cfg(feature = "rendezvous")]
        bootstrap,
        transport,
        peer,
        journal,
        events,
        #[cfg(feature = "rendezvous")]
        rpc: None,
        #[cfg(feature = "rendezvous")]
        session: None,
    };
    let mut maintenance = tokio::time::interval(Duration::from_secs(5));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            command = receive.recv() => {
                let Some(command) = command else { break; };
                match command {
                    WorkerCommand::Request(command) => {
                        let mut command = *command;
                        if command.reply.is_closed() || Instant::now() >= command.deadline { continue; }
                        let result = tokio::select! {
                            _ = command.reply.closed() => continue,
                            result = tokio::time::timeout_at(command.deadline, worker.exchange(&command.request, command.key, command.deadline)) => result.unwrap_or(Err(Error::Provider)),
                        };
                        let _ = command.reply.send(result);
                    }
                    #[cfg(feature = "rendezvous")]
                    WorkerCommand::RenewSession(command) => worker.renew_session(command).await,
                }
            }
            _ = maintenance.tick() => {
                // Maintenance is bounded so queued callers retain most of their total deadline.
                let _ = tokio::time::timeout(Duration::from_secs(2), worker.maintain()).await;
            }
        }
    }
    worker.close().await;
}

struct Worker {
    #[cfg(feature = "rendezvous")]
    bootstrap: Bootstrap,
    transport: Option<Arc<Transport>>,
    peer: String,
    journal: DeliveryJournal,
    events: broadcast::Sender<()>,
    #[cfg(feature = "rendezvous")]
    rpc: Option<pubky_transport::p2p::DirectRpcClient>,
    #[cfg(feature = "rendezvous")]
    session: Option<session::SessionTransport>,
}

impl Worker {
    #[cfg(feature = "rendezvous")]
    async fn renew_session(&mut self, command: SessionRenewal) {
        let credentials = command.credentials.clone();
        let peer = self.peer.clone();
        let active = self.session.is_some();
        self.renew_session_with(command, async move {
            if active {
                credentials.connect_client(&peer).await.map(Some)
            } else {
                // Keep offline recovery lazy when no session has connected yet.
                Ok(None)
            }
        })
        .await;
    }

    #[cfg(feature = "rendezvous")]
    async fn renew_session_with(
        &mut self,
        mut command: SessionRenewal,
        prepare: impl std::future::Future<
            Output = Result<Option<pubky_transport::session_rpc::SessionClient>>,
        >,
    ) {
        if command.reply.is_closed() || Instant::now() >= command.deadline {
            return;
        }
        let Bootstrap::Session(current) = &self.bootstrap else {
            let _ = command.reply.send(Err(Error::Invalid(
                "only session providers support grant renewal",
            )));
            return;
        };
        if !current.same_binding(&command.credentials) {
            let _ = command.reply.send(Err(Error::Conflict));
            return;
        }
        let replacement = tokio::select! {
            _ = command.reply.closed() => return,
            result = tokio::time::timeout_at(command.deadline, prepare) => {
                result.unwrap_or(Err(Error::Provider))
            }
        };
        let client = match replacement {
            Ok(client) => client,
            Err(error) => {
                let _ = command.reply.send(Err(error));
                return;
            }
        };
        if command.reply.is_closed() || Instant::now() >= command.deadline {
            return;
        }
        // There are no suspension points between replacing credentials and acknowledging.
        // If the caller cancels after this commit, the renewed provider remains available.
        if let (Some(transport), Some(client)) = (&mut self.session, client) {
            transport.replace_client(client);
        }
        self.bootstrap = Bootstrap::Session(command.credentials);
        let _ = command.reply.send(Ok(()));
        let _ = self.events.send(());
    }

    async fn exchange(
        &mut self,
        request: &SwapMessage,
        key: ReplyKey,
        deadline: Instant,
    ) -> Result<SwapMessage> {
        self.journal.prepare(request)?;
        if let Some(reply) = self.journal.reply(key)? {
            return validate_reply(reply, key);
        }
        #[cfg(feature = "rendezvous")]
        {
            if let Bootstrap::Session(credentials) = &self.bootstrap {
                if self.session.is_none() {
                    self.session = Some(credentials.connect(&self.peer).await?);
                }
                let reply = self
                    .session
                    .as_ref()
                    .ok_or(Error::Provider)?
                    .exchange(request, key)
                    .await?;
                return self.save_matching(reply, key);
            }
            if let Bootstrap::Secret(secret) = &self.bootstrap {
                if self.rpc.is_none() {
                    self.rpc = pubky_transport::p2p::DirectRpcClient::new(**secret, &self.peer)
                        .await
                        .ok();
                }
                if let Some(rpc) = &self.rpc {
                    let request = pubky_transport::p2p::DirectRequest {
                        message: serde_json::to_value(request).map_err(|_| Error::Validation)?,
                    };
                    // Both attempts carry the same request and quote IDs, including an ambiguous send.
                    for _ in 0..2 {
                        let attempt_deadline =
                            deadline.min(Instant::now() + Duration::from_secs(10));
                        if let Ok(Ok(bytes)) =
                            tokio::time::timeout_at(attempt_deadline, rpc.request(&request)).await
                        {
                            let reply =
                                serde_json::from_slice(&bytes).map_err(|_| Error::Validation)?;
                            return self.save_matching(reply, key);
                        }
                    }
                }
            }
        }
        self.exchange_dm(request, key, deadline).await
    }

    #[cfg(feature = "rendezvous")]
    fn save_matching(&self, reply: SwapMessage, key: ReplyKey) -> Result<SwapMessage> {
        if !key.matches(&reply) {
            return Err(Error::Validation);
        }
        self.journal.save_reply(&reply)?;
        if matches!(reply, SwapMessage::SwapAccept(_)) {
            let _ = self.events.send(());
        }
        validate_reply(reply, key)
    }

    async fn exchange_dm(
        &mut self,
        request: &SwapMessage,
        key: ReplyKey,
        deadline: Instant,
    ) -> Result<SwapMessage> {
        if self
            .transport
            .as_ref()
            .is_some_and(|transport| transport.has_pending_messages(&self.peer))
        {
            match self.poll(Some(key)).await {
                Ok(Some(reply)) => return validate_reply(reply, key),
                Err(Error::Storage | Error::Conflict) => return Err(Error::Storage),
                _ => {}
            }
        }
        let transport = self.transport.as_ref().ok_or(Error::Provider)?;
        // Restore pending inbox deliveries before a replay. Startup never blanket-acknowledges.
        if let Some(reply) = self.poll(Some(key)).await? {
            return validate_reply(reply, key);
        }
        #[cfg(feature = "rendezvous")]
        if let Bootstrap::Secret(secret) = &self.bootstrap {
            let ring_deadline = deadline.min(Instant::now() + Duration::from_secs(5));
            let _ = tokio::time::timeout_at(
                ring_deadline,
                pubky_transport::p2p::ring_provider(**secret, &self.peer),
            )
            .await;
        }
        #[cfg(not(feature = "rendezvous"))]
        let _ = deadline;
        let scope = request.delivery_scope();
        let durable = self.journal.directory.is_some();
        if durable {
            transport
                .enqueue_with_scope(&self.peer, &scope, request)
                .map_err(map_transport_error)?;
        } else {
            transport
                .send_with_scope(&self.peer, &scope, request)
                .await
                .map_err(map_transport_error)?;
        }
        #[cfg(feature = "rendezvous")]
        let mut ring_retry_at = Some(Instant::now() + Duration::from_secs(10));
        loop {
            #[cfg(feature = "rendezvous")]
            if ring_retry_at.is_some_and(|retry_at| Instant::now() >= retry_at) {
                ring_retry_at = None;
                if let Bootstrap::Secret(secret) = &self.bootstrap {
                    let ring_deadline = deadline.min(Instant::now() + Duration::from_secs(5));
                    let _ = tokio::time::timeout_at(
                        ring_deadline,
                        pubky_transport::p2p::ring_provider(**secret, &self.peer),
                    )
                    .await;
                }
            }
            if durable {
                transport
                    .process_outbox(crate::validation::now(), 8)
                    .await
                    .map_err(map_transport_error)?;
                if transport
                    .outbox_status()
                    .map_err(|_| Error::Storage)?
                    .iter()
                    .any(|status| {
                        status.peer == self.peer
                            && (status.scope == scope
                                || status.scope.starts_with(&format!("{scope}:")))
                            && status.publication_paused
                    })
                {
                    return Err(Error::Invalid("message delivery paused; correct the cause and explicitly retry the saved delivery"));
                }
            }
            if let Some(reply) = self.poll(Some(key)).await? {
                return validate_reply(reply, key);
            }
            sleep(Duration::from_millis(500)).await;
        }
    }

    async fn poll(&self, expected: Option<ReplyKey>) -> Result<Option<SwapMessage>> {
        let transport = self.transport.as_ref().ok_or(Error::Provider)?;
        let received = transport
            .poll_from::<SwapMessage>(&self.peer)
            .await
            .map_err(map_transport_error)?;
        let mut matched = None;
        for inbound in received {
            self.journal.save_reply(&inbound.message)?;
            inbound.receipt.acknowledge().map_err(|_| Error::Storage)?;
            let is_match = expected.is_some_and(|key| key.matches(&inbound.message));
            if !is_match || matches!(inbound.message, SwapMessage::SwapAccept(_)) {
                let _ = self.events.send(());
            }
            if is_match {
                matched = Some(inbound.message);
            }
        }
        Ok(matched)
    }

    async fn maintain(&self) -> Result<()> {
        let Some(transport) = &self.transport else {
            return Ok(());
        };
        let status = transport.outbox_status().map_err(|_| Error::Storage)?;
        let pending = transport.has_pending_messages(&self.peer);
        if pending {
            let _ = self.poll(None).await;
        }
        if status.is_empty() {
            return Ok(());
        }
        self.transport
            .as_ref()
            .ok_or(Error::Provider)?
            .process_outbox(crate::validation::now(), 8)
            .await
            .map_err(map_transport_error)?;
        self.poll(None).await.map(|_| ())
    }

    async fn close(&self) {
        #[cfg(feature = "rendezvous")]
        {
            if let Some(rpc) = &self.rpc {
                rpc.close().await;
            }
            if let Some(session) = &self.session {
                session.close().await;
            }
        }
    }
}

fn map_transport_error(error: pubky_transport::TransportError) -> Error {
    match error {
        pubky_transport::TransportError::Delivery { failure, .. } if !failure.is_retryable() => Error::Invalid("message delivery paused; correct the cause and explicitly retry the saved delivery"),
        _ => Error::Provider,
    }
}

#[cfg(all(test, feature = "rendezvous"))]
#[path = "session_renewal_tests.rs"]
mod session_renewal_tests;
