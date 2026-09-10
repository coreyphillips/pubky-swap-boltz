//! Negotiation and lifecycle orchestration, independent of the HTTP server.

use crate::{
    chain::Chain, fees, model::*, provider::Provider, store::Store, validation, Error, Result,
};
use bitcoin::{consensus::deserialize, Address, Network, Transaction};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use swap_common::{
    messages::*, timelock::TimelockParams, validate::ClientPolicy, SwapDirection, SwapState,
};
use tokio::sync::{broadcast, Mutex};
use uuid::Uuid;

pub struct BridgeSettings {
    pub network: Network,
    pub max_fee_bps: u16,
    pub max_amount_sat: u64,
}

pub struct Bridge {
    pub(crate) provider: Arc<dyn Provider>,
    pub(crate) chain: Arc<dyn Chain>,
    pub(crate) store: Store,
    network: Network,
    policy: ClientPolicy,
    mutation: Mutex<()>,
    events: broadcast::Sender<SwapUpdate>,
}

impl Bridge {
    pub fn new(
        provider: Arc<dyn Provider>,
        chain: Arc<dyn Chain>,
        store: Store,
        settings: BridgeSettings,
    ) -> Arc<Self> {
        let mut policy = ClientPolicy::for_network(settings.network, TimelockParams::default());
        policy.max_fee_bps = settings.max_fee_bps;
        policy.max_total_sat = settings.max_amount_sat;
        Arc::new(Self {
            provider,
            chain,
            store,
            network: settings.network,
            policy,
            mutation: Mutex::new(()),
            events: broadcast::channel(128).0,
        })
    }

    /// Export public contracts without waiting for provider or chain requests.
    pub fn export_snapshot(&self) -> Result<crate::store::StoreSnapshot> {
        self.store.export_snapshot()
    }

    pub fn validate_import_snapshot(&self, snapshot: &crate::store::StoreSnapshot) -> Result<()> {
        self.store.validate_import(snapshot)
    }

    /// Refuse concurrent mutation instead of waiting behind an in-flight network request.
    pub async fn import_snapshot(&self, snapshot: &crate::store::StoreSnapshot) -> Result<()> {
        let _guard = self.mutation.try_lock().map_err(|_| Error::Busy)?;
        self.store.import_snapshot(snapshot)
    }

    pub async fn offer(&self) -> Result<SwapOffer> {
        let offer = self.provider.offer().await?;
        validation::offer(&offer, &self.provider.provider_key(), self.network)?;
        Ok(offer)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SwapUpdate> {
        self.events.subscribe()
    }

    pub async fn pairs(&self, direction: SwapDirection) -> Result<Value> {
        let mut offer = self.offer().await?;
        let provider_hash = fees::pair_hash(&offer, direction)?;
        let limit_ppm = u64::from(self.policy.max_fee_bps) * 100;
        let fixed = u128::from(offer.base_fee_sat) + u128::from(offer.onchain_fee_sat);
        if offer.fee_ppm > limit_ppm || (offer.fee_ppm == limit_ppm && fixed > 0) {
            return Ok(json!({}));
        }
        let minimum = if limit_ppm == offer.fee_ppm {
            0
        } else {
            fixed
                .saturating_mul(1_000_000)
                .div_ceil(u128::from(limit_ppm - offer.fee_ppm))
        };
        offer.min_amount_sat = offer
            .effective_min_amount_sat()
            .max(u64::try_from(minimum).unwrap_or(u64::MAX));
        offer.max_amount_sat = offer.max_amount_sat.min(fees::output_within_budget(
            &offer,
            self.policy.max_total_sat,
        ));
        if offer.min_amount_sat > offer.max_amount_sat {
            return Ok(json!({}));
        }
        // Pair hashes commit to the provider quote terms, which admission verifies again.
        let mut pairs = fees::pairs(&offer, direction)?;
        if let Some(pair) = pairs.get_mut("BTC").and_then(|p| p.get_mut("BTC")) {
            pair["hash"] = json!(provider_hash);
        }
        Ok(pairs)
    }

    pub fn cached_update(&self, id: Uuid) -> Result<SwapUpdate> {
        let record = self.store.get(id)?;
        if record.response.is_none() {
            return Err(Error::NotFound);
        }
        Ok(record.update)
    }

    pub async fn create(
        &self,
        mut request: CreateRequest,
        idempotency: Option<String>,
    ) -> Result<Value> {
        let _guard = self.mutation.try_lock().map_err(|_| Error::Busy)?;
        let hash = validation::request(&mut request, self.network)?;
        let exists = self.store.find_request(&request)?.is_some();
        let quote = if exists {
            None
        } else {
            Some(self.prepare_quote(&request).await?)
        };
        let mut record = self.store.reserve(request, hash, idempotency.as_deref())?;
        if let Some(response) = &record.response {
            return Ok(response.clone());
        }
        if record.native_request.is_none() {
            let quote = match quote {
                Some(q) => q,
                None => self.prepare_quote(&record.request).await?,
            };
            record.native_request = Some(native_request(&record, &quote, self.provider.identity()));
            record.quote = Some(quote);
            self.store.save(&record)?;
        }
        self.admit(&mut record).await
    }

    async fn prepare_quote(&self, creation: &CreateRequest) -> Result<Quote> {
        let offer = self.offer().await?;
        let direction = creation.direction();
        if !creation.pair_hash().is_empty()
            && creation.pair_hash() != fees::pair_hash(&offer, direction)?
        {
            return Err(Error::Invalid("pair hash changed; fetch current pairs"));
        }
        let amount = self.request_amount(creation, &offer)?;
        if !offer.supports(direction) || !offer.accepts_amount(amount) {
            return Err(Error::Invalid("amount out of range"));
        }
        let request = QuoteRequest {
            request_id: Some(Uuid::new_v4()),
            offer_id: offer.offer_id,
            client_pkarr: self.provider.identity(),
            direction,
            amount_sat: amount,
            protocol_version: PROTOCOL_VERSION,
            features: vec!["boltz-taproot-v1".into(), "swap-status-v1".into()],
        };
        let quote = self.provider.quote(request.clone()).await?;
        swap_common::validate::validate_quote(&quote, &request, validation::now(), &self.policy)
            .map_err(|_| Error::Validation)?;
        if quote.request_id != request.request_id || quote.total_sat != fees::total(&offer, amount)?
        {
            return Err(Error::Validation);
        }
        if let CreateRequest::Reverse(r) = creation {
            if r.invoice_amount != 0 && quote.total_sat != r.invoice_amount {
                return Err(Error::Validation);
            }
        }
        Ok(quote)
    }

    fn request_amount(&self, request: &CreateRequest, offer: &SwapOffer) -> Result<u64> {
        match request {
            CreateRequest::Submarine(r) => {
                let invoice = validation::invoice(&r.invoice, self.network)?;
                if invoice.is_expired() {
                    return Err(Error::Invalid("invoice expired"));
                }
                Ok(invoice.amount_milli_satoshis().ok_or(Error::Validation)? / 1000)
            }
            CreateRequest::Reverse(r) if r.onchain_amount > 0 => Ok(r.onchain_amount),
            CreateRequest::Reverse(r) => fees::reverse_output(offer, r.invoice_amount),
        }
    }

    async fn admit(&self, record: &mut StoredSwap) -> Result<Value> {
        let request = record.native_request.clone().ok_or(Error::Validation)?;
        if record.admission_tip.is_none() {
            record.admission_tip = Some(self.chain.tip().await?);
            self.store.save(record)?;
        }
        let accept = match self.provider.create(request.clone()).await {
            Ok(accept)=>accept,
            Err(error)=>match self.provider.snapshot_by_quote(request.quote_id).await {
                Ok(snapshot)=>snapshot.accept,
                Err(Error::NotFound)=>return Err(Error::Invalid("provider has no durable admission for this quote; use a new invoice or payment hash")),
                Err(_)=>return Err(error),
            },
        };
        let tip = self.chain.tip().await?;
        validation::accept(record, &accept, tip, &self.policy, self.network)?;
        let response = creation_response(record.id, &accept)?;
        record.accept = Some(accept);
        record.response = Some(response.clone());
        self.store.save(record)?;
        let _ = self.events.send(record.update.clone());
        Ok(response)
    }

    pub async fn refresh(&self, id: Uuid) -> Result<SwapUpdate> {
        let _guard = self.mutation.lock().await;
        let mut record = self.store.get(id)?;
        let accept = record.accept.as_ref().ok_or(Error::NotFound)?;
        let snapshot = self.provider.snapshot(accept.swap_id).await?;
        if snapshot.accept != *accept
            || snapshot.network.to_bitcoin_network() != self.network
            || snapshot.required_confirmations
                != record
                    .quote
                    .as_ref()
                    .ok_or(Error::Validation)?
                    .required_confirmations
        {
            return Err(Error::Validation);
        }
        let observed = self.chain.observe(accept).await?;
        let mut update = SwapUpdate::initial(id, accept.direction);
        update.status = state_status(accept.direction, &snapshot.state).into();
        if matches!(snapshot.state, SwapState::Claimed | SwapState::Refunded) {
            update.status = if accept.direction == SwapDirection::Reverse {
                "invoice.paid"
            } else {
                "invoice.pending"
            }
            .into();
        }
        if let Some(observation) = observed {
            let required = self
                .policy
                .effective_confirmations(snapshot.required_confirmations);
            verify_funding_reference(&snapshot, observation.outpoint)?;
            if let Some(spend) = &observation.spend {
                if observation.spend_confirmations >= required {
                    let claim =
                        verified_preimage(spend, observation.outpoint, &record.payment_hash)?;
                    update.status = match (claim, accept.direction, &snapshot.state) {
                        (true, SwapDirection::Reverse, SwapState::Claimed) => "invoice.settled",
                        (true, SwapDirection::Reverse, _) => "invoice.paid",
                        (true, SwapDirection::Submarine, _) => "transaction.claimed",
                        (false, _, _) => "transaction.refunded",
                    }
                    .into();
                } else {
                    update.status = "invoice.paid".into();
                }
                if accept.direction == SwapDirection::Submarine {
                    update.transaction = Some(observation.transaction);
                }
            } else if accept.direction == SwapDirection::Submarine {
                update.transaction = Some(observation.transaction);
                update.status = if observation.confirmations >= required {
                    "transaction.confirmed"
                } else {
                    "transaction.mempool"
                }
                .into();
            } else {
                let tip = self.chain.tip().await?;
                let has_claim_window = tip
                    .saturating_add(swap_common::timelock::CLIENT_CLAIM_WINDOW)
                    < accept.timeout_block_height;
                if observation.confirmations >= required
                    && has_claim_window
                    && !snapshot.state.is_terminal()
                {
                    update.transaction = Some(observation.transaction);
                    update.status = "transaction.confirmed".into();
                } else {
                    update.status = "invoice.paid".into();
                }
            }
        }
        if update != record.update {
            record.update = update.clone();
            self.store.save(&record)?;
            let _ = self.events.send(update.clone());
        }
        Ok(update)
    }

    pub async fn recover_and_refresh(&self) -> Result<()> {
        for record in self.store.all()? {
            if record.response.is_none() && record.native_request.is_some() {
                let _guard = self.mutation.lock().await;
                let mut current = self.store.get(record.id)?;
                if current.response.is_none() {
                    let _ = self.admit(&mut current).await;
                }
            } else if record.response.is_some() {
                let _ = self.refresh(record.id).await;
            }
        }
        Ok(())
    }

    /// Inspect an accepted lockup using only saved terms and the configured chain service.
    /// No provider request is made. The caller must enforce its claim or refund safety window
    /// immediately before signing, and keep private keys and preimages outside the bridge.
    pub async fn spend_info(&self, id: Uuid) -> Result<SpendInfo> {
        let record = self.recovery_record(id)?;
        let accept = record.accept.as_ref().ok_or(Error::NotFound)?;
        let quote = record.quote.as_ref().ok_or(Error::Validation)?;
        let observation = self.chain.observe(accept).await?.ok_or(Error::NotFound)?;
        if observation.spend.is_some() {
            return Err(Error::Invalid("swap output is already spent"));
        }
        let transaction: Transaction =
            deserialize(&hex::decode(&observation.transaction.hex).map_err(|_| Error::Validation)?)
                .map_err(|_| Error::Validation)?;
        let txid = transaction.compute_txid();
        if txid != observation.outpoint.txid || txid.to_string() != observation.transaction.id {
            return Err(Error::Validation);
        }
        let output = transaction
            .output
            .get(observation.outpoint.vout as usize)
            .ok_or(Error::Validation)?;
        let script = Address::from_str(&accept.htlc_address)
            .map_err(|_| Error::Validation)?
            .require_network(self.network)
            .map_err(|_| Error::Validation)?
            .script_pubkey();
        if output.script_pubkey != script
            || output.value.to_sat() < accept.onchain_amount_sat
            || output.value.to_sat() > accept.onchain_amount_sat.saturating_add(10_000)
        {
            return Err(Error::Validation);
        }
        Ok(SpendInfo {
            outpoint: observation.outpoint,
            output: output.clone(),
            confirmations: observation.confirmations,
            required_confirmations: self
                .policy
                .effective_confirmations(quote.required_confirmations),
            tip: self.chain.tip().await?,
            timeout_block_height: accept.timeout_block_height,
        })
    }

    /// Find all outputs recoverable by the client's submarine refund key without requiring
    /// the funding amount to match the quote. The caller enforces timeout before signing.
    pub async fn refund_info(&self, id: Uuid) -> Result<RefundInfo> {
        let record = self.recovery_record(id)?;
        let accept = record.accept.as_ref().ok_or(Error::NotFound)?;
        if accept.direction != SwapDirection::Submarine {
            return Err(Error::Unsupported);
        }
        let script = Address::from_str(&accept.htlc_address)
            .map_err(|_| Error::Validation)?
            .require_network(self.network)
            .map_err(|_| Error::Validation)?
            .script_pubkey();
        let utxos = self.chain.refund_utxos(accept).await?;
        if utxos.is_empty() {
            return Err(Error::NotFound);
        }
        let mut seen = HashSet::new();
        let mut transactions = HashMap::new();
        for (outpoint, output) in &utxos {
            if !seen.insert(*outpoint) || output.script_pubkey != script {
                return Err(Error::Validation);
            }
            if let std::collections::hash_map::Entry::Vacant(entry) =
                transactions.entry(outpoint.txid)
            {
                let info = self.chain.transaction(outpoint.txid).await?;
                let transaction: Transaction =
                    deserialize(&hex::decode(&info.hex).map_err(|_| Error::Validation)?)
                        .map_err(|_| Error::Validation)?;
                if transaction.compute_txid() != outpoint.txid
                    || info.id != outpoint.txid.to_string()
                {
                    return Err(Error::Validation);
                }
                entry.insert(transaction);
            }
            if transactions
                .get(&outpoint.txid)
                .and_then(|transaction| transaction.output.get(outpoint.vout as usize))
                != Some(output)
            {
                return Err(Error::Validation);
            }
        }
        Ok(RefundInfo {
            utxos,
            tip: self.chain.tip().await?,
            timeout_block_height: accept.timeout_block_height,
        })
    }

    fn recovery_record(&self, id: Uuid) -> Result<StoredSwap> {
        let record = self.store.get(id)?;
        validation::persisted_accept(&record, &self.policy, self.network)?;
        let accept = record.accept.as_ref().ok_or(Error::NotFound)?;
        let quote = record.quote.as_ref().ok_or(Error::Validation)?;
        let expected = native_request(&record, quote, self.provider.identity());
        if record.native_request.as_ref() != Some(&expected)
            || record.response.as_ref() != Some(&creation_response(id, accept)?)
        {
            return Err(Error::Validation);
        }
        Ok(record)
    }

    pub async fn swap_transaction(&self, id: Uuid) -> Result<Value> {
        let update = self.refresh(id).await?;
        let transaction = update.transaction.ok_or(Error::NotFound)?;
        let record = self.store.get(id)?;
        let timeout = record.accept.ok_or(Error::NotFound)?.timeout_block_height;
        let eta = validation::now()
            .saturating_add(u64::from(timeout.saturating_sub(self.chain.tip().await?)) * 600);
        Ok(
            json!({"id":transaction.id,"hex":transaction.hex,"timeoutBlockHeight":timeout,"timeoutEta":eta}),
        )
    }
}

fn verify_funding_reference(
    snapshot: &SwapStatusSnapshot,
    outpoint: bitcoin::OutPoint,
) -> Result<()> {
    match (&snapshot.funding_txid_hex, snapshot.funding_vout) {
        (Some(txid), Some(vout)) if txid == &outpoint.txid.to_string() && vout == outpoint.vout => {
            Ok(())
        }
        (None, None) => Ok(()),
        _ => Err(Error::Validation),
    }
}

fn verified_preimage(
    info: &TransactionInfo,
    outpoint: bitcoin::OutPoint,
    hash: &str,
) -> Result<bool> {
    let bytes = hex::decode(&info.hex).map_err(|_| Error::Validation)?;
    let transaction: bitcoin::Transaction =
        bitcoin::consensus::deserialize(&bytes).map_err(|_| Error::Validation)?;
    if transaction.compute_txid().to_string() != info.id {
        return Err(Error::Validation);
    }
    let hash: [u8; 32] = hex::decode(hash)
        .map_err(|_| Error::Validation)?
        .try_into()
        .map_err(|_| Error::Validation)?;
    Ok(swap_common::onchain::extract_preimage(&transaction, &outpoint, &hash).is_some())
}

pub(crate) fn native_request(record: &StoredSwap, quote: &Quote, identity: String) -> SwapRequest {
    let (claim, refund, invoice) = match &record.request {
        CreateRequest::Submarine(r) => (
            None,
            Some(r.refund_public_key.clone()),
            Some(r.invoice.clone()),
        ),
        CreateRequest::Reverse(r) => (Some(r.claim_public_key.clone()), None, None),
    };
    SwapRequest {
        quote_id: quote.quote_id,
        client_pkarr: identity,
        direction: record.request.direction(),
        payment_hash_hex: record.payment_hash.clone(),
        client_claim_pubkey_hex: claim,
        client_refund_pubkey_hex: refund,
        invoice,
        script_type: SwapScript::TaprootBoltz,
    }
}

pub(crate) fn creation_response(id: Uuid, accept: &SwapAccept) -> Result<Value> {
    let tree = accept.swap_tree.as_ref().ok_or(Error::Validation)?;
    Ok(match accept.direction {
        SwapDirection::Submarine => {
            json!({"id":id,"address":accept.htlc_address,"bip21":format!("bitcoin:{}?amount={}.{:08}",accept.htlc_address,accept.onchain_amount_sat/100_000_000,accept.onchain_amount_sat%100_000_000),"swapTree":tree,"claimPublicKey":accept.provider_pubkey_hex,"timeoutBlockHeight":accept.timeout_block_height,"expectedAmount":accept.onchain_amount_sat,"acceptZeroConf":false})
        }
        SwapDirection::Reverse => {
            json!({"id":id,"invoice":accept.invoice,"swapTree":tree,"refundPublicKey":accept.provider_pubkey_hex,"lockupAddress":accept.htlc_address,"timeoutBlockHeight":accept.timeout_block_height,"onchainAmount":accept.onchain_amount_sat})
        }
    })
}

fn state_status(direction: SwapDirection, state: &SwapState) -> &'static str {
    match state {
        SwapState::Claimed if direction == SwapDirection::Reverse => "invoice.settled",
        SwapState::Claimed => "transaction.claimed",
        SwapState::Refunded => "transaction.refunded",
        SwapState::Expired => "swap.expired",
        SwapState::Failed(_) => "transaction.failed",
        SwapState::InvoicePaid if direction == SwapDirection::Submarine => "invoice.paid",
        SwapState::InvoicePending if direction == SwapDirection::Submarine => "invoice.pending",
        _ if direction == SwapDirection::Submarine => "invoice.set",
        _ => "swap.created",
    }
}
