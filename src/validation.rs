//! Validate public input and independently reconstruct every provider lockup.

use crate::{
    model::{CreateRequest, StoredSwap},
    Error, Result,
};
use bitcoin::{hashes::Hash, Network, PublicKey};
use lightning_invoice::{Bolt11Invoice, Currency};
use std::{
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use swap_common::{
    messages::{SwapAccept, SwapOffer, SwapScript},
    validate::ClientPolicy,
    SwapDirection,
};

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub(crate) fn request(request: &mut CreateRequest, network: Network) -> Result<String> {
    let (from, to, referral, error) = match request {
        CreateRequest::Submarine(r) => (&r.from, &r.to, &r.referral_id, &r.error),
        CreateRequest::Reverse(r) => (&r.from, &r.to, &r.referral_id, &r.error),
    };
    if from != "BTC" || to != "BTC" || !referral.is_empty() || !error.is_empty() {
        return Err(Error::Unsupported);
    }
    let key = PublicKey::from_str(request.client_key())
        .map_err(|_| Error::Invalid("invalid compressed public key"))?;
    if !key.compressed {
        return Err(Error::Invalid("public key must be compressed"));
    }
    match request {
        CreateRequest::Submarine(r) => {
            r.refund_public_key = key.to_string();
            let invoice = invoice(&r.invoice, network)?;
            r.invoice = invoice.to_string();
            Ok(invoice.payment_hash().to_string())
        }
        CreateRequest::Reverse(r) => {
            r.claim_public_key = key.to_string();
            let hash = bitcoin::hashes::sha256::Hash::from_str(&r.preimage_hash)
                .map_err(|_| Error::Invalid("invalid preimage hash"))?;
            r.preimage_hash = hash.to_string();
            if (r.invoice_amount == 0) == (r.onchain_amount == 0) {
                return Err(Error::Invalid(
                    "provide exactly one of invoiceAmount and onchainAmount",
                ));
            }
            Ok(r.preimage_hash.clone())
        }
    }
}

pub(crate) fn invoice(text: &str, network: Network) -> Result<Bolt11Invoice> {
    let invoice =
        Bolt11Invoice::from_str(text).map_err(|_| Error::Invalid("invalid BOLT11 invoice"))?;
    invoice
        .check_signature()
        .map_err(|_| Error::Invalid("invalid invoice signature"))?;
    let currency = match network {
        Network::Bitcoin => Currency::Bitcoin,
        Network::Testnet => Currency::BitcoinTestnet,
        Network::Signet => Currency::Signet,
        Network::Regtest => Currency::Regtest,
        _ => return Err(Error::Unsupported),
    };
    if invoice.currency() != currency {
        return Err(Error::Invalid("invoice network mismatch"));
    }
    let amount = invoice
        .amount_milli_satoshis()
        .ok_or(Error::Invalid("invoice must specify an amount"))?;
    if amount == 0 || amount % 1000 != 0 {
        return Err(Error::Invalid("invoice must specify whole satoshis"));
    }
    Ok(invoice)
}

pub(crate) fn offer(offer: &SwapOffer, provider: &str, network: Network) -> Result<()> {
    if !pubky_transport::same_pubky(&offer.provider_pkarr, provider)
        || offer.network.to_bitcoin_network() != network
        || !swap_common::messages::protocol_version_supported(offer.protocol_version)
    {
        return Err(Error::Validation);
    }
    if !offer.features.iter().any(|f| f == "boltz-taproot-v1")
        || !offer.features.iter().any(|f| f == "swap-status-v1")
    {
        return Err(Error::Invalid(
            "provider does not support the required Taproot and recovery capabilities",
        ));
    }
    if offer.valid_until_unix <= now()
        || offer.required_confirmations == 0
        || offer.min_amount_sat == 0
        || offer.max_amount_sat < offer.effective_min_amount_sat()
    {
        return Err(Error::Validation);
    }
    Ok(())
}

pub(crate) fn accept(
    record: &StoredSwap,
    accept: &SwapAccept,
    tip: u32,
    policy: &ClientPolicy,
    network: Network,
) -> Result<()> {
    accept_at(record, accept, tip, policy, network, now())
}

/// Check saved terms without imposing a new negotiation window on funds already locked.
pub(crate) fn persisted_accept(
    record: &StoredSwap,
    policy: &ClientPolicy,
    network: Network,
) -> Result<()> {
    let accept = record.accept.as_ref().ok_or(Error::NotFound)?;
    let tip = record.admission_tip.ok_or(Error::Validation)?;
    let mut original = record.request.clone();
    if request(&mut original, network)? != record.payment_hash
        || original.direction() != accept.direction
    {
        return Err(Error::Validation);
    }
    let quote = record.quote.as_ref().ok_or(Error::Validation)?;
    let amount_matches = match &original {
        CreateRequest::Submarine(request) => {
            invoice(&request.invoice, network)?.amount_milli_satoshis()
                == quote.amount_sat.checked_mul(1000)
        }
        CreateRequest::Reverse(request) if request.onchain_amount > 0 => {
            request.onchain_amount == quote.amount_sat
        }
        CreateRequest::Reverse(request) => request.invoice_amount == quote.total_sat,
    };
    if !amount_matches || quote.amount_sat.checked_add(quote.fee_sat) != Some(quote.total_sat) {
        return Err(Error::Validation);
    }
    // Invoice lifetime was checked at admission. Recheck its original lifetime and immutable
    // payment terms, allowing recovery after that invoice or the HTLC timeout has expired.
    let invoice_time = if let Some(encoded) = &accept.invoice {
        invoice(encoded, network)?.duration_since_epoch().as_secs()
    } else {
        0
    };
    accept_at(record, accept, tip, policy, network, invoice_time)
}

fn accept_at(
    record: &StoredSwap,
    accept: &SwapAccept,
    tip: u32,
    policy: &ClientPolicy,
    network: Network,
    invoice_time: u64,
) -> Result<()> {
    let quote = record.quote.as_ref().ok_or(Error::Validation)?;
    let admission_tip = record.admission_tip.ok_or(Error::Validation)?;
    swap_common::validate::validate_accept(accept, quote, Some(admission_tip), policy)
        .map_err(|_| Error::Validation)?;
    let mut timelock = policy.timelock;
    timelock.required_confirmations = policy.effective_confirmations(quote.required_confirmations);
    match accept.direction {
        SwapDirection::Submarine => swap_common::timelock::check_client_submarine_accept(
            tip,
            accept.timeout_block_height,
            &timelock,
        ),
        SwapDirection::Reverse => swap_common::timelock::check_client_reverse_accept(
            tip,
            accept.timeout_block_height,
            &timelock,
        ),
    }
    .map_err(|_| Error::Validation)?;
    if accept.script_type != SwapScript::TaprootBoltz || !accept.htlc_script_hex.is_empty() {
        return Err(Error::Validation);
    }
    let provider =
        PublicKey::from_str(&accept.provider_pubkey_hex).map_err(|_| Error::Validation)?;
    let client = PublicKey::from_str(record.request.client_key()).map_err(|_| Error::Validation)?;
    if !provider.compressed || provider == client {
        return Err(Error::Validation);
    }
    let hash = bitcoin::hashes::sha256::Hash::from_str(&record.payment_hash)
        .map_err(|_| Error::Validation)?
        .to_byte_array();
    let (claim, refund) = match accept.direction {
        SwapDirection::Submarine => (provider, client),
        SwapDirection::Reverse => (client, provider),
    };
    let contract = swap_common::taproot::BoltzTaprootSwap::new(
        accept.direction,
        &hash,
        &claim,
        &refund,
        accept.timeout_block_height,
    )
    .map_err(|_| Error::Validation)?;
    if contract.address(network).to_string() != accept.htlc_address
        || Some(contract.swap_tree()) != accept.swap_tree
    {
        return Err(Error::Validation);
    }
    if accept.direction == SwapDirection::Reverse {
        let invoice = invoice(accept.invoice.as_deref().ok_or(Error::Validation)?, network)
            .map_err(|_| Error::Validation)?;
        let decoded = swap_common::validate::DecodedHoldInvoice {
            payment_hash: invoice.payment_hash().to_byte_array(),
            amount_msat: invoice.amount_milli_satoshis().ok_or(Error::Validation)?,
            amount_is_explicit: true,
            expires_at_unix: invoice.expires_at().ok_or(Error::Validation)?.as_secs(),
        };
        swap_common::validate::validate_hold_invoice(&decoded, quote, &hash, invoice_time, policy)
            .map_err(|_| Error::Validation)?;
    } else if accept.invoice.is_some() {
        return Err(Error::Validation);
    }
    Ok(())
}
