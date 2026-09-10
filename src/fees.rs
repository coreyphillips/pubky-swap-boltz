//! Checked integer translation of native output amounts and fee schedules.

use crate::{Error, Result};
use bitcoin::hashes::{sha256, Hash};
use serde_json::{json, Value};
use swap_common::{messages::SwapOffer, SwapDirection};

const PPM: u128 = 1_000_000;

pub fn total(offer: &SwapOffer, amount: u64) -> Result<u64> {
    let proportional = u128::from(amount) * u128::from(offer.fee_ppm) / PPM;
    let total = u128::from(amount)
        + proportional
        + u128::from(offer.base_fee_sat)
        + u128::from(offer.onchain_fee_sat);
    u64::try_from(total).map_err(|_| Error::Invalid("amount overflow"))
}

/// Only exact invoices are accepted: no silent rounding of a caller's Lightning payment.
pub fn reverse_output(offer: &SwapOffer, invoice_amount: u64) -> Result<u64> {
    let low = output_within_budget(offer, invoice_amount);
    if total(offer, low)? != invoice_amount {
        return Err(Error::Invalid(
            "invoiceAmount cannot be represented by this provider fee schedule; use onchainAmount",
        ));
    }
    Ok(low)
}

pub fn output_within_budget(offer: &SwapOffer, invoice_amount: u64) -> u64 {
    let (mut low, mut high) = (0, invoice_amount);
    while low < high {
        let mid = low + (high - low).div_ceil(2);
        if total(offer, mid).is_ok_and(|n| n <= invoice_amount) {
            low = mid;
        } else {
            high = mid - 1;
        }
    }
    low
}

pub fn pair_hash(offer: &SwapOffer, direction: SwapDirection) -> Result<String> {
    let fields = json!([
        offer.provider_pkarr,
        offer.offer_id,
        offer.network,
        direction,
        offer.min_amount_sat,
        offer.max_amount_sat,
        offer.base_fee_sat,
        offer.fee_ppm,
        offer.onchain_fee_sat,
        offer.fee_rate_sat_vb,
        offer.required_confirmations,
        offer.htlc_timeout_blocks
    ]);
    let bytes = serde_json::to_vec(&fields).map_err(|_| Error::Validation)?;
    Ok(sha256::Hash::hash(&bytes).to_string())
}

pub fn pairs(offer: &SwapOffer, direction: SwapDirection) -> Result<Value> {
    if !offer.directions.contains(&direction) {
        return Ok(json!({}));
    }
    let fixed = offer
        .base_fee_sat
        .checked_add(offer.onchain_fee_sat)
        .ok_or(Error::Validation)?;
    let min = offer.effective_min_amount_sat();
    let (minimum, maximum, miner) = match direction {
        SwapDirection::Submarine => (min, offer.max_amount_sat, json!(fixed)),
        SwapDirection::Reverse => (
            total(offer, min)?,
            total(offer, offer.max_amount_sat)?,
            json!({"lockup":fixed,"claim":offer.fee_rate_sat_vb.saturating_mul(180)}),
        ),
    };
    Ok(json!({"BTC":{"BTC":{
        "hash":pair_hash(offer,direction)?,"rate":1,
        "limits":{"minimal":minimum,"maximal":maximum,"minimalBatched":minimum,"maximalZeroConf":0},
        "fees":{"percentage":offer.fee_ppm as f64 / 10_000.0,"minerFees":miner}
    }}}))
}
