//! Boltz request and lifecycle data, with no signing secrets.

use bitcoin::hashes::{sha256, Hash};
use serde::{Deserialize, Serialize};
use swap_common::{
    messages::{Quote, SwapAccept, SwapRequest},
    SwapDirection,
};
use uuid::Uuid;

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SubmarineRequest {
    pub from: String,
    pub to: String,
    pub invoice: String,
    pub refund_public_key: String,
    #[serde(default)]
    pub pair_hash: String,
    #[serde(default)]
    pub referral_id: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReverseRequest {
    pub from: String,
    pub to: String,
    pub preimage_hash: String,
    pub claim_public_key: String,
    #[serde(default)]
    pub invoice_amount: u64,
    #[serde(default)]
    pub onchain_amount: u64,
    #[serde(default)]
    pub pair_hash: String,
    #[serde(default)]
    pub referral_id: String,
    #[serde(default)]
    pub error: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "direction", content = "request", rename_all = "lowercase")]
pub enum CreateRequest {
    Submarine(SubmarineRequest),
    Reverse(ReverseRequest),
}

impl CreateRequest {
    pub fn direction(&self) -> SwapDirection {
        match self {
            Self::Submarine(_) => SwapDirection::Submarine,
            Self::Reverse(_) => SwapDirection::Reverse,
        }
    }

    pub fn pair_hash(&self) -> &str {
        match self {
            Self::Submarine(r) => &r.pair_hash,
            Self::Reverse(r) => &r.pair_hash,
        }
    }

    pub fn client_key(&self) -> &str {
        match self {
            Self::Submarine(r) => &r.refund_public_key,
            Self::Reverse(r) => &r.claim_public_key,
        }
    }

    pub fn fingerprint(&self) -> crate::Result<String> {
        let bytes = serde_json::to_vec(self).map_err(|_| crate::Error::Validation)?;
        Ok(sha256::Hash::hash(&bytes).to_string())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StoredSwap {
    pub id: Uuid,
    pub request: CreateRequest,
    pub payment_hash: String,
    pub quote: Option<Quote>,
    pub native_request: Option<SwapRequest>,
    #[serde(default)]
    pub admission_tip: Option<u32>,
    pub accept: Option<SwapAccept>,
    pub response: Option<serde_json::Value>,
    pub update: SwapUpdate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SwapUpdate {
    pub id: Uuid,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction: Option<TransactionInfo>,
    pub zero_conf_rejected: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionInfo {
    pub id: String,
    pub hex: String,
}

impl SwapUpdate {
    pub fn initial(id: Uuid, direction: SwapDirection) -> Self {
        Self {
            id,
            status: match direction {
                SwapDirection::Submarine => "invoice.set",
                SwapDirection::Reverse => "swap.created",
            }
            .into(),
            transaction: None,
            zero_conf_rejected: true,
        }
    }

    pub fn terminal(&self) -> bool {
        matches!(
            self.status.as_str(),
            "transaction.claimed"
                | "invoice.settled"
                | "transaction.refunded"
                | "swap.expired"
                | "transaction.failed"
        )
    }
}

/// Independently observed funding data for a caller-owned claim or refund signer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpendInfo {
    pub outpoint: bitcoin::OutPoint,
    pub output: bitcoin::TxOut,
    pub confirmations: u32,
    pub required_confirmations: u32,
    pub tip: u32,
    pub timeout_block_height: u32,
}

/// All independently observed outputs the client can refund from its submarine contract.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefundInfo {
    pub utxos: Vec<(bitcoin::OutPoint, bitcoin::TxOut)>,
    pub tip: u32,
    pub timeout_block_height: u32,
}
