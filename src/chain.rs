//! Independent chain observations keep provider state from implying Bitcoin confirmation.

use crate::{model::TransactionInfo, Error, Result};
use async_trait::async_trait;
use bitcoin::{consensus::encode, Address, Network, OutPoint, ScriptBuf, Transaction, Txid};
use electrum_client::{Client, ConfigBuilder, ElectrumApi};
use std::{str::FromStr, sync::Arc};
use swap_common::messages::SwapAccept;

#[derive(Clone)]
pub struct Observation {
    pub transaction: TransactionInfo,
    pub confirmations: u32,
    pub outpoint: OutPoint,
    pub spend: Option<TransactionInfo>,
    pub spend_confirmations: u32,
}

#[async_trait]
pub trait Chain: Send + Sync {
    async fn tip(&self) -> Result<u32>;
    async fn fee(&self) -> Result<u64>;
    async fn observe(&self, accept: &SwapAccept) -> Result<Option<Observation>>;
    async fn transaction(&self, txid: Txid) -> Result<TransactionInfo>;
    async fn broadcast(&self, transaction: Transaction) -> Result<Txid>;
}

pub struct ElectrumChain {
    client: Arc<Client>,
    network: Network,
}

impl ElectrumChain {
    pub async fn connect(url: String, network: Network) -> Result<Self> {
        let client = blocking(move || {
            let config = ConfigBuilder::new().timeout(Some(10)).retry(1).build();
            let client = Client::from_config(&url, config).map_err(|_| Error::Chain)?;
            let genesis = client.block_header(0).map_err(|_| Error::Chain)?;
            if genesis.block_hash()
                != bitcoin::blockdata::constants::genesis_block(network).block_hash()
            {
                return Err(Error::Invalid(
                    "Electrum network does not match configuration",
                ));
            }
            Ok(Arc::new(client))
        })
        .await?;
        Ok(Self { client, network })
    }
}

#[async_trait]
impl Chain for ElectrumChain {
    async fn tip(&self) -> Result<u32> {
        let client = self.client.clone();
        blocking(move || {
            client
                .block_headers_subscribe()
                .map(|h| h.height as u32)
                .map_err(|_| Error::Chain)
        })
        .await
    }

    async fn fee(&self) -> Result<u64> {
        let client = self.client.clone();
        let floor = if self.network == Network::Bitcoin {
            5
        } else {
            1
        };
        blocking(move || {
            let estimate = client.estimate_fee(2).map_err(|_| Error::Chain)?;
            Ok(swap_common::onchain::btc_per_kvb_to_sat_per_vb(estimate)
                .unwrap_or(floor)
                .clamp(floor, 1000))
        })
        .await
    }

    async fn observe(&self, accept: &SwapAccept) -> Result<Option<Observation>> {
        let script = Address::from_str(&accept.htlc_address)
            .map_err(|_| Error::Validation)?
            .require_network(self.network)
            .map_err(|_| Error::Validation)?
            .script_pubkey();
        let client = self.client.clone();
        let value = accept.onchain_amount_sat;
        blocking(move || observe(&client, &script, value)).await
    }

    async fn transaction(&self, txid: Txid) -> Result<TransactionInfo> {
        let client = self.client.clone();
        blocking(move || {
            let tx = client.transaction_get(&txid).map_err(|_| Error::Chain)?;
            if tx.compute_txid() != txid {
                return Err(Error::Chain);
            }
            Ok(info(&tx))
        })
        .await
    }

    async fn broadcast(&self, transaction: Transaction) -> Result<Txid> {
        let client = self.client.clone();
        blocking(move || {
            client
                .transaction_broadcast(&transaction)
                .map_err(|_| Error::Chain)
        })
        .await
    }
}

fn observe(client: &Client, script: &ScriptBuf, value: u64) -> Result<Option<Observation>> {
    let tip = client
        .block_headers_subscribe()
        .map_err(|_| Error::Chain)?
        .height;
    let tip = u32::try_from(tip).map_err(|_| Error::Chain)?;
    let history = client
        .script_get_history(script)
        .map_err(|_| Error::Chain)?;
    let transactions = history
        .into_iter()
        .map(|entry| {
            let transaction = client
                .transaction_get(&entry.tx_hash)
                .map_err(|_| Error::Chain)?;
            if transaction.compute_txid() != entry.tx_hash {
                return Err(Error::Chain);
            }
            Ok((transaction, entry.height))
        })
        .collect::<Result<Vec<_>>>()?;
    classify_history(script, value, tip, &transactions)
}

fn classify_history(
    script: &ScriptBuf,
    value: u64,
    tip: u32,
    history: &[(Transaction, i32)],
) -> Result<Option<Observation>> {
    let mut funding = None;
    for (transaction, height) in history {
        for (index, output) in transaction.output.iter().enumerate() {
            if output.script_pubkey != *script
                || output.value.to_sat() < value
                || output.value.to_sat() > value.saturating_add(10_000)
            {
                continue;
            }
            if funding.is_some() {
                return Err(Error::Validation);
            }
            funding = Some(funding_observation(
                transaction,
                index,
                confirmations(*height, tip)?,
            )?);
        }
    }
    if let Some(observation) = &mut funding {
        identify_spend(observation, tip, history)?;
    }
    Ok(funding)
}

fn funding_observation(
    transaction: &Transaction,
    index: usize,
    confirmations: u32,
) -> Result<Observation> {
    let index = u32::try_from(index).map_err(|_| Error::Validation)?;
    Ok(Observation {
        transaction: info(transaction),
        confirmations,
        outpoint: OutPoint::new(transaction.compute_txid(), index),
        spend: None,
        spend_confirmations: 0,
    })
}

fn identify_spend(
    observation: &mut Observation,
    tip: u32,
    history: &[(Transaction, i32)],
) -> Result<()> {
    for (transaction, height) in history {
        if !transaction
            .input
            .iter()
            .any(|input| input.previous_output == observation.outpoint)
        {
            continue;
        }
        if observation.spend.is_some() {
            return Err(Error::Validation);
        }
        observation.spend = Some(info(transaction));
        observation.spend_confirmations = confirmations(*height, tip)?;
    }
    Ok(())
}

fn confirmations(height: i32, tip: u32) -> Result<u32> {
    if height <= 0 {
        return Ok(0);
    }
    tip.checked_sub(height as u32)
        .and_then(|depth| depth.checked_add(1))
        .ok_or(Error::Chain)
}

pub fn info(transaction: &Transaction) -> TransactionInfo {
    TransactionInfo {
        id: transaction.compute_txid().to_string(),
        hex: encode::serialize_hex(transaction),
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|_| Error::Chain)?
}

#[cfg(test)]
#[path = "chain_tests.rs"]
mod tests;
