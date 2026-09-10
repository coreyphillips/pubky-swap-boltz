//! Versioned logical backups of public swap contracts and retry bindings.

use super::{decode, encode, Store};
use crate::{
    model::{CreateRequest, StoredSwap, SwapUpdate},
    service::{creation_response, native_request},
    validation, Error, Result,
};
use bitcoin::Network;
use fs2::FileExt;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde::{Deserialize, Deserializer, Serialize};
use std::{collections::BTreeMap, fs::OpenOptions, path::Path};
use swap_common::{
    messages::{QuoteRequest, PROTOCOL_VERSION},
    timelock::TimelockParams,
    validate::ClientPolicy,
};

pub const SNAPSHOT_VERSION: u32 = 1;
pub const MAX_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ENTRIES: usize = 10_000;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreSnapshot {
    pub version: u32,
    pub identity_binding: String,
    pub swaps: Vec<SnapshotSwap>,
    pub idempotency: Vec<IdempotencyBinding>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotSwap {
    pub fingerprint: String,
    #[serde(deserialize_with = "strict_record")]
    pub record: StoredSwap,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdempotencyBinding {
    pub key: String,
    pub fingerprint: String,
}

// Shared protocol types intentionally tolerate future wire fields. A backup instead uses an
// exact schema, preventing unknown fields or opaque response objects from carrying secrets.
fn strict_record<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<StoredSwap, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    let record: StoredSwap =
        serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
    if serde_json::to_value(&record).map_err(serde::de::Error::custom)? != value {
        return Err(serde::de::Error::custom(
            "noncanonical swap snapshot fields",
        ));
    }
    Ok(record)
}

impl StoreSnapshot {
    /// Parse a bounded, exact-schema JSON snapshot before validating its contracts.
    pub fn from_json(json: &str) -> Result<Self> {
        if json.len() > MAX_SNAPSHOT_BYTES {
            return Err(Error::Invalid("snapshot exceeds size limit"));
        }
        let snapshot: Self = serde_json::from_str(json).map_err(|_| Error::Validation)?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(self).map_err(|_| Error::Storage)
    }

    /// Validate without opening a database or contacting a provider or chain service.
    pub fn validate(&self) -> Result<()> {
        if self.version != SNAPSHOT_VERSION
            || self.swaps.len() > MAX_ENTRIES
            || self.idempotency.len() > MAX_ENTRIES
            || serde_json::to_vec(self)
                .map_err(|_| Error::Validation)?
                .len()
                > MAX_SNAPSHOT_BYTES
        {
            return Err(Error::Invalid("unsupported snapshot version or size"));
        }
        let (identity, network) = binding(&self.identity_binding)?;
        let mut ids = BTreeMap::new();
        let mut fingerprints = BTreeMap::new();
        let mut hashes = BTreeMap::new();
        let mut quote_ids = BTreeMap::new();
        let mut native_ids = BTreeMap::new();
        for entry in &self.swaps {
            validate_record(entry, &identity, network)?;
            let record = &entry.record;
            if ids.insert(record.id, ()).is_some()
                || fingerprints
                    .insert(entry.fingerprint.as_str(), ())
                    .is_some()
                || hashes.insert(record.payment_hash.as_str(), ()).is_some()
                || record
                    .quote
                    .as_ref()
                    .is_some_and(|quote| quote_ids.insert(quote.quote_id, ()).is_some())
                || record
                    .accept
                    .as_ref()
                    .is_some_and(|accept| native_ids.insert(accept.swap_id, ()).is_some())
            {
                return Err(Error::Conflict);
            }
        }
        let mut keys = BTreeMap::new();
        for entry in &self.idempotency {
            if entry.key.is_empty()
                || entry.key.len() > 256
                || keys.insert(entry.key.as_str(), ()).is_some()
                || !fingerprints.contains_key(entry.fingerprint.as_str())
            {
                return Err(Error::Validation);
            }
        }
        Ok(())
    }
}

fn binding(value: &str) -> Result<(String, Network)> {
    let parts: Vec<_> = value.split('|').collect();
    if parts.len() != 3 || value.len() > 512 {
        return Err(Error::Invalid("snapshot identity binding is invalid"));
    }
    let identity = crate::canonical_pubky(parts[0])?;
    crate::canonical_pubky(parts[1])?;
    let network = match parts[2] {
        "bitcoin" | "Bitcoin" | "mainnet" => Network::Bitcoin,
        "testnet" | "Testnet" => Network::Testnet,
        "signet" | "Signet" => Network::Signet,
        "regtest" | "Regtest" => Network::Regtest,
        _ => return Err(Error::Invalid("snapshot network is invalid")),
    };
    Ok((identity, network))
}

fn validate_record(entry: &SnapshotSwap, identity: &str, network: Network) -> Result<()> {
    let record = &entry.record;
    let mut request = record.request.clone();
    if record.id.is_nil()
        || record.update.id != record.id
        || record.update.transaction.is_some()
        || !record.update.zero_conf_rejected
        || !valid_status(&record.update.status)
        || validation::request(&mut request, network)? != record.payment_hash
        || record.request.fingerprint()? != entry.fingerprint
        || request.fingerprint()? != entry.fingerprint
    {
        return Err(Error::Validation);
    }
    let Some(quote) = &record.quote else {
        if record.native_request.is_some()
            || record.admission_tip.is_some()
            || record.accept.is_some()
            || record.response.is_some()
        {
            return Err(Error::Validation);
        }
        return Ok(());
    };
    if record.native_request.as_ref() != Some(&native_request(record, quote, identity.into())) {
        return Err(Error::Validation);
    }
    let amount_matches = match &request {
        CreateRequest::Submarine(request) => {
            validation::invoice(&request.invoice, network)?.amount_milli_satoshis()
                == quote.amount_sat.checked_mul(1000)
        }
        CreateRequest::Reverse(request) if request.onchain_amount > 0 => {
            request.onchain_amount == quote.amount_sat
        }
        CreateRequest::Reverse(request) => request.invoice_amount == quote.total_sat,
    };
    if !amount_matches
        || quote.amount_sat == 0
        || quote.amount_sat > bitcoin::Amount::MAX_MONEY.to_sat()
        || quote.total_sat > bitcoin::Amount::MAX_MONEY.to_sat()
        || quote.htlc_timeout_blocks == 0
        || quote.htlc_timeout_blocks >= 500_000_000
        || record.admission_tip.is_some_and(|tip| tip >= 500_000_000)
    {
        return Err(Error::Validation);
    }
    let mut policy = ClientPolicy::for_network(network, TimelockParams::default());
    // Preserve previously admitted contracts even if the application's current fee cap changed.
    policy.max_fee_bps = u16::MAX;
    let quote_request = QuoteRequest {
        request_id: quote.request_id,
        offer_id: quote.offer_id,
        client_pkarr: identity.into(),
        direction: request.direction(),
        amount_sat: quote.amount_sat,
        protocol_version: PROTOCOL_VERSION,
        features: Vec::new(),
    };
    // A backup can outlive its quote. Validate immutable economics at its final valid second.
    swap_common::validate::validate_quote(
        quote,
        &quote_request,
        quote.valid_until_unix.saturating_sub(1),
        &policy,
    )
    .map_err(|_| Error::Validation)?;
    match (&record.accept, &record.response) {
        (Some(accept), Some(response)) => {
            if accept.timeout_block_height >= 500_000_000 {
                return Err(Error::Validation);
            }
            validation::persisted_accept(record, &policy, network)?;
            if *response != creation_response(record.id, accept)? {
                return Err(Error::Validation);
            }
        }
        (None, None) => {}
        _ => return Err(Error::Validation),
    }
    Ok(())
}

fn valid_status(status: &str) -> bool {
    matches!(
        status,
        "swap.created"
            | "invoice.set"
            | "invoice.pending"
            | "invoice.paid"
            | "invoice.settled"
            | "transaction.mempool"
            | "transaction.confirmed"
            | "transaction.claimed"
            | "transaction.refunded"
            | "swap.expired"
            | "transaction.failed"
    )
}

impl Store {
    pub fn export_snapshot(&self) -> Result<StoreSnapshot> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let snapshot = read_snapshot(&connection)?;
        snapshot.validate()?;
        Ok(snapshot)
    }

    /// Check all existing bindings and immutable contracts without modifying the database.
    pub fn validate_import(&self, snapshot: &StoreSnapshot) -> Result<()> {
        snapshot.validate()?;
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        merge(read_snapshot(&connection)?, snapshot).map(|_| ())
    }

    /// Merge atomically. Existing progress wins; new records require fresh observations.
    pub fn import_snapshot(&self, snapshot: &StoreSnapshot) -> Result<()> {
        snapshot.validate()?;
        let mut connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let transaction = connection.transaction()?;
        let merged = merge(read_snapshot(&transaction)?, snapshot)?;
        for mut entry in merged.swaps {
            // Preserve local observations exactly. Only exported and newly restored records
            // omit transaction caches; existing witness data remains in the local cache.
            let existing: Option<String> = transaction
                .query_row(
                    "SELECT body FROM swaps WHERE id=?1",
                    [entry.record.id.to_string()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(body) = existing {
                entry.record.update = decode(&body)?.update;
            }
            transaction.execute(
                "INSERT INTO swaps VALUES (?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET body=excluded.body",
                params![entry.record.id.to_string(), entry.fingerprint, entry.record.payment_hash, encode(&entry.record)?],
            )?;
        }
        for entry in merged.idempotency {
            transaction.execute(
                "INSERT OR IGNORE INTO idempotency VALUES (?1,?2)",
                params![entry.key, entry.fingerprint],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Read a disconnected store while holding its normal exclusive process lock.
    /// An active store must instead be exported through its existing Store or Bridge handle.
    pub fn snapshot_directory(directory: &Path) -> Result<StoreSnapshot> {
        let store = open_existing(directory)?;
        store.export_snapshot()
    }

    pub fn validate_directory_import(directory: &Path, snapshot: &StoreSnapshot) -> Result<()> {
        snapshot.validate()?;
        if directory
            .join("swaps.sqlite3")
            .try_exists()
            .map_err(|_| Error::Storage)?
        {
            open_existing(directory)?.validate_import(snapshot)?;
        }
        Ok(())
    }

    pub fn import_directory(directory: &Path, snapshot: &StoreSnapshot) -> Result<()> {
        Self::validate_directory_import(directory, snapshot)?;
        Self::open(directory, &snapshot.identity_binding)?.import_snapshot(snapshot)
    }
}

fn open_existing(directory: &Path) -> Result<Store> {
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("process.lock"))
        .map_err(|_| Error::Storage)?;
    lock.try_lock_exclusive().map_err(|_| Error::Busy)?;
    let connection = Connection::open_with_flags(
        directory.join("swaps.sqlite3"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    Ok(Store {
        connection: std::sync::Mutex::new(connection),
        _process_lock: Some(lock),
    })
}

fn read_snapshot(connection: &Connection) -> Result<StoreSnapshot> {
    let identity_binding = connection.query_row(
        "SELECT value FROM metadata WHERE key='binding'",
        [],
        |row| row.get(0),
    )?;
    let mut swaps = Vec::new();
    let mut statement =
        connection.prepare("SELECT id,fingerprint,payment_hash,body FROM swaps ORDER BY id")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
        ))
    })?;
    for row in rows {
        let (id, fingerprint, hash, body) = row?;
        let mut record = decode(&body)?;
        if id != record.id.to_string()
            || hash != record.payment_hash
            || fingerprint != record.request.fingerprint()?
        {
            return Err(Error::Validation);
        }
        record.update.transaction = None;
        swaps.push(SnapshotSwap {
            fingerprint,
            record,
        });
    }
    let mut statement =
        connection.prepare("SELECT key,fingerprint FROM idempotency ORDER BY key")?;
    let idempotency = statement
        .query_map([], |row| {
            Ok(IdempotencyBinding {
                key: row.get(0)?,
                fingerprint: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(StoreSnapshot {
        version: SNAPSHOT_VERSION,
        identity_binding,
        swaps,
        idempotency,
    })
}

fn merge(local: StoreSnapshot, incoming: &StoreSnapshot) -> Result<StoreSnapshot> {
    local.validate()?;
    if local.identity_binding != incoming.identity_binding {
        return Err(Error::Invalid(
            "snapshot belongs to another identity, provider, or network",
        ));
    }
    let mut swaps: BTreeMap<_, _> = local
        .swaps
        .into_iter()
        .map(|entry| (entry.record.id, entry))
        .collect();
    for entry in &incoming.swaps {
        if let Some(existing) = swaps.get_mut(&entry.record.id) {
            if existing.fingerprint != entry.fingerprint
                || existing.record.payment_hash != entry.record.payment_hash
            {
                return Err(Error::Conflict);
            }
            merge_field(&mut existing.record.quote, &entry.record.quote)?;
            merge_field(
                &mut existing.record.native_request,
                &entry.record.native_request,
            )?;
            merge_field(
                &mut existing.record.admission_tip,
                &entry.record.admission_tip,
            )?;
            merge_field(&mut existing.record.accept, &entry.record.accept)?;
            merge_field(&mut existing.record.response, &entry.record.response)?;
        } else {
            let mut entry = entry.clone();
            entry.record.update =
                SwapUpdate::initial(entry.record.id, entry.record.request.direction());
            swaps.insert(entry.record.id, entry);
        }
    }
    let mut keys: BTreeMap<_, _> = local
        .idempotency
        .into_iter()
        .map(|entry| (entry.key, entry.fingerprint))
        .collect();
    for entry in &incoming.idempotency {
        if keys
            .get(&entry.key)
            .is_some_and(|fingerprint| *fingerprint != entry.fingerprint)
        {
            return Err(Error::Conflict);
        }
        keys.insert(entry.key.clone(), entry.fingerprint.clone());
    }
    let merged = StoreSnapshot {
        version: SNAPSHOT_VERSION,
        identity_binding: local.identity_binding,
        swaps: swaps.into_values().collect(),
        idempotency: keys
            .into_iter()
            .map(|(key, fingerprint)| IdempotencyBinding { key, fingerprint })
            .collect(),
    };
    merged.validate()?;
    Ok(merged)
}

fn merge_field<T: Clone + Serialize>(local: &mut Option<T>, incoming: &Option<T>) -> Result<()> {
    match (local.as_ref(), incoming) {
        (Some(a), Some(b))
            if serde_json::to_value(a).map_err(|_| Error::Validation)?
                != serde_json::to_value(b).map_err(|_| Error::Validation)? =>
        {
            Err(Error::Conflict)
        }
        (None, Some(value)) => {
            *local = Some(value.clone());
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;
