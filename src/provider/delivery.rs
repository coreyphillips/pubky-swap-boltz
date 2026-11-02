//! Durable request intent and replies, isolated by transport identity, network and provider.

use super::*;
use bitcoin::{
    hashes::{sha256, Hash},
    Network,
};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::Mutex,
};

pub(super) struct DeliveryJournal {
    connection: Mutex<Connection>,
    _lock: Option<File>,
    pub directory: Option<PathBuf>,
}

impl DeliveryJournal {
    pub fn open(root: Option<&Path>, identity: &str, peer: &str, network: Network) -> Result<Self> {
        let binding = format!("{identity}|{network}|{peer}");
        let directory = root.map(|root| scoped_directory(root, &binding));
        let (connection, lock) = match &directory {
            Some(directory) => {
                crate::store::private_directory(directory)?;
                let lock = crate::store::private_file(&directory.join("delivery.lock"))?;
                lock.try_lock_exclusive().map_err(|_| Error::Busy)?;
                let path = directory.join("replies.sqlite3");
                crate::store::private_file(&path)?;
                (Connection::open(path)?, Some(lock))
            }
            None => (Connection::open_in_memory()?, None),
        };
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS requests (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS replies (id TEXT PRIMARY KEY, body TEXT NOT NULL, recorded_at INTEGER NOT NULL);")?;
        connection.execute(
            "INSERT OR IGNORE INTO metadata VALUES ('binding', ?1)",
            [&binding],
        )?;
        connection.execute("INSERT OR IGNORE INTO metadata VALUES ('version', '1')", [])?;
        let version: String = connection.query_row(
            "SELECT value FROM metadata WHERE key='version'",
            [],
            |row| row.get(0),
        )?;
        if version != "1" {
            return Err(Error::Invalid("unsupported delivery journal version"));
        }
        let stored: String = connection.query_row(
            "SELECT value FROM metadata WHERE key='binding'",
            [],
            |row| row.get(0),
        )?;
        if stored != binding {
            return Err(Error::Invalid(
                "delivery journal belongs to another identity, network, or provider",
            ));
        }
        Ok(Self {
            connection: Mutex::new(connection),
            _lock: lock,
            directory,
        })
    }

    pub fn prepare(&self, message: &SwapMessage) -> Result<()> {
        // Creation intent must survive a cancelled or ambiguous send. Read-only requests carry
        // their correlation IDs in the caller and do not require durable admission records.
        let SwapMessage::SwapRequest(request) = message else {
            return Ok(());
        };
        let id = request.quote_id.to_string();
        let body = serde_json::to_string(message).map_err(|_| Error::Storage)?;
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let previous: Option<String> = connection
            .query_row("SELECT body FROM requests WHERE id=?1", [&id], |row| {
                row.get(0)
            })
            .optional()?;
        if previous.as_ref().is_some_and(|previous| previous != &body) {
            return Err(Error::Conflict);
        }
        connection.execute(
            "INSERT OR IGNORE INTO requests VALUES (?1, ?2)",
            params![id, body],
        )?;
        Ok(())
    }

    pub fn save_reply(&self, message: &SwapMessage) -> Result<()> {
        let body = serde_json::to_string(message).map_err(|_| Error::Storage)?;
        let id = reply_id(message)
            .unwrap_or_else(|| format!("event:{}", sha256::Hash::hash(body.as_bytes())));
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        if matches!(message, SwapMessage::SwapAccept(_)) {
            let previous: Option<String> = connection
                .query_row("SELECT body FROM replies WHERE id=?1", [&id], |row| {
                    row.get(0)
                })
                .optional()?;
            if previous.as_ref().is_some_and(|previous| previous != &body) {
                return Err(Error::Conflict);
            }
        }
        connection.execute(
            "INSERT OR REPLACE INTO replies VALUES (?1, ?2, ?3)",
            params![id, body, crate::validation::now()],
        )?;
        Ok(())
    }

    pub fn reply(&self, key: ReplyKey) -> Result<Option<SwapMessage>> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let body: Option<String> = connection
            .query_row("SELECT body FROM replies WHERE id=?1", [key.id()], |row| {
                row.get(0)
            })
            .optional()?;
        body.map(|body| serde_json::from_str(&body).map_err(|_| Error::Storage))
            .transpose()
    }
}

fn scoped_directory(root: &Path, binding: &str) -> PathBuf {
    root.join("delivery")
        .join(sha256::Hash::hash(binding.as_bytes()).to_string())
}

fn reply_id(message: &SwapMessage) -> Option<String> {
    match message {
        SwapMessage::Offer(reply) => reply.request_id.map(ReplyKey::Offer),
        SwapMessage::Quote(reply) => reply.request_id.map(ReplyKey::Quote),
        SwapMessage::SwapAccept(reply) => Some(ReplyKey::Accept(reply.quote_id)),
        SwapMessage::SwapStatusSnapshot(reply) => reply.request_id.map(ReplyKey::Snapshot),
        _ => None,
    }
    .map(ReplyKey::id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_restores_a_saved_reply() {
        let directory = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let reply = SwapMessage::Offer(SwapOffer {
            request_id: Some(id),
            ..serde_json::from_value(serde_json::json!({
                "offer_id": Uuid::new_v4(), "provider_pkarr": "provider", "directions": ["reverse"],
                "min_amount_sat": 100, "max_amount_sat": 10000, "base_fee_sat": 1,
                "fee_ppm": 100, "onchain_fee_sat": 1, "valid_until_unix": 100,
                "protocol_version": 1, "features": [], "required_confirmations": 1,
                "network": "regtest", "htlc_timeout_blocks": 144, "lightning_node_id": null
            }))
            .unwrap()
        });
        let journal =
            DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
                .unwrap();
        journal.save_reply(&reply).unwrap();
        drop(journal);
        let reopened =
            DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
                .unwrap();
        assert_eq!(
            serde_json::to_value(reopened.reply(ReplyKey::Offer(id)).unwrap()).unwrap(),
            serde_json::to_value(Some(reply)).unwrap()
        );
    }

    #[test]
    fn journals_are_isolated_and_exclusive() {
        let directory = tempfile::tempdir().unwrap();
        let first =
            DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest)
                .unwrap();
        assert!(matches!(
            DeliveryJournal::open(Some(directory.path()), "owner", "peer", Network::Regtest),
            Err(Error::Busy)
        ));
        for (owner, peer, network) in [
            ("other", "peer", Network::Regtest),
            ("owner", "other", Network::Regtest),
            ("owner", "peer", Network::Bitcoin),
        ] {
            let other =
                DeliveryJournal::open(Some(directory.path()), owner, peer, network).unwrap();
            assert_ne!(first.directory, other.directory);
        }
    }
}
