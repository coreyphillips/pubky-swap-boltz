//! Durable public swap records, written before native admission and HTTP acceptance.

use crate::{
    model::{CreateRequest, StoredSwap, SwapUpdate},
    Error, Result,
};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension};
use std::{
    fs::{File, OpenOptions},
    path::Path,
    sync::Mutex,
};
use uuid::Uuid;

mod snapshot;
pub use snapshot::{
    IdempotencyBinding, SnapshotSwap, StoreSnapshot, MAX_SNAPSHOT_BYTES, SNAPSHOT_VERSION,
};

pub struct Store {
    connection: Mutex<Connection>,
    _process_lock: Option<File>,
}

impl Store {
    pub fn open(directory: &Path, identity_binding: &str) -> Result<Self> {
        private_directory(directory)?;
        let lock = private_file(&directory.join("process.lock"))?;
        lock.try_lock_exclusive().map_err(|_| Error::Busy)?;
        let path = directory.join("swaps.sqlite3");
        private_file(&path)?;
        let connection = Connection::open(path)?;
        Self::initialize(connection, Some(lock), identity_binding)
    }

    pub fn memory(identity_binding: &str) -> Result<Self> {
        Self::initialize(Connection::open_in_memory()?, None, identity_binding)
    }

    fn initialize(connection: Connection, lock: Option<File>, binding: &str) -> Result<Self> {
        connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS swaps (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL UNIQUE, payment_hash TEXT NOT NULL UNIQUE, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS idempotency (key TEXT PRIMARY KEY, fingerprint TEXT NOT NULL);")?;
        connection.execute(
            "INSERT OR IGNORE INTO metadata VALUES ('binding',?1)",
            [binding],
        )?;
        let stored: String =
            connection.query_row("SELECT value FROM metadata WHERE key='binding'", [], |r| {
                r.get(0)
            })?;
        if stored != binding {
            return Err(Error::Invalid(
                "state directory belongs to another identity, provider, or network",
            ));
        }
        Ok(Self {
            connection: Mutex::new(connection),
            _process_lock: lock,
        })
    }

    pub fn reserve(
        &self,
        request: CreateRequest,
        payment_hash: String,
        key: Option<&str>,
    ) -> Result<StoredSwap> {
        let fingerprint = request.fingerprint()?;
        let mut connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let tx = connection.transaction()?;
        if let Some(key) = key {
            let previous: Option<String> = tx
                .query_row(
                    "SELECT fingerprint FROM idempotency WHERE key=?1",
                    [key],
                    |r| r.get(0),
                )
                .optional()?;
            if previous.as_ref().is_some_and(|p| p != &fingerprint) {
                return Err(Error::Conflict);
            }
            if previous.is_none() {
                let count: u64 =
                    tx.query_row("SELECT COUNT(*) FROM idempotency", [], |r| r.get(0))?;
                if count >= 10_000 {
                    return Err(Error::Busy);
                }
            }
            tx.execute(
                "INSERT OR IGNORE INTO idempotency VALUES (?1,?2)",
                params![key, fingerprint],
            )?;
        }
        let existing: Option<String> = tx
            .query_row(
                "SELECT body FROM swaps WHERE fingerprint=?1",
                [&fingerprint],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(body) = existing {
            tx.commit()?;
            return decode(&body);
        }
        let reused: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM swaps WHERE payment_hash=?1)",
            [&payment_hash],
            |r| r.get(0),
        )?;
        if reused {
            return Err(Error::Conflict);
        }
        let count: u64 = tx.query_row("SELECT COUNT(*) FROM swaps", [], |r| r.get(0))?;
        if count >= 10_000 {
            return Err(Error::Busy);
        }
        let id = Uuid::new_v4();
        let record = StoredSwap {
            id,
            update: SwapUpdate::initial(id, request.direction()),
            request,
            payment_hash,
            quote: None,
            native_request: None,
            admission_tip: None,
            accept: None,
            response: None,
        };
        tx.execute(
            "INSERT INTO swaps VALUES (?1,?2,?3,?4)",
            params![
                id.to_string(),
                fingerprint,
                record.payment_hash,
                encode(&record)?
            ],
        )?;
        tx.commit()?;
        Ok(record)
    }

    pub fn find_request(&self, request: &CreateRequest) -> Result<Option<StoredSwap>> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let body: Option<String> = connection
            .query_row(
                "SELECT body FROM swaps WHERE fingerprint=?1",
                [request.fingerprint()?],
                |r| r.get(0),
            )
            .optional()?;
        body.map(|s| decode(&s)).transpose()
    }

    pub fn save(&self, record: &StoredSwap) -> Result<()> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        if connection.execute(
            "UPDATE swaps SET body=?1 WHERE id=?2",
            params![encode(record)?, record.id.to_string()],
        )? != 1
        {
            return Err(Error::NotFound);
        }
        Ok(())
    }

    pub fn get(&self, id: Uuid) -> Result<StoredSwap> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let body: Option<String> = connection
            .query_row(
                "SELECT body FROM swaps WHERE id=?1",
                [id.to_string()],
                |r| r.get(0),
            )
            .optional()?;
        decode(&body.ok_or(Error::NotFound)?)
    }

    pub fn all(&self) -> Result<Vec<StoredSwap>> {
        let connection = self.connection.lock().map_err(|_| Error::Storage)?;
        let mut statement = connection.prepare("SELECT body FROM swaps ORDER BY rowid")?;
        let rows = statement.query_map([], |r| r.get::<_, String>(0))?;
        rows.map(|r| decode(&r?)).collect()
    }
}

fn encode(record: &StoredSwap) -> Result<String> {
    serde_json::to_string(record).map_err(|_| Error::Storage)
}
fn decode(body: &str) -> Result<StoredSwap> {
    serde_json::from_str(body).map_err(|_| Error::Storage)
}

fn private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).map_err(|_| Error::Storage)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| Error::Storage)?;
    }
    Ok(())
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path).map_err(|_| Error::Storage)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| Error::Storage)?;
    }
    Ok(file)
}
