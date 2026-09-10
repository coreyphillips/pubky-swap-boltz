//! Errors carry stable public text, never remote messages or sensitive payloads.

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(&'static str),
    #[error("unsupported feature")]
    Unsupported,
    #[error("swap not found")]
    NotFound,
    #[error("request conflicts with a previously recorded swap")]
    Conflict,
    #[error("provider unavailable; retry the same request to recover its result")]
    Provider,
    #[error("provider response failed validation")]
    Validation,
    #[error("chain service unavailable")]
    Chain,
    #[error("persistent state unavailable")]
    Storage,
    #[error("local request capacity reached")]
    Busy,
}

impl From<rusqlite::Error> for Error {
    fn from(_: rusqlite::Error) -> Self {
        Self::Storage
    }
}
