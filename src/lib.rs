//! Local Boltz compatibility for end-user-controlled Pubky Swap identities.

pub mod chain;
pub mod config;
pub mod error;
pub mod fees;
pub mod http;
pub mod model;
pub mod provider;
pub mod service;
pub mod store;
mod validation;
mod websocket;

pub use error::{Error, Result};
pub use service::Bridge;
