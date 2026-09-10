//! Local Boltz compatibility for end-user-controlled Pubky Swap identities.

pub mod chain;
#[cfg(feature = "server")]
pub mod config;
pub mod error;
pub mod fees;
#[cfg(feature = "server")]
pub mod http;
pub mod model;
pub mod provider;
pub mod service;
pub mod store;
mod validation;
#[cfg(feature = "server")]
mod websocket;

pub use error::{Error, Result};
pub use service::Bridge;

pub use model::{RefundInfo, SpendInfo};
pub use provider::{canonical_pubky, identity_from_secret};
pub use pubky_transport;
pub use swap_common;

#[cfg(all(feature = "rendezvous", target_os = "android"))]
pub use pubky_transport::p2p::install_android_jni_context;

#[cfg(all(feature = "mobile", target_os = "android"))]
mod android;
#[cfg(all(feature = "mobile", target_os = "android"))]
pub use android::initialize_android_verifier;
