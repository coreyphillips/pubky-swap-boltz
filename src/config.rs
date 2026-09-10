//! Local runtime configuration. Identity secrets can be read from private files.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};
use swap_common::swap::NetworkSpec;

#[derive(Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub bind: SocketAddr,
    pub network: NetworkSpec,
    pub provider: String,
    pub recovery_file: String,
    pub recovery_phrase: swap_config::SecretSource,
    pub passphrase: swap_config::SecretSource,
    pub data_dir: PathBuf,
    pub electrum_url: String,
    pub request_timeout_seconds: u64,
    pub poll_seconds: u64,
    pub max_fee_bps: u16,
    pub max_amount_sat: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from(([127, 0, 0, 1], 9001)),
            network: NetworkSpec::Regtest,
            provider: String::new(),
            recovery_file: String::new(),
            recovery_phrase: Default::default(),
            passphrase: Default::default(),
            data_dir: "./data".into(),
            electrum_url: "tcp://127.0.0.1:60001".into(),
            request_timeout_seconds: 60,
            poll_seconds: 5,
            max_fee_bps: 1000,
            max_amount_sat: 1_000_000,
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if !self.bind.ip().is_loopback() {
            return Err(Error::Invalid("the proxy must bind to a loopback address"));
        }
        if self.provider.is_empty() {
            return Err(Error::Invalid("provider is required"));
        }
        if self.request_timeout_seconds == 0
            || self.request_timeout_seconds > 300
            || self.poll_seconds == 0
            || self.max_amount_sat == 0
            || self.max_fee_bps == 0
        {
            return Err(Error::Invalid("invalid runtime bounds"));
        }
        Ok(())
    }
}
