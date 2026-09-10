use clap::Parser;
use pubky_swap_boltz::{
    chain::ElectrumChain,
    config::Config,
    http,
    provider::PubkyProvider,
    service::{Bridge, BridgeSettings},
    store::Store,
};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(
    version,
    about = "Local Bitcoin compatibility between Boltz clients and Pubky Swap"
)]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    let raw = std::fs::read_to_string(args.config)?;
    let config: Config =
        toml::from_str(&raw).map_err(|_| anyhow::anyhow!("invalid configuration"))?;
    config.validate()?;
    let identity = swap_config::resolve_identity(
        &config.recovery_file,
        &config.recovery_phrase,
        &config.passphrase,
    )?;
    let transport = if identity.method == "file" {
        pubky_transport::Transport::from_recovery_file(&identity.value, &identity.passphrase).await
    } else {
        pubky_transport::Transport::from_recovery_phrase(
            &identity.value,
            Some(&identity.passphrase),
        )
        .await
    }
    .map_err(|_| anyhow::anyhow!("Pubky sign-in failed"))?;
    let binding = format!(
        "{}|{}|{:?}",
        transport.public_key_string(),
        config.provider,
        config.network
    );
    let store = Store::open(&config.data_dir, &binding)?;
    let provider = PubkyProvider::new(
        Arc::new(transport),
        config.provider,
        Duration::from_secs(config.request_timeout_seconds),
    )
    .await?;
    let chain =
        ElectrumChain::connect(config.electrum_url, config.network.to_bitcoin_network()).await?;
    let bridge = Bridge::new(
        Arc::new(provider),
        Arc::new(chain),
        store,
        BridgeSettings {
            network: config.network.to_bitcoin_network(),
            max_fee_bps: config.max_fee_bps,
            max_amount_sat: config.max_amount_sat,
        },
    );
    bridge.offer().await?;
    let listener = tokio::net::TcpListener::bind(config.bind).await?;
    let worker = bridge.clone();
    let interval = Duration::from_secs(config.poll_seconds);
    tokio::spawn(async move {
        loop {
            if let Err(error) = worker.recover_and_refresh().await {
                tracing::error!("{error}");
            }
            tokio::time::sleep(interval).await;
        }
    });
    tracing::info!(address=%config.bind,"local proxy ready");
    axum::serve(listener, http::router(bridge, config.bind))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
