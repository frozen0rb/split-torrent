use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use st_server::{config, tls, Config, Server};
use tracing_subscriber::EnvFilter;

/// Downloads torrents into RAM and streams them to a st-client over TLS.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// TOML config file.
    #[arg(short, long, env = "ST_SERVER_CONFIG")]
    config: Option<PathBuf>,
    /// Client listen address (overrides config).
    #[arg(long)]
    listen: Option<SocketAddr>,
    /// Client token (overrides config).
    #[arg(long, env = "ST_SERVER_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Directory for the TLS certificate and generated token.
    #[arg(long)]
    state_dir: Option<PathBuf>,
    /// RAM buffer limit, e.g. 384MiB (overrides config).
    #[arg(long, value_parser = config::parse_size)]
    buffer_limit: Option<u64>,
    /// BitTorrent listen port (overrides config).
    #[arg(long)]
    bt_port: Option<u16>,
    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the server (default).
    Run,
    /// Print the certificate fingerprint and token, creating them if needed.
    Show,
    /// Print a new random token.
    GenToken,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info,librqbit=warn,librqbit_dht=warn")),
        )
        .init();

    let cli = Cli::parse();
    let mut cfg = match &cli.config {
        Some(p) => Config::load(p)?,
        None => Config::default(),
    };
    if let Some(v) = cli.listen {
        cfg.listen = v;
    }
    if let Some(v) = cli.token {
        cfg.token = Some(v);
    }
    if let Some(v) = cli.state_dir {
        cfg.state_dir = v;
    }
    if let Some(v) = cli.buffer_limit {
        cfg.buffer_limit = v;
    }
    if let Some(v) = cli.bt_port {
        cfg.bt_port = v;
    }
    cfg.validate()?;

    match cli.cmd.unwrap_or(Cmd::Run) {
        Cmd::GenToken => println!("{}", tls::generate_token()),
        Cmd::Show => {
            let id = tls::TlsIdentity::load_or_generate(&cfg.state_dir)?;
            let token = match cfg.token {
                Some(t) => t,
                None => tls::load_or_generate_token(&cfg.state_dir)?.0,
            };
            println!("fingerprint: {}", id.fingerprint());
            println!("token:       {token}");
        }
        Cmd::Run => {
            let server = Server::bind(cfg).await?;
            println!(
                "TLS certificate fingerprint (SHA-256): {}",
                server.fingerprint()
            );
            println!("Listening for clients on {}", server.local_addr());
            let cancel = server.cancellation_token();
            tokio::spawn(async move {
                shutdown_signal().await;
                tracing::info!("shutting down");
                cancel.cancel();
            });
            server.run().await.context("server failed")?;
        }
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
