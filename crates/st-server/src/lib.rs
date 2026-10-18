//! Split-torrent server.
//!
//! Downloads torrents with librqbit into a capped RAM buffer and streams
//! hash-verified pieces to an authenticated client over TLS. Torrent data is
//! never written to disk.
//!
//! Embedding (e.g. in tests):
//!
//! ```no_run
//! # async fn f() -> anyhow::Result<()> {
//! let config = st_server::Config {
//!     listen: "127.0.0.1:0".parse()?,
//!     state_dir: "/tmp/st-test".into(),
//!     ..Default::default()
//! };
//! let server = st_server::Server::bind(config).await?;
//! let (addr, fingerprint, token) = (server.local_addr(), server.fingerprint().to_owned(), server.token().to_owned());
//! let handle = server.spawn();
//! // ... connect a client ...
//! handle.shutdown().await;
//! # Ok(()) }
//! ```

pub mod buffer;
pub mod config;
mod conn;
pub mod engine;
pub mod net;
pub mod storage;
pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;

pub use config::Config;
pub use engine::Engine;

pub struct Server {
    listener: TcpListener,
    acceptor: TlsAcceptor,
    engine: Arc<Engine>,
    token: Arc<str>,
    fingerprint: String,
    cancel: CancellationToken,
}

impl Server {
    /// Loads (or creates) the TLS identity and token, starts the BitTorrent
    /// session and binds the client listener.
    pub async fn bind(config: Config) -> anyhow::Result<Self> {
        config.validate()?;
        let _ = rustls::crypto::ring::default_provider().install_default();
        let identity = tls::TlsIdentity::load_or_generate(&config.state_dir)?;
        let token: Arc<str> = match &config.token {
            Some(t) => t.as_str().into(),
            None => {
                let (t, generated) = tls::load_or_generate_token(&config.state_dir)?;
                if generated {
                    tracing::info!(
                        path = %config.state_dir.join("token").display(),
                        "generated client token"
                    );
                }
                t.into()
            }
        };
        let acceptor = identity.acceptor()?;
        let listener = TcpListener::bind(config.listen)
            .await
            .with_context(|| format!("binding {}", config.listen))?;
        let cancel = CancellationToken::new();
        let engine = Engine::new(&config, cancel.child_token()).await?;
        Ok(Self {
            listener,
            acceptor,
            engine,
            token,
            fingerprint: identity.fingerprint(),
            cancel,
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().expect("bound listener")
    }

    /// SHA-256 certificate fingerprint for the client to pin.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    /// Address librqbit accepts BitTorrent peers on.
    pub fn bt_listen_addr(&self) -> Option<SocketAddr> {
        self.engine.bt_listen_addr()
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Accepts clients until the cancellation token fires.
    pub async fn run(self) -> anyhow::Result<()> {
        tracing::info!(
            listen = %self.local_addr(),
            bt = ?self.bt_listen_addr(),
            buffer_limit = self.engine.pool().limit(),
            "st-server ready"
        );
        loop {
            let (tcp, peer) = tokio::select! {
                _ = self.cancel.cancelled() => break,
                r = self.listener.accept() => match r {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!("accept failed: {e}");
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                        continue;
                    }
                },
            };
            let _ = tcp.set_nodelay(true);
            let acceptor = self.acceptor.clone();
            let engine = self.engine.clone();
            let token = self.token.clone();
            let cancel = self.cancel.clone();
            tokio::spawn(async move {
                let tls = match tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    acceptor.accept(tcp),
                )
                .await
                {
                    Ok(Ok(s)) => s,
                    Ok(Err(e)) => {
                        tracing::info!(%peer, "TLS handshake failed: {e}");
                        return;
                    }
                    Err(_) => return,
                };
                tokio::select! {
                    _ = cancel.cancelled() => {}
                    _ = conn::handle(engine, token, tls, peer) => {}
                }
            });
        }
        self.engine.shutdown().await;
        Ok(())
    }

    pub fn spawn(self) -> ServerHandle {
        let cancel = self.cancel.clone();
        let addr = self.local_addr();
        ServerHandle {
            addr,
            engine: self.engine.clone(),
            cancel,
            task: tokio::spawn(self.run()),
        }
    }
}

pub struct ServerHandle {
    pub addr: SocketAddr,
    pub engine: Arc<Engine>,
    cancel: CancellationToken,
    task: JoinHandle<anyhow::Result<()>>,
}

impl ServerHandle {
    pub async fn shutdown(self) {
        self.cancel.cancel();
        let _ = self.task.await;
    }
}
