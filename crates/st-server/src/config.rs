use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Deserializer};

pub const DEFAULT_BUFFER_LIMIT: u64 = 384 * 1024 * 1024;
pub const DEFAULT_MAX_INFLIGHT_PIECES: usize = 32;
pub const DEFAULT_BT_PORT: u16 = 4240;

/// Server configuration, usually loaded from TOML. All fields have defaults.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// TLS listen address for clients.
    pub listen: SocketAddr,
    /// Shared secret clients must send. If unset, one is generated and stored
    /// in `state_dir/token`.
    pub token: Option<String>,
    /// Holds the TLS certificate/key and the generated token. Torrent data is
    /// never written here (or anywhere else).
    pub state_dir: PathBuf,
    /// RAM budget for partial and not-yet-ACKed pieces, e.g. `"384MiB"`.
    #[serde(deserialize_with = "de_size")]
    pub buffer_limit: u64,
    /// Swarm downloads are throttled while this many pieces are partially
    /// downloaded.
    pub max_inflight_pieces: usize,
    /// BitTorrent listen port (TCP). `0` picks a random port.
    pub bt_port: u16,
    pub dht: bool,
    /// Local service discovery (multicast on the LAN).
    pub lsd: bool,
    pub upnp: bool,
    pub trackers: bool,
    /// Serve pieces that are still in RAM to other peers.
    pub seed: bool,
    /// Max connected peers per torrent.
    pub peer_limit: usize,
    /// Swarm download limit in bytes per second, `0` = unlimited.
    #[serde(deserialize_with = "de_size")]
    pub download_limit: u64,
    #[serde(deserialize_with = "de_size")]
    pub upload_limit: u64,
    /// Peers to connect to for every torrent, in addition to DHT/trackers.
    pub initial_peers: Vec<SocketAddr>,
    /// Use IPv6 for BitTorrent in addition to IPv4. Unset = detect at startup
    /// (off when the host has no IPv6 stack).
    pub ipv6: Option<bool>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], st_protocol::DEFAULT_PORT)),
            token: None,
            state_dir: PathBuf::from("/var/lib/st-server"),
            buffer_limit: DEFAULT_BUFFER_LIMIT,
            max_inflight_pieces: DEFAULT_MAX_INFLIGHT_PIECES,
            bt_port: DEFAULT_BT_PORT,
            dht: true,
            lsd: true,
            upnp: false,
            trackers: true,
            seed: true,
            peer_limit: 64,
            download_limit: 0,
            upload_limit: 0,
            initial_peers: Vec::new(),
            ipv6: None,
        }
    }
}

impl Config {
    pub fn from_toml(s: &str) -> anyhow::Result<Self> {
        let c: Self = toml::from_str(s)?;
        c.validate()?;
        Ok(c)
    }

    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::from_toml(&s).with_context(|| format!("parsing config {}", path.display()))
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.buffer_limit >= 16 * 1024 * 1024,
            "buffer_limit must be at least 16MiB"
        );
        anyhow::ensure!(
            self.max_inflight_pieces > 0,
            "max_inflight_pieces must be > 0"
        );
        if let Some(t) = &self.token {
            anyhow::ensure!(t.len() >= 16, "token must be at least 16 characters");
        }
        Ok(())
    }
}

/// Parses `1234`, `"64KiB"`, `"384MiB"`, `"1GiB"`, `"10MB"` and so on.
pub fn parse_size(s: &str) -> anyhow::Result<u64> {
    let s = s.trim();
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: u64 = num.parse().with_context(|| format!("bad size {s:?}"))?;
    let mult: u64 = match unit.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kib" => 1 << 10,
        "m" | "mib" => 1 << 20,
        "g" | "gib" => 1 << 30,
        "kb" => 1_000,
        "mb" => 1_000_000,
        "gb" => 1_000_000_000,
        other => anyhow::bail!("unknown size unit {other:?}"),
    };
    n.checked_mul(mult).context("size overflows")
}

fn de_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(u64),
        S(String),
    }
    match V::deserialize(d)? {
        V::N(n) => Ok(n),
        V::S(s) => parse_size(&s).map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_size("384MiB").unwrap(), 384 << 20);
        assert_eq!(parse_size("1 GiB").unwrap(), 1 << 30);
        assert_eq!(parse_size("10MB").unwrap(), 10_000_000);
        assert_eq!(parse_size("42").unwrap(), 42);
        assert!(parse_size("1XB").is_err());
        assert!(parse_size("MiB").is_err());
    }

    #[test]
    fn example_config_parses() {
        let c = Config::from_toml(include_str!("../../../deploy/st-server.toml")).unwrap();
        assert_eq!(c.buffer_limit, DEFAULT_BUFFER_LIMIT);
        assert_eq!(c.state_dir, Config::default().state_dir);
    }

    #[test]
    fn toml_defaults_and_overrides() {
        let c = Config::from_toml("").unwrap();
        assert_eq!(c.buffer_limit, DEFAULT_BUFFER_LIMIT);
        assert_eq!(c.listen.port(), 7443);

        let c = Config::from_toml(
            r#"
            listen = "127.0.0.1:9000"
            token = "0123456789abcdef"
            state_dir = "/tmp/x"
            buffer_limit = "128MiB"
            download_limit = 1048576
            dht = false
            initial_peers = ["127.0.0.1:6881"]
            ipv6 = false
            "#,
        )
        .unwrap();
        assert_eq!(c.buffer_limit, 128 << 20);
        assert_eq!(c.download_limit, 1 << 20);
        assert!(!c.dht);
        assert_eq!(c.initial_peers.len(), 1);
        assert_eq!(c.ipv6, Some(false));
        assert_eq!(Config::default().ipv6, None);

        assert!(Config::from_toml("buffer_limit = \"1MiB\"").is_err());
        assert!(Config::from_toml("token = \"short\"").is_err());
        assert!(Config::from_toml("unknown_key = 1").is_err());
    }
}
