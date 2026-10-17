//! Platform-independent logic of `st-client`.
//!
//! [`Client`] runs one background task that keeps a pinned-TLS connection to
//! `st-server`, registers torrents with their resume bitfield, re-verifies
//! and writes every received piece into pre-allocated files, persists the
//! bitfield in `<download dir>/<name>.st-state` and only then acknowledges the
//! piece. The connection is re-established with exponential backoff.

pub mod bencode;
mod client;
pub mod metainfo;
pub mod storage;
pub mod testkit;
pub mod tls;

pub use client::{
    AddError, Backoff, Client, ClientConfig, ConfigError, ConnectionState, Event, Notify,
    ServerStatus, TorrentSnapshot,
};
pub use metainfo::{InfoHash, Metainfo};
pub use st_protocol::TorrentState;
pub use tls::Fingerprint;
