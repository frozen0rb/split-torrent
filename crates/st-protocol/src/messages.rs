use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::Bitfield;

/// Bumped on any incompatible wire change. The server rejects other versions.
pub const PROTOCOL_VERSION: u16 = 1;

/// Server-assigned torrent handle, stable for the lifetime of the server
/// process. Re-adding the same torrent (same info-hash) returns the same id.
pub type TorrentId = u32;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ClientMessage {
    /// Must be the first message on a connection.
    Hello {
        token: String,
        version: u16,
    },
    /// Start (or continue) a torrent. `have` marks pieces the client already
    /// has on disk; the server will neither download nor send them. An empty
    /// bitfield (`len == 0`) means "nothing yet".
    AddTorrent {
        torrent_bytes: Bytes,
        have: Bitfield,
    },
    /// The piece has been written and verified on the client; the server may
    /// free it.
    Ack {
        torrent_id: TorrentId,
        index: u32,
    },
    Pause {
        torrent_id: TorrentId,
    },
    Resume {
        torrent_id: TorrentId,
    },
    /// Stop the torrent on the server and drop all its buffered pieces.
    Remove {
        torrent_id: TorrentId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ServerMessage {
    HelloOk {
        version: u16,
        /// Server RAM buffer limit in bytes.
        buffer_limit: u64,
    },
    /// Reply to `AddTorrent`.
    TorrentAdded {
        torrent_id: TorrentId,
        info_hash: [u8; 20],
        name: String,
        piece_count: u32,
        piece_length: u32,
        total_length: u64,
    },
    /// Sent about once per second for every torrent of this connection.
    Status {
        torrent_id: TorrentId,
        state: TorrentState,
        peers: u32,
        /// Bytes per second from the swarm.
        down_speed: u64,
        /// Bytes per second to the swarm.
        up_speed: u64,
        /// Fraction (0.0..=1.0) of pieces the client has acknowledged or had.
        progress: f32,
        pieces_done: u32,
        pieces_total: u32,
        /// Bytes currently held in the server RAM buffer (all torrents).
        buffer_used: u64,
        buffer_limit: u64,
    },
    /// A complete piece whose SHA-1 the server already verified.
    Piece {
        torrent_id: TorrentId,
        index: u32,
        data: Bytes,
    },
    /// Reply to `Remove`.
    Removed { torrent_id: TorrentId },
    /// `torrent_id` is `None` for connection-level errors (after which the
    /// server closes the connection).
    Error {
        torrent_id: Option<TorrentId>,
        code: ErrorCode,
        msg: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TorrentState {
    Initializing,
    Downloading,
    Paused,
    /// Every piece has been acknowledged by the client.
    Finished,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCode {
    Unauthorized,
    VersionMismatch,
    /// Message out of order (e.g. anything before `Hello`).
    Protocol,
    BadTorrent,
    UnknownTorrent,
    Internal,
}
