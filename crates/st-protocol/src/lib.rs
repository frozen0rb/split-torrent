//! Wire protocol between `st-server` and `st-client`.
//!
//! Every frame on the wire is a big-endian `u32` length followed by a
//! [`postcard`]-encoded message. The client sends [`ClientMessage`]s and the
//! server sends [`ServerMessage`]s. Use [`client_framed`] / [`server_framed`]
//! to wrap any `AsyncRead + AsyncWrite` stream (e.g. a TLS stream).
//!
//! Typical session:
//!
//! 1. client → `Hello { token, version }`, server → `HelloOk` (or `Error` and close)
//! 2. client → `AddTorrent { torrent_bytes, have }`, server → `TorrentAdded`
//! 3. server → `Piece { .. }` (SHA-1 already verified), client → `Ack { .. }`
//!    after the piece is durably written; server → `Status` about once a second
//! 4. client → `Pause` / `Resume` / `Remove` at any time

mod bitfield;
mod codec;
mod fingerprint;
mod messages;

pub use bitfield::Bitfield;
pub use codec::{
    client_framed, server_framed, ClientCodec, MessageCodec, ProtocolError, ServerCodec,
    MAX_FRAME_LEN,
};
pub use fingerprint::{cert_fingerprint, format_fingerprint, parse_fingerprint};
pub use messages::{
    ClientMessage, ErrorCode, ServerMessage, TorrentId, TorrentState, PROTOCOL_VERSION,
};

/// Default TCP port of `st-server`.
pub const DEFAULT_PORT: u16 = 7443;
