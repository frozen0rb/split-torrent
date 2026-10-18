use std::marker::PhantomData;

use bytes::{BufMut, BytesMut};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::codec::{Decoder, Encoder, Framed, LengthDelimitedCodec};

use crate::{ClientMessage, ServerMessage};

/// Largest accepted frame. Fits a 32 MiB piece plus headers, and bounds
/// memory a peer can make us allocate for one frame.
pub const MAX_FRAME_LEN: usize = 33 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("i/o: {0}")]
    Io(#[from] std::io::Error),
    #[error("encode: {0}")]
    Encode(postcard::Error),
    #[error("decode: {0}")]
    Decode(postcard::Error),
    #[error("frame of {0} bytes exceeds MAX_FRAME_LEN")]
    FrameTooLarge(usize),
}

/// Length-delimited postcard codec that encodes `Out` and decodes `In`.
pub struct MessageCodec<Out, In> {
    inner: LengthDelimitedCodec,
    _p: PhantomData<fn(Out) -> In>,
}

/// Used by the client: sends [`ClientMessage`], receives [`ServerMessage`].
pub type ClientCodec = MessageCodec<ClientMessage, ServerMessage>;
/// Used by the server: sends [`ServerMessage`], receives [`ClientMessage`].
pub type ServerCodec = MessageCodec<ServerMessage, ClientMessage>;

impl<Out, In> MessageCodec<Out, In> {
    pub fn new() -> Self {
        Self {
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(MAX_FRAME_LEN)
                .new_codec(),
            _p: PhantomData,
        }
    }
}

impl<Out, In> Default for MessageCodec<Out, In> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Out: Serialize, In> Encoder<Out> for MessageCodec<Out, In> {
    type Error = ProtocolError;

    fn encode(&mut self, item: Out, dst: &mut BytesMut) -> Result<(), ProtocolError> {
        let start = dst.len();
        dst.put_u32(0);
        let mut w = dst.split_off(dst.len()).writer();
        postcard::to_io(&item, &mut w).map_err(ProtocolError::Encode)?;
        let body = w.into_inner();
        let len = body.len();
        if len > MAX_FRAME_LEN {
            dst.truncate(start);
            return Err(ProtocolError::FrameTooLarge(len));
        }
        dst[start..start + 4].copy_from_slice(&(len as u32).to_be_bytes());
        dst.unsplit(body);
        Ok(())
    }
}

impl<Out, In: DeserializeOwned> Decoder for MessageCodec<Out, In> {
    type Item = In;
    type Error = ProtocolError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<In>, ProtocolError> {
        match self.inner.decode(src)? {
            Some(frame) => postcard::from_bytes(&frame)
                .map(Some)
                .map_err(ProtocolError::Decode),
            None => Ok(None),
        }
    }
}

pub fn client_framed<T>(io: T) -> Framed<T, ClientCodec>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    Framed::new(io, ClientCodec::new())
}

pub fn server_framed<T>(io: T) -> Framed<T, ServerCodec>
where
    T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
{
    Framed::new(io, ServerCodec::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Bitfield, ErrorCode, TorrentState, PROTOCOL_VERSION};
    use bytes::Bytes;
    use futures::{SinkExt, StreamExt};

    fn client_msgs() -> Vec<ClientMessage> {
        let mut have = Bitfield::new(13);
        have.set(3, true);
        vec![
            ClientMessage::Hello {
                token: "secret".into(),
                version: PROTOCOL_VERSION,
            },
            ClientMessage::AddTorrent {
                torrent_bytes: Bytes::from_static(b"d4:infod...ee"),
                have: have.clone(),
                files: vec![0, 2],
            },
            ClientMessage::SetFiles {
                torrent_id: 7,
                files: vec![1],
                have,
            },
            ClientMessage::Ack {
                torrent_id: 7,
                index: 42,
            },
            ClientMessage::Pause { torrent_id: 7 },
            ClientMessage::Resume { torrent_id: 7 },
            ClientMessage::Remove { torrent_id: 7 },
        ]
    }

    fn server_msgs() -> Vec<ServerMessage> {
        vec![
            ServerMessage::HelloOk {
                version: PROTOCOL_VERSION,
                buffer_limit: 384 << 20,
            },
            ServerMessage::TorrentAdded {
                torrent_id: 1,
                info_hash: [0xab; 20],
                name: "ubuntu.iso".into(),
                piece_count: 100,
                piece_length: 1 << 18,
                total_length: 100 << 18,
            },
            ServerMessage::Status {
                torrent_id: 1,
                state: TorrentState::Downloading,
                peers: 12,
                down_speed: 1_000_000,
                up_speed: 0,
                progress: 0.5,
                pieces_done: 50,
                pieces_total: 100,
                buffer_used: 1 << 20,
                buffer_limit: 384 << 20,
            },
            ServerMessage::Piece {
                torrent_id: 1,
                index: 9,
                data: Bytes::from(vec![0x5a; 1 << 18]),
            },
            ServerMessage::Removed { torrent_id: 1 },
            ServerMessage::Error {
                torrent_id: None,
                code: ErrorCode::Unauthorized,
                msg: "bad token".into(),
            },
        ]
    }

    #[test]
    fn roundtrip_all_messages_in_one_buffer() {
        let mut buf = BytesMut::new();
        let mut c = ClientCodec::new();
        for m in client_msgs() {
            c.encode(m, &mut buf).unwrap();
        }
        let mut s = ServerCodec::new();
        let mut got = Vec::new();
        while let Some(m) = s.decode(&mut buf).unwrap() {
            got.push(m);
        }
        assert_eq!(got, client_msgs());

        let mut buf = BytesMut::new();
        for m in server_msgs() {
            s.encode(m, &mut buf).unwrap();
        }
        let mut got = Vec::new();
        while let Some(m) = c.decode(&mut buf).unwrap() {
            got.push(m);
        }
        assert_eq!(got, server_msgs());
    }

    #[test]
    fn partial_frames_wait_for_more_data() {
        let mut full = BytesMut::new();
        ServerCodec::new()
            .encode(server_msgs().remove(3), &mut full)
            .unwrap();
        let mut c = ClientCodec::new();
        let mut buf = BytesMut::new();
        let split = full.len() / 2;
        buf.extend_from_slice(&full[..split]);
        assert!(c.decode(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&full[split..]);
        assert_eq!(c.decode(&mut buf).unwrap(), Some(server_msgs().remove(3)));
    }

    #[test]
    fn piece_overhead_is_small() {
        let mut buf = BytesMut::new();
        ServerCodec::new()
            .encode(server_msgs().remove(3), &mut buf)
            .unwrap();
        assert!(buf.len() < (1 << 18) + 16, "len {}", buf.len());
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut buf = BytesMut::new();
        buf.put_u32((MAX_FRAME_LEN + 1) as u32);
        buf.extend_from_slice(&[0; 16]);
        assert!(matches!(
            ClientCodec::new().decode(&mut buf),
            Err(ProtocolError::Io(_))
        ));

        let msg = ServerMessage::Piece {
            torrent_id: 1,
            index: 0,
            data: Bytes::from(vec![0; MAX_FRAME_LEN + 1]),
        };
        let mut buf = BytesMut::new();
        assert!(matches!(
            ServerCodec::new().encode(msg, &mut buf),
            Err(ProtocolError::FrameTooLarge(_))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn garbage_is_decode_error() {
        let mut buf = BytesMut::new();
        buf.put_u32(2);
        buf.extend_from_slice(&[0xff, 0xff]);
        assert!(matches!(
            ServerCodec::new().decode(&mut buf),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[tokio::test]
    async fn framed_over_duplex() {
        let (a, b) = tokio::io::duplex(64 * 1024);
        let mut client = client_framed(a);
        let mut server = server_framed(b);
        let send = tokio::spawn(async move {
            for m in server_msgs() {
                server.send(m).await.unwrap();
            }
            let mut got = Vec::new();
            while let Some(m) = server.next().await {
                got.push(m.unwrap());
            }
            got
        });
        let mut got = Vec::new();
        for _ in 0..server_msgs().len() {
            got.push(client.next().await.unwrap().unwrap());
        }
        assert_eq!(got, server_msgs());
        for m in client_msgs() {
            client.send(m).await.unwrap();
        }
        drop(client);
        assert_eq!(send.await.unwrap(), client_msgs());
    }
}
