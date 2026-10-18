//! One client connection.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use st_protocol::{
    server_framed, ClientMessage, ErrorCode, ServerMessage, TorrentId, TorrentState,
    PROTOCOL_VERSION,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::engine::{ClientError, ConnId, Engine, TorrentEntry};
use crate::tls::token_eq;

const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
const STATUS_INTERVAL: Duration = Duration::from_secs(1);
/// Outgoing messages queued per connection. Pieces are `Bytes` clones of the
/// buffer, so this costs no extra memory, it only bounds the queue.
const OUT_QUEUE: usize = 16;

fn err(torrent_id: Option<TorrentId>, code: ErrorCode, msg: impl Into<String>) -> ServerMessage {
    ServerMessage::Error {
        torrent_id,
        code,
        msg: msg.into(),
    }
}

pub async fn handle<S>(engine: Arc<Engine>, token: Arc<str>, io: S, peer: SocketAddr)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sink, mut stream) = server_framed(io).split();

    let hello = tokio::time::timeout(HELLO_TIMEOUT, stream.next()).await;
    let reject = match hello {
        Ok(Some(Ok(ClientMessage::Hello { token: t, version }))) => {
            if version != PROTOCOL_VERSION {
                Some(err(
                    None,
                    ErrorCode::VersionMismatch,
                    format!("server speaks protocol {PROTOCOL_VERSION}, client {version}"),
                ))
            } else if !token_eq(&t, &token) {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Some(err(None, ErrorCode::Unauthorized, "bad token"))
            } else {
                None
            }
        }
        Ok(Some(Ok(_))) => Some(err(None, ErrorCode::Protocol, "expected Hello")),
        Ok(Some(Err(e))) => {
            tracing::debug!(%peer, "bad first frame: {e}");
            return;
        }
        Ok(None) => return,
        Err(_) => Some(err(None, ErrorCode::Protocol, "Hello timeout")),
    };
    if let Some(msg) = reject {
        tracing::warn!(%peer, ?msg, "rejecting client");
        let _ = sink.send(msg).await;
        let _ = sink.close().await;
        return;
    }
    let pool = engine.pool().clone();
    if sink
        .send(ServerMessage::HelloOk {
            version: PROTOCOL_VERSION,
            buffer_limit: pool.limit(),
        })
        .await
        .is_err()
    {
        return;
    }
    tracing::info!(%peer, "client authenticated");

    let (tx, mut rx) = mpsc::channel::<ServerMessage>(OUT_QUEUE);
    let writer = tokio::spawn(async move {
        while let Some(m) = rx.recv().await {
            if let Err(e) = sink.send(m).await {
                tracing::debug!("write failed: {e}");
                break;
            }
        }
        let _ = sink.close().await;
    });

    let mut conn = Conn {
        id: engine.new_conn_id(),
        engine,
        tx,
        owned: HashMap::new(),
    };
    let mut ticker = tokio::time::interval(STATUS_INTERVAL);
    loop {
        tokio::select! {
            m = stream.next() => match m {
                Some(Ok(m)) => {
                    if conn.on_message(m).await.is_err() {
                        break;
                    }
                }
                Some(Err(e)) => {
                    tracing::info!(%peer, "connection error: {e}");
                    break;
                }
                None => break,
            },
            _ = ticker.tick() => {
                if conn.send_status().await.is_err() {
                    break;
                }
            }
        }
    }
    tracing::info!(%peer, "client disconnected");
    for (_, t) in conn.owned.drain() {
        t.sender.abort();
    }
    drop(conn);
    let _ = writer.await;
}

struct Owned {
    entry: Arc<TorrentEntry>,
    sender: JoinHandle<()>,
    reported_error: bool,
}

struct Conn {
    id: ConnId,
    engine: Arc<Engine>,
    tx: mpsc::Sender<ServerMessage>,
    owned: HashMap<TorrentId, Owned>,
}

struct Closed;

impl Conn {
    async fn send(&self, m: ServerMessage) -> Result<(), Closed> {
        self.tx.send(m).await.map_err(|_| Closed)
    }

    async fn reply(&self, id: Option<TorrentId>, r: Result<(), ClientError>) -> Result<(), Closed> {
        match r {
            Ok(()) => Ok(()),
            Err(e) => self.send(err(id, e.code, e.msg)).await,
        }
    }

    async fn on_message(&mut self, m: ClientMessage) -> Result<(), Closed> {
        match m {
            ClientMessage::Hello { .. } => {
                self.send(err(None, ErrorCode::Protocol, "duplicate Hello"))
                    .await
            }
            ClientMessage::AddTorrent {
                torrent_bytes,
                have,
                files,
            } => match self.engine.add(self.id, torrent_bytes, have, files).await {
                Ok(entry) => {
                    self.send(entry.added_message()).await?;
                    self.own(entry);
                    Ok(())
                }
                Err(e) => self.send(err(None, e.code, e.msg)).await,
            },
            ClientMessage::Ack { torrent_id, index } => {
                let r = self.engine.ack(torrent_id, index).await;
                self.reply(Some(torrent_id), r).await
            }
            ClientMessage::SetFiles {
                torrent_id,
                files,
                have,
            } => {
                let r = self.engine.set_files(torrent_id, files, have).await;
                self.reply(Some(torrent_id), r).await
            }
            ClientMessage::Pause { torrent_id } => {
                let r = self.engine.set_paused(torrent_id, true).await;
                self.reply(Some(torrent_id), r).await
            }
            ClientMessage::Resume { torrent_id } => {
                let r = self.engine.set_paused(torrent_id, false).await;
                self.reply(Some(torrent_id), r).await
            }
            ClientMessage::Remove { torrent_id } => {
                if let Some(o) = self.owned.remove(&torrent_id) {
                    o.sender.abort();
                }
                match self.engine.remove(torrent_id).await {
                    Ok(()) => self.send(ServerMessage::Removed { torrent_id }).await,
                    Err(e) => self.send(err(Some(torrent_id), e.code, e.msg)).await,
                }
            }
        }
    }

    fn own(&mut self, entry: Arc<TorrentEntry>) {
        if let Some(old) = self.owned.remove(&entry.id) {
            old.sender.abort();
        }
        let sender = tokio::spawn(send_pieces(self.id, entry.clone(), self.tx.clone()));
        self.owned.insert(
            entry.id,
            Owned {
                entry,
                sender,
                reported_error: false,
            },
        );
    }

    async fn send_status(&mut self) -> Result<(), Closed> {
        let me = self.id;
        self.owned.retain(|_, o| {
            let keep = o.entry.owner() == Some(me);
            if !keep {
                o.sender.abort();
            }
            keep
        });
        let mut msgs = Vec::new();
        for o in self.owned.values_mut() {
            let status = self.engine.status(&o.entry);
            if let ServerMessage::Status {
                state: TorrentState::Error,
                ..
            } = status
            {
                if !o.reported_error {
                    o.reported_error = true;
                    let msg = self
                        .engine
                        .torrent_error(&o.entry)
                        .unwrap_or_else(|| "torrent failed".into());
                    msgs.push(err(Some(o.entry.id), ErrorCode::Internal, msg));
                }
            }
            msgs.push(status);
        }
        for m in msgs {
            self.send(m).await?;
        }
        Ok(())
    }
}

/// Streams complete pieces of one torrent while this connection owns it.
async fn send_pieces(conn: ConnId, entry: Arc<TorrentEntry>, tx: mpsc::Sender<ServerMessage>) {
    loop {
        if entry.owner() != Some(conn) {
            return;
        }
        let buffer = entry.buffer();
        if let Some((index, data)) = buffer.next_unsent() {
            let m = ServerMessage::Piece {
                torrent_id: entry.id,
                index,
                data,
            };
            if tx.send(m).await.is_err() {
                return;
            }
            continue;
        }
        tokio::select! {
            _ = buffer.wait_ready() => {}
            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
        }
    }
}
