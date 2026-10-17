//! librqbit session plus the registry of torrents managed for clients.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::Bytes;
use librqbit::limits::LimitsConfig;
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ListenerMode, ListenerOptions,
    ManagedTorrent, Session, SessionOptions, TorrentStatsState,
};
use parking_lot::Mutex;
use st_protocol::{Bitfield, ErrorCode, ServerMessage, TorrentId, TorrentState};
use tokio_util::sync::CancellationToken;

use crate::buffer::{BufferPool, Geometry, TorrentBuffer};
use crate::storage::RamStorageFactory;
use crate::Config;

/// Swarm download rate while under backpressure. One BitTorrent block per
/// second; the rate limiter cannot go below one block.
const THROTTLED_BPS: u32 = 16 * 1024;

pub type ConnId = u64;

/// Error reported to the client.
#[derive(Debug)]
pub struct ClientError {
    pub code: ErrorCode,
    pub msg: String,
}

impl ClientError {
    fn new(code: ErrorCode, msg: impl Into<String>) -> Self {
        Self {
            code,
            msg: msg.into(),
        }
    }
}

struct FileSpan {
    pieces: std::ops::Range<u32>,
}

pub struct TorrentEntry {
    pub id: TorrentId,
    pub info_hash: [u8; 20],
    pub name: String,
    pub geom: Geometry,
    files: Vec<FileSpan>,
    torrent_bytes: Bytes,
    state: Mutex<EntryState>,
}

struct EntryState {
    buffer: Arc<TorrentBuffer>,
    handle: Option<Arc<ManagedTorrent>>,
    user_paused: bool,
    owner: Option<ConnId>,
}

impl TorrentEntry {
    pub fn buffer(&self) -> Arc<TorrentBuffer> {
        self.state.lock().buffer.clone()
    }

    pub fn owner(&self) -> Option<ConnId> {
        self.state.lock().owner
    }

    pub fn added_message(&self) -> ServerMessage {
        ServerMessage::TorrentAdded {
            torrent_id: self.id,
            info_hash: self.info_hash,
            name: self.name.clone(),
            piece_count: self.geom.piece_count(),
            piece_length: self.geom.piece_length,
            total_length: self.geom.total_length,
        }
    }

    /// Files that still contain at least one piece the client lacks, or
    /// `None` if that is every file.
    fn only_files(&self, have: &Bitfield) -> Option<Vec<usize>> {
        let needed: Vec<usize> = self
            .files
            .iter()
            .enumerate()
            .filter(|(_, f)| f.pieces.clone().any(|p| !have.get(p)))
            .map(|(i, _)| i)
            .collect();
        (needed.len() != self.files.len()).then_some(needed)
    }
}

pub struct Engine {
    session: Arc<Session>,
    pool: Arc<BufferPool>,
    torrents: Mutex<Registry>,
    initial_peers: Vec<SocketAddr>,
    peer_limit: usize,
    disable_trackers: bool,
    base_download: Option<NonZeroU32>,
    next_conn: AtomicU64,
}

#[derive(Default)]
struct Registry {
    by_id: HashMap<TorrentId, Arc<TorrentEntry>>,
    by_hash: HashMap<[u8; 20], TorrentId>,
    next_id: TorrentId,
}

fn nz(v: u64) -> Option<NonZeroU32> {
    NonZeroU32::new(v.min(u64::from(u32::MAX)) as u32)
}

impl Engine {
    pub async fn new(config: &Config, cancel: CancellationToken) -> anyhow::Result<Arc<Self>> {
        let base_download = nz(config.download_limit);
        let opts = SessionOptions {
            dht: config.dht.then(|| librqbit::DhtSessionConfig {
                persistence: None,
                ..Default::default()
            }),
            disable_trackers: !config.trackers,
            fastresume: false,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: SocketAddr::from(([0, 0, 0, 0], config.bt_port)),
                enable_upnp_port_forwarding: config.upnp,
                ipv4_only: true,
                ..Default::default()
            }),
            cancellation_token: Some(cancel),
            ratelimits: LimitsConfig {
                download_bps: base_download,
                upload_bps: nz(config.upload_limit),
            },
            peer_limit: Some(config.peer_limit),
            disable_upload: !config.seed,
            disable_local_service_discovery: !config.lsd,
            ipv4_only: true,
            ..Default::default()
        };
        // librqbit wants a default output folder; our storage never uses it.
        let session = Session::new_with_opts(std::env::temp_dir(), opts)
            .await
            .context("starting BitTorrent session")?;
        let engine = Arc::new(Self {
            session,
            pool: BufferPool::new(config.buffer_limit, config.max_inflight_pieces),
            torrents: Mutex::new(Registry::default()),
            initial_peers: config.initial_peers.clone(),
            peer_limit: config.peer_limit,
            disable_trackers: !config.trackers,
            base_download,
            next_conn: AtomicU64::new(1),
        });
        tokio::spawn(engine.clone().backpressure_loop());
        Ok(engine)
    }

    pub fn pool(&self) -> &Arc<BufferPool> {
        &self.pool
    }

    pub fn bt_listen_addr(&self) -> Option<SocketAddr> {
        self.session.listen_addr()
    }

    pub fn new_conn_id(&self) -> ConnId {
        self.next_conn.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get(&self, id: TorrentId) -> Option<Arc<TorrentEntry>> {
        self.torrents.lock().by_id.get(&id).cloned()
    }

    /// Throttles the whole session while the buffer is under pressure.
    async fn backpressure_loop(self: Arc<Self>) {
        let mut on = false;
        let cancel = self.session.cancellation_token().clone();
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = self.pool.changed() => {}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            let p = self.pool.pressure(on);
            if p != on {
                on = p;
                let stats = self.pool.stats();
                if on {
                    tracing::info!(?stats, "buffer under pressure, throttling downloads");
                    self.session
                        .ratelimits
                        .set_download_bps(NonZeroU32::new(THROTTLED_BPS));
                } else {
                    tracing::info!(?stats, "buffer pressure relieved, resuming downloads");
                    self.session.ratelimits.set_download_bps(self.base_download);
                }
            }
        }
    }

    /// Handles `AddTorrent`: creates the torrent or attaches to an existing
    /// one (same info-hash), and makes `conn` its owner.
    pub async fn add(
        &self,
        conn: ConnId,
        torrent_bytes: Bytes,
        have: Bitfield,
    ) -> Result<Arc<TorrentEntry>, ClientError> {
        let parsed = parse_torrent(&torrent_bytes)
            .map_err(|e| ClientError::new(ErrorCode::BadTorrent, format!("{e:#}")))?;
        let piece_count = parsed.geom.piece_count();
        let have = if have.is_empty() {
            Bitfield::new(piece_count)
        } else if have.len() == piece_count {
            have
        } else {
            return Err(ClientError::new(
                ErrorCode::BadTorrent,
                format!(
                    "bitfield has {} bits, torrent has {piece_count} pieces",
                    have.len()
                ),
            ));
        };
        if u64::from(parsed.geom.piece_length) * 4 > self.pool.limit() {
            return Err(ClientError::new(
                ErrorCode::BadTorrent,
                format!(
                    "piece size {} is too large for buffer_limit {}",
                    parsed.geom.piece_length,
                    self.pool.limit()
                ),
            ));
        }

        let existing = {
            let g = self.torrents.lock();
            g.by_hash
                .get(&parsed.info_hash)
                .and_then(|id| g.by_id.get(id).cloned())
        };
        if let Some(entry) = existing {
            let buffer = entry.buffer();
            let lost = buffer.lost_pieces(&have);
            if lost.is_empty() {
                buffer.merge_have(&have);
            } else {
                tracing::warn!(
                    torrent = entry.id,
                    lost = lost.len(),
                    "client lost pieces that were already freed, restarting torrent"
                );
                self.start(&entry, have).await?;
            }
            let mut st = entry.state.lock();
            st.owner = Some(conn);
            st.buffer.reset_unsent();
            drop(st);
            return Ok(entry);
        }

        let entry = {
            let mut g = self.torrents.lock();
            let id = g.next_id;
            g.next_id += 1;
            let entry = Arc::new(TorrentEntry {
                id,
                info_hash: parsed.info_hash,
                name: parsed.name,
                files: parsed.files,
                torrent_bytes,
                state: Mutex::new(EntryState {
                    buffer: TorrentBuffer::new(
                        self.pool.clone(),
                        parsed.geom.clone(),
                        Bitfield::new(piece_count),
                    ),
                    handle: None,
                    user_paused: false,
                    owner: Some(conn),
                }),
                geom: parsed.geom,
            });
            g.by_hash.insert(entry.info_hash, id);
            g.by_id.insert(id, entry.clone());
            entry
        };
        if let Err(e) = self.start(&entry, have).await {
            let mut g = self.torrents.lock();
            g.by_id.remove(&entry.id);
            g.by_hash.remove(&entry.info_hash);
            return Err(e);
        }
        Ok(entry)
    }

    /// (Re)starts the librqbit torrent with a fresh buffer.
    async fn start(&self, entry: &TorrentEntry, have: Bitfield) -> Result<(), ClientError> {
        self.stop_librqbit(entry).await;
        let buffer = TorrentBuffer::new(self.pool.clone(), entry.geom.clone(), have.clone());
        let old = std::mem::replace(&mut entry.state.lock().buffer, buffer.clone());
        old.close();
        if have.is_full() {
            return Ok(());
        }
        let only_files = entry.only_files(&have);
        let paused = entry.state.lock().user_paused;
        let opts = AddTorrentOptions {
            paused,
            only_files,
            overwrite: true,
            disable_trackers: self.disable_trackers,
            initial_peers: (!self.initial_peers.is_empty()).then(|| self.initial_peers.clone()),
            peer_limit: Some(self.peer_limit),
            storage_factory: Some(librqbit::storage::StorageFactoryExt::boxed(
                RamStorageFactory::new(buffer),
            )),
            ..Default::default()
        };
        let resp = self
            .session
            .add_torrent(
                AddTorrent::from_bytes(entry.torrent_bytes.clone()),
                Some(opts),
            )
            .await
            .map_err(|e| ClientError::new(ErrorCode::Internal, format!("{e:#}")))?;
        let handle = match resp {
            AddTorrentResponse::Added(_, h) => h,
            AddTorrentResponse::AlreadyManaged(_, h) => h,
            AddTorrentResponse::ListOnly(_) => {
                return Err(ClientError::new(
                    ErrorCode::Internal,
                    "unexpected list-only",
                ))
            }
        };
        entry.state.lock().handle = Some(handle);
        Ok(())
    }

    async fn stop_librqbit(&self, entry: &TorrentEntry) {
        let handle = entry.state.lock().handle.take();
        if let Some(h) = handle {
            if let Err(e) = self.session.delete(h.id().into(), false).await {
                tracing::warn!(torrent = entry.id, "error removing torrent: {e:#}");
            }
        }
    }

    pub async fn set_paused(&self, id: TorrentId, paused: bool) -> Result<(), ClientError> {
        let entry = self
            .get(id)
            .ok_or_else(|| ClientError::new(ErrorCode::UnknownTorrent, "unknown torrent"))?;
        let handle = {
            let mut st = entry.state.lock();
            st.user_paused = paused;
            st.handle.clone()
        };
        let Some(h) = handle else { return Ok(()) };
        let r = if paused {
            if h.is_paused() {
                return Ok(());
            }
            self.session.pause(&h).await
        } else {
            if !h.is_paused() {
                return Ok(());
            }
            self.session.unpause(&h).await
        };
        r.map_err(|e| ClientError::new(ErrorCode::Internal, format!("{e:#}")))
    }

    pub async fn remove(&self, id: TorrentId) -> Result<(), ClientError> {
        let entry = {
            let mut g = self.torrents.lock();
            let e = g
                .by_id
                .remove(&id)
                .ok_or_else(|| ClientError::new(ErrorCode::UnknownTorrent, "unknown torrent"))?;
            g.by_hash.remove(&e.info_hash);
            e
        };
        self.stop_librqbit(&entry).await;
        entry.buffer().close();
        Ok(())
    }

    /// Handles `Ack`. Once every piece is with the client the librqbit
    /// torrent is dropped: there is nothing left to seed from RAM.
    pub async fn ack(&self, id: TorrentId, index: u32) -> Result<(), ClientError> {
        let entry = self
            .get(id)
            .ok_or_else(|| ClientError::new(ErrorCode::UnknownTorrent, "unknown torrent"))?;
        let buffer = entry.buffer();
        buffer.ack(index);
        if buffer.is_finished() {
            self.stop_librqbit(&entry).await;
        }
        Ok(())
    }

    pub fn status(&self, entry: &TorrentEntry) -> ServerMessage {
        let (buffer, handle, user_paused) = {
            let st = entry.state.lock();
            (st.buffer.clone(), st.handle.clone(), st.user_paused)
        };
        let pieces_total = entry.geom.piece_count();
        let pieces_done = buffer.done_count();
        let mut peers = 0;
        let mut down_speed = 0;
        let mut up_speed = 0;
        let state = if buffer.is_finished() {
            TorrentState::Finished
        } else if let Some(h) = &handle {
            let s = h.stats();
            if let Some(live) = &s.live {
                peers = live.snapshot.peer_stats.live;
                down_speed = (live.download_speed.mbps * 1024.0 * 1024.0) as u64;
                up_speed = (live.upload_speed.mbps * 1024.0 * 1024.0) as u64;
            }
            match s.state {
                TorrentStatsState::Initializing { .. } => TorrentState::Initializing,
                TorrentStatsState::Live => TorrentState::Downloading,
                TorrentStatsState::Paused => TorrentState::Paused,
                TorrentStatsState::Error => TorrentState::Error,
            }
        } else if user_paused {
            TorrentState::Paused
        } else {
            TorrentState::Error
        };
        let stats = self.pool.stats();
        ServerMessage::Status {
            torrent_id: entry.id,
            state,
            peers,
            down_speed,
            up_speed,
            progress: if pieces_total == 0 {
                1.0
            } else {
                pieces_done as f32 / pieces_total as f32
            },
            pieces_done,
            pieces_total,
            buffer_used: stats.used,
            buffer_limit: stats.limit,
        }
    }

    /// Error text of a torrent in librqbit's error state.
    pub fn torrent_error(&self, entry: &TorrentEntry) -> Option<String> {
        let h = entry.state.lock().handle.clone()?;
        h.stats().error
    }

    pub async fn shutdown(&self) {
        let entries: Vec<_> = self.torrents.lock().by_id.values().cloned().collect();
        for e in entries {
            self.stop_librqbit(&e).await;
            e.buffer().close();
        }
        self.session.stop().await;
    }
}

struct ParsedTorrent {
    info_hash: [u8; 20],
    name: String,
    geom: Geometry,
    files: Vec<FileSpan>,
}

fn parse_torrent(bytes: &[u8]) -> anyhow::Result<ParsedTorrent> {
    let t = librqbit::torrent_from_bytes(bytes).context("invalid .torrent file")?;
    let info_hash = t.info_hash.0;
    let info = t.info.data.validate().context("invalid torrent info")?;
    let lengths = *info.lengths();
    let name = info.name().map(|n| n.into_owned()).unwrap_or_default();
    let mut file_offsets = Vec::new();
    let mut files = Vec::new();
    for f in info.iter_file_details_ext() {
        file_offsets.push(f.offset);
        files.push(FileSpan { pieces: f.pieces });
    }
    Ok(ParsedTorrent {
        info_hash,
        name,
        geom: Geometry {
            piece_length: lengths.default_piece_length(),
            total_length: lengths.total_length(),
            file_offsets,
        },
        files,
    })
}
