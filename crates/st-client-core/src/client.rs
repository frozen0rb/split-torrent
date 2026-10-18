//! The client actor: one background task that owns the connection to
//! `st-server`, the torrents and their storage. UIs talk to it through
//! [`Client`] and receive [`Event`]s.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rustls::pki_types::ServerName;
use st_protocol::{
    client_framed, ClientMessage, ErrorCode, ProtocolError, ServerMessage, TorrentId, TorrentState,
    DEFAULT_PORT, PROTOCOL_VERSION,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsConnector;

use crate::metainfo::{InfoHash, Metainfo, MetainfoError};
use crate::storage::{Progress, Storage, StorageError};
use crate::tls::{self, BadFingerprint, Fingerprint};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
const HELLO_TIMEOUT: Duration = Duration::from_secs(15);
/// No message for this long (the server sends `Status` every second while a
/// torrent is active) is treated as a dead connection.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const FLUSH_INTERVAL: Duration = Duration::from_millis(200);
const FLUSH_BYTES: u64 = 16 << 20;
const UI_TICK: Duration = Duration::from_millis(500);

/// Reconnect delays.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    pub min: Duration,
    pub max: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            min: Duration::from_secs(1),
            max: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub host: String,
    pub port: u16,
    pub token: String,
    pub fingerprint: Fingerprint,
    pub backoff: Backoff,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("server address is empty")]
    EmptyAddress,
    #[error("invalid port in server address")]
    BadPort,
    #[error(transparent)]
    Fingerprint(#[from] BadFingerprint),
}

impl ClientConfig {
    /// `server` is `host`, `host:port`, `[v6]:port` or a bare IPv6 address;
    /// the port defaults to [`DEFAULT_PORT`].
    pub fn new(server: &str, token: &str, fingerprint: &str) -> Result<Self, ConfigError> {
        let (host, port) = parse_server(server)?;
        Ok(Self {
            host,
            port,
            token: token.to_owned(),
            fingerprint: Fingerprint::parse(fingerprint)?,
            backoff: Backoff::default(),
        })
    }
}

fn parse_server(s: &str) -> Result<(String, u16), ConfigError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ConfigError::EmptyAddress);
    }
    let port = |p: &str| {
        p.parse::<u16>()
            .ok()
            .filter(|&p| p != 0)
            .ok_or(ConfigError::BadPort)
    };
    if let Some(rest) = s.strip_prefix('[') {
        let (host, tail) = rest.split_once(']').ok_or(ConfigError::EmptyAddress)?;
        return match tail.strip_prefix(':') {
            Some(p) => Ok((host.into(), port(p)?)),
            None if tail.is_empty() => Ok((host.into(), DEFAULT_PORT)),
            None => Err(ConfigError::BadPort),
        };
    }
    match s.matches(':').count() {
        0 => Ok((s.into(), DEFAULT_PORT)),
        1 => {
            let (h, p) = s.split_once(':').expect("one colon");
            if h.is_empty() {
                return Err(ConfigError::EmptyAddress);
            }
            Ok((h.into(), port(p)?))
        }
        _ => Ok((s.into(), DEFAULT_PORT)),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionState {
    Connecting,
    Connected { buffer_limit: u64 },
    Disconnected { error: String, retry_in: Duration },
    Stopped,
}

/// The server's latest view of a torrent.
#[derive(Debug, Clone, PartialEq)]
pub struct ServerStatus {
    pub state: TorrentState,
    pub peers: u32,
    pub down_speed: u64,
    pub up_speed: u64,
    pub progress: f32,
    pub buffer_used: u64,
    pub buffer_limit: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TorrentSnapshot {
    pub info_hash: InfoHash,
    pub name: String,
    pub download_dir: PathBuf,
    /// Length of the whole torrent.
    pub total_length: u64,
    pub piece_count: u32,
    /// One entry per torrent file (see [`Metainfo::files`]).
    pub selected: Vec<bool>,
    /// Pieces overlapping a selected file.
    pub pieces_wanted: u32,
    /// Of those, pieces on disk.
    pub pieces_have: u32,
    /// Total length of the selected files.
    pub bytes_wanted: u64,
    /// Bytes of the selected files on disk.
    pub bytes_have: u64,
    /// Per file: bytes on disk; 0 for files that are not selected.
    pub file_bytes_have: Vec<u64>,
    /// Bytes per second received from the server.
    pub recv_speed: u64,
    pub paused: bool,
    /// Registered with the server on the current connection.
    pub online: bool,
    pub server: Option<ServerStatus>,
    pub error: Option<String>,
}

impl TorrentSnapshot {
    /// Every selected file is on disk.
    pub fn complete(&self) -> bool {
        self.pieces_have == self.pieces_wanted
    }

    /// Fraction of the selected bytes on disk.
    pub fn progress(&self) -> f32 {
        if self.bytes_wanted == 0 {
            1.0
        } else {
            (self.bytes_have as f64 / self.bytes_wanted as f64) as f32
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Connection(ConnectionState),
    Torrent(TorrentSnapshot),
    Removed(InfoHash),
    /// Human-readable warning (write failure, server error for a torrent…).
    Warning(String),
}

#[derive(Debug, thiserror::Error)]
pub enum AddError {
    #[error(transparent)]
    Metainfo(#[from] MetainfoError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("this torrent is already in the list")]
    Duplicate,
    #[error("no files selected")]
    NothingSelected,
    #[error("client is stopped")]
    Stopped,
}

enum Command {
    Add(Box<Torrent>),
    Pause(InfoHash),
    Resume(InfoHash),
    SetFiles(InfoHash, Vec<bool>),
    Remove {
        info_hash: InfoHash,
        delete_files: bool,
    },
    Shutdown,
}

pub type Notify = Arc<dyn Fn() + Send + Sync>;

/// Handle to the client task. Dropping it stops the task.
pub struct Client {
    cmd: mpsc::UnboundedSender<Command>,
    known: Arc<Mutex<HashSet<InfoHash>>>,
}

impl Client {
    /// Spawns the client task on `rt`. `notify` is called after every event,
    /// e.g. to wake up a GUI.
    pub fn start(
        config: ClientConfig,
        rt: &tokio::runtime::Handle,
        notify: Option<Notify>,
    ) -> (Self, mpsc::UnboundedReceiver<Event>) {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let known = Arc::new(Mutex::new(HashSet::new()));
        let actor = Actor {
            config,
            events: ev_tx,
            notify,
            torrents: HashMap::new(),
            known: known.clone(),
            outbox: Vec::new(),
            online: false,
        };
        rt.spawn(actor.run(cmd_rx));
        (Self { cmd: cmd_tx, known }, ev_rx)
    }

    /// Parses the torrent, creates/pre-allocates the selected files under
    /// `download_dir` and loads resume state. `selected` has one entry per
    /// file of the torrent; `None` keeps the selection saved with the resume
    /// state, or selects every file. Blocks on file I/O.
    pub fn add_torrent(
        &self,
        torrent_bytes: Vec<u8>,
        download_dir: &Path,
        paused: bool,
        selected: Option<&[bool]>,
    ) -> Result<TorrentSnapshot, AddError> {
        let meta = Arc::new(Metainfo::parse(&torrent_bytes)?);
        if selected.is_some_and(|s| !s.contains(&true)) {
            return Err(AddError::NothingSelected);
        }
        if !self.known.lock().unwrap().insert(meta.info_hash) {
            return Err(AddError::Duplicate);
        }
        let storage = match Storage::open(meta.clone(), download_dir, selected) {
            Ok(s) => s,
            Err(e) => {
                self.known.lock().unwrap().remove(&meta.info_hash);
                return Err(e.into());
            }
        };
        let t = Torrent::new(storage, torrent_bytes.into(), paused);
        let snap = t.snapshot();
        self.cmd
            .send(Command::Add(Box::new(t)))
            .map_err(|_| AddError::Stopped)?;
        Ok(snap)
    }

    pub fn pause(&self, info_hash: InfoHash) {
        let _ = self.cmd.send(Command::Pause(info_hash));
    }

    pub fn resume(&self, info_hash: InfoHash) {
        let _ = self.cmd.send(Command::Resume(info_hash));
    }

    /// Changes which files of a torrent are downloaded, while it runs or is
    /// paused. Files that are deselected keep what is already on disk.
    pub fn set_files(&self, info_hash: InfoHash, selected: Vec<bool>) {
        let _ = self.cmd.send(Command::SetFiles(info_hash, selected));
    }

    pub fn remove(&self, info_hash: InfoHash, delete_files: bool) {
        let _ = self.cmd.send(Command::Remove {
            info_hash,
            delete_files,
        });
    }

    /// Flushes all pending writes and stops; a final
    /// `Event::Connection(Stopped)` is emitted.
    pub fn shutdown(&self) {
        let _ = self.cmd.send(Command::Shutdown);
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct Torrent {
    meta: Arc<Metainfo>,
    storage: Arc<Mutex<Storage>>,
    download_dir: PathBuf,
    torrent_bytes: Bytes,
    paused: bool,
    server_id: Option<TorrentId>,
    add_sent: bool,
    server: Option<ServerStatus>,
    error: Option<String>,
    selected: Vec<bool>,
    progress: Progress,
    /// The selection changed after `AddTorrent` was sent but before the
    /// server answered; `SetFiles` follows `TorrentAdded`.
    files_changed: bool,
    recv_bytes: u64,
    recv_speed: u64,
    pending_acks: Vec<u32>,
    changed: bool,
}

impl Torrent {
    fn new(storage: Storage, torrent_bytes: Bytes, paused: bool) -> Self {
        Self {
            meta: storage.meta().clone(),
            download_dir: storage.download_dir().to_owned(),
            selected: storage.selected().to_vec(),
            progress: storage.progress().clone(),
            files_changed: false,
            storage: Arc::new(Mutex::new(storage)),
            torrent_bytes,
            paused,
            server_id: None,
            add_sent: false,
            server: None,
            error: None,
            recv_bytes: 0,
            recv_speed: 0,
            pending_acks: Vec::new(),
            changed: true,
        }
    }

    fn complete(&self) -> bool {
        self.progress.pieces_have == self.progress.pieces_wanted
    }

    fn snapshot(&self) -> TorrentSnapshot {
        TorrentSnapshot {
            info_hash: self.meta.info_hash,
            name: self.meta.name.clone(),
            download_dir: self.download_dir.clone(),
            total_length: self.meta.total_length,
            piece_count: self.meta.piece_count(),
            selected: self.selected.clone(),
            pieces_wanted: self.progress.pieces_wanted,
            pieces_have: self.progress.pieces_have,
            bytes_wanted: self.progress.bytes_wanted,
            bytes_have: self.progress.bytes_have,
            file_bytes_have: self.progress.file_bytes_have.clone(),
            recv_speed: self.recv_speed,
            paused: self.paused,
            online: self.server_id.is_some(),
            server: self.server.clone(),
            error: self.error.clone(),
        }
    }

    fn go_offline(&mut self) {
        self.server_id = None;
        self.add_sent = false;
        self.files_changed = false;
        self.server = None;
        self.recv_speed = 0;
        self.pending_acks.clear();
        self.changed = true;
    }
}

#[derive(Debug, thiserror::Error)]
enum SessionError {
    #[error("connect to {0}: {1}")]
    Connect(String, std::io::Error),
    #[error("timed out")]
    Timeout,
    #[error("TLS: {0}")]
    Tls(std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("server closed the connection")]
    Closed,
    #[error("unexpected message before HelloOk")]
    Unexpected,
    #[error("server error ({code:?}): {msg}")]
    Server { code: ErrorCode, msg: String },
    #[error("{0}; reconnecting to re-request it")]
    BadPiece(StorageError),
}

impl SessionError {
    /// Errors that will not fix themselves; retried at the slowest rate.
    fn is_config_problem(&self) -> bool {
        match self {
            SessionError::Server { code, .. } => {
                matches!(code, ErrorCode::Unauthorized | ErrorCode::VersionMismatch)
            }
            SessionError::Tls(e) => e.to_string().contains("fingerprint"),
            _ => false,
        }
    }
}

/// Outcome of a session that did not fail.
struct Shutdown;

struct Actor {
    config: ClientConfig,
    events: mpsc::UnboundedSender<Event>,
    notify: Option<Notify>,
    torrents: HashMap<InfoHash, Torrent>,
    known: Arc<Mutex<HashSet<InfoHash>>>,
    /// Messages for the server, drained by the session after each step and
    /// discarded while offline (state is re-sent on reconnect).
    outbox: Vec<ClientMessage>,
    /// `HelloOk` received on the current connection.
    online: bool,
}

impl Actor {
    fn emit(&self, ev: Event) {
        let _ = self.events.send(ev);
        if let Some(n) = &self.notify {
            n();
        }
    }

    async fn run(mut self, mut cmds: mpsc::UnboundedReceiver<Command>) {
        let mut backoff = self.config.backoff.min;
        loop {
            self.emit(Event::Connection(ConnectionState::Connecting));
            let mut hello_ok = false;
            let result = self.connect_and_run(&mut cmds, &mut hello_ok).await;
            self.online = false;
            self.flush_all().await;
            for t in self.torrents.values_mut() {
                t.go_offline();
            }
            self.outbox.clear();
            self.emit_changed();
            let err = match result {
                Ok(Shutdown) => break,
                Err(e) => e,
            };
            if hello_ok {
                backoff = self.config.backoff.min;
            }
            let delay = if err.is_config_problem() {
                self.config.backoff.max
            } else {
                backoff
            };
            backoff = (backoff * 2).min(self.config.backoff.max);
            self.emit(Event::Connection(ConnectionState::Disconnected {
                error: err.to_string(),
                retry_in: delay,
            }));
            let sleep = tokio::time::sleep(delay);
            tokio::pin!(sleep);
            loop {
                tokio::select! {
                    _ = &mut sleep => break,
                    cmd = cmds.recv() => {
                        if !self.handle_command(cmd).await {
                            self.outbox.clear();
                            self.emit(Event::Connection(ConnectionState::Stopped));
                            return;
                        }
                        self.outbox.clear();
                        self.emit_changed();
                    }
                }
            }
        }
        self.emit(Event::Connection(ConnectionState::Stopped));
    }

    async fn connect_and_run(
        &mut self,
        cmds: &mut mpsc::UnboundedReceiver<Command>,
        hello_ok: &mut bool,
    ) -> Result<Shutdown, SessionError> {
        let addr = format!("{}:{}", self.config.host, self.config.port);
        let tcp = tokio::time::timeout(
            CONNECT_TIMEOUT,
            TcpStream::connect((self.config.host.as_str(), self.config.port)),
        )
        .await
        .map_err(|_| SessionError::Timeout)?
        .map_err(|e| SessionError::Connect(addr, e))?;
        let _ = tcp.set_nodelay(true);
        let name = ServerName::try_from(tls::SERVER_NAME).expect("valid name");
        let connector = TlsConnector::from(tls::client_config(self.config.fingerprint));
        let tls = tokio::time::timeout(HELLO_TIMEOUT, connector.connect(name, tcp))
            .await
            .map_err(|_| SessionError::Timeout)?
            .map_err(SessionError::Tls)?;
        self.session(tls, cmds, hello_ok).await
    }

    async fn session<S: AsyncRead + AsyncWrite + Unpin>(
        &mut self,
        io: S,
        cmds: &mut mpsc::UnboundedReceiver<Command>,
        hello_ok: &mut bool,
    ) -> Result<Shutdown, SessionError> {
        let mut conn = client_framed(io);
        conn.send(ClientMessage::Hello {
            token: self.config.token.clone(),
            version: PROTOCOL_VERSION,
        })
        .await?;
        let buffer_limit = match tokio::time::timeout(HELLO_TIMEOUT, conn.next())
            .await
            .map_err(|_| SessionError::Timeout)?
        {
            Some(Ok(ServerMessage::HelloOk { buffer_limit, .. })) => buffer_limit,
            Some(Ok(ServerMessage::Error { code, msg, .. })) => {
                return Err(SessionError::Server { code, msg })
            }
            Some(Ok(_)) => return Err(SessionError::Unexpected),
            Some(Err(e)) => return Err(e.into()),
            None => return Err(SessionError::Closed),
        };
        *hello_ok = true;
        self.online = true;
        self.emit(Event::Connection(ConnectionState::Connected {
            buffer_limit,
        }));
        self.queue_adds();

        let mut flush_tick = tokio::time::interval(FLUSH_INTERVAL);
        let mut ui_tick = tokio::time::interval(UI_TICK);
        let mut last_ui = Instant::now();
        let mut last_rx = Instant::now();
        let mut pending_bytes = 0u64;
        loop {
            for m in self.outbox.drain(..) {
                conn.feed(m).await?;
            }
            conn.flush().await?;
            tokio::select! {
                msg = conn.next() => {
                    last_rx = Instant::now();
                    match msg {
                        None => return Err(SessionError::Closed),
                        Some(Err(e)) => return Err(e.into()),
                        Some(Ok(m)) => pending_bytes += self.handle_server(m).await?,
                    }
                    if pending_bytes >= FLUSH_BYTES {
                        self.flush_all().await;
                        pending_bytes = 0;
                    }
                }
                cmd = cmds.recv() => {
                    if !self.handle_command(cmd).await {
                        for m in self.outbox.drain(..) {
                            conn.feed(m).await?;
                        }
                        self.flush_all().await;
                        for m in self.outbox.drain(..) {
                            conn.feed(m).await?;
                        }
                        conn.flush().await?;
                        return Ok(Shutdown);
                    }
                    self.emit_changed();
                }
                _ = flush_tick.tick() => {
                    self.flush_all().await;
                    pending_bytes = 0;
                }
                _ = ui_tick.tick() => {
                    if last_rx.elapsed() > IDLE_TIMEOUT {
                        return Err(SessionError::Timeout);
                    }
                    let dt = last_ui.elapsed().as_secs_f64().max(0.001);
                    last_ui = Instant::now();
                    for t in self.torrents.values_mut() {
                        let speed = (t.recv_bytes as f64 / dt) as u64;
                        t.recv_bytes = 0;
                        if speed != t.recv_speed {
                            t.recv_speed = speed;
                            t.changed = true;
                        }
                    }
                    self.emit_changed();
                }
            }
        }
    }

    fn queue_adds(&mut self) {
        for t in self.torrents.values_mut() {
            if t.add_sent || t.complete() || t.error.is_some() {
                continue;
            }
            let (have, files) = {
                let s = t.storage.lock().unwrap();
                (s.have().clone(), s.selected_indices())
            };
            t.add_sent = true;
            self.outbox.push(ClientMessage::AddTorrent {
                torrent_bytes: t.torrent_bytes.clone(),
                have,
                files,
            });
        }
    }

    fn by_server_id(&mut self, id: TorrentId) -> Option<&mut Torrent> {
        self.torrents.values_mut().find(|t| t.server_id == Some(id))
    }

    /// Returns the number of bytes written but not yet acknowledged.
    async fn handle_server(&mut self, msg: ServerMessage) -> Result<u64, SessionError> {
        match msg {
            ServerMessage::TorrentAdded {
                torrent_id,
                info_hash,
                piece_count,
                ..
            } => match self.torrents.get_mut(&info_hash) {
                Some(t) if t.meta.piece_count() != piece_count => {
                    t.error = Some(format!(
                        "server reports {piece_count} pieces, torrent has {}",
                        t.meta.piece_count()
                    ));
                    t.changed = true;
                    self.outbox.push(ClientMessage::Remove { torrent_id });
                }
                Some(t) => {
                    t.server_id = Some(torrent_id);
                    t.changed = true;
                    if t.paused {
                        self.outbox.push(ClientMessage::Pause { torrent_id });
                    }
                    if std::mem::take(&mut t.files_changed) {
                        let s = t.storage.lock().unwrap();
                        self.outbox.push(ClientMessage::SetFiles {
                            torrent_id,
                            files: s.selected_indices(),
                            have: s.have().clone(),
                        });
                    }
                }
                None => self.outbox.push(ClientMessage::Remove { torrent_id }),
            },
            ServerMessage::Status {
                torrent_id,
                state,
                peers,
                down_speed,
                up_speed,
                progress,
                buffer_used,
                buffer_limit,
                ..
            } => {
                if let Some(t) = self.by_server_id(torrent_id) {
                    t.server = Some(ServerStatus {
                        state,
                        peers,
                        down_speed,
                        up_speed,
                        progress,
                        buffer_used,
                        buffer_limit,
                    });
                    t.changed = true;
                }
            }
            ServerMessage::Piece {
                torrent_id,
                index,
                data,
            } => return self.handle_piece(torrent_id, index, data).await,
            ServerMessage::Error {
                torrent_id: Some(id),
                code,
                msg,
            } => {
                let text = format!("server error ({code:?}): {msg}");
                if let Some(t) = self.by_server_id(id) {
                    t.error = Some(text.clone());
                    t.changed = true;
                }
                self.emit(Event::Warning(text));
            }
            ServerMessage::Error {
                torrent_id: None,
                code,
                msg,
            } => return Err(SessionError::Server { code, msg }),
            ServerMessage::HelloOk { .. } | ServerMessage::Removed { .. } => {}
        }
        Ok(0)
    }

    async fn handle_piece(
        &mut self,
        torrent_id: TorrentId,
        index: u32,
        data: Bytes,
    ) -> Result<u64, SessionError> {
        let Some(t) = self.by_server_id(torrent_id) else {
            return Ok(0);
        };
        let len = data.len() as u64;
        t.recv_bytes += len;
        let storage = t.storage.clone();
        let res = tokio::task::spawn_blocking(move || {
            let mut s = storage.lock().unwrap();
            let r = s.write_piece(index, &data);
            (r, s.progress().clone())
        })
        .await
        .expect("storage task panicked");
        let t = self.by_server_id(torrent_id).expect("still present");
        match res {
            (Ok(_), progress) => {
                t.progress = progress;
                t.pending_acks.push(index);
                t.changed = true;
                Ok(len)
            }
            (Err(e @ (StorageError::HashMismatch(_) | StorageError::WrongSize { .. })), ..) => {
                Err(SessionError::BadPiece(e))
            }
            (Err(e), ..) => {
                let text = format!("{}: write failed, pausing: {e}", t.meta.name);
                t.error = Some(e.to_string());
                t.paused = true;
                t.changed = true;
                self.outbox.push(ClientMessage::Pause { torrent_id });
                self.emit(Event::Warning(text));
                Ok(0)
            }
        }
    }

    /// Makes written pieces durable, then queues their `Ack`s.
    async fn flush_all(&mut self) {
        let jobs: Vec<_> = self
            .torrents
            .values()
            .filter(|t| !t.pending_acks.is_empty())
            .map(|t| (t.meta.info_hash, t.storage.clone()))
            .collect();
        if jobs.is_empty() {
            return;
        }
        let results = tokio::task::spawn_blocking(move || {
            jobs.into_iter()
                .map(|(ih, s)| (ih, s.lock().unwrap().flush()))
                .collect::<Vec<_>>()
        })
        .await
        .expect("storage task panicked");
        for (ih, res) in results {
            let Some(t) = self.torrents.get_mut(&ih) else {
                continue;
            };
            let acks = std::mem::take(&mut t.pending_acks);
            match (res, t.server_id) {
                (Ok(()), Some(torrent_id)) => {
                    self.outbox.extend(
                        acks.into_iter()
                            .map(|index| ClientMessage::Ack { torrent_id, index }),
                    );
                }
                (Ok(()), None) => {}
                (Err(e), id) => {
                    let text = format!("{}: flush failed, pausing: {e}", t.meta.name);
                    t.error = Some(e.to_string());
                    t.paused = true;
                    t.changed = true;
                    if let Some(torrent_id) = id {
                        self.outbox.push(ClientMessage::Pause { torrent_id });
                    }
                    self.emit(Event::Warning(text));
                }
            }
        }
    }

    /// Returns `false` on shutdown.
    async fn handle_command(&mut self, cmd: Option<Command>) -> bool {
        match cmd {
            None | Some(Command::Shutdown) => return false,
            Some(Command::Add(t)) => {
                let ih = t.meta.info_hash;
                self.torrents.insert(ih, *t);
                self.queue_adds_if_online();
            }
            Some(Command::Pause(ih)) => {
                if let Some(t) = self.torrents.get_mut(&ih) {
                    t.paused = true;
                    t.changed = true;
                    if let Some(torrent_id) = t.server_id {
                        self.outbox.push(ClientMessage::Pause { torrent_id });
                    }
                }
            }
            Some(Command::Resume(ih)) => {
                if let Some(t) = self.torrents.get_mut(&ih) {
                    t.paused = false;
                    t.error = None;
                    t.changed = true;
                    if let Some(torrent_id) = t.server_id {
                        self.outbox.push(ClientMessage::Resume { torrent_id });
                    }
                }
                self.queue_adds_if_online();
            }
            Some(Command::SetFiles(ih, selected)) => self.set_files(ih, selected).await,
            Some(Command::Remove {
                info_hash,
                delete_files,
            }) => {
                if let Some(t) = self.torrents.remove(&info_hash) {
                    if let Some(torrent_id) = t.server_id {
                        self.outbox.push(ClientMessage::Remove { torrent_id });
                    }
                    let storage = t.storage;
                    let name = t.meta.name.clone();
                    let res = tokio::task::spawn_blocking(move || {
                        let s = Arc::try_unwrap(storage)
                            .ok()
                            .expect("no other storage users")
                            .into_inner()
                            .unwrap();
                        if delete_files {
                            s.delete_files()
                        } else {
                            let mut s = s;
                            s.flush()
                        }
                    })
                    .await
                    .expect("storage task panicked");
                    if let Err(e) = res {
                        self.emit(Event::Warning(format!("{name}: {e}")));
                    }
                    self.known.lock().unwrap().remove(&info_hash);
                    self.emit(Event::Removed(info_hash));
                }
            }
        }
        true
    }

    async fn set_files(&mut self, ih: InfoHash, selected: Vec<bool>) {
        if !self.torrents.contains_key(&ih) {
            return;
        }
        if !selected.contains(&true) {
            self.emit(Event::Warning(AddError::NothingSelected.to_string()));
            return;
        }
        // ACK what is written first: the new selection may drop pieces again
        // and the server must see them as lost, not as still unacknowledged.
        self.flush_all().await;
        let Some(t) = self.torrents.get_mut(&ih) else {
            return;
        };
        let storage = t.storage.clone();
        let res = tokio::task::spawn_blocking(move || {
            let mut s = storage.lock().unwrap();
            s.set_selection(&selected)?;
            Ok::<_, StorageError>((
                s.selected().to_vec(),
                s.selected_indices(),
                s.progress().clone(),
                s.have().clone(),
            ))
        })
        .await
        .expect("storage task panicked");
        let (selected, files, progress, have) = match res {
            Ok(r) => r,
            Err(e) => {
                let text = format!("{}: changing files failed: {e}", t.meta.name);
                self.emit(Event::Warning(text));
                return;
            }
        };
        t.selected = selected;
        t.progress = progress;
        t.changed = true;
        match t.server_id {
            Some(torrent_id) => self.outbox.push(ClientMessage::SetFiles {
                torrent_id,
                files,
                have,
            }),
            None if t.add_sent => t.files_changed = true,
            None => {}
        }
        self.queue_adds_if_online();
    }

    fn queue_adds_if_online(&mut self) {
        if self.online {
            self.queue_adds();
        }
    }

    fn emit_changed(&mut self) {
        let snaps: Vec<_> = self
            .torrents
            .values_mut()
            .filter_map(|t| std::mem::take(&mut t.changed).then(|| t.snapshot()))
            .collect();
        for s in snaps {
            self.emit(Event::Torrent(s));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_address_forms() {
        let p = |s| parse_server(s).map_err(|e| e.to_string());
        assert_eq!(p("1.2.3.4:9000"), Ok(("1.2.3.4".into(), 9000)));
        assert_eq!(p(" example.com "), Ok(("example.com".into(), DEFAULT_PORT)));
        assert_eq!(p("[::1]:80"), Ok(("::1".into(), 80)));
        assert_eq!(p("[::1]"), Ok(("::1".into(), DEFAULT_PORT)));
        assert_eq!(p("fe80::1"), Ok(("fe80::1".into(), DEFAULT_PORT)));
        assert!(p("").is_err());
        assert!(p("host:0").is_err());
        assert!(p("host:x").is_err());
        assert!(p(":80").is_err());
    }
}
