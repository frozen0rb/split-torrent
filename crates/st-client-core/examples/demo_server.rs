//! A fake `st-server` for trying the client without a swarm: serves two
//! generated torrents at a throttled rate over pinned TLS.
//!
//! ```text
//! cargo run -p st-client-core --example demo_server -- /tmp/st-demo
//! ```
//! Then add the `.torrent` files written to that directory in the client.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use st_client_core::testkit::{make_torrent, pseudo_random};
use st_client_core::{Fingerprint, Metainfo};
use st_protocol::{server_framed, ClientMessage, ServerMessage, TorrentState, PROTOCOL_VERSION};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const TOKEN: &str = "demo-token";
const RATE: usize = 3 << 19;
const BUFFER_LIMIT: u64 = 384 << 20;

struct Demo {
    meta: Metainfo,
    pieces: Vec<Bytes>,
}

fn demo_torrents(dir: &std::path::Path) -> Vec<Demo> {
    std::fs::create_dir_all(dir).unwrap();
    let sets: [(&str, Vec<(String, usize)>); 2] = [
        (
            "Open Movie Collection",
            vec![
                ("Big Buck Bunny/bbb_1080p.mkv".into(), 40 << 20),
                ("Big Buck Bunny/bbb.srt".into(), 40_000),
                ("Sintel/sintel_720p.mkv".into(), 30 << 20),
                ("README.txt".into(), 1200),
            ],
        ),
        (
            "debian-live-sample.iso",
            vec![("debian-live-sample.iso".into(), 24 << 20)],
        ),
    ];
    sets.into_iter()
        .enumerate()
        .map(|(i, (name, files))| {
            let data: Vec<(String, Vec<u8>)> = files
                .into_iter()
                .enumerate()
                .map(|(j, (p, len))| (p, pseudo_random(len, (i * 10 + j) as u64)))
                .collect();
            let refs: Vec<(&str, &[u8])> = data
                .iter()
                .map(|(p, d)| (p.as_str(), d.as_slice()))
                .collect();
            let (torrent, pieces) = make_torrent(name, &refs, 256 << 10);
            std::fs::write(dir.join(format!("{name}.torrent")), &torrent).unwrap();
            Demo {
                meta: Metainfo::parse(&torrent).unwrap(),
                pieces: pieces.into_iter().map(Bytes::from).collect(),
            }
        })
        .collect()
}

#[tokio::main]
async fn main() {
    let dir = std::env::args().nth(1).unwrap_or_else(|| "st-demo".into());
    let listen = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "127.0.0.1:7443".into());
    let torrents = Arc::new(demo_torrents(std::path::Path::new(&dir)));

    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = ck.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
    println!("listening on {listen}");
    println!("token:       {TOKEN}");
    println!("fingerprint: {}", Fingerprint::of(&cert));
    println!("torrents in: {dir}");
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind(&listen).await.unwrap();
    loop {
        let (tcp, peer) = listener.accept().await.unwrap();
        let acceptor = acceptor.clone();
        let torrents = torrents.clone();
        tokio::spawn(async move {
            let Ok(tls) = acceptor.accept(tcp).await else {
                return;
            };
            println!("{peer}: connected");
            serve(server_framed(tls), &torrents).await;
            println!("{peer}: disconnected");
        });
    }
}

struct Active {
    idx: usize,
    queue: Vec<u32>,
    unacked: u64,
    have: Vec<bool>,
    wanted: Vec<bool>,
    paused: bool,
}

impl Active {
    fn new(idx: usize, meta: &Metainfo, have: &st_protocol::Bitfield, files: &[u32]) -> Self {
        let mut a = Self {
            idx,
            queue: Vec::new(),
            unacked: 0,
            have: Vec::new(),
            wanted: Vec::new(),
            paused: false,
        };
        a.select(meta, have, files);
        a
    }

    fn select(&mut self, meta: &Metainfo, have: &st_protocol::Bitfield, files: &[u32]) {
        let n = meta.piece_count();
        self.have = (0..n).map(|i| have.get(i)).collect();
        self.wanted = vec![false; n as usize];
        for &f in files.iter().filter(|&&f| (f as usize) < meta.files.len()) {
            for p in meta.pieces_of_file(f as usize) {
                self.wanted[p as usize] = true;
            }
        }
        self.queue = (0..n)
            .filter(|&i| self.wanted[i as usize] && !self.have[i as usize])
            .rev()
            .collect();
    }

    fn counts(&self) -> (u32, u32) {
        let total = self.wanted.iter().filter(|&&w| w).count() as u32;
        let done = (0..self.wanted.len())
            .filter(|&i| self.wanted[i] && self.have[i])
            .count() as u32;
        (done, total)
    }
}

async fn serve(
    mut conn: tokio_util::codec::Framed<
        tokio_rustls::server::TlsStream<tokio::net::TcpStream>,
        st_protocol::ServerCodec,
    >,
    torrents: &[Demo],
) {
    match conn.next().await {
        Some(Ok(ClientMessage::Hello { token, .. })) if token == TOKEN => {}
        _ => {
            let _ = conn
                .send(ServerMessage::Error {
                    torrent_id: None,
                    code: st_protocol::ErrorCode::Unauthorized,
                    msg: "bad token".into(),
                })
                .await;
            return;
        }
    }
    let _ = conn
        .send(ServerMessage::HelloOk {
            version: PROTOCOL_VERSION,
            buffer_limit: BUFFER_LIMIT,
        })
        .await;
    let mut active: HashMap<u32, Active> = HashMap::new();
    let tick = Duration::from_millis(100);
    let mut ticker = tokio::time::interval(tick);
    let mut status = tokio::time::interval(Duration::from_secs(1));
    let mut sent_last_sec: HashMap<u32, u64> = HashMap::new();
    loop {
        tokio::select! {
            msg = conn.next() => {
                let Some(Ok(msg)) = msg else { return };
                match msg {
                    ClientMessage::AddTorrent { torrent_bytes, have, files } => {
                        let Ok(meta) = Metainfo::parse(&torrent_bytes) else { continue };
                        let Some(idx) = torrents.iter().position(|t| t.meta.info_hash == meta.info_hash) else { continue };
                        let id = idx as u32 + 1;
                        active.insert(id, Active::new(idx, &meta, &have, &files));
                        let _ = conn.send(ServerMessage::TorrentAdded {
                            torrent_id: id,
                            info_hash: meta.info_hash,
                            name: meta.name.clone(),
                            piece_count: meta.piece_count(),
                            piece_length: meta.piece_length,
                            total_length: meta.total_length,
                        }).await;
                    }
                    ClientMessage::Ack { torrent_id, index } => {
                        if let Some(a) = active.get_mut(&torrent_id) {
                            a.unacked = a.unacked.saturating_sub(torrents[a.idx].pieces[index as usize].len() as u64);
                            if let Some(h) = a.have.get_mut(index as usize) {
                                *h = true;
                            }
                        }
                    }
                    ClientMessage::SetFiles { torrent_id, files, have } => {
                        if let Some(a) = active.get_mut(&torrent_id) {
                            a.select(&torrents[a.idx].meta, &have, &files);
                        }
                    }
                    ClientMessage::Pause { torrent_id } => { if let Some(a) = active.get_mut(&torrent_id) { a.paused = true } }
                    ClientMessage::Resume { torrent_id } => { if let Some(a) = active.get_mut(&torrent_id) { a.paused = false } }
                    ClientMessage::Remove { torrent_id } => {
                        active.remove(&torrent_id);
                        let _ = conn.send(ServerMessage::Removed { torrent_id }).await;
                    }
                    ClientMessage::Hello { .. } => {}
                }
            }
            _ = ticker.tick() => {
                for (&id, a) in active.iter_mut().filter(|(_, a)| !a.paused) {
                    let mut budget = RATE / 10;
                    while budget > 0 {
                        let Some(index) = a.queue.pop() else { break };
                        let data = torrents[a.idx].pieces[index as usize].clone();
                        budget = budget.saturating_sub(data.len());
                        a.unacked += data.len() as u64;
                        *sent_last_sec.entry(id).or_default() += data.len() as u64;
                        if conn.feed(ServerMessage::Piece { torrent_id: id, index, data }).await.is_err() {
                            return;
                        }
                    }
                }
                if conn.flush().await.is_err() {
                    return;
                }
            }
            _ = status.tick() => {
                let buffer_used: u64 = active.values().map(|a| a.unacked).sum::<u64>() + (18 << 20);
                for (&id, a) in &active {
                    let (done, total) = a.counts();
                    let state = if done == total {
                        TorrentState::Finished
                    } else if a.paused {
                        TorrentState::Paused
                    } else {
                        TorrentState::Downloading
                    };
                    let sent = sent_last_sec.remove(&id).unwrap_or(0);
                    let _ = conn.send(ServerMessage::Status {
                        torrent_id: id,
                        state,
                        peers: if state == TorrentState::Downloading { 9 + id * 7 } else { 0 },
                        down_speed: if state == TorrentState::Downloading { sent * 11 / 10 } else { 0 },
                        up_speed: 0,
                        progress: if total == 0 { 1.0 } else { done as f32 / total as f32 },
                        pieces_done: done,
                        pieces_total: total,
                        buffer_used,
                        buffer_limit: BUFFER_LIMIT,
                    }).await;
                }
            }
        }
    }
}
