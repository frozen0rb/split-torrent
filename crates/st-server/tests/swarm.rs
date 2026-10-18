//! Server against a local librqbit seed, with a minimal protocol client.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use librqbit::{AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions};
use rand::RngCore;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use st_protocol::{
    cert_fingerprint, client_framed, parse_fingerprint, Bitfield, ClientMessage, ErrorCode,
    ServerMessage, TorrentState, PROTOCOL_VERSION,
};
use st_server::{Config, Server};
use tokio_rustls::TlsConnector;

const PIECE: u32 = 256 * 1024;

#[derive(Debug)]
struct Pinned([u8; 32]);

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if cert_fingerprint(end_entity) == self.0 {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        _: &[u8],
        _: &CertificateDer<'_>,
        _: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Err(rustls::Error::General("TLS 1.2 not supported".into()))
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &rustls::crypto::ring::default_provider().signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

type Client = tokio_util::codec::Framed<
    tokio_rustls::client::TlsStream<tokio::net::TcpStream>,
    st_protocol::ClientCodec,
>;

async fn connect(addr: SocketAddr, fingerprint: &str, token: &str) -> (Client, ServerMessage) {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned(parse_fingerprint(fingerprint).unwrap())))
        .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from("st-server").unwrap(), tcp)
        .await
        .unwrap();
    let mut c = client_framed(tls);
    c.send(ClientMessage::Hello {
        token: token.into(),
        version: PROTOCOL_VERSION,
    })
    .await
    .unwrap();
    let reply = c.next().await.unwrap().unwrap();
    (c, reply)
}

struct Seed {
    _session: Arc<Session>,
    addr: SocketAddr,
    torrent: bytes::Bytes,
    content: Vec<u8>,
    /// Pieces overlapping each file, in torrent order.
    file_pieces: Vec<std::ops::Range<u32>>,
}

async fn start_seed(dir: &Path) -> Seed {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let mut content = Vec::new();
    // Sizes not aligned to pieces so pieces span files.
    for (name, len) in [
        ("a.bin", 3 * PIECE as usize + 1234),
        ("b.bin", 9 * PIECE as usize + 77),
    ] {
        let mut buf = vec![0u8; len];
        rand::rng().fill_bytes(&mut buf);
        std::fs::write(data.join(name), &buf).unwrap();
        content.extend_from_slice(&buf);
    }
    let created = librqbit::create_torrent(
        &data,
        CreateTorrentOptions {
            name: Some("data"),
            trackers: vec![],
            piece_length: Some(PIECE),
        },
        &librqbit::spawn_utils::BlockingSpawner::new(2),
    )
    .await
    .unwrap();
    let torrent = created.as_bytes().unwrap();
    // The torrent's file order is the directory walk order, not ours.
    let info = librqbit::torrent_from_bytes(&torrent)
        .unwrap()
        .info
        .data
        .validate()
        .unwrap();
    content.clear();
    for f in info.iter_file_details() {
        content.extend(std::fs::read(data.join(f.filename.to_pathbuf())).unwrap());
    }
    let file_pieces = info.iter_file_details_ext().map(|f| f.pieces).collect();
    let session = Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(librqbit::ListenerOptions {
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                ipv4_only: true,
                ..Default::default()
            }),
            ipv4_only: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let h = session
        .add_torrent(
            AddTorrent::from_bytes(torrent.clone()),
            Some(AddTorrentOptions {
                output_folder: Some(created.output_folder.to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    h.wait_until_initialized().await.unwrap();
    assert!(h.stats().finished, "seed must have all data");
    let port = session.listen_addr().unwrap().port();
    Seed {
        _session: session,
        addr: SocketAddr::from(([127, 0, 0, 1], port)),
        torrent,
        content,
        file_pieces,
    }
}

fn server_config(dir: &Path, seed: SocketAddr) -> Config {
    Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        token: Some("test-token-0123456789".into()),
        state_dir: dir.join("state"),
        buffer_limit: 16 * 1024 * 1024,
        max_inflight_pieces: 4,
        bt_port: 0,
        dht: false,
        lsd: false,
        trackers: false,
        initial_peers: vec![seed],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejects_bad_token_and_wrong_fingerprint() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = server_config(dir.path(), "127.0.0.1:1".parse().unwrap());
    cfg.initial_peers.clear();
    let server = Server::bind(cfg).await.unwrap();
    let (addr, fp) = (server.local_addr(), server.fingerprint().to_owned());
    let h = server.spawn();

    let (_c, reply) = connect(addr, &fp, "wrong-token-0000000000").await;
    assert!(matches!(
        reply,
        ServerMessage::Error {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));

    let other = st_protocol::format_fingerprint(&[0; 32]);
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned(parse_fingerprint(&other).unwrap())))
        .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let r = TlsConnector::from(Arc::new(cfg))
        .connect(ServerName::try_from("st-server").unwrap(), tcp)
        .await;
    assert!(r.is_err());
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_from_seed_with_backpressure_and_resume() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("st_server=debug,librqbit=info")
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let seed = start_seed(dir.path()).await;
    let server = Server::bind(server_config(dir.path(), seed.addr))
        .await
        .unwrap();
    let (addr, fp, token) = (
        server.local_addr(),
        server.fingerprint().to_owned(),
        server.token().to_owned(),
    );
    let h = server.spawn();
    let pool = h.engine.pool().clone();
    let limit = pool.limit();

    let (mut c, hello) = connect(addr, &fp, &token).await;
    assert!(matches!(hello, ServerMessage::HelloOk { .. }));

    // The client already has piece 1.
    let total = seed.content.len() as u64;
    let piece_count = total.div_ceil(u64::from(PIECE)) as u32;
    let mut have = Bitfield::new(piece_count);
    have.set(1, true);
    c.send(ClientMessage::AddTorrent {
        torrent_bytes: seed.torrent.clone(),
        have: have.clone(),
        files: vec![0, 1],
    })
    .await
    .unwrap();
    let tid = match c.next().await.unwrap().unwrap() {
        ServerMessage::TorrentAdded {
            torrent_id,
            piece_count: n,
            piece_length,
            total_length,
            ..
        } => {
            assert_eq!((n, piece_length, total_length), (piece_count, PIECE, total));
            torrent_id
        }
        m => panic!("unexpected {m:?}"),
    };

    let check = |index: u32, data: &[u8]| {
        let start = index as usize * PIECE as usize;
        assert_eq!(
            data,
            &seed.content[start..start + data.len()],
            "piece {index}"
        );
    };

    // Phase 1: receive without ACKing. The buffer (64 pieces) is larger than
    // the torrent (13 pieces), so we get everything except piece 1.
    let mut got = Vec::new();
    let mut max_used = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while got.len() < piece_count as usize - 1 {
        let m = tokio::time::timeout_at(deadline, c.next())
            .await
            .expect("timed out waiting for pieces")
            .unwrap()
            .unwrap();
        match m {
            ServerMessage::Piece {
                torrent_id,
                index,
                data,
            } => {
                assert_eq!(torrent_id, tid);
                assert_ne!(index, 1, "server sent a piece the client has");
                check(index, &data);
                got.push(index);
            }
            ServerMessage::Status { buffer_used, .. } => max_used = max_used.max(buffer_used),
            ServerMessage::Error { msg, .. } => panic!("server error: {msg}"),
            _ => {}
        }
        assert!(pool.used() <= limit);
    }
    assert!(max_used <= limit);
    let unacked = pool.used();
    assert!(unacked > 0, "pieces stay in RAM until ACK");

    // Reconnect before ACKing: the server must resend every unACKed piece.
    drop(c);
    let (mut c, _) = connect(addr, &fp, &token).await;
    c.send(ClientMessage::AddTorrent {
        torrent_bytes: seed.torrent.clone(),
        have,
        files: vec![0, 1],
    })
    .await
    .unwrap();
    let mut resent = std::collections::BTreeSet::new();
    let mut finished = false;
    while !finished {
        let m = tokio::time::timeout_at(deadline, c.next())
            .await
            .expect("timed out after reconnect")
            .unwrap()
            .unwrap();
        match m {
            ServerMessage::TorrentAdded { torrent_id, .. } => assert_eq!(torrent_id, tid),
            ServerMessage::Piece { index, data, .. } => {
                check(index, &data);
                resent.insert(index);
                c.send(ClientMessage::Ack {
                    torrent_id: tid,
                    index,
                })
                .await
                .unwrap();
            }
            ServerMessage::Status { state, .. } => finished = state == TorrentState::Finished,
            ServerMessage::Error { msg, .. } => panic!("server error: {msg}"),
            _ => {}
        }
    }
    assert_eq!(resent.len(), piece_count as usize - 1);
    assert_eq!(pool.used(), 0, "memory freed after ACK");

    c.send(ClientMessage::Remove { torrent_id: tid })
        .await
        .unwrap();
    loop {
        if let ServerMessage::Removed { torrent_id } = c.next().await.unwrap().unwrap() {
            assert_eq!(torrent_id, tid);
            break;
        }
    }
    let state_files: Vec<_> = std::fs::read_dir(dir.path().join("state"))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(state_files.len(), 2, "only cert and key: {state_files:?}");
    h.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn buffer_limit_holds_when_client_does_not_ack() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("big");
    std::fs::create_dir_all(&data).unwrap();
    // 40 MiB torrent through a 16 MiB buffer.
    let mut buf = vec![0u8; 40 * 1024 * 1024];
    rand::rng().fill_bytes(&mut buf);
    std::fs::write(data.join("big.bin"), &buf).unwrap();
    let seed = {
        let created = librqbit::create_torrent(
            &data,
            CreateTorrentOptions {
                name: Some("big"),
                trackers: vec![],
                piece_length: Some(1024 * 1024),
            },
            &librqbit::spawn_utils::BlockingSpawner::new(2),
        )
        .await
        .unwrap();
        let session = Session::new_with_opts(
            dir.path().to_path_buf(),
            SessionOptions {
                dht: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(librqbit::ListenerOptions {
                    listen_addr: "127.0.0.1:0".parse().unwrap(),
                    ipv4_only: true,
                    ..Default::default()
                }),
                ipv4_only: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let h = session
            .add_torrent(
                AddTorrent::from_bytes(created.as_bytes().unwrap()),
                Some(AddTorrentOptions {
                    output_folder: Some(created.output_folder.to_str().unwrap().to_owned()),
                    overwrite: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_handle()
            .unwrap();
        h.wait_until_initialized().await.unwrap();
        let port = session.listen_addr().unwrap().port();
        (
            session,
            created.as_bytes().unwrap(),
            SocketAddr::from(([127, 0, 0, 1], port)),
        )
    };

    let server = Server::bind(server_config(dir.path(), seed.2))
        .await
        .unwrap();
    let (addr, fp, token) = (
        server.local_addr(),
        server.fingerprint().to_owned(),
        server.token().to_owned(),
    );
    let h = server.spawn();
    let pool = h.engine.pool().clone();
    let (mut c, _) = connect(addr, &fp, &token).await;
    c.send(ClientMessage::AddTorrent {
        torrent_bytes: seed.1.clone(),
        have: Bitfield::default(),
        files: vec![0],
    })
    .await
    .unwrap();

    let mut received = std::collections::BTreeMap::new();
    let stall = async {
        while let Some(Ok(m)) = c.next().await {
            if let ServerMessage::Piece { index, data, .. } = m {
                received.insert(index, data);
            }
            assert!(pool.used() <= pool.limit(), "{:?}", pool.stats());
        }
    };
    let _ = tokio::time::timeout(Duration::from_secs(8), stall).await;
    let stats = pool.stats();
    assert!(stats.used <= stats.limit, "{stats:?}");
    assert!(
        received.len() < 40,
        "download must stall without ACKs, got {}",
        received.len()
    );
    assert!(!received.is_empty());

    // ACK everything; download completes.
    let mut done = std::collections::BTreeSet::new();
    for (&i, d) in &received {
        assert_eq!(&d[..], &buf[i as usize * (1 << 20)..][..d.len()]);
        c.send(ClientMessage::Ack {
            torrent_id: 0,
            index: i,
        })
        .await
        .unwrap();
        done.insert(i);
    }
    let finish = async {
        while done.len() < 40 {
            match c.next().await.unwrap().unwrap() {
                ServerMessage::Piece { index, data, .. } => {
                    assert_eq!(&data[..], &buf[index as usize * (1 << 20)..][..data.len()]);
                    c.send(ClientMessage::Ack {
                        torrent_id: 0,
                        index,
                    })
                    .await
                    .unwrap();
                    done.insert(index);
                }
                ServerMessage::Error { msg, .. } => panic!("{msg}"),
                _ => {}
            }
            assert!(pool.used() <= pool.limit());
        }
    };
    tokio::time::timeout(Duration::from_secs(60), finish)
        .await
        .expect("download did not finish after ACKs");
    h.shutdown().await;
}

fn pieces_of(seed: &Seed, files: &[usize]) -> std::collections::BTreeSet<u32> {
    files
        .iter()
        .flat_map(|&f| seed.file_pieces[f].clone())
        .collect()
}

/// Receives (and optionally ACKs) pieces until the server reports
/// `Finished` for exactly `want` selected pieces, checking every piece's data
/// and that it belongs to `allowed`.
async fn drain_until_finished(
    c: &mut Client,
    seed: &Seed,
    tid: u32,
    allowed: &std::collections::BTreeSet<u32>,
    want: u32,
) -> std::collections::BTreeSet<u32> {
    let mut got = std::collections::BTreeSet::new();
    let run = async {
        loop {
            match c.next().await.unwrap().unwrap() {
                ServerMessage::Piece { index, data, .. } => {
                    assert!(allowed.contains(&index), "piece {index} is not selected");
                    let start = index as usize * PIECE as usize;
                    assert_eq!(&data[..], &seed.content[start..start + data.len()]);
                    got.insert(index);
                    c.send(ClientMessage::Ack {
                        torrent_id: tid,
                        index,
                    })
                    .await
                    .unwrap();
                }
                ServerMessage::Status {
                    state: TorrentState::Finished,
                    pieces_total,
                    pieces_done,
                    ..
                } => {
                    assert_eq!((pieces_done, pieces_total), (want, want));
                    return;
                }
                ServerMessage::Error { msg, .. } => panic!("server error: {msg}"),
                _ => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("selection did not finish");
    got
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_selected_files_and_follows_selection_changes() {
    let dir = tempfile::tempdir().unwrap();
    let seed = start_seed(dir.path()).await;
    let server = Server::bind(server_config(dir.path(), seed.addr))
        .await
        .unwrap();
    let (addr, fp, token) = (
        server.local_addr(),
        server.fingerprint().to_owned(),
        server.token().to_owned(),
    );
    let h = server.spawn();
    let pool = h.engine.pool().clone();
    let piece_count = seed.content.len().div_ceil(PIECE as usize) as u32;
    let only0 = pieces_of(&seed, &[0]);
    let only1 = pieces_of(&seed, &[1]);
    let shared: Vec<u32> = only0.intersection(&only1).copied().collect();
    assert_eq!(shared.len(), 1, "the files share one piece");

    let (mut c, _) = connect(addr, &fp, &token).await;
    c.send(ClientMessage::AddTorrent {
        torrent_bytes: seed.torrent.clone(),
        have: Bitfield::default(),
        files: vec![1],
    })
    .await
    .unwrap();
    let tid = match c.next().await.unwrap().unwrap() {
        ServerMessage::TorrentAdded { torrent_id, .. } => torrent_id,
        m => panic!("unexpected {m:?}"),
    };

    // Receive all of file 1 without ACKing: it stays buffered.
    let mut got = std::collections::BTreeSet::new();
    while got.len() < only1.len() {
        let m = tokio::time::timeout(Duration::from_secs(60), c.next())
            .await
            .expect("timed out waiting for file 1")
            .unwrap()
            .unwrap();
        if let ServerMessage::Piece { index, .. } = m {
            assert!(only1.contains(&index), "piece {index} is not in file 1");
            got.insert(index);
        }
    }
    let size = |set: &std::collections::BTreeSet<u32>| -> u64 {
        set.iter()
            .map(|&i| (seed.content.len() - i as usize * PIECE as usize).min(PIECE as usize) as u64)
            .sum()
    };
    assert_eq!(pool.used(), size(&only1));

    // Switch to file 0: the buffered pieces of file 1 are freed, except the
    // piece it shares with file 0. Paused, so pieces of file 0 do not refill
    // the buffer before it is measured.
    c.send(ClientMessage::Pause { torrent_id: tid })
        .await
        .unwrap();
    c.send(ClientMessage::SetFiles {
        torrent_id: tid,
        files: vec![0],
        have: Bitfield::default(),
    })
    .await
    .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while pool.used() > size(&shared.iter().copied().collect()) {
        assert!(tokio::time::Instant::now() < deadline, "{:?}", pool.stats());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    c.send(ClientMessage::Resume { torrent_id: tid })
        .await
        .unwrap();
    // The shared piece was already sent; ACK it like a client that stored it.
    c.send(ClientMessage::Ack {
        torrent_id: tid,
        index: shared[0],
    })
    .await
    .unwrap();
    let mut allowed = only0.clone();
    allowed.extend(&only1);
    let mut have = Bitfield::new(piece_count);
    for i in drain_until_finished(&mut c, &seed, tid, &allowed, only0.len() as u32).await {
        if only0.contains(&i) {
            have.set(i, true);
        }
    }
    have.set(shared[0], true);
    assert!(only0.iter().all(|&i| have.get(i)));
    assert_eq!(pool.used(), 0);

    // Select everything. The client dropped the shared piece because it only
    // wrote file 0's part of it, so the server must fetch it again even
    // though librqbit already had it.
    have.set(shared[0], false);
    c.send(ClientMessage::SetFiles {
        torrent_id: tid,
        files: vec![0, 1],
        have: have.clone(),
    })
    .await
    .unwrap();
    let all: std::collections::BTreeSet<u32> = (0..piece_count).collect();
    let rest = drain_until_finished(&mut c, &seed, tid, &all, piece_count).await;
    let missing: std::collections::BTreeSet<u32> = have.iter_zeros().collect();
    assert!(
        missing.is_subset(&rest),
        "missing {missing:?}, got {rest:?}"
    );
    assert!(rest.contains(&shared[0]));
    assert!(rest.iter().all(|i| !only0.contains(i) || *i == shared[0]));
    h.shutdown().await;
}
