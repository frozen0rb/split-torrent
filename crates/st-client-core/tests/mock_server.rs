//! Drives `st_client_core::Client` against a mock `st-server` speaking the
//! real protocol over pinned TLS.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use st_client_core::testkit::{make_torrent, pseudo_random};
use st_client_core::{
    Backoff, Client, ClientConfig, ConnectionState, Event, Fingerprint, Metainfo, TorrentSnapshot,
};
use st_protocol::{
    server_framed, Bitfield, ClientMessage, ErrorCode, ServerCodec, ServerMessage, TorrentState,
    PROTOCOL_VERSION,
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;

type Conn = Framed<TlsStream<TcpStream>, ServerCodec>;
const TOKEN: &str = "s3cret";
const T: Duration = Duration::from_secs(10);

struct Mock {
    addr: SocketAddr,
    fingerprint: Fingerprint,
    conns: mpsc::UnboundedReceiver<Conn>,
}

async fn mock_server() -> Mock {
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert: CertificateDer<'static> = ck.cert.der().clone();
    let fingerprint = Fingerprint::of(&cert);
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(ck.signing_key.serialize_der()));
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(vec![cert], key)
    .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, conns) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let (tcp, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                if let Ok(tls) = acceptor.accept(tcp).await {
                    let _ = tx.send(server_framed(tls));
                }
            });
        }
    });
    Mock {
        addr,
        fingerprint,
        conns,
    }
}

impl Mock {
    fn config(&self, token: &str, fingerprint: Fingerprint) -> ClientConfig {
        let mut c =
            ClientConfig::new(&self.addr.to_string(), token, &fingerprint.to_string()).unwrap();
        c.backoff = Backoff {
            min: Duration::from_millis(50),
            max: Duration::from_millis(300),
        };
        c
    }

    async fn next_conn(&mut self) -> Conn {
        tokio::time::timeout(T, self.conns.recv())
            .await
            .unwrap()
            .unwrap()
    }
}

async fn recv(conn: &mut Conn) -> ClientMessage {
    tokio::time::timeout(T, conn.next())
        .await
        .expect("timed out waiting for client")
        .expect("client closed")
        .unwrap()
}

/// Hello + AddTorrent; returns the `have` bitfield.
async fn accept_torrent(conn: &mut Conn, meta: &Metainfo, id: u32) -> Bitfield {
    assert_eq!(
        recv(conn).await,
        ClientMessage::Hello {
            token: TOKEN.into(),
            version: PROTOCOL_VERSION
        }
    );
    conn.send(ServerMessage::HelloOk {
        version: PROTOCOL_VERSION,
        buffer_limit: 64 << 20,
    })
    .await
    .unwrap();
    let ClientMessage::AddTorrent {
        torrent_bytes,
        have,
    } = recv(conn).await
    else {
        panic!("expected AddTorrent");
    };
    assert_eq!(
        Metainfo::parse(&torrent_bytes).unwrap().info_hash,
        meta.info_hash
    );
    conn.send(ServerMessage::TorrentAdded {
        torrent_id: id,
        info_hash: meta.info_hash,
        name: meta.name.clone(),
        piece_count: meta.piece_count(),
        piece_length: meta.piece_length,
        total_length: meta.total_length,
    })
    .await
    .unwrap();
    have
}

/// Sends `indices` and waits for an `Ack` for each of them.
async fn send_pieces(conn: &mut Conn, id: u32, pieces: &[Vec<u8>], indices: &[u32]) {
    for &i in indices {
        conn.send(ServerMessage::Piece {
            torrent_id: id,
            index: i,
            data: Bytes::from(pieces[i as usize].clone()),
        })
        .await
        .unwrap();
    }
    let mut acked = Vec::new();
    while acked.len() < indices.len() {
        match recv(conn).await {
            ClientMessage::Ack { torrent_id, index } => {
                assert_eq!(torrent_id, id);
                acked.push(index);
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    acked.sort();
    let mut want = indices.to_vec();
    want.sort();
    assert_eq!(acked, want);
}

async fn wait_event(
    rx: &mut mpsc::UnboundedReceiver<Event>,
    mut pred: impl FnMut(&Event) -> bool,
) -> Event {
    tokio::time::timeout(T, async {
        loop {
            let ev = rx.recv().await.expect("client stopped");
            if pred(&ev) {
                return ev;
            }
        }
    })
    .await
    .expect("timed out waiting for event")
}

fn complete(ev: &Event) -> bool {
    matches!(ev, Event::Torrent(s) if s.complete())
}

struct Fixture {
    torrent: Vec<u8>,
    meta: Metainfo,
    pieces: Vec<Vec<u8>>,
    files: Vec<(String, Vec<u8>)>,
}

fn fixture() -> Fixture {
    let files = vec![
        ("season 1/ep1.mkv".to_string(), pseudo_random(5000, 1)),
        ("season 1/ep2.mkv".to_string(), pseudo_random(3333, 2)),
        ("readme.txt".to_string(), pseudo_random(17, 3)),
    ];
    let refs: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(p, d)| (p.as_str(), d.as_slice()))
        .collect();
    let (torrent, pieces) = make_torrent("Show", &refs, 1024);
    let meta = Metainfo::parse(&torrent).unwrap();
    Fixture {
        torrent,
        meta,
        pieces,
        files,
    }
}

fn assert_files(dir: &Path, f: &Fixture) {
    for (path, data) in &f.files {
        let got = std::fs::read(dir.join("Show").join(path)).unwrap();
        assert!(got == *data, "{path} differs");
    }
}

#[tokio::test]
async fn downloads_and_resumes_after_disconnect() {
    let mut mock = mock_server().await;
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let n = f.meta.piece_count();
    assert_eq!(n, 9);

    let (client, mut rx) = Client::start(
        mock.config(TOKEN, mock.fingerprint),
        &tokio::runtime::Handle::current(),
        None,
    );
    let snap = client
        .add_torrent(f.torrent.clone(), dir.path(), false)
        .unwrap();
    assert_eq!(snap.pieces_have, 0);
    assert!(matches!(
        client.add_torrent(f.torrent.clone(), dir.path(), false),
        Err(st_client_core::AddError::Duplicate)
    ));

    // First connection: deliver five pieces out of order, then drop.
    let mut conn = mock.next_conn().await;
    let have = accept_torrent(&mut conn, &f.meta, 7).await;
    assert_eq!(have.count_ones(), 0);
    send_pieces(&mut conn, 7, &f.pieces, &[8, 0, 4, 5, 2]).await;
    conn.send(ServerMessage::Status {
        torrent_id: 7,
        state: TorrentState::Downloading,
        peers: 3,
        down_speed: 1000,
        up_speed: 0,
        progress: 5.0 / 9.0,
        pieces_done: 5,
        pieces_total: n,
        buffer_used: 4096,
        buffer_limit: 64 << 20,
    })
    .await
    .unwrap();
    wait_event(&mut rx, |e| {
        matches!(e, Event::Torrent(TorrentSnapshot { server: Some(s), pieces_have: 5, .. }) if s.peers == 3)
    })
    .await;
    drop(conn);
    wait_event(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Disconnected { .. }))
    })
    .await;

    // Reconnect: the client resends AddTorrent with exactly the acked pieces.
    let mut conn = mock.next_conn().await;
    let have = accept_torrent(&mut conn, &f.meta, 8).await;
    assert_eq!(have.iter_ones().collect::<Vec<_>>(), vec![0, 2, 4, 5, 8]);
    // A duplicate of a piece the client already has is acked, not rewritten.
    let mut missing: Vec<u32> = have.iter_zeros().collect();
    missing.push(0);
    send_pieces(&mut conn, 8, &f.pieces, &missing).await;
    wait_event(&mut rx, complete).await;
    assert_files(dir.path(), &f);

    client.shutdown();
    wait_event(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Stopped))
    })
    .await;

    // A new client (e.g. after an app restart) resumes from `.st-state` and
    // does not register a finished torrent again.
    let (client, mut rx) = Client::start(
        mock.config(TOKEN, mock.fingerprint),
        &tokio::runtime::Handle::current(),
        None,
    );
    let snap = client
        .add_torrent(f.torrent.clone(), dir.path(), false)
        .unwrap();
    assert!(snap.complete());
    let mut conn = mock.next_conn().await;
    recv(&mut conn).await;
    conn.send(ServerMessage::HelloOk {
        version: PROTOCOL_VERSION,
        buffer_limit: 1,
    })
    .await
    .unwrap();
    wait_event(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Connected { .. }))
    })
    .await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), conn.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn corrupt_piece_forces_reconnect_without_ack() {
    let mut mock = mock_server().await;
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let (client, mut rx) = Client::start(
        mock.config(TOKEN, mock.fingerprint),
        &tokio::runtime::Handle::current(),
        None,
    );
    client
        .add_torrent(f.torrent.clone(), dir.path(), false)
        .unwrap();
    let mut conn = mock.next_conn().await;
    accept_torrent(&mut conn, &f.meta, 1).await;
    let mut bad = f.pieces[3].clone();
    bad[10] ^= 0xff;
    conn.send(ServerMessage::Piece {
        torrent_id: 1,
        index: 3,
        data: bad.into(),
    })
    .await
    .unwrap();
    let ev = wait_event(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Disconnected { .. }))
    })
    .await;
    assert!(format!("{ev:?}").contains("SHA-1"), "{ev:?}");
    // No Ack: the connection just ends (EOF or a TLS error, never a message).
    assert!(!matches!(
        tokio::time::timeout(T, conn.next()).await.unwrap(),
        Some(Ok(_))
    ));

    let mut conn = mock.next_conn().await;
    let have = accept_torrent(&mut conn, &f.meta, 1).await;
    assert_eq!(have.count_ones(), 0);
    let all: Vec<u32> = (0..f.meta.piece_count()).collect();
    send_pieces(&mut conn, 1, &f.pieces, &all).await;
    wait_event(&mut rx, complete).await;
    assert_files(dir.path(), &f);
}

#[tokio::test]
async fn pause_resume_remove_are_forwarded() {
    let mut mock = mock_server().await;
    let f = fixture();
    let dir = tempfile::tempdir().unwrap();
    let (client, mut rx) = Client::start(
        mock.config(TOKEN, mock.fingerprint),
        &tokio::runtime::Handle::current(),
        None,
    );
    // Added paused: the client pauses it right after registration.
    let snap = client
        .add_torrent(f.torrent.clone(), dir.path(), true)
        .unwrap();
    let mut conn = mock.next_conn().await;
    accept_torrent(&mut conn, &f.meta, 5).await;
    assert_eq!(
        recv(&mut conn).await,
        ClientMessage::Pause { torrent_id: 5 }
    );
    client.resume(snap.info_hash);
    assert_eq!(
        recv(&mut conn).await,
        ClientMessage::Resume { torrent_id: 5 }
    );
    client.pause(snap.info_hash);
    assert_eq!(
        recv(&mut conn).await,
        ClientMessage::Pause { torrent_id: 5 }
    );
    client.remove(snap.info_hash, true);
    assert_eq!(
        recv(&mut conn).await,
        ClientMessage::Remove { torrent_id: 5 }
    );
    wait_event(
        &mut rx,
        |e| matches!(e, Event::Removed(ih) if *ih == snap.info_hash),
    )
    .await;
    assert!(!dir.path().join("Show").exists());
    // Pieces for a removed torrent are ignored.
    conn.send(ServerMessage::Piece {
        torrent_id: 5,
        index: 0,
        data: f.pieces[0].clone().into(),
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), conn.next())
            .await
            .is_err()
    );
    // It can be added again afterwards.
    client
        .add_torrent(f.torrent.clone(), dir.path(), false)
        .unwrap();
    assert!(matches!(
        recv(&mut conn).await,
        ClientMessage::AddTorrent { .. }
    ));
}

#[tokio::test]
async fn wrong_fingerprint_is_rejected() {
    let mut mock = mock_server().await;
    let (_client, mut rx) = Client::start(
        mock.config(TOKEN, Fingerprint([0x11; 32])),
        &tokio::runtime::Handle::current(),
        None,
    );
    let ev = wait_event(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Disconnected { .. }))
    })
    .await;
    let Event::Connection(ConnectionState::Disconnected { error, retry_in }) = ev else {
        unreachable!()
    };
    assert!(error.contains("fingerprint"), "{error}");
    assert_eq!(retry_in, Duration::from_millis(300));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), mock.conns.recv())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn bad_token_reports_server_error_and_retries() {
    let mut mock = mock_server().await;
    let (_client, mut rx) = Client::start(
        mock.config("wrong", mock.fingerprint),
        &tokio::runtime::Handle::current(),
        None,
    );
    for _ in 0..2 {
        let mut conn = mock.next_conn().await;
        assert!(
            matches!(recv(&mut conn).await, ClientMessage::Hello { token, .. } if token == "wrong")
        );
        conn.send(ServerMessage::Error {
            torrent_id: None,
            code: ErrorCode::Unauthorized,
            msg: "bad token".into(),
        })
        .await
        .unwrap();
        let ev = wait_event(&mut rx, |e| {
            matches!(e, Event::Connection(ConnectionState::Disconnected { .. }))
        })
        .await;
        assert!(format!("{ev:?}").contains("Unauthorized"), "{ev:?}");
    }
}
