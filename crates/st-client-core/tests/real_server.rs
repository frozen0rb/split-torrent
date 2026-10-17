//! `Client` against an in-process `st-server` fed by a local librqbit seed.
#![cfg(unix)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use librqbit::{AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions};
use st_client_core::testkit::pseudo_random;
use st_client_core::{Client, ClientConfig, ConnectionState, Event, TorrentSnapshot};
use st_server::{Config, Server};
use tokio::sync::mpsc::UnboundedReceiver;

const PIECE: u32 = 256 * 1024;
const T: Duration = Duration::from_secs(60);

const FILES: [(&str, usize); 3] = [
    ("a.bin", 5 * PIECE as usize + 1234),
    ("sub/b.bin", 17),
    ("sub/c.bin", 20 * PIECE as usize + 77),
];

struct Seed {
    _session: Arc<Session>,
    addr: SocketAddr,
    torrent: Vec<u8>,
    data_dir: PathBuf,
    /// Unique torrent name; every payload file name also contains it.
    tag: String,
}

fn file_name(tag: &str, path: &str) -> String {
    match path.rsplit_once('/') {
        Some((dir, f)) => format!("{dir}/{tag}-{f}"),
        None => format!("{tag}-{path}"),
    }
}

async fn start_seed(dir: &Path) -> Seed {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let tag = format!("st-e2e-{}-{nanos}", std::process::id());
    let data = dir.join(&tag);
    std::fs::create_dir_all(data.join("sub")).unwrap();
    for (i, (name, len)) in FILES.into_iter().enumerate() {
        std::fs::write(
            data.join(file_name(&tag, name)),
            pseudo_random(len, i as u64 + 100),
        )
        .unwrap();
    }
    let created = librqbit::create_torrent(
        &data,
        CreateTorrentOptions {
            name: Some(&tag),
            trackers: vec![],
            piece_length: Some(PIECE),
        },
        &librqbit::spawn_utils::BlockingSpawner::new(2),
    )
    .await
    .unwrap();
    let torrent = created.as_bytes().unwrap();
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
        torrent: torrent.to_vec(),
        data_dir: data,
        tag,
    }
}

async fn wait(rx: &mut UnboundedReceiver<Event>, mut pred: impl FnMut(&Event) -> bool) -> Event {
    tokio::time::timeout(T, async {
        loop {
            let ev = rx.recv().await.expect("client stopped");
            if pred(&ev) {
                return ev;
            }
        }
    })
    .await
    .expect("timed out")
}

fn assert_same_tree(tag: &str, want: &Path, got: &Path) {
    for (name, _) in FILES {
        let name = file_name(tag, name);
        let a = std::fs::read(want.join(&name)).unwrap();
        let b = std::fs::read(got.join(&name)).unwrap();
        assert!(a == b, "{name} differs");
    }
}

/// Calls `f(path, len)` for every file under `dir` up to `depth` levels,
/// skipping `skip` and anything unreadable (other users' temp files).
fn walk(dir: &Path, depth: usize, skip: &Path, f: &mut impl FnMut(&Path, u64)) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let path = e.path();
        if path.starts_with(skip) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(&path) else {
            continue;
        };
        if meta.is_dir() && depth > 0 {
            walk(&path, depth - 1, skip, f);
        } else if meta.is_file() {
            f(&path, meta.len());
        }
    }
}

/// The server must hold payload only in RAM: its state dir holds just the
/// certificate, key and token, and neither it nor librqbit's default output
/// folder (the system temp dir) gets any file named after the torrent.
fn assert_no_payload_on_server_disk(state_dir: &Path, test_dir: &Path, tag: &str) {
    let payload: u64 = FILES.iter().map(|(_, l)| *l as u64).sum();
    let mut total = 0;
    walk(
        state_dir,
        8,
        test_dir.join("none").as_path(),
        &mut |p, len| {
            total += len;
            assert!(
                len < 64 * 1024,
                "unexpectedly large server file {}",
                p.display()
            );
        },
    );
    assert!(
        total < 256 * 1024 && total < payload / 20,
        "state dir holds {total} bytes"
    );
    let mut seen = 0;
    walk(test_dir, 4, &test_dir.join("none"), &mut |p, _| {
        seen += usize::from(p.to_string_lossy().contains(tag));
    });
    assert!(seen >= 6, "scanner must find the seed and client copies");
    let mut hits = Vec::new();
    walk(&std::env::temp_dir(), 4, test_dir, &mut |p, _| {
        if p.to_string_lossy().contains(tag) {
            hits.push(p.to_owned());
        }
    });
    assert!(
        hits.is_empty(),
        "payload written outside the client: {hits:?}"
    );
    assert!(!std::env::temp_dir().join(tag).exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downloads_from_real_server_and_resumes_after_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let seed = start_seed(&tmp.path().join("seed")).await;
    let server = Server::bind(Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        token: Some("client-core-test-token".into()),
        state_dir: tmp.path().join("server-state"),
        buffer_limit: 16 * 1024 * 1024,
        max_inflight_pieces: 2,
        bt_port: 0,
        dht: false,
        lsd: false,
        trackers: false,
        initial_peers: vec![seed.addr],
        ..Default::default()
    })
    .await
    .unwrap();
    let config = ClientConfig::new(
        &server.local_addr().to_string(),
        server.token(),
        server.fingerprint(),
    )
    .unwrap();
    let state_dir = tmp.path().join("server-state");
    let server = server.spawn();
    let dl = tmp.path().join("downloads");
    let rt = tokio::runtime::Handle::current();

    // First run: stop the client as soon as some pieces are on disk.
    let (client, mut rx) = Client::start(config.clone(), &rt, None);
    let snap = client
        .add_torrent(seed.torrent.clone(), &dl, false)
        .unwrap();
    assert_eq!(snap.piece_count, 26);
    let ev = wait(
        &mut rx,
        |e| matches!(e, Event::Torrent(s) if s.pieces_have > 0),
    )
    .await;
    let Event::Torrent(first) = ev else {
        unreachable!()
    };
    client.shutdown();
    wait(&mut rx, |e| {
        matches!(e, Event::Connection(ConnectionState::Stopped))
    })
    .await;
    drop(client);

    // Second run (like an app restart): resumes from `.st-state`.
    let (client, mut rx) = Client::start(config, &rt, None);
    let snap = client
        .add_torrent(seed.torrent.clone(), &dl, false)
        .unwrap();
    assert!(snap.pieces_have >= first.pieces_have.min(1));
    let ev = wait(&mut rx, |e| matches!(e, Event::Torrent(s) if s.complete())).await;
    let Event::Torrent(TorrentSnapshot {
        bytes_have,
        total_length,
        ..
    }) = ev
    else {
        unreachable!()
    };
    assert_eq!(bytes_have, total_length);
    assert_same_tree(&seed.tag, &seed.data_dir, &dl.join(&seed.tag));
    assert_no_payload_on_server_disk(&state_dir, tmp.path(), &seed.tag);

    client.shutdown();
    server.shutdown().await;
}
