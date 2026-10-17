//! Helpers for tests and demos: build `.torrent` files from in-memory data.

use std::collections::BTreeMap;

use sha1::{Digest, Sha1};

use crate::bencode::Value;

/// Builds a v1 torrent. A single entry whose path equals `name` produces the
/// single-file layout; anything else is multi-file with `/`-separated paths.
/// Returns the torrent bytes and the pieces' data.
pub fn make_torrent(name: &str, files: &[(&str, &[u8])], piece_length: u32) -> (Vec<u8>, Vec<Vec<u8>>) {
    let all: Vec<u8> = files.iter().flat_map(|(_, d)| d.iter().copied()).collect();
    let pieces: Vec<Vec<u8>> = all.chunks(piece_length as usize).map(<[u8]>::to_vec).collect();
    let hashes: Vec<u8> = pieces.iter().flat_map(|p| Sha1::digest(p)).collect();

    let s = |v: &str| Value::Bytes(v.as_bytes().to_vec());
    let mut info = BTreeMap::new();
    info.insert(b"name".to_vec(), s(name));
    info.insert(b"piece length".to_vec(), Value::Int(i64::from(piece_length)));
    info.insert(b"pieces".to_vec(), Value::Bytes(hashes));
    if let [(path, data)] = files {
        if *path == name {
            info.insert(b"length".to_vec(), Value::Int(data.len() as i64));
        }
    }
    if !info.contains_key(b"length".as_slice()) {
        let list = files
            .iter()
            .map(|(path, data)| {
                let mut f = BTreeMap::new();
                f.insert(b"length".to_vec(), Value::Int(data.len() as i64));
                f.insert(
                    b"path".to_vec(),
                    Value::List(path.split('/').map(s).collect()),
                );
                Value::Dict(f)
            })
            .collect();
        info.insert(b"files".to_vec(), Value::List(list));
    }
    let mut root = BTreeMap::new();
    root.insert(b"announce".to_vec(), s("http://127.0.0.1:1/announce"));
    root.insert(b"info".to_vec(), Value::Dict(info));
    let mut out = Vec::new();
    Value::Dict(root).encode(&mut out);
    (out, pieces)
}

/// Deterministic pseudo-random bytes.
pub fn pseudo_random(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}
