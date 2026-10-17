//! `.torrent` (BEP 3) metainfo: piece hashes and the file layout.

use std::path::{Component, Path, PathBuf};

use sha1::{Digest, Sha1};

use crate::bencode::{self, Value};

pub type InfoHash = [u8; 20];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileEntry {
    /// Sanitized path relative to the torrent root.
    pub path: PathBuf,
    pub length: u64,
    /// Byte offset of this file within the concatenated torrent data.
    pub offset: u64,
    /// BEP 47 padding file: occupies offsets but is never written to disk.
    pub pad: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metainfo {
    pub info_hash: InfoHash,
    /// Sanitized torrent name, used as the file name (single-file) or the
    /// root directory (multi-file).
    pub name: String,
    pub piece_length: u32,
    pub pieces: Vec<[u8; 20]>,
    pub files: Vec<FileEntry>,
    pub total_length: u64,
    /// `true` for the single-file layout (no `files` list).
    pub single_file: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum MetainfoError {
    #[error(transparent)]
    Bencode(#[from] bencode::DecodeError),
    #[error("invalid torrent: {0}")]
    Invalid(&'static str),
}

fn invalid(what: &'static str) -> MetainfoError {
    MetainfoError::Invalid(what)
}

impl Metainfo {
    pub fn parse(torrent: &[u8]) -> Result<Self, MetainfoError> {
        let (root, span) = bencode::decode_with_span(torrent, b"info")?;
        let span = span.ok_or(invalid("missing info dictionary"))?;
        let info_hash: InfoHash = Sha1::digest(&torrent[span]).into();
        let info = root.get("info").ok_or(invalid("missing info"))?;

        let name_raw = utf8_field(info, "name").ok_or(invalid("missing name"))?;
        let name = sanitize_component(&name_raw);

        let piece_length = info
            .get("piece length")
            .and_then(Value::as_int)
            .filter(|&l| l > 0 && l <= i64::from(u32::MAX))
            .ok_or(invalid("bad piece length"))? as u32;

        let raw = info
            .get("pieces")
            .and_then(Value::as_bytes)
            .ok_or(invalid("missing pieces (v2-only torrents are unsupported)"))?;
        if raw.len() % 20 != 0 {
            return Err(invalid("pieces length not a multiple of 20"));
        }
        let pieces: Vec<[u8; 20]> = raw.as_chunks::<20>().0.to_vec();

        let mut files = Vec::new();
        let mut offset = 0u64;
        let single_file = info.get("files").is_none();
        if single_file {
            let length = non_negative(info.get("length")).ok_or(invalid("bad length"))?;
            files.push(FileEntry {
                path: PathBuf::from(&name),
                length,
                offset: 0,
                pad: false,
            });
            offset = length;
        } else {
            let list = info
                .get("files")
                .and_then(Value::as_list)
                .ok_or(invalid("bad files"))?;
            if list.is_empty() {
                return Err(invalid("empty files list"));
            }
            for f in list {
                let length = non_negative(f.get("length")).ok_or(invalid("bad file length"))?;
                let comps = f
                    .get("path.utf-8")
                    .or_else(|| f.get("path"))
                    .and_then(Value::as_list)
                    .filter(|l| !l.is_empty())
                    .ok_or(invalid("bad file path"))?;
                let mut path = PathBuf::new();
                for c in comps {
                    let c = c.as_bytes().ok_or(invalid("bad path component"))?;
                    path.push(sanitize_component(&String::from_utf8_lossy(c)));
                }
                let pad = f
                    .get("attr")
                    .and_then(Value::as_bytes)
                    .is_some_and(|a| a.contains(&b'p'));
                files.push(FileEntry {
                    path,
                    length,
                    offset,
                    pad,
                });
                offset = offset
                    .checked_add(length)
                    .ok_or(invalid("total too large"))?;
            }
        }

        let expected = offset.div_ceil(u64::from(piece_length));
        if expected != pieces.len() as u64 || pieces.len() > u32::MAX as usize {
            return Err(invalid("piece count does not match total length"));
        }

        Ok(Self {
            info_hash,
            name,
            piece_length,
            pieces,
            files,
            total_length: offset,
            single_file,
        })
    }

    pub fn piece_count(&self) -> u32 {
        self.pieces.len() as u32
    }

    /// Exact size of piece `index` (the last piece may be shorter).
    pub fn piece_size(&self, index: u32) -> u64 {
        let start = u64::from(index) * u64::from(self.piece_length);
        (self.total_length - start).min(u64::from(self.piece_length))
    }

    /// Path of a file relative to the download directory.
    pub fn file_path(&self, file: &FileEntry) -> PathBuf {
        if self.single_file {
            file.path.clone()
        } else {
            Path::new(&self.name).join(&file.path)
        }
    }

    /// Splits piece `index` into `(file index, offset in file, offset in
    /// piece, length)` segments.
    pub fn piece_segments(&self, index: u32) -> Vec<(usize, u64, usize, usize)> {
        let start = u64::from(index) * u64::from(self.piece_length);
        let end = start + self.piece_size(index);
        let first = self.files.partition_point(|f| f.offset + f.length <= start);
        self.files[first..]
            .iter()
            .enumerate()
            .take_while(|(_, f)| f.offset < end)
            .filter(|(_, f)| f.length > 0)
            .map(|(i, f)| {
                let s = start.max(f.offset);
                let e = end.min(f.offset + f.length);
                (
                    first + i,
                    s - f.offset,
                    (s - start) as usize,
                    (e - s) as usize,
                )
            })
            .collect()
    }

    /// Pieces that touch file `file_index`.
    pub fn pieces_of_file(&self, file_index: usize) -> std::ops::Range<u32> {
        let f = &self.files[file_index];
        if f.length == 0 {
            return 0..0;
        }
        let pl = u64::from(self.piece_length);
        (f.offset / pl) as u32..(f.offset + f.length).div_ceil(pl) as u32
    }
}

fn non_negative(v: Option<&Value>) -> Option<u64> {
    v.and_then(Value::as_int)
        .and_then(|i| u64::try_from(i).ok())
}

fn utf8_field(v: &Value, key: &str) -> Option<String> {
    v.get(&format!("{key}.utf-8"))
        .or_else(|| v.get(key))
        .and_then(Value::as_bytes)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

/// Turns one untrusted path component into a safe single file name on both
/// Windows and Unix: no separators, no `.`/`..`, no drive prefixes, no
/// reserved device names, no characters Windows forbids.
pub fn sanitize_component(raw: &str) -> String {
    let mut s: String = raw
        .chars()
        .map(|c| match c {
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' => '_',
            c if c.is_control() => '_',
            c => c,
        })
        .collect();
    while s.ends_with(['.', ' ']) {
        s.pop();
    }
    let s = s.trim_start().to_string();
    if s.is_empty() {
        return "_".into();
    }
    let stem = s
        .split('.')
        .next()
        .unwrap_or("")
        .trim_end()
        .to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && stem.as_bytes()[3].is_ascii_digit());
    let s = if reserved { format!("_{s}") } else { s };
    debug_assert!(
        matches!(
            Path::new(&s).components().collect::<Vec<_>>()[..],
            [Component::Normal(_)]
        ),
        "{s:?}"
    );
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::make_torrent;

    #[test]
    fn sanitizes_hostile_names() {
        assert_eq!(sanitize_component(".."), "_");
        assert_eq!(sanitize_component("."), "_");
        assert_eq!(sanitize_component(""), "_");
        assert_eq!(sanitize_component("../../etc/passwd"), ".._.._etc_passwd");
        assert_eq!(sanitize_component("C:\\Windows"), "C__Windows");
        assert_eq!(sanitize_component("con.txt"), "_con.txt");
        assert_eq!(sanitize_component("COM1"), "_COM1");
        assert_eq!(sanitize_component("COMX"), "COMX");
        assert_eq!(sanitize_component("a\0b\nc"), "a_b_c");
        assert_eq!(sanitize_component("name. . "), "name");
        assert_eq!(sanitize_component("ok name.mkv"), "ok name.mkv");
    }

    #[test]
    fn parses_multi_file_layout() {
        let files: &[(&str, &[u8])] = &[
            ("a/one.bin", &[1; 10]),
            ("../evil", &[2; 0]),
            ("b.bin", &[3; 25]),
        ];
        let (bytes, _) = make_torrent("My: Torrent", files, 16);
        let m = Metainfo::parse(&bytes).unwrap();
        assert_eq!(m.name, "My_ Torrent");
        assert!(!m.single_file);
        assert_eq!(m.total_length, 35);
        assert_eq!(m.piece_count(), 3);
        assert_eq!(m.piece_size(2), 3);
        assert_eq!(m.files[0].path, Path::new("a").join("one.bin"));
        assert_eq!(m.files[1].path, Path::new("_").join("evil"));
        assert_eq!(m.files[2].offset, 10);
        assert_eq!(
            m.file_path(&m.files[2]),
            Path::new("My_ Torrent").join("b.bin")
        );
        assert_eq!(m.piece_segments(0), vec![(0, 0, 0, 10), (2, 0, 10, 6)]);
        assert_eq!(m.piece_segments(1), vec![(2, 6, 0, 16)]);
        assert_eq!(m.piece_segments(2), vec![(2, 22, 0, 3)]);
        assert_eq!(m.pieces_of_file(0), 0..1);
        assert_eq!(m.pieces_of_file(1), 0..0);
        assert_eq!(m.pieces_of_file(2), 0..3);
    }

    #[test]
    fn single_file_and_errors() {
        let (bytes, _) = make_torrent("x.iso", &[("x.iso", &[7; 40])], 16);
        let m = Metainfo::parse(&bytes).unwrap();
        assert!(m.single_file);
        assert_eq!(m.file_path(&m.files[0]), Path::new("x.iso"));
        assert!(Metainfo::parse(b"d4:infod4:name1:aee").is_err());
        assert!(Metainfo::parse(b"garbage").is_err());
    }
}
