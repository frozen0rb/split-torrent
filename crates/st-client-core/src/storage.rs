//! Writes verified pieces into pre-allocated files and keeps the resume
//! bitfield in a `.st-state` file next to the download.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use sha1::{Digest, Sha1};
use st_protocol::Bitfield;

use crate::metainfo::{InfoHash, Metainfo};

const STATE_MAGIC: &[u8; 8] = b"STSTATE1";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("piece {0} out of range")]
    OutOfRange(u32),
    #[error("piece {index} has {got} bytes, expected {want}")]
    WrongSize { index: u32, got: usize, want: u64 },
    #[error("piece {0} failed SHA-1 verification")]
    HashMismatch(u32),
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

fn io_err(path: &Path) -> impl FnOnce(io::Error) -> StorageError + '_ {
    move |source| StorageError::Io {
        path: path.to_owned(),
        source,
    }
}

/// Path of the state file for a torrent downloaded into `download_dir`.
pub fn state_path(download_dir: &Path, meta: &Metainfo) -> PathBuf {
    download_dir.join(format!("{}.st-state", meta.name))
}

pub struct Storage {
    meta: Arc<Metainfo>,
    download_dir: PathBuf,
    files: Vec<Option<File>>,
    dirty: Vec<bool>,
    have: Bitfield,
    state_dirty: bool,
}

impl Storage {
    /// Creates the directory tree, pre-allocates every file and loads the
    /// bitfield from `.st-state`. Pieces that touch a file which had to be
    /// created or resized are dropped from the bitfield.
    pub fn open(meta: Arc<Metainfo>, download_dir: &Path) -> Result<Self, StorageError> {
        let mut have = load_state(&state_path(download_dir, &meta), &meta)
            .unwrap_or_else(|| Bitfield::new(meta.piece_count()));
        let mut files = Vec::with_capacity(meta.files.len());
        for (i, f) in meta.files.iter().enumerate() {
            if f.pad {
                files.push(None);
                continue;
            }
            let path = download_dir.join(meta.file_path(f));
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(io_err(parent))?;
            }
            let existing = fs::metadata(&path)
                .ok()
                .filter(|m| m.is_file())
                .map(|m| m.len());
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
                .map_err(io_err(&path))?;
            if existing != Some(f.length) {
                file.set_len(f.length).map_err(io_err(&path))?;
                for p in meta.pieces_of_file(i) {
                    have.set(p, false);
                }
            }
            files.push(Some(file));
        }
        let mut s = Self {
            dirty: vec![false; files.len()],
            meta,
            download_dir: download_dir.to_owned(),
            files,
            have,
            state_dirty: true,
        };
        s.flush()?;
        Ok(s)
    }

    pub fn meta(&self) -> &Arc<Metainfo> {
        &self.meta
    }

    pub fn info_hash(&self) -> InfoHash {
        self.meta.info_hash
    }

    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn bytes_have(&self) -> u64 {
        self.have.iter_ones().map(|i| self.meta.piece_size(i)).sum()
    }

    /// Verifies and writes one piece. The data reaches the OS but is not yet
    /// durable; call [`Storage::flush`] before acknowledging it.
    /// Returns `false` if the piece was already present.
    pub fn write_piece(&mut self, index: u32, data: &[u8]) -> Result<bool, StorageError> {
        if index >= self.meta.piece_count() {
            return Err(StorageError::OutOfRange(index));
        }
        if self.have.get(index) {
            return Ok(false);
        }
        let want = self.meta.piece_size(index);
        if data.len() as u64 != want {
            return Err(StorageError::WrongSize {
                index,
                got: data.len(),
                want,
            });
        }
        if <[u8; 20]>::from(Sha1::digest(data)) != self.meta.pieces[index as usize] {
            return Err(StorageError::HashMismatch(index));
        }
        for (fi, file_off, piece_off, len) in self.meta.piece_segments(index) {
            let Some(file) = self.files[fi].as_mut() else {
                continue;
            };
            let path = || {
                self.download_dir
                    .join(self.meta.file_path(&self.meta.files[fi]))
            };
            file.seek(SeekFrom::Start(file_off))
                .and_then(|_| file.write_all(&data[piece_off..piece_off + len]))
                .map_err(|e| io_err(&path())(e))?;
            self.dirty[fi] = true;
        }
        self.have.set(index, true);
        self.state_dirty = true;
        Ok(true)
    }

    /// Reads piece `index` back from disk.
    pub fn read_piece(&mut self, index: u32) -> Result<Vec<u8>, StorageError> {
        if index >= self.meta.piece_count() {
            return Err(StorageError::OutOfRange(index));
        }
        let mut buf = vec![0u8; self.meta.piece_size(index) as usize];
        for (fi, file_off, piece_off, len) in self.meta.piece_segments(index) {
            let Some(file) = self.files[fi].as_mut() else {
                continue;
            };
            let path = self
                .download_dir
                .join(self.meta.file_path(&self.meta.files[fi]));
            file.seek(SeekFrom::Start(file_off))
                .and_then(|_| file.read_exact(&mut buf[piece_off..piece_off + len]))
                .map_err(io_err(&path))?;
        }
        Ok(buf)
    }

    /// Syncs written data, then atomically persists the bitfield.
    pub fn flush(&mut self) -> Result<(), StorageError> {
        for (i, dirty) in self.dirty.iter_mut().enumerate() {
            if *dirty {
                if let Some(f) = &self.files[i] {
                    let path = self
                        .download_dir
                        .join(self.meta.file_path(&self.meta.files[i]));
                    f.sync_data().map_err(io_err(&path))?;
                }
                *dirty = false;
            }
        }
        if self.state_dirty {
            let path = state_path(&self.download_dir, &self.meta);
            save_state(&path, &self.meta.info_hash, &self.have).map_err(io_err(&path))?;
            self.state_dirty = false;
        }
        Ok(())
    }

    /// Closes and deletes the downloaded files and the state file.
    pub fn delete_files(self) -> Result<(), StorageError> {
        let Self {
            meta,
            download_dir,
            files,
            ..
        } = self;
        drop(files);
        for f in meta.files.iter().filter(|f| !f.pad) {
            let path = download_dir.join(meta.file_path(f));
            match fs::remove_file(&path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(io_err(&path)(e)),
                _ => {}
            }
        }
        if !meta.single_file {
            remove_empty_dirs(&download_dir.join(&meta.name));
        }
        let state = state_path(&download_dir, &meta);
        let _ = fs::remove_file(state);
        Ok(())
    }
}

fn remove_empty_dirs(dir: &Path) {
    if let Ok(entries) = fs::read_dir(dir) {
        for e in entries.flatten() {
            if e.file_type().is_ok_and(|t| t.is_dir()) {
                remove_empty_dirs(&e.path());
            }
        }
    }
    let _ = fs::remove_dir(dir);
}

fn load_state(path: &Path, meta: &Metainfo) -> Option<Bitfield> {
    let buf = fs::read(path).ok()?;
    let rest = buf.strip_prefix(STATE_MAGIC.as_slice())?;
    let (hash, rest) = rest.split_at_checked(20)?;
    let (count, bits) = rest.split_at_checked(4)?;
    let count = u32::from_le_bytes(count.try_into().ok()?);
    if hash != meta.info_hash || count != meta.piece_count() {
        return None;
    }
    Bitfield::from_bytes(bits.to_vec(), count)
}

fn save_state(path: &Path, info_hash: &InfoHash, have: &Bitfield) -> io::Result<()> {
    let mut buf = Vec::with_capacity(32 + have.as_bytes().len());
    buf.extend_from_slice(STATE_MAGIC);
    buf.extend_from_slice(info_hash);
    buf.extend_from_slice(&have.len().to_le_bytes());
    buf.extend_from_slice(have.as_bytes());
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::{make_torrent, pseudo_random};

    fn setup(piece_len: u32) -> (Arc<Metainfo>, Vec<Vec<u8>>, Vec<Vec<u8>>) {
        let a = pseudo_random(10, 1);
        let b = pseudo_random(0, 2);
        let c = pseudo_random(45, 3);
        let files: &[(&str, &[u8])] = &[("dir/a.bin", &a), ("empty", &b), ("c.bin", &c)];
        let (bytes, pieces) = make_torrent("multi", files, piece_len);
        let meta = Arc::new(Metainfo::parse(&bytes).unwrap());
        (meta, pieces, vec![a, b, c])
    }

    #[test]
    fn writes_pieces_across_files_in_any_order() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, data) = setup(16);
        let mut s = Storage::open(meta.clone(), tmp.path()).unwrap();
        let root = tmp.path().join("multi");
        assert_eq!(fs::metadata(root.join("dir/a.bin")).unwrap().len(), 10);
        assert_eq!(fs::metadata(root.join("empty")).unwrap().len(), 0);
        assert_eq!(fs::metadata(root.join("c.bin")).unwrap().len(), 45);

        for i in [3, 0, 2, 1] {
            assert!(s.write_piece(i, &pieces[i as usize]).unwrap());
        }
        assert!(!s.write_piece(1, &pieces[1]).unwrap());
        s.flush().unwrap();
        assert!(s.have().is_full());
        assert_eq!(s.bytes_have(), 55);
        assert_eq!(fs::read(root.join("dir/a.bin")).unwrap(), data[0]);
        assert_eq!(fs::read(root.join("c.bin")).unwrap(), data[2]);
        assert_eq!(s.read_piece(0).unwrap(), pieces[0]);
    }

    #[test]
    fn rejects_bad_pieces() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        let mut s = Storage::open(meta, tmp.path()).unwrap();
        let mut bad = pieces[0].clone();
        bad[0] ^= 1;
        assert!(matches!(
            s.write_piece(0, &bad),
            Err(StorageError::HashMismatch(0))
        ));
        assert!(matches!(
            s.write_piece(0, &bad[1..]),
            Err(StorageError::WrongSize { .. })
        ));
        assert!(matches!(
            s.write_piece(9, &bad),
            Err(StorageError::OutOfRange(9))
        ));
        assert_eq!(s.have().count_ones(), 0);
    }

    #[test]
    fn state_persists_and_resumes() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        {
            let mut s = Storage::open(meta.clone(), tmp.path()).unwrap();
            s.write_piece(1, &pieces[1]).unwrap();
            s.write_piece(3, &pieces[3]).unwrap();
            s.flush().unwrap();
        }
        assert!(state_path(tmp.path(), &meta).exists());
        let s = Storage::open(meta.clone(), tmp.path()).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);

        // Unflushed state is not persisted.
        {
            let mut s = Storage::open(meta.clone(), tmp.path()).unwrap();
            s.write_piece(0, &pieces[0]).unwrap();
            std::mem::forget(s);
        }
        let s = Storage::open(meta.clone(), tmp.path()).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);

        // A file that was truncated invalidates the pieces touching it.
        fs::write(tmp.path().join("multi/dir/a.bin"), b"short").unwrap();
        let s = Storage::open(meta.clone(), tmp.path()).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);
        fs::remove_file(tmp.path().join("multi/c.bin")).unwrap();
        let s = Storage::open(meta.clone(), tmp.path()).unwrap();
        assert_eq!(s.have().count_ones(), 0);

        // A state file for another torrent is ignored.
        let (other, _) = make_torrent("multi", &[("x", &[1; 5])], 16);
        let other = Arc::new(Metainfo::parse(&other).unwrap());
        assert!(load_state(&state_path(tmp.path(), &other), &other).is_none());

        s.delete_files().unwrap();
        assert!(!tmp.path().join("multi").exists());
        assert!(!state_path(tmp.path(), &meta).exists());
    }

    #[test]
    fn single_file_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let data = pseudo_random(100, 9);
        let (bytes, pieces) = make_torrent("file.iso", &[("file.iso", &data)], 32);
        let meta = Arc::new(Metainfo::parse(&bytes).unwrap());
        let mut s = Storage::open(meta, tmp.path()).unwrap();
        for (i, p) in pieces.iter().enumerate().rev() {
            s.write_piece(i as u32, p).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(fs::read(tmp.path().join("file.iso")).unwrap(), data);
        assert!(tmp.path().join("file.iso.st-state").exists());
    }
}
