//! Writes verified pieces into pre-allocated files and keeps the resume
//! bitfield and the file selection in a `.st-state` file in the client's
//! state folder (see [`state_path`]). Older versions kept it next to the
//! download as `<name>.st-state`; such files are migrated on open.
//!
//! Only selected files are created and written. A piece that also covers an
//! unselected file is still verified as a whole, and counts as present once
//! its selected parts are on disk. When such a file is selected later, those
//! shared pieces are dropped from the bitfield so they are fetched again and
//! the newly selected part gets written too. Deselecting a file keeps what
//! is already on disk.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use sha1::{Digest, Sha1};
use st_protocol::Bitfield;

use crate::metainfo::{hex, InfoHash, Metainfo};
use crate::persist::{self, FileOps, RealFs, Replaced, SavePolicy};

const STATE_MAGIC_V1: &[u8; 8] = b"STSTATE1";
const STATE_MAGIC: &[u8; 8] = b"STSTATE2";

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("piece {0} out of range")]
    OutOfRange(u32),
    #[error("piece {index} has {got} bytes, expected {want}")]
    WrongSize { index: u32, got: usize, want: u64 },
    #[error("piece {0} failed SHA-1 verification")]
    HashMismatch(u32),
    #[error("file selection has {got} entries, torrent has {want} files")]
    BadSelection { got: usize, want: usize },
    #[error("{op}: {}: {source}", path.display())]
    Io {
        op: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

fn io_err<'a>(op: &'static str, path: &'a Path) -> impl FnOnce(io::Error) -> StorageError + 'a {
    move |source| StorageError::Io {
        op,
        path: path.to_owned(),
        source,
    }
}

/// Where and how resume state is saved.
#[derive(Debug, Clone)]
pub struct StateConfig {
    pub state_dir: PathBuf,
    pub ops: Arc<dyn FileOps>,
    pub policy: SavePolicy,
}

impl StateConfig {
    pub fn new(state_dir: impl Into<PathBuf>) -> Self {
        Self {
            state_dir: state_dir.into(),
            ops: Arc::new(RealFs),
            policy: SavePolicy::default(),
        }
    }
}

/// The per-user folder for resume state: `%APPDATA%\\split-torrent\\data\\state`
/// on Windows, `~/Library/Application Support/split-torrent/state` on macOS,
/// `$XDG_DATA_HOME/split-torrent/state` (default `~/.local/share/…`)
/// elsewhere. Matches the GUI's settings folder.
pub fn default_state_dir() -> PathBuf {
    let env = |k| {
        std::env::var_os(k)
            .map(PathBuf::from)
            .filter(|p| p.is_absolute())
    };
    let home = env("HOME").or_else(|| env("USERPROFILE"));
    let base = if cfg!(windows) {
        env("APPDATA").map(|p| p.join("split-torrent").join("data"))
    } else if cfg!(target_os = "macos") {
        home.map(|h| h.join("Library/Application Support/split-torrent"))
    } else {
        env("XDG_DATA_HOME")
            .or_else(|| home.map(|h| h.join(".local/share")))
            .map(|p| p.join("split-torrent"))
    };
    base.unwrap_or_else(|| std::env::temp_dir().join("split-torrent"))
        .join("state")
}

/// `<state_dir>/<info-hash hex>-<download dir key>.st-state`. The key (8 hex
/// digits of the SHA-1 of the absolute download folder, case-folded on
/// Windows) keeps state for the same torrent in different folders apart.
pub fn state_path(state_dir: &Path, download_dir: &Path, meta: &Metainfo) -> PathBuf {
    let dir = std::path::absolute(download_dir).unwrap_or_else(|_| download_dir.to_owned());
    let mut dir = dir.to_string_lossy().into_owned();
    while dir.len() > 1 && dir.ends_with(['/', '\\']) {
        dir.pop();
    }
    if cfg!(windows) {
        dir = dir.to_lowercase();
    }
    let key = Sha1::digest(dir.as_bytes());
    state_dir.join(format!(
        "{}-{}.st-state",
        hex(&meta.info_hash),
        hex(&key[..4])
    ))
}

/// Where versions before 0.2 kept the state: next to the download.
pub fn legacy_state_path(download_dir: &Path, meta: &Metainfo) -> PathBuf {
    download_dir.join(format!("{}.st-state", meta.name))
}

/// Download progress relative to the file selection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Progress {
    /// Pieces overlapping a selected file.
    pub pieces_wanted: u32,
    /// Of those, pieces that are on disk.
    pub pieces_have: u32,
    /// Total length of the selected files.
    pub bytes_wanted: u64,
    /// Bytes of the selected files that are on disk.
    pub bytes_have: u64,
    /// Per file: bytes on disk. Always 0 for files that are not selected,
    /// even if data from an earlier selection is still there.
    pub file_bytes_have: Vec<u64>,
}

pub struct Storage {
    meta: Arc<Metainfo>,
    download_dir: PathBuf,
    files: Vec<Option<File>>,
    dirty: Vec<bool>,
    selected: Vec<bool>,
    wanted: Bitfield,
    have: Bitfield,
    progress: Progress,
    state_dirty: bool,
    state: StateConfig,
    state_path: PathBuf,
    /// A valid legacy state file to delete once the new one is saved.
    legacy_state: Option<PathBuf>,
    save: SaveStatus,
}

/// Failures to save the state since the last successful save.
#[derive(Debug, Default)]
struct SaveStatus {
    failures: u32,
    unsaved_since: Option<Instant>,
    next_try: Option<Instant>,
    problem: Option<String>,
    notice: Option<String>,
}

impl Storage {
    /// Creates the directories and pre-allocates the selected files, and
    /// loads the bitfield from `.st-state`. `selected` has one entry per
    /// torrent file; `None` keeps the selection saved in `.st-state` (or
    /// selects everything). Padding files are never selected.
    ///
    /// Failing to save the state is not an error here or in
    /// [`Storage::flush`]: it is retried later, see [`Storage::state_saved`].
    pub fn open(
        meta: Arc<Metainfo>,
        download_dir: &Path,
        state: &StateConfig,
        selected: Option<&[bool]>,
    ) -> Result<Self, StorageError> {
        let n = meta.files.len();
        let state_path = state_path(&state.state_dir, download_dir, &meta);
        let legacy = legacy_state_path(download_dir, &meta);
        let legacy_loaded = load_state(&legacy, &meta);
        let legacy_state = legacy_loaded.is_some().then_some(legacy);
        let (have, saved) = load_state(&state_path, &meta)
            .or(legacy_loaded)
            .unwrap_or_else(|| (Bitfield::new(meta.piece_count()), vec![true; n]));
        let want = match selected {
            Some(s) if s.len() != n => {
                return Err(StorageError::BadSelection {
                    got: s.len(),
                    want: n,
                })
            }
            Some(s) => s.to_vec(),
            None => saved.clone(),
        };
        let mut s = Self {
            files: (0..n).map(|_| None).collect(),
            dirty: vec![false; n],
            selected: vec![false; n],
            wanted: Bitfield::new(meta.piece_count()),
            have,
            progress: Progress::default(),
            state_dirty: true,
            state: state.clone(),
            state_path,
            legacy_state,
            save: SaveStatus::default(),
            download_dir: download_dir.to_owned(),
            meta,
        };
        // Files selected when the state was saved count as already open:
        // their parts of shared pieces are on disk.
        for (i, &was) in saved.iter().enumerate() {
            s.selected[i] = was && !s.meta.files[i].pad;
        }
        s.apply_selection(&want)?;
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

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Everything written so far is recorded in the state file.
    pub fn state_saved(&self) -> bool {
        !self.state_dirty
    }

    /// Why the state could not be saved, while it is unsaved.
    pub fn state_problem(&self) -> Option<&str> {
        self.save.problem.as_deref()
    }

    /// When saving first failed since the last successful save.
    pub fn unsaved_since(&self) -> Option<Instant> {
        self.save.unsaved_since
    }

    /// A one-off note about a save that worked only via the fallback.
    pub fn take_notice(&mut self) -> Option<String> {
        self.save.notice.take()
    }

    pub fn have(&self) -> &Bitfield {
        &self.have
    }

    pub fn selected(&self) -> &[bool] {
        &self.selected
    }

    /// Indices of the selected files, as sent to the server.
    pub fn selected_indices(&self) -> Vec<u32> {
        self.selected
            .iter()
            .enumerate()
            .filter(|(_, &s)| s)
            .map(|(i, _)| i as u32)
            .collect()
    }

    pub fn progress(&self) -> &Progress {
        &self.progress
    }

    /// Every selected piece is on disk.
    pub fn complete(&self) -> bool {
        self.progress.pieces_have == self.progress.pieces_wanted
    }

    /// Changes the file selection. Newly selected files are created (or
    /// checked) and their pieces shared with other files are dropped from
    /// the bitfield; deselected files are closed but kept on disk. Flushes.
    pub fn set_selection(&mut self, selected: &[bool]) -> Result<(), StorageError> {
        if selected.len() != self.meta.files.len() {
            return Err(StorageError::BadSelection {
                got: selected.len(),
                want: self.meta.files.len(),
            });
        }
        self.flush()?;
        self.apply_selection(selected)?;
        Ok(())
    }

    fn apply_selection(&mut self, selected: &[bool]) -> Result<(), StorageError> {
        for (i, &sel) in selected.iter().enumerate() {
            let want = sel && !self.meta.files[i].pad;
            let was_selected = self.selected[i];
            if want && self.files[i].is_none() {
                self.open_file(i)?;
                if !was_selected {
                    self.drop_shared_pieces(i);
                }
            } else if !want {
                self.files[i] = None;
                self.dirty[i] = false;
            }
            self.selected[i] = want;
        }
        self.wanted = Bitfield::new(self.meta.piece_count());
        for i in (0..self.meta.files.len()).filter(|&i| self.selected[i]) {
            for p in self.meta.pieces_of_file(i) {
                self.wanted.set(p, true);
            }
        }
        self.recompute_progress();
        self.state_dirty = true;
        self.flush()
    }

    /// Opens (creating if needed) file `i` at its full length. If it had to
    /// be created or resized, the pieces touching it are dropped.
    fn open_file(&mut self, i: usize) -> Result<(), StorageError> {
        let f = &self.meta.files[i];
        let path = self.download_dir.join(self.meta.file_path(f));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(io_err("creating folder", parent))?;
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
            .map_err(io_err("opening file", &path))?;
        if existing != Some(f.length) {
            file.set_len(f.length)
                .map_err(io_err("allocating file", &path))?;
            for p in self.meta.pieces_of_file(i) {
                self.have.set(p, false);
            }
        }
        self.files[i] = Some(file);
        Ok(())
    }

    /// Drops present pieces of file `i` that also cover another (non-pad)
    /// file: they were written while `i` was not selected, so `i`'s part of
    /// them may be missing.
    fn drop_shared_pieces(&mut self, i: usize) {
        for p in self.meta.pieces_of_file(i) {
            if self.have.get(p)
                && self
                    .meta
                    .piece_segments(p)
                    .iter()
                    .any(|&(fi, ..)| fi != i && !self.meta.files[fi].pad)
            {
                self.have.set(p, false);
            }
        }
    }

    fn recompute_progress(&mut self) {
        let meta = &self.meta;
        let mut file_bytes_have = vec![0u64; meta.files.len()];
        for p in self.have.iter_ones() {
            for (fi, _, _, len) in meta.piece_segments(p) {
                if self.selected[fi] {
                    file_bytes_have[fi] += len as u64;
                }
            }
        }
        let sel = |i: &usize| self.selected[*i];
        self.progress = Progress {
            pieces_wanted: self.wanted.count_ones(),
            pieces_have: self
                .wanted
                .iter_ones()
                .filter(|&p| self.have.get(p))
                .count() as u32,
            bytes_wanted: (0..meta.files.len())
                .filter(sel)
                .map(|i| meta.files[i].length)
                .sum(),
            bytes_have: (0..meta.files.len())
                .filter(sel)
                .map(|i| file_bytes_have[i])
                .sum(),
            file_bytes_have,
        };
    }

    /// Verifies and writes the selected parts of one piece. The data reaches
    /// the OS but is not yet durable; call [`Storage::flush`] before
    /// acknowledging it. Returns `false` if nothing was written: the piece
    /// was already present or covers no selected file.
    pub fn write_piece(&mut self, index: u32, data: &[u8]) -> Result<bool, StorageError> {
        if index >= self.meta.piece_count() {
            return Err(StorageError::OutOfRange(index));
        }
        if self.have.get(index) || !self.wanted.get(index) {
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
        let segments = self.meta.piece_segments(index);
        for &(fi, file_off, piece_off, len) in &segments {
            let Some(file) = self.files[fi].as_mut() else {
                continue;
            };
            let path = || {
                self.download_dir
                    .join(self.meta.file_path(&self.meta.files[fi]))
            };
            file.seek(SeekFrom::Start(file_off))
                .and_then(|_| file.write_all(&data[piece_off..piece_off + len]))
                .map_err(|e| io_err("writing piece", &path())(e))?;
            self.dirty[fi] = true;
        }
        self.have.set(index, true);
        self.progress.pieces_have += 1;
        for (fi, _, _, len) in segments {
            if self.selected[fi] {
                self.progress.file_bytes_have[fi] += len as u64;
                self.progress.bytes_have += len as u64;
            }
        }
        self.state_dirty = true;
        Ok(true)
    }

    /// Reads the selected parts of piece `index` back from disk; other parts
    /// are zero.
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
                .map_err(io_err("reading piece", &path))?;
        }
        Ok(buf)
    }

    /// Syncs written data, then persists the bitfield and the selection.
    /// Only syncing errors are returned; a failed state save is recorded and
    /// retried on a later call (with backoff), see [`Storage::state_saved`].
    pub fn flush(&mut self) -> Result<(), StorageError> {
        for (i, dirty) in self.dirty.iter_mut().enumerate() {
            if *dirty {
                if let Some(f) = &self.files[i] {
                    let path = self
                        .download_dir
                        .join(self.meta.file_path(&self.meta.files[i]));
                    f.sync_data()
                        .map_err(io_err("saving downloaded data (sync)", &path))?;
                }
                *dirty = false;
            }
        }
        if self.state_dirty && self.save.next_try.is_none_or(|t| Instant::now() >= t) {
            self.save_state();
        }
        Ok(())
    }

    fn save_state(&mut self) {
        let data = encode_state(&self.meta.info_hash, &self.have, &self.selected);
        let res = fs::create_dir_all(&self.state.state_dir)
            .map_err(|e| {
                format!(
                    "saving progress (create state folder): {}: {e}",
                    self.state.state_dir.display()
                )
            })
            .and_then(|()| {
                persist::replace_file(
                    &*self.state.ops,
                    &self.state.policy,
                    &self.state_path,
                    &data,
                    "saving progress",
                )
                .map_err(|e| e.to_string())
            });
        match res {
            Ok(r) => {
                if let Replaced::Overwritten { rename_error } = r {
                    self.save.notice = Some(format!(
                        "saved progress by overwriting {} in place (replacing it failed: {rename_error})",
                        self.state_path.display()
                    ));
                }
                self.state_dirty = false;
                self.save = SaveStatus {
                    notice: self.save.notice.take(),
                    ..SaveStatus::default()
                };
                if let Some(legacy) = self.legacy_state.take() {
                    let _ = fs::remove_file(&legacy);
                    let mut tmp = legacy.into_os_string();
                    tmp.push(".tmp");
                    let _ = fs::remove_file(tmp);
                }
            }
            Err(problem) => {
                let now = Instant::now();
                self.save.failures += 1;
                self.save.unsaved_since.get_or_insert(now);
                self.save.next_try = Some(now + self.state.policy.retry_after(self.save.failures));
                self.save.problem = Some(problem);
            }
        }
    }

    /// Closes and deletes the downloaded files (selected or not) and the
    /// state file.
    pub fn delete_files(self) -> Result<(), StorageError> {
        let Self {
            meta,
            download_dir,
            files,
            state_path,
            legacy_state,
            ..
        } = self;
        drop(files);
        for f in meta.files.iter().filter(|f| !f.pad) {
            let path = download_dir.join(meta.file_path(f));
            match fs::remove_file(&path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => {
                    return Err(io_err("deleting file", &path)(e))
                }
                _ => {}
            }
        }
        if !meta.single_file {
            remove_empty_dirs(&download_dir.join(&meta.name));
        }
        for p in std::iter::once(state_path).chain(legacy_state) {
            let _ = fs::remove_file(&p);
        }
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

/// Reads `MAGIC, info hash, piece count (u32 LE), have bits` and, in v2,
/// `file count (u32 LE), selection bits`. v1 files mean "every file".
fn load_state(path: &Path, meta: &Metainfo) -> Option<(Bitfield, Vec<bool>)> {
    let buf = fs::read(path).ok()?;
    let (v2, rest) = if let Some(r) = buf.strip_prefix(STATE_MAGIC.as_slice()) {
        (true, r)
    } else {
        (false, buf.strip_prefix(STATE_MAGIC_V1.as_slice())?)
    };
    let (hash, rest) = rest.split_at_checked(20)?;
    let (count, rest) = rest.split_at_checked(4)?;
    let count = u32::from_le_bytes(count.try_into().ok()?);
    if hash != meta.info_hash || count != meta.piece_count() {
        return None;
    }
    let (bits, rest) = rest.split_at_checked(count.div_ceil(8) as usize)?;
    let have = Bitfield::from_bytes(bits.to_vec(), count)?;
    let n = meta.files.len();
    if !v2 {
        return rest.is_empty().then(|| (have, vec![true; n]));
    }
    let (fc, rest) = rest.split_at_checked(4)?;
    let fc = u32::from_le_bytes(fc.try_into().ok()?);
    if fc as usize != n {
        return None;
    }
    let sel = Bitfield::from_bytes(rest.to_vec(), fc)?;
    Some((have, (0..fc).map(|i| sel.get(i)).collect()))
}

fn encode_state(info_hash: &InfoHash, have: &Bitfield, selected: &[bool]) -> Vec<u8> {
    let mut sel = Bitfield::new(selected.len() as u32);
    for (i, _) in selected.iter().enumerate().filter(|(_, &s)| s) {
        sel.set(i as u32, true);
    }
    let mut buf = Vec::with_capacity(40 + have.as_bytes().len() + sel.as_bytes().len());
    buf.extend_from_slice(STATE_MAGIC);
    buf.extend_from_slice(info_hash);
    buf.extend_from_slice(&have.len().to_le_bytes());
    buf.extend_from_slice(have.as_bytes());
    buf.extend_from_slice(&sel.len().to_le_bytes());
    buf.extend_from_slice(sel.as_bytes());
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persist::tests::{fast_policy, DenyRename};
    use crate::testkit::{make_torrent, pseudo_random};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn st(tmp: &tempfile::TempDir) -> StateConfig {
        StateConfig::new(tmp.path().join("state"))
    }

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
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
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
        assert_eq!(s.progress().bytes_have, 55);
        assert_eq!(fs::read(root.join("dir/a.bin")).unwrap(), data[0]);
        assert_eq!(fs::read(root.join("c.bin")).unwrap(), data[2]);
        assert_eq!(s.read_piece(0).unwrap(), pieces[0]);
    }

    #[test]
    fn rejects_bad_pieces() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        let mut s = Storage::open(meta, tmp.path(), &st(&tmp), None).unwrap();
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
            let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
            s.write_piece(1, &pieces[1]).unwrap();
            s.write_piece(3, &pieces[3]).unwrap();
            s.flush().unwrap();
        }
        assert!(state_path(&st(&tmp).state_dir, tmp.path(), &meta).exists());
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);

        // Unflushed state is not persisted.
        {
            let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
            s.write_piece(0, &pieces[0]).unwrap();
            std::mem::forget(s);
        }
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);

        // A file that was truncated invalidates the pieces touching it.
        fs::write(tmp.path().join("multi/dir/a.bin"), b"short").unwrap();
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 3]);
        drop(s);
        fs::remove_file(tmp.path().join("multi/c.bin")).unwrap();
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().count_ones(), 0);

        // A state file for another torrent is ignored.
        let (other, _) = make_torrent("multi", &[("x", &[1; 5])], 16);
        let other = Arc::new(Metainfo::parse(&other).unwrap());
        assert!(load_state(&state_path(&st(&tmp).state_dir, tmp.path(), &other), &other).is_none());

        s.delete_files().unwrap();
        assert!(!tmp.path().join("multi").exists());
        assert!(!state_path(&st(&tmp).state_dir, tmp.path(), &meta).exists());
    }

    #[test]
    fn writes_only_selected_files() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, data) = setup(16);
        let root = tmp.path().join("multi");
        let mut s = Storage::open(
            meta.clone(),
            tmp.path(),
            &st(&tmp),
            Some(&[true, false, false]),
        )
        .unwrap();
        assert!(root.join("dir/a.bin").exists());
        assert!(!root.join("c.bin").exists());
        assert!(!root.join("empty").exists());
        assert_eq!(s.selected_indices(), vec![0]);
        let p = s.progress().clone();
        assert_eq!((p.pieces_wanted, p.bytes_wanted, p.bytes_have), (1, 10, 0));

        // Piece 0 covers a.bin and the start of c.bin; only a.bin is written.
        assert!(!s.write_piece(1, &pieces[1]).unwrap(), "not selected");
        assert!(s.write_piece(0, &pieces[0]).unwrap());
        s.flush().unwrap();
        assert!(s.complete());
        assert_eq!(s.progress().bytes_have, 10);
        assert_eq!(s.progress().file_bytes_have, vec![10, 0, 0]);
        assert_eq!(fs::read(root.join("dir/a.bin")).unwrap(), data[0]);
        assert!(!root.join("c.bin").exists());
        drop(s);

        // Reopening keeps the saved selection.
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.selected(), &[true, false, false]);
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![0]);

        // Selecting c.bin drops the shared piece 0: its c.bin part is missing.
        s.set_selection(&[true, true, true]).unwrap();
        assert!(s.have().count_ones() == 0);
        assert!(!s.complete());
        assert_eq!(s.progress().bytes_wanted, 55);
        for i in 0..4 {
            assert!(s.write_piece(i, &pieces[i as usize]).unwrap());
        }
        s.flush().unwrap();
        assert!(s.complete());
        assert_eq!(fs::read(root.join("dir/a.bin")).unwrap(), data[0]);
        assert_eq!(fs::read(root.join("c.bin")).unwrap(), data[2]);
    }

    #[test]
    fn deselecting_keeps_data() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, data) = setup(16);
        let root = tmp.path().join("multi");
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        for i in 0..4 {
            s.write_piece(i, &pieces[i as usize]).unwrap();
        }
        s.set_selection(&[false, false, true]).unwrap();
        assert!(s.complete());
        assert_eq!(s.progress().bytes_wanted, 45);
        assert_eq!(fs::read(root.join("dir/a.bin")).unwrap(), data[0]);
        drop(s);
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.selected(), &[false, false, true]);
        assert!(s.have().is_full());
        // a.bin was not selected when piece 0 was last confirmed: refetch it.
        s.set_selection(&[true, false, true]).unwrap();
        assert_eq!(s.have().iter_zeros().collect::<Vec<_>>(), vec![0]);
        assert!(matches!(
            s.set_selection(&[true]),
            Err(StorageError::BadSelection { got: 1, want: 3 })
        ));
    }

    #[test]
    fn reads_v1_state_as_all_selected() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        {
            let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
            s.write_piece(2, &pieces[2]).unwrap();
            s.flush().unwrap();
        }
        let mut have = Bitfield::new(4);
        have.set(2, true);
        let mut v1 = STATE_MAGIC_V1.to_vec();
        v1.extend_from_slice(&meta.info_hash);
        v1.extend_from_slice(&4u32.to_le_bytes());
        v1.extend_from_slice(have.as_bytes());
        fs::write(state_path(&st(&tmp).state_dir, tmp.path(), &meta), v1).unwrap();
        let s = Storage::open(meta, tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.selected(), &[true, true, true]);
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn single_file_layout() {
        let tmp = tempfile::tempdir().unwrap();
        let data = pseudo_random(100, 9);
        let (bytes, pieces) = make_torrent("file.iso", &[("file.iso", &data)], 32);
        let meta = Arc::new(Metainfo::parse(&bytes).unwrap());
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        for (i, p) in pieces.iter().enumerate().rev() {
            s.write_piece(i as u32, p).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(fs::read(tmp.path().join("file.iso")).unwrap(), data);
        assert!(!tmp.path().join("file.iso.st-state").exists());
        assert!(state_path(&st(&tmp).state_dir, tmp.path(), &meta).exists());
    }

    #[test]
    fn state_lives_in_state_dir_keyed_by_download_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, _, _) = setup(16);
        let sd = tmp.path().join("state");
        let a = state_path(&sd, &tmp.path().join("a"), &meta);
        assert_eq!(a.parent().unwrap(), sd);
        let name = a.file_name().unwrap().to_string_lossy().into_owned();
        assert!(
            name.starts_with(&crate::metainfo::hex(&meta.info_hash)),
            "{name}"
        );
        assert!(name.ends_with(".st-state"));
        assert_ne!(a, state_path(&sd, &tmp.path().join("b"), &meta));
        assert_eq!(a, state_path(&sd, &tmp.path().join("a/"), &meta));
    }

    #[test]
    fn migrates_legacy_state_next_to_download() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        let legacy = legacy_state_path(tmp.path(), &meta);
        {
            let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
            s.write_piece(1, &pieces[1]).unwrap();
            s.flush().unwrap();
            // Pretend an older version wrote it next to the download.
            fs::rename(s.state_path(), &legacy).unwrap();
        }
        fs::write(legacy.with_extension("st-state.tmp"), b"junk").unwrap();
        let mut s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1]);
        assert!(!legacy.exists(), "legacy state is removed after migration");
        assert!(!legacy.with_extension("st-state.tmp").exists());
        assert!(s.state_path().exists());
        s.write_piece(2, &pieces[2]).unwrap();
        s.flush().unwrap();
        drop(s);
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 2]);

        // A legacy file of a different torrent with the same name is left alone.
        drop(s);
        let (other, _) = make_torrent("multi", &[("x", &[1; 5])], 16);
        let other = Arc::new(Metainfo::parse(&other).unwrap());
        let mut o = Storage::open(other, tmp.path(), &st(&tmp), Some(&[false])).unwrap();
        o.flush().unwrap();
        fs::copy(o.state_path(), &legacy).unwrap();
        let s = Storage::open(meta.clone(), tmp.path(), &st(&tmp), None).unwrap();
        assert!(legacy.exists());
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![1, 2]);

        // Removing with files deletes the state in the state folder.
        let path = s.state_path().to_owned();
        s.delete_files().unwrap();
        assert!(!path.exists());
    }

    #[test]
    fn denied_rename_falls_back_to_overwrite() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        let ops = Arc::new(DenyRename::default());
        let cfg = StateConfig {
            ops: ops.clone(),
            policy: fast_policy(),
            ..st(&tmp)
        };
        let mut s = Storage::open(meta.clone(), tmp.path(), &cfg, None).unwrap();
        assert!(s.state_saved());
        assert!(s.take_notice().unwrap().contains("overwriting"));
        s.write_piece(0, &pieces[0]).unwrap();
        s.flush().unwrap();
        assert!(s.state_saved());
        assert!(s.state_problem().is_none());
        assert!(ops.renames.load(Ordering::SeqCst) >= 6);
        drop(s);
        let s = Storage::open(meta, tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn failed_saves_are_retried_not_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        let (meta, pieces, _) = setup(16);
        let ops = Arc::new(DenyRename {
            overwrite_failures: 3,
            ..Default::default()
        });
        let cfg = StateConfig {
            ops: ops.clone(),
            policy: SavePolicy {
                retry_min: Duration::from_millis(50),
                retry_max: Duration::from_millis(50),
                ..fast_policy()
            },
            ..st(&tmp)
        };
        // Opening succeeds even though the first save fails.
        let mut s = Storage::open(meta.clone(), tmp.path(), &cfg, None).unwrap();
        assert!(!s.state_saved());
        let problem = s.state_problem().unwrap().to_owned();
        assert!(
            problem.starts_with("saving progress (replace "),
            "{problem}"
        );
        assert!(problem.contains(".st-state"), "{problem}");
        let since = s.unsaved_since().unwrap();

        s.write_piece(0, &pieces[0]).unwrap();
        // Within the backoff no new attempt is made.
        s.flush().unwrap();
        assert_eq!(ops.overwrites.load(Ordering::SeqCst), 1);
        assert!(!s.state_saved());
        for _ in 0..2 {
            std::thread::sleep(Duration::from_millis(60));
            s.flush().unwrap();
        }
        assert_eq!(ops.overwrites.load(Ordering::SeqCst), 3);
        assert!(!s.state_saved());
        assert_eq!(s.unsaved_since(), Some(since));
        std::thread::sleep(Duration::from_millis(60));
        s.flush().unwrap();
        assert!(s.state_saved());
        assert!(s.state_problem().is_none() && s.unsaved_since().is_none());
        drop(s);
        let s = Storage::open(meta, tmp.path(), &st(&tmp), None).unwrap();
        assert_eq!(s.have().iter_ones().collect::<Vec<_>>(), vec![0]);
    }
}
