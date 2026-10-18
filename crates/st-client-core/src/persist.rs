//! Replacing small files (the resume state) robustly, including on network
//! shares where a rename over an existing file can be refused while another
//! process (indexer, antivirus, the NAS itself) briefly holds the target.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// File operations used to persist state; replaceable in tests.
pub trait FileOps: fmt::Debug + Send + Sync {
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    /// Truncates `path` (creating it if needed) and writes `data` into it.
    fn overwrite(&self, path: &Path, data: &[u8]) -> io::Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        f.write_all(data)?;
        f.sync_all()
    }
}

#[derive(Debug, Default)]
pub struct RealFs;

impl FileOps for RealFs {}

/// Timings for saving resume state.
#[derive(Debug, Clone)]
pub struct SavePolicy {
    /// Delays before each rename attempt; after the last one fails the
    /// target is overwritten in place.
    pub rename_delays: Vec<Duration>,
    /// After a save failed completely, the next attempt waits this long,
    /// doubling per failure up to `retry_max`.
    pub retry_min: Duration,
    pub retry_max: Duration,
    /// How long the client holds `Ack`s while progress cannot be saved.
    /// After that it acknowledges anyway (the data is synced to disk), so
    /// the download keeps going; such pieces may be fetched again after a
    /// restart.
    pub ack_unsaved_after: Duration,
}

impl Default for SavePolicy {
    fn default() -> Self {
        let ms = Duration::from_millis;
        Self {
            rename_delays: vec![ms(0), ms(50), ms(100), ms(200), ms(400), ms(800)],
            retry_min: Duration::from_secs(2),
            retry_max: Duration::from_secs(30),
            ack_unsaved_after: Duration::from_secs(30),
        }
    }
}

impl SavePolicy {
    pub fn retry_after(&self, failures: u32) -> Duration {
        let factor = 1u32 << failures.saturating_sub(1).min(16);
        self.retry_min.saturating_mul(factor).min(self.retry_max)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Replaced {
    Renamed,
    /// Every rename failed; the target was overwritten in place.
    Overwritten {
        rename_error: String,
    },
}

#[derive(Debug, thiserror::Error)]
#[error("{op}: {}: {source}", path.display())]
pub struct PersistError {
    pub op: String,
    pub path: PathBuf,
    #[source]
    pub source: io::Error,
}

fn file_name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

/// Atomically replaces `path` with `data` via `<path>.tmp` and a rename,
/// retrying the rename per `policy`, then falls back to overwriting `path`
/// in place. `what` names the data in error messages ("saving progress").
pub fn replace_file(
    ops: &dyn FileOps,
    policy: &SavePolicy,
    path: &Path,
    data: &[u8],
    what: &str,
) -> Result<Replaced, PersistError> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let write_tmp = || -> io::Result<()> {
        let mut f = File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()
    };
    write_tmp().map_err(|source| PersistError {
        op: format!("{what} (write {})", file_name(&tmp)),
        path: tmp.clone(),
        source,
    })?;

    let mut rename_error = None;
    for delay in &policy.rename_delays {
        std::thread::sleep(*delay);
        match ops.rename(&tmp, path) {
            Ok(()) => return Ok(Replaced::Renamed),
            Err(e) => rename_error = Some(e),
        }
    }
    let rename_error = rename_error.map_or_else(|| "no attempts".into(), |e| e.to_string());
    let res = ops.overwrite(path, data);
    let _ = fs::remove_file(&tmp);
    match res {
        Ok(()) => Ok(Replaced::Overwritten { rename_error }),
        Err(source) => Err(PersistError {
            op: format!(
                "{what} (replace {}; rename failed: {rename_error}; overwrite in place failed)",
                file_name(path)
            ),
            path: path.to_owned(),
            source,
        }),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// Rename always fails with "access denied"; overwriting fails for the
    /// first `overwrite_failures` calls.
    #[derive(Debug, Default)]
    pub struct DenyRename {
        pub renames: AtomicU32,
        pub overwrites: AtomicU32,
        pub overwrite_failures: u32,
    }

    impl FileOps for DenyRename {
        fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
            self.renames.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::from(io::ErrorKind::PermissionDenied))
        }

        fn overwrite(&self, path: &Path, data: &[u8]) -> io::Result<()> {
            let n = self.overwrites.fetch_add(1, Ordering::SeqCst);
            if n < self.overwrite_failures {
                return Err(io::Error::from(io::ErrorKind::PermissionDenied));
            }
            RealFs.overwrite(path, data)
        }
    }

    pub fn fast_policy() -> SavePolicy {
        SavePolicy {
            rename_delays: vec![Duration::ZERO; 3],
            retry_min: Duration::ZERO,
            retry_max: Duration::ZERO,
            ack_unsaved_after: Duration::from_secs(3600),
        }
    }

    #[test]
    fn renames_normally() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("x.st-state");
        fs::write(&p, b"old").unwrap();
        let r = replace_file(&RealFs, &fast_policy(), &p, b"new", "saving progress").unwrap();
        assert_eq!(r, Replaced::Renamed);
        assert_eq!(fs::read(&p).unwrap(), b"new");
        assert!(!tmp.path().join("x.st-state.tmp").exists());
    }

    #[test]
    fn falls_back_to_overwrite_when_rename_is_denied() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("x.st-state");
        fs::write(&p, b"old contents").unwrap();
        let ops = DenyRename::default();
        let r = replace_file(&ops, &fast_policy(), &p, b"new", "saving progress").unwrap();
        assert!(matches!(r, Replaced::Overwritten { .. }));
        assert_eq!(ops.renames.load(Ordering::SeqCst), 3);
        assert_eq!(fs::read(&p).unwrap(), b"new");
        assert!(!tmp.path().join("x.st-state.tmp").exists());
    }

    #[test]
    fn reports_operation_when_everything_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("x.st-state");
        let ops = DenyRename {
            overwrite_failures: 1,
            ..Default::default()
        };
        let e = replace_file(&ops, &fast_policy(), &p, b"new", "saving progress").unwrap_err();
        let msg = e.to_string();
        assert!(
            msg.starts_with("saving progress (replace x.st-state;"),
            "{msg}"
        );
        assert!(msg.contains("x.st-state"), "{msg}");
        let e = replace_file(
            &RealFs,
            &fast_policy(),
            &tmp.path().join("missing-dir/x.st-state"),
            b"",
            "saving progress",
        )
        .unwrap_err();
        assert!(
            e.to_string()
                .starts_with("saving progress (write x.st-state.tmp)"),
            "{e}"
        );
    }

    #[test]
    fn retry_backoff() {
        let p = SavePolicy::default();
        assert_eq!(p.retry_after(1), Duration::from_secs(2));
        assert_eq!(p.retry_after(2), Duration::from_secs(4));
        assert_eq!(p.retry_after(10), Duration::from_secs(30));
        let total: Duration = p.rename_delays.iter().sum();
        assert!(total <= Duration::from_secs(2));
    }
}
