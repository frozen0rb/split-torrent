use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use st_client_core::InfoHash;

pub const APP_ID: &str = "split-torrent";
pub const SETTINGS_KEY: &str = "settings";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Settings {
    pub server: String,
    pub token: String,
    pub fingerprint: String,
    pub download_dir: String,
    /// Reconnect on start-up (the user did not press Disconnect).
    pub auto_connect: bool,
    pub torrents: Vec<SavedTorrent>,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            server: String::new(),
            token: String::new(),
            fingerprint: String::new(),
            download_dir: default_download_dir().display().to_string(),
            auto_connect: false,
            torrents: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavedTorrent {
    pub info_hash: String,
    pub name: String,
    /// Private copy of the `.torrent`, so the original may be deleted.
    pub torrent_file: PathBuf,
    pub download_dir: PathBuf,
    pub paused: bool,
}

impl Settings {
    pub fn find(&self, ih: &InfoHash) -> Option<usize> {
        let hex = hex(ih);
        self.torrents.iter().position(|t| t.info_hash == hex)
    }
}

pub fn hex(ih: &InfoHash) -> String {
    ih.iter().map(|b| format!("{b:02x}")).collect()
}

fn default_download_dir() -> PathBuf {
    let home = std::env::var_os(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join("Downloads")
}

/// Where copies of added `.torrent` files are kept.
pub fn torrents_dir() -> PathBuf {
    eframe::storage_dir(APP_ID)
        .unwrap_or_else(|| std::env::temp_dir().join(APP_ID))
        .join("torrents")
}

pub fn store_torrent_copy(dir: &Path, ih: &InfoHash, bytes: &[u8]) -> std::io::Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{}.torrent", hex(ih)));
    std::fs::write(&path, bytes)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_fields_use_defaults() {
        let empty = serde::de::value::MapDeserializer::<
            std::vec::IntoIter<(String, String)>,
            serde::de::value::Error,
        >::new(Vec::new().into_iter());
        let s = Settings::deserialize(empty).unwrap();
        assert_eq!(s, Settings::default());
        assert!(s.download_dir.ends_with("Downloads"));
    }

    #[test]
    fn torrent_copy_and_lookup() {
        let tmp = std::env::temp_dir().join(format!("st-client-test-{}", std::process::id()));
        let ih = [0xab; 20];
        let p = store_torrent_copy(&tmp, &ih, b"d4:infodee").unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"d4:infodee");
        assert!(p.ends_with(format!("{}.torrent", "ab".repeat(20))));
        let mut s = Settings::default();
        s.torrents.push(SavedTorrent {
            info_hash: hex(&ih),
            name: "x".into(),
            torrent_file: p,
            download_dir: tmp.clone(),
            paused: false,
        });
        assert_eq!(s.find(&ih), Some(0));
        assert_eq!(s.find(&[0; 20]), None);
        std::fs::remove_dir_all(tmp).unwrap();
    }
}
