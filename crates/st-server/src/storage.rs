//! librqbit storage backed by a [`TorrentBuffer`]: nothing touches the disk.

use std::path::Path;
use std::sync::Arc;

use librqbit::storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage};
use librqbit::{ManagedTorrentShared, TorrentMetadata};
use librqbit_core::lengths::ValidPieceIndex;

use crate::buffer::TorrentBuffer;

/// One factory per torrent, created by the engine before adding the torrent.
#[derive(Clone)]
pub struct RamStorageFactory {
    buffer: Arc<TorrentBuffer>,
}

impl RamStorageFactory {
    pub fn new(buffer: Arc<TorrentBuffer>) -> Self {
        Self { buffer }
    }
}

impl StorageFactory for RamStorageFactory {
    type Storage = RamStorage;

    fn create(
        &self,
        _shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<RamStorage> {
        let geom = self.buffer.geometry();
        let lengths = metadata.lengths();
        anyhow::ensure!(
            lengths.total_length() == geom.total_length
                && lengths.default_piece_length() == geom.piece_length
                && metadata.file_infos.len() == geom.file_offsets.len()
                && metadata
                    .file_infos
                    .iter()
                    .zip(&geom.file_offsets)
                    .all(|(fi, &off)| fi.offset_in_torrent == off),
            "torrent geometry does not match the RAM buffer"
        );
        Ok(RamStorage {
            buffer: self.buffer.clone(),
        })
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}

pub struct RamStorage {
    buffer: Arc<TorrentBuffer>,
}

impl RamStorage {
    fn abs(&self, file_id: usize, offset: u64) -> anyhow::Result<u64> {
        let base = self
            .buffer
            .geometry()
            .file_offsets
            .get(file_id)
            .copied()
            .ok_or_else(|| anyhow::anyhow!("bad file id {file_id}"))?;
        Ok(base + offset)
    }
}

impl TorrentStorage for RamStorage {
    fn init(
        &mut self,
        _shared: &ManagedTorrentShared,
        _metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.buffer.read(self.abs(file_id, offset)?, buf)
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.buffer.write(self.abs(file_id, offset)?, buf)
    }

    fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
        Ok(())
    }

    /// The buffer outlives pause/unpause, so both handles share it.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        Ok(Box::new(RamStorage {
            buffer: self.buffer.clone(),
        }))
    }

    fn on_piece_completed(&self, piece_index: ValidPieceIndex) -> anyhow::Result<()> {
        self.buffer.piece_completed(piece_index.get());
        Ok(())
    }
}
