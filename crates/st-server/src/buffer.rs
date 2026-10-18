//! RAM buffer for torrent pieces.
//!
//! [`BufferPool`] is the global byte budget shared by all torrents.
//! [`TorrentBuffer`] holds one torrent's pieces: *partial* pieces that are
//! being downloaded and *complete* (hash-verified) pieces waiting for the
//! client's ACK. Memory is returned to the pool as soon as a piece is ACKed or
//! turns out to be one the client already has or did not select.
//!
//! Backpressure has two levels:
//! - soft: [`BufferPool::pressure`] tells the engine to throttle swarm
//!   downloads once the buffer is mostly full or too many pieces are in flight;
//! - hard: allocating a new partial piece blocks (librqbit calls storage from
//!   `block_in_place`) while the byte limit is exceeded and there are complete
//!   pieces whose ACK will free memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::{Condvar, Mutex};
use st_protocol::Bitfield;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub used: u64,
    pub limit: u64,
    pub inflight_pieces: usize,
    pub inflight_bytes: u64,
    pub complete_bytes: u64,
}

#[derive(Default)]
struct PoolState {
    used: u64,
    inflight_pieces: usize,
    inflight_bytes: u64,
    complete_bytes: u64,
}

pub struct BufferPool {
    limit: u64,
    max_inflight: usize,
    state: Mutex<PoolState>,
    freed: Condvar,
    changed: tokio::sync::Notify,
}

impl BufferPool {
    pub fn new(limit: u64, max_inflight_pieces: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            max_inflight: max_inflight_pieces.max(1),
            state: Mutex::new(PoolState::default()),
            freed: Condvar::new(),
            changed: tokio::sync::Notify::new(),
        })
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn max_inflight(&self) -> usize {
        self.max_inflight
    }

    pub fn stats(&self) -> PoolStats {
        let s = self.state.lock();
        PoolStats {
            used: s.used,
            limit: self.limit,
            inflight_pieces: s.inflight_pieces,
            inflight_bytes: s.inflight_bytes,
            complete_bytes: s.complete_bytes,
        }
    }

    pub fn used(&self) -> u64 {
        self.state.lock().used
    }

    /// Soft backpressure with hysteresis: turns on at 75% of the limit or at
    /// `max_inflight` partial pieces, turns off below 50% and below the
    /// in-flight cap.
    pub fn pressure(&self, currently_on: bool) -> bool {
        let s = self.state.lock();
        let inflight_full = s.inflight_pieces >= self.max_inflight;
        if currently_on {
            inflight_full || s.used >= self.limit / 2
        } else {
            inflight_full || s.used >= self.limit - self.limit / 4
        }
    }

    /// Resolves whenever usage changes.
    pub async fn changed(&self) {
        self.changed.notified().await
    }

    /// Reserves memory for a new partial piece. Blocks while the limit would
    /// be exceeded and some complete pieces can still be freed by an ACK.
    /// Without complete pieces nothing could ever free memory, so the
    /// reservation goes through (bounded by soft backpressure).
    fn reserve_partial(&self, len: u64, cancelled: &AtomicBool) -> anyhow::Result<()> {
        let mut s = self.state.lock();
        loop {
            if s.used + len <= self.limit || s.complete_bytes == 0 {
                s.used += len;
                s.inflight_pieces += 1;
                s.inflight_bytes += len;
                drop(s);
                self.changed.notify_waiters();
                return Ok(());
            }
            if cancelled.load(Ordering::Relaxed) {
                anyhow::bail!("torrent buffer closed");
            }
            self.freed.wait_for(&mut s, Duration::from_millis(250));
        }
    }

    fn partial_to_complete(&self, len: u64) {
        let mut s = self.state.lock();
        s.inflight_pieces -= 1;
        s.inflight_bytes -= len;
        s.complete_bytes += len;
        drop(s);
        self.changed.notify_waiters();
    }

    fn release_partial(&self, len: u64) {
        let mut s = self.state.lock();
        s.used -= len;
        s.inflight_pieces -= 1;
        s.inflight_bytes -= len;
        drop(s);
        self.freed.notify_all();
        self.changed.notify_waiters();
    }

    fn release_complete(&self, len: u64) {
        let mut s = self.state.lock();
        s.used -= len;
        s.complete_bytes -= len;
        drop(s);
        self.freed.notify_all();
        self.changed.notify_waiters();
    }
}

/// Byte layout of a torrent, independent of librqbit types.
#[derive(Debug, Clone)]
pub struct Geometry {
    pub piece_length: u32,
    pub total_length: u64,
    /// Absolute offset of every file, indexed by librqbit `file_id`.
    pub file_offsets: Vec<u64>,
}

impl Geometry {
    pub fn piece_count(&self) -> u32 {
        self.total_length.div_ceil(u64::from(self.piece_length)) as u32
    }

    pub fn piece_len(&self, index: u32) -> u32 {
        let start = u64::from(index) * u64::from(self.piece_length);
        (self.total_length - start).min(u64::from(self.piece_length)) as u32
    }
}

struct Partial {
    data: Vec<u8>,
    last_write: Instant,
}

struct Inner {
    partial: HashMap<u32, Partial>,
    complete: BTreeMap<u32, Bytes>,
    /// Pieces the client has (ACKed, or reported in `AddTorrent`).
    done: Bitfield,
    /// Pieces overlapping the client's selected files.
    wanted: Bitfield,
    /// Pieces librqbit verified while this buffer was in use. librqbit will
    /// not download them again.
    verified: Bitfield,
    /// Complete pieces not yet sent on the current connection.
    unsent: BTreeSet<u32>,
}

pub struct TorrentBuffer {
    pool: Arc<BufferPool>,
    geom: Geometry,
    inner: Mutex<Inner>,
    ready: tokio::sync::Notify,
    closed: AtomicBool,
}

impl TorrentBuffer {
    /// `have` and `wanted` must have `geom.piece_count()` bits.
    pub fn new(
        pool: Arc<BufferPool>,
        geom: Geometry,
        have: Bitfield,
        wanted: Bitfield,
    ) -> Arc<Self> {
        let n = geom.piece_count();
        assert_eq!(have.len(), n);
        assert_eq!(wanted.len(), n);
        Arc::new(Self {
            pool,
            geom,
            inner: Mutex::new(Inner {
                partial: HashMap::new(),
                complete: BTreeMap::new(),
                done: have,
                wanted,
                verified: Bitfield::new(n),
                unsent: BTreeSet::new(),
            }),
            ready: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
        })
    }

    pub fn geometry(&self) -> &Geometry {
        &self.geom
    }

    fn piece_span(&self, abs: u64, len: usize) -> anyhow::Result<(u32, usize)> {
        let pl = u64::from(self.geom.piece_length);
        let index = u32::try_from(abs / pl)?;
        anyhow::ensure!(index < self.geom.piece_count(), "offset {abs} out of range");
        let in_piece = (abs % pl) as usize;
        let n = len.min(self.geom.piece_len(index) as usize - in_piece);
        Ok((index, n))
    }

    /// Writes torrent bytes at an absolute offset (may span pieces).
    pub fn write(&self, mut abs: u64, mut buf: &[u8]) -> anyhow::Result<()> {
        while !buf.is_empty() {
            let (index, n) = self.piece_span(abs, buf.len())?;
            let in_piece = (abs % u64::from(self.geom.piece_length)) as usize;
            self.write_in_piece(index, in_piece, &buf[..n])?;
            abs += n as u64;
            buf = &buf[n..];
        }
        Ok(())
    }

    fn write_in_piece(&self, index: u32, at: usize, data: &[u8]) -> anyhow::Result<()> {
        if self.closed.load(Ordering::Relaxed) {
            anyhow::bail!("torrent buffer closed");
        }
        {
            let mut g = self.inner.lock();
            if let Some(p) = g.partial.get_mut(&index) {
                p.data[at..at + data.len()].copy_from_slice(data);
                p.last_write = Instant::now();
                return Ok(());
            }
            if g.complete.contains_key(&index) {
                return Ok(());
            }
        }
        let plen = self.geom.piece_len(index);
        self.pool.reserve_partial(u64::from(plen), &self.closed)?;
        let mut g = self.inner.lock();
        if self.closed.load(Ordering::Relaxed) {
            drop(g);
            self.pool.release_partial(u64::from(plen));
            anyhow::bail!("torrent buffer closed");
        }
        if g.complete.contains_key(&index) {
            drop(g);
            self.pool.release_partial(u64::from(plen));
            return Ok(());
        }
        let mut inserted = false;
        let p = g.partial.entry(index).or_insert_with(|| {
            inserted = true;
            Partial {
                data: vec![0; plen as usize],
                last_write: Instant::now(),
            }
        });
        p.data[at..at + data.len()].copy_from_slice(data);
        p.last_write = Instant::now();
        if !inserted {
            // Another writer allocated it while we waited for the pool.
            drop(g);
            self.pool.release_partial(u64::from(plen));
        }
        Ok(())
    }

    /// Reads buffered bytes. Fails for pieces that are not in RAM.
    pub fn read(&self, mut abs: u64, mut buf: &mut [u8]) -> anyhow::Result<()> {
        let g = self.inner.lock();
        while !buf.is_empty() {
            let (index, n) = self.piece_span(abs, buf.len())?;
            let at = (abs % u64::from(self.geom.piece_length)) as usize;
            let src: &[u8] = if let Some(p) = g.partial.get(&index) {
                &p.data
            } else if let Some(p) = g.complete.get(&index) {
                p
            } else {
                anyhow::bail!("piece {index} is not in the RAM buffer");
            };
            let (head, tail) = buf.split_at_mut(n);
            head.copy_from_slice(&src[at..at + n]);
            buf = tail;
            abs += n as u64;
        }
        Ok(())
    }

    /// Called after librqbit verified the piece hash.
    pub fn piece_completed(&self, index: u32) {
        let mut g = self.inner.lock();
        let Some(Partial { data, .. }) = g.partial.remove(&index) else {
            return;
        };
        let len = data.len() as u64;
        self.pool.partial_to_complete(len);
        g.verified.set(index, true);
        if g.done.get(index) || !g.wanted.get(index) {
            drop(g);
            self.pool.release_complete(len);
            return;
        }
        g.complete.insert(index, Bytes::from(data));
        g.unsent.insert(index);
        drop(g);
        self.ready.notify_one();
    }

    /// Next complete piece to send on the current connection.
    pub fn next_unsent(&self) -> Option<(u32, Bytes)> {
        let mut g = self.inner.lock();
        while let Some(index) = g.unsent.pop_first() {
            if let Some(data) = g.complete.get(&index) {
                return Some((index, data.clone()));
            }
        }
        None
    }

    /// Waits until [`next_unsent`](Self::next_unsent) may return something.
    pub async fn wait_ready(&self) {
        self.ready.notified().await
    }

    /// Marks every complete piece as unsent, e.g. for a new connection.
    pub fn reset_unsent(&self) {
        let mut g = self.inner.lock();
        g.unsent = g.complete.keys().copied().collect();
        drop(g);
        self.ready.notify_one();
    }

    /// The client stored the piece; frees its memory. Returns whether it was
    /// buffered.
    pub fn ack(&self, index: u32) -> bool {
        let mut g = self.inner.lock();
        if index < g.done.len() {
            g.done.set(index, true);
        }
        g.unsent.remove(&index);
        let freed = g.complete.remove(&index);
        drop(g);
        match freed {
            Some(d) => {
                self.pool.release_complete(d.len() as u64);
                true
            }
            None => false,
        }
    }

    /// Merges a client bitfield (e.g. after reconnect) and frees buffered
    /// pieces the client already has.
    pub fn merge_have(&self, have: &Bitfield) {
        let mut g = self.inner.lock();
        let mut freed = 0u64;
        for i in have.iter_ones() {
            if i >= g.done.len() {
                break;
            }
            g.done.set(i, true);
            g.unsent.remove(&i);
            if let Some(d) = g.complete.remove(&i) {
                freed += d.len() as u64;
            }
        }
        drop(g);
        if freed > 0 {
            self.pool.release_complete(freed);
        }
    }

    /// Pieces in `wanted` that the client lacks according to `have` and that
    /// librqbit will not fetch again: they were ACKed or verified and then
    /// freed. They can only be recovered by restarting the torrent.
    pub fn lost_pieces(&self, have: &Bitfield, wanted: &Bitfield) -> Vec<u32> {
        let g = self.inner.lock();
        wanted
            .iter_ones()
            .filter(|&i| {
                !have.get(i) && !g.complete.contains_key(&i) && (g.done.get(i) || g.verified.get(i))
            })
            .collect()
    }

    /// Changes the selection and frees complete pieces that are no longer
    /// wanted. Partial pieces stay: librqbit may still be writing them, and
    /// they are freed on completion or by [`drop_stale`](Self::drop_stale).
    pub fn set_wanted(&self, wanted: Bitfield) {
        assert_eq!(wanted.len(), self.geom.piece_count());
        let mut g = self.inner.lock();
        let gone: Vec<u32> = g
            .complete
            .keys()
            .copied()
            .filter(|&i| !wanted.get(i))
            .collect();
        let mut freed = 0u64;
        for i in gone {
            g.unsent.remove(&i);
            if let Some(d) = g.complete.remove(&i) {
                freed += d.len() as u64;
            }
        }
        g.wanted = wanted;
        drop(g);
        if freed > 0 {
            self.pool.release_complete(freed);
        }
    }

    /// Frees partial pieces that are not wanted and were not written for
    /// `max_idle`, e.g. a deselected piece whose peer went away.
    pub fn drop_stale(&self, max_idle: Duration) -> usize {
        let mut g = self.inner.lock();
        let Inner {
            partial, wanted, ..
        } = &mut *g;
        let stale: Vec<u32> = partial
            .iter()
            .filter(|(i, p)| !wanted.get(**i) && p.last_write.elapsed() >= max_idle)
            .map(|(i, _)| *i)
            .collect();
        let lens: Vec<u64> = stale
            .iter()
            .filter_map(|i| partial.remove(i))
            .map(|p| p.data.len() as u64)
            .collect();
        drop(g);
        for &len in &lens {
            self.pool.release_partial(len);
        }
        lens.len()
    }

    /// Moves complete pieces that `self` still needs from `old` into `self`,
    /// e.g. when the torrent is restarted with a fresh buffer.
    pub fn adopt_complete(&self, old: &TorrentBuffer) {
        let mut from = old.inner.lock();
        let mut to = self.inner.lock();
        let keep: Vec<u32> = from
            .complete
            .keys()
            .copied()
            .filter(|&i| to.wanted.get(i) && !to.done.get(i))
            .collect();
        for i in keep {
            let data = from.complete.remove(&i).expect("listed");
            from.unsent.remove(&i);
            to.complete.insert(i, data);
            to.unsent.insert(i);
        }
        drop((from, to));
        self.ready.notify_one();
    }

    /// Selected pieces the client has.
    pub fn done_count(&self) -> u32 {
        let g = self.inner.lock();
        g.wanted.iter_ones().filter(|&i| g.done.get(i)).count() as u32
    }

    pub fn wanted_count(&self) -> u32 {
        self.inner.lock().wanted.count_ones()
    }

    /// Pieces the client has (ACKed or reported).
    pub fn done(&self) -> Bitfield {
        self.inner.lock().done.clone()
    }

    /// The client has every selected piece.
    pub fn is_finished(&self) -> bool {
        let g = self.inner.lock();
        (0..g.wanted.len()).all(|i| !g.wanted.get(i) || g.done.get(i))
    }

    pub fn buffered_bytes(&self) -> u64 {
        let g = self.inner.lock();
        g.partial.values().map(|p| p.data.len() as u64).sum::<u64>()
            + g.complete.values().map(|p| p.len() as u64).sum::<u64>()
    }

    /// Frees everything; later writes fail and blocked writers wake up.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        let mut g = self.inner.lock();
        let partial: Vec<u64> = g
            .partial
            .drain()
            .map(|(_, p)| p.data.len() as u64)
            .collect();
        let complete: u64 = std::mem::take(&mut g.complete)
            .into_values()
            .map(|d| d.len() as u64)
            .sum();
        g.unsent.clear();
        drop(g);
        for len in partial {
            self.pool.release_partial(len);
        }
        if complete > 0 {
            self.pool.release_complete(complete);
        }
        self.ready.notify_waiters();
    }
}

impl Drop for TorrentBuffer {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    const PL: u32 = 1024;

    fn geom(pieces: u32, last: u32) -> Geometry {
        Geometry {
            piece_length: PL,
            total_length: u64::from(PL) * u64::from(pieces - 1) + u64::from(last),
            file_offsets: vec![0],
        }
    }

    fn fill(buf: &TorrentBuffer, index: u32) {
        let len = buf.geometry().piece_len(index) as usize;
        let data = vec![index as u8; len];
        buf.write(u64::from(index) * u64::from(PL), &data).unwrap();
    }

    #[test]
    fn geometry_last_piece() {
        let g = geom(3, 100);
        assert_eq!(g.piece_count(), 3);
        assert_eq!(g.piece_len(0), PL);
        assert_eq!(g.piece_len(2), 100);
    }

    #[test]
    fn write_read_complete_ack_frees_memory() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(3, 100),
            Bitfield::new(3),
            Bitfield::full(3),
        );

        buf.write(10, &[1, 2, 3]).unwrap();
        assert_eq!(pool.stats().inflight_pieces, 1);
        assert_eq!(pool.used(), u64::from(PL));
        let mut out = [0; 3];
        buf.read(10, &mut out).unwrap();
        assert_eq!(out, [1, 2, 3]);

        buf.write(PL as u64 * 2, &[9; 100]).unwrap();
        assert_eq!(pool.used(), u64::from(PL) + 100);

        buf.piece_completed(0);
        let s = pool.stats();
        assert_eq!((s.inflight_pieces, s.complete_bytes), (1, u64::from(PL)));
        let (i, data) = buf.next_unsent().unwrap();
        assert_eq!(i, 0);
        assert_eq!(&data[10..13], &[1, 2, 3]);
        assert!(buf.next_unsent().is_none());

        assert!(buf.ack(0));
        assert_eq!(pool.used(), 100);
        assert!(
            buf.read(0, &mut out).is_err(),
            "freed piece must not be readable"
        );
        assert!(!buf.ack(0));
        assert_eq!(buf.done_count(), 1);
    }

    #[test]
    fn write_spanning_two_pieces() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(3, PL),
            Bitfield::new(3),
            Bitfield::full(3),
        );
        buf.write(u64::from(PL) - 2, &[7, 7, 8, 8]).unwrap();
        assert_eq!(pool.stats().inflight_pieces, 2);
        let mut out = [0; 4];
        buf.read(u64::from(PL) - 2, &mut out).unwrap();
        assert_eq!(out, [7, 7, 8, 8]);
        assert!(buf.write(3 * u64::from(PL), &[0]).is_err());
    }

    #[test]
    fn pieces_client_has_are_freed_on_completion() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let mut have = Bitfield::new(4);
        have.set(1, true);
        let buf = TorrentBuffer::new(pool.clone(), geom(4, PL), have, Bitfield::full(4));
        fill(&buf, 1);
        buf.piece_completed(1);
        assert_eq!(pool.used(), 0);
        assert!(buf.next_unsent().is_none());
    }

    #[test]
    fn merge_have_frees_and_lost_pieces_detected() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        for i in 0..3 {
            fill(&buf, i);
            buf.piece_completed(i);
        }
        buf.ack(0);
        assert_eq!(pool.used(), 2 * u64::from(PL));

        let mut have = Bitfield::new(4);
        have.set(0, true);
        have.set(1, true);
        let all = Bitfield::full(4);
        assert!(buf.lost_pieces(&have, &all).is_empty());
        buf.merge_have(&have);
        assert_eq!(pool.used(), u64::from(PL));
        assert_eq!(buf.next_unsent().map(|p| p.0), Some(2));

        assert_eq!(buf.lost_pieces(&Bitfield::new(4), &all), vec![0, 1]);
    }

    fn bits(n: u32, ones: &[u32]) -> Bitfield {
        let mut b = Bitfield::new(n);
        for &i in ones {
            b.set(i, true);
        }
        b
    }

    #[test]
    fn unwanted_pieces_are_not_kept_and_do_not_block_finish() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            bits(4, &[0, 1]),
        );
        assert_eq!(buf.wanted_count(), 2);
        fill(&buf, 3);
        buf.piece_completed(3);
        assert_eq!(pool.used(), 0, "unwanted piece freed on completion");
        assert!(buf.next_unsent().is_none());
        for i in 0..2 {
            fill(&buf, i);
            buf.piece_completed(i);
            buf.ack(i);
        }
        assert!(buf.is_finished());
        assert_eq!(buf.done_count(), 2);
    }

    #[test]
    fn deselecting_frees_complete_pieces_and_stale_partials() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        for i in 0..3 {
            fill(&buf, i);
        }
        buf.piece_completed(0);
        buf.piece_completed(1);
        assert_eq!(pool.used(), 3 * u64::from(PL));

        // Keep piece 0 (e.g. shared with a still-selected file).
        buf.set_wanted(bits(4, &[0, 3]));
        assert_eq!(pool.used(), 2 * u64::from(PL), "piece 1 freed");
        assert_eq!(buf.next_unsent().map(|p| p.0), Some(0));
        assert!(buf.next_unsent().is_none());

        // The partial piece 2 survives until it is idle long enough.
        assert_eq!(buf.drop_stale(Duration::from_secs(60)), 0);
        assert_eq!(buf.drop_stale(Duration::ZERO), 1);
        assert_eq!(pool.used(), u64::from(PL));
        assert_eq!(pool.stats().inflight_pieces, 0);

        // Piece 1 was verified by librqbit and freed: it is lost if wanted again.
        assert_eq!(
            buf.lost_pieces(&Bitfield::new(4), &Bitfield::full(4)),
            vec![1]
        );
        assert!(buf
            .lost_pieces(&Bitfield::new(4), &bits(4, &[0, 2, 3]))
            .is_empty());
    }

    #[test]
    fn restart_adopts_complete_pieces() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let old = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        for i in 0..3 {
            fill(&old, i);
            old.piece_completed(i);
        }
        let new = TorrentBuffer::new(pool.clone(), geom(4, PL), bits(4, &[0]), bits(4, &[0, 1]));
        new.adopt_complete(&old);
        old.close();
        assert_eq!(pool.used(), u64::from(PL), "only piece 1 moved");
        let (i, data) = new.next_unsent().unwrap();
        assert_eq!((i, data[0]), (1, 1));
        new.ack(1);
        assert_eq!(pool.used(), 0);
        assert!(new.is_finished());
    }

    #[test]
    fn reset_unsent_resends_unacked() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(pool, geom(4, PL), Bitfield::new(4), Bitfield::full(4));
        for i in [2, 0] {
            fill(&buf, i);
            buf.piece_completed(i);
        }
        assert_eq!(buf.next_unsent().map(|p| p.0), Some(0));
        assert_eq!(buf.next_unsent().map(|p| p.0), Some(2));
        assert!(buf.next_unsent().is_none());
        buf.ack(0);
        buf.reset_unsent();
        assert_eq!(buf.next_unsent().map(|p| p.0), Some(2));
        assert!(buf.next_unsent().is_none());
    }

    #[test]
    fn soft_pressure_has_hysteresis_and_inflight_cap() {
        let pool = BufferPool::new(8 * u64::from(PL), 3);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(8, PL),
            Bitfield::new(8),
            Bitfield::full(8),
        );
        fill(&buf, 0);
        fill(&buf, 1);
        assert!(!pool.pressure(false));
        fill(&buf, 2);
        assert!(pool.pressure(false), "3 in-flight pieces hit the cap");
        for i in 0..3 {
            buf.piece_completed(i);
        }
        assert!(!pool.pressure(false));
        for i in 3..6 {
            fill(&buf, i);
            buf.piece_completed(i);
        }
        // 6/8 used = 75%
        assert!(pool.pressure(false));
        buf.ack(0);
        buf.ack(1);
        // 4/8 used: still on (needs < 50% to turn off), would not turn on
        assert!(pool.pressure(true));
        assert!(!pool.pressure(false));
        buf.ack(2);
        assert!(!pool.pressure(true));
    }

    #[test]
    fn hard_limit_blocks_until_ack() {
        let pool = BufferPool::new(2 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        fill(&buf, 0);
        fill(&buf, 1);
        buf.piece_completed(0);
        buf.piece_completed(1);
        assert_eq!(pool.used(), pool.limit());

        let b2 = buf.clone();
        let started = Instant::now();
        let writer = std::thread::spawn(move || {
            fill(&b2, 2);
            started.elapsed()
        });
        std::thread::sleep(Duration::from_millis(300));
        assert!(
            !writer.is_finished(),
            "write must block while buffer is full"
        );
        assert_eq!(pool.used(), pool.limit());
        buf.ack(0);
        let waited = writer.join().unwrap();
        assert!(waited >= Duration::from_millis(300));
        assert_eq!(pool.used(), pool.limit());
        assert!(pool.used() <= pool.limit());
    }

    #[test]
    fn hard_limit_does_not_deadlock_without_complete_pieces() {
        let pool = BufferPool::new(u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        fill(&buf, 0);
        fill(&buf, 1);
        assert_eq!(pool.used(), 2 * u64::from(PL));
    }

    #[test]
    fn close_frees_everything_and_unblocks_writers() {
        let pool = BufferPool::new(u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        fill(&buf, 0);
        buf.piece_completed(0);
        let b2 = buf.clone();
        let writer = std::thread::spawn(move || b2.write(u64::from(PL), &[1]));
        std::thread::sleep(Duration::from_millis(100));
        buf.close();
        assert!(writer.join().unwrap().is_err());
        assert_eq!(pool.stats(), BufferPool::new(u64::from(PL), 8).stats());
        assert!(buf.write(0, &[1]).is_err());
    }

    #[test]
    fn drop_returns_memory_to_pool() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let buf = TorrentBuffer::new(
            pool.clone(),
            geom(4, PL),
            Bitfield::new(4),
            Bitfield::full(4),
        );
        fill(&buf, 0);
        fill(&buf, 1);
        buf.piece_completed(1);
        drop(buf);
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.stats().inflight_pieces, 0);
    }

    #[test]
    fn finished_when_all_done() {
        let pool = BufferPool::new(10 * u64::from(PL), 8);
        let mut have = Bitfield::new(2);
        have.set(0, true);
        let buf = TorrentBuffer::new(pool, geom(2, 10), have, Bitfield::full(2));
        assert!(!buf.is_finished());
        fill(&buf, 1);
        buf.piece_completed(1);
        buf.ack(1);
        assert!(buf.is_finished());
    }
}
