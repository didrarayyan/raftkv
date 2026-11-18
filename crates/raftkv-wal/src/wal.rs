//! Segmented, CRC-checked write-ahead log and atomic snapshot manager.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use raft_core::{Entry, HardState, Index, Recovered, Snapshot};
use thiserror::Error;

use crate::format::{
    DecodeFrameError, Record, WAL_HEADER_LEN, decode_record, decode_segment_header,
    decode_snapshot, encode_entry_record, encode_hard_state_record, encode_segment_header,
    encode_snapshot,
};
use crate::fs::{Fs, FsFile};

/// Configuration options for opening and rotating a [`Wal`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalOptions {
    /// Target byte threshold at which the active WAL segment rotates to a new sequence file.
    pub segment_bytes: u64,
    /// Number of entries below the latest snapshot index retained before old segments are removed.
    pub retain_entries: u64,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            segment_bytes: 64 * 1024 * 1024,
            retain_entries: 1024,
        }
    }
}

/// Atomic persistence batch passed to [`Wal::persist`].
#[derive(Debug, Clone, Copy)]
pub struct Batch<'a> {
    /// Optional snapshot received from a leader to install durably before entries and hard state.
    pub snapshot: Option<&'a Snapshot>,
    /// Optional updated durable hard state.
    pub hard_state: Option<&'a HardState>,
    /// Log entries to append; an entry with `index <= last written` truncates the old suffix.
    pub entries: &'a [Entry],
}

/// Errors returned by WAL recovery and persistence operations.
#[derive(Debug, Error)]
pub enum WalError {
    /// Underlying filesystem or sync failure; triggers fail-stop poisoning on write paths.
    #[error("WAL I/O error: {0}")]
    Io(#[from] io::Error),
    /// Checksum, header, or structural corruption detected on disk.
    #[error("WAL corruption: {0}")]
    Corruption(String),
    /// A previous write or sync failed, permanently poisoning this WAL instance.
    #[error("WAL is poisoned after a prior I/O failure")]
    Poisoned,
}

#[derive(Debug, Clone, Copy)]
struct SegmentInfo {
    max_index: Index,
}

/// Write-ahead log and snapshot store parameterized over a synchronous [`Fs`].
#[derive(Debug)]
pub struct Wal<F: Fs> {
    fs: F,
    log_dir: PathBuf,
    snap_dir: PathBuf,
    opts: WalOptions,
    active_seq: u64,
    active_file: Option<F::File>,
    active_bytes: u64,
    segments: BTreeMap<u64, SegmentInfo>,
    last_written_index: Index,
    latest_hard_state: HardState,
    latest_snapshot_name: Option<String>,
    poisoned: bool,
}

impl<F: Fs> Wal<F> {
    /// Opens an existing WAL directory or initializes a fresh one, recovering durable state.
    pub fn open(fs: F, dir: &Path, opts: WalOptions) -> Result<(Self, Recovered), WalError> {
        let wal_dir = dir.join("wal");
        let snap_dir = dir.join("snap");
        fs.create_dir_all(&wal_dir)?;
        fs.create_dir_all(&snap_dir)?;

        cleanup_temp_files(&fs, &snap_dir)?;
        cleanup_temp_files(&fs, &wal_dir)?;

        let (snapshot, latest_snapshot_name) = load_newest_snapshot(&fs, &snap_dir)?;
        let snapshot_index = snapshot.as_ref().map_or(Index(0), |s| s.meta.index);

        let mut segment_files = list_segments(&fs, &wal_dir)?;
        segment_files.sort_by_key(|(seq, _)| *seq);

        let mut entries: Vec<Entry> = Vec::new();
        let mut last_index = snapshot_index;
        let mut hard_state = HardState::default();
        let mut segments: BTreeMap<u64, SegmentInfo> = BTreeMap::new();

        for idx in 0..segment_files.len() {
            let (seq, ref path) = segment_files[idx];
            let is_last = idx + 1 == segment_files.len();
            let raw = fs.read_file(path)?;

            match decode_segment_header(&raw) {
                Ok(hdr_seq) if hdr_seq == seq => {}
                Ok(_) => {
                    return Err(WalError::Corruption(format!(
                        "segment {} sequence mismatch in header",
                        path.display()
                    )));
                }
                Err(DecodeFrameError::Truncated) if is_last => {
                    let mut f = fs.open_append(path)?;
                    f.truncate(0)?;
                    let hdr = encode_segment_header(seq);
                    f.write_all(&hdr)?;
                    f.sync_data()?;
                    segments.insert(
                        seq,
                        SegmentInfo {
                            max_index: Index(0),
                        },
                    );
                    continue;
                }
                Err(DecodeFrameError::Truncated | DecodeFrameError::Invalid(_)) => {
                    return Err(WalError::Corruption(format!(
                        "corrupt header in segment {}",
                        path.display()
                    )));
                }
            }

            let mut offset = WAL_HEADER_LEN;
            let mut seg_max_index = Index(0);
            let mut truncated_tail_at: Option<usize> = None;

            while offset < raw.len() {
                match decode_record(&raw[offset..]) {
                    Ok((Record::Entry(entry), len)) => {
                        offset += len;
                        seg_max_index = seg_max_index.max(entry.index);
                        if entry.index <= snapshot_index {
                            continue;
                        }
                        if entry.index <= last_index {
                            let keep = (entry.index.0 - snapshot_index.0 - 1) as usize;
                            entries.truncate(keep);
                            last_index = entry.index.prev();
                        }
                        if entry.index != last_index.next() {
                            return Err(WalError::Corruption(format!(
                                "non-contiguous entry {} after {} in {}",
                                entry.index,
                                last_index,
                                path.display()
                            )));
                        }
                        last_index = entry.index;
                        entries.push(entry);
                    }
                    Ok((Record::HardState(hs), len)) => {
                        offset += len;
                        hard_state = hs;
                    }
                    Err(DecodeFrameError::Truncated | DecodeFrameError::Invalid(_)) if is_last => {
                        truncated_tail_at = Some(offset);
                        break;
                    }
                    Err(DecodeFrameError::Truncated) => {
                        return Err(WalError::Corruption(format!(
                            "truncated record in non-final segment {} at offset {offset}",
                            path.display()
                        )));
                    }
                    Err(DecodeFrameError::Invalid(reason)) => {
                        return Err(WalError::Corruption(format!(
                            "corrupt record in non-final segment {} at offset {offset}: {reason}",
                            path.display()
                        )));
                    }
                }
            }

            if let Some(valid_end) = truncated_tail_at {
                let mut f = fs.open_append(path)?;
                f.truncate(valid_end as u64)?;
                f.sync_data()?;
            }
            segments.insert(
                seq,
                SegmentInfo {
                    max_index: seg_max_index,
                },
            );
        }

        hard_state.commit = hard_state.commit.min(last_index);

        let (active_seq, active_file, active_bytes) =
            if let Some((seq, path)) = segment_files.last() {
                let file = fs.open_append(path)?;
                let len = file.len()?;
                (*seq, file, len)
            } else {
                let seq = 1u64;
                let path = segment_path(&wal_dir, seq);
                let mut file = fs.create_file(&path)?;
                let hdr = encode_segment_header(seq);
                file.write_all(&hdr)?;
                file.sync_data()?;
                fs.sync_dir(&wal_dir)?;
                segments.insert(
                    seq,
                    SegmentInfo {
                        max_index: Index(0),
                    },
                );
                (seq, file, WAL_HEADER_LEN as u64)
            };

        let recovered = Recovered {
            hard_state,
            snapshot,
            entries,
        };
        let wal = Self {
            fs,
            log_dir: wal_dir,
            snap_dir,
            opts,
            active_seq,
            active_file: Some(active_file),
            active_bytes,
            segments,
            last_written_index: last_index,
            latest_hard_state: hard_state,
            latest_snapshot_name,
            poisoned: false,
        };
        Ok((wal, recovered))
    }

    /// Persists an atomic batch (`snapshot`, then `entries`, then `hard_state`) and waits for sync.
    pub fn persist(&mut self, batch: Batch<'_>) -> Result<(), WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }

        if let Some(snap) = batch.snapshot {
            self.save_snapshot(snap)?;
        }

        if batch.entries.is_empty() && batch.hard_state.is_none() {
            return Ok(());
        }

        let res = self.persist_records(batch.entries, batch.hard_state);
        if let Err(WalError::Io(_)) = res {
            self.poisoned = true;
        }
        res
    }

    /// Atomically persists `snapshot`, removes obsolete WAL segments, and prunes older snapshots.
    pub fn save_snapshot(&mut self, snapshot: &Snapshot) -> Result<(), WalError> {
        if self.poisoned {
            return Err(WalError::Poisoned);
        }
        let res = self.save_snapshot_inner(snapshot);
        if let Err(WalError::Io(_)) = res {
            self.poisoned = true;
        }
        res
    }

    fn persist_records(
        &mut self,
        entries: &[Entry],
        hard_state: Option<&HardState>,
    ) -> Result<(), WalError> {
        if self.active_bytes >= self.opts.segment_bytes {
            self.rotate_segment()?;
        }

        let mut buf = Vec::with_capacity(entries.len() * 64 + 48);
        for entry in entries {
            encode_entry_record(entry, &mut buf);
            self.last_written_index = entry.index;
            if let Some(info) = self.segments.get_mut(&self.active_seq) {
                info.max_index = info.max_index.max(entry.index);
            }
        }
        if let Some(hs) = hard_state {
            self.latest_hard_state = *hs;
            encode_hard_state_record(hs, &mut buf);
        }

        let file = self
            .active_file
            .as_mut()
            .ok_or_else(|| io::Error::other("active WAL segment closed"))?;
        file.write_all(&buf)?;
        file.sync_data()?;
        self.active_bytes = self.active_bytes.saturating_add(buf.len() as u64);
        Ok(())
    }

    fn rotate_segment(&mut self) -> Result<(), WalError> {
        drop(self.active_file.take());
        let next_seq = self.active_seq + 1;
        let path = segment_path(&self.log_dir, next_seq);
        let mut file = self.fs.create_file(&path)?;

        let mut init_buf = Vec::with_capacity(WAL_HEADER_LEN + 33);
        init_buf.extend_from_slice(&encode_segment_header(next_seq));
        encode_hard_state_record(&self.latest_hard_state, &mut init_buf);
        file.write_all(&init_buf)?;
        file.sync_data()?;
        self.fs.sync_dir(&self.log_dir)?;

        self.active_seq = next_seq;
        self.active_bytes = init_buf.len() as u64;
        self.active_file = Some(file);
        self.segments.insert(
            next_seq,
            SegmentInfo {
                max_index: Index(0),
            },
        );
        Ok(())
    }

    fn save_snapshot_inner(&mut self, snapshot: &Snapshot) -> Result<(), WalError> {
        let name = format!(
            "{:020}-{:020}.snap",
            snapshot.meta.index.0, snapshot.meta.term.0
        );
        let tmp_name = format!("{name}.tmp");
        let tmp_path = self.snap_dir.join(&tmp_name);
        let final_path = self.snap_dir.join(&name);

        let bytes = encode_snapshot(snapshot);
        let mut file = self.fs.create_file(&tmp_path)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        drop(file);

        self.fs.rename(&tmp_path, &final_path)?;
        self.fs.sync_dir(&self.snap_dir)?;
        self.latest_snapshot_name = Some(name.clone());
        self.last_written_index = self.last_written_index.max(snapshot.meta.index);

        let retain_cutoff = Index(
            snapshot
                .meta
                .index
                .0
                .saturating_sub(self.opts.retain_entries),
        );
        let obsolete_seqs: Vec<u64> = self
            .segments
            .iter()
            .filter(|&(&seq, info)| seq < self.active_seq && info.max_index <= retain_cutoff)
            .map(|(&seq, _)| seq)
            .collect();
        if !obsolete_seqs.is_empty() {
            for seq in obsolete_seqs {
                let p = segment_path(&self.log_dir, seq);
                self.fs.remove_file(&p)?;
                self.segments.remove(&seq);
            }
            self.fs.sync_dir(&self.log_dir)?;
        }

        let mut deleted_old_snap = false;
        for existing in self.fs.read_dir(&self.snap_dir)? {
            let Some(fname) = existing.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let is_snap = existing
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("snap"));
            if is_snap && fname != name {
                self.fs.remove_file(&existing)?;
                deleted_old_snap = true;
            }
        }
        if deleted_old_snap {
            self.fs.sync_dir(&self.snap_dir)?;
        }
        Ok(())
    }
}

fn segment_path(wal_dir: &Path, seq: u64) -> PathBuf {
    wal_dir.join(format!("{seq:016x}.wal"))
}

fn cleanup_temp_files<F: Fs>(fs: &F, dir: &Path) -> Result<(), WalError> {
    let mut removed = false;
    for path in fs.read_dir(dir)? {
        if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("tmp"))
        {
            fs.remove_file(&path)?;
            removed = true;
        }
    }
    if removed {
        fs.sync_dir(dir)?;
    }
    Ok(())
}

fn load_newest_snapshot<F: Fs>(
    fs: &F,
    snap_dir: &Path,
) -> Result<(Option<Snapshot>, Option<String>), WalError> {
    let mut snaps: Vec<(String, PathBuf)> = fs
        .read_dir(snap_dir)?
        .into_iter()
        .filter_map(|p| {
            let is_snap = p
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("snap"));
            let name = p.file_name()?.to_str()?.to_string();
            if is_snap { Some((name, p)) } else { None }
        })
        .collect();
    snaps.sort_by(|a, b| a.0.cmp(&b.0));

    let Some((name, path)) = snaps.pop() else {
        return Ok((None, None));
    };
    let data = fs.read_file(&path)?;
    let snap = decode_snapshot(&data)
        .map_err(|reason| WalError::Corruption(format!("corrupt snapshot {name}: {reason}")))?;
    Ok((Some(snap), Some(name)))
}

fn list_segments<F: Fs>(fs: &F, wal_dir: &Path) -> Result<Vec<(u64, PathBuf)>, WalError> {
    let mut out = Vec::new();
    for path in fs.read_dir(wal_dir)? {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if let Some(stem) = name.strip_suffix(".wal")
            && stem.len() == 16
            && let Ok(seq) = u64::from_str_radix(stem, 16)
        {
            out.push((seq, path));
        }
    }
    Ok(out)
}
