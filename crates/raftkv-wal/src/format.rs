//! Binary wire framing for WAL segment headers, records, and snapshot files.

use bytes::Bytes;
use raft_core::{Entry, HardState, Index, NodeId, Payload, Snapshot, SnapshotMeta, Term};

pub(crate) const WAL_MAGIC: &[u8; 8] = b"RKVWAL01";
pub(crate) const SNAP_MAGIC: &[u8; 8] = b"RKVSNP01";
pub(crate) const WAL_HEADER_LEN: usize = 16;
pub(crate) const MAX_RECORD_PAYLOAD: usize = 64 * 1024 * 1024;

const KIND_ENTRY: u8 = 1;
const KIND_HARD_STATE: u8 = 2;

const PAYLOAD_NOOP: u8 = 0;
const PAYLOAD_COMMAND: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Record {
    Entry(Entry),
    HardState(HardState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DecodeFrameError {
    Truncated,
    Invalid(&'static str),
}

pub(crate) fn encode_segment_header(seq: u64) -> [u8; WAL_HEADER_LEN] {
    let mut hdr = [0u8; WAL_HEADER_LEN];
    hdr[..8].copy_from_slice(WAL_MAGIC);
    hdr[8..16].copy_from_slice(&seq.to_le_bytes());
    hdr
}

pub(crate) fn decode_segment_header(bytes: &[u8]) -> Result<u64, DecodeFrameError> {
    if bytes.len() < WAL_HEADER_LEN {
        return Err(DecodeFrameError::Truncated);
    }
    if &bytes[..8] != WAL_MAGIC {
        return Err(DecodeFrameError::Invalid("bad WAL segment magic"));
    }
    let mut seq_bytes = [0u8; 8];
    seq_bytes.copy_from_slice(&bytes[8..16]);
    Ok(u64::from_le_bytes(seq_bytes))
}

pub(crate) fn encode_entry_record(entry: &Entry, out: &mut Vec<u8>) {
    let data_len = match &entry.payload {
        Payload::Noop => 0,
        Payload::Command(b) => b.len(),
    };
    let payload_len = (17 + data_len) as u32;
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.push(KIND_ENTRY);
    out.extend_from_slice(&entry.term.0.to_le_bytes());
    out.extend_from_slice(&entry.index.0.to_le_bytes());
    match &entry.payload {
        Payload::Noop => out.push(PAYLOAD_NOOP),
        Payload::Command(b) => {
            out.push(PAYLOAD_COMMAND);
            out.extend_from_slice(b);
        }
    }
    let crc = crc32fast::hash(&out[start + 4..]);
    out[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

pub(crate) fn encode_hard_state_record(hs: &HardState, out: &mut Vec<u8>) {
    let payload_len: u32 = 24;
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&payload_len.to_le_bytes());
    out.push(KIND_HARD_STATE);
    out.extend_from_slice(&hs.term.0.to_le_bytes());
    let voted = hs.voted_for.map_or(0, |id| id.0);
    out.extend_from_slice(&voted.to_le_bytes());
    out.extend_from_slice(&hs.commit.0.to_le_bytes());
    let crc = crc32fast::hash(&out[start + 4..]);
    out[start..start + 4].copy_from_slice(&crc.to_le_bytes());
}

pub(crate) fn decode_record(buf: &[u8]) -> Result<(Record, usize), DecodeFrameError> {
    if buf.len() < 9 {
        return Err(DecodeFrameError::Truncated);
    }
    let crc = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let payload_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
    if payload_len > MAX_RECORD_PAYLOAD {
        return Err(DecodeFrameError::Invalid("record payload exceeds 64 MiB"));
    }
    let frame_len = 9 + payload_len;
    if buf.len() < frame_len {
        return Err(DecodeFrameError::Truncated);
    }
    let actual_crc = crc32fast::hash(&buf[4..frame_len]);
    if crc != actual_crc {
        return Err(DecodeFrameError::Invalid("record CRC mismatch"));
    }

    let kind = buf[8];
    let payload = &buf[9..frame_len];
    let record = match kind {
        KIND_ENTRY => {
            if payload.len() < 17 {
                return Err(DecodeFrameError::Invalid("short entry record"));
            }
            let term = Term(u64::from_le_bytes(read_8(&payload[0..8])));
            let index = Index(u64::from_le_bytes(read_8(&payload[8..16])));
            let pkind = payload[16];
            let pdata = &payload[17..];
            let entry_payload = match pkind {
                PAYLOAD_NOOP => {
                    if !pdata.is_empty() {
                        return Err(DecodeFrameError::Invalid("non-empty noop payload"));
                    }
                    Payload::Noop
                }
                PAYLOAD_COMMAND => Payload::Command(Bytes::copy_from_slice(pdata)),
                _ => return Err(DecodeFrameError::Invalid("unknown entry payload kind")),
            };
            Record::Entry(Entry {
                term,
                index,
                payload: entry_payload,
            })
        }
        KIND_HARD_STATE => {
            if payload.len() != 24 {
                return Err(DecodeFrameError::Invalid("invalid hard state length"));
            }
            let term = Term(u64::from_le_bytes(read_8(&payload[0..8])));
            let voted_raw = u64::from_le_bytes(read_8(&payload[8..16]));
            let voted_for = (voted_raw != 0).then_some(NodeId(voted_raw));
            let commit = Index(u64::from_le_bytes(read_8(&payload[16..24])));
            Record::HardState(HardState {
                term,
                voted_for,
                commit,
            })
        }
        _ => return Err(DecodeFrameError::Invalid("unknown record kind")),
    };

    Ok((record, frame_len))
}

pub(crate) fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let data_len = snapshot.data.len() as u64;
    let mut out = Vec::with_capacity(36 + snapshot.data.len());
    out.extend_from_slice(SNAP_MAGIC);
    out.extend_from_slice(&snapshot.meta.index.0.to_le_bytes());
    out.extend_from_slice(&snapshot.meta.term.0.to_le_bytes());
    out.extend_from_slice(&data_len.to_le_bytes());
    out.extend_from_slice(&snapshot.data);
    let crc = crc32fast::hash(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

pub(crate) fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, &'static str> {
    if bytes.len() < 36 {
        return Err("snapshot shorter than header + CRC");
    }
    if &bytes[..8] != SNAP_MAGIC {
        return Err("bad snapshot magic");
    }
    let index = Index(u64::from_le_bytes(read_8(&bytes[8..16])));
    let term = Term(u64::from_le_bytes(read_8(&bytes[16..24])));
    let data_len = u64::from_le_bytes(read_8(&bytes[24..32])) as usize;
    if bytes.len() != 36 + data_len {
        return Err("snapshot length mismatch");
    }
    let expected_crc = u32::from_le_bytes([
        bytes[32 + data_len],
        bytes[33 + data_len],
        bytes[34 + data_len],
        bytes[35 + data_len],
    ]);
    let actual_crc = crc32fast::hash(&bytes[..32 + data_len]);
    if expected_crc != actual_crc {
        return Err("snapshot CRC mismatch");
    }
    Ok(Snapshot {
        meta: SnapshotMeta { index, term },
        data: Bytes::copy_from_slice(&bytes[32..32 + data_len]),
    })
}

fn read_8(slice: &[u8]) -> [u8; 8] {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(slice);
    arr
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn record_strategy() -> impl Strategy<Value = Record> {
        let entry_strat = (
            any::<u64>(),
            any::<u64>(),
            prop_oneof![
                Just(Payload::Noop),
                proptest::collection::vec(any::<u8>(), 0..512)
                    .prop_map(|v| Payload::Command(Bytes::from(v))),
            ],
        )
            .prop_map(|(term, index, payload)| {
                Record::Entry(Entry {
                    term: Term(term),
                    index: Index(index),
                    payload,
                })
            });

        let hs_strat = (
            any::<u64>(),
            proptest::option::of(1u64..=1000).prop_map(|o| o.map(NodeId)),
            any::<u64>(),
        )
            .prop_map(|(term, voted_for, commit)| {
                Record::HardState(HardState {
                    term: Term(term),
                    voted_for,
                    commit: Index(commit),
                })
            });

        prop_oneof![entry_strat, hs_strat]
    }

    proptest! {
        #[test]
        fn record_codec_round_trips(records in proptest::collection::vec(record_strategy(), 1..40)) {
            let mut buf = Vec::new();
            for rec in &records {
                match rec {
                    Record::Entry(e) => encode_entry_record(e, &mut buf),
                    Record::HardState(hs) => encode_hard_state_record(hs, &mut buf),
                }
            }

            let mut off = 0usize;
            let mut decoded = Vec::new();
            while off < buf.len() {
                let (rec, len) = decode_record(&buf[off..]).unwrap();
                decoded.push(rec);
                off += len;
            }
            prop_assert_eq!(off, buf.len());
            prop_assert_eq!(decoded, records);
        }
    }
}
