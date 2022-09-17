use crate::common::buffer::*;
use crate::*;
use std::cmp::{max, min};

pub struct SerializedInsertion {
    pub first_insert_offset: u64,
    pub last_insert_offset: u64,
    pub next_offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub buffer: Vec<u8>,
}

pub fn serialize_insertion(inserts: &Vec<&Insertion>, next_offset: u64) -> SerializedInsertion {
    let mut buffer: Vec<u8> = Vec::new();
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let first_insert_offset = next_offset;
    let mut cur_offset = first_insert_offset;
    for insert in (&inserts).iter() {
        let timestamp = super::time::insertion_timestamp(&insert);
        let nonce = match insert.nonce {
            None => u128::MAX,
            Some(v) => v,
        };
        min_timestamp = min(min_timestamp, timestamp);
        max_timestamp = max(max_timestamp, timestamp);
        append_u64(&mut buffer, cur_offset);
        append_i64(&mut buffer, timestamp);
        append_u128(&mut buffer, nonce);
        append_u32(&mut buffer, insert.record.len() as u32);
        append_buffer(&mut buffer, &insert.record);
        append_u32(&mut buffer, 36 + insert.record.len() as u32);
        cur_offset += 1;
    }
    SerializedInsertion {
        first_insert_offset,
        last_insert_offset: cur_offset - 1,
        next_offset: cur_offset,
        min_timestamp,
        max_timestamp,
        buffer,
    }
}

pub struct RecordFilter {
    pub defined: bool,
    pub max_size: u64,
    pub start_offset: u64,
    pub start_timestamp: i64,
    pub start_nonce: u128,
    pub direction: Direction,
}
impl RecordFilter {
    pub fn from(
        position: &StartPosition,
        max_size: u64,
        direction: Direction,
    ) -> RecordFilter {
        match direction {
            Direction::Forwards => match position {
                StartPosition::Offset(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: v.clone(),
                    start_timestamp: i64::MIN,
                    start_nonce: u128::MIN,
                    direction,
                },
                StartPosition::Nonce(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: u64::MIN,
                    start_timestamp: i64::MIN,
                    start_nonce: v.clone(),
                    direction,
                },
                StartPosition::Timestamp(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: u64::MIN,
                    start_timestamp: v.clone(),
                    start_nonce: u128::MIN,
                    direction,
                },
                StartPosition::First => RecordFilter {
                    defined: false,
                    max_size,
                    start_offset: u64::MIN,
                    start_timestamp: i64::MIN,
                    start_nonce: u128::MIN,
                    direction,
                },
            },
            Direction::Backwards => match position {
                StartPosition::Offset(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: v.clone(),
                    start_timestamp: i64::MAX,
                    start_nonce: u128::MAX,
                    direction,
                },
                StartPosition::Nonce(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: u64::MAX,
                    start_timestamp: i64::MAX,
                    start_nonce: v.clone(),
                    direction,
                },
                StartPosition::Timestamp(v) => RecordFilter {
                    defined: true,
                    max_size,
                    start_offset: u64::MAX,
                    start_timestamp: v.clone(),
                    start_nonce: u128::MAX,
                    direction,
                },
                StartPosition::First => RecordFilter {
                    defined: false,
                    max_size,
                    start_offset: u64::MAX,
                    start_timestamp: i64::MAX,
                    start_nonce: u128::MAX,
                    direction,
                },
            },
        }
    }
    pub fn for_offset(
        start_offset: u64,
        max_size: u64,
        direction: Direction,
    ) -> RecordFilter {
        match direction {
            Direction::Forwards => RecordFilter {
                defined: true,
                max_size,
                start_offset: start_offset,
                start_timestamp: i64::MIN,
                start_nonce: u128::MIN,
                direction,
            },
            Direction::Backwards => RecordFilter {
                defined: true,
                max_size,
                start_offset: start_offset,
                start_timestamp: i64::MAX,
                start_nonce: u128::MAX,
                direction,
            },
        }
    }
}

fn record_in_range(
    header: &RecordHeader,
    filter: &RecordFilter,
    found_first_match: bool,
) -> bool {
    match filter.direction {
        Direction::Forwards => {
            if header.offset < filter.start_offset {
                return false;
            }
            if header.timestamp < filter.start_timestamp {
                return false;
            }
            match header.nonce {
                None => {
                    if filter.defined && !found_first_match {
                        return false;
                    }
                }
                Some(record_nonce) => {
                    if record_nonce < filter.start_nonce {
                        return false;
                    }
                }
            }
        }
        Direction::Backwards => {
            if header.offset > filter.start_offset {
                return false;
            }
            if header.timestamp > filter.start_timestamp {
                return false;
            }
            match header.nonce {
                None => {
                    if filter.defined && !found_first_match {
                        return false;
                    }
                }
                Some(record_nonce) => {
                    if record_nonce > filter.start_nonce {
                        return false;
                    }
                }
            }
        }
    }

    return true;
}

pub fn deserialize_and_filter_records(
    buffer: &Vec<u8>,
    records: &mut Vec<Record>,
    filter: &RecordFilter,
    continuation_offset: u64,
) -> Result<bool, StoreError> {
    match filter.direction {
        Direction::Forwards => {
            let mut pos: usize = 0;
            while pos < buffer.len() && (records.len() as u64) < filter.max_size {
                // deserialize header and check if it's in range
                let header = RecordHeader::deserialize(buffer, pos)?;
                pos += RecordHeader::SIZE;
                if header.offset >= continuation_offset
                    && record_in_range(&header, filter, !records.is_empty())
                {
                    // matching, add to records
                    let value = read_bytes_copy(buffer, pos, header.length as usize);
                    records.push(Record {
                        offset: header.offset,
                        timestamp: header.timestamp,
                        nonce: header.nonce,
                        value,
                    });
                }
                pos += header.length as usize;
                pos += 4; // trailing total length field
            }
            // return if read fully
            return Ok(pos == buffer.len());
        }
        Direction::Backwards => {
            let mut pos: usize = buffer.len();
            while pos > 0 && (records.len() as u64) < filter.max_size {
                // read total length of trailing record
                pos -= 4;
                let total_length = read_u32(buffer, pos)?;
                pos -= total_length as usize;
                // deserialize header and check if it's in range
                let header = RecordHeader::deserialize(buffer, pos)?;
                if header.offset <= continuation_offset
                    && record_in_range(&header, filter, !records.is_empty())
                {
                    // matching, add to records
                    let value = read_bytes_copy(buffer, pos, header.length as usize);
                    records.push(Record {
                        offset: header.offset,
                        timestamp: header.timestamp,
                        nonce: header.nonce,
                        value,
                    });
                }
            }
            // return if read fully
            return Ok(pos == 0);
        }
    }
}

#[derive(Debug)]
struct RecordHeader {
    pub offset: u64,
    pub timestamp: i64,
    pub nonce: Option<u128>,
    pub length: u32,
}
impl RecordHeader {
    const SIZE: usize = 36;
    fn deserialize(buffer: &[u8], mut pos: usize) -> Result<Self, StoreError> {
        let offset = read_u64(buffer, pos)?;
        pos += 8;
        let timestamp = read_i64(buffer, pos)?;
        pos += 8;
        let nonce = match read_u128(buffer, pos)? {
            u128::MAX => None,
            v => Some(v),
        };
        pos += 16;
        let length = read_u32(buffer, pos)?;
        Ok(Self {
            offset,
            timestamp,
            nonce,
            length,
        })
    }
}

pub struct NonceFilterResult<'a> {
    pub records: Vec<&'a Insertion>,
    pub first_nonce: Option<u128>,
    pub first_potential_nonce: u128,
    pub next_nonce: u128,
}

pub fn nonce_filter<'a>(records: &'a Vec<Insertion>, next_nonce: u128) -> NonceFilterResult<'a> {
    let mut first_nonce: Option<u128> = None;
    let first_potential_nonce = next_nonce;
    let mut next_nonce = next_nonce;
    let records: Vec<&Insertion> = (&records)
        .iter()
        .filter(|e| match e.nonce {
            Some(nonce) => {
                if nonce >= next_nonce {
                    if let None = first_nonce {
                        first_nonce = Some(nonce);
                    }
                    next_nonce = nonce + 1;
                    true
                } else {
                    false
                }
            }
            None => true,
        })
        .collect();
    NonceFilterResult {
        records,
        first_nonce,
        first_potential_nonce,
        next_nonce,
    }
}
