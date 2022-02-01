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

pub fn serialize_insertion(items: &Vec<&Insertion>, next_offset: u64) -> SerializedInsertion {
    let mut buffer: Vec<u8> = Vec::new();
    let mut min_timestamp = i64::MAX;
    let mut max_timestamp = i64::MIN;
    let first_insert_offset = next_offset;
    let mut cur_offset = first_insert_offset;
    for item in (&items).iter() {
        let timestamp = super::time::insertion_timestamp(&item);
        let nonce = match item.nonce {
            None => u128::MAX,
            Some(v) => v,
        };
        min_timestamp = min(min_timestamp, timestamp);
        max_timestamp = max(max_timestamp, timestamp);
        append_u64(&mut buffer, cur_offset);
        append_i64(&mut buffer, timestamp);
        append_u128(&mut buffer, nonce);
        append_u32(&mut buffer, item.value.len() as u32);
        append_buffer(&mut buffer, &item.value);
        append_u32(&mut buffer, 36 + item.value.len() as u32);
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

pub struct DefaultedItemFilter {
    pub max_size: u64,
    pub start_offset: u64,
    pub start_timestamp: i64,
    pub start_nonce: u128,
    pub nonce_defined: bool,
    pub order: IterationOrder,
}
impl DefaultedItemFilter {
    pub fn from(
        filter: &Option<ItemFilter>,
        max_size: u64,
        order: IterationOrder,
    ) -> DefaultedItemFilter {
        match order {
            IterationOrder::Forwards => match filter {
                Some(v) => DefaultedItemFilter {
                    max_size,
                    start_offset: match v.start_offset {
                        Some(v) => v,
                        None => u64::MIN,
                    },
                    start_timestamp: match v.start_timestamp {
                        Some(v) => v,
                        None => i64::MIN,
                    },
                    start_nonce: match v.start_nonce {
                        Some(v) => v,
                        None => u128::MIN,
                    },
                    nonce_defined: true,
                    order,
                },
                None => DefaultedItemFilter {
                    max_size,
                    start_offset: 0,
                    start_timestamp: i64::MIN,
                    start_nonce: u128::MIN,
                    nonce_defined: false,
                    order,
                },
            },
            IterationOrder::Backwards => match filter {
                Some(v) => DefaultedItemFilter {
                    max_size,
                    start_offset: match v.start_offset {
                        Some(v) => v,
                        None => u64::MAX,
                    },
                    start_timestamp: match v.start_timestamp {
                        Some(v) => v,
                        None => i64::MAX,
                    },
                    start_nonce: match v.start_nonce {
                        Some(v) => v,
                        None => u128::MAX,
                    },
                    nonce_defined: true,
                    order,
                },
                None => DefaultedItemFilter {
                    max_size,
                    start_offset: u64::MAX,
                    start_timestamp: i64::MAX,
                    start_nonce: u128::MAX,
                    nonce_defined: false,
                    order,
                },
            },
        }
    }
}

fn item_in_range(
    header: &ItemHeader,
    filter: &DefaultedItemFilter,
    found_first_match: bool,
) -> bool {
    match filter.order {
        IterationOrder::Forwards => {
            if header.offset < filter.start_offset {
                return false;
            }
            if header.timestamp < filter.start_timestamp {
                return false;
            }
            match header.nonce {
                None => {
                    if filter.nonce_defined && !found_first_match {
                        return false;
                    }
                }
                Some(item_nonce) => {
                    if item_nonce < filter.start_nonce {
                        return false;
                    }
                }
            }
        }
        IterationOrder::Backwards => {
            if header.offset > filter.start_offset {
                return false;
            }
            if header.timestamp > filter.start_timestamp {
                return false;
            }
            match header.nonce {
                None => {
                    if filter.nonce_defined && !found_first_match {
                        return false;
                    }
                }
                Some(item_nonce) => {
                    if item_nonce > filter.start_nonce {
                        return false;
                    }
                }
            }
        }
    }

    return true;
}

pub fn deserialize_and_filter_items(
    buffer: &Vec<u8>,
    items: &mut Vec<Item>,
    filter: &DefaultedItemFilter,
    continuation_offset: u64,
) -> Result<bool, StoreError> {
    match filter.order {
        IterationOrder::Forwards => {
            let mut pos: usize = 0;
            while pos < buffer.len() && (items.len() as u64) < filter.max_size {
                // deserialize header and check if it's in range
                let header = ItemHeader::deserialize(buffer, pos)?;
                pos += ItemHeader::SIZE;
                if header.offset >= continuation_offset
                    && item_in_range(&header, filter, !items.is_empty())
                {
                    // matching, add to items
                    let value = read_bytes_copy(buffer, pos, header.length as usize);
                    items.push(Item {
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
        IterationOrder::Backwards => {
            let mut pos: usize = buffer.len();
            while pos > 0 && (items.len() as u64) < filter.max_size {
                // read total length of trailing item
                pos -= 4;
                let total_length = read_u32(buffer, pos)?;
                pos -= total_length as usize;
                // deserialize header and check if it's in range
                let header = ItemHeader::deserialize(buffer, pos)?;
                if header.offset <= continuation_offset
                    && item_in_range(&header, filter, !items.is_empty())
                {
                    // matching, add to items
                    let value = read_bytes_copy(buffer, pos, header.length as usize);
                    items.push(Item {
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
struct ItemHeader {
    pub offset: u64,
    pub timestamp: i64,
    pub nonce: Option<u128>,
    pub length: u32,
}
impl ItemHeader {
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
    pub items: Vec<&'a Insertion>,
    pub first_nonce: Option<u128>,
    pub first_potential_nonce: u128,
    pub next_nonce: u128,
}

pub fn nonce_filter<'a>(items: &'a Vec<Insertion>, next_nonce: u128) -> NonceFilterResult<'a> {
    let mut first_nonce: Option<u128> = None;
    let first_potential_nonce = next_nonce;
    let mut next_nonce = next_nonce;
    let items: Vec<&Insertion> = (&items)
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
        items,
        first_nonce,
        first_potential_nonce,
        next_nonce,
    }
}
