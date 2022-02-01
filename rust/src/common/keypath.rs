use crate::common::buffer::*;
use crate::common::items::DefaultedItemFilter;
use crate::{IterationOrder, KeyMetadata, StoreError};
use regex::Regex;

#[derive(Clone, Debug)]
pub struct Watermark {
    pub offset: u64,
}
impl Watermark {
    pub fn new(offset: u64) -> Self {
        Self { offset }
    }
    pub fn path(root_prefix: &str, keyspace: &str, key: &str) -> String {
        format!("{}{}/{}/watermark", root_prefix, keyspace, key)
    }
    pub fn from(buffer: &Vec<u8>) -> Result<Watermark, StoreError> {
        let offset = read_u64(&buffer, 0)?;
        return Ok(Watermark { offset });
    }
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::new();
        append_u64(&mut buf, self.offset);
        return buf;
    }
    pub fn start_from(&self, root_prefix: &str, keyspace: &str, key: &str) -> String {
        format!(
            "{}{}/{}/data_o{:0>20}-",
            root_prefix, keyspace, key, &self.offset,
        )
    }
}

pub struct KeyspacePath {}
impl KeyspacePath {
    pub fn config_path(root_prefix: &str, keyspace: &str) -> String {
        format!("{}{}_config.ini", root_prefix, keyspace)
    }
    // pub fn prefix(root_prefix: &str, keyspace: &str) -> String {
    //     format!("{}{}/", root_prefix, keyspace)
    // }
}

#[derive(Debug, Clone)]
pub struct KeyPath {
    pub first_offset: u64,
    pub last_offset: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub first_nonce: u128,
    pub next_nonce: u128,
    pub size: u64,
    pub prior_start_offset: u64,
}
impl KeyPath {
    pub fn to_path(&self, root_prefix: &str, keyspace: &str, key: &str) -> String {
        format!(
            "{}{}/{}/data_o{:0>20}-o{}_t{}-t{}_n{}-n{}_s{}_p{}.bin",
            root_prefix,
            keyspace,
            key,
            &self.first_offset,
            &self.last_offset,
            &self.min_timestamp,
            &self.max_timestamp,
            &self.first_nonce,
            &self.next_nonce,
            &self.size,
            &self.prior_start_offset,
        )
    }
    // pub fn prefix(root_prefix: &str, keyspace: &str, key: &str) -> String {
    //     format!("{}{}/{}/", root_prefix, keyspace, key)
    // }
    pub fn prefix_data_only(root_prefix: &str, keyspace: &str, key: &str) -> String {
        format!("{}{}/{}/data_", root_prefix, keyspace, key)
    }
    pub fn watermark_prefix(
        root_prefix: &str,
        keyspace: &str,
        key: &str,
        watermark: &Watermark,
    ) -> String {
        format!(
            "{}{}/{}/data_o{:0>20}",
            root_prefix, keyspace, key, watermark.offset
        )
    }
    pub fn after_watermark_prefix(
        root_prefix: &str,
        keyspace: &str,
        key: &str,
        watermark: &Watermark,
    ) -> String {
        KeyPath::after_offset_prefix(root_prefix, keyspace, key, watermark.offset)
    }
    pub fn after_offset_prefix(
        root_prefix: &str,
        keyspace: &str,
        key: &str,
        offset: u64,
    ) -> String {
        if offset == 0 {
            format!("{}{}/{}/data_o", root_prefix, keyspace, key)
        } else {
            format!(
                "{}{}/{}/data_o{:0>20}",
                root_prefix,
                keyspace,
                key,
                offset + 1
            )
        }
    }
    pub fn to_metadata(&self) -> KeyMetadata {
        KeyMetadata {
            next_offset: self.last_offset + 1,
            next_nonce: self.next_nonce,
        }
    }
    pub fn matches(&self, filter: &DefaultedItemFilter) -> bool {
        match filter.order {
            IterationOrder::Forwards => {
                return filter.start_offset <= self.last_offset
                    && filter.start_nonce < self.next_nonce
                    && filter.start_timestamp <= self.max_timestamp;
            }
            IterationOrder::Backwards => {
                return filter.start_offset >= self.first_offset
                    && filter.start_nonce >= self.first_nonce
                    && filter.start_timestamp >= self.min_timestamp;
            }
        }
    }
}

pub struct KeyPathParser {
    rex: Regex,
}
impl KeyPathParser {
    pub fn new() -> Self {
        Self {
            rex: Regex::new(
                r"/data_o(\d+)-o(\d+)_t(-?\d+)-t(-?\d+)_n(\d+)-n(\d+)_s(\d+)_p(\d+)\.bin$",
            )
            .unwrap(),
        }
    }
    pub fn parse(&self, path: &str) -> Option<KeyPath> {
        match self.rex.captures(path) {
            None => None,
            Some(cap) => Some(KeyPath {
                first_offset: cap[1].parse::<u64>().unwrap(),
                last_offset: cap[2].parse::<u64>().unwrap(),
                min_timestamp: cap[3].parse::<i64>().unwrap(),
                max_timestamp: cap[4].parse::<i64>().unwrap(),
                first_nonce: cap[5].parse::<u128>().unwrap(),
                next_nonce: cap[6].parse::<u128>().unwrap(),
                size: cap[7].parse::<u64>().unwrap(),
                prior_start_offset: cap[8].parse::<u64>().unwrap(),
            }),
        }
    }
    pub fn parse_or_error(&self, path: &str) -> Result<KeyPath, StoreError> {
        match self.parse(path) {
            None => return Err(StoreError::BadData(format!("invalid key path {}", path))),
            Some(v) => return Ok(v),
        };
    }
}
