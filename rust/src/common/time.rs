use crate::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn insertion_timestamp(insert: &Insertion) -> i64 {
    match insert.timestamp {
        Some(v) => v,
        None => SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time is negative")
            .as_millis() as i64,
    }
}

pub fn time_now_as_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is negative")
        .as_millis() as u64
}
