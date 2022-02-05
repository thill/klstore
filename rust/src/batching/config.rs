use crate::StoreError;
use ini::Ini;
use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct BatchingStoreWriterConfig {
    pub writer_thread_count: usize,
    pub writer_thread_queue_capacity: Option<usize>,
    pub batch_check_interval_millis: u64,
    pub batch_flush_interval_millis: u64,
    pub batch_flush_item_count_threshold: u64,
    pub batch_flush_size_threshold: u64,
}
impl BatchingStoreWriterConfig {
    pub fn new() -> Self {
        Self {
            writer_thread_count: 1,
            writer_thread_queue_capacity: None,         // unbound
            batch_check_interval_millis: 100,           // 100 milliseconds
            batch_flush_interval_millis: 1000,          // 1 second
            batch_flush_item_count_threshold: u64::MAX, // unbound
            batch_flush_size_threshold: 1024 * 1024,    // 1MB
        }
    }
    /// set the number of writer threads. defaults to 1.
    /// each key will always be written by the same thread, so consider number of keys when determining the number of threads to use.
    pub fn set_writer_thread_count(mut self, v: usize) -> Self {
        self.writer_thread_count = v;
        self
    }
    /// set the capacity of the queue for each writer thread. defaults to unbound.
    /// this is the maximum number of writes that can be queued per thread.
    /// consider this parameter, a typical write size, and the number of writer threads to determine memory requirements.
    /// when set to None, there will be no capacity limitations.
    pub fn set_writer_thread_queue_capacity(mut self, v: Option<usize>) -> Self {
        self.writer_thread_queue_capacity = v;
        self
    }
    /// set the interval at which to check to flush batches. defaults to 100 milliseconds.
    pub fn set_batch_check_interval_millis(mut self, v: u64) -> Self {
        self.batch_check_interval_millis = v;
        self
    }
    /// set the interval at which to flush batches, regardless of size. defaults to 1 second.
    /// this interval plus processing time is the maximum additional delay introduced by batching.
    pub fn set_batch_flush_interval_millis(mut self, v: u64) -> Self {
        self.batch_flush_interval_millis = v;
        self
    }
    /// flush a batch upon reaching a specific item count. defaults to u64::MAX items.
    pub fn set_batch_flush_item_count_threshold(mut self, v: u64) -> Self {
        self.batch_flush_item_count_threshold = v;
        self
    }
    /// flush a batch upon reaching a specific batch size. defaults to 1MB.
    /// each key builds a separate batch, so this parameter and flush_interval+throughput will help determine memory requirements.
    pub fn set_batch_flush_size_threshold(mut self, v: u64) -> Self {
        self.batch_flush_size_threshold = v;
        self
    }
    pub fn load(ini: &Ini) -> Result<Self, StoreError> {
        let s3 = match ini.section(Some("s3")) {
            None => {
                return Err(StoreError::BadConfiguration(
                    "[s3] config missing".to_string(),
                ))
            }
            Some(v) => v,
        };
        let mut cfg = Self::new();

        // pub writer_thread_count: ,
        // pub : Option<usize>,
        // pub : u64,
        // pub : u64,
        // pub : u64,
        // pub : u64,

        if let Some(v) = s3.get("writer_thread_count") {
            match usize::from_str(v) {
                Ok(v) => cfg = cfg.set_writer_thread_count(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 writer_thread_count".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("writer_thread_queue_capacity") {
            match usize::from_str(v) {
                Ok(v) => cfg = cfg.set_writer_thread_queue_capacity(Some(v)),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 writer_thread_queue_capacity".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("batch_check_interval_millis") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_batch_check_interval_millis(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 batch_check_interval_millis".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("batch_flush_interval_millis") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_batch_flush_interval_millis(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 batch_flush_interval_millis".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("batch_flush_item_count_threshold") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_batch_flush_item_count_threshold(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 batch_flush_item_count_threshold".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("batch_flush_size_threshold") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_batch_flush_size_threshold(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 batch_flush_size_threshold".to_string(),
                    ))
                }
            }
        }

        return Ok(cfg);
    }
}
