extern crate awscreds;
extern crate ini;
extern crate linked_hash_map;
extern crate rdkafka;
extern crate regex;
extern crate s3 as aws_s3;
extern crate threadlanes;

mod batching;
mod common;
mod kafka;
mod s3;

pub type S3StoreConfig = s3::S3StoreConfig;
pub type S3StoreReader = s3::S3StoreReader;
pub type S3StoreWriter = s3::S3StoreWriter;

pub type BatchingStoreWriterConfig = batching::BatchingStoreWriterConfig;
pub type BatchingStoreWriter<W> = batching::BatchingStoreWriter<W>;

pub type KafkaConsumerBridgeConfig = kafka::KafkaConsumerBridgeConfig;
pub type KafkaConsumerBridge<W> = kafka::KafkaConsumerBridge<W>;
pub type KafkaConsumerNumberParser = kafka::KafkaConsumerNumberParser;
pub type KafkaConsumerUtf8Parser = kafka::KafkaConsumerUtf8Parser;

/// A Key-List Store Writer.
/// Batching and nonce checking requires that a single key is bound to a single writer at any given time.
pub trait StoreWriter {
    /// create a new keyspace, returning an error on failure or if the keyspace already existed
    fn create_keyspace(&self, keyspace: &str) -> Result<CreatedKeyspace, StoreError>;
    /// append items to a list, creating a new key if necessary.
    /// in some implementations, this may be dispatched and executed asynchronously.
    fn append_list(
        &self,
        keyspace: &str,
        key: &str,
        items: Vec<Insertion>,
    ) -> Result<(), StoreError>;
    /// flush pending writes for a specific key
    fn flush_key(&self, keyspace: &str, key: &str) -> Result<(), StoreError>;
    /// flush all pending asynchronous operations
    fn flush_all(&self) -> Result<(), StoreError>;
    /// should be called periodically for implementation that require it.
    /// this will trigger scheduled operations, like flushing a pending batch.
    fn duty_cycle(&self) -> Result<(), StoreError>;
}

/// A Key-List Store Reader.
/// Many readers can read from a single key concurrently.
pub trait StoreReader {
    /// read metadata for a keyspace, returning an error if the keyspace does not exist
    fn read_keyspace_metadata(&self, keyspace: &str) -> Result<KeyspaceMetadata, StoreError>;
    /// read metadata for the given key, returning None if the key does not exist
    fn read_key_metadata(
        &self,
        keyspace: &str,
        key: &str,
    ) -> Result<Option<KeyMetadata>, StoreError>;
    /// read the next page from a list, returning an empty list if the key does not exist
    /// when no filter params and continuation is empty, it will start iterating from the first available offset.
    /// continuation can be used to read from the next page
    /// if specified, max_size determines the max elements to be returned, otherwise a configured default is used
    /// filter opations can be specified to start reading from an item other than the first available offset
    fn read_page(
        &self,
        keyspace: &str,
        key: &str,
        order: IterationOrder,
        max_size: Option<u64>,
        filter: Option<ItemFilter>,
        continuation: Option<String>,
    ) -> Result<ItemList, StoreError>;
}

#[derive(Debug, Clone)]
pub enum IterationOrder {
    Forwards,
    Backwards,
}

#[derive(Debug)]
pub enum StoreError {
    /// IO error communicating with underlying storage, likely retryable
    IOError(String),
    // Underlying data has an issue, likely not retryable
    BadData(String),
    // Bad configuration
    BadConfiguration(String),
    // Invalid continuation token
    InvalidContinuation(String),
    // Keyspace already existed
    KeyspaceAlreadyExists,
    // Keyspace not found
    KeyspaceNotFound,
}
impl ToString for StoreError {
    fn to_string(&self) -> String {
        match self {
            StoreError::IOError(s) => format!("IOError({})", s),
            StoreError::BadData(s) => format!("BadData({})", s),
            StoreError::BadConfiguration(s) => format!("BadConfiguration({})", s),
            StoreError::InvalidContinuation(s) => format!("InvalidContinuation({})", s),
            StoreError::KeyspaceAlreadyExists => "KeyspaceAlreadyExists".to_string(),
            StoreError::KeyspaceNotFound => "KeyspaceNotFound".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreatedKeyspace {
    pub keyspace: String,
}

#[derive(Debug, Clone)]
pub struct KeyspaceMetadata {
    pub created_timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct KeyMetadata {
    pub next_offset: u64,
    pub next_nonce: u128,
}

#[derive(Debug)]
pub struct Insertion {
    pub value: Vec<u8>,
    pub nonce: Option<u128>,
    pub timestamp: Option<i64>,
}
#[derive(Debug)]
pub struct AppendList {
    pub keyspace: String,
    pub key: String,
    pub items: Vec<Insertion>,
}

#[derive(Debug, Clone)]
pub struct ItemFilter {
    pub start_offset: Option<u64>,
    pub start_timestamp: Option<i64>,
    pub start_nonce: Option<u128>,
}
#[derive(Debug, Clone)]
pub struct Item {
    pub offset: u64,
    pub timestamp: i64,
    pub nonce: Option<u128>,
    pub value: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct ItemList {
    pub keyspace: String,
    pub key: String,
    pub items: Vec<Item>,
    pub continuation: Option<String>,
    pub stats: ListStats,
}

#[derive(Debug, Clone)]
pub struct ListStats {
    pub list_operation_count: u64,
    pub read_operation_count: u64,
    pub read_size_total: u64,
    pub continuation_miss_count: u64,
}
