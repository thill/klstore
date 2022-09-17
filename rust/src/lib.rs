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

/// A Key-Log Store Writer.
/// Batching and nonce checking requires that a single key is bound to a single writer at any given time.
pub trait StoreWriter {
    /// create a new keyspace, returning an error on failure or if the keyspace already existed
    fn create_keyspace(&self, keyspace: &str) -> Result<CreatedKeyspace, StoreError>;
    /// append records to a log, creating a new key if necessary.
    /// in some implementations, this may be dispatched and executed asynchronously.
    fn append(&self, keyspace: &str, key: &str, inserts: Vec<Insertion>) -> Result<(), StoreError>;
    /// flush pending writes for a specific key
    fn flush_key(&self, keyspace: &str, key: &str) -> Result<(), StoreError>;
    /// flush all pending asynchronous operations
    fn flush_all(&self) -> Result<(), StoreError>;
    /// should be called periodically for implementation that require it.
    /// this will trigger scheduled operations, like flushing a pending batch.
    fn duty_cycle(&self) -> Result<(), StoreError>;
}

/// A Key-Log Store Reader.
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
    /// read the first page of a log, returning an empty log if the key does not exist.
    /// the page will begin iteration from the given StartPosition, advancing in the given Direction
    /// if specified, page_size determines the max records to be returned, otherwise a configured default is used.
    /// the Page result will contain an optional continuation token that can be passed to the read_next_page function.
    fn read_first_page(
        &self,
        keyspace: &str,
        key: &str,
        direction: Direction,
        start: StartPosition,
        page_size: Option<u64>,
    ) -> Result<Page, StoreError>;
    /// read the next page of a log based on the given continuation token.
    /// if specified, page_size determines the max records to be returned, otherwise a configured default is used.
    fn read_next_page(
        &self,
        keyspace: &str,
        key: &str,
        continuation: String,
        page_size: Option<u64>,
    ) -> Result<Page, StoreError>;
}

#[derive(Debug, Clone)]
pub enum Direction {
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
    pub record: Vec<u8>,
    pub nonce: Option<u128>,
    pub timestamp: Option<i64>,
}
#[derive(Debug)]
pub struct Append {
    pub keyspace: String,
    pub key: String,
    pub records: Vec<Insertion>,
}

#[derive(Debug)]
pub enum StartPosition {
    First,
    Nonce(u128),
    Timestamp(i64),
    Offset(u64),
}
#[derive(Debug, Clone)]
pub struct Record {
    pub offset: u64,
    pub timestamp: i64,
    pub nonce: Option<u128>,
    pub value: Vec<u8>,
}
#[derive(Debug, Clone)]
pub struct Page {
    pub keyspace: String,
    pub key: String,
    pub records: Vec<Record>,
    pub continuation: Option<String>,
}
