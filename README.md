# klstore

Appendable and iterable key/log storage, backed by S3.


## General Overview

Per key, a single writer appends to underlying storage, enabling many concurrent readers to iterate through pages of records.
Writers are able to perform batching and nonce checking to enable high-throughput, exactly-once persistence of data.
Readers are stateless and able to deterministically page backwards and forwards through a log, which is uniquely identified by a key per keyspace.
Readers can begin iterating from any point in a log, searchable by either offset, timestamp, or nonce.


## Terminology

| Term       | Description
|------------|------------
| `keyspace` | A collection of keys
| `key`      | A unique key in a keyspace
| `log`      | An iterable collection of records, identified by a key
| `offset`   | The position of a single record in a log
| `nonce`    | An ever-growing number per key, used for de-duplication of records


## Rust API

### Reader

The `StoreReader` trait expresses the API around reading from an S3-backed key/log store:
```rust
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
```

A returned page contains contains a vector of records. Iteration can continue using the returned `continuation_token`:
```rust
pub struct Page {
    pub keyspace: String,
    pub key: String,
    pub records: Vec<Record>,
    pub continuation: Option<String>,
}
```

### Writer

The `StoreWriter` trait expresses the API around writing to an S3-backed key/log store:
```rust
pub trait StoreWriter {
    /// create a new keyspace, returning an error on failure or if the keyspace already existed
    fn create_keyspace(&self, keyspace: &str) -> Result<CreatedKeyspace, StoreError>;

    /// append records to a log, creating a new key if necessary.
    /// in some implementations, this may be dispatched and executed asynchronously.
    fn append(
        &self,
        keyspace: &str,
        key: &str,
        inserts: Vec<Insertion>,
    ) -> Result<(), StoreError>;

    /// flush pending writes for a specific key
    fn flush_key(&self, keyspace: &str, key: &str) -> Result<(), StoreError>;

    /// flush all pending asynchronous operations
    fn flush_all(&self) -> Result<(), StoreError>;

    /// should be called periodically for implementation that require it.
    /// this will trigger scheduled operations, like flushing a pending batch.
    fn duty_cycle(&self) -> Result<(), StoreError>;
}
```

The `append` function allows writing of a vector of records, each of which will be individually nonce-checked.
The user has the option of specifiying a timestamp. When left undefined, the `StoreWriter` implementation will use the system clock.
```rust
pub struct Insertion {
    pub record: Vec<u8>,
    pub nonce: Option<u128>,
    pub timestamp: Option<i64>,
}
```


## S3

The `S3StoreWriter` and `S3StoreReader` offset reader and writer functionality backed by an S3 store.
Both can be configured from the `S3StoreConfig` object, but some configuration parameters are only used by one of the two implementations.

### Object Keys

The following structure is used for object keys to enable bi-directional iteration and O(log(n)) binary searchability by offset, timestamp and nonce.
```
{prefix}{keyspace}/{key}/data_o{firstOffset}-o{lastOffset}_t{minTimestamp}-t{maxTimestamp}_n{firstNonce}-n{nextNonce}_s{sizeInBytes}_p{priorBatchStartOffset}.bin
```

### Shared Config

The following parameters are used to specify S3-connection details:
```rust
/// object prefix, defaults to an empty string, which would put the keyspace at the root of the bucket
object_prefix: String

/// required, the bucket name
bucket_name: Option<String>

/// optional, used to override the S3 endpoint
endpoint: Option<String>

/// optional, defaults to us-east-1
region: String

/// enable path-style endpoints, defaults to false
path_style: bool

/// use default credentials instead of given access keys, defaults to true
use_default_credentials: bool

/// optional, used when use_default_credentials=false
access_key: Option<String>

/// optional, used when use_default_credentials=false
secret_key: Option<String>

/// optional, used when use_default_credentials=false
security_token: Option<String>

/// optional, used when use_default_credentials=false
session_token: Option<String>

/// optional, used when use_default_credentials=false
profile: Option<String>
```

### Reader-Specific Config

The following parameters are used to specify reader default behavior when not defined in a request:
```rust
/// set the default page size used when none is defined in the request
default_page_size: u64
```

### Writer-Specific Config

The following parameters are used to specify writer cache and compaction behavior:
```rust
/// set the maximum number of cached keys kept in memory in the writer, defaults to 100k
max_cached_keys: usize,

/// set the size threshold to trigger object compaction of a complete batch, defaults to 1000
compact_records_threshold: u64

/// set the size threshold to trigger object compaction of a complete batch, defaults to 1MiB
compact_size_threshold: u64

/// set the object count threshold to trigger compaction of a partial batch, defaults to 100
compact_objects_threshold: 100
```


## Batching

The `BatchingStoreWriter` implements the `StoreWriterTrait` and wraps an underlying `StoreWriter` implementation to enable batching of insertions to optimize throughput.
Multiple threads can be utilized to further increase write throughput. 
Individual keys will be batched by the same writer thread.
The `BatchingStoreWriterConfig` allows the user to configure the following batching parameters:
```rust
/// set the number of writer threads. defaults to 1.
/// each key will always be written by the same thread, so consider number of keys when determining the number of threads to use.
writer_thread_count: usize

/// set the capacity of the queue for each writer thread. defaults to unbound.
/// this is the maximum number of writes that can be queued per thread.
/// consider this parameter, a typical write size, and the number of writer threads to determine memory requirements.
/// when set to None, there will be no capacity limitations.
writer_thread_queue_capacity: usize

/// set the interval at which to check to flush batches. defaults to 100 milliseconds.
batch_check_interval_millis: u64

/// set the interval at which to flush batches, regardless of size. defaults to 1 second.
/// this interval plus processing time is the maximum additional delay introduced by batching.
batch_flush_interval_millis: u64

/// flush a batch upon reaching a specific record count. defaults to u64::MAX records.
batch_flush_record_count_threshold: u64

/// flush a batch upon reaching a specific batch size. defaults to 1MB.
/// each key builds a separate batch, so this parameter and flush_interval+throughput will help determine memory requirements.
batch_flush_size_threshold: u64
```


## Kafka Bridge

A `KafkaConsumerBridge` couples an `S3StoreWriter` and `BatchingStoreWriter` with a `KafkaConsumer`.
It allows for efficient and configurable batching of data sourced from Kafka via a consumer group.

### Configuation

Here is an example `KafkaConsumerBridge` configuration:
```ini
[s3]
endpoint="http://localhost:4566"
bucket_name="my-bucket"
region="us-east-1"
path_style=true
use_default_credentials=false
access_key="test"
secret_key="test"
security_token="test"
session_token="test"
profile="test"
compact_size_threshold=1000000
compact_records_threshold=100
compact_objects_threshold=10

[batcher]
writer_thread_count=1
writer_thread_queue_capacity=4096
batch_check_interval_millis=100
batch_flush_interval_millis=1000
batch_flush_record_count_threshold=100000
batch_flush_size_threshold=1000000

[kafka]
topic="inbound"
offset_commit_interval_seconds=60
group.id="test_group"
bootstrap.servers="127.0.0.1:9092"
auto.offset.reset=earliest

[parser]
nonce_parser="RecordOffset"
timestamp_parser="None"
keyspace_parser="Static(my_keyspace)"
key_parser="RecordPartition"
```

### Consumer Group Offsets

The `offset_commit_interval_seconds` property indicates how often the batcher will be flushed and offsets will be committed for the consumer group.
Note that `enable.auto.commit` will always be set to `false` and `enable.auto.offset.store` will always be set to `true` so that the Kafka Bridge can deterministically commit offsets after writes.

### UTF-8 Parsers

`key_parser` and `keyspace_parser` can be configured as follows:

| Value                          | Description
|--------------------------------|------------
| `Static(some_string)`          | Use the given string
| `RecordHeader(my_header_name)` | Parse the given header as UTF-8 from each record
| `RecordKey`                    | Parse each record key as UTF-8
| `RecordPartition`              | Use the record partition

### Number Parsers

`nonce_parser` and `timestamp_parser` can be configured as follows:

| Value                                      | Description                                      
|--------------------------------------------|------------
| `None`                                     | Always set to None
| `RecordHeaderBigEndian(my_header_name)`    | Parse the given header as little-endian from each record
| `RecordHeaderLittleEndian(my_header_name)` | Parse the given header as big-endian from each record
| `RecordHeaderUtf8(my_header_name)`         | Parse the given header as UTF-8 converted to a number from each record
| `RecordKeyBigEndian`                       | Parse each record key as little-endian
| `RecordKeyLittleEndian`                    | Parse each record key as big-endian
| `RecordKeyUtf8`                            | Parse each record key as UTF-8 converted to a number
| `RecordOffset`                             | Use the record offset
| `RecordPartition`                          | Use the record partition