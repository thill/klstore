# klstore

Appendable and iterable key/list storage, backed by S3.


## General Overview

Per key, a single writer appends to underlying storage, enabling many concurrent readers to iterate through pages of elements.
Writers are able to perform batching and nonce checking to enable high-throughput, exactly-once persistence of data.
Readers are stateless and able to deterministically page backwards and forwards through a list, which is uniquely identified by a key per keyspace.
Readers can begin iterating from any point in a list, searchable by either offset, timestamp, or nonce.


## Terminology
* `keyspace`: A collection of keys
* `key`: A unique key in a keyspace
* `list`: An iterable collection of elements, identified by a key
* `offset`: The position of a single element in a list
* `nonce`: An ever-growing number per key, used for de-duplication of records 


## Rust API
### Reader
The `StoreReader` trait expresses the API around reading from an S3-backed key/list store:
```
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
```
An optional `ItemFilter` can be defined to begin iteration from an arbitrary element in a key's list:
```
pub struct ItemFilter {
    pub start_offset: Option<u64>,
    pub start_timestamp: Option<i64>,
    pub start_nonce: Option<u128>,
}
```
A returned item list represents a single page of results. Iteration can continue using the returned `continuation_token`:
```
pub struct ItemList {
    pub keyspace: String,
    pub key: String,
    pub items: Vec<Item>,
    pub continuation: Option<String>,
    pub stats: ListStats,
}
```

### Writer
The `StoreWriter` trait expresses the API around writing to an S3-backed key/list store:
```
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
```
The `append_list` function allows writing of a vector of items, each of which will be individually nonce-checked.
The user has the option of specifiying a timestamp. When left undefined, the `StoreWriter` implementation will use the system clock.
```
pub struct Insertion {
    pub value: Vec<u8>,
    pub nonce: Option<u128>,
    pub timestamp: Option<i64>,
}
```


## S3
The `S3StoreWriter` and `S3StoreReader` offset reader and writer functionality backed by an S3 store.
Both can be configured from the `S3StoreConfig` object, but some configuration parameters are only used by one of the two implementations.

### Shared Config
The following parameters are used to specify S3-connection details:
```
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
```
/// set the default number of max results used when none is defined in the request
default_max_results: u64
```

### Writer-Specific Config
The following parameters are used to specify writer cache and compaction behavior:
```
/// set the maximum number of cached keys kept in memory in the writer, defaults to 100k
max_cached_keys: usize,

/// set the size threshold to trigger object compaction of a complete batch, defaults to 1MB
compact_items_threshold: u64

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
```
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

/// flush a batch upon reaching a specific item count. defaults to u64::MAX items.
batch_flush_item_count_threshold: u64

/// flush a batch upon reaching a specific batch size. defaults to 1MB.
/// each key builds a separate batch, so this parameter and flush_interval+throughput will help determine memory requirements.
batch_flush_size_threshold: u64
```


## Kafka Bridge
A `KafkaConsumerBridge` couples an `S3StoreWriter` and `BatchingStoreWriter` with a Kafka `Consumer`.
It allows for efficient and configurable batching of data sourced from Kafka via a consumer group.

### Configuation
Here is an example `KafkaConsumerBridge` configuration:
```
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
compact_items_threshold=100
compact_objects_threshold=10

[batcher]
writer_thread_count=1,
writer_thread_queue_capacity=4096
batch_check_interval_millis=100
batch_flush_interval_millis=1000
batch_flush_item_count_threshold=100000
batch_flush_size_threshold=1000000

[kafka]
topic="inbound"
group.id="test_group"
bootstrap.servers="127.0.0.1:9092"
auto.offset.reset=earliest
enable.auto.offset.store=true
enable.auto.commit=true

[parser]
nonce_parser="RecordOffset"
timestamp_parser="None"
keyspace_parser="Static(my_keyspace)"
key_parser="RecordPartition"
```

`key_parser` and `keyspace_parser` can be configured as follows:
* `Static(some_string)` - use the given string
* `RecordHeader(my_header_name)` - parse the given header as UTF-8 from each record
* `RecordKey` - parse each record key as UTF-8
* `RecordPartition` - use the record partition

`nonce_parser` and `timestamp_parser` can be configured as follows:
* `None` - always set to None
* `RecordHeaderBigEndian(my_header_name)` - parse the given header as little-endian from each record
* `RecordHeaderLittleEndian(my_header_name)` - parse the given header as big-endian from each record
* `RecordHeaderUtf8(my_header_name)` - parse the given header as UTF-8 converted to a number from each record
* `RecordKeyBigEndian` - parse each record key as little-endian
* `RecordKeyLittleEndian` - parse each record key as big-endian
* `RecordKeyUtf8` - parse each record key as UTF-8 converted to a number
* `RecordOffset` - use the record offset
* `RecordPartition` - use the record partition