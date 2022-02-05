use super::parse::*;
use super::KafkaConsumerBridgeConfig;
use super::{KafkaConsumerNumberParser, KafkaConsumerUtf8Parser};
use crate::common::time::*;
use crate::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, Rebalance};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::Offset;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::time::Duration;

pub struct KafkaConsumerBridge<W: StoreWriter> {
    consumer: BaseConsumer<RebalanceLogger>,
    topic: String,
    nonce_parser: KafkaConsumerNumberParser,
    timestamp_parser: KafkaConsumerNumberParser,
    keyspace_parser: KafkaConsumerUtf8Parser,
    key_parser: KafkaConsumerUtf8Parser,
    offset_commit_interval_millis: u64,
    writer: W,
    internal_mut: RefCell<BridgeInternalMut>,
}
impl<W: StoreWriter> KafkaConsumerBridge<W> {
    pub fn new(config: KafkaConsumerBridgeConfig, writer: W) -> Result<Self, StoreError> {
        if let None = config.topic {
            return Err(StoreError::BadConfiguration(
                "config.topic not defined".to_string(),
            ));
        }
        if let KafkaConsumerUtf8Parser::None = config.keyspace_parser {
            return Err(StoreError::BadConfiguration(
                "config.keyspace_parser not defined".to_string(),
            ));
        }
        if let KafkaConsumerUtf8Parser::None = config.key_parser {
            return Err(StoreError::BadConfiguration(
                "config.key_parser not defined".to_string(),
            ));
        }
        if let None = config.consumer_config.get("bootstrap.servers") {
            return Err(StoreError::BadConfiguration(
                "consumer config bootstrap.servers not defined".to_string(),
            ));
        }
        if let None = config.consumer_config.get("group.id") {
            return Err(StoreError::BadConfiguration(
                "consumer config group.id not defined".to_string(),
            ));
        }
        let mut consumer_config = ClientConfig::new();
        for (k, v) in config.consumer_config.iter() {
            consumer_config.set(k, v);
        }
        consumer_config.set("enable.auto.commit", "false");
        consumer_config.set("enable.auto.offset.store", "true");
        let consumer = consumer_config.create_with_context(RebalanceLogger {});
        if let Err(err) = consumer {
            return Err(StoreError::IOError(format!(
                "could not create kafka consumer: {}",
                err.to_string()
            )));
        }
        let offset_commit_interval_millis = 1000 * config.offset_commit_interval_seconds;
        Ok(Self {
            consumer: consumer.unwrap(),
            topic: config.topic.unwrap(),
            nonce_parser: config.nonce_parser,
            timestamp_parser: config.timestamp_parser,
            keyspace_parser: config.keyspace_parser,
            key_parser: config.key_parser,
            writer: writer,
            internal_mut: RefCell::new(BridgeInternalMut {
                next_commit_timestamp: time_now_as_millis() + offset_commit_interval_millis,
                commit_stats: CommitStats::new(),
            }),
            offset_commit_interval_millis,
        })
    }
    pub fn subscribe(&self) -> Result<(), StoreError> {
        match self.consumer.subscribe(&[&self.topic]) {
            Ok(_) => Ok(()),
            Err(err) => Err(StoreError::IOError(format!(
                "kafka subscribe error: {}",
                err.to_string(),
            ))),
        }
    }
    pub fn poll(&self, timeout: Duration) -> Result<(), StoreError> {
        let now = time_now_as_millis();
        let mut internal_mut = self.internal_mut.borrow_mut();
        let result = self.poll_kafka_consumer(&mut internal_mut, timeout);
        if now >= internal_mut.next_commit_timestamp {
            log::info!(
                "scheduled commit: {}",
                internal_mut.commit_stats.to_string()
            );
            internal_mut.next_commit_timestamp = now + self.offset_commit_interval_millis;
            if internal_mut.commit_stats.record_count_since_commit > 0 {
                log::info!("flushing writer");
                self.writer.flush_all()?;
                log::info!("commiting offsets");
                if let Err(err) = self.consumer.commit_consumer_state(CommitMode::Async) {
                    return Err(StoreError::IOError(format!(
                        "consumer commit failed: {}",
                        err.to_string()
                    )));
                }
            }
            internal_mut.commit_stats.reset();
            log::info!("commit complete");
        }
        return result;
    }
    fn poll_kafka_consumer(
        &self,
        internal_mut: &mut BridgeInternalMut,
        timeout: Duration,
    ) -> Result<(), StoreError> {
        match self.consumer.poll(timeout) {
            Some(result) => {
                if let Err(err) = result {
                    return Err(StoreError::IOError(err.to_string()));
                }
                let message = result.unwrap();
                if let None = message.payload() {
                    return Ok(());
                }
                let timestamp = self.parse_timestamp(&message)?;
                let items = vec![Insertion {
                    value: message.payload().unwrap().to_vec(),
                    nonce: self.parse_nonce(&message)?,
                    timestamp: timestamp,
                }];
                self.writer.append_list(
                    &self.parse_keyspace(&message)?,
                    &self.parse_key(&message)?,
                    items,
                )?;
                internal_mut
                    .commit_stats
                    .increment(message.partition(), timestamp);
                return Ok(());
            }
            None => return Ok(()),
        }
    }
    fn parse_nonce(&self, message: &BorrowedMessage<'_>) -> Result<Option<u128>, StoreError> {
        parse_u128_opt(&self.nonce_parser, message)
    }
    fn parse_timestamp(&self, message: &BorrowedMessage<'_>) -> Result<Option<i64>, StoreError> {
        parse_i64_opt(&self.timestamp_parser, message)
    }
    fn parse_keyspace(&self, message: &BorrowedMessage<'_>) -> Result<String, StoreError> {
        parse_utf8_req(&self.keyspace_parser, message)
    }
    fn parse_key(&self, message: &BorrowedMessage<'_>) -> Result<String, StoreError> {
        parse_utf8_req(&self.key_parser, message)
    }
}

struct RebalanceLogger {}
impl rdkafka::client::ClientContext for RebalanceLogger {}
impl rdkafka::consumer::ConsumerContext for RebalanceLogger {
    fn post_rebalance(&self, rebalance: &Rebalance) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                log::info!(
                    "partitions assigned: {}",
                    topic_map_to_string(&tpl.to_topic_map())
                )
            }
            Rebalance::Revoke(tpl) => {
                log::info!(
                    "partitions revoked: {}",
                    topic_map_to_string(&tpl.to_topic_map())
                )
            }
            Rebalance::Error(err) => log::error!("rebalance error: {}", err.to_string()),
        }
    }
}

fn topic_map_to_string(map: &HashMap<(String, i32), Offset>) -> String {
    let mut topics: HashMap<String, Vec<i32>> = HashMap::new();
    for ((topic, partition), _) in map.iter() {
        if let None = topics.get_mut(topic) {
            topics.insert(topic.to_string(), Vec::new());
        }
        topics.get_mut(topic).unwrap().push(partition.clone());
    }
    let mut result: String = "[ ".to_string();
    for (topic, partitions) in topics.iter() {
        result += &format!("{}:{:?} ", topic, partitions);
    }
    result += "]";
    return result;
}

struct BridgeInternalMut {
    next_commit_timestamp: u64,
    commit_stats: CommitStats,
}

struct CommitStats {
    record_count_since_commit: usize,
    partition_stats: BTreeMap<i32, PartitionStats>,
}
impl CommitStats {
    fn new() -> Self {
        Self {
            record_count_since_commit: 0,
            partition_stats: BTreeMap::new(),
        }
    }
    fn reset(&mut self) {
        self.partition_stats.clear();
    }
    fn increment(&mut self, partition: i32, timestamp: Option<i64>) {
        self.record_count_since_commit += 1;
        match self.partition_stats.get_mut(&partition) {
            Some(entry) => entry.increment(timestamp),
            None => {
                self.partition_stats
                    .insert(partition, PartitionStats::first(timestamp));
                ()
            }
        }
    }
}
impl ToString for CommitStats {
    fn to_string(&self) -> String {
        let mut s = "[ ".to_string();
        for (k, v) in self.partition_stats.iter() {
            s += &format!("{}:[{}] ", k, v.to_string());
        }
        s += "]";
        return s;
    }
}

struct PartitionStats {
    record_count: usize,
    last_timestamp: Option<i64>,
}
impl PartitionStats {
    fn first(last_timestamp: Option<i64>) -> Self {
        Self {
            record_count: 1,
            last_timestamp,
        }
    }
    fn increment(&mut self, timestamp: Option<i64>) {
        self.record_count += 1;
        if let Some(_) = timestamp {
            self.last_timestamp = timestamp;
        }
    }
}
impl ToString for PartitionStats {
    fn to_string(&self) -> String {
        match self.last_timestamp {
            Some(timestamp) => format!("count={} timestamp={}", self.record_count, timestamp),
            None => format!("count={}", self.record_count),
        }
    }
}
