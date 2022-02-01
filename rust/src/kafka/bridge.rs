use super::parse::*;
use super::KafkaConsumerBridgeConfig;
use super::{KafkaConsumerNumberParser, KafkaConsumerUtf8Parser};
use crate::*;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer, Rebalance};
use rdkafka::message::{BorrowedMessage, Message};
use rdkafka::Offset;
use std::collections::HashMap;
use std::time::Duration;

pub struct KafkaConsumerBridge<W: StoreWriter> {
    consumer: BaseConsumer<RebalanceLogger>,
    topic: String,
    nonce_parser: KafkaConsumerNumberParser,
    timestamp_parser: KafkaConsumerNumberParser,
    keyspace_parser: KafkaConsumerUtf8Parser,
    key_parser: KafkaConsumerUtf8Parser,
    writer: W,
}
impl<W: StoreWriter> KafkaConsumerBridge<W> {
    pub fn new(config: KafkaConsumerBridgeConfig, writer: W) -> Result<Self, StoreError> {
        if let None = config.topic {
            return Err(StoreError::InvalidContinuation(
                "config.topic not defined".to_string(),
            ));
        }
        if let KafkaConsumerUtf8Parser::None = config.keyspace_parser {
            return Err(StoreError::InvalidContinuation(
                "config.keyspace_parser not defined".to_string(),
            ));
        }
        if let KafkaConsumerUtf8Parser::None = config.key_parser {
            return Err(StoreError::InvalidContinuation(
                "config.key_parser not defined".to_string(),
            ));
        }
        if let None = config.consumer_config.get("bootstrap.servers") {
            return Err(StoreError::InvalidContinuation(
                "consumer config bootstrap.servers not defined".to_string(),
            ));
        }
        if let None = config.consumer_config.get("group.id") {
            return Err(StoreError::InvalidContinuation(
                "consumer config group.id not defined".to_string(),
            ));
        }
        let mut consumer_config = ClientConfig::new();
        for (k, v) in config.consumer_config.iter() {
            consumer_config.set(k, v);
        }
        let consumer = consumer_config.create_with_context(RebalanceLogger {});
        if let Err(err) = consumer {
            return Err(StoreError::IOError(format!(
                "could not create kafka consumer: {}",
                err.to_string()
            )));
        }
        Ok(Self {
            consumer: consumer.unwrap(),
            topic: config.topic.unwrap(),
            nonce_parser: config.nonce_parser,
            timestamp_parser: config.timestamp_parser,
            keyspace_parser: config.keyspace_parser,
            key_parser: config.key_parser,
            writer: writer,
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
    pub fn poll(&self, timeout: Duration) -> Result<usize, StoreError> {
        match self.consumer.poll(timeout) {
            Some(result) => {
                if let Err(err) = result {
                    return Err(StoreError::IOError(err.to_string()));
                }
                let message = result.unwrap();
                if let None = message.payload() {
                    return Ok(0);
                }
                let items = vec![Insertion {
                    value: message.payload().unwrap().to_vec(),
                    nonce: self.parse_nonce(&message)?,
                    timestamp: self.parse_timestamp(&message)?,
                }];
                self.writer.append_list(
                    &self.parse_keyspace(&message)?,
                    &self.parse_key(&message)?,
                    items,
                )?;
                return Ok(1);
            }
            None => return Ok(0),
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
                    "Partitions Assigned: {}",
                    topic_map_to_string(&tpl.to_topic_map())
                )
            }
            Rebalance::Revoke(tpl) => {
                log::info!(
                    "Partitions Revoked: {}",
                    topic_map_to_string(&tpl.to_topic_map())
                )
            }
            Rebalance::Error(err) => log::error!("Rebalance Error: {}", err.to_string()),
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
    let mut result: String = "[".to_string();
    for (topic, partitions) in topics.iter() {
        if result.len() > 1 {
            result += ",";
        }
        result += &format!("{}={:?}", topic, partitions);
    }
    result += "]";
    return result;
}
