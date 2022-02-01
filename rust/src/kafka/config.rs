use super::parse::*;
use super::{KafkaConsumerNumberParser, KafkaConsumerUtf8Parser};
use crate::StoreError;
use ini::Ini;
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub struct KafkaConsumerBridgeConfig {
    pub consumer_config: HashMap<String, String>,
    pub topic: Option<String>,
    pub nonce_parser: KafkaConsumerNumberParser,
    pub timestamp_parser: KafkaConsumerNumberParser,
    pub keyspace_parser: KafkaConsumerUtf8Parser,
    pub key_parser: KafkaConsumerUtf8Parser,
}
impl KafkaConsumerBridgeConfig {
    pub fn new() -> Self {
        Self {
            consumer_config: HashMap::new(),
            topic: None,
            nonce_parser: KafkaConsumerNumberParser::None,
            timestamp_parser: KafkaConsumerNumberParser::None,
            keyspace_parser: KafkaConsumerUtf8Parser::None,
            key_parser: KafkaConsumerUtf8Parser::None,
        }
    }
    pub fn set_consumer_config(mut self, v: HashMap<String, String>) -> Self {
        self.consumer_config = v;
        self
    }
    pub fn set_topic(mut self, v: String) -> Self {
        self.topic = Some(v);
        self
    }
    pub fn add_consumer_config(mut self, key: String, value: String) -> Self {
        self.consumer_config.insert(key, value);
        self
    }
    pub fn set_nonce_parser(mut self, v: KafkaConsumerNumberParser) -> Self {
        self.nonce_parser = v;
        self
    }
    pub fn timestamp_parser(mut self, v: KafkaConsumerNumberParser) -> Self {
        self.timestamp_parser = v;
        self
    }
    pub fn set_keyspace_parser(mut self, v: KafkaConsumerUtf8Parser) -> Self {
        self.keyspace_parser = v;
        self
    }
    pub fn set_key_parser(mut self, v: KafkaConsumerUtf8Parser) -> Self {
        self.key_parser = v;
        self
    }
    pub fn load(ini: &Ini) -> Result<Self, StoreError> {
        let kafka = ini.section(Some("kafka"));
        let parser = ini.section(Some("parser"));
        if let None = kafka {
            return Err(StoreError::InvalidContinuation(
                "[kafka] config missing".to_string(),
            ));
        }
        if let None = parser {
            return Err(StoreError::InvalidContinuation(
                "[parser] config missing".to_string(),
            ));
        }
        let kafka = kafka.unwrap();
        let parser = parser.unwrap();

        let mut topic: Option<String> = None;
        let mut consumer_config: HashMap<String, String> = HashMap::new();
        for (k, v) in kafka.iter() {
            match k {
                "topic" => {
                    topic = Some(v.to_string());
                }
                _ => {
                    consumer_config.insert(k.to_string(), v.to_string());
                }
            }
        }

        let nonce_parser = create_number_parser(parser.get("nonce_parser"))?;
        let timestamp_parser = create_number_parser(parser.get("timestamp_parser"))?;
        let keyspace_parser = create_utf8_parser(parser.get("keyspace_parser"))?;
        let key_parser = create_utf8_parser(parser.get("key_parser"))?;

        Ok(Self {
            consumer_config,
            topic,
            nonce_parser,
            timestamp_parser,
            keyspace_parser,
            key_parser,
        })
    }
}
