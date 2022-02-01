mod bridge;
mod config;
mod parse;

pub type KafkaConsumerBridgeConfig = config::KafkaConsumerBridgeConfig;
pub type KafkaConsumerBridge<W> = bridge::KafkaConsumerBridge<W>;
pub type KafkaConsumerNumberParser = parse::KafkaConsumerNumberParser;
pub type KafkaConsumerUtf8Parser = parse::KafkaConsumerUtf8Parser;
