use super::super::common::buffer::*;
use crate::StoreError;
use rdkafka::message::Headers;
use rdkafka::message::{BorrowedMessage, Message};
use regex::Regex;
use std::str;

#[derive(Clone, Debug)]
pub enum KafkaConsumerNumberParser {
    None,
    RecordHeaderBigEndian(String),
    RecordHeaderLittleEndian(String),
    RecordHeaderUtf8(String),
    RecordKeyBigEndian,
    RecordKeyLittleEndian,
    RecordKeyUtf8,
    RecordOffset,
    RecordPartition,
}

#[derive(Clone, Debug)]
pub enum KafkaConsumerUtf8Parser {
    None,
    Static(String),
    RecordHeader(String),
    RecordKey,
    RecordPartition,
}

pub fn create_number_parser(cfg: Option<&str>) -> Result<KafkaConsumerNumberParser, StoreError> {
    if let None = cfg {
        return Ok(KafkaConsumerNumberParser::None);
    }
    let cfg = cfg.unwrap();
    match cfg {
        "None" => return Ok(KafkaConsumerNumberParser::None),
        "RecordKeyBigEndian" => return Ok(KafkaConsumerNumberParser::RecordKeyBigEndian),
        "RecordKeyLittleEndian" => return Ok(KafkaConsumerNumberParser::RecordKeyLittleEndian),
        "RecordKeyUtf8" => return Ok(KafkaConsumerNumberParser::RecordKeyUtf8),
        "RecordOffset" => return Ok(KafkaConsumerNumberParser::RecordOffset),
        "RecordPartition" => return Ok(KafkaConsumerNumberParser::RecordPartition),
        _ => {}
    }
    let rex =
        Regex::new(r"^(.+)\((.+)\)$").expect("create_number_parser regex compilation failure");
    if let Some(cap) = rex.captures(cfg) {
        match &cap[1] {
            "RecordHeaderBigEndian" => {
                return Ok(KafkaConsumerNumberParser::RecordHeaderBigEndian(
                    cap[2].to_string(),
                ));
            }
            "RecordHeaderLittleEndian" => {
                return Ok(KafkaConsumerNumberParser::RecordHeaderLittleEndian(
                    cap[2].to_string(),
                ));
            }
            "RecordHeaderUtf8" => {
                return Ok(KafkaConsumerNumberParser::RecordHeaderUtf8(
                    cap[2].to_string(),
                ));
            }
            _ => {}
        }
    }
    return Err(StoreError::BadConfiguration(format!(
        "invalid number parser: {}",
        cfg
    )));
}

pub fn create_utf8_parser(cfg: Option<&str>) -> Result<KafkaConsumerUtf8Parser, StoreError> {
    if let None = cfg {
        return Ok(KafkaConsumerUtf8Parser::None);
    }
    let cfg = cfg.unwrap();
    match cfg {
        "None" => return Ok(KafkaConsumerUtf8Parser::None),
        "RecordKey" => return Ok(KafkaConsumerUtf8Parser::RecordKey),
        "RecordPartition" => return Ok(KafkaConsumerUtf8Parser::RecordPartition),
        _ => {}
    }
    let rex = Regex::new(r"^(.+)\((.+)\)$").expect("create_utf8_parser regex compilation failure");
    if let Some(cap) = rex.captures(cfg) {
        match &cap[1] {
            "Static" => {
                return Ok(KafkaConsumerUtf8Parser::Static(cap[2].to_string()));
            }
            "RecordHeader" => {
                return Ok(KafkaConsumerUtf8Parser::RecordHeader(cap[2].to_string()));
            }
            _ => {}
        }
    }
    return Err(StoreError::BadConfiguration(format!(
        "invalid number parser: {}",
        cfg
    )));
}

pub fn parse_utf8_req(
    parser: &KafkaConsumerUtf8Parser,
    message: &BorrowedMessage<'_>,
) -> Result<String, StoreError> {
    match parse_utf8_opt(parser, message)? {
        None => Err(StoreError::BadData(
            "required utf8 value not present".to_string(),
        )),
        Some(v) => Ok(v),
    }
}

pub fn parse_utf8_opt(
    parser: &KafkaConsumerUtf8Parser,
    message: &BorrowedMessage<'_>,
) -> Result<Option<String>, StoreError> {
    match parser {
        KafkaConsumerUtf8Parser::None => Ok(None),
        KafkaConsumerUtf8Parser::Static(v) => Ok(Some(v.to_string())),
        KafkaConsumerUtf8Parser::RecordHeader(name) => parse_utf8_header(message, name),
        KafkaConsumerUtf8Parser::RecordKey => parse_opt_utf8(message.key()),
        KafkaConsumerUtf8Parser::RecordPartition => Ok(Some(message.partition().to_string())),
    }
}

pub fn parse_u128_opt(
    parser: &KafkaConsumerNumberParser,
    message: &BorrowedMessage<'_>,
) -> Result<Option<u128>, StoreError> {
    match parser {
        KafkaConsumerNumberParser::None => Ok(None),
        KafkaConsumerNumberParser::RecordHeaderBigEndian(name) => {
            parse_be_header_as_u128(message, name)
        }
        KafkaConsumerNumberParser::RecordHeaderLittleEndian(name) => {
            parse_le_header_as_u128(message, name)
        }
        KafkaConsumerNumberParser::RecordHeaderUtf8(name) => {
            parse_utf8_header_as_u128(message, name)
        }
        KafkaConsumerNumberParser::RecordOffset => Ok(Some(message.offset() as u128)),
        KafkaConsumerNumberParser::RecordKeyBigEndian => parse_opt_be_as_u128(message.key()),
        KafkaConsumerNumberParser::RecordKeyLittleEndian => parse_opt_le_as_u128(message.key()),
        KafkaConsumerNumberParser::RecordKeyUtf8 => parse_opt_utf8_as_u128(message.key()),
        KafkaConsumerNumberParser::RecordPartition => Ok(Some(message.partition() as u128)),
    }
}

pub fn parse_i64_opt(
    parser: &KafkaConsumerNumberParser,
    message: &BorrowedMessage<'_>,
) -> Result<Option<i64>, StoreError> {
    match parser {
        KafkaConsumerNumberParser::None => Ok(None),
        KafkaConsumerNumberParser::RecordHeaderBigEndian(name) => {
            parse_be_header_as_i64(message, name)
        }
        KafkaConsumerNumberParser::RecordHeaderLittleEndian(name) => {
            parse_le_header_as_i64(message, name)
        }
        KafkaConsumerNumberParser::RecordHeaderUtf8(name) => {
            parse_utf8_header_as_i64(message, name)
        }
        KafkaConsumerNumberParser::RecordOffset => Ok(Some(message.offset() as i64)),
        KafkaConsumerNumberParser::RecordKeyBigEndian => parse_opt_be_as_i64(message.key()),
        KafkaConsumerNumberParser::RecordKeyLittleEndian => parse_opt_le_as_i64(message.key()),
        KafkaConsumerNumberParser::RecordKeyUtf8 => parse_opt_utf8_as_i64(message.key()),
        KafkaConsumerNumberParser::RecordPartition => Ok(Some(message.partition() as i64)),
    }
}

fn parse_le_header_as_u128(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<u128>, StoreError> {
    match message.headers() {
        None => return Ok(None),
        Some(headers) => {
            for i in 0..headers.count() {
                let (n, value) = headers.get(i).unwrap();
                if n == name {
                    return parse_le_as_u128(value);
                }
            }
            return Ok(None);
        }
    }
}

fn parse_be_header_as_u128(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<u128>, StoreError> {
    match message.headers() {
        None => return Ok(None),
        Some(headers) => {
            for i in 0..headers.count() {
                let (n, value) = headers.get(i).unwrap();
                if n == name {
                    return parse_be_as_u128(value);
                }
            }
            return Ok(None);
        }
    }
}

fn parse_utf8_header_as_u128(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<u128>, StoreError> {
    match parse_utf8_header(message, name)? {
        None => Ok(None),
        Some(v) => parse_str_as_u128(&v),
    }
}

fn parse_le_header_as_i64(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<i64>, StoreError> {
    match message.headers() {
        None => return Ok(None),
        Some(headers) => {
            for i in 0..headers.count() {
                let (n, value) = headers.get(i).unwrap();
                if n == name {
                    return parse_le_as_i64(value);
                }
            }
            return Ok(None);
        }
    }
}

fn parse_be_header_as_i64(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<i64>, StoreError> {
    match message.headers() {
        None => return Ok(None),
        Some(headers) => {
            for i in 0..headers.count() {
                let (n, value) = headers.get(i).unwrap();
                if n == name {
                    return parse_be_as_i64(value);
                }
            }
            return Ok(None);
        }
    }
}

fn parse_utf8_header_as_i64(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<i64>, StoreError> {
    match parse_utf8_header(message, name)? {
        None => Ok(None),
        Some(v) => parse_str_as_i64(&v),
    }
}

fn parse_utf8_header(
    message: &BorrowedMessage<'_>,
    name: &String,
) -> Result<Option<String>, StoreError> {
    match message.headers() {
        None => return Ok(None),
        Some(headers) => {
            for i in 0..headers.count() {
                let (n, v) = headers.get(i).unwrap();
                if n == name {
                    return Ok(Some(parse_utf8(v)?));
                }
            }
            return Ok(None);
        }
    }
}

fn parse_utf8(v: &[u8]) -> Result<String, StoreError> {
    match str::from_utf8(v) {
        Ok(s) => Ok(s.to_string()),
        Err(_) => return Err(StoreError::BadData("not parsable as utf8".to_string())),
    }
}

fn parse_opt_utf8(v: Option<&[u8]>) -> Result<Option<String>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => Ok(Some(parse_utf8(v)?)),
    }
}

fn parse_opt_le_as_u128(v: Option<&[u8]>) -> Result<Option<u128>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_le_as_u128(v),
    }
}

fn parse_opt_be_as_u128(v: Option<&[u8]>) -> Result<Option<u128>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_be_as_u128(v),
    }
}

fn parse_opt_utf8_as_u128(v: Option<&[u8]>) -> Result<Option<u128>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_utf8_as_u128(v),
    }
}

fn parse_le_as_u128(v: &[u8]) -> Result<Option<u128>, StoreError> {
    match v.len() {
        1 => Ok(Some(v[0] as u128)),
        2 => Ok(Some(read_u16(v, 0)? as u128)),
        4 => Ok(Some(read_u32(v, 0)? as u128)),
        8 => Ok(Some(read_u64(v, 0)? as u128)),
        16 => Ok(Some(read_u128(v, 0)?)),
        _ => Err(StoreError::BadData(format!(
            "u128 little endian header size {}",
            v.len()
        ))),
    }
}

fn parse_be_as_u128(v: &[u8]) -> Result<Option<u128>, StoreError> {
    match v.len() {
        1 => Ok(Some(v[0] as u128)),
        2 => Ok(Some(read_u16_be(v, 0)? as u128)),
        4 => Ok(Some(read_u32_be(v, 0)? as u128)),
        8 => Ok(Some(read_u64_be(v, 0)? as u128)),
        16 => Ok(Some(read_u128_be(v, 0)?)),
        _ => Err(StoreError::BadData(format!(
            "u128 big endian header size {}",
            v.len()
        ))),
    }
}

fn parse_utf8_as_u128(v: &[u8]) -> Result<Option<u128>, StoreError> {
    parse_str_as_u128(&parse_utf8(v)?)
}

fn parse_str_as_u128(s: &str) -> Result<Option<u128>, StoreError> {
    match s.parse::<u128>() {
        Ok(v) => return Ok(Some(v)),
        Err(_) => return Err(StoreError::BadData(format!("utf8 '{}' not a u128", s))),
    }
}

fn parse_opt_le_as_i64(v: Option<&[u8]>) -> Result<Option<i64>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_le_as_i64(v),
    }
}

fn parse_opt_be_as_i64(v: Option<&[u8]>) -> Result<Option<i64>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_be_as_i64(v),
    }
}

fn parse_opt_utf8_as_i64(v: Option<&[u8]>) -> Result<Option<i64>, StoreError> {
    match &v {
        None => Ok(None),
        Some(v) => parse_utf8_as_i64(v),
    }
}

fn parse_le_as_i64(v: &[u8]) -> Result<Option<i64>, StoreError> {
    match v.len() {
        1 => Ok(Some(read_i8(v, 0)? as i64)),
        2 => Ok(Some(read_i16(v, 0)? as i64)),
        4 => Ok(Some(read_i32(v, 0)? as i64)),
        8 => Ok(Some(read_i64(v, 0)?)),
        _ => Err(StoreError::BadData(format!(
            "i64 little endian header size {}",
            v.len()
        ))),
    }
}

fn parse_be_as_i64(v: &[u8]) -> Result<Option<i64>, StoreError> {
    match v.len() {
        1 => Ok(Some(read_i8_be(v, 0)? as i64)),
        2 => Ok(Some(read_i16_be(v, 0)? as i64)),
        4 => Ok(Some(read_i32_be(v, 0)? as i64)),
        8 => Ok(Some(read_i64_be(v, 0)?)),
        _ => Err(StoreError::BadData(format!(
            "i64 big endian header size {}",
            v.len()
        ))),
    }
}

fn parse_utf8_as_i64(v: &[u8]) -> Result<Option<i64>, StoreError> {
    parse_str_as_i64(&parse_utf8(v)?)
}

fn parse_str_as_i64(s: &str) -> Result<Option<i64>, StoreError> {
    match s.parse::<i64>() {
        Ok(v) => return Ok(Some(v)),
        Err(_) => return Err(StoreError::BadData(format!("utf8 '{}' not an i64", s))),
    }
}
