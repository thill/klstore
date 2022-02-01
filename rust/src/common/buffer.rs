use crate::StoreError;
use std::convert::TryInto;

pub fn read_u16(buffer: &[u8], position: usize) -> Result<u16, StoreError> {
    Ok(u16::from_le_bytes(
        match buffer[position..(position + 2)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u16".to_string())),
        },
    ))
}

pub fn read_u32(buffer: &[u8], position: usize) -> Result<u32, StoreError> {
    Ok(u32::from_le_bytes(
        match buffer[position..(position + 4)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u32".to_string())),
        },
    ))
}

pub fn read_u64(buffer: &[u8], position: usize) -> Result<u64, StoreError> {
    Ok(u64::from_le_bytes(
        match buffer[position..(position + 8)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u64".to_string())),
        },
    ))
}

pub fn read_u128(buffer: &[u8], position: usize) -> Result<u128, StoreError> {
    Ok(u128::from_le_bytes(
        match buffer[position..(position + 16)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u128".to_string())),
        },
    ))
}

pub fn read_u16_be(buffer: &[u8], position: usize) -> Result<u16, StoreError> {
    Ok(u16::from_be_bytes(
        match buffer[position..(position + 2)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u16_be".to_string())),
        },
    ))
}

pub fn read_u32_be(buffer: &[u8], position: usize) -> Result<u32, StoreError> {
    Ok(u32::from_be_bytes(
        match buffer[position..(position + 4)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u32_be".to_string())),
        },
    ))
}

pub fn read_u64_be(buffer: &[u8], position: usize) -> Result<u64, StoreError> {
    Ok(u64::from_be_bytes(
        match buffer[position..(position + 8)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u64_be".to_string())),
        },
    ))
}

pub fn read_u128_be(buffer: &[u8], position: usize) -> Result<u128, StoreError> {
    Ok(u128::from_be_bytes(
        match buffer[position..(position + 16)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_u128_be".to_string())),
        },
    ))
}

pub fn read_i8(buffer: &[u8], position: usize) -> Result<i8, StoreError> {
    Ok(i8::from_le_bytes(
        match buffer[position..(position + 1)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i8".to_string())),
        },
    ))
}

pub fn read_i16(buffer: &[u8], position: usize) -> Result<i16, StoreError> {
    Ok(i16::from_le_bytes(
        match buffer[position..(position + 2)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i16".to_string())),
        },
    ))
}

pub fn read_i32(buffer: &[u8], position: usize) -> Result<i32, StoreError> {
    Ok(i32::from_le_bytes(
        match buffer[position..(position + 4)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i32".to_string())),
        },
    ))
}

pub fn read_i64(buffer: &[u8], position: usize) -> Result<i64, StoreError> {
    Ok(i64::from_le_bytes(
        match buffer[position..(position + 8)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i64".to_string())),
        },
    ))
}


pub fn read_i8_be(buffer: &[u8], position: usize) -> Result<i8, StoreError> {
    Ok(i8::from_be_bytes(
        match buffer[position..(position + 1)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i8_be".to_string())),
        },
    ))
}

pub fn read_i16_be(buffer: &[u8], position: usize) -> Result<i16, StoreError> {
    Ok(i16::from_be_bytes(
        match buffer[position..(position + 2)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i16_be".to_string())),
        },
    ))
}

pub fn read_i32_be(buffer: &[u8], position: usize) -> Result<i32, StoreError> {
    Ok(i32::from_be_bytes(
        match buffer[position..(position + 4)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i32_be".to_string())),
        },
    ))
}

pub fn read_i64_be(buffer: &[u8], position: usize) -> Result<i64, StoreError> {
    Ok(i64::from_be_bytes(
        match buffer[position..(position + 8)].try_into() {
            Ok(v) => v,
            Err(_) => return Err(StoreError::BadData("read_i64_be".to_string())),
        },
    ))
}

pub fn read_bytes_copy(buffer: &[u8], position: usize, length: usize) -> Vec<u8> {
    buffer[position..(position + length)].to_vec()
}

pub fn append_u32(buffer: &mut Vec<u8>, value: u32) {
    buffer.append(&mut value.to_le_bytes().try_into().expect("append_u32"));
}

pub fn append_i64(buffer: &mut Vec<u8>, value: i64) {
    buffer.append(&mut value.to_le_bytes().try_into().expect("append_i64"));
}

pub fn append_u64(buffer: &mut Vec<u8>, value: u64) {
    buffer.append(&mut value.to_le_bytes().try_into().expect("append_u64"));
}

pub fn append_u128(buffer: &mut Vec<u8>, value: u128) {
    buffer.append(&mut value.to_le_bytes().try_into().expect("append_u128"));
}

pub fn append_buffer(buffer: &mut Vec<u8>, value: &[u8]) {
    buffer.extend_from_slice(&value);
}
