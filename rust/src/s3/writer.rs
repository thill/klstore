use super::bucket::*;
use super::cache::*;
use crate::common::cache::*;
use crate::common::keypath::*;
use crate::common::records::*;
use crate::common::time::time_now_as_millis;
use crate::s3::S3StoreConfig;
use crate::s3::*;
use crate::*;
use aws_s3::bucket::Bucket;

pub struct S3StoreWriter {
    bucket: Bucket,
    config: S3StoreConfig,
    write_cache: StoreCache<CachedKey, S3CacheFetcher>,
    key_path_parser: KeyPathParser,
}
impl S3StoreWriter {
    pub fn new(config: S3StoreConfig) -> Result<Self, StoreError> {
        let bucket = bucket::create(&config)?;
        Ok(Self {
            bucket: bucket.clone(),
            write_cache: StoreCache::new(
                S3CacheFetcher::new(
                    bucket,
                    config.object_prefix.clone(),
                    config.compact_records_threshold,
                ),
                config.max_cached_keys,
            ),
            config: config,
            key_path_parser: KeyPathParser::new(),
        })
    }
}
impl Clone for S3StoreWriter {
    fn clone(&self) -> Self {
        Self::new(self.config.clone()).unwrap()
    }
}
impl StoreWriter for S3StoreWriter {
    fn create_keyspace(&self, keyspace: &str) -> Result<CreatedKeyspace, StoreError> {
        let keyspace_config = KeyspacePath::config_path(&self.config.object_prefix, &keyspace);
        match self.bucket.get_object(keyspace_config.clone()) {
            Ok((_, 404)) => {}
            Ok((_, 200)) => return Err(StoreError::KeyspaceAlreadyExists),
            Ok((_, code)) => return Err(StoreError::IOError(format!("code {}", code))),
            Err(err) => return Err(StoreError::IOError(err.to_string())),
        }
        let content = format!("[keyspace]\ncreated={}", time_now_as_millis());
        match put_object(&self.bucket, keyspace_config, content.as_bytes()) {
            Ok(_) => Ok(CreatedKeyspace {
                keyspace: keyspace.to_string(),
            }),
            Err(err) => Err(err),
        }
    }
    fn append(&self, keyspace: &str, key: &str, records: Vec<Insertion>) -> Result<(), StoreError> {
        let mut kinfo = self.write_cache.get_or_read_key(&keyspace, &key)?;
        // determine what will be written
        let filtered = nonce_filter(&records, kinfo.metadata.next_nonce);

        // nothing to insert due to nonce checking
        if filtered.records.is_empty() {
            return Ok(());
        }

        // create buffer
        let serialized = serialize_insertion(&filtered.records, kinfo.metadata.next_offset);

        // write buffer to bucket
        let object_key = KeyPath {
            first_offset: serialized.first_insert_offset,
            last_offset: serialized.last_insert_offset,
            min_timestamp: serialized.min_timestamp,
            max_timestamp: serialized.max_timestamp,
            first_nonce: match filtered.first_nonce {
                None => filtered.first_potential_nonce,
                Some(v) => v,
            },
            next_nonce: filtered.next_nonce,
            size: serialized.buffer.len() as u64,
            prior_start_offset: kinfo.prior_start_offset,
        }
        .to_path(&self.config.object_prefix, &keyspace, &key);
        put_object(&self.bucket, object_key, &serialized.buffer)?;

        kinfo.metadata.next_nonce = filtered.next_nonce;
        kinfo.metadata.next_offset = serialized.next_offset;
        kinfo.uncompacted_records += filtered.records.len() as u64;
        kinfo.uncompacted_size += serialized.buffer.len() as u64;
        kinfo.uncompacted_objects += 1;
        kinfo.prior_start_offset = serialized.first_insert_offset;

        // check for compaction
        let kinfo = check_compaction(
            kinfo,
            &self.bucket,
            &self.config.object_prefix,
            keyspace,
            key,
            &self.key_path_parser,
            self.config.compact_records_threshold,
            self.config.compact_size_threshold,
            self.config.compact_objects_threshold,
        )?;

        // update cache
        self.write_cache.set_key(keyspace, key, kinfo);

        // return result
        return Ok(());
    }
    fn flush_key(&self, _keyspace: &str, _key: &str) -> Result<(), StoreError> {
        // no-op
        Ok(())
    }
    fn flush_all(&self) -> Result<(), StoreError> {
        // no-op
        Ok(())
    }
    fn duty_cycle(&self) -> Result<(), StoreError> {
        // no-op
        Ok(())
    }
}

pub fn check_compaction(
    key_data: CachedKey,
    bucket: &Bucket,
    root_prefix: &str,
    keyspace: &str,
    key: &str,
    key_path_parser: &KeyPathParser,
    compact_records_threshold: u64,
    compact_size_threshold: u64,
    compact_objects_threshold: u64,
) -> Result<CachedKey, StoreError> {
    if key_data.uncompacted_records < compact_records_threshold
        && key_data.uncompacted_objects < compact_objects_threshold
        && key_data.uncompacted_size < compact_size_threshold
    {
        // nothing to do
        return Ok(key_data);
    }

    // only advance watermark if size or record count is surpassed
    // this will leave pending a compaction due to object count
    let advance_watermark = key_data.uncompacted_records >= compact_records_threshold
        || key_data.uncompacted_size >= compact_size_threshold;

    // batch everything after matching watermark object
    let key_data_prefix = KeyPath::prefix_data_only(root_prefix, keyspace, key);
    let start_from =
        KeyPath::after_watermark_prefix(root_prefix, keyspace, key, &key_data.watermark);
    let objects_to_merge = list_exhaustive(bucket, &key_data_prefix, Some(start_from))?;

    let first_key = key_path_parser
        .parse_or_error(&objects_to_merge.first().expect("objects_to_merge empty"))?;
    let last_key = key_path_parser
        .parse_or_error(&objects_to_merge.last().expect("objects_to_merge empty"))?;

    if objects_to_merge.len() == 1 {
        // compacting one object is meaningless, return now to avoid deleting self
        // this will only happen for cases where a watermark would be advanced
        let new_watermark = Watermark::new(first_key.first_offset);
        put_object(
            bucket,
            Watermark::path(root_prefix, keyspace, key),
            &new_watermark.serialize(),
        )?;
        // return this object as an entire batch
        return Ok(CachedKey {
            metadata: key_data.metadata,
            uncompacted_records: 0,
            uncompacted_objects: 0,
            uncompacted_size: 0,
            prior_start_offset: key_data.prior_start_offset,
            watermark: Watermark::new(first_key.first_offset),
        });
    }

    // append all buffers
    let mut buffer: Vec<u8> = Vec::new();
    for obj_path in &objects_to_merge {
        let mut contents = get_object_required(bucket, obj_path.clone())?;
        buffer.append(&mut contents);
    }

    // write new object
    let key_path = KeyPath {
        first_offset: first_key.first_offset,
        last_offset: last_key.last_offset,
        min_timestamp: first_key.min_timestamp,
        max_timestamp: last_key.max_timestamp,
        first_nonce: first_key.first_nonce,
        next_nonce: last_key.next_nonce,
        size: buffer.len() as u64,
        prior_start_offset: first_key.prior_start_offset,
    };
    put_object(
        bucket,
        key_path.to_path(&root_prefix, &keyspace, &key),
        &buffer,
    )?;

    // delete old objects
    for obj_path in objects_to_merge {
        delete_object(bucket, obj_path)?;
    }

    if advance_watermark {
        // write new watermark
        let new_watermark = Watermark::new(first_key.first_offset);
        put_object(
            bucket,
            Watermark::path(root_prefix, keyspace, key),
            &new_watermark.serialize(),
        )?;
        // reset cache info
        return Ok(CachedKey {
            metadata: key_data.metadata,
            uncompacted_records: 0,
            uncompacted_objects: 0,
            uncompacted_size: 0,
            prior_start_offset: first_key.first_offset,
            watermark: new_watermark,
        });
    } else {
        // only reset uncompacted_objects, do not advance watermark
        return Ok(CachedKey {
            metadata: key_data.metadata,
            uncompacted_records: key_data.uncompacted_records,
            uncompacted_objects: 1,
            uncompacted_size: key_data.uncompacted_size,
            prior_start_offset: first_key.first_offset,
            watermark: key_data.watermark,
        });
    }
}
