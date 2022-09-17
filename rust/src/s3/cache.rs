use super::bucket::*;
use crate::common::cache::*;
use crate::common::keypath::*;
use crate::*;
use aws_s3::bucket::Bucket;

#[derive(Clone, Debug)]
pub struct CachedKey {
    pub metadata: KeyMetadata,
    pub uncompacted_objects: u64,
    pub uncompacted_records: u64,
    pub uncompacted_size: u64,
    pub prior_start_offset: u64,
    pub watermark: Watermark,
}

pub struct S3CacheFetcher {
    bucket: Bucket,
    root_prefix: String,
    key_path_parser: KeyPathParser,
    compact_records_threshold: u64,
}
impl S3CacheFetcher {
    pub fn new(bucket: Bucket, root_prefix: String, compact_records_threshold: u64) -> Self {
        Self {
            bucket,
            root_prefix,
            key_path_parser: KeyPathParser::new(),
            compact_records_threshold,
        }
    }
}
impl CacheFetcher<CachedKey> for S3CacheFetcher {
    fn load_key(&self, keyspace: &str, key: &str) -> Result<CachedKey, StoreError> {
        let watermark_path = Watermark::path(&self.root_prefix, keyspace, key);
        let watermark_contents_opt = get_object_optional(&self.bucket, watermark_path)?;
        let watermark_opt = match watermark_contents_opt {
            Some(contents) => Some(Watermark::from(&contents)?),
            None => None,
        };
        // strategy: list from watermark until end
        // use optional watermark to determine where to start listing from
        let key_data_prefix = KeyPath::prefix_data_only(&self.root_prefix, keyspace, key);
        let list_from = match &watermark_opt {
            Some(watermark_contents) => Some(KeyPath::watermark_prefix(
                &self.root_prefix,
                keyspace,
                key,
                &watermark_contents,
            )),
            None => None,
        };
        // list exhaustive from watermark
        let list = list_exhaustive(&self.bucket, &key_data_prefix, list_from)?;
        if list.is_empty() {
            // empty key, return default
            return Ok(CachedKey {
                metadata: KeyMetadata {
                    next_nonce: 0,
                    next_offset: 1,
                },
                uncompacted_records: 0,
                uncompacted_objects: 0,
                uncompacted_size: 0,
                prior_start_offset: 0,
                watermark: Watermark::new(0),
            });
        }
        // summarize pending from watermark
        let mut uncompacted_records: u64 = 0;
        let mut uncompacted_objects: u64 = 0;
        let mut uncompacted_size: u64 = 0;
        let mut next_nonce: u128 = 0;
        let mut next_offset: u64 = 0;
        let mut prior_start_offset: u64 = 0;
        for i in 0..list.len() {
            match &self.key_path_parser.parse(&list[i]) {
                Some(key) => {
                    let object_records_count = key.last_offset - key.first_offset + 1;
                    next_nonce = key.next_nonce;
                    next_offset = key.last_offset + 1;
                    prior_start_offset = key.prior_start_offset;
                    if i == 0 && object_records_count >= self.compact_records_threshold {
                        // first record at watermark is a complete batch, skip it for counts
                    } else {
                        uncompacted_records += object_records_count;
                        uncompacted_size += key.size;
                        uncompacted_objects += 1;
                    }
                }
                None => return Err(StoreError::BadData(format!("invalid key {}", list[i]))),
            }
        }
        return Ok(CachedKey {
            metadata: KeyMetadata {
                next_nonce,
                next_offset,
            },
            uncompacted_records,
            uncompacted_objects,
            uncompacted_size,
            prior_start_offset,
            watermark: match watermark_opt {
                Some(wm) => wm,
                None => Watermark::new(0),
            },
        });
    }
}
