use super::bucket::*;
use super::collect::*;
use crate::common::config::*;
use crate::common::keypath::*;
use crate::s3::S3StoreConfig;
use crate::s3::*;
use crate::*;
use aws_s3::bucket::Bucket;

pub struct S3StoreReader {
    bucket: Bucket,
    config: S3StoreConfig,
    key_path_parser: KeyPathParser,
    continuation_parser: ContinuationParser,
}
impl S3StoreReader {
    pub fn new(config: S3StoreConfig) -> Result<Self, StoreError> {
        let bucket = bucket::create(&config)?;
        Ok(Self {
            bucket: bucket.clone(),
            config: config,
            key_path_parser: KeyPathParser::new(),
            continuation_parser: ContinuationParser::new(),
        })
    }
}
impl Clone for S3StoreReader {
    fn clone(&self) -> Self {
        Self::new(self.config.clone()).unwrap()
    }
}
impl StoreReader for S3StoreReader {
    fn read_keyspace_metadata(&self, keyspace: &str) -> Result<KeyspaceMetadata, StoreError> {
        let conf_path = KeyspacePath::config_path(&self.config.object_prefix, &keyspace);
        let contents = match self.bucket.get_object(conf_path.clone()) {
            Ok((contents, 200)) => contents,
            Ok((_, code)) => return Err(StoreError::IOError(format!("code {}", code))),
            Err(err) => {
                return Err(StoreError::IOError(err.to_string()));
            }
        };
        let ini_string = match std::str::from_utf8(&contents) {
            Ok(v) => v,
            Err(err) => return Err(StoreError::IOError(err.to_string())),
        };
        match ini::Ini::load_from_str(ini_string) {
            Ok(ini) => keyspace_metadata_from_ini(&ini),
            Err(_) => Err(StoreError::IOError(format!(
                "could not load config for keyspace {}",
                keyspace
            ))),
        }
    }
    fn read_key_metadata(
        &self,
        keyspace: &str,
        key: &str,
    ) -> Result<Option<KeyMetadata>, StoreError> {
        let watermark_path = Watermark::path(&self.config.object_prefix, keyspace, key);
        let watermark_content_opt = get_object_optional(&self.bucket, watermark_path)?;

        if let Some(watermark_content) = watermark_content_opt {
            // use available watermark
            let watermark = Watermark::from(&watermark_content)?;
            // increasing details all come from last key, start from watermark
            let list = list_exhaustive(
                &self.bucket,
                &KeyPath::prefix_data_only(&self.config.object_prefix, keyspace, key),
                Some(watermark.start_from(&self.config.object_prefix, keyspace, key)),
            )?;
            if list.is_empty() {
                return Err(StoreError::IOError(format!(
                    "{} is not pointing to any data",
                    Watermark::path(&self.config.object_prefix, keyspace, key),
                )));
            }
            return Ok(Some(
                self.key_path_parser
                    .parse_or_error(&list.last().unwrap())?
                    .to_metadata(),
            ));
        } else {
            // no watermark, list all data files for key
            let list = list_exhaustive(
                &self.bucket,
                &KeyPath::prefix_data_only(&self.config.object_prefix, keyspace, key),
                None,
            )?;
            if list.is_empty() {
                // empty, key does not exist
                return Ok(None);
            }
            return Ok(Some(
                self.key_path_parser
                    .parse_or_error(&list.last().unwrap())?
                    .to_metadata(),
            ));
        }
    }
    fn read_first_page(
        &self,
        keyspace: &str,
        key: &str,
        direction: Direction,
        start: StartPosition,
        page_size: Option<u64>,
    ) -> Result<Page, StoreError> {
        let mut stats = ReadStats {
            list_operation_count: 0,
            read_operation_count: 0,
            read_size_total: 0,
            continuation_miss_count: 0,
        };
        let data_prefix = KeyPath::prefix_data_only(&self.config.object_prefix, keyspace, key);
        let page_size = match page_size {
            None => self.config.default_page_size,
            Some(v) => v,
        };

        // try collecting first page of records
        let collect_outcome = collect_first_page(
            &mut stats,
            &self.bucket,
            &self.config.object_prefix,
            keyspace,
            key,
            &data_prefix,
            &start,
            page_size,
            &self.key_path_parser,
            &direction,
        )?;

        if collect_outcome.requires_retry && collect_outcome.records.is_empty() {
            // failed with no results, return done so that the client doesn't end up in a continuous empty paging loop
            return Ok(Page {
                keyspace: keyspace.to_string(),
                key: key.to_string(),
                continuation: None,
                records: collect_outcome.records,
            });
        }

        log::debug!("s3 read_first_page stats: {:#?}", stats);

        // collect worked, return results
        return Ok(Page {
            keyspace: keyspace.to_string(),
            key: key.to_string(),
            continuation: collect_outcome.continuation(),
            records: collect_outcome.records,
        });
    }

    fn read_next_page(
        &self,
        keyspace: &str,
        key: &str,
        continuation: String,
        page_size: Option<u64>,
    ) -> Result<Page, StoreError> {
        let mut stats = ReadStats {
            list_operation_count: 0,
            read_operation_count: 0,
            read_size_total: 0,
            continuation_miss_count: 0,
        };
        let data_prefix = KeyPath::prefix_data_only(&self.config.object_prefix, keyspace, key);
        let page_size = match page_size {
            None => self.config.default_page_size,
            Some(v) => v,
        };

        // try collecting next page of records
        let mut collect_outcome = collect_next_page(
            &mut stats,
            &self.bucket,
            &self.config.object_prefix,
            keyspace,
            key,
            &data_prefix,
            page_size,
            &self.key_path_parser,
            &continuation,
            &self.continuation_parser,
        )?;

        if collect_outcome.requires_retry && collect_outcome.records.is_empty() {
            // read failed with no results, likely timing of a concurrent compaction
            // try again now that compaction would be complete (new object is created before compacted objects are deleted)
            collect_outcome = collect_next_page(
                &mut stats,
                &self.bucket,
                &self.config.object_prefix,
                keyspace,
                key,
                &data_prefix,
                page_size,
                &self.key_path_parser,
                &continuation,
                &self.continuation_parser,
            )?;
        }

        if collect_outcome.requires_retry && collect_outcome.records.is_empty() {
            // failed twice with no results, return done so that the client doesn't end up in a continuous empty paging loop
            return Ok(Page {
                keyspace: keyspace.to_string(),
                key: key.to_string(),
                continuation: None,
                records: collect_outcome.records,
            });
        }

        log::debug!("s3 read_next_page stats: {:#?}", stats);

        // collect worked, return results
        return Ok(Page {
            keyspace: keyspace.to_string(),
            key: key.to_string(),
            continuation: collect_outcome.continuation(),
            records: collect_outcome.records,
        });
    }
}
