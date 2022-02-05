use crate::StoreError;
use ini::Ini;
use std::str::FromStr;

#[derive(Debug, Clone)]
pub struct S3StoreConfig {
    pub object_prefix: String,
    pub bucket_name: Option<String>,
    pub endpoint: Option<String>,
    pub region: String,
    pub path_style: bool,
    pub use_default_credentials: bool,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub security_token: Option<String>,
    pub session_token: Option<String>,
    pub profile: Option<String>,
    pub max_cached_keys: usize,
    pub compact_items_threshold: u64,
    pub compact_size_threshold: u64,
    pub compact_objects_threshold: u64,
    pub default_max_results: u64,
}
impl S3StoreConfig {
    pub fn new() -> Self {
        Self {
            object_prefix: "".to_string(),
            bucket_name: None,
            endpoint: None,
            region: "us-east-1".to_string(),
            path_style: false,
            use_default_credentials: true,
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
            profile: None,
            max_cached_keys: 100 * 1024, // 100k
            compact_items_threshold: 1000,
            compact_size_threshold: 1024 * 1024, // 1MB
            compact_objects_threshold: 100,
            default_max_results: 1000,
        }
    }
    /// object prefix, defaults to an empty string, which would put the keyspace at the root of the bucket
    pub fn set_object_prefix(mut self, v: String) -> Self {
        self.endpoint = Some(v);
        self
    }
    /// required, the bucket name
    pub fn set_bucket_name(mut self, v: String) -> Self {
        self.bucket_name = Some(v);
        self
    }
    /// optional, used to override the S3 endpoint
    pub fn set_endpoint(mut self, v: String) -> Self {
        self.endpoint = Some(v);
        self
    }
    /// optional, defaults to us-east-1
    pub fn set_region(mut self, v: String) -> Self {
        self.region = v;
        self
    }
    /// enable path-style endpoints, defaults to false
    pub fn set_path_style(mut self, v: bool) -> Self {
        self.path_style = v;
        self
    }
    /// use default credentials instead of given access keys, defaults to true
    pub fn set_use_default_credentials(mut self, v: bool) -> Self {
        self.use_default_credentials = v;
        self
    }
    /// optional, used when use_default_credentials=false
    pub fn set_access_key(mut self, v: String) -> Self {
        self.access_key = Some(v);
        self
    }
    /// optional, used when use_default_credentials=false
    pub fn set_secret_key(mut self, v: String) -> Self {
        self.secret_key = Some(v);
        self
    }
    /// optional, used when use_default_credentials=false
    pub fn set_security_token(mut self, v: String) -> Self {
        self.security_token = Some(v);
        self
    }
    /// optional, used when use_default_credentials=false
    pub fn set_session_token(mut self, v: String) -> Self {
        self.session_token = Some(v);
        self
    }
    /// optional, used when use_default_credentials=false
    pub fn set_profile(mut self, v: String) -> Self {
        self.profile = Some(v);
        self
    }
    /// set the maximum number of cached keys kept in memory in the writer, defaults to 100k
    pub fn set_max_cached_keys(mut self, v: usize) -> Self {
        self.max_cached_keys = v;
        self
    }
    /// set the item count threshold to trigger compaction of a complete batch, defaults to 1000
    pub fn set_compact_items_threshold(mut self, v: u64) -> Self {
        self.compact_items_threshold = v;
        self
    }
    /// set the object count threshold to trigger compaction of a partial batch, defaults to 100
    pub fn set_compact_objects_threshold(mut self, v: u64) -> Self {
        self.compact_objects_threshold = v;
        self
    }
    /// set the size threshold to trigger object compaction of a complete batch, defaults to 1MiB
    pub fn set_compact_size_threshold(mut self, v: u64) -> Self {
        self.compact_size_threshold = v;
        self
    }
    /// set the default number of max results used when none is defined in the request
    pub fn set_default_max_results(mut self, v: u64) -> Self {
        self.default_max_results = v;
        self
    }
    pub fn load(ini: &Ini) -> Result<Self, StoreError> {
        let s3 = match ini.section(Some("s3")) {
            None => {
                return Err(StoreError::BadConfiguration(
                    "[s3] config missing".to_string(),
                ))
            }
            Some(v) => v,
        };
        let mut cfg = Self::new();
        if let Some(v) = s3.get("object_prefix") {
            cfg = cfg.set_object_prefix(v.to_string());
        }
        if let Some(v) = s3.get("bucket_name") {
            cfg = cfg.set_bucket_name(v.to_string());
        }
        if let Some(v) = s3.get("endpoint") {
            cfg = cfg.set_endpoint(v.to_string());
        }
        if let Some(v) = s3.get("region") {
            cfg = cfg.set_region(v.to_string());
        }
        if let Some(v) = s3.get("path_style") {
            match bool::from_str(v) {
                Ok(v) => cfg = cfg.set_path_style(v),
                Err(_) => return Err(StoreError::BadConfiguration("s3 path_style".to_string())),
            }
        }
        if let Some(v) = s3.get("use_default_credentials") {
            match bool::from_str(v) {
                Ok(v) => cfg = cfg.set_use_default_credentials(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 use_default_credentials".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("access_key") {
            cfg = cfg.set_access_key(v.to_string());
        }
        if let Some(v) = s3.get("secret_key") {
            cfg = cfg.set_secret_key(v.to_string());
        }
        if let Some(v) = s3.get("security_token") {
            cfg = cfg.set_security_token(v.to_string());
        }
        if let Some(v) = s3.get("session_token") {
            cfg = cfg.set_session_token(v.to_string());
        }
        if let Some(v) = s3.get("profile") {
            cfg = cfg.set_profile(v.to_string());
        }
        if let Some(v) = s3.get("max_cached_keys") {
            match usize::from_str(v) {
                Ok(v) => cfg = cfg.set_max_cached_keys(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 max_cached_keys".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("compact_items_threshold") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_compact_items_threshold(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 compact_items_threshold".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("compact_size_threshold") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_compact_size_threshold(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 compact_size_threshold".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("compact_objects_threshold") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_compact_objects_threshold(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 compact_objects_threshold".to_string(),
                    ))
                }
            }
        }
        if let Some(v) = s3.get("default_max_results") {
            match u64::from_str(v) {
                Ok(v) => cfg = cfg.set_default_max_results(v),
                Err(_) => {
                    return Err(StoreError::BadConfiguration(
                        "s3 default_max_results".to_string(),
                    ))
                }
            }
        }
        return Ok(cfg);
    }
}
