use super::config::S3StoreConfig;
use crate::*;
use aws_s3::bucket::Bucket;
use aws_s3::Region;
use awscreds::Credentials;

pub fn create(config: &S3StoreConfig) -> Result<Bucket, StoreError> {
    if let None = config.bucket_name {
        return Err(StoreError::BadConfiguration(
            "bucket_name not defined".to_string(),
        ));
    }
    let bucket_name = config.bucket_name.clone().unwrap();
    let region: Region = match config.endpoint.clone() {
        Some(endpoint) => Region::Custom {
            region: config.region.clone(),
            endpoint: endpoint,
        },
        None => {
            let region: Result<Region, _> = config.region.parse();
            match region {
                Ok(region) => region,
                Err(_) => {
                    return Err(StoreError::BadConfiguration(format!(
                        "invalid region: {}",
                        config.region
                    )))
                }
            }
        }
    };

    let credentials = match config.use_default_credentials {
        true => Credentials::default(),
        false => {
            let mut access_key: Option<&str> = None;
            let mut secret_key: Option<&str> = None;
            let mut security_token: Option<&str> = None;
            let mut session_token: Option<&str> = None;
            let mut profile: Option<&str> = None;
            if let Some(v) = &config.access_key {
                access_key = Some(v);
            }
            if let Some(v) = &config.secret_key {
                secret_key = Some(v);
            }
            if let Some(v) = &config.security_token {
                security_token = Some(v);
            }
            if let Some(v) = &config.session_token {
                session_token = Some(v);
            }
            if let Some(v) = &config.profile {
                profile = Some(v);
            }
            Credentials::new(
                access_key,
                secret_key,
                security_token,
                session_token,
                profile,
            )
        }
    };
    if let Err(err) = credentials {
        return Err(StoreError::BadConfiguration(format!(
            "could not create credentials: {}",
            err.to_string()
        )));
    }
    let credentials = credentials.unwrap();
    if config.path_style {
        return match Bucket::new_with_path_style(&bucket_name, region, credentials) {
            Ok(bucket) => Ok(bucket),
            Err(err) => Err(StoreError::BadConfiguration(format!(
                "could not create bucket: {}",
                err.to_string(),
            ))),
        };
    } else {
        return match Bucket::new(&bucket_name, region, credentials) {
            Ok(bucket) => Ok(bucket),
            Err(err) => Err(StoreError::BadConfiguration(format!(
                "could not create bucket: {}",
                err.to_string(),
            ))),
        };
    }
}

pub fn list_exhaustive(
    bucket: &Bucket,
    prefix: &str,
    mut start_from: Option<String>,
) -> Result<Vec<String>, StoreError> {
    let mut results: Vec<String> = Vec::new();
    let mut s3_cont_token: Option<String> = None;
    loop {
        let (list, next_cont_token) = list_page(bucket, prefix, start_from, s3_cont_token, None)?;
        for obj in list {
            results.push(obj);
        }
        start_from = None;
        if let None = next_cont_token {
            break;
        }
        s3_cont_token = next_cont_token;
    }
    return Ok(results);
}

// pub fn list_first(
//     bucket: &Bucket,
//     prefix: &str,
//     start_from: Option<String>,
// ) -> Result<Option<String>, StoreError> {
//     let list = match bucket.list_page(prefix.to_string(), None, None, start_from, Some(1)) {
//         Ok((list, 200)) => list,
//         Ok((_, code)) => return Err(StoreError::IOError(format!("code {}", code))),
//         Err(err) => return Err(StoreError::IOError(err.to_string())),
//     };
//     if list.contents.is_empty() {
//         return Ok(None);
//     } else {
//         return Ok(Some(list.contents[0].key.to_string()));
//     }
// }

pub fn list_page(
    bucket: &Bucket,
    prefix: &str,
    start_from: Option<String>,
    s3_cont_token: Option<String>,
    max_results: Option<usize>,
) -> Result<(Vec<String>, Option<String>), StoreError> {
    let mut results: Vec<String> = Vec::new();
    let list = match bucket.list_page(
        prefix.to_string(),
        None,
        s3_cont_token,
        start_from,
        max_results,
    ) {
        Ok((list, 200)) => list,
        Ok((_, code)) => return Err(StoreError::IOError(format!("code {}", code))),
        Err(err) => return Err(StoreError::IOError(err.to_string())),
    };
    for obj in &list.contents {
        results.push(obj.key.clone());
    }
    return Ok((results, list.next_continuation_token));
}

pub fn put_object(bucket: &Bucket, object_path: String, buffer: &[u8]) -> Result<(), StoreError> {
    match bucket.put_object(object_path, &buffer) {
        Ok((_, 200)) => Ok(()),
        Ok((_, code)) => Err(StoreError::IOError(format!("code {}", code))),
        Err(err) => Err(StoreError::IOError(err.to_string())),
    }
}

pub fn delete_object(bucket: &Bucket, object_path: String) -> Result<(), StoreError> {
    match bucket.delete_object(object_path) {
        Ok((_, 200)) => Ok(()),
        Ok((_, 204)) => Ok(()),
        Ok((_, code)) => Err(StoreError::IOError(format!("code {}", code))),
        Err(err) => Err(StoreError::IOError(err.to_string())),
    }
}

pub fn get_object_optional(bucket: &Bucket, path: String) -> Result<Option<Vec<u8>>, StoreError> {
    match bucket.get_object(path.clone()) {
        Ok((contents, 200)) => Ok(Some(contents)),
        Ok((_, 404)) => Ok(None),
        Ok((_, code)) => Err(StoreError::IOError(format!("code {}", code))),
        Err(err) => Err(StoreError::IOError(err.to_string())),
    }
}

pub fn get_object_required(bucket: &Bucket, path: String) -> Result<Vec<u8>, StoreError> {
    match get_object_optional(bucket, path) {
        Ok(Some(contents)) => Ok(contents),
        Ok(None) => Err(StoreError::BadData(format!("object not found"))),
        Err(err) => Err(err),
    }
}
