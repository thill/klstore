use crate::*;

pub fn keyspace_metadata_from_ini(ini: &ini::Ini) -> Result<KeyspaceMetadata, StoreError> {
    match ini.section(Some("keyspace")) {
        Some(section) => match section.get("created") {
            Some(created) => match created.parse::<i64>() {
                Ok(created) => Ok(KeyspaceMetadata {
                    created_timestamp: created,
                }),
                Err(_) => Err(StoreError::BadData(
                    "invalid keyspace created_timestamp".to_string(),
                )),
            },
            None => Err(StoreError::BadData(format!(
                "missing keyspace created_timestamp",
            ))),
        },
        None => Err(StoreError::BadData(
            format!("missing [keyspace] in config",),
        )),
    }
}
