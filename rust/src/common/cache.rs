use crate::*;
use linked_hash_map::LinkedHashMap;
use std::cell::RefCell;

pub trait CacheFetcher<K> {
    fn load_key(&self, keyspace: &str, key: &str) -> Result<K, StoreError>;
}

pub struct StoreCache<K, L: CacheFetcher<K>> {
    loader: L,
    max_cached_keys: usize,
    keys: RefCell<LinkedHashMap<(String, String), K>>,
}
impl<K: Clone, L: CacheFetcher<K>> StoreCache<K, L> {
    pub fn new(loader: L, max_cached_keys: usize) -> Self {
        Self {
            loader,
            max_cached_keys,
            keys: RefCell::new(LinkedHashMap::new()),
        }
    }
    pub fn get_or_read_key(&self, keyspace: &str, key: &str) -> Result<K, StoreError> {
        let mapk = (keyspace.to_string(), key.to_string());
        let mut keys = self.keys.borrow_mut();
        if let Some(k) = keys.get(&mapk) {
            return Ok(k.clone());
        }
        let result = match self.loader.load_key(keyspace, key) {
            Ok(k) => {
                keys.insert(mapk.clone(), k.clone());
                k
            }
            Err(err) => return Err(err),
        };
        if keys.len() > self.max_cached_keys && keys.len() > 1 {
            keys.pop_front();
        }
        Ok(result)
    }
    pub fn set_key(&self, keyspace: &str, key: &str, value: K) {
        let mapk = (keyspace.to_string(), key.to_string());
        let mut keys = self.keys.borrow_mut();
        if let None = keys.insert(mapk, value) {
            if keys.len() > self.max_cached_keys && keys.len() > 1 {
                keys.pop_front();
            }
        }
    }
}
