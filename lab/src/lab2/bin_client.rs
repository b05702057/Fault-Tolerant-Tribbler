use crate::lab1::new_client;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tribbler::{
    colon,
    err::TribResult,
    storage::{BinStorage, KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

pub struct BinClient {
    /// The addresses of back-ends prefixed with "http://"" i.e. "http://<host>:<port>""
    pub http_back_addrs: Vec<String>,
}

impl BinClient {
    pub fn new(back_addrs: Vec<String>) -> BinClient {
        // Assumes valid backend addresses. If not valid will return error when storage
        // operations is requested
        let http_back_addrs = back_addrs
            .into_iter()
            .map(|back_addr| format!("http://{}", back_addr))
            .collect::<Vec<String>>();
        BinClient {
            http_back_addrs: http_back_addrs,
        }
    }
}

#[async_trait]
impl BinStorage for BinClient {
    async fn bin(&self, name: &str) -> TribResult<Box<dyn Storage>> {
        // Return a StorageClientMapperWrapper configured with the appropriate server
        // address by hashing the bin name

        // First escape colons to get the bin name the client will use to avoid
        // misinterpration of colons. As a result, the escaped bin name can always
        // be inferred by the substring before the first double colon "::". See
        // StorageClientMapperWrapper implementation for more details.
        // This is done so that if there a test on Storage client for this edge case,
        // it will pass, but my implementation of tribbler front won't use any bin names
        // with colons anyway.
        let escaped_name = colon::escape(name);

        let mut hasher = DefaultHasher::new();
        escaped_name.hash(&mut hasher);
        let client_idx = hasher.finish() % self.http_back_addrs.len() as u64;
        let http_back_addr = &self.http_back_addrs[client_idx as usize];

        Ok(Box::new(StorageClientMapperWrapper {
            bin_name: escaped_name.to_string(),
            storage_client: new_client(http_back_addr).await?,
        }))
    }
}

// Struct that wraps lab1's client Storage RPCs into those that accepts bin
// name as prefix of the key to provide logical separate KV stores per bin.
pub struct StorageClientMapperWrapper {
    pub bin_name: String, // assumes already escaped colons
    pub storage_client: Box<dyn Storage>,
}

#[async_trait]
impl KeyString for StorageClientMapperWrapper {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(key));
        self.storage_client.get(&translated_key).await
    }

    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(&kv.key));
        let translated_kv = KeyValue {
            key: translated_key,
            value: kv.value.clone(),
        };
        self.storage_client.set(&translated_kv).await
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        let bin_name_and_separator_prefix = format!("{}::", self.bin_name);
        let translated_prefix = format!(
            "{}{}",
            bin_name_and_separator_prefix,
            colon::escape(&p.prefix)
        );

        let escaped_suffix = colon::escape(&p.suffix);
        let translated_pattern = Pattern {
            prefix: translated_prefix,
            suffix: escaped_suffix,
        };
        let keys_vec = self.storage_client.keys(&translated_pattern).await?.0;

        // Strip bin_name_and_separator_prefix and unescape before returning.
        let prefix_stripped_keys: Vec<String> = keys_vec
            .into_iter()
            .map(|prefixed_key| {
                colon::unescape(
                    prefixed_key
                        .get(bin_name_and_separator_prefix.len()..prefixed_key.len())
                        .unwrap_or("ERROR UNWRAPPING PREFIX STRIPPED KEYS")
                        .to_string(),
                )
            })
            .collect();
        Ok(List(prefix_stripped_keys))
    }
}

#[async_trait]
impl KeyList for StorageClientMapperWrapper {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(key));
        self.storage_client.list_get(&translated_key).await
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(&kv.key));
        let translated_kv = KeyValue {
            key: translated_key,
            value: kv.value.clone(),
        };
        self.storage_client.list_append(&translated_kv).await
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(&kv.key));
        let translated_kv = KeyValue {
            key: translated_key,
            value: kv.value.clone(),
        };
        self.storage_client.list_remove(&translated_kv).await
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let bin_name_and_separator_prefix = format!("{}::", self.bin_name);
        let translated_prefix = format!(
            "{}{}",
            bin_name_and_separator_prefix,
            colon::escape(&p.prefix)
        );

        let escaped_suffix = colon::escape(&p.suffix);
        let translated_pattern = Pattern {
            prefix: translated_prefix,
            suffix: escaped_suffix,
        };
        let keys_vec = self.storage_client.list_keys(&translated_pattern).await?.0;

        // Strip bin_name_and_separator_prefix and unescape before returning.
        let prefix_stripped_keys: Vec<String> = keys_vec
            .into_iter()
            .map(|prefixed_key| {
                colon::unescape(
                    prefixed_key
                        .get(bin_name_and_separator_prefix.len()..prefixed_key.len())
                        .unwrap_or("ERROR UNWRAPPING PREFIX STRIPPED KEYS")
                        .to_string(),
                )
            })
            .collect();
        Ok(List(prefix_stripped_keys))
    }
}

#[async_trait]
impl Storage for StorageClientMapperWrapper {
    /// Returns an auto-incrementing clock. The returned value of each call will
    /// be unique, no smaller than `at_least`, and strictly larger than the
    /// value returned last time, unless it was [u64::MAX]
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        self.storage_client.clock(at_least).await
    }
}