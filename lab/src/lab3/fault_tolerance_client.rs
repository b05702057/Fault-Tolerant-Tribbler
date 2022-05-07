use super::storage_structs::{BackendType, MarkedValue};
use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::RwLock;
use tribbler::{
    colon,
    err::{TribResult, TribblerError},
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

const PREFIX: &str = "PREFIX_";
const SUFFIX: &str = "SUFFIX_";

#[derive(Debug)]
pub struct ReplicateIndices {
    pub primary: usize,
    pub backup: Option<usize>,
}
pub struct StorageFaultToleranceClient {
    pub bin_name: String,
    pub storage_clients: Vec<StorageClient>,
    pub live_http_back_addr_idx: Arc<RwLock<Vec<usize>>>,
}

impl StorageFaultToleranceClient {
    async fn get_key_index_in_list(&self, list: &Vec<String>, key: &str) -> TribResult<usize> {
        for (i, item) in list.iter().enumerate() {
            if item == key {
                return Ok(i);
            }
        }
        Err(Box::new(TribblerError::Unknown(
            "Get key index in list error. Some contention happened".to_string(),
        )))
    }

    async fn remove_key_value(&self, storage: &StorageClient, kv: &KeyValue) -> TribResult<u32> {
        // Use MarkedValue structure format for comparing value
        let mut list = match self.get_dedup_concat_list_struct(storage, &kv.key).await {
            Ok(list) => list,
            Err(err) => return Err(err),
        };

        // Deduplicate to avoid counting replicated values
        list.sort();
        list.dedup();

        let prefix_key = format!("{}{}", PREFIX, kv.key);
        let suffix_key = format!("{}{}", SUFFIX, kv.key);

        let list_iter = list.iter();
        let mut removed: u32 = 0;

        for item in list_iter {
            // Remove from both prefix_list and suffix_list when we find matching value
            if item.value == kv.value {
                _ = storage
                    .list_remove(&KeyValue {
                        key: prefix_key.to_string(),
                        value: serde_json::to_string(item)?,
                    })
                    .await;
                _ = storage
                    .list_remove(&KeyValue {
                        key: suffix_key.to_string(),
                        value: serde_json::to_string(item)?,
                    })
                    .await;
                removed = removed + 1;
            }
        }
        return Ok(removed);
    }

    // Find the lower_bound
    fn lower_bound_in_list(&self, live_https: &Vec<usize>, num: usize) -> usize {
        for (i, https) in live_https.iter().enumerate() {
            if https >= &num {
                return i;
            }
        }

        return 0;
    }

    async fn find_target_backends(&self) -> TribResult<ReplicateIndices> {
        let name = &self.bin_name;

        // Hash bin_name and get replicates' index from live backend list
        let mut hasher = DefaultHasher::new();
        name.hash(&mut hasher);

        let live_https_guard = self.live_http_back_addr_idx.read().await;
        let mut live_https = (*live_https_guard).clone();
        drop(live_https_guard);
        let storage_clients_len = self.storage_clients.len();
        let primary_hash = hasher.finish() % storage_clients_len as u64;

        live_https.sort();
        //println!("Live https: {:?}", live_https);

        // Get lower_bound for primary backend and the next backend as backup
        let primary_idx = self.lower_bound_in_list(&live_https, primary_hash as usize);
        let backup_idx = (primary_idx + 1) % live_https.len() as usize;

        if primary_idx != backup_idx {
            return Ok(ReplicateIndices {
                primary: live_https[primary_idx],
                backup: Some(live_https[backup_idx]),
            });
        }

        Ok(ReplicateIndices {
            primary: live_https[primary_idx],
            backup: None,
        })
    }

    // Get the list returning to client -> contains only ordered value
    async fn get_processed_list(
        &self,
        storage: &StorageClient,
        key: &str,
    ) -> TribResult<Vec<String>> {
        let prefix_key = format!("{}{}", PREFIX, key);
        let suffix_key = format!("{}{}", SUFFIX, key);

        let prefix_list = match storage.list_get(&prefix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };
        let suffix_list = match storage.list_get(&suffix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };

        // concatinate prefix and suffix lists and eliminate the overlapped part
        let concat_list = match self.dedup_and_concat_two_list_struct(&prefix_list, &suffix_list) {
            Ok(list) => list,
            Err(err) => return Err(err),
        };
        Ok(self.get_consistent_order_value(concat_list).await)
    }

    // The whole list with key and the output as vector of serialized string of MarkedValue structure.
    async fn get_dedup_concat_list_string(
        &self,
        storage: &StorageClient,
        key: &str,
    ) -> TribResult<Vec<String>> {
        let prefix_key = format!("{}{}", PREFIX, key);
        let suffix_key = format!("{}{}", SUFFIX, key);

        let prefix_list = match storage.list_get(&prefix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };
        let suffix_list = match storage.list_get(&suffix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };

        self.dedup_and_concat_two_list_string(&prefix_list, &suffix_list)
    }

    async fn get_dedup_concat_list_struct(
        &self,
        storage: &StorageClient,
        key: &str,
    ) -> TribResult<Vec<MarkedValue>> {
        let prefix_key = format!("{}{}", PREFIX, key);
        let suffix_key = format!("{}{}", SUFFIX, key);

        let prefix_list = match storage.list_get(&prefix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };
        let suffix_list = match storage.list_get(&suffix_key).await {
            Ok(List(vec)) => vec,
            // storage crashed
            Err(err) => return Err(err),
        };

        self.dedup_and_concat_two_list_struct(&prefix_list, &suffix_list)
    }

    // concatinate two lists as MarkedValue struct
    fn dedup_and_concat_two_list_struct(
        &self,
        prefix_list: &Vec<String>,
        suffix_list: &Vec<String>,
    ) -> TribResult<Vec<MarkedValue>> {
        let mut concat_list = Vec::<MarkedValue>::new();

        if prefix_list.len() == 0 && suffix_list.len() == 0 {
            return Ok(concat_list);
        }

        let mut records = HashSet::<String>::new();

        for item in prefix_list.iter() {
            let item_info: MarkedValue = serde_json::from_str(item)?;
            let unique_id: String =
                format!("{}::{}", item_info.backend_id, item_info.clock).to_string();
            if !records.contains(&unique_id) {
                concat_list.push(item_info);
                records.insert(unique_id);
            }
        }

        for item in suffix_list.iter() {
            let item_info: MarkedValue = serde_json::from_str(item)?;
            let unique_id: String =
                format!("{}::{}", item_info.backend_id, item_info.clock).to_string();
            if !records.contains(&unique_id) {
                concat_list.push(item_info);
                records.insert(unique_id);
            }
        }

        Ok(concat_list)
    }

    // concatinate two lists into serialized string
    fn dedup_and_concat_two_list_string(
        &self,
        prefix_list: &Vec<String>,
        suffix_list: &Vec<String>,
    ) -> TribResult<Vec<String>> {
        let mut concat_list = Vec::<String>::new();
        // Manually for now => optimization
        if prefix_list.len() == 0 && suffix_list.len() == 0 {
            return Ok(concat_list);
        }

        let mut records = HashSet::<String>::new();
        for item in prefix_list.iter() {
            let item_info: MarkedValue = serde_json::from_str(item)?;
            let unique_id: String =
                format!("{}::{}", item_info.backend_id, item_info.clock).to_string();
            if !records.contains(&unique_id) {
                records.insert(unique_id);
                concat_list.push(item.clone().to_string());
            }
        }

        for item in suffix_list.iter() {
            let item_info: MarkedValue = serde_json::from_str(item)?;
            let unique_id: String =
                format!("{}::{}", item_info.backend_id, item_info.clock).to_string();
            if !records.contains(&unique_id) {
                records.insert(unique_id);
                concat_list.push(item.clone().to_string());
            }
        }

        Ok(concat_list)
    }

    //sorting the list_get list to a consistent list
    async fn get_consistent_order_value(&self, mut list: Vec<MarkedValue>) -> Vec<String> {
        let list_iter = list.iter_mut();

        let mut sublist = Vec::<MarkedValue>::new();
        let mut sorted_list = Vec::<String>::new();

        for item in list_iter {
            if item.backend_type == BackendType::Primary {
                if sublist.len() > 0 {
                    sublist.sort();
                    sublist.dedup();
                    for v in sublist.into_iter() {
                        sorted_list.push(v.value);
                    }
                    //sublist.clear();
                    sublist = Vec::<MarkedValue>::new();
                }
                sorted_list.push(item.value.clone());
            } else {
                if sublist.len() > 0 && sublist[0].backend_id != item.backend_id {
                    sublist.sort();
                    sublist.dedup();
                    for v in sublist.into_iter() {
                        sorted_list.push(v.value);
                    }
                    //sublist.clear();
                    sublist = Vec::<MarkedValue>::new();
                }
                sublist.push(item.clone());
            }
        }

        if sublist.len() > 0 {
            sublist.sort();
            sublist.dedup();
            for v in sublist.into_iter() {
                sorted_list.push(v.value);
            }
        }

        return sorted_list;
    }
}

#[async_trait]
impl KeyString for StorageFaultToleranceClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(key));

        // Fault tolerance
        let backend_indices = self.find_target_backends().await?;

        let primary = &self.storage_clients[backend_indices.primary];

        let mut result = primary.get(&translated_key).await;

        match result {
            // When the first backend crashed after BinClient scan through all the backend servers
            Err(_) => {
                match backend_indices.backup {
                    Some(index) => {
                        let backup = &self.storage_clients[index];
                        result = backup.get(&translated_key).await;
                    }
                    None => (),
                };
            }
            // When visited a working backend server before
            // If we cannot get the result might because the "primary" has not finished migration
            // Try getting from backup
            Ok(None) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    match backup.get(&translated_key).await {
                        Err(_) => (),
                        Ok(ret) => result = Ok(ret),
                    }
                }
                None => (),
            },
            // Already get the value, no need to get from backup
            Ok(_) => (),
        }

        result
    }

    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(&kv.key));
        let translated_kv = KeyValue {
            key: translated_key,
            value: kv.value.clone(),
        };

        //Fault tolerance
        let backend_indices = self.find_target_backends().await?;
        let primary = &self.storage_clients[backend_indices.primary];

        let mut result = primary.set(&translated_kv).await;

        match result {
            // When the first backend crashed after BinClient scan through all the backend servers
            // Check set on backup
            Err(_) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    result = backup.set(&translated_kv).await;
                }
                None => (),
            },
            // When visited a working backend server before -> the correct answer should already been returned
            // Still need to do the operation on backup
            Ok(_) => {
                match backend_indices.backup {
                    Some(index) => {
                        let backup = &self.storage_clients[index];
                        _ = backup.set(&translated_kv).await;
                    }
                    // No available backup
                    None => (),
                }
            }
        }

        result
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

        // Fault tolerance
        let backend_indices = self.find_target_backends().await?;

        let primary = &self.storage_clients[backend_indices.primary];

        let mut result = primary.keys(&translated_pattern).await;

        match result {
            Ok(List(vec)) => {
                // Check whether backup contains keys => if the primary has not finished migration
                // return backup's result
                let mut primary_vec = vec;
                match backend_indices.backup {
                    Some(index) => {
                        let backup = &self.storage_clients[index];
                        match backup.keys(&translated_pattern).await {
                            Ok(List(backup_vec)) => {
                                if backup_vec.len() > primary_vec.len() {
                                    primary_vec = backup_vec;
                                }
                            }
                            Err(_) => (),
                        }
                    }
                    None => (),
                }
                result = Ok(List(primary_vec));
            }
            Err(_) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    result = backup.keys(&translated_pattern).await;
                }
                None => (),
            },
        }

        let keys_vec = match result {
            Ok(List(vec)) => vec,
            // Somehow all the replicates cannot be accessed -> should we deal with this situation?
            // although this might not happen in lab3's cases since one backend crashed at a time
            Err(err) => return Err(err),
        };

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
impl KeyList for StorageFaultToleranceClient {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        let escaped_key = colon::escape(key);
        let translated_key = format!("{}::{}", self.bin_name, &escaped_key);

        // Fault tolerance
        let backend_indices = self.find_target_backends().await?;
        let primary = &self.storage_clients[backend_indices.primary];

        let backup_list;
        match backend_indices.backup {
            Some(index) => {
                let backup = &self.storage_clients[index];
                backup_list = self.get_processed_list(backup, &translated_key).await;
            }
            None => {
                backup_list = Err(Box::new(TribblerError::Unknown(
                    "No working backends".to_string(),
                )))
            }
        };
        let primary_list = self.get_processed_list(primary, &translated_key).await;

        // Get list from primary
        match primary_list {
            Ok(p_list) => match backup_list {
                // If backup has longer list -> primary is still migrating
                // then use the result from backup
                Ok(f_list) => {
                    if p_list.len() < f_list.len() {
                        return Ok(List(f_list));
                    }
                    return Ok(List(p_list));
                }
                Err(_) => return Ok(List(p_list)),
            },
            // Get list from backup
            Err(_) => match backup_list {
                Ok(f_list) => return Ok(List(f_list)),
                Err(_) => (),
            },
        }

        return Err(Box::new(TribblerError::Unknown(
            "No working backends".to_string(),
        )));
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        //println!("List append {:?}", kv);
        let escaped_key = colon::escape(&kv.key);
        let translated_key = format!("{}::{}", self.bin_name, escaped_key);
        let typed_translated_key = format!("{}{}", SUFFIX, translated_key);
        // Fault tolerance
        let mut backend_indices = self.find_target_backends().await?;

        //println!("backend_indices: {:?}", backend_indices);

        let primary_value: String;
        let mut translated_kv: KeyValue;

        let mut primary = &self.storage_clients[backend_indices.primary];
        let mut backup: Option<&StorageClient> = match backend_indices.backup {
            Some(idx) => Some(&self.storage_clients[idx]),
            None => None,
        };

        let backup_list;
        match backend_indices.backup {
            Some(index) => {
                let backup = &self.storage_clients[index];
                backup_list = self.get_processed_list(backup, &translated_key).await;
            }
            None => {
                backup_list = Err(Box::new(TribblerError::Unknown(
                    "No working backends".to_string(),
                )))
            }
        };
        let primary_list = self.get_processed_list(primary, &translated_key).await;

        // cases:
        // 1. primary and backup and primary as primary
        // 2. primary and backup and backup as primary
        // 3. only primary working
        // 4. only backup working

        // Deciding primary and backup
        match primary_list {
            Ok(p_list) => match backup_list {
                Ok(f_list) => {
                    // case 2
                    if p_list.len() < f_list.len() {
                        match backup {
                            Some(ret) => {
                                // swap primary and backup
                                let tmp = primary;
                                primary = &ret;
                                backup = Some(tmp);
                                let tmp_idx = match backend_indices.backup {
                                    Some(index) => index,
                                    None => {
                                        return Err(Box::new(TribblerError::Unknown(
                                            "This should not happend. Missing backup index"
                                                .to_string(),
                                        )));
                                    }
                                };
                                backend_indices.backup = Some(backend_indices.primary);
                                backend_indices.primary = tmp_idx;
                            }
                            None => (),
                        }
                    }
                }
                // case 3
                Err(_) => backup = None,
            },
            Err(_) => {
                match backup_list {
                    // case 4
                    Ok(_) => {
                        primary = match backup {
                            Some(storage) => storage,
                            None => {
                                return Err(Box::new(TribblerError::Unknown(
                                    "No available backend".to_string(),
                                )))
                            }
                        };
                    }
                    // no backend is working => this situation should not happen
                    Err(_) => (),
                }
            }
        }

        let result: TribResult<bool>;

        // Append to suffix list
        let primary_clock = primary.clock(0).await?;

        primary_value = serde_json::to_string(&MarkedValue {
            backend_type: BackendType::Primary,
            backend_id: backend_indices.primary,
            clock: primary_clock,
            // Does not know the index for primary
            index: 0,
            value: kv.value.clone(),
        })?;

        translated_kv = KeyValue {
            key: typed_translated_key,
            value: primary_value.clone(),
        };

        result = primary.list_append(&translated_kv).await;
        let primary_appended_list = self
            .get_dedup_concat_list_string(primary, &translated_key)
            .await?;

        // Get index in the concat list
        let index = self
            .get_key_index_in_list(&primary_appended_list, &primary_value)
            .await?;

        // Add index information for value passed into backup -> for consistent ordering
        let backup_value = serde_json::to_string(&MarkedValue {
            backend_type: BackendType::Backup,
            backend_id: backend_indices.primary,
            clock: primary_clock,
            index: index,
            value: kv.value.clone(),
        })?;

        match backup {
            Some(backup_storage) => {
                //println!("Append to backup {:?}", backup_value);
                translated_kv.value = backup_value;
                _ = backup_storage.list_append(&translated_kv).await;
            }
            None => (),
        }

        result
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        let translated_key = format!("{}::{}", self.bin_name, colon::escape(&kv.key));
        let translated_kv = KeyValue {
            key: translated_key,
            value: kv.value.clone(),
        };

        // Fault tolerance
        let backend_indices = self.find_target_backends().await?;
        let primary = &self.storage_clients[backend_indices.primary];
        let mut result = self.remove_key_value(primary, &translated_kv.clone()).await;

        match result {
            Err(_) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    result = self.remove_key_value(backup, &translated_kv.clone()).await;
                }
                None => (),
            },
            Ok(primary_removed) => {
                // Remove key value in backup and check whether it removed more in backup -> primary might haven't not finished migration
                match backend_indices.backup {
                    Some(index) => {
                        let backup = &self.storage_clients[index];
                        match self.remove_key_value(backup, &translated_kv.clone()).await {
                            Ok(backup_removed) => {
                                // If backend removes more -> use this removed count for return
                                if backup_removed > primary_removed {
                                    result = Ok(backup_removed);
                                }
                            }
                            Err(_) => (),
                        }
                    }
                    None => (),
                }
            }
        }

        result
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    ///
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        let bin_name_and_separator_prefix = format!("{}::", self.bin_name);
        let translated_prefix = format!(
            "{}{}",
            bin_name_and_separator_prefix,
            colon::escape(&p.prefix)
        );

        let escaped_suffix = colon::escape(&p.suffix);

        let prefix_translated_pattern = Pattern {
            prefix: format!("{}{}", PREFIX, translated_prefix.clone()),
            suffix: escaped_suffix.clone(),
        };
        let suffix_translated_pattern = Pattern {
            prefix: format!("{}{}", SUFFIX, translated_prefix.clone()),
            suffix: escaped_suffix.clone(),
        };

        //Fault tolerance
        let backend_indices = self.find_target_backends().await?;

        let mut matched_keys: HashSet<String> = HashSet::new();
        let primary = &self.storage_clients[backend_indices.primary];

        // Insert all the matching keys in primary and backups to hashset
        // then we don't need to do the comparison since one should be a subset of another or they are the same
        match primary.list_keys(&prefix_translated_pattern).await {
            Ok(List(keys)) => {
                //println!("list_keys primary prefix_list len: {:?}", keys.len());
                for k in keys.iter() {
                    let key_split: Vec<&str> = k.split(PREFIX).collect();
                    matched_keys.insert(key_split[1].to_string());
                }
            }
            Err(_) => (),
        }
        match primary.list_keys(&suffix_translated_pattern).await {
            Ok(List(keys)) => {
                //println!("list_keys primary suffix_list len: {:?}", keys.len());
                for k in keys.iter() {
                    let key_split: Vec<&str> = k.split(SUFFIX).collect();
                    matched_keys.insert(key_split[1].to_string());
                }
            }
            Err(_) => (),
        }

        //println!("[DEBUGGING] Backend_indices: {:?}", backend_indices);

        match backend_indices.backup {
            Some(index) => {
                let backup = &self.storage_clients[index];
                match backup.list_keys(&prefix_translated_pattern).await {
                    Ok(List(keys)) => {
                        //println!("list_keys backup prefix_list len: {:?}", keys.len());
                        for k in keys.iter() {
                            let key_split: Vec<&str> = k.split(PREFIX).collect();
                            matched_keys.insert(key_split[1].to_string());
                        }
                    }
                    Err(_) => (),
                }
                match backup.list_keys(&suffix_translated_pattern).await {
                    Ok(List(keys)) => {
                        //println!("list_keys backup suffix_list len: {:?}", keys.len());
                        for k in keys.iter() {
                            let key_split: Vec<&str> = k.split(SUFFIX).collect();
                            matched_keys.insert(key_split[1].to_string());
                        }
                    }
                    Err(_) => (),
                }
            }
            None => (),
        }

        let keys_vec: Vec<String> = matched_keys.into_iter().collect();

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

        //println!("prefix_stripped_keys: {:?}", prefix_stripped_keys);
        Ok(List(prefix_stripped_keys))
    }
}

#[async_trait]
impl Storage for StorageFaultToleranceClient {
    /// Returns an auto-incrementing clock. The returned value of each call will
    /// be unique, no smaller than `at_least`, and strictly larger than the
    /// value returned last time, unless it was [u64::MAX]
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        // Fault tolerance
        let backend_indices = self.find_target_backends().await?;
        let primary = &self.storage_clients[backend_indices.primary];

        let mut result = primary.clock(at_least).await;
        match result {
            // When the first backend crashed after BinClient scan through all the backend servers
            Err(_) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    result = backup.clock(at_least).await;
                }
                None => (),
            },
            // When visited a working backend server before -> the correct answer should already been returned
            Ok(_) => match backend_indices.backup {
                Some(index) => {
                    let backup = &self.storage_clients[index];
                    _ = backup.clock(at_least).await;
                }
                None => (),
            },
        }

        result
    }
}
