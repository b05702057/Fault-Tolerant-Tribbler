use super::keeper_server::{
    BackendEvent, BackendEventType, DoneEntry, LIST_KEY_TYPE_STR, REGULARY_KEY_TYPE_STR,
};
use crate::keeper::keeper_rpc_client::KeeperRpcClient;
use crate::keeper::Key;
use crate::lab1::client::StorageClient;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tribbler::storage::Storage;

use tribbler::{
    err::TribResult,
    storage::{KeyList, KeyString, KeyValue, List, Pattern},
};

const PREFIX: &str = "PREFIX_";
const SUFFIX: &str = "SUFFIX_";

async fn fetch_all_bins(storage: Arc<StorageClient>) -> TribResult<Vec<String>> {
    // fetch both keys and list_keys => do it seperately
    // fatch all keys whose format would be "bin_name::key", and get the bin_name
    let all_keys = match storage
        .keys(&Pattern {
            prefix: "".to_string(),
            suffix: "".to_string(),
        })
        .await
    {
        Ok(List(vec)) => vec,
        Err(_) => return Err("Backend crashed when doing migration".into()),
    };

    let mut records = HashSet::<String>::new();

    for key in all_keys.iter() {
        // key format, bin_name::key
        // split string and try to get only the bin_name part
        let key_split: Vec<&str> = key.split("::").collect();
        records.insert(key_split[0].to_string());
    }

    let all_list_keys = match storage
        .list_keys(&Pattern {
            prefix: "".to_string(),
            suffix: "".to_string(),
        })
        .await
    {
        Ok(List(vec)) => vec,
        Err(_) => return Err("Backend crashed when gdoing migratino".into()),
    };

    // PREFIX & SUFFIX have the same length
    // truncate the type part from key
    let prefix_len = PREFIX.len();

    for key in all_list_keys.iter() {
        let untyped_key = &key[prefix_len..];
        let key_split: Vec<&str> = untyped_key.split("::").collect();
        records.insert(key_split[0].to_string());
    }

    let keys_vec: Vec<String> = records.into_iter().collect();

    Ok(keys_vec)
}

// Find the lower_bound
fn lower_bound_in_list(live_https: &Vec<usize>, num: usize) -> usize {
    for (i, https) in live_https.iter().enumerate() {
        if https >= &num {
            return i;
        }
    }

    return 0;
}

async fn hash_bin_name_to_backend_idx(
    bin_name: &str,
    live_https: &Vec<usize>,
    storage_clients_len: usize,
) -> TribResult<usize> {
    let mut hasher = DefaultHasher::new();
    bin_name.hash(&mut hasher);

    let primary_hash = hasher.finish() % storage_clients_len as u64;

    let primary_idx = lower_bound_in_list(&live_https, primary_hash as usize);

    Ok(live_https[primary_idx])
}

async fn migration_join(
    new: usize,
    live_https: Vec<usize>,
    storage_clients: Vec<Arc<StorageClient>>,
    last_keeper_clock: u64,
    successor_keeper_client: Option<KeeperRpcClient<tonic::transport::Channel>>,
    // <bin, vector of migrated keys> -> prevent redundant operations
    migration_log: Option<HashSet<DoneEntry>>,
) -> TribResult<bool> {
    let live_https_len = live_https.len();

    let new_node_idx_in_live_https = lower_bound_in_list(&live_https.clone(), new);
    let succ = live_https[(new_node_idx_in_live_https + 1) % live_https_len as usize];

    println!(
        "[DEBUGGING] bin_migration: newly joined backend is index {}, successor is back_idx {}",
        new, succ
    );

    _ = storage_clients[new].clock(last_keeper_clock).await;

    let succ_bins = match fetch_all_bins(Arc::clone(&storage_clients[succ])).await {
        Ok(vec) => vec,
        Err(err) => return Err(err),
    };

    // If there is only one live backend => do nothing
    if live_https_len == 1 {
        return Ok(true);
    }

    // If there are only two backends => copy all data from the old one to another
    if live_https_len == 2 {
        for bin in succ_bins {
            //tokio::spawn(async move {
            match bin_migration(
                &bin.clone(),
                Arc::clone(&storage_clients[succ]),
                Arc::clone(&storage_clients[new]),
                migration_log.clone(),
                successor_keeper_client.clone(),
            )
            .await
            {
                Ok(_) => (),
                Err(_) => return Err("Migration err".into()),
            }
            //});
        }
        return Ok(true);
    }

    // If there are more than three backends => copy from all the users except the ones between new node and its successor
    for bin in succ_bins.iter() {
        let idx = hash_bin_name_to_backend_idx(&bin, &live_https, storage_clients.len()).await?;
        // Add wrap around check
        if check_in_bound_wrap_around_inclusive((new + 1) % storage_clients.len(), succ, idx) {
            continue;
        }
        //tokio::spawn(async move {
        match bin_migration(
            bin,
            Arc::clone(&storage_clients[succ]),
            Arc::clone(&storage_clients[new]),
            migration_log.clone(),
            successor_keeper_client.clone(),
        )
        .await
        {
            Ok(_) => (),
            Err(err) => return Err(err),
        }
        //});
    }

    Ok(true)
}

fn check_in_bound_wrap_around_inclusive(left: usize, right: usize, target: usize) -> bool {
    if left > right {
        if target <= right || target >= left {
            return true;
        } else {
            return false;
        }
    } else {
        if target >= left && target <= right {
            return true;
        } else {
            return false;
        }
    }
}

async fn migration_crash(
    crashed: usize,
    live_https: Vec<usize>,
    storage_clients: Vec<Arc<StorageClient>>,
    // <bin, vector of migrated keys> -> prevent redundant operations
    migration_log: Option<HashSet<DoneEntry>>,
    successor_keeper_client: Option<KeeperRpcClient<tonic::transport::Channel>>,
) -> TribResult<bool> {
    let live_https_len = live_https.len();
    let succ_idx_in_live_https = lower_bound_in_list(&live_https, crashed);
    let succ = live_https[succ_idx_in_live_https];
    let next_succ = live_https[(succ_idx_in_live_https + 1) % live_https_len as usize];
    let pred = live_https[(succ_idx_in_live_https + live_https_len - 1) % live_https_len as usize];
    let prev_pred = live_https
        [(succ_idx_in_live_https + live_https_len + live_https_len - 2) % live_https_len as usize];

    // If there is only one node left => no nothing
    if pred == succ {
        return Ok(true);
    }

    let pred_bins = match fetch_all_bins(Arc::clone(&storage_clients[pred])).await {
        Ok(vec) => vec,
        Err(err) => return Err(err),
    };
    let succ_bins = match fetch_all_bins(Arc::clone(&storage_clients[succ])).await {
        Ok(vec) => vec,
        Err(err) => return Err(err),
    };

    // Move all the data in bin which is hashed into the range (id of node before predecessor, id of predecessor] to crashed node's successor
    for bin in pred_bins.iter() {
        let idx = hash_bin_name_to_backend_idx(bin, &live_https, storage_clients.len()).await?;
        if check_in_bound_wrap_around_inclusive((prev_pred + 1) % storage_clients.len(), pred, idx)
        {
            match bin_migration(
                bin,
                storage_clients[pred].clone(),
                storage_clients[succ].clone(),
                migration_log.clone(),
                successor_keeper_client.clone(),
            )
            .await
            {
                Ok(_) => (),
                Err(err) => return Err(err),
            }
        }
    }

    // Move all the data in bin which is hashed into the range (id of predecessor, crashed_node] to the node succeed crashed node's successor
    for bin in succ_bins.iter() {
        let idx = hash_bin_name_to_backend_idx(bin, &live_https, storage_clients.len()).await?;
        if check_in_bound_wrap_around_inclusive((pred + 1) % storage_clients.len(), crashed, idx) {
            match bin_migration(
                bin,
                storage_clients[succ].clone(),
                storage_clients[next_succ].clone(),
                migration_log.clone(),
                successor_keeper_client.clone(),
            )
            .await
            {
                Ok(_) => (),
                Err(err) => return Err(err),
            }
        }
    }

    Ok(true)
}

async fn bin_migration(
    bin_name: &str,
    from: Arc<StorageClient>,
    to: Arc<StorageClient>,
    // <vector of migrated keys> -> prevent redundant operations
    migration_log: Option<HashSet<DoneEntry>>,
    successor_keeper_client: Option<KeeperRpcClient<tonic::transport::Channel>>,
) -> TribResult<bool> {
    // Copying KeyValue pair
    let keys = from
        .keys(&Pattern {
            prefix: bin_name.clone().to_string(),
            suffix: "".to_string(),
        })
        .await?
        .0;

    // Check whether taking over the task from other keeper
    // If not => set the hashset as empty set
    let log = match migration_log {
        Some(mlog) => mlog,
        None => HashSet::<DoneEntry>::new(),
    };

    // Get value from storage and append it to backend
    for key in keys.iter() {
        // Skip the key that has been migrated before
        let check_entry = DoneEntry {
            key_type: REGULARY_KEY_TYPE_STR.to_string(),
            key: format!("{}::{}", bin_name, key),
        };

        if log.contains(&check_entry) {
            continue;
        }

        let value = match from.get(key).await {
            Ok(Some(val)) => val,
            Ok(None) => continue,
            Err(_) => return Err("Backend crashed when doing migration".into()),
        };
        match to
            .set(&KeyValue {
                key: key.clone(),
                value,
            })
            .await
        {
            Ok(_) => (),
            Err(_) => return Err("Backend crashed when doing migration".into()),
        }

        match &successor_keeper_client {
            Some(k) => {
                let mut k_clone = k.clone();
                _ = k_clone
                    .send_key(Key {
                        key_type: REGULARY_KEY_TYPE_STR.to_string(),
                        key: format!("{}::{}", bin_name, key),
                    })
                    .await;
            }
            None => (),
        }
    }

    // Copying list

    // Keeper sends acknowledgement when receiving both prefix and suffix log
    // Record bin_name::key part to group the operations for the same keys together
    let mut list_keys_hs = HashSet::<String>::new();

    // For stripping PREFIX_ and SUFFIX_
    let prefix_len = PREFIX.len();

    // Should be PREFIX_prefix_username::trib, PREFIX_prefix_username::follow_log
    let prefix_list_keys = from
        .list_keys(&Pattern {
            prefix: format!("{}{}::", PREFIX, &bin_name.clone().to_string()),
            suffix: "".to_string(),
        })
        .await?
        .0;
    // Should be suffix_username::trib, suffix_username::follow_log
    let suffix_list_keys = from
        .list_keys(&Pattern {
            prefix: format!("{}{}::", SUFFIX, &bin_name.clone().to_string()),
            suffix: "".to_string(),
        })
        .await?
        .0;

    for key in prefix_list_keys.iter() {
        let untyped_list_key = &key[prefix_len..];
        list_keys_hs.insert(untyped_list_key.to_string());
    }

    for key in suffix_list_keys.iter() {
        let untyped_list_key = &key[prefix_len..];
        list_keys_hs.insert(untyped_list_key.to_string());
    }

    let list_keys: Vec<String> = list_keys_hs.into_iter().collect();

    // Debugging
    println!(
        "[DEBUGGING] bin_migration: list keys to migrate are {:?}",
        &list_keys
    );

    for key in list_keys.iter() {
        // Skip the key that has been migrated before
        let check_entry = DoneEntry {
            key_type: LIST_KEY_TYPE_STR.to_string(),
            key: format!("{}::{}", bin_name, key),
        };

        if log.contains(&check_entry) {
            continue;
        }
        // Append both entries from prefix list and suffix list to the prefix list in the backend we're copying to
        let prefix_key = format!("{}{}", PREFIX, key);

        let prefix_key_list = match from.list_get(&prefix_key).await {
            Ok(List(vec)) => vec,
            Err(_) => return Err("Backend crashed when doing migration".into()),
        };
        for item in prefix_key_list.iter() {
            // println!(
            //     "[DEBUGGING] bin_migration: moving item from prefix list with key {:} and value {:?}",
            //     key, &item
            // ); // TODO REMOVE TO AVOID EXCESSIVE PRINTING
            match to
                .list_append(&KeyValue {
                    key: format!("{}{}", PREFIX, key),
                    value: item.clone(),
                })
                .await
            {
                Ok(_) => (),
                Err(_) => return Err("Backend crashed when doing migration".into()),
            }
        }

        let suffix_key = format!("{}{}", SUFFIX, key);
        let suffix_key_list = match from.list_get(&suffix_key).await {
            Ok(List(vec)) => vec,
            Err(_) => return Err("Backend crashed when doing migration".into()),
        };
        for item in suffix_key_list.iter() {
            // println!(
            //     "[DEBUGGING] bin_migration: moving item from suffix list with key {:} and value {:?}",
            //     key, &item
            // ); // TODO REMOVE TO AVOID EXCESSIVE PRINTING
            match to
                .list_append(&KeyValue {
                    key: format!("{}{}", PREFIX, key),
                    value: item.clone(),
                })
                .await
            {
                Ok(_) => (),
                Err(_) => return Err("Backend crashed when doing migration".into()),
            }
        }
        // Keeper sends keys to other keeper to acknowledge they finished copying this list
        match &successor_keeper_client {
            Some(k) => {
                let mut k_clone = k.clone();
                _ = k_clone
                    .send_key(Key {
                        key_type: LIST_KEY_TYPE_STR.to_string(),
                        key: format!("{}::{}", &bin_name, key),
                    })
                    .await;
            }
            None => (),
        }
    }

    Ok(true)
}

pub async fn migration_event(
    back_event: &BackendEvent,
    live_https: Vec<usize>,
    storage_clients: Vec<Arc<StorageClient>>,
    migration_log: Option<HashSet<DoneEntry>>,
    last_keeper_clock: u64,
    successor_keeper_client: Option<KeeperRpcClient<tonic::transport::Channel>>,
) -> TribResult<bool> {
    if back_event.event_type == BackendEventType::Join {
        return migration_join(
            back_event.back_idx,
            live_https,
            storage_clients,
            last_keeper_clock,
            successor_keeper_client,
            migration_log,
        )
        .await;
    }
    if back_event.event_type == BackendEventType::Leave {
        return migration_crash(
            back_event.back_idx,
            live_https,
            storage_clients,
            migration_log,
            successor_keeper_client,
        )
        .await;
    }

    Ok(true)
}
