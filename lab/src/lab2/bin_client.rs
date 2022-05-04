use crate::lab1::client::StorageClient;
use async_trait::async_trait;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;
use tribbler::{
    colon,
    err::TribResult,
    rpc::trib_storage_client::TribStorageClient,
    storage::{BinStorage, KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

use rand::Rng;
use std::sync::Arc;
use tokio::sync::RwLock;

const NUM_CLIENTS_SHARED_PER_BACKEND: usize = 2;
const BIN_CLIENT_SCANNING_BACKEND_INTERVAL: Duration = Duration::from_secs(3);
pub struct BinClient {
    /// The addresses of back-ends prefixed with "http://"" i.e. "http://<host>:<port>""
    pub http_back_addrs: Vec<String>,
    /// Vector of client_opts to be shared per backend (corresponding to the back_addrs)
    /// when creating and returning new clients from bin()
    /// First dimension will be pools. 2nd dimension will correspond to backends.
    pub client_opt_pools:
        Vec<Vec<Arc<RwLock<Option<TribStorageClient<tonic::transport::Channel>>>>>>,

    pub live_http_back_addrs: Arc<RwLock<Vec<String>>>,
}

impl BinClient {
    pub fn new(back_addrs: Vec<String>) -> BinClient {
        let back_addrs_len = back_addrs.len();

        // Assumes valid backend addresses. If not valid will return error when storage
        // operations is requested
        let http_back_addrs = back_addrs
            .into_iter()
            .map(|back_addr| format!("http://{}", back_addr))
            .collect::<Vec<String>>();

        let ret_obj = BinClient {
            http_back_addrs: http_back_addrs,
            client_opt_pools: vec![
                vec![Arc::new(RwLock::new(None)); NUM_CLIENTS_SHARED_PER_BACKEND];
                back_addrs_len
            ],
            live_http_back_addrs: Arc::new(RwLock::new(vec![])),
        };

        // Prepare resources needed for periodic scanning
        let mut clients_for_scanning_arcs = vec![];
        for http_addr in ret_obj.http_back_addrs.iter() {
            clients_for_scanning_arcs.push(Arc::new(StorageClient::new(http_addr)));
        }
        let live_http_back_addrs_arc = Arc::clone(&ret_obj.live_http_back_addrs);
        let http_back_addrs_clone = ret_obj.http_back_addrs.clone();

        tokio::spawn(async move {
            Self::periodic_scan(
                http_back_addrs_clone,
                live_http_back_addrs_arc,
                clients_for_scanning_arcs,
            )
            .await
        });

        ret_obj
    }

    async fn periodic_scan(
        http_back_addrs: Vec<String>,
        live_http_back_addrs_arc: Arc<RwLock<Vec<String>>>,
        clients_for_scanning: Vec<Arc<StorageClient>>,
    ) -> TribResult<()> {
        let mut now = std::time::Instant::now();

        loop {
            println!(
                "[DEBUGGING] bin_client's periodic_scan: LOOPING, time since last loop: {} ms",
                now.elapsed().as_millis()
            );
            now = std::time::Instant::now();

            let clients_for_scanning_clones = clients_for_scanning.clone();

            let tasks: Vec<_> = clients_for_scanning_clones
                .into_iter()
                .map(|client| {
                    // Note deliberately NOT adding ";" to the async function as well as the
                    // spawn statements since they are used as expression return results
                    // Clocking to see if server alive
                    tokio::spawn(async move { client.clock(0).await })
                })
                .collect();

            let mut temp_live_http_back_addrs = vec![];

            // Note chaining of "??" is needed. One is for the tokio's spawned task error
            // capturing (a Result<>) and the other is from our connect() function which
            // is also another Result
            for (back_vec_idx, task) in tasks.into_iter().enumerate() {
                // If connection successful, then server is live
                match task.await? {
                    Ok(_) => temp_live_http_back_addrs.push(http_back_addrs[back_vec_idx].clone()),
                    Err(_) => (),
                }
            }

            println!(
                "[DEBUGGING] bin_client's periodic_scan: http_back_addrs.len(): {}",
                http_back_addrs.len()
            );
            println!(
                "[DEBUGGING] bin_client's periodic_scan: live_addrs.len(): {}",
                temp_live_http_back_addrs.len()
            );

            let mut live_http_back_addrs = live_http_back_addrs_arc.write().await;
            *live_http_back_addrs = temp_live_http_back_addrs;
            drop(live_http_back_addrs); // Important to drop lock, don't hold across sleep below

            // Wait interval til next scan
            tokio::time::sleep(BIN_CLIENT_SCANNING_BACKEND_INTERVAL).await;
        }
    }

    // async fn periodic_scan(
    //     http_back_addrs: Vec<String>,
    //     live_http_back_addrs_arc: Arc<RwLock<Vec<String>>>,
    //     clients_for_scanning: StorageClient,
    // ) -> TribResult<()> {
    //     let mut now = std::time::Instant::now();

    //     loop {
    //         println!(
    //             "[DEBUGGING] bin_client's periodic_scan: LOOPING, time since last loop: {} ms",
    //             now.elapsed().as_millis()
    //         );
    //         now = std::time::Instant::now();

    //         let http_back_addrs_clone = http_back_addrs.clone();

    //         let tasks: Vec<_> = http_back_addrs_clone
    //             .into_iter()
    //             .map(|http_back_addr| {
    //                 // Note deliberately NOT adding ";" to the async function as well as the
    //                 // spawn statements since they are used as expression return results
    //                 // Attempt connection
    //                 tokio::spawn(async move { TribStorageClient::connect(http_back_addr).await })
    //             })
    //             .collect();

    //         let mut temp_live_http_back_addrs = vec![];

    //         // Note chaining of "??" is needed. One is for the tokio's spawned task error
    //         // capturing (a Result<>) and the other is from our connect() function which
    //         // is also another Result
    //         for (back_vec_idx, task) in tasks.into_iter().enumerate() {
    //             // If connection successful, then server is live
    //             match task.await? {
    //                 Ok(_) => temp_live_http_back_addrs.push(http_back_addrs[back_vec_idx].clone()),
    //                 Err(_) => (),
    //             }
    //         }

    //         println!(
    //             "[DEBUGGING] bin_client's periodic_scan: http_back_addrs.len(): {}",
    //             http_back_addrs.len()
    //         );
    //         println!(
    //             "[DEBUGGING] bin_client's periodic_scan: live_addrs.len(): {}",
    //             temp_live_http_back_addrs.len()
    //         );

    //         let mut live_http_back_addrs = live_http_back_addrs_arc.write().await;
    //         *live_http_back_addrs = temp_live_http_back_addrs;
    //         drop(live_http_back_addrs); // Important to drop lock, don't hold across sleep below

    //         // Wait interval til next scan
    //         tokio::time::sleep(BIN_CLIENT_SCANNING_BACKEND_INTERVAL).await;
    //     }
    // }
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

        // Random connection in pool
        let mut rng = rand::thread_rng();
        let rand_idx_in_pool = rng.gen_range(0..NUM_CLIENTS_SHARED_PER_BACKEND);

        let client_opt = Arc::clone(&self.client_opt_pools[client_idx as usize][rand_idx_in_pool]);

        // TODO: change initialization to 
        // client_opt_pools: vec![
        //     vec![Arc::new(RwLock::new(None)); back_addrs_len];
        //     NUM_CLIENTS_SHARED_PER_BACKEND
        // ],
        // let client_opt_vec = self.client_opt_pools[rand_idx_in_pool].clone(); // cloned vector (which recursively clones Arcs)
        // let mut storage_client_vec = vec![];
        // for client_opt in client_opt_vec {
        //     storage_client_vec.push(Box::new(StorageClient::new_with_client_opt(
        //         http_back_addr,
        //         client_opt,
        //     )));
        // }
        

        Ok(Box::new(StorageClientMapperWrapper {
            bin_name: escaped_name.to_string(),
            storage_client: Box::new(StorageClient::new_with_client_opt(
                http_back_addr,
                client_opt,
            )),
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
