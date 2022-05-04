use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tribbler::err::TribResult;

use crate::keeper::{
    trib_storage_client::TribStorageClient,
    {Address, Bool, Clock, Key},
};

pub struct KeeperClient {
    pub addrs: Arc<RwLock<Vec<String>>>, // the addresses of all keepers
    pub statuses: Arc<RwLock<Vec<bool>>>, // the statuses of all keepers
    pub client_opts: Arc<Mutex<Vec<Option<TribStorageClient<tonic::transport::Channel>>>>>,
    pub this: Arc<RwLock<usize>>,    // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>, // to sync
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists as a helper
    pub event_backend: Arc<RwLock<String>>, // hold the lock when the keeper runs scan()
    pub scan_count: Arc<RwLock<u64>>, // count #scan since the clock is sent
    pub is_new: Arc<RwLock<bool>>,
}

impl KeeperClient {
    pub fn new(addrs: Vec<String>, this: usize) -> KeeperClient {
        let mut statuses = Vec::<bool>::new();
        let mut client_opts = Vec::<Option<TribStorageClient<tonic::transport::Channel>>>::new();
        for _ in &addrs {
            statuses.push(false);
            client_opts.push(None);
        }

        KeeperClient {
            addrs: Arc::new(RwLock::new(addrs)),
            statuses: Arc::new(RwLock::new(statuses)),
            client_opts: Arc::new(Mutex::new(client_opts)),
            this: Arc::new(RwLock::new(this)),
            timestamp: Arc::new(RwLock::new(0)),
            key_list: Arc::new(RwLock::new(HashSet::<String>::new())),
            event_backend: Arc::new(RwLock::new("".to_string())),
            scan_count: Arc::new(RwLock::new(0)),
            is_new: Arc::new(RwLock::new(true)),
        }
    }

    // Connects client if not already connected
    pub async fn connect(&self, idx: usize) -> TribResult<()> {
        let mut client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        // To prevent unnecessary reconnection, we only connect if we haven't.
        if let None = client_opts[idx] {
            let addr = &self.addrs.read().await[idx];
            client_opts[idx] = Some(TribStorageClient::connect(addr.clone()).await?);
            drop(addr);
        }
        Ok(())
    }

    pub async fn send_clock(&self, idx: usize, is_first_clock: bool) -> TribResult<bool> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        self.connect(idx).await?;
        let client_opt = &client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(client_opts);

        let timestamp = self.timestamp.read().await;
        client
            .send_clock(Clock {
                timestamp: *timestamp,
                idx: idx.try_into().unwrap(),
                is_first_clock,
            })
            .await?;
        drop(timestamp);
        Ok(true)
    }

    pub async fn send_key(&self, idx: usize, key: String) -> TribResult<bool> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        self.connect(idx).await?;
        let client_opt = &client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(client_opts);
        client.send_key(Key { key }).await?;
        Ok(true)
    }
}
