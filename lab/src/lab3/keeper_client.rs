use std::collections::HashSet;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tribbler::config::KeeperConfig;
use tribbler::err::TribResult;

use crate::keeper::{
    trib_storage_client::TribStorageClient,
    {Acknowledgement, Bool, Clock, Key},
};

pub struct KeeperClient {
    pub addrs: Arc<RwLock<Vec<String>>>, // the addresses of all keepers
    pub statuses: Arc<RwLock<Vec<bool>>>, // the statuses of all keepers
    pub client_opts: Arc<Mutex<Vec<Option<TribStorageClient<tonic::transport::Channel>>>>>,
    pub this: Arc<RwLock<usize>>,    // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>, // to sync
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists as a helper
    pub acknowledged_event: Arc<RwLock<String>>, // hold the lock when the keeper runs scan()
    pub acknowledged_time: Arc<RwLock<Instant>>,
    pub scan_count: Arc<RwLock<u64>>, // count #scan since the clock is sent
    pub is_new: Arc<RwLock<bool>>,
    pub ready: Option<Sender<bool>>,
    pub mutex: Mutex<u64>,
}

impl KeeperClient {
    pub fn new(kc: KeeperConfig) -> KeeperClient {
        let addrs = kc.addrs;
        let this = kc.this;
        let ready = kc.ready;

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
            acknowledged_event: Arc::new(RwLock::new("".to_string())),
            acknowledged_time: Arc::new(RwLock::new(Instant::now())),
            scan_count: Arc::new(RwLock::new(0)),
            is_new: Arc::new(RwLock::new(true)),
            ready,
            mutex: Mutex::new(0),
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

    pub async fn send_clock(
        &self,
        idx: usize,
        initializing: bool,
        step: u64,
    ) -> TribResult<Acknowledgement> {
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
        let acknowledgement = client
            .send_clock(Clock {
                timestamp: *timestamp,
                idx: idx.try_into().unwrap(),
                initializing,
                step,
            })
            .await?;
        drop(timestamp);
        Ok(acknowledgement.into_inner())
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

    pub async fn initialization(&self) -> TribResult<()> {
        let addrs = self.addrs.read().await;
        let mut normal_join = false;

        // detect other keepers for 5 seconds
        let start_time = Instant::now();
        let mut current_time = Instant::now();
        while current_time.duration_since(start_time) < Duration::new(5, 0) {
            for idx in 0..addrs.len() {
                let acknowledgement = self.send_clock(idx, true, 1).await; // step 1
                match acknowledgement {
                    Err(_) => (),
                    Ok(acknowledgement) => {
                        // just a normal join operation
                        if !acknowledgement.initializing {
                            normal_join = true;
                            break;
                        }
                        // maintain the keeper statuses
                        let mut statuses = self.statuses.write().await;
                        statuses[idx] = true;
                        drop(statuses);
                    }
                }
            }
            current_time = Instant::now();
        }

        if normal_join {
            // first scan the backends

            // sleep for a specific amount of time
            thread::sleep(Duration::from_secs(4));

            for idx in 0..addrs.len() {
                let acknowledgement = self.send_clock(idx, true, 2).await; // step 2
                match acknowledgement {
                    Err(_) => (),
                    Ok(acknowledgement) => {
                        if acknowledgement.event.len() != 0 {
                            let mut acknowledged_event = self.acknowledged_event.write().await;
                            *acknowledged_event = acknowledgement.event;
                            drop(acknowledged_event);

                            let mut acknowledged_time = self.acknowledged_time.write().await;
                            *acknowledged_time = Instant::now();
                            drop(acknowledged_time);

                            // There will be at most 1 event because only the successor keeper sends its event.
                            break;
                        }
                    }
                }
            }
            // send the ready signal
            match &self.ready {
                Some(ready) => {
                    ready.send(true)?;
                }
                None => (),
            }
        } else {
            // scan and maintain the backend list based on statuses

            // send the ready signal
            match &self.ready {
                Some(ready) => {
                    ready.send(true)?;
                }
                None => (),
            }
        }
        Ok(())
    }
}
