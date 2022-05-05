use crate::keeper::trib_storage_client::TribStorageClient;
use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
};

use async_trait::async_trait;
// use priority_queue::PriorityQueue;
use std::cmp;
use std::collections::HashSet;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tonic::Response;
use tribbler::config::KeeperConfig;
use tribbler::err::TribResult;

pub struct KeeperServer {
    pub addrs: Arc<RwLock<Vec<String>>>,          // keeper addresses
    pub statuses: Arc<RwLock<Vec<bool>>>,         // keeper statuses
    pub this: Arc<RwLock<usize>>,                 // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>,              // timestamp of this keeper
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists to help migration
    pub detecting_event: Arc<RwLock<String>>,   // the most recent backend event
    pub detecting_time: Arc<RwLock<Instant>>,   // the time of the backend event
    pub acknowledged_event: Arc<RwLock<String>>, // the most recent acknowledged event
    pub acknowledged_time: Arc<RwLock<Instant>>, // the time of the acknowledged event
    pub acknowledging_time: Arc<RwLock<Instant>>, // the most recent acknowledging event
    pub mutex: Mutex<u64>,                      // event/time mutex
    pub initializing: Arc<RwLock<bool>>,        // if this keeper is initializing
    pub client_opts: Arc<Mutex<Vec<Option<TribStorageClient<tonic::transport::Channel>>>>>, // keeper connections
}

impl KeeperServer {
    pub fn new(kc: KeeperConfig) -> KeeperServer {
        let addrs = kc.addrs;
        let this = kc.this;
        let mut statuses = Vec::<bool>::new();
        let mut client_opts = Vec::<Option<TribStorageClient<tonic::transport::Channel>>>::new();
        for _ in &addrs {
            statuses.push(false);
            client_opts.push(None);
        }

        KeeperServer {
            addrs: Arc::new(RwLock::new(addrs)),
            statuses: Arc::new(RwLock::new(statuses)),
            this: Arc::new(RwLock::new(this)),
            timestamp: Arc::new(RwLock::new(0)),
            key_list: Arc::new(RwLock::new(HashSet::<String>::new())),
            detecting_event: Arc::new(RwLock::new("".to_string())),
            detecting_time: Arc::new(RwLock::new(Instant::now())),
            acknowledged_event: Arc::new(RwLock::new("".to_string())),
            acknowledged_time: Arc::new(RwLock::new(Instant::now())),
            acknowledging_time: Arc::new(RwLock::new(Instant::now())),
            mutex: Mutex::new(0),
            initializing: Arc::new(RwLock::new(true)),
            client_opts: Arc::new(Mutex::new(client_opts)),
        }
    }

    pub async fn initialization(&self) -> TribResult<bool> {
        // detect other keepers for 5 seconds
        let addrs = self.addrs.read().await;
        let this = self.this.read().await;
        let mut normal_join = false; // if this is a normal join operation
        let start_time = Instant::now();
        let mut current_time = Instant::now();

        while current_time.duration_since(start_time) < Duration::new(5, 0) {
            for idx in 0..addrs.len() {
                // only contact other keepers
                if idx == *this {
                    continue;
                }
                // check acknowledgement
                let acknowledgement = self.client_send_clock(idx, true, 1).await; // step 1
                let mut statuses = self.statuses.write().await;
                match acknowledgement {
                    // alive
                    Ok(acknowledgement) => {
                        // just a normal join operation
                        if !acknowledgement.initializing {
                            normal_join = true; // don't break here because we want to establish a complete keeper view
                        }
                        statuses[idx] = true; // maintain the keeper statuses
                    }
                    // down
                    Err(_) => {
                        statuses[idx] = false; // maintain the keeper statuses
                    }
                }
                drop(statuses)
            }
            // break here because we have got enough information
            if normal_join {
                break;
            }
            current_time = Instant::now(); // reset current time
        }
        drop(this);
        drop(addrs);

        // prepare the scan range here!!!
        // first find the predecessor
        // then find the backend range for the predecessor and current keeper

        if normal_join {
            // scan the backends and sleep for a specific amount of time
            // todo!();
            thread::sleep(Duration::from_secs(4));

            // find the successor
            // let this = self.this.read().await;
            // let addrs = self.addrs.read().await;
            // let statuses = self.statuses.read().await;
            // let mut alive_servers = PriorityQueue::new();
            // for idx in 0..addrs.len() {
            //     // ignore itself and dead servers
            //     if idx == *this || !statuses[idx] {
            //         continue;
            //     }

            //     if idx > *this {
            //         alive_servers.push(idx, *this - idx);
            //     } else {
            //         alive_servers.push(idx, *this - idx - addrs.len());
            //     }
            // }

            for idx in 0..addrs.len() {
                let acknowledgement = self.client_send_clock(idx, true, 2).await; // step 2
                match acknowledgement {
                    // alive
                    Ok(acknowledgement) => {
                        if acknowledgement.event.len() != 0 {
                            let mut acknowledged_event = self.acknowledged_event.write().await;
                            *acknowledged_event = acknowledgement.event;
                            drop(acknowledged_event);

                            let mut acknowledged_time = self.acknowledged_time.write().await;
                            *acknowledged_time = Instant::now();
                            drop(acknowledged_time);
                            break; // only the successor
                        }
                    }
                    // down
                    Err(_) => (),
                }
            }
        } else {
            // scan and maintain the backend list based on statuses
            todo!();
        }
        Ok(true) // can send the ready signal
    }

    // Client Functions
    pub async fn client_connect(&self, idx: usize) -> TribResult<()> {
        // connect if not already connected
        let mut client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        // To prevent unnecessary reconnection, we only connect if we haven't.
        if let None = client_opts[idx] {
            let addr = &self.addrs.read().await[idx];
            client_opts[idx] = Some(TribStorageClient::connect(addr.clone()).await?);
            drop(addr);
        }
        Ok(())
    }

    pub async fn client_send_clock(
        &self,
        idx: usize,
        initializing: bool,
        step: u64,
    ) -> TribResult<Acknowledgement> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        self.client_connect(idx).await?;
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

    pub async fn client_send_key(&self, idx: usize, key: String) -> TribResult<bool> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let client_opts = Arc::clone(&self.client_opts).lock_owned().await;
        self.client_connect(idx).await?;
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

// The below approach should handle most cases:
// If an event happen after the ACK, the new keeper would handle it.
// If an event happen before the ACK, the old keeper would handle it.
// When a heatbeat is recieved, the old keeper sends ACK after the next scan and remove the backends.

#[async_trait]
impl keeper::trib_storage_server::TribStorage for KeeperServer {
    async fn send_clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let received_request = request.into_inner();

        // store the timestamp from another keeper and use it to sync later
        let cur_timestamp = self.timestamp.read().await;
        if received_request.timestamp > *cur_timestamp {
            drop(cur_timestamp);
            let mut cur_timestamp = self.timestamp.write().await;
            *cur_timestamp = cmp::max(*cur_timestamp, received_request.timestamp);
        }

        // 1) just a normal clock heartbeat
        // 2) just checking if this keeper is initialziing
        if !received_request.initializing || received_request.step == 1 {
            let initializing = self.initializing.read().await;
            let return_initializing = initializing.clone();
            drop(initializing);
            return Ok(Response::new(Acknowledgement {
                event: "".to_string(),
                initializing: return_initializing,
            }));
        }

        // step 2: join after knowing other keepers are not initializing
        let guard = self.mutex.lock();
        let current_time = Instant::now();
        let detecting_time = self.detecting_time.read().await;
        let mut return_event = "".to_string();
        if current_time.duration_since(*detecting_time) < Duration::new(10, 0) {
            let mut acknowledging_time = self.acknowledging_time.write().await;
            *acknowledging_time = Instant::now();
            let detecting_event = self.detecting_event.read().await;
            return_event = detecting_event.clone();
            drop(detecting_time);
            drop(acknowledging_time);
            drop(detecting_event);
        }
        let initializing = self.initializing.read().await;
        let return_initializing = initializing.clone();
        drop(initializing);
        drop(guard);
        return Ok(Response::new(Acknowledgement {
            event: return_event,
            initializing: return_initializing,
        }));
    }

    async fn send_key(
        &self,
        request: tonic::Request<Key>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        // record the log entry to avoid repetitive migration
        let received_key = request.into_inner();
        let mut key_list = self.key_list.write().await;
        key_list.insert(received_key.key);

        // The log entry is received and pushed.
        return Ok(Response::new(Bool { value: true }));
    }
}

// When a keeper joins, it waits for 5 secs.
// During the 5 secs, if it receives no clock, that means it is the only initializing keeper.
// If it receives an existing clock, it stops waiting and join.
// If it receives initializing clocks, they initialize together.

// When a keeper runs, it should always make sure its backend list aligns with the alive keeper list.
