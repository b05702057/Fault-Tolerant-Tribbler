use crate::keeper::trib_storage_client::TribStorageClient;
use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
};

use async_trait::async_trait;
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

const MAX_BACKEND_NUM: u64 = 300;
pub struct KeeperServer {
    pub backs: Arc<RwLock<Vec<String>>>,       // backend addresses
    pub addrs: Arc<RwLock<Vec<String>>>,       // keeper addresses
    pub statuses: Arc<RwLock<Vec<bool>>>,      // keeper statuses
    pub end_positions: Arc<RwLock<Vec<u64>>>,  // keeper end positions on the ring
    pub manage_range: Arc<RwLock<(u64, u64)>>, // keeper range
    pub pre_manage_range: Arc<RwLock<(u64, u64)>>, // predecessor range
    pub this: Arc<RwLock<usize>>,              // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>,           // timestamp of this keeper
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
        let backs = kc.backs;
        let addrs = kc.addrs;
        let this = kc.this;
        let mut statuses = Vec::<bool>::new();
        let mut end_positions = Vec::<u64>::new();
        let manage_num = MAX_BACKEND_NUM / (addrs.len() as u64); // use 300 directly to avoid edge cases of using backs.len()
        let mut client_opts = Vec::<Option<TribStorageClient<tonic::transport::Channel>>>::new();
        for idx in 0..addrs.len() {
            statuses.push(false);
            let end_position = (idx as u64 + 1) * manage_num - 1;
            end_positions.push(end_position);
            client_opts.push(None);
        }
        statuses[this] = true;

        KeeperServer {
            backs: Arc::new(RwLock::new(backs)),
            addrs: Arc::new(RwLock::new(addrs)),
            statuses: Arc::new(RwLock::new(statuses)),
            end_positions: Arc::new(RwLock::new(end_positions)),
            pre_manage_range: Arc::new(RwLock::new((0, 0))),
            manage_range: Arc::new(RwLock::new((0, 0))),
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
        let addrs = self.addrs.read().await;
        let this = self.this.read().await;
        let mut normal_join = false; // if this is a normal join operation
        let start_time = Instant::now();
        let mut current_time = Instant::now();

        // detect other keepers for 5 seconds
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

        // get end positions of alive keepers
        let mut alive_vector = Vec::<u64>::new();
        let statuses = self.statuses.read().await;
        let end_positions = self.end_positions.read().await;
        for idx in 0..addrs.len() {
            if statuses[idx] {
                alive_vector.push(end_positions[idx]);
            }
        }
        // get the range
        let mut pre_manage_range = self.pre_manage_range.write().await;
        let mut manage_range = self.manage_range.write().await;
        let alive_num = alive_vector.len();
        if alive_num == 1 {
            *pre_manage_range = (
                (end_positions[*this] + 1) % MAX_BACKEND_NUM,
                end_positions[*this],
            );
            *manage_range = (
                (end_positions[*this] + 1) % MAX_BACKEND_NUM,
                end_positions[*this],
            );
        } else {
            for idx in 0..alive_num {
                if alive_vector[idx] == end_positions[*this] {
                    let start_idx = ((idx - 1) + alive_num) % alive_num;
                    let pre_start_idx = ((idx - 2) + alive_num) % alive_num;
                    *pre_manage_range = (
                        (alive_vector[pre_start_idx] + 1) % MAX_BACKEND_NUM,
                        alive_vector[start_idx],
                    );
                    *manage_range = (
                        (alive_vector[start_idx] + 1) % MAX_BACKEND_NUM,
                        alive_vector[idx],
                    );
                }
            }
        }
        drop(this);
        drop(addrs);
        drop(pre_manage_range);
        drop(manage_range);

        if normal_join {
            // scan the backends and sleep for a specific amount of time
            // todo!();
            thread::sleep(Duration::from_secs(4)); // sleep after the first scan

            // find the successor
            let this = self.this.read().await;
            let addrs = self.addrs.read().await;
            let statuses = self.statuses.read().await;
            let mut successor_index = *this;
            for idx in *this + 1..addrs.len() {
                if statuses[idx] {
                    successor_index = idx;
                    break;
                }
            }
            drop(addrs);
            // hasn't find the successor yet
            if successor_index == *this {
                for idx in 0..*this {
                    if statuses[idx] {
                        successor_index = idx;
                        break;
                    }
                }
            }
            drop(statuses);

            // found a successor => get the acknowledged event
            if successor_index != *this {
                let acknowledgement = self.client_send_clock(successor_index, true, 2).await; // step 2
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
                        }
                    }
                    // down
                    Err(_) => (),
                }
            }
        } else {
            // starting phase
            // scan to maintain the backends
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

    // send clock to other keepers to maintain the keeper view
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

    // send keys of finished lists
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
        // 2) just checking if this keeper is initialziing (step 1 of initialization)
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

// When a keeper joins, it needs to know if it's at the starting phase.
// 1) If it finds a working keeper => normal join
// 2) else => starting phase
