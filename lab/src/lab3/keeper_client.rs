use crate::keeper::keeper_rpc_client::KeeperRpcClient;
use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
    lab3::keeper_server::BackendEvent,
    lab3::keeper_server::BackendEventType,
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
pub struct KeeperClient {
    pub backs: Arc<RwLock<Vec<String>>>,       // backend addresses
    pub addrs: Arc<RwLock<Vec<String>>>,       // keeper addresses
    pub statuses: Arc<RwLock<Vec<bool>>>,      // keeper statuses
    pub end_positions: Arc<RwLock<Vec<u64>>>,  // keeper end positions on the ring
    pub manage_range: Arc<RwLock<(u64, u64)>>, // keeper range
    pub pre_manage_range: Arc<RwLock<(u64, u64)>>, // predecessor range
    pub this: Arc<RwLock<usize>>,              // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>,           // timestamp of this keeper
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists to help migration
    pub event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
    pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
    pub acknowledging_time: Arc<RwLock<Instant>>, // the most recent acknowledging event
    pub event_handling_mutex: Arc<Mutex<u64>>,    // event/time mutex
    pub initializing: Arc<RwLock<bool>>,          // if this keeper is initializing
    pub keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>, // keeper connections
}

impl KeeperClient {
    pub fn new(
        backs: Arc<RwLock<Vec<String>>>,
        addrs: Arc<RwLock<Vec<String>>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        end_positions: Arc<RwLock<Vec<u64>>>,
        manage_range: Arc<RwLock<(u64, u64)>>,
        pre_manage_range: Arc<RwLock<(u64, u64)>>,
        this: Arc<RwLock<usize>>,
        timestamp: Arc<RwLock<u64>>,
        key_list: Arc<RwLock<HashSet<String>>>,
        event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
        event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
        acknowledging_time: Arc<RwLock<Instant>>,
        event_handling_mutex: Arc<Mutex<u64>>,
        initializing: Arc<RwLock<bool>>,
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
    ) -> KeeperClient {
        KeeperClient {
            backs,
            addrs,
            statuses,
            end_positions,
            manage_range,
            pre_manage_range,
            this,
            timestamp,
            key_list,
            event_detected_by_this,
            event_acked_by_successor,
            acknowledging_time,
            event_handling_mutex,
            initializing,
            keeper_client_opts,
        }
    }

    // connect client if not already connected
    pub async fn connect(&self, idx: usize) -> TribResult<()> {
        // connect if not already connected
        let mut keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
        // To prevent unnecessary reconnection, we only connect if we haven't.
        if let None = keeper_client_opts[idx] {
            let addr = &self.addrs.read().await[idx];
            keeper_client_opts[idx] = Some(KeeperRpcClient::connect(addr.clone()).await?);
            drop(addr);
        }
        Ok(())
    }

    // send clock to other keepers to maintain the keeper view
    pub async fn send_clock(
        &self,
        idx: usize,
        initializing: bool,
        step: u64,
    ) -> TribResult<Acknowledgement> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
        self.connect(idx).await?;
        let client_opt = &keeper_client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(keeper_client_opts);

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
    pub async fn send_key(&self, idx: usize, key: String) -> TribResult<bool> {
        // client_opt is a tokio::sync::OwnedMutexGuard
        let keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
        self.connect(idx).await?;
        let client_opt = &keeper_client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(keeper_client_opts);
        client.send_key(Key { key }).await?;
        Ok(true)
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
                let acknowledgement = self.send_clock(idx, true, 1).await; // step 1
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
                let acknowledgement = self.send_clock(successor_index, true, 2).await; // step 2
                match acknowledgement {
                    // alive
                    Ok(acknowledgement) => {
                        if acknowledgement.event_type != "None" {
                            let mut event_type = BackendEventType::Join;
                            if acknowledgement.event_type == "Leave" {
                                event_type = BackendEventType::Leave;
                            }

                            let backend_event = BackendEvent {
                                event_type,
                                back_idx: acknowledgement.back_idx as usize,
                                timestamp: Instant::now(),
                            };

                            let mut event_acked_by_successor =
                                self.event_acked_by_successor.write().await;

                            *event_acked_by_successor = Some(backend_event);
                            drop(event_acked_by_successor);
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
        let mut initializing = self.initializing.write().await;
        *initializing = false;
        drop(initializing);
        Ok(true) // can send the ready signal
    }
}