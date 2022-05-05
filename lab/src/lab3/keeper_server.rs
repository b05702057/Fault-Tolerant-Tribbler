use crate::keeper::trib_storage_client::TribStorageClient;
use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
    lab3::keeper_client::KeeperClient,
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

#[derive(PartialEq, Clone)]
pub enum BackendEventType {
    Join,
    Leave,
    None,
}

#[derive(Clone)]
pub struct BackendEvent {
    pub event_type: BackendEventType,
    pub back_idx: usize,
    pub timestamp: Instant,
}

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
    pub event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
    pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
    pub acknowledging_time: Arc<RwLock<Instant>>, // the most recent acknowledging event
    pub event_handling_mutex: Arc<Mutex<u64>>,    // event/time mutex
    pub initializing: Arc<RwLock<bool>>,          // if this keeper is initializing
    pub client_opts: Arc<Mutex<Vec<Option<TribStorageClient<tonic::transport::Channel>>>>>, // keeper connections
    pub keeper_client: Arc<RwLock<Option<KeeperClient>>>,
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

        let detected_event_type = BackendEventType::None;
        let detected_backend_event = BackendEvent {
            event_type: detected_event_type,
            back_idx: 0,
            timestamp: Instant::now(),
        };
        let acked_event_type = BackendEventType::None;
        let acked_backend_event = BackendEvent {
            event_type: acked_event_type,
            back_idx: 0,
            timestamp: Instant::now(),
        };

        let mut keeper_server = KeeperServer {
            backs: Arc::new(RwLock::new(backs)),
            addrs: Arc::new(RwLock::new(addrs)),
            statuses: Arc::new(RwLock::new(statuses)),
            end_positions: Arc::new(RwLock::new(end_positions)),
            pre_manage_range: Arc::new(RwLock::new((0, 0))),
            manage_range: Arc::new(RwLock::new((0, 0))),
            this: Arc::new(RwLock::new(this)),
            timestamp: Arc::new(RwLock::new(0)),
            key_list: Arc::new(RwLock::new(HashSet::<String>::new())),
            event_detected_by_this: Arc::new(RwLock::new(Some(detected_backend_event))),
            event_acked_by_successor: Arc::new(RwLock::new(Some(acked_backend_event))),
            acknowledging_time: Arc::new(RwLock::new(Instant::now())),
            event_handling_mutex: Arc::new(Mutex::new(0)),
            initializing: Arc::new(RwLock::new(true)),
            client_opts: Arc::new(Mutex::new(client_opts)),
            keeper_client: Arc::new(RwLock::new(None)),
        };

        keeper_server.keeper_client = Arc::new(RwLock::new(Some(KeeperClient::new(
            Arc::clone(&keeper_server.backs),
            Arc::clone(&keeper_server.addrs),
            Arc::clone(&keeper_server.statuses),
            Arc::clone(&keeper_server.end_positions),
            Arc::clone(&keeper_server.pre_manage_range),
            Arc::clone(&keeper_server.manage_range),
            Arc::clone(&keeper_server.this),
            Arc::clone(&keeper_server.timestamp),
            Arc::clone(&keeper_server.key_list),
            Arc::clone(&keeper_server.event_detected_by_this),
            Arc::clone(&keeper_server.event_acked_by_successor),
            Arc::clone(&keeper_server.acknowledging_time),
            Arc::clone(&keeper_server.event_handling_mutex),
            Arc::clone(&keeper_server.initializing),
            Arc::clone(&keeper_server.client_opts),
        ))));
        return keeper_server;
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
                event_type: "None".to_string(),
                back_idx: 0,
                initializing: return_initializing,
            }));
        }

        // step 2: join after knowing other keepers are not initializing
        let guard = self.event_handling_mutex.lock();
        let current_time = Instant::now();

        let event_detected_by_this = self.event_detected_by_this.read().await;
        let event_detected = event_detected_by_this.as_ref().unwrap();
        let mut return_event = "None".to_string();
        let mut back_idx = 0;
        let detecting_time = event_detected.timestamp;
        if current_time.duration_since(detecting_time) < Duration::new(10, 0) {
            let mut acknowledging_time = self.acknowledging_time.write().await;
            *acknowledging_time = Instant::now();
            match event_detected.event_type {
                BackendEventType::None => {
                    return_event = "None".to_string();
                }
                BackendEventType::Join => {
                    return_event = "Join".to_string();
                    back_idx = event_detected.back_idx;
                }
                BackendEventType::Leave => {
                    return_event = "Leave".to_string();
                    back_idx = event_detected.back_idx;
                }
            }
        }
        let initializing = self.initializing.read().await;
        let return_initializing = initializing.clone();

        // should update the range here!!!!

        drop(event_detected_by_this);
        drop(initializing);
        drop(guard);
        return Ok(Response::new(Acknowledgement {
            event_type: return_event,
            back_idx: back_idx as u64,
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

// TODO
// sync the fields
// separate the two structs
