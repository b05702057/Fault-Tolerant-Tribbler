use std::cmp;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tribbler::config::KeeperConfig;

use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
};
use async_trait::async_trait;
use tonic::Response;

pub struct KeeperServer {
    pub addrs: Arc<RwLock<Vec<String>>>,
    pub statuses: Arc<RwLock<Vec<bool>>>, // the statuses of all keepers
    pub this: Arc<RwLock<usize>>,         // the index of this keeper
    pub timestamp: Arc<RwLock<u64>>,      // to sync
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists as a helper
    pub detecting_event: Arc<RwLock<String>>, // hold the lock when the keeper runs scan()
    pub detecting_time: Arc<RwLock<Instant>>,
    pub acknowledging_time: Arc<RwLock<Instant>>,
    pub scan_count: Arc<RwLock<u64>>, // count #scan since the clock is sent
    pub initializing: Arc<RwLock<bool>>,
    pub mutex: Mutex<u64>,
}

impl KeeperServer {
    pub fn new(kc: KeeperConfig) -> KeeperServer {
        let addrs = kc.addrs;
        let this = kc.this;

        let mut statuses = Vec::<bool>::new();
        for _ in &addrs {
            statuses.push(false);
        }

        KeeperServer {
            addrs: Arc::new(RwLock::new(addrs)),
            statuses: Arc::new(RwLock::new(statuses)),
            this: Arc::new(RwLock::new(this)),
            timestamp: Arc::new(RwLock::new(0)),
            key_list: Arc::new(RwLock::new(HashSet::<String>::new())),
            detecting_event: Arc::new(RwLock::new("".to_string())),
            detecting_time: Arc::new(RwLock::new(Instant::now())),
            acknowledging_time: Arc::new(RwLock::new(Instant::now())),
            scan_count: Arc::new(RwLock::new(0)),
            initializing: Arc::new(RwLock::new(true)),
            mutex: Mutex::new(0),
        }
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
