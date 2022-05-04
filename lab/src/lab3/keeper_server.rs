use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    keeper,
    keeper::{Address, Bool, Clock, Key},
};
use async_trait::async_trait;
use tonic::Response;

pub struct KeeperServer {
    pub timestamp: Arc<RwLock<u64>>,                         // to sync
    pub keeper_statuses: Arc<RwLock<HashMap<String, bool>>>, // get the keeper status with an address
    pub key_list: Arc<RwLock<HashSet<String>>>, // store the keys of finsihed lists as a helper
    pub event_backend: Arc<RwLock<String>>,     // hold the lock when the keeper runs scan()
    pub scan_count: Arc<RwLock<u64>>,           // count #scan since the clock is sent
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
    ) -> Result<tonic::Response<Address>, tonic::Status> {
        let received_request = request.into_inner();

        // store the timestamp from another keeper and use it to sync later
        let cur_timestamp = self.timestamp.read().await;
        if received_request.timestamp > *cur_timestamp {
            drop(cur_timestamp);
            let mut cur_timestamp = self.timestamp.write().await;
            *cur_timestamp = received_request.timestamp;
        }

        // just a normal clock heartbeat
        if !received_request.is_first_clock {
            return Ok(Response::new(Address {
                address: "".to_string(),
            }));
        }

        // update the keeper status
        let mut keeper_map = self.keeper_statuses.write().await;
        keeper_map.insert(received_request.address, true);
        drop(keeper_map);

        // two scans
        let mut scan_count = self.scan_count.write().await;
        *scan_count = 0;
        drop(scan_count);
        let mut scan_count = self.scan_count.read().await;
        while *scan_count < 2 {
            drop(scan_count);
            scan_count = self.scan_count.read().await;
        }

        // return the backend address
        let event_backend = self.event_backend.read().await;
        let return_address = event_backend.clone();
        drop(event_backend);
        return Ok(Response::new(Address {
            address: return_address,
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
