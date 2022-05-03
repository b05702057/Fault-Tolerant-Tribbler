use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    keeper,
    keeper::{BackendStatusList, Bool, Clock, LogEntry, None},
};
use async_trait::async_trait;
use tonic::Response;

pub struct KeeperServer {
    pub timestamp: Arc<RwLock<u64>>,                 // to sync
    pub keeper_statuses: Vec<bool>,                  // keepers alive or crashed
    pub log_entries: Arc<RwLock<Vec<LogEntry>>>,     // as a helper
    pub backend_statuses: Arc<RwLock<Vec<bool>>>,    // backends alive or crashed
    pub backend_addresses: Arc<RwLock<Vec<String>>>, // backend addresses
}

#[async_trait]
impl keeper::trib_storage_server::TribStorage for KeeperServer {
    async fn send_clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        // store the timestamp from another keeper and use it to sync later
        let received_timestamp = request.into_inner().timestamp;
        let cur_timestamp = self.timestamp.read().await;
        if received_timestamp > *cur_timestamp {
            drop(cur_timestamp);
            let mut cur_timestamp = self.timestamp.write().await;
            *cur_timestamp = received_timestamp;
        }

        // The clock is received and will be used to sync.
        return Ok(Response::new(Bool { value: true }));
    }

    async fn send_log_entry(
        &self,
        request: tonic::Request<LogEntry>,
    ) -> Result<tonic::Response<Bool>, tonic::Status> {
        // record the log entry to avoid repetitive migration
        let received_log_entry = request.into_inner();
        let mut cur_entries = self.log_entries.write().await;
        cur_entries.push(received_log_entry);

        // The log entry is received and pushed.
        return Ok(Response::new(Bool { value: true }));
    }

    async fn get_backend_status_list(
        &self,
        _request: tonic::Request<None>,
    ) -> Result<tonic::Response<BackendStatusList>, tonic::Status> {
        // simply return the backend
        let backend_addresses = self.backend_addresses.read().await;
        let backend_statuses = self.backend_statuses.read().await;
        return Ok(Response::new(BackendStatusList {
            address_list: backend_addresses.to_vec(),
            status_list: backend_statuses.to_vec(),
        }));
    }
}
