// use crate::keeper::keeper_rpc_client::KeeperRpcClient;
// use crate::{
//     keeper,
//     keeper::{Acknowledgement, Bool, Clock, Key},
//     lab3::keeper_client::KeeperClient,
// };

// use async_trait::async_trait;
// use std::cmp;
// use std::collections::HashSet;
// use std::sync::Arc;
// use std::time::Duration;
// use std::time::Instant;
// use tokio::sync::Mutex;
// use tokio::sync::RwLock;
// use tonic::Response;
// use tribbler::config::KeeperConfig;
// use tribbler::err::TribResult;

// const MAX_BACKEND_NUM: u64 = 300;

// #[derive(PartialEq, Clone)]
// pub enum BackendEventType {
//     Join,
//     Leave,
//     None,
// }

// #[derive(Clone)]
// pub struct BackendEvent {
//     pub event_type: BackendEventType,
//     pub back_idx: usize,
//     pub timestamp: Instant,
// }

// pub struct LogEntry {
//     pub key: String,
//     pub timestamp: Instant, // use the timestamp to know if it belongs to the current event
// }

// pub struct KeeperServer {
//     pub backs: Arc<RwLock<Vec<String>>>,         // backend addresses
//     pub keeper_addrs: Arc<RwLock<Vec<String>>>,  // keeper addresses
//     pub statuses: Arc<RwLock<Vec<bool>>>,        // keeper statuses
//     pub end_positions: Arc<RwLock<Vec<u64>>>,    // keeper end positions on the ring
//     pub this: Arc<RwLock<usize>>,                // the index of this keeper
//     pub keeper_clock: Arc<RwLock<u64>>,          // keeper_clock of this keeper
//     pub log_entries: Arc<RwLock<Vec<LogEntry>>>, // store the keys of finsihed lists to help migration
//     pub event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
//     pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
//     pub latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//     pub predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//     pub ack_to_predecessor_time: Arc<RwLock<Instant>>, // the most recent acknowledging event
//     pub event_handling_mutex: Arc<Mutex<u64>>,         // event/time mutex
//     pub initializing: Arc<RwLock<bool>>,               // if this keeper is initializing
//     pub keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>, // keeper connections
//     pub keeper_client: Arc<RwLock<Option<KeeperClient>>>,
// }

// impl KeeperServer {
//     pub fn new(kc: KeeperConfig) -> KeeperServer {
//         let backs = kc.backs;
//         let keeper_addrs = kc.addrs;
//         let this = kc.this;
//         let mut statuses = Vec::<bool>::new();
//         let mut end_positions = Vec::<u64>::new();
//         let manage_num = MAX_BACKEND_NUM / (keeper_addrs.len() as u64); // use 300 directly to avoid edge cases of using backs.len()
//         let mut keeper_client_opts =
//             Vec::<Option<KeeperRpcClient<tonic::transport::Channel>>>::new();
//         for idx in 0..keeper_addrs.len() {
//             statuses.push(false);
//             let end_position = (idx as u64 + 1) * manage_num - 1;
//             end_positions.push(end_position);
//             keeper_client_opts.push(None);
//         }
//         statuses[this] = true;

//         let detected_event_type = BackendEventType::None;
//         let detected_backend_event = BackendEvent {
//             event_type: detected_event_type,
//             back_idx: 0,
//             timestamp: Instant::now(),
//         };
//         let acked_event_type = BackendEventType::None;
//         let acked_backend_event = BackendEvent {
//             event_type: acked_event_type,
//             back_idx: 0,
//             timestamp: Instant::now(),
//         };

//         let mut keeper_server = KeeperServer {
//             backs: Arc::new(RwLock::new(backs)),
//             keeper_addrs: Arc::new(RwLock::new(keeper_addrs)),
//             statuses: Arc::new(RwLock::new(statuses)),
//             end_positions: Arc::new(RwLock::new(end_positions)),
//             this: Arc::new(RwLock::new(this)),
//             keeper_clock: Arc::new(RwLock::new(0)),
//             log_entries: Arc::new(RwLock::new(Vec::<LogEntry>::new())),
//             event_detected_by_this: Arc::new(RwLock::new(Some(detected_backend_event))),
//             event_acked_by_successor: Arc::new(RwLock::new(Some(acked_backend_event))),
//             latest_monitoring_range_inclusive: Arc::new(RwLock::new(None)),
//             predecessor_monitoring_range_inclusive: Arc::new(RwLock::new(None)),
//             ack_to_predecessor_time: Arc::new(RwLock::new(Instant::now())),
//             event_handling_mutex: Arc::new(Mutex::new(0)),
//             initializing: Arc::new(RwLock::new(true)),
//             keeper_client_opts: Arc::new(Mutex::new(keeper_client_opts)),
//             keeper_client: Arc::new(RwLock::new(None)),
//         };

//         keeper_server.keeper_client = Arc::new(RwLock::new(Some(KeeperClient::new(
//             Arc::clone(&keeper_server.backs),
//             Arc::clone(&keeper_server.keeper_addrs),
//             Arc::clone(&keeper_server.statuses),
//             Arc::clone(&keeper_server.end_positions),
//             Arc::clone(&keeper_server.this),
//             Arc::clone(&keeper_server.keeper_clock),
//             Arc::clone(&keeper_server.log_entries),
//             Arc::clone(&keeper_server.event_detected_by_this),
//             Arc::clone(&keeper_server.event_acked_by_successor),
//             Arc::clone(&keeper_server.latest_monitoring_range_inclusive),
//             Arc::clone(&keeper_server.predecessor_monitoring_range_inclusive),
//             Arc::clone(&keeper_server.ack_to_predecessor_time),
//             Arc::clone(&keeper_server.event_handling_mutex),
//             Arc::clone(&keeper_server.initializing),
//             Arc::clone(&keeper_server.keeper_client_opts),
//         ))));
//         return keeper_server;
//     }
// }

// #[async_trait]
// impl keeper::keeper_rpc_server::KeeperRpc for KeeperServer {
//     async fn send_clock(
//         &self,
//         request: tonic::Request<Clock>,
//     ) -> Result<tonic::Response<Acknowledgement>, tonic::Status> {
//         let received_request = request.into_inner();

//         // store the timestamp from another keeper and use it to sync later
//         let keeper_clock_lock = self.keeper_clock.read().await;
//         let keeper_clock = keeper_clock_lock.clone();
//         drop(keeper_clock_lock);

//         if received_request.timestamp > keeper_clock {
//             let mut keeper_clock = self.keeper_clock.write().await;
//             *keeper_clock = cmp::max(*keeper_clock, received_request.timestamp);
//             drop(keeper_clock);
//         }

//         // 1) just a normal clock heartbeat
//         // 2) just checking if this keeper is initialziing (step 1 of initialization)
//         if !received_request.initializing || received_request.step == 1 {
//             let initializing_lock = self.initializing.read().await;
//             let initializing = initializing_lock.clone();
//             drop(initializing_lock);
//             return Ok(Response::new(Acknowledgement {
//                 event_type: "None".to_string(),
//                 back_idx: 0,
//                 initializing,
//             }));
//         }

//         // step 2: join after knowing other keepers are not initializing
//         let guard = self.event_handling_mutex.lock();
//         let current_time = Instant::now();

//         let event_detected_by_this_lock = self.event_detected_by_this.read().await;
//         let event_detected_by_this = event_detected_by_this_lock.as_ref().unwrap().clone();
//         drop(event_detected_by_this_lock);

//         let mut return_event = "None".to_string();
//         let mut back_idx = 0;
//         let detect_time = event_detected_by_this.timestamp;
//         if current_time.duration_since(detect_time) < Duration::new(10, 0) {
//             let mut ack_to_predecessor_time = self.ack_to_predecessor_time.write().await;
//             *ack_to_predecessor_time = Instant::now();
//             drop(ack_to_predecessor_time);
//             match event_detected_by_this.event_type {
//                 BackendEventType::None => {
//                     return_event = "None".to_string();
//                 }
//                 BackendEventType::Join => {
//                     return_event = "Join".to_string();
//                     back_idx = event_detected_by_this.back_idx;
//                 }
//                 BackendEventType::Leave => {
//                     return_event = "Leave".to_string();
//                     back_idx = event_detected_by_this.back_idx;
//                 }
//             }
//         }
//         let initializing = self.initializing.read().await;
//         let return_initializing = initializing.clone();
//         drop(initializing);

//         // change the state of the predecessor
//         let mut statuses = self.statuses.write().await;
//         statuses[received_request.idx as usize] = true;
//         drop(statuses);

//         // update the range
//         let keeper_client_lock = self.keeper_client.write().await;
//         let keeper_client = keeper_client_lock.as_ref().unwrap();
//         let _update_result = keeper_client.update_ranges().await;
//         drop(keeper_client);

//         drop(guard);
//         return Ok(Response::new(Acknowledgement {
//             event_type: return_event,
//             back_idx: back_idx as u64,
//             initializing: return_initializing,
//         }));
//     }

//     async fn send_key(
//         &self,
//         request: tonic::Request<Key>,
//     ) -> Result<tonic::Response<Bool>, tonic::Status> {
//         // record the log entry to avoid repetitive migration
//         let received_key = request.into_inner().key;
//         let mut log_entries = self.log_entries.write().await;
//         log_entries.push(LogEntry {
//             key: received_key,
//             timestamp: Instant::now(),
//         });
//         // The log entry is received and pushed.
//         return Ok(Response::new(Bool { value: true }));
//     }
// }

// // When a keeper joins, it needs to know if it's at the starting phase.
// // 1) If it finds a working keeper => normal join
// // 2) else => starting phase
