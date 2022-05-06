// use crate::keeper::keeper_rpc_client::KeeperRpcClient;
// use crate::{
//     keeper::{Acknowledgement, Clock, Key},
//     lab2::keeper_server::BackendEvent,
//     lab2::keeper_server::BackendEventType,
//     lab2::keeper_server::LogEntry,
// };
// use std::sync::Arc;
// use std::time::Duration;
// use std::time::Instant;
// use tokio::sync::Mutex;
// use tokio::sync::RwLock;
// use tokio::time::sleep;
// use tribbler::err::TribResult;

// const MAX_BACKEND_NUM: u64 = 300;
// pub struct KeeperClient {
//     pub http_back_addrs: Vec<String>,            // backend addresses
//     pub keeper_addrs: Vec<String>,               // keeper addresses
//     pub statuses: Arc<RwLock<Vec<bool>>>,        // keeper statuses
//     pub end_positions: Arc<RwLock<Vec<u64>>>,    // keeper end positions on the ring
//     pub this: usize,                             // the index of this keeper
//     pub keeper_clock: Arc<RwLock<u64>>,          // keeper_clock of this keeper
//     pub log_entries: Arc<RwLock<Vec<LogEntry>>>, // store the keys of finsihed lists to help migration
//     pub event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
//     pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
//     pub latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//     pub predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//     pub ack_to_predecessor_time: Arc<RwLock<Option<Instant>>>, // the most recent acknowledging event
//     pub event_handling_mutex: Arc<Mutex<u64>>,                 // event/time mutex
//     pub initializing: Arc<RwLock<bool>>,                       // if this keeper is initializing
//     pub keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>, // keeper connections
// }

// impl KeeperClient {
//     pub fn new(
//         http_back_addrs: Vec<String>,
//         keeper_addrs: Vec<String>,
//         statuses: Arc<RwLock<Vec<bool>>>,
//         end_positions: Arc<RwLock<Vec<u64>>>,
//         this: usize,
//         keeper_clock: Arc<RwLock<u64>>,
//         log_entries: Arc<RwLock<Vec<LogEntry>>>,
//         event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
//         event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
//         latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//         predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
//         ack_to_predecessor_time: Arc<RwLock<Option<Instant>>>,
//         event_handling_mutex: Arc<Mutex<u64>>,
//         initializing: Arc<RwLock<bool>>,
//         keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
//     ) -> KeeperClient {
//         KeeperClient {
//             http_back_addrs,
//             keeper_addrs,
//             statuses,
//             end_positions,
//             this,
//             keeper_clock,
//             log_entries,
//             event_detected_by_this,
//             event_acked_by_successor,
//             latest_monitoring_range_inclusive,
//             predecessor_monitoring_range_inclusive,
//             ack_to_predecessor_time,
//             event_handling_mutex,
//             initializing,
//             keeper_client_opts,
//         }
//     }

//     // connect client if not already connected
//     pub async fn connect(&self, idx: usize) -> TribResult<()> {
//         // connect if not already connected
//         let mut keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
//         // To prevent unnecessary reconnection, we only connect if we haven't.

//         if let None = keeper_client_opts[idx] {
//             let keeper_addr = &self.keeper_addrs[idx];
//             keeper_client_opts[idx] = Some(KeeperRpcClient::connect(keeper_addr.clone()).await?);
//         }
//         drop(keeper_client_opts);
//         Ok(())
//     }

//     // send clock to other keepers to maintain the keeper view
//     pub async fn send_clock(
//         &self,
//         idx: usize,
//         initializing: bool,
//         step: u64,
//     ) -> TribResult<Acknowledgement> {
//         self.connect(idx).await?;
//         let keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
//         let client_opt = &keeper_client_opts[idx];

//         // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
//         let mut client = match client_opt {
//             // RPC client is clonable and works fine with concurrent clients.
//             Some(client) => client.clone(),
//             None => return Err("Client was somehow not connected / be initialized!".into()),
//         };
//         drop(keeper_client_opts); // client_opt needs the lock

//         let keeper_clock_lock = self.keeper_clock.read().await;
//         let keeper_clock = keeper_clock_lock.clone();
//         drop(keeper_clock_lock);
//         let acknowledgement = client
//             .send_clock(Clock {
//                 timestamp: keeper_clock,
//                 idx: idx as u64,
//                 initializing,
//                 step,
//             })
//             .await?;
//         Ok(acknowledgement.into_inner())
//     }

//     // send keys of finished lists
//     pub async fn send_key(&self, idx: usize, key: String) -> TribResult<bool> {
//         self.connect(idx).await?;
//         let keeper_client_opts = Arc::clone(&self.keeper_client_opts).lock_owned().await;
//         let client_opt = &keeper_client_opts[idx];

//         // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
//         let mut client = match client_opt {
//             // RPC client is clonable and works fine with concurrent clients.
//             Some(client) => client.clone(),
//             None => return Err("Client was somehow not connected / be initialized!".into()),
//         };
//         drop(keeper_client_opts); // client_opt needs the lock
//         client.send_key(Key { key }).await?;
//         Ok(true)
//     }

//     pub async fn initialization(&self) -> TribResult<bool> {
//         let addrs = self.keeper_addrs.clone();
//         let this = self.this;
//         let mut normal_join = false; // if this is a normal join operation
//         let start_time = Instant::now();
//         let mut current_time = Instant::now();

//         // detect other keepers for 5 seconds
//         while current_time.duration_since(start_time) < Duration::new(5, 0) {
//             for idx in 0..addrs.len() {
//                 // only contact other keepers
//                 if idx == this {
//                     continue;
//                 }
//                 // check acknowledgement
//                 let acknowledgement = self.send_clock(idx, true, 1).await; // step 1
//                 let mut statuses = self.statuses.write().await;
//                 match acknowledgement {
//                     // alive
//                     Ok(acknowledgement) => {
//                         // just a normal join operation
//                         if !acknowledgement.initializing {
//                             normal_join = true; // don't break here because we want to establish a complete keeper view
//                         }
//                         statuses[idx] = true; // maintain the keeper statuses
//                     }
//                     // down
//                     Err(_) => {
//                         statuses[idx] = false; // maintain the keeper statuses
//                     }
//                 }
//                 drop(statuses)
//             }
//             // break here because we have got enough information
//             if normal_join {
//                 break;
//             }
//             current_time = Instant::now(); // reset current time
//         }
//         // update the range
//         let _update_result = self.update_ranges().await;

//         // scan the backends and sleep for a specific amount of time
//         // todo!(); TODO
//         tokio::time::sleep(Duration::from_secs(4)); // sleep after the first scan
//         if normal_join {
//             // find the successor
//             let this = self.this;
//             let addrs = self.keeper_addrs.clone();
//             let statuses = self.statuses.read().await;
//             let mut successor_index = this;
//             for idx in this + 1..addrs.len() {
//                 if statuses[idx] {
//                     successor_index = idx;
//                     break;
//                 }
//             }
//             // hasn't find the successor yet
//             if successor_index == this {
//                 for idx in 0..this {
//                     if statuses[idx] {
//                         successor_index = idx;
//                         break;
//                     }
//                 }
//             }
//             drop(statuses);

//             // found a successor => get the acknowledged event
//             if successor_index != this {
//                 let acknowledgement = self.send_clock(successor_index, true, 2).await; // step 2
//                 match acknowledgement {
//                     // alive
//                     Ok(acknowledgement) => {
//                         if acknowledgement.event_type != "None" {
//                             let mut event_type = BackendEventType::Join;
//                             if acknowledgement.event_type == "Leave" {
//                                 event_type = BackendEventType::Leave;
//                             }

//                             let backend_event = BackendEvent {
//                                 event_type,
//                                 back_idx: acknowledgement.back_idx as usize,
//                                 timestamp: Instant::now(),
//                             };

//                             let mut event_acked_by_successor =
//                                 self.event_acked_by_successor.write().await;

//                             *event_acked_by_successor = Some(backend_event);
//                             drop(event_acked_by_successor);
//                         }
//                         // no need to store anything if the event is "None"
//                     }
//                     // Since this keeper just join, it's not possible that it's successor dies now.
//                     Err(_) => (),
//                 }
//             }
//         }
//         // finish initialization
//         let mut initializing = self.initializing.write().await;
//         *initializing = false;
//         drop(initializing);
//         Ok(true) // can send the ready signal
//     }

//     pub async fn update_ranges(&self) -> TribResult<()> {
//         let keeper_addrs = self.keeper_addrs.clone();
//         let this = self.this;
//         let http_back_addrs = self.http_back_addrs.clone();
//         let back_num = http_back_addrs.len();

//         let end_positions_lock = self.end_positions.read().await;
//         let end_positions = end_positions_lock.clone();
//         drop(end_positions_lock);

//         let statuses_lock = self.statuses.read().await;
//         let statuses = statuses_lock.clone();
//         drop(statuses_lock);

//         // get end positions of alive keepers
//         let mut alive_vector = Vec::<u64>::new();

//         for idx in 0..keeper_addrs.len() {
//             if statuses[idx] {
//                 alive_vector.push(end_positions[idx]);
//             }
//         }
//         // get the range
//         let mut predecessor_monitoring_range_inclusive =
//             self.predecessor_monitoring_range_inclusive.write().await;
//         let mut latest_monitoring_range_inclusive =
//             self.latest_monitoring_range_inclusive.write().await;
//         let alive_num = alive_vector.len();
//         if alive_num == 1 {
//             *predecessor_monitoring_range_inclusive = None;
//             let end_position = end_positions[this];
//             let start_position = (end_position + 1) % MAX_BACKEND_NUM;
//             if start_position >= back_num as u64 && end_position >= back_num as u64 {
//                 *latest_monitoring_range_inclusive = None;
//             } else if start_position >= back_num as u64 {
//                 *latest_monitoring_range_inclusive = Some((0, end_position as usize));
//             } else if end_position >= back_num as u64 {
//                 *latest_monitoring_range_inclusive = Some((start_position as usize, back_num - 1));
//             }
//         } else {
//             for idx in 0..alive_num {
//                 if alive_vector[idx] == end_positions[this] {
//                     let start_idx = ((idx - 1) + alive_num) % alive_num;
//                     let pre_start_idx = ((idx - 2) + alive_num) % alive_num;
//                     let start_position = (alive_vector[start_idx] + 1) % MAX_BACKEND_NUM;
//                     let end_position = end_positions[this];
//                     if start_position >= back_num as u64 && end_position >= back_num as u64 {
//                         *latest_monitoring_range_inclusive = None
//                     } else if start_position >= back_num as u64 {
//                         *latest_monitoring_range_inclusive = Some((0, end_position as usize));
//                     } else if end_position >= back_num as u64 {
//                         *latest_monitoring_range_inclusive =
//                             Some((start_position as usize, back_num - 1));
//                     }

//                     let pre_start_position = (alive_vector[pre_start_idx] + 1) % MAX_BACKEND_NUM;
//                     let pre_end_position = start_position - 1;
//                     if pre_start_position >= back_num as u64 && pre_end_position >= back_num as u64
//                     {
//                         *predecessor_monitoring_range_inclusive = None
//                     } else if pre_start_position >= back_num as u64 {
//                         *predecessor_monitoring_range_inclusive =
//                             Some((0, pre_end_position as usize));
//                     } else if pre_end_position >= back_num as u64 {
//                         *predecessor_monitoring_range_inclusive =
//                             Some((pre_start_position as usize, back_num - 1));
//                     }
//                 }
//             }
//         }
//         drop(latest_monitoring_range_inclusive);
//         drop(predecessor_monitoring_range_inclusive);
//         Ok(())
//     }

//     // monitor the statuses of other keepers and update the backend ranges
//     pub async fn monitor(&self) -> TribResult<bool> {
//         let addrs = self.keeper_addrs.clone(); // get keeper addresses
//         let this = self.this.clone(); // get current keeper index
//         loop {
//             // find the index of the predecessor
//             let predecessor_monitoring_range_inclusive_lock =
//                 self.predecessor_monitoring_range_inclusive.read().await;
//             let mut predecessor_index = None;
//             match *predecessor_monitoring_range_inclusive_lock {
//                 None => (),
//                 Some(predecessor_range) => {
//                     let predecessor_position = predecessor_range.1;
//                     let end_positions = self.end_positions.read().await;
//                     for idx in 0..end_positions.len() {
//                         if predecessor_position == end_positions[idx] as usize {
//                             predecessor_index = Some(idx);
//                             break;
//                         }
//                     }
//                     drop(end_positions);
//                 }
//             }
//             drop(predecessor_monitoring_range_inclusive_lock);

//             for idx in 0..addrs.len() {
//                 // only contact other keepers
//                 if idx == this {
//                     continue;
//                 }
//                 // check acknowledgement
//                 let acknowledgement = self.send_clock(idx, false, 0).await; // just a normal clock heartbeat
//                 let mut statuses = self.statuses.write().await;
//                 match acknowledgement {
//                     // alive
//                     Ok(acknowledgement) => {
//                         // When a keeper is initializing, we don't update its status.
//                         // When its successor acknowledge it, and it finishes initialization, we will update its status.
//                         if !acknowledgement.initializing {
//                             statuses[idx] = true; // maintain the keeper statuses
//                         }
//                     }
//                     // down
//                     Err(_) => {
//                         statuses[idx] = false; // maintain the keeper statuses
//                         match predecessor_index {
//                             None => (),
//                             Some(predecessor_index) => {
//                                 if predecessor_index == idx {
//                                     // call the take over function
//                                 }
//                             }
//                         }
//                     }
//                 }
//                 drop(statuses);
//             }
//             let _update_result = self.update_ranges().await; // update the range
//             sleep(Duration::from_secs(1)).await; // sleep for 1 second
//         }
//         Ok(true)
//     }
// }
// // When the predecessor crashes, we have to handle the backend crash for it.
