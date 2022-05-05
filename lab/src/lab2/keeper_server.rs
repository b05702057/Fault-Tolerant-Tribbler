use crate::lab1::client::StorageClient;
use std::{sync::{mpsc::Sender, Arc}};
use tokio::sync::{mpsc::Receiver, RwLock, Mutex};
use std::time::{Duration, Instant};
use tribbler::{config::KeeperConfig, err::TribResult, storage::Storage};
use std::collections::HashSet;

const KEEPER_SCAN_AND_SYNC_BACKEND_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct LiveBackendsView {
    /// start_idx, end_indx inclusive. Note it may be start_idx > end_idx 
    /// in which case we need to wrap around
    /// If None, then not monitoring any range for now.
    pub monitoring_range_inclusive: Option<(usize, usize)>,
    /// Indices that were known to be live from last scan of monitoring_range_inlusive
    pub live_backend_indices_in_range: Vec<usize>,
}

#[derive(PartialEq, Clone)]
pub enum BackendEventType {
    Join,
    Leave,
}

#[derive(Clone)]
pub struct BackendEvent {
    pub event_type: BackendEventType,
    pub back_idx: usize,
    pub timestamp: Instant,
}

// Periodically syncs backends
pub struct KeeperServer {
    /// The addresses of back-ends prefixed with "http://"" i.e. "http://<host>:<port>""
    /// HTTP2 gRPC client needs address of this form
    pub http_back_addrs: Vec<String>,
    /// The storage_clients clients to connect to each back end
    /// Each element in Vector correponds to the back_addrs at the same idx
    pub storage_clients: Vec<Arc<StorageClient>>,
    /// The addresses of keepers
    pub keeper_addrs: Vec<String>,
    /// The index of this back-end
    pub this_back_index: usize,
    /// Non zero incarnation identifier
    pub id: u128,
    /// Send a value when the keeper is ready. The distributed key-value
    /// service should be ready to serve when *any* of the keepers is
    /// ready.
    pub ready_sender_opt: Option<Sender<bool>>,
    /// When a message is received on this channel, it should trigger a
    /// graceful shutdown of the server. If no channel is present, then
    /// no graceful shutdown mechanism needs to be implemented.
    pub shutdown_receiver_opt: Option<Receiver<()>>,
    /// Whether to shutdown keeper or not. Note tokio::sync:RwLock needed
    /// since bin_run requires the KeeperServer returned by serve_keeper
    /// to by Sync (for tokio spawn)
    pub should_shutdown: Arc<RwLock<bool>>,

    /// Handles to abort when shutdown is received
    pub saved_tasks_spawned_handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    
    /// Last backends view result from last scan. Should be initialized after 1st scan
    pub live_backends_view: Arc<RwLock<LiveBackendsView>>,
    /// Latest range known to monitor / range to use for next scan
    /// Note it may be start_idx > end_idx in which case we need to wrap around
    pub latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,

    /// Last time we sent ack to predecessor who was requesting join intialization 
    pub recent_keeper_join_ACK_time: Arc<RwLock<Option<Instant>>>,

    /// Event last Acked by successor
    pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,

    /// To lock before code that uses data for event handling negotiation between 
    /// predecessor and successor
    pub event_handling_mutex: Arc<Mutex<u64>>,
}

impl KeeperServer {
    pub async fn new(kc: KeeperConfig) -> TribResult<KeeperServer> {
        let http_back_addrs = kc
            .backs
            .into_iter()
            .map(|back_addr| format!("http://{}", back_addr))
            .collect::<Vec<String>>();

        let mut storage_clients = vec![];
        for http_back_addr in http_back_addrs.iter() {
            storage_clients.push(Arc::new(StorageClient::new(http_back_addr)));
        }

        Ok(KeeperServer {
            http_back_addrs: http_back_addrs,
            storage_clients: storage_clients,
            keeper_addrs: kc.addrs,
            this_back_index: kc.this,
            id: kc.id,
            ready_sender_opt: kc.ready,
            shutdown_receiver_opt: kc.shutdown,
            should_shutdown: Arc::new(RwLock::new(false)),
            saved_tasks_spawned_handles: Arc::new(Mutex::new(vec![])),
            live_backends_view: Arc::new(RwLock::new(LiveBackendsView {
                monitoring_range_inclusive: None,
                live_backend_indices_in_range: vec![],
            })),
            latest_monitoring_range_inclusive: Arc::new(RwLock::new(None)),
            recent_keeper_join_ACK_time: Arc::new(RwLock::new(None)),
            event_acked_by_successor: Arc::new(RwLock::new(None)),
            event_handling_mutex: Arc::new(Mutex::new(0)),
        })
    }

    fn is_same_backend_event(earlier_backend_event: BackendEvent, later_backend_event: BackendEvent) -> bool {
        // Must be same event type and backend idx to have a chance of being same event
        if later_backend_event.back_idx != earlier_backend_event.back_idx || later_backend_event.event_type != earlier_backend_event.event_type {
            return false;
        }
        
        // Since 1 event every 30 secs at most, this is good estimation. We don't even need 20 since scanning interval is a lot smaller
        return later_backend_event.timestamp.saturating_duration_since(earlier_backend_event.timestamp) < Duration::from_secs(20);
    }


    async fn periodic_scan_and_sync_backend(
        live_backends_view_arc: Arc<RwLock<LiveBackendsView>>,
        clients_for_scanning: Vec<Arc<StorageClient>>,  // all backend's clients
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,  // expect to be latest_monitoring_range_inclusive
        should_shutdown: Arc<RwLock<bool>>,
        keeper_clock: Arc<RwLock<u64>>,
        recent_keeper_join_ACK_time: Arc<RwLock<Option<Instant>>>,
        event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
        event_handling_mutex: Arc<Mutex<u64>>,
    ) -> TribResult<()> {
        // To synchronize backends. Initialize to 0
        loop {
            let should_shutdown_guard = should_shutdown.read().await;

            // First if should_shutdown is set to true (by async listen for shutdown function), can break.
            if *should_shutdown_guard == true {
                break;
            } else {
                drop(should_shutdown_guard);
            }

            // Sync backends by retrieving the global maximum clock and calling clock with that
            // value on all backends

            let range_to_scan_lock = latest_monitoring_range_inclusive.read().await;
            let range_to_scan_opt = *range_to_scan_lock;
            drop(range_to_scan_lock);

            // Only scan if there is a valid range to do so
            if let Some(range_to_scan) = range_to_scan_opt {
                // Clone clients since later moving into async for tokio spawn async execution.
                let storage_clients_clones = clients_for_scanning.clone();

                let mut keeper_clock_lock = keeper_clock.read().await;
                let global_max_clock = *keeper_clock_lock;
                drop(keeper_clock_lock);

                println!("[DEBUGGING] keeper_server's periodic_scan_and_sync: Before syncing clock(global_max_clock = {}) on backends", global_max_clock);

                let (cur_live_back_indices, global_max_clock) = Self::single_scan_and_sync(storage_clients_clones, range_to_scan, global_max_clock).await?;
                println!(
                    "[DEBUGGING] bin_client's periodic_scan: live_addrs.len(): {}",
                    cur_live_back_indices.len()
                );

                // TODO update clock reference of keeper
                let mut k_clock = keeper_clock.write().await;
                *k_clock = global_max_clock;
                drop(k_clock);


                // Get previous live view as well as update it to the new live view
                let new_live_backends_view = LiveBackendsView {
                    monitoring_range_inclusive: Some(range_to_scan),
                    live_backend_indices_in_range: cur_live_back_indices.clone(),
                };
                let mut live_backends_view_lock = live_backends_view_arc.write().await;
                let prev_live_backends_view = (*live_backends_view_lock).clone();  // save prev view for event detection
                *live_backends_view_lock = new_live_backends_view;  // update view
                drop(live_backends_view_lock);


                // If range of previous view and current view is different, only compare the
                // view portion that are overlapping 
                // Later extract the relevant range of view to compare for event detection if needed
                let mut prev_live_indices_in_overlapping_range: Vec<usize> = vec![];
                let mut cur_live_indices_in_overlapping_range: Vec<usize> = vec![];
                
                let prev_monitor_range = prev_live_backends_view.monitoring_range_inclusive.clone();

                // Get overlapping range and filter out indices in  prev_live_indices_in_overlapping_range and 
                // cur_live_indices_in_overlapping_range that are not in the overlapping range 
                if prev_monitor_range != range_to_scan_opt {
                    
                    // Set of backend indices that were in the previous view range
                    let mut prev_range_set: HashSet<usize> = HashSet::new();
                    match prev_monitor_range {
                        Some((prev_range_start, prev_range_end)) => {
                            let mut back_idx = prev_range_start;
                            loop {
                                prev_range_set.insert(back_idx);
                                // Break once idx at end of range has been processed
                                if back_idx == prev_range_end {
                                    break;
                                }
                                back_idx = (back_idx + 1) % clients_for_scanning.len();
                            }
                        },
                        None => (),
                    }

                    // Set of backend indices that are in the recently scanned view range
                    let mut cur_range_set: HashSet<usize> = HashSet::new();
                    let (cur_range_start, cur_range_end) = range_to_scan;
                    let mut back_idx = cur_range_start;
                    loop {
                        cur_range_set.insert(back_idx);
                        // Break once idx at end of range has been processed
                        if back_idx == cur_range_end {
                            break;
                        }
                        back_idx = (back_idx + 1) % clients_for_scanning.len();
                    }

                    // Extract prev view back indices in overlapping range
                    for back_idx in prev_live_backends_view.live_backend_indices_in_range.iter() {
                        // Overlapping if in both set
                        if prev_range_set.contains(back_idx) && cur_range_set.contains(back_idx) {
                            prev_live_indices_in_overlapping_range.push(back_idx.clone());
                        }
                    }

                    // Extract cur view back indices in overlapping range
                    for back_idx in cur_live_back_indices.iter() {
                        // Overlapping if in both set
                        if prev_range_set.contains(back_idx) && cur_range_set.contains(back_idx) {
                            cur_live_indices_in_overlapping_range.push(back_idx.clone());
                        }
                    }

                    // TODO update prev_live_indices_in_overlapping_range and prev_live_indices_in_overlapping_range
                    // to only have entries of back_idx in the appropriate ranges
                } else {
                    // Else since the ranges are the same, overlapping range is the same, so no need to do any filter
                    prev_live_indices_in_overlapping_range = prev_live_backends_view.live_backend_indices_in_range;
                    cur_live_indices_in_overlapping_range = cur_live_back_indices;
                }

                // Compare cur and prev overlapping views to detect any events

                let mut event_detected: Option<BackendEvent> = None;
                // View can only change by 1 addition or 1 removal of server. 
                // Since both crash and leave cannot happen within 30 seconds, if length the same,
                // means that no events occured
                if cur_live_indices_in_overlapping_range.len() > prev_live_indices_in_overlapping_range.len() {
                    // Join event case

                    // Make hash set of prev.
                    let prev_live_set: HashSet<usize> = prev_live_indices_in_overlapping_range.into_iter().collect();
                    // Check which entry of curr is not in prev hash set
                    for back_idx in cur_live_indices_in_overlapping_range {
                        // If not in prev hash set, then that is the newly joined backend
                        if !prev_live_set.contains(&back_idx) {
                            event_detected = Some(BackendEvent {
                                event_type: BackendEventType::Join,
                                back_idx: back_idx, 
                                timestamp: Instant::now(),
                            });
                            break;
                        }
                    }

                } else if cur_live_indices_in_overlapping_range.len() < prev_live_indices_in_overlapping_range.len() {
                    // Crash event case

                    // Make hash set of cur.
                    let cur_live_set: HashSet<usize> = cur_live_indices_in_overlapping_range.into_iter().collect();
                    // Check which entry of prev is not in cur hash set
                    for back_idx in prev_live_indices_in_overlapping_range {
                        // If not in cur hash set, then that is the crashed backend
                        if !cur_live_set.contains(&back_idx) {
                            event_detected = Some(BackendEvent {
                                event_type: BackendEventType::Leave,
                                back_idx: back_idx, 
                                timestamp: Instant::now(),
                            });
                            break;
                        }
                    }
                }
                
                // If detected an event, decide whether to do something
                if let Some(event) = event_detected {

                    // Initialized to true, and conditions check below will set to false if necessary.
                    let mut should_handle_event = true;

                    //------------------ Start of large ATOMIC section------------------------
                    let event_handling_mutex_lock = event_handling_mutex.lock().await;
                    
                    let last_ack_time_lock = recent_keeper_join_ACK_time.read().await;
                    let last_ack_time_opt = *last_ack_time_lock;
                    drop(last_ack_time_lock);

                    // If our last ack time was recent, and the event is in the predecessor range that we 
                    // just gave up, then let predecessor take care of it, ignore event.
                    // This can be done by checking to see if the event's back_idx is still in the latest 
                    // range we have. If not, then the range must have reduced compared to the
                    // range_to_scan we started with and we just gave up that range to the predecessor.
                    
                    if let Some(last_ack_time) = last_ack_time_opt {
                        if Instant::now().saturating_duration_since(last_ack_time) < Duration::from_secs(10) {
                            // Now here know we recently sent an ACK to the predecessor.
                            // Only ignore if event is in predecessor range that we just gave up. 

                            // Fetch the latest range again to check for changes since scan start
                            let new_range_lock = latest_monitoring_range_inclusive.read().await;
                            let new_range = new_range_lock.clone();
                            drop(new_range_lock);

                            match new_range {
                                // See if event back idx still in our range. If yes then we handle event, else
                                // predecessor will handle since we acked recently.
                                Some((new_range_start, new_range_end)) => {
                                    let mut still_in_range = false;
                                    let mut back_idx = new_range_start;
                                    loop {
                                        if back_idx == event.back_idx {
                                            still_in_range = true;
                                            break;
                                        }
                                        // Break once idx at end of range has been processed
                                        if back_idx == new_range_end {
                                            break;
                                        }
                                        back_idx = (back_idx + 1) % clients_for_scanning.len();
                                    }
                                    
                                    // If in predecessor range, then let it handle now since we already acked.
                                    should_handle_event = !still_in_range;
                                },
                                // If new range is None, we have reduced our scan range to nothing so the 
                                // event back idx must be in predecessors range.
                                None => should_handle_event = false,
                            }
                        }
                    }

                    let event_acked_by_successor_lock = event_acked_by_successor.read().await;
                    let event_acked_by_successor_opt = (*event_acked_by_successor_lock).clone();
                    drop(event_acked_by_successor_lock);

                    // If our successor acked to us the event (recently), let successor do it
                    if let Some(event_acked_by_successor) = event_acked_by_successor_opt {
                        // If acked by successor, then successor is handling it.
                        if Self::is_same_backend_event(event_acked_by_successor, event) {
                            should_handle_event = false;
                        } 
                    } 

                    drop(event_handling_mutex_lock);
                    //----------------------- End of ATOMIC section-----------------

                    // TODO depending on event, will do migration / replication / deletion logic
                    // This logic can be blocking since, no need to scan if this is in progress, since no new backend 
                    // events will happen during that period
                    if should_handle_event {
                        // TODO
                    }

                }

            }

            // Wait interval til next scan
            tokio::time::sleep(KEEPER_SCAN_AND_SYNC_BACKEND_INTERVAL).await;
        }

        Ok(())
    }

    // // PSEUDOCODE TODO
    // async fn replicate_data_from_A_to_B(clientA, clientB, backend_event) {
    //     if backend_event is Join:
        
    //     else:

    // }


    // Performs scan on clients in range and returns live_backend_indexes from that range as well as the max clock
    // Range_to_scan (start_idx, end_idx) may have start_idx > end_idx, in which case we wrap around
    // Assume storage_clients are client vec for all backends
    async fn single_scan_and_sync(storage_clients: Vec<Arc<StorageClient>>, range_to_scan: (usize, usize), global_max_clock: u64) -> TribResult<(Vec<usize>, u64)> {
        let mut global_max_clock = global_max_clock;
        // Only operate on those in range
        let (scan_range_start, scan_range_end) = range_to_scan;
        let mut storage_clients_in_range = vec![];
        
        // Rertrieve the clients that need to be scanned
        let mut cur_back_idx = scan_range_start;
        loop {
            storage_clients_in_range.push(Arc::clone(&storage_clients[cur_back_idx]));
            // Break once idx at scan_range_end has been processed
            if (cur_back_idx == scan_range_end) {
                break;
            }
            cur_back_idx = (cur_back_idx + 1) % storage_clients.len();
        }

        let tasks: Vec<_> = storage_clients_in_range
            .into_iter()
            .map(|storage_client| {
                // Note deliberately NOT adding ";" to the async function as well as the
                // spawn statements since they are used as expression return results
                // Calling clock with largest seen so faar
                tokio::spawn(async move { storage_client.clock(global_max_clock).await })
            })
            .collect();

        let mut cur_live_back_indices = vec![];

        // Note chaining of "??" is needed. One is for the tokio's spawned task error
        // capturing (a Result<>) and the other is from our connect() function which
        // is also another Result
        let mut cur_back_idx = scan_range_start;
        for task in tasks.into_iter() {
            // If connection successful, then server is live
            match task.await? {
                Ok(clock_val) =>{
                    global_max_clock = std::cmp::max(global_max_clock, clock_val);
                    cur_live_back_indices.push(cur_back_idx);
                },
                Err(_) => (),
            }
            // Wrap around
            cur_back_idx = (cur_back_idx + 1) % storage_clients.len();
        }

        Ok((cur_live_back_indices, global_max_clock))
    }

    // Range_to_scan (start_idx, end_idx) may have start_idx > end_idx, in which case we wrap around
    async fn first_scan_for_initialization(&self, range_to_scan: (usize, usize)) -> TribResult<Vec<usize>> {
        let storage_clients_clones = self.storage_clients.clone();
        let (cur_live_back_indices, _) = Self::single_scan_and_sync(storage_clients_clones, range_to_scan, 0).await?;

        let new_live_backends_view = LiveBackendsView {
            monitoring_range_inclusive: Some(range_to_scan),
            live_backend_indices_in_range: cur_live_back_indices.clone()
        };

        // Update live backends view
        let mut live_backends_view = self.live_backends_view.write().await;
        *live_backends_view = new_live_backends_view;
        drop(live_backends_view);

        Ok(cur_live_back_indices)
    }

    pub async fn serve(&mut self) -> TribResult<()> {
        // Listen for shut down asynchronously
        let should_shutdown_clone = Arc::clone(&self.should_shutdown);

        // Send ready
        if let Some(ready_sender) = &self.ready_sender_opt {
            ready_sender.send(true)?;
        }

        // Sync backends every 1 second
        const KEEPER_BACKEND_SYNC_INTERVAL: Duration = Duration::from_millis(1000);

        let periodic_scan_and_sync_handle = tokio::spawn(async move {
            Self::periodic_scan(
                http_back_addrs,
                live_http_back_addrs_arc,
                clients_for_scanning,
            )
            .await;
            ()
        });
        // TODO Push to tasks_spawned_handles 
        // Maybe not needed for this function since shutdown guard is checked in there.
        let saved_tasks_spawned_handles_lock = self.saved_tasks_spawned_handles.lock().await;
        (*saved_tasks_spawned_handles_lock).push(periodic_scan_and_sync_handle);


        // Block on the shutdown signal if exists
        if let Some(shutdown_receiver) = self.shutdown_receiver_opt.take() {
            shutdown_receiver.recv().await;
            // Gracefully close
            shutdown_receiver.close();

            // Indicate shut down is requested so that other async tasks would shutdown
            let mut should_shutdown_guard = self.should_shutdown.write().await;
            *should_shutdown_guard = true;
        } else {
            // Else block indefinitely by awaiting on the pending future
            let future = core::future::pending();
            let res: i32 = future.await;
        }

        Ok(())
    }
}
