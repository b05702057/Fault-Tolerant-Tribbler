use super::migration::migration_event;
use crate::keeper::keeper_rpc_client::KeeperRpcClient;
use crate::lab1::client::StorageClient;
use std::collections::{HashMap, HashSet};
use std::iter::successors;
use std::sync::{mpsc::Sender, Arc};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc::Receiver, Mutex, RwLock};

use tonic::Response;
use tribbler::{config::KeeperConfig, err::TribResult, storage::Storage};

use crate::{
    keeper,
    keeper::{Acknowledgement, Bool, Clock, Key},
};

use async_trait::async_trait;
use std::cmp;
const KEEPER_SCAN_AND_SYNC_BACKEND_INTERVAL: Duration = Duration::from_secs(2);
const KEEPER_WAIT_UPON_EVENT_DETECTION_INTERVAL: Duration = Duration::from_secs(5);
const MAX_BACKEND_NUM: u64 = 300;
pub const LIST_KEY_TYPE_STR: &str = "list_key_type";
pub const REGULARY_KEY_TYPE_STR: &str = "regular_key_type";

#[derive(Clone)]
pub struct LiveBackendsView {
    /// start_idx, end_indx inclusive. Note it may be start_idx > end_idx
    /// in which case we need to wrap around
    /// If None, then not monitoring any range for now.
    pub monitoring_range_inclusive: Option<(usize, usize)>,
    /// Indices that were known to be live from last scan of monitoring_range_inlusive
    pub live_backend_indices_in_range: Vec<usize>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum BackendEventType {
    Join,
    Leave,
    None, // Only for RPC to save type.
}

#[derive(Clone, Debug)]
pub struct BackendEvent {
    pub event_type: BackendEventType,
    pub back_idx: usize,
    pub timestamp: Instant,
}

#[derive(PartialEq, Clone)]
pub struct LogEntry {
    pub key: String,
    pub key_type: String,
    pub timestamp: Instant, // use the timestamp to know if it belongs to the current event
}

impl Eq for LogEntry {}

#[derive(PartialEq, Clone, Hash, Debug)]
pub struct DoneEntry {
    pub key: String,
    pub key_type: String,
}

impl Eq for DoneEntry {}

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
    pub this: usize,
    /// Non zero incarnation identifier
    pub id: u128,
    /// Send a value when the keeper is ready. The distributed key-value
    /// service should be ready to serve when *any* of the keepers is
    /// ready.
    pub ready_sender_opt: Arc<Mutex<Option<Sender<bool>>>>,
    /// When a message is received on this channel, it should trigger a
    /// graceful shutdown of the server. If no channel is present, then
    /// no graceful shutdown mechanism needs to be implemented.
    pub shutdown_receiver_opt: Arc<Mutex<Option<Receiver<()>>>>,
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
    /// Range is assumed to be based on the backend list length.
    pub latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,

    /// Last predecessor backends view result from last scan for predecessor region
    pub predecessor_live_backends_view: Arc<RwLock<LiveBackendsView>>,
    /// Latest range known to monitor / range to use for next scan
    /// Note it may be start_idx > end_idx in which case we need to wrap around
    /// Range is assumed to be based on the backend list length.
    pub predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
    /// To save detected event in the predecessor region in case predecessor fails
    pub predecessor_event_detected: Arc<RwLock<Option<BackendEvent>>>,

    /// Last time we sent ack to predecessor who was requesting join intialization
    pub ack_to_predecessor_time: Arc<RwLock<Option<Instant>>>,

    /// Event last Acked by successor
    pub event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,

    /// Event detected by us
    pub event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,

    /// To lock before code that uses data for event handling negotiation between
    /// predecessor and successor
    pub event_handling_mutex: Arc<Mutex<u64>>,

    pub statuses: Arc<RwLock<Vec<bool>>>,
    pub end_positions: Arc<RwLock<Vec<u64>>>, // keeper end positions on the ring
    pub keeper_clock: Arc<RwLock<u64>>,       // keeper_clock of this keeper
    pub log_entries: Arc<RwLock<Vec<LogEntry>>>, // store the keys of finsihed lists to help migration
    pub initializing: Arc<RwLock<bool>>,         // if this keeper is initializing
    pub keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>, // keeper connections
}

impl KeeperServer {
    pub async fn new(
        kc: KeeperConfig,
        should_shutdown: Arc<RwLock<bool>>,
    ) -> TribResult<KeeperServer> {
        let http_back_addrs = kc
            .backs
            .into_iter()
            .map(|back_addr| format!("http://{}", back_addr))
            .collect::<Vec<String>>();

        let mut storage_clients = vec![];
        for http_back_addr in http_back_addrs.iter() {
            storage_clients.push(Arc::new(StorageClient::new(http_back_addr)));
        }

        let keeper_addrs = kc
            .addrs
            .into_iter()
            .map(|keeper_addr| format!("http://{}", keeper_addr))
            .collect::<Vec<String>>();

        let this = kc.this;
        let mut statuses = Vec::<bool>::new();
        let mut end_positions = Vec::<u64>::new();
        let manage_num = MAX_BACKEND_NUM / (keeper_addrs.len() as u64); // use 300 directly to avoid edge cases of using http_back_addrs.len()
        let mut keeper_client_opts =
            Vec::<Option<KeeperRpcClient<tonic::transport::Channel>>>::new();
        for idx in 0..keeper_addrs.len() {
            statuses.push(false);
            let end_position = (idx as u64 + 1) * manage_num - 1;
            end_positions.push(end_position);
            keeper_client_opts.push(None);
        }
        statuses[this] = true;

        let keeper_server = KeeperServer {
            http_back_addrs,
            storage_clients,
            keeper_addrs,
            this: kc.this,
            id: kc.id,
            ready_sender_opt: Arc::new(Mutex::new(kc.ready)),
            shutdown_receiver_opt: Arc::new(Mutex::new(kc.shutdown)),
            should_shutdown,
            saved_tasks_spawned_handles: Arc::new(Mutex::new(vec![])),
            live_backends_view: Arc::new(RwLock::new(LiveBackendsView {
                monitoring_range_inclusive: None,
                live_backend_indices_in_range: vec![],
            })),
            latest_monitoring_range_inclusive: Arc::new(RwLock::new(None)),
            predecessor_live_backends_view: Arc::new(RwLock::new(LiveBackendsView {
                monitoring_range_inclusive: None,
                live_backend_indices_in_range: vec![],
            })),
            predecessor_monitoring_range_inclusive: Arc::new(RwLock::new(None)),
            predecessor_event_detected: Arc::new(RwLock::new(None)),
            ack_to_predecessor_time: Arc::new(RwLock::new(None)),
            event_acked_by_successor: Arc::new(RwLock::new(None)),
            event_detected_by_this: Arc::new(RwLock::new(None)),
            event_handling_mutex: Arc::new(Mutex::new(0)),
            statuses: Arc::new(RwLock::new(statuses)),
            end_positions: Arc::new(RwLock::new(end_positions)),
            keeper_clock: Arc::new(RwLock::new(0)),
            log_entries: Arc::new(RwLock::new(Vec::<LogEntry>::new())),
            initializing: Arc::new(RwLock::new(true)),
            keeper_client_opts: Arc::new(Mutex::new(keeper_client_opts)),
        };
        Ok(keeper_server)
    }

    fn is_same_backend_event(
        earlier_backend_event: &BackendEvent,
        later_backend_event: &BackendEvent,
    ) -> bool {
        // Must be same event type and backend idx to have a chance of being same event
        if later_backend_event.back_idx != earlier_backend_event.back_idx
            || later_backend_event.event_type != earlier_backend_event.event_type
        {
            return false;
        }

        // Since 1 event every 30 secs at most, this is good estimation. We don't even need 20 since scanning interval is a lot smaller
        return later_backend_event
            .timestamp
            .saturating_duration_since(earlier_backend_event.timestamp)
            < Duration::from_secs(20);
    }

    // For our range
    async fn periodic_scan_and_sync_backend(
        live_backends_view_arc: Arc<RwLock<LiveBackendsView>>,
        clients_for_scanning: Vec<Arc<StorageClient>>, // all backend's clients
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>, // expect to be latest_monitoring_range_inclusive
        should_shutdown: Arc<RwLock<bool>>,
        keeper_clock: Arc<RwLock<u64>>,
        ack_to_predecessor_time: Arc<RwLock<Option<Instant>>>,
        event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
        event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
        event_handling_mutex: Arc<Mutex<u64>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        keeper_addrs: Vec<String>,
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        this: usize, // this keeper index
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

                let keeper_clock_lock = keeper_clock.read().await;
                let global_max_clock = *keeper_clock_lock;
                drop(keeper_clock_lock);

                // println!("[DEBUGGING] keeper_server's periodic_scan_and_sync: Before syncing clock(global_max_clock = {}) on backends", global_max_clock);

                let (cur_live_back_indices, global_max_clock) = Self::single_scan_and_sync(
                    storage_clients_clones,
                    range_to_scan,
                    global_max_clock,
                )
                .await?;
                // println!(
                //     "[DEBUGGING] bin_client's periodic_scan: live_addrs.len(): {}",
                //     cur_live_back_indices.len()
                // );

                // Update clock reference of keeper
                let mut k_clock = keeper_clock.write().await;
                *k_clock = global_max_clock;
                drop(k_clock);

                // Get previous live view as well as update it to the new live view
                let new_live_backends_view = LiveBackendsView {
                    monitoring_range_inclusive: Some(range_to_scan),
                    live_backend_indices_in_range: cur_live_back_indices.clone(),
                };
                let mut live_backends_view_lock = live_backends_view_arc.write().await;
                let prev_live_backends_view = (*live_backends_view_lock).clone(); // save prev view for event detection
                *live_backends_view_lock = new_live_backends_view; // update view
                drop(live_backends_view_lock);

                // Now is logic to detect any event

                // If range of previous view and current view is different, only compare the
                // view portion that are overlapping
                // Later extract the relevant range of view to compare for event detection if needed
                let mut prev_live_indices_in_overlapping_range: Vec<usize> = vec![];
                let mut cur_live_indices_in_overlapping_range: Vec<usize> = vec![];

                let prev_monitor_range = prev_live_backends_view.monitoring_range_inclusive.clone();

                // Get overlapping range and filter out indices in vectors to compare that are not in the overlapping range
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
                        }
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
                    prev_live_indices_in_overlapping_range =
                        prev_live_backends_view.live_backend_indices_in_range;
                    cur_live_indices_in_overlapping_range = cur_live_back_indices;
                }

                // Compare cur and prev overlapping views to detect any events

                let mut event_detected: Option<BackendEvent> = None;
                // View can only change by 1 addition or 1 removal of server.
                // Since both crash and leave cannot happen within 30 seconds, if length the same,
                // means that no events occured
                if cur_live_indices_in_overlapping_range.len()
                    > prev_live_indices_in_overlapping_range.len()
                {
                    // Join event case

                    // Make hash set of prev.
                    let prev_live_set: HashSet<usize> =
                        prev_live_indices_in_overlapping_range.into_iter().collect();
                    // Check which entry of curr is not in prev hash set
                    for back_idx in cur_live_indices_in_overlapping_range {
                        // If not in prev hash set, then that is the newly joined backend
                        if !prev_live_set.contains(&back_idx) {
                            event_detected = Some(BackendEvent {
                                event_type: BackendEventType::Join,
                                back_idx,
                                timestamp: Instant::now(),
                            });
                            break;
                        }
                    }
                } else if cur_live_indices_in_overlapping_range.len()
                    < prev_live_indices_in_overlapping_range.len()
                {
                    // Crash event case

                    // Make hash set of cur.
                    let cur_live_set: HashSet<usize> =
                        cur_live_indices_in_overlapping_range.into_iter().collect();
                    // Check which entry of prev is not in cur hash set
                    for back_idx in prev_live_indices_in_overlapping_range {
                        // If not in cur hash set, then that is the crashed backend
                        if !cur_live_set.contains(&back_idx) {
                            event_detected = Some(BackendEvent {
                                event_type: BackendEventType::Leave,
                                back_idx,
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

                    let last_ack_time_lock = ack_to_predecessor_time.read().await;
                    let last_ack_time_opt = *last_ack_time_lock;
                    drop(last_ack_time_lock);

                    // If our last ack time was recent, and the event is in the predecessor range that we
                    // just gave up, then let predecessor take care of it, ignore event.
                    // This can be done by checking to see if the event's back_idx is still in the latest
                    // range we have. If not, then the range must have reduced compared to the
                    // range_to_scan we started with and we just gave up that range to the predecessor.

                    if let Some(last_ack_time) = last_ack_time_opt {
                        if Instant::now().saturating_duration_since(last_ack_time)
                            < Duration::from_secs(10)
                        {
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
                                }
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
                        if Self::is_same_backend_event(&event_acked_by_successor, &event) {
                            should_handle_event = false;
                        }
                    }

                    if should_handle_event {
                        // Remeber that we will be processing this event for future ACKs to predecessor.
                        let mut event_detected_by_this_lock = event_detected_by_this.write().await;
                        *event_detected_by_this_lock = Some(event.clone());
                        drop(event_detected_by_this_lock);
                    }

                    drop(event_handling_mutex_lock);
                    //----------------------- End of ATOMIC section-----------------

                    // TODO depending on event, will do migration / replication / deletion logic
                    // This logic can be blocking since, no need to scan if this is in progress, since no new backend
                    // events will happen during that period
                    if should_handle_event {
                        // Start scan first of all range (spawn)
                        // Wait KEEPER_WAIT_UPON_EVENT_DETECTION_INTERVAL seconds and then do migration etc.

                        // Firsto scanning ALL to have all live backends for easy migration/replication processing

                        // Clone clients since later moving into async for tokio spawn async execution.
                        let storage_clients_clones = clients_for_scanning.clone();

                        let all_backends_range = (0, storage_clients_clones.len() - 1);

                        // Spawn so can start wait timer immediately as the scan is running
                        let scan_all_join_handle = tokio::spawn(async move {
                            Self::single_scan_and_sync(
                                storage_clients_clones,
                                all_backends_range,
                                global_max_clock,
                            )
                            .await
                        });

                        tokio::time::sleep(KEEPER_WAIT_UPON_EVENT_DETECTION_INTERVAL).await;

                        // Note double chaining
                        let (all_live_back_indices, _) = scan_all_join_handle.await??;
                        let storage_clients_clones = clients_for_scanning.clone();

                        let mut successor_keeper_client: Option<
                            KeeperRpcClient<tonic::transport::Channel>,
                        > = None;

                        // Keeper statuses
                        let statuses_lock = statuses.write().await;
                        let statuses_clone = (*statuses_lock).clone();
                        drop(statuses_lock);

                        let found_index =
                            Self::find_successor_index(statuses_clone, this, keeper_addrs.len());
                        if found_index != this {
                            let successor_index = found_index;

                            Self::connect(
                                Arc::clone(&keeper_client_opts),
                                keeper_addrs.clone(),
                                successor_index,
                            )
                            .await?;
                            let keeper_client_opts =
                                Arc::clone(&keeper_client_opts).lock_owned().await;
                            successor_keeper_client = match &keeper_client_opts[successor_index] {
                                Some(keeper_client) => Some(keeper_client.clone()),
                                None => None,
                            };
                        }

                        println!("\n[DEBUGGING] ----------Regular migration started!-----------\n");
                        println!("event is: {:?}", &event);
                        println!("live_https is: {:?}", &all_live_back_indices);

                        // TODO call migration event passing in all_live_back_indices, event, and storage_clients_clones
                        match migration_event(
                            &event,
                            all_live_back_indices,
                            storage_clients_clones,
                            None,
                            global_max_clock,
                            successor_keeper_client,
                        )
                        .await
                        {
                            Ok(_) => (),
                            Err(_) => return Err("Migration err".into()),
                        }
                    }
                }
            }

            // Wait interval til next scan
            tokio::time::sleep(KEEPER_SCAN_AND_SYNC_BACKEND_INTERVAL).await;
        }

        Ok(())
    }

    // For predecessor range
    async fn predecessor_range_periodic_scan_backend(
        predecessor_live_backends_view_arc: Arc<RwLock<LiveBackendsView>>,
        clients_for_scanning: Vec<Arc<StorageClient>>, // all backend's clients
        predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>, // expect to be predecessor_monitoring_range_inclusive
        should_shutdown: Arc<RwLock<bool>>,
        predecessor_event_detected: Arc<RwLock<Option<BackendEvent>>>,
    ) -> TribResult<()> {
        // To help monitor predecessor region
        let mut global_max_clock = 0;
        loop {
            let should_shutdown_guard = should_shutdown.read().await;

            // First if should_shutdown is set to true (by async listen for shutdown function), can break.
            if *should_shutdown_guard == true {
                break;
            } else {
                drop(should_shutdown_guard);
            }

            // Check for any events in predecessor region

            let range_to_scan_lock = predecessor_monitoring_range_inclusive.read().await;
            let range_to_scan_opt = *range_to_scan_lock;
            drop(range_to_scan_lock);

            // Only scan if there is a valid range to do so
            if let Some(range_to_scan) = range_to_scan_opt {
                // Clone clients since later moving into async for tokio spawn async execution.
                let storage_clients_clones = clients_for_scanning.clone();

                // println!("[DEBUGGING] keeper_server's predecessor scan: Before syncing clock(global_max_clock = {}) on backends", global_max_clock);

                let (cur_live_back_indices, clock_max_res) = Self::single_scan_and_sync(
                    storage_clients_clones,
                    range_to_scan,
                    global_max_clock,
                )
                .await?;

                global_max_clock = clock_max_res;

                // Get previous live view as well as update it to the new live view
                let new_live_backends_view = LiveBackendsView {
                    monitoring_range_inclusive: Some(range_to_scan),
                    live_backend_indices_in_range: cur_live_back_indices.clone(),
                };
                let mut predecessor_live_backends_view_lock =
                    predecessor_live_backends_view_arc.write().await;
                let prev_predecessor_live_backends_view =
                    (*predecessor_live_backends_view_lock).clone(); // save prev view for event detection
                *predecessor_live_backends_view_lock = new_live_backends_view; // update view
                drop(predecessor_live_backends_view_lock);

                // Now is logic to detect any event

                // If range of previous view and current view is different, only compare the
                // view portion that are overlapping
                // Later extract the relevant range of view to compare for event detection if needed
                let mut prev_live_indices_in_overlapping_range: Vec<usize> = vec![];
                let mut cur_live_indices_in_overlapping_range: Vec<usize> = vec![];

                let prev_monitor_range = prev_predecessor_live_backends_view
                    .monitoring_range_inclusive
                    .clone();

                // Get overlapping range and filter out indices in vectors to compare that are not in the overlapping range
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
                        }
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
                    for back_idx in prev_predecessor_live_backends_view
                        .live_backend_indices_in_range
                        .iter()
                    {
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
                    prev_live_indices_in_overlapping_range =
                        prev_predecessor_live_backends_view.live_backend_indices_in_range;
                    cur_live_indices_in_overlapping_range = cur_live_back_indices;
                }

                // Compare cur and prev overlapping views to detect any events

                let mut event_detected: Option<BackendEvent> = None;
                // View can only change by 1 addition or 1 removal of server.
                // Since both crash and leave cannot happen within 30 seconds, if length the same,
                // means that no events occured
                if cur_live_indices_in_overlapping_range.len()
                    > prev_live_indices_in_overlapping_range.len()
                {
                    // Join event case

                    // Make hash set of prev.
                    let prev_live_set: HashSet<usize> =
                        prev_live_indices_in_overlapping_range.into_iter().collect();
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
                } else if cur_live_indices_in_overlapping_range.len()
                    < prev_live_indices_in_overlapping_range.len()
                {
                    // Crash event case

                    // Make hash set of cur.
                    let cur_live_set: HashSet<usize> =
                        cur_live_indices_in_overlapping_range.into_iter().collect();
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

                // If detected an event for predecessor, save it in case predecessor dies
                if let Some(event) = event_detected {
                    let mut predecessor_event_detected_lock =
                        predecessor_event_detected.write().await;
                    *predecessor_event_detected_lock = Some(event);
                    drop(predecessor_event_detected_lock);
                }
            }

            // Wait interval til next scan
            tokio::time::sleep(KEEPER_SCAN_AND_SYNC_BACKEND_INTERVAL).await;
        }

        Ok(())
    }

    // Performs scan on clients in range and returns live_backend_indexes from that range as well as the max clock
    // Range_to_scan (start_idx, end_idx) may have start_idx > end_idx, in which case we wrap around
    // Assume storage_clients are client vec for all backends
    // Returns (live_back_indices_in_range, max_clock_received)
    async fn single_scan_and_sync(
        storage_clients: Vec<Arc<StorageClient>>,
        range_to_scan: (usize, usize),
        global_max_clock: u64,
    ) -> TribResult<(Vec<usize>, u64)> {
        let mut global_max_clock = global_max_clock;
        // Only operate on those in range
        let (scan_range_start, scan_range_end) = range_to_scan;
        let mut storage_clients_in_range = vec![];

        // Rertrieve the clients that need to be scanned
        let mut cur_back_idx = scan_range_start;
        loop {
            storage_clients_in_range.push(Arc::clone(&storage_clients[cur_back_idx]));
            // Break once idx at scan_range_end has been processed
            if cur_back_idx == scan_range_end {
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
                Ok(clock_val) => {
                    global_max_clock = std::cmp::max(global_max_clock, clock_val);
                    cur_live_back_indices.push(cur_back_idx);
                }
                Err(_) => (),
            }
            // Wrap around
            cur_back_idx = (cur_back_idx + 1) % storage_clients.len();
        }

        Ok((cur_live_back_indices, global_max_clock))
    }

    // Range_to_scan (start_idx, end_idx) may have start_idx > end_idx, in which case we wrap around
    async fn first_scan_for_initialization(
        live_backends_view_arc: Arc<RwLock<LiveBackendsView>>,
        storage_clients: Vec<Arc<StorageClient>>,
        range_to_scan: (usize, usize),
    ) -> TribResult<Vec<usize>> {
        let storage_clients_clones = storage_clients.clone();
        let (cur_live_back_indices, _) =
            Self::single_scan_and_sync(storage_clients_clones, range_to_scan, 0).await?;

        let new_live_backends_view = LiveBackendsView {
            monitoring_range_inclusive: Some(range_to_scan),
            live_backend_indices_in_range: cur_live_back_indices.clone(),
        };

        // Update live backends view
        let mut live_backends_view = live_backends_view_arc.write().await;
        *live_backends_view = new_live_backends_view;
        drop(live_backends_view);

        Ok(cur_live_back_indices)
    }

    pub async fn start_task(
        http_back_addrs: Vec<String>,
        storage_clients: Vec<Arc<StorageClient>>,
        keeper_addrs: Vec<String>,
        this: usize,
        ready_sender_opt: Arc<Mutex<Option<Sender<bool>>>>,
        should_shutdown: Arc<RwLock<bool>>,
        live_backends_view: Arc<RwLock<LiveBackendsView>>,
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        predecessor_live_backends_view: Arc<RwLock<LiveBackendsView>>,
        predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        predecessor_event_detected: Arc<RwLock<Option<BackendEvent>>>,
        ack_to_predecessor_time: Arc<RwLock<Option<Instant>>>,
        event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
        event_detected_by_this: Arc<RwLock<Option<BackendEvent>>>,
        event_handling_mutex: Arc<Mutex<u64>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        end_positions: Arc<RwLock<Vec<u64>>>, // keeper end positions on the ring
        keeper_clock: Arc<RwLock<u64>>,       // keeper_clock of this keeper
        log_entries: Arc<RwLock<Vec<LogEntry>>>, // store the keys of finsihed lists to help migration
        initializing: Arc<RwLock<bool>>,         // if this keeper is initializing
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>, // keeper connections
        join_handles_to_abort: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> TribResult<()> {
        Self::initialization(
            keeper_client_opts.clone(),
            keeper_addrs.clone(),
            keeper_clock.clone(),
            statuses.clone(),
            http_back_addrs.clone(),
            end_positions.clone(),
            this.clone(),
            latest_monitoring_range_inclusive.clone(),
            predecessor_monitoring_range_inclusive.clone(),
            event_acked_by_successor.clone(),
            initializing.clone(),
            live_backends_view.clone(),
            storage_clients.clone(),
        )
        .await?;

        // Send ready signal
        let ready_sender_opt = ready_sender_opt.lock().await;
        // Send ready
        if let Some(ready_sender) = &*ready_sender_opt {
            ready_sender.send(true)?;
        }
        drop(ready_sender_opt);

        // TESTING
        let range = latest_monitoring_range_inclusive.write().await;
        println!("Our range is: {:?} and our index is: {:?}", range, this);
        drop(range);
        let pre_range = predecessor_monitoring_range_inclusive.read().await;
        println!("Pre range is: {:?} and our index is: {:?}", pre_range, this);
        drop(pre_range);

        let live_backends_view_clone = live_backends_view.clone();
        let storage_clients_clone = storage_clients.clone();
        let latest_monitoring_range_inclusive_clone = latest_monitoring_range_inclusive.clone();
        let should_shutdown_clone = should_shutdown.clone();
        let keeper_clock_clone = keeper_clock.clone();
        let ack_to_predecessor_time_clone = ack_to_predecessor_time.clone();
        let event_acked_by_successor_clone = event_acked_by_successor.clone();
        let event_detected_by_this_clone = event_detected_by_this.clone();
        let event_handling_mutex_clone = event_handling_mutex.clone();
        let statuses_clone = statuses.clone();
        let keeper_addrs_clone = keeper_addrs.clone();
        let keeper_client_opts_clone = keeper_client_opts.clone();

        let handle1 = tokio::spawn(async move {
            let res = Self::periodic_scan_and_sync_backend(
                live_backends_view_clone,
                storage_clients_clone,
                latest_monitoring_range_inclusive_clone,
                should_shutdown_clone,
                keeper_clock_clone,
                ack_to_predecessor_time_clone,
                event_acked_by_successor_clone,
                event_detected_by_this_clone,
                event_handling_mutex_clone,
                statuses_clone,
                keeper_addrs_clone,
                keeper_client_opts_clone,
                this,
            )
            .await;
            if let Err(e) = res {
                println!("Error periodic scan: {}", e);
            }
            ()
        });

        let storage_clients_clone = storage_clients.clone();
        let keeper_addrs_clone = keeper_addrs.clone();
        let http_back_addrs_clone = http_back_addrs.clone();
        let this_clone = this.clone();
        let keeper_clock_clone = keeper_clock.clone();
        let latest_monitoring_range_inclusive_clone = latest_monitoring_range_inclusive.clone();
        let predecessor_monitoring_range_inclusive_clone =
            predecessor_monitoring_range_inclusive.clone();
        let end_positions_clone = end_positions.clone();
        let keeper_client_opts_clone = keeper_client_opts.clone();
        let statuses_clone = statuses.clone();
        let predecessor_event_detected_clone = predecessor_event_detected.clone();
        let log_entries_clone = log_entries.clone();

        let handle2 = tokio::spawn(async move {
            let res = Self::monitor(
                keeper_addrs_clone,
                http_back_addrs_clone,
                this_clone,
                keeper_clock_clone,
                latest_monitoring_range_inclusive_clone,
                predecessor_monitoring_range_inclusive_clone,
                end_positions_clone,
                keeper_client_opts_clone,
                statuses_clone,
                predecessor_event_detected_clone,
                storage_clients_clone,
                log_entries_clone,
            )
            .await;
            if let Err(e) = res {
                println!("Error monitor: {}", e);
            }
            ()
        });

        let handle3 = tokio::spawn(async move {
            let res = Self::predecessor_range_periodic_scan_backend(
                predecessor_live_backends_view,
                storage_clients,
                predecessor_monitoring_range_inclusive,
                should_shutdown,
                predecessor_event_detected,
            )
            .await;

            if let Err(e) = res {
                println!("Error predecessor scan: {}", e);
            }
            ()
        });

        let mut join_handles_to_abort_lock = join_handles_to_abort.lock().await;
        (*join_handles_to_abort_lock).push(handle1);
        (*join_handles_to_abort_lock).push(handle2);
        (*join_handles_to_abort_lock).push(handle3);
        drop(join_handles_to_abort_lock);
        Ok(())
    }

    pub fn start(
        &self,
        join_handles_to_abort: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> TribResult<()> {
        let http_back_addrs_clone = self.http_back_addrs.clone();
        let storage_clients_clone = self.storage_clients.clone();
        let keeper_addrs_clone = self.keeper_addrs.clone();
        let this_clone = self.this.clone();
        let ready_sender_opt_clone = self.ready_sender_opt.clone();
        let should_shutdown_clone = self.should_shutdown.clone();
        let live_backends_view_clone = self.live_backends_view.clone();
        let latest_monitoring_range_inclusive_clone =
            self.latest_monitoring_range_inclusive.clone();
        let predecessor_live_backends_clone = self.predecessor_live_backends_view.clone();
        let predecessor_monitoring_range_inclusive_clone =
            self.predecessor_monitoring_range_inclusive.clone();
        let predecessor_event_detected_clone = self.predecessor_event_detected.clone();
        let ack_to_predecessor_time_clone = self.ack_to_predecessor_time.clone();
        let event_acked_by_successor_clone = self.event_acked_by_successor.clone();
        let event_detected_by_this_clone = self.event_detected_by_this.clone();
        let event_handling_mutex_clone = self.event_handling_mutex.clone();
        let statuses_clone = self.statuses.clone();
        let end_positions_clone = self.end_positions.clone();
        let keeper_clock_clone = self.keeper_clock.clone();
        let log_entries_clone = self.log_entries.clone();
        let initializing_clone = self.initializing.clone();
        let keeper_client_opts_clone = self.keeper_client_opts.clone();

        let join_handles_to_abort_clone = join_handles_to_abort.clone();

        tokio::spawn(async move {
            Self::start_task(
                http_back_addrs_clone,
                storage_clients_clone,
                keeper_addrs_clone,
                this_clone,
                ready_sender_opt_clone,
                should_shutdown_clone,
                live_backends_view_clone,
                latest_monitoring_range_inclusive_clone,
                predecessor_live_backends_clone,
                predecessor_monitoring_range_inclusive_clone,
                predecessor_event_detected_clone,
                ack_to_predecessor_time_clone,
                event_acked_by_successor_clone,
                event_detected_by_this_clone,
                event_handling_mutex_clone,
                statuses_clone,
                end_positions_clone,
                keeper_clock_clone,
                log_entries_clone,
                initializing_clone,
                keeper_client_opts_clone,
                join_handles_to_abort_clone,
            )
            .await
        });

        // Pseudocode steps
        // server start(handles_to_abort_on_shutdown: Arc<Vec<JoinHandle>>):
        //     spawn initialization , remember first scan in there
        //     intiialization will send ready signal and call monitor and spawn periodic_scan at end
        //     push join handles into handles_to_abort_on_shutdown
        //     return

        // in_outer_serve_keeper:
        //     create server object
        //     extract kc.config.http_addr [this] and make to_socket_addr to get address to server later
        //     handles_to_abort_on_shutdown = Arc::new(vec![])
        //     call start().await
        //     wrap server object
        //     serve_with_shutdown with shutdown future aborting join_handles_to_abort

        // // TODO Push to tasks_spawned_handles
        // // Maybe not needed for this function since shutdown guard is checked in there.
        // let saved_tasks_spawned_handles_lock = self.saved_tasks_spawned_handles.lock().await;
        // (*saved_tasks_spawned_handles_lock).push(periodic_scan_and_sync_handle);

        Ok(())
    }

    //----------------- "Client" code---------------

    // connect client if not already connected
    pub async fn connect(
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        keeper_addrs: Vec<String>,
        idx: usize,
    ) -> TribResult<()> {
        // connect if not already connected
        let mut keeper_client_opts = Arc::clone(&keeper_client_opts).lock_owned().await;
        // To prevent unnecessary reconnection, we only connect if we haven't.

        if let None = keeper_client_opts[idx] {
            let keeper_addr = &keeper_addrs[idx];
            keeper_client_opts[idx] = Some(KeeperRpcClient::connect(keeper_addr.clone()).await?);
        }
        drop(keeper_client_opts);
        Ok(())
    }

    // send clock to other keepers to maintain the keeper view
    pub async fn send_clock(
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        keeper_addrs: Vec<String>,
        keeper_clock: Arc<RwLock<u64>>,
        idx: usize,
        initializing: bool,
        step: u64,
    ) -> TribResult<Acknowledgement> {
        Self::connect(Arc::clone(&keeper_client_opts), keeper_addrs, idx).await?;
        let keeper_client_opts = Arc::clone(&keeper_client_opts).lock_owned().await;
        let client_opt = &keeper_client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(keeper_client_opts); // client_opt needs the lock

        let keeper_clock_lock = keeper_clock.read().await;
        let keeper_clock = keeper_clock_lock.clone();
        drop(keeper_clock_lock);
        let acknowledgement = client
            .send_clock(Clock {
                timestamp: keeper_clock,
                idx: idx as u64,
                initializing,
                step,
            })
            .await?;
        Ok(acknowledgement.into_inner())
    }

    // send keys of finished lists
    pub async fn send_key(
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        keeper_addrs: Vec<String>,
        idx: usize,
        key: String,
        key_type: String,
    ) -> TribResult<bool> {
        Self::connect(Arc::clone(&keeper_client_opts), keeper_addrs, idx).await?;
        let keeper_client_opts = Arc::clone(&keeper_client_opts).lock_owned().await;
        let client_opt = &keeper_client_opts[idx];

        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match client_opt {
            // RPC client is clonable and works fine with concurrent clients.
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };
        drop(keeper_client_opts); // client_opt needs the lock
        client.send_key(Key { key, key_type }).await?;
        Ok(true)
    }

    pub async fn update_ranges(
        keeper_addrs: Vec<String>,
        this: usize,
        http_back_addrs: Vec<String>,
        end_positions: Arc<RwLock<Vec<u64>>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
    ) -> TribResult<()> {
        let back_num = http_back_addrs.len();

        let end_positions_lock = end_positions.read().await;
        let end_positions = end_positions_lock.clone();
        drop(end_positions_lock);

        println!(
            "[DEBUGGING] update_ranges: keeper {}'s end_positions are {:?}",
            this, end_positions
        );

        let statuses_lock = statuses.read().await;
        let statuses = statuses_lock.clone();
        drop(statuses_lock);

        println!(
            "[DEBUGGING] update_ranges: keeper {}'s statuses are {:?}",
            this, statuses
        );

        // get end positions of alive keepers
        let mut alive_vector = Vec::<u64>::new();
        for idx in 0..keeper_addrs.len() {
            if statuses[idx] {
                alive_vector.push(end_positions[idx]);
            }
        }
        // get the range
        let mut predecessor_monitoring_range_inclusive =
            predecessor_monitoring_range_inclusive.write().await;
        let mut latest_monitoring_range_inclusive = latest_monitoring_range_inclusive.write().await;
        let alive_num = alive_vector.len() as i32;
        if alive_num == 1 {
            *predecessor_monitoring_range_inclusive = None;
            *latest_monitoring_range_inclusive = Some((0, back_num - 1 as usize));
        } else {
            for idx in 0..alive_num {
                if alive_vector[idx as usize] == end_positions[this] {
                    let start_idx = ((idx as i32 - 1) + alive_num) % alive_num;
                    let pre_start_idx = ((idx as i32 - 2) + alive_num) % alive_num;
                    let start_position = (alive_vector[start_idx as usize] + 1) % MAX_BACKEND_NUM;
                    let end_position = end_positions[this];
                    if start_position >= back_num as u64
                        && end_position >= back_num as u64
                        && start_position <= end_position
                    {
                        *latest_monitoring_range_inclusive = None;
                    } else if start_position >= back_num as u64 {
                        *latest_monitoring_range_inclusive = Some((0, end_position as usize));
                    } else if end_position >= back_num as u64 {
                        *latest_monitoring_range_inclusive =
                            Some((start_position as usize, back_num - 1));
                    }

                    let pre_start_position =
                        (alive_vector[pre_start_idx as usize] + 1) % MAX_BACKEND_NUM;
                    let pre_end_position = (start_position + MAX_BACKEND_NUM - 1) % MAX_BACKEND_NUM;
                    if pre_start_position >= back_num as u64
                        && pre_end_position >= back_num as u64
                        && pre_start_position <= pre_end_position
                    {
                        *predecessor_monitoring_range_inclusive = None
                    } else if pre_start_position >= back_num as u64 {
                        *predecessor_monitoring_range_inclusive =
                            Some((0, pre_end_position as usize));
                    } else if pre_end_position >= back_num as u64 {
                        *predecessor_monitoring_range_inclusive =
                            Some((pre_start_position as usize, back_num - 1));
                    }
                }
            }
        }
        drop(latest_monitoring_range_inclusive);
        drop(predecessor_monitoring_range_inclusive);
        Ok(())
    }

    pub async fn initialization(
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        keeper_addrs: Vec<String>,
        keeper_clock: Arc<RwLock<u64>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        http_back_addrs: Vec<String>,
        end_positions: Arc<RwLock<Vec<u64>>>,
        this: usize,
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        event_acked_by_successor: Arc<RwLock<Option<BackendEvent>>>,
        initializing: Arc<RwLock<bool>>,
        live_backends_view_arc: Arc<RwLock<LiveBackendsView>>,
        storage_clients: Vec<Arc<StorageClient>>,
    ) -> TribResult<bool> {
        let keeper_num = keeper_addrs.len();
        let mut normal_join = false; // if this is a normal join operation
        let start_time = Instant::now();
        let mut current_time = Instant::now();

        // detect other keepers for 5 seconds
        while current_time.duration_since(start_time) < Duration::new(5, 0) {
            for idx in 0..keeper_num {
                // only contact other keepers
                if idx == this {
                    continue;
                }
                // check acknowledgement
                let acknowledgement = Self::send_clock(
                    Arc::clone(&keeper_client_opts),
                    keeper_addrs.clone(),
                    Arc::clone(&keeper_clock),
                    idx,
                    true,
                    1,
                )
                .await; // step 1
                let mut statuses = statuses.write().await;
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

        // update the range
        let _update_result = Self::update_ranges(
            keeper_addrs.clone(),
            this,
            http_back_addrs,
            end_positions,
            Arc::clone(&statuses),
            latest_monitoring_range_inclusive.clone(),
            predecessor_monitoring_range_inclusive,
        )
        .await;

        // Do first scan (if range is not None)
        let latest_monitoring_range_inclusive_lock = latest_monitoring_range_inclusive.read().await;
        let latest_range = latest_monitoring_range_inclusive_lock.clone();
        drop(latest_monitoring_range_inclusive_lock);
        if let Some(range_to_scan) = latest_range {
            let _ = Self::first_scan_for_initialization(
                live_backends_view_arc,
                storage_clients,
                range_to_scan,
            )
            .await?;
        }
        tokio::time::sleep(Duration::from_secs(4)).await; // sleep after the first scan

        if normal_join {
            // find the successor
            let statuses_lock = statuses.read().await;
            let statuses = statuses_lock.clone();
            drop(statuses_lock);
            let successor_index = Self::find_successor_index(statuses, this, keeper_num);

            // found a successor => get the acknowledged event
            if successor_index != this {
                let acknowledgement = Self::send_clock(
                    keeper_client_opts,
                    keeper_addrs,
                    keeper_clock,
                    successor_index,
                    true,
                    2,
                )
                .await; // step 2
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
                                event_acked_by_successor.write().await;

                            *event_acked_by_successor = Some(backend_event);
                            drop(event_acked_by_successor);
                        }
                        // no need to store anything if the event is "None"
                    }
                    // Since this keeper just join, it's not possible that it's successor dies now.
                    Err(_) => (),
                }
            }
        }
        // finish initialization
        let mut initializing = initializing.write().await;
        *initializing = false;
        drop(initializing);
        Ok(true) // can send the ready signal
    }

    pub fn find_successor_index(statuses: Vec<bool>, this: usize, keeper_num: usize) -> usize {
        let mut successor_index = this;
        for idx in this + 1..keeper_num {
            if statuses[idx] {
                successor_index = idx;
                break;
            }
        }
        // hasn't find the successor yet
        if successor_index == this {
            for idx in 0..this {
                if statuses[idx] {
                    successor_index = idx;
                    break;
                }
            }
        }
        return successor_index;
    }

    // monitor the statuses of other keepers and update the backend ranges
    pub async fn monitor(
        keeper_addrs: Vec<String>,
        http_back_addrs: Vec<String>,
        this: usize,
        keeper_clock: Arc<RwLock<u64>>,
        latest_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        predecessor_monitoring_range_inclusive: Arc<RwLock<Option<(usize, usize)>>>,
        end_positions: Arc<RwLock<Vec<u64>>>,
        keeper_client_opts: Arc<Mutex<Vec<Option<KeeperRpcClient<tonic::transport::Channel>>>>>,
        statuses: Arc<RwLock<Vec<bool>>>,
        predecessor_event_detected: Arc<RwLock<Option<BackendEvent>>>,
        clients_for_scanning: Vec<Arc<StorageClient>>, // all backends,
        log_entries: Arc<RwLock<Vec<LogEntry>>>, // store the keys of finsihed lists to help migration
    ) -> TribResult<bool> {
        let keeper_num = keeper_addrs.len();
        loop {
            // find the index of the predecessor
            let predecessor_monitoring_range_inclusive_lock =
                predecessor_monitoring_range_inclusive.read().await;
            let predecessor_monitoring_range = predecessor_monitoring_range_inclusive_lock.clone();
            drop(predecessor_monitoring_range_inclusive_lock);
            let mut predecessor_index = None;
            match predecessor_monitoring_range {
                None => (),
                Some(predecessor_range) => {
                    let predecessor_position = predecessor_range.1;
                    let end_positions = end_positions.read().await;
                    for idx in 0..end_positions.len() {
                        if predecessor_position == end_positions[idx] as usize {
                            predecessor_index = Some(idx);
                            break;
                        }
                    }
                    drop(end_positions);
                }
            }

            for idx in 0..keeper_num {
                // only contact other keepers
                if idx == this {
                    continue;
                }
                // check acknowledgement
                let acknowledgement = Self::send_clock(
                    Arc::clone(&keeper_client_opts),
                    keeper_addrs.clone(),
                    Arc::clone(&keeper_clock),
                    idx,
                    false,
                    0,
                )
                .await; // just a normal clock heartbeat
                let mut statuses = statuses.write().await;
                match acknowledgement {
                    // alive
                    Ok(acknowledgement) => {
                        // When a keeper is initializing, we don't update its status.
                        // When its successor acknowledge it, and it finishes initialization, we will update its status.
                        if !acknowledgement.initializing {
                            statuses[idx] = true; // maintain the keeper statuses
                        }
                    }
                    // down
                    Err(_) => {
                        statuses[idx] = false; // maintain the keeper statuses
                        match predecessor_index {
                            None => (),
                            Some(predecessor_index) => {
                                if predecessor_index == idx {
                                    // call the take over function

                                    let mut successor_keeper_client: Option<
                                        KeeperRpcClient<tonic::transport::Channel>,
                                    > = None;

                                    let found_index = Self::find_successor_index(
                                        statuses.clone(),
                                        this,
                                        keeper_addrs.len(),
                                    );
                                    if found_index != this {
                                        let successor_index = found_index;

                                        Self::connect(
                                            Arc::clone(&keeper_client_opts),
                                            keeper_addrs.clone(),
                                            successor_index,
                                        )
                                        .await?;
                                        let keeper_client_opts =
                                            Arc::clone(&keeper_client_opts).lock_owned().await;
                                        successor_keeper_client =
                                            match &keeper_client_opts[successor_index] {
                                                Some(keeper_client) => Some(keeper_client.clone()),
                                                None => None,
                                            };
                                    }

                                    Self::take_over_pred_event_handling_if_needed(
                                        predecessor_event_detected.clone(),
                                        clients_for_scanning.clone(),
                                        log_entries.clone(),
                                        successor_keeper_client.clone(),
                                        keeper_clock.clone(),
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
                drop(statuses);
            }
            let _update_result = Self::update_ranges(
                keeper_addrs.clone(),
                this,
                http_back_addrs.clone(),
                Arc::clone(&end_positions),
                Arc::clone(&statuses),
                Arc::clone(&latest_monitoring_range_inclusive),
                Arc::clone(&predecessor_monitoring_range_inclusive),
            )
            .await; // update the range
            tokio::time::sleep(Duration::from_secs(1)).await; // sleep for 1 second
        }
    }

    async fn take_over_pred_event_handling_if_needed(
        predecessor_event_detected: Arc<RwLock<Option<BackendEvent>>>,
        clients_for_scanning: Vec<Arc<StorageClient>>, // all backend's clients
        log_entries: Arc<RwLock<Vec<LogEntry>>>,
        successor_keeper_client: Option<KeeperRpcClient<tonic::transport::Channel>>,
        keeper_clock: Arc<RwLock<u64>>,
    ) -> TribResult<()> {
        let pred_event_lock = predecessor_event_detected.write().await;
        let pred_event = pred_event_lock.clone();
        drop(pred_event_lock);

        // If no pred event or not a recent event, return
        let back_ev = match pred_event {
            None => return Ok(()),
            Some(back_ev) => {
                if Instant::now().saturating_duration_since(back_ev.timestamp)
                    >= Duration::from_secs(20)
                {
                    return Ok(());
                }
                back_ev
            }
        };

        // Clone clients since later moving into async for tokio spawn async execution.
        let clients_for_scanning_clones = clients_for_scanning.clone();

        // Scan range range
        let all_backends_range = (0, clients_for_scanning.len() - 1);
        let (all_live_back_indices, _) =
            Self::single_scan_and_sync(clients_for_scanning_clones, all_backends_range, 0).await?;

        let mut done_keys: HashSet<DoneEntry> = HashSet::new();

        let now = Instant::now();
        // Extract recent entries
        let mut logs_done_lock = log_entries.write().await;
        for log_entry in (*logs_done_lock).iter() {
            if now.saturating_duration_since(log_entry.timestamp) < Duration::from_secs(20) {
                done_keys.insert(DoneEntry {
                    key: log_entry.key.clone(),
                    key_type: log_entry.key_type.clone(),
                });
            }
        }
        // Reset to empty vec
        *logs_done_lock = vec![];

        drop(logs_done_lock);

        let keeper_clock_lock = keeper_clock.read().await;
        let last_keeper_clock = *keeper_clock_lock;
        drop(keeper_clock_lock);

        println!("\n[DEBUGGING] ----------Take over migration started!-----------\n");
        println!("event is: {:?}", &back_ev);
        println!("live_https is: {:?}", &all_live_back_indices);
        println!("done_keys is: {:?}", &done_keys);

        // TODO Call migration using storage_client_clones, all_live_back_indices, last_keeper_clock, successor_keeper_client, back_ev
        match migration_event(
            &back_ev,
            all_live_back_indices,
            clients_for_scanning.clone(),
            Some(done_keys),
            last_keeper_clock,
            successor_keeper_client,
        )
        .await
        {
            Ok(_) => (),
            Err(_) => return Err("Migration err".into()),
        }

        Ok(())
    }
}

#[async_trait]
impl keeper::keeper_rpc_server::KeeperRpc for KeeperServer {
    async fn send_clock(
        &self,
        request: tonic::Request<Clock>,
    ) -> Result<tonic::Response<Acknowledgement>, tonic::Status> {
        let received_request = request.into_inner();

        // store the timestamp from another keeper and use it to sync later
        let keeper_clock_lock = self.keeper_clock.read().await;
        let keeper_clock = keeper_clock_lock.clone();
        drop(keeper_clock_lock);

        if received_request.timestamp > keeper_clock {
            let mut keeper_clock = self.keeper_clock.write().await;
            *keeper_clock = cmp::max(*keeper_clock, received_request.timestamp);
            drop(keeper_clock);
        }

        // 1) just a normal clock heartbeat
        // 2) just checking if this keeper is initialziing (step 1 of initialization)
        if !received_request.initializing || received_request.step == 1 {
            let initializing_lock = self.initializing.read().await;
            let initializing = initializing_lock.clone();
            drop(initializing_lock);
            return Ok(Response::new(Acknowledgement {
                event_type: "None".to_string(),
                back_idx: 0,
                initializing,
            }));
        }

        // step 2: join after knowing other keepers are not initializing
        let guard = self.event_handling_mutex.lock();

        let event_detected_by_this_lock = self.event_detected_by_this.read().await;
        let event_detected_by_this = event_detected_by_this_lock.as_ref().unwrap().clone();
        drop(event_detected_by_this_lock);

        let current_time = Instant::now();
        let mut return_event = "None".to_string();
        let mut back_idx = 0;
        let detect_time = event_detected_by_this.timestamp;
        if current_time.duration_since(detect_time) < Duration::new(10, 0) {
            let mut ack_to_predecessor_time = self.ack_to_predecessor_time.write().await;
            *ack_to_predecessor_time = Some(Instant::now());
            drop(ack_to_predecessor_time);
            match event_detected_by_this.event_type {
                BackendEventType::None => {
                    return_event = "None".to_string();
                }
                BackendEventType::Join => {
                    return_event = "Join".to_string();
                    back_idx = event_detected_by_this.back_idx;
                }
                BackendEventType::Leave => {
                    return_event = "Leave".to_string();
                    back_idx = event_detected_by_this.back_idx;
                }
            }
        }
        let initializing = self.initializing.read().await;
        let return_initializing = initializing.clone();
        drop(initializing);

        // change the state of the predecessor
        let mut statuses = self.statuses.write().await;
        statuses[received_request.idx as usize] = true;
        drop(statuses);

        // update the range
        let _result = Self::update_ranges(
            self.keeper_addrs.clone(),
            self.this.clone(),
            self.http_back_addrs.clone(),
            Arc::clone(&self.end_positions),
            Arc::clone(&self.statuses),
            Arc::clone(&self.latest_monitoring_range_inclusive),
            Arc::clone(&self.predecessor_monitoring_range_inclusive),
        )
        .await;

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
        let mut log_entries = self.log_entries.write().await;
        log_entries.push(LogEntry {
            key: received_key.key,
            key_type: received_key.key_type,
            timestamp: Instant::now(),
        });

        // The log entry is received and pushed.
        return Ok(Response::new(Bool { value: true }));
    }
}

// When a keeper joins, it needs to know if it's at the starting phase.
// 1) If it finds a working keeper => normal join
// 2) else => starting phase
