use crate::lab1::new_client;
use std::sync::{mpsc::Sender, Arc};
use tokio::sync::{mpsc::Receiver, Mutex};
use tokio::time::Duration;
use tribbler::{config::KeeperConfig, err::TribResult, storage::Storage};

// Periodically syncs backends
pub struct KeeperServer {
    /// The addresses of back-ends prefixed with "http://"" i.e. "http://<host>:<port>""
    /// HTTP2 gRPC client needs address of this form
    pub http_back_addrs: Vec<String>,
    /// The storage_clients clients to connect to each back end
    /// Each element in Vector correponds to the back_addrs at the same idx
    pub storage_clients: Vec<Arc<Box<dyn Storage>>>,
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
    /// Whether to shutdown keeper or not. Note tokio::sync:Mutex needed
    /// since bin_run requires the KeeperServer returned by serve_keeper
    /// to by Sync (for tokio spawn)
    pub should_shutdown: Arc<Mutex<bool>>,
    // Perhaps in the future can hold the latest clock seen for each backend
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
            storage_clients.push(Arc::new(new_client(http_back_addr).await?));
        }

        Ok(KeeperServer {
            http_back_addrs: http_back_addrs,
            storage_clients: storage_clients,
            keeper_addrs: kc.addrs,
            this_back_index: kc.this,
            id: kc.id,
            ready_sender_opt: kc.ready,
            shutdown_receiver_opt: kc.shutdown,
            should_shutdown: Arc::new(Mutex::new(false)),
        })
    }

    pub async fn serve(&mut self) -> TribResult<()> {
        // Listen for shut down asynchronously
        let should_shutdown_clone = Arc::clone(&self.should_shutdown);
        // takes ownership
        if let Some(shutdown_receiver) = self.shutdown_receiver_opt.take() {
            tokio::spawn(async move {
                KeeperServer::listen_for_shutdown_signal(shutdown_receiver, should_shutdown_clone)
                    .await
            });
        }

        // Send ready
        if let Some(ready_sender) = &self.ready_sender_opt {
            ready_sender.send(true)?;
        }

        // Sync backends every 1 second
        const KEEPER_BACKEND_SYNC_INTERVAL: Duration = Duration::from_millis(1000);

        // To synchronize backends. Initialize to 0
        let mut global_max_clock = 0u64;
        loop {
            // Wait 1 sec til next sync
            tokio::time::sleep(KEEPER_BACKEND_SYNC_INTERVAL).await;

            let should_shutdown_guard = self.should_shutdown.lock().await;

            // First if should_shutdown is set to true (by async listen for shutdown function), can break.
            if *should_shutdown_guard == true {
                break;
            } else {
                drop(should_shutdown_guard);
            }

            // Sync backends by retrieving the global maximum clock and calling clock with that
            // value on all backends

            // Clone clients since later moving into async for tokio spawn async execution.
            let storage_clients_clones = self.storage_clients.clone();

            let tasks: Vec<_> = storage_clients_clones
                .into_iter()
                .map(|storage_client| {
                    // Note deliberately NOT adding ";" to the async function as well as the
                    // spawn statements since they are used as expression return results
                    // Calling clock with largest seen so faar
                    tokio::spawn(async move { storage_client.clock(global_max_clock).await })
                })
                .collect();

            // Note chaining of "??" is needed. One is for the tokio's spawned task error
            // capturing (a Result<>) and the other is from our client clock() function which
            // is also another Result
            for task in tasks {
                // let res = task.await??;  // old version that doesnt ignore error

                // If backend is not serving then might get error. Ignore and use the one that responds only.
                match task.await? {
                    Ok(clock_val) => {
                        global_max_clock = std::cmp::max(global_max_clock, clock_val);
                    }
                    // Ignore errors, possibly because that backend address is not serving. Work with what we have only
                    Err(_) => (),
                }
            }

            println!("[DEBUGGING] keeper_server's serve: Before syncing clock(global_max_clock = {}) on backends", global_max_clock);

            // Once again clone clients since later moving into async for tokio spawn async execution.
            // Clone clients since later moving into async for tokio spawn async execution.
            let storage_clients_clones = self.storage_clients.clone();

            // Send global max clock to all backends to sync them to latest logical time stamp
            let _: Vec<_> = storage_clients_clones
                .into_iter()
                .map(|storage_client| {
                    // Note deliberately NOT adding ";" to the async function as well as the
                    // spawn statements since they are used as expression return results
                    // Calling clock with global_max_clock to make sure everyone is coarsely synchronized
                    tokio::spawn(async move { storage_client.clock(global_max_clock).await })
                })
                .collect();

            // Don't await on handles and check result this time, since no need response for now
        }

        Ok(())
    }

    async fn listen_for_shutdown_signal(
        mut shutdown_receiver: Receiver<()>,
        should_shutdown: Arc<Mutex<bool>>,
    ) {
        shutdown_receiver.recv().await;

        println!(
            "[DEBUGGING] keeper_server's listen_for_shutdown_signal: Shutdown signal received"
        );

        // Gracefully close
        shutdown_receiver.close();

        // Indicate shut down is requested
        let mut should_shutdown_guard = should_shutdown.lock().await;
        *should_shutdown_guard = true;
    }
}
