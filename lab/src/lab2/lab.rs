use crate::keeper::keeper_rpc_server::KeeperRpcServer;
use crate::lab2::{bin_client::BinClient, keeper_server::KeeperServer, trib_front::TribFront};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tribbler::{config::KeeperConfig, err::TribResult, storage::BinStorage, trib::Server};

/// This function accepts a list of backend addresses, and returns a
/// type which should implement the [BinStorage] trait to access the
/// underlying storage system.
#[allow(unused_variables)]
pub async fn new_bin_client(backs: Vec<String>) -> TribResult<Box<dyn BinStorage>> {
    Ok(Box::new(BinClient::new(backs).await?))
}

/// this async function accepts a [KeeperConfig] that should be used to start
/// a new keeper server on the address given in the config.
///
/// This function should block indefinitely and only return upon erroring. Make
/// sure to send the proper signal to the channel in `kc` when the keeper has
/// started.
#[allow(unused_variables)]
pub async fn serve_keeper(kc: KeeperConfig) -> TribResult<()> {
    // DEBUGGING TODO
    let back_addrs = kc.backs.clone();
    println!("[DEBUG] lab2 serve_keeper: Config is: {:?}", kc);

    // Parse socket address using apt keeper addr
    let mut addr_iter = match kc.addrs[kc.this].clone().to_socket_addrs() {
        Ok(r) => r,
        Err(_) => {
            return Err("serve_keeper: Unable to parse address!".into());
        }
    };

    let parsed_sock_addr = match addr_iter.next() {
        Some(parsed_sock_addr) => parsed_sock_addr,
        None => {
            return Err("Serve_keeper: Unable to extract address despite sucessful parse!".into())
        }
    };

    let should_shutdown = Arc::new(RwLock::new(false));

    let mut kc = kc;
    let shutdown = kc.shutdown.take();
    // Our server objects
    let keeper_server = KeeperServer::new(kc, Arc::clone(&should_shutdown)).await?;

    // TODO
    let join_handles_to_abort: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>> =
        Arc::new(Mutex::new(vec![]));

    // Start keeper processes
    if let Err(e) = keeper_server.start(Arc::clone(&join_handles_to_abort)) {
        println!("Error starting keeper: {}", e);
    }

    // RPC servers wrapping our server objects
    let keeper_rpc_server = KeeperRpcServer::new(keeper_server);
    let server_router = tonic::transport::Server::builder().add_service(keeper_rpc_server);

    // Serve function will be different depending on whether shutdown channel exists
    if let Some(mut shutdown_channel_receiver) = shutdown {
        // Future with unit type (i.e. the '()' type) return. Need a impl Future<Output = ()> type for serve_with_shutdown
        let shutdown_future = async {
            // Currently always shut down (resolve this async block) when recv() await returns regardless of either 2 cases below:
            // 1. If Some(()) received, then Received "()" shutdown indication.
            // 2. If None received means All senders have dropped.
            // If need to continue serving for the None case (or when all senders have dropped) can do something like below:
            // Example of using core::future::pending() for a future that never resolves to prevent current async block from
            // resolving and prevent shutdown:
            //     let future = core::future::pending();
            //     let res: i32 = future.await;
            match shutdown_channel_receiver.recv().await {
                // Continue execution to resolve current async block future.
                _ => (),
            };
            // Gracefully close
            shutdown_channel_receiver.close();

            let mut should_shutdown_guard = should_shutdown.write().await;
            *should_shutdown_guard = true;

            let join_handles_to_abort_vec = join_handles_to_abort.lock().await;
            // Abort background tasks
            for handle in &*join_handles_to_abort_vec {
                handle.abort();
            }
            drop(join_handles_to_abort_vec);

            () // Explicitly return () for clarity of Future output type
        };

        match server_router
            .serve_with_shutdown(parsed_sock_addr, shutdown_future)
            .await
        {
            Ok(_) => (),
            Err(e) => println!("[DEBUG] Serve_keeper: ERROR is {:?}", e),
        }
    } else {
        // Without shutdown channel
        server_router.serve(parsed_sock_addr).await?
    }

    Ok(())

    // let mut keeper = KeeperServer::new(kc).await?;
    // match keeper.serve().await {
    //     Ok(_) => Ok(()),
    //     Err(e) => {
    //         println!("[DEBUG] lab2 serve_keeper error: {}", e.to_string());
    //         Err(e)
    //     }
    // }

    // Ok(keeper.serve().await?)
}

/// this function accepts a [BinStorage] client which should be used in order to
/// implement the [Server] trait.
///
/// You'll need to translate calls from the tribbler front-end into storage
/// calls using the [BinStorage] interface.
///
/// Additionally, two trait bounds [Send] and [Sync] are required of your
/// implementation. This should guarantee your front-end is safe to use in the
/// tribbler front-end service launched by the`trib-front` command
#[allow(unused_variables)]
pub async fn new_front(
    bin_storage: Box<dyn BinStorage>,
) -> TribResult<Box<dyn Server + Send + Sync>> {
    Ok(Box::new(TribFront::new(bin_storage)))
}
