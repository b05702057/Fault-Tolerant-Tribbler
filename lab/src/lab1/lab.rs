use crate::lab1::{client::StorageClient, server::StorageServer};
use std::net::ToSocketAddrs;
use tribbler::rpc::trib_storage_server::TribStorageServer;
use tribbler::{config::BackConfig, err::TribResult, storage::Storage};

/// an async function which blocks indefinitely until interrupted serving on
/// the host and port specified in the [BackConfig] parameter.
pub async fn serve_back(config: BackConfig) -> TribResult<()> {
    let ready_channel_opt = config.ready; // type is Opt<tokio::sync::mpsc:Receiver<()>>

    // Parse socket and send error if fails.
    // Note to_socket_addr returns iterator over the parsed values.
    let mut addr_iter = match config.addr.clone().to_socket_addrs() {
        Ok(r) => r,
        Err(e) => {
            // Send false to indicate failed setup if ready channel is available
            if let Some(ready_channel) = ready_channel_opt {
                ready_channel.send(false)?;
            }
            return Err("Serve_back: Unable to parse address!".into());
        }
    };

    let parsed_sock_addr = match addr_iter.next() {
        Some(parsed_sock_addr) => parsed_sock_addr,
        None => {
            // Send false to indicate failed setup if ready channel is available
            if let Some(ready_channel) = ready_channel_opt {
                ready_channel.send(false)?;
            }
            return Err("Serve_back: Unable to extract address despite sucessful parse!".into());
        }
    };

    // Our server objects
    let storage_server = StorageServer::new(config.storage);

    // RPC servers wrapping our server objects
    let trib_storage_server = TribStorageServer::new(storage_server);
    let server_router = tonic::transport::Server::builder().add_service(trib_storage_server);

    // Below is logic to add minor delay before sending ready signal to make sure server is serving. Not the cleanest but sufficient
    if let Some(ready_channel) = ready_channel_opt {
        tokio::task::spawn(async move {
            // move needed since ready_channel is used. no need clones bc not used anywhere else AFTER
            tokio::time::sleep(std::time::Duration::from_millis(10)).await; // delay 10 millis

            // Send ready signal. Currently need to be careful since ready signal may be received early before
            // server is actually served (client may receive ready and try to connect before server is available
            // and receiving a connection refused error, especially for tests with large I/O or prints
            match ready_channel.send(true) {
                Ok(_) => (),
                Err(e) => {
                    println!("[DEBUG] Serve_back: Error sending ready from server!");
                    // return Err(Box::new(e));  // not dealing with error here since in another task to send ready
                }
            }
        });
    }

    // Serve function will be different depending on whether shutdown channel exists
    if let Some(mut shutdown_channel_receiver) = config.shutdown {
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

            () // Explicitly return () for clarity of Future output type
        };

        match server_router
            .serve_with_shutdown(parsed_sock_addr, shutdown_future)
            .await
        {
            Ok(_) => (),
            Err(e) => println!("[DEBUG] Serve_back: ERROR is {:?}", e),
        }
    } else {
        // Without shutdown channel
        server_router.serve(parsed_sock_addr).await?
    }

    Ok(())
}

/// This function should create a new client which implements the [Storage]
/// trait. It should communicate with the backend that is started in the
/// [serve_back] function.
pub async fn new_client(addr: &str) -> TribResult<Box<dyn Storage>> {
    Ok(Box::new(StorageClient::new(&addr.to_string())))
}
