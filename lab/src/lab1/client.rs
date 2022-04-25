use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::Mutex;

use tribbler::{
    err::TribResult,
    rpc::{self, trib_storage_client::TribStorageClient},
    storage::{KeyList, KeyString, KeyValue, List, Pattern, Storage},
};

pub struct StorageClient {
    /// The addresses of back-ends prefixed with "http://"" i.e. "http://<host>:<port>""
    /// HTTP2 gRPC client needs address of this form
    pub http_addr: String,
    // To save connected client for reuse.
    // tokio::sync::Mutex needed for mutable borrow needed for RPC calls (see RPC signatures) and locking across await of those calls.
    // Arc wrapping is not necessary for single thread but needed for multiple threads.
    // Since the tests in lab1_test.rs are multi-threaded, to be safe use Arc wrapping. Thus, clone the client_opt arc before doing
    // reading or placing values into the option.
    // With Arc wrapping, prefer this when getting mutable borrow: let client = &mut Arc::clone(client_opt).lock_owned().await;
    // Without Arc wrapping, prefer this when getting mutable borrow: let client = &mut Arc::clone(client_opt).lock().await;
    // Option wrapping is because during initialization of server client_opt can be None until connection is performed
    // ALSO, clone the CLIENT (inside client_opt) for each call, since RPCClient<tonic::transport::Channel> is clonable and works
    // fine with conccurent requests (see https://docs.rs/tonic/latest/tonic/client/index.html and
    // https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html#multiplexing-requests)
    pub client_opt: Arc<Mutex<Option<TribStorageClient<tonic::transport::Channel>>>>,
}

impl StorageClient {
    // Creates new StorageClient struct with http_addr and a connected client
    pub fn new(http_addr: &String) -> StorageClient {
        StorageClient {
            http_addr: http_addr.clone(),
            client_opt: Arc::new(Mutex::new(None)),
        }
    }

    // Connects client if not already connected
    pub async fn connect(&self) -> TribResult<()> {
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        // Only connect if not already connected. There may be races to self.connect() so
        // this prevents unnecessary reconnection.
        if let None = *client_opt {
            *client_opt = Some(TribStorageClient::connect(self.http_addr.clone()).await?);
        }

        Ok(())
    }
}

#[async_trait] // VERY IMPORTANT !!
impl KeyString for StorageClient {
    /// Gets a value. If no value set, return [None]
    async fn get(&self, key: &str) -> TribResult<Option<String>> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        // This function handles rpc call result differntly due to relying on specific Error type for information:
        // If RPC result has error, check for key not_found error to return None instead of propagating error
        match client
            .get(rpc::Key {
                key: key.to_string(),
            })
            .await
        {
            Err(status) => {
                // Key not found is a specific error to which client should return None
                if status.code() == tonic::Code::NotFound
                    && status.message() == "Key not found in storage!".to_string()
                {
                    Ok(None) // When key is not found, return None as result
                } else {
                    Err(Box::new(status)) // Propagate all other errors appropriately
                }
            }
            Ok(r) => Ok(Some(r.into_inner().value)),
        }
    }

    /// Set kv.Key to kv.Value. return true when no error.
    async fn set(&self, kv: &KeyValue) -> TribResult<bool> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .set(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    /// List all the keys of non-empty pairs where the key matches
    /// the given pattern.
    async fn keys(&self, p: &Pattern) -> TribResult<List> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }
}

#[async_trait]
impl KeyList for StorageClient {
    /// Get the list. Empty if not set.
    async fn list_get(&self, key: &str) -> TribResult<List> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .list_get(rpc::Key {
                key: key.to_string(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }

    /// Append a string to the list. return true when no error.
    async fn list_append(&self, kv: &KeyValue) -> TribResult<bool> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .list_append(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().value)
    }

    /// Removes all elements that are equal to `kv.value` in list `kv.key`
    /// returns the number of elements removed.
    async fn list_remove(&self, kv: &KeyValue) -> TribResult<u32> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .list_remove(rpc::KeyValue {
                key: kv.key.clone(),
                value: kv.value.clone(),
            })
            .await?;
        Ok(r.into_inner().removed)
    }

    /// List all the keys of non-empty lists, where the key matches
    /// the given pattern.
    async fn list_keys(&self, p: &Pattern) -> TribResult<List> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .list_keys(rpc::Pattern {
                prefix: p.prefix.clone(),
                suffix: p.suffix.clone(),
            })
            .await?;
        Ok(List(r.into_inner().list))
    }
}

#[async_trait]
impl Storage for StorageClient {
    /// Returns an auto-incrementing clock. The returned value of each call will
    /// be unique, no smaller than `at_least`, and strictly larger than the
    /// value returned last time, unless it was [u64::MAX]
    async fn clock(&self, at_least: u64) -> TribResult<u64> {
        // client_opt is an tokio::sync::OwnedMutexGuard
        let mut client_opt = Arc::clone(&self.client_opt).lock_owned().await;

        // If not connected, initialize and connect client
        if let None = *client_opt {
            // Important: drop lock guard since connect function needs to acquire
            // There may be race conditions where 2 trait functions attempt connect but self.connect() function ensures only one does
            drop(client_opt);
            self.connect().await?;

            // Important: reassign client_opt acquiring lock
            client_opt = Arc::clone(&self.client_opt).lock_owned().await;
        };

        // At this point client should be connected
        // Get mutable client for RPC call.
        // Note the client_opt lock guard previously acquired will be held throughout this call, preventing concurrency issues.
        let mut client = match &*client_opt {
            // RPC client is clonable and works fine with concurrent clients (see TribStorageClient implementation) (essentially clones transport channel)
            Some(client) => client.clone(),
            None => return Err("Client was somehow not connected / be initialized!".into()),
        };

        // Drop early to avoid unnecessary contention No longer need lock since extracted client clone already.
        drop(client_opt);

        let r = client
            .clock(rpc::Clock {
                timestamp: at_least,
            })
            .await?;
        Ok(r.into_inner().timestamp)
    }
}
