use async_trait::async_trait;

use tribbler::{
    rpc,
    storage::{KeyValue, Pattern, Storage},
};

pub struct StorageServer {
    pub storage: Box<dyn Storage>,
}

impl StorageServer {
    // Creates new StorageServer struct with config
    pub fn new(storage: Box<dyn Storage>) -> StorageServer {
        StorageServer { storage }
    }
}

#[async_trait]
impl rpc::trib_storage_server::TribStorage for StorageServer {
    async fn get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::Value>, tonic::Status> {
        let storage = &self.storage;
        match storage.get(&request.into_inner().key).await {
            Ok(r) => match r {
                Some(ret) => Ok(tonic::Response::new(rpc::Value { value: ret })),
                None => Err(tonic::Status::not_found("Key not found in storage!")),
            },
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let key_val: rpc::KeyValue = request.into_inner();

        let storage = &self.storage;
        match storage
            .set(&KeyValue {
                key: key_val.key,
                value: key_val.value,
            })
            .await
        {
            Ok(r) => Ok(tonic::Response::new(rpc::Bool { value: r })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let pattern: rpc::Pattern = request.into_inner();

        let storage = &self.storage;
        match storage
            .keys(&Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await
        {
            Ok(storage_list) => Ok(tonic::Response::new(rpc::StringList {
                list: storage_list.0,
            })), // extract vect from tuple struct using ".0" (see struct List(pub Vec<String>) in storage.rs)
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn list_get(
        &self,
        request: tonic::Request<rpc::Key>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let storage = &self.storage;
        match storage.list_get(&request.into_inner().key).await {
            Ok(storage_list) => Ok(tonic::Response::new(rpc::StringList {
                list: storage_list.0,
            })), // extract vect from tuple struct using ".0" (see struct List(pub Vec<String>) in storage.rs)
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn list_append(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::Bool>, tonic::Status> {
        let key_val: rpc::KeyValue = request.into_inner();

        let storage = &self.storage;
        match storage
            .list_append(&KeyValue {
                key: key_val.key,
                value: key_val.value,
            })
            .await
        {
            Ok(bool_val) => Ok(tonic::Response::new(rpc::Bool { value: bool_val })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn list_remove(
        &self,
        request: tonic::Request<rpc::KeyValue>,
    ) -> Result<tonic::Response<rpc::ListRemoveResponse>, tonic::Status> {
        let key_val: rpc::KeyValue = request.into_inner();

        let storage = &self.storage;
        match storage
            .list_remove(&KeyValue {
                key: key_val.key,
                value: key_val.value,
            })
            .await
        {
            Ok(num_elems_removed) => Ok(tonic::Response::new(rpc::ListRemoveResponse {
                removed: num_elems_removed,
            })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn list_keys(
        &self,
        request: tonic::Request<rpc::Pattern>,
    ) -> Result<tonic::Response<rpc::StringList>, tonic::Status> {
        let pattern: rpc::Pattern = request.into_inner();

        let storage = &self.storage;
        match storage
            .list_keys(&Pattern {
                prefix: pattern.prefix,
                suffix: pattern.suffix,
            })
            .await
        {
            Ok(storage_list) => Ok(tonic::Response::new(rpc::StringList {
                list: storage_list.0,
            })), // extract vect from tuple struct using ".0" (see struct List(pub Vec<String>) in storage.rs)
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }

    async fn clock(
        &self,
        request: tonic::Request<rpc::Clock>,
    ) -> Result<tonic::Response<rpc::Clock>, tonic::Status> {
        let storage = &self.storage;
        match storage.clock(request.into_inner().timestamp).await {
            Ok(timestamp) => Ok(tonic::Response::new(rpc::Clock {
                timestamp: timestamp,
            })),
            Err(e) => Err(tonic::Status::internal(e.to_string())),
        }
    }
}
