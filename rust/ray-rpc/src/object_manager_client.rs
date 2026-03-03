// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! ObjectManager RPC client — typed wrapper around tonic-generated stubs.
//!
//! Replaces `src/ray/rpc/object_manager/object_manager_client.h`.
//! Provides retryable Push/Pull/FreeObjects calls for inter-node object transfer.

use parking_lot::Mutex;
use tonic::transport::Channel;
use tonic::Status;

use ray_proto::ray::rpc;

use crate::client::{RetryConfig, RetryableGrpcClient};

type StubClient = rpc::object_manager_service_client::ObjectManagerServiceClient<Channel>;

/// Macro to implement an ObjectManager RPC method with retry.
macro_rules! impl_om_rpc {
    ($self:ident, $method:ident, $req:expr) => {{
        let retry = &$self.retry_client;
        let stub_mutex = &$self.stub;
        retry
            .call_with_retry(0, None, || {
                let mut stub = stub_mutex.lock().clone();
                let req = $req.clone();
                async move {
                    stub.$method(tonic::Request::new(req))
                        .await
                        .map(|resp| resp.into_inner())
                }
            })
            .await
    }};
}

/// ObjectManager RPC client for inter-node object transfer.
pub struct ObjectManagerClient {
    retry_client: RetryableGrpcClient,
    stub: Mutex<StubClient>,
    /// Remote node address (for logging/debugging).
    address: String,
}

impl ObjectManagerClient {
    /// Connect to a remote ObjectManager service.
    pub async fn connect(
        addr: &str,
        retry_config: RetryConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = RetryableGrpcClient::connect(addr).await?;
        Ok(Self::from_channel(channel, retry_config, addr.to_string()))
    }

    /// Create from an existing channel.
    pub fn from_channel(channel: Channel, retry_config: RetryConfig, address: String) -> Self {
        let retry_client = RetryableGrpcClient::new(channel.clone(), retry_config);
        Self {
            retry_client,
            stub: Mutex::new(StubClient::new(channel)),
            address,
        }
    }

    /// Create a lazily-connecting client.
    pub fn connect_lazy(addr: &str, retry_config: RetryConfig) -> Self {
        let channel = RetryableGrpcClient::connect_lazy(addr);
        Self::from_channel(channel, retry_config, addr.to_string())
    }

    /// Get the remote address.
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Check if the client is connected.
    pub fn is_connected(&self) -> bool {
        self.retry_client.is_connected()
    }

    /// Push an object to this node.
    ///
    /// The object data is included in the request message. For very large objects,
    /// chunked streaming should be used (future enhancement).
    pub async fn push(
        &self,
        req: rpc::PushRequest,
    ) -> Result<rpc::PushReply, Status> {
        impl_om_rpc!(self, push, req)
    }

    /// Pull objects from this node — request them to be sent to us.
    pub async fn pull(
        &self,
        req: rpc::PullRequest,
    ) -> Result<rpc::PullReply, Status> {
        impl_om_rpc!(self, pull, req)
    }

    /// Free (delete) objects from this node's object store.
    pub async fn free_objects(
        &self,
        req: rpc::FreeObjectsRequest,
    ) -> Result<rpc::FreeObjectsReply, Status> {
        impl_om_rpc!(self, free_objects, req)
    }
}

/// Pool of ObjectManager clients, one per remote node.
///
/// Maintains a map of node_id → client so we reuse connections.
pub struct ObjectManagerClientPool {
    clients: Mutex<std::collections::HashMap<String, ObjectManagerClient>>,
    retry_config: RetryConfig,
}

impl ObjectManagerClientPool {
    /// Create a new client pool.
    pub fn new(retry_config: RetryConfig) -> Self {
        Self {
            clients: Mutex::new(std::collections::HashMap::new()),
            retry_config,
        }
    }

    /// Get or create a client for the given node address.
    pub fn get_or_create(&self, node_id: &str, address: &str) -> bool {
        let mut clients = self.clients.lock();
        if clients.contains_key(node_id) {
            return false; // Already exists.
        }
        let client =
            ObjectManagerClient::connect_lazy(address, self.retry_config.clone());
        clients.insert(node_id.to_string(), client);
        true
    }

    /// Remove a client (e.g., when a node dies).
    pub fn remove(&self, node_id: &str) -> bool {
        self.clients.lock().remove(node_id).is_some()
    }

    /// Number of active client connections.
    pub fn num_connections(&self) -> usize {
        self.clients.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connect_lazy_creates_client() {
        let client = ObjectManagerClient::connect_lazy(
            "http://127.0.0.1:50001",
            RetryConfig::default(),
        );
        assert_eq!(client.address(), "http://127.0.0.1:50001");
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_client_pool_get_or_create() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        assert_eq!(pool.num_connections(), 0);

        assert!(pool.get_or_create("node1", "http://10.0.0.1:8000"));
        assert_eq!(pool.num_connections(), 1);

        // Second call for same node returns false (already exists).
        assert!(!pool.get_or_create("node1", "http://10.0.0.1:8000"));
        assert_eq!(pool.num_connections(), 1);

        assert!(pool.get_or_create("node2", "http://10.0.0.2:8000"));
        assert_eq!(pool.num_connections(), 2);
    }

    #[tokio::test]
    async fn test_client_pool_remove() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        pool.get_or_create("node1", "http://10.0.0.1:8000");
        pool.get_or_create("node2", "http://10.0.0.2:8000");

        assert!(pool.remove("node1"));
        assert_eq!(pool.num_connections(), 1);

        assert!(!pool.remove("node1")); // Already removed.
        assert_eq!(pool.num_connections(), 1);
    }
}
