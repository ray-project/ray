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
    pub async fn push(&self, req: rpc::PushRequest) -> Result<rpc::PushReply, Status> {
        impl_om_rpc!(self, push, req)
    }

    /// Pull objects from this node — request them to be sent to us.
    pub async fn pull(&self, req: rpc::PullRequest) -> Result<rpc::PullReply, Status> {
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
        let client = ObjectManagerClient::connect_lazy(address, self.retry_config.clone());
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
        let client =
            ObjectManagerClient::connect_lazy("http://127.0.0.1:50001", RetryConfig::default());
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

    #[tokio::test]
    async fn test_from_channel() {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let client = ObjectManagerClient::from_channel(
            channel,
            RetryConfig::default(),
            "http://[::1]:1".to_string(),
        );
        assert_eq!(client.address(), "http://[::1]:1");
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_client_pool_remove_nonexistent() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        assert!(!pool.remove("nonexistent"));
    }

    #[tokio::test]
    async fn test_client_pool_reinsert_after_remove() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        pool.get_or_create("node1", "http://10.0.0.1:8000");
        pool.remove("node1");
        // Can re-create after removal.
        assert!(pool.get_or_create("node1", "http://10.0.0.1:8001"));
        assert_eq!(pool.num_connections(), 1);
    }

    #[tokio::test]
    async fn test_pool_num_connections_starts_at_zero() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        assert_eq!(pool.num_connections(), 0);
    }

    #[tokio::test]
    async fn test_pool_many_nodes() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        for i in 0..100 {
            let node_id = format!("node_{}", i);
            let addr = format!("http://10.0.0.{}:{}", i % 256, 8000 + i);
            assert!(pool.get_or_create(&node_id, &addr));
        }
        assert_eq!(pool.num_connections(), 100);

        // Remove half of them.
        for i in 0..50 {
            let node_id = format!("node_{}", i);
            assert!(pool.remove(&node_id));
        }
        assert_eq!(pool.num_connections(), 50);
    }

    #[tokio::test]
    async fn test_pool_get_or_create_same_node_different_address() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        // First call creates the client.
        assert!(pool.get_or_create("node1", "http://10.0.0.1:8000"));
        // Second call with a different address returns false (node already tracked).
        assert!(!pool.get_or_create("node1", "http://10.0.0.1:9999"));
        assert_eq!(pool.num_connections(), 1);
    }

    #[tokio::test]
    async fn test_pool_remove_all() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        pool.get_or_create("a", "http://10.0.0.1:8000");
        pool.get_or_create("b", "http://10.0.0.2:8000");
        pool.get_or_create("c", "http://10.0.0.3:8000");
        assert_eq!(pool.num_connections(), 3);

        pool.remove("a");
        pool.remove("b");
        pool.remove("c");
        assert_eq!(pool.num_connections(), 0);
    }

    #[tokio::test]
    async fn test_pool_concurrent_get_or_create() {
        use std::sync::Arc;

        let pool = Arc::new(ObjectManagerClientPool::new(RetryConfig::default()));
        let mut handles = Vec::new();

        // Spawn 20 tasks, each trying to create a client for one of 5 nodes.
        for i in 0..20 {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                let node_id = format!("node_{}", i % 5);
                let addr = format!("http://10.0.0.{}:8000", i % 5);
                pool.get_or_create(&node_id, &addr)
            }));
        }

        let mut created_count = 0;
        for h in handles {
            if h.await.unwrap() {
                created_count += 1;
            }
        }
        // Exactly 5 unique nodes should have been created.
        assert_eq!(pool.num_connections(), 5);
        assert_eq!(created_count, 5);
    }

    #[tokio::test]
    async fn test_pool_concurrent_remove() {
        use std::sync::Arc;

        let pool = Arc::new(ObjectManagerClientPool::new(RetryConfig::default()));
        // Pre-populate 10 nodes.
        for i in 0..10 {
            pool.get_or_create(&format!("node_{}", i), &format!("http://10.0.0.{}:8000", i));
        }
        assert_eq!(pool.num_connections(), 10);

        let mut handles = Vec::new();
        // Spawn 20 tasks trying to remove the same 10 nodes (each removed twice).
        for i in 0..20 {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                let node_id = format!("node_{}", i % 10);
                pool.remove(&node_id)
            }));
        }

        let mut removed_count = 0;
        for h in handles {
            if h.await.unwrap() {
                removed_count += 1;
            }
        }
        // Exactly 10 should succeed, the other 10 fail (already removed).
        assert_eq!(removed_count, 10);
        assert_eq!(pool.num_connections(), 0);
    }

    #[tokio::test]
    async fn test_pool_concurrent_create_and_remove() {
        use std::sync::Arc;

        let pool = Arc::new(ObjectManagerClientPool::new(RetryConfig::default()));
        pool.get_or_create("node_0", "http://10.0.0.1:8000");
        pool.get_or_create("node_1", "http://10.0.0.2:8000");

        let pool_c = Arc::clone(&pool);
        let pool_r = Arc::clone(&pool);

        // Create new nodes while simultaneously removing existing ones.
        let creator = tokio::spawn(async move {
            for i in 2..12 {
                pool_c.get_or_create(&format!("node_{}", i), &format!("http://10.0.0.{}:8000", i));
            }
        });
        let remover = tokio::spawn(async move {
            pool_r.remove("node_0");
            pool_r.remove("node_1");
        });

        creator.await.unwrap();
        remover.await.unwrap();

        // node_0 and node_1 removed, nodes 2..12 created = 10 nodes.
        assert_eq!(pool.num_connections(), 10);
    }

    #[tokio::test]
    async fn test_client_connect_lazy_various_addresses() {
        let addrs = [
            "http://127.0.0.1:8000",
            "http://[::1]:50051",
            "http://localhost:9999",
            "http://10.255.255.255:1234",
        ];
        for addr in &addrs {
            let client = ObjectManagerClient::connect_lazy(addr, RetryConfig::default());
            assert_eq!(client.address(), *addr);
            assert!(client.is_connected());
        }
    }

    #[tokio::test]
    async fn test_client_from_channel_preserves_custom_address() {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let client = ObjectManagerClient::from_channel(
            channel,
            RetryConfig::default(),
            "my-custom-label".to_string(),
        );
        assert_eq!(client.address(), "my-custom-label");
        assert!(client.is_connected());
    }

    #[tokio::test]
    async fn test_client_from_channel_custom_retry_config() {
        let channel = Channel::from_static("http://[::1]:1").connect_lazy();
        let config = RetryConfig {
            max_retries: 5,
            initial_delay: std::time::Duration::from_millis(200),
            max_delay: std::time::Duration::from_secs(30),
            multiplier: 1.5,
            server_unavailable_timeout: std::time::Duration::from_secs(90),
            max_pending_bytes: 200 * 1024 * 1024,
        };
        let client =
            ObjectManagerClient::from_channel(channel, config, "http://[::1]:1".to_string());
        assert!(client.is_connected());
        assert_eq!(client.address(), "http://[::1]:1");
    }

    #[tokio::test]
    async fn test_pool_with_custom_retry_config() {
        let config = RetryConfig {
            max_retries: 10,
            ..RetryConfig::default()
        };
        let pool = ObjectManagerClientPool::new(config);
        pool.get_or_create("node1", "http://10.0.0.1:8000");
        assert_eq!(pool.num_connections(), 1);
    }

    #[tokio::test]
    async fn test_pool_remove_returns_false_for_empty_pool() {
        let pool = ObjectManagerClientPool::new(RetryConfig::default());
        assert_eq!(pool.num_connections(), 0);
        assert!(!pool.remove("anything"));
        assert_eq!(pool.num_connections(), 0);
    }
}
