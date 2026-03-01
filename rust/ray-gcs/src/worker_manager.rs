// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Worker Manager — tracks worker processes.
//!
//! Replaces `src/ray/gcs/gcs_worker_manager.h/cc`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::pubsub_handler::{ChannelType, InternalPubSubHandler};
use crate::table_storage::GcsTableStorage;

/// Callback invoked when a worker dies unexpectedly.
pub type WorkerDeadCallback = Box<dyn Fn(&ray_proto::ray::rpc::WorkerTableData) + Send + Sync>;

/// The GCS worker manager tracks worker processes.
pub struct GcsWorkerManager {
    /// Listeners for unexpected worker deaths.
    dead_listeners: RwLock<Vec<WorkerDeadCallback>>,
    /// Counter for system error worker crashes.
    system_error_count: AtomicI64,
    /// Counter for OOM worker crashes.
    oom_count: AtomicI64,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
    /// Pubsub handler for publishing worker state changes.
    pubsub_handler: RwLock<Option<Arc<InternalPubSubHandler>>>,
}

impl GcsWorkerManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            dead_listeners: RwLock::new(Vec::new()),
            system_error_count: AtomicI64::new(0),
            oom_count: AtomicI64::new(0),
            table_storage,
            pubsub_handler: RwLock::new(None),
        }
    }

    /// Set the pubsub handler (called during server initialization).
    pub fn set_pubsub_handler(&self, handler: Arc<InternalPubSubHandler>) {
        *self.pubsub_handler.write() = Some(handler);
    }

    /// Publish worker failure via pubsub.
    fn publish_worker_failure(&self, worker_data: &ray_proto::ray::rpc::WorkerTableData) {
        let pubsub = self.pubsub_handler.read();
        if let Some(ref handler) = *pubsub {
            // WorkerDeltaData is a separate type from WorkerTableData.
            // Build the delta from the failure info.
            let addr = worker_data.worker_address.as_ref();
            let delta = ray_proto::ray::rpc::WorkerDeltaData {
                node_id: addr.map(|a| a.node_id.clone()).unwrap_or_default(),
                worker_id: addr.map(|a| a.worker_id.clone()).unwrap_or_default(),
            };
            let key_id = delta.worker_id.clone();
            let pub_msg = ray_proto::ray::rpc::PubMessage {
                channel_type: ChannelType::GcsWorkerDeltaChannel as i32,
                key_id,
                inner_message: Some(
                    ray_proto::ray::rpc::pub_message::InnerMessage::WorkerDeltaMessage(delta),
                ),
                ..Default::default()
            };
            handler.publish_pubmessage(pub_msg);
        }
    }

    /// Handle ReportWorkerFailure RPC.
    pub async fn handle_report_worker_failure(
        &self,
        worker_data: ray_proto::ray::rpc::WorkerTableData,
    ) -> Result<(), tonic::Status> {
        let key = hex::encode(
            worker_data
                .worker_address
                .as_ref()
                .map(|a| a.worker_id.clone())
                .unwrap_or_default(),
        );

        // Persist
        self.table_storage
            .worker_table()
            .put(&key, &worker_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Classify crash type
        let exit_type = worker_data.exit_type.unwrap_or(0);
        if exit_type == 4 {
            // SYSTEM_ERROR_EXIT
            self.system_error_count.fetch_add(1, Ordering::Relaxed);
        } else if exit_type == 8 {
            // NODE_OUT_OF_MEMORY
            self.oom_count.fetch_add(1, Ordering::Relaxed);
        }

        // Publish via pubsub
        self.publish_worker_failure(&worker_data);

        // Notify listeners
        let listeners = self.dead_listeners.read();
        for listener in listeners.iter() {
            listener(&worker_data);
        }

        Ok(())
    }

    /// Handle AddWorkerInfo RPC.
    pub async fn handle_add_worker_info(
        &self,
        worker_data: ray_proto::ray::rpc::WorkerTableData,
    ) -> Result<(), tonic::Status> {
        let key = hex::encode(
            worker_data
                .worker_address
                .as_ref()
                .map(|a| a.worker_id.clone())
                .unwrap_or_default(),
        );

        self.table_storage
            .worker_table()
            .put(&key, &worker_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        Ok(())
    }

    /// Handle GetAllWorkerInfo RPC.
    pub async fn handle_get_all_worker_info(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<ray_proto::ray::rpc::WorkerTableData>, tonic::Status> {
        let all = self
            .table_storage
            .worker_table()
            .get_all()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        if let Some(limit) = limit {
            Ok(all.into_values().take(limit).collect())
        } else {
            Ok(all.into_values().collect())
        }
    }

    /// Register a dead-worker listener.
    pub fn add_dead_listener(&self, callback: WorkerDeadCallback) {
        self.dead_listeners.write().push(callback);
    }

    pub fn system_error_count(&self) -> i64 {
        self.system_error_count.load(Ordering::Relaxed)
    }

    pub fn oom_count(&self) -> i64 {
        self.oom_count.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store_client::InMemoryStoreClient;

    #[tokio::test]
    async fn test_add_worker_info() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![1, 2, 3],
                ..Default::default()
            }),
            ..Default::default()
        };

        mgr.handle_add_worker_info(worker).await.unwrap();
        let all = mgr.handle_get_all_worker_info(None).await.unwrap();
        assert_eq!(all.len(), 1);
    }

    #[tokio::test]
    async fn test_report_system_error_failure() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![1, 2, 3],
                ..Default::default()
            }),
            exit_type: Some(4), // SYSTEM_ERROR_EXIT
            ..Default::default()
        };

        mgr.handle_report_worker_failure(worker).await.unwrap();
        assert_eq!(mgr.system_error_count(), 1);
        assert_eq!(mgr.oom_count(), 0);
    }

    #[tokio::test]
    async fn test_report_oom_failure() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![4, 5, 6],
                ..Default::default()
            }),
            exit_type: Some(8), // NODE_OUT_OF_MEMORY
            ..Default::default()
        };

        mgr.handle_report_worker_failure(worker).await.unwrap();
        assert_eq!(mgr.oom_count(), 1);
        assert_eq!(mgr.system_error_count(), 0);
    }

    #[tokio::test]
    async fn test_dead_listener_called() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called_clone = Arc::clone(&called);
        mgr.add_dead_listener(Box::new(move |_data| {
            called_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        }));

        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![1],
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_report_worker_failure(worker).await.unwrap();
        assert!(called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_get_all_worker_info_with_limit() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        for i in 1..=5u8 {
            let worker = ray_proto::ray::rpc::WorkerTableData {
                worker_address: Some(ray_proto::ray::rpc::Address {
                    worker_id: vec![i],
                    ..Default::default()
                }),
                ..Default::default()
            };
            mgr.handle_add_worker_info(worker).await.unwrap();
        }

        let all = mgr.handle_get_all_worker_info(None).await.unwrap();
        assert_eq!(all.len(), 5);
        let limited = mgr.handle_get_all_worker_info(Some(3)).await.unwrap();
        assert_eq!(limited.len(), 3);
    }

    #[tokio::test]
    async fn test_worker_failure_publishes_to_pubsub() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);

        let handler = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(handler.clone());

        handler.handle_subscribe_command(
            b"test_sub".to_vec(),
            ChannelType::GcsWorkerDeltaChannel as i32,
            vec![],
        );

        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![1, 2, 3],
                node_id: vec![10, 20, 30],
                ..Default::default()
            }),
            exit_type: Some(4),
            ..Default::default()
        };
        mgr.handle_report_worker_failure(worker).await.unwrap();

        let messages = handler.handle_subscriber_poll(b"test_sub", 0).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].channel_type,
            ChannelType::GcsWorkerDeltaChannel as i32
        );
        match &messages[0].inner_message {
            Some(ray_proto::ray::rpc::pub_message::InnerMessage::WorkerDeltaMessage(delta)) => {
                assert_eq!(delta.worker_id, vec![1, 2, 3]);
                assert_eq!(delta.node_id, vec![10, 20, 30]);
            }
            other => panic!("expected WorkerDeltaMessage, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_no_pubsub_handler_does_not_panic() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsWorkerManager::new(storage);
        // No pubsub handler — should not panic
        let worker = ray_proto::ray::rpc::WorkerTableData {
            worker_address: Some(ray_proto::ray::rpc::Address {
                worker_id: vec![1],
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_report_worker_failure(worker).await.unwrap();
    }
}
