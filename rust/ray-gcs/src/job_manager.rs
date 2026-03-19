// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! GCS Job Manager — tracks job lifecycle.
//!
//! Replaces `src/ray/gcs/gcs_job_manager.h/cc`.

use std::sync::{Arc, OnceLock};

use dashmap::DashMap;
use parking_lot::RwLock;
use ray_common::id::{JobID, NodeID};

use crate::pubsub_handler::{ChannelType, InternalPubSubHandler};
use crate::table_storage::GcsTableStorage;

/// Callback invoked when a job finishes.
pub type JobFinishCallback = Box<dyn Fn(&JobID) + Send + Sync>;

/// The GCS job manager tracks all jobs in the cluster.
pub struct GcsJobManager {
    /// Currently running jobs: job_id → start_time_ms.
    running_jobs: DashMap<JobID, i64>,
    /// Cached job configs: job_id → JobConfig proto.
    job_configs: DashMap<JobID, ray_proto::ray::rpc::JobConfig>,
    /// All job data (including finished) — DashMap for per-entry concurrency.
    job_data: DashMap<JobID, ray_proto::ray::rpc::JobTableData>,
    /// Listeners notified when a job finishes.
    finish_listeners: RwLock<Vec<JobFinishCallback>>,
    /// Count of finished jobs since GCS start.
    finished_jobs_count: std::sync::atomic::AtomicI64,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
    /// Pubsub handler for publishing job state changes (set once during init).
    pubsub_handler: OnceLock<Arc<InternalPubSubHandler>>,
}

impl GcsJobManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            running_jobs: DashMap::new(),
            job_configs: DashMap::new(),
            job_data: DashMap::new(),
            finish_listeners: RwLock::new(Vec::new()),
            finished_jobs_count: std::sync::atomic::AtomicI64::new(0),
            table_storage,
            pubsub_handler: OnceLock::new(),
        }
    }

    /// Set the pubsub handler (called once during server initialization).
    pub fn set_pubsub_handler(&self, handler: Arc<InternalPubSubHandler>) {
        let _ = self.pubsub_handler.set(handler);
    }

    /// Publish job state change via pubsub.
    fn publish_job_state(&self, job_data: &ray_proto::ray::rpc::JobTableData) {
        if let Some(handler) = self.pubsub_handler.get() {
            let pub_msg = ray_proto::ray::rpc::PubMessage {
                channel_type: ChannelType::GcsJobChannel as i32,
                key_id: job_data.job_id.clone(),
                inner_message: Some(ray_proto::ray::rpc::pub_message::InnerMessage::JobMessage(
                    job_data.clone(),
                )),
                ..Default::default()
            };
            handler.publish_pubmessage(pub_msg);
        }
    }

    /// Publish a job error via pubsub.
    fn publish_job_error(&self, error_data: &ray_proto::ray::rpc::ErrorTableData) {
        if let Some(handler) = self.pubsub_handler.get() {
            let job_id =
                JobID::from_binary(error_data.job_id.as_slice().try_into().unwrap_or(&[0u8; 4]));
            let pub_msg = ray_proto::ray::rpc::PubMessage {
                channel_type: ChannelType::RayErrorInfoChannel as i32,
                key_id: job_id.hex().into_bytes(),
                inner_message: Some(
                    ray_proto::ray::rpc::pub_message::InnerMessage::ErrorInfoMessage(
                        error_data.clone(),
                    ),
                ),
                ..Default::default()
            };
            handler.publish_pubmessage(pub_msg);
        }
    }

    /// Initialize from persisted data.
    pub async fn initialize(&self) -> anyhow::Result<()> {
        let all_jobs = self
            .table_storage
            .job_table()
            .get_all()
            .await
            .map_err(|e| anyhow::anyhow!("{}", e))?;

        for (key, job) in all_jobs {
            let job_id = JobID::from_hex(&key);
            if !job.is_dead {
                self.running_jobs.insert(job_id, job.start_time as i64);
            }
            if let Some(config) = &job.config {
                self.job_configs.insert(job_id, config.clone());
            }
            self.job_data.insert(job_id, job);
        }
        Ok(())
    }

    /// Handle AddJob RPC.
    pub async fn handle_add_job(
        &self,
        job_data: ray_proto::ray::rpc::JobTableData,
    ) -> Result<(), tonic::Status> {
        let job_id_bytes = &job_data.job_id;
        let job_id = JobID::from_binary(job_id_bytes.as_slice().try_into().unwrap_or(&[0u8; 4]));
        let key = hex::encode(job_id_bytes);
        let now = ray_util::time::current_time_ms() as i64;

        // Cache config
        if let Some(config) = &job_data.config {
            self.job_configs.insert(job_id, config.clone());
        }

        self.running_jobs.insert(job_id, now);
        self.job_data.insert(job_id, job_data.clone());

        // Persist
        self.table_storage
            .job_table()
            .put(&key, &job_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        // Publish via pubsub
        self.publish_job_state(&job_data);

        tracing::info!(?job_id, "Job added");
        Ok(())
    }

    /// Handle MarkJobFinished RPC.
    pub async fn handle_mark_job_finished(&self, job_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let job_id = JobID::from_binary(job_id_bytes.try_into().unwrap_or(&[0u8; 4]));
        let key = hex::encode(job_id_bytes);

        // Update in-memory state
        self.running_jobs.remove(&job_id);
        self.finished_jobs_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Update job data — DashMap entry lock is held only briefly
        let updated = if let Some(mut entry) = self.job_data.get_mut(&job_id) {
            entry.is_dead = true;
            entry.end_time = ray_util::time::current_time_ms();
            Some(entry.clone())
        } else {
            None
        };

        if let Some(ref updated) = updated {
            let _ = self.table_storage.job_table().put(&key, updated).await;
            // Publish via pubsub
            self.publish_job_state(updated);
        }

        // Notify listeners
        let listeners = self.finish_listeners.read();
        for listener in listeners.iter() {
            listener(&job_id);
        }

        tracing::info!(?job_id, "Job finished");
        Ok(())
    }

    /// Handle GetAllJobInfo RPC.
    pub fn handle_get_all_job_info(
        &self,
        limit: Option<usize>,
    ) -> Vec<ray_proto::ray::rpc::JobTableData> {
        if let Some(limit) = limit {
            self.job_data.iter().take(limit).map(|r| r.value().clone()).collect()
        } else {
            self.job_data.iter().map(|r| r.value().clone()).collect()
        }
    }

    /// Handle GetNextJobID RPC.
    pub async fn handle_get_next_job_id(&self) -> Result<i32, tonic::Status> {
        self.table_storage
            .store_client()
            .get_next_job_id()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    /// Handle ReportJobError RPC.
    pub fn handle_report_job_error(
        &self,
        error_data: ray_proto::ray::rpc::ErrorTableData,
    ) -> Result<(), tonic::Status> {
        self.publish_job_error(&error_data);
        Ok(())
    }

    /// Register a listener for job completion.
    pub fn add_finish_listener(&self, callback: JobFinishCallback) {
        self.finish_listeners.write().push(callback);
    }

    /// Handle node death — mark all jobs whose driver was on that node as finished.
    ///
    /// Matches C++ `GcsJobManager::OnNodeDead`: iterates running jobs, checks if
    /// the job's `driver_address.node_id` matches the dead node, and if so marks
    /// the job as finished (is_dead=true, end_time set) and notifies listeners.
    ///
    /// This method is synchronous so it can be called from node-removed listeners.
    /// Persistence to storage is spawned as a background task.
    pub fn on_node_dead(&self, node_id: &NodeID) {
        let node_id_bytes = node_id.binary().to_vec();

        // Collect job IDs whose driver was on this node.
        let affected_jobs: Vec<JobID> = self
            .running_jobs
            .iter()
            .filter_map(|entry| {
                let job_id = *entry.key();
                if let Some(job) = self.job_data.get(&job_id) {
                    if let Some(ref addr) = job.driver_address {
                        if addr.node_id == node_id_bytes {
                            return Some(job_id);
                        }
                    }
                }
                None
            })
            .collect();

        for job_id in &affected_jobs {
            // Mark finished in-memory
            self.running_jobs.remove(job_id);
            self.finished_jobs_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            let updated = if let Some(mut entry) = self.job_data.get_mut(job_id) {
                entry.is_dead = true;
                entry.end_time = ray_util::time::current_time_ms();
                Some(entry.clone())
            } else {
                None
            };

            if let Some(ref updated) = updated {
                self.publish_job_state(updated);

                // Persist asynchronously
                let key = hex::encode(&updated.job_id);
                let storage = Arc::clone(&self.table_storage);
                let updated_clone = updated.clone();
                tokio::spawn(async move {
                    let _ = storage.job_table().put(&key, &updated_clone).await;
                });
            }

            // Notify listeners
            let listeners = self.finish_listeners.read();
            for listener in listeners.iter() {
                listener(job_id);
            }

            tracing::info!(?job_id, ?node_id, "Job finished due to node death");
        }
    }

    /// Get a cached job config.
    pub fn get_job_config(&self, job_id: &JobID) -> Option<ray_proto::ray::rpc::JobConfig> {
        self.job_configs.get(job_id).map(|r| r.value().clone())
    }

    /// Number of currently running jobs.
    pub fn num_running_jobs(&self) -> usize {
        self.running_jobs.len()
    }

    /// Total finished jobs since GCS start.
    pub fn finished_jobs_count(&self) -> i64 {
        self.finished_jobs_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub_handler::InternalPubSubHandler;
    use crate::store_client::InMemoryStoreClient;

    #[tokio::test]
    async fn test_add_and_finish_job() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            is_dead: false,
            config: Some(ray_proto::ray::rpc::JobConfig {
                ..Default::default()
            }),
            ..Default::default()
        };

        mgr.handle_add_job(job_data).await.unwrap();
        assert_eq!(mgr.num_running_jobs(), 1);

        mgr.handle_mark_job_finished(&[1, 0, 0, 0]).await.unwrap();
        assert_eq!(mgr.num_running_jobs(), 0);
        assert_eq!(mgr.finished_jobs_count(), 1);
    }

    #[tokio::test]
    async fn test_get_all_job_info() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        for i in 1..=3u8 {
            let job_data = ray_proto::ray::rpc::JobTableData {
                job_id: vec![i, 0, 0, 0],
                ..Default::default()
            };
            mgr.handle_add_job(job_data).await.unwrap();
        }

        let all = mgr.handle_get_all_job_info(None);
        assert_eq!(all.len(), 3);

        let limited = mgr.handle_get_all_job_info(Some(2));
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn test_job_config_cached() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let job_id_bytes = vec![1, 0, 0, 0];
        let job_id = JobID::from_binary(job_id_bytes.as_slice().try_into().unwrap());

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: job_id_bytes,
            config: Some(ray_proto::ray::rpc::JobConfig {
                ray_namespace: "test-ns".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };

        mgr.handle_add_job(job_data).await.unwrap();
        let config = mgr.get_job_config(&job_id).unwrap();
        assert_eq!(config.ray_namespace, "test-ns");
    }

    #[tokio::test]
    async fn test_finish_listener_called() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let finished_id = Arc::new(parking_lot::Mutex::new(None));
        let finished_clone = Arc::clone(&finished_id);
        mgr.add_finish_listener(Box::new(move |job_id| {
            *finished_clone.lock() = Some(*job_id);
        }));

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            ..Default::default()
        };
        mgr.handle_add_job(job_data).await.unwrap();
        mgr.handle_mark_job_finished(&[1, 0, 0, 0]).await.unwrap();

        let finished = finished_id.lock();
        assert!(finished.is_some());
    }

    #[tokio::test]
    async fn test_get_next_job_id() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let id1 = mgr.handle_get_next_job_id().await.unwrap();
        let id2 = mgr.handle_get_next_job_id().await.unwrap();
        assert_eq!(id2, id1 + 1);
    }

    #[tokio::test]
    async fn test_on_node_dead_finishes_jobs_on_that_node() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let mut node1_id = vec![0u8; 28];
        node1_id[0] = 1;
        let mut node2_id = vec![0u8; 28];
        node2_id[0] = 2;

        // Job on node 1
        let job1 = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            driver_address: Some(ray_proto::ray::rpc::Address {
                node_id: node1_id.clone(),
                ip_address: "10.0.0.1".to_string(),
                port: 1000,
                worker_id: vec![],
            }),
            ..Default::default()
        };
        mgr.handle_add_job(job1).await.unwrap();

        // Job on node 2
        let job2 = ray_proto::ray::rpc::JobTableData {
            job_id: vec![2, 0, 0, 0],
            driver_address: Some(ray_proto::ray::rpc::Address {
                node_id: node2_id.clone(),
                ip_address: "10.0.0.2".to_string(),
                port: 2000,
                worker_id: vec![],
            }),
            ..Default::default()
        };
        mgr.handle_add_job(job2).await.unwrap();

        assert_eq!(mgr.num_running_jobs(), 2);

        // Kill node 1
        let nid = NodeID::from_binary(node1_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&nid);

        // Job 1 should be finished, job 2 still running
        assert_eq!(mgr.num_running_jobs(), 1);
        assert_eq!(mgr.finished_jobs_count(), 1);

        // Verify job 1 is marked dead
        let job1_id = JobID::from_binary(&[1, 0, 0, 0]);
        let job1_data = mgr.job_data.get(&job1_id).unwrap();
        assert!(job1_data.is_dead);
        assert!(job1_data.end_time > 0);

        // Verify job 2 is still alive
        let job2_id = JobID::from_binary(&[2, 0, 0, 0]);
        let job2_data = mgr.job_data.get(&job2_id).unwrap();
        assert!(!job2_data.is_dead);
    }

    #[tokio::test]
    async fn test_on_node_dead_calls_finish_listeners() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let finished_ids = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let finished_clone = Arc::clone(&finished_ids);
        mgr.add_finish_listener(Box::new(move |job_id| {
            finished_clone.lock().push(*job_id);
        }));

        let mut node_id = vec![0u8; 28];
        node_id[0] = 1;

        let job = ray_proto::ray::rpc::JobTableData {
            job_id: vec![5, 0, 0, 0],
            driver_address: Some(ray_proto::ray::rpc::Address {
                node_id: node_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_add_job(job).await.unwrap();

        let nid = NodeID::from_binary(node_id.as_slice().try_into().unwrap());
        mgr.on_node_dead(&nid);

        let ids = finished_ids.lock();
        assert_eq!(ids.len(), 1);
    }

    #[tokio::test]
    async fn test_on_node_dead_no_matching_jobs() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let mut node1_id = vec![0u8; 28];
        node1_id[0] = 1;
        let mut node99_id = [0u8; 28];
        node99_id[0] = 99;

        let job = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            driver_address: Some(ray_proto::ray::rpc::Address {
                node_id: node1_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        mgr.handle_add_job(job).await.unwrap();

        // Kill a different node — no jobs should be affected
        let nid = NodeID::from_binary(&node99_id);
        mgr.on_node_dead(&nid);
        assert_eq!(mgr.num_running_jobs(), 1);
        assert_eq!(mgr.finished_jobs_count(), 0);
    }

    #[tokio::test]
    async fn test_add_job_publishes_to_pubsub() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let handler = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(handler.clone());

        handler.handle_subscribe_command(
            b"test_sub".to_vec(),
            ChannelType::GcsJobChannel as i32,
            vec![],
        );

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            is_dead: false,
            ..Default::default()
        };
        mgr.handle_add_job(job_data).await.unwrap();

        let messages = handler.handle_subscriber_poll(b"test_sub", 0).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].channel_type, ChannelType::GcsJobChannel as i32);
        match &messages[0].inner_message {
            Some(ray_proto::ray::rpc::pub_message::InnerMessage::JobMessage(data)) => {
                assert_eq!(data.job_id, vec![1, 0, 0, 0]);
                assert!(!data.is_dead);
            }
            other => panic!("expected JobMessage, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_finish_job_publishes_dead_state() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);

        let handler = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(handler.clone());

        handler.handle_subscribe_command(
            b"test_sub".to_vec(),
            ChannelType::GcsJobChannel as i32,
            vec![],
        );

        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            is_dead: false,
            ..Default::default()
        };
        mgr.handle_add_job(job_data).await.unwrap();
        mgr.handle_mark_job_finished(&[1, 0, 0, 0]).await.unwrap();

        let messages = handler.handle_subscriber_poll(b"test_sub", 0).await;
        // 2 messages: add + finish
        assert_eq!(messages.len(), 2);
        match &messages[1].inner_message {
            Some(ray_proto::ray::rpc::pub_message::InnerMessage::JobMessage(data)) => {
                assert!(data.is_dead);
            }
            other => panic!("expected JobMessage with is_dead=true, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_no_pubsub_handler_does_not_panic() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);
        // No pubsub handler set — should not panic
        let job_data = ray_proto::ray::rpc::JobTableData {
            job_id: vec![1, 0, 0, 0],
            ..Default::default()
        };
        mgr.handle_add_job(job_data).await.unwrap();
        mgr.handle_mark_job_finished(&[1, 0, 0, 0]).await.unwrap();
    }

    #[tokio::test]
    async fn test_report_job_error_publishes_to_pubsub() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = Arc::new(GcsTableStorage::new(store));
        let mgr = GcsJobManager::new(storage);
        let handler = Arc::new(InternalPubSubHandler::new());
        mgr.set_pubsub_handler(handler.clone());

        let subscriber_id = b"sub-error".to_vec();
        handler.handle_subscribe_command(
            subscriber_id.clone(),
            ChannelType::RayErrorInfoChannel as i32,
            vec![],
        );

        let error = ray_proto::ray::rpc::ErrorTableData {
            job_id: vec![7, 0, 0, 0],
            r#type: "worker_crash".into(),
            error_message: "boom".into(),
            timestamp: 123.0,
            ..Default::default()
        };

        mgr.handle_report_job_error(error.clone()).unwrap();

        let pending = handler.pending_messages_for_test(&subscriber_id);
        assert_eq!(pending.len(), 1);
        let (msg, _) = &pending[0];
        assert_eq!(msg.channel_type, ChannelType::RayErrorInfoChannel as i32);
        match msg.inner_message.as_ref() {
            Some(ray_proto::ray::rpc::pub_message::InnerMessage::ErrorInfoMessage(inner)) => {
                assert_eq!(inner.job_id, error.job_id);
                assert_eq!(inner.r#type, error.r#type);
                assert_eq!(inner.error_message, error.error_message);
            }
            other => panic!("expected error info pubsub message, got {other:?}"),
        }
    }
}
