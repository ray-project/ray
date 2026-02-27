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

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use ray_common::id::{JobID, NodeID};

use crate::table_storage::GcsTableStorage;

/// Callback invoked when a job finishes.
pub type JobFinishCallback = Box<dyn Fn(&JobID) + Send + Sync>;

/// The GCS job manager tracks all jobs in the cluster.
pub struct GcsJobManager {
    /// Currently running jobs: job_id → start_time_ms.
    running_jobs: RwLock<HashMap<JobID, i64>>,
    /// Cached job configs: job_id → JobConfig proto.
    job_configs: RwLock<HashMap<JobID, ray_proto::ray::rpc::JobConfig>>,
    /// All job data (including finished).
    job_data: RwLock<HashMap<JobID, ray_proto::ray::rpc::JobTableData>>,
    /// Listeners notified when a job finishes.
    finish_listeners: RwLock<Vec<JobFinishCallback>>,
    /// Count of finished jobs since GCS start.
    finished_jobs_count: std::sync::atomic::AtomicI64,
    /// Persistence.
    table_storage: Arc<GcsTableStorage>,
}

impl GcsJobManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            running_jobs: RwLock::new(HashMap::new()),
            job_configs: RwLock::new(HashMap::new()),
            job_data: RwLock::new(HashMap::new()),
            finish_listeners: RwLock::new(Vec::new()),
            finished_jobs_count: std::sync::atomic::AtomicI64::new(0),
            table_storage,
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

        let mut running = self.running_jobs.write();
        let mut configs = self.job_configs.write();
        let mut data = self.job_data.write();

        for (key, job) in all_jobs {
            let job_id = JobID::from_hex(&key);
            if !job.is_dead {
                running.insert(job_id, job.start_time as i64);
            }
            if let Some(config) = &job.config {
                configs.insert(job_id, config.clone());
            }
            data.insert(job_id, job);
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
            self.job_configs.write().insert(job_id, config.clone());
        }

        self.running_jobs.write().insert(job_id, now);
        self.job_data.write().insert(job_id, job_data.clone());

        // Persist
        self.table_storage
            .job_table()
            .put(&key, &job_data)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;

        tracing::info!(?job_id, "Job added");
        Ok(())
    }

    /// Handle MarkJobFinished RPC.
    pub async fn handle_mark_job_finished(&self, job_id_bytes: &[u8]) -> Result<(), tonic::Status> {
        let job_id = JobID::from_binary(job_id_bytes.try_into().unwrap_or(&[0u8; 4]));
        let key = hex::encode(job_id_bytes);

        // Update in-memory state
        self.running_jobs.write().remove(&job_id);
        self.finished_jobs_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Update persisted data (clone outside lock to avoid holding lock across await)
        let updated = {
            let mut job_data = self.job_data.write();
            if let Some(data) = job_data.get_mut(&job_id) {
                data.is_dead = true;
                data.end_time = ray_util::time::current_time_ms();
                Some(data.clone())
            } else {
                None
            }
        };
        if let Some(updated) = updated {
            let _ = self.table_storage.job_table().put(&key, &updated).await;
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
        let data = self.job_data.read();
        if let Some(limit) = limit {
            data.values().take(limit).cloned().collect()
        } else {
            data.values().cloned().collect()
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

    /// Register a listener for job completion.
    pub fn add_finish_listener(&self, callback: JobFinishCallback) {
        self.finish_listeners.write().push(callback);
    }

    /// Handle node death — mark all jobs on that node as finished.
    pub fn on_node_dead(&self, _node_id: &NodeID) {
        // In the C++ implementation, this checks driver_address
        // to match jobs to nodes. For now, we leave this as a no-op
        // since job-to-node mapping requires more state.
    }

    /// Get a cached job config.
    pub fn get_job_config(&self, job_id: &JobID) -> Option<ray_proto::ray::rpc::JobConfig> {
        self.job_configs.read().get(job_id).cloned()
    }

    /// Number of currently running jobs.
    pub fn num_running_jobs(&self) -> usize {
        self.running_jobs.read().len()
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
}
