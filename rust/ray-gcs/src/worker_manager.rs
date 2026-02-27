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
}

impl GcsWorkerManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            dead_listeners: RwLock::new(Vec::new()),
            system_error_count: AtomicI64::new(0),
            oom_count: AtomicI64::new(0),
            table_storage,
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
}
