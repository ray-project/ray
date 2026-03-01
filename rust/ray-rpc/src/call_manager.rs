// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Client call manager — tracks RPC lifecycle metadata.
//!
//! Replaces `src/ray/rpc/client_call.h` (ClientCallManager).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::MetadataMap;
use tonic::Request;

/// Manages metadata injection and call tracking for outgoing RPCs.
///
/// Each `ClientCallManager` is associated with a cluster and injects
/// request IDs and cluster identity into outgoing gRPC metadata.
#[derive(Clone)]
pub struct ClientCallManager {
    cluster_id: Arc<String>,
    next_request_id: Arc<AtomicU64>,
    default_timeout: Duration,
    total_calls: Arc<AtomicU64>,
    successful_calls: Arc<AtomicU64>,
    failed_calls: Arc<AtomicU64>,
}

impl ClientCallManager {
    pub fn new(cluster_id: String, default_timeout: Duration) -> Self {
        Self {
            cluster_id: Arc::new(cluster_id),
            next_request_id: Arc::new(AtomicU64::new(1)),
            default_timeout,
            total_calls: Arc::new(AtomicU64::new(0)),
            successful_calls: Arc::new(AtomicU64::new(0)),
            failed_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Generate a unique request ID.
    pub fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// Get the default timeout for RPC calls.
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }

    /// Prepare a tonic Request by injecting cluster metadata and deadline.
    pub fn prepare_request<T>(&self, mut request: Request<T>) -> Request<T> {
        let request_id = self.next_request_id();
        let metadata = request.metadata_mut();
        inject_metadata(metadata, &self.cluster_id, request_id);
        request.set_timeout(self.default_timeout);
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        request
    }

    /// Prepare a request with a custom timeout override.
    pub fn prepare_request_with_timeout<T>(
        &self,
        mut request: Request<T>,
        timeout: Duration,
    ) -> Request<T> {
        let request_id = self.next_request_id();
        let metadata = request.metadata_mut();
        inject_metadata(metadata, &self.cluster_id, request_id);
        request.set_timeout(timeout);
        self.total_calls.fetch_add(1, Ordering::Relaxed);
        request
    }

    /// Record a successful call.
    pub fn record_success(&self) {
        self.successful_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed call.
    pub fn record_failure(&self) {
        self.failed_calls.fetch_add(1, Ordering::Relaxed);
    }

    /// Total number of calls prepared.
    pub fn total_calls(&self) -> u64 {
        self.total_calls.load(Ordering::Relaxed)
    }

    /// Number of successful calls recorded.
    pub fn successful_calls(&self) -> u64 {
        self.successful_calls.load(Ordering::Relaxed)
    }

    /// Number of failed calls recorded.
    pub fn failed_calls(&self) -> u64 {
        self.failed_calls.load(Ordering::Relaxed)
    }
}

/// Inject standard Ray metadata into a gRPC metadata map.
fn inject_metadata(metadata: &mut MetadataMap, cluster_id: &str, request_id: u64) {
    if let Ok(val) = cluster_id.parse() {
        metadata.insert("x-ray-cluster-id", val);
    }
    if let Ok(val) = request_id.to_string().parse() {
        metadata.insert("x-ray-request-id", val);
    }
}

/// Extract the cluster ID from incoming request metadata.
pub fn extract_cluster_id<T>(request: &Request<T>) -> Option<&str> {
    request
        .metadata()
        .get("x-ray-cluster-id")
        .and_then(|v| v.to_str().ok())
}

/// Extract the request ID from incoming request metadata.
pub fn extract_request_id<T>(request: &Request<T>) -> Option<u64> {
    request
        .metadata()
        .get("x-ray-request-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_manager() -> ClientCallManager {
        ClientCallManager::new("test-cluster-123".to_string(), Duration::from_secs(30))
    }

    #[test]
    fn test_request_id_uniqueness() {
        let mgr = make_manager();
        let id1 = mgr.next_request_id();
        let id2 = mgr.next_request_id();
        let id3 = mgr.next_request_id();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_eq!(id2, id1 + 1);
        assert_eq!(id3, id2 + 1);
    }

    #[test]
    fn test_request_ids_start_at_one() {
        let mgr = make_manager();
        assert_eq!(mgr.next_request_id(), 1);
        assert_eq!(mgr.next_request_id(), 2);
    }

    #[test]
    fn test_cluster_id() {
        let mgr = make_manager();
        assert_eq!(mgr.cluster_id(), "test-cluster-123");
    }

    #[test]
    fn test_default_timeout() {
        let mgr = make_manager();
        assert_eq!(mgr.default_timeout(), Duration::from_secs(30));
    }

    #[test]
    fn test_prepare_request_injects_metadata() {
        let mgr = make_manager();
        let request = Request::new(());
        let prepared = mgr.prepare_request(request);
        let metadata = prepared.metadata();
        assert_eq!(
            metadata.get("x-ray-cluster-id").unwrap().to_str().unwrap(),
            "test-cluster-123"
        );
        assert!(metadata.get("x-ray-request-id").is_some());
    }

    #[test]
    fn test_prepare_request_increments_total_calls() {
        let mgr = make_manager();
        assert_eq!(mgr.total_calls(), 0);
        let _ = mgr.prepare_request(Request::new(()));
        assert_eq!(mgr.total_calls(), 1);
        let _ = mgr.prepare_request(Request::new(()));
        assert_eq!(mgr.total_calls(), 2);
    }

    #[test]
    fn test_prepare_request_with_custom_timeout() {
        let mgr = make_manager();
        let request = Request::new(());
        let prepared =
            mgr.prepare_request_with_timeout(request, Duration::from_millis(500));
        let metadata = prepared.metadata();
        assert!(metadata.get("x-ray-cluster-id").is_some());
    }

    #[test]
    fn test_record_success_and_failure() {
        let mgr = make_manager();
        assert_eq!(mgr.successful_calls(), 0);
        assert_eq!(mgr.failed_calls(), 0);

        mgr.record_success();
        mgr.record_success();
        mgr.record_failure();

        assert_eq!(mgr.successful_calls(), 2);
        assert_eq!(mgr.failed_calls(), 1);
    }

    #[test]
    fn test_extract_cluster_id() {
        let mgr = make_manager();
        let prepared = mgr.prepare_request(Request::new(()));
        assert_eq!(extract_cluster_id(&prepared), Some("test-cluster-123"));
    }

    #[test]
    fn test_extract_request_id() {
        let mgr = make_manager();
        let prepared = mgr.prepare_request(Request::new(()));
        let req_id = extract_request_id(&prepared);
        assert!(req_id.is_some());
        assert!(req_id.unwrap() >= 1);
    }

    #[test]
    fn test_extract_from_empty_metadata() {
        let request = Request::new(());
        assert_eq!(extract_cluster_id(&request), None);
        assert_eq!(extract_request_id(&request), None);
    }

    #[test]
    fn test_clone_shares_state() {
        let mgr = make_manager();
        let clone = mgr.clone();
        let _ = mgr.prepare_request(Request::new(()));
        assert_eq!(clone.total_calls(), 1);
        mgr.record_success();
        assert_eq!(clone.successful_calls(), 1);
    }
}
