// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Mock RPC client stubs for unit testing.
//!
//! These are intentionally simple in Phase 0 — they record requests for inspection.
//! Later phases will add response generation.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use ray_proto::ray::rpc;

/// Captured request variants from a fake raylet client.
#[derive(Debug, Clone)]
pub enum FakeRayletRequest {
    /// A resource report was sent.
    ReportWorkerBacklog(rpc::ReportWorkerBacklogRequest),
    /// A lease request was sent.
    RequestWorkerLease(Box<rpc::RequestWorkerLeaseRequest>),
    /// A return request was sent.
    ReturnWorkerLease(rpc::ReturnWorkerLeaseRequest),
    /// A release unused bundles request.
    ReleaseUnusedBundles(rpc::ReleaseUnusedBundlesRequest),
}

/// A fake raylet client that records requests for test inspection.
#[derive(Clone)]
pub struct FakeRayletClient {
    requests: Arc<Mutex<VecDeque<FakeRayletRequest>>>,
}

impl FakeRayletClient {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Record a request.
    pub fn push_request(&self, request: FakeRayletRequest) {
        self.requests.lock().unwrap().push_back(request);
    }

    /// Pop the oldest recorded request.
    pub fn pop_request(&self) -> Option<FakeRayletRequest> {
        self.requests.lock().unwrap().pop_front()
    }

    /// Number of recorded requests.
    pub fn num_requests(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Default for FakeRayletClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Captured request variants from a fake core worker client.
#[derive(Debug, Clone)]
pub enum FakeCoreWorkerRequest {
    /// A push task request.
    PushTask(Box<rpc::PushTaskRequest>),
    /// A kill actor request.
    KillActor(Box<rpc::KillActorRequest>),
}

/// A fake core worker client that records requests for test inspection.
#[derive(Clone)]
pub struct FakeCoreWorkerClient {
    requests: Arc<Mutex<VecDeque<FakeCoreWorkerRequest>>>,
}

impl FakeCoreWorkerClient {
    pub fn new() -> Self {
        Self {
            requests: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Record a request.
    pub fn push_request(&self, request: FakeCoreWorkerRequest) {
        self.requests.lock().unwrap().push_back(request);
    }

    /// Pop the oldest recorded request.
    pub fn pop_request(&self) -> Option<FakeCoreWorkerRequest> {
        self.requests.lock().unwrap().pop_front()
    }

    /// Number of recorded requests.
    pub fn num_requests(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
}

impl Default for FakeCoreWorkerClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fake_raylet_client_records_requests() {
        let client = FakeRayletClient::new();
        assert_eq!(client.num_requests(), 0);

        client.push_request(FakeRayletRequest::ReturnWorkerLease(
            rpc::ReturnWorkerLeaseRequest::default(),
        ));
        client.push_request(FakeRayletRequest::ReturnWorkerLease(
            rpc::ReturnWorkerLeaseRequest::default(),
        ));

        assert_eq!(client.num_requests(), 2);

        let req = client.pop_request();
        assert!(req.is_some());
        assert_eq!(client.num_requests(), 1);

        let req = client.pop_request();
        assert!(req.is_some());
        assert_eq!(client.num_requests(), 0);

        assert!(client.pop_request().is_none());
    }

    #[test]
    fn test_fake_core_worker_client_records_requests() {
        let client = FakeCoreWorkerClient::new();
        assert_eq!(client.num_requests(), 0);

        client.push_request(FakeCoreWorkerRequest::PushTask(
            Box::new(rpc::PushTaskRequest::default()),
        ));
        assert_eq!(client.num_requests(), 1);

        client.push_request(FakeCoreWorkerRequest::KillActor(
            Box::new(rpc::KillActorRequest::default()),
        ));
        assert_eq!(client.num_requests(), 2);

        let _ = client.pop_request();
        let _ = client.pop_request();
        assert_eq!(client.num_requests(), 0);
    }

    #[test]
    fn test_fake_clients_are_clone_and_thread_safe() {
        let client = FakeRayletClient::new();
        let client2 = client.clone();

        client.push_request(FakeRayletRequest::ReturnWorkerLease(
            rpc::ReturnWorkerLeaseRequest::default(),
        ));

        // Both clones see the same state
        assert_eq!(client2.num_requests(), 1);
    }
}
