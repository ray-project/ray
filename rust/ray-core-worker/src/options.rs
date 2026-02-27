// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Core worker configuration options.

use ray_common::id::{ClusterID, JobID, NodeID, WorkerID};

/// The type of worker process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WorkerType {
    Worker,
    Driver,
    SpillWorker,
    RestoreWorker,
}

impl WorkerType {
    /// Convert to the protobuf `WorkerType` enum value.
    pub fn to_proto(self) -> i32 {
        match self {
            WorkerType::Worker => 0,
            WorkerType::Driver => 1,
            WorkerType::SpillWorker => 2,
            WorkerType::RestoreWorker => 3,
        }
    }

    /// Convert from the protobuf `WorkerType` enum value.
    pub fn from_proto(value: i32) -> Option<Self> {
        match value {
            0 => Some(WorkerType::Worker),
            1 => Some(WorkerType::Driver),
            2 => Some(WorkerType::SpillWorker),
            3 => Some(WorkerType::RestoreWorker),
            _ => None,
        }
    }
}

/// The programming language of the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Language {
    Python,
    Java,
    Cpp,
}

impl Language {
    /// Convert to the protobuf `Language` enum value.
    pub fn to_proto(self) -> i32 {
        match self {
            Language::Python => 0,
            Language::Java => 1,
            Language::Cpp => 2,
        }
    }

    /// Convert from the protobuf `Language` enum value.
    pub fn from_proto(value: i32) -> Option<Self> {
        match value {
            0 => Some(Language::Python),
            1 => Some(Language::Java),
            2 => Some(Language::Cpp),
            _ => None,
        }
    }
}

/// Options for initializing a CoreWorker.
#[derive(Debug, Clone)]
pub struct CoreWorkerOptions {
    pub worker_type: WorkerType,
    pub language: Language,
    pub store_socket: String,
    pub raylet_socket: String,
    pub job_id: JobID,
    pub gcs_address: String,
    pub node_ip_address: String,
    pub worker_id: WorkerID,
    pub node_id: NodeID,
    pub cluster_id: ClusterID,
    pub session_name: String,
    pub num_workers: usize,
}

impl Default for CoreWorkerOptions {
    fn default() -> Self {
        Self {
            worker_type: WorkerType::Worker,
            language: Language::Python,
            store_socket: String::new(),
            raylet_socket: String::new(),
            job_id: JobID::nil(),
            gcs_address: String::new(),
            node_ip_address: "127.0.0.1".to_string(),
            worker_id: WorkerID::from_random(),
            node_id: NodeID::nil(),
            cluster_id: ClusterID::nil(),
            session_name: String::new(),
            num_workers: 1,
        }
    }
}
