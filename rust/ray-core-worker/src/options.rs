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
    /// Maximum concurrent task execution. 0 = unlimited.
    pub max_concurrency: usize,
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
            max_concurrency: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_type_proto_roundtrip() {
        for (wt, val) in [
            (WorkerType::Worker, 0),
            (WorkerType::Driver, 1),
            (WorkerType::SpillWorker, 2),
            (WorkerType::RestoreWorker, 3),
        ] {
            assert_eq!(wt.to_proto(), val);
            assert_eq!(WorkerType::from_proto(val), Some(wt));
        }
        assert_eq!(WorkerType::from_proto(99), None);
    }

    #[test]
    fn test_language_proto_roundtrip() {
        for (lang, val) in [
            (Language::Python, 0),
            (Language::Java, 1),
            (Language::Cpp, 2),
        ] {
            assert_eq!(lang.to_proto(), val);
            assert_eq!(Language::from_proto(val), Some(lang));
        }
        assert_eq!(Language::from_proto(99), None);
    }

    #[test]
    fn test_default_options() {
        let opts = CoreWorkerOptions::default();
        assert_eq!(opts.worker_type, WorkerType::Worker);
        assert_eq!(opts.language, Language::Python);
        assert_eq!(opts.node_ip_address, "127.0.0.1");
        assert_eq!(opts.num_workers, 1);
        assert_eq!(opts.max_concurrency, 0);
        assert!(opts.job_id.is_nil());
        assert!(opts.node_id.is_nil());
        assert!(!opts.worker_id.is_nil()); // random, not nil
    }

    #[test]
    fn test_options_clone() {
        let opts1 = CoreWorkerOptions {
            worker_type: WorkerType::Driver,
            job_id: JobID::from_int(42),
            ..CoreWorkerOptions::default()
        };
        let opts2 = opts1.clone();
        assert_eq!(opts2.worker_type, WorkerType::Driver);
        assert_eq!(opts2.job_id, JobID::from_int(42));
    }
}
