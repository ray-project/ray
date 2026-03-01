// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Smoke tests verifying proto types can roundtrip through JSON via serde.

use ray_proto::ray::rpc;

#[test]
fn test_address_serde_roundtrip() {
    let addr = rpc::Address {
        node_id: b"test-node-id-0123456789ab".to_vec(),
        ip_address: "127.0.0.1".to_string(),
        port: 6379,
        worker_id: b"test-worker-id-012345678a".to_vec(),
    };

    let json = serde_json::to_string(&addr).expect("serialize Address");
    let roundtrip: rpc::Address = serde_json::from_str(&json).expect("deserialize Address");

    assert_eq!(addr, roundtrip);
    assert_eq!(roundtrip.ip_address, "127.0.0.1");
    assert_eq!(roundtrip.port, 6379);
}

#[test]
fn test_job_table_data_serde_roundtrip() {
    let job = rpc::JobTableData {
        job_id: vec![0, 0, 0, 42],
        is_dead: false,
        timestamp: 1700000000000,
        driver_ip_address: "10.0.0.1".to_string(),
        driver_pid: 12345,
        start_time: 1700000000000,
        end_time: 0,
        ..Default::default()
    };

    let json = serde_json::to_string(&job).expect("serialize JobTableData");
    let roundtrip: rpc::JobTableData = serde_json::from_str(&json).expect("deserialize JobTableData");

    assert_eq!(job, roundtrip);
    assert_eq!(roundtrip.driver_pid, 12345);
}

#[test]
fn test_task_spec_serde_roundtrip() {
    let task = rpc::TaskSpec {
        r#type: rpc::TaskType::NormalTask as i32,
        name: "my_task".to_string(),
        language: rpc::Language::Python as i32,
        job_id: vec![0, 0, 0, 1],
        task_id: vec![0u8; 24],
        parent_task_id: vec![0u8; 24],
        num_returns: 1,
        ..Default::default()
    };

    let json = serde_json::to_string(&task).expect("serialize TaskSpec");
    let roundtrip: rpc::TaskSpec = serde_json::from_str(&json).expect("deserialize TaskSpec");

    assert_eq!(task, roundtrip);
    assert_eq!(roundtrip.name, "my_task");
    assert_eq!(roundtrip.num_returns, 1);
}

#[test]
fn test_address_json_pretty_format() {
    // Verify that JSON serialization produces readable output.
    let addr = rpc::Address {
        node_id: vec![],
        ip_address: "10.0.0.2".to_string(),
        port: 8080,
        worker_id: vec![],
    };

    let json = serde_json::to_string_pretty(&addr).expect("serialize Address");
    assert!(json.contains("10.0.0.2"));
    assert!(json.contains("8080"));

    let roundtrip: rpc::Address = serde_json::from_str(&json).expect("deserialize Address");
    assert_eq!(addr, roundtrip);
}
