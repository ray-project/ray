// Copyright 2024 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0

//! Proto message builders matching C++ `test_utils.h` semantics.
//!
//! These generate realistic proto messages for use in tests.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use ray_common::id::*;
use ray_proto::ray::rpc;

use crate::generators;

fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Generate a GcsNodeInfo, matching C++ `GenNodeInfo`.
pub fn gen_node_info(port: u32, address: &str, node_name: &str) -> rpc::GcsNodeInfo {
    rpc::GcsNodeInfo {
        node_id: NodeID::from_random().binary(),
        node_manager_port: port as i32,
        node_manager_address: address.to_string(),
        node_name: node_name.to_string(),
        instance_id: "instance_x".to_string(),
        state: rpc::gcs_node_info::GcsNodeState::Alive as i32,
        ..Default::default()
    }
}

/// Generate a JobTableData, matching C++ `GenJobTableData`.
pub fn gen_job_table_data(job_id: &JobID) -> rpc::JobTableData {
    rpc::JobTableData {
        job_id: job_id.binary(),
        is_dead: false,
        timestamp: current_time_ms(),
        driver_ip_address: "127.0.0.1".to_string(),
        driver_pid: 5667,
        driver_address: Some(rpc::Address {
            ip_address: "127.0.0.1".to_string(),
            port: 1234,
            node_id: UniqueID::from_random().binary(),
            worker_id: UniqueID::from_random().binary(),
        }),
        ..Default::default()
    }
}

/// Generate an ActorTableData, matching C++ `GenActorTableData`.
pub fn gen_actor_table_data(job_id: &JobID) -> rpc::ActorTableData {
    let task_id = generators::random_task_id();
    let actor_id = ActorID::of(job_id, &task_id, 0);
    rpc::ActorTableData {
        actor_id: actor_id.binary(),
        job_id: job_id.binary(),
        state: rpc::actor_table_data::ActorState::Alive as i32,
        max_restarts: 1,
        num_restarts: 0,
        ..Default::default()
    }
}

/// Generate an ErrorTableData, matching C++ `GenErrorTableData`.
pub fn gen_error_table_data(job_id: &JobID) -> rpc::ErrorTableData {
    rpc::ErrorTableData {
        job_id: job_id.binary(),
        ..Default::default()
    }
}

/// Generate a WorkerTableData, matching C++ `GenWorkerTableData`.
pub fn gen_worker_table_data() -> rpc::WorkerTableData {
    rpc::WorkerTableData {
        timestamp: current_time_ms(),
        ..Default::default()
    }
}

/// Generate an AddJobRequest, matching C++ `GenAddJobRequest`.
pub fn gen_add_job_request(
    job_id: &JobID,
    namespace: &str,
    submission_id: Option<&str>,
    address: Option<rpc::Address>,
) -> rpc::AddJobRequest {
    let driver_address = address.unwrap_or_else(|| rpc::Address {
        port: 1234,
        node_id: NodeID::from_random().binary(),
        ip_address: "123.456.7.8".to_string(),
        worker_id: WorkerID::from_random().binary(),
    });

    let mut config = rpc::JobConfig {
        ray_namespace: namespace.to_string(),
        ..Default::default()
    };
    if let Some(sid) = submission_id {
        config
            .metadata
            .insert("job_submission_id".to_string(), sid.to_string());
    }

    let job_data = rpc::JobTableData {
        job_id: job_id.binary(),
        config: Some(config),
        driver_address: Some(driver_address),
        ..Default::default()
    };

    rpc::AddJobRequest {
        data: Some(job_data),
    }
}

/// Build an actor creation task spec (internal helper).
fn gen_actor_creation_task_spec(
    job_id: &JobID,
    max_restarts: i64,
    detached: bool,
    name: &str,
    ray_namespace: &str,
    owner_address: &rpc::Address,
) -> rpc::TaskSpec {
    let actor_id = ActorID::of(job_id, &generators::random_task_id(), 0);
    let task_id = TaskID::for_actor_creation_task(&actor_id);

    let scheduling_strategy = rpc::SchedulingStrategy {
        scheduling_strategy: Some(
            rpc::scheduling_strategy::SchedulingStrategy::DefaultSchedulingStrategy(
                rpc::DefaultSchedulingStrategy {},
            ),
        ),
    };

    rpc::TaskSpec {
        r#type: rpc::TaskType::ActorCreationTask as i32,
        name: format!("{name}::()"),
        language: rpc::Language::Python as i32,
        job_id: job_id.binary(),
        task_id: task_id.binary(),
        caller_id: task_id.binary(),
        caller_address: Some(owner_address.clone()),
        num_returns: 1,
        scheduling_strategy: Some(scheduling_strategy),
        actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
            actor_id: actor_id.binary(),
            max_actor_restarts: max_restarts,
            is_detached: detached,
            name: name.to_string(),
            ray_namespace: ray_namespace.to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Generate a CreateActorRequest, matching C++ `GenCreateActorRequest`.
pub fn gen_create_actor_request(
    job_id: &JobID,
    max_restarts: i64,
    detached: bool,
    name: &str,
    namespace: &str,
) -> rpc::CreateActorRequest {
    let owner_address = rpc::Address {
        node_id: NodeID::from_random().binary(),
        ip_address: "1234".to_string(),
        port: 5678,
        worker_id: WorkerID::from_random().binary(),
    };
    let task_spec =
        gen_actor_creation_task_spec(job_id, max_restarts, detached, name, namespace, &owner_address);
    rpc::CreateActorRequest {
        task_spec: Some(task_spec),
    }
}

/// Generate a RegisterActorRequest, matching C++ `GenRegisterActorRequest`.
pub fn gen_register_actor_request(
    job_id: &JobID,
    max_restarts: i64,
    detached: bool,
    name: &str,
    namespace: &str,
) -> rpc::RegisterActorRequest {
    let owner_address = rpc::Address {
        node_id: NodeID::from_random().binary(),
        ip_address: "1234".to_string(),
        port: 5678,
        worker_id: WorkerID::from_random().binary(),
    };
    let task_spec =
        gen_actor_creation_task_spec(job_id, max_restarts, detached, name, namespace, &owner_address);
    rpc::RegisterActorRequest {
        task_spec: Some(task_spec),
    }
}

/// Generate a PlacementGroupTableData, matching C++ `GenPlacementGroupTableData`.
pub fn gen_placement_group_table_data(
    pg_id: &PlacementGroupID,
    job_id: &JobID,
    bundles: &[HashMap<String, f64>],
    nodes: &[String],
    strategy: i32,
    state: i32,
    name: &str,
) -> rpc::PlacementGroupTableData {
    assert_eq!(bundles.len(), nodes.len());

    let bundle_specs: Vec<rpc::Bundle> = bundles
        .iter()
        .zip(nodes.iter())
        .enumerate()
        .map(|(i, (bundle, node))| {
            let mut spec = rpc::Bundle {
                bundle_id: Some(rpc::bundle::BundleIdentifier {
                    placement_group_id: pg_id.binary(),
                    bundle_index: i as i32,
                }),
                unit_resources: bundle.clone(),
                ..Default::default()
            };
            if !node.is_empty() {
                spec.node_id = node.clone().into_bytes();
            }
            spec
        })
        .collect();

    rpc::PlacementGroupTableData {
        placement_group_id: pg_id.binary(),
        creator_job_id: job_id.binary(),
        state,
        name: name.to_string(),
        strategy,
        bundles: bundle_specs,
        ..Default::default()
    }
}

/// Generate a ResourceDemand, matching C++ `GenResourceDemand`.
pub fn gen_resource_demand(
    shape: &HashMap<String, f64>,
    num_ready_queued: u64,
    num_infeasible: u64,
    backlog_size: i64,
) -> rpc::ResourceDemand {
    rpc::ResourceDemand {
        shape: shape.clone(),
        num_ready_requests_queued: num_ready_queued,
        num_infeasible_requests_queued: num_infeasible,
        backlog_size,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gen_node_info() {
        let info = gen_node_info(6379, "10.0.0.1", "test-node");
        assert!(!info.node_id.is_empty());
        assert_eq!(info.node_manager_port, 6379);
        assert_eq!(info.node_manager_address, "10.0.0.1");
        assert_eq!(info.node_name, "test-node");
        assert_eq!(info.instance_id, "instance_x");
        assert_eq!(
            info.state,
            rpc::gcs_node_info::GcsNodeState::Alive as i32
        );
    }

    #[test]
    fn test_gen_job_table_data() {
        let job_id = JobID::from_int(42);
        let data = gen_job_table_data(&job_id);
        assert_eq!(data.job_id, job_id.binary());
        assert!(!data.is_dead);
        assert!(data.timestamp > 0);
        assert_eq!(data.driver_ip_address, "127.0.0.1");
        assert_eq!(data.driver_pid, 5667);
        assert!(data.driver_address.is_some());
    }

    #[test]
    fn test_gen_actor_table_data() {
        let job_id = JobID::from_int(7);
        let data = gen_actor_table_data(&job_id);
        assert!(!data.actor_id.is_empty());
        assert_eq!(data.job_id, job_id.binary());
        assert_eq!(data.max_restarts, 1);
    }

    #[test]
    fn test_gen_error_table_data() {
        let job_id = JobID::from_int(1);
        let data = gen_error_table_data(&job_id);
        assert_eq!(data.job_id, job_id.binary());
    }

    #[test]
    fn test_gen_worker_table_data() {
        let data = gen_worker_table_data();
        assert!(data.timestamp > 0);
    }

    #[test]
    fn test_gen_add_job_request() {
        let job_id = JobID::from_int(10);
        let req = gen_add_job_request(&job_id, "test_ns", Some("sub-123"), None);
        let data = req.data.unwrap();
        assert_eq!(data.job_id, job_id.binary());
        let config = data.config.unwrap();
        assert_eq!(config.ray_namespace, "test_ns");
        assert_eq!(
            config.metadata.get("job_submission_id").unwrap(),
            "sub-123"
        );
    }

    #[test]
    fn test_gen_create_actor_request() {
        let job_id = JobID::from_int(5);
        let req = gen_create_actor_request(&job_id, 3, true, "my_actor", "ns");
        let spec = req.task_spec.unwrap();
        assert_eq!(spec.r#type, rpc::TaskType::ActorCreationTask as i32);
        assert_eq!(spec.language, rpc::Language::Python as i32);
        let actor_spec = spec.actor_creation_task_spec.unwrap();
        assert_eq!(actor_spec.max_actor_restarts, 3);
        assert!(actor_spec.is_detached);
        assert_eq!(actor_spec.name, "my_actor");
        assert_eq!(actor_spec.ray_namespace, "ns");
    }

    #[test]
    fn test_gen_register_actor_request() {
        let job_id = JobID::from_int(8);
        let req = gen_register_actor_request(&job_id, 0, false, "actor2", "test");
        let spec = req.task_spec.unwrap();
        assert_eq!(spec.r#type, rpc::TaskType::ActorCreationTask as i32);
    }

    #[test]
    fn test_gen_placement_group_table_data() {
        let job_id = JobID::from_int(1);
        let pg_id = PlacementGroupID::of(&job_id);
        let bundles = vec![
            HashMap::from([("CPU".to_string(), 1.0)]),
            HashMap::from([("CPU".to_string(), 2.0)]),
        ];
        let nodes = vec!["node1".to_string(), "node2".to_string()];
        let data = gen_placement_group_table_data(
            &pg_id,
            &job_id,
            &bundles,
            &nodes,
            rpc::PlacementStrategy::Spread as i32,
            rpc::placement_group_table_data::PlacementGroupState::Created as i32,
            "test_pg",
        );
        assert_eq!(data.placement_group_id, pg_id.binary());
        assert_eq!(data.name, "test_pg");
        assert_eq!(data.bundles.len(), 2);
    }

    #[test]
    fn test_gen_resource_demand() {
        let shape = HashMap::from([("CPU".to_string(), 4.0), ("GPU".to_string(), 1.0)]);
        let demand = gen_resource_demand(&shape, 5u64, 2u64, 10);
        assert_eq!(demand.num_ready_requests_queued, 5u64);
        assert_eq!(demand.num_infeasible_requests_queued, 2u64);
        assert_eq!(demand.backlog_size, 10);
        assert_eq!(demand.shape.len(), 2);
    }
}
