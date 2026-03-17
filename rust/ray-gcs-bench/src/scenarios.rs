use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tonic::transport::Channel;

use crate::stats::BenchStats;

type RpcResult<T> = Result<tonic::Response<T>, tonic::Status>;
use ray_proto::ray::rpc as rpc;

/// Run KV put+get throughput benchmark.
pub async fn kv_throughput(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for i in 0..requests_per_client {
                let key = format!("bench-{client_id}-{i}").into_bytes();
                let value = format!("value-{i}").into_bytes();

                // PUT
                let put_start = Instant::now();
                let put_result: RpcResult<rpc::InternalKvPutReply> = client
                    .internal_kv_put(rpc::InternalKvPutRequest {
                        key: key.clone(),
                        value: value.clone(),
                        overwrite: true,
                        ..Default::default()
                    })
                    .await;
                match put_result {
                    Ok(_) => stats.record(put_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // GET
                let get_start = Instant::now();
                let get_result: RpcResult<rpc::InternalKvGetReply> = client
                    .internal_kv_get(rpc::InternalKvGetRequest {
                        key,
                        ..Default::default()
                    })
                    .await;
                match get_result {
                    Ok(_) => stats.record(get_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run actor register + lookup benchmark.
pub async fn actor_lookup(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));

    // First, register actors
    let mut client =
        rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(channel.clone());

    for i in 0..clients * 2 {
        let actor_id = make_actor_id(i as u32);
        let task_spec = make_register_task_spec(&actor_id, &format!("bench-actor-{i}"));
        let _ = client
            .register_actor(rpc::RegisterActorRequest {
                task_spec: Some(task_spec),
            })
            .await;
    }

    // Now benchmark lookups
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for i in 0..requests_per_client {
                let actor_idx = (client_id * 2 + i % 2) as u32;
                let actor_id = make_actor_id(actor_idx);

                let req_start = Instant::now();
                let result: RpcResult<rpc::GetActorInfoReply> = client
                    .get_actor_info(rpc::GetActorInfoRequest {
                        actor_id: actor_id.to_vec(),
                        name: String::new(),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run node register + GetAllNodeInfo benchmark.
pub async fn node_info(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));

    // Register some nodes first
    let mut client =
        rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel.clone());
    for i in 0..10 {
        let node_id = make_node_id(i);
        let _ = client
            .register_node(rpc::RegisterNodeRequest {
                node_info: Some(rpc::GcsNodeInfo {
                    node_id: node_id.to_vec(),
                    state: 0, // ALIVE
                    node_manager_address: format!("127.0.0.{i}"),
                    node_manager_port: 8000 + i as i32,
                    ..Default::default()
                }),
            })
            .await;
    }

    // Benchmark GetAllNodeInfo
    let mut handles = Vec::new();
    let start = Instant::now();

    for _client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for _ in 0..requests_per_client {
                let req_start = Instant::now();
                let result: RpcResult<rpc::GetAllNodeInfoReply> = client
                    .get_all_node_info(rpc::GetAllNodeInfoRequest {
                                limit: None,
                                node_selectors: vec![],
                                state_filter: None,
                            })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run job add + get all + finish lifecycle benchmark.
pub async fn job_lifecycle(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::job_info_gcs_service_client::JobInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for i in 0..requests_per_client {
                let job_id = make_job_id((client_id * requests_per_client + i) as u32);

                // AddJob
                let req_start = Instant::now();
                let result = client
                    .add_job(rpc::AddJobRequest {
                        data: Some(rpc::JobTableData {
                            job_id: job_id.clone(),
                            is_dead: false,
                            config: Some(rpc::JobConfig {
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // GetAllJobInfo
                let req_start = Instant::now();
                let result = client
                    .get_all_job_info(rpc::GetAllJobInfoRequest {
                        ..Default::default()
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // MarkJobFinished
                let req_start = Instant::now();
                let result = client
                    .mark_job_finished(rpc::MarkJobFinishedRequest {
                        job_id: job_id.clone(),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run mixed workload benchmark: 60% KV, 20% actor, 10% node, 10% job.
pub async fn mixed(
    channel: Channel,
    clients: usize,
    duration_secs: u64,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let deadline = Instant::now() + Duration::from_secs(duration_secs);

    // Pre-register some actors and nodes
    {
        let mut actor_client =
            rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(channel.clone());
        for i in 0..20 {
            let actor_id = make_actor_id(i);
            let task_spec = make_register_task_spec(&actor_id, &format!("mixed-actor-{i}"));
            let _ = actor_client
                .register_actor(rpc::RegisterActorRequest {
                    task_spec: Some(task_spec),
                })
                .await;
        }
        let mut node_client =
            rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(channel.clone());
        for i in 0..5 {
            let node_id = make_node_id(100 + i);
            let _ = node_client
                .register_node(rpc::RegisterNodeRequest {
                    node_info: Some(rpc::GcsNodeInfo {
                        node_id: node_id.to_vec(),
                        state: 0,
                        node_manager_address: format!("10.0.0.{i}"),
                        node_manager_port: 9000 + i as i32,
                        ..Default::default()
                    }),
                })
                .await;
        }
    }

    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut kv_client =
                rpc::internal_kv_gcs_service_client::InternalKvGcsServiceClient::new(ch.clone());
            let mut actor_client =
                rpc::actor_info_gcs_service_client::ActorInfoGcsServiceClient::new(ch.clone());
            let mut node_client =
                rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(ch.clone());
            let mut job_client =
                rpc::job_info_gcs_service_client::JobInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();
            let mut counter = 0u64;

            while Instant::now() < deadline {
                let op = counter % 10;
                counter += 1;

                let req_start = Instant::now();
                let ok = match op {
                    // 60% KV
                    0..=5 => {
                        let key = format!("mixed-{client_id}-{counter}").into_bytes();
                        kv_client
                            .internal_kv_put(rpc::InternalKvPutRequest {
                                key,
                                value: b"v".to_vec(),
                                overwrite: true,
                                ..Default::default()
                            })
                            .await
                            .is_ok()
                    }
                    // 20% actor lookup
                    6 | 7 => {
                        let actor_id = make_actor_id((counter % 20) as u32);
                        actor_client
                            .get_actor_info(rpc::GetActorInfoRequest {
                                actor_id: actor_id.to_vec(),
                                name: String::new(),
                            })
                            .await
                            .is_ok()
                    }
                    // 10% node info
                    8 => {
                        node_client
                            .get_all_node_info(rpc::GetAllNodeInfoRequest {
                                limit: None,
                                node_selectors: vec![],
                                state_filter: None,
                            })
                            .await
                            .is_ok()
                    }
                    // 10% job
                    _ => {
                        job_client
                            .get_all_job_info(rpc::GetAllJobInfoRequest {
                                ..Default::default()
                            })
                            .await
                            .is_ok()
                    }
                };

                if ok {
                    stats.record(req_start.elapsed());
                } else {
                    stats.record_error();
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run node register + query + unregister churn benchmark.
/// Stresses node_manager (alive_nodes, dead_nodes, draining_nodes DashMaps).
pub async fn node_churn(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::node_info_gcs_service_client::NodeInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for i in 0..requests_per_client {
                let node_id = make_node_id((client_id * requests_per_client + i) as u32 + 10000);

                // RegisterNode
                let req_start = Instant::now();
                let result = client
                    .register_node(rpc::RegisterNodeRequest {
                        node_info: Some(rpc::GcsNodeInfo {
                            node_id: node_id.clone(),
                            state: 0, // ALIVE
                            node_manager_address: format!("10.{}.{}.{}", client_id % 256, (i >> 8) & 0xff, i & 0xff),
                            node_manager_port: 8000 + (i % 1000) as i32,
                            ..Default::default()
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // GetAllNodeInfo (read under write pressure)
                let req_start = Instant::now();
                let result = client
                    .get_all_node_info(rpc::GetAllNodeInfoRequest {
                        limit: None,
                        node_selectors: vec![],
                        state_filter: None,
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // UnregisterNode (moves node from alive to dead)
                let req_start = Instant::now();
                let result = client
                    .unregister_node(rpc::UnregisterNodeRequest {
                        node_id: node_id.clone(),
                        node_death_info: Some(rpc::NodeDeathInfo {
                            reason: 1, // EXPECTED_TERMINATION
                            reason_message: "bench".to_string(),
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run placement group create + get + remove storm benchmark.
/// Stresses placement_group_manager (placement_groups, named_placement_groups DashMaps).
pub async fn pg_storm(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::placement_group_info_gcs_service_client::PlacementGroupInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();

            for i in 0..requests_per_client {
                let pg_id = make_placement_group_id((client_id * requests_per_client + i) as u32);
                let pg_name = format!("bench-pg-{client_id}-{i}");
                let job_id = make_job_id(1);

                // CreatePlacementGroup
                let req_start = Instant::now();
                let result = client
                    .create_placement_group(rpc::CreatePlacementGroupRequest {
                        placement_group_spec: Some(rpc::PlacementGroupSpec {
                            placement_group_id: pg_id.clone(),
                            name: pg_name.clone(),
                            bundles: vec![rpc::Bundle {
                                bundle_id: Some(rpc::bundle::BundleIdentifier {
                                    placement_group_id: pg_id.clone(),
                                    bundle_index: 0,
                                }),
                                unit_resources: [("CPU".to_string(), 1.0)].into_iter().collect(),
                                ..Default::default()
                            }],
                            strategy: 1, // SPREAD
                            creator_job_id: job_id.clone(),
                            is_detached: false,
                            ..Default::default()
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // GetPlacementGroup (read under write pressure)
                let req_start = Instant::now();
                let result = client
                    .get_placement_group(rpc::GetPlacementGroupRequest {
                        placement_group_id: pg_id.clone(),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // RemovePlacementGroup
                let req_start = Instant::now();
                let result = client
                    .remove_placement_group(rpc::RemovePlacementGroupRequest {
                        placement_group_id: pg_id.clone(),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

/// Run task event ingestion + query benchmark.
/// Stresses task_manager (task_events, job_index DashMaps).
pub async fn task_events(
    channel: Channel,
    clients: usize,
    requests_per_client: usize,
) -> BenchStats {
    let sem = Arc::new(Semaphore::new(clients));
    let mut handles = Vec::new();
    let start = Instant::now();

    for client_id in 0..clients {
        let ch = channel.clone();
        let sem = sem.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            let mut client =
                rpc::task_info_gcs_service_client::TaskInfoGcsServiceClient::new(ch);
            let mut stats = BenchStats::new();
            let job_id = make_job_id(client_id as u32 + 1);

            for i in 0..requests_per_client {
                // AddTaskEventData — batch of 10 task events
                let base = (client_id * requests_per_client + i) as u32 * 10;
                let events: Vec<rpc::TaskEvents> = (0..10)
                    .map(|j| {
                        let task_id = make_task_id(base + j);
                        rpc::TaskEvents {
                            task_id: task_id.clone(),
                            attempt_number: 0,
                            task_info: Some(rpc::TaskInfoEntry {
                                r#type: 0, // NORMAL_TASK
                                name: format!("bench_task_{base}_{j}"),
                                func_or_class_name: "bench_fn".to_string(),
                                job_id: job_id.clone(),
                                task_id,
                                ..Default::default()
                            }),
                            state_updates: None,
                            profile_events: None,
                            job_id: job_id.clone(),
                        }
                    })
                    .collect();

                let req_start = Instant::now();
                let result = client
                    .add_task_event_data(rpc::AddTaskEventDataRequest {
                        data: Some(rpc::TaskEventData {
                            events_by_task: events,
                            job_id: job_id.clone(),
                            ..Default::default()
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }

                // GetTaskEvents — query by job_id (read under write pressure)
                let req_start = Instant::now();
                let result = client
                    .get_task_events(rpc::GetTaskEventsRequest {
                        limit: Some(100),
                        filters: Some(rpc::get_task_events_request::Filters {
                            job_filters: vec![
                                rpc::get_task_events_request::filters::JobIdFilter {
                                    predicate: 0, // EQUAL
                                    job_id: job_id.clone(),
                                },
                            ],
                            ..Default::default()
                        }),
                    })
                    .await;
                match result {
                    Ok(_) => stats.record(req_start.elapsed()),
                    Err(_) => stats.record_error(),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for h in handles {
        let s = h.await.unwrap();
        merged.merge(&s);
    }
    merged.set_elapsed(start.elapsed());
    merged
}

// --- Helper functions ---

fn make_actor_id(id: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 16];
    bytes[0..4].copy_from_slice(&id.to_be_bytes());
    bytes
}

fn make_node_id(id: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 28];
    bytes[0..4].copy_from_slice(&id.to_be_bytes());
    bytes
}

fn make_job_id(id: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 4];
    bytes.copy_from_slice(&id.to_be_bytes());
    bytes
}

fn make_placement_group_id(id: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 18];
    bytes[0..4].copy_from_slice(&id.to_be_bytes());
    bytes
}

fn make_task_id(id: u32) -> Vec<u8> {
    let mut bytes = vec![0u8; 24];
    bytes[0..4].copy_from_slice(&id.to_be_bytes());
    bytes
}

fn make_register_task_spec(actor_id: &[u8], name: &str) -> rpc::TaskSpec {
    rpc::TaskSpec {
        r#type: 1, // ACTOR_CREATION_TASK
        job_id: vec![0, 0, 0, 1],
        caller_address: Some(rpc::Address {
            node_id: vec![0u8; 28],
            worker_id: vec![0u8; 28],
            ..Default::default()
        }),
        function_descriptor: Some(rpc::FunctionDescriptor {
            function_descriptor: Some(
                rpc::function_descriptor::FunctionDescriptor::PythonFunctionDescriptor(
                    rpc::PythonFunctionDescriptor {
                        class_name: format!("BenchActor_{name}"),
                        ..Default::default()
                    },
                ),
            ),
        }),
        actor_creation_task_spec: Some(rpc::ActorCreationTaskSpec {
            actor_id: actor_id.to_vec(),
            name: name.to_string(),
            ray_namespace: "bench".to_string(),
            ..Default::default()
        }),
        ..Default::default()
    }
}
